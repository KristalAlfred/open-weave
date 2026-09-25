use weave_core::{
    DesiredHop, EgressStatus, HopEndpointClass, HopProfile, HopState, HopStatus, LinkCondition,
    NetworkAttachment, NetworkListeners, NodeCapabilities, NodeDescriptor, NodeStatus,
    NodeTopology, PortRange, ResolvedAddr, RoleSet, SocketSpec, SocketStatus, SrtEndpoint,
    SrtListener, SrtSocket, StreamDefinition, StreamDestination, StreamTransport, Transport,
    TransportClass,
};

use crate::keys::LinkKeys;
use crate::path::{HeldPorts, PlannedStream, PortAllocator, derive_stream};

fn profile(id: &str, merge: bool) -> HopProfile {
    let srt = || {
        HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        })
    };
    HopProfile {
        id: id.to_string(),
        ingress: srt(),
        egress: srt(),
        max_egresses: None,
        merge,
        accepts: None,
    }
}

fn attachment(id: &str, network: &str, listener: Option<(&str, u16)>) -> NetworkAttachment {
    NetworkAttachment {
        id: id.to_string(),
        network: network.to_string(),
        dial: true,
        listeners: NetworkListeners {
            srt: listener.map(|(host, ports)| SrtListener {
                host: host.to_string(),
                port_range: PortRange {
                    start: 20_000,
                    end: 20_000 + ports - 1,
                },
            }),
            whip: None,
            whep: None,
            rist: None,
        },
    }
}

fn node(id: &str, attachments: Vec<NetworkAttachment>) -> NodeDescriptor {
    NodeDescriptor {
        id: id.to_string(),
        endpoint: format!("http://{id}"),
        status: NodeStatus::Ready,
        capabilities: NodeCapabilities {
            adapters: Vec::new(),
            hop_profiles: vec![profile("srt-forward", false), profile("srt-merge", true)],
        },
        topology: NodeTopology { attachments },
    }
}

/// Dials out over `internet-a` and `internet-b` and listens only on a site
/// network of its own.
fn nat_node(id: &str) -> NodeDescriptor {
    node(
        id,
        vec![
            attachment("out-a", "internet-a", None),
            attachment("out-b", "internet-b", None),
            attachment("site", &format!("{id}-site"), Some(("192.168.0.10", 1000))),
        ],
    )
}

fn relay(id: &str, network: &str, host: &str, ports: u16) -> NodeDescriptor {
    node(id, vec![attachment("wan", network, Some((host, ports)))])
}

fn set_status(nodes: &mut [NodeDescriptor], id: &str, status: NodeStatus) {
    nodes.iter_mut().find(|node| node.id == id).unwrap().status = status;
}

fn stream(destinations: &[(&str, &str, u8)]) -> StreamDefinition {
    let endpoint = |node: &str| {
        StreamTransport::Srt(SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        })
    };
    StreamDefinition {
        name: "feed".to_string(),
        enabled: true,
        allow_cleartext_links: false,
        source: endpoint("source"),
        destinations: destinations
            .iter()
            .map(|(id, node, paths)| StreamDestination {
                id: (*id).to_string(),
                paths: *paths,
                endpoint: endpoint(node),
            })
            .collect(),
    }
}

fn plan(
    definition: &StreamDefinition,
    nodes: &[NodeDescriptor],
    observed: &[HopStatus],
) -> PlannedStream {
    derive_stream(
        definition,
        nodes,
        observed,
        &mut PortAllocator::holding(HeldPorts::from_reports(observed, nodes)),
        &LinkKeys::for_tests(),
    )
    .expect("derive")
}

/// What each node reports for the hops it was given, resolved the way the
/// Strom adapter resolves them.
fn running(planned: &PlannedStream, nodes: &[NodeDescriptor]) -> Vec<HopStatus> {
    let status = |socket: &SocketSpec, own_host: &str| SocketStatus {
        condition: LinkCondition::Flowing,
        resolved: match socket {
            SocketSpec::Srt(SrtSocket::Listen { port, .. }) => Some(ResolvedAddr {
                host: own_host.to_string(),
                port: *port,
            }),
            SocketSpec::Srt(SrtSocket::Connect { host, port, .. }) => Some(ResolvedAddr {
                host: host.clone(),
                port: *port,
            }),
            _ => None,
        },
        stats: None,
    };
    planned
        .path
        .hops
        .iter()
        .map(|hop| {
            let own_host = nodes
                .iter()
                .find(|node| node.id == hop.node_id)
                .and_then(|node| {
                    node.topology
                        .attachments
                        .iter()
                        .find_map(|attachment| attachment.listeners.srt.as_ref())
                })
                .map_or("0.0.0.0", |listener| listener.host.as_str());
            HopStatus {
                id: hop.id.clone(),
                node_id: hop.node_id.clone(),
                state: HopState::Provisioned,
                ingress: status(&hop.ingress, own_host),
                merge_ingress: hop
                    .merge_ingress
                    .as_ref()
                    .map(|socket| status(socket, own_host)),
                egresses: hop
                    .egresses
                    .iter()
                    .map(|egress| EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: status(&egress.socket, own_host),
                    })
                    .collect(),
            }
        })
        .collect()
}

fn hop<'a>(planned: &'a PlannedStream, id: &str) -> &'a DesiredHop {
    planned
        .path
        .hops
        .iter()
        .find(|hop| hop.id == id)
        .unwrap_or_else(|| panic!("no hop {id}"))
}

const FIRST: &str = "weave-feed-bridge-studio-0";
const SECOND: &str = "weave-feed-bridge-studio.2-0";

/// A NAT'd source and merging studio, and relays `relay-a` and `relay-c` on
/// `internet-a` and `relay-b` on `internet-b`.
fn three_relays() -> Vec<NodeDescriptor> {
    vec![
        nat_node("source"),
        nat_node("studio-node"),
        relay("relay-a", "internet-a", "10.0.0.2", 100),
        relay("relay-b", "internet-b", "10.1.0.2", 100),
        relay("relay-c", "internet-a", "10.0.0.3", 100),
    ]
}

#[test]
fn a_first_path_that_loses_its_relay_leaves_the_running_second_path_alone() {
    let definition = stream(&[("studio", "studio-node", 2)]);
    let mut nodes = three_relays();
    let steady = plan(&definition, &nodes, &[]);
    assert_eq!(hop(&steady, FIRST).node_id, "relay-a");
    assert_eq!(hop(&steady, SECOND).node_id, "relay-b");

    set_status(&mut nodes, "relay-a", NodeStatus::Offline);
    let moved = plan(&definition, &nodes, &running(&steady, &nodes));
    assert!(moved.single_path.is_empty(), "{:?}", moved.single_path);
    assert_eq!(hop(&moved, FIRST).node_id, "relay-c");
    assert_eq!(hop(&moved, SECOND), hop(&steady, SECOND));
}

#[test]
fn a_first_path_with_no_other_relay_still_takes_the_second_paths_relay() {
    let definition = stream(&[("studio", "studio-node", 2)]);
    let mut nodes = three_relays();
    nodes.retain(|node| node.id != "relay-c");
    let steady = plan(&definition, &nodes, &[]);

    set_status(&mut nodes, "relay-a", NodeStatus::Offline);
    let moved = plan(&definition, &nodes, &running(&steady, &nodes));
    assert_eq!(hop(&moved, FIRST).node_id, "relay-b");
    assert_eq!(
        moved
            .single_path
            .iter()
            .map(|shortfall| shortfall.destination.as_str())
            .collect::<Vec<_>>(),
        ["studio"],
        "the stream stays placed on one path"
    );
}

/// Each relay holds one NAT-to-NAT bridge: its two listeners.
fn full_relays(with_room: bool) -> Vec<NodeDescriptor> {
    let mut nodes = vec![
        nat_node("source"),
        nat_node("studio-node"),
        nat_node("preview-node"),
        relay("relay-a", "internet-a", "10.0.0.2", 2),
        relay("relay-b", "internet-b", "10.1.0.2", 2),
    ];
    if with_room {
        nodes.push(relay("relay-c", "internet-a", "10.0.0.3", 2));
    }
    nodes
}

#[test]
fn a_new_destination_takes_a_free_relay_before_a_running_second_paths() {
    let nodes = full_relays(true);
    let steady = plan(&stream(&[("studio", "studio-node", 2)]), &nodes, &[]);
    assert_eq!(hop(&steady, FIRST).node_id, "relay-a");
    assert_eq!(hop(&steady, SECOND).node_id, "relay-b");

    let grown = stream(&[("preview", "preview-node", 1), ("studio", "studio-node", 2)]);
    let after = plan(&grown, &nodes, &running(&steady, &nodes));
    assert!(after.single_path.is_empty(), "{:?}", after.single_path);
    assert_eq!(
        hop(&after, "weave-feed-bridge-preview-0").node_id,
        "relay-c"
    );
    assert_eq!(hop(&after, FIRST), hop(&steady, FIRST));
    assert_eq!(hop(&after, SECOND), hop(&steady, SECOND));
}

#[test]
fn a_new_destination_with_no_free_relay_takes_the_second_paths_rather_than_fail() {
    let nodes = full_relays(false);
    let steady = plan(&stream(&[("studio", "studio-node", 2)]), &nodes, &[]);

    let grown = stream(&[("preview", "preview-node", 1), ("studio", "studio-node", 2)]);
    let after = plan(&grown, &nodes, &running(&steady, &nodes));
    assert_eq!(hop(&after, FIRST), hop(&steady, FIRST));
    assert_eq!(
        hop(&after, "weave-feed-bridge-preview-0").node_id,
        "relay-b"
    );
    assert_eq!(
        after
            .single_path
            .iter()
            .map(|shortfall| shortfall.destination.as_str())
            .collect::<Vec<_>>(),
        ["studio"]
    );
}
