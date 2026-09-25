use weave_core::{
    DesiredHop, EgressStatus, HopEndpointClass, HopProfile, HopState, HopStatus, LinkCondition,
    NetworkAttachment, NetworkListeners, NodeCapabilities, NodeDescriptor, NodeStatus,
    NodeTopology, ObservedState, Path, PortRange, RoleSet, SocketStatus, SrtEndpoint, SrtListener,
    StreamDefinition, StreamDestination, StreamTransport, Transport, TransportClass,
};

use crate::keys::LinkKeys;
use crate::path::{PortAllocator, derive_path};
use crate::{ReconcileOutcome, reconcile};

fn srt_forward() -> HopProfile {
    let srt = || {
        HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        })
    };
    HopProfile {
        id: "srt-forward".to_string(),
        ingress: srt(),
        egress: srt(),
        max_egresses: None,
        merge: false,
        accepts: None,
    }
}

fn attachment(id: &str, network: &str, listener: Option<(&str, u16, u16)>) -> NetworkAttachment {
    NetworkAttachment {
        id: id.to_string(),
        network: network.to_string(),
        dial: true,
        listeners: NetworkListeners {
            srt: listener.map(|(host, start, end)| SrtListener {
                host: host.to_string(),
                port_range: PortRange { start, end },
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
            hop_profiles: vec![srt_forward()],
        },
        topology: NodeTopology { attachments },
    }
}

/// Dials the internet and listens only on a site network of its own.
fn nat_node(id: &str) -> NodeDescriptor {
    node(
        id,
        vec![
            attachment("out", "internet", None),
            attachment(
                "site",
                &format!("{id}-site"),
                Some(("192.168.0.10", 20_000, 20_999)),
            ),
        ],
    )
}

/// A public relay with `ports` SRT ports.
fn relay(id: &str, host: &str, ports: u16) -> NodeDescriptor {
    node(
        id,
        vec![attachment(
            "wan",
            "internet",
            Some((host, 20_000, 20_000 + ports - 1)),
        )],
    )
}

/// `nodes`, with room on each relay for the two listeners of one NAT-to-NAT
/// bridge.
fn two_port_relays() -> Vec<NodeDescriptor> {
    let mut nodes = nodes();
    for (index, id) in ["relay-a", "relay-b"].into_iter().enumerate() {
        let host = format!("198.51.100.{}", 10 * (index + 1));
        let slot = nodes.iter_mut().find(|node| node.id == id).unwrap();
        *slot = relay(id, &host, 2);
    }
    nodes
}

fn nodes() -> Vec<NodeDescriptor> {
    vec![
        nat_node("source"),
        nat_node("studio-node"),
        nat_node("truck-node"),
        relay("relay-a", "198.51.100.10", 100),
        relay("relay-b", "198.51.100.20", 100),
    ]
}

fn set_status(nodes: &mut [NodeDescriptor], id: &str, status: NodeStatus) {
    nodes.iter_mut().find(|node| node.id == id).unwrap().status = status;
}

fn endpoint(node: &str, via: &[&str]) -> StreamTransport {
    StreamTransport::Srt(SrtEndpoint {
        node: Some(node.to_string()),
        remote: None,
        via: via.iter().map(ToString::to_string).collect(),
        network: None,
        latency: None,
        passphrase: None,
        format: None,
        accepts: None,
    })
}

fn stream(destinations: &[(&str, &str)]) -> StreamDefinition {
    named("feed", destinations)
}

fn named(name: &str, destinations: &[(&str, &str)]) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        enabled: true,
        source: endpoint("source", &[]),
        destinations: destinations
            .iter()
            .map(|(id, node)| StreamDestination {
                id: (*id).to_string(),
                paths: 1,
                endpoint: endpoint(node, &[]),
            })
            .collect(),
    }
}

fn plan(stream: &StreamDefinition, nodes: &[NodeDescriptor], observed: &[HopStatus]) -> Path {
    derive_path(
        stream,
        nodes,
        observed,
        &mut PortAllocator::new(),
        &LinkKeys::for_tests(),
    )
    .expect("derive")
}

/// What each node reports for the hops `path` gave it.
fn reported(path: &Path, state: HopState) -> Vec<HopStatus> {
    let socket = || SocketStatus {
        condition: LinkCondition::Flowing,
        resolved: None,
        stats: None,
    };
    path.hops
        .iter()
        .map(|hop| HopStatus {
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            state,
            ingress: socket(),
            merge_ingress: None,
            egresses: hop
                .egresses
                .iter()
                .map(|egress| EgressStatus {
                    branch_id: egress.branch_id.clone(),
                    status: socket(),
                })
                .collect(),
        })
        .collect()
}

fn hop<'a>(path: &'a Path, id: &str) -> &'a DesiredHop {
    path.hops
        .iter()
        .find(|hop| hop.id == id)
        .unwrap_or_else(|| panic!("no hop {id}"))
}

const STUDIO_BRIDGE: &str = "weave-feed-bridge-studio-0";

#[test]
fn a_bridge_stays_on_its_relay_when_an_earlier_relay_comes_back() {
    let definition = stream(&[("studio", "studio-node")]);
    let mut nodes = nodes();
    set_status(&mut nodes, "relay-a", NodeStatus::Offline);
    let moved = plan(&definition, &nodes, &[]);
    assert_eq!(hop(&moved, STUDIO_BRIDGE).node_id, "relay-b");

    set_status(&mut nodes, "relay-a", NodeStatus::Ready);
    assert_eq!(
        plan(
            &definition,
            &nodes,
            &reported(&moved, HopState::Provisioned)
        ),
        moved,
        "the bridge, its ports and every other hop stay as they are"
    );
    assert_eq!(
        plan(&definition, &nodes, &reported(&moved, HopState::Pending)),
        moved,
        "a relay still provisioning the bridge keeps it"
    );
    assert_eq!(
        hop(&plan(&definition, &nodes, &[]), STUDIO_BRIDGE).node_id,
        "relay-a",
        "with nothing reported, the lowest id wins"
    );
}

#[test]
fn a_bridge_moves_off_a_relay_that_goes_offline() {
    let definition = stream(&[("studio", "studio-node")]);
    let mut nodes = nodes();
    let first = plan(&definition, &nodes, &[]);
    assert_eq!(hop(&first, STUDIO_BRIDGE).node_id, "relay-a");

    set_status(&mut nodes, "relay-a", NodeStatus::Offline);
    let moved = plan(
        &definition,
        &nodes,
        &reported(&first, HopState::Provisioned),
    );
    assert_eq!(hop(&moved, STUDIO_BRIDGE).node_id, "relay-b");
}

#[test]
fn a_relay_that_reports_the_bridge_failed_does_not_keep_it() {
    let definition = stream(&[("studio", "studio-node")]);
    let mut nodes = nodes();
    set_status(&mut nodes, "relay-a", NodeStatus::Offline);
    let moved = plan(&definition, &nodes, &[]);
    set_status(&mut nodes, "relay-a", NodeStatus::Ready);

    let failed = plan(&definition, &nodes, &reported(&moved, HopState::Failed));
    assert_eq!(hop(&failed, STUDIO_BRIDGE).node_id, "relay-a");
}

#[test]
fn a_via_pin_wins_over_the_relay_running_the_bridge() {
    let mut nodes = nodes();
    set_status(&mut nodes, "relay-a", NodeStatus::Offline);
    let moved = plan(&stream(&[("studio", "studio-node")]), &nodes, &[]);
    assert_eq!(hop(&moved, STUDIO_BRIDGE).node_id, "relay-b");
    set_status(&mut nodes, "relay-a", NodeStatus::Ready);

    let mut pinned = stream(&[("studio", "studio-node")]);
    pinned.destinations[0].endpoint = endpoint("studio-node", &["relay-a"]);
    let path = plan(&pinned, &nodes, &reported(&moved, HopState::Provisioned));
    assert_eq!(hop(&path, STUDIO_BRIDGE).node_id, "relay-a");
}

#[test]
fn a_new_destination_takes_the_next_relay_and_leaves_an_existing_bridge_alone() {
    let nodes = two_port_relays();
    let before = plan(&stream(&[("studio", "studio-node")]), &nodes, &[]);
    assert_eq!(hop(&before, STUDIO_BRIDGE).node_id, "relay-a");

    let grown = stream(&[("studio", "studio-node"), ("alpha", "truck-node")]);
    let after = plan(&grown, &nodes, &reported(&before, HopState::Provisioned));
    for id in [STUDIO_BRIDGE, "weave-feed-receiver-studio"] {
        assert_eq!(hop(&after, id), hop(&before, id), "{id} is unchanged");
    }
    assert_eq!(hop(&after, "weave-feed-bridge-alpha-0").node_id, "relay-b");
    let branches: Vec<_> = after.hops[0]
        .egresses
        .iter()
        .map(|egress| egress.branch_id.as_str())
        .collect();
    assert_eq!(branches, ["alpha", "studio"], "egresses stay in id order");
    let order: Vec<_> = after.hops.iter().map(|hop| hop.id.as_str()).collect();
    assert_eq!(
        order,
        [
            "weave-feed-sender",
            "weave-feed-bridge-alpha-0",
            "weave-feed-receiver-alpha",
            STUDIO_BRIDGE,
            "weave-feed-receiver-studio",
        ],
        "hops stay in destination id order"
    );

    let unreported = plan(&grown, &nodes, &[]);
    assert_eq!(
        hop(&unreported, STUDIO_BRIDGE).node_id,
        "relay-b",
        "with nothing reported, the earlier id takes the first relay"
    );
}

fn run(
    streams: Vec<StreamDefinition>,
    nodes: &[NodeDescriptor],
    hops: Vec<HopStatus>,
) -> ReconcileOutcome {
    reconcile(
        streams,
        &ObservedState {
            nodes: nodes.to_vec(),
            endpoints: Vec::new(),
            hops,
        },
        &LinkKeys::for_tests(),
    )
}

#[test]
fn a_new_stream_that_sorts_first_takes_the_next_relay_and_leaves_an_existing_one_alone() {
    let nodes = two_port_relays();
    let existing = named("feed", &[("studio", "studio-node")]);
    let newcomer = named("alpha", &[("truck", "truck-node")]);

    let before = run(vec![existing.clone()], &nodes, Vec::new());
    let running = &before.hops_by_stream["feed"];
    assert_eq!(hop_in(running, STUDIO_BRIDGE).node_id, "relay-a");
    let reports = reported(
        &Path {
            stream: "feed".to_string(),
            enabled: true,
            hops: running.clone(),
        },
        HopState::Provisioned,
    );

    let after = run(vec![newcomer.clone(), existing.clone()], &nodes, reports);
    assert_eq!(
        &after.hops_by_stream["feed"], running,
        "the existing stream's hops, ports and keys are unchanged"
    );
    assert_eq!(
        hop_in(&after.hops_by_stream["alpha"], "weave-alpha-bridge-truck-0").node_id,
        "relay-b"
    );
    let names: Vec<_> = after
        .streams
        .iter()
        .map(|status| status.name.as_str())
        .collect();
    assert_eq!(names, ["alpha", "feed"], "statuses stay in name order");
    let senders: Vec<_> = after.desired_by_node["source"]
        .iter()
        .map(|hop| hop.id.as_str())
        .collect();
    assert_eq!(
        senders,
        ["weave-alpha-sender", "weave-feed-sender"],
        "a node's desired hops stay in stream name order"
    );

    let unreported = run(vec![newcomer, existing], &nodes, Vec::new());
    assert_eq!(
        hop_in(&unreported.hops_by_stream["feed"], STUDIO_BRIDGE).node_id,
        "relay-b",
        "with nothing reported, the earlier name takes the first relay"
    );
}

fn hop_in<'a>(hops: &'a [DesiredHop], id: &str) -> &'a DesiredHop {
    hops.iter()
        .find(|hop| hop.id == id)
        .unwrap_or_else(|| panic!("no hop {id}"))
}
