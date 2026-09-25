use std::collections::BTreeMap;

use weave_core::{
    DesiredHop, EgressStatus, HopEndpointClass, HopProfile, HopState, HopStatus, LinkCondition,
    NetworkAttachment, NetworkListeners, NodeCapabilities, NodeDescriptor, NodeStatus,
    NodeTopology, ObservedState, PortRange, ResolvedAddr, RoleSet, SocketSpec, SocketStatus,
    SrtEndpoint, SrtListener, SrtSocket, StreamDefinition, StreamDestination, StreamTransport,
    Transport, TransportClass,
};

use crate::keys::LinkKeys;
use crate::reconcile;

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

fn attachment(id: &str, network: &str, host: Option<&str>) -> NetworkAttachment {
    NetworkAttachment {
        id: id.to_string(),
        network: network.to_string(),
        dial: true,
        listeners: NetworkListeners {
            srt: host.map(|host| SrtListener {
                host: host.to_string(),
                port_range: PortRange {
                    start: 20_000,
                    end: 20_099,
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
            hop_profiles: vec![srt_forward()],
        },
        topology: NodeTopology { attachments },
    }
}

fn public(id: &str, host: &str) -> NodeDescriptor {
    node(id, vec![attachment("wan", "internet", Some(host))])
}

fn natted(id: &str) -> NodeDescriptor {
    node(
        id,
        vec![
            attachment("out", "internet", None),
            attachment("site", &format!("{id}-site"), Some("192.168.0.10")),
        ],
    )
}

fn public_pair() -> Vec<NodeDescriptor> {
    vec![
        public("source", "198.51.100.1"),
        public("studio", "198.51.100.2"),
    ]
}

fn relayed() -> Vec<NodeDescriptor> {
    vec![
        natted("source"),
        natted("studio"),
        public("relay", "198.51.100.9"),
    ]
}

fn endpoint(node: &str) -> StreamTransport {
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
}

fn stream(name: &str, destinations: &[&str]) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        enabled: true,
        allow_cleartext_links: false,
        source: endpoint("source"),
        destinations: destinations
            .iter()
            .map(|id| StreamDestination {
                id: (*id).to_string(),
                paths: 1,
                endpoint: endpoint("studio"),
            })
            .collect(),
    }
}

type Hops = BTreeMap<String, Vec<DesiredHop>>;

fn run(streams: &[StreamDefinition], nodes: &[NodeDescriptor], hops: Vec<HopStatus>) -> Hops {
    reconcile(
        streams.to_vec(),
        &ObservedState {
            nodes: nodes.to_vec(),
            endpoints: Vec::new(),
            hops,
        },
        &LinkKeys::for_tests(),
    )
    .hops_by_stream
}

/// What each node reports for the hops it was given, the way the Strom adapter
/// resolves them: a listener at the node's SRT listener host, a caller at the
/// address it dials. Hops named in `failed` report `failed`.
fn reported(planned: &Hops, nodes: &[NodeDescriptor], failed: &[&str]) -> Vec<HopStatus> {
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
        .values()
        .flatten()
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
                state: if failed.contains(&hop.id.as_str()) {
                    HopState::Failed
                } else {
                    HopState::Provisioned
                },
                ingress: status(&hop.ingress, own_host),
                merge_ingress: None,
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

/// The hops of `planned` that carry only `destination` of stream `feed`.
fn branch(planned: &Hops, destination: &str) -> Vec<DesiredHop> {
    planned["feed"]
        .iter()
        .filter(|hop| {
            hop.id == format!("weave-feed-receiver-{destination}")
                || hop
                    .id
                    .starts_with(&format!("weave-feed-bridge-{destination}-"))
        })
        .cloned()
        .collect()
}

/// Adds a new destination `a{n}` next to a running `b`, lets it run, then
/// fails its receiver. Returns the candidates for which anything already
/// running moved.
fn moved_when_a_destination_joins(nodes: &[NodeDescriptor]) -> Vec<String> {
    let steady = run(&[stream("feed", &["b"])], nodes, Vec::new());
    let mut moved = Vec::new();
    for n in 0..200 {
        let newcomer = format!("a{n}");
        let grown = [stream("feed", &[&newcomer, "b"])];
        let joined = run(&grown, nodes, reported(&steady, nodes, &[]));
        let running = run(&grown, nodes, reported(&joined, nodes, &[]));
        let receiver = format!("weave-feed-receiver-{newcomer}");
        let failing = run(&grown, nodes, reported(&joined, nodes, &[&receiver]));
        if branch(&joined, "b") != branch(&steady, "b")
            || running != joined
            || branch(&failing, "b") != branch(&joined, "b")
        {
            moved.push(newcomer);
        }
    }
    moved
}

#[test]
fn running_destinations_keep_their_ports_when_another_joins_runs_or_fails() {
    assert_eq!(
        moved_when_a_destination_joins(&public_pair()),
        Vec::<String>::new()
    );
}

#[test]
fn running_bridges_keep_their_relay_ports_when_another_joins_runs_or_fails() {
    assert_eq!(
        moved_when_a_destination_joins(&relayed()),
        Vec::<String>::new()
    );
}

#[test]
fn running_streams_keep_their_ports_when_another_stream_joins_runs_or_fails() {
    let nodes = public_pair();
    let existing = stream("feed", &["b"]);
    let steady = run(std::slice::from_ref(&existing), &nodes, Vec::new());
    let mut moved = Vec::new();
    for n in 0..200 {
        let newcomer = stream(&format!("a{n}"), &["x"]);
        let both = [newcomer.clone(), existing.clone()];
        let joined = run(&both, &nodes, reported(&steady, &nodes, &[]));
        let running = run(&both, &nodes, reported(&joined, &nodes, &[]));
        let receiver = format!("weave-{}-receiver-x", newcomer.name);
        let failing = run(&both, &nodes, reported(&joined, &nodes, &[&receiver]));
        if joined["feed"] != steady["feed"]
            || running != joined
            || failing["feed"] != joined["feed"]
        {
            moved.push(newcomer.name);
        }
    }
    assert_eq!(moved, Vec::<String>::new());
}

fn streams(count: usize) -> Vec<StreamDefinition> {
    (0..count)
        .map(|index| stream(&format!("feed-{index}"), &["studio"]))
        .collect()
}

/// Nodes with room for `streams` direct streams: a sender ingress each on
/// `source`, and a receiver ingress and consumer each on `studio`.
fn roomy_pair(streams: u16) -> Vec<NodeDescriptor> {
    let mut nodes = public_pair();
    for node in &mut nodes {
        node.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .unwrap()
            .port_range = PortRange {
            start: 20_000,
            end: 20_000 + 3 * streams,
        };
    }
    nodes
}

/// Times a reconcile of `count` running streams against their own reports.
fn time_a_reconcile_with_reports(count: usize) -> std::time::Duration {
    let nodes = roomy_pair(u16::try_from(count).unwrap());
    let definitions = streams(count);
    let first = run(&definitions, &nodes, Vec::new());
    assert_eq!(first.len(), count, "every stream is placed");
    let reports = reported(&first, &nodes, &[]);
    let started = std::time::Instant::now();
    let again = run(&definitions, &nodes, reports);
    let elapsed = started.elapsed();
    assert_eq!(again, first);
    eprintln!(
        "{count} streams, {} reports: reconcile {elapsed:?}",
        first.values().map(Vec::len).sum::<usize>()
    );
    elapsed
}

#[test]
#[ignore = "timing; run with `cargo test -p weave-controller reconcile_with_reports -- --ignored --nocapture --test-threads=1`"]
fn a_reconcile_with_reports_at_one_and_two_thousand_streams() {
    time_a_reconcile_with_reports(1000);
    time_a_reconcile_with_reports(2000);
}
