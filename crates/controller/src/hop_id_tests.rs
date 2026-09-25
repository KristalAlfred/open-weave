use std::collections::BTreeSet;

use weave_core::{
    HopEndpointClass, HopProfile, NetworkAttachment, NetworkListeners, NodeCapabilities,
    NodeDescriptor, NodeStatus, NodeTopology, ObservedState, PathStatus, PortRange, RoleSet,
    SrtEndpoint, SrtListener, StreamConditionReason, StreamConditionType, StreamDefinition,
    StreamDestination, StreamTransport, Transport, TransportClass, validate_stream,
};

use crate::keys::LinkKeys;
use crate::{ReconcileOutcome, reconcile};

fn node(id: &str, network: &str) -> NodeDescriptor {
    let srt = || {
        HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        })
    };
    NodeDescriptor {
        id: id.to_string(),
        endpoint: format!("http://{id}"),
        status: NodeStatus::Ready,
        capabilities: NodeCapabilities {
            adapters: Vec::new(),
            hop_profiles: vec![HopProfile {
                id: "srt-forward".to_string(),
                ingress: srt(),
                egress: srt(),
                max_egresses: None,
                merge: false,
                accepts: None,
            }],
        },
        topology: NodeTopology {
            attachments: vec![NetworkAttachment {
                id: "wan".to_string(),
                network: network.to_string(),
                dial: true,
                listeners: NetworkListeners {
                    srt: Some(SrtListener {
                        host: format!("{id}.example"),
                        port_range: PortRange {
                            start: 20_000,
                            end: 20_999,
                        },
                    }),
                    whip: None,
                    whep: None,
                },
            }],
        },
    }
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

fn stream(name: &str, source: &str, destinations: &[(&str, &str)]) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        enabled: true,
        source: endpoint(source),
        destinations: destinations
            .iter()
            .map(|(id, node)| StreamDestination {
                id: (*id).to_string(),
                paths: 1,
                endpoint: endpoint(node),
            })
            .collect(),
    }
}

/// `edge` and `core` share a network; `site` reaches `core` only through `edge`,
/// so a stream from `site` to `core` bridges on `edge`.
fn nodes() -> Vec<NodeDescriptor> {
    let mut edge = node("edge", "internet");
    edge.topology.attachments.push(NetworkAttachment {
        id: "lan".to_string(),
        ..node("edge", "site-lan").topology.attachments.remove(0)
    });
    vec![node("site", "site-lan"), edge, node("core", "internet")]
}

fn run(streams: Vec<StreamDefinition>) -> ReconcileOutcome {
    run_on(nodes(), streams)
}

fn run_on(nodes: Vec<NodeDescriptor>, streams: Vec<StreamDefinition>) -> ReconcileOutcome {
    for stream in &streams {
        assert!(
            validate_stream(stream).is_empty(),
            "{} is valid",
            stream.name
        );
    }
    reconcile(
        streams,
        &ObservedState {
            nodes,
            endpoints: Vec::new(),
            hops: Vec::new(),
        },
        &LinkKeys::for_tests(),
    )
}

/// Hop ids planned by more than one stream.
fn shared_ids(outcome: &ReconcileOutcome) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut shared = Vec::new();
    for hops in outcome.hops_by_stream.values() {
        for hop in hops {
            if !seen.insert(hop.id.clone()) {
                shared.push(hop.id.clone());
            }
        }
    }
    shared
}

/// Pairs of valid streams whose hop ids, joined with `-`, spell the same id on
/// one node: a sender and a receiver, two receivers, a receiver and a bridge,
/// and two bridges.
fn colliding_pairs() -> Vec<[StreamDefinition; 2]> {
    vec![
        [
            stream("x", "core", &[("a-sender", "edge")]),
            stream("x-receiver-a", "edge", &[("b", "core")]),
        ],
        [
            stream("a", "core", &[("receiver-b", "edge")]),
            stream("a-receiver", "core", &[("b", "edge")]),
        ],
        [
            stream("x", "core", &[("y-bridge-z-0", "edge")]),
            stream("x-receiver-y", "site", &[("z", "core")]),
        ],
        [
            stream("a", "site", &[("bridge-b-0", "core")]),
            stream("a-bridge", "site", &[("b-0", "core")]),
        ],
    ]
}

#[test]
fn two_valid_streams_can_spell_the_same_hop_id_on_one_node() {
    for [first, second] in colliding_pairs() {
        let alone = run(vec![first.clone()]);
        let first_hops = &alone.hops_by_stream[&first.name];
        let other = run(vec![second.clone()]);
        let shared = other.hops_by_stream[&second.name]
            .iter()
            .find(|hop| {
                first_hops
                    .iter()
                    .any(|held| held.id == hop.id && held.node_id == hop.node_id)
            })
            .unwrap_or_else(|| panic!("{} and {} share no hop id", first.name, second.name));
        assert_eq!(shared.node_id, "edge");
        let shared = shared.id.clone();

        let outcome = run(vec![second.clone(), first.clone()]);
        assert_eq!(shared_ids(&outcome), Vec::<String>::new());
        assert_eq!(&outcome.hops_by_stream[&first.name], first_hops);
        assert!(!outcome.hops_by_stream.contains_key(&second.name));

        let refused = outcome
            .streams
            .iter()
            .find(|status| status.name == second.name)
            .unwrap();
        assert_eq!(refused.status, PathStatus::Pending);
        let placement = refused
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::PlacementReady)
            .unwrap();
        assert_eq!(placement.reason, StreamConditionReason::PlacementFailed);
        assert_eq!(
            placement.detail,
            format!(
                "hop id {shared} is already planned for stream {}",
                first.name
            )
        );
    }
}

#[test]
fn a_refused_stream_claims_no_port() {
    let mut nodes = nodes();
    for attachment in &mut nodes[1].topology.attachments {
        attachment.listeners.srt.as_mut().unwrap().port_range = PortRange {
            start: 20_000,
            end: 20_003,
        };
    }
    let [first, second] = colliding_pairs().remove(0);
    let later = stream("y", "core", &[("c", "edge")]);
    let outcome = run_on(nodes, vec![first, second, later.clone()]);
    assert!(
        outcome.hops_by_stream.contains_key(&later.name),
        "four ports on edge hold two receivers"
    );
}
