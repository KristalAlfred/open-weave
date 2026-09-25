use weave_core::{
    DesiredHop, EgressStatus, HopEndpointClass, HopProfile, HopState, HopStatus, LinkCondition,
    NetworkAttachment, NetworkListeners, NodeCapabilities, NodeDescriptor, NodeStatus,
    NodeTopology, ObservedState, Passphrase, PathStatus, PortRange, RoleSet, SocketRole,
    SocketSpec, SocketStatus, SrtEndpoint, SrtListener, SrtSocket, StreamConditionReason,
    StreamConditionStatus, StreamConditionType, StreamDefinition, StreamDestination,
    StreamTransport, Transport, TransportClass,
};

use crate::keys::LinkKeys;
use crate::path::{
    PlacementError, PlannedStream, PortAllocator, SinglePath, derive_path, derive_stream,
    destination_nodes, destination_path_status, shared_hop_id,
};
use crate::reconcile;

fn class(transport: Transport, roles: RoleSet) -> HopEndpointClass {
    HopEndpointClass::Transport(TransportClass { transport, roles })
}

fn profile(id: &str, ingress: Transport, merge: bool) -> HopProfile {
    HopProfile {
        id: id.to_string(),
        ingress: class(ingress, RoleSet::both()),
        egress: class(Transport::Srt, RoleSet::both()),
        max_egresses: None,
        merge,
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
                    end: 20_999,
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
            hop_profiles: vec![profile("srt-forward", Transport::Srt, false)],
        },
        topology: NodeTopology { attachments },
    }
}

fn merging(mut node: NodeDescriptor) -> NodeDescriptor {
    node.capabilities
        .hop_profiles
        .push(profile("srt-merge", Transport::Srt, true));
    node
}

/// Dials out over one uplink per network in `uplinks` and listens only on a
/// site network of its own.
fn nat_node(id: &str, uplinks: &[&str]) -> NodeDescriptor {
    let mut attachments: Vec<_> = uplinks
        .iter()
        .map(|network| attachment(&format!("out-{network}"), network, None))
        .collect();
    attachments.push(attachment(
        "site",
        &format!("{id}-site"),
        Some("192.168.0.10"),
    ));
    node(id, attachments)
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

fn destination(id: &str, node: &str, paths: u8) -> StreamDestination {
    StreamDestination {
        id: id.to_string(),
        paths,
        endpoint: endpoint(node),
    }
}

fn stream(name: &str, destinations: Vec<StreamDestination>) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        enabled: true,
        source: endpoint("source"),
        destinations,
    }
}

/// Source and studio both on `net-a` and `net-b`, each with a listener on both.
fn dual_homed() -> Vec<NodeDescriptor> {
    vec![
        node(
            "source",
            vec![
                attachment("a", "net-a", Some("10.0.0.1")),
                attachment("b", "net-b", Some("10.1.0.1")),
            ],
        ),
        merging(node(
            "studio-node",
            vec![
                attachment("a", "net-a", Some("10.0.0.2")),
                attachment("b", "net-b", Some("10.1.0.2")),
            ],
        )),
        node(
            "preview-node",
            vec![attachment("a", "net-a", Some("10.0.0.3"))],
        ),
    ]
}

/// A NAT'd source and studio with an uplink to `internet-a` and `internet-b`,
/// and one public relay on each.
fn nat_pair() -> Vec<NodeDescriptor> {
    vec![
        nat_node("source", &["internet-a", "internet-b"]),
        merging(nat_node("studio-node", &["internet-a", "internet-b"])),
        node(
            "relay-a",
            vec![attachment("wan", "internet-a", Some("10.0.0.2"))],
        ),
        node(
            "relay-b",
            vec![attachment("wan", "internet-b", Some("10.1.0.2"))],
        ),
    ]
}

fn plan(definition: &StreamDefinition, nodes: &[NodeDescriptor]) -> PlannedStream {
    derive_stream(
        definition,
        nodes,
        &[],
        &mut PortAllocator::new(),
        &LinkKeys::for_tests(),
    )
    .unwrap()
}

fn hop<'a>(planned: &'a PlannedStream, id: &str) -> &'a DesiredHop {
    planned
        .path
        .hops
        .iter()
        .find(|hop| hop.id == id)
        .unwrap_or_else(|| panic!("no hop {id}"))
}

fn connect_host(socket: &SocketSpec) -> &str {
    match socket {
        SocketSpec::Srt(SrtSocket::Connect { host, .. }) => host,
        other => panic!("expected an SRT caller, got {other:?}"),
    }
}

#[test]
fn two_paths_over_two_networks_share_no_attachment_at_either_end() {
    let definition = stream("feed", vec![destination("studio", "studio-node", 2)]);
    let planned = plan(&definition, &dual_homed());
    assert!(planned.single_path.is_empty());

    let sender = hop(&planned, "weave-feed-sender");
    let branches: Vec<_> = sender
        .egresses
        .iter()
        .map(|egress| (egress.branch_id.as_str(), connect_host(&egress.socket)))
        .collect();
    assert_eq!(branches, [("studio", "10.0.0.2"), ("studio.2", "10.1.0.2")]);

    let receiver = hop(&planned, "weave-feed-receiver-studio");
    assert_eq!(receiver.profile_id, "srt-merge");
    let (
        SocketSpec::Srt(SrtSocket::Listen { port: first, .. }),
        Some(SocketSpec::Srt(SrtSocket::Listen { port: second, .. })),
    ) = (&receiver.ingress, &receiver.merge_ingress)
    else {
        panic!("expected two SRT listeners on the receiver");
    };
    assert_ne!(first, second);
    let SocketSpec::Srt(SrtSocket::Connect { port, params, .. }) = &sender.egresses[1].socket
    else {
        panic!("expected an SRT caller");
    };
    assert_eq!(port, second);
    assert_eq!(
        params.passphrase,
        Some(LinkKeys::for_tests().link("weave-feed-receiver-studio.2", ["source", "studio-node"]))
    );
    assert_eq!(planned.path.hops.len(), 2);
}

#[test]
fn a_nat_pair_gets_its_second_path_through_a_second_relay() {
    let definition = stream("feed", vec![destination("studio", "studio-node", 2)]);
    let planned = plan(&definition, &nat_pair());
    assert!(planned.single_path.is_empty());

    assert_eq!(
        hop(&planned, "weave-feed-bridge-studio-0").node_id,
        "relay-a"
    );
    let second = hop(&planned, "weave-feed-bridge-studio.2-0");
    assert_eq!(second.node_id, "relay-b");
    assert_eq!(second.egresses[0].branch_id, "studio.2");
    let receiver = hop(&planned, "weave-feed-receiver-studio");
    assert_eq!(
        connect_host(receiver.merge_ingress.as_ref().unwrap()),
        "10.1.0.2"
    );
    assert_eq!(connect_host(&receiver.ingress), "10.0.0.2");
}

#[test]
fn a_shared_uplink_or_a_single_relay_leaves_one_path() {
    let definition = stream("feed", vec![destination("studio", "studio-node", 2)]);

    let mut one_uplink = nat_pair();
    one_uplink[0] = nat_node("source", &["internet-a"]);
    one_uplink[3]
        .topology
        .attachments
        .push(attachment("wan-a", "internet-a", Some("10.0.0.3")));
    let planned = plan(&definition, &one_uplink);
    assert_eq!(
        planned.single_path,
        [SinglePath {
            destination: "studio".to_string(),
            reason: PlacementError::NoRelayAvailable {
                upstream: "source".to_string(),
                downstream: "studio-node".to_string(),
            },
        }]
    );
    assert!(
        hop(&planned, "weave-feed-receiver-studio")
            .merge_ingress
            .is_none()
    );

    let mut one_relay = nat_pair();
    one_relay.pop();
    let planned = plan(&definition, &one_relay);
    assert_eq!(planned.single_path.len(), 1);
    assert_eq!(planned.path.hops.len(), 3);
    assert!(
        planned.path.hops[0]
            .egresses
            .iter()
            .all(|egress| egress.branch_id == "studio")
    );
}

#[test]
fn a_receiver_that_cannot_merge_gets_one_path() {
    let mut nodes = dual_homed();
    nodes[1].capabilities.hop_profiles.pop();
    let definition = stream("feed", vec![destination("studio", "studio-node", 2)]);
    let planned = plan(&definition, &nodes);
    assert_eq!(
        planned.single_path,
        [SinglePath {
            destination: "studio".to_string(),
            reason: PlacementError::NoMergeProfile {
                node: "studio-node".to_string(),
            },
        }]
    );
    assert_eq!(planned.path.hops[0].egresses.len(), 1);
}

#[test]
fn asking_for_a_second_path_leaves_the_first_as_it_was() {
    for mut nodes in [dual_homed(), nat_pair()] {
        nodes.retain(|node| node.id != "preview-node");
        nodes.push(node(
            "preview-node",
            vec![
                attachment("a", "net-a", Some("10.0.0.3")),
                attachment("ia", "internet-a", Some("10.0.0.3")),
            ],
        ));
        let with = |paths| {
            stream(
                "feed",
                vec![
                    destination("studio", "studio-node", paths),
                    destination("preview", "preview-node", 1),
                ],
            )
        };
        let one = plan(&with(1), &nodes);
        let two = plan(&with(2), &nodes);
        assert!(two.single_path.is_empty());
        assert_eq!(
            two.path.hops[0].egresses.len(),
            one.path.hops[0].egresses.len() + 1
        );
        for before in &one.path.hops {
            let after = hop(&two, &before.id);
            assert_eq!(after.node_id, before.node_id);
            assert_eq!(after.ingress, before.ingress);
            assert_eq!(after.egresses[..before.egresses.len()], before.egresses[..]);
        }
    }
}

#[test]
fn a_second_path_that_fails_claims_no_port() {
    let mut nodes = dual_homed();
    for attachment in &mut nodes[1].topology.attachments {
        attachment.listeners.srt.as_mut().unwrap().port_range = PortRange {
            start: 20_000,
            end: 20_003,
        };
    }
    nodes[1].capabilities.hop_profiles[1].ingress =
        class(Transport::Whip, RoleSet::only(SocketRole::Listen));
    let feed = stream("feed", vec![destination("studio", "studio-node", 2)]);
    let later = stream("later", vec![destination("late", "studio-node", 1)]);

    let mut ports = PortAllocator::new();
    let keys = LinkKeys::for_tests();
    let planned = derive_stream(&feed, &nodes, &[], &mut ports, &keys).unwrap();
    assert!(matches!(
        planned.single_path[0].reason,
        PlacementError::NoHopProfile { .. }
    ));
    derive_path(&later, &nodes, &[], &mut ports, &keys).expect("four ports hold two receivers");
}

fn reported(hops: &[DesiredHop], degraded_branch: Option<&str>) -> Vec<HopStatus> {
    let socket = |condition| SocketStatus {
        condition,
        resolved: None,
        stats: None,
    };
    hops.iter()
        .map(|hop| HopStatus {
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            state: HopState::Provisioned,
            ingress: socket(LinkCondition::Flowing),
            merge_ingress: hop
                .merge_ingress
                .as_ref()
                .map(|_| socket(LinkCondition::Flowing)),
            egresses: hop
                .egresses
                .iter()
                .map(|egress| EgressStatus {
                    branch_id: egress.branch_id.clone(),
                    status: socket(if Some(egress.branch_id.as_str()) == degraded_branch {
                        LinkCondition::Connecting
                    } else {
                        LinkCondition::Flowing
                    }),
                })
                .collect(),
        })
        .collect()
}

#[test]
fn a_destination_rolls_up_both_of_its_paths_and_no_other() {
    let mut nodes = dual_homed();
    nodes.push(node(
        "studio-2-node",
        vec![attachment("a", "net-a", Some("10.0.0.4"))],
    ));
    let definition = stream(
        "feed",
        vec![
            destination("studio", "studio-node", 2),
            destination("studio-2", "studio-2-node", 1),
        ],
    );
    let path = plan(&definition, &nodes).path;

    let flowing = reported(&path.hops, None);
    assert_eq!(
        destination_path_status(&path, "studio", &flowing),
        PathStatus::Flowing
    );
    let second_down = reported(&path.hops, Some("studio.2"));
    assert_eq!(
        destination_path_status(&path, "studio", &second_down),
        PathStatus::Degraded
    );
    assert_eq!(
        destination_path_status(&path, "studio-2", &second_down),
        PathStatus::Flowing
    );

    let mut no_merge_report = flowing;
    for status in &mut no_merge_report {
        status.merge_ingress = None;
    }
    assert_eq!(
        destination_path_status(&path, "studio", &no_merge_report),
        PathStatus::Pending
    );
    assert_eq!(
        destination_nodes(&path, "studio"),
        ["source", "studio-node"]
    );
    assert_eq!(
        destination_nodes(&path, "studio-2"),
        ["source", "studio-2-node"]
    );
}

#[test]
fn a_destination_with_one_path_of_two_says_so_in_its_placement_condition() {
    let mut nodes = dual_homed();
    nodes[1].capabilities.hop_profiles.pop();
    let definition = stream(
        "feed",
        vec![
            destination("studio", "studio-node", 2),
            destination("preview", "preview-node", 1),
        ],
    );
    let outcome = reconcile(
        vec![definition],
        &ObservedState {
            nodes,
            endpoints: Vec::new(),
            hops: Vec::new(),
        },
        &LinkKeys::for_tests(),
    );
    let status = &outcome.streams[0];
    let placement = |conditions: &[weave_core::StreamCondition]| {
        conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::PlacementReady)
            .cloned()
            .unwrap()
    };
    let detail = "destination studio has one path of two: node studio-node has no hop profile that merges a second path";

    let stream_placement = placement(&status.conditions);
    assert_eq!(stream_placement.status, StreamConditionStatus::True);
    assert_eq!(stream_placement.reason, StreamConditionReason::SinglePath);
    assert_eq!(stream_placement.detail, detail);

    let studio = status
        .destinations
        .iter()
        .find(|destination| destination.id == "studio")
        .unwrap();
    let studio_placement = placement(&studio.conditions);
    assert_eq!(studio_placement.reason, StreamConditionReason::SinglePath);
    assert_eq!(studio_placement.detail, detail);

    let preview = status
        .destinations
        .iter()
        .find(|destination| destination.id == "preview")
        .unwrap();
    assert_eq!(
        placement(&preview.conditions).reason,
        StreamConditionReason::Placed
    );
}

#[tokio::test]
async fn a_plan_with_one_path_of_two_gives_the_reason() {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::Json;
    use axum::extract::State;
    use http_body_util::BodyExt;
    use weave_core::{NodeRegistration, PROTOCOL_VERSION, PlanStatus, StreamPlan};

    use crate::store::MemStore;
    use crate::{AppState, plan_stream};

    let state = AppState::hydrate(
        Arc::new(MemStore::new()),
        Duration::from_secs(15),
        Duration::from_secs(300),
        None,
        LinkKeys::for_tests(),
    )
    .await
    .unwrap();
    let mut nodes = dual_homed();
    nodes[1].capabilities.hop_profiles.pop();
    for node in nodes {
        state.nodes.write().await.insert(
            node.id.clone(),
            NodeRegistration {
                protocol_version: PROTOCOL_VERSION,
                node,
                endpoints: Vec::new(),
                hop_status: Vec::new(),
            },
        );
    }
    let definition = stream("feed", vec![destination("studio", "studio-node", 2)]);
    let response = plan_stream(State(state), Ok(Json(definition))).await;
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let plan: StreamPlan = serde_json::from_slice(&body).unwrap();
    assert_eq!(plan.status, PlanStatus::Placed);
    assert_eq!(
        plan.reason.as_deref(),
        Some(
            "destination studio has one path of two: node studio-node has no hop profile that merges a second path"
        )
    );
}

/// The derived keys a hop's SRT sockets carry. Terminal sockets in these
/// fixtures set no manifest passphrase, so every key here is a link key.
fn link_keys(hop: &DesiredHop) -> Vec<Passphrase> {
    std::iter::once(&hop.ingress)
        .chain(hop.merge_ingress.as_ref())
        .chain(hop.egresses.iter().map(|egress| &egress.socket))
        .filter_map(|socket| match socket {
            SocketSpec::Srt(
                SrtSocket::Listen { params, .. } | SrtSocket::Connect { params, .. },
            ) => params.passphrase.clone(),
            _ => None,
        })
        .collect()
}

/// Stream `x` to `a-receiver-b` and stream `x-receiver-a` to `b` spell the same
/// receiver id, so their merge links are both fed by
/// `weave-x-receiver-a-receiver-b.2`.
#[test]
fn two_streams_that_spell_one_merge_link_id_never_both_carry_its_key() {
    let nodes = dual_homed();
    let first = stream("x", vec![destination("a-receiver-b", "studio-node", 2)]);
    let second = stream("x-receiver-a", vec![destination("b", "studio-node", 2)]);
    let merge_key = |definition: &StreamDefinition| {
        let planned = plan(definition, &nodes);
        assert!(planned.single_path.is_empty());
        let receiver = hop(&planned, "weave-x-receiver-a-receiver-b");
        let merge = receiver.merge_ingress.clone().expect("a merge ingress");
        link_keys(&DesiredHop {
            egresses: Vec::new(),
            merge_ingress: None,
            ingress: merge,
            ..receiver.clone()
        })
    };
    assert_eq!(
        merge_key(&first),
        merge_key(&second),
        "planned apart, both merge links derive one key"
    );
    assert_eq!(
        shared_hop_id(&first, &second).as_deref(),
        Some("weave-x-receiver-a-receiver-b"),
        "the apply check refuses the pair by its receivers"
    );

    let feed = stream("feed", vec![destination("studio", "studio-node", 2)]);
    let outcome = reconcile(
        vec![second, first, feed],
        &ObservedState {
            nodes,
            endpoints: Vec::new(),
            hops: Vec::new(),
        },
        &LinkKeys::for_tests(),
    );
    let mut ends = std::collections::BTreeMap::<Passphrase, usize>::new();
    for hop in outcome.hops_by_stream.values().flatten() {
        for key in link_keys(hop) {
            *ends.entry(key).or_default() += 1;
        }
    }
    assert_eq!(ends.len(), 4, "two links for each placed stream");
    assert!(
        ends.values().all(|count| *count == 2),
        "each derived key sits on the two ends of one link"
    );
    assert!(outcome.hops_by_stream.contains_key("x"));
    assert!(outcome.hops_by_stream.contains_key("feed"));
    assert!(
        !outcome.hops_by_stream.contains_key("x-receiver-a"),
        "the plan-time check leaves the later stream unplaced"
    );
}
