use std::collections::BTreeSet;

use weave_core::{
    DesiredHop, HopEndpointClass, HopProfile, NetworkAttachment, NetworkListeners,
    NodeCapabilities, NodeDescriptor, NodeStatus, NodeTopology, ObservedState, PathStatus,
    PortRange, RistListener, RistSocket, RoleSet, SocketRole, SocketSpec, SrtEndpoint, SrtListener,
    SrtSocket, StreamConditionReason, StreamConditionStatus, StreamConditionType, StreamDefinition,
    StreamDestination, StreamTransport, Transport, TransportClass,
};

use crate::keys::LinkKeys;
use crate::path::{PlacementError, PortAllocator, derive_path};
use crate::reconcile;

fn class(transport: Transport, roles: RoleSet) -> HopEndpointClass {
    HopEndpointClass::Transport(TransportClass { transport, roles })
}

fn profile(id: &str, ingress: HopEndpointClass, egress: HopEndpointClass) -> HopProfile {
    HopProfile {
        id: id.to_string(),
        ingress,
        egress,
        max_egresses: None,
        merge: false,
        accepts: None,
    }
}

/// A node offering what the Strom adapter offers for SRT and RIST.
fn node(id: &str, attachments: Vec<NetworkAttachment>) -> NodeDescriptor {
    let srt = || class(Transport::Srt, RoleSet::both());
    NodeDescriptor {
        id: id.to_string(),
        endpoint: format!("http://{id}"),
        status: NodeStatus::Ready,
        capabilities: NodeCapabilities {
            adapters: Vec::new(),
            hop_profiles: vec![
                profile("srt-forward", srt(), srt()),
                profile(
                    "srt-to-rist",
                    srt(),
                    class(Transport::Rist, RoleSet::only(SocketRole::Connect)),
                ),
                profile(
                    "rist-to-srt",
                    class(Transport::Rist, RoleSet::only(SocketRole::Listen)),
                    srt(),
                ),
            ],
        },
        topology: NodeTopology { attachments },
    }
}

fn range(start: u16, end: u16) -> PortRange {
    PortRange { start, end }
}

fn attachment(
    id: &str,
    network: &str,
    dial: bool,
    srt: Option<(&str, PortRange)>,
    rist: Option<(&str, PortRange)>,
) -> NetworkAttachment {
    NetworkAttachment {
        id: id.to_string(),
        network: network.to_string(),
        dial,
        listeners: NetworkListeners {
            srt: srt.map(|(host, port_range)| SrtListener {
                host: host.to_string(),
                port_range,
            }),
            whip: None,
            whep: None,
            rist: rist.map(|(host, port_range)| RistListener {
                host: host.to_string(),
                port_range,
            }),
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

/// A stream that allows cleartext links, so the planner may use RIST.
fn stream(destinations: &[&str]) -> StreamDefinition {
    StreamDefinition {
        name: "feed".to_string(),
        enabled: true,
        allow_cleartext_links: true,
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

fn plan(
    definition: &StreamDefinition,
    nodes: &[NodeDescriptor],
) -> Result<Vec<DesiredHop>, PlacementError> {
    derive_path(
        definition,
        nodes,
        &[],
        &mut PortAllocator::new(),
        &LinkKeys::for_tests(),
    )
    .map(|path| path.hops)
}

fn source() -> NodeDescriptor {
    node(
        "source",
        vec![attachment(
            "wan",
            "internet",
            true,
            Some(("192.0.2.1", range(20_000, 20_100))),
            None,
        )],
    )
}

/// A studio that takes RIST from the internet, never dials out, and serves SRT
/// only on its own LAN.
fn rist_only_studio(rist: PortRange, srt: PortRange) -> NodeDescriptor {
    node(
        "studio",
        vec![
            attachment("lan", "studio-lan", false, Some(("10.0.0.5", srt)), None),
            attachment("wan", "internet", false, None, Some(("192.0.2.2", rist))),
        ],
    )
}

#[test]
fn a_link_srt_can_carry_stays_srt() {
    let studio = node(
        "studio",
        vec![attachment(
            "wan",
            "internet",
            true,
            Some(("192.0.2.2", range(20_000, 20_100))),
            Some(("192.0.2.2", range(21_000, 21_100))),
        )],
    );
    let hops = plan(&stream(&["studio"]), &[source(), studio]).unwrap();
    assert_eq!(hops[0].profile_id, "srt-forward");
    assert!(matches!(
        hops[1].ingress,
        SocketSpec::Srt(SrtSocket::Listen { .. })
    ));
}

#[test]
fn a_link_only_rist_can_carry_goes_over_rist() {
    let studio = rist_only_studio(range(21_000, 21_100), range(20_000, 20_100));
    let hops = plan(&stream(&["studio"]), &[source(), studio]).unwrap();

    let sender = &hops[0];
    assert_eq!(sender.profile_id, "srt-to-rist");
    let SocketSpec::Rist(RistSocket::Connect { host, port }) = &sender.egresses[0].socket else {
        panic!("the sender pushes RIST: {:?}", sender.egresses[0].socket);
    };
    assert_eq!(host, "192.0.2.2");

    let receiver = &hops[1];
    assert_eq!(receiver.profile_id, "rist-to-srt");
    assert_eq!(
        receiver.ingress,
        SocketSpec::Rist(RistSocket::Listen { port: *port })
    );
    assert_eq!(port % 2, 0);
    assert!(matches!(
        receiver.egresses[0].socket,
        SocketSpec::Srt(SrtSocket::Listen { .. })
    ));
}

#[test]
fn the_sender_never_listens_for_rist() {
    let source = node(
        "source",
        vec![
            attachment(
                "lan",
                "producer-lan",
                false,
                Some(("10.0.1.5", range(20_000, 20_100))),
                None,
            ),
            attachment(
                "wan",
                "internet",
                true,
                None,
                Some(("192.0.2.1", range(21_000, 21_100))),
            ),
        ],
    );
    let studio = node(
        "studio",
        vec![
            attachment(
                "lan",
                "studio-lan",
                false,
                Some(("10.0.0.5", range(20_000, 20_100))),
                None,
            ),
            attachment("wan", "internet", true, None, None),
        ],
    );
    let nodes = [source, studio];
    let result = plan(&stream(&["studio"]), &nodes);
    assert!(
        matches!(result, Err(PlacementError::NoRelayAvailable { .. })),
        "{result:?}"
    );
    let result = plan(&keyed_only(stream(&["studio"])), &nodes);
    assert!(
        matches!(result, Err(PlacementError::NoRelayAvailable { .. })),
        "a stream RIST cannot place either keeps its own reason: {result:?}"
    );
}

fn keyed_only(mut definition: StreamDefinition) -> StreamDefinition {
    definition.allow_cleartext_links = false;
    definition
}

#[test]
fn a_link_only_rist_can_carry_is_not_planned_unless_the_stream_allows_cleartext() {
    let nodes = [
        source(),
        rist_only_studio(range(21_000, 21_100), range(20_000, 20_100)),
    ];
    assert_eq!(
        plan(&keyed_only(stream(&["studio"])), &nodes),
        Err(PlacementError::CleartextLink {
            node: "studio".to_string()
        })
    );

    let observed = ObservedState {
        nodes: nodes.to_vec(),
        endpoints: Vec::new(),
        hops: Vec::new(),
    };
    let outcome = reconcile(
        vec![keyed_only(stream(&["studio"]))],
        &observed,
        &LinkKeys::for_tests(),
    );
    let status = &outcome.streams[0];
    assert_eq!(status.status, PathStatus::Pending);
    let placement = status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == StreamConditionType::PlacementReady)
        .unwrap();
    assert_eq!(placement.status, StreamConditionStatus::False);
    assert_eq!(placement.reason, StreamConditionReason::CleartextNotAllowed);
    assert!(
        placement.detail.contains("allow_cleartext_links"),
        "{}",
        placement.detail
    );
    assert!(outcome.hops_by_stream.get("feed").is_none_or(Vec::is_empty));
}

#[test]
fn allowing_cleartext_leaves_srt_links_keyed() {
    let studio = node(
        "studio",
        vec![attachment(
            "wan",
            "internet",
            true,
            Some(("192.0.2.2", range(20_000, 20_100))),
            Some(("192.0.2.2", range(21_000, 21_100))),
        )],
    );
    let nodes = [source(), studio];
    for definition in [stream(&["studio"]), keyed_only(stream(&["studio"]))] {
        let hops = plan(&definition, &nodes).unwrap();
        let SocketSpec::Srt(socket) = &hops[1].ingress else {
            panic!("the link stays SRT: {:?}", hops[1].ingress);
        };
        assert!(socket.params().passphrase.is_some());
        assert_eq!(socket.params().pbkeylen, Some(32));
    }
}

#[test]
fn rist_takes_even_port_pairs_that_srt_never_shares() {
    let studio = rist_only_studio(range(20_000, 20_009), range(20_000, 20_009));
    let hops = plan(&stream(&["preview", "program"]), &[source(), studio]).unwrap();

    let mut taken = BTreeSet::new();
    for hop in hops.iter().filter(|hop| hop.node_id == "studio") {
        let SocketSpec::Rist(RistSocket::Listen { port }) = hop.ingress else {
            panic!("a studio receiver listens for RIST: {:?}", hop.ingress);
        };
        assert_eq!(port % 2, 0);
        assert!(taken.insert(port) && taken.insert(port + 1));
        let SocketSpec::Srt(SrtSocket::Listen { port, .. }) = hop.egresses[0].socket else {
            panic!("the consumer socket is SRT");
        };
        assert!(taken.insert(port), "SRT port {port} is already taken");
    }
    assert_eq!(taken.len(), 6);
}

#[test]
fn rist_and_srt_can_fill_a_range_they_share() {
    let shared = range(20_000, 20_008);
    let studio = rist_only_studio(shared, shared);
    let hops = plan(&stream(&["a", "b", "c"]), &[source(), studio]).unwrap();

    let mut taken = BTreeSet::new();
    for hop in hops.iter().filter(|hop| hop.node_id == "studio") {
        let SocketSpec::Rist(RistSocket::Listen { port }) = hop.ingress else {
            panic!("a studio receiver listens for RIST: {:?}", hop.ingress);
        };
        assert!(taken.insert(port) && taken.insert(port + 1));
        let SocketSpec::Srt(SrtSocket::Listen { port, .. }) = hop.egresses[0].socket else {
            panic!("the consumer socket is SRT");
        };
        assert!(taken.insert(port), "SRT port {port} is already taken");
    }
    assert_eq!(taken, (20_000..=20_008).collect());
}
