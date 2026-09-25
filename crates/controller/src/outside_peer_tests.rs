use weave_core::{
    AudioCodec, AudioConstraint, AudioFormat, ChromaSubsampling, Container, FormatConstraint,
    Framerate, HopEndpointClass, HopProfile, MediaFormat, NetworkAttachment, NetworkListeners,
    NodeCapabilities, NodeDescriptor, NodeStatus, NodeTopology, ObservedState, PathStatus,
    PortRange, RoleSet, SignallingEndpoint, SignallingListener, SocketRole, SocketSpec,
    SrtEndpoint, SrtListener, StreamConditionReason, StreamConditionStatus, StreamConditionType,
    StreamDefinition, StreamDestination, StreamStatus, StreamTransport, Track, Transport,
    TransportClass, VideoCodec, VideoConstraint, VideoFormat, validate_stream,
};

use crate::keys::LinkKeys;
use crate::path::{PlacementError, PortAllocator, derive_path, stream_endpoints};
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

fn h264_and_opus() -> FormatConstraint {
    FormatConstraint {
        container: None,
        video: Some(VideoConstraint {
            codec: Some(vec![VideoCodec::H264]),
            ..VideoConstraint::default()
        }),
        audio: Some(AudioConstraint {
            codec: Some(vec![AudioCodec::Opus]),
            ..AudioConstraint::default()
        }),
    }
}

fn listeners(host: &str, signalling: &str) -> NetworkListeners {
    NetworkListeners {
        srt: Some(SrtListener {
            host: host.to_string(),
            port_range: PortRange {
                start: 20_000,
                end: 20_999,
            },
        }),
        whip: Some(SignallingListener {
            base_url: format!("{signalling}/whip"),
        }),
        whep: Some(SignallingListener {
            base_url: format!("{signalling}/whep"),
        }),
        rist: None,
    }
}

/// A node advertising Strom's three profiles, reachable on `internet` and, for
/// peers on a venue LAN, on `venue-lan` at `192.168.1.10:{lan_port}`.
fn strom(id: &str, host: &str, lan_port: u16) -> NodeDescriptor {
    NodeDescriptor {
        id: id.to_string(),
        endpoint: format!("http://{id}"),
        status: NodeStatus::Ready,
        capabilities: NodeCapabilities {
            adapters: Vec::new(),
            hop_profiles: vec![
                profile(
                    "srt-forward",
                    class(Transport::Srt, RoleSet::both()),
                    class(Transport::Srt, RoleSet::both()),
                ),
                HopProfile {
                    accepts: Some(h264_and_opus()),
                    ..profile(
                        "whip-to-srt",
                        class(Transport::Whip, RoleSet::only(SocketRole::Listen)),
                        class(Transport::Srt, RoleSet::both()),
                    )
                },
                profile(
                    "srt-to-whep",
                    class(Transport::Srt, RoleSet::both()),
                    class(Transport::Whep, RoleSet::only(SocketRole::Listen)),
                ),
            ],
        },
        topology: NodeTopology {
            attachments: vec![
                NetworkAttachment {
                    id: "a-routed".to_string(),
                    network: "internet".to_string(),
                    dial: true,
                    listeners: listeners(host, &format!("http://{host}:8080")),
                },
                NetworkAttachment {
                    id: "venue".to_string(),
                    network: "venue-lan".to_string(),
                    dial: true,
                    listeners: listeners(host, &format!("http://192.168.1.10:{lan_port}")),
                },
            ],
        },
    }
}

fn nodes() -> Vec<NodeDescriptor> {
    vec![
        strom("strom-node-1", "10.97.26.10", 28080),
        strom("strom-node-2", "10.97.27.10", 28081),
    ]
}

fn signalling(node: &str, network: Option<&str>) -> SignallingEndpoint {
    SignallingEndpoint {
        node: node.to_string(),
        network: network.map(str::to_string),
        format: None,
        accepts: None,
    }
}

fn srt(node: &str) -> StreamTransport {
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

fn stream(source: StreamTransport, destinations: Vec<(&str, StreamTransport)>) -> StreamDefinition {
    let stream = StreamDefinition {
        name: "feed".to_string(),
        enabled: true,
        source,
        destinations: destinations
            .into_iter()
            .map(|(id, endpoint)| StreamDestination {
                id: id.to_string(),
                paths: 1,
                endpoint,
            })
            .collect(),
    };
    assert_eq!(validate_stream(&stream), []);
    stream
}

fn listen_url(socket: &SocketSpec) -> &str {
    match socket {
        SocketSpec::Whip(listener) | SocketSpec::Whep(listener)
            if listener.role == SocketRole::Listen =>
        {
            &listener.url
        }
        other => panic!("expected a WHIP or WHEP listener, got {other:?}"),
    }
}

fn plan(definition: &StreamDefinition) -> weave_core::Path {
    derive_path(
        definition,
        &nodes(),
        &[],
        &mut PortAllocator::new(),
        &LinkKeys::for_tests(),
    )
    .unwrap()
}

#[test]
fn an_outside_whip_sender_pushes_to_a_gateway_on_the_named_node() {
    let definition = stream(
        StreamTransport::Whip(signalling("strom-node-1", None)),
        vec![("studio", srt("strom-node-2"))],
    );
    let path = plan(&definition);
    let sender = &path.hops[0];
    assert_eq!(sender.node_id, "strom-node-1");
    assert_eq!(sender.profile_id, "whip-to-srt");
    assert!(matches!(sender.ingress, SocketSpec::Whip(_)));
    assert_eq!(
        listen_url(&sender.ingress),
        "http://10.97.26.10:8080/whip/weave-feed-sender"
    );

    let endpoints = stream_endpoints(&definition, &path, &nodes()).unwrap();
    let ingress = endpoints.ingress.unwrap();
    assert_eq!(ingress.node, "strom-node-1");
    assert_eq!(
        ingress.url,
        "http://10.97.26.10:8080/whip/weave-feed-sender"
    );
    assert_eq!(ingress.host, None);
    assert_eq!(ingress.port, None);
    let json = serde_json::to_value(&ingress).unwrap();
    assert!(json.get("host").is_none() && json.get("port").is_none());
    assert!(
        endpoints.destinations[0]
            .endpoint
            .as_ref()
            .unwrap()
            .url
            .starts_with("srt://")
    );
}

#[test]
fn an_outside_whep_player_pulls_from_a_receiver_on_the_named_node() {
    let definition = stream(
        srt("strom-node-1"),
        vec![(
            "monitor",
            StreamTransport::Whep(signalling("strom-node-2", None)),
        )],
    );
    let path = plan(&definition);
    let receiver = &path.hops[1];
    assert_eq!(receiver.node_id, "strom-node-2");
    assert_eq!(receiver.profile_id, "srt-to-whep");
    assert!(matches!(receiver.egresses[0].socket, SocketSpec::Whep(_)));
    let url = "http://10.97.27.10:8080/whep/weave-feed-receiver-monitor";
    assert_eq!(listen_url(&receiver.egresses[0].socket), url);

    let endpoints = stream_endpoints(&definition, &path, &nodes()).unwrap();
    let output = endpoints.destinations[0].endpoint.as_ref().unwrap();
    assert_eq!(output.node, "strom-node-2");
    assert_eq!(output.url, url);
    assert_eq!((output.host.as_ref(), output.port), (None, None));
}

#[test]
fn whip_in_and_whep_out_on_one_node_is_two_hops_joined_over_srt() {
    let definition = stream(
        StreamTransport::Whip(signalling("strom-node-1", None)),
        vec![(
            "monitor",
            StreamTransport::Whep(signalling("strom-node-1", None)),
        )],
    );
    let path = plan(&definition);
    let profiles: Vec<_> = path
        .hops
        .iter()
        .map(|hop| (hop.node_id.as_str(), hop.profile_id.as_str()))
        .collect();
    assert_eq!(
        profiles,
        [
            ("strom-node-1", "whip-to-srt"),
            ("strom-node-1", "srt-to-whep")
        ]
    );
    assert!(matches!(
        path.hops[0].egresses[0].socket,
        SocketSpec::Srt(_)
    ));
    assert!(matches!(path.hops[1].ingress, SocketSpec::Srt(_)));
}

#[test]
fn a_network_pin_picks_the_signalling_base_on_that_network() {
    let definition = stream(
        StreamTransport::Whip(signalling("strom-node-1", Some("venue-lan"))),
        vec![(
            "monitor",
            StreamTransport::Whep(signalling("strom-node-2", Some("venue-lan"))),
        )],
    );
    let path = plan(&definition);
    let endpoints = stream_endpoints(&definition, &path, &nodes()).unwrap();
    assert_eq!(
        endpoints.ingress.unwrap().url,
        "http://192.168.1.10:28080/whip/weave-feed-sender"
    );
    assert_eq!(
        endpoints.destinations[0].endpoint.as_ref().unwrap().url,
        "http://192.168.1.10:28081/whep/weave-feed-receiver-monitor"
    );
}

#[test]
fn a_node_without_a_signalling_base_cannot_take_an_outside_peer() {
    let mut nodes = nodes();
    for attachment in &mut nodes[0].topology.attachments {
        attachment.listeners.whip = None;
    }
    let definition = stream(
        StreamTransport::Whip(signalling("strom-node-1", None)),
        vec![("studio", srt("strom-node-2"))],
    );
    assert_eq!(
        derive_path(
            &definition,
            &nodes,
            &[],
            &mut PortAllocator::new(),
            &LinkKeys::for_tests(),
        ),
        Err(PlacementError::NoSignalling {
            node: "strom-node-1".to_string(),
            transport: Transport::Whip,
        })
    );

    let pinned = stream(
        StreamTransport::Whip(signalling("strom-node-1", Some("studio-lan"))),
        vec![("studio", srt("strom-node-2"))],
    );
    assert!(matches!(
        derive_path(
            &pinned,
            &nodes,
            &[],
            &mut PortAllocator::new(),
            &LinkKeys::for_tests(),
        ),
        Err(PlacementError::UnknownNetwork { .. })
    ));
}

#[test]
fn a_placed_stream_reports_the_urls_outside_peers_call() {
    let definition = stream(
        StreamTransport::Whip(signalling("strom-node-1", None)),
        vec![(
            "monitor",
            StreamTransport::Whep(signalling("strom-node-2", None)),
        )],
    );
    let outcome = reconcile(
        vec![definition],
        &ObservedState {
            nodes: nodes(),
            endpoints: Vec::new(),
            hops: Vec::new(),
        },
        &LinkKeys::for_tests(),
    );
    let status = &outcome.streams[0];
    assert!(status.conditions.iter().any(|condition| {
        condition.condition_type == StreamConditionType::PlacementReady
            && condition.status == weave_core::StreamConditionStatus::True
    }));
    assert_eq!(
        status.ingress.as_ref().unwrap().url,
        "http://10.97.26.10:8080/whip/weave-feed-sender"
    );
    assert_eq!(
        status.destinations[0].endpoint.as_ref().unwrap().url,
        "http://10.97.27.10:8080/whep/weave-feed-receiver-monitor"
    );
    assert_eq!(
        outcome.endpoints["feed"].ingress, status.ingress,
        "GET /streams/feed/endpoints serves the same addresses"
    );
}

fn sender_format(video: VideoCodec) -> MediaFormat {
    MediaFormat {
        container: Container::Rtp,
        video: Some(VideoFormat {
            codec: video,
            width: 1280,
            height: 720,
            framerate: Framerate::new(30, 1),
            chroma_subsampling: ChromaSubsampling::Yuv420,
        }),
        audio: Some(AudioFormat {
            codec: AudioCodec::Opus,
            sample_rate: 48_000,
            channels: 2,
        }),
    }
}

fn reconciled(format: Option<MediaFormat>) -> StreamStatus {
    reconcile_whip_source(format)
        .streams
        .into_iter()
        .next()
        .unwrap()
}

fn reconcile_whip_source(format: Option<MediaFormat>) -> crate::ReconcileOutcome {
    let definition = stream(
        StreamTransport::Whip(SignallingEndpoint {
            format,
            ..signalling("strom-node-1", None)
        }),
        vec![("studio", srt("strom-node-2"))],
    );
    reconcile(
        vec![definition],
        &ObservedState {
            nodes: nodes(),
            endpoints: Vec::new(),
            hops: Vec::new(),
        },
        &LinkKeys::for_tests(),
    )
}

fn format_compatible(conditions: &[weave_core::StreamCondition]) -> &weave_core::StreamCondition {
    conditions
        .iter()
        .find(|condition| condition.condition_type == StreamConditionType::FormatCompatible)
        .unwrap()
}

#[test]
fn a_whip_sender_declaring_a_codec_the_ingest_cannot_take_is_a_format_mismatch() {
    let status = reconciled(Some(sender_format(VideoCodec::Vp8)));
    assert_eq!(status.status, PathStatus::Degraded);
    assert!(status.conditions.iter().any(|condition| {
        condition.condition_type == StreamConditionType::PlacementReady
            && condition.status == StreamConditionStatus::True
    }));
    let detail = "node strom-node-1 cannot take the source format through profile whip-to-srt: \
                  video.codec is vp8 but accepts h264";
    for conditions in [&status.conditions, &status.destinations[0].conditions] {
        let condition = format_compatible(conditions);
        assert_eq!(condition.status, StreamConditionStatus::False);
        assert_eq!(condition.reason, StreamConditionReason::FormatMismatch);
        assert_eq!(condition.detail, detail);
    }
    assert_eq!(status.destinations[0].status, PathStatus::Degraded);
}

#[test]
fn a_whip_sender_declaring_what_the_ingest_takes_is_compatible() {
    let status = reconciled(Some(sender_format(VideoCodec::H264)));
    assert_eq!(status.status, PathStatus::Pending);
    let condition = format_compatible(&status.conditions);
    assert_eq!(condition.status, StreamConditionStatus::True);
    assert_eq!(condition.reason, StreamConditionReason::FormatCompatible);

    let undeclared = reconciled(None);
    assert_eq!(
        format_compatible(&undeclared.conditions).reason,
        StreamConditionReason::FormatUnknown
    );
}

#[test]
fn a_whip_sender_declaring_one_track_is_built_and_checked_for_that_track() {
    let video_only = MediaFormat {
        audio: None,
        ..sender_format(VideoCodec::H264)
    };
    let audio_only = MediaFormat {
        video: None,
        ..sender_format(VideoCodec::H264)
    };
    for (format, tracks) in [
        (video_only.clone(), vec![Track::Video]),
        (audio_only, vec![Track::Audio]),
    ] {
        let outcome = reconcile_whip_source(Some(format));
        let condition = format_compatible(&outcome.streams[0].conditions);
        assert_eq!(
            condition.reason,
            StreamConditionReason::FormatCompatible,
            "{tracks:?}: {}",
            condition.detail
        );
        for hop in outcome.desired_by_node.values().flatten() {
            assert_eq!(hop.tracks.as_ref(), Some(&tracks), "{}", hop.id);
        }
    }

    let vp8_only = MediaFormat {
        video: video_only.video.map(|video| VideoFormat {
            codec: VideoCodec::Vp8,
            ..video
        }),
        ..video_only
    };
    assert_eq!(
        format_compatible(&reconciled(Some(vp8_only)).conditions).reason,
        StreamConditionReason::FormatMismatch,
        "the track that is sent is still checked"
    );
}
