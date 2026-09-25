//! One stream fanned out to a few hundred registered nodes, and the dashboard
//! view of many running streams. Timings are printed, not asserted; run with
//! `--nocapture` to read them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde::Serialize;
use tower::ServiceExt;
use weave_core::auth::{Guard, NodeGuard};
use weave_core::{
    DesiredHop, EgressStatus, HopEndpointClass, HopProfile, HopRole, HopState, HopStatus,
    LinkCondition, NetworkAttachment, NetworkListeners, NodeCapabilities, NodeDescriptor,
    NodeHeartbeat, NodeRegistration, NodeStatus, NodeTopology, PROTOCOL_VERSION, PortRange,
    RoleSet, SocketStatus, SrtEndpoint, SrtListener, StreamConditionStatus, StreamConditionType,
    StreamDefinition, StreamDestination, StreamTransport, Transport, TransportClass,
};

use crate::keys::LinkKeys;
use crate::path::{PortAllocator, derive_path};
use crate::store::MemStore;
use crate::{AppState, observed_state, reconcile_tick, router};

fn srt_listener(host: &str) -> NetworkListeners {
    NetworkListeners {
        srt: Some(SrtListener {
            host: host.to_string(),
            port_range: PortRange {
                start: 20_000,
                end: 20_999,
            },
        }),
        whip: None,
        whep: None,
        rist: None,
    }
}

fn attachment(id: &str, network: &str, listeners: NetworkListeners) -> NetworkAttachment {
    NetworkAttachment {
        id: id.to_string(),
        network: network.to_string(),
        dial: true,
        listeners,
    }
}

fn registration(id: &str, attachments: Vec<NetworkAttachment>) -> NodeRegistration {
    let srt = || {
        HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        })
    };
    NodeRegistration {
        protocol_version: PROTOCOL_VERSION,
        node: NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("http://{id}:8080"),
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
            topology: NodeTopology { attachments },
        },
        endpoints: Vec::new(),
        hop_status: Vec::new(),
    }
}

fn public_node(id: &str, host: &str) -> NodeRegistration {
    registration(id, vec![attachment("wan", "internet", srt_listener(host))])
}

/// Dials out to the internet and listens only on a site network nobody else
/// is on, so every link to or from it needs a relay.
fn nat_node(id: &str) -> NodeRegistration {
    registration(
        id,
        vec![
            attachment("outbound", "internet", NetworkListeners::default()),
            attachment("site", &format!("{id}-site"), srt_listener("192.168.0.10")),
        ],
    )
}

fn receiver_id(index: usize) -> String {
    format!("rx-{index:03}")
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

fn fan_out_stream(receivers: usize) -> StreamDefinition {
    StreamDefinition {
        name: "feed".to_string(),
        enabled: true,
        allow_cleartext_links: false,
        source: endpoint("source"),
        destinations: (0..receivers)
            .map(|index| StreamDestination {
                id: receiver_id(index),
                paths: 1,
                endpoint: endpoint(&receiver_id(index)),
            })
            .collect(),
    }
}

async fn post(app: &Router, uri: &str, body: &impl Serialize, create: bool) -> StatusCode {
    let mut request = Request::post(uri).header(header::CONTENT_TYPE, "application/json");
    if create {
        request = request.header(header::IF_NONE_MATCH, "*");
    }
    let request = request
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

async fn fresh_state() -> AppState {
    AppState::hydrate(
        Arc::new(MemStore::new()),
        Duration::from_secs(15),
        Duration::from_secs(300),
        None,
        LinkKeys::for_tests(),
    )
    .await
    .unwrap()
}

async fn fan_out(label: &str, nodes: Vec<NodeRegistration>, receivers: usize) -> AppState {
    let state = fresh_state().await;
    let app = router(state.clone(), Guard::Disabled, NodeGuard::Disabled);
    for node in &nodes {
        assert_eq!(
            post(&app, "/nodes/register", node, false).await,
            StatusCode::ACCEPTED
        );
    }
    let stream = fan_out_stream(receivers);
    assert_eq!(
        post(&app, "/streams", &stream, true).await,
        StatusCode::ACCEPTED
    );

    let observed = observed_state(&*state.nodes.read().await);
    let started = Instant::now();
    let path = derive_path(
        &stream,
        &observed.nodes,
        &[],
        &mut PortAllocator::new(),
        &LinkKeys::for_tests(),
    )
    .unwrap();
    let plan = started.elapsed();

    let started = Instant::now();
    reconcile_tick(&state).await;
    let reconcile = started.elapsed();
    eprintln!(
        "{label}: {} nodes, {} hops, plan {plan:?}, reconcile tick {reconcile:?}",
        nodes.len(),
        path.hops.len()
    );

    let view = state.view.read().await;
    let status = &view.streams[0];
    assert_eq!(status.destinations.len(), receivers);
    assert!(status.conditions.iter().any(|condition| {
        condition.condition_type == StreamConditionType::PlacementReady
            && condition.status == StreamConditionStatus::True
    }));
    let desired = state.desired.read().await;
    assert_eq!(desired["source"].hops.len(), 1);
    assert_eq!(desired["source"].hops[0].egresses.len(), receivers);
    for index in 0..receivers {
        let hops = &desired[&receiver_id(index)].hops;
        assert_eq!(hops.len(), 1);
        assert_eq!(hops[0].role, HopRole::Receiver);
    }
    drop(desired);
    drop(view);
    state
}

fn direct_nodes(receivers: usize) -> Vec<NodeRegistration> {
    let mut nodes = vec![public_node("source", "10.0.0.1")];
    nodes.extend((0..receivers).map(|index| {
        public_node(
            &receiver_id(index),
            &format!("10.1.{}.{}", index / 250, index % 250 + 1),
        )
    }));
    nodes
}

fn relayed_nodes(receivers: usize) -> Vec<NodeRegistration> {
    let mut nodes = vec![
        nat_node("source"),
        public_node("relay-a", "10.0.0.2"),
        public_node("relay-b", "10.0.0.3"),
    ];
    nodes.extend((0..receivers).map(|index| nat_node(&receiver_id(index))));
    nodes
}

#[tokio::test]
async fn direct_fan_out_to_three_hundred_nodes() {
    fan_out("direct", direct_nodes(300), 300).await;
}

#[tokio::test]
async fn relayed_fan_out_to_three_hundred_nodes() {
    let state = fan_out("relayed", relayed_nodes(300), 300).await;

    let desired = state.desired.read().await;
    assert_eq!(desired["relay-a"].hops.len(), 300);
    assert!(desired["relay-b"].hops.is_empty());
}

#[tokio::test]
#[ignore = "about a second in debug; run with `cargo test -p weave-controller thousand -- --ignored --nocapture --test-threads=1`"]
async fn direct_fan_out_to_a_thousand_nodes() {
    fan_out("direct", direct_nodes(1000), 1000).await;
}

#[tokio::test]
#[ignore = "about a second in debug; run with `cargo test -p weave-controller thousand -- --ignored --nocapture --test-threads=1`"]
async fn relayed_fan_out_to_a_thousand_nodes() {
    let state = fan_out("relayed", relayed_nodes(1000), 1000).await;

    let desired = state.desired.read().await;
    assert_eq!(desired["relay-a"].hops.len(), 500);
    assert_eq!(desired["relay-b"].hops.len(), 500);
}

fn roomy_node(id: &str, host: &str, ports: u16) -> NodeRegistration {
    let mut node = public_node(id, host);
    for attachment in &mut node.node.topology.attachments {
        if let Some(srt) = &mut attachment.listeners.srt {
            srt.port_range.end = srt.port_range.start + ports;
        }
    }
    node
}

fn one_destination_stream(name: &str) -> StreamDefinition {
    StreamDefinition {
        name: name.to_string(),
        enabled: true,
        allow_cleartext_links: false,
        source: endpoint("source"),
        destinations: vec![StreamDestination {
            id: "studio".to_string(),
            paths: 1,
            endpoint: endpoint("studio"),
        }],
    }
}

fn flowing() -> SocketStatus {
    SocketStatus {
        condition: LinkCondition::Flowing,
        resolved: None,
        stats: None,
    }
}

/// A report of each of `hops` provisioned with every socket flowing.
fn running(hops: &[DesiredHop]) -> Vec<HopStatus> {
    hops.iter()
        .map(|hop| HopStatus {
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            state: HopState::Provisioned,
            ingress: flowing(),
            merge_ingress: None,
            egresses: hop
                .egresses
                .iter()
                .map(|egress| EgressStatus {
                    branch_id: egress.branch_id.clone(),
                    status: flowing(),
                })
                .collect(),
        })
        .collect()
}

/// Times `GET /view` with `count` streams from `source` to `studio`, placed and
/// reported running by both nodes.
async fn time_the_view_with_reports(count: usize) {
    let state = fresh_state().await;
    let app = router(state.clone(), Guard::Disabled, NodeGuard::Disabled);
    let ports = u16::try_from(3 * count).unwrap();
    for node in [
        roomy_node("source", "10.0.0.1", ports),
        roomy_node("studio", "10.0.0.2", ports),
    ] {
        assert_eq!(
            post(&app, "/nodes/register", &node, false).await,
            StatusCode::ACCEPTED
        );
    }
    for index in 0..count {
        let stream = one_destination_stream(&format!("feed-{index}"));
        assert_eq!(
            post(&app, "/streams", &stream, true).await,
            StatusCode::ACCEPTED
        );
    }
    reconcile_tick(&state).await;
    let desired: Vec<(String, Vec<DesiredHop>)> = state
        .desired
        .read()
        .await
        .iter()
        .map(|(node, snapshot)| (node.clone(), snapshot.hops.clone()))
        .collect();
    let mut reports = 0;
    for (node, hops) in &desired {
        let heartbeat = NodeHeartbeat {
            node_id: node.clone(),
            status: NodeStatus::Ready,
            endpoints: Vec::new(),
            hop_status: running(hops),
        };
        reports += heartbeat.hop_status.len();
        assert_eq!(
            post(&app, &format!("/nodes/{node}/heartbeat"), &heartbeat, false).await,
            StatusCode::ACCEPTED
        );
    }

    let started = Instant::now();
    let response = app
        .clone()
        .oneshot(Request::get("/view").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let elapsed = started.elapsed();
    eprintln!("{count} streams, {reports} reports: view {elapsed:?}");

    let view: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let streams = view["streams"].as_array().unwrap();
    assert_eq!(streams.len(), count);
    for stream in streams {
        let hops = stream["hops"].as_array().unwrap();
        assert_eq!(hops.len(), 2);
        for hop in hops {
            assert_eq!(hop["state"], "provisioned");
            assert_eq!(hop["egresses"][0]["condition"], "flowing");
        }
    }
}

#[tokio::test]
#[ignore = "timing; run with `cargo test -p weave-controller view_with_reports -- --ignored --nocapture --test-threads=1`"]
async fn the_view_with_reports_at_one_and_two_thousand_streams() {
    time_the_view_with_reports(1000).await;
    time_the_view_with_reports(2000).await;
}
