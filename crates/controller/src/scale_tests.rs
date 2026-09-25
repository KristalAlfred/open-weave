//! One stream fanned out to a few hundred registered nodes. Timings are printed,
//! not asserted; run with `--nocapture` to read them.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use serde::Serialize;
use tower::ServiceExt;
use weave_core::auth::{Guard, NodeGuard};
use weave_core::{
    HopEndpointClass, HopProfile, HopRole, NetworkAttachment, NetworkListeners, NodeCapabilities,
    NodeDescriptor, NodeRegistration, NodeStatus, NodeTopology, PROTOCOL_VERSION, PortRange,
    RoleSet, SrtEndpoint, SrtListener, StreamConditionStatus, StreamConditionType,
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

fn fan_out_stream(receivers: usize) -> StreamDefinition {
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

async fn fan_out(label: &str, nodes: Vec<NodeRegistration>, receivers: usize) -> AppState {
    let state = AppState::hydrate(
        Arc::new(MemStore::new()),
        Duration::from_secs(15),
        Duration::from_secs(300),
        None,
        LinkKeys::for_tests(),
    )
    .await
    .unwrap();
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
