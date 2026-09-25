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

const RECEIVERS: usize = 300;

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

fn fan_out_stream() -> StreamDefinition {
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
        destinations: (0..RECEIVERS)
            .map(|index| StreamDestination {
                id: receiver_id(index),
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

async fn fan_out(label: &str, nodes: Vec<NodeRegistration>) -> AppState {
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
    let stream = fan_out_stream();
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
    assert_eq!(status.destinations.len(), RECEIVERS);
    assert!(status.conditions.iter().any(|condition| {
        condition.condition_type == StreamConditionType::PlacementReady
            && condition.status == StreamConditionStatus::True
    }));
    let desired = state.desired.read().await;
    assert_eq!(desired["source"].hops.len(), 1);
    assert_eq!(desired["source"].hops[0].egresses.len(), RECEIVERS);
    for index in 0..RECEIVERS {
        let hops = &desired[&receiver_id(index)].hops;
        assert_eq!(hops.len(), 1);
        assert_eq!(hops[0].role, HopRole::Receiver);
    }
    drop(desired);
    drop(view);
    state
}

#[tokio::test]
async fn direct_fan_out_to_three_hundred_nodes() {
    let mut nodes = vec![public_node("source", "10.0.0.1")];
    nodes.extend((0..RECEIVERS).map(|index| {
        public_node(
            &receiver_id(index),
            &format!("10.1.{}.{}", index / 250, index % 250 + 1),
        )
    }));
    fan_out("direct", nodes).await;
}

#[tokio::test]
#[ignore = "about 0.7 s in debug; run with `cargo test -p weave-controller relayed_fan_out -- --ignored --nocapture`"]
async fn relayed_fan_out_to_three_hundred_nodes() {
    let mut nodes = vec![
        nat_node("source"),
        public_node("relay-a", "10.0.0.2"),
        public_node("relay-b", "10.0.0.3"),
    ];
    nodes.extend((0..RECEIVERS).map(|index| nat_node(&receiver_id(index))));
    let state = fan_out("relayed", nodes).await;

    let desired = state.desired.read().await;
    assert_eq!(desired["relay-a"].hops.len(), RECEIVERS);
    assert!(desired["relay-b"].hops.is_empty());
}
