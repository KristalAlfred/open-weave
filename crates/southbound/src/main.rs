//! Southbound API — adapter and media-node surface for observed and desired state.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;
use weave_core::{
    DesiredHop, EndpointDescriptor, NodeDescriptor, NodeHeartbeat, NodeRegistration, ObservedState,
};

const DEFAULT_ADDR: &str = "127.0.0.1:8081";

/// Durable southbound state: registered nodes and their desired hops.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Persisted {
    #[serde(default)]
    nodes: BTreeMap<String, NodeRegistration>,
    #[serde(default)]
    desired: BTreeMap<String, Vec<DesiredHop>>,
}

#[derive(Clone, Default)]
struct AppState {
    nodes: Arc<RwLock<BTreeMap<String, NodeRegistration>>>,
    desired: Arc<RwLock<BTreeMap<String, Vec<DesiredHop>>>>,
    state_file: Option<Arc<PathBuf>>,
    writes: Arc<AtomicU64>,
}

impl AppState {
    fn new(state_file: Option<PathBuf>) -> Result<Self> {
        let persisted = match &state_file {
            Some(path) => weave_core::load::<Persisted>(path)
                .context("loading southbound state")?
                .unwrap_or_default(),
            None => Persisted::default(),
        };
        Ok(Self {
            nodes: Arc::new(RwLock::new(persisted.nodes)),
            desired: Arc::new(RwLock::new(persisted.desired)),
            state_file: state_file.map(Arc::new),
            writes: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn persist(&self) {
        let Some(path) = &self.state_file else {
            return;
        };
        let snapshot = {
            let nodes = self.nodes.read().await;
            let desired = self.desired.read().await;
            Persisted {
                nodes: nodes.clone(),
                desired: desired.clone(),
            }
        };
        match weave_core::store(path.as_path(), &snapshot) {
            Ok(()) => {
                self.writes.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => tracing::error!(%err, "failed to persist southbound state"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("WEAVE_SOUTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let state_file = std::env::var_os("WEAVE_SOUTHBOUND_STATE").map(PathBuf::from);
    let app = router(AppState::new(state_file)?);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding southbound listener on {addr}"))?;
    tracing::info!(%addr, "southbound API listening");

    axum::serve(listener, app)
        .await
        .context("southbound server error")?;
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/nodes", get(list_nodes))
        .route("/nodes/register", post(register_node))
        .route("/nodes/{node_id}/heartbeat", post(node_heartbeat))
        .route(
            "/nodes/{node_id}/desired",
            get(get_desired).put(put_desired),
        )
        .route("/endpoints", get(list_endpoints))
        .route("/state", get(get_state))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_nodes(State(state): State<AppState>) -> Json<Vec<NodeDescriptor>> {
    let nodes = state.nodes.read().await;
    Json(
        nodes
            .values()
            .map(|registration| registration.node.clone())
            .collect(),
    )
}

async fn list_endpoints(State(state): State<AppState>) -> Json<Vec<EndpointDescriptor>> {
    let nodes = state.nodes.read().await;
    Json(
        nodes
            .values()
            .flat_map(|registration| registration.endpoints.clone())
            .collect(),
    )
}

async fn get_state(State(state): State<AppState>) -> Json<ObservedState> {
    let nodes = state.nodes.read().await;
    Json(ObservedState {
        nodes: nodes.values().map(|r| r.node.clone()).collect(),
        endpoints: nodes.values().flat_map(|r| r.endpoints.clone()).collect(),
        hops: nodes.values().flat_map(|r| r.hop_status.clone()).collect(),
    })
}

async fn register_node(
    State(state): State<AppState>,
    Json(registration): Json<NodeRegistration>,
) -> (StatusCode, Json<Value>) {
    let node_id = registration.node.id.clone();
    let endpoint_count = registration.endpoints.len();

    state
        .nodes
        .write()
        .await
        .insert(node_id.clone(), registration);
    state.persist().await;

    tracing::info!(%node_id, endpoint_count, "node registered");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "node_id": node_id })),
    )
}

async fn node_heartbeat(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(heartbeat): Json<NodeHeartbeat>,
) -> (StatusCode, Json<Value>) {
    if node_id != heartbeat.node_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "message": "node id mismatch" })),
        );
    }

    let mut nodes = state.nodes.write().await;
    let Some(registration) = nodes.get_mut(&node_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "status": "error", "message": "unknown node" })),
        );
    };

    registration.node.status = heartbeat.status;
    registration.endpoints = heartbeat.endpoints;
    registration.hop_status = heartbeat.hop_status;

    tracing::debug!(%node_id, status = ?registration.node.status, "node heartbeat");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "node_id": node_id })),
    )
}

/// Full-replace the desired hops for a node. Idempotent, and accepted even before
/// the node registers — it sits until the node comes online and pulls it.
async fn put_desired(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(hops): Json<Vec<DesiredHop>>,
) -> (StatusCode, Json<Value>) {
    let hop_count = hops.len();
    let changed = {
        let mut desired = state.desired.write().await;
        let changed = desired.get(&node_id) != Some(&hops);
        desired.insert(node_id.clone(), hops);
        changed
    };
    if changed {
        state.persist().await;
    }

    tracing::info!(%node_id, hop_count, changed, "desired hops set");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "node_id": node_id, "hops": hop_count })),
    )
}

async fn get_desired(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Json<Vec<DesiredHop>> {
    let desired = state.desired.read().await;
    Json(desired.get(&node_id).cloned().unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use weave_core::{
        HopRole, HopState, HopStatus, LinkCondition, NodeCapabilities, NodeStatus, SocketRole,
        SocketSpec, SrtParams, Transport,
    };

    fn hop(id: &str, node_id: &str) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: node_id.to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Listen,
                host: None,
                port: Some(7001),
                params: SrtParams { latency: Some(200) },
            },
            egresses: vec![SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Connect,
                host: Some("10.0.0.2".to_string()),
                port: Some(7002),
                params: SrtParams::default(),
            }],
        }
    }

    fn registration(node_id: &str) -> NodeRegistration {
        NodeRegistration {
            node: NodeDescriptor {
                id: node_id.to_string(),
                endpoint: "http://10.0.0.1:8080".to_string(),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities::default(),
            },
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(body.map_or(Body::empty(), |v| {
                Body::from(serde_json::to_vec(&v).unwrap())
            }))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, value)
    }

    #[tokio::test]
    async fn desired_round_trips_and_full_replaces() {
        let app = router(AppState::default());

        let (status, _) = send(
            &app,
            "PUT",
            "/nodes/strom-node-1/desired",
            Some(serde_json::to_value([hop("weave-a", "strom-node-1")]).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, body) = send(&app, "GET", "/nodes/strom-node-1/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let stored: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(stored, vec![hop("weave-a", "strom-node-1")]);

        let (_, _) = send(
            &app,
            "PUT",
            "/nodes/strom-node-1/desired",
            Some(serde_json::to_value([hop("weave-b", "strom-node-1")]).unwrap()),
        )
        .await;
        let (_, body) = send(&app, "GET", "/nodes/strom-node-1/desired", None).await;
        let stored: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(
            stored,
            vec![hop("weave-b", "strom-node-1")],
            "PUT fully replaces prior desired state"
        );
    }

    #[tokio::test]
    async fn desired_for_unset_node_is_empty() {
        let app = router(AppState::default());
        let (status, body) = send(&app, "GET", "/nodes/never-set/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let stored: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert!(stored.is_empty());
    }

    #[tokio::test]
    async fn desired_accepted_before_node_registers() {
        let app = router(AppState::default());

        let (status, _) = send(
            &app,
            "PUT",
            "/nodes/future-node/desired",
            Some(serde_json::to_value([hop("weave-a", "future-node")]).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (_, nodes) = send(&app, "GET", "/nodes", None).await;
        assert_eq!(
            nodes.as_array().map(Vec::len),
            Some(0),
            "desired state does not register the node"
        );

        let (_, body) = send(&app, "GET", "/nodes/future-node/desired", None).await;
        let stored: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(stored, vec![hop("weave-a", "future-node")]);
    }

    #[tokio::test]
    async fn state_aggregates_hop_status_from_heartbeats() {
        let app = router(AppState::default());

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(registration("strom-node-1")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let heartbeat = NodeHeartbeat {
            node_id: "strom-node-1".to_string(),
            status: NodeStatus::Ready,
            endpoints: Vec::new(),
            hop_status: vec![HopStatus {
                id: "weave-a".to_string(),
                node_id: "strom-node-1".to_string(),
                state: HopState::Provisioned,
                ingress: LinkCondition::Flowing,
                egress: LinkCondition::Connected,
                resolved_ingress: None,
                resolved_egress: None,
                stats: None,
            }],
        };
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, body) = send(&app, "GET", "/state", None).await;
        assert_eq!(status, StatusCode::OK);
        let observed: ObservedState = serde_json::from_value(body).unwrap();
        assert_eq!(observed.nodes.len(), 1);
        assert_eq!(observed.hops.len(), 1);
        assert_eq!(observed.hops[0].id, "weave-a");
        assert_eq!(observed.hops[0].state, HopState::Provisioned);
        assert_eq!(observed.hops[0].ingress, LinkCondition::Flowing);
    }

    fn temp_state_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weave-sb-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{name}.json"));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn read_persisted(path: &PathBuf) -> Persisted {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    fn heartbeat(node_id: &str) -> NodeHeartbeat {
        NodeHeartbeat {
            node_id: node_id.to_string(),
            status: NodeStatus::Ready,
            endpoints: Vec::new(),
            hop_status: vec![HopStatus {
                id: "weave-a".to_string(),
                node_id: node_id.to_string(),
                state: HopState::Provisioned,
                ingress: LinkCondition::Flowing,
                egress: LinkCondition::Connected,
                resolved_ingress: None,
                resolved_egress: None,
                stats: None,
            }],
        }
    }

    #[tokio::test]
    async fn register_persists_node_to_state_file() {
        let path = temp_state_path("register-persists");
        let state = AppState::new(Some(path.clone())).unwrap();
        let app = router(state.clone());

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(registration("strom-node-1")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let persisted = read_persisted(&path);
        assert!(persisted.nodes.contains_key("strom-node-1"));
        assert_eq!(state.writes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn changed_desired_writes_but_unchanged_does_not() {
        let path = temp_state_path("desired-diff");
        let state = AppState::new(Some(path.clone())).unwrap();
        let app = router(state.clone());

        let put = |hops: Value| {
            let app = app.clone();
            async move { send(&app, "PUT", "/nodes/strom-node-1/desired", Some(hops)).await }
        };

        let first = serde_json::to_value([hop("weave-a", "strom-node-1")]).unwrap();
        let (status, _) = put(first.clone()).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(state.writes.load(Ordering::Relaxed), 1);

        let (_, _) = put(first).await;
        assert_eq!(
            state.writes.load(Ordering::Relaxed),
            1,
            "identical desired must not trigger a write"
        );

        let changed = serde_json::to_value([hop("weave-b", "strom-node-1")]).unwrap();
        let (_, _) = put(changed).await;
        assert_eq!(state.writes.load(Ordering::Relaxed), 2);
        assert_eq!(
            read_persisted(&path).desired.get("strom-node-1"),
            Some(&vec![hop("weave-b", "strom-node-1")])
        );
    }

    #[tokio::test]
    async fn heartbeat_does_not_write_or_change_persisted_state() {
        let path = temp_state_path("heartbeat-writefree");
        let state = AppState::new(Some(path.clone())).unwrap();
        let app = router(state.clone());

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(registration("strom-node-1")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(state.writes.load(Ordering::Relaxed), 1);
        let before = std::fs::read(&path).unwrap();

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            Some(serde_json::to_value(heartbeat("strom-node-1")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        assert_eq!(
            state.writes.load(Ordering::Relaxed),
            1,
            "heartbeat must not write to disk"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "heartbeat must not change persisted contents"
        );
    }

    #[tokio::test]
    async fn state_loads_from_pre_seeded_file() {
        let path = temp_state_path("sb-pre-seeded");
        let mut nodes = BTreeMap::new();
        nodes.insert("strom-node-1".to_string(), registration("strom-node-1"));
        let mut desired = BTreeMap::new();
        desired.insert(
            "strom-node-1".to_string(),
            vec![hop("weave-a", "strom-node-1")],
        );
        let seed = Persisted { nodes, desired };
        std::fs::write(&path, serde_json::to_vec(&seed).unwrap()).unwrap();

        let app = router(AppState::new(Some(path)).unwrap());

        let (_, nodes_body) = send(&app, "GET", "/nodes", None).await;
        assert_eq!(nodes_body.as_array().map(Vec::len), Some(1));
        let (_, desired_body) = send(&app, "GET", "/nodes/strom-node-1/desired", None).await;
        let stored: Vec<DesiredHop> = serde_json::from_value(desired_body).unwrap();
        assert_eq!(stored, vec![hop("weave-a", "strom-node-1")]);
    }
}
