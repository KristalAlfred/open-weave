//! `weave-controller` — the single stateful control-plane service. It owns the
//! stream and node registries (persisted to Postgres), serves the northbound and
//! southbound HTTP surfaces, and reconciles desired streams into per-node desired
//! hops on a fixed interval, entirely from in-memory state.

mod path;
mod store;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::{
    DesiredHop, EndpointDescriptor, NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus,
    ObservedState, PathStatus, ReconcileReport, ReconcileStatus, StreamDefinition,
};

use path::{PortAllocator, StreamEndpoints, derive_path, path_status, stream_endpoints};
use store::{MemStore, PgStore, StateStore};

#[derive(Debug, Parser)]
#[command(name = "weave-controller", version, about = "open-weave reconciler")]
struct Args {
    #[arg(long, env = "WEAVE_CONTROLLER_ADDR", default_value = "127.0.0.1:8082")]
    listen: String,
    #[arg(long, env = "WEAVE_RECONCILE_INTERVAL_SECS", default_value_t = 5)]
    interval_secs: u64,
    /// A node is marked `Offline` once this many seconds elapse without a
    /// heartbeat. Its desired hops are still computed and served so a returning
    /// adapter resumes on the same deterministic ports.
    #[arg(long, env = "WEAVE_NODE_TTL_SECS", default_value_t = 15)]
    node_ttl_secs: u64,
    /// Postgres connection URL. When unset the controller runs with an
    /// in-memory store and does not persist state across restarts.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
}

/// Latest reconcile snapshot served by the read-only status/discovery API.
#[derive(Default)]
struct ControllerView {
    status: Value,
    streams: BTreeSet<String>,
    endpoints: BTreeMap<String, StreamEndpoints>,
}

#[derive(Clone)]
struct AppState {
    store: Arc<dyn StateStore>,
    streams: Arc<RwLock<BTreeMap<String, StreamDefinition>>>,
    nodes: Arc<RwLock<BTreeMap<String, NodeRegistration>>>,
    last_seen: Arc<RwLock<BTreeMap<String, Instant>>>,
    node_ttl: Duration,
    desired: Arc<RwLock<BTreeMap<String, Vec<DesiredHop>>>>,
    view: Arc<RwLock<ControllerView>>,
}

impl AppState {
    async fn hydrate(store: Arc<dyn StateStore>, node_ttl: Duration) -> Result<Self> {
        let streams = store
            .load_streams()
            .await
            .context("hydrating streams")?
            .into_iter()
            .map(|stream| (stream.name.clone(), stream))
            .collect();
        let nodes = store
            .load_nodes()
            .await
            .context("hydrating nodes")?
            .into_iter()
            .map(|registration| (registration.node.id.clone(), registration))
            .collect::<BTreeMap<String, NodeRegistration>>();
        let boot = Instant::now();
        let last_seen = nodes.keys().map(|id| (id.clone(), boot)).collect();
        Ok(Self {
            store,
            streams: Arc::new(RwLock::new(streams)),
            nodes: Arc::new(RwLock::new(nodes)),
            last_seen: Arc::new(RwLock::new(last_seen)),
            node_ttl,
            desired: Arc::new(RwLock::new(BTreeMap::new())),
            view: Arc::new(RwLock::new(ControllerView {
                status: json!({ "status": "starting" }),
                ..ControllerView::default()
            })),
        })
    }
}

#[derive(Debug, Serialize)]
struct StreamStatus {
    name: String,
    status: PathStatus,
    nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoints: Option<StreamEndpoints>,
}

struct ReconcileOutcome {
    report: ReconcileReport,
    streams: Vec<StreamStatus>,
    endpoints: BTreeMap<String, StreamEndpoints>,
    desired_by_node: BTreeMap<String, Vec<DesiredHop>>,
}

impl ReconcileOutcome {
    fn status_json(&self) -> Value {
        json!({
            "status": self.report.status,
            "summary": self.report.summary,
            "streams": self.streams,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let interval = Duration::from_secs(args.interval_secs);
    let node_ttl = Duration::from_secs(args.node_ttl_secs);

    let store: Arc<dyn StateStore> = match &args.database_url {
        Some(url) => {
            tracing::info!("connecting controller store to postgres");
            Arc::new(
                PgStore::connect(url)
                    .await
                    .context("opening postgres store")?,
            )
        }
        None => {
            tracing::warn!("DATABASE_URL unset; using in-memory store (state is not persisted)");
            Arc::new(MemStore::new())
        }
    };

    let state = AppState::hydrate(store, node_ttl).await?;
    let api = spawn_api_server(args.listen.clone(), state.clone());

    tracing::info!(interval_secs = args.interval_secs, "controller starting");

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("waiting for shutdown signal")?;
                tracing::info!("controller shutting down");
                api.abort();
                return Ok(());
            }
            () = async {
                reconcile_tick(&state).await;
                tokio::time::sleep(interval).await;
            } => {}
        }
    }
}

async fn reconcile_tick(state: &AppState) {
    let streams: Vec<StreamDefinition> = state.streams.read().await.values().cloned().collect();
    let observed = {
        let mut nodes = state.nodes.write().await;
        let last_seen = state.last_seen.read().await;
        mark_offline(&mut nodes, &last_seen, Instant::now(), state.node_ttl);
        observed_state(&nodes)
    };
    let outcome = reconcile(streams, &observed);

    for stream in &outcome.streams {
        tracing::info!(stream = %stream.name, status = ?stream.status, "stream status");
    }
    tracing::info!(
        status = ?outcome.report.status,
        summary = %outcome.report.summary,
        "reconcile tick"
    );

    let status_json = outcome.status_json();
    let names = outcome.streams.iter().map(|s| s.name.clone()).collect();
    *state.desired.write().await = outcome.desired_by_node;
    let mut view = state.view.write().await;
    view.status = status_json;
    view.streams = names;
    view.endpoints = outcome.endpoints;
}

/// Mark nodes whose last heartbeat is older than `ttl` as [`NodeStatus::Offline`].
/// Nodes seen within the TTL keep their reported status. Pure: the caller supplies
/// `now`, so the boundary is testable without a clock.
fn mark_offline(
    nodes: &mut BTreeMap<String, NodeRegistration>,
    last_seen: &BTreeMap<String, Instant>,
    now: Instant,
    ttl: Duration,
) {
    for (id, registration) in nodes.iter_mut() {
        if let Some(seen) = last_seen.get(id)
            && now.saturating_duration_since(*seen) > ttl
        {
            registration.node.status = NodeStatus::Offline;
        }
    }
}

fn observed_state(nodes: &BTreeMap<String, NodeRegistration>) -> ObservedState {
    ObservedState {
        nodes: nodes.values().map(|r| r.node.clone()).collect(),
        endpoints: nodes.values().flat_map(|r| r.endpoints.clone()).collect(),
        hops: nodes.values().flat_map(|r| r.hop_status.clone()).collect(),
    }
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(get_status))
        .route("/streams", get(list_streams).post(submit_stream))
        .route("/streams/{name}", axum::routing::delete(delete_stream))
        .route("/streams/{name}/endpoints", get(get_endpoints))
        .route("/nodes", get(list_nodes))
        .route("/nodes/register", post(register_node))
        .route("/nodes/{node_id}/heartbeat", post(node_heartbeat))
        .route("/nodes/{node_id}/desired", get(get_desired))
        .route("/endpoints", get(list_endpoints))
        .route("/state", get(get_state))
        .with_state(state)
}

fn spawn_api_server(addr: String, state: AppState) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = router(state);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("binding controller listener on {addr}"))?;
        tracing::info!(%addr, "controller API listening");
        axum::serve(listener, app)
            .await
            .context("controller server error")?;
        Ok(())
    })
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn get_status(State(state): State<AppState>) -> Json<Value> {
    Json(state.view.read().await.status.clone())
}

// --- stream registry (northbound surface) ---

async fn list_streams(State(state): State<AppState>) -> Json<Vec<StreamDefinition>> {
    Json(state.streams.read().await.values().cloned().collect())
}

async fn submit_stream(
    State(state): State<AppState>,
    Json(stream): Json<StreamDefinition>,
) -> Response {
    if stream.name.trim().is_empty() {
        return error(StatusCode::BAD_REQUEST, "stream name must not be empty");
    }
    let name = stream.name.clone();
    {
        let mut streams = state.streams.write().await;
        if let Err(err) = state.store.upsert_stream(&stream).await {
            tracing::error!(%err, %name, "persisting stream failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to persist stream",
            );
        }
        streams.insert(name.clone(), stream);
    }
    tracing::info!(%name, "stream accepted");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "name": name })),
    )
        .into_response()
}

async fn delete_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let mut streams = state.streams.write().await;
    if !streams.contains_key(&name) {
        return error(StatusCode::NOT_FOUND, "stream not found");
    }
    if let Err(err) = state.store.delete_stream(&name).await {
        tracing::error!(%err, %name, "deleting stream failed");
        return error(StatusCode::INTERNAL_SERVER_ERROR, "failed to delete stream");
    }
    streams.remove(&name);
    tracing::info!(%name, "stream deleted");
    StatusCode::NO_CONTENT.into_response()
}

/// Concrete `srt://` endpoints for a placed stream: `200` when placed, `503` when
/// the stream is known but not yet placed, `404` when unknown.
async fn get_endpoints(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let view = state.view.read().await;
    if let Some(endpoints) = view.endpoints.get(&name) {
        (StatusCode::OK, Json(endpoints)).into_response()
    } else if view.streams.contains(&name) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "pending", "stream": name })),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "stream not found" })),
        )
            .into_response()
    }
}

// --- node registry (southbound surface) ---

async fn list_nodes(State(state): State<AppState>) -> Json<Vec<NodeDescriptor>> {
    Json(
        state
            .nodes
            .read()
            .await
            .values()
            .map(|r| r.node.clone())
            .collect(),
    )
}

async fn list_endpoints(State(state): State<AppState>) -> Json<Vec<EndpointDescriptor>> {
    Json(
        state
            .nodes
            .read()
            .await
            .values()
            .flat_map(|r| r.endpoints.clone())
            .collect(),
    )
}

async fn get_state(State(state): State<AppState>) -> Json<ObservedState> {
    Json(observed_state(&*state.nodes.read().await))
}

async fn register_node(
    State(state): State<AppState>,
    Json(registration): Json<NodeRegistration>,
) -> Response {
    let node_id = registration.node.id.clone();
    let endpoint_count = registration.endpoints.len();
    {
        let mut nodes = state.nodes.write().await;
        if let Err(err) = state.store.upsert_node(&registration).await {
            tracing::error!(%err, %node_id, "persisting node registration failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to persist node registration",
            );
        }
        nodes.insert(node_id.clone(), registration);
    }
    state
        .last_seen
        .write()
        .await
        .insert(node_id.clone(), Instant::now());
    tracing::info!(%node_id, endpoint_count, "node registered");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "node_id": node_id })),
    )
        .into_response()
}

/// Heartbeats update only in-memory observed fields; they never touch the store.
async fn node_heartbeat(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(heartbeat): Json<NodeHeartbeat>,
) -> Response {
    if node_id != heartbeat.node_id {
        return error(StatusCode::BAD_REQUEST, "node id mismatch");
    }
    let mut nodes = state.nodes.write().await;
    let Some(registration) = nodes.get_mut(&node_id) else {
        return error(StatusCode::NOT_FOUND, "unknown node");
    };
    registration.node.status = heartbeat.status;
    registration.endpoints = heartbeat.endpoints;
    registration.hop_status = heartbeat.hop_status;
    state
        .last_seen
        .write()
        .await
        .insert(node_id.clone(), Instant::now());

    tracing::debug!(%node_id, status = ?registration.node.status, "node heartbeat");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "node_id": node_id })),
    )
        .into_response()
}

/// Serve the desired hops computed for a node on the last reconcile tick.
async fn get_desired(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Json<Vec<DesiredHop>> {
    Json(
        state
            .desired
            .read()
            .await
            .get(&node_id)
            .cloned()
            .unwrap_or_default(),
    )
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// Compute per-node desired hops, endpoints, and an aggregate report from the
/// current stream and node state. Pure: no IO, deterministic for a given input.
fn reconcile(mut streams: Vec<StreamDefinition>, observed: &ObservedState) -> ReconcileOutcome {
    // Stable order so the per-tick port allocator assigns deterministically for a
    // given stream set.
    streams.sort_by(|a, b| a.name.cmp(&b.name));

    // Seed every registered node with an empty desired list so deleted or disabled
    // streams' hops are cleared.
    let mut desired_by_node: BTreeMap<String, Vec<DesiredHop>> = observed
        .nodes
        .iter()
        .map(|node| (node.id.clone(), Vec::new()))
        .collect();

    let offline: BTreeSet<&str> = observed
        .nodes
        .iter()
        .filter(|node| node.status == NodeStatus::Offline)
        .map(|node| node.id.as_str())
        .collect();

    let mut stream_statuses = Vec::with_capacity(streams.len());
    let mut endpoints_by_stream: BTreeMap<String, StreamEndpoints> = BTreeMap::new();
    let mut enabled = 0usize;
    let mut flowing = 0usize;
    let mut ports = PortAllocator::new();

    for stream in &streams {
        if !stream.enabled {
            stream_statuses.push(StreamStatus {
                name: stream.name.clone(),
                status: PathStatus::Idle,
                nodes: Vec::new(),
                reason: None,
                endpoints: None,
            });
            continue;
        }
        enabled += 1;

        let status = match derive_path(stream, &observed.nodes, &observed.hops, &mut ports) {
            Ok(path) => {
                let mut nodes = Vec::new();
                for hop in &path.hops {
                    if !nodes.contains(&hop.node_id) {
                        nodes.push(hop.node_id.clone());
                    }
                    desired_by_node
                        .entry(hop.node_id.clone())
                        .or_default()
                        .push(hop.clone());
                }
                let (status, reason) = match nodes.iter().find(|id| offline.contains(id.as_str())) {
                    Some(id) => (PathStatus::Degraded, Some(format!("node {id} lost"))),
                    None => (path_status(&path, &observed.hops), None),
                };
                let endpoints = match stream_endpoints(stream, &path, &observed.nodes) {
                    Ok(endpoints) => {
                        endpoints_by_stream.insert(stream.name.clone(), endpoints.clone());
                        Some(endpoints)
                    }
                    Err(error) => {
                        tracing::warn!(stream = %stream.name, %error, "cannot resolve stream endpoints");
                        None
                    }
                };
                stream_statuses.push(StreamStatus {
                    name: stream.name.clone(),
                    status,
                    nodes,
                    reason,
                    endpoints,
                });
                status
            }
            Err(error) => {
                tracing::warn!(stream = %stream.name, %error, "cannot place stream; retrying next tick");
                stream_statuses.push(StreamStatus {
                    name: stream.name.clone(),
                    status: PathStatus::Pending,
                    nodes: Vec::new(),
                    reason: None,
                    endpoints: None,
                });
                PathStatus::Pending
            }
        };
        if status == PathStatus::Flowing {
            flowing += 1;
        }
    }

    // Control-plane convergence, not media flow: a fully provisioned path that is
    // only waiting for its source (AwaitingInput) is converged, not degraded.
    let status = if enabled == 0 {
        ReconcileStatus::Idle
    } else if stream_statuses
        .iter()
        .any(|s| matches!(s.status, PathStatus::Failed | PathStatus::Degraded))
    {
        ReconcileStatus::Degraded
    } else if stream_statuses
        .iter()
        .any(|s| s.status == PathStatus::Pending)
    {
        ReconcileStatus::Converging
    } else {
        ReconcileStatus::Converged
    };
    let report = ReconcileReport {
        status,
        summary: format!(
            "{flowing}/{enabled} stream(s) flowing across {} node(s)",
            desired_by_node.len()
        ),
    };

    ReconcileOutcome {
        report,
        streams: stream_statuses,
        endpoints: endpoints_by_stream,
        desired_by_node,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use weave_core::{NodeCapabilities, PortRange, SrtEndpoint, StreamTransport};

    fn mem_state() -> (AppState, Arc<MemStore>) {
        let mem = Arc::new(MemStore::new());
        let state = AppState {
            store: mem.clone(),
            streams: Arc::new(RwLock::new(BTreeMap::new())),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            last_seen: Arc::new(RwLock::new(BTreeMap::new())),
            node_ttl: Duration::from_secs(15),
            desired: Arc::new(RwLock::new(BTreeMap::new())),
            view: Arc::new(RwLock::new(ControllerView::default())),
        };
        (state, mem)
    }

    fn node_registration(id: &str, host: &str) -> NodeRegistration {
        NodeRegistration {
            node: NodeDescriptor {
                id: id.to_string(),
                endpoint: format!("http://{id}:8080"),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities {
                    data_plane: BTreeMap::from([(
                        weave_core::DEFAULT_DATA_PLANE_ALIAS.to_string(),
                        host.to_string(),
                    )]),
                    port_range: Some(PortRange {
                        start: 7000,
                        end: 7999,
                    }),
                    ..NodeCapabilities::default()
                },
            },
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    fn stream(name: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                network: None,
                latency: Some(200),
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                network: None,
                latency: Some(1000),
            })],
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
    async fn post_stream_then_get_returns_it_and_writes_through() {
        let (state, mem) = mem_state();
        let app = router(state);

        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, body) = send(&app, "GET", "/streams", None).await;
        assert_eq!(status, StatusCode::OK);
        let listed: Vec<StreamDefinition> = serde_json::from_value(body).unwrap();
        assert_eq!(listed, vec![stream("basic")]);

        assert_eq!(
            mem.upsert_stream_calls(),
            1,
            "stream was written through the store"
        );
        assert_eq!(mem.load_streams().await.unwrap(), vec![stream("basic")]);
    }

    #[tokio::test]
    async fn delete_stream_removes_and_writes_through() {
        let (state, mem) = mem_state();
        let app = router(state);

        send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        let (status, _) = send(&app, "DELETE", "/streams/basic", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(mem.delete_stream_calls(), 1);

        let (_, body) = send(&app, "GET", "/streams", None).await;
        let listed: Vec<StreamDefinition> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());

        let (status, _) = send(&app, "DELETE", "/streams/basic", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "second delete is a miss");
    }

    #[tokio::test]
    async fn heartbeat_updates_memory_but_never_the_store() {
        let (state, mem) = mem_state();
        let app = router(state);

        send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("strom-node-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(mem.upsert_node_calls(), 1);

        let heartbeat = NodeHeartbeat {
            node_id: "strom-node-1".to_string(),
            status: NodeStatus::Degraded,
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        };
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            mem.upsert_node_calls(),
            1,
            "heartbeat must not write through to the store"
        );

        let (_, body) = send(&app, "GET", "/state", None).await;
        let observed: ObservedState = serde_json::from_value(body).unwrap();
        assert_eq!(observed.nodes[0].status, NodeStatus::Degraded);
    }

    #[tokio::test]
    async fn desired_reflects_computed_hops_after_a_reconcile_tick() {
        let (state, _mem) = mem_state();
        {
            let mut nodes = state.nodes.write().await;
            nodes.insert(
                "strom-node-1".to_string(),
                node_registration("strom-node-1", "172.26.0.10"),
            );
            nodes.insert(
                "strom-node-2".to_string(),
                node_registration("strom-node-2", "172.27.0.10"),
            );
            state
                .streams
                .write()
                .await
                .insert("basic".to_string(), stream("basic"));
        }

        reconcile_tick(&state).await;

        let app = router(state);
        let (status, body) = send(&app, "GET", "/nodes/strom-node-1/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "sender hop placed on node 1");
        assert_eq!(hops[0].id, "weave-basic-sender");

        let (_, body) = send(&app, "GET", "/nodes/strom-node-2/desired", None).await;
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "receiver hop placed on node 2");
        assert_eq!(hops[0].id, "weave-basic-receiver-0");
    }

    #[tokio::test]
    async fn endpoints_route_pending_then_placed() {
        let (state, _mem) = mem_state();
        {
            let mut view = state.view.write().await;
            view.streams = BTreeSet::from(["basic".to_string()]);
        }
        let app = router(state);
        let (status, _) = send(&app, "GET", "/streams/basic/endpoints", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let (status, _) = send(&app, "GET", "/streams/nope/endpoints", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn mark_offline_marks_stale_preserves_fresh_and_spares_exact_ttl() {
        let mut nodes = BTreeMap::from([
            (
                "stale".to_string(),
                node_registration("stale", "172.26.0.10"),
            ),
            (
                "fresh".to_string(),
                node_registration("fresh", "172.27.0.10"),
            ),
            ("edge".to_string(), node_registration("edge", "172.28.0.10")),
        ]);
        nodes.get_mut("fresh").unwrap().node.status = NodeStatus::Degraded;

        let ttl = Duration::from_secs(15);
        let now = Instant::now();
        let last_seen = BTreeMap::from([
            ("stale".to_string(), now - Duration::from_secs(20)),
            ("fresh".to_string(), now - Duration::from_secs(5)),
            ("edge".to_string(), now - ttl),
        ]);

        mark_offline(&mut nodes, &last_seen, now, ttl);

        assert_eq!(nodes["stale"].node.status, NodeStatus::Offline);
        assert_eq!(
            nodes["fresh"].node.status,
            NodeStatus::Degraded,
            "fresh node keeps its reported status"
        );
        assert_eq!(
            nodes["edge"].node.status,
            NodeStatus::Ready,
            "age == ttl is not yet offline"
        );
    }

    #[test]
    fn reconcile_degrades_stream_when_a_hop_node_is_offline() {
        let mut nodes = BTreeMap::from([
            (
                "strom-node-1".to_string(),
                node_registration("strom-node-1", "172.26.0.10"),
            ),
            (
                "strom-node-2".to_string(),
                node_registration("strom-node-2", "172.27.0.10"),
            ),
        ]);
        nodes.get_mut("strom-node-1").unwrap().node.status = NodeStatus::Offline;
        let observed = observed_state(&nodes);

        let outcome = reconcile(vec![stream("basic")], &observed);

        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert_eq!(basic.status, PathStatus::Degraded);
        assert_eq!(basic.reason.as_deref(), Some("node strom-node-1 lost"));
        assert!(
            !outcome.desired_by_node["strom-node-1"].is_empty(),
            "desired hops for the offline node are still computed"
        );
    }

    #[tokio::test]
    async fn tick_marks_offline_node_but_still_serves_its_desired_hops() {
        let (state, _mem) = mem_state();
        {
            let mut nodes = state.nodes.write().await;
            nodes.insert(
                "strom-node-1".to_string(),
                node_registration("strom-node-1", "172.26.0.10"),
            );
            nodes.insert(
                "strom-node-2".to_string(),
                node_registration("strom-node-2", "172.27.0.10"),
            );
            state
                .streams
                .write()
                .await
                .insert("basic".to_string(), stream("basic"));
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now - Duration::from_secs(60));
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;

        let app = router(state);
        let (status, body) = send(&app, "GET", "/nodes", None).await;
        assert_eq!(status, StatusCode::OK);
        let nodes: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        let node1 = nodes.iter().find(|n| n.id == "strom-node-1").unwrap();
        assert_eq!(node1.status, NodeStatus::Offline);

        let (status, body) = send(&app, "GET", "/nodes/strom-node-1/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert!(
            !hops.is_empty(),
            "offline node still receives its desired hops"
        );
    }
}
