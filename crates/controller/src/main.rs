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
use weave_core::auth::{self, Guard, require_bearer};
use weave_core::{
    API_V1, DesiredHop, EndpointDescriptor, HopStatus, NodeDescriptor, NodeHeartbeat,
    NodeRegistration, NodeStatus, ObservedState, PROTOCOL_VERSION, PathStatus, ReconcileReport,
    ReconcileStatus, StreamDefinition, protocol_compatible,
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
    /// `None` until the first reconcile tick completes.
    report: Option<ReconcileReport>,
    streams: Vec<StreamStatus>,
    endpoints: BTreeMap<String, StreamEndpoints>,
    /// Desired hops per stream, as derived on the last tick.
    hops: BTreeMap<String, Vec<DesiredHop>>,
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
            view: Arc::new(RwLock::new(ControllerView::default())),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct StreamStatus {
    name: String,
    status: PathStatus,
    nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoints: Option<StreamEndpoints>,
}

/// Everything the dashboard renders, in one response. See [`get_view`].
#[derive(Debug, Serialize)]
struct SystemView {
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<ReconcileReport>,
    nodes: Vec<NodeView>,
    streams: Vec<StreamView>,
}

#[derive(Debug, Serialize)]
struct NodeView {
    id: String,
    status: NodeStatus,
    endpoint: String,
    data_plane: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port_range: Option<weave_core::PortRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_seen_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct StreamView {
    name: String,
    status: PathStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoints: Option<StreamEndpoints>,
    hops: Vec<HopView>,
}

/// A desired hop joined with the status its node last reported. The observed
/// fields are `None` until the node's adapter has picked the hop up.
#[derive(Debug, Serialize)]
struct HopView {
    id: String,
    node: String,
    role: weave_core::HopRole,
    ingress: SocketView,
    egresses: Vec<SocketView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<weave_core::HopState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingress_condition: Option<weave_core::LinkCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    egress_condition: Option<weave_core::LinkCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<weave_core::LinkStats>,
}

#[derive(Debug, Serialize)]
struct SocketView {
    mode: weave_core::SocketRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
}

impl From<&weave_core::SocketSpec> for SocketView {
    fn from(spec: &weave_core::SocketSpec) -> Self {
        Self {
            mode: spec.role,
            host: spec.host.clone(),
            port: spec.port,
        }
    }
}

struct ReconcileOutcome {
    report: ReconcileReport,
    streams: Vec<StreamStatus>,
    endpoints: BTreeMap<String, StreamEndpoints>,
    hops_by_stream: BTreeMap<String, Vec<DesiredHop>>,
    desired_by_node: BTreeMap<String, Vec<DesiredHop>>,
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

    // Fail closed on both surfaces: the controller owns all state, so serving it
    // open is strictly worse than refusing to start.
    let north = Guard::from_env(auth::NORTHBOUND_TOKEN_VAR)?;
    let south = Guard::from_env(auth::SOUTHBOUND_TOKEN_VAR)?;
    if north.is_disabled() {
        tracing::warn!(
            "{}=1: controller serves its API without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }

    let state = AppState::hydrate(store, node_ttl).await?;
    let api = spawn_api_server(args.listen.clone(), state.clone(), north, south);

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

    *state.desired.write().await = outcome.desired_by_node;
    let mut view = state.view.write().await;
    view.report = Some(outcome.report);
    view.streams = outcome.streams;
    view.endpoints = outcome.endpoints;
    view.hops = outcome.hops_by_stream;
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

/// The controller serves the union of both versioned contracts — northbound and
/// southbound are stateless proxies onto it — so every route either side exposes
/// is nested under [`API_V1`] here too.
///
/// It backs both surfaces, so it validates both tokens and requires the one
/// matching the surface a route belongs to — an adapter's southbound token cannot
/// create streams.
///
/// Unversioned by design: `/health`, which compose healthchecks and load
/// balancers address directly, and the dashboard (`/`, `/ui`, `/view`), which
/// ships inside this binary. `/view` carries no stability guarantee.
///
/// The dashboard is also deliberately left open, as is the `/v1/status` rollup it
/// shares its data with: both are browser-reachable, and a bearer token cannot
/// travel with a page load without a cookie/session mechanism or a reverse proxy.
/// They expose topology and allocated ports, so **the controller port must not be
/// publicly exposed** — put it behind a proxy or keep it on a private network.
fn router(state: AppState, north: Guard, south: Guard) -> Router {
    let streams = Router::new()
        .route("/streams", get(list_streams).post(submit_stream))
        .route("/streams/{name}", axum::routing::delete(delete_stream))
        .route("/streams/{name}/endpoints", get(get_endpoints))
        .layer(axum::middleware::from_fn_with_state(north, require_bearer));

    let nodes = Router::new()
        .route("/nodes", get(list_nodes))
        .route("/nodes/register", post(register_node))
        .route("/nodes/{node_id}/heartbeat", post(node_heartbeat))
        .route("/nodes/{node_id}/desired", get(get_desired))
        .route("/endpoints", get(list_endpoints))
        .route("/state", get(get_state))
        .layer(axum::middleware::from_fn_with_state(south, require_bearer));

    // `/status` is part of the operator contract — it is a scriptable rollup, not
    // a dashboard detail — so it is versioned, but unauthenticated like `/view`.
    let v1 = Router::new()
        .route("/status", get(get_status))
        .merge(streams)
        .merge(nodes);

    Router::new()
        .route("/", get(ui))
        .route("/ui", get(ui))
        .route("/health", get(health))
        .route("/view", get(get_view))
        .nest(API_V1, v1)
        .with_state(state)
}

fn spawn_api_server(
    addr: String,
    state: AppState,
    north: Guard,
    south: Guard,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = router(state, north, south);
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
    let view = state.view.read().await;
    Json(match &view.report {
        Some(report) => json!({
            "status": report.status,
            "summary": report.summary,
            "streams": view.streams,
        }),
        None => json!({ "status": "starting" }),
    })
}

/// One UI-shaped document describing the whole system: the last reconcile
/// report, every registered node, and every stream with its desired hops
/// merged against the hop status the nodes report. This is what `/ui` polls.
async fn get_view(State(state): State<AppState>) -> Json<SystemView> {
    let nodes = state.nodes.read().await;
    let last_seen = state.last_seen.read().await;
    let view = state.view.read().await;
    let now = Instant::now();

    let observed: Vec<&HopStatus> = nodes.values().flat_map(|r| &r.hop_status).collect();

    let node_views = nodes
        .values()
        .map(|r| NodeView {
            id: r.node.id.clone(),
            status: r.node.status,
            endpoint: r.node.endpoint.clone(),
            data_plane: r.node.capabilities.data_plane.clone(),
            port_range: r.node.capabilities.port_range,
            last_seen_secs: last_seen
                .get(&r.node.id)
                .map(|seen| now.saturating_duration_since(*seen).as_secs()),
        })
        .collect();

    let streams = view
        .streams
        .iter()
        .map(|stream| StreamView {
            name: stream.name.clone(),
            status: stream.status,
            reason: stream.reason.clone(),
            nodes: stream.nodes.clone(),
            endpoints: stream.endpoints.clone(),
            hops: view
                .hops
                .get(&stream.name)
                .into_iter()
                .flatten()
                .map(|hop| {
                    let status = observed
                        .iter()
                        .find(|s| s.id == hop.id && s.node_id == hop.node_id);
                    HopView {
                        id: hop.id.clone(),
                        node: hop.node_id.clone(),
                        role: hop.role,
                        ingress: SocketView::from(&hop.ingress),
                        egresses: hop.egresses.iter().map(SocketView::from).collect(),
                        state: status.map(|s| s.state),
                        ingress_condition: status.map(|s| s.ingress),
                        egress_condition: status.map(|s| s.egress),
                        stats: status.and_then(|s| s.stats),
                    }
                })
                .collect(),
        })
        .collect();

    Json(SystemView {
        report: view.report.clone(),
        nodes: node_views,
        streams,
    })
}

/// The embedded single-file dashboard. It polls [`get_view`] and needs no
/// build step or external assets.
async fn ui() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("ui.html"))
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
    } else if view.streams.iter().any(|s| s.name == name) {
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

/// Registration is also the version handshake: an adapter declaring a protocol
/// this controller does not speak is turned away here rather than accepted and
/// then served desired state it cannot realise.
async fn register_node(
    State(state): State<AppState>,
    Json(registration): Json<NodeRegistration>,
) -> Response {
    let node_id = registration.node.id.clone();
    let endpoint_count = registration.endpoints.len();

    if !protocol_compatible(registration.protocol_version) {
        tracing::warn!(
            %node_id,
            reported = registration.protocol_version,
            supported = PROTOCOL_VERSION,
            "rejecting node registration: incompatible southbound protocol version"
        );
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "incompatible southbound protocol version",
                "node_id": node_id,
                "reported_protocol_version": registration.protocol_version,
                "supported_protocol_version": PROTOCOL_VERSION,
            })),
        )
            .into_response();
    }

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
    let mut hops_by_stream: BTreeMap<String, Vec<DesiredHop>> = BTreeMap::new();
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
                hops_by_stream.insert(stream.name.clone(), path.hops.clone());
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
        hops_by_stream,
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

    const NORTH_TOKEN: &str = "controller-north-test-token";
    const SOUTH_TOKEN: &str = "controller-south-test-token";

    /// A router with authentication switched off, for the behavioural tests.
    fn open_router(state: AppState) -> Router {
        router(state, Guard::Disabled, Guard::Disabled)
    }

    /// A router requiring a distinct token per surface.
    fn guarded_router(state: AppState) -> Router {
        router(
            state,
            Guard::Required(auth::Token::new(NORTH_TOKEN).unwrap()),
            Guard::Required(auth::Token::new(SOUTH_TOKEN).unwrap()),
        )
    }

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
            protocol_version: PROTOCOL_VERSION,
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
        let app = open_router(state);

        let (status, _) = send(
            &app,
            "POST",
            "/v1/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, body) = send(&app, "GET", "/v1/streams", None).await;
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
        let app = open_router(state);

        send(
            &app,
            "POST",
            "/v1/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        let (status, _) = send(&app, "DELETE", "/v1/streams/basic", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(mem.delete_stream_calls(), 1);

        let (_, body) = send(&app, "GET", "/v1/streams", None).await;
        let listed: Vec<StreamDefinition> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());

        let (status, _) = send(&app, "DELETE", "/v1/streams/basic", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "second delete is a miss");
    }

    #[tokio::test]
    async fn heartbeat_updates_memory_but_never_the_store() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        send(
            &app,
            "POST",
            "/v1/nodes/register",
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
            "/v1/nodes/strom-node-1/heartbeat",
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            mem.upsert_node_calls(),
            1,
            "heartbeat must not write through to the store"
        );

        let (_, body) = send(&app, "GET", "/v1/state", None).await;
        let observed: ObservedState = serde_json::from_value(body).unwrap();
        assert_eq!(observed.nodes[0].status, NodeStatus::Degraded);
    }

    /// A stale adapter is turned away at the handshake, and nothing about it is
    /// recorded — a registration the controller cannot serve is worse than none.
    #[tokio::test]
    async fn registration_with_an_incompatible_protocol_version_is_rejected() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        for reported in [0, PROTOCOL_VERSION + 1] {
            let mut registration = node_registration("strom-node-1", "172.26.0.10");
            registration.protocol_version = reported;

            let (status, body) = send(
                &app,
                "POST",
                "/v1/nodes/register",
                Some(serde_json::to_value(&registration).unwrap()),
            )
            .await;

            assert_eq!(status, StatusCode::CONFLICT, "reported version {reported}");
            assert_eq!(body["node_id"], "strom-node-1");
            assert_eq!(body["reported_protocol_version"], reported);
            assert_eq!(body["supported_protocol_version"], PROTOCOL_VERSION);
        }

        assert_eq!(
            mem.upsert_node_calls(),
            0,
            "a rejected node is never persisted"
        );
        let (_, body) = send(&app, "GET", "/v1/nodes", None).await;
        assert_eq!(body, json!([]), "a rejected node is never registered");
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

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/v1/nodes/strom-node-1/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "sender hop placed on node 1");
        assert_eq!(hops[0].id, "weave-basic-sender");

        let (_, body) = send(&app, "GET", "/v1/nodes/strom-node-2/desired", None).await;
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "receiver hop placed on node 2");
        assert_eq!(hops[0].id, "weave-basic-receiver-0");
    }

    #[tokio::test]
    async fn endpoints_route_pending_then_placed() {
        let (state, _mem) = mem_state();
        {
            let mut view = state.view.write().await;
            view.streams = vec![StreamStatus {
                name: "basic".to_string(),
                status: PathStatus::Pending,
                nodes: Vec::new(),
                reason: None,
                endpoints: None,
            }];
        }
        let app = open_router(state);
        let (status, _) = send(&app, "GET", "/v1/streams/basic/endpoints", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let (status, _) = send(&app, "GET", "/v1/streams/nope/endpoints", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn view_joins_desired_hops_with_reported_status() {
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
            seen.insert("strom-node-1".to_string(), now);
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;

        // Node 1 reports its sender hop; node 2 has not picked its hop up yet.
        state
            .nodes
            .write()
            .await
            .get_mut("strom-node-1")
            .unwrap()
            .hop_status = vec![weave_core::HopStatus {
            id: "weave-basic-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            state: weave_core::HopState::Provisioned,
            ingress: weave_core::LinkCondition::Flowing,
            egress: weave_core::LinkCondition::Connected,
            resolved_ingress: None,
            resolved_egress: None,
            stats: Some(weave_core::LinkStats {
                ingress_rate_mbps: 3.2,
                ..weave_core::LinkStats::default()
            }),
        }];

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/view", None).await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(body["report"]["status"], "converging");
        assert_eq!(body["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(body["nodes"][0]["last_seen_secs"], 0);

        let basic = &body["streams"][0];
        assert_eq!(basic["name"], "basic");
        let hops = basic["hops"].as_array().unwrap();
        assert_eq!(hops.len(), 2, "sender + receiver");

        let sender = &hops[0];
        assert_eq!(sender["id"], "weave-basic-sender");
        assert_eq!(sender["node"], "strom-node-1");
        assert_eq!(sender["state"], "provisioned");
        assert_eq!(sender["ingress_condition"], "flowing");
        assert_eq!(sender["egress_condition"], "connected");
        assert_eq!(sender["stats"]["ingress_rate_mbps"], 3.2);
        assert_eq!(sender["ingress"]["mode"], "listen");
        assert_eq!(sender["egresses"][0]["mode"], "connect");
        assert_eq!(sender["egresses"][0]["host"], "172.27.0.10");

        let receiver = &hops[1];
        assert_eq!(receiver["node"], "strom-node-2");
        assert!(
            receiver.get("state").is_none(),
            "unreported hop carries no observed fields"
        );
    }

    #[tokio::test]
    async fn view_before_first_tick_has_no_report() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/view", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("report").is_none());
        assert_eq!(body["streams"], json!([]));
    }

    #[tokio::test]
    async fn ui_is_served_at_root_and_ui() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        for uri in ["/", "/ui"] {
            let request = Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let content_type = response.headers()["content-type"].to_str().unwrap();
            assert!(content_type.starts_with("text/html"), "{content_type}");
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            assert!(String::from_utf8_lossy(&bytes).contains("open-weave"));
        }
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

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/v1/nodes", None).await;
        assert_eq!(status, StatusCode::OK);
        let nodes: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        let node1 = nodes.iter().find(|n| n.id == "strom-node-1").unwrap();
        assert_eq!(node1.status, NodeStatus::Offline);

        let (status, body) = send(&app, "GET", "/v1/nodes/strom-node-1/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert!(
            !hops.is_empty(),
            "offline node still receives its desired hops"
        );
    }

    /// Send a request carrying an optional `Authorization` header, returning the
    /// status and the `WWW-Authenticate` challenge if one was issued.
    async fn send_auth(
        app: &Router,
        method: &str,
        uri: &str,
        authorization: Option<&str>,
    ) -> (StatusCode, Option<String>) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(authorization) = authorization {
            request = request.header("authorization", authorization);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from("{}")).unwrap())
            .await
            .unwrap();
        let challenge = response
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .map(|value| value.to_str().unwrap().to_string());
        (response.status(), challenge)
    }

    const NORTH_ROUTES: [(&str, &str); 4] = [
        ("GET", "/v1/streams"),
        ("POST", "/v1/streams"),
        ("DELETE", "/v1/streams/basic"),
        ("GET", "/v1/streams/basic/endpoints"),
    ];

    const SOUTH_ROUTES: [(&str, &str); 6] = [
        ("GET", "/v1/nodes"),
        ("POST", "/v1/nodes/register"),
        ("POST", "/v1/nodes/strom-node-1/heartbeat"),
        ("GET", "/v1/nodes/strom-node-1/desired"),
        ("GET", "/v1/endpoints"),
        ("GET", "/v1/state"),
    ];

    /// The prefix is a clean break, not an alias: the paths this service used to
    /// serve are gone, so a client that never moved fails loudly.
    #[tokio::test]
    async fn unversioned_api_paths_are_not_served() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        for (method, uri) in NORTH_ROUTES.iter().chain(&SOUTH_ROUTES) {
            let unversioned = uri.strip_prefix(API_V1).expect("route is versioned");
            let (status, _) = send(&app, method, unversioned, None).await;
            assert_eq!(
                status,
                StatusCode::NOT_FOUND,
                "{method} {unversioned} must not be served alongside {uri}"
            );
        }
        let (status, _) = send(&app, "GET", "/status", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// The dashboard surface is deliberately unauthenticated — it is browser-
    /// loaded and cannot carry a bearer token. `/health` is open for healthchecks.
    #[tokio::test]
    async fn dashboard_and_health_stay_open() {
        let (state, _mem) = mem_state();
        let app = guarded_router(state);
        for uri in ["/", "/ui", "/health", "/view", "/v1/status"] {
            let (status, _) = send_auth(&app, "GET", uri, None).await;
            assert_eq!(status, StatusCode::OK, "{uri} must not require a token");
        }
    }

    #[tokio::test]
    async fn api_routes_reject_missing_and_wrong_tokens() {
        let (state, _mem) = mem_state();
        let app = guarded_router(state);

        for authorization in [None, Some("Bearer wrong-token"), Some("Basic ignored")] {
            for (method, uri) in NORTH_ROUTES.iter().chain(&SOUTH_ROUTES) {
                let (status, challenge) = send_auth(&app, method, uri, authorization).await;
                assert_eq!(
                    status,
                    StatusCode::UNAUTHORIZED,
                    "{method} {uri} with authorization={authorization:?}"
                );
                assert_eq!(challenge.as_deref(), Some("Bearer"));
            }
        }
    }

    /// The surfaces are separated, not merely authenticated: a node's southbound
    /// token cannot create or delete streams, and the operator token cannot
    /// register nodes or read their desired hops.
    #[tokio::test]
    async fn each_surface_rejects_the_other_surfaces_token() {
        let (state, _mem) = mem_state();
        let app = guarded_router(state);
        let north = format!("Bearer {NORTH_TOKEN}");
        let south = format!("Bearer {SOUTH_TOKEN}");

        for (method, uri) in NORTH_ROUTES {
            let (status, _) = send_auth(&app, method, uri, Some(&south)).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must reject the southbound token"
            );
        }
        for (method, uri) in SOUTH_ROUTES {
            let (status, _) = send_auth(&app, method, uri, Some(&north)).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must reject the northbound token"
            );
        }
    }

    #[tokio::test]
    async fn each_surface_accepts_its_own_token() {
        let (state, _mem) = mem_state();
        let app = guarded_router(state);

        // Past the guard is enough: these are covered behaviourally elsewhere, so
        // only "not 401" matters here.
        let (status, _) = send_auth(
            &app,
            "GET",
            "/v1/streams",
            Some(&format!("Bearer {NORTH_TOKEN}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) = send_auth(
            &app,
            "GET",
            "/v1/state",
            Some(&format!("Bearer {SOUTH_TOKEN}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}
