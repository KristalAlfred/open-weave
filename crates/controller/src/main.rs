//! `weave-controller` — the single stateful control-plane service. It owns the
//! stream and node registries (persisted to Postgres), serves the northbound and
//! southbound HTTP surfaces, and reconciles desired streams into per-node desired
//! hops on a fixed interval, entirely from in-memory state.

mod path;
mod store;
mod webhook;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
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
use weave_core::webhook::{EventType, NodeSummary};
use weave_core::{
    API_PREFIX, AcceptedState, ApiError, ApiErrorCode, DesiredHop, EndpointDescriptor, HopStatus,
    NodeAccepted, NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus, ObservedState,
    PROTOCOL_VERSION, PathStatus, PlanStatus, ROUTE_ENDPOINTS, ROUTE_NODE_DESIRED,
    ROUTE_NODE_HEARTBEAT, ROUTE_NODE_REGISTER, ROUTE_NODES, ROUTE_STATE, ROUTE_STATUS,
    ROUTE_STREAM, ROUTE_STREAM_ENDPOINTS, ROUTE_STREAM_PLANS, ROUTE_STREAMS, ReconcileReport,
    ReconcileStatus, RunningStatus, StartingState, StartingStatus, StatusResponse, StreamAccepted,
    StreamCondition, StreamConditionReason, StreamConditionStatus, StreamConditionType,
    StreamDefinition, StreamEndpoints, StreamPlan, StreamResource, StreamStatus, ValidationIssue,
    protocol_compatible, resource_id_issue, validate_resource_id, validate_stream,
};

use path::{PlacementError, PortAllocator, derive_path, path_status, stream_endpoints};
use store::{MemStore, PgStore, StateStore, StoreError, StoredStream};

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
    /// Absolute URL that receives node lifecycle events. Webhooks are off when unset.
    #[arg(long, env = "WEAVE_WEBHOOK_URL")]
    webhook_url: Option<String>,
    /// Presented to the receiver as `Authorization: Bearer <token>`.
    #[arg(long, env = "WEAVE_WEBHOOK_TOKEN")]
    webhook_token: Option<String>,
    /// Event types to deliver, comma-separated. Defaults to all of them.
    #[arg(long, env = "WEAVE_WEBHOOK_EVENTS", value_delimiter = ',')]
    webhook_events: Vec<String>,
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
    streams: Arc<RwLock<BTreeMap<String, StoredStream>>>,
    nodes: Arc<RwLock<BTreeMap<String, NodeRegistration>>>,
    last_seen: Arc<RwLock<BTreeMap<String, Instant>>>,
    node_ttl: Duration,
    desired: Arc<RwLock<BTreeMap<String, Vec<DesiredHop>>>>,
    view: Arc<RwLock<ControllerView>>,
    /// `None` when no receiver is configured; every emit site is then a no-op.
    webhooks: Option<Arc<webhook::Emitter>>,
}

impl AppState {
    async fn hydrate(
        store: Arc<dyn StateStore>,
        node_ttl: Duration,
        webhooks: Option<Arc<webhook::Emitter>>,
    ) -> Result<Self> {
        let loaded_streams = store.load_streams().await.context("hydrating streams")?;
        for stream in &loaded_streams {
            if let Some(issue) = validate_stream(&stream.spec).into_iter().next() {
                anyhow::bail!(
                    "stored stream {:?} is invalid at {}: {}",
                    stream.spec.name,
                    issue.field,
                    issue.message
                );
            }
        }
        let streams = loaded_streams
            .into_iter()
            .map(|stream| (stream.spec.name.clone(), stream))
            .collect();
        let nodes = store
            .load_nodes()
            .await
            .context("hydrating nodes")?
            .into_iter()
            .filter(|registration| {
                if let Err(error) = validate_resource_id(&registration.node.id) {
                    tracing::warn!(
                        node_id = %registration.node.id,
                        %error,
                        "dropping a stored registration with an invalid node id; the node must re-register with a valid id"
                    );
                    false
                } else {
                    true
                }
            })
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
            webhooks,
        })
    }

    fn emit(&self, event_type: EventType, node: NodeSummary) {
        if let Some(emitter) = &self.webhooks {
            emitter.emit(event_type, node);
        }
    }
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
    data_plane: BTreeMap<String, weave_core::DataPlaneAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port_range: Option<weave_core::PortRange>,
    /// Whether the planner may draw this node as transit for other nodes' streams.
    relay: bool,
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
    egresses: Vec<EgressView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<weave_core::HopState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingress_condition: Option<weave_core::LinkCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingress_stats: Option<weave_core::LinkStats>,
}

#[derive(Debug, Serialize)]
struct EgressView {
    branch_id: String,
    #[serde(flatten)]
    socket: SocketView,
    #[serde(skip_serializing_if = "Option::is_none")]
    condition: Option<weave_core::LinkCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved: Option<weave_core::ResolvedAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<weave_core::LinkStats>,
}

/// One socket as the dashboard reads it: every transport's fields flattened
/// into one object, with the ones this socket does not carry left out.
#[derive(Debug, Serialize)]
struct SocketView {
    transport: &'static str,
    mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}

impl From<&weave_core::SocketSpec> for SocketView {
    fn from(spec: &weave_core::SocketSpec) -> Self {
        use weave_core::{DEVICE_TRANSPORT, SocketSpec, SrtSocket, Transport};

        let (transport, mode, host, port, url) = match spec {
            SocketSpec::Srt(socket) => (
                Transport::Srt.name(),
                socket.role().name(),
                match socket {
                    SrtSocket::Connect { host, .. } => Some(host.clone()),
                    SrtSocket::Listen { .. } => None,
                },
                Some(socket.port()),
                None,
            ),
            SocketSpec::Whip(socket) => (
                Transport::Whip.name(),
                socket.role.name(),
                None,
                None,
                Some(socket.url.clone()),
            ),
            SocketSpec::Whep(socket) => (
                Transport::Whep.name(),
                socket.role.name(),
                None,
                None,
                Some(socket.url.clone()),
            ),
            SocketSpec::Device(kind) => (DEVICE_TRANSPORT, kind.name(), None, None, None),
        };

        Self {
            transport,
            mode,
            host,
            port,
            url,
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

    let north = Guard::from_env(auth::NORTHBOUND_TOKEN_VAR)?;
    let south = Guard::from_env(auth::SOUTHBOUND_TOKEN_VAR)?;
    if north.is_disabled() {
        tracing::warn!(
            "{}=1: controller serves its API without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }

    let webhooks = webhook::Emitter::new(webhook::Config {
        url: args.webhook_url.clone(),
        token: args.webhook_token.clone(),
        events: args.webhook_events.clone(),
        ..webhook::Config::default()
    })
    .map(Arc::new);

    let state = AppState::hydrate(store, node_ttl, webhooks).await?;
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
    let streams = state.streams.read().await;
    let definitions = streams.values().map(|stream| stream.spec.clone()).collect();
    let (observed, went_offline) = {
        let mut nodes = state.nodes.write().await;
        let last_seen = state.last_seen.read().await;
        let transitioned = mark_offline(&mut nodes, &last_seen, Instant::now(), state.node_ttl);
        let summaries: Vec<NodeSummary> = transitioned
            .iter()
            .filter_map(|id| nodes.get(id))
            .map(|registration| NodeSummary::from(&registration.node))
            .collect();
        (observed_state(&nodes), summaries)
    };
    for node in went_offline {
        state.emit(EventType::NodeOffline, node);
    }
    let mut outcome = reconcile(definitions, &observed);
    for status in &mut outcome.streams {
        let stored = streams
            .get(&status.name)
            .expect("reconcile returns every submitted stream");
        status.generation = stored.generation;
        status.observed_generation = Some(stored.generation);
    }

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
    stamp_condition_transition_times(&mut outcome.streams, &view.streams, &now_rfc3339());
    view.report = Some(outcome.report);
    view.streams = outcome.streams;
    view.endpoints = outcome.endpoints;
    view.hops = outcome.hops_by_stream;
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn stamp_condition_transition_times(
    current: &mut [StreamStatus],
    previous: &[StreamStatus],
    now: &str,
) {
    for stream in current {
        let previous = previous
            .iter()
            .find(|candidate| candidate.name == stream.name);
        for condition in &mut stream.conditions {
            condition.last_transition_time = previous
                .and_then(|stream| {
                    stream.conditions.iter().find(|candidate| {
                        candidate.condition_type == condition.condition_type
                            && candidate.status == condition.status
                    })
                })
                .map_or_else(
                    || now.to_string(),
                    |condition| condition.last_transition_time.clone(),
                );
        }
    }
}

async fn update_pending_generation(state: &AppState, stored: &StoredStream) {
    let mut view = state.view.write().await;
    if let Some(status) = view
        .streams
        .iter_mut()
        .find(|status| status.name == stored.spec.name)
    {
        status.generation = stored.generation;
        return;
    }
    let now = now_rfc3339();
    let conditions = [
        StreamConditionType::PlacementReady,
        StreamConditionType::NodesAvailable,
        StreamConditionType::HopsReady,
        StreamConditionType::FormatCompatible,
        StreamConditionType::MediaFlowing,
    ]
    .into_iter()
    .map(|condition_type| StreamCondition {
        condition_type,
        status: StreamConditionStatus::Unknown,
        reason: StreamConditionReason::NotReady,
        detail: "the controller has not reconciled this generation".to_string(),
        last_transition_time: now.clone(),
    })
    .collect();
    view.streams.push(StreamStatus {
        name: stored.spec.name.clone(),
        generation: stored.generation,
        observed_generation: None,
        status: PathStatus::Pending,
        nodes: Vec::new(),
        conditions,
        endpoints: None,
    });
    view.streams
        .sort_by(|left, right| left.name.cmp(&right.name));
}

async fn remove_stream_view(state: &AppState, name: &str) {
    let mut view = state.view.write().await;
    view.streams.retain(|status| status.name != name);
    view.endpoints.remove(name);
    view.hops.remove(name);
}

/// Mark nodes whose last heartbeat is older than `ttl` as [`NodeStatus::Offline`],
/// returning the ids that changed. Nodes seen within the TTL keep their reported
/// status. Pure: the caller supplies `now`, so the boundary is testable without a
/// clock, and the caller — not this — emits for the transitions.
fn mark_offline(
    nodes: &mut BTreeMap<String, NodeRegistration>,
    last_seen: &BTreeMap<String, Instant>,
    now: Instant,
    ttl: Duration,
) -> Vec<String> {
    let mut transitioned = Vec::new();
    for (id, registration) in nodes.iter_mut() {
        if registration.node.status != NodeStatus::Offline
            && let Some(seen) = last_seen.get(id)
            && now.saturating_duration_since(*seen) > ttl
        {
            registration.node.status = NodeStatus::Offline;
            transitioned.push(id.clone());
        }
    }
    transitioned
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
/// is nested under [`API_PREFIX`] here too.
///
/// It backs both surfaces, so it validates both tokens and requires the one
/// matching the surface a route belongs to — an adapter's southbound token cannot
/// create streams.
///
/// Unversioned: `/health`, which compose healthchecks and load balancers address
/// directly, and the dashboard (`/`, `/ui`, `/view`), which ships inside this
/// binary. `/view` carries no stability guarantee.
///
/// The dashboard is unauthenticated, as is the `/v5/status` rollup it shares its
/// data with: both are browser-reachable, and a bearer token cannot travel with a
/// page load without a cookie/session mechanism or a reverse proxy.
/// They expose topology and allocated ports, so **the controller port must not be
/// publicly exposed** — put it behind a proxy or keep it on a private network.
fn router(state: AppState, north: Guard, south: Guard) -> Router {
    let streams = Router::new()
        .route(ROUTE_STREAMS, get(list_streams).post(submit_stream))
        .route(ROUTE_STREAM, get(get_stream).delete(delete_stream))
        .route(ROUTE_STREAM_ENDPOINTS, get(get_endpoints))
        .route(ROUTE_STREAM_PLANS, post(plan_stream))
        .layer(axum::middleware::from_fn_with_state(north, require_bearer));

    let nodes = Router::new()
        .route(ROUTE_NODES, get(list_nodes))
        .route(ROUTE_NODE_REGISTER, post(register_node))
        .route(ROUTE_NODE_HEARTBEAT, post(node_heartbeat))
        .route(ROUTE_NODE_DESIRED, get(get_desired))
        .route(ROUTE_ENDPOINTS, get(list_endpoints))
        .route(ROUTE_STATE, get(get_state))
        .layer(axum::middleware::from_fn_with_state(south, require_bearer));

    // `/status` is versioned as part of the operator contract, but unauthenticated
    // like `/view`.
    let api = Router::new()
        .route(ROUTE_STATUS, get(get_status))
        .merge(streams)
        .merge(nodes)
        .fallback(api_route_not_found)
        .method_not_allowed_fallback(api_method_not_allowed);

    Router::new()
        .route("/", get(ui))
        .route("/ui", get(ui))
        .route("/health", get(health))
        .route("/view", get(get_view))
        .nest(API_PREFIX, api)
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

async fn api_route_not_found() -> Response {
    error(
        StatusCode::NOT_FOUND,
        ApiErrorCode::RouteNotFound,
        "API route not found",
    )
}

async fn api_method_not_allowed() -> Response {
    error(
        StatusCode::METHOD_NOT_ALLOWED,
        ApiErrorCode::MethodNotAllowed,
        "method not allowed",
    )
}

async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let view = state.view.read().await;
    Json(match &view.report {
        Some(report) => StatusResponse::Running(RunningStatus {
            status: report.status,
            summary: report.summary.clone(),
            streams: view.streams.clone(),
        }),
        None => StatusResponse::Starting(StartingStatus {
            status: StartingState::Starting,
        }),
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
            relay: r.node.capabilities.relay,
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
            reason: stream
                .conditions
                .iter()
                .find(|condition| condition.status == StreamConditionStatus::False)
                .map(|condition| condition.detail.clone()),
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
                        egresses: hop
                            .egresses
                            .iter()
                            .map(|egress| {
                                let observed = status.and_then(|status| {
                                    status
                                        .egresses
                                        .iter()
                                        .find(|reported| reported.branch_id == egress.branch_id)
                                });
                                EgressView {
                                    branch_id: egress.branch_id.clone(),
                                    socket: SocketView::from(&egress.socket),
                                    condition: observed.map(|reported| reported.status.condition),
                                    resolved: observed
                                        .and_then(|reported| reported.status.resolved.clone()),
                                    stats: observed.and_then(|reported| reported.status.stats),
                                }
                            })
                            .collect(),
                        state: status.map(|s| s.state),
                        ingress_condition: status.map(|s| s.ingress.condition),
                        ingress_stats: status.and_then(|s| s.ingress.stats),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamWritePrecondition {
    Absent,
    Revision(u64),
}

struct RequestError {
    status: StatusCode,
    code: ApiErrorCode,
    message: &'static str,
}

impl RequestError {
    fn response(self) -> Response {
        error(self.status, self.code, self.message)
    }
}

fn stream_resource(stream: &StoredStream) -> StreamResource {
    StreamResource {
        generation: stream.generation,
        spec: stream.spec.clone(),
    }
}

fn revision_etag(revision: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("\"revision-{revision}\""))
        .expect("numeric revision always forms a valid ETag")
}

fn with_etag(mut response: Response, revision: u64) -> Response {
    response
        .headers_mut()
        .insert(header::ETAG, revision_etag(revision));
    response
}

fn parse_revision(value: &HeaderValue) -> Option<u64> {
    value
        .to_str()
        .ok()?
        .strip_prefix("\"revision-")?
        .strip_suffix('"')?
        .parse()
        .ok()
}

fn required_revision(headers: &HeaderMap) -> Result<u64, RequestError> {
    let Some(value) = headers.get(header::IF_MATCH) else {
        return Err(RequestError {
            status: StatusCode::PRECONDITION_REQUIRED,
            code: ApiErrorCode::PreconditionRequired,
            message: "If-Match is required",
        });
    };
    parse_revision(value).ok_or(RequestError {
        status: StatusCode::BAD_REQUEST,
        code: ApiErrorCode::InvalidRequest,
        message: "If-Match must contain one current stream ETag",
    })
}

fn stream_write_precondition(headers: &HeaderMap) -> Result<StreamWritePrecondition, RequestError> {
    match (
        headers.get(header::IF_MATCH),
        headers.get(header::IF_NONE_MATCH),
    ) {
        (None, None) => Err(RequestError {
            status: StatusCode::PRECONDITION_REQUIRED,
            code: ApiErrorCode::PreconditionRequired,
            message: "If-Match or If-None-Match is required",
        }),
        (Some(_), Some(_)) => Err(RequestError {
            status: StatusCode::BAD_REQUEST,
            code: ApiErrorCode::InvalidRequest,
            message: "send either If-Match or If-None-Match, not both",
        }),
        (Some(value), None) => parse_revision(value)
            .map(StreamWritePrecondition::Revision)
            .ok_or(RequestError {
                status: StatusCode::BAD_REQUEST,
                code: ApiErrorCode::InvalidRequest,
                message: "If-Match must contain one current stream ETag",
            }),
        (None, Some(value)) if value == "*" => Ok(StreamWritePrecondition::Absent),
        (None, Some(_)) => Err(RequestError {
            status: StatusCode::BAD_REQUEST,
            code: ApiErrorCode::InvalidRequest,
            message: "If-None-Match must be * when creating a stream",
        }),
    }
}

async fn list_streams(State(state): State<AppState>) -> Json<Vec<StreamResource>> {
    Json(
        state
            .streams
            .read()
            .await
            .values()
            .map(stream_resource)
            .collect(),
    )
}

async fn get_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    match state.streams.read().await.get(&name).cloned() {
        Some(stream) => with_etag(
            Json(stream_resource(&stream)).into_response(),
            stream.revision,
        ),
        None => error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::StreamNotFound,
            "stream not found",
        ),
    }
}

async fn submit_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<StreamDefinition>, JsonRejection>,
) -> Response {
    let Json(stream) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    let issues = validate_stream(&stream);
    if !issues.is_empty() {
        return invalid_request("stream validation failed", issues);
    }
    let precondition = match stream_write_precondition(&headers) {
        Ok(precondition) => precondition,
        Err(error) => return error.response(),
    };
    let name = stream.name.clone();
    let (stored, changed) = {
        let mut streams = state.streams.write().await;
        let changed = streams
            .get(&name)
            .is_none_or(|current| current.spec != stream);
        let result = match precondition {
            StreamWritePrecondition::Absent => state.store.create_stream(&stream).await,
            StreamWritePrecondition::Revision(revision) => {
                state.store.update_stream(&stream, revision).await
            }
        };
        let stored = match result {
            Ok(stored) => stored,
            Err(StoreError::PreconditionFailed) => {
                return error(
                    StatusCode::PRECONDITION_FAILED,
                    ApiErrorCode::PreconditionFailed,
                    "stream changed or the requested create name already exists",
                );
            }
            Err(err) => {
                tracing::error!(%err, %name, "persisting stream failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::PersistenceFailed,
                    "failed to persist stream",
                );
            }
        };
        streams.insert(name.clone(), stored.clone());
        (stored, changed)
    };
    update_pending_generation(&state, &stored).await;
    tracing::info!(%name, "stream accepted");
    with_etag(
        (
            StatusCode::ACCEPTED,
            Json(StreamAccepted {
                status: AcceptedState::Accepted,
                name,
                generation: stored.generation,
                changed,
            }),
        )
            .into_response(),
        stored.revision,
    )
}

async fn plan_stream(
    State(state): State<AppState>,
    payload: Result<Json<StreamDefinition>, JsonRejection>,
) -> Response {
    let Json(stream) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    let issues = validate_stream(&stream);
    if !issues.is_empty() {
        return invalid_request("stream validation failed", issues);
    }

    let mut streams: BTreeMap<String, StreamDefinition> = state
        .streams
        .read()
        .await
        .iter()
        .map(|(name, stream)| (name.clone(), stream.spec.clone()))
        .collect();
    streams.insert(stream.name.clone(), stream.clone());
    let nodes = state.nodes.read().await;
    let mut observed = observed_state(&nodes);
    drop(nodes);
    observed.hops.clear();
    let mut outcome = reconcile(streams.into_values().collect(), &observed);
    let planned = outcome
        .streams
        .into_iter()
        .find(|status| status.name == stream.name)
        .expect("candidate stream is included in plan");
    let hops = outcome
        .hops_by_stream
        .remove(&stream.name)
        .unwrap_or_default();
    let status = if !stream.enabled {
        PlanStatus::Disabled
    } else if hops.is_empty() {
        PlanStatus::Unplaced
    } else {
        PlanStatus::Placed
    };

    Json(StreamPlan {
        name: planned.name,
        status,
        nodes: planned.nodes,
        hops,
        endpoints: planned.endpoints,
        reason: (status == PlanStatus::Unplaced)
            .then(|| {
                planned
                    .conditions
                    .iter()
                    .find(|condition| {
                        condition.condition_type == StreamConditionType::PlacementReady
                    })
                    .map(|condition| condition.detail.clone())
            })
            .flatten(),
    })
    .into_response()
}

async fn delete_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    let revision = match required_revision(&headers) {
        Ok(revision) => revision,
        Err(error) => return error.response(),
    };
    let mut streams = state.streams.write().await;
    match state.store.delete_stream(&name, revision).await {
        Ok(()) => {}
        Err(StoreError::PreconditionFailed) => {
            return error(
                StatusCode::PRECONDITION_FAILED,
                ApiErrorCode::PreconditionFailed,
                "stream changed or no longer exists",
            );
        }
        Err(err) => {
            tracing::error!(%err, %name, "deleting stream failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::PersistenceFailed,
                "failed to delete stream",
            );
        }
    }
    streams.remove(&name);
    drop(streams);
    remove_stream_view(&state, &name).await;
    tracing::info!(%name, "stream deleted");
    StatusCode::NO_CONTENT.into_response()
}

/// Concrete `srt://` endpoints for a placed stream: `200` when placed, `503` when
/// the stream is known but not yet placed, `404` when unknown. A `device` end
/// has nothing to dial and reads `null` in its place.
async fn get_endpoints(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    let view = state.view.read().await;
    if let Some(endpoints) = view.endpoints.get(&name) {
        (StatusCode::OK, Json(endpoints)).into_response()
    } else if view.streams.iter().any(|s| s.name == name) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new(
                ApiErrorCode::StreamNotReady,
                format!("stream {name} is not placed"),
            )),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ApiError::new(
                ApiErrorCode::StreamNotFound,
                "stream not found",
            )),
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
/// this controller does not speak is turned away here.
async fn register_node(
    State(state): State<AppState>,
    payload: Result<Json<NodeRegistration>, JsonRejection>,
) -> Response {
    let Json(registration) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
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
            Json(ApiError::with_details(
                ApiErrorCode::IncompatibleProtocolVersion,
                "incompatible southbound protocol version",
                vec![ValidationIssue::new(
                    "protocol_version",
                    "unsupported",
                    format!(
                        "reported {}; supported {}",
                        registration.protocol_version, PROTOCOL_VERSION
                    ),
                )],
            )),
        )
            .into_response();
    }
    if let Err(reason) = validate_resource_id(&node_id) {
        return invalid_request(
            "node id is invalid",
            vec![resource_id_issue("node.id", "node id", reason)],
        );
    }
    if let Err(issue) = validate_endpoint_node_ids(&registration.endpoints) {
        return invalid_request("endpoint node id is invalid", vec![issue]);
    }
    if registration
        .hop_status
        .iter()
        .any(|status| status.node_id != node_id)
    {
        return invalid_request(
            "hop status node id does not match registration node id",
            vec![ValidationIssue::new(
                "hop_status",
                "node_id_mismatch",
                "every hop status node id must match node.id",
            )],
        );
    }

    let summary = NodeSummary::from(&registration.node);
    {
        let mut nodes = state.nodes.write().await;
        if let Err(err) = state.store.upsert_node(&registration).await {
            tracing::error!(%err, %node_id, "persisting node registration failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::PersistenceFailed,
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
    state.emit(EventType::NodeRegistered, summary);
    (
        StatusCode::ACCEPTED,
        Json(NodeAccepted {
            status: AcceptedState::Accepted,
            node_id,
        }),
    )
        .into_response()
}

/// Heartbeats update only in-memory observed fields; they never touch the store.
async fn node_heartbeat(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    payload: Result<Json<NodeHeartbeat>, JsonRejection>,
) -> Response {
    let Json(heartbeat) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    if let Err(reason) = validate_resource_id(&node_id) {
        return invalid_request(
            "node id is invalid",
            vec![resource_id_issue("node_id", "node id", reason)],
        );
    }
    if node_id != heartbeat.node_id {
        return invalid_request(
            "node id mismatch",
            vec![ValidationIssue::new(
                "node_id",
                "path_mismatch",
                "body node_id must match the path node id",
            )],
        );
    }
    if let Err(issue) = validate_endpoint_node_ids(&heartbeat.endpoints) {
        return invalid_request("endpoint node id is invalid", vec![issue]);
    }
    if heartbeat
        .hop_status
        .iter()
        .any(|status| status.node_id != node_id)
    {
        return invalid_request(
            "hop status node id does not match heartbeat node id",
            vec![ValidationIssue::new(
                "hop_status",
                "node_id_mismatch",
                "every hop status node id must match node_id",
            )],
        );
    }
    let mut nodes = state.nodes.write().await;
    let Some(registration) = nodes.get_mut(&node_id) else {
        return error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::NodeNotFound,
            "unknown node",
        );
    };
    let was_offline = registration.node.status == NodeStatus::Offline;
    registration.node.status = heartbeat.status;
    registration.endpoints = heartbeat.endpoints;
    registration.hop_status = heartbeat.hop_status;
    let status = registration.node.status;
    let recovered = (was_offline && status != NodeStatus::Offline)
        .then(|| NodeSummary::from(&registration.node));
    drop(nodes);
    state
        .last_seen
        .write()
        .await
        .insert(node_id.clone(), Instant::now());
    if let Some(summary) = recovered {
        state.emit(EventType::NodeOnline, summary);
    }

    tracing::debug!(%node_id, ?status, "node heartbeat");
    (
        StatusCode::ACCEPTED,
        Json(NodeAccepted {
            status: AcceptedState::Accepted,
            node_id,
        }),
    )
        .into_response()
}

/// Serve the desired hops computed for a node on the last reconcile tick.
async fn get_desired(State(state): State<AppState>, Path(node_id): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&node_id) {
        return invalid_request(
            "node id is invalid",
            vec![resource_id_issue("node_id", "node id", reason)],
        );
    }
    Json(
        state
            .desired
            .read()
            .await
            .get(&node_id)
            .cloned()
            .unwrap_or_default(),
    )
    .into_response()
}

fn error(status: StatusCode, code: ApiErrorCode, message: &str) -> Response {
    ApiError::new(code, message).response(status)
}

fn invalid_request(message: &str, details: Vec<ValidationIssue>) -> Response {
    ApiError::with_details(ApiErrorCode::InvalidRequest, message, details)
        .response(StatusCode::BAD_REQUEST)
}

fn invalid_json(rejection: JsonRejection) -> Response {
    ApiError::with_details(
        ApiErrorCode::InvalidJson,
        "request body is not valid for this endpoint",
        vec![ValidationIssue::new(
            "body",
            "invalid_json",
            rejection.body_text(),
        )],
    )
    .response(StatusCode::BAD_REQUEST)
}

fn validate_endpoint_node_ids(endpoints: &[EndpointDescriptor]) -> Result<(), ValidationIssue> {
    for (index, endpoint) in endpoints.iter().enumerate() {
        if let Some(node_id) = endpoint.node_id.as_deref()
            && let Err(error) = validate_resource_id(node_id)
        {
            return Err(resource_id_issue(
                &format!("endpoints[{index}].node_id"),
                "node id",
                error,
            ));
        }
    }
    Ok(())
}

/// One line naming every destination that cannot accept the declared source
/// format, or `None` when the manifest declares nothing to check.
///
/// Reported, never acted on. The hops are placed and the media flows either way;
/// what this says is that it will arrive somewhere it cannot be decoded, which is
/// worth knowing long before anything can convert it.
fn format_conflict_reason(stream: &StreamDefinition) -> Option<String> {
    let conflicts = weave_core::stream_format_conflicts(stream);
    if conflicts.is_empty() {
        return None;
    }
    Some(
        conflicts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; "),
    )
}

fn stream_condition(
    condition_type: StreamConditionType,
    status: StreamConditionStatus,
    reason: StreamConditionReason,
    detail: impl Into<String>,
) -> StreamCondition {
    StreamCondition {
        condition_type,
        status,
        reason,
        detail: detail.into(),
        last_transition_time: String::new(),
    }
}

fn format_condition(stream: &StreamDefinition) -> StreamCondition {
    let source_declared = matches!(
        &stream.source,
        weave_core::StreamTransport::Srt(endpoint) if endpoint.format.is_some()
    );
    let constrained = stream.destinations.iter().any(|destination| {
        matches!(
            destination,
            weave_core::StreamTransport::Srt(endpoint) if endpoint.accepts.is_some()
        )
    });
    if !source_declared || !constrained {
        return stream_condition(
            StreamConditionType::FormatCompatible,
            StreamConditionStatus::Unknown,
            StreamConditionReason::FormatUnknown,
            "source format or destination constraints are not declared",
        );
    }
    match format_conflict_reason(stream) {
        Some(detail) => stream_condition(
            StreamConditionType::FormatCompatible,
            StreamConditionStatus::False,
            StreamConditionReason::FormatMismatch,
            detail,
        ),
        None => stream_condition(
            StreamConditionType::FormatCompatible,
            StreamConditionStatus::True,
            StreamConditionReason::FormatCompatible,
            "declared formats are compatible",
        ),
    }
}

fn media_condition(status: PathStatus) -> StreamCondition {
    let (condition_status, reason, detail) = match status {
        PathStatus::Idle => (
            StreamConditionStatus::False,
            StreamConditionReason::MediaIdle,
            "stream is disabled",
        ),
        PathStatus::Failed => (
            StreamConditionStatus::False,
            StreamConditionReason::MediaFailed,
            "a hop failed",
        ),
        PathStatus::Pending => (
            StreamConditionStatus::Unknown,
            StreamConditionReason::NotReady,
            "media status is not known until the path is ready",
        ),
        PathStatus::AwaitingInput => (
            StreamConditionStatus::False,
            StreamConditionReason::AwaitingInput,
            "the source is not providing media",
        ),
        PathStatus::Degraded => (
            StreamConditionStatus::False,
            StreamConditionReason::MediaDegraded,
            "media is not flowing across every branch",
        ),
        PathStatus::Flowing => (
            StreamConditionStatus::True,
            StreamConditionReason::MediaFlowing,
            "media is flowing across every branch",
        ),
    };
    stream_condition(
        StreamConditionType::MediaFlowing,
        condition_status,
        reason,
        detail,
    )
}

fn disabled_conditions(stream: &StreamDefinition) -> Vec<StreamCondition> {
    vec![
        stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::False,
            StreamConditionReason::Disabled,
            "stream is disabled",
        ),
        stream_condition(
            StreamConditionType::NodesAvailable,
            StreamConditionStatus::Unknown,
            StreamConditionReason::Disabled,
            "stream is disabled",
        ),
        stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::Unknown,
            StreamConditionReason::Disabled,
            "stream is disabled",
        ),
        format_condition(stream),
        media_condition(PathStatus::Idle),
    ]
}

fn placement_failed_conditions(
    stream: &StreamDefinition,
    error: &PlacementError,
) -> Vec<StreamCondition> {
    let detail = error.to_string();
    let (node_status, node_reason) = if matches!(error, PlacementError::NodeNotRegistered { .. }) {
        (
            StreamConditionStatus::False,
            StreamConditionReason::NodeMissing,
        )
    } else {
        (
            StreamConditionStatus::Unknown,
            StreamConditionReason::NotReady,
        )
    };
    vec![
        stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::False,
            StreamConditionReason::PlacementFailed,
            detail.clone(),
        ),
        stream_condition(
            StreamConditionType::NodesAvailable,
            node_status,
            node_reason,
            detail,
        ),
        stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::Unknown,
            StreamConditionReason::NotReady,
            "no desired hops exist until placement succeeds",
        ),
        format_condition(stream),
        media_condition(PathStatus::Pending),
    ]
}

fn placed_conditions(
    stream: &StreamDefinition,
    path_status: PathStatus,
    offline_node: Option<&str>,
) -> Vec<StreamCondition> {
    let nodes = match offline_node {
        Some(node) => stream_condition(
            StreamConditionType::NodesAvailable,
            StreamConditionStatus::False,
            StreamConditionReason::NodeOffline,
            format!("node {node} is offline"),
        ),
        None => stream_condition(
            StreamConditionType::NodesAvailable,
            StreamConditionStatus::True,
            StreamConditionReason::NodesAvailable,
            "every placed node is available",
        ),
    };
    let hops = match path_status {
        PathStatus::Pending => stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::False,
            StreamConditionReason::HopsPending,
            "one or more desired hops have not reported ready",
        ),
        PathStatus::Failed => stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::False,
            StreamConditionReason::HopFailed,
            "one or more desired hops failed",
        ),
        _ => stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::True,
            StreamConditionReason::HopsReady,
            "every desired hop is provisioned",
        ),
    };
    vec![
        stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::True,
            StreamConditionReason::Placed,
            "the stream has a complete path",
        ),
        nodes,
        hops,
        format_condition(stream),
        media_condition(if offline_node.is_some() {
            PathStatus::Degraded
        } else {
            path_status
        }),
    ]
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
                generation: 0,
                observed_generation: None,
                status: PathStatus::Idle,
                nodes: Vec::new(),
                conditions: disabled_conditions(stream),
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
                let path_status = path_status(&path, &observed.hops);
                let offline_node = nodes
                    .iter()
                    .find(|id| offline.contains(id.as_str()))
                    .cloned();
                let status = if offline_node.is_some() || format_conflict_reason(stream).is_some() {
                    PathStatus::Degraded
                } else {
                    path_status
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
                    generation: 0,
                    observed_generation: None,
                    status,
                    nodes,
                    conditions: placed_conditions(stream, path_status, offline_node.as_deref()),
                    endpoints,
                });
                status
            }
            Err(error) => {
                tracing::warn!(stream = %stream.name, %error, "cannot place stream; retrying next tick");
                stream_statuses.push(StreamStatus {
                    name: stream.name.clone(),
                    generation: 0,
                    observed_generation: None,
                    status: PathStatus::Pending,
                    nodes: Vec::new(),
                    conditions: placement_failed_conditions(stream, &error),
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

    use crate::webhook::tests::{Sink, sink};

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
            webhooks: None,
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
                        weave_core::DataPlaneAddr::dialable(host),
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

    fn nat_registration(id: &str, host: &str) -> NodeRegistration {
        let mut registration = node_registration(id, host);
        registration.node.capabilities.data_plane.insert(
            weave_core::DEFAULT_DATA_PLANE_ALIAS.to_string(),
            weave_core::DataPlaneAddr {
                host: host.to_string(),
                reachability: weave_core::Reachability::OutboundOnly,
                signalling: weave_core::Signalling::default(),
            },
        );
        registration
    }

    fn relay_registration(id: &str, host: &str) -> NodeRegistration {
        let mut registration = node_registration(id, host);
        registration.node.capabilities.relay = true;
        registration
    }

    fn stream(name: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: Some(200),
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: Some(1000),
            })],
        }
    }

    fn stored_stream(spec: StreamDefinition) -> StoredStream {
        StoredStream {
            spec,
            generation: 1,
            revision: 1,
        }
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if method == "POST" && uri == "/v5/streams" {
            request = request.header("if-none-match", "*");
        }
        if method == "DELETE" && uri.starts_with("/v5/streams/") {
            request = request.header("if-match", "\"revision-1\"");
        }
        let request = request
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

    async fn send_with_headers(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> (StatusCode, HeaderMap, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = app
            .clone()
            .oneshot(
                request
                    .body(body.map_or(Body::empty(), |value| {
                        Body::from(serde_json::to_vec(&value).unwrap())
                    }))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, headers, value)
    }

    fn response_etag(headers: &HeaderMap) -> String {
        headers
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn post_stream_then_get_returns_it_and_writes_through() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        let (status, _) = send(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, body) = send(&app, "GET", "/v5/streams", None).await;
        assert_eq!(status, StatusCode::OK);
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert_eq!(listed[0].generation, 1);
        assert_eq!(listed[0].spec, stream("basic"));

        let (status, body) = send(&app, "GET", "/v5/streams/basic", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_value::<StreamResource>(body).unwrap(),
            StreamResource {
                generation: 1,
                spec: stream("basic")
            }
        );

        let (status, body) = send(&app, "GET", "/v5/streams/missing", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "stream_not_found");

        assert_eq!(
            mem.upsert_stream_calls(),
            1,
            "stream was written through the store"
        );
        assert_eq!(mem.load_streams().await.unwrap()[0].spec, stream("basic"));
    }

    #[tokio::test]
    async fn stream_writes_require_and_enforce_etag_preconditions() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let original = stream("basic");

        let (status, _, body) = send_with_headers(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(body["code"], "precondition_required");

        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let first_etag = response_etag(&headers);
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.generation, 1);
        assert!(accepted.changed);

        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), first_etag);
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.generation, 1);
        assert!(!accepted.changed);

        let mut changed = original.clone();
        changed.enabled = false;
        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&changed).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let second_etag = response_etag(&headers);
        assert_ne!(second_etag, first_etag);
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.generation, 2);
        assert!(accepted.changed);

        let (status, _, body) = send_with_headers(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["code"], "precondition_failed");

        let (status, headers, body) =
            send_with_headers(&app, "GET", "/v5/streams/basic", None, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response_etag(&headers), second_etag);
        let resource: StreamResource = serde_json::from_value(body).unwrap();
        assert_eq!(resource.generation, 2);
        assert_eq!(resource.spec, changed);
    }

    #[tokio::test]
    async fn status_distinguishes_current_and_observed_generations() {
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());
        let definition = stream("basic");
        let (_, headers, _) = send_with_headers(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&definition).unwrap()),
            &[("if-none-match", "*")],
        )
        .await;
        let etag = response_etag(&headers);
        reconcile_tick(&state).await;
        {
            let view = state.view.read().await;
            assert_eq!(view.streams[0].generation, 1);
            assert_eq!(view.streams[0].observed_generation, Some(1));
            assert_eq!(view.streams[0].conditions.len(), 5);
            assert!(
                view.streams[0]
                    .conditions
                    .iter()
                    .all(|condition| !condition.last_transition_time.is_empty())
            );
            for condition in &view.streams[0].conditions {
                time::OffsetDateTime::parse(
                    &condition.last_transition_time,
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap();
            }
        }

        let mut changed = definition;
        changed.enabled = false;
        let (status, _, _) = send_with_headers(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&changed).unwrap()),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        {
            let view = state.view.read().await;
            assert_eq!(view.streams[0].generation, 2);
            assert_eq!(view.streams[0].observed_generation, Some(1));
        }

        reconcile_tick(&state).await;
        let view = state.view.read().await;
        assert_eq!(view.streams[0].generation, 2);
        assert_eq!(view.streams[0].observed_generation, Some(2));
        assert_eq!(
            view.streams[0].conditions[0].reason,
            StreamConditionReason::Disabled
        );
    }

    #[test]
    fn condition_transition_time_changes_only_when_status_changes() {
        let mut previous = StreamStatus {
            name: "basic".to_string(),
            generation: 1,
            observed_generation: Some(1),
            status: PathStatus::Pending,
            nodes: Vec::new(),
            conditions: vec![stream_condition(
                StreamConditionType::PlacementReady,
                StreamConditionStatus::False,
                StreamConditionReason::PlacementFailed,
                "missing node",
            )],
            endpoints: None,
        };
        previous.conditions[0].last_transition_time = "2026-09-18T10:00:00Z".to_string();
        let mut current = previous.clone();
        current.conditions[0].reason = StreamConditionReason::Disabled;
        stamp_condition_transition_times(
            std::slice::from_mut(&mut current),
            std::slice::from_ref(&previous),
            "2026-09-18T10:01:00Z",
        );
        assert_eq!(
            current.conditions[0].last_transition_time,
            "2026-09-18T10:00:00Z"
        );

        current.conditions[0].status = StreamConditionStatus::True;
        stamp_condition_transition_times(
            std::slice::from_mut(&mut current),
            std::slice::from_ref(&previous),
            "2026-09-18T10:02:00Z",
        );
        assert_eq!(
            current.conditions[0].last_transition_time,
            "2026-09-18T10:02:00Z"
        );
    }

    #[tokio::test]
    async fn invalid_stream_is_rejected_before_persistence() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        let mut invalid = stream("invalid");
        let StreamTransport::Srt(destination) = &mut invalid.destinations[0] else {
            unreachable!()
        };
        destination.remote = Some(weave_core::RemoteAddr {
            host: "example.test".to_string(),
            port: 9000,
        });

        let (status, body) = send(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(&invalid).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["details"][0]["field"], "destinations[0].srt");
        assert_eq!(body["details"][0]["code"], "mutually_exclusive");
        assert_eq!(mem.upsert_stream_calls(), 0);

        let (status, body) = send(
            &app,
            "POST",
            "/v5/stream-plans",
            Some(serde_json::to_value(invalid).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(mem.upsert_stream_calls(), 0);

        let (_, body) = send(&app, "GET", "/v5/streams", None).await;
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn plan_places_without_changing_desired_state() {
        let (state, mem) = mem_state();
        let app = open_router(state.clone());
        for registration in [
            node_registration("strom-node-1", "172.26.0.10"),
            node_registration("strom-node-2", "172.27.0.10"),
        ] {
            let (status, _) = send(
                &app,
                "POST",
                "/v5/nodes/register",
                Some(serde_json::to_value(registration).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED);
        }

        let (status, body) = send(
            &app,
            "POST",
            "/v5/stream-plans",
            Some(serde_json::to_value(stream("preview")).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Placed);
        assert_eq!(plan.nodes, ["strom-node-1", "strom-node-2"]);
        assert_eq!(plan.hops.len(), 2);
        assert!(plan.endpoints.is_some());
        assert_eq!(mem.upsert_stream_calls(), 0);
        assert!(state.streams.read().await.is_empty());
        assert!(state.desired.read().await.is_empty());
        assert!(state.view.read().await.streams.is_empty());
    }

    #[tokio::test]
    async fn plan_distinguishes_unplaced_and_disabled_streams() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        let (status, body) = send(
            &app,
            "POST",
            "/v5/stream-plans",
            Some(serde_json::to_value(stream("unplaced")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Unplaced);
        assert!(plan.hops.is_empty());
        assert!(plan.reason.unwrap().contains("not registered"));

        let mut disabled = stream("disabled");
        disabled.enabled = false;
        let (status, body) = send(
            &app,
            "POST",
            "/v5/stream-plans",
            Some(serde_json::to_value(disabled).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Disabled);
        assert!(plan.hops.is_empty());
        assert!(plan.reason.is_none());
        assert_eq!(mem.upsert_stream_calls(), 0);
    }

    #[tokio::test]
    async fn plan_allocates_ports_alongside_existing_streams() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        for mut registration in [
            node_registration("strom-node-1", "172.26.0.10"),
            node_registration("strom-node-2", "172.27.0.10"),
        ] {
            registration.node.capabilities.port_range = Some(PortRange {
                start: 7000,
                end: 7000,
            });
            send(
                &app,
                "POST",
                "/v5/nodes/register",
                Some(serde_json::to_value(registration).unwrap()),
            )
            .await;
        }
        send(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(stream("existing")).unwrap()),
        )
        .await;

        let (status, body) = send(
            &app,
            "POST",
            "/v5/stream-plans",
            Some(serde_json::to_value(stream("preview")).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Unplaced);
        assert!(plan.reason.unwrap().contains("no free port"));
        assert_eq!(mem.upsert_stream_calls(), 1, "plan did not write a stream");
        let (_, body) = send(&app, "GET", "/v5/streams", None).await;
        let streams: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert_eq!(streams[0].spec, stream("existing"));
    }

    #[tokio::test]
    async fn invalid_json_has_a_structured_error() {
        let (state, mem) = mem_state();
        let request = Request::builder()
            .method("POST")
            .uri("/v5/streams")
            .header("content-type", "application/json")
            .body(Body::from("{"))
            .unwrap();

        let response = open_router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let error: ApiError = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.code, ApiErrorCode::InvalidJson);
        assert_eq!(error.details[0].field, "body");
        assert_eq!(mem.upsert_stream_calls(), 0);
    }

    #[tokio::test]
    async fn hydration_refuses_invalid_persisted_streams() {
        let mem = Arc::new(MemStore::new());
        let mut invalid = stream("broken");
        invalid.destinations.clear();
        mem.create_stream(&invalid).await.unwrap();

        let error = AppState::hydrate(mem, Duration::from_secs(15), None)
            .await
            .err()
            .expect("invalid stored stream must prevent startup");

        assert_eq!(
            error.to_string(),
            "stored stream \"broken\" is invalid at destinations: stream must have at least one destination"
        );
    }

    #[tokio::test]
    async fn hydration_drops_persisted_nodes_with_invalid_ids() {
        let mem = Arc::new(MemStore::new());
        let registration = node_registration("node/one", "172.26.0.10");
        mem.upsert_node(&registration).await.unwrap();

        let state = AppState::hydrate(mem, Duration::from_secs(15), None)
            .await
            .expect("invalid cached nodes must not prevent startup");

        assert!(state.nodes.read().await.is_empty());
    }

    #[tokio::test]
    async fn delete_stream_removes_and_writes_through() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        send(
            &app,
            "POST",
            "/v5/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        let (status, _) = send(&app, "DELETE", "/v5/streams/basic", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(mem.delete_stream_calls(), 1);

        let (_, body) = send(&app, "GET", "/v5/streams", None).await;
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());

        let (status, _) = send(&app, "DELETE", "/v5/streams/basic", None).await;
        assert_eq!(
            status,
            StatusCode::PRECONDITION_FAILED,
            "second delete fails its revision precondition"
        );
    }

    #[tokio::test]
    async fn invalid_resource_paths_are_rejected() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        for (method, uri, body) in [
            ("GET", "/v5/streams/foo%3Fignored", None),
            ("DELETE", "/v5/streams/foo%3Fignored", None),
            ("GET", "/v5/streams/foo%3Fignored/endpoints", None),
            (
                "POST",
                "/v5/nodes/foo%3Fignored/heartbeat",
                Some(json!({ "node_id": "foo?ignored", "status": "ready" })),
            ),
            ("GET", "/v5/nodes/foo%3Fignored/desired", None),
        ] {
            let (status, _) = send(&app, method, uri, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {uri}");
        }

        assert_eq!(mem.delete_stream_calls(), 0);
    }

    #[tokio::test]
    async fn heartbeat_updates_memory_but_never_the_store() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        send(
            &app,
            "POST",
            "/v5/nodes/register",
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
            "/v5/nodes/strom-node-1/heartbeat",
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            mem.upsert_node_calls(),
            1,
            "heartbeat must not write through to the store"
        );

        let (_, body) = send(&app, "GET", "/v5/state", None).await;
        let observed: ObservedState = serde_json::from_value(body).unwrap();
        assert_eq!(observed.nodes[0].status, NodeStatus::Degraded);
    }

    /// A stale adapter is turned away at the handshake, and nothing about it is
    /// recorded.
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
                "/v5/nodes/register",
                Some(serde_json::to_value(&registration).unwrap()),
            )
            .await;

            assert_eq!(status, StatusCode::CONFLICT, "reported version {reported}");
            assert_eq!(body["code"], "incompatible_protocol_version");
            assert_eq!(body["details"][0]["field"], "protocol_version");
            assert_eq!(
                body["details"][0]["message"],
                format!("reported {reported}; supported {PROTOCOL_VERSION}")
            );
        }

        assert_eq!(
            mem.upsert_node_calls(),
            0,
            "a rejected node is never persisted"
        );
        let (_, body) = send(&app, "GET", "/v5/nodes", None).await;
        assert_eq!(body, json!([]), "a rejected node is never registered");
    }

    #[tokio::test]
    async fn registration_with_an_invalid_node_id_is_rejected() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        let registration = node_registration("node/one", "172.26.0.10");

        let (status, body) = send(
            &app,
            "POST",
            "/v5/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["details"][0]["field"], "node.id");
        assert_eq!(body["details"][0]["code"], "invalid_characters");
        assert_eq!(mem.upsert_node_calls(), 0);
        let (_, body) = send(&app, "GET", "/v5/nodes", None).await;
        assert_eq!(body, json!([]));

        let mut registration = node_registration("node-one", "172.26.0.10");
        registration.endpoints.push(EndpointDescriptor {
            id: "capture-1".to_string(),
            label: "Capture".to_string(),
            node_id: Some("node/one".to_string()),
            kind: weave_core::EndpointKind::Source,
            transports: Vec::new(),
            metadata: Value::Null,
        });
        let (status, body) = send(
            &app,
            "POST",
            "/v5/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["details"][0]["field"], "endpoints[0].node_id");
        assert_eq!(mem.upsert_node_calls(), 0);
    }

    #[tokio::test]
    async fn node_cannot_report_another_nodes_hop_status() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        let status_for = |node_id: &str| weave_core::HopStatus {
            id: "weave-basic-sender".to_string(),
            node_id: node_id.to_string(),
            state: weave_core::HopState::Provisioned,
            ingress: weave_core::SocketStatus {
                condition: weave_core::LinkCondition::Flowing,
                resolved: None,
                stats: None,
            },
            egresses: Vec::new(),
        };

        let mut registration = node_registration("strom-node-1", "172.26.0.10");
        registration.hop_status = vec![status_for("strom-node-2")];
        let (status, _) = send(
            &app,
            "POST",
            "/v5/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(mem.upsert_node_calls(), 0);

        registration.hop_status.clear();
        let (status, _) = send(
            &app,
            "POST",
            "/v5/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let heartbeat = NodeHeartbeat {
            node_id: "strom-node-1".to_string(),
            status: NodeStatus::Ready,
            endpoints: Vec::new(),
            hop_status: vec![status_for("strom-node-2")],
        };
        let (status, _) = send(
            &app,
            "POST",
            "/v5/nodes/strom-node-1/heartbeat",
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// A browser node registers with a `browser://<id>` endpoint. The field is
    /// opaque here: the controller stores it, makes no outbound call to any node
    /// (it has no HTTP client at all), and serves the node's desired hops for the
    /// page to pull like any adapter.
    #[tokio::test]
    async fn a_browser_endpoint_is_stored_verbatim_and_never_dialled() {
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());

        let mut browser = node_registration("browser-a1b2", "browser");
        browser.node.endpoint = "browser://browser-a1b2".to_string();
        browser.node.capabilities.port_range = None;
        browser.node.capabilities.transports = vec![
            weave_core::TransportOffer::with_roles(
                weave_core::Transport::Whip,
                weave_core::RoleSet::only(weave_core::SocketRole::Connect),
            ),
            weave_core::TransportOffer::with_roles(
                weave_core::Transport::Whep,
                weave_core::RoleSet::only(weave_core::SocketRole::Connect),
            ),
        ];
        browser.node.capabilities.devices = [
            weave_core::DeviceKind::Capture,
            weave_core::DeviceKind::Display,
        ]
        .into_iter()
        .collect();
        browser.node.capabilities.data_plane.insert(
            weave_core::DEFAULT_DATA_PLANE_ALIAS.to_string(),
            weave_core::DataPlaneAddr {
                host: "browser".to_string(),
                reachability: weave_core::Reachability::OutboundOnly,
                signalling: weave_core::Signalling::default(),
            },
        );
        let (status, _) = send(
            &app,
            "POST",
            "/v5/nodes/register",
            Some(serde_json::to_value(&browser).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let mut strom = node_registration("strom-node-2", "172.27.0.10");
        strom.node.capabilities.transports = vec![
            weave_core::TransportOffer::new(weave_core::Transport::Srt),
            weave_core::TransportOffer::with_roles(
                weave_core::Transport::Whip,
                weave_core::RoleSet::only(weave_core::SocketRole::Listen),
            ),
        ];
        strom
            .node
            .capabilities
            .data_plane
            .get_mut(weave_core::DEFAULT_DATA_PLANE_ALIAS)
            .unwrap()
            .signalling
            .whip = Some("http://172.27.0.10:8080/whip".to_string());
        send(
            &app,
            "POST",
            "/v5/nodes/register",
            Some(serde_json::to_value(&strom).unwrap()),
        )
        .await;

        let mut cam = stream("alice-cam");
        cam.source = StreamTransport::Device(weave_core::NodeEndpoint {
            node: "browser-a1b2".to_string(),
            network: None,
        });
        state
            .streams
            .write()
            .await
            .insert(cam.name.clone(), stored_stream(cam));

        reconcile_tick(&state).await;

        let (_, body) = send(&app, "GET", "/v5/nodes", None).await;
        let nodes: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        let page = nodes.iter().find(|n| n.id == "browser-a1b2").unwrap();
        assert_eq!(page.endpoint, "browser://browser-a1b2");

        let (status, body) = send(&app, "GET", "/v5/nodes/browser-a1b2/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1);
        assert!(
            matches!(
                hops[0].ingress,
                weave_core::SocketSpec::Device(weave_core::DeviceKind::Capture)
            ),
            "the page's own camera feeds the hop: {:?}",
            hops[0].ingress
        );
        assert!(
            matches!(hops[0].egresses[0].socket, weave_core::SocketSpec::Whip(_)),
            "the page pushes to the Strom's ingest: {:?}",
            hops[0].egresses[0]
        );
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
                .insert("basic".to_string(), stored_stream(stream("basic")));
        }

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/v5/nodes/strom-node-1/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "sender hop placed on node 1");
        assert_eq!(hops[0].id, "weave-basic-sender");

        let (_, body) = send(&app, "GET", "/v5/nodes/strom-node-2/desired", None).await;
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
                generation: 1,
                observed_generation: None,
                status: PathStatus::Pending,
                nodes: Vec::new(),
                conditions: Vec::new(),
                endpoints: None,
            }];
        }
        let app = open_router(state);
        let (status, _) = send(&app, "GET", "/v5/streams/basic/endpoints", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let (status, _) = send(&app, "GET", "/v5/streams/nope/endpoints", None).await;
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
            let mut definition = stream("basic");
            definition
                .destinations
                .push(definition.destinations[0].clone());
            state
                .streams
                .write()
                .await
                .insert("basic".to_string(), stored_stream(definition));
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
            ingress: weave_core::SocketStatus {
                condition: weave_core::LinkCondition::Flowing,
                resolved: None,
                stats: Some(weave_core::LinkStats {
                    rate_mbps: 3.2,
                    ..weave_core::LinkStats::default()
                }),
            },
            egresses: vec![
                weave_core::EgressStatus {
                    branch_id: "destination-0".to_string(),
                    status: weave_core::SocketStatus {
                        condition: weave_core::LinkCondition::Flowing,
                        resolved: None,
                        stats: Some(weave_core::LinkStats {
                            rate_mbps: 3.1,
                            ..weave_core::LinkStats::default()
                        }),
                    },
                },
                weave_core::EgressStatus {
                    branch_id: "destination-1".to_string(),
                    status: weave_core::SocketStatus {
                        condition: weave_core::LinkCondition::Connecting,
                        resolved: Some(weave_core::ResolvedAddr {
                            host: "172.27.0.10".to_string(),
                            port: 7555,
                        }),
                        stats: Some(weave_core::LinkStats {
                            rate_mbps: 0.0,
                            ..weave_core::LinkStats::default()
                        }),
                    },
                },
            ],
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
        assert_eq!(hops.len(), 3, "sender + two receivers");

        let sender = &hops[0];
        assert_eq!(sender["id"], "weave-basic-sender");
        assert_eq!(sender["node"], "strom-node-1");
        assert_eq!(sender["state"], "provisioned");
        assert_eq!(sender["ingress_condition"], "flowing");
        assert_eq!(sender["ingress_stats"]["rate_mbps"], 3.2);
        assert_eq!(sender["ingress"]["mode"], "listen");
        assert_eq!(sender["egresses"][0]["branch_id"], "destination-0");
        assert_eq!(sender["egresses"][0]["condition"], "flowing");
        assert_eq!(sender["egresses"][0]["stats"]["rate_mbps"], 3.1);
        assert_eq!(sender["egresses"][0]["mode"], "connect");
        assert_eq!(sender["egresses"][0]["host"], "172.27.0.10");
        assert_eq!(sender["egresses"][1]["branch_id"], "destination-1");
        assert_eq!(sender["egresses"][1]["condition"], "connecting");
        assert_eq!(sender["egresses"][1]["resolved"]["port"], 7555);

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
    fn mark_offline_reports_only_the_nodes_it_transitioned() {
        let mut nodes = BTreeMap::from([
            (
                "stale".to_string(),
                node_registration("stale", "172.26.0.10"),
            ),
            (
                "fresh".to_string(),
                node_registration("fresh", "172.27.0.10"),
            ),
        ]);

        let ttl = Duration::from_secs(15);
        let now = Instant::now();
        let last_seen = BTreeMap::from([
            ("stale".to_string(), now - Duration::from_secs(20)),
            ("fresh".to_string(), now - Duration::from_secs(5)),
        ]);

        assert_eq!(
            mark_offline(&mut nodes, &last_seen, now, ttl),
            vec!["stale".to_string()]
        );
        assert!(
            mark_offline(&mut nodes, &last_seen, now, ttl).is_empty(),
            "a node already offline does not transition again"
        );
    }

    fn webhook_state(sink: &Sink) -> AppState {
        let (mut state, _mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        state
    }

    #[tokio::test]
    async fn registering_a_node_emits_node_registered() {
        let mut sink = sink(StatusCode::OK).await;
        let app = open_router(webhook_state(&sink));

        let (status, _) = send(
            &app,
            "POST",
            "/v5/nodes/register",
            Some(serde_json::to_value(node_registration("guest-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let event = sink.next().await.event;
        assert_eq!(
            event.event_type,
            weave_core::webhook::EventType::NodeRegistered
        );
        assert_eq!(event.node.id, "guest-1");
        assert_eq!(event.node.endpoint, "http://guest-1:8080");
        assert_eq!(event.node.status, NodeStatus::Ready);
    }

    #[tokio::test]
    async fn a_node_past_its_ttl_emits_node_offline_once() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        state.nodes.write().await.insert(
            "guest-1".to_string(),
            node_registration("guest-1", "172.26.0.10"),
        );
        state.last_seen.write().await.insert(
            "guest-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );

        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let event = sink.next().await.event;
        assert_eq!(
            event.event_type,
            weave_core::webhook::EventType::NodeOffline
        );
        assert_eq!(event.node.id, "guest-1");
        assert_eq!(event.node.status, NodeStatus::Offline);
        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn a_heartbeat_from_an_offline_node_emits_node_online() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        let mut registration = node_registration("guest-1", "172.26.0.10");
        registration.node.status = NodeStatus::Offline;
        state
            .nodes
            .write()
            .await
            .insert("guest-1".to_string(), registration);
        let app = open_router(state);

        let (status, _) = send(
            &app,
            "POST",
            "/v5/nodes/guest-1/heartbeat",
            Some(json!({ "node_id": "guest-1", "status": "ready" })),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let event = sink.next().await.event;
        assert_eq!(event.event_type, weave_core::webhook::EventType::NodeOnline);
        assert_eq!(event.node.status, NodeStatus::Ready);
    }

    #[tokio::test]
    async fn a_heartbeat_from_a_live_node_emits_nothing() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        state.nodes.write().await.insert(
            "guest-1".to_string(),
            node_registration("guest-1", "172.26.0.10"),
        );
        let app = open_router(state);

        send(
            &app,
            "POST",
            "/v5/nodes/guest-1/heartbeat",
            Some(json!({ "node_id": "guest-1", "status": "ready" })),
        )
        .await;

        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn registration_is_accepted_while_the_receiver_refuses_connections() {
        let (mut state, _mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            // Reserved for documentation; nothing listens there.
            url: Some("http://192.0.2.1:1/hook".to_string()),
            timeout: Duration::from_millis(50),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let app = open_router(state);

        let accepted = tokio::time::timeout(
            Duration::from_secs(2),
            send(
                &app,
                "POST",
                "/v5/nodes/register",
                Some(serde_json::to_value(node_registration("guest-1", "172.26.0.10")).unwrap()),
            ),
        )
        .await
        .expect("registration must not wait on the webhook receiver");

        assert_eq!(accepted.0, StatusCode::ACCEPTED);
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

        let mut definition = stream("basic");
        let weave_core::StreamTransport::Srt(source) = &mut definition.source else {
            unreachable!()
        };
        source.format = Some(weave_core::MediaFormat {
            container: weave_core::Container::MpegTs,
            video: None,
            audio: None,
        });
        let weave_core::StreamTransport::Srt(destination) = &mut definition.destinations[0] else {
            unreachable!()
        };
        destination.accepts = Some(weave_core::FormatConstraint {
            container: Some(vec![weave_core::Container::Rtp]),
            ..Default::default()
        });
        let outcome = reconcile(vec![definition], &observed);

        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert_eq!(basic.status, PathStatus::Degraded);
        let nodes = basic
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::NodesAvailable)
            .unwrap();
        assert_eq!(nodes.reason, StreamConditionReason::NodeOffline);
        assert_eq!(nodes.detail, "node strom-node-1 is offline");
        let format = basic
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::FormatCompatible)
            .unwrap();
        assert_eq!(format.status, StreamConditionStatus::False);
        assert_eq!(format.reason, StreamConditionReason::FormatMismatch);
        assert!(
            !outcome.desired_by_node["strom-node-1"].is_empty(),
            "desired hops for the offline node are still computed"
        );
    }

    #[test]
    fn reconcile_reports_why_a_stream_is_pending() {
        let nodes = BTreeMap::from([(
            "strom-node-1".to_string(),
            node_registration("strom-node-1", "172.26.0.10"),
        )]);
        let observed = observed_state(&nodes);

        let outcome = reconcile(vec![stream("basic")], &observed);

        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert_eq!(basic.status, PathStatus::Pending);
        let placement = basic
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::PlacementReady)
            .unwrap();
        assert_eq!(placement.reason, StreamConditionReason::PlacementFailed);
        assert_eq!(placement.detail, "node strom-node-2 is not registered");
    }

    #[test]
    fn reconcile_replans_a_stream_off_an_offline_relay() {
        let mut nodes = BTreeMap::from([
            (
                "strom-node-1".to_string(),
                nat_registration("strom-node-1", "172.26.0.10"),
            ),
            (
                "strom-node-2".to_string(),
                nat_registration("strom-node-2", "172.27.0.10"),
            ),
            (
                "relay-a".to_string(),
                relay_registration("relay-a", "198.51.100.10"),
            ),
            (
                "relay-b".to_string(),
                relay_registration("relay-b", "198.51.100.20"),
            ),
        ]);
        nodes.get_mut("relay-a").unwrap().node.status = NodeStatus::Offline;
        let observed = observed_state(&nodes);

        let outcome = reconcile(vec![stream("basic")], &observed);

        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert!(basic.nodes.contains(&"relay-b".to_string()));
        assert!(!basic.nodes.contains(&"relay-a".to_string()));
        assert_ne!(basic.status, PathStatus::Degraded);
        assert!(basic.conditions.iter().all(|condition| {
            condition.condition_type != StreamConditionType::NodesAvailable
                || condition.status == StreamConditionStatus::True
        }));
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
                .insert("basic".to_string(), stored_stream(stream("basic")));
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now - Duration::from_secs(60));
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/v5/nodes", None).await;
        assert_eq!(status, StatusCode::OK);
        let nodes: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        let node1 = nodes.iter().find(|n| n.id == "strom-node-1").unwrap();
        assert_eq!(node1.status, NodeStatus::Offline);

        let (status, body) = send(&app, "GET", "/v5/nodes/strom-node-1/desired", None).await;
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

    const NORTH_ROUTES: [(&str, &str); 6] = [
        ("GET", "/v5/streams"),
        ("POST", "/v5/streams"),
        ("POST", "/v5/stream-plans"),
        ("GET", "/v5/streams/basic"),
        ("DELETE", "/v5/streams/basic"),
        ("GET", "/v5/streams/basic/endpoints"),
    ];

    const SOUTH_ROUTES: [(&str, &str); 6] = [
        ("GET", "/v5/nodes"),
        ("POST", "/v5/nodes/register"),
        ("POST", "/v5/nodes/strom-node-1/heartbeat"),
        ("GET", "/v5/nodes/strom-node-1/desired"),
        ("GET", "/v5/endpoints"),
        ("GET", "/v5/state"),
    ];

    /// Unversioned and retired-major paths are gone: this is a clean break, not
    /// an alias.
    #[tokio::test]
    async fn unversioned_and_retired_api_paths_are_not_served() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        for (method, uri) in NORTH_ROUTES.iter().chain(&SOUTH_ROUTES) {
            let unversioned = uri.strip_prefix(API_PREFIX).expect("route is versioned");
            let (status, _) = send(&app, method, unversioned, None).await;
            assert_eq!(
                status,
                StatusCode::NOT_FOUND,
                "{method} {unversioned} must not be served alongside {uri}"
            );
        }
        let (status, _) = send(&app, "GET", "/status", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        for retired_prefix in ["/v1", "/v2", "/v3", "/v4"] {
            for (method, uri) in NORTH_ROUTES.iter().chain(&SOUTH_ROUTES) {
                let retired = uri.replacen(API_PREFIX, retired_prefix, 1);
                let (status, _) = send(&app, method, &retired, None).await;
                assert_eq!(status, StatusCode::NOT_FOUND, "{retired} must stay retired");
            }
            let (status, _) = send(&app, "GET", &format!("{retired_prefix}/status"), None).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
    }

    /// The dashboard surface is unauthenticated — it is browser-loaded and cannot
    /// carry a bearer token. `/health` is open for healthchecks.
    #[tokio::test]
    async fn dashboard_and_health_stay_open() {
        let (state, _mem) = mem_state();
        let app = guarded_router(state);
        for uri in ["/", "/ui", "/health", "/view", "/v5/status"] {
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
            "/v5/streams",
            Some(&format!("Bearer {NORTH_TOKEN}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) = send_auth(
            &app,
            "GET",
            "/v5/state",
            Some(&format!("Bearer {SOUTH_TOKEN}")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}
