//! `weave-controller` — the single stateful control-plane service. It owns the
//! stream and node registries (persisted to Postgres), serves the northbound and
//! southbound HTTP surfaces, and reconciles desired streams into per-node desired
//! hops on a fixed interval, entirely from in-memory state.

mod desired;
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
    AcceptedState, ApiError, ApiErrorCode, DesiredHop, EndpointDescriptor, HopStatus, NodeAccepted,
    NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus, ObservedState, PROTOCOL_VERSION,
    PathStatus, PlanStatus, ROUTE_ENDPOINTS, ROUTE_NODE_DESIRED, ROUTE_NODE_HEARTBEAT,
    ROUTE_NODE_REGISTER, ROUTE_NODES, ROUTE_STATE, ROUTE_STATUS, ROUTE_STREAM,
    ROUTE_STREAM_ENDPOINTS, ROUTE_STREAM_PLANS, ROUTE_STREAM_SET, ROUTE_STREAM_SETS, ROUTE_STREAMS,
    ReconcileReport, ReconcileStatus, RunningStatus, StartingState, StartingStatus, StatusResponse,
    StreamAccepted, StreamCondition, StreamConditionReason, StreamConditionStatus,
    StreamConditionType, StreamDefinition, StreamDestinationStatus, StreamEndpoints, StreamPlan,
    StreamResource, StreamSetAccepted, StreamSetAction, StreamSetApply, StreamSetMemberResult,
    StreamSetResource, StreamStatus, ValidationIssue, protocol_compatible, resource_id_issue,
    validate_node, validate_resource_id, validate_stream,
};

use path::{
    PlacementError, PortAllocator, derive_path, destination_nodes, destination_path_status,
    path_status, stream_endpoints,
};
use store::{
    MemStore, PgStore, StateStore, StoreError, StoredStream, StreamSetMemberAction, StreamSetWrite,
};

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
    stream_sets: Arc<RwLock<BTreeMap<String, u64>>>,
    nodes: Arc<RwLock<BTreeMap<String, NodeRegistration>>>,
    last_seen: Arc<RwLock<BTreeMap<String, Instant>>>,
    node_ttl: Duration,
    desired: Arc<RwLock<BTreeMap<String, desired::DesiredSnapshot>>>,
    view: Arc<RwLock<ControllerView>>,
    /// `None` when no receiver is configured; every emit site is then a no-op.
    webhooks: Option<Arc<webhook::Emitter>>,
}

#[derive(Clone)]
struct SharedReadGuards {
    north: Guard,
    south: Guard,
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
            if let Some(owner) = stream.owner.as_deref()
                && let Err(error) = validate_resource_id(owner)
            {
                anyhow::bail!(
                    "stored stream {:?} has invalid owner: {error}",
                    stream.spec.name
                );
            }
        }
        let streams = loaded_streams
            .into_iter()
            .map(|stream| (stream.spec.name.clone(), stream))
            .collect();
        let stream_sets = store
            .load_stream_sets()
            .await
            .context("hydrating stream sets")?
            .into_iter()
            .map(|stream_set| {
                validate_resource_id(&stream_set.owner).map_err(|error| {
                    anyhow::anyhow!(
                        "stored stream set {:?} has an invalid owner: {error}",
                        stream_set.owner
                    )
                })?;
                Ok((stream_set.owner, stream_set.revision))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let nodes = store
            .load_nodes()
            .await
            .context("hydrating nodes")?
            .into_iter()
            .filter(|registration| {
                let issues = validate_node(&registration.node);
                if !issues.is_empty() {
                    tracing::warn!(
                        node_id = %registration.node.id,
                        ?issues,
                        "dropping a stored registration with an invalid node descriptor; the node must re-register"
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
            stream_sets: Arc::new(RwLock::new(stream_sets)),
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
    topology: weave_core::NodeTopology,
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
    profile_id: String,
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

    let desired = desired::snapshots(outcome.desired_by_node);
    for (node_id, snapshot) in &desired {
        tracing::debug!(%node_id, revision = %snapshot.revision, hops = snapshot.hops.len(), "desired snapshot");
    }
    *state.desired.write().await = desired;
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
        stamp_conditions(
            &mut stream.conditions,
            previous.map(|stream| stream.conditions.as_slice()),
            now,
        );
        for destination in &mut stream.destinations {
            let previous_conditions = previous
                .and_then(|stream| {
                    stream
                        .destinations
                        .iter()
                        .find(|candidate| candidate.id == destination.id)
                })
                .map(|destination| destination.conditions.as_slice());
            stamp_conditions(&mut destination.conditions, previous_conditions, now);
        }
    }
}

fn stamp_conditions(
    current: &mut [StreamCondition],
    previous: Option<&[StreamCondition]>,
    now: &str,
) {
    for condition in current {
        condition.last_transition_time = previous
            .and_then(|conditions| {
                conditions.iter().find(|candidate| {
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

fn unreconciled_stream_status(stored: &StoredStream) -> StreamStatus {
    let now = now_rfc3339();
    let conditions: Vec<StreamCondition> = [
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
    let mut status = StreamStatus {
        name: stored.spec.name.clone(),
        generation: stored.generation,
        observed_generation: None,
        status: PathStatus::Pending,
        nodes: Vec::new(),
        destinations: stored
            .spec
            .destinations
            .iter()
            .map(|destination| StreamDestinationStatus {
                id: destination.id.clone(),
                status: PathStatus::Pending,
                nodes: Vec::new(),
                conditions: conditions.clone(),
                endpoint: None,
            })
            .collect(),
        conditions,
        ingress: None,
    };
    status
        .destinations
        .sort_by(|left, right| left.id.cmp(&right.id));
    status
}

fn update_view_generation(view: &mut ControllerView, stored: &StoredStream) {
    if let Some(status) = view
        .streams
        .iter_mut()
        .find(|status| status.name == stored.spec.name)
    {
        status.generation = stored.generation;
    } else {
        view.streams.push(unreconciled_stream_status(stored));
    }
}

async fn update_pending_generation(state: &AppState, stored: &StoredStream) {
    let mut view = state.view.write().await;
    update_view_generation(&mut view, stored);
    view.streams
        .sort_by(|left, right| left.name.cmp(&right.name));
}

async fn update_stream_set_view(state: &AppState, write: &StreamSetWrite) {
    let mut view = state.view.write().await;
    for name in &write.deleted {
        view.streams.retain(|status| status.name != *name);
        view.endpoints.remove(name);
        view.hops.remove(name);
    }
    for stream in &write.stream_set.streams {
        update_view_generation(&mut view, stream);
    }
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

/// The controller serves the union of both contracts. Northbound and southbound
/// are stateless proxies onto it.
///
/// It backs both surfaces, so it validates both tokens and requires the one
/// matching the surface a route belongs to. Node inventory is a shared read;
/// mutations and node desired state remain separated.
///
/// `/health` is used by compose healthchecks and load balancers. The dashboard
/// (`/`, `/ui`, `/view`) ships inside this
/// binary. `/view` carries no stability guarantee.
///
/// The dashboard is unauthenticated, as is the `/status` rollup it shares its
/// data with: both are browser-reachable, and a bearer token cannot travel with a
/// page load without a cookie/session mechanism or a reverse proxy.
/// They expose topology and allocated ports, so **the controller port must not be
/// publicly exposed** — put it behind a proxy or keep it on a private network.
fn router(state: AppState, north: Guard, south: Guard) -> Router {
    let shared_reads = Router::new().route(ROUTE_NODES, get(list_nodes)).layer(
        axum::middleware::from_fn_with_state(
            SharedReadGuards {
                north: north.clone(),
                south: south.clone(),
            },
            require_shared_read_bearer,
        ),
    );

    let streams = Router::new()
        .route(ROUTE_STREAMS, get(list_streams).post(submit_stream))
        .route(ROUTE_STREAM, get(get_stream).delete(delete_stream))
        .route(ROUTE_STREAM_ENDPOINTS, get(get_endpoints))
        .route(ROUTE_STREAM_PLANS, post(plan_stream))
        .route(ROUTE_STREAM_SETS, get(list_stream_sets))
        .route(ROUTE_STREAM_SET, get(get_stream_set).put(put_stream_set))
        .layer(axum::middleware::from_fn_with_state(north, require_bearer));

    let nodes = Router::new()
        .route(ROUTE_NODE_REGISTER, post(register_node))
        .route(ROUTE_NODE_HEARTBEAT, post(node_heartbeat))
        .route(ROUTE_NODE_DESIRED, get(get_desired))
        .route(ROUTE_ENDPOINTS, get(list_endpoints))
        .route(ROUTE_STATE, get(get_state))
        .layer(axum::middleware::from_fn_with_state(south, require_bearer));

    let api = Router::new()
        .route(ROUTE_STATUS, get(get_status))
        .merge(shared_reads)
        .merge(streams)
        .merge(nodes)
        .fallback(api_route_not_found)
        .method_not_allowed_fallback(api_method_not_allowed);

    Router::new()
        .route("/", get(ui))
        .route("/ui", get(ui))
        .route("/health", get(health))
        .route("/view", get(get_view))
        .merge(api)
        .with_state(state)
}

async fn require_shared_read_bearer(
    State(guards): State<SharedReadGuards>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if guards.north.is_disabled() || guards.south.is_disabled() {
        return next.run(request).await;
    }
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            guards
                .north
                .token()
                .is_some_and(|token| token.matches_header(value))
                || guards
                    .south
                    .token()
                    .is_some_and(|token| token.matches_header(value))
        });
    if authorized {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
        Json(ApiError::new(
            ApiErrorCode::Unauthorized,
            "missing or invalid bearer token",
        )),
    )
        .into_response()
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
            topology: r.node.topology.clone(),
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
            endpoints: view.endpoints.get(&stream.name).cloned(),
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
                        profile_id: hop.profile_id.clone(),
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
        owner: stream.owner.clone(),
        spec: stream.spec.clone(),
    }
}

fn revision_etag(revision: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("\"revision-{revision}\""))
        .expect("numeric revision always forms a valid ETag")
}

fn stream_set_etag(revision: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("\"set-revision-{revision}\""))
        .expect("numeric revision always forms a valid ETag")
}

fn with_etag(mut response: Response, revision: u64) -> Response {
    response
        .headers_mut()
        .insert(header::ETAG, revision_etag(revision));
    response
}

fn with_stream_set_etag(mut response: Response, revision: u64) -> Response {
    response
        .headers_mut()
        .insert(header::ETAG, stream_set_etag(revision));
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

fn parse_stream_set_revision(value: &HeaderValue) -> Option<u64> {
    value
        .to_str()
        .ok()?
        .strip_prefix("\"set-revision-")?
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

fn stream_set_write_precondition(
    headers: &HeaderMap,
) -> Result<StreamWritePrecondition, RequestError> {
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
        (Some(value), None) => parse_stream_set_revision(value)
            .map(StreamWritePrecondition::Revision)
            .ok_or(RequestError {
                status: StatusCode::BAD_REQUEST,
                code: ApiErrorCode::InvalidRequest,
                message: "If-Match must contain one current stream-set ETag",
            }),
        (None, Some(value)) if value == "*" => Ok(StreamWritePrecondition::Absent),
        (None, Some(_)) => Err(RequestError {
            status: StatusCode::BAD_REQUEST,
            code: ApiErrorCode::InvalidRequest,
            message: "If-None-Match must be * when creating a stream set",
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

fn stream_set_resource(owner: &str, streams: &BTreeMap<String, StoredStream>) -> StreamSetResource {
    StreamSetResource {
        owner: owner.to_string(),
        streams: streams
            .values()
            .filter(|stream| stream.owner.as_deref() == Some(owner))
            .map(stream_resource)
            .collect(),
    }
}

async fn list_stream_sets(State(state): State<AppState>) -> Json<Vec<StreamSetResource>> {
    let streams = state.streams.read().await;
    let stream_sets = state.stream_sets.read().await;
    Json(
        stream_sets
            .keys()
            .map(|owner| stream_set_resource(owner, &streams))
            .collect(),
    )
}

async fn get_stream_set(State(state): State<AppState>, Path(owner): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&owner) {
        return invalid_request(
            "stream-set owner is invalid",
            vec![resource_id_issue("owner", "stream-set owner", reason)],
        );
    }
    let streams = state.streams.read().await;
    let stream_sets = state.stream_sets.read().await;
    match stream_sets.get(&owner).copied() {
        Some(revision) => with_stream_set_etag(
            Json(stream_set_resource(&owner, &streams)).into_response(),
            revision,
        ),
        None => error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::StreamSetNotFound,
            "stream set not found",
        ),
    }
}

fn validate_stream_set(owner: &str, apply: &StreamSetApply) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    if let Err(reason) = validate_resource_id(owner) {
        issues.push(resource_id_issue("owner", "stream-set owner", reason));
    }
    if apply.streams.is_empty() && !apply.prune {
        issues.push(ValidationIssue::new(
            "streams",
            "required",
            "streams must not be empty unless prune is true",
        ));
    }
    let mut names = BTreeSet::new();
    for (index, stream) in apply.streams.iter().enumerate() {
        for mut issue in validate_stream(stream) {
            issue.field = format!("streams[{index}].{}", issue.field);
            issues.push(issue);
        }
        if !names.insert(stream.name.as_str()) {
            issues.push(ValidationIssue::new(
                format!("streams[{index}].name"),
                "duplicate",
                format!("stream {:?} appears more than once", stream.name),
            ));
        }
    }
    issues
}

fn stream_set_action(action: StreamSetMemberAction) -> StreamSetAction {
    match action {
        StreamSetMemberAction::Created => StreamSetAction::Created,
        StreamSetMemberAction::Updated => StreamSetAction::Updated,
        StreamSetMemberAction::Unchanged => StreamSetAction::Unchanged,
    }
}

async fn put_stream_set(
    State(state): State<AppState>,
    Path(owner): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<StreamSetApply>, JsonRejection>,
) -> Response {
    let Json(apply) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    let issues = validate_stream_set(&owner, &apply);
    if !issues.is_empty() {
        return invalid_request("stream-set validation failed", issues);
    }
    let precondition = match stream_set_write_precondition(&headers) {
        Ok(precondition) => precondition,
        Err(error) => return error.response(),
    };

    let write = {
        let mut streams = state.streams.write().await;
        let mut stream_sets = state.stream_sets.write().await;
        let result = match precondition {
            StreamWritePrecondition::Absent => {
                state
                    .store
                    .create_stream_set(&owner, &apply.streams, apply.prune)
                    .await
            }
            StreamWritePrecondition::Revision(revision) => {
                state
                    .store
                    .update_stream_set(&owner, &apply.streams, apply.prune, revision)
                    .await
            }
        };
        let write = match result {
            Ok(write) => write,
            Err(StoreError::PreconditionFailed) => {
                return error(
                    StatusCode::PRECONDITION_FAILED,
                    ApiErrorCode::PreconditionFailed,
                    "stream set changed or the requested owner already exists",
                );
            }
            Err(StoreError::OwnershipConflict { .. }) => {
                return error(
                    StatusCode::CONFLICT,
                    ApiErrorCode::OwnershipConflict,
                    "a stream name belongs to another workflow",
                );
            }
            Err(StoreError::DuplicateStreamName { .. }) => {
                return invalid_request("stream-set validation failed", Vec::new());
            }
            Err(err) => {
                tracing::error!(%err, %owner, "persisting stream set failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::PersistenceFailed,
                    "failed to persist stream set",
                );
            }
        };
        streams.retain(|_, stream| stream.owner.as_deref() != Some(owner.as_str()));
        for stream in &write.stream_set.streams {
            streams.insert(stream.spec.name.clone(), stream.clone());
        }
        stream_sets.insert(owner.clone(), write.stream_set.revision);
        write
    };

    update_stream_set_view(&state, &write).await;
    let response = StreamSetAccepted {
        status: AcceptedState::Accepted,
        owner: owner.clone(),
        changed: write.changed,
        streams: write
            .stream_set
            .streams
            .iter()
            .map(|stream| StreamSetMemberResult {
                name: stream.spec.name.clone(),
                generation: stream.generation,
                action: stream_set_action(write.actions[&stream.spec.name]),
            })
            .collect(),
        pruned: write.deleted.clone(),
    };
    tracing::info!(%owner, changed = write.changed, "stream set accepted");
    with_stream_set_etag(
        (StatusCode::ACCEPTED, Json(response)).into_response(),
        write.stream_set.revision,
    )
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
            Err(StoreError::OwnershipConflict { .. }) => {
                return error(
                    StatusCode::CONFLICT,
                    ApiErrorCode::StreamOwned,
                    "stream belongs to a stream set",
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
    let endpoints = outcome.endpoints.remove(&stream.name);
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
        endpoints,
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
        Err(StoreError::OwnershipConflict { .. }) => {
            return error(
                StatusCode::CONFLICT,
                ApiErrorCode::StreamOwned,
                "stream belongs to a stream set",
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
    let node_issues = validate_node(&registration.node);
    if !node_issues.is_empty() {
        return invalid_request("node descriptor is invalid", node_issues);
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
            .map(|snapshot| snapshot.hops.clone())
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
            &destination.endpoint,
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

fn destination_stream(stream: &StreamDefinition, id: &str) -> StreamDefinition {
    let destination = stream
        .destinations
        .iter()
        .find(|destination| destination.id == id)
        .expect("destination status is built from the stream")
        .clone();
    StreamDefinition {
        name: stream.name.clone(),
        enabled: stream.enabled,
        source: stream.source.clone(),
        destinations: vec![destination],
    }
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
            let conditions = disabled_conditions(stream);
            stream_statuses.push(StreamStatus {
                name: stream.name.clone(),
                generation: 0,
                observed_generation: None,
                status: PathStatus::Idle,
                nodes: Vec::new(),
                destinations: stream
                    .destinations
                    .iter()
                    .map(|destination| StreamDestinationStatus {
                        id: destination.id.clone(),
                        status: PathStatus::Idle,
                        nodes: Vec::new(),
                        conditions: disabled_conditions(&destination_stream(
                            stream,
                            &destination.id,
                        )),
                        endpoint: None,
                    })
                    .collect(),
                conditions,
                ingress: None,
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
                let destination_statuses = stream
                    .destinations
                    .iter()
                    .map(|destination| {
                        let nodes = destination_nodes(&path, &destination.id);
                        let branch_status =
                            destination_path_status(&path, &destination.id, &observed.hops);
                        let offline_node = nodes
                            .iter()
                            .find(|id| offline.contains(id.as_str()))
                            .cloned();
                        let destination_stream = destination_stream(stream, &destination.id);
                        let status = if offline_node.is_some()
                            || format_conflict_reason(&destination_stream).is_some()
                        {
                            PathStatus::Degraded
                        } else {
                            branch_status
                        };
                        StreamDestinationStatus {
                            id: destination.id.clone(),
                            status,
                            nodes,
                            conditions: placed_conditions(
                                &destination_stream,
                                branch_status,
                                offline_node.as_deref(),
                            ),
                            endpoint: endpoints.as_ref().and_then(|endpoints| {
                                endpoints
                                    .destinations
                                    .iter()
                                    .find(|candidate| candidate.id == destination.id)
                                    .and_then(|candidate| candidate.endpoint.clone())
                            }),
                        }
                    })
                    .collect();
                stream_statuses.push(StreamStatus {
                    name: stream.name.clone(),
                    generation: 0,
                    observed_generation: None,
                    status,
                    nodes,
                    ingress: endpoints.as_ref().and_then(|value| value.ingress.clone()),
                    destinations: destination_statuses,
                    conditions: placed_conditions(stream, path_status, offline_node.as_deref()),
                });
                status
            }
            Err(error) => {
                tracing::warn!(stream = %stream.name, %error, "cannot place stream; retrying next tick");
                let conditions = placement_failed_conditions(stream, &error);
                stream_statuses.push(StreamStatus {
                    name: stream.name.clone(),
                    generation: 0,
                    observed_generation: None,
                    status: PathStatus::Pending,
                    nodes: Vec::new(),
                    destinations: stream
                        .destinations
                        .iter()
                        .map(|destination| StreamDestinationStatus {
                            id: destination.id.clone(),
                            status: PathStatus::Pending,
                            nodes: Vec::new(),
                            conditions: placement_failed_conditions(
                                &destination_stream(stream, &destination.id),
                                &error,
                            ),
                            endpoint: None,
                        })
                        .collect(),
                    conditions,
                    ingress: None,
                });
                PathStatus::Pending
            }
        };
        if status == PathStatus::Flowing {
            flowing += 1;
        }
    }

    for stream in &mut stream_statuses {
        stream
            .destinations
            .sort_by(|left, right| left.id.cmp(&right.id));
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
