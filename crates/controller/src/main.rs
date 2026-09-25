//! `weave-controller` — the single stateful control-plane service. It owns the
//! stream and node registries (persisted to Postgres), serves the northbound and
//! southbound HTTP surfaces, and reconciles desired streams into per-node desired
//! hops on a fixed interval, entirely from in-memory state.
//!
//! With Postgres, any number of controllers can share one database. The one
//! holding the lease serves; the others answer `503 not_leader` until it lapses.

mod desired;
#[cfg(test)]
mod hop_id_tests;
mod keys;
#[cfg(test)]
mod outside_peer_tests;
mod path;
#[cfg(test)]
mod port_hold_tests;
#[cfg(test)]
mod redundant_paths_tests;
#[cfg(test)]
mod relay_choice_tests;
#[cfg(test)]
mod rist_tests;
#[cfg(test)]
mod scale_tests;
mod store;
mod webhook;

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::{
    Extension, Json, Router,
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
use tower::ServiceExt;
use tracing_subscriber::EnvFilter;
use weave_core::auth::{
    self, Guard, NodeCaller, NodeGuard, refuse_other_node, require_bearer, require_node_token,
    unauthorized,
};
use weave_core::webhook::{EventType, NodeSummary, StreamSummary, Subject};
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

use keys::{LinkKeys, SecretSource};
use path::{
    HeldPorts, HopIds, PlacementError, PortAllocator, SinglePath, derive_stream, destination_nodes,
    destination_path_status, path_status, shared_hop_id, stream_endpoints,
};
use store::{
    LeaseTerm, MemStore, PgStore, StateStore, StoreError, StoredStream, StreamSetMemberAction,
    StreamSetWrite,
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
    /// An offline node is removed, from memory and from the store, once this
    /// many seconds elapse without a heartbeat. A node a stored stream names is
    /// kept.
    #[arg(long, env = "WEAVE_NODE_FORGET_SECS", default_value_t = 300)]
    node_forget_secs: u64,
    /// Postgres connection URL. When unset the controller runs with an
    /// in-memory store and does not persist state across restarts.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    /// With `DATABASE_URL`, how long the controller lease lasts without a
    /// renewal. A standby takes over at most this long after the leader stops.
    #[arg(long, env = "WEAVE_LEASE_TTL_SECS", default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    lease_ttl_secs: u64,
    /// Absolute URL that receives node and stream events. Webhooks are off when unset.
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
    /// Written only while holding the `nodes` write lock, so a tick never sees
    /// a node's new status beside its old heartbeat time.
    last_seen: Arc<RwLock<BTreeMap<String, Instant>>>,
    node_ttl: Duration,
    node_forget: Duration,
    desired: Arc<RwLock<BTreeMap<String, desired::DesiredSnapshot>>>,
    view: Arc<RwLock<ControllerView>>,
    /// `None` when no receiver is configured; every emit site is then a no-op.
    webhooks: Option<Arc<webhook::Emitter>>,
    keys: LinkKeys,
    unsaved: Arc<std::sync::Mutex<Unsaved>>,
}

/// Nodes and stream statuses whose last store write failed. Each tick writes
/// them again as they stand then.
#[derive(Default)]
struct Unsaved {
    nodes: BTreeSet<String>,
    streams: BTreeSet<String>,
}

impl AppState {
    fn unsaved(&self) -> std::sync::MutexGuard<'_, Unsaved> {
        self.unsaved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone)]
struct SharedReadGuards {
    north: Guard,
    south: NodeGuard,
}

impl AppState {
    async fn hydrate(
        store: Arc<dyn StateStore>,
        node_ttl: Duration,
        node_forget: Duration,
        webhooks: Option<Arc<webhook::Emitter>>,
        keys: LinkKeys,
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
        let streams: BTreeMap<String, StoredStream> = loaded_streams
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
        let statuses = store
            .load_stream_statuses()
            .await
            .context("hydrating stream statuses")?
            .into_iter()
            .filter(|status| streams.contains_key(&status.name))
            .collect();
        let boot = Instant::now();
        let last_seen = nodes.keys().map(|id| (id.clone(), boot)).collect();
        Ok(Self {
            store,
            streams: Arc::new(RwLock::new(streams)),
            stream_sets: Arc::new(RwLock::new(stream_sets)),
            nodes: Arc::new(RwLock::new(nodes)),
            last_seen: Arc::new(RwLock::new(last_seen)),
            node_ttl,
            node_forget,
            desired: Arc::new(RwLock::new(BTreeMap::new())),
            view: Arc::new(RwLock::new(ControllerView {
                streams: statuses,
                ..ControllerView::default()
            })),
            webhooks,
            keys,
            unsaved: Arc::default(),
        })
    }

    fn emit(&self, event_type: EventType, subject: impl Into<Subject>) {
        if let Some(emitter) = &self.webhooks {
            emitter.emit(event_type, subject);
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
        use weave_core::{DEVICE_TRANSPORT, RistSocket, SocketSpec, SrtSocket, Transport};

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
            SocketSpec::Rist(socket) => (
                Transport::Rist.name(),
                socket.role().name(),
                match socket {
                    RistSocket::Connect { host, .. } => Some(host.clone()),
                    RistSocket::Listen { .. } => None,
                },
                Some(socket.port()),
                None,
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

    let north = Guard::from_env(auth::NORTHBOUND_TOKEN_VAR)?;
    let south = NodeGuard::from_env(auth::SOUTHBOUND_KEY_VAR)?;
    if north.is_disabled() {
        tracing::warn!(
            "{}=1: controller serves its API without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }
    let (keys, secret_source) = LinkKeys::from_env()?;
    if secret_source == SecretSource::Generated {
        tracing::warn!(
            "{} unset: generated a random one, so every SRT link key changes when the controller restarts or another controller takes over",
            keys::SECRET_VAR
        );
    }

    let webhooks = webhook::Emitter::new(webhook::Config {
        url: args.webhook_url.clone(),
        token: args.webhook_token.clone(),
        events: args.webhook_events.clone(),
        ..webhook::Config::default()
    })
    .map(Arc::new);

    let pg = match &args.database_url {
        Some(url) => {
            tracing::info!("connecting controller store to postgres");
            Some(Arc::new(
                PgStore::connect(url)
                    .await
                    .context("opening postgres store")?,
            ))
        }
        None => {
            tracing::warn!("DATABASE_URL unset; using in-memory store (state is not persisted)");
            None
        }
    };
    let controller = Controller {
        node_ttl: Duration::from_secs(args.node_ttl_secs),
        node_forget: Duration::from_secs(args.node_forget_secs),
        webhooks,
        keys,
        north,
        south,
        interval: Duration::from_secs(args.interval_secs),
    };
    tracing::info!(interval_secs = args.interval_secs, "controller starting");

    match pg {
        Some(pg) => {
            let leadership = Leadership::default();
            let api = spawn_api_server(
                bind(&args.listen).await?,
                leadership_router(leadership.clone()),
            );
            let timing = LeaseTiming::new(Duration::from_secs(args.lease_ttl_secs));
            let result = run_with_lease(pg, &controller, &leadership, timing, shutdown()).await;
            api.abort();
            result
        }
        None => {
            let (state, app) = controller.lead(Arc::new(MemStore::new())).await?;
            let api = spawn_api_server(bind(&args.listen).await?, app);
            let result = tokio::select! {
                result = shutdown() => result,
                never = tick_every(&state, controller.interval) => match never {},
            };
            api.abort();
            result
        }
    }
}

/// Everything a controller needs to start leading, whenever it gets to.
struct Controller {
    node_ttl: Duration,
    node_forget: Duration,
    webhooks: Option<Arc<webhook::Emitter>>,
    keys: LinkKeys,
    north: Guard,
    south: NodeGuard,
    interval: Duration,
}

impl Controller {
    /// Load the stored state and run a first tick over it, giving the state the
    /// tick loop drives and the router that serves it.
    async fn lead(&self, store: Arc<dyn StateStore>) -> Result<(AppState, Router)> {
        let state = AppState::hydrate(
            store,
            self.node_ttl,
            self.node_forget,
            self.webhooks.clone(),
            self.keys.clone(),
        )
        .await?;
        let app = router_after_first_tick(&state, self.north.clone(), self.south.clone()).await;
        Ok((state, app))
    }
}

async fn tick_every(state: &AppState, interval: Duration) -> Infallible {
    loop {
        tokio::time::sleep(interval).await;
        reconcile_tick(state).await;
    }
}

async fn shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("waiting for SIGTERM")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("waiting for shutdown signal")?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("waiting for shutdown signal")?;
    tracing::info!("controller shutting down");
    Ok(())
}

/// How often the lease is renewed and how long the leader serves without a
/// renewal. `hold_for` ends a third of the TTL before the lease can expire, so
/// a leader cut off from Postgres stops serving before a standby can take over.
#[derive(Debug, Clone, Copy)]
struct LeaseTiming {
    ttl: Duration,
    renew_every: Duration,
    hold_for: Duration,
    retry_every: Duration,
}

impl LeaseTiming {
    fn new(ttl: Duration) -> Self {
        let third = ttl / 3;
        Self {
            ttl,
            renew_every: third,
            hold_for: ttl - third,
            retry_every: third.min(Duration::from_secs(1)),
        }
    }
}

/// Which router, if any, a controller on a shared store serves.
#[derive(Clone, Default)]
struct Leadership(Arc<std::sync::RwLock<Role>>);

#[derive(Default)]
enum Role {
    #[default]
    Standby,
    Taking,
    Leading(Router),
}

impl Leadership {
    fn set(&self, role: Role) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = role;
    }

    fn taking(&self) {
        self.set(Role::Taking);
    }

    /// Serve `app`, unless the lease was lost since [`Leadership::taking`].
    fn lead(&self, app: Router) -> bool {
        let mut role = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*role, Role::Taking) {
            return false;
        }
        *role = Role::Leading(app);
        true
    }

    fn stand_by(&self) {
        self.set(Role::Standby);
    }

    fn router(&self) -> Option<Router> {
        match &*self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            Role::Leading(app) => Some(app.clone()),
            Role::Standby | Role::Taking => None,
        }
    }
}

/// Serve the dashboard page and `/health` always, and everything else from the
/// leader's router, or `503 not_leader` while there is none.
fn leadership_router(leadership: Leadership) -> Router {
    Router::new()
        .route("/", get(ui))
        .route("/ui", get(ui))
        .route("/health", get(health))
        .fallback(serve_if_leading)
        .with_state(leadership)
}

async fn serve_if_leading(
    State(leadership): State<Leadership>,
    request: axum::extract::Request,
) -> Response {
    match leadership.router() {
        Some(app) => match app.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        },
        None => not_leader(),
    }
}

fn not_leader() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        ApiErrorCode::NotLeader,
        "this controller does not hold the lease; another controller leads",
    )
}

/// Take the lease whenever it is free, lead until it is lost, and stand by
/// again, until `shutdown` resolves. A lease held at shutdown is released so a
/// standby takes over at once.
async fn run_with_lease(
    pg: Arc<PgStore>,
    controller: &Controller,
    leadership: &Leadership,
    timing: LeaseTiming,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    tokio::pin!(shutdown);
    let holder = holder_id()?;
    tracing::info!(%holder, "standing by for the controller lease");
    loop {
        let (term, asked) = tokio::select! {
            taken = wait_for_lease(&pg, &holder, timing) => taken,
            result = &mut shutdown => return result,
        };
        tracing::info!(
            epoch = term.epoch,
            "took the controller lease; loading state"
        );
        leadership.taking();
        let mut hold = tokio::spawn(hold_lease(pg.clone(), leadership.clone(), asked, timing));
        if let Some(emitter) = &controller.webhooks {
            emitter.count_from(term.started_micros);
        }
        let led = tokio::select! {
            led = controller.lead(Arc::new(pg.for_term(term.epoch))) => led,
            lost = &mut hold => {
                tracing::warn!(reason = lost.unwrap_or("renewal task failed"), "lost the controller lease while loading state; standing by");
                continue;
            }
            result = &mut shutdown => {
                step_down(&pg, leadership, hold).await;
                return result;
            }
        };
        let state = match led {
            Ok((state, app)) => {
                if !leadership.lead(app) {
                    let lost = hold.await;
                    tracing::warn!(
                        reason = lost.unwrap_or("renewal task failed"),
                        "lost the controller lease while loading state; standing by"
                    );
                    continue;
                }
                state
            }
            Err(err) => {
                step_down(&pg, leadership, hold).await;
                return Err(err);
            }
        };
        tracing::info!(epoch = term.epoch, "leading");
        let lost = tokio::select! {
            lost = &mut hold => lost,
            result = &mut shutdown => {
                step_down(&pg, leadership, hold).await;
                return result;
            }
            never = tick_every(&state, controller.interval) => match never {},
        };
        tracing::warn!(
            epoch = term.epoch,
            reason = lost.unwrap_or("renewal task failed"),
            "lost the controller lease; standing by"
        );
    }
}

async fn step_down(pg: &PgStore, leadership: &Leadership, hold: JoinHandle<&'static str>) {
    hold.abort();
    leadership.stand_by();
    if let Err(err) = pg.release_lease().await {
        tracing::warn!(%err, "releasing the controller lease failed; it expires on its own");
    }
}

fn holder_id() -> Result<String> {
    let mut id = [0u8; 8];
    getrandom::fill(&mut id)
        .map_err(|err| anyhow::anyhow!("generating a lease holder id: {err}"))?;
    Ok(id.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// Try for the lease until it is taken. The instant is from before the try
/// that took it, so a hold counted from it ends no later than the lease.
async fn wait_for_lease(pg: &PgStore, holder: &str, timing: LeaseTiming) -> (LeaseTerm, Instant) {
    let mut held_elsewhere = false;
    loop {
        let asked = Instant::now();
        match pg.acquire_lease(holder, timing.ttl).await {
            Ok(Some(term)) => return (term, asked),
            Ok(None) if !held_elsewhere => {
                tracing::info!("another controller holds the lease");
                held_elsewhere = true;
            }
            Ok(None) => {}
            Err(err) => tracing::warn!(%err, "taking the controller lease failed; retrying"),
        }
        tokio::time::sleep(timing.retry_every).await;
    }
}

/// Renew the lease until a renewal is refused or none succeeds within
/// `hold_for` of the last one sent. Then stop serving and writing, before the
/// lease can expire, and say why.
async fn hold_lease(
    pg: Arc<PgStore>,
    leadership: Leadership,
    asked: Instant,
    timing: LeaseTiming,
) -> &'static str {
    let mut deadline = asked + timing.hold_for;
    let mut wait = timing.renew_every;
    let reason = loop {
        tokio::time::sleep_until((Instant::now() + wait).min(deadline).into()).await;
        let sent = Instant::now();
        if sent >= deadline {
            break "no renewal succeeded in time";
        }
        match tokio::time::timeout_at(deadline.into(), pg.renew_lease(timing.ttl)).await {
            Ok(Ok(true)) => {
                deadline = sent + timing.hold_for;
                wait = timing.renew_every;
            }
            Ok(Ok(false)) => break "the lease expired or another controller took it",
            Ok(Err(err)) => {
                tracing::warn!(%err, "renewing the controller lease failed; retrying");
                wait = timing.retry_every;
            }
            Err(_) => break "no renewal succeeded in time",
        }
    };
    leadership.stand_by();
    pg.drop_lease();
    reason
}

async fn bind(addr: &str) -> Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding controller listener on {addr}"))
}

/// The router, built only once a reconcile tick has filled the desired map.
/// Serving any earlier answers a restarted controller's nodes from nothing.
async fn router_after_first_tick(state: &AppState, north: Guard, south: NodeGuard) -> Router {
    reconcile_tick(state).await;
    router(state.clone(), north, south)
}

async fn reconcile_tick(state: &AppState) {
    let streams = state.streams.read().await;
    let definitions = streams.values().map(|stream| stream.spec.clone()).collect();
    let (observed, went_offline, forgotten) = {
        let mut nodes = state.nodes.write().await;
        let mut last_seen = state.last_seen.write().await;
        let now = Instant::now();
        let transitioned = mark_offline(&mut nodes, &last_seen, now, state.node_ttl);
        let went_offline: Vec<NodeSummary> = transitioned
            .iter()
            .filter_map(|id| nodes.get(id))
            .map(|registration| NodeSummary::from(&registration.node))
            .collect();
        let mut to_store = std::mem::take(&mut state.unsaved().nodes);
        to_store.extend(transitioned);
        for registration in to_store.iter().filter_map(|id| nodes.get(id)) {
            if let Err(err) = state.store.upsert_node(registration).await {
                tracing::warn!(%err, node_id = %registration.node.id, "storing a node failed; retrying next tick");
                state.unsaved().nodes.insert(registration.node.id.clone());
            }
        }
        let named = named_nodes(streams.values().map(|stream| &stream.spec));
        let mut forgotten = Vec::new();
        for id in forgettable(&nodes, &last_seen, &named, now, state.node_forget) {
            if let Err(err) = state.store.delete_node(&id).await {
                tracing::warn!(%err, node_id = %id, "forgetting an offline node failed; retrying next tick");
                continue;
            }
            last_seen.remove(&id);
            if let Some(registration) = nodes.remove(&id) {
                tracing::info!(node_id = %id, "forgot an offline node that no stream names");
                forgotten.push(NodeSummary::from(&registration.node));
            }
        }
        (observed_state(&nodes), went_offline, forgotten)
    };
    for node in went_offline {
        state.emit(EventType::NodeOffline, node);
    }
    for node in forgotten {
        state.emit(EventType::NodeForgotten, node);
    }
    let mut outcome = reconcile(definitions, &observed, &state.keys);
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
    let changed = changed_streams(&outcome.streams, &view.streams);
    let mut to_store = std::mem::take(&mut state.unsaved().streams);
    to_store.extend(changed.iter().map(|stream| stream.name.clone()));
    let statuses: Vec<StreamStatus> = outcome
        .streams
        .iter()
        .filter(|stream| to_store.contains(&stream.name))
        .cloned()
        .collect();
    view.report = Some(outcome.report);
    view.streams = outcome.streams;
    view.endpoints = outcome.endpoints;
    view.hops = outcome.hops_by_stream;
    drop(view);
    drop(streams);
    if !statuses.is_empty()
        && let Err(err) = state.store.save_stream_statuses(&statuses).await
    {
        tracing::warn!(%err, "storing stream statuses failed; retrying next tick");
        state
            .unsaved()
            .streams
            .extend(statuses.into_iter().map(|stream| stream.name));
    }
    for stream in &changed {
        state.emit(EventType::StreamChanged, StreamSummary::from(stream));
    }
}

/// Every stream whose conditions differ from the previous tick's by type,
/// status or reason, on the stream or on any destination. A stream no earlier
/// tick computed has changed; one only accepted since then has a placeholder
/// status with no `observed_generation`.
fn changed_streams(current: &[StreamStatus], previous: &[StreamStatus]) -> Vec<StreamStatus> {
    current
        .iter()
        .filter(|stream| {
            previous
                .iter()
                .find(|candidate| {
                    candidate.name == stream.name && candidate.observed_generation.is_some()
                })
                .is_none_or(|previous| condition_keys(previous) != condition_keys(stream))
        })
        .cloned()
        .collect()
}

type ConditionKey<'a> = (
    Option<&'a str>,
    StreamConditionType,
    StreamConditionStatus,
    StreamConditionReason,
);

fn condition_keys(stream: &StreamStatus) -> Vec<ConditionKey<'_>> {
    std::iter::once((None, &stream.conditions))
        .chain(
            stream
                .destinations
                .iter()
                .map(|destination| (Some(destination.id.as_str()), &destination.conditions)),
        )
        .flat_map(|(destination, conditions)| {
            conditions.iter().map(move |condition| {
                (
                    destination,
                    condition.condition_type,
                    condition.status,
                    condition.reason,
                )
            })
        })
        .collect()
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

/// Offline nodes that have not heartbeated for longer than `after` and that
/// `named` does not hold.
fn forgettable(
    nodes: &BTreeMap<String, NodeRegistration>,
    last_seen: &BTreeMap<String, Instant>,
    named: &BTreeSet<&str>,
    now: Instant,
    after: Duration,
) -> Vec<String> {
    nodes
        .iter()
        .filter(|(id, registration)| {
            registration.node.status == NodeStatus::Offline
                && !named.contains(id.as_str())
                && last_seen
                    .get(*id)
                    .is_some_and(|seen| now.saturating_duration_since(*seen) > after)
        })
        .map(|(id, _)| id.clone())
        .collect()
}

/// Every node a stream names as its source, a destination, or a `via` relay.
fn named_nodes<'a>(streams: impl IntoIterator<Item = &'a StreamDefinition>) -> BTreeSet<&'a str> {
    let mut named = BTreeSet::new();
    for stream in streams {
        let endpoints = std::iter::once(&stream.source).chain(
            stream
                .destinations
                .iter()
                .map(|destination| &destination.endpoint),
        );
        for endpoint in endpoints {
            named.extend(endpoint.node());
            if let weave_core::StreamTransport::Srt(srt) = endpoint {
                named.extend(srt.via.iter().map(String::as_str));
            }
        }
    }
    named
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
/// It backs both surfaces, so it validates both kinds of token and requires the
/// one matching the surface a route belongs to. Node inventory is a shared read
/// open to the northbound token and to any node token; mutations and node
/// desired state remain separated, and a node token acts only for its own node.
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
fn router(state: AppState, north: Guard, south: NodeGuard) -> Router {
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
        .layer(axum::middleware::from_fn_with_state(
            south,
            require_node_token,
        ));

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
                || guards.south.caller(Some(value)).is_some()
        });
    if authorized {
        return next.run(request).await;
    }
    unauthorized()
}

fn spawn_api_server(listener: tokio::net::TcpListener, app: Router) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        if let Ok(addr) = listener.local_addr() {
            tracing::info!(%addr, "controller API listening");
        }
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
        if let Some(conflict) = stream_set_hop_id_conflict(&owner, &apply, &streams) {
            return conflict;
        }
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
            Err(StoreError::NotLeader) => return not_leader(),
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

/// The first of `others` that can plan a hop id `stream` can, and that id.
fn first_shared_hop_id<'a>(
    stream: &StreamDefinition,
    others: impl IntoIterator<Item = &'a StreamDefinition>,
) -> Option<(&'a str, String)> {
    others
        .into_iter()
        .find_map(|other| shared_hop_id(stream, other).map(|hop| (other.name.as_str(), hop)))
}

/// A stream-set write refused because a stream it changes can plan a hop id
/// another stream can: one kept from the store, or another stream in the write.
/// Streams the write replaces or prunes are not kept, and an unchanged stream
/// is not checked, so reapplying a set is never refused for a collision it
/// already held.
fn stream_set_hop_id_conflict(
    owner: &str,
    apply: &StreamSetApply,
    stored: &BTreeMap<String, StoredStream>,
) -> Option<Response> {
    let written: BTreeSet<&str> = apply
        .streams
        .iter()
        .map(|stream| stream.name.as_str())
        .collect();
    let kept: Vec<&StreamDefinition> = stored
        .values()
        .filter(|stream| !written.contains(stream.spec.name.as_str()))
        .filter(|stream| !(apply.prune && stream.owner.as_deref() == Some(owner)))
        .map(|stream| &stream.spec)
        .collect();
    apply
        .streams
        .iter()
        .enumerate()
        .filter(|(_, stream)| {
            stored
                .get(&stream.name)
                .is_none_or(|current| current.spec != **stream)
        })
        .find_map(|(index, stream)| {
            let others = kept.iter().copied().chain(
                apply
                    .streams
                    .iter()
                    .filter(|other| other.name != stream.name),
            );
            first_shared_hop_id(stream, others).map(|(other, hop)| {
                hop_id_conflict(&format!("streams[{index}].name"), &stream.name, other, &hop)
            })
        })
}

fn hop_id_conflict(field: &str, stream: &str, other: &str, hop: &str) -> Response {
    let message =
        format!("stream {stream} can plan hop id {hop}, which stream {other} can also plan");
    ApiError::with_details(
        ApiErrorCode::HopIdConflict,
        format!("stream {stream} and stream {other} can plan the same hop id"),
        vec![ValidationIssue::new(field, "hop_id_conflict", message)],
    )
    .response(StatusCode::CONFLICT)
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
        let current = streams.get(&name);
        let changed = current.is_none_or(|current| current.spec != stream);
        let owned = current.is_some_and(|current| current.owner.is_some());
        if changed
            && !owned
            && let Some((other, hop)) = first_shared_hop_id(
                &stream,
                streams
                    .values()
                    .map(|stored| &stored.spec)
                    .filter(|stored| stored.name != name),
            )
        {
            return hop_id_conflict("name", &name, other, &hop);
        }
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
            Err(StoreError::NotLeader) => return not_leader(),
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
    let observed = observed_state(&nodes);
    drop(nodes);
    let mut outcome = reconcile(streams.into_values().collect(), &observed, &state.keys);
    let endpoints = outcome.endpoints.remove(&stream.name);
    let planned = outcome
        .streams
        .into_iter()
        .find(|status| status.name == stream.name)
        .expect("candidate stream is included in plan");
    let mut hops = outcome
        .hops_by_stream
        .remove(&stream.name)
        .unwrap_or_default();
    withhold_passphrases(&mut hops);
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
        reason: planned
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::PlacementReady)
            .filter(|condition| {
                status == PlanStatus::Unplaced
                    || condition.reason == StreamConditionReason::SinglePath
            })
            .map(|condition| condition.detail.clone()),
    })
    .into_response()
}

/// Drop every SRT passphrase from `hops`, leaving `pbkeylen` to show which
/// sockets are keyed. Keys reach adapters through their desired hops and
/// nowhere else.
fn withhold_passphrases(hops: &mut [DesiredHop]) {
    for socket in hops.iter_mut().flat_map(DesiredHop::sockets_mut) {
        if let weave_core::SocketSpec::Srt(socket) = socket {
            socket.params_mut().passphrase = None;
        }
    }
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
        Err(StoreError::NotLeader) => return not_leader(),
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
    Extension(caller): Extension<NodeCaller>,
    payload: Result<Json<NodeRegistration>, JsonRejection>,
) -> Response {
    let Json(registration) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    let node_id = registration.node.id.clone();
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
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
    if let Some(response) = refuse_other_nodes_endpoints(&caller, &registration.endpoints) {
        return response;
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
        match state.store.upsert_node(&registration).await {
            Ok(()) => {}
            Err(StoreError::NotLeader) => return not_leader(),
            Err(err) => {
                tracing::error!(%err, %node_id, "persisting node registration failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::PersistenceFailed,
                    "failed to persist node registration",
                );
            }
        }
        nodes.insert(node_id.clone(), registration);
        state
            .last_seen
            .write()
            .await
            .insert(node_id.clone(), Instant::now());
    }
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

/// A heartbeat writes the store only when [`reported`] changes, so a controller
/// that starts or takes over plans from what each node last reported.
async fn node_heartbeat(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    Path(node_id): Path<String>,
    payload: Result<Json<NodeHeartbeat>, JsonRejection>,
) -> Response {
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
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
    if let Some(response) = refuse_other_nodes_endpoints(&caller, &heartbeat.endpoints) {
        return response;
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
    let mut updated = registration.clone();
    updated.node.status = heartbeat.status;
    updated.endpoints = heartbeat.endpoints;
    updated.hop_status = heartbeat.hop_status;
    if reported(&updated) != reported(registration) {
        match state.store.upsert_node(&updated).await {
            Ok(()) => {}
            Err(StoreError::NotLeader) => return not_leader(),
            Err(err) => {
                tracing::warn!(%err, %node_id, "storing a changed node report failed; retrying next tick");
                state.unsaved().nodes.insert(node_id.clone());
            }
        }
    }
    *registration = updated;
    let status = registration.node.status;
    let recovered = (was_offline && status != NodeStatus::Offline)
        .then(|| NodeSummary::from(&registration.node));
    state
        .last_seen
        .write()
        .await
        .insert(node_id.clone(), Instant::now());
    drop(nodes);
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

type HopReport<'a> = (
    &'a str,
    weave_core::HopState,
    weave_core::LinkCondition,
    Option<weave_core::LinkCondition>,
    Vec<(&'a str, weave_core::LinkCondition)>,
);

/// What planning and stream conditions read from a node's reports: its status
/// and each hop's state and socket conditions, without rates or addresses.
fn reported(registration: &NodeRegistration) -> (NodeStatus, Vec<HopReport<'_>>) {
    (
        registration.node.status,
        registration
            .hop_status
            .iter()
            .map(|hop| {
                (
                    hop.id.as_str(),
                    hop.state,
                    hop.ingress.condition,
                    hop.merge_ingress.as_ref().map(|socket| socket.condition),
                    hop.egresses
                        .iter()
                        .map(|egress| (egress.branch_id.as_str(), egress.status.condition))
                        .collect(),
                )
            })
            .collect(),
    )
}

/// Serve the desired hops computed for a node on the last reconcile tick. A
/// node that tick did not cover gets `404`, never an empty list: an adapter
/// removes every hop it runs when told it should run none.
async fn get_desired(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    Path(node_id): Path<String>,
) -> Response {
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
    if let Err(reason) = validate_resource_id(&node_id) {
        return invalid_request(
            "node id is invalid",
            vec![resource_id_issue("node_id", "node id", reason)],
        );
    }
    match state.desired.read().await.get(&node_id) {
        Some(snapshot) => Json(snapshot.hops.clone()).into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::NodeNotFound,
            "no desired state for this node yet",
        ),
    }
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

/// The `403` for an endpoint whose `node_id` names a node `caller` may not act
/// as, `None` when every endpoint is the caller's or names no node.
fn refuse_other_nodes_endpoints(
    caller: &NodeCaller,
    endpoints: &[EndpointDescriptor],
) -> Option<Response> {
    endpoints
        .iter()
        .filter_map(|endpoint| endpoint.node_id.as_deref())
        .find_map(|node_id| refuse_other_node(caller, node_id))
}

/// The constraint the placed sender's profile puts on the media entering it.
struct IngressAccepts<'a> {
    node: &'a str,
    profile: &'a str,
    accepts: weave_core::FormatConstraint,
}

/// The sender hop's profile constraint, when its profile declares one, on the
/// tracks the hop carries: a hop built for video alone does not need audio.
fn ingress_accepts<'a>(
    path: &'a weave_core::Path,
    nodes: &'a [NodeDescriptor],
) -> Option<IngressAccepts<'a>> {
    let sender = path.hops.first()?;
    let profile = nodes
        .iter()
        .find(|node| node.id == sender.node_id)?
        .capabilities
        .hop_profiles
        .iter()
        .find(|profile| profile.id == sender.profile_id)?;
    let mut accepts = profile.accepts.clone()?;
    if let Some(tracks) = &sender.tracks {
        if !tracks.contains(&weave_core::Track::Video) {
            accepts.video = None;
        }
        if !tracks.contains(&weave_core::Track::Audio) {
            accepts.audio = None;
        }
    }
    Some(IngressAccepts {
        node: &sender.node_id,
        profile: &profile.id,
        accepts,
    })
}

/// One line naming the sender that cannot take the declared source format and
/// every destination that cannot accept it, or `None` when nothing declared
/// conflicts.
///
/// Reported, never acted on. The hops are placed and the media flows either way;
/// what this says is that it will arrive somewhere it cannot be decoded, which is
/// worth knowing long before anything can convert it.
fn format_conflict_reason(
    stream: &StreamDefinition,
    ingress: Option<&IngressAccepts>,
) -> Option<String> {
    let ingress_conflict = ingress
        .zip(stream.source.format())
        .and_then(|(ingress, format)| {
            let mismatches = ingress.accepts.mismatches(format);
            (!mismatches.is_empty()).then(|| {
                format!(
                    "node {} cannot take the source format through profile {}: {}",
                    ingress.node,
                    ingress.profile,
                    mismatches
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ")
                )
            })
        });
    let conflicts: Vec<String> = ingress_conflict
        .into_iter()
        .chain(
            weave_core::stream_format_conflicts(stream)
                .iter()
                .map(ToString::to_string),
        )
        .collect();
    (!conflicts.is_empty()).then(|| conflicts.join("; "))
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

fn format_condition(
    stream: &StreamDefinition,
    ingress: Option<&IngressAccepts>,
) -> StreamCondition {
    let source_declared = stream.source.format().is_some();
    let constrained = ingress.is_some()
        || stream
            .destinations
            .iter()
            .any(|destination| destination.endpoint.accepts().is_some());
    if !source_declared || !constrained {
        return stream_condition(
            StreamConditionType::FormatCompatible,
            StreamConditionStatus::Unknown,
            StreamConditionReason::FormatUnknown,
            "source format or destination constraints are not declared",
        );
    }
    match format_conflict_reason(stream, ingress) {
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
        format_condition(stream, None),
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
    let placement_reason = if matches!(error, PlacementError::CleartextLink { .. }) {
        StreamConditionReason::CleartextNotAllowed
    } else {
        StreamConditionReason::PlacementFailed
    };
    vec![
        stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::False,
            placement_reason,
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
        format_condition(stream, None),
        media_condition(PathStatus::Pending),
    ]
}

/// One line naming every destination in `shortfalls` and why it got one path of
/// the two it asked for, or `None` when there are none.
fn single_path_detail<'a>(shortfalls: impl IntoIterator<Item = &'a SinglePath>) -> Option<String> {
    let details: Vec<String> = shortfalls
        .into_iter()
        .map(|shortfall| {
            format!(
                "destination {} has one path of two: {}",
                shortfall.destination, shortfall.reason
            )
        })
        .collect();
    (!details.is_empty()).then(|| details.join("; "))
}

fn placed_conditions(
    stream: &StreamDefinition,
    ingress: Option<&IngressAccepts>,
    path_status: PathStatus,
    offline_node: Option<&str>,
    single_path: Option<&str>,
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
    let placement = match single_path {
        Some(detail) => stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::True,
            StreamConditionReason::SinglePath,
            detail,
        ),
        None => stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::True,
            StreamConditionReason::Placed,
            "the stream has a complete path",
        ),
    };
    vec![
        placement,
        nodes,
        hops,
        format_condition(stream, ingress),
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
        allow_cleartext_links: false,
        source: stream.source.clone(),
        destinations: vec![destination],
    }
}

/// Compute per-node desired hops, endpoints, and an aggregate report from the
/// current stream and node state. Pure: no IO, deterministic for a given input.
fn reconcile(
    mut streams: Vec<StreamDefinition>,
    observed: &ObservedState,
    keys: &LinkKeys,
) -> ReconcileOutcome {
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
    let mut ports =
        PortAllocator::holding(HeldPorts::from_reports(&observed.hops, &observed.nodes));
    let mut hop_ids = HopIds::default();

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

        let mut stream_ports = ports.clone();
        let placed = derive_stream(
            stream,
            &observed.nodes,
            &observed.hops,
            &mut stream_ports,
            keys,
        )
        .and_then(|planned| hop_ids.claim(&planned.path).map(|()| planned));
        let status = match placed {
            Ok(planned) => {
                ports = stream_ports;
                let path = planned.path;
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
                let ingress = ingress_accepts(&path, &observed.nodes);
                let status = if offline_node.is_some()
                    || format_conflict_reason(stream, ingress.as_ref()).is_some()
                {
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
                            || format_conflict_reason(&destination_stream, ingress.as_ref())
                                .is_some()
                        {
                            PathStatus::Degraded
                        } else {
                            branch_status
                        };
                        let single_path = single_path_detail(
                            planned
                                .single_path
                                .iter()
                                .filter(|shortfall| shortfall.destination == destination.id),
                        );
                        StreamDestinationStatus {
                            id: destination.id.clone(),
                            status,
                            nodes,
                            conditions: placed_conditions(
                                &destination_stream,
                                ingress.as_ref(),
                                branch_status,
                                offline_node.as_deref(),
                                single_path.as_deref(),
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
                    conditions: placed_conditions(
                        stream,
                        ingress.as_ref(),
                        path_status,
                        offline_node.as_deref(),
                        single_path_detail(&planned.single_path).as_deref(),
                    ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use weave_core::{
        HopEndpointClass, HopProfile, NetworkAttachment, NetworkListeners, NodeCapabilities,
        NodeTopology, PortRange, RoleSet, SrtEndpoint, SrtListener, StreamDestination,
        StreamTransport, Transport, TransportClass,
    };

    use crate::webhook::tests::{Sink, sink};

    fn open_router(state: AppState) -> Router {
        router(state, Guard::Disabled, NodeGuard::Disabled)
    }

    fn mem_state() -> (AppState, Arc<MemStore>) {
        let mem = Arc::new(MemStore::new());
        let state = AppState {
            store: mem.clone(),
            streams: Arc::new(RwLock::new(BTreeMap::new())),
            stream_sets: Arc::new(RwLock::new(BTreeMap::new())),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            last_seen: Arc::new(RwLock::new(BTreeMap::new())),
            node_ttl: Duration::from_secs(15),
            node_forget: Duration::from_secs(300),
            desired: Arc::new(RwLock::new(BTreeMap::new())),
            view: Arc::new(RwLock::new(ControllerView::default())),
            webhooks: None,
            keys: LinkKeys::for_tests(),
            unsaved: Arc::default(),
        };
        (state, mem)
    }

    fn srt_forward() -> HopProfile {
        let srt = HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        });
        HopProfile {
            id: "srt-forward".to_string(),
            ingress: srt.clone(),
            egress: srt,
            max_egresses: None,
            merge: false,
            accepts: None,
        }
    }

    /// A node on the `internet` network that dials out and listens for SRT on
    /// `host`.
    fn node_registration(id: &str, host: &str) -> NodeRegistration {
        NodeRegistration {
            protocol_version: PROTOCOL_VERSION,
            node: NodeDescriptor {
                id: id.to_string(),
                endpoint: format!("http://{id}:8080"),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities {
                    adapters: Vec::new(),
                    hop_profiles: vec![srt_forward()],
                },
                topology: NodeTopology {
                    attachments: vec![NetworkAttachment {
                        id: "wan".to_string(),
                        network: "internet".to_string(),
                        dial: true,
                        listeners: NetworkListeners {
                            srt: Some(SrtListener {
                                host: host.to_string(),
                                port_range: PortRange {
                                    start: 7000,
                                    end: 7999,
                                },
                            }),
                            whip: None,
                            whep: None,
                            rist: None,
                        },
                    }],
                },
            },
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    /// A node behind NAT: it dials out to `internet`, and listens only on its
    /// own site network for a local producer or consumer.
    fn nat_registration(id: &str, host: &str) -> NodeRegistration {
        let mut registration = node_registration(id, host);
        let attachments = &mut registration.node.topology.attachments;
        let mut site = attachments[0].clone();
        site.id = "site".to_string();
        site.network = format!("{id}-local");
        attachments[0].listeners.srt = None;
        attachments.push(site);
        registration
    }

    fn srt_endpoint(node: &str, latency: u32) -> StreamTransport {
        StreamTransport::Srt(SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            format: None,
            accepts: None,
            network: None,
            latency: Some(latency),
            passphrase: None,
        })
    }

    fn stream_between(name: &str, source: &str, destination: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            allow_cleartext_links: false,
            source: srt_endpoint(source, 200),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                paths: 1,
                endpoint: srt_endpoint(destination, 1000),
            }],
        }
    }

    fn stream(name: &str) -> StreamDefinition {
        stream_between(name, "strom-node-1", "strom-node-2")
    }

    fn stored_stream(spec: StreamDefinition) -> StoredStream {
        StoredStream {
            spec,
            generation: 1,
            revision: 1,
            owner: None,
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
        if method == "POST" && uri == "/streams" {
            request = request.header("if-none-match", "*");
        }
        if method == "DELETE" && uri.starts_with("/streams/") {
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

    async fn desired_hops(app: &Router, node_id: &str) -> (StatusCode, Value) {
        send(app, "GET", &format!("/nodes/{node_id}/desired"), None).await
    }

    async fn two_nodes_and_basic(state: &AppState) {
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

    #[tokio::test]
    async fn desired_reflects_computed_hops_after_a_reconcile_tick() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "sender hop placed on node 1");
        assert_eq!(hops[0].id, "weave-basic-sender");

        let (_, body) = desired_hops(&app, "strom-node-2").await;
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "receiver hop placed on node 2");
        assert_eq!(hops[0].id, "weave-basic-receiver-studio");
    }

    #[tokio::test]
    async fn post_stream_then_get_returns_it_and_writes_through() {
        let (state, mem) = mem_state();
        let app = open_router(state);

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
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert_eq!(listed[0].generation, 1);
        assert_eq!(listed[0].spec, stream("basic"));

        let (status, body) = send(&app, "GET", "/streams/basic", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_value::<StreamResource>(body).unwrap(),
            StreamResource {
                generation: 1,
                owner: None,
                spec: stream("basic")
            }
        );

        let (status, body) = send(&app, "GET", "/streams/missing", None).await;
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
            "/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(body["code"], "precondition_required");

        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
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
            "/streams",
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
            "/streams",
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
            "/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["code"], "precondition_failed");

        let (status, headers, body) =
            send_with_headers(&app, "GET", "/streams/basic", None, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response_etag(&headers), second_etag);
        let resource: StreamResource = serde_json::from_value(body).unwrap();
        assert_eq!(resource.generation, 2);
        assert_eq!(resource.spec, changed);
    }

    #[tokio::test]
    async fn stream_set_apply_retains_noops_and_prunes_atomically() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let alpha = stream("alpha");
        let beta = stream("beta");
        let create = StreamSetApply {
            streams: vec![alpha.clone(), beta.clone()],
            prune: false,
        };
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(create).unwrap()),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let first_etag = response_etag(&headers);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert!(accepted.changed);
        assert_eq!(accepted.streams.len(), 2);
        assert!(
            accepted
                .streams
                .iter()
                .all(|stream| stream.action == StreamSetAction::Created)
        );

        let mut changed_alpha = alpha;
        changed_alpha.enabled = false;
        let gamma = stream("gamma");
        let update = StreamSetApply {
            streams: vec![changed_alpha.clone(), gamma.clone()],
            prune: false,
        };
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(&update).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let second_etag = response_etag(&headers);
        assert_ne!(second_etag, first_etag);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(
            accepted
                .streams
                .iter()
                .map(|stream| (stream.name.as_str(), stream.generation, stream.action))
                .collect::<Vec<_>>(),
            [
                ("alpha", 2, StreamSetAction::Updated),
                ("beta", 1, StreamSetAction::Unchanged),
                ("gamma", 1, StreamSetAction::Created),
            ]
        );

        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(&update).unwrap()),
            &[("if-match", &second_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), second_etag);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert!(!accepted.changed);
        assert!(
            accepted
                .streams
                .iter()
                .all(|stream| stream.action == StreamSetAction::Unchanged)
        );

        let prune = StreamSetApply {
            streams: vec![changed_alpha.clone()],
            prune: true,
        };
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(prune).unwrap()),
            &[("if-match", &second_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let third_etag = response_etag(&headers);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.pruned, ["beta", "gamma"]);

        let mut stale = changed_alpha;
        stale.enabled = true;
        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stale],
                    prune: true,
                })
                .unwrap(),
            ),
            &[("if-match", &second_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["code"], "precondition_failed");

        let (status, headers, body) =
            send_with_headers(&app, "GET", "/stream-sets/studio-a", None, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response_etag(&headers), third_etag);
        let resource: StreamSetResource = serde_json::from_value(body).unwrap();
        assert_eq!(resource.streams.len(), 1);
        assert_eq!(resource.streams[0].owner.as_deref(), Some("studio-a"));
        assert_eq!(resource.streams[0].spec.name, "alpha");
    }

    #[tokio::test]
    async fn stream_set_conflicts_do_not_partially_apply() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("taken")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream("new"), stream("taken")],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "ownership_conflict");
        let (status, _) = send(&app, "GET", "/streams/new", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&app, "GET", "/stream-sets/studio-a", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// Its receiver for `a-sender` is `weave-x-receiver-a-sender`, which is also
    /// the sender of `x-receiver-a`.
    fn stream_x_to_a_sender() -> StreamDefinition {
        let mut stream = stream("x");
        stream.destinations[0].id = "a-sender".to_string();
        stream
    }

    const X_CONFLICT: &str = "stream x can plan hop id weave-x-receiver-a-sender, \
                              which stream x-receiver-a can also plan";

    fn assert_hop_id_conflict(body: &Value, field: &str) {
        assert_eq!(body["code"], "hop_id_conflict");
        assert_eq!(
            body["message"],
            "stream x and stream x-receiver-a can plan the same hop id"
        );
        assert_eq!(body["details"][0]["field"], field);
        assert_eq!(body["details"][0]["code"], "hop_id_conflict");
        assert_eq!(body["details"][0]["message"], X_CONFLICT);
    }

    async fn register_both_nodes(state: &AppState) {
        let mut nodes = state.nodes.write().await;
        for (id, host) in [
            ("strom-node-1", "172.26.0.10"),
            ("strom-node-2", "172.27.0.10"),
        ] {
            nodes.insert(id.to_string(), node_registration(id, host));
        }
    }

    #[tokio::test]
    async fn a_stream_that_can_plan_a_stored_streams_hop_id_is_refused() {
        let (state, mem) = mem_state();
        register_both_nodes(&state).await;
        let app = open_router(state.clone());
        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("x-receiver-a")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        assert!(state.view.read().await.hops.contains_key("x-receiver-a"));

        let (status, body) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream_x_to_a_sender()).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_hop_id_conflict(&body, "name");
        assert_eq!(
            mem.upsert_stream_calls(),
            1,
            "the refused stream is not stored"
        );

        reconcile_tick(&state).await;
        let view = state.view.read().await;
        assert!(
            view.hops.contains_key("x-receiver-a"),
            "the stored stream stays placed"
        );
        assert!(!view.streams.iter().any(|status| status.name == "x"));
    }

    #[tokio::test]
    async fn a_stream_set_write_that_can_plan_a_kept_streams_hop_id_writes_nothing() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("x-receiver-a")).unwrap()),
        )
        .await;

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream("alpha"), stream_x_to_a_sender()],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_hop_id_conflict(&body, "streams[1].name");
        let (status, _) = send(&app, "GET", "/stream-sets/studio-a", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&app, "GET", "/streams/alpha", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn two_streams_in_one_set_write_that_can_plan_the_same_hop_id_are_refused() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream_x_to_a_sender(), stream("x-receiver-a")],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_hop_id_conflict(&body, "streams[0].name");
    }

    #[tokio::test]
    async fn a_set_write_may_prune_the_stream_it_would_share_a_hop_id_with() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let apply =
            |streams, prune| Some(serde_json::to_value(StreamSetApply { streams, prune }).unwrap());
        let (status, headers, _) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(vec![stream("x-receiver-a")], false),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let etag = response_etag(&headers);

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(vec![stream_x_to_a_sender()], false),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "without prune the member stays"
        );
        assert_hop_id_conflict(&body, "streams[0].name");

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(vec![stream_x_to_a_sender()], true),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.pruned, ["x-receiver-a"]);
    }

    /// Streams stored before the apply check could already collide. Reapplying
    /// them unchanged is a no-op, and planning keeps the earlier name.
    #[tokio::test]
    async fn streams_that_already_collide_reapply_unchanged_and_plan_in_name_order() {
        let (state, mem) = mem_state();
        register_both_nodes(&state).await;
        let loose = mem.create_stream(&stream("x-receiver-a")).await.unwrap();
        state
            .streams
            .write()
            .await
            .insert(loose.spec.name.clone(), loose);
        let set = mem
            .create_stream_set("studio-a", &[stream_x_to_a_sender()], false)
            .await
            .unwrap();
        for stored in &set.stream_set.streams {
            state
                .streams
                .write()
                .await
                .insert(stored.spec.name.clone(), stored.clone());
        }
        state
            .stream_sets
            .write()
            .await
            .insert("studio-a".to_string(), set.stream_set.revision);
        let app = open_router(state.clone());

        let (_, headers, _) =
            send_with_headers(&app, "GET", "/streams/x-receiver-a", None, &[]).await;
        let etag = response_etag(&headers);
        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("x-receiver-a")).unwrap()),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), etag);
        assert_eq!(body["changed"], false);

        let (_, headers, _) =
            send_with_headers(&app, "GET", "/stream-sets/studio-a", None, &[]).await;
        let etag = response_etag(&headers);
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream_x_to_a_sender()],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), etag);
        assert_eq!(body["changed"], false);
        assert_eq!(body["streams"][0]["generation"], 1);

        reconcile_tick(&state).await;
        let view = state.view.read().await;
        assert!(view.hops.contains_key("x"));
        let later = view
            .streams
            .iter()
            .find(|status| status.name == "x-receiver-a")
            .unwrap();
        assert_eq!(later.status, PathStatus::Pending);
        assert!(later.conditions.iter().any(|condition| condition.detail
            == "hop id weave-x-receiver-a-sender is already planned for stream x"));
    }

    #[tokio::test]
    async fn owned_streams_reject_single_resource_mutations() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, _, _) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream("alpha")],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (_, headers, _) = send_with_headers(&app, "GET", "/streams/alpha", None, &[]).await;
        let stream_etag = response_etag(&headers);

        for (method, body) in [
            ("POST", Some(serde_json::to_value(stream("alpha")).unwrap())),
            ("DELETE", None),
        ] {
            let (status, _, body) = send_with_headers(
                &app,
                method,
                if method == "POST" {
                    "/streams"
                } else {
                    "/streams/alpha"
                },
                body,
                &[("if-match", &stream_etag)],
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(body["code"], "stream_owned");
        }
    }

    #[tokio::test]
    async fn delete_stream_removes_and_writes_through() {
        let (state, mem) = mem_state();
        let app = open_router(state);

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
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());

        let (status, _) = send(&app, "DELETE", "/streams/basic", None).await;
        assert_eq!(
            status,
            StatusCode::PRECONDITION_FAILED,
            "second delete fails its revision precondition"
        );
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
                ingress: None,
                destinations: Vec::new(),
            }];
        }
        let app = open_router(state);
        let (status, _) = send(&app, "GET", "/streams/basic/endpoints", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let (status, _) = send(&app, "GET", "/streams/nope/endpoints", None).await;
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
            let mut preview = definition.destinations[0].clone();
            preview.id = "preview".to_string();
            definition.destinations.push(preview);
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
            merge_ingress: None,
            egresses: vec![
                weave_core::EgressStatus {
                    branch_id: "preview".to_string(),
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
                    branch_id: "studio".to_string(),
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
        assert_eq!(sender["egresses"][0]["branch_id"], "preview");
        assert_eq!(sender["egresses"][0]["condition"], "flowing");
        assert_eq!(sender["egresses"][0]["stats"]["rate_mbps"], 3.1);
        assert_eq!(sender["egresses"][0]["mode"], "connect");
        assert_eq!(sender["egresses"][0]["host"], "172.27.0.10");
        assert_eq!(sender["egresses"][1]["branch_id"], "studio");
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
    async fn invalid_json_has_a_structured_error() {
        let (state, mem) = mem_state();
        let request = Request::builder()
            .method("POST")
            .uri("/streams")
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
    async fn invalid_resource_paths_are_rejected() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        for (method, uri, body) in [
            ("GET", "/streams/foo%3Fignored", None),
            ("DELETE", "/streams/foo%3Fignored", None),
            ("GET", "/streams/foo%3Fignored/endpoints", None),
            (
                "POST",
                "/nodes/foo%3Fignored/heartbeat",
                Some(json!({ "node_id": "foo?ignored", "status": "ready" })),
            ),
            ("GET", "/nodes/foo%3Fignored/desired", None),
        ] {
            let (status, _) = send(&app, method, uri, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {uri}");
        }

        assert_eq!(mem.delete_stream_calls(), 0);
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

    pub(super) const NORTH_ROUTES: [(&str, &str); 9] = [
        ("GET", "/streams"),
        ("POST", "/streams"),
        ("POST", "/stream-plans"),
        ("GET", "/streams/basic"),
        ("DELETE", "/streams/basic"),
        ("GET", "/streams/basic/endpoints"),
        ("GET", "/stream-sets"),
        ("GET", "/stream-sets/studio-a"),
        ("PUT", "/stream-sets/studio-a"),
    ];

    pub(super) const SOUTH_ROUTES: [(&str, &str); 5] = [
        ("POST", "/nodes/register"),
        ("POST", "/nodes/strom-node-1/heartbeat"),
        ("GET", "/nodes/strom-node-1/desired"),
        ("GET", "/endpoints"),
        ("GET", "/state"),
    ];

    pub(super) const SHARED_READ_ROUTES: [(&str, &str); 1] = [("GET", "/nodes")];

    /// Version-prefixed paths are not aliases for the current contract.
    #[tokio::test]
    async fn versioned_api_paths_are_not_served() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        for retired_prefix in ["/v1", "/v2", "/v3", "/v4", "/v5", "/v6"] {
            for (method, uri) in NORTH_ROUTES
                .iter()
                .chain(&SOUTH_ROUTES)
                .chain(&SHARED_READ_ROUTES)
            {
                let retired = format!("{retired_prefix}{uri}");
                let (status, _) = send(&app, method, &retired, None).await;
                assert_eq!(status, StatusCode::NOT_FOUND, "{retired} must stay retired");
            }
            let (status, _) = send(&app, "GET", &format!("{retired_prefix}/status"), None).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn status_distinguishes_current_and_observed_generations() {
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());
        let definition = stream("basic");
        let (_, headers, _) = send_with_headers(
            &app,
            "POST",
            "/streams",
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
            "/streams",
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
            ingress: None,
            destinations: Vec::new(),
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
        let StreamTransport::Srt(destination) = &mut invalid.destinations[0].endpoint else {
            unreachable!()
        };
        destination.remote = Some(weave_core::RemoteAddr {
            host: "example.test".to_string(),
            port: 9000,
            network: "internet".to_string(),
        });

        let (status, body) = send(
            &app,
            "POST",
            "/streams",
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
            "/stream-plans",
            Some(serde_json::to_value(invalid).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(mem.upsert_stream_calls(), 0);

        let (_, body) = send(&app, "GET", "/streams", None).await;
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());
    }

    fn with_port_range(
        mut registration: NodeRegistration,
        start: u16,
        end: u16,
    ) -> NodeRegistration {
        registration.node.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .expect("an SRT listener")
            .port_range = PortRange { start, end };
        registration
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
                "/nodes/register",
                Some(serde_json::to_value(registration).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED);
        }

        let (status, body) = send(
            &app,
            "POST",
            "/stream-plans",
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
            "/stream-plans",
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
            "/stream-plans",
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

    /// The heartbeat a relay's adapter sends after it misses a desired fetch
    /// reports the hops of its last desired set (the Strom adapter's
    /// `hop_status`). The bridge stays; an empty report would move it.
    #[tokio::test]
    async fn a_relay_that_misses_one_desired_fetch_keeps_its_bridge() {
        let bridge = "weave-feed-bridge-studio-0";
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());
        let register = |registration: NodeRegistration| {
            let app = app.clone();
            async move {
                let (status, _) = send(
                    &app,
                    "POST",
                    "/nodes/register",
                    Some(serde_json::to_value(registration).unwrap()),
                )
                .await;
                assert_eq!(status, StatusCode::ACCEPTED);
            }
        };
        register(nat_registration("nat-1", "192.168.0.10")).await;
        register(nat_registration("nat-2", "192.168.0.20")).await;
        register(node_registration("relay-b", "198.51.100.20")).await;
        state.streams.write().await.insert(
            "feed".to_string(),
            stored_stream(stream_between("feed", "nat-1", "nat-2")),
        );
        reconcile_tick(&state).await;
        let (_, body) = desired_hops(&app, "relay-b").await;
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops[0].id, bridge);
        register(node_registration("relay-a", "198.51.100.10")).await;

        let heartbeat = |hop_status: Vec<weave_core::HopStatus>| NodeHeartbeat {
            node_id: "relay-b".to_string(),
            status: NodeStatus::Ready,
            endpoints: Vec::new(),
            hop_status,
        };
        let running = hops
            .iter()
            .map(|hop| weave_core::HopStatus {
                id: hop.id.clone(),
                node_id: hop.node_id.clone(),
                state: weave_core::HopState::Provisioned,
                ingress: weave_core::SocketStatus {
                    condition: weave_core::LinkCondition::Flowing,
                    resolved: None,
                    stats: None,
                },
                merge_ingress: None,
                egresses: hop
                    .egresses
                    .iter()
                    .map(|egress| weave_core::EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: weave_core::SocketStatus {
                            condition: weave_core::LinkCondition::Flowing,
                            resolved: None,
                            stats: None,
                        },
                    })
                    .collect(),
            })
            .collect();
        let relay_of_bridge = |state: AppState| async move {
            reconcile_tick(&state).await;
            state.view.read().await.hops["feed"]
                .iter()
                .find(|hop| hop.id == bridge)
                .unwrap()
                .node_id
                .clone()
        };

        for (hop_status, relay) in [(running, "relay-b"), (Vec::new(), "relay-a")] {
            let (status, _) = send(
                &app,
                "POST",
                "/nodes/relay-b/heartbeat",
                Some(serde_json::to_value(heartbeat(hop_status)).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED);
            assert_eq!(relay_of_bridge(state.clone()).await, relay);
        }
    }

    #[tokio::test]
    async fn a_plan_keeps_a_bridge_on_the_relay_that_reports_running_it() {
        let bridge = "weave-feed-bridge-studio-0";
        let plan_relay = |relay_b: NodeRegistration| async move {
            let (state, _mem) = mem_state();
            let app = open_router(state);
            for registration in [
                nat_registration("nat-1", "192.168.0.10"),
                nat_registration("nat-2", "192.168.0.20"),
                node_registration("relay-a", "198.51.100.10"),
                relay_b,
            ] {
                let (status, _) = send(
                    &app,
                    "POST",
                    "/nodes/register",
                    Some(serde_json::to_value(registration).unwrap()),
                )
                .await;
                assert_eq!(status, StatusCode::ACCEPTED);
            }
            let (_, body) = send(
                &app,
                "POST",
                "/stream-plans",
                Some(serde_json::to_value(stream_between("feed", "nat-1", "nat-2")).unwrap()),
            )
            .await;
            let plan: StreamPlan = serde_json::from_value(body).unwrap();
            plan.hops
                .iter()
                .find(|hop| hop.id == bridge)
                .map(|hop| hop.node_id.clone())
        };

        let idle = node_registration("relay-b", "198.51.100.20");
        assert_eq!(plan_relay(idle.clone()).await.as_deref(), Some("relay-a"));

        let mut running = idle;
        running.hop_status = vec![weave_core::HopStatus {
            id: bridge.to_string(),
            node_id: "relay-b".to_string(),
            state: weave_core::HopState::Provisioned,
            ingress: weave_core::SocketStatus {
                condition: weave_core::LinkCondition::Flowing,
                resolved: None,
                stats: None,
            },
            merge_ingress: None,
            egresses: Vec::new(),
        }];
        assert_eq!(plan_relay(running).await.as_deref(), Some("relay-b"));
    }

    #[tokio::test]
    async fn plan_allocates_ports_alongside_existing_streams() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        // Node 1 has room for one sender ingress; node 2 for one receiver's
        // ingress and consumer port. One stream fits and a second does not.
        for registration in [
            with_port_range(node_registration("strom-node-1", "172.26.0.10"), 7000, 7000),
            with_port_range(node_registration("strom-node-2", "172.27.0.10"), 7000, 7001),
        ] {
            send(
                &app,
                "POST",
                "/nodes/register",
                Some(serde_json::to_value(registration).unwrap()),
            )
            .await;
        }
        let (_, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(stream("preview")).unwrap()),
        )
        .await;
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Placed, "alone, the preview fits");

        send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("existing")).unwrap()),
        )
        .await;

        let (status, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(stream("preview")).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Unplaced);
        assert!(plan.reason.unwrap().contains("no free port"));
        assert_eq!(mem.upsert_stream_calls(), 1, "plan did not write a stream");
        let (_, body) = send(&app, "GET", "/streams", None).await;
        let streams: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert_eq!(streams[0].spec, stream("existing"));
    }

    #[tokio::test]
    async fn hydration_refuses_invalid_persisted_streams() {
        let mem = Arc::new(MemStore::new());
        let mut invalid = stream("broken");
        invalid.destinations.clear();
        mem.create_stream(&invalid).await.unwrap();

        let error = AppState::hydrate(
            mem,
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
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

        let state = AppState::hydrate(
            mem,
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .expect("invalid cached nodes must not prevent startup");

        assert!(state.nodes.read().await.is_empty());
    }

    #[tokio::test]
    async fn a_heartbeat_writes_the_store_only_when_its_reports_change() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("strom-node-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(mem.upsert_node_calls(), 1);

        let socket = |condition, rate_mbps| weave_core::SocketStatus {
            condition,
            resolved: None,
            stats: Some(weave_core::LinkStats {
                rate_mbps,
                ..weave_core::LinkStats::default()
            }),
        };
        let hop = |condition, rate_mbps| HopStatus {
            id: "weave-basic-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            state: weave_core::HopState::Provisioned,
            ingress: socket(condition, rate_mbps),
            merge_ingress: None,
            egresses: vec![weave_core::EgressStatus {
                branch_id: "studio".to_string(),
                status: socket(condition, rate_mbps),
            }],
        };
        let heartbeat = |status, hop_status| NodeHeartbeat {
            node_id: "strom-node-1".to_string(),
            status,
            endpoints: Vec::new(),
            hop_status,
        };
        for (step, beat, writes) in [
            ("unchanged", heartbeat(NodeStatus::Ready, Vec::new()), 1),
            ("new status", heartbeat(NodeStatus::Degraded, Vec::new()), 2),
            (
                "new hop",
                heartbeat(
                    NodeStatus::Degraded,
                    vec![hop(weave_core::LinkCondition::Flowing, 2.5)],
                ),
                3,
            ),
            (
                "new rate only",
                heartbeat(
                    NodeStatus::Degraded,
                    vec![hop(weave_core::LinkCondition::Flowing, 2.7)],
                ),
                3,
            ),
            (
                "new condition",
                heartbeat(
                    NodeStatus::Degraded,
                    vec![hop(weave_core::LinkCondition::Stalled, 0.0)],
                ),
                4,
            ),
        ] {
            let (status, _) = send(
                &app,
                "POST",
                "/nodes/strom-node-1/heartbeat",
                Some(serde_json::to_value(&beat).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED, "{step}");
            assert_eq!(mem.upsert_node_calls(), writes, "{step}");
        }

        let stored = mem.load_nodes().await.unwrap();
        assert_eq!(stored[0].node.status, NodeStatus::Degraded);
        assert_eq!(
            stored[0].hop_status[0].ingress.condition,
            weave_core::LinkCondition::Stalled
        );
        let (_, body) = send(&app, "GET", "/state", None).await;
        let observed: ObservedState = serde_json::from_value(body).unwrap();
        assert_eq!(
            observed.hops[0].ingress.condition,
            weave_core::LinkCondition::Stalled
        );
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
                "/nodes/register",
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
        let (_, body) = send(&app, "GET", "/nodes", None).await;
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
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["details"][0]["field"], "node.id");
        assert_eq!(body["details"][0]["code"], "invalid_characters");
        assert_eq!(mem.upsert_node_calls(), 0);
        let (_, body) = send(&app, "GET", "/nodes", None).await;
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
            "/nodes/register",
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
            merge_ingress: None,
            egresses: Vec::new(),
        };

        let mut registration = node_registration("strom-node-1", "172.26.0.10");
        registration.hop_status = vec![status_for("strom-node-2")];
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(mem.upsert_node_calls(), 0);

        registration.hop_status.clear();
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
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
            "/nodes/strom-node-1/heartbeat",
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
        browser.node.capabilities.hop_profiles = vec![HopProfile {
            id: "camera-to-whip".to_string(),
            ingress: HopEndpointClass::Device(weave_core::DeviceClass {
                device: weave_core::DeviceKind::Capture,
                tracks: None,
            }),
            egress: HopEndpointClass::Transport(TransportClass {
                transport: Transport::Whip,
                roles: RoleSet::only(weave_core::SocketRole::Connect),
            }),
            max_egresses: Some(1),
            merge: false,
            accepts: None,
        }];
        browser.node.topology.attachments[0].listeners = NetworkListeners::default();
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&browser).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let mut strom = node_registration("strom-node-2", "172.27.0.10");
        strom.node.capabilities.hop_profiles.push(HopProfile {
            id: "whip-to-srt".to_string(),
            ingress: HopEndpointClass::Transport(TransportClass {
                transport: Transport::Whip,
                roles: RoleSet::only(weave_core::SocketRole::Listen),
            }),
            egress: HopEndpointClass::Transport(TransportClass {
                transport: Transport::Srt,
                roles: RoleSet::both(),
            }),
            max_egresses: None,
            merge: false,
            accepts: None,
        });
        strom.node.topology.attachments[0].listeners.whip = Some(weave_core::SignallingListener {
            base_url: "http://172.27.0.10:8080/whip".to_string(),
        });
        send(
            &app,
            "POST",
            "/nodes/register",
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

        let (_, body) = send(&app, "GET", "/nodes", None).await;
        let nodes: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        let page = nodes.iter().find(|n| n.id == "browser-a1b2").unwrap();
        assert_eq!(page.endpoint, "browser://browser-a1b2");

        let (status, body) = send(&app, "GET", "/nodes/browser-a1b2/desired", None).await;
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
            "/nodes/register",
            Some(serde_json::to_value(node_registration("guest-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let event = sink.next().await.event;
        assert_eq!(event.event_type, EventType::NodeRegistered);
        let Subject::Node(node) = event.subject else {
            panic!("expected a node event");
        };
        assert_eq!(node.id, "guest-1");
        assert_eq!(node.endpoint, "http://guest-1:8080");
        assert_eq!(node.status, NodeStatus::Ready);
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
        assert_eq!(event.event_type, EventType::NodeOffline);
        let Subject::Node(node) = event.subject else {
            panic!("expected a node event");
        };
        assert_eq!(node.id, "guest-1");
        assert_eq!(node.status, NodeStatus::Offline);
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
            "/nodes/guest-1/heartbeat",
            Some(json!({ "node_id": "guest-1", "status": "ready" })),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let event = sink.next().await.event;
        assert_eq!(event.event_type, EventType::NodeOnline);
        let Subject::Node(node) = event.subject else {
            panic!("expected a node event");
        };
        assert_eq!(node.status, NodeStatus::Ready);
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
            "/nodes/guest-1/heartbeat",
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
                "/nodes/register",
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
        let StreamTransport::Srt(source) = &mut definition.source else {
            unreachable!()
        };
        source.format = Some(weave_core::MediaFormat {
            container: weave_core::Container::MpegTs,
            video: None,
            audio: None,
        });
        let StreamTransport::Srt(destination) = &mut definition.destinations[0].endpoint else {
            unreachable!()
        };
        destination.accepts = Some(weave_core::FormatConstraint {
            container: Some(vec![weave_core::Container::Rtp]),
            ..Default::default()
        });
        let outcome = reconcile(vec![definition], &observed, &LinkKeys::for_tests());

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

        let outcome = reconcile(vec![stream("basic")], &observed, &LinkKeys::for_tests());

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
                node_registration("relay-a", "198.51.100.10"),
            ),
            (
                "relay-b".to_string(),
                node_registration("relay-b", "198.51.100.20"),
            ),
        ]);
        let observed = observed_state(&nodes);
        let outcome = reconcile(vec![stream("basic")], &observed, &LinkKeys::for_tests());
        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert!(
            basic.nodes.contains(&"relay-a".to_string()),
            "the lowest-id relay carries the stream while it is online: {:?}",
            basic.nodes
        );

        nodes.get_mut("relay-a").unwrap().node.status = NodeStatus::Offline;
        let observed = observed_state(&nodes);
        let outcome = reconcile(vec![stream("basic")], &observed, &LinkKeys::for_tests());

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
        two_nodes_and_basic(&state).await;
        {
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now - Duration::from_secs(60));
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/nodes", None).await;
        assert_eq!(status, StatusCode::OK);
        let nodes: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        let node1 = nodes.iter().find(|n| n.id == "strom-node-1").unwrap();
        assert_eq!(node1.status, NodeStatus::Offline);

        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert!(
            !hops.is_empty(),
            "offline node still receives its desired hops"
        );
    }

    async fn seed_restart_case(store: &dyn StateStore) {
        if let Some(existing) = store
            .load_streams()
            .await
            .unwrap()
            .into_iter()
            .find(|stored| stored.spec.name == "restart")
        {
            store
                .delete_stream("restart", existing.revision)
                .await
                .unwrap();
        }
        store
            .create_stream(&stream_between(
                "restart",
                "restart-node-1",
                "restart-node-2",
            ))
            .await
            .unwrap();
        for (id, host) in [
            ("restart-node-1", "172.26.0.10"),
            ("restart-node-2", "172.27.0.10"),
        ] {
            store
                .upsert_node(&node_registration(id, host))
                .await
                .unwrap();
        }
    }

    /// Two controllers hydrated from one store, one after the other, stand in
    /// for a restart. The second answers before any tick but the one it runs
    /// on the way up.
    async fn assert_a_restart_serves_the_stored_hops(store: Arc<dyn StateStore>) {
        seed_restart_case(store.as_ref()).await;
        let before = AppState::hydrate(
            store.clone(),
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let before = router_after_first_tick(&before, Guard::Disabled, NodeGuard::Disabled).await;
        let after = AppState::hydrate(
            store,
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let after = router_after_first_tick(&after, Guard::Disabled, NodeGuard::Disabled).await;

        for node in ["restart-node-1", "restart-node-2"] {
            let (status, served) = desired_hops(&before, node).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(served.as_array().map(Vec::len), Some(1), "{node}: {served}");
            assert_eq!(
                desired_hops(&after, node).await,
                (StatusCode::OK, served),
                "{node} gets the same hops from the restarted controller"
            );
        }
    }

    #[tokio::test]
    async fn a_restart_serves_the_stored_hops_on_its_first_request() {
        assert_a_restart_serves_the_stored_hops(Arc::new(MemStore::new())).await;
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_restart_on_postgres_serves_the_stored_hops_on_its_first_request() {
        let url = store::tests::fresh_database().await;
        let store: Arc<dyn StateStore> = Arc::new(store::tests::leading_pg_store(&url).await);
        assert_a_restart_serves_the_stored_hops(store).await;
    }

    async fn get_ok(app: &Router, uri: &str) -> Value {
        let (status, body) = send(app, "GET", uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        body
    }

    #[tokio::test]
    async fn a_standby_answers_not_leader_on_every_api_route() {
        let app = leadership_router(Leadership::default());
        for (method, uri) in [
            ("GET", "/status"),
            ("GET", "/streams"),
            ("POST", "/streams"),
            ("GET", "/streams/basic"),
            ("DELETE", "/streams/basic"),
            ("GET", "/streams/basic/endpoints"),
            ("POST", "/stream-plans"),
            ("GET", "/stream-sets"),
            ("PUT", "/stream-sets/production"),
            ("GET", "/nodes"),
            ("POST", "/nodes/register"),
            ("POST", "/nodes/strom-node-1/heartbeat"),
            ("GET", "/nodes/strom-node-1/desired"),
            ("GET", "/endpoints"),
            ("GET", "/state"),
            ("GET", "/view"),
            ("GET", "/v1/status"),
        ] {
            let (status, body) = send(&app, method, uri, None).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{method} {uri}");
            assert_eq!(body["code"], "not_leader", "{method} {uri}");
        }
        assert_eq!(get_ok(&app, "/health").await["status"], "ok");
        for page in ["/", "/ui"] {
            let response = app
                .clone()
                .oneshot(Request::get(page).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{page}");
        }
    }

    #[tokio::test]
    async fn a_leader_serves_through_the_gate_until_it_stands_by() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;
        let leadership = Leadership::default();
        let app = leadership_router(leadership.clone());

        leadership.taking();
        assert_eq!(
            desired_hops(&app, "strom-node-1").await.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "nothing is served while the state loads"
        );
        let led = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;
        assert!(leadership.lead(led));
        let hops = get_ok(&app, "/nodes/strom-node-1/desired").await;
        assert_eq!(hops.as_array().map(Vec::len), Some(1), "{hops}");

        leadership.stand_by();
        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "not_leader");
    }

    #[test]
    fn a_lease_lost_while_the_state_loads_is_never_served() {
        let (state, _mem) = mem_state();
        let leadership = Leadership::default();
        leadership.taking();
        leadership.stand_by();

        assert!(!leadership.lead(open_router(state)));
        assert!(leadership.router().is_none());
    }

    /// What a node running `hops` with media on every socket reports.
    fn flowing_reports(hops: &[DesiredHop]) -> Vec<HopStatus> {
        let flowing = || weave_core::SocketStatus {
            condition: weave_core::LinkCondition::Flowing,
            resolved: None,
            stats: None,
        };
        hops.iter()
            .map(|hop| HopStatus {
                id: hop.id.clone(),
                node_id: hop.node_id.clone(),
                state: weave_core::HopState::Provisioned,
                ingress: flowing(),
                merge_ingress: hop.merge_ingress.as_ref().map(|_| flowing()),
                egresses: hop
                    .egresses
                    .iter()
                    .map(|egress| weave_core::EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: flowing(),
                    })
                    .collect(),
            })
            .collect()
    }

    /// Heartbeat `node_id` with its desired hops all flowing.
    async fn report_flowing(app: &Router, node_id: &str) {
        let hops: Vec<DesiredHop> =
            serde_json::from_value(get_ok(app, &format!("/nodes/{node_id}/desired")).await)
                .unwrap();
        let heartbeat = NodeHeartbeat {
            node_id: node_id.to_string(),
            status: NodeStatus::Ready,
            endpoints: Vec::new(),
            hop_status: flowing_reports(&hops),
        };
        let (status, body) = send(
            app,
            "POST",
            &format!("/nodes/{node_id}/heartbeat"),
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{node_id}: {body}");
    }

    async fn hydrated(store: Arc<MemStore>, webhooks: Option<Arc<webhook::Emitter>>) -> AppState {
        AppState::hydrate(
            store,
            Duration::from_secs(15),
            Duration::from_secs(300),
            webhooks,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_restart_keeps_unchanged_conditions_and_reports_no_change() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let before = hydrated(mem.clone(), None).await;
        let before_app =
            router_after_first_tick(&before, Guard::Disabled, NodeGuard::Disabled).await;
        for node in ["restart-node-1", "restart-node-2"] {
            report_flowing(&before_app, node).await;
        }
        reconcile_tick(&before).await;
        let flowing = before.view.read().await.streams.clone();
        assert_eq!(flowing[0].status, PathStatus::Flowing, "{flowing:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;

        let mut sink = sink(StatusCode::OK).await;
        let webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let restarted = hydrated(mem, webhooks).await;
        let restarted_app =
            router_after_first_tick(&restarted, Guard::Disabled, NodeGuard::Disabled).await;

        let RunningStatus { streams, .. } =
            serde_json::from_value(get_ok(&restarted_app, "/status").await).unwrap();
        assert_eq!(
            streams, flowing,
            "the same status, and every condition keeps its transition time"
        );
        assert!(
            stream_events(&mut sink).await.is_empty(),
            "nothing changed, so nothing is reported"
        );
    }

    fn hop_nodes(app_desired: &[(String, Vec<DesiredHop>)], hop_id: &str) -> Vec<String> {
        app_desired
            .iter()
            .filter(|(_, hops)| hops.iter().any(|hop| hop.id == hop_id))
            .map(|(node, _)| node.clone())
            .collect()
    }

    async fn desired_everywhere(app: &Router, nodes: &[&str]) -> Vec<(String, Vec<DesiredHop>)> {
        let mut desired = Vec::new();
        for node in nodes {
            let hops = get_ok(app, &format!("/nodes/{node}/desired")).await;
            desired.push(((*node).to_string(), serde_json::from_value(hops).unwrap()));
        }
        desired
    }

    #[tokio::test]
    async fn a_restart_keeps_a_relayed_stream_on_the_relay_that_runs_it() {
        const NODES: [&str; 4] = ["relay-a", "relay-b", "source", "studio-node"];
        let bridge = crate::path::bridge_hop_id("feed", "studio", 0);
        let mem = Arc::new(MemStore::new());
        mem.create_stream(&stream_between("feed", "source", "studio-node"))
            .await
            .unwrap();
        for registration in [
            nat_registration("source", "192.168.1.10"),
            nat_registration("studio-node", "192.168.2.10"),
            node_registration("relay-a", "198.51.100.10"),
            node_registration("relay-b", "198.51.100.20"),
        ] {
            mem.upsert_node(&registration).await.unwrap();
        }

        let before = hydrated(mem.clone(), None).await;
        let before_app =
            router_after_first_tick(&before, Guard::Disabled, NodeGuard::Disabled).await;
        assert_eq!(
            hop_nodes(&desired_everywhere(&before_app, &NODES).await, &bridge),
            ["relay-a"]
        );
        before.last_seen.write().await.insert(
            "relay-a".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        reconcile_tick(&before).await;
        let moved = desired_everywhere(&before_app, &NODES).await;
        assert_eq!(
            hop_nodes(&moved, &bridge),
            ["relay-b"],
            "relay-a went offline"
        );
        let (status, _) = send(
            &before_app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("relay-a", "198.51.100.10")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        for node in NODES {
            report_flowing(&before_app, node).await;
        }
        reconcile_tick(&before).await;
        assert_eq!(
            desired_everywhere(&before_app, &NODES).await,
            moved,
            "relay-a is back, and the bridge stays where it runs"
        );

        let restarted = hydrated(mem, None).await;
        let restarted_app =
            router_after_first_tick(&restarted, Guard::Disabled, NodeGuard::Disabled).await;
        assert_eq!(
            desired_everywhere(&restarted_app, &NODES).await,
            moved,
            "the restarted controller keeps the bridge, and its ports, on relay-b"
        );

        restarted.last_seen.write().await.insert(
            "relay-b".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        reconcile_tick(&restarted).await;
        assert_eq!(
            hop_nodes(&desired_everywhere(&restarted_app, &NODES).await, &bridge),
            ["relay-a"],
            "a relay that stops heartbeating after the restart still loses the bridge"
        );
    }

    #[tokio::test]
    async fn a_failed_status_save_is_retried_on_the_next_tick() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let state = hydrated(mem.clone(), None).await;
        mem.fail_writes(1);

        reconcile_tick(&state).await;
        assert!(mem.load_stream_statuses().await.unwrap().is_empty());
        reconcile_tick(&state).await;

        assert_eq!(
            mem.load_stream_statuses().await.unwrap(),
            state.view.read().await.streams
        );
    }

    #[tokio::test]
    async fn failed_node_writes_are_retried_on_the_next_tick() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let state = hydrated(mem.clone(), None).await;
        let app = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;

        mem.fail_writes(1);
        report_flowing(&app, "restart-node-2").await;
        state.last_seen.write().await.insert(
            "restart-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        mem.fail_writes(2);
        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let stored: BTreeMap<String, NodeRegistration> = mem
            .load_nodes()
            .await
            .unwrap()
            .into_iter()
            .map(|registration| (registration.node.id.clone(), registration))
            .collect();
        assert_eq!(stored, *state.nodes.read().await);
        assert_eq!(stored["restart-node-1"].node.status, NodeStatus::Offline);
        assert!(!stored["restart-node-2"].hop_status.is_empty());
    }

    #[tokio::test]
    async fn a_node_marked_offline_is_still_offline_after_a_restart() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let before = hydrated(mem.clone(), None).await;
        before.last_seen.write().await.insert(
            "restart-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        reconcile_tick(&before).await;

        let restarted = hydrated(mem, None).await;
        let app = router_after_first_tick(&restarted, Guard::Disabled, NodeGuard::Disabled).await;
        let nodes: Vec<NodeDescriptor> =
            serde_json::from_value(get_ok(&app, "/nodes").await).unwrap();
        assert_eq!(
            nodes
                .iter()
                .map(|node| (node.id.as_str(), node.status))
                .collect::<Vec<_>>(),
            [
                ("restart-node-1", NodeStatus::Offline),
                ("restart-node-2", NodeStatus::Ready),
            ]
        );
    }

    #[tokio::test]
    async fn a_takeover_counts_every_stored_node_as_heard_at_the_takeover() {
        let store = Arc::new(MemStore::new());
        seed_restart_case(store.as_ref()).await;
        store
            .upsert_node(&node_registration("spare-node", "172.28.0.10"))
            .await
            .unwrap();
        let took_over = Instant::now();
        let state = AppState::hydrate(
            store,
            Duration::from_secs(15),
            Duration::ZERO,
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let app = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;

        let nodes: Vec<NodeDescriptor> =
            serde_json::from_value(get_ok(&app, "/nodes").await).unwrap();
        assert_eq!(
            nodes
                .iter()
                .map(|node| (node.id.as_str(), node.status))
                .collect::<Vec<_>>(),
            [
                ("restart-node-1", NodeStatus::Ready),
                ("restart-node-2", NodeStatus::Ready),
                ("spare-node", NodeStatus::Ready),
            ],
            "the first tick marks nobody offline and forgets nobody, with a zero forget interval"
        );
        assert!(
            state
                .last_seen
                .read()
                .await
                .values()
                .all(|seen| *seen >= took_over)
        );
    }

    async fn pg_controller(url: &str) -> (Arc<PgStore>, Controller) {
        let pg = Arc::new(PgStore::connect(url).await.expect("connect"));
        let controller = Controller {
            node_ttl: Duration::from_secs(15),
            node_forget: Duration::from_secs(300),
            webhooks: None,
            keys: LinkKeys::for_tests(),
            north: Guard::Disabled,
            south: NodeGuard::Disabled,
            interval: Duration::from_millis(200),
        };
        (pg, controller)
    }

    /// Runs [`run_with_lease`] until the returned sender fires or is dropped.
    async fn start_controller(
        url: &str,
        timing: LeaseTiming,
    ) -> (
        Arc<PgStore>,
        Leadership,
        tokio::sync::oneshot::Sender<()>,
        JoinHandle<Result<()>>,
    ) {
        let (pg, controller) = pg_controller(url).await;
        let leadership = Leadership::default();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn({
            let pg = pg.clone();
            let leadership = leadership.clone();
            async move {
                run_with_lease(pg, &controller, &leadership, timing, async {
                    let _ = stopped.await;
                    Ok(())
                })
                .await
            }
        });
        (pg, leadership, stop, run)
    }

    async fn until_leading(leadership: &Leadership, within: Duration) {
        let deadline = Instant::now() + within;
        while leadership.router().is_none() {
            assert!(Instant::now() < deadline, "not leading within {within:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn until_standing_by(leadership: &Leadership, within: Duration) {
        let deadline = Instant::now() + within;
        while leadership.router().is_some() {
            assert!(Instant::now() < deadline, "still leading after {within:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// What a takeover must keep: the hops each node is told to run, and every
    /// generation and ETag.
    async fn served_state(app: &Router) -> Value {
        let (_, stream_headers, stream) =
            send_with_headers(app, "GET", "/streams/restart", None, &[]).await;
        let (_, set_headers, _) =
            send_with_headers(app, "GET", "/stream-sets/production", None, &[]).await;
        json!({
            "node-1": get_ok(app, "/nodes/restart-node-1/desired").await,
            "node-2": get_ok(app, "/nodes/restart-node-2/desired").await,
            "stream": stream,
            "stream_etag": response_etag(&stream_headers),
            "set_etag": response_etag(&set_headers),
        })
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_standby_on_postgres_takes_over_when_the_leader_stops() {
        let url = store::tests::fresh_database().await;
        let seed = store::tests::leading_pg_store(&url).await;
        seed_restart_case(&seed).await;
        seed.create_stream_set(
            "production",
            &[stream_between("owned", "restart-node-2", "restart-node-1")],
            true,
        )
        .await
        .unwrap();
        seed.release_lease().await.unwrap();
        let timing = LeaseTiming::new(Duration::from_secs(3));

        let (first_pg, first, stop_first, first_run) = start_controller(&url, timing).await;
        until_leading(&first, Duration::from_secs(5)).await;
        let first_app = leadership_router(first.clone());
        let before = served_state(&first_app).await;
        assert_eq!(
            before["node-1"].as_array().map(Vec::len),
            Some(2),
            "{before}"
        );

        let (_second_pg, second, _stop_second, _second_run) = start_controller(&url, timing).await;
        let second_app = leadership_router(second.clone());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(second.router().is_none(), "the lease has one holder");
        assert_eq!(
            desired_hops(&second_app, "restart-node-1").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            served_state(&first_app).await,
            before,
            "renewals keep it leading"
        );

        stop_first.send(()).unwrap();
        first_run.await.unwrap().unwrap();
        assert_eq!(
            desired_hops(&first_app, "restart-node-1").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(matches!(
            first_pg
                .upsert_node(&node_registration("late", "172.28.0.10"))
                .await,
            Err(StoreError::NotLeader)
        ));

        until_leading(&second, Duration::from_secs(5)).await;
        assert_eq!(
            served_state(&second_app).await,
            before,
            "the new leader serves the same hops, generations and ETags"
        );
    }

    /// A request an earlier term's router is still handling when the same
    /// process takes the lease again must not write under the new term.
    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_write_from_an_earlier_term_is_refused_after_the_lease_is_taken_again() {
        let url = store::tests::fresh_database().await;
        let seed = store::tests::leading_pg_store(&url).await;
        seed.create_stream(&stream("x")).await.unwrap();
        seed.release_lease().await.unwrap();
        let timing = LeaseTiming::new(Duration::from_secs(3));
        let (_pg, leadership, _stop, _run) = start_controller(&url, timing).await;
        until_leading(&leadership, Duration::from_secs(5)).await;
        let earlier = leadership.router().unwrap();

        let other = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::query(
            "UPDATE controller_lease SET holder = 'other', epoch = epoch + 1,
             expires_at = now() + interval '1 second'",
        )
        .execute(&other)
        .await
        .unwrap();
        until_standing_by(&leadership, timing.renew_every * 3).await;
        until_leading(&leadership, Duration::from_secs(5)).await;
        let current = leadership_router(leadership.clone());

        let (status, body) = send(&earlier, "DELETE", "/streams/x", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(body["code"], "not_leader");
        let stored: Vec<String> = sqlx::query_scalar("SELECT name FROM streams")
            .fetch_all(&other)
            .await
            .unwrap();
        assert_eq!(stored, ["x"]);
        assert_eq!(
            send(&current, "GET", "/streams/x", None).await.0,
            StatusCode::OK
        );
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_leader_whose_lease_is_taken_stops_serving_and_stands_by() {
        let url = store::tests::fresh_database().await;
        let timing = LeaseTiming::new(Duration::from_secs(3));
        let (_pg, leadership, _stop, _run) = start_controller(&url, timing).await;
        until_leading(&leadership, Duration::from_secs(5)).await;

        let other = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::query(
            "UPDATE controller_lease SET holder = 'other', epoch = epoch + 1,
             expires_at = now() + interval '5 seconds'",
        )
        .execute(&other)
        .await
        .unwrap();
        until_standing_by(&leadership, timing.renew_every * 3).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            leadership.router().is_none(),
            "it waits while the other holder's lease lasts"
        );

        until_leading(&leadership, Duration::from_secs(8)).await;
        let (holder, epoch): (String, i64) =
            sqlx::query_as("SELECT holder, epoch FROM controller_lease")
                .fetch_one(&other)
                .await
                .unwrap();
        assert!(
            holder != "other" && epoch >= 3,
            "it took the lease again once the other one lapsed: {holder} {epoch}"
        );
    }

    #[tokio::test]
    async fn a_node_the_last_tick_did_not_cover_gets_404_for_its_desired_hops() {
        let (state, _mem) = mem_state();
        let app = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;

        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "node_not_found");

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("strom-node-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            desired_hops(&app, "strom-node-1").await.0,
            StatusCode::NOT_FOUND,
            "registered after the last tick"
        );

        reconcile_tick(&state).await;
        assert_eq!(
            desired_hops(&app, "strom-node-1").await,
            (StatusCode::OK, json!([])),
            "a reconciled node with nothing to run is told so"
        );
    }

    #[tokio::test]
    async fn deleting_the_last_stream_tells_its_nodes_to_run_nothing() {
        let (state, _mem) = mem_state();
        {
            let mut nodes = state.nodes.write().await;
            for (id, host) in [
                ("strom-node-1", "172.26.0.10"),
                ("strom-node-2", "172.27.0.10"),
            ] {
                nodes.insert(id.to_string(), node_registration(id, host));
            }
        }
        let app = open_router(state.clone());
        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        let (_, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(body.as_array().map(Vec::len), Some(1));

        let (status, _) = send(&app, "DELETE", "/streams/basic", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        reconcile_tick(&state).await;
        assert_eq!(
            desired_hops(&app, "strom-node-1").await,
            (StatusCode::OK, json!([]))
        );
    }

    fn observed_status(conditions: Vec<StreamCondition>) -> StreamStatus {
        StreamStatus {
            name: "basic".to_string(),
            generation: 1,
            observed_generation: Some(1),
            status: PathStatus::AwaitingInput,
            nodes: Vec::new(),
            conditions: conditions.clone(),
            ingress: None,
            destinations: vec![StreamDestinationStatus {
                id: "studio".to_string(),
                status: PathStatus::AwaitingInput,
                nodes: Vec::new(),
                conditions,
                endpoint: None,
            }],
        }
    }

    fn media(status: StreamConditionStatus, reason: StreamConditionReason) -> StreamCondition {
        let mut condition = stream_condition(
            StreamConditionType::MediaFlowing,
            status,
            reason,
            "media detail",
        );
        condition.last_transition_time = "2026-09-25T10:00:00Z".to_string();
        condition
    }

    fn awaiting_input() -> StreamStatus {
        observed_status(vec![media(
            StreamConditionStatus::False,
            StreamConditionReason::AwaitingInput,
        )])
    }

    fn changed_names(current: &[StreamStatus], previous: &[StreamStatus]) -> Vec<String> {
        changed_streams(current, previous)
            .into_iter()
            .map(|stream| stream.name)
            .collect()
    }

    #[test]
    fn a_stream_no_tick_computed_before_has_changed() {
        let current = [awaiting_input()];
        assert_eq!(changed_names(&current, &[]), ["basic"]);

        let placeholder = unreconciled_stream_status(&stored_stream(stream("basic")));
        assert_eq!(
            changed_names(&current, std::slice::from_ref(&placeholder)),
            ["basic"],
            "a stream accepted since the last tick"
        );
    }

    #[test]
    fn unchanged_conditions_are_not_a_change() {
        let previous = [awaiting_input()];
        assert!(changed_names(&previous, &previous).is_empty());

        let mut detail = awaiting_input();
        detail.conditions[0].detail = "other words".to_string();
        assert!(
            changed_names(std::slice::from_ref(&detail), &previous).is_empty(),
            "detail is free text"
        );

        let mut generation = awaiting_input();
        generation.generation = 2;
        generation.observed_generation = Some(2);
        assert!(changed_names(std::slice::from_ref(&generation), &previous).is_empty());
    }

    #[test]
    fn a_status_reason_or_destination_change_is_one_change() {
        let previous = [awaiting_input()];

        let mut flowing = awaiting_input();
        flowing.conditions[0] = media(
            StreamConditionStatus::True,
            StreamConditionReason::MediaFlowing,
        );
        flowing.destinations[0].conditions[0] = flowing.conditions[0].clone();
        assert_eq!(
            changed_names(std::slice::from_ref(&flowing), &previous),
            ["basic"]
        );

        let mut degraded = awaiting_input();
        degraded.conditions[0].reason = StreamConditionReason::MediaDegraded;
        assert_eq!(
            changed_names(std::slice::from_ref(&degraded), &previous),
            ["basic"],
            "awaiting_input and media_degraded share a status"
        );

        let mut destination = awaiting_input();
        destination.destinations[0].conditions[0].reason = StreamConditionReason::MediaDegraded;
        assert_eq!(
            changed_names(std::slice::from_ref(&destination), &previous),
            ["basic"]
        );

        let mut added = awaiting_input();
        let mut preview = added.destinations[0].clone();
        preview.id = "preview".to_string();
        added.destinations.insert(0, preview);
        assert_eq!(
            changed_names(std::slice::from_ref(&added), &previous),
            ["basic"]
        );
    }

    #[test]
    fn a_reason_change_keeps_the_transition_time_it_reports() {
        let previous = [awaiting_input()];
        let mut current = awaiting_input();
        current.conditions[0].reason = StreamConditionReason::MediaDegraded;
        current.conditions[0].last_transition_time = String::new();
        stamp_condition_transition_times(
            std::slice::from_mut(&mut current),
            &previous,
            "2026-09-25T10:05:00Z",
        );

        let changed = changed_streams(std::slice::from_ref(&current), &previous);
        assert_eq!(
            changed[0].conditions[0].last_transition_time,
            "2026-09-25T10:00:00Z"
        );
    }

    async fn stream_events(sink: &mut Sink) -> Vec<StreamSummary> {
        let mut events = Vec::new();
        while let Ok(delivery) = tokio::time::timeout(Duration::from_millis(300), sink.next()).await
        {
            assert_eq!(delivery.event.event_type, EventType::StreamChanged);
            let Subject::Stream(stream) = delivery.event.subject else {
                panic!("expected a stream event");
            };
            events.push(stream);
        }
        events
    }

    #[tokio::test]
    async fn the_first_tick_after_a_start_reports_every_stream() {
        let mut sink = sink(StatusCode::OK).await;
        let mem = Arc::new(MemStore::new());
        mem.create_stream(&stream("basic")).await.unwrap();
        mem.create_stream(&stream("backup")).await.unwrap();
        let webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let state = AppState::hydrate(
            mem,
            Duration::from_secs(15),
            Duration::from_secs(300),
            webhooks,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();

        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let events = stream_events(&mut sink).await;
        let names: Vec<&str> = events.iter().map(|stream| stream.name.as_str()).collect();
        assert_eq!(names, ["backup", "basic"], "once each, on the first tick");
        assert_eq!(events[1].generation, 1);
        assert_eq!(events[1].observed_generation, Some(1));
        assert_eq!(events[1].status, PathStatus::Pending);
        let placement = &events[1].conditions[0];
        assert_eq!(
            placement.condition_type,
            StreamConditionType::PlacementReady
        );
        assert_eq!(placement.reason, StreamConditionReason::PlacementFailed);
        assert_eq!(events[1].destinations[0].id, "studio");
    }

    #[tokio::test]
    async fn a_hop_status_change_reports_the_stream_once() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        two_nodes_and_basic(&state).await;
        reconcile_tick(&state).await;
        let first = stream_events(&mut sink).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].status, PathStatus::Pending, "no hop has reported");

        let hops = state.view.read().await.hops["basic"].clone();
        {
            let mut nodes = state.nodes.write().await;
            for hop in &hops {
                nodes
                    .get_mut(&hop.node_id)
                    .unwrap()
                    .hop_status
                    .push(HopStatus {
                        id: hop.id.clone(),
                        node_id: hop.node_id.clone(),
                        state: weave_core::HopState::Provisioned,
                        ingress: weave_core::SocketStatus {
                            condition: weave_core::LinkCondition::Connecting,
                            resolved: None,
                            stats: None,
                        },
                        merge_ingress: None,
                        egresses: hop
                            .egresses
                            .iter()
                            .map(|egress| weave_core::EgressStatus {
                                branch_id: egress.branch_id.clone(),
                                status: weave_core::SocketStatus {
                                    condition: weave_core::LinkCondition::Connecting,
                                    resolved: None,
                                    stats: None,
                                },
                            })
                            .collect(),
                    });
            }
        }
        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let second = stream_events(&mut sink).await;
        assert_eq!(second.len(), 1, "{second:?}");
        assert_ne!(second[0].status, PathStatus::Pending);
        let hops_ready = second[0]
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::HopsReady)
            .unwrap();
        assert_eq!(hops_ready.status, StreamConditionStatus::True);
    }

    #[tokio::test]
    async fn a_tick_does_not_wait_on_an_unreachable_receiver() {
        let (mut state, _mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            // Reserved for documentation; nothing listens there.
            url: Some("http://192.0.2.1:1/hook".to_string()),
            timeout: Duration::from_secs(5),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        {
            let mut streams = state.streams.write().await;
            for index in 0..300 {
                let name = format!("stream-{index}");
                streams.insert(name.clone(), stored_stream(stream(&name)));
            }
        }

        tokio::time::timeout(Duration::from_secs(2), reconcile_tick(&state))
            .await
            .expect("a tick must not wait on the webhook receiver");
    }

    #[test]
    fn only_unnamed_offline_nodes_past_the_interval_are_forgettable() {
        let offline = |id: &str| {
            let mut registration = node_registration(id, "172.26.0.10");
            registration.node.status = NodeStatus::Offline;
            (id.to_string(), registration)
        };
        let mut nodes = BTreeMap::from([
            offline("stale"),
            offline("named"),
            offline("edge"),
            offline("recent"),
            (
                "silent".to_string(),
                node_registration("silent", "172.26.0.10"),
            ),
        ]);
        nodes.get_mut("silent").unwrap().node.status = NodeStatus::Ready;
        let after = Duration::from_secs(300);
        let now = Instant::now();
        let last_seen = BTreeMap::from([
            ("stale".to_string(), now - Duration::from_secs(301)),
            ("named".to_string(), now - Duration::from_secs(301)),
            ("edge".to_string(), now - after),
            ("recent".to_string(), now - Duration::from_secs(20)),
            ("silent".to_string(), now - Duration::from_secs(301)),
        ]);
        let named = BTreeSet::from(["named"]);

        assert_eq!(
            forgettable(&nodes, &last_seen, &named, now, after),
            ["stale"]
        );
    }

    #[test]
    fn a_stream_names_its_source_destinations_and_via_relays() {
        let mut definition = stream_between("basic", "source-node", "studio-node");
        let StreamTransport::Srt(studio) = &mut definition.destinations[0].endpoint else {
            unreachable!()
        };
        studio.via = vec!["relay-node".to_string()];
        definition.destinations.push(StreamDestination {
            id: "partner".to_string(),
            paths: 1,
            endpoint: StreamTransport::Srt(SrtEndpoint {
                node: None,
                remote: Some(weave_core::RemoteAddr {
                    host: "203.0.113.7".to_string(),
                    port: 9000,
                    network: "internet".to_string(),
                }),
                via: vec!["egress-node".to_string()],
                format: None,
                accepts: None,
                network: None,
                latency: None,
                passphrase: None,
            }),
        });
        let mut disabled = stream_between("spare", "spare-node", "studio-node");
        disabled.enabled = false;

        assert_eq!(
            named_nodes([&definition, &disabled]),
            BTreeSet::from([
                "egress-node",
                "relay-node",
                "source-node",
                "spare-node",
                "studio-node"
            ])
        );
    }

    fn event_types(events: &[weave_core::webhook::Event]) -> Vec<EventType> {
        events.iter().map(|event| event.event_type).collect()
    }

    async fn deliveries(sink: &mut Sink) -> Vec<weave_core::webhook::Event> {
        let mut events = Vec::new();
        while let Ok(delivery) = tokio::time::timeout(Duration::from_millis(300), sink.next()).await
        {
            events.push(delivery.event);
        }
        events
    }

    #[tokio::test]
    async fn a_tick_forgets_an_offline_node_no_stream_names() {
        let mut sink = sink(StatusCode::OK).await;
        let (mut state, mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            events: vec!["node.offline".to_string(), "node.forgotten".to_string()],
            ..webhook::Config::default()
        })
        .map(Arc::new);
        two_nodes_and_basic(&state).await;
        let guest = node_registration("guest-1", "172.28.0.10");
        mem.upsert_node(&guest).await.unwrap();
        state
            .nodes
            .write()
            .await
            .insert("guest-1".to_string(), guest.clone());
        {
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now);
            seen.insert("strom-node-2".to_string(), now);
            seen.insert("guest-1".to_string(), now - Duration::from_secs(301));
        }
        let app = open_router(state.clone());
        reconcile_tick(&state).await;
        let placed = desired_hops(&app, "strom-node-1").await;

        let events = deliveries(&mut sink).await;
        assert_eq!(
            event_types(&events),
            [EventType::NodeOffline, EventType::NodeForgotten]
        );
        assert_eq!(events[1].subject.id(), "guest-1");
        let (_, body) = send(&app, "GET", "/nodes", None).await;
        let listed: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            ["strom-node-1", "strom-node-2"]
        );
        assert!(!state.last_seen.read().await.contains_key("guest-1"));
        assert!(
            mem.load_nodes()
                .await
                .unwrap()
                .iter()
                .all(|registration| registration.node.id != "guest-1")
        );
        assert_eq!(desired_hops(&app, "guest-1").await.0, StatusCode::NOT_FOUND);

        reconcile_tick(&state).await;
        assert_eq!(
            desired_hops(&app, "strom-node-1").await,
            placed,
            "forgetting a node no stream names moves no hop"
        );

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/guest-1/heartbeat",
            Some(json!({ "node_id": "guest-1", "status": "ready" })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&guest).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(state.nodes.read().await.contains_key("guest-1"));
    }

    #[tokio::test]
    async fn a_node_a_stream_names_stays_listed_as_offline() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;
        {
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now - Duration::from_secs(301));
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (_, body) = send(&app, "GET", "/nodes", None).await;
        let listed: Vec<NodeDescriptor> = serde_json::from_value(body).unwrap();
        let node1 = listed
            .iter()
            .find(|node| node.id == "strom-node-1")
            .unwrap();
        assert_eq!(node1.status, NodeStatus::Offline);
        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
    }

    /// Holds `last_seen` so a request stops at its first wait on it, then plays
    /// the start of a tick, which takes the nodes lock and then `last_seen`.
    async fn offline_check_during(
        state: &AppState,
        request: impl std::future::Future<Output = (StatusCode, Value)>,
    ) -> String {
        let last_seen = state.last_seen.write().await;
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut request)
                .await
                .is_err(),
            "the request waits for last_seen"
        );
        let tick_in_the_gap = state
            .nodes
            .try_write()
            .map(|mut nodes| mark_offline(&mut nodes, &last_seen, Instant::now(), state.node_ttl));
        drop(last_seen);
        assert_eq!(request.await.0, StatusCode::ACCEPTED);
        format!("{tick_in_the_gap:?}")
    }

    #[tokio::test]
    async fn a_tick_cannot_see_a_heartbeat_half_applied() {
        let (state, _mem) = mem_state();
        state.nodes.write().await.insert(
            "strom-node-1".to_string(),
            node_registration("strom-node-1", "172.26.0.10"),
        );
        state.last_seen.write().await.insert(
            "strom-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        let app = open_router(state.clone());

        let heartbeat = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            Some(json!({ "node_id": "strom-node-1", "status": "ready" })),
        );
        let seen = offline_check_during(&state, heartbeat).await;

        assert!(seen.starts_with("Err"), "a tick in the gap got {seen}");
        reconcile_tick(&state).await;
        assert_eq!(
            state.nodes.read().await["strom-node-1"].node.status,
            NodeStatus::Ready
        );
    }

    #[tokio::test]
    async fn a_tick_cannot_see_a_registration_half_applied() {
        let (state, _mem) = mem_state();
        let registration = node_registration("strom-node-1", "172.26.0.10");
        state
            .nodes
            .write()
            .await
            .insert("strom-node-1".to_string(), registration.clone());
        state.last_seen.write().await.insert(
            "strom-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        let app = open_router(state.clone());

        let register = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        );
        let seen = offline_check_during(&state, register).await;

        assert!(seen.starts_with("Err"), "a tick in the gap got {seen}");
    }
}

#[cfg(test)]
mod node_auth_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use weave_core::auth::{MinEpochs, NodeKey, Token};

    use super::tests::{NORTH_ROUTES, SHARED_READ_ROUTES, SOUTH_ROUTES};

    const NORTH: &str = "north-test-token";
    const KEY: &str = "south-test-key-0123456789abcdef0123";

    async fn app() -> (Router, AppState) {
        let state = AppState::hydrate(
            Arc::new(MemStore::new()),
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let app = router_after_first_tick(
            &state,
            Guard::Required(Token::new(NORTH).unwrap()),
            NodeGuard::Required(NodeKey::new(KEY).unwrap()),
        )
        .await;
        (app, state)
    }

    fn node_bearer(node_id: &str) -> String {
        format!(
            "Bearer {}",
            NodeKey::new(KEY).unwrap().token_for(node_id, 0)
        )
    }

    fn registration(node_id: &str) -> Value {
        json!({
            "protocol_version": PROTOCOL_VERSION,
            "node": {
                "id": node_id,
                "endpoint": format!("http://{node_id}:8091"),
                "status": "ready",
                "capabilities": { "adapters": [], "hop_profiles": [] },
                "topology": {
                    "attachments": [{ "id": "wan", "network": "internet", "dial": true, "listeners": {} }]
                }
            },
            "endpoints": [],
            "hop_status": []
        })
    }

    fn heartbeat(node_id: &str) -> Value {
        json!({ "node_id": node_id, "status": "ready", "endpoints": [], "hop_status": [] })
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        authorization: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", authorization)
            .header("content-type", "application/json")
            .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn node_ids(app: &Router) -> Vec<String> {
        let (status, nodes) = send(app, "GET", "/nodes", &format!("Bearer {NORTH}"), None).await;
        assert_eq!(status, StatusCode::OK);
        nodes
            .as_array()
            .unwrap()
            .iter()
            .map(|node| node["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn a_node_registers_heartbeats_and_reads_desired_as_itself() {
        let (app, state) = app().await;
        let me = node_bearer("strom-node-1");

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            &me,
            Some(registration("strom-node-1")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            &me,
            Some(heartbeat("strom-node-1")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        let (status, hops) = send(&app, "GET", "/nodes/strom-node-1/desired", &me, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(hops, json!([]));
        assert_eq!(node_ids(&app).await, ["strom-node-1"]);
    }

    #[tokio::test]
    async fn a_node_token_is_refused_for_another_node() {
        let (app, _) = app().await;
        let other = node_bearer("strom-node-2");
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            &node_bearer("strom-node-1"),
            Some(registration("strom-node-1")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        for (method, uri, body) in [
            (
                "POST",
                "/nodes/register",
                Some(registration("strom-node-1")),
            ),
            (
                "POST",
                "/nodes/register",
                Some(registration("strom-node-3")),
            ),
            (
                "POST",
                "/nodes/strom-node-1/heartbeat",
                Some(heartbeat("strom-node-1")),
            ),
            ("GET", "/nodes/strom-node-1/desired", None),
        ] {
            let (status, error) = send(&app, method, uri, &other, body).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}");
            assert_eq!(error["code"], "forbidden", "{method} {uri}");
        }
        assert_eq!(
            node_ids(&app).await,
            ["strom-node-1"],
            "a forged registration is not recorded"
        );
    }

    fn with_endpoints(mut body: Value, owners: &[Option<&str>]) -> Value {
        body["endpoints"] = owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                json!({ "id": format!("cam-{index}"), "label": "cam", "node_id": owner, "kind": "source" })
            })
            .collect();
        body
    }

    async fn endpoint_owners(app: &Router) -> Vec<Option<String>> {
        let (status, endpoints) =
            send(app, "GET", "/endpoints", &node_bearer("strom-node-1"), None).await;
        assert_eq!(status, StatusCode::OK);
        endpoints
            .as_array()
            .unwrap()
            .iter()
            .map(|endpoint| endpoint["node_id"].as_str().map(str::to_string))
            .collect()
    }

    #[tokio::test]
    async fn a_node_publishes_endpoints_only_under_its_own_id() {
        let (app, _) = app().await;
        let me = node_bearer("strom-node-1");
        let foreign = [Some("strom-node-1"), Some("strom-node-2")];

        let (status, error) = send(
            &app,
            "POST",
            "/nodes/register",
            &me,
            Some(with_endpoints(registration("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error["code"], "forbidden");
        assert!(node_ids(&app).await.is_empty(), "nothing is recorded");

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            &me,
            Some(with_endpoints(
                registration("strom-node-1"),
                &[Some("strom-node-1"), None],
            )),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, error) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            &me,
            Some(with_endpoints(heartbeat("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error["code"], "forbidden");
        assert_eq!(
            endpoint_owners(&app).await,
            [Some("strom-node-1".to_string()), None],
            "the refused heartbeat changed nothing"
        );
    }

    #[tokio::test]
    async fn without_auth_a_node_may_publish_endpoints_under_any_id() {
        let (_, state) = app().await;
        let app = router(state, Guard::Disabled, NodeGuard::Disabled);
        let foreign = [Some("strom-node-2")];
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            "",
            Some(with_endpoints(registration("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            "",
            Some(with_endpoints(heartbeat("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            endpoint_owners(&app).await,
            [Some("strom-node-2".to_string())]
        );
    }

    #[tokio::test]
    async fn a_token_below_its_nodes_minimum_epoch_gets_401_and_others_pass() {
        let (_, state) = app().await;
        let key = NodeKey::new(KEY).unwrap();
        let app = router(
            state,
            Guard::Required(Token::new(NORTH).unwrap()),
            NodeGuard::Required(
                key.clone()
                    .with_min_epochs(MinEpochs::parse("strom-node-1=1").unwrap()),
            ),
        );
        for (node_id, epoch, expected) in [
            ("strom-node-1", 0, StatusCode::UNAUTHORIZED),
            ("strom-node-1", 1, StatusCode::ACCEPTED),
            ("strom-node-2", 0, StatusCode::ACCEPTED),
        ] {
            let bearer = format!("Bearer {}", key.token_for(node_id, epoch));
            let (status, _) = send(
                &app,
                "POST",
                "/nodes/register",
                &bearer,
                Some(registration(node_id)),
            )
            .await;
            assert_eq!(status, expected, "{node_id} at epoch {epoch}");
        }
    }

    #[tokio::test]
    async fn node_inventory_accepts_the_north_token_or_any_node_token() {
        let (app, _) = app().await;
        for authorization in [format!("Bearer {NORTH}"), node_bearer("browser-a1b2")] {
            let (status, _) = send(&app, "GET", "/nodes", &authorization, None).await;
            assert_eq!(status, StatusCode::OK, "{authorization}");
        }
        for authorization in [
            "Bearer wrong".to_string(),
            format!("Bearer {KEY}"),
            "Bearer south-test-token".to_string(),
        ] {
            let (status, _) = send(&app, "GET", "/nodes", &authorization, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{authorization}");
        }
    }

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

    /// The dashboard surface is unauthenticated — it is browser-loaded and cannot
    /// carry a bearer token. `/health` is open for healthchecks.
    #[tokio::test]
    async fn dashboard_and_health_stay_open() {
        let (app, _) = app().await;
        for uri in ["/", "/ui", "/health", "/view", "/status"] {
            let (status, _) = send_auth(&app, "GET", uri, None).await;
            assert_eq!(status, StatusCode::OK, "{uri} must not require a token");
        }
    }

    #[tokio::test]
    async fn api_routes_reject_missing_and_wrong_tokens() {
        let (app, _) = app().await;

        for authorization in [None, Some("Bearer wrong-token"), Some("Basic ignored")] {
            for (method, uri) in NORTH_ROUTES
                .iter()
                .chain(&SOUTH_ROUTES)
                .chain(&SHARED_READ_ROUTES)
            {
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

    /// The surfaces are separated, not merely authenticated: a node's token
    /// cannot create or delete streams, and the operator token cannot register
    /// nodes or read their desired hops.
    #[tokio::test]
    async fn each_surface_rejects_the_other_surfaces_token() {
        let (app, _) = app().await;
        let north = format!("Bearer {NORTH}");
        let node = node_bearer("strom-node-1");

        for (method, uri) in NORTH_ROUTES {
            let (status, _) = send_auth(&app, method, uri, Some(&node)).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must reject a node token"
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
        let (app, _) = app().await;

        // Past the guard is enough: these are covered behaviourally elsewhere, so
        // only "not 401" matters here.
        let (status, _) =
            send_auth(&app, "GET", "/streams", Some(&format!("Bearer {NORTH}"))).await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) =
            send_auth(&app, "GET", "/state", Some(&node_bearer("strom-node-1"))).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn surfaces_do_not_accept_each_others_tokens() {
        let (app, _) = app().await;
        let (status, _) = send(&app, "GET", "/streams", &node_bearer("strom-node-1"), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        for (uri, past_auth) in [
            ("/endpoints", StatusCode::OK),
            ("/state", StatusCode::OK),
            ("/nodes/strom-node-1/desired", StatusCode::NOT_FOUND),
        ] {
            let (status, _) = send(&app, "GET", uri, &format!("Bearer {NORTH}"), None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
            let (status, _) = send(&app, "GET", uri, &node_bearer("strom-node-1"), None).await;
            assert_eq!(status, past_auth, "{uri}");
        }
    }
}

#[cfg(test)]
mod key_exposure_tests {
    use super::*;
    use crate::webhook::tests::sink;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use weave_core::{
        HopEndpointClass, HopProfile, NetworkAttachment, NetworkListeners, NodeCapabilities,
        NodeTopology, Passphrase, PortRange, RoleSet, SocketSpec, SrtEndpoint, SrtListener,
        StreamDestination, StreamTransport, Transport, TransportClass,
    };

    const PRODUCER_KEY: &str = "producer-passphrase-1";
    const CONSUMER_KEY: &str = "consumer-passphrase-1";

    fn attachment(network: &str, host: &str) -> NetworkAttachment {
        NetworkAttachment {
            id: network.to_string(),
            network: network.to_string(),
            dial: true,
            listeners: NetworkListeners {
                srt: Some(SrtListener {
                    host: host.to_string(),
                    port_range: PortRange {
                        start: 20_000,
                        end: 20_100,
                    },
                }),
                whip: None,
                whep: None,
                rist: None,
            },
        }
    }

    /// A node on `net-a` and `net-b`, so a destination on it can take two paths.
    fn registration(id: &str, hosts: [&str; 2], merge: bool) -> NodeRegistration {
        let srt = || {
            HopEndpointClass::Transport(TransportClass {
                transport: Transport::Srt,
                roles: RoleSet::both(),
            })
        };
        let profile = |id: &str, merge| HopProfile {
            id: id.to_string(),
            ingress: srt(),
            egress: srt(),
            max_egresses: None,
            merge,
            accepts: None,
        };
        let mut hop_profiles = vec![profile("srt-forward", false)];
        if merge {
            hop_profiles.push(profile("srt-merge", true));
        }
        NodeRegistration {
            protocol_version: PROTOCOL_VERSION,
            node: NodeDescriptor {
                id: id.to_string(),
                endpoint: format!("http://{id}"),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities {
                    adapters: Vec::new(),
                    hop_profiles,
                },
                topology: NodeTopology {
                    attachments: vec![attachment("net-a", hosts[0]), attachment("net-b", hosts[1])],
                },
            },
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    fn endpoint(node: &str, passphrase: &str) -> StreamTransport {
        StreamTransport::Srt(SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: Some(Passphrase::new(passphrase)),
            format: None,
            accepts: None,
        })
    }

    fn keyed_stream(paths: u8) -> StreamDefinition {
        StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            allow_cleartext_links: false,
            source: endpoint("node-a", PRODUCER_KEY),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                paths,
                endpoint: endpoint("node-b", CONSUMER_KEY),
            }],
        }
    }

    async fn send(app: &Router, request: Request<Body>) -> (StatusCode, String) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    fn json_request(method: &str, uri: &str, value: &impl Serialize) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::IF_NONE_MATCH, "*")
            .body(Body::from(serde_json::to_vec(value).unwrap()))
            .unwrap()
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn hops(body: &str) -> Vec<DesiredHop> {
        serde_json::from_str(body).unwrap_or_else(|error| panic!("{error}: {body}"))
    }

    fn passphrases(hops: &[DesiredHop]) -> Vec<String> {
        hops.iter()
            .flat_map(DesiredHop::sockets)
            .filter_map(|socket| match socket {
                SocketSpec::Srt(socket) => socket.params().passphrase.as_ref(),
                _ => None,
            })
            .map(|passphrase| passphrase.expose().to_string())
            .collect()
    }

    async fn keys_reach_desired_hops_and_no_other_route(paths: u8) {
        let mut hooks = sink(StatusCode::OK).await;
        let webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(hooks.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let state = AppState::hydrate(
            Arc::new(MemStore::new()),
            Duration::from_secs(60),
            Duration::from_secs(300),
            webhooks,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let app = router(state.clone(), Guard::Disabled, NodeGuard::Disabled);
        for node in [
            registration("node-a", ["192.0.2.1", "198.51.100.1"], false),
            registration("node-b", ["192.0.2.2", "198.51.100.2"], true),
        ] {
            let (status, _) = send(&app, json_request("POST", ROUTE_NODE_REGISTER, &node)).await;
            assert_eq!(status, StatusCode::ACCEPTED);
        }
        let (status, _) = send(
            &app,
            json_request("POST", ROUTE_STREAMS, &keyed_stream(paths)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;

        let sender = passphrases(&hops(&send(&app, get("/nodes/node-a/desired")).await.1));
        let receiver = passphrases(&hops(&send(&app, get("/nodes/node-b/desired")).await.1));
        assert!(sender.contains(&PRODUCER_KEY.to_string()));
        assert!(receiver.contains(&CONSUMER_KEY.to_string()));
        let mut link_keys: Vec<String> = sender
            .iter()
            .filter(|key| key.as_str() != PRODUCER_KEY)
            .cloned()
            .collect();
        link_keys.sort();
        let mut receiver_link_keys: Vec<String> = receiver
            .iter()
            .filter(|key| key.as_str() != CONSUMER_KEY)
            .cloned()
            .collect();
        receiver_link_keys.sort();
        assert_eq!(link_keys, receiver_link_keys, "both ends share each key");
        link_keys.dedup();
        assert_eq!(link_keys.len(), usize::from(paths), "one key per path");

        let (_, resource) = send(&app, get("/streams/feed")).await;
        assert!(
            resource.contains(PRODUCER_KEY),
            "the manifest reads back as written"
        );

        let (status, plan) = send(
            &app,
            json_request("POST", ROUTE_STREAM_PLANS, &keyed_stream(paths)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let planned: Value = serde_json::from_str(&plan).unwrap();
        let planned = hops(&planned["hops"].to_string());
        assert_eq!(passphrases(&planned), Vec::<String>::new());
        assert_eq!(
            planned.iter().any(|hop| hop.merge_ingress.is_some()),
            paths == 2,
            "{plan}"
        );
        assert!(plan.contains("\"pbkeylen\":32"));

        let mut events = Vec::new();
        while let Ok(delivery) =
            tokio::time::timeout(Duration::from_millis(300), hooks.next()).await
        {
            events.push(serde_json::to_string(&delivery.event).unwrap());
        }
        assert!(!events.is_empty(), "webhooks were sent");

        let routes = [
            ("/view", send(&app, get("/view")).await.1),
            (ROUTE_STATUS, send(&app, get(ROUTE_STATUS)).await.1),
            (ROUTE_STREAMS, send(&app, get(ROUTE_STREAMS)).await.1),
            ("/streams/feed", resource),
            (
                "/streams/feed/endpoints",
                send(&app, get("/streams/feed/endpoints")).await.1,
            ),
            (ROUTE_STREAM_PLANS, plan),
        ];
        for (route, body) in &routes {
            assert!(
                body.contains("node-a"),
                "{route} describes the stream: {body}"
            );
            for secret in &link_keys {
                assert!(!body.contains(secret), "{route} leaks a link key: {body}");
            }
        }
        for (route, body) in routes
            .iter()
            .filter(|(route, _)| !matches!(*route, ROUTE_STREAMS | "/streams/feed"))
        {
            for secret in [PRODUCER_KEY, CONSUMER_KEY] {
                assert!(!body.contains(secret), "{route} leaks a key: {body}");
            }
        }
        for event in &events {
            for secret in link_keys
                .iter()
                .map(String::as_str)
                .chain([PRODUCER_KEY, CONSUMER_KEY])
            {
                assert!(!event.contains(secret), "a webhook leaks a key: {event}");
            }
        }
    }

    #[tokio::test]
    async fn one_path_keys_reach_desired_hops_and_no_other_route() {
        keys_reach_desired_hops_and_no_other_route(1).await;
    }

    #[tokio::test]
    async fn two_path_keys_reach_desired_hops_and_no_other_route() {
        keys_reach_desired_hops_and_no_other_route(2).await;
    }
}
