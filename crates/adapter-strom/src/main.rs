//! `weave-adapter-strom` — southbound adapter for Strom instances.

mod config;
mod provision;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::{Json, Router, routing::get};
use clap::Parser;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::auth::{self, Token};
use weave_core::{
    API_V1, AdapterDescriptor, AdapterKind, DEFAULT_DATA_PLANE_ALIAS, DesiredHop,
    EndpointDescriptor, EndpointKind, HopStatus, LinkCondition, LinkStats, NodeCapabilities,
    NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus, PROTOCOL_VERSION,
    TransportDescriptor,
};
use weave_strom::{
    FlowSpec, FlowStats, StromClient, StromError, StromFlow, flow_spec_from_hop, parse_flow_stats,
};

use config::AdapterConfig;
use provision::{
    IngressObservation, StallTracker, diff_hops, hop_state, resolved_addr, socket_condition,
};

#[derive(Debug, Parser)]
#[command(
    name = "weave-adapter-strom",
    version,
    about = "open-weave southbound adapter for Strom"
)]
struct Args {
    /// Path to the adapter config file (YAML).
    #[arg(long, env = "WEAVE_NODE_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let config = AdapterConfig::load(&args.config)?;
    let public_endpoint = config.node.public_endpoint();

    let token = config.node.resolve_southbound_token()?;
    if token.is_none() {
        tracing::warn!(
            "{}=1: calling southbound without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }
    let southbound = Southbound::new(config.node.southbound_url.clone(), token);
    let strom_token = config.strom.resolve_token();
    let strom_auth = strom_token.is_some();
    let strom = StromClient::new(&config.strom.url).with_token(strom_token);
    let health_server = spawn_health_server(config.node.listen.clone());

    tracing::info!(
        node_id = %config.node.id,
        strom_url = %config.strom.url,
        southbound_url = %config.node.southbound_url,
        poll_interval_secs = config.strom.poll_interval_secs,
        strom_auth,
        data_plane = ?config.node.data_plane,
        port_range = ?config.node.port_range,
        "Strom adapter starting"
    );

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("waiting for shutdown signal")?;
            tracing::info!("Strom adapter shutting down");
            health_server.abort();
            Ok(())
        }
        result = sync_loop(&southbound, &strom, &config, &public_endpoint) => {
            health_server.abort();
            result
        }
    }
}

/// The southbound API as this adapter sees it: a base URL plus the bearer token
/// presented on every request. Bundling them keeps the token from having to be
/// threaded through the sync loop alongside the client.
///
/// Call sites name contract-relative paths; [`API_V1`] is applied in
/// [`Southbound::join`], so the version prefix appears once.
struct Southbound {
    http: Client,
    url: String,
    /// `None` only when authentication is explicitly disabled.
    token: Option<Token>,
}

impl Southbound {
    fn new(url: String, token: Option<Token>) -> Self {
        Self {
            http: Client::new(),
            url,
            token,
        }
    }

    fn get(&self, path: &str) -> RequestBuilder {
        self.authorized(self.http.get(self.join(path)))
    }

    fn post(&self, path: &str) -> RequestBuilder {
        self.authorized(self.http.post(self.join(path)))
    }

    fn authorized(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.header(reqwest::header::AUTHORIZATION, token.header_value()),
            None => request,
        }
    }

    fn join(&self, path: &str) -> String {
        format!("{}{API_V1}{path}", self.url.trim_end_matches('/'))
    }
}

fn spawn_health_server(addr: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = Router::new().route("/health", get(health));
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("binding Strom adapter health listener on {addr}"))?;
        tracing::info!(%addr, "Strom adapter health API listening");
        axum::serve(listener, app)
            .await
            .context("Strom adapter health server error")?;
        Ok(())
    })
}

async fn sync_loop(
    southbound: &Southbound,
    strom: &StromClient,
    config: &AdapterConfig,
    public_endpoint: &str,
) -> Result<()> {
    let mut registered = false;
    let mut tracker = StallTracker::default();
    let interval = Duration::from_secs(config.strom.poll_interval_secs);

    loop {
        match sync_once(
            southbound,
            strom,
            config,
            public_endpoint,
            registered,
            &mut tracker,
        )
        .await
        {
            Ok(next_registered) => registered = next_registered,
            Err(error) if error.is::<RegistrationRejected>() => {
                tracing::error!(
                    node_id = %config.node.id,
                    %error,
                    "Strom adapter cannot register with this control plane; exiting"
                );
                return Err(error);
            }
            Err(error) => {
                registered = false;
                tracing::warn!(%error, "Strom adapter sync failed");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

async fn sync_once(
    southbound: &Southbound,
    strom: &StromClient,
    config: &AdapterConfig,
    public_endpoint: &str,
    registered: bool,
    tracker: &mut StallTracker,
) -> Result<bool> {
    let node_id = &config.node.id;
    let (status, flows) = match strom.list_flows().await {
        Ok(flows) => (NodeStatus::Ready, flows),
        Err(error) => {
            tracing::warn!(%error, "Strom observation failed");
            (NodeStatus::Degraded, Vec::new())
        }
    };
    let endpoints = strom_endpoints(node_id, &flows);
    let data_plane_host = config
        .node
        .data_plane
        .get(DEFAULT_DATA_PLANE_ALIAS)
        .map(|addr| addr.host.as_str());

    let hop_status = if status == NodeStatus::Ready {
        match provision(southbound, strom, node_id, &flows, data_plane_host, tracker).await {
            Ok(hop_status) => hop_status,
            Err(error) => {
                tracing::warn!(%error, "provisioning desired hops failed");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let registration = registration(
        config,
        public_endpoint,
        status,
        endpoints.clone(),
        hop_status.clone(),
    );

    if !registered {
        register_node(southbound, &registration).await?;
        return Ok(true);
    }

    let heartbeat = NodeHeartbeat {
        node_id: node_id.clone(),
        status,
        endpoints,
        hop_status,
    };

    if heartbeat_node(southbound, &heartbeat).await? == StatusCode::NOT_FOUND {
        register_node(southbound, &registration).await?;
    }

    Ok(true)
}

/// Minimal Strom flow operations the reconciler needs, behind a trait so
/// provisioning order stays testable with a recording fake.
#[async_trait::async_trait]
trait FlowApi {
    async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError>;
    async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError>;
    async fn start_flow(&self, id: &str) -> Result<(), StromError>;
    async fn delete_flow(&self, id: &str) -> Result<(), StromError>;
    async fn srt_stats(&self, id: &str) -> Result<Value, StromError>;
}

#[async_trait::async_trait]
impl FlowApi for StromClient {
    async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError> {
        StromClient::list_flows(self).await
    }
    async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError> {
        StromClient::create_flow(self, spec).await
    }
    async fn start_flow(&self, id: &str) -> Result<(), StromError> {
        StromClient::start_flow(self, id).await
    }
    async fn delete_flow(&self, id: &str) -> Result<(), StromError> {
        StromClient::delete_flow(self, id).await
    }
    async fn srt_stats(&self, id: &str) -> Result<Value, StromError> {
        StromClient::srt_stats(self, id).await
    }
}

/// Pull desired hops for this node, reconcile them into Strom flows, and report
/// each hop's realised status. Inert when no desired hops are set.
async fn provision(
    southbound: &Southbound,
    strom: &StromClient,
    node_id: &str,
    flows: &[StromFlow],
    data_plane_host: Option<&str>,
    tracker: &mut StallTracker,
) -> Result<Vec<HopStatus>> {
    let desired = fetch_desired(southbound, node_id).await?;
    Ok(reconcile(strom, &desired, flows, data_plane_host, tracker).await)
}

/// Reconcile desired hops against observed flows in one poll cycle.
///
/// Deletes run before creates so a flow being torn down frees its SRT listener
/// port before a re-applied stream on the same port is created and started.
async fn reconcile(
    flow_api: &dyn FlowApi,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    data_plane_host: Option<&str>,
    tracker: &mut StallTracker,
) -> Vec<HopStatus> {
    let plan = diff_hops(desired, flows);

    for flow_id in &plan.delete {
        match flow_api.delete_flow(flow_id).await {
            Ok(()) => tracing::info!(flow_id = %flow_id, "deleted undesired managed flow"),
            Err(error) => tracing::warn!(flow_id = %flow_id, %error, "deleting flow failed"),
        }
    }

    let mut failed = std::collections::HashSet::new();
    for hop in &plan.create {
        if let Err(error) = provision_hop(flow_api, hop).await {
            tracing::warn!(hop = %hop.id, %error, "provisioning hop failed");
            failed.insert(hop.id.clone());
        }
    }
    for flow_id in &plan.start {
        if let Err(error) = flow_api.start_flow(flow_id).await {
            tracing::warn!(flow_id = %flow_id, %error, "starting adopted flow failed");
        }
    }

    let refreshed;
    let current: &[StromFlow] = if plan.is_empty() {
        flows
    } else {
        refreshed = flow_api.list_flows().await.unwrap_or_default();
        &refreshed
    };

    let desired_ids: std::collections::HashSet<&str> =
        desired.iter().map(|h| h.id.as_str()).collect();
    tracker.retain(&desired_ids);
    hop_statuses(
        flow_api,
        desired,
        current,
        data_plane_host,
        &failed,
        tracker,
    )
    .await
}

async fn hop_statuses(
    strom: &dyn FlowApi,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    data_plane_host: Option<&str>,
    failed: &std::collections::HashSet<String>,
    tracker: &mut StallTracker,
) -> Vec<HopStatus> {
    let mut statuses = Vec::with_capacity(desired.len());
    for hop in desired {
        let flow = flows.iter().find(|f| f.name == hop.id);
        let stats = match flow {
            Some(flow) => match strom.srt_stats(&flow.id).await {
                Ok(value) => Some(parse_flow_stats(&value)),
                Err(error) => {
                    tracing::debug!(hop = %hop.id, %error, "srt-stats unavailable");
                    None
                }
            },
            None => None,
        };

        let ingress = stats.as_ref().and_then(FlowStats::ingress);
        let (ingress_connected, ingress_rate) =
            ingress.map_or((false, 0.0), |e| (e.connected, e.rate_mbps));
        let (egress_connected, egress_rate) = stats
            .as_ref()
            .and_then(FlowStats::egress)
            .map_or((false, 0.0), |e| (e.connected, e.rate_mbps));

        let ingress_stalled = tracker.observe(
            &hop.id,
            IngressObservation {
                bytes_received: ingress.map(|e| e.bytes_received),
                running: flow.is_some_and(|f| f.running),
                gst_paused: flow.and_then(|f| f.gst_state.as_deref()) == Some("Paused"),
            },
        );

        let egress = hop.egresses.first();
        statuses.push(HopStatus {
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            state: hop_state(flow, failed.contains(&hop.id)),
            ingress: socket_condition(
                hop.ingress.role,
                ingress_connected,
                ingress_rate,
                ingress_stalled,
            ),
            egress: egress.map_or(LinkCondition::Idle, |e| {
                socket_condition(e.role, egress_connected, egress_rate, false)
            }),
            resolved_ingress: resolved_addr(&hop.ingress, data_plane_host),
            resolved_egress: egress.and_then(|e| resolved_addr(e, data_plane_host)),
            stats: stats.map(LinkStats::from),
        });
    }
    statuses
}

async fn provision_hop(strom: &dyn FlowApi, hop: &DesiredHop) -> Result<()> {
    let spec = flow_spec_from_hop(hop).with_context(|| format!("mapping hop {}", hop.id))?;
    let id = strom
        .create_flow(&spec)
        .await
        .with_context(|| format!("creating hop {}", hop.id))?;
    strom
        .start_flow(&id)
        .await
        .with_context(|| format!("starting hop {}", hop.id))?;
    tracing::info!(hop = %hop.id, flow_id = %id, "provisioned hop");
    Ok(())
}

async fn fetch_desired(southbound: &Southbound, node_id: &str) -> Result<Vec<DesiredHop>> {
    southbound
        .get(&format!("/nodes/{node_id}/desired"))
        .send()
        .await
        .context("fetching desired hops")?
        .error_for_status()
        .context("southbound desired request failed")?
        .json::<Vec<DesiredHop>>()
        .await
        .context("decoding desired hops")
}

fn registration(
    config: &AdapterConfig,
    public_endpoint: &str,
    status: NodeStatus,
    endpoints: Vec<EndpointDescriptor>,
    hop_status: Vec<HopStatus>,
) -> NodeRegistration {
    NodeRegistration {
        protocol_version: PROTOCOL_VERSION,
        node: NodeDescriptor {
            id: config.node.id.clone(),
            endpoint: public_endpoint.to_string(),
            status,
            capabilities: NodeCapabilities {
                adapters: vec![AdapterDescriptor {
                    name: "strom".to_string(),
                    kind: AdapterKind::Strom,
                }],
                transports: config
                    .node
                    .transports
                    .iter()
                    .map(|name| TransportDescriptor { name: name.clone() })
                    .collect(),
                data_plane: config.node.data_plane.clone(),
                port_range: Some(config.node.port_range),
                relay: config.node.relay,
            },
        },
        endpoints,
        hop_status,
    }
}

/// Registration the control plane will never accept, however long this adapter
/// keeps dialling — currently only a protocol-version mismatch, which the
/// controller answers with `409`. Distinct from a transient failure: the sync loop
/// stops on it instead of retrying.
#[derive(Debug, thiserror::Error)]
#[error("southbound rejected registration permanently: {status}: {body}")]
struct RegistrationRejected {
    status: StatusCode,
    body: String,
}

async fn register_node(southbound: &Southbound, registration: &NodeRegistration) -> Result<()> {
    let response = southbound
        .post("/nodes/register")
        .json(registration)
        .send()
        .await
        .context("registering Strom adapter")?;

    if response.status() == StatusCode::CONFLICT {
        return Err(RegistrationRejected {
            status: response.status(),
            body: response_body(response).await,
        }
        .into());
    }

    ensure_success(response, "southbound node registration failed").await?;
    tracing::info!(
        node_id = %registration.node.id,
        protocol_version = registration.protocol_version,
        "Strom adapter registered"
    );
    Ok(())
}

async fn heartbeat_node(southbound: &Southbound, heartbeat: &NodeHeartbeat) -> Result<StatusCode> {
    let response = southbound
        .post(&format!("/nodes/{}/heartbeat", heartbeat.node_id))
        .json(heartbeat)
        .send()
        .await
        .context("sending Strom adapter heartbeat")?;

    let status = response.status();
    if status.is_success() || status == StatusCode::NOT_FOUND {
        return Ok(status);
    }

    let body = response_body(response).await;
    bail!("southbound heartbeat failed: {status}: {body}")
}

async fn ensure_success(response: reqwest::Response, context: &str) -> Result<()> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }

    let body = response_body(response).await;
    bail!("{context}: {status}: {body}")
}

async fn response_body(response: reqwest::Response) -> String {
    match response.text().await {
        Ok(body) => body,
        Err(error) => format!("<failed to read body: {error}>"),
    }
}

fn strom_endpoints(node_id: &str, flows: &[StromFlow]) -> Vec<EndpointDescriptor> {
    flows
        .iter()
        .map(|flow| EndpointDescriptor {
            id: format!("strom:{node_id}:flow:{}", flow.id),
            label: flow.name.clone(),
            node_id: Some(node_id.to_string()),
            kind: EndpointKind::Flow,
            transports: flow_transports(flow)
                .into_iter()
                .map(|name| TransportDescriptor { name })
                .collect(),
            metadata: json!({
                "source": "strom",
                "strom_flow_id": flow.id,
                "running": flow.running,
            }),
        })
        .collect()
}

fn flow_transports(flow: &StromFlow) -> Vec<String> {
    let mut transports = Vec::new();

    for element in &flow.elements {
        collect_transport(&mut transports, &element.element_type);
        for value in element.properties.values() {
            collect_transport_value(&mut transports, value);
        }
    }

    for block in &flow.blocks {
        collect_transport(&mut transports, &block.block_definition_id);
        for value in block.properties.values() {
            collect_transport_value(&mut transports, value);
        }
    }

    transports
}

fn collect_transport(transports: &mut Vec<String>, text: &str) {
    let lower = text.to_ascii_lowercase();
    let transport = if lower.contains("srt") {
        Some("srt")
    } else if lower.contains("whip") || lower.contains("whep") || lower.contains("webrtc") {
        Some("webrtc")
    } else if lower.contains("aes67") {
        Some("aes67")
    } else if lower.contains("ndi") {
        Some("ndi")
    } else if lower.contains("decklink") {
        Some("decklink")
    } else if lower.contains("rtp") {
        Some("rtp")
    } else {
        None
    };

    if let Some(name) = transport {
        push_unique(transports, name);
    }
}

fn collect_transport_value(transports: &mut Vec<String>, value: &Value) {
    match value {
        Value::String(text) => collect_transport(transports, text),
        Value::Array(values) => {
            for value in values {
                collect_transport_value(transports, value);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_transport_value(transports, value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if values.iter().any(|existing| existing == value) {
        return;
    }
    values.push(value.to_string());
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use weave_core::{HopRole, SocketRole, SocketSpec, SrtParams, Transport};

    #[derive(Debug, Clone, PartialEq)]
    enum Op {
        Delete(String),
        Create(String),
        Start(String),
        List,
        Stats(String),
    }

    struct RecordingFlowApi {
        ops: Mutex<Vec<Op>>,
        flows_after: Vec<(String, String)>,
    }

    impl RecordingFlowApi {
        fn new(flows_after: Vec<(&str, &str)>) -> Self {
            Self {
                ops: Mutex::new(Vec::new()),
                flows_after: flows_after
                    .into_iter()
                    .map(|(name, id)| (name.to_string(), id.to_string()))
                    .collect(),
            }
        }

        fn ops(&self) -> Vec<Op> {
            self.ops.lock().expect("ops lock").clone()
        }

        fn record(&self, op: Op) {
            self.ops.lock().expect("ops lock").push(op);
        }
    }

    #[async_trait::async_trait]
    impl FlowApi for RecordingFlowApi {
        async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError> {
            self.record(Op::List);
            Ok(self
                .flows_after
                .iter()
                .map(|(name, id)| flow(name, id, true))
                .collect())
        }
        async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError> {
            self.record(Op::Create(spec.name.clone()));
            Ok(format!("id-{}", spec.name))
        }
        async fn start_flow(&self, id: &str) -> Result<(), StromError> {
            self.record(Op::Start(id.to_string()));
            Ok(())
        }
        async fn delete_flow(&self, id: &str) -> Result<(), StromError> {
            self.record(Op::Delete(id.to_string()));
            Ok(())
        }
        async fn srt_stats(&self, id: &str) -> Result<Value, StromError> {
            self.record(Op::Stats(id.to_string()));
            Ok(json!({}))
        }
    }

    fn hop(id: &str, port: u16) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: "strom-node-1".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Listen,
                host: None,
                port: Some(port),
                params: SrtParams::default(),
            },
            egresses: vec![SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Connect,
                host: Some("10.0.0.2".to_string()),
                port: Some(port + 1),
                params: SrtParams::default(),
            }],
        }
    }

    fn flow(name: &str, id: &str, running: bool) -> StromFlow {
        serde_json::from_value(json!({ "id": id, "name": name, "running": running }))
            .expect("flow fixture")
    }

    #[tokio::test]
    async fn reconcile_deletes_before_creating_on_same_ports() {
        // Old flows on ports 7001/7002; a differently-named stream re-applied on
        // the same ports. Every delete must precede every create so the listener
        // ports are freed before the new flows are created and started.
        let desired = vec![
            hop("weave-srt-latency-sender", 7001),
            hop("weave-srt-latency-receiver-0", 7002),
        ];
        let flows = vec![
            flow("weave-basic-sender", "id-basic-sender", true),
            flow("weave-basic-receiver-0", "id-basic-receiver-0", true),
        ];
        let fake = RecordingFlowApi::new(vec![
            ("weave-srt-latency-sender", "id-weave-srt-latency-sender"),
            (
                "weave-srt-latency-receiver-0",
                "id-weave-srt-latency-receiver-0",
            ),
        ]);

        let mut tracker = StallTracker::default();
        let _ = reconcile(&fake, &desired, &flows, None, &mut tracker).await;

        let ops = fake.ops();
        let last_delete = ops
            .iter()
            .rposition(|op| matches!(op, Op::Delete(_)))
            .expect("a delete was recorded");
        let first_create = ops
            .iter()
            .position(|op| matches!(op, Op::Create(_)))
            .expect("a create was recorded");
        assert!(
            last_delete < first_create,
            "all deletes must precede all creates within one cycle: {ops:?}"
        );
    }
}
