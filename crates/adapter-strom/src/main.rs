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
    AdapterDescriptor, AdapterKind, DesiredHop, EgressStatus, EndpointDescriptor, EndpointKind,
    HopEndpointClass, HopProfile, HopStatus, LinkCondition, LinkStats, NodeCapabilities,
    NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus, PROTOCOL_VERSION, RoleSet,
    SocketRole, SocketSpec, SocketStatus, Transport, TransportClass,
};
use weave_strom::{
    FlowSpec, FlowStats, StromClient, StromError, StromFlow, flow_spec_from_hop, parse_flow_stats,
};

use config::AdapterConfig;
use provision::{
    Side, SideObservation, StallTracker, diff_hops, hop_state, resolved_addr, socket_condition,
    webrtc_condition,
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
        topology = ?config.node.topology,
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
        format!("{}{path}", self.url.trim_end_matches('/'))
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
    let listener_host = config
        .node
        .topology
        .attachments
        .iter()
        .find_map(|attachment| attachment.listeners.srt.as_ref())
        .map(|listener| listener.host.as_str());

    let hop_status = if status == NodeStatus::Ready {
        match provision(southbound, strom, node_id, &flows, listener_host, tracker).await {
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
/// each hop's realised status. Touches no flow unless southbound answers `2xx`.
async fn provision(
    southbound: &Southbound,
    strom: &dyn FlowApi,
    node_id: &str,
    flows: &[StromFlow],
    listener_host: Option<&str>,
    tracker: &mut StallTracker,
) -> Result<Vec<HopStatus>> {
    let desired = fetch_desired(southbound, node_id).await?;
    Ok(reconcile(strom, &desired, flows, listener_host, tracker).await)
}

/// Reconcile desired hops against observed flows in one poll cycle.
///
/// Deletes run before creates so a flow being torn down frees its SRT listener
/// port before a re-applied stream on the same port is created and started.
async fn reconcile(
    flow_api: &dyn FlowApi,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    listener_host: Option<&str>,
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
    hop_statuses(flow_api, desired, current, listener_host, &failed, tracker).await
}

async fn hop_statuses(
    strom: &dyn FlowApi,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    listener_host: Option<&str>,
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
        let running = flow.is_some_and(|f| f.running);
        let gst_paused = flow.and_then(|f| f.gst_state.as_deref()) == Some("Paused");
        let ingress_stalled = tracker.observe(
            &hop.id,
            Side::Ingress,
            SideObservation {
                bytes: ingress.map(|e| e.bytes_received),
                running,
                gst_paused,
            },
        );
        let mut egress_stalled = Vec::with_capacity(hop.egresses.len());
        for (index, _) in hop.egresses.iter().enumerate() {
            let branch_stats = stats.as_ref().and_then(|stats| stats.egress_at(index));
            egress_stalled.push(tracker.observe(
                &hop.id,
                Side::Egress(index),
                SideObservation {
                    bytes: branch_stats.map(|stats| stats.bytes_sent),
                    running,
                    gst_paused,
                },
            ));
        }
        let playing = running && !gst_paused;
        let any_egress_advanced = hop
            .egresses
            .iter()
            .enumerate()
            .any(|(index, _)| tracker.advanced(&hop.id, Side::Egress(index)));
        let all_egresses_stalled =
            !egress_stalled.is_empty() && egress_stalled.iter().all(|stalled| *stalled);

        statuses.push(HopStatus {
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            state: hop_state(flow, failed.contains(&hop.id)),
            ingress: SocketStatus {
                condition: match &hop.ingress {
                    SocketSpec::Srt(socket) => socket_condition(
                        socket.role(),
                        ingress_connected,
                        ingress_rate,
                        ingress_stalled,
                    ),
                    SocketSpec::Whip(socket) | SocketSpec::Whep(socket) => webrtc_condition(
                        socket.role,
                        playing,
                        any_egress_advanced,
                        all_egresses_stalled,
                    ),
                    SocketSpec::Device(_) => LinkCondition::Idle,
                },
                resolved: resolved_addr(&hop.ingress, listener_host),
                stats: ingress.map(LinkStats::from),
            },
            egresses: hop
                .egresses
                .iter()
                .enumerate()
                .map(|(index, egress)| {
                    let branch_stats = stats.as_ref().and_then(|stats| stats.egress_at(index));
                    EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: SocketStatus {
                            condition: match &egress.socket {
                                SocketSpec::Srt(socket) => socket_condition(
                                    socket.role(),
                                    branch_stats.is_some_and(|stats| stats.connected),
                                    branch_stats.map_or(0.0, |stats| stats.rate_mbps),
                                    egress_stalled[index],
                                ),
                                SocketSpec::Whip(socket) | SocketSpec::Whep(socket) => {
                                    webrtc_condition(
                                        socket.role,
                                        playing,
                                        tracker.advanced(&hop.id, Side::Ingress),
                                        ingress_stalled,
                                    )
                                }
                                SocketSpec::Device(_) => LinkCondition::Idle,
                            },
                            resolved: resolved_addr(&egress.socket, listener_host),
                            stats: branch_stats.map(LinkStats::from),
                        },
                    }
                })
                .collect(),
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
                hop_profiles: strom_hop_profiles(),
            },
            topology: config.node.topology.clone(),
        },
        endpoints,
        hop_status,
    }
}

fn transport_class(transport: Transport, roles: RoleSet) -> HopEndpointClass {
    HopEndpointClass::Transport(TransportClass { transport, roles })
}

fn strom_hop_profiles() -> Vec<HopProfile> {
    vec![
        HopProfile {
            id: "srt-forward".to_string(),
            ingress: transport_class(Transport::Srt, RoleSet::both()),
            egress: transport_class(Transport::Srt, RoleSet::both()),
            max_egresses: None,
        },
        HopProfile {
            id: "whip-to-srt".to_string(),
            ingress: transport_class(Transport::Whip, RoleSet::only(SocketRole::Listen)),
            egress: transport_class(Transport::Srt, RoleSet::both()),
            max_egresses: None,
        },
        HopProfile {
            id: "srt-to-whep".to_string(),
            ingress: transport_class(Transport::Srt, RoleSet::both()),
            egress: transport_class(Transport::Whep, RoleSet::only(SocketRole::Listen)),
            max_egresses: None,
        },
    ]
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
            transports: flow_transports(flow),
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
    use weave_core::{DesiredEgress, HopRole};

    #[derive(Debug, Clone, PartialEq)]
    enum Op {
        Create(String),
        Start(String),
        Delete(String),
    }

    #[derive(Default)]
    struct RecordingFlowApi {
        ops: Mutex<Vec<Op>>,
        flows_after: Vec<(String, String)>,
        stats: Value,
    }

    impl RecordingFlowApi {
        fn listing(flows_after: &[(&str, &str)]) -> Self {
            Self {
                flows_after: flows_after
                    .iter()
                    .map(|(name, id)| ((*name).to_string(), (*id).to_string()))
                    .collect(),
                ..Self::default()
            }
        }

        fn with_stats(mut self, stats: Value) -> Self {
            self.stats = stats;
            self
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
            Ok(self
                .flows_after
                .iter()
                .map(|(name, id)| flow(name, id))
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
        async fn srt_stats(&self, _id: &str) -> Result<Value, StromError> {
            Ok(self.stats.clone())
        }
    }

    fn flow(name: &str, id: &str) -> StromFlow {
        serde_json::from_value(json!({ "id": id, "name": name, "running": true }))
            .expect("flow fixture")
    }

    /// A southbound whose desired-hops route answers `status` with `body`.
    async fn southbound_answering(status: StatusCode, body: Value) -> Southbound {
        let app = Router::new().route(
            "/nodes/{node_id}/desired",
            get(move || async move { (status, Json(body)) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Southbound::new(format!("http://{addr}"), None)
    }

    async fn provision_against(southbound: &Southbound, flows: &[StromFlow]) -> (bool, Vec<Op>) {
        let strom = RecordingFlowApi::default();
        let result = provision(
            southbound,
            &strom,
            "strom-node-1",
            flows,
            None,
            &mut StallTracker::default(),
        )
        .await;
        (result.is_ok(), strom.ops())
    }

    #[tokio::test]
    async fn a_desired_response_other_than_2xx_leaves_every_flow_alone() {
        let flows = [flow("weave-basic-sender", "id-managed")];
        let not_reconciled = json!({ "code": "node_not_found", "message": "no desired state" });
        for status in [
            StatusCode::NOT_FOUND,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let southbound = southbound_answering(status, not_reconciled.clone()).await;
            assert_eq!(
                provision_against(&southbound, &flows).await,
                (false, Vec::new()),
                "{status}"
            );
        }

        let unreachable = Southbound::new("http://127.0.0.1:1".to_string(), None);
        assert_eq!(
            provision_against(&unreachable, &flows).await,
            (false, Vec::new())
        );
    }

    #[tokio::test]
    async fn an_empty_desired_list_deletes_only_managed_flows() {
        let flows = [
            flow("weave-basic-sender", "id-managed"),
            flow("studio-mixer", "id-unmanaged"),
        ];
        let southbound = southbound_answering(StatusCode::OK, json!([])).await;
        assert_eq!(
            provision_against(&southbound, &flows).await,
            (true, vec![Op::Delete("id-managed".to_string())])
        );
    }

    fn hop(id: &str, port: u16) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: "strom-node-1".to_string(),
            profile_id: "srt-forward".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(port, 200),
            egresses: vec![DesiredEgress {
                branch_id: "studio".to_string(),
                socket: SocketSpec::srt_connect("10.0.0.2", port + 1, 1000),
            }],
        }
    }

    #[tokio::test]
    async fn hop_status_reports_each_fanout_branch_independently() {
        let mut desired = hop("weave-fanout-sender", 7001);
        desired.egresses.push(DesiredEgress {
            branch_id: "preview".to_string(),
            socket: SocketSpec::srt_connect("10.0.0.3", 7003, 1000),
        });
        let flows = vec![flow("weave-fanout-sender", "id-fanout")];
        let fake = RecordingFlowApi::default().with_stats(json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [
                    { "recv_rate_mbps": 4.5, "bytes_received": 1000 }
                ]},
                "srtsink_0": { "connected": true, "callers": [
                    { "send_rate_mbps": 4.4, "bytes_sent": 900,
                      "packets_sent_lost": 2 }
                ]},
                "srtsink_1": { "connected": false, "callers": [] }
            }}
        }));
        let mut tracker = StallTracker::default();

        let statuses = hop_statuses(
            &fake,
            &[desired],
            &flows,
            Some("10.0.0.1"),
            &std::collections::HashSet::new(),
            &mut tracker,
        )
        .await;

        let status = &statuses[0];
        assert_eq!(status.ingress.condition, LinkCondition::Flowing);
        assert_eq!(
            status.ingress.stats.as_ref().map(|stats| stats.rate_mbps),
            Some(4.5)
        );
        assert_eq!(status.egresses.len(), 2);
        assert_eq!(status.egresses[0].branch_id, "studio");
        assert_eq!(status.egresses[0].status.condition, LinkCondition::Flowing);
        assert_eq!(
            status.egresses[0]
                .status
                .resolved
                .as_ref()
                .map(|address| (address.host.as_str(), address.port)),
            Some(("10.0.0.2", 7002))
        );
        assert_eq!(
            status.egresses[0]
                .status
                .stats
                .as_ref()
                .map(|stats| (stats.rate_mbps, stats.packets_sent_lost)),
            Some((4.4, 2))
        );
        assert_eq!(status.egresses[1].branch_id, "preview");
        assert_eq!(
            status.egresses[1].status.condition,
            LinkCondition::Connecting
        );
        assert_eq!(
            status.egresses[1]
                .status
                .resolved
                .as_ref()
                .map(|address| (address.host.as_str(), address.port)),
            Some(("10.0.0.3", 7003))
        );
        assert_eq!(
            status.egresses[1]
                .status
                .stats
                .as_ref()
                .map(|stats| stats.rate_mbps),
            Some(0.0)
        );
    }

    #[tokio::test]
    async fn reconcile_deletes_before_creating_on_same_ports() {
        // Old flows on ports 7001/7002; a differently-named stream re-applied on
        // the same ports. Every delete must precede every create so the listener
        // ports are freed before the new flows are created and started.
        let desired = vec![
            hop("weave-srt-latency-sender", 7001),
            hop("weave-srt-latency-receiver-studio", 7002),
        ];
        let flows = vec![
            flow("weave-basic-sender", "id-basic-sender"),
            flow("weave-basic-receiver-studio", "id-basic-receiver-studio"),
        ];
        let fake = RecordingFlowApi::listing(&[
            ("weave-srt-latency-sender", "id-weave-srt-latency-sender"),
            (
                "weave-srt-latency-receiver-studio",
                "id-weave-srt-latency-receiver-studio",
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
