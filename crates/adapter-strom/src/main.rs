//! `weave-adapter-strom` — southbound adapter for Strom instances.

mod provision;

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::{Json, Router, routing::get};
use clap::Parser;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::{
    AdapterDescriptor, AdapterKind, DEFAULT_DATA_PLANE_ALIAS, DesiredHop, EndpointDescriptor,
    EndpointKind, HopStatus, LinkCondition, LinkStats, NodeCapabilities, NodeDescriptor,
    NodeHeartbeat, NodeRegistration, NodeStatus, PortRange, TransportDescriptor,
};
use weave_strom::{
    FlowSpec, FlowStats, StromClient, StromError, StromFlow, flow_spec_from_hop, parse_flow_stats,
};

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
    #[arg(long, env = "WEAVE_NODE_ID", default_value = "strom-local")]
    node_id: String,
    #[arg(
        long,
        env = "WEAVE_STROM_ADAPTER_ADDR",
        default_value = "127.0.0.1:8091"
    )]
    listen: String,
    #[arg(long, env = "WEAVE_STROM_ADAPTER_PUBLIC_ENDPOINT")]
    public_endpoint: Option<String>,
    /// Data-plane addresses advertised for placement, as `alias=host` pairs
    /// (e.g. `default=10.0.0.5,wan=203.0.113.7`). The `default` alias is used
    /// when a manifest pins no network.
    #[arg(long, env = "WEAVE_DATA_PLANE")]
    data_plane: Option<String>,
    /// Inclusive port range the controller may assign from, as `start-end`.
    #[arg(long, env = "WEAVE_PORT_RANGE")]
    port_range: Option<String>,
    #[arg(
        long,
        env = "WEAVE_SOUTHBOUND_URL",
        default_value = "http://127.0.0.1:8081"
    )]
    southbound_url: String,
    #[arg(
        long,
        env = "WEAVE_STROM_URL",
        default_value = "http://127.0.0.1:18080"
    )]
    strom_url: String,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "srt,webrtc,aes67,ndi,decklink"
    )]
    transports: Vec<String>,
    #[arg(long, env = "WEAVE_STROM_POLL_INTERVAL_SECS", default_value_t = 5)]
    poll_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let public_endpoint = args
        .public_endpoint
        .clone()
        .unwrap_or_else(|| format!("http://{}", args.listen));
    let data_plane = parse_data_plane(args.data_plane.as_deref());
    let port_range = parse_port_range(args.port_range.as_deref());
    let client = Client::new();
    let strom = StromClient::new(&args.strom_url);
    let health_server = spawn_health_server(args.listen.clone());

    tracing::info!(
        node_id = %args.node_id,
        strom_url = %args.strom_url,
        southbound_url = %args.southbound_url,
        poll_interval_secs = args.poll_interval_secs,
        data_plane = ?data_plane,
        port_range = ?port_range,
        "Strom adapter starting"
    );

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("waiting for shutdown signal")?;
            tracing::info!("Strom adapter shutting down");
            health_server.abort();
            Ok(())
        }
        result = sync_loop(&client, &strom, &args, &public_endpoint, &data_plane, port_range) => {
            health_server.abort();
            result
        }
    }
}

/// Parse `alias=host` pairs into a data-plane map, dropping malformed entries.
fn parse_data_plane(raw: Option<&str>) -> BTreeMap<String, String> {
    raw.map(|value| {
        value
            .split(',')
            .filter_map(|pair| {
                let (alias, host) = pair.split_once('=')?;
                let (alias, host) = (alias.trim(), host.trim());
                (!alias.is_empty() && !host.is_empty())
                    .then(|| (alias.to_string(), host.to_string()))
            })
            .collect()
    })
    .unwrap_or_default()
}

/// Parse an inclusive `start-end` port range.
fn parse_port_range(raw: Option<&str>) -> Option<PortRange> {
    let (start, end) = raw?.split_once('-')?;
    Some(PortRange {
        start: start.trim().parse().ok()?,
        end: end.trim().parse().ok()?,
    })
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
    client: &Client,
    strom: &StromClient,
    args: &Args,
    public_endpoint: &str,
    data_plane: &BTreeMap<String, String>,
    port_range: Option<PortRange>,
) -> Result<()> {
    let mut registered = false;
    let mut tracker = StallTracker::default();
    let interval = Duration::from_secs(args.poll_interval_secs);

    loop {
        match sync_once(
            client,
            strom,
            args,
            public_endpoint,
            data_plane,
            port_range,
            registered,
            &mut tracker,
        )
        .await
        {
            Ok(next_registered) => registered = next_registered,
            Err(error) => {
                registered = false;
                tracing::warn!(%error, "Strom adapter sync failed");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn sync_once(
    client: &Client,
    strom: &StromClient,
    args: &Args,
    public_endpoint: &str,
    data_plane: &BTreeMap<String, String>,
    port_range: Option<PortRange>,
    registered: bool,
    tracker: &mut StallTracker,
) -> Result<bool> {
    let (status, flows) = match strom.list_flows().await {
        Ok(flows) => (NodeStatus::Ready, flows),
        Err(error) => {
            tracing::warn!(%error, "Strom observation failed");
            (NodeStatus::Degraded, Vec::new())
        }
    };
    let endpoints = strom_endpoints(&args.node_id, &flows);
    let data_plane_host = data_plane.get(DEFAULT_DATA_PLANE_ALIAS).map(String::as_str);

    let hop_status = if status == NodeStatus::Ready {
        match provision(
            client,
            strom,
            &args.southbound_url,
            &args.node_id,
            &flows,
            data_plane_host,
            tracker,
        )
        .await
        {
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
        args,
        public_endpoint,
        data_plane,
        port_range,
        status,
        endpoints.clone(),
        hop_status.clone(),
    );

    if !registered {
        register_node(client, &args.southbound_url, &registration).await?;
        return Ok(true);
    }

    let heartbeat = NodeHeartbeat {
        node_id: args.node_id.clone(),
        status,
        endpoints,
        hop_status,
    };

    if heartbeat_node(client, &args.southbound_url, &heartbeat).await? == StatusCode::NOT_FOUND {
        register_node(client, &args.southbound_url, &registration).await?;
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
    client: &Client,
    strom: &StromClient,
    southbound_url: &str,
    node_id: &str,
    flows: &[StromFlow],
    data_plane_host: Option<&str>,
    tracker: &mut StallTracker,
) -> Result<Vec<HopStatus>> {
    let desired = fetch_desired(client, southbound_url, node_id).await?;
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

async fn fetch_desired(
    client: &Client,
    southbound_url: &str,
    node_id: &str,
) -> Result<Vec<DesiredHop>> {
    client
        .get(join_url(
            southbound_url,
            &format!("/nodes/{node_id}/desired"),
        ))
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
    args: &Args,
    public_endpoint: &str,
    data_plane: &BTreeMap<String, String>,
    port_range: Option<PortRange>,
    status: NodeStatus,
    endpoints: Vec<EndpointDescriptor>,
    hop_status: Vec<HopStatus>,
) -> NodeRegistration {
    NodeRegistration {
        node: NodeDescriptor {
            id: args.node_id.clone(),
            endpoint: public_endpoint.to_string(),
            status,
            capabilities: NodeCapabilities {
                adapters: vec![AdapterDescriptor {
                    name: "strom".to_string(),
                    kind: AdapterKind::Strom,
                }],
                transports: args
                    .transports
                    .iter()
                    .map(|name| TransportDescriptor { name: name.clone() })
                    .collect(),
                data_plane: data_plane.clone(),
                port_range,
            },
        },
        endpoints,
        hop_status,
    }
}

async fn register_node(
    client: &Client,
    southbound_url: &str,
    registration: &NodeRegistration,
) -> Result<()> {
    let response = client
        .post(join_url(southbound_url, "/nodes/register"))
        .json(registration)
        .send()
        .await
        .context("registering Strom adapter")?;

    ensure_success(response, "southbound node registration failed").await?;
    tracing::info!(node_id = %registration.node.id, "Strom adapter registered");
    Ok(())
}

async fn heartbeat_node(
    client: &Client,
    southbound_url: &str,
    heartbeat: &NodeHeartbeat,
) -> Result<StatusCode> {
    let response = client
        .post(join_url(
            southbound_url,
            &format!("/nodes/{}/heartbeat", heartbeat.node_id),
        ))
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

fn join_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
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

    #[test]
    fn parses_data_plane_pairs_and_skips_malformed() {
        let map = parse_data_plane(Some("default=10.0.0.5, wan=203.0.113.7 ,bad,=x,y="));
        assert_eq!(map.get("default").map(String::as_str), Some("10.0.0.5"));
        assert_eq!(map.get("wan").map(String::as_str), Some("203.0.113.7"));
        assert_eq!(map.len(), 2);
        assert!(parse_data_plane(None).is_empty());
    }

    #[test]
    fn parses_port_range() {
        assert_eq!(
            parse_port_range(Some("7000-7999")),
            Some(PortRange {
                start: 7000,
                end: 7999
            })
        );
        assert_eq!(parse_port_range(Some("nonsense")), None);
        assert_eq!(parse_port_range(None), None);
    }
}
