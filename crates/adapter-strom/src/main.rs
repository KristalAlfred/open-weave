//! `weave-adapter-strom` — southbound adapter for Strom instances.

mod provision;

use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::{Json, Router, routing::get};
use clap::Parser;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::{
    AdapterDescriptor, AdapterKind, DesiredHop, EndpointDescriptor, EndpointKind, HopStatus,
    LinkStats, NodeCapabilities, NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus,
    TransportDescriptor,
};
use weave_strom::{StromClient, StromFlow, flow_spec_from_hop, parse_flow_stats};

use provision::{diff_hops, hop_state, resolved_addr};

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
    let client = Client::new();
    let strom = StromClient::new(&args.strom_url);
    let health_server = spawn_health_server(args.listen.clone());

    tracing::info!(
        node_id = %args.node_id,
        strom_url = %args.strom_url,
        southbound_url = %args.southbound_url,
        poll_interval_secs = args.poll_interval_secs,
        "Strom adapter starting"
    );

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("waiting for shutdown signal")?;
            tracing::info!("Strom adapter shutting down");
            health_server.abort();
            Ok(())
        }
        result = sync_loop(&client, &strom, &args, &public_endpoint) => {
            health_server.abort();
            result
        }
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
    client: &Client,
    strom: &StromClient,
    args: &Args,
    public_endpoint: &str,
) -> Result<()> {
    let mut registered = false;
    let interval = Duration::from_secs(args.poll_interval_secs);

    loop {
        match sync_once(client, strom, args, public_endpoint, registered).await {
            Ok(next_registered) => registered = next_registered,
            Err(error) => {
                registered = false;
                tracing::warn!(%error, "Strom adapter sync failed");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

async fn sync_once(
    client: &Client,
    strom: &StromClient,
    args: &Args,
    public_endpoint: &str,
    registered: bool,
) -> Result<bool> {
    let (status, flows) = match strom.list_flows().await {
        Ok(flows) => (NodeStatus::Ready, flows),
        Err(error) => {
            tracing::warn!(%error, "Strom observation failed");
            (NodeStatus::Degraded, Vec::new())
        }
    };
    let endpoints = strom_endpoints(&args.node_id, &flows);

    let hop_status = if status == NodeStatus::Ready {
        match provision(client, strom, &args.southbound_url, &args.node_id, &flows).await {
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

/// Pull desired hops for this node, reconcile them into Strom flows, and report
/// each hop's realised status. Inert when no desired hops are set.
async fn provision(
    client: &Client,
    strom: &StromClient,
    southbound_url: &str,
    node_id: &str,
    flows: &[StromFlow],
) -> Result<Vec<HopStatus>> {
    let desired = fetch_desired(client, southbound_url, node_id).await?;
    let plan = diff_hops(&desired, flows);

    let mut failed = std::collections::HashSet::new();
    for hop in &plan.create {
        if let Err(error) = provision_hop(strom, hop).await {
            tracing::warn!(hop = %hop.id, %error, "provisioning hop failed");
            failed.insert(hop.id.clone());
        }
    }
    for flow_id in &plan.start {
        if let Err(error) = strom.start_flow(flow_id).await {
            tracing::warn!(flow_id = %flow_id, %error, "starting adopted flow failed");
        }
    }
    for flow_id in &plan.delete {
        match strom.delete_flow(flow_id).await {
            Ok(()) => tracing::info!(flow_id = %flow_id, "deleted undesired managed flow"),
            Err(error) => tracing::warn!(flow_id = %flow_id, %error, "deleting flow failed"),
        }
    }

    let refreshed;
    let current: &[StromFlow] = if plan.is_empty() {
        flows
    } else {
        refreshed = strom.list_flows().await.unwrap_or_default();
        &refreshed
    };

    Ok(hop_statuses(strom, &desired, current, &failed).await)
}

async fn hop_statuses(
    strom: &StromClient,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    failed: &std::collections::HashSet<String>,
) -> Vec<HopStatus> {
    let mut statuses = Vec::with_capacity(desired.len());
    for hop in desired {
        let flow = flows.iter().find(|f| f.name == hop.id);
        let (connected, stats) = match flow {
            Some(flow) => match strom.srt_stats(&flow.id).await {
                Ok(value) => {
                    let flow_stats = parse_flow_stats(&value);
                    (flow_stats.connected, Some(LinkStats::from(flow_stats)))
                }
                Err(error) => {
                    tracing::debug!(hop = %hop.id, %error, "srt-stats unavailable");
                    (false, None)
                }
            },
            None => (false, None),
        };

        statuses.push(HopStatus {
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            state: hop_state(flow, connected, failed.contains(&hop.id)),
            resolved_ingress: resolved_addr(&hop.ingress),
            resolved_egress: resolved_addr(&hop.egress),
            stats,
        });
    }
    statuses
}

async fn provision_hop(strom: &StromClient, hop: &DesiredHop) -> Result<()> {
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
