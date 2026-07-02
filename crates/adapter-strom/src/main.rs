//! `weave-adapter-strom` — southbound adapter for Strom instances.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail};
use axum::{Json, Router, routing::get};
use clap::Parser;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::{
    AdapterDescriptor, AdapterKind, EndpointDescriptor, EndpointKind, NodeCapabilities,
    NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus, TransportDescriptor,
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
    #[arg(
        long,
        env = "WEAVE_SOUTHBOUND_URL",
        default_value = "http://127.0.0.1:8081"
    )]
    southbound_url: String,
    #[arg(long, env = "WEAVE_STROM_URL", default_value = "http://127.0.0.1:8080")]
    strom_url: String,
    #[arg(long, env = "WEAVE_STROM_API_KEY")]
    strom_api_key: Option<String>,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "srt,webrtc,aes67,ndi,decklink"
    )]
    transports: Vec<String>,
    #[arg(long, env = "WEAVE_STROM_POLL_INTERVAL_SECS", default_value_t = 5)]
    poll_interval_secs: u64,
}

#[derive(Debug, Deserialize)]
struct FlowListResponse {
    #[serde(default)]
    flows: Vec<StromFlow>,
}

#[derive(Debug, Deserialize)]
struct StromFlow {
    id: String,
    name: String,
    #[serde(default)]
    running: bool,
    #[serde(default)]
    elements: Vec<StromElement>,
    #[serde(default)]
    blocks: Vec<StromBlock>,
}

#[derive(Debug, Deserialize)]
struct StromElement {
    #[serde(default)]
    element_type: String,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct StromBlock {
    #[serde(default)]
    block_definition_id: String,
    #[serde(default)]
    properties: BTreeMap<String, Value>,
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
        result = sync_loop(&client, &args, &public_endpoint) => {
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

async fn sync_loop(client: &Client, args: &Args, public_endpoint: &str) -> Result<()> {
    let mut registered = false;
    let interval = Duration::from_secs(args.poll_interval_secs);

    loop {
        match sync_once(client, args, public_endpoint, registered).await {
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
    args: &Args,
    public_endpoint: &str,
    registered: bool,
) -> Result<bool> {
    let (status, endpoints) = match fetch_flows(client, args).await {
        Ok(flows) => (NodeStatus::Ready, strom_endpoints(&args.node_id, &flows)),
        Err(error) => {
            tracing::warn!(%error, "Strom observation failed");
            (NodeStatus::Degraded, Vec::new())
        }
    };

    let registration = registration(args, public_endpoint, status, endpoints.clone());

    if !registered {
        register_node(client, &args.southbound_url, &registration).await?;
        return Ok(true);
    }

    let heartbeat = NodeHeartbeat {
        node_id: args.node_id.clone(),
        status,
        endpoints,
    };

    if heartbeat_node(client, &args.southbound_url, &heartbeat).await? == StatusCode::NOT_FOUND {
        register_node(client, &args.southbound_url, &registration).await?;
    }

    Ok(true)
}

fn registration(
    args: &Args,
    public_endpoint: &str,
    status: NodeStatus,
    endpoints: Vec<EndpointDescriptor>,
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
    }
}

async fn fetch_flows(client: &Client, args: &Args) -> Result<Vec<StromFlow>> {
    let response = strom_get(client, args, "/api/flows")
        .send()
        .await
        .context("fetching Strom flows")?
        .error_for_status()
        .context("Strom flow list request failed")?;

    let flows = response
        .json::<FlowListResponse>()
        .await
        .context("decoding Strom flows")?;

    Ok(flows.flows)
}

fn strom_get(client: &Client, args: &Args, path: &str) -> reqwest::RequestBuilder {
    let request = client.get(join_url(&args.strom_url, path));
    if let Some(api_key) = &args.strom_api_key {
        request.bearer_auth(api_key)
    } else {
        request
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
