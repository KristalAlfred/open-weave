//! `weave-media-node` — managed edge agent for unmanaged media endpoints.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use clap::Parser;
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::{
    AdapterDescriptor, AdapterKind, NodeCapabilities, NodeDescriptor, NodeRegistration, NodeStatus,
    PortRange, TransportDescriptor,
};

#[derive(Debug, Parser)]
#[command(
    name = "weave-media-node",
    version,
    about = "open-weave managed edge node"
)]
struct Args {
    #[arg(long, env = "WEAVE_NODE_ID", default_value = "local-media-node")]
    node_id: String,
    #[arg(long, env = "WEAVE_NODE_ADDR", default_value = "127.0.0.1:8090")]
    listen: String,
    #[arg(
        long,
        env = "WEAVE_SOUTHBOUND_URL",
        default_value = "http://127.0.0.1:8081"
    )]
    southbound_url: String,
    #[arg(long, env = "WEAVE_NODE_PUBLIC_ENDPOINT")]
    public_endpoint: Option<String>,
    #[arg(long, value_delimiter = ',', default_value = "srt,rist,webrtc,st2110")]
    transports: Vec<String>,
    /// Data-plane addresses advertised for placement, as `alias=host` pairs
    /// (e.g. `default=10.0.0.5,wan=203.0.113.7`).
    #[arg(long, env = "WEAVE_DATA_PLANE")]
    data_plane: Option<String>,
    /// Inclusive port range the controller may assign from, as `start-end`.
    #[arg(long, env = "WEAVE_PORT_RANGE")]
    port_range: Option<String>,
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
    let registration = registration(
        &args.node_id,
        &public_endpoint,
        &args.transports,
        data_plane,
        port_range,
    );

    register(&args.southbound_url, &registration).await?;
    serve_health(args.listen).await
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

fn registration(
    node_id: &str,
    endpoint: &str,
    transports: &[String],
    data_plane: BTreeMap<String, String>,
    port_range: Option<PortRange>,
) -> NodeRegistration {
    NodeRegistration {
        node: NodeDescriptor {
            id: node_id.to_string(),
            endpoint: endpoint.to_string(),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                adapters: vec![AdapterDescriptor {
                    name: "media-node".to_string(),
                    kind: AdapterKind::MediaNode,
                }],
                transports: transports
                    .iter()
                    .map(|name| TransportDescriptor { name: name.clone() })
                    .collect(),
                data_plane,
                port_range,
            },
        },
        endpoints: Vec::new(),
        hop_status: Vec::new(),
    }
}

async fn register(southbound_url: &str, registration: &NodeRegistration) -> Result<()> {
    let client = reqwest::Client::new();
    client
        .post(format!("{southbound_url}/nodes/register"))
        .json(registration)
        .send()
        .await
        .context("registering media node")?
        .error_for_status()
        .context("southbound node registration failed")?;

    tracing::info!(node_id = %registration.node.id, southbound_url, "media node registered");
    Ok(())
}

async fn serve_health(addr: String) -> Result<()> {
    let app = Router::new().route("/health", get(health));
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding media-node listener on {addr}"))?;
    tracing::info!(%addr, "media-node health API listening");

    axum::serve(listener, app)
        .await
        .context("media-node server error")?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
