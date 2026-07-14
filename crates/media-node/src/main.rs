//! `weave-media-node` — managed edge agent for unmanaged media endpoints.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use clap::Parser;
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::{
    AdapterDescriptor, AdapterKind, NodeCapabilities, NodeConfig, NodeDescriptor, NodeRegistration,
    NodeStatus, TransportDescriptor,
};

#[derive(Debug, Parser)]
#[command(
    name = "weave-media-node",
    version,
    about = "open-weave managed edge node"
)]
struct Args {
    /// Path to the node config file (YAML).
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
    let config = load_config(&args.config)?;
    let registration = registration(&config);

    register(&config.southbound_url, &registration).await?;
    serve_health(config.listen).await
}

fn load_config(path: &Path) -> Result<NodeConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config {}", path.display()))?;
    let config: NodeConfig = serde_norway::from_str(&text)
        .with_context(|| format!("parsing config {}", path.display()))?;
    config
        .validate()
        .with_context(|| format!("validating config {}", path.display()))?;
    Ok(config)
}

fn registration(config: &NodeConfig) -> NodeRegistration {
    NodeRegistration {
        node: NodeDescriptor {
            id: config.id.clone(),
            endpoint: config.public_endpoint(),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                adapters: vec![AdapterDescriptor {
                    name: "media-node".to_string(),
                    kind: AdapterKind::MediaNode,
                }],
                transports: config
                    .transports
                    .iter()
                    .map(|name| TransportDescriptor { name: name.clone() })
                    .collect(),
                data_plane: config.data_plane.clone(),
                port_range: Some(config.port_range),
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
