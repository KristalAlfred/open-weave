//! `weave-media-node` — managed edge agent for unmanaged media endpoints.

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use clap::Parser;
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::{
    AdapterDescriptor, AdapterKind, NodeCapabilities, NodeDescriptor, NodeRegistration, NodeStatus,
    TransportDescriptor,
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
    let registration = registration(&args.node_id, &public_endpoint, &args.transports);

    register(&args.southbound_url, &registration).await?;
    serve_health(args.listen).await
}

fn registration(node_id: &str, endpoint: &str, transports: &[String]) -> NodeRegistration {
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
            },
        },
        endpoints: Vec::new(),
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
