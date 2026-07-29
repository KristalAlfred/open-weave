//! `weave-media-node` — managed edge agent for unmanaged media endpoints.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use clap::Parser;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::auth::{self, Token};
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
    let config = load_config(&args.config)?.node;
    let registration = registration(&config);

    // Fail closed, like the Strom adapter: a node with no token can never
    // register, so surface that at startup rather than as a 401.
    let token = config.resolve_southbound_token()?;
    if token.is_none() {
        tracing::warn!(
            "{}=1: calling southbound without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }

    register(&config.southbound_url, token.as_ref(), &registration).await?;
    serve_health(config.listen).await
}

/// Node-config file wrapper. Accepts an extra `strom:` section (present in
/// adapter configs) without failing so both share one file shape.
#[derive(Debug, Deserialize)]
struct MediaNodeConfig {
    node: NodeConfig,
}

fn load_config(path: &Path) -> Result<MediaNodeConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config {}", path.display()))?;
    let config: MediaNodeConfig = serde_norway::from_str(&text)
        .with_context(|| format!("parsing config {}", path.display()))?;
    config
        .node
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

async fn register(
    southbound_url: &str,
    token: Option<&Token>,
    registration: &NodeRegistration,
) -> Result<()> {
    let mut request = reqwest::Client::new().post(format!("{southbound_url}/nodes/register"));
    if let Some(token) = token {
        request = request.header(reqwest::header::AUTHORIZATION, token.header_value());
    }
    request
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_wrapped_config_with_extra_strom_section() {
        let dir = std::env::temp_dir();
        let path = dir.join("weave-media-node-adapter-config.yaml");
        std::fs::write(
            &path,
            "node:\n  id: strom-node-1\n  southbound_url: http://10.0.0.1:8081\n  listen: 0.0.0.0:8091\n  public_endpoint: http://10.0.0.2:8091\n  data_plane:\n    default: 10.0.0.2\n  port_range:\n    start: 20000\n    end: 20999\n  transports: [srt]\nstrom:\n  url: http://10.0.0.2:8080\n  poll_interval_secs: 5\n",
        )
        .unwrap();

        let config = load_config(&path).expect("adapter-shaped config should parse");

        assert_eq!(config.node.id, "strom-node-1");
        assert_eq!(config.node.public_endpoint(), "http://10.0.0.2:8091");

        std::fs::remove_file(&path).ok();
    }
}
