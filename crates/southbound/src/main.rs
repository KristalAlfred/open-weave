//! Southbound API — the media-node-facing side of the control plane.
//!
//! Phase 0 is a stateless HTTP transport. A persistent-connection transport
//! (gRPC streaming or WebSocket) for real-time reconciliation and telemetry may be
//! added later; it lives in this binary and does not affect the northbound binary.

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::NodeDescriptor;

const DEFAULT_ADDR: &str = "127.0.0.1:8081";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("WEAVE_SOUTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());

    let app = Router::new()
        .route("/health", get(health))
        .route("/nodes", get(list_nodes));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding southbound listener on {addr}"))?;
    tracing::info!(%addr, "southbound API listening");

    axum::serve(listener, app)
        .await
        .context("southbound server error")?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_nodes() -> Json<Vec<NodeDescriptor>> {
    Json(Vec::new())
}
