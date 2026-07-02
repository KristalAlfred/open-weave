//! Northbound API — accepts desired-state definitions from operators and systems.
//!
//! Phase 0: a stateless HTTP surface that acknowledges submissions. Reconciliation
//! and persistence are not yet implemented.

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::Definition;

const DEFAULT_ADDR: &str = "127.0.0.1:8080";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("WEAVE_NORTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());

    let app = Router::new()
        .route("/health", get(health))
        .route("/definitions", post(submit_definition));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding northbound listener on {addr}"))?;
    tracing::info!(%addr, "northbound API listening");

    axum::serve(listener, app)
        .await
        .context("northbound server error")?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn submit_definition(Json(definition): Json<Definition>) -> (StatusCode, Json<Value>) {
    tracing::info!(id = %definition.id, name = %definition.name, "definition accepted");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "id": definition.id })),
    )
}
