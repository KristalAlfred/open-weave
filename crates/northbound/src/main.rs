//! Northbound API — desired-state surface for operators and systems.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;
use weave_core::Definition;

const DEFAULT_ADDR: &str = "127.0.0.1:8080";

#[derive(Clone, Default)]
struct AppState {
    definitions: Arc<RwLock<BTreeMap<String, Definition>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("WEAVE_NORTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let state = AppState::default();

    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/definitions",
            get(list_definitions).post(submit_definition),
        )
        .with_state(state);

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

async fn list_definitions(State(state): State<AppState>) -> Json<Vec<Definition>> {
    let definitions = state.definitions.read().await;
    Json(definitions.values().cloned().collect())
}

async fn submit_definition(
    State(state): State<AppState>,
    Json(definition): Json<Definition>,
) -> (StatusCode, Json<Value>) {
    let id = definition.id.clone();
    let name = definition.name.clone();

    state
        .definitions
        .write()
        .await
        .insert(id.clone(), definition);

    tracing::info!(%id, %name, "definition accepted");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "id": id })),
    )
}
