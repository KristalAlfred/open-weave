//! Southbound API — adapter and media-node surface for observed state.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;
use weave_core::{EndpointDescriptor, NodeDescriptor, NodeHeartbeat, NodeRegistration};

const DEFAULT_ADDR: &str = "127.0.0.1:8081";

#[derive(Clone, Default)]
struct AppState {
    nodes: Arc<RwLock<BTreeMap<String, NodeRegistration>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("WEAVE_SOUTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let state = AppState::default();

    let app = Router::new()
        .route("/health", get(health))
        .route("/nodes", get(list_nodes))
        .route("/nodes/register", post(register_node))
        .route("/nodes/{node_id}/heartbeat", post(node_heartbeat))
        .route("/endpoints", get(list_endpoints))
        .with_state(state);

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

async fn list_nodes(State(state): State<AppState>) -> Json<Vec<NodeDescriptor>> {
    let nodes = state.nodes.read().await;
    Json(
        nodes
            .values()
            .map(|registration| registration.node.clone())
            .collect(),
    )
}

async fn list_endpoints(State(state): State<AppState>) -> Json<Vec<EndpointDescriptor>> {
    let nodes = state.nodes.read().await;
    Json(
        nodes
            .values()
            .flat_map(|registration| registration.endpoints.clone())
            .collect(),
    )
}

async fn register_node(
    State(state): State<AppState>,
    Json(registration): Json<NodeRegistration>,
) -> (StatusCode, Json<Value>) {
    let node_id = registration.node.id.clone();
    let endpoint_count = registration.endpoints.len();

    state
        .nodes
        .write()
        .await
        .insert(node_id.clone(), registration);

    tracing::info!(%node_id, endpoint_count, "node registered");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "node_id": node_id })),
    )
}

async fn node_heartbeat(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    Json(heartbeat): Json<NodeHeartbeat>,
) -> (StatusCode, Json<Value>) {
    if node_id != heartbeat.node_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "message": "node id mismatch" })),
        );
    }

    let mut nodes = state.nodes.write().await;
    let Some(registration) = nodes.get_mut(&node_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "status": "error", "message": "unknown node" })),
        );
    };

    registration.node.status = heartbeat.status;
    registration.endpoints = heartbeat.endpoints;

    tracing::debug!(%node_id, status = ?registration.node.status, "node heartbeat");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "node_id": node_id })),
    )
}
