//! Northbound API — desired-state surface for operators and systems.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;
use weave_core::{StreamDefinition, StreamTransport};

const DEFAULT_ADDR: &str = "127.0.0.1:9080";

#[derive(Clone, Default)]
struct AppState {
    streams: Arc<RwLock<BTreeMap<String, StreamDefinition>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("WEAVE_NORTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let app = router(AppState::default());

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding northbound listener on {addr}"))?;
    tracing::info!(%addr, "northbound API listening");

    axum::serve(listener, app)
        .await
        .context("northbound server error")?;
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/streams", get(list_streams).post(submit_stream))
        .route("/streams/{name}", axum::routing::delete(delete_stream))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_streams(State(state): State<AppState>) -> Json<Vec<StreamDefinition>> {
    let streams = state.streams.read().await;
    Json(streams.values().cloned().collect())
}

async fn submit_stream(
    State(state): State<AppState>,
    Json(stream): Json<StreamDefinition>,
) -> Response {
    if stream.name.trim().is_empty() {
        return error(StatusCode::BAD_REQUEST, "stream name must not be empty");
    }
    if stream.destinations.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "stream must have at least one destination",
        );
    }
    let StreamTransport::Srt(source) = &stream.source;
    if source.node.trim().is_empty() {
        return error(StatusCode::BAD_REQUEST, "source node must not be empty");
    }
    if stream.destinations.iter().any(|dest| {
        let StreamTransport::Srt(dest) = dest;
        dest.node.trim().is_empty()
    }) {
        return error(
            StatusCode::BAD_REQUEST,
            "each destination node must not be empty",
        );
    }

    let name = stream.name.clone();
    state.streams.write().await.insert(name.clone(), stream);

    tracing::info!(%name, "stream accepted");
    (
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "name": name })),
    )
        .into_response()
}

async fn delete_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if state.streams.write().await.remove(&name).is_some() {
        tracing::info!(%name, "stream deleted");
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(StatusCode::NOT_FOUND, "stream not found")
    }
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use weave_core::SrtEndpoint;

    fn sample_stream() -> StreamDefinition {
        StreamDefinition {
            name: "cam1-to-studio".to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: "strom-node-1".to_string(),
                network: None,
                latency: Some(200),
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                node: "strom-node-2".to_string(),
                network: None,
                latency: None,
            })],
        }
    }

    async fn body_json(response: Response) -> Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("json body")
    }

    #[tokio::test]
    async fn post_then_get_returns_stored_stream() {
        let app = router(AppState::default());
        let stream = sample_stream();

        let post = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&stream).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(post.status(), StatusCode::ACCEPTED);
        assert_eq!(body_json(post).await["name"], "cam1-to-studio");

        let get = app
            .oneshot(
                Request::builder()
                    .uri("/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::OK);

        let listed: Vec<StreamDefinition> =
            serde_json::from_value(body_json(get).await).expect("stream list");
        assert_eq!(listed, vec![stream]);
    }

    #[tokio::test]
    async fn post_then_delete_removes_stream() {
        let app = router(AppState::default());
        let stream = sample_stream();

        let post = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&stream).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(post.status(), StatusCode::ACCEPTED);

        let delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/streams/cam1-to-studio")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);

        let get = app
            .oneshot(
                Request::builder()
                    .uri("/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let listed: Vec<StreamDefinition> =
            serde_json::from_value(body_json(get).await).expect("stream list");
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn delete_unknown_stream_is_not_found() {
        let app = router(AppState::default());
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/streams/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn post_empty_destinations_is_rejected() {
        let app = router(AppState::default());
        let mut stream = sample_stream();
        stream.destinations.clear();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&stream).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap()
                .contains("destination")
        );
    }

    async fn post_stream(stream: &StreamDefinition) -> Response {
        post_raw(serde_json::to_value(stream).unwrap()).await
    }

    async fn post_raw(body: Value) -> Response {
        router(AppState::default())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn source_without_node_field_is_rejected_by_serde() {
        let response = post_raw(json!({
            "name": "cam1-to-studio",
            "source": { "srt": { "network": "wan" } },
            "destinations": [{ "srt": { "node": "strom-node-2" } }]
        }))
        .await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn destination_without_node_field_is_rejected_by_serde() {
        let response = post_raw(json!({
            "name": "cam1-to-studio",
            "source": { "srt": { "node": "strom-node-1" } },
            "destinations": [{ "srt": { "network": "wan" } }]
        }))
        .await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn source_with_node_is_accepted() {
        let response = post_stream(&sample_stream()).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn blank_source_node_is_rejected() {
        let mut stream = sample_stream();
        let StreamTransport::Srt(source) = &mut stream.source;
        source.node = "  ".to_string();

        let response = post_stream(&stream).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap()
                .contains("source node must not be empty")
        );
    }

    #[tokio::test]
    async fn blank_destination_node_is_rejected() {
        let mut stream = sample_stream();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.node = String::new();

        let response = post_stream(&stream).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap()
                .contains("destination node must not be empty")
        );
    }
}
