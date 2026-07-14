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
use weave_core::{SrtEndpoint, StreamDefinition, StreamTransport};

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
    if let Err(message) = validate_endpoint(source, true) {
        return error(StatusCode::BAD_REQUEST, message);
    }
    for dest in &stream.destinations {
        let StreamTransport::Srt(dest) = dest;
        if let Err(message) = validate_endpoint(dest, false) {
            return error(StatusCode::BAD_REQUEST, message);
        }
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

/// Enforce node-XOR-remote on an endpoint: exactly one of `node`/`remote` must be
/// set, a present node must be non-empty, a present remote must name a host, and a
/// source may not be remote.
fn validate_endpoint(endpoint: &SrtEndpoint, is_source: bool) -> Result<(), &'static str> {
    match (&endpoint.node, &endpoint.remote) {
        (Some(_), Some(_)) => Err("endpoint must set either node or remote, not both"),
        (None, None) => Err("endpoint must set either node or remote"),
        (Some(node), None) if node.trim().is_empty() => Err("node must not be empty"),
        (Some(_), None) => Ok(()),
        (None, Some(_)) if is_source => Err("source must be a node, not a remote endpoint"),
        (None, Some(remote)) if remote.host.trim().is_empty() => {
            Err("remote host must not be empty")
        }
        (None, Some(_)) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use weave_core::RemoteAddr;

    fn node_ref(id: &str) -> SrtEndpoint {
        SrtEndpoint {
            node: Some(id.to_string()),
            remote: None,
            network: None,
            latency: None,
        }
    }

    fn sample_stream() -> StreamDefinition {
        StreamDefinition {
            name: "cam1-to-studio".to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                latency: Some(200),
                ..node_ref("strom-node-1")
            }),
            destinations: vec![StreamTransport::Srt(node_ref("strom-node-2"))],
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
    async fn source_with_neither_node_nor_remote_is_rejected() {
        // node is optional at the serde layer now; the missing-placement rule is
        // enforced by validation, so this is a 400, not a serde 422.
        let response = post_raw(json!({
            "name": "cam1-to-studio",
            "source": { "srt": { "network": "wan" } },
            "destinations": [{ "srt": { "node": "strom-node-2" } }]
        }))
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn destination_with_neither_node_nor_remote_is_rejected() {
        let response = post_raw(json!({
            "name": "cam1-to-studio",
            "source": { "srt": { "node": "strom-node-1" } },
            "destinations": [{ "srt": { "network": "wan" } }]
        }))
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn endpoint_with_both_node_and_remote_is_rejected() {
        let mut stream = sample_stream();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.remote = Some(RemoteAddr {
            host: "198.51.100.5".to_string(),
            port: 9000,
        });

        let response = post_stream(&stream).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap()
                .contains("not both")
        );
    }

    #[tokio::test]
    async fn remote_source_is_rejected() {
        let mut stream = sample_stream();
        stream.source = StreamTransport::Srt(SrtEndpoint {
            node: None,
            remote: Some(RemoteAddr {
                host: "198.51.100.5".to_string(),
                port: 9000,
            }),
            network: None,
            latency: None,
        });

        let response = post_stream(&stream).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap()
                .contains("source must be a node")
        );
    }

    #[tokio::test]
    async fn remote_destination_is_accepted() {
        let mut stream = sample_stream();
        stream.destinations = vec![StreamTransport::Srt(SrtEndpoint {
            node: None,
            remote: Some(RemoteAddr {
                host: "198.51.100.5".to_string(),
                port: 9000,
            }),
            network: None,
            latency: None,
        })];

        let response = post_stream(&stream).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
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
        source.node = Some("  ".to_string());

        let response = post_stream(&stream).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap()
                .contains("node must not be empty")
        );
    }

    #[tokio::test]
    async fn blank_destination_node_is_rejected() {
        let mut stream = sample_stream();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.node = Some(String::new());

        let response = post_stream(&stream).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap()
                .contains("node must not be empty")
        );
    }
}
