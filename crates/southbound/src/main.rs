//! Southbound API — adapter and media-node surface. Stateless: every request is
//! proxied to the controller, which owns all node and desired state. Adapters
//! keep dialing this service; it simply relays to the controller.

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;

const DEFAULT_ADDR: &str = "127.0.0.1:8081";
const DEFAULT_CONTROLLER_URL: &str = "http://127.0.0.1:8082";

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    controller_url: String,
}

impl AppState {
    fn new(controller_url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            controller_url,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = std::env::var("WEAVE_SOUTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let controller_url = std::env::var("WEAVE_CONTROLLER_URL")
        .unwrap_or_else(|_| DEFAULT_CONTROLLER_URL.to_string());
    let app = router(AppState::new(controller_url.clone()));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding southbound listener on {addr}"))?;
    tracing::info!(%addr, %controller_url, "southbound API listening");

    axum::serve(listener, app)
        .await
        .context("southbound server error")?;
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/nodes", get(list_nodes))
        .route("/nodes/register", post(register_node))
        .route("/nodes/{node_id}/heartbeat", post(node_heartbeat))
        .route("/nodes/{node_id}/desired", get(get_desired))
        .route("/endpoints", get(list_endpoints))
        .route("/state", get(get_state))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_nodes(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, "/nodes", None).await
}

async fn list_endpoints(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, "/endpoints", None).await
}

async fn get_state(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, "/state", None).await
}

async fn register_node(State(state): State<AppState>, body: Bytes) -> Response {
    proxy(&state, reqwest::Method::POST, "/nodes/register", Some(body)).await
}

async fn node_heartbeat(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
    body: Bytes,
) -> Response {
    proxy(
        &state,
        reqwest::Method::POST,
        &format!("/nodes/{node_id}/heartbeat"),
        Some(body),
    )
    .await
}

async fn get_desired(State(state): State<AppState>, Path(node_id): Path<String>) -> Response {
    proxy(
        &state,
        reqwest::Method::GET,
        &format!("/nodes/{node_id}/desired"),
        None,
    )
    .await
}

/// Forward a request to the controller, passing its status and body back faithfully.
async fn proxy(
    state: &AppState,
    method: reqwest::Method,
    path: &str,
    body: Option<Bytes>,
) -> Response {
    let url = format!("{}{path}", state.controller_url.trim_end_matches('/'));
    let mut request = state.http.request(method, &url);
    if let Some(body) = body {
        request = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
    }
    match request.send().await {
        Ok(response) => relay(response).await,
        Err(err) => {
            tracing::warn!(%err, %url, "proxying to controller failed");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "controller unreachable" })),
            )
                .into_response()
        }
    }
}

async fn relay(response: reqwest::Response) -> Response {
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response.bytes().await.unwrap_or_default();
    let mut out = (status, body).into_response();
    if let Some(value) = content_type.and_then(|ct| ct.parse().ok()) {
        out.headers_mut()
            .insert(reqwest::header::CONTENT_TYPE, value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::State as AxState;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    #[derive(Clone, Default)]
    struct Captured {
        method: String,
        path: String,
        body: Vec<u8>,
    }

    async fn stub_controller(status: StatusCode) -> (String, Arc<Mutex<Option<Captured>>>) {
        let captured: Arc<Mutex<Option<Captured>>> = Arc::new(Mutex::new(None));

        async fn record(
            AxState((captured, status)): AxState<(Arc<Mutex<Option<Captured>>>, StatusCode)>,
            request: Request<Body>,
        ) -> Response {
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let body = request
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec();
            *captured.lock().unwrap() = Some(Captured { method, path, body });
            (status, Json(json!({ "ok": true }))).into_response()
        }

        let app = Router::new()
            .fallback(record)
            .with_state((captured.clone(), status));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), captured)
    }

    #[tokio::test]
    async fn register_forwards_post_body_and_status() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = router(AppState::new(url));

        let payload = json!({ "node": { "id": "strom-node-1" } });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/nodes/register")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("controller saw a request");
        assert_eq!(seen.method, "POST");
        assert_eq!(seen.path, "/nodes/register");
        let forwarded: Value = serde_json::from_slice(&seen.body).unwrap();
        assert_eq!(forwarded, payload);
    }

    #[tokio::test]
    async fn heartbeat_forwards_node_scoped_path() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = router(AppState::new(url));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/nodes/strom-node-1/heartbeat")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "node_id": "strom-node-1" }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("controller saw a request");
        assert_eq!(seen.method, "POST");
        assert_eq!(seen.path, "/nodes/strom-node-1/heartbeat");
    }

    #[tokio::test]
    async fn get_desired_forwards_and_relays_status() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = router(AppState::new(url));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/nodes/strom-node-1/desired")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("controller saw a request");
        assert_eq!(seen.method, "GET");
        assert_eq!(seen.path, "/nodes/strom-node-1/desired");
    }

    #[tokio::test]
    async fn unreachable_controller_is_bad_gateway() {
        let app = router(AppState::new("http://127.0.0.1:1".to_string()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }
}
