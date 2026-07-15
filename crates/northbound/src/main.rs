//! Northbound API — desired-state surface for operators and systems. Stateless:
//! it validates stream submissions at the boundary and proxies every request to
//! the controller, which owns all state.

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::{SrtEndpoint, StreamDefinition, StreamTransport};

const DEFAULT_ADDR: &str = "127.0.0.1:9080";
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

    let addr = std::env::var("WEAVE_NORTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let controller_url = std::env::var("WEAVE_CONTROLLER_URL")
        .unwrap_or_else(|_| DEFAULT_CONTROLLER_URL.to_string());
    let app = router(AppState::new(controller_url.clone()));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding northbound listener on {addr}"))?;
    tracing::info!(%addr, %controller_url, "northbound API listening");

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

async fn list_streams(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, "/streams", None).await
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

    let body = match serde_json::to_vec(&stream) {
        Ok(body) => body,
        Err(err) => {
            tracing::error!(%err, "serializing validated stream failed");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "failed to encode stream");
        }
    };
    proxy(&state, reqwest::Method::POST, "/streams", Some(body.into())).await
}

async fn delete_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    proxy(
        &state,
        reqwest::Method::DELETE,
        &format!("/streams/{name}"),
        None,
    )
    .await
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
            error(StatusCode::BAD_GATEWAY, "controller unreachable")
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
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;
    use weave_core::RemoteAddr;

    #[derive(Clone, Default)]
    struct Captured {
        method: String,
        path: String,
        body: Vec<u8>,
    }

    /// A stub controller listening on a real socket. Records the last request and
    /// returns a canned status + JSON body so proxy fidelity can be asserted.
    async fn stub_controller(status: StatusCode) -> (String, Arc<Mutex<Option<Captured>>>) {
        use axum::extract::State as AxState;
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
            source: StreamTransport::Srt(node_ref("strom-node-1")),
            destinations: vec![StreamTransport::Srt(node_ref("strom-node-2"))],
        }
    }

    async fn body_json(response: Response) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn post_forwards_body_and_passes_status_through() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = router(AppState::new(url));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&sample_stream()).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(body_json(response).await["ok"], true);

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("controller saw a request");
        assert_eq!(seen.method, "POST");
        assert_eq!(seen.path, "/streams");
        let forwarded: StreamDefinition = serde_json::from_slice(&seen.body).unwrap();
        assert_eq!(forwarded, sample_stream());
    }

    #[tokio::test]
    async fn delete_forwards_method_and_path() {
        let (url, captured) = stub_controller(StatusCode::NO_CONTENT).await;
        let app = router(AppState::new(url));

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/streams/basic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("controller saw a request");
        assert_eq!(seen.method, "DELETE");
        assert_eq!(seen.path, "/streams/basic");
    }

    #[tokio::test]
    async fn get_passes_controller_status_through() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = router(AppState::new(url));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn invalid_stream_is_rejected_before_proxying() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = router(AppState::new(url));

        let mut stream = sample_stream();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.remote = Some(RemoteAddr {
            host: "198.51.100.5".to_string(),
            port: 9000,
        });

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
            captured.lock().unwrap().is_none(),
            "invalid stream never reaches the controller"
        );
    }
}
