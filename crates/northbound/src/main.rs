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
use weave_core::auth::{self, Guard, Token, require_bearer};
use weave_core::{API_V1, SrtEndpoint, StreamDefinition, StreamTransport};

const DEFAULT_ADDR: &str = "127.0.0.1:9080";
const DEFAULT_CONTROLLER_URL: &str = "http://127.0.0.1:8082";

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    controller_url: String,
    /// Re-presented to the controller on every proxied request. `None` when
    /// authentication is disabled.
    token: Option<Token>,
}

impl AppState {
    fn new(controller_url: String, token: Option<Token>) -> Self {
        Self {
            http: reqwest::Client::new(),
            controller_url,
            token,
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

    let guard = Guard::from_env(auth::NORTHBOUND_TOKEN_VAR)?;
    if guard.is_disabled() {
        tracing::warn!(
            "{}=1: northbound serves and proxies without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }
    let app = router(
        AppState::new(controller_url.clone(), guard.token().cloned()),
        guard,
    );

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding northbound listener on {addr}"))?;
    tracing::info!(%addr, %controller_url, "northbound API listening");

    axum::serve(listener, app)
        .await
        .context("northbound server error")?;
    Ok(())
}

/// The operator contract is served under [`API_V1`]; `/health` stays unversioned
/// and open for compose healthchecks and load balancers. Every stream route
/// requires the northbound bearer token.
fn router(state: AppState, guard: Guard) -> Router {
    let streams = Router::new()
        .route("/streams", get(list_streams).post(submit_stream))
        .route("/streams/{name}", axum::routing::delete(delete_stream))
        .layer(axum::middleware::from_fn_with_state(guard, require_bearer));

    Router::new()
        .route("/health", get(health))
        .nest(API_V1, streams)
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
/// source may not be remote. `via` names transit for a destination only, so a
/// source may not pin it and its entries must be usable node ids.
fn validate_endpoint(endpoint: &SrtEndpoint, is_source: bool) -> Result<(), &'static str> {
    validate_via(endpoint, is_source)?;
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

fn validate_via(endpoint: &SrtEndpoint, is_source: bool) -> Result<(), &'static str> {
    if endpoint.via.is_empty() {
        return Ok(());
    }
    if is_source {
        return Err("via belongs on a destination, not the source");
    }
    if endpoint.via.iter().any(|node| node.trim().is_empty()) {
        return Err("via must not contain an empty node id");
    }
    // A relay appearing twice would place two bridges on one node for the same
    // destination, which is never what a pin means.
    let mut seen = std::collections::HashSet::new();
    if !endpoint.via.iter().all(|node| seen.insert(node.as_str())) {
        return Err("via must not repeat a node");
    }
    Ok(())
}

/// Forward a request to the controller, passing its status and body back
/// faithfully. `path` is contract-relative: [`API_V1`] is applied here, so the
/// handlers name the same paths this service serves.
async fn proxy(
    state: &AppState,
    method: reqwest::Method,
    path: &str,
    body: Option<Bytes>,
) -> Response {
    let url = format!(
        "{}{API_V1}{path}",
        state.controller_url.trim_end_matches('/')
    );
    let mut request = state.http.request(method, &url);
    if let Some(token) = &state.token {
        request = request.header(reqwest::header::AUTHORIZATION, token.header_value());
    }
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

    const TOKEN: &str = "northbound-test-token";

    #[derive(Clone, Default)]
    struct Captured {
        method: String,
        path: String,
        body: Vec<u8>,
        authorization: Option<String>,
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
            let authorization = request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let body = request
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec();
            *captured.lock().unwrap() = Some(Captured {
                method,
                path,
                body,
                authorization,
            });
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

    fn token() -> Token {
        Token::new(TOKEN).unwrap()
    }

    /// A router with authentication switched off, for the proxy-fidelity tests.
    fn open_app(controller_url: String) -> Router {
        router(AppState::new(controller_url, None), Guard::Disabled)
    }

    /// A router requiring [`TOKEN`], which it also re-presents to the controller.
    fn guarded_app(controller_url: String) -> Router {
        router(
            AppState::new(controller_url, Some(token())),
            Guard::Required(token()),
        )
    }

    fn node_ref(id: &str) -> SrtEndpoint {
        SrtEndpoint {
            node: Some(id.to_string()),
            remote: None,
            via: Vec::new(),
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
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/streams")
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
        assert_eq!(seen.path, "/v1/streams");
        let forwarded: StreamDefinition = serde_json::from_slice(&seen.body).unwrap();
        assert_eq!(forwarded, sample_stream());
    }

    #[tokio::test]
    async fn delete_forwards_method_and_path() {
        let (url, captured) = stub_controller(StatusCode::NO_CONTENT).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/streams/basic")
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
        assert_eq!(seen.path, "/v1/streams/basic");
    }

    #[tokio::test]
    async fn get_passes_controller_status_through() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/streams")
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
        let app = open_app(url);

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
                    .uri("/v1/streams")
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

    #[test]
    fn via_is_accepted_on_a_destination_and_refused_on_a_source() {
        let mut dest = node_ref("strom-node-2");
        dest.via = vec!["edge-relay".to_string()];
        assert_eq!(validate_endpoint(&dest, false), Ok(()));
        assert_eq!(
            validate_endpoint(&dest, true),
            Err("via belongs on a destination, not the source")
        );
    }

    #[test]
    fn via_rejects_blank_and_repeated_entries() {
        let mut blank = node_ref("strom-node-2");
        blank.via = vec!["  ".to_string()];
        assert_eq!(
            validate_endpoint(&blank, false),
            Err("via must not contain an empty node id")
        );

        let mut repeated = node_ref("strom-node-2");
        repeated.via = vec!["edge-relay".to_string(), "edge-relay".to_string()];
        assert_eq!(
            validate_endpoint(&repeated, false),
            Err("via must not repeat a node")
        );
    }

    /// The paths this service served before the `/v1` prefix are gone, and nothing
    /// is forwarded on their behalf.
    #[tokio::test]
    async fn unversioned_stream_paths_are_not_served() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        for (method, uri) in [
            ("GET", "/streams"),
            ("POST", "/streams"),
            ("DELETE", "/streams/basic"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&sample_stream()).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
        }
        assert!(
            captured.lock().unwrap().is_none(),
            "an unversioned path never reaches the controller"
        );
    }

    #[tokio::test]
    async fn health_is_reachable_without_a_token() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let response = guarded_app(url)
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Missing and wrong tokens are both rejected, on read and write, with a
    /// `Bearer` challenge — and nothing reaches the controller.
    #[tokio::test]
    async fn stream_routes_reject_missing_and_wrong_tokens() {
        for header in [None, Some("Bearer wrong-token"), Some("Basic ignored")] {
            let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
            let app = guarded_app(url);

            for (method, uri, body) in [
                ("GET", "/v1/streams", Body::empty()),
                (
                    "POST",
                    "/v1/streams",
                    Body::from(serde_json::to_vec(&sample_stream()).unwrap()),
                ),
                ("DELETE", "/v1/streams/basic", Body::empty()),
            ] {
                let mut request = Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json");
                if let Some(header) = header {
                    request = request.header("authorization", header);
                }
                let response = app
                    .clone()
                    .oneshot(request.body(body).unwrap())
                    .await
                    .unwrap();

                assert_eq!(
                    response.status(),
                    StatusCode::UNAUTHORIZED,
                    "{method} {uri} with authorization={header:?}"
                );
                assert_eq!(
                    response.headers()[axum::http::header::WWW_AUTHENTICATE],
                    "Bearer"
                );
            }
            assert!(
                captured.lock().unwrap().is_none(),
                "an unauthenticated request never reaches the controller"
            );
        }
    }

    #[tokio::test]
    async fn correct_token_is_accepted_and_re_presented_to_the_controller() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let response = guarded_app(url)
            .oneshot(
                Request::builder()
                    .uri("/v1/streams")
                    .header("authorization", format!("Bearer {TOKEN}"))
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
        assert_eq!(
            seen.authorization.as_deref(),
            Some(format!("Bearer {TOKEN}").as_str()),
            "northbound authenticates its own hop to the controller"
        );
    }
}
