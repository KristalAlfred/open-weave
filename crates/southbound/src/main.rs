//! Southbound API — the adapter-facing surface. Stateless: every request is
//! proxied to the controller, which owns all node and desired state. Adapters
//! keep dialing this service; it simply relays to the controller.

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing_subscriber::EnvFilter;
use weave_core::API_V1;
use weave_core::auth::{self, Guard, Token, require_bearer};

const DEFAULT_ADDR: &str = "127.0.0.1:8081";
const DEFAULT_CONTROLLER_URL: &str = "http://127.0.0.1:8082";

/// Origin a browser-hosted node may call the adapter contract from. Unset means
/// no CORS headers at all, which is right for every adapter that is not a web
/// page. `*` allows any origin, for development.
const CORS_ORIGIN_VAR: &str = "WEAVE_SOUTHBOUND_CORS_ORIGIN";

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

    let addr = std::env::var("WEAVE_SOUTHBOUND_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let controller_url = std::env::var("WEAVE_CONTROLLER_URL")
        .unwrap_or_else(|_| DEFAULT_CONTROLLER_URL.to_string());

    let guard = Guard::from_env(auth::SOUTHBOUND_TOKEN_VAR)?;
    if guard.is_disabled() {
        tracing::warn!(
            "{}=1: southbound serves and proxies without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }
    let cors = match std::env::var(CORS_ORIGIN_VAR) {
        Ok(origin) if !origin.trim().is_empty() => {
            tracing::info!(origin = %origin, "allowing browser nodes from this origin");
            Some(cors_layer(origin.trim())?)
        }
        _ => None,
    };
    let app = router(
        AppState::new(controller_url.clone(), guard.token().cloned()),
        guard,
        cors,
    );

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding southbound listener on {addr}"))?;
    tracing::info!(%addr, %controller_url, "southbound API listening");

    axum::serve(listener, app)
        .await
        .context("southbound server error")?;
    Ok(())
}

/// The adapter contract is served under [`API_V1`] — third-party adapters bind to
/// it, so it is the surface that must stay stable within a version. `/health`
/// stays unversioned and open for compose healthchecks and load balancers.
/// Everything else — registration, heartbeats, and the desired-state and topology
/// reads — requires the southbound bearer token.
///
/// A browser-hosted node calls this contract from a web page, so the `/v1`
/// routes optionally carry CORS headers. The layer sits outside the bearer
/// check: a preflight carries no `Authorization` header and must be answered
/// before it, not refused by it.
fn router(state: AppState, guard: Guard, cors: Option<CorsLayer>) -> Router {
    let mut nodes = Router::new()
        .route("/nodes", get(list_nodes))
        .route("/nodes/register", post(register_node))
        .route("/nodes/{node_id}/heartbeat", post(node_heartbeat))
        .route("/nodes/{node_id}/desired", get(get_desired))
        .route("/endpoints", get(list_endpoints))
        .route("/state", get(get_state))
        .layer(axum::middleware::from_fn_with_state(guard, require_bearer));
    if let Some(cors) = cors {
        nodes = nodes.layer(cors);
    }

    Router::new()
        .route("/health", get(health))
        .nest(API_V1, nodes)
        .with_state(state)
}

/// CORS for the adapter contract: `origin` is an exact origin or `*`. The page
/// sends `Authorization` and `Content-Type`, so the preflight must allow both.
fn cors_layer(origin: &str) -> Result<CorsLayer> {
    let allow_origin = if origin == "*" {
        AllowOrigin::any()
    } else {
        AllowOrigin::exact(
            HeaderValue::from_str(origin)
                .with_context(|| format!("{CORS_ORIGIN_VAR} is not a valid origin: {origin}"))?,
        )
    };
    Ok(CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]))
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

    const TOKEN: &str = "southbound-test-token";

    #[derive(Clone, Default)]
    struct Captured {
        method: String,
        path: String,
        body: Vec<u8>,
        authorization: Option<String>,
    }

    async fn stub_controller(status: StatusCode) -> (String, Arc<Mutex<Option<Captured>>>) {
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
        router(AppState::new(controller_url, None), Guard::Disabled, None)
    }

    /// A router requiring [`TOKEN`], which it also re-presents to the controller.
    fn guarded_app(controller_url: String) -> Router {
        router(
            AppState::new(controller_url, Some(token())),
            Guard::Required(token()),
            None,
        )
    }

    /// [`guarded_app`] that also admits a browser page served from `origin`.
    fn cors_app(controller_url: String, origin: &str) -> Router {
        router(
            AppState::new(controller_url, Some(token())),
            Guard::Required(token()),
            Some(cors_layer(origin).unwrap()),
        )
    }

    const PAGE: &str = "http://172.25.0.40:8000";

    /// A browser's preflight for an authenticated `POST` carries no bearer token;
    /// it must be answered with the allowed headers, not refused with `401`, and
    /// it never reaches the controller.
    #[tokio::test]
    async fn preflight_is_answered_before_the_bearer_check() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = cors_app(url, PAGE);

        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/v1/nodes/register")
                    .header("origin", PAGE)
                    .header("access-control-request-method", "POST")
                    .header(
                        "access-control-request-headers",
                        "authorization, content-type",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        assert_eq!(headers["access-control-allow-origin"], PAGE);
        let allowed = headers["access-control-allow-headers"]
            .to_str()
            .unwrap()
            .to_ascii_lowercase();
        assert!(allowed.contains("authorization"), "{allowed}");
        assert!(allowed.contains("content-type"), "{allowed}");
        assert!(
            headers["access-control-allow-methods"]
                .to_str()
                .unwrap()
                .contains("POST")
        );
        assert!(captured.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn cors_headers_accompany_an_authenticated_response() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = cors_app(url, PAGE);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/browser-a1b2/desired")
                    .header("origin", PAGE)
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["access-control-allow-origin"], PAGE);
    }

    #[tokio::test]
    async fn a_foreign_origin_is_not_allowed() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = cors_app(url, PAGE);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/browser-a1b2/desired")
                    .header("origin", "http://evil.example")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.headers()["access-control-allow-origin"],
            PAGE,
            "the allowed origin is stated, never the requester's, so the browser refuses"
        );
    }

    #[tokio::test]
    async fn wildcard_origin_allows_any_page() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = cors_app(url, "*");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes")
                    .header("origin", "http://localhost:3000")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.headers()["access-control-allow-origin"], "*");
    }

    /// Without the variable the adapter contract carries no CORS headers at all,
    /// and a preflight is just an unauthenticated request.
    #[tokio::test]
    async fn without_a_configured_origin_there_is_no_cors() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = guarded_app(url);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/v1/nodes/register")
                    .header("origin", PAGE)
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes")
                    .header("origin", PAGE)
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );
        assert!(captured.lock().unwrap().is_some());
    }

    #[test]
    fn cors_origin_must_be_a_header_value() {
        assert!(cors_layer("http://172.25.0.40:8000").is_ok());
        assert!(cors_layer("*").is_ok());
        assert!(cors_layer("not a\nheader").is_err());
    }

    #[tokio::test]
    async fn register_forwards_post_body_and_status() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);

        let payload = json!({ "node": { "id": "strom-node-1" } });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes/register")
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
        assert_eq!(seen.path, "/v1/nodes/register");
        let forwarded: Value = serde_json::from_slice(&seen.body).unwrap();
        assert_eq!(forwarded, payload);
    }

    #[tokio::test]
    async fn heartbeat_forwards_node_scoped_path() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/nodes/strom-node-1/heartbeat")
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
        assert_eq!(seen.path, "/v1/nodes/strom-node-1/heartbeat");
    }

    #[tokio::test]
    async fn get_desired_forwards_and_relays_status() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/nodes/strom-node-1/desired")
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
        assert_eq!(seen.path, "/v1/nodes/strom-node-1/desired");
    }

    #[tokio::test]
    async fn unreachable_controller_is_bad_gateway() {
        let app = open_app("http://127.0.0.1:1".to_string());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/state")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    /// The paths adapters used to dial before the `/v1` prefix are gone: it is a
    /// clean break, not an alias.
    #[tokio::test]
    async fn unversioned_node_paths_are_not_served() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        for (method, uri) in [
            ("POST", "/nodes/register"),
            ("GET", "/nodes/strom-node-1/desired"),
            ("POST", "/nodes/strom-node-1/heartbeat"),
            ("GET", "/nodes"),
            ("GET", "/endpoints"),
            ("GET", "/state"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Body::from("{}"))
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

    /// Registering a node id and pulling that node's topology and allocated ports
    /// are both unreachable without the token, and neither reaches the controller.
    #[tokio::test]
    async fn node_routes_reject_missing_and_wrong_tokens() {
        for header in [None, Some("Bearer wrong-token"), Some("Basic ignored")] {
            let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
            let app = guarded_app(url);

            for (method, uri) in [
                ("POST", "/v1/nodes/register"),
                ("GET", "/v1/nodes/strom-node-1/desired"),
                ("POST", "/v1/nodes/strom-node-1/heartbeat"),
                ("GET", "/v1/nodes"),
                ("GET", "/v1/endpoints"),
                ("GET", "/v1/state"),
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
                    .oneshot(request.body(Body::from("{}")).unwrap())
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
                    .uri("/v1/nodes/strom-node-1/desired")
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
            "southbound authenticates its own hop to the controller"
        );
    }
}
