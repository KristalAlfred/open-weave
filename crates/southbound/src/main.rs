//! Southbound API — the adapter-facing surface. Stateless: every request is
//! proxied to the controller, which owns all node and desired state. Adapters
//! keep dialing this service; it relays to the controller with the node's own
//! token, and to whichever of several controllers leads.

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing_subscriber::EnvFilter;
use weave_core::auth::{self, NodeCaller, NodeGuard, refuse_other_node, require_node_token};
use weave_core::upstream::{Answer, Controllers};
use weave_core::{
    ApiError, ApiErrorCode, ROUTE_ENDPOINTS, ROUTE_NODE_DESIRED, ROUTE_NODE_HEARTBEAT,
    ROUTE_NODE_REGISTER, ROUTE_NODES, ROUTE_STATE, resource_id_issue, validate_resource_id,
};

const DEFAULT_ADDR: &str = "127.0.0.1:8081";

/// Origin a browser-hosted node may call the adapter contract from. Unset means
/// no CORS headers at all, which is right for every adapter that is not a web
/// page. `*` allows any origin, for development.
const CORS_ORIGIN_VAR: &str = "WEAVE_SOUTHBOUND_CORS_ORIGIN";

#[derive(Clone)]
struct AppState {
    controllers: Arc<Controllers>,
}

impl AppState {
    fn new(controllers: Controllers) -> Self {
        Self {
            controllers: Arc::new(controllers),
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
    let controllers = Controllers::from_env()?;
    let controller_urls = controllers.urls().join(",");

    let guard = NodeGuard::from_env(auth::SOUTHBOUND_KEY_VAR)?;
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
    let app = router(AppState::new(controllers), guard, cors);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding southbound listener on {addr}"))?;
    tracing::info!(%addr, controllers = %controller_urls, "southbound API listening");

    axum::serve(listener, app)
        .await
        .context("southbound server error")?;
    Ok(())
}

/// `/health` stays open for compose healthchecks and load balancers.
/// Everything else — registration, heartbeats, and the desired-state and topology
/// reads — requires a node token. Registration, heartbeat and desired hops also
/// require the token to belong to the node named, which the controller checks
/// again.
///
/// A browser-hosted node calls this contract from a web page, so API routes
/// optionally carry CORS headers. The layer sits outside the bearer
/// check: a preflight carries no `Authorization` header and must be answered
/// before it, not refused by it.
fn router(state: AppState, guard: NodeGuard, cors: Option<CorsLayer>) -> Router {
    let mut nodes = Router::new()
        .route(ROUTE_NODES, get(list_nodes))
        .route(ROUTE_NODE_REGISTER, post(register_node))
        .route(ROUTE_NODE_HEARTBEAT, post(node_heartbeat))
        .route(ROUTE_NODE_DESIRED, get(get_desired))
        .route(ROUTE_ENDPOINTS, get(list_endpoints))
        .route(ROUTE_STATE, get(get_state))
        .fallback(api_route_not_found)
        .method_not_allowed_fallback(api_method_not_allowed)
        .layer(axum::middleware::from_fn_with_state(
            guard,
            require_node_token,
        ));
    if let Some(cors) = cors {
        nodes = nodes.layer(cors);
    }

    Router::new()
        .route("/health", get(health))
        .merge(nodes)
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

async fn api_route_not_found() -> Response {
    ApiError::new(ApiErrorCode::RouteNotFound, "API route not found")
        .response(StatusCode::NOT_FOUND)
}

async fn api_method_not_allowed() -> Response {
    ApiError::new(ApiErrorCode::MethodNotAllowed, "method not allowed")
        .response(StatusCode::METHOD_NOT_ALLOWED)
}

async fn list_nodes(State(state): State<AppState>, headers: HeaderMap) -> Response {
    proxy(&state, &headers, reqwest::Method::GET, "/nodes", None).await
}

async fn list_endpoints(State(state): State<AppState>, headers: HeaderMap) -> Response {
    proxy(&state, &headers, reqwest::Method::GET, "/endpoints", None).await
}

async fn get_state(State(state): State<AppState>, headers: HeaderMap) -> Response {
    proxy(&state, &headers, reqwest::Method::GET, "/state", None).await
}

/// A body that does not name a node id is forwarded as it is, for the
/// controller to refuse with its own validation error.
async fn register_node(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(node_id) = registering_node_id(&body)
        && let Some(response) = refuse_other_node(&caller, &node_id)
    {
        return response;
    }
    proxy(
        &state,
        &headers,
        reqwest::Method::POST,
        "/nodes/register",
        Some(body),
    )
    .await
}

fn registering_node_id(body: &[u8]) -> Option<String> {
    let registration: Value = serde_json::from_slice(body).ok()?;
    registration["node"]["id"].as_str().map(str::to_string)
}

async fn node_heartbeat(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = validate_resource_id(&node_id) {
        return invalid_node_id(error);
    }
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
    proxy(
        &state,
        &headers,
        reqwest::Method::POST,
        &format!("/nodes/{node_id}/heartbeat"),
        Some(body),
    )
    .await
}

async fn get_desired(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = validate_resource_id(&node_id) {
        return invalid_node_id(error);
    }
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
    proxy(
        &state,
        &headers,
        reqwest::Method::GET,
        &format!("/nodes/{node_id}/desired"),
        None,
    )
    .await
}

/// Forward a request to the leading controller with the caller's
/// `Authorization`, passing its status and body back faithfully. Handlers name
/// the same paths this service serves.
async fn proxy(
    state: &AppState,
    headers: &HeaderMap,
    method: reqwest::Method,
    path: &str,
    body: Option<Bytes>,
) -> Response {
    let authorization = headers.get(header::AUTHORIZATION);
    let sent = state
        .controllers
        .send(method, path, |mut request| {
            if let Some(authorization) = authorization {
                request = request.header(reqwest::header::AUTHORIZATION, authorization.as_bytes());
            }
            if let Some(body) = &body {
                request = request
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body.clone());
            }
            request
        })
        .await;
    match sent {
        Ok(answer) => relay(answer),
        Err(unanswered) => {
            tracing::warn!(err = %unanswered.source, url = %unanswered.url, "proxying to controller failed");
            ApiError::new(
                ApiErrorCode::ControllerUnreachable,
                "controller unreachable",
            )
            .response(StatusCode::BAD_GATEWAY)
        }
    }
}

fn invalid_node_id(error: weave_core::ResourceIdError) -> Response {
    ApiError::with_details(
        ApiErrorCode::InvalidRequest,
        "node id is invalid",
        vec![resource_id_issue("node_id", "node id", error)],
    )
    .response(StatusCode::BAD_REQUEST)
}

fn relay(answer: Answer) -> Response {
    let status = StatusCode::from_u16(answer.status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = answer
        .headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let mut out = (status, answer.body).into_response();
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
    use weave_core::auth::NodeKey;

    const KEY: &str = "southbound-test-key";

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

    fn guard() -> NodeGuard {
        NodeGuard::Required(NodeKey::new(KEY).unwrap())
    }

    /// `Authorization` value presenting `node_id`'s token under [`KEY`].
    fn bearer(node_id: &str) -> String {
        format!("Bearer {}", NodeKey::new(KEY).unwrap().token_for(node_id))
    }

    /// A router with authentication switched off, for the proxy-fidelity tests.
    fn state(controller_url: &str) -> AppState {
        AppState::new(Controllers::new(controller_url).unwrap())
    }

    fn open_app(controller_url: String) -> Router {
        router(state(&controller_url), NodeGuard::Disabled, None)
    }

    /// A router requiring node tokens derived from [`KEY`].
    fn guarded_app(controller_url: String) -> Router {
        router(state(&controller_url), guard(), None)
    }

    async fn body_json(response: Response) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// [`guarded_app`] that also admits a browser page served from `origin`.
    fn cors_app(controller_url: String, origin: &str) -> Router {
        router(
            state(&controller_url),
            guard(),
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
                    .uri("/nodes/register")
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
                    .uri("/nodes/browser-a1b2/desired")
                    .header("origin", PAGE)
                    .header("authorization", bearer("browser-a1b2"))
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
                    .uri("/nodes/browser-a1b2/desired")
                    .header("origin", "http://evil.example")
                    .header("authorization", bearer("browser-a1b2"))
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
                    .uri("/nodes")
                    .header("origin", "http://localhost:3000")
                    .header("authorization", bearer("browser-a1b2"))
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
                    .uri("/nodes/register")
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
                    .uri("/nodes")
                    .header("origin", PAGE)
                    .header("authorization", bearer("browser-a1b2"))
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
        let app = open_app(url);

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

    /// A controller on standby: every request gets `503 not_leader`.
    async fn standby_controller() -> String {
        let app = Router::new().fallback(|| async {
            ApiError::new(ApiErrorCode::NotLeader, "standby")
                .response(StatusCode::SERVICE_UNAVAILABLE)
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn requests_reach_the_leading_controller_past_a_standby() {
        let standby = standby_controller().await;
        let (leader, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(format!("{standby},{leader}"));
        let desired = || {
            Request::builder()
                .uri("/nodes/strom-node-1/desired")
                .body(Body::empty())
                .unwrap()
        };

        let response = app.clone().oneshot(desired()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            captured.lock().unwrap().clone().unwrap().path,
            "/nodes/strom-node-1/desired"
        );

        let app = open_app(format!("{standby},{standby}"));
        let response = app.oneshot(desired()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(response).await["code"], "not_leader");
    }

    #[tokio::test]
    async fn get_desired_forwards_and_relays_status() {
        for status in [StatusCode::OK, StatusCode::NOT_FOUND] {
            let (url, captured) = stub_controller(status).await;
            let app = open_app(url);

            let response = app
                .oneshot(
                    Request::builder()
                        .uri("/nodes/strom-node-1/desired")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status);

            let seen = captured
                .lock()
                .unwrap()
                .clone()
                .expect("controller saw a request");
            assert_eq!(seen.method, "GET");
            assert_eq!(seen.path, "/nodes/strom-node-1/desired");
        }
    }

    #[tokio::test]
    async fn unreachable_controller_is_bad_gateway() {
        let app = open_app("http://127.0.0.1:1".to_string());
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

    #[tokio::test]
    async fn invalid_node_path_is_rejected_before_proxying() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        for (method, uri) in [
            ("POST", "/nodes/foo%3Fignored/heartbeat"),
            ("GET", "/nodes/foo%3Fignored/desired"),
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
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{method} {uri}");
        }
        assert!(captured.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn route_and_method_errors_use_the_shared_envelope() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        for (method, uri, expected_status, expected_code) in [
            (
                "GET",
                "/no-such-route",
                StatusCode::NOT_FOUND,
                "route_not_found",
            ),
            (
                "PUT",
                "/nodes",
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(body_json(response).await["code"], expected_code);
        }
    }

    /// Version-prefixed paths are not aliases for the current contract.
    #[tokio::test]
    async fn retired_node_paths_are_not_served() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        for (method, uri) in [
            ("POST", "/v1/nodes/register"),
            ("GET", "/v1/nodes/strom-node-1/desired"),
            ("POST", "/v1/nodes/strom-node-1/heartbeat"),
            ("GET", "/v1/nodes"),
            ("GET", "/v1/endpoints"),
            ("GET", "/v1/state"),
            ("POST", "/v2/nodes/register"),
            ("GET", "/v2/nodes/strom-node-1/desired"),
            ("POST", "/v2/nodes/strom-node-1/heartbeat"),
            ("GET", "/v2/nodes"),
            ("GET", "/v2/endpoints"),
            ("GET", "/v2/state"),
            ("POST", "/v3/nodes/register"),
            ("GET", "/v3/nodes/strom-node-1/desired"),
            ("POST", "/v3/nodes/strom-node-1/heartbeat"),
            ("GET", "/v3/nodes"),
            ("GET", "/v3/endpoints"),
            ("GET", "/v3/state"),
            ("POST", "/v4/nodes/register"),
            ("GET", "/v4/nodes/strom-node-1/desired"),
            ("POST", "/v4/nodes/strom-node-1/heartbeat"),
            ("GET", "/v4/nodes"),
            ("GET", "/v4/endpoints"),
            ("GET", "/v4/state"),
            ("POST", "/v5/nodes/register"),
            ("GET", "/v5/nodes/strom-node-1/desired"),
            ("POST", "/v5/nodes/strom-node-1/heartbeat"),
            ("GET", "/v5/nodes"),
            ("GET", "/v5/endpoints"),
            ("GET", "/v5/state"),
            ("POST", "/v6/nodes/register"),
            ("GET", "/v6/nodes/strom-node-1/desired"),
            ("POST", "/v6/nodes/strom-node-1/heartbeat"),
            ("GET", "/v6/nodes"),
            ("GET", "/v6/endpoints"),
            ("GET", "/v6/state"),
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
            "a retired path never reaches the controller"
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
    /// are both unreachable without a node token, and neither reaches the
    /// controller. The key itself and a pre-node-token shared secret are not
    /// tokens.
    #[tokio::test]
    async fn node_routes_reject_missing_and_wrong_tokens() {
        let key_as_token = format!("Bearer {KEY}");
        for header in [
            None,
            Some("Bearer wrong-token"),
            Some("Bearer bench-southbound-token"),
            Some(key_as_token.as_str()),
            Some("Basic ignored"),
        ] {
            let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
            let app = guarded_app(url);

            for (method, uri) in [
                ("POST", "/nodes/register"),
                ("GET", "/nodes/strom-node-1/desired"),
                ("POST", "/nodes/strom-node-1/heartbeat"),
                ("GET", "/nodes"),
                ("GET", "/endpoints"),
                ("GET", "/state"),
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
    async fn node_token_is_forwarded_to_the_controller_verbatim() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let response = guarded_app(url)
            .oneshot(
                Request::builder()
                    .uri("/nodes/strom-node-1/desired")
                    .header("authorization", bearer("strom-node-1"))
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
            Some(bearer("strom-node-1").as_str()),
            "the controller checks the node's own token again"
        );
    }

    #[tokio::test]
    async fn a_node_token_is_refused_for_another_node() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = guarded_app(url);
        let registration = json!({ "node": { "id": "strom-node-2" } }).to_string();
        let heartbeat = json!({ "node_id": "strom-node-2" }).to_string();

        for (method, uri, body) in [
            ("POST", "/nodes/register", registration),
            ("POST", "/nodes/strom-node-2/heartbeat", heartbeat),
            ("GET", "/nodes/strom-node-2/desired", String::new()),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("authorization", bearer("strom-node-1"))
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method} {uri}");
            assert_eq!(body_json(response).await["code"], "forbidden");
        }
        assert!(
            captured.lock().unwrap().is_none(),
            "a refused request never reaches the controller"
        );
    }

    #[tokio::test]
    async fn a_node_registers_as_itself_and_reads_shared_inventory() {
        for (method, uri, body) in [
            (
                "POST",
                "/nodes/register",
                json!({ "node": { "id": "strom-node-1" } }).to_string(),
            ),
            ("GET", "/nodes", String::new()),
            ("GET", "/endpoints", String::new()),
            ("GET", "/state", String::new()),
        ] {
            let (url, captured) = stub_controller(StatusCode::OK).await;
            let response = guarded_app(url)
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(uri)
                        .header("authorization", bearer("strom-node-1"))
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{method} {uri}");
            assert!(captured.lock().unwrap().is_some(), "{method} {uri}");
        }
    }
}
