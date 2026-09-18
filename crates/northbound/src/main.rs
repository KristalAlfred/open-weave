//! Northbound API — desired-state surface for operators and systems. Stateless:
//! it validates stream submissions at the boundary and proxies every request to
//! the controller, which owns all state.

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;
use weave_core::auth::{self, Guard, Token, require_bearer};
use weave_core::{
    ApiError, ApiErrorCode, ROUTE_NODES, ROUTE_STATUS, ROUTE_STREAM, ROUTE_STREAM_ENDPOINTS,
    ROUTE_STREAM_PLANS, ROUTE_STREAM_SET, ROUTE_STREAM_SETS, ROUTE_STREAMS, StreamDefinition,
    StreamSetApply, ValidationIssue, resource_id_issue, validate_resource_id, validate_stream,
};

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

/// `/health` stays open for compose healthchecks and load balancers. Every contract route
/// requires the northbound bearer token, `/status` included — unlike on the
/// controller, where the embedded dashboard reads the same rollup.
fn router(state: AppState, guard: Guard) -> Router {
    let operator = Router::new()
        .route(ROUTE_NODES, get(list_nodes))
        .route(ROUTE_STREAMS, get(list_streams).post(submit_stream))
        .route(ROUTE_STREAM, get(get_stream).delete(delete_stream))
        .route(ROUTE_STREAM_ENDPOINTS, get(get_endpoints))
        .route(ROUTE_STREAM_PLANS, axum::routing::post(plan_stream))
        .route(ROUTE_STREAM_SETS, get(list_stream_sets))
        .route(ROUTE_STREAM_SET, get(get_stream_set).put(put_stream_set))
        .route(ROUTE_STATUS, get(get_status))
        .fallback(api_route_not_found)
        .method_not_allowed_fallback(api_method_not_allowed)
        .layer(axum::middleware::from_fn_with_state(guard, require_bearer));

    Router::new()
        .route("/health", get(health))
        .merge(operator)
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn api_route_not_found() -> Response {
    error(
        StatusCode::NOT_FOUND,
        ApiErrorCode::RouteNotFound,
        "API route not found",
    )
}

async fn api_method_not_allowed() -> Response {
    error(
        StatusCode::METHOD_NOT_ALLOWED,
        ApiErrorCode::MethodNotAllowed,
        "method not allowed",
    )
}

async fn list_streams(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, "/streams", None).await
}

async fn list_nodes(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, ROUTE_NODES, None).await
}

async fn get_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    proxy(
        &state,
        reqwest::Method::GET,
        &format!("/streams/{name}"),
        None,
    )
    .await
}

async fn submit_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<StreamDefinition>, JsonRejection>,
) -> Response {
    let Json(stream) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    let issues = validate_stream(&stream);
    if !issues.is_empty() {
        return invalid_request("stream validation failed", issues);
    }

    let body = match serde_json::to_vec(&stream) {
        Ok(body) => body,
        Err(err) => {
            tracing::error!(%err, "serializing validated stream failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::EncodingFailed,
                "failed to encode stream",
            );
        }
    };
    proxy_with_headers(
        &state,
        reqwest::Method::POST,
        "/streams",
        Some(body.into()),
        &headers,
        &[reqwest::header::IF_MATCH, reqwest::header::IF_NONE_MATCH],
    )
    .await
}

async fn plan_stream(
    State(state): State<AppState>,
    payload: Result<Json<StreamDefinition>, JsonRejection>,
) -> Response {
    let Json(stream) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    let issues = validate_stream(&stream);
    if !issues.is_empty() {
        return invalid_request("stream validation failed", issues);
    }
    let body = match serde_json::to_vec(&stream) {
        Ok(body) => body,
        Err(err) => {
            tracing::error!(%err, "serializing validated stream plan failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::EncodingFailed,
                "failed to encode stream",
            );
        }
    };
    proxy(
        &state,
        reqwest::Method::POST,
        ROUTE_STREAM_PLANS,
        Some(body.into()),
    )
    .await
}

async fn list_stream_sets(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, ROUTE_STREAM_SETS, None).await
}

async fn get_stream_set(State(state): State<AppState>, Path(owner): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&owner) {
        return invalid_request(
            "stream-set owner is invalid",
            vec![resource_id_issue("owner", "stream-set owner", reason)],
        );
    }
    proxy(
        &state,
        reqwest::Method::GET,
        &format!("/stream-sets/{owner}"),
        None,
    )
    .await
}

async fn put_stream_set(
    State(state): State<AppState>,
    Path(owner): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<StreamSetApply>, JsonRejection>,
) -> Response {
    if let Err(reason) = validate_resource_id(&owner) {
        return invalid_request(
            "stream-set owner is invalid",
            vec![resource_id_issue("owner", "stream-set owner", reason)],
        );
    }
    let Json(apply) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection),
    };
    let body = match serde_json::to_vec(&apply) {
        Ok(body) => body,
        Err(err) => {
            tracing::error!(%err, "serializing stream set failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::EncodingFailed,
                "failed to encode stream set",
            );
        }
    };
    proxy_with_headers(
        &state,
        reqwest::Method::PUT,
        &format!("/stream-sets/{owner}"),
        Some(body.into()),
        &headers,
        &[reqwest::header::IF_MATCH, reqwest::header::IF_NONE_MATCH],
    )
    .await
}

async fn delete_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    proxy_with_headers(
        &state,
        reqwest::Method::DELETE,
        &format!("/streams/{name}"),
        None,
        &headers,
        &[reqwest::header::IF_MATCH],
    )
    .await
}

async fn get_endpoints(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    proxy(
        &state,
        reqwest::Method::GET,
        &format!("/streams/{name}/endpoints"),
        None,
    )
    .await
}

async fn get_status(State(state): State<AppState>) -> Response {
    proxy(&state, reqwest::Method::GET, "/status", None).await
}

fn error(status: StatusCode, code: ApiErrorCode, message: &str) -> Response {
    ApiError::new(code, message).response(status)
}

fn invalid_request(message: &str, details: Vec<ValidationIssue>) -> Response {
    ApiError::with_details(ApiErrorCode::InvalidRequest, message, details)
        .response(StatusCode::BAD_REQUEST)
}

fn invalid_json(rejection: JsonRejection) -> Response {
    ApiError::with_details(
        ApiErrorCode::InvalidJson,
        "request body is not valid for this endpoint",
        vec![ValidationIssue::new(
            "body",
            "invalid_json",
            rejection.body_text(),
        )],
    )
    .response(StatusCode::BAD_REQUEST)
}

/// Forward a request to the controller, passing its status and body back
/// faithfully. Handlers name the same paths this service serves.
async fn proxy(
    state: &AppState,
    method: reqwest::Method,
    path: &str,
    body: Option<Bytes>,
) -> Response {
    proxy_with_headers(state, method, path, body, &HeaderMap::new(), &[]).await
}

async fn proxy_with_headers(
    state: &AppState,
    method: reqwest::Method,
    path: &str,
    body: Option<Bytes>,
    headers: &HeaderMap,
    forwarded_headers: &[reqwest::header::HeaderName],
) -> Response {
    let url = format!("{}{path}", state.controller_url.trim_end_matches('/'));
    let mut request = state.http.request(method, &url);
    if let Some(token) = &state.token {
        request = request.header(reqwest::header::AUTHORIZATION, token.header_value());
    }
    for name in forwarded_headers {
        if let Some(value) = headers.get(name) {
            request = request.header(name, value);
        }
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
            error(
                StatusCode::BAD_GATEWAY,
                ApiErrorCode::ControllerUnreachable,
                "controller unreachable",
            )
        }
    }
}

async fn relay(response: reqwest::Response) -> Response {
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .cloned();
    let etag = response.headers().get(reqwest::header::ETAG).cloned();
    let body = response.bytes().await.unwrap_or_default();
    let mut out = (status, body).into_response();
    if let Some(value) = content_type {
        out.headers_mut()
            .insert(reqwest::header::CONTENT_TYPE, value);
    }
    if let Some(value) = etag {
        out.headers_mut().insert(reqwest::header::ETAG, value);
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
    use weave_core::{RemoteAddr, SrtEndpoint, StreamDestination, StreamTransport};

    const TOKEN: &str = "northbound-test-token";

    #[derive(Clone, Default)]
    struct Captured {
        method: String,
        path: String,
        body: Vec<u8>,
        authorization: Option<String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
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
            let if_match = request
                .headers()
                .get(axum::http::header::IF_MATCH)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let if_none_match = request
                .headers()
                .get(axum::http::header::IF_NONE_MATCH)
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
                if_match,
                if_none_match,
            });
            let mut response = (status, Json(json!({ "ok": true }))).into_response();
            response.headers_mut().insert(
                axum::http::header::ETAG,
                axum::http::HeaderValue::from_static("\"revision-7\""),
            );
            response
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
            format: None,
            accepts: None,
            network: None,
            latency: None,
        }
    }

    fn sample_stream() -> StreamDefinition {
        StreamDefinition {
            name: "cam1-to-studio".to_string(),
            enabled: true,
            source: StreamTransport::Srt(node_ref("strom-node-1")),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                endpoint: StreamTransport::Srt(node_ref("strom-node-2")),
            }],
        }
    }

    async fn body_json(response: Response) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn device_endpoints_are_accepted_and_forwarded() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);

        let payload = json!({
            "name": "alice-cam",
            "source": { "device": { "node": "browser-a1b2" } },
            "destinations": [ { "id": "studio", "srt": { "node": "strom-node-2" } } ]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let seen = captured.lock().unwrap().clone().expect("forwarded");
        let forwarded: Value = serde_json::from_slice(&seen.body).unwrap();
        assert_eq!(forwarded["source"]["device"]["node"], "browser-a1b2");
    }

    #[tokio::test]
    async fn device_endpoint_with_an_empty_node_is_rejected() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);

        let payload = json!({
            "name": "alice-return",
            "source": { "srt": { "node": "strom-node-2" } },
            "destinations": [ { "id": "preview", "device": { "node": "  " } } ]
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_json(response).await;
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["message"], "stream validation failed");
        assert_eq!(body["details"][0]["field"], "destinations[0].device.node");
        assert!(
            captured.lock().unwrap().is_none(),
            "rejected at the boundary"
        );
    }

    #[tokio::test]
    async fn post_forwards_body_and_passes_status_through() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);

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
    async fn post_forwards_conditional_headers() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .header("if-match", "\"revision-6\"")
                    .header("if-none-match", "*")
                    .body(Body::from(serde_json::to_vec(&sample_stream()).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let seen = captured.lock().unwrap().clone().expect("forwarded");
        assert_eq!(seen.if_match.as_deref(), Some("\"revision-6\""));
        assert_eq!(seen.if_none_match.as_deref(), Some("*"));
    }

    #[tokio::test]
    async fn plan_forwards_body_and_passes_status_through() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/stream-plans")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&sample_stream()).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let seen = captured.lock().unwrap().clone().expect("forwarded");
        assert_eq!(seen.method, "POST");
        assert_eq!(seen.path, "/stream-plans");
        assert_eq!(
            serde_json::from_slice::<StreamDefinition>(&seen.body).unwrap(),
            sample_stream()
        );
    }

    #[tokio::test]
    async fn stream_set_reads_forward_paths_and_etags() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/stream-sets")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            captured.lock().unwrap().as_ref().unwrap().path,
            "/stream-sets"
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream-sets/studio-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[axum::http::header::ETAG],
            "\"revision-7\""
        );
        assert_eq!(
            captured.lock().unwrap().as_ref().unwrap().path,
            "/stream-sets/studio-a"
        );
    }

    #[tokio::test]
    async fn stream_set_put_forwards_body_and_conditional_headers() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);
        let payload = json!({ "streams": [sample_stream()], "prune": true });

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/stream-sets/studio-a")
                    .header("content-type", "application/json")
                    .header("if-match", "\"set-revision-6\"")
                    .header("if-none-match", "*")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response.headers()[axum::http::header::ETAG],
            "\"revision-7\""
        );

        let seen = captured.lock().unwrap().clone().expect("forwarded");
        assert_eq!(seen.method, "PUT");
        assert_eq!(seen.path, "/stream-sets/studio-a");
        assert_eq!(seen.if_match.as_deref(), Some("\"set-revision-6\""));
        assert_eq!(seen.if_none_match.as_deref(), Some("*"));
        assert_eq!(
            serde_json::from_slice::<Value>(&seen.body).unwrap(),
            payload
        );
    }

    #[tokio::test]
    async fn invalid_stream_set_owner_is_rejected_before_proxying() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        for (method, uri) in [
            ("GET", "/stream-sets/foo%3Fignored"),
            ("PUT", "/stream-sets/foo%3Fignored"),
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
            let body = body_json(response).await;
            assert_eq!(body["code"], "invalid_request");
            assert_eq!(body["details"][0]["field"], "owner");
        }
        assert!(captured.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_forwards_method_and_path() {
        let (url, captured) = stub_controller(StatusCode::NO_CONTENT).await;
        let app = open_app(url);

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
    async fn delete_forwards_if_match() {
        let (url, captured) = stub_controller(StatusCode::NO_CONTENT).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/streams/basic")
                    .header("if-match", "\"revision-7\"")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let seen = captured.lock().unwrap().clone().expect("forwarded");
        assert_eq!(seen.if_match.as_deref(), Some("\"revision-7\""));
        assert_eq!(seen.if_none_match, None);
    }

    #[tokio::test]
    async fn single_stream_forwards_method_and_path() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/streams/basic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let seen = captured.lock().unwrap().clone().expect("forwarded");
        assert_eq!(seen.method, "GET");
        assert_eq!(seen.path, "/streams/basic");
    }

    #[tokio::test]
    async fn controller_etag_is_passed_through() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/streams/basic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.headers()[axum::http::header::ETAG],
            "\"revision-7\""
        );
    }

    #[tokio::test]
    async fn endpoints_forwards_path_and_passes_status_through() {
        let (url, captured) = stub_controller(StatusCode::SERVICE_UNAVAILABLE).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/streams/basic/endpoints")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_json(response).await["ok"], true);

        let seen = captured
            .lock()
            .unwrap()
            .clone()
            .expect("controller saw a request");
        assert_eq!(seen.method, "GET");
        assert_eq!(seen.path, "/streams/basic/endpoints");
    }

    #[tokio::test]
    async fn status_forwards_method_and_path() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
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
        assert_eq!(seen.path, "/status");
    }

    #[tokio::test]
    async fn get_passes_controller_status_through() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

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
    async fn nodes_forwards_get_with_the_northbound_token() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let response = guarded_app(url)
            .oneshot(
                Request::builder()
                    .uri("/nodes")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let seen = captured.lock().unwrap().clone().expect("forwarded");
        assert_eq!(seen.method, "GET");
        assert_eq!(seen.path, "/nodes");
        assert_eq!(
            seen.authorization.as_deref(),
            Some(format!("Bearer {TOKEN}").as_str())
        );
    }

    #[tokio::test]
    async fn invalid_stream_is_rejected_before_proxying() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let app = open_app(url);

        let mut stream = sample_stream();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0].endpoint else {
            unreachable!("fixture endpoint is srt");
        };
        dest.remote = Some(RemoteAddr {
            host: "198.51.100.5".to_string(),
            port: 9000,
            network: "internet".to_string(),
        });

        for uri in ["/streams", "/stream-plans"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(&stream).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = body_json(response).await;
            assert_eq!(body["code"], "invalid_request");
            assert_eq!(body["details"][0]["field"], "destinations[0].srt");
            assert_eq!(body["details"][0]["code"], "mutually_exclusive");
        }
        assert!(
            captured.lock().unwrap().is_none(),
            "invalid stream never reaches the controller"
        );
    }

    #[tokio::test]
    async fn invalid_json_has_a_structured_error_and_is_not_proxied() {
        let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
        let response = open_app(url)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/streams")
                    .header("content-type", "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_json(response).await;
        assert_eq!(body["code"], "invalid_json");
        assert_eq!(body["details"][0]["field"], "body");
        assert!(captured.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn authentication_errors_use_the_shared_envelope() {
        let (url, _captured) = stub_controller(StatusCode::OK).await;
        let response = guarded_app(url)
            .oneshot(
                Request::builder()
                    .uri("/streams")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(body_json(response).await["code"], "unauthorized");
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
                "/streams",
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

    #[tokio::test]
    async fn invalid_stream_path_is_rejected_before_proxying() {
        let (url, captured) = stub_controller(StatusCode::NO_CONTENT).await;
        let app = open_app(url);

        for (method, uri) in [
            ("GET", "/streams/foo%3Fignored"),
            ("DELETE", "/streams/foo%3Fignored"),
            ("GET", "/streams/foo%3Fignored/endpoints"),
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
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{method} {uri}");
        }
        assert!(captured.lock().unwrap().is_none());
    }

    /// Version-prefixed paths are not aliases for the current contract.
    #[tokio::test]
    async fn retired_contract_paths_are_not_served() {
        let (url, captured) = stub_controller(StatusCode::OK).await;
        let app = open_app(url);

        for (method, uri) in [
            ("GET", "/v1/streams"),
            ("GET", "/v1/nodes"),
            ("POST", "/v1/streams"),
            ("POST", "/v1/stream-plans"),
            ("GET", "/v1/streams/basic"),
            ("DELETE", "/v1/streams/basic"),
            ("GET", "/v1/streams/basic/endpoints"),
            ("GET", "/v1/status"),
            ("GET", "/v2/streams"),
            ("GET", "/v2/nodes"),
            ("POST", "/v2/streams"),
            ("POST", "/v2/stream-plans"),
            ("GET", "/v2/streams/basic"),
            ("DELETE", "/v2/streams/basic"),
            ("GET", "/v2/streams/basic/endpoints"),
            ("GET", "/v2/status"),
            ("GET", "/v3/streams"),
            ("GET", "/v3/nodes"),
            ("POST", "/v3/streams"),
            ("POST", "/v3/stream-plans"),
            ("GET", "/v3/streams/basic"),
            ("DELETE", "/v3/streams/basic"),
            ("GET", "/v3/streams/basic/endpoints"),
            ("GET", "/v3/status"),
            ("GET", "/v4/streams"),
            ("GET", "/v4/nodes"),
            ("POST", "/v4/streams"),
            ("POST", "/v4/stream-plans"),
            ("GET", "/v4/streams/basic"),
            ("DELETE", "/v4/streams/basic"),
            ("GET", "/v4/streams/basic/endpoints"),
            ("GET", "/v4/status"),
            ("GET", "/v5/streams"),
            ("GET", "/v5/nodes"),
            ("POST", "/v5/streams"),
            ("POST", "/v5/stream-plans"),
            ("GET", "/v5/streams/basic"),
            ("DELETE", "/v5/streams/basic"),
            ("GET", "/v5/streams/basic/endpoints"),
            ("GET", "/v5/stream-sets"),
            ("GET", "/v5/stream-sets/studio-a"),
            ("PUT", "/v5/stream-sets/studio-a"),
            ("GET", "/v5/status"),
            ("GET", "/v6/streams"),
            ("GET", "/v6/nodes"),
            ("POST", "/v6/streams"),
            ("POST", "/v6/stream-plans"),
            ("GET", "/v6/streams/basic"),
            ("DELETE", "/v6/streams/basic"),
            ("GET", "/v6/streams/basic/endpoints"),
            ("GET", "/v6/stream-sets"),
            ("GET", "/v6/stream-sets/studio-a"),
            ("PUT", "/v6/stream-sets/studio-a"),
            ("GET", "/v6/status"),
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

    /// Missing and wrong tokens are both rejected, on read and write, with a
    /// `Bearer` challenge — and nothing reaches the controller.
    #[tokio::test]
    async fn contract_routes_reject_missing_and_wrong_tokens() {
        for header in [None, Some("Bearer wrong-token"), Some("Basic ignored")] {
            let (url, captured) = stub_controller(StatusCode::ACCEPTED).await;
            let app = guarded_app(url);

            for (method, uri, body) in [
                ("GET", "/nodes", Body::empty()),
                ("GET", "/streams", Body::empty()),
                (
                    "POST",
                    "/streams",
                    Body::from(serde_json::to_vec(&sample_stream()).unwrap()),
                ),
                (
                    "POST",
                    "/stream-plans",
                    Body::from(serde_json::to_vec(&sample_stream()).unwrap()),
                ),
                ("GET", "/streams/basic", Body::empty()),
                ("DELETE", "/streams/basic", Body::empty()),
                ("GET", "/streams/basic/endpoints", Body::empty()),
                ("GET", "/stream-sets", Body::empty()),
                ("GET", "/stream-sets/studio-a", Body::empty()),
                ("PUT", "/stream-sets/studio-a", Body::from("{}")),
                ("GET", "/status", Body::empty()),
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
                    .uri("/streams")
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
