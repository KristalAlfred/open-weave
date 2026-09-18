//! `weave` — operator CLI for the open-weave control plane.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use reqwest::RequestBuilder;
use reqwest::header::{ETAG, HeaderValue, IF_MATCH, IF_NONE_MATCH};
use tracing_subscriber::EnvFilter;
use weave_core::auth::{self, Token};
use weave_core::{
    API_PREFIX, ApiError, StreamDefinition, StreamPlan, StreamResource, validate_resource_id,
};

#[derive(Parser)]
#[command(name = "weave", version, about = "open-weave control plane CLI")]
struct Cli {
    /// Northbound API base URL.
    #[arg(
        long,
        env = "WEAVE_NORTHBOUND_URL",
        default_value = "http://127.0.0.1:9080",
        global = true
    )]
    url: String,
    /// Bearer token presented to the northbound API. Required unless the server
    /// runs with WEAVE_AUTH_DISABLED=1.
    // `hide_env_values` keeps the token value itself out of `--help` output,
    // which clap would otherwise print alongside the variable name.
    #[arg(
        long,
        env = auth::NORTHBOUND_TOKEN_VAR,
        hide_env_values = true,
        global = true
    )]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply a stream definition (YAML) as desired state.
    Apply {
        /// Path to the stream YAML file.
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    /// Preview validation and placement without changing desired state.
    Plan {
        /// Path to the stream YAML file.
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    /// Get resources from the northbound API.
    Get {
        #[command(subcommand)]
        resource: GetResource,
    },
    /// Delete resources through the northbound API.
    Delete {
        #[command(subcommand)]
        resource: DeleteResource,
    },
    /// List registered nodes.
    Nodes,
}

#[derive(Subcommand)]
enum GetResource {
    /// List desired streams.
    Streams,
    /// Get one desired stream.
    Stream {
        /// Name of the stream to get.
        name: String,
    },
}

#[derive(Subcommand)]
enum DeleteResource {
    /// Delete a desired stream.
    Stream {
        /// Name of the stream to delete.
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let Cli {
        url,
        token,
        command,
    } = Cli::parse();
    let token = token.as_deref().and_then(Token::new);

    match command {
        Command::Apply { file } => apply(&url, token.as_ref(), &file).await,
        Command::Plan { file } => plan(&url, token.as_ref(), &file).await,
        Command::Get { resource } => match resource {
            GetResource::Streams => get_streams(&url, token.as_ref()).await,
            GetResource::Stream { name } => get_stream(&url, token.as_ref(), &name).await,
        },
        Command::Delete { resource } => match resource {
            DeleteResource::Stream { name } => delete_stream(&url, token.as_ref(), &name).await,
        },
        Command::Nodes => nodes(),
    }
}

/// Present the bearer token when one is configured. Without it northbound
/// answers `401`; [`unauthorized_hint`] explains why.
fn authorized(request: RequestBuilder, token: Option<&Token>) -> RequestBuilder {
    match token {
        Some(token) => request.header(reqwest::header::AUTHORIZATION, token.header_value()),
        None => request,
    }
}

/// Extra guidance appended to a `401`, since a missing `--token` is by far the
/// likeliest cause and the bare status does not say so.
fn unauthorized_hint(token: Option<&Token>) -> &'static str {
    if token.is_none() {
        "\nno token was sent: pass --token or set WEAVE_NORTHBOUND_TOKEN"
    } else {
        "\nthe token sent was rejected: check it matches the server's WEAVE_NORTHBOUND_TOKEN"
    }
}

fn error_detail(body: &str) -> String {
    let Ok(error) = serde_json::from_str::<ApiError>(body) else {
        return body.to_string();
    };
    let code = serde_json::to_string(&error.code)
        .unwrap_or_else(|_| "\"unknown\"".to_string())
        .trim_matches('"')
        .to_string();
    let mut detail = format!("[{code}] {}", error.message);
    for issue in error.details {
        detail.push_str(&format!(
            "\n{} [{}]: {}",
            issue.field, issue.code, issue.message
        ));
    }
    detail
}

fn parse_stream(yaml: &str) -> Result<StreamDefinition> {
    serde_norway::with::singleton_map_recursive::deserialize(serde_norway::Deserializer::from_str(
        yaml,
    ))
    .context("parsing stream YAML")
}

async fn apply(url: &str, token: Option<&Token>, file: &Path) -> Result<()> {
    let stream = read_stream(file)?;
    apply_stream(url, token, &stream).await
}

async fn apply_stream(url: &str, token: Option<&Token>, stream: &StreamDefinition) -> Result<()> {
    if let Err(error) = validate_resource_id(&stream.name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let existing = lookup_stream(&client, url, token, &stream.name).await?;

    let request = authorized(client.post(api_url(url, "/streams")), token).json(&stream);
    let request = match existing {
        Some(existing) => request.header(IF_MATCH, existing.etag),
        None => request.header(IF_NONE_MATCH, "*"),
    };
    let response = request
        .send()
        .await
        .context("posting stream to northbound")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
            unauthorized_hint(token)
        } else {
            ""
        };
        bail!(
            "northbound rejected stream: {status}: {}{hint}",
            error_detail(&body)
        );
    }

    tracing::info!(name = %stream.name, %status, "stream applied");
    println!("{body}");
    Ok(())
}

fn read_stream(file: &Path) -> Result<StreamDefinition> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    parse_stream(&text).with_context(|| format!("parsing stream from {}", file.display()))
}

async fn plan(url: &str, token: Option<&Token>, file: &Path) -> Result<()> {
    let stream = read_stream(file)?;
    plan_stream(url, token, &stream).await
}

async fn plan_stream(url: &str, token: Option<&Token>, stream: &StreamDefinition) -> Result<()> {
    let response = authorized(
        reqwest::Client::new().post(api_url(url, "/stream-plans")),
        token,
    )
    .json(&stream)
    .send()
    .await
    .context("planning stream through northbound")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
            unauthorized_hint(token)
        } else {
            ""
        };
        bail!(
            "northbound rejected stream plan: {status}: {}{hint}",
            error_detail(&body)
        );
    }
    let plan: StreamPlan = serde_json::from_str(&body).context("decoding stream plan")?;
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}

async fn get_streams(url: &str, token: Option<&Token>) -> Result<()> {
    let response = authorized(reqwest::Client::new().get(api_url(url, "/streams")), token)
        .send()
        .await
        .context("fetching streams")?;

    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        bail!(
            "northbound streams request failed: {status}{}",
            unauthorized_hint(token)
        );
    }
    let streams = response
        .error_for_status()
        .context("northbound streams request failed")?
        .json::<Vec<StreamResource>>()
        .await
        .context("decoding streams")?;

    println!("{}", serde_json::to_string_pretty(&streams)?);
    Ok(())
}

async fn get_stream(url: &str, token: Option<&Token>, name: &str) -> Result<()> {
    if let Err(error) = validate_resource_id(name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let Some(stream) = lookup_stream(&client, url, token, name).await? else {
        bail!("no stream named {name}");
    };
    println!("{}", serde_json::to_string_pretty(&stream.resource)?);
    Ok(())
}

async fn delete_stream(url: &str, token: Option<&Token>, name: &str) -> Result<()> {
    if let Err(error) = validate_resource_id(name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let Some(existing) = lookup_stream(&client, url, token, name).await? else {
        bail!("no stream named {name}");
    };
    let response = authorized(
        client.delete(api_url(url, &format!("/streams/{name}"))),
        token,
    )
    .header(IF_MATCH, existing.etag)
    .send()
    .await
    .context("deleting stream on northbound")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::NOT_FOUND {
        let detail = if body.trim().is_empty() {
            String::new()
        } else {
            format!(": {body}")
        };
        bail!("no stream named {name}{detail}");
    }
    if !status.is_success() {
        let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
            unauthorized_hint(token)
        } else {
            ""
        };
        bail!(
            "northbound rejected delete: {status}: {}{hint}",
            error_detail(&body)
        );
    }

    tracing::info!(%name, %status, "stream deleted");
    println!("deleted stream {name}");
    Ok(())
}

struct StreamLookup {
    resource: StreamResource,
    etag: HeaderValue,
}

async fn lookup_stream(
    client: &reqwest::Client,
    url: &str,
    token: Option<&Token>,
    name: &str,
) -> Result<Option<StreamLookup>> {
    let response = authorized(client.get(api_url(url, &format!("/streams/{name}"))), token)
        .send()
        .await
        .context("fetching stream")?;

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let etag = response.headers().get(ETAG).cloned();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
            unauthorized_hint(token)
        } else {
            ""
        };
        bail!(
            "northbound stream request failed: {status}: {}{hint}",
            error_detail(&body)
        );
    }
    let etag = etag.context("northbound stream response is missing its ETag")?;
    let resource = serde_json::from_str(&body).context("decoding stream resource")?;
    Ok(Some(StreamLookup { resource, etag }))
}

fn nodes() -> Result<()> {
    tracing::info!("nodes: not implemented");
    Ok(())
}

/// Build a northbound API URL from a contract-relative `path`, inserting the
/// version prefix so the literal lives only in [`weave_core::API_PREFIX`].
fn api_url(base: &str, path: &str) -> String {
    format!("{}{}{path}", base.trim_end_matches('/'), API_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{Request, StatusCode};
    use axum::response::{IntoResponse, Response};
    use std::sync::{Arc, Mutex};
    use weave_core::{ApiErrorCode, SrtEndpoint, StreamAccepted, StreamTransport};

    #[test]
    fn parses_fanout_yaml_with_defaults_and_srt_tag() {
        let yaml = r#"
name: cam1-to-studio
source:
  srt:
    node: strom-node-1
    latency: 200
destinations:
  - srt:
      node: strom-node-2
  - srt:
      node: strom-node-2
      network: wan
"#;

        let stream = parse_stream(yaml).expect("parse stream yaml");

        assert_eq!(stream.name, "cam1-to-studio");
        assert!(stream.enabled, "enabled defaults to true when omitted");
        assert_eq!(
            stream.source,
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: Some(200),
            })
        );
        assert_eq!(stream.destinations.len(), 2);
        assert_eq!(
            stream.destinations[1],
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: Some("wan".to_string()),
                latency: None,
            })
        );
    }

    #[test]
    fn structured_errors_show_field_details() {
        let body = serde_json::json!({
            "code": "invalid_request",
            "message": "stream validation failed",
            "details": [{
                "field": "name",
                "code": "invalid_characters",
                "message": "stream name is invalid"
            }]
        })
        .to_string();

        assert_eq!(
            error_detail(&body),
            "[invalid_request] stream validation failed\nname [invalid_characters]: stream name is invalid"
        );
    }

    #[test]
    fn structured_precondition_errors_keep_their_stable_code() {
        let body = serde_json::json!({
            "code": "precondition_required",
            "message": "If-Match or If-None-Match is required"
        })
        .to_string();

        assert_eq!(
            error_detail(&body),
            "[precondition_required] If-Match or If-None-Match is required"
        );
    }

    #[test]
    fn api_url_inserts_the_version_prefix_once() {
        assert_eq!(
            api_url("http://127.0.0.1:9080", "/streams"),
            "http://127.0.0.1:9080/v5/streams"
        );
        assert_eq!(
            api_url("http://127.0.0.1:9080/", "/streams"),
            "http://127.0.0.1:9080/v5/streams",
            "a trailing slash on the base does not double up"
        );
    }

    #[test]
    fn parsed_yaml_serializes_to_northbound_json_shape() {
        let yaml = r#"
name: paused
enabled: false
source:
  srt:
    node: strom-node-1
destinations:
  - srt:
      node: strom-node-2
"#;

        let stream = parse_stream(yaml).expect("parse stream yaml");
        assert!(!stream.enabled);

        let json = serde_json::to_value(&stream).unwrap();
        assert_eq!(json["source"]["srt"]["node"], "strom-node-1");
        assert_eq!(json["destinations"][0]["srt"]["node"], "strom-node-2");
    }

    fn sample_stream() -> StreamDefinition {
        StreamDefinition {
            name: "cam1-to-studio".to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                format: None,
                accepts: None,
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                format: None,
                accepts: None,
            })],
        }
    }

    #[derive(Clone)]
    struct ResourceStub {
        seen: Arc<Mutex<Vec<String>>>,
        resource: Option<StreamResource>,
        mutation_status: StatusCode,
        include_etag: bool,
    }

    async fn stub_resource(
        resource: Option<StreamResource>,
        mutation_status: StatusCode,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        start_resource_stub(resource, mutation_status, true).await
    }

    async fn start_resource_stub(
        resource: Option<StreamResource>,
        mutation_status: StatusCode,
        include_etag: bool,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        async fn record(State(state): State<ResourceStub>, request: Request<Body>) -> Response {
            let authorization = request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            let if_match = request
                .headers()
                .get(axum::http::header::IF_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            let if_none_match = request
                .headers()
                .get(axum::http::header::IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            state.seen.lock().unwrap().push(format!(
                "{} {} {authorization} {if_match} {if_none_match}",
                request.method(),
                request.uri().path()
            ));

            if request.method() == axum::http::Method::GET {
                return match &state.resource {
                    Some(resource) => {
                        let mut response = axum::Json(resource).into_response();
                        if state.include_etag {
                            response.headers_mut().insert(
                                axum::http::header::ETAG,
                                axum::http::HeaderValue::from_static("\"revision-7\""),
                            );
                        }
                        response
                    }
                    None => (
                        StatusCode::NOT_FOUND,
                        axum::Json(ApiError::new(
                            ApiErrorCode::StreamNotFound,
                            "stream not found",
                        )),
                    )
                        .into_response(),
                };
            }

            if state.mutation_status.is_success() {
                if request.method() == axum::http::Method::DELETE {
                    return StatusCode::NO_CONTENT.into_response();
                }
                return (
                    state.mutation_status,
                    axum::Json(StreamAccepted {
                        status: weave_core::AcceptedState::Accepted,
                        name: "cam1-to-studio".to_string(),
                        generation: state
                            .resource
                            .as_ref()
                            .map_or(1, |resource| resource.generation + 1),
                        changed: true,
                    }),
                )
                    .into_response();
            }

            let code = if state.mutation_status == StatusCode::PRECONDITION_REQUIRED {
                ApiErrorCode::PreconditionRequired
            } else {
                ApiErrorCode::PreconditionFailed
            };
            (
                state.mutation_status,
                axum::Json(ApiError::new(code, "stream revision precondition failed")),
            )
                .into_response()
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().fallback(record).with_state(ResourceStub {
            seen: Arc::clone(&seen),
            resource,
            mutation_status,
            include_etag,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    async fn stub_plan() -> (String, Arc<Mutex<Option<String>>>) {
        async fn record(
            State(seen): State<Arc<Mutex<Option<String>>>>,
            method: axum::http::Method,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
            axum::Json(stream): axum::Json<StreamDefinition>,
        ) -> axum::Json<StreamPlan> {
            let authorization = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            *seen.lock().unwrap() = Some(format!(
                "{method} {} {authorization} {}",
                uri.path(),
                stream.name
            ));
            axum::Json(StreamPlan {
                name: stream.name,
                status: weave_core::PlanStatus::Unplaced,
                nodes: Vec::new(),
                hops: Vec::new(),
                endpoints: None,
                reason: Some("node missing is not registered".to_string()),
            })
        }

        let seen = Arc::new(Mutex::new(None));
        let app = Router::new().fallback(record).with_state(Arc::clone(&seen));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn plan_calls_the_versioned_route_with_auth_and_body() {
        let token = Token::new("cli-test-token").unwrap();
        let stream = StreamDefinition {
            name: "preview".to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: Some("missing".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                format: None,
                accepts: None,
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                node: Some("also-missing".to_string()),
                remote: None,
                via: Vec::new(),
                network: None,
                latency: None,
                format: None,
                accepts: None,
            })],
        };
        let (url, seen) = stub_plan().await;

        plan_stream(&url, Some(&token), &stream).await.unwrap();

        assert_eq!(
            seen.lock().unwrap().as_deref(),
            Some("POST /v5/stream-plans Bearer cli-test-token preview")
        );
    }

    #[tokio::test]
    async fn get_stream_calls_the_versioned_route_with_auth() {
        let token = Token::new("cli-test-token").unwrap();
        let resource = StreamResource {
            generation: 7,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::ACCEPTED).await;

        get_stream(&url, Some(&token), "cam1-to-studio")
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["GET /v5/streams/cam1-to-studio Bearer cli-test-token - -"]
        );
    }

    #[tokio::test]
    async fn apply_creates_with_if_none_match_after_a_missing_lookup() {
        let token = Token::new("cli-test-token").unwrap();
        let (url, seen) = stub_resource(None, StatusCode::ACCEPTED).await;

        apply_stream(&url, Some(&token), &sample_stream())
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v5/streams/cam1-to-studio Bearer cli-test-token - -",
                "POST /v5/streams Bearer cli-test-token - *"
            ]
        );
    }

    #[tokio::test]
    async fn apply_updates_once_with_the_revision_it_read() {
        let token = Token::new("cli-test-token").unwrap();
        let resource = StreamResource {
            generation: 7,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::ACCEPTED).await;

        apply_stream(&url, Some(&token), &sample_stream())
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v5/streams/cam1-to-studio Bearer cli-test-token - -",
                "POST /v5/streams Bearer cli-test-token \"revision-7\" -"
            ]
        );
    }

    #[tokio::test]
    async fn apply_surfaces_a_structured_conflict_without_retrying() {
        let resource = StreamResource {
            generation: 7,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::PRECONDITION_FAILED).await;

        let error = apply_stream(&url, None, &sample_stream())
            .await
            .expect_err("a stale revision must fail");

        assert!(
            error.to_string().contains("[precondition_failed]"),
            "{error}"
        );
        assert_eq!(seen.lock().unwrap().len(), 2, "apply must not retry 412");
    }

    #[tokio::test]
    async fn apply_refuses_to_update_without_the_get_etag() {
        let resource = StreamResource {
            generation: 7,
            spec: sample_stream(),
        };
        let (url, seen) = start_resource_stub(Some(resource), StatusCode::ACCEPTED, false).await;

        let error = apply_stream(&url, None, &sample_stream())
            .await
            .expect_err("an update needs the revision ETag");

        assert!(error.to_string().contains("missing its ETag"), "{error}");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn lookup_decodes_the_resource_generation() {
        let resource = StreamResource {
            generation: 23,
            spec: sample_stream(),
        };
        let (url, _seen) = stub_resource(Some(resource), StatusCode::ACCEPTED).await;

        let found = lookup_stream(&reqwest::Client::new(), &url, None, "cam1-to-studio")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(found.resource.generation, 23);
    }

    #[tokio::test]
    async fn get_stream_rejects_an_unsafe_name_locally() {
        let error = get_stream("not a URL", None, "foo?ignored")
            .await
            .expect_err("unsafe name must be rejected locally");
        assert!(error.to_string().starts_with("invalid stream name:"));
    }

    #[tokio::test]
    async fn delete_calls_the_versioned_route_and_reports_an_unknown_stream() {
        let token = Token::new("cli-test-token").unwrap();

        let resource = StreamResource {
            generation: 7,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::NO_CONTENT).await;
        delete_stream(&url, Some(&token), "cam1-to-studio")
            .await
            .expect("204 deletes the stream");
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v5/streams/cam1-to-studio Bearer cli-test-token - -",
                "DELETE /v5/streams/cam1-to-studio Bearer cli-test-token \"revision-7\" -"
            ]
        );

        let (url, _seen) = stub_resource(None, StatusCode::NO_CONTENT).await;
        let err = delete_stream(&url, Some(&token), "missing")
            .await
            .expect_err("404 is an error");
        assert!(err.to_string().contains("missing"), "{err}");
    }

    #[tokio::test]
    async fn delete_rejects_an_unsafe_name_before_building_a_request() {
        let error = delete_stream("not a URL", None, "foo?ignored")
            .await
            .expect_err("unsafe name must be rejected locally");
        assert_eq!(
            error.to_string(),
            "invalid stream name: must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit"
        );
    }
}
