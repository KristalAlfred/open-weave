//! `weave` — operator CLI for the open-weave control plane.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use reqwest::RequestBuilder;
use tracing_subscriber::EnvFilter;
use weave_core::auth::{self, Token};
use weave_core::{API_V1, StreamDefinition};

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
        Command::Get { resource } => match resource {
            GetResource::Streams => get_streams(&url, token.as_ref()).await,
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

fn parse_stream(yaml: &str) -> Result<StreamDefinition> {
    serde_norway::with::singleton_map_recursive::deserialize(serde_norway::Deserializer::from_str(
        yaml,
    ))
    .context("parsing stream YAML")
}

async fn apply(url: &str, token: Option<&Token>, file: &Path) -> Result<()> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let stream =
        parse_stream(&text).with_context(|| format!("parsing stream from {}", file.display()))?;

    let response = authorized(reqwest::Client::new().post(api_url(url, "/streams")), token)
        .json(&stream)
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
        bail!("northbound rejected stream: {status}: {body}{hint}");
    }

    tracing::info!(name = %stream.name, %status, "stream applied");
    println!("{body}");
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
        .json::<Vec<StreamDefinition>>()
        .await
        .context("decoding streams")?;

    println!("{}", serde_json::to_string_pretty(&streams)?);
    Ok(())
}

async fn delete_stream(url: &str, token: Option<&Token>, name: &str) -> Result<()> {
    let response = authorized(
        reqwest::Client::new().delete(api_url(url, &format!("/streams/{name}"))),
        token,
    )
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
        bail!("northbound rejected delete: {status}: {body}{hint}");
    }

    tracing::info!(%name, %status, "stream deleted");
    println!("deleted stream {name}");
    Ok(())
}

fn nodes() -> Result<()> {
    tracing::info!("nodes: not implemented");
    Ok(())
}

/// Build a northbound API URL from a contract-relative `path`, inserting the
/// version prefix so the literal lives only in [`weave_core::API_V1`].
fn api_url(base: &str, path: &str) -> String {
    format!("{}{}{path}", base.trim_end_matches('/'), API_V1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{Request, StatusCode};
    use std::sync::{Arc, Mutex};
    use weave_core::{SrtEndpoint, StreamTransport};

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
    fn api_url_inserts_the_version_prefix_once() {
        assert_eq!(
            api_url("http://127.0.0.1:9080", "/streams"),
            "http://127.0.0.1:9080/v1/streams"
        );
        assert_eq!(
            api_url("http://127.0.0.1:9080/", "/streams"),
            "http://127.0.0.1:9080/v1/streams",
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

    /// A stub northbound on a real socket, recording the request line and
    /// authorization of the last request and answering with a canned status.
    async fn stub_northbound(status: StatusCode) -> (String, Arc<Mutex<Option<String>>>) {
        async fn record(
            State((seen, status)): State<(Arc<Mutex<Option<String>>>, StatusCode)>,
            request: Request<Body>,
        ) -> StatusCode {
            let authorization = request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_string();
            *seen.lock().unwrap() = Some(format!(
                "{} {} {authorization}",
                request.method(),
                request.uri().path()
            ));
            status
        }

        let seen = Arc::new(Mutex::new(None));
        let app = Router::new()
            .fallback(record)
            .with_state((Arc::clone(&seen), status));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn delete_calls_the_versioned_route_and_reports_an_unknown_stream() {
        let token = Token::new("cli-test-token").unwrap();

        let (url, seen) = stub_northbound(StatusCode::NO_CONTENT).await;
        delete_stream(&url, Some(&token), "cam1-to-studio")
            .await
            .expect("204 deletes the stream");
        assert_eq!(
            seen.lock().unwrap().as_deref(),
            Some("DELETE /v1/streams/cam1-to-studio Bearer cli-test-token")
        );

        let (url, _seen) = stub_northbound(StatusCode::NOT_FOUND).await;
        let err = delete_stream(&url, Some(&token), "missing")
            .await
            .expect_err("404 is an error");
        assert!(err.to_string().contains("missing"), "{err}");
    }
}
