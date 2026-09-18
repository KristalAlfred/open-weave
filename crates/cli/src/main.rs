//! `weave` — operator CLI for the open-weave control plane.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::RequestBuilder;
use reqwest::header::{ETAG, HeaderValue, IF_MATCH, IF_NONE_MATCH};
use tracing_subscriber::EnvFilter;
use weave_core::auth::{self, Token};
use weave_core::{
    API_PREFIX, ApiError, NodeDescriptor, PathStatus, PlanStatus, ReconcileStatus, StatusResponse,
    StreamAccepted, StreamDefinition, StreamEndpoints, StreamPlan, StreamResource,
    StreamSetAccepted, StreamSetAction, StreamSetApply, StreamSetResource, validate_resource_id,
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
    /// Output format.
    #[arg(short, long, value_enum, default_value_t = OutputFormat::Human, global = true)]
    output: OutputFormat,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Human,
    Yaml,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Apply a stream definition (YAML) as desired state.
    Apply {
        /// Path to the stream YAML file.
        #[arg(short = 'f', long = "file")]
        file: PathBuf,
    },
    /// Atomically apply a stream ownership set (YAML) as desired state.
    ApplySet {
        /// Owner of the stream set.
        owner: String,
        /// Path to the stream-set YAML file.
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
    /// Get the current control-plane status.
    Status,
    /// List registered nodes.
    Nodes,
    /// Get the resolved endpoints for one stream.
    Endpoints {
        /// Name of the stream whose endpoints to get.
        name: String,
    },
    /// List stream ownership sets.
    StreamSets,
    /// Get one stream ownership set.
    StreamSet {
        /// Owner of the stream set to get.
        owner: String,
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
        output,
        command,
    } = Cli::parse();
    let token = token.as_deref().and_then(Token::new);

    match command {
        Command::Apply { file } => apply(&url, token.as_ref(), &file, output).await,
        Command::ApplySet { owner, file } => {
            apply_set(&url, token.as_ref(), &owner, &file, output).await
        }
        Command::Plan { file } => plan(&url, token.as_ref(), &file, output).await,
        Command::Get { resource } => match resource {
            GetResource::Streams => get_streams(&url, token.as_ref(), output).await,
            GetResource::Stream { name } => get_stream(&url, token.as_ref(), &name, output).await,
            GetResource::Status => get_status(&url, token.as_ref(), output).await,
            GetResource::Nodes => get_nodes(&url, token.as_ref(), output).await,
            GetResource::Endpoints { name } => {
                get_endpoints(&url, token.as_ref(), &name, output).await
            }
            GetResource::StreamSets => get_stream_sets(&url, token.as_ref(), output).await,
            GetResource::StreamSet { owner } => {
                get_stream_set(&url, token.as_ref(), &owner, output).await
            }
        },
        Command::Delete { resource } => match resource {
            DeleteResource::Stream { name } => {
                delete_stream(&url, token.as_ref(), &name, output).await
            }
        },
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

fn format_output(output: OutputFormat, value: &serde_json::Value, human: String) -> Result<String> {
    Ok(match output {
        OutputFormat::Human => human,
        OutputFormat::Yaml => serde_norway::to_string(&value).context("encoding YAML output")?,
        OutputFormat::Json => {
            serde_json::to_string_pretty(&value).context("encoding JSON output")?
        }
    })
}

fn emit(output: OutputFormat, value: serde_json::Value, human: String) -> Result<()> {
    let rendered = format_output(output, &value, human)?;
    println!("{}", rendered.trim_end());
    Ok(())
}

fn table(headers: &[&str], rows: Vec<Vec<String>>) -> String {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.len());
        }
    }

    let render_row = |row: Vec<String>| {
        row.into_iter()
            .enumerate()
            .map(|(index, value)| {
                if index + 1 == widths.len() {
                    value
                } else {
                    format!("{value:<width$}", width = widths[index])
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };

    let mut lines = vec![render_row(
        headers.iter().map(|header| (*header).to_string()).collect(),
    )];
    lines.extend(rows.into_iter().map(render_row));
    lines.join("\n")
}

fn stream_rows(streams: &[StreamResource]) -> Vec<Vec<String>> {
    let mut streams = streams.iter().collect::<Vec<_>>();
    streams.sort_by(|left, right| left.spec.name.cmp(&right.spec.name));
    streams
        .into_iter()
        .map(|stream| {
            vec![
                stream.spec.name.clone(),
                stream.generation.to_string(),
                stream.owner.clone().unwrap_or_else(|| "-".to_string()),
                stream.spec.enabled.to_string(),
            ]
        })
        .collect()
}

fn render_streams(streams: &[StreamResource]) -> String {
    table(
        &["NAME", "GENERATION", "OWNER", "ENABLED"],
        stream_rows(streams),
    )
}

fn render_nodes(nodes: &[NodeDescriptor]) -> String {
    let mut nodes = nodes.iter().collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    let rows = nodes
        .into_iter()
        .map(|node| {
            let transports = if node.capabilities.transports.is_empty() {
                "srt".to_string()
            } else {
                node.capabilities
                    .transports
                    .iter()
                    .map(|offer| offer.name.name())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            vec![
                node.id.clone(),
                format!("{:?}", node.status).to_ascii_lowercase(),
                node.endpoint.clone(),
                transports,
                node.capabilities.relay.to_string(),
            ]
        })
        .collect();
    table(&["ID", "STATUS", "ENDPOINT", "TRANSPORTS", "RELAY"], rows)
}

fn path_status_name(status: PathStatus) -> &'static str {
    match status {
        PathStatus::Idle => "idle",
        PathStatus::Failed => "failed",
        PathStatus::Pending => "pending",
        PathStatus::AwaitingInput => "awaiting_input",
        PathStatus::Degraded => "degraded",
        PathStatus::Flowing => "flowing",
    }
}

fn reconcile_status_name(status: ReconcileStatus) -> &'static str {
    match status {
        ReconcileStatus::Idle => "idle",
        ReconcileStatus::Converging => "converging",
        ReconcileStatus::Converged => "converged",
        ReconcileStatus::Degraded => "degraded",
    }
}

fn render_status(status: &StatusResponse) -> String {
    match status {
        StatusResponse::Starting(_) => "STATUS\nstarting".to_string(),
        StatusResponse::Running(status) => {
            let mut streams = status.streams.iter().collect::<Vec<_>>();
            streams.sort_by(|left, right| left.name.cmp(&right.name));
            let rows = streams
                .into_iter()
                .map(|stream| {
                    vec![
                        stream.name.clone(),
                        stream.generation.to_string(),
                        stream
                            .observed_generation
                            .map_or_else(|| "-".to_string(), |value| value.to_string()),
                        path_status_name(stream.status).to_string(),
                        if stream.nodes.is_empty() {
                            "-".to_string()
                        } else {
                            stream.nodes.join(",")
                        },
                    ]
                })
                .collect();
            format!(
                "STATUS: {}\nSUMMARY: {}\n\n{}",
                reconcile_status_name(status.status),
                status.summary,
                table(&["NAME", "GENERATION", "OBSERVED", "STATUS", "NODES"], rows,)
            )
        }
    }
}

fn render_endpoints(endpoints: &StreamEndpoints) -> String {
    let mut rows = Vec::with_capacity(endpoints.outputs.len() + 1);
    rows.push(match &endpoints.ingress {
        Some(endpoint) => vec![
            "ingress".to_string(),
            "-".to_string(),
            endpoint.node.clone(),
            endpoint.url.clone(),
        ],
        None => vec![
            "ingress".to_string(),
            "-".to_string(),
            "-".to_string(),
            "-".to_string(),
        ],
    });
    rows.extend(
        endpoints
            .outputs
            .iter()
            .enumerate()
            .map(|(index, endpoint)| match endpoint {
                Some(endpoint) => vec![
                    "output".to_string(),
                    index.to_string(),
                    endpoint.node.clone(),
                    endpoint.url.clone(),
                ],
                None => vec![
                    "output".to_string(),
                    index.to_string(),
                    "-".to_string(),
                    "-".to_string(),
                ],
            }),
    );
    table(&["ROLE", "INDEX", "NODE", "URL"], rows)
}

fn render_stream_sets(stream_sets: &[StreamSetResource]) -> String {
    let mut sets = stream_sets.iter().collect::<Vec<_>>();
    sets.sort_by(|left, right| left.owner.cmp(&right.owner));
    table(
        &["OWNER", "STREAMS"],
        sets.into_iter()
            .map(|set| vec![set.owner.clone(), set.streams.len().to_string()])
            .collect(),
    )
}

fn render_stream_set(stream_set: &StreamSetResource) -> String {
    format!(
        "OWNER: {}\n\n{}",
        stream_set.owner,
        render_streams(&stream_set.streams)
    )
}

fn render_stream_set_accepted(accepted: &StreamSetAccepted) -> String {
    let mut rows = accepted
        .streams
        .iter()
        .map(|stream| {
            let action = match stream.action {
                StreamSetAction::Created => "created",
                StreamSetAction::Updated => "updated",
                StreamSetAction::Unchanged => "unchanged",
            };
            vec![
                stream.name.clone(),
                action.to_string(),
                stream.generation.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    rows.extend(
        accepted
            .pruned
            .iter()
            .map(|name| vec![name.clone(), "pruned".to_string(), "-".to_string()]),
    );
    format!(
        "OWNER: {}\nCHANGED: {}\n\n{}",
        accepted.owner,
        accepted.changed,
        table(&["NAME", "ACTION", "GENERATION"], rows)
    )
}

fn render_plan(plan: &StreamPlan) -> String {
    let status = match plan.status {
        PlanStatus::Disabled => "disabled",
        PlanStatus::Placed => "placed",
        PlanStatus::Unplaced => "unplaced",
    };
    table(
        &["NAME", "STATUS", "NODES", "REASON"],
        vec![vec![
            plan.name.clone(),
            status.to_string(),
            if plan.nodes.is_empty() {
                "-".to_string()
            } else {
                plan.nodes.join(",")
            },
            plan.reason.clone().unwrap_or_else(|| "-".to_string()),
        ]],
    )
}

async fn get_body(
    client: &reqwest::Client,
    url: &str,
    token: Option<&Token>,
    path: &str,
    label: &str,
) -> Result<String> {
    let response = authorized(client.get(api_url(url, path)), token)
        .send()
        .await
        .with_context(|| format!("fetching {label}"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
            unauthorized_hint(token)
        } else {
            ""
        };
        bail!(
            "northbound {label} request failed: {status}: {}{hint}",
            error_detail(&body)
        );
    }
    Ok(body)
}

fn parse_stream(yaml: &str) -> Result<StreamDefinition> {
    serde_norway::with::singleton_map_recursive::deserialize(serde_norway::Deserializer::from_str(
        yaml,
    ))
    .context("parsing stream YAML")
}

fn parse_stream_set(yaml: &str) -> Result<StreamSetApply> {
    serde_norway::with::singleton_map_recursive::deserialize(serde_norway::Deserializer::from_str(
        yaml,
    ))
    .context("parsing stream-set YAML")
}

async fn apply(url: &str, token: Option<&Token>, file: &Path, output: OutputFormat) -> Result<()> {
    let stream = read_stream(file)?;
    apply_stream(url, token, &stream, output).await
}

async fn apply_stream(
    url: &str,
    token: Option<&Token>,
    stream: &StreamDefinition,
    output: OutputFormat,
) -> Result<()> {
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
    let accepted: StreamAccepted =
        serde_json::from_str(&body).context("decoding accepted stream")?;
    let human = table(
        &["NAME", "GENERATION", "CHANGED"],
        vec![vec![
            accepted.name.clone(),
            accepted.generation.to_string(),
            accepted.changed.to_string(),
        ]],
    );
    emit(output, serde_json::to_value(&accepted)?, human)
}

fn read_stream(file: &Path) -> Result<StreamDefinition> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    parse_stream(&text).with_context(|| format!("parsing stream from {}", file.display()))
}

fn read_stream_set(file: &Path) -> Result<StreamSetApply> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    parse_stream_set(&text).with_context(|| format!("parsing stream set from {}", file.display()))
}

async fn apply_set(
    url: &str,
    token: Option<&Token>,
    owner: &str,
    file: &Path,
    output: OutputFormat,
) -> Result<()> {
    let apply = read_stream_set(file)?;
    apply_stream_set(url, token, owner, &apply, output).await
}

async fn apply_stream_set(
    url: &str,
    token: Option<&Token>,
    owner: &str,
    apply: &StreamSetApply,
    output: OutputFormat,
) -> Result<()> {
    if let Err(error) = validate_resource_id(owner) {
        bail!("invalid stream-set owner: {error}");
    }
    let client = reqwest::Client::new();
    let existing = lookup_stream_set(&client, url, token, owner).await?;
    let request = authorized(
        client.put(api_url(url, &format!("/stream-sets/{owner}"))),
        token,
    )
    .json(apply);
    let request = match existing {
        Some(existing) => request.header(IF_MATCH, existing.etag),
        None => request.header(IF_NONE_MATCH, "*"),
    };
    let response = request
        .send()
        .await
        .context("putting stream set to northbound")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let hint = if status == reqwest::StatusCode::UNAUTHORIZED {
            unauthorized_hint(token)
        } else {
            ""
        };
        bail!(
            "northbound rejected stream set: {status}: {}{hint}",
            error_detail(&body)
        );
    }

    let accepted: StreamSetAccepted =
        serde_json::from_str(&body).context("decoding accepted stream set")?;
    let human = render_stream_set_accepted(&accepted);
    emit(output, serde_json::to_value(&accepted)?, human)
}

async fn plan(url: &str, token: Option<&Token>, file: &Path, output: OutputFormat) -> Result<()> {
    let stream = read_stream(file)?;
    plan_stream(url, token, &stream, output).await
}

async fn plan_stream(
    url: &str,
    token: Option<&Token>,
    stream: &StreamDefinition,
    output: OutputFormat,
) -> Result<()> {
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
    let human = render_plan(&plan);
    emit(output, serde_json::to_value(&plan)?, human)
}

async fn get_streams(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<()> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/streams", "streams").await?;
    let streams: Vec<StreamResource> = serde_json::from_str(&body).context("decoding streams")?;
    let human = render_streams(&streams);
    emit(output, serde_json::to_value(&streams)?, human)
}

async fn get_stream(
    url: &str,
    token: Option<&Token>,
    name: &str,
    output: OutputFormat,
) -> Result<()> {
    if let Err(error) = validate_resource_id(name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let Some(stream) = lookup_stream(&client, url, token, name).await? else {
        bail!("no stream named {name}");
    };
    let human = render_streams(std::slice::from_ref(&stream.resource));
    emit(output, serde_json::to_value(&stream.resource)?, human)
}

async fn get_status(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<()> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/status", "status").await?;
    let status: StatusResponse = serde_json::from_str(&body).context("decoding status")?;
    let human = render_status(&status);
    emit(output, serde_json::to_value(&status)?, human)
}

async fn get_nodes(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<()> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/nodes", "nodes").await?;
    let nodes: Vec<NodeDescriptor> = serde_json::from_str(&body).context("decoding nodes")?;
    let human = render_nodes(&nodes);
    emit(output, serde_json::to_value(&nodes)?, human)
}

async fn get_endpoints(
    url: &str,
    token: Option<&Token>,
    name: &str,
    output: OutputFormat,
) -> Result<()> {
    if let Err(error) = validate_resource_id(name) {
        bail!("invalid stream name: {error}");
    }
    let client = reqwest::Client::new();
    let body = get_body(
        &client,
        url,
        token,
        &format!("/streams/{name}/endpoints"),
        "stream endpoints",
    )
    .await?;
    let endpoints: StreamEndpoints =
        serde_json::from_str(&body).context("decoding stream endpoints")?;
    let human = render_endpoints(&endpoints);
    emit(output, serde_json::to_value(&endpoints)?, human)
}

async fn get_stream_sets(url: &str, token: Option<&Token>, output: OutputFormat) -> Result<()> {
    let client = reqwest::Client::new();
    let body = get_body(&client, url, token, "/stream-sets", "stream sets").await?;
    let stream_sets: Vec<StreamSetResource> =
        serde_json::from_str(&body).context("decoding stream sets")?;
    let human = render_stream_sets(&stream_sets);
    emit(output, serde_json::to_value(&stream_sets)?, human)
}

async fn get_stream_set(
    url: &str,
    token: Option<&Token>,
    owner: &str,
    output: OutputFormat,
) -> Result<()> {
    if let Err(error) = validate_resource_id(owner) {
        bail!("invalid stream-set owner: {error}");
    }
    let client = reqwest::Client::new();
    let body = get_body(
        &client,
        url,
        token,
        &format!("/stream-sets/{owner}"),
        "stream set",
    )
    .await?;
    let stream_set: StreamSetResource =
        serde_json::from_str(&body).context("decoding stream set")?;
    let human = render_stream_set(&stream_set);
    emit(output, serde_json::to_value(&stream_set)?, human)
}

async fn delete_stream(
    url: &str,
    token: Option<&Token>,
    name: &str,
    output: OutputFormat,
) -> Result<()> {
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
    emit(
        output,
        serde_json::json!({ "deleted": name }),
        format!("deleted stream {name}"),
    )
}

struct StreamLookup {
    resource: StreamResource,
    etag: HeaderValue,
}

struct StreamSetLookup {
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

async fn lookup_stream_set(
    client: &reqwest::Client,
    url: &str,
    token: Option<&Token>,
    owner: &str,
) -> Result<Option<StreamSetLookup>> {
    let response = authorized(
        client.get(api_url(url, &format!("/stream-sets/{owner}"))),
        token,
    )
    .send()
    .await
    .context("fetching stream set")?;

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
            "northbound stream-set request failed: {status}: {}{hint}",
            error_detail(&body)
        );
    }
    let etag = etag.context("northbound stream-set response is missing its ETag")?;
    serde_json::from_str::<StreamSetResource>(&body).context("decoding stream set")?;
    Ok(Some(StreamSetLookup { etag }))
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
    use axum::body::{Body, Bytes};
    use axum::extract::State;
    use axum::http::{Request, StatusCode};
    use axum::response::{IntoResponse, Response};
    use serde_json::Value;
    use std::sync::{Arc, Mutex};
    use weave_core::{
        ApiErrorCode, SrtEndpoint, StreamAccepted, StreamSetMemberResult, StreamTransport,
    };

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
            "http://127.0.0.1:9080/v6/streams"
        );
        assert_eq!(
            api_url("http://127.0.0.1:9080/", "/streams"),
            "http://127.0.0.1:9080/v6/streams",
            "a trailing slash on the base does not double up"
        );
    }

    #[test]
    fn parses_read_commands_and_output_formats() {
        let cases = [
            (vec!["weave", "get", "nodes"], "nodes"),
            (vec!["weave", "get", "status"], "status"),
            (
                vec!["weave", "get", "endpoints", "cam1-to-studio"],
                "endpoints",
            ),
            (vec!["weave", "get", "stream-sets"], "stream-sets"),
            (vec!["weave", "get", "stream-set", "studio-a"], "stream-set"),
        ];

        for (args, expected) in cases {
            let cli = Cli::try_parse_from(args).unwrap();
            assert_eq!(cli.output, OutputFormat::Human);
            let Command::Get { resource } = cli.command else {
                panic!("expected get command");
            };
            let actual = match resource {
                GetResource::Nodes => "nodes",
                GetResource::Status => "status",
                GetResource::Endpoints { .. } => "endpoints",
                GetResource::StreamSets => "stream-sets",
                GetResource::StreamSet { .. } => "stream-set",
                GetResource::Streams | GetResource::Stream { .. } => "unexpected",
            };
            assert_eq!(actual, expected);
        }

        let cli = Cli::try_parse_from(["weave", "-o", "yaml", "get", "streams"]).unwrap();
        assert_eq!(cli.output, OutputFormat::Yaml);
        let cli = Cli::try_parse_from(["weave", "get", "streams", "--output", "json"]).unwrap();
        assert_eq!(cli.output, OutputFormat::Json);
        assert!(Cli::try_parse_from(["weave", "nodes"]).is_err());
    }

    #[test]
    fn parses_apply_set_command_and_strict_yaml() {
        let cli = Cli::try_parse_from([
            "weave",
            "apply-set",
            "studio-a",
            "-f",
            "streams.yaml",
            "--output",
            "json",
        ])
        .unwrap();
        assert_eq!(cli.output, OutputFormat::Json);
        let Command::ApplySet { owner, file } = cli.command else {
            panic!("expected apply-set command");
        };
        assert_eq!(owner, "studio-a");
        assert_eq!(file, PathBuf::from("streams.yaml"));

        let yaml = r#"
streams:
  - name: cam1-to-studio
    source:
      srt: { node: strom-node-1 }
    destinations:
      - srt: { node: strom-node-2 }
"#;
        let apply = parse_stream_set(yaml).unwrap();
        assert!(!apply.prune);
        assert_eq!(apply.streams, vec![sample_stream()]);

        let invalid = format!("{yaml}unknown: true\n");
        assert!(parse_stream_set(&invalid).is_err());
    }

    #[test]
    fn json_and_yaml_outputs_preserve_the_typed_value() {
        let value = serde_json::json!({
            "generation": 3,
            "owner": "studio-a",
            "spec": { "name": "cam1-to-studio" }
        });

        let json = format_output(OutputFormat::Json, &value, String::new()).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&json).unwrap(), value);

        let yaml = format_output(OutputFormat::Yaml, &value, String::new()).unwrap();
        assert_eq!(serde_norway::from_str::<Value>(&yaml).unwrap(), value);
    }

    #[test]
    fn human_stream_and_endpoint_tables_are_stable() {
        let streams = vec![StreamResource {
            generation: 3,
            owner: Some("studio-a".to_string()),
            spec: sample_stream(),
        }];
        assert_eq!(
            render_streams(&streams),
            "NAME            GENERATION  OWNER     ENABLED\ncam1-to-studio  3           studio-a  true"
        );

        let endpoints: StreamEndpoints = serde_json::from_value(serde_json::json!({
            "ingress": {
                "node": "strom-node-1",
                "host": "172.26.0.10",
                "port": 20000,
                "url": "srt://172.26.0.10:20000"
            },
            "outputs": [null]
        }))
        .unwrap();
        assert_eq!(
            render_endpoints(&endpoints),
            "ROLE     INDEX  NODE          URL\ningress  -      strom-node-1  srt://172.26.0.10:20000\noutput   0      -             -"
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

    #[derive(Clone)]
    struct StreamSetStub {
        seen: Arc<Mutex<Vec<String>>>,
        existing: bool,
        mutation_status: StatusCode,
    }

    async fn stub_stream_set(
        existing: bool,
        mutation_status: StatusCode,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        async fn record(
            State(state): State<StreamSetStub>,
            method: axum::http::Method,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
            body: Bytes,
        ) -> Response {
            let if_match = headers
                .get(axum::http::header::IF_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            let if_none_match = headers
                .get(axum::http::header::IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("-");
            let body_summary = if method == axum::http::Method::PUT {
                let apply: StreamSetApply = serde_json::from_slice(&body).unwrap();
                format!("{} {}", apply.streams.len(), apply.prune)
            } else {
                "- -".to_string()
            };
            state.seen.lock().unwrap().push(format!(
                "{method} {} {if_match} {if_none_match} {body_summary}",
                uri.path()
            ));

            if method == axum::http::Method::GET {
                if !state.existing {
                    return (
                        StatusCode::NOT_FOUND,
                        axum::Json(ApiError::new(
                            ApiErrorCode::StreamSetNotFound,
                            "stream set not found",
                        )),
                    )
                        .into_response();
                }
                let mut response = axum::Json(StreamSetResource {
                    owner: "studio-a".to_string(),
                    streams: vec![StreamResource {
                        generation: 7,
                        owner: Some("studio-a".to_string()),
                        spec: sample_stream(),
                    }],
                })
                .into_response();
                response.headers_mut().insert(
                    axum::http::header::ETAG,
                    axum::http::HeaderValue::from_static("\"set-revision-7\""),
                );
                return response;
            }

            if state.mutation_status.is_success() {
                return (
                    state.mutation_status,
                    axum::Json(StreamSetAccepted {
                        status: weave_core::AcceptedState::Accepted,
                        owner: "studio-a".to_string(),
                        changed: true,
                        streams: vec![StreamSetMemberResult {
                            name: "cam1-to-studio".to_string(),
                            generation: if state.existing { 8 } else { 1 },
                            action: if state.existing {
                                StreamSetAction::Updated
                            } else {
                                StreamSetAction::Created
                            },
                        }],
                        pruned: Vec::new(),
                    }),
                )
                    .into_response();
            }

            let code = if state.mutation_status == StatusCode::CONFLICT {
                ApiErrorCode::OwnershipConflict
            } else {
                ApiErrorCode::PreconditionFailed
            };
            (
                state.mutation_status,
                axum::Json(ApiError::new(code, "stream-set mutation failed")),
            )
                .into_response()
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().fallback(record).with_state(StreamSetStub {
            seen: Arc::clone(&seen),
            existing,
            mutation_status,
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

    #[derive(Clone)]
    struct GetStub {
        seen: Arc<Mutex<Option<String>>>,
        status: StatusCode,
        body: Value,
    }

    async fn stub_get(status: StatusCode, body: Value) -> (String, Arc<Mutex<Option<String>>>) {
        async fn record(
            State(state): State<GetStub>,
            method: axum::http::Method,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
        ) -> Response {
            let authorization = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            *state.seen.lock().unwrap() = Some(format!("{method} {} {authorization}", uri.path()));
            (state.status, axum::Json(state.body)).into_response()
        }

        let seen = Arc::new(Mutex::new(None));
        let app = Router::new().fallback(record).with_state(GetStub {
            seen: Arc::clone(&seen),
            status,
            body,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    #[tokio::test]
    async fn read_commands_call_their_versioned_routes_with_auth() {
        let token = Token::new("cli-test-token").unwrap();
        let cases = [
            (
                "/v6/nodes",
                serde_json::json!([{
                    "id": "strom-node-1",
                    "endpoint": "http://strom-node-1:8091",
                    "status": "ready"
                }]),
                "nodes",
            ),
            (
                "/v6/status",
                serde_json::json!({ "status": "starting" }),
                "status",
            ),
            (
                "/v6/streams/cam1-to-studio/endpoints",
                serde_json::json!({ "ingress": null, "outputs": [null] }),
                "endpoints",
            ),
            ("/v6/stream-sets", serde_json::json!([]), "stream-sets"),
            (
                "/v6/stream-sets/studio-a",
                serde_json::json!({ "owner": "studio-a", "streams": [] }),
                "stream-set",
            ),
        ];

        for (path, body, command) in cases {
            let (url, seen) = stub_get(StatusCode::OK, body).await;
            match command {
                "nodes" => get_nodes(&url, Some(&token), OutputFormat::Json)
                    .await
                    .unwrap(),
                "status" => get_status(&url, Some(&token), OutputFormat::Json)
                    .await
                    .unwrap(),
                "endpoints" => {
                    get_endpoints(&url, Some(&token), "cam1-to-studio", OutputFormat::Json)
                        .await
                        .unwrap()
                }
                "stream-sets" => get_stream_sets(&url, Some(&token), OutputFormat::Json)
                    .await
                    .unwrap(),
                "stream-set" => get_stream_set(&url, Some(&token), "studio-a", OutputFormat::Json)
                    .await
                    .unwrap(),
                _ => unreachable!(),
            }
            assert_eq!(
                seen.lock().unwrap().as_deref(),
                Some(format!("GET {path} Bearer cli-test-token").as_str())
            );
        }
    }

    #[tokio::test]
    async fn get_commands_surface_structured_errors() {
        let error = serde_json::json!({
            "code": "controller_unreachable",
            "message": "controller unreachable",
            "details": [{
                "field": "controller",
                "code": "unavailable",
                "message": "try again later"
            }]
        });
        let (url, _seen) = stub_get(StatusCode::BAD_GATEWAY, error).await;

        let error = get_nodes(&url, None, OutputFormat::Human)
            .await
            .expect_err("a failed GET must preserve the structured error");
        assert!(
            error
                .to_string()
                .contains("[controller_unreachable] controller unreachable")
        );
        assert!(
            error
                .to_string()
                .contains("controller [unavailable]: try again later")
        );
    }

    #[tokio::test]
    async fn endpoint_and_stream_set_ids_are_validated_locally() {
        let endpoint_error = get_endpoints("not a URL", None, "foo?ignored", OutputFormat::Human)
            .await
            .expect_err("unsafe stream name must be rejected locally");
        assert!(
            endpoint_error
                .to_string()
                .starts_with("invalid stream name:")
        );

        let owner_error = get_stream_set("not a URL", None, "foo?ignored", OutputFormat::Human)
            .await
            .expect_err("unsafe owner must be rejected locally");
        assert!(
            owner_error
                .to_string()
                .starts_with("invalid stream-set owner:")
        );
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

        plan_stream(&url, Some(&token), &stream, OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_deref(),
            Some("POST /v6/stream-plans Bearer cli-test-token preview")
        );
    }

    #[tokio::test]
    async fn get_stream_calls_the_versioned_route_with_auth() {
        let token = Token::new("cli-test-token").unwrap();
        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::ACCEPTED).await;

        get_stream(&url, Some(&token), "cam1-to-studio", OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            ["GET /v6/streams/cam1-to-studio Bearer cli-test-token - -"]
        );
    }

    #[tokio::test]
    async fn apply_set_creates_with_one_conditional_put() {
        let apply = StreamSetApply {
            streams: vec![sample_stream()],
            prune: true,
        };
        let (url, seen) = stub_stream_set(false, StatusCode::ACCEPTED).await;

        apply_stream_set(&url, None, "studio-a", &apply, OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v6/stream-sets/studio-a - - - -",
                "PUT /v6/stream-sets/studio-a - * 1 true"
            ]
        );
    }

    #[tokio::test]
    async fn apply_set_updates_with_the_set_etag() {
        let apply = StreamSetApply {
            streams: vec![sample_stream()],
            prune: false,
        };
        let (url, seen) = stub_stream_set(true, StatusCode::ACCEPTED).await;

        apply_stream_set(&url, None, "studio-a", &apply, OutputFormat::Human)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v6/stream-sets/studio-a - - - -",
                "PUT /v6/stream-sets/studio-a \"set-revision-7\" - 1 false"
            ]
        );
    }

    #[tokio::test]
    async fn apply_set_surfaces_conflicts_without_retrying() {
        let apply = StreamSetApply {
            streams: vec![sample_stream()],
            prune: false,
        };

        for (status, code) in [
            (StatusCode::CONFLICT, "ownership_conflict"),
            (StatusCode::PRECONDITION_FAILED, "precondition_failed"),
        ] {
            let (url, seen) = stub_stream_set(true, status).await;
            let error = apply_stream_set(&url, None, "studio-a", &apply, OutputFormat::Json)
                .await
                .expect_err("conflicting set apply must fail");
            assert!(error.to_string().contains(&format!("[{code}]")), "{error}");
            assert_eq!(seen.lock().unwrap().len(), 2, "apply-set must not retry");
        }
    }

    #[tokio::test]
    async fn apply_set_rejects_an_unsafe_owner_locally() {
        let apply = StreamSetApply {
            streams: Vec::new(),
            prune: true,
        };
        let error = apply_stream_set(
            "not a URL",
            None,
            "foo?ignored",
            &apply,
            OutputFormat::Human,
        )
        .await
        .expect_err("unsafe owner must be rejected locally");
        assert!(error.to_string().starts_with("invalid stream-set owner:"));
    }

    #[test]
    fn human_apply_set_output_lists_member_actions_and_prunes() {
        let accepted = StreamSetAccepted {
            status: weave_core::AcceptedState::Accepted,
            owner: "studio-a".to_string(),
            changed: true,
            streams: vec![StreamSetMemberResult {
                name: "cam1-to-studio".to_string(),
                generation: 2,
                action: StreamSetAction::Updated,
            }],
            pruned: vec!["old-feed".to_string()],
        };

        assert_eq!(
            render_stream_set_accepted(&accepted),
            "OWNER: studio-a\nCHANGED: true\n\nNAME            ACTION   GENERATION\ncam1-to-studio  updated  2\nold-feed        pruned   -"
        );
    }

    #[tokio::test]
    async fn apply_creates_with_if_none_match_after_a_missing_lookup() {
        let token = Token::new("cli-test-token").unwrap();
        let (url, seen) = stub_resource(None, StatusCode::ACCEPTED).await;

        apply_stream(&url, Some(&token), &sample_stream(), OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v6/streams/cam1-to-studio Bearer cli-test-token - -",
                "POST /v6/streams Bearer cli-test-token - *"
            ]
        );
    }

    #[tokio::test]
    async fn apply_updates_once_with_the_revision_it_read() {
        let token = Token::new("cli-test-token").unwrap();
        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::ACCEPTED).await;

        apply_stream(&url, Some(&token), &sample_stream(), OutputFormat::Json)
            .await
            .unwrap();

        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v6/streams/cam1-to-studio Bearer cli-test-token - -",
                "POST /v6/streams Bearer cli-test-token \"revision-7\" -"
            ]
        );
    }

    #[tokio::test]
    async fn apply_surfaces_a_structured_conflict_without_retrying() {
        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::PRECONDITION_FAILED).await;

        let error = apply_stream(&url, None, &sample_stream(), OutputFormat::Json)
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
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = start_resource_stub(Some(resource), StatusCode::ACCEPTED, false).await;

        let error = apply_stream(&url, None, &sample_stream(), OutputFormat::Json)
            .await
            .expect_err("an update needs the revision ETag");

        assert!(error.to_string().contains("missing its ETag"), "{error}");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn lookup_decodes_the_resource_generation() {
        let resource = StreamResource {
            generation: 23,
            owner: None,
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
        let error = get_stream("not a URL", None, "foo?ignored", OutputFormat::Json)
            .await
            .expect_err("unsafe name must be rejected locally");
        assert!(error.to_string().starts_with("invalid stream name:"));
    }

    #[tokio::test]
    async fn delete_calls_the_versioned_route_and_reports_an_unknown_stream() {
        let token = Token::new("cli-test-token").unwrap();

        let resource = StreamResource {
            generation: 7,
            owner: None,
            spec: sample_stream(),
        };
        let (url, seen) = stub_resource(Some(resource), StatusCode::NO_CONTENT).await;
        delete_stream(&url, Some(&token), "cam1-to-studio", OutputFormat::Json)
            .await
            .expect("204 deletes the stream");
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [
                "GET /v6/streams/cam1-to-studio Bearer cli-test-token - -",
                "DELETE /v6/streams/cam1-to-studio Bearer cli-test-token \"revision-7\" -"
            ]
        );

        let (url, _seen) = stub_resource(None, StatusCode::NO_CONTENT).await;
        let err = delete_stream(&url, Some(&token), "missing", OutputFormat::Json)
            .await
            .expect_err("404 is an error");
        assert!(err.to_string().contains("missing"), "{err}");
    }

    #[tokio::test]
    async fn delete_rejects_an_unsafe_name_before_building_a_request() {
        let error = delete_stream("not a URL", None, "foo?ignored", OutputFormat::Json)
            .await
            .expect_err("unsafe name must be rejected locally");
        assert_eq!(
            error.to_string(),
            "invalid stream name: must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit"
        );
    }
}
