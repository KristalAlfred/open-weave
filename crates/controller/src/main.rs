//! `weave-controller` — reconciles desired streams into per-node desired hops.

mod path;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use clap::Parser;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::{
    DesiredHop, ObservedState, PathStatus, ReconcileReport, ReconcileStatus, StreamDefinition,
};

use path::{StreamEndpoints, derive_path, path_status, stream_endpoints};

#[derive(Debug, Parser)]
#[command(name = "weave-controller", version, about = "open-weave reconciler")]
struct Args {
    #[arg(
        long,
        env = "WEAVE_NORTHBOUND_URL",
        default_value = "http://127.0.0.1:9080"
    )]
    northbound_url: String,
    #[arg(
        long,
        env = "WEAVE_SOUTHBOUND_URL",
        default_value = "http://127.0.0.1:8081"
    )]
    southbound_url: String,
    #[arg(long, env = "WEAVE_CONTROLLER_ADDR", default_value = "127.0.0.1:8082")]
    listen: String,
    #[arg(long, env = "WEAVE_RECONCILE_INTERVAL_SECS", default_value_t = 5)]
    interval_secs: u64,
}

/// Latest reconcile snapshot served by the read-only status/discovery API.
#[derive(Default)]
struct ControllerView {
    status: Value,
    streams: BTreeSet<String>,
    endpoints: BTreeMap<String, StreamEndpoints>,
}

type SharedView = Arc<RwLock<ControllerView>>;

#[derive(Debug, Serialize)]
struct StreamStatus {
    name: String,
    status: PathStatus,
    nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoints: Option<StreamEndpoints>,
}

struct ReconcileOutcome {
    report: ReconcileReport,
    streams: Vec<StreamStatus>,
    endpoints: BTreeMap<String, StreamEndpoints>,
}

impl ReconcileOutcome {
    fn status_json(&self) -> Value {
        json!({
            "status": self.report.status,
            "summary": self.report.summary,
            "streams": self.streams,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let http = reqwest::Client::new();
    let interval = Duration::from_secs(args.interval_secs);

    let view: SharedView = Arc::new(RwLock::new(ControllerView {
        status: json!({ "status": "starting" }),
        ..ControllerView::default()
    }));
    let health_server = spawn_health_server(args.listen.clone(), view.clone());

    tracing::info!(
        northbound_url = %args.northbound_url,
        southbound_url = %args.southbound_url,
        interval_secs = args.interval_secs,
        "controller starting"
    );

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.context("waiting for shutdown signal")?;
                tracing::info!("controller shutting down");
                health_server.abort();
                return Ok(());
            }
            result = reconcile_once(&http, &args.northbound_url, &args.southbound_url) => {
                match result {
                    Ok(outcome) => {
                        for stream in &outcome.streams {
                            tracing::info!(stream = %stream.name, status = ?stream.status, "stream status");
                        }
                        tracing::info!(
                            status = ?outcome.report.status,
                            summary = %outcome.report.summary,
                            "reconcile tick"
                        );
                        let status_json = outcome.status_json();
                        let names = outcome.streams.iter().map(|s| s.name.clone()).collect();
                        let mut guard = view.write().await;
                        guard.status = status_json;
                        guard.streams = names;
                        guard.endpoints = outcome.endpoints;
                    }
                    Err(error) => tracing::warn!(%error, "reconcile tick failed"),
                }
                tokio::time::sleep(interval).await;
            }
        }
    }
}

fn router(view: SharedView) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/status", get(get_status))
        .route("/streams/{name}/endpoints", get(get_endpoints))
        .with_state(view)
}

fn spawn_health_server(addr: String, view: SharedView) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = router(view);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("binding controller health listener on {addr}"))?;
        tracing::info!(%addr, "controller health API listening");
        axum::serve(listener, app)
            .await
            .context("controller health server error")?;
        Ok(())
    })
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn get_status(State(view): State<SharedView>) -> Json<Value> {
    Json(view.read().await.status.clone())
}

/// Concrete `srt://` endpoints for a placed stream: `200` when placed, `503` when
/// the stream is known but not yet placed, `404` when unknown.
async fn get_endpoints(State(view): State<SharedView>, Path(name): Path<String>) -> Response {
    let view = view.read().await;
    if let Some(endpoints) = view.endpoints.get(&name) {
        (StatusCode::OK, Json(endpoints)).into_response()
    } else if view.streams.contains(&name) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "pending", "stream": name })),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "stream not found" })),
        )
            .into_response()
    }
}

async fn reconcile_once(
    http: &reqwest::Client,
    northbound_url: &str,
    southbound_url: &str,
) -> Result<ReconcileOutcome> {
    let streams = fetch_streams(http, northbound_url).await?;
    let observed = fetch_state(http, southbound_url).await?;

    // Seed every registered node with an empty desired list so deleted or disabled
    // streams' hops are cleared by the full-replace PUT below.
    let mut desired_by_node: BTreeMap<String, Vec<DesiredHop>> = observed
        .nodes
        .iter()
        .map(|node| (node.id.clone(), Vec::new()))
        .collect();

    let mut stream_statuses = Vec::with_capacity(streams.len());
    let mut endpoints_by_stream: BTreeMap<String, StreamEndpoints> = BTreeMap::new();
    let mut enabled = 0usize;
    let mut flowing = 0usize;

    for stream in &streams {
        if !stream.enabled {
            stream_statuses.push(StreamStatus {
                name: stream.name.clone(),
                status: PathStatus::Idle,
                nodes: Vec::new(),
                endpoints: None,
            });
            continue;
        }
        enabled += 1;

        let status = match derive_path(stream, &observed.nodes, &observed.hops) {
            Ok(path) => {
                let mut nodes = Vec::new();
                for hop in &path.hops {
                    if !nodes.contains(&hop.node_id) {
                        nodes.push(hop.node_id.clone());
                    }
                    desired_by_node
                        .entry(hop.node_id.clone())
                        .or_default()
                        .push(hop.clone());
                }
                let status = path_status(&path, &observed.hops);
                let endpoints = match stream_endpoints(stream, &path, &observed.nodes) {
                    Ok(endpoints) => {
                        endpoints_by_stream.insert(stream.name.clone(), endpoints.clone());
                        Some(endpoints)
                    }
                    Err(error) => {
                        tracing::warn!(stream = %stream.name, %error, "cannot resolve stream endpoints");
                        None
                    }
                };
                stream_statuses.push(StreamStatus {
                    name: stream.name.clone(),
                    status,
                    nodes,
                    endpoints,
                });
                status
            }
            Err(error) => {
                tracing::warn!(stream = %stream.name, %error, "cannot place stream; retrying next tick");
                stream_statuses.push(StreamStatus {
                    name: stream.name.clone(),
                    status: PathStatus::Pending,
                    nodes: Vec::new(),
                    endpoints: None,
                });
                PathStatus::Pending
            }
        };
        if status == PathStatus::Flowing {
            flowing += 1;
        }
    }

    for (node_id, hops) in &desired_by_node {
        if let Err(error) = put_desired(http, southbound_url, node_id, hops).await {
            tracing::warn!(node = %node_id, %error, "setting desired hops failed");
        }
    }

    // Control-plane convergence, not media flow: a fully provisioned path that is
    // only waiting for its source (AwaitingInput) is converged, not degraded.
    let status = if enabled == 0 {
        ReconcileStatus::Idle
    } else if stream_statuses
        .iter()
        .any(|s| matches!(s.status, PathStatus::Failed | PathStatus::Degraded))
    {
        ReconcileStatus::Degraded
    } else if stream_statuses
        .iter()
        .any(|s| s.status == PathStatus::Pending)
    {
        ReconcileStatus::Converging
    } else {
        ReconcileStatus::Converged
    };
    let report = ReconcileReport {
        status,
        summary: format!(
            "{flowing}/{enabled} stream(s) flowing across {} node(s)",
            desired_by_node.len()
        ),
    };

    Ok(ReconcileOutcome {
        report,
        streams: stream_statuses,
        endpoints: endpoints_by_stream,
    })
}

async fn fetch_streams(
    http: &reqwest::Client,
    northbound_url: &str,
) -> Result<Vec<StreamDefinition>> {
    http.get(format!("{}/streams", northbound_url.trim_end_matches('/')))
        .send()
        .await
        .context("fetching streams")?
        .error_for_status()
        .context("northbound streams request failed")?
        .json::<Vec<StreamDefinition>>()
        .await
        .context("decoding streams")
}

async fn fetch_state(http: &reqwest::Client, southbound_url: &str) -> Result<ObservedState> {
    http.get(format!("{}/state", southbound_url.trim_end_matches('/')))
        .send()
        .await
        .context("fetching observed state")?
        .error_for_status()
        .context("southbound state request failed")?
        .json::<ObservedState>()
        .await
        .context("decoding observed state")
}

async fn put_desired(
    http: &reqwest::Client,
    southbound_url: &str,
    node_id: &str,
    hops: &[DesiredHop],
) -> Result<()> {
    http.put(format!(
        "{}/nodes/{node_id}/desired",
        southbound_url.trim_end_matches('/')
    ))
    .json(hops)
    .send()
    .await
    .context("sending desired hops")?
    .error_for_status()
    .context("southbound desired PUT failed")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use path::EndpointAddr;
    use tower::ServiceExt;

    fn endpoint(node: &str, host: &str, port: u16) -> EndpointAddr {
        EndpointAddr {
            node: node.to_string(),
            host: host.to_string(),
            port,
            url: format!("srt://{host}:{port}"),
        }
    }

    fn view_with(streams: &[&str], placed: &[(&str, StreamEndpoints)]) -> SharedView {
        Arc::new(RwLock::new(ControllerView {
            status: json!({ "status": "converged" }),
            streams: streams.iter().map(|s| (*s).to_string()).collect(),
            endpoints: placed
                .iter()
                .map(|(name, endpoints)| ((*name).to_string(), endpoints.clone()))
                .collect(),
        }))
    }

    async fn get_endpoints_status(view: SharedView, name: &str) -> StatusCode {
        router(view)
            .oneshot(
                Request::builder()
                    .uri(format!("/streams/{name}/endpoints"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn endpoints_route_returns_placed_stream() {
        let endpoints = StreamEndpoints {
            ingress: endpoint("strom-node-1", "172.26.0.10", 20001),
            outputs: vec![endpoint("strom-node-2", "172.27.0.10", 20002)],
        };
        let view = view_with(&["basic"], &[("basic", endpoints)]);
        assert_eq!(get_endpoints_status(view, "basic").await, StatusCode::OK);
    }

    #[tokio::test]
    async fn endpoints_route_pending_for_known_but_unplaced_stream() {
        let view = view_with(&["unplaceable"], &[]);
        assert_eq!(
            get_endpoints_status(view, "unplaceable").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn endpoints_route_not_found_for_unknown_stream() {
        let view = view_with(&[], &[]);
        assert_eq!(
            get_endpoints_status(view, "nope").await,
            StatusCode::NOT_FOUND
        );
    }
}
