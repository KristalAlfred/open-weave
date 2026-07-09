//! `weave-controller` — reconciles desired streams into running Strom flows.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::get};
use clap::Parser;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::{
    NodeDescriptor, ReconcileReport, ReconcileStatus, StreamDefinition, StreamTransport,
};
use weave_strom::{
    StromClient, flow_spec_from_stream, parse_flow_stats, receiver_flow_from_stream,
    receiver_flow_name,
};

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
        env = "WEAVE_STROM_URL",
        default_value = "http://127.0.0.1:18080"
    )]
    strom_url: String,
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

type StatusHolder = Arc<RwLock<Value>>;

#[derive(Debug, Serialize)]
struct FlowStatus {
    name: String,
    flow_id: Option<String>,
    connected: bool,
    packets_sent_lost: i64,
    packets_retransmitted: i64,
}

struct ReconcileOutcome {
    report: ReconcileReport,
    created: Vec<String>,
    flows: Vec<FlowStatus>,
}

impl ReconcileOutcome {
    fn status_json(&self) -> Value {
        json!({
            "status": self.report.status,
            "summary": self.report.summary,
            "created": self.created,
            "flows": self.flows,
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
    let northbound = reqwest::Client::new();
    let strom = StromClient::new(&args.strom_url);
    let interval = Duration::from_secs(args.interval_secs);

    let status: StatusHolder = Arc::new(RwLock::new(json!({ "status": "starting" })));
    let health_server = spawn_health_server(args.listen.clone(), status.clone());

    tracing::info!(
        northbound_url = %args.northbound_url,
        strom_url = %strom.base_url(),
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
            result = reconcile_once(&northbound, &strom, &args.northbound_url, &args.southbound_url) => {
                match result {
                    Ok(outcome) => {
                        tracing::info!(
                            status = ?outcome.report.status,
                            summary = %outcome.report.summary,
                            "reconcile tick"
                        );
                        *status.write().await = outcome.status_json();
                    }
                    Err(error) => tracing::warn!(%error, "reconcile tick failed"),
                }
                tokio::time::sleep(interval).await;
            }
        }
    }
}

fn spawn_health_server(addr: String, status: StatusHolder) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(health))
            .route("/status", get(get_status))
            .with_state(status);
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

async fn get_status(State(status): State<StatusHolder>) -> Json<Value> {
    Json(status.read().await.clone())
}

async fn reconcile_once(
    http: &reqwest::Client,
    strom: &StromClient,
    northbound_url: &str,
    southbound_url: &str,
) -> Result<ReconcileOutcome> {
    let desired = fetch_streams(http, northbound_url).await?;
    let observed = strom.list_flows().await.context("listing strom flows")?;

    let mut flow_ids: HashMap<String, String> = observed
        .into_iter()
        .map(|flow| (flow.name, flow.id))
        .collect();
    let observed_names: HashSet<String> = flow_ids.keys().cloned().collect();

    let mut created = Vec::new();
    for stream in streams_to_create(&desired, &observed_names) {
        let spec = flow_spec_from_stream(stream)
            .with_context(|| format!("mapping stream {}", stream.name))?;
        let id = strom
            .create_flow(&spec)
            .await
            .with_context(|| format!("creating flow {}", stream.name))?;
        strom
            .start_flow(&id)
            .await
            .with_context(|| format!("starting flow {}", stream.name))?;
        tracing::info!(name = %stream.name, id = %id, "created+started flow");
        flow_ids.insert(stream.name.clone(), id);
        created.push(stream.name.clone());
    }

    let nodes = match fetch_nodes(http, southbound_url).await {
        Ok(nodes) => nodes,
        Err(error) => {
            tracing::warn!(%error, "fetching registered nodes failed; skipping receiver placement");
            Vec::new()
        }
    };
    created.extend(reconcile_receivers(http, &nodes, &desired).await);

    let flows = collect_flow_status(strom, &desired, &flow_ids).await;

    let enabled = desired.iter().filter(|s| s.enabled).count();
    let status = if enabled == 0 {
        ReconcileStatus::Idle
    } else if flows.iter().any(|f| !f.connected) {
        ReconcileStatus::Degraded
    } else {
        ReconcileStatus::Converged
    };

    let connected = flows.iter().filter(|f| f.connected).count();
    let report = ReconcileReport {
        status,
        summary: format!(
            "{enabled} enabled stream(s), {} created this tick, {connected}/{enabled} connected",
            created.len()
        ),
    };

    Ok(ReconcileOutcome {
        report,
        created,
        flows,
    })
}

async fn collect_flow_status(
    strom: &StromClient,
    desired: &[StreamDefinition],
    flow_ids: &HashMap<String, String>,
) -> Vec<FlowStatus> {
    let mut flows = Vec::new();
    for stream in desired.iter().filter(|s| s.enabled) {
        let Some(id) = flow_ids.get(&stream.name) else {
            tracing::warn!(name = %stream.name, "enabled stream has no flow");
            flows.push(FlowStatus {
                name: stream.name.clone(),
                flow_id: None,
                connected: false,
                packets_sent_lost: 0,
                packets_retransmitted: 0,
            });
            continue;
        };

        let stats = match strom.srt_stats(id).await {
            Ok(value) => parse_flow_stats(&value),
            Err(error) => {
                tracing::debug!(name = %stream.name, %error, "srt-stats unavailable");
                weave_strom::FlowStats::default()
            }
        };

        tracing::info!(
            name = %stream.name,
            id = %id,
            connected = stats.connected,
            packets_sent_lost = stats.packets_sent_lost,
            packets_retransmitted = stats.packets_retransmitted,
            "flow status"
        );

        flows.push(FlowStatus {
            name: stream.name.clone(),
            flow_id: Some(id.clone()),
            connected: stats.connected,
            packets_sent_lost: stats.packets_sent_lost,
            packets_retransmitted: stats.packets_retransmitted,
        });
    }
    flows
}

async fn fetch_streams(
    northbound: &reqwest::Client,
    northbound_url: &str,
) -> Result<Vec<StreamDefinition>> {
    northbound
        .get(format!("{northbound_url}/streams"))
        .send()
        .await
        .context("fetching streams")?
        .error_for_status()
        .context("northbound streams request failed")?
        .json::<Vec<StreamDefinition>>()
        .await
        .context("decoding streams")
}

async fn fetch_nodes(http: &reqwest::Client, southbound_url: &str) -> Result<Vec<NodeDescriptor>> {
    http.get(format!("{}/nodes", southbound_url.trim_end_matches('/')))
        .send()
        .await
        .context("fetching nodes")?
        .error_for_status()
        .context("southbound nodes request failed")?
        .json::<Vec<NodeDescriptor>>()
        .await
        .context("decoding nodes")
}

/// Ensure a receiver flow exists on each destination node's Strom for every enabled
/// stream. Returns the names of receiver flows created this tick.
async fn reconcile_receivers(
    http: &reqwest::Client,
    nodes: &[NodeDescriptor],
    desired: &[StreamDefinition],
) -> Vec<String> {
    let mut created = Vec::new();
    let mut flows_by_node: HashMap<String, HashSet<String>> = HashMap::new();

    for stream in desired.iter().filter(|s| s.enabled) {
        let Some(host) = first_destination_host(stream) else {
            tracing::warn!(name = %stream.name, "stream has no destination host; skipping receiver");
            continue;
        };
        let Some(node_url) = node_strom_url_for_host(nodes, &host) else {
            tracing::info!(
                name = %stream.name,
                host = %host,
                "no registered node matches destination host; skipping receiver placement"
            );
            continue;
        };
        let node_url = node_url.to_string();
        let node_strom = StromClient::with_client(&node_url, http.clone());

        if !flows_by_node.contains_key(&node_url) {
            match node_strom.list_flows().await {
                Ok(flows) => {
                    flows_by_node.insert(
                        node_url.clone(),
                        flows.into_iter().map(|f| f.name).collect(),
                    );
                }
                Err(error) => {
                    tracing::warn!(name = %stream.name, node = %node_url, %error, "listing destination node flows failed; skipping receiver");
                    continue;
                }
            }
        }

        let recv_name = receiver_flow_name(&stream.name);
        if flows_by_node
            .get(&node_url)
            .is_some_and(|names| names.contains(&recv_name))
        {
            continue;
        }

        let spec = match receiver_flow_from_stream(stream) {
            Ok(spec) => spec,
            Err(error) => {
                tracing::warn!(name = %stream.name, %error, "mapping receiver flow failed");
                continue;
            }
        };

        match node_strom.create_flow(&spec).await {
            Ok(id) => {
                if let Err(error) = node_strom.start_flow(&id).await {
                    tracing::warn!(name = %recv_name, node = %node_url, %error, "starting receiver flow failed");
                    continue;
                }
                tracing::info!(name = %recv_name, node = %node_url, id = %id, "created+started receiver flow");
                if let Some(names) = flows_by_node.get_mut(&node_url) {
                    names.insert(recv_name.clone());
                }
                created.push(recv_name);
            }
            Err(error) => {
                tracing::warn!(name = %recv_name, node = %node_url, %error, "creating receiver flow failed");
            }
        }
    }

    created
}

fn first_destination_host(stream: &StreamDefinition) -> Option<String> {
    let StreamTransport::Srt(endpoint) = stream.destinations.first()?;
    url_host(&endpoint.url)
}

fn node_strom_url_for_host<'a>(nodes: &'a [NodeDescriptor], host: &str) -> Option<&'a str> {
    nodes
        .iter()
        .find(|node| url_host(&node.endpoint).as_deref() == Some(host))
        .map(|node| node.endpoint.as_str())
}

/// Extract the host from a URL with or without a scheme (e.g. `srt://h:7002`, `http://h:8080`).
fn url_host(url: &str) -> Option<String> {
    let authority = url
        .rsplit("://")
        .next()?
        .split(['/', '?'])
        .next()
        .unwrap_or_default();
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host);
    (!host.is_empty()).then(|| host.to_string())
}

/// Enabled desired streams that have no same-named flow yet. Pure and idempotent.
fn streams_to_create<'a>(
    desired: &'a [StreamDefinition],
    observed_names: &HashSet<String>,
) -> Vec<&'a StreamDefinition> {
    desired
        .iter()
        .filter(|stream| stream.enabled && !observed_names.contains(&stream.name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{SrtEndpoint, SrtMode, StreamTransport};

    fn stream(name: &str, enabled: bool) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled,
            source: StreamTransport::Srt(SrtEndpoint {
                url: "srt://0.0.0.0:7001".to_string(),
                mode: SrtMode::Listener,
                latency: Some(200),
                node: None,
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                url: "srt://172.31.0.10:7002".to_string(),
                mode: SrtMode::Caller,
                latency: Some(1000),
                node: None,
            })],
        }
    }

    #[test]
    fn creates_only_enabled_streams_without_existing_flow() {
        let desired = vec![stream("a", true), stream("b", false), stream("c", true)];
        let observed: HashSet<String> = ["c".to_string()].into_iter().collect();

        let to_create = streams_to_create(&desired, &observed);
        let names: Vec<_> = to_create.iter().map(|s| s.name.as_str()).collect();

        assert_eq!(names, vec!["a"]);
    }

    #[test]
    fn nothing_to_create_when_all_present_or_disabled() {
        let desired = vec![stream("a", true), stream("b", false)];
        let observed: HashSet<String> = ["a".to_string()].into_iter().collect();
        assert!(streams_to_create(&desired, &observed).is_empty());
    }

    #[test]
    fn url_host_handles_scheme_port_and_path() {
        assert_eq!(
            url_host("srt://172.27.0.10:7002").as_deref(),
            Some("172.27.0.10")
        );
        assert_eq!(
            url_host("http://172.27.0.10:8080").as_deref(),
            Some("172.27.0.10")
        );
        assert_eq!(url_host("http://node:8080/api").as_deref(), Some("node"));
        assert_eq!(url_host("172.27.0.10:7002").as_deref(), Some("172.27.0.10"));
        assert_eq!(url_host("").as_deref(), None);
    }

    #[test]
    fn matches_destination_host_to_registered_node_strom() {
        use weave_core::{NodeCapabilities, NodeStatus};

        let nodes = vec![
            NodeDescriptor {
                id: "strom-node-1".to_string(),
                endpoint: "http://172.26.0.10:8080".to_string(),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities::default(),
            },
            NodeDescriptor {
                id: "strom-node-2".to_string(),
                endpoint: "http://172.27.0.10:8080".to_string(),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities::default(),
            },
        ];

        assert_eq!(
            node_strom_url_for_host(&nodes, "172.27.0.10"),
            Some("http://172.27.0.10:8080")
        );
        assert_eq!(node_strom_url_for_host(&nodes, "10.0.0.9"), None);
    }
}
