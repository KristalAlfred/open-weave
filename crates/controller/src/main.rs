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
use weave_core::{ReconcileReport, ReconcileStatus, StreamDefinition};
use weave_strom::{StromClient, flow_spec_from_stream, parse_flow_stats};

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
            result = reconcile_once(&northbound, &strom, &args.northbound_url) => {
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
    northbound: &reqwest::Client,
    strom: &StromClient,
    northbound_url: &str,
) -> Result<ReconcileOutcome> {
    let desired = fetch_streams(northbound, northbound_url).await?;
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
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                url: "srt://172.31.0.10:7002".to_string(),
                mode: SrtMode::Caller,
                latency: Some(1000),
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
}
