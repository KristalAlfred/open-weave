//! `weave-controller` — reconciler loop for desired and observed media state.

use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use clap::Parser;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;
use weave_core::{Definition, NodeDescriptor, ReconcileReport, ReconcileStatus};

#[derive(Debug, Parser)]
#[command(name = "weave-controller", version, about = "open-weave reconciler")]
struct Args {
    #[arg(
        long,
        env = "WEAVE_NORTHBOUND_URL",
        default_value = "http://127.0.0.1:8080"
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let client = reqwest::Client::new();
    let interval = Duration::from_secs(args.interval_secs);

    let health_server = spawn_health_server(args.listen.clone());

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
            result = reconcile_once(&client, &args.northbound_url, &args.southbound_url) => {
                match result {
                    Ok(report) => tracing::info!(status = ?report.status, summary = %report.summary, "reconcile tick"),
                    Err(error) => tracing::warn!(%error, "reconcile tick failed"),
                }
                tokio::time::sleep(interval).await;
            }
        }
    }
}

fn spawn_health_server(addr: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = Router::new().route("/health", get(health));
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

async fn reconcile_once(
    client: &reqwest::Client,
    northbound_url: &str,
    southbound_url: &str,
) -> Result<ReconcileReport> {
    let definitions = client
        .get(format!("{northbound_url}/definitions"))
        .send()
        .await
        .context("fetching definitions")?
        .error_for_status()
        .context("northbound definitions request failed")?
        .json::<Vec<Definition>>()
        .await
        .context("decoding definitions")?;

    let nodes = client
        .get(format!("{southbound_url}/nodes"))
        .send()
        .await
        .context("fetching nodes")?
        .error_for_status()
        .context("southbound nodes request failed")?
        .json::<Vec<NodeDescriptor>>()
        .await
        .context("decoding nodes")?;

    let status = if definitions.is_empty() {
        ReconcileStatus::Idle
    } else if nodes.is_empty() {
        ReconcileStatus::Degraded
    } else {
        ReconcileStatus::Converging
    };

    Ok(ReconcileReport {
        status,
        summary: format!(
            "{} desired definition(s), {} observed node(s)",
            definitions.len(),
            nodes.len()
        ),
    })
}
