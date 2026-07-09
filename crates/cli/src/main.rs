//! `weave` — operator CLI for the open-weave control plane.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use weave_core::StreamDefinition;

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
    /// List known media nodes.
    Nodes,
}

#[derive(Subcommand)]
enum GetResource {
    /// List desired streams.
    Streams,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let Cli { url, command } = Cli::parse();

    match command {
        Command::Apply { file } => apply(&url, &file).await,
        Command::Get { resource } => match resource {
            GetResource::Streams => get_streams(&url).await,
        },
        Command::Nodes => nodes(),
    }
}

fn parse_stream(yaml: &str) -> Result<StreamDefinition> {
    serde_norway::with::singleton_map_recursive::deserialize(serde_norway::Deserializer::from_str(
        yaml,
    ))
    .context("parsing stream YAML")
}

async fn apply(url: &str, file: &Path) -> Result<()> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
    let stream =
        parse_stream(&text).with_context(|| format!("parsing stream from {}", file.display()))?;

    let response = reqwest::Client::new()
        .post(join_url(url, "/streams"))
        .json(&stream)
        .send()
        .await
        .context("posting stream to northbound")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("northbound rejected stream: {status}: {body}");
    }

    tracing::info!(name = %stream.name, %status, "stream applied");
    println!("{body}");
    Ok(())
}

async fn get_streams(url: &str) -> Result<()> {
    let streams = reqwest::Client::new()
        .get(join_url(url, "/streams"))
        .send()
        .await
        .context("fetching streams")?
        .error_for_status()
        .context("northbound streams request failed")?
        .json::<Vec<StreamDefinition>>()
        .await
        .context("decoding streams")?;

    println!("{}", serde_json::to_string_pretty(&streams)?);
    Ok(())
}

fn nodes() -> Result<()> {
    tracing::info!("nodes: not implemented");
    Ok(())
}

fn join_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{SrtEndpoint, SrtMode, StreamTransport};

    #[test]
    fn parses_fanout_yaml_with_defaults_and_srt_tag() {
        let yaml = r#"
name: cam1-to-studio
source:
  srt:
    url: srt://0.0.0.0:7001
    mode: listener
    latency: 200
destinations:
  - srt:
      url: srt://studio:7002
      mode: caller
  - srt:
      url: srt://backup:7002
      mode: caller
"#;

        let stream = parse_stream(yaml).expect("parse stream yaml");

        assert_eq!(stream.name, "cam1-to-studio");
        assert!(stream.enabled, "enabled defaults to true when omitted");
        assert_eq!(
            stream.source,
            StreamTransport::Srt(SrtEndpoint {
                url: "srt://0.0.0.0:7001".to_string(),
                mode: SrtMode::Listener,
                latency: Some(200),
                node: None,
            })
        );
        assert_eq!(stream.destinations.len(), 2);
        assert_eq!(
            stream.destinations[1],
            StreamTransport::Srt(SrtEndpoint {
                url: "srt://backup:7002".to_string(),
                mode: SrtMode::Caller,
                latency: None,
                node: None,
            })
        );
    }

    #[test]
    fn parsed_yaml_serializes_to_northbound_json_shape() {
        let yaml = r#"
name: paused
enabled: false
source:
  srt:
    url: srt://0.0.0.0:7001
    mode: listener
destinations:
  - srt:
      url: srt://studio:7002
      mode: caller
"#;

        let stream = parse_stream(yaml).expect("parse stream yaml");
        assert!(!stream.enabled);

        let json = serde_json::to_value(&stream).unwrap();
        assert_eq!(json["source"]["srt"]["mode"], "listener");
        assert_eq!(json["destinations"][0]["srt"]["url"], "srt://studio:7002");
    }
}
