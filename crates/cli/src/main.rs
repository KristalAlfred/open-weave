//! `weave` — operator CLI for the open-weave control plane (Phase 0 stubs).

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "weave", version, about = "open-weave control plane CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply a definition file describing desired state.
    Apply {
        /// Path to the definition file.
        file: PathBuf,
    },
    /// List or get known definitions.
    Get,
    /// List known media nodes.
    Nodes,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Apply { file } => apply(&file),
        Command::Get => get(),
        Command::Nodes => nodes(),
    }
}

fn apply(file: &PathBuf) -> Result<()> {
    tracing::info!(?file, "apply: not implemented");
    Ok(())
}

fn get() -> Result<()> {
    tracing::info!("get: not implemented");
    Ok(())
}

fn nodes() -> Result<()> {
    tracing::info!("nodes: not implemented");
    Ok(())
}
