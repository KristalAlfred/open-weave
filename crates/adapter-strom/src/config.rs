//! Adapter configuration loaded from a YAML file at startup.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use weave_core::NodeConfig;
use weave_core::auth::Token;

/// Environment variable holding the bearer token presented to Strom.
pub const STROM_TOKEN_VAR: &str = "WEAVE_STROM_TOKEN";

/// Strom adapter configuration: a shared node section plus the Strom endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub node: NodeConfig,
    pub strom: StromSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StromSection {
    pub url: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
}

fn default_poll_interval_secs() -> u64 {
    5
}

impl StromSection {
    /// Resolve the bearer token presented to Strom: the config value when set,
    /// otherwise [`STROM_TOKEN_VAR`] from the environment. `None` presents no
    /// credential and is not an error — Strom authenticates on its own terms, so
    /// [`weave_core::auth::AUTH_DISABLED_VAR`] does not apply.
    #[must_use]
    pub fn resolve_token(&self) -> Option<Token> {
        pick_token(
            self.token.as_deref(),
            std::env::var(STROM_TOKEN_VAR).ok().as_deref(),
        )
    }
}

fn pick_token(config: Option<&str>, env: Option<&str>) -> Option<Token> {
    config
        .and_then(Token::new)
        .or_else(|| env.and_then(Token::new))
}

impl AdapterConfig {
    /// Load and validate the adapter config from a YAML file.
    ///
    /// # Errors
    /// Returns an error if the file cannot be read, the YAML is malformed or
    /// carries unknown fields, or the node config fails validation.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let config: Self = serde_norway::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        config
            .node
            .validate()
            .with_context(|| format!("validating node config {}", path.display()))?;
        Ok(config)
    }
}
