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

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r"
node:
  id: strom-node-1
  southbound_url: http://127.0.0.1:8081
  listen: 0.0.0.0:8091
  data_plane:
    default: 172.26.0.10
  port_range:
    start: 20000
    end: 20999
  transports: [srt]
strom:
  url: http://172.26.0.10:8080
";

    #[test]
    fn parses_valid_config_and_defaults_poll_interval() {
        let config: AdapterConfig = serde_norway::from_str(VALID).expect("parse");
        assert_eq!(config.node.id, "strom-node-1");
        assert_eq!(
            config.node.data_plane.get("default").unwrap().host,
            "172.26.0.10"
        );
        assert!(
            config.node.data_plane["default"].is_dialable(),
            "a bare host is dialable"
        );
        assert!(!config.node.relay, "relay is opt-in");
        assert_eq!(config.strom.url, "http://172.26.0.10:8080");
        assert_eq!(config.strom.poll_interval_secs, 5);
        assert_eq!(config.node.validate(), Ok(()));
    }

    /// The token may live in the node YAML instead of the environment. Which
    /// source wins is [`NodeConfig::resolve_southbound_token`]'s job and depends
    /// on process env, so it is covered on the bench rather than here.
    #[test]
    fn accepts_an_inline_southbound_token() {
        let config: AdapterConfig = serde_norway::from_str(VALID).expect("parse");
        assert_eq!(
            config.node.southbound_token, None,
            "the field is optional; deployments may use the env var instead"
        );

        let yaml = VALID.replace(
            "southbound_url: http://127.0.0.1:8081",
            "southbound_url: http://127.0.0.1:8081\n  southbound_token: from-yaml",
        );
        let config: AdapterConfig = serde_norway::from_str(&yaml).expect("parse");
        assert_eq!(config.node.southbound_token.as_deref(), Some("from-yaml"));
    }

    #[test]
    fn strom_token_is_optional_and_parses_when_present() {
        let config: AdapterConfig = serde_norway::from_str(VALID).expect("parse");
        assert_eq!(
            config.strom.token, None,
            "an unauthenticated Strom needs no token"
        );

        let yaml = VALID.replace(
            "url: http://172.26.0.10:8080",
            "url: http://172.26.0.10:8080\n  token: from-yaml",
        );
        let config: AdapterConfig = serde_norway::from_str(&yaml).expect("parse");
        assert_eq!(config.strom.token.as_deref(), Some("from-yaml"));
    }

    /// Precedence without mutating process env, which parallel tests share.
    /// [`StromSection::resolve_token`] adds only the [`STROM_TOKEN_VAR`] lookup.
    #[test]
    fn config_token_wins_over_the_environment() {
        assert_eq!(
            pick_token(Some("from-yaml"), Some("from-env")),
            Token::new("from-yaml")
        );
        assert_eq!(pick_token(None, Some("from-env")), Token::new("from-env"));
        assert_eq!(pick_token(None, None), None);
        assert_eq!(
            pick_token(Some("  "), Some("from-env")),
            Token::new("from-env"),
            "a blank config value counts as absent"
        );
        assert_eq!(pick_token(Some("  "), Some("")), None);
    }

    #[test]
    fn rejects_unknown_fields() {
        let yaml = format!("{VALID}  bogus: true\n");
        let result: Result<AdapterConfig, _> = serde_norway::from_str(&yaml);
        assert!(result.is_err(), "deny_unknown_fields rejects typos");
    }

    #[test]
    fn rejects_config_missing_default_alias() {
        let yaml = r"
node:
  id: strom-node-1
  southbound_url: http://127.0.0.1:8081
  listen: 0.0.0.0:8091
  data_plane:
    wan: 203.0.113.7
  port_range:
    start: 20000
    end: 20999
strom:
  url: http://172.26.0.10:8080
";
        let config: AdapterConfig = serde_norway::from_str(yaml).expect("parse");
        assert!(config.node.validate().is_err());
    }
}
