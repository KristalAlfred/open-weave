//! Adapter configuration loaded from a YAML file at startup.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use weave_core::NodeConfig;

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
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
}

fn default_poll_interval_secs() -> u64 {
    5
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
            config.node.data_plane.get("default").unwrap(),
            "172.26.0.10"
        );
        assert_eq!(config.strom.url, "http://172.26.0.10:8080");
        assert_eq!(config.strom.poll_interval_secs, 5);
        assert_eq!(config.node.validate(), Ok(()));
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
