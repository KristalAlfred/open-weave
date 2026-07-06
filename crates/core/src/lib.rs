//! Shared domain types for open-weave.

use serde::{Deserialize, Serialize};

/// Operator intent: one source streamed to one or more destinations over a transport.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub source: StreamTransport,
    pub destinations: Vec<StreamTransport>,
}

fn default_enabled() -> bool {
    true
}

/// Transport carrying a stream endpoint. Externally tagged by transport name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTransport {
    Srt(SrtEndpoint),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SrtEndpoint {
    pub url: String,
    pub mode: SrtMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SrtMode {
    Listener,
    Caller,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub id: String,
    pub endpoint: String,
    pub status: NodeStatus,
    #[serde(default)]
    pub capabilities: NodeCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NodeCapabilities {
    #[serde(default)]
    pub adapters: Vec<AdapterDescriptor>,
    #[serde(default)]
    pub transports: Vec<TransportDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterDescriptor {
    pub name: String,
    pub kind: AdapterKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterKind {
    MediaNode,
    Strom,
    Nmos,
    MxlDomain,
    MxlK8s,
    Mcm,
    Vendor,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportDescriptor {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointDescriptor {
    pub id: String,
    pub label: String,
    pub node_id: Option<String>,
    pub kind: EndpointKind,
    #[serde(default)]
    pub transports: Vec<TransportDescriptor>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Source,
    Destination,
    Bidirectional,
    Flow,
    Gateway,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub node: NodeDescriptor,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub node_id: String,
    pub status: NodeStatus,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedState {
    #[serde(default)]
    pub nodes: Vec<NodeDescriptor>,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileReport {
    pub status: ReconcileStatus,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileStatus {
    Idle,
    Converging,
    Converged,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Unknown,
    Ready,
    Degraded,
    Offline,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_json_uses_snake_case_transport_tag_and_defaults_enabled() {
        let json = serde_json::json!({
            "name": "cam1-to-studio",
            "source": { "srt": { "url": "srt://0.0.0.0:7001", "mode": "listener", "latency": 200 } },
            "destinations": [
                { "srt": { "url": "srt://studio:7002", "mode": "caller" } }
            ]
        });

        let stream: StreamDefinition = serde_json::from_value(json).expect("parse stream");

        assert!(stream.enabled, "enabled defaults to true when omitted");
        assert_eq!(
            stream.source,
            StreamTransport::Srt(SrtEndpoint {
                url: "srt://0.0.0.0:7001".to_string(),
                mode: SrtMode::Listener,
                latency: Some(200),
            })
        );
        assert_eq!(
            stream.destinations[0],
            StreamTransport::Srt(SrtEndpoint {
                url: "srt://studio:7002".to_string(),
                mode: SrtMode::Caller,
                latency: None,
            })
        );

        let round_trip: StreamDefinition =
            serde_json::from_str(&serde_json::to_string(&stream).unwrap()).unwrap();
        assert_eq!(stream, round_trip);
        assert_eq!(
            serde_json::to_value(&stream).unwrap()["source"]
                .as_object()
                .unwrap()
                .keys()
                .next()
                .unwrap(),
            "srt"
        );
    }
}
