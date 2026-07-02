//! Shared domain types for open-weave.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definition {
    pub id: String,
    pub name: String,
    pub spec: serde_json::Value,
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
