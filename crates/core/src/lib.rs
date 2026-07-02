//! Shared domain types for open-weave.
//!
//! Phase 0 placeholders. These describe the desired-state vocabulary the control
//! plane speaks: `Definition`s come in from the north, `NodeDescriptor`s track the
//! media nodes reconciled from the south. Fields and shapes will change.

use serde::{Deserialize, Serialize};

/// A unit of desired state submitted by an operator or system.
///
/// The `spec` is an opaque placeholder until the desired-state schema is defined.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definition {
    pub id: String,
    pub name: String,
    pub spec: serde_json::Value,
}

/// A media node known to the control plane and its last observed status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub id: String,
    pub endpoint: String,
    pub status: NodeStatus,
}

/// Last observed reconciliation status of a media node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Unknown,
    Ready,
    Degraded,
    Offline,
}
