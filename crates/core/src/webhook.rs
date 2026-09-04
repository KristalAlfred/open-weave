//! Node lifecycle events the controller pushes to a configured receiver.
//!
//! The shape lives in core so a Rust consumer deserializes exactly what the
//! controller serializes. Delivery — queueing, retries, auth — belongs to the
//! controller; see `weave-controller`'s `webhook` module.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{NodeCapabilities, NodeDescriptor, NodeStatus};

/// One lifecycle event. The whole JSON body of a delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Stable across retries of one delivery, so a receiver can deduplicate.
    pub event_id: String,
    pub event_type: EventType,
    /// RFC 3339, UTC.
    pub occurred_at: String,
    pub node: NodeSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    #[serde(rename = "node.registered")]
    NodeRegistered,
    #[serde(rename = "node.online")]
    NodeOnline,
    #[serde(rename = "node.offline")]
    NodeOffline,
}

impl EventType {
    /// Every type, in the order a node's lifecycle reaches them.
    pub const ALL: [Self; 3] = [Self::NodeRegistered, Self::NodeOnline, Self::NodeOffline];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodeRegistered => "node.registered",
            Self::NodeOnline => "node.online",
            Self::NodeOffline => "node.offline",
        }
    }

    /// The type named by its wire form, `None` for anything else.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The node an event is about.
///
/// A [`crate::NodeRegistration`] also carries `endpoints` and `hop_status`.
/// Those describe hops rather than the node, and are not part of this contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSummary {
    pub id: String,
    pub status: NodeStatus,
    pub endpoint: String,
    #[serde(default)]
    pub capabilities: NodeCapabilities,
}

impl From<&NodeDescriptor> for NodeSummary {
    fn from(node: &NodeDescriptor) -> Self {
        Self {
            id: node.id.clone(),
            status: node.status,
            endpoint: node.endpoint.clone(),
            capabilities: node.capabilities.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeviceKind, DeviceSet, NodeRegistration, PROTOCOL_VERSION};

    fn registration() -> NodeRegistration {
        NodeRegistration {
            protocol_version: PROTOCOL_VERSION,
            node: NodeDescriptor {
                id: "guest-1".to_string(),
                endpoint: "http://guest-1:8080".to_string(),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities {
                    devices: DeviceSet::from_iter([DeviceKind::Capture]),
                    ..NodeCapabilities::default()
                },
            },
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    #[test]
    fn summary_carries_the_node_and_nothing_else_from_the_registration() {
        let registration = registration();
        let summary = NodeSummary::from(&registration.node);

        assert_eq!(summary.id, "guest-1");
        assert_eq!(summary.status, NodeStatus::Ready);
        assert_eq!(summary.endpoint, "http://guest-1:8080");
        assert!(summary.capabilities.offers_device(DeviceKind::Capture));

        let body = serde_json::to_value(&summary).unwrap();
        let fields: Vec<&str> = body
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(fields, ["capabilities", "endpoint", "id", "status"]);
    }

    #[test]
    fn event_types_round_trip_through_their_wire_names() {
        for kind in EventType::ALL {
            assert_eq!(EventType::parse(kind.as_str()), Some(kind));
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::json!(kind.as_str())
            );
        }
        assert_eq!(EventType::parse("node.exploded"), None);
    }
}
