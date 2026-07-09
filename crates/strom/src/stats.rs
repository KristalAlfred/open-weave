//! Defensive parsing of Strom's `srt-stats` payload (shape inferred, all fields optional).

use serde_json::Value;

/// Field names Strom may use for a caller's receive/send rate, in preference order.
const RECV_RATE_KEYS: &[&str] = &["recv_rate_mbps", "mbps_recv_rate", "mbpsRecvRate"];
const SEND_RATE_KEYS: &[&str] = &["send_rate_mbps", "mbps_send_rate", "mbpsSendRate"];

/// Per-element connection status parsed from one `connections.{element}` entry.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ElementStats {
    pub id: String,
    pub connected: bool,
    pub rate_mbps: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FlowStats {
    pub elements: Vec<ElementStats>,
    pub connected: bool,
    pub packets_sent_lost: i64,
    pub packets_retransmitted: i64,
    pub packets_received_lost: i64,
    pub packets_received_retransmitted: i64,
}

impl FlowStats {
    /// The first element whose id starts with `srtsrc` (a hop's ingress socket).
    #[must_use]
    pub fn ingress(&self) -> Option<&ElementStats> {
        self.element_with_prefix("srtsrc")
    }

    /// The first element whose id starts with `srtsink` (a hop's egress socket).
    #[must_use]
    pub fn egress(&self) -> Option<&ElementStats> {
        self.element_with_prefix("srtsink")
    }

    fn element_with_prefix(&self, prefix: &str) -> Option<&ElementStats> {
        self.elements.iter().find(|e| e.id.starts_with(prefix))
    }
}

impl From<FlowStats> for weave_core::LinkStats {
    fn from(stats: FlowStats) -> Self {
        Self {
            connections: stats.elements.len(),
            connected: stats.connected,
            packets_sent_lost: stats.packets_sent_lost,
            packets_retransmitted: stats.packets_retransmitted,
            packets_received_lost: stats.packets_received_lost,
            packets_received_retransmitted: stats.packets_received_retransmitted,
        }
    }
}

/// Parse per-element connection status and aggregate loss counters from a
/// `srt-stats` payload. Every field is optional; missing shapes yield defaults.
///
/// The flow-level `connected` flag is derived as "at least one element and every
/// one connected", preserving the pre-per-element semantics for existing callers.
#[must_use]
pub fn parse_flow_stats(value: &Value) -> FlowStats {
    let Some(connections) = value
        .pointer("/stats/connections")
        .and_then(Value::as_object)
    else {
        return FlowStats::default();
    };

    let mut stats = FlowStats::default();
    for (id, connection) in connections {
        let connected = connection
            .get("connected")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let rate_keys = if id.starts_with("srtsink") {
            SEND_RATE_KEYS
        } else {
            RECV_RATE_KEYS
        };

        let mut rate_mbps = 0.0;
        if let Some(callers) = connection.get("callers").and_then(Value::as_array) {
            for caller in callers {
                stats.packets_sent_lost += field_i64(caller, "packets_sent_lost");
                stats.packets_retransmitted += field_i64(caller, "packets_retransmitted");
                stats.packets_received_lost += field_i64(caller, "packets_received_lost");
                stats.packets_received_retransmitted +=
                    field_i64(caller, "packets_received_retransmitted");
                rate_mbps += rate_field(caller, rate_keys);
            }
        }

        stats.elements.push(ElementStats {
            id: id.clone(),
            connected,
            rate_mbps,
        });
    }

    stats.connected = !stats.elements.is_empty() && stats.elements.iter().all(|e| e.connected);
    stats
}

fn field_i64(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or(0)
}

fn rate_field(value: &Value, keys: &[&str]) -> f64 {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_f64))
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_per_element_connected_rate_and_summed_loss() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsrc_0": { "role": "source", "connected": true, "callers": [
                    { "recv_rate_mbps": 4.5,
                      "packets_sent_lost": 0, "packets_retransmitted": 0,
                      "packets_received_lost": 5, "packets_received_retransmitted": 5 }
                ]},
                "srtsink_0": { "role": "sink", "connected": true, "callers": [
                    { "send_rate_mbps": 4.4, "packets_sent_lost": 42, "packets_retransmitted": 42 }
                ]}
            }}
        });
        let stats = parse_flow_stats(&value);

        assert_eq!(stats.elements.len(), 2);
        assert!(stats.connected);
        assert_eq!(stats.ingress().map(|e| e.rate_mbps), Some(4.5));
        assert_eq!(stats.egress().map(|e| e.rate_mbps), Some(4.4));
        assert!(stats.ingress().is_some_and(|e| e.connected));
        assert_eq!(stats.packets_sent_lost, 42);
        assert_eq!(stats.packets_retransmitted, 42);
        assert_eq!(stats.packets_received_lost, 5);
        assert_eq!(stats.packets_received_retransmitted, 5);
    }

    #[test]
    fn any_disconnected_element_marks_flow_disconnected_but_keeps_element_detail() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [] },
                "srtsink_0": { "connected": false, "callers": [] }
            }}
        });
        let stats = parse_flow_stats(&value);
        assert!(!stats.connected);
        assert!(stats.ingress().is_some_and(|e| e.connected));
        assert!(stats.egress().is_some_and(|e| !e.connected));
    }

    #[test]
    fn connected_element_with_zero_rate_stays_connected() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [ { "recv_rate_mbps": 0.0 } ] }
            }}
        });
        let stats = parse_flow_stats(&value);
        let ingress = stats.ingress().expect("ingress element");
        assert!(ingress.connected);
        assert_eq!(ingress.rate_mbps, 0.0);
    }

    #[test]
    fn empty_or_absent_stats_are_disconnected() {
        assert_eq!(
            parse_flow_stats(&serde_json::json!({})),
            FlowStats::default()
        );
        assert_eq!(
            parse_flow_stats(&serde_json::json!({ "stats": { "connections": {} } })),
            FlowStats::default()
        );
    }
}
