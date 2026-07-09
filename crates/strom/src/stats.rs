//! Defensive parsing of Strom's `srt-stats` payload (shape inferred, all fields optional).

use serde_json::Value;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowStats {
    pub connections: usize,
    pub connected: bool,
    pub packets_sent_lost: i64,
    pub packets_retransmitted: i64,
    pub packets_received_lost: i64,
    pub packets_received_retransmitted: i64,
}

/// A flow is `connected` only when it has at least one connection and every one reports `connected: true`.
#[must_use]
pub fn parse_flow_stats(value: &Value) -> FlowStats {
    let Some(connections) = value
        .pointer("/stats/connections")
        .and_then(Value::as_object)
    else {
        return FlowStats::default();
    };
    if connections.is_empty() {
        return FlowStats::default();
    }

    let mut stats = FlowStats {
        connections: connections.len(),
        connected: true,
        ..FlowStats::default()
    };

    for connection in connections.values() {
        if connection.get("connected").and_then(Value::as_bool) != Some(true) {
            stats.connected = false;
        }
        let Some(callers) = connection.get("callers").and_then(Value::as_array) else {
            continue;
        };
        for caller in callers {
            stats.packets_sent_lost += field_i64(caller, "packets_sent_lost");
            stats.packets_retransmitted += field_i64(caller, "packets_retransmitted");
            stats.packets_received_lost += field_i64(caller, "packets_received_lost");
            stats.packets_received_retransmitted +=
                field_i64(caller, "packets_received_retransmitted");
        }
    }

    stats
}

fn field_i64(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sums_callers_and_reports_connected() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsrc_0": { "role": "source", "connected": true, "callers": [
                    { "packets_sent_lost": 0, "packets_retransmitted": 0,
                      "packets_received_lost": 5, "packets_received_retransmitted": 5 }
                ]},
                "srtsink_0": { "role": "sink", "connected": true, "callers": [
                    { "packets_sent_lost": 42, "packets_retransmitted": 42 }
                ]}
            }}
        });
        let stats = parse_flow_stats(&value);
        assert_eq!(stats.connections, 2);
        assert!(stats.connected);
        assert_eq!(stats.packets_sent_lost, 42);
        assert_eq!(stats.packets_retransmitted, 42);
        assert_eq!(stats.packets_received_lost, 5);
        assert_eq!(stats.packets_received_retransmitted, 5);
    }

    #[test]
    fn any_disconnected_connection_marks_flow_disconnected() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [] },
                "srtsink_0": { "connected": false, "callers": [] }
            }}
        });
        assert!(!parse_flow_stats(&value).connected);
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
