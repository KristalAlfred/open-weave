//! Defensive parsing of Strom's `srt-stats` (shape inferred) and `webrtc-stats`
//! payloads. All fields are optional.

use std::collections::BTreeMap;

use serde_json::Value;

/// Field names Strom may use for a caller's receive/send rate, in preference order.
const RECV_RATE_KEYS: &[&str] = &["recv_rate_mbps", "mbps_recv_rate", "mbpsRecvRate"];
const SEND_RATE_KEYS: &[&str] = &["send_rate_mbps", "mbps_send_rate", "mbpsSendRate"];

/// Per-element connection status parsed from one `connections.{element}` entry.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ElementStats {
    pub id: String,
    pub connected: bool,
    pub connections: usize,
    pub rate_mbps: f64,
    /// Cumulative bytes received across this element's callers. Byte progress over
    /// time is the only reliable signal that a connected socket is truly flowing.
    pub bytes_received: i64,
    /// Cumulative bytes sent across this element's callers; the sink-side
    /// counterpart of `bytes_received`.
    pub bytes_sent: i64,
    pub packets_sent_lost: i64,
    pub packets_retransmitted: i64,
    pub packets_received_lost: i64,
    pub packets_received_retransmitted: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FlowStats {
    pub elements: Vec<ElementStats>,
}

impl FlowStats {
    /// The first `srtsrc` element: `srtsrc_0` in an element flow, `<block>:srtsrc`
    /// inside a block. A hop's SRT ingress socket.
    #[must_use]
    pub fn ingress(&self) -> Option<&ElementStats> {
        self.element_named("srtsrc")
    }

    /// The first `srtsink` element, likewise. A hop's SRT egress socket.
    #[must_use]
    pub fn egress(&self) -> Option<&ElementStats> {
        self.egress_at(0)
    }

    /// The SRT element carrying desired egress `index`.
    #[must_use]
    pub fn egress_at(&self, index: usize) -> Option<&ElementStats> {
        let element_id = format!("srtsink_{index}");
        let block_element_id = format!("srt_out_{index}:srtsink");
        self.elements.iter().find(|element| {
            element.id == element_id
                || element.id == block_element_id
                || (index == 0 && element.id == "srt_out:srtsink")
        })
    }

    fn element_named(&self, element: &str) -> Option<&ElementStats> {
        self.elements
            .iter()
            .find(|entry| is_srt_element(&entry.id, element))
    }
}

/// Whether a `connections` key names `element` (`srtsrc`/`srtsink`), either as a
/// bare element id such as `srtsrc_0` or inside a block as `<block>:srtsrc`.
fn is_srt_element(id: &str, element: &str) -> bool {
    id.starts_with(element) || id.rsplit(':').next() == Some(element)
}

impl From<&ElementStats> for weave_core::LinkStats {
    fn from(stats: &ElementStats) -> Self {
        Self {
            connections: stats.connections,
            rate_mbps: stats.rate_mbps,
            packets_sent_lost: stats.packets_sent_lost,
            packets_retransmitted: stats.packets_retransmitted,
            packets_received_lost: stats.packets_received_lost,
            packets_received_retransmitted: stats.packets_received_retransmitted,
        }
    }
}

/// Parse per-element connection status and aggregate loss counters from a
/// `srt-stats` payload. Every field is optional; missing shapes yield defaults.
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

        let rate_keys = if is_srt_element(id, "srtsink") {
            SEND_RATE_KEYS
        } else {
            RECV_RATE_KEYS
        };

        let mut connections = 0;
        let mut rate_mbps = 0.0;
        let mut bytes_received = 0;
        let mut bytes_sent = 0;
        let mut packets_sent_lost = 0;
        let mut packets_retransmitted = 0;
        let mut packets_received_lost = 0;
        let mut packets_received_retransmitted = 0;
        if let Some(callers) = connection.get("callers").and_then(Value::as_array) {
            connections = callers.len();
            for caller in callers {
                packets_sent_lost += field_i64(caller, "packets_sent_lost");
                packets_retransmitted += field_i64(caller, "packets_retransmitted");
                packets_received_lost += field_i64(caller, "packets_received_lost");
                packets_received_retransmitted +=
                    field_i64(caller, "packets_received_retransmitted");
                rate_mbps += rate_field(caller, rate_keys);
                bytes_received += field_i64(caller, "bytes_received");
                bytes_sent += field_i64(caller, "bytes_sent");
            }
        }

        stats.elements.push(ElementStats {
            id: id.clone(),
            connected,
            connections: connections.max(usize::from(connected)),
            rate_mbps,
            bytes_received,
            bytes_sent,
            packets_sent_lost,
            packets_retransmitted,
            packets_received_lost,
            packets_received_retransmitted,
        });
    }

    stats
}

/// Media through one WebRTC block, summed over the sessions Strom reports for it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionStats {
    /// The block id, such as `whip_in` or `whep_out_0`.
    pub block: String,
    /// Sessions carrying at least one RTP stream. Strom keeps a session's entry
    /// for a while after it ends, with no RTP streams, and those are not counted.
    pub sessions: usize,
    pub bytes_received: i64,
    pub bytes_sent: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct WebRtcStats {
    pub blocks: Vec<SessionStats>,
}

impl WebRtcStats {
    /// The `whip_in` block: a hop's WHIP ingress.
    #[must_use]
    pub fn ingress(&self) -> Option<&SessionStats> {
        self.block("whip_in")
    }

    /// The `whep_out_{index}` block carrying desired egress `index`.
    #[must_use]
    pub fn egress_at(&self, index: usize) -> Option<&SessionStats> {
        self.block(&format!("whep_out_{index}"))
    }

    #[must_use]
    pub fn block(&self, id: &str) -> Option<&SessionStats> {
        self.blocks.iter().find(|block| block.block == id)
    }
}

/// Parse per-block session counts and byte totals from a `webrtc-stats`
/// payload. Its `connections` are keyed `<block>:session_<consumer>:<webrtcbin>`,
/// one per session; a key with no block prefix is skipped. Every field is
/// optional; missing shapes yield defaults.
#[must_use]
pub fn parse_webrtc_stats(value: &Value) -> WebRtcStats {
    let Some(connections) = value
        .pointer("/stats/connections")
        .and_then(Value::as_object)
    else {
        return WebRtcStats::default();
    };

    let mut blocks = BTreeMap::<&str, SessionStats>::new();
    for (key, connection) in connections {
        let Some((block_id, _)) = key.split_once(':') else {
            continue;
        };
        let inbound = rtp_streams(connection, "inbound_rtp");
        let outbound = rtp_streams(connection, "outbound_rtp");
        let block = blocks.entry(block_id).or_insert_with(|| SessionStats {
            block: block_id.to_string(),
            ..SessionStats::default()
        });
        if !inbound.is_empty() || !outbound.is_empty() {
            block.sessions += 1;
        }
        block.bytes_received += inbound.iter().map(|s| field_i64(s, "bytes")).sum::<i64>();
        block.bytes_sent += outbound.iter().map(|s| field_i64(s, "bytes")).sum::<i64>();
    }
    WebRtcStats {
        blocks: blocks.into_values().collect(),
    }
}

fn rtp_streams<'a>(connection: &'a Value, key: &str) -> &'a [Value] {
    connection
        .get(key)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
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
        assert_eq!(stats.ingress().map(|e| e.rate_mbps), Some(4.5));
        assert_eq!(stats.egress().map(|e| e.rate_mbps), Some(4.4));
        assert!(stats.ingress().is_some_and(|e| e.connected));
        let ingress = stats.ingress().expect("ingress stats");
        assert_eq!(ingress.packets_received_lost, 5);
        assert_eq!(ingress.packets_received_retransmitted, 5);
        let egress = stats.egress().expect("egress stats");
        assert_eq!(egress.packets_sent_lost, 42);
        assert_eq!(egress.packets_retransmitted, 42);
    }

    #[test]
    fn sums_bytes_received_per_element_from_callers() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [
                    { "recv_rate_mbps": 2.8, "bytes_received": 721_519_840_i64 }
                ]},
                "srtsink_0": { "connected": true, "callers": [
                    { "send_rate_mbps": 2.6, "bytes_received": 0 }
                ]}
            }}
        });
        let stats = parse_flow_stats(&value);
        assert_eq!(stats.ingress().map(|e| e.bytes_received), Some(721_519_840));
        assert_eq!(stats.egress().map(|e| e.bytes_received), Some(0));
    }

    /// A block's SRT element is keyed `<block>:srtsink`, as the bench Strom
    /// (0.6.6) reports it for a `mpegtssrt_output` block.
    #[test]
    fn finds_srt_elements_inside_blocks() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srt_out:srtsink": { "role": "sink", "mode": "listener", "connected": true, "callers": [
                    { "bytes_sent": 4096, "send_rate_mbps": 1.5, "bytes_received": null }
                ]}
            }}
        });
        let stats = parse_flow_stats(&value);
        assert!(stats.ingress().is_none());
        let egress = stats.egress().expect("block srtsink");
        assert_eq!(egress.id, "srt_out:srtsink");
        assert_eq!(egress.rate_mbps, 1.5, "a sink reads the send rate");
        assert_eq!(egress.bytes_sent, 4096);
        assert_eq!(egress.bytes_received, 0);

        let value = serde_json::json!({
            "stats": { "connections": {
                "srt_in:srtsrc": { "role": "source", "connected": true, "callers": [
                    { "bytes_received": 512, "recv_rate_mbps": 0.7 }
                ]}
            }}
        });
        let stats = parse_flow_stats(&value);
        assert_eq!(stats.ingress().map(|e| e.bytes_received), Some(512));
        assert!(stats.egress().is_none());
    }

    #[test]
    fn per_element_connection_detail_is_preserved_across_mixed_states() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [] },
                "srtsink_0": { "connected": false, "callers": [] }
            }}
        });
        let stats = parse_flow_stats(&value);
        assert!(stats.ingress().is_some_and(|e| e.connected));
        assert!(stats.egress().is_some_and(|e| !e.connected));
    }

    #[test]
    fn egresses_are_selected_by_branch_index_with_isolated_stats() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srtsink_1": { "connected": false, "callers": [] },
                "srtsink_0": { "connected": true, "callers": [
                    { "send_rate_mbps": 4.4, "bytes_sent": 4096,
                      "packets_sent_lost": 3, "packets_retransmitted": 2 }
                ]}
            }}
        });
        let stats = parse_flow_stats(&value);

        let first = stats.egress_at(0).expect("branch 0");
        assert_eq!(first.id, "srtsink_0");
        assert_eq!(first.rate_mbps, 4.4);
        assert_eq!(first.packets_sent_lost, 3);
        assert_eq!(first.connections, 1);

        let second = stats.egress_at(1).expect("branch 1");
        assert_eq!(second.id, "srtsink_1");
        assert_eq!(second.rate_mbps, 0.0);
        assert_eq!(second.packets_sent_lost, 0);
        assert_eq!(second.connections, 0);
        assert!(stats.egress_at(2).is_none());
    }

    #[test]
    fn block_egresses_are_selected_by_branch_index() {
        let value = serde_json::json!({
            "stats": { "connections": {
                "srt_out_1:srtsink": { "connected": true, "callers": [
                    { "send_rate_mbps": 1.2 }
                ]},
                "srt_out_0:srtsink": { "connected": true, "callers": [
                    { "send_rate_mbps": 2.3 }
                ]}
            }}
        });
        let stats = parse_flow_stats(&value);

        assert_eq!(stats.egress_at(0).map(|entry| entry.rate_mbps), Some(2.3));
        assert_eq!(stats.egress_at(1).map(|entry| entry.rate_mbps), Some(1.2));
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

    fn recorded(name: &str) -> Value {
        let text = match name {
            "poll-0" => include_str!("testdata/webrtc-stats/whip-whep-poll-0.json"),
            "poll-1" => include_str!("testdata/webrtc-stats/whip-whep-poll-1.json"),
            "ended" => include_str!("testdata/webrtc-stats/whip-whep-ended.json"),
            other => panic!("no recording {other}"),
        };
        serde_json::from_str(text).expect("recorded webrtc-stats")
    }

    /// Recorded from Strom 0.6.6: a WebKit page sending H264 and Opus over WHIP
    /// into `whip_in`, and a Chromium page playing `whep_out_0` over WHEP.
    #[test]
    fn sums_each_blocks_session_bytes_by_direction() {
        let stats = parse_webrtc_stats(&recorded("poll-1"));

        let ingress = stats.ingress().expect("whip_in");
        assert_eq!(ingress.sessions, 1);
        assert_eq!(ingress.bytes_received, 601_663 + 47_216);
        assert_eq!(ingress.bytes_sent, 0);

        let egress = stats.egress_at(0).expect("whep_out_0");
        assert_eq!(egress.sessions, 1);
        assert_eq!(egress.bytes_sent, 554_553 + 50_231);
        assert_eq!(egress.bytes_received, 0);
        assert!(stats.egress_at(1).is_none());
    }

    #[test]
    fn byte_totals_advance_between_recorded_polls() {
        let (first, second) = (
            parse_webrtc_stats(&recorded("poll-0")),
            parse_webrtc_stats(&recorded("poll-1")),
        );
        let received = |s: &WebRtcStats| s.ingress().map(|b| b.bytes_received);
        let sent = |s: &WebRtcStats| s.egress_at(0).map(|b| b.bytes_sent);
        assert!(received(&second) > received(&first));
        assert!(sent(&second) > sent(&first));
    }

    /// Recorded ~18 s after both pages closed their peer connections: the WHIP
    /// session is gone and the WHEP session's entry remains with no RTP streams.
    #[test]
    fn an_ended_session_is_not_counted() {
        let stats = parse_webrtc_stats(&recorded("ended"));
        assert!(stats.ingress().is_none());
        let egress = stats.egress_at(0).expect("the entry Strom kept");
        assert_eq!(egress.sessions, 0);
        assert_eq!(egress.bytes_sent, 0);
    }

    #[test]
    fn sessions_of_one_block_are_summed() {
        let stream = |bytes: i64| serde_json::json!({ "media_type": "video", "bytes": bytes });
        let value = serde_json::json!({ "stats": { "connections": {
            "whep_out_0:session_a:webrtcbin-a": { "inbound_rtp": [], "outbound_rtp": [stream(100)] },
            "whep_out_0:session_b:webrtcbin-b": { "inbound_rtp": [], "outbound_rtp": [stream(50)] },
            "whep_out_0:session_c:webrtcbin-c": { "inbound_rtp": [], "outbound_rtp": [] },
            "webrtcbin7": { "inbound_rtp": [stream(9)], "outbound_rtp": [] }
        }}});
        let stats = parse_webrtc_stats(&value);
        assert_eq!(stats.blocks.len(), 1, "a key without a block is skipped");
        let egress = stats.egress_at(0).expect("whep_out_0");
        assert_eq!((egress.sessions, egress.bytes_sent), (2, 150));
    }

    #[test]
    fn an_unfed_flow_or_absent_stats_have_no_blocks() {
        let unfed = serde_json::json!({ "flow_id": "x", "stats": { "connections": {} } });
        assert_eq!(parse_webrtc_stats(&unfed), WebRtcStats::default());
        assert_eq!(
            parse_webrtc_stats(&serde_json::json!({})),
            WebRtcStats::default()
        );
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
