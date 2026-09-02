//! Serializable Strom flow spec and mapping from a desired hop.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use weave_core::{DesiredHop, SocketSpec, SrtSocket};

const PLACEHOLDER_ID: &str = "00000000-0000-0000-0000-000000000000";
const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowSpec {
    pub id: String,
    pub name: String,
    pub elements: Vec<Element>,
    pub links: Vec<Link>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Element {
    pub id: String,
    pub element_type: String,
    pub properties: Map<String, Value>,
    pub position: [f64; 2],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub from: String,
    pub to: String,
}

#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    #[error("hop has no egress socket")]
    NoEgress,
    #[error("no Strom flow shape carries a {0} socket")]
    UnsupportedSocket(String),
}

/// Map a desired hop to a Strom flow.
///
/// A single egress yields a linear srtsrc→queue→srtsink flow; multiple egresses
/// yield a fan-out srtsrc→tee→N×(queue→srtsink). The flow name is the hop id, so
/// flows are adopted by name.
pub fn flow_spec_from_hop(hop: &DesiredHop) -> Result<FlowSpec, MappingError> {
    let src_props = src_props(srt_socket(&hop.ingress)?);

    let sinks = hop
        .egresses
        .iter()
        .map(|egress| srt_socket(egress).map(sink_props))
        .collect::<Result<Vec<_>, _>>()?;

    match sinks.len() {
        0 => Err(MappingError::NoEgress),
        1 => Ok(linear_srt_flow(
            hop.id.clone(),
            src_props,
            sinks.into_iter().next().unwrap_or_default(),
        )),
        _ => Ok(tee_srt_flow(hop.id.clone(), src_props, sinks)),
    }
}

/// The SRT socket a Strom flow is built from. No flow shape carries a WebRTC
/// or device socket yet.
fn srt_socket(spec: &SocketSpec) -> Result<&SrtSocket, MappingError> {
    match spec {
        SocketSpec::Srt(socket) => Ok(socket),
        other => Err(MappingError::UnsupportedSocket(other.to_string())),
    }
}

fn src_props(socket: &SrtSocket) -> Map<String, Value> {
    let mut props = Map::new();
    props.insert("uri".to_string(), Value::String(socket_uri(socket)));
    props.insert(
        "latency".to_string(),
        Value::from(socket.params().latency.unwrap_or(DEFAULT_SRC_LATENCY)),
    );
    // A listener srtsrc otherwise EOSes and never re-binds once its peer
    // disconnects or an idle socket errors; keep-listening reuses the socket.
    if matches!(socket, SrtSocket::Listen { .. }) {
        props.insert("keep-listening".to_string(), Value::Bool(true));
    }
    props
}

fn sink_props(socket: &SrtSocket) -> Map<String, Value> {
    let mut props = Map::new();
    props.insert("uri".to_string(), Value::String(socket_uri(socket)));
    props.insert(
        "latency".to_string(),
        Value::from(socket.params().latency.unwrap_or(DEFAULT_SINK_LATENCY)),
    );
    props.insert("wait-for-connection".to_string(), Value::Bool(false));
    props
}

/// Parse `(host, port)` from an `srt://host:port?...` URI. A listener URI
/// (`srt://:port`) yields an empty host. Returns `None` when the string is not a
/// recognisable `srt://` authority with a numeric port.
#[must_use]
pub fn parse_srt_endpoint(uri: &str) -> Option<(String, u16)> {
    let authority = uri.strip_prefix("srt://")?.split(['?', '/']).next()?;
    let (host, port) = authority.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

fn socket_uri(socket: &SrtSocket) -> String {
    match socket {
        SrtSocket::Listen { port, .. } => format!("srt://:{port}?mode=listener"),
        SrtSocket::Connect { host, port, .. } => format!("srt://{host}:{port}?mode=caller"),
    }
}

fn linear_srt_flow(
    name: String,
    src_props: Map<String, Value>,
    sink_props: Map<String, Value>,
) -> FlowSpec {
    FlowSpec {
        id: PLACEHOLDER_ID.to_string(),
        name,
        elements: vec![
            Element {
                id: "srtsrc_0".to_string(),
                element_type: "srtsrc".to_string(),
                properties: src_props,
                position: [100.0, 200.0],
            },
            Element {
                id: "queue_0".to_string(),
                element_type: "queue".to_string(),
                properties: Map::new(),
                position: [300.0, 200.0],
            },
            Element {
                id: "srtsink_0".to_string(),
                element_type: "srtsink".to_string(),
                properties: sink_props,
                position: [500.0, 200.0],
            },
        ],
        links: vec![
            Link {
                from: "srtsrc_0:src".to_string(),
                to: "queue_0:sink".to_string(),
            },
            Link {
                from: "queue_0:src".to_string(),
                to: "srtsink_0:sink".to_string(),
            },
        ],
    }
}

/// srtsrc→tee→N×(queue→srtsink). The tee's request source pads are named
/// `src_%u` per GStreamer; unnamed request-pad links are dropped by Strom.
fn tee_srt_flow(
    name: String,
    src_props: Map<String, Value>,
    sinks: Vec<Map<String, Value>>,
) -> FlowSpec {
    let mut elements = vec![
        Element {
            id: "srtsrc_0".to_string(),
            element_type: "srtsrc".to_string(),
            properties: src_props,
            position: [100.0, 200.0],
        },
        Element {
            id: "tee_0".to_string(),
            element_type: "tee".to_string(),
            properties: Map::new(),
            position: [300.0, 200.0],
        },
    ];
    let mut links = vec![Link {
        from: "srtsrc_0:src".to_string(),
        to: "tee_0:sink".to_string(),
    }];

    for (i, sink_props) in sinks.into_iter().enumerate() {
        let queue = format!("queue_{i}");
        let sink = format!("srtsink_{i}");
        let y = 200.0 + (i as f64) * 150.0;
        elements.push(Element {
            id: queue.clone(),
            element_type: "queue".to_string(),
            properties: Map::new(),
            position: [500.0, y],
        });
        elements.push(Element {
            id: sink.clone(),
            element_type: "srtsink".to_string(),
            properties: sink_props,
            position: [700.0, y],
        });
        links.push(Link {
            from: format!("tee_0:src_{i}"),
            to: format!("{queue}:sink"),
        });
        links.push(Link {
            from: format!("{queue}:src"),
            to: format!("{sink}:sink"),
        });
    }

    FlowSpec {
        id: PLACEHOLDER_ID.to_string(),
        name,
        elements,
        links,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{DeviceKind, HopRole, SignallingTransport, SocketRole, SrtParams};

    fn demo_ingress_hop(id: &str) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: "strom-node-1".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(7001, 200),
            egresses: vec![SocketSpec::srt_connect("172.31.0.10", 7002, 1000)],
        }
    }

    fn demo_recv_hop(id: &str) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: "strom-node-2".to_string(),
            role: HopRole::Receiver,
            ingress: SocketSpec::srt_listen(7002, 1000),
            egresses: vec![SocketSpec::srt_listen(7003, 200)],
        }
    }

    #[test]
    fn maps_ingress_hop_to_known_good_payload() {
        let spec = flow_spec_from_hop(&demo_ingress_hop("bench-ingress")).expect("map");
        let produced = serde_json::to_value(&spec).expect("serialize");

        let golden: Value = serde_json::from_str(include_str!("testdata/ingress.json"))
            .expect("parse ingress.json");

        assert_eq!(produced, golden);
    }

    #[test]
    fn maps_receiver_hop_to_known_good_payload() {
        let spec = flow_spec_from_hop(&demo_recv_hop("bench-recv")).expect("map");
        let produced = serde_json::to_value(&spec).expect("serialize");

        let golden: Value = serde_json::from_str(include_str!("testdata/receiver.json"))
            .expect("parse receiver.json");

        assert_eq!(produced, golden);
    }

    fn demo_tee_hop(id: &str) -> DesiredHop {
        let mut hop = demo_ingress_hop(id);
        hop.egresses
            .push(SocketSpec::srt_connect("172.31.0.20", 7002, 1000));
        hop
    }

    #[test]
    fn maps_fanout_hop_to_tee_graph() {
        let spec = flow_spec_from_hop(&demo_tee_hop("bench-tee")).expect("map");
        let produced = serde_json::to_value(&spec).expect("serialize");

        let golden: Value =
            serde_json::from_str(include_str!("testdata/tee.json")).expect("parse tee.json");

        assert_eq!(produced, golden);
    }

    #[test]
    fn no_egress_hop_is_an_error() {
        let mut hop = demo_ingress_hop("x");
        hop.egresses.clear();
        assert!(matches!(
            flow_spec_from_hop(&hop),
            Err(MappingError::NoEgress)
        ));
    }

    #[test]
    fn a_socket_no_flow_shape_carries_is_an_error() {
        let mut webrtc = demo_ingress_hop("x");
        webrtc.ingress = SocketSpec::signalling(
            SignallingTransport::Whip,
            SocketRole::Listen,
            "http://172.26.0.10:8080/whip",
            "x",
        );
        let error = flow_spec_from_hop(&webrtc).expect_err("whip has no flow shape");
        assert_eq!(
            error.to_string(),
            "no Strom flow shape carries a whip socket"
        );

        let mut device = demo_ingress_hop("x");
        device.ingress = SocketSpec::Device(DeviceKind::Capture);
        let error = flow_spec_from_hop(&device).expect_err("a device has no flow shape");
        assert_eq!(
            error.to_string(),
            "no Strom flow shape carries a capture device socket"
        );
    }

    #[test]
    fn listener_ingress_keeps_listening_but_caller_ingress_does_not() {
        let listen = flow_spec_from_hop(&demo_ingress_hop("x")).expect("map");
        assert_eq!(
            listen.elements[0].properties["keep-listening"],
            Value::Bool(true)
        );

        let mut caller = demo_ingress_hop("x");
        caller.ingress = SocketSpec::srt_connect("172.31.0.99", 7001, 200);
        let caller = flow_spec_from_hop(&caller).expect("map");
        assert!(!caller.elements[0].properties.contains_key("keep-listening"));
    }

    #[test]
    fn parse_srt_endpoint_reads_host_and_port() {
        assert_eq!(
            parse_srt_endpoint("srt://:7001?mode=listener"),
            Some((String::new(), 7001))
        );
        assert_eq!(
            parse_srt_endpoint("srt://10.0.0.2:7002?mode=caller"),
            Some(("10.0.0.2".to_string(), 7002))
        );
        assert_eq!(parse_srt_endpoint("http://x:1"), None);
        assert_eq!(parse_srt_endpoint("srt://nohost"), None);
    }

    #[test]
    fn default_latencies_applied_when_hop_latency_absent() {
        let mut hop = demo_ingress_hop("x");
        hop.ingress = SocketSpec::Srt(SrtSocket::Listen {
            port: 7001,
            params: SrtParams::default(),
        });
        hop.egresses[0] = SocketSpec::Srt(SrtSocket::Connect {
            host: "172.31.0.10".to_string(),
            port: 7002,
            params: SrtParams::default(),
        });
        let spec = flow_spec_from_hop(&hop).expect("map");
        assert_eq!(spec.elements[0].properties["latency"], Value::from(200));
        assert_eq!(spec.elements[2].properties["latency"], Value::from(1000));
    }
}
