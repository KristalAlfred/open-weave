//! Serializable Strom flow spec and mapping from `StreamDefinition`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use weave_core::{
    DesiredHop, SocketRole, SocketSpec, SrtEndpoint, SrtMode, StreamDefinition, StreamTransport,
    Transport,
};

const PLACEHOLDER_ID: &str = "00000000-0000-0000-0000-000000000000";
const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;
const RECV_NAME_SUFFIX: &str = "-recv";
const RECV_CONSUMER_LATENCY: u32 = 200;

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
    #[error("stream has no destinations")]
    NoDestination,
    #[error("invalid srt url: {0}")]
    InvalidUrl(String),
    #[error("incomplete socket spec: missing {0}")]
    IncompleteSocket(&'static str),
}

/// Map operator intent (source + first destination) to a linear srtsrc→queue→srtsink flow.
///
/// Multi-destination fan-out (tee) is not yet modelled; only the first destination is used.
pub fn flow_spec_from_stream(stream: &StreamDefinition) -> Result<FlowSpec, MappingError> {
    let source = as_srt(&stream.source);
    let destination = stream
        .destinations
        .first()
        .ok_or(MappingError::NoDestination)?;
    let sink = as_srt(destination);

    let mut src_props = Map::new();
    src_props.insert("uri".to_string(), Value::String(srt_uri(source)?));
    src_props.insert(
        "latency".to_string(),
        Value::from(source.latency.unwrap_or(DEFAULT_SRC_LATENCY)),
    );

    let mut sink_props = Map::new();
    sink_props.insert("uri".to_string(), Value::String(srt_uri(sink)?));
    sink_props.insert(
        "latency".to_string(),
        Value::from(sink.latency.unwrap_or(DEFAULT_SINK_LATENCY)),
    );
    sink_props.insert("wait-for-connection".to_string(), Value::Bool(false));

    Ok(linear_srt_flow(stream.name.clone(), src_props, sink_props))
}

/// Name of the receiver flow placed on the destination node for a stream.
#[must_use]
pub fn receiver_flow_name(stream_name: &str) -> String {
    format!("{stream_name}{RECV_NAME_SUFFIX}")
}

/// Build the receiver flow to run on the destination node's Strom.
///
/// A listener on the destination port accepts the sender's SRT caller; media is
/// forwarded to a second listener on `port + 1` where a downstream consumer attaches.
pub fn receiver_flow_from_stream(stream: &StreamDefinition) -> Result<FlowSpec, MappingError> {
    let destination = stream
        .destinations
        .first()
        .ok_or(MappingError::NoDestination)?;
    let dest = as_srt(destination);
    let (_, port) = split_host_port(&dest.url)?;
    let consumer_port = port
        .checked_add(1)
        .ok_or_else(|| MappingError::InvalidUrl(dest.url.clone()))?;

    let mut src_props = Map::new();
    src_props.insert(
        "uri".to_string(),
        Value::String(format!("srt://:{port}?mode=listener")),
    );
    src_props.insert(
        "latency".to_string(),
        Value::from(dest.latency.unwrap_or(DEFAULT_SINK_LATENCY)),
    );

    let mut sink_props = Map::new();
    sink_props.insert(
        "uri".to_string(),
        Value::String(format!("srt://:{consumer_port}?mode=listener")),
    );
    sink_props.insert("latency".to_string(), Value::from(RECV_CONSUMER_LATENCY));
    sink_props.insert("wait-for-connection".to_string(), Value::Bool(false));

    Ok(linear_srt_flow(
        receiver_flow_name(&stream.name),
        src_props,
        sink_props,
    ))
}

/// Map a desired hop to a linear srtsrc→queue→srtsink Strom flow.
///
/// Byte-compatible with `flow_spec_from_stream`/`receiver_flow_from_stream` for
/// equivalent inputs. The flow name is the hop id, so flows are adopted by name.
pub fn flow_spec_from_hop(hop: &DesiredHop) -> Result<FlowSpec, MappingError> {
    let mut src_props = Map::new();
    src_props.insert("uri".to_string(), Value::String(socket_uri(&hop.ingress)?));
    src_props.insert(
        "latency".to_string(),
        Value::from(hop.ingress.params.latency.unwrap_or(DEFAULT_SRC_LATENCY)),
    );

    let mut sink_props = Map::new();
    sink_props.insert("uri".to_string(), Value::String(socket_uri(&hop.egress)?));
    sink_props.insert(
        "latency".to_string(),
        Value::from(hop.egress.params.latency.unwrap_or(DEFAULT_SINK_LATENCY)),
    );
    sink_props.insert("wait-for-connection".to_string(), Value::Bool(false));

    Ok(linear_srt_flow(hop.id.clone(), src_props, sink_props))
}

fn socket_uri(spec: &SocketSpec) -> Result<String, MappingError> {
    let Transport::Srt = spec.transport;
    let port = spec.port.ok_or(MappingError::IncompleteSocket("port"))?;
    Ok(match spec.role {
        SocketRole::Listen => format!("srt://:{port}?mode=listener"),
        SocketRole::Connect => {
            let host = spec
                .host
                .as_deref()
                .ok_or(MappingError::IncompleteSocket("host"))?;
            format!("srt://{host}:{port}?mode=caller")
        }
    })
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

fn as_srt(transport: &StreamTransport) -> &SrtEndpoint {
    match transport {
        StreamTransport::Srt(endpoint) => endpoint,
    }
}

fn srt_uri(endpoint: &SrtEndpoint) -> Result<String, MappingError> {
    let (host, port) = split_host_port(&endpoint.url)?;
    Ok(match endpoint.mode {
        SrtMode::Listener => format!("srt://:{port}?mode=listener"),
        SrtMode::Caller => format!("srt://{host}:{port}?mode=caller"),
    })
}

fn split_host_port(url: &str) -> Result<(String, u16), MappingError> {
    let authority = url
        .strip_prefix("srt://")
        .unwrap_or(url)
        .split(['?', '/'])
        .next()
        .unwrap_or_default();
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| MappingError::InvalidUrl(url.to_string()))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| MappingError::InvalidUrl(url.to_string()))?;
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{HopRole, SrtParams};

    fn demo_ingress_hop(id: &str) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: "strom-node-1".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Listen,
                host: None,
                port: Some(7001),
                params: SrtParams { latency: Some(200) },
            },
            egress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Connect,
                host: Some("172.31.0.10".to_string()),
                port: Some(7002),
                params: SrtParams {
                    latency: Some(1000),
                },
            },
        }
    }

    fn demo_recv_hop(id: &str) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: "strom-node-2".to_string(),
            role: HopRole::Receiver,
            ingress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Listen,
                host: None,
                port: Some(7002),
                params: SrtParams {
                    latency: Some(1000),
                },
            },
            egress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Listen,
                host: None,
                port: Some(7003),
                params: SrtParams { latency: Some(200) },
            },
        }
    }

    fn demo_stream(name: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                url: "srt://0.0.0.0:7001".to_string(),
                mode: SrtMode::Listener,
                latency: Some(200),
                node: None,
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                url: "srt://172.31.0.10:7002".to_string(),
                mode: SrtMode::Caller,
                latency: Some(1000),
                node: None,
            })],
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

    #[test]
    fn hop_flow_matches_stream_flow_for_equivalent_inputs() {
        let from_hop = flow_spec_from_hop(&demo_ingress_hop("bench-ingress")).expect("hop");
        let from_stream = flow_spec_from_stream(&demo_stream("bench-ingress")).expect("stream");
        assert_eq!(from_hop, from_stream);
    }

    #[test]
    fn connect_socket_without_host_is_an_error() {
        let mut hop = demo_ingress_hop("x");
        hop.egress.host = None;
        assert!(matches!(
            flow_spec_from_hop(&hop),
            Err(MappingError::IncompleteSocket("host"))
        ));
    }

    #[test]
    fn socket_without_port_is_an_error() {
        let mut hop = demo_ingress_hop("x");
        hop.ingress.port = None;
        assert!(matches!(
            flow_spec_from_hop(&hop),
            Err(MappingError::IncompleteSocket("port"))
        ));
    }

    #[test]
    fn receiver_flow_name_appends_suffix() {
        assert_eq!(receiver_flow_name("cam1-to-studio"), "cam1-to-studio-recv");
    }

    #[test]
    fn receiver_listens_on_dest_port_and_consumer_on_next_port() {
        let spec = receiver_flow_from_stream(&demo_stream("x")).expect("map");
        assert_eq!(
            spec.elements[0].properties["uri"],
            Value::from("srt://:7002?mode=listener")
        );
        assert_eq!(
            spec.elements[2].properties["uri"],
            Value::from("srt://:7003?mode=listener")
        );
    }

    #[test]
    fn receiver_missing_destination_is_an_error() {
        let mut stream = demo_stream("x");
        stream.destinations.clear();
        assert!(matches!(
            receiver_flow_from_stream(&stream),
            Err(MappingError::NoDestination)
        ));
    }

    #[test]
    fn listener_uri_drops_host_caller_keeps_it() {
        let src = SrtEndpoint {
            url: "srt://0.0.0.0:7001".to_string(),
            mode: SrtMode::Listener,
            latency: None,
            node: None,
        };
        let sink = SrtEndpoint {
            url: "srt://172.31.0.10:7002?mode=caller".to_string(),
            mode: SrtMode::Caller,
            latency: None,
            node: None,
        };
        assert_eq!(srt_uri(&src).unwrap(), "srt://:7001?mode=listener");
        assert_eq!(
            srt_uri(&sink).unwrap(),
            "srt://172.31.0.10:7002?mode=caller"
        );
    }

    #[test]
    fn default_latencies_applied_when_absent() {
        let mut stream = demo_stream("x");
        let StreamTransport::Srt(s) = &mut stream.source;
        s.latency = None;
        let StreamTransport::Srt(d) = &mut stream.destinations[0];
        d.latency = None;
        let spec = flow_spec_from_stream(&stream).unwrap();
        assert_eq!(spec.elements[0].properties["latency"], Value::from(200));
        assert_eq!(spec.elements[2].properties["latency"], Value::from(1000));
    }

    #[test]
    fn missing_destination_is_an_error() {
        let mut stream = demo_stream("x");
        stream.destinations.clear();
        assert!(matches!(
            flow_spec_from_stream(&stream),
            Err(MappingError::NoDestination)
        ));
    }
}
