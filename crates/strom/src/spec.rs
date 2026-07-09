//! Serializable Strom flow spec and mapping from a desired hop.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use weave_core::{DesiredHop, SocketRole, SocketSpec, Transport};

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
    #[error("incomplete socket spec: missing {0}")]
    IncompleteSocket(&'static str),
}

/// Map a desired hop to a linear srtsrc→queue→srtsink Strom flow.
///
/// The flow name is the hop id, so flows are adopted by name.
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
    fn default_latencies_applied_when_hop_latency_absent() {
        let mut hop = demo_ingress_hop("x");
        hop.ingress.params.latency = None;
        hop.egress.params.latency = None;
        let spec = flow_spec_from_hop(&hop).expect("map");
        assert_eq!(spec.elements[0].properties["latency"], Value::from(200));
        assert_eq!(spec.elements[2].properties["latency"], Value::from(1000));
    }
}
