//! Serializable Strom flow spec and mapping from `StreamDefinition`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use weave_core::{SrtEndpoint, SrtMode, StreamDefinition, StreamTransport};

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
    #[error("stream has no destinations")]
    NoDestination,
    #[error("invalid srt url: {0}")]
    InvalidUrl(String),
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

    Ok(FlowSpec {
        id: PLACEHOLDER_ID.to_string(),
        name: stream.name.clone(),
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
    })
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

    fn demo_stream(name: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                url: "srt://0.0.0.0:7001".to_string(),
                mode: SrtMode::Listener,
                latency: Some(200),
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                url: "srt://172.31.0.10:7002".to_string(),
                mode: SrtMode::Caller,
                latency: Some(1000),
            })],
        }
    }

    #[test]
    fn maps_demo_stream_to_known_good_ingress_payload() {
        let spec = flow_spec_from_stream(&demo_stream("bench-ingress")).expect("map");
        let produced = serde_json::to_value(&spec).expect("serialize");

        let golden: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../bench/flows/ingress.json"
        )))
        .expect("parse ingress.json");

        assert_eq!(produced, golden);
    }

    #[test]
    fn listener_uri_drops_host_caller_keeps_it() {
        let src = SrtEndpoint {
            url: "srt://0.0.0.0:7001".to_string(),
            mode: SrtMode::Listener,
            latency: None,
        };
        let sink = SrtEndpoint {
            url: "srt://172.31.0.10:7002?mode=caller".to_string(),
            mode: SrtMode::Caller,
            latency: None,
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
