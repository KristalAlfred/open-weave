//! Serializable Strom flow spec and mapping from a desired hop.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use weave_core::{DesiredHop, SignallingSocket, SocketSpec, SrtSocket};

const PLACEHOLDER_ID: &str = "00000000-0000-0000-0000-000000000000";
const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowSpec {
    pub id: String,
    pub name: String,
    pub elements: Vec<Element>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocks: Vec<Block>,
    pub links: Vec<Link>,
}

/// A Strom block: a packaged sub-pipeline addressed by `block_definition_id`,
/// linked through its external pads as `<id>:<pad>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub id: String,
    pub block_definition_id: String,
    pub name: String,
    pub properties: Map<String, Value>,
    pub position: [f64; 2],
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
    #[error(
        "no Strom flow shape fans one {ingress} ingress out over both {first} and {second} egresses"
    )]
    MixedEgress {
        ingress: String,
        first: String,
        second: String,
    },
    #[error(
        "media progress cannot be reported for a {ingress} ingress feeding a {egress} egress: the Strom adapter reads progress from a hop's SRT byte counters and this hop has no SRT side"
    )]
    WebRtcOnBothSides { ingress: String, egress: String },
}

/// Map a desired hop to a Strom flow. The flow name is the hop id, so flows are
/// adopted by name.
///
/// The shape follows the hop's (ingress, egress) sockets:
///
/// - `srt → srt`: elements only, a byte relay. A single egress yields
///   srtsrc→queue→srtsink; several yield srtsrc→tee→N×(queue→srtsink).
/// - `whip → srt`: a gateway from a WHIP ingest hosted here to an SRT socket,
///   `whip_input → videoenc → mpegtssrt_output`, audio passed straight to the
///   muxer. Several egresses tee after the encoder.
/// - `srt → whep`: `mpegtssrt_input(decode) → whep_output`. Several egresses tee
///   the decoded video and audio.
///
/// A hop with WebRTC on both sides is [`MappingError::WebRtcOnBothSides`]:
/// Strom can build `whip_input → whep_output`, but the adapter reports media
/// progress from the hop's SRT byte counters and such a hop has none.
///
/// Every egress of a hop must ask for one shape; a hop asked to fan out over two
/// is [`MappingError::MixedEgress`].
pub fn flow_spec_from_hop(hop: &DesiredHop) -> Result<FlowSpec, MappingError> {
    let egress = sole_egress(hop)?;
    match (shape(&hop.ingress), shape(egress)) {
        (Shape::Srt, Shape::Srt) => srt_relay_flow(hop),
        (Shape::Whip, Shape::Srt) => whip_to_srt_flow(hop),
        (Shape::Srt, Shape::Whep) => srt_to_whep_flow(hop),
        (Shape::Whip, Shape::Whep) => Err(MappingError::WebRtcOnBothSides {
            ingress: hop.ingress.to_string(),
            egress: egress.to_string(),
        }),
        (_, Shape::Srt | Shape::Whep) => {
            Err(MappingError::UnsupportedSocket(hop.ingress.to_string()))
        }
        (_, _) => Err(MappingError::UnsupportedSocket(egress.to_string())),
    }
}

/// The flow shape a socket asks for, setting aside the address it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Srt,
    Whip,
    Whep,
    Device,
}

fn shape(spec: &SocketSpec) -> Shape {
    match spec {
        SocketSpec::Srt(_) => Shape::Srt,
        SocketSpec::Whip(_) => Shape::Whip,
        SocketSpec::Whep(_) => Shape::Whep,
        SocketSpec::Device(_) => Shape::Device,
    }
}

/// The first egress of `hop`, checking that every egress asks for the same
/// shape: one flow carries one egress shape.
fn sole_egress(hop: &DesiredHop) -> Result<&SocketSpec, MappingError> {
    let first = hop.egresses.first().ok_or(MappingError::NoEgress)?;
    match hop.egresses.iter().find(|e| shape(e) != shape(first)) {
        Some(other) => Err(MappingError::MixedEgress {
            ingress: hop.ingress.to_string(),
            first: first.to_string(),
            second: other.to_string(),
        }),
        None => Ok(first),
    }
}

fn srt_relay_flow(hop: &DesiredHop) -> Result<FlowSpec, MappingError> {
    let src_props = src_props(srt_socket(&hop.ingress)?);

    let mut sinks = hop
        .egresses
        .iter()
        .map(|egress| srt_socket(egress).map(sink_props))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(if sinks.len() == 1 {
        linear_srt_flow(hop.id.clone(), src_props, sinks.remove(0))
    } else {
        tee_srt_flow(hop.id.clone(), src_props, sinks)
    })
}

/// The SRT socket a Strom element or SRT block is built from.
fn srt_socket(spec: &SocketSpec) -> Result<&SrtSocket, MappingError> {
    match spec {
        SocketSpec::Srt(socket) => Ok(socket),
        other => Err(MappingError::UnsupportedSocket(other.to_string())),
    }
}

/// The signalling socket a WHIP or WHEP block is built from.
fn signalling_socket(spec: &SocketSpec) -> Result<&SignallingSocket, MappingError> {
    match spec {
        SocketSpec::Whip(socket) | SocketSpec::Whep(socket) => Ok(socket),
        other => Err(MappingError::UnsupportedSocket(other.to_string())),
    }
}

fn whip_input_block(id: &str, socket: &SignallingSocket) -> Block {
    let mut props = Map::new();
    props.insert(
        "endpoint_id".to_string(),
        Value::String(socket.endpoint_id.clone()),
    );
    props.insert("mode".to_string(), Value::String("audio_video".to_string()));
    props.insert("decode".to_string(), Value::Bool(true));
    props.insert("max_sessions".to_string(), Value::from(1));
    block(
        id,
        "builtin.whip_input",
        "WHIP ingest",
        props,
        [100.0, 200.0],
    )
}

fn whep_output_block(id: &str, socket: &SignallingSocket, position: [f64; 2]) -> Block {
    let mut props = Map::new();
    props.insert(
        "endpoint_id".to_string(),
        Value::String(socket.endpoint_id.clone()),
    );
    block(id, "builtin.whep_output", "WHEP playback", props, position)
}

fn videoenc_block(id: &str, position: [f64; 2]) -> Block {
    let mut props = Map::new();
    props.insert("codec".to_string(), Value::String("h264".to_string()));
    block(id, "builtin.videoenc", "H.264 encoder", props, position)
}

fn mpegtssrt_output_block(id: &str, socket: &SrtSocket, position: [f64; 2]) -> Block {
    let mut props = Map::new();
    props.insert("srt_uri".to_string(), Value::String(socket_uri(socket)));
    props.insert(
        "latency".to_string(),
        Value::from(socket.params().latency.unwrap_or(DEFAULT_SINK_LATENCY)),
    );
    props.insert("wait_for_connection".to_string(), Value::Bool(false));
    block(
        id,
        "builtin.mpegtssrt_output",
        "SRT output",
        props,
        position,
    )
}

fn mpegtssrt_input_block(id: &str, socket: &SrtSocket) -> Block {
    let mut props = Map::new();
    props.insert("srt_uri".to_string(), Value::String(socket_uri(socket)));
    props.insert(
        "latency".to_string(),
        Value::from(socket.params().latency.unwrap_or(DEFAULT_SRC_LATENCY)),
    );
    props.insert("decode".to_string(), Value::Bool(true));
    if matches!(socket, SrtSocket::Listen { .. }) {
        props.insert("keep_listening".to_string(), Value::Bool(true));
    }
    block(
        id,
        "builtin.mpegtssrt_input",
        "SRT input",
        props,
        [100.0, 200.0],
    )
}

fn block(
    id: &str,
    definition: &str,
    name: &str,
    properties: Map<String, Value>,
    position: [f64; 2],
) -> Block {
    Block {
        id: id.to_string(),
        block_definition_id: definition.to_string(),
        name: name.to_string(),
        properties,
        position,
    }
}

fn element(id: &str, element_type: &str, position: [f64; 2]) -> Element {
    Element {
        id: id.to_string(),
        element_type: element_type.to_string(),
        properties: Map::new(),
        position,
    }
}

fn link(from: &str, to: &str) -> Link {
    Link {
        from: from.to_string(),
        to: to.to_string(),
    }
}

/// A decoded video and audio pair fanned out to `count` consumers. One consumer
/// links straight; more go through a tee and a queue per branch, since a src pad
/// links once and tee branches need a queue each to run independently. Returns
/// the `(video, audio)` pad to link each consumer's inputs from.
fn fan_out(
    spec: &mut FlowSpec,
    video_src: &str,
    audio_src: &str,
    count: usize,
) -> Vec<(String, String)> {
    if count <= 1 {
        return vec![(video_src.to_string(), audio_src.to_string())];
    }
    spec.elements.push(element("tee_v", "tee", [450.0, 200.0]));
    spec.elements.push(element("tee_a", "tee", [450.0, 350.0]));
    spec.links.push(link(video_src, "tee_v:sink"));
    spec.links.push(link(audio_src, "tee_a:sink"));
    (0..count)
        .map(|i| {
            let y = 200.0 + (i as f64) * 150.0;
            let (qv, qa) = (format!("queue_v{i}"), format!("queue_a{i}"));
            spec.elements.push(element(&qv, "queue", [600.0, y]));
            spec.elements.push(element(&qa, "queue", [600.0, y + 50.0]));
            spec.links
                .push(link(&format!("tee_v:src_{i}"), &format!("{qv}:sink")));
            spec.links
                .push(link(&format!("tee_a:src_{i}"), &format!("{qa}:sink")));
            (format!("{qv}:src"), format!("{qa}:src"))
        })
        .collect()
}

fn empty_flow(name: &str) -> FlowSpec {
    FlowSpec {
        id: PLACEHOLDER_ID.to_string(),
        name: name.to_string(),
        elements: Vec::new(),
        blocks: Vec::new(),
        links: Vec::new(),
    }
}

/// `whip_input → videoenc → mpegtssrt_output`, audio straight into the muxer.
fn whip_to_srt_flow(hop: &DesiredHop) -> Result<FlowSpec, MappingError> {
    let mut spec = empty_flow(&hop.id);
    spec.blocks.push(whip_input_block(
        "whip_in",
        signalling_socket(&hop.ingress)?,
    ));
    spec.blocks.push(videoenc_block("venc", [300.0, 200.0]));
    spec.links.push(link("whip_in:video_out", "venc:video_in"));

    let branches = fan_out(
        &mut spec,
        "venc:encoded_out",
        "whip_in:audio_out",
        hop.egresses.len(),
    );
    for (i, (egress, (video, audio))) in hop.egresses.iter().zip(branches).enumerate() {
        let id = format!("srt_out_{i}");
        let y = 200.0 + (i as f64) * 150.0;
        spec.blocks
            .push(mpegtssrt_output_block(&id, srt_socket(egress)?, [800.0, y]));
        spec.links.push(link(&video, &format!("{id}:video_in")));
        spec.links.push(link(&audio, &format!("{id}:audio_in_0")));
    }
    Ok(spec)
}

/// `mpegtssrt_input(decode) → whep_output`.
fn srt_to_whep_flow(hop: &DesiredHop) -> Result<FlowSpec, MappingError> {
    let mut spec = empty_flow(&hop.id);
    spec.blocks
        .push(mpegtssrt_input_block("srt_in", srt_socket(&hop.ingress)?));
    let branches = fan_out(
        &mut spec,
        "srt_in:video_out",
        "srt_in:audio_out_0",
        hop.egresses.len(),
    );
    for (i, (egress, (video, audio))) in hop.egresses.iter().zip(branches).enumerate() {
        let id = format!("whep_out_{i}");
        let y = 200.0 + (i as f64) * 150.0;
        spec.blocks.push(whep_output_block(
            &id,
            signalling_socket(egress)?,
            [800.0, y],
        ));
        spec.links.push(link(&video, &format!("{id}:video_in")));
        spec.links.push(link(&audio, &format!("{id}:audio_in")));
    }
    Ok(spec)
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
        blocks: Vec::new(),
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
        blocks: Vec::new(),
        links,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{
        DeviceKind, HopRole, SignallingSocket, SignallingTransport, SocketRole, SrtParams,
    };

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
        let mut pulled = demo_ingress_hop("x");
        pulled.ingress = SocketSpec::signalling(
            SignallingTransport::Whep,
            SocketRole::Connect,
            "http://172.26.0.10:8080/whep",
            "x",
        );
        let error = flow_spec_from_hop(&pulled).expect_err("Strom cannot pull WHEP in");
        assert_eq!(
            error.to_string(),
            "no Strom flow shape carries a whep socket"
        );

        let mut pushed = demo_ingress_hop("x");
        pushed.egresses = vec![SocketSpec::signalling(
            SignallingTransport::Whip,
            SocketRole::Connect,
            "http://172.26.0.10:8080/whip",
            "x",
        )];
        let error = flow_spec_from_hop(&pushed).expect_err("Strom cannot push WHIP out");
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

    fn whip_socket(role: SocketRole, endpoint: &str) -> SocketSpec {
        SocketSpec::signalling(
            SignallingTransport::Whip,
            role,
            "http://172.27.0.10:8080/whip",
            endpoint,
        )
    }

    fn whep_socket(role: SocketRole, endpoint: &str) -> SocketSpec {
        SocketSpec::signalling(
            SignallingTransport::Whep,
            role,
            "http://172.27.0.10:8080/whep",
            endpoint,
        )
    }

    /// The Strom side of `alice-cam`: a browser pushes WHIP in, a consumer pulls
    /// SRT out.
    fn whip_gateway_hop() -> DesiredHop {
        DesiredHop {
            id: "weave-alice-cam-receiver-0".to_string(),
            node_id: "strom-node-2".to_string(),
            role: HopRole::Receiver,
            ingress: whip_socket(SocketRole::Listen, "weave-alice-cam-receiver-0"),
            egresses: vec![SocketSpec::srt_listen(7003, 200)],
        }
    }

    /// The Strom side of `alice-return`: a producer pushes SRT in, a browser
    /// pulls WHEP out.
    fn whep_gateway_hop() -> DesiredHop {
        DesiredHop {
            id: "weave-alice-return-sender".to_string(),
            node_id: "strom-node-2".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(7001, 200),
            egresses: vec![whep_socket(
                SocketRole::Listen,
                "weave-alice-return-receiver-0",
            )],
        }
    }

    #[test]
    fn maps_whip_ingress_to_srt_gateway_payload() {
        let spec = flow_spec_from_hop(&whip_gateway_hop()).expect("map");
        let produced = serde_json::to_value(&spec).expect("serialize");
        let golden: Value = serde_json::from_str(include_str!("testdata/whip-srt.json"))
            .expect("parse whip-srt.json");
        assert_eq!(
            produced,
            golden,
            "{}",
            serde_json::to_string_pretty(&produced).unwrap()
        );
    }

    #[test]
    fn maps_srt_ingress_to_whep_gateway_payload() {
        let spec = flow_spec_from_hop(&whep_gateway_hop()).expect("map");
        let produced = serde_json::to_value(&spec).expect("serialize");
        let golden: Value = serde_json::from_str(include_str!("testdata/srt-whep.json"))
            .expect("parse srt-whep.json");
        assert_eq!(
            produced,
            golden,
            "{}",
            serde_json::to_string_pretty(&produced).unwrap()
        );
    }

    #[test]
    fn whip_block_endpoint_id_comes_from_the_carried_field_not_the_url() {
        let mut hop = whip_gateway_hop();
        hop.ingress = SocketSpec::Whip(SignallingSocket {
            role: SocketRole::Listen,
            url: "http://172.27.0.10:8080/whip/not-the-endpoint-id".to_string(),
            endpoint_id: "weave-alice-cam-receiver-0".to_string(),
        });
        let spec = flow_spec_from_hop(&hop).expect("map");
        assert_eq!(
            spec.blocks[0].properties["endpoint_id"],
            Value::from("weave-alice-cam-receiver-0")
        );
    }

    #[test]
    fn a_hop_with_webrtc_on_both_sides_is_refused() {
        let mut hop = whip_gateway_hop();
        hop.egresses = vec![whep_socket(SocketRole::Listen, "weave-relayed-receiver-0")];
        let error = flow_spec_from_hop(&hop).expect_err("no SRT side to read progress from");
        assert_eq!(
            error.to_string(),
            "media progress cannot be reported for a whip ingress feeding a whep egress: \
             the Strom adapter reads progress from a hop's SRT byte counters and this hop \
             has no SRT side"
        );
    }

    #[test]
    fn whep_fanout_tees_decoded_video_and_audio_through_queues() {
        let mut hop = whep_gateway_hop();
        hop.egresses.push(whep_socket(
            SocketRole::Listen,
            "weave-alice-return-receiver-1",
        ));
        let spec = flow_spec_from_hop(&hop).expect("map");

        let elements: Vec<(&str, &str)> = spec
            .elements
            .iter()
            .map(|e| (e.id.as_str(), e.element_type.as_str()))
            .collect();
        assert_eq!(
            elements,
            vec![
                ("tee_v", "tee"),
                ("tee_a", "tee"),
                ("queue_v0", "queue"),
                ("queue_a0", "queue"),
                ("queue_v1", "queue"),
                ("queue_a1", "queue"),
            ]
        );
        assert_eq!(spec.blocks.len(), 3, "one input, two outputs");
        let has = |from: &str, to: &str| spec.links.iter().any(|l| l.from == from && l.to == to);
        assert!(has("srt_in:video_out", "tee_v:sink"));
        assert!(has("tee_v:src_1", "queue_v1:sink"));
        assert!(has("queue_v1:src", "whep_out_1:video_in"));
        assert!(has("queue_a1:src", "whep_out_1:audio_in"));
        assert!(
            !has("srt_in:video_out", "whep_out_0:video_in"),
            "a src pad links once; every branch goes through the tee"
        );
    }

    #[test]
    fn mixed_egress_transports_are_an_error() {
        let mut hop = whep_gateway_hop();
        hop.egresses
            .push(SocketSpec::srt_connect("172.26.0.10", 7002, 1000));
        let error = flow_spec_from_hop(&hop).expect_err("one flow carries one egress shape");
        assert_eq!(
            error.to_string(),
            "no Strom flow shape fans one srt ingress out over both whep and srt egresses"
        );
    }

    #[test]
    fn element_flows_serialize_without_a_blocks_field() {
        let spec = flow_spec_from_hop(&demo_ingress_hop("x")).expect("map");
        let value = serde_json::to_value(&spec).unwrap();
        assert!(
            value.get("blocks").is_none(),
            "unchanged wire shape for SRT relays"
        );
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
