//! Shared domain types for open-weave.

pub mod api;
pub mod auth;
pub mod contracts;
pub mod media;
pub mod validation;
pub mod webhook;

use std::{collections::BTreeMap, ops::Deref};

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

pub use api::{
    AcceptedState, ApiError, ApiErrorCode, NodeAccepted, PlanStatus, RunningStatus, StartingState,
    StartingStatus, StatusResponse, StreamAccepted, StreamCondition, StreamConditionReason,
    StreamConditionStatus, StreamConditionType, StreamDestinationStatus, StreamPlan,
    StreamResource, StreamSetAccepted, StreamSetAction, StreamSetApply, StreamSetMemberResult,
    StreamSetResource, StreamStatus,
};
pub use media::{
    AudioCodec, AudioConstraint, AudioFormat, ChromaSubsampling, Container, FormatConstraint,
    Framerate, MediaFormat, Mismatch, VideoCodec, VideoConstraint, VideoFormat,
};
pub use validation::{
    RESOURCE_ID_MAX_LEN, ResourceIdError, ValidationIssue, resource_id_issue, validate_node,
    validate_resource_id, validate_stream,
};

pub const ROUTE_ENDPOINTS: &str = "/endpoints";
pub const ROUTE_NODES: &str = "/nodes";
pub const ROUTE_NODE_DESIRED: &str = "/nodes/{node_id}/desired";
pub const ROUTE_NODE_HEARTBEAT: &str = "/nodes/{node_id}/heartbeat";
pub const ROUTE_NODE_REGISTER: &str = "/nodes/register";
pub const ROUTE_STATE: &str = "/state";
pub const ROUTE_STATUS: &str = "/status";
pub const ROUTE_STREAM: &str = "/streams/{name}";
pub const ROUTE_STREAM_ENDPOINTS: &str = "/streams/{name}/endpoints";
pub const ROUTE_STREAM_PLANS: &str = "/stream-plans";
pub const ROUTE_STREAM_SET: &str = "/stream-sets/{owner}";
pub const ROUTE_STREAM_SETS: &str = "/stream-sets";
pub const ROUTE_STREAMS: &str = "/streams";

/// Wire-protocol version an adapter declares when it registers.
///
/// A stale adapter is rejected at registration instead of being served desired
/// state it cannot realise. This changes when southbound behavior or payload
/// semantics become incompatible.
pub const PROTOCOL_VERSION: u32 = 5;

/// Whether the controller can serve an adapter declaring protocol `version`.
///
/// Exactly one version is supported at a time, so equality is the whole rule.
/// `0` is what an adapter predating the handshake deserializes to (the field
/// defaults) and is never compatible.
#[must_use]
pub fn protocol_compatible(version: u32) -> bool {
    version == PROTOCOL_VERSION
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    fn endpoint(node: &str) -> StreamTransport {
        StreamTransport::Srt(SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        })
    }

    fn destination(id: &str, node: &str) -> StreamDestination {
        StreamDestination {
            id: id.to_string(),
            paths: 1,
            endpoint: endpoint(node),
        }
    }

    #[test]
    fn destination_order_is_not_semantic() {
        let left = StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            source: endpoint("source"),
            destinations: vec![destination("studio", "a"), destination("preview", "b")],
        };
        let mut right = left.clone();
        right.destinations.reverse();
        assert_eq!(left, right);
    }

    #[test]
    fn duplicate_destination_ids_are_rejected() {
        let stream = StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            source: endpoint("source"),
            destinations: vec![destination("studio", "a"), destination("studio", "b")],
        };
        assert!(
            validate_stream(&stream)
                .iter()
                .any(|issue| { issue.field == "destinations[1].id" && issue.code == "duplicate" })
        );
    }

    #[test]
    fn destination_endpoint_is_flattened() {
        let value = serde_json::to_value(destination("studio", "studio-node")).unwrap();
        assert_eq!(value["id"], "studio");
        assert_eq!(value["srt"]["node"], "studio-node");
        assert!(value.get("endpoint").is_none());
    }

    #[test]
    fn a_keyed_socket_round_trips_and_debug_never_prints_the_key() {
        let socket = SocketSpec::Srt(SrtSocket::Listen {
            port: 7001,
            params: SrtParams {
                latency: Some(200),
                passphrase: Some(Passphrase::new("correct horse battery")),
                pbkeylen: Some(32),
            },
        });
        let value = serde_json::to_value(&socket).unwrap();
        assert_eq!(value["params"]["passphrase"], "correct horse battery");
        assert_eq!(value["params"]["pbkeylen"], 32);
        assert_eq!(serde_json::from_value::<SocketSpec>(value).unwrap(), socket);
        assert!(!format!("{socket:?}").contains("correct horse"));
    }

    #[test]
    fn a_socket_without_a_key_decodes_unkeyed() {
        let socket: SocketSpec = serde_json::from_value(serde_json::json!({
            "transport": "srt",
            "role": "listen",
            "port": 7001,
            "params": { "latency": 200 }
        }))
        .unwrap();
        let SocketSpec::Srt(socket) = socket else {
            panic!("expected an SRT socket");
        };
        assert_eq!(socket.params().passphrase, None);
        assert_eq!(socket.params().pbkeylen, None);
    }

    #[test]
    fn destination_paths_default_to_one_and_serialize_only_when_two() {
        let parsed: StreamDestination = serde_json::from_value(serde_json::json!({
            "id": "studio",
            "srt": { "node": "studio-node" }
        }))
        .unwrap();
        assert_eq!(parsed.paths, 1);
        assert!(
            serde_json::to_value(&parsed)
                .unwrap()
                .get("paths")
                .is_none()
        );

        let two: StreamDestination = serde_json::from_value(serde_json::json!({
            "id": "studio",
            "paths": 2,
            "srt": { "node": "studio-node" }
        }))
        .unwrap();
        assert_eq!(two.paths, 2);
        assert_eq!(serde_json::to_value(&two).unwrap()["paths"], 2);
        assert!(
            serde_json::from_value::<StreamDestination>(serde_json::json!({
                "id": "studio",
                "path": 2,
                "srt": { "node": "studio-node" }
            }))
            .is_err()
        );
    }

    #[test]
    fn superseded_capability_fields_are_rejected() {
        let result = serde_json::from_value::<NodeCapabilities>(serde_json::json!({
            "transports": ["srt"],
            "relay": true
        }));
        assert!(result.is_err());
    }
}

/// Operator intent: one source streamed to one or more destinations over a transport.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamDefinition {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub name: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub source: StreamTransport,
    #[schemars(length(min = 1))]
    pub destinations: Vec<StreamDestination>,
}

impl PartialEq for StreamDefinition {
    fn eq(&self, other: &Self) -> bool {
        if self.name != other.name || self.enabled != other.enabled || self.source != other.source {
            return false;
        }
        let mut left: Vec<_> = self.destinations.iter().collect();
        let mut right: Vec<_> = other.destinations.iter().collect();
        left.sort_by(|a, b| a.id.cmp(&b.id));
        right.sort_by(|a, b| a.id.cmp(&b.id));
        left == right
    }
}

/// One stable, named output branch of a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamDestination {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    /// How many disjoint paths the planner places to this destination. A second
    /// path needs a receiver whose hop profile merges.
    #[serde(default = "one_path", skip_serializing_if = "is_one_path")]
    #[schemars(range(min = 1, max = 2))]
    pub paths: u8,
    #[serde(flatten)]
    pub endpoint: StreamTransport,
}

fn one_path() -> u8 {
    1
}

fn is_one_path(paths: &u8) -> bool {
    *paths == 1
}

impl Deref for StreamDestination {
    type Target = StreamTransport;

    fn deref(&self) -> &Self::Target {
        &self.endpoint
    }
}

fn default_enabled() -> bool {
    true
}

/// Wire tag for a device end: the node's own capture or display device.
pub const DEVICE_TRANSPORT: &str = "device";

/// Transport carrying a stream endpoint. Externally tagged by transport name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum StreamTransport {
    Srt(SrtEndpoint),
    /// The media starts or ends at a node's own capture or display device: a
    /// camera when this is the source, a screen when it is a destination. The
    /// controller chooses the transport that carries it to or from the node.
    Device(NodeEndpoint),
    /// A WHIP sender open-weave does not manage pushes the media to an ingest
    /// on `node`. Sources only.
    Whip(SignallingEndpoint),
    /// A WHEP player open-weave does not manage pulls the media from `node`.
    /// Destinations only.
    Whep(SignallingEndpoint),
}

impl StreamTransport {
    /// The registered node this endpoint is placed on, when it names one.
    #[must_use]
    pub fn node(&self) -> Option<&str> {
        match self {
            Self::Srt(endpoint) => endpoint.node.as_deref(),
            Self::Device(endpoint) => Some(&endpoint.node),
            Self::Whip(endpoint) | Self::Whep(endpoint) => Some(&endpoint.node),
        }
    }

    /// The shared network this endpoint uses, when pinned.
    #[must_use]
    pub fn network(&self) -> Option<&str> {
        match self {
            Self::Srt(endpoint) => endpoint.network.as_deref(),
            Self::Device(endpoint) => endpoint.network.as_deref(),
            Self::Whip(endpoint) | Self::Whep(endpoint) => endpoint.network.as_deref(),
        }
    }

    /// The manifest tag of this variant.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Srt(_) => Transport::Srt.name(),
            Self::Device(_) => DEVICE_TRANSPORT,
            Self::Whip(_) => Transport::Whip.name(),
            Self::Whep(_) => Transport::Whep.name(),
        }
    }

    /// The format this endpoint declares it sends, when it declares one.
    #[must_use]
    pub fn format(&self) -> Option<&MediaFormat> {
        match self {
            Self::Srt(endpoint) => endpoint.format.as_ref(),
            Self::Whip(endpoint) | Self::Whep(endpoint) => endpoint.format.as_ref(),
            Self::Device(_) => None,
        }
    }

    /// What this endpoint declares it accepts, when it declares a constraint.
    #[must_use]
    pub fn accepts(&self) -> Option<&FormatConstraint> {
        match self {
            Self::Srt(endpoint) => endpoint.accepts.as_ref(),
            Self::Whip(endpoint) | Self::Whep(endpoint) => endpoint.accepts.as_ref(),
            Self::Device(_) => None,
        }
    }
}

/// A WHIP or WHEP peer open-weave does not manage, reaching the signalling
/// listener `node` declares. Like an SRT producer or consumer, the peer dials
/// the node; the manifest names the node, never an address.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignallingEndpoint {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node: String,
    /// Shared network the peer reaches the node on. Absent lets the planner
    /// choose an attachment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
    /// What the WHIP sender sends. Sources only; see [`SrtEndpoint::format`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<MediaFormat>,
    /// What the WHEP player accepts. Destinations only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepts: Option<FormatConstraint>,
}

/// An endpoint that is nothing but a node: the node itself produces or consumes
/// the media, so there is no address, latency, or format to declare.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeEndpoint {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node: String,
    /// Shared network constraint. Absent lets the planner choose an attachment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SrtEndpoint {
    /// Registered node id hosting this endpoint. Mutually exclusive with
    /// [`SrtEndpoint::remote`]; exactly one must be set (enforced at validation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node: Option<String>,
    /// External SRT listener this endpoint dials out to. Destinations only;
    /// mutually exclusive with [`SrtEndpoint::node`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RemoteAddr>,
    /// Registered node ids to relay through, upstream-first, before reaching this
    /// destination. Destinations only. Pins transit the planner would otherwise
    /// choose itself; it still inserts a relay of its own when a link needs one
    /// and none is pinned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(inner(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    ))]
    pub via: Vec<String>,
    /// Shared network constraint for a node endpoint. Absent lets the planner
    /// choose an attachment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<u32>,
    /// Encrypts this endpoint's own socket: the ingress a producer dials, the
    /// output a consumer dials, or the remote listener the stream dials. Absent
    /// leaves that socket in the clear. Links between nodes are keyed by the
    /// controller whether or not this is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = Passphrase::MIN_LEN, max = Passphrase::MAX_LEN))]
    pub passphrase: Option<Passphrase>,
    /// What the producer feeding this endpoint sends. Sources only.
    ///
    /// Declared, not discovered: an SRT flow that only moves bytes never parses
    /// its payload, so nothing downstream knows what is inside it. Absent means
    /// the format is unknown and nothing is checked against it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<MediaFormat>,
    /// What this endpoint will accept. Destinations only. Absent accepts
    /// anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepts: Option<FormatConstraint>,
}

/// An external SRT listener a stream dials out to. Placed by no node: the sender
/// simply gains a caller egress to this address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RemoteAddr {
    pub host: String,
    pub port: u16,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub network: String,
}

/// Destinations whose declared `accepts` the stream's source format does not
/// satisfy, in manifest order.
///
/// Empty whenever the source declares no format or no destination declares a
/// constraint — an undeclared format is unknown, not wrong, so nothing is
/// inferred from its absence.
///
/// This only reports. Nothing here places a conversion: a mismatch is a fact
/// about the manifest, and saying so is useful well before anything can fix it.
#[must_use]
pub fn stream_format_conflicts(stream: &StreamDefinition) -> Vec<media::FormatConflict> {
    let Some(format) = stream.source.format() else {
        return Vec::new();
    };

    stream
        .destinations
        .iter()
        .filter_map(|destination| {
            let mismatches = destination.endpoint.accepts()?.mismatches(format);
            (!mismatches.is_empty()).then_some(media::FormatConflict {
                destination: destination.id.clone(),
                mismatches,
            })
        })
        .collect()
}

/// Prefix marking a hop id (and thus its provisioned flow) as owned by open-weave.
/// Adapters only delete flows whose name carries this prefix.
pub const HOP_ID_PREFIX: &str = "weave-";

/// Whether a flow/hop name is owned by open-weave and safe to reconcile or delete.
#[must_use]
pub fn is_managed_hop_id(name: &str) -> bool {
    name.starts_with(HOP_ID_PREFIX)
}

/// An ordered chain of hops (upstream→downstream) realising one stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Path {
    pub stream: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub hops: Vec<DesiredHop>,
}

/// One provisioning unit placed on a single node: one ingress socket fanned out
/// to one or more egress sockets (a sender tees to every destination).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DesiredHop {
    pub id: String,
    pub node_id: String,
    pub profile_id: String,
    pub role: HopRole,
    pub ingress: SocketSpec,
    /// A second ingress carrying another copy of the same media over a disjoint
    /// path, for the hop to merge with `ingress`. Only a receiver whose profile
    /// merges has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_ingress: Option<SocketSpec>,
    pub egresses: Vec<DesiredEgress>,
}

impl DesiredHop {
    /// Every socket on the hop: the ingress, the merge ingress, then each egress.
    pub fn sockets(&self) -> impl Iterator<Item = &SocketSpec> {
        // Destructured so a new field fails to compile here until it is placed.
        let Self {
            id: _,
            node_id: _,
            profile_id: _,
            role: _,
            ingress,
            merge_ingress,
            egresses,
        } = self;
        std::iter::once(ingress)
            .chain(merge_ingress)
            .chain(egresses.iter().map(|egress| &egress.socket))
    }

    /// [`DesiredHop::sockets`], mutably.
    pub fn sockets_mut(&mut self) -> impl Iterator<Item = &mut SocketSpec> {
        let Self {
            id: _,
            node_id: _,
            profile_id: _,
            role: _,
            ingress,
            merge_ingress,
            egresses,
        } = self;
        std::iter::once(ingress)
            .chain(merge_ingress)
            .chain(egresses.iter_mut().map(|egress| &mut egress.socket))
    }
}

/// One identified output branch of a desired hop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DesiredEgress {
    pub branch_id: String,
    #[serde(flatten)]
    pub socket: SocketSpec,
}

impl Deref for DesiredEgress {
    type Target = SocketSpec;

    fn deref(&self) -> &Self::Target {
        &self.socket
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HopRole {
    Sender,
    Bridge,
    Receiver,
}

/// One socket on a hop: where media enters or leaves it, in the terms of the
/// transport carrying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketSpec {
    Srt(SrtSocket),
    /// WebRTC ingest: the `Connect` end pushes media to the `Listen` end's URL.
    Whip(SignallingSocket),
    /// WebRTC playback: the `Connect` end pulls media from the `Listen` end's URL.
    Whep(SignallingSocket),
    /// The node's own device, where the media starts or ends.
    Device(DeviceKind),
}

impl JsonSchema for SocketSpec {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SocketSpec".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SocketSpec").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        generator.subschema_for::<SocketRepr>()
    }
}

impl SocketSpec {
    /// An SRT listener bound to `port`.
    #[must_use]
    pub fn srt_listen(port: u16, latency: u32) -> Self {
        Self::Srt(SrtSocket::Listen {
            port,
            params: SrtParams {
                latency: Some(latency),
                ..SrtParams::default()
            },
        })
    }

    /// An SRT caller dialling `host:port`.
    #[must_use]
    pub fn srt_connect(host: impl Into<String>, port: u16, latency: u32) -> Self {
        Self::Srt(SrtSocket::Connect {
            host: host.into(),
            port,
            params: SrtParams {
                latency: Some(latency),
                ..SrtParams::default()
            },
        })
    }

    /// One end of a `transport`-signalled WebRTC link, addressed at
    /// `{base}/{endpoint_id}` — see [`SignallingSocket::new`].
    #[must_use]
    pub fn signalling(
        transport: SignallingTransport,
        role: SocketRole,
        base: &str,
        endpoint_id: &str,
    ) -> Self {
        let socket = SignallingSocket::new(role, base, endpoint_id);
        match transport {
            SignallingTransport::Whip => Self::Whip(socket),
            SignallingTransport::Whep => Self::Whep(socket),
        }
    }

    /// The wire name of this socket's end: its [`SocketRole`] for a link
    /// transport, its [`DeviceKind`] for a device.
    #[must_use]
    pub fn end_name(&self) -> &'static str {
        match self {
            Self::Srt(socket) => socket.role().name(),
            Self::Whip(socket) | Self::Whep(socket) => socket.role.name(),
            Self::Device(kind) => kind.name(),
        }
    }
}

impl std::fmt::Display for SocketSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Srt(_) => f.write_str(Transport::Srt.name()),
            Self::Whip(_) => f.write_str(Transport::Whip.name()),
            Self::Whep(_) => f.write_str(Transport::Whep.name()),
            Self::Device(kind) => write!(f, "{kind} device"),
        }
    }
}

/// One end of an SRT link: a listener binds a port, a caller dials one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SrtSocket {
    Listen {
        port: u16,
        params: SrtParams,
    },
    Connect {
        host: String,
        port: u16,
        params: SrtParams,
    },
}

impl SrtSocket {
    #[must_use]
    pub fn role(&self) -> SocketRole {
        match self {
            Self::Listen { .. } => SocketRole::Listen,
            Self::Connect { .. } => SocketRole::Connect,
        }
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        match self {
            Self::Listen { port, .. } | Self::Connect { port, .. } => *port,
        }
    }

    #[must_use]
    pub fn params(&self) -> &SrtParams {
        match self {
            Self::Listen { params, .. } | Self::Connect { params, .. } => params,
        }
    }

    pub fn params_mut(&mut self) -> &mut SrtParams {
        match self {
            Self::Listen { params, .. } | Self::Connect { params, .. } => params,
        }
    }
}

/// One end of a WebRTC link, addressed by the signalling URL the `Connect` end
/// calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignallingSocket {
    pub role: SocketRole,
    pub url: String,
    /// Id the URL was built from — carried alongside `url` rather than
    /// re-derived by callers.
    pub endpoint_id: String,
}

impl SignallingSocket {
    /// Builds the signalling URL from `base` (trailing `/` trimmed) and
    /// `endpoint_id`, joined `{base}/{endpoint_id}` — the one place that joins
    /// a signalling URL from its parts.
    #[must_use]
    pub fn new(role: SocketRole, base: &str, endpoint_id: impl Into<String>) -> Self {
        let endpoint_id = endpoint_id.into();
        let url = format!("{}/{endpoint_id}", base.trim_end_matches('/'));
        Self {
            role,
            url,
            endpoint_id,
        }
    }
}

/// Flat wire form of a [`SocketSpec`]: a `transport` and `role` naming the
/// variant, plus the fields that variant carries.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SocketRepr {
    transport: SocketTransport,
    role: SocketEnd,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    endpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    params: Option<SrtParams>,
}

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SocketTransport {
    Srt,
    Whip,
    Whep,
    Device,
}

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SocketEnd {
    Listen,
    Connect,
    Capture,
    Display,
}

impl SocketEnd {
    fn name(self) -> &'static str {
        match self {
            Self::Listen => SocketRole::Listen.name(),
            Self::Connect => SocketRole::Connect.name(),
            Self::Capture => DeviceKind::Capture.name(),
            Self::Display => DeviceKind::Display.name(),
        }
    }

    fn link_role(self) -> Result<SocketRole, String> {
        match self {
            Self::Listen => Ok(SocketRole::Listen),
            Self::Connect => Ok(SocketRole::Connect),
            Self::Capture | Self::Display => Err(format!("{} is not a socket role", self.name())),
        }
    }

    fn device_kind(self) -> Result<DeviceKind, String> {
        match self {
            Self::Capture => Ok(DeviceKind::Capture),
            Self::Display => Ok(DeviceKind::Display),
            Self::Listen | Self::Connect => Err(format!("{} is not a device role", self.name())),
        }
    }
}

impl From<SocketRole> for SocketEnd {
    fn from(role: SocketRole) -> Self {
        match role {
            SocketRole::Listen => Self::Listen,
            SocketRole::Connect => Self::Connect,
        }
    }
}

impl From<DeviceKind> for SocketEnd {
    fn from(kind: DeviceKind) -> Self {
        match kind {
            DeviceKind::Capture => Self::Capture,
            DeviceKind::Display => Self::Display,
        }
    }
}

/// Reject a socket that carries a field its transport has no use for, naming
/// the first such field.
fn reject_extra_fields<const N: usize>(
    socket: &str,
    fields: [(&str, bool); N],
) -> Result<(), String> {
    match fields.into_iter().find(|(_, present)| *present) {
        Some((field, _)) => Err(format!("a {socket} socket carries no {field}")),
        None => Ok(()),
    }
}

impl From<&SocketSpec> for SocketRepr {
    fn from(spec: &SocketSpec) -> Self {
        let bare = |transport, role| Self {
            transport,
            role,
            host: None,
            port: None,
            url: None,
            endpoint_id: None,
            params: None,
        };
        match spec {
            SocketSpec::Srt(SrtSocket::Listen { port, params }) => Self {
                port: Some(*port),
                params: Some(params.clone()),
                ..bare(SocketTransport::Srt, SocketEnd::Listen)
            },
            SocketSpec::Srt(SrtSocket::Connect { host, port, params }) => Self {
                host: Some(host.clone()),
                port: Some(*port),
                params: Some(params.clone()),
                ..bare(SocketTransport::Srt, SocketEnd::Connect)
            },
            SocketSpec::Whip(socket) => Self {
                url: Some(socket.url.clone()),
                endpoint_id: Some(socket.endpoint_id.clone()),
                ..bare(SocketTransport::Whip, socket.role.into())
            },
            SocketSpec::Whep(socket) => Self {
                url: Some(socket.url.clone()),
                endpoint_id: Some(socket.endpoint_id.clone()),
                ..bare(SocketTransport::Whep, socket.role.into())
            },
            SocketSpec::Device(kind) => bare(SocketTransport::Device, (*kind).into()),
        }
    }
}

impl TryFrom<SocketRepr> for SocketSpec {
    type Error = String;

    fn try_from(repr: SocketRepr) -> Result<Self, Self::Error> {
        let SocketRepr {
            transport,
            role,
            host,
            port,
            url,
            endpoint_id,
            params,
        } = repr;

        match transport {
            SocketTransport::Srt => {
                reject_extra_fields(
                    "srt",
                    [
                        ("url", url.is_some()),
                        ("endpoint_id", endpoint_id.is_some()),
                    ],
                )?;
                let port = port.ok_or_else(|| "an srt socket needs a port".to_string())?;
                let params = params.unwrap_or_default();
                match role.link_role()? {
                    SocketRole::Listen => {
                        reject_extra_fields("listening srt", [("host", host.is_some())])?;
                        Ok(Self::Srt(SrtSocket::Listen { port, params }))
                    }
                    SocketRole::Connect => {
                        let host =
                            host.ok_or_else(|| "a calling srt socket needs a host".to_string())?;
                        Ok(Self::Srt(SrtSocket::Connect { host, port, params }))
                    }
                }
            }
            SocketTransport::Whip | SocketTransport::Whep => {
                let whip = matches!(transport, SocketTransport::Whip);
                let name = if whip {
                    Transport::Whip
                } else {
                    Transport::Whep
                }
                .name();
                reject_extra_fields(
                    name,
                    [
                        ("host", host.is_some()),
                        ("port", port.is_some()),
                        ("params", params.is_some()),
                    ],
                )?;
                let socket = SignallingSocket {
                    role: role.link_role()?,
                    url: url.ok_or_else(|| format!("a {name} socket needs a url"))?,
                    endpoint_id: endpoint_id
                        .ok_or_else(|| format!("a {name} socket needs an endpoint_id"))?,
                };
                Ok(if whip {
                    Self::Whip(socket)
                } else {
                    Self::Whep(socket)
                })
            }
            SocketTransport::Device => {
                reject_extra_fields(
                    DEVICE_TRANSPORT,
                    [
                        ("host", host.is_some()),
                        ("port", port.is_some()),
                        ("url", url.is_some()),
                        ("endpoint_id", endpoint_id.is_some()),
                        ("params", params.is_some()),
                    ],
                )?;
                Ok(Self::Device(role.device_kind()?))
            }
        }
    }
}

impl Serialize for SocketSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SocketRepr::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SocketSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::try_from(SocketRepr::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Which end of a link a socket is: `Listen` hosts and `Connect` dials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SocketRole {
    Listen,
    Connect,
}

impl SocketRole {
    /// The wire name, as serialized.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Listen => "listen",
            Self::Connect => "connect",
        }
    }
}

/// A transport that carries media between two nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Srt,
    /// WebRTC ingest. The `Connect` end pushes media to the `Listen` end's
    /// signalling URL.
    Whip,
    /// WebRTC playback. The `Connect` end pulls media from the `Listen` end's
    /// signalling URL.
    Whep,
}

impl Transport {
    /// The wire name, as serialized.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Srt => "srt",
            Self::Whip => "whip",
            Self::Whep => "whep",
        }
    }

    /// This transport's signalling counterpart, when it is signalled at all.
    #[must_use]
    pub fn signalling(self) -> Option<SignallingTransport> {
        match self {
            Self::Srt => None,
            Self::Whip => Some(SignallingTransport::Whip),
            Self::Whep => Some(SignallingTransport::Whep),
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A [`Transport`] that is signalled: [`Signalling`] carries a base URL per
/// variant, and [`SocketSpec::signalling`] builds a socket for one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignallingTransport {
    Whip,
    Whep,
}

impl SignallingTransport {
    /// The [`Transport`] this signalling applies to.
    #[must_use]
    pub fn transport(self) -> Transport {
        match self {
            Self::Whip => Transport::Whip,
            Self::Whep => Transport::Whep,
        }
    }
}

/// The node's own capture or display device, where media starts or ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeviceKind {
    Capture,
    Display,
}

impl DeviceKind {
    /// The wire name, as serialized.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Capture => "capture",
            Self::Display => "display",
        }
    }
}

impl std::fmt::Display for DeviceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub struct SrtParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<u32>,
    /// Encrypts the socket. Both ends of a link carry the same passphrase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = Passphrase::MIN_LEN, max = Passphrase::MAX_LEN))]
    pub passphrase: Option<Passphrase>,
    /// AES key length in bytes, 16, 24 or 32, used with `passphrase`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("enum" = [16, 24, 32]))]
    pub pbkeylen: Option<u8>,
}

/// An SRT passphrase. [`std::fmt::Debug`] redacts it, no [`std::fmt::Display`]
/// is implemented, and [`Passphrase::expose`] is the only accessor.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct Passphrase(String);

impl Passphrase {
    /// Fewest bytes libsrt accepts in a passphrase.
    pub const MIN_LEN: usize = 10;
    /// Most bytes libsrt accepts in a passphrase.
    pub const MAX_LEN: usize = 80;

    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Passphrase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Passphrase(<redacted>)")
    }
}

/// Node-reported realisation status for one desired hop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HopStatus {
    pub id: String,
    pub node_id: String,
    pub state: HopState,
    pub ingress: SocketStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_ingress: Option<SocketStatus>,
    #[serde(default)]
    pub egresses: Vec<EgressStatus>,
}

/// Observed condition, address, and statistics for one socket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SocketStatus {
    #[serde(default)]
    pub condition: LinkCondition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedAddr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<LinkStats>,
}

/// Observed status for one identified egress branch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EgressStatus {
    pub branch_id: String,
    #[serde(flatten)]
    pub status: SocketStatus,
}

impl HopStatus {
    /// The lifecycle and socket conditions consumed by [`roll_up_path`].
    ///
    /// A status describes `desired` only when it reports each desired branch
    /// exactly once and no others, and a merge ingress exactly when one is
    /// desired.
    #[must_use]
    pub fn conditions(&self, desired: &DesiredHop) -> Option<HopConditions> {
        let merge_ingress = match (&desired.merge_ingress, &self.merge_ingress) {
            (Some(_), Some(status)) => Some(status.condition),
            (None, None) => None,
            _ => return None,
        };
        let mut observed = BTreeMap::new();
        for egress in &self.egresses {
            if observed
                .insert(&egress.branch_id, egress.status.condition)
                .is_some()
            {
                return None;
            }
        }

        let egresses = desired
            .egresses
            .iter()
            .map(|egress| observed.remove(&egress.branch_id))
            .collect::<Option<Vec<_>>>()?;
        if !observed.is_empty() {
            return None;
        }

        Some(HopConditions {
            state: self.state,
            ingress: self.ingress.condition,
            merge_ingress,
            egresses,
        })
    }
}

/// Control-plane lifecycle of a hop's provisioning. Runtime link health is
/// reported separately per socket via [`LinkCondition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HopState {
    Pending,
    Provisioned,
    Failed,
}

/// Observed condition of one socket on a hop, independent of control-plane lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LinkCondition {
    /// No connection; the socket is a listener patiently waiting. Healthy.
    #[default]
    Idle,
    /// No connection; the socket is a caller still retrying. Ambiguous, not degraded.
    Connecting,
    /// Connection up but rate ~0 — fine on our end, nothing coming through yet.
    Connected,
    /// Connection up and media flowing.
    Flowing,
    /// Ingress once carried media but byte progress has frozen while the flow still
    /// claims to run — detected across polls, not from any instantaneous field.
    Stalled,
}

/// Lifecycle plus every socket condition of one hop — the unit rolled up per path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopConditions {
    pub state: HopState,
    pub ingress: LinkCondition,
    pub merge_ingress: Option<LinkCondition>,
    pub egresses: Vec<LinkCondition>,
}

impl HopConditions {
    fn ingresses(&self) -> impl Iterator<Item = LinkCondition> + '_ {
        std::iter::once(self.ingress).chain(self.merge_ingress)
    }
}

/// End-to-end status of a path, derived from its hops' conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PathStatus {
    /// The path is disabled.
    Idle,
    /// At least one hop failed to provision.
    Failed,
    /// A hop is still provisioning or has not reported yet.
    Pending,
    /// Provisioned end to end, but no media is entering at the source.
    AwaitingInput,
    /// Media is entering at the source but not flowing all the way through.
    Degraded,
    /// Media is flowing across every hop.
    Flowing,
}

/// Roll a path's ordered (source→destination) hop conditions into one status.
///
/// `None` entries mark desired hops that have not reported yet. Precedence, first
/// match wins: disabled → `Idle`; any hop failed → `Failed`; any hop pending or
/// missing → `Pending`; any hop stalled → `Degraded`; source not yet receiving
/// media → `AwaitingInput`; media entering but not flowing end to end → `Degraded`;
/// all flowing → `Flowing`.
#[must_use]
pub fn roll_up_path(enabled: bool, hops: &[Option<HopConditions>]) -> PathStatus {
    if !enabled {
        return PathStatus::Idle;
    }
    if hops
        .iter()
        .any(|hop| hop.as_ref().is_some_and(|h| h.state == HopState::Failed))
    {
        return PathStatus::Failed;
    }
    if hops
        .iter()
        .any(|hop| hop.as_ref().is_none_or(|h| h.state == HopState::Pending))
    {
        return PathStatus::Pending;
    }
    if hops.iter().flatten().any(|h| {
        h.ingresses()
            .any(|condition| condition == LinkCondition::Stalled)
            || h.egresses.contains(&LinkCondition::Stalled)
    }) {
        return PathStatus::Degraded;
    }

    let source_flowing = hops.first().and_then(|hop| hop.as_ref().map(|h| h.ingress))
        == Some(LinkCondition::Flowing);
    if !source_flowing {
        return PathStatus::AwaitingInput;
    }

    let all_flowing = hops.iter().flatten().all(|h| {
        h.ingresses()
            .all(|condition| condition == LinkCondition::Flowing)
            && h.egresses
                .iter()
                .all(|condition| *condition == LinkCondition::Flowing)
    });
    if all_flowing {
        PathStatus::Flowing
    } else {
        PathStatus::Degraded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedAddr {
    pub host: String,
    pub port: u16,
}

/// Socket-level SRT stats mirrored from Strom's `srt-stats` payload.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct LinkStats {
    #[serde(default)]
    pub connections: usize,
    #[serde(default)]
    pub rate_mbps: f64,
    #[serde(default)]
    pub packets_sent_lost: i64,
    #[serde(default)]
    pub packets_retransmitted: i64,
    #[serde(default)]
    pub packets_received_lost: i64,
    #[serde(default)]
    pub packets_received_retransmitted: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NodeDescriptor {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub endpoint: String,
    pub status: NodeStatus,
    #[serde(default)]
    pub capabilities: NodeCapabilities,
    pub topology: NodeTopology,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct NodeCapabilities {
    #[serde(default)]
    pub adapters: Vec<AdapterDescriptor>,
    #[serde(default)]
    pub hop_profiles: Vec<HopProfile>,
}

impl NodeCapabilities {
    #[must_use]
    pub fn offers_ingress(&self, transport: Transport, role: SocketRole) -> bool {
        self.hop_profiles
            .iter()
            .any(|profile| profile.ingress.offers_transport(transport, role))
    }

    #[must_use]
    pub fn offers_egress(&self, transport: Transport, role: SocketRole) -> bool {
        self.hop_profiles
            .iter()
            .any(|profile| profile.egress.offers_transport(transport, role))
    }

    #[must_use]
    pub fn offers_ingress_device(&self, kind: DeviceKind) -> bool {
        self.hop_profiles.iter().any(
            |profile| matches!(&profile.ingress, HopEndpointClass::Device(device) if device.device == kind),
        )
    }

    #[must_use]
    pub fn offers_egress_device(&self, kind: DeviceKind) -> bool {
        self.hop_profiles.iter().any(
            |profile| matches!(&profile.egress, HopEndpointClass::Device(device) if device.device == kind),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HopProfile {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub ingress: HopEndpointClass,
    pub egress: HopEndpointClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_egresses: Option<usize>,
    /// The hop can take a second ingress of the `ingress` class carrying another
    /// copy of the same media, and merge the two.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub merge: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum HopEndpointClass {
    Transport(TransportClass),
    Device(DeviceClass),
}

impl HopEndpointClass {
    #[must_use]
    pub fn offers_transport(&self, transport: Transport, role: SocketRole) -> bool {
        matches!(self, Self::Transport(class) if class.transport == transport && class.roles.contains(role))
    }

    #[must_use]
    pub fn matches_socket(&self, socket: &SocketSpec) -> bool {
        match (self, socket) {
            (Self::Transport(class), SocketSpec::Srt(socket)) => {
                class.transport == Transport::Srt && class.roles.contains(socket.role())
            }
            (Self::Transport(class), SocketSpec::Whip(socket)) => {
                class.transport == Transport::Whip && class.roles.contains(socket.role)
            }
            (Self::Transport(class), SocketSpec::Whep(socket)) => {
                class.transport == Transport::Whep && class.roles.contains(socket.role)
            }
            (Self::Device(class), SocketSpec::Device(device)) => class.device == *device,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransportClass {
    pub transport: Transport,
    pub roles: RoleSet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeviceClass {
    pub device: DeviceKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct NodeTopology {
    #[serde(default)]
    pub attachments: Vec<NetworkAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NetworkAttachment {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub network: String,
    #[serde(default)]
    pub dial: bool,
    #[serde(default)]
    pub listeners: NetworkListeners,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NetworkListeners {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub srt: Option<SrtListener>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whip: Option<SignallingListener>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whep: Option<SignallingListener>,
}

impl NetworkListeners {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.srt.is_none() && self.whip.is_none() && self.whep.is_none()
    }

    #[must_use]
    pub fn signalling(&self, transport: SignallingTransport) -> Option<&str> {
        match transport {
            SignallingTransport::Whip => self.whip.as_ref().map(|v| v.base_url.as_str()),
            SignallingTransport::Whep => self.whep.as_ref().map(|v| v.base_url.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SrtListener {
    pub host: String,
    pub port_range: PortRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignallingListener {
    pub base_url: String,
}

/// Inclusive `[start, end]` range of ports a node offers for controller-side
/// assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

impl PortRange {
    /// Number of ports beyond `start` the range spans (saturating).
    #[must_use]
    pub fn span(self) -> u16 {
        self.end.saturating_sub(self.start)
    }
}

/// Static configuration a node loads at startup and self-registers from.
/// Shared across adapters; adapter-specific sections wrap this type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub southbound_url: String,
    /// This node's own southbound token (see [`auth`]). Falls back to
    /// [`auth::SOUTHBOUND_TOKEN_VAR`] when unset here, so a deployment can keep
    /// the secret out of the config file — see
    /// [`NodeConfig::resolve_southbound_token`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub southbound_token: Option<String>,
    pub listen: String,
    /// Endpoint peers use to reach this node's control API. Defaults to
    /// `http://{listen}` via [`NodeConfig::public_endpoint`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_endpoint: Option<String>,
    pub topology: NodeTopology,
}

impl NodeConfig {
    /// The control endpoint peers use to reach this node, defaulting to
    /// `http://{listen}` when unset.
    #[must_use]
    pub fn public_endpoint(&self) -> String {
        self.public_endpoint
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.listen))
    }

    /// Resolve the bearer token this node presents to southbound: the config
    /// value when set, otherwise [`auth::SOUTHBOUND_TOKEN_VAR`] from the
    /// environment. `None` means authentication is switched off.
    ///
    /// # Errors
    /// Returns [`auth::AuthError::MissingToken`] when neither source carries a
    /// token and [`auth::AUTH_DISABLED_VAR`] is not engaged.
    pub fn resolve_southbound_token(&self) -> Result<Option<auth::Token>, auth::AuthError> {
        if auth::auth_disabled() {
            return Ok(None);
        }
        self.southbound_token
            .as_deref()
            .and_then(auth::Token::new)
            .or_else(|| auth::Token::from_env(auth::SOUTHBOUND_TOKEN_VAR))
            .map(Some)
            .ok_or_else(|| auth::AuthError::MissingToken {
                var: auth::SOUTHBOUND_TOKEN_VAR.to_string(),
            })
    }

    /// # Errors
    /// Returns [`ConfigError`] describing the first violated invariant.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_resource_id(&self.id).map_err(ConfigError::InvalidId)?;
        let mut ids = std::collections::HashSet::new();
        for attachment in &self.topology.attachments {
            validate_resource_id(&attachment.id).map_err(ConfigError::InvalidAttachmentId)?;
            validate_resource_id(&attachment.network).map_err(ConfigError::InvalidNetworkId)?;
            if !ids.insert(&attachment.id) {
                return Err(ConfigError::DuplicateAttachment(attachment.id.clone()));
            }
            if let Some(listener) = &attachment.listeners.srt {
                if listener.host.trim().is_empty() {
                    return Err(ConfigError::BlankListener);
                }
                if listener.port_range.start > listener.port_range.end {
                    return Err(ConfigError::InvalidPortRange {
                        start: listener.port_range.start,
                        end: listener.port_range.end,
                    });
                }
            }
            if attachment
                .listeners
                .whip
                .as_ref()
                .into_iter()
                .chain(attachment.listeners.whep.as_ref())
                .any(|listener| listener.base_url.trim().is_empty())
            {
                return Err(ConfigError::BlankListener);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("node id {0}")]
    InvalidId(ResourceIdError),
    #[error("attachment id {0}")]
    InvalidAttachmentId(ResourceIdError),
    #[error("network id {0}")]
    InvalidNetworkId(ResourceIdError),
    #[error("duplicate attachment id {0}")]
    DuplicateAttachment(String),
    #[error("listener host or base_url must not be blank")]
    BlankListener,
    #[error("port_range start {start} exceeds end {end}")]
    InvalidPortRange { start: u16, end: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AdapterDescriptor {
    pub name: String,
    pub kind: AdapterKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdapterKind {
    Strom,
    Nmos,
    MxlDomain,
    MxlK8s,
    Mcm,
    Vendor,
    Custom,
}

/// A non-empty set of the socket roles a node can take over one transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleSet {
    listen: bool,
    connect: bool,
}

impl JsonSchema for RoleSet {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "RoleSet".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RoleSet").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let mut schema = generator.subschema_for::<Vec<SocketRole>>();
        schema.insert("minItems".to_string(), 1.into());
        schema
    }
}

impl RoleSet {
    #[must_use]
    pub fn both() -> Self {
        Self {
            listen: true,
            connect: true,
        }
    }

    #[must_use]
    pub fn only(role: SocketRole) -> Self {
        Self {
            listen: matches!(role, SocketRole::Listen),
            connect: matches!(role, SocketRole::Connect),
        }
    }

    #[must_use]
    pub fn contains(self, role: SocketRole) -> bool {
        match role {
            SocketRole::Listen => self.listen,
            SocketRole::Connect => self.connect,
        }
    }

    fn roles(self) -> impl Iterator<Item = SocketRole> {
        [
            self.listen.then_some(SocketRole::Listen),
            self.connect.then_some(SocketRole::Connect),
        ]
        .into_iter()
        .flatten()
    }
}

impl Serialize for RoleSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.roles())
    }
}

impl<'de> Deserialize<'de> for RoleSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut set = Self {
            listen: false,
            connect: false,
        };
        for role in Vec::<SocketRole>::deserialize(deserializer)? {
            match role {
                SocketRole::Listen => set.listen = true,
                SocketRole::Connect => set.connect = true,
            }
        }
        if !set.listen && !set.connect {
            return Err(serde::de::Error::custom("a transport offered in no role"));
        }
        Ok(set)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EndpointDescriptor {
    pub id: String,
    pub label: String,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node_id: Option<String>,
    pub kind: EndpointKind,
    /// Transport labels an adapter recognised on this endpoint, such as
    /// `webrtc` or `ndi`. Guesses read off the underlying system, not offers.
    #[serde(default)]
    pub transports: Vec<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Source,
    Destination,
    Bidirectional,
    Flow,
    Gateway,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NodeRegistration {
    /// Southbound protocol version the registering adapter speaks. Absent means
    /// an adapter predating the handshake, which reads as `0` and is rejected —
    /// see [`protocol_compatible`].
    #[serde(default)]
    pub protocol_version: u32,
    pub node: NodeDescriptor,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hop_status: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NodeHeartbeat {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node_id: String,
    pub status: NodeStatus,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hop_status: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ObservedState {
    #[serde(default)]
    pub nodes: Vec<NodeDescriptor>,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hops: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconcileReport {
    pub status: ReconcileStatus,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamEndpoints {
    pub ingress: Option<EndpointAddr>,
    pub destinations: Vec<DestinationEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DestinationEndpoint {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub endpoint: Option<EndpointAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EndpointAddr {
    pub node: String,
    /// The SRT listener's host. Absent for a WHIP or WHEP endpoint, whose `url`
    /// carries it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// What a peer dials: `srt://host:port`, or the WHIP or WHEP URL.
    pub url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileStatus {
    Idle,
    Converging,
    Converged,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Unknown,
    Ready,
    Degraded,
    Offline,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_json_uses_snake_case_transport_tag_and_defaults_enabled() {
        let json = serde_json::json!({
            "name": "cam1-to-studio",
            "source": { "srt": { "node": "strom-node-1", "latency": 200 } },
            "destinations": [
                { "id": "studio", "srt": { "node": "strom-node-2", "network": "wan" } }
            ]
        });

        let stream: StreamDefinition = serde_json::from_value(json).expect("parse stream");

        assert!(stream.enabled, "enabled defaults to true when omitted");
        assert_eq!(
            stream.source,
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: Some(200),
                passphrase: None,
            })
        );
        assert_eq!(stream.destinations[0].id, "studio");
        assert_eq!(
            stream.destinations[0].endpoint,
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: Some("wan".to_string()),
                latency: None,
                passphrase: None,
            })
        );

        let round_trip: StreamDefinition =
            serde_json::from_str(&serde_json::to_string(&stream).unwrap()).unwrap();
        assert_eq!(stream, round_trip);
        assert_eq!(
            serde_json::to_value(&stream).unwrap()["source"]
                .as_object()
                .unwrap()
                .keys()
                .next()
                .unwrap(),
            "srt"
        );
    }

    fn sample_hop() -> DesiredHop {
        DesiredHop {
            id: "weave-contribution-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            profile_id: "srt-forward".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(7001, 200),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "studio".to_string(),
                socket: SocketSpec::srt_connect("172.31.0.10", 7002, 1000),
            }],
        }
    }

    #[test]
    fn path_round_trips_and_defaults_enabled_and_hops() {
        let path = Path {
            stream: "contribution".to_string(),
            enabled: true,
            hops: vec![sample_hop()],
        };
        let round_trip: Path =
            serde_json::from_str(&serde_json::to_string(&path).unwrap()).unwrap();
        assert_eq!(path, round_trip);

        let minimal: Path = serde_json::from_value(serde_json::json!({
            "stream": "contribution"
        }))
        .expect("parse minimal path");
        assert!(minimal.enabled, "enabled defaults to true");
        assert!(minimal.hops.is_empty(), "hops defaults to empty");
    }

    #[test]
    fn hop_with_multiple_egresses_round_trips() {
        let mut hop = sample_hop();
        hop.egresses.push(DesiredEgress {
            branch_id: "preview".to_string(),
            socket: SocketSpec::srt_connect("172.31.0.10", 7003, 1000),
        });
        assert_eq!(hop.egresses.len(), 2);

        let value = serde_json::to_value(&hop).unwrap();
        assert_eq!(value["profile_id"], "srt-forward");
        assert_eq!(value["egresses"][1]["branch_id"], "preview");
        assert_eq!(value["egresses"][1]["transport"], "srt");
        let round_trip: DesiredHop = serde_json::from_value(value).unwrap();
        assert_eq!(hop, round_trip);
    }

    #[test]
    fn a_listening_srt_socket_carries_no_host() {
        let value = serde_json::to_value(SocketSpec::srt_listen(7001, 200)).unwrap();
        assert!(value.get("host").is_none(), "a listener has no host");
        assert_eq!(value["role"], "listen");
        assert_eq!(value["transport"], "srt");
    }

    #[test]
    fn hop_status_round_trips_with_optional_fields_absent() {
        let status = HopStatus {
            id: "weave-contribution-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            state: HopState::Provisioned,
            ingress: SocketStatus {
                condition: LinkCondition::Flowing,
                resolved: Some(ResolvedAddr {
                    host: "0.0.0.0".to_string(),
                    port: 7001,
                }),
                stats: Some(LinkStats {
                    connections: 1,
                    rate_mbps: 4.5,
                    ..LinkStats::default()
                }),
            },
            merge_ingress: None,
            egresses: vec![EgressStatus {
                branch_id: "studio".to_string(),
                status: SocketStatus {
                    condition: LinkCondition::Connected,
                    resolved: None,
                    stats: None,
                },
            }],
        };
        let round_trip: HopStatus =
            serde_json::from_str(&serde_json::to_string(&status).unwrap()).unwrap();
        assert_eq!(status, round_trip);
    }

    #[test]
    fn heartbeat_parses_without_hop_status_field() {
        let heartbeat: NodeHeartbeat = serde_json::from_value(serde_json::json!({
            "node_id": "strom-node-1",
            "status": "ready"
        }))
        .expect("parse legacy heartbeat");
        assert!(heartbeat.hop_status.is_empty());
    }

    #[test]
    fn registration_without_protocol_version_reads_as_incompatible_zero() {
        let registration: NodeRegistration = serde_json::from_value(serde_json::json!({
            "node": {
                "id": "strom-node-1",
                "endpoint": "http://strom-node-1:8091",
                "status": "ready",
                "topology": {}
            }
        }))
        .expect("parse pre-handshake registration");

        assert_eq!(registration.protocol_version, 0);
        assert!(
            !protocol_compatible(registration.protocol_version),
            "an adapter that declares no version is not compatible"
        );
    }

    #[test]
    fn protocol_compatible_accepts_only_the_supported_version() {
        assert!(protocol_compatible(PROTOCOL_VERSION));
        assert!(!protocol_compatible(PROTOCOL_VERSION + 1));
        assert!(!protocol_compatible(0));
    }

    #[test]
    fn is_managed_hop_id_matches_only_prefixed_names() {
        assert!(is_managed_hop_id("weave-contribution-sender"));
        assert!(!is_managed_hop_id("contribution"));
        assert!(!is_managed_hop_id("contribution-recv"));
    }

    fn hc(state: HopState, ingress: LinkCondition, egress: LinkCondition) -> Option<HopConditions> {
        Some(HopConditions {
            state,
            ingress,
            merge_ingress: None,
            egresses: vec![egress],
        })
    }

    #[test]
    fn rollup_disabled_is_idle() {
        let hops = [hc(
            HopState::Provisioned,
            LinkCondition::Flowing,
            LinkCondition::Flowing,
        )];
        assert_eq!(roll_up_path(false, &hops), PathStatus::Idle);
    }

    #[test]
    fn rollup_failed_hop_wins_over_flowing() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Failed,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Failed);
    }

    #[test]
    fn rollup_pending_or_missing_hop_is_pending() {
        let pending = [hc(
            HopState::Pending,
            LinkCondition::Idle,
            LinkCondition::Idle,
        )];
        assert_eq!(roll_up_path(true, &pending), PathStatus::Pending);

        let missing = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            None,
        ];
        assert_eq!(roll_up_path(true, &missing), PathStatus::Pending);
    }

    #[test]
    fn rollup_source_not_receiving_is_awaiting_input() {
        let idle = [hc(
            HopState::Provisioned,
            LinkCondition::Idle,
            LinkCondition::Idle,
        )];
        assert_eq!(roll_up_path(true, &idle), PathStatus::AwaitingInput);

        let silent = [hc(
            HopState::Provisioned,
            LinkCondition::Connected,
            LinkCondition::Connected,
        )];
        assert_eq!(roll_up_path(true, &silent), PathStatus::AwaitingInput);
    }

    #[test]
    fn rollup_flowing_source_with_stalled_downstream_is_degraded() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Provisioned,
                LinkCondition::Connected,
                LinkCondition::Connected,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Degraded);
    }

    #[test]
    fn rollup_stalled_source_is_degraded_not_awaiting_input() {
        let stalled_source = [hc(
            HopState::Provisioned,
            LinkCondition::Stalled,
            LinkCondition::Idle,
        )];
        assert_eq!(roll_up_path(true, &stalled_source), PathStatus::Degraded);
    }

    #[test]
    fn rollup_stalled_downstream_hop_is_degraded() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Provisioned,
                LinkCondition::Stalled,
                LinkCondition::Idle,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Degraded);
    }

    #[test]
    fn rollup_all_flowing_is_flowing() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Flowing);
    }

    #[test]
    fn rollup_checks_every_fanout_branch() {
        let hops = [Some(HopConditions {
            state: HopState::Provisioned,
            ingress: LinkCondition::Flowing,
            merge_ingress: None,
            egresses: vec![LinkCondition::Flowing, LinkCondition::Connecting],
        })];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Degraded);
    }

    #[test]
    fn hop_status_conditions_match_desired_branches_by_id() {
        let mut desired = sample_hop();
        desired.egresses.push(DesiredEgress {
            branch_id: "preview".to_string(),
            socket: SocketSpec::srt_connect("172.31.0.11", 7003, 1000),
        });
        let status = HopStatus {
            id: "weave-a".to_string(),
            node_id: "n1".to_string(),
            state: HopState::Provisioned,
            ingress: SocketStatus {
                condition: LinkCondition::Flowing,
                resolved: None,
                stats: None,
            },
            merge_ingress: None,
            egresses: vec![
                EgressStatus {
                    branch_id: "preview".to_string(),
                    status: SocketStatus {
                        condition: LinkCondition::Connected,
                        resolved: None,
                        stats: None,
                    },
                },
                EgressStatus {
                    branch_id: "studio".to_string(),
                    status: SocketStatus {
                        condition: LinkCondition::Flowing,
                        resolved: None,
                        stats: None,
                    },
                },
            ],
        };
        assert_eq!(
            status.conditions(&desired),
            Some(HopConditions {
                state: HopState::Provisioned,
                ingress: LinkCondition::Flowing,
                merge_ingress: None,
                egresses: vec![LinkCondition::Flowing, LinkCondition::Connected],
            })
        );
    }

    #[test]
    fn hop_status_conditions_reject_missing_duplicate_and_extra_branches() {
        let desired = sample_hop();
        let socket = SocketStatus {
            condition: LinkCondition::Flowing,
            resolved: None,
            stats: None,
        };
        let mut status = HopStatus {
            id: desired.id.clone(),
            node_id: desired.node_id.clone(),
            state: HopState::Provisioned,
            ingress: socket.clone(),
            merge_ingress: None,
            egresses: Vec::new(),
        };
        assert_eq!(status.conditions(&desired), None);

        status.egresses = vec![
            EgressStatus {
                branch_id: "studio".to_string(),
                status: socket.clone(),
            },
            EgressStatus {
                branch_id: "studio".to_string(),
                status: socket.clone(),
            },
        ];
        assert_eq!(status.conditions(&desired), None);

        status.egresses = vec![EgressStatus {
            branch_id: "preview".to_string(),
            status: socket,
        }];
        assert_eq!(status.conditions(&desired), None);
    }

    #[test]
    fn srt_endpoint_serde_allows_node_or_remote_and_denies_unknown_fields() {
        // node is now optional; the node-XOR-remote rule is enforced at
        // validation, not by serde, so a bare endpoint still parses.
        let bare: SrtEndpoint =
            serde_json::from_value(serde_json::json!({ "network": "wan" })).expect("parse bare");
        assert_eq!(bare.node, None);
        assert_eq!(bare.remote, None);

        let via: SrtEndpoint = serde_json::from_value(serde_json::json!({
            "node": "strom-node-2",
            "via": ["edge-relay"]
        }))
        .expect("parse via");
        assert_eq!(via.via, vec!["edge-relay".to_string()]);
        assert!(
            serde_json::to_value(&bare).unwrap().get("via").is_none(),
            "an empty via is not serialized"
        );

        let remote: SrtEndpoint = serde_json::from_value(serde_json::json!({
            "remote": { "host": "198.51.100.5", "port": 9000, "network": "internet" }
        }))
        .expect("parse remote");
        assert_eq!(
            remote.remote,
            Some(RemoteAddr {
                host: "198.51.100.5".to_string(),
                port: 9000,
                network: "internet".to_string(),
            })
        );
        let round_trip: SrtEndpoint =
            serde_json::from_str(&serde_json::to_string(&remote).unwrap()).unwrap();
        assert_eq!(remote, round_trip);

        let bogus: Result<SrtEndpoint, _> =
            serde_json::from_value(serde_json::json!({ "node": "n", "bogus": true }));
        assert!(bogus.is_err(), "deny_unknown_fields rejects typos");
    }

    fn node_config() -> NodeConfig {
        NodeConfig {
            id: "strom-node-1".to_string(),
            southbound_url: "http://127.0.0.1:8081".to_string(),
            southbound_token: None,
            listen: "0.0.0.0:8091".to_string(),
            public_endpoint: None,
            topology: NodeTopology {
                attachments: vec![NetworkAttachment {
                    id: "lan".to_string(),
                    network: "studio-lan".to_string(),
                    dial: true,
                    listeners: NetworkListeners {
                        srt: Some(SrtListener {
                            host: "172.26.0.10".to_string(),
                            port_range: PortRange {
                                start: 20000,
                                end: 20999,
                            },
                        }),
                        whip: None,
                        whep: None,
                    },
                }],
            },
        }
    }

    #[test]
    fn node_config_public_endpoint_defaults_to_listen() {
        let config = node_config();
        assert_eq!(config.public_endpoint(), "http://0.0.0.0:8091");

        let mut pinned = node_config();
        pinned.public_endpoint = Some("http://172.25.0.21:8091".to_string());
        assert_eq!(pinned.public_endpoint(), "http://172.25.0.21:8091");
    }

    #[test]
    fn node_config_validate_accepts_valid_config() {
        assert_eq!(node_config().validate(), Ok(()));
    }

    #[test]
    fn node_config_validate_rejects_invariant_violations() {
        let mut empty_id = node_config();
        empty_id.id = "  ".to_string();
        assert_eq!(
            empty_id.validate(),
            Err(ConfigError::InvalidId(ResourceIdError::InvalidCharacters))
        );

        let mut long_id = node_config();
        long_id.id = "a".repeat(RESOURCE_ID_MAX_LEN + 1);
        assert_eq!(
            long_id.validate(),
            Err(ConfigError::InvalidId(ResourceIdError::InvalidLength))
        );

        let mut blank_host = node_config();
        blank_host.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .unwrap()
            .host = String::new();
        assert_eq!(blank_host.validate(), Err(ConfigError::BlankListener));

        let mut bad_range = node_config();
        bad_range.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .unwrap()
            .port_range = PortRange {
            start: 21000,
            end: 20000,
        };
        assert_eq!(
            bad_range.validate(),
            Err(ConfigError::InvalidPortRange {
                start: 21000,
                end: 20000,
            })
        );
    }

    fn stream_with_formats(
        format: Option<MediaFormat>,
        accepts: Option<FormatConstraint>,
    ) -> StreamDefinition {
        let mut source = SrtEndpoint {
            node: Some("strom-node-1".to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        };
        let mut destination = source.clone();
        source.format = format;
        destination.node = Some("strom-node-2".to_string());
        destination.accepts = accepts;

        StreamDefinition {
            name: "formats".to_string(),
            enabled: true,
            source: StreamTransport::Srt(source),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(destination),
            }],
        }
    }

    fn aac_48k() -> MediaFormat {
        MediaFormat {
            container: Container::MpegTs,
            video: None,
            audio: Some(AudioFormat {
                codec: AudioCodec::Aac,
                sample_rate: 48_000,
                channels: 2,
            }),
        }
    }

    fn wants_44k() -> FormatConstraint {
        FormatConstraint {
            audio: Some(AudioConstraint {
                sample_rate: Some(vec![44_100]),
                ..AudioConstraint::default()
            }),
            ..FormatConstraint::default()
        }
    }

    #[test]
    fn a_destination_that_cannot_accept_the_source_format_is_reported() {
        let stream = stream_with_formats(Some(aac_48k()), Some(wants_44k()));
        let conflicts = stream_format_conflicts(&stream);

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].destination, "studio");
        assert_eq!(
            conflicts[0].to_string(),
            "destination studio cannot accept the source format: \
             audio.sample_rate is 48000 but accepts 44100"
        );
    }

    #[test]
    fn an_undeclared_format_or_constraint_conflicts_with_nothing() {
        // Absence means unknown, not wrong: nothing is inferred either way.
        assert!(stream_format_conflicts(&stream_with_formats(None, Some(wants_44k()))).is_empty());
        assert!(stream_format_conflicts(&stream_with_formats(Some(aac_48k()), None)).is_empty());
        assert!(stream_format_conflicts(&stream_with_formats(None, None)).is_empty());
    }

    #[test]
    fn only_the_destinations_that_conflict_are_reported() {
        let mut stream = stream_with_formats(Some(aac_48k()), None);
        let StreamTransport::Srt(base) = &stream.destinations[0].endpoint else {
            unreachable!("fixture destination is srt");
        };

        let mut fussy = base.clone();
        fussy.accepts = Some(wants_44k());
        let mut relaxed = base.clone();
        relaxed.accepts = Some(FormatConstraint::default());

        let destination = |id: &str, endpoint: &SrtEndpoint| StreamDestination {
            id: id.to_string(),
            paths: 1,
            endpoint: StreamTransport::Srt(endpoint.clone()),
        };
        stream.destinations = vec![
            destination("relaxed", &relaxed),
            destination("fussy-a", &fussy),
            destination("fussy-b", &fussy),
        ];

        let offenders: Vec<String> = stream_format_conflicts(&stream)
            .into_iter()
            .map(|c| c.destination)
            .collect();
        assert_eq!(offenders, vec!["fussy-a", "fussy-b"], "in manifest order");
    }

    #[test]
    fn a_whep_player_that_cannot_accept_a_whip_senders_format_is_reported() {
        let mut stream = stream_with_formats(None, None);
        stream.source = StreamTransport::Whip(SignallingEndpoint {
            node: "strom-node-1".to_string(),
            network: None,
            format: Some(aac_48k()),
            accepts: None,
        });
        stream.destinations[0].endpoint = StreamTransport::Whep(SignallingEndpoint {
            node: "strom-node-2".to_string(),
            network: None,
            format: None,
            accepts: Some(wants_44k()),
        });
        let conflicts = stream_format_conflicts(&stream);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].destination, "studio");
    }

    #[test]
    fn network_attachment_rejects_a_misspelled_field() {
        let attachment: NetworkAttachment = serde_json::from_value(serde_json::json!({
            "id": "client",
            "network": "internet",
            "dial": true
        }))
        .expect("parse attachment");
        assert!(attachment.dial);
        assert!(attachment.listeners.is_empty());

        let result: Result<NetworkAttachment, _> = serde_json::from_value(serde_json::json!({
            "id": "client",
            "network": "internet",
            "dail": true
        }));
        assert!(result.is_err(), "a typo must not read as dial-less");
    }

    #[test]
    fn node_config_rejects_unknown_fields() {
        let mut value = serde_json::json!({
            "id": "n1",
            "southbound_url": "http://127.0.0.1:8081",
            "listen": "0.0.0.0:8091",
            "topology": { "attachments": [] }
        });
        assert!(serde_json::from_value::<NodeConfig>(value.clone()).is_ok());

        value["bogus"] = serde_json::json!(true);
        let result: Result<NodeConfig, _> = serde_json::from_value(value);
        assert!(result.is_err(), "deny_unknown_fields rejects typos");
    }

    #[test]
    fn device_endpoint_round_trips_and_denies_unknown_fields() {
        let json = serde_json::json!({
            "name": "alice-cam",
            "source": { "device": { "node": "browser-a1b2" } },
            "destinations": [ { "id": "studio", "srt": { "node": "strom-node-2" } } ]
        });
        let stream: StreamDefinition = serde_json::from_value(json).expect("parse stream");
        assert_eq!(
            stream.source,
            StreamTransport::Device(NodeEndpoint {
                node: "browser-a1b2".to_string(),
                network: None,
            })
        );
        assert_eq!(stream.source.node(), Some("browser-a1b2"));
        assert_eq!(stream.source.kind(), "device");

        let round_trip: StreamDefinition =
            serde_json::from_str(&serde_json::to_string(&stream).unwrap()).unwrap();
        assert_eq!(stream, round_trip);
        let value = serde_json::to_value(&stream).unwrap();
        assert_eq!(
            value["source"]["device"],
            serde_json::json!({ "node": "browser-a1b2" })
        );

        let with_latency: Result<StreamTransport, _> = serde_json::from_value(serde_json::json!({
            "device": { "node": "browser-a1b2", "latency": 200 }
        }));
        assert!(with_latency.is_err(), "a device endpoint has no SRT fields");
    }

    #[test]
    fn device_source_declares_no_format_so_nothing_conflicts() {
        let stream = StreamDefinition {
            name: "alice-cam".to_string(),
            enabled: true,
            source: StreamTransport::Device(NodeEndpoint {
                node: "browser-a1b2".to_string(),
                network: None,
            }),
            destinations: vec![StreamDestination {
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(SrtEndpoint {
                    node: Some("strom-node-2".to_string()),
                    remote: None,
                    via: Vec::new(),
                    network: None,
                    latency: None,
                    passphrase: None,
                    format: None,
                    accepts: Some(wants_44k()),
                }),
            }],
        };
        assert!(stream_format_conflicts(&stream).is_empty());
    }

    #[test]
    fn every_socket_variant_round_trips_through_its_flat_wire_form() {
        let cases = [
            (
                SocketSpec::srt_listen(7001, 200),
                serde_json::json!({
                    "transport": "srt", "role": "listen", "port": 7001,
                    "params": { "latency": 200 }
                }),
            ),
            (
                SocketSpec::srt_connect("10.0.0.2", 7002, 1000),
                serde_json::json!({
                    "transport": "srt", "role": "connect", "host": "10.0.0.2", "port": 7002,
                    "params": { "latency": 1000 }
                }),
            ),
            (
                SocketSpec::signalling(
                    SignallingTransport::Whip,
                    SocketRole::Listen,
                    "http://172.26.0.10:8080/whip",
                    "weave-x",
                ),
                serde_json::json!({
                    "transport": "whip", "role": "listen",
                    "url": "http://172.26.0.10:8080/whip/weave-x", "endpoint_id": "weave-x"
                }),
            ),
            (
                SocketSpec::signalling(
                    SignallingTransport::Whep,
                    SocketRole::Connect,
                    "http://172.26.0.10:8080/whep",
                    "weave-x",
                ),
                serde_json::json!({
                    "transport": "whep", "role": "connect",
                    "url": "http://172.26.0.10:8080/whep/weave-x", "endpoint_id": "weave-x"
                }),
            ),
            (
                SocketSpec::Device(DeviceKind::Capture),
                serde_json::json!({ "transport": "device", "role": "capture" }),
            ),
            (
                SocketSpec::Device(DeviceKind::Display),
                serde_json::json!({ "transport": "device", "role": "display" }),
            ),
        ];

        for (socket, wire) in cases {
            assert_eq!(serde_json::to_value(&socket).unwrap(), wire);
            let parsed: SocketSpec = serde_json::from_value(wire).unwrap();
            assert_eq!(parsed, socket);
        }
    }

    /// A socket stored before `params` was optional on the wire.
    #[test]
    fn an_srt_socket_parses_with_empty_params() {
        let stored: SocketSpec = serde_json::from_value(serde_json::json!({
            "transport": "srt", "role": "listen", "port": 7001, "params": {}
        }))
        .expect("hydrate");
        assert_eq!(
            stored,
            SocketSpec::Srt(SrtSocket::Listen {
                port: 7001,
                params: SrtParams::default(),
            })
        );
    }

    #[test]
    fn a_socket_that_mixes_up_its_transport_is_rejected() {
        let rejected = [
            serde_json::json!({ "transport": "srt", "role": "listen", "params": {} }),
            serde_json::json!({ "transport": "srt", "role": "connect", "port": 7002 }),
            serde_json::json!({
                "transport": "srt", "role": "listen", "port": 7001, "host": "10.0.0.2"
            }),
            serde_json::json!({
                "transport": "srt", "role": "listen", "port": 7001, "url": "http://x/whip/y"
            }),
            serde_json::json!({ "transport": "whip", "role": "listen" }),
            serde_json::json!({
                "transport": "whip", "role": "listen", "url": "http://x/whip/y"
            }),
            serde_json::json!({
                "transport": "whip", "role": "listen", "url": "http://x/whip/y", "port": 7001,
                "endpoint_id": "y"
            }),
            serde_json::json!({
                "transport": "whep", "role": "connect", "url": "http://x/whep/y",
                "endpoint_id": "y", "params": {}
            }),
            serde_json::json!({ "transport": "device", "role": "connect" }),
            serde_json::json!({ "transport": "srt", "role": "capture", "port": 7001 }),
            serde_json::json!({ "transport": "device", "role": "capture", "port": 7001 }),
            serde_json::json!({ "transport": "srt", "role": "listen", "port": 7001, "bogus": 1 }),
            serde_json::json!({
                "transport": "srt", "role": "listen", "port": 7001, "endpoint_id": "y"
            }),
            serde_json::json!({ "transport": "device", "role": "capture", "endpoint_id": "y" }),
        ];

        for wire in rejected {
            let result: Result<SocketSpec, _> = serde_json::from_value(wire.clone());
            assert!(result.is_err(), "{wire} must not parse");
        }
    }

    #[test]
    fn socket_display_names_the_transport_or_the_device() {
        assert_eq!(SocketSpec::srt_listen(7001, 200).to_string(), "srt");
        assert_eq!(
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Listen,
                "http://x/whip",
                "y"
            )
            .to_string(),
            "whip"
        );
        assert_eq!(
            SocketSpec::signalling(
                SignallingTransport::Whep,
                SocketRole::Connect,
                "http://x/whep",
                "y"
            )
            .to_string(),
            "whep"
        );
        assert_eq!(
            SocketSpec::Device(DeviceKind::Capture).to_string(),
            "capture device"
        );
        assert_eq!(
            SocketSpec::Device(DeviceKind::Display).to_string(),
            "display device"
        );
    }

    #[test]
    fn a_transport_offered_in_no_role_or_under_no_known_name_is_rejected() {
        let no_role: Result<TransportClass, _> =
            serde_json::from_value(serde_json::json!({ "transport": "whip", "roles": [] }));
        assert!(no_role.is_err(), "a transport offered in no role");

        let unknown: Result<TransportClass, _> =
            serde_json::from_value(serde_json::json!({ "transport": "rist", "roles": ["listen"] }));
        assert!(unknown.is_err(), "an unknown transport name is an error");
    }

    #[test]
    fn hop_profiles_decide_which_transports_roles_and_devices_a_node_offers() {
        let capabilities: NodeCapabilities = serde_json::from_value(serde_json::json!({
            "hop_profiles": [{
                "id": "camera-to-whip",
                "ingress": { "device": "capture" },
                "egress": { "transport": "whip", "roles": ["connect"] },
                "max_egresses": 1
            }]
        }))
        .expect("parse capabilities");

        assert!(capabilities.offers_ingress_device(DeviceKind::Capture));
        assert!(!capabilities.offers_ingress_device(DeviceKind::Display));
        assert!(!capabilities.offers_egress_device(DeviceKind::Capture));
        assert!(capabilities.offers_egress(Transport::Whip, SocketRole::Connect));
        assert!(!capabilities.offers_egress(Transport::Whip, SocketRole::Listen));
        assert!(!capabilities.offers_ingress(Transport::Whip, SocketRole::Connect));
        assert!(!capabilities.offers_ingress(Transport::Srt, SocketRole::Listen));

        let round_trip: NodeCapabilities =
            serde_json::from_str(&serde_json::to_string(&capabilities).unwrap()).unwrap();
        assert_eq!(capabilities, round_trip);

        let bare = NodeCapabilities::default();
        assert!(!bare.offers_ingress(Transport::Srt, SocketRole::Listen));
        assert!(!bare.offers_egress(Transport::Srt, SocketRole::Connect));
        assert!(!bare.offers_ingress_device(DeviceKind::Capture));
    }

    #[test]
    fn network_listeners_carry_signalling_bases_per_webrtc_transport() {
        let listeners: NetworkListeners = serde_json::from_value(serde_json::json!({
            "whip": { "base_url": "http://172.26.0.10:8080/whip" },
            "whep": { "base_url": "http://172.26.0.10:8080/whep" }
        }))
        .expect("parse");
        assert!(!listeners.is_empty());
        assert_eq!(
            listeners.signalling(SignallingTransport::Whip),
            Some("http://172.26.0.10:8080/whip")
        );
        assert_eq!(
            listeners.signalling(SignallingTransport::Whep),
            Some("http://172.26.0.10:8080/whep")
        );
        let round_trip: NetworkListeners =
            serde_json::from_str(&serde_json::to_string(&listeners).unwrap()).unwrap();
        assert_eq!(listeners, round_trip);

        let srt_only: NetworkListeners = serde_json::from_value(serde_json::json!({
            "srt": { "host": "172.26.0.10", "port_range": { "start": 20000, "end": 20999 } }
        }))
        .expect("parse srt listener");
        assert_eq!(srt_only.signalling(SignallingTransport::Whip), None);
        assert_eq!(
            serde_json::to_value(&srt_only).unwrap(),
            serde_json::json!({
                "srt": { "host": "172.26.0.10", "port_range": { "start": 20000, "end": 20999 } }
            }),
            "a listener set hosting no signalling serializes none"
        );
    }

    #[test]
    fn signalling_socket_joins_base_and_endpoint_id() {
        let trimmed = SignallingSocket::new(SocketRole::Listen, "http://x:8080/whip/", "weave-a");
        let bare = SignallingSocket::new(SocketRole::Listen, "http://x:8080/whip", "weave-a");
        assert_eq!(trimmed.url, "http://x:8080/whip/weave-a");
        assert_eq!(trimmed.endpoint_id, "weave-a");
        assert_eq!(
            trimmed, bare,
            "a trailing slash on the base changes nothing"
        );
    }

    #[test]
    fn signalling_transport_maps_both_ways_with_transport() {
        assert_eq!(Transport::Srt.signalling(), None);
        assert_eq!(
            Transport::Whip.signalling(),
            Some(SignallingTransport::Whip)
        );
        assert_eq!(
            Transport::Whep.signalling(),
            Some(SignallingTransport::Whep)
        );
        assert_eq!(SignallingTransport::Whip.transport(), Transport::Whip);
        assert_eq!(SignallingTransport::Whep.transport(), Transport::Whep);
    }

    #[test]
    fn transport_and_device_names_match_their_wire_form() {
        for transport in [Transport::Srt, Transport::Whip, Transport::Whep] {
            assert_eq!(
                serde_json::to_value(transport).unwrap(),
                serde_json::json!(transport.name())
            );
            assert_eq!(transport.to_string(), transport.name());
        }
        for kind in [DeviceKind::Capture, DeviceKind::Display] {
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::json!(kind.name())
            );
            assert_eq!(kind.to_string(), kind.name());
        }
    }

    #[test]
    fn a_merge_ingress_is_reported_exactly_when_desired() {
        let socket = |condition| SocketStatus {
            condition,
            resolved: None,
            stats: None,
        };
        let mut desired = sample_hop();
        desired.merge_ingress = Some(SocketSpec::srt_listen(7004, 1000));
        let mut status = HopStatus {
            id: desired.id.clone(),
            node_id: desired.node_id.clone(),
            state: HopState::Provisioned,
            ingress: socket(LinkCondition::Flowing),
            merge_ingress: None,
            egresses: vec![EgressStatus {
                branch_id: "studio".to_string(),
                status: socket(LinkCondition::Flowing),
            }],
        };
        assert_eq!(status.conditions(&desired), None);

        status.merge_ingress = Some(socket(LinkCondition::Idle));
        let conditions = status.conditions(&desired).unwrap();
        assert_eq!(conditions.merge_ingress, Some(LinkCondition::Idle));
        assert_eq!(status.conditions(&sample_hop()), None);

        let sender = hc(
            HopState::Provisioned,
            LinkCondition::Flowing,
            LinkCondition::Flowing,
        );
        assert_eq!(
            roll_up_path(true, &[sender.clone(), Some(conditions.clone())]),
            PathStatus::Degraded
        );
        let flowing = HopConditions {
            merge_ingress: Some(LinkCondition::Flowing),
            ..conditions.clone()
        };
        assert_eq!(
            roll_up_path(true, &[sender.clone(), Some(flowing)]),
            PathStatus::Flowing
        );
        let stalled = HopConditions {
            merge_ingress: Some(LinkCondition::Stalled),
            ..conditions
        };
        assert_eq!(
            roll_up_path(true, &[sender, Some(stalled)]),
            PathStatus::Degraded
        );
    }

    #[test]
    fn hop_profile_merge_defaults_off_and_is_omitted_when_off() {
        let profile: HopProfile = serde_json::from_value(serde_json::json!({
            "id": "srt-merge",
            "ingress": { "transport": "srt", "roles": ["listen", "connect"] },
            "egress": { "transport": "srt", "roles": ["listen"] }
        }))
        .unwrap();
        assert!(!profile.merge);
        assert!(
            serde_json::to_value(&profile)
                .unwrap()
                .get("merge")
                .is_none()
        );
        let merging = HopProfile {
            merge: true,
            ..profile
        };
        assert_eq!(serde_json::to_value(&merging).unwrap()["merge"], true);
    }
}
