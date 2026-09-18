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
pub const PROTOCOL_VERSION: u32 = 4;

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
            format: None,
            accepts: None,
        })
    }

    fn destination(id: &str, node: &str) -> StreamDestination {
        StreamDestination {
            id: id.to_string(),
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
    #[serde(flatten)]
    pub endpoint: StreamTransport,
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
}

impl StreamTransport {
    /// The registered node this endpoint is placed on, when it names one.
    #[must_use]
    pub fn node(&self) -> Option<&str> {
        match self {
            Self::Srt(endpoint) => endpoint.node.as_deref(),
            Self::Device(endpoint) => Some(&endpoint.node),
        }
    }

    /// The shared network this endpoint uses, when pinned.
    #[must_use]
    pub fn network(&self) -> Option<&str> {
        match self {
            Self::Srt(endpoint) => endpoint.network.as_deref(),
            Self::Device(endpoint) => endpoint.network.as_deref(),
        }
    }

    /// The manifest tag of this variant.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Srt(_) => "srt",
            Self::Device(_) => DEVICE_TRANSPORT,
        }
    }
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
    let StreamTransport::Srt(source) = &stream.source else {
        return Vec::new();
    };
    let Some(format) = &source.format else {
        return Vec::new();
    };

    stream
        .destinations
        .iter()
        .filter_map(|destination| {
            let StreamTransport::Srt(endpoint) = &destination.endpoint else {
                return None;
            };
            let mismatches = endpoint.accepts.as_ref()?.mismatches(format);
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
    pub egresses: Vec<DesiredEgress>,
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
    pub fn params(&self) -> SrtParams {
        match self {
            Self::Listen { params, .. } | Self::Connect { params, .. } => *params,
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
                params: Some(*params),
                ..bare(SocketTransport::Srt, SocketEnd::Listen)
            },
            SocketSpec::Srt(SrtSocket::Connect { host, port, params }) => Self {
                host: Some(host.clone()),
                port: Some(*port),
                params: Some(*params),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub struct SrtParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<u32>,
}

/// Node-reported realisation status for one desired hop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HopStatus {
    pub id: String,
    pub node_id: String,
    pub state: HopState,
    pub ingress: SocketStatus,
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
    /// exactly once and no others.
    #[must_use]
    pub fn conditions(&self, desired: &DesiredHop) -> Option<HopConditions> {
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
    pub egresses: Vec<LinkCondition>,
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
        h.ingress == LinkCondition::Stalled || h.egresses.contains(&LinkCondition::Stalled)
    }) {
        return PathStatus::Degraded;
    }

    let source_flowing = hops.first().and_then(|hop| hop.as_ref().map(|h| h.ingress))
        == Some(LinkCondition::Flowing);
    if !source_flowing {
        return PathStatus::AwaitingInput;
    }

    let all_flowing = hops.iter().flatten().all(|h| {
        h.ingress == LinkCondition::Flowing
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
    /// Bearer token presented to southbound. Falls back to
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
    pub host: String,
    pub port: u16,
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
