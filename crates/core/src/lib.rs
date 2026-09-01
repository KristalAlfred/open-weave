//! Shared domain types for open-weave.

pub mod auth;
pub mod media;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use media::{
    AudioCodec, AudioConstraint, AudioFormat, ChromaSubsampling, Container, FormatConstraint,
    Framerate, MediaFormat, Mismatch, VideoCodec, VideoConstraint, VideoFormat,
};

/// Conventional data-plane alias resolved when a manifest pins no network.
pub const DEFAULT_DATA_PLANE_ALIAS: &str = "default";

/// Path prefix every control-plane API route is served under. Servers nest their
/// routes behind it and clients build their paths from it, so the literal exists
/// once for the whole workspace.
///
/// The operator (northbound) and adapter (southbound) contracts share one prefix:
/// they are two halves of the same control plane and move to a `/v2` together.
///
/// Outside it: `/health` on every service, which healthchecks and
/// load balancers address directly, and the controller's `/`, `/ui`, and `/view` —
/// the dashboard ships inside the controller binary and versions with it.
pub const API_V1: &str = "/v1";

/// Wire-protocol version an adapter declares when it registers.
///
/// [`API_V1`] tells an adapter *where* to send a request; this tells the
/// controller *what* the adapter on the other end speaks, so a stale adapter is
/// rejected at registration instead of being served desired state it cannot
/// realise. The two move together — a breaking southbound change bumps both.
pub const PROTOCOL_VERSION: u32 = 1;

/// Whether the controller can serve an adapter declaring protocol `version`.
///
/// Exactly one version is supported at a time, so equality is the whole rule.
/// `0` is what an adapter predating the handshake deserializes to (the field
/// defaults) and is never compatible.
#[must_use]
pub fn protocol_compatible(version: u32) -> bool {
    version == PROTOCOL_VERSION
}

/// Operator intent: one source streamed to one or more destinations over a transport.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamDefinition {
    pub name: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub source: StreamTransport,
    pub destinations: Vec<StreamTransport>,
}

fn default_enabled() -> bool {
    true
}

/// Transport carrying a stream endpoint. Externally tagged by transport name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTransport {
    Srt(SrtEndpoint),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SrtEndpoint {
    /// Registered node id hosting this endpoint. Mutually exclusive with
    /// [`SrtEndpoint::remote`]; exactly one must be set (enforced at validation).
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    pub via: Vec<String>,
    /// Data-plane alias resolved against the node's declared address map.
    /// Absent means [`DEFAULT_DATA_PLANE_ALIAS`].
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteAddr {
    pub host: String,
    pub port: u16,
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
    let StreamTransport::Srt(source) = &stream.source;
    let Some(format) = &source.format else {
        return Vec::new();
    };

    stream
        .destinations
        .iter()
        .enumerate()
        .filter_map(|(index, destination)| {
            let StreamTransport::Srt(destination) = destination;
            let mismatches = destination.accepts.as_ref()?.mismatches(format);
            (!mismatches.is_empty()).then_some(media::FormatConflict {
                destination: index,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Path {
    pub stream: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub hops: Vec<DesiredHop>,
}

/// One provisioning unit placed on a single node: one ingress socket fanned out
/// to one or more egress sockets (a sender tees to every destination).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DesiredHop {
    pub id: String,
    pub node_id: String,
    pub role: HopRole,
    pub ingress: SocketSpec,
    pub egresses: Vec<SocketSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HopRole {
    Sender,
    Bridge,
    Receiver,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketSpec {
    pub transport: Transport,
    pub role: SocketRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default)]
    pub params: SrtParams,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketRole {
    Listen,
    Connect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Srt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SrtParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<u32>,
}

/// Node-reported realisation status for one desired hop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HopStatus {
    pub id: String,
    pub node_id: String,
    pub state: HopState,
    #[serde(default)]
    pub ingress: LinkCondition,
    #[serde(default)]
    pub egress: LinkCondition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_ingress: Option<ResolvedAddr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_egress: Option<ResolvedAddr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<LinkStats>,
}

impl HopStatus {
    /// The lifecycle + per-socket conditions consumed by [`roll_up_path`].
    #[must_use]
    pub fn conditions(&self) -> HopConditions {
        HopConditions {
            state: self.state,
            ingress: self.ingress,
            egress: self.egress,
        }
    }
}

/// Control-plane lifecycle of a hop's provisioning. Runtime link health is
/// reported separately per socket via [`LinkCondition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HopState {
    Pending,
    Provisioned,
    Failed,
}

/// Observed condition of one socket on a hop, independent of control-plane lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LinkCondition {
    /// No SRT connection; the socket is a listener patiently waiting. Healthy.
    #[default]
    Idle,
    /// No SRT connection; the socket is a caller still retrying. Ambiguous, not degraded.
    Connecting,
    /// SRT connection up but rate ~0 — fine on our end, nothing coming through yet.
    Connected,
    /// SRT connection up and media flowing.
    Flowing,
    /// Ingress once carried media but byte progress has frozen while the flow still
    /// claims to run — detected across polls, not from any instantaneous field.
    Stalled,
}

/// Lifecycle plus both socket conditions of one hop — the unit rolled up per path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HopConditions {
    pub state: HopState,
    pub ingress: LinkCondition,
    pub egress: LinkCondition,
}

/// End-to-end status of a path, derived from its hops' conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
        .any(|hop| hop.is_some_and(|h| h.state == HopState::Failed))
    {
        return PathStatus::Failed;
    }
    if hops
        .iter()
        .any(|hop| hop.is_none_or(|h| h.state == HopState::Pending))
    {
        return PathStatus::Pending;
    }
    if hops
        .iter()
        .flatten()
        .any(|h| h.ingress == LinkCondition::Stalled || h.egress == LinkCondition::Stalled)
    {
        return PathStatus::Degraded;
    }

    let source_flowing =
        hops.first().and_then(|hop| hop.map(|h| h.ingress)) == Some(LinkCondition::Flowing);
    if !source_flowing {
        return PathStatus::AwaitingInput;
    }

    let all_flowing = hops
        .iter()
        .flatten()
        .all(|h| h.ingress == LinkCondition::Flowing && h.egress == LinkCondition::Flowing);
    if all_flowing {
        PathStatus::Flowing
    } else {
        PathStatus::Degraded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedAddr {
    pub host: String,
    pub port: u16,
}

/// Link-level SRT stats mirrored from Strom's `srt-stats` payload.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub struct LinkStats {
    #[serde(default)]
    pub connections: usize,
    #[serde(default)]
    pub ingress_rate_mbps: f64,
    #[serde(default)]
    pub egress_rate_mbps: f64,
    #[serde(default)]
    pub packets_sent_lost: i64,
    #[serde(default)]
    pub packets_retransmitted: i64,
    #[serde(default)]
    pub packets_received_lost: i64,
    #[serde(default)]
    pub packets_received_retransmitted: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDescriptor {
    pub id: String,
    pub endpoint: String,
    pub status: NodeStatus,
    #[serde(default)]
    pub capabilities: NodeCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NodeCapabilities {
    #[serde(default)]
    pub adapters: Vec<AdapterDescriptor>,
    #[serde(default)]
    pub transports: Vec<TransportDescriptor>,
    /// Data-plane addresses this node advertises, keyed by alias. The
    /// [`DEFAULT_DATA_PLANE_ALIAS`] entry serves node-referenced endpoints
    /// that pin no network.
    #[serde(default)]
    pub data_plane: BTreeMap<String, DataPlaneAddr>,
    /// Inclusive port range the controller may assign from for this node's
    /// hops. A soft contract: bind failures surface as [`HopState::Failed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port_range: Option<PortRange>,
    /// Whether this node may carry transit for streams that neither terminate
    /// nor originate on it — the pool the planner draws a relay from when two
    /// endpoints cannot dial each other.
    #[serde(default)]
    pub relay: bool,
}

/// One data-plane address a node advertises, plus whether peers can open
/// connections to it.
///
/// Deserializes from either a bare host string or a full mapping, so
/// `default: 172.26.0.10` and
/// `wan: { host: 203.0.113.7, reachability: outbound_only }` are both valid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DataPlaneAddr {
    pub host: String,
    pub reachability: Reachability,
}

impl DataPlaneAddr {
    /// An address peers can dial — the shorthand form's meaning.
    #[must_use]
    pub fn dialable(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            reachability: Reachability::Dialable,
        }
    }

    #[must_use]
    pub fn is_dialable(&self) -> bool {
        self.reachability == Reachability::Dialable
    }
}

impl<'de> Deserialize<'de> for DataPlaneAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Full {
            host: String,
            #[serde(default)]
            reachability: Reachability,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Host(String),
            Full(Full),
        }

        Ok(match Repr::deserialize(deserializer)? {
            Repr::Host(host) => Self::dialable(host),
            Repr::Full(Full { host, reachability }) => Self { host, reachability },
        })
    }
}

/// Whether peers can open a connection to an address, or the node behind it can
/// only dial out. Planning reads this to decide which end of a link listens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Reachability {
    /// Peers can connect to this address.
    #[default]
    Dialable,
    /// Behind NAT or a firewall: this node must initiate every connection.
    OutboundOnly,
}

/// Inclusive `[start, end]` range of ports a node offers for controller-side
/// assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
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
    /// Data-plane addresses advertised for placement, keyed by alias. Must
    /// contain the [`DEFAULT_DATA_PLANE_ALIAS`] entry. Each is either a bare
    /// host (dialable) or a mapping pinning its [`Reachability`].
    pub data_plane: BTreeMap<String, DataPlaneAddr>,
    /// Inclusive port range the controller may assign from for this node.
    pub port_range: PortRange,
    #[serde(default)]
    pub transports: Vec<String>,
    /// Whether this node offers itself as transit for other nodes' streams.
    #[serde(default)]
    pub relay: bool,
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

    /// Enforce the invariants placement depends on: a named node, a `default`
    /// data-plane alias with no blank entries, and a well-ordered port range.
    ///
    /// # Errors
    /// Returns [`ConfigError`] describing the first violated invariant.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.id.trim().is_empty() {
            return Err(ConfigError::EmptyId);
        }
        if !self.data_plane.contains_key(DEFAULT_DATA_PLANE_ALIAS) {
            return Err(ConfigError::MissingDefaultAlias);
        }
        if self
            .data_plane
            .iter()
            .any(|(alias, addr)| alias.trim().is_empty() || addr.host.trim().is_empty())
        {
            return Err(ConfigError::EmptyDataPlaneEntry);
        }
        if self.port_range.start > self.port_range.end {
            return Err(ConfigError::InvalidPortRange {
                start: self.port_range.start,
                end: self.port_range.end,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("node id must not be empty")]
    EmptyId,
    #[error("data_plane must define the '{DEFAULT_DATA_PLANE_ALIAS}' alias")]
    MissingDefaultAlias,
    #[error("data_plane has an empty alias or host")]
    EmptyDataPlaneEntry,
    #[error("port_range start {start} exceeds end {end}")]
    InvalidPortRange { start: u16, end: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterDescriptor {
    pub name: String,
    pub kind: AdapterKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportDescriptor {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointDescriptor {
    pub id: String,
    pub label: String,
    pub node_id: Option<String>,
    pub kind: EndpointKind,
    #[serde(default)]
    pub transports: Vec<TransportDescriptor>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Source,
    Destination,
    Bidirectional,
    Flow,
    Gateway,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub node_id: String,
    pub status: NodeStatus,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hop_status: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservedState {
    #[serde(default)]
    pub nodes: Vec<NodeDescriptor>,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hops: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileReport {
    pub status: ReconcileStatus,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileStatus {
    Idle,
    Converging,
    Converged,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
                { "srt": { "node": "strom-node-2", "network": "wan" } }
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
            })
        );
        assert_eq!(
            stream.destinations[0],
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: Some("wan".to_string()),
                latency: None,
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
            role: HopRole::Sender,
            ingress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Listen,
                host: None,
                port: Some(7001),
                params: SrtParams { latency: Some(200) },
            },
            egresses: vec![SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Connect,
                host: Some("172.31.0.10".to_string()),
                port: Some(7002),
                params: SrtParams {
                    latency: Some(1000),
                },
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
        let mut second = hop.egresses[0].clone();
        second.port = Some(7003);
        hop.egresses.push(second);
        assert_eq!(hop.egresses.len(), 2);

        let round_trip: DesiredHop =
            serde_json::from_str(&serde_json::to_string(&hop).unwrap()).unwrap();
        assert_eq!(hop, round_trip);
    }

    #[test]
    fn socket_spec_omits_absent_host_and_port() {
        let listen = SocketSpec {
            transport: Transport::Srt,
            role: SocketRole::Listen,
            host: None,
            port: Some(7001),
            params: SrtParams::default(),
        };
        let value = serde_json::to_value(&listen).unwrap();
        assert!(value.get("host").is_none(), "absent host is not serialized");
        assert_eq!(value["role"], "listen");
        assert_eq!(value["transport"], "srt");
    }

    #[test]
    fn hop_status_round_trips_with_optional_fields_absent() {
        let status = HopStatus {
            id: "weave-contribution-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            state: HopState::Provisioned,
            ingress: LinkCondition::Flowing,
            egress: LinkCondition::Connected,
            resolved_ingress: Some(ResolvedAddr {
                host: "0.0.0.0".to_string(),
                port: 7001,
            }),
            resolved_egress: None,
            stats: Some(LinkStats {
                connections: 1,
                ingress_rate_mbps: 4.5,
                ..LinkStats::default()
            }),
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
                "status": "ready"
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
            egress,
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
    fn hop_status_conditions_extracts_state_and_link_conditions() {
        let status = HopStatus {
            id: "weave-a".to_string(),
            node_id: "n1".to_string(),
            state: HopState::Provisioned,
            ingress: LinkCondition::Flowing,
            egress: LinkCondition::Connected,
            resolved_ingress: None,
            resolved_egress: None,
            stats: None,
        };
        assert_eq!(
            status.conditions(),
            HopConditions {
                state: HopState::Provisioned,
                ingress: LinkCondition::Flowing,
                egress: LinkCondition::Connected,
            }
        );
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
            "remote": { "host": "198.51.100.5", "port": 9000 }
        }))
        .expect("parse remote");
        assert_eq!(
            remote.remote,
            Some(RemoteAddr {
                host: "198.51.100.5".to_string(),
                port: 9000,
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
            data_plane: BTreeMap::from([(
                DEFAULT_DATA_PLANE_ALIAS.to_string(),
                DataPlaneAddr::dialable("172.26.0.10"),
            )]),
            port_range: PortRange {
                start: 20000,
                end: 20999,
            },
            transports: vec!["srt".to_string()],
            relay: false,
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
        assert_eq!(empty_id.validate(), Err(ConfigError::EmptyId));

        let mut no_default = node_config();
        no_default.data_plane =
            BTreeMap::from([("wan".to_string(), DataPlaneAddr::dialable("203.0.113.7"))]);
        assert_eq!(no_default.validate(), Err(ConfigError::MissingDefaultAlias));

        let mut blank_host = node_config();
        blank_host
            .data_plane
            .insert("wan".to_string(), DataPlaneAddr::dialable(""));
        assert_eq!(blank_host.validate(), Err(ConfigError::EmptyDataPlaneEntry));

        let mut bad_range = node_config();
        bad_range.port_range = PortRange {
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
            destinations: vec![StreamTransport::Srt(destination)],
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
        assert_eq!(conflicts[0].destination, 0);
        assert_eq!(
            conflicts[0].to_string(),
            "destination 0 cannot accept the source format: \
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
        let StreamTransport::Srt(base) = &stream.destinations[0];

        let mut fussy = base.clone();
        fussy.accepts = Some(wants_44k());
        let mut relaxed = base.clone();
        relaxed.accepts = Some(FormatConstraint::default());

        stream.destinations = vec![
            StreamTransport::Srt(relaxed),
            StreamTransport::Srt(fussy.clone()),
            StreamTransport::Srt(fussy),
        ];

        let offenders: Vec<usize> = stream_format_conflicts(&stream)
            .iter()
            .map(|c| c.destination)
            .collect();
        assert_eq!(offenders, vec![1, 2], "indices match manifest order");
    }

    #[test]
    fn data_plane_addr_parses_shorthand_and_full_forms() {
        let capabilities: NodeCapabilities = serde_json::from_value(serde_json::json!({
            "data_plane": {
                "default": "172.26.0.10",
                "wan": { "host": "203.0.113.7", "reachability": "outbound_only" },
                "lan": { "host": "10.0.0.4" }
            }
        }))
        .expect("parse capabilities");

        assert_eq!(
            capabilities.data_plane["default"],
            DataPlaneAddr::dialable("172.26.0.10"),
            "a bare host is dialable"
        );
        assert_eq!(
            capabilities.data_plane["wan"].reachability,
            Reachability::OutboundOnly
        );
        assert!(
            capabilities.data_plane["lan"].is_dialable(),
            "an omitted reachability defaults to dialable"
        );
        assert!(!capabilities.relay, "relay defaults off");

        let round_trip: NodeCapabilities =
            serde_json::from_str(&serde_json::to_string(&capabilities).unwrap()).unwrap();
        assert_eq!(capabilities, round_trip);
    }

    /// Registrations persisted before reachability existed carry bare host
    /// strings and no `relay`. They are read back, not migrated, so the shorthand
    /// has to keep meaning what it always did.
    #[test]
    fn a_registration_stored_before_reachability_still_hydrates() {
        let stored: NodeRegistration = serde_json::from_value(serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "node": {
                "id": "strom-node-1",
                "endpoint": "http://strom-node-1:8091",
                "status": "ready",
                "capabilities": {
                    "data_plane": { "default": "172.26.0.10" },
                    "port_range": { "start": 20000, "end": 20999 }
                }
            }
        }))
        .expect("hydrate stored registration");

        let capabilities = &stored.node.capabilities;
        assert!(capabilities.data_plane["default"].is_dialable());
        assert!(!capabilities.relay);
    }

    #[test]
    fn data_plane_addr_rejects_a_misspelled_field() {
        let result: Result<DataPlaneAddr, _> = serde_json::from_value(serde_json::json!({
            "host": "10.0.0.4",
            "reachablity": "outbound_only"
        }));
        assert!(result.is_err(), "a typo must not read as dialable");
    }

    #[test]
    fn node_config_rejects_unknown_fields() {
        let result: Result<NodeConfig, _> = serde_json::from_value(serde_json::json!({
            "id": "n1",
            "southbound_url": "http://127.0.0.1:8081",
            "listen": "0.0.0.0:8091",
            "data_plane": { "default": "10.0.0.1" },
            "port_range": { "start": 1, "end": 2 },
            "bogus": true
        }));
        assert!(result.is_err(), "deny_unknown_fields rejects typos");
    }
}
