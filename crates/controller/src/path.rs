//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use weave_core::{
    DEFAULT_DATA_PLANE_ALIAS, DataPlaneAddr, DesiredHop, DeviceKind, HOP_ID_PREFIX, HopConditions,
    HopRole, HopStatus, NodeDescriptor, NodeStatus, Path, PathStatus, PortRange, RemoteAddr,
    SocketRole, SocketSpec, SrtSocket, StreamDefinition, StreamTransport, Transport, roll_up_path,
};

const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;
const RECV_CONSUMER_LATENCY: u32 = 200;

/// Link transports in the order the planner prefers them.
const TRANSPORT_PREFERENCE: [Transport; 3] = [Transport::Srt, Transport::Whip, Transport::Whep];

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("stream has no destinations")]
    NoDestination,
    #[error("node {node} is not registered")]
    NodeNotRegistered { node: String },
    #[error("node {node} declares no data-plane address for alias {alias}")]
    UnknownAlias { node: String, alias: String },
    #[error("node {node} declares no assignable port range")]
    NoPortRange { node: String },
    #[error("node {node} has no free port left in its range")]
    PortRangeExhausted { node: String },
    #[error("path for stream {stream} has no hop {hop}")]
    MissingHop { stream: String, hop: String },
    #[error("hop {hop} has no socket for consumers to dial")]
    NoConsumerSocket { hop: String },
    #[error(
        "hop {hop} carries a {} socket where an SRT listener is needed",
        socket_end_description(.socket)
    )]
    NotAnSrtListener { hop: String, socket: SocketSpec },
    #[error("stream source must be a node, not a remote endpoint")]
    RemoteSource,
    #[error("endpoint must set exactly one of node or remote")]
    EndpointPlacement,
    #[error(
        "no route from {upstream} to {downstream}: neither can be dialled and no relay node is available"
    )]
    NoRelayAvailable {
        upstream: String,
        downstream: String,
    },
    #[error(
        "no transport connects {upstream} to {downstream}: they offer none in complementary roles and no relay node bridges them"
    )]
    NoCommonTransport {
        upstream: String,
        downstream: String,
    },
    #[error("node {node} hosts a {transport} link but declares no signalling base for it")]
    NoSignalling { node: String, transport: Transport },
    #[error("node {node} offers no {kind} device")]
    NoDevice { node: String, kind: DeviceKind },
    #[error("a source endpoint must not pin via")]
    SourceVia,
}

/// Names `socket` for [`PlacementError::NotAnSrtListener`]. A device's `Display`
/// already names which one it is; a link transport's does not say which end, so
/// its role is prefixed on.
fn socket_end_description(socket: &SocketSpec) -> String {
    match socket {
        SocketSpec::Device(_) => socket.to_string(),
        _ => format!("{socket} {}", socket.end_name()),
    }
}

#[must_use]
pub fn sender_hop_id(stream: &str) -> String {
    format!("{HOP_ID_PREFIX}{stream}-sender")
}

#[must_use]
pub fn receiver_hop_id(stream: &str, index: usize) -> String {
    format!("{HOP_ID_PREFIX}{stream}-receiver-{index}")
}

/// Id of the bridge at `position` along the chain carrying destination `dest`.
/// Position is counted after relay insertion, so an auto-inserted relay and a
/// pinned one are named the same way.
#[must_use]
pub fn bridge_hop_id(stream: &str, dest: usize, position: usize) -> String {
    format!("{HOP_ID_PREFIX}{stream}-bridge-{dest}-{position}")
}

/// Where a stream endpoint is placed: on a registered node, or dialed out to an
/// external SRT listener.
enum Placement<'a> {
    Node(&'a str),
    Remote(&'a RemoteAddr),
}

/// How the media enters or leaves the stream at a manifest endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    /// An SRT socket a producer or consumer dials.
    Srt,
    /// The node's own capture or display device; nothing external attaches.
    Device,
}

/// A manifest endpoint as the planner reads it, whichever variant wrote it.
struct Endpoint<'a> {
    placement: Placement<'a>,
    network: Option<&'a str>,
    via: &'a [String],
    latency: Option<u32>,
    terminal: Terminal,
}

fn read_endpoint(endpoint: &StreamTransport) -> Result<Endpoint<'_>, PlacementError> {
    match endpoint {
        StreamTransport::Srt(endpoint) => Ok(Endpoint {
            placement: match (&endpoint.node, &endpoint.remote) {
                (Some(node), None) => Placement::Node(node),
                (None, Some(remote)) => Placement::Remote(remote),
                _ => return Err(PlacementError::EndpointPlacement),
            },
            network: endpoint.network.as_deref(),
            via: &endpoint.via,
            latency: endpoint.latency,
            terminal: Terminal::Srt,
        }),
        StreamTransport::Device(endpoint) => Ok(Endpoint {
            placement: Placement::Node(&endpoint.node),
            network: endpoint.network.as_deref(),
            via: &[],
            latency: None,
            terminal: Terminal::Device,
        }),
    }
}

/// Per-tick, per-node port occupancy. Assigns every port a path needs from the
/// node's declared range, preferring a deterministic FNV offset then linear
/// probing forward (wrapping within the range) to the first free port, so a
/// given stream set always resolves to the same collision-free assignment.
#[derive(Debug, Default)]
pub struct PortAllocator {
    used: HashMap<String, HashSet<u16>>,
}

impl PortAllocator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn claim(&mut self, node: &NodeDescriptor, key: &str) -> Result<u16, PlacementError> {
        let range = node
            .capabilities
            .port_range
            .ok_or_else(|| PlacementError::NoPortRange {
                node: node.id.clone(),
            })?;
        let count = u32::from(range.span()) + 1;
        let preferred = preferred_offset(range, key);
        let occupied = self.used.entry(node.id.clone()).or_default();
        for step in 0..count {
            let offset = (preferred + step) % count;
            #[allow(clippy::cast_possible_truncation)]
            let port = range.start.saturating_add(offset as u16);
            if occupied.insert(port) {
                return Ok(port);
            }
        }
        Err(PlacementError::PortRangeExhausted {
            node: node.id.clone(),
        })
    }
}

/// Derive the ordered (source→destination) hop chain realising one stream.
///
/// Placement is by `node`: the sender runs on `source.node`, each node-referenced
/// receiver on its destination's `node`, and a bridge on every node the
/// destination relays through. Addresses resolve at planning time from the
/// station's data-plane alias, and every port is claimed from `ports`, the
/// per-tick collision-aware allocator. A hop's reported `resolved_ingress` is
/// observability only and never rewrites a planned socket.
///
/// Each link's transport and direction come from both ends' declared
/// capabilities, see [`link_transport`]: SRT when both ends offer it, with the
/// downstream listening whenever it can be dialled and the direction reversing
/// otherwise; WHIP when a node that can only push media meets one hosting an
/// ingest; WHEP when a node hosting playback meets one that pulls it. When no
/// transport connects the two ends directly, [`chain_hops`] inserts a relay node
/// compatible with both halves — the NAT-to-NAT case and the browser-to-browser
/// case resolve into two links under the same rule rather than a special path.
///
/// A `remote` destination places no receiver hop and claims no port for its
/// terminal link: whichever hop precedes it gains one caller egress to the
/// external listener. A `device` endpoint claims no port either: the media
/// starts at the node's camera or ends at its screen, so its terminal socket is
/// a [`SocketSpec::Device`] with no address. The source must be a node; a remote
/// source is rejected.
///
/// Fan-out is one sender hop teeing to one egress per destination, each with its
/// own chain. Placement is all-or-nothing: if any destination is unplaceable the
/// whole derivation fails and the stream stays pending.
pub fn derive_path(
    stream: &StreamDefinition,
    nodes: &[NodeDescriptor],
    _observed: &[HopStatus],
    ports: &mut PortAllocator,
) -> Result<Path, PlacementError> {
    let source = read_endpoint(&stream.source)?;
    if stream.destinations.is_empty() {
        return Err(PlacementError::NoDestination);
    }
    if !source.via.is_empty() {
        return Err(PlacementError::SourceVia);
    }

    let sender_node = source_node(&source)?;
    let sender_id = sender_hop_id(&stream.name);
    let source_station = Station {
        node_id: sender_node.to_string(),
        network: source.network.map(str::to_string),
    };

    let mut sender_egresses = Vec::with_capacity(stream.destinations.len());
    let mut downstream = Vec::new();

    for (index, dest) in stream.destinations.iter().enumerate() {
        let dest = read_endpoint(dest)?;
        let latency = dest.latency.unwrap_or(DEFAULT_SINK_LATENCY);

        let chain = chain_hops(&stream.name, index, &source_station, &dest, nodes)?;

        // Each link attaches its upstream socket to the hop before it, which is
        // the sender for the first link and the previous chain hop after that.
        let mut hops: Vec<DesiredHop> = Vec::with_capacity(chain.bridges.len() + 1);
        let mut upstream = LinkEnd {
            station: &source_station,
            hop_id: &sender_id,
        };

        for bridge in &chain.bridges {
            let (up_socket, hop) = plan_hop(&upstream, bridge, latency, nodes, ports)?;
            push_egress(&mut sender_egresses, &mut hops, up_socket);
            hops.push(hop);
            upstream = LinkEnd {
                station: &bridge.station,
                hop_id: &bridge.id,
            };
        }

        match &chain.terminal {
            // An external listener terminates the chain: the last hop dials it
            // and no hop is placed for it.
            ChainTerminal::Remote(remote) => {
                let socket = SocketSpec::srt_connect(remote.host.clone(), remote.port, latency);
                push_egress(&mut sender_egresses, &mut hops, socket);
            }
            // The chain ends on a receiver hop, whose remaining egress is the
            // socket the consumer dials or the device the media ends on.
            ChainTerminal::Receiver(receiver) => {
                let (up_socket, mut hop) = plan_hop(&upstream, receiver, latency, nodes, ports)?;
                push_egress(&mut sender_egresses, &mut hops, up_socket);
                hop.egresses.push(match dest.terminal {
                    Terminal::Srt => {
                        let key = consumer_key(&receiver.id);
                        let port = claim_port(&hop.node_id, &key, nodes, ports)?;
                        SocketSpec::srt_listen(port, RECV_CONSUMER_LATENCY)
                    }
                    Terminal::Device => device_socket(&hop.node_id, DeviceKind::Display, nodes)?,
                });
                hops.push(hop);
            }
        }

        downstream.extend(hops);
    }

    let sender = DesiredHop {
        id: sender_id.clone(),
        node_id: sender_node.to_string(),
        role: HopRole::Sender,
        ingress: source_socket(&source, sender_node, &sender_id, nodes, ports)?,
        egresses: sender_egresses,
    };

    let mut hops = Vec::with_capacity(1 + downstream.len());
    hops.push(sender);
    hops.extend(downstream);

    Ok(Path {
        stream: stream.name.clone(),
        enabled: stream.enabled,
        hops,
    })
}

/// Plan the link that feeds `hop` and place it: the socket the upstream end of
/// that link owns, and the hop itself, its ingress the downstream socket.
fn plan_hop(
    upstream: &LinkEnd,
    hop: &ChainHop,
    latency: u32,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<(SocketSpec, DesiredHop), PlacementError> {
    let downstream = LinkEnd {
        station: &hop.station,
        hop_id: &hop.id,
    };
    let (up_socket, down_socket) = plan_link(upstream, &downstream, latency, nodes, ports)?;
    Ok((
        up_socket,
        DesiredHop {
            id: hop.id.clone(),
            node_id: hop.station.node_id.clone(),
            role: hop.role,
            ingress: down_socket,
            egresses: Vec::new(),
        },
    ))
}

/// Attach a link's upstream socket to the hop it leaves from: the sender when the
/// chain is still empty, otherwise the chain's last hop.
fn push_egress(
    sender_egresses: &mut Vec<SocketSpec>,
    chain: &mut [DesiredHop],
    socket: SocketSpec,
) {
    match chain.last_mut() {
        Some(hop) => hop.egresses.push(socket),
        None => sender_egresses.push(socket),
    }
}

/// One node a stream passes through, with the alias its peers address it by.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Station {
    node_id: String,
    network: Option<String>,
}

impl Station {
    /// A relay is addressed on its default alias: `via` names a node, not a
    /// network, and an auto-inserted relay was chosen for that alias too.
    fn relay(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            network: None,
        }
    }
}

/// One hop to place along a destination's chain.
struct ChainHop {
    station: Station,
    id: String,
    role: HopRole,
}

/// The hops between the sender and one destination, and how the chain ends.
struct Chain<'a> {
    bridges: Vec<ChainHop>,
    terminal: ChainTerminal<'a>,
}

/// Where a destination's chain ends.
enum ChainTerminal<'a> {
    /// A hop placed on the destination node.
    Receiver(ChainHop),
    /// An external listener the hop before it dials; no hop is placed for it.
    Remote(&'a RemoteAddr),
}

/// One end of a link: where it sits and which hop owns the socket.
struct LinkEnd<'a> {
    station: &'a Station,
    hop_id: &'a str,
}

/// Build one destination's chain: a bridge per relayed node, a relay wherever no
/// transport carries a link directly, and the terminal the last link runs into.
fn chain_hops<'a>(
    stream: &str,
    dest_index: usize,
    source: &Station,
    dest: &Endpoint<'a>,
    nodes: &[NodeDescriptor],
) -> Result<Chain<'a>, PlacementError> {
    let mut stations: Vec<Station> = Vec::with_capacity(dest.via.len());
    for station in dest.via.iter().map(|id| Station::relay(id)) {
        relay_before(&mut stations, source, &station, nodes)?;
        stations.push(station);
    }

    let terminal = match dest.placement {
        Placement::Remote(remote) => ChainTerminal::Remote(remote),
        Placement::Node(node_id) => {
            let station = Station {
                node_id: node_id.to_string(),
                network: dest.network.map(str::to_string),
            };
            relay_before(&mut stations, source, &station, nodes)?;
            ChainTerminal::Receiver(ChainHop {
                station,
                id: receiver_hop_id(stream, dest_index),
                role: HopRole::Receiver,
            })
        }
    };

    let bridges = stations
        .into_iter()
        .enumerate()
        .map(|(position, station)| ChainHop {
            station,
            id: bridge_hop_id(stream, dest_index, position),
            role: HopRole::Bridge,
        })
        .collect();

    Ok(Chain { bridges, terminal })
}

/// Extend `chain` with a relay when no transport carries the link into `next`
/// from the station before it — the source's own station when the chain is
/// still empty.
///
/// A spliced relay is compatible with both halves by construction, so each
/// resolves under the ordinary rule — the upstream reaches the relay, and the
/// relay reaches the downstream. One pass is enough; no inserted link can itself
/// need a relay.
fn relay_before(
    chain: &mut Vec<Station>,
    source: &Station,
    next: &Station,
    nodes: &[NodeDescriptor],
) -> Result<(), PlacementError> {
    let upstream = chain.last().unwrap_or(source);
    if let Err(failure) = station_link(upstream, next, nodes) {
        let relay = pick_relay(nodes, upstream, next, failure)?;
        chain.push(relay);
    }
    Ok(())
}

/// The lowest-id online relay node that can carry both halves of a link no
/// transport connects directly. Sorting keeps the choice stable across ticks, so
/// a stream does not migrate between equally eligible relays.
///
/// When none qualifies the error names why the direct link failed: two ends that
/// do share a transport but cannot dial each other read as a routing problem,
/// two that share none as a capability problem.
fn pick_relay(
    nodes: &[NodeDescriptor],
    upstream: &Station,
    downstream: &Station,
    failure: LinkFailure,
) -> Result<Station, PlacementError> {
    nodes
        .iter()
        .filter(|node| node.capabilities.relay)
        .filter(|node| node.status != NodeStatus::Offline)
        .filter(|node| node.id != upstream.node_id && node.id != downstream.node_id)
        .filter(|node| {
            node.capabilities
                .data_plane
                .get(DEFAULT_DATA_PLANE_ALIAS)
                .is_some_and(DataPlaneAddr::is_dialable)
        })
        .filter(|node| {
            let relay = Station::relay(&node.id);
            station_link(upstream, &relay, nodes).is_ok()
                && station_link(&relay, downstream, nodes).is_ok()
        })
        .min_by(|a, b| a.id.cmp(&b.id))
        .map(|node| Station::relay(&node.id))
        .ok_or_else(|| failure.into_error(&upstream.node_id, &downstream.node_id))
}

/// Which end of a link hosts the socket the other dials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Listener {
    Upstream,
    Downstream,
}

/// Why no transport connects two ends directly.
#[derive(Debug, PartialEq, Eq)]
enum LinkFailure {
    /// They share a transport in complementary roles, but the end that would
    /// have to listen cannot be dialled.
    Undialable,
    /// They offer no transport in complementary roles at all.
    NoCommonTransport,
    /// A node or alias in the link does not resolve.
    Placement(PlacementError),
}

impl LinkFailure {
    /// The placement error a failed link reports, named for the two ends.
    fn into_error(self, upstream: &str, downstream: &str) -> PlacementError {
        match self {
            Self::Undialable => PlacementError::NoRelayAvailable {
                upstream: upstream.to_string(),
                downstream: downstream.to_string(),
            },
            Self::NoCommonTransport => PlacementError::NoCommonTransport {
                upstream: upstream.to_string(),
                downstream: downstream.to_string(),
            },
            Self::Placement(error) => error,
        }
    }
}

impl From<PlacementError> for LinkFailure {
    fn from(error: PlacementError) -> Self {
        Self::Placement(error)
    }
}

/// One end of a link resolved to its node and the address it is reached at.
struct ResolvedEnd<'a> {
    node: &'a NodeDescriptor,
    addr: &'a DataPlaneAddr,
}

impl ResolvedEnd<'_> {
    fn offers(&self, transport: Transport, role: SocketRole) -> bool {
        self.node.capabilities.offers(transport, role)
    }
}

/// The transport and direction of a link between two resolved ends, or why none
/// fits.
///
/// Candidates are taken in [`TRANSPORT_PREFERENCE`] order. SRT works in either
/// direction, so the downstream listens when it can be dialled and the upstream
/// otherwise. WHIP carries media from the connecting end to the listening one,
/// so only the downstream may host it; WHEP carries media from the listening end
/// to the connecting one, so only the upstream may. In every case the end that
/// listens must offer that role and be dialable, and the other end must offer
/// the complementary role.
fn link_transport(
    upstream: &ResolvedEnd,
    downstream: &ResolvedEnd,
) -> Result<(Transport, Listener), LinkFailure> {
    let mut complementary = false;
    for transport in TRANSPORT_PREFERENCE {
        let listeners: &[Listener] = match transport {
            Transport::Srt => &[Listener::Downstream, Listener::Upstream],
            Transport::Whip => &[Listener::Downstream],
            Transport::Whep => &[Listener::Upstream],
        };
        for &listener in listeners {
            let (host, dialer) = match listener {
                Listener::Downstream => (downstream, upstream),
                Listener::Upstream => (upstream, downstream),
            };
            if !(host.offers(transport, SocketRole::Listen)
                && dialer.offers(transport, SocketRole::Connect))
            {
                continue;
            }
            complementary = true;
            if host.addr.is_dialable() {
                return Ok((transport, listener));
            }
        }
    }
    Err(if complementary {
        LinkFailure::Undialable
    } else {
        LinkFailure::NoCommonTransport
    })
}

/// [`link_transport`] between two stations, resolving both first.
fn station_link(
    upstream: &Station,
    downstream: &Station,
    nodes: &[NodeDescriptor],
) -> Result<(Transport, Listener), LinkFailure> {
    let up = resolve_station(upstream, nodes)?;
    let down = resolve_station(downstream, nodes)?;
    link_transport(&up, &down)
}

/// Plan one link's socket pair: `(upstream egress, downstream ingress)`.
///
/// The transport and which end listens come from [`link_transport`]. An SRT
/// listener claims its port on the listening node, always keyed by the
/// downstream hop id so an assignment stays stable when a link's direction is
/// the same across ticks. A WebRTC listener claims no port: its socket is
/// signalled at the base its node declares, addressed by the downstream hop id
/// so every link is a distinct endpoint.
fn plan_link(
    upstream: &LinkEnd,
    downstream: &LinkEnd,
    latency: u32,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<(SocketSpec, SocketSpec), PlacementError> {
    let up = resolve_station(upstream.station, nodes)?;
    let down = resolve_station(downstream.station, nodes)?;
    let (transport, listener) = link_transport(&up, &down).map_err(|failure| {
        failure.into_error(&upstream.station.node_id, &downstream.station.node_id)
    })?;

    let host = match listener {
        Listener::Downstream => &down,
        Listener::Upstream => &up,
    };
    let (listen, connect) = match transport.signalling() {
        None => {
            let port = ports.claim(host.node, downstream.hop_id)?;
            (
                SocketSpec::srt_listen(port, latency),
                SocketSpec::srt_connect(host.addr.host.clone(), port, latency),
            )
        }
        Some(signalling) => {
            let base = host.addr.signalling.base(signalling).ok_or_else(|| {
                PlacementError::NoSignalling {
                    node: host.node.id.clone(),
                    transport,
                }
            })?;
            (
                SocketSpec::signalling(signalling, SocketRole::Listen, base, downstream.hop_id),
                SocketSpec::signalling(signalling, SocketRole::Connect, base, downstream.hop_id),
            )
        }
    };
    Ok(match listener {
        Listener::Downstream => (connect, listen),
        Listener::Upstream => (listen, connect),
    })
}

/// The node and data-plane address a station is reached at, via its alias map.
fn resolve_station<'a>(
    station: &Station,
    nodes: &'a [NodeDescriptor],
) -> Result<ResolvedEnd<'a>, PlacementError> {
    let node =
        find_node(nodes, &station.node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
            node: station.node_id.clone(),
        })?;
    let addr = resolve_addr(node, station.network.as_deref())?;
    Ok(ResolvedEnd { node, addr })
}

/// Roll the path's hops up into one end-to-end status via observed hop conditions.
#[must_use]
pub fn path_status(path: &Path, observed: &[HopStatus]) -> PathStatus {
    let conditions: Vec<Option<HopConditions>> = path
        .hops
        .iter()
        .map(|hop| {
            observed
                .iter()
                .find(|status| status.id == hop.id)
                .map(HopStatus::conditions)
        })
        .collect();
    roll_up_path(path.enabled, &conditions)
}

fn source_node<'a>(source: &Endpoint<'a>) -> Result<&'a str, PlacementError> {
    match source.placement {
        Placement::Node(node) => Ok(node),
        Placement::Remote(_) => Err(PlacementError::RemoteSource),
    }
}

/// The sender's ingress: an SRT listener a producer dials, or the node's own
/// camera when the source is a `device`.
fn source_socket(
    source: &Endpoint,
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<SocketSpec, PlacementError> {
    match source.terminal {
        Terminal::Srt => {
            let latency = source.latency.unwrap_or(DEFAULT_SRC_LATENCY);
            let port = claim_port(node_id, hop_id, nodes, ports)?;
            Ok(SocketSpec::srt_listen(port, latency))
        }
        Terminal::Device => device_socket(node_id, DeviceKind::Capture, nodes),
    }
}

/// A socket on the node's own `kind` device, which the node must advertise.
fn device_socket(
    node_id: &str,
    kind: DeviceKind,
    nodes: &[NodeDescriptor],
) -> Result<SocketSpec, PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    if !node.capabilities.offers_device(kind) {
        return Err(PlacementError::NoDevice {
            node: node_id.to_string(),
            kind,
        });
    }
    Ok(SocketSpec::Device(kind))
}

fn claim_port(
    node_id: &str,
    key: &str,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<u16, PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    ports.claim(node, key)
}

/// The allocator key for a receiver's consumer socket — distinct from the
/// receiver's ingress key so both claim independent ports.
fn consumer_key(receiver_id: &str) -> String {
    format!("{receiver_id}-consumer")
}

/// The node's data-plane address for a manifest `network` alias, defaulting to
/// [`DEFAULT_DATA_PLANE_ALIAS`] when unset.
fn resolve_addr<'a>(
    node: &'a NodeDescriptor,
    network: Option<&str>,
) -> Result<&'a DataPlaneAddr, PlacementError> {
    let alias = network.unwrap_or(DEFAULT_DATA_PLANE_ALIAS);
    node.capabilities
        .data_plane
        .get(alias)
        .ok_or_else(|| PlacementError::UnknownAlias {
            node: node.id.clone(),
            alias: alias.to_string(),
        })
}

/// Deterministic FNV-1a offset of `key` within `range` (`0..=span`). It is only
/// the starting point for linear probing, so re-derivation stays stable across
/// ticks while the allocator still avoids collisions.
fn preferred_offset(range: PortRange, key: &str) -> u32 {
    let span = u64::from(range.span()) + 1;
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    #[allow(clippy::cast_possible_truncation)]
    {
        (hash % span) as u32
    }
}

fn find_node<'a>(nodes: &'a [NodeDescriptor], id: &str) -> Option<&'a NodeDescriptor> {
    nodes.iter().find(|node| node.id == id)
}

/// Concrete `srt://` addresses a producer and consumers use to reach a placed
/// stream, resolved against the same node data-plane aliases planning used.
///
/// A `device` end has nothing to dial, so it reads `null`: the ingress when the
/// source is a device, and the output at that destination's index otherwise.
/// Outputs keep manifest order, so index `i` is always destination `i`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamEndpoints {
    pub ingress: Option<EndpointAddr>,
    pub outputs: Vec<Option<EndpointAddr>>,
}

/// One resolved data-plane socket: the node hosting it plus its dialable address.
/// A remote (external) output carries an empty `node`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EndpointAddr {
    pub node: String,
    pub host: String,
    pub port: u16,
    pub url: String,
}

/// Resolve the concrete `srt://` addresses of a placed stream: the source node's
/// ingress socket a producer dials, and each destination's consumer socket.
///
/// Hosts follow the manifest `network` alias per endpoint; the ingress port is the
/// sender hop's listen port and each node output is its receiver's assigned
/// consumer port. A remote destination reports the external listener URL the
/// sender dials out to. A `device` end reports `None`.
///
/// # Errors
/// Returns [`PlacementError`] if a referenced node is unregistered, declares no
/// address for the requested alias, the source is remote, or the path does not
/// carry the hops and SRT listeners the stream's shape calls for.
pub fn stream_endpoints(
    stream: &StreamDefinition,
    path: &Path,
    nodes: &[NodeDescriptor],
) -> Result<StreamEndpoints, PlacementError> {
    let source = read_endpoint(&stream.source)?;
    let source_node = source_node(&source)?;
    let sender = path
        .hops
        .first()
        .ok_or_else(|| PlacementError::MissingHop {
            stream: path.stream.clone(),
            hop: sender_hop_id(&path.stream),
        })?;
    let ingress = match source.terminal {
        Terminal::Device => None,
        Terminal::Srt => {
            let port = listener_port(&sender.ingress, &sender.id)?;
            Some(endpoint_addr(source_node, source.network, port, nodes)?)
        }
    };

    let mut outputs = Vec::with_capacity(stream.destinations.len());
    for (index, dest) in stream.destinations.iter().enumerate() {
        let dest = read_endpoint(dest)?;
        match (&dest.placement, dest.terminal) {
            (Placement::Remote(remote), _) => outputs.push(Some(remote_endpoint_addr(remote))),
            (Placement::Node(_), Terminal::Device) => outputs.push(None),
            (Placement::Node(node_id), Terminal::Srt) => {
                let receiver_id = receiver_hop_id(&stream.name, index);
                let receiver = path
                    .hops
                    .iter()
                    .find(|hop| hop.id == receiver_id)
                    .ok_or_else(|| PlacementError::MissingHop {
                        stream: path.stream.clone(),
                        hop: receiver_id.clone(),
                    })?;
                let consumer =
                    receiver
                        .egresses
                        .first()
                        .ok_or_else(|| PlacementError::NoConsumerSocket {
                            hop: receiver.id.clone(),
                        })?;
                let consumer_port = listener_port(consumer, &receiver.id)?;
                outputs.push(Some(endpoint_addr(
                    node_id,
                    dest.network,
                    consumer_port,
                    nodes,
                )?));
            }
        }
    }

    Ok(StreamEndpoints { ingress, outputs })
}

fn endpoint_addr(
    node_id: &str,
    network: Option<&str>,
    port: u16,
    nodes: &[NodeDescriptor],
) -> Result<EndpointAddr, PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    let host = resolve_addr(node, network)?.host.clone();
    let url = format!("srt://{host}:{port}");
    Ok(EndpointAddr {
        node: node_id.to_string(),
        host,
        port,
        url,
    })
}

fn remote_endpoint_addr(remote: &RemoteAddr) -> EndpointAddr {
    EndpointAddr {
        node: String::new(),
        host: remote.host.clone(),
        port: remote.port,
        url: format!("srt://{}:{}", remote.host, remote.port),
    }
}

/// The port an SRT listener publishes, for the addresses producers and consumers
/// dial.
fn listener_port(spec: &SocketSpec, hop_id: &str) -> Result<u16, PlacementError> {
    match spec {
        SocketSpec::Srt(SrtSocket::Listen { port, .. }) => Ok(*port),
        other => Err(PlacementError::NotAnSrtListener {
            hop: hop_id.to_string(),
            socket: other.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use weave_core::{
        HopState, LinkCondition, NodeCapabilities, NodeEndpoint, NodeStatus, PortRange,
        Reachability, ResolvedAddr, RoleSet, Signalling, SignallingTransport, SocketRole,
        SrtEndpoint, SrtSocket, TransportOffer,
    };

    /// The SRT socket a spec carries, for tests asserting on an address.
    fn srt(spec: &SocketSpec) -> &SrtSocket {
        match spec {
            SocketSpec::Srt(socket) => socket,
            other => panic!("planned a {other} socket"),
        }
    }

    fn host(spec: &SocketSpec) -> Option<&str> {
        match srt(spec) {
            SrtSocket::Connect { host, .. } => Some(host),
            SrtSocket::Listen { .. } => None,
        }
    }

    /// The address behind an end that has one; a device end reads `None`.
    fn addr(end: &Option<EndpointAddr>) -> &EndpointAddr {
        end.as_ref().expect("a dialable endpoint")
    }

    fn node(id: &str, host: &str) -> NodeDescriptor {
        node_with_aliases(id, &[(DEFAULT_DATA_PLANE_ALIAS, host)])
    }

    fn node_with_aliases(id: &str, aliases: &[(&str, &str)]) -> NodeDescriptor {
        node_from_addrs(
            id,
            aliases
                .iter()
                .map(|(alias, host)| ((*alias).to_string(), DataPlaneAddr::dialable(*host)))
                .collect(),
        )
    }

    /// A node reachable only outbound on its default alias: it can dial peers but
    /// no peer can dial it.
    fn nat_node(id: &str, host: &str) -> NodeDescriptor {
        node_from_addrs(
            id,
            BTreeMap::from([(
                DEFAULT_DATA_PLANE_ALIAS.to_string(),
                DataPlaneAddr {
                    host: host.to_string(),
                    reachability: Reachability::OutboundOnly,
                    signalling: Signalling::default(),
                },
            )]),
        )
    }

    fn relay_node(id: &str, host: &str) -> NodeDescriptor {
        let mut node = node(id, host);
        node.capabilities.relay = true;
        node
    }

    fn node_from_addrs(id: &str, data_plane: BTreeMap<String, DataPlaneAddr>) -> NodeDescriptor {
        NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("http://{id}:8080"),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                data_plane,
                port_range: Some(PortRange {
                    start: 7000,
                    end: 7999,
                }),
                ..NodeCapabilities::default()
            },
        }
    }

    fn node_with_range(id: &str, start: u16, end: u16) -> NodeDescriptor {
        let mut node = node(id, "10.0.0.1");
        node.capabilities.port_range = Some(PortRange { start, end });
        node
    }

    fn node_ref(id: &str, latency: u32) -> SrtEndpoint {
        SrtEndpoint {
            node: Some(id.to_string()),
            remote: None,
            via: Vec::new(),
            format: None,
            accepts: None,
            network: None,
            latency: Some(latency),
        }
    }

    fn remote_dest() -> SrtEndpoint {
        SrtEndpoint {
            node: None,
            remote: Some(RemoteAddr {
                host: "198.51.100.5".to_string(),
                port: 9000,
            }),
            via: Vec::new(),
            format: None,
            accepts: None,
            network: None,
            latency: Some(800),
        }
    }

    fn contribution() -> StreamDefinition {
        StreamDefinition {
            name: "contribution".to_string(),
            enabled: true,
            source: StreamTransport::Srt(node_ref("strom-node-1", 200)),
            destinations: vec![StreamTransport::Srt(node_ref("strom-node-2", 1000))],
        }
    }

    fn nodes() -> Vec<NodeDescriptor> {
        vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
        ]
    }

    fn derive(stream: &StreamDefinition, nodes: &[NodeDescriptor]) -> Result<Path, PlacementError> {
        derive_path(stream, nodes, &[], &mut PortAllocator::new())
    }

    #[test]
    fn places_sender_and_receiver_by_node() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        assert_eq!(path.hops.len(), 2);

        let sender = &path.hops[0];
        assert_eq!(sender.role, HopRole::Sender);
        assert_eq!(sender.node_id, "strom-node-1");
        assert_eq!(srt(&sender.ingress).role(), SocketRole::Listen);
        let ingress_port = srt(&sender.ingress).port();
        assert!((7000..=7999).contains(&ingress_port));
        assert_eq!(sender.egresses.len(), 1);
        assert_eq!(srt(&sender.egresses[0]).role(), SocketRole::Connect);
        assert_eq!(host(&sender.egresses[0]), Some("172.27.0.10"));

        let receiver = &path.hops[1];
        assert_eq!(receiver.role, HopRole::Receiver);
        assert_eq!(receiver.node_id, "strom-node-2");
        let dest_port = srt(&receiver.ingress).port();
        assert_eq!(srt(&sender.egresses[0]).port(), dest_port);
        assert!((7000..=7999).contains(&srt(&receiver.egresses[0]).port()));
    }

    #[test]
    fn receiver_is_placed_on_its_declared_node() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.node = Some("strom-node-1".to_string());

        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(path.hops[1].node_id, "strom-node-1");
    }

    #[test]
    fn source_on_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        let StreamTransport::Srt(source) = &mut stream.source else {
            unreachable!("fixture endpoint is srt");
        };
        source.node = Some("ghost".to_string());

        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::NodeNotRegistered {
                node: "ghost".to_string()
            })
        );
    }

    #[test]
    fn node_ref_destination_resolves_host_and_assigns_port_in_range() {
        let stream = contribution();

        let path = derive(&stream, &nodes()).expect("derive");
        let egress = &path.hops[0].egresses[0];
        assert_eq!(host(egress), Some("172.27.0.10"));
        let port = srt(egress).port();
        assert!((7000..=7999).contains(&port), "port {port} within range");
        assert_eq!(
            srt(&path.hops[1].ingress).port(),
            port,
            "receiver listens on it"
        );
    }

    #[test]
    fn node_ref_destination_resolves_named_alias() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.network = Some("wan".to_string());

        let mut nodes = nodes();
        nodes[1] = node_with_aliases(
            "strom-node-2",
            &[("default", "172.27.0.10"), ("wan", "203.0.113.7")],
        );

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(host(&path.hops[0].egresses[0]), Some("203.0.113.7"));
    }

    #[test]
    fn node_ref_destination_with_unknown_alias_is_rejected() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.network = Some("mgmt".to_string());

        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::UnknownAlias {
                node: "strom-node-2".to_string(),
                alias: "mgmt".to_string(),
            })
        );
    }

    #[test]
    fn node_ref_destination_on_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.node = Some("strom-node-404".to_string());

        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::NodeNotRegistered {
                node: "strom-node-404".to_string()
            })
        );
    }

    #[test]
    fn node_ref_destination_without_port_range_is_rejected() {
        let stream = contribution();

        let mut node2 = node("strom-node-2", "172.27.0.10");
        node2.capabilities.port_range = None;
        let nodes = vec![node("strom-node-1", "172.26.0.10"), node2];

        assert_eq!(
            derive(&stream, &nodes),
            Err(PlacementError::NoPortRange {
                node: "strom-node-2".to_string(),
            })
        );
    }

    #[test]
    fn assigned_ports_are_deterministic() {
        let node = node("n", "10.0.0.1");
        let a = PortAllocator::new().claim(&node, "weave-x").expect("claim");
        let b = PortAllocator::new().claim(&node, "weave-x").expect("claim");
        assert_eq!(a, b, "same key alone maps to the same port");
        assert!((7000..=7999).contains(&a));

        // Distinct keys on one allocator never collide.
        let mut ports = PortAllocator::new();
        let x = ports.claim(&node, "weave-contribution-receiver-0").unwrap();
        let y = ports.claim(&node, "weave-contribution-receiver-1").unwrap();
        assert_ne!(x, y, "distinct keys claim distinct ports");

        // Whole-path derivation is stable across ticks.
        let stream = contribution();
        let first = derive(&stream, &nodes()).expect("derive");
        let second = derive(&stream, &nodes()).expect("derive");
        assert_eq!(
            srt(&first.hops[0].egresses[0]).port(),
            srt(&second.hops[0].egresses[0]).port()
        );
    }

    #[test]
    fn allocator_probes_past_a_collision_to_a_distinct_port() {
        let range = PortRange {
            start: 7000,
            end: 7001,
        };
        // Two ports means at most two preferred offsets, so colliding keys exist.
        let mut seen: HashMap<u32, String> = HashMap::new();
        let mut collision = None;
        for i in 0..1000 {
            let key = format!("weave-collide-{i}");
            let offset = preferred_offset(range, &key);
            if let Some(prev) = seen.get(&offset) {
                collision = Some((prev.clone(), key));
                break;
            }
            seen.insert(offset, key);
        }
        let (first, second) = collision.expect("two colliding keys within a 2-port range");

        let node = node_with_range("n", 7000, 7001);
        let mut ports = PortAllocator::new();
        let a = ports.claim(&node, &first).expect("claim first");
        let b = ports.claim(&node, &second).expect("claim second");
        assert_ne!(a, b, "linear probe yields a distinct port on collision");
        assert!((7000..=7001).contains(&a) && (7000..=7001).contains(&b));
    }

    #[test]
    fn allocator_errors_when_range_is_exhausted() {
        let node = node_with_range("n", 7000, 7000);
        let mut ports = PortAllocator::new();
        assert_eq!(ports.claim(&node, "a").expect("first claim"), 7000);
        assert_eq!(
            ports.claim(&node, "b"),
            Err(PlacementError::PortRangeExhausted {
                node: "n".to_string()
            })
        );
    }

    #[test]
    fn port_range_exhaustion_surfaces_as_a_placement_error() {
        let nodes = vec![
            node("strom-node-1", "172.26.0.10"),
            node_with_range("strom-node-2", 7000, 7000),
        ];
        // The receiver needs an ingress port and a consumer port, but only one
        // port exists on its node.
        assert_eq!(
            derive(&contribution(), &nodes),
            Err(PlacementError::PortRangeExhausted {
                node: "strom-node-2".to_string()
            })
        );
    }

    #[test]
    fn consumer_port_is_within_range_and_distinct_from_ingress() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        let receiver = &path.hops[1];
        let ingress = srt(&receiver.ingress).port();
        let consumer = srt(&receiver.egresses[0]).port();
        assert!((7000..=7999).contains(&consumer));
        assert_ne!(ingress, consumer, "consumer never collides with ingress");
    }

    #[test]
    fn data_plane_ip_change_on_reregistration_reconverges() {
        let stream = contribution();

        let before = vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
        ];
        let path = derive(&stream, &before).expect("derive");
        assert_eq!(host(&path.hops[0].egresses[0]), Some("172.27.0.10"));

        let after = vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.55"),
        ];
        let path = derive(&stream, &after).expect("derive");
        assert_eq!(host(&path.hops[0].egresses[0]), Some("172.27.0.55"));
    }

    #[test]
    fn sender_egress_uses_planned_delivery_when_no_resolved_ingress() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        assert_eq!(host(&path.hops[0].egresses[0]), Some("172.27.0.10"));
        assert_eq!(
            srt(&path.hops[0].egresses[0]).port(),
            srt(&path.hops[1].ingress).port()
        );
    }

    #[test]
    fn sender_egress_ignores_reported_resolved_ingress_even_for_wan() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.network = Some("wan".to_string());

        let mut nodes = nodes();
        nodes[1] = node_with_aliases(
            "strom-node-2",
            &[("default", "172.27.0.10"), ("wan", "203.0.113.7")],
        );

        // A concrete resolved_ingress reporting the default host must not rewrite
        // the wan-aliased planned delivery.
        let observed = vec![HopStatus {
            id: receiver_hop_id("contribution", 0),
            node_id: "strom-node-2".to_string(),
            state: HopState::Provisioned,
            ingress: LinkCondition::Idle,
            egress: LinkCondition::Idle,
            resolved_ingress: Some(ResolvedAddr {
                host: "172.27.0.10".to_string(),
                port: 9002,
            }),
            resolved_egress: None,
            stats: None,
        }];

        let path =
            derive_path(&stream, &nodes, &observed, &mut PortAllocator::new()).expect("derive");
        assert_eq!(
            host(&path.hops[0].egresses[0]),
            Some("203.0.113.7"),
            "keeps the wan host"
        );
        assert_eq!(
            srt(&path.hops[0].egresses[0]).port(),
            srt(&path.hops[1].ingress).port(),
            "keeps the planned port"
        );
    }

    #[test]
    fn hop_ids_are_deterministic_and_managed() {
        let a = derive(&contribution(), &nodes()).expect("derive");
        let b = derive(&contribution(), &nodes()).expect("derive");
        assert_eq!(a.hops[0].id, b.hops[0].id);
        assert_eq!(a.hops[0].id, "weave-contribution-sender");
        assert_eq!(a.hops[1].id, "weave-contribution-receiver-0");
        assert!(weave_core::is_managed_hop_id(&a.hops[0].id));
        assert!(weave_core::is_managed_hop_id(&a.hops[1].id));
    }

    #[test]
    fn missing_destination_is_an_error() {
        let mut stream = contribution();
        stream.destinations.clear();
        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::NoDestination)
        );
    }

    #[test]
    fn remote_destination_adds_egress_without_a_receiver_hop() {
        let mut stream = contribution();
        stream.destinations = vec![StreamTransport::Srt(remote_dest())];

        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(path.hops.len(), 1, "only the sender hop is placed");
        let sender = &path.hops[0];
        assert_eq!(sender.egresses.len(), 1);
        assert_eq!(srt(&sender.egresses[0]).role(), SocketRole::Connect);
        assert_eq!(host(&sender.egresses[0]), Some("198.51.100.5"));
        assert_eq!(srt(&sender.egresses[0]).port(), 9000);

        let endpoints = stream_endpoints(&stream, &path, &nodes()).expect("endpoints");
        assert_eq!(endpoints.outputs.len(), 1);
        assert_eq!(addr(&endpoints.outputs[0]).url, "srt://198.51.100.5:9000");
        assert!(addr(&endpoints.outputs[0]).node.is_empty());
    }

    #[test]
    fn mixed_node_and_remote_destinations() {
        let mut stream = contribution();
        stream.destinations = vec![
            StreamTransport::Srt(node_ref("strom-node-2", 1000)),
            StreamTransport::Srt(remote_dest()),
        ];

        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(
            path.hops.len(),
            2,
            "sender plus one receiver for the node dest"
        );
        let sender = &path.hops[0];
        assert_eq!(sender.egresses.len(), 2, "one egress per destination");
        assert_eq!(host(&sender.egresses[1]), Some("198.51.100.5"));
        assert_eq!(path.hops[1].id, receiver_hop_id("contribution", 0));

        let endpoints = stream_endpoints(&stream, &path, &nodes()).expect("endpoints");
        assert_eq!(endpoints.outputs.len(), 2);
        assert_eq!(addr(&endpoints.outputs[0]).node, "strom-node-2");
        assert_eq!(addr(&endpoints.outputs[1]).url, "srt://198.51.100.5:9000");
    }

    #[test]
    fn remote_source_is_rejected() {
        let mut stream = contribution();
        stream.source = StreamTransport::Srt(remote_dest());
        assert_eq!(derive(&stream, &nodes()), Err(PlacementError::RemoteSource));
    }

    #[test]
    fn endpoint_with_neither_node_nor_remote_is_rejected() {
        let mut stream = contribution();
        stream.destinations = vec![StreamTransport::Srt(SrtEndpoint {
            node: None,
            remote: None,
            via: Vec::new(),
            format: None,
            accepts: None,
            network: None,
            latency: None,
        })];
        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::EndpointPlacement)
        );
    }

    fn fanout() -> StreamDefinition {
        let mut stream = contribution();
        stream.name = "fanout".to_string();
        stream.destinations = vec![
            StreamTransport::Srt(node_ref("strom-node-2", 1000)),
            StreamTransport::Srt(node_ref("strom-node-1", 1000)),
        ];
        stream
    }

    #[test]
    fn fanout_builds_a_sender_teeing_to_one_receiver_per_destination() {
        let path = derive(&fanout(), &nodes()).expect("derive");
        assert_eq!(path.hops.len(), 3);

        let sender = &path.hops[0];
        assert_eq!(sender.id, "weave-fanout-sender");
        assert_eq!(sender.role, HopRole::Sender);
        assert_eq!(sender.node_id, "strom-node-1");
        assert_eq!(sender.egresses.len(), 2, "one egress per destination");
        assert_eq!(host(&sender.egresses[0]), Some("172.27.0.10"));
        assert_eq!(host(&sender.egresses[1]), Some("172.26.0.10"));

        let receiver0 = &path.hops[1];
        assert_eq!(receiver0.id, "weave-fanout-receiver-0");
        assert_eq!(receiver0.role, HopRole::Receiver);
        assert_eq!(receiver0.node_id, "strom-node-2");

        let receiver1 = &path.hops[2];
        assert_eq!(receiver1.id, "weave-fanout-receiver-1");
        assert_eq!(receiver1.role, HopRole::Receiver);
        assert_eq!(
            receiver1.node_id, "strom-node-1",
            "second destination is co-located with the source node"
        );
    }

    #[test]
    fn fanout_is_all_or_nothing_when_a_destination_is_unplaceable() {
        let mut stream = fanout();
        let StreamTransport::Srt(dest) = &mut stream.destinations[1] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.node = Some("ghost".to_string());
        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::NodeNotRegistered {
                node: "ghost".to_string()
            })
        );
    }

    // --- link direction and transit ---

    #[test]
    fn dialable_destination_keeps_the_sender_calling() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        assert_eq!(srt(&path.hops[0].egresses[0]).role(), SocketRole::Connect);
        assert_eq!(srt(&path.hops[1].ingress).role(), SocketRole::Listen);
    }

    #[test]
    fn outbound_only_destination_reverses_the_link() {
        let nodes = vec![
            node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
        ];

        let path = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(path.hops.len(), 2, "no relay is needed; the link reverses");

        let sender_egress = &path.hops[0].egresses[0];
        let receiver = &path.hops[1];
        assert_eq!(
            srt(sender_egress).role(),
            SocketRole::Listen,
            "the dialable end listens"
        );
        assert_eq!(
            srt(&receiver.ingress).role(),
            SocketRole::Connect,
            "the NAT'd end dials out"
        );
        assert_eq!(
            host(&receiver.ingress),
            Some("172.26.0.10"),
            "it dials the source node's data-plane address"
        );
        assert_eq!(srt(&receiver.ingress).port(), srt(sender_egress).port());
    }

    #[test]
    fn reversed_link_claims_its_port_on_the_listening_node() {
        // Disjoint ranges make the owning node legible from the port alone: an
        // egress in 7xxx was claimed on node-1, in 8xxx on node-2.
        let mut nat = nat_node("strom-node-2", "172.27.0.10");
        nat.capabilities.port_range = Some(PortRange {
            start: 8000,
            end: 8999,
        });
        let nodes = vec![node("strom-node-1", "172.26.0.10"), nat];

        let path = derive(&contribution(), &nodes).expect("derive");
        let sender = &path.hops[0];
        let receiver = &path.hops[1];

        let egress_port = srt(&sender.egresses[0]).port();
        assert!(
            (7000..=7999).contains(&egress_port),
            "the reversed link listens on node-1, so its port comes from node-1's range"
        );
        assert_ne!(srt(&sender.ingress).port(), srt(&sender.egresses[0]).port());
        assert!(
            (8000..=8999).contains(&srt(&receiver.egresses[0]).port()),
            "the consumer socket still belongs to the destination node"
        );
    }

    #[test]
    fn outbound_only_pair_relays_through_a_node_both_dial() {
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            relay_node("edge-relay", "198.51.100.9"),
        ];

        let path = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(path.hops.len(), 3, "sender, bridge, receiver");

        let sender = &path.hops[0];
        let bridge = &path.hops[1];
        let receiver = &path.hops[2];

        assert_eq!(bridge.role, HopRole::Bridge);
        assert_eq!(bridge.node_id, "edge-relay");
        assert_eq!(bridge.id, "weave-contribution-bridge-0-0");

        assert_eq!(
            srt(&sender.egresses[0]).role(),
            SocketRole::Connect,
            "the NAT'd source calls out"
        );
        assert_eq!(host(&sender.egresses[0]), Some("198.51.100.9"));
        assert_eq!(
            srt(&receiver.ingress).role(),
            SocketRole::Connect,
            "the NAT'd destination calls out too"
        );
        assert_eq!(host(&receiver.ingress), Some("198.51.100.9"));

        assert_eq!(
            srt(&bridge.ingress).role(),
            SocketRole::Listen,
            "the relay listens on both sides"
        );
        assert_eq!(srt(&bridge.egresses[0]).role(), SocketRole::Listen);
        assert_eq!(srt(&bridge.ingress).port(), srt(&sender.egresses[0]).port());
        assert_eq!(
            srt(&bridge.egresses[0]).port(),
            srt(&receiver.ingress).port()
        );
        assert_ne!(
            srt(&bridge.ingress).port(),
            srt(&bridge.egresses[0]).port(),
            "the relay's two sockets are distinct"
        );
    }

    #[test]
    fn outbound_only_pair_without_a_relay_is_unplaceable() {
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            // Dialable, but not offered as transit.
            node("bystander", "198.51.100.9"),
        ];

        assert_eq!(
            derive(&contribution(), &nodes),
            Err(PlacementError::NoRelayAvailable {
                upstream: "strom-node-1".to_string(),
                downstream: "strom-node-2".to_string(),
            })
        );
    }

    #[test]
    fn an_outbound_only_relay_is_never_chosen() {
        let mut unreachable_relay = nat_node("edge-relay", "198.51.100.9");
        unreachable_relay.capabilities.relay = true;
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            unreachable_relay,
        ];

        assert!(
            matches!(
                derive(&contribution(), &nodes),
                Err(PlacementError::NoRelayAvailable { .. })
            ),
            "a relay nobody can dial cannot bridge anything"
        );
    }

    #[test]
    fn relay_choice_is_the_lowest_id_and_stable_across_ticks() {
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            relay_node("relay-b", "198.51.100.20"),
            relay_node("relay-a", "198.51.100.10"),
        ];

        let first = derive(&contribution(), &nodes).expect("derive");
        let second = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(first.hops[1].node_id, "relay-a");
        assert_eq!(first, second, "re-derivation is stable");
    }

    #[test]
    fn an_offline_relay_is_passed_over_for_a_healthy_one() {
        let mut lost = relay_node("relay-a", "198.51.100.10");
        lost.status = NodeStatus::Offline;
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            lost,
            relay_node("relay-b", "198.51.100.20"),
        ];

        let path = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(path.hops.len(), 3, "sender, bridge, receiver");
        assert_eq!(path.hops[1].node_id, "relay-b");
        assert_eq!(
            host(&path.hops[0].egresses[0]),
            Some("198.51.100.20"),
            "the source calls the relay that is up"
        );
    }

    #[test]
    fn relay_choice_is_the_lowest_online_id_and_stable_across_ticks() {
        let mut lost = relay_node("relay-a", "198.51.100.10");
        lost.status = NodeStatus::Offline;
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            relay_node("relay-c", "198.51.100.30"),
            lost,
            relay_node("relay-b", "198.51.100.20"),
        ];

        let first = derive(&contribution(), &nodes).expect("derive");
        let second = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(first.hops[1].node_id, "relay-b");
        assert_eq!(first, second, "re-derivation is stable");
    }

    #[test]
    fn an_offline_relay_alone_leaves_the_pair_unplaceable() {
        let mut lost = relay_node("edge-relay", "198.51.100.9");
        lost.status = NodeStatus::Offline;
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            lost,
        ];

        assert_eq!(
            derive(&contribution(), &nodes),
            Err(PlacementError::NoRelayAvailable {
                upstream: "strom-node-1".to_string(),
                downstream: "strom-node-2".to_string(),
            })
        );
    }

    #[test]
    fn via_pins_a_bridge_on_an_otherwise_direct_link() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.via = vec!["edge-relay".to_string()];

        let mut nodes = nodes();
        nodes.push(relay_node("edge-relay", "198.51.100.9"));

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(
            path.hops.len(),
            3,
            "the pin is honoured, not optimised away"
        );
        assert_eq!(path.hops[1].node_id, "edge-relay");
        assert_eq!(path.hops[1].role, HopRole::Bridge);
        assert_eq!(
            srt(&path.hops[2].ingress).role(),
            SocketRole::Listen,
            "both ends are dialable, so the relay calls the receiver"
        );
        assert_eq!(srt(&path.hops[1].egresses[0]).role(), SocketRole::Connect);
    }

    #[test]
    fn via_pins_a_node_that_need_not_advertise_as_a_relay() {
        // `relay: true` gates automatic selection. An explicit pin is operator
        // intent and does not consult it.
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.via = vec!["transit".to_string()];

        let mut nodes = nodes();
        nodes.push(node("transit", "198.51.100.9"));

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(path.hops[1].node_id, "transit");
    }

    #[test]
    fn via_chains_multiple_relays_in_order() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.via = vec!["relay-first".to_string(), "relay-second".to_string()];

        let mut nodes = nodes();
        nodes.push(relay_node("relay-first", "198.51.100.10"));
        nodes.push(relay_node("relay-second", "198.51.100.20"));

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(path.hops.len(), 4);
        assert_eq!(path.hops[1].node_id, "relay-first");
        assert_eq!(path.hops[2].node_id, "relay-second");
        assert_eq!(path.hops[3].node_id, "strom-node-2");
        assert_eq!(path.hops[1].id, "weave-contribution-bridge-0-0");
        assert_eq!(path.hops[2].id, "weave-contribution-bridge-0-1");
        assert_eq!(
            host(&path.hops[1].egresses[0]),
            Some("198.51.100.20"),
            "each bridge dials the next"
        );
    }

    #[test]
    fn via_relays_out_to_a_remote_destination() {
        let mut stream = contribution();
        let mut dest = remote_dest();
        dest.via = vec!["edge-relay".to_string()];
        stream.destinations = vec![StreamTransport::Srt(dest)];

        let mut nodes = nodes();
        nodes.push(relay_node("edge-relay", "198.51.100.9"));

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(path.hops.len(), 2, "sender and bridge; no receiver hop");
        let bridge = &path.hops[1];
        assert_eq!(bridge.role, HopRole::Bridge);
        assert_eq!(
            host(&bridge.egresses[0]),
            Some("198.51.100.5"),
            "the bridge dials the external listener"
        );
        assert_eq!(srt(&bridge.egresses[0]).port(), 9000);

        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");
        assert_eq!(addr(&endpoints.outputs[0]).url, "srt://198.51.100.5:9000");
    }

    #[test]
    fn a_pinned_via_still_gets_a_relay_when_its_own_link_is_undialable() {
        // node-1 and the pinned transit node are both outbound-only, so the link
        // between them needs a relay of its own on top of the pin.
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.via = vec!["transit".to_string()];

        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
            nat_node("transit", "172.28.0.10"),
            relay_node("edge-relay", "198.51.100.9"),
        ];

        let path = derive(&stream, &nodes).expect("derive");
        let via_nodes: Vec<&str> = path.hops[1..].iter().map(|h| h.node_id.as_str()).collect();
        assert_eq!(via_nodes, vec!["edge-relay", "transit", "strom-node-2"]);
    }

    #[test]
    fn fanout_relays_only_the_destination_that_needs_it() {
        let mut stream = fanout();
        stream.destinations = vec![
            StreamTransport::Srt(node_ref("strom-node-2", 1000)),
            StreamTransport::Srt(node_ref("nat-node", 1000)),
        ];

        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
            nat_node("nat-node", "172.28.0.10"),
            relay_node("edge-relay", "198.51.100.9"),
        ];

        let path = derive(&stream, &nodes).expect("derive");
        let placed: Vec<(&str, &str)> = path
            .hops
            .iter()
            .map(|h| (h.id.as_str(), h.node_id.as_str()))
            .collect();
        assert_eq!(
            placed,
            vec![
                ("weave-fanout-sender", "strom-node-1"),
                ("weave-fanout-receiver-0", "strom-node-2"),
                ("weave-fanout-bridge-1-0", "edge-relay"),
                ("weave-fanout-receiver-1", "nat-node"),
            ]
        );
        assert_eq!(
            path.hops[0].egresses.len(),
            2,
            "the sender still tees once per destination"
        );
    }

    #[test]
    fn source_via_is_rejected() {
        let mut stream = contribution();
        let StreamTransport::Srt(source) = &mut stream.source else {
            unreachable!("fixture endpoint is srt");
        };
        source.via = vec!["edge-relay".to_string()];
        assert_eq!(derive(&stream, &nodes()), Err(PlacementError::SourceVia));
    }

    #[test]
    fn via_to_an_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.via = vec!["ghost".to_string()];

        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::NodeNotRegistered {
                node: "ghost".to_string()
            })
        );
    }

    #[test]
    fn bridge_hops_are_managed_and_deterministic() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.via = vec!["edge-relay".to_string()];

        let mut nodes = nodes();
        nodes.push(relay_node("edge-relay", "198.51.100.9"));

        let a = derive(&stream, &nodes).expect("derive");
        let b = derive(&stream, &nodes).expect("derive");
        assert_eq!(a, b);
        assert!(weave_core::is_managed_hop_id(&a.hops[1].id));
    }

    #[test]
    fn stream_endpoints_are_unchanged_by_an_intervening_bridge() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.via = vec!["edge-relay".to_string()];

        let mut nodes = nodes();
        nodes.push(relay_node("edge-relay", "198.51.100.9"));

        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");

        assert_eq!(addr(&endpoints.ingress).node, "strom-node-1");
        assert_eq!(
            addr(&endpoints.ingress).port,
            srt(&path.hops[0].ingress).port()
        );
        assert_eq!(endpoints.outputs.len(), 1);
        assert_eq!(
            addr(&endpoints.outputs[0]).node,
            "strom-node-2",
            "the consumer still attaches at the destination, not the relay"
        );
        let consumer_port = srt(&path.hops[2].egresses[0]).port();
        assert_eq!(addr(&endpoints.outputs[0]).port, consumer_port);
    }

    #[test]
    fn stream_endpoints_resolve_ingress_and_outputs() {
        let stream = contribution();
        let nodes = nodes();
        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");

        let ingress_port = srt(&path.hops[0].ingress).port();
        let ingress = addr(&endpoints.ingress);
        assert_eq!(ingress.node, "strom-node-1");
        assert_eq!(ingress.host, "172.26.0.10");
        assert_eq!(ingress.port, ingress_port);
        assert_eq!(ingress.url, format!("srt://172.26.0.10:{ingress_port}"));

        assert_eq!(endpoints.outputs.len(), 1);
        let output = addr(&endpoints.outputs[0]);
        let consumer_port = srt(&path.hops[1].egresses[0]).port();
        assert_eq!(output.node, "strom-node-2");
        assert_eq!(output.host, "172.27.0.10");
        assert_eq!(output.port, consumer_port);
        assert_eq!(output.url, format!("srt://172.27.0.10:{consumer_port}"));
    }

    #[test]
    fn stream_endpoints_follow_named_alias() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0] else {
            unreachable!("fixture endpoint is srt");
        };
        dest.network = Some("wan".to_string());

        let mut nodes = nodes();
        nodes[1] = node_with_aliases(
            "strom-node-2",
            &[("default", "172.27.0.10"), ("wan", "203.0.113.7")],
        );

        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");
        assert_eq!(addr(&endpoints.outputs[0]).host, "203.0.113.7");
    }

    #[test]
    fn stream_endpoints_on_unregistered_node_error() {
        let stream = contribution();
        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(
            stream_endpoints(&stream, &path, &[]),
            Err(PlacementError::NodeNotRegistered {
                node: "strom-node-1".to_string()
            })
        );
    }

    /// A web page: pushes WHIP, pulls WHEP, owns a camera and a screen, sits
    /// behind whatever network it is on, and has no ports to assign.
    fn browser_node(id: &str) -> NodeDescriptor {
        NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("browser://{id}"),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                transports: vec![
                    TransportOffer::with_roles(Transport::Whip, RoleSet::only(SocketRole::Connect)),
                    TransportOffer::with_roles(Transport::Whep, RoleSet::only(SocketRole::Connect)),
                ],
                devices: [DeviceKind::Capture, DeviceKind::Display]
                    .into_iter()
                    .collect(),
                data_plane: BTreeMap::from([(
                    DEFAULT_DATA_PLANE_ALIAS.to_string(),
                    DataPlaneAddr {
                        host: "browser".to_string(),
                        reachability: Reachability::OutboundOnly,
                        signalling: Signalling::default(),
                    },
                )]),
                port_range: None,
                ..NodeCapabilities::default()
            },
        }
    }

    /// A Strom node that also hosts WHIP ingest and WHEP playback. The bases it
    /// declares are its own to choose, so they are nothing the planner could
    /// have guessed.
    fn webrtc_node(id: &str, host: &str) -> NodeDescriptor {
        let mut node = node(id, host);
        node.capabilities.transports = vec![
            TransportOffer::new(Transport::Srt),
            TransportOffer::with_roles(Transport::Whip, RoleSet::only(SocketRole::Listen)),
            TransportOffer::with_roles(Transport::Whep, RoleSet::only(SocketRole::Listen)),
        ];
        *signalling_of(&mut node) = Signalling {
            whip: Some(format!("http://{host}:8080/ingest")),
            whep: Some(format!("http://{host}:8080/playback")),
        };
        node
    }

    fn signalling_of(node: &mut NodeDescriptor) -> &mut Signalling {
        &mut node
            .capabilities
            .data_plane
            .get_mut(DEFAULT_DATA_PLANE_ALIAS)
            .expect("the default alias")
            .signalling
    }

    fn device_ref(id: &str) -> StreamTransport {
        StreamTransport::Device(NodeEndpoint {
            node: id.to_string(),
            network: None,
        })
    }

    fn alice_cam() -> StreamDefinition {
        StreamDefinition {
            name: "alice-cam".to_string(),
            enabled: true,
            source: device_ref("browser-a1b2"),
            destinations: vec![StreamTransport::Srt(node_ref("strom-node-2", 1000))],
        }
    }

    fn alice_return() -> StreamDefinition {
        StreamDefinition {
            name: "alice-return".to_string(),
            enabled: true,
            source: StreamTransport::Srt(node_ref("strom-node-2", 200)),
            destinations: vec![device_ref("browser-a1b2")],
        }
    }

    fn webrtc_nodes() -> Vec<NodeDescriptor> {
        vec![
            browser_node("browser-a1b2"),
            webrtc_node("strom-node-2", "172.27.0.10"),
        ]
    }

    #[test]
    fn device_sender_to_strom_is_whip_hosted_on_strom() {
        let path = derive(&alice_cam(), &webrtc_nodes()).expect("derive");
        assert_eq!(path.hops.len(), 2);
        let signalled = SocketSpec::signalling(
            SignallingTransport::Whip,
            SocketRole::Listen,
            "http://172.27.0.10:8080/ingest",
            "weave-alice-cam-receiver-0",
        );

        let sender = &path.hops[0];
        assert_eq!(sender.node_id, "browser-a1b2");
        assert_eq!(
            sender.ingress,
            SocketSpec::Device(DeviceKind::Capture),
            "the media starts at the camera"
        );
        assert_eq!(
            sender.egresses,
            vec![SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Connect,
                "http://172.27.0.10:8080/ingest",
                "weave-alice-cam-receiver-0",
            )]
        );

        let receiver = &path.hops[1];
        assert_eq!(receiver.node_id, "strom-node-2");
        assert_eq!(receiver.ingress, signalled, "Strom hosts the ingest");
        assert_eq!(srt(&receiver.egresses[0]).role(), SocketRole::Listen);
        assert!((7000..=7999).contains(&srt(&receiver.egresses[0]).port()));

        let endpoints = stream_endpoints(&alice_cam(), &path, &webrtc_nodes()).expect("endpoints");
        assert_eq!(endpoints.ingress, None, "a camera has nothing to dial");
        assert_eq!(endpoints.outputs.len(), 1);
        assert_eq!(addr(&endpoints.outputs[0]).node, "strom-node-2");
    }

    #[test]
    fn strom_to_device_receiver_is_whep_hosted_on_strom() {
        let path = derive(&alice_return(), &webrtc_nodes()).expect("derive");
        assert_eq!(path.hops.len(), 2);
        let signalled = SocketSpec::signalling(
            SignallingTransport::Whep,
            SocketRole::Listen,
            "http://172.27.0.10:8080/playback",
            "weave-alice-return-receiver-0",
        );

        let sender = &path.hops[0];
        assert_eq!(sender.node_id, "strom-node-2");
        assert_eq!(srt(&sender.ingress).role(), SocketRole::Listen);
        assert_eq!(
            sender.egresses,
            vec![signalled],
            "Strom hosts the playback the browser pulls"
        );

        let receiver = &path.hops[1];
        assert_eq!(receiver.node_id, "browser-a1b2");
        assert_eq!(
            receiver.ingress,
            SocketSpec::signalling(
                SignallingTransport::Whep,
                SocketRole::Connect,
                "http://172.27.0.10:8080/playback",
                "weave-alice-return-receiver-0",
            )
        );
        assert_eq!(
            receiver.egresses,
            vec![SocketSpec::Device(DeviceKind::Display)],
            "the media ends on the screen"
        );

        let endpoints =
            stream_endpoints(&alice_return(), &path, &webrtc_nodes()).expect("endpoints");
        assert_eq!(addr(&endpoints.ingress).node, "strom-node-2");
        assert_eq!(
            endpoints.outputs,
            vec![None],
            "a screen has nothing to dial"
        );
    }

    #[test]
    fn a_hosting_nodes_declared_base_takes_the_hop_id_and_nothing_else() {
        let mut strom = webrtc_node("strom-node-2", "172.27.0.10");
        signalling_of(&mut strom).whip = Some("https://edge.example/sessions/".to_string());
        let nodes = vec![browser_node("browser-a1b2"), strom];

        let path = derive(&alice_cam(), &nodes).expect("derive");
        assert_eq!(
            path.hops[1].ingress,
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Listen,
                "https://edge.example/sessions",
                "weave-alice-cam-receiver-0",
            )
        );
    }

    #[test]
    fn hosting_webrtc_without_a_signalling_base_is_rejected() {
        let mut strom = webrtc_node("strom-node-2", "172.27.0.10");
        *signalling_of(&mut strom) = Signalling::default();
        let nodes = vec![browser_node("browser-a1b2"), strom];
        assert_eq!(
            derive(&alice_cam(), &nodes),
            Err(PlacementError::NoSignalling {
                node: "strom-node-2".to_string(),
                transport: Transport::Whip,
            })
        );
    }

    #[test]
    fn a_base_for_one_webrtc_transport_does_not_serve_the_other() {
        let mut strom = webrtc_node("strom-node-2", "172.27.0.10");
        signalling_of(&mut strom).whep = None;
        let nodes = vec![browser_node("browser-a1b2"), strom];

        derive(&alice_cam(), &nodes).expect("the whip base still hosts the camera's link");
        assert_eq!(
            derive(&alice_return(), &nodes),
            Err(PlacementError::NoSignalling {
                node: "strom-node-2".to_string(),
                transport: Transport::Whep,
            })
        );
    }

    #[test]
    fn endpoints_serialize_device_ends_as_null() {
        let nodes = webrtc_nodes();
        let cam = derive(&alice_cam(), &nodes).expect("derive");
        let value =
            serde_json::to_value(stream_endpoints(&alice_cam(), &cam, &nodes).unwrap()).unwrap();
        assert!(value["ingress"].is_null());
        assert_eq!(value["outputs"][0]["node"], "strom-node-2");
        assert!(
            value["outputs"][0]["url"]
                .as_str()
                .unwrap()
                .starts_with("srt://172.27.0.10:")
        );

        let ret = derive(&alice_return(), &nodes).expect("derive");
        let value =
            serde_json::to_value(stream_endpoints(&alice_return(), &ret, &nodes).unwrap()).unwrap();
        assert_eq!(value["ingress"]["node"], "strom-node-2");
        assert_eq!(value["outputs"], serde_json::json!([null]));
    }

    #[test]
    fn srt_only_endpoints_json_shape_is_unchanged() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        let value =
            serde_json::to_value(stream_endpoints(&contribution(), &path, &nodes()).unwrap())
                .unwrap();
        let ingress = value["ingress"].as_object().unwrap();
        let mut keys: Vec<&str> = ingress.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["host", "node", "port", "url"]);
        assert_eq!(value["outputs"].as_array().unwrap().len(), 1);
        assert!(value["outputs"][0].is_object());
    }

    #[test]
    fn srt_is_preferred_when_both_ends_offer_it() {
        let pair = vec![
            webrtc_node("strom-node-1", "172.26.0.10"),
            webrtc_node("strom-node-2", "172.27.0.10"),
        ];
        let path = derive(&contribution(), &pair).expect("derive");
        assert!(matches!(path.hops[0].egresses[0], SocketSpec::Srt(_)));
        assert!(matches!(path.hops[1].ingress, SocketSpec::Srt(_)));
        assert_eq!(
            path,
            derive(&contribution(), &nodes()).expect("derive"),
            "declaring srt explicitly plans the same as declaring nothing"
        );
    }

    #[test]
    fn two_browsers_without_a_relay_share_no_transport() {
        let mut stream = alice_cam();
        stream.destinations = vec![device_ref("browser-c3d4")];
        let nodes = vec![browser_node("browser-a1b2"), browser_node("browser-c3d4")];

        assert_eq!(
            derive(&stream, &nodes),
            Err(PlacementError::NoCommonTransport {
                upstream: "browser-a1b2".to_string(),
                downstream: "browser-c3d4".to_string(),
            })
        );
    }

    #[test]
    fn two_browsers_bridge_through_a_strom_relay() {
        let mut stream = alice_cam();
        stream.destinations = vec![device_ref("browser-c3d4")];
        let mut relay = webrtc_node("strom-node-1", "172.26.0.10");
        relay.capabilities.relay = true;
        let nodes = vec![
            browser_node("browser-a1b2"),
            browser_node("browser-c3d4"),
            relay,
        ];

        let path = derive(&stream, &nodes).expect("derive");
        let placed: Vec<(&str, &str)> = path
            .hops
            .iter()
            .map(|h| (h.id.as_str(), h.node_id.as_str()))
            .collect();
        assert_eq!(
            placed,
            vec![
                ("weave-alice-cam-sender", "browser-a1b2"),
                ("weave-alice-cam-bridge-0-0", "strom-node-1"),
                ("weave-alice-cam-receiver-0", "browser-c3d4"),
            ]
        );
        let bridge = &path.hops[1];
        assert_eq!(
            bridge.ingress,
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Listen,
                "http://172.26.0.10:8080/ingest",
                "weave-alice-cam-bridge-0-0",
            )
        );
        assert_eq!(
            bridge.egresses,
            vec![SocketSpec::signalling(
                SignallingTransport::Whep,
                SocketRole::Listen,
                "http://172.26.0.10:8080/playback",
                "weave-alice-cam-receiver-0",
            )]
        );
        assert_eq!(
            path.hops[0].egresses,
            vec![SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Connect,
                "http://172.26.0.10:8080/ingest",
                "weave-alice-cam-bridge-0-0",
            )],
            "the camera pushes into the relay's ingest"
        );
        assert_eq!(
            path.hops[2].ingress,
            SocketSpec::signalling(
                SignallingTransport::Whep,
                SocketRole::Connect,
                "http://172.26.0.10:8080/playback",
                "weave-alice-cam-receiver-0",
            ),
            "the far browser pulls it back out"
        );
        assert_eq!(
            path.hops[2].egresses,
            vec![SocketSpec::Device(DeviceKind::Display)]
        );
    }

    #[test]
    fn a_relay_that_cannot_carry_both_halves_is_passed_over() {
        let mut stream = alice_cam();
        stream.destinations = vec![device_ref("browser-c3d4")];
        // SRT-only relay: dialable and offered, but a browser speaks no SRT.
        let nodes = vec![
            browser_node("browser-a1b2"),
            browser_node("browser-c3d4"),
            relay_node("srt-relay", "198.51.100.9"),
        ];
        assert!(matches!(
            derive(&stream, &nodes),
            Err(PlacementError::NoCommonTransport { .. })
        ));
    }

    #[test]
    fn a_device_endpoint_needs_a_node_that_offers_one() {
        let mut stream = alice_cam();
        let StreamTransport::Device(source) = &mut stream.source else {
            unreachable!()
        };
        source.node = "strom-node-2".to_string();
        assert_eq!(
            derive(&stream, &webrtc_nodes()),
            Err(PlacementError::NoDevice {
                node: "strom-node-2".to_string(),
                kind: DeviceKind::Capture,
            })
        );

        let mut stream = alice_return();
        stream.destinations = vec![device_ref("strom-node-2")];
        assert_eq!(
            derive(&stream, &webrtc_nodes()),
            Err(PlacementError::NoDevice {
                node: "strom-node-2".to_string(),
                kind: DeviceKind::Display,
            })
        );
    }

    #[test]
    fn a_strom_node_that_only_speaks_srt_is_never_handed_a_webrtc_socket() {
        // node-2 declares srt only; the browser cannot reach it, and there is
        // no relay, so the stream stays unplaced rather than misplanned.
        let mut srt_only = node("strom-node-2", "172.27.0.10");
        srt_only.capabilities.transports = vec![TransportOffer::new(Transport::Srt)];
        let nodes = vec![browser_node("browser-a1b2"), srt_only];
        assert!(matches!(
            derive(&alice_cam(), &nodes),
            Err(PlacementError::NoCommonTransport { .. })
        ));
    }

    #[test]
    fn webrtc_plans_are_deterministic() {
        let a = derive(&alice_cam(), &webrtc_nodes()).expect("derive");
        let b = derive(&alice_cam(), &webrtc_nodes()).expect("derive");
        assert_eq!(a, b);
    }

    #[test]
    fn endpoints_name_a_hop_the_path_is_missing() {
        let stream = contribution();
        let mut path = derive(&stream, &nodes()).expect("derive");
        path.hops.retain(|hop| hop.role != HopRole::Receiver);
        assert_eq!(
            stream_endpoints(&stream, &path, &nodes()),
            Err(PlacementError::MissingHop {
                stream: "contribution".to_string(),
                hop: "weave-contribution-receiver-0".to_string(),
            })
        );
    }

    #[test]
    fn endpoints_name_a_receiver_with_no_consumer_socket() {
        let stream = contribution();
        let mut path = derive(&stream, &nodes()).expect("derive");
        path.hops[1].egresses.clear();
        assert_eq!(
            stream_endpoints(&stream, &path, &nodes()),
            Err(PlacementError::NoConsumerSocket {
                hop: "weave-contribution-receiver-0".to_string(),
            })
        );
    }

    #[test]
    fn endpoints_name_a_consumer_socket_that_is_not_an_srt_listener() {
        let stream = contribution();
        let mut path = derive(&stream, &nodes()).expect("derive");
        path.hops[1].egresses[0] = SocketSpec::Device(DeviceKind::Display);
        let error = stream_endpoints(&stream, &path, &nodes()).unwrap_err();
        assert_eq!(
            error,
            PlacementError::NotAnSrtListener {
                hop: "weave-contribution-receiver-0".to_string(),
                socket: SocketSpec::Device(DeviceKind::Display),
            }
        );
        assert_eq!(
            error.to_string(),
            "hop weave-contribution-receiver-0 carries a display device socket where an SRT listener is needed"
        );

        path.hops[1].egresses[0] = SocketSpec::srt_connect("198.51.100.5", 9000, 200);
        let error = stream_endpoints(&stream, &path, &nodes()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "hop weave-contribution-receiver-0 carries a srt connect socket where an SRT listener is needed",
            "names the caller, not just the transport"
        );
    }
}
