//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use std::collections::{HashMap, HashSet};

use weave_core::{
    DesiredEgress, DesiredHop, DestinationEndpoint, DeviceKind, EndpointAddr, HOP_ID_PREFIX,
    HopConditions, HopRole, HopStatus, NetworkAttachment, NodeDescriptor, NodeStatus, Path,
    PathStatus, PortRange, RemoteAddr, SocketRole, SocketSpec, SrtSocket, StreamDefinition,
    StreamEndpoints, StreamTransport, Transport, roll_up_path,
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
    #[error("node {node} has no attachment to network {network}")]
    UnknownNetwork { node: String, network: String },
    #[error("node {node} cannot dial network {network}")]
    CannotDialNetwork { node: String, network: String },
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
    #[error("node {node} has no hop profile for {ingress} to {egress} with {egresses} egress(es)")]
    NoHopProfile {
        node: String,
        ingress: String,
        egress: String,
        egresses: usize,
    },
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
pub fn receiver_hop_id(stream: &str, destination: &str) -> String {
    format!("{HOP_ID_PREFIX}{stream}-receiver-{destination}")
}

/// Id of the bridge at `position` along the chain carrying destination `dest`.
/// Position is counted after relay insertion, so an auto-inserted relay and a
/// pinned one are named the same way.
#[must_use]
pub fn bridge_hop_id(stream: &str, destination: &str, position: usize) -> String {
    format!("{HOP_ID_PREFIX}{stream}-bridge-{destination}-{position}")
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
#[derive(Debug, Clone, Default)]
pub struct PortAllocator {
    used: HashMap<String, HashSet<u16>>,
}

impl PortAllocator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn claim(
        &mut self,
        node: &NodeDescriptor,
        attachment: &NetworkAttachment,
        key: &str,
    ) -> Result<u16, PlacementError> {
        let range = attachment
            .listeners
            .srt
            .as_ref()
            .map(|listener| listener.port_range)
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
/// destination relays through. Attachments resolve at planning time from shared
/// networks, and every port is claimed from `ports`, the
/// per-tick collision-aware allocator. A hop's reported ingress address is
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
    committed_ports: &mut PortAllocator,
) -> Result<Path, PlacementError> {
    let mut ports = committed_ports.clone();
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

    let mut destinations: Vec<_> = stream.destinations.iter().collect();
    destinations.sort_by(|left, right| left.id.cmp(&right.id));
    for destination in destinations {
        let branch_id = destination.id.clone();
        let dest = read_endpoint(&destination.endpoint)?;
        let latency = dest.latency.unwrap_or(DEFAULT_SINK_LATENCY);

        let chain = chain_hops(&stream.name, &destination.id, &source_station, &dest, nodes)?;

        // Each link attaches its upstream socket to the hop before it, which is
        // the sender for the first link and the previous chain hop after that.
        let mut hops: Vec<DesiredHop> = Vec::with_capacity(chain.bridges.len() + 1);
        let mut upstream = LinkEnd {
            station: &source_station,
            hop_id: &sender_id,
        };

        for bridge in &chain.bridges {
            let (up_socket, hop) = plan_hop(&upstream, bridge, latency, nodes, &mut ports)?;
            push_egress(&mut sender_egresses, &mut hops, &branch_id, up_socket);
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
                if !can_dial_network(&upstream.station.node_id, &remote.network, nodes)? {
                    let relay = pick_remote_relay(upstream.station, &remote.network, nodes)?;
                    let bridge = ChainHop {
                        station: Station::relay(&relay),
                        id: bridge_hop_id(&stream.name, &destination.id, chain.bridges.len()),
                        role: HopRole::Bridge,
                    };
                    let (up_socket, hop) =
                        plan_hop(&upstream, &bridge, latency, nodes, &mut ports)?;
                    push_egress(&mut sender_egresses, &mut hops, &branch_id, up_socket);
                    hops.push(hop);
                }
                let socket = SocketSpec::srt_connect(remote.host.clone(), remote.port, latency);
                push_egress(&mut sender_egresses, &mut hops, &branch_id, socket);
            }
            // The chain ends on a receiver hop, whose remaining egress is the
            // socket the consumer dials or the device the media ends on.
            ChainTerminal::Receiver(receiver) => {
                let (up_socket, mut hop) =
                    plan_hop(&upstream, receiver, latency, nodes, &mut ports)?;
                push_egress(&mut sender_egresses, &mut hops, &branch_id, up_socket);
                let socket = match dest.terminal {
                    Terminal::Srt => {
                        let key = consumer_key(&receiver.id);
                        let port = claim_port(&hop.node_id, dest.network, &key, nodes, &mut ports)?;
                        SocketSpec::srt_listen(port, RECV_CONSUMER_LATENCY)
                    }
                    Terminal::Device => device_socket(&hop.node_id, DeviceKind::Display, nodes)?,
                };
                hop.egresses.push(DesiredEgress {
                    branch_id: branch_id.clone(),
                    socket,
                });
                hops.push(hop);
            }
        }

        downstream.extend(hops);
    }

    let sender = DesiredHop {
        id: sender_id.clone(),
        node_id: sender_node.to_string(),
        profile_id: String::new(),
        role: HopRole::Sender,
        ingress: source_socket(&source, sender_node, &sender_id, nodes, &mut ports)?,
        egresses: sender_egresses,
    };

    let mut hops = Vec::with_capacity(1 + downstream.len());
    hops.push(sender);
    hops.extend(downstream);

    for hop in &mut hops {
        hop.profile_id = select_profile(hop, nodes)?;
    }
    *committed_ports = ports;

    Ok(Path {
        stream: stream.name.clone(),
        enabled: stream.enabled,
        hops,
    })
}

fn can_dial_network(
    node_id: &str,
    network: &str,
    nodes: &[NodeDescriptor],
) -> Result<bool, PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    Ok(node
        .topology
        .attachments
        .iter()
        .any(|attachment| attachment.network == network && attachment.dial))
}

fn pick_remote_relay(
    upstream: &Station,
    network: &str,
    nodes: &[NodeDescriptor],
) -> Result<String, PlacementError> {
    nodes
        .iter()
        .filter(|node| node.status != NodeStatus::Offline && node.id != upstream.node_id)
        .filter(|node| {
            node.topology
                .attachments
                .iter()
                .any(|attachment| attachment.network == network && attachment.dial)
        })
        .filter(|node| {
            let relay = Station::relay(&node.id);
            let Ok(link) = station_link(upstream, &relay, nodes) else {
                return false;
            };
            let ingress_role = role_for_end(link.listener, Listener::Downstream);
            node.capabilities.hop_profiles.iter().any(|profile| {
                profile
                    .ingress
                    .offers_transport(link.transport, ingress_role)
                    && profile
                        .egress
                        .offers_transport(Transport::Srt, SocketRole::Connect)
                    && profile.max_egresses.is_none_or(|maximum| maximum >= 1)
            })
        })
        .min_by(|left, right| left.id.cmp(&right.id))
        .map(|node| node.id.clone())
        .ok_or_else(|| PlacementError::CannotDialNetwork {
            node: upstream.node_id.clone(),
            network: network.to_string(),
        })
}

fn select_profile(hop: &DesiredHop, nodes: &[NodeDescriptor]) -> Result<String, PlacementError> {
    let node = find_node(nodes, &hop.node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: hop.node_id.clone(),
    })?;
    let mut profiles: Vec<_> = node.capabilities.hop_profiles.iter().collect();
    profiles.sort_by(|left, right| left.id.cmp(&right.id));
    if let Some(profile) = profiles.into_iter().find(|profile| {
        profile.ingress.matches_socket(&hop.ingress)
            && hop
                .egresses
                .iter()
                .all(|egress| profile.egress.matches_socket(&egress.socket))
            && profile
                .max_egresses
                .is_none_or(|maximum| hop.egresses.len() <= maximum)
    }) {
        return Ok(profile.id.clone());
    }
    Err(PlacementError::NoHopProfile {
        node: node.id.clone(),
        ingress: hop.ingress.to_string(),
        egress: hop
            .egresses
            .first()
            .map_or_else(|| "none".to_string(), |egress| egress.socket.to_string()),
        egresses: hop.egresses.len(),
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
            profile_id: String::new(),
            role: hop.role,
            ingress: down_socket,
            egresses: Vec::new(),
        },
    ))
}

/// Attach a link's upstream socket to the hop it leaves from: the sender when the
/// chain is still empty, otherwise the chain's last hop.
fn push_egress(
    sender_egresses: &mut Vec<DesiredEgress>,
    chain: &mut [DesiredHop],
    branch_id: &str,
    socket: SocketSpec,
) {
    let egress = DesiredEgress {
        branch_id: branch_id.to_string(),
        socket,
    };
    match chain.last_mut() {
        Some(hop) => hop.egresses.push(egress),
        None => sender_egresses.push(egress),
    }
}

/// One node a stream passes through, optionally constrained to one network.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Station {
    node_id: String,
    network: Option<String>,
}

impl Station {
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
    destination_id: &str,
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
                id: receiver_hop_id(stream, destination_id),
                role: HopRole::Receiver,
            })
        }
    };

    let bridges = stations
        .into_iter()
        .enumerate()
        .map(|(position, station)| ChainHop {
            station,
            id: bridge_hop_id(stream, destination_id, position),
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
        .filter(|node| node.status != NodeStatus::Offline)
        .filter(|node| node.id != upstream.node_id && node.id != downstream.node_id)
        .filter(|node| {
            let relay = Station::relay(&node.id);
            let Ok(ingress) = station_link(upstream, &relay, nodes) else {
                return false;
            };
            let Ok(egress) = station_link(&relay, downstream, nodes) else {
                return false;
            };
            let ingress_role = role_for_end(ingress.listener, Listener::Downstream);
            let egress_role = role_for_end(egress.listener, Listener::Upstream);
            node.capabilities.hop_profiles.iter().any(|profile| {
                profile
                    .ingress
                    .offers_transport(ingress.transport, ingress_role)
                    && profile
                        .egress
                        .offers_transport(egress.transport, egress_role)
                    && profile.max_egresses.is_none_or(|max| max >= 1)
            })
        })
        .min_by(|a, b| a.id.cmp(&b.id))
        .map(|node| Station::relay(&node.id))
        .ok_or_else(|| failure.into_error(&upstream.node_id, &downstream.node_id))
}

fn role_for_end(listener: Listener, end: Listener) -> SocketRole {
    if listener == end {
        SocketRole::Listen
    } else {
        SocketRole::Connect
    }
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
    /// A node or network constraint in the link does not resolve.
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

struct ResolvedEnd<'a> {
    node: &'a NodeDescriptor,
    station: &'a Station,
}

impl ResolvedEnd<'_> {
    fn offers_ingress(&self, transport: Transport, role: SocketRole) -> bool {
        self.node.capabilities.offers_ingress(transport, role)
    }

    fn offers_egress(&self, transport: Transport, role: SocketRole) -> bool {
        self.node.capabilities.offers_egress(transport, role)
    }

    fn attachments(&self) -> Vec<&NetworkAttachment> {
        let mut attachments: Vec<_> = self
            .node
            .topology
            .attachments
            .iter()
            .filter(|attachment| {
                self.station
                    .network
                    .as_deref()
                    .is_none_or(|network| attachment.network == network)
            })
            .collect();
        attachments
            .sort_by(|left, right| (&left.network, &left.id).cmp(&(&right.network, &right.id)));
        attachments
    }
}

#[derive(Clone)]
struct LinkChoice {
    transport: Transport,
    listener: Listener,
    attachment: NetworkAttachment,
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
    upstream: &ResolvedEnd<'_>,
    downstream: &ResolvedEnd<'_>,
) -> Result<LinkChoice, LinkFailure> {
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
            let complementary_roles = match listener {
                Listener::Downstream => {
                    downstream.offers_ingress(transport, SocketRole::Listen)
                        && upstream.offers_egress(transport, SocketRole::Connect)
                }
                Listener::Upstream => {
                    upstream.offers_egress(transport, SocketRole::Listen)
                        && downstream.offers_ingress(transport, SocketRole::Connect)
                }
            };
            if !complementary_roles {
                continue;
            }
            complementary = true;
            for attachment in host.attachments() {
                if !has_listener(attachment, transport) {
                    continue;
                }
                if dialer
                    .attachments()
                    .iter()
                    .any(|candidate| candidate.network == attachment.network && candidate.dial)
                {
                    return Ok(LinkChoice {
                        transport,
                        listener,
                        attachment: attachment.clone(),
                    });
                }
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
) -> Result<LinkChoice, LinkFailure> {
    let up = resolve_station(upstream, nodes)?;
    let down = resolve_station(downstream, nodes)?;
    link_transport(&up, &down)
}

fn has_listener(attachment: &NetworkAttachment, transport: Transport) -> bool {
    match transport {
        Transport::Srt => attachment.listeners.srt.is_some(),
        Transport::Whip => attachment.listeners.whip.is_some(),
        Transport::Whep => attachment.listeners.whep.is_some(),
    }
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
    let choice = link_transport(&up, &down).map_err(|failure| {
        failure.into_error(&upstream.station.node_id, &downstream.station.node_id)
    })?;

    let host = match choice.listener {
        Listener::Downstream => &down,
        Listener::Upstream => &up,
    };
    let (listen, connect) = match choice.transport.signalling() {
        None => {
            let listener = choice
                .attachment
                .listeners
                .srt
                .as_ref()
                .expect("SRT link choice has an SRT listener");
            let port = ports.claim(host.node, &choice.attachment, downstream.hop_id)?;
            (
                SocketSpec::srt_listen(port, latency),
                SocketSpec::srt_connect(listener.host.clone(), port, latency),
            )
        }
        Some(signalling) => {
            let base = choice
                .attachment
                .listeners
                .signalling(signalling)
                .ok_or_else(|| PlacementError::NoSignalling {
                    node: host.node.id.clone(),
                    transport: choice.transport,
                })?;
            (
                SocketSpec::signalling(signalling, SocketRole::Listen, base, downstream.hop_id),
                SocketSpec::signalling(signalling, SocketRole::Connect, base, downstream.hop_id),
            )
        }
    };
    Ok(match choice.listener {
        Listener::Downstream => (connect, listen),
        Listener::Upstream => (listen, connect),
    })
}

/// Resolve a station's node and optional network constraint.
fn resolve_station<'a>(
    station: &'a Station,
    nodes: &'a [NodeDescriptor],
) -> Result<ResolvedEnd<'a>, PlacementError> {
    let node =
        find_node(nodes, &station.node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
            node: station.node_id.clone(),
        })?;
    if let Some(network) = &station.network
        && !node
            .topology
            .attachments
            .iter()
            .any(|attachment| attachment.network == *network)
    {
        return Err(PlacementError::UnknownNetwork {
            node: node.id.clone(),
            network: network.clone(),
        });
    }
    Ok(ResolvedEnd { node, station })
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
                .find(|status| status.id == hop.id && status.node_id == hop.node_id)
                .and_then(|status| status.conditions(hop))
        })
        .collect();
    roll_up_path(path.enabled, &conditions)
}

#[must_use]
pub fn destination_path_status(
    path: &Path,
    destination_id: &str,
    observed: &[HopStatus],
) -> PathStatus {
    let hops: Vec<Option<HopConditions>> = path
        .hops
        .iter()
        .filter_map(|hop| {
            let egresses: Vec<_> = hop
                .egresses
                .iter()
                .filter(|egress| egress.branch_id == destination_id)
                .cloned()
                .collect();
            if egresses.is_empty() {
                return None;
            }
            let mut branch = hop.clone();
            branch.egresses = egresses;
            Some(
                observed
                    .iter()
                    .find(|status| status.id == branch.id && status.node_id == branch.node_id)
                    .and_then(|status| {
                        let mut branch_status = status.clone();
                        branch_status
                            .egresses
                            .retain(|egress| egress.branch_id == destination_id);
                        branch_status.conditions(&branch)
                    }),
            )
        })
        .collect();
    roll_up_path(path.enabled, &hops)
}

#[must_use]
pub fn destination_nodes(path: &Path, destination_id: &str) -> Vec<String> {
    let mut nodes = Vec::new();
    for hop in &path.hops {
        if hop
            .egresses
            .iter()
            .any(|egress| egress.branch_id == destination_id)
            && !nodes.contains(&hop.node_id)
        {
            nodes.push(hop.node_id.clone());
        }
    }
    nodes
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
            let port = claim_port(node_id, source.network, hop_id, nodes, ports)?;
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
    let supported = match kind {
        DeviceKind::Capture => node.capabilities.offers_ingress_device(kind),
        DeviceKind::Display => node.capabilities.offers_egress_device(kind),
    };
    if !supported {
        return Err(PlacementError::NoDevice {
            node: node_id.to_string(),
            kind,
        });
    }
    Ok(SocketSpec::Device(kind))
}

fn claim_port(
    node_id: &str,
    network: Option<&str>,
    key: &str,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<u16, PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    let attachment = srt_listener_attachment(node, network)?;
    ports.claim(node, attachment, key)
}

/// The allocator key for a receiver's consumer socket — distinct from the
/// receiver's ingress key so both claim independent ports.
fn consumer_key(receiver_id: &str) -> String {
    format!("{receiver_id}-consumer")
}

fn srt_listener_attachment<'a>(
    node: &'a NodeDescriptor,
    network: Option<&str>,
) -> Result<&'a NetworkAttachment, PlacementError> {
    let mut candidates: Vec<_> = node
        .topology
        .attachments
        .iter()
        .filter(|attachment| network.is_none_or(|network| attachment.network == network))
        .filter(|attachment| attachment.listeners.srt.is_some())
        .collect();
    candidates.sort_by(|left, right| (&left.id, &left.network).cmp(&(&right.id, &right.network)));
    candidates.into_iter().next().ok_or_else(|| {
        network.map_or_else(
            || PlacementError::NoPortRange {
                node: node.id.clone(),
            },
            |network| PlacementError::UnknownNetwork {
                node: node.id.clone(),
                network: network.to_string(),
            },
        )
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

/// Resolve the concrete `srt://` addresses of a placed stream: the source node's
/// ingress socket a producer dials, and each destination's consumer socket.
///
/// Hosts follow the manifest network constraint per endpoint; the ingress port
/// is the sender hop's listen port and each node output is its receiver's assigned
/// consumer port. A remote destination reports the external listener URL the
/// sender dials out to. A `device` end reports `None`.
///
/// # Errors
/// Returns [`PlacementError`] if a referenced node is unregistered, declares no
/// attachment for the requested network, the source is remote, or the path does
/// not carry the hops and SRT listeners the stream's shape calls for.
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

    let mut destinations = Vec::with_capacity(stream.destinations.len());
    for destination in &stream.destinations {
        let dest = read_endpoint(&destination.endpoint)?;
        let endpoint =
            match (&dest.placement, dest.terminal) {
                (Placement::Remote(remote), _) => Some(remote_endpoint_addr(remote)),
                (Placement::Node(_), Terminal::Device) => None,
                (Placement::Node(node_id), Terminal::Srt) => {
                    let receiver_id = receiver_hop_id(&stream.name, &destination.id);
                    let receiver = path
                        .hops
                        .iter()
                        .find(|hop| hop.id == receiver_id)
                        .ok_or_else(|| PlacementError::MissingHop {
                            stream: path.stream.clone(),
                            hop: receiver_id.clone(),
                        })?;
                    let consumer = receiver.egresses.first().ok_or_else(|| {
                        PlacementError::NoConsumerSocket {
                            hop: receiver.id.clone(),
                        }
                    })?;
                    let consumer_port = listener_port(consumer, &receiver.id)?;
                    Some(endpoint_addr(node_id, dest.network, consumer_port, nodes)?)
                }
            };
        destinations.push(DestinationEndpoint {
            id: destination.id.clone(),
            endpoint,
        });
    }
    destinations.sort_by(|left, right| left.id.cmp(&right.id));

    Ok(StreamEndpoints {
        ingress,
        destinations,
    })
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
    let host = srt_listener_attachment(node, network)?
        .listeners
        .srt
        .as_ref()
        .expect("selected SRT listener attachment has listener data")
        .host
        .clone();
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
mod contract_tests {
    use super::*;
    use weave_core::{
        DeviceClass, EgressStatus, HopEndpointClass, HopProfile, HopState, LinkCondition,
        NetworkListeners, NodeCapabilities, NodeEndpoint, NodeTopology, SignallingListener,
        SocketStatus, SrtEndpoint, SrtListener, StreamDestination, TransportClass,
    };

    fn class(transport: Transport, roles: weave_core::RoleSet) -> HopEndpointClass {
        HopEndpointClass::Transport(TransportClass { transport, roles })
    }

    fn srt_profile(id: &str) -> HopProfile {
        HopProfile {
            id: id.to_string(),
            ingress: class(Transport::Srt, weave_core::RoleSet::both()),
            egress: class(Transport::Srt, weave_core::RoleSet::both()),
            max_egresses: None,
        }
    }

    fn attachment(id: &str, network: &str, dial: bool, host: Option<&str>) -> NetworkAttachment {
        NetworkAttachment {
            id: id.to_string(),
            network: network.to_string(),
            dial,
            listeners: NetworkListeners {
                srt: host.map(|host| SrtListener {
                    host: host.to_string(),
                    port_range: PortRange {
                        start: 20_000,
                        end: 20_100,
                    },
                }),
                whip: None,
                whep: None,
            },
        }
    }

    fn node(id: &str, attachments: Vec<NetworkAttachment>) -> NodeDescriptor {
        NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("http://{id}"),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                adapters: Vec::new(),
                hop_profiles: vec![srt_profile("srt-forward")],
            },
            topology: NodeTopology { attachments },
        }
    }

    fn srt_endpoint(node: &str) -> StreamTransport {
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
            endpoint: srt_endpoint(node),
        }
    }

    fn stream(destinations: Vec<StreamDestination>) -> StreamDefinition {
        StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            source: srt_endpoint("source"),
            destinations,
        }
    }

    fn shared_nodes() -> Vec<NodeDescriptor> {
        vec![
            node(
                "source",
                vec![attachment("wan", "internet", true, Some("192.0.2.1"))],
            ),
            node(
                "studio-node",
                vec![attachment("wan", "internet", true, Some("192.0.2.2"))],
            ),
            node(
                "preview-node",
                vec![attachment("wan", "internet", true, Some("192.0.2.3"))],
            ),
        ]
    }

    #[test]
    fn destination_reorder_keeps_hops_and_ports_stable() {
        let nodes = shared_nodes();
        let first = stream(vec![
            destination("studio", "studio-node"),
            destination("preview", "preview-node"),
        ]);
        let second = stream(vec![
            destination("preview", "preview-node"),
            destination("studio", "studio-node"),
        ]);
        let left = derive_path(&first, &nodes, &[], &mut PortAllocator::new()).unwrap();
        let right = derive_path(&second, &nodes, &[], &mut PortAllocator::new()).unwrap();
        assert_eq!(left, right);
        assert!(
            left.hops
                .iter()
                .any(|hop| hop.id == "weave-feed-receiver-studio")
        );
        assert!(
            left.hops
                .iter()
                .any(|hop| hop.id == "weave-feed-receiver-preview")
        );
    }

    #[test]
    fn shared_network_is_required_and_attachment_choice_is_deterministic() {
        let mut nodes = shared_nodes();
        nodes[1].topology.attachments = vec![
            attachment("z", "internet", true, Some("192.0.2.99")),
            attachment("a", "internet", true, Some("192.0.2.10")),
        ];
        let path = derive_path(
            &stream(vec![destination("studio", "studio-node")]),
            &nodes,
            &[],
            &mut PortAllocator::new(),
        )
        .unwrap();
        let SocketSpec::Srt(SrtSocket::Connect { host, .. }) = &path.hops[0].egresses[0].socket
        else {
            panic!("expected SRT connect");
        };
        assert_eq!(host, "192.0.2.10");

        nodes[1].topology.attachments[0].network = "studio-lan".to_string();
        nodes[1].topology.attachments[1].network = "studio-lan".to_string();
        assert!(
            derive_path(
                &stream(vec![destination("studio", "studio-node")]),
                &nodes,
                &[],
                &mut PortAllocator::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn dial_only_browser_attachment_reaches_a_listener() {
        let browser = NodeDescriptor {
            id: "browser".to_string(),
            endpoint: "browser://browser".to_string(),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                adapters: Vec::new(),
                hop_profiles: vec![HopProfile {
                    id: "camera-to-whip".to_string(),
                    ingress: HopEndpointClass::Device(DeviceClass {
                        device: DeviceKind::Capture,
                    }),
                    egress: class(
                        Transport::Whip,
                        weave_core::RoleSet::only(SocketRole::Connect),
                    ),
                    max_egresses: Some(1),
                }],
            },
            topology: NodeTopology {
                attachments: vec![attachment("client", "internet", true, None)],
            },
        };
        let mut strom = node(
            "studio-node",
            vec![attachment("wan", "internet", true, Some("192.0.2.2"))],
        );
        strom.topology.attachments[0].listeners.whip = Some(SignallingListener {
            base_url: "https://media.example/whip".to_string(),
        });
        strom.capabilities.hop_profiles.push(HopProfile {
            id: "whip-to-srt".to_string(),
            ingress: class(
                Transport::Whip,
                weave_core::RoleSet::only(SocketRole::Listen),
            ),
            egress: class(Transport::Srt, weave_core::RoleSet::both()),
            max_egresses: None,
        });
        let stream = StreamDefinition {
            name: "camera".to_string(),
            enabled: true,
            source: StreamTransport::Device(NodeEndpoint {
                node: "browser".to_string(),
                network: Some("internet".to_string()),
            }),
            destinations: vec![destination("studio", "studio-node")],
        };
        let path = derive_path(&stream, &[browser, strom], &[], &mut PortAllocator::new()).unwrap();
        assert_eq!(path.hops[0].profile_id, "camera-to-whip");
    }

    #[test]
    fn profile_limit_rejects_browser_capture_fanout() {
        let mut nodes = shared_nodes();
        let mut browser = nodes.remove(0);
        browser.id = "browser".to_string();
        browser.capabilities.hop_profiles = vec![HopProfile {
            id: "capture-to-srt".to_string(),
            ingress: HopEndpointClass::Device(DeviceClass {
                device: DeviceKind::Capture,
            }),
            egress: class(Transport::Srt, weave_core::RoleSet::both()),
            max_egresses: Some(1),
        }];
        let definition = StreamDefinition {
            name: "fanout".to_string(),
            enabled: true,
            source: StreamTransport::Device(NodeEndpoint {
                node: "browser".to_string(),
                network: None,
            }),
            destinations: vec![
                destination("preview", "preview-node"),
                destination("studio", "studio-node"),
            ],
        };
        nodes.push(browser);
        assert!(matches!(
            derive_path(&definition, &nodes, &[], &mut PortAllocator::new()),
            Err(PlacementError::NoHopProfile { egresses: 2, .. })
        ));
    }

    #[test]
    fn profile_mismatch_fails_before_desired_state() {
        let mut nodes = shared_nodes();
        nodes[0].capabilities.hop_profiles = vec![HopProfile {
            id: "srt-to-whep".to_string(),
            ingress: class(Transport::Srt, weave_core::RoleSet::both()),
            egress: class(
                Transport::Whep,
                weave_core::RoleSet::only(SocketRole::Listen),
            ),
            max_egresses: Some(1),
        }];
        let error = derive_path(
            &stream(vec![destination("studio", "studio-node")]),
            &nodes,
            &[],
            &mut PortAllocator::new(),
        )
        .unwrap_err();
        let reason = error.to_string();
        assert!(
            reason.contains("profile") || reason.contains("transport") || reason.contains("route"),
            "placement names the unsupported shape: {reason}"
        );
    }

    #[test]
    fn transit_eligibility_comes_from_a_matching_profile() {
        let source = node(
            "source",
            vec![attachment("a", "net-a", true, Some("10.0.0.1"))],
        );
        let destination_node = node(
            "studio-node",
            vec![attachment("b", "net-b", true, Some("10.1.0.1"))],
        );
        let relay = node(
            "relay",
            vec![
                attachment("a", "net-a", true, Some("10.0.0.2")),
                attachment("b", "net-b", true, Some("10.1.0.2")),
            ],
        );
        let definition = stream(vec![destination("studio", "studio-node")]);
        let path = derive_path(
            &definition,
            &[source.clone(), destination_node.clone(), relay.clone()],
            &[],
            &mut PortAllocator::new(),
        )
        .unwrap();
        assert!(path.hops.iter().any(|hop| hop.node_id == "relay"));

        let mut ineligible = relay;
        ineligible.capabilities.hop_profiles.clear();
        assert!(
            derive_path(
                &definition,
                &[source, destination_node, ineligible],
                &[],
                &mut PortAllocator::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn each_destination_has_an_independent_media_status() {
        let definition = stream(vec![
            destination("preview", "preview-node"),
            destination("studio", "studio-node"),
        ]);
        let path =
            derive_path(&definition, &shared_nodes(), &[], &mut PortAllocator::new()).unwrap();
        let observed: Vec<_> = path
            .hops
            .iter()
            .map(|hop| HopStatus {
                id: hop.id.clone(),
                node_id: hop.node_id.clone(),
                state: HopState::Provisioned,
                ingress: SocketStatus {
                    condition: LinkCondition::Flowing,
                    resolved: None,
                    stats: None,
                },
                egresses: hop
                    .egresses
                    .iter()
                    .map(|egress| EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: SocketStatus {
                            condition: if egress.branch_id == "preview" {
                                LinkCondition::Connecting
                            } else {
                                LinkCondition::Flowing
                            },
                            resolved: None,
                            stats: None,
                        },
                    })
                    .collect(),
            })
            .collect();
        assert_eq!(
            destination_path_status(&path, "studio", &observed),
            PathStatus::Flowing
        );
        assert_eq!(
            destination_path_status(&path, "preview", &observed),
            PathStatus::Degraded
        );
    }
}
