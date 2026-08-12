//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use weave_core::{
    DEFAULT_DATA_PLANE_ALIAS, DataPlaneAddr, DesiredHop, HOP_ID_PREFIX, HopConditions, HopRole,
    HopStatus, NodeDescriptor, Path, PathStatus, PortRange, RemoteAddr, SocketRole, SocketSpec,
    SrtEndpoint, SrtParams, StreamDefinition, StreamTransport, Transport, roll_up_path,
};

const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;
const RECV_CONSUMER_LATENCY: u32 = 200;

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
    #[error("hop {0} has no assigned port")]
    UnassignedPort(String),
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
    #[error("a source endpoint must not pin via")]
    SourceVia,
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
enum Placement {
    Node(String),
    Remote(RemoteAddr),
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
/// Which end of a link listens follows [`Reachability`](weave_core::Reachability)
/// rather than a fixed template: the downstream listens when it can be dialled,
/// otherwise the upstream listens and the downstream calls it. When neither end
/// can be dialled the link needs transit, and [`splice_relays`] inserts a relay
/// node both ends can call — the NAT-to-NAT case, which resolves into two links
/// under the same rule rather than a special path.
///
/// A `remote` destination places no receiver hop and claims no port for its
/// terminal link: whichever hop precedes it gains one caller egress to the
/// external listener. The source must be a node; a remote source is rejected.
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
    let StreamTransport::Srt(source) = &stream.source;
    if stream.destinations.is_empty() {
        return Err(PlacementError::NoDestination);
    }
    if !source.via.is_empty() {
        return Err(PlacementError::SourceVia);
    }

    let sender_node = source_node(source)?;
    let sender_id = sender_hop_id(&stream.name);
    let source_station = Station {
        node_id: sender_node.clone(),
        network: source.network.clone(),
    };

    let mut sender_egresses = Vec::with_capacity(stream.destinations.len());
    let mut downstream = Vec::new();

    for (index, dest) in stream.destinations.iter().enumerate() {
        let StreamTransport::Srt(dest) = dest;
        let latency = dest.latency.unwrap_or(DEFAULT_SINK_LATENCY);
        let placement = endpoint_placement(dest)?;

        let chain = chain_hops(
            &stream.name,
            index,
            &source_station,
            dest,
            &placement,
            nodes,
        )?;

        // Each link attaches its upstream socket to the hop before it, which is
        // the sender for the first link and the previous chain hop after that.
        let mut hops: Vec<DesiredHop> = Vec::with_capacity(chain.len());
        let mut upstream = LinkEnd {
            station: &source_station,
            hop_id: &sender_id,
        };

        for entry in &chain {
            let (up_socket, down_socket) = plan_link(
                &upstream,
                &LinkEnd {
                    station: &entry.station,
                    hop_id: &entry.id,
                },
                latency,
                nodes,
                ports,
            )?;
            push_egress(&mut sender_egresses, &mut hops, up_socket);
            hops.push(DesiredHop {
                id: entry.id.clone(),
                node_id: entry.station.node_id.clone(),
                role: entry.role,
                ingress: down_socket,
                egresses: Vec::new(),
            });
            upstream = LinkEnd {
                station: &entry.station,
                hop_id: &entry.id,
            };
        }

        match &placement {
            // An external listener terminates the chain: the last hop dials it
            // and no hop is placed for it.
            Placement::Remote(remote) => {
                let socket = connect_socket(remote.host.clone(), remote.port, latency);
                push_egress(&mut sender_egresses, &mut hops, socket);
            }
            // The last hop is the receiver; its remaining egress is the socket
            // the consumer dials.
            Placement::Node(_) => {
                let receiver = hops
                    .last_mut()
                    .ok_or_else(|| PlacementError::UnassignedPort(stream.name.clone()))?;
                let port =
                    claim_port(&receiver.node_id, &consumer_key(&receiver.id), nodes, ports)?;
                receiver
                    .egresses
                    .push(listen_socket(port, RECV_CONSUMER_LATENCY));
            }
        }

        downstream.extend(hops);
    }

    let sender = DesiredHop {
        id: sender_id.clone(),
        node_id: sender_node.clone(),
        role: HopRole::Sender,
        ingress: source_socket(source, &sender_node, &sender_id, nodes, ports)?,
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

/// One end of a link: where it sits and which hop owns the socket.
struct LinkEnd<'a> {
    station: &'a Station,
    hop_id: &'a str,
}

/// Build the hops between the sender and one destination's terminal: a bridge per
/// relayed node, then the receiver when the destination is a node.
fn chain_hops(
    stream: &str,
    dest_index: usize,
    source: &Station,
    dest: &SrtEndpoint,
    placement: &Placement,
    nodes: &[NodeDescriptor],
) -> Result<Vec<ChainHop>, PlacementError> {
    let mut stations: Vec<Station> = dest.via.iter().map(|id| Station::relay(id)).collect();
    if let Placement::Node(node_id) = placement {
        stations.push(Station {
            node_id: node_id.clone(),
            network: dest.network.clone(),
        });
    }

    let stations = splice_relays(source, stations, nodes)?;
    let terminal_is_node = matches!(placement, Placement::Node(_));
    let last = stations.len().saturating_sub(1);

    Ok(stations
        .into_iter()
        .enumerate()
        .map(|(position, station)| {
            if terminal_is_node && position == last {
                ChainHop {
                    station,
                    id: receiver_hop_id(stream, dest_index),
                    role: HopRole::Receiver,
                }
            } else {
                ChainHop {
                    station,
                    id: bridge_hop_id(stream, dest_index, position),
                    role: HopRole::Bridge,
                }
            }
        })
        .collect())
}

/// Insert a relay ahead of any link whose ends cannot dial each other.
///
/// A spliced relay is dialable by construction, so both halves of the split link
/// resolve under the ordinary rule — the upstream calls the relay, and the
/// downstream calls it too. One pass is enough; no inserted link can itself need
/// a relay.
fn splice_relays(
    source: &Station,
    stations: Vec<Station>,
    nodes: &[NodeDescriptor],
) -> Result<Vec<Station>, PlacementError> {
    let mut resolved = Vec::with_capacity(stations.len());
    let mut upstream = source.clone();

    for station in stations {
        if !link_dialable(&upstream, &station, nodes)? {
            let relay = pick_relay(nodes, &upstream, &station)?;
            resolved.push(relay);
        }
        upstream = station.clone();
        resolved.push(station);
    }

    Ok(resolved)
}

/// Whether either end of a link can be dialled by the other.
fn link_dialable(
    upstream: &Station,
    downstream: &Station,
    nodes: &[NodeDescriptor],
) -> Result<bool, PlacementError> {
    if station_addr(downstream, nodes)?.is_dialable() {
        return Ok(true);
    }
    Ok(station_addr(upstream, nodes)?.is_dialable())
}

/// The lowest-id relay node both ends of an undialable link can call. Sorting
/// keeps the choice stable across ticks, so a stream does not migrate between
/// equally eligible relays.
fn pick_relay(
    nodes: &[NodeDescriptor],
    upstream: &Station,
    downstream: &Station,
) -> Result<Station, PlacementError> {
    nodes
        .iter()
        .filter(|node| node.capabilities.relay)
        .filter(|node| node.id != upstream.node_id && node.id != downstream.node_id)
        .filter(|node| {
            node.capabilities
                .data_plane
                .get(DEFAULT_DATA_PLANE_ALIAS)
                .is_some_and(DataPlaneAddr::is_dialable)
        })
        .min_by(|a, b| a.id.cmp(&b.id))
        .map(|node| Station::relay(&node.id))
        .ok_or_else(|| PlacementError::NoRelayAvailable {
            upstream: upstream.node_id.clone(),
            downstream: downstream.node_id.clone(),
        })
}

/// Plan one link's socket pair: `(upstream egress, downstream ingress)`.
///
/// The downstream listens whenever it can be dialled, which keeps the common case
/// identical to a fixed sender-calls-receiver template. Otherwise the direction
/// reverses and the downstream dials the upstream. The port is claimed on
/// whichever node listens, always keyed by the downstream hop id so an assignment
/// stays stable when a link's direction is the same across ticks.
fn plan_link(
    upstream: &LinkEnd,
    downstream: &LinkEnd,
    latency: u32,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<(SocketSpec, SocketSpec), PlacementError> {
    let down_addr = station_addr(downstream.station, nodes)?.clone();
    if down_addr.is_dialable() {
        let port = claim_port(&downstream.station.node_id, downstream.hop_id, nodes, ports)?;
        return Ok((
            connect_socket(down_addr.host, port, latency),
            listen_socket(port, latency),
        ));
    }

    let up_addr = station_addr(upstream.station, nodes)?.clone();
    if up_addr.is_dialable() {
        let port = claim_port(&upstream.station.node_id, downstream.hop_id, nodes, ports)?;
        return Ok((
            listen_socket(port, latency),
            connect_socket(up_addr.host, port, latency),
        ));
    }

    Err(PlacementError::NoRelayAvailable {
        upstream: upstream.station.node_id.clone(),
        downstream: downstream.station.node_id.clone(),
    })
}

/// The data-plane address a station is reached at, via its node's alias map.
fn station_addr<'a>(
    station: &Station,
    nodes: &'a [NodeDescriptor],
) -> Result<&'a DataPlaneAddr, PlacementError> {
    let node =
        find_node(nodes, &station.node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
            node: station.node_id.clone(),
        })?;
    resolve_addr(node, station.network.as_deref())
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

fn endpoint_placement(endpoint: &SrtEndpoint) -> Result<Placement, PlacementError> {
    match (&endpoint.node, &endpoint.remote) {
        (Some(node), None) => Ok(Placement::Node(node.clone())),
        (None, Some(remote)) => Ok(Placement::Remote(remote.clone())),
        _ => Err(PlacementError::EndpointPlacement),
    }
}

fn source_node(source: &SrtEndpoint) -> Result<String, PlacementError> {
    match endpoint_placement(source)? {
        Placement::Node(node) => Ok(node),
        Placement::Remote(_) => Err(PlacementError::RemoteSource),
    }
}

fn source_socket(
    source: &SrtEndpoint,
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<SocketSpec, PlacementError> {
    let latency = source.latency.unwrap_or(DEFAULT_SRC_LATENCY);
    let port = claim_port(node_id, hop_id, nodes, ports)?;
    Ok(listen_socket(port, latency))
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

fn connect_socket(host: String, port: u16, latency: u32) -> SocketSpec {
    SocketSpec {
        transport: Transport::Srt,
        role: SocketRole::Connect,
        host: Some(host),
        port: Some(port),
        params: SrtParams {
            latency: Some(latency),
        },
    }
}

fn listen_socket(port: u16, latency: u32) -> SocketSpec {
    SocketSpec {
        transport: Transport::Srt,
        role: SocketRole::Listen,
        host: None,
        port: Some(port),
        params: SrtParams {
            latency: Some(latency),
        },
    }
}

/// Concrete `srt://` addresses a producer and consumers use to reach a placed
/// stream, resolved against the same node data-plane aliases planning used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamEndpoints {
    pub ingress: EndpointAddr,
    pub outputs: Vec<EndpointAddr>,
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
/// sender dials out to.
///
/// # Errors
/// Returns [`PlacementError`] if a referenced node is unregistered, declares no
/// address for the requested alias, the source is remote, or the path carries an
/// unassigned port.
pub fn stream_endpoints(
    stream: &StreamDefinition,
    path: &Path,
    nodes: &[NodeDescriptor],
) -> Result<StreamEndpoints, PlacementError> {
    let StreamTransport::Srt(source) = &stream.source;
    let source_node = source_node(source)?;
    let sender = path
        .hops
        .first()
        .ok_or_else(|| PlacementError::UnassignedPort(path.stream.clone()))?;
    let ingress_port = hop_port(&sender.ingress, &sender.id)?;
    let ingress = endpoint_addr(&source_node, source.network.as_deref(), ingress_port, nodes)?;

    let mut outputs = Vec::with_capacity(stream.destinations.len());
    for (index, dest) in stream.destinations.iter().enumerate() {
        let StreamTransport::Srt(dest) = dest;
        match endpoint_placement(dest)? {
            Placement::Remote(remote) => outputs.push(remote_endpoint_addr(&remote)),
            Placement::Node(node_id) => {
                let receiver_id = receiver_hop_id(&stream.name, index);
                let receiver = path
                    .hops
                    .iter()
                    .find(|hop| hop.id == receiver_id)
                    .ok_or_else(|| PlacementError::UnassignedPort(receiver_id.clone()))?;
                let consumer = receiver
                    .egresses
                    .first()
                    .ok_or_else(|| PlacementError::UnassignedPort(receiver.id.clone()))?;
                let consumer_port = hop_port(consumer, &receiver.id)?;
                outputs.push(endpoint_addr(
                    &node_id,
                    dest.network.as_deref(),
                    consumer_port,
                    nodes,
                )?);
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

fn hop_port(spec: &SocketSpec, hop_id: &str) -> Result<u16, PlacementError> {
    spec.port
        .ok_or_else(|| PlacementError::UnassignedPort(hop_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use weave_core::{
        HopState, LinkCondition, NodeCapabilities, NodeStatus, PortRange, Reachability,
        ResolvedAddr,
    };

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
        assert_eq!(sender.ingress.role, SocketRole::Listen);
        let ingress_port = sender.ingress.port.expect("ingress port");
        assert!((7000..=7999).contains(&ingress_port));
        assert_eq!(sender.egresses.len(), 1);
        assert_eq!(sender.egresses[0].role, SocketRole::Connect);
        assert_eq!(sender.egresses[0].host.as_deref(), Some("172.27.0.10"));

        let receiver = &path.hops[1];
        assert_eq!(receiver.role, HopRole::Receiver);
        assert_eq!(receiver.node_id, "strom-node-2");
        let dest_port = receiver.ingress.port.expect("dest port");
        assert_eq!(sender.egresses[0].port, Some(dest_port));
        assert!((7000..=7999).contains(&receiver.egresses[0].port.expect("consumer port")));
    }

    #[test]
    fn receiver_is_placed_on_its_declared_node() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.node = Some("strom-node-1".to_string());

        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(path.hops[1].node_id, "strom-node-1");
    }

    #[test]
    fn source_on_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        let StreamTransport::Srt(source) = &mut stream.source;
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
        assert_eq!(egress.host.as_deref(), Some("172.27.0.10"));
        let port = egress.port.expect("assigned port");
        assert!((7000..=7999).contains(&port), "port {port} within range");
        assert_eq!(
            path.hops[1].ingress.port,
            Some(port),
            "receiver listens on it"
        );
    }

    #[test]
    fn node_ref_destination_resolves_named_alias() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.network = Some("wan".to_string());

        let mut nodes = nodes();
        nodes[1] = node_with_aliases(
            "strom-node-2",
            &[("default", "172.27.0.10"), ("wan", "203.0.113.7")],
        );

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("203.0.113.7")
        );
    }

    #[test]
    fn node_ref_destination_with_unknown_alias_is_rejected() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
            first.hops[0].egresses[0].port,
            second.hops[0].egresses[0].port
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
        let ingress = receiver.ingress.port.expect("ingress port");
        let consumer = receiver.egresses[0].port.expect("consumer port");
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
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.10")
        );

        let after = vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.55"),
        ];
        let path = derive(&stream, &after).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.55")
        );
    }

    #[test]
    fn sender_egress_uses_planned_delivery_when_no_resolved_ingress() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.10")
        );
        assert_eq!(path.hops[0].egresses[0].port, path.hops[1].ingress.port);
    }

    #[test]
    fn sender_egress_ignores_reported_resolved_ingress_even_for_wan() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
            path.hops[0].egresses[0].host.as_deref(),
            Some("203.0.113.7"),
            "keeps the wan host"
        );
        assert_eq!(
            path.hops[0].egresses[0].port, path.hops[1].ingress.port,
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
        assert_eq!(sender.egresses[0].role, SocketRole::Connect);
        assert_eq!(sender.egresses[0].host.as_deref(), Some("198.51.100.5"));
        assert_eq!(sender.egresses[0].port, Some(9000));

        let endpoints = stream_endpoints(&stream, &path, &nodes()).expect("endpoints");
        assert_eq!(endpoints.outputs.len(), 1);
        assert_eq!(endpoints.outputs[0].url, "srt://198.51.100.5:9000");
        assert!(endpoints.outputs[0].node.is_empty());
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
        assert_eq!(sender.egresses[1].host.as_deref(), Some("198.51.100.5"));
        assert_eq!(path.hops[1].id, receiver_hop_id("contribution", 0));

        let endpoints = stream_endpoints(&stream, &path, &nodes()).expect("endpoints");
        assert_eq!(endpoints.outputs.len(), 2);
        assert_eq!(endpoints.outputs[0].node, "strom-node-2");
        assert_eq!(endpoints.outputs[1].url, "srt://198.51.100.5:9000");
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
        assert_eq!(sender.egresses[0].host.as_deref(), Some("172.27.0.10"));
        assert_eq!(sender.egresses[1].host.as_deref(), Some("172.26.0.10"));

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
        let StreamTransport::Srt(dest) = &mut stream.destinations[1];
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
        assert_eq!(path.hops[0].egresses[0].role, SocketRole::Connect);
        assert_eq!(path.hops[1].ingress.role, SocketRole::Listen);
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
            sender_egress.role,
            SocketRole::Listen,
            "the dialable end listens"
        );
        assert_eq!(
            receiver.ingress.role,
            SocketRole::Connect,
            "the NAT'd end dials out"
        );
        assert_eq!(
            receiver.ingress.host.as_deref(),
            Some("172.26.0.10"),
            "it dials the source node's data-plane address"
        );
        assert_eq!(receiver.ingress.port, sender_egress.port);
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

        let egress_port = sender.egresses[0].port.expect("egress port");
        assert!(
            (7000..=7999).contains(&egress_port),
            "the reversed link listens on node-1, so its port comes from node-1's range"
        );
        assert_ne!(sender.ingress.port, sender.egresses[0].port);
        assert!(
            (8000..=8999).contains(&receiver.egresses[0].port.expect("consumer port")),
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
            sender.egresses[0].role,
            SocketRole::Connect,
            "the NAT'd source calls out"
        );
        assert_eq!(sender.egresses[0].host.as_deref(), Some("198.51.100.9"));
        assert_eq!(
            receiver.ingress.role,
            SocketRole::Connect,
            "the NAT'd destination calls out too"
        );
        assert_eq!(receiver.ingress.host.as_deref(), Some("198.51.100.9"));

        assert_eq!(
            bridge.ingress.role,
            SocketRole::Listen,
            "the relay listens on both sides"
        );
        assert_eq!(bridge.egresses[0].role, SocketRole::Listen);
        assert_eq!(bridge.ingress.port, sender.egresses[0].port);
        assert_eq!(bridge.egresses[0].port, receiver.ingress.port);
        assert_ne!(
            bridge.ingress.port, bridge.egresses[0].port,
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
    fn via_pins_a_bridge_on_an_otherwise_direct_link() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
            path.hops[2].ingress.role,
            SocketRole::Listen,
            "both ends are dialable, so the relay calls the receiver"
        );
        assert_eq!(path.hops[1].egresses[0].role, SocketRole::Connect);
    }

    #[test]
    fn via_pins_a_node_that_need_not_advertise_as_a_relay() {
        // `relay: true` gates automatic selection. An explicit pin is operator
        // intent and does not consult it.
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.via = vec!["transit".to_string()];

        let mut nodes = nodes();
        nodes.push(node("transit", "198.51.100.9"));

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(path.hops[1].node_id, "transit");
    }

    #[test]
    fn via_chains_multiple_relays_in_order() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
            path.hops[1].egresses[0].host.as_deref(),
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
            bridge.egresses[0].host.as_deref(),
            Some("198.51.100.5"),
            "the bridge dials the external listener"
        );
        assert_eq!(bridge.egresses[0].port, Some(9000));

        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");
        assert_eq!(endpoints.outputs[0].url, "srt://198.51.100.5:9000");
    }

    #[test]
    fn a_pinned_via_still_gets_a_relay_when_its_own_link_is_undialable() {
        // node-1 and the pinned transit node are both outbound-only, so the link
        // between them needs a relay of its own on top of the pin.
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
        let StreamTransport::Srt(source) = &mut stream.source;
        source.via = vec!["edge-relay".to_string()];
        assert_eq!(derive(&stream, &nodes()), Err(PlacementError::SourceVia));
    }

    #[test]
    fn via_to_an_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
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
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.via = vec!["edge-relay".to_string()];

        let mut nodes = nodes();
        nodes.push(relay_node("edge-relay", "198.51.100.9"));

        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");

        assert_eq!(endpoints.ingress.node, "strom-node-1");
        assert_eq!(endpoints.ingress.port, path.hops[0].ingress.port.unwrap());
        assert_eq!(endpoints.outputs.len(), 1);
        assert_eq!(
            endpoints.outputs[0].node, "strom-node-2",
            "the consumer still attaches at the destination, not the relay"
        );
        let consumer_port = path.hops[2].egresses[0].port.expect("consumer port");
        assert_eq!(endpoints.outputs[0].port, consumer_port);
    }

    #[test]
    fn stream_endpoints_resolve_ingress_and_outputs() {
        let stream = contribution();
        let nodes = nodes();
        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");

        let ingress_port = path.hops[0].ingress.port.expect("ingress port");
        assert_eq!(endpoints.ingress.node, "strom-node-1");
        assert_eq!(endpoints.ingress.host, "172.26.0.10");
        assert_eq!(endpoints.ingress.port, ingress_port);
        assert_eq!(
            endpoints.ingress.url,
            format!("srt://172.26.0.10:{ingress_port}")
        );

        assert_eq!(endpoints.outputs.len(), 1);
        let output = &endpoints.outputs[0];
        let consumer_port = path.hops[1].egresses[0].port.expect("consumer port");
        assert_eq!(output.node, "strom-node-2");
        assert_eq!(output.host, "172.27.0.10");
        assert_eq!(output.port, consumer_port);
        assert_eq!(output.url, format!("srt://172.27.0.10:{consumer_port}"));
    }

    #[test]
    fn stream_endpoints_follow_named_alias() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.network = Some("wan".to_string());

        let mut nodes = nodes();
        nodes[1] = node_with_aliases(
            "strom-node-2",
            &[("default", "172.27.0.10"), ("wan", "203.0.113.7")],
        );

        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");
        assert_eq!(endpoints.outputs[0].host, "203.0.113.7");
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
}
