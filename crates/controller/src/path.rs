//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use std::collections::{HashMap, HashSet};

use serde::Serialize;
use weave_core::{
    DEFAULT_DATA_PLANE_ALIAS, DesiredHop, HOP_ID_PREFIX, HopConditions, HopRole, HopStatus,
    NodeDescriptor, Path, PathStatus, PortRange, RemoteAddr, SocketRole, SocketSpec, SrtEndpoint,
    SrtParams, StreamDefinition, StreamTransport, Transport, roll_up_path,
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
}

#[must_use]
pub fn sender_hop_id(stream: &str) -> String {
    format!("{HOP_ID_PREFIX}{stream}-sender")
}

#[must_use]
pub fn receiver_hop_id(stream: &str, index: usize) -> String {
    format!("{HOP_ID_PREFIX}{stream}-receiver-{index}")
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
/// receiver on its destination's `node`. Delivery addresses resolve at planning
/// time — the receiver node's data-plane alias supplies the host and every port
/// is claimed from `ports`, the per-tick collision-aware allocator. Sender egress
/// always targets this planned delivery; the receiver's reported `resolved_ingress`
/// is observability only and never rewrites egress.
///
/// A `remote` destination places no receiver hop and claims no port: the sender
/// gains one caller egress to the external listener. The source must be a node;
/// a remote source is rejected.
///
/// Fan-out is one sender hop teeing to one egress per destination. Placement is
/// all-or-nothing: if any destination is unplaceable the whole derivation fails
/// and the stream stays pending.
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

    let sender_node = source_node(source)?;
    let sender_id = sender_hop_id(&stream.name);

    let mut sender_egresses = Vec::with_capacity(stream.destinations.len());
    let mut receivers = Vec::new();

    for (index, dest) in stream.destinations.iter().enumerate() {
        let StreamTransport::Srt(dest) = dest;
        let dest_latency = dest.latency.unwrap_or(DEFAULT_SINK_LATENCY);

        match endpoint_placement(dest)? {
            Placement::Remote(remote) => {
                sender_egresses.push(connect_socket(remote.host, remote.port, dest_latency));
            }
            Placement::Node(receiver_node) => {
                let receiver_id = receiver_hop_id(&stream.name, index);
                let (dest_host, dest_port) =
                    resolve_delivery(dest, &receiver_node, &receiver_id, nodes, ports)?;
                let consumer_port =
                    claim_port(&receiver_node, &consumer_key(&receiver_id), nodes, ports)?;

                sender_egresses.push(connect_socket(dest_host, dest_port, dest_latency));
                receivers.push(DesiredHop {
                    id: receiver_id,
                    node_id: receiver_node,
                    role: HopRole::Receiver,
                    ingress: listen_socket(dest_port, dest_latency),
                    egresses: vec![listen_socket(consumer_port, RECV_CONSUMER_LATENCY)],
                });
            }
        }
    }

    let sender = DesiredHop {
        id: sender_id.clone(),
        node_id: sender_node.clone(),
        role: HopRole::Sender,
        ingress: source_socket(source, &sender_node, &sender_id, nodes, ports)?,
        egresses: sender_egresses,
    };

    let mut hops = Vec::with_capacity(1 + receivers.len());
    hops.push(sender);
    hops.extend(receivers);

    Ok(Path {
        stream: stream.name.clone(),
        enabled: stream.enabled,
        hops,
    })
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

/// Resolve the concrete `(host, port)` a peer uses to reach this endpoint: the
/// node's data-plane alias supplies the host and a port is claimed from the
/// node's declared range.
fn resolve_delivery(
    endpoint: &SrtEndpoint,
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
) -> Result<(String, u16), PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    let host = resolve_host(node, endpoint.network.as_deref())?;
    Ok((host, ports.claim(node, hop_id)?))
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

/// The node's data-plane host for a manifest `network` alias, defaulting to
/// [`DEFAULT_DATA_PLANE_ALIAS`] when unset.
fn resolve_host(node: &NodeDescriptor, network: Option<&str>) -> Result<String, PlacementError> {
    let alias = network.unwrap_or(DEFAULT_DATA_PLANE_ALIAS);
    node.capabilities
        .data_plane
        .get(alias)
        .cloned()
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
    let host = resolve_host(node, network)?;
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
    use weave_core::{HopState, LinkCondition, NodeCapabilities, NodeStatus, PortRange, ResolvedAddr};

    fn node(id: &str, host: &str) -> NodeDescriptor {
        node_with_aliases(id, &[(DEFAULT_DATA_PLANE_ALIAS, host)])
    }

    fn node_with_aliases(id: &str, aliases: &[(&str, &str)]) -> NodeDescriptor {
        NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("http://{id}:8080"),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                data_plane: aliases
                    .iter()
                    .map(|(alias, host)| ((*alias).to_string(), (*host).to_string()))
                    .collect(),
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
            path.hops[0].egresses[0].port,
            path.hops[1].ingress.port,
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
        assert_eq!(derive(&stream, &nodes()), Err(PlacementError::NoDestination));
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
        assert_eq!(path.hops.len(), 2, "sender plus one receiver for the node dest");
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
