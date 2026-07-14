//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use serde::Serialize;
use weave_core::{
    DEFAULT_DATA_PLANE_ALIAS, DesiredHop, HOP_ID_PREFIX, HopConditions, HopRole, HopStatus,
    NodeDescriptor, Path, PathStatus, PortRange, SocketRole, SocketSpec, SrtEndpoint, SrtParams,
    StreamDefinition, StreamTransport, Transport, roll_up_path,
};

const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;
const RECV_CONSUMER_LATENCY: u32 = 200;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("stream has no destinations")]
    NoDestination,
    #[error("port {0} leaves no room for a consumer port")]
    PortOverflow(u16),
    #[error("node {node} is not registered")]
    NodeNotRegistered { node: String },
    #[error("node {node} declares no data-plane address for alias {alias}")]
    UnknownAlias { node: String, alias: String },
    #[error("node {node} declares no assignable port range")]
    NoPortRange { node: String },
    #[error("hop {0} has no assigned port")]
    UnassignedPort(String),
}

#[must_use]
pub fn sender_hop_id(stream: &str) -> String {
    format!("{HOP_ID_PREFIX}{stream}-sender")
}

#[must_use]
pub fn receiver_hop_id(stream: &str, index: usize) -> String {
    format!("{HOP_ID_PREFIX}{stream}-receiver-{index}")
}

/// Derive the ordered (source→destination) hop chain realising one stream.
///
/// Placement is by `node`: the sender runs on `source.node`, each receiver on its
/// destination's `node`. Delivery addresses resolve at planning time — the
/// receiver node's data-plane alias supplies the host and a port is assigned
/// deterministically from the node's declared range. A sender egress connects to
/// its receiver's reported ingress when resolved to a concrete host, else the
/// planned delivery address.
///
/// Fan-out is one sender hop teeing to one egress per destination, plus one
/// receiver hop per destination. Placement is all-or-nothing: if any destination
/// is unplaceable the whole derivation fails and the stream stays pending.
pub fn derive_path(
    stream: &StreamDefinition,
    nodes: &[NodeDescriptor],
    observed: &[HopStatus],
) -> Result<Path, PlacementError> {
    let StreamTransport::Srt(source) = &stream.source;
    if stream.destinations.is_empty() {
        return Err(PlacementError::NoDestination);
    }

    let sender_id = sender_hop_id(&stream.name);
    let sender_node = source.node.clone();

    let mut sender_egresses = Vec::with_capacity(stream.destinations.len());
    let mut receivers = Vec::with_capacity(stream.destinations.len());

    for (index, dest) in stream.destinations.iter().enumerate() {
        let StreamTransport::Srt(dest) = dest;
        let receiver_node = dest.node.clone();
        let receiver_id = receiver_hop_id(&stream.name, index);

        let (dest_host, dest_port) = resolve_delivery(dest, &receiver_node, &receiver_id, nodes)?;
        let dest_latency = dest.latency.unwrap_or(DEFAULT_SINK_LATENCY);
        let consumer_port = dest_port
            .checked_add(1)
            .ok_or(PlacementError::PortOverflow(dest_port))?;

        let (egress_host, egress_port) =
            connect_target(observed, &receiver_id, &dest_host, dest_port);

        sender_egresses.push(connect_socket(egress_host, egress_port, dest_latency));
        receivers.push(DesiredHop {
            id: receiver_id,
            node_id: receiver_node,
            role: HopRole::Receiver,
            ingress: listen_socket(dest_port, dest_latency),
            egresses: vec![listen_socket(consumer_port, RECV_CONSUMER_LATENCY)],
        });
    }

    let sender = DesiredHop {
        id: sender_id.clone(),
        node_id: sender_node.clone(),
        role: HopRole::Sender,
        ingress: source_socket(source, &sender_node, &sender_id, nodes)?,
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

/// The address a sender egress connects to for `downstream_id`: the receiver's
/// reported ingress when resolved to a concrete host, else the planned delivery
/// address computed at derivation time.
fn connect_target(
    observed: &[HopStatus],
    downstream_id: &str,
    planned_host: &str,
    planned_port: u16,
) -> (String, u16) {
    observed
        .iter()
        .find(|status| status.id == downstream_id)
        .and_then(|status| status.resolved_ingress.as_ref())
        .filter(|addr| !is_wildcard_host(&addr.host))
        .map_or_else(
            || (planned_host.to_string(), planned_port),
            |addr| (addr.host.clone(), addr.port),
        )
}

fn source_socket(
    source: &SrtEndpoint,
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
) -> Result<SocketSpec, PlacementError> {
    let latency = source.latency.unwrap_or(DEFAULT_SRC_LATENCY);
    let port = resolve_listen_port(node_id, hop_id, nodes)?;
    Ok(listen_socket(port, latency))
}

/// Resolve the concrete `(host, port)` a peer uses to reach this endpoint: the
/// node's data-plane alias supplies the host and a deterministic port is assigned
/// from the node's declared range.
fn resolve_delivery(
    endpoint: &SrtEndpoint,
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
) -> Result<(String, u16), PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    let host = resolve_host(node, endpoint.network.as_deref())?;
    Ok((host, assign_port(node, hop_id)?))
}

/// A listener needs only a port, assigned from the node's declared range.
fn resolve_listen_port(
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
) -> Result<u16, PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    assign_port(node, hop_id)
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

fn assign_port(node: &NodeDescriptor, hop_id: &str) -> Result<u16, PlacementError> {
    let range = node
        .capabilities
        .port_range
        .ok_or_else(|| PlacementError::NoPortRange {
            node: node.id.clone(),
        })?;
    Ok(port_in_range(range, hop_id))
}

/// Map a hop id into a node's port range deterministically (FNV-1a), so planning
/// stays a pure function and re-derivation is stable across ticks.
fn port_in_range(range: PortRange, key: &str) -> u16 {
    let span = u64::from(range.span()) + 1;
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    #[allow(clippy::cast_possible_truncation)]
    let offset = (hash % span) as u16;
    range.start.saturating_add(offset)
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

fn is_wildcard_host(host: &str) -> bool {
    matches!(host, "" | "0.0.0.0" | "::" | "[::]")
}

/// Concrete `srt://` addresses a producer and consumers use to reach a placed
/// stream, resolved against the same node data-plane aliases planning used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StreamEndpoints {
    pub ingress: EndpointAddr,
    pub outputs: Vec<EndpointAddr>,
}

/// One resolved data-plane socket: the node hosting it plus its dialable address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EndpointAddr {
    pub node: String,
    pub host: String,
    pub port: u16,
    pub url: String,
}

/// Resolve the concrete `srt://` addresses of a placed stream: the source node's
/// ingress socket a producer dials, and each destination node's consumer socket.
///
/// Hosts follow the manifest `network` alias per endpoint; the ingress port is the
/// sender hop's listen port and each output port is its receiver's consumer port
/// (`dest_port + 1`).
///
/// # Errors
/// Returns [`PlacementError`] if a referenced node is unregistered, declares no
/// address for the requested alias, or the path carries an unassigned port.
pub fn stream_endpoints(
    stream: &StreamDefinition,
    path: &Path,
    nodes: &[NodeDescriptor],
) -> Result<StreamEndpoints, PlacementError> {
    let StreamTransport::Srt(source) = &stream.source;
    let sender = path
        .hops
        .first()
        .ok_or_else(|| PlacementError::UnassignedPort(path.stream.clone()))?;
    let ingress_port = hop_port(&sender.ingress, &sender.id)?;
    let ingress = endpoint_addr(&source.node, source.network.as_deref(), ingress_port, nodes)?;

    let mut outputs = Vec::with_capacity(stream.destinations.len());
    for (index, dest) in stream.destinations.iter().enumerate() {
        let StreamTransport::Srt(dest) = dest;
        let receiver = path
            .hops
            .get(index + 1)
            .ok_or_else(|| PlacementError::UnassignedPort(receiver_hop_id(&stream.name, index)))?;
        let consumer = receiver
            .egresses
            .first()
            .ok_or_else(|| PlacementError::UnassignedPort(receiver.id.clone()))?;
        let consumer_port = hop_port(consumer, &receiver.id)?;
        outputs.push(endpoint_addr(
            &dest.node,
            dest.network.as_deref(),
            consumer_port,
            nodes,
        )?);
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

fn hop_port(spec: &SocketSpec, hop_id: &str) -> Result<u16, PlacementError> {
    spec.port
        .ok_or_else(|| PlacementError::UnassignedPort(hop_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{
        HopState, LinkCondition, NodeCapabilities, NodeStatus, PortRange, ResolvedAddr,
    };

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

    fn contribution() -> StreamDefinition {
        StreamDefinition {
            name: "contribution".to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                node: "strom-node-1".to_string(),
                network: None,
                latency: Some(200),
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                node: "strom-node-2".to_string(),
                network: None,
                latency: Some(1000),
            })],
        }
    }

    fn nodes() -> Vec<NodeDescriptor> {
        vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
        ]
    }

    #[test]
    fn places_sender_and_receiver_by_node() {
        let path = derive_path(&contribution(), &nodes(), &[]).expect("derive");
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
        assert_eq!(receiver.egresses[0].port, Some(dest_port + 1));
    }

    #[test]
    fn receiver_is_placed_on_its_declared_node() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.node = "strom-node-1".to_string();

        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(path.hops[1].node_id, "strom-node-1");
    }

    #[test]
    fn source_on_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        let StreamTransport::Srt(source) = &mut stream.source;
        source.node = "ghost".to_string();

        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::NodeNotRegistered {
                node: "ghost".to_string()
            })
        );
    }

    #[test]
    fn node_ref_destination_resolves_host_and_assigns_port_in_range() {
        let stream = contribution();

        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
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

        let path = derive_path(&stream, &nodes, &[]).expect("derive");
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
            derive_path(&stream, &nodes(), &[]),
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
        dest.node = "strom-node-404".to_string();

        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
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
            derive_path(&stream, &nodes, &[]),
            Err(PlacementError::NoPortRange {
                node: "strom-node-2".to_string(),
            })
        );
    }

    #[test]
    fn assigned_ports_are_deterministic() {
        let range = PortRange {
            start: 7000,
            end: 7999,
        };
        let a = port_in_range(range, "weave-contribution-receiver-0");
        let b = port_in_range(range, "weave-contribution-receiver-0");
        assert_eq!(a, b, "same key maps to same port");
        assert!((7000..=7999).contains(&a));
        assert_ne!(
            a,
            port_in_range(range, "weave-contribution-receiver-1"),
            "distinct keys spread across the range"
        );

        let stream = contribution();
        let first = derive_path(&stream, &nodes(), &[]).expect("derive");
        let second = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(
            first.hops[0].egresses[0].port,
            second.hops[0].egresses[0].port
        );
    }

    #[test]
    fn data_plane_ip_change_on_reregistration_reconverges() {
        let stream = contribution();

        let before = vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
        ];
        let path = derive_path(&stream, &before, &[]).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.10")
        );

        let after = vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.55"),
        ];
        let path = derive_path(&stream, &after, &[]).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.55")
        );
    }

    #[test]
    fn sender_egress_uses_planned_delivery_when_no_resolved_ingress() {
        let path = derive_path(&contribution(), &nodes(), &[]).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.10")
        );
        assert_eq!(path.hops[0].egresses[0].port, path.hops[1].ingress.port);
    }

    #[test]
    fn sender_egress_uses_downstream_resolved_ingress_when_concrete() {
        let observed = vec![HopStatus {
            id: receiver_hop_id("contribution", 0),
            node_id: "strom-node-2".to_string(),
            state: HopState::Provisioned,
            ingress: LinkCondition::Idle,
            egress: LinkCondition::Idle,
            resolved_ingress: Some(ResolvedAddr {
                host: "172.27.0.99".to_string(),
                port: 9002,
            }),
            resolved_egress: None,
            stats: None,
        }];

        let path = derive_path(&contribution(), &nodes(), &observed).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.99")
        );
        assert_eq!(path.hops[0].egresses[0].port, Some(9002));
    }

    #[test]
    fn wildcard_resolved_ingress_falls_back_to_planned_delivery() {
        let observed = vec![HopStatus {
            id: receiver_hop_id("contribution", 0),
            node_id: "strom-node-2".to_string(),
            state: HopState::Provisioned,
            ingress: LinkCondition::Idle,
            egress: LinkCondition::Idle,
            resolved_ingress: Some(ResolvedAddr {
                host: "0.0.0.0".to_string(),
                port: 7002,
            }),
            resolved_egress: None,
            stats: None,
        }];

        let path = derive_path(&contribution(), &nodes(), &observed).expect("derive");
        assert_eq!(
            path.hops[0].egresses[0].host.as_deref(),
            Some("172.27.0.10")
        );
        assert_eq!(path.hops[0].egresses[0].port, path.hops[1].ingress.port);
    }

    #[test]
    fn hop_ids_are_deterministic_and_managed() {
        let a = derive_path(&contribution(), &nodes(), &[]).expect("derive");
        let b = derive_path(&contribution(), &nodes(), &[]).expect("derive");
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
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::NoDestination)
        );
    }

    fn fanout() -> StreamDefinition {
        let mut stream = contribution();
        stream.name = "fanout".to_string();
        stream.destinations = vec![
            StreamTransport::Srt(SrtEndpoint {
                node: "strom-node-2".to_string(),
                network: None,
                latency: Some(1000),
            }),
            StreamTransport::Srt(SrtEndpoint {
                node: "strom-node-1".to_string(),
                network: None,
                latency: Some(1000),
            }),
        ];
        stream
    }

    #[test]
    fn fanout_builds_a_sender_teeing_to_one_receiver_per_destination() {
        let path = derive_path(&fanout(), &nodes(), &[]).expect("derive");
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
        dest.node = "ghost".to_string();
        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::NodeNotRegistered {
                node: "ghost".to_string()
            })
        );
    }

    #[test]
    fn stream_endpoints_resolve_ingress_and_outputs() {
        let stream = contribution();
        let nodes = nodes();
        let path = derive_path(&stream, &nodes, &[]).expect("derive");
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
        let dest_port = path.hops[1].ingress.port.expect("dest port");
        assert_eq!(output.node, "strom-node-2");
        assert_eq!(output.host, "172.27.0.10");
        assert_eq!(output.port, dest_port + 1);
        assert_eq!(output.url, format!("srt://172.27.0.10:{}", dest_port + 1));
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

        let path = derive_path(&stream, &nodes, &[]).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");
        assert_eq!(endpoints.outputs[0].host, "203.0.113.7");
    }

    #[test]
    fn stream_endpoints_on_unregistered_node_error() {
        let stream = contribution();
        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(
            stream_endpoints(&stream, &path, &[]),
            Err(PlacementError::NodeNotRegistered {
                node: "strom-node-1".to_string()
            })
        );
    }
}
