//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use weave_core::{
    DEFAULT_DATA_PLANE_ALIAS, DesiredHop, HOP_ID_PREFIX, HopConditions, HopRole, HopStatus,
    NodeDescriptor, Path, PathStatus, PortRange, SocketRole, SocketSpec, SrtEndpoint, SrtMode,
    SrtParams, StreamDefinition, StreamTransport, Transport, roll_up_path,
};

const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;
const RECV_CONSUMER_LATENCY: u32 = 200;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlacementError {
    #[error("stream has no destinations")]
    NoDestination,
    #[error("invalid srt url: {0}")]
    InvalidUrl(String),
    #[error("port {0} leaves no room for a consumer port")]
    PortOverflow(u16),
    #[error("source references no node")]
    UnplaceableSource,
    #[error("destination references no node")]
    UnplaceableDestination,
    #[error("node {node} declares no data-plane address for alias {alias}")]
    UnknownAlias { node: String, alias: String },
    #[error("node {node} declares no assignable port range")]
    NoPortRange { node: String },
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
/// destination's `node`. Delivery addresses resolve at planning time — a raw URL
/// pins host+port, otherwise the receiver node's data-plane alias supplies the
/// host and a port is assigned deterministically from the node's declared range.
/// A sender egress connects to its receiver's reported ingress when resolved to a
/// concrete host, else the planned delivery address.
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
    let sender_node = source
        .node
        .clone()
        .ok_or(PlacementError::UnplaceableSource)?;

    let mut sender_egresses = Vec::with_capacity(stream.destinations.len());
    let mut receivers = Vec::with_capacity(stream.destinations.len());

    for (index, dest) in stream.destinations.iter().enumerate() {
        let StreamTransport::Srt(dest) = dest;
        let receiver_node = dest
            .node
            .clone()
            .ok_or(PlacementError::UnplaceableDestination)?;
        let receiver_id = receiver_hop_id(&stream.name, index);

        let (dest_host, dest_port) = resolve_delivery(
            dest,
            &receiver_node,
            &receiver_id,
            nodes,
            PlacementError::UnplaceableDestination,
        )?;
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
    Ok(match source.mode {
        SrtMode::Listener => {
            let port = resolve_listen_port(source, node_id, hop_id, nodes)?;
            listen_socket(port, latency)
        }
        SrtMode::Caller => {
            let (host, port) = resolve_delivery(
                source,
                node_id,
                hop_id,
                nodes,
                PlacementError::UnplaceableSource,
            )?;
            connect_socket(host, port, latency)
        }
    })
}

/// Resolve the concrete `(host, port)` a peer uses to reach this endpoint: a raw
/// URL pins both, otherwise the node's data-plane alias supplies the host and a
/// deterministic port is assigned from the node's declared range.
fn resolve_delivery(
    endpoint: &SrtEndpoint,
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
    unplaceable: PlacementError,
) -> Result<(String, u16), PlacementError> {
    if let Some(url) = &endpoint.url {
        return split_host_port(url);
    }
    let node = find_node(nodes, node_id).ok_or(unplaceable)?;
    let alias = endpoint
        .network
        .as_deref()
        .unwrap_or(DEFAULT_DATA_PLANE_ALIAS);
    let host = node
        .capabilities
        .data_plane
        .get(alias)
        .cloned()
        .ok_or_else(|| PlacementError::UnknownAlias {
            node: node_id.to_string(),
            alias: alias.to_string(),
        })?;
    Ok((host, assign_port(node, hop_id)?))
}

/// A listener needs only a port: from a raw URL if pinned, else assigned from the
/// node's declared range.
fn resolve_listen_port(
    endpoint: &SrtEndpoint,
    node_id: &str,
    hop_id: &str,
    nodes: &[NodeDescriptor],
) -> Result<u16, PlacementError> {
    if let Some(url) = &endpoint.url {
        return Ok(split_host_port(url)?.1);
    }
    let node = find_node(nodes, node_id).ok_or(PlacementError::UnplaceableSource)?;
    assign_port(node, hop_id)
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

fn split_host_port(url: &str) -> Result<(String, u16), PlacementError> {
    let authority = url
        .strip_prefix("srt://")
        .unwrap_or(url)
        .split(['?', '/'])
        .next()
        .unwrap_or_default();
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| PlacementError::InvalidUrl(url.to_string()))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| PlacementError::InvalidUrl(url.to_string()))?;
    Ok((host.to_string(), port))
}

fn is_wildcard_host(host: &str) -> bool {
    matches!(host, "" | "0.0.0.0" | "::" | "[::]")
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
                url: Some("srt://0.0.0.0:7001".to_string()),
                mode: SrtMode::Listener,
                latency: Some(200),
                node: Some("strom-node-1".to_string()),
                network: None,
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                url: Some("srt://172.27.0.10:7002".to_string()),
                mode: SrtMode::Caller,
                latency: Some(1000),
                node: Some("strom-node-2".to_string()),
                network: None,
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
        assert_eq!(sender.ingress.port, Some(7001));
        assert_eq!(sender.egresses.len(), 1);
        assert_eq!(sender.egresses[0].role, SocketRole::Connect);
        assert_eq!(sender.egresses[0].host.as_deref(), Some("172.27.0.10"));
        assert_eq!(sender.egresses[0].port, Some(7002));

        let receiver = &path.hops[1];
        assert_eq!(receiver.role, HopRole::Receiver);
        assert_eq!(receiver.node_id, "strom-node-2");
        assert_eq!(receiver.ingress.port, Some(7002));
        assert_eq!(receiver.egresses[0].port, Some(7003));
    }

    #[test]
    fn receiver_is_placed_on_its_declared_node() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.node = Some("strom-node-1".to_string());

        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(path.hops[1].node_id, "strom-node-1");
    }

    #[test]
    fn caller_source_places_by_node_and_connects_to_its_url() {
        let mut stream = contribution();
        stream.source = StreamTransport::Srt(SrtEndpoint {
            url: Some("srt://172.26.0.10:7001".to_string()),
            mode: SrtMode::Caller,
            latency: None,
            node: Some("strom-node-1".to_string()),
            network: None,
        });

        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(path.hops[0].node_id, "strom-node-1");
        assert_eq!(path.hops[0].ingress.role, SocketRole::Connect);
        assert_eq!(path.hops[0].ingress.host.as_deref(), Some("172.26.0.10"));
    }

    #[test]
    fn listener_source_without_node_is_unplaceable() {
        let mut stream = contribution();
        let StreamTransport::Srt(source) = &mut stream.source;
        source.node = None;

        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::UnplaceableSource)
        );
    }

    #[test]
    fn destination_without_node_is_unplaceable() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.node = None;

        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::UnplaceableDestination)
        );
    }

    #[test]
    fn node_ref_destination_resolves_host_and_assigns_port_in_range() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.url = None;

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
        dest.url = None;
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
        dest.url = None;
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
    fn node_ref_destination_on_unregistered_node_is_unplaceable() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.url = None;
        dest.node = Some("strom-node-404".to_string());

        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::UnplaceableDestination)
        );
    }

    #[test]
    fn node_ref_destination_without_port_range_is_rejected() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.url = None;

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
    fn raw_url_pin_overrides_node_data_plane() {
        // strom-node-2 advertises 172.27.0.10 but the manifest pins a raw host.
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.url = Some("srt://10.0.0.99:9999".to_string());

        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(path.hops[0].egresses[0].host.as_deref(), Some("10.0.0.99"));
        assert_eq!(path.hops[0].egresses[0].port, Some(9999));
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

        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.url = None;
        let first = derive_path(&stream, &nodes(), &[]).expect("derive");
        let second = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(
            first.hops[0].egresses[0].port,
            second.hops[0].egresses[0].port
        );
    }

    #[test]
    fn data_plane_ip_change_on_reregistration_reconverges() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.url = None;

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
        assert_eq!(path.hops[0].egresses[0].port, Some(7002));
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
        assert_eq!(path.hops[0].egresses[0].port, Some(7002));
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
                url: Some("srt://172.27.0.10:7002".to_string()),
                mode: SrtMode::Caller,
                latency: Some(1000),
                node: Some("strom-node-2".to_string()),
                network: None,
            }),
            StreamTransport::Srt(SrtEndpoint {
                url: Some("srt://172.26.0.10:7002".to_string()),
                mode: SrtMode::Caller,
                latency: Some(1000),
                node: Some("strom-node-1".to_string()),
                network: None,
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
        dest.node = None;
        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::UnplaceableDestination)
        );
    }
}
