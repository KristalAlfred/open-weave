//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use weave_core::{
    DesiredHop, HOP_ID_PREFIX, HopConditions, HopRole, HopStatus, NodeDescriptor, Path, PathStatus,
    SocketRole, SocketSpec, SrtEndpoint, SrtMode, SrtParams, StreamDefinition, StreamTransport,
    Transport, roll_up_path,
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
    #[error("no node hosts the source")]
    UnplaceableSource,
    #[error("no node hosts the destination")]
    UnplaceableDestination,
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
/// Placement: sender node from `source.node`, else registry host-match on the
/// source URL host; receiver node from the destination the same way. Address
/// wiring: the sender's egress connects to the receiver's reported ingress when
/// resolved to a concrete host, else the static destination host/port.
///
/// Multi-destination fan-out (tee) is not modelled; only the first destination
/// is realised.
pub fn derive_path(
    stream: &StreamDefinition,
    nodes: &[NodeDescriptor],
    observed: &[HopStatus],
) -> Result<Path, PlacementError> {
    let StreamTransport::Srt(source) = &stream.source;
    let StreamTransport::Srt(dest) = stream
        .destinations
        .first()
        .ok_or(PlacementError::NoDestination)?;

    let sender_node = place(source, nodes).ok_or(PlacementError::UnplaceableSource)?;
    let receiver_node = place(dest, nodes).ok_or(PlacementError::UnplaceableDestination)?;

    let (dest_host, dest_port) = split_host_port(&dest.url)?;
    let dest_latency = dest.latency.unwrap_or(DEFAULT_SINK_LATENCY);
    let consumer_port = dest_port
        .checked_add(1)
        .ok_or_else(|| PlacementError::InvalidUrl(dest.url.clone()))?;

    let receiver_id = receiver_hop_id(&stream.name, 0);
    let receiver = DesiredHop {
        id: receiver_id.clone(),
        node_id: receiver_node,
        role: HopRole::Receiver,
        ingress: listen_socket(dest_port, dest_latency),
        egress: listen_socket(consumer_port, RECV_CONSUMER_LATENCY),
    };

    let (egress_host, egress_port) = connect_target(observed, &receiver_id, &dest_host, dest_port);
    let sender = DesiredHop {
        id: sender_hop_id(&stream.name),
        node_id: sender_node,
        role: HopRole::Sender,
        ingress: source_socket(source)?,
        egress: SocketSpec {
            transport: Transport::Srt,
            role: SocketRole::Connect,
            host: Some(egress_host),
            port: Some(egress_port),
            params: SrtParams {
                latency: Some(dest_latency),
            },
        },
    };

    Ok(Path {
        stream: stream.name.clone(),
        enabled: stream.enabled,
        hops: vec![sender, receiver],
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

fn place(endpoint: &SrtEndpoint, nodes: &[NodeDescriptor]) -> Option<String> {
    if let Some(node) = &endpoint.node {
        return Some(node.clone());
    }
    let host = url_host(&endpoint.url)?;
    if is_wildcard_host(&host) {
        return None;
    }
    nodes
        .iter()
        .find(|node| url_host(&node.endpoint).as_deref() == Some(host.as_str()))
        .map(|node| node.id.clone())
}

fn connect_target(
    observed: &[HopStatus],
    downstream_id: &str,
    static_host: &str,
    static_port: u16,
) -> (String, u16) {
    observed
        .iter()
        .find(|status| status.id == downstream_id)
        .and_then(|status| status.resolved_ingress.as_ref())
        .filter(|addr| !is_wildcard_host(&addr.host))
        .map_or_else(
            || (static_host.to_string(), static_port),
            |addr| (addr.host.clone(), addr.port),
        )
}

fn source_socket(source: &SrtEndpoint) -> Result<SocketSpec, PlacementError> {
    let (host, port) = split_host_port(&source.url)?;
    let latency = source.latency.unwrap_or(DEFAULT_SRC_LATENCY);
    Ok(match source.mode {
        SrtMode::Listener => listen_socket(port, latency),
        SrtMode::Caller => SocketSpec {
            transport: Transport::Srt,
            role: SocketRole::Connect,
            host: Some(host),
            port: Some(port),
            params: SrtParams {
                latency: Some(latency),
            },
        },
    })
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

fn url_host(url: &str) -> Option<String> {
    let authority = url
        .rsplit("://")
        .next()?
        .split(['/', '?'])
        .next()
        .unwrap_or_default();
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host);
    (!host.is_empty()).then(|| host.to_string())
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
    use weave_core::{HopState, LinkCondition, NodeCapabilities, NodeStatus, ResolvedAddr};

    fn node(id: &str, host: &str) -> NodeDescriptor {
        NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("http://{host}:8080"),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities::default(),
        }
    }

    fn contribution() -> StreamDefinition {
        StreamDefinition {
            name: "contribution".to_string(),
            enabled: true,
            source: StreamTransport::Srt(SrtEndpoint {
                url: "srt://0.0.0.0:7001".to_string(),
                mode: SrtMode::Listener,
                latency: Some(200),
                node: Some("strom-node-1".to_string()),
            }),
            destinations: vec![StreamTransport::Srt(SrtEndpoint {
                url: "srt://172.27.0.10:7002".to_string(),
                mode: SrtMode::Caller,
                latency: Some(1000),
                node: None,
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
    fn places_sender_by_explicit_node_and_receiver_by_host_match() {
        let path = derive_path(&contribution(), &nodes(), &[]).expect("derive");
        assert_eq!(path.hops.len(), 2);

        let sender = &path.hops[0];
        assert_eq!(sender.role, HopRole::Sender);
        assert_eq!(sender.node_id, "strom-node-1");
        assert_eq!(sender.ingress.role, SocketRole::Listen);
        assert_eq!(sender.ingress.port, Some(7001));
        assert_eq!(sender.egress.role, SocketRole::Connect);
        assert_eq!(sender.egress.host.as_deref(), Some("172.27.0.10"));
        assert_eq!(sender.egress.port, Some(7002));

        let receiver = &path.hops[1];
        assert_eq!(receiver.role, HopRole::Receiver);
        assert_eq!(receiver.node_id, "strom-node-2");
        assert_eq!(receiver.ingress.port, Some(7002));
        assert_eq!(receiver.egress.port, Some(7003));
    }

    #[test]
    fn explicit_node_wins_over_host_match() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.node = Some("strom-node-1".to_string());

        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(path.hops[1].node_id, "strom-node-1");
    }

    #[test]
    fn caller_source_places_by_host_match() {
        let mut stream = contribution();
        stream.source = StreamTransport::Srt(SrtEndpoint {
            url: "srt://172.26.0.10:7001".to_string(),
            mode: SrtMode::Caller,
            latency: None,
            node: None,
        });

        let path = derive_path(&stream, &nodes(), &[]).expect("derive");
        assert_eq!(path.hops[0].node_id, "strom-node-1");
        assert_eq!(path.hops[0].ingress.role, SocketRole::Connect);
        assert_eq!(path.hops[0].ingress.host.as_deref(), Some("172.26.0.10"));
    }

    #[test]
    fn wildcard_listener_source_without_node_is_unplaceable() {
        let mut stream = contribution();
        let StreamTransport::Srt(source) = &mut stream.source;
        source.node = None;

        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::UnplaceableSource)
        );
    }

    #[test]
    fn unmatched_destination_host_is_unplaceable() {
        let mut stream = contribution();
        let StreamTransport::Srt(dest) = &mut stream.destinations[0];
        dest.url = "srt://10.9.9.9:7002".to_string();

        assert_eq!(
            derive_path(&stream, &nodes(), &[]),
            Err(PlacementError::UnplaceableDestination)
        );
    }

    #[test]
    fn sender_egress_uses_static_destination_when_no_resolved_ingress() {
        let path = derive_path(&contribution(), &nodes(), &[]).expect("derive");
        assert_eq!(path.hops[0].egress.host.as_deref(), Some("172.27.0.10"));
        assert_eq!(path.hops[0].egress.port, Some(7002));
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
        assert_eq!(path.hops[0].egress.host.as_deref(), Some("172.27.0.99"));
        assert_eq!(path.hops[0].egress.port, Some(9002));
    }

    #[test]
    fn wildcard_resolved_ingress_falls_back_to_static_destination() {
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
        assert_eq!(path.hops[0].egress.host.as_deref(), Some("172.27.0.10"));
        assert_eq!(path.hops[0].egress.port, Some(7002));
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
}
