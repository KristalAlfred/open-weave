//! Pure derivation of a per-stream [`Path`] from operator intent and observed state.

use std::collections::{HashMap, HashSet};

use weave_core::{
    DesiredEgress, DesiredHop, DestinationEndpoint, DeviceKind, EndpointAddr, HOP_ID_PREFIX,
    HopConditions, HopRole, HopState, HopStatus, NetworkAttachment, NodeDescriptor, NodeStatus,
    Passphrase, Path, PathStatus, PortRange, RemoteAddr, SignallingEndpoint, SignallingTransport,
    SocketRole, SocketSpec, SrtParams, SrtSocket, StreamDefinition, StreamEndpoints,
    StreamTransport, Transport, roll_up_path,
};

use crate::keys::LinkKeys;

const DEFAULT_SRC_LATENCY: u32 = 200;
const DEFAULT_SINK_LATENCY: u32 = 1000;
const RECV_CONSUMER_LATENCY: u32 = 200;
/// AES key length, in bytes, set on every keyed SRT socket.
const SRT_PBKEYLEN: u8 = 32;

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
    #[error(
        "hop {hop} carries a {} socket where a WHIP or WHEP listener is needed",
        socket_end_description(.socket)
    )]
    NotASignallingListener { hop: String, socket: SocketSpec },
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
    #[error("node {node} has no hop profile that merges a second path")]
    NoMergeProfile { node: String },
    #[error("a remote destination has no receiver to merge a second path")]
    NoMergeReceiver,
    #[error("a {kind} endpoint cannot be {end}")]
    WrongEnd {
        kind: &'static str,
        end: &'static str,
    },
    #[error("hop id {hop} is already planned for stream {stream}")]
    HopIdTaken { hop: String, stream: String },
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
    format!("{}{position}", bridge_hop_id_prefix(stream, destination))
}

fn bridge_hop_id_prefix(stream: &str, destination: &str) -> String {
    format!("{HOP_ID_PREFIX}{stream}-bridge-{destination}-")
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
    /// A WHIP ingest or WHEP playback listener an outside peer calls.
    Signalling(SignallingTransport),
}

/// A manifest endpoint as the planner reads it, whichever variant wrote it.
struct Endpoint<'a> {
    placement: Placement<'a>,
    network: Option<&'a str>,
    via: &'a [String],
    latency: Option<u32>,
    passphrase: Option<&'a Passphrase>,
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
            passphrase: endpoint.passphrase.as_ref(),
            terminal: Terminal::Srt,
        }),
        StreamTransport::Device(endpoint) => Ok(Endpoint {
            placement: Placement::Node(&endpoint.node),
            network: endpoint.network.as_deref(),
            via: &[],
            latency: None,
            passphrase: None,
            terminal: Terminal::Device,
        }),
        StreamTransport::Whip(endpoint) => Ok(signalled(endpoint, SignallingTransport::Whip)),
        StreamTransport::Whep(endpoint) => Ok(signalled(endpoint, SignallingTransport::Whep)),
    }
}

fn signalled(endpoint: &SignallingEndpoint, transport: SignallingTransport) -> Endpoint<'_> {
    Endpoint {
        placement: Placement::Node(&endpoint.node),
        network: endpoint.network.as_deref(),
        via: &[],
        latency: None,
        passphrase: None,
        terminal: Terminal::Signalling(transport),
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

    /// Whether `node` has a free port for each of `listeners`, taken in turn.
    fn can_claim(&self, node: &NodeDescriptor, listeners: &[NetworkAttachment]) -> bool {
        let mut trial = Self {
            used: self
                .used
                .get(&node.id)
                .map(|ports| HashMap::from([(node.id.clone(), ports.clone())]))
                .unwrap_or_default(),
        };
        listeners
            .iter()
            .all(|attachment| trial.claim(node, attachment, "").is_ok())
    }
}

/// The hop ids placed streams hold in one tick, each with the stream holding it.
///
/// Hop ids join names with `-`, so two streams can spell the same one, and an
/// id names a flow on its node and the key of the link feeding it.
#[derive(Debug, Default)]
pub struct HopIds {
    held: HashMap<String, String>,
}

impl HopIds {
    /// Hold every hop id of `path` for its stream, or fail naming the first one
    /// another stream holds. Holds nothing on failure.
    pub fn claim(&mut self, path: &Path) -> Result<(), PlacementError> {
        if let Some((hop, stream)) = path
            .hops
            .iter()
            .find_map(|hop| self.held.get(&hop.id).map(|stream| (&hop.id, stream)))
        {
            return Err(PlacementError::HopIdTaken {
                hop: hop.clone(),
                stream: stream.clone(),
            });
        }
        self.held.extend(
            path.hops
                .iter()
                .map(|hop| (hop.id.clone(), path.stream.clone())),
        );
        Ok(())
    }
}

/// A hop id a stream can plan on any nodes: one exact id, or a bridge id with
/// any position after the prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HopIdForm {
    Exact(String),
    Bridge(String),
}

impl HopIdForm {
    fn shared_id(&self, other: &Self) -> Option<String> {
        match (self, other) {
            (Self::Exact(left), Self::Exact(right)) => (left == right).then(|| left.clone()),
            (Self::Exact(id), Self::Bridge(prefix)) | (Self::Bridge(prefix), Self::Exact(id)) => id
                .strip_prefix(prefix.as_str())
                .is_some_and(is_bridge_position)
                .then(|| id.clone()),
            (Self::Bridge(left), Self::Bridge(right)) => {
                (left == right).then(|| format!("{left}0"))
            }
        }
    }
}

/// Whether `text` is a position as [`bridge_hop_id`] writes one.
fn is_bridge_position(text: &str) -> bool {
    text.parse::<usize>()
        .is_ok_and(|position| position.to_string() == text)
}

fn hop_id_forms(stream: &StreamDefinition) -> Vec<HopIdForm> {
    let mut forms = vec![HopIdForm::Exact(sender_hop_id(&stream.name))];
    for destination in &stream.destinations {
        if destination.node().is_some() {
            forms.push(HopIdForm::Exact(receiver_hop_id(
                &stream.name,
                &destination.id,
            )));
        }
        forms.push(HopIdForm::Bridge(bridge_hop_id_prefix(
            &stream.name,
            &destination.id,
        )));
        if destination.paths > 1 {
            forms.push(HopIdForm::Bridge(bridge_hop_id_prefix(
                &stream.name,
                &second_path_branch_id(&destination.id),
            )));
        }
    }
    forms
}

/// A hop id both streams can plan, whatever nodes each lands on.
#[must_use]
pub fn shared_hop_id(left: &StreamDefinition, right: &StreamDefinition) -> Option<String> {
    let right_forms = hop_id_forms(right);
    hop_id_forms(left)
        .iter()
        .find_map(|form| right_forms.iter().find_map(|other| form.shared_id(other)))
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
/// Every SRT link between two hops carries the key `keys` derives for the hop
/// it feeds, on both of its sockets. A terminal SRT socket carries the
/// passphrase its manifest endpoint declares, or none.
///
/// Fan-out is one sender hop teeing to one egress per destination, each with its
/// own chain. Placement is all-or-nothing: if any destination is unplaceable the
/// whole derivation fails and the stream stays pending.
///
/// A destination asking for two paths gets its second once every first path is
/// placed, see [`place_second_path`]. A second path that cannot be placed leaves
/// the stream placed with one and is reported in [`PlannedStream::single_path`].
pub fn derive_stream(
    stream: &StreamDefinition,
    nodes: &[NodeDescriptor],
    observed: &[HopStatus],
    committed_ports: &mut PortAllocator,
    keys: &LinkKeys,
) -> Result<PlannedStream, PlacementError> {
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
        avoid: Vec::new(),
    };

    let mut sender_egresses = Vec::with_capacity(stream.destinations.len());
    let mut downstream = Vec::new();
    let mut first_paths = Vec::new();
    let mut single_path = Vec::new();
    let mut relays = RelayCache::new(observed);
    let mut branches = Vec::with_capacity(stream.destinations.len());

    let mut destinations: Vec<_> = stream.destinations.iter().collect();
    destinations.sort_by_key(|destination| {
        (
            !relays.carries(&stream.name, &destination.id),
            &destination.id,
        )
    });
    for destination in destinations {
        let branch_id = destination.id.clone();
        let mut branch_egresses = Vec::new();
        let dest = read_endpoint(&destination.endpoint)?;
        let latency = dest.latency.unwrap_or(DEFAULT_SINK_LATENCY);

        let chain = chain_hops(
            &stream.name,
            &destination.id,
            &source_station,
            &dest,
            nodes,
            &ports,
            &mut relays,
        )?;

        // Each link attaches its upstream socket to the hop before it, which is
        // the sender for the first link and the previous chain hop after that.
        let mut hops: Vec<DesiredHop> = Vec::with_capacity(chain.bridges.len() + 1);
        let mut upstream = LinkEnd {
            station: &source_station,
            hop_id: &sender_id,
        };
        let mut sender_attachments = None;

        for bridge in &chain.bridges {
            let (up_socket, hop, attachments) =
                plan_hop(&upstream, bridge, latency, nodes, &mut ports, keys)?;
            sender_attachments.get_or_insert(attachments.upstream);
            push_egress(&mut branch_egresses, &mut hops, &branch_id, up_socket);
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
                    let id = bridge_hop_id(&stream.name, &destination.id, chain.bridges.len());
                    let keep = relays.running(&id).to_vec();
                    let relay = pick_remote_relay(
                        upstream.station,
                        &remote.network,
                        nodes,
                        &ports,
                        &mut relays,
                        &keep,
                    )?;
                    let bridge = ChainHop {
                        station: Station::relay(&relay),
                        id,
                        role: HopRole::Bridge,
                    };
                    let (up_socket, hop, _) =
                        plan_hop(&upstream, &bridge, latency, nodes, &mut ports, keys)?;
                    push_egress(&mut branch_egresses, &mut hops, &branch_id, up_socket);
                    hops.push(hop);
                }
                let socket = SocketSpec::Srt(SrtSocket::Connect {
                    host: remote.host.clone(),
                    port: remote.port,
                    params: srt_params(latency, dest.passphrase.cloned()),
                });
                push_egress(&mut branch_egresses, &mut hops, &branch_id, socket);
                if destination.paths > 1 {
                    single_path.push(SinglePath {
                        destination: destination.id.clone(),
                        reason: PlacementError::NoMergeReceiver,
                    });
                }
            }
            // The chain ends on a receiver hop, whose remaining egress is the
            // socket the consumer dials or the device the media ends on.
            ChainTerminal::Receiver(receiver) => {
                let (up_socket, mut hop, attachments) =
                    plan_hop(&upstream, receiver, latency, nodes, &mut ports, keys)?;
                push_egress(&mut branch_egresses, &mut hops, &branch_id, up_socket);
                let socket = match dest.terminal {
                    Terminal::Srt => {
                        let key = consumer_key(&receiver.id);
                        let port = claim_port(&hop.node_id, dest.network, &key, nodes, &mut ports)?;
                        SocketSpec::Srt(SrtSocket::Listen {
                            port,
                            params: srt_params(RECV_CONSUMER_LATENCY, dest.passphrase.cloned()),
                        })
                    }
                    Terminal::Device => device_socket(&hop.node_id, DeviceKind::Display, nodes)?,
                    Terminal::Signalling(SignallingTransport::Whep) => signalling_listener(
                        &hop.node_id,
                        dest.network,
                        SignallingTransport::Whep,
                        &receiver.id,
                        nodes,
                    )?,
                    Terminal::Signalling(SignallingTransport::Whip) => {
                        return Err(PlacementError::WrongEnd {
                            kind: "whip",
                            end: "a destination",
                        });
                    }
                };
                hop.egresses.push(DesiredEgress {
                    branch_id: branch_id.clone(),
                    socket,
                });
                if destination.paths > 1 {
                    first_paths.push(FirstPath {
                        destination: destination.id.clone(),
                        latency,
                        sender_attachments: sender_attachments.unwrap_or(attachments.upstream),
                        receiver: receiver.station.clone(),
                        receiver_attachments: attachments.downstream,
                        relays: chain
                            .bridges
                            .iter()
                            .map(|bridge| bridge.station.node_id.clone())
                            .collect(),
                    });
                }
                hops.push(hop);
            }
        }

        branches.push((destination.id.clone(), branch_egresses, hops));
    }
    branches.sort_by(|left, right| left.0.cmp(&right.0));
    for (_, egresses, hops) in branches {
        sender_egresses.extend(egresses);
        downstream.extend(hops);
    }

    let sender = DesiredHop {
        id: sender_id.clone(),
        node_id: sender_node.to_string(),
        profile_id: String::new(),
        role: HopRole::Sender,
        ingress: source_socket(&source, sender_node, &sender_id, nodes, &mut ports)?,
        merge_ingress: None,
        egresses: sender_egresses,
    };

    let mut hops = Vec::with_capacity(1 + downstream.len());
    hops.push(sender);
    hops.extend(downstream);

    for hop in &mut hops {
        hop.profile_id = select_profile(hop, nodes)?;
    }

    let first_path_egresses = hops[0].egresses.len();
    let mut second_paths = Vec::with_capacity(first_paths.len());
    for first in &first_paths {
        match place_second_path(
            &stream.name,
            first,
            &source_station,
            &mut hops,
            nodes,
            &mut ports,
            keys,
            &mut relays,
        ) {
            Ok(bridges) => second_paths.push((first.destination.clone(), bridges)),
            Err(reason) => single_path.push(SinglePath {
                destination: first.destination.clone(),
                reason,
            }),
        }
    }
    hops[0].egresses[first_path_egresses..].sort_by(|left, right| {
        second_path_destination(&left.branch_id).cmp(second_path_destination(&right.branch_id))
    });
    second_paths.sort_by(|left, right| left.0.cmp(&right.0));
    hops.extend(second_paths.into_iter().flat_map(|(_, bridges)| bridges));
    single_path.sort_by(|left, right| left.destination.cmp(&right.destination));
    *committed_ports = ports;

    Ok(PlannedStream {
        path: Path {
            stream: stream.name.clone(),
            enabled: stream.enabled,
            hops,
        },
        single_path,
    })
}

/// [`derive_stream`]'s path alone.
#[cfg(test)]
pub fn derive_path(
    stream: &StreamDefinition,
    nodes: &[NodeDescriptor],
    observed: &[HopStatus],
    committed_ports: &mut PortAllocator,
    keys: &LinkKeys,
) -> Result<Path, PlacementError> {
    derive_stream(stream, nodes, observed, committed_ports, keys).map(|planned| planned.path)
}

/// A placed stream, and the destinations that asked for two paths and got one.
#[derive(Debug, PartialEq, Eq)]
pub struct PlannedStream {
    pub path: Path,
    pub single_path: Vec<SinglePath>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SinglePath {
    pub destination: String,
    pub reason: PlacementError,
}

/// Appended to a destination id to name its second path. `.` is outside the
/// resource id alphabet, so no destination id can end with it.
const SECOND_PATH_SUFFIX: &str = ".2";

/// The branch id a destination's second path carries on the sender and on its
/// bridges.
fn second_path_branch_id(destination: &str) -> String {
    format!("{destination}{SECOND_PATH_SUFFIX}")
}

/// The destination a second path's branch id belongs to.
fn second_path_destination(branch_id: &str) -> &str {
    branch_id
        .strip_suffix(SECOND_PATH_SUFFIX)
        .unwrap_or(branch_id)
}

/// Whether an egress with `branch_id` carries media to `destination`, over its
/// first path or its second.
fn carries_destination(branch_id: &str, destination: &str) -> bool {
    branch_id
        .strip_prefix(destination)
        .is_some_and(|rest| rest.is_empty() || rest == SECOND_PATH_SUFFIX)
}

/// What a destination's first path used, so its second can avoid it.
struct FirstPath {
    destination: String,
    latency: u32,
    sender_attachments: Vec<String>,
    receiver: Station,
    receiver_attachments: Vec<String>,
    relays: Vec<String>,
}

/// Plan a destination's second path: add one more sender egress and the
/// receiver's merge ingress to `hops`, and return the bridges of its own chain.
///
/// The second path uses no relay the first one does, and at the sender and the
/// receiver no attachment the first one may carry its link on. It is planned
/// against a copy of `ports` and changes neither `hops` nor `ports` unless every
/// hop it touches still has a matching profile. The first path's hops, ids and
/// ports are left as they are.
#[allow(clippy::too_many_arguments)]
fn place_second_path<'n>(
    stream: &str,
    first: &FirstPath,
    source: &Station,
    hops: &mut [DesiredHop],
    nodes: &'n [NodeDescriptor],
    committed_ports: &mut PortAllocator,
    keys: &LinkKeys,
    relays: &mut RelayCache<'n>,
) -> Result<Vec<DesiredHop>, PlacementError> {
    let receiver_node = find_node(nodes, &first.receiver.node_id).ok_or_else(|| {
        PlacementError::NodeNotRegistered {
            node: first.receiver.node_id.clone(),
        }
    })?;
    if !receiver_node
        .capabilities
        .hop_profiles
        .iter()
        .any(|profile| profile.merge)
    {
        return Err(PlacementError::NoMergeProfile {
            node: receiver_node.id.clone(),
        });
    }

    let mut ports = committed_ports.clone();
    let branch_id = second_path_branch_id(&first.destination);
    let sender = Station {
        avoid: first.sender_attachments.clone(),
        ..source.clone()
    };
    let receiver = Station {
        avoid: first.receiver_attachments.clone(),
        ..first.receiver.clone()
    };
    let mut stations = Vec::new();
    relay_before(
        &mut stations,
        (stream, &branch_id),
        &sender,
        &receiver,
        nodes,
        &first.relays,
        &ports,
        relays,
    )?;
    let bridges: Vec<ChainHop> = stations
        .into_iter()
        .enumerate()
        .map(|(position, station)| ChainHop {
            station,
            id: bridge_hop_id(stream, &branch_id, position),
            role: HopRole::Bridge,
        })
        .collect();

    let sender_id = sender_hop_id(stream);
    let mut sender_egresses = Vec::with_capacity(1);
    let mut new_hops: Vec<DesiredHop> = Vec::with_capacity(bridges.len());
    let mut upstream = LinkEnd {
        station: &sender,
        hop_id: &sender_id,
    };
    for bridge in &bridges {
        let (up_socket, hop, _) =
            plan_hop(&upstream, bridge, first.latency, nodes, &mut ports, keys)?;
        push_egress(&mut sender_egresses, &mut new_hops, &branch_id, up_socket);
        new_hops.push(hop);
        upstream = LinkEnd {
            station: &bridge.station,
            hop_id: &bridge.id,
        };
    }
    let merge_id = receiver_hop_id(stream, &branch_id);
    let merge_end = LinkEnd {
        station: &receiver,
        hop_id: &merge_id,
    };
    let link = plan_link(
        &upstream,
        &merge_end,
        first.latency,
        nodes,
        &mut ports,
        keys,
    )?;
    push_egress(
        &mut sender_egresses,
        &mut new_hops,
        &branch_id,
        link.upstream,
    );

    let receiver_id = receiver_hop_id(stream, &first.destination);
    let receiver_index = hops
        .iter()
        .position(|hop| hop.id == receiver_id)
        .ok_or_else(|| PlacementError::MissingHop {
            stream: stream.to_string(),
            hop: receiver_id.clone(),
        })?;
    let mut sender_hop = hops[0].clone();
    sender_hop.egresses.extend(sender_egresses);
    let mut receiver_hop = hops[receiver_index].clone();
    receiver_hop.merge_ingress = Some(link.downstream);
    for hop in [&mut sender_hop, &mut receiver_hop]
        .into_iter()
        .chain(new_hops.iter_mut())
    {
        hop.profile_id = select_profile(hop, nodes)?;
    }

    hops[0] = sender_hop;
    hops[receiver_index] = receiver_hop;
    *committed_ports = ports;
    Ok(new_hops)
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

fn pick_remote_relay<'n>(
    upstream: &Station,
    network: &str,
    nodes: &'n [NodeDescriptor],
    ports: &PortAllocator,
    relays: &mut RelayCache<'n>,
    keep: &[String],
) -> Result<String, PlacementError> {
    let candidates = relays
        .reachable_from(upstream, nodes)
        .iter()
        .filter(|(node, _)| {
            node.topology
                .attachments
                .iter()
                .any(|attachment| attachment.network == network && attachment.dial)
        })
        .filter_map(|(node, link)| {
            let ingress_role = role_for_end(link.listener, Listener::Downstream);
            node.capabilities
                .hop_profiles
                .iter()
                .any(|profile| {
                    profile
                        .ingress
                        .offers_transport(link.transport, ingress_role)
                        && profile
                            .egress
                            .offers_transport(Transport::Srt, SocketRole::Connect)
                        && profile.max_egresses.is_none_or(|maximum| maximum >= 1)
                })
                .then(|| {
                    let listeners = srt_listener_at(link, Listener::Downstream)
                        .into_iter()
                        .collect();
                    (*node, listeners)
                })
        });
    match relay_with_ports(candidates, ports, keep) {
        Some(relay) => relay.map(|node| node.id.clone()),
        None => Err(PlacementError::CannotDialNetwork {
            node: upstream.node_id.clone(),
            network: network.to_string(),
        }),
    }
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
                .merge_ingress
                .as_ref()
                .is_none_or(|socket| profile.merge && profile.ingress.matches_socket(socket))
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
    keys: &LinkKeys,
) -> Result<(SocketSpec, DesiredHop, LinkAttachments), PlacementError> {
    let downstream = LinkEnd {
        station: &hop.station,
        hop_id: &hop.id,
    };
    let link = plan_link(upstream, &downstream, latency, nodes, ports, keys)?;
    Ok((
        link.upstream,
        DesiredHop {
            id: hop.id.clone(),
            node_id: hop.station.node_id.clone(),
            profile_id: String::new(),
            role: hop.role,
            ingress: link.downstream,
            merge_ingress: None,
            egresses: Vec::new(),
        },
        link.attachments,
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

/// One node a stream passes through, optionally constrained to one network and
/// kept off some of its attachments.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Station {
    node_id: String,
    network: Option<String>,
    avoid: Vec<String>,
}

impl Station {
    fn relay(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            network: None,
            avoid: Vec::new(),
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
fn chain_hops<'a, 'n>(
    stream: &str,
    destination_id: &str,
    source: &Station,
    dest: &Endpoint<'a>,
    nodes: &'n [NodeDescriptor],
    ports: &PortAllocator,
    relays: &mut RelayCache<'n>,
) -> Result<Chain<'a>, PlacementError> {
    let mut stations: Vec<Station> = Vec::with_capacity(dest.via.len());
    for station in dest.via.iter().map(|id| Station::relay(id)) {
        relay_before(
            &mut stations,
            (stream, destination_id),
            source,
            &station,
            nodes,
            &[],
            ports,
            relays,
        )?;
        stations.push(station);
    }

    let terminal = match dest.placement {
        Placement::Remote(remote) => ChainTerminal::Remote(remote),
        Placement::Node(node_id) => {
            let station = Station {
                node_id: node_id.to_string(),
                network: dest.network.map(str::to_string),
                avoid: Vec::new(),
            };
            relay_before(
                &mut stations,
                (stream, destination_id),
                source,
                &station,
                nodes,
                &[],
                ports,
                relays,
            )?;
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

/// The online nodes an upstream station can link to as a relay, and how, found
/// once per upstream for one stream's planning. Without it every relayed
/// destination rescans every node. Also the nodes that report running each hop,
/// leaving out reports whose state is `failed`.
struct RelayCache<'n> {
    reachable: HashMap<Station, Vec<(&'n NodeDescriptor, LinkChoice)>>,
    running: HashMap<String, Vec<String>>,
}

impl<'n> RelayCache<'n> {
    fn new(observed: &[HopStatus]) -> Self {
        let mut running: HashMap<String, Vec<String>> = HashMap::new();
        for status in observed
            .iter()
            .filter(|status| status.state != HopState::Failed)
        {
            running
                .entry(status.id.clone())
                .or_default()
                .push(status.node_id.clone());
        }
        Self {
            reachable: HashMap::new(),
            running,
        }
    }

    fn running(&self, hop_id: &str) -> &[String] {
        self.running.get(hop_id).map_or(&[], Vec::as_slice)
    }

    /// Whether a node reports running the receiver or first bridge of
    /// `destination`.
    fn carries(&self, stream: &str, destination: &str) -> bool {
        !self
            .running(&receiver_hop_id(stream, destination))
            .is_empty()
            || !self
                .running(&bridge_hop_id(stream, destination, 0))
                .is_empty()
    }

    fn reachable_from(
        &mut self,
        upstream: &Station,
        nodes: &'n [NodeDescriptor],
    ) -> &[(&'n NodeDescriptor, LinkChoice)] {
        self.reachable.entry(upstream.clone()).or_insert_with(|| {
            nodes
                .iter()
                .filter(|node| node.status != NodeStatus::Offline && node.id != upstream.node_id)
                .filter_map(|node| {
                    station_link(upstream, &Station::relay(&node.id), nodes)
                        .ok()
                        .map(|link| (node, link))
                })
                .collect()
        })
    }
}

/// Extend `chain` with a relay when no transport carries the link into `next`
/// from the station before it — the source's own station when the chain is
/// still empty. The relay is none of `avoid_relays`, and is the node that
/// reports running the bridge `(stream, branch)` would place there when that
/// node still qualifies.
///
/// A spliced relay is compatible with both halves by construction, so each
/// resolves under the ordinary rule — the upstream reaches the relay, and the
/// relay reaches the downstream. One pass is enough; no inserted link can itself
/// need a relay.
#[allow(clippy::too_many_arguments)]
fn relay_before<'n>(
    chain: &mut Vec<Station>,
    (stream, branch): (&str, &str),
    source: &Station,
    next: &Station,
    nodes: &'n [NodeDescriptor],
    avoid_relays: &[String],
    ports: &PortAllocator,
    relays: &mut RelayCache<'n>,
) -> Result<(), PlacementError> {
    let upstream = chain.last().unwrap_or(source);
    if let Err(failure) = station_link(upstream, next, nodes) {
        let keep = relays
            .running(&bridge_hop_id(stream, branch, chain.len()))
            .to_vec();
        let relay = pick_relay(
            nodes,
            upstream,
            next,
            failure,
            avoid_relays,
            ports,
            relays,
            &keep,
        )?;
        chain.push(relay);
    }
    Ok(())
}

/// The online relay node that can carry both halves of a link no transport
/// connects directly and has a free port for every SRT listener it would host:
/// one of `keep` when one qualifies, otherwise the lowest-id one. Sorting keeps
/// the choice stable across ticks, and `keep` keeps a bridge on the relay
/// running it when an earlier relay comes back.
///
/// When none qualifies the error names why the direct link failed: two ends that
/// do share a transport but cannot dial each other read as a routing problem,
/// two that share none as a capability problem. When relays qualify but none has
/// the ports, it names the lowest-id one as out of ports.
#[allow(clippy::too_many_arguments)]
fn pick_relay<'n>(
    nodes: &'n [NodeDescriptor],
    upstream: &Station,
    downstream: &Station,
    failure: LinkFailure,
    avoid: &[String],
    ports: &PortAllocator,
    relays: &mut RelayCache<'n>,
    keep: &[String],
) -> Result<Station, PlacementError> {
    let candidates = relays
        .reachable_from(upstream, nodes)
        .iter()
        .filter(|(node, _)| node.id != downstream.node_id && !avoid.contains(&node.id))
        .filter_map(|(node, ingress)| {
            let relay = Station::relay(&node.id);
            let egress = station_link(&relay, downstream, nodes).ok()?;
            let ingress_role = role_for_end(ingress.listener, Listener::Downstream);
            let egress_role = role_for_end(egress.listener, Listener::Upstream);
            node.capabilities
                .hop_profiles
                .iter()
                .any(|profile| {
                    profile
                        .ingress
                        .offers_transport(ingress.transport, ingress_role)
                        && profile
                            .egress
                            .offers_transport(egress.transport, egress_role)
                        && profile.max_egresses.is_none_or(|max| max >= 1)
                })
                .then(|| {
                    let listeners = srt_listener_at(ingress, Listener::Downstream)
                        .into_iter()
                        .chain(srt_listener_at(&egress, Listener::Upstream))
                        .collect();
                    (*node, listeners)
                })
        });
    match relay_with_ports(candidates, ports, keep) {
        Some(relay) => relay.map(|node| Station::relay(&node.id)),
        None => Err(failure.into_error(&upstream.node_id, &downstream.node_id)),
    }
}

/// The attachment a relay listens on for `link` over SRT, when the relay is the
/// link's `end` and SRT carries it. A WHIP or WHEP listener claims no port.
fn srt_listener_at(link: &LinkChoice, end: Listener) -> Option<NetworkAttachment> {
    (link.listener == end && link.transport == Transport::Srt).then(|| link.attachment.clone())
}

/// The candidate with a free port for each SRT listener it would host, one of
/// `keep` before the lowest-id one; `PortRangeExhausted` for the lowest-id one
/// when none has, or `None` when there are no candidates.
fn relay_with_ports<'a>(
    candidates: impl Iterator<Item = (&'a NodeDescriptor, Vec<NetworkAttachment>)>,
    ports: &PortAllocator,
    keep: &[String],
) -> Option<Result<&'a NodeDescriptor, PlacementError>> {
    let mut candidates: Vec<_> = candidates.collect();
    candidates.sort_by(|(left, _), (right, _)| left.id.cmp(&right.id));
    let (lowest, _) = candidates.first()?;
    Some(
        candidates
            .iter()
            .filter(|(node, _)| keep.contains(&node.id))
            .chain(&candidates)
            .find(|(node, listeners)| ports.can_claim(node, listeners))
            .map(|(node, _)| *node)
            .ok_or_else(|| PlacementError::PortRangeExhausted {
                node: lowest.id.clone(),
            }),
    )
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
            .filter(|attachment| !self.station.avoid.contains(&attachment.id))
            .collect();
        attachments
            .sort_by(|left, right| (&left.network, &left.id).cmp(&(&right.network, &right.id)));
        attachments
    }

    /// Ids of the attachments this end dials `network` from, or `None` when it
    /// cannot. A connect socket names no local address, so any dialing
    /// attachment on the network may carry the link, and an end that must avoid
    /// one of them cannot dial it at all.
    fn dialing_attachments(&self, network: &str) -> Option<Vec<String>> {
        let dialing: Vec<_> = self
            .node
            .topology
            .attachments
            .iter()
            .filter(|attachment| attachment.network == network && attachment.dial)
            .filter(|attachment| {
                self.station
                    .network
                    .as_deref()
                    .is_none_or(|constraint| attachment.network == constraint)
            })
            .collect();
        if dialing.is_empty()
            || dialing
                .iter()
                .any(|attachment| self.station.avoid.contains(&attachment.id))
        {
            return None;
        }
        Some(
            dialing
                .into_iter()
                .map(|attachment| attachment.id.clone())
                .collect(),
        )
    }
}

#[derive(Clone)]
struct LinkChoice {
    transport: Transport,
    listener: Listener,
    attachment: NetworkAttachment,
    dialer_attachments: Vec<String>,
}

/// The ids of the attachments each end of a link may carry it on.
struct LinkAttachments {
    upstream: Vec<String>,
    downstream: Vec<String>,
}

/// One planned link: the socket each end owns, and the attachments carrying it.
struct PlannedLink {
    upstream: SocketSpec,
    downstream: SocketSpec,
    attachments: LinkAttachments,
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
                if let Some(dialer_attachments) = dialer.dialing_attachments(&attachment.network) {
                    return Ok(LinkChoice {
                        transport,
                        listener,
                        attachment: attachment.clone(),
                        dialer_attachments,
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

/// Plan one link's socket pair: the upstream egress and the downstream ingress.
///
/// The transport and which end listens come from [`link_transport`]. An SRT
/// listener claims its port on the listening node, always keyed by the
/// downstream hop id so an assignment stays stable when a link's direction is
/// the same across ticks. Both SRT sockets carry the key derived from that same
/// id and the two end nodes, so the key survives a replan and a change of
/// direction, and changes when either end moves to another node. A WebRTC
/// listener claims no port: its socket is signalled at the base its node
/// declares, addressed by the downstream hop id so every link is a distinct
/// endpoint.
fn plan_link(
    upstream: &LinkEnd,
    downstream: &LinkEnd,
    latency: u32,
    nodes: &[NodeDescriptor],
    ports: &mut PortAllocator,
    keys: &LinkKeys,
) -> Result<PlannedLink, PlacementError> {
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
            let ends = [
                upstream.station.node_id.as_str(),
                downstream.station.node_id.as_str(),
            ];
            let params = srt_params(latency, Some(keys.link(downstream.hop_id, ends)));
            (
                SocketSpec::Srt(SrtSocket::Listen {
                    port,
                    params: params.clone(),
                }),
                SocketSpec::Srt(SrtSocket::Connect {
                    host: listener.host.clone(),
                    port,
                    params,
                }),
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
    let listened = vec![choice.attachment.id.clone()];
    Ok(match choice.listener {
        Listener::Downstream => PlannedLink {
            upstream: connect,
            downstream: listen,
            attachments: LinkAttachments {
                upstream: choice.dialer_attachments,
                downstream: listened,
            },
        },
        Listener::Upstream => PlannedLink {
            upstream: listen,
            downstream: connect,
            attachments: LinkAttachments {
                upstream: listened,
                downstream: choice.dialer_attachments,
            },
        },
    })
}

fn srt_params(latency: u32, passphrase: Option<Passphrase>) -> SrtParams {
    SrtParams {
        latency: Some(latency),
        pbkeylen: passphrase.is_some().then_some(SRT_PBKEYLEN),
        passphrase,
    }
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
                .filter(|egress| carries_destination(&egress.branch_id, destination_id))
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
                        branch_status.egresses.retain(|egress| {
                            carries_destination(&egress.branch_id, destination_id)
                        });
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
            .any(|egress| carries_destination(&egress.branch_id, destination_id))
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

/// The sender's ingress: an SRT listener a producer dials, the node's own
/// camera when the source is a `device`, or the WHIP ingest an outside sender
/// calls.
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
            Ok(SocketSpec::Srt(SrtSocket::Listen {
                port,
                params: srt_params(latency, source.passphrase.cloned()),
            }))
        }
        Terminal::Device => device_socket(node_id, DeviceKind::Capture, nodes),
        Terminal::Signalling(SignallingTransport::Whip) => signalling_listener(
            node_id,
            source.network,
            SignallingTransport::Whip,
            hop_id,
            nodes,
        ),
        Terminal::Signalling(SignallingTransport::Whep) => Err(PlacementError::WrongEnd {
            kind: "whep",
            end: "the source",
        }),
    }
}

/// A `transport` listener on `node_id` for an outside peer, addressed by
/// `endpoint_id` at the signalling base of the first attachment, in network
/// then id order, that declares one on `network`.
fn signalling_listener(
    node_id: &str,
    network: Option<&str>,
    transport: SignallingTransport,
    endpoint_id: &str,
    nodes: &[NodeDescriptor],
) -> Result<SocketSpec, PlacementError> {
    let node = find_node(nodes, node_id).ok_or_else(|| PlacementError::NodeNotRegistered {
        node: node_id.to_string(),
    })?;
    if let Some(network) = network
        && !node
            .topology
            .attachments
            .iter()
            .any(|attachment| attachment.network == network)
    {
        return Err(PlacementError::UnknownNetwork {
            node: node.id.clone(),
            network: network.to_string(),
        });
    }
    let mut candidates: Vec<_> = node
        .topology
        .attachments
        .iter()
        .filter(|attachment| network.is_none_or(|network| attachment.network == network))
        .filter_map(|attachment| {
            attachment
                .listeners
                .signalling(transport)
                .map(|base| (attachment, base))
        })
        .collect();
    candidates.sort_by(|(left, _), (right, _)| {
        (&left.network, &left.id).cmp(&(&right.network, &right.id))
    });
    let (_, base) = candidates
        .first()
        .ok_or_else(|| PlacementError::NoSignalling {
            node: node.id.clone(),
            transport: transport.transport(),
        })?;
    Ok(SocketSpec::signalling(
        transport,
        SocketRole::Listen,
        base,
        endpoint_id,
    ))
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
    candidates.sort_by(|left, right| (&left.network, &left.id).cmp(&(&right.network, &right.id)));
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
        Terminal::Signalling(_) => Some(signalling_addr(source_node, &sender.ingress, &sender.id)?),
    };

    let mut destinations = Vec::with_capacity(stream.destinations.len());
    for destination in &stream.destinations {
        let dest = read_endpoint(&destination.endpoint)?;
        let endpoint = match (&dest.placement, dest.terminal) {
            (Placement::Remote(remote), _) => Some(remote_endpoint_addr(remote)),
            (Placement::Node(_), Terminal::Device) => None,
            (Placement::Node(node_id), Terminal::Srt) => {
                let (receiver, consumer) = consumer_socket(path, &destination.id)?;
                let consumer_port = listener_port(consumer, &receiver.id)?;
                Some(endpoint_addr(node_id, dest.network, consumer_port, nodes)?)
            }
            (Placement::Node(node_id), Terminal::Signalling(_)) => {
                let (receiver, consumer) = consumer_socket(path, &destination.id)?;
                Some(signalling_addr(node_id, consumer, &receiver.id)?)
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

/// The receiver hop carrying `destination`, and the socket its consumer uses.
fn consumer_socket<'a>(
    path: &'a Path,
    destination: &str,
) -> Result<(&'a DesiredHop, &'a SocketSpec), PlacementError> {
    let receiver_id = receiver_hop_id(&path.stream, destination);
    let receiver = path
        .hops
        .iter()
        .find(|hop| hop.id == receiver_id)
        .ok_or_else(|| PlacementError::MissingHop {
            stream: path.stream.clone(),
            hop: receiver_id.clone(),
        })?;
    let consumer = receiver
        .egresses
        .first()
        .ok_or_else(|| PlacementError::NoConsumerSocket {
            hop: receiver.id.clone(),
        })?;
    Ok((receiver, &consumer.socket))
}

/// The URL an outside WHIP sender or WHEP player calls to reach `socket`.
fn signalling_addr(
    node_id: &str,
    socket: &SocketSpec,
    hop_id: &str,
) -> Result<EndpointAddr, PlacementError> {
    match socket {
        SocketSpec::Whip(listener) | SocketSpec::Whep(listener)
            if listener.role == SocketRole::Listen =>
        {
            Ok(EndpointAddr {
                node: node_id.to_string(),
                host: None,
                port: None,
                url: listener.url.clone(),
            })
        }
        other => Err(PlacementError::NotASignallingListener {
            hop: hop_id.to_string(),
            socket: other.clone(),
        }),
    }
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
        host: Some(host),
        port: Some(port),
        url,
    })
}

fn remote_endpoint_addr(remote: &RemoteAddr) -> EndpointAddr {
    EndpointAddr {
        node: String::new(),
        host: Some(remote.host.clone()),
        port: Some(remote.port),
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

    fn keys() -> LinkKeys {
        LinkKeys::for_tests()
    }

    fn class(transport: Transport, roles: weave_core::RoleSet) -> HopEndpointClass {
        HopEndpointClass::Transport(TransportClass { transport, roles })
    }

    fn srt_profile(id: &str) -> HopProfile {
        HopProfile {
            id: id.to_string(),
            ingress: class(Transport::Srt, weave_core::RoleSet::both()),
            egress: class(Transport::Srt, weave_core::RoleSet::both()),
            max_egresses: None,
            merge: false,
            accepts: None,
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
            passphrase: None,
            format: None,
            accepts: None,
        })
    }

    fn destination(id: &str, node: &str) -> StreamDestination {
        StreamDestination {
            id: id.to_string(),
            paths: 1,
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
        let left = derive_path(&first, &nodes, &[], &mut PortAllocator::new(), &keys()).unwrap();
        let right = derive_path(&second, &nodes, &[], &mut PortAllocator::new(), &keys()).unwrap();
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
            &keys(),
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
                &keys(),
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
                    merge: false,
                    accepts: None,
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
            merge: false,
            accepts: None,
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
        let path = derive_path(
            &stream,
            &[browser, strom],
            &[],
            &mut PortAllocator::new(),
            &keys(),
        )
        .unwrap();
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
            merge: false,
            accepts: None,
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
            derive_path(&definition, &nodes, &[], &mut PortAllocator::new(), &keys()),
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
            merge: false,
            accepts: None,
        }];
        let error = derive_path(
            &stream(vec![destination("studio", "studio-node")]),
            &nodes,
            &[],
            &mut PortAllocator::new(),
            &keys(),
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
            &keys(),
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
                &keys(),
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
        let path = derive_path(
            &definition,
            &shared_nodes(),
            &[],
            &mut PortAllocator::new(),
            &keys(),
        )
        .unwrap();
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
                merge_ingress: None,
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

    fn srt_params(socket: &SocketSpec) -> &SrtParams {
        match socket {
            SocketSpec::Srt(socket) => socket.params(),
            other => panic!("expected an SRT socket, got {other}"),
        }
    }

    fn link_key(socket: &SocketSpec) -> Option<&str> {
        srt_params(socket)
            .passphrase
            .as_ref()
            .map(Passphrase::expose)
    }

    fn hop<'a>(path: &'a Path, id: &str) -> &'a DesiredHop {
        path.hops
            .iter()
            .find(|hop| hop.id == id)
            .unwrap_or_else(|| panic!("no hop {id}"))
    }

    fn egress<'a>(hop: &'a DesiredHop, branch_id: &str) -> &'a SocketSpec {
        &hop.egresses
            .iter()
            .find(|egress| egress.branch_id == branch_id)
            .unwrap_or_else(|| panic!("no egress {branch_id} on {}", hop.id))
            .socket
    }

    fn plan(definition: &StreamDefinition, nodes: &[NodeDescriptor]) -> Path {
        derive_path(definition, nodes, &[], &mut PortAllocator::new(), &keys()).unwrap()
    }

    #[test]
    fn both_ends_of_a_link_share_a_key_no_other_link_has() {
        let definition = stream(vec![
            destination("preview", "preview-node"),
            destination("studio", "studio-node"),
        ]);
        let path = plan(&definition, &shared_nodes());
        let sender = hop(&path, "weave-feed-sender");
        let mut link_keys = Vec::new();
        for branch in ["preview", "studio"] {
            let receiver = hop(&path, &format!("weave-feed-receiver-{branch}"));
            let key = link_key(egress(sender, branch)).expect("link is keyed");
            assert_eq!(link_key(&receiver.ingress), Some(key));
            assert_eq!(
                key,
                keys()
                    .link(&receiver.id, [&receiver.node_id, "source"])
                    .expose()
            );
            assert_eq!(srt_params(&receiver.ingress).pbkeylen, Some(32));
            assert_eq!(srt_params(egress(sender, branch)).pbkeylen, Some(32));
            link_keys.push(key.to_string());
        }
        assert_ne!(link_keys[0], link_keys[1]);
        assert_eq!(
            path,
            plan(&definition, &shared_nodes()),
            "a replan is identical"
        );
    }

    #[test]
    fn adding_a_destination_leaves_existing_link_keys_alone() {
        let one = plan(
            &stream(vec![destination("studio", "studio-node")]),
            &shared_nodes(),
        );
        let two = plan(
            &stream(vec![
                destination("preview", "preview-node"),
                destination("studio", "studio-node"),
            ]),
            &shared_nodes(),
        );
        let receiver = "weave-feed-receiver-studio";
        assert_eq!(
            link_key(&hop(&one, receiver).ingress),
            link_key(&hop(&two, receiver).ingress)
        );
    }

    #[test]
    fn a_reversed_link_keeps_its_key() {
        let definition = stream(vec![destination("studio", "studio-node")]);
        let forward = plan(&definition, &shared_nodes());
        let mut nodes = shared_nodes();
        nodes[1].topology.attachments = vec![
            attachment("wan", "internet", true, None),
            attachment("lan", "studio-lan", false, Some("10.0.0.5")),
        ];
        let reversed = plan(&definition, &nodes);

        let receiver = "weave-feed-receiver-studio";
        assert!(matches!(
            hop(&reversed, receiver).ingress,
            SocketSpec::Srt(SrtSocket::Connect { .. })
        ));
        assert_eq!(
            link_key(&hop(&reversed, receiver).ingress),
            link_key(&hop(&forward, receiver).ingress)
        );
        assert_eq!(
            link_key(egress(hop(&reversed, "weave-feed-sender"), "studio")),
            link_key(&hop(&forward, receiver).ingress)
        );
    }

    #[test]
    fn a_link_rekeys_when_either_end_moves_to_another_node() {
        let mut nodes = shared_nodes();
        for (id, host) in [("relay-a", "192.0.2.4"), ("relay-b", "192.0.2.5")] {
            nodes.push(node(
                id,
                vec![attachment("wan", "internet", true, Some(host))],
            ));
        }
        let via = |relay: &str| {
            let mut definition = stream(vec![destination("studio", "studio-node")]);
            let StreamTransport::Srt(endpoint) = &mut definition.destinations[0].endpoint else {
                unreachable!()
            };
            endpoint.via = vec![relay.to_string()];
            plan(&definition, &nodes)
        };
        let (a, b) = (via("relay-a"), via("relay-b"));
        let bridge = "weave-feed-bridge-studio-0";
        let receiver = "weave-feed-receiver-studio";
        assert_eq!(hop(&a, bridge).node_id, "relay-a");
        assert_eq!(hop(&b, bridge).node_id, "relay-b");
        assert_ne!(
            link_key(&hop(&a, bridge).ingress),
            link_key(&hop(&b, bridge).ingress),
            "the link into the relay"
        );
        assert_ne!(
            link_key(&hop(&a, receiver).ingress),
            link_key(&hop(&b, receiver).ingress),
            "the link out of the relay"
        );
        assert_eq!(
            link_key(egress(hop(&b, "weave-feed-sender"), "studio")),
            link_key(&hop(&b, bridge).ingress)
        );
        assert_eq!(
            link_key(egress(hop(&b, bridge), "studio")),
            link_key(&hop(&b, receiver).ingress)
        );

        let direct = plan(&stream(vec![destination("studio", "studio-node")]), &nodes);
        let moved = plan(&stream(vec![destination("studio", "preview-node")]), &nodes);
        assert_eq!(hop(&moved, receiver).node_id, "preview-node");
        assert_ne!(
            link_key(&hop(&moved, receiver).ingress),
            link_key(&hop(&direct, receiver).ingress)
        );
    }

    #[test]
    fn terminal_sockets_carry_the_manifest_passphrase_or_none() {
        let keyed = |endpoint: &mut StreamTransport, passphrase: &str| {
            let StreamTransport::Srt(endpoint) = endpoint else {
                unreachable!()
            };
            endpoint.passphrase = Some(Passphrase::new(passphrase));
        };
        let mut definition = stream(vec![destination("studio", "studio-node")]);
        let path = plan(&definition, &shared_nodes());
        assert_eq!(link_key(&path.hops[0].ingress), None);
        assert_eq!(srt_params(&path.hops[0].ingress).pbkeylen, None);
        let receiver = hop(&path, "weave-feed-receiver-studio");
        assert_eq!(link_key(egress(receiver, "studio")), None);

        keyed(&mut definition.source, "producer-passphrase");
        keyed(
            &mut definition.destinations[0].endpoint,
            "consumer-passphrase",
        );
        definition.destinations.push(StreamDestination {
            id: "uplink".to_string(),
            paths: 1,
            endpoint: StreamTransport::Srt(SrtEndpoint {
                node: None,
                remote: Some(RemoteAddr {
                    host: "198.51.100.5".to_string(),
                    port: 9000,
                    network: "internet".to_string(),
                }),
                via: Vec::new(),
                network: None,
                latency: None,
                passphrase: Some(Passphrase::new("remote-passphrase")),
                format: None,
                accepts: None,
            }),
        });
        let path = plan(&definition, &shared_nodes());
        let sender = hop(&path, "weave-feed-sender");
        assert_eq!(link_key(&sender.ingress), Some("producer-passphrase"));
        assert_eq!(srt_params(&sender.ingress).pbkeylen, Some(32));
        assert_eq!(
            link_key(egress(sender, "uplink")),
            Some("remote-passphrase")
        );
        let receiver = hop(&path, "weave-feed-receiver-studio");
        assert_eq!(
            link_key(egress(receiver, "studio")),
            Some("consumer-passphrase")
        );
        assert_eq!(
            link_key(&receiver.ingress),
            Some(
                keys()
                    .link(&receiver.id, ["source", "studio-node"])
                    .expose()
            ),
            "the manifest passphrase does not replace the link key"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{
        DeviceClass, EgressStatus, HopEndpointClass, HopProfile, HopState, LinkCondition,
        NetworkListeners, NodeCapabilities, NodeEndpoint, NodeTopology, ResolvedAddr, RoleSet,
        SignallingListener, SignallingTransport, SocketStatus, SrtEndpoint, SrtListener,
        StreamDestination, TransportClass,
    };

    const SHARED: &str = "internet";

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

    fn class(transport: Transport, roles: RoleSet) -> HopEndpointClass {
        HopEndpointClass::Transport(TransportClass { transport, roles })
    }

    fn device(kind: DeviceKind) -> HopEndpointClass {
        HopEndpointClass::Device(DeviceClass { device: kind })
    }

    fn profile(
        id: &str,
        ingress: HopEndpointClass,
        egress: HopEndpointClass,
        max_egresses: Option<usize>,
    ) -> HopProfile {
        HopProfile {
            id: id.to_string(),
            ingress,
            egress,
            max_egresses,
            merge: false,
            accepts: None,
        }
    }

    fn srt_forward() -> HopProfile {
        profile(
            "srt-forward",
            class(Transport::Srt, RoleSet::both()),
            class(Transport::Srt, RoleSet::both()),
            None,
        )
    }

    fn srt_listener(host: &str, start: u16, end: u16) -> NetworkListeners {
        NetworkListeners {
            srt: Some(SrtListener {
                host: host.to_string(),
                port_range: PortRange { start, end },
            }),
            whip: None,
            whep: None,
        }
    }

    fn attachment(
        id: &str,
        network: &str,
        dial: bool,
        listeners: NetworkListeners,
    ) -> NetworkAttachment {
        NetworkAttachment {
            id: id.to_string(),
            network: network.to_string(),
            dial,
            listeners,
        }
    }

    fn node_with(id: &str, attachments: Vec<NetworkAttachment>) -> NodeDescriptor {
        NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("http://{id}:8080"),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                adapters: Vec::new(),
                hop_profiles: vec![srt_forward()],
            },
            topology: NodeTopology { attachments },
        }
    }

    /// A node every peer on the shared network can dial.
    fn node(id: &str, host: &str) -> NodeDescriptor {
        node_with(
            id,
            vec![attachment(
                "wan",
                SHARED,
                true,
                srt_listener(host, 7000, 7999),
            )],
        )
    }

    /// A node behind NAT: it dials the shared network but no peer can dial it.
    /// Its SRT listener sits on its own site network, where local producers and
    /// consumers reach it.
    fn nat_node(id: &str, host: &str) -> NodeDescriptor {
        node_with(
            id,
            vec![
                attachment("outbound", SHARED, true, NetworkListeners::default()),
                attachment(
                    "site",
                    &format!("{id}-site"),
                    true,
                    srt_listener(host, 7000, 7999),
                ),
            ],
        )
    }

    fn node_with_range(id: &str, start: u16, end: u16) -> NodeDescriptor {
        node_with(
            id,
            vec![attachment(
                "wan",
                SHARED,
                true,
                srt_listener("10.0.0.1", start, end),
            )],
        )
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
            passphrase: None,
        }
    }

    fn remote_dest() -> SrtEndpoint {
        SrtEndpoint {
            node: None,
            remote: Some(RemoteAddr {
                host: "198.51.100.5".to_string(),
                port: 9000,
                network: SHARED.to_string(),
            }),
            via: Vec::new(),
            format: None,
            accepts: None,
            network: None,
            latency: Some(800),
            passphrase: None,
        }
    }

    fn dest(id: &str, endpoint: SrtEndpoint) -> StreamDestination {
        StreamDestination {
            id: id.to_string(),
            paths: 1,
            endpoint: StreamTransport::Srt(endpoint),
        }
    }

    fn contribution() -> StreamDefinition {
        StreamDefinition {
            name: "contribution".to_string(),
            enabled: true,
            source: StreamTransport::Srt(node_ref("strom-node-1", 200)),
            destinations: vec![dest("studio", node_ref("strom-node-2", 1000))],
        }
    }

    fn srt_dest(stream: &mut StreamDefinition, index: usize) -> &mut SrtEndpoint {
        match &mut stream.destinations[index].endpoint {
            StreamTransport::Srt(endpoint) => endpoint,
            _ => unreachable!("fixture endpoint is srt"),
        }
    }

    fn srt_source(stream: &mut StreamDefinition) -> &mut SrtEndpoint {
        match &mut stream.source {
            StreamTransport::Srt(endpoint) => endpoint,
            _ => unreachable!("fixture endpoint is srt"),
        }
    }

    fn nodes() -> Vec<NodeDescriptor> {
        vec![
            node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
        ]
    }

    /// `nodes`, with node-2 also listening on a second network node-1 can dial.
    fn nodes_with_a_public_network() -> Vec<NodeDescriptor> {
        let mut nodes = nodes();
        nodes[0].topology.attachments.push(attachment(
            "public",
            "public",
            true,
            NetworkListeners::default(),
        ));
        nodes[1].topology.attachments.push(attachment(
            "public",
            "public",
            true,
            srt_listener("203.0.113.7", 7000, 7999),
        ));
        nodes
    }

    fn derive(stream: &StreamDefinition, nodes: &[NodeDescriptor]) -> Result<Path, PlacementError> {
        derive_path(
            stream,
            nodes,
            &[],
            &mut PortAllocator::new(),
            &LinkKeys::for_tests(),
        )
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
        srt_dest(&mut stream, 0).node = Some("strom-node-1".to_string());

        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(path.hops[1].node_id, "strom-node-1");
    }

    #[test]
    fn source_on_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        srt_source(&mut stream).node = Some("ghost".to_string());

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
    fn node_ref_destination_resolves_named_network() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).network = Some("public".to_string());

        let path = derive(&stream, &nodes_with_a_public_network()).expect("derive");
        assert_eq!(host(&path.hops[0].egresses[0]), Some("203.0.113.7"));
    }

    #[test]
    fn node_ref_destination_with_unknown_network_is_rejected() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).network = Some("mgmt".to_string());

        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::UnknownNetwork {
                node: "strom-node-2".to_string(),
                network: "mgmt".to_string(),
            })
        );
    }

    #[test]
    fn node_ref_destination_on_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).node = Some("strom-node-404".to_string());

        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::NodeNotRegistered {
                node: "strom-node-404".to_string()
            })
        );
    }

    #[test]
    fn node_ref_destination_without_an_srt_listener_is_rejected() {
        let stream = contribution();

        let node2 = node_with(
            "strom-node-2",
            vec![attachment("wan", SHARED, true, NetworkListeners::default())],
        );
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
        let wan = &node.topology.attachments[0];
        let a = PortAllocator::new()
            .claim(&node, wan, "weave-x")
            .expect("claim");
        let b = PortAllocator::new()
            .claim(&node, wan, "weave-x")
            .expect("claim");
        assert_eq!(a, b, "same key alone maps to the same port");
        assert!((7000..=7999).contains(&a));

        // Distinct keys on one allocator never collide.
        let mut ports = PortAllocator::new();
        let x = ports
            .claim(&node, wan, "weave-contribution-receiver-0")
            .unwrap();
        let y = ports
            .claim(&node, wan, "weave-contribution-receiver-1")
            .unwrap();
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
        let wan = &node.topology.attachments[0];
        let mut ports = PortAllocator::new();
        let a = ports.claim(&node, wan, &first).expect("claim first");
        let b = ports.claim(&node, wan, &second).expect("claim second");
        assert_ne!(a, b, "linear probe yields a distinct port on collision");
        assert!((7000..=7001).contains(&a) && (7000..=7001).contains(&b));
    }

    #[test]
    fn allocator_errors_when_range_is_exhausted() {
        let node = node_with_range("n", 7000, 7000);
        let wan = &node.topology.attachments[0];
        let mut ports = PortAllocator::new();
        assert_eq!(ports.claim(&node, wan, "a").expect("first claim"), 7000);
        assert_eq!(
            ports.claim(&node, wan, "b"),
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
    fn listener_host_change_on_reregistration_reconverges() {
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
    fn sender_egress_uses_planned_delivery_without_a_reported_ingress_address() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        assert_eq!(host(&path.hops[0].egresses[0]), Some("172.27.0.10"));
        assert_eq!(
            srt(&path.hops[0].egresses[0]).port(),
            srt(&path.hops[1].ingress).port()
        );
    }

    #[test]
    fn sender_egress_ignores_a_reported_ingress_address_even_for_a_named_network() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).network = Some("public".to_string());
        let nodes = nodes_with_a_public_network();

        // A reported ingress address on the shared-network host must not
        // rewrite the planned delivery on the named network.
        let observed = vec![HopStatus {
            id: receiver_hop_id("contribution", "studio"),
            node_id: "strom-node-2".to_string(),
            state: HopState::Provisioned,
            ingress: SocketStatus {
                condition: LinkCondition::Idle,
                resolved: Some(ResolvedAddr {
                    host: "172.27.0.10".to_string(),
                    port: 9002,
                }),
                stats: None,
            },
            merge_ingress: None,
            egresses: vec![EgressStatus {
                branch_id: "studio".to_string(),
                status: SocketStatus {
                    condition: LinkCondition::Idle,
                    resolved: None,
                    stats: None,
                },
            }],
        }];

        let path = derive_path(
            &stream,
            &nodes,
            &observed,
            &mut PortAllocator::new(),
            &LinkKeys::for_tests(),
        )
        .expect("derive");
        assert_eq!(
            host(&path.hops[0].egresses[0]),
            Some("203.0.113.7"),
            "keeps the named network's host"
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
        assert_eq!(a.hops[1].id, "weave-contribution-receiver-studio");
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
        stream.destinations = vec![dest("uplink", remote_dest())];

        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(path.hops.len(), 1, "only the sender hop is placed");
        let sender = &path.hops[0];
        assert_eq!(sender.egresses.len(), 1);
        assert_eq!(srt(&sender.egresses[0]).role(), SocketRole::Connect);
        assert_eq!(host(&sender.egresses[0]), Some("198.51.100.5"));
        assert_eq!(srt(&sender.egresses[0]).port(), 9000);

        let endpoints = stream_endpoints(&stream, &path, &nodes()).expect("endpoints");
        assert_eq!(endpoints.destinations.len(), 1);
        let output = addr(&endpoints.destinations[0].endpoint);
        assert_eq!(output.url, "srt://198.51.100.5:9000");
        assert!(output.node.is_empty());
    }

    #[test]
    fn mixed_node_and_remote_destinations() {
        let mut stream = contribution();
        stream.destinations = vec![
            dest("studio", node_ref("strom-node-2", 1000)),
            dest("uplink", remote_dest()),
        ];

        let path = derive(&stream, &nodes()).expect("derive");
        assert_eq!(
            path.hops.len(),
            2,
            "sender plus one receiver for the node dest"
        );
        let sender = &path.hops[0];
        assert_eq!(sender.egresses.len(), 2, "one egress per destination");
        assert_eq!(sender.egresses[1].branch_id, "uplink");
        assert_eq!(host(&sender.egresses[1]), Some("198.51.100.5"));
        assert_eq!(path.hops[1].id, receiver_hop_id("contribution", "studio"));

        let endpoints = stream_endpoints(&stream, &path, &nodes()).expect("endpoints");
        assert_eq!(endpoints.destinations.len(), 2);
        assert_eq!(
            addr(&endpoints.destinations[0].endpoint).node,
            "strom-node-2"
        );
        assert_eq!(
            addr(&endpoints.destinations[1].endpoint).url,
            "srt://198.51.100.5:9000"
        );
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
        stream.destinations = vec![dest(
            "studio",
            SrtEndpoint {
                node: None,
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: None,
                passphrase: None,
            },
        )];
        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::EndpointPlacement)
        );
    }

    fn fanout() -> StreamDefinition {
        let mut stream = contribution();
        stream.name = "fanout".to_string();
        stream.destinations = vec![
            dest("studio", node_ref("strom-node-2", 1000)),
            dest("venue", node_ref("strom-node-1", 1000)),
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
        assert_eq!(sender.egresses[0].branch_id, "studio");
        assert_eq!(sender.egresses[1].branch_id, "venue");
        assert_eq!(host(&sender.egresses[0]), Some("172.27.0.10"));
        assert_eq!(host(&sender.egresses[1]), Some("172.26.0.10"));

        let studio = &path.hops[1];
        assert_eq!(studio.id, "weave-fanout-receiver-studio");
        assert_eq!(studio.role, HopRole::Receiver);
        assert_eq!(studio.node_id, "strom-node-2");
        assert_eq!(studio.egresses[0].branch_id, "studio");

        let venue = &path.hops[2];
        assert_eq!(venue.id, "weave-fanout-receiver-venue");
        assert_eq!(venue.role, HopRole::Receiver);
        assert_eq!(
            venue.node_id, "strom-node-1",
            "second destination is co-located with the source node"
        );
        assert_eq!(venue.egresses[0].branch_id, "venue");
    }

    #[test]
    fn fanout_status_checks_every_branch_and_the_reporting_node() {
        let path = derive(&fanout(), &nodes()).expect("derive");
        let mut observed: Vec<HopStatus> = path
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
                merge_ingress: None,
                egresses: hop
                    .egresses
                    .iter()
                    .map(|egress| EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: SocketStatus {
                            condition: LinkCondition::Flowing,
                            resolved: None,
                            stats: None,
                        },
                    })
                    .collect(),
            })
            .collect();

        assert_eq!(path_status(&path, &observed), PathStatus::Flowing);
        observed[0].egresses[1].status.condition = LinkCondition::Connecting;
        assert_eq!(path_status(&path, &observed), PathStatus::Degraded);

        observed[0].egresses[1].status.condition = LinkCondition::Flowing;
        observed[0].node_id = "wrong-node".to_string();
        assert_eq!(path_status(&path, &observed), PathStatus::Pending);
    }

    #[test]
    fn fanout_is_all_or_nothing_when_a_destination_is_unplaceable() {
        let mut stream = fanout();
        srt_dest(&mut stream, 1).node = Some("ghost".to_string());
        assert_eq!(
            derive(&stream, &nodes()),
            Err(PlacementError::NodeNotRegistered {
                node: "ghost".to_string()
            })
        );
    }

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
            "it dials the source node's listener"
        );
        assert_eq!(srt(&receiver.ingress).port(), srt(sender_egress).port());
    }

    #[test]
    fn reversed_link_claims_its_port_on_the_listening_node() {
        // Disjoint ranges make the owning node legible from the port alone: an
        // egress in 7xxx was claimed on node-1, in 8xxx on node-2.
        let mut nat = nat_node("strom-node-2", "172.27.0.10");
        nat.topology.attachments[1].listeners = srt_listener("172.27.0.10", 8000, 8999);
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
            node("edge-relay", "198.51.100.9"),
        ];

        let path = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(path.hops.len(), 3, "sender, bridge, receiver");

        let sender = &path.hops[0];
        let bridge = &path.hops[1];
        let receiver = &path.hops[2];

        assert_eq!(bridge.role, HopRole::Bridge);
        assert_eq!(bridge.node_id, "edge-relay");
        assert_eq!(bridge.id, "weave-contribution-bridge-studio-0");

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
        let mut bystander = node("bystander", "198.51.100.9");
        bystander.capabilities.hop_profiles.clear();
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            // Dialable, but offers no hop profile to carry the transit.
            bystander,
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
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            nat_node("edge-relay", "198.51.100.9"),
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
            node("relay-b", "198.51.100.20"),
            node("relay-a", "198.51.100.10"),
        ];

        let first = derive(&contribution(), &nodes).expect("derive");
        let second = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(first.hops[1].node_id, "relay-a");
        assert_eq!(first, second, "re-derivation is stable");
    }

    #[test]
    fn an_offline_relay_is_passed_over_for_a_healthy_one() {
        let mut lost = node("relay-a", "198.51.100.10");
        lost.status = NodeStatus::Offline;
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            lost,
            node("relay-b", "198.51.100.20"),
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
        let mut lost = node("relay-a", "198.51.100.10");
        lost.status = NodeStatus::Offline;
        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            node("relay-c", "198.51.100.30"),
            lost,
            node("relay-b", "198.51.100.20"),
        ];

        let first = derive(&contribution(), &nodes).expect("derive");
        let second = derive(&contribution(), &nodes).expect("derive");
        assert_eq!(first.hops[1].node_id, "relay-b");
        assert_eq!(first, second, "re-derivation is stable");
    }

    /// A NAT'd source fanned out to three NAT'd receivers, and a public relay
    /// per id in `relays` whose SRT range holds four ports: two relayed
    /// destinations, since the relay listens on both halves of each.
    fn relayed_fan_out(relays: &[&str]) -> (StreamDefinition, Vec<NodeDescriptor>) {
        let mut nodes = vec![nat_node("strom-node-1", "172.26.0.10")];
        let mut stream = contribution();
        stream.destinations.clear();
        for receiver in ["a", "b", "c"] {
            let node_id = format!("rx-{receiver}");
            nodes.push(nat_node(&node_id, "192.168.0.10"));
            stream
                .destinations
                .push(dest(receiver, node_ref(&node_id, 1000)));
        }
        for (index, relay) in relays.iter().enumerate() {
            nodes.push(node_with(
                relay,
                vec![attachment(
                    "wan",
                    SHARED,
                    true,
                    srt_listener(&format!("198.51.100.{}", 10 + index), 7000, 7003),
                )],
            ));
        }
        (stream, nodes)
    }

    #[test]
    fn a_relayed_fan_out_moves_on_to_the_next_relay_when_one_is_out_of_ports() {
        let (stream, nodes) = relayed_fan_out(&["relay-a", "relay-b"]);
        let path = derive(&stream, &nodes).expect("derive");
        let bridges: Vec<_> = path
            .hops
            .iter()
            .filter(|hop| hop.role == HopRole::Bridge)
            .map(|hop| (hop.id.as_str(), hop.node_id.as_str()))
            .collect();
        assert_eq!(
            bridges,
            [
                ("weave-contribution-bridge-a-0", "relay-a"),
                ("weave-contribution-bridge-b-0", "relay-a"),
                ("weave-contribution-bridge-c-0", "relay-b"),
            ]
        );
        assert_eq!(derive(&stream, &nodes), Ok(path), "stable across ticks");
    }

    #[test]
    fn a_relayed_fan_out_no_relay_has_ports_for_names_the_full_relay() {
        let (stream, nodes) = relayed_fan_out(&["relay-a"]);
        assert_eq!(
            derive(&stream, &nodes),
            Err(PlacementError::PortRangeExhausted {
                node: "relay-a".to_string(),
            })
        );
    }

    #[test]
    fn an_offline_relay_alone_leaves_the_pair_unplaceable() {
        let mut lost = node("edge-relay", "198.51.100.9");
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

    fn bench_nodes() -> Vec<NodeDescriptor> {
        let routed = |host| srt_listener(host, 20_000, 20_999);
        vec![
            node_with(
                "strom-node-1",
                vec![
                    attachment("a-routed", SHARED, true, routed("10.97.26.10")),
                    attachment("z-docker-host", "docker-host", true, routed("10.97.26.10")),
                ],
            ),
            node_with(
                "strom-node-2",
                vec![attachment("routed", SHARED, true, routed("10.97.27.10"))],
            ),
            node_with(
                "strom-node-3",
                vec![
                    attachment("outbound", SHARED, true, NetworkListeners::default()),
                    attachment("site", "node-3-local", true, routed("10.97.29.10")),
                ],
            ),
            node_with(
                "strom-node-4",
                vec![
                    attachment("outbound", SHARED, true, NetworkListeners::default()),
                    attachment("site", "node-4-local", true, routed("10.97.30.10")),
                ],
            ),
        ]
    }

    fn assert_bridged_through(path: &Path, relay: &str, relay_host: &str) {
        let [sender, bridge, receiver] = path.hops.as_slice() else {
            panic!("expected sender, bridge and receiver, got {:?}", path.hops);
        };
        assert_eq!(
            (sender.role, sender.node_id.as_str()),
            (HopRole::Sender, "strom-node-3")
        );
        assert_eq!(
            (bridge.role, bridge.node_id.as_str()),
            (HopRole::Bridge, relay)
        );
        assert_eq!(
            (receiver.role, receiver.node_id.as_str()),
            (HopRole::Receiver, "strom-node-4")
        );

        assert_eq!(srt(&sender.ingress).role(), SocketRole::Listen);
        assert_eq!(host(&sender.egresses[0]), Some(relay_host));
        assert_eq!(srt(&bridge.ingress).role(), SocketRole::Listen);
        assert_eq!(srt(&bridge.ingress).port(), srt(&sender.egresses[0]).port());
        assert_eq!(srt(&bridge.egresses[0]).role(), SocketRole::Listen);
        assert_eq!(host(&receiver.ingress), Some(relay_host));
        assert_eq!(
            srt(&receiver.ingress).port(),
            srt(&bridge.egresses[0]).port()
        );
        assert_eq!(srt(&receiver.egresses[0]).role(), SocketRole::Listen);
    }

    #[test]
    fn bench_nat_sites_bridge_through_the_lowest_online_node_both_dial() {
        let stream = StreamDefinition {
            name: "nat-transit".to_string(),
            enabled: true,
            source: StreamTransport::Srt(node_ref("strom-node-3", 200)),
            destinations: vec![dest("output", node_ref("strom-node-4", 1000))],
        };
        let mut nodes = bench_nodes();
        let path = derive(&stream, &nodes).expect("derive");
        assert_bridged_through(&path, "strom-node-1", "10.97.26.10");

        nodes[0].status = NodeStatus::Offline;
        let path = derive(&stream, &nodes).expect("derive");
        assert_bridged_through(&path, "strom-node-2", "10.97.27.10");
    }

    #[test]
    fn via_pins_a_bridge_on_an_otherwise_direct_link() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).via = vec!["edge-relay".to_string()];

        let mut nodes = nodes();
        nodes.push(node("edge-relay", "198.51.100.9"));

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
    fn via_chains_multiple_relays_in_order() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).via = vec!["relay-first".to_string(), "relay-second".to_string()];

        let mut nodes = nodes();
        nodes.push(node("relay-first", "198.51.100.10"));
        nodes.push(node("relay-second", "198.51.100.20"));

        let path = derive(&stream, &nodes).expect("derive");
        assert_eq!(path.hops.len(), 4);
        assert_eq!(path.hops[1].node_id, "relay-first");
        assert_eq!(path.hops[2].node_id, "relay-second");
        assert_eq!(path.hops[3].node_id, "strom-node-2");
        assert_eq!(path.hops[1].id, "weave-contribution-bridge-studio-0");
        assert_eq!(path.hops[2].id, "weave-contribution-bridge-studio-1");
        assert_eq!(
            host(&path.hops[1].egresses[0]),
            Some("198.51.100.20"),
            "each bridge dials the next"
        );
    }

    #[test]
    fn via_relays_out_to_a_remote_destination() {
        let mut stream = contribution();
        let mut uplink = remote_dest();
        uplink.via = vec!["edge-relay".to_string()];
        stream.destinations = vec![dest("uplink", uplink)];

        let mut nodes = nodes();
        nodes.push(node("edge-relay", "198.51.100.9"));

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
        assert_eq!(
            addr(&endpoints.destinations[0].endpoint).url,
            "srt://198.51.100.5:9000"
        );
    }

    #[test]
    fn a_pinned_via_still_gets_a_relay_when_its_own_link_is_undialable() {
        // node-1 and the pinned transit node are both outbound-only, so the link
        // between them needs a relay of its own on top of the pin.
        let mut stream = contribution();
        srt_dest(&mut stream, 0).via = vec!["transit".to_string()];

        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
            nat_node("transit", "172.28.0.10"),
            node("edge-relay", "198.51.100.9"),
        ];

        let path = derive(&stream, &nodes).expect("derive");
        let via_nodes: Vec<&str> = path.hops[1..].iter().map(|h| h.node_id.as_str()).collect();
        assert_eq!(via_nodes, vec!["edge-relay", "transit", "strom-node-2"]);
    }

    #[test]
    fn fanout_relays_only_the_destination_that_needs_it() {
        let mut stream = fanout();
        stream.destinations = vec![
            dest("studio", node_ref("strom-node-2", 1000)),
            dest("truck", node_ref("nat-node", 1000)),
        ];

        let nodes = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            node("strom-node-2", "172.27.0.10"),
            nat_node("nat-node", "172.28.0.10"),
            node("edge-relay", "198.51.100.9"),
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
                ("weave-fanout-receiver-studio", "strom-node-2"),
                ("weave-fanout-bridge-truck-0", "edge-relay"),
                ("weave-fanout-receiver-truck", "nat-node"),
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
        srt_source(&mut stream).via = vec!["edge-relay".to_string()];
        assert_eq!(derive(&stream, &nodes()), Err(PlacementError::SourceVia));
    }

    #[test]
    fn via_to_an_unregistered_node_is_not_registered() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).via = vec!["ghost".to_string()];

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
        srt_dest(&mut stream, 0).via = vec!["edge-relay".to_string()];

        let mut nodes = nodes();
        nodes.push(node("edge-relay", "198.51.100.9"));

        let a = derive(&stream, &nodes).expect("derive");
        let b = derive(&stream, &nodes).expect("derive");
        assert_eq!(a, b);
        assert!(weave_core::is_managed_hop_id(&a.hops[1].id));
    }

    #[test]
    fn stream_endpoints_are_unchanged_by_an_intervening_bridge() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).via = vec!["edge-relay".to_string()];

        let mut nodes = nodes();
        nodes.push(node("edge-relay", "198.51.100.9"));

        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");

        assert_eq!(addr(&endpoints.ingress).node, "strom-node-1");
        assert_eq!(
            addr(&endpoints.ingress).port,
            Some(srt(&path.hops[0].ingress).port())
        );
        assert_eq!(endpoints.destinations.len(), 1);
        assert_eq!(
            addr(&endpoints.destinations[0].endpoint).node,
            "strom-node-2",
            "the consumer still attaches at the destination, not the relay"
        );
        let consumer_port = srt(&path.hops[2].egresses[0]).port();
        assert_eq!(
            addr(&endpoints.destinations[0].endpoint).port,
            Some(consumer_port)
        );
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
        assert_eq!(ingress.host.as_deref(), Some("172.26.0.10"));
        assert_eq!(ingress.port, Some(ingress_port));
        assert_eq!(ingress.url, format!("srt://172.26.0.10:{ingress_port}"));

        assert_eq!(endpoints.destinations.len(), 1);
        assert_eq!(endpoints.destinations[0].id, "studio");
        let output = addr(&endpoints.destinations[0].endpoint);
        let consumer_port = srt(&path.hops[1].egresses[0]).port();
        assert_eq!(output.node, "strom-node-2");
        assert_eq!(output.host.as_deref(), Some("172.27.0.10"));
        assert_eq!(output.port, Some(consumer_port));
        assert_eq!(output.url, format!("srt://172.27.0.10:{consumer_port}"));
    }

    #[test]
    fn stream_endpoints_follow_named_network() {
        let mut stream = contribution();
        srt_dest(&mut stream, 0).network = Some("public".to_string());
        let nodes = nodes_with_a_public_network();

        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");
        assert_eq!(
            addr(&endpoints.destinations[0].endpoint).host.as_deref(),
            Some("203.0.113.7")
        );
    }

    fn plan(stream: &StreamDefinition, nodes: &[NodeDescriptor]) -> Path {
        derive_stream(
            stream,
            nodes,
            &[],
            &mut PortAllocator::new(),
            &LinkKeys::for_tests(),
        )
        .expect("derive")
        .path
    }

    #[test]
    fn every_planned_hop_id_has_a_form_its_stream_declares() {
        let nat_pair = vec![
            nat_node("strom-node-1", "172.26.0.10"),
            nat_node("strom-node-2", "172.27.0.10"),
            node("edge-relay", "198.51.100.9"),
        ];

        let mut pinned = contribution();
        srt_dest(&mut pinned, 0).via = vec!["relay-first".to_string(), "relay-second".to_string()];
        let mut pinned_nodes = nodes();
        pinned_nodes.push(node("relay-first", "198.51.100.10"));
        pinned_nodes.push(node("relay-second", "198.51.100.20"));

        let mut uplink = contribution();
        let mut remote = remote_dest();
        remote.via = vec!["edge-relay".to_string()];
        uplink.destinations = vec![dest("uplink", remote)];
        let mut uplink_nodes = nodes();
        uplink_nodes.push(node("edge-relay", "198.51.100.9"));

        let mut redundant = contribution();
        redundant.destinations[0].paths = 2;
        let two_uplinks = |id: &str, host: &str| {
            node_with(
                id,
                vec![
                    attachment("out-a", "internet-a", true, NetworkListeners::default()),
                    attachment("out-b", "internet-b", true, NetworkListeners::default()),
                    attachment(
                        "site",
                        &format!("{id}-site"),
                        true,
                        srt_listener(host, 7000, 7999),
                    ),
                ],
            )
        };
        let mut merging = two_uplinks("strom-node-2", "172.27.0.10");
        let mut merge = srt_forward();
        merge.id = "srt-merge".to_string();
        merge.merge = true;
        merging.capabilities.hop_profiles.push(merge);
        let redundant_nodes = vec![
            two_uplinks("strom-node-1", "172.26.0.10"),
            merging,
            node_with(
                "relay-a",
                vec![attachment(
                    "wan",
                    "internet-a",
                    true,
                    srt_listener("10.0.0.2", 7000, 7999),
                )],
            ),
            node_with(
                "relay-b",
                vec![attachment(
                    "wan",
                    "internet-b",
                    true,
                    srt_listener("10.1.0.2", 7000, 7999),
                )],
            ),
        ];

        let mut planned = Vec::new();
        for (stream, nodes) in [
            (contribution(), nodes()),
            (contribution(), nat_pair),
            (pinned, pinned_nodes),
            (uplink, uplink_nodes),
            (redundant, redundant_nodes),
        ] {
            let path = plan(&stream, &nodes);
            planned.extend(path.hops.iter().map(|hop| (stream.clone(), hop.id.clone())));
        }
        assert!(
            planned.iter().any(|(_, id)| id.contains(".2-")),
            "a second path's bridge is among the planned ids"
        );

        for (stream, id) in planned {
            assert!(
                hop_id_forms(&stream)
                    .iter()
                    .any(|form| form.shared_id(&HopIdForm::Exact(id.clone())).is_some()),
                "{} planned {id}, which none of its forms spells",
                stream.name
            );
        }
    }

    #[test]
    fn a_bridge_form_matches_only_positions_as_bridge_hop_id_writes_them() {
        let form = HopIdForm::Bridge(bridge_hop_id_prefix("x", "a"));
        for position in [0, 1, 10] {
            let id = bridge_hop_id("x", "a", position);
            assert_eq!(form.shared_id(&HopIdForm::Exact(id.clone())), Some(id));
        }
        for id in [
            "weave-x-bridge-a-",
            "weave-x-bridge-a-01",
            "weave-x-bridge-a-+1",
            "weave-x-bridge-a-1-0",
        ] {
            assert_eq!(
                form.shared_id(&HopIdForm::Exact(id.to_string())),
                None,
                "{id}"
            );
        }
        assert_eq!(
            form.shared_id(&HopIdForm::Bridge(bridge_hop_id_prefix("x", "a-1"))),
            None
        );
    }

    /// Node 2 listens on two networks whose order by network differs from their
    /// order by attachment id; node 1 dials both.
    fn nodes_with_two_srt_listeners() -> Vec<NodeDescriptor> {
        let mut nodes = nodes();
        nodes[0].topology.attachments = vec![
            attachment("lan", "alpha", true, NetworkListeners::default()),
            attachment("wan", "zeta", true, srt_listener("172.26.0.10", 7000, 7999)),
        ];
        nodes[1].topology.attachments = vec![
            attachment("a-site", "zeta", true, srt_listener("10.9.0.2", 7000, 7099)),
            attachment("z-lan", "alpha", true, srt_listener("10.1.0.2", 8000, 8099)),
        ];
        nodes
    }

    /// Whether `node` has one SRT listener at `host` whose range holds `port`.
    fn listens_at(node: &NodeDescriptor, host: &str, port: u16) -> bool {
        node.topology.attachments.iter().any(|attachment| {
            attachment.listeners.srt.as_ref().is_some_and(|srt| {
                srt.host == host && (srt.port_range.start..=srt.port_range.end).contains(&port)
            })
        })
    }

    #[test]
    fn every_srt_address_handed_out_pairs_a_listener_host_with_its_own_range() {
        let nodes = nodes_with_two_srt_listeners();
        let stream = contribution();
        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");
        let (source, studio) = (&nodes[0], &nodes[1]);

        let SocketSpec::Srt(SrtSocket::Connect { host, port, .. }) =
            &path.hops[0].egresses[0].socket
        else {
            panic!("the sender dials the receiver");
        };
        assert_eq!(srt(&path.hops[1].ingress).port(), *port);
        assert!(
            listens_at(studio, host, *port),
            "the link dials {host}:{port}"
        );

        let consumer = addr(&endpoints.destinations[0].endpoint);
        let (consumer_host, consumer_port) =
            (consumer.host.as_deref().unwrap(), consumer.port.unwrap());
        assert_eq!(srt(&path.hops[1].egresses[0]).port(), consumer_port);
        assert!(listens_at(studio, consumer_host, consumer_port));

        let producer = addr(&endpoints.ingress);
        let (producer_host, producer_port) =
            (producer.host.as_deref().unwrap(), producer.port.unwrap());
        assert_eq!(srt(&path.hops[0].ingress).port(), producer_port);
        assert!(listens_at(source, producer_host, producer_port));
    }

    #[test]
    fn link_and_consumer_endpoint_pick_the_same_attachment() {
        let nodes = nodes_with_two_srt_listeners();
        let stream = contribution();
        let path = derive(&stream, &nodes).expect("derive");
        let endpoints = stream_endpoints(&stream, &path, &nodes).expect("endpoints");

        let link_host = host(&path.hops[0].egresses[0]).expect("sender dials the receiver");
        assert_eq!(link_host, "10.1.0.2", "the link takes the lowest network");
        let consumer = addr(&endpoints.destinations[0].endpoint);
        assert_eq!(
            consumer.host.as_deref(),
            Some(link_host),
            "the consumer endpoint sits on the attachment the link chose"
        );
        assert!(
            consumer
                .port
                .is_some_and(|port| (8000..=8099).contains(&port))
        );
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

    /// A web page: pushes WHIP from its camera, pulls WHEP to its screen, and
    /// dials out with no listener of its own.
    fn browser_node(id: &str) -> NodeDescriptor {
        NodeDescriptor {
            id: id.to_string(),
            endpoint: format!("browser://{id}"),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                adapters: Vec::new(),
                hop_profiles: vec![
                    profile(
                        "camera-to-whip",
                        device(DeviceKind::Capture),
                        class(Transport::Whip, RoleSet::only(SocketRole::Connect)),
                        Some(1),
                    ),
                    profile(
                        "whep-to-display",
                        class(Transport::Whep, RoleSet::only(SocketRole::Connect)),
                        device(DeviceKind::Display),
                        Some(1),
                    ),
                ],
            },
            topology: NodeTopology {
                attachments: vec![attachment(
                    "client",
                    SHARED,
                    true,
                    NetworkListeners::default(),
                )],
            },
        }
    }

    /// A Strom node that also hosts WHIP ingest and WHEP playback. The bases it
    /// declares are its own to choose, so they are nothing the planner could
    /// have guessed.
    fn webrtc_node(id: &str, host: &str) -> NodeDescriptor {
        let mut node = node(id, host);
        node.capabilities.hop_profiles.extend([
            profile(
                "whip-to-srt",
                class(Transport::Whip, RoleSet::only(SocketRole::Listen)),
                class(Transport::Srt, RoleSet::both()),
                None,
            ),
            profile(
                "srt-to-whep",
                class(Transport::Srt, RoleSet::both()),
                class(Transport::Whep, RoleSet::only(SocketRole::Listen)),
                None,
            ),
        ]);
        let listeners = listeners_of(&mut node);
        listeners.whip = Some(SignallingListener {
            base_url: format!("http://{host}:8080/ingest"),
        });
        listeners.whep = Some(SignallingListener {
            base_url: format!("http://{host}:8080/playback"),
        });
        node
    }

    fn listeners_of(node: &mut NodeDescriptor) -> &mut NetworkListeners {
        &mut node.topology.attachments[0].listeners
    }

    fn device_ref(id: &str) -> StreamTransport {
        StreamTransport::Device(NodeEndpoint {
            node: id.to_string(),
            network: None,
        })
    }

    fn device_dest(id: &str, node: &str) -> StreamDestination {
        StreamDestination {
            id: id.to_string(),
            paths: 1,
            endpoint: device_ref(node),
        }
    }

    fn alice_cam() -> StreamDefinition {
        StreamDefinition {
            name: "alice-cam".to_string(),
            enabled: true,
            source: device_ref("browser-a1b2"),
            destinations: vec![dest("studio", node_ref("strom-node-2", 1000))],
        }
    }

    fn alice_return() -> StreamDefinition {
        StreamDefinition {
            name: "alice-return".to_string(),
            enabled: true,
            source: StreamTransport::Srt(node_ref("strom-node-2", 200)),
            destinations: vec![device_dest("guest", "browser-a1b2")],
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
            "weave-alice-cam-receiver-studio",
        );

        let sender = &path.hops[0];
        assert_eq!(sender.node_id, "browser-a1b2");
        assert_eq!(
            sender.ingress,
            SocketSpec::Device(DeviceKind::Capture),
            "the media starts at the camera"
        );
        assert_eq!(sender.egresses.len(), 1);
        assert_eq!(sender.egresses[0].branch_id, "studio");
        assert_eq!(
            sender.egresses[0].socket,
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Connect,
                "http://172.27.0.10:8080/ingest",
                "weave-alice-cam-receiver-studio",
            )
        );

        let receiver = &path.hops[1];
        assert_eq!(receiver.node_id, "strom-node-2");
        assert_eq!(receiver.ingress, signalled, "Strom hosts the ingest");
        assert_eq!(srt(&receiver.egresses[0]).role(), SocketRole::Listen);
        assert!((7000..=7999).contains(&srt(&receiver.egresses[0]).port()));

        let endpoints = stream_endpoints(&alice_cam(), &path, &webrtc_nodes()).expect("endpoints");
        assert_eq!(endpoints.ingress, None, "a camera has nothing to dial");
        assert_eq!(endpoints.destinations.len(), 1);
        assert_eq!(
            addr(&endpoints.destinations[0].endpoint).node,
            "strom-node-2"
        );
    }

    #[test]
    fn strom_to_device_receiver_is_whep_hosted_on_strom() {
        let path = derive(&alice_return(), &webrtc_nodes()).expect("derive");
        assert_eq!(path.hops.len(), 2);
        let signalled = SocketSpec::signalling(
            SignallingTransport::Whep,
            SocketRole::Listen,
            "http://172.27.0.10:8080/playback",
            "weave-alice-return-receiver-guest",
        );

        let sender = &path.hops[0];
        assert_eq!(sender.node_id, "strom-node-2");
        assert_eq!(srt(&sender.ingress).role(), SocketRole::Listen);
        assert_eq!(sender.egresses.len(), 1);
        assert_eq!(
            sender.egresses[0].socket, signalled,
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
                "weave-alice-return-receiver-guest",
            )
        );
        assert_eq!(receiver.egresses.len(), 1);
        assert_eq!(
            receiver.egresses[0].socket,
            SocketSpec::Device(DeviceKind::Display),
            "the media ends on the screen"
        );

        let endpoints =
            stream_endpoints(&alice_return(), &path, &webrtc_nodes()).expect("endpoints");
        assert_eq!(addr(&endpoints.ingress).node, "strom-node-2");
        assert_eq!(
            endpoints.destinations,
            vec![DestinationEndpoint {
                id: "guest".to_string(),
                endpoint: None,
            }],
            "a screen has nothing to dial"
        );
    }

    #[test]
    fn a_hosting_nodes_declared_base_takes_the_hop_id_and_nothing_else() {
        let mut strom = webrtc_node("strom-node-2", "172.27.0.10");
        listeners_of(&mut strom).whip = Some(SignallingListener {
            base_url: "https://edge.example/sessions/".to_string(),
        });
        let nodes = vec![browser_node("browser-a1b2"), strom];

        let path = derive(&alice_cam(), &nodes).expect("derive");
        assert_eq!(
            path.hops[1].ingress,
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Listen,
                "https://edge.example/sessions",
                "weave-alice-cam-receiver-studio",
            )
        );
    }

    #[test]
    fn hosting_webrtc_without_a_signalling_listener_is_unplaceable() {
        let mut strom = webrtc_node("strom-node-2", "172.27.0.10");
        let listeners = listeners_of(&mut strom);
        listeners.whip = None;
        listeners.whep = None;
        let nodes = vec![browser_node("browser-a1b2"), strom];
        assert_eq!(
            derive(&alice_cam(), &nodes),
            Err(PlacementError::NoRelayAvailable {
                upstream: "browser-a1b2".to_string(),
                downstream: "strom-node-2".to_string(),
            })
        );
    }

    #[test]
    fn a_listener_for_one_webrtc_transport_does_not_serve_the_other() {
        let mut strom = webrtc_node("strom-node-2", "172.27.0.10");
        listeners_of(&mut strom).whep = None;
        let nodes = vec![browser_node("browser-a1b2"), strom];

        derive(&alice_cam(), &nodes).expect("the whip listener still hosts the camera's link");
        assert_eq!(
            derive(&alice_return(), &nodes),
            Err(PlacementError::NoRelayAvailable {
                upstream: "strom-node-2".to_string(),
                downstream: "browser-a1b2".to_string(),
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
        assert_eq!(value["destinations"][0]["endpoint"]["node"], "strom-node-2");
        assert!(
            value["destinations"][0]["endpoint"]["url"]
                .as_str()
                .unwrap()
                .starts_with("srt://172.27.0.10:")
        );

        let ret = derive(&alice_return(), &nodes).expect("derive");
        let value =
            serde_json::to_value(stream_endpoints(&alice_return(), &ret, &nodes).unwrap()).unwrap();
        assert_eq!(value["ingress"]["node"], "strom-node-2");
        assert_eq!(
            value["destinations"],
            serde_json::json!([{ "id": "guest", "endpoint": null }])
        );
    }

    #[test]
    fn srt_only_endpoints_serialize_ingress_and_destination_addresses() {
        let path = derive(&contribution(), &nodes()).expect("derive");
        let value =
            serde_json::to_value(stream_endpoints(&contribution(), &path, &nodes()).unwrap())
                .unwrap();
        let ingress = value["ingress"].as_object().unwrap();
        let mut keys: Vec<&str> = ingress.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["host", "node", "port", "url"]);
        assert_eq!(value["destinations"].as_array().unwrap().len(), 1);
        assert_eq!(value["destinations"][0]["id"], "studio");
        assert!(value["destinations"][0]["endpoint"].is_object());
    }

    #[test]
    fn srt_is_preferred_when_both_ends_offer_it() {
        let pair = vec![
            webrtc_node("strom-node-1", "172.26.0.10"),
            webrtc_node("strom-node-2", "172.27.0.10"),
        ];
        let path = derive(&contribution(), &pair).expect("derive");
        assert!(matches!(
            path.hops[0].egresses[0].socket,
            SocketSpec::Srt(_)
        ));
        assert!(matches!(path.hops[1].ingress, SocketSpec::Srt(_)));
        assert_eq!(
            path,
            derive(&contribution(), &nodes()).expect("derive"),
            "offering WebRTC as well plans the same as offering SRT alone"
        );
    }

    #[test]
    fn two_browsers_without_a_relay_share_no_transport() {
        let mut stream = alice_cam();
        stream.destinations = vec![device_dest("guest", "browser-c3d4")];
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
    fn two_browsers_bridge_only_through_a_relay_with_a_whip_to_whep_profile() {
        let mut stream = alice_cam();
        stream.destinations = vec![device_dest("guest", "browser-c3d4")];
        let mut relay = webrtc_node("strom-node-1", "172.26.0.10");
        let mut nodes = vec![
            browser_node("browser-a1b2"),
            browser_node("browser-c3d4"),
            relay.clone(),
        ];
        assert_eq!(
            derive(&stream, &nodes),
            Err(PlacementError::NoCommonTransport {
                upstream: "browser-a1b2".to_string(),
                downstream: "browser-c3d4".to_string(),
            }),
            "Strom's own profiles carry no WHIP to WHEP hop"
        );

        relay.capabilities.hop_profiles.push(profile(
            "whip-to-whep",
            class(Transport::Whip, RoleSet::only(SocketRole::Listen)),
            class(Transport::Whep, RoleSet::only(SocketRole::Listen)),
            None,
        ));
        nodes[2] = relay;

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
                ("weave-alice-cam-bridge-guest-0", "strom-node-1"),
                ("weave-alice-cam-receiver-guest", "browser-c3d4"),
            ]
        );
        let bridge = &path.hops[1];
        assert_eq!(bridge.profile_id, "whip-to-whep");
        assert_eq!(
            bridge.ingress,
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Listen,
                "http://172.26.0.10:8080/ingest",
                "weave-alice-cam-bridge-guest-0",
            )
        );
        assert_eq!(bridge.egresses.len(), 1);
        assert_eq!(
            bridge.egresses[0].socket,
            SocketSpec::signalling(
                SignallingTransport::Whep,
                SocketRole::Listen,
                "http://172.26.0.10:8080/playback",
                "weave-alice-cam-receiver-guest",
            )
        );
        assert_eq!(path.hops[0].egresses.len(), 1);
        assert_eq!(
            path.hops[0].egresses[0].socket,
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Connect,
                "http://172.26.0.10:8080/ingest",
                "weave-alice-cam-bridge-guest-0",
            ),
            "the camera pushes into the relay's ingest"
        );
        assert_eq!(
            path.hops[2].ingress,
            SocketSpec::signalling(
                SignallingTransport::Whep,
                SocketRole::Connect,
                "http://172.26.0.10:8080/playback",
                "weave-alice-cam-receiver-guest",
            ),
            "the far browser pulls it back out"
        );
        assert_eq!(path.hops[2].egresses.len(), 1);
        assert_eq!(
            path.hops[2].egresses[0].socket,
            SocketSpec::Device(DeviceKind::Display)
        );
    }

    #[test]
    fn a_relay_that_cannot_carry_both_halves_is_passed_over() {
        let mut stream = alice_cam();
        stream.destinations = vec![device_dest("guest", "browser-c3d4")];
        // SRT-only relay: dialable, but a browser speaks no SRT.
        let nodes = vec![
            browser_node("browser-a1b2"),
            browser_node("browser-c3d4"),
            node("srt-relay", "198.51.100.9"),
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
        stream.destinations = vec![device_dest("guest", "strom-node-2")];
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
        // node-2 offers only srt-forward; the browser cannot reach it, and there
        // is no relay, so the stream stays unplaced rather than misplanned.
        let nodes = vec![
            browser_node("browser-a1b2"),
            node("strom-node-2", "172.27.0.10"),
        ];
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
                hop: "weave-contribution-receiver-studio".to_string(),
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
                hop: "weave-contribution-receiver-studio".to_string(),
            })
        );
    }

    #[test]
    fn endpoints_name_a_consumer_socket_that_is_not_an_srt_listener() {
        let stream = contribution();
        let mut path = derive(&stream, &nodes()).expect("derive");
        path.hops[1].egresses[0].socket = SocketSpec::Device(DeviceKind::Display);
        let error = stream_endpoints(&stream, &path, &nodes()).unwrap_err();
        assert_eq!(
            error,
            PlacementError::NotAnSrtListener {
                hop: "weave-contribution-receiver-studio".to_string(),
                socket: SocketSpec::Device(DeviceKind::Display),
            }
        );
        assert_eq!(
            error.to_string(),
            "hop weave-contribution-receiver-studio carries a display device socket where an SRT listener is needed"
        );

        path.hops[1].egresses[0].socket = SocketSpec::srt_connect("198.51.100.5", 9000, 200);
        let error = stream_endpoints(&stream, &path, &nodes()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "hop weave-contribution-receiver-studio carries a srt connect socket where an SRT listener is needed",
            "names the caller, not just the transport"
        );
    }
}
