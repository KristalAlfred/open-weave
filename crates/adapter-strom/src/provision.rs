//! Pure diff between desired hops and Strom's actual flows.

use std::collections::{HashMap, HashSet};

use weave_core::{
    DesiredHop, HopState, LinkCondition, ResolvedAddr, SocketRole, SocketSpec, is_managed_hop_id,
};
use weave_strom::StromFlow;

/// Consecutive polls without ingress byte progress before a running, ever-flowed
/// hop is judged stalled. At the default 5s poll this is ~15s of frozen bytes.
const STALL_POLLS: u32 = 3;

/// Per-poll ingress observation fed to the [`StallTracker`].
#[derive(Debug, Clone, Copy)]
pub struct IngressObservation {
    /// Cumulative ingress bytes, or `None` when stats are unavailable this cycle.
    pub bytes_received: Option<i64>,
    /// Whether the flow still claims to be running.
    pub running: bool,
    /// Whether the flow's GStreamer state is `Paused` (healthy-idle listener).
    pub gst_paused: bool,
}

#[derive(Debug, Default)]
struct HopProgress {
    last_bytes: Option<i64>,
    stale_polls: u32,
    ever_flowed: bool,
}

/// In-memory byte-progress tracker keyed by hop id. Byte progress across polls is
/// the only reliable signal that a connected ingress is truly flowing; no single
/// instantaneous field separates a dead flow from a live one.
#[derive(Debug, Default)]
pub struct StallTracker {
    hops: HashMap<String, HopProgress>,
}

impl StallTracker {
    /// Fold one poll's ingress observation for `hop_id` and report whether the
    /// ingress is stalled: it has flowed since we began watching, its bytes have
    /// been frozen for at least [`STALL_POLLS`] polls, and the flow still runs and
    /// is not a paused idle listener.
    pub fn observe(&mut self, hop_id: &str, obs: IngressObservation) -> bool {
        let progress = self.hops.entry(hop_id.to_string()).or_default();

        if let Some(bytes) = obs.bytes_received {
            match progress.last_bytes {
                // First sighting only establishes a baseline: a counter that is
                // already frozen (e.g. after an adapter restart) reads never-flowed
                // until we witness it advance.
                None => {}
                Some(prev) if bytes > prev => {
                    progress.ever_flowed = true;
                    progress.stale_polls = 0;
                }
                // Equal (frozen) or lower (SRT settles the counter down at caller
                // disconnect, or the flow was recreated) is not forward progress.
                Some(_) => progress.stale_polls = progress.stale_polls.saturating_add(1),
            }
            progress.last_bytes = Some(bytes);
        }

        progress.ever_flowed
            && obs.running
            && !obs.gst_paused
            && progress.stale_polls >= STALL_POLLS
    }

    /// Drop tracked hops no longer desired so state cannot grow without bound.
    pub fn retain(&mut self, desired: &HashSet<&str>) {
        self.hops.retain(|id, _| desired.contains(id.as_str()));
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct HopPlan {
    /// Desired hops with no same-named flow yet (flow name == hop id).
    pub create: Vec<DesiredHop>,
    /// Strom flow ids that are desired and present but not running.
    pub start: Vec<String>,
    /// Strom flow ids owned by open-weave that are no longer desired.
    pub delete: Vec<String>,
}

impl HopPlan {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.create.is_empty() && self.start.is_empty() && self.delete.is_empty()
    }
}

/// Reconcile desired hops against observed Strom flows.
///
/// Flows are adopted by name. Only flows carrying the managed hop-id prefix are
/// ever deleted, so flows created outside open-weave are left untouched.
#[must_use]
pub fn diff_hops(desired: &[DesiredHop], flows: &[StromFlow]) -> HopPlan {
    let flow_names: HashSet<&str> = flows.iter().map(|f| f.name.as_str()).collect();
    let desired_ids: HashSet<&str> = desired.iter().map(|h| h.id.as_str()).collect();

    let create = desired
        .iter()
        .filter(|hop| !flow_names.contains(hop.id.as_str()))
        .cloned()
        .collect();

    let start = flows
        .iter()
        .filter(|flow| !flow.running && desired_ids.contains(flow.name.as_str()))
        .map(|flow| flow.id.clone())
        .collect();

    let delete = flows
        .iter()
        .filter(|flow| is_managed_hop_id(&flow.name) && !desired_ids.contains(flow.name.as_str()))
        .map(|flow| flow.id.clone())
        .collect();

    HopPlan {
        create,
        start,
        delete,
    }
}

/// Derive a hop's control-plane lifecycle state from its flow presence.
///
/// Runtime link health (connection up, media flowing) is reported separately per
/// socket via [`socket_condition`], not folded into the lifecycle state.
#[must_use]
pub fn hop_state(flow: Option<&StromFlow>, failed: bool) -> HopState {
    if failed {
        return HopState::Failed;
    }
    match flow {
        None => HopState::Pending,
        Some(_) => HopState::Provisioned,
    }
}

/// Map an observed socket to its link condition.
///
/// A `stalled` verdict overrides all else: the socket carried media but its bytes
/// froze while the flow claims to run. Otherwise a socket with no SRT connection is
/// `Idle` when it listens (healthy waiting) and `Connecting` when it calls (retrying
/// — ambiguous, not degraded). A live connection is `Flowing` when media moves and
/// `Connected` when it is silent.
#[must_use]
pub fn socket_condition(
    role: SocketRole,
    connected: bool,
    rate_mbps: f64,
    stalled: bool,
) -> LinkCondition {
    if stalled {
        return LinkCondition::Stalled;
    }
    match (connected, role) {
        (false, SocketRole::Listen) => LinkCondition::Idle,
        (false, SocketRole::Connect) => LinkCondition::Connecting,
        (true, _) if rate_mbps > 0.0 => LinkCondition::Flowing,
        (true, _) => LinkCondition::Connected,
    }
}

/// Best-effort resolved address for a socket spec. A listener with no explicit
/// host resolves to the node's advertised data-plane host so peers connect to a
/// concrete address, falling back to the wildcard when the node declares none; a
/// connector with no host cannot be resolved.
#[must_use]
pub fn resolved_addr(spec: &SocketSpec, data_plane_host: Option<&str>) -> Option<ResolvedAddr> {
    let port = spec.port?;
    let host = match spec.role {
        SocketRole::Listen => spec
            .host
            .clone()
            .unwrap_or_else(|| data_plane_host.unwrap_or("0.0.0.0").to_string()),
        SocketRole::Connect => spec.host.clone()?,
    };
    Some(ResolvedAddr { host, port })
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{HopRole, SrtParams, Transport};

    fn hop(id: &str) -> DesiredHop {
        DesiredHop {
            id: id.to_string(),
            node_id: "strom-node-1".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Listen,
                host: None,
                port: Some(7001),
                params: SrtParams::default(),
            },
            egresses: vec![SocketSpec {
                transport: Transport::Srt,
                role: SocketRole::Connect,
                host: Some("10.0.0.2".to_string()),
                port: Some(7002),
                params: SrtParams::default(),
            }],
        }
    }

    fn flow(name: &str, id: &str, running: bool) -> StromFlow {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "name": name,
            "running": running,
        }))
        .unwrap()
    }

    #[test]
    fn creates_desired_hops_without_a_matching_flow() {
        let desired = vec![hop("weave-a"), hop("weave-b")];
        let flows = vec![flow("weave-a", "id-a", true)];

        let plan = diff_hops(&desired, &flows);

        let created: Vec<_> = plan.create.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(created, vec!["weave-b"]);
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn deletes_only_managed_flows_no_longer_desired() {
        let desired = vec![hop("weave-a")];
        let flows = vec![
            flow("weave-a", "id-a", true),
            flow("weave-stale", "id-stale", true),
            flow("operator-manual", "id-manual", true),
        ];

        let plan = diff_hops(&desired, &flows);

        assert_eq!(plan.delete, vec!["id-stale".to_string()]);
        assert!(
            plan.create.is_empty(),
            "adopted-by-name flow is not recreated"
        );
    }

    #[test]
    fn never_deletes_unmanaged_flows_even_when_not_desired() {
        let desired: Vec<DesiredHop> = Vec::new();
        let flows = vec![
            flow("contribution", "id-1", true),
            flow("contribution-recv", "id-2", true),
        ];

        let plan = diff_hops(&desired, &flows);

        assert!(plan.is_empty(), "old controller flows are left untouched");
    }

    #[test]
    fn starts_desired_flow_that_is_present_but_stopped() {
        let desired = vec![hop("weave-a")];
        let flows = vec![flow("weave-a", "id-a", false)];

        let plan = diff_hops(&desired, &flows);

        assert_eq!(plan.start, vec!["id-a".to_string()]);
        assert!(plan.create.is_empty());
    }

    #[test]
    fn hop_state_reports_lifecycle_only() {
        let running = flow("weave-a", "id-a", true);
        let stopped = flow("weave-a", "id-a", false);

        assert_eq!(hop_state(None, false), HopState::Pending);
        assert_eq!(hop_state(Some(&stopped), false), HopState::Provisioned);
        assert_eq!(hop_state(Some(&running), false), HopState::Provisioned);
        assert_eq!(hop_state(Some(&running), true), HopState::Failed);
        assert_eq!(hop_state(None, true), HopState::Failed);
    }

    #[test]
    fn socket_condition_distinguishes_listener_and_caller_when_down() {
        assert_eq!(
            socket_condition(SocketRole::Listen, false, 0.0, false),
            LinkCondition::Idle
        );
        assert_eq!(
            socket_condition(SocketRole::Connect, false, 0.0, false),
            LinkCondition::Connecting
        );
    }

    #[test]
    fn socket_condition_flows_only_when_connected_with_rate() {
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 0.0, false),
            LinkCondition::Connected
        );
        assert_eq!(
            socket_condition(SocketRole::Connect, true, 3.2, false),
            LinkCondition::Flowing
        );
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 3.2, false),
            LinkCondition::Flowing
        );
    }

    #[test]
    fn socket_condition_stalled_overrides_connected_state() {
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 0.0, true),
            LinkCondition::Stalled
        );
    }

    fn flowing(bytes: i64) -> IngressObservation {
        IngressObservation {
            bytes_received: Some(bytes),
            running: true,
            gst_paused: false,
        }
    }

    #[test]
    fn never_flowed_hop_never_stalls() {
        let mut tracker = StallTracker::default();
        for _ in 0..6 {
            assert!(!tracker.observe("weave-a", flowing(0)));
        }
    }

    #[test]
    fn flowed_then_frozen_stalls_after_three_polls() {
        let mut tracker = StallTracker::default();
        assert!(!tracker.observe("weave-a", flowing(1000)));
        assert!(!tracker.observe("weave-a", flowing(2000)));
        assert!(!tracker.observe("weave-a", flowing(2000)), "1 frozen poll");
        assert!(!tracker.observe("weave-a", flowing(2000)), "2 frozen polls");
        assert!(tracker.observe("weave-a", flowing(2000)), "3 frozen polls");
    }

    #[test]
    fn byte_progress_clears_a_stall() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", flowing(1000));
        for _ in 0..3 {
            tracker.observe("weave-a", flowing(2000));
        }
        assert!(tracker.observe("weave-a", flowing(2000)), "stalled");
        assert!(
            !tracker.observe("weave-a", flowing(3000)),
            "recovers when bytes advance"
        );
    }

    #[test]
    fn paused_flow_is_never_stalled() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", flowing(1000));
        tracker.observe("weave-a", flowing(2000));
        for _ in 0..4 {
            let obs = IngressObservation {
                bytes_received: Some(2000),
                running: true,
                gst_paused: true,
            };
            assert!(!tracker.observe("weave-a", obs));
        }
    }

    #[test]
    fn stopped_flow_is_never_stalled() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", flowing(1000));
        tracker.observe("weave-a", flowing(2000));
        for _ in 0..4 {
            let obs = IngressObservation {
                bytes_received: Some(2000),
                running: false,
                gst_paused: false,
            };
            assert!(!tracker.observe("weave-a", obs));
        }
    }

    #[test]
    fn counter_settling_lower_then_freezing_still_stalls() {
        // SRT settles bytes_received down at caller disconnect before freezing; a
        // hop that has flowed must still be judged stalled, not treated as fresh.
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", flowing(71_416_276));
        tracker.observe("weave-a", flowing(72_000_000));
        assert!(
            !tracker.observe("weave-a", flowing(68_958_964)),
            "settle-down poll"
        );
        assert!(!tracker.observe("weave-a", flowing(68_958_964)));
        assert!(
            tracker.observe("weave-a", flowing(68_958_964)),
            "frozen after settle -> stalled"
        );
    }

    #[test]
    fn frozen_from_first_observation_reads_never_flowed() {
        // After an adapter restart the tracker adopts an already-dead flow; with no
        // observed advance it must look never-flowed rather than stall.
        let mut tracker = StallTracker::default();
        for _ in 0..6 {
            assert!(!tracker.observe("weave-a", flowing(9_299_932)));
        }
    }

    #[test]
    fn missing_stats_do_not_advance_a_stall() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", flowing(1000));
        tracker.observe("weave-a", flowing(2000));
        let missing = IngressObservation {
            bytes_received: None,
            running: true,
            gst_paused: false,
        };
        for _ in 0..5 {
            assert!(!tracker.observe("weave-a", missing));
        }
    }

    #[test]
    fn retain_drops_undesired_hops() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", flowing(1000));
        tracker.observe("weave-b", flowing(1000));
        let desired: HashSet<&str> = ["weave-a"].into_iter().collect();
        tracker.retain(&desired);
        assert!(tracker.hops.contains_key("weave-a"));
        assert!(!tracker.hops.contains_key("weave-b"));
    }

    #[test]
    fn resolved_addr_uses_data_plane_host_for_listener_else_wildcard() {
        let listen = SocketSpec {
            transport: Transport::Srt,
            role: SocketRole::Listen,
            host: None,
            port: Some(7001),
            params: SrtParams::default(),
        };
        assert_eq!(
            resolved_addr(&listen, Some("172.26.0.10")),
            Some(ResolvedAddr {
                host: "172.26.0.10".to_string(),
                port: 7001
            })
        );
        assert_eq!(
            resolved_addr(&listen, None),
            Some(ResolvedAddr {
                host: "0.0.0.0".to_string(),
                port: 7001
            })
        );

        let connect_no_host = SocketSpec {
            transport: Transport::Srt,
            role: SocketRole::Connect,
            host: None,
            port: Some(7002),
            params: SrtParams::default(),
        };
        assert_eq!(resolved_addr(&connect_no_host, Some("172.26.0.10")), None);
    }
}
