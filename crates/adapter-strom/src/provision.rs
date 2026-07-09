//! Pure diff between desired hops and Strom's actual flows.

use std::collections::HashSet;

use weave_core::{
    DesiredHop, HopState, LinkCondition, ResolvedAddr, SocketRole, SocketSpec, is_managed_hop_id,
};
use weave_strom::StromFlow;

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
/// A socket with no SRT connection is `Idle` when it listens (healthy waiting)
/// and `Connecting` when it calls (retrying — ambiguous, not degraded). A live
/// connection is `Flowing` when media moves and `Connected` when it is silent.
#[must_use]
pub fn socket_condition(role: SocketRole, connected: bool, rate_mbps: f64) -> LinkCondition {
    match (connected, role) {
        (false, SocketRole::Listen) => LinkCondition::Idle,
        (false, SocketRole::Connect) => LinkCondition::Connecting,
        (true, _) if rate_mbps > 0.0 => LinkCondition::Flowing,
        (true, _) => LinkCondition::Connected,
    }
}

/// Best-effort resolved address for a socket spec. A listener with no explicit
/// host resolves to the wildcard; a connector with no host cannot be resolved.
#[must_use]
pub fn resolved_addr(spec: &SocketSpec) -> Option<ResolvedAddr> {
    let port = spec.port?;
    let host = match spec.role {
        SocketRole::Listen => spec.host.clone().unwrap_or_else(|| "0.0.0.0".to_string()),
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
            socket_condition(SocketRole::Listen, false, 0.0),
            LinkCondition::Idle
        );
        assert_eq!(
            socket_condition(SocketRole::Connect, false, 0.0),
            LinkCondition::Connecting
        );
    }

    #[test]
    fn socket_condition_flows_only_when_connected_with_rate() {
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 0.0),
            LinkCondition::Connected
        );
        assert_eq!(
            socket_condition(SocketRole::Connect, true, 3.2),
            LinkCondition::Flowing
        );
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 3.2),
            LinkCondition::Flowing
        );
    }

    #[test]
    fn resolved_addr_defaults_listener_host_to_wildcard() {
        let listen = SocketSpec {
            transport: Transport::Srt,
            role: SocketRole::Listen,
            host: None,
            port: Some(7001),
            params: SrtParams::default(),
        };
        assert_eq!(
            resolved_addr(&listen),
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
        assert_eq!(resolved_addr(&connect_no_host), None);
    }
}
