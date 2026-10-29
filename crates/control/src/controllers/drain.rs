//! Cordon and drain, through Kubernetes.
//!
//! * **Cordon.** The Kubernetes node of a `Cordoned`, `Draining` or
//!   `Drained` host is cordoned (`kubectl cordon`): new pods go elsewhere.
//!   The kube controller asks a running K3s server to do it
//!   ([`KubeAdmin::cordon`](crate::proto::KubeAdmin::cordon)), and to
//!   uncordon the node once the host is `Ready` again.
//! * **Drain.** A `Draining` host's node is drained as well
//!   (`kubectl drain`, [`KubeAdmin::drain`](crate::proto::KubeAdmin::drain)):
//!   its pods are evicted, so their controllers start them elsewhere and
//!   Longhorn moves its volumes' engines with them. Once the server reports
//!   the node holds nothing but DaemonSet and static pods
//!   ([`KubeStatus::drained`]), the host is `Drained`. A host that runs no
//!   Kubernetes has nothing to drain and is `Drained` at once.
//!
//!   Everything a drain goes by is in the replicated state or reported
//!   again by the servers, so a new leader picks it up where it was.
//!   Uncordoning the host (`Ready`) cancels it.
//! * **Blocked.** While the server reports that eviction is refused (a
//!   PodDisruptionBudget, say; [`KubeStatus::drain_blocked`]), a
//!   [`DRAIN_BLOCKED`] alert on the draining host says why; its message
//!   follows the reason, and it is resolved once the drain gets past that
//!   (or is cancelled).

use super::alerts::AlertKind;
use super::{Action, Volatile};
use crate::proto::{KubePhase, KubeRole, KubeStatus};
use crate::state::{ClusterState, Command, Lifecycle, ObjectKind, ObjectRef};
use model::HostId;
use std::collections::{BTreeMap, BTreeSet};

pub const DRAIN_BLOCKED: &str = AlertKind::DrainBlocked.as_str();

/// The hosts whose Kubernetes nodes are kept cordoned.
pub fn cordoned(state: &ClusterState) -> BTreeSet<HostId> {
    let held = |l: &Lifecycle| matches!(l, Lifecycle::Cordoned | Lifecycle::Draining | Lifecycle::Drained);
    state.hosts.values().filter(|h| held(&h.lifecycle)).map(|h| h.id.clone()).collect()
}

/// The hosts whose Kubernetes nodes are being drained.
pub fn draining(state: &ClusterState) -> BTreeSet<HostId> {
    state.hosts.values().filter(|h| h.lifecycle == Lifecycle::Draining).map(|h| h.id.clone()).collect()
}

/// The statuses of the K3s servers that run: each says which nodes it drained.
fn servers<'a>(state: &'a ClusterState, v: &'a Volatile) -> impl Iterator<Item = &'a KubeStatus> {
    state
        .hosts
        .values()
        .filter_map(|h| v.kube_status.get(&h.id).or(h.kube.as_ref()))
        .filter(|s| s.role == Some(KubeRole::Server) && s.phase == KubePhase::Running)
}

/// Whether `host` runs no Kubernetes at all, so has nothing to drain.
fn runs_no_kube(state: &ClusterState, v: &Volatile, host: &HostId) -> bool {
    let status = v.kube_status.get(host).or_else(|| state.hosts.get(host)?.kube.as_ref());
    status.is_none_or(|s| s.role.is_none())
}

pub fn run(state: &ClusterState, v: &Volatile) -> Vec<Action> {
    let mut out = vec![];
    let mut blocked = BTreeMap::new();
    for host in draining(state) {
        if runs_no_kube(state, v, &host) || servers(state, v).any(|s| s.drained.contains(&host)) {
            out.push(Action::Propose(Command::SetLifecycle { id: host, lifecycle: Lifecycle::Drained }));
        } else if let Some(why) = servers(state, v).find_map(|s| s.drain_blocked.get(&host)) {
            blocked.insert(host, why.clone());
        }
    }
    out.extend(alerts(state, v, blocked));
    out
}

/// Whether a controller proposal matching `pick` is in flight.
fn in_flight(v: &Volatile, pick: impl Fn(&Command) -> bool) -> bool {
    v.in_flight.values().any(pick)
}

/// One [`DRAIN_BLOCKED`] alert per host in `blocked`, saying why; an open
/// one whose host is no longer blocked is resolved, one blocked for another
/// reason gets the new message. Ids start with the host and the leader's
/// term, plus a counter if that id was used already.
fn alerts(state: &ClusterState, v: &Volatile, mut blocked: BTreeMap<HostId, String>) -> Vec<Action> {
    let mut out = vec![];
    let open = state.alerts.values().filter(|a| a.kind == DRAIN_BLOCKED && a.resolved_at.is_none() && a.entity.kind == ObjectKind::Host);
    for a in open {
        let host = HostId::new(&a.entity.id);
        match blocked.remove(&host) {
            Some(message) if message == a.message => {}
            Some(message) => out.push(Action::Propose(Command::SetAlertMessage { id: a.id.clone(), message })),
            None => out.push(Action::Propose(Command::ResolveAlert { id: a.id.clone() })),
        }
    }
    for (host, message) in blocked {
        let entity = ObjectRef::host(&host);
        if in_flight(v, |c| matches!(c, Command::RaiseAlert { kind, entity: e, subject: None, .. } if kind == DRAIN_BLOCKED && *e == entity)) {
            continue;
        }
        let base = format!("drainblocked-{host}-{}", v.term);
        let id = (1..)
            .map(|n| if n == 1 { base.clone() } else { format!("{base}-{n}") })
            .find(|id| !state.alerts.contains_key(id))
            .expect("some id is free");
        out.push(Action::Propose(Command::RaiseAlert { id, kind: DRAIN_BLOCKED.into(), entity, subject: None, message }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{seeded_durable, SeedHost};
    use crate::proto::LogCmd;
    use crate::raft::EntryPayload;
    use crate::state::NetworkResult;
    use model::{Inventory, ProfileId};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    /// `m1` and `m2` Ready; `m1` runs a K3s server and `m2` an agent.
    fn cluster() -> (ClusterState, Volatile) {
        let first = SeedHost { id: hid("m1"), inventory: Inventory::from_profile(ProfileId::Ms01, 0), location: "lab".into() };
        let d = seeded_durable(&[first], &[hid("m1")].into(), "s3cret", 0);
        let mut s = ClusterState::new();
        for e in &d.log {
            if let EntryPayload::Normal(LogCmd { at, cmd }) = &e.payload {
                s.apply(*at, cmd.clone()).unwrap();
            }
        }
        let inv = Inventory::from_profile(ProfileId::Ms01, 0);
        let profile = model::HardwareProfile::with_discovered(&inv);
        s.apply(0, Command::AddPendingHost { id: hid("m2"), profile, inventory: inv, location: "lab".into() }).unwrap();
        s.apply(0, Command::ApproveHost { id: hid("m2"), roles: crate::bootstrap::seed_rule_roles(ProfileId::Ms01) }).unwrap();
        let ips = [(crate::proto::mgmt(), std::net::Ipv4Addr::new(10, 0, 0, 9))].into();
        s.apply(0, Command::ProvisionHost { id: hid("m2"), template: None, ips }).unwrap();
        for id in ["m1", "m2"] {
            let config = s.hosts[&hid(id)].net.desired.clone().unwrap_or_default();
            s.apply(0, Command::SetDesiredNetwork { id: hid(id), config }).unwrap();
            let generation = s.hosts[&hid(id)].net.desired_generation;
            s.apply(0, Command::RecordNetworkResult { id: hid(id), generation, result: NetworkResult::Applied }).unwrap();
            s.apply(0, Command::SetLifecycle { id: hid(id), lifecycle: Lifecycle::Ready }).unwrap();
        }
        let mut v = Volatile::new(0, 1);
        v.kube_status.insert(hid("m1"), running(KubeRole::Server));
        v.kube_status.insert(hid("m2"), running(KubeRole::Agent));
        (s, v)
    }

    fn running(role: KubeRole) -> KubeStatus {
        KubeStatus { role: Some(role), phase: KubePhase::Running, node_ready: true, version: "v1".into(), ..KubeStatus::default() }
    }

    fn set_lifecycle(s: &mut ClusterState, id: &str, lifecycle: Lifecycle) {
        s.apply(0, Command::SetLifecycle { id: hid(id), lifecycle }).unwrap();
    }

    fn drained(id: &str) -> Action {
        Action::Propose(Command::SetLifecycle { id: hid(id), lifecycle: Lifecycle::Drained })
    }

    #[test]
    fn cordoned_draining_and_drained_hosts_keep_their_nodes_cordoned() {
        let (mut s, _) = cluster();
        assert!(cordoned(&s).is_empty());
        set_lifecycle(&mut s, "m2", Lifecycle::Cordoned);
        assert_eq!(cordoned(&s), [hid("m2")].into());
        assert!(draining(&s).is_empty(), "a cordon evicts nothing");
        set_lifecycle(&mut s, "m2", Lifecycle::Draining);
        assert_eq!((cordoned(&s), draining(&s)), ([hid("m2")].into(), [hid("m2")].into()));
        set_lifecycle(&mut s, "m2", Lifecycle::Drained);
        assert_eq!((cordoned(&s), draining(&s)), ([hid("m2")].into(), BTreeSet::new()));
        set_lifecycle(&mut s, "m2", Lifecycle::Ready);
        assert!(cordoned(&s).is_empty(), "uncordoned");
    }

    #[test]
    fn a_draining_host_is_drained_once_a_server_says_its_node_is_empty() {
        let (mut s, mut v) = cluster();
        set_lifecycle(&mut s, "m2", Lifecycle::Draining);
        assert!(run(&s, &v).is_empty(), "its pods are still being evicted");
        v.kube_status.get_mut(&hid("m1")).unwrap().drained = [hid("m2")].into();
        assert_eq!(run(&s, &v), [drained("m2")]);
    }

    #[test]
    fn a_host_that_runs_no_kubernetes_is_drained_at_once() {
        let (mut s, mut v) = cluster();
        v.kube_status.remove(&hid("m2"));
        set_lifecycle(&mut s, "m2", Lifecycle::Draining);
        assert_eq!(run(&s, &v), [drained("m2")]);
    }

    #[test]
    fn a_refused_eviction_raises_an_alert_that_follows_the_reason_and_resolves() {
        let (mut s, mut v) = cluster();
        set_lifecycle(&mut s, "m2", Lifecycle::Draining);
        let why = "Cannot evict pod as it would violate the pod's disruption budget.";
        v.kube_status.get_mut(&hid("m1")).unwrap().drain_blocked = [(hid("m2"), why.to_string())].into();
        let actions = run(&s, &v);
        let [Action::Propose(raise @ Command::RaiseAlert { id, message, .. })] = &actions[..] else { panic!("{actions:?}") };
        assert_eq!((id.as_str(), message.as_str()), ("drainblocked-m2-1", why));
        s.apply(0, raise.clone()).unwrap();
        assert!(run(&s, &v).is_empty(), "raised once");

        v.kube_status.get_mut(&hid("m1")).unwrap().drain_blocked = [(hid("m2"), "other".to_string())].into();
        assert_eq!(run(&s, &v), [Action::Propose(Command::SetAlertMessage { id: "drainblocked-m2-1".into(), message: "other".into() })]);

        set_lifecycle(&mut s, "m2", Lifecycle::Ready);
        assert_eq!(run(&s, &v), [Action::Propose(Command::ResolveAlert { id: "drainblocked-m2-1".into() })], "cancelled");
    }
}
