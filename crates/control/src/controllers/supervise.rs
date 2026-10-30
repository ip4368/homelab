//! Supervising K3s: the leader watches the servers' health reports and
//! repairs what it safely can.
//!
//! This slice covers the leader's own API only (`KubeApiDown`, Critical):
//! * Its server reports its API not ready for [`KUBE_API_DOWN_GRACE_MS`]:
//!   raise the alert.
//! * Then restart its K3s, at most [`KUBE_REPAIR_ATTEMPTS`] times,
//!   [`KUBE_REPAIR_SPACING_MS`] apart. Each attempt is recorded in the log
//!   (with an event naming it) before it is carried out, and only while
//!   etcd keeps a quorum without the restarting server. The agent runs each
//!   recorded attempt once, so re-sends are safe.
//! * Still down once the last attempt settled: hand leadership — and with
//!   it the VIP — to a voter whose API is ready.
//! * Ready again: resolve the alert, clear the repairs, and note what
//!   happened as an event.
//!
//! Observations (health reports) live in the leader's memory; decisions
//! (the alert, the attempts) live in the log, so a new leader neither
//! restarts blind nor restarts what an earlier one gave up on. A missing or
//! stale report is unknown, never down: nothing is raised, repaired,
//! transferred or resolved without a fresh report.
//!
//! The replica runs this beside `membership::run`: it needs the leader's
//! own id (which `run_all` does not get) and returns a transfer for the
//! replica to start on its Raft node.

use super::alerts::AlertKind;
use super::kube::{etcd_keeps_quorum_without, etcd_members};
use super::{Action, Volatile};
use crate::proto::{mgmt, KubeHealth, Msg};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, KubeRepairAction, Lifecycle, ObjectKind, ObjectRef};
use model::HostId;

pub const KUBE_API_DOWN: &str = AlertKind::KubeApiDown.as_str();
/// How long the leader's API stays not ready before the alert fires.
pub const KUBE_API_DOWN_GRACE_MS: u64 = 60_000;
/// K3s restarts per incident before the controller gives up and hands over.
pub const KUBE_REPAIR_ATTEMPTS: usize = 3;
/// How far apart the restarts are.
pub const KUBE_REPAIR_SPACING_MS: u64 = 10 * 60_000;
/// A health report older than this says nothing: the API is unknown.
pub const KUBE_HEALTH_FRESH_MS: u64 = 30_000;

/// What the controller wants this run: actions as for every controller,
/// and the voter to hand leadership to, if the budget is spent.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Plan {
    pub actions: Vec<Action>,
    pub transfer: Option<HostId>,
}

pub fn run(state: &ClusterState, v: &Volatile, leader: &HostId, now: SimTime) -> Plan {
    match fresh_health(v, leader, now) {
        Some(h) if h.api_ready => resolve(state, leader),
        Some(_) if down_past_grace(v, leader, now) => repair(state, v, leader, now),
        _ => Plan::default(),
    }
}

/// The host's health report, if one arrived within [`KUBE_HEALTH_FRESH_MS`].
fn fresh_health<'v>(v: &'v Volatile, id: &HostId, now: SimTime) -> Option<&'v KubeHealth> {
    v.kube_health.get(id).filter(|r| now.saturating_sub(r.at) <= KUBE_HEALTH_FRESH_MS).map(|r| &r.health)
}

/// Whether the leader's API has been continuously not ready for the grace
/// period. The replica stamps `api_down_since` when the reports arrive.
fn down_past_grace(v: &Volatile, leader: &HostId, now: SimTime) -> bool {
    v.api_down_since.get(leader).is_some_and(|since| now.saturating_sub(*since) >= KUBE_API_DOWN_GRACE_MS)
}

fn open_message(state: &ClusterState, host: &HostId) -> Option<(String, String)> {
    let entity = ObjectRef::host(host);
    state.open_alert(KUBE_API_DOWN, &entity, None).map(|a| (a.id.clone(), a.message.clone()))
}

/// The leader's API is down past its grace: raise the alert, then restart
/// K3s within the budget, then hand over to a voter whose API is ready.
fn repair(state: &ClusterState, v: &Volatile, leader: &HostId, now: SimTime) -> Plan {
    let mut actions = vec![];
    let since = v.api_down_since.get(leader).copied().unwrap_or(now);
    let Some((alert_id, alert_message)) = open_message(state, leader) else {
        actions.push(Action::Propose(Command::RaiseAlert {
            // Stable across runs and unique per incident: when this leader
            // first saw the API down.
            id: format!("kubeapi-{leader}-{}-{since}", v.term),
            kind: KUBE_API_DOWN.into(),
            entity: ObjectRef::host(leader),
            subject: None,
            message: message(leader, 0, None, false, false),
        }));
        return Plan { actions, transfer: None };
    };
    let repairs = state.hosts.get(leader).map(|h| h.kube_repairs.as_slice()).unwrap_or(&[]);
    let last = repairs.last();
    let sent = last.is_some_and(|l| v.kube_repair_sent.get(leader).is_some_and(|s| *s >= l.at));
    let settled = last.is_some_and(|l| now >= l.at + KUBE_REPAIR_SPACING_MS);
    // Spent: every attempt sent and settled, so the next step is a handover
    // (or giving up, if no voter can take over).
    let spent = repairs.len() >= KUBE_REPAIR_ATTEMPTS && sent && settled;
    let mut transfer = None;
    let mut gated = false;
    if spent {
        transfer = ready_voter(state, v, leader, now);
    } else if last.is_none() || settled {
        if restart_allowed(state, v, leader) {
            let n = repairs.len() + 1;
            actions.push(Action::Propose(Command::RecordRepair { id: leader.clone(), action: KubeRepairAction::RestartK3s }));
            actions.push(Action::Propose(Command::Event {
                entity: ObjectRef::host(leader),
                message: format!("restarting K3s on {leader} (attempt {n}/{KUBE_REPAIR_ATTEMPTS}): its API is not ready"),
            }));
        } else {
            gated = true;
        }
    } else if !sent {
        // The attempt is recorded; carry it out. The agent runs each
        // attempt once, so a new leader re-sending an old one is safe.
        let to = Endpoint::new(Addr::Host(leader.clone()), Svc::Agent);
        actions.push(Action::Send(to, mgmt(), Msg::KubeRepair { attempt: last.map(|l| l.at).unwrap_or(now) }));
    }
    let want = message(leader, repairs.len(), transfer.as_ref(), spent, gated);
    if want != alert_message {
        actions.push(Action::Propose(Command::SetAlertMessage { id: alert_id, message: want }));
    }
    Plan { actions, transfer }
}

/// A restart goes ahead only while etcd keeps a quorum without the
/// restarting server (unknown members count as no quorum). One server at a
/// time holds by construction: only the leader is ever repaired, and its
/// attempts are spaced in the log.
fn restart_allowed(state: &ClusterState, v: &Volatile, leader: &HostId) -> bool {
    let status = |id: &HostId| v.kube_status.get(id).cloned().or_else(|| state.hosts.get(id)?.kube.clone());
    etcd_members(state, &status).is_some_and(|m| etcd_keeps_quorum_without(state, &m, &status, leader))
}

/// The voter to hand leadership to: a member voter, not the leader, whose
/// API is freshly ready; the lowest id, so every run picks the same one.
fn ready_voter(state: &ClusterState, v: &Volatile, leader: &HostId, now: SimTime) -> Option<HostId> {
    let member = |h: &HostId| {
        state.hosts.get(h).is_some_and(|h| {
            matches!(h.lifecycle, Lifecycle::Ready | Lifecycle::Cordoned | Lifecycle::Draining | Lifecycle::Drained)
        })
    };
    state
        .cp
        .voters
        .iter()
        .find(|h| *h != leader && member(h) && fresh_health(v, h, now).is_some_and(|hh| hh.api_ready))
        .cloned()
}

fn message(leader: &HostId, attempts: usize, transfer: Option<&HostId>, spent: bool, gated: bool) -> String {
    let mut m = format!("{leader}'s K3s API is not ready, so the VIP serves no API");
    if attempts > 0 {
        m += &format!("; restarted K3s {attempts} {}", if attempts == 1 { "time" } else { "times" });
    }
    if spent {
        match transfer {
            Some(to) => m += &format!("; handing leadership to {to}"),
            None => m += &format!("; gave up after {KUBE_REPAIR_ATTEMPTS} restarts; needs you"),
        }
    } else if gated {
        m += "; waiting to restart (etcd has no quorum to spare)";
    }
    m
}

/// The leader's API is ready: resolve every open `KubeApiDown`, clear the
/// incident's repairs, and note how it ended.
fn resolve(state: &ClusterState, leader: &HostId) -> Plan {
    let mut actions = vec![];
    let mut cleared = false;
    let open: Vec<(String, ObjectRef)> =
        state.alerts.values().filter(|a| a.kind == KUBE_API_DOWN && a.resolved_at.is_none()).map(|a| (a.id.clone(), a.entity.clone())).collect();
    for (id, entity) in &open {
        actions.push(Action::Propose(Command::ResolveAlert { id: id.clone() }));
        let host_of = |entity: &ObjectRef| (entity.kind == ObjectKind::Host).then(|| HostId::new(&entity.id));
        if let Some(h) = host_of(entity)
            && state.hosts.get(&h).is_some_and(|h| !h.kube_repairs.is_empty())
        {
            actions.push(Action::Propose(Command::ClearKubeRepairs { id: h.clone() }));
            if h == *leader {
                cleared = true;
            }
        }
        actions.push(Action::Propose(Command::Event { entity: entity.clone(), message: end_message(state, leader, entity) }));
    }
    if !cleared && state.hosts.get(leader).is_some_and(|h| !h.kube_repairs.is_empty()) {
        actions.push(Action::Propose(Command::ClearKubeRepairs { id: leader.clone() }));
    }
    Plan { actions, transfer: None }
}

/// How the incident ended, for the event log: recovered here, or
/// leadership moved while the old leader's API was down.
fn end_message(state: &ClusterState, leader: &HostId, entity: &ObjectRef) -> String {
    if entity.kind == ObjectKind::Host && entity.id == leader.as_str() {
        let n = state.hosts.get(leader).map(|h| h.kube_repairs.len()).unwrap_or(0);
        if n > 0 {
            return format!("{leader}'s K3s API is ready again after {n} {}", if n == 1 { "restart" } else { "restarts" });
        }
        return format!("{leader}'s K3s API is ready again");
    }
    match entity.kind {
        ObjectKind::Host => format!("leadership moved to {leader} while {}'s K3s API was down", entity.id),
        _ => format!("{leader}'s K3s API is ready again"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::KubeHealthReading;
    use crate::proto::{KubePhase, KubeRole, KubeStatus};
    use crate::state::Lifecycle;
    use model::{profile, Inventory, ProfileId};
    use std::collections::BTreeSet;

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn running_server(members: &[&str]) -> KubeStatus {
        KubeStatus {
            role: Some(KubeRole::Server),
            phase: KubePhase::Running,
            node_ready: true,
            etcd_members: members.iter().map(|s| hid(s)).collect(),
            ..Default::default()
        }
    }

    /// Three Ready MS-01 voters running K3s servers that see each other.
    fn state() -> ClusterState {
        let mut s = ClusterState::new();
        let voters: BTreeSet<HostId> = ["v1", "v2", "v3"].map(hid).into();
        s.apply(0, Command::SetCp { voters, mode: crate::state::CpMode::Suggest }).unwrap();
        for v in ["v1", "v2", "v3"] {
            let id = hid(v);
            s.apply(0, Command::AddPendingHost {
                id: id.clone(),
                profile: profile(ProfileId::Ms01, 0),
                inventory: Inventory::from_profile(ProfileId::Ms01, 0),
                location: "rack-0".into(),
            })
            .unwrap();
            s.apply(0, Command::ApproveHost { id: id.clone(), roles: [model::Role::Compute].into() }).unwrap();
            for lc in [Lifecycle::Provisioning, Lifecycle::Ready] {
                s.apply(0, Command::SetLifecycle { id: id.clone(), lifecycle: lc }).unwrap();
            }
            s.apply(0, Command::RecordKube { id, status: running_server(&["v1", "v2", "v3"]) }).unwrap();
        }
        s
    }

    /// Files a fresh health report for `id` at `now`, keeping the first
    /// down stamp, as the replica does.
    fn report(v: &mut Volatile, id: &str, now: SimTime, api_ready: bool) {
        v.kube_health.insert(hid(id), KubeHealthReading { at: now, health: KubeHealth { api_ready } });
        if api_ready {
            v.api_down_since.remove(&hid(id));
        } else {
            v.api_down_since.entry(hid(id)).or_insert(now);
        }
    }

    fn run_at(s: &ClusterState, v: &Volatile, now: SimTime) -> Plan {
        run(s, v, &hid("v1"), now)
    }

    /// Runs the controller and applies its proposals.
    fn step(s: &mut ClusterState, v: &Volatile, now: SimTime) -> Plan {
        let plan = run_at(s, v, now);
        for c in proposals(&plan) {
            s.apply(now, c).unwrap();
        }
        plan
    }

    fn proposals(plan: &Plan) -> Vec<Command> {
        plan.actions.iter().filter_map(|a| if let Action::Propose(c) = a { Some(c.clone()) } else { None }).collect()
    }

    fn sends(plan: &Plan) -> Vec<Msg> {
        plan.actions.iter().filter_map(|a| if let Action::Send(_, _, m) = a { Some(m.clone()) } else { None }).collect()
    }

    #[test]
    fn unknown_or_grace_means_nothing() {
        let s = state();
        let mut v = Volatile::new(0, 1);
        assert_eq!(run_at(&s, &v, 100_000), Plan::default(), "no report is unknown, never down");

        report(&mut v, "v1", 1000, false);
        assert!(run_at(&s, &v, 1000 + KUBE_API_DOWN_GRACE_MS - 1).actions.is_empty(), "not before the grace");
        // A report that stopped coming says nothing either.
        assert!(run_at(&s, &v, 1000 + KUBE_HEALTH_FRESH_MS + 1).actions.is_empty(), "a stale report is unknown");

        assert_eq!(
            s.clone().apply(0, Command::RecordRepair { id: hid("zz"), action: KubeRepairAction::RestartK3s }),
            Err(crate::state::ApplyError::NotFound)
        );
        assert_eq!(
            s.clone().apply(0, Command::ClearKubeRepairs { id: hid("zz") }),
            Err(crate::state::ApplyError::NotFound)
        );
    }

    #[test]
    fn down_past_grace_raises_repairs_once_per_attempt() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        report(&mut v, "v1", 1000, false);
        let t0 = 1000 + KUBE_API_DOWN_GRACE_MS;
        report(&mut v, "v1", t0, false);

        let plan = step(&mut s, &v, t0);
        assert!(matches!(plan.actions.as_slice(), [Action::Propose(Command::RaiseAlert { id, .. })] if id == "kubeapi-v1-1-1000"), "{:?}", plan.actions);
        assert_eq!(plan.transfer, None);

        // Recorded (with an event) before it is carried out; nothing is sent yet.
        let t1 = t0 + 1000;
        report(&mut v, "v1", t1, false);
        let plan = step(&mut s, &v, t1);
        assert_eq!(
            proposals(&plan),
            [
                Command::RecordRepair { id: hid("v1"), action: KubeRepairAction::RestartK3s },
                Command::Event { entity: ObjectRef::host(&hid("v1")), message: "restarting K3s on v1 (attempt 1/3): its API is not ready".into() },
            ],
            "{:?}",
            plan.actions
        );
        assert!(sends(&plan).is_empty());

        // The attempt applied: the message counts it, and the restart goes out, once.
        let t2 = t1 + 1000;
        report(&mut v, "v1", t2, false);
        let plan = run_at(&s, &v, t2);
        assert_eq!(
            proposals(&plan),
            [Command::SetAlertMessage {
                id: "kubeapi-v1-1-1000".into(),
                message: "v1's K3s API is not ready, so the VIP serves no API; restarted K3s 1 time".into(),
            }]
        );
        assert_eq!(sends(&plan), [Msg::KubeRepair { attempt: t1 }]);
        for c in proposals(&plan) {
            s.apply(t2, c).unwrap();
        }
        v.kube_repair_sent.insert(hid("v1"), t2);

        // Sent and counted: quiet until the next attempt is due.
        let t3 = t2 + 1000;
        report(&mut v, "v1", t3, false);
        assert!(run_at(&s, &v, t3).actions.is_empty());
        assert_eq!(run_at(&s, &v, t3).transfer, None);
    }

    #[test]
    fn three_spaced_attempts_then_handover_to_a_ready_voter() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        report(&mut v, "v1", 1000, false);
        report(&mut v, "v2", 1000, true);
        let mut now = 1000 + KUBE_API_DOWN_GRACE_MS;
        report(&mut v, "v1", now, false);
        report(&mut v, "v2", now, true);
        step(&mut s, &v, now);
        for n in 1..=KUBE_REPAIR_ATTEMPTS {
            now += 1000;
            report(&mut v, "v1", now, false);
            report(&mut v, "v2", now, true);
            let plan = step(&mut s, &v, now);
            assert!(proposals(&plan).iter().any(|c| matches!(c, Command::RecordRepair { .. })), "attempt {n}: {plan:?}");
            now += 1000;
            report(&mut v, "v1", now, false);
            report(&mut v, "v2", now, true);
            let plan = run_at(&s, &v, now);
            assert_eq!(sends(&plan), [Msg::KubeRepair { attempt: now - 1000 }], "attempt {n} is carried out");
            for c in proposals(&plan) {
                s.apply(now, c).unwrap();
            }
            v.kube_repair_sent.insert(hid("v1"), now);
            now += KUBE_REPAIR_SPACING_MS;
        }
        assert_eq!(s.hosts[&hid("v1")].kube_repairs.len(), 3);

        report(&mut v, "v1", now, false);
        report(&mut v, "v2", now, true);
        let plan = run_at(&s, &v, now);
        assert_eq!(plan.transfer, Some(hid("v2")), "the budget is spent: hand over");
        assert!(!proposals(&plan).iter().any(|c| matches!(c, Command::RecordRepair { .. })), "never a fourth: {plan:?}");
        assert!(proposals(&plan).iter().any(|c| matches!(c, Command::SetAlertMessage { message, .. } if message.ends_with("; handing leadership to v2"))), "{plan:?}");

        // The lowest ready voter wins; a NotReady one never does.
        report(&mut v, "v3", now, true);
        assert_eq!(run_at(&s, &v, now).transfer, Some(hid("v2")));
        s.apply(now, Command::SetLifecycle { id: hid("v2"), lifecycle: Lifecycle::NotReady }).unwrap();
        assert_eq!(run_at(&s, &v, now).transfer, Some(hid("v3")));
        s.apply(now, Command::SetLifecycle { id: hid("v3"), lifecycle: Lifecycle::NotReady }).unwrap();
        let plan = run_at(&s, &v, now);
        assert_eq!(plan.transfer, None);
        assert!(proposals(&plan).iter().any(|c| matches!(c, Command::SetAlertMessage { message, .. } if message.ends_with("; gave up after 3 restarts; needs you"))), "{plan:?}");
    }

    #[test]
    fn restart_waits_for_etcd_quorum_to_spare() {
        let mut s = state();
        for v in ["v1", "v2", "v3"] {
            let mut st = running_server(&["v1", "v2", "v3"]);
            st.etcd_unread = Some("unreachable".into());
            s.apply(0, Command::RecordKube { id: hid(v), status: st }).unwrap();
        }
        let mut v = Volatile::new(0, 1);
        report(&mut v, "v1", 1000, false);
        let now = 1000 + KUBE_API_DOWN_GRACE_MS;
        report(&mut v, "v1", now, false);
        step(&mut s, &v, now);

        let plan = run_at(&s, &v, now + 1000);
        assert!(!proposals(&plan).iter().any(|c| matches!(c, Command::RecordRepair { .. })), "unknown members: no restart: {plan:?}");
        assert!(proposals(&plan).iter().any(|c| matches!(c, Command::SetAlertMessage { message, .. } if message.ends_with("; waiting to restart (etcd has no quorum to spare)"))), "{plan:?}");
    }

    #[test]
    fn ready_again_resolves_clears_and_notes_recovery() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        report(&mut v, "v1", 1000, false);
        let now = 1000 + KUBE_API_DOWN_GRACE_MS;
        report(&mut v, "v1", now, false);
        step(&mut s, &v, now);
        step(&mut s, &v, now + 1000);
        assert_eq!(s.hosts[&hid("v1")].kube_repairs.len(), 1);

        report(&mut v, "v1", now + 2000, true);
        let plan = step(&mut s, &v, now + 2000);
        assert_eq!(
            proposals(&plan),
            [
                Command::ResolveAlert { id: "kubeapi-v1-1-1000".into() },
                Command::ClearKubeRepairs { id: hid("v1") },
                Command::Event {
                    entity: ObjectRef::host(&hid("v1")),
                    message: "v1's K3s API is ready again after 1 restart".into(),
                },
            ],
            "{plan:?}"
        );
        assert!(s.hosts[&hid("v1")].kube_repairs.is_empty());
        assert!(run_at(&s, &v, now + 3000).actions.is_empty(), "the budget resets with the alert");
    }

    #[test]
    fn new_leader_resolves_the_handover_it_inherited() {
        let mut s = state();
        // v2 led with a down API, spent its budget, and stepped down.
        s.apply(0, Command::RaiseAlert {
            id: "kubeapi-v2-1-500".into(),
            kind: KUBE_API_DOWN.into(),
            entity: ObjectRef::host(&hid("v2")),
            subject: None,
            message: "v2's K3s API is not ready, so the VIP serves no API; restarted K3s 3 times; handing leadership to v1".into(),
        })
        .unwrap();
        for at in [1000, 1000 + KUBE_REPAIR_SPACING_MS, 1000 + 2 * KUBE_REPAIR_SPACING_MS] {
            s.apply(at, Command::RecordRepair { id: hid("v2"), action: KubeRepairAction::RestartK3s }).unwrap();
        }
        // v1 leads now, with a ready API.
        let mut v = Volatile::new(2_000_000, 2);
        report(&mut v, "v1", 2_000_000, true);
        let plan = step(&mut s, &v, 2_000_000);
        assert_eq!(
            proposals(&plan),
            [
                Command::ResolveAlert { id: "kubeapi-v2-1-500".into() },
                Command::ClearKubeRepairs { id: hid("v2") },
                Command::Event {
                    entity: ObjectRef::host(&hid("v2")),
                    message: "leadership moved to v1 while v2's K3s API was down".into(),
                },
            ],
            "{plan:?}"
        );
    }

    #[test]
    fn stale_report_resolves_nothing() {
        let mut s = state();
        s.apply(0, Command::RaiseAlert {
            id: "kubeapi-v1-1-1000".into(),
            kind: KUBE_API_DOWN.into(),
            entity: ObjectRef::host(&hid("v1")),
            subject: None,
            message: "v1's K3s API is not ready, so the VIP serves no API".into(),
        })
        .unwrap();
        let mut v = Volatile::new(0, 1);
        v.kube_health.insert(hid("v1"), KubeHealthReading { at: 1000, health: KubeHealth { api_ready: true } });
        assert!(run_at(&s, &v, 1000 + KUBE_HEALTH_FRESH_MS + 1).actions.is_empty(), "a stale ready is unknown");
    }
}
