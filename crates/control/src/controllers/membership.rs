//! Control-plane membership: keeps the Raft voters at the target size (3,
//! or 5 once the fleet has more than [`LARGE_FLEET`] hosts) and replaces
//! voters whose hosts are dead, always one single-server change at a time:
//! add a learner, promote it once caught up, then remove the dead member.
//!
//! * **Growth** adds members once the leader has led for [`GROWTH_SETTLE_MS`].
//! * **Replacement:** a voter whose host has been `NotReady` for more than
//!   [`CP_REPLACE_AFTER_MS`] is replaced automatically in
//!   `Auto` mode; in `Suggest` mode a `CpReplacementSuggested` alert offers
//!   the replacement as a button (`ApiOp::ReplaceCpMember`).
//! * A voter is removed only while the voters outnumber the target and its
//!   host is down, so the controller never takes away a live member.
//! * `CpQuorumAtRisk` is raised while the live voters are down to a bare quorum.
//!
//! * **Hand-off** (`ApiOp::HandOffCp`) moves a live voter's place to a
//!   host the user picks: the host joins as a learner and is promoted, and
//!   once it runs a K3s server (so etcd never runs short of a member while
//!   it installs), the old voter is removed. It ends, recorded as an event,
//!   once the old voter is gone, or when the new host can no longer join.
//!   Until the old voter is gone, the user may cancel it
//!   (`ApiOp::CancelCpHandoff`): the new host is removed again, if it
//!   joined (say its K3s server never starts), and the hand-off ends.
//!
//! Nothing changes while a config change is uncommitted. Candidates are
//! `Ready`, control-plane-eligible hosts, MS-01s first, then hosts on an
//! access switch (rack) no voter uses, then by id. The control-plane role
//! follows the committed voters: it is not the user's to set.

use super::{Action, Volatile};
use crate::proto::{mgmt, KubePhase, KubeRole, Msg};
use crate::raft::{MemberChange, RaftNode};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, CpMode, HostObj, Lifecycle, ObjectRef};
use model::{eligible, HostId, ProfileId, Role};
use std::collections::BTreeSet;

pub const CP_REPLACEMENT_SUGGESTED: &str = "CpReplacementSuggested";
pub const CP_QUORUM_AT_RISK: &str = "CpQuorumAtRisk";
/// How long a voter's host stays `NotReady` before it is replaced.
pub const CP_REPLACE_AFTER_MS: u64 = 10 * 60_000;
/// How long a leader leads before it grows the control plane.
pub const GROWTH_SETTLE_MS: u64 = 60_000;
/// A fleet with more hosts than this runs 5 voters; otherwise 3.
pub const LARGE_FLEET: usize = 50;

const QUORUM_ALERT_PREFIX: &str = "cpquorum-";

/// The membership as the leader's Raft node holds it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RaftMembers {
    pub voters: BTreeSet<HostId>,
    pub learners: BTreeSet<HostId>,
    /// A config change is appended but not committed.
    pub pending: bool,
    /// The learners close enough to the leader's log, and heard from
    /// recently enough, to be promoted.
    pub caught_up: BTreeSet<HostId>,
}

impl RaftMembers {
    pub fn of<T: Clone>(raft: &RaftNode<T>) -> Self {
        Self {
            voters: raft.voters().clone(),
            learners: raft.learners().clone(),
            pending: raft.config_pending(),
            caught_up: raft.learners().iter().filter(|l| raft.caught_up(l) && raft.responsive(l)).cloned().collect(),
        }
    }

    fn is_member(&self, h: &HostId) -> bool {
        self.voters.contains(h) || self.learners.contains(h)
    }
}

/// What the controller wants this run: actions as for every controller,
/// and at most one membership change for the replica to make.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Plan {
    pub actions: Vec<Action>,
    pub change: Option<MemberChange>,
}

pub fn run(state: &ClusterState, v: &Volatile, raft: &RaftMembers, now: SimTime) -> Plan {
    let mut actions = quorum_alert(state, v, raft);
    actions.extend(suggestions(state, v, raft, now));
    actions.extend(roles(state));
    actions.extend(end_handoff(state, raft));
    let change = next_change(state, v, raft, now);
    match &change {
        Some(MemberChange::AddLearner(h)) => actions.push(start_replica(h, raft)),
        // A learner's replica may not have started (the message was lost): ask again.
        None if !raft.pending => {
            let starting = raft.learners.iter().filter(|l| !raft.caught_up.contains(*l) && !down(state, l));
            actions.extend(starting.map(|l| start_replica(l, raft)).collect::<Vec<_>>());
        }
        _ => {}
    }
    Plan { actions, change }
}

/// 3 voters, or 5 for a fleet of more than [`LARGE_FLEET`] hosts.
pub fn target_size(state: &ClusterState) -> usize {
    let hosts = state.hosts.values().filter(|h| h.lifecycle != Lifecycle::Removed).count();
    if hosts > LARGE_FLEET { 5 } else { 3 }
}

/// Hosts that may join the control plane, best first.
pub fn candidates(state: &ClusterState, raft: &RaftMembers) -> Vec<HostId> {
    let voter_racks: BTreeSet<&str> =
        raft.voters.iter().filter_map(|v| state.hosts.get(v)).map(|h| h.location.as_str()).collect();
    let mut out: Vec<&HostObj> = state
        .hosts
        .values()
        .filter(|h| h.lifecycle == Lifecycle::Ready && !raft.is_member(&h.id))
        .filter(|h| eligible(&h.profile, Role::ControlPlane).is_ok())
        .collect();
    out.sort_by_key(|h| (h.profile.id != ProfileId::Ms01, voter_racks.contains(h.location.as_str()), h.id.clone()));
    out.into_iter().map(|h| h.id.clone()).collect()
}

/// Why the leader refuses to replace voter `dead` with `with`, if it does.
pub fn check_replace(state: &ClusterState, raft: &RaftMembers, dead: &HostId, with: &HostId) -> Result<(), String> {
    if raft.pending || !raft.learners.is_empty() {
        return Err("a control-plane membership change is in progress".into());
    }
    if !raft.voters.contains(dead) {
        return Err(format!("{dead} is not a control-plane voter"));
    }
    if !down(state, dead) {
        return Err(format!("{dead} is not down"));
    }
    if !candidates(state, raft).contains(with) {
        return Err(format!("{with} cannot join the control plane (it must be Ready, eligible and not a member)"));
    }
    Ok(())
}

/// Why the leader refuses to hand voter `from`'s place to `to`, if it does.
pub fn check_handoff(state: &ClusterState, raft: &RaftMembers, from: &HostId, to: &HostId) -> Result<(), String> {
    if raft.pending || !raft.learners.is_empty() || state.cp.handoff.is_some() {
        return Err("a control-plane membership change is in progress".into());
    }
    if !raft.voters.contains(from) {
        return Err(format!("{from} is not a control-plane voter"));
    }
    if let Some(d) = raft.voters.iter().find(|v| down(state, v)) {
        return Err(format!("control-plane voter {d} is down: replace it first"));
    }
    if !candidates(state, raft).contains(to) {
        return Err(format!("{to} cannot join the control plane (it must be Ready, eligible and not a member)"));
    }
    Ok(())
}

/// Why the leader refuses to cancel the hand-off, if it does: none is in
/// progress, it is already cancelled, or its old voter has left (it is
/// done but for its event).
pub fn check_cancel_handoff(state: &ClusterState) -> Result<(), String> {
    let Some(h) = &state.cp.handoff else { return Err("no control-plane hand-off is in progress".into()) };
    if h.cancelled {
        return Err(format!("the hand-off of {}'s control-plane place to {} is already cancelled", h.from, h.to));
    }
    if !state.cp.voters.contains(&h.from) {
        return Err(format!("{} has already handed its control-plane place to {}", h.from, h.to));
    }
    Ok(())
}

/// Why setting (`wanted`) or clearing host `id`'s control-plane role by
/// hand is refused: the role follows the voters.
pub fn control_plane_role_refused(id: &HostId, wanted: bool) -> String {
    if wanted {
        format!("the control-plane role follows the control-plane voters, and {id} is not one: hand a voter's place to it instead")
    } else {
        format!("the control-plane role follows the control-plane voters, and {id} is one: hand its place to another host instead")
    }
}

/// Whether `id`'s host runs a K3s server, as voters do, or the cluster has
/// no Kubernetes to wait for.
fn serves_kube(state: &ClusterState, id: &HostId) -> bool {
    let running = |h: &HostObj| h.kube.as_ref().is_some_and(|k| k.role == Some(KubeRole::Server) && k.phase == KubePhase::Running);
    state.kube.is_none() || state.hosts.get(id).is_some_and(running)
}

/// Ends the hand-off in progress once its old voter is gone, once it is
/// cancelled and its new host is no member, or once its new host is no
/// member and can no longer join.
fn end_handoff(state: &ClusterState, raft: &RaftMembers) -> Option<Action> {
    let h = state.cp.handoff.as_ref()?;
    if raft.pending {
        return None;
    }
    let message = if !raft.voters.contains(&h.from) && raft.voters.contains(&h.to) {
        format!("{} handed its control-plane place to {}", h.from, h.to)
    } else if !raft.voters.contains(&h.from) {
        format!("gave up handing {}'s control-plane place to {}: {} left the control plane first", h.from, h.to, h.from)
    } else if h.cancelled && !raft.is_member(&h.to) {
        format!("stopped handing {}'s control-plane place to {}: cancelled by the user", h.from, h.to)
    } else if !raft.is_member(&h.to) && !candidates(state, raft).contains(&h.to) {
        format!("gave up handing {}'s control-plane place to {}: {} cannot join (it must be Ready, eligible and not a member)", h.from, h.to, h.to)
    } else {
        return None;
    };
    Some(Action::Propose(Command::EndCpHandoff { message }))
}

/// Whether the state knows `h`'s host and it is not up (up: `Ready`, or
/// cordoned, draining or drained, which still run the replica). A member
/// the state has no host for is not judged.
fn down(state: &ClusterState, h: &HostId) -> bool {
    state.hosts.get(h).is_some_and(|h| {
        !matches!(h.lifecycle, Lifecycle::Ready | Lifecycle::Cordoned | Lifecycle::Draining | Lifecycle::Drained)
    })
}

/// Whether voter `h` is due for replacement: its host has been `NotReady`
/// for more than [`CP_REPLACE_AFTER_MS`].
fn dead(state: &ClusterState, h: &HostId, now: SimTime) -> bool {
    state.hosts.get(h).is_some_and(|h| {
        h.lifecycle == Lifecycle::NotReady && now.saturating_sub(h.last_transition) > CP_REPLACE_AFTER_MS
    })
}

fn next_change(state: &ClusterState, v: &Volatile, raft: &RaftMembers, now: SimTime) -> Option<MemberChange> {
    if raft.pending {
        return None;
    }
    // A cancelled hand-off's new host goes again, learner or voter (it is
    // an extra voter while the old one stays), before a learner is promoted.
    if let Some(h) = state.cp.handoff.as_ref().filter(|h| h.cancelled && raft.voters.contains(&h.from) && raft.is_member(&h.to)) {
        return Some(MemberChange::Remove(h.to.clone()));
    }
    if let Some(l) = raft.learners.first() {
        return if down(state, l) {
            Some(MemberChange::Remove(l.clone()))
        } else if raft.caught_up.contains(l) {
            Some(MemberChange::Promote(l.clone()))
        } else {
            None
        };
    }
    let target = target_size(state);
    if raft.voters.len() > target {
        // Longest down first.
        let downed = raft.voters.iter().filter_map(|h| state.hosts.get(h).filter(|_| down(state, h)));
        if let Some(h) = downed.min_by_key(|h| (h.last_transition, h.id.clone())).map(|h| &h.id) {
            return Some(MemberChange::Remove(h.clone()));
        }
    }
    if let Some(h) = state.cp.handoff.as_ref().filter(|h| !h.cancelled && raft.voters.contains(&h.from)) {
        if raft.voters.contains(&h.to) && serves_kube(state, &h.to) {
            return Some(MemberChange::Remove(h.from.clone()));
        }
        if !raft.is_member(&h.to) && candidates(state, raft).contains(&h.to) {
            return Some(MemberChange::AddLearner(h.to.clone()));
        }
    }
    let replace = state.cp.mode == CpMode::Auto && raft.voters.iter().any(|h| dead(state, h, now));
    let grow = raft.voters.len() < target && now >= v.elected_at + GROWTH_SETTLE_MS;
    if replace || grow {
        return candidates(state, raft).into_iter().next().map(MemberChange::AddLearner);
    }
    None
}

fn start_replica(h: &HostId, raft: &RaftMembers) -> Action {
    let agent = Endpoint::new(Addr::Host(h.clone()), Svc::Agent);
    Action::Send(agent, mgmt(), Msg::StartCpReplica { voters: raft.voters.clone() })
}

/// Raises `CpQuorumAtRisk` while a voter is down and the rest are a bare
/// quorum (one more failure stops the control plane); resolves it after.
fn quorum_alert(state: &ClusterState, v: &Volatile, raft: &RaftMembers) -> Vec<Action> {
    let live_voters = raft.voters.iter().filter(|h| !down(state, h)).count();
    let at_risk = live_voters < raft.voters.len() && live_voters <= raft.voters.len() / 2 + 1;
    let open = state.alerts.values().find(|a| a.kind == CP_QUORUM_AT_RISK && a.resolved_at.is_none());
    match open {
        Some(a) if !at_risk => vec![Action::Propose(Command::ResolveAlert { id: a.id.clone() })],
        None if at_risk => {
            let in_flight = v
                .in_flight
                .values()
                .any(|c| matches!(c, Command::RaiseAlert { id, .. } if id.starts_with(QUORUM_ALERT_PREFIX)));
            if in_flight {
                return vec![];
            }
            let base = format!("{QUORUM_ALERT_PREFIX}{}-{}", v.term, v.elected_at);
            let id = (1..)
                .map(|n| if n == 1 { base.clone() } else { format!("{base}-{n}") })
                .find(|id| !state.alerts.contains_key(id))
                .expect("some id is free");
            let message = format!("{live_voters} of {} control-plane voters are up: one more failure loses quorum", raft.voters.len());
            vec![Action::Propose(Command::RaiseAlert {
                id,
                kind: CP_QUORUM_AT_RISK.into(),
                entity: ObjectRef::cp(),
                subject: None,
                message,
            })]
        }
        _ => vec![],
    }
}

/// In `Suggest` mode, one `CpReplacementSuggested` alert per dead voter
/// (per incident: the id carries when its host went down), naming the best
/// candidate. Resolved once the host is no voter, is back, or the mode is `Auto`.
fn suggestions(state: &ClusterState, v: &Volatile, raft: &RaftMembers, now: SimTime) -> Vec<Action> {
    let suggest = state.cp.mode == CpMode::Suggest;
    let wanted: Vec<&HostId> = raft.voters.iter().filter(|h| suggest && dead(state, h, now)).collect();
    let mut out: Vec<Action> = state
        .alerts
        .values()
        .filter(|a| a.kind == CP_REPLACEMENT_SUGGESTED && a.resolved_at.is_none())
        .filter(|a| !wanted.iter().any(|h| a.entity == ObjectRef::host(h)))
        .map(|a| Action::Propose(Command::ResolveAlert { id: a.id.clone() }))
        .collect();
    let Some(with) = candidates(state, raft).into_iter().next() else { return out };
    for dead in wanted {
        let id = format!("cpreplace-{dead}-{}", state.hosts[dead].last_transition);
        let in_flight = v.in_flight.values().any(|c| matches!(c, Command::SuggestCpReplacement { id: i, .. } if *i == id));
        if !state.alerts.contains_key(&id) && !in_flight {
            out.push(Action::Propose(Command::SuggestCpReplacement { id, dead: dead.clone(), with: with.clone() }));
        }
    }
    out
}

/// The committed voters have the control-plane role; other hosts don't.
fn roles(state: &ClusterState) -> Vec<Action> {
    let settable = |h: &&HostObj| !matches!(h.lifecycle, Lifecycle::Pending | Lifecycle::Removed);
    let mut out = vec![];
    for h in state.hosts.values().filter(settable) {
        let voter = state.cp.voters.contains(&h.id);
        if voter == h.roles.contains(&Role::ControlPlane) {
            continue;
        }
        let mut roles = h.roles.clone();
        if voter {
            roles.insert(Role::ControlPlane);
        } else {
            roles.remove(&Role::ControlPlane);
        }
        out.push(Action::Propose(Command::SetRoles { id: h.id.clone(), roles }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Alert, Severity};
    use model::{profile, Inventory};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn ids(v: &[&str]) -> BTreeSet<HostId> {
        v.iter().map(|s| hid(s)).collect()
    }

    /// Adds Ready host `id` at time 0, with the control-plane role if `voter`.
    fn add(s: &mut ClusterState, id: &str, p: ProfileId, location: &str, voter: bool) {
        let h = hid(id);
        let inventory = Inventory::from_profile(p, 0);
        s.apply(0, Command::AddPendingHost { id: h.clone(), profile: profile(p, 0), inventory, location: location.into() })
            .unwrap();
        let mut roles = BTreeSet::from([Role::Compute]);
        if voter {
            roles.insert(Role::ControlPlane);
        }
        s.apply(0, Command::ApproveHost { id: h.clone(), roles }).unwrap();
        s.apply(0, Command::SetLifecycle { id: h.clone(), lifecycle: Lifecycle::Provisioning }).unwrap();
        s.apply(0, Command::SetLifecycle { id: h, lifecycle: Lifecycle::Ready }).unwrap();
    }

    fn set_lifecycle(s: &mut ClusterState, id: &str, lifecycle: Lifecycle, at: SimTime) {
        s.apply(at, Command::SetLifecycle { id: hid(id), lifecycle }).unwrap();
    }

    /// Voters v1..v3 (MS-01s in rack-0) and spare hosts: an MS-01, mini PCs
    /// in racks 1 and 2, and a Pi 5 (not eligible).
    fn state(mode: CpMode) -> (ClusterState, RaftMembers) {
        let mut s = ClusterState::new();
        s.apply(0, Command::SetCp { voters: ids(&["v1", "v2", "v3"]), mode }).unwrap();
        for v in ["v1", "v2", "v3"] {
            add(&mut s, v, ProfileId::Ms01, "rack-0", true);
        }
        add(&mut s, "ms01-9", ProfileId::Ms01, "rack-0", false);
        add(&mut s, "mini-1", ProfileId::MiniPc, "rack-1", false);
        add(&mut s, "mini-2", ProfileId::MiniPc, "rack-2", false);
        add(&mut s, "pi-1", ProfileId::Pi5, "rack-1", false);
        let raft = RaftMembers { voters: ids(&["v1", "v2", "v3"]), ..Default::default() };
        (s, raft)
    }

    fn proposals(p: &Plan) -> Vec<Command> {
        p.actions.iter().filter_map(|a| if let Action::Propose(c) = a { Some(c.clone()) } else { None }).collect()
    }

    fn started(p: &Plan) -> Vec<HostId> {
        let started = |a: &Action| match a {
            Action::Send(Endpoint { addr: Addr::Host(h), svc: Svc::Agent }, _, Msg::StartCpReplica { .. }) => Some(h.clone()),
            _ => None,
        };
        p.actions.iter().filter_map(started).collect()
    }

    fn open_alerts<'s>(s: &'s ClusterState, kind: &str) -> Vec<&'s Alert> {
        s.alerts.values().filter(|a| a.kind == kind && a.resolved_at.is_none()).collect()
    }

    /// Runs the controller and applies its proposals.
    fn step(s: &mut ClusterState, v: &Volatile, raft: &RaftMembers, now: SimTime) -> Plan {
        let plan = run(s, v, raft, now);
        for c in proposals(&plan) {
            s.apply(now, c).unwrap();
        }
        plan
    }

    #[test]
    fn candidates_prefer_ms01_then_an_unused_rack_then_id() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        add(&mut s, "mini-0", ProfileId::MiniPc, "rack-1", false);
        add(&mut s, "ms01-8", ProfileId::Ms01, "rack-0", false);
        set_lifecycle(&mut s, "ms01-8", Lifecycle::Cordoned, 0);
        assert_eq!(candidates(&s, &raft), [hid("ms01-9"), hid("mini-0"), hid("mini-1"), hid("mini-2")]);
        // With a voter in rack-1, rack-2 comes first; learners are no candidates.
        raft.voters.insert(hid("mini-0"));
        raft.learners.insert(hid("ms01-9"));
        assert_eq!(candidates(&s, &raft), [hid("mini-2"), hid("mini-1")]);
    }

    #[test]
    fn target_is_5_above_50_hosts() {
        let (mut s, _) = state(CpMode::Suggest);
        for i in s.hosts.len()..50 {
            add(&mut s, &format!("x-{i}"), ProfileId::Pi5, "rack-3", false);
        }
        assert_eq!(target_size(&s), 3);
        add(&mut s, "x-last", ProfileId::Pi5, "rack-3", false);
        assert_eq!(target_size(&s), 5);
    }

    #[test]
    fn grows_after_settling_one_change_at_a_time() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        for i in s.hosts.len()..51 {
            add(&mut s, &format!("x-{i}"), ProfileId::Pi5, "rack-3", false);
        }
        let v = Volatile::new(1000, 2);
        assert_eq!(step(&mut s, &v, &raft, 1000 + GROWTH_SETTLE_MS - 1).change, None, "not before the fleet settles");
        let plan = step(&mut s, &v, &raft, 1000 + GROWTH_SETTLE_MS);
        assert_eq!(plan.change, Some(MemberChange::AddLearner(hid("ms01-9"))));
        assert_eq!(started(&plan), [hid("ms01-9")], "the new member's replica is started");

        raft.learners.insert(hid("ms01-9"));
        raft.pending = true;
        let plan = step(&mut s, &v, &raft, 70_000);
        assert_eq!((plan.change.clone(), started(&plan)), (None, vec![]), "nothing while a change is uncommitted");
        raft.pending = false;
        let plan = step(&mut s, &v, &raft, 71_000);
        assert_eq!((plan.change.clone(), started(&plan)), (None, vec![hid("ms01-9")]), "waits for catch-up, asking again");
        raft.caught_up.insert(hid("ms01-9"));
        assert_eq!(step(&mut s, &v, &raft, 72_000).change, Some(MemberChange::Promote(hid("ms01-9"))));

        set_lifecycle(&mut s, "ms01-9", Lifecycle::NotReady, 72_000);
        raft.caught_up.clear();
        assert_eq!(step(&mut s, &v, &raft, 73_000).change, Some(MemberChange::Remove(hid("ms01-9"))), "a dead learner goes");
    }

    #[test]
    fn auto_mode_replaces_a_member_down_for_10_min() {
        let (mut s, mut raft) = state(CpMode::Auto);
        let v = Volatile::new(0, 1);
        set_lifecycle(&mut s, "v2", Lifecycle::NotReady, 5000);
        assert_eq!(step(&mut s, &v, &raft, 5000 + CP_REPLACE_AFTER_MS).change, None);
        let plan = step(&mut s, &v, &raft, 5001 + CP_REPLACE_AFTER_MS);
        assert_eq!(plan.change, Some(MemberChange::AddLearner(hid("ms01-9"))));
        assert!(open_alerts(&s, CP_REPLACEMENT_SUGGESTED).is_empty(), "auto mode does not suggest");

        // Promoted: four voters over a target of three, so the dead one goes.
        raft.voters.insert(hid("ms01-9"));
        assert_eq!(step(&mut s, &v, &raft, 700_000).change, Some(MemberChange::Remove(hid("v2"))));
        raft.voters.remove(&hid("v2"));
        assert_eq!(step(&mut s, &v, &raft, 701_000).change, None, "done");
    }

    #[test]
    fn suggest_mode_raises_one_suggestion_and_changes_nothing() {
        let (mut s, raft) = state(CpMode::Suggest);
        let v = Volatile::new(0, 1);
        set_lifecycle(&mut s, "v2", Lifecycle::NotReady, 5000);
        step(&mut s, &v, &raft, 5000 + CP_REPLACE_AFTER_MS);
        assert!(open_alerts(&s, CP_REPLACEMENT_SUGGESTED).is_empty(), "not before 10 min");
        let plan = step(&mut s, &v, &raft, 5001 + CP_REPLACE_AFTER_MS);
        assert_eq!(plan.change, None);
        let open = open_alerts(&s, CP_REPLACEMENT_SUGGESTED);
        assert_eq!(open.len(), 1);
        assert_eq!((open[0].entity.clone(), open[0].severity), (ObjectRef::host(&hid("v2")), Severity::Warning));
        assert_eq!(open[0].action, Some(crate::proto::ApiOp::ReplaceCpMember { dead: hid("v2"), with: hid("ms01-9") }));
        step(&mut s, &v, &raft, 700_000);
        assert_eq!(open_alerts(&s, CP_REPLACEMENT_SUGGESTED).len(), 1, "raised once");

        assert_eq!(check_replace(&s, &raft, &hid("v2"), &hid("ms01-9")), Ok(()));
        assert!(check_replace(&s, &raft, &hid("v1"), &hid("ms01-9")).is_err(), "v1 is up");
        assert!(check_replace(&s, &raft, &hid("v2"), &hid("pi-1")).is_err(), "a Pi 5 is not eligible");
        assert!(check_replace(&s, &raft, &hid("mini-1"), &hid("ms01-9")).is_err(), "mini-1 is no voter");

        // The host comes back: the suggestion resolves.
        set_lifecycle(&mut s, "v2", Lifecycle::Ready, 701_000);
        step(&mut s, &v, &raft, 702_000);
        assert!(open_alerts(&s, CP_REPLACEMENT_SUGGESTED).is_empty());
    }

    #[test]
    fn quorum_at_risk_while_live_voters_are_a_bare_quorum() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        let v = Volatile::new(0, 1);
        step(&mut s, &v, &raft, 1000);
        assert!(open_alerts(&s, CP_QUORUM_AT_RISK).is_empty(), "3 of 3 up");
        set_lifecycle(&mut s, "v3", Lifecycle::NotReady, 2000);
        step(&mut s, &v, &raft, 3000);
        let open = open_alerts(&s, CP_QUORUM_AT_RISK);
        assert_eq!(open.len(), 1, "2 of 3 up");
        assert_eq!((open[0].severity, open[0].entity.clone()), (Severity::Critical, ObjectRef::cp()));
        step(&mut s, &v, &raft, 4000);
        assert_eq!(s.alerts.values().filter(|a| a.kind == CP_QUORUM_AT_RISK).count(), 1, "raised once");

        // Five voters with one down still have a spare.
        raft.voters.extend([hid("mini-1"), hid("mini-2")]);
        step(&mut s, &v, &raft, 5000);
        assert!(open_alerts(&s, CP_QUORUM_AT_RISK).is_empty(), "4 of 5 up");
        set_lifecycle(&mut s, "mini-2", Lifecycle::NotReady, 6000);
        step(&mut s, &v, &raft, 7000);
        assert_eq!(open_alerts(&s, CP_QUORUM_AT_RISK).len(), 1, "3 of 5 up");

        // A single configured voter is not at risk by failure.
        let (s, _) = state(CpMode::Suggest);
        let solo = RaftMembers { voters: ids(&["v1"]), ..Default::default() };
        assert!(quorum_alert(&s, &v, &solo).is_empty());
    }

    fn k3s(s: &mut ClusterState, id: &str, role: KubeRole, phase: KubePhase) {
        let status = crate::proto::KubeStatus {
            role: Some(role),
            phase,
            node_ready: true,
            labels: Default::default(),
            storage_disks: vec![],
            version: "v1".into(),
            detail: None,
            etcd_members: BTreeSet::new(),
            etcd_unread: None,
            etcd_unstarted: BTreeSet::new(),
            ..Default::default()
        };
        s.apply(0, Command::RecordKube { id: hid(id), status }).unwrap();
    }

    fn last_event(s: &ClusterState) -> String {
        s.events.values().last().unwrap().message.clone()
    }

    #[test]
    fn only_a_live_voter_hands_off_and_only_to_a_candidate_with_no_change_in_progress() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        assert_eq!(check_handoff(&s, &raft, &hid("v1"), &hid("mini-1")), Ok(()));
        assert!(check_handoff(&s, &raft, &hid("mini-2"), &hid("mini-1")).is_err(), "mini-2 is no voter");
        assert!(check_handoff(&s, &raft, &hid("v1"), &hid("v2")).is_err(), "v2 is a member already");
        assert!(check_handoff(&s, &raft, &hid("v1"), &hid("pi-1")).is_err(), "a Pi 5 is not eligible");
        set_lifecycle(&mut s, "v3", Lifecycle::NotReady, 0);
        assert_eq!(check_handoff(&s, &raft, &hid("v1"), &hid("mini-1")), Err("control-plane voter v3 is down: replace it first".into()));
        set_lifecycle(&mut s, "v3", Lifecycle::Ready, 0);
        raft.learners.insert(hid("ms01-9"));
        assert!(check_handoff(&s, &raft, &hid("v1"), &hid("mini-1")).is_err(), "a learner is joining");
        raft.learners.clear();
        s.apply(0, Command::StartCpHandoff { from: hid("v1"), to: hid("mini-1") }).unwrap();
        assert!(check_handoff(&s, &raft, &hid("v2"), &hid("mini-2")).is_err(), "one hand-off at a time");
    }

    #[test]
    fn a_handoff_adds_the_chosen_host_and_removes_the_old_voter_once_the_new_one_runs_a_k3s_server() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        let v = Volatile::new(0, 1);
        s.apply(0, Command::SetKube { token: "t".into(), init_host: hid("v1") }).unwrap();
        for id in ["v1", "v2", "v3"] {
            k3s(&mut s, id, KubeRole::Server, KubePhase::Running);
        }
        k3s(&mut s, "mini-1", KubeRole::Agent, KubePhase::Running);
        s.apply(0, Command::StartCpHandoff { from: hid("v1"), to: hid("mini-1") }).unwrap();
        assert_eq!(last_event(&s), "handing v1's control-plane place to mini-1");

        let plan = step(&mut s, &v, &raft, 1000);
        assert_eq!(plan.change, Some(MemberChange::AddLearner(hid("mini-1"))), "the chosen host, not the best candidate (ms01-9)");
        assert_eq!(started(&plan), [hid("mini-1")]);
        raft.learners.insert(hid("mini-1"));
        raft.caught_up.insert(hid("mini-1"));
        assert_eq!(step(&mut s, &v, &raft, 2000).change, Some(MemberChange::Promote(hid("mini-1"))));
        raft.learners.clear();
        raft.voters.insert(hid("mini-1"));
        s.apply_membership(raft.voters.clone());

        // mini-1 is reinstalling as a K3s server: v1 stays, so etcd is never a member short.
        k3s(&mut s, "mini-1", KubeRole::Server, KubePhase::Installing);
        let plan = step(&mut s, &v, &raft, 3000);
        assert_eq!(plan.change, None);
        assert!(s.hosts[&hid("mini-1")].roles.contains(&Role::ControlPlane), "the role follows the voters");
        k3s(&mut s, "mini-1", KubeRole::Server, KubePhase::Running);
        assert_eq!(step(&mut s, &v, &raft, 4000).change, Some(MemberChange::Remove(hid("v1"))));
        raft.voters.remove(&hid("v1"));
        s.apply_membership(raft.voters.clone());

        let plan = step(&mut s, &v, &raft, 5000);
        assert_eq!(plan.change, None, "done: four voters were never the target");
        assert_eq!(s.cp.handoff, None);
        assert_eq!(last_event(&s), "v1 handed its control-plane place to mini-1");
        assert!(!s.hosts[&hid("v1")].roles.contains(&Role::ControlPlane));
    }

    #[test]
    fn a_handoff_whose_new_host_goes_down_before_it_joins_is_given_up() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        let v = Volatile::new(0, 1);
        s.apply(0, Command::StartCpHandoff { from: hid("v1"), to: hid("mini-1") }).unwrap();
        assert_eq!(step(&mut s, &v, &raft, 1000).change, Some(MemberChange::AddLearner(hid("mini-1"))));
        raft.learners.insert(hid("mini-1"));
        set_lifecycle(&mut s, "mini-1", Lifecycle::NotReady, 2000);
        assert_eq!(step(&mut s, &v, &raft, 3000).change, Some(MemberChange::Remove(hid("mini-1"))), "a dead learner goes");
        raft.learners.clear();
        assert_eq!(step(&mut s, &v, &raft, 4000).change, None, "v1 stays");
        assert_eq!(s.cp.handoff, None);
        assert!(last_event(&s).starts_with("gave up handing v1's control-plane place to mini-1: mini-1 cannot join"), "{}", last_event(&s));
    }

    #[test]
    fn a_cancelled_handoff_removes_its_new_host_again_and_keeps_the_old_voter() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        let v = Volatile::new(0, 1);
        s.apply(0, Command::SetKube { token: "t".into(), init_host: hid("v1") }).unwrap();
        for id in ["v1", "v2", "v3"] {
            k3s(&mut s, id, KubeRole::Server, KubePhase::Running);
        }
        assert_eq!(check_cancel_handoff(&s), Err("no control-plane hand-off is in progress".into()));
        s.apply(0, Command::StartCpHandoff { from: hid("v1"), to: hid("mini-1") }).unwrap();
        raft.voters.insert(hid("mini-1"));
        s.apply_membership(raft.voters.clone());
        // mini-1's K3s server never starts: the hand-off waits on it for good.
        k3s(&mut s, "mini-1", KubeRole::Server, KubePhase::Installing);
        assert_eq!(step(&mut s, &v, &raft, 1000).change, None);

        assert_eq!(check_cancel_handoff(&s), Ok(()));
        s.apply(2000, Command::CancelCpHandoff).unwrap();
        assert_eq!(last_event(&s), "cancelling the hand-off of v1's control-plane place to mini-1");
        assert!(check_cancel_handoff(&s).unwrap_err().contains("already cancelled"));
        assert_eq!(step(&mut s, &v, &raft, 3000).change, Some(MemberChange::Remove(hid("mini-1"))), "mini-1 goes, not v1");
        raft.voters.remove(&hid("mini-1"));
        s.apply_membership(raft.voters.clone());

        assert_eq!(step(&mut s, &v, &raft, 4000).change, None, "nothing joins in its place");
        assert_eq!(s.cp.handoff, None);
        assert_eq!(last_event(&s), "stopped handing v1's control-plane place to mini-1: cancelled by the user");
        assert_eq!(s.cp.voters, ids(&["v1", "v2", "v3"]));
        assert!(!s.hosts[&hid("mini-1")].roles.contains(&Role::ControlPlane));
    }

    #[test]
    fn a_cancelled_handoff_removes_a_learner_rather_than_promote_it_and_is_refused_once_the_old_voter_left() {
        let (mut s, mut raft) = state(CpMode::Suggest);
        let v = Volatile::new(0, 1);
        s.apply(0, Command::StartCpHandoff { from: hid("v1"), to: hid("mini-1") }).unwrap();
        raft.learners.insert(hid("mini-1"));
        raft.caught_up.insert(hid("mini-1"));
        s.apply(1000, Command::CancelCpHandoff).unwrap();
        assert_eq!(step(&mut s, &v, &raft, 2000).change, Some(MemberChange::Remove(hid("mini-1"))));
        raft.learners.clear();
        assert_eq!(step(&mut s, &v, &raft, 3000).change, None);
        assert_eq!(s.cp.handoff, None);

        s.apply(4000, Command::StartCpHandoff { from: hid("v1"), to: hid("mini-1") }).unwrap();
        s.apply_membership(ids(&["v2", "v3", "mini-1"]));
        assert_eq!(check_cancel_handoff(&s), Err("v1 has already handed its control-plane place to mini-1".into()));
    }

    #[test]
    fn the_control_plane_role_follows_the_committed_voters() {
        let (mut s, raft) = state(CpMode::Suggest);
        let v = Volatile::new(0, 1);
        assert!(proposals(&run(&s, &v, &raft, 0)).is_empty());
        s.apply_membership(ids(&["v1", "v2", "ms01-9"]));
        let plan = step(&mut s, &v, &raft, 1000);
        assert_eq!(proposals(&plan).len(), 2);
        assert!(s.hosts[&hid("ms01-9")].roles.contains(&Role::ControlPlane));
        assert!(!s.hosts[&hid("v3")].roles.contains(&Role::ControlPlane));
        assert_eq!(s.hosts[&hid("v3")].roles, [Role::Compute].into());
    }
}
