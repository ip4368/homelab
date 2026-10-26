//! Drain and live migration.
//!
//! * **Live migration.** A VM is committed `Migrating { to }` first (its host
//!   still the source; only a VM `Running` there may start one, so the API
//!   and a drain never both start one). Then the target's agent is sent
//!   `MigrateIn` with the VM's spec and a deadline [`MIGRATION_MARGIN_MS`]
//!   short of [`MIGRATION_TIMEOUT_MS`] after the migration began (again
//!   every [`RESEND_MS`] until the target runs it). The target starts an
//!   incoming instance and copies the VM's memory from the source over the
//!   `migration` network; once the copy is complete the incoming instance
//!   takes over and the target reports it (`MigrationDone`, then its
//!   heartbeats list the instance). The VM is committed `Running` on the
//!   target, and a source that still lists it is sent `StopInstance`
//!   (failover's stale-instance cleanup).
//! * **Failed migration.** The target gives an incoming instance up, never
//!   completing it, at the deadline (or once the source no longer runs the
//!   VM). [`MIGRATION_TIMEOUT_MS`] after the migration began (the migration
//!   network is blackholed, say), once a heartbeat of the target's from
//!   after the deadline does not list the VM (or [`MIGRATION_UNHEARD_MS`]
//!   later, if none comes), the VM is committed `Running` on the source
//!   again, the target is sent `AbortMigration` (which never stops a running
//!   instance), and an event records it. A target that does run the VM by
//!   then is adopted instead.
//! * **Drain.** A `Draining` host is emptied one step at a time, each step
//!   once the one before is done:
//!   1. its VMs live-migrate away, one at a time, each to the host the
//!      scheduler picks;
//!   2. its pods move to the hosts the scheduler picks;
//!   3. each of its volume replicas gets a replacement on another host,
//!      copied from a healthy replica, and is deleted once the volume has all
//!      its replicas healthy without it (the storage controller does this for
//!      the hosts [`evacuating`] names);
//!   4. once the state places nothing on it and its agent reports no
//!      instances and no replicas, it is `Drained`.
//!
//!   Everything a drain goes by is in the replicated state, so a new leader
//!   picks it up where it was. Uncordoning the host (`Ready`) cancels it.
//! * **Blocked.** While a step finds no host to move a workload to, or
//!   cannot go on for another reason (a VM starting on the host for
//!   [`DRAIN_STALL_MS`], a replica kept because its volume has no healthy
//!   one, an instance the control plane does not manage), a
//!   [`DRAIN_BLOCKED`] alert on the draining host says which and why; its
//!   message follows the reason, and it is resolved once the drain gets
//!   past that (or is cancelled).

use super::alerts::AlertKind;
use super::failover::assigned;
use super::scheduler::{agent, in_flight, reported, vm_spec, Hosts, RESEND_MS, STARTING};
use super::storage::{excluded, storage_views};
use super::{Action, Volatile};
use crate::proto::{mgmt, vm_instance, Msg, MIGRATION_MARGIN_MS, MIGRATION_TIMEOUT_MS};
use crate::runtime::SimTime;
use crate::state::{ClusterState, Command, Lifecycle, ObjectKind, ObjectRef};
use model::{place_replicas, HostId, PlacementReq, PodPhase, PodStatus, ReplicaState, VmPhase, VmStatus};
use std::collections::{BTreeMap, BTreeSet};

/// A failed migration whose target was not heard from after its deadline
/// goes back to its source this long after the timeout anyway.
pub const MIGRATION_UNHEARD_MS: u64 = 60_000;
/// A VM starting on a draining host this long blocks the drain (it is said so).
pub const DRAIN_STALL_MS: u64 = 60_000;
pub const DRAIN_BLOCKED: &str = AlertKind::DrainBlocked.as_str();

/// Moves migrations on and drains the draining hosts. The scheduler calls
/// it with its host views, so placements of both count against each other,
/// and with its own actions of this run (`scheduled`): a pod or VM those
/// propose a status for is left alone until it applies.
pub(super) fn run(state: &ClusterState, v: &Volatile, now: SimTime, hosts: &mut Hosts, scheduled: &[Action]) -> Vec<Action> {
    let mut out = migrations(state, v, now, scheduled);
    let mut blocked = BTreeMap::new();
    for h in state.hosts.values().filter(|h| h.lifecycle == Lifecycle::Draining) {
        let busy: Vec<&Action> = scheduled.iter().chain(&out).collect();
        let (actions, why) = drain(state, v, now, &h.id, hosts, &busy);
        out.extend(actions);
        if let Some(why) = why {
            blocked.insert(h.id.clone(), why);
        }
    }
    out.extend(alerts(state, v, blocked));
    out
}

/// The draining (or drained) hosts whose replicas move off now: those with
/// no VM or pod left on them.
pub fn evacuating(state: &ClusterState) -> BTreeSet<HostId> {
    state
        .hosts
        .values()
        .filter(|h| matches!(h.lifecycle, Lifecycle::Draining | Lifecycle::Drained))
        .filter(|h| vms_on(state, &h.id).is_empty() && pods_on(state, &h.id).is_empty())
        .map(|h| h.id.clone())
        .collect()
}

/// The `SetVmStatus` that starts live-migrating running VM `vm` to `to`, or
/// with `None` to the host the scheduler picks (never its own), and the
/// target; or why it cannot.
pub fn migrate_command(state: &ClusterState, v: &Volatile, vm: &str, to: Option<HostId>) -> Result<(Command, HostId), String> {
    let (Some(st), Some(obj)) = (state.vm_status.get(vm), state.vms.get(vm)) else { return Err(format!("vm {vm} not found")) };
    if in_flight(v, vm_status_of(vm)) {
        return Err(format!("vm {vm} is busy"));
    }
    let from = match (&st.status.phase, &st.status.host) {
        (VmPhase::Running, Some(h)) => h.clone(),
        (VmPhase::Migrating { .. }, _) => return Err(format!("vm {vm} is already migrating")),
        _ => return Err(format!("vm {vm} is not running")),
    };
    let req = PlacementReq::vm(&obj.vm);
    let mut hosts = Hosts::new(state, v);
    let target = match to {
        None => hosts.place_excluding(&req, &[from.clone()].into()).map_err(|why| format!("cannot migrate vm {vm}: {why}"))?,
        Some(to) if to == from => return Err(format!("vm {vm} already runs on {to}")),
        Some(to) => hosts.place_on(&req, &to).map_err(|why| format!("cannot migrate vm {vm} to {to}: {why}"))?,
    };
    let cmd = Command::SetVmStatus { id: vm.into(), host: Some(from), phase: VmPhase::Migrating { to: target.clone() } };
    Ok((cmd, target))
}

/// Whether one of `actions` proposes a command matching `pick`.
fn proposes<'a>(actions: impl IntoIterator<Item = &'a Action>, pick: impl Fn(&Command) -> bool) -> bool {
    actions.into_iter().any(|a| matches!(a, Action::Propose(c) if pick(c)))
}

fn vm_status_of(id: &str) -> impl Fn(&Command) -> bool + '_ {
    move |c| matches!(c, Command::SetVmStatus { id: i, .. } if i == id)
}

/// The VMs whose status puts them on `host`: starting, running or migrating
/// there, or migrating to it.
fn vms_on<'a>(state: &'a ClusterState, host: &HostId) -> Vec<(&'a String, &'a VmStatus)> {
    state
        .vm_status
        .iter()
        .map(|(id, st)| (id, &st.status))
        .filter(|(_, st)| match &st.phase {
            VmPhase::Starting | VmPhase::Running => st.host.as_ref() == Some(host),
            VmPhase::Migrating { to } => st.host.as_ref() == Some(host) || to == host,
            VmPhase::Pending(_) | VmPhase::Stopped => false,
        })
        .collect()
}

/// The pods placed on `host`.
fn pods_on<'a>(state: &'a ClusterState, host: &HostId) -> Vec<&'a PodStatus> {
    state.pods.values().map(|p| &p.status).filter(|p| p.host.as_ref() == Some(host)).collect()
}

/// The deadline the target of a migration that began at `since` is given:
/// past it the target never completes the migration.
pub fn migration_deadline(since: SimTime) -> SimTime {
    since + MIGRATION_TIMEOUT_MS - MIGRATION_MARGIN_MS
}

/// Each migrating VM: `Running` on the target once the target runs it,
/// back on the source once it timed out and the target is known not to,
/// else `MigrateIn` to the target until its deadline.
fn migrations(state: &ClusterState, v: &Volatile, now: SimTime, scheduled: &[Action]) -> Vec<Action> {
    let mut out = vec![];
    for (id, st) in &state.vm_status {
        let (VmPhase::Migrating { to }, Some(from)) = (&st.status.phase, &st.status.host) else { continue };
        let Some(vm) = state.vms.get(id).map(|o| &o.vm) else { continue };
        if in_flight(v, vm_status_of(id)) || proposes(scheduled, vm_status_of(id)) {
            continue;
        }
        let instance = vm_instance(id);
        let event = |message: String| Action::Propose(Command::Event { entity: ObjectRef::vm(id), message });
        let (since, deadline) = (st.status.since, migration_deadline(st.status.since));
        if reported(v, to, &instance) {
            // Even past the timeout: a target that runs the VM keeps it.
            out.push(Action::Propose(Command::SetVmStatus { id: id.clone(), host: Some(to.clone()), phase: VmPhase::Running }));
            out.push(event(format!("{id} migrated from {from} to {to}")));
        } else if now >= since + MIGRATION_TIMEOUT_MS {
            // A heartbeat from after the deadline that does not list the VM
            // means the target gave it up; a second's slack for its travel.
            let gave_up = v.last_heartbeat.get(to).is_some_and(|at| *at >= deadline + 1000);
            if gave_up || now >= since + MIGRATION_TIMEOUT_MS + MIGRATION_UNHEARD_MS {
                out.push(Action::Propose(Command::SetVmStatus { id: id.clone(), host: Some(from.clone()), phase: VmPhase::Running }));
                out.push(Action::Send(agent(to), mgmt(), Msg::AbortMigration(instance)));
                let secs = MIGRATION_TIMEOUT_MS / 1000;
                out.push(event(format!("migration of {id} from {from} to {to} failed: not complete after {secs} s; it runs on {from}")));
            }
        } else if now < deadline {
            let recent = v.instance_started.get(&instance).is_some_and(|(h, at)| h == to && now < at + RESEND_MS);
            if !recent {
                out.push(Action::Send(agent(to), mgmt(), Msg::MigrateIn { spec: vm_spec(state, vm, to), from: from.clone(), deadline }));
            }
        }
    }
    out
}

/// The next step of draining `host`, and why the drain is blocked, if it is.
/// A pod or VM with a status proposal in flight or among `busy` waits.
fn drain(state: &ClusterState, v: &Volatile, now: SimTime, host: &HostId, hosts: &mut Hosts, busy: &[&Action]) -> (Vec<Action>, Option<String>) {
    let event = |entity: ObjectRef, message: String| Action::Propose(Command::Event { entity, message });
    let away = BTreeSet::from([host.clone()]);
    // 1. VMs, one migration at a time.
    let vms = vms_on(state, host);
    if !vms.is_empty() {
        let waiting = |(id, st): &(&String, &VmStatus)| {
            st.phase != VmPhase::Running || in_flight(v, vm_status_of(id)) || proposes(busy.iter().copied(), vm_status_of(id))
        };
        if vms.iter().any(waiting) {
            let stuck = vms.iter().find(|(_, st)| st.phase == VmPhase::Starting && now.saturating_sub(st.since) >= DRAIN_STALL_MS);
            // A fixed message: one that counted the seconds would be rewritten every run.
            let why = stuck.map(|(id, _)| format!("vm {id} has been starting on {host} for over {} s", DRAIN_STALL_MS / 1000));
            return (vec![], why);
        }
        let mut blocked = None;
        for (id, _) in vms {
            let Some(vm) = state.vms.get(id).map(|o| &o.vm) else { continue };
            match hosts.place_excluding(&PlacementReq::vm(vm), &away) {
                Ok(to) => {
                    let status = Command::SetVmStatus { id: id.clone(), host: Some(host.clone()), phase: VmPhase::Migrating { to: to.clone() } };
                    let message = format!("draining {host}: migrating {id} to {to}");
                    return (vec![Action::Propose(status), event(ObjectRef::vm(id), message)], blocked);
                }
                Err(why) => {
                    blocked.get_or_insert(format!("cannot migrate vm {id} off {host}: {why}"));
                }
            }
        }
        return (vec![], blocked);
    }
    // 2. Pods, all at once.
    let pods = pods_on(state, host);
    if !pods.is_empty() {
        let (mut out, mut blocked) = (vec![], None);
        for p in pods {
            let Some(d) = state.deployments.get(&p.deployment).map(|o| &o.deployment) else { continue };
            let this = |c: &Command| matches!(c, Command::SetPodStatus { deployment, idx, .. } if *deployment == p.deployment && *idx == p.idx);
            if in_flight(v, this) || proposes(busy.iter().copied(), this) {
                continue;
            }
            let (dep, idx) = (&p.deployment, p.idx);
            match hosts.place_excluding(&PlacementReq::pod(d), &away) {
                Ok(to) => {
                    let phase = PodPhase::Pending(STARTING.into());
                    out.push(Action::Propose(Command::SetPodStatus { deployment: dep.clone(), idx, host: Some(to.clone()), phase }));
                    out.push(event(ObjectRef::pod(dep, idx), format!("draining {host}: moving pod {dep}/{idx} to {to}")));
                }
                Err(why) => {
                    blocked.get_or_insert(format!("cannot move pod {dep}/{idx} off {host}: {why}"));
                }
            }
        }
        return (out, blocked);
    }
    // 3. Replicas: the storage controller moves them; say if one has nowhere
    // to go (once every host was heard from: placement skips the others).
    let heard_all = state.hosts.values().all(|h| h.lifecycle == Lifecycle::NotReady || v.replicas.contains_key(&h.id));
    let mut views = None;
    let mut blocked = None;
    let mut leaving = false;
    for (volume, st) in &state.volume_status {
        let replicas = &st.status.replicas;
        if !replicas.iter().any(|r| r.host == *host && r.state != ReplicaState::Stale) {
            continue;
        }
        leaving = true;
        let Some(vol) = state.volumes.get(volume).map(|o| &o.volume) else { continue };
        let staying = replicas.iter().filter(|r| r.host != *host && r.state != ReplicaState::Stale).count();
        if !heard_all || blocked.is_some() || staying >= usize::from(vol.replicas) {
            continue;
        }
        let views = views.get_or_insert_with(|| storage_views(state));
        if place_replicas(1, vol.size_gb, views, &excluded(state, v, volume, replicas, true)).is_empty() {
            blocked = Some(format!("cannot move volume {volume}'s replica off {host}: no storage host can take one"));
        }
    }
    if leaving {
        return (vec![], blocked);
    }
    // 4. Drained, once the host itself says it is empty. Say what stays that
    // nothing will remove: an instance of no form the control plane knows
    // (failover leaves those alone), a replica kept for salvage.
    let (Some(instances), Some(replicas)) = (v.instances.get(host), v.replicas.get(host)) else { return (vec![], None) };
    if !instances.is_empty() || !replicas.is_empty() {
        let unmanaged = instances.iter().find(|i| assigned(state, host, i));
        let kept = replicas.iter().find(|r| {
            state.volume_status.get(&r.volume).is_some_and(|st| {
                let rs = &st.status.replicas;
                rs.iter().any(|x| x.host == *host) && !rs.iter().any(|x| x.state == ReplicaState::Healthy)
            })
        });
        let why = match (unmanaged, kept) {
            (Some(i), _) => Some(format!("{host} still runs {i}, which the control plane does not manage")),
            (None, Some(r)) => Some(format!("volume {}'s replica on {host} is kept: the volume has no healthy replica", r.volume)),
            (None, None) => None,
        };
        return (vec![], why);
    }
    let drained = Command::SetLifecycle { id: host.clone(), lifecycle: Lifecycle::Drained };
    (vec![Action::Propose(drained), event(ObjectRef::host(host), format!("{host} drained"))], None)
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
        out.push(Action::Propose(Command::RaiseAlert {
            id,
            kind: DRAIN_BLOCKED.into(),
            entity,
            subject: None,
            message,
            ground_truth: false,
        }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::scheduler::tests::{apply, deployment, healthy, hid, proposals, rebuilding, rep, sends, state, three_ms01, vm, STORAGE_VM};
    use super::super::{scheduler, storage};
    use super::*;
    use crate::proto::InstanceSpec;
    use model::{ProfileId, ReplicaRef, Volume, VolumeHealth};

    /// Four MS-01s; VMs `vm1` and `vm2` run on m1, each volume healthy on m1, m2 and m3.
    fn two_vms_on_m1() -> ClusterState {
        let mut s = three_ms01();
        let m4 = state(&[("m4", ProfileId::Ms01, STORAGE_VM, "c")]).hosts.into_values().next().unwrap();
        s.hosts.insert(m4.id.clone(), m4);
        for id in ["vm1", "vm2"] {
            let volume = format!("{id}-disk");
            s.apply(0, Command::CreateVolume { volume: Volume::new(&volume, 40) }).unwrap();
            let replicas = vec![healthy("m1"), healthy("m2"), healthy("m3")];
            s.apply(0, Command::SetVolumeStatus { id: volume, replicas, health: VolumeHealth::Healthy }).unwrap();
            s.apply(0, Command::CreateVm { vm: vm(id, 2) }).unwrap();
            s.apply(0, Command::SetVmStatus { id: id.into(), host: Some(hid("m1")), phase: VmPhase::Running }).unwrap();
        }
        s
    }

    fn set_lifecycle(s: &mut ClusterState, host: &str, lifecycle: Lifecycle) {
        s.apply(0, Command::SetLifecycle { id: hid(host), lifecycle }).unwrap();
    }

    fn migrating(s: &mut ClusterState, at: SimTime, id: &str, from: &str, to: &str) {
        s.apply(at, Command::SetVmStatus { id: id.into(), host: Some(hid(from)), phase: VmPhase::Migrating { to: hid(to) } }).unwrap();
    }

    fn vm_statuses(cmds: &[Command]) -> Vec<Command> {
        cmds.iter().filter(|c| matches!(c, Command::SetVmStatus { .. })).cloned().collect()
    }

    fn events(cmds: &[Command]) -> Vec<String> {
        cmds.iter().filter_map(|c| if let Command::Event { message, .. } = c { Some(message.clone()) } else { None }).collect()
    }

    fn raised(cmds: &[Command]) -> Vec<(String, String)> {
        cmds.iter()
            .filter_map(|c| match c {
                Command::RaiseAlert { kind, entity, message, .. } if kind == DRAIN_BLOCKED => Some((entity.id.clone(), message.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn migrate_vm_picks_a_target_or_says_why_not() {
        let s = two_vms_on_m1();
        let v = Volatile::new(0, 1);
        let (cmd, to) = migrate_command(&s, &v, "vm1", None).unwrap();
        assert_ne!(to, hid("m1"));
        assert_eq!(cmd, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Migrating { to } });
        let (_, to) = migrate_command(&s, &v, "vm1", Some(hid("m3"))).unwrap();
        assert_eq!(to, hid("m3"));
        assert_eq!(migrate_command(&s, &v, "vm1", Some(hid("m1"))).unwrap_err(), "vm vm1 already runs on m1");
        assert_eq!(migrate_command(&s, &v, "nope", None).unwrap_err(), "vm nope not found");
        assert_eq!(migrate_command(&s, &v, "vm1", Some(hid("zz"))).unwrap_err(), "cannot migrate vm vm1 to zz: no host zz");
        let mut s2 = s.clone();
        set_lifecycle(&mut s2, "m3", Lifecycle::Cordoned);
        assert_eq!(migrate_command(&s2, &v, "vm1", Some(hid("m3"))).unwrap_err(), "cannot migrate vm vm1 to m3: no fit: 1 cordoned or draining");
        migrating(&mut s2, 0, "vm1", "m1", "m2");
        assert_eq!(migrate_command(&s2, &v, "vm1", None).unwrap_err(), "vm vm1 is already migrating");
        s2.apply(0, Command::SetVmStatus { id: "vm2".into(), host: None, phase: VmPhase::Pending("x".into()) }).unwrap();
        assert_eq!(migrate_command(&s2, &v, "vm2", None).unwrap_err(), "vm vm2 is not running");
    }

    #[test]
    fn a_migration_is_sent_to_its_target_until_it_runs_there() {
        let mut s = two_vms_on_m1();
        migrating(&mut s, 1000, "vm1", "m1", "m2");
        let mut v = Volatile::new(0, 1);
        let out = scheduler::run(&s, &v, 2000);
        let [(to, Msg::MigrateIn { spec, from, deadline })]: [(HostId, Msg); 1] = sends(&out).try_into().unwrap() else { panic!("{out:?}") };
        assert_eq!((to, spec.id.as_str(), from, deadline), (hid("m2"), "vm/vm1", hid("m1"), 1000 + MIGRATION_TIMEOUT_MS - MIGRATION_MARGIN_MS));
        assert!(proposals(&out).is_empty(), "{out:?}");
        v.note_sent(&agent(&hid("m2")), &Msg::MigrateIn { spec: spec.clone(), from: hid("m1"), deadline }, 2000);
        assert!(sends(&scheduler::run(&s, &v, 2000 + RESEND_MS - 1)).is_empty());
        assert_eq!(sends(&scheduler::run(&s, &v, 2000 + RESEND_MS)).len(), 1, "not running yet: sent again");
        assert!(sends(&scheduler::run(&s, &v, deadline)).is_empty(), "not past its deadline");
        // The target runs it: the VM runs there.
        v.instances.insert(hid("m2"), ["vm/vm1".to_string()].into());
        let out = proposals(&scheduler::run(&s, &v, 15_000));
        assert_eq!(vm_statuses(&out), [Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m2")), phase: VmPhase::Running }]);
        assert_eq!(events(&out), ["vm1 migrated from m1 to m2"]);
    }

    #[test]
    fn a_migration_not_complete_after_120_s_goes_back_to_its_source() {
        let mut s = two_vms_on_m1();
        migrating(&mut s, 1000, "vm1", "m1", "m2");
        let mut v = Volatile::new(0, 1);
        v.instances.insert(hid("m2"), Default::default());
        v.instance_started.insert("vm/vm1".into(), (hid("m2"), 1000 + MIGRATION_TIMEOUT_MS - 2000));
        assert!(scheduler::run(&s, &v, 1000 + MIGRATION_TIMEOUT_MS - 1).is_empty());
        // Timed out, but the target was last heard before its deadline: it may have the VM.
        v.last_heartbeat.insert(hid("m2"), migration_deadline(1000) - 2000);
        assert!(scheduler::run(&s, &v, 1000 + MIGRATION_TIMEOUT_MS).is_empty(), "waits for word from the target");
        // A heartbeat from after the deadline without the VM: the target gave it up.
        v.last_heartbeat.insert(hid("m2"), migration_deadline(1000) + 1000);
        let out = scheduler::run(&s, &v, 1000 + MIGRATION_TIMEOUT_MS);
        assert_eq!(vm_statuses(&proposals(&out)), [Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Running }]);
        assert_eq!(sends(&out), [(hid("m2"), Msg::AbortMigration("vm/vm1".into()))], "an abort, never a stop");
        assert_eq!(events(&proposals(&out)), ["migration of vm1 from m1 to m2 failed: not complete after 120 s; it runs on m1"]);
        // Nothing is said twice while the status is in flight.
        v.in_flight.insert(1, vm_statuses(&proposals(&out))[0].clone());
        assert!(scheduler::run(&s, &v, 1000 + MIGRATION_TIMEOUT_MS + 1000).is_empty());
    }

    #[test]
    fn a_failed_migration_whose_target_is_not_heard_goes_back_a_minute_later() {
        let mut s = two_vms_on_m1();
        migrating(&mut s, 1000, "vm1", "m1", "m2");
        let mut v = Volatile::new(0, 1);
        v.last_heartbeat.insert(hid("m2"), 5000);
        let late = 1000 + MIGRATION_TIMEOUT_MS + MIGRATION_UNHEARD_MS;
        assert!(proposals(&scheduler::run(&s, &v, late - 1)).is_empty());
        let out = scheduler::run(&s, &v, late);
        assert_eq!(vm_statuses(&proposals(&out)), [Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Running }]);
        assert_eq!(sends(&out), [(hid("m2"), Msg::AbortMigration("vm/vm1".into()))]);
    }

    #[test]
    fn a_target_that_runs_the_vm_just_after_the_timeout_is_adopted() {
        let mut s = two_vms_on_m1();
        migrating(&mut s, 1000, "vm1", "m1", "m2");
        let mut v = Volatile::new(0, 1);
        // It completed just before its deadline; word of it comes after the timeout.
        v.instances.insert(hid("m2"), ["vm/vm1".to_string()].into());
        v.last_heartbeat.insert(hid("m2"), 1000 + MIGRATION_TIMEOUT_MS + 1);
        let out = scheduler::run(&s, &v, 1000 + MIGRATION_TIMEOUT_MS + 1);
        assert_eq!(vm_statuses(&proposals(&out)), [Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m2")), phase: VmPhase::Running }]);
        assert!(sends(&out).is_empty(), "nothing stops or aborts the target: {out:?}");
        assert_eq!(events(&proposals(&out)), ["vm1 migrated from m1 to m2"]);
    }

    #[test]
    fn migrate_vm_refuses_a_vm_whose_status_is_in_flight() {
        let s = two_vms_on_m1();
        let mut v = Volatile::new(0, 1);
        v.in_flight.insert(1, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Migrating { to: hid("m2") } });
        assert_eq!(migrate_command(&s, &v, "vm1", None).unwrap_err(), "vm vm1 is busy");
        assert!(migrate_command(&s, &v, "vm2", None).is_ok());
    }

    #[test]
    fn a_migration_starts_only_from_running_on_its_source_and_ends_on_its_source_or_target() {
        let mut s = two_vms_on_m1();
        let mig = |to: &str| Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Migrating { to: hid(to) } };
        let running = |on: &str| Command::SetVmStatus { id: "vm1".into(), host: Some(hid(on)), phase: VmPhase::Running };
        let bad = Err(crate::state::ApplyError::InvalidVmPhase);
        assert_eq!(s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m2")), phase: VmPhase::Migrating { to: hid("m3") } }), bad, "not its host");
        assert_eq!(s.apply(0, mig("m1")), bad, "not to its own host");
        s.apply(0, mig("m2")).unwrap();
        assert_eq!(s.apply(0, mig("m3")), bad, "a second migration does not start");
        assert_eq!(s.apply(0, running("m3")), bad, "nor ends anywhere but the source or target");
        s.apply(0, running("m2")).unwrap();
        assert_eq!(s.apply(0, mig("m3")), bad, "m1 no longer runs it");
        // Back on its source works too, and a stop or failover is not hindered.
        let mut s = two_vms_on_m1();
        s.apply(0, mig("m2")).unwrap();
        s.apply(0, running("m1")).unwrap();
        s.apply(0, mig("m2")).unwrap();
        s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m3")), phase: VmPhase::Starting }).unwrap();
        s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m3")), phase: VmPhase::Running }).unwrap();
    }

    #[test]
    fn a_drain_says_what_it_waits_for_that_will_not_go_by_itself() {
        let mut s = two_vms_on_m1();
        s.apply(0, Command::SetVmStatus { id: "vm2".into(), host: Some(hid("m2")), phase: VmPhase::Running }).unwrap();
        s.apply(1000, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Starting }).unwrap();
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let v = Volatile::new(0, 1);
        assert!(raised(&proposals(&scheduler::run(&s, &v, 1000 + DRAIN_STALL_MS - 1))).is_empty(), "a VM starting briefly is no block");
        let out = proposals(&scheduler::run(&s, &v, 1000 + DRAIN_STALL_MS));
        assert_eq!(raised(&out), [("m1".to_string(), "vm vm1 has been starting on m1 for over 60 s".to_string())]);
        let mut later = s.clone();
        apply(&mut later, out.clone());
        assert!(proposals(&scheduler::run(&later, &v, 1000 + DRAIN_STALL_MS + 30_000)).is_empty(), "the message stands");
        // The reason changes: the alert's message follows it.
        apply(&mut s, out);
        for id in ["vm1", "vm2"] {
            s.apply(0, Command::DeleteVm { id: id.into() }).unwrap();
            s.apply(0, Command::DeleteVolume { id: format!("{id}-disk") }).unwrap();
        }
        let mut v = Volatile::new(0, 1);
        for h in s.hosts.keys() {
            v.replicas.insert(h.clone(), vec![]);
        }
        v.instances.insert(hid("m1"), ["legacy-agent".to_string()].into());
        let out = proposals(&scheduler::run(&s, &v, 100_000));
        let why = "m1 still runs legacy-agent, which the control plane does not manage".to_string();
        assert!(out.contains(&Command::SetAlertMessage { id: "drainblocked-m1-1".into(), message: why }), "{out:?}");
        assert!(!out.iter().any(|c| matches!(c, Command::ResolveAlert { .. } | Command::RaiseAlert { .. })), "{out:?}");
    }

    #[test]
    fn a_replica_kept_for_salvage_blocks_the_drain_and_says_so() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("data", 40) }).unwrap();
        let replicas = vec![ReplicaRef { state: ReplicaState::Stale, ..healthy("m1") }, ReplicaRef { state: ReplicaState::Stale, ..healthy("m2") }];
        s.apply(0, Command::SetVolumeStatus { id: "data".into(), replicas, health: VolumeHealth::Faulted }).unwrap();
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let mut v = heard(&s, "data");
        v.instances.insert(hid("m1"), Default::default());
        let out = proposals(&scheduler::run(&s, &v, 1000));
        assert_eq!(raised(&out), [("m1".to_string(), "volume data's replica on m1 is kept: the volume has no healthy replica".to_string())]);
    }

    #[test]
    fn a_new_leader_says_a_replica_has_nowhere_to_go_only_once_it_heard_every_host() {
        let mut s = three_ms01();
        let m4 = state(&[("m4", ProfileId::Ms01, STORAGE_VM, "c")]).hosts.into_values().next().unwrap();
        s.hosts.insert(m4.id.clone(), m4);
        s.apply(0, Command::CreateVolume { volume: Volume::new("data", 40) }).unwrap();
        let replicas = vec![healthy("m1"), healthy("m2"), healthy("m3")];
        s.apply(0, Command::SetVolumeStatus { id: "data".into(), replicas, health: VolumeHealth::Healthy }).unwrap();
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let mut v = heard(&s, "data");
        v.replicas.remove(&hid("m4"));
        assert!(raised(&proposals(&scheduler::run(&s, &v, 1000))).is_empty(), "m4 might take it");
    }

    #[test]
    fn a_draining_host_migrates_its_vms_one_at_a_time() {
        let mut s = two_vms_on_m1();
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let v = Volatile::new(0, 1);
        let out = proposals(&scheduler::run(&s, &v, 1000));
        let statuses: [Command; 1] = vm_statuses(&out).try_into().unwrap();
        let [Command::SetVmStatus { id, host: Some(from), phase: VmPhase::Migrating { to } }] = statuses else { panic!("{out:?}") };
        assert_eq!((id.as_str(), from.as_str()), ("vm1", "m1"));
        assert_ne!(to, hid("m1"));
        assert_eq!(events(&out), [format!("draining m1: migrating vm1 to {to}")]);
        apply(&mut s, out);
        assert!(vm_statuses(&proposals(&scheduler::run(&s, &v, 2000))).is_empty(), "one at a time");
        s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: Some(to.clone()), phase: VmPhase::Running }).unwrap();
        let out = proposals(&scheduler::run(&s, &v, 3000));
        assert!(matches!(vm_statuses(&out).as_slice(), [Command::SetVmStatus { id, phase: VmPhase::Migrating { .. }, .. }] if id == "vm2"), "{out:?}");
    }

    #[test]
    fn a_drain_with_nowhere_to_go_is_blocked_with_an_alert_until_it_moves_on() {
        let mut s = two_vms_on_m1();
        for h in ["m2", "m3", "m4"] {
            set_lifecycle(&mut s, h, Lifecycle::Cordoned);
        }
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let v = Volatile::new(0, 1);
        let out = proposals(&scheduler::run(&s, &v, 1000));
        assert!(vm_statuses(&out).is_empty());
        let why = "cannot migrate vm vm1 off m1: no fit: 4 cordoned or draining".to_string();
        assert_eq!(raised(&out), [("m1".to_string(), why.clone())]);
        let Some(Command::RaiseAlert { id, .. }) = out.iter().find(|c| matches!(c, Command::RaiseAlert { .. })) else { unreachable!() };
        assert_eq!(id, "drainblocked-m1-1");
        apply(&mut s, out);
        assert!(proposals(&scheduler::run(&s, &v, 2000)).is_empty(), "raised once");
        // Room again: the drain moves on and the alert is resolved.
        set_lifecycle(&mut s, "m2", Lifecycle::Ready);
        let out = proposals(&scheduler::run(&s, &v, 3000));
        assert_eq!(vm_statuses(&out).len(), 1);
        assert!(out.contains(&Command::ResolveAlert { id: "drainblocked-m1-1".into() }), "{out:?}");
    }

    #[test]
    fn uncordoning_cancels_a_drain_and_resolves_its_alert() {
        let mut s = two_vms_on_m1();
        for h in ["m2", "m3", "m4"] {
            set_lifecycle(&mut s, h, Lifecycle::Cordoned);
        }
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let v = Volatile::new(0, 1);
        let out = proposals(&scheduler::run(&s, &v, 1000));
        apply(&mut s, out);
        set_lifecycle(&mut s, "m1", Lifecycle::Ready);
        set_lifecycle(&mut s, "m2", Lifecycle::Ready);
        let out = proposals(&scheduler::run(&s, &v, 2000));
        assert_eq!(out, [Command::ResolveAlert { id: "drainblocked-m1-1".into() }], "nothing moves off a host that is not draining");
    }

    #[test]
    fn pods_move_off_once_the_vms_are_gone_then_the_host_is_drained_once_empty() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateDeployment { deployment: deployment("web", 2, 100, None) }).unwrap();
        for idx in 0..2 {
            s.apply(0, Command::SetPodStatus { deployment: "web".into(), idx, host: Some(hid("m1")), phase: PodPhase::Running }).unwrap();
        }
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let mut v = Volatile::new(0, 1);
        let out = proposals(&scheduler::run(&s, &v, 1000));
        let moved: Vec<(u32, HostId)> = out
            .iter()
            .filter_map(|c| match c {
                Command::SetPodStatus { idx, host: Some(h), phase: PodPhase::Pending(why), .. } if why == STARTING => Some((*idx, h.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(moved.len(), 2, "{out:?}");
        assert!(moved.iter().all(|(_, h)| *h != hid("m1")));
        assert!(events(&out).contains(&format!("draining m1: moving pod web/0 to {}", moved[0].1)));
        apply(&mut s, out);
        // Nothing is on m1 in the state, but its agent has not said it is empty yet.
        assert!(proposals(&scheduler::run(&s, &v, 2000)).iter().all(|c| !matches!(c, Command::SetLifecycle { .. })));
        v.instances.insert(hid("m1"), ["pod/web/0".to_string()].into());
        v.replicas.insert(hid("m1"), vec![]);
        assert!(proposals(&scheduler::run(&s, &v, 3000)).iter().all(|c| !matches!(c, Command::SetLifecycle { .. })));
        v.instances.insert(hid("m1"), Default::default());
        let out = proposals(&scheduler::run(&s, &v, 4000));
        assert!(out.contains(&Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::Drained }), "{out:?}");
        assert_eq!(events(&out), ["m1 drained"]);
    }

    /// A leader that has heard from every host of `s`, which hold `volume` as its status says.
    fn heard(s: &ClusterState, volume: &str) -> Volatile {
        let mut v = Volatile::new(0, 1);
        for h in s.hosts.keys() {
            v.replicas.insert(h.clone(), vec![]);
            v.last_heartbeat.insert(h.clone(), SimTime::MAX);
        }
        for r in &s.volume_status[volume].status.replicas {
            v.replicas.insert(r.host.clone(), vec![rep(volume, &r.disk, 100, false)]);
        }
        v
    }

    fn volume_status(out: &[Command]) -> Option<Vec<ReplicaRef>> {
        out.iter().find_map(|c| if let Command::SetVolumeStatus { replicas, .. } = c { Some(replicas.clone()) } else { None })
    }

    #[test]
    fn replicas_leave_a_drained_host_only_once_replaced() {
        let mut s = two_vms_on_m1();
        s.apply(0, Command::DeleteVm { id: "vm2".into() }).unwrap();
        s.apply(0, Command::DeleteVolume { id: "vm2-disk".into() }).unwrap();
        s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m2")), phase: VmPhase::Running }).unwrap();
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        assert_eq!(evacuating(&s), [hid("m1")].into());
        let mut v = heard(&s, "vm1-disk");
        let out = proposals(&storage::run(&s, &v, 1000));
        let placed = volume_status(&out).expect("a replacement");
        assert_eq!(placed, [healthy("m1"), healthy("m2"), healthy("m3"), rebuilding("m4", 0)], "m1 stays until m4 is healthy");
        assert!(!out.iter().any(|c| matches!(c, Command::RaiseAlert { .. })), "no volume alert: {out:?}");
        apply(&mut s, out);
        let out = storage::run(&s, &v, 2000);
        let [(to, Msg::CreateReplica { volume, source: Some(source), .. })]: [(HostId, Msg); 1] = sends(&out).try_into().unwrap() else { panic!("{out:?}") };
        assert_eq!((to.as_str(), volume.as_str()), ("m4", "vm1-disk"));
        assert_ne!(source, hid("m1"), "a Ready host is the copy source");
        assert!(proposals(&scheduler::run(&s, &v, 2000)).is_empty(), "the drain waits");
        // Half way, m1 is still needed.
        v.replicas.insert(hid("m4"), vec![rep("vm1-disk", "nvme0", 50, true)]);
        let out = proposals(&storage::run(&s, &v, 3000));
        assert_eq!(volume_status(&out).unwrap()[..3], [healthy("m1"), healthy("m2"), healthy("m3")]);
        apply(&mut s, out);
        // m4 complete: m1's replica goes, and is deleted.
        v.replicas.insert(hid("m4"), vec![rep("vm1-disk", "nvme0", 100, false)]);
        let out = proposals(&storage::run(&s, &v, 4000));
        assert_eq!(volume_status(&out).unwrap(), [healthy("m2"), healthy("m3"), healthy("m4")]);
        apply(&mut s, out);
        assert_eq!(sends(&storage::run(&s, &v, 5000)), [(hid("m1"), Msg::DeleteReplica { volume: "vm1-disk".into() })]);
    }

    #[test]
    fn a_replica_with_nowhere_to_go_stays_and_blocks_the_drain() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("data", 40) }).unwrap();
        let replicas = vec![healthy("m1"), healthy("m2"), healthy("m3")];
        s.apply(0, Command::SetVolumeStatus { id: "data".into(), replicas: replicas.clone(), health: VolumeHealth::Healthy }).unwrap();
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let v = heard(&s, "data");
        assert!(storage::run(&s, &v, 1000).is_empty(), "no replacement, no change, no volume alert");
        let out = proposals(&scheduler::run(&s, &v, 1000));
        assert_eq!(raised(&out), [("m1".to_string(), "cannot move volume data's replica off m1: no storage host can take one".to_string())]);
    }

    #[test]
    fn a_copy_onto_a_draining_host_is_given_up() {
        let mut s = three_ms01();
        let m4 = state(&[("m4", ProfileId::Ms01, STORAGE_VM, "c")]).hosts.into_values().next().unwrap();
        s.hosts.insert(m4.id.clone(), m4);
        s.apply(0, Command::CreateVolume { volume: Volume::new("data", 40) }).unwrap();
        let replicas = vec![healthy("m2"), healthy("m3"), rebuilding("m1", 30)];
        s.apply(0, Command::SetVolumeStatus { id: "data".into(), replicas, health: VolumeHealth::Rebuilding }).unwrap();
        set_lifecycle(&mut s, "m1", Lifecycle::Draining);
        let mut v = heard(&s, "data");
        v.replicas.insert(hid("m1"), vec![rep("data", "nvme0", 30, true)]);
        let out = proposals(&storage::run(&s, &v, 1000));
        assert_eq!(volume_status(&out).unwrap(), [healthy("m2"), healthy("m3"), rebuilding("m4", 0)]);
    }

    #[test]
    fn a_migration_spec_is_what_the_target_would_run() {
        let mut s = two_vms_on_m1();
        migrating(&mut s, 0, "vm1", "m1", "m2");
        let out = scheduler::run(&s, &Volatile::new(0, 1), 1000);
        let [(_, Msg::MigrateIn { spec, .. })]: [(HostId, Msg); 1] = sends(&out).try_into().unwrap() else { panic!("{out:?}") };
        let expected: InstanceSpec = vm_spec(&s, &s.vms["vm1"].vm, &hid("m2"));
        assert_eq!(spec, expected);
        assert!(spec.flows.iter().all(|f| f.to != crate::proto::FlowEnd::Host(hid("m2"))), "no replica writes to itself");
    }
}
