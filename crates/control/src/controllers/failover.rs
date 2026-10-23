//! Failover: what the control plane does about workloads on hosts it lost.
//!
//! * **VMs.** A VM whose host has been `NotReady` for more than
//!   [`FAILOVER_GRACE_MS`] (or is gone) is placed again with that host
//!   excluded and goes `Starting` on the new host, once its volume has a
//!   healthy replica. Otherwise it goes `Pending` with the reason (waiting
//!   for the volume, or no fit), and the scheduler tries again, every
//!   [`NO_FIT_RETRY_MS`] if nothing fit. An event records the outcome.
//! * **Pods.** A pod whose host is lost goes back to `Pending` with no host
//!   ([`UNSCHEDULED`]), so the scheduler places it again; with an event.
//! * **Stale instances.** An instance that a host's heartbeat lists but the
//!   state does not assign to that host (its VM or pod moved elsewhere, is
//!   pending, or was deleted) is stopped with `StopInstance`, at most every
//!   [`RESEND_MS`]. There is no fencing: until the old host is heard from
//!   again a failed-over VM may run twice (split brain), which only ground
//!   truth can show.

use super::scheduler::{agent, volume_ready, Hosts, RESEND_MS};
use super::{Action, Volatile};
use crate::proto::{mgmt, Msg};
use crate::runtime::SimTime;
use crate::state::{ClusterState, Command, Lifecycle, ObjectRef, UNSCHEDULED};
use model::{HostId, PlacementReq, PodPhase, Vm, VmPhase};

/// How long a host must have been `NotReady` before its VMs are restarted
/// elsewhere and its volume replicas count as lost.
pub const FAILOVER_GRACE_MS: u64 = 60_000;
/// A VM that fits nowhere is tried again this often.
pub const NO_FIT_RETRY_MS: u64 = 30_000;
/// How a placement that found no host begins ([`model::place`]).
pub const NO_FIT: &str = "no fit:";

/// Whether `host` is lost: gone from the state, or `NotReady` for more than [`FAILOVER_GRACE_MS`].
pub fn host_lost(state: &ClusterState, host: &HostId, now: SimTime) -> bool {
    state
        .hosts
        .get(host)
        .is_none_or(|h| h.lifecycle == Lifecycle::NotReady && now.saturating_sub(h.last_transition) > FAILOVER_GRACE_MS)
}

/// Restarts VM `id`, which ran on the lost host `from`, on another host: the
/// status that places it (or says why it cannot start), and an event.
pub(super) fn fail_over(state: &ClusterState, now: SimTime, id: &str, vm: &Vm, from: &HostId, hosts: &mut Hosts) -> Vec<Action> {
    // `from` is lost, so its replica does not count towards the volume being ready.
    let placed = volume_ready(state, &vm.volume, now).and_then(|()| hosts.place_excluding(&PlacementReq::vm(vm), &[from.clone()].into()));
    let (host, phase, message) = match placed {
        Ok(to) => (Some(to.clone()), VmPhase::Starting, format!("{from} lost: restarting {id} on {to}")),
        Err(why) => (None, VmPhase::Pending(why.clone()), format!("{from} lost: {id} cannot restart, {why}")),
    };
    vec![
        Action::Propose(Command::SetVmStatus { id: id.into(), host, phase }),
        Action::Propose(Command::Event { entity: ObjectRef::vm(id), message }),
    ]
}

/// Sends pod `idx` of `deployment`, which ran on the lost host `from`, back
/// to the scheduler: pending with no host, and an event.
pub(super) fn evict_pod(deployment: &str, idx: u32, from: &HostId) -> Vec<Action> {
    let phase = PodPhase::Pending(UNSCHEDULED.into());
    vec![
        Action::Propose(Command::SetPodStatus { deployment: deployment.into(), idx, host: None, phase }),
        Action::Propose(Command::Event {
            entity: ObjectRef::pod(deployment, idx),
            message: format!("{from} lost: rescheduling pod {deployment}/{idx}"),
        }),
    ]
}

/// `StopInstance` for every instance a host runs that the state does not assign to it.
pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = vec![];
    for (host, instances) in &v.instances {
        for id in instances.iter().filter(|id| !assigned(state, host, id)) {
            let recent = v.instance_stopped.get(&(host.clone(), id.clone())).is_some_and(|at| now < at + RESEND_MS);
            if !recent {
                out.push(Action::Send(agent(host), mgmt(), Msg::StopInstance(id.clone())));
            }
        }
    }
    out
}

/// Whether the state wants `instance` running on `host`: its VM starting or
/// running there (or migrating to or from it), its pod placed there.
/// Instances of no form the control plane knows are left alone.
fn assigned(state: &ClusterState, host: &HostId, instance: &str) -> bool {
    if let Some(vm) = instance.strip_prefix("vm/") {
        return state.vm_status.get(vm).is_some_and(|s| {
            let on = s.status.host.as_ref() == Some(host);
            match &s.status.phase {
                VmPhase::Starting | VmPhase::Running => on,
                VmPhase::Migrating { to } => on || to == host,
                VmPhase::Pending(_) | VmPhase::Stopped => false,
            }
        });
    }
    let Some((deployment, idx)) = instance.strip_prefix("pod/").and_then(|p| p.rsplit_once('/')) else { return true };
    let Ok(idx) = idx.parse::<u32>() else { return true };
    state.pods.get(&(deployment.to_string(), idx)).is_some_and(|p| p.status.host.as_ref() == Some(host))
}

#[cfg(test)]
mod tests {
    use super::super::scheduler::tests::{apply, deployment, hid, proposals, sends, three_ms01, vm};
    use super::super::scheduler::{self, STARTING};
    use super::*;
    use model::{PodPhase, ReplicaRef, ReplicaState, Volume, VolumeHealth};

    /// Three MS-01s with VM `vm1` (and its healthy volume) running on m1.
    fn running_on_m1() -> ClusterState {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("vm1-disk", 40) }).unwrap();
        let replicas = ["m1", "m2", "m3"].map(|h| ReplicaRef { host: hid(h), disk: "nvme0".into(), state: ReplicaState::Healthy }).to_vec();
        s.apply(0, Command::SetVolumeStatus { id: "vm1-disk".into(), replicas, health: VolumeHealth::Healthy }).unwrap();
        s.apply(0, Command::CreateVm { vm: vm("vm1", 2) }).unwrap();
        s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Running }).unwrap();
        s
    }

    fn not_ready(s: &mut ClusterState, host: &str, at: SimTime) {
        s.apply(at, Command::SetLifecycle { id: hid(host), lifecycle: Lifecycle::NotReady }).unwrap();
    }

    fn vm_statuses(cmds: &[Command]) -> Vec<Command> {
        cmds.iter().filter(|c| matches!(c, Command::SetVmStatus { .. })).cloned().collect()
    }

    #[test]
    fn a_vm_restarts_elsewhere_once_its_host_is_not_ready_for_the_grace_period() {
        let mut s = running_on_m1();
        not_ready(&mut s, "m1", 10_000);
        let mut v = Volatile::new(0, 1);
        v.instances.insert(hid("m1"), ["vm/vm1".to_string()].into());
        assert!(vm_statuses(&proposals(&scheduler::run(&s, &v, 10_000 + FAILOVER_GRACE_MS))).is_empty(), "not past the grace yet");
        let out = proposals(&scheduler::run(&s, &v, 10_001 + FAILOVER_GRACE_MS));
        let moved = Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m2")), phase: VmPhase::Starting };
        assert_eq!(vm_statuses(&out), [moved]);
        assert!(out.iter().any(|c| matches!(c, Command::Event { message, .. } if message == "m1 lost: restarting vm1 on m2")), "{out:?}");
        apply(&mut s, out);
        // The scheduler takes it from there.
        let out = scheduler::run(&s, &v, 12_000 + FAILOVER_GRACE_MS);
        assert!(matches!(sends(&out).as_slice(), [(h, Msg::StartInstance(spec))] if *h == hid("m2") && spec.id == "vm/vm1"), "{out:?}");
    }

    #[test]
    fn a_removed_host_is_lost_at_once() {
        let mut s = running_on_m1();
        s.apply(0, Command::RemoveHost { id: hid("m1") }).unwrap();
        let out = proposals(&scheduler::run(&s, &Volatile::new(0, 1), 1000));
        assert!(matches!(vm_statuses(&out).as_slice(), [Command::SetVmStatus { host: Some(h), phase: VmPhase::Starting, .. }] if *h != hid("m1")), "{out:?}");
    }

    #[test]
    fn without_room_the_vm_pends_and_is_retried_every_30_s() {
        let mut s = running_on_m1();
        for h in ["m2", "m3"] {
            s.apply(0, Command::SetLifecycle { id: hid(h), lifecycle: Lifecycle::Cordoned }).unwrap();
        }
        not_ready(&mut s, "m1", 0);
        let v = Volatile::new(0, 1);
        let t0 = FAILOVER_GRACE_MS + 1000;
        let out = proposals(&scheduler::run(&s, &v, t0));
        let why = "no fit: 1 not ready, 2 cordoned or draining".to_string();
        assert_eq!(vm_statuses(&out), [Command::SetVmStatus { id: "vm1".into(), host: None, phase: VmPhase::Pending(why.clone()) }]);
        assert!(out.iter().any(|c| matches!(c, Command::Event { message, .. } if *message == format!("m1 lost: vm1 cannot restart, {why}"))));
        for c in out {
            s.apply(t0, c).unwrap();
        }
        // m2 has room again, but nothing is tried until the next retry.
        s.apply(t0, Command::SetLifecycle { id: hid("m2"), lifecycle: Lifecycle::Ready }).unwrap();
        for k in 1..30 {
            assert!(vm_statuses(&proposals(&scheduler::run(&s, &v, t0 + k * 1000))).is_empty(), "tried at +{k} s");
        }
        let out = proposals(&scheduler::run(&s, &v, t0 + NO_FIT_RETRY_MS));
        assert_eq!(vm_statuses(&out), [Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m2")), phase: VmPhase::Starting }]);
    }

    #[test]
    fn a_vm_whose_volume_has_no_healthy_replica_waits_for_it() {
        let mut s = running_on_m1();
        let stale = vec![ReplicaRef { host: hid("m1"), disk: "nvme0".into(), state: ReplicaState::Stale }];
        s.apply(0, Command::SetVolumeStatus { id: "vm1-disk".into(), replicas: stale, health: VolumeHealth::Faulted }).unwrap();
        not_ready(&mut s, "m1", 0);
        let out = proposals(&scheduler::run(&s, &Volatile::new(0, 1), FAILOVER_GRACE_MS + 1000));
        let waiting = VmPhase::Pending("waiting for volume vm1-disk".into());
        assert_eq!(vm_statuses(&out), [Command::SetVmStatus { id: "vm1".into(), host: None, phase: waiting }]);
    }

    #[test]
    fn a_vm_whose_only_healthy_replica_was_on_the_lost_host_waits() {
        let mut s = running_on_m1();
        let only_m1 = vec![ReplicaRef { host: hid("m1"), disk: "nvme0".into(), state: ReplicaState::Healthy }];
        s.apply(0, Command::SetVolumeStatus { id: "vm1-disk".into(), replicas: only_m1, health: VolumeHealth::Healthy }).unwrap();
        not_ready(&mut s, "m1", 0);
        let v = Volatile::new(0, 1);
        let now = FAILOVER_GRACE_MS + 1000;
        let out = scheduler::run(&s, &v, now);
        let waiting = VmPhase::Pending("waiting for volume vm1-disk".into());
        assert_eq!(vm_statuses(&proposals(&out)), [Command::SetVmStatus { id: "vm1".into(), host: None, phase: waiting.clone() }], "m1's replica is still Healthy in the status, but m1 is lost");
        assert!(sends(&out).is_empty(), "{out:?}");
        // A VM already starting elsewhere when that happens goes back to waiting, without StartInstance.
        s.apply(now, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m2")), phase: VmPhase::Starting }).unwrap();
        let out = scheduler::run(&s, &v, now + 1000);
        assert_eq!(vm_statuses(&proposals(&out)), [Command::SetVmStatus { id: "vm1".into(), host: None, phase: waiting }]);
        assert!(sends(&out).is_empty(), "{out:?}");
    }

    #[test]
    fn pods_on_a_lost_host_go_back_to_the_scheduler() {
        let mut s = running_on_m1();
        s.apply(0, Command::CreateDeployment { deployment: deployment("web", 2, 100, None) }).unwrap();
        s.apply(0, Command::SetPodStatus { deployment: "web".into(), idx: 0, host: Some(hid("m1")), phase: PodPhase::Running }).unwrap();
        s.apply(0, Command::SetPodStatus { deployment: "web".into(), idx: 1, host: Some(hid("m1")), phase: PodPhase::Pending(STARTING.into()) }).unwrap();
        not_ready(&mut s, "m1", 0);
        let v = Volatile::new(0, 1);
        let pods = |out: &[Command]| -> Vec<Command> { out.iter().filter(|c| matches!(c, Command::SetPodStatus { .. })).cloned().collect() };
        assert!(pods(&proposals(&scheduler::run(&s, &v, FAILOVER_GRACE_MS))).is_empty(), "within the grace period");
        let out = proposals(&scheduler::run(&s, &v, FAILOVER_GRACE_MS + 1));
        let unscheduled = |idx| Command::SetPodStatus { deployment: "web".into(), idx, host: None, phase: PodPhase::Pending(UNSCHEDULED.into()) };
        assert_eq!(pods(&out), [unscheduled(0), unscheduled(1)], "running or still starting");
        assert!(out.iter().any(|c| matches!(c, Command::Event { message, .. } if message == "m1 lost: rescheduling pod web/0")), "{out:?}");
        apply(&mut s, out);
        let out = proposals(&scheduler::run(&s, &v, FAILOVER_GRACE_MS + 2000));
        assert!(pods(&out).iter().all(|c| matches!(c, Command::SetPodStatus { host: Some(h), .. } if *h != hid("m1"))) && pods(&out).len() == 2, "{out:?}");
    }

    #[test]
    fn a_running_vm_or_pod_its_host_no_longer_lists_starts_again() {
        let mut s = running_on_m1();
        s.apply(0, Command::CreateDeployment { deployment: deployment("web", 1, 100, None) }).unwrap();
        s.apply(0, Command::SetPodStatus { deployment: "web".into(), idx: 0, host: Some(hid("m2")), phase: PodPhase::Running }).unwrap();
        let mut v = Volatile::new(0, 1);
        assert!(proposals(&scheduler::run(&s, &v, 1000)).is_empty(), "nothing heard yet: nothing known to be missing");
        v.instances.insert(hid("m1"), ["vm/vm1".to_string()].into());
        v.instances.insert(hid("m2"), ["pod/web/0".to_string()].into());
        assert!(proposals(&scheduler::run(&s, &v, 2000)).is_empty());
        // Both hosts rebooted faster than the not-ready window.
        v.instances.insert(hid("m1"), Default::default());
        v.instances.insert(hid("m2"), Default::default());
        assert_eq!(proposals(&scheduler::run(&s, &v, 3000)), [
            Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Starting },
            Command::SetPodStatus { deployment: "web".into(), idx: 0, host: Some(hid("m2")), phase: PodPhase::Pending(STARTING.into()) },
        ]);
    }

    #[test]
    fn instances_not_assigned_to_their_host_are_stopped_at_a_pace() {
        let mut s = running_on_m1();
        s.apply(0, Command::CreateDeployment { deployment: deployment("web", 1, 100, None) }).unwrap();
        s.apply(0, Command::SetPodStatus { deployment: "web".into(), idx: 0, host: Some(hid("m2")), phase: PodPhase::Running }).unwrap();
        let mut v = Volatile::new(0, 1);
        let listed = |ids: &[&str]| ids.iter().map(|i| i.to_string()).collect();
        v.instances.insert(hid("m1"), listed(&["vm/vm1", "pod/web/0", "pod/gone/0", "vm/deleted", "other"]));
        v.instances.insert(hid("m2"), listed(&["pod/web/0", "vm/vm1"]));
        let stops = |out: Vec<Action>| -> Vec<(HostId, String)> {
            sends(&out).into_iter().map(|(h, m)| match m { Msg::StopInstance(i) => (h, i), m => panic!("{m:?}") }).collect()
        };
        let stop = |h: &str, i: &str| (hid(h), i.to_string());
        assert_eq!(stops(run(&s, &v, 1000)), [stop("m1", "pod/gone/0"), stop("m1", "pod/web/0"), stop("m1", "vm/deleted"), stop("m2", "vm/vm1")]);
        v.instance_stopped.insert((hid("m2"), "vm/vm1".into()), 1000);
        assert_eq!(stops(run(&s, &v, 1000 + RESEND_MS - 1)).len(), 3, "m2 was just told");
        assert_eq!(stops(run(&s, &v, 1000 + RESEND_MS)).len(), 4, "still running: told again");
        // A VM that went pending is assigned nowhere.
        s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: None, phase: VmPhase::Pending("no fit: x".into()) }).unwrap();
        assert!(stops(run(&s, &v, 1000)).contains(&stop("m1", "vm/vm1")));
        // Migrating: both ends keep it.
        s.apply(0, Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Migrating { to: hid("m2") } }).unwrap();
        let out = stops(run(&s, &v, 1000));
        assert!(!out.contains(&stop("m1", "vm/vm1")) && !out.contains(&stop("m2", "vm/vm1")), "{out:?}");
    }
}
