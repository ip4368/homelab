//! Storage: keeps every volume at its replica count, Longhorn-like.
//!
//! * **Placement.** A volume with fewer live (not stale) replicas than it
//!   asks for gets [`place_replicas`] targets, written to its status before
//!   the targets are sent `CreateReplica`. A volume's first replicas (it has
//!   none, so it never held data) are placed `Seeding` and start empty;
//!   later ones are placed `Rebuilding` and copied from a healthy replica,
//!   waiting until there is one. A target is never a host that holds a
//!   replica of the volume that the status does not list (an old one, to be
//!   deleted), nor a host this leader has not heard from (it might hold
//!   one): a host's report of a complete replica only counts for one the
//!   control plane placed there.
//! * **Progress.** A target's heartbeat reports its progress, which is
//!   written in steps of [`PROGRESS_STEP_PCT`], and `Healthy` once complete.
//!   A copy its target reports interrupted is sent again, with a healthy
//!   source, so it resumes.
//! * **Loss.** A replica whose host is lost (`NotReady` for more than
//!   [`FAILOVER_GRACE_MS`](super::failover::FAILOVER_GRACE_MS), or gone) or whose disk its host reports failed is
//!   marked `Stale`, and so is a copy whose progress has not moved for
//!   [`REBUILD_STALL_MS`] (abandoned). Stale entries stay in the status, so
//!   their hosts are not picked again and the volume keeps the mark of
//!   having held data, until all the replicas it asks for are healthy again.
//!   Only when no other host can take a replica may the host of a stale
//!   entry be picked again, once it has dropped its old copy. A volume left
//!   with no healthy replica salvages a stale one whose host is back with it
//!   complete.
//! * **Drains.** A replica on a host being drained ([`evacuating`]) gets a
//!   replacement placed as if it were missing, but stays (and stays a copy
//!   source) until the volume has all the replicas it asks for healthy
//!   without it; then it is dropped, and so deleted. A copy onto such a host
//!   is given up at once.
//! * **Cleanup.** A replica a host reports that its volume's status does not
//!   list as live, or whose volume is gone, gets `DeleteReplica` (at most
//!   every [`RESEND_MS`]), except a stale one while its volume has no healthy
//!   replica: it may yet be salvaged.
//! * **Alerts**, one per incident and volume: [`INSUFFICIENT_STORAGE_HOSTS`]
//!   while too few storage hosts can take its replicas, [`VOLUME_DEGRADED`]
//!   while fewer of them are healthy than it asks for, [`VOLUME_FAULTED`]
//!   once none is (for a volume that held data).
//!
//! Statuses are only proposed when they would change.

use super::drain::evacuating;
use super::failover::host_lost;
use super::scheduler::{access_switch, active_roles, agent, in_flight, RESEND_MS};
use super::{Action, Volatile};
use crate::proto::{mgmt, Msg, ReplicaReport};
use crate::runtime::SimTime;
use crate::state::{Alert, ClusterState, Command, Lifecycle, ObjectKind, ObjectRef};
use model::{place_replicas, DiskView, HostId, ReplicaRef, ReplicaState, StorageHostView, VolumeHealth};
use std::collections::{BTreeMap, BTreeSet};

pub const INSUFFICIENT_STORAGE_HOSTS: &str = "InsufficientStorageHosts";
pub const VOLUME_DEGRADED: &str = "VolumeDegraded";
pub const VOLUME_FAULTED: &str = "VolumeFaulted";
/// Copy progress is written to the volume status in steps of this many percent.
pub const PROGRESS_STEP_PCT: u8 = 10;
/// A copy whose reported progress has not moved for this long is abandoned.
pub const REBUILD_STALL_MS: u64 = 60_000;

/// Each volume alert's kind and the start of its ids.
const ALERTS: [(&str, &str); 3] = [
    (INSUFFICIENT_STORAGE_HOSTS, "insufficientstorage"),
    (VOLUME_DEGRADED, "volumedegraded"),
    (VOLUME_FAULTED, "volumefaulted"),
];

pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = volumes(state, v, now);
    out.extend(delete_unwanted(state, v, now));
    out
}

/// The storage hosts as [`place_replicas`] sees them: every host (so that
/// the switches of replicas on hosts that cannot take more still count),
/// each disk's free space less the replicas the volume statuses put there.
pub(super) fn storage_views(state: &ClusterState) -> Vec<StorageHostView> {
    let mut used: BTreeMap<(&HostId, &str), u32> = BTreeMap::new();
    for (id, st) in &state.volume_status {
        let size = state.volumes.get(id).map_or(0, |v| v.volume.size_gb);
        for r in &st.status.replicas {
            *used.entry((&r.host, r.disk.as_str())).or_default() += size;
        }
    }
    state
        .hosts
        .values()
        .map(|h| StorageHostView {
            id: h.id.clone(),
            lifecycle: h.lifecycle,
            active_roles: active_roles(h),
            disks: h
                .inventory
                .disks
                .iter()
                .map(|d| DiskView {
                    name: d.name.clone(),
                    kind: d.kind,
                    free_gb: d.size_gb.saturating_sub(used.get(&(&h.id, d.name.as_str())).copied().unwrap_or(0)),
                    failed: d.failed,
                })
                .collect(),
            switch: access_switch(h),
        })
        .collect()
}

fn volumes(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut views = storage_views(state);
    let leaving = evacuating(state);
    let mut open: BTreeMap<(&str, &str), &Alert> = state
        .alerts
        .values()
        .filter(|a| a.resolved_at.is_none() && a.entity.kind == ObjectKind::Volume && ALERTS.iter().any(|(k, ..)| *k == a.kind))
        .map(|a| ((a.kind.as_str(), a.entity.id.as_str()), a))
        .collect();
    let mut out = vec![];
    for (id, vol) in &state.volumes {
        let alerts = ALERTS.map(|(kind, ..)| open.remove(&(kind, id.as_str())));
        let Some(st) = state.volume_status.get(id).map(|s| &s.status) else { continue };
        if in_flight(v, |c| matches!(c, Command::SetVolumeStatus { id: i, .. } if i == id)) {
            continue;
        }
        let (size_gb, want) = (vol.volume.size_gb, usize::from(vol.volume.replicas));
        let (replicas, short) = plan(state, v, now, id, size_gb, want, &st.replicas, &leaving, &mut views);
        let health = health(&replicas, want);
        if replicas != st.replicas || health != st.health {
            out.push(Action::Propose(Command::SetVolumeStatus { id: id.clone(), replicas: replicas.clone(), health }));
        }
        out.extend(create_replicas(state, v, now, id, size_gb, &st.replicas));
        let wanted = wanted_alerts(id, &replicas, want, short, alerts[0].is_some());
        for (((kind, slug), open), message) in ALERTS.into_iter().zip(alerts).zip(wanted) {
            out.extend(alert(state, v, (kind, slug), id, message, open));
        }
    }
    // Alerts of volumes that are gone.
    out.extend(open.values().map(|a| Action::Propose(Command::ResolveAlert { id: a.id.clone() })));
    out
}

/// Where `volume`'s replicas should be, from its committed `replicas`:
/// progress written, lost and stalled replicas marked stale, a stale one
/// salvaged if nothing healthy is left, stale entries dropped once the volume
/// is whole, replicas on the `leaving` hosts dropped once it is whole
/// without them, and targets placed for what is missing or leaving. Also
/// whether too few hosts could take the targets it is missing.
#[allow(clippy::too_many_arguments)]
fn plan(
    state: &ClusterState,
    v: &Volatile,
    now: SimTime,
    volume: &str,
    size_gb: u32,
    want: usize,
    committed: &[ReplicaRef],
    leaving: &BTreeSet<HostId>,
    views: &mut [StorageHostView],
) -> (Vec<ReplicaRef>, bool) {
    let mut replicas = committed.to_vec();
    for r in replicas.iter_mut() {
        progress(v, volume, r);
        if r.state != ReplicaState::Stale && (lost(state, now, r) || stalled(v, now, volume, r)) {
            r.state = ReplicaState::Stale;
        }
    }
    let copying = |r: &ReplicaRef| matches!(r.state, ReplicaState::Seeding | ReplicaState::Rebuilding { .. });
    replicas.retain(|r| !(leaving.contains(&r.host) && copying(r)));
    let healthy = |rs: &[ReplicaRef]| rs.iter().filter(|r| r.state == ReplicaState::Healthy).count();
    if healthy(&replicas) == 0 {
        for r in replicas.iter_mut().filter(|r| r.state == ReplicaState::Stale) {
            salvage(state, v, volume, r);
        }
    }
    if healthy(&replicas) >= want {
        replicas.retain(|r| r.state != ReplicaState::Stale);
    }
    let staying = |r: &&ReplicaRef| !leaving.contains(&r.host);
    if replicas.iter().filter(staying).filter(|r| r.state == ReplicaState::Healthy).count() >= want {
        replicas.retain(|r| !leaving.contains(&r.host));
    }
    let live = replicas.iter().filter(|r| r.state != ReplicaState::Stale).count();
    let live_staying = replicas.iter().filter(staying).filter(|r| r.state != ReplicaState::Stale).count();
    // A volume with no replicas at all never held data: its first replicas
    // start empty. Any later one needs a healthy one to copy.
    let seed = replicas.is_empty();
    if live_staying >= want || !(seed || healthy(&replicas) > 0) {
        return (replicas, false);
    }
    let missing = want.saturating_sub(live);
    let need = u8::try_from(want - live_staying).unwrap_or(u8::MAX);
    let mut picks = place_replicas(need, size_gb, views, &excluded(state, v, volume, &replicas, false));
    if picks.len() < usize::from(need) {
        let mut relaxed = excluded(state, v, volume, &replicas, true);
        relaxed.extend(picks.iter().map(|(h, _)| h.clone()));
        picks.extend(place_replicas(need - picks.len() as u8, size_gb, views, &relaxed));
    }
    // Only what the volume is missing counts: a replica that cannot leave stays (a drain says so).
    let short = picks.len() < missing;
    for (host, disk) in picks {
        take_disk(views, &host, &disk, size_gb);
        // A stale entry on the host is superseded.
        replicas.retain(|r| r.host != host);
        let state = if seed { ReplicaState::Seeding } else { ReplicaState::Rebuilding { pct: 0 } };
        replicas.push(ReplicaRef { host, disk, state });
    }
    (replicas, short)
}

/// The hosts a new replica of `volume` must not go to: those of its
/// `replicas` (unless `relaxed`, the stale ones whose host dropped its copy
/// are allowed again), those holding a replica of it that `replicas` does
/// not list, and those not heard from (they might hold one).
pub(super) fn excluded(state: &ClusterState, v: &Volatile, volume: &str, replicas: &[ReplicaRef], relaxed: bool) -> Vec<HostId> {
    let mut out: Vec<HostId> = replicas
        .iter()
        .filter(|r| !(relaxed && r.state == ReplicaState::Stale && dropped(state, v, volume, &r.host)))
        .map(|r| r.host.clone())
        .collect();
    for h in state.hosts.keys() {
        let unlisted = report(v, h, volume).is_some() && !replicas.iter().any(|r| r.host == *h);
        if unlisted || !v.replicas.contains_key(h) {
            out.push(h.clone());
        }
    }
    out
}

/// Whether `host` is `Ready`, heard from, and holds no replica of `volume`.
fn dropped(state: &ClusterState, v: &Volatile, volume: &str, host: &HostId) -> bool {
    state.hosts.get(host).is_some_and(|h| h.lifecycle == Lifecycle::Ready)
        && v.replicas.contains_key(host)
        && report(v, host, volume).is_none()
}

/// What `host`'s latest heartbeat says of its replica of `volume`.
fn report<'a>(v: &'a Volatile, host: &HostId, volume: &str) -> Option<&'a ReplicaReport> {
    v.replicas.get(host)?.iter().find(|r| r.volume == volume)
}

/// Moves a seeding or copying replica on as far as its host reported it:
/// in steps of [`PROGRESS_STEP_PCT`], `Healthy` once complete, on the disk
/// the host chose. A seeding replica reported part-copied was given a source
/// (data appeared meanwhile) and is copying.
fn progress(v: &Volatile, volume: &str, r: &mut ReplicaRef) {
    let floor = match r.state {
        ReplicaState::Rebuilding { pct } => pct,
        ReplicaState::Seeding => 0,
        ReplicaState::Healthy | ReplicaState::Stale => return,
    };
    let Some(rep) = report(v, &r.host, volume) else { return };
    r.disk = rep.disk.clone();
    if rep.pct >= 100 {
        r.state = ReplicaState::Healthy;
    } else {
        let step = rep.pct / PROGRESS_STEP_PCT * PROGRESS_STEP_PCT;
        r.state = ReplicaState::Rebuilding { pct: step.max(floor) };
    }
}

/// Whether replica `r` is lost: its host is lost, or reports its disk failed.
fn lost(state: &ClusterState, now: SimTime, r: &ReplicaRef) -> bool {
    host_lost(state, &r.host, now)
        || state.hosts.get(&r.host).is_some_and(|h| h.inventory.disks.iter().any(|d| d.name == r.disk && d.failed))
}

/// Whether `r` is a copy whose reported progress has not moved for [`REBUILD_STALL_MS`].
fn stalled(v: &Volatile, now: SimTime, volume: &str, r: &ReplicaRef) -> bool {
    matches!(r.state, ReplicaState::Rebuilding { .. })
        && v
            .copy_progress
            .get(&(volume.to_string(), r.host.clone()))
            .is_some_and(|(_, since)| now.saturating_sub(*since) > REBUILD_STALL_MS)
}

/// Makes stale replica `r` healthy again if its host is back (not
/// `NotReady`) and reports it complete.
fn salvage(state: &ClusterState, v: &Volatile, volume: &str, r: &mut ReplicaRef) {
    let back = state.hosts.get(&r.host).is_some_and(|h| h.lifecycle != Lifecycle::NotReady);
    if let Some(rep) = report(v, &r.host, volume).filter(|rep| back && rep.pct >= 100) {
        r.disk = rep.disk.clone();
        r.state = ReplicaState::Healthy;
    }
}

/// Counts a new replica against the free space of the disk it lands on.
fn take_disk(views: &mut [StorageHostView], host: &HostId, disk: &str, size_gb: u32) {
    if let Some(d) = views.iter_mut().filter(|h| h.id == *host).flat_map(|h| h.disks.iter_mut()).find(|d| d.name == disk) {
        d.free_gb = d.free_gb.saturating_sub(size_gb);
    }
}

/// `Healthy` with all the replicas it asks for healthy, `Faulted` with none,
/// `Rebuilding` while one is seeding or copying, else `Degraded`.
fn health(replicas: &[ReplicaRef], want: usize) -> VolumeHealth {
    let healthy = replicas.iter().filter(|r| r.state == ReplicaState::Healthy).count();
    if healthy == 0 {
        VolumeHealth::Faulted
    } else if healthy >= want {
        VolumeHealth::Healthy
    } else if replicas.iter().any(|r| matches!(r.state, ReplicaState::Rebuilding { .. } | ReplicaState::Seeding)) {
        VolumeHealth::Rebuilding
    } else {
        VolumeHealth::Degraded
    }
}

/// `CreateReplica` to each host the committed status has seeding or
/// copying a replica that its heartbeat does not list, or lists incomplete
/// with no copy running (interrupted), at most every [`RESEND_MS`]. The copy
/// comes from a healthy replica, one on a `Ready` host if there is one. Only
/// a replica placed as `Seeding` is created empty, and only while the volume
/// has no healthy replica; a copy with no healthy source waits.
fn create_replicas(state: &ClusterState, v: &Volatile, now: SimTime, volume: &str, size_gb: u32, replicas: &[ReplicaRef]) -> Vec<Action> {
    let healthy = || replicas.iter().filter(|r| r.state == ReplicaState::Healthy);
    let ready = |r: &&ReplicaRef| state.hosts.get(&r.host).is_some_and(|h| h.lifecycle == Lifecycle::Ready);
    let healthy_source = healthy().find(ready).or_else(|| healthy().next()).map(|r| r.host.clone());
    let mut out = vec![];
    for r in replicas {
        let source = match (&r.state, &healthy_source) {
            (ReplicaState::Seeding | ReplicaState::Rebuilding { .. }, Some(s)) => Some(s.clone()),
            (ReplicaState::Seeding, None) => None,
            _ => continue,
        };
        let wanted = report(v, &r.host, volume).is_none_or(|rep| rep.pct < 100 && !rep.copying);
        let recent = v.replica_created.get(&(volume.to_string(), r.host.clone())).is_some_and(|at| now < at + RESEND_MS);
        if wanted && !recent {
            let msg = Msg::CreateReplica { volume: volume.into(), size_gb, source };
            out.push(Action::Send(agent(&r.host), mgmt(), msg));
        }
    }
    out
}

/// The message of each of [`ALERTS`] that should be open for `volume` with
/// these (planned) replicas, in order. Not enough storage hosts: raised
/// when placement came up `short`, and kept (`insufficient_open`) until
/// every replica is placed. Degraded: some but not all healthy. Faulted:
/// none healthy, though it held data. A volume still seeding its first
/// replicas is neither.
fn wanted_alerts(volume: &str, replicas: &[ReplicaRef], want: usize, short: bool, insufficient_open: bool) -> [Option<String>; 3] {
    let live = replicas.iter().filter(|r| r.state != ReplicaState::Stale).count();
    let healthy = replicas.iter().filter(|r| r.state == ReplicaState::Healthy).count();
    let seeding = replicas.iter().any(|r| r.state == ReplicaState::Seeding);
    let held_data = replicas.iter().any(|r| r.state != ReplicaState::Seeding);
    let insufficient = if insufficient_open { live < want } else { short };
    [
        insufficient.then(|| format!("volume {volume} has {live} of {want} replicas: not enough storage hosts")),
        (healthy > 0 && healthy < want && !seeding).then(|| format!("volume {volume} has {healthy} of {want} replicas healthy")),
        (healthy == 0 && held_data).then(|| format!("volume {volume} has no healthy replica")),
    ]
}

/// Resolves the volume's `open` alert of `kind` once no `message` wants it,
/// and raises one (unless a raise is in flight) when a message does and none
/// is open. Ids start with `slug`, the volume and the leader's term, plus a
/// counter if an incident of this term already used the id.
fn alert(
    state: &ClusterState,
    v: &Volatile,
    (kind, slug): (&str, &str),
    volume: &str,
    message: Option<String>,
    open: Option<&Alert>,
) -> Option<Action> {
    match (open, message) {
        (Some(a), None) => Some(Action::Propose(Command::ResolveAlert { id: a.id.clone() })),
        (None, Some(message)) => {
            let entity = ObjectRef::volume(volume);
            if in_flight(v, |c| matches!(c, Command::RaiseAlert { kind: k, entity: e, .. } if k == kind && *e == entity)) {
                return None;
            }
            let base = format!("{slug}-{volume}-{}", v.term);
            let id = (1..)
                .map(|n| if n == 1 { base.clone() } else { format!("{base}-{n}") })
                .find(|id| !state.alerts.contains_key(id))
                .expect("some id is free");
            Some(Action::Propose(Command::RaiseAlert {
                id,
                kind: kind.into(),
                entity,
                subject: None,
                message,
                ground_truth: false,
            }))
        }
        _ => None,
    }
}

/// `DeleteReplica` for each replica a host reports that its volume does
/// not want there: the volume is gone, or its committed status does not
/// list the host as live. A stale replica is kept while the volume has no
/// healthy one, for salvage. A `NotReady` host's report is old news; it is
/// cleaned up once it is back.
fn delete_unwanted(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = vec![];
    let back = |h: &HostId| state.hosts.get(h).is_some_and(|h| h.lifecycle != Lifecycle::NotReady);
    for (host, reports) in v.replicas.iter().filter(|(h, _)| back(h)) {
        for rep in reports {
            let keep = state.volume_status.get(&rep.volume).is_some_and(|st| {
                let replicas = &st.status.replicas;
                let healthy = replicas.iter().any(|r| r.state == ReplicaState::Healthy);
                replicas.iter().any(|r| r.host == *host && (r.state != ReplicaState::Stale || !healthy))
            });
            let recent = v.replica_deleted.get(&(rep.volume.clone(), host.clone())).is_some_and(|at| now < at + RESEND_MS);
            if !keep && !recent {
                out.push(Action::Send(agent(host), mgmt(), Msg::DeleteReplica { volume: rep.volume.clone() }));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::scheduler::tests::{
        apply, healthy, hid, proposals, rebuilding, rep, report_all, seeding, sends, state, three_ms01, STORAGE_VM,
    };
    use super::super::failover::FAILOVER_GRACE_MS;
    use super::*;
    use model::{ProfileId, Role, Volume};

    /// A leader of `term` that has heard from every host of `s`, which holds no replicas.
    fn heard_from(s: &ClusterState, term: u64) -> Volatile {
        let mut v = Volatile::new(0, term);
        for h in s.hosts.keys() {
            v.replicas.insert(h.clone(), vec![]);
        }
        v
    }

    /// The actions, less alert raises.
    fn no_alerts(actions: Vec<Action>) -> Vec<Action> {
        actions.into_iter().filter(|a| !matches!(a, Action::Propose(Command::RaiseAlert { .. }))).collect()
    }

    /// The ids of the alerts raised.
    fn raised(actions: &[Action]) -> Vec<String> {
        proposals(actions).into_iter().filter_map(|c| if let Command::RaiseAlert { id, .. } = c { Some(id) } else { None }).collect()
    }
    #[test]
    fn a_new_volume_gets_empty_replicas_status_first_then_healthy_once_reported() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let mut v = heard_from(&s, 1);
        let out = run(&s, &v, 1000);
        assert!(sends(&out).is_empty(), "nothing is sent before the status names the targets");
        let placed = vec![seeding("m1"), seeding("m3"), seeding("m2")];
        assert_eq!(proposals(&out), [Command::SetVolumeStatus { id: "d".into(), replicas: placed.clone(), health: VolumeHealth::Faulted }]);
        apply(&mut s, proposals(&out));

        let out = run(&s, &v, 2000);
        assert!(proposals(&out).is_empty());
        let create = Msg::CreateReplica { volume: "d".into(), size_gb: 40, source: None };
        assert_eq!(sends(&out), [(hid("m1"), create.clone()), (hid("m3"), create.clone()), (hid("m2"), create.clone())], "first replicas start empty");
        v.replica_created.insert(("d".into(), hid("m1")), 2000);
        assert_eq!(sends(&run(&s, &v, 2000 + RESEND_MS - 1)).len(), 2, "m1 was just sent one");
        assert_eq!(sends(&run(&s, &v, 2000 + RESEND_MS)).len(), 3, "not acted on: sent again");

        report_all(&mut v, &s, "d", 100);
        let out = run(&s, &v, 3000);
        assert!(sends(&out).is_empty(), "reported: no more creates");
        let done = vec![healthy("m1"), healthy("m3"), healthy("m2")];
        assert_eq!(proposals(&out), [Command::SetVolumeStatus { id: "d".into(), replicas: done, health: VolumeHealth::Healthy }]);
        apply(&mut s, proposals(&out));
        assert!(run(&s, &v, 4000).is_empty(), "nothing changes, nothing is proposed");
    }

    #[test]
    fn a_missing_replica_is_copied_from_a_healthy_one_with_progress_in_steps() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let st = Command::SetVolumeStatus { id: "d".into(), replicas: vec![healthy("m1"), healthy("m3")], health: VolumeHealth::Degraded };
        s.apply(0, st).unwrap();
        let mut v = Volatile::new(0, 1);
        assert!(proposals(&no_alerts(run(&s, &v, 1000))).is_empty(), "m2 not heard from yet: it might hold an old copy");
        v.replicas.insert(hid("m2"), vec![]);
        let out = run(&s, &v, 1000);
        assert_eq!(raised(&out), ["volumedegraded-d-1"]);
        let out = no_alerts(out);
        let placed = vec![healthy("m1"), healthy("m3"), rebuilding("m2", 0)];
        assert_eq!(proposals(&out), [Command::SetVolumeStatus { id: "d".into(), replicas: placed, health: VolumeHealth::Rebuilding }]);
        apply(&mut s, proposals(&out));
        let degraded = || Command::RaiseAlert {
            id: "volumedegraded-d-1".into(),
            kind: VOLUME_DEGRADED.into(),
            entity: ObjectRef::volume("d"),
            subject: None,
            message: "volume d has 2 of 3 replicas healthy".into(),
            ground_truth: false,
        };
        s.apply(0, degraded()).unwrap();
        let create = Msg::CreateReplica { volume: "d".into(), size_gb: 40, source: Some(hid("m1")) };
        assert_eq!(sends(&run(&s, &v, 2000)), [(hid("m2"), create)]);

        v.replicas.insert(hid("m2"), vec![rep("d", "nvme0", 9, true)]);
        assert!(run(&s, &v, 3000).is_empty(), "below the first step: nothing to write");
        v.replicas.insert(hid("m2"), vec![rep("d", "nvme1", 37, true)]);
        let out = run(&s, &v, 4000);
        let at30 = ReplicaRef { host: hid("m2"), disk: "nvme1".into(), state: ReplicaState::Rebuilding { pct: 30 } };
        assert_eq!(proposals(&out), [Command::SetVolumeStatus { id: "d".into(), replicas: vec![healthy("m1"), healthy("m3"), at30], health: VolumeHealth::Rebuilding }], "the disk the host chose is recorded");
        apply(&mut s, proposals(&out));
        v.replicas.insert(hid("m2"), vec![rep("d", "nvme1", 39, true)]);
        assert!(run(&s, &v, 5000).is_empty());
        v.replicas.insert(hid("m2"), vec![rep("d", "nvme1", 100, false)]);
        let out = run(&s, &v, 6000);
        assert!(matches!(proposals(&out).as_slice(), [Command::SetVolumeStatus { health: VolumeHealth::Healthy, .. }, Command::ResolveAlert { id }] if id == "volumedegraded-d-1"), "{out:?}");
    }

    #[test]
    fn a_copy_waits_for_a_healthy_source() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let st = Command::SetVolumeStatus { id: "d".into(), replicas: vec![rebuilding("m1", 0)], health: VolumeHealth::Faulted };
        s.apply(0, st).unwrap();
        let out = run(&s, &Volatile::new(0, 1), 1000);
        assert_eq!(raised(&out), ["volumefaulted-d-1"], "it held data and has no healthy replica left");
        let out = no_alerts(out);
        assert!(proposals(&out).is_empty(), "no healthy replica to copy from: {out:?}");
        assert!(sends(&out).is_empty(), "and a copy is never created empty: {out:?}");
    }

    #[test]
    fn a_seeding_replica_is_copied_once_another_is_healthy() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume { replicas: 2, ..Volume::new("d", 40) } }).unwrap();
        let st = Command::SetVolumeStatus { id: "d".into(), replicas: vec![healthy("m1"), seeding("m2")], health: VolumeHealth::Rebuilding };
        s.apply(0, st).unwrap();
        let out = run(&s, &Volatile::new(0, 1), 1000);
        let create = Msg::CreateReplica { volume: "d".into(), size_gb: 40, source: Some(hid("m1")) };
        assert_eq!(sends(&out), [(hid("m2"), create)], "m1 may hold data by now");
        assert!(raised(&out).is_empty(), "still seeding its first replicas: neither degraded nor faulted");
    }

    #[test]
    fn an_interrupted_copy_is_sent_again_with_a_healthy_source() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume { replicas: 2, ..Volume::new("d", 40) } }).unwrap();
        let st = Command::SetVolumeStatus { id: "d".into(), replicas: vec![healthy("m1"), rebuilding("m2", 40)], health: VolumeHealth::Rebuilding };
        s.apply(0, st).unwrap();
        let mut v = Volatile::new(0, 1);
        v.replicas.insert(hid("m2"), vec![rep("d", "nvme0", 45, true)]);
        assert!(no_alerts(run(&s, &v, 1000)).is_empty(), "copying: nothing to do");
        v.replicas.insert(hid("m2"), vec![rep("d", "nvme0", 45, false)]);
        let create = Msg::CreateReplica { volume: "d".into(), size_gb: 40, source: Some(hid("m1")) };
        assert_eq!(sends(&run(&s, &v, 2000)), [(hid("m2"), create)], "interrupted: resumed from a healthy source");
        v.replica_created.insert(("d".into(), hid("m2")), 2000);
        assert!(no_alerts(run(&s, &v, 2000 + RESEND_MS - 1)).is_empty(), "paced");
    }

    #[test]
    fn too_few_storage_hosts_leave_the_volume_degraded_with_one_alert() {
        let mut s = state(&[("m1", ProfileId::Ms01, STORAGE_VM, "a"), ("m2", ProfileId::Ms01, STORAGE_VM, "b"), ("p1", ProfileId::Pi5, &[Role::Compute], "a")]);
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let mut v = heard_from(&s, 4);
        let out = proposals(&run(&s, &v, 1000));
        assert_eq!(out.len(), 2, "{out:?}");
        let Command::RaiseAlert { id, kind, entity, message, .. } = &out[1] else { panic!("{out:?}") };
        assert_eq!((id.as_str(), kind.as_str(), entity), ("insufficientstorage-d-4", INSUFFICIENT_STORAGE_HOSTS, &ObjectRef::volume("d")));
        assert_eq!(message, "volume d has 2 of 3 replicas: not enough storage hosts");
        v.in_flight.insert(7, out[1].clone());
        apply(&mut s, vec![out[0].clone()]);
        assert!(proposals(&run(&s, &v, 2000)).is_empty(), "the raise is in flight");
        apply(&mut s, vec![v.in_flight.remove(&7).unwrap()]);
        report_all(&mut v, &s, "d", 100);
        let out = proposals(&run(&s, &v, 3000));
        assert!(matches!(out.as_slice(), [Command::SetVolumeStatus { health: VolumeHealth::Degraded, .. }, Command::RaiseAlert { id, .. }] if id == "volumedegraded-d-4"), "{out:?}");
        apply(&mut s, out);
        assert!(run(&s, &v, 4000).is_empty(), "no busy loop");

        // A third storage host arrives: the replica is placed there, which resolves the alert.
        let mut more = state(&[("m3", ProfileId::Ms01, STORAGE_VM, "c")]);
        s.hosts.append(&mut more.hosts);
        v.replicas.insert(hid("m3"), vec![]);
        let out = proposals(&run(&s, &v, 5000));
        assert!(matches!(out.as_slice(), [Command::SetVolumeStatus { health: VolumeHealth::Rebuilding, .. }, Command::ResolveAlert { id }] if id == "insufficientstorage-d-4"), "{out:?}");
    }

    #[test]
    fn replicas_spread_over_switches_and_count_held_space() {
        let mut s = three_ms01();
        // m3's disks are nearly full of another volume's replica.
        s.apply(0, Command::CreateVolume { volume: Volume::new("big", 1990) }).unwrap();
        let big = vec![ReplicaRef { host: hid("m3"), disk: "nvme0".into(), state: ReplicaState::Healthy }, ReplicaRef { host: hid("m3"), disk: "nvme1".into(), state: ReplicaState::Healthy }];
        s.apply(0, Command::SetVolumeStatus { id: "big".into(), replicas: big, health: VolumeHealth::Degraded }).unwrap();
        s.apply(0, Command::CreateVolume { volume: Volume { replicas: 2, ..Volume::new("d", 20) } }).unwrap();
        let out = proposals(&run(&s, &heard_from(&s, 1), 1000));
        let placed = out.iter().find_map(|c| match c {
            Command::SetVolumeStatus { id, replicas, .. } if id == "d" => Some(replicas.iter().map(|r| r.host.clone()).collect::<Vec<_>>()),
            _ => None,
        });
        assert_eq!(placed, Some(vec![hid("m1"), hid("m2")]), "m3 has no room left");
    }

    /// MS-01s m1..m`n` on switches a, a, b, b, c, ..., all heard from, with
    /// volume `d` (3 replicas, 40 GB) healthy on m1, m2, m3.
    fn whole(n: usize) -> (ClusterState, Volatile) {
        let ids: Vec<String> = (1..=n).map(|i| format!("m{i}")).collect();
        let switches = ["a", "a", "b", "b", "c", "c"];
        let hosts: Vec<(&str, ProfileId, &[Role], &str)> =
            ids.iter().zip(switches).map(|(h, sw)| (h.as_str(), ProfileId::Ms01, STORAGE_VM, sw)).collect();
        let mut s = state(&hosts);
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let st = vec![healthy("m1"), healthy("m2"), healthy("m3")];
        s.apply(0, Command::SetVolumeStatus { id: "d".into(), replicas: st, health: VolumeHealth::Healthy }).unwrap();
        let mut v = Volatile::new(0, 1);
        for h in &ids {
            v.replicas.insert(hid(h), vec![]);
        }
        report_all(&mut v, &s, "d", 100);
        (s, v)
    }

    fn stale(host: &str) -> ReplicaRef {
        ReplicaRef { host: hid(host), disk: "nvme0".into(), state: ReplicaState::Stale }
    }

    fn statuses(actions: &[Action]) -> Vec<(Vec<ReplicaRef>, VolumeHealth)> {
        proposals(actions)
            .into_iter()
            .filter_map(|c| if let Command::SetVolumeStatus { replicas, health, .. } = c { Some((replicas, health)) } else { None })
            .collect()
    }

    fn deletes(actions: &[Action]) -> Vec<(HostId, String)> {
        sends(actions).into_iter().filter_map(|(h, m)| if let Msg::DeleteReplica { volume } = m { Some((h, volume)) } else { None }).collect()
    }

    #[test]
    fn a_replica_on_a_lost_host_goes_stale_and_is_rebuilt_elsewhere() {
        let (mut s, mut v) = whole(4);
        assert!(run(&s, &v, 1000).is_empty(), "healthy: nothing to do");
        s.apply(10_000, Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::NotReady }).unwrap();
        assert!(run(&s, &v, 10_000 + FAILOVER_GRACE_MS).is_empty(), "within the grace period");
        let out = run(&s, &v, 10_001 + FAILOVER_GRACE_MS);
        let placed = vec![stale("m1"), healthy("m2"), healthy("m3"), rebuilding("m4", 0)];
        assert_eq!(statuses(&out), [(placed, VolumeHealth::Rebuilding)], "the stale entry stays while the copy runs");
        assert_eq!(raised(&out), ["volumedegraded-d-1"]);
        apply(&mut s, proposals(&out));
        let out = run(&s, &v, 72_000);
        assert_eq!(sends(&out), [(hid("m4"), Msg::CreateReplica { volume: "d".into(), size_gb: 40, source: Some(hid("m2")) })]);
        v.replicas.insert(hid("m4"), vec![rep("d", "nvme0", 100, false)]);
        let out = run(&s, &v, 73_000);
        assert_eq!(statuses(&out), [(vec![healthy("m2"), healthy("m3"), healthy("m4")], VolumeHealth::Healthy)], "whole again: the stale entry goes");
        assert!(proposals(&out).contains(&Command::ResolveAlert { id: "volumedegraded-d-1".into() }));
        apply(&mut s, proposals(&out));
        // m1 comes back with its old copy, which nothing wants now.
        s.apply(80_000, Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::Ready }).unwrap();
        assert_eq!(deletes(&run(&s, &v, 80_000)), [(hid("m1"), "d".to_string())]);
        v.replica_deleted.insert(("d".into(), hid("m1")), 80_000);
        assert!(run(&s, &v, 80_000 + RESEND_MS - 1).is_empty(), "paced");
    }

    #[test]
    fn a_replica_on_a_failed_disk_goes_stale_and_its_host_is_not_picked_again() {
        let (mut s, v) = whole(4);
        let mut inventory = s.hosts[&hid("m1")].inventory.clone();
        inventory.disks.iter_mut().find(|d| d.name == "nvme0").unwrap().failed = true;
        s.apply(5000, Command::ObserveHost { id: hid("m1"), lldp: s.hosts[&hid("m1")].lldp.clone(), inventory }).unwrap();
        let out = run(&s, &v, 6000);
        let placed = vec![stale("m1"), healthy("m2"), healthy("m3"), rebuilding("m4", 0)];
        assert_eq!(statuses(&out), [(placed, VolumeHealth::Rebuilding)], "m1's other disk has room, but m1 holds the stale entry");
    }

    #[test]
    fn a_copy_without_progress_for_60_s_is_abandoned_and_placed_elsewhere() {
        let (mut s, mut v) = whole(5);
        let st = vec![healthy("m1"), healthy("m2"), rebuilding("m3", 10)];
        s.apply(0, Command::SetVolumeStatus { id: "d".into(), replicas: st, health: VolumeHealth::Rebuilding }).unwrap();
        v.replicas.insert(hid("m3"), vec![rep("d", "nvme0", 12, true)]);
        v.copy_progress.insert(("d".into(), hid("m3")), (12_000_000, 1000));
        assert!(statuses(&run(&s, &v, 1000 + REBUILD_STALL_MS)).is_empty(), "not yet");
        let out = run(&s, &v, 1001 + REBUILD_STALL_MS);
        let placed = vec![healthy("m1"), healthy("m2"), stale("m3"), rebuilding("m5", 0)];
        assert_eq!(statuses(&out), [(placed, VolumeHealth::Rebuilding)], "m4 shares m3's switch; m5 is the fresh one");
        apply(&mut s, proposals(&out));
        assert_eq!(deletes(&run(&s, &v, 63_000)), [(hid("m3"), "d".to_string())], "the abandoned copy is dropped");
    }

    #[test]
    fn note_replicas_tracks_when_each_copy_last_moved() {
        let mut v = Volatile::new(0, 1);
        let sent = |v: &mut Volatile, now| {
            let to = agent(&hid("m3"));
            v.note_sent(&to, &Msg::CreateReplica { volume: "d".into(), size_gb: 40, source: Some(hid("m1")) }, now);
        };
        sent(&mut v, 1000);
        sent(&mut v, 11_000);
        assert_eq!(v.copy_progress[&("d".to_string(), hid("m3"))], (0, 1000), "a resend is no progress");
        v.note_replicas(hid("m3"), vec![rep("d", "nvme0", 0, true)], 5000);
        assert_eq!(v.copy_progress[&("d".to_string(), hid("m3"))], (0, 1000));
        v.note_replicas(hid("m3"), vec![rep("d", "nvme0", 3, true)], 10_000);
        assert_eq!(v.copy_progress[&("d".to_string(), hid("m3"))], (3_000_000, 10_000));
        // Progress too small to move the percentage still counts.
        let crawl = ReplicaReport { copied_bytes: 3_000_001, ..rep("d", "nvme0", 3, true) };
        v.note_replicas(hid("m3"), vec![crawl.clone()], 70_000);
        assert_eq!(v.copy_progress[&("d".to_string(), hid("m3"))], (3_000_001, 70_000));
        v.note_replicas(hid("m3"), vec![crawl], 75_000);
        assert_eq!(v.copy_progress[&("d".to_string(), hid("m3"))], (3_000_001, 70_000), "no bytes: no progress");
        v.note_replicas(hid("m3"), vec![rep("d", "nvme0", 100, false)], 15_000);
        assert!(v.copy_progress.is_empty(), "complete: forgotten");
        assert_eq!(v.replicas[&hid("m3")], [rep("d", "nvme0", 100, false)]);
    }

    #[test]
    fn placing_a_target_afresh_or_marking_it_stale_restarts_its_stall_clock() {
        let (s, mut v) = whole(4);
        let key = |h: &str| ("d".to_string(), hid(h));
        for h in ["m1", "m4"] {
            v.copy_progress.insert(key(h), (5, 0));
        }
        v.copy_progress.insert(key("m2"), (5, 0));
        let cmd = Command::SetVolumeStatus {
            id: "d".into(),
            replicas: vec![stale("m1"), healthy("m2"), healthy("m3"), rebuilding("m4", 0)],
            health: VolumeHealth::Rebuilding,
        };
        v.note_proposed(&s, &cmd);
        assert_eq!(v.copy_progress.keys().cloned().collect::<Vec<_>>(), [key("m2")], "m1 went stale, m4 is a new target");
        // A status that only moves an existing copy on keeps its clock.
        let mut s = s;
        apply(&mut s, vec![cmd]);
        v.copy_progress.insert(key("m4"), (5, 0));
        let progress = Command::SetVolumeStatus {
            id: "d".into(),
            replicas: vec![stale("m1"), healthy("m2"), healthy("m3"), rebuilding("m4", 10)],
            health: VolumeHealth::Rebuilding,
        };
        v.note_proposed(&s, &progress);
        assert!(v.copy_progress.contains_key(&key("m4")));
    }

    #[test]
    fn with_no_other_host_a_stale_hosts_slot_is_reused_once_it_dropped_its_copy() {
        let (mut s, mut v) = whole(3);
        let st = vec![healthy("m1"), healthy("m2"), stale("m3")];
        s.apply(0, Command::SetVolumeStatus { id: "d".into(), replicas: st, health: VolumeHealth::Degraded }).unwrap();
        v.replicas.insert(hid("m3"), vec![rep("d", "nvme0", 12, true)]);
        let out = run(&s, &v, 1000);
        assert!(statuses(&out).is_empty(), "m3 still holds its copy: {out:?}");
        assert_eq!(raised(&out), ["insufficientstorage-d-1", "volumedegraded-d-1"]);
        assert_eq!(deletes(&out), [(hid("m3"), "d".to_string())]);
        v.replicas.insert(hid("m3"), vec![]);
        let out = run(&s, &v, 2000);
        let again = ReplicaRef { disk: "nvme1".into(), ..rebuilding("m3", 0) };
        assert_eq!(statuses(&out), [(vec![healthy("m1"), healthy("m2"), again], VolumeHealth::Rebuilding)], "the stale entry still counts on nvme0");
    }

    #[test]
    fn a_faulted_volume_keeps_its_stale_replica_and_salvages_it_when_its_host_returns() {
        let (mut s, mut v) = whole(3);
        s.apply(0, Command::CreateVolume { volume: Volume { replicas: 1, ..Volume::new("one", 10) } }).unwrap();
        s.apply(0, Command::SetVolumeStatus { id: "one".into(), replicas: vec![healthy("m1")], health: VolumeHealth::Healthy }).unwrap();
        v.replicas.insert(hid("m1"), vec![rep("d", "nvme0", 100, false), rep("one", "nvme0", 100, false)]);
        s.apply(0, Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::NotReady }).unwrap();
        let out = run(&s, &v, 61_000);
        let one = |out: &[Action]| {
            proposals(out).into_iter().find_map(|c| match c {
                Command::SetVolumeStatus { id, replicas, health } if id == "one" => Some((replicas, health)),
                _ => None,
            })
        };
        assert_eq!(one(&out), Some((vec![stale("m1")], VolumeHealth::Faulted)), "never emptied: it held data");
        assert!(raised(&out).contains(&"volumefaulted-one-1".to_string()), "{out:?}");
        apply(&mut s, proposals(&out));
        // m1 is back: its copy of `one` is the only one, so it is salvaged, not deleted.
        assert_eq!(one(&run(&s, &v, 62_000)), None, "not while m1 is NotReady");
        s.apply(63_000, Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::Ready }).unwrap();
        let out = run(&s, &v, 63_000);
        assert_eq!(deletes(&out), [(hid("m1"), "d".to_string())], "only its copy of d, which has healthy ones");
        assert_eq!(one(&out), Some((vec![healthy("m1")], VolumeHealth::Healthy)), "salvaged");
        assert!(proposals(&out).contains(&Command::ResolveAlert { id: "volumefaulted-one-1".into() }));
    }

    #[test]
    fn replicas_nothing_wants_are_deleted() {
        let (mut s, mut v) = whole(4);
        s.apply(0, Command::CreateVolume { volume: Volume::new("e", 40) }).unwrap();
        v.replicas.insert(hid("m4"), vec![rep("gone", "nvme0", 100, false), rep("d", "nvme1", 100, false), rep("e", "nvme0", 30, false)]);
        let mut out = deletes(&run(&s, &v, 1000));
        out.sort();
        let del = |h: &str, vol: &str| (hid(h), vol.to_string());
        assert_eq!(out, [del("m4", "d"), del("m4", "e"), del("m4", "gone")], "unlisted for its volume, or the volume is gone");
    }

    #[test]
    fn a_host_holding_an_unlisted_copy_or_not_heard_from_is_no_target() {
        // A volume deleted and created again: m1 still holds the old one, complete.
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume { replicas: 2, ..Volume::new("d", 40) } }).unwrap();
        let mut v = heard_from(&s, 1);
        v.replicas.insert(hid("m1"), vec![rep("d", "nvme0", 100, false)]);
        let out = run(&s, &v, 1000);
        assert_eq!(statuses(&out), [(vec![seeding("m3"), seeding("m2")], VolumeHealth::Faulted)], "never on m1");
        assert_eq!(deletes(&out), [(hid("m1"), "d".to_string())]);
        // Nor does a copy (or a seed) go to a host not heard from: it might hold an old copy.
        let (mut s, mut v) = whole(4);
        s.apply(0, Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::NotReady }).unwrap();
        v.replicas.remove(&hid("m4"));
        let out = run(&s, &v, 61_000);
        assert_eq!(statuses(&out), [(vec![stale("m1"), healthy("m2"), healthy("m3")], VolumeHealth::Degraded)]);
    }
}
