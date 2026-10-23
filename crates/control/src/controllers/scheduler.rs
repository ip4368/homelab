//! Scheduler: runs what the user asked for on the hosts.
//!
//! * **Pods and VMs.** A pending one is placed ([`place`]) on a `Ready` host
//!   with the active roles, capability and room for it; the status naming the
//!   host is committed first (a pod stays `Pending` with its host, a VM goes
//!   `Starting`), then the host's agent is sent `StartInstance` (again every
//!   [`RESEND_MS`] until it reports the instance). Once the assigned host's
//!   heartbeat lists the instance it is `Running`. A VM waits for its volume
//!   to have a healthy replica. One that fits nowhere stays `Pending` with
//!   the reason, written only when it changes.
//! * **Volumes.** A volume with fewer live (not stale) replicas than it asks
//!   for gets [`place_replicas`] targets, written to its status as
//!   `Rebuilding` before the targets are sent `CreateReplica`. A volume's
//!   first replicas start empty; later ones are copied from a healthy
//!   replica, and wait until there is one. A target's heartbeat reports its
//!   progress, which is written in steps of [`PROGRESS_STEP_PCT`], and
//!   `Healthy` once complete. A volume that cannot get all its replicas stays
//!   `Degraded` with an [`INSUFFICIENT_STORAGE_HOSTS`] alert until it can.
//!
//! Statuses are only proposed when they would change.

use super::{Action, Volatile};
use crate::proto::{mgmt, pod_instance, vm_instance, FlowEnd, FlowKind, FlowSpec, InstanceSpec, Msg, UsageProfile};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{Alert, ClusterState, Command, HostObj, Lifecycle, ObjectKind, ObjectRef, Severity, MGMT_NIC};
use model::{
    allocatable, place, place_replicas, DiskView, HostId, HostView, NetworkId, PlacementReq, PodPhase, ReplicaRef,
    ReplicaState, Role, StorageHostView, SwitchId, Vm, VmPhase, VolumeHealth,
};
use std::collections::{BTreeMap, BTreeSet};

pub const INSUFFICIENT_STORAGE_HOSTS: &str = "InsufficientStorageHosts";
/// A `StartInstance` or `CreateReplica` the host has not acted on is sent again this often.
pub const RESEND_MS: u64 = 10_000;
/// Copy progress is written to the volume status in steps of this many percent.
pub const PROGRESS_STEP_PCT: u8 = 10;
/// Why an assigned pod is not running yet.
pub const STARTING: &str = "starting";

/// A VM writes to every replica of its volume on another host (Mbit/s range).
pub const REPLICA_WRITE_MBPS: (f64, f64) = (20.0, 200.0);
/// A VM's client traffic to the router on its first `vm-*` network.
pub const VM_CLIENT_MBPS: (f64, f64) = (5.0, 50.0);
/// A transcode pod streams this much to the router.
pub const TRANSCODE_MBPS: f64 = 15.0;
/// Any other pod's traffic, to the router.
pub const POD_MBPS: (f64, f64) = (1.0, 10.0);
const VM_USAGE: UsageProfile = UsageProfile { base_pct: 30.0, noise_pct: 30.0, burst_every_s: 300 };
const TRANSCODE_USAGE: UsageProfile = UsageProfile { base_pct: 50.0, noise_pct: 30.0, burst_every_s: 0 };
const POD_USAGE: UsageProfile = UsageProfile { base_pct: 20.0, noise_pct: 20.0, burst_every_s: 120 };

pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut hosts = Hosts::new(state, v);
    let mut out = volumes(state, v, now);
    out.extend(vms(state, v, now, &mut hosts));
    out.extend(pods(state, v, now, &mut hosts));
    out
}

/// The hosts as [`place`] sees them, with what each placement of this run takes.
struct Hosts {
    views: Vec<HostView>,
}

impl Hosts {
    /// Every host, with the requests of the pods and VMs assigned to it in
    /// the state or by a status proposal still in flight.
    fn new(state: &ClusterState, v: &Volatile) -> Self {
        let mut views: Vec<HostView> = state.hosts.values().map(host_view).collect();
        let mut take = |host: &HostId, (cpu_m, ram_mb): (u32, u32)| {
            if let Some(h) = views.iter_mut().find(|h| h.id == *host) {
                h.used_cpu_m = h.used_cpu_m.saturating_add(cpu_m);
                h.used_ram_mb = h.used_ram_mb.saturating_add(ram_mb);
            }
        };
        for pod in state.pods.values() {
            if let (Some(host), Some(d)) = (&pod.status.host, state.deployments.get(&pod.status.deployment)) {
                take(host, (d.deployment.cpu_m, d.deployment.ram_mb));
            }
        }
        for (id, st) in &state.vm_status {
            let Some(vm) = state.vms.get(id) else { continue };
            for host in vm_hosts(&st.status.host, &st.status.phase) {
                take(host, vm_request(&vm.vm));
            }
        }
        for cmd in v.in_flight.values() {
            match cmd {
                Command::SetPodStatus { deployment, host: Some(host), .. } => {
                    if let Some(d) = state.deployments.get(deployment) {
                        take(host, (d.deployment.cpu_m, d.deployment.ram_mb));
                    }
                }
                Command::SetVmStatus { id, host, phase } => {
                    if let Some(vm) = state.vms.get(id) {
                        for host in vm_hosts(host, phase) {
                            take(host, vm_request(&vm.vm));
                        }
                    }
                }
                _ => {}
            }
        }
        Self { views }
    }

    /// Places `req` and counts it against the host it lands on.
    fn place(&mut self, req: &PlacementReq) -> Result<HostId, String> {
        let host = place(req, &self.views, &BTreeSet::new())?;
        if let Some(h) = self.views.iter_mut().find(|h| h.id == host) {
            h.used_cpu_m = h.used_cpu_m.saturating_add(req.cpu_m);
            h.used_ram_mb = h.used_ram_mb.saturating_add(req.ram_mb);
        }
        Ok(host)
    }
}

fn host_view(h: &HostObj) -> HostView {
    let (alloc_cpu_m, alloc_ram_mb) = allocatable(&h.profile);
    HostView {
        id: h.id.clone(),
        lifecycle: h.lifecycle,
        active_roles: active_roles(h),
        capabilities: h.profile.capabilities.clone(),
        alloc_cpu_m,
        alloc_ram_mb,
        used_cpu_m: 0,
        used_ram_mb: 0,
        switch: access_switch(h),
    }
}

/// The host's roles, less those held inactive by a network it cannot use.
fn active_roles(h: &HostObj) -> BTreeSet<Role> {
    h.roles.iter().copied().filter(|r| !h.net.held_roles.contains_key(r)).collect()
}

/// The switch on the far end of the host's mgmt NIC, as LLDP reported it.
fn access_switch(h: &HostObj) -> Option<SwitchId> {
    h.lldp.iter().find(|n| n.local_port.as_str() == MGMT_NIC).map(|n| n.switch.clone())
}

/// The hosts a VM in this state takes resources on: where it runs, and
/// where it is migrating to.
fn vm_hosts<'a>(host: &'a Option<HostId>, phase: &'a VmPhase) -> Vec<&'a HostId> {
    match phase {
        VmPhase::Starting | VmPhase::Running => host.iter().collect(),
        VmPhase::Migrating { to } => host.iter().chain([to]).collect(),
        VmPhase::Pending(_) | VmPhase::Stopped => vec![],
    }
}

fn vm_request(vm: &Vm) -> (u32, u32) {
    let r = PlacementReq::vm(vm);
    (r.cpu_m, r.ram_mb)
}

/// Whether a proposal matching `pick` is in flight.
fn in_flight(v: &Volatile, pick: impl Fn(&Command) -> bool) -> bool {
    v.in_flight.values().any(pick)
}

/// Whether `host`'s latest heartbeat lists `instance`.
fn reported(v: &Volatile, host: &HostId, instance: &str) -> bool {
    v.instances.get(host).is_some_and(|i| i.contains(instance))
}

fn agent(host: &HostId) -> Endpoint {
    Endpoint::new(Addr::Host(host.clone()), Svc::Agent)
}

/// `StartInstance` for `spec` on `host`, unless it was sent there less than [`RESEND_MS`] ago.
fn start(v: &Volatile, now: SimTime, host: &HostId, spec: InstanceSpec) -> Option<Action> {
    let recent = v.instance_started.get(&spec.id).is_some_and(|(h, at)| h == host && now < at + RESEND_MS);
    (!recent).then(|| Action::Send(agent(host), mgmt(), Msg::StartInstance(spec)))
}

fn vms(state: &ClusterState, v: &Volatile, now: SimTime, hosts: &mut Hosts) -> Vec<Action> {
    let mut out = vec![];
    for (id, st) in &state.vm_status {
        let Some(vm) = state.vms.get(id).map(|o| &o.vm) else { continue };
        if in_flight(v, |c| matches!(c, Command::SetVmStatus { id: i, .. } if i == id)) {
            continue;
        }
        let status = &st.status;
        match (&status.phase, &status.host) {
            (VmPhase::Pending(_), _) => {
                let (host, phase) = match volume_ready(state, &vm.volume).and_then(|()| hosts.place(&PlacementReq::vm(vm))) {
                    Ok(host) => (Some(host), VmPhase::Starting),
                    Err(why) => (None, VmPhase::Pending(why)),
                };
                if (&host, &phase) != (&status.host, &status.phase) {
                    out.push(Action::Propose(Command::SetVmStatus { id: id.clone(), host, phase }));
                }
            }
            (VmPhase::Starting, Some(host)) => {
                if reported(v, host, &vm_instance(id)) {
                    out.push(Action::Propose(Command::SetVmStatus { id: id.clone(), host: Some(host.clone()), phase: VmPhase::Running }));
                } else {
                    out.extend(start(v, now, host, vm_spec(state, vm, host)));
                }
            }
            _ => {}
        }
    }
    out
}

/// A VM only starts once its volume has a healthy replica.
fn volume_ready(state: &ClusterState, volume: &str) -> Result<(), String> {
    let healthy = state
        .volume_status
        .get(volume)
        .is_some_and(|s| s.status.replicas.iter().any(|r| r.state == ReplicaState::Healthy));
    if healthy { Ok(()) } else { Err(format!("waiting for volume {volume}")) }
}

/// What `host` runs for `vm`: writes to each replica of its volume on
/// another host, and client traffic on its first `vm-*` network.
fn vm_spec(state: &ClusterState, vm: &Vm, host: &HostId) -> InstanceSpec {
    let mut flows: Vec<FlowSpec> = vec![];
    let replicas = state.volume_status.get(&vm.volume).map_or(&[][..], |s| &s.status.replicas[..]);
    let mut remote: Vec<&HostId> = replicas.iter().filter(|r| r.state != ReplicaState::Stale && r.host != *host).map(|r| &r.host).collect();
    remote.dedup();
    for r in remote {
        flows.push(flow(FlowKind::ReplicaWrite, FlowEnd::Host(r.clone()), NetworkId::from("storage"), REPLICA_WRITE_MBPS));
    }
    if let Some(net) = vm.networks.iter().find(|n| n.as_str().starts_with("vm-")) {
        flows.push(flow(FlowKind::VmClient, FlowEnd::Router, net.clone(), VM_CLIENT_MBPS));
    }
    let (cpu_m, ram_mb) = vm_request(vm);
    InstanceSpec { id: vm_instance(&vm.id), cpu_m, ram_mb, usage: VM_USAGE, flows }
}

fn flow(kind: FlowKind, to: FlowEnd, network: NetworkId, (min_mbps, max_mbps): (f64, f64)) -> FlowSpec {
    FlowSpec { kind, to, network, min_mbps, max_mbps }
}

fn pods(state: &ClusterState, v: &Volatile, now: SimTime, hosts: &mut Hosts) -> Vec<Action> {
    let mut out = vec![];
    for ((dep, idx), pod) in &state.pods {
        let Some(d) = state.deployments.get(dep).map(|o| &o.deployment) else { continue };
        if in_flight(v, |c| matches!(c, Command::SetPodStatus { deployment, idx: i, .. } if deployment == dep && i == idx)) {
            continue;
        }
        let status = &pod.status;
        let set = |host: Option<HostId>, phase: PodPhase| {
            Action::Propose(Command::SetPodStatus { deployment: dep.clone(), idx: *idx, host, phase })
        };
        match (&status.phase, &status.host) {
            (PodPhase::Pending(_), None) => {
                let (host, phase) = match hosts.place(&PlacementReq::pod(d)) {
                    Ok(host) => (Some(host), PodPhase::Pending(STARTING.into())),
                    Err(why) => (None, PodPhase::Pending(why)),
                };
                if (&host, &phase) != (&status.host, &status.phase) {
                    out.push(set(host, phase));
                }
            }
            (PodPhase::Pending(_), Some(host)) => {
                let id = pod_instance(dep, *idx);
                if reported(v, host, &id) {
                    out.push(set(Some(host.clone()), PodPhase::Running));
                } else {
                    let transcode = d.required_roles().contains(&Role::Transcode);
                    let (usage, flow) = if transcode {
                        (TRANSCODE_USAGE, flow(FlowKind::Transcode, FlowEnd::Router, mgmt(), (TRANSCODE_MBPS, TRANSCODE_MBPS)))
                    } else {
                        (POD_USAGE, flow(FlowKind::Pod, FlowEnd::Router, mgmt(), POD_MBPS))
                    };
                    let spec = InstanceSpec { id, cpu_m: d.cpu_m, ram_mb: d.ram_mb, usage, flows: vec![flow] };
                    out.extend(start(v, now, host, spec));
                }
            }
            _ => {}
        }
    }
    out
}

/// The storage hosts as [`place_replicas`] sees them: every host (so that
/// the switches of replicas on hosts that cannot take more still count),
/// each disk's free space less the replicas the volume statuses put there.
fn storage_views(state: &ClusterState) -> Vec<StorageHostView> {
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
    let mut open: BTreeMap<&str, &Alert> = state
        .alerts
        .values()
        .filter(|a| a.kind == INSUFFICIENT_STORAGE_HOSTS && a.resolved_at.is_none() && a.entity.kind == ObjectKind::Volume)
        .map(|a| (a.entity.id.as_str(), a))
        .collect();
    let mut out = vec![];
    for (id, vol) in &state.volumes {
        let alert = open.remove(id.as_str());
        let Some(st) = state.volume_status.get(id).map(|s| &s.status) else { continue };
        if in_flight(v, |c| matches!(c, Command::SetVolumeStatus { id: i, .. } if i == id)) {
            continue;
        }
        let (size_gb, want) = (vol.volume.size_gb, usize::from(vol.volume.replicas));
        let mut replicas = st.replicas.clone();
        for r in replicas.iter_mut() {
            progress(v, id, r);
        }
        let live = replicas.iter().filter(|r| r.state != ReplicaState::Stale).count();
        // The first replicas start empty; later ones need a healthy one to copy.
        let can_place = replicas.is_empty() || replicas.iter().any(|r| r.state == ReplicaState::Healthy);
        let mut short = false;
        if live < want && can_place {
            let existing: Vec<HostId> = replicas.iter().map(|r| r.host.clone()).collect();
            let need = u8::try_from(want - live).unwrap_or(u8::MAX);
            let picks = place_replicas(need, size_gb, &views, &existing);
            short = picks.len() < usize::from(need);
            for (host, disk) in picks {
                take_disk(&mut views, &host, &disk, size_gb);
                replicas.push(ReplicaRef { host, disk, state: ReplicaState::Rebuilding { pct: 0 } });
            }
        }
        let health = health(&replicas, want);
        if replicas != st.replicas || health != st.health {
            out.push(Action::Propose(Command::SetVolumeStatus { id: id.clone(), replicas: replicas.clone(), health }));
        }
        out.extend(create_replicas(state, v, now, id, size_gb, &st.replicas));
        let live = replicas.iter().filter(|r| r.state != ReplicaState::Stale).count();
        out.extend(insufficient_alert(state, v, id, short, (live, want), alert));
    }
    // Alerts of volumes that are gone.
    out.extend(open.values().map(|a| Action::Propose(Command::ResolveAlert { id: a.id.clone() })));
    out
}

/// Moves a copying replica on as far as its host reported it: in steps of
/// [`PROGRESS_STEP_PCT`], `Healthy` once complete, on the disk the host chose.
fn progress(v: &Volatile, volume: &str, r: &mut ReplicaRef) {
    let ReplicaState::Rebuilding { pct } = r.state else { return };
    let Some((_, disk, done)) = v.replicas.get(&r.host).and_then(|rs| rs.iter().find(|(vol, _, _)| vol == volume)) else {
        return;
    };
    r.disk = disk.clone();
    if *done >= 100 {
        r.state = ReplicaState::Healthy;
    } else {
        let step = done / PROGRESS_STEP_PCT * PROGRESS_STEP_PCT;
        r.state = ReplicaState::Rebuilding { pct: step.max(pct) };
    }
}

/// Counts a new replica against the free space of the disk it lands on.
fn take_disk(views: &mut [StorageHostView], host: &HostId, disk: &str, size_gb: u32) {
    if let Some(d) = views.iter_mut().filter(|h| h.id == *host).flat_map(|h| h.disks.iter_mut()).find(|d| d.name == disk) {
        d.free_gb = d.free_gb.saturating_sub(size_gb);
    }
}

/// `Healthy` with all the replicas it asks for healthy, `Faulted` with none,
/// `Rebuilding` while one is copying, else `Degraded`.
fn health(replicas: &[ReplicaRef], want: usize) -> VolumeHealth {
    let healthy = replicas.iter().filter(|r| r.state == ReplicaState::Healthy).count();
    if healthy == 0 {
        VolumeHealth::Faulted
    } else if healthy >= want {
        VolumeHealth::Healthy
    } else if replicas.iter().any(|r| matches!(r.state, ReplicaState::Rebuilding { .. })) {
        VolumeHealth::Rebuilding
    } else {
        VolumeHealth::Degraded
    }
}

/// `CreateReplica` to each host the committed status has copying a replica
/// that its heartbeat does not list yet, at most every [`RESEND_MS`]. The
/// copy comes from a healthy replica, one on a `Ready` host if there is one,
/// or is empty when the volume has no healthy replica (its first replicas).
fn create_replicas(state: &ClusterState, v: &Volatile, now: SimTime, volume: &str, size_gb: u32, replicas: &[ReplicaRef]) -> Vec<Action> {
    let healthy = || replicas.iter().filter(|r| r.state == ReplicaState::Healthy);
    let ready = |r: &&ReplicaRef| state.hosts.get(&r.host).is_some_and(|h| h.lifecycle == Lifecycle::Ready);
    let source = healthy().find(ready).or_else(|| healthy().next()).map(|r| r.host.clone());
    replicas
        .iter()
        .filter(|r| matches!(r.state, ReplicaState::Rebuilding { .. }))
        .filter(|r| !v.replicas.get(&r.host).is_some_and(|rs| rs.iter().any(|(vol, _, _)| vol == volume)))
        .filter(|r| v.replica_created.get(&(volume.to_string(), r.host.clone())).is_none_or(|at| now >= at + RESEND_MS))
        .map(|r| {
            let msg = Msg::CreateReplica { volume: volume.into(), size_gb, source: source.clone() };
            Action::Send(agent(&r.host), mgmt(), msg)
        })
        .collect()
}

/// Raises the volume's [`INSUFFICIENT_STORAGE_HOSTS`] alert when there are
/// not enough storage hosts for its replicas (`short`), and resolves it once
/// every replica it wants is placed (`live` of `want` are).
fn insufficient_alert(
    state: &ClusterState,
    v: &Volatile,
    volume: &str,
    short: bool,
    (live, want): (usize, usize),
    open: Option<&Alert>,
) -> Option<Action> {
    match open {
        Some(a) if live >= want => Some(Action::Propose(Command::ResolveAlert { id: a.id.clone() })),
        None if short => {
            let prefix = format!("insufficientstorage-{volume}-");
            if in_flight(v, |c| matches!(c, Command::RaiseAlert { id, .. } if id.starts_with(&prefix))) {
                return None;
            }
            let base = format!("{prefix}{}", v.term);
            let id = (1..)
                .map(|n| if n == 1 { base.clone() } else { format!("{base}-{n}") })
                .find(|id| !state.alerts.contains_key(id))
                .expect("some id is free");
            Some(Action::Propose(Command::RaiseAlert {
                id,
                severity: Severity::Warning,
                kind: INSUFFICIENT_STORAGE_HOSTS.into(),
                entity: ObjectRef::volume(volume),
                message: format!("volume {volume} has {live} of {want} replicas: not enough storage hosts"),
                ground_truth: false,
            }))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ReplicaReport;
    use model::network::LldpNeighbor;
    use model::{profile, Deployment, Inventory, PortName, ProfileId, Selector, Volume};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    /// Ready hosts: (id, profile, roles, access switch).
    fn state(hosts: &[(&str, ProfileId, &[Role], &str)]) -> ClusterState {
        let mut s = ClusterState::new();
        for &(h, p, roles, switch) in hosts {
            let id = hid(h);
            let lldp = vec![LldpNeighbor { local_port: PortName::from(MGMT_NIC), switch: SwitchId::from(switch), switch_port: PortName::from("p1") }];
            for cmd in [
                Command::AddPendingHost { id: id.clone(), profile: profile(p, 0), inventory: Inventory::from_profile(p, 0), location: "rack-1".into() },
                Command::ApproveHost { id: id.clone(), roles: roles.iter().copied().collect() },
                Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Provisioning },
                Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Ready },
                Command::ObserveHost { id: id.clone(), lldp, inventory: Inventory::from_profile(p, 0) },
            ] {
                s.apply(0, cmd).unwrap();
            }
        }
        s
    }

    const STORAGE_VM: &[Role] = &[Role::Storage, Role::Vm, Role::Compute];

    /// Three MS-01s on switches a, a, b.
    fn three_ms01() -> ClusterState {
        state(&[("m1", ProfileId::Ms01, STORAGE_VM, "a"), ("m2", ProfileId::Ms01, STORAGE_VM, "a"), ("m3", ProfileId::Ms01, STORAGE_VM, "b")])
    }

    fn proposals(actions: &[Action]) -> Vec<Command> {
        actions.iter().filter_map(|a| if let Action::Propose(c) = a { Some(c.clone()) } else { None }).collect()
    }

    fn sends(actions: &[Action]) -> Vec<(HostId, Msg)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Endpoint { addr: Addr::Host(h), svc: Svc::Agent }, _, m) => Some((h.clone(), m.clone())),
                _ => None,
            })
            .collect()
    }

    fn apply(s: &mut ClusterState, cmds: Vec<Command>) {
        for c in cmds {
            s.apply(0, c).unwrap();
        }
    }

    /// Every replica host in the volume's status reports its replica `pct` complete.
    fn report_all(v: &mut Volatile, s: &ClusterState, volume: &str, pct: u8) {
        for r in &s.volume_status[volume].status.replicas {
            let rs: &mut Vec<ReplicaReport> = v.replicas.entry(r.host.clone()).or_default();
            rs.retain(|(vol, _, _)| vol != volume);
            rs.push((volume.into(), r.disk.clone(), pct));
        }
    }

    fn rebuilding(host: &str, pct: u8) -> ReplicaRef {
        ReplicaRef { host: hid(host), disk: "nvme0".into(), state: ReplicaState::Rebuilding { pct } }
    }

    fn healthy(host: &str) -> ReplicaRef {
        ReplicaRef { host: hid(host), disk: "nvme0".into(), state: ReplicaState::Healthy }
    }

    #[test]
    fn a_new_volume_gets_empty_replicas_status_first_then_healthy_once_reported() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let mut v = Volatile::new(0, 1);
        let out = run(&s, &v, 1000);
        assert!(sends(&out).is_empty(), "nothing is sent before the status names the targets");
        let placed = vec![rebuilding("m1", 0), rebuilding("m3", 0), rebuilding("m2", 0)];
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
        let out = run(&s, &v, 1000);
        let placed = vec![healthy("m1"), healthy("m3"), rebuilding("m2", 0)];
        assert_eq!(proposals(&out), [Command::SetVolumeStatus { id: "d".into(), replicas: placed, health: VolumeHealth::Rebuilding }]);
        apply(&mut s, proposals(&out));
        let create = Msg::CreateReplica { volume: "d".into(), size_gb: 40, source: Some(hid("m1")) };
        assert_eq!(sends(&run(&s, &v, 2000)), [(hid("m2"), create)]);

        v.replicas.insert(hid("m2"), vec![("d".into(), "nvme0".into(), 9)]);
        assert!(run(&s, &v, 3000).is_empty(), "below the first step: nothing to write");
        v.replicas.insert(hid("m2"), vec![("d".into(), "nvme1".into(), 37)]);
        let out = run(&s, &v, 4000);
        let at30 = ReplicaRef { host: hid("m2"), disk: "nvme1".into(), state: ReplicaState::Rebuilding { pct: 30 } };
        assert_eq!(proposals(&out), [Command::SetVolumeStatus { id: "d".into(), replicas: vec![healthy("m1"), healthy("m3"), at30], health: VolumeHealth::Rebuilding }], "the disk the host chose is recorded");
        apply(&mut s, proposals(&out));
        v.replicas.insert(hid("m2"), vec![("d".into(), "nvme1".into(), 39)]);
        assert!(run(&s, &v, 5000).is_empty());
        v.replicas.insert(hid("m2"), vec![("d".into(), "nvme1".into(), 100)]);
        let out = run(&s, &v, 6000);
        assert!(matches!(proposals(&out).as_slice(), [Command::SetVolumeStatus { health: VolumeHealth::Healthy, .. }]));
    }

    #[test]
    fn a_copy_waits_for_a_healthy_source() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let st = Command::SetVolumeStatus { id: "d".into(), replicas: vec![rebuilding("m1", 0)], health: VolumeHealth::Faulted };
        s.apply(0, st).unwrap();
        let out = run(&s, &Volatile::new(0, 1), 1000);
        assert!(proposals(&out).is_empty(), "no healthy replica to copy from: {out:?}");
    }

    #[test]
    fn too_few_storage_hosts_leave_the_volume_degraded_with_one_alert() {
        let mut s = state(&[("m1", ProfileId::Ms01, STORAGE_VM, "a"), ("m2", ProfileId::Ms01, STORAGE_VM, "b"), ("p1", ProfileId::Pi5, &[Role::Compute], "a")]);
        s.apply(0, Command::CreateVolume { volume: Volume::new("d", 40) }).unwrap();
        let mut v = Volatile::new(0, 4);
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
        assert!(matches!(out.as_slice(), [Command::SetVolumeStatus { health: VolumeHealth::Degraded, .. }]), "{out:?}");
        apply(&mut s, out);
        assert!(run(&s, &v, 4000).is_empty(), "no busy loop");

        // A third storage host arrives: the replica is placed there, which resolves the alert.
        let mut more = state(&[("m3", ProfileId::Ms01, STORAGE_VM, "c")]);
        s.hosts.append(&mut more.hosts);
        let out = proposals(&run(&s, &v, 5000));
        assert!(matches!(out.as_slice(), [Command::SetVolumeStatus { health: VolumeHealth::Rebuilding, .. }, Command::ResolveAlert { id }] if id == "insufficientstorage-d-4"), "{out:?}");
    }

    fn vm(id: &str, vcpu: u32) -> Vm {
        Vm { id: id.into(), vcpu, ram_mb: 4096, disk_gb: 40, networks: vec!["vm-iot".into()], volume: format!("{id}-disk") }
    }

    #[test]
    fn a_vm_waits_for_its_volume_is_placed_status_first_and_runs_once_reported() {
        let mut s = three_ms01();
        s.apply(0, Command::CreateVolume { volume: Volume::new("vm1-disk", 40) }).unwrap();
        s.apply(0, Command::CreateVm { vm: vm("vm1", 2) }).unwrap();
        let mut v = Volatile::new(0, 1);
        let out = proposals(&run(&s, &v, 1000));
        let waiting = Command::SetVmStatus { id: "vm1".into(), host: None, phase: VmPhase::Pending("waiting for volume vm1-disk".into()) };
        assert!(out.contains(&waiting), "{out:?}");
        apply(&mut s, out);
        let replicas = vec![healthy("m1"), healthy("m3"), healthy("m2")];
        s.apply(0, Command::SetVolumeStatus { id: "vm1-disk".into(), replicas, health: VolumeHealth::Healthy }).unwrap();

        let out = run(&s, &v, 2000);
        assert!(sends(&out).is_empty());
        let starting = Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Starting };
        assert_eq!(proposals(&out), [starting]);
        apply(&mut s, proposals(&out));

        let out = run(&s, &v, 3000);
        let [(to, Msg::StartInstance(spec))]: [(HostId, Msg); 1] = sends(&out).try_into().unwrap() else { panic!("{out:?}") };
        assert_eq!((to, spec.id.as_str(), spec.cpu_m, spec.ram_mb), (hid("m1"), "vm/vm1", 2000, 4096));
        let flows: Vec<_> = spec.flows.iter().map(|f| (f.kind, f.to.clone(), f.network.as_str(), f.min_mbps, f.max_mbps)).collect();
        assert_eq!(flows, [
            (FlowKind::ReplicaWrite, FlowEnd::Host(hid("m3")), "storage", 20.0, 200.0),
            (FlowKind::ReplicaWrite, FlowEnd::Host(hid("m2")), "storage", 20.0, 200.0),
            (FlowKind::VmClient, FlowEnd::Router, "vm-iot", 5.0, 50.0),
        ], "writes to the replicas on other hosts, client traffic on its vm network");
        v.instance_started.insert("vm/vm1".into(), (hid("m1"), 3000));
        assert!(sends(&run(&s, &v, 3000 + RESEND_MS - 1)).is_empty());
        assert_eq!(sends(&run(&s, &v, 3000 + RESEND_MS)).len(), 1, "not reported: sent again");

        v.instances.insert(hid("m2"), ["vm/vm1".to_string()].into());
        assert!(proposals(&run(&s, &v, 4000)).is_empty(), "only the assigned host counts");
        v.instances.insert(hid("m1"), ["vm/vm1".to_string()].into());
        let out = run(&s, &v, 4000);
        assert_eq!(proposals(&out), [Command::SetVmStatus { id: "vm1".into(), host: Some(hid("m1")), phase: VmPhase::Running }]);
        assert!(sends(&out).is_empty());
    }

    fn deployment(id: &str, replicas: u32, cpu_m: u32, prefix: Option<&str>) -> Deployment {
        Deployment {
            id: id.into(),
            image: "img".into(),
            replicas,
            cpu_m,
            ram_mb: 512,
            selector: Selector { roles: Default::default(), capability_prefix: prefix.map(Into::into) },
        }
    }

    #[test]
    fn pods_spread_over_hosts_and_unfit_ones_say_why_once() {
        let mut s = state(&[
            ("a", ProfileId::MiniPc, &[Role::Compute, Role::Transcode], "a"),
            ("b", ProfileId::MiniPc, &[Role::Compute], "a"),
            ("p", ProfileId::Pi5, &[Role::Compute, Role::Transcode], "a"),
        ]);
        s.apply(0, Command::CreateDeployment { deployment: deployment("web", 3, 1000, None) }).unwrap();
        s.apply(0, Command::CreateDeployment { deployment: deployment("tc", 2, 3000, Some("hw-transcode:")) }).unwrap();
        let mut v = Volatile::new(0, 1);
        let out = proposals(&run(&s, &v, 1000));
        let hosts: Vec<(String, u32, Option<HostId>, PodPhase)> = out
            .iter()
            .map(|c| match c {
                Command::SetPodStatus { deployment, idx, host, phase } => (deployment.clone(), *idx, host.clone(), phase.clone()),
                c => panic!("{c:?}"),
            })
            .collect();
        let starting = || PodPhase::Pending(STARTING.into());
        // tc pods take 3000 m: one each on the two transcode hosts; web spreads over what is left.
        assert_eq!(hosts, [
            ("tc".into(), 0, Some(hid("a")), starting()),
            ("tc".into(), 1, Some(hid("p")), starting()),
            ("web".into(), 0, Some(hid("b")), starting()),
            ("web".into(), 1, Some(hid("b")), starting()),
            ("web".into(), 2, Some(hid("b")), starting()),
        ]);
        for c in &out {
            v.in_flight.insert(v.in_flight.len() as u64, c.clone());
        }
        assert!(proposals(&run(&s, &v, 2000)).is_empty(), "nothing is placed twice while its status is in flight");
        v.in_flight.clear();
        apply(&mut s, out);

        let out = run(&s, &v, 3000);
        let started: Vec<_> = sends(&out)
            .into_iter()
            .map(|(h, m)| match m {
                Msg::StartInstance(spec) => (h, spec.id, spec.flows[0].kind, spec.flows[0].to.clone(), spec.flows[0].network.clone(), spec.flows[0].min_mbps, spec.flows[0].max_mbps),
                m => panic!("{m:?}"),
            })
            .collect();
        assert_eq!(started[0], (hid("a"), "pod/tc/0".into(), FlowKind::Transcode, FlowEnd::Router, mgmt(), 15.0, 15.0));
        assert_eq!(started[2], (hid("b"), "pod/web/0".into(), FlowKind::Pod, FlowEnd::Router, mgmt(), 1.0, 10.0));

        s.apply(0, Command::CreateDeployment { deployment: deployment("big", 1, 3000, None) }).unwrap();
        let out = proposals(&run(&s, &v, 4000));
        let why = "no fit: 3 insufficient cpu".to_string();
        assert_eq!(out, [Command::SetPodStatus { deployment: "big".into(), idx: 0, host: None, phase: PodPhase::Pending(why) }]);
        apply(&mut s, out);
        assert!(proposals(&run(&s, &v, 5000)).is_empty(), "the reason is written only when it changes");

        v.instances.insert(hid("a"), ["pod/tc/0".to_string()].into());
        let out = proposals(&run(&s, &v, 6000));
        assert_eq!(out, [Command::SetPodStatus { deployment: "tc".into(), idx: 0, host: Some(hid("a")), phase: PodPhase::Running }]);
    }

    #[test]
    fn replicas_spread_over_switches_and_count_held_space() {
        let mut s = three_ms01();
        // m3's disks are nearly full of another volume's replica.
        s.apply(0, Command::CreateVolume { volume: Volume::new("big", 1990) }).unwrap();
        let big = vec![ReplicaRef { host: hid("m3"), disk: "nvme0".into(), state: ReplicaState::Healthy }, ReplicaRef { host: hid("m3"), disk: "nvme1".into(), state: ReplicaState::Healthy }];
        s.apply(0, Command::SetVolumeStatus { id: "big".into(), replicas: big, health: VolumeHealth::Degraded }).unwrap();
        s.apply(0, Command::CreateVolume { volume: Volume { replicas: 2, ..Volume::new("d", 20) } }).unwrap();
        let out = proposals(&run(&s, &Volatile::new(0, 1), 1000));
        let placed = out.iter().find_map(|c| match c {
            Command::SetVolumeStatus { id, replicas, .. } if id == "d" => Some(replicas.iter().map(|r| r.host.clone()).collect::<Vec<_>>()),
            _ => None,
        });
        assert_eq!(placed, Some(vec![hid("m1"), hid("m2")]), "m3 has no room left");
    }
}
