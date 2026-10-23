//! Scheduler: runs what the user asked for on the hosts.
//!
//! * **Pods and VMs.** A pending one is placed ([`place`]) on a `Ready` host
//!   with the active roles, capability and room for it; the status naming the
//!   host is committed first (a pod stays `Pending` with its host, a VM goes
//!   `Starting`), then the host's agent is sent `StartInstance` (again every
//!   [`RESEND_MS`] until it reports the instance). Once the assigned host's
//!   heartbeat lists the instance it is `Running`. A VM waits for its volume
//!   to have a healthy replica on a host that is not lost, and one starting
//!   goes back to waiting if the volume loses it. One that fits nowhere stays `Pending` with
//!   the reason, written only when it changes; a VM that fits nowhere is
//!   only tried again every [`NO_FIT_RETRY_MS`].
//! * **Lost instances.** A running pod or VM whose host's heartbeat no
//!   longer lists it (the host rebooted within the not-ready window) goes
//!   back to starting on the same host, so it is sent `StartInstance` again.
//! * **Lost hosts.** A VM whose host is lost is placed again elsewhere, a
//!   pod is sent back to pending ([`failover`](super::failover)). Volumes are the storage controller's
//!   ([`storage`](super::storage)).
//! * **Migrations and drains** run here too ([`drain`](super::drain)), so
//!   their placements and the scheduler's count against each other.
//!
//! Statuses are only proposed when they would change.

use super::drain;
use super::failover::{self, host_lost, NO_FIT, NO_FIT_RETRY_MS};
use super::{Action, Volatile};
use crate::proto::{mgmt, pod_instance, vm_instance, FlowEnd, FlowKind, FlowSpec, InstanceSpec, Msg, UsageProfile};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, HostObj, MGMT_NIC};
use model::{allocatable, place, HostId, HostView, NetworkId, PlacementReq, PodPhase, ReplicaState, Role, SwitchId, Vm, VmPhase};
use std::collections::{BTreeMap, BTreeSet};

/// A `StartInstance`, `CreateReplica`, `StopInstance` or `DeleteReplica` the
/// host has not acted on is sent again this often.
pub const RESEND_MS: u64 = 10_000;
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
    let mut out = vms(state, v, now, &mut hosts);
    out.extend(pods(state, v, now, &mut hosts));
    let more = drain::run(state, v, now, &mut hosts, &out);
    out.extend(more);
    out
}

/// A pod (deployment, idx) or a VM.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Workload<'a> {
    Pod(&'a str, u32),
    Vm(&'a str),
}

/// The hosts as [`place`] sees them, with what each placement of this run takes.
pub(super) struct Hosts {
    views: Vec<HostView>,
}

impl Hosts {
    /// Every host, with the requests of the pods and VMs assigned to it: by
    /// a status proposal still in flight, else by the state.
    pub(super) fn new(state: &ClusterState, v: &Volatile) -> Self {
        let mut views: Vec<HostView> = state.hosts.values().map(host_view).collect();
        // Where each pod and VM takes resources, and how much.
        let mut charges: BTreeMap<Workload, (Vec<&HostId>, (u32, u32))> = BTreeMap::new();
        let pod_request = |dep: &str| state.deployments.get(dep).map(|d| (d.deployment.cpu_m, d.deployment.ram_mb));
        for pod in state.pods.values() {
            if let Some(req) = pod_request(&pod.status.deployment) {
                charges.insert(Workload::Pod(&pod.status.deployment, pod.status.idx), (pod.status.host.iter().collect(), req));
            }
        }
        for (id, st) in &state.vm_status {
            if let Some(vm) = state.vms.get(id) {
                charges.insert(Workload::Vm(id), (vm_hosts(&st.status.host, &st.status.phase), vm_request(&vm.vm)));
            }
        }
        // An in-flight proposal replaces the committed assignment.
        for cmd in v.in_flight.values() {
            match cmd {
                Command::SetPodStatus { deployment, idx, host, .. } => {
                    if let Some(req) = pod_request(deployment) {
                        charges.insert(Workload::Pod(deployment, *idx), (host.iter().collect(), req));
                    }
                }
                Command::SetVmStatus { id, host, phase } => {
                    if let Some(vm) = state.vms.get(id) {
                        charges.insert(Workload::Vm(id), (vm_hosts(host, phase), vm_request(&vm.vm)));
                    }
                }
                _ => {}
            }
        }
        for (hosts, (cpu_m, ram_mb)) in charges.into_values() {
            for host in hosts {
                if let Some(h) = views.iter_mut().find(|h| h.id == *host) {
                    h.used_cpu_m = h.used_cpu_m.saturating_add(cpu_m);
                    h.used_ram_mb = h.used_ram_mb.saturating_add(ram_mb);
                }
            }
        }
        Self { views }
    }

    /// Places `req` and counts it against the host it lands on.
    fn place(&mut self, req: &PlacementReq) -> Result<HostId, String> {
        self.place_excluding(req, &BTreeSet::new())
    }

    /// As [`Hosts::place`], only ever on `host`.
    pub(super) fn place_on(&mut self, req: &PlacementReq, host: &HostId) -> Result<HostId, String> {
        let view = self.views.iter_mut().find(|h| h.id == *host).ok_or_else(|| format!("no host {host}"))?;
        let host = place(req, std::slice::from_ref(&*view), &BTreeSet::new())?;
        view.used_cpu_m = view.used_cpu_m.saturating_add(req.cpu_m);
        view.used_ram_mb = view.used_ram_mb.saturating_add(req.ram_mb);
        Ok(host)
    }

    /// As [`Hosts::place`], never on a host in `exclude`.
    pub(super) fn place_excluding(&mut self, req: &PlacementReq, exclude: &BTreeSet<HostId>) -> Result<HostId, String> {
        let host = place(req, &self.views, exclude)?;
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
pub(super) fn active_roles(h: &HostObj) -> BTreeSet<Role> {
    h.roles.iter().copied().filter(|r| !h.net.held_roles.contains_key(r)).collect()
}

/// The switch on the far end of the host's mgmt NIC, as LLDP reported it.
pub(super) fn access_switch(h: &HostObj) -> Option<SwitchId> {
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
pub(super) fn in_flight(v: &Volatile, pick: impl Fn(&Command) -> bool) -> bool {
    v.in_flight.values().any(pick)
}

/// Whether `host`'s latest heartbeat lists `instance`.
pub(super) fn reported(v: &Volatile, host: &HostId, instance: &str) -> bool {
    v.instances.get(host).is_some_and(|i| i.contains(instance))
}

/// Whether `host`'s latest heartbeat leaves `instance` out: it is not running there.
fn missing(v: &Volatile, host: &HostId, instance: &str) -> bool {
    v.instances.get(host).is_some_and(|i| !i.contains(instance))
}

pub(super) fn agent(host: &HostId) -> Endpoint {
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
            (VmPhase::Starting | VmPhase::Running, Some(host)) if host_lost(state, host, now) => {
                out.extend(failover::fail_over(state, now, id, vm, host, hosts));
            }
            (VmPhase::Pending(why), _) if why.starts_with(NO_FIT) && !retry_due(status.since, now) => {}
            (VmPhase::Pending(_), _) => {
                let (host, phase) = match volume_ready(state, &vm.volume, now).and_then(|()| hosts.place(&PlacementReq::vm(vm))) {
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
                } else if let Err(why) = volume_ready(state, &vm.volume, now) {
                    // Its volume lost its last healthy replica before the VM came up.
                    out.push(Action::Propose(Command::SetVmStatus { id: id.clone(), host: None, phase: VmPhase::Pending(why) }));
                } else {
                    out.extend(start(v, now, host, vm_spec(state, vm, host)));
                }
            }
            (VmPhase::Running, Some(host)) if missing(v, host, &vm_instance(id)) => {
                out.push(Action::Propose(Command::SetVmStatus { id: id.clone(), host: Some(host.clone()), phase: VmPhase::Starting }));
            }
            _ => {}
        }
    }
    out
}

/// Whether a VM that fit nowhere when its status was written at `since` is
/// due another try: once every [`NO_FIT_RETRY_MS`], in the controller run
/// that falls in the first period of each.
fn retry_due(since: SimTime, now: SimTime) -> bool {
    now.saturating_sub(since) % NO_FIT_RETRY_MS < crate::replica::CONTROLLER_PERIOD_MS
}

/// A VM only starts once its volume has a healthy replica on a host that is
/// not lost: the storage controller may not have marked a lost host's
/// replicas stale yet (it acts on the same committed state in the same run).
pub(super) fn volume_ready(state: &ClusterState, volume: &str, now: SimTime) -> Result<(), String> {
    let healthy = state.volume_status.get(volume).is_some_and(|s| {
        s.status.replicas.iter().any(|r| r.state == ReplicaState::Healthy && !host_lost(state, &r.host, now))
    });
    if healthy { Ok(()) } else { Err(format!("waiting for volume {volume}")) }
}

/// What `host` runs for `vm`: writes to each replica of its volume on
/// another host, and client traffic on its first `vm-*` network.
pub(super) fn vm_spec(state: &ClusterState, vm: &Vm, host: &HostId) -> InstanceSpec {
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
            (_, Some(host)) if host_lost(state, host, now) => out.extend(failover::evict_pod(dep, *idx, host)),
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
            (PodPhase::Running, Some(host)) if missing(v, host, &pod_instance(dep, *idx)) => {
                out.push(set(Some(host.clone()), PodPhase::Pending(STARTING.into())));
            }
            _ => {}
        }
    }
    out
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::proto::ReplicaReport;
    use crate::state::Lifecycle;
    use model::network::LldpNeighbor;
    use model::{profile, Deployment, Inventory, PortName, ProfileId, ReplicaRef, Selector, Volume, VolumeHealth};

    pub(crate) fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    /// Ready hosts: (id, profile, roles, access switch).
    pub(crate) fn state(hosts: &[(&str, ProfileId, &[Role], &str)]) -> ClusterState {
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

    pub(crate) const STORAGE_VM: &[Role] = &[Role::Storage, Role::Vm, Role::Compute];

    /// Three MS-01s on switches a, a, b.
    pub(crate) fn three_ms01() -> ClusterState {
        state(&[("m1", ProfileId::Ms01, STORAGE_VM, "a"), ("m2", ProfileId::Ms01, STORAGE_VM, "a"), ("m3", ProfileId::Ms01, STORAGE_VM, "b")])
    }

    pub(crate) fn proposals(actions: &[Action]) -> Vec<Command> {
        actions.iter().filter_map(|a| if let Action::Propose(c) = a { Some(c.clone()) } else { None }).collect()
    }

    pub(crate) fn sends(actions: &[Action]) -> Vec<(HostId, Msg)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Endpoint { addr: Addr::Host(h), svc: Svc::Agent }, _, m) => Some((h.clone(), m.clone())),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn apply(s: &mut ClusterState, cmds: Vec<Command>) {
        for c in cmds {
            s.apply(0, c).unwrap();
        }
    }

    /// Every replica host in the volume's status reports its replica `pct` complete.
    pub(crate) fn report_all(v: &mut Volatile, s: &ClusterState, volume: &str, pct: u8) {
        for r in &s.volume_status[volume].status.replicas {
            let rs: &mut Vec<ReplicaReport> = v.replicas.entry(r.host.clone()).or_default();
            rs.retain(|rep| rep.volume != volume);
            rs.push(rep(volume, &r.disk, pct, pct < 100));
        }
    }

    /// A report of a replica `pct` copied; each percent is a million bytes.
    pub(crate) fn rep(volume: &str, disk: &str, pct: u8, copying: bool) -> ReplicaReport {
        ReplicaReport { volume: volume.into(), disk: disk.into(), pct, copied_bytes: u64::from(pct) * 1_000_000, copying }
    }

    pub(crate) fn seeding(host: &str) -> ReplicaRef {
        ReplicaRef { host: hid(host), disk: "nvme0".into(), state: ReplicaState::Seeding }
    }

    pub(crate) fn rebuilding(host: &str, pct: u8) -> ReplicaRef {
        ReplicaRef { host: hid(host), disk: "nvme0".into(), state: ReplicaState::Rebuilding { pct } }
    }

    pub(crate) fn healthy(host: &str) -> ReplicaRef {
        ReplicaRef { host: hid(host), disk: "nvme0".into(), state: ReplicaState::Healthy }
    }

    #[test]
    fn an_in_flight_placement_replaces_the_committed_one() {
        let mut s = state(&[("a", ProfileId::MiniPc, &[Role::Compute], "a"), ("b", ProfileId::MiniPc, &[Role::Compute], "a")]);
        s.apply(0, Command::CreateDeployment { deployment: deployment("x", 1, 3000, None) }).unwrap();
        s.apply(0, Command::SetPodStatus { deployment: "x".into(), idx: 0, host: Some(hid("a")), phase: PodPhase::Pending(STARTING.into()) }).unwrap();
        s.apply(0, Command::CreateDeployment { deployment: deployment("y", 1, 3000, None) }).unwrap();
        let mut v = Volatile::new(0, 1);
        v.in_flight.insert(1, Command::SetPodStatus { deployment: "x".into(), idx: 0, host: Some(hid("b")), phase: PodPhase::Pending(STARTING.into()) });
        let out = proposals(&run(&s, &v, 1000));
        let y = Command::SetPodStatus { deployment: "y".into(), idx: 0, host: Some(hid("a")), phase: PodPhase::Pending(STARTING.into()) };
        assert_eq!(out, [y], "x is moving to b, so a has room");
    }

    pub(crate) fn vm(id: &str, vcpu: u32) -> Vm {
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

    pub(crate) fn deployment(id: &str, replicas: u32, cpu_m: u32, prefix: Option<&str>) -> Deployment {
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
}
