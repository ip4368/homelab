//! Workloads: the scheduler places pods and VMs, volumes get their
//! replicas, and the instances run on the simulated hosts with resource
//! usage and traffic. Tests read ground truth only to check the outcome.

use control::client::{ClientActor, CLIENT};
use control::controllers::storage::INSUFFICIENT_STORAGE_HOSTS;
use control::proto::{ApiOp, ApiResult, LogCmd};
use control::raft::EntryPayload;
use control::replica::CpReplica;
use control::runtime::{Addr, Endpoint, Svc};
use control::state::{Command, Lifecycle, ObjectKind, ObjectRef};
use model::{
    Deployment, HostId, NetworkId, PodPhase, PodStatus, PortName, ProfileId, ReplicaState, Role, Selector, SwitchId, Vm, VmPhase, Volume,
    VolumeHealth, TRANSCODE_CAPABILITY_PREFIX,
};
use sim::engine::Engine;
use sim::faults::Fault;
use sim::presets::{FleetSpec, Preset};
use sim::scenario::{Scenario, SEED_VM_FILL, TRANSCODE_DEPLOYMENT};
use sim::topology::{Node, PartitionMember, PortMode};
use sim::traffic::{FlowEnd, FlowKind};
use sim::truth::SPLIT_BRAIN;
use std::collections::BTreeSet;

fn default_fleet(seed: u64) -> Engine {
    let mut e = Engine::new(seed);
    e.load_preset(Preset::Default);
    assert!(e.run_until(10_000, |e| e.client_view().leader.is_some()), "no leader");
    e
}

/// Everything the scheduler was asked to run is running, and every volume is healthy.
fn all_running(e: &Engine) -> bool {
    let s = &e.client_view().state;
    !s.vms.is_empty()
        && s.vm_status.values().all(|v| v.status.phase == VmPhase::Running)
        && s.pods.values().all(|p| p.status.phase == PodPhase::Running)
        && s.volume_status.values().all(|v| v.status.health == VolumeHealth::Healthy)
}

/// The default fleet with the seed workloads, all running.
fn seeded(seed: u64) -> Engine {
    let mut e = default_fleet(seed);
    e.scenario(Scenario::SeedWorkloads).unwrap();
    let deadline = e.now() + 120_000;
    let ok = e.run_until(deadline, all_running);
    let s = &e.client_view().state;
    let not_running: Vec<String> = s
        .vm_status
        .iter()
        .filter(|(_, v)| v.status.phase != VmPhase::Running)
        .map(|(id, v)| format!("{id}: {:?}", v.status.phase))
        .chain(s.pods.values().filter(|p| p.status.phase != PodPhase::Running).map(|p| format!("{:?}", p.status)))
        .chain(s.volume_status.iter().filter(|(_, v)| v.status.health != VolumeHealth::Healthy).map(|(id, v)| format!("{id}: {:?}", v.status)))
        .take(10)
        .collect();
    assert!(ok, "not all running within 120 s: {not_running:?}");
    e
}

#[test]
fn seed_workloads_all_running() {
    let e = seeded(41);
    let s = &e.client_view().state;
    // 60 hosts: 30 VMs with their volumes, the transcode deployment and 6 general ones.
    assert_eq!((s.vms.len(), s.volumes.len(), s.deployments.len()), (30, 30, 7));
    assert_eq!(s.pods.len(), 12 + 6 * 3, "transcode: SBC count / 2 = 12; general: 6 x 3");
    for vm in s.vms.values().map(|o| &o.vm) {
        assert!((2..=4).contains(&vm.vcpu) && (4096..=8192).contains(&vm.ram_mb) && (40..=80).contains(&vm.disk_gb), "{vm:?}");
        assert!(vm.networks == [NetworkId::from("vm-lan")] || vm.networks == [NetworkId::from("vm-iot")], "{vm:?}");
        assert_eq!(s.volumes[&vm.volume].volume.size_gb, vm.disk_gb);
    }
    // Ground truth agrees: every instance runs exactly where the control plane put it.
    let w = e.fabric().world();
    let mut expected: BTreeSet<(HostId, String)> = BTreeSet::new();
    for (id, v) in &s.vm_status {
        expected.insert((v.status.host.clone().unwrap(), format!("vm/{id}")));
    }
    for p in s.pods.values() {
        expected.insert((p.status.host.clone().unwrap(), format!("pod/{}/{}", p.status.deployment, p.status.idx)));
    }
    let actual: BTreeSet<(HostId, String)> =
        w.hosts.iter().flat_map(|(h, hw)| hw.instances.keys().map(move |i| (h.clone(), i.clone()))).collect();
    assert_eq!(actual, expected);
}

#[test]
fn seed_vms_fit_the_vm_hosts() {
    // One MS-01 can give VMs 19.5 cores; 60 hosts would otherwise get 30 VMs of 2-4 vCPU.
    let mut e = Engine::new(43);
    e.load_fleet(&FleetSpec { ms01: 1, minipc: 59, pi5: 0, rk3588: 0 }, 3);
    e.scenario(Scenario::SeedWorkloads).unwrap();
    let deadline = e.now() + 120_000;
    let running = |e: &Engine| {
        let s = &e.client_view().state;
        !s.vms.is_empty() && s.vm_status.len() == s.vms.len() && s.vm_status.values().all(|v| v.status.phase == VmPhase::Running)
    };
    assert!(e.run_until(deadline, running), "{:?}", e.client_view().state.vm_status.values().map(|v| &v.status.phase).collect::<Vec<_>>());
    let s = &e.client_view().state;
    let vcpus: u32 = s.vms.values().map(|o| o.vm.vcpu).sum();
    assert!(s.vms.len() < 30, "{} VMs", s.vms.len());
    assert!(f64::from(vcpus * 1000) <= SEED_VM_FILL * 19_500.0, "{vcpus} vCPUs asked of one MS-01");
    assert_eq!(s.volumes.len(), s.vms.len(), "a volume per VM");
}

#[test]
fn vm_replicas_on_three_storage_hosts() {
    let e = seeded(42);
    let s = &e.client_view().state;
    let w = e.fabric().world();
    for vm in s.vms.values().map(|o| &o.vm) {
        let replicas = &s.volume_status[&vm.volume].status.replicas;
        let hosts: BTreeSet<&HostId> = replicas.iter().map(|r| &r.host).collect();
        assert_eq!((replicas.len(), hosts.len()), (3, 3), "{}: {replicas:?}", vm.volume);
        for r in replicas {
            assert_eq!(r.state, ReplicaState::Healthy);
            assert!(s.hosts[&r.host].roles.contains(&Role::Storage), "{} on {}", vm.volume, r.host);
            let held = w.hosts[&r.host].replicas.get(&vm.volume).unwrap_or_else(|| panic!("{} holds no {}", r.host, vm.volume));
            assert_eq!((&held.disk, held.pct), (&r.disk, 100));
        }
    }
}

#[test]
fn transcode_pods_only_on_capable_hosts() {
    let e = seeded(43);
    let s = &e.client_view().state;
    let transcode: Vec<_> = s.pods.values().filter(|p| p.status.deployment == TRANSCODE_DEPLOYMENT).collect();
    assert_eq!(transcode.len(), 12);
    for p in transcode {
        let host = &s.hosts[p.status.host.as_ref().unwrap()];
        assert!(host.roles.contains(&Role::Transcode), "{p:?} on {}", host.id);
        assert!(host.profile.capabilities.iter().any(|c| c.starts_with(TRANSCODE_CAPABILITY_PREFIX)), "{p:?} on {}", host.id);
    }
}

#[test]
fn workload_traffic_and_usage() {
    let mut e = seeded(44);
    e.run_for(30_000);
    let s = e.client_view().state.clone();
    let w = e.fabric().world();
    let last = w.traffic.last();
    let flows: Vec<_> = w.traffic.flows().collect();
    let of = |k: FlowKind| flows.iter().filter(move |f| f.kind == k);
    // Replica writes: one per remote replica of each VM's volume, on storage, 20-200 Mbps.
    for (id, vs) in &s.vm_status {
        let host = vs.status.host.clone().unwrap();
        let vm = &s.vms[id].vm;
        let remote: BTreeSet<FlowEnd> = s.volume_status[&vm.volume]
            .status
            .replicas
            .iter()
            .filter(|r| r.host != host)
            .map(|r| FlowEnd::Host(r.host.clone()))
            .collect();
        let writes: BTreeSet<FlowEnd> =
            of(FlowKind::ReplicaWrite).filter(|f| f.src == FlowEnd::Host(host.clone())).map(|f| f.dst.clone()).collect();
        assert!(remote.is_subset(&writes), "{id} on {host}: writes to {writes:?}, replicas {remote:?}");
        let client: Vec<_> = of(FlowKind::VmClient).filter(|f| f.src == FlowEnd::Host(host.clone())).collect();
        assert!(client.iter().any(|f| f.dst == FlowEnd::Router && f.network == vm.networks[0]), "{id}: {client:?}");
    }
    for f in of(FlowKind::ReplicaWrite) {
        assert_eq!(f.network, NetworkId::from("storage"));
        assert!((20.0..=200.0).contains(&f.demand_mbps), "{f:?}");
        assert!(last.rate[&f.id] > 0.0, "replica write {f:?} has no route");
    }
    for f in of(FlowKind::VmClient) {
        assert!((5.0..=50.0).contains(&f.demand_mbps) && last.rate[&f.id] > 0.0, "{f:?}");
    }
    let transcode: Vec<_> = of(FlowKind::Transcode).collect();
    assert_eq!(transcode.len(), 12);
    for f in transcode {
        assert_eq!((&f.dst, f.network.as_str(), f.demand_mbps), (&FlowEnd::Router, "mgmt", 15.0));
        assert_eq!(last.rate[&f.id], 15.0);
    }
    assert_eq!(of(FlowKind::Pod).count(), 18);
    for f in of(FlowKind::Pod) {
        assert!(f.network.as_str() == "mgmt" && (1.0..=10.0).contains(&f.demand_mbps), "{f:?}");
    }
    // Demands follow seeded curves: they move over time.
    let before: Vec<f64> = of(FlowKind::ReplicaWrite).map(|f| f.demand_mbps).collect();
    e.run_for(60_000);
    let after: Vec<f64> = e.fabric().world().traffic.flows().filter(|f| f.kind == FlowKind::ReplicaWrite).map(|f| f.demand_mbps).collect();
    assert_ne!(before, after);

    // Hosts running instances report more than the 3% baseline; idle ones report it.
    let s = &e.client_view().state;
    let busy: BTreeSet<&HostId> = s.vm_status.values().filter_map(|v| v.status.host.as_ref()).collect();
    let m = &e.client_view().metrics;
    for h in &busy {
        assert!(m[*h].cpu_pct > 3.0 && m[*h].instances > 0, "{h}: {:?}", m[*h]);
    }
    let w = e.fabric().world();
    for h in s.hosts.keys().filter(|h| w.hosts[*h].instances.is_empty()) {
        assert_eq!((m[h].cpu_pct, m[h].instances), (3.0, 0), "{h}");
    }
    // Replicas fill their disks, above the 5 % an empty disk shows.
    let r = s.volume_status.values().next().unwrap().status.replicas[0].clone();
    assert!(m[&r.host].disk_pct[&r.disk] > 5.0, "{:?}", m[&r.host].disk_pct);
}

/// Log entries of the leader's replica that satisfy `pick`.
fn logged(e: &Engine, pick: impl Fn(&Command) -> bool) -> usize {
    let leader = e.true_leader().expect("a leader");
    let replica = e.actor::<CpReplica>(&Endpoint::new(Addr::Host(leader), Svc::Cp)).unwrap();
    replica
        .durable()
        .log
        .iter()
        .filter(|entry| matches!(&entry.payload, EntryPayload::Normal(LogCmd { cmd, .. }) if pick(cmd)))
        .count()
}

fn about_volume(cmd: &Command, id: &str) -> bool {
    let entity = ObjectRef { kind: ObjectKind::Volume, id: id.into() };
    match cmd {
        Command::SetVolumeStatus { id: v, .. } => v == id,
        Command::RaiseAlert { entity: e, .. } | Command::Event { entity: e, .. } => *e == entity,
        Command::ResolveAlert { id: a } => a.contains(id),
        _ => false,
    }
}

fn api(e: &mut Engine, op: ApiOp) -> ApiResult {
    let req = e.api(op);
    let deadline = e.now() + 3000;
    e.run_until(deadline, |e| e.actor::<ClientActor>(&CLIENT).is_some_and(|c| c.result(req).is_some()));
    e.take_api_result(req).expect("an answer within 3 s")
}

#[test]
fn insufficient_storage_hosts_degraded_alert() {
    // Two MS-01s hold storage; the RK3588 is the third CP voter and holds none.
    let mut e = Engine::new(45);
    e.load_fleet(&FleetSpec { ms01: 2, minipc: 0, pi5: 0, rk3588: 1 }, 3);
    assert!(e.run_until(15_000, |e| e.client_view().leader.is_some()), "no leader");
    assert_eq!(api(&mut e, ApiOp::CreateVolume { volume: Volume::new("data", 50) }), ApiResult::Ok(serde_json::Value::Null));
    let created = logged(&e, |c| about_volume(c, "data"));
    e.run_for(60_000);
    let s = &e.client_view().state;
    let status = &s.volume_status["data"].status;
    assert_eq!(status.health, VolumeHealth::Degraded, "{status:?}");
    assert_eq!(status.replicas.len(), 2);
    assert!(status.replicas.iter().all(|r| r.state == ReplicaState::Healthy), "{status:?}");
    let alerts: Vec<_> = s.alerts.values().filter(|a| a.kind == INSUFFICIENT_STORAGE_HOSTS && a.resolved_at.is_none()).collect();
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0].entity, ObjectRef { kind: ObjectKind::Volume, id: "data".into() });
    let first_minute = logged(&e, |c| about_volume(c, "data")) - created;
    assert!(first_minute <= 5, "{first_minute} entries for the volume in a minute");
    e.run_for(60_000);
    let second_minute = logged(&e, |c| about_volume(c, "data")) - created - first_minute;
    assert_eq!(second_minute, 0, "nothing changes, so nothing is written");
}

#[test]
fn workload_api_validates() {
    let mut e = default_fleet(46);
    let dep = |replicas| Deployment {
        id: "web".into(),
        image: "nginx".into(),
        replicas,
        cpu_m: 100,
        ram_mb: 128,
        selector: Selector::default(),
    };
    for replicas in [0, 1001] {
        let r = api(&mut e, ApiOp::CreateDeployment { deployment: dep(replicas) });
        assert!(matches!(r, ApiResult::Err(_)), "{replicas} replicas: {r:?}");
    }
    for (size_gb, replicas) in [(0, 3), (10, 0), (10, 6)] {
        let r = api(&mut e, ApiOp::CreateVolume { volume: Volume { id: "v".into(), size_gb, replicas } });
        assert!(matches!(r, ApiResult::Err(_)), "{size_gb} GB x {replicas}: {r:?}");
    }
    assert!(e.client_view().state.deployments.is_empty() && e.client_view().state.volumes.is_empty());
    assert_eq!(api(&mut e, ApiOp::CreateDeployment { deployment: dep(2) }), ApiResult::Ok(serde_json::Value::Null));
    assert_eq!(api(&mut e, ApiOp::DeleteDeployment { id: "web".into() }), ApiResult::Ok(serde_json::Value::Null));
    assert!(matches!(api(&mut e, ApiOp::DeleteVolume { id: "nope".into() }), ApiResult::Err(_)));
}

/// How far the copy of `volume` onto `host` has got, in ground truth.
fn copy(e: &Engine, host: &HostId, volume: &str) -> Option<(u8, bool)> {
    let r = e.fabric().world().hosts.get(host)?.replicas.get(volume)?;
    Some((r.pct, r.flow.is_some()))
}

#[test]
fn interrupted_copies_resume() {
    // Two storage hosts hold the volume; a third joins and gets a copy.
    let mut e = Engine::new(47);
    e.load_fleet(&FleetSpec { ms01: 2, minipc: 0, pi5: 0, rk3588: 1 }, 3);
    assert!(e.run_until(15_000, |e| e.client_view().leader.is_some()), "no leader");
    assert_eq!(api(&mut e, ApiOp::CreateVolume { volume: Volume::new("data", 20) }), ApiResult::Ok(serde_json::Value::Null));
    e.run_for(30_000);
    e.scenario(Scenario::AddHosts { profile: model::ProfileId::Ms01, count: 1, misconfigure: None }).unwrap();
    let target = HostId::from("ms01-03");
    let deadline = e.now() + 180_000;
    assert!(e.run_until(deadline, |e| copy(e, &target, "data").is_some_and(|(pct, _)| pct >= 10)), "no copy onto {target}");
    let rebuilding = &e.client_view().state.volume_status["data"].status;
    assert!(rebuilding.replicas.iter().any(|r| r.host == target && matches!(r.state, ReplicaState::Rebuilding { .. })), "{rebuilding:?}");

    // The target loses power mid-copy and comes back: the copy goes on from where it was.
    e.fault(Fault::KillHost { host: target.clone() }).unwrap();
    let (kept, copying) = copy(&e, &target, "data").unwrap();
    assert!(!copying && kept >= 10);
    e.run_for(5000);
    e.fault(Fault::RestoreHost { host: target.clone() }).unwrap();
    assert_eq!(copy(&e, &target, "data"), Some((kept, false)), "the disk kept the partial copy");
    let deadline = e.now() + 60_000;
    assert!(e.run_until(deadline, |e| copy(e, &target, "data").is_some_and(|(pct, on)| on && pct > kept)), "target copy never resumed");

    // The source loses power: the copy resumes from the other healthy replica.
    let w = e.fabric().world();
    let flow = w.traffic.flows().find(|f| f.kind == FlowKind::Rebuild).expect("a rebuild flow");
    let FlowEnd::Host(source) = flow.src.clone() else { panic!("{flow:?}") };
    e.fault(Fault::KillHost { host: source.clone() }).unwrap();
    let (kept, copying) = copy(&e, &target, "data").unwrap();
    assert!(!copying, "a copy from a dead source stops");
    let deadline = e.now() + 120_000;
    let resumed = |e: &Engine| {
        let from_other = e.fabric().world().traffic.flows().any(|f| f.kind == FlowKind::Rebuild && f.src != FlowEnd::Host(source.clone()));
        from_other && copy(e, &target, "data").is_some_and(|(pct, on)| on && pct > kept)
    };
    assert!(e.run_until(deadline, resumed), "copy never resumed from another source: {:?}", copy(&e, &target, "data"));
    e.fault(Fault::RestoreHost { host: source }).unwrap();
    let deadline = e.now() + 300_000;
    let healthy = |e: &Engine| {
        e.client_view().state.volume_status["data"].status.replicas.iter().any(|r| r.host == target && r.state == ReplicaState::Healthy)
    };
    assert!(e.run_until(deadline, healthy), "copy never completed: {:?}", e.client_view().state.volume_status["data"]);
    assert_eq!(copy(&e, &target, "data"), Some((100, false)));
}

fn ok() -> ApiResult {
    ApiResult::Ok(serde_json::Value::Null)
}

/// Submits every op at once and checks each was accepted.
fn api_all(e: &mut Engine, ops: Vec<ApiOp>) {
    let reqs: Vec<u64> = ops.into_iter().map(|op| e.api(op)).collect();
    let deadline = e.now() + 5000;
    e.run_until(deadline, |e| e.actor::<ClientActor>(&CLIENT).is_some_and(|c| reqs.iter().all(|r| c.result(*r).is_some())));
    for r in reqs {
        assert_eq!(e.take_api_result(r), Some(ok()), "request {r}");
    }
}

/// Creates `n` VMs `vm-01..`, each with `vcpu` vCPUs and its own `disk_gb` volume, and runs until all run.
fn create_vms(e: &mut Engine, n: u32, vcpu: u32, disk_gb: u32) {
    let ids: Vec<String> = (1..=n).map(|i| format!("vm-{i:02}")).collect();
    api_all(e, ids.iter().map(|id| ApiOp::CreateVolume { volume: Volume::new(format!("{id}-disk"), disk_gb) }).collect());
    let vm = |id: &String| Vm { id: id.clone(), vcpu, ram_mb: 8192, disk_gb, networks: vec![NetworkId::from("vm-lan")], volume: format!("{id}-disk") };
    api_all(e, ids.iter().map(|id| ApiOp::CreateVm { vm: vm(id) }).collect());
    let deadline = e.now() + 120_000;
    assert!(e.run_until(deadline, all_running), "VMs not all running: {:?}", e.client_view().state.vm_status);
}

/// The host the control plane runs `vm` on.
fn vm_host(e: &Engine, vm: &str) -> Option<HostId> {
    e.client_view().state.vm_status[vm].status.host.clone()
}

/// Whether `vm` runs on a host other than `not`, as the control plane sees it, and really does there.
fn running_elsewhere(e: &Engine, vm: &str, not: &HostId) -> bool {
    let st = &e.client_view().state.vm_status[vm].status;
    st.phase == VmPhase::Running
        && st.host.as_ref().is_some_and(|h| h != not && e.fabric().world().hosts[h].instances.contains_key(&format!("vm/{vm}")))
}

/// The volumes with a replica (of any state) on `host`.
fn volumes_on(e: &Engine, host: &HostId) -> Vec<String> {
    let s = &e.client_view().state;
    s.volume_status.iter().filter(|(_, v)| v.status.replicas.iter().any(|r| r.host == *host)).map(|(id, _)| id.clone()).collect()
}

/// Every one of `volumes` is healthy with none of its replicas on `host`.
fn healthy_without(e: &Engine, volumes: &[String], host: &HostId) -> bool {
    let s = &e.client_view().state;
    volumes.iter().all(|v| {
        let st = &s.volume_status[v].status;
        st.health == VolumeHealth::Healthy && st.replicas.iter().all(|r| r.host != *host)
    })
}

/// A rebuild flow on `storage` got a rate at the latest traffic tick, and the port it leaves through carried traffic.
fn rebuild_traffic_on_storage(e: &Engine) -> bool {
    let traffic = &e.fabric().world().traffic;
    let last = traffic.last();
    traffic.flows().filter(|f| f.kind == FlowKind::Rebuild && f.network.as_str() == "storage").any(|f| {
        last.rate.get(&f.id).is_some_and(|&r| r > 0.0)
            && last.paths.get(&f.id).and_then(|p| p.first()).is_some_and(|hop| last.port_bps.get(&hop.from).is_some_and(|&(_, tx)| tx > 0.0))
    })
}

/// Runs a second at a time until `done` holds or `deadline` passes, calling `each` after every second.
fn run_checking(e: &mut Engine, deadline: u64, mut each: impl FnMut(&Engine), done: impl Fn(&Engine) -> bool) -> bool {
    while !done(e) {
        if e.now() >= deadline {
            return false;
        }
        e.run_for(1000);
        each(e);
    }
    true
}

#[test]
fn criterion_4_host_failure() {
    let mut e = default_fleet(51);
    create_vms(&mut e, 24, 2, 20);
    let s = e.client_view().state.clone();
    let h = s.vm_status.values().filter_map(|v| v.status.host.clone()).find(|h| !s.cp.voters.contains(h)).expect("a VM host that is not a CP host");
    let vms: Vec<String> = s.vm_status.keys().filter(|id| vm_host(&e, id).as_ref() == Some(&h)).cloned().collect();
    let volumes = volumes_on(&e, &h);
    assert!(!vms.is_empty() && !volumes.is_empty(), "{h}: vms {vms:?}, volumes {volumes:?}");

    let killed = e.now();
    e.fault(Fault::KillHost { host: h.clone() }).unwrap();
    let moved = |e: &Engine| vms.iter().all(|vm| running_elsewhere(e, vm, &h));
    assert!(e.run_until(killed + 60_000 + 40_000 + 15_000, moved), "VMs of {h} not running elsewhere: {:?}", e.client_view().state.vm_status);

    let mut rebuild_seen = false;
    let healed = run_checking(&mut e, killed + 300_000, |e| rebuild_seen |= rebuild_traffic_on_storage(e), |e| healthy_without(e, &volumes, &h));
    let st: Vec<_> = volumes.iter().map(|v| &e.client_view().state.volume_status[v].status).collect();
    assert!(healed, "volumes not rebuilt off {h} within 300 s: {st:?}");
    assert!(rebuild_seen, "no rebuild traffic seen on the storage network");
}

/// Every host cabled to `switch`.
fn hosts_on(e: &Engine, switch: &str) -> Vec<HostId> {
    let t = &e.fabric().world().topology;
    let w = e.fabric().world();
    w.hosts
        .iter()
        .filter(|(id, hw)| {
            hw.profile.nics.iter().any(|n| {
                matches!(t.peer(&Node::HostPort((*id).clone(), PortName::new(&n.name))), Some(Node::SwitchPort(s, _)) if s.as_str() == switch)
            })
        })
        .map(|(id, _)| id.clone())
        .collect()
}

#[test]
fn criterion_6_partition_split_brain() {
    let mut e = default_fleet(52);
    // The default fleet's MS-01s all hang off access-1; the next one racked lands on access-2.
    e.scenario(Scenario::AddHosts { profile: ProfileId::Ms01, count: 1, misconfigure: None }).unwrap();
    let m = HostId::from("ms01-13");
    let peer = e.fabric().world().topology.peer(&Node::HostPort(m.clone(), PortName::from("eth0"))).cloned();
    assert!(matches!(&peer, Some(Node::SwitchPort(s, _)) if s.as_str() == "access-2"), "{peer:?}");
    let ready = |e: &Engine| e.client_view().state.hosts.get(&m).is_some_and(|h| h.lifecycle == Lifecycle::Ready && h.net.held_roles.is_empty());
    let deadline = e.now() + 120_000;
    assert!(e.run_until(deadline, ready), "{m} never ready");
    create_vms(&mut e, 13, 2, 20);
    let s = e.client_view().state.clone();
    let vm = s.vm_status.keys().find(|id| vm_host(&e, id).as_ref() == Some(&m)).expect("a VM on the new MS-01").clone();

    let mut group: Vec<PartitionMember> = vec![PartitionMember::Switch(SwitchId::from("access-2"))];
    group.extend(hosts_on(&e, "access-2").into_iter().map(PartitionMember::Host));
    assert!(group.contains(&PartitionMember::Host(m.clone())));
    let cut = e.now();
    e.fault(Fault::Partition { nodes: group }).unwrap();
    let split = |e: &Engine| running_elsewhere(e, &vm, &m) && e.truth().split_brain_vms.contains(&vm);
    assert!(e.run_until(cut + 60_000 + 40_000 + 15_000, split), "{vm} not restarted elsewhere: {:?}", e.client_view().state.vm_status[&vm]);
    assert!(e.now() >= cut + 60_000 + 35_000, "restarted before the grace period");
    assert!(e.fabric().world().hosts[&m].instances.contains_key(&format!("vm/{vm}")), "the old instance runs on behind the partition");
    let t = e.truth();
    let alert = t.alerts.iter().find(|a| a.entity == ObjectRef::vm(&vm)).expect("a ground-truth split-brain alert");
    assert!(alert.ground_truth && alert.resolved_at.is_none() && alert.kind == SPLIT_BRAIN, "{alert:?}");
    assert!(e.client_view().state.alerts.values().all(|a| !a.ground_truth), "ground truth stays out of the control plane's state");
    e.run_for(10_000);
    assert!(e.truth().split_brain_vms.contains(&vm), "split brain holds with the partition");

    let healed = e.now();
    e.fault(Fault::HealPartitions).unwrap();
    let stopped = |e: &Engine| !e.fabric().world().hosts[&m].instances.contains_key(&format!("vm/{vm}")) && e.truth().split_brain_vms.is_empty();
    assert!(e.run_until(healed + 15_000, stopped), "stale instance of {vm} still on {m}");
    assert!(running_elsewhere(&e, &vm, &m));
    e.run_for(2000);
    let t = e.truth();
    assert!(t.alerts.iter().filter(|a| a.entity == ObjectRef::vm(&vm)).all(|a| a.resolved_at.is_some()), "{:?}", t.alerts);
}

#[test]
fn disk_failure_rebuilds() {
    let mut e = default_fleet(53);
    create_vms(&mut e, 6, 2, 20);
    let s = e.client_view().state.clone();
    let r = s.volume_status["vm-01-disk"].status.replicas[0].clone();
    let on_disk: Vec<String> = s
        .volume_status
        .iter()
        .filter(|(_, v)| v.status.replicas.iter().any(|x| x.host == r.host && x.disk == r.disk))
        .map(|(id, _)| id.clone())
        .collect();
    let failed = e.now();
    e.fault(Fault::FailDisk { host: r.host.clone(), disk: r.disk.clone() }).unwrap();
    let hw = &e.fabric().world().hosts[&r.host];
    assert!(hw.failed_disks.contains(&r.disk) && hw.replicas.values().all(|x| x.disk != r.disk), "its replicas are lost");
    let reported = |e: &Engine| e.client_view().state.hosts[&r.host].inventory.disks.iter().any(|d| d.name == r.disk && d.failed);
    assert!(e.run_until(failed + 15_000, reported), "the agent never reported the failed disk");
    let rebuilt = |e: &Engine| {
        let s = &e.client_view().state;
        on_disk.iter().all(|v| {
            let st = &s.volume_status[v].status;
            st.health == VolumeHealth::Healthy && st.replicas.iter().all(|x| !(x.host == r.host && x.disk == r.disk))
        })
    };
    assert!(e.run_until(failed + 150_000, rebuilt), "{:?}", on_disk.iter().map(|v| &e.client_view().state.volume_status[v].status).collect::<Vec<_>>());
    assert!(e.fault(Fault::FailDisk { host: r.host.clone(), disk: "nope".into() }).is_err());
}

/// Takes VLAN 20 off every trunk `host` is cabled to: its storage traffic goes nowhere.
fn blackhole_storage(e: &mut Engine, host: &HostId) {
    block_vlan(e, host, 20);
}

/// Takes `vlan` off every trunk `host` is cabled to.
fn block_vlan(e: &mut Engine, host: &HostId, vlan: u16) {
    let w = e.fabric().world();
    let mut edits = vec![];
    for n in &w.hosts[host].profile.nics {
        let Some(Node::SwitchPort(s, p)) = w.topology.peer(&Node::HostPort(host.clone(), PortName::new(&n.name))).cloned() else { continue };
        let mut cfg = w.topology.switches[&s].port(&p).unwrap().clone();
        if let PortMode::Trunk { allowed, .. } = &mut cfg.mode {
            allowed.remove(&vlan);
            edits.push((s, p, cfg));
        }
    }
    assert!(!edits.is_empty(), "{host} has no trunk carrying VLAN {vlan}");
    for (switch, port, cfg) in edits {
        e.fault(Fault::SetSwitchPort { switch, port, cfg }).unwrap();
    }
}

/// The host `volume`'s status has copying (rebuilding) a replica, if any.
fn rebuild_target(e: &Engine, volume: &str) -> Option<HostId> {
    let st = &e.client_view().state.volume_status[volume].status;
    st.replicas.iter().find(|r| matches!(r.state, ReplicaState::Rebuilding { .. })).map(|r| r.host.clone())
}

#[test]
fn rebuild_stall_retargets() {
    let mut e = Engine::new(54);
    e.load_fleet(&FleetSpec { ms01: 3, minipc: 3, pi5: 0, rk3588: 0 }, 3);
    assert!(e.run_until(15_000, |e| e.client_view().leader.is_some()), "no leader");
    api_all(&mut e, vec![ApiOp::CreateVolume { volume: Volume::new("data", 20) }]);
    let deadline = e.now() + 30_000;
    let healthy = |e: &Engine| e.client_view().state.volume_status.get("data").is_some_and(|v| v.status.health == VolumeHealth::Healthy);
    assert!(e.run_until(deadline, healthy), "volume never healthy");
    let r = e.client_view().state.volume_status["data"].status.replicas[0].clone();
    e.fault(Fault::FailDisk { host: r.host.clone(), disk: r.disk.clone() }).unwrap();
    let deadline = e.now() + 30_000;
    assert!(e.run_until(deadline, |e| rebuild_target(e, "data").is_some()), "no rebuild placed");
    let t = rebuild_target(&e, "data").unwrap();
    blackhole_storage(&mut e, &t);
    let cut = e.now();

    e.run_for(50_000);
    assert_eq!(rebuild_target(&e, "data"), Some(t.clone()), "abandoned before 60 s without progress");
    let retargeted = |e: &Engine| rebuild_target(e, "data").is_some_and(|x| x != t);
    assert!(e.run_until(cut + 60_000 + 15_000, retargeted), "{:?}", e.client_view().state.volume_status["data"]);
    let new = rebuild_target(&e, "data").unwrap();
    assert_ne!(new, r.host);
    let st = &e.client_view().state.volume_status["data"].status;
    assert!(st.replicas.iter().all(|x| x.host != t || x.state == ReplicaState::Stale), "{st:?}");
    let done = |e: &Engine| healthy_without(e, &["data".to_string()], &t) && !e.fabric().world().hosts[&t].replicas.contains_key("data");
    let deadline = e.now() + 200_000;
    assert!(e.run_until(deadline, done), "{:?}", e.client_view().state.volume_status["data"]);
}

#[test]
fn restore_after_failover() {
    let mut e = default_fleet(55);
    create_vms(&mut e, 24, 2, 20);
    let s = e.client_view().state.clone();
    let leader = e.true_leader().unwrap();
    // A CP follower that runs VMs: its replica must catch up once it is back.
    let h = s.cp.voters.iter().find(|h| **h != leader && s.vm_status.values().any(|v| v.status.host.as_ref() == Some(*h))).unwrap().clone();
    let vms: Vec<String> = s.vm_status.keys().filter(|id| vm_host(&e, id).as_ref() == Some(&h)).cloned().collect();
    let volumes = volumes_on(&e, &h);
    let killed = e.now();
    e.fault(Fault::KillHost { host: h.clone() }).unwrap();
    let recovered = |e: &Engine| vms.iter().all(|vm| running_elsewhere(e, vm, &h)) && healthy_without(e, &volumes, &h);
    assert!(e.run_until(killed + 300_000, recovered), "no failover and rebuild off {h}");

    let restored = e.now();
    e.fault(Fault::RestoreHost { host: h.clone() }).unwrap();
    let cp = |e: &Engine, x: &HostId| e.actor::<CpReplica>(&Endpoint::new(Addr::Host(x.clone()), Svc::Cp)).map(|r| r.commit_index());
    let clean = |e: &Engine| {
        let hw = &e.fabric().world().hosts[&h];
        let s = &e.client_view().state;
        let stale = hw.replicas.keys().any(|v| {
            s.volume_status.get(v).is_none_or(|st| !st.status.replicas.iter().any(|r| r.host == h && r.state != ReplicaState::Stale))
        });
        let leader = e.true_leader();
        !hw.instances.keys().any(|i| i.starts_with("vm/")) && !stale && leader.is_some_and(|l| l != h && cp(e, &h) == cp(e, &l))
    };
    assert!(e.run_until(restored + 30_000, clean), "{h} not clean: instances {:?}, replicas {:?}", e.fabric().world().hosts[&h].instances.keys().collect::<Vec<_>>(), e.fabric().world().hosts[&h].replicas.keys().collect::<Vec<_>>());
}

#[test]
fn failover_without_room_waits_pending_without_busy_looping() {
    // Four MS-01s each run one VM too big to share a host; ms01-04 is not a CP voter.
    let mut e = Engine::new(56);
    e.load_fleet(&FleetSpec { ms01: 4, minipc: 0, pi5: 0, rk3588: 0 }, 3);
    assert!(e.run_until(15_000, |e| e.client_view().leader.is_some()), "no leader");
    create_vms(&mut e, 4, 16, 10);
    let h = HostId::from("ms01-04");
    let vm = e.client_view().state.vm_status.keys().find(|id| vm_host(&e, id).as_ref() == Some(&h)).unwrap().clone();
    let killed = e.now();
    e.fault(Fault::KillHost { host: h.clone() }).unwrap();
    let pending = |e: &Engine| matches!(&e.client_view().state.vm_status[&vm].status.phase, VmPhase::Pending(why) if why.starts_with("no fit:"));
    assert!(e.run_until(killed + 115_000, pending), "{:?}", e.client_view().state.vm_status[&vm]);
    let VmPhase::Pending(why) = e.client_view().state.vm_status[&vm].status.phase.clone() else { unreachable!() };
    assert!(why.contains("insufficient cpu"), "{why}");
    let about_vm = |c: &Command| matches!(c, Command::SetVmStatus { id, .. } if *id == vm);
    let before = logged(&e, about_vm);
    e.run_for(120_000);
    let written = logged(&e, about_vm) - before;
    assert!(written <= 4, "{written} status writes for {vm} in 2 minutes");
    assert!(pending(&e), "{:?}", e.client_view().state.vm_status[&vm]);

    // Room comes back: the next retry places it.
    let restored = e.now();
    e.fault(Fault::RestoreHost { host: h.clone() }).unwrap();
    let back = |e: &Engine| e.client_view().state.vm_status[&vm].status.phase == VmPhase::Running;
    assert!(e.run_until(restored + 30_000 + 15_000, back), "{:?}", e.client_view().state.vm_status[&vm]);
}

#[test]
fn instances_lost_in_a_quick_reboot_start_again() {
    let mut e = default_fleet(57);
    create_vms(&mut e, 12, 2, 20);
    let s = e.client_view().state.clone();
    let h = s.vm_status.values().filter_map(|v| v.status.host.clone()).find(|h| !s.cp.voters.contains(h)).unwrap();
    let vms: Vec<String> = s.vm_status.keys().filter(|id| vm_host(&e, id).as_ref() == Some(&h)).cloned().collect();
    e.fault(Fault::KillHost { host: h.clone() }).unwrap();
    e.run_for(10_000);
    let restored = e.now();
    e.fault(Fault::RestoreHost { host: h.clone() }).unwrap();
    // Back well within the not-ready window: the control plane must notice the instances are gone.
    let running = |e: &Engine| {
        let w = &e.fabric().world().hosts[&h];
        vms.iter().all(|vm| w.instances.contains_key(&format!("vm/{vm}")) && vm_host(e, vm).as_ref() == Some(&h))
    };
    assert!(e.run_until(restored + 30_000, running), "instances on {h} not started again");
    assert_eq!(e.client_view().state.hosts[&h].lifecycle, Lifecycle::Ready);
}

/// Where the copy onto `host` of `volume` comes from, if one runs.
fn copy_source(e: &Engine, host: &HostId, volume: &str) -> Option<FlowEnd> {
    let w = e.fabric().world();
    let flow = w.hosts.get(host)?.replicas.get(volume)?.flow?;
    w.traffic.flow(flow).map(|f| f.src.clone())
}

/// A small fleet with volume `data` (`size_gb`, three replicas) healthy on its MS-01s.
fn fleet_with_volume(seed: u64, spec: FleetSpec, size_gb: u32) -> Engine {
    let mut e = Engine::new(seed);
    e.load_fleet(&spec, 3);
    assert!(e.run_until(15_000, |e| e.client_view().leader.is_some()), "no leader");
    api_all(&mut e, vec![ApiOp::CreateVolume { volume: Volume::new("data", size_gb) }]);
    let deadline = e.now() + 30_000;
    let healthy = |e: &Engine| e.client_view().state.volume_status.get("data").is_some_and(|v| v.status.health == VolumeHealth::Healthy);
    assert!(e.run_until(deadline, healthy), "volume never healthy");
    e
}

#[test]
fn a_copy_whose_source_disk_fails_waits_for_a_real_source() {
    let mut e = fleet_with_volume(58, FleetSpec { ms01: 3, minipc: 2, pi5: 0, rk3588: 0 }, 20);
    let r = e.client_view().state.volume_status["data"].status.replicas[0].clone();
    e.fault(Fault::FailDisk { host: r.host.clone(), disk: r.disk.clone() }).unwrap();
    let deadline = e.now() + 30_000;
    assert!(e.run_until(deadline, |e| rebuild_target(e, "data").is_some_and(|t| copy_source(e, &t, "data").is_some())), "no copy started");
    let t = rebuild_target(&e, "data").unwrap();
    let Some(FlowEnd::Host(source)) = copy_source(&e, &t, "data") else { panic!("copy from no host") };
    e.run_for(2000);
    let disk = e.fabric().world().hosts[&source].replicas["data"].disk.clone();
    e.fault(Fault::FailDisk { host: source.clone(), disk }).unwrap();
    let (kept, copying) = copy(&e, &t, "data").unwrap();
    assert!(!copying && kept < 100, "the copy from the failed disk ends at {kept} %");
    // The control plane still lists the source healthy for a heartbeat or so, and may ask
    // again to copy from it (here such a request arrives at once): nothing may come of that.
    e.fabric_mut().world_mut().create_replica(&t, "data", 20, Some(source.clone()));
    assert_eq!(copy(&e, &t, "data"), Some((kept, false)), "no copy from a replica that is gone");
    let mut progress_from_failed = false;
    let done = |e: &Engine| e.client_view().state.volume_status["data"].status.replicas.iter().any(|x| x.host == t && x.state == ReplicaState::Healthy);
    let deadline = e.now() + 200_000;
    let healed = run_checking(
        &mut e,
        deadline,
        |e| {
            let src = copy_source(e, &t, "data");
            progress_from_failed |= src == Some(FlowEnd::Host(source.clone()));
        },
        done,
    );
    assert!(healed, "{:?}", e.client_view().state.volume_status["data"]);
    assert!(!progress_from_failed, "a copy ran from the host whose disk failed");
    assert_eq!(copy(&e, &t, "data"), Some((100, false)));
}

#[test]
fn a_slow_rebuild_is_not_abandoned_while_bytes_move() {
    // Three MS-01s hold an 80 GB volume; the only other storage host is a mini PC whose
    // link runs at 100 Mbps: its copy moves well under 1 % a minute.
    let mut e = fleet_with_volume(59, FleetSpec { ms01: 3, minipc: 1, pi5: 0, rk3588: 0 }, 80);
    let mini = HostId::from("minipc-01");
    e.fault(Fault::DegradePort { host: mini.clone(), port: PortName::from("eth0"), mbps: 100 }).unwrap();
    let r = e.client_view().state.volume_status["data"].status.replicas[0].clone();
    assert_ne!(r.host, mini);
    e.fault(Fault::FailDisk { host: r.host.clone(), disk: r.disk.clone() }).unwrap();
    let deadline = e.now() + 30_000;
    assert!(e.run_until(deadline, |e| rebuild_target(e, "data").is_some()), "no rebuild placed");
    assert_eq!(rebuild_target(&e, "data"), Some(mini.clone()));
    let mut abandoned = false;
    let done = |e: &Engine| e.client_view().state.volume_status["data"].status.health == VolumeHealth::Healthy;
    // 80 GB at 100 Mbit/s is 6400 s.
    let deadline = e.now() + 8_000_000;
    let healed = run_checking(
        &mut e,
        deadline,
        |e| {
            let st = &e.client_view().state.volume_status["data"].status;
            abandoned |= st.replicas.iter().any(|x| x.host == mini && x.state == ReplicaState::Stale);
        },
        done,
    );
    assert!(!abandoned, "the slow copy was abandoned");
    assert!(healed, "{:?}", e.client_view().state.volume_status["data"]);
    assert!(e.client_view().state.volume_status["data"].status.replicas.iter().any(|x| x.host == mini && x.state == ReplicaState::Healthy));
}

#[test]
fn pods_on_a_lost_host_run_elsewhere() {
    let mut e = default_fleet(60);
    let dep = Deployment { id: "web".into(), image: "nginx".into(), replicas: 12, cpu_m: 500, ram_mb: 512, selector: Selector::default() };
    api_all(&mut e, vec![ApiOp::CreateDeployment { deployment: dep }]);
    let pods_running = |e: &Engine| {
        let s = &e.client_view().state;
        s.pods.len() == 12 && s.pods.values().all(|p| p.status.phase == PodPhase::Running)
    };
    let deadline = e.now() + 60_000;
    assert!(e.run_until(deadline, pods_running), "pods not running");
    let s = e.client_view().state.clone();
    let h = s.pods.values().filter_map(|p| p.status.host.clone()).find(|h| !s.cp.voters.contains(h)).expect("a pod host that is not a CP host");
    let on_h: Vec<String> = s.pods.values().filter(|p| p.status.host.as_ref() == Some(&h)).map(|p| PodStatus::key(&p.status.deployment, p.status.idx)).collect();
    let killed = e.now();
    e.fault(Fault::KillHost { host: h.clone() }).unwrap();
    let elsewhere = |e: &Engine| {
        let w = e.fabric().world();
        pods_running(e)
            && e.client_view().state.pods.values().all(|p| {
                let host = p.status.host.clone().unwrap();
                host != h && w.hosts[&host].instances.contains_key(&format!("pod/{}/{}", p.status.deployment, p.status.idx))
            })
    };
    assert!(e.run_until(killed + 60_000 + 40_000 + 15_000, elsewhere), "pods {on_h:?} of {h} not running elsewhere: {:?}", e.client_view().state.pods);
    assert!(e.now() >= killed + 60_000 + 35_000, "rescheduled before the grace period");
}

/// The VMs the control plane runs on `host`.
fn vms_on(e: &Engine, host: &HostId) -> Vec<String> {
    e.client_view().state.vm_status.iter().filter(|(_, v)| v.status.host.as_ref() == Some(host)).map(|(id, _)| id.clone()).collect()
}

/// The hosts that really run `vm`, not counting an incoming migration.
fn truly_running(e: &Engine, vm: &str) -> Vec<HostId> {
    let w = e.fabric().world();
    w.hosts.iter().filter(|(id, hw)| hw.powered && w.instances(id).contains(&format!("vm/{vm}"))).map(|(id, _)| id.clone()).collect()
}

/// The migration flows on the wire, with the rate each got at the latest tick.
fn migration_flows(e: &Engine) -> Vec<(sim::traffic::Flow, f64)> {
    let traffic = &e.fabric().world().traffic;
    traffic
        .flows()
        .filter(|f| f.kind == FlowKind::Migration)
        .map(|f| (f.clone(), traffic.last().rate.get(&f.id).copied().unwrap_or(0.0)))
        .collect()
}

#[test]
fn drain_empties_host() {
    let mut e = seeded(61);
    let s = e.client_view().state.clone();
    let h = s.vm_status.values().filter_map(|v| v.status.host.clone()).find(|h| !s.cp.voters.contains(h)).expect("a VM host that is not a CP host");
    let vms = vms_on(&e, &h);
    let volumes = volumes_on(&e, &h);
    assert!(!vms.is_empty() && !volumes.is_empty(), "{h}: vms {vms:?}, volumes {volumes:?}");
    assert_eq!(api(&mut e, ApiOp::Drain { host: h.clone() }), ok());
    let started = e.now();
    let draining = |e: &Engine| e.client_view().state.hosts[&h].lifecycle == Lifecycle::Draining;
    assert!(e.run_until(started + 2000, draining), "{:?}", e.client_view().state.hosts[&h].lifecycle);
    let drained = |e: &Engine| e.client_view().state.hosts[&h].lifecycle == Lifecycle::Drained;
    let mut stopped: Vec<String> = vec![];
    let mut down: Vec<String> = vec![];
    let ok = run_checking(
        &mut e,
        started + 600_000,
        |e| {
            let s = &e.client_view().state;
            stopped.extend(s.vm_status.iter().filter(|(_, v)| v.status.phase == VmPhase::Stopped).map(|(id, _)| id.clone()));
            // Live migration: each VM of the drained host keeps running somewhere, all along.
            down.extend(vms.iter().filter(|vm| truly_running(e, vm).is_empty()).cloned());
        },
        drained,
    );
    let hw = &e.fabric().world().hosts[&h];
    assert!(ok, "{h} not drained within 600 s: instances {:?}, replicas {:?}", hw.instances.keys().collect::<Vec<_>>(), hw.replicas.keys().collect::<Vec<_>>());
    assert!(stopped.is_empty(), "VMs went Stopped: {stopped:?}");
    assert!(down.is_empty(), "VMs ran nowhere during the drain: {down:?}");
    assert!(hw.instances.is_empty() && hw.replicas.is_empty(), "instances {:?}, replicas {:?}", hw.instances.keys().collect::<Vec<_>>(), hw.replicas.keys().collect::<Vec<_>>());
    for vm in &vms {
        assert!(running_elsewhere(&e, vm, &h), "{vm}: {:?}", e.client_view().state.vm_status[vm]);
        assert_eq!(truly_running(&e, vm).len(), 1, "{vm} runs once");
    }
    assert!(healthy_without(&e, &volumes, &h), "{:?}", volumes.iter().map(|v| &e.client_view().state.volume_status[v].status).collect::<Vec<_>>());
    assert!(e.client_view().state.pods.values().all(|p| p.status.host.as_ref() != Some(&h)));
    assert!(e.truth().split_brain_vms.is_empty());
}

#[test]
fn migration_traffic_on_migration_network() {
    let mut e = seeded(62);
    let (vm, from) = e.client_view().state.vm_status.iter().map(|(id, v)| (id.clone(), v.status.host.clone().unwrap())).next().unwrap();
    let ApiResult::Ok(serde_json::Value::String(to)) = api(&mut e, ApiOp::MigrateVm { vm: vm.clone(), to: None }) else { panic!("migration refused") };
    let to = HostId::from(to.as_str());
    assert_ne!(to, from);
    let migrating = |e: &Engine| e.client_view().state.vm_status[&vm].status.phase == VmPhase::Migrating { to: to.clone() };
    let deadline = e.now() + 2000;
    assert!(e.run_until(deadline, migrating), "{:?}", e.client_view().state.vm_status[&vm]);
    let deadline = e.now() + 10_000;
    assert!(e.run_until(deadline, |e| migration_flows(e).iter().any(|(_, rate)| *rate > 0.0)), "no migration traffic");
    let flows = migration_flows(&e);
    let [(flow, rate)] = flows.as_slice() else { panic!("{flows:?}") };
    assert_eq!((&flow.src, &flow.dst, flow.network.as_str()), (&FlowEnd::Host(from.clone()), &FlowEnd::Host(to.clone()), "migration"));
    assert!(*rate > 0.0);
    assert_eq!(truly_running(&e, &vm), [from.clone()], "the VM runs on its source until the copy completes");

    let moved = |e: &Engine| {
        let st = &e.client_view().state.vm_status[&vm].status;
        st.phase == VmPhase::Running && st.host.as_ref() == Some(&to)
    };
    let deadline = e.now() + 60_000;
    assert!(e.run_until(deadline, moved), "{:?}", e.client_view().state.vm_status[&vm]);
    assert_eq!(truly_running(&e, &vm), [to.clone()]);
    assert!(migration_flows(&e).is_empty(), "the migration flow ended");
    let events = &e.client_view().state.events;
    assert!(events.values().any(|ev| ev.entity == ObjectRef::vm(&vm) && ev.message.contains("migrated")), "no event for the migration");
}

#[test]
fn migration_fails_when_vlan30_blocked() {
    let mut e = seeded(63);
    let s = e.client_view().state.clone();
    let (vm, from) = s.vm_status.iter().map(|(id, v)| (id.clone(), v.status.host.clone().unwrap())).next().unwrap();
    // Another VM host, cut off from the migration network.
    let to = s.hosts.values().find(|h| h.id != from && h.roles.contains(&Role::Vm) && h.lifecycle == Lifecycle::Ready).unwrap().id.clone();
    block_vlan(&mut e, &to, 30);
    assert_eq!(api(&mut e, ApiOp::MigrateVm { vm: vm.clone(), to: Some(to.clone()) }), ApiResult::Ok(serde_json::Value::String(to.to_string())));
    let started = e.now();
    e.run_for(10_000);
    let flows = migration_flows(&e);
    assert!(!flows.is_empty() && flows.iter().all(|(f, rate)| f.network.as_str() == "migration" && *rate == 0.0), "blackholed: {flows:?}");

    let reverted = |e: &Engine| {
        let st = &e.client_view().state.vm_status[&vm].status;
        st.phase == VmPhase::Running && st.host.as_ref() == Some(&from)
    };
    let deadline = started + 135_000;
    let mut early = false;
    let ok = run_checking(&mut e, deadline, |e| early |= reverted(e) && e.now() < started + 119_000, reverted);
    assert!(ok, "not reverted within 135 s: {:?}", e.client_view().state.vm_status[&vm]);
    assert!(!early, "gave up before 120 s");
    e.run_for(5000);
    assert_eq!(truly_running(&e, &vm), [from.clone()]);
    assert!(!e.fabric().world().hosts[&to].instances.contains_key(&format!("vm/{vm}")), "the incoming instance on {to} is gone");
    assert!(migration_flows(&e).is_empty());
    let events = &e.client_view().state.events;
    assert!(events.values().any(|ev| ev.entity == ObjectRef::vm(&vm) && ev.message.contains("failed")), "no event for the failure");
    assert!(e.truth().split_brain_vms.is_empty());
}
