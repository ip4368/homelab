//! Workloads: the scheduler places pods and VMs, volumes get their
//! replicas, and the instances run on the simulated hosts with resource
//! usage and traffic. Tests read ground truth only to check the outcome.

use control::client::{ClientActor, CLIENT};
use control::controllers::scheduler::INSUFFICIENT_STORAGE_HOSTS;
use control::proto::{ApiOp, ApiResult, LogCmd};
use control::raft::EntryPayload;
use control::replica::CpReplica;
use control::runtime::{Addr, Endpoint, Svc};
use control::state::{Command, ObjectKind, ObjectRef};
use model::{
    Deployment, HostId, NetworkId, PodPhase, ReplicaState, Role, Selector, VmPhase, Volume, VolumeHealth,
    TRANSCODE_CAPABILITY_PREFIX,
};
use sim::engine::Engine;
use sim::presets::{FleetSpec, Preset};
use sim::scenario::{Scenario, TRANSCODE_DEPLOYMENT};
use sim::traffic::{FlowEnd, FlowKind};
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
    // Replicas fill their disks.
    let r = s.volume_status.values().next().unwrap().status.replicas[0].clone();
    assert!(m[&r.host].disk_pct[&r.disk] > 0.0);
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
