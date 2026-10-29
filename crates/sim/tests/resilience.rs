//! Control-plane resilience over the simulated fabric. Tests may read ground
//! truth (the fabric, replica internals); control code never does.

mod common;

use common::*;
use control::agent::HostAgent;
use control::proto::{ApiOp, ApiResult, LogCmd};
use control::raft::{EntryPayload, RaftRole};
use control::replica::CpReplica;
use control::state::{Alert, Command, CpMode, Lifecycle, ObjectRef, Severity};
use model::{Deployment, HostId, ProfileId, Role, Selector, SwitchId, Volume};
use sim::engine::Engine;
use sim::fabric::Fabric;
use sim::faults::Fault;
use sim::presets::Preset;
use sim::scenario::Scenario;
use sim::topology::PartitionMember;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn three_replicas_elect_and_claim_vip() {
    let mut e = engine(1);
    let leader = elect(&mut e, 6000);
    for h in HOSTS {
        assert_eq!(e.fabric().resolve_vip(&hid(h)), Some(leader.clone()), "VIP as seen from {h}");
    }
}

#[test]
fn client_sync_sees_leader() {
    let mut e = engine(2);
    let leader = elect(&mut e, 6000);
    let deadline = e.now() + 2000;
    assert!(
        e.run_until(deadline, |e| e.client_view().leader.as_ref() == Some(&leader)),
        "client view leader {:?}, true leader {leader}",
        e.client_view().leader
    );
    // The seeded hosts arrive with the leader's state.
    let view = e.client_view();
    assert_eq!(view.state.hosts.len(), 3);
    assert!(view.state.hosts.values().all(|h| h.lifecycle == Lifecycle::Ready));
    assert!(view.last_sync_at.is_some());
}

#[test]
fn minority_leader_stops_controllers() {
    let mut e = engine(3);
    let leader = elect(&mut e, 6000);
    e.run_for(1000); // let the leader's no-op commit and settle
    e.deliver_mut::<Fabric>().unwrap().world_mut().topology.partition([PartitionMember::Host(leader.clone())].into());
    let len = e.replica_log_len(&leader).unwrap();
    e.run_for(2000);
    assert_eq!(e.replica_log_len(&leader), Some(len), "the isolated leader appended entries");
    assert!(!leaders(&e).contains(&leader), "the isolated leader should have stepped down");

    // The majority side carries on: a new leader whose log grew (its own no-op).
    let deadline = e.now() + 6000;
    assert!(e.run_until(deadline, |e| leaders(e).len() == 1), "majority elects a new leader");
    let new_leader = leaders(&e).pop().unwrap();
    assert_ne!(new_leader, leader);
    assert!(e.replica_log_len(&new_leader).unwrap() > len);
}

#[test]
fn api_write_is_answered_after_commit_and_reaches_the_view() {
    let mut e = engine(4);
    elect(&mut e, 6000);
    let req = e.api(ApiOp::Cordon { id: hid("h2") });
    e.run_for(2000);
    assert_eq!(e.take_api_results(), [(req, ApiResult::Ok(serde_json::Value::Null))]);
    assert_eq!(e.client_view().state.hosts[&hid("h2")].lifecycle, Lifecycle::Cordoned);

    let req = e.api(ApiOp::CreateJoinToken);
    e.run_for(2000);
    let results = e.take_api_results();
    let [(r, ApiResult::Ok(serde_json::Value::String(token)))] = results.as_slice() else {
        panic!("expected one token result, got {results:?}")
    };
    assert_eq!(*r, req);
    assert!(e.client_view().state.tokens.contains_key(token));

    // A failing command comes back as an error.
    let req = e.api(ApiOp::Uncordon { id: hid("nope") });
    e.run_for(2000);
    assert!(matches!(e.take_api_results().as_slice(), [(r, ApiResult::Err(_))] if *r == req));
}

#[test]
fn heartbeat_loss_marks_not_ready_and_recovery_restores() {
    let mut e = engine(5);
    let leader = elect(&mut e, 6000);
    let victim = HOSTS.iter().map(|h| hid(h)).find(|h| *h != leader).unwrap();
    // Cordon first, so recovery must restore Cordoned rather than Ready (R14).
    e.api(ApiOp::Cordon { id: victim.clone() });
    e.run_for(1000);
    e.kill(&agent(&victim));
    let host_down = |e: &Engine| {
        let v = e.client_view();
        v.state.hosts[&victim].lifecycle == Lifecycle::NotReady
            && v.state.alerts.values().any(|a| {
                a.kind == "HostDown" && a.entity == ObjectRef::host(&victim) && a.resolved_at.is_none()
            })
    };
    // Not before 40 s of silence...
    e.run_for(35_000);
    assert!(!host_down(&e), "marked down too early");
    // ...but within 40 s + one controller period + sync.
    let deadline = e.now() + 12_000;
    assert!(e.run_until(deadline, host_down), "host never marked NotReady with an open HostDown alert");
    let open: Vec<_> = e.client_view().state.alerts.values().filter(|a| a.kind == "HostDown").cloned().collect();
    assert_eq!(open.len(), 1, "exactly one alert per incident: {open:?}");

    e.spawn(agent(&victim), Box::new(HeartbeatStub { host: victim.clone() }));
    let deadline = e.now() + 8000;
    assert!(
        e.run_until(deadline, |e| {
            let v = e.client_view();
            v.state.hosts[&victim].lifecycle == Lifecycle::Cordoned
                && v.state.alerts.values().all(|a| a.resolved_at.is_some())
        }),
        "recovery should restore Cordoned and resolve the alert"
    );
    // The other hosts kept heartbeating and were never touched.
    for h in HOSTS.iter().map(|h| hid(h)).filter(|h| *h != victim) {
        assert_eq!(e.client_view().state.hosts[&h].lifecycle, Lifecycle::Ready);
    }
}

fn lifecycle(e: &Engine, h: &HostId) -> Option<Lifecycle> {
    e.client_view().state.hosts.get(h).map(|o| o.lifecycle)
}

#[test]
fn join_pending_then_approve() {
    let mut e = engine(6);
    elect(&mut e, 6000);
    let token = join_token(&mut e);
    let m1 = boot_host(&mut e, "m1", ProfileId::MiniPc, 0, Some(token));
    let deadline = e.now() + 10_000;
    assert!(e.run_until(deadline, |e| lifecycle(e, &m1) == Some(Lifecycle::Pending)), "m1 never showed up Pending");
    let host = &e.client_view().state.hosts[&m1];
    assert_eq!(host.inventory.profile, ProfileId::MiniPc);
    assert!(host.inventory.nics.iter().any(|n| n.name == "eth0" && n.link_up), "{:?}", host.inventory.nics);
    assert_eq!(host.location, "unknown", "the join path knows no location yet");

    // No rule matches a mini PC here: it waits for a human.
    e.run_for(10_000);
    assert_eq!(lifecycle(&e, &m1), Some(Lifecycle::Pending));

    let roles: BTreeSet<Role> = [Role::Compute, Role::Storage].into();
    e.api(ApiOp::ApproveHost { id: m1.clone(), roles: roles.clone() });
    let deadline = e.now() + 15_000;
    assert!(e.run_until(deadline, |e| lifecycle(e, &m1) == Some(Lifecycle::Ready)), "approved m1 never became Ready");
    assert_eq!(e.client_view().state.hosts[&m1].roles, roles);
    let desired = e.client_view().state.hosts[&m1].net.desired_generation;
    assert_eq!(e.actor::<HostAgent>(&agent(&m1)).unwrap().net_generation(), desired, "the agent runs the config it was told");

    // Joined agents report metrics, which reach the client through the leader.
    let deadline = e.now() + 12_000;
    assert!(e.run_until(deadline, |e| e.client_view().metrics.contains_key(&m1)), "no metrics from m1");
    let sample = &e.client_view().metrics[&m1];
    assert!((3.0..10.0).contains(&sample.cpu_pct), "{sample:?}");
    assert!(sample.disk_pct.contains_key("nvme0"), "{sample:?}");
}

#[test]
fn invalid_token_rejected() {
    let mut e = engine(7);
    elect(&mut e, 6000);
    let bad = boot_host(&mut e, "bad", ProfileId::MiniPc, 0, Some("not-a-token".into()));
    e.run_for(20_000);
    let view = e.client_view();
    assert!(!view.state.hosts.contains_key(&bad), "a host joined with an unknown token");
    let rejected: Vec<_> = view.state.events.values().filter(|ev| ev.message.contains("join rejected")).collect();
    // The agent retried every 5 s; rejections are logged at most once a minute per host.
    assert_eq!(rejected.len(), 1, "{rejected:?}");
    assert_eq!(rejected[0].entity, ObjectRef::host(&bad));
}

#[test]
fn auto_approve_rk3588() {
    let mut e = engine(8);
    elect(&mut e, 6000);
    let roles: BTreeSet<Role> = [Role::Transcode, Role::Compute].into();
    api_ok(&mut e, ApiOp::UpsertRule { id: "sbc".into(), profile: ProfileId::Rk3588, roles: roles.clone(), template: None });
    let token = join_token(&mut e);
    let rk = boot_host(&mut e, "rk1", ProfileId::Rk3588, 0, Some(token));
    // No ApproveHost from here on: the rule does it.
    let deadline = e.now() + 20_000;
    assert!(e.run_until(deadline, |e| lifecycle(e, &rk) == Some(Lifecycle::Ready)), "rk1 never became Ready");
    assert_eq!(e.client_view().state.hosts[&rk].roles, roles);
}

#[test]
fn auto_approve_drops_ineligible_roles_with_an_event() {
    let mut e = engine(9);
    elect(&mut e, 6000);
    let roles: BTreeSet<Role> = [Role::Storage, Role::Transcode].into();
    api_ok(&mut e, ApiOp::UpsertRule { id: "sbc".into(), profile: ProfileId::Rk3588, roles, template: None });
    let token = join_token(&mut e);
    // Variant 0 has only eMMC, so it cannot store.
    let rk = boot_host(&mut e, "rk1", ProfileId::Rk3588, 0, Some(token));
    let deadline = e.now() + 20_000;
    assert!(e.run_until(deadline, |e| lifecycle(e, &rk) == Some(Lifecycle::Ready)), "rk1 never became Ready");
    let view = e.client_view();
    assert_eq!(view.state.hosts[&rk].roles, [Role::Transcode].into());
    let dropped: Vec<_> = view.state.events.values().filter(|ev| ev.message.contains("Storage")).collect();
    assert_eq!(dropped.len(), 1, "one event per dropped role: {dropped:?}");
    assert_eq!(dropped[0].entity, ObjectRef::host(&rk));
}

#[test]
fn replica_state_is_stored_on_its_host_and_reloaded() {
    let mut e = engine(10);
    let leader = elect(&mut e, 6000);
    e.api(ApiOp::Cordon { id: leader.clone() });
    e.run_for(2000);
    for h in HOSTS.map(hid) {
        let live = e.actor::<CpReplica>(&cp(&h)).unwrap().durable().clone();
        assert_eq!(e.fabric().world().hosts[&h].cp_durable.as_ref(), Some(&live), "{h}'s disk lags its replica");
    }
    // A replica started with nothing picks up what its host stored.
    let follower = HOSTS.map(hid).into_iter().find(|h| *h != leader).unwrap();
    let len = e.replica_log_len(&follower).unwrap();
    e.kill(&cp(&follower));
    e.spawn(cp(&follower), Box::new(CpReplica::new(follower.clone(), None, BTreeSet::new())));
    assert_eq!(e.replica_log_len(&follower), Some(len));
}

// ---- the default preset (60 hosts) ----

fn default_fleet(seed: u64) -> Engine {
    let mut e = Engine::new(seed);
    e.load_preset(Preset::Default);
    e
}

fn ready_count(e: &Engine) -> usize {
    e.client_view().state.hosts.values().filter(|h| h.lifecycle == Lifecycle::Ready).count()
}

fn replica<'e>(e: &'e Engine, h: &HostId) -> &'e CpReplica {
    e.actor::<CpReplica>(&cp(h)).unwrap_or_else(|| panic!("{h} runs no replica"))
}

/// Runs until the client sees all 60 hosts Ready under the true leader; returns it.
fn settle(e: &mut Engine) -> HostId {
    let deadline = e.now() + 30_000;
    let settled = |e: &Engine| e.true_leader().is_some() && e.client_view().leader == e.true_leader() && ready_count(e) == 60;
    assert!(e.run_until(deadline, settled), "fleet did not settle by t={deadline}: {} Ready", ready_count(e));
    e.true_leader().unwrap()
}

#[test]
fn default_preset_all_ready() {
    let mut e = default_fleet(11);
    assert_eq!(e.fabric().world().hosts.len(), 60);
    assert!(e.run_until(30_000, |e| ready_count(e) == 60), "only {} Ready after 30 s", ready_count(&e));
    let view = e.client_view();
    assert_eq!(view.state.hosts.len(), 60);
    assert_eq!(view.state.cp.voters.len(), 3, "R6: the default fleet starts with 3 voters");
    assert!(view.state.cp.voters.iter().all(|v| v.as_str().starts_with("ms01-")));
}

#[test]
fn every_ready_host_runs_kubernetes_with_servers_on_the_voters() {
    use control::proto::{KubePhase, KubeRole};
    let mut e = default_fleet(12);
    settle(&mut e);
    let converged = |e: &Engine| {
        let s = &e.client_view().state;
        s.cp.voters.len() == 5
            && s.hosts.values().all(|h| {
                let server = h.roles.contains(&Role::ControlPlane);
                h.kube.as_ref().is_some_and(|k| k.phase == KubePhase::Running && k.role == Some(if server { KubeRole::Server } else { KubeRole::Agent }))
            })
    };
    let deadline = e.now() + 5 * 60_000;
    assert!(e.run_until(deadline, converged), "kubernetes did not converge: {:?}", e.client_view().state.hosts.values().filter(|h| h.kube.is_none()).map(|h| h.id.clone()).collect::<Vec<_>>());
    let s = &e.client_view().state;
    let servers: BTreeSet<HostId> = s.hosts.values().filter(|h| h.kube.as_ref().unwrap().role == Some(KubeRole::Server)).map(|h| h.id.clone()).collect();
    assert_eq!(servers, s.cp.voters, "the K3s servers are the CP voters");
    let kube = &e.fabric().world().kube;
    assert_eq!(kube.etcd, s.cp.voters, "every server is an etcd member");
    assert!(kube.violations.is_empty(), "{:?}", kube.violations);
    let pi = &s.hosts[&hid("pi5-01")];
    assert_eq!(pi.kube.as_ref().unwrap().labels.get("homelab.io/transcode").map(String::as_str), Some("true"));

    // The leader hands out its K3s server's kubeconfig, so a client needs no access to a host.
    let leader = e.true_leader().expect("a settled leader");
    let req = e.api(ApiOp::GetKubeconfig);
    e.run_for(1000);
    let Some(ApiResult::Ok(serde_json::Value::String(k))) = e.take_api_result(req) else { panic!("GetKubeconfig gave no kubeconfig") };
    assert!(k.contains(&format!("server on {leader}")), "{k}");
}

/// Kills the settled leader at the end of a client write, so its log may
/// hold an entry it has not committed. Returns (leader, kill time, its log
/// and commit index at the kill).
fn kill_leader(e: &mut Engine) -> (HostId, u64, Vec<control::raft::Entry<LogCmd>>, u64) {
    let l = settle(e);
    e.run_for(1000);
    let len = e.replica_log_len(&l).unwrap();
    e.api(ApiOp::Cordon { id: hid("pi5-01") });
    let deadline = e.now() + 1000;
    assert!(e.run_until(deadline, |e| e.replica_log_len(&l).unwrap() > len), "the leader never appended the write");
    let (log, commit) = (replica(e, &l).durable().log.clone(), replica(e, &l).commit_index());
    let kill_at = e.now();
    e.fault(Fault::KillHost { host: l.clone() }).unwrap();
    (l, kill_at, log, commit)
}

#[test]
fn leader_failover_criterion_3() {
    let mut e = default_fleet(12);
    let (l, kill_at, log_at_kill, commit_at_kill) = kill_leader(&mut e);
    assert!(e.actor::<CpReplica>(&cp(&l)).is_none() && e.actor::<HostAgent>(&agent(&l)).is_none());

    assert!(e.run_until(kill_at + 6000, |e| e.true_leader().is_some_and(|n| n != l)), "no new leader within 6 s");
    let new = e.true_leader().unwrap();
    // The moment the new leader's no-op applied: before it, neither its controllers nor the API may write.
    assert!(e.run_until(kill_at + 6000, |e| replica(e, &new).caught_up()), "the new leader never caught up");
    let caught_up_at = e.now();
    let deadline = e.now() + 2000;
    assert!(
        e.run_until(deadline, |e| e.client_view().leader.as_ref() == Some(&new)),
        "client view shows leader {:?}, true leader {new}",
        e.client_view().leader
    );

    e.run_for(2000);
    let stored = e.fabric().world().hosts[&l].cp_durable.clone().unwrap();
    assert_eq!(stored.log, log_at_kill, "a powered-off host's log changed after the kill");
    let r = replica(&e, &new);
    let (new_log, commit) = (&r.durable().log, r.commit_index() as usize);
    assert!(commit >= commit_at_kill as usize);
    assert_eq!(new_log[..commit_at_kill as usize], log_at_kill[..commit_at_kill as usize], "a committed entry was lost");
    // No entry was proposed in the gap: between the kill and the new leader catching up, no
    // controller acted. The gap is short here; `a_new_leader_that_cannot_catch_up_neither_writes_nor_answers`
    // holds it open to pin down the gates themselves.
    for entry in &new_log[..commit] {
        if let EntryPayload::Normal(LogCmd { at, ref cmd }) = entry.payload {
            assert!(at <= kill_at || at >= caught_up_at, "entry {} ({cmd:?}) was proposed at {at}, between the kill at {kill_at} and the new leader catching up at {caught_up_at}", entry.index);
        }
    }
    assert!(new_log[..commit].iter().any(|en| en.term == r.durable().term && matches!(en.payload, EntryPayload::Noop)), "the new leader's no-op committed");
}

/// Criterion 3's gates in the running system. The window between a new
/// leader's election and its no-op applying is normally one round trip, too
/// short for a controller run or a client request to land in; here it is held
/// open. The leader is killed as the followers learn a new deployment
/// committed, so the new leader's state holds pods to place. The only other
/// voter is cut off as the new leader wins, so its ack of the no-op never
/// arrives, while the new leader keeps quorum for a fresh leader's
/// check-quorum window. Until it catches up it must not write (controllers or
/// API), answer a write, or answer a sync.
#[test]
fn a_new_leader_that_cannot_catch_up_neither_writes_nor_answers() {
    let mut e = default_fleet(14);
    let l = settle(&mut e);
    e.run_for(1000);
    let followers: Vec<HostId> = replica(&e, &l).voters().iter().filter(|v| **v != l).cloned().collect();
    let dep = Deployment { id: "web".into(), image: "nginx".into(), replicas: 3, cpu_m: 100, ram_mb: 128, selector: Selector::default() };
    e.api(ApiOp::CreateDeployment { deployment: dep });
    let created = |e: &Engine| {
        let log = &replica(e, &l).durable().log;
        log.iter().find(|en| matches!(&en.payload, EntryPayload::Normal(LogCmd { cmd: Command::CreateDeployment { .. }, .. }))).map(|en| en.index)
    };
    assert!(e.run_until(e.now() + 1000, |e| created(e).is_some()), "the leader never appended the deployment");
    let idx = created(&e).unwrap();
    let applied = |e: &Engine| followers.iter().all(|f| replica(e, f).commit_index() >= idx);
    assert!(e.run_until(e.now() + 1000, applied), "the followers never learned the deployment committed");
    e.fault(Fault::KillHost { host: l.clone() }).unwrap();
    let kill_at = e.now();

    assert!(e.run_until(kill_at + 6000, |e| e.true_leader().is_some_and(|n| n != l)), "no new leader within 6 s");
    let new = e.true_leader().unwrap();
    let other = followers.iter().find(|f| **f != new).unwrap().clone();
    e.fault(Fault::Partition { nodes: vec![PartitionMember::Host(other)] }).unwrap();
    let (elected_at, term, len) = (e.now(), replica(&e, &new).durable().term, replica(&e, &new).log_len());
    assert!(!replica(&e, &new).caught_up());
    let r = replica(&e, &new);
    let placed = r.durable().log[..r.commit_index() as usize].iter().any(|en| matches!(&en.payload, EntryPayload::Normal(LogCmd { cmd: Command::SetPodStatus { .. }, .. })));
    assert!(!placed, "precondition: the new leader's state holds unplaced pods");
    let req = e.api(ApiOp::Cordon { id: hid("pi5-01") });

    let stuck = |e: &Engine| replica(e, &new).is_leader() && replica(e, &new).durable().term == term;
    let mut answers = vec![];
    while stuck(&e) && e.now() < elected_at + 5000 {
        e.run_for(50);
        assert!(!replica(&e, &new).caught_up() || !stuck(&e), "the no-op applied without the other voter");
        assert_eq!(replica(&e, &new).log_len(), len, "the new leader wrote before catching up");
        assert_ne!(e.client_view().leader.as_ref(), Some(&new), "a sync was answered before catching up");
        answers.extend(e.take_api_results());
    }
    let led_for = e.now() - elected_at;
    assert!(led_for > control::replica::CONTROLLER_PERIOD_MS, "the window held for only {led_for} ms: no controller run fell in it");
    assert!(answers.iter().all(|(r, a)| *r != req || *a == ApiResult::Unavailable), "{answers:?}");

    e.fault(Fault::HealPartitions).unwrap();
    let deadline = e.now() + 10_000;
    assert!(e.run_until(deadline, |e| e.true_leader().is_some_and(|n| replica(e, &n).caught_up())), "no leader caught up after healing");
}

#[test]
fn restore_host_rejoins_as_follower() {
    let mut e = default_fleet(13);
    let (l, kill_at, ..) = kill_leader(&mut e);
    assert!(e.run_until(kill_at + 6000, |e| e.true_leader().is_some_and(|n| n != l)), "no new leader");
    let new = e.true_leader().unwrap();
    e.run_for(3000);
    let stored = e.fabric().world().hosts[&l].cp_durable.clone().unwrap();

    e.fault(Fault::RestoreHost { host: l.clone() }).unwrap();
    assert!(e.fabric().world().hosts[&l].powered);
    assert!(e.actor::<HostAgent>(&agent(&l)).is_some(), "the agent is back");
    let back = replica(&e, &l).durable();
    assert_eq!((back.term, &back.voted_for, &back.log), (stored.term, &stored.voted_for, &stored.log), "resumed from disk");

    let target = replica(&e, &new).commit_index();
    let deadline = e.now() + 5000;
    assert!(
        e.run_until(deadline, |e| replica(e, &l).commit_index() >= target),
        "restored replica at commit {} never reached the leader's {target}",
        replica(&e, &l).commit_index()
    );
    assert_eq!(replica(&e, &l).raft_role(), Some(RaftRole::Follower));
    assert_eq!(e.true_leader(), Some(new), "the rejoining host must not depose the leader");
}

#[test]
fn respawned_replica_prefers_its_disk_over_the_seed() {
    let mut e = default_fleet(14);
    let seeded = e.fabric().world().hosts[&hid("ms01-01")].cp_durable.clone().expect("voters store the seed at start");
    let (l, kill_at, ..) = kill_leader(&mut e);
    assert!(e.run_until(kill_at + 6000, |e| e.true_leader().is_some_and(|n| n != l)), "no new leader");
    let stored = e.fabric().world().hosts[&l].cp_durable.clone().unwrap();
    assert!(stored.term > seeded.term && stored.log.len() > seeded.log.len(), "the leader moved past the seed");

    e.fabric_mut().host_mut(&l).unwrap().powered = true;
    e.spawn(cp(&l), Box::new(CpReplica::new(l.clone(), Some(seeded.clone()), seeded.voters.clone())));
    let d = replica(&e, &l).durable();
    assert_eq!((d.term, &d.voted_for, &d.log), (stored.term, &stored.voted_for, &stored.log));
}

#[test]
fn kill_host_marks_not_ready() {
    let mut e = default_fleet(15);
    settle(&mut e);
    let victim = hid("pi5-03");
    assert!(!e.client_view().state.cp.voters.contains(&victim));
    e.fault(Fault::KillHost { host: victim.clone() }).unwrap();
    let down = |e: &Engine| {
        let v = e.client_view();
        v.state.hosts[&victim].lifecycle == Lifecycle::NotReady
            && v.state.alerts.values().any(|a| a.kind == "HostDown" && a.entity == ObjectRef::host(&victim) && a.resolved_at.is_none())
    };
    let deadline = e.now() + 45_000;
    assert!(e.run_until(deadline, down), "{victim} not NotReady with a HostDown alert within 45 s");
}

#[test]
fn add_hosts_join_through_the_seed_rules() {
    let mut e = default_fleet(16);
    settle(&mut e);
    e.scenario(Scenario::AddHosts { profile: ProfileId::Rk3588, count: 2, misconfigure: None }).unwrap();
    let added = [hid("rk3588-13"), hid("rk3588-14")];
    let deadline = e.now() + 30_000;
    assert!(
        e.run_until(deadline, |e| added.iter().all(|h| lifecycle(e, h) == Some(Lifecycle::Ready))),
        "added hosts: {:?}",
        added.iter().map(|h| lifecycle(&e, h)).collect::<Vec<_>>()
    );
    for h in &added {
        let obj = &e.client_view().state.hosts[h];
        assert_eq!(obj.roles, [Role::Transcode, Role::Compute].into());
    }
    // The join path knows no location; the agents' LLDP says they hang off access-2.
    let located = |e: &Engine| added.iter().all(|h| e.client_view().state.hosts[h].location == "rack-2");
    assert!(e.run_until(e.now() + 15_000, located), "locations: {:?}", added.map(|h| e.client_view().state.hosts[&h].location.clone()));
}

#[test]
fn restored_scenario_host_boots_as_a_member() {
    let mut e = default_fleet(18);
    settle(&mut e);
    e.scenario(Scenario::AddHosts { profile: ProfileId::Pi5, count: 1, misconfigure: None }).unwrap();
    let h = hid("pi5-13");
    let deadline = e.now() + 30_000;
    assert!(e.run_until(deadline, |e| lifecycle(e, &h) == Some(Lifecycle::Ready)), "pi5-13 never became Ready");
    assert_eq!(e.fabric().world().hosts[&h].join_token, None, "joining is recorded on the host");

    e.fault(Fault::KillHost { host: h.clone() }).unwrap();
    let deadline = e.now() + 45_000;
    assert!(e.run_until(deadline, |e| lifecycle(e, &h) == Some(Lifecycle::NotReady)), "never marked NotReady");
    e.fault(Fault::RestoreHost { host: h.clone() }).unwrap();
    assert!(e.actor::<HostAgent>(&agent(&h)).unwrap().is_member(), "booted without a token");
    let deadline = e.now() + 15_000;
    assert!(e.run_until(deadline, |e| lifecycle(e, &h) == Some(Lifecycle::Ready)), "pi5-13 never recovered");
    // The restarted agent forgot its network generation; the leader pushes it again.
    let desired = e.client_view().state.hosts[&h].net.desired_generation;
    let converged = |e: &Engine| e.actor::<HostAgent>(&agent(&h)).unwrap().net_generation() == desired;
    assert!(e.run_until(e.now() + 15_000, converged), "the restarted agent was not told its config again");
}

#[test]
fn add_hosts_fails_without_a_control_plane() {
    let mut e = default_fleet(17);
    settle(&mut e);
    for v in e.client_view().state.cp.voters.clone() {
        e.fault(Fault::KillHost { host: v }).unwrap();
    }
    let start = e.now();
    let err = e.scenario(Scenario::AddHosts { profile: ProfileId::Pi5, count: 1, misconfigure: None }).unwrap_err();
    assert!(err.contains("join token"), "{err}");
    assert!(e.now() <= start + 5000, "waited past 5 s");
    assert!(!e.fabric().world().hosts.contains_key(&hid("pi5-13")), "no host is added without a token");
}

// ---- control-plane membership ----

/// Runs up to `ms` until the client sees exactly `n` CP voters.
fn voters_reach(e: &mut Engine, n: usize, ms: u64) -> bool {
    let deadline = e.now() + ms;
    e.run_until(deadline, |e| e.client_view().state.cp.voters.len() == n)
}

/// The Raft voters as the true leader holds them.
fn raft_voters(e: &Engine) -> BTreeSet<HostId> {
    let l = e.true_leader().expect("a leader");
    replica(e, &l).voters().clone()
}

/// Kills a voter that is not the leader; returns it and when it died.
fn kill_follower_voter(e: &mut Engine) -> (HostId, u64) {
    let leader = settle(e);
    let dead = e.client_view().state.cp.voters.iter().find(|v| **v != leader).unwrap().clone();
    e.fault(Fault::KillHost { host: dead.clone() }).unwrap();
    (dead, e.now())
}

#[test]
fn default_fleet_grows_to_5() {
    let mut e = default_fleet(21);
    e.run_for(55_000);
    assert_eq!(raft_voters(&e).len(), 3, "R6: no growth while the fleet settles");
    assert!(voters_reach(&mut e, 5, 65_000), "voters after 120 s: {:?}", e.client_view().state.cp.voters);
    let voters = e.client_view().state.cp.voters.clone();
    assert_eq!(raft_voters(&e), voters, "the view shows the Raft membership");
    assert!(replica(&e, &e.true_leader().unwrap()).learners().is_empty());
    for v in &voters {
        assert!(v.as_str().starts_with("ms01-"), "MS-01s are preferred: {voters:?}");
        assert!(replica(&e, v).voters().contains(v), "{v} runs a replica that knows it votes");
    }
    let has_role = |e: &Engine| {
        let hosts = &e.client_view().state.hosts;
        hosts.values().filter(|h| h.roles.contains(&Role::ControlPlane)).map(|h| h.id.clone()).collect::<BTreeSet<_>>()
    };
    assert!(e.run_until(e.now() + 5000, |e| has_role(e) == voters), "control-plane role on {:?}", has_role(&e));
}

#[test]
fn auto_replace_dead_member() {
    let mut e = default_fleet(22);
    settle(&mut e);
    let req = e.api(ApiOp::SetCpMode { mode: CpMode::Auto });
    e.run_for(2000);
    assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    assert_eq!(e.client_view().state.cp.mode, CpMode::Auto);
    let (dead, killed_at) = kill_follower_voter(&mut e);

    e.run_until(killed_at + 10 * 60_000 + 60_000, |_| false);
    let voters = e.client_view().state.cp.voters.clone();
    assert_eq!(voters.len(), 5, "{voters:?}");
    assert!(!voters.contains(&dead), "the dead member was replaced: {voters:?}");
    assert_eq!(raft_voters(&e), voters);
    assert!(!e.client_view().state.hosts[&dead].roles.contains(&Role::ControlPlane), "the dead host lost the role");
}

/// Each host's Kubernetes as the client sees it: (role, phase).
fn kube_of(e: &Engine, h: &HostId) -> Option<(Option<control::proto::KubeRole>, control::proto::KubePhase)> {
    e.client_view().state.hosts.get(h)?.kube.as_ref().map(|k| (k.role, k.phase.clone()))
}

#[test]
fn replacing_a_dead_k3s_server_removes_its_etcd_member_then_adds_the_new_one() {
    use control::proto::{KubePhase, KubeRole};
    use sim::world::EtcdChange;
    let mut e = default_fleet(24);
    settle(&mut e);
    let etcd_is_voters = |e: &Engine| {
        let voters = &e.client_view().state.cp.voters;
        voters.len() == 5 && e.fabric().world().kube.etcd == *voters && voters.iter().all(|v| kube_of(e, v) == Some((Some(KubeRole::Server), KubePhase::Running)))
    };
    assert!(e.run_until(e.now() + 5 * 60_000, etcd_is_voters), "etcd {:?}", e.fabric().world().kube.etcd);
    let (dead, _) = kill_follower_voter(&mut e);
    assert!(e.run_until(e.now() + 60_000, |e| lifecycle(e, &dead) == Some(Lifecycle::NotReady)), "{dead} never went NotReady");
    assert_eq!(e.fabric().world().kube.etcd.len(), 5, "a dead voter keeps its member until it is replaced");

    // Take the replacement at once, rather than 10 min on: an MS-01 that runs a K3s agent.
    let s = &e.client_view().state;
    let ready_ms01 = |h: &&control::state::HostObj| h.lifecycle == Lifecycle::Ready && h.profile.id == ProfileId::Ms01;
    let with = s.hosts.values().filter(ready_ms01).find(|h| !s.cp.voters.contains(&h.id)).expect("a spare MS-01").id.clone();
    assert_eq!(kube_of(&e, &with), Some((Some(KubeRole::Agent), KubePhase::Running)));
    let req = e.api(ApiOp::ReplaceCpMember { dead: dead.clone(), with: with.clone() });
    e.run_for(2000);
    assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    let replaced = |e: &Engine| {
        let voters = &e.client_view().state.cp.voters;
        voters.contains(&with) && !voters.contains(&dead) && etcd_is_voters(e)
    };
    assert!(e.run_until(e.now() + 3 * 60_000, replaced), "etcd {:?}, {with}: {:?}", e.fabric().world().kube.etcd, kube_of(&e, &with));
    let kube = &e.fabric().world().kube;
    assert!(!e.fabric().world().hosts[&dead].powered, "etcd lists exactly the live servers while {dead} is still off");
    let at = |c: EtcdChange| kube.etcd_log.iter().position(|l| *l == c);
    let (removed, joined) = (at(EtcdChange::Removed(dead.clone())), at(EtcdChange::Joined(with.clone())));
    assert!(removed.is_some() && removed < joined, "remove the dead member, then add the new one: {:?}", kube.etcd_log);
    assert!(kube.violations.is_empty(), "{:?}", kube.violations);

    // The dead host comes back: it reinstalls as an agent; etcd keeps the voters.
    e.fault(Fault::RestoreHost { host: dead.clone() }).unwrap();
    let agent = |e: &Engine| lifecycle(e, &dead) == Some(Lifecycle::Ready) && kube_of(e, &dead) == Some((Some(KubeRole::Agent), KubePhase::Running));
    assert!(e.run_until(e.now() + 2 * 60_000, agent), "{dead}: {:?} {:?}", lifecycle(&e, &dead), kube_of(&e, &dead));
    assert!(etcd_is_voters(&e), "etcd {:?}", e.fabric().world().kube.etcd);
    let kube = &e.fabric().world().kube;
    assert!(kube.violations.is_empty(), "{:?}", kube.violations);
}

#[test]
fn the_leader_hands_its_control_plane_place_to_a_host_the_user_picks() {
    use control::proto::{KubePhase, KubeRole};
    use sim::world::EtcdChange;
    let mut e = default_fleet(26);
    settle(&mut e);
    let etcd_is_voters = |e: &Engine| {
        let voters = &e.client_view().state.cp.voters;
        voters.len() == 5 && e.fabric().world().kube.etcd == *voters && voters.iter().all(|v| kube_of(e, v) == Some((Some(KubeRole::Server), KubePhase::Running)))
    };
    assert!(e.run_until(e.now() + 5 * 60_000, etcd_is_voters), "etcd {:?}", e.fabric().world().kube.etcd);
    let from = e.true_leader().expect("a leader");
    let s = &e.client_view().state;
    let ready_ms01 = |h: &&control::state::HostObj| h.lifecycle == Lifecycle::Ready && h.profile.id == ProfileId::Ms01;
    let to = s.hosts.values().filter(ready_ms01).find(|h| !s.cp.voters.contains(&h.id)).expect("a spare MS-01").id.clone();
    assert_eq!(kube_of(&e, &to), Some((Some(KubeRole::Agent), KubePhase::Running)));

    // The role is not set by hand: that would flip the host between a K3s server and an agent.
    let mut roles = s.hosts[&to].roles.clone();
    roles.insert(Role::ControlPlane);
    let req = e.api(ApiOp::SetRoles { id: to.clone(), roles });
    e.run_for(2000);
    assert!(matches!(e.take_api_result(req), Some(ApiResult::Err(m)) if m.contains("hand a voter's place to it")));

    let req = e.api(ApiOp::HandOffCp { from: from.clone(), to: to.clone() });
    e.run_for(2000);
    assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    let handed = |e: &Engine| {
        let s = &e.client_view().state;
        s.cp.voters.contains(&to) && !s.cp.voters.contains(&from) && s.cp.handoff.is_none() && etcd_is_voters(e)
            && kube_of(e, &from) == Some((Some(KubeRole::Agent), KubePhase::Running))
    };
    assert!(e.run_until(e.now() + 3 * 60_000, handed), "voters {:?}, etcd {:?}, {from}: {:?}", e.client_view().state.cp.voters, e.fabric().world().kube.etcd, kube_of(&e, &from));
    assert_ne!(e.true_leader(), Some(from.clone()), "the old leader stepped down");
    let s = &e.client_view().state;
    assert!(s.hosts[&to].roles.contains(&Role::ControlPlane) && !s.hosts[&from].roles.contains(&Role::ControlPlane));
    assert_eq!(s.hosts[&from].lifecycle, Lifecycle::Ready, "the old voter stays in the fleet");
    let kube = &e.fabric().world().kube;
    let at = |c: EtcdChange| kube.etcd_log.iter().position(|l| *l == c);
    let (joined, left) = (at(EtcdChange::Joined(to.clone())), at(EtcdChange::Left(from.clone())));
    assert!(joined.is_some() && joined < left, "the new server joins etcd before the old one leaves: {:?}", kube.etcd_log);
    assert!(kube.violations.is_empty(), "{:?}", kube.violations);
}

#[test]
fn a_handoff_whose_new_k3s_server_never_starts_is_cancelled_and_the_old_voter_stays() {
    use control::proto::{KubePhase, KubeRole};
    let mut e = default_fleet(28);
    settle(&mut e);
    let etcd_is_voters = |e: &Engine| {
        let voters = &e.client_view().state.cp.voters;
        let kube = &e.fabric().world().kube;
        voters.len() == 5 && kube.etcd == *voters && kube.unstarted.is_empty() && voters.iter().all(|v| kube_of(e, v) == Some((Some(KubeRole::Server), KubePhase::Running)))
    };
    assert!(e.run_until(e.now() + 5 * 60_000, etcd_is_voters), "etcd {:?}", e.fabric().world().kube.etcd);
    let s = &e.client_view().state;
    let (voters, leader) = (s.cp.voters.clone(), e.true_leader().expect("a leader"));
    let from = voters.iter().find(|v| **v != leader).unwrap().clone();
    let ready_ms01 = |h: &&control::state::HostObj| h.lifecycle == Lifecycle::Ready && h.profile.id == ProfileId::Ms01;
    let to = s.hosts.values().filter(ready_ms01).find(|h| !s.cp.voters.contains(&h.id)).expect("a spare MS-01").id.clone();

    // `to` joins the control plane, but its K3s server never starts: the hand-off waits on it for good.
    e.fabric_mut().world_mut().kube.stall_joins.insert(to.clone());
    let req = e.api(ApiOp::HandOffCp { from: from.clone(), to: to.clone() });
    e.run_for(2000);
    assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    let stuck = |e: &Engine| e.client_view().state.cp.voters.contains(&to) && kube_of(e, &to) == Some((Some(KubeRole::Server), KubePhase::Installing));
    assert!(e.run_until(e.now() + 3 * 60_000, stuck), "{to}: {:?}", kube_of(&e, &to));
    e.run_for(60_000);
    assert!(e.client_view().state.cp.voters.contains(&from), "{from} stays while {to}'s server is not running");

    let req = e.api(ApiOp::CancelCpHandoff);
    e.run_for(2000);
    assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    let undone = |e: &Engine| {
        let s = &e.client_view().state;
        s.cp.voters == voters && s.cp.handoff.is_none() && etcd_is_voters(e) && kube_of(e, &to) == Some((Some(KubeRole::Agent), KubePhase::Running))
    };
    assert!(e.run_until(e.now() + 3 * 60_000, undone), "voters {:?}, etcd {:?}, unstarted {:?}, {to}: {:?}", e.client_view().state.cp.voters, e.fabric().world().kube.etcd, e.fabric().world().kube.unstarted, kube_of(&e, &to));
    let s = &e.client_view().state;
    assert!(s.events.values().any(|ev| ev.message == format!("stopped handing {from}'s control-plane place to {to}: cancelled by the user")));
    assert!(!s.hosts[&to].roles.contains(&Role::ControlPlane) && s.hosts[&from].roles.contains(&Role::ControlPlane));
    let req = e.api(ApiOp::CancelCpHandoff);
    e.run_for(2000);
    assert!(matches!(e.take_api_result(req), Some(ApiResult::Err(m)) if m.contains("no control-plane hand-off")));
    assert!(e.fabric().world().kube.violations.is_empty(), "{:?}", e.fabric().world().kube.violations);
}

#[test]
fn a_server_that_leaves_its_etcd_member_behind_as_it_becomes_an_agent_has_it_removed() {
    use control::proto::{KubePhase, KubeRole};
    use sim::world::EtcdChange;
    let mut e = default_fleet(27);
    settle(&mut e);
    let etcd_is_voters = |e: &Engine| {
        let voters = &e.client_view().state.cp.voters;
        voters.len() == 5 && e.fabric().world().kube.etcd == *voters && voters.iter().all(|v| kube_of(e, v) == Some((Some(KubeRole::Server), KubePhase::Running)))
    };
    assert!(e.run_until(e.now() + 5 * 60_000, etcd_is_voters), "etcd {:?}", e.fabric().world().kube.etcd);
    let s = &e.client_view().state;
    let leader = e.true_leader().expect("a leader");
    let from = s.cp.voters.iter().find(|v| **v != leader).unwrap().clone();
    let ready_ms01 = |h: &&control::state::HostObj| h.lifecycle == Lifecycle::Ready && h.profile.id == ProfileId::Ms01;
    let to = s.hosts.values().filter(ready_ms01).find(|h| !s.cp.voters.contains(&h.id)).expect("a spare MS-01").id.clone();
    // Deleting its own node fails as `from` reinstalls as an agent.
    e.fabric_mut().world_mut().kube.keep_member_on_leave.insert(from.clone());
    let req = e.api(ApiOp::HandOffCp { from: from.clone(), to: to.clone() });
    e.run_for(2000);
    assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    let handed = |e: &Engine| etcd_is_voters(e) && !e.client_view().state.cp.voters.contains(&from) && kube_of(e, &from) == Some((Some(KubeRole::Agent), KubePhase::Running));
    assert!(e.run_until(e.now() + 3 * 60_000, handed), "etcd {:?}, log {:?}", e.fabric().world().kube.etcd, e.fabric().world().kube.etcd_log);
    let kube = &e.fabric().world().kube;
    let at = |c: EtcdChange| kube.etcd_log.iter().position(|l| *l == c);
    let (behind, removed) = (at(EtcdChange::LeftBehind(from.clone())), at(EtcdChange::Removed(from.clone())));
    assert!(behind.is_some() && behind < removed, "the member left behind is removed: {:?}", kube.etcd_log);
    assert!(kube.violations.is_empty(), "{:?}", kube.violations);
    assert!(e.fabric().world().hosts[&from].powered, "while its host runs, as an agent");
}

#[test]
fn a_replacement_that_dies_joining_etcd_leaves_a_member_that_goes_once_it_is_replaced_in_turn() {
    use control::proto::{KubePhase, KubeRole};
    use sim::world::EtcdChange;
    let mut e = default_fleet(25);
    settle(&mut e);
    let etcd_is_voters = |e: &Engine| {
        let (voters, kube) = (&e.client_view().state.cp.voters, &e.fabric().world().kube);
        voters.len() == 5 && kube.etcd == *voters && kube.unstarted.is_empty() && voters.iter().all(|v| kube_of(e, v) == Some((Some(KubeRole::Server), KubePhase::Running)))
    };
    assert!(e.run_until(e.now() + 5 * 60_000, etcd_is_voters), "etcd {:?}", e.fabric().world().kube.etcd);
    let spare = |e: &Engine| {
        let s = &e.client_view().state;
        let h = s.hosts.values().find(|h| h.lifecycle == Lifecycle::Ready && h.profile.id == ProfileId::Ms01 && !s.cp.voters.contains(&h.id));
        h.expect("a spare MS-01").id.clone()
    };
    let replace = |e: &mut Engine, dead: &HostId, with: &HostId| {
        assert!(e.run_until(e.now() + 60_000, |e| lifecycle(e, dead) == Some(Lifecycle::NotReady)), "{dead} never went NotReady");
        let req = e.api(ApiOp::ReplaceCpMember { dead: dead.clone(), with: with.clone() });
        e.run_for(2000);
        assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    };

    // The replacement dies while joining: etcd added its member, which never starts.
    let (dead, _) = kill_follower_voter(&mut e);
    let with = spare(&e);
    e.fabric_mut().world_mut().kube.stall_joins.insert(with.clone());
    replace(&mut e, &dead, &with);
    let stalled = |e: &Engine| e.fabric().world().kube.unstarted.contains(&with) && kube_of(e, &with) == Some((Some(KubeRole::Server), KubePhase::Installing));
    assert!(e.run_until(e.now() + 3 * 60_000, stalled), "{with}: {:?}, {:?}", kube_of(&e, &with), e.fabric().world().kube.etcd_log);
    e.fault(Fault::KillHost { host: with.clone() }).unwrap();

    // Replaced in turn: its unstarted member goes, found by its address, and then the next one joins.
    let next = spare(&e);
    replace(&mut e, &with, &next);
    let replaced = |e: &Engine| e.client_view().state.cp.voters.contains(&next) && etcd_is_voters(e);
    assert!(e.run_until(e.now() + 3 * 60_000, replaced), "etcd {:?}, unstarted {:?}, {next}: {:?}", e.fabric().world().kube.etcd, e.fabric().world().kube.unstarted, kube_of(&e, &next));
    let kube = &e.fabric().world().kube;
    let at = |c: EtcdChange| kube.etcd_log.iter().position(|l| *l == c);
    let order = [at(EtcdChange::Removed(dead.clone())), at(EtcdChange::Stalled(with.clone())), at(EtcdChange::Removed(with.clone())), at(EtcdChange::Joined(next.clone()))];
    assert!(order.iter().all(Option::is_some) && order.is_sorted(), "{:?}", kube.etcd_log);
    assert!(kube.violations.is_empty(), "{:?}", kube.violations);
}

#[test]
fn suggest_mode_only_alerts() {
    let mut e = default_fleet(23);
    let (dead, killed_at) = kill_follower_voter(&mut e);
    let suggestion = |e: &Engine| {
        let alerts = &e.client_view().state.alerts;
        alerts.values().find(|a| a.kind == "CpReplacementSuggested" && a.resolved_at.is_none()).cloned()
    };
    e.run_until(killed_at + 10 * 60_000 - 5000, |_| false);
    assert!(suggestion(&e).is_none(), "suggested before 10 min");
    e.run_until(killed_at + 10 * 60_000 + 60_000, |_| false);
    let voters = e.client_view().state.cp.voters.clone();
    assert!(voters.contains(&dead), "suggest mode replaced the member on its own: {voters:?}");
    let alert = suggestion(&e).expect("a CpReplacementSuggested alert");
    assert_eq!(alert.entity, ObjectRef::host(&dead));
    let Some(ApiOp::ReplaceCpMember { dead: d, with }) = alert.action.clone() else {
        panic!("the alert offers the replacement: {alert:?}")
    };
    assert_eq!(d, dead);
    assert!(!voters.contains(&with));

    // Taking the suggestion carries the replacement out.
    let req = e.api(ApiOp::ReplaceCpMember { dead: dead.clone(), with: with.clone() });
    e.run_for(2000);
    assert_eq!(e.take_api_result(req), Some(ApiResult::Ok(serde_json::Value::Null)));
    let replaced = |e: &Engine| {
        let v = &e.client_view().state.cp.voters;
        v.contains(&with) && !v.contains(&dead)
    };
    assert!(e.run_until(e.now() + 30_000, replaced), "voters: {:?}", e.client_view().state.cp.voters);
    assert_eq!(e.client_view().state.cp.voters.len(), voters.len());
    assert!(e.run_until(e.now() + 5000, |e| suggestion(e).is_none()), "the suggestion resolves once taken");
}

// ---- alerts ----

fn open_alerts(e: &Engine, kind: &str) -> Vec<Alert> {
    e.client_view().state.alerts.values().filter(|a| a.kind == kind && a.resolved_at.is_none()).cloned().collect()
}

#[test]
fn disk_usage_alert_over_85() {
    let mut e = default_fleet(24);
    settle(&mut e);
    assert!(open_alerts(&e, "DiskUsageHigh").is_empty());
    // 1800 GB on a 2000 GB disk: 90%, plus the OS's share. Only MS-01 disks fit it.
    api_ok(&mut e, ApiOp::CreateVolume { volume: Volume { id: "big".into(), size_gb: 1800, replicas: 2 } });
    let placed = |e: &Engine| -> BTreeSet<(String, String)> {
        let status = e.client_view().state.volume_status.get("big").map(|s| s.status.replicas.clone()).unwrap_or_default();
        status.into_iter().map(|r| (r.host.to_string(), r.disk)).collect()
    };
    let alerted = |e: &Engine| -> BTreeSet<(String, String)> {
        open_alerts(e, "DiskUsageHigh").into_iter().map(|a| (a.entity.id, a.subject.unwrap_or_default())).collect()
    };
    let deadline = e.now() + 60_000;
    let full = |e: &Engine| placed(e).len() == 2 && alerted(e) == placed(e);
    assert!(e.run_until(deadline, full), "placed {:?}, alerted {:?}", placed(&e), alerted(&e));
    assert!(open_alerts(&e, "DiskUsageHigh").iter().all(|a| a.severity == Severity::Warning));
    e.run_for(30_000);
    assert_eq!(open_alerts(&e, "DiskUsageHigh").len(), 2, "one alert per full disk, however many samples");

    // A new leader starts without samples: the alerts stay open under their ids.
    let ids = |e: &Engine| open_alerts(e, "DiskUsageHigh").into_iter().map(|a| a.id).collect::<BTreeSet<_>>();
    let before = ids(&e);
    let old = e.true_leader().unwrap();
    e.fault(Fault::KillHost { host: old.clone() }).unwrap();
    let deadline = e.now() + 10_000;
    let moved = |e: &Engine| e.true_leader().is_some_and(|l| l != old) && e.client_view().leader != Some(old.clone());
    assert!(e.run_until(deadline, moved), "no new leader");
    e.run_for(30_000);
    assert_eq!(ids(&e), before, "resolved or raised again across the failover");

    api_ok(&mut e, ApiOp::DeleteVolume { id: "big".into() });
    let deadline = e.now() + 60_000;
    assert!(e.run_until(deadline, |e| alerted(e).is_empty()), "still alerted: {:?}", alerted(&e));
}

/// The open alerts by identity; panics if two share one.
fn open_by_identity(e: &Engine) -> BTreeMap<(String, ObjectRef, Option<String>), String> {
    let mut by_identity = BTreeMap::new();
    for a in e.client_view().state.alerts.values().filter(|a| a.resolved_at.is_none()) {
        let key = (a.kind.clone(), a.entity.clone(), a.subject.clone());
        if let Some(other) = by_identity.insert(key.clone(), a.id.clone()) {
            panic!("{key:?} is open twice: {other} and {}", a.id);
        }
    }
    by_identity
}

#[test]
fn alerts_deduplicated() {
    let mut e = default_fleet(25);
    settle(&mut e);
    let core2 = SwitchId::from("core-2");
    let deadline = e.now() + 35_000;
    assert!(e.run_until(deadline, |e| e.client_view().state.switches.contains_key(&core2)), "core-2 never polled");
    e.fault(Fault::KillSwitch { switch: core2 }).unwrap();
    let ms01s = e.client_view().state.hosts.keys().filter(|h| h.as_str().starts_with("ms01-")).count();
    // Polls go unanswered for 90 s before core-2 counts as unreachable.
    let noted = |e: &Engine| {
        let noted = |kind| open_alerts(e, kind).iter().filter(|a| a.message.ends_with("(peer switch core-2 unreachable)")).count();
        noted("PortDown") == ms01s && noted("BondDegraded") == ms01s
    };
    let deadline = e.now() + 150_000;
    assert!(e.run_until(deadline, noted), "t={} switch {:?} bonds {:?}", e.now(), open_alerts(&e, "SwitchUnreachable"), open_alerts(&e, "BondDegraded").first());
    assert_eq!(open_alerts(&e, "SwitchUnreachable").len(), 1);
    let before = open_by_identity(&e);
    let mismatches = open_alerts(&e, "NetworkMismatch");
    assert!(mismatches.iter().all(|a| !a.message.contains("not cabled")), "the cable is fine, the switch is dead: {mismatches:?}");

    // A new leader judges the same conditions afresh, under a new term.
    let old = e.true_leader().unwrap();
    e.fault(Fault::KillHost { host: old.clone() }).unwrap();
    let deadline = e.now() + 10_000;
    let moved = |e: &Engine| e.true_leader().is_some_and(|l| l != old) && e.client_view().leader != Some(old.clone());
    assert!(e.run_until(deadline, moved), "no new leader");
    e.run_for(60_000);
    let after = open_by_identity(&e);
    for (key, id) in &before {
        assert_eq!(after.get(key), Some(id), "{key:?} was raised again or resolved");
    }
    let still_noted = open_alerts(&e, "PortDown").iter().filter(|a| a.message.ends_with("(peer switch core-2 unreachable)")).count();
    assert_eq!(still_noted, ms01s, "the new leader never heard where sfp1 goes, but keeps the note");
}
