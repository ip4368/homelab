//! Control-plane resilience over the simulated fabric. Tests may read ground
//! truth (the fabric, replica internals); control code never does.

mod common;

use common::*;
use control::proto::{ApiOp, ApiResult};
use control::state::{Lifecycle, ObjectRef};
use sim::engine::Engine;
use sim::fabric::Fabric;
use sim::topology::NodeGroupKey;

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
    e.deliver_mut::<Fabric>().unwrap().world_mut().topology.partition([NodeGroupKey::Host(leader.clone())].into());
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
