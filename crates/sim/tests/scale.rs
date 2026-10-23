//! Scale: the 500-host preset comes up, runs well faster than real time with
//! its workloads, and keeps the UI's sync traffic small. Every test runs in
//! the workspace suite (about 12 s in a debug build); the wall-clock budgets
//! only mean something in an optimized build, so only that checks them
//! (`cargo test -p sim --release --test scale`).

use control::client::{ClientActor, CLIENT};
use control::state::Lifecycle;
use model::{PodPhase, VmPhase};
use sim::engine::Engine;
use sim::presets::Preset;
use sim::scenario::Scenario;
use std::time::Instant;

fn scale500(seed: u64) -> Engine {
    let mut e = Engine::new(seed);
    e.load_preset(Preset::Scale500);
    e
}

fn ready_hosts(e: &Engine) -> usize {
    e.client_view().state.hosts.values().filter(|h| h.lifecycle == Lifecycle::Ready).count()
}

/// The scale fleet with its seed workloads, settled for 60 s.
fn seeded(seed: u64) -> Engine {
    let mut e = scale500(seed);
    assert!(e.run_until(30_000, |e| ready_hosts(e) == 500), "the fleet comes up");
    let started = Instant::now();
    e.scenario(Scenario::SeedWorkloads).expect("the seed workloads are created");
    e.run_for(60_000);
    eprintln!("seeding and settling took {:.1} wall-s", started.elapsed().as_secs_f64());
    e
}

#[test]
fn scale500_preset_ready() {
    let mut e = scale500(26);
    let started = Instant::now();
    assert!(e.run_until(60_000, |e| ready_hosts(e) == 500), "only {} of 500 hosts Ready by 60 sim-s", ready_hosts(&e));
    eprintln!("500 Ready at {} sim-ms after {:.2} wall-s", e.now(), started.elapsed().as_secs_f64());
    e.run_until(60_000, |_| false);
    assert_eq!(ready_hosts(&e), 500, "and they stay Ready");
    assert_eq!(e.client_view().state.cp.voters.len(), 5);
}

#[test]
fn scale500_seed_workloads_fit() {
    let e = seeded(27);
    let s = &e.client_view().state;
    let pending: Vec<_> = s.vm_status.iter().filter(|(_, v)| matches!(v.status.phase, VmPhase::Pending(_))).collect();
    assert!(pending.is_empty(), "{} VMs pending, e.g. {:?}", pending.len(), pending.first());
    assert!(s.vms.len() >= 150, "the cap leaves a real VM load: {}", s.vms.len());
    assert!(s.vm_status.values().all(|v| v.status.phase == VmPhase::Running), "every VM runs");
    assert!(s.pods.values().all(|p| p.status.phase == PodPhase::Running), "every pod runs");
}

#[test]
fn scale500_runs_faster_than_realtime() {
    let mut e = seeded(28);
    let started = Instant::now();
    e.run_for(120_000);
    let wall = started.elapsed().as_secs_f64();
    eprintln!("120 sim-s with workloads took {wall:.2} wall-s");
    if !cfg!(debug_assertions) {
        assert!(wall < 40.0, "120 sim-s took {wall:.1} wall-s (budget 40)");
        // 60x playback asks for 120 sim-s in 2 wall-s; keep well clear of losing it.
        assert!(wall < 10.0, "120 sim-s took {wall:.1} wall-s: too slow to keep 60x playback comfortable (budget 10)");
    }
    assert_eq!(ready_hosts(&e), 500);
}

#[test]
fn scale500_sync_payload_bounded() {
    let mut e = seeded(29);
    let client = |e: &Engine| e.actor::<ClientActor>(&CLIENT).expect("client").sync_stats();
    let before = client(&e);
    e.run_for(60_000);
    let after = client(&e);
    let syncs = after.syncs - before.syncs;
    let avg = (after.upserts - before.upserts) as f64 / syncs as f64;
    eprintln!("{syncs} syncs in 60 sim-s, {avg:.1} upserts per sync");
    assert!(syncs >= 100, "the client keeps syncing: {syncs}");
    assert!(avg < 200.0, "{avg:.1} upserts per sync on average");
}
