//! Observation: the control plane learns cabling (LLDP from agents) and
//! switch configuration (polling) over the fabric, without reading the world.
//! Tests may read ground truth to inject faults and to check the log.

use control::proto::LogCmd;
use control::raft::EntryPayload;
use control::replica::CpReplica;
use control::runtime::{Addr, Endpoint, Svc};
use control::state::{Command, ObjectRef};
use model::network::{LldpNeighbor, PortMode, SwitchPortCfg};
use model::{HostId, PortName, SwitchId};
use sim::engine::Engine;
use sim::faults::Fault;
use sim::presets::Preset;

fn hid(s: &str) -> HostId {
    HostId::from(s)
}

fn sid(s: &str) -> SwitchId {
    SwitchId::from(s)
}

fn neighbor(local: &str, switch: &str, port: &str) -> LldpNeighbor {
    LldpNeighbor { local_port: PortName::from(local), switch: sid(switch), switch_port: PortName::from(port) }
}

fn default_fleet(seed: u64) -> Engine {
    let mut e = Engine::new(seed);
    e.load_preset(Preset::Default);
    e
}

fn lldp_of(e: &Engine, host: &str) -> Vec<LldpNeighbor> {
    e.client_view().state.hosts.get(&hid(host)).map(|h| h.lldp.clone()).unwrap_or_default()
}

/// Runs until every switch of the default fleet has an observation in the view.
fn observed_all_switches(e: &mut Engine) {
    let deadline = e.now() + 60_000;
    assert!(e.run_until(deadline, |e| e.client_view().state.switches.len() == 4), "switches observed: {:?}", e.client_view().state.switches.keys());
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

#[test]
fn lldp_reported() {
    let mut e = default_fleet(31);
    let deadline = 15_000;
    let seen = |e: &Engine| lldp_of(e, "ms01-01").contains(&neighbor("sfp0", "core-1", "p1"));
    assert!(e.run_until(deadline, seen), "ms01-01 lldp after 15 s: {:?}", lldp_of(&e, "ms01-01"));
    assert_eq!(
        lldp_of(&e, "ms01-01"),
        [neighbor("sfp0", "core-1", "p1"), neighbor("sfp1", "core-2", "p1"), neighbor("eth0", "access-1", "p1")]
    );
    // Hosts with one cable report one neighbour.
    let all = |e: &Engine| e.client_view().state.hosts.values().all(|h| !h.lldp.is_empty());
    assert!(e.run_until(15_000, all), "some host never reported");
    assert_eq!(lldp_of(&e, "pi5-11"), [neighbor("eth0", "access-2", "p1")]);
}

#[test]
fn lldp_follows_cabling_and_reports_only_changes() {
    let mut e = default_fleet(32);
    observed_all_switches(&mut e);
    e.run_for(10_000);
    let (host, nic) = (hid("ms01-02"), PortName::from("sfp0"));
    assert_eq!(lldp_of(&e, "ms01-02").len(), 3);

    e.fault(Fault::UnplugPort { host: host.clone(), port: nic.clone() }).unwrap();
    let gone = |e: &Engine| lldp_of(e, "ms01-02") == [neighbor("sfp1", "core-2", "p2"), neighbor("eth0", "access-1", "p2")];
    assert!(e.run_until(e.now() + 15_000, gone), "after unplug: {:?}", lldp_of(&e, "ms01-02"));
    let inv = &e.client_view().state.hosts[&host].inventory;
    assert!(!inv.nics.iter().find(|n| n.name == "sfp0").unwrap().link_up, "the inventory follows the link too");

    e.fault(Fault::ReplugPort { host, port: nic }).unwrap();
    assert!(e.run_until(e.now() + 15_000, |e| lldp_of(e, "ms01-02").len() == 3), "after replug: {:?}", lldp_of(&e, "ms01-02"));

    // Nothing changes: no observation is written for 2 sim-minutes.
    let before = logged(&e, |c| matches!(c, Command::ObserveHost { .. }));
    e.run_for(120_000);
    assert_eq!(logged(&e, |c| matches!(c, Command::ObserveHost { .. })), before);
}

#[test]
fn switch_config_observed() {
    let mut e = default_fleet(33);
    observed_all_switches(&mut e);
    let access1 = sid("access-1");
    let p3 = PortName::from("p3");
    let cfg = |e: &Engine| e.client_view().state.switches[&access1].ports.iter().find(|p| p.port == p3).unwrap().cfg.clone();
    assert_eq!(cfg(&e).mode, PortMode::Access(1));
    let core = &e.client_view().state.switches[&sid("core-1")];
    assert_eq!((core.model.as_str(), core.mlag_peer.clone(), core.ports.len()), ("core-64x10g-sfp+", Some(sid("core-2")), 65));

    let changed = SwitchPortCfg { mode: PortMode::Access(20), mtu: 9216, lacp_group: None, admin_up: true };
    e.fault(Fault::SetSwitchPort { switch: access1.clone(), port: p3.clone(), cfg: changed.clone() }).unwrap();
    let deadline = e.now() + 35_000;
    assert!(e.run_until(deadline, |e| cfg(e) == changed), "access-1 p3 still {:?} 35 s after the change", cfg(&e));
}

#[test]
fn quiet_switches_are_written_at_most_once_a_minute() {
    let mut e = default_fleet(34);
    observed_all_switches(&mut e);
    e.run_for(30_000);
    let observes = |e: &Engine| logged(e, |c| matches!(c, Command::ObserveSwitch { .. }));
    let before = observes(&e);
    e.run_for(300_000);
    let writes = observes(&e) - before;
    // 4 switches, 5 minutes, a refresh each minute: polling every 30 s would write 40.
    assert!((16..=24).contains(&writes), "{writes} switch writes in 5 minutes");
    // The refreshed timestamp is recent without a write per poll.
    let now = e.now();
    for sw in e.client_view().state.switches.values() {
        assert!(now - sw.last_seen <= 65_000, "{} last_seen {} at {now}", sw.id, sw.last_seen);
    }
}

#[test]
fn unreachable_switch_goes_stale() {
    let mut e = default_fleet(35);
    observed_all_switches(&mut e);
    e.run_for(35_000);
    let access2 = sid("access-2");
    let frozen = e.client_view().state.switches[&access2].last_seen;
    let access1_before = e.client_view().state.switches[&sid("access-1")].last_seen;
    let alert = |e: &Engine| {
        e.client_view().state.alerts.values().find(|a| a.kind == "SwitchUnreachable" && a.entity == ObjectRef::switch(&sid("access-2"))).cloned()
    };
    assert!(alert(&e).is_none());

    e.fault(Fault::KillSwitch { switch: access2.clone() }).unwrap();
    let killed_at = e.now();
    e.run_for(60_000);
    assert!(alert(&e).is_none() || e.now() - frozen > 90_000, "alert before 90 s of silence");
    let deadline = killed_at + 120_000;
    assert!(e.run_until(deadline, |e| alert(e).is_some()), "no SwitchUnreachable alert 120 s after the kill");
    let a = alert(&e).unwrap();
    assert!(a.resolved_at.is_none());
    assert!(a.raised_at >= frozen + 90_000, "raised at {} but the switch was last written at {frozen}", a.raised_at);
    assert_eq!(e.client_view().state.switches[&access2].last_seen, frozen, "last_seen stays frozen");
    assert!(e.client_view().state.switches[&sid("access-1")].last_seen > access1_before, "other switches keep reporting");

    // It comes back: the next poll resolves the alert and moves last_seen.
    e.fault(Fault::RestoreSwitch { switch: access2.clone() }).unwrap();
    let back = |e: &Engine| alert(e).is_some_and(|a| a.resolved_at.is_some()) && e.client_view().state.switches[&access2].last_seen > frozen;
    assert!(e.run_until(e.now() + 40_000, back), "alert not resolved after the switch came back");
}

#[test]
fn a_new_leader_keeps_observing() {
    let mut e = default_fleet(36);
    observed_all_switches(&mut e);
    e.run_for(10_000);
    let old = e.true_leader().expect("a leader");
    e.fault(Fault::KillHost { host: old.clone() }).unwrap();
    assert!(e.run_until(e.now() + 15_000, |e| e.true_leader().is_some_and(|l| l != old)), "no new leader");

    // The new leader starts with empty memory: it polls the switches it reads from the log
    // and gets the hosts' observations by asking for them.
    let p5 = PortName::from("p5");
    let changed = SwitchPortCfg { mode: PortMode::Access(30), mtu: 9216, lacp_group: None, admin_up: true };
    e.fault(Fault::SetSwitchPort { switch: sid("access-1"), port: p5.clone(), cfg: changed.clone() }).unwrap();
    e.fault(Fault::UnplugPort { host: hid("ms01-10"), port: PortName::from("sfp1") }).unwrap();
    let observed = |e: &Engine| {
        let port = e.client_view().state.switches[&sid("access-1")].ports.iter().find(|p| p.port == p5).unwrap().cfg.clone();
        port == changed && lldp_of(e, "ms01-10").len() == 2
    };
    assert!(e.run_until(e.now() + 40_000, observed), "the new leader missed a change");
}
