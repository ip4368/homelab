//! Observation: the control plane learns cabling (LLDP from agents) and
//! switch configuration (polling) over the fabric, without reading the world.
//! Tests may read ground truth to inject faults and to check the log.

use control::agent::HostAgent;
use control::controllers::network::{NETWORK_APPLY_FAILED, NETWORK_MISMATCH};
use control::proto::{ApiOp, LogCmd};
use control::raft::EntryPayload;
use control::replica::CpReplica;
use control::runtime::{Addr, Endpoint, Svc};
use control::state::{Alert, Command, HostObj, Lifecycle, NetState, ObjectRef};
use model::network::{LldpNeighbor, PortMode, SwitchPortCfg};
use model::{HostId, NetworkId, PortName, ProfileId, Role, SwitchId};
use sim::engine::Engine;
use sim::faults::Fault;
use sim::presets::Preset;
use sim::scenario::{Misconfig, Scenario};
use sim::topology::Node;
use std::collections::BTreeSet;

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

// ---- the network controller: templated joins, held roles, auto-revert ----

fn host_obj(e: &Engine, id: &HostId) -> Option<HostObj> {
    e.client_view().state.hosts.get(id).cloned()
}

/// Runs until the default fleet is settled: all 60 hosts `Ready` and every switch observed.
fn settled_fleet(seed: u64) -> Engine {
    let mut e = default_fleet(seed);
    observed_all_switches(&mut e);
    let ready = |e: &Engine| e.client_view().state.hosts.values().filter(|h| h.lifecycle == Lifecycle::Ready).count() == 60;
    assert!(e.run_until(e.now() + 30_000, ready), "the fleet did not settle");
    e
}

/// Open alerts of `kind` about `host`.
fn open_alerts(e: &Engine, kind: &str, host: &HostId) -> Vec<Alert> {
    let entity = ObjectRef::host(host);
    e.client_view().state.alerts.values().filter(|a| a.kind == kind && a.entity == entity && a.resolved_at.is_none()).cloned().collect()
}

#[test]
fn default_preset_starts_clean() {
    let mut e = default_fleet(40);
    e.run_for(60_000);
    let s = &e.client_view().state;
    assert_eq!(s.hosts.len(), 60);
    for h in s.hosts.values() {
        assert_eq!((h.lifecycle, h.net.state), (Lifecycle::Ready, NetState::Applied), "{}", h.id);
        assert!(h.net.desired_generation > 0 && h.net.applied_generation == h.net.desired_generation, "{}: {:?}", h.id, h.net);
        assert!(h.net.held_roles.is_empty() && h.net.issues.is_empty(), "{}: {:?}", h.id, h.net);
        assert_eq!(e.fabric().world().hosts[&h.id].applied_net, *h.net.desired.as_ref().unwrap(), "{} runs its desired config", h.id);
    }
    let mismatches: Vec<_> = s.alerts.values().filter(|a| a.kind == NETWORK_MISMATCH).collect();
    assert!(mismatches.is_empty(), "{mismatches:?}");
    // The template is what the hosts run: an MS-01 has its storage bond, with an address.
    let ms01 = &s.hosts[&hid("ms01-01")];
    assert_eq!(ms01.template.as_deref(), Some("ms01-standard"));
    let storage = ms01.net.desired.as_ref().unwrap().attachments.iter().find(|a| a.network.as_str() == "storage").cloned();
    assert_eq!(storage.map(|a| (a.link, a.ip)), Some(("bond0".to_string(), ms01.ips.get(&NetworkId::from("storage")).copied())));
    assert!(ms01.ips.contains_key(&NetworkId::from("storage")) && ms01.ips.contains_key(&NetworkId::from("migration")));
    // Every agent converged on the generation it was told (a restart re-pushes it).
    for h in s.hosts.values() {
        let a = e.actor::<HostAgent>(&Endpoint::new(Addr::Host(h.id.clone()), Svc::Agent)).unwrap();
        assert_eq!(a.net_generation(), h.net.desired_generation, "{}", h.id);
    }
}

#[test]
fn criterion_2_twenty_rk3588_join() {
    let mut e = settled_fleet(41);
    let rule = &e.client_view().state.rules["rk3588"];
    assert_eq!((rule.roles.clone(), rule.template.as_deref()), ([Role::Transcode, Role::Compute].into(), Some("sbc-transcode")));
    e.scenario(Scenario::AddHosts { profile: ProfileId::Rk3588, count: 20, misconfigure: None }).unwrap();
    let added: Vec<HostId> = (13..=32).map(|n| hid(&format!("rk3588-{n}"))).collect();
    let done = |e: &Engine| {
        added.iter().all(|h| host_obj(e, h).is_some_and(|o| o.lifecycle == Lifecycle::Ready && o.net.state == NetState::Applied))
    };
    let deadline = e.now() + 60_000;
    assert!(
        e.run_until(deadline, done),
        "{:?}",
        added.iter().map(|h| host_obj(&e, h).map(|o| (o.lifecycle, o.net.state))).collect::<Vec<_>>()
    );
    let s = &e.client_view().state;
    let mut mgmt_ips = BTreeSet::new();
    for h in &added {
        let o = &s.hosts[h];
        assert_eq!(o.template.as_deref(), Some("sbc-transcode"));
        assert_eq!(o.roles, [Role::Transcode, Role::Compute].into());
        let ip = o.ips[&NetworkId::from("mgmt")];
        assert!(mgmt_ips.insert(ip), "{h} got a duplicate address {ip}");
        assert_eq!(e.fabric().world().hosts[h].applied_net, *o.net.desired.as_ref().unwrap(), "{h} runs the template");
    }
}

#[test]
fn misconfigured_join_ready_but_role_held() {
    let mut e = settled_fleet(42);
    let roles: BTreeSet<Role> = [Role::Storage, Role::Compute].into();
    let rule = ApiOp::UpsertRule {
        id: "minipc".into(),
        profile: ProfileId::MiniPc,
        roles: roles.clone(),
        template: Some("minipc-standard".into()),
    };
    e.api(rule);
    e.run_for(2000);
    let s = Scenario::AddHosts { profile: ProfileId::MiniPc, count: 1, misconfigure: Some(Misconfig::MissingVlan(20)) };
    e.scenario(s).unwrap();
    let h = hid("minipc-25");
    let held = |e: &Engine| {
        host_obj(e, &h).is_some_and(|o| o.lifecycle == Lifecycle::Ready && o.net.held_roles.contains_key(&Role::Storage))
            && open_alerts(e, NETWORK_MISMATCH, &h).iter().any(|a| a.message.contains("add VLAN 20"))
    };
    let deadline = e.now() + 60_000;
    assert!(e.run_until(deadline, held), "{:?} alerts {:?}", host_obj(&e, &h).map(|o| (o.lifecycle, o.net)), open_alerts(&e, NETWORK_MISMATCH, &h));
    let o = host_obj(&e, &h).unwrap();
    assert_eq!(o.roles, roles);
    assert_eq!(o.net.held_roles, [(Role::Storage, NetworkId::from("storage"))].into(), "compute only needs mgmt");
    assert_eq!(o.net.state, NetState::Applied);
    let alerts = open_alerts(&e, NETWORK_MISMATCH, &h);
    assert_eq!(alerts.len(), 1, "one alert per host: {alerts:?}");
    let port = e.client_view().state.hosts[&h].lldp[0].clone();
    let want = format!("{} {}: add VLAN 20 to trunk allowed", port.switch, port.switch_port);
    assert!(alerts[0].message.contains(&want), "{} lacks {want}", alerts[0].message);

    // The technician fixes the port: the alert resolves and the role is released.
    let mut cfg = e.fabric().world().topology.switches[&port.switch].port(&port.switch_port).unwrap().clone();
    let PortMode::Trunk { allowed, .. } = &mut cfg.mode else { panic!("a mini PC port is a trunk: {cfg:?}") };
    allowed.insert(20);
    e.fault(Fault::SetSwitchPort { switch: port.switch.clone(), port: port.switch_port.clone(), cfg }).unwrap();
    let fixed = |e: &Engine| host_obj(e, &h).is_some_and(|o| o.net.held_roles.is_empty()) && open_alerts(e, NETWORK_MISMATCH, &h).is_empty();
    assert!(e.run_until(e.now() + 40_000, fixed), "not released after the fix: {:?}", host_obj(&e, &h).map(|o| o.net));
}

#[test]
fn mgmt_breaking_override_reverts() {
    let mut e = settled_fleet(43);
    let h = hid("pi5-01");
    let before = host_obj(&e, &h).unwrap();
    let running = e.fabric().world().hosts[&h].applied_net.clone();
    // mgmt tagged on VLAN 1, but the host sits on an access port: it would lose mgmt.
    let mut broken = running.clone();
    broken.attachments[0].tagged = true;
    e.api(ApiOp::SetHostOverride { id: h.clone(), config: Some(broken) });
    let start = e.now();
    let reverted = |e: &Engine| host_obj(e, &h).is_some_and(|o| o.net.state == NetState::Reverted);
    assert!(e.run_until(start + 35_000, reverted), "not Reverted 35 s after the override: {:?}", host_obj(&e, &h).map(|o| o.net));
    let o = host_obj(&e, &h).unwrap();
    assert_eq!(o.net.desired_generation, before.net.desired_generation + 1);
    let cleared = |e: &Engine| {
        host_obj(e, &h).is_some_and(|o| o.net.held_roles.is_empty() && o.net.issues.is_empty())
            && open_alerts(e, NETWORK_MISMATCH, &h).is_empty()
    };
    assert!(e.run_until(e.now() + 3000, cleared), "the config it went back to is fine: {:?}", host_obj(&e, &h).map(|o| o.net));
    assert_eq!(o.net.applied_generation, before.net.desired_generation, "still on the old generation");
    assert!(o.net.drift, "an override that differs from the template is drift");
    assert_eq!(e.fabric().world().hosts[&h].applied_net, running, "the agent put the old config back");
    let alerted = |e: &Engine| open_alerts(e, NETWORK_APPLY_FAILED, &h).len() == 1;
    assert!(e.run_until(e.now() + 2000, alerted), "the failure is not alerted");

    // The host stays reachable: it keeps heartbeating, stays Ready, and the failed generation is not retried.
    e.run_for(60_000);
    let o = host_obj(&e, &h).unwrap();
    assert_eq!((o.lifecycle, o.net.state), (Lifecycle::Ready, NetState::Reverted));
    assert_eq!(e.fabric().world().hosts[&h].applied_net, running, "no retry of the broken generation");
    let mgmt = e.fabric().world().networks[&NetworkId::from("mgmt")].clone();
    let eth0 = Node::HostPort(h.clone(), PortName::from("eth0"));
    assert!(e.fabric().reachable(&eth0, &Node::Client, mgmt.vlan, mgmt.mtu));

    // Clearing the override changes the desired config again: it applies and the alert resolves.
    e.api(ApiOp::SetHostOverride { id: h.clone(), config: None });
    let healed = |e: &Engine| host_obj(e, &h).is_some_and(|o| o.net.state == NetState::Applied && !o.net.drift)
        && open_alerts(e, NETWORK_APPLY_FAILED, &h).is_empty();
    assert!(e.run_until(e.now() + 15_000, healed), "{:?}", host_obj(&e, &h).map(|o| o.net));
}

#[test]
fn a_host_rebooted_during_a_trial_boots_its_last_confirmed_config() {
    let mut e = settled_fleet(44);
    let h = hid("pi5-02");
    let running = e.fabric().world().hosts[&h].applied_net.clone();
    let mut broken = running.clone();
    broken.attachments[0].tagged = true;
    e.api(ApiOp::SetHostOverride { id: h.clone(), config: Some(broken.clone()) });
    let on_trial = |e: &Engine| e.fabric().world().hosts[&h].applied_net == broken;
    assert!(e.run_until(e.now() + 5000, on_trial), "the agent never applied the override");

    // Power-cycled before the trial runs out: the unconfirmed config was never written for boot.
    e.fault(Fault::KillHost { host: h.clone() }).unwrap();
    e.run_for(5000);
    e.fault(Fault::RestoreHost { host: h.clone() }).unwrap();
    assert_eq!(e.fabric().world().hosts[&h].applied_net, running, "booted the last confirmed config");
    let mgmt = e.fabric().world().networks[&NetworkId::from("mgmt")].clone();
    let eth0 = Node::HostPort(h.clone(), PortName::from("eth0"));
    assert!(e.fabric().reachable(&eth0, &Node::Client, mgmt.vlan, mgmt.mtu), "reachable again");

    // The leader tries the generation again; it reverts again, and the host stays reachable.
    let reverted = |e: &Engine| host_obj(e, &h).is_some_and(|o| o.net.state == NetState::Reverted);
    assert!(e.run_until(e.now() + 45_000, reverted), "{:?}", host_obj(&e, &h).map(|o| o.net));
    assert_eq!(e.fabric().world().hosts[&h].applied_net, running);
    let ready = |e: &Engine| host_obj(e, &h).is_some_and(|o| o.lifecycle == Lifecycle::Ready);
    assert!(e.run_until(e.now() + 15_000, ready), "{:?}", host_obj(&e, &h).map(|o| o.lifecycle));
}
