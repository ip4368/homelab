//! Observation: what the control plane learns by listening, never by looking
//! at the simulated world.
//!
//! * Hosts: agents send their LLDP neighbours and inventory when they change;
//!   the leader keeps the latest in [`Volatile::host_reports`] and writes
//!   `ObserveHost` only when it differs from the host object.
//! * Switches: the leader polls every switch it knows of (from LLDP or an
//!   earlier observation) each [`SWITCH_POLL_MS`]. `ObserveSwitch` is written
//!   when a report differs from the switch object, or to refresh `last_seen`
//!   once it is [`SWITCH_REFRESH_MS`] old, so the log does not grow with every
//!   poll. A switch silent for [`SWITCH_UNREACHABLE_AFTER_MS`] raises a
//!   `SwitchUnreachable` alert; its next report resolves it.

use super::{Action, Volatile};
use crate::proto::{mgmt, Msg};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{Alert, ClusterState, Command, ObjectRef, Severity};
use model::SwitchId;
use std::collections::BTreeSet;

pub const SWITCH_POLL_MS: u64 = 30_000;
pub const SWITCH_REFRESH_MS: u64 = 60_000;
pub const SWITCH_UNREACHABLE_AFTER_MS: u64 = 90_000;
pub const SWITCH_UNREACHABLE: &str = "SwitchUnreachable";

pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = observe_hosts(state, v);
    let known = known_switches(state);
    out.extend(poll_switches(&known, v, now));
    out.extend(observe_switches(state, v));
    out.extend(switch_alerts(state, &known, v, now));
    out
}

fn observe_hosts(state: &ClusterState, v: &Volatile) -> Vec<Action> {
    v.host_reports
        .iter()
        .filter_map(|(id, report)| {
            let host = state.hosts.get(id)?;
            let o = &report.observed;
            (host.lldp != o.lldp || host.inventory != o.inventory).then(|| {
                Action::Propose(Command::ObserveHost { id: id.clone(), lldp: o.lldp.clone(), inventory: o.inventory.clone() })
            })
        })
        .collect()
}

/// The switches the control plane has heard of: observed before, or named by
/// a host's LLDP neighbours.
fn known_switches(state: &ClusterState) -> BTreeSet<SwitchId> {
    let from_lldp = state.hosts.values().flat_map(|h| h.lldp.iter().map(|n| n.switch.clone()));
    state.switches.keys().cloned().chain(from_lldp).collect()
}

fn poll_switches(known: &BTreeSet<SwitchId>, v: &Volatile, now: SimTime) -> Vec<Action> {
    known
        .iter()
        .filter(|id| v.switch_polled_at.get(*id).is_none_or(|&at| now >= at + SWITCH_POLL_MS))
        .map(|id| {
            let to = Endpoint::new(Addr::Switch(id.clone()), Svc::Switch);
            Action::Send(to, mgmt(), Msg::SwitchPoll)
        })
        .collect()
}

fn observe_switches(state: &ClusterState, v: &Volatile) -> Vec<Action> {
    v.switch_reports
        .iter()
        .filter(|(id, r)| {
            state.switches.get(*id).is_none_or(|o| {
                let changed = o.model != r.model || o.ports != r.ports || o.mlag_peer != r.mlag_peer;
                changed || r.at >= o.last_seen + SWITCH_REFRESH_MS
            })
        })
        .map(|(id, r)| {
            Action::Propose(Command::ObserveSwitch {
                id: id.clone(),
                model: r.model.clone(),
                ports: r.ports.clone(),
                mlag_peer: r.mlag_peer.clone(),
                last_seen: r.at,
            })
        })
        .collect()
}

fn switch_alerts(state: &ClusterState, known: &BTreeSet<SwitchId>, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = vec![];
    for id in known {
        let seen = v.switch_last_seen(id);
        let open = open_alert(state, id);
        if now.saturating_sub(seen) > SWITCH_UNREACHABLE_AFTER_MS {
            if open.is_none() {
                out.push(Action::Propose(Command::RaiseAlert {
                    // Stable across runs and unique per incident, as for HostDown.
                    id: format!("switchdown-{id}-{}-{seen}", v.term),
                    severity: Severity::Critical,
                    kind: SWITCH_UNREACHABLE.into(),
                    entity: ObjectRef::switch(id),
                    message: format!("{id} did not answer polls for {} s", SWITCH_UNREACHABLE_AFTER_MS / 1000),
                    ground_truth: false,
                }));
            }
        } else if let Some(a) = open.filter(|_| v.switch_reports.contains_key(id)) {
            // Only a real report counts as recovery, not the election grace.
            out.push(Action::Propose(Command::ResolveAlert { id: a.id.clone() }));
        }
    }
    out
}

fn open_alert<'a>(state: &'a ClusterState, id: &SwitchId) -> Option<&'a Alert> {
    let entity = ObjectRef::switch(id);
    state.alerts.values().find(|a| a.kind == SWITCH_UNREACHABLE && a.entity == entity && a.resolved_at.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::{HostReport, SwitchReading};
    use crate::proto::Observed;
    use model::network::{LldpNeighbor, ObservedSwitchPort, PortMode, SwitchPortCfg};
    use model::{profile, HostId, Inventory, PortName, ProfileId};

    fn sid(s: &str) -> SwitchId {
        SwitchId::from(s)
    }

    fn port(switch: &str, vlan: u16) -> ObservedSwitchPort {
        let cfg = SwitchPortCfg { mode: PortMode::Access(vlan), mtu: 9216, lacp_group: None, admin_up: true };
        ObservedSwitchPort { switch: sid(switch), port: PortName::from("p1"), cfg, mlag_peer: None, link_up: true, speed_mbps: 1000, neighbor: None }
    }

    fn reading(switch: &str, at: SimTime, vlan: u16) -> SwitchReading {
        SwitchReading { at, model: "m".into(), ports: vec![port(switch, vlan)], mlag_peer: None }
    }

    fn observe(switch: &str, at: SimTime, vlan: u16) -> Command {
        Command::ObserveSwitch { id: sid(switch), model: "m".into(), ports: vec![port(switch, vlan)], mlag_peer: None, last_seen: at }
    }

    fn proposals(actions: Vec<Action>) -> Vec<Command> {
        actions.into_iter().filter_map(|a| if let Action::Propose(c) = a { Some(c) } else { None }).collect()
    }

    fn polls(actions: Vec<Action>) -> Vec<SwitchId> {
        actions
            .into_iter()
            .filter_map(|a| match a {
                Action::Send(Endpoint { addr: Addr::Switch(s), svc: Svc::Switch }, net, Msg::SwitchPoll) => {
                    assert_eq!(net, mgmt());
                    Some(s)
                }
                _ => None,
            })
            .collect()
    }

    fn lldp(switch: &str) -> Vec<LldpNeighbor> {
        vec![LldpNeighbor { local_port: PortName::from("eth0"), switch: sid(switch), switch_port: PortName::from("p1") }]
    }

    /// Hosts h1 (cabled to access-1) and h2 (to core-1 and access-2), each Pending.
    fn state_with_hosts() -> ClusterState {
        let mut s = ClusterState::new();
        for (h, switches) in [("h1", ["access-1", ""]), ("h2", ["core-1", "access-2"])] {
            let p = ProfileId::MiniPc;
            let cmd = Command::AddPendingHost {
                id: HostId::from(h),
                profile: profile(p, 0),
                inventory: Inventory::from_profile(p, 0),
                location: "rack-1".into(),
            };
            s.apply(0, cmd).unwrap();
            let neighbors = switches.iter().filter(|sw| !sw.is_empty()).flat_map(|sw| lldp(sw)).collect();
            s.apply(0, Command::ObserveHost { id: HostId::from(h), lldp: neighbors, inventory: Inventory::from_profile(p, 0) }).unwrap();
        }
        s
    }

    #[test]
    fn host_observation_is_proposed_only_when_it_differs() {
        let s = state_with_hosts();
        let mut v = Volatile::new(0, 1);
        let h1 = HostId::from("h1");
        let same = Observed { lldp: lldp("access-1"), inventory: Inventory::from_profile(ProfileId::MiniPc, 0) };
        v.host_reports.insert(h1.clone(), HostReport { observed_at: 5, observed: same.clone() });
        assert!(proposals(run(&s, &v, 1000)).is_empty(), "identical to the host object");

        let mut moved = same.clone();
        moved.lldp = lldp("access-2");
        v.host_reports.insert(h1.clone(), HostReport { observed_at: 9, observed: moved.clone() });
        assert_eq!(
            proposals(run(&s, &v, 1000)),
            [Command::ObserveHost { id: h1.clone(), lldp: moved.lldp, inventory: moved.inventory }]
        );

        let mut down = same;
        down.inventory.nics[0].link_up = false;
        v.host_reports.insert(h1.clone(), HostReport { observed_at: 9, observed: down.clone() });
        assert!(matches!(proposals(run(&s, &v, 1000)).as_slice(), [Command::ObserveHost { inventory, .. }] if *inventory == down.inventory));

        v.host_reports.insert(HostId::from("gone"), HostReport { observed_at: 1, observed: down });
        assert_eq!(proposals(run(&s, &v, 1000)).len(), 1, "a report for a host that does not exist is ignored");
    }

    #[test]
    fn every_known_switch_is_polled_every_30_s() {
        let mut s = state_with_hosts();
        let mut v = Volatile::new(0, 1);
        // Heard of through LLDP: access-1, core-1 and access-2.
        assert_eq!(polls(run(&s, &v, 0)), [sid("access-1"), sid("access-2"), sid("core-1")]);
        for id in ["access-1", "access-2", "core-1"] {
            v.switch_polled_at.insert(sid(id), 1000);
        }
        assert!(polls(run(&s, &v, 30_999)).is_empty());
        assert_eq!(polls(run(&s, &v, 31_000)).len(), 3);
        v.switch_polled_at.insert(sid("core-1"), 20_000);
        assert_eq!(polls(run(&s, &v, 31_000)), [sid("access-1"), sid("access-2")]);

        // A switch observed before stays polled after its LLDP neighbours disappear.
        s.apply(1, observe("core-2", 1, 1)).unwrap();
        assert!(polls(run(&s, &v, 31_000)).contains(&sid("core-2")));
    }

    #[test]
    fn switch_reports_are_written_when_new_changed_or_a_minute_old() {
        let mut s = ClusterState::new();
        let mut v = Volatile::new(0, 1);
        v.switch_reports.insert(sid("a"), reading("a", 1000, 1));
        assert_eq!(proposals(run(&s, &v, 1000)), [observe("a", 1000, 1)], "first sight");
        s.apply(1000, observe("a", 1000, 1)).unwrap();
        assert!(proposals(run(&s, &v, 1500)).is_empty());

        v.switch_reports.insert(sid("a"), reading("a", 31_000, 1));
        assert!(proposals(run(&s, &v, 31_000)).is_empty(), "a poll that shows nothing new writes nothing");
        v.switch_reports.insert(sid("a"), reading("a", 61_000, 1));
        assert_eq!(proposals(run(&s, &v, 61_000)), [observe("a", 61_000, 1)], "last_seen is a minute old");
        s.apply(61_000, observe("a", 61_000, 1)).unwrap();
        assert!(proposals(run(&s, &v, 62_000)).is_empty());

        v.switch_reports.insert(sid("a"), reading("a", 91_000, 20));
        assert_eq!(proposals(run(&s, &v, 91_000)), [observe("a", 91_000, 20)], "a change is written at once");

        // The switch goes silent: the stale reading is not written again and again.
        s.apply(91_000, observe("a", 91_000, 20)).unwrap();
        let later = proposals(run(&s, &v, 91_000 + 10 * SWITCH_REFRESH_MS));
        assert!(later.iter().all(|c| !matches!(c, Command::ObserveSwitch { .. })), "{later:?}");
    }

    #[test]
    fn a_link_change_alone_is_written_at_once() {
        let mut s = ClusterState::new();
        let mut v = Volatile::new(0, 1);
        s.apply(1000, observe("a", 1000, 1)).unwrap();
        let mut r = reading("a", 2000, 1);
        v.switch_reports.insert(sid("a"), r.clone());
        assert!(proposals(run(&s, &v, 2000)).is_empty());

        r.ports[0].link_up = false;
        v.switch_reports.insert(sid("a"), r.clone());
        assert!(matches!(proposals(run(&s, &v, 2000)).as_slice(), [Command::ObserveSwitch { ports, .. }] if !ports[0].link_up));

        r.ports[0].link_up = true;
        r.ports[0].speed_mbps = 40_000;
        v.switch_reports.insert(sid("a"), r.clone());
        assert_eq!(proposals(run(&s, &v, 2000)).len(), 1, "negotiated speed");
        r.ports[0].speed_mbps = 1000;
        r.ports[0].neighbor = Some(model::network::LldpPeer { switch: sid("b"), port: PortName::from("p2") });
        v.switch_reports.insert(sid("a"), r);
        assert_eq!(proposals(run(&s, &v, 2000)).len(), 1, "neighbour");
    }

    fn alert_kinds(actions: Vec<Action>) -> Vec<Command> {
        proposals(actions).into_iter().filter(|c| matches!(c, Command::RaiseAlert { .. } | Command::ResolveAlert { .. })).collect()
    }

    #[test]
    fn silent_switch_raises_one_alert_after_90_s_and_its_next_report_resolves_it() {
        let mut s = ClusterState::new();
        s.apply(0, observe("a", 0, 1)).unwrap();
        let mut v = Volatile::new(100_000, 3);
        // A new leader gives every switch a full grace period.
        assert!(alert_kinds(run(&s, &v, 100_000 + SWITCH_UNREACHABLE_AFTER_MS)).is_empty());
        let raise = Command::RaiseAlert {
            id: "switchdown-a-3-100000".into(),
            severity: Severity::Critical,
            kind: SWITCH_UNREACHABLE.into(),
            entity: ObjectRef::switch(&sid("a")),
            message: "a did not answer polls for 90 s".into(),
            ground_truth: false,
        };
        assert_eq!(alert_kinds(run(&s, &v, 100_000 + SWITCH_UNREACHABLE_AFTER_MS + 1)), std::slice::from_ref(&raise));
        s.apply(0, raise).unwrap();
        assert!(alert_kinds(run(&s, &v, 200_000)).is_empty(), "one alert per incident");
        assert!(alert_kinds(run(&s, &v, 400_000)).is_empty(), "the election grace is not a report");

        v.switch_reports.insert(sid("a"), reading("a", 410_000, 1));
        assert_eq!(alert_kinds(run(&s, &v, 411_000)), [Command::ResolveAlert { id: "switchdown-a-3-100000".into() }]);
        s.apply(411_000, Command::ResolveAlert { id: "switchdown-a-3-100000".into() }).unwrap();
        assert!(alert_kinds(run(&s, &v, 412_000)).is_empty());
        // Silent again: a new incident, a new alert id.
        let later = alert_kinds(run(&s, &v, 410_000 + SWITCH_UNREACHABLE_AFTER_MS + 1));
        assert!(matches!(later.as_slice(), [Command::RaiseAlert { id, .. }] if id == "switchdown-a-3-410000"), "{later:?}");
    }

    #[test]
    fn a_switch_heard_of_only_through_lldp_can_go_unreachable() {
        let s = state_with_hosts();
        let v = Volatile::new(0, 1);
        let raised = alert_kinds(run(&s, &v, SWITCH_UNREACHABLE_AFTER_MS + 1));
        assert_eq!(raised.len(), 3, "access-1, access-2 and core-1 never answered: {raised:?}");
    }
}
