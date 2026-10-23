//! Port alerts, from what each host's agent reports about its NICs (the
//! inventory in the host object) and the network config it runs:
//!
//! * `PortDown`: a port one of the host's links uses has no link.
//! * `BondDegraded`: a bond has fewer than all of its members up.
//! * `LinkSpeedLow`: a NIC with link negotiated below its profile speed.
//!
//! One alert per (kind, host, port or link), raised while the condition
//! holds and resolved once it clears (or the host is removed). Ids are
//! stable across controller runs and unique per incident: the leader's term
//! and when the agent saw its observation change.

use super::network::managed;
use super::{Action, Volatile};
use crate::state::{Alert, ClusterState, Command, HostObj, ObjectKind, ObjectRef, Severity};
use model::{HostId, LinkKind};
use std::collections::BTreeMap;

pub const PORT_DOWN: &str = "PortDown";
pub const BOND_DEGRADED: &str = "BondDegraded";
pub const LINK_SPEED_LOW: &str = "LinkSpeedLow";

pub fn run(state: &ClusterState, v: &Volatile) -> Vec<Action> {
    let mut open: BTreeMap<HostId, Vec<&Alert>> = BTreeMap::new();
    let ours = |a: &&Alert| {
        [PORT_DOWN, BOND_DEGRADED, LINK_SPEED_LOW].contains(&a.kind.as_str())
            && a.resolved_at.is_none()
            && a.entity.kind == ObjectKind::Host
    };
    for a in state.alerts.values().filter(ours) {
        open.entry(HostId::new(&a.entity.id)).or_default().push(a);
    }
    let mut out = vec![];
    for host in state.hosts.values() {
        let wanted = if managed(host.lifecycle) { conditions(host) } else { vec![] };
        let open = open.remove(&host.id).unwrap_or_default();
        out.extend(reconcile(state, host, v, &wanted, &open));
    }
    // Alerts of hosts that are gone altogether.
    for a in open.into_values().flatten() {
        out.push(Action::Propose(Command::ResolveAlert { id: a.id.clone() }));
    }
    out
}

/// One alertable condition on a host: what kind, its id prefix (which names
/// the host and port or link) and the message.
struct Condition {
    kind: &'static str,
    prefix: String,
    message: String,
}

/// What is wrong with `host`'s ports now, as its inventory and the network
/// config it runs (or is to run, before any applied) say.
fn conditions(host: &HostObj) -> Vec<Condition> {
    let id = &host.id;
    let condition = |kind: &'static str, slug: &str, subject: &str, message: String| Condition {
        kind,
        prefix: format!("{slug}-{id}-{subject}-"),
        message,
    };
    let nic = |name: &str| host.inventory.nics.iter().find(|n| n.name == name);
    let mut out = vec![];
    if let Some(cfg) = host.net.applied.as_ref().or(host.net.desired.as_ref()) {
        for link in &cfg.links {
            let ports = link.kind.ports();
            let down: Vec<_> = ports.iter().filter(|p| nic(p.as_str()).is_some_and(|n| !n.link_up)).collect();
            for p in &down {
                out.push(condition(PORT_DOWN, "portdown", p.as_str(), format!("{id} {p}: link down")));
            }
            if matches!(link.kind, LinkKind::Bond { .. }) && !down.is_empty() {
                let message = format!("{id} {}: {} of {} members up", link.name, ports.len() - down.len(), ports.len());
                out.push(condition(BOND_DEGRADED, "bonddegraded", &link.name, message));
            }
        }
    }
    for n in host.inventory.nics.iter().filter(|n| n.link_up) {
        let Some(expected) = host.profile.nics.iter().find(|p| p.name == n.name).map(|p| p.speed_mbps) else { continue };
        if n.speed_mbps < expected {
            let message = format!("{id} {}: {} Mbps, expected {expected} Mbps", n.name, n.speed_mbps);
            out.push(condition(LINK_SPEED_LOW, "linkspeedlow", &n.name, message));
        }
    }
    out
}

/// Resolves the host's open alerts no condition wants any more and raises
/// one for each condition with neither an open alert nor a raise in flight.
/// A new alert's id ends in the leader's term and when the agent saw its
/// observation change (the election, if this leader has no report yet),
/// plus a counter if a condition recurs without a new observation (e.g. the
/// host's config dropped the port and took it back).
fn reconcile(state: &ClusterState, host: &HostObj, v: &Volatile, wanted: &[Condition], open: &[&Alert]) -> Vec<Action> {
    let mut out: Vec<Action> = open
        .iter()
        .filter(|a| !wanted.iter().any(|c| a.kind == c.kind && a.id.starts_with(&c.prefix)))
        .map(|a| Action::Propose(Command::ResolveAlert { id: a.id.clone() }))
        .collect();
    let stamp = v.host_reports.get(&host.id).map_or(v.elected_at, |r| r.observed_at);
    for c in wanted {
        let is_open = open.iter().any(|a| a.kind == c.kind && a.id.starts_with(&c.prefix));
        let in_flight =
            v.in_flight.values().any(|cmd| matches!(cmd, Command::RaiseAlert { id, .. } if id.starts_with(&c.prefix)));
        if !is_open && !in_flight {
            let base = format!("{}{}-{stamp}", c.prefix, v.term);
            let id = (1..)
                .map(|n| if n == 1 { base.clone() } else { format!("{base}-{n}") })
                .find(|id| !state.alerts.contains_key(id))
                .expect("some id is free");
            out.push(Action::Propose(Command::RaiseAlert {
                id,
                severity: Severity::Warning,
                kind: c.kind.into(),
                entity: ObjectRef::host(&host.id),
                message: c.message.clone(),
                ground_truth: false,
            }));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::HostReport;
    use crate::proto::Observed;
    use crate::state::{Lifecycle, NetworkResult};
    use model::{profile, seed_templates, Inventory, ProfileId, Role};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    /// MS-01 `m`, Ready, running `ms01-standard` (bond0 = sfp0 + sfp1, eth0 for mgmt).
    fn state() -> ClusterState {
        let mut s = ClusterState::new();
        let id = hid("m");
        let p = ProfileId::Ms01;
        let config = seed_templates().into_iter().find(|t| t.id == "ms01-standard").unwrap().config;
        let cmds = [
            Command::AddPendingHost { id: id.clone(), profile: profile(p, 0), inventory: Inventory::from_profile(p, 0), location: "rack-0".into() },
            Command::ApproveHost { id: id.clone(), roles: [Role::Compute].into() },
            Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Provisioning },
            Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Ready },
            Command::SetDesiredNetwork { id: id.clone(), config },
            Command::RecordNetworkResult { id, generation: 1, result: NetworkResult::Applied },
        ];
        for c in cmds {
            s.apply(0, c).unwrap();
        }
        s
    }

    /// The agent of `m` reported, at `observed_at`, its inventory as changed by `edit`.
    fn observe(s: &mut ClusterState, v: &mut Volatile, observed_at: u64, edit: impl FnOnce(&mut Inventory)) {
        let mut inventory = Inventory::from_profile(ProfileId::Ms01, 0);
        edit(&mut inventory);
        let observed = Observed { lldp: vec![], inventory: inventory.clone() };
        v.host_reports.insert(hid("m"), HostReport { observed_at, observed });
        s.apply(observed_at, Command::ObserveHost { id: hid("m"), lldp: vec![], inventory }).unwrap();
    }

    fn nic<'a>(inv: &'a mut Inventory, name: &str) -> &'a mut model::NicState {
        inv.nics.iter_mut().find(|n| n.name == name).unwrap()
    }

    fn proposals(actions: Vec<Action>) -> Vec<Command> {
        actions.into_iter().map(|a| if let Action::Propose(c) = a { c } else { panic!("unexpected {a:?}") }).collect()
    }

    fn raise(id: &str, kind: &str, message: &str) -> Command {
        Command::RaiseAlert {
            id: id.into(),
            severity: Severity::Warning,
            kind: kind.into(),
            entity: ObjectRef::host(&hid("m")),
            message: message.into(),
            ground_truth: false,
        }
    }

    fn apply_all(s: &mut ClusterState, at: u64, cmds: Vec<Command>) {
        for c in cmds {
            s.apply(at, c).unwrap();
        }
    }

    #[test]
    fn a_down_bond_member_raises_port_down_and_bond_degraded_until_it_is_back() {
        let mut s = state();
        let mut v = Volatile::new(0, 2);
        assert!(run(&s, &v).is_empty(), "everything is up");

        observe(&mut s, &mut v, 7000, |inv| nic(inv, "sfp0").link_up = false);
        let raised = proposals(run(&s, &v));
        assert_eq!(raised, [
            raise("portdown-m-sfp0-2-7000", PORT_DOWN, "m sfp0: link down"),
            raise("bonddegraded-m-bond0-2-7000", BOND_DEGRADED, "m bond0: 1 of 2 members up"),
        ]);
        assert_eq!(proposals(run(&s, &v)), raised, "the same commands until they apply");
        apply_all(&mut s, 8000, raised);
        assert!(run(&s, &v).is_empty(), "one alert per incident");

        observe(&mut s, &mut v, 20_000, |_| {});
        let resolved = proposals(run(&s, &v));
        assert_eq!(resolved, [
            Command::ResolveAlert { id: "bonddegraded-m-bond0-2-7000".into() },
            Command::ResolveAlert { id: "portdown-m-sfp0-2-7000".into() },
        ]);
        apply_all(&mut s, 21_000, resolved);
        assert!(run(&s, &v).is_empty());

        // Both members down: a new incident, new ids.
        observe(&mut s, &mut v, 50_000, |inv| {
            nic(inv, "sfp0").link_up = false;
            nic(inv, "sfp1").link_up = false;
        });
        assert_eq!(proposals(run(&s, &v)), [
            raise("portdown-m-sfp0-2-50000", PORT_DOWN, "m sfp0: link down"),
            raise("portdown-m-sfp1-2-50000", PORT_DOWN, "m sfp1: link down"),
            raise("bonddegraded-m-bond0-2-50000", BOND_DEGRADED, "m bond0: 0 of 2 members up"),
        ]);
    }

    #[test]
    fn a_port_no_link_uses_is_not_alerted() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        observe(&mut s, &mut v, 1000, |inv| nic(inv, "eth1").link_up = false);
        assert!(run(&s, &v).is_empty(), "eth1 is not in the host's config");
    }

    #[test]
    fn link_speed_low_while_a_nic_negotiates_below_its_profile_speed() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        observe(&mut s, &mut v, 3000, |inv| {
            nic(inv, "sfp1").speed_mbps = 1000;
            // A NIC without link has no speed worth judging.
            let eth1 = nic(inv, "eth1");
            eth1.speed_mbps = 100;
            eth1.link_up = false;
        });
        let raised = proposals(run(&s, &v));
        assert_eq!(raised, [raise("linkspeedlow-m-sfp1-1-3000", LINK_SPEED_LOW, "m sfp1: 1000 Mbps, expected 10000 Mbps")]);
        apply_all(&mut s, 3000, raised);
        assert!(run(&s, &v).is_empty());
        observe(&mut s, &mut v, 9000, |_| {});
        assert_eq!(proposals(run(&s, &v)), [Command::ResolveAlert { id: "linkspeedlow-m-sfp1-1-3000".into() }]);
    }

    #[test]
    fn a_raise_in_flight_is_not_repeated_under_a_new_stamp() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        observe(&mut s, &mut v, 7000, |inv| nic(inv, "sfp0").link_up = false);
        v.in_flight.insert(9, raise("portdown-m-sfp0-1-7000", PORT_DOWN, "m sfp0: link down"));
        // The observation changed again before the raise applied.
        observe(&mut s, &mut v, 7500, |inv| nic(inv, "sfp0").link_up = false);
        assert_eq!(proposals(run(&s, &v)), [raise("bonddegraded-m-bond0-1-7500", BOND_DEGRADED, "m bond0: 1 of 2 members up")]);
    }

    #[test]
    fn without_a_report_the_incident_dates_from_the_election() {
        let mut s = state();
        let mut v = Volatile::new(40_000, 3);
        observe(&mut s, &mut v, 7000, |inv| nic(inv, "sfp0").link_up = false);
        v.host_reports.clear();
        let ids: Vec<String> =
            proposals(run(&s, &v)).into_iter().filter_map(|c| if let Command::RaiseAlert { id, .. } = c { Some(id) } else { None }).collect();
        assert_eq!(ids, ["portdown-m-sfp0-3-40000", "bonddegraded-m-bond0-3-40000"]);
    }

    #[test]
    fn a_condition_back_without_a_new_observation_gets_a_fresh_id() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        observe(&mut s, &mut v, 7000, |inv| nic(inv, "eth0").link_up = false);
        let raised = proposals(run(&s, &v));
        assert_eq!(raised, [raise("portdown-m-eth0-1-7000", PORT_DOWN, "m eth0: link down")]);
        apply_all(&mut s, 7000, raised);
        // The config stops using eth0, then uses it again; the inventory never changed.
        let mut cfg = s.hosts[&hid("m")].net.desired.clone().unwrap();
        let eth0 = cfg.links.pop().unwrap();
        for c in [cfg.clone(), { cfg.links.push(eth0); cfg }] {
            s.apply(8000, Command::SetDesiredNetwork { id: hid("m"), config: c }).unwrap();
            let generation = s.hosts[&hid("m")].net.desired_generation;
            s.apply(8000, Command::RecordNetworkResult { id: hid("m"), generation, result: NetworkResult::Applied }).unwrap();
            let cmds = proposals(run(&s, &v));
            apply_all(&mut s, 8000, cmds);
        }
        assert!(s.alerts["portdown-m-eth0-1-7000"].resolved_at.is_some());
        assert!(s.alerts["portdown-m-eth0-1-7000-2"].resolved_at.is_none());
    }

    #[test]
    fn alerts_of_removed_hosts_are_resolved() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        observe(&mut s, &mut v, 7000, |inv| nic(inv, "sfp0").link_up = false);
        let raised = proposals(run(&s, &v));
        apply_all(&mut s, 7000, raised);
        s.apply(8000, Command::RemoveHost { id: hid("m") }).unwrap();
        assert_eq!(proposals(run(&s, &v)), [
            Command::ResolveAlert { id: "bonddegraded-m-bond0-1-7000".into() },
            Command::ResolveAlert { id: "portdown-m-sfp0-1-7000".into() },
        ]);
    }
}
