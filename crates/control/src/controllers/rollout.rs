//! Rollout: an edited template or logical network reaches the hosts it
//! affects a batch at a time (design §6).
//!
//! - [`preview`] shows what the change would do, on a copy of the state:
//!   the hosts it affects, those with an override (skipped unless included)
//!   and those not ready (skipped), what validating the new configs finds,
//!   and the switch-side changes needed.
//! - `StartRollout` applies the change and pins every concerned host to its
//!   current config until its batch starts ([`concerned`] decides, at apply time).
//! - [`run`] drives the rollout that is running or soaking: once every host
//!   of the current batch applied a generation newer than when its batch
//!   started, the batch soaks, then the next one starts. A host that failed
//!   or reverted it, or that is not ready, pauses the rollout. The rollout
//!   lives in the log, so a new leader picks it up at the same batch.

use super::network::{address_owners, managed, render_fresh, render_host, suggestion, validate_config};
use super::Action;
use crate::runtime::SimTime;
use crate::state::{Change, ClusterState, Command, Lifecycle, NetState, Rollout, RolloutState};
use model::network::LogicalNetwork;
use model::{HostId, Issue};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ts_rs::TS;

pub const DEFAULT_BATCH_PCT: u8 = 10;
pub const DEFAULT_SOAK_S: u32 = 30;

/// What a change would do, before it is rolled out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Preview {
    /// Ready hosts whose config the change alters, without an override.
    pub affected: Vec<HostId>,
    /// Hosts the change concerns that have an override: skipped unless included.
    pub overridden: Vec<HostId>,
    /// Hosts whose config the change alters that are not ready: skipped.
    pub skipped: Vec<HostId>,
    /// What validating each affected host's new config finds, for hosts with issues.
    pub issues: BTreeMap<HostId, Vec<Issue>>,
    /// The switch port changes those issues call for, each once.
    pub switch_changes: Vec<String>,
}

/// Hosts per batch: `batch_pct` percent of `affected`, rounded up, at least one.
pub fn batch_size(affected: usize, batch_pct: u8) -> usize {
    (affected * batch_pct as usize).div_ceil(100).max(1)
}

/// The state as it would be with `change` applied.
fn with_change(state: &ClusterState, change: &Change) -> ClusterState {
    let mut after = state.clone();
    let cmd = match change.clone() {
        Change::Template(template) => Command::UpsertTemplate { template },
        Change::Network(network) => Command::UpsertNetwork { network },
    };
    after.apply(0, cmd).expect("an upsert always applies");
    after
}

/// The managed hosts a change concerns, each list in id order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Concerned {
    /// Without an override, and whose config the change alters.
    pub affected: Vec<HostId>,
    /// With an override.
    pub overridden: Vec<HostId>,
}

/// The managed hosts `change` concerns. A template concerns the hosts
/// provisioned on it; a network, the hosts whose config attaches it, unless
/// the network is unchanged.
pub fn concerned(state: &ClusterState, change: &Change) -> Concerned {
    let after = with_change(state, change);
    let mut c = Concerned { affected: vec![], overridden: vec![] };
    for host in state.hosts.values().filter(|h| managed(h.lifecycle)) {
        let concerned = match change {
            Change::Template(t) => {
                host.template.as_deref() == Some(t.id.as_str())
                    && (host.override_.is_some() || render_fresh(state, host) != render_fresh(&after, &after.hosts[&host.id]))
            }
            Change::Network(n) => {
                state.networks.get(&n.id).map(|o| &o.network) != Some(n)
                    && render_fresh(state, host).attachments.iter().any(|a| a.network == n.id)
            }
        };
        match (concerned, host.override_.is_some()) {
            (false, _) => {}
            (true, false) => c.affected.push(host.id.clone()),
            (true, true) => c.overridden.push(host.id.clone()),
        }
    }
    c
}

/// What `change` would do, judged on a copy of the state with it applied.
/// Hosts whose cabling was never observed are not judged.
pub fn preview(state: &ClusterState, change: &Change) -> Preview {
    let Concerned { affected, overridden } = concerned(state, change);
    let ready = |h: &HostId| state.hosts[h].lifecycle != Lifecycle::NotReady;
    let (affected, skipped): (Vec<HostId>, Vec<HostId>) = affected.into_iter().partition(ready);
    let after = with_change(state, change);
    let networks: Vec<LogicalNetwork> = after.networks.values().map(|n| n.network.clone()).collect();
    let owners = address_owners(&after);
    let mut issues = BTreeMap::new();
    let mut switch_changes: Vec<String> = vec![];
    for host in affected.iter().map(|h| &after.hosts[h]).filter(|h| !h.lldp.is_empty()) {
        let found = validate_config(&after, host, &render_fresh(&after, host), &networks, &owners);
        for issue in found.iter().filter(|i| switch_side(i)) {
            let change = suggestion(&after, &host.id, issue);
            if !switch_changes.contains(&change) {
                switch_changes.push(change);
            }
        }
        if !found.is_empty() {
            issues.insert(host.id.clone(), found);
        }
    }
    Preview { affected, overridden, skipped, issues, switch_changes }
}

/// Whether fixing `issue` means changing a switch port.
fn switch_side(issue: &Issue) -> bool {
    match issue {
        Issue::VlanNotAllowed { .. } | Issue::NativeVlanMismatch { .. } | Issue::LacpNotConfigured { .. } => true,
        Issue::MtuTooSmall { where_, .. } => !where_.starts_with("link "),
        _ => false,
    }
}

pub fn run(state: &ClusterState, now: SimTime) -> Vec<Action> {
    let active = |r: &&Rollout| matches!(r.state, RolloutState::Running | RolloutState::Soaking { .. });
    let Some(r) = state.rollouts.values().find(active) else { return vec![] };
    let mut out = vec![];
    let (mut failed, mut not_ready) = (vec![], vec![]);
    let mut applied = true;
    // Hosts gone, no longer managed or skipped have nothing to apply.
    let batch = r.batch(r.current_batch).iter().filter(|h| !r.skipped.contains(h)).filter_map(|h| state.hosts.get(h));
    for host in batch.filter(|h| managed(h.lifecycle)) {
        if host.lifecycle == Lifecycle::NotReady {
            not_ready.push(host.id.clone());
            continue;
        }
        let base = r.base_generation.get(&host.id).copied().unwrap_or(0);
        if host.net.desired_generation <= base {
            // Usually the network controller asks for the same command, as the
            // pin is gone. If the config renders as before (a network change),
            // this is what pushes it again.
            out.push(Action::Propose(Command::SetDesiredNetwork { id: host.id.clone(), config: render_host(state, host) }));
            applied = false;
            continue;
        }
        match host.net.state {
            NetState::Applied => {}
            NetState::Failed | NetState::Reverted => failed.push(host.id.clone()),
            NetState::Pending | NetState::Applying => applied = false,
        }
    }
    let id = r.id.clone();
    let batch = r.current_batch;
    let reason = |hosts: &[HostId], what: &str| {
        let names: Vec<String> = hosts.iter().map(ToString::to_string).collect();
        format!("batch {} of {}: {} {what}", batch + 1, r.batches(), names.join(", "))
    };
    let next = if !failed.is_empty() {
        Some(Command::PauseRollout { id: id.clone(), reason: reason(&failed, "failed or reverted"), failed })
    } else if !not_ready.is_empty() {
        Some(Command::PauseRollout { id: id.clone(), reason: reason(&not_ready, "not ready"), failed: vec![] })
    } else {
        match r.state {
            RolloutState::Running if applied => Some(Command::SoakRollout { id: id.clone(), batch }),
            RolloutState::Soaking { until } if now >= until => Some(Command::AdvanceRollout { id: id.clone(), batch }),
            _ => None,
        }
    };
    out.extend(next.map(Action::Propose));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::{network, Volatile};
    use crate::state::{ApplyError, Lifecycle, NetworkResult};
    use model::network::{LldpNeighbor, ObservedSwitchPort, PortMode, SwitchPortCfg};
    use model::{profile, seed_networks, seed_templates, Inventory, NetworkId, NetworkTemplate, PortName, ProfileId, Role, SwitchId};
    use std::net::Ipv4Addr;

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn template(id: &str) -> NetworkTemplate {
        seed_templates().into_iter().find(|t| t.id == id).unwrap()
    }

    fn minipc_mtu(mtu: u16) -> NetworkTemplate {
        let mut t = template("minipc-standard");
        t.config.links[0].mtu = mtu;
        t
    }

    /// Mini PCs `m1`..=`m<n>` on `minipc-standard` and one Pi 5 `p1` on
    /// `sbc-transcode`, each cabled to its own port of access-1, which is a
    /// trunk allowing VLANs 1 and 20. Every host runs its rendered config.
    fn state(n: usize) -> ClusterState {
        let mut s = ClusterState::new();
        for network in seed_networks() {
            s.apply(0, Command::UpsertNetwork { network }).unwrap();
        }
        for template in seed_templates() {
            s.apply(0, Command::UpsertTemplate { template }).unwrap();
        }
        let mut ports = vec![];
        let hosts = (1..=n).map(|i| (format!("m{i}"), ProfileId::MiniPc, "minipc-standard")).chain([("p1".into(), ProfileId::Pi5, "sbc-transcode")]);
        for (i, (id, p, t)) in hosts.enumerate() {
            let port = PortName::new(format!("p{}", i + 1));
            let trunk = SwitchPortCfg { mode: PortMode::Trunk { native: Some(1), allowed: [1, 20].into() }, mtu: 9216, lacp_group: None, admin_up: true };
            ports.push(ObservedSwitchPort { switch: SwitchId::from("access-1"), port: port.clone(), cfg: trunk, mlag_peer: None, link_up: true, speed_mbps: 1000, neighbor: None });
            let inventory = Inventory::from_profile(p, 1);
            s.apply(0, Command::AddPendingHost { id: hid(&id), profile: profile(p, 1), inventory: inventory.clone(), location: "rack-1".into() }).unwrap();
            s.apply(0, Command::ApproveHost { id: hid(&id), roles: [Role::Compute].into() }).unwrap();
            let ips: BTreeMap<NetworkId, Ipv4Addr> = [(NetworkId::from("mgmt"), Ipv4Addr::new(10, 0, 0, 10 + i as u8))].into();
            s.apply(0, Command::ProvisionHost { id: hid(&id), template: Some(t.into()), ips }).unwrap();
            let lldp = vec![LldpNeighbor { local_port: PortName::from("eth0"), switch: SwitchId::from("access-1"), switch_port: port }];
            s.apply(0, Command::ObserveHost { id: hid(&id), lldp, inventory }).unwrap();
            s.apply(0, Command::SetLifecycle { id: hid(&id), lifecycle: Lifecycle::Ready }).unwrap();
            let config = network::render_host(&s, &s.hosts[&hid(&id)]);
            s.apply(0, Command::SetDesiredNetwork { id: hid(&id), config }).unwrap();
            s.apply(0, Command::RecordNetworkResult { id: hid(&id), generation: 1, result: NetworkResult::Applied }).unwrap();
        }
        s.apply(0, Command::ObserveSwitch { id: SwitchId::from("access-1"), model: "m".into(), ports, mlag_peer: None, last_seen: 0 }).unwrap();
        s
    }

    /// One controller period of the network and rollout controllers, with
    /// their proposals applied (identical ones once, as the replica does).
    /// Agents answer every push with `answer`; `None` is an agent that is down.
    fn step(s: &mut ClusterState, v: &mut Volatile, now: SimTime, answer: impl Fn(&HostId) -> Option<NetworkResult>) {
        let mut out = network::run(s, v, now);
        out.extend(run(s, now));
        let mut done: Vec<Command> = vec![];
        for a in out {
            match a {
                Action::Propose(c) if !done.contains(&c) => {
                    done.push(c.clone());
                    // A duplicate rollout step finds the rollout moved on; nothing else may fail.
                    match s.apply(now, c.clone()) {
                        Ok(()) | Err(ApplyError::InvalidRolloutState) => {}
                        Err(e) => panic!("{c:?} failed: {e}"),
                    }
                }
                Action::Propose(_) => {}
                Action::Send(to, _, msg) => {
                    v.note_sent(&to, &msg, now);
                    if let (crate::runtime::Addr::Host(h), crate::proto::Msg::ApplyNetwork { generation, .. }) = (&to.addr, &msg)
                        && let Some(result) = answer(h)
                    {
                        v.net_results.insert(h.clone(), (*generation, result));
                    }
                }
            }
        }
    }

    fn start(id: &str, change: Change, include_overridden: bool, batch_pct: u8) -> Command {
        Command::StartRollout { id: id.into(), change, include_overridden, batch_pct, soak_ms: 30_000 }
    }

    fn rollout(s: &ClusterState) -> &Rollout {
        s.rollouts.values().next().expect("a rollout")
    }

    fn mtu(s: &ClusterState, h: &str) -> u16 {
        s.hosts[&hid(h)].net.desired.as_ref().unwrap().links[0].mtu
    }

    fn applied_mtu(s: &ClusterState, h: &str) -> u16 {
        s.hosts[&hid(h)].net.applied.as_ref().unwrap().links[0].mtu
    }

    fn ok(_: &HostId) -> Option<NetworkResult> {
        Some(NetworkResult::Applied)
    }

    #[test]
    fn batches_are_a_tenth_rounded_up() {
        assert_eq!([(24, 10), (12, 10), (1, 10), (0, 10), (5, 100), (7, 50)].map(|(n, p)| batch_size(n, p)), [3, 2, 1, 1, 5, 4]);
    }

    #[test]
    fn affected_hosts_are_the_ones_whose_config_changes() {
        let mut s = state(3);
        s.apply(0, Command::SetHostOverride { host: hid("m2"), config: Some(template("minipc-standard").config) }).unwrap();
        let affected = |s: &ClusterState, change| {
            let c = concerned(s, &change);
            (c.affected, c.overridden)
        };
        assert_eq!(affected(&s, Change::Template(minipc_mtu(1500))), (vec![hid("m1"), hid("m3")], vec![hid("m2")]));
        assert_eq!(affected(&s, Change::Template(template("minipc-standard"))), (vec![], vec![hid("m2")]), "unchanged");

        // A network concerns every host attaching it, unless it is unchanged.
        let mut storage = seed_networks().into_iter().find(|n| n.id.as_str() == "storage").unwrap();
        assert_eq!(affected(&s, Change::Network(storage.clone())), (vec![], vec![]));
        storage.vlan = 21;
        assert_eq!(affected(&s, Change::Network(storage)), (vec![hid("m1"), hid("m3")], vec![hid("m2")]));
        let mut mgmt = seed_networks().remove(0);
        mgmt.mtu = 1400;
        assert_eq!(affected(&s, Change::Network(mgmt)).0, [hid("m1"), hid("m3"), hid("p1")]);
    }

    #[test]
    fn preview_validates_without_side_effects() {
        let s = state(2);
        let mut t = template("minipc-standard");
        t.config.attachments.push(model::Attachment { network: NetworkId::from("migration"), link: "eth0".into(), tagged: true, ip: None });
        let before = s.version;
        let p = preview(&s, &Change::Template(t));
        assert_eq!(s.version, before);
        assert_eq!(s.templates["minipc-standard"].template, template("minipc-standard"));
        assert_eq!((p.affected, p.overridden), (vec![hid("m1"), hid("m2")], vec![]));
        assert_eq!(p.switch_changes, ["access-1 p1: add VLAN 30 to trunk allowed", "access-1 p2: add VLAN 30 to trunk allowed"]);
        assert!(matches!(p.issues[&hid("m1")].as_slice(), [Issue::VlanNotAllowed { vlan: 30, .. }]), "{:?}", p.issues);

        // Host-side problems are issues, but no switch change.
        let p = preview(&s, &Change::Template(minipc_mtu(1500)));
        assert!(matches!(p.issues[&hid("m1")].as_slice(), [Issue::MtuTooSmall { need: 9000, .. }]), "{:?}", p.issues);
        assert!(p.switch_changes.is_empty(), "{:?}", p.switch_changes);
    }

    #[test]
    fn a_rollout_applies_the_change_and_pins_hosts_until_their_batch() {
        let mut s = state(5);
        s.apply(100, start("r", Change::Template(minipc_mtu(1500)), false, 40)).unwrap();
        assert_eq!(s.templates["minipc-standard"].template, minipc_mtu(1500));
        let r = rollout(&s);
        assert_eq!((r.hosts.len(), r.batch_size, r.soak_ms), (5, 2, 30_000));
        assert_eq!(r.previous, Some(Change::Template(template("minipc-standard"))));
        assert_eq!((r.current_batch, &r.state, r.batches()), (0, &RolloutState::Running, 3));
        assert_eq!(r.base_generation, [(hid("m1"), 1), (hid("m2"), 1)].into());
        assert!(s.hosts[&hid("m1")].net.pinned_config.is_none());
        assert_eq!(s.hosts[&hid("m3")].net.pinned_config, s.hosts[&hid("m3")].net.desired);

        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 1000, ok);
        let mtus: Vec<u16> = ["m1", "m2", "m3", "m4", "m5"].map(|h| mtu(&s, h)).into();
        assert_eq!(mtus, [1500, 1500, 9000, 9000, 9000], "only the first batch renders the change");
        assert_eq!(s.hosts[&hid("m3")].net.desired_generation, 1);
    }

    #[test]
    fn batches_soak_then_advance_until_done() {
        let mut s = state(5);
        s.apply(0, start("r", Change::Template(minipc_mtu(1500)), false, 40)).unwrap();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 1000, ok); // render
        step(&mut s, &mut v, 2000, ok); // push; the agents answer
        step(&mut s, &mut v, 3000, ok); // record
        assert_eq!(rollout(&s).state, RolloutState::Running);
        step(&mut s, &mut v, 4000, ok); // all applied: soak
        assert_eq!(rollout(&s).state, RolloutState::Soaking { until: 34_000 });
        assert_eq!((applied_mtu(&s, "m1"), applied_mtu(&s, "m2"), applied_mtu(&s, "m3")), (1500, 1500, 9000));
        step(&mut s, &mut v, 33_999, ok);
        assert_eq!(rollout(&s).current_batch, 0, "still soaking");
        step(&mut s, &mut v, 34_000, ok);
        assert_eq!((rollout(&s).current_batch, &rollout(&s).state), (1, &RolloutState::Running));
        assert!(s.hosts[&hid("m3")].net.pinned_config.is_none() && s.hosts[&hid("m5")].net.pinned_config.is_some());
        let mut now = 34_000;
        while rollout(&s).state != RolloutState::Done && now < 200_000 {
            now += 1000;
            step(&mut s, &mut v, now, ok);
        }
        assert_eq!(rollout(&s).state, RolloutState::Done, "{:?}", rollout(&s));
        assert!(["m1", "m2", "m3", "m4", "m5"].iter().all(|h| applied_mtu(&s, h) == 1500));
        assert_eq!(mtu(&s, "p1"), 1500, "another template's host is untouched");
        assert_eq!(s.hosts[&hid("p1")].net.desired_generation, 1);
        assert!(s.hosts.values().all(|h| h.net.pinned_config.is_none()));
    }

    #[test]
    fn a_failure_pauses_and_resume_retries_the_failed_hosts() {
        let mut s = state(5);
        s.apply(0, start("r", Change::Template(minipc_mtu(1500)), false, 40)).unwrap();
        let mut v = Volatile::new(0, 1);
        let m2_reverts = |h: &HostId| Some(if *h == hid("m2") { NetworkResult::Reverted } else { NetworkResult::Applied });
        for now in [1000, 2000, 3000, 4000] {
            step(&mut s, &mut v, now, m2_reverts);
        }
        let r = rollout(&s);
        assert_eq!(r.state, RolloutState::Paused { reason: "batch 1 of 3: m2 failed or reverted".into() });
        assert_eq!((r.failed.clone(), r.current_batch), (vec![hid("m2")], 0));
        for now in (5..60).map(|i| i * 1000) {
            step(&mut s, &mut v, now, ok);
        }
        assert_eq!(rollout(&s).current_batch, 0, "paused: no next batch");
        assert_eq!(s.hosts[&hid("m3")].net.desired_generation, 1, "the rest stay pinned");
        assert_eq!(s.hosts[&hid("m2")].net.desired_generation, 2, "the failed generation is not pushed again");

        // A pause or a second rollout is refused meanwhile.
        assert_eq!(s.apply(60_000, Command::PauseRollout { id: "r".into(), reason: "x".into(), failed: vec![] }), Err(ApplyError::InvalidRolloutState));
        let other = start("r2", Change::Template(template("sbc-transcode")), false, 10);
        let err = s.apply(60_000, other).unwrap_err();
        assert_eq!(err.to_string(), "rollout r in progress");

        s.apply(60_000, Command::ResumeRollout { id: "r".into() }).unwrap();
        assert!(rollout(&s).failed.is_empty());
        for now in [61_000, 62_000, 63_000, 64_000] {
            step(&mut s, &mut v, now, ok);
        }
        assert_eq!(s.hosts[&hid("m2")].net.desired_generation, 3, "retried with a new generation");
        assert_eq!(applied_mtu(&s, "m2"), 1500);
        assert!(matches!(rollout(&s).state, RolloutState::Soaking { .. }), "{:?}", rollout(&s));
    }

    #[test]
    fn a_network_change_pushes_each_batch_again() {
        let mut s = state(2);
        let mut storage = seed_networks().into_iter().find(|n| n.id.as_str() == "storage").unwrap();
        storage.mtu = 8000;
        s.apply(0, start("r", Change::Network(storage.clone()), false, 50)).unwrap();
        assert_eq!(s.networks[&NetworkId::from("storage")].network, storage);
        let mut v = Volatile::new(0, 1);
        for now in [1000, 2000, 3000, 4000] {
            step(&mut s, &mut v, now, ok);
        }
        let generation = |s: &ClusterState, h: &str| (s.hosts[&hid(h)].net.desired_generation, s.hosts[&hid(h)].net.applied_generation);
        assert_eq!((generation(&s, "m1"), generation(&s, "m2")), ((2, 2), (1, 1)));
        assert!(matches!(rollout(&s).state, RolloutState::Soaking { .. }));
    }

    #[test]
    fn included_overridden_hosts_move_to_the_template() {
        let mut s = state(2);
        let mut ov = template("minipc-standard").config;
        ov.links[0].mtu = 9100;
        s.apply(0, Command::SetHostOverride { host: hid("m2"), config: Some(ov) }).unwrap();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0, ok);
        assert_eq!(mtu(&s, "m2"), 9100);
        s.apply(1000, start("r", Change::Template(minipc_mtu(1500)), true, 50)).unwrap();
        assert_eq!(rollout(&s).hosts, [hid("m1"), hid("m2")]);
        assert_eq!(s.hosts[&hid("m2")].override_, None);
        assert_eq!(rollout(&s).dropped_overrides.keys().collect::<Vec<_>>(), [&hid("m2")]);
        step(&mut s, &mut v, 2000, ok);
        assert_eq!(mtu(&s, "m2"), 9100, "pinned until its batch");
        let mut now = 2000;
        while rollout(&s).state != RolloutState::Done && now < 100_000 {
            now += 1000;
            step(&mut s, &mut v, now, ok);
        }
        assert_eq!(applied_mtu(&s, "m2"), 1500);
    }

    #[test]
    fn an_override_set_mid_rollout_replaces_the_pin() {
        let mut s = state(2);
        s.apply(0, start("r", Change::Template(minipc_mtu(1500)), false, 50)).unwrap();
        // Clearing an override (here: none) leaves a waiting host waiting.
        s.apply(1, Command::SetHostOverride { host: hid("m2"), config: None }).unwrap();
        assert!(s.hosts[&hid("m2")].net.pinned_config.is_some());
        s.apply(1, Command::SetHostOverride { host: hid("m2"), config: Some(template("sbc-transcode").config) }).unwrap();
        assert!(s.hosts[&hid("m2")].net.pinned_config.is_none());
        assert_eq!(network::render_host(&s, &s.hosts[&hid("m2")]).links[0].mtu, 1500);
    }

    #[test]
    fn rollout_commands_check_state_and_batch() {
        let mut s = state(4);
        s.apply(0, start("r", Change::Template(minipc_mtu(1500)), false, 50)).unwrap();
        let cmd = |c: Command, s: &mut ClusterState| s.apply(10, c);
        assert_eq!(cmd(Command::AdvanceRollout { id: "r".into(), batch: 0 }, &mut s), Err(ApplyError::InvalidRolloutState), "not soaking");
        assert_eq!(cmd(Command::SoakRollout { id: "r".into(), batch: 1 }, &mut s), Err(ApplyError::InvalidRolloutState), "not that batch");
        assert_eq!(cmd(Command::SoakRollout { id: "x".into(), batch: 0 }, &mut s), Err(ApplyError::NotFound));
        cmd(Command::SoakRollout { id: "r".into(), batch: 0 }, &mut s).unwrap();
        assert_eq!(cmd(Command::SoakRollout { id: "r".into(), batch: 0 }, &mut s), Err(ApplyError::InvalidRolloutState), "only once");
        assert_eq!(cmd(Command::ResumeRollout { id: "r".into() }, &mut s), Err(ApplyError::InvalidRolloutState), "not paused");
        cmd(Command::PauseRollout { id: "r".into(), reason: "by user".into(), failed: vec![] }, &mut s).unwrap();
        cmd(Command::ResumeRollout { id: "r".into() }, &mut s).unwrap();
        assert_eq!(rollout(&s).state, RolloutState::Running);
        cmd(Command::SoakRollout { id: "r".into(), batch: 0 }, &mut s).unwrap();
        cmd(Command::AdvanceRollout { id: "r".into(), batch: 0 }, &mut s).unwrap();
        cmd(Command::SoakRollout { id: "r".into(), batch: 1 }, &mut s).unwrap();
        cmd(Command::AdvanceRollout { id: "r".into(), batch: 1 }, &mut s).unwrap();
        assert_eq!(rollout(&s).state, RolloutState::Done);
        // Done: a new rollout may start, and the old one stays as history.
        s.apply(20, start("r2", Change::Template(minipc_mtu(1400)), false, 50)).unwrap();
        assert_eq!(s.rollouts.len(), 2);
        // A rollout with no hosts is done at once.
        let mut t = state(1);
        t.apply(0, start("r3", Change::Template(template("ms01-standard")), false, 10)).unwrap();
        assert_eq!((rollout(&t).hosts.len(), &rollout(&t).state), (0, &RolloutState::Done));
        // The batch must be 1 to 100 percent.
        let mut t = state(1);
        for pct in [0, 101] {
            assert_eq!(t.apply(0, start("r", Change::Template(minipc_mtu(1500)), false, pct)), Err(ApplyError::InvalidBatchPct(pct)));
        }
        assert!(t.rollouts.is_empty() && t.templates["minipc-standard"].template == template("minipc-standard"));
    }

    #[test]
    fn hosts_are_chosen_and_pinned_when_the_rollout_applies() {
        let mut s = state(3);
        // m3 is down and m4 joins after the leader proposed: both decided at apply time.
        s.apply(0, Command::SetLifecycle { id: hid("m3"), lifecycle: Lifecycle::NotReady }).unwrap();
        let p = preview(&s, &Change::Template(minipc_mtu(1500)));
        assert_eq!((p.affected, p.skipped), (vec![hid("m1"), hid("m2")], vec![hid("m3")]));
        let cmd = start("r", Change::Template(minipc_mtu(1500)), false, 50);
        let mut later = state(4);
        later.apply(0, Command::SetLifecycle { id: hid("m3"), lifecycle: Lifecycle::NotReady }).unwrap();
        later.apply(0, cmd).unwrap();
        let r = rollout(&later);
        assert_eq!((r.hosts.clone(), r.skipped.clone()), (vec![hid("m1"), hid("m2"), hid("m4")], vec![hid("m3")]));
        assert!(later.hosts[&hid("m3")].net.pinned_config.is_some(), "a skipped host waits for the rollout to finish");
        // A host with no desired config yet is not pinned to nothing.
        let mut fresh = state(2);
        fresh.hosts.get_mut(&hid("m2")).unwrap().net = Default::default();
        fresh.apply(0, start("r", Change::Template(minipc_mtu(1500)), false, 50)).unwrap();
        assert!(fresh.hosts[&hid("m2")].net.pinned_config.is_none());
        assert_eq!(network::render_host(&fresh, &fresh.hosts[&hid("m2")]).links[0].mtu, 1500);
    }

    #[test]
    fn an_unready_host_pauses_and_resume_skips_it() {
        let mut s = state(4);
        s.apply(0, start("r", Change::Template(minipc_mtu(1500)), false, 50)).unwrap();
        s.apply(0, Command::SetLifecycle { id: hid("m2"), lifecycle: Lifecycle::NotReady }).unwrap();
        let mut v = Volatile::new(0, 1);
        let answer = |h: &HostId| (*h != hid("m2")).then_some(NetworkResult::Applied);
        for now in [1000, 2000, 3000, 4000] {
            step(&mut s, &mut v, now, answer);
        }
        let r = rollout(&s);
        assert_eq!(r.state, RolloutState::Paused { reason: "batch 1 of 2: m2 not ready".into() });
        assert!(r.failed.is_empty());
        s.apply(5000, Command::ResumeRollout { id: "r".into() }).unwrap();
        assert_eq!(rollout(&s).skipped, [hid("m2")]);
        assert_eq!(mtu(&s, "m2"), 1500, "rendered when its batch started");
        let mut now = 5000;
        while rollout(&s).state != RolloutState::Done && now < 200_000 {
            now += 1000;
            assert_eq!(network::render_host(&s, &s.hosts[&hid("m2")]).links[0].mtu, 9000, "pinned to what it last applied");
            step(&mut s, &mut v, now, answer);
        }
        assert_eq!(rollout(&s).state, RolloutState::Done);
        assert!(s.hosts[&hid("m2")].net.pinned_config.is_none(), "unpinned once the rollout is done");
    }

    #[test]
    fn abort_takes_the_change_back() {
        let mut s = state(4);
        let mut ov = template("minipc-standard").config;
        ov.links[0].mtu = 9100;
        s.apply(0, Command::SetHostOverride { host: hid("m4"), config: Some(ov.clone()) }).unwrap();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0, ok);
        step(&mut s, &mut v, 1, ok);
        s.apply(0, start("r", Change::Template(minipc_mtu(1500)), true, 50)).unwrap();
        let m1_reverts = |h: &HostId| Some(if *h == hid("m1") { NetworkResult::Reverted } else { NetworkResult::Applied });
        for now in [1000, 2000, 3000, 4000] {
            step(&mut s, &mut v, now, m1_reverts);
        }
        assert!(matches!(rollout(&s).state, RolloutState::Paused { .. }));
        s.apply(5000, Command::AbortRollout { id: "r".into() }).unwrap();
        assert_eq!(rollout(&s).state, RolloutState::Aborted);
        assert_eq!(s.templates["minipc-standard"].template, template("minipc-standard"));
        assert_eq!(s.hosts[&hid("m4")].override_, Some(ov));
        assert!(s.hosts.values().all(|h| h.net.pinned_config.is_none()));
        assert_eq!(s.apply(5000, Command::AbortRollout { id: "r".into() }), Err(ApplyError::InvalidRolloutState));
        for now in (6..20).map(|i| i * 1000) {
            step(&mut s, &mut v, now, ok);
        }
        let mtus: Vec<u16> = ["m1", "m2", "m3", "m4"].map(|h| applied_mtu(&s, h)).into();
        assert_eq!(mtus, [9000, 9000, 9000, 9100], "everyone is back on the old config");
        assert!(s.hosts.values().all(|h| h.net.state == NetState::Applied), "the reverted host too");
        s.apply(20_000, start("r2", Change::Template(minipc_mtu(1400)), false, 50)).unwrap();
    }
}
