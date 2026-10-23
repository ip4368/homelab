//! Network: keeps every provisioned host on its network configuration
//! (design §6). Each run, for each host:
//!
//! 1. **Render** its config: the override, else its template, else its
//!    profile's bootstrap config, with its addresses ([`render_host`]); a
//!    host a rollout has not reached yet keeps its pinned config instead. A
//!    config different from the stored desired one becomes the next
//!    generation (`SetDesiredNetwork`).
//! 2. **Push** the stored desired generation to the agent (`ApplyNetwork`)
//!    while the agent does not run it, at most every [`PUSH_RETRY_MS`]. The
//!    agent applies it on trial and reports over the new config; the replica
//!    confirms on receipt. A generation that failed or was reverted is not
//!    pushed again: the next change of the desired config is the next attempt.
//! 3. **Record** the agent's report for the desired generation.
//! 4. **Assess** the desired config (or, once that failed or was reverted,
//!    the applied one the host went back to) against the host's LLDP
//!    neighbours and the observed switch ports: issues, the changes that
//!    would fix them, held roles and drift are written when they change. A
//!    host whose cabling was never observed is not judged.
//! 5. **Alert**: one `NetworkMismatch` per host while it has issues, listing
//!    the fixes (replaced when the issues or fixes change), and a
//!    `NetworkApplyFailed` for a failed or reverted generation.

use super::{Action, Volatile};
use crate::proto::{mgmt, Msg};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{Alert, ClusterState, Command, HostObj, Lifecycle, NetState, NetworkResult, ObjectKind, ObjectRef, Severity};
use model::network::{bootstrap_config, HostNetworkConfig, LogicalNetwork, ObservedSwitchPort, PortMode};
use model::{effective_roles, render, unavailable_networks, validate_host, HostId, Issue, NetworkTemplate};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

pub const NETWORK_MISMATCH: &str = "NetworkMismatch";
pub const NETWORK_APPLY_FAILED: &str = "NetworkApplyFailed";
/// A push the agent did not act on (lost, or the agent was unreachable) is repeated this often.
pub const PUSH_RETRY_MS: u64 = 10_000;

pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let networks: Vec<LogicalNetwork> = state.networks.values().map(|n| n.network.clone()).collect();
    let owners = address_owners(state);
    let open = open_alerts(state);
    let mut out = vec![];
    for host in state.hosts.values().filter(|h| managed(h.lifecycle)) {
        let desired = render_host(state, host);
        if host.net.desired.as_ref() != Some(&desired) {
            out.push(Action::Propose(Command::SetDesiredNetwork { id: host.id.clone(), config: desired.clone() }));
        } else {
            out.extend(push(host, v, now));
        }
        out.extend(record(host, v));
        out.extend(assess(state, host, &desired, &networks, &owners));
        out.extend(alerts(state, host, open.get(&host.id).map_or(&[][..], Vec::as_slice)));
    }
    for (id, alerts) in &open {
        if state.hosts.get(id).is_none_or(|h| h.lifecycle == Lifecycle::Removed) {
            out.extend(alerts.iter().map(|a| Action::Propose(Command::ResolveAlert { id: a.id.clone() })));
        }
    }
    out
}

/// Hosts the controller manages: provisioned and not removed.
pub(crate) fn managed(lc: Lifecycle) -> bool {
    !matches!(lc, Lifecycle::Pending | Lifecycle::Approved | Lifecycle::Removed)
}

/// The config `host` should run: the one a rollout pinned it to until the
/// rollout reaches it, else [`render_fresh`]. Deterministic, so it is only a
/// new generation when an input changed.
pub fn render_host(state: &ClusterState, host: &HostObj) -> HostNetworkConfig {
    host.net.pinned_config.clone().unwrap_or_else(|| render_fresh(state, host))
}

/// The config the host's inputs render to, ignoring any rollout pin: its
/// override, else its template, else the bootstrap config of its profile,
/// with each attachment addressed from the host's `ips`.
pub fn render_fresh(state: &ClusterState, host: &HostObj) -> HostNetworkConfig {
    render(&base_template(state, host), host.override_.as_ref(), &host.ips)
}

/// What the host's config is derived from: its template, or its profile's bootstrap config.
fn base_template(state: &ClusterState, host: &HostObj) -> NetworkTemplate {
    match host.template.as_ref().and_then(|t| state.templates.get(t)) {
        Some(t) => t.template.clone(),
        None => NetworkTemplate {
            id: "bootstrap".into(),
            profile: host.profile.id,
            role: None,
            config: bootstrap_config(&host.profile),
        },
    }
}

/// `ApplyNetwork` for the stored desired generation, unless the agent runs
/// it (as far as its heartbeats, or else the log, say), it failed or was
/// reverted, or it was pushed less than [`PUSH_RETRY_MS`] ago. A generation
/// is pushed even if the agent runs it while it is unconfirmed: not yet
/// applied in the log, or pushed by this leader, and with no report here.
/// The agent may be on trial with its report lost (or sent to an earlier
/// leader); it answers again and is confirmed before its trial runs out.
fn push(host: &HostObj, v: &Volatile, now: SimTime) -> Option<Action> {
    let net = &host.net;
    let (generation, config) = (net.desired_generation, net.desired.as_ref()?);
    let report = v.net_results.get(&host.id).filter(|(g, _)| *g == generation).map(|(_, r)| r);
    let failed = matches!(net.state, NetState::Failed | NetState::Reverted) || report.is_some_and(|r| *r != NetworkResult::Applied);
    if failed {
        return None;
    }
    let running = v.net_generation.get(&host.id).copied().unwrap_or(net.applied_generation);
    let pushed_here = v.net_pushed.get(&host.id).is_some_and(|&(g, _)| g == generation);
    let unconfirmed = (net.state != NetState::Applied || pushed_here) && report.is_none();
    let recent = v.net_pushed.get(&host.id).is_some_and(|&(g, at)| g == generation && now < at + PUSH_RETRY_MS);
    ((running < generation || unconfirmed) && !recent).then(|| {
        let agent = Endpoint::new(Addr::Host(host.id.clone()), Svc::Agent);
        Action::Send(agent, mgmt(), Msg::ApplyNetwork { generation, config: config.clone() })
    })
}

/// The agent's report for the desired generation, if the log does not have it yet.
fn record(host: &HostObj, v: &Volatile) -> Option<Action> {
    let (generation, result) = v.net_results.get(&host.id).filter(|(g, _)| *g == host.net.desired_generation)?;
    let recorded = match result {
        NetworkResult::Applied => NetState::Applied,
        NetworkResult::Failed(_) => NetState::Failed,
        NetworkResult::Reverted => NetState::Reverted,
    } == host.net.state;
    (!recorded).then(|| {
        Action::Propose(Command::RecordNetworkResult { id: host.id.clone(), generation: *generation, result: result.clone() })
    })
}

/// Which hosts hold each address.
pub(crate) fn address_owners(state: &ClusterState) -> BTreeMap<Ipv4Addr, Vec<&HostId>> {
    let mut owners: BTreeMap<Ipv4Addr, Vec<&HostId>> = BTreeMap::new();
    for h in state.hosts.values() {
        for ip in h.ips.values() {
            owners.entry(*ip).or_default().push(&h.id);
        }
    }
    owners
}

/// `AssessNetwork` when validating the host's config finds other issues,
/// held roles or drift than the log has. The config is `desired`, unless
/// that failed or was reverted. A host with no LLDP neighbours was never
/// observed (a host with no cable up cannot report), so its issues are kept.
fn assess(
    state: &ClusterState,
    host: &HostObj,
    desired: &HostNetworkConfig,
    networks: &[LogicalNetwork],
    owners: &BTreeMap<Ipv4Addr, Vec<&HostId>>,
) -> Option<Action> {
    let net = &host.net;
    // A failed or reverted desired config is not what the host runs: judge
    // the one it went back to, so a host-side mistake holds no roles and
    // suggests no switch-side change.
    let failed = matches!(net.state, NetState::Failed | NetState::Reverted);
    let running = if failed { net.applied.as_ref().unwrap_or(desired) } else { desired };
    let (issues, fixes, held_roles) = if host.lldp.is_empty() {
        (net.issues.clone(), net.fixes.clone(), net.held_roles.clone())
    } else {
        let issues = validate_config(state, host, running, networks, owners);
        let (_, held) = effective_roles(&host.roles, &unavailable_networks(&issues));
        let mut fixes: Vec<String> = vec![];
        for fix in issues.iter().map(|i| suggestion(state, &host.id, i)) {
            if !fixes.contains(&fix) {
                fixes.push(fix);
            }
        }
        (issues, fixes, held)
    };
    let drift = host.override_.as_ref().is_some_and(|o| *o != base_template(state, host).config);
    let changed = net.issues != issues || net.fixes != fixes || net.held_roles != held_roles || net.drift != drift;
    changed.then(|| Action::Propose(Command::AssessNetwork { id: host.id.clone(), issues, fixes, held_roles, drift }))
}

/// What is wrong with `cfg` on `host`, judged against the host's LLDP
/// neighbours and the observed switch ports; an address `cfg` uses that
/// another host holds is a conflict.
pub(crate) fn validate_config(
    state: &ClusterState,
    host: &HostObj,
    cfg: &HostNetworkConfig,
    networks: &[LogicalNetwork],
    owners: &BTreeMap<Ipv4Addr, Vec<&HostId>>,
) -> Vec<Issue> {
    let ports: Vec<ObservedSwitchPort> = host
        .lldp
        .iter()
        .filter_map(|n| state.switches.get(&n.switch)?.ports.iter().find(|p| p.port == n.switch_port).cloned())
        .collect();
    let in_use: BTreeMap<Ipv4Addr, HostId> = cfg
        .attachments
        .iter()
        .filter_map(|a| {
            let ip = a.ip?;
            let other = owners.get(&ip)?.iter().find(|o| ***o != host.id)?;
            Some((ip, (*other).clone()))
        })
        .collect();
    validate_host(cfg, &host.roles, networks, &host.lldp, &ports, &in_use)
}

/// Open `NetworkMismatch` and `NetworkApplyFailed` alerts, by host.
fn open_alerts(state: &ClusterState) -> BTreeMap<HostId, Vec<&Alert>> {
    let mut open: BTreeMap<HostId, Vec<&Alert>> = BTreeMap::new();
    let ours = |a: &&Alert| {
        (a.kind == NETWORK_MISMATCH || a.kind == NETWORK_APPLY_FAILED) && a.resolved_at.is_none() && a.entity.kind == ObjectKind::Host
    };
    for a in state.alerts.values().filter(ours) {
        open.entry(HostId::new(&a.entity.id)).or_default().push(a);
    }
    open
}

/// Brings the host's alerts in line with the log: the `NetworkMismatch` of
/// its current issues and fixes (one incident per `issues_since`) and the
/// `NetworkApplyFailed` of a failed desired generation, raised if missing;
/// any other open one is resolved.
fn alerts(state: &ClusterState, host: &HostObj, open: &[&Alert]) -> Vec<Action> {
    let net = &host.net;
    let id = &host.id;
    let mismatch = (!net.issues.is_empty()).then(|| {
        (format!("netmismatch-{id}-{}", net.issues_since), NETWORK_MISMATCH, format!("{id}: {}", net.fixes.join("; ")))
    });
    let failure = matches!(net.state, NetState::Failed | NetState::Reverted).then(|| {
        let why = net.error.as_deref().unwrap_or("unknown");
        let message = format!("{id}: network config generation {} not applied: {why}", net.desired_generation);
        (format!("netapply-{id}-{}", net.desired_generation), NETWORK_APPLY_FAILED, message)
    });
    let wanted: BTreeSet<&str> = [&mismatch, &failure].into_iter().flatten().map(|(id, _, _)| id.as_str()).collect();
    let mut out: Vec<Action> = open
        .iter()
        .filter(|a| !wanted.contains(a.id.as_str()))
        .map(|a| Action::Propose(Command::ResolveAlert { id: a.id.clone() }))
        .collect();
    for (alert_id, kind, message) in [mismatch, failure].into_iter().flatten() {
        if !state.alerts.contains_key(&alert_id) {
            out.push(Action::Propose(Command::RaiseAlert {
                id: alert_id,
                severity: Severity::Warning,
                kind: kind.into(),
                entity: ObjectRef::host(id),
                message,
                ground_truth: false,
            }));
        }
    }
    out
}

/// The change that would fix `issue` on `host`, switch-side where it can be:
/// e.g. `access-1 p7: add VLAN 20 to trunk allowed`. A VLAN missing on a
/// port is phrased for the port's observed mode.
pub fn suggestion(state: &ClusterState, host: &HostId, issue: &Issue) -> String {
    match issue {
        Issue::VlanNotAllowed { switch, switch_port, vlan, .. } => {
            let mode = state.switches.get(switch).and_then(|s| s.ports.iter().find(|p| p.port == *switch_port)).map(|p| &p.cfg.mode);
            match mode {
                Some(PortMode::Trunk { .. }) => format!("{switch} {switch_port}: add VLAN {vlan} to trunk allowed"),
                Some(PortMode::Access(access)) => {
                    format!("{switch} {switch_port}: convert to a trunk (native VLAN {access}) allowing VLAN {vlan}")
                }
                None => format!("{switch} {switch_port}: allow VLAN {vlan} tagged"),
            }
        }
        Issue::NativeVlanMismatch { switch, switch_port, vlan, native, .. } => {
            let is = native.map_or("none".to_string(), |n| n.to_string());
            format!("{switch} {switch_port}: set untagged VLAN to {vlan} (is {is})")
        }
        Issue::MtuTooSmall { where_, have, need, .. } if where_.starts_with("link ") => {
            format!("{host} {where_}: raise MTU to {need} (is {have})")
        }
        Issue::MtuTooSmall { where_, have, need, .. } => format!("{where_}: raise MTU to {need} (is {have})"),
        Issue::BondSplitWithoutMlag { link, .. } => format!("{host} {link}: cable all members to one switch or an MLAG pair"),
        Issue::LacpNotConfigured { switch, port, .. } => format!("{switch} {port}: set LACP group {host}"),
        Issue::IpConflict { network, ip, other_host } => format!("{host} {network}: address {ip} is also used by {other_host}"),
        Issue::PortNotCabled { port, .. } => format!("{host} {port}: not cabled"),
        Issue::UnknownNetwork { id } => format!("{host}: network {id} does not exist"),
        Issue::MissingRequiredNetwork { role, network } => format!("{host}: role {role:?} needs network {network}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{mgmt, Msg};
    use crate::runtime::{Addr, Endpoint, Svc};
    use crate::state::{Command, Lifecycle, NetState, NetworkResult, ObjectRef, Severity};
    use model::network::{HostNetworkConfig, LldpNeighbor, ObservedSwitchPort, PortMode, SwitchPortCfg};
    use model::{profile, seed_networks, seed_templates, HostId, Inventory, NetworkId, PortName, ProfileId, Role, SwitchId};
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn nid(s: &str) -> NetworkId {
        NetworkId::from(s)
    }

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    fn template(id: &str) -> HostNetworkConfig {
        seed_templates().into_iter().find(|t| t.id == id).unwrap().config
    }

    /// `template` with its attachments addressed from `ips`.
    fn rendered(id: &str, ips: &BTreeMap<NetworkId, Ipv4Addr>) -> HostNetworkConfig {
        let mut cfg = template(id);
        for a in &mut cfg.attachments {
            a.ip = ips.get(&a.network).copied();
        }
        cfg
    }

    fn trunk(allowed: &[u16], mtu: u16) -> SwitchPortCfg {
        SwitchPortCfg { mode: PortMode::Trunk { native: Some(1), allowed: allowed.iter().copied().collect() }, mtu, lacp_group: None, admin_up: true }
    }

    fn observe_switch(port_cfg: SwitchPortCfg) -> Command {
        let id = SwitchId::from("access-1");
        let port = ObservedSwitchPort { switch: id.clone(), port: PortName::from("p7"), cfg: port_cfg, mlag_peer: None, link_up: true, speed_mbps: 1000, neighbor: None };
        Command::ObserveSwitch { id, model: "m".into(), ports: vec![port], mlag_peer: None, last_seen: 0 }
    }

    /// The seed networks and templates, and host `m` (a mini PC with roles
    /// Storage and Compute) provisioned on `minipc-standard`, cabled to
    /// access-1 p7, which is a trunk allowing VLANs 1 and 20.
    fn state() -> ClusterState {
        let mut s = ClusterState::new();
        for network in seed_networks() {
            s.apply(0, Command::UpsertNetwork { network }).unwrap();
        }
        for template in seed_templates() {
            s.apply(0, Command::UpsertTemplate { template }).unwrap();
        }
        add_host(&mut s, "m", ProfileId::MiniPc, Some("minipc-standard"), &[("mgmt", "10.0.0.2"), ("storage", "10.20.0.2")]);
        s.apply(0, observe_switch(trunk(&[1, 20], 9216))).unwrap();
        s
    }

    fn add_host(s: &mut ClusterState, id: &str, p: ProfileId, template: Option<&str>, ips: &[(&str, &str)]) {
        let inventory = Inventory::from_profile(p, 1);
        s.apply(0, Command::AddPendingHost { id: hid(id), profile: profile(p, 1), inventory: inventory.clone(), location: "rack-1".into() })
            .unwrap();
        let roles = if p == ProfileId::MiniPc { [Role::Storage, Role::Compute].into() } else { [Role::Compute].into() };
        s.apply(0, Command::ApproveHost { id: hid(id), roles }).unwrap();
        let ips = ips.iter().map(|(n, a)| (nid(n), ip(a))).collect();
        s.apply(0, Command::ProvisionHost { id: hid(id), template: template.map(String::from), ips }).unwrap();
        let lldp = vec![LldpNeighbor { local_port: PortName::from("eth0"), switch: SwitchId::from("access-1"), switch_port: PortName::from("p7") }];
        s.apply(0, Command::ObserveHost { id: hid(id), lldp, inventory }).unwrap();
    }

    fn proposals(actions: &[Action]) -> Vec<Command> {
        actions.iter().filter_map(|a| if let Action::Propose(c) = a { Some(c.clone()) } else { None }).collect()
    }

    fn pushes(actions: &[Action]) -> Vec<(HostId, u64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Endpoint { addr: Addr::Host(h), svc: Svc::Agent }, net, Msg::ApplyNetwork { generation, .. }) => {
                    assert_eq!(*net, mgmt());
                    Some((h.clone(), *generation))
                }
                _ => None,
            })
            .collect()
    }

    /// Runs the controller and applies what it proposes, at `now`; returns its actions.
    fn step(s: &mut ClusterState, v: &mut Volatile, now: SimTime) -> Vec<Action> {
        let out = run(s, v, now);
        for a in &out {
            match a {
                Action::Propose(c) => s.apply(now, c.clone()).unwrap(),
                Action::Send(to, _, msg) => v.note_sent(to, msg, now),
            }
        }
        out
    }

    fn m_ips() -> BTreeMap<NetworkId, Ipv4Addr> {
        [(nid("mgmt"), ip("10.0.0.2")), (nid("storage"), ip("10.20.0.2"))].into()
    }

    #[test]
    fn the_rendered_config_becomes_the_next_desired_generation() {
        let mut s = state();
        let v = Volatile::new(0, 1);
        let want = rendered("minipc-standard", &m_ips());
        assert_eq!(proposals(&run(&s, &v, 0))[0], Command::SetDesiredNetwork { id: hid("m"), config: want.clone() });
        s.apply(0, Command::SetDesiredNetwork { id: hid("m"), config: want }).unwrap();
        assert!(!proposals(&run(&s, &v, 0)).iter().any(|c| matches!(c, Command::SetDesiredNetwork { .. })), "unchanged");

        // An override replaces the template; its attachments get the host's addresses too.
        let mut ov = template("sbc-transcode");
        ov.links[0].mtu = 9000;
        s.apply(1, Command::SetHostOverride { host: hid("m"), config: Some(ov.clone()) }).unwrap();
        ov.attachments[0].ip = Some(ip("10.0.0.2"));
        assert_eq!(proposals(&run(&s, &v, 1))[0], Command::SetDesiredNetwork { id: hid("m"), config: ov });

        // A template edit changes it as well.
        s.apply(2, Command::SetHostOverride { host: hid("m"), config: None }).unwrap();
        let mut t = seed_templates().into_iter().find(|t| t.id == "minipc-standard").unwrap();
        t.config.links[0].mtu = 9100;
        s.apply(2, Command::UpsertTemplate { template: t }).unwrap();
        let Command::SetDesiredNetwork { config, .. } = &proposals(&run(&s, &v, 2))[0] else { panic!() };
        assert_eq!(config.links[0].mtu, 9100);
    }

    #[test]
    fn a_host_without_a_template_runs_its_bootstrap_config_with_its_address() {
        let mut s = state();
        add_host(&mut s, "rk", ProfileId::Rk3588, None, &[("mgmt", "10.0.0.3")]);
        let mut want = model::bootstrap_config(&profile(ProfileId::Rk3588, 1));
        want.attachments[0].ip = Some(ip("10.0.0.3"));
        let cmds = proposals(&run(&s, &Volatile::new(0, 1), 0));
        assert!(cmds.contains(&Command::SetDesiredNetwork { id: hid("rk"), config: want }), "{cmds:?}");
    }

    #[test]
    fn hosts_not_yet_provisioned_are_left_alone() {
        let mut s = ClusterState::new();
        for network in seed_networks() {
            s.apply(0, Command::UpsertNetwork { network }).unwrap();
        }
        let p = ProfileId::Pi5;
        s.apply(0, Command::AddPendingHost { id: hid("p"), profile: profile(p, 0), inventory: Inventory::from_profile(p, 0), location: "x".into() })
            .unwrap();
        assert!(run(&s, &Volatile::new(0, 1), 0).is_empty());
        s.apply(0, Command::ApproveHost { id: hid("p"), roles: [Role::Compute].into() }).unwrap();
        assert!(run(&s, &Volatile::new(0, 1), 0).is_empty());
    }

    #[test]
    fn the_desired_generation_is_pushed_until_the_agent_confirms_it() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0);
        let out = step(&mut s, &mut v, 1000);
        assert_eq!(pushes(&out), [(hid("m"), 1)]);
        let Some(Action::Send(_, _, Msg::ApplyNetwork { config, .. })) = out.iter().find(|a| matches!(a, Action::Send(..))) else { panic!() };
        assert_eq!(Some(config), s.hosts[&hid("m")].net.desired.as_ref(), "the stored desired config is what is pushed");
        assert!(pushes(&step(&mut s, &mut v, 2000)).is_empty(), "not again right away");
        assert_eq!(pushes(&step(&mut s, &mut v, 1000 + PUSH_RETRY_MS)), [(hid("m"), 1)], "the push may have been lost");

        // The agent applied it and said so over its new config: recorded once.
        v.net_results.insert(hid("m"), (1, NetworkResult::Applied));
        v.net_generation.insert(hid("m"), 1);
        let out = step(&mut s, &mut v, 12_000);
        assert_eq!(proposals(&out), [Command::RecordNetworkResult { id: hid("m"), generation: 1, result: NetworkResult::Applied }]);
        assert_eq!((s.hosts[&hid("m")].net.state, s.hosts[&hid("m")].net.applied_generation), (NetState::Applied, 1));
        assert!(step(&mut s, &mut v, 13_000 + PUSH_RETRY_MS).is_empty(), "nothing more to do");
    }

    #[test]
    fn an_agent_that_lost_its_generation_is_pushed_again() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0);
        s.apply(0, Command::RecordNetworkResult { id: hid("m"), generation: 1, result: NetworkResult::Applied }).unwrap();
        let mut v = Volatile::new(0, 1);
        assert!(pushes(&run(&s, &v, 0)).is_empty(), "a new leader trusts the log until the agent says otherwise");
        v.net_generation.insert(hid("m"), 0);
        assert_eq!(pushes(&run(&s, &v, 0)), [(hid("m"), 1)], "the agent restarted");
        v.net_generation.insert(hid("m"), 1);
        assert!(pushes(&run(&s, &v, 0)).is_empty());

        // A new leader with the generation not yet confirmed pushes, even if the agent runs it:
        // the agent answers again, and is confirmed before its trial runs out.
        let mut t = state();
        step(&mut t, &mut Volatile::new(0, 1), 0);
        assert_eq!(pushes(&run(&t, &v, 0)), [(hid("m"), 1)]);
    }

    #[test]
    fn a_failed_or_reverted_generation_is_recorded_alerted_and_not_retried() {
        for (result, error) in [(NetworkResult::Reverted, "reverted"), (NetworkResult::Failed("no NIC sfp0".into()), "no NIC sfp0")] {
            let mut s = state();
            let mut v = Volatile::new(0, 1);
            step(&mut s, &mut v, 0);
            step(&mut s, &mut v, 1000);
            v.net_results.insert(hid("m"), (1, result.clone()));
            v.net_generation.insert(hid("m"), 0);
            let out = step(&mut s, &mut v, 31_000);
            assert_eq!(proposals(&out), [Command::RecordNetworkResult { id: hid("m"), generation: 1, result: result.clone() }]);
            assert!(pushes(&out).is_empty(), "not even before it is recorded");
            let out = step(&mut s, &mut v, 32_000);
            let cmds = proposals(&out);
            let [Command::RaiseAlert { id, severity, kind, entity, message, .. }] = cmds.as_slice() else { panic!("{out:?}") };
            assert_eq!((id.as_str(), *severity, kind.as_str(), entity), ("netapply-m-1", Severity::Warning, NETWORK_APPLY_FAILED, &ObjectRef::host(&hid("m"))));
            assert!(message.contains("generation 1") && message.contains(error), "{message}");
            assert!(step(&mut s, &mut v, 32_000 + 10 * PUSH_RETRY_MS).is_empty(), "the failed generation is not pushed again");

            // A new desired config is the next attempt; the old failure is history.
            let mut ov = template("minipc-standard");
            ov.links[0].mtu = 9100;
            s.apply(40_000, Command::SetHostOverride { host: hid("m"), config: Some(ov) }).unwrap();
            step(&mut s, &mut v, 40_000);
            let out = step(&mut s, &mut v, 41_000);
            assert_eq!(pushes(&out), [(hid("m"), 2)]);
            assert!(proposals(&out).contains(&Command::ResolveAlert { id: "netapply-m-1".into() }), "{out:?}");
        }
    }

    #[test]
    fn issues_and_held_roles_are_recorded_and_alerted_once_per_change() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0);
        assert!(s.hosts[&hid("m")].net.issues.is_empty(), "the port is right");
        assert!(!s.alerts.values().any(|a| a.kind == NETWORK_MISMATCH));

        s.apply(10_000, observe_switch(trunk(&[1], 9216))).unwrap();
        let out = step(&mut s, &mut v, 10_000);
        let issue = Issue::VlanNotAllowed {
            network: nid("storage"),
            host_port: PortName::from("eth0"),
            switch: SwitchId::from("access-1"),
            switch_port: PortName::from("p7"),
            vlan: 20,
        };
        assert!(proposals(&out).contains(&Command::AssessNetwork {
            id: hid("m"),
            issues: vec![issue],
            fixes: vec!["access-1 p7: add VLAN 20 to trunk allowed".into()],
            held_roles: [(Role::Storage, nid("storage"))].into(),
            drift: false,
        }));
        let out = step(&mut s, &mut v, 11_000);
        let raise = |id: &str, message: &str| Command::RaiseAlert {
            id: id.into(),
            severity: Severity::Warning,
            kind: NETWORK_MISMATCH.into(),
            entity: ObjectRef::host(&hid("m")),
            message: message.into(),
            ground_truth: false,
        };
        assert_eq!(proposals(&out), [raise("netmismatch-m-10000", "m: access-1 p7: add VLAN 20 to trunk allowed")]);
        assert!(proposals(&step(&mut s, &mut v, 12_000)).is_empty(), "one alert per incident");

        // The issues change: the alert is replaced.
        s.apply(20_000, observe_switch(trunk(&[1], 1500))).unwrap();
        step(&mut s, &mut v, 20_000);
        let out = proposals(&step(&mut s, &mut v, 21_000));
        assert_eq!(out[0], Command::ResolveAlert { id: "netmismatch-m-10000".into() });
        let Command::RaiseAlert { id, message, .. } = &out[1] else { panic!("{out:?}") };
        assert_eq!(id, "netmismatch-m-20000");
        assert_eq!(message, "m: access-1 p7: add VLAN 20 to trunk allowed; access-1 p7: raise MTU to 9000 (is 1500)");

        // Fixed: released and resolved.
        s.apply(30_000, observe_switch(trunk(&[1, 20], 9216))).unwrap();
        step(&mut s, &mut v, 30_000);
        let net = &s.hosts[&hid("m")].net;
        assert!(net.issues.is_empty() && net.held_roles.is_empty(), "{net:?}");
        assert_eq!(proposals(&step(&mut s, &mut v, 31_000)), [Command::ResolveAlert { id: "netmismatch-m-20000".into() }]);
    }

    #[test]
    fn a_host_whose_cabling_was_never_observed_is_not_judged() {
        let mut s = state();
        s.apply(0, Command::ObserveHost { id: hid("m"), lldp: vec![], inventory: Inventory::from_profile(ProfileId::MiniPc, 1) }).unwrap();
        let cmds = proposals(&run(&s, &Volatile::new(0, 1), 0));
        assert!(!cmds.iter().any(|c| matches!(c, Command::AssessNetwork { .. })), "{cmds:?}");
    }

    #[test]
    fn an_override_that_differs_from_the_template_is_drift() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        s.apply(0, Command::SetHostOverride { host: hid("m"), config: Some(template("minipc-standard")) }).unwrap();
        step(&mut s, &mut v, 0);
        assert!(!s.hosts[&hid("m")].net.drift, "the same as the template");
        let mut ov = template("minipc-standard");
        ov.links[0].mtu = 9100;
        s.apply(1, Command::SetHostOverride { host: hid("m"), config: Some(ov) }).unwrap();
        step(&mut s, &mut v, 1000);
        assert!(s.hosts[&hid("m")].net.drift);
    }

    #[test]
    fn duplicate_addresses_are_issues() {
        let mut s = state();
        add_host(&mut s, "m2", ProfileId::MiniPc, Some("minipc-standard"), &[("mgmt", "10.0.0.3")]);
        s.hosts.get_mut(&hid("m2")).unwrap().ips.insert(nid("mgmt"), ip("10.0.0.2"));
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0);
        step(&mut s, &mut v, 0);
        let conflict = |other: &str| Issue::IpConflict { network: nid("mgmt"), ip: ip("10.0.0.2"), other_host: hid(other) };
        assert!(s.hosts[&hid("m")].net.issues.contains(&conflict("m2")), "{:?}", s.hosts[&hid("m")].net.issues);
        assert!(s.hosts[&hid("m2")].net.issues.contains(&conflict("m")));
    }

    #[test]
    fn alerts_of_removed_hosts_are_resolved() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        s.apply(0, observe_switch(trunk(&[1], 9216))).unwrap();
        step(&mut s, &mut v, 0);
        step(&mut s, &mut v, 0);
        assert_eq!(s.alerts.values().filter(|a| a.resolved_at.is_none()).count(), 1);
        s.apply(1, Command::RemoveHost { id: hid("m") }).unwrap();
        assert_eq!(proposals(&run(&s, &v, 1000)), [Command::ResolveAlert { id: "netmismatch-m-0".into() }]);
    }

    #[test]
    fn suggestions_name_the_fix() {
        let h = hid("h");
        let sw = || SwitchId::from("access-1");
        let p = |s: &str| PortName::from(s);
        // access-1 p7 is a trunk, p8 an access port on VLAN 1; p9 was never observed.
        let mut s = ClusterState::new();
        let port = |name: &str, cfg| ObservedSwitchPort { switch: sw(), port: p(name), cfg, mlag_peer: None, link_up: true, speed_mbps: 1000, neighbor: None };
        let access = SwitchPortCfg { mode: PortMode::Access(1), ..trunk(&[1], 9216) };
        let ports = vec![port("p7", trunk(&[1], 9216)), port("p8", access)];
        s.apply(0, Command::ObserveSwitch { id: sw(), model: "m".into(), ports, mlag_peer: None, last_seen: 0 }).unwrap();
        let vlan = |port: &str, vlan| Issue::VlanNotAllowed { network: nid("storage"), host_port: p("eth0"), switch: sw(), switch_port: p(port), vlan };
        let cases = [
            (vlan("p7", 20), "access-1 p7: add VLAN 20 to trunk allowed"),
            (vlan("p8", 20), "access-1 p8: convert to a trunk (native VLAN 1) allowing VLAN 20"),
            (vlan("p8", 1), "access-1 p8: convert to a trunk (native VLAN 1) allowing VLAN 1"),
            (vlan("p9", 20), "access-1 p9: allow VLAN 20 tagged"),
            (Issue::NativeVlanMismatch { network: nid("mgmt"), host_port: p("eth0"), switch: sw(), switch_port: p("p7"), vlan: 1, native: Some(20) }, "access-1 p7: set untagged VLAN to 1 (is 20)"),
            (Issue::NativeVlanMismatch { network: nid("mgmt"), host_port: p("eth0"), switch: sw(), switch_port: p("p7"), vlan: 1, native: None }, "access-1 p7: set untagged VLAN to 1 (is none)"),
            (Issue::MtuTooSmall { network: nid("storage"), where_: "access-1 p7".into(), have: 1500, need: 9000 }, "access-1 p7: raise MTU to 9000 (is 1500)"),
            (Issue::MtuTooSmall { network: nid("storage"), where_: "link eth0".into(), have: 1500, need: 9000 }, "h link eth0: raise MTU to 9000 (is 1500)"),
            (Issue::BondSplitWithoutMlag { link: "bond0".into(), networks: Default::default() }, "h bond0: cable all members to one switch or an MLAG pair"),
            (Issue::LacpNotConfigured { link: "bond0".into(), switch: sw(), port: p("p7"), networks: Default::default() }, "access-1 p7: set LACP group h"),
            (Issue::IpConflict { network: nid("mgmt"), ip: ip("10.0.0.2"), other_host: hid("x") }, "h mgmt: address 10.0.0.2 is also used by x"),
            (Issue::PortNotCabled { port: p("sfp1"), networks: Default::default() }, "h sfp1: not cabled"),
            (Issue::UnknownNetwork { id: nid("vm-x") }, "h: network vm-x does not exist"),
            (Issue::MissingRequiredNetwork { role: Role::Storage, network: nid("storage") }, "h: role Storage needs network storage"),
        ];
        for (issue, want) in cases {
            assert_eq!(suggestion(&s, &h, &issue), want);
        }
    }

    #[test]
    fn ready_hosts_are_managed_too() {
        let mut s = state();
        s.apply(0, Command::SetLifecycle { id: hid("m"), lifecycle: Lifecycle::Ready }).unwrap();
        assert!(proposals(&run(&s, &Volatile::new(0, 1), 0)).iter().any(|c| matches!(c, Command::SetDesiredNetwork { .. })));
    }

    #[test]
    fn after_a_revert_the_config_the_host_went_back_to_is_judged() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0);
        s.apply(0, Command::RecordNetworkResult { id: hid("m"), generation: 1, result: NetworkResult::Applied }).unwrap();
        // An override with storage untagged: the port's native VLAN is mgmt's, so storage breaks.
        let mut ov = template("minipc-standard");
        ov.attachments[1].tagged = false;
        s.apply(1, Command::SetHostOverride { host: hid("m"), config: Some(ov) }).unwrap();
        step(&mut s, &mut v, 1000);
        step(&mut s, &mut v, 2000);
        assert_eq!(s.hosts[&hid("m")].net.held_roles, [(Role::Storage, nid("storage"))].into(), "judged on the desired config while it applies");

        // The agent reverted to generation 1, which is fine: nothing is held, nothing to fix switch-side.
        v.net_results.insert(hid("m"), (2, NetworkResult::Reverted));
        for now in [33_000, 34_000, 35_000] {
            step(&mut s, &mut v, now);
        }
        let net = &s.hosts[&hid("m")].net;
        assert_eq!(net.state, NetState::Reverted);
        assert!(net.issues.is_empty() && net.held_roles.is_empty(), "{net:?}");
        assert!(net.drift, "the override is still there");
        let open: Vec<_> = s.alerts.values().filter(|a| a.resolved_at.is_none()).map(|a| a.kind.as_str()).collect();
        assert_eq!(open, [NETWORK_APPLY_FAILED], "the failure is alerted, not a switch mismatch");
    }

    #[test]
    fn a_pushed_generation_is_pushed_again_until_this_leader_hears_back() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0);
        s.apply(0, Command::RecordNetworkResult { id: hid("m"), generation: 1, result: NetworkResult::Applied }).unwrap();
        let mut v = Volatile::new(0, 1);
        v.net_generation.insert(hid("m"), 0);
        assert_eq!(pushes(&step(&mut s, &mut v, 1000)), [(hid("m"), 1)], "the agent restarted");
        // It runs generation 1 on trial, but its report was lost: ask again before its trial runs out.
        v.net_generation.insert(hid("m"), 1);
        assert_eq!(pushes(&step(&mut s, &mut v, 1000 + PUSH_RETRY_MS)), [(hid("m"), 1)]);
        v.net_results.insert(hid("m"), (1, NetworkResult::Applied));
        assert!(pushes(&step(&mut s, &mut v, 1000 + 2 * PUSH_RETRY_MS)).is_empty(), "heard back");
    }

    #[test]
    fn a_new_suggestion_for_the_same_issues_is_a_new_alert() {
        // The technician's port change is not polled yet: the port still shows access VLAN 1.
        let mut s = state();
        let access = SwitchPortCfg { mode: PortMode::Access(1), ..trunk(&[1], 9216) };
        s.apply(0, observe_switch(access)).unwrap();
        let mut v = Volatile::new(0, 1);
        step(&mut s, &mut v, 0);
        step(&mut s, &mut v, 1000);
        let message = |s: &ClusterState| {
            let open: Vec<_> = s.alerts.values().filter(|a| a.kind == NETWORK_MISMATCH && a.resolved_at.is_none()).collect();
            assert_eq!(open.len(), 1, "{open:?}");
            open[0].message.clone()
        };
        assert_eq!(message(&s), "m: access-1 p7: convert to a trunk (native VLAN 1) allowing VLAN 20");
        // Polled: a trunk without VLAN 20. The issue is the same; the fix is not.
        s.apply(10_000, observe_switch(trunk(&[1], 9216))).unwrap();
        step(&mut s, &mut v, 10_000);
        assert_eq!(s.hosts[&hid("m")].net.fixes, ["access-1 p7: add VLAN 20 to trunk allowed"]);
        step(&mut s, &mut v, 11_000);
        assert_eq!(message(&s), "m: access-1 p7: add VLAN 20 to trunk allowed");
    }
}
