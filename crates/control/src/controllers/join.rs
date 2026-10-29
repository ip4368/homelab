//! Join: an unjoined agent sends `JoinRequest`s with a token. A known,
//! unexpired token creates a `Pending` host from the reported inventory
//! ([`on_request`], answered by the replica). The periodic [`run`] then
//! approves pending hosts that match an auto-approve rule, and provisions
//! approved hosts: it picks their network template and allocates their
//! addresses. The network controller pushes the rendered config; once the
//! agent confirmed applying it (over its new mgmt), the host is `Ready`.

use super::{Action, Volatile};
use crate::runtime::{Addr, SimTime};
use crate::state::{ClusterState, Command, HostObj, Lifecycle, NetState, ObjectRef};
use model::network::{bootstrap_config, HostNetworkConfig, LogicalNetwork, NetworkPurpose};
use model::{allocate, eligible, HardwareProfile, HostId, Inventory, NetworkId};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

/// At most one "join rejected" event per host this often, however often its agent retries.
pub const REJECT_EVENT_EVERY_MS: u64 = 60_000;
/// A joining host's location until something knows better (LLDP).
pub const UNKNOWN_LOCATION: &str = "unknown";
pub const INVALID_TOKEN: &str = "invalid token";

/// What the leader does with a `JoinRequest`.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinOutcome {
    /// The host already exists: answer `Ok` now.
    Accepted,
    /// Propose this command and answer `Ok` once it applies.
    Add(Command),
    /// An `Add` for this host is in flight. Stay silent; the agent retries.
    InProgress,
    /// Answer with `reason`, and propose `event` if there is one.
    Rejected { reason: String, event: Option<Command> },
}

/// Decides a `JoinRequest` from `host`. Records rejections in `v` to rate-limit their events.
pub fn on_request(
    state: &ClusterState,
    v: &mut Volatile,
    now: SimTime,
    host: &HostId,
    token: &str,
    inventory: Inventory,
) -> JoinOutcome {
    if state.hosts.get(host).is_some_and(|h| h.lifecycle != Lifecycle::Removed) {
        return JoinOutcome::Accepted;
    }
    let from = Addr::Host(host.clone());
    if v.pending_joins.values().any(|p| p.from.addr == from) {
        return JoinOutcome::InProgress;
    }
    if !state.tokens.get(token).is_some_and(|t| now < t.expires_at) {
        let quiet = v.join_rejected_at.get(host).is_some_and(|&at| now < at + REJECT_EVENT_EVERY_MS);
        let event = (!quiet).then(|| {
            v.join_rejected_at.insert(host.clone(), now);
            Command::Event { entity: ObjectRef::host(host), message: format!("join rejected for {host}: {INVALID_TOKEN}") }
        });
        return JoinOutcome::Rejected { reason: INVALID_TOKEN.into(), event };
    }
    JoinOutcome::Add(Command::AddPendingHost {
        id: host.clone(),
        profile: HardwareProfile::with_discovered(&inventory),
        inventory,
        location: UNKNOWN_LOCATION.into(),
    })
}

pub fn run(state: &ClusterState, v: &Volatile) -> Vec<Action> {
    let mut out = vec![];
    let mut taken = addresses_taken(state, v);
    for host in state.hosts.values() {
        let id = &host.id;
        match host.lifecycle {
            Lifecycle::Pending => out.extend(auto_approve(state, host)),
            Lifecycle::Approved => out.push(Action::Propose(provision(state, v, host, &mut taken))),
            // `Applied` comes from the agent over its new config: mgmt works.
            Lifecycle::Provisioning if host.net.state == NetState::Applied => {
                out.push(Action::Propose(Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Ready }))
            }
            _ => {}
        }
    }
    out
}

/// Addresses held by hosts, or being handed out by provisioning proposals in flight.
fn addresses_taken(state: &ClusterState, v: &Volatile) -> BTreeSet<Ipv4Addr> {
    let held = state.hosts.values().flat_map(|h| h.ips.values().copied());
    let in_flight = v.in_flight.values().flat_map(|c| match c {
        Command::ProvisionHost { ips, .. } => ips.values().copied().collect(),
        _ => vec![],
    });
    held.chain(in_flight).collect()
}

/// Provisioning for an approved host: its template (see [`pick_template`])
/// and an address for each mgmt, storage and migration network the config
/// it will run attaches. A proposal already in flight is asked for again
/// unchanged, so its addresses stay put.
fn provision(state: &ClusterState, v: &Volatile, host: &HostObj, taken: &mut BTreeSet<Ipv4Addr>) -> Command {
    let in_flight = v.in_flight.values().find(|c| matches!(c, Command::ProvisionHost { id, .. } if *id == host.id));
    if let Some(cmd) = in_flight {
        return cmd.clone();
    }
    let template = pick_template(state, host);
    let config = match (&host.override_, template.as_ref().and_then(|t| state.templates.get(t))) {
        (Some(o), _) => o.clone(),
        (None, Some(t)) => t.template.config.clone(),
        (None, None) => bootstrap_config(&host.profile),
    };
    let ips = allocate_ips(&config, |n| state.networks.get(n).map(|o| &o.network), taken);
    Command::ProvisionHost { id: host.id.clone(), template, ips }
}

/// The template of the auto-approve rule for the host's profile, if it
/// names one that exists; else the first template written for the profile.
fn pick_template(state: &ClusterState, host: &HostObj) -> Option<String> {
    let rule = state.rules.values().find(|r| r.profile == host.profile.id);
    let from_rule = rule.and_then(|r| r.template.clone()).filter(|t| state.templates.contains_key(t));
    from_rule.or_else(|| {
        state.templates.values().find(|t| t.template.profile == host.profile.id).map(|t| t.template.id.clone())
    })
}

/// The lowest free address for each mgmt, storage and migration network
/// `config` attaches (VM networks address their guests, not the host). Each
/// address handed out joins `taken`. Networks `network` does not know, or
/// that are full, get none.
pub fn allocate_ips<'n>(
    config: &HostNetworkConfig,
    network: impl Fn(&NetworkId) -> Option<&'n LogicalNetwork>,
    taken: &mut BTreeSet<Ipv4Addr>,
) -> BTreeMap<NetworkId, Ipv4Addr> {
    let mut ips = BTreeMap::new();
    for a in &config.attachments {
        let Some(net) = network(&a.network) else { continue };
        let addressed = matches!(net.purpose, NetworkPurpose::Mgmt | NetworkPurpose::Storage | NetworkPurpose::Migration);
        if !addressed || ips.contains_key(&net.id) {
            continue;
        }
        if let Some(ip) = allocate(net, taken) {
            taken.insert(ip);
            ips.insert(net.id.clone(), ip);
        }
    }
    ips
}

/// Approval by the first rule (by id) for the host's profile, keeping only
/// the roles the host is eligible for, plus an event per dropped role. The
/// approval comes first, so once it applies the events are not asked for again.
fn auto_approve(state: &ClusterState, host: &HostObj) -> Vec<Action> {
    let Some(rule) = state.rules.values().find(|r| r.profile == host.profile.id) else { return vec![] };
    let mut roles = BTreeSet::new();
    let mut events = vec![];
    for &role in &rule.roles {
        match eligible(&host.profile, role) {
            Ok(()) => {
                roles.insert(role);
            }
            Err(reason) => events.push(Action::Propose(Command::Event {
                entity: ObjectRef::host(&host.id),
                message: format!("auto-approve rule {}: dropped role {role:?} for {}: {reason}", rule.id, host.id),
            })),
        }
    }
    let mut out = vec![Action::Propose(Command::ApproveHost { id: host.id.clone(), roles })];
    out.extend(events);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::PendingJoin;
    use crate::runtime::{Endpoint, Svc};
    use crate::state::NetworkResult;
    use model::{profile, seed_networks, seed_templates, NetworkId, ProfileId, Role};
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn inv(p: ProfileId, variant: u8) -> Inventory {
        Inventory::from_profile(p, variant)
    }

    fn with_token(expires_in: u64) -> ClusterState {
        let mut s = ClusterState::new();
        s.apply(0, Command::CreateJoinToken { token: "tok".into(), ttl_ms: expires_in }).unwrap();
        s
    }

    fn add(s: &mut ClusterState, id: &str, p: ProfileId, variant: u8) {
        let JoinOutcome::Add(cmd) = on_request(s, &mut Volatile::new(0, 1), 0, &hid(id), "tok", inv(p, variant))
        else {
            panic!("expected Add")
        };
        s.apply(0, cmd).unwrap();
    }

    fn proposals(actions: &[Action]) -> Vec<Command> {
        actions.iter().filter_map(|a| if let Action::Propose(c) = a { Some(c.clone()) } else { None }).collect()
    }

    #[test]
    fn valid_token_adds_a_pending_host_from_the_inventory() {
        let s = with_token(1000);
        let out = on_request(&s, &mut Volatile::new(0, 1), 999, &hid("m1"), "tok", inv(ProfileId::Pi5, 1));
        assert_eq!(
            out,
            JoinOutcome::Add(Command::AddPendingHost {
                id: hid("m1"),
                profile: profile(ProfileId::Pi5, 1),
                inventory: inv(ProfileId::Pi5, 1),
                location: UNKNOWN_LOCATION.into(),
            })
        );
    }

    #[test]
    fn known_host_is_accepted_whatever_the_token_and_in_flight_join_waits() {
        let mut s = with_token(1000);
        let mut v = Volatile::new(0, 1);
        v.pending_joins.insert(7, PendingJoin { from: Endpoint::new(Addr::Host(hid("m2")), Svc::Agent), term: 1 });
        assert_eq!(on_request(&s, &mut v, 0, &hid("m2"), "tok", inv(ProfileId::MiniPc, 0)), JoinOutcome::InProgress);
        add(&mut s, "m1", ProfileId::MiniPc, 0);
        assert_eq!(on_request(&s, &mut v, 5000, &hid("m1"), "gone", inv(ProfileId::MiniPc, 0)), JoinOutcome::Accepted);
    }

    #[test]
    fn unknown_or_expired_token_is_rejected_with_a_rate_limited_event() {
        let s = with_token(1000);
        let mut v = Volatile::new(0, 1);
        let ask = |v: &mut Volatile, at, host: &str, token| on_request(&s, v, at, &hid(host), token, inv(ProfileId::MiniPc, 0));
        let event = |host: &str| Command::Event {
            entity: ObjectRef::host(&hid(host)),
            message: format!("join rejected for {host}: invalid token"),
        };
        let rejected = |event| JoinOutcome::Rejected { reason: INVALID_TOKEN.into(), event };

        assert_eq!(ask(&mut v, 1000, "a", "tok"), rejected(Some(event("a"))), "expired at 1000");
        assert_eq!(ask(&mut v, 6000, "a", "nope"), rejected(None), "within a minute: answer only");
        assert_eq!(ask(&mut v, 6000, "b", "nope"), rejected(Some(event("b"))), "limited per host");
        assert_eq!(ask(&mut v, 1000 + REJECT_EVENT_EVERY_MS, "a", "nope"), rejected(Some(event("a"))));
    }

    #[test]
    fn rule_approves_with_eligible_roles_and_reports_dropped_ones() {
        let mut s = with_token(1000);
        let roles = [Role::Storage, Role::Transcode, Role::Vm].into();
        s.apply(0, Command::UpsertRule { id: "sbc".into(), profile: ProfileId::Pi5, roles, template: None }).unwrap();
        add(&mut s, "pi", ProfileId::Pi5, 0); // SD card, 8 GB: no storage, no vm
        add(&mut s, "mini", ProfileId::MiniPc, 0); // no rule: left for a human
        let cmds = proposals(&run(&s, &Volatile::new(0, 1)));
        assert_eq!(cmds.len(), 3, "{cmds:?}");
        assert_eq!(cmds[0], Command::ApproveHost { id: hid("pi"), roles: [Role::Transcode].into() });
        let Command::Event { entity, message } = &cmds[1] else { panic!("{cmds:?}") };
        assert_eq!(*entity, ObjectRef::host(&hid("pi")));
        assert_eq!(message, "auto-approve rule sbc: dropped role Storage for pi: storage needs a non-SD, non-eMMC disk");
        assert!(matches!(&cmds[2], Command::Event { message, .. } if message.contains("role Vm")), "{cmds:?}");
        for c in cmds {
            s.apply(0, c).unwrap();
        }
        assert_eq!(s.hosts[&hid("pi")].lifecycle, Lifecycle::Approved);
    }

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    fn ips(pairs: &[(&str, &str)]) -> BTreeMap<NetworkId, Ipv4Addr> {
        pairs.iter().map(|(n, a)| (NetworkId::from(*n), ip(a))).collect()
    }

    /// A token, the seed networks and templates, and `hosts` approved for Compute.
    fn with_networks(hosts: &[(&str, ProfileId)]) -> ClusterState {
        let mut s = with_token(1000);
        for network in seed_networks() {
            s.apply(0, Command::UpsertNetwork { network }).unwrap();
        }
        for template in seed_templates() {
            s.apply(0, Command::UpsertTemplate { template }).unwrap();
        }
        for &(id, p) in hosts {
            add(&mut s, id, p, 1);
            s.apply(0, Command::ApproveHost { id: hid(id), roles: [Role::Compute].into() }).unwrap();
        }
        s
    }

    fn provision(id: &str, template: Option<&str>, addrs: &[(&str, &str)]) -> Command {
        Command::ProvisionHost { id: hid(id), template: template.map(String::from), ips: ips(addrs) }
    }

    #[test]
    fn approved_host_is_provisioned_with_its_rule_template_and_addresses() {
        let mut s = with_networks(&[("ms", ProfileId::Ms01), ("mini", ProfileId::MiniPc), ("rk", ProfileId::Rk3588)]);
        let rule = |id: &str, profile, template: &str| Command::UpsertRule {
            id: id.into(),
            profile,
            roles: [Role::Compute].into(),
            template: Some(template.into()),
        };
        s.apply(0, rule("rk", ProfileId::Rk3588, "sbc-transcode")).unwrap();
        // A rule naming a template that does not exist is passed over for profile matching.
        s.apply(0, rule("mini", ProfileId::MiniPc, "gone")).unwrap();
        assert_eq!(
            proposals(&run(&s, &Volatile::new(0, 1))),
            [
                // mini: the first template written for its profile; mgmt and storage get addresses.
                provision("mini", Some("minipc-standard"), &[("mgmt", "10.0.0.2"), ("storage", "10.20.0.2")]),
                // ms: VM networks get no host address; mgmt, storage and migration do.
                provision("ms", Some("ms01-standard"), &[("mgmt", "10.0.0.3"), ("migration", "10.30.0.2"), ("storage", "10.20.0.3")]),
                // rk: no template is profiled Rk3588, but its rule names one.
                provision("rk", Some("sbc-transcode"), &[("mgmt", "10.0.0.4")]),
            ]
        );
    }

    #[test]
    fn a_host_without_a_template_gets_a_mgmt_address_for_its_bootstrap_config() {
        let mut s = with_token(1000);
        for network in seed_networks() {
            s.apply(0, Command::UpsertNetwork { network }).unwrap();
        }
        add(&mut s, "rk", ProfileId::Rk3588, 0);
        s.apply(0, Command::ApproveHost { id: hid("rk"), roles: [Role::Compute].into() }).unwrap();
        assert_eq!(proposals(&run(&s, &Volatile::new(0, 1))), [provision("rk", None, &[("mgmt", "10.0.0.2")])]);
        // No networks at all: nothing to address, but the host still provisions.
        let mut s = with_token(1000);
        add(&mut s, "rk", ProfileId::Rk3588, 0);
        s.apply(0, Command::ApproveHost { id: hid("rk"), roles: [Role::Compute].into() }).unwrap();
        assert_eq!(proposals(&run(&s, &Volatile::new(0, 1))), [provision("rk", None, &[])]);
    }

    #[test]
    fn addresses_skip_those_held_or_being_handed_out() {
        let mut s = with_networks(&[("a", ProfileId::Pi5), ("b", ProfileId::Pi5), ("c", ProfileId::Pi5)]);
        s.apply(0, provision("a", Some("sbc-transcode"), &[("mgmt", "10.0.0.2")])).unwrap();
        let mut v = Volatile::new(0, 1);
        let b = provision("b", Some("sbc-transcode"), &[("mgmt", "10.0.0.4")]);
        v.in_flight.insert(9, b.clone());
        // b's proposal (from an earlier run) still stands, so c steers clear of it, and b's is asked for unchanged.
        assert_eq!(proposals(&run(&s, &v)), [b, provision("c", Some("sbc-transcode"), &[("mgmt", "10.0.0.3")])]);
    }

    #[test]
    fn provisioning_host_is_ready_once_its_network_config_applied() {
        let mut s = with_networks(&[("m1", ProfileId::Pi5)]);
        s.apply(1, provision("m1", Some("sbc-transcode"), &[("mgmt", "10.0.0.2")])).unwrap();
        let mut v = Volatile::new(0, 1);
        v.last_heartbeat.insert(hid("m1"), 2500);
        assert!(run(&s, &v).is_empty(), "a heartbeat is not enough: nothing applied yet");
        let config = seed_templates()[2].config.clone();
        s.apply(2, Command::SetDesiredNetwork { id: hid("m1"), config: config.clone() }).unwrap();
        s.apply(3, Command::RecordNetworkResult { id: hid("m1"), generation: 1, result: NetworkResult::Reverted }).unwrap();
        assert!(run(&s, &v).is_empty(), "a reverted host stays provisioning");
        let mut changed = config;
        changed.links[0].mtu = 1400;
        s.apply(4, Command::SetDesiredNetwork { id: hid("m1"), config: changed }).unwrap();
        assert!(run(&s, &v).is_empty());
        s.apply(5, Command::RecordNetworkResult { id: hid("m1"), generation: 2, result: NetworkResult::Applied }).unwrap();
        let ready = Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::Ready };
        assert_eq!(run(&s, &v), [Action::Propose(ready.clone())]);
        s.apply(6, ready).unwrap();
        assert!(run(&s, &v).is_empty());
    }
}
