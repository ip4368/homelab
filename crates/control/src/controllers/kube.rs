//! Kubernetes (K3s): every member host on its final network runs K3s, a
//! server (API server and etcd member) on the control-plane voters and an
//! agent everywhere else, and storage hosts give Longhorn their data disks. The
//! controller works out what each host should run ([`desired`]), sends it
//! until the host reports that it runs it, and records what hosts report.
//!
//! * A host gets nothing until it is a member on its static addresses: K3s
//!   registers with the mgmt address, which must not change afterwards.
//! * The cluster's first host starts the Kubernetes cluster
//!   (`--cluster-init`); every other host joins through the VIP once some
//!   server runs, so a join never races the first server's install.
//! * A control-plane-eligible host gets nothing while the voters are below
//!   target: membership is about to make one of them a voter, and a voter
//!   runs a server.
//! * A host whose storage role is held for a network problem gives Longhorn
//!   no disks, and nor does any other host without an active storage role:
//!   a running server is asked to take such a host's Longhorn disks out of
//!   scheduling ([`KubeAdmin::storage_off`]), and to give them back once the
//!   host stores again.
//! * Longhorn replicates over the storage network when it has a reserved
//!   range for Longhorn's pods ([`storage_network`]): every host attached
//!   to it gets it in its spec, with its own address there.
//!
//! Replacing a dead server, etcd's way: remove the dead member, then add the
//! new one.
//! * Once a dead voter's host is no longer a control-plane voter (membership
//!   replaced it), a running server is asked to remove its etcd member
//!   ([`etcd_removal`]). Only one at a time, only while the members on live
//!   servers are a quorum without it, and never a live member.
//! * So is the member of a host that is no voter and runs a K3s agent: its
//!   server left, but deleting its own node failed, so the member stayed
//!   behind with nothing to run it.
//! * A server that is not an etcd member yet waits to join while a dead
//!   member is still to go, or a dead voter still to leave the control
//!   plane, or while etcd would be short of a quorum until it starts
//!   ([`join_blocked`]).
//! * A member that has not started has no name yet: servers report it by
//!   peer address, which is its host's mgmt address. One whose server died
//!   while joining goes like any dead member.
//! * Servers report etcd's members, or why they could not read them. While
//!   no server can, the members are unknown: nothing is removed and no
//!   server joins.

use super::drain;
use super::membership::target_size;
use super::{Action, Volatile};
use crate::bootstrap::profile_prefix;
use crate::proto::{mgmt, KubeAdmin, KubeRole, KubeSpec, KubeStatus, Msg, StorageNetwork};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, HostObj, Lifecycle};
use model::network::NetworkPurpose;
use model::{eligible, HostId, NetworkId, Role};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;

/// How long the controller waits for a host to run its spec before sending
/// it again, and for a server to do the cluster's work before asking again.
pub const KUBE_RESEND_MS: u64 = 15_000;
/// The prefix of the node labels the control plane sets.
pub const LABEL_PREFIX: &str = "homelab.io/";
/// The node label of hosts attached to the storage network: Longhorn runs
/// on them (it needs the network on every node it runs on).
pub const STORAGE_NETWORK_LABEL: &str = "homelab.io/storage-network";

/// A role's name in a node label, e.g. `homelab.io/control-plane=true`.
pub fn role_label(r: Role) -> &'static str {
    match r {
        Role::ControlPlane => "control-plane",
        Role::Storage => "storage",
        Role::Compute => "compute",
        Role::Vm => "vm",
        Role::Transcode => "transcode",
    }
}

/// Whether `s` is a valid label name segment: up to 63 alphanumerics, `-`,
/// `_` and `.`, starting and ending alphanumeric.
fn name_segment(s: &str) -> bool {
    let alnum = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric());
    !s.is_empty()
        && s.len() <= 63
        && alnum(s.chars().next())
        && alnum(s.chars().last())
        && s.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
}

/// Whether `key=value` is a valid Kubernetes label: an optional DNS prefix
/// and `/`, a name segment, and an empty or name-segment value.
pub fn valid_label(key: &str, value: &str) -> bool {
    let name = match key.split_once('/') {
        Some((prefix, name)) => {
            let dns = !prefix.is_empty() && prefix.len() <= 253 && prefix.split('.').all(|p| name_segment(p) && !p.contains('_'));
            if !dns {
                return false;
            }
            name
        }
        None => key,
    };
    name_segment(name) && (value.is_empty() || name_segment(value))
}

/// The node labels for `h`: its profile and active roles under
/// [`LABEL_PREFIX`] (a role held for a network problem is left off, so
/// workloads that select it go elsewhere), and those of its own labels
/// Kubernetes accepts (the control plane's prefix is reserved).
pub fn node_labels(h: &HostObj) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = h
        .labels
        .iter()
        .filter(|(k, v)| !k.starts_with(LABEL_PREFIX) && valid_label(k, v))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    out.insert(format!("{LABEL_PREFIX}profile"), profile_prefix(h.profile.id).into());
    for &r in h.roles.iter().filter(|r| !h.net.held_roles.contains_key(r)) {
        out.insert(format!("{LABEL_PREFIX}{}", role_label(r)), "true".into());
    }
    out
}

fn member(h: &HostObj) -> bool {
    matches!(h.lifecycle, Lifecycle::Ready | Lifecycle::Cordoned | Lifecycle::Draining | Lifecycle::Drained)
}

/// Whether `id`'s host is up: a member host the control plane hears from.
fn up(state: &ClusterState, id: &HostId) -> bool {
    state.hosts.get(id).is_some_and(member)
}

/// Whether `h` keeps Longhorn replicas: it has the storage role, not held.
fn stores(h: &HostObj) -> bool {
    h.roles.contains(&Role::Storage) && !h.net.held_roles.contains_key(&Role::Storage)
}

fn runs_server(status: Option<&KubeStatus>) -> bool {
    status.is_some_and(|s| s.role == Some(KubeRole::Server) && s.phase == crate::proto::KubePhase::Running)
}

/// The network Longhorn replicates over: the storage network, if it has a
/// reserved range for Longhorn's pods to take addresses from (its first).
pub fn storage_network(state: &ClusterState) -> Option<(NetworkId, StorageNetwork)> {
    let n = state.networks.values().map(|n| &n.network).find(|n| n.purpose == NetworkPurpose::Storage)?;
    let pods = *n.reserved.first()?;
    Some((n.id.clone(), StorageNetwork { subnet: n.subnet, pods }))
}

/// The etcd members, as the servers on up hosts report them, a member that
/// has not started by the host with its peer address; `None` while servers
/// run but none of them could read the members.
pub fn etcd_members(state: &ClusterState, status: &impl Fn(&HostId) -> Option<KubeStatus>) -> Option<BTreeSet<HostId>> {
    let reports: Vec<KubeStatus> = state.hosts.keys().filter(|id| up(state, id)).filter_map(|id| status(id).filter(|s| runs_server(Some(s)))).collect();
    let read: Vec<&KubeStatus> = reports.iter().filter(|s| s.etcd_unread.is_none()).collect();
    if read.is_empty() && !reports.is_empty() {
        return None;
    }
    let by_peer = |ip: &Ipv4Addr| state.hosts.values().find(|h| h.ips.get(&mgmt()) == Some(ip)).map(|h| h.id.clone());
    Some(read.into_iter().flat_map(|s| s.etcd_members.iter().cloned().chain(s.etcd_unstarted.iter().filter_map(by_peer))).collect())
}

/// The members whose hosts are up and run their server.
fn live_members(state: &ClusterState, members: &BTreeSet<HostId>, status: &impl Fn(&HostId) -> Option<KubeStatus>) -> usize {
    members.iter().filter(|m| up(state, m) && runs_server(status(m).as_ref())).count()
}

/// Whether etcd member `m` is to go: its host is no control-plane voter
/// (membership replaced it) and is down, or gone from the cluster, or runs
/// a K3s agent (its server left without its member).
fn gone(state: &ClusterState, m: &HostId, status: &impl Fn(&HostId) -> Option<KubeStatus>) -> bool {
    !state.cp.voters.contains(m) && (!up(state, m) || runs_agent(status(m).as_ref()))
}

/// Whether the host's K3s is an agent: its server, if it had one, is uninstalled.
fn runs_agent(status: Option<&KubeStatus>) -> bool {
    status.is_some_and(|s| s.role == Some(KubeRole::Agent))
}

/// The etcd member to remove now, if any: the first that is [`gone`], and
/// only while the members on live servers are a quorum of all members
/// (a removal needs one, and then they stay a quorum of those left).
pub fn etcd_removal<'m>(state: &ClusterState, members: &'m BTreeSet<HostId>, status: &impl Fn(&HostId) -> Option<KubeStatus>) -> Option<&'m HostId> {
    let m = members.iter().find(|m| gone(state, m, status))?;
    (live_members(state, members, status) > members.len() / 2).then_some(m)
}

/// Whether a server that is no etcd member yet must wait to join `members`:
/// while a dead member is still to go, or a dead voter still to leave the
/// control plane (in both cases its replacement joins after it went), or
/// while the members on live servers and the new one would not be a quorum
/// of the members with it (as etcd itself requires to promote it).
pub fn join_blocked(state: &ClusterState, members: &BTreeSet<HostId>, status: &impl Fn(&HostId) -> Option<KubeStatus>) -> bool {
    let leaving = state.cp.voters.len() > target_size(state) && state.cp.voters.iter().any(|v| !up(state, v));
    members.iter().any(|m| gone(state, m, status)) || leaving || live_members(state, members, status) + 1 < (members.len() + 1) / 2 + 1
}

/// What `id` should run of Kubernetes now, or `None` for nothing yet (see
/// the module docs). `status` is each host's latest reported status.
pub fn desired(state: &ClusterState, id: &HostId, status: &impl Fn(&HostId) -> Option<KubeStatus>) -> Option<KubeSpec> {
    let kube = state.kube.as_ref()?;
    let h = state.hosts.get(id)?;
    if !member(h) {
        return None;
    }
    let node_ip = *h.ips.get(&mgmt())?;
    // The voters, not the control-plane role: the role catches up with a
    // Raft config a moment after it commits, so it may name a host that is
    // not a voter (yet, or any more), and a host must not be reinstalled
    // for that.
    let role = if state.cp.voters.contains(id) {
        KubeRole::Server
    } else if eligible(&h.profile, Role::ControlPlane).is_ok() && state.cp.voters.len() < target_size(state) {
        return None;
    } else {
        KubeRole::Agent
    };
    let other_server = state.hosts.keys().any(|o| o != id && runs_server(status(o).as_ref()));
    let init = role == KubeRole::Server && kube.init_host == *id && !other_server;
    if !init && !other_server && !runs_server(status(id).as_ref()) {
        return None;
    }
    if role == KubeRole::Server && !init && !runs_server(status(id).as_ref()) {
        // With etcd's members unknown, a join waits until a server reads them.
        let blocked = etcd_members(state, status).is_none_or(|members| !members.contains(id) && join_blocked(state, &members, status));
        if blocked {
            return None;
        }
    }
    let storage_disks = if stores(h) { h.inventory.disks.iter().filter(|d| !d.failed).map(|d| d.name.clone()).collect() } else { vec![] };
    let net = storage_network(state);
    let storage_ip = net.as_ref().and_then(|(n, _)| h.ips.get(n).copied());
    let mut labels = node_labels(h);
    if storage_ip.is_some() {
        labels.insert(STORAGE_NETWORK_LABEL.into(), "true".into());
    }
    Some(KubeSpec { role, init, token: kube.token.clone(), node_ip, labels, storage_disks, storage_network: net.map(|(_, n)| n), storage_ip })
}

/// What a running server is to do for the cluster now, sent to the first
/// up host running one: remove the etcd member [`etcd_removal`] picks, keep
/// Longhorn off the disks of member hosts that do not store, and cordon and
/// drain nodes ([`drain`](super::drain)). Asked while there is a member to
/// remove, a node to drain, or the server has not applied those sets of
/// hosts, at most every [`KUBE_RESEND_MS`].
fn admin(state: &ClusterState, v: &Volatile, status: &impl Fn(&HostId) -> Option<KubeStatus>, now: SimTime) -> Option<Action> {
    let via = state.hosts.keys().find(|id| up(state, id) && runs_server(status(id).as_ref()))?;
    let members = etcd_members(state, status).unwrap_or_default();
    let remove_etcd_members: BTreeSet<HostId> = etcd_removal(state, &members, status).into_iter().cloned().collect();
    let etcd_peers = remove_etcd_members.iter().filter_map(|m| Some((m.clone(), *state.hosts.get(m)?.ips.get(&mgmt())?))).collect();
    let etcd_agents = remove_etcd_members.iter().filter(|m| up(state, m) && runs_agent(status(m).as_ref())).cloned().collect();
    let storage_off: BTreeSet<HostId> = state.hosts.values().filter(|h| member(h) && !stores(h)).map(|h| h.id.clone()).collect();
    let cordon = drain::cordoned(state);
    let drain = drain::draining(state);
    let applied = status(via).is_some_and(|s| s.storage_off == storage_off && s.cordoned == cordon && drain.is_subset(&s.drained));
    if (remove_etcd_members.is_empty() && applied) || v.kube_admin_sent.is_some_and(|at| now < at + KUBE_RESEND_MS) {
        return None;
    }
    let to = Endpoint::new(Addr::Host(via.clone()), Svc::Agent);
    let admin = KubeAdmin { remove_etcd_members, etcd_peers, etcd_agents, storage_off, cordon, drain };
    Some(Action::Send(to, mgmt(), Msg::KubeAdmin(admin)))
}

pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out: Vec<Action> = v
        .kube_status
        .iter()
        .filter(|(id, s)| state.hosts.get(*id).is_some_and(|h| h.kube.as_ref() != Some(*s)))
        .map(|(id, s)| Action::Propose(Command::RecordKube { id: id.clone(), status: s.clone() }))
        .collect();
    let status = |id: &HostId| v.kube_status.get(id).cloned().or_else(|| state.hosts.get(id)?.kube.clone());
    for id in state.hosts.keys() {
        let Some(spec) = desired(state, id, &status) else { continue };
        if status(id).is_some_and(|s| s.satisfies(&spec)) {
            continue;
        }
        if v.kube_sent.get(id).is_some_and(|at| now < at + KUBE_RESEND_MS) {
            continue;
        }
        out.push(Action::Send(Endpoint::new(Addr::Host(id.clone()), Svc::Agent), mgmt(), Msg::Kube(spec)));
    }
    out.extend(admin(state, v, &status, now));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{seeded_durable, SeedHost};
    use crate::proto::{KubePhase, LogCmd};
    use crate::raft::EntryPayload;
    use crate::state::NetworkResult;
    use model::{Inventory, ProfileId};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    /// A cluster bootstrapped on `ms01-01` (Ready), with `others` approved
    /// and Ready on their templates.
    fn cluster(others: &[(&str, ProfileId)]) -> ClusterState {
        let first = SeedHost { id: hid("ms01-01"), inventory: Inventory::from_profile(ProfileId::Ms01, 0), location: "lab".into() };
        let d = seeded_durable(&[first], &[hid("ms01-01")].into(), "s3cret", 0);
        let mut s = ClusterState::new();
        for e in &d.log {
            if let EntryPayload::Normal(LogCmd { at, cmd }) = &e.payload {
                s.apply(*at, cmd.clone()).unwrap();
            }
        }
        ready(&mut s, "ms01-01");
        for (i, (id, p)) in others.iter().enumerate() {
            let profile = model::HardwareProfile::with_discovered(&Inventory::from_profile(*p, 0));
            s.apply(0, Command::AddPendingHost { id: hid(id), profile, inventory: Inventory::from_profile(*p, 0), location: "lab".into() }).unwrap();
            let roles = crate::bootstrap::seed_rule_roles(*p);
            s.apply(0, Command::ApproveHost { id: hid(id), roles }).unwrap();
            let mut ips: BTreeMap<_, _> = [(mgmt(), std::net::Ipv4Addr::new(10, 0, 0, 10 + i as u8))].into();
            if *p != ProfileId::Pi5 {
                ips.insert(storage(), std::net::Ipv4Addr::new(10, 20, 0, 10 + i as u8));
            }
            s.apply(0, Command::ProvisionHost { id: hid(id), template: None, ips }).unwrap();
            ready(&mut s, id);
        }
        s
    }

    fn ready(s: &mut ClusterState, id: &str) {
        let config = s.hosts[&hid(id)].net.desired.clone().unwrap_or_default();
        s.apply(0, Command::SetDesiredNetwork { id: hid(id), config }).unwrap();
        let generation = s.hosts[&hid(id)].net.desired_generation;
        s.apply(0, Command::RecordNetworkResult { id: hid(id), generation, result: NetworkResult::Applied }).unwrap();
        s.apply(0, Command::SetLifecycle { id: hid(id), lifecycle: Lifecycle::Ready }).unwrap();
    }

    fn storage() -> model::NetworkId {
        model::NetworkId::from("storage")
    }

    fn running(role: KubeRole) -> KubeStatus {
        KubeStatus {
            role: Some(role),
            phase: KubePhase::Running,
            node_ready: true,
            labels: BTreeMap::new(),
            storage_disks: vec![],
            version: "v1".into(),
            detail: None,
            etcd_members: BTreeSet::new(),
            etcd_unread: None,
            etcd_unstarted: BTreeSet::new(),
            ..KubeStatus::default()
        }
    }

    /// A running server that sees `members` in etcd.
    fn server_seeing(members: &[&str]) -> KubeStatus {
        KubeStatus { etcd_members: members.iter().map(|m| hid(m)).collect(), ..running(KubeRole::Server) }
    }

    /// The work servers are asked to do for the cluster, by server.
    fn asked(actions: &[Action]) -> Vec<(HostId, KubeAdmin)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Endpoint { addr: Addr::Host(h), .. }, _, Msg::KubeAdmin(admin)) => Some((h.clone(), admin.clone())),
                _ => None,
            })
            .collect()
    }

    fn set(ids: &[&str]) -> BTreeSet<HostId> {
        ids.iter().map(|i| hid(i)).collect()
    }

    /// Voters ms01-01..03 run K3s servers, all three in etcd; ms01-04 runs an agent.
    fn three_servers() -> (ClusterState, Volatile) {
        let mut s = cluster(&[("ms01-02", ProfileId::Ms01), ("ms01-03", ProfileId::Ms01), ("ms01-04", ProfileId::Ms01)]);
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-03"]));
        let mut v = Volatile::new(0, 1);
        for id in ["ms01-01", "ms01-02", "ms01-03"] {
            v.kube_status.insert(hid(id), server_seeing(&["ms01-01", "ms01-02", "ms01-03"]));
        }
        v.kube_status.insert(hid("ms01-04"), running(KubeRole::Agent));
        (s, v)
    }

    fn down(s: &mut ClusterState, id: &str) {
        s.apply(0, Command::SetLifecycle { id: hid(id), lifecycle: Lifecycle::NotReady }).unwrap();
    }

    fn hold_storage(s: &mut ClusterState, id: &str, held: bool) {
        let held_roles = if held { [(Role::Storage, storage())].into() } else { BTreeMap::new() };
        s.apply(0, Command::AssessNetwork { id: hid(id), issues: vec![], fixes: vec![], held_roles, drift: false }).unwrap();
    }

    fn sent(actions: &[Action]) -> BTreeMap<HostId, KubeSpec> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Endpoint { addr: Addr::Host(h), .. }, _, Msg::Kube(spec)) => Some((h.clone(), spec.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn the_first_host_starts_kubernetes_and_others_wait_for_a_running_server() {
        let s = cluster(&[("pi5-01", ProfileId::Pi5)]);
        let v = Volatile::new(0, 1);
        let specs = sent(&run(&s, &v, 0));
        let first = &specs[&hid("ms01-01")];
        assert_eq!((first.role, first.init, first.token.as_str()), (KubeRole::Server, true, "s3cret"));
        assert_eq!(first.node_ip.to_string(), "10.0.0.2", "its static mgmt address");
        assert_eq!(first.labels["homelab.io/control-plane"], "true");
        assert_eq!(first.labels["homelab.io/profile"], "ms01");
        assert_eq!(first.storage_disks, ["nvme0", "nvme1"], "an MS-01 stores on both data disks");
        assert!(!specs.contains_key(&hid("pi5-01")), "no server runs yet to join");

        let mut v = Volatile::new(0, 1);
        v.kube_status.insert(hid("ms01-01"), running(KubeRole::Server));
        let specs = sent(&run(&s, &v, 0));
        let pi = &specs[&hid("pi5-01")];
        assert_eq!((pi.role, pi.init), (KubeRole::Agent, false));
        assert!(pi.storage_disks.is_empty(), "the seed Pi rule has no storage role");
        assert!(pi.labels.contains_key("homelab.io/transcode"));
    }

    #[test]
    fn an_eligible_host_waits_while_the_control_plane_grows_then_follows_its_role() {
        let mut s = cluster(&[("ms01-02", ProfileId::Ms01)]);
        let mut v = Volatile::new(0, 1);
        v.kube_status.insert(hid("ms01-01"), running(KubeRole::Server));
        assert!(!sent(&run(&s, &v, 0)).contains_key(&hid("ms01-02")), "it may become a voter");
        s.apply_membership([hid("ms01-01"), hid("ms01-02")].into());
        let mut roles = s.hosts[&hid("ms01-02")].roles.clone();
        roles.insert(Role::ControlPlane);
        s.apply(0, Command::SetRoles { id: hid("ms01-02"), roles }).unwrap();
        let spec = &sent(&run(&s, &v, 0))[&hid("ms01-02")];
        assert_eq!((spec.role, spec.init), (KubeRole::Server, false), "a voter joins as a server");
    }

    #[test]
    fn a_new_voter_is_a_server_before_its_role_catches_up() {
        let mut s = cluster(&[("ms01-02", ProfileId::Ms01), ("ms01-03", ProfileId::Ms01)]);
        let mut v = Volatile::new(0, 1);
        v.kube_status.insert(hid("ms01-01"), running(KubeRole::Server));
        // The third voter's config committed; membership has not yet given it the role.
        s.apply_membership([hid("ms01-01"), hid("ms01-02"), hid("ms01-03")].into());
        assert!(!s.hosts[&hid("ms01-03")].roles.contains(&Role::ControlPlane));
        let specs = sent(&run(&s, &v, 0));
        assert_eq!(specs[&hid("ms01-03")].role, KubeRole::Server, "not an agent, to be reinstalled a second later");
    }

    #[test]
    fn the_control_plane_role_on_a_host_that_is_no_voter_makes_no_server() {
        // Given the role by hand, which membership takes back a moment later:
        // a server spec now would be an agent spec then, and the host would
        // flip between the two.
        let (mut s, v) = three_servers();
        let status = |id: &HostId| v.kube_status.get(id).cloned();
        for id in ["ms01-03", "ms01-04"] {
            let mut roles = s.hosts[&hid(id)].roles.clone();
            roles.insert(Role::ControlPlane);
            s.apply(0, Command::SetRoles { id: hid(id), roles }).unwrap();
        }
        assert_eq!(desired(&s, &hid("ms01-04"), &status).unwrap().role, KubeRole::Agent);

        // ms01-04 takes ms01-03's place: the voters decide, before the roles catch up.
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-04"]));
        assert_eq!(desired(&s, &hid("ms01-03"), &status).unwrap().role, KubeRole::Agent);
        assert_eq!(desired(&s, &hid("ms01-04"), &status).unwrap().role, KubeRole::Server);
    }

    #[test]
    fn a_host_running_its_spec_is_left_alone_and_one_that_is_not_is_resent_after_a_pause() {
        let s = cluster(&[]);
        let mut v = Volatile::new(0, 1);
        let spec = desired(&s, &hid("ms01-01"), &|_| None).unwrap();
        v.kube_sent.insert(hid("ms01-01"), 1000);
        assert!(sent(&run(&s, &v, 1000 + KUBE_RESEND_MS - 1)).is_empty(), "sent a moment ago");
        assert_eq!(sent(&run(&s, &v, 1000 + KUBE_RESEND_MS)).len(), 1);
        let done = KubeStatus { labels: spec.labels.clone(), storage_disks: spec.storage_disks.clone(), ..running(KubeRole::Server) };
        v.kube_status.insert(hid("ms01-01"), done.clone());
        let actions = run(&s, &v, 1000 + KUBE_RESEND_MS);
        assert!(sent(&actions).is_empty());
        assert_eq!(actions, [Action::Propose(Command::RecordKube { id: hid("ms01-01"), status: done })], "what it reports is recorded");
    }

    #[test]
    fn a_held_storage_role_gives_longhorn_no_disks_and_no_storage_label() {
        let mut s = cluster(&[]);
        let h = s.hosts[&hid("ms01-01")].clone();
        let held = [(Role::Storage, model::NetworkId::from("storage"))].into();
        s.apply(0, Command::AssessNetwork { id: h.id.clone(), issues: vec![], fixes: vec![], held_roles: held, drift: false }).unwrap();
        let spec = desired(&s, &h.id, &|_| None).unwrap();
        assert!(spec.storage_disks.is_empty());
        assert!(!spec.labels.contains_key("homelab.io/storage"), "workloads selecting storage hosts go elsewhere");
        assert_eq!(spec.labels["homelab.io/vm"], "true", "its other roles stay");
    }

    /// Statuses are in the Raft log: one an earlier homelabd logged must
    /// replay after an upgrade (a host whose log cannot replay crash-loops).
    #[test]
    fn a_status_logged_by_the_first_release_still_replays() {
        let logged = r#"{"at":1,"cmd":{"RecordKube":{"id":"ms01-01","status":{"role":"Server","phase":"Installing","node_ready":false,"labels":{},"storage_disks":[],"version":"v1.36.5+k3s1"}}}}"#;
        let cmd: LogCmd = serde_json::from_str(logged).expect("the first release's RecordKube replays");
        let Command::RecordKube { status, .. } = cmd.cmd else { panic!("{cmd:?}") };
        assert_eq!((status.phase, status.detail), (KubePhase::Installing, None));
        assert!(status.etcd_members.is_empty() && status.etcd_unread.is_none() && status.storage_off.is_empty());
    }

    #[test]
    fn a_member_left_behind_by_a_server_that_became_an_agent_goes_through_etcd_before_a_server_joins() {
        let (mut s, mut v) = three_servers();
        // ms01-04 ran a server for a moment, then reinstalled as an agent
        // without its member: etcd counts four, three of which run.
        let four = server_seeing(&["ms01-01", "ms01-02", "ms01-03", "ms01-04"]);
        for id in ["ms01-01", "ms01-02", "ms01-03"] {
            v.kube_status.insert(hid(id), four.clone());
        }
        // A server reinstalling as an agent still has its server: its member stays.
        let reinstalling = KubeStatus { phase: KubePhase::Installing, detail: Some("installing K3s as Agent".into()), ..running(KubeRole::Server) };
        v.kube_status.insert(hid("ms01-04"), reinstalling);
        assert!(asked(&run(&s, &v, 0)).iter().all(|(_, a)| a.remove_etcd_members.is_empty()));

        v.kube_status.insert(hid("ms01-04"), running(KubeRole::Agent));
        let peer = s.hosts[&hid("ms01-04")].ips[&mgmt()];
        let want = KubeAdmin { remove_etcd_members: set(&["ms01-04"]), etcd_peers: [(hid("ms01-04"), peer)].into(), etcd_agents: set(&["ms01-04"]), storage_off: BTreeSet::new(), cordon: BTreeSet::new(), drain: BTreeSet::new() };
        assert_eq!(asked(&run(&s, &v, 0)), [(hid("ms01-01"), want)]);

        // ms01-04 takes ms01-03's place: it becomes a voter, so its member is
        // no longer left behind, and it reinstalls as a server.
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-04"]));
        let actions = run(&s, &v, 0);
        assert!(asked(&actions).iter().all(|(_, a)| a.remove_etcd_members.is_empty()), "a voter's member stays");
        assert_eq!(sent(&actions)[&hid("ms01-04")].role, KubeRole::Server);

        // Any other new voter waits while ms01-04's member is left behind.
        let mut s = cluster(&[("ms01-02", ProfileId::Ms01), ("ms01-03", ProfileId::Ms01), ("ms01-04", ProfileId::Ms01), ("ms01-05", ProfileId::Ms01)]);
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-03", "ms01-05"]));
        v.kube_status.insert(hid("ms01-04"), running(KubeRole::Agent));
        v.kube_status.insert(hid("ms01-05"), running(KubeRole::Agent));
        assert!(!sent(&run(&s, &v, 0)).contains_key(&hid("ms01-05")), "it joins once the member left behind is gone");
    }

    #[test]
    fn a_member_that_never_started_counts_by_its_peer_address_and_goes_like_any_dead_one() {
        let (mut s, mut v) = three_servers();
        let peer = |s: &ClusterState, id: &str| s.hosts[&hid(id)].ips[&mgmt()];
        // ms01-03 died while joining: etcd added its member, which never started.
        let joining = KubeStatus { etcd_unstarted: [peer(&s, "ms01-03")].into(), ..server_seeing(&["ms01-01", "ms01-02"]) };
        for id in ["ms01-01", "ms01-02"] {
            v.kube_status.insert(hid(id), joining.clone());
        }
        v.kube_status.remove(&hid("ms01-03"));
        down(&mut s, "ms01-03");
        let status = |id: &HostId| v.kube_status.get(id).cloned();
        assert_eq!(etcd_members(&s, &status), Some(set(&["ms01-01", "ms01-02", "ms01-03"])));
        assert!(asked(&run(&s, &v, 0)).is_empty(), "a voter's member stays, started or not");

        // Membership replaced it with ms01-04: the unstarted member goes, by its peer address, before ms01-04 joins.
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-04"]));
        let actions = run(&s, &v, 0);
        let want = KubeAdmin { remove_etcd_members: set(&["ms01-03"]), etcd_peers: [(hid("ms01-03"), peer(&s, "ms01-03"))].into(), etcd_agents: BTreeSet::new(), storage_off: BTreeSet::new(), cordon: BTreeSet::new(), drain: BTreeSet::new() };
        assert_eq!(asked(&actions), [(hid("ms01-01"), want)]);
        assert!(!sent(&actions).contains_key(&hid("ms01-04")));

        // ms01-04's own member, added but not started yet, is no reason for it to wait.
        let joined = KubeStatus { etcd_unstarted: [peer(&s, "ms01-04")].into(), ..server_seeing(&["ms01-01", "ms01-02"]) };
        for id in ["ms01-01", "ms01-02"] {
            v.kube_status.insert(hid(id), joined.clone());
        }
        let actions = run(&s, &v, 0);
        assert_eq!(sent(&actions)[&hid("ms01-04")].role, KubeRole::Server);
        assert!(asked(&actions).is_empty(), "a live host's member stays");
    }

    #[test]
    fn while_no_server_can_read_etcds_members_none_is_removed_and_no_server_joins() {
        let (mut s, mut v) = three_servers();
        down(&mut s, "ms01-03");
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-04"]));
        let unread = KubeStatus { etcd_unread: Some("curl: (7) Failed to connect".into()), ..running(KubeRole::Server) };
        for id in ["ms01-01", "ms01-02"] {
            v.kube_status.insert(hid(id), unread.clone());
        }
        let status = |id: &HostId| v.kube_status.get(id).cloned();
        assert_eq!(etcd_members(&s, &status), None, "an empty list is not an unread one");
        let actions = run(&s, &v, 0);
        assert!(asked(&actions).iter().all(|(_, a)| a.remove_etcd_members.is_empty()), "{:?}", asked(&actions));
        assert!(!sent(&actions).contains_key(&hid("ms01-04")), "the replacement waits to know etcd's members");

        // One server reads them again: the dead member goes.
        v.kube_status.insert(hid("ms01-02"), server_seeing(&["ms01-01", "ms01-02", "ms01-03"]));
        let status = |id: &HostId| v.kube_status.get(id).cloned();
        assert_eq!(etcd_members(&s, &status), Some(set(&["ms01-01", "ms01-02", "ms01-03"])), "from the server that read them");
        assert_eq!(asked(&run(&s, &v, 0))[0].1.remove_etcd_members, set(&["ms01-03"]));
    }

    #[test]
    fn a_replaced_servers_etcd_member_goes_before_its_replacement_joins() {
        let (mut s, mut v) = three_servers();
        down(&mut s, "ms01-03");
        assert!(asked(&run(&s, &v, 0)).is_empty(), "a dead voter keeps its member until it is replaced");

        // Membership replaced it with ms01-04.
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-04"]));
        let actions = run(&s, &v, 0);
        let peer = s.hosts[&hid("ms01-03")].ips[&mgmt()];
        let want = KubeAdmin { remove_etcd_members: set(&["ms01-03"]), etcd_peers: [(hid("ms01-03"), peer)].into(), etcd_agents: BTreeSet::new(), storage_off: BTreeSet::new(), cordon: BTreeSet::new(), drain: BTreeSet::new() };
        assert_eq!(asked(&actions), [(hid("ms01-01"), want)], "through the first running server");
        assert!(!sent(&actions).contains_key(&hid("ms01-04")), "the replacement waits for the dead member to go");
        v.kube_admin_sent = Some(0);
        assert!(asked(&run(&s, &v, KUBE_RESEND_MS - 1)).is_empty(), "asked a moment ago");
        assert_eq!(asked(&run(&s, &v, KUBE_RESEND_MS)).len(), 1, "asked again while the member stays");

        // The member is gone: the replacement joins as a server, and nothing is left to do.
        for id in ["ms01-01", "ms01-02"] {
            v.kube_status.insert(hid(id), server_seeing(&["ms01-01", "ms01-02"]));
        }
        let actions = run(&s, &v, 2 * KUBE_RESEND_MS);
        let spec = &sent(&actions)[&hid("ms01-04")];
        assert_eq!((spec.role, spec.init), (KubeRole::Server, false));
        assert!(asked(&actions).is_empty());
    }

    #[test]
    fn a_new_server_waits_while_a_dead_voter_is_still_to_leave() {
        let (mut s, v) = three_servers();
        down(&mut s, "ms01-03");
        // ms01-04 was promoted; the dead ms01-03 leaves the voters next.
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-03", "ms01-04"]));
        let actions = run(&s, &v, 0);
        assert!(!sent(&actions).contains_key(&hid("ms01-04")), "remove first, then add");
        assert!(asked(&actions).is_empty(), "a voter's member stays");
    }

    #[test]
    fn no_live_member_is_removed_and_no_removal_or_join_costs_etcd_its_quorum() {
        let (mut s, v) = three_servers();
        let status = |id: &HostId| v.kube_status.get(id).cloned();
        let members = set(&["ms01-01", "ms01-02", "ms01-03"]);
        // No longer a voter but up: it leaves etcd itself as it reinstalls as an agent.
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-04"]));
        assert_eq!(etcd_removal(&s, &members, &status), None);
        // Two of three members dead: the live one is no quorum, so a removal could not commit.
        down(&mut s, "ms01-02");
        down(&mut s, "ms01-03");
        assert_eq!(etcd_removal(&s, &members, &status), None);

        // Two voters dead, none replaced yet: a fourth member would leave etcd short of a quorum.
        let (mut s, v) = three_servers();
        let status = |id: &HostId| v.kube_status.get(id).cloned();
        down(&mut s, "ms01-02");
        down(&mut s, "ms01-03");
        assert!(join_blocked(&s, &members, &status), "two of four would be no quorum");
        assert!(!join_blocked(&s, &set(&["ms01-01", "ms01-02"]), &status), "two of three are");

        // A host removed from the cluster: its member goes.
        let (mut s, v) = three_servers();
        let status = |id: &HostId| v.kube_status.get(id).cloned();
        s.apply_membership(set(&["ms01-01", "ms01-02", "ms01-04"]));
        s.apply(0, Command::RemoveHost { id: hid("ms01-03") }).unwrap();
        assert_eq!(etcd_removal(&s, &members, &status), Some(&hid("ms01-03")));
        assert!(join_blocked(&s, &members, &status));
        assert!(!join_blocked(&s, &set(&["ms01-01", "ms01-02"]), &status));
        assert!(!join_blocked(&s, &set(&["ms01-01"]), &status), "the second server joins the first");
        assert!(!join_blocked(&s, &BTreeSet::new(), &status), "servers that report no members do not stop a join");
    }

    #[test]
    fn longhorn_keeps_off_the_disks_of_hosts_that_do_not_store() {
        let (mut s, mut v) = three_servers();
        assert!(asked(&run(&s, &v, 0)).is_empty(), "every host stores");
        hold_storage(&mut s, "ms01-04", true);
        let want = KubeAdmin { remove_etcd_members: BTreeSet::new(), etcd_peers: BTreeMap::new(), etcd_agents: BTreeSet::new(), storage_off: set(&["ms01-04"]), cordon: BTreeSet::new(), drain: BTreeSet::new() };
        assert_eq!(asked(&run(&s, &v, 0)), [(hid("ms01-01"), want.clone())]);
        v.kube_status.get_mut(&hid("ms01-01")).unwrap().storage_off = want.storage_off;
        assert!(asked(&run(&s, &v, 0)).is_empty(), "the server applied it");

        // The network is fixed: the disks are given back.
        hold_storage(&mut s, "ms01-04", false);
        assert_eq!(asked(&run(&s, &v, 0)), [(hid("ms01-01"), KubeAdmin { remove_etcd_members: BTreeSet::new(), etcd_peers: BTreeMap::new(), etcd_agents: BTreeSet::new(), storage_off: BTreeSet::new(), cordon: BTreeSet::new(), drain: BTreeSet::new() })]);
        v.kube_status.get_mut(&hid("ms01-01")).unwrap().storage_off.clear();
        // A host without the storage role never stores.
        let mut roles = s.hosts[&hid("ms01-04")].roles.clone();
        roles.remove(&Role::Storage);
        s.apply(0, Command::SetRoles { id: hid("ms01-04"), roles }).unwrap();
        assert_eq!(asked(&run(&s, &v, 0))[0].1.storage_off, set(&["ms01-04"]));
    }

    #[test]
    fn longhorn_replicates_over_the_storage_network_on_the_hosts_attached_to_it() {
        let mut s = cluster(&[("pi5-01", ProfileId::Pi5)]);
        let first = desired(&s, &hid("ms01-01"), &|_| None).unwrap();
        let net = first.storage_network.clone().expect("the seed storage network reserves Longhorn's pod addresses");
        assert_eq!((net.subnet.to_string(), net.pods), ("10.20.0.0/22".to_string(), model::network::STORAGE_RESERVED));
        assert_eq!(first.storage_ip, s.hosts[&hid("ms01-01")].ips.get(&storage()).copied());
        assert!(first.storage_ip.is_some());
        assert_eq!(first.labels[STORAGE_NETWORK_LABEL], "true", "Longhorn runs here");
        let server = |id: &HostId| (id.as_str() == "ms01-01").then(|| running(KubeRole::Server));
        let pi = desired(&s, &hid("pi5-01"), &server).unwrap();
        assert_eq!((pi.storage_network, pi.storage_ip), (Some(net), None), "the same network, no address on it");
        assert!(!pi.labels.contains_key(STORAGE_NETWORK_LABEL), "Longhorn stays off a host it cannot replicate from");

        // A storage network without a range for the pods: Longhorn stays on the pod network.
        let mut network = s.networks[&storage()].network.clone();
        network.reserved.clear();
        s.apply(0, Command::UpsertNetwork { network }).unwrap();
        let spec = desired(&s, &hid("ms01-01"), &|_| None).unwrap();
        assert_eq!((spec.storage_network, spec.storage_ip), (None, None));
        assert!(!spec.labels.contains_key(STORAGE_NETWORK_LABEL));
    }

    #[test]
    fn only_valid_labels_reach_kubernetes() {
        assert!(valid_label("homelab.io/storage", "true"));
        assert!(valid_label("rack", ""));
        assert!(!valid_label("bad key", "x"));
        assert!(!valid_label("-lead", "x"));
        assert!(!valid_label("ok", "has space"));
        assert!(!valid_label("/name", "x"));
    }
}
