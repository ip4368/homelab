//! Kubernetes (K3s): every member host on its final network runs K3s, a
//! server (API server and etcd member) on control-plane hosts and an agent
//! everywhere else, and storage hosts give Longhorn their data disks. The
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
//!   no disks.
//! * A replaced dead server (down, no longer a voter) has its etcd member
//!   removed by the live servers (`KubeSpec.etcd_remove`, via `kubectl
//!   delete node`, which K3s turns into an etcd removal), once its Raft
//!   replacement is done. Never a live member, and never without quorum.

use super::membership::target_size;
use super::{Action, Volatile};
use crate::bootstrap::profile_prefix;
use crate::proto::{mgmt, KubeRole, KubeSpec, KubeStatus, Msg};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, HostObj, Lifecycle};
use model::{eligible, HostId, Role};
use std::collections::BTreeMap;

/// How long the controller waits for a host to run its spec before sending it again.
pub const KUBE_RESEND_MS: u64 = 15_000;
/// The prefix of the node labels the control plane sets.
pub const LABEL_PREFIX: &str = "homelab.io/";

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

fn runs_server(status: Option<&KubeStatus>) -> bool {
    status.is_some_and(|s| s.role == Some(KubeRole::Server) && s.phase == crate::proto::KubePhase::Running)
}

/// Dead K3s servers whose etcd members live servers must remove: hosts that
/// were servers (their last status says so), are down (`NotReady`), and are
/// no longer voters (their Raft replacement is done). Empty while etcd lacks
/// quorum (live servers are not a majority of all known server members),
/// since a removal could not commit then anyway. Sorted for a deterministic
/// spec. Liveness in Kubernetes itself (a `Ready` node) is checked by the
/// worker at removal time, since the last status here is stale once the
/// host is down.
pub fn etcd_remove(state: &ClusterState, status: &impl Fn(&HostId) -> Option<KubeStatus>) -> Vec<HostId> {
    let is_server = |id: &HostId| status(id).is_some_and(|s| s.role == Some(KubeRole::Server));
    let live: usize = state.hosts.values().filter(|h| member(h) && runs_server(status(&h.id).as_ref())).count();
    let total = state.hosts.keys().filter(|id| is_server(id)).count();
    if total == 0 || live < total / 2 + 1 {
        return vec![];
    }
    let mut dead: Vec<HostId> = state
        .hosts
        .values()
        .filter(|h| h.lifecycle == Lifecycle::NotReady && !state.cp.voters.contains(&h.id) && is_server(&h.id))
        .map(|h| h.id.clone())
        .collect();
    dead.sort();
    dead
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
    // A new voter gets the control-plane role a moment after its Raft config
    // commits: in between it already counts toward the target.
    let role = if h.roles.contains(&Role::ControlPlane) || state.cp.voters.contains(id) {
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
    let storage = h.roles.contains(&Role::Storage) && !h.net.held_roles.contains_key(&Role::Storage);
    let storage_disks = if storage { h.inventory.disks.iter().filter(|d| !d.failed).map(|d| d.name.clone()).collect() } else { vec![] };
    // Only live servers remove etcd members (agents lack the RBAC), and
    // never themselves.
    let etcd_remove =
        if role == KubeRole::Server { etcd_remove(state, status).into_iter().filter(|d| d != id).collect() } else { vec![] };
    Some(KubeSpec { role, init, token: kube.token.clone(), node_ip, labels: node_labels(h), storage_disks, etcd_remove })
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
        // `satisfies` ignores `etcd_remove` (a task, not state): a Running
        // server with removals to do still gets its spec.
        let removal = !spec.etcd_remove.is_empty();
        if !removal && status(id).is_some_and(|s| s.satisfies(&spec)) {
            continue;
        }
        if v.kube_sent.get(id).is_some_and(|at| now < at + KUBE_RESEND_MS) {
            continue;
        }
        out.push(Action::Send(Endpoint::new(Addr::Host(id.clone()), Svc::Agent), mgmt(), Msg::Kube(spec)));
    }
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
        let first = SeedHost { id: hid("ms01-01"), inventory: Inventory::from_profile(ProfileId::Ms01, 0), location: "lab".into(), running: None };
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
            let ips = [(mgmt(), std::net::Ipv4Addr::new(10, 0, 0, 10 + i as u8))].into();
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

    fn running(role: KubeRole) -> KubeStatus {
        KubeStatus { role: Some(role), phase: KubePhase::Running, node_ready: true, labels: BTreeMap::new(), storage_disks: vec![], version: "v1".into(), detail: None }
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

    /// Three voters (ms01-01..03) all Running servers, plus ms01-04 (a
    /// future voter, an agent for now) and pi5-01 (always an agent).
    fn three_voters() -> (ClusterState, BTreeMap<HostId, KubeStatus>) {
        let mut s = cluster(&[
            ("ms01-02", ProfileId::Ms01),
            ("ms01-03", ProfileId::Ms01),
            ("ms01-04", ProfileId::Ms01),
            ("pi5-01", ProfileId::Pi5),
        ]);
        s.apply_membership([hid("ms01-01"), hid("ms01-02"), hid("ms01-03")].into());
        for id in ["ms01-02", "ms01-03"] {
            let mut roles = s.hosts[&hid(id)].roles.clone();
            roles.insert(Role::ControlPlane);
            s.apply(0, Command::SetRoles { id: hid(id), roles }).unwrap();
        }
        let mut st = BTreeMap::new();
        for id in ["ms01-01", "ms01-02", "ms01-03"] {
            st.insert(hid(id), running(KubeRole::Server));
        }
        st.insert(hid("ms01-04"), running(KubeRole::Agent));
        st.insert(hid("pi5-01"), running(KubeRole::Agent));
        (s, st)
    }

    #[test]
    fn a_replaced_dead_server_is_removed_from_etcd_by_the_live_servers() {
        let (mut s, mut st) = three_voters();
        // ms01-03 goes down and is replaced by ms01-04 (Raft done): 01, 02
        // live servers, 04 a new live server, 03 down and no longer a voter.
        s.apply(0, Command::SetLifecycle { id: hid("ms01-03"), lifecycle: Lifecycle::NotReady }).unwrap();
        s.apply_membership([hid("ms01-01"), hid("ms01-02"), hid("ms01-04")].into());
        st.insert(hid("ms01-04"), running(KubeRole::Server));
        let status = |id: &HostId| st.get(id).cloned();
        assert_eq!(etcd_remove(&s, &status), [hid("ms01-03")]);
        let spec_01 = desired(&s, &hid("ms01-01"), &status).unwrap();
        assert_eq!(spec_01.etcd_remove, [hid("ms01-03")]);
        let spec_04 = desired(&s, &hid("ms01-04"), &status).unwrap();
        assert_eq!((spec_04.role, spec_04.etcd_remove.as_slice()), (KubeRole::Server, [hid("ms01-03")].as_slice()));
        let pi = desired(&s, &hid("pi5-01"), &status).unwrap();
        assert_eq!((pi.role, pi.etcd_remove.as_slice()), (KubeRole::Agent, [].as_slice()), "agents never remove");

        // A Running server with a removal to do still gets its spec.
        let mut v = Volatile::new(0, 1);
        for (id, st) in &st {
            v.kube_status.insert(id.clone(), st.clone());
        }
        // ms01-01 already runs its labels/disks: without a removal it would
        // be left alone, with one it is told.
        let done = KubeStatus {
            labels: spec_01.labels.clone(),
            storage_disks: spec_01.storage_disks.clone(),
            ..running(KubeRole::Server)
        };
        v.kube_status.insert(hid("ms01-01"), done);
        let specs = sent(&run(&s, &v, 0));
        assert_eq!(specs[&hid("ms01-01")].etcd_remove, [hid("ms01-03")]);
        assert!(!specs.contains_key(&hid("ms01-03")), "the dead host gets nothing");
    }

    #[test]
    fn etcd_removal_never_takes_a_live_member_or_breaks_quorum() {
        let (mut s, mut st) = three_voters();
        // Still a voter (replacement not done): nothing, even down.
        s.apply(0, Command::SetLifecycle { id: hid("ms01-03"), lifecycle: Lifecycle::NotReady }).unwrap();
        assert!(etcd_remove(&s, &|id| st.get(id).cloned()).is_empty());
        // Back up (live): nothing, even no longer a voter.
        s.apply_membership([hid("ms01-01"), hid("ms01-02"), hid("ms01-04")].into());
        s.apply(0, Command::SetLifecycle { id: hid("ms01-03"), lifecycle: Lifecycle::Ready }).unwrap();
        assert!(etcd_remove(&s, &|id| st.get(id).cloned()).is_empty(), "a live host is never removed");
        // Down again, but an agent (never a server): nothing.
        s.apply(0, Command::SetLifecycle { id: hid("ms01-03"), lifecycle: Lifecycle::NotReady }).unwrap();
        st.insert(hid("ms01-03"), running(KubeRole::Agent));
        assert!(etcd_remove(&s, &|id| st.get(id).cloned()).is_empty());
        // Down as a server, but etcd without quorum (only 1 live of 3): nothing.
        st.insert(hid("ms01-03"), running(KubeRole::Server));
        st.remove(&hid("ms01-02"));
        s.apply(0, Command::SetLifecycle { id: hid("ms01-02"), lifecycle: Lifecycle::NotReady }).unwrap();
        assert!(etcd_remove(&s, &|id| st.get(id).cloned()).is_empty(), "1 live of 3 has no quorum");
    }
}
