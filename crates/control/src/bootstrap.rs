//! A freshly installed cluster: the Raft state its first replicas start
//! from, like `k3s server --cluster-init`. The simulator seeds whole fleets
//! with it; a real first host (`homelabd bootstrap`) seeds a fleet of one.

use crate::controllers::join::allocate_ips;
use crate::proto::LogCmd;
use crate::raft::{Durable, Entry, EntryPayload};
use crate::runtime::SimTime;
use crate::state::{Command, CpMode, Lifecycle, NetworkResult};
use model::{eligible, seed_networks, seed_templates, HardwareProfile, HostId, HostNetworkConfig, Inventory, ProfileId, Role};
use std::collections::{BTreeMap, BTreeSet};

/// A host the cluster is installed with.
#[derive(Debug, Clone)]
pub struct SeedHost {
    pub id: HostId,
    pub inventory: Inventory,
    pub location: String,
    /// The network config the host already runs, addresses included: it is
    /// recorded as applied (generation 1) and the host is `Ready`. `None`
    /// provisions the host on its rule's template with addresses from IPAM,
    /// for the network controller to push like it would for any new host.
    pub running: Option<HostNetworkConfig>,
}

/// The Raft state of a freshly installed cluster: one term-1 log entry per
/// command, stamped `at`, all uncommitted until the first leader's no-op
/// commits them. The log sets the CP voters, adds the seed networks and
/// templates and the seed auto-approve rules (each naming its template), and
/// adds every host at its location, approved with its rule's roles (those it
/// is eligible for, plus `ControlPlane` for voters) and provisioned on its
/// rule's template (see [`SeedHost::running`]).
pub fn seeded_durable(hosts: &[SeedHost], voters: &BTreeSet<HostId>, at: SimTime) -> Durable<LogCmd> {
    let mut cmds = vec![Command::SetCp { voters: voters.clone(), mode: CpMode::Suggest }];
    let networks = seed_networks();
    let templates: BTreeMap<String, _> = seed_templates().into_iter().map(|t| (t.id.clone(), t)).collect();
    cmds.extend(networks.iter().cloned().map(|network| Command::UpsertNetwork { network }));
    cmds.extend(templates.values().cloned().map(|template| Command::UpsertTemplate { template }));
    for p in [ProfileId::Ms01, ProfileId::MiniPc, ProfileId::Pi5, ProfileId::Rk3588] {
        cmds.push(Command::UpsertRule {
            id: profile_prefix(p).into(),
            profile: p,
            roles: seed_rule_roles(p),
            template: Some(seed_rule_template(p).into()),
        });
    }
    let mut taken = BTreeSet::new();
    for h in hosts {
        let running_ips = h.running.iter().flat_map(|c| &c.attachments).filter_map(|a| a.ip);
        taken.extend(running_ips);
    }
    for h in hosts {
        let profile = HardwareProfile::with_discovered(&h.inventory);
        let mut roles: BTreeSet<Role> =
            seed_rule_roles(profile.id).into_iter().filter(|&r| eligible(&profile, r).is_ok()).collect();
        if voters.contains(&h.id) {
            roles.insert(Role::ControlPlane);
        }
        let template = seed_rule_template(profile.id);
        let ips = match &h.running {
            Some(cfg) => cfg.attachments.iter().filter_map(|a| Some((a.network.clone(), a.ip?))).collect(),
            None => allocate_ips(&templates[template].config, |n| networks.iter().find(|net| &net.id == n), &mut taken),
        };
        cmds.extend([
            Command::AddPendingHost {
                id: h.id.clone(),
                profile,
                inventory: h.inventory.clone(),
                location: h.location.clone(),
            },
            Command::ApproveHost { id: h.id.clone(), roles },
            Command::ProvisionHost { id: h.id.clone(), template: Some(template.into()), ips },
        ]);
        if let Some(cfg) = &h.running {
            cmds.extend([
                Command::SetDesiredNetwork { id: h.id.clone(), config: cfg.clone() },
                Command::RecordNetworkResult { id: h.id.clone(), generation: 1, result: NetworkResult::Applied },
                Command::SetLifecycle { id: h.id.clone(), lifecycle: Lifecycle::Ready },
            ]);
        }
    }
    let mut d = Durable::new(voters.clone());
    d.term = 1;
    d.log = cmds
        .into_iter()
        .zip(1..)
        .map(|(cmd, index)| Entry { term: 1, index, payload: EntryPayload::Normal(LogCmd { at, cmd }) })
        .collect();
    d
}

/// The seed auto-approve rule's roles for a profile.
pub fn seed_rule_roles(p: ProfileId) -> BTreeSet<Role> {
    match p {
        ProfileId::Ms01 => [Role::Storage, Role::Vm, Role::Compute].into(),
        ProfileId::MiniPc => [Role::Compute, Role::Transcode, Role::Storage].into(),
        ProfileId::Pi5 | ProfileId::Rk3588 => [Role::Transcode, Role::Compute].into(),
    }
}

/// The network template a profile's seed auto-approve rule applies.
pub fn seed_rule_template(p: ProfileId) -> &'static str {
    match p {
        ProfileId::Ms01 => "ms01-standard",
        ProfileId::MiniPc => "minipc-standard",
        // `sbc-transcode` is profiled Pi5 but deliberately shared by Rk3588 (same eth0-only NICs).
        ProfileId::Pi5 | ProfileId::Rk3588 => "sbc-transcode",
    }
}

/// Host ids are `<prefix>-<nn>`, e.g. `ms01-01`.
pub fn profile_prefix(p: ProfileId) -> &'static str {
    match p {
        ProfileId::Ms01 => "ms01",
        ProfileId::MiniPc => "minipc",
        ProfileId::Pi5 => "pi5",
        ProfileId::Rk3588 => "rk3588",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{ClusterState, NetState};
    use model::NetworkId;

    fn replay(d: &Durable<LogCmd>) -> ClusterState {
        let mut s = ClusterState::new();
        for e in &d.log {
            if let EntryPayload::Normal(LogCmd { at, cmd }) = &e.payload {
                s.apply(*at, cmd.clone()).unwrap_or_else(|err| panic!("entry {} fails: {err}", e.index));
            }
        }
        s
    }

    #[test]
    fn a_host_not_yet_on_its_template_is_provisioned_with_ipam_addresses() {
        let id = HostId::from("ms01-1");
        let mut inventory = Inventory::from_profile(ProfileId::Ms01, 0);
        (inventory.cpu_threads, inventory.ram_gb) = (8, 16);
        let host = SeedHost { id: id.clone(), inventory, location: "lab".into(), running: None };
        let d = seeded_durable(&[host], &[id.clone()].into(), 1_700_000_000_000);
        assert!(d.log.iter().all(|e| matches!(&e.payload, EntryPayload::Normal(c) if c.at == 1_700_000_000_000)));
        let s = replay(&d);
        let h = &s.hosts[&id];
        assert_eq!(h.lifecycle, Lifecycle::Provisioning, "the network controller takes it from here");
        assert_eq!(h.template.as_deref(), Some("ms01-standard"));
        assert_eq!((h.profile.cpu_threads, h.profile.ram_gb), (8, 16), "scheduled on what it has");
        assert_eq!(h.roles, [Role::ControlPlane, Role::Storage, Role::Vm, Role::Compute].into());
        let ip = |n: &str| h.ips.get(&NetworkId::from(n)).map(ToString::to_string);
        assert_eq!(ip("mgmt").as_deref(), Some("10.0.0.2"));
        assert_eq!(ip("storage").as_deref(), Some("10.20.0.2"));
        assert_eq!(ip("migration").as_deref(), Some("10.30.0.2"));
        assert_eq!(ip("vm-lan"), None, "VM networks address guests, not hosts");
        assert_eq!(h.net.state, NetState::Pending);
        assert_eq!(s.rules.len(), 4);
        assert_eq!(s.templates.len(), 3);
    }
}
