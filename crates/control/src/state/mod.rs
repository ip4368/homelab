//! The replicated cluster state machine. Every control-plane replica applies
//! the same committed commands in the same order, so [`ClusterState::apply`]
//! is deterministic: no randomness, and time comes only from `at`.

mod command;
mod objects;

pub use command::{ApplyError, Command};
pub use objects::*;

use crate::runtime::SimTime;
use model::{eligible, HardwareProfile, HostId, NetworkId, Role};
use std::collections::{BTreeMap, BTreeSet};

/// Events kept in the ring; older ones are dropped.
pub const EVENT_RING: usize = 5000;

#[derive(Debug, Clone)]
pub struct ClusterState {
    pub version: u64,
    pub hosts: BTreeMap<HostId, HostObj>,
    pub tokens: BTreeMap<String, JoinToken>,
    pub rules: BTreeMap<String, AutoApproveRule>,
    pub networks: BTreeMap<NetworkId, NetworkObj>,
    pub templates: BTreeMap<String, TemplateObj>,
    pub alerts: BTreeMap<String, Alert>,
    /// Keyed by `seq`, oldest first.
    pub events: BTreeMap<u64, Event>,
    pub cp: CpInfo,
    next_event_seq: u64,
    /// Removed objects and the version at which they went. Unbounded, which is
    /// fine for a prototype. Events leave the ring silently and have no tombstones.
    tombstones: Vec<(u64, ObjectRef)>,
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterState {
    pub fn new() -> Self {
        Self {
            version: 0,
            hosts: BTreeMap::new(),
            tokens: BTreeMap::new(),
            rules: BTreeMap::new(),
            networks: BTreeMap::new(),
            templates: BTreeMap::new(),
            alerts: BTreeMap::new(),
            events: BTreeMap::new(),
            cp: CpInfo { voters: Default::default(), mode: CpMode::Suggest, version: 0 },
            next_event_seq: 1,
            tombstones: Vec::new(),
        }
    }

    /// Applies one committed command at log time `at`. A failed command leaves
    /// the state, including `version`, untouched: each arm validates before it mutates.
    pub fn apply(&mut self, at: SimTime, cmd: Command) -> Result<(), ApplyError> {
        let v = self.version + 1;
        match cmd {
            Command::CreateJoinToken { token, ttl_ms } => {
                if self.tokens.contains_key(&token) {
                    return Err(ApplyError::AlreadyExists);
                }
                let expires_at = at.saturating_add(ttl_ms);
                self.revive(ObjectKind::Token, &token);
                self.tokens.insert(token.clone(), JoinToken { token, expires_at, version: v });
            }
            Command::UpsertRule { id, profile, roles, template } => {
                self.revive(ObjectKind::Rule, &id);
                self.rules.insert(id.clone(), AutoApproveRule { id, profile, roles, template, version: v });
            }
            Command::DeleteRule { id } => {
                if self.rules.remove(&id).is_none() {
                    return Err(ApplyError::NotFound);
                }
                self.tombstone(v, ObjectKind::Rule, id);
            }
            Command::AddPendingHost { id, profile, inventory, location } => {
                if self.hosts.contains_key(&id) {
                    return Err(ApplyError::AlreadyExists);
                }
                self.revive(ObjectKind::Host, id.as_str());
                let host = HostObj {
                    id: id.clone(),
                    profile,
                    inventory,
                    location,
                    lifecycle: Lifecycle::Pending,
                    roles: Default::default(),
                    labels: Default::default(),
                    before_not_ready: None,
                    override_: None,
                    last_transition: at,
                    version: v,
                };
                self.hosts.insert(id, host);
            }
            Command::ApproveHost { id, roles } => {
                let host = self.hosts.get_mut(&id).ok_or(ApplyError::NotFound)?;
                if host.lifecycle != Lifecycle::Pending {
                    return Err(ApplyError::InvalidLifecycle);
                }
                check_eligible(&host.profile, &roles)?;
                host.roles = roles;
                host.lifecycle = Lifecycle::Approved;
                host.last_transition = at;
                host.version = v;
            }
            Command::SetLifecycle { id, lifecycle } => {
                let host = self.hosts.get_mut(&id).ok_or(ApplyError::NotFound)?;
                if !host.lifecycle.can_become(lifecycle) {
                    return Err(ApplyError::InvalidLifecycle);
                }
                // Leaving NotReady for Ready means "recovered": go back to what
                // the host was doing before it dropped out.
                let next = match (host.lifecycle, lifecycle) {
                    (Lifecycle::NotReady, Lifecycle::Ready) => host.before_not_ready.unwrap_or(Lifecycle::Ready),
                    _ => lifecycle,
                };
                host.before_not_ready = (lifecycle == Lifecycle::NotReady).then_some(host.lifecycle);
                host.lifecycle = next;
                host.last_transition = at;
                host.version = v;
            }
            Command::SetRoles { id, roles } => {
                let host = self.hosts.get_mut(&id).ok_or(ApplyError::NotFound)?;
                if matches!(host.lifecycle, Lifecycle::Pending | Lifecycle::Removed) {
                    return Err(ApplyError::InvalidLifecycle);
                }
                check_eligible(&host.profile, &roles)?;
                host.roles = roles;
                host.version = v;
            }
            Command::SetLabels { id, labels } => {
                let host = self.hosts.get_mut(&id).ok_or(ApplyError::NotFound)?;
                host.labels = labels;
                host.version = v;
            }
            Command::RemoveHost { id } => {
                if self.hosts.remove(&id).is_none() {
                    return Err(ApplyError::NotFound);
                }
                self.tombstone(v, ObjectKind::Host, id.to_string());
            }
            Command::RaiseAlert { id, severity, kind, entity, message, ground_truth } => {
                if self.alerts.contains_key(&id) {
                    return Err(ApplyError::AlreadyExists);
                }
                let alert = Alert {
                    id: id.clone(),
                    severity,
                    kind,
                    entity,
                    message,
                    raised_at: at,
                    resolved_at: None,
                    ground_truth,
                    version: v,
                };
                self.alerts.insert(id, alert);
            }
            Command::ResolveAlert { id } => {
                let alert = self.alerts.get_mut(&id).ok_or(ApplyError::NotFound)?;
                if alert.resolved_at.is_some() {
                    return Err(ApplyError::AlreadyExists);
                }
                alert.resolved_at = Some(at);
                alert.version = v;
            }
            Command::Event { entity, message } => {
                let seq = self.next_event_seq;
                self.next_event_seq += 1;
                self.events.insert(seq, Event { seq, at, entity, message, version: v });
                while self.events.len() > EVENT_RING {
                    self.events.pop_first();
                }
            }
            Command::SetCp { voters, mode } => {
                self.cp = CpInfo { voters, mode, version: v };
            }
            Command::UpsertNetwork { network } => {
                self.revive(ObjectKind::Network, network.id.as_str());
                self.networks.insert(network.id.clone(), NetworkObj { network, version: v });
            }
            Command::DeleteNetwork { id } => {
                if !self.networks.contains_key(&id) {
                    return Err(ApplyError::NotFound);
                }
                let users = self.network_users(&id);
                if !users.is_empty() {
                    return Err(ApplyError::InUse(users));
                }
                self.networks.remove(&id);
                self.tombstone(v, ObjectKind::Network, id.to_string());
            }
            Command::UpsertTemplate { template } => {
                self.revive(ObjectKind::Template, &template.id);
                self.templates.insert(template.id.clone(), TemplateObj { template, version: v });
            }
            Command::DeleteTemplate { id } => {
                if !self.templates.contains_key(&id) {
                    return Err(ApplyError::NotFound);
                }
                let users: Vec<_> =
                    self.rules.values().filter(|r| r.template.as_deref() == Some(id.as_str())).map(|r| ObjectRef::rule(&r.id)).collect();
                if !users.is_empty() {
                    return Err(ApplyError::InUse(users));
                }
                self.templates.remove(&id);
                self.tombstone(v, ObjectKind::Template, id);
            }
            Command::SetHostOverride { host, config } => {
                let host = self.hosts.get_mut(&host).ok_or(ApplyError::NotFound)?;
                host.override_ = config;
                host.version = v;
            }
        }
        self.version = v;
        Ok(())
    }

    /// Everything that changed after version `v`: current objects and tombstones.
    pub fn changes_since(&self, v: u64) -> Changes {
        let mut upserts = Vec::new();
        upserts.extend(self.hosts.values().filter(|o| o.version > v).cloned().map(Object::Host));
        upserts.extend(self.tokens.values().filter(|o| o.version > v).cloned().map(Object::Token));
        upserts.extend(self.rules.values().filter(|o| o.version > v).cloned().map(Object::Rule));
        upserts.extend(self.networks.values().filter(|o| o.version > v).cloned().map(Object::Network));
        upserts.extend(self.templates.values().filter(|o| o.version > v).cloned().map(Object::Template));
        upserts.extend(self.alerts.values().filter(|o| o.version > v).cloned().map(Object::Alert));
        upserts.extend(self.events.values().filter(|o| o.version > v).cloned().map(Object::Event));
        if self.cp.version > v {
            upserts.push(Object::Cp(self.cp.clone()));
        }
        let removed = self.tombstones.iter().filter(|(at, _)| *at > v).map(|(_, r)| r.clone()).collect();
        Changes { version: self.version, upserts, removed }
    }

    /// The templates and hosts (through their override) that attach `network`.
    fn network_users(&self, network: &NetworkId) -> Vec<ObjectRef> {
        let attaches = |cfg: &model::HostNetworkConfig| cfg.attachments.iter().any(|a| a.network == *network);
        let templates = self.templates.values().filter(|t| attaches(&t.template.config)).map(|t| ObjectRef::template(&t.template.id));
        let hosts = self.hosts.values().filter(|h| h.override_.as_ref().is_some_and(attaches)).map(|h| ObjectRef::host(&h.id));
        let mut users: Vec<_> = templates.chain(hosts).collect();
        users.sort();
        users
    }

    pub(crate) fn tombstone(&mut self, v: u64, kind: ObjectKind, id: String) {
        self.tombstones.push((v, ObjectRef { kind, id }));
    }

    /// An object is (re)created: a tombstone for the same ref would contradict
    /// the upsert, so drop it.
    pub(crate) fn revive(&mut self, kind: ObjectKind, id: &str) {
        self.tombstones.retain(|(_, r)| !(r.kind == kind && r.id == id));
    }
}

fn check_eligible(profile: &HardwareProfile, roles: &BTreeSet<Role>) -> Result<(), ApplyError> {
    for &role in roles {
        eligible(profile, role).map_err(|reason| ApplyError::IneligibleRole { role, reason })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{profile, Inventory, NetworkId, NetworkTemplate, ProfileId, Role};
    use std::collections::{BTreeMap, BTreeSet};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn roles(r: &[Role]) -> BTreeSet<Role> {
        r.iter().copied().collect()
    }

    fn add_cmd(id: &str, p: ProfileId, variant: u8) -> Command {
        Command::AddPendingHost {
            id: hid(id),
            profile: profile(p, variant),
            inventory: Inventory::from_profile(p, variant),
            location: "rack-1".into(),
        }
    }

    fn ready_host(s: &mut ClusterState, id: &str, p: ProfileId, variant: u8) {
        s.apply(1, add_cmd(id, p, variant)).unwrap();
        s.apply(2, Command::ApproveHost { id: hid(id), roles: roles(&[Role::Compute]) }).unwrap();
        s.apply(3, Command::SetLifecycle { id: hid(id), lifecycle: Lifecycle::Provisioning }).unwrap();
        s.apply(4, Command::SetLifecycle { id: hid(id), lifecycle: Lifecycle::Ready }).unwrap();
    }

    fn host_ref(id: &str) -> ObjectRef {
        ObjectRef::host(&hid(id))
    }

    #[test]
    fn apply_bumps_version_and_tracks_changes() {
        let mut s = ClusterState::new();
        s.apply(100, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        assert_eq!(s.version, 1);
        let c = s.changes_since(0);
        assert_eq!(c.version, 1);
        assert_eq!(c.upserts.len(), 1);
        let Object::Host(h) = &c.upserts[0] else { panic!("expected host, got {:?}", c.upserts[0]) };
        assert_eq!((h.id.as_str(), h.lifecycle, h.version, h.last_transition), ("h1", Lifecycle::Pending, 1, 100));
        assert_eq!(h.location, "rack-1");
        assert!(h.roles.is_empty());
        let c = s.changes_since(s.version);
        assert!(c.upserts.is_empty() && c.removed.is_empty());
        assert_eq!(c.version, 1);
    }

    #[test]
    fn changes_since_returns_only_newer_objects() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        s.apply(2, add_cmd("h2", ProfileId::MiniPc, 0)).unwrap();
        let c = s.changes_since(1);
        assert_eq!(c.upserts.len(), 1);
        assert!(matches!(&c.upserts[0], Object::Host(h) if h.id == hid("h2")));
    }

    #[test]
    fn object_json_is_tagged_by_kind() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        let v = serde_json::to_value(&s.changes_since(0).upserts[0]).unwrap();
        assert_eq!(v["type"], "Host");
        assert_eq!(v["id"], "h1");
        assert_eq!(v["lifecycle"], "Pending");
    }

    #[test]
    fn add_pending_host_twice_is_rejected_without_side_effects() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        assert_eq!(s.apply(2, add_cmd("h1", ProfileId::MiniPc, 0)), Err(ApplyError::AlreadyExists));
        assert_eq!(s.version, 1);
    }

    #[test]
    fn approve_moves_pending_to_approved_with_roles() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::Ms01, 0)).unwrap();
        s.apply(50, Command::ApproveHost { id: hid("h1"), roles: roles(&[Role::Storage, Role::Vm]) }).unwrap();
        let h = &s.hosts[&hid("h1")];
        assert_eq!(h.lifecycle, Lifecycle::Approved);
        assert_eq!(h.roles, roles(&[Role::Storage, Role::Vm]));
        assert_eq!((h.last_transition, h.version), (50, 2));
    }

    #[test]
    fn approve_requires_pending() {
        let mut s = ClusterState::new();
        ready_host(&mut s, "h1", ProfileId::MiniPc, 0);
        let v = s.version;
        let r = s.apply(9, Command::ApproveHost { id: hid("h1"), roles: roles(&[Role::Compute]) });
        assert_eq!(r, Err(ApplyError::InvalidLifecycle));
        assert_eq!(s.version, v);
        assert_eq!(
            s.apply(9, Command::ApproveHost { id: hid("nope"), roles: roles(&[]) }),
            Err(ApplyError::NotFound)
        );
    }

    #[test]
    fn approve_rejects_ineligible_role() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("pi", ProfileId::Pi5, 0)).unwrap(); // SD card
        let err = s
            .apply(2, Command::ApproveHost { id: hid("pi"), roles: roles(&[Role::Compute, Role::Storage]) })
            .unwrap_err();
        assert!(matches!(&err, ApplyError::IneligibleRole { role: Role::Storage, .. }), "{err:?}");
        assert!(err.to_string().contains("Storage"), "{err}");
        // nothing changed: still pending, no roles
        assert_eq!(s.hosts[&hid("pi")].lifecycle, Lifecycle::Pending);
        assert!(s.hosts[&hid("pi")].roles.is_empty());
        assert_eq!(s.version, 1);
    }

    #[test]
    fn set_lifecycle_follows_the_transition_table() {
        let mut s = ClusterState::new();
        ready_host(&mut s, "h1", ProfileId::MiniPc, 0);
        let set = |s: &mut ClusterState, l| s.apply(10, Command::SetLifecycle { id: hid("h1"), lifecycle: l });
        assert_eq!(set(&mut s, Lifecycle::Drained), Err(ApplyError::InvalidLifecycle));
        assert_eq!(set(&mut s, Lifecycle::Ready), Err(ApplyError::InvalidLifecycle));
        assert_eq!(set(&mut s, Lifecycle::Removed), Err(ApplyError::InvalidLifecycle));
        set(&mut s, Lifecycle::Draining).unwrap();
        set(&mut s, Lifecycle::Drained).unwrap();
        set(&mut s, Lifecycle::Ready).unwrap();
        set(&mut s, Lifecycle::NotReady).unwrap();
        set(&mut s, Lifecycle::Ready).unwrap();
        assert_eq!(s.hosts[&hid("h1")].last_transition, 10);
        assert_eq!(
            s.apply(1, Command::SetLifecycle { id: hid("zz"), lifecycle: Lifecycle::Ready }),
            Err(ApplyError::NotFound)
        );
    }

    #[test]
    fn not_ready_recovery_restores_prior_lifecycle() {
        let set = |s: &mut ClusterState, l| s.apply(10, Command::SetLifecycle { id: hid("h1"), lifecycle: l });
        // Ready -> NotReady -> Ready
        let mut s = ClusterState::new();
        ready_host(&mut s, "h1", ProfileId::MiniPc, 0);
        set(&mut s, Lifecycle::NotReady).unwrap();
        assert_eq!(s.hosts[&hid("h1")].before_not_ready, Some(Lifecycle::Ready));
        set(&mut s, Lifecycle::Ready).unwrap();
        assert_eq!(s.hosts[&hid("h1")].lifecycle, Lifecycle::Ready);
        assert_eq!(s.hosts[&hid("h1")].before_not_ready, None);
        // Cordoned, Draining and Drained hosts come back as they were
        for prior in [Lifecycle::Cordoned, Lifecycle::Draining, Lifecycle::Drained] {
            let mut s = ClusterState::new();
            ready_host(&mut s, "h1", ProfileId::MiniPc, 0);
            if prior == Lifecycle::Cordoned {
                set(&mut s, Lifecycle::Cordoned).unwrap();
            } else {
                set(&mut s, Lifecycle::Draining).unwrap();
                if prior == Lifecycle::Drained {
                    set(&mut s, Lifecycle::Drained).unwrap();
                }
            }
            set(&mut s, Lifecycle::NotReady).unwrap();
            set(&mut s, Lifecycle::Ready).unwrap();
            assert_eq!(s.hosts[&hid("h1")].lifecycle, prior);
            assert_eq!(s.hosts[&hid("h1")].before_not_ready, None);
        }
        // leaving NotReady any other way clears the memory
        let mut s = ClusterState::new();
        ready_host(&mut s, "h1", ProfileId::MiniPc, 0);
        set(&mut s, Lifecycle::Cordoned).unwrap();
        set(&mut s, Lifecycle::NotReady).unwrap();
        set(&mut s, Lifecycle::Draining).unwrap();
        assert_eq!(s.hosts[&hid("h1")].before_not_ready, None);
        // a Provisioning host cannot go NotReady
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        s.apply(2, Command::ApproveHost { id: hid("h1"), roles: roles(&[]) }).unwrap();
        set(&mut s, Lifecycle::Provisioning).unwrap();
        assert_eq!(set(&mut s, Lifecycle::NotReady), Err(ApplyError::InvalidLifecycle));
    }

    #[test]
    fn every_object_variant_roundtrips_through_json() {
        let mut s = ClusterState::new();
        ready_host(&mut s, "h1", ProfileId::MiniPc, 0);
        s.apply(5, Command::SetLifecycle { id: hid("h1"), lifecycle: Lifecycle::NotReady }).unwrap();
        s.apply(5, Command::CreateJoinToken { token: "t".into(), ttl_ms: 9 }).unwrap();
        s.apply(5, Command::UpsertRule {
            id: "r".into(),
            profile: ProfileId::Pi5,
            roles: roles(&[Role::Compute]),
            template: Some("x".into()),
        })
        .unwrap();
        s.apply(5, Command::UpsertNetwork { network: model::seed_networks().remove(1) }).unwrap();
        s.apply(5, Command::UpsertTemplate { template: model::seed_templates().remove(0) }).unwrap();
        s.apply(5, Command::SetHostOverride { host: hid("h1"), config: Some(model::seed_templates().remove(2).config) }).unwrap();
        s.apply(5, Command::RaiseAlert {
            id: "a".into(),
            severity: Severity::Warning,
            kind: "HostDown".into(),
            entity: host_ref("h1"),
            message: "m".into(),
            ground_truth: false,
        })
        .unwrap();
        s.apply(5, Command::Event { entity: host_ref("h1"), message: "e".into() }).unwrap();
        s.apply(5, Command::SetCp { voters: [hid("h1")].into(), mode: CpMode::Auto }).unwrap();
        let upserts = s.changes_since(0).upserts;
        let kinds: Vec<_> = upserts.iter().map(|o| serde_json::to_value(o).unwrap()["type"].clone()).collect();
        assert_eq!(kinds, ["Host", "Token", "Rule", "Network", "Template", "Alert", "Event", "Cp"]);
        for o in upserts {
            let json = serde_json::to_string(&o).unwrap();
            let back: Object = serde_json::from_str(&json).unwrap();
            assert_eq!(back, o, "{json}");
        }
        // networks and templates are flat: the wire shape is the model type plus `version`
        let net = serde_json::to_value(Object::Network(s.networks[&NetworkId::from("storage")].clone())).unwrap();
        assert_eq!((net["type"].as_str(), net["id"].as_str(), net["vlan"].as_u64()), (Some("Network"), Some("storage"), Some(20)));
        assert_eq!(net["subnet"], "10.20.0.0/22");
        assert!(net["version"].is_u64());
        let tpl = serde_json::to_value(Object::Template(s.templates["ms01-standard"].clone())).unwrap();
        assert_eq!((tpl["type"].as_str(), tpl["id"].as_str(), tpl["profile"].as_str()), (Some("Template"), Some("ms01-standard"), Some("Ms01")));
        // Alert keeps its own `kind` field next to the `type` tag
        let alert = serde_json::to_value(Object::Alert(s.alerts["a"].clone())).unwrap();
        assert_eq!((alert["type"].as_str(), alert["kind"].as_str()), (Some("Alert"), Some("HostDown")));
    }

    #[test]
    fn set_roles_checks_eligibility_and_lifecycle() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("pi", ProfileId::Pi5, 1)).unwrap(); // NVMe HAT, 8 GB
        let set = |s: &mut ClusterState, r: &[Role]| s.apply(5, Command::SetRoles { id: hid("pi"), roles: roles(r) });
        assert_eq!(set(&mut s, &[Role::Compute]), Err(ApplyError::InvalidLifecycle)); // still pending
        s.apply(2, Command::ApproveHost { id: hid("pi"), roles: roles(&[Role::Compute]) }).unwrap();
        set(&mut s, &[Role::Compute, Role::Storage, Role::Transcode]).unwrap();
        assert_eq!(s.hosts[&hid("pi")].roles, roles(&[Role::Compute, Role::Storage, Role::Transcode]));
        let err = set(&mut s, &[Role::Vm]).unwrap_err();
        assert!(matches!(err, ApplyError::IneligibleRole { role: Role::Vm, .. }));
        assert_eq!(s.hosts[&hid("pi")].roles.len(), 3);
    }

    #[test]
    fn set_labels_replaces_labels() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        let l = |k: &str, v: &str| BTreeMap::from([(k.to_string(), v.to_string())]);
        s.apply(2, Command::SetLabels { id: hid("h1"), labels: l("zone", "a") }).unwrap();
        s.apply(3, Command::SetLabels { id: hid("h1"), labels: l("gpu", "no") }).unwrap();
        assert_eq!(s.hosts[&hid("h1")].labels, l("gpu", "no"));
        assert_eq!(s.hosts[&hid("h1")].version, 3);
        assert_eq!(
            s.apply(4, Command::SetLabels { id: hid("zz"), labels: BTreeMap::new() }),
            Err(ApplyError::NotFound)
        );
    }

    #[test]
    fn remove_host_leaves_tombstone() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        let v = s.version;
        s.apply(2, Command::RemoveHost { id: hid("h1") }).unwrap();
        assert!(s.hosts.is_empty());
        let c = s.changes_since(v);
        assert_eq!(c.removed, vec![host_ref("h1")]);
        assert!(c.upserts.is_empty());
        // clients that already saw the removal are not told again
        assert!(s.changes_since(s.version).removed.is_empty());
        assert_eq!(s.apply(3, Command::RemoveHost { id: hid("h1") }), Err(ApplyError::NotFound));
    }

    #[test]
    fn readding_a_removed_host_drops_its_tombstone() {
        let mut s = ClusterState::new();
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        s.apply(2, Command::RemoveHost { id: hid("h1") }).unwrap();
        s.apply(3, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        let c = s.changes_since(0);
        assert!(c.removed.is_empty());
        assert_eq!(c.upserts.len(), 1);
    }

    #[test]
    fn join_tokens_expire_at_log_time_plus_ttl() {
        let mut s = ClusterState::new();
        s.apply(1_000, Command::CreateJoinToken { token: "t1".into(), ttl_ms: 600_000 }).unwrap();
        assert_eq!(s.tokens["t1"].expires_at, 601_000);
        assert_eq!(
            s.apply(2_000, Command::CreateJoinToken { token: "t1".into(), ttl_ms: 1 }),
            Err(ApplyError::AlreadyExists)
        );
        assert_eq!(s.tokens["t1"].expires_at, 601_000);
    }

    #[test]
    fn rules_upsert_replace_and_delete_with_tombstone() {
        let mut s = ClusterState::new();
        let up = |tpl: Option<&str>| Command::UpsertRule {
            id: "rk".into(),
            profile: ProfileId::Rk3588,
            roles: roles(&[Role::Transcode, Role::Compute]),
            template: tpl.map(String::from),
        };
        s.apply(1, up(None)).unwrap();
        s.apply(2, up(Some("sbc-transcode"))).unwrap();
        assert_eq!(s.rules.len(), 1);
        assert_eq!(s.rules["rk"].template.as_deref(), Some("sbc-transcode"));
        assert_eq!(s.rules["rk"].version, 2);
        s.apply(3, Command::DeleteRule { id: "rk".into() }).unwrap();
        let c = s.changes_since(2);
        assert_eq!(c.removed, vec![ObjectRef { kind: ObjectKind::Rule, id: "rk".into() }]);
        assert_eq!(s.apply(4, Command::DeleteRule { id: "rk".into() }), Err(ApplyError::NotFound));
    }

    #[test]
    fn alerts_raise_and_resolve() {
        let mut s = ClusterState::new();
        let raise = || Command::RaiseAlert {
            id: "down-h1".into(),
            severity: Severity::Critical,
            kind: "HostDown".into(),
            entity: host_ref("h1"),
            message: "no heartbeat".into(),
            ground_truth: true,
        };
        s.apply(10, raise()).unwrap();
        assert_eq!(s.apply(11, raise()), Err(ApplyError::AlreadyExists));
        let a = &s.alerts["down-h1"];
        assert_eq!((a.raised_at, a.resolved_at, a.ground_truth), (10, None, true));
        s.apply(20, Command::ResolveAlert { id: "down-h1".into() }).unwrap();
        assert_eq!(s.alerts["down-h1"].resolved_at, Some(20));
        // resolving twice is rejected, resolving a stranger is not found
        assert_eq!(s.apply(21, Command::ResolveAlert { id: "down-h1".into() }), Err(ApplyError::AlreadyExists));
        assert_eq!(s.apply(22, Command::ResolveAlert { id: "x".into() }), Err(ApplyError::NotFound));
        assert!(s.changes_since(1).upserts.iter().any(|o| matches!(o, Object::Alert(a) if a.resolved_at == Some(20))));
    }

    #[test]
    fn events_get_increasing_seq() {
        let mut s = ClusterState::new();
        for m in ["a", "b"] {
            s.apply(7, Command::Event { entity: host_ref("h1"), message: m.into() }).unwrap();
        }
        let seqs: Vec<_> = s.events.values().map(|e| (e.seq, e.at, e.message.as_str())).collect();
        assert_eq!(seqs, [(1, 7, "a"), (2, 7, "b")]);
    }

    #[test]
    fn events_ring_caps_at_5000() {
        let mut s = ClusterState::new();
        for i in 0..5_010u64 {
            s.apply(i, Command::Event { entity: ObjectRef::cp(), message: format!("e{i}") }).unwrap();
        }
        assert_eq!(s.events.len(), EVENT_RING);
        assert_eq!(s.events.values().next().unwrap().seq, 11);
        assert_eq!(s.events.values().last().unwrap().seq, 5_010);
        assert_eq!(s.events.values().next().unwrap().message, "e10");
        // the ring is what a fresh client sees
        assert_eq!(s.changes_since(0).upserts.len(), 5_000);
    }

    #[test]
    fn set_cp_replaces_membership() {
        let mut s = ClusterState::new();
        let voters: BTreeSet<_> = ["a", "b", "c"].into_iter().map(hid).collect();
        s.apply(1, Command::SetCp { voters: voters.clone(), mode: CpMode::Auto }).unwrap();
        assert_eq!((s.cp.voters.clone(), s.cp.mode, s.cp.version), (voters, CpMode::Auto, 1));
        assert!(s.changes_since(0).upserts.iter().any(|o| matches!(o, Object::Cp(_))));
    }

    fn seeded_networks() -> ClusterState {
        let mut s = ClusterState::new();
        for n in model::seed_networks() {
            s.apply(1, Command::UpsertNetwork { network: n }).unwrap();
        }
        s
    }

    fn template(id: &str) -> NetworkTemplate {
        model::seed_templates().into_iter().find(|t| t.id == id).unwrap()
    }

    fn nid(s: &str) -> NetworkId {
        NetworkId::from(s)
    }

    fn net_ref(id: &str) -> ObjectRef {
        ObjectRef { kind: ObjectKind::Network, id: id.into() }
    }

    #[test]
    fn networks_upsert_replace_and_delete_with_tombstone() {
        let mut s = seeded_networks();
        assert_eq!(s.networks.len(), 5);
        assert_eq!(s.networks[&nid("storage")].network.vlan, 20);
        let v = s.version;
        let mut changed = s.networks[&nid("storage")].network.clone();
        (changed.vlan, changed.mtu) = (21, 8000);
        s.apply(2, Command::UpsertNetwork { network: changed }).unwrap();
        assert_eq!((s.networks[&nid("storage")].network.vlan, s.networks[&nid("storage")].version), (21, v + 1));
        s.apply(3, Command::DeleteNetwork { id: nid("vm-iot") }).unwrap();
        assert!(!s.networks.contains_key(&nid("vm-iot")));
        assert_eq!(s.changes_since(v + 1).removed, vec![net_ref("vm-iot")]);
        assert_eq!(s.apply(4, Command::DeleteNetwork { id: nid("vm-iot") }), Err(ApplyError::NotFound));
        // recreating drops the tombstone
        s.apply(5, Command::UpsertNetwork { network: model::seed_networks().pop().unwrap() }).unwrap();
        assert!(s.changes_since(0).removed.is_empty());
    }

    #[test]
    fn delete_in_use_network_rejected() {
        let mut s = seeded_networks();
        s.apply(2, Command::UpsertTemplate { template: template("ms01-standard") }).unwrap();
        let v = s.version;
        let err = s.apply(3, Command::DeleteNetwork { id: nid("storage") }).unwrap_err();
        assert_eq!(err, ApplyError::InUse(vec![ObjectRef::template("ms01-standard")]));
        assert_eq!(err.to_string(), "in use by template ms01-standard");
        assert_eq!(s.version, v, "a rejected delete changes nothing");
        assert!(s.networks.contains_key(&nid("storage")));
        // a network no template carries can go
        s.apply(4, Command::UpsertTemplate { template: template("sbc-transcode") }).unwrap();
        s.apply(5, Command::DeleteTemplate { id: "ms01-standard".into() }).unwrap();
        s.apply(6, Command::DeleteNetwork { id: nid("storage") }).unwrap();
    }

    #[test]
    fn delete_network_lists_every_template_and_host_override_using_it() {
        let mut s = seeded_networks();
        ready_host(&mut s, "h1", ProfileId::MiniPc, 0);
        ready_host(&mut s, "h2", ProfileId::MiniPc, 0);
        for t in ["ms01-standard", "minipc-standard", "sbc-transcode"] {
            s.apply(5, Command::UpsertTemplate { template: template(t) }).unwrap();
        }
        s.apply(6, Command::SetHostOverride { host: hid("h2"), config: Some(template("minipc-standard").config) }).unwrap();
        let err = s.apply(7, Command::DeleteNetwork { id: nid("storage") }).unwrap_err();
        let want = [ObjectRef::host(&hid("h2")), ObjectRef::template("minipc-standard"), ObjectRef::template("ms01-standard")];
        let ApplyError::InUse(mut got) = err else { panic!("{err:?}") };
        got.sort();
        let mut want = want.to_vec();
        want.sort();
        assert_eq!(got, want);
        // h1 has no override: clearing h2's frees storage only from the host side
        s.apply(8, Command::SetHostOverride { host: hid("h2"), config: None }).unwrap();
        let ApplyError::InUse(got) = s.apply(9, Command::DeleteNetwork { id: nid("storage") }).unwrap_err() else { panic!() };
        assert_eq!(got, [ObjectRef::template("minipc-standard"), ObjectRef::template("ms01-standard")]);
        // vm-iot is carried by ms01-standard only
        let ApplyError::InUse(got) = s.apply(9, Command::DeleteNetwork { id: nid("vm-iot") }).unwrap_err() else { panic!() };
        assert_eq!(got, [ObjectRef::template("ms01-standard")]);
    }

    #[test]
    fn upsert_network_with_changed_vlan_or_mtu_is_allowed_while_in_use() {
        let mut s = seeded_networks();
        s.apply(2, Command::UpsertTemplate { template: template("minipc-standard") }).unwrap();
        let mut storage = s.networks[&nid("storage")].network.clone();
        storage.vlan = 25;
        storage.mtu = 9216;
        s.apply(3, Command::UpsertNetwork { network: storage.clone() }).unwrap();
        assert_eq!(s.networks[&nid("storage")].network, storage);
    }

    #[test]
    fn templates_upsert_replace_and_delete_with_tombstone() {
        let mut s = ClusterState::new();
        s.apply(1, Command::UpsertTemplate { template: template("sbc-transcode") }).unwrap();
        let mut t = template("sbc-transcode");
        t.config.links[0].mtu = 1400;
        s.apply(2, Command::UpsertTemplate { template: t }).unwrap();
        assert_eq!((s.templates.len(), s.templates["sbc-transcode"].template.config.links[0].mtu, s.templates["sbc-transcode"].version), (1, 1400, 2));
        s.apply(3, Command::DeleteTemplate { id: "sbc-transcode".into() }).unwrap();
        assert_eq!(s.changes_since(2).removed, vec![ObjectRef::template("sbc-transcode")]);
        assert_eq!(s.apply(4, Command::DeleteTemplate { id: "sbc-transcode".into() }), Err(ApplyError::NotFound));
    }

    #[test]
    fn delete_template_in_use_by_rules_rejected() {
        let mut s = ClusterState::new();
        s.apply(1, Command::UpsertTemplate { template: template("sbc-transcode") }).unwrap();
        for (id, p) in [("pi5", ProfileId::Pi5), ("rk3588", ProfileId::Rk3588)] {
            s.apply(2, Command::UpsertRule { id: id.into(), profile: p, roles: roles(&[Role::Compute]), template: Some("sbc-transcode".into()) })
                .unwrap();
        }
        s.apply(2, Command::UpsertRule { id: "other".into(), profile: ProfileId::Ms01, roles: roles(&[]), template: None }).unwrap();
        let v = s.version;
        let err = s.apply(3, Command::DeleteTemplate { id: "sbc-transcode".into() }).unwrap_err();
        assert_eq!(err, ApplyError::InUse(vec![ObjectRef::rule("pi5"), ObjectRef::rule("rk3588")]));
        assert_eq!(err.to_string(), "in use by rule pi5, rule rk3588");
        assert_eq!(s.version, v);
        s.apply(4, Command::DeleteRule { id: "pi5".into() }).unwrap();
        s.apply(4, Command::UpsertRule { id: "rk3588".into(), profile: ProfileId::Rk3588, roles: roles(&[]), template: None }).unwrap();
        s.apply(5, Command::DeleteTemplate { id: "sbc-transcode".into() }).unwrap();
    }

    #[test]
    fn set_host_override_sets_and_clears() {
        let mut s = ClusterState::new();
        assert_eq!(
            s.apply(1, Command::SetHostOverride { host: hid("h1"), config: None }),
            Err(ApplyError::NotFound)
        );
        s.apply(1, add_cmd("h1", ProfileId::MiniPc, 0)).unwrap();
        assert_eq!(s.hosts[&hid("h1")].override_, None);
        let cfg = template("sbc-transcode").config;
        s.apply(2, Command::SetHostOverride { host: hid("h1"), config: Some(cfg.clone()) }).unwrap();
        assert_eq!((s.hosts[&hid("h1")].override_.clone(), s.hosts[&hid("h1")].version), (Some(cfg), 2));
        assert!(s.changes_since(1).upserts.iter().any(|o| matches!(o, Object::Host(h) if h.override_.is_some())));
        s.apply(3, Command::SetHostOverride { host: hid("h1"), config: None }).unwrap();
        assert_eq!(s.hosts[&hid("h1")].override_, None);
    }

    #[test]
    fn apply_is_deterministic() {
        let cmds = vec![
            (1, Command::CreateJoinToken { token: "t".into(), ttl_ms: 5 }),
            (2, add_cmd("h1", ProfileId::Ms01, 0)),
            (3, Command::ApproveHost { id: hid("h1"), roles: roles(&[Role::Vm, Role::Storage]) }),
            (4, Command::Event { entity: host_ref("h1"), message: "x".into() }),
            (5, Command::RemoveHost { id: hid("h1") }),
        ];
        let run = || {
            let mut s = ClusterState::new();
            for (at, c) in cmds.clone() {
                s.apply(at, c).unwrap();
            }
            serde_json::to_string(&s.changes_since(0)).unwrap()
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn command_roundtrips_through_json() {
        let c = add_cmd("h1", ProfileId::Rk3588, 1);
        let back: Command = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }
}
