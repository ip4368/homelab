//! The alert set, and the alerts raised on hosts from what their agents
//! report.
//!
//! Every alert has one of the kinds in [`AlertKind`], whose severity it
//! takes. An alert's identity is `(kind, entity, subject)`; the state
//! refuses to open a second one with the same identity while one is open.
//!
//! Host alerts, from the inventory in the host object, the network config
//! the host runs and its latest metrics sample:
//!
//! * `PortDown`: a port one of the host's links uses has no link.
//! * `BondDegraded`: a bond has fewer than all of its members up.
//! * `LinkSpeedLow`: a NIC with link negotiated below its profile speed.
//! * `DiskUsageHigh`: a disk more than [`DISK_USAGE_HIGH_PCT`] full.
//!
//! One alert per (kind, host, subject: the port, link or disk), raised while
//! the condition holds and resolved once it clears (or the host is removed).
//! A port that is down whose switch (the one its LLDP last named) has an
//! open `SwitchUnreachable` alert says so in its `PortDown` and
//! `BondDegraded` messages. Ids are stable across controller runs and
//! unique per incident: the leader's term and when the agent saw its
//! observation change (or, for disks, the sample's time).

use super::network::managed;
use super::observe::SWITCH_UNREACHABLE;
use super::{Action, Volatile};
use crate::runtime::SimTime;
use crate::state::{Alert, ClusterState, Command, HostObj, ObjectKind, ObjectRef, Severity};
use model::{HostId, LinkKind, PortName, SwitchId};
use std::collections::{BTreeMap, BTreeSet};

/// Every kind of alert the cluster raises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertKind {
    HostDown,
    CpQuorumAtRisk,
    VolumeFaulted,
    /// Ground truth only: the simulator's, never the control plane's.
    VmSplitBrain,
    BondDegraded,
    PortDown,
    NetworkMismatch,
    NetworkApplyFailed,
    VolumeDegraded,
    DiskUsageHigh,
    LinkSpeedLow,
    InsufficientStorageHosts,
    SwitchUnreachable,
    CpReplacementSuggested,
}

impl AlertKind {
    pub const ALL: [Self; 14] = [
        Self::HostDown,
        Self::CpQuorumAtRisk,
        Self::VolumeFaulted,
        Self::VmSplitBrain,
        Self::BondDegraded,
        Self::PortDown,
        Self::NetworkMismatch,
        Self::NetworkApplyFailed,
        Self::VolumeDegraded,
        Self::DiskUsageHigh,
        Self::LinkSpeedLow,
        Self::InsufficientStorageHosts,
        Self::SwitchUnreachable,
        Self::CpReplacementSuggested,
    ];

    /// The name an alert of this kind carries in `Alert::kind`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HostDown => "HostDown",
            Self::CpQuorumAtRisk => "CpQuorumAtRisk",
            Self::VolumeFaulted => "VolumeFaulted",
            Self::VmSplitBrain => "VmSplitBrain",
            Self::BondDegraded => "BondDegraded",
            Self::PortDown => "PortDown",
            Self::NetworkMismatch => "NetworkMismatch",
            Self::NetworkApplyFailed => "NetworkApplyFailed",
            Self::VolumeDegraded => "VolumeDegraded",
            Self::DiskUsageHigh => "DiskUsageHigh",
            Self::LinkSpeedLow => "LinkSpeedLow",
            Self::InsufficientStorageHosts => "InsufficientStorageHosts",
            Self::SwitchUnreachable => "SwitchUnreachable",
            Self::CpReplacementSuggested => "CpReplacementSuggested",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_str() == name)
    }

    pub const fn severity(self) -> Severity {
        match self {
            Self::HostDown | Self::CpQuorumAtRisk | Self::VolumeFaulted | Self::VmSplitBrain => Severity::Critical,
            Self::BondDegraded
            | Self::PortDown
            | Self::NetworkMismatch
            | Self::NetworkApplyFailed
            | Self::VolumeDegraded
            | Self::DiskUsageHigh
            | Self::LinkSpeedLow
            | Self::InsufficientStorageHosts
            | Self::SwitchUnreachable
            | Self::CpReplacementSuggested => Severity::Warning,
        }
    }
}

pub const PORT_DOWN: &str = AlertKind::PortDown.as_str();
pub const BOND_DEGRADED: &str = AlertKind::BondDegraded.as_str();
pub const LINK_SPEED_LOW: &str = AlertKind::LinkSpeedLow.as_str();
pub const DISK_USAGE_HIGH: &str = AlertKind::DiskUsageHigh.as_str();
/// A disk fuller than this, in percent, raises `DiskUsageHigh`.
pub const DISK_USAGE_HIGH_PCT: f32 = 85.0;

/// The kinds this controller owns.
const HOST_KINDS: [&str; 4] = [PORT_DOWN, BOND_DEGRADED, LINK_SPEED_LOW, DISK_USAGE_HIGH];

pub fn run(state: &ClusterState, v: &Volatile) -> Vec<Action> {
    let mut open: BTreeMap<HostId, Vec<&Alert>> = BTreeMap::new();
    let ours =
        |a: &&Alert| HOST_KINDS.contains(&a.kind.as_str()) && a.resolved_at.is_none() && a.entity.kind == ObjectKind::Host;
    for a in state.alerts.values().filter(ours) {
        open.entry(HostId::new(&a.entity.id)).or_default().push(a);
    }
    let unreachable: BTreeSet<SwitchId> = state
        .alerts
        .values()
        .filter(|a| a.kind == SWITCH_UNREACHABLE && a.resolved_at.is_none() && a.entity.kind == ObjectKind::Switch)
        .map(|a| SwitchId::new(&a.entity.id))
        .collect();
    let mut out = vec![];
    for host in state.hosts.values() {
        let managed = managed(host.lifecycle);
        let wanted = if managed { conditions(host, v, &unreachable) } else { vec![] };
        let mut open = open.remove(&host.id).unwrap_or_default();
        // Without a sample from the host (e.g. a leader elected since), how
        // full its disks are is unknown: their alerts stay as they are.
        if managed && !v.metrics.contains_key(&host.id) {
            open.retain(|a| a.kind != DISK_USAGE_HIGH);
        }
        out.extend(reconcile(state, host, v, &wanted, &open));
    }
    // Alerts of hosts that are gone altogether.
    for a in open.into_values().flatten() {
        out.push(Action::Propose(Command::ResolveAlert { id: a.id.clone() }));
    }
    out
}

/// One alertable condition on a host: what kind, about what (port, link or
/// disk), the start of its ids, the message, and the time stamped into a
/// new alert's id.
struct Condition {
    kind: &'static str,
    subject: String,
    slug: &'static str,
    message: String,
    stamp: SimTime,
    /// False when the message lacks the unreachable-switch note only because
    /// this leader never heard where a port is cabled (e.g. it was elected
    /// after the switch died); an open alert then keeps the note it has.
    note_known: bool,
}

impl Condition {
    fn is(&self, a: &Alert) -> bool {
        a.kind == self.kind && a.subject.as_deref() == Some(self.subject.as_str())
    }
}

/// What is wrong with `host` now: its ports, as its inventory and the
/// network config it runs (or is to run, before any applied) say, and its
/// disks, as its latest sample says. `unreachable` are the switches with an
/// open `SwitchUnreachable` alert.
fn conditions(host: &HostObj, v: &Volatile, unreachable: &BTreeSet<SwitchId>) -> Vec<Condition> {
    let id = &host.id;
    let observed_at = v.host_reports.get(id).map_or(v.elected_at, |r| r.observed_at);
    let condition = |kind: &'static str, slug: &'static str, subject: &str, message: String| Condition {
        kind,
        subject: subject.into(),
        slug,
        message,
        stamp: observed_at,
        note_known: true,
    };
    let nic = |name: &str| host.inventory.nics.iter().find(|n| n.name == name);
    // The switch a port is (or was last) cabled to, as far as this leader heard.
    let peer = |port: &PortName| {
        let now = host.lldp.iter().find(|n| n.local_port == *port).map(|n| &n.switch);
        now.or_else(|| v.lldp_seen.get(id)?.get(port))
    };
    // The note naming the unreachable switches behind ports that are down;
    // `None` if this leader never heard where one of them is cabled.
    let note = |down: &[&PortName]| -> Option<String> {
        let mut dead = BTreeSet::new();
        for p in down {
            let switch = peer(p)?;
            if unreachable.contains(switch) {
                dead.insert(switch.as_str());
            }
        }
        let dead: Vec<&str> = dead.into_iter().collect();
        Some(match dead.as_slice() {
            [] => String::new(),
            [one] => format!(" (peer switch {one} unreachable)"),
            many => format!(" (peer switches {} unreachable)", many.join(", ")),
        })
    };
    let noted = |c: Condition, note: Option<String>| match note {
        Some(note) => Condition { message: c.message + &note, ..c },
        None => Condition { note_known: false, ..c },
    };
    let mut out = vec![];
    if let Some(cfg) = host.net.applied.as_ref().or(host.net.desired.as_ref()) {
        for link in &cfg.links {
            let ports = link.kind.ports();
            let down: Vec<_> = ports.iter().filter(|p| nic(p.as_str()).is_some_and(|n| !n.link_up)).collect();
            for p in &down {
                let c = condition(PORT_DOWN, "portdown", p.as_str(), format!("{id} {p}: link down"));
                out.push(noted(c, note(&[p])));
            }
            if matches!(link.kind, LinkKind::Bond { .. }) && !down.is_empty() {
                let message = format!("{id} {}: {} of {} members up", link.name, ports.len() - down.len(), ports.len());
                out.push(noted(condition(BOND_DEGRADED, "bonddegraded", &link.name, message), note(&down)));
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
    if let Some(sample) = v.metrics.get(id) {
        for (disk, _) in sample.disk_pct.iter().filter(|(_, pct)| **pct > DISK_USAGE_HIGH_PCT) {
            out.push(Condition {
                kind: DISK_USAGE_HIGH,
                subject: disk.clone(),
                slug: "diskusagehigh",
                message: format!("{id} {disk}: more than {DISK_USAGE_HIGH_PCT}% full"),
                stamp: sample.at,
                note_known: true,
            });
        }
    }
    out
}

/// Resolves the host's open alerts no condition wants any more, rewords
/// those whose condition now reads differently, and raises one for each
/// condition with neither an open alert nor a raise in flight. A new
/// alert's id is `<slug>-<host>-<subject>-<term>-<stamp>`, plus a counter if
/// a condition recurs with the same stamp (e.g. the host's config dropped
/// the port and took it back).
fn reconcile(state: &ClusterState, host: &HostObj, v: &Volatile, wanted: &[Condition], open: &[&Alert]) -> Vec<Action> {
    let mut out: Vec<Action> = open
        .iter()
        .filter(|a| !wanted.iter().any(|c| c.is(a)))
        .map(|a| Action::Propose(Command::ResolveAlert { id: a.id.clone() }))
        .collect();
    let entity = ObjectRef::host(&host.id);
    let in_flight = |c: &Condition| {
        v.in_flight.values().any(|cmd| {
            matches!(cmd, Command::RaiseAlert { kind, entity: e, subject, .. }
                if *e == entity && kind == c.kind && subject.as_deref() == Some(c.subject.as_str()))
        })
    };
    for c in wanted {
        if let Some(a) = open.iter().find(|a| c.is(a)) {
            let kept_note = !c.note_known && a.message.starts_with(&c.message);
            if a.message != c.message && !kept_note {
                out.push(Action::Propose(Command::SetAlertMessage { id: a.id.clone(), message: c.message.clone() }));
            }
            continue;
        }
        if in_flight(c) {
            continue;
        }
        let base = format!("{}-{}-{}-{}-{}", c.slug, host.id, c.subject, v.term, c.stamp);
        let id = (1..)
            .map(|n| if n == 1 { base.clone() } else { format!("{base}-{n}") })
            .find(|id| !state.alerts.contains_key(id))
            .expect("some id is free");
        out.push(Action::Propose(Command::RaiseAlert {
            id,
            kind: c.kind.into(),
            entity: entity.clone(),
            subject: Some(c.subject.clone()),
            message: c.message.clone(),
            ground_truth: false,
        }));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::HostReport;
    use crate::proto::Observed;
    use crate::state::{Lifecycle, NetworkResult};
    use model::network::LldpNeighbor;
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
        observe_cabled(s, v, observed_at, &[], edit);
    }

    /// As [`observe`], hearing LLDP from these (port, switch) pairs.
    fn observe_cabled(s: &mut ClusterState, v: &mut Volatile, observed_at: u64, cables: &[(&str, &str)], edit: impl FnOnce(&mut Inventory)) {
        let mut inventory = Inventory::from_profile(ProfileId::Ms01, 0);
        edit(&mut inventory);
        let lldp: Vec<LldpNeighbor> = cables
            .iter()
            .map(|(port, sw)| LldpNeighbor { local_port: PortName::from(*port), switch: SwitchId::from(*sw), switch_port: PortName::from("p1") })
            .collect();
        let observed = Observed { lldp: lldp.clone(), inventory: inventory.clone() };
        v.note_report(hid("m"), HostReport { observed_at, observed });
        s.apply(observed_at, Command::ObserveHost { id: hid("m"), lldp, inventory }).unwrap();
    }

    fn nic<'a>(inv: &'a mut Inventory, name: &str) -> &'a mut model::NicState {
        inv.nics.iter_mut().find(|n| n.name == name).unwrap()
    }

    fn proposals(actions: Vec<Action>) -> Vec<Command> {
        actions.into_iter().map(|a| if let Action::Propose(c) = a { c } else { panic!("unexpected {a:?}") }).collect()
    }

    /// A raise on `m`; the subject is the third part of the id (`<slug>-m-<subject>-...`).
    fn raise(id: &str, kind: &str, message: &str) -> Command {
        raise_on(id, kind, id.split('-').nth(2).unwrap(), message)
    }

    fn raise_on(id: &str, kind: &str, subject: &str, message: &str) -> Command {
        Command::RaiseAlert {
            id: id.into(),
            kind: kind.into(),
            entity: ObjectRef::host(&hid("m")),
            subject: Some(subject.into()),
            message: message.into(),
            ground_truth: false,
        }
    }

    #[test]
    fn every_alert_kind_has_its_severity() {
        use crate::controllers::health::HOST_DOWN;
        use crate::controllers::membership::{CP_QUORUM_AT_RISK, CP_REPLACEMENT_SUGGESTED};
        use crate::controllers::network::{NETWORK_APPLY_FAILED, NETWORK_MISMATCH};
        use crate::controllers::observe::SWITCH_UNREACHABLE;
        use crate::controllers::storage::{INSUFFICIENT_STORAGE_HOSTS, VOLUME_DEGRADED, VOLUME_FAULTED};
        let severity = |kind: &str| AlertKind::parse(kind).unwrap_or_else(|| panic!("{kind} is not in the set")).severity();
        for kind in [HOST_DOWN, CP_QUORUM_AT_RISK, VOLUME_FAULTED, "VmSplitBrain"] {
            assert_eq!(severity(kind), Severity::Critical, "{kind}");
        }
        for kind in [
            BOND_DEGRADED,
            PORT_DOWN,
            NETWORK_MISMATCH,
            NETWORK_APPLY_FAILED,
            VOLUME_DEGRADED,
            DISK_USAGE_HIGH,
            LINK_SPEED_LOW,
            INSUFFICIENT_STORAGE_HOSTS,
            SWITCH_UNREACHABLE,
            CP_REPLACEMENT_SUGGESTED,
        ] {
            assert_eq!(severity(kind), Severity::Warning, "{kind}");
        }
        assert_eq!(AlertKind::parse("Gremlins"), None);
        assert!(AlertKind::ALL.iter().all(|k| AlertKind::parse(k.as_str()) == Some(*k)));
    }

    /// `m` runs bonds `data` (sfp0 + sfp1) and `data-2` (eth0 + eth1).
    fn hyphenated_bonds(s: &mut ClusterState) {
        let mut cfg = s.hosts[&hid("m")].net.desired.clone().unwrap();
        cfg.links[0].name = "data".into();
        let members = vec![PortName::from("eth0"), PortName::from("eth1")];
        cfg.links[1] = model::Link { name: "data-2".into(), kind: LinkKind::Bond { members, mode: model::BondMode::ActiveBackup }, mtu: 1500 };
        s.apply(1, Command::SetDesiredNetwork { id: hid("m"), config: cfg }).unwrap();
        let generation = s.hosts[&hid("m")].net.desired_generation;
        s.apply(1, Command::RecordNetworkResult { id: hid("m"), generation, result: NetworkResult::Applied }).unwrap();
    }

    #[test]
    fn alerts_are_told_apart_by_subject_not_by_id_prefix() {
        let mut s = state();
        hyphenated_bonds(&mut s);
        let mut v = Volatile::new(0, 1);
        observe(&mut s, &mut v, 1000, |inv| nic(inv, "eth0").link_up = false);
        let raised = proposals(run(&s, &v));
        assert_eq!(raised, [
            raise("portdown-m-eth0-1-1000", PORT_DOWN, "m eth0: link down"),
            raise_on("bonddegraded-m-data-2-1-1000", BOND_DEGRADED, "data-2", "m data-2: 1 of 2 members up"),
        ]);
        apply_all(&mut s, 1000, raised);
        // `data` degrades too: its id is a prefix of `data-2`'s, but it is another alert.
        observe(&mut s, &mut v, 2000, |inv| {
            nic(inv, "eth0").link_up = false;
            nic(inv, "sfp0").link_up = false;
        });
        let raised = proposals(run(&s, &v));
        assert_eq!(raised, [
            raise("portdown-m-sfp0-1-2000", PORT_DOWN, "m sfp0: link down"),
            raise_on("bonddegraded-m-data-1-2000", BOND_DEGRADED, "data", "m data: 1 of 2 members up"),
        ]);
        // A raise of the same kind and subject on another host is not this one in flight.
        let mut other = raise_on("bonddegraded-m-x-data-1-2000", BOND_DEGRADED, "data", "m-x data: 1 of 2 members up");
        if let Command::RaiseAlert { entity, .. } = &mut other {
            *entity = ObjectRef::host(&hid("m-x"));
        }
        v.in_flight.insert(7, other);
        assert_eq!(proposals(run(&s, &v)), raised);
        apply_all(&mut s, 2000, raised);
        // `data` recovers; `data-2`'s alert stays open.
        observe(&mut s, &mut v, 3000, |inv| nic(inv, "eth0").link_up = false);
        assert_eq!(proposals(run(&s, &v)), [
            Command::ResolveAlert { id: "bonddegraded-m-data-1-2000".into() },
            Command::ResolveAlert { id: "portdown-m-sfp0-1-2000".into() },
        ]);
    }

    fn sample(at: u64, disk_pct: &[(&str, f32)]) -> crate::proto::MetricSample {
        crate::proto::MetricSample {
            at,
            cpu_pct: 1.0,
            ram_pct: 1.0,
            temp_c: 30.0,
            disk_pct: disk_pct.iter().map(|(d, p)| (d.to_string(), *p)).collect(),
            port_rx_bps: Default::default(),
            port_tx_bps: Default::default(),
            instances: 0,
        }
    }

    #[test]
    fn disk_usage_high_while_a_disk_is_over_85_pct() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        v.note_sample(hid("m"), sample(10_000, &[("nvme0", 85.0), ("nvme1", 20.0)]));
        assert!(run(&s, &v).is_empty(), "85% is not over 85%");
        v.note_sample(hid("m"), sample(20_000, &[("nvme0", 85.5), ("nvme1", 99.0)]));
        let raised = proposals(run(&s, &v));
        let cmd = |disk: &str| raise_on(&format!("diskusagehigh-m-{disk}-1-20000"), DISK_USAGE_HIGH, disk, &format!("m {disk}: more than 85% full"));
        assert_eq!(raised, [cmd("nvme0"), cmd("nvme1")]);
        apply_all(&mut s, 20_000, raised);
        assert_eq!(s.alerts["diskusagehigh-m-nvme0-1-20000"].severity, Severity::Warning);
        v.note_sample(hid("m"), sample(30_000, &[("nvme0", 90.0), ("nvme1", 99.0)]));
        assert!(run(&s, &v).is_empty(), "the same incidents, however full");
        // A new leader has no sample yet: the alerts stay, under their ids, until one comes.
        let mut fresh = Volatile::new(35_000, 2);
        assert!(run(&s, &fresh).is_empty(), "{:?}", run(&s, &fresh));
        fresh.note_sample(hid("m"), sample(40_000, &[("nvme0", 40.0), ("nvme1", 99.0)]));
        assert_eq!(proposals(run(&s, &fresh)), [Command::ResolveAlert { id: "diskusagehigh-m-nvme0-1-20000".into() }]);
        // A host that is removed takes its alerts with it, sample or not.
        s.apply(41_000, Command::RemoveHost { id: hid("m") }).unwrap();
        let resolved = proposals(run(&s, &Volatile::new(41_000, 3)));
        assert!(resolved.contains(&Command::ResolveAlert { id: "diskusagehigh-m-nvme1-1-20000".into() }), "{resolved:?}");
    }

    #[test]
    fn ports_down_behind_an_unreachable_switch_say_so() {
        let mut s = state();
        let mut v = Volatile::new(0, 1);
        let cabled = [("sfp0", "core-1"), ("sfp1", "core-2"), ("eth0", "access-1")];
        observe_cabled(&mut s, &mut v, 1000, &cabled, |_| {});
        // core-2 dies: sfp1 loses link, and with it its LLDP.
        observe_cabled(&mut s, &mut v, 2000, &[cabled[0], cabled[2]], |inv| nic(inv, "sfp1").link_up = false);
        let raised = proposals(run(&s, &v));
        assert_eq!(raised, [
            raise("portdown-m-sfp1-1-2000", PORT_DOWN, "m sfp1: link down"),
            raise("bonddegraded-m-bond0-1-2000", BOND_DEGRADED, "m bond0: 1 of 2 members up"),
        ]);
        apply_all(&mut s, 2000, raised);
        // Polls go unanswered until the switch is declared unreachable.
        let unreachable = Command::RaiseAlert {
            id: "switchdown-core-2".into(),
            kind: "SwitchUnreachable".into(),
            entity: ObjectRef::switch(&SwitchId::from("core-2")),
            subject: None,
            message: "core-2 did not answer polls".into(),
            ground_truth: false,
        };
        s.apply(95_000, unreachable).unwrap();
        let reworded = proposals(run(&s, &v));
        assert_eq!(reworded, [
            Command::SetAlertMessage { id: "portdown-m-sfp1-1-2000".into(), message: "m sfp1: link down (peer switch core-2 unreachable)".into() },
            Command::SetAlertMessage {
                id: "bonddegraded-m-bond0-1-2000".into(),
                message: "m bond0: 1 of 2 members up (peer switch core-2 unreachable)".into()
            },
        ]);
        apply_all(&mut s, 95_000, reworded);
        assert!(run(&s, &v).is_empty());
        // A new leader never heard sfp1's LLDP: it keeps the note rather than drop it.
        let mut fresh = Volatile::new(96_000, 2);
        observe_cabled(&mut s, &mut fresh, 2000, &[cabled[0], cabled[2]], |inv| nic(inv, "sfp1").link_up = false);
        assert!(run(&s, &fresh).is_empty(), "{:?}", run(&s, &fresh));
        // The switch answers again before the link is back: the context goes.
        s.apply(100_000, Command::ResolveAlert { id: "switchdown-core-2".into() }).unwrap();
        assert_eq!(proposals(run(&s, &v)), [
            Command::SetAlertMessage { id: "portdown-m-sfp1-1-2000".into(), message: "m sfp1: link down".into() },
            Command::SetAlertMessage { id: "bonddegraded-m-bond0-1-2000".into(), message: "m bond0: 1 of 2 members up".into() },
        ]);
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
