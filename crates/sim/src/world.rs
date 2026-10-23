//! Simulator ground truth: topology, logical networks and host hardware,
//! plus what each host's OS sees of it (the engine exposes this to the
//! actors on a host as their `HostOs`).

use crate::topology::{Node, Topology};
use control::proto::LogCmd;
use control::raft::Durable;
use control::runtime::HostUsage;
use model::network::{HostNetworkConfig, LogicalNetwork, bootstrap_config, seed_networks};
use model::{DiskState, HardwareProfile, HostId, Inventory, NetworkId, NicState, PortName, ProfileId, profile};
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet};

/// Synthetic idle usage until workloads exist (Task 20): a baseline plus up
/// to the noise on top, re-drawn per reading.
const IDLE_CPU_PCT: f32 = 3.0;
const CPU_NOISE_PCT: f32 = 2.0;
const IDLE_RAM_PCT: f32 = 10.0;
const RAM_NOISE_PCT: f32 = 1.0;
const IDLE_DISK_PCT: f32 = 5.0;

#[derive(Debug, Clone, PartialEq)]
pub struct HostHw {
    pub profile: HardwareProfile,
    /// The catalogue variant `profile` was built from.
    pub variant: u8,
    pub powered: bool,
    /// The network config the host OS has actually applied.
    pub applied_net: HostNetworkConfig,
    /// Names of failed disks.
    pub failed_disks: BTreeSet<String>,
    /// The CP replica's Raft state on this host's disk; survives power loss.
    pub cp_durable: Option<Durable<LogCmd>>,
}

impl HostHw {
    /// A powered-on catalogue machine running its bootstrap network config.
    pub fn new(id: ProfileId, variant: u8) -> Self {
        let profile = profile(id, variant);
        let applied_net = bootstrap_config(&profile);
        Self { profile, variant, powered: true, applied_net, failed_disks: BTreeSet::new(), cp_durable: None }
    }

    /// What the OS measures now: idle usage with seeded noise, every healthy
    /// disk at a fixed fill. Port rates stay empty until traffic is modelled (Task 16).
    pub fn usage(&self, rng: &mut impl Rng) -> HostUsage {
        HostUsage {
            cpu_pct: IDLE_CPU_PCT + CPU_NOISE_PCT * rng.random::<f32>(),
            ram_pct: IDLE_RAM_PCT + RAM_NOISE_PCT * rng.random::<f32>(),
            disk_pct: self
                .profile
                .disks
                .iter()
                .filter(|d| !self.failed_disks.contains(&d.name))
                .map(|d| (d.name.clone(), IDLE_DISK_PCT))
                .collect(),
            port_bps: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct World {
    pub topology: Topology,
    /// VLAN and MTU per network, used to place host attachments on the wire.
    pub networks: BTreeMap<NetworkId, LogicalNetwork>,
    pub hosts: BTreeMap<HostId, HostHw>,
}

impl World {
    /// Empty topology, no hosts, the seed networks.
    pub fn new() -> Self {
        Self {
            topology: Topology::new(),
            networks: seed_networks().into_iter().map(|n| (n.id.clone(), n)).collect(),
            hosts: BTreeMap::new(),
        }
    }

    /// What `host`'s OS discovers about its hardware: the profile's NICs and
    /// disks, each NIC's link state and which disks failed.
    pub fn inventory(&self, host: &HostId) -> Option<Inventory> {
        let hw = self.hosts.get(host)?;
        let nics = hw.profile.nics.iter().map(|n| {
            let port = Node::HostPort(host.clone(), PortName::new(&n.name));
            NicState { name: n.name.clone(), speed_mbps: n.speed_mbps, link_up: hw.powered && self.link_up(&port) }
        });
        let disks = hw.profile.disks.iter().map(|d| DiskState {
            name: d.name.clone(),
            kind: d.kind,
            size_gb: d.size_gb,
            failed: hw.failed_disks.contains(&d.name),
        });
        Some(Inventory { profile: hw.profile.id, variant: hw.variant, nics: nics.collect(), disks: disks.collect() })
    }

    /// Whether the port at `end` has carrier: cabled, the cable plugged in,
    /// and a far-end switch that is up with the port admin-up.
    fn link_up(&self, end: &Node) -> bool {
        let Some(peer) = self.topology.peer(end) else { return false };
        if !self.topology.is_cable_up(end, peer) {
            return false;
        }
        match peer {
            Node::SwitchPort(s, p) => {
                self.topology.switches.get(s).is_some_and(|sw| sw.up && sw.port(p).is_some_and(|c| c.admin_up))
            }
            _ => true,
        }
    }
}

impl Default for World {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{PortMode, Switch, SwitchPortCfg};
    use model::SwitchId;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn sp(p: &str) -> Node {
        Node::SwitchPort(SwitchId::from("sw"), PortName::from(p))
    }

    /// An MS-01 with sfp0 on sw/p1 and eth0 on sw/p2; sfp1 and eth1 uncabled.
    fn world() -> World {
        let mut w = World::new();
        let cfg = SwitchPortCfg { mode: PortMode::Access(1), mtu: 1500, lacp_group: None, admin_up: true };
        let ports = ["p1", "p2"].map(|p| (PortName::from(p), (10_000, cfg.clone()))).into();
        w.topology.add_switch(Switch { id: SwitchId::from("sw"), model: "t".into(), ports, mlag_peer: None, up: true });
        w.topology.connect(Node::HostPort(hid("h"), PortName::from("sfp0")), sp("p1"));
        w.topology.connect(Node::HostPort(hid("h"), PortName::from("eth0")), sp("p2"));
        w.hosts.insert(hid("h"), HostHw::new(ProfileId::Ms01, 1));
        w
    }

    fn links_up(w: &World) -> Vec<(String, bool)> {
        w.inventory(&hid("h")).unwrap().nics.into_iter().map(|n| (n.name, n.link_up)).collect()
    }

    #[test]
    fn inventory_reports_catalogue_hardware_with_live_state() {
        let mut w = world();
        w.hosts.get_mut(&hid("h")).unwrap().failed_disks.insert("nvme1".into());
        let inv = w.inventory(&hid("h")).unwrap();
        assert_eq!((inv.profile, inv.variant), (ProfileId::Ms01, 1));
        let up = |n: &str, b: bool| (n.to_string(), b);
        assert_eq!(links_up(&w), [up("sfp0", true), up("sfp1", false), up("eth0", true), up("eth1", false)]);
        assert_eq!(inv.disks.iter().map(|d| (d.name.as_str(), d.failed)).collect::<Vec<_>>(), [
            ("nvme0", false),
            ("nvme1", true)
        ]);
        assert_eq!(w.inventory(&hid("nope")), None);
    }

    /// Takes something down (`false`) or brings it back (`true`).
    type Toggle = fn(&mut World, bool);

    #[test]
    fn link_goes_down_with_cable_port_switch_or_power() {
        let cases: [(&str, Toggle); 4] = [
            ("unplug", |w, up| w.topology.set_cable_up(&sp("p1"), up)),
            ("port admin down", |w, up| {
                let mut cfg = w.topology.switches[&SwitchId::from("sw")].port(&PortName::from("p1")).unwrap().clone();
                cfg.admin_up = up;
                w.topology.set_port(&SwitchId::from("sw"), &PortName::from("p1"), cfg);
            }),
            ("switch down", |w, up| w.topology.set_switch_up(&SwitchId::from("sw"), up)),
            ("power off", |w, up| w.hosts.get_mut(&hid("h")).unwrap().powered = up),
        ];
        for (what, set) in cases {
            let mut w = world();
            set(&mut w, false);
            assert!(!links_up(&w)[0].1, "{what}: sfp0 still up");
            set(&mut w, true);
            assert!(links_up(&w)[0].1, "{what}: sfp0 did not come back");
        }
    }

    #[test]
    fn idle_usage_is_noisy_around_a_baseline_and_skips_failed_disks() {
        let mut hw = HostHw::new(ProfileId::Ms01, 0);
        hw.failed_disks.insert("nvme1".into());
        let mut rng = ChaCha8Rng::seed_from_u64(1);
        let (a, b) = (hw.usage(&mut rng), hw.usage(&mut rng));
        assert_ne!(a.cpu_pct, b.cpu_pct, "re-drawn per reading");
        for u in [&a, &b] {
            assert!((IDLE_CPU_PCT..IDLE_CPU_PCT + CPU_NOISE_PCT).contains(&u.cpu_pct), "{u:?}");
            assert!((IDLE_RAM_PCT..IDLE_RAM_PCT + RAM_NOISE_PCT).contains(&u.ram_pct), "{u:?}");
            assert_eq!(u.disk_pct, [("nvme0".to_string(), IDLE_DISK_PCT)].into());
            assert!(u.port_bps.is_empty());
        }
    }
}
