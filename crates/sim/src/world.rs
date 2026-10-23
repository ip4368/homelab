//! Simulator ground truth: topology, logical networks and host hardware,
//! plus what each host's OS sees of it (the engine exposes this to the
//! actors on a host as their `HostOs`).

use crate::topology::{Node, Topology};
use crate::traffic::Traffic;
use control::proto::LogCmd;
use control::raft::Durable;
use control::runtime::{HostUsage, SwitchConfig};
use model::network::{
    HostNetworkConfig, LldpNeighbor, LogicalNetwork, ObservedSwitchPort, bootstrap_config, seed_networks,
};
use model::{DiskState, HardwareProfile, HostId, Inventory, NetworkId, NicState, PortName, ProfileId, SwitchId, profile};
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
    /// The network config written to disk, which the host boots with: the
    /// last one its agent confirmed (an unconfirmed trial is not written).
    pub boot_net: HostNetworkConfig,
    /// Names of failed disks.
    pub failed_disks: BTreeSet<String>,
    /// NICs that negotiate below their own speed (e.g. a bad cable), by name, in Mbit/s.
    pub degraded_nics: BTreeMap<String, u32>,
    /// The CP replica's Raft state on this host's disk; survives power loss.
    pub cp_durable: Option<Durable<LogCmd>>,
    /// The token the host's agent joins with, until the agent becomes a
    /// member ([`HostHw::mark_joined`]); `None` for a member. A restored host
    /// boots its agent with whatever is stored here.
    pub join_token: Option<String>,
}

impl HostHw {
    /// A powered-on catalogue machine running (and booting) its bootstrap network config.
    pub fn new(id: ProfileId, variant: u8) -> Self {
        let profile = profile(id, variant);
        Self::running(id, variant, bootstrap_config(&profile))
    }

    /// A powered-on catalogue machine running (and booting) `net`.
    pub fn running(id: ProfileId, variant: u8, net: HostNetworkConfig) -> Self {
        Self {
            profile: profile(id, variant),
            variant,
            powered: true,
            boot_net: net.clone(),
            applied_net: net,
            failed_disks: BTreeSet::new(),
            degraded_nics: BTreeMap::new(),
            cp_durable: None,
            join_token: None,
        }
    }

    /// Powers the host on: it boots the network config written to disk.
    pub fn power_on(&mut self) {
        self.powered = true;
        self.applied_net = self.boot_net.clone();
    }

    /// Writes the running network config as the one to boot with.
    pub fn persist_network(&mut self) {
        self.boot_net = self.applied_net.clone();
    }

    /// The agent became a cluster member: it never needs the token again.
    pub fn mark_joined(&mut self) {
        self.join_token = None;
    }

    /// What the OS measures now: idle usage with seeded noise, every healthy
    /// disk at a fixed fill. Port rates are the world's to fill ([`World::usage`]).
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
    /// The flows on the wire and what the last traffic tick made of them.
    pub traffic: Traffic,
}

impl World {
    /// Empty topology, no hosts, the seed networks, no traffic.
    pub fn new() -> Self {
        Self {
            topology: Topology::new(),
            networks: seed_networks().into_iter().map(|n| (n.id.clone(), n)).collect(),
            hosts: BTreeMap::new(),
            traffic: Traffic::default(),
        }
    }

    /// The speed `host`'s NIC `port` negotiates, in Mbit/s: its own (or the
    /// degraded speed it is stuck at), capped by the switch port it is cabled
    /// to. `None` for an unknown host or NIC.
    pub fn nic_speed(&self, host: &HostId, port: &str) -> Option<u32> {
        let hw = self.hosts.get(host)?;
        let own = hw.profile.nics.iter().find(|n| n.name == port)?.speed_mbps;
        let own = hw.degraded_nics.get(port).map_or(own, |&d| d.min(own));
        let far = match self.topology.peer(&Node::HostPort(host.clone(), PortName::new(port))) {
            Some(Node::SwitchPort(s, p)) => self.topology.switches.get(s).and_then(|sw| sw.ports.get(p)).map(|(mbps, _)| *mbps),
            _ => None,
        };
        Some(far.map_or(own, |f| f.min(own)))
    }

    /// What `host`'s OS measures now ([`HostHw::usage`]), with every NIC's
    /// (rx, tx) from the latest traffic tick.
    pub fn usage(&self, host: &HostId, rng: &mut impl Rng) -> Option<HostUsage> {
        let hw = self.hosts.get(host)?;
        let mut usage = hw.usage(rng);
        let ports = &self.traffic.last().port_bps;
        usage.port_bps = hw
            .profile
            .nics
            .iter()
            .map(|n| {
                let node = Node::HostPort(host.clone(), PortName::new(&n.name));
                (n.name.clone(), ports.get(&node).copied().unwrap_or((0.0, 0.0)))
            })
            .collect();
        Some(usage)
    }

    /// What `host`'s OS discovers about its hardware: the profile's NICs and
    /// disks, each NIC's link state and negotiated speed, and which disks failed.
    pub fn inventory(&self, host: &HostId) -> Option<Inventory> {
        let hw = self.hosts.get(host)?;
        let nics = hw.profile.nics.iter().map(|n| {
            let port = Node::HostPort(host.clone(), PortName::new(&n.name));
            let speed_mbps = self.nic_speed(host, &n.name).unwrap_or(n.speed_mbps);
            NicState { name: n.name.clone(), speed_mbps, link_up: hw.powered && self.link_up(&port) }
        });
        let disks = hw.profile.disks.iter().map(|d| DiskState {
            name: d.name.clone(),
            kind: d.kind,
            size_gb: d.size_gb,
            failed: hw.failed_disks.contains(&d.name),
        });
        Some(Inventory { profile: hw.profile.id, variant: hw.variant, nics: nics.collect(), disks: disks.collect() })
    }

    /// What `host`'s LLDP daemon hears: for each NIC (in profile order) whose
    /// link is up and whose cable ends in a switch port, that switch port.
    /// Partitions do not matter: LLDP never leaves the cable.
    pub fn lldp(&self, host: &HostId) -> Vec<LldpNeighbor> {
        let Some(hw) = self.hosts.get(host).filter(|hw| hw.powered) else { return vec![] };
        hw.profile
            .nics
            .iter()
            .filter_map(|nic| {
                let local_port = PortName::new(&nic.name);
                let end = Node::HostPort(host.clone(), local_port.clone());
                match self.topology.peer(&end) {
                    Some(Node::SwitchPort(switch, switch_port)) if self.link_up(&end) => {
                        Some(LldpNeighbor { local_port, switch: switch.clone(), switch_port: switch_port.clone() })
                    }
                    _ => None,
                }
            })
            .collect()
    }

    /// A switch's own configuration, which is what its management interface
    /// reports. `None` for an unknown switch.
    pub fn switch_config(&self, id: &SwitchId) -> Option<SwitchConfig> {
        let sw = self.topology.switches.get(id)?;
        let ports = sw
            .ports
            .iter()
            .map(|(port, (_, cfg))| ObservedSwitchPort {
                switch: id.clone(),
                port: port.clone(),
                cfg: cfg.clone(),
                mlag_peer: sw.mlag_peer.clone(),
            })
            .collect();
        Some(SwitchConfig { model: sw.model.clone(), ports, mlag_peer: sw.mlag_peer.clone() })
    }

    /// Whether the port at `end` has carrier: cabled, the cable plugged in,
    /// and a far-end switch that is up with the port admin-up. Partitions do
    /// not matter: they cut traffic, not carrier.
    pub fn link_up(&self, end: &Node) -> bool {
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
    fn nics_negotiate_down_to_the_slower_end_or_a_degraded_speed() {
        let mut w = world();
        let speeds = |w: &World| w.inventory(&hid("h")).unwrap().nics.into_iter().map(|n| n.speed_mbps).collect::<Vec<_>>();
        assert_eq!(speeds(&w), [10_000, 10_000, 2_500, 2_500], "eth0 is a 2.5G NIC on a 10G port");
        w.hosts.get_mut(&hid("h")).unwrap().degraded_nics.insert("sfp0".into(), 1_000);
        assert_eq!(speeds(&w)[0], 1_000);
        w.hosts.get_mut(&hid("h")).unwrap().degraded_nics.clear();
        w.topology.switches.get_mut(&SwitchId::from("sw")).unwrap().ports.get_mut(&PortName::from("p1")).unwrap().0 = 2_500;
        assert_eq!(speeds(&w)[0], 2_500, "a 10G NIC on a 2.5G port");
        assert_eq!(w.nic_speed(&hid("h"), "eth9"), None);
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

    fn lldp_of(w: &World) -> Vec<(String, String, String)> {
        w.lldp(&hid("h")).into_iter().map(|n| (n.local_port.to_string(), n.switch.to_string(), n.switch_port.to_string())).collect()
    }

    #[test]
    fn lldp_lists_the_switch_port_at_the_far_end_of_every_live_cable() {
        let w = world();
        let n = |l: &str, p: &str| (l.to_string(), "sw".to_string(), p.to_string());
        assert_eq!(lldp_of(&w), [n("sfp0", "p1"), n("eth0", "p2")], "in NIC order; uncabled NICs hear nothing");
        assert!(w.lldp(&hid("nope")).is_empty());

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
            assert!(!lldp_of(&w).iter().any(|(l, _, _)| l == "sfp0"), "{what}: sfp0 still has a neighbour");
            set(&mut w, true);
            assert_eq!(lldp_of(&w).len(), 2, "{what}: neighbour did not come back");
        }
        // Re-cabling sfp0 into eth0's old port changes its neighbour and leaves eth0 uncabled.
        let mut w = world();
        w.topology.connect(Node::HostPort(hid("h"), PortName::from("sfp0")), sp("p2"));
        assert_eq!(lldp_of(&w), [n("sfp0", "p2")]);
    }

    #[test]
    fn switch_config_is_the_switchs_own_ground_truth() {
        let mut w = world();
        let sw = SwitchId::from("sw");
        let c = w.switch_config(&sw).unwrap();
        assert_eq!((c.model.as_str(), c.mlag_peer.clone()), ("t", None));
        assert_eq!(c.ports.iter().map(|p| p.port.as_str()).collect::<Vec<_>>(), ["p1", "p2"]);
        assert!(c.ports.iter().all(|p| p.switch == sw && p.cfg.mode == PortMode::Access(1)));
        let changed = SwitchPortCfg { mode: PortMode::Access(20), ..c.ports[0].cfg.clone() };
        w.topology.set_port(&sw, &PortName::from("p1"), changed.clone());
        assert_eq!(w.switch_config(&sw).unwrap().ports[0].cfg, changed);
        assert!(w.switch_config(&SwitchId::from("nope")).is_none());
    }

    #[test]
    fn a_power_cycle_boots_the_persisted_network_config() {
        let mut hw = HostHw::new(ProfileId::Pi5, 0);
        let boot = hw.applied_net.clone();
        let mut trial = boot.clone();
        trial.links[0].mtu = 1400;
        hw.applied_net = trial.clone();
        hw.powered = false;
        hw.power_on();
        assert_eq!((hw.powered, &hw.applied_net), (true, &boot), "an unpersisted change is gone");
        hw.applied_net = trial.clone();
        hw.persist_network();
        hw.powered = false;
        hw.power_on();
        assert_eq!(hw.applied_net, trial);
    }
}
