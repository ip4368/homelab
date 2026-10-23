//! Simulator ground truth: topology, logical networks and host hardware,
//! plus what each host's OS sees of it (the engine exposes this to the
//! actors on a host as their `HostOs`), including the workload instances it
//! runs and the volume replicas on its disks.

use crate::topology::{Node, Topology};
use crate::traffic::{FlowEnd, FlowKind, Traffic};
use control::proto::{InstanceSpec, LogCmd, ReplicaReport, UsageProfile};
use control::raft::Durable;
use control::runtime::{HostUsage, SimTime, SwitchConfig};
use model::network::{
    HostNetworkConfig, LldpNeighbor, LldpPeer, LogicalNetwork, ObservedSwitchPort, bootstrap_config, seed_networks,
};
use model::{
    DiskKind, DiskState, HardwareProfile, HostId, Inventory, NetworkId, NicState, PortName, ProfileId, SwitchId, profile,
};
use std::collections::{BTreeMap, BTreeSet};

/// CPU and RAM every powered host uses on top of its instances, in percent.
pub const BASELINE_PCT: f32 = 3.0;
/// How full a disk is before any replica lands on it (OS, logs).
const IDLE_DISK_PCT: f32 = 5.0;
/// A replica is copied from its source at up to this rate, on `storage`.
pub const REBUILD_MBPS: f64 = 2000.0;
/// Usage and demand curves move between seeded knots this far apart.
const CURVE_PERIOD_MS: u64 = 30_000;
/// A usage burst lasts this long and adds this much.
const BURST_MS: u64 = 20_000;
const BURST_PCT: f32 = 40.0;

/// A workload instance running on a host.
#[derive(Debug, Clone, PartialEq)]
pub struct Instance {
    pub spec: InstanceSpec,
    /// Seeds its usage and demand curves.
    pub seed: u64,
    /// The flow started for each of `spec.flows`, in order.
    pub flows: Vec<u64>,
}

/// A volume replica on one of a host's disks.
#[derive(Debug, Clone, PartialEq)]
pub struct Replica {
    pub disk: String,
    pub size_gb: u32,
    /// How much of it is copied, 0 to 100.
    pub pct: u8,
    /// The rebuild flow copying it from its source, while it copies.
    pub flow: Option<u64>,
}

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
    /// What runs on the host, by instance id; gone when it loses power.
    pub instances: BTreeMap<String, Instance>,
    /// Volume replicas on its disks, by volume; they survive power loss.
    pub replicas: BTreeMap<String, Replica>,
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
            instances: BTreeMap::new(),
            replicas: BTreeMap::new(),
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

    /// The disk a new `size_gb` replica goes on: of the disks that are not
    /// SD or eMMC, not failed and have room for it, the one with most room
    /// (then the lowest name).
    fn disk_for(&self, size_gb: u32) -> Option<String> {
        self.profile
            .disks
            .iter()
            .filter(|d| !matches!(d.kind, DiskKind::Sd | DiskKind::Emmc) && !self.failed_disks.contains(&d.name))
            .map(|d| (d.size_gb.saturating_sub(self.replica_gb(&d.name)), &d.name))
            .filter(|(free, _)| *free >= size_gb)
            .min_by_key(|(free, name)| (std::cmp::Reverse(*free), *name))
            .map(|(_, name)| name.clone())
    }

    /// The space the replicas on `disk` take.
    fn replica_gb(&self, disk: &str) -> u32 {
        self.replicas.values().filter(|r| r.disk == disk).map(|r| r.size_gb).sum()
    }

    /// What the OS measures now: the baseline plus each instance's usage
    /// curve as a share of the machine, and every healthy disk filled by the
    /// copied part of its replicas. Port rates are the world's to fill ([`World::usage`]).
    pub fn usage(&self, now: SimTime) -> HostUsage {
        let cpu_m = f64::from(self.profile.cpu_threads) * 1000.0;
        let ram_mb = f64::from(self.profile.ram_gb) * 1024.0;
        let (mut cpu_pct, mut ram_pct) = (f64::from(BASELINE_PCT), f64::from(BASELINE_PCT));
        for i in self.instances.values() {
            let (cpu, ram) = (usage_pct(&i.spec.usage, i.seed, now), usage_pct(&i.spec.usage, !i.seed, now));
            cpu_pct += f64::from(i.spec.cpu_m) * f64::from(cpu) / cpu_m;
            ram_pct += f64::from(i.spec.ram_mb) * f64::from(ram) / ram_mb;
        }
        let disk_pct = self
            .profile
            .disks
            .iter()
            .filter(|d| !self.failed_disks.contains(&d.name))
            .map(|d| {
                let copied: f64 = self.replicas.values().filter(|r| r.disk == d.name).map(|r| f64::from(r.size_gb) * f64::from(r.pct) / 100.0).sum();
                (d.name.clone(), IDLE_DISK_PCT + (100.0 * copied / f64::from(d.size_gb.max(1))) as f32)
            })
            .collect();
        HostUsage {
            cpu_pct: cpu_pct.min(100.0) as f32,
            ram_pct: ram_pct.min(100.0) as f32,
            disk_pct,
            port_bps: BTreeMap::new(),
        }
    }
}

/// Where an instance's usage is at `now`, as a percentage of what it asked
/// for: its base, smooth noise up to `noise_pct`, and a burst of
/// [`BURST_PCT`] for [`BURST_MS`] at a seeded moment of every `burst_every_s`.
fn usage_pct(p: &UsageProfile, seed: u64, now: SimTime) -> f32 {
    let mut pct = p.base_pct + p.noise_pct * curve(seed, now) as f32;
    if p.burst_every_s > 0 {
        let period = u64::from(p.burst_every_s) * 1000;
        let start = mix(seed ^ 0xB0B5, now / period) % period.saturating_sub(BURST_MS).max(1);
        if (start..start + BURST_MS).contains(&(now % period)) {
            pct += BURST_PCT;
        }
    }
    pct.clamp(0.0, 100.0)
}

/// A smooth deterministic curve in [0, 1): seeded values every
/// [`CURVE_PERIOD_MS`], joined by straight lines.
fn curve(seed: u64, now: SimTime) -> f64 {
    let (knot, into) = (now / CURVE_PERIOD_MS, now % CURVE_PERIOD_MS);
    let unit = |k: u64| (mix(seed, k) >> 11) as f64 / (1u64 << 53) as f64;
    let (a, b) = (unit(knot), unit(knot + 1));
    a + (b - a) * into as f64 / CURVE_PERIOD_MS as f64
}

/// SplitMix64 of `seed` and `k`: a well-spread hash.
fn mix(seed: u64, k: u64) -> u64 {
    let mut z = seed ^ k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
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

    /// What `host`'s OS measures at `now` ([`HostHw::usage`]), with every
    /// NIC's (rx, tx) from the latest traffic tick.
    pub fn usage(&self, host: &HostId, now: SimTime) -> Option<HostUsage> {
        let hw = self.hosts.get(host)?;
        let mut usage = hw.usage(now);
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

    /// Runs `spec` on `host` (if it is powered) with curves seeded by `seed`,
    /// putting its flows on the wire. The same spec again changes nothing; a
    /// different one replaces the instance.
    pub fn start_instance(&mut self, host: &HostId, spec: InstanceSpec, seed: u64) {
        let Some(hw) = self.hosts.get_mut(host).filter(|hw| hw.powered) else { return };
        if hw.instances.get(&spec.id).is_some_and(|i| i.spec == spec) {
            return;
        }
        for flow in hw.instances.remove(&spec.id).into_iter().flat_map(|i| i.flows) {
            self.traffic.stop(flow);
        }
        let src = FlowEnd::Host(host.clone());
        // Demands follow their curves from the next traffic tick on.
        let flows = spec.flows.iter().map(|f| self.traffic.start(src.clone(), f.to.clone(), f.network.clone(), f.min_mbps, f.kind, None)).collect();
        hw.instances.insert(spec.id.clone(), Instance { spec, seed, flows });
    }

    /// Stops instance `id` on `host`, taking its flows off the wire.
    pub fn stop_instance(&mut self, host: &HostId, id: &str) {
        let Some(i) = self.hosts.get_mut(host).and_then(|hw| hw.instances.remove(id)) else { return };
        for flow in i.flows {
            self.traffic.stop(flow);
        }
    }

    /// The instances running on `host`.
    pub fn instances(&self, host: &HostId) -> Vec<String> {
        self.hosts.get(host).map(|hw| hw.instances.keys().cloned().collect()).unwrap_or_default()
    }

    /// Puts a replica of `volume` on the disk of powered `host` with most room
    /// for it: empty and complete when `source` is `None`, else copied from
    /// `source` by a [`REBUILD_MBPS`] rebuild flow on `storage`. A volume the
    /// host holds keeps its replica. The replica's disk, or `None` if no disk
    /// has room.
    pub fn create_replica(&mut self, host: &HostId, volume: &str, size_gb: u32, source: Option<HostId>) -> Option<String> {
        let hw = self.hosts.get_mut(host).filter(|hw| hw.powered)?;
        if let Some(r) = hw.replicas.get(volume) {
            return Some(r.disk.clone());
        }
        let disk = hw.disk_for(size_gb)?;
        let flow = source.filter(|s| s != host).map(|s| {
            let storage = NetworkId::from("storage");
            self.traffic.start(FlowEnd::Host(s), FlowEnd::Host(host.clone()), storage, REBUILD_MBPS, FlowKind::Rebuild, None)
        });
        let pct = if flow.is_some() { 0 } else { 100 };
        hw.replicas.insert(volume.into(), Replica { disk: disk.clone(), size_gb, pct, flow });
        Some(disk)
    }

    /// Drops `host`'s replica of `volume`, ending its copy if one runs.
    pub fn delete_replica(&mut self, host: &HostId, volume: &str) {
        let Some(r) = self.hosts.get_mut(host).and_then(|hw| hw.replicas.remove(volume)) else { return };
        if let Some(flow) = r.flow {
            self.traffic.stop(flow);
        }
    }

    /// The replicas on `host`: (volume, disk, pct copied).
    pub fn replicas(&self, host: &HostId) -> Vec<ReplicaReport> {
        let Some(hw) = self.hosts.get(host) else { return vec![] };
        hw.replicas.iter().map(|(v, r)| (v.clone(), r.disk.clone(), r.pct)).collect()
    }

    /// Moves every instance flow's demand to where its curve is at `now`.
    pub fn update_demands(&mut self, now: SimTime) {
        for i in self.hosts.values().flat_map(|hw| hw.instances.values()) {
            for (n, (&flow, f)) in (1..).zip(i.flows.iter().zip(&i.spec.flows)) {
                let at = curve(mix(i.seed, n), now);
                self.traffic.set_demand(flow, f.min_mbps + (f.max_mbps - f.min_mbps) * at);
            }
        }
    }

    /// Advances each copying replica by the bytes its rebuild flow has
    /// carried (`size_gb` × 8e9 bits is all of it); a complete one ends its flow.
    pub fn advance_copies(&mut self) {
        for r in self.hosts.values_mut().flat_map(|hw| hw.replicas.values_mut()) {
            let Some(flow) = r.flow else { continue };
            let (copied, total) = (self.traffic.carried_bytes(flow), f64::from(r.size_gb) * 1e9);
            if copied >= total {
                r.pct = 100;
                r.flow = None;
                self.traffic.stop(flow);
            } else {
                r.pct = (100.0 * copied / total).floor() as u8;
            }
        }
    }

    /// Cuts `host`'s power: its instances and the copies onto it end (their
    /// flows leave the wire); its disks, replicas included, keep what they hold.
    pub fn power_off(&mut self, host: &HostId) {
        let Some(hw) = self.hosts.get_mut(host) else { return };
        hw.powered = false;
        let flows = std::mem::take(&mut hw.instances).into_values().flat_map(|i| i.flows);
        let copies: Vec<u64> = hw.replicas.values_mut().filter_map(|r| r.flow.take()).collect();
        for flow in flows.chain(copies) {
            self.traffic.stop(flow);
        }
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

    /// A switch's own view of itself, which is what its management interface
    /// reports: port configuration plus what its ports see on the wire (carrier,
    /// negotiated speed, and the LLDP neighbour of switch-to-switch cables).
    /// `None` for an unknown switch.
    pub fn switch_config(&self, id: &SwitchId) -> Option<SwitchConfig> {
        let sw = self.topology.switches.get(id)?;
        let ports = sw
            .ports
            .iter()
            .map(|(port, (own_mbps, cfg))| {
                let end = Node::SwitchPort(id.clone(), port.clone());
                let far = self.topology.peer(&end);
                let speed_mbps = match far {
                    Some(Node::SwitchPort(s, p)) => {
                        self.topology.switches.get(s).and_then(|o| o.ports.get(p)).map_or(*own_mbps, |(f, _)| (*f).min(*own_mbps))
                    }
                    Some(Node::HostPort(h, p)) => self.nic_speed(h, p.as_str()).map_or(*own_mbps, |f| f.min(*own_mbps)),
                    _ => *own_mbps,
                };
                let neighbor = match far {
                    Some(Node::SwitchPort(switch, port)) => Some(LldpPeer { switch: switch.clone(), port: port.clone() }),
                    _ => None,
                };
                ObservedSwitchPort {
                    switch: id.clone(),
                    port: port.clone(),
                    cfg: cfg.clone(),
                    mlag_peer: sw.mlag_peer.clone(),
                    link_up: cfg.admin_up && self.link_up(&end),
                    speed_mbps,
                    neighbor,
                }
            })
            .collect();
        Some(SwitchConfig { model: sw.model.clone(), ports, mlag_peer: sw.mlag_peer.clone() })
    }

    /// Whether the port at `end` has carrier: cabled, the cable plugged in,
    /// and a far end that is alive: a switch that is up with the port admin-up,
    /// or a powered host. Partitions do not matter: they cut traffic, not carrier.
    pub fn link_up(&self, end: &Node) -> bool {
        let Some(peer) = self.topology.peer(end) else { return false };
        if !self.topology.is_cable_up(end, peer) {
            return false;
        }
        match peer {
            Node::SwitchPort(s, p) => {
                self.topology.switches.get(s).is_some_and(|sw| sw.up && sw.port(p).is_some_and(|c| c.admin_up))
            }
            Node::HostPort(h, _) => self.hosts.get(h).is_some_and(|hw| hw.powered),
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
    use control::proto::FlowSpec;
    use model::SwitchId;

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

    fn spec(id: &str, cpu_m: u32, flows: Vec<FlowSpec>) -> InstanceSpec {
        InstanceSpec { id: id.into(), cpu_m, ram_mb: 4096, usage: UsageProfile { base_pct: 50.0, noise_pct: 0.0, burst_every_s: 0 }, flows }
    }

    fn to_router(min_mbps: f64, max_mbps: f64) -> FlowSpec {
        FlowSpec { kind: FlowKind::VmClient, to: FlowEnd::Router, network: NetworkId::from("vm-lan"), min_mbps, max_mbps }
    }

    #[test]
    fn usage_is_a_baseline_plus_the_instances_and_skips_failed_disks() {
        let mut w = world();
        let h = hid("h");
        w.hosts.get_mut(&h).unwrap().failed_disks.insert("nvme1".into());
        let idle = w.usage(&h, 0).unwrap();
        assert_eq!((idle.cpu_pct, idle.ram_pct), (BASELINE_PCT, BASELINE_PCT));
        assert_eq!(idle.disk_pct, [("nvme0".to_string(), IDLE_DISK_PCT)].into());
        // 4000 m at 50% of a 20-thread machine is 10%; 4 GB at 50% of 64 GB is 3.125%.
        w.start_instance(&h, spec("vm/a", 4000, vec![]), 7);
        let busy = w.usage(&h, 0).unwrap();
        assert!((busy.cpu_pct - 13.0).abs() < 1e-4, "{busy:?}");
        assert!((busy.ram_pct - (BASELINE_PCT + 3.125)).abs() < 1e-4, "{busy:?}");
        assert!(w.usage(&hid("nope"), 0).is_none());
    }

    #[test]
    fn usage_curves_are_seeded_smooth_and_burst() {
        let p = UsageProfile { base_pct: 20.0, noise_pct: 30.0, burst_every_s: 120 };
        let samples: Vec<f32> = (0..600).map(|s| usage_pct(&p, 9, s * 1000)).collect();
        assert_eq!(samples, (0..600).map(|s| usage_pct(&p, 9, s * 1000)).collect::<Vec<_>>(), "deterministic");
        assert_ne!(samples, (0..600).map(|s| usage_pct(&p, 10, s * 1000)).collect::<Vec<_>>(), "seeded");
        let calm = UsageProfile { burst_every_s: 0, ..p };
        for s in 0..600 {
            let v = usage_pct(&calm, 9, s * 1000);
            assert!((20.0..50.0).contains(&v), "{v}");
            assert!((v - usage_pct(&calm, 9, s * 1000 + 1000)).abs() <= 1.0, "smooth: at most 30% per 30 s");
        }
        let bursting = (0..600).filter(|&s| samples[s as usize] > usage_pct(&calm, 9, s * 1000)).count();
        assert_eq!(bursting, 5 * 20, "a 20 s burst every 2 minutes");
    }

    #[test]
    fn instances_put_their_flows_on_the_wire_until_stopped() {
        let mut w = world();
        let h = hid("h");
        w.start_instance(&h, spec("vm/a", 1000, vec![to_router(5.0, 50.0), to_router(5.0, 50.0)]), 7);
        assert_eq!(w.instances(&h), ["vm/a"]);
        let flows: Vec<u64> = w.hosts[&h].instances["vm/a"].flows.clone();
        assert_eq!(flows.len(), 2);
        for id in &flows {
            let f = w.traffic.flow(*id).unwrap();
            assert_eq!((&f.src, &f.dst, f.kind), (&FlowEnd::Host(h.clone()), &FlowEnd::Router, FlowKind::VmClient));
        }
        w.start_instance(&h, spec("vm/a", 1000, vec![to_router(5.0, 50.0), to_router(5.0, 50.0)]), 8);
        assert_eq!(w.hosts[&h].instances["vm/a"].flows, flows, "the same spec again changes nothing");
        w.start_instance(&h, spec("vm/a", 1000, vec![to_router(5.0, 50.0)]), 8);
        assert_eq!(w.traffic.flows().count(), 1, "a new spec replaces the instance and its flows");

        // Demands move along their curves within their range.
        let mut demands = BTreeSet::new();
        for s in 0..300 {
            w.update_demands(s * 1000);
            let d = w.traffic.flows().next().unwrap().demand_mbps;
            assert!((5.0..=50.0).contains(&d), "{d}");
            demands.insert(d.to_bits());
        }
        assert!(demands.len() > 100);

        w.stop_instance(&h, "vm/a");
        assert!(w.instances(&h).is_empty() && w.traffic.flows().count() == 0);
        w.hosts.get_mut(&h).unwrap().powered = false;
        w.start_instance(&h, spec("vm/b", 1000, vec![]), 1);
        assert!(w.instances(&h).is_empty(), "a powered-off host runs nothing");
    }

    #[test]
    fn replicas_go_on_the_disk_with_most_room_and_fill_it() {
        let mut w = world();
        let h = hid("h");
        assert_eq!(w.create_replica(&h, "a", 1500, None), Some("nvme0".into()));
        assert_eq!(w.create_replica(&h, "b", 100, None), Some("nvme1".into()), "nvme1 has more room now");
        assert_eq!(w.create_replica(&h, "a", 1500, None), Some("nvme0".into()), "a held volume keeps its replica");
        assert_eq!(w.create_replica(&h, "c", 1950, None), None, "no disk has room");
        assert_eq!(w.replicas(&h), [("a".to_string(), "nvme0".to_string(), 100), ("b".to_string(), "nvme1".to_string(), 100)]);
        let u = w.usage(&h, 0).unwrap();
        assert_eq!(u.disk_pct["nvme0"], IDLE_DISK_PCT + 75.0);
        w.delete_replica(&h, "a");
        assert_eq!(w.replicas(&h).len(), 1);
        let mut pi = World::new();
        pi.hosts.insert(hid("pi"), HostHw::new(ProfileId::Pi5, 0));
        assert_eq!(pi.create_replica(&hid("pi"), "a", 1, None), None, "never on an SD card");
    }

    /// Two MS-01s whose sfp0 (storage tagged, MTU 9000) is cabled to a jumbo trunk on `sw`.
    fn storage_pair() -> World {
        use model::network::{Attachment, Link, LinkKind};
        let mut w = World::new();
        let cfg = SwitchPortCfg { mode: PortMode::Trunk { native: None, allowed: [20].into() }, mtu: 9216, lacp_group: None, admin_up: true };
        let ports = ["p1", "p2"].map(|p| (PortName::from(p), (10_000, cfg.clone()))).into();
        w.topology.add_switch(Switch { id: SwitchId::from("sw"), model: "t".into(), ports, mlag_peer: None, up: true });
        for (h, p) in [("a", "p1"), ("b", "p2")] {
            let net = HostNetworkConfig {
                links: vec![Link { name: "data".into(), kind: LinkKind::Single(PortName::from("sfp0")), mtu: 9000 }],
                attachments: vec![Attachment { network: NetworkId::from("storage"), link: "data".into(), tagged: true, ip: None }],
            };
            w.hosts.insert(hid(h), HostHw::running(ProfileId::Ms01, 0, net));
            w.topology.connect(Node::HostPort(hid(h), PortName::from("sfp0")), sp(p));
        }
        w
    }

    #[test]
    fn a_copy_advances_by_the_bytes_its_rebuild_flow_carries() {
        use crate::engine::Deliver;
        let mut w = storage_pair();
        assert_eq!(w.create_replica(&hid("b"), "v", 1, Some(hid("a"))), Some("nvme0".into()));
        let flow = w.hosts[&hid("b")].replicas["v"].flow.unwrap();
        let f = w.traffic.flow(flow).unwrap().clone();
        assert_eq!((f.src, f.dst, f.network.as_str(), f.demand_mbps, f.kind), (FlowEnd::Host(hid("a")), FlowEnd::Host(hid("b")), "storage", REBUILD_MBPS, FlowKind::Rebuild));
        let mut fabric = crate::fabric::Fabric::new(w, 1);
        // 1 GB at 2000 Mbit/s is 4 s.
        let pct = |f: &crate::fabric::Fabric| f.world().replicas(&hid("b"))[0].2;
        fabric.tick(1000);
        assert_eq!(pct(&fabric), 25);
        fabric.tick(2000);
        fabric.tick(3000);
        assert_eq!(pct(&fabric), 75);
        fabric.tick(4000);
        assert_eq!(pct(&fabric), 100);
        assert!(fabric.world().hosts[&hid("b")].replicas["v"].flow.is_none() && fabric.world().traffic.flow(flow).is_none(), "done: the flow ends");
    }

    #[test]
    fn power_off_ends_instances_and_copies_but_keeps_replicas() {
        let mut w = storage_pair();
        w.start_instance(&hid("b"), spec("vm/a", 1000, vec![to_router(5.0, 50.0)]), 7);
        w.create_replica(&hid("b"), "v", 1, Some(hid("a")));
        w.create_replica(&hid("b"), "u", 1, None);
        w.power_off(&hid("b"));
        let hw = &w.hosts[&hid("b")];
        assert!(!hw.powered && hw.instances.is_empty());
        assert_eq!(w.traffic.flows().count(), 0);
        assert_eq!(w.replicas(&hid("b")), [("u".to_string(), "nvme1".to_string(), 100), ("v".to_string(), "nvme0".to_string(), 0)]);
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
    fn switch_ports_report_carrier_speed_and_switch_neighbours() {
        let mut w = world();
        let sw = SwitchId::from("sw");
        let cfg = SwitchPortCfg { mode: PortMode::Access(1), mtu: 1500, lacp_group: None, admin_up: true };
        let other = SwitchId::from("sw2");
        let ports = [("p1", 1_000)].map(|(p, mbps)| (PortName::from(p), (mbps, cfg.clone()))).into();
        w.topology.add_switch(Switch { id: other.clone(), model: "t".into(), ports, mlag_peer: None, up: true });
        // sw p2 is re-cabled from the host to the slower switch sw2.
        w.topology.connect(sp("p2"), Node::SwitchPort(other.clone(), PortName::from("p1")));
        let report = |w: &World, id: &SwitchId, p: &str| w.switch_config(id).unwrap().ports.into_iter().find(|o| o.port.as_str() == p).unwrap();

        let host_facing = report(&w, &sw, "p1");
        assert_eq!((host_facing.link_up, host_facing.speed_mbps, host_facing.neighbor), (true, 10_000, None));
        let trunk = report(&w, &sw, "p2");
        let peer = LldpPeer { switch: other.clone(), port: PortName::from("p1") };
        assert_eq!((trunk.link_up, trunk.speed_mbps, trunk.neighbor.clone()), (true, 1_000, Some(peer.clone())));

        w.topology.set_switch_up(&other, false);
        let down = report(&w, &sw, "p2");
        assert_eq!((down.link_up, down.neighbor), (false, Some(peer)), "the far switch is dead but the cabling is known");
        w.hosts.get_mut(&hid("h")).unwrap().powered = false;
        assert!(!report(&w, &sw, "p1").link_up, "a powered-off host has no carrier");
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
