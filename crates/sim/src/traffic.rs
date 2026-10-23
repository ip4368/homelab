//! The traffic model (design §8): flows, not packets. Each flow has a source,
//! a destination, a network and a demand in Mbit/s. Every tick the flows are
//! routed over the topology as it is and given max-min fair rates over the
//! capacities of the cables they cross.
//!
//! Routing a flow:
//! 1. Each end puts the flow onto a switch through the link its attachment
//!    for the network rides on. A bond picks a member with carrier: LACP
//!    hashes by flow id over the members that are up, active-backup takes
//!    the first one up. Carrier is physical (cable, switch, port admin
//!    state); a member that is up but cannot carry the frame (VLAN not
//!    allowed, MTU too small, cut by a partition) blackholes the flows
//!    hashed onto it, as a real bond would.
//! 2. Between the two switches, the shortest path over switch-to-switch
//!    cables that carry the VLAN at the network's MTU, ties going to the
//!    lowest switch id.
//! 3. A flow with no route gets rate 0 (a blackhole).
//!
//! Rates are max-min fair (progressive filling): every directed cable
//! traversal is a resource with the capacity of its slower end, and each
//! flow is capped by its demand.

use crate::engine::TICK_MS;
use crate::fabric::{Fabric, LinkHop};
use crate::topology::Node;
use crate::world::World;
use control::runtime::SimTime;
use model::{BondMode, HostId, LinkKind, LogicalNetwork, NetworkId, PortName};
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use ts_rs::TS;

/// Where a flow starts or ends.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum FlowEnd {
    Host(HostId),
    /// The upstream router: VM client traffic and transcode streams end here.
    Router,
}

/// What generates a flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowKind {
    Iperf,
    ReplicaWrite,
    Rebuild,
    Migration,
    VmClient,
    Transcode,
    Pod,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Flow {
    pub id: u64,
    pub src: FlowEnd,
    pub dst: FlowEnd,
    pub network: NetworkId,
    pub demand_mbps: f64,
    pub kind: FlowKind,
    /// When the flow stops by itself; `None` runs until stopped.
    pub until: Option<SimTime>,
}

/// What one tick decided.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TickResult {
    /// Mbit/s per flow; 0 for a flow with no route.
    pub rate: BTreeMap<u64, f64>,
    /// Per port that carried traffic: (rx, tx) in bits per second.
    pub port_bps: BTreeMap<Node, (f64, f64)>,
    /// The cables each routed flow crosses, in travel order.
    pub paths: BTreeMap<u64, Vec<LinkHop>>,
}

/// The flows in the world, registered on behalf of whatever generates them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Traffic {
    flows: BTreeMap<u64, Flow>,
    next_id: u64,
    /// Bytes each flow has carried so far.
    carried: BTreeMap<u64, f64>,
    last: TickResult,
}

impl Traffic {
    /// Registers a flow and returns its id (ids are never reused).
    pub fn start(
        &mut self,
        src: FlowEnd,
        dst: FlowEnd,
        network: NetworkId,
        demand_mbps: f64,
        kind: FlowKind,
        until: Option<SimTime>,
    ) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        self.flows.insert(id, Flow { id, src, dst, network, demand_mbps, kind, until });
        self.carried.insert(id, 0.0);
        id
    }

    pub fn stop(&mut self, id: u64) -> Option<Flow> {
        self.carried.remove(&id);
        self.flows.remove(&id)
    }

    pub fn flows(&self) -> impl Iterator<Item = &Flow> {
        self.flows.values()
    }

    pub fn flow(&self, id: u64) -> Option<&Flow> {
        self.flows.get(&id)
    }

    /// Bytes flow `id` has carried since it started (0 for an unknown flow).
    pub fn carried_bytes(&self, id: u64) -> f64 {
        self.carried.get(&id).copied().unwrap_or(0.0)
    }

    /// The latest tick's result.
    pub fn last(&self) -> &TickResult {
        &self.last
    }

    /// Stops every flow whose time is up at `now`.
    pub fn expire(&mut self, now: SimTime) {
        let done: Vec<u64> = self.flows.values().filter(|f| f.until.is_some_and(|u| u <= now)).map(|f| f.id).collect();
        for id in done {
            self.stop(id);
        }
    }

    /// Routes every flow over `fabric` as it is now, shares the capacity out
    /// and accounts one tick's worth of bytes to each flow.
    pub fn tick(&mut self, fabric: &Fabric) -> &TickResult {
        let world = fabric.world();
        let mut result = TickResult::default();
        let mut routed: Vec<(&Flow, Vec<LinkHop>)> = vec![];
        for f in self.flows.values() {
            match route(world, fabric, f) {
                Some(path) => routed.push((f, path)),
                None => {
                    result.rate.insert(f.id, 0.0);
                }
            }
        }
        // Every directed cable traversal is a resource with the capacity of its slower end.
        let mut index: HashMap<(&Node, &Node), usize> = HashMap::new();
        let mut capacity: Vec<f64> = vec![];
        let uses: Vec<Vec<usize>> = routed
            .iter()
            .map(|(_, path)| {
                path.iter()
                    .map(|hop| {
                        *index.entry((&hop.from, &hop.to)).or_insert_with(|| {
                            capacity.push(port_mbps(world, &hop.from).min(port_mbps(world, &hop.to)));
                            capacity.len() - 1
                        })
                    })
                    .collect()
            })
            .collect();
        let demand: Vec<f64> = routed.iter().map(|(f, _)| f.demand_mbps.max(0.0)).collect();
        let rates = max_min(&demand, &uses, &capacity);
        let tick_s = TICK_MS as f64 / 1000.0;
        for ((f, path), rate) in routed.into_iter().zip(rates) {
            result.rate.insert(f.id, rate);
            *self.carried.entry(f.id).or_default() += rate * 1e6 / 8.0 * tick_s;
            if rate > 0.0 {
                let bps = rate * 1e6;
                for hop in &path {
                    result.port_bps.entry(hop.from.clone()).or_default().1 += bps;
                    result.port_bps.entry(hop.to.clone()).or_default().0 += bps;
                }
            }
            result.paths.insert(f.id, path);
        }
        self.last = result;
        &self.last
    }
}

/// The cables flow `f` crosses now, or `None` if it has no route. A flow
/// between two services on one machine never touches the wire.
fn route(world: &World, fabric: &Fabric, f: &Flow) -> Option<Vec<LinkHop>> {
    if f.src == f.dst {
        return Some(vec![]);
    }
    let net = world.networks.get(&f.network)?;
    let first = onto_switch(world, fabric, &f.src, f.id, net)?;
    let last = onto_switch(world, fabric, &f.dst, f.id, net)?;
    let (Node::SwitchPort(a, _), Node::SwitchPort(b, _)) = (&first.to, &last.to) else { return None };
    let middle = fabric.switch_path(a, b, net.vlan, net.mtu)?;
    let mut path = Vec::with_capacity(middle.len() + 2);
    path.push(first);
    path.extend(middle);
    path.push(LinkHop { from: last.to, to: last.from });
    Some(path)
}

/// The cable `end` puts flow `id`'s frames of network `net` onto a switch
/// by: the router's own, or a host's chosen member of the link its
/// attachment for `net` rides on.
fn onto_switch(world: &World, fabric: &Fabric, end: &FlowEnd, id: u64, net: &LogicalNetwork) -> Option<LinkHop> {
    match end {
        // The router speaks tagged or untagged, whatever its port accepts.
        FlowEnd::Router => fabric
            .ingress(&Node::Router, net.vlan, false, net.mtu)
            .or_else(|| fabric.ingress(&Node::Router, net.vlan, true, net.mtu)),
        FlowEnd::Host(host) => {
            let hw = world.hosts.get(host).filter(|hw| hw.powered)?;
            let cfg = &hw.applied_net;
            let att = cfg.attachments.iter().find(|a| a.network == net.id)?;
            let link = cfg.link(&att.link).filter(|l| l.mtu >= net.mtu)?;
            let port = member(world, host, &link.kind, id)?;
            fabric.ingress(&Node::HostPort(host.clone(), port.clone()), net.vlan, att.tagged, net.mtu)
        }
    }
}

/// The port of a link flow `id` leaves by: a single link's port; for a
/// bond, of the members with carrier, the first (active-backup) or the
/// one the flow id hashes to (LACP).
fn member<'a>(world: &World, host: &HostId, kind: &'a LinkKind, id: u64) -> Option<&'a PortName> {
    let (members, mode) = match kind {
        LinkKind::Single(p) => return Some(p),
        LinkKind::Bond { members, mode } => (members, mode),
    };
    let mut up = members.iter().filter(|p| world.link_up(&Node::HostPort(host.clone(), (*p).clone())));
    match mode {
        BondMode::ActiveBackup => up.next(),
        BondMode::Lacp => {
            let up: Vec<&PortName> = up.collect();
            let n = u64::try_from(up.len()).ok().filter(|&n| n > 0)?;
            Some(up[usize::try_from(id % n).expect("an index below the member count")])
        }
    }
}

/// How fast a cable end can send or receive, in Mbit/s. The client and the
/// router are never the bottleneck.
fn port_mbps(world: &World, end: &Node) -> f64 {
    match end {
        Node::HostPort(h, p) => world.nic_speed(h, p.as_str()).map_or(0.0, f64::from),
        Node::SwitchPort(s, p) => world.topology.switches.get(s).and_then(|sw| sw.ports.get(p)).map_or(0.0, |(mbps, _)| f64::from(*mbps)),
        Node::Client | Node::Router => f64::INFINITY,
    }
}

/// A water level, ordered totally so it can key a heap.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Level(f64);

impl Eq for Level {}
impl PartialOrd for Level {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Level {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// What reaches its limit at a level: a flow its demand, or a resource
/// (as of its `version`) its capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Limit {
    Flow(usize),
    Resource(usize, u64),
}

/// Max-min fair rates by progressive filling: every unfrozen flow's rate
/// rises together until a flow reaches its demand or a resource fills up,
/// which freezes that flow or every flow on that resource. `uses[f]` lists
/// the resources flow `f` crosses. Levels come off a heap, so this costs
/// O((flows × path length + resources) × log) rather than a rescan per step.
fn max_min(demand: &[f64], uses: &[Vec<usize>], capacity: &[f64]) -> Vec<f64> {
    let mut users: Vec<Vec<usize>> = vec![vec![]; capacity.len()];
    for (f, rs) in uses.iter().enumerate() {
        for &r in rs {
            users[r].push(f);
        }
    }
    // Capacity not yet given to frozen flows, and how many unfrozen flows share it.
    let mut left = capacity.to_vec();
    let mut sharing: Vec<usize> = users.iter().map(Vec::len).collect();
    let mut version = vec![0u64; capacity.len()];
    let mut rate: Vec<Option<f64>> = vec![None; demand.len()];
    let mut heap: BinaryHeap<Reverse<(Level, Limit)>> = BinaryHeap::new();
    for (f, &d) in demand.iter().enumerate() {
        heap.push(Reverse((Level(d), Limit::Flow(f))));
    }
    for r in 0..capacity.len() {
        if sharing[r] > 0 {
            heap.push(Reverse((Level(left[r] / sharing[r] as f64), Limit::Resource(r, 0))));
        }
    }
    let mut level = 0.0_f64;
    while let Some(Reverse((Level(at), limit))) = heap.pop() {
        let freeze: Vec<usize> = match limit {
            Limit::Flow(f) if rate[f].is_none() => vec![f],
            Limit::Resource(r, v) if v == version[r] && sharing[r] > 0 => {
                users[r].iter().copied().filter(|&f| rate[f].is_none()).collect()
            }
            _ => continue,
        };
        level = level.max(at);
        for f in freeze {
            let x = level.min(demand[f]);
            rate[f] = Some(x);
            for &r in &uses[f] {
                left[r] -= x;
                sharing[r] -= 1;
                version[r] += 1;
                if sharing[r] > 0 {
                    let next = (left[r] / sharing[r] as f64).max(level);
                    heap.push(Reverse((Level(next), Limit::Resource(r, version[r]))));
                }
            }
        }
    }
    rate.into_iter().map(|r| r.unwrap_or(0.0)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{PortMode, Switch, SwitchPortCfg};
    use crate::world::HostHw;
    use model::network::{Attachment, HostNetworkConfig, Link};
    use model::{ProfileId, SwitchId};

    const TEN_G: f64 = 10_000.0;

    fn h(id: &str) -> HostId {
        HostId::from(id)
    }
    fn pn(p: &str) -> PortName {
        PortName::from(p)
    }
    fn hp(host: &str, port: &str) -> Node {
        Node::HostPort(h(host), pn(port))
    }
    fn sp(switch: &str, port: &str) -> Node {
        Node::SwitchPort(SwitchId::from(switch), pn(port))
    }
    fn storage() -> NetworkId {
        NetworkId::from("storage")
    }
    fn trunk(allowed: &[u16], mtu: u16) -> SwitchPortCfg {
        SwitchPortCfg {
            mode: PortMode::Trunk { native: Some(1), allowed: allowed.iter().copied().collect() },
            mtu,
            lacp_group: None,
            admin_up: true,
        }
    }

    /// sw-a and sw-b (ten 10G ports each, host ports trunks carrying VLANs 20
    /// and 100) joined by a 10G trunk on p10 of `uplink_mtu`. The router hangs
    /// off sw-a p9.
    fn fabric_world(uplink_mtu: u16) -> World {
        let mut w = World::new();
        for s in ["sw-a", "sw-b"] {
            let mut ports: BTreeMap<PortName, (u32, SwitchPortCfg)> =
                (1..=9).map(|i| (PortName::new(format!("p{i}")), (10_000, trunk(&[20, 100], 9216)))).collect();
            ports.insert(pn("p10"), (10_000, trunk(&[1, 20, 100], uplink_mtu)));
            w.topology.add_switch(Switch { id: SwitchId::from(s), model: "t".into(), ports, mlag_peer: None, up: true });
        }
        w.topology.connect(sp("sw-a", "p10"), sp("sw-b", "p10"));
        w.topology.connect(Node::Router, sp("sw-a", "p9"));
        w
    }

    /// An MS-01 whose link `kind` (MTU 9000) carries storage and vm-lan
    /// tagged, its NICs cabled as given: (nic, switch, port).
    fn add_host(w: &mut World, id: &str, kind: LinkKind, cables: &[(&str, &str, &str)]) {
        let net = HostNetworkConfig {
            links: vec![Link { name: "data".into(), kind, mtu: 9000 }],
            attachments: ["storage", "vm-lan"]
                .map(|n| Attachment { network: NetworkId::from(n), link: "data".into(), tagged: true, ip: None })
                .into(),
        };
        w.hosts.insert(h(id), HostHw::running(ProfileId::Ms01, 0, net));
        for (nic, switch, port) in cables {
            w.topology.connect(hp(id, nic), sp(switch, port));
        }
    }

    fn single(p: &str) -> LinkKind {
        LinkKind::Single(pn(p))
    }

    fn bond(mode: BondMode) -> LinkKind {
        LinkKind::Bond { members: vec![pn("sfp0"), pn("sfp1")], mode }
    }

    /// h1 and h2, each with one 10G port on sw-a.
    fn two_singles() -> World {
        let mut w = fabric_world(9216);
        add_host(&mut w, "h1", single("sfp0"), &[("sfp0", "sw-a", "p1")]);
        add_host(&mut w, "h2", single("sfp0"), &[("sfp0", "sw-a", "p2")]);
        w
    }

    fn host_flow(t: &mut Traffic, from: &str, to: &str, mbps: f64) -> u64 {
        t.start(FlowEnd::Host(h(from)), FlowEnd::Host(h(to)), storage(), mbps, FlowKind::Iperf, None)
    }

    #[test]
    fn fair_share_two_flows_one_link() {
        let f = Fabric::new(two_singles(), 1);
        let mut t = Traffic::default();
        let ids = [host_flow(&mut t, "h1", "h2", TEN_G), host_flow(&mut t, "h1", "h2", TEN_G)];
        let r = t.tick(&f).clone();
        assert_eq!(ids.map(|id| r.rate[&id]), [5000.0, 5000.0]);
        assert_eq!(r.paths[&ids[0]], vec![
            LinkHop { from: hp("h1", "sfp0"), to: sp("sw-a", "p1") },
            LinkHop { from: sp("sw-a", "p2"), to: hp("h2", "sfp0") },
        ]);
        // Every port the traffic crosses counts it, in bits per second.
        assert_eq!(r.port_bps[&hp("h1", "sfp0")], (0.0, 10e9));
        assert_eq!(r.port_bps[&sp("sw-a", "p1")], (10e9, 0.0));
        assert_eq!(r.port_bps[&hp("h2", "sfp0")], (10e9, 0.0));
        assert!(!r.port_bps.contains_key(&sp("sw-a", "p10")), "only ports with traffic are listed");
    }

    #[test]
    fn small_demands_leave_capacity_to_the_rest() {
        let f = Fabric::new(two_singles(), 1);
        let mut t = Traffic::default();
        let small = host_flow(&mut t, "h1", "h2", 1000.0);
        let big = host_flow(&mut t, "h1", "h2", TEN_G);
        let r = t.tick(&f);
        assert_eq!((r.rate[&small], r.rate[&big]), (1000.0, 9000.0));
    }

    #[test]
    fn max_min_fills_bottlenecks_first() {
        // Links A (10) and B (4); f0 crosses both, f1 only A, f2 only B, f3 nothing.
        let rates = max_min(&[100.0, 100.0, 100.0, 7.0], &[vec![0, 1], vec![0], vec![1], vec![]], &[10.0, 4.0]);
        assert_eq!(rates, [2.0, 8.0, 2.0, 7.0]);
    }

    #[test]
    fn lacp_spreads_flows() {
        let mut w = fabric_world(9216);
        add_host(&mut w, "h1", bond(BondMode::Lacp), &[("sfp0", "sw-a", "p1"), ("sfp1", "sw-a", "p2")]);
        add_host(&mut w, "h2", bond(BondMode::Lacp), &[("sfp0", "sw-a", "p3"), ("sfp1", "sw-a", "p4")]);
        let f = Fabric::new(w, 1);
        let mut t = Traffic::default();
        let ids: Vec<u64> = (0..4).map(|_| host_flow(&mut t, "h1", "h2", TEN_G)).collect();
        let r = t.tick(&f);
        let used: std::collections::BTreeSet<Node> = ids.iter().map(|id| r.paths[id][0].from.clone()).collect();
        assert_eq!(used, [hp("h1", "sfp0"), hp("h1", "sfp1")].into());
        assert!(ids.iter().all(|id| r.rate[id] == 5000.0), "two flows per 10G member: {:?}", r.rate);
        assert_eq!(r.port_bps[&hp("h1", "sfp0")].1 + r.port_bps[&hp("h1", "sfp1")].1, 20e9);

        // One member loses carrier: every flow hashes onto the other.
        let mut f = f;
        f.world_mut().topology.set_cable_up(&hp("h1", "sfp0"), false);
        let r = t.tick(&f);
        assert!(ids.iter().all(|id| r.paths[id][0].from == hp("h1", "sfp1")), "{:?}", r.paths);
        assert_eq!(ids.iter().map(|id| r.rate[id]).sum::<f64>(), TEN_G);
    }

    #[test]
    fn active_backup_uses_the_first_member_up() {
        let mut w = fabric_world(9216);
        add_host(&mut w, "h1", bond(BondMode::ActiveBackup), &[("sfp0", "sw-a", "p1"), ("sfp1", "sw-b", "p1")]);
        add_host(&mut w, "h2", single("sfp0"), &[("sfp0", "sw-b", "p2")]);
        let mut f = Fabric::new(w, 1);
        let mut t = Traffic::default();
        let ids: Vec<u64> = (0..2).map(|_| host_flow(&mut t, "h1", "h2", TEN_G)).collect();
        let r = t.tick(&f).clone();
        assert!(ids.iter().all(|id| r.paths[id][0].from == hp("h1", "sfp0")));
        assert_eq!(r.paths[&ids[0]].len(), 3, "sw-a to sw-b over the uplink");
        f.world_mut().topology.set_switch_up(&SwitchId::from("sw-a"), false);
        let r = t.tick(&f);
        assert!(ids.iter().all(|id| r.paths[id] == [
            LinkHop { from: hp("h1", "sfp1"), to: sp("sw-b", "p1") },
            LinkHop { from: sp("sw-b", "p2"), to: hp("h2", "sfp0") },
        ]));
        assert_eq!(ids.iter().map(|id| r.rate[id]).sum::<f64>(), TEN_G);
    }

    #[test]
    fn blackhole_on_mtu() {
        let jumbo = |uplink_mtu| {
            let mut w = fabric_world(uplink_mtu);
            add_host(&mut w, "h1", single("sfp0"), &[("sfp0", "sw-a", "p1")]);
            add_host(&mut w, "h2", single("sfp0"), &[("sfp0", "sw-b", "p1")]);
            Fabric::new(w, 1)
        };
        let mut t = Traffic::default();
        let id = host_flow(&mut t, "h1", "h2", TEN_G);
        assert_eq!(t.tick(&jumbo(9216)).rate[&id], TEN_G, "control: a jumbo path end to end");
        let r = t.tick(&jumbo(1500));
        assert_eq!(r.rate[&id], 0.0, "storage frames are 9000 bytes; the trunk takes 1500");
        assert!(!r.paths.contains_key(&id) && r.port_bps.is_empty());
    }

    #[test]
    fn a_vlan_missing_on_the_member_blackholes_its_flows() {
        let mut w = two_singles();
        w.topology.set_port(&SwitchId::from("sw-a"), &pn("p1"), trunk(&[100], 9216));
        let f = Fabric::new(w, 1);
        let mut t = Traffic::default();
        let id = host_flow(&mut t, "h1", "h2", TEN_G);
        assert_eq!(t.tick(&f).rate[&id], 0.0);
    }

    #[test]
    fn flows_reach_the_router_and_respect_negotiated_speed() {
        let mut w = two_singles();
        w.hosts.get_mut(&h("h1")).unwrap().degraded_nics.insert("sfp0".into(), 1000);
        let f = Fabric::new(w, 1);
        let mut t = Traffic::default();
        let id = t.start(FlowEnd::Host(h("h1")), FlowEnd::Router, NetworkId::from("vm-lan"), TEN_G, FlowKind::VmClient, None);
        let r = t.tick(&f);
        assert_eq!(r.rate[&id], 1000.0, "the NIC negotiated 1 Gbit/s");
        assert_eq!(r.paths[&id].last(), Some(&LinkHop { from: sp("sw-a", "p9"), to: Node::Router }));
        assert_eq!(r.port_bps[&Node::Router], (1e9, 0.0));
    }

    #[test]
    fn partitions_and_power_stop_flows() {
        let mut f = Fabric::new(two_singles(), 1);
        let mut t = Traffic::default();
        let id = host_flow(&mut t, "h1", "h2", TEN_G);
        f.world_mut().topology.partition([crate::topology::PartitionMember::Host(h("h2"))].into());
        assert_eq!(t.tick(&f).rate[&id], 0.0);
        f.world_mut().topology.heal_partitions();
        f.host_mut(&h("h2")).unwrap().powered = false;
        assert_eq!(t.tick(&f).rate[&id], 0.0);
    }

    #[test]
    fn usage_reports_every_nic_from_the_latest_tick() {
        use rand::SeedableRng;
        let f = Fabric::new(two_singles(), 1);
        let mut t = Traffic::default();
        host_flow(&mut t, "h1", "h2", 2500.0);
        t.tick(&f);
        let mut w = f.world().clone();
        w.traffic = t;
        let u = w.usage(&h("h1"), &mut rand_chacha::ChaCha8Rng::seed_from_u64(1)).unwrap();
        let zero = (0.0, 0.0);
        assert_eq!(u.port_bps, [("sfp0".into(), (0.0, 2.5e9)), ("sfp1".into(), zero), ("eth0".into(), zero), ("eth1".into(), zero)].into());
        assert!(w.usage(&h("nope"), &mut rand_chacha::ChaCha8Rng::seed_from_u64(1)).is_none());
    }

    #[test]
    fn ticks_count_carried_bytes_and_flows_expire() {
        let f = Fabric::new(two_singles(), 1);
        let mut t = Traffic::default();
        let id = t.start(FlowEnd::Host(h("h1")), FlowEnd::Host(h("h2")), storage(), 800.0, FlowKind::Rebuild, Some(5000));
        let other = host_flow(&mut t, "h1", "h2", 800.0);
        t.tick(&f);
        t.tick(&f);
        // 800 Mbit/s for two 1 s ticks is 200 MB.
        assert_eq!(t.carried_bytes(id), 200e6);
        t.expire(4999);
        assert!(t.flow(id).is_some());
        t.expire(5000);
        assert!(t.flow(id).is_none() && t.carried_bytes(id) == 0.0);
        assert_eq!(t.flows().map(|f| f.id).collect::<Vec<_>>(), [other]);
        assert!(other > id, "ids are not reused");
    }
}
