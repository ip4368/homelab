//! The simulated L2 fabric: decides whether a frame from one node reaches
//! another on a VLAN at a given MTU, honouring cables, switch and port state,
//! VLAN tagging, MTU and partitions. It owns the [`World`] (ground truth) and
//! is the engine's [`Deliver`] implementation, including per-switch VIP ARP.
//!
//! Reachability is computed over a switch graph (switches joined by
//! switch-to-switch cables that carry the VLAN tagged), cached per
//! (VLAN, MTU) and rebuilt whenever `topology.version` changes or the world
//! is borrowed mutably through [`Fabric::world_mut`]. Host,
//! client and router attachments are evaluated per query, because they also
//! depend on host power and applied network config, which change without
//! touching the topology.
//!
//! Every engine tick the fabric moves the world's traffic model forward
//! ([`crate::traffic`]), which routes flows with the same rules.

use crate::engine::Deliver;
use crate::topology::Node;
use crate::traffic::Traffic;
use crate::world::{HostHw, World};
use control::proto::InstanceSpec;
use control::runtime::{Addr, Endpoint, SimTime};
use model::{HostId, NetworkId, PortName, SwitchId};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

/// Control messages are small; they need the path to carry standard frames.
pub const CONTROL_MTU: u16 = 1500;
/// The network VIP ARP runs on.
const MGMT: &str = "mgmt";
/// An MTU every port satisfies: reachability ignoring frame size.
const ANY_MTU: u16 = 0;

/// One cable traversal, in the direction of travel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkHop {
    pub from: Node,
    pub to: Node,
}

/// Where traffic enters or leaves the fabric.
enum Station<'a> {
    /// A host, optionally restricted to frames sent out of one port.
    Host(&'a HostId, Option<&'a PortName>),
    Client,
    Router,
    Switch(&'a SwitchId),
}

impl<'a> Station<'a> {
    fn of_node(n: &'a Node) -> Self {
        match n {
            Node::HostPort(h, p) => Station::Host(h, Some(p)),
            Node::SwitchPort(s, _) => Station::Switch(s),
            Node::Client => Station::Client,
            Node::Router => Station::Router,
        }
    }

    fn of_addr(a: &'a Addr) -> Option<Self> {
        match a {
            Addr::Host(h) => Some(Station::Host(h, None)),
            Addr::Client => Some(Station::Client),
            Addr::Router => Some(Station::Router),
            Addr::Switch(s) => Some(Station::Switch(s)),
            Addr::Vip => None,
        }
    }
}

/// A station's way onto a switch: the cable it crosses (none when the station
/// is the switch itself) and the switch it lands on.
struct Attach {
    hop: Option<LinkHop>,
    switch: SwitchId,
}

/// Switch-to-switch adjacency: (local port, neighbour, neighbour port).
type SwitchGraph = BTreeMap<SwitchId, Vec<(PortName, SwitchId, PortName)>>;

#[derive(Default)]
struct GraphCache {
    /// The topology version the graphs were built at; `None` when invalidated.
    version: Option<u64>,
    graphs: HashMap<(u16, u16), SwitchGraph>,
}

pub struct Fabric {
    world: World,
    rng: ChaCha8Rng,
    graphs: RefCell<GraphCache>,
    /// Which host each switch believes owns the VIP (last gratuitous ARP seen).
    arp: BTreeMap<SwitchId, HostId>,
}

impl Fabric {
    pub fn new(world: World, seed: u64) -> Self {
        Self {
            world,
            rng: ChaCha8Rng::seed_from_u64(seed),
            graphs: RefCell::new(GraphCache::default()),
            arp: BTreeMap::new(),
        }
    }

    pub fn world(&self) -> &World {
        &self.world
    }

    /// Drops the cached graphs: whatever the caller changes, even topology
    /// fields edited without bumping `version`, is seen by the next query.
    pub fn world_mut(&mut self) -> &mut World {
        *self.graphs.get_mut() = GraphCache::default();
        &mut self.world
    }

    /// One host's hardware. Host state never feeds the cached switch graphs
    /// (attachments are evaluated per query), so this keeps them.
    pub fn host_mut(&mut self, id: &HostId) -> Option<&mut HostHw> {
        self.world.hosts.get_mut(id)
    }

    /// The world's flows. Traffic never feeds the cached switch graphs, so this keeps them.
    pub fn traffic_mut(&mut self) -> &mut Traffic {
        &mut self.world.traffic
    }

    // Workloads live on the hosts and their flows in the traffic, neither of
    // which feeds the cached switch graphs: these keep them.

    /// See [`World::start_instance`].
    pub fn start_instance(&mut self, host: &HostId, spec: InstanceSpec, seed: u64) {
        self.world.start_instance(host, spec, seed);
    }

    /// See [`World::stop_instance`].
    pub fn stop_instance(&mut self, host: &HostId, id: &str) {
        self.world.stop_instance(host, id);
    }

    /// See [`World::create_replica`].
    pub fn create_replica(&mut self, host: &HostId, volume: &str, size_gb: u32, source: Option<HostId>) -> Option<String> {
        self.world.create_replica(host, volume, size_gb, source)
    }

    /// See [`World::delete_replica`].
    pub fn delete_replica(&mut self, host: &HostId, volume: &str) {
        self.world.delete_replica(host, volume);
    }

    /// See [`World::power_off`]. Link state changes, but carrier is
    /// evaluated per query, so the cached switch graphs stay valid.
    pub fn power_off(&mut self, host: &HostId) {
        self.world.power_off(host);
    }

    /// Which host each switch currently believes owns the VIP.
    pub fn vip_arp(&self) -> &BTreeMap<SwitchId, HostId> {
        &self.arp
    }

    /// Can a frame of `mtu` bytes get from `a` to `b` on `vlan`? A host port
    /// node sends out of that port on whichever of its attachments uses `vlan`;
    /// a switch port node stands for its whole switch.
    pub fn reachable(&self, a: &Node, b: &Node, vlan: u16, mtu: u16) -> bool {
        self.find_path(&Station::of_node(a), &Station::of_node(b), vlan, mtu).is_some()
    }

    /// Shortest cable path from `a` to `b` on `vlan`, ignoring MTU.
    pub fn path(&self, a: &Node, b: &Node, vlan: u16) -> Option<Vec<LinkHop>> {
        self.find_path(&Station::of_node(a), &Station::of_node(b), vlan, ANY_MTU)
    }

    /// The cable a `vlan` frame of `mtu` bytes sent out of `port` (a host
    /// port, the client or the router; `tagged` or not) crosses onto a
    /// switch, if that switch takes it: the cable is up and uncut, the
    /// switch is up, and its port is up, big enough and accepts the frame.
    pub fn ingress(&self, port: &Node, vlan: u16, tagged: bool, mtu: u16) -> Option<LinkHop> {
        self.attach_node(port.clone(), vlan, tagged, mtu)?.hop
    }

    /// Shortest path between two switches over switch-to-switch cables that
    /// carry `vlan` tagged at `mtu`, ties going to the lowest switch id.
    /// Empty when `a` is `b`; `None` if either is down or there is no path.
    pub fn switch_path(&self, a: &SwitchId, b: &SwitchId, vlan: u16, mtu: u16) -> Option<Vec<LinkHop>> {
        self.find_path(&Station::Switch(a), &Station::Switch(b), vlan, mtu)
    }

    /// A gratuitous ARP from `host`: every switch it reaches on mgmt learns it.
    pub fn record_vip_claim(&mut self, host: &HostId) {
        let Some(vlan) = self.mgmt_vlan() else { return };
        let starts: Vec<SwitchId> =
            self.attach(&Station::Host(host, None), vlan, ANY_MTU).into_iter().map(|a| a.switch).collect();
        let reached = self.with_graph(vlan, ANY_MTU, |g| {
            let mut seen: BTreeSet<SwitchId> = starts.iter().cloned().collect();
            let mut queue: VecDeque<SwitchId> = starts.into_iter().collect();
            while let Some(s) = queue.pop_front() {
                for (_, next, _) in g.get(&s).into_iter().flatten() {
                    if seen.insert(next.clone()) {
                        queue.push_back(next.clone());
                    }
                }
            }
            seen
        });
        for s in reached {
            self.arp.insert(s, host.clone());
        }
    }

    /// The VIP owner according to the access switch of `from_host`'s mgmt port.
    pub fn resolve_vip(&self, from_host: &HostId) -> Option<HostId> {
        self.vip_seen_by(&Station::Host(from_host, None))
    }

    fn vip_seen_by(&self, station: &Station) -> Option<HostId> {
        let first = self.attach(station, self.mgmt_vlan()?, ANY_MTU).into_iter().next()?;
        self.arp.get(&first.switch).cloned()
    }

    fn mgmt_vlan(&self) -> Option<u16> {
        self.world.networks.get(&NetworkId::from(MGMT)).map(|n| n.vlan)
    }

    /// 0.2 ms per hop plus 0–0.1 ms jitter, rounded up to whole ms, at least 1.
    fn latency_ms(&mut self, hops: usize) -> u64 {
        let micros = 200 * hops as u64 + self.rng.random_range(0..=100);
        micros.div_ceil(1000).max(1)
    }

    /// Multi-source BFS from `a`'s attachments to any of `b`'s, over the
    /// switch graph. Ties go to the first attachment, then the lowest
    /// neighbour switch and port.
    fn find_path(&self, a: &Station, b: &Station, vlan: u16, mtu: u16) -> Option<Vec<LinkHop>> {
        let src = self.attach(a, vlan, mtu);
        let dst = self.attach(b, vlan, mtu);
        if src.is_empty() || dst.is_empty() {
            return None;
        }
        self.with_graph(vlan, mtu, |g| {
            // Per reached switch: how we got there (attachment index, or the cable used).
            let mut came: BTreeMap<SwitchId, Result<usize, LinkHop>> = BTreeMap::new();
            let mut queue = VecDeque::new();
            for (i, at) in src.iter().enumerate() {
                if !came.contains_key(&at.switch) {
                    came.insert(at.switch.clone(), Ok(i));
                    queue.push_back(at.switch.clone());
                }
            }
            while let Some(s) = queue.pop_front() {
                if let Some(exit) = dst.iter().find(|d| d.switch == s) {
                    let mut hops: Vec<LinkHop> = exit.hop.iter().map(reverse).collect();
                    let mut cur = s;
                    loop {
                        match &came[&cur] {
                            Ok(i) => {
                                hops.extend(src[*i].hop.clone());
                                break;
                            }
                            Err(hop) => {
                                hops.push(hop.clone());
                                let Node::SwitchPort(prev, _) = &hop.from else { unreachable!("switch hop") };
                                cur = prev.clone();
                            }
                        }
                    }
                    hops.reverse();
                    return Some(hops);
                }
                for (port, next, next_port) in g.get(&s).into_iter().flatten() {
                    if !came.contains_key(next) {
                        let hop = LinkHop {
                            from: Node::SwitchPort(s.clone(), port.clone()),
                            to: Node::SwitchPort(next.clone(), next_port.clone()),
                        };
                        came.insert(next.clone(), Err(hop));
                        queue.push_back(next.clone());
                    }
                }
            }
            None
        })
    }

    /// The switches `station` can put a `vlan` frame of `mtu` bytes onto.
    fn attach(&self, station: &Station, vlan: u16, mtu: u16) -> Vec<Attach> {
        let w = &self.world;
        match station {
            Station::Host(id, via) => {
                let Some(hw) = w.hosts.get(*id).filter(|hw| hw.powered) else { return vec![] };
                let cfg = &hw.applied_net;
                let mut out = vec![];
                for att in &cfg.attachments {
                    if w.networks.get(&att.network).is_none_or(|n| n.vlan != vlan) {
                        continue;
                    }
                    let Some(link) = cfg.link(&att.link).filter(|l| l.mtu >= mtu) else { continue };
                    for port in link.kind.ports().iter().filter(|p| via.is_none_or(|v| v == *p)) {
                        let node = Node::HostPort((*id).clone(), port.clone());
                        out.extend(self.attach_node(node, vlan, att.tagged, mtu));
                    }
                }
                out
            }
            Station::Client => self.attach_node(Node::Client, vlan, false, mtu).into_iter().collect(),
            Station::Router => self
                .attach_node(Node::Router, vlan, false, mtu)
                .or_else(|| self.attach_node(Node::Router, vlan, true, mtu))
                .into_iter()
                .collect(),
            Station::Switch(s) => match w.topology.switches.get(*s) {
                Some(sw) if sw.up => vec![Attach { hop: None, switch: sw.id.clone() }],
                _ => vec![],
            },
        }
    }

    /// The switch `node`'s cable delivers a frame to, if the cable is up and
    /// uncut, the switch is up, and its port is up, big enough and accepts it.
    fn attach_node(&self, node: Node, vlan: u16, tagged: bool, mtu: u16) -> Option<Attach> {
        let t = &self.world.topology;
        let peer = t.peer(&node)?;
        let Node::SwitchPort(s, p) = peer else { return None };
        if !t.is_cable_up(&node, peer) || t.is_cut(&node, peer) {
            return None;
        }
        let sw = t.switches.get(s).filter(|sw| sw.up)?;
        let cfg = sw.port(p)?;
        if !(cfg.admin_up && cfg.mtu >= mtu && cfg.accepts(vlan, tagged)) {
            return None;
        }
        Some(Attach { switch: s.clone(), hop: Some(LinkHop { from: node, to: peer.clone() }) })
    }

    /// Runs `f` on the cached switch graph for (`vlan`, `mtu`), building it if
    /// the topology changed since the cache was filled.
    fn with_graph<R>(&self, vlan: u16, mtu: u16, f: impl FnOnce(&SwitchGraph) -> R) -> R {
        let mut cache = self.graphs.borrow_mut();
        let version = Some(self.world.topology.version);
        if cache.version != version {
            *cache = GraphCache { version, graphs: HashMap::new() };
        }
        let g = cache.graphs.entry((vlan, mtu)).or_insert_with(|| self.build_graph(vlan, mtu));
        f(g)
    }

    /// Switch-to-switch cables that are up, uncut, between up switches, with
    /// both ports up, at least `mtu`, and carrying `vlan` tagged.
    fn build_graph(&self, vlan: u16, mtu: u16) -> SwitchGraph {
        let t = &self.world.topology;
        let port_ok = |s: &SwitchId, p: &PortName| {
            t.switches
                .get(s)
                .filter(|sw| sw.up)
                .and_then(|sw| sw.port(p))
                .is_some_and(|c| c.admin_up && c.mtu >= mtu && c.accepts(vlan, true))
        };
        let mut g = SwitchGraph::new();
        for (a, b) in &t.cables {
            let (Node::SwitchPort(sa, pa), Node::SwitchPort(sb, pb)) = (a, b) else { continue };
            // Each cable is stored both ways; this direction adds `a`'s adjacency only.
            if t.is_cable_up(a, b) && !t.is_cut(a, b) && port_ok(sa, pa) && port_ok(sb, pb) {
                g.entry(sa.clone()).or_default().push((pa.clone(), sb.clone(), pb.clone()));
            }
        }
        for next in g.values_mut() {
            next.sort_by(|(pa, sa, _), (pb, sb, _)| (sa, pa).cmp(&(sb, pb)));
        }
        g
    }
}

fn reverse(h: &LinkHop) -> LinkHop {
    LinkHop { from: h.to.clone(), to: h.from.clone() }
}

impl Deliver for Fabric {
    /// Resolves the VIP via the sender's access switch, then requires a
    /// path on the message's network that carries `CONTROL_MTU` frames.
    /// Messages between services on one host never touch the wire.
    fn route(&mut self, from: &Endpoint, to: &Endpoint, net: &NetworkId) -> Option<(Endpoint, u64)> {
        let vlan = self.world.networks.get(net)?.vlan;
        // A switch's management interface lives on the mgmt VLAN and nowhere else.
        let to_or_from_switch = [from, to].iter().any(|e| matches!(e.addr, Addr::Switch(_)));
        if to_or_from_switch && Some(vlan) != self.mgmt_vlan() {
            return None;
        }
        let src = Station::of_addr(&from.addr)?;
        let dest = match &to.addr {
            Addr::Vip => Endpoint::new(Addr::Host(self.vip_seen_by(&src)?), to.svc),
            _ => to.clone(),
        };
        if dest.addr == from.addr {
            return Some((dest, 1));
        }
        let hops = self.find_path(&src, &Station::of_addr(&dest.addr)?, vlan, CONTROL_MTU)?.len();
        let latency = self.latency_ms(hops);
        Some((dest, latency))
    }

    fn claim_vip(&mut self, host: &HostId) {
        self.record_vip_claim(host);
    }

    /// Moves instance demands along their curves, routes every flow as the
    /// world is now and accounts the interval just ended, then ends the flows
    /// whose time is up and advances replica copies by what they carried.
    fn tick(&mut self, now: SimTime) {
        self.world.update_demands(now);
        let mut traffic = std::mem::take(&mut self.world.traffic);
        traffic.tick(self);
        traffic.expire(now);
        self.world.traffic = traffic;
        self.world.advance_copies();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::topology::{PartitionMember, PortMode, Switch, SwitchPortCfg};
    use crate::world::HostHw;
    use control::proto::Msg;
    use control::runtime::{Actor, Addr, Ctx, SimTime, Svc};
    use model::network::{Attachment, Link, LinkKind};
    use model::{PortName, ProfileId, SwitchId};
    use std::sync::{Arc, Mutex};

    fn h(id: &str) -> HostId {
        HostId::from(id)
    }
    fn sw(id: &str) -> SwitchId {
        SwitchId::from(id)
    }
    fn hp(host: &str, port: &str) -> Node {
        Node::HostPort(h(host), PortName::from(port))
    }
    fn sp(switch: &str, port: &str) -> Node {
        Node::SwitchPort(sw(switch), PortName::from(port))
    }
    fn access(vlan: u16, mtu: u16) -> SwitchPortCfg {
        SwitchPortCfg { mode: PortMode::Access(vlan), mtu, lacp_group: None, admin_up: true }
    }
    fn trunk(native: Option<u16>, allowed: &[u16], mtu: u16) -> SwitchPortCfg {
        SwitchPortCfg {
            mode: PortMode::Trunk { native, allowed: allowed.iter().copied().collect() },
            mtu,
            lacp_group: None,
            admin_up: true,
        }
    }
    fn switch(id: &str, ports: &[(&str, SwitchPortCfg)]) -> Switch {
        Switch {
            id: sw(id),
            model: "test-switch".into(),
            ports: ports.iter().map(|(p, c)| (PortName::from(*p), (10_000, c.clone()))).collect(),
            mlag_peer: None,
            up: true,
        }
    }

    const H1: &str = "h1";
    const H2: &str = "h2";

    /// sw-a and sw-b joined by a trunk (p48) allowing {1, 20}. h1 is on sw-a p1,
    /// h2 on sw-b p1, both access 1 with bootstrap config. The client is on sw-a p2.
    fn fixture() -> World {
        let mut w = World::new();
        let t = &mut w.topology;
        let up = trunk(None, &[1, 20], 9216);
        t.add_switch(switch("sw-a", &[("p1", access(1, 9216)), ("p2", access(1, 9216)), ("p48", up.clone())]));
        t.add_switch(switch("sw-b", &[("p1", access(1, 9216)), ("p48", up)]));
        t.connect(sp("sw-a", "p48"), sp("sw-b", "p48"));
        t.connect(hp(H1, "eth0"), sp("sw-a", "p1"));
        t.connect(hp(H2, "eth0"), sp("sw-b", "p1"));
        t.connect(Node::Client, sp("sw-a", "p2"));
        for id in [H1, H2] {
            w.hosts.insert(h(id), HostHw::new(ProfileId::MiniPc, 0));
        }
        w
    }

    /// Adds `storage` tagged on eth0 (link MTU 9000) to both hosts and turns their
    /// switch ports into trunks: native 1 for mgmt, VLAN 20 allowed.
    fn with_storage(mut w: World) -> World {
        for id in [H1, H2] {
            let net = &mut w.hosts.get_mut(&h(id)).unwrap().applied_net;
            net.links = vec![Link { name: "eth0".into(), kind: LinkKind::Single(PortName::from("eth0")), mtu: 9000 }];
            net.attachments.push(Attachment { network: NetworkId::from("storage"), link: "eth0".into(), tagged: true, ip: None });
        }
        for s in ["sw-a", "sw-b"] {
            w.topology.set_port(&sw(s), &PortName::from("p1"), trunk(Some(1), &[20], 9216));
        }
        w
    }

    fn fabric(w: World) -> Fabric {
        Fabric::new(w, 7)
    }

    fn h1_h2(f: &Fabric, vlan: u16, mtu: u16) -> bool {
        f.reachable(&hp(H1, "eth0"), &hp(H2, "eth0"), vlan, mtu)
    }

    #[test]
    fn a_flow_carries_its_last_interval_before_it_expires() {
        use crate::traffic::{FlowEnd, FlowKind};
        let mut f = fabric(fixture());
        let mgmt = NetworkId::from("mgmt");
        let id = f.traffic_mut().start(FlowEnd::Host(h(H1)), FlowEnd::Host(h(H2)), mgmt, 800.0, FlowKind::Iperf, Some(5000));
        for now in [1000, 2000, 3000, 4000, 5000] {
            f.tick(now);
        }
        let t = &f.world().traffic;
        assert!(t.flow(id).is_none(), "its time is up");
        // 800 Mbit/s for 5 s: the tick at 5000 accounts the fifth second before the flow ends.
        assert_eq!(t.carried_bytes(id), 500e6);
    }

    #[test]
    fn untagged_mgmt_reachable() {
        let f = fabric(fixture());
        assert!(h1_h2(&f, 1, 1500));
        assert!(f.reachable(&hp(H2, "eth0"), &hp(H1, "eth0"), 1, 1500));
        assert!(!h1_h2(&f, 20, 1500), "bootstrap config has no storage attachment");
    }

    #[test]
    fn tagged_vlan_not_allowed_blocked() {
        let f = fabric(with_storage(fixture()));
        assert!(h1_h2(&f, 20, 1500), "control: storage works with VLAN 20 allowed on both host ports");

        let mut w = with_storage(fixture());
        w.topology.set_port(&sw("sw-b"), &PortName::from("p1"), trunk(Some(1), &[1], 9216));
        let f = fabric(w);
        assert!(!h1_h2(&f, 20, 1500));
        assert!(h1_h2(&f, 1, 1500), "untagged mgmt still rides the native VLAN");
    }

    #[test]
    fn untagged_frame_needs_matching_access_or_native_vlan() {
        let mut w = fixture();
        w.topology.set_port(&sw("sw-b"), &PortName::from("p1"), access(100, 9216));
        assert!(!h1_h2(&fabric(w.clone()), 1, 1500));
        w.topology.set_port(&sw("sw-b"), &PortName::from("p1"), trunk(None, &[1], 9216));
        assert!(!h1_h2(&fabric(w), 1, 1500), "trunk without native drops untagged mgmt");
    }

    #[test]
    fn switch_link_must_allow_vlan_tagged() {
        let mut w = with_storage(fixture());
        w.topology.set_port(&sw("sw-a"), &PortName::from("p48"), trunk(None, &[1], 9216));
        let f = fabric(w);
        assert!(!h1_h2(&f, 20, 1500));
        assert!(h1_h2(&f, 1, 1500));
    }

    #[test]
    fn mtu_blackhole() {
        let mut w = with_storage(fixture());
        assert!(h1_h2(&fabric(w.clone()), 20, 9000), "control: jumbo path end to end");
        for s in ["sw-a", "sw-b"] {
            w.topology.set_port(&sw(s), &PortName::from("p48"), trunk(None, &[1, 20], 1500));
        }
        let f = fabric(w.clone());
        assert!(!h1_h2(&f, 20, 9000));
        assert!(h1_h2(&f, 20, 1400));

        // The host link MTU counts too.
        let mut w = with_storage(fixture());
        w.hosts.get_mut(&h(H2)).unwrap().applied_net.links[0].mtu = 1500;
        let f = fabric(w);
        assert!(!h1_h2(&f, 20, 9000));
        assert!(h1_h2(&f, 20, 1500));
    }

    #[test]
    fn unplug_cable_and_kill_switch() {
        let mut f = fabric(fixture());
        type Toggle = fn(&mut World, bool);
        let breakers: Vec<(&str, Toggle)> = vec![
            ("unplug h2 cable", |w, up| w.topology.set_cable_up(&hp(H2, "eth0"), up)),
            ("unplug trunk", |w, up| w.topology.set_cable_up(&sp("sw-a", "p48"), up)),
            ("kill sw-b", |w, up| w.topology.set_switch_up(&sw("sw-b"), up)),
            ("admin down h1 port", |w, up| {
                let mut cfg = access(1, 9216);
                cfg.admin_up = up;
                w.topology.set_port(&sw("sw-a"), &PortName::from("p1"), cfg);
            }),
            ("power off h2", |w, up| w.hosts.get_mut(&h(H2)).unwrap().powered = up),
        ];
        for (name, set) in breakers {
            assert!(h1_h2(&f, 1, 1500), "before {name}");
            set(f.world_mut(), false);
            assert!(!h1_h2(&f, 1, 1500), "{name} should break reachability");
            set(f.world_mut(), true);
            assert!(h1_h2(&f, 1, 1500), "restoring after {name} should heal");
        }
    }

    #[test]
    fn world_mut_invalidates_cached_graphs_even_without_a_version_bump() {
        let mut f = fabric(fixture());
        assert!(h1_h2(&f, 1, 1500), "fills the cache");
        // Edit the cable set directly, bypassing Topology's version-bumping methods.
        let key = crate::topology::cable_key(&sp("sw-a", "p48"), &sp("sw-b", "p48"));
        f.world_mut().topology.cable_up.remove(&key);
        assert!(!h1_h2(&f, 1, 1500), "stale graph still routes over the unplugged trunk");
        f.world_mut().topology.cable_up.insert(key);
        assert!(h1_h2(&f, 1, 1500));
    }

    #[test]
    fn host_mut_reaches_host_hardware() {
        let mut f = fabric(fixture());
        f.host_mut(&h(H2)).unwrap().powered = false;
        assert!(!h1_h2(&f, 1, 1500), "host state is read per query, never cached");
        assert!(f.host_mut(&h("nope")).is_none());
    }

    #[test]
    fn partition_by_switch() {
        let mut f = fabric(fixture());
        f.world_mut().topology.partition([PartitionMember::Switch(sw("sw-b"))].into());
        assert!(!h1_h2(&f, 1, 1500));
        assert!(f.reachable(&hp(H1, "eth0"), &Node::Client, 1, 1500));
        assert!(!f.reachable(&hp(H2, "eth0"), &Node::Client, 1, 1500));
        assert!(f.reachable(&hp(H2, "eth0"), &sp("sw-b", "p1"), 1, 1500), "h2 stays with its switch");
        f.world_mut().topology.heal_partitions();
        assert!(h1_h2(&f, 1, 1500));
    }

    #[test]
    fn partition_by_host_cuts_its_ports() {
        let mut f = fabric(fixture());
        f.world_mut().topology.partition([PartitionMember::Host(h(H2))].into());
        assert!(!h1_h2(&f, 1, 1500));
        assert!(f.reachable(&hp(H1, "eth0"), &Node::Client, 1, 1500));
    }

    #[test]
    fn path_lists_each_cable_in_travel_order() {
        let f = fabric(fixture());
        let hop = |a: Node, b: Node| LinkHop { from: a, to: b };
        assert_eq!(
            f.path(&hp(H1, "eth0"), &hp(H2, "eth0"), 1),
            Some(vec![
                hop(hp(H1, "eth0"), sp("sw-a", "p1")),
                hop(sp("sw-a", "p48"), sp("sw-b", "p48")),
                hop(sp("sw-b", "p1"), hp(H2, "eth0")),
            ])
        );
        assert_eq!(
            f.path(&Node::Client, &hp(H1, "eth0"), 1),
            Some(vec![hop(Node::Client, sp("sw-a", "p2")), hop(sp("sw-a", "p1"), hp(H1, "eth0"))])
        );
        assert_eq!(f.path(&hp(H1, "eth0"), &hp(H2, "eth0"), 30), None);
    }

    #[test]
    fn vip_follows_last_reachable_claim() {
        let mut f = fabric(fixture());
        assert_eq!(f.resolve_vip(&h(H1)), None, "no claim yet");
        f.record_vip_claim(&h(H1));
        assert_eq!(f.resolve_vip(&h(H2)), Some(h(H1)));

        f.world_mut().topology.partition([PartitionMember::Switch(sw("sw-b"))].into());
        f.record_vip_claim(&h(H2));
        assert_eq!(f.resolve_vip(&h(H1)), Some(h(H1)), "sw-a keeps its ARP entry");
        assert_eq!(f.resolve_vip(&h(H2)), Some(h(H2)), "sw-b learned h2");

        f.world_mut().topology.heal_partitions();
        assert_eq!(f.resolve_vip(&h(H1)), Some(h(H1)), "healing alone does not refresh ARP");
        f.record_vip_claim(&h(H2));
        assert_eq!(f.resolve_vip(&h(H1)), Some(h(H2)));
        assert_eq!(f.resolve_vip(&h(H2)), Some(h(H2)));
    }

    fn ep(host: &str, svc: Svc) -> Endpoint {
        Endpoint::new(Addr::Host(h(host)), svc)
    }

    #[test]
    fn route_uses_network_vlan_and_resolves_vip_keeping_svc() {
        let mut f = fabric(with_storage(fixture()));
        let mgmt = NetworkId::from("mgmt");
        assert_eq!(f.route(&ep(H1, Svc::Agent), &ep(H2, Svc::Agent), &mgmt), Some((ep(H2, Svc::Agent), 1)));
        assert!(f.route(&ep(H1, Svc::Agent), &ep(H2, Svc::Agent), &NetworkId::from("storage")).is_some());
        assert_eq!(f.route(&ep(H1, Svc::Agent), &ep(H2, Svc::Agent), &NetworkId::from("vm-lan")), None);
        assert_eq!(f.route(&ep(H1, Svc::Agent), &ep(H2, Svc::Agent), &NetworkId::from("nope")), None);

        let vip = Endpoint::new(Addr::Vip, Svc::Cp);
        assert_eq!(f.route(&ep(H2, Svc::Agent), &vip, &mgmt), None, "nobody holds the VIP");
        f.record_vip_claim(&h(H1));
        assert_eq!(f.route(&ep(H2, Svc::Agent), &vip, &mgmt), Some((ep(H1, Svc::Cp), 1)));
        let client = Endpoint::new(Addr::Client, Svc::Client);
        assert_eq!(f.route(&client, &vip, &mgmt), Some((ep(H1, Svc::Cp), 1)));
        assert_eq!(f.route(&ep(H1, Svc::Agent), &vip, &mgmt), Some((ep(H1, Svc::Cp), 1)), "loopback");
    }

    #[test]
    fn route_follows_stale_arp_into_the_void() {
        let mut f = fabric(fixture());
        f.record_vip_claim(&h(H1));
        f.world_mut().topology.partition([PartitionMember::Switch(sw("sw-b"))].into());
        let vip = Endpoint::new(Addr::Vip, Svc::Cp);
        // sw-b still points at h1, which it can no longer reach.
        assert_eq!(f.route(&ep(H2, Svc::Agent), &vip, &NetworkId::from("mgmt")), None);
    }

    #[test]
    fn latency_is_point_two_ms_per_hop_plus_jitter_rounded_up() {
        let mut f = fabric(fixture());
        for _ in 0..50 {
            assert_eq!(f.latency_ms(0), 1, "minimum 1 ms");
            assert_eq!(f.latency_ms(3), 1);
            assert_eq!(f.latency_ms(12), 3, "2.4–2.5 ms rounds up to 3");
        }
    }

    fn switch_ep(id: &str) -> Endpoint {
        Endpoint::new(Addr::Switch(sw(id)), Svc::Switch)
    }

    #[test]
    fn switch_management_is_reachable_on_mgmt_through_the_fabric() {
        let mut f = fabric(fixture());
        let mgmt = NetworkId::from("mgmt");
        // From a host on the switch itself, on another switch, and from the client.
        assert_eq!(f.route(&switch_ep("sw-a"), &ep(H1, Svc::Cp), &mgmt), Some((ep(H1, Svc::Cp), 1)));
        assert_eq!(f.route(&ep(H1, Svc::Cp), &switch_ep("sw-a"), &mgmt), Some((switch_ep("sw-a"), 1)));
        assert_eq!(f.route(&ep(H1, Svc::Cp), &switch_ep("sw-b"), &mgmt), Some((switch_ep("sw-b"), 1)), "over the trunk");
        let client = Endpoint::new(Addr::Client, Svc::Client);
        assert!(f.route(&client, &switch_ep("sw-b"), &mgmt).is_some());
        // Switch to switch.
        assert!(f.route(&switch_ep("sw-a"), &switch_ep("sw-b"), &mgmt).is_some());
        // The VIP resolves through the switch's own ARP table.
        f.record_vip_claim(&h(H2));
        let vip = Endpoint::new(Addr::Vip, Svc::Cp);
        assert_eq!(f.route(&switch_ep("sw-a"), &vip, &mgmt), Some((ep(H2, Svc::Cp), 1)));
        // Unknown switches and networks other than mgmt go nowhere.
        assert_eq!(f.route(&ep(H1, Svc::Cp), &switch_ep("nope"), &mgmt), None);
        let f2 = fabric(with_storage(fixture()));
        let mut f2 = f2;
        assert_eq!(f2.route(&ep(H1, Svc::Cp), &switch_ep("sw-b"), &NetworkId::from("storage")), None, "the interface is mgmt only");
        assert_eq!(f2.route(&switch_ep("sw-b"), &ep(H1, Svc::Cp), &NetworkId::from("storage")), None);
    }

    #[test]
    fn switch_management_follows_switch_state_cables_and_partitions() {
        let mut f = fabric(fixture());
        let mgmt = NetworkId::from("mgmt");
        let to_b = |f: &mut Fabric| f.route(&ep(H1, Svc::Cp), &switch_ep("sw-b"), &mgmt).is_some();
        let from_b = |f: &mut Fabric| f.route(&switch_ep("sw-b"), &ep(H1, Svc::Cp), &mgmt).is_some();
        type Toggle = fn(&mut World, bool);
        let breakers: Vec<(&str, Toggle)> = vec![
            ("kill sw-b", |w, up| w.topology.set_switch_up(&sw("sw-b"), up)),
            ("unplug the trunk", |w, up| w.topology.set_cable_up(&sp("sw-a", "p48"), up)),
            ("kill sw-a, which h1 hangs off", |w, up| w.topology.set_switch_up(&sw("sw-a"), up)),
            ("h1 loses its cable", |w, up| w.topology.set_cable_up(&hp(H1, "eth0"), up)),
            ("h1's port leaves mgmt", |w, up| w.topology.set_port(&sw("sw-a"), &PortName::from("p1"), access(if up { 1 } else { 100 }, 9216))),
            ("trunk drops VLAN 1", |w, up| {
                let allowed: &[u16] = if up { &[1, 20] } else { &[20] };
                w.topology.set_port(&sw("sw-b"), &PortName::from("p48"), trunk(None, allowed, 9216));
            }),
        ];
        for (name, set) in breakers {
            assert!(to_b(&mut f) && from_b(&mut f), "before {name}");
            set(f.world_mut(), false);
            assert!(!to_b(&mut f) && !from_b(&mut f), "{name} should cut the management interface off");
            set(f.world_mut(), true);
            assert!(to_b(&mut f) && from_b(&mut f), "healing {name}");
        }
        // A dead switch cannot be reached or reach anyone, even on its own side.
        f.world_mut().topology.set_switch_up(&sw("sw-b"), false);
        assert!(f.route(&switch_ep("sw-b"), &ep(H2, Svc::Cp), &mgmt).is_none());
        f.world_mut().topology.set_switch_up(&sw("sw-b"), true);

        // Partitioning a switch cuts it off from hosts outside, but not from its own.
        f.world_mut().topology.partition([PartitionMember::Switch(sw("sw-b"))].into());
        assert!(!to_b(&mut f) && !from_b(&mut f));
        assert!(f.route(&switch_ep("sw-b"), &ep(H2, Svc::Cp), &mgmt).is_some(), "h2 stays with its switch");
        assert!(f.route(&switch_ep("sw-a"), &ep(H1, Svc::Cp), &mgmt).is_some());
        f.world_mut().topology.heal_partitions();
        // A partitioned host is cut off from the switch it hangs off.
        f.world_mut().topology.partition([PartitionMember::Host(h(H2))].into());
        assert!(f.route(&switch_ep("sw-b"), &ep(H2, Svc::Cp), &mgmt).is_none());
        assert!(f.route(&switch_ep("sw-b"), &ep(H1, Svc::Cp), &mgmt).is_some());
    }

    type Log = Arc<Mutex<Vec<(Endpoint, Endpoint, Msg, SimTime)>>>;

    /// Optionally claims the VIP on start; pings `target` at each `ping_at`;
    /// logs (me, from, msg, now) for every message received.
    struct Probe {
        log: Log,
        claim: bool,
        target: Option<Endpoint>,
        ping_at: Vec<u64>,
    }

    impl Actor for Probe {
        fn on_start(&mut self, ctx: &mut dyn Ctx) {
            if self.claim {
                ctx.claim_vip();
            }
            for &t in &self.ping_at {
                ctx.set_timer(t, t);
            }
        }
        fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg) {
            self.log.lock().unwrap().push((ctx.me(), from, msg, ctx.now()));
        }
        fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64) {
            if let Some(t) = self.target.clone() {
                ctx.send(t, NetworkId::from("mgmt"), Msg::Ping(token));
            }
        }
    }

    #[test]
    fn engine_routes_vip_through_fabric() {
        let log: Log = Arc::new(Mutex::new(vec![]));
        let probe = |claim, target: Option<Endpoint>, ping_at: &[u64]| {
            Box::new(Probe { log: log.clone(), claim, target, ping_at: ping_at.to_vec() })
        };
        let mut e = Engine::new(1);
        e.set_deliver(Box::new(fabric(fixture())));
        e.spawn(ep(H1, Svc::Agent), probe(false, None, &[]));
        e.spawn(ep(H1, Svc::Cp), probe(true, None, &[]));
        e.spawn(ep(H2, Svc::Agent), probe(false, Some(Endpoint::new(Addr::Vip, Svc::Cp)), &[10, 100]));
        e.run_for(50);
        assert_eq!(*log.lock().unwrap(), vec![(ep(H1, Svc::Cp), ep(H2, Svc::Agent), Msg::Ping(10), 11)]);

        let f = e.deliver_mut::<Fabric>().expect("fabric installed");
        f.world_mut().topology.partition([PartitionMember::Switch(sw("sw-b"))].into());
        e.run_for(100);
        assert_eq!(log.lock().unwrap().len(), 1, "partitioned ping must be dropped");
        assert_eq!(e.deliver::<Fabric>().unwrap().resolve_vip(&h(H2)), Some(h(H1)));
    }
}
