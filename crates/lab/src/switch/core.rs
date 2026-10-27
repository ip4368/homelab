//! The switches' forwarding logic, free of I/O: a frame enters a port and
//! the frames that leave external ports come back. Everything a real VLAN
//! switch does that the control plane can observe or trip over is here:
//! per-port access/trunk VLANs and MTU, MAC learning per VLAN, LACP link
//! aggregation, LLDP, admin state, link state, port speed (policed), and the
//! lab's faults (a dead switch, partitions, a degraded port).

use super::frame::{self, Header, LacpInfo, Mac};
use model::network::{LldpPeer, ObservedSwitchPort, PortMode, SwitchPortCfg};
use model::{HostId, PortName, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};
use ts_rs::TS;

pub type PortId = usize;

/// How often the switch sends LACPDUs (fast rate) and LLDP.
pub const LACP_EVERY: Duration = Duration::from_secs(1);
pub const LLDP_EVERY: Duration = Duration::from_secs(5);
/// A LAG member whose partner has been silent this long leaves the aggregate.
pub const LACP_EXPIRE: Duration = Duration::from_secs(3);
/// Learned addresses are forgotten after this long without a frame from them.
pub const FDB_AGE: Duration = Duration::from_secs(300);

/// What is on the other end of a port's cable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Peer {
    None,
    Host { host: HostId, nic: String, nic_mbps: u32 },
    /// The lab host: the router, and where switch management answers.
    Router,
    Switch { switch: SwitchId, port: PortName },
}

#[derive(Debug, Clone)]
pub struct PortDef {
    pub switch: SwitchId,
    pub name: PortName,
    pub speed_mbps: u32,
    pub cfg: SwitchPortCfg,
    pub peer: Peer,
}

/// A member of a partition group: a listed host puts all its ports inside.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
#[ts(export, rename = "LabMember")]
pub enum Member {
    Host(HostId),
    Switch(SwitchId),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PortCounters {
    #[ts(type = "number")]
    pub rx_bytes: u64,
    #[ts(type = "number")]
    pub tx_bytes: u64,
    #[ts(type = "number")]
    pub rx_frames: u64,
    #[ts(type = "number")]
    pub tx_frames: u64,
    /// Frames dropped for a VLAN the port does not carry.
    #[ts(type = "number")]
    pub drop_vlan: u64,
    /// Frames dropped for exceeding the port's MTU.
    #[ts(type = "number")]
    pub drop_mtu: u64,
    /// Frames dropped by the port's speed limit.
    #[ts(type = "number")]
    pub drop_rate: u64,
    /// Frames dropped crossing a partition.
    #[ts(type = "number")]
    pub drop_partition: u64,
    /// Frames dropped arriving on a LAG member LACP has not readied.
    #[ts(type = "number")]
    pub drop_lacp: u64,
}

/// A token bucket in bits: refills at `rate` and holds a few ms of it (it starts full).
#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn take(&mut self, bits: f64, rate_mbps: u32, now: Instant) -> bool {
        let rate = f64::from(rate_mbps) * 1e6;
        let cap = (rate * 0.005).max(1e6);
        self.tokens = (self.tokens + now.saturating_duration_since(self.at).as_secs_f64() * rate).min(cap);
        self.at = now;
        if self.tokens >= bits {
            self.tokens -= bits;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
struct Port {
    def: PortDef,
    mac: Mac,
    /// The far end is there and its link is up (a VM's NIC: powered on, not unplugged).
    peer_up: bool,
    /// Crosses a partition: frames do not pass, the link stays up.
    cut: bool,
    /// A lower speed the lab forced on the port (a bad cable), if any.
    degraded_mbps: Option<u32>,
    rx: Bucket,
    tx: Bucket,
    counters: PortCounters,
    /// The LACP partner's latest LACPDU, and when it came.
    partner: Option<(LacpInfo, Instant)>,
    last_lacp: Option<Instant>,
    last_lldp: Option<Instant>,
}

impl Port {
    fn speed(&self) -> u32 {
        let nic = match &self.def.peer {
            Peer::Host { nic_mbps, .. } => *nic_mbps,
            _ => self.def.speed_mbps,
        };
        let s = self.def.speed_mbps.min(nic);
        self.degraded_mbps.map_or(s, |d| s.min(d))
    }
}

#[derive(Debug, Clone)]
struct Switch {
    model: String,
    up: bool,
    index: u8,
    /// (VLAN, MAC) → where it was last seen, and when.
    fdb: HashMap<(u16, Mac), (Egress, Instant)>,
}

/// Where a switch sends a frame: one port, or a LAG (by group name).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Egress {
    Port(PortId),
    Lag(String),
}

/// A frame to put on the wire of an external port.
pub type Out = (PortId, Vec<u8>);

#[derive(Debug, Clone)]
pub struct Core {
    switches: BTreeMap<SwitchId, Switch>,
    ports: Vec<Port>,
    index: HashMap<(SwitchId, PortName), PortId>,
    partitions: Vec<BTreeSet<Member>>,
}

impl Core {
    /// Switches (id, model) and their ports. Ports whose peer is another
    /// switch must name a port that names them back.
    pub fn new(switches: &[(SwitchId, String)], ports: Vec<PortDef>, now: Instant) -> Self {
        let switches: BTreeMap<SwitchId, Switch> = switches
            .iter()
            .enumerate()
            .map(|(i, (id, model))| (id.clone(), Switch { model: model.clone(), up: true, index: i as u8, fdb: HashMap::new() }))
            .collect();
        let index = ports.iter().enumerate().map(|(i, p)| ((p.switch.clone(), p.name.clone()), i)).collect();
        let ports = ports
            .into_iter()
            .enumerate()
            .map(|(i, def)| {
                let s = switches[&def.switch].index;
                let peer_up = matches!(def.peer, Peer::Router | Peer::Switch { .. });
                Port {
                    mac: [0x02, 0x5e, s, (i >> 8) as u8, i as u8, 0x01],
                    def,
                    peer_up,
                    cut: false,
                    degraded_mbps: None,
                    rx: Bucket { tokens: f64::MAX, at: now },
                    tx: Bucket { tokens: f64::MAX, at: now },
                    counters: PortCounters::default(),
                    partner: None,
                    last_lacp: None,
                    last_lldp: None,
                }
            })
            .collect();
        Self { switches, ports, index, partitions: vec![] }
    }

    pub fn port_id(&self, switch: &SwitchId, port: &PortName) -> Option<PortId> {
        self.index.get(&(switch.clone(), port.clone())).copied()
    }

    pub fn ports(&self) -> impl Iterator<Item = (PortId, &PortDef)> {
        self.ports.iter().enumerate().map(|(i, p)| (i, &p.def))
    }

    pub fn switch_ids(&self) -> impl Iterator<Item = &SwitchId> {
        self.switches.keys()
    }

    pub fn counters(&self, port: PortId) -> PortCounters {
        self.ports[port].counters
    }

    pub fn switch_up(&self, s: &SwitchId) -> bool {
        self.switches.get(s).is_some_and(|s| s.up)
    }

    pub fn partitions(&self) -> &[BTreeSet<Member>] {
        &self.partitions
    }

    // ---- what the lab changes -------------------------------------------

    pub fn set_cfg(&mut self, port: PortId, cfg: SwitchPortCfg) {
        let p = &mut self.ports[port];
        let lag_changed = p.def.cfg.lacp_group != cfg.lacp_group;
        p.def.cfg = cfg;
        if lag_changed {
            p.partner = None;
        }
        let s = p.def.switch.clone();
        self.flush_fdb(&s);
    }

    /// Re-cables `port` to `peer` (its link stays down until the lab says otherwise).
    pub fn set_peer(&mut self, port: PortId, peer: Peer) {
        let p = &mut self.ports[port];
        p.def.peer = peer;
        p.peer_up = matches!(p.def.peer, Peer::Router | Peer::Switch { .. });
        p.partner = None;
        self.recut();
    }

    pub fn set_peer_up(&mut self, port: PortId, up: bool) {
        let p = &mut self.ports[port];
        if p.peer_up != up {
            p.peer_up = up;
            p.partner = None;
            let s = p.def.switch.clone();
            self.flush_fdb(&s);
        }
    }

    pub fn set_switch_up(&mut self, s: &SwitchId, up: bool) {
        if let Some(sw) = self.switches.get_mut(s) {
            sw.up = up;
            sw.fdb.clear();
        }
        for p in self.ports.iter_mut().filter(|p| &p.def.switch == s) {
            p.partner = None;
        }
    }

    pub fn degrade(&mut self, port: PortId, mbps: Option<u32>) {
        self.ports[port].degraded_mbps = mbps;
    }

    pub fn partition(&mut self, group: BTreeSet<Member>) {
        self.partitions.push(group);
        self.recut();
    }

    pub fn heal(&mut self) {
        self.partitions.clear();
        self.recut();
    }

    /// Marks every port whose cable crosses a partition boundary (R10): a
    /// listed host takes all its cables inside its group, an unlisted host
    /// or the router follows its switch.
    fn recut(&mut self) {
        let parts = self.partitions.clone();
        for p in &mut self.ports {
            let sw_in = |g: &BTreeSet<Member>, s: &SwitchId| g.contains(&Member::Switch(s.clone()));
            p.cut = parts.iter().any(|g| {
                let here = sw_in(g, &p.def.switch);
                match &p.def.peer {
                    Peer::Host { host, .. } => here != (here || g.contains(&Member::Host(host.clone()))),
                    Peer::Switch { switch, .. } => here != sw_in(g, switch),
                    Peer::None | Peer::Router => false,
                }
            });
        }
        let ids: Vec<SwitchId> = self.switches.keys().cloned().collect();
        for s in ids {
            self.flush_fdb(&s);
        }
    }

    fn flush_fdb(&mut self, s: &SwitchId) {
        if let Some(sw) = self.switches.get_mut(s) {
            sw.fdb.clear();
        }
    }

    // ---- state ------------------------------------------------------------

    /// Whether `port` has carrier: cabled, both ends up and admin-up.
    pub fn link_up(&self, port: PortId) -> bool {
        let p = &self.ports[port];
        let far = match &p.def.peer {
            Peer::None => false,
            Peer::Host { .. } | Peer::Router => p.peer_up,
            Peer::Switch { switch, port: far } => {
                self.switch_up(switch) && self.port_id(switch, far).is_some_and(|f| self.ports[f].def.cfg.admin_up)
            }
        };
        far && p.def.cfg.admin_up && self.switch_up(&p.def.switch)
    }

    /// Whether the switch's end of `port` gives a peer carrier: the switch
    /// is powered and the port admin-up.
    pub fn far_end_up(&self, port: PortId) -> bool {
        let p = &self.ports[port];
        p.def.cfg.admin_up && self.switch_up(&p.def.switch)
    }

    /// Whether frames can cross `port` now (link up, not cut).
    fn passes(&self, port: PortId) -> bool {
        self.link_up(port) && !self.ports[port].cut
    }

    /// A LAG member carries traffic once its LACP partner is heard and in sync.
    fn lag_member_ready(&self, port: PortId, now: Instant) -> bool {
        self.passes(port)
            && self.ports[port]
                .partner
                .is_some_and(|(info, at)| now.duration_since(at) < LACP_EXPIRE && info.state & frame::LACP_SYNC != 0)
    }

    /// The switch's own view of its ports, as its management interface reports them.
    pub fn report(&self, s: &SwitchId) -> Vec<ObservedSwitchPort> {
        let mut out: Vec<ObservedSwitchPort> = self
            .ports
            .iter()
            .enumerate()
            .filter(|(_, p)| &p.def.switch == s)
            .map(|(i, p)| ObservedSwitchPort {
                switch: s.clone(),
                port: p.def.name.clone(),
                cfg: p.def.cfg.clone(),
                mlag_peer: None,
                link_up: self.link_up(i),
                speed_mbps: if self.link_up(i) { p.speed() } else { 0 },
                neighbor: match &p.def.peer {
                    Peer::Switch { switch, port } if self.link_up(i) => Some(LldpPeer { switch: switch.clone(), port: port.clone() }),
                    _ => None,
                },
            })
            .collect();
        out.sort_by(|a, b| a.port.cmp(&b.port));
        out
    }

    pub fn model(&self, s: &SwitchId) -> Option<&str> {
        self.switches.get(s).map(|s| s.model.as_str())
    }

    /// Whether `s`'s management interface (on the mgmt VLAN) can be reached
    /// from the router: a path of live, uncut cables between up switches.
    pub fn reachable_from_router(&self, s: &SwitchId) -> bool {
        let start: Vec<&SwitchId> =
            self.ports.iter().enumerate().filter(|(i, p)| p.def.peer == Peer::Router && self.passes(*i)).map(|(_, p)| &p.def.switch).collect();
        let mut seen: BTreeSet<&SwitchId> = start.iter().copied().filter(|s| self.switch_up(s)).collect();
        let mut todo: Vec<&SwitchId> = seen.iter().copied().collect();
        while let Some(cur) = todo.pop() {
            for (i, p) in self.ports.iter().enumerate().filter(|(_, p)| &p.def.switch == cur) {
                if let Peer::Switch { switch, .. } = &p.def.peer
                    && self.passes(i)
                    && seen.insert(switch)
                {
                    todo.push(switch);
                }
            }
        }
        seen.contains(s)
    }

    // ---- forwarding -------------------------------------------------------

    /// What comes of a frame arriving from the wire at external port `port`.
    pub fn ingress(&mut self, port: PortId, f: &[u8], now: Instant) -> Vec<Out> {
        let mut out = vec![];
        self.enter(port, f, now, &mut out, 0);
        out
    }

    fn enter(&mut self, port: PortId, f: &[u8], now: Instant, out: &mut Vec<Out>, depth: u8) {
        let Some(h) = frame::parse(f) else { return };
        if !self.passes(port) {
            if self.link_up(port) {
                self.ports[port].counters.drop_partition += 1;
            }
            return;
        }
        let speed = self.ports[port].speed();
        let p = &mut self.ports[port];
        p.counters.rx_frames += 1;
        p.counters.rx_bytes += f.len() as u64;
        if !p.rx.take(f.len() as f64 * 8.0, speed, now) {
            p.counters.drop_rate += 1;
            return;
        }
        // Link-local protocols the switch answers itself; never forwarded.
        if h.dst == frame::SLOW_DST {
            if let Some((actor, _)) = frame::parse_lacpdu(f, &h)
                && p.def.cfg.lacp_group.is_some()
            {
                p.partner = Some((actor, now));
            }
            return;
        }
        if h.dst == frame::LLDP_DST {
            return;
        }
        // A LAG member takes frames only once LACP readied it, as a bond sends only then.
        if p.def.cfg.lacp_group.is_some() && !self.lag_member_ready(port, now) {
            self.ports[port].counters.drop_lacp += 1;
            return;
        }
        let p = &mut self.ports[port];
        let Some(vlan) = classify(&p.def.cfg, &h) else {
            p.counters.drop_vlan += 1;
            return;
        };
        if h.payload_len > usize::from(p.def.cfg.mtu) {
            p.counters.drop_mtu += 1;
            return;
        }
        let s = p.def.switch.clone();
        let from = match &p.def.cfg.lacp_group {
            Some(g) => Egress::Lag(g.clone()),
            None => Egress::Port(port),
        };
        let sw = self.switches.get_mut(&s).expect("port's switch exists");
        if !frame::is_multicast(&h.src) {
            sw.fdb.insert((vlan, h.src), (from.clone(), now));
        }
        let known = sw.fdb.get(&(vlan, h.dst)).filter(|(_, at)| now.duration_since(*at) < FDB_AGE).map(|(e, _)| e.clone());
        let targets: Vec<Egress> = match known {
            Some(e) if !frame::is_multicast(&h.dst) => vec![e],
            _ => self.egresses(&s),
        };
        for e in targets.into_iter().filter(|e| *e != from) {
            let Some(q) = self.pick(&s, &e, f, &h, now) else { continue };
            self.leave(q, f, &h, vlan, now, out, depth);
        }
    }

    /// Every egress of switch `s`: its LAGs once each, and its other ports.
    fn egresses(&self, s: &SwitchId) -> Vec<Egress> {
        let mut seen = BTreeSet::new();
        let mut out = vec![];
        for (i, p) in self.ports.iter().enumerate().filter(|(_, p)| &p.def.switch == s) {
            match &p.def.cfg.lacp_group {
                Some(g) => {
                    if seen.insert(g.clone()) {
                        out.push(Egress::Lag(g.clone()));
                    }
                }
                None => out.push(Egress::Port(i)),
            }
        }
        out
    }

    /// The port an egress sends on: itself, or the LAG member a flow hashes to.
    fn pick(&self, s: &SwitchId, e: &Egress, f: &[u8], h: &Header, now: Instant) -> Option<PortId> {
        match e {
            Egress::Port(p) => Some(*p),
            Egress::Lag(g) => {
                let members: Vec<PortId> = self
                    .ports
                    .iter()
                    .enumerate()
                    .filter(|(i, p)| &p.def.switch == s && p.def.cfg.lacp_group.as_ref() == Some(g) && self.lag_member_ready(*i, now))
                    .map(|(i, _)| i)
                    .collect();
                (!members.is_empty()).then(|| members[(frame::flow_hash(f, h) % members.len() as u64) as usize])
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn leave(&mut self, q: PortId, f: &[u8], h: &Header, vlan: u16, now: Instant, out: &mut Vec<Out>, depth: u8) {
        if !self.link_up(q) {
            return;
        }
        let p = &self.ports[q];
        let Some(tag) = egress_tag(&p.def.cfg, vlan) else { return };
        if h.payload_len > usize::from(p.def.cfg.mtu) {
            self.ports[q].counters.drop_mtu += 1;
            return;
        }
        if p.cut {
            self.ports[q].counters.drop_partition += 1;
            return;
        }
        let speed = p.speed();
        let wire = frame::retag(f, h, tag);
        let p = &mut self.ports[q];
        if !p.tx.take(wire.len() as f64 * 8.0, speed, now) {
            p.counters.drop_rate += 1;
            return;
        }
        p.counters.tx_frames += 1;
        p.counters.tx_bytes += wire.len() as u64;
        match p.def.peer.clone() {
            Peer::Switch { switch, port } if depth < 8 => {
                if let Some(far) = self.port_id(&switch, &port) {
                    self.enter(far, &wire, now, out, depth + 1);
                }
            }
            Peer::Switch { .. } | Peer::None => {}
            Peer::Host { .. } | Peer::Router => out.push((q, wire)),
        }
    }

    /// Frames the switches send on their own: LACPDUs on LAG members every
    /// second and LLDP on every live external port every 5 s.
    pub fn tick(&mut self, now: Instant) -> Vec<Out> {
        let mut out = vec![];
        for i in 0..self.ports.len() {
            if !self.passes(i) || matches!(self.ports[i].def.peer, Peer::Switch { .. } | Peer::None) {
                continue;
            }
            let sw = &self.switches[&self.ports[i].def.switch];
            let system = [0x02, 0x5e, sw.index, 0, 0, 0];
            let p = &mut self.ports[i];
            if let Some(group) = p.def.cfg.lacp_group.clone()
                && p.last_lacp.is_none_or(|t| now.duration_since(t) >= LACP_EVERY)
            {
                p.last_lacp = Some(now);
                let heard = p.partner.filter(|(_, at)| now.duration_since(*at) < LACP_EXPIRE);
                let mut state = frame::LACP_ACTIVITY | frame::LACP_TIMEOUT | frame::LACP_AGGREGATION;
                if heard.is_some() {
                    state |= frame::LACP_SYNC | frame::LACP_COLLECTING | frame::LACP_DISTRIBUTING;
                } else {
                    state |= frame::LACP_DEFAULTED;
                }
                let key = group.bytes().fold(0u16, |k, b| k.wrapping_mul(31).wrapping_add(u16::from(b))) | 1;
                let actor = LacpInfo { system_priority: 32768, system, key, port_priority: 32768, port: (i + 1) as u16, state };
                let partner = heard.map(|(info, _)| info).unwrap_or_default();
                out.push((i, frame::lacpdu(p.mac, &actor, &partner)));
            }
            if p.last_lldp.is_none_or(|t| now.duration_since(t) >= LLDP_EVERY) {
                p.last_lldp = Some(now);
                out.push((i, frame::lldp(p.mac, p.def.switch.as_str(), p.def.name.as_str())));
            }
        }
        out
    }
}

/// The VLAN an arriving frame belongs to, if the port accepts it.
fn classify(cfg: &SwitchPortCfg, h: &Header) -> Option<u16> {
    let vlan = match (&cfg.mode, h.vlan) {
        (PortMode::Access(v), None) => *v,
        (PortMode::Trunk { native, .. }, None) => (*native)?,
        (PortMode::Access(_), Some(_)) => return None,
        (PortMode::Trunk { .. }, Some(v)) => v,
    };
    cfg.accepts(vlan, h.vlan.is_some()).then_some(vlan)
}

/// How a frame on `vlan` leaves a port: `Some(None)` untagged,
/// `Some(Some(v))` tagged, `None` not at all (the port does not carry it).
fn egress_tag(cfg: &SwitchPortCfg, vlan: u16) -> Option<Option<u16>> {
    match &cfg.mode {
        PortMode::Access(v) => (*v == vlan).then_some(None),
        PortMode::Trunk { native, allowed } => {
            if *native == Some(vlan) {
                Some(None)
            } else {
                allowed.contains(&vlan).then_some(Some(vlan))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switch::frame::{lacpdu, parse, ETH_P_8021Q, LACP_SYNC};

    fn frame(dst: Mac, src: Mac, vlan: Option<u16>, payload: usize) -> Vec<u8> {
        let mut f = vec![];
        f.extend_from_slice(&dst);
        f.extend_from_slice(&src);
        if let Some(v) = vlan {
            f.extend_from_slice(&ETH_P_8021Q.to_be_bytes());
            f.extend_from_slice(&v.to_be_bytes());
        }
        f.extend_from_slice(&0x0800u16.to_be_bytes());
        f.extend(std::iter::repeat_n(0, payload));
        f
    }

    const A: Mac = [0x52, 0x54, 0, 0, 0, 0xa];
    const B: Mac = [0x52, 0x54, 0, 0, 0, 0xb];
    const C: Mac = [0x52, 0x54, 0, 0, 0, 0xc];
    const BCAST: Mac = [0xff; 6];

    fn access(vlan: u16, mtu: u16) -> SwitchPortCfg {
        SwitchPortCfg { mode: PortMode::Access(vlan), mtu, lacp_group: None, admin_up: true }
    }

    fn trunk(native: Option<u16>, allowed: &[u16], mtu: u16) -> SwitchPortCfg {
        SwitchPortCfg { mode: PortMode::Trunk { native, allowed: allowed.iter().copied().collect() }, mtu, lacp_group: None, admin_up: true }
    }

    fn host(h: &str, nic: &str) -> Peer {
        Peer::Host { host: HostId::from(h), nic: nic.into(), nic_mbps: 10_000 }
    }

    fn port(s: &str, p: &str, cfg: SwitchPortCfg, peer: Peer) -> PortDef {
        PortDef { switch: SwitchId::from(s), name: PortName::from(p), speed_mbps: 10_000, cfg, peer }
    }

    /// `sw1` with hosts h1 (p1, access 1), h2 (p2, trunk native 1 + 20), a
    /// trunk p9 to `sw2`, whose p1 is h3 (trunk native 1 + 20) and p8 the router.
    fn lab(now: Instant) -> Core {
        let up = |s: &str, p: &str| Peer::Switch { switch: SwitchId::from(s), port: PortName::from(p) };
        let ports = vec![
            port("sw1", "p1", access(1, 1500), host("h1", "eth0")),
            port("sw1", "p2", trunk(Some(1), &[20], 9000), host("h2", "eth0")),
            port("sw1", "p9", trunk(Some(1), &[20], 9000), up("sw2", "p9")),
            port("sw2", "p9", trunk(Some(1), &[20], 9000), up("sw1", "p9")),
            port("sw2", "p1", trunk(Some(1), &[20], 9000), host("h3", "eth0")),
            port("sw2", "p8", trunk(Some(1), &[20], 9000), Peer::Router),
        ];
        let mut c = Core::new(&[(SwitchId::from("sw1"), "m".into()), (SwitchId::from("sw2"), "m".into())], ports, now);
        for i in [0, 1, 4] {
            c.set_peer_up(i, true);
        }
        c
    }

    fn ports_of(out: &[Out]) -> Vec<PortId> {
        let mut v: Vec<PortId> = out.iter().map(|(p, _)| *p).collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn untagged_frames_flood_on_their_vlan_then_learned_addresses_go_direct() {
        let now = Instant::now();
        let mut c = lab(now);
        let out = c.ingress(0, &frame(BCAST, A, None, 100), now);
        assert_eq!(ports_of(&out), [1, 4, 5], "flooded across the trunk to every port on VLAN 1");
        assert!(out.iter().all(|(_, f)| parse(f).unwrap().vlan.is_none()), "VLAN 1 is native everywhere: untagged");
        let out = c.ingress(4, &frame(A, C, None, 100), now);
        assert_eq!(ports_of(&out), [0], "A was learned on sw1 p1, via the trunk");
    }

    #[test]
    fn tagged_frames_stay_on_ports_that_carry_their_vlan() {
        let now = Instant::now();
        let mut c = lab(now);
        let out = c.ingress(1, &frame(BCAST, B, Some(20), 100), now);
        assert_eq!(ports_of(&out), [4, 5], "not to h1's access port");
        assert!(out.iter().all(|(_, f)| parse(f).unwrap().vlan == Some(20)));
        let dropped = c.ingress(0, &frame(BCAST, A, Some(20), 100), now);
        assert!(dropped.is_empty(), "an access port drops tagged frames");
        assert_eq!(c.counters(0).drop_vlan, 1);
        let unknown = c.ingress(1, &frame(BCAST, B, Some(30), 100), now);
        assert!(unknown.is_empty(), "VLAN 30 is not allowed on the trunk");
    }

    #[test]
    fn removing_a_vlan_from_a_port_stops_its_traffic_both_ways() {
        let now = Instant::now();
        let mut c = lab(now);
        c.set_cfg(4, trunk(Some(1), &[], 9000));
        assert_eq!(ports_of(&c.ingress(1, &frame(BCAST, B, Some(20), 100), now)), [5], "h3 no longer gets VLAN 20");
        assert!(c.ingress(4, &frame(BCAST, C, Some(20), 100), now).is_empty(), "nor can it send on it");
        assert_eq!(ports_of(&c.ingress(4, &frame(BCAST, C, None, 100), now)), [0, 1, 5], "mgmt still works");
    }

    #[test]
    fn oversized_frames_are_dropped_at_either_end() {
        let now = Instant::now();
        let mut c = lab(now);
        assert!(c.ingress(0, &frame(BCAST, A, None, 1501), now).is_empty());
        assert_eq!(c.counters(0).drop_mtu, 1);
        let out = c.ingress(1, &frame(BCAST, B, None, 9000), now);
        assert_eq!(ports_of(&out), [4, 5], "a jumbo frame does not fit h1's 1500 port");
        assert_eq!(c.counters(0).drop_mtu, 2);
    }

    #[test]
    fn a_dead_switch_or_unplugged_port_carries_nothing() {
        let now = Instant::now();
        let mut c = lab(now);
        c.set_switch_up(&SwitchId::from("sw2"), false);
        assert_eq!(ports_of(&c.ingress(0, &frame(BCAST, A, None, 64), now)), [1]);
        assert!(!c.link_up(2) && !c.link_up(4));
        c.set_switch_up(&SwitchId::from("sw2"), true);
        c.set_peer_up(1, false);
        assert_eq!(ports_of(&c.ingress(0, &frame(BCAST, A, None, 64), now)), [4, 5]);
        let mut down = access(1, 1500);
        down.admin_up = false;
        c.set_cfg(0, down);
        assert!(c.ingress(0, &frame(BCAST, A, None, 64), now).is_empty(), "admin down");
    }

    #[test]
    fn partitions_cut_crossing_cables_but_leave_links_up() {
        let now = Instant::now();
        let mut c = lab(now);
        c.partition([Member::Host(HostId::from("h3"))].into());
        assert!(c.link_up(4), "R22: the link stays up");
        assert_eq!(ports_of(&c.ingress(0, &frame(BCAST, A, None, 64), now)), [1, 5], "h3 is cut off");
        assert!(c.ingress(4, &frame(BCAST, C, None, 64), now).is_empty());
        assert!(c.reachable_from_router(&SwitchId::from("sw1")));
        c.heal();
        c.partition([Member::Switch(SwitchId::from("sw1"))].into());
        assert_eq!(ports_of(&c.ingress(0, &frame(BCAST, A, None, 64), now)), [1], "h1 and h2 follow sw1 inside");
        assert!(!c.reachable_from_router(&SwitchId::from("sw1")), "sw1's management is beyond the cut");
        c.heal();
        assert_eq!(ports_of(&c.ingress(0, &frame(BCAST, A, None, 64), now)), [1, 4, 5]);
    }

    #[test]
    fn a_degraded_port_polices_to_its_speed() {
        let now = Instant::now();
        let mut c = lab(now);
        c.degrade(0, Some(10));
        let later = now + Duration::from_secs(1);
        let sent = (0..2000).filter(|_| !c.ingress(0, &frame(BCAST, A, None, 1400), later).is_empty()).count();
        assert!(sent < 2000 && c.counters(0).drop_rate > 0, "10 Mbit/s cannot carry 2000 × 1400 B at once ({sent})");
    }

    /// A LAG of two ports to one host: nothing crosses until the host's
    /// LACP is heard, then flows spread over the members, and a member that
    /// goes dark leaves the aggregate.
    #[test]
    fn lacp_gates_and_spreads_a_lag() {
        let now = Instant::now();
        let lag = |p: &str, nic: &str| {
            let mut cfg = trunk(None, &[20], 9000);
            cfg.lacp_group = Some("po1".into());
            port("sw", p, cfg, host("h", nic))
        };
        let ports = vec![lag("p1", "sfp0"), lag("p2", "sfp1"), port("sw", "p3", trunk(None, &[20], 9000), host("o", "eth0"))];
        let mut c = Core::new(&[(SwitchId::from("sw"), "m".into())], ports, now);
        for i in 0..3 {
            c.set_peer_up(i, true);
        }
        let to_lag = |c: &mut Core, src: Mac, t| c.ingress(2, &frame(BCAST, src, Some(20), 64), t);
        assert!(to_lag(&mut c, C, now).is_empty(), "no partner heard yet");
        let from_lag = |c: &mut Core, t| c.ingress(0, &frame(BCAST, A, Some(20), 64), t);
        assert!(from_lag(&mut c, now).is_empty(), "nor taken from it");
        assert_eq!(c.counters(0).drop_lacp, 1);
        let pdus = c.tick(now);
        assert_eq!(pdus.iter().filter(|(_, f)| parse(f).unwrap().dst == frame::SLOW_DST).count(), 2, "LACPDUs on both members");
        let host_says = LacpInfo { system: A, key: 1, port: 1, state: LACP_SYNC, ..LacpInfo::default() };
        for p in [0, 1] {
            assert!(c.ingress(p, &lacpdu(A, &host_says, &LacpInfo::default()), now).is_empty(), "consumed, not forwarded");
        }
        assert_eq!(from_lag(&mut c, now).iter().map(|(p, _)| *p).collect::<Vec<_>>(), [2], "a ready member forwards");
        let used: BTreeSet<PortId> = (0..64u8).flat_map(|i| to_lag(&mut c, [0x52, 0, 0, 0, 1, i], now)).map(|(p, _)| p).collect();
        assert_eq!(used, [0, 1].into(), "flows spread over both members");
        c.set_peer_up(1, false);
        let used: BTreeSet<PortId> = (0..64u8).flat_map(|i| to_lag(&mut c, [0x52, 0, 0, 0, 1, i], now)).map(|(p, _)| p).collect();
        assert_eq!(used, [0].into(), "the unplugged member left");
        let later = now + LACP_EXPIRE;
        assert!(to_lag(&mut c, C, later).is_empty(), "the partner fell silent");
    }

    #[test]
    fn the_switch_reports_its_ports_and_neighbours() {
        let now = Instant::now();
        let mut c = lab(now);
        c.set_peer_up(1, false);
        let r = c.report(&SwitchId::from("sw1"));
        let rows: Vec<_> = r.iter().map(|p| (p.port.as_str(), p.link_up, p.speed_mbps, p.neighbor.as_ref().map(|n| n.switch.to_string()))).collect();
        assert_eq!(rows, [("p1", true, 10_000, None), ("p2", false, 0, None), ("p9", true, 10_000, Some("sw2".into()))]);
        let lldp: Vec<PortId> = c.tick(now).into_iter().filter(|(_, f)| parse(f).unwrap().dst == frame::LLDP_DST).map(|(p, _)| p).collect();
        assert_eq!(lldp, [0, 4, 5], "LLDP on every live external port");
    }
}
