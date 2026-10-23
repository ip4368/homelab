//! Physical topology ground truth: switches, cables and partitions.
//!
//! Mutate through the methods here: each bumps [`Topology::version`], which
//! the fabric uses to invalidate its cached reachability graphs. (The fabric
//! also drops them whenever its world is borrowed mutably, so a direct field
//! edit cannot leave them stale.)

pub use model::network::{PortMode, SwitchPortCfg};
use model::{HostId, PortName, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Switch {
    pub id: SwitchId,
    pub model: String,
    /// Port name to (speed in Mbps, configuration).
    pub ports: BTreeMap<PortName, (u32, SwitchPortCfg)>,
    pub mlag_peer: Option<SwitchId>,
    pub up: bool,
}

impl Switch {
    pub fn port(&self, name: &PortName) -> Option<&SwitchPortCfg> {
        self.ports.get(name).map(|(_, cfg)| cfg)
    }
}

/// One end of a cable. Exported to the UI as `CableEnd` (the DOM owns `Node`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
#[ts(export, rename = "CableEnd")]
pub enum Node {
    HostPort(HostId, PortName),
    SwitchPort(SwitchId, PortName),
    /// The UI client's machine. Sends untagged frames.
    Client,
    /// The upstream router. Speaks tagged or untagged, whatever its port accepts.
    Router,
}

/// Normalised (lower, higher) pair identifying a cable regardless of direction.
pub type CableKey = (Node, Node);

pub fn cable_key(a: &Node, b: &Node) -> CableKey {
    if a <= b { (a.clone(), b.clone()) } else { (b.clone(), a.clone()) }
}

/// A member of a partition group. A host member puts all its ports inside.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum PartitionMember {
    Host(HostId),
    Switch(SwitchId),
}

/// Is cable end `end` (whose other end is `peer`) inside `group`? Ports of
/// listed switches and hosts are inside. An unlisted endpoint (host port,
/// client, router) sits on the side of the switch it is cabled to, so
/// partitioning a switch takes its attached hosts with it, while listing a
/// host cuts its legs to switches outside the group.
fn inside(group: &BTreeSet<PartitionMember>, end: &Node, peer: &Node) -> bool {
    let switch_in = |n: &Node| matches!(n, Node::SwitchPort(s, _) if group.contains(&PartitionMember::Switch(s.clone())));
    match end {
        Node::SwitchPort(..) => switch_in(end),
        Node::HostPort(h, _) => group.contains(&PartitionMember::Host(h.clone())) || switch_in(peer),
        Node::Client | Node::Router => switch_in(peer),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Topology {
    pub switches: BTreeMap<SwitchId, Switch>,
    /// Symmetric: every cable is stored in both directions.
    pub cables: BTreeMap<Node, Node>,
    /// Cables whose link is up (plugged in at both ends).
    pub cable_up: BTreeSet<CableKey>,
    /// Each group is cut off from everything outside it (see [`Topology::is_cut`]).
    pub partitions: Vec<BTreeSet<PartitionMember>>,
    pub version: u64,
}

impl Topology {
    pub fn new() -> Self {
        Self::default()
    }

    fn bump(&mut self) {
        self.version += 1;
    }

    pub fn add_switch(&mut self, sw: Switch) {
        self.switches.insert(sw.id.clone(), sw);
        self.bump();
    }

    pub fn set_switch_up(&mut self, id: &SwitchId, up: bool) {
        if let Some(sw) = self.switches.get_mut(id) {
            sw.up = up;
        }
        self.bump();
    }

    /// Replaces a port's configuration, keeping its speed. Unknown ports are ignored.
    pub fn set_port(&mut self, id: &SwitchId, port: &PortName, cfg: SwitchPortCfg) {
        if let Some((_, c)) = self.switches.get_mut(id).and_then(|sw| sw.ports.get_mut(port)) {
            *c = cfg;
        }
        self.bump();
    }

    /// Cables `a` to `b` with the link up, first removing any cable either end had.
    pub fn connect(&mut self, a: Node, b: Node) {
        self.disconnect(&a);
        self.disconnect(&b);
        self.cable_up.insert(cable_key(&a, &b));
        self.cables.insert(a.clone(), b.clone());
        self.cables.insert(b, a);
        self.bump();
    }

    /// Removes the cable at `end`, if any.
    pub fn disconnect(&mut self, end: &Node) {
        if let Some(other) = self.cables.remove(end) {
            self.cables.remove(&other);
            self.cable_up.remove(&cable_key(end, &other));
        }
        self.bump();
    }

    /// Unplugs (`false`) or replugs (`true`) the cable at `end`, keeping it in place.
    pub fn set_cable_up(&mut self, end: &Node, up: bool) {
        if let Some(other) = self.cables.get(end) {
            let key = cable_key(end, other);
            if up {
                self.cable_up.insert(key);
            } else {
                self.cable_up.remove(&key);
            }
        }
        self.bump();
    }

    pub fn partition(&mut self, group: BTreeSet<PartitionMember>) {
        self.partitions.push(group);
        self.bump();
    }

    pub fn heal_partitions(&mut self) {
        self.partitions.clear();
        self.bump();
    }

    /// The node at the other end of `end`'s cable.
    pub fn peer(&self, end: &Node) -> Option<&Node> {
        self.cables.get(end)
    }

    pub fn is_cable_up(&self, a: &Node, b: &Node) -> bool {
        self.cable_up.contains(&cable_key(a, b))
    }

    /// True when the cable `a`–`b` has exactly one end inside some partition
    /// group. Unlisted hosts follow the switch they are cabled to.
    pub fn is_cut(&self, a: &Node, b: &Node) -> bool {
        self.partitions.iter().any(|g| inside(g, a, b) != inside(g, b, a))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hp(h: &str, p: &str) -> Node {
        Node::HostPort(HostId::from(h), PortName::from(p))
    }
    fn sp(s: &str, p: &str) -> Node {
        Node::SwitchPort(SwitchId::from(s), PortName::from(p))
    }

    #[test]
    fn cables_are_symmetric_and_mutations_bump_version() {
        let mut t = Topology::new();
        t.connect(hp("h1", "eth0"), sp("sw", "p1"));
        assert_eq!(t.peer(&hp("h1", "eth0")), Some(&sp("sw", "p1")));
        assert_eq!(t.peer(&sp("sw", "p1")), Some(&hp("h1", "eth0")));
        assert!(t.is_cable_up(&sp("sw", "p1"), &hp("h1", "eth0")));

        let v = t.version;
        t.set_cable_up(&sp("sw", "p1"), false);
        assert!(t.version > v);
        assert!(!t.is_cable_up(&hp("h1", "eth0"), &sp("sw", "p1")));
        assert_eq!(t.peer(&hp("h1", "eth0")), Some(&sp("sw", "p1")), "unplugged cable stays in place");

        // Re-cabling the switch port moves it off h1.
        t.connect(hp("h2", "eth0"), sp("sw", "p1"));
        assert_eq!(t.peer(&hp("h1", "eth0")), None);
        assert_eq!(t.cables.len(), 2);
        assert_eq!(t.cable_up.len(), 1);
    }

    #[test]
    fn partition_cuts_only_cables_crossing_the_group() {
        let mut t = Topology::new();
        t.partition([PartitionMember::Switch(SwitchId::from("b")), PartitionMember::Host(HostId::from("h2"))].into());
        assert!(t.is_cut(&sp("a", "up"), &sp("b", "up")));
        assert!(!t.is_cut(&hp("h2", "eth0"), &sp("b", "p1")), "both ends inside");
        assert!(!t.is_cut(&hp("h1", "eth0"), &sp("a", "p1")), "both ends outside");
        assert!(t.is_cut(&hp("h2", "sfp0"), &sp("a", "p2")), "a host member brings all its ports inside");
        assert!(!t.is_cut(&hp("h3", "eth0"), &sp("b", "p3")), "an unlisted host follows its switch");
        assert!(!t.is_cut(&Node::Client, &sp("b", "p4")), "so does the client");
        assert!(!t.is_cut(&Node::Router, &sp("a", "p5")));
        t.heal_partitions();
        assert!(!t.is_cut(&sp("a", "up"), &sp("b", "up")));
    }
}
