//! Logical networks, host network configuration and switch port
//! configuration. A host's [`HostNetworkConfig`] says which links it builds
//! from its NICs and which networks ride on them, tagged or untagged.

use crate::hardware::HardwareProfile;
use crate::ids::{NetworkId, PortName, SwitchId};
use ipnet::Ipv4Net;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum NetworkPurpose {
    Mgmt,
    Storage,
    Migration,
    Vm,
}

/// An inclusive range of addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct IpRange {
    #[ts(type = "string")]
    pub first: Ipv4Addr,
    #[ts(type = "string")]
    pub last: Ipv4Addr,
}

impl IpRange {
    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        self.first <= ip && ip <= self.last
    }
}

impl LogicalNetwork {
    /// The first reserved range that is out of order or reaches outside the
    /// subnet, as a message naming it.
    pub fn check_reserved(&self) -> Result<(), String> {
        for r in &self.reserved {
            if r.first > r.last {
                return Err(format!("reserved range {}-{} of {} ends before it starts", r.first, r.last, self.id));
            }
            if !self.subnet.contains(&r.first) || !self.subnet.contains(&r.last) {
                return Err(format!("reserved range {}-{} is outside {}'s subnet {}", r.first, r.last, self.id, self.subnet));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LogicalNetwork {
    pub id: NetworkId,
    pub purpose: NetworkPurpose,
    pub vlan: u16,
    #[ts(type = "string")]
    pub subnet: Ipv4Net,
    pub mtu: u16,
    /// Addresses IPAM never hands out: those something outside the cluster
    /// uses (a DHCP pool for hosts that have not joined yet, switch
    /// management interfaces, the control-plane VIP).
    #[serde(default)]
    pub reserved: Vec<IpRange>,
    /// The DNS resolvers a host with a static address on this network uses
    /// (only the mgmt network's are configured).
    #[serde(default)]
    #[ts(type = "Array<string>")]
    pub dns: Vec<Ipv4Addr>,
}

/// The `mgmt` block kept out of IPAM: the bootstrap DHCP pool
/// (10.0.3.100–199), switch management interfaces and the VIP (above 200).
pub const MGMT_RESERVED: IpRange =
    IpRange { first: Ipv4Addr::new(10, 0, 3, 100), last: Ipv4Addr::new(10, 0, 3, 255) };

/// The `storage` block kept out of IPAM: the addresses Longhorn's pods take
/// on the storage network (its replication traffic runs there).
pub const STORAGE_RESERVED: IpRange =
    IpRange { first: Ipv4Addr::new(10, 20, 3, 0), last: Ipv4Addr::new(10, 20, 3, 255) };

/// The seed networks every cluster starts with (Global Constraints table).
pub fn seed_networks() -> Vec<LogicalNetwork> {
    let net = |id: &str, purpose, vlan, subnet: &str, mtu| LogicalNetwork {
        id: NetworkId::from(id),
        purpose,
        vlan,
        subnet: subnet.parse().expect("seed subnet is valid"),
        mtu,
        reserved: vec![],
        dns: vec![],
    };
    vec![
        LogicalNetwork { reserved: vec![MGMT_RESERVED], dns: vec![Ipv4Addr::new(10, 0, 0, 1)], ..net("mgmt", NetworkPurpose::Mgmt, 1, "10.0.0.0/22", 1500) },
        LogicalNetwork { reserved: vec![STORAGE_RESERVED], ..net("storage", NetworkPurpose::Storage, 20, "10.20.0.0/22", 9000) },
        net("migration", NetworkPurpose::Migration, 30, "10.30.0.0/22", 9000),
        net("vm-lan", NetworkPurpose::Vm, 100, "10.100.0.0/22", 1500),
        net("vm-iot", NetworkPurpose::Vm, 110, "10.110.0.0/24", 1500),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum BondMode {
    Lacp,
    ActiveBackup,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum LinkKind {
    Single(PortName),
    Bond { members: Vec<PortName>, mode: BondMode },
}

impl LinkKind {
    /// The physical ports this link sends frames out of.
    pub fn ports(&self) -> &[PortName] {
        match self {
            LinkKind::Single(p) => std::slice::from_ref(p),
            LinkKind::Bond { members, .. } => members,
        }
    }
}

/// A host-side L2 link: one NIC, or a bond of several.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Link {
    pub name: String,
    pub kind: LinkKind,
    pub mtu: u16,
}

/// A logical network carried on a link. Tagged attachments send 802.1Q
/// frames with the network's VLAN; untagged ones rely on the switch port's
/// access or native VLAN.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Attachment {
    pub network: NetworkId,
    pub link: String,
    pub tagged: bool,
    pub ip: Option<Ipv4Addr>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostNetworkConfig {
    pub links: Vec<Link>,
    pub attachments: Vec<Attachment>,
}

impl HostNetworkConfig {
    pub fn link(&self, name: &str) -> Option<&Link> {
        self.links.iter().find(|l| l.name == name)
    }
}

/// The config a fresh host boots with: `mgmt` untagged on `eth0` at MTU 1500,
/// address from DHCP. Every catalogue profile has an `eth0`; a profile
/// without one falls back to its first NIC.
pub fn bootstrap_config(p: &HardwareProfile) -> HostNetworkConfig {
    let nic = p.nics.iter().find(|n| n.name == "eth0").or(p.nics.first()).expect("profile has a NIC");
    HostNetworkConfig {
        links: vec![Link { name: nic.name.clone(), kind: LinkKind::Single(PortName::new(&nic.name)), mtu: 1500 }],
        attachments: vec![Attachment {
            network: NetworkId::from("mgmt"),
            link: nic.name.clone(),
            tagged: false,
            ip: None,
        }],
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum PortMode {
    /// Untagged frames join this VLAN; tagged frames are dropped.
    Access(u16),
    /// Tagged frames are accepted for VLANs in `allowed`; untagged frames
    /// join `native` (dropped when `None`).
    Trunk { native: Option<u16>, allowed: BTreeSet<u16> },
}

/// Configuration of one switch port, as the switch reports it when polled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SwitchPortCfg {
    pub mode: PortMode,
    pub mtu: u16,
    pub lacp_group: Option<String>,
    pub admin_up: bool,
}

impl SwitchPortCfg {
    /// Whether a frame entering this port lands on `vlan`: a tagged frame
    /// needs a trunk allowing `vlan`; an untagged one takes the access or
    /// native VLAN, which must equal `vlan`. Ignores `admin_up` and MTU.
    pub fn accepts(&self, vlan: u16, tagged: bool) -> bool {
        match (&self.mode, tagged) {
            (PortMode::Access(v), false) => *v == vlan,
            (PortMode::Access(_), true) => false,
            (PortMode::Trunk { native, .. }, false) => *native == Some(vlan),
            (PortMode::Trunk { allowed, .. }, true) => allowed.contains(&vlan),
        }
    }
}

/// What a host's LLDP daemon reports for one of its ports: the switch port
/// on the other end of the cable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LldpNeighbor {
    pub local_port: PortName,
    pub switch: SwitchId,
    pub switch_port: PortName,
    /// The switch's management address, from LLDP's Management Address TLV
    /// (if the switch sends one): where the control plane polls it.
    #[serde(default)]
    #[ts(type = "string | null")]
    pub mgmt_ip: Option<Ipv4Addr>,
}

/// A switch port's configuration as the controller observed it by polling.
pub type SwitchPortCfgView = SwitchPortCfg;

/// What a switch's LLDP daemon reports for one of its ports: the port of
/// another switch on the other end of the cable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LldpPeer {
    pub switch: SwitchId,
    pub port: PortName,
}

/// One polled switch port, with the MLAG peer of the switch it sits on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ObservedSwitchPort {
    pub switch: SwitchId,
    pub port: PortName,
    pub cfg: SwitchPortCfgView,
    pub mlag_peer: Option<SwitchId>,
    /// The port has carrier: cabled, cable plugged in, admin-up on both ends, far end alive.
    pub link_up: bool,
    /// The negotiated speed in Mbit/s: the slower of the two ends.
    pub speed_mbps: u32,
    /// The switch port on the other end of a switch-to-switch cable. `None`
    /// for host-facing and uncabled ports (hosts report their own LLDP).
    pub neighbor: Option<LldpPeer>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hardware::{ProfileId, profile};

    #[test]
    fn reserved_ranges_must_be_in_order_and_inside_the_subnet() {
        let mut mgmt = seed_networks().remove(0);
        assert_eq!(mgmt.check_reserved(), Ok(()));
        let range = |first: &str, last: &str| IpRange { first: first.parse().unwrap(), last: last.parse().unwrap() };
        mgmt.reserved = vec![range("10.0.0.0", "10.0.3.255")];
        assert_eq!(mgmt.check_reserved(), Ok(()), "the whole subnet, ends included");
        mgmt.reserved = vec![range("10.0.3.200", "10.0.3.100")];
        assert_eq!(mgmt.check_reserved(), Err("reserved range 10.0.3.200-10.0.3.100 of mgmt ends before it starts".into()));
        mgmt.reserved = vec![range("10.0.3.100", "10.0.4.10")];
        assert_eq!(mgmt.check_reserved(), Err("reserved range 10.0.3.100-10.0.4.10 is outside mgmt's subnet 10.0.0.0/22".into()));
        mgmt.reserved = vec![range("9.255.255.255", "10.0.0.5")];
        assert!(mgmt.check_reserved().is_err(), "starts before the subnet");
    }

    #[test]
    fn seed_networks_match_global_constraints() {
        let got: Vec<_> = seed_networks()
            .into_iter()
            .map(|n| (n.id.to_string(), n.purpose, n.vlan, n.subnet.to_string(), n.mtu))
            .collect();
        let want = [
            ("mgmt", NetworkPurpose::Mgmt, 1, "10.0.0.0/22", 1500),
            ("storage", NetworkPurpose::Storage, 20, "10.20.0.0/22", 9000),
            ("migration", NetworkPurpose::Migration, 30, "10.30.0.0/22", 9000),
            ("vm-lan", NetworkPurpose::Vm, 100, "10.100.0.0/22", 1500),
            ("vm-iot", NetworkPurpose::Vm, 110, "10.110.0.0/24", 1500),
        ]
        .map(|(id, p, v, s, m)| (id.to_string(), p, v, s.to_string(), m));
        assert_eq!(got, want);
    }

    #[test]
    fn bootstrap_is_untagged_mgmt_on_eth0() {
        for id in [ProfileId::Ms01, ProfileId::MiniPc, ProfileId::Pi5, ProfileId::Rk3588] {
            let cfg = bootstrap_config(&profile(id, 0));
            assert_eq!(
                cfg,
                HostNetworkConfig {
                    links: vec![Link {
                        name: "eth0".into(),
                        kind: LinkKind::Single(PortName::from("eth0")),
                        mtu: 1500
                    }],
                    attachments: vec![Attachment {
                        network: NetworkId::from("mgmt"),
                        link: "eth0".into(),
                        tagged: false,
                        ip: None
                    }],
                },
                "{id:?}"
            );
        }
    }

    #[test]
    fn port_acceptance_rules() {
        let cfg = |mode| SwitchPortCfg { mode, mtu: 1500, lacp_group: None, admin_up: true };
        let access = cfg(PortMode::Access(1));
        assert!(access.accepts(1, false));
        assert!(!access.accepts(1, true), "access ports drop tagged frames");
        assert!(!access.accepts(20, false));

        let trunk = cfg(PortMode::Trunk { native: Some(1), allowed: [20, 30].into() });
        assert!(trunk.accepts(1, false), "untagged joins the native VLAN");
        assert!(trunk.accepts(20, true));
        assert!(!trunk.accepts(100, true), "VLAN not in the allowed set");
        assert!(!trunk.accepts(20, false));

        let no_native = cfg(PortMode::Trunk { native: None, allowed: [1].into() });
        assert!(!no_native.accepts(1, false), "no native VLAN drops untagged frames");
        assert!(no_native.accepts(1, true));
    }

    #[test]
    fn network_serializes_subnet_as_string() {
        let json = serde_json::to_value(&seed_networks()[0]).unwrap();
        assert_eq!(json["subnet"], "10.0.0.0/22");
        assert_eq!(json["purpose"], "Mgmt");
        let back: LogicalNetwork = serde_json::from_value(json).unwrap();
        assert_eq!(back, seed_networks()[0]);
    }
}
