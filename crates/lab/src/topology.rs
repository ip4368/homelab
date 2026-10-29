//! The lab as built: its switches, its VMs and how they are cabled, and the
//! addresses the homelab network around them uses. Mirrors what a person
//! would set up: the switch ports are configured the way a technician would
//! for each machine's template, before the machine is plugged in.

use crate::switch::core::{Peer, PortDef};
use model::network::{PortMode, SwitchPortCfg, MGMT_RESERVED};
use model::{HostId, PortName, ProfileId, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use ts_rs::TS;

pub const ROUTER_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
pub const MGMT_NET: &str = "10.0.0.0/22";
pub const VIP: Ipv4Addr = Ipv4Addr::new(10, 0, 3, 253);
/// The bootstrap DHCP pool, inside the mgmt network's reserved block.
pub const DHCP_FIRST: Ipv4Addr = MGMT_RESERVED.first;
pub const DHCP_LAST: Ipv4Addr = Ipv4Addr::new(10, 0, 3, 199);
/// Where the lab serves the `homelabd` binary to guests.
pub const BIN_PORT: u16 = 7480;
/// The tap the lab host joins the lab network through: the router port.
pub const ROUTER_TAP: &str = "lab-router";

pub const MGMT_VLAN: u16 = 1;
pub const SWITCH_MTU: u16 = 9216;
/// The VLANs the router port carries: mgmt native, the VM networks tagged.
pub const ROUTER_VLANS: [u16; 2] = [100, 110];
/// What a mini PC's port carries: mgmt untagged, storage tagged.
pub const MINIPC_VLANS: [u16; 1] = [20];
/// What an MS-01's SFP+ bond carries tagged: storage, migration and the VM networks.
pub const MS01_BOND_VLANS: [u16; 4] = [20, 30, 100, 110];
/// Every network's VLAN: what a switch-to-switch trunk carries tagged.
pub const ALL_VLANS: [u16; 4] = [20, 30, 100, 110];
/// The router's address on each network it carries (the `.1` of its subnet).
pub const ROUTER_VLAN_IPS: [(u16, &str); 2] = [(100, "10.100.0.1/22"), (110, "10.110.0.1/24")];

pub const CORE: &str = "core-1";
pub const ACCESS: &str = "access-1";
const CORE_PORTS: u32 = 16;
const ACCESS_PORTS: u32 = 16;
/// The trunk between the switches: core-1 p16 ↔ access-1 x1.
const CORE_UPLINK: &str = "p16";
const ACCESS_UPLINK: &str = "x1";
/// The router (the lab host) on access-1.
const ACCESS_ROUTER: &str = "x2";

/// A mistake the technician makes on a new machine's switch ports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Mistake {
    /// VLAN 20 (storage) left out of the trunk.
    MissingStorageVlan,
    /// The port MTU left at 1500 where jumbo frames are needed.
    MtuTooSmall,
    /// The bond's ports get no LACP group.
    NoLacp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchSpec {
    pub id: SwitchId,
    pub model: String,
    pub mgmt_ip: Ipv4Addr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NicSpec {
    pub name: String,
    pub mac: [u8; 6],
    pub speed_mbps: u32,
    /// The switch port its cable goes to; `None` if uncabled.
    pub cable: Option<(SwitchId, PortName)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VmSpec {
    pub id: HostId,
    pub profile: ProfileId,
    /// Lab-wide index, used in MACs and file names.
    pub index: u8,
    pub vcpus: u32,
    pub ram_mb: u32,
    pub nics: Vec<NicSpec>,
    /// Data disks: name (its serial) and size.
    pub disks: Vec<(String, u32)>,
}

impl VmSpec {
    pub fn mac_str(mac: &[u8; 6]) -> String {
        mac.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":")
    }
}

/// The lab's hardware and cabling.
#[derive(Debug, Clone)]
pub struct LabSpec {
    pub switches: Vec<SwitchSpec>,
    pub vms: Vec<VmSpec>,
    /// Every switch port with its initial configuration and what it is cabled to.
    pub ports: Vec<PortDef>,
}

fn port(i: u32) -> PortName {
    PortName::new(format!("p{i}"))
}

fn sw(id: &str) -> SwitchId {
    SwitchId::from(id)
}

fn trunk(native: Option<u16>, allowed: &[u16]) -> PortMode {
    PortMode::Trunk { native, allowed: allowed.iter().copied().collect::<BTreeSet<u16>>() }
}

/// An unused port: access on mgmt, admin up.
pub fn spare_port_cfg() -> SwitchPortCfg {
    SwitchPortCfg { mode: PortMode::Access(MGMT_VLAN), mtu: SWITCH_MTU, lacp_group: None, admin_up: true }
}

/// How a technician configures the port `nic` of a new `profile` machine
/// lands on, to suit its seed template (with one core instead of an MLAG
/// pair), optionally making `mistake`.
pub fn technician_cfg(profile: ProfileId, host: &HostId, nic: &str, mistake: Option<Mistake>) -> SwitchPortCfg {
    let mut cfg = match (profile, nic) {
        (ProfileId::MiniPc, _) => SwitchPortCfg { mode: trunk(Some(MGMT_VLAN), &MINIPC_VLANS), ..spare_port_cfg() },
        (ProfileId::Ms01, n) if n.starts_with("sfp") => {
            SwitchPortCfg { mode: trunk(None, &MS01_BOND_VLANS), lacp_group: Some(host.to_string()), ..spare_port_cfg() }
        }
        _ => spare_port_cfg(),
    };
    let carries_storage = matches!(&cfg.mode, PortMode::Trunk { allowed, .. } if allowed.contains(&20));
    match mistake {
        Some(Mistake::MissingStorageVlan) if carries_storage => {
            if let PortMode::Trunk { allowed, .. } = &mut cfg.mode {
                allowed.remove(&20);
            }
        }
        Some(Mistake::MtuTooSmall) if carries_storage => cfg.mtu = 1500,
        Some(Mistake::NoLacp) => cfg.lacp_group = None,
        _ => {}
    }
    cfg
}

/// The lab's two switches and their ports, nothing cabled to hosts yet.
fn switches_and_ports() -> (Vec<SwitchSpec>, Vec<PortDef>) {
    let switches = vec![
        SwitchSpec { id: sw(CORE), model: "lab-core-16x10g".into(), mgmt_ip: Ipv4Addr::new(10, 0, 3, 251) },
        SwitchSpec { id: sw(ACCESS), model: "lab-access-16x2.5g".into(), mgmt_ip: Ipv4Addr::new(10, 0, 3, 252) },
    ];
    let def = |s: &str, p: PortName, speed_mbps, cfg, peer| PortDef { switch: sw(s), name: p, speed_mbps, cfg, peer };
    let uplink = SwitchPortCfg { mode: trunk(Some(MGMT_VLAN), &ALL_VLANS), ..spare_port_cfg() };
    let mut ports = vec![];
    for i in 1..CORE_PORTS {
        ports.push(def(CORE, port(i), 10_000, spare_port_cfg(), Peer::None));
    }
    let to = |s: &str, p: &str| Peer::Switch { switch: sw(s), port: PortName::from(p) };
    ports.push(def(CORE, PortName::from(CORE_UPLINK), 10_000, uplink.clone(), to(ACCESS, ACCESS_UPLINK)));
    for i in 1..=ACCESS_PORTS {
        ports.push(def(ACCESS, port(i), 2_500, spare_port_cfg(), Peer::None));
    }
    ports.push(def(ACCESS, PortName::from(ACCESS_UPLINK), 10_000, uplink, to(CORE, CORE_UPLINK)));
    let router = SwitchPortCfg { mode: trunk(Some(MGMT_VLAN), &ROUTER_VLANS), ..spare_port_cfg() };
    ports.push(def(ACCESS, PortName::from(ACCESS_ROUTER), 10_000, router, Peer::Router));
    (switches, ports)
}

impl LabSpec {
    /// The lab of the validation: three MS-01s and a mini PC.
    pub fn default_lab() -> Self {
        let (switches, ports) = switches_and_ports();
        let mut lab = Self { switches, vms: vec![], ports };
        for p in [ProfileId::Ms01, ProfileId::Ms01, ProfileId::Ms01, ProfileId::MiniPc] {
            lab.add_vm(p, None).expect("the default lab fits its switches");
        }
        lab
    }

    pub fn vm(&self, id: &HostId) -> Option<&VmSpec> {
        self.vms.iter().find(|v| &v.id == id)
    }

    fn free(&self, s: &str, i: u32) -> bool {
        self.ports.iter().any(|p| p.switch == sw(s) && p.name == port(i) && p.peer == Peer::None)
    }

    fn cable(&mut self, s: &str, i: u32, host: &HostId, nic: &NicSpec, cfg: SwitchPortCfg) {
        let p = self.ports.iter_mut().find(|p| p.switch == sw(s) && p.name == port(i)).expect("port exists");
        p.peer = Peer::Host { host: host.clone(), nic: nic.name.clone(), nic_mbps: nic.speed_mbps };
        p.cfg = cfg;
    }

    /// Racks another machine of `profile`: names it, cables it to the next
    /// free ports (an MS-01's SFP+ pair to core-1, every `eth0` to access-1)
    /// and has the technician configure those ports. Fails if no room.
    pub fn add_vm(&mut self, profile: ProfileId, mistake: Option<Mistake>) -> Result<VmSpec, String> {
        let (prefix, vcpus, ram_mb) = match profile {
            ProfileId::Ms01 => ("ms01", 8, 16 * 1024),
            ProfileId::MiniPc => ("minipc", 4, 8 * 1024),
            other => return Err(format!("the lab only builds x86 machines, not {other:?}")),
        };
        let n = (1..).find(|n| !self.vms.iter().any(|v| v.id.as_str() == format!("{prefix}-{n:02}"))).expect("a free name");
        let id = HostId::new(format!("{prefix}-{n:02}"));
        let index = self.vms.iter().map(|v| v.index + 1).max().unwrap_or(1);
        let mac = |nic: u8| [0x52, 0x54, 0x00, 0x1a, index, nic];
        let catalogue = model::profile(profile, 0);
        let mut nics: Vec<NicSpec> = catalogue
            .nics
            .iter()
            .enumerate()
            .map(|(i, n)| NicSpec { name: n.name.clone(), mac: mac(i as u8 + 1), speed_mbps: n.speed_mbps, cable: None })
            .collect();
        let access = (1..=ACCESS_PORTS).find(|&i| self.free(ACCESS, i)).ok_or("access-1 is full")?;
        let core = (1..CORE_PORTS - 1).step_by(2).find(|&i| self.free(CORE, i) && self.free(CORE, i + 1));
        for nic in &mut nics {
            let target = match nic.name.as_str() {
                "eth0" => Some((ACCESS, access)),
                "sfp0" => Some((CORE, core.ok_or("core-1 is full")?)),
                "sfp1" => Some((CORE, core.ok_or("core-1 is full")? + 1)),
                _ => None,
            };
            if let Some((s, i)) = target {
                nic.cable = Some((sw(s), port(i)));
            }
        }
        for nic in &nics {
            if let Some((s, p)) = &nic.cable {
                let i: u32 = p.as_str()[1..].parse().expect("pN");
                self.cable(s.as_str(), i, &id, nic, technician_cfg(profile, &id, &nic.name, mistake));
            }
        }
        let disks = match profile {
            ProfileId::Ms01 => vec![("nvme0".into(), 64), ("nvme1".into(), 64)],
            _ => vec![("nvme0".into(), 64)],
        };
        let vm = VmSpec { id, profile, index, vcpus, ram_mb, nics, disks };
        self.vms.push(vm.clone());
        Ok(vm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_lab_cables_three_ms01s_and_a_mini_pc() {
        let lab = LabSpec::default_lab();
        let ids: Vec<&str> = lab.vms.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, ["ms01-01", "ms01-02", "ms01-03", "minipc-01"]);
        let ms2 = lab.vm(&HostId::from("ms01-02")).unwrap();
        let cables: Vec<_> = ms2.nics.iter().map(|n| (n.name.as_str(), n.cable.as_ref().map(|(s, p)| format!("{s}/{p}")))).collect();
        assert_eq!(
            cables,
            [("sfp0", Some("core-1/p3".into())), ("sfp1", Some("core-1/p4".into())), ("eth0", Some("access-1/p2".into())), ("eth1", None)]
        );
        assert_eq!((ms2.vcpus, ms2.ram_mb, ms2.disks.len()), (8, 16384, 2));
        let p3 = lab.ports.iter().find(|p| p.switch == sw(CORE) && p.name == port(3)).unwrap();
        assert_eq!(p3.cfg.lacp_group.as_deref(), Some("ms01-02"));
        assert_eq!(p3.cfg.mode, trunk(None, &MS01_BOND_VLANS));
        let mini = lab.ports.iter().find(|p| p.switch == sw(ACCESS) && p.name == port(4)).unwrap();
        assert_eq!(mini.cfg.mode, trunk(Some(1), &[20]));
        assert!(matches!(&mini.peer, Peer::Host { host, .. } if host.as_str() == "minipc-01"));
        let macs: BTreeSet<_> = lab.vms.iter().flat_map(|v| v.nics.iter().map(|n| n.mac)).collect();
        assert_eq!(macs.len(), 13, "every NIC has its own MAC");
    }

    #[test]
    fn technician_mistakes_land_on_the_ports_they_concern() {
        let h = HostId::from("ms01-04");
        let missing = technician_cfg(ProfileId::Ms01, &h, "sfp0", Some(Mistake::MissingStorageVlan));
        assert_eq!(missing.mode, trunk(None, &[30, 100, 110]));
        assert_eq!(technician_cfg(ProfileId::Ms01, &h, "eth0", Some(Mistake::MtuTooSmall)).mtu, SWITCH_MTU, "mgmt carries no storage");
        assert_eq!(technician_cfg(ProfileId::Ms01, &h, "sfp1", Some(Mistake::MtuTooSmall)).mtu, 1500);
        assert_eq!(technician_cfg(ProfileId::Ms01, &h, "sfp1", Some(Mistake::NoLacp)).lacp_group, None);
        let mut lab = LabSpec::default_lab();
        let vm = lab.add_vm(ProfileId::Ms01, Some(Mistake::MissingStorageVlan)).unwrap();
        assert_eq!(vm.id.as_str(), "ms01-04");
        assert_eq!(vm.nics[0].cable, Some((sw(CORE), port(7))));
    }
}
