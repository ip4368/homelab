//! Validation of a host's network configuration against what the controller
//! observes: LLDP neighbours and polled switch ports (design §6.3, §6.4).
//! The uplink MTU along the whole path is the fabric's business, not checked here.

use crate::ids::{HostId, NetworkId, PortName, SwitchId};
use crate::network::{
    BondMode, HostNetworkConfig, LinkKind, LldpNeighbor, LogicalNetwork, NetworkPurpose, ObservedSwitchPort, PortMode,
};
use crate::roles::Role;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use ts_rs::TS;

/// Stands for "at least one `vm-*` network" in [`required_networks`] and in
/// `MissingRequiredNetwork` when a VM host has no usable one.
pub const ANY_VM_NETWORK: &str = "vm-*";

/// A problem with a host's network setup. Every issue except
/// `MissingRequiredNetwork` names the networks it stops traffic on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Issue {
    /// The host has `role` but no (usable) attachment for `network`.
    MissingRequiredNetwork { role: Role, network: NetworkId },
    /// A tagged attachment's VLAN is not allowed on the switch port.
    VlanNotAllowed { network: NetworkId, host_port: PortName, switch: SwitchId, switch_port: PortName, vlan: u16 },
    /// An untagged attachment's VLAN is not the port's native or access VLAN (`native`).
    NativeVlanMismatch {
        network: NetworkId,
        host_port: PortName,
        switch: SwitchId,
        switch_port: PortName,
        vlan: u16,
        native: Option<u16>,
    },
    /// `where_` is `link <name>` or `<switch> <port>`; it has `have`, the network needs `need`.
    MtuTooSmall { network: NetworkId, where_: String, have: u16, need: u16 },
    /// An LACP bond's members sit on different switches that are not an MLAG pair.
    BondSplitWithoutMlag { link: String, networks: BTreeSet<NetworkId> },
    /// A bond member's switch port has no LACP group, or a different one from its siblings.
    LacpNotConfigured { link: String, switch: SwitchId, port: PortName, networks: BTreeSet<NetworkId> },
    /// The attachment's address is already held by `other_host`.
    IpConflict { network: NetworkId, ip: Ipv4Addr, other_host: HostId },
    /// No LLDP neighbour on this host port: nothing is plugged in. `networks` is
    /// empty when another member of the same bond is still cabled (degraded, not down).
    PortNotCabled { port: PortName, networks: BTreeSet<NetworkId> },
    /// An attachment names a network that does not exist.
    UnknownNetwork { id: NetworkId },
}

/// The networks `role` needs on a host: `mgmt` for every role, plus
/// `storage` for Storage, and `migration` and [`ANY_VM_NETWORK`] for Vm.
pub fn required_networks(role: Role) -> Vec<NetworkId> {
    let mut v = vec![NetworkId::from("mgmt")];
    match role {
        Role::Storage => v.push(NetworkId::from("storage")),
        Role::Vm => v.extend([NetworkId::from("migration"), NetworkId::from(ANY_VM_NETWORK)]),
        Role::ControlPlane | Role::Compute | Role::Transcode => {}
    }
    v
}

/// Checks `cfg` for a host holding `roles`. `lldp` is the host's own
/// neighbour table; `switch_ports` the polled ports (a port with no
/// observation is not judged); `ips_in_use` the addresses held by *other* hosts.
///
/// An attachment whose `link` the config does not define is ignored, and does
/// not count as attached for the role requirements.
pub fn validate_host(
    cfg: &HostNetworkConfig,
    roles: &BTreeSet<Role>,
    networks: &[LogicalNetwork],
    lldp: &[LldpNeighbor],
    switch_ports: &[ObservedSwitchPort],
    ips_in_use: &BTreeMap<Ipv4Addr, HostId>,
) -> Vec<Issue> {
    let mut issues = vec![];
    let neighbor = |p: &PortName| lldp.iter().find(|n| n.local_port == *p);
    let observed = |p: &PortName| {
        let n = neighbor(p)?;
        switch_ports.iter().find(|s| s.switch == n.switch && s.port == n.switch_port)
    };

    let mut unknown = BTreeSet::new();
    for a in &cfg.attachments {
        let Some(net) = networks.iter().find(|n| n.id == a.network) else {
            if unknown.insert(&a.network) {
                issues.push(Issue::UnknownNetwork { id: a.network.clone() });
            }
            continue;
        };
        let Some(link) = cfg.link(&a.link) else { continue };
        if link.mtu < net.mtu {
            issues.push(Issue::MtuTooSmall {
                network: net.id.clone(),
                where_: format!("link {}", link.name),
                have: link.mtu,
                need: net.mtu,
            });
        }
        if let Some((ip, other)) = a.ip.and_then(|ip| Some((ip, ips_in_use.get(&ip)?))) {
            issues.push(Issue::IpConflict { network: net.id.clone(), ip, other_host: other.clone() });
        }
        for host_port in link.kind.ports() {
            // Uncabled members are reported per link below; unobserved ports can't be judged.
            let Some(obs) = observed(host_port) else { continue };
            if !obs.cfg.accepts(net.vlan, a.tagged) {
                let (switch, switch_port, vlan) = (obs.switch.clone(), obs.port.clone(), net.vlan);
                issues.push(if a.tagged {
                    Issue::VlanNotAllowed { network: net.id.clone(), host_port: host_port.clone(), switch, switch_port, vlan }
                } else {
                    let native = match &obs.cfg.mode {
                        PortMode::Access(v) => Some(*v),
                        PortMode::Trunk { native, .. } => *native,
                    };
                    Issue::NativeVlanMismatch {
                        network: net.id.clone(),
                        host_port: host_port.clone(),
                        switch,
                        switch_port,
                        vlan,
                        native,
                    }
                });
            }
            if obs.cfg.mtu < net.mtu {
                issues.push(Issue::MtuTooSmall {
                    network: net.id.clone(),
                    where_: format!("{} {}", obs.switch, obs.port),
                    have: obs.cfg.mtu,
                    need: net.mtu,
                });
            }
        }
    }

    for link in &cfg.links {
        let on_link: BTreeSet<NetworkId> =
            cfg.attachments.iter().filter(|a| a.link == link.name).map(|a| a.network.clone()).collect();
        // A bond with one leg still cabled is degraded, not down: the port is
        // reported but the link's networks stay up. They break only when no
        // member is cabled.
        let any_cabled = link.kind.ports().iter().any(|p| neighbor(p).is_some());
        for port in link.kind.ports() {
            if neighbor(port).is_none() {
                let networks = if any_cabled { BTreeSet::new() } else { on_link.clone() };
                issues.push(Issue::PortNotCabled { port: port.clone(), networks });
            }
        }
        if let LinkKind::Bond { members, mode: BondMode::Lacp } = &link.kind {
            let seen: Vec<&ObservedSwitchPort> = members.iter().filter_map(&observed).collect();
            let switches: BTreeSet<&SwitchId> = seen.iter().map(|o| &o.switch).collect();
            let mlag_pair = switches.len() == 2
                && seen.iter().any(|o| o.mlag_peer.as_ref().is_some_and(|p| *p != o.switch && switches.contains(p)));
            if switches.len() > 2 || (switches.len() == 2 && !mlag_pair) {
                issues.push(Issue::BondSplitWithoutMlag { link: link.name.clone(), networks: on_link.clone() });
            }
            let reference = seen.iter().find_map(|o| lacp_group(o));
            for o in &seen {
                if reference.is_none() || lacp_group(o) != reference {
                    issues.push(Issue::LacpNotConfigured {
                        link: link.name.clone(),
                        switch: o.switch.clone(),
                        port: o.port.clone(),
                        networks: on_link.clone(),
                    });
                }
            }
        }
    }

    let broken = broken_networks(&issues);
    // An attachment on a link the config does not define carries nothing.
    let real: Vec<_> = cfg.attachments.iter().filter(|a| cfg.link(&a.link).is_some()).collect();
    let attached = |id: &NetworkId| real.iter().any(|a| a.network == *id);
    let usable_vm = real.iter().any(|a| {
        !broken.contains(&a.network)
            && networks.iter().any(|n| n.id == a.network && n.purpose == NetworkPurpose::Vm)
    });
    for &role in roles {
        for network in required_networks(role) {
            let met = if network.as_str() == ANY_VM_NETWORK { usable_vm } else { attached(&network) };
            if !met {
                issues.push(Issue::MissingRequiredNetwork { role, network });
            }
        }
    }
    issues
}

/// The port's LACP group, if it has a non-empty one.
fn lacp_group(o: &ObservedSwitchPort) -> Option<&str> {
    o.cfg.lacp_group.as_deref().filter(|g| !g.is_empty())
}

/// Networks an issue stops traffic on. `MissingRequiredNetwork` is not among
/// them: the network is fine, the host just is not on it.
pub fn broken_networks(issues: &[Issue]) -> BTreeSet<NetworkId> {
    let mut out = BTreeSet::new();
    for issue in issues {
        match issue {
            Issue::MissingRequiredNetwork { .. } => {}
            Issue::VlanNotAllowed { network, .. }
            | Issue::NativeVlanMismatch { network, .. }
            | Issue::MtuTooSmall { network, .. }
            | Issue::IpConflict { network, .. } => {
                out.insert(network.clone());
            }
            Issue::UnknownNetwork { id } => {
                out.insert(id.clone());
            }
            Issue::BondSplitWithoutMlag { networks, .. }
            | Issue::LacpNotConfigured { networks, .. }
            | Issue::PortNotCabled { networks, .. } => out.extend(networks.iter().cloned()),
        }
    }
    out
}

/// [`broken_networks`] plus the networks hosts are missing. This is what to
/// pass to [`effective_roles`] to also hold roles whose network is absent.
pub fn unavailable_networks(issues: &[Issue]) -> BTreeSet<NetworkId> {
    let mut out = broken_networks(issues);
    out.extend(issues.iter().filter_map(|i| match i {
        Issue::MissingRequiredNetwork { network, .. } => Some(network.clone()),
        _ => None,
    }));
    out
}

/// Splits `roles` into those the host can serve and those held back: a role
/// is held, with the first offending network, when any network it needs is
/// in `unavailable`.
pub fn effective_roles(
    roles: &BTreeSet<Role>,
    unavailable: &BTreeSet<NetworkId>,
) -> (BTreeSet<Role>, BTreeMap<Role, NetworkId>) {
    let mut active = BTreeSet::new();
    let mut held = BTreeMap::new();
    for &role in roles {
        match required_networks(role).into_iter().find(|n| unavailable.contains(n)) {
            Some(n) => {
                held.insert(role, n);
            }
            None => {
                active.insert(role);
            }
        }
    }
    (active, held)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{seed_networks, Attachment, Link, SwitchPortCfg};
    use crate::template::seed_templates;

    fn nid(s: &str) -> NetworkId {
        NetworkId::from(s)
    }
    fn pn(s: &str) -> PortName {
        PortName::from(s)
    }
    fn sid(s: &str) -> SwitchId {
        SwitchId::from(s)
    }
    fn roles(r: &[Role]) -> BTreeSet<Role> {
        r.iter().copied().collect()
    }
    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }
    fn template_cfg(id: &str) -> HostNetworkConfig {
        seed_templates().into_iter().find(|t| t.id == id).unwrap().config
    }

    fn neighbor(local: &str, switch: &str, port: &str) -> LldpNeighbor {
        LldpNeighbor { local_port: pn(local), switch: sid(switch), switch_port: pn(port) }
    }

    fn trunk(native: Option<u16>, allowed: &[u16], mtu: u16, lacp: Option<&str>) -> SwitchPortCfg {
        SwitchPortCfg {
            mode: PortMode::Trunk { native, allowed: allowed.iter().copied().collect() },
            mtu,
            lacp_group: lacp.map(String::from),
            admin_up: true,
        }
    }

    fn access(vlan: u16, mtu: u16) -> SwitchPortCfg {
        SwitchPortCfg { mode: PortMode::Access(vlan), mtu, lacp_group: None, admin_up: true }
    }

    fn observed(switch: &str, port: &str, cfg: SwitchPortCfg, mlag_peer: Option<&str>) -> ObservedSwitchPort {
        ObservedSwitchPort { switch: sid(switch), port: pn(port), cfg, mlag_peer: mlag_peer.map(sid), link_up: true, speed_mbps: 1000, neighbor: None }
    }

    /// One mgmt-only host on `eth0`, cabled to access-1 p1 (access VLAN 1).
    fn eth0_cfg(attachments: Vec<Attachment>, mtu: u16) -> HostNetworkConfig {
        HostNetworkConfig {
            links: vec![Link { name: "eth0".into(), kind: LinkKind::Single(pn("eth0")), mtu }],
            attachments,
        }
    }

    fn att(network: &str, link: &str, tagged: bool, ip: Option<&str>) -> Attachment {
        Attachment { network: nid(network), link: link.into(), tagged, ip: ip.map(|s| s.parse().unwrap()) }
    }

    fn run(
        cfg: &HostNetworkConfig,
        r: &[Role],
        lldp: &[LldpNeighbor],
        ports: &[ObservedSwitchPort],
    ) -> Vec<Issue> {
        validate_host(cfg, &roles(r), &seed_networks(), lldp, ports, &BTreeMap::new())
    }

    /// The ports a technician cabling an MS-01 ends up with: LACP trunks on
    /// both cores (MLAG pair) and an access port for eth0.
    fn technician_fixture() -> (Vec<LldpNeighbor>, Vec<ObservedSwitchPort>) {
        let lldp = vec![
            neighbor("sfp0", "core-1", "p1"),
            neighbor("sfp1", "core-2", "p1"),
            neighbor("eth0", "access-1", "p1"),
        ];
        let allowed = [20, 30, 100, 110];
        let ports = vec![
            observed("core-1", "p1", trunk(None, &allowed, 9216, Some("ms01-01")), Some("core-2")),
            observed("core-2", "p1", trunk(None, &allowed, 9216, Some("ms01-01")), Some("core-1")),
            observed("access-1", "p1", access(1, 9216), None),
        ];
        (lldp, ports)
    }

    #[test]
    fn ms01_standard_valid_on_technician_ports() {
        let (lldp, ports) = technician_fixture();
        let cfg = template_cfg("ms01-standard");
        let r = [Role::Storage, Role::Vm, Role::Compute];
        assert_eq!(run(&cfg, &r, &lldp, &ports), vec![]);
    }

    #[test]
    fn minipc_standard_valid_with_a_trunk_native_mgmt() {
        let cfg = template_cfg("minipc-standard");
        let lldp = [neighbor("eth0", "access-2", "p3")];
        let ports = [observed("access-2", "p3", trunk(Some(1), &[20], 9216, None), None)];
        assert_eq!(run(&cfg, &[Role::Storage, Role::Compute], &lldp, &ports), vec![]);
    }

    #[test]
    fn missing_required_network() {
        let cfg = eth0_cfg(vec![att("mgmt", "eth0", false, None)], 1500);
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", access(1, 9216), None)];
        let issues = run(&cfg, &[Role::Storage, Role::Compute], &lldp, &ports);
        assert_eq!(issues, vec![Issue::MissingRequiredNetwork { role: Role::Storage, network: nid("storage") }]);
    }

    #[test]
    fn vm_role_needs_migration_and_a_vm_network() {
        let cfg = eth0_cfg(vec![att("mgmt", "eth0", false, None)], 1500);
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", access(1, 9216), None)];
        let issues = run(&cfg, &[Role::Vm], &lldp, &ports);
        assert_eq!(
            issues,
            vec![
                Issue::MissingRequiredNetwork { role: Role::Vm, network: nid("migration") },
                Issue::MissingRequiredNetwork { role: Role::Vm, network: nid(ANY_VM_NETWORK) },
            ]
        );
        // either vm-* network satisfies the requirement
        let cfg = HostNetworkConfig {
            links: vec![Link { name: "eth0".into(), kind: LinkKind::Single(pn("eth0")), mtu: 9000 }],
            attachments: vec![
                att("mgmt", "eth0", false, None),
                att("migration", "eth0", true, None),
                att("vm-iot", "eth0", true, None),
            ],
        };
        let ports = [observed("access-1", "p1", trunk(Some(1), &[30, 110], 9216, None), None)];
        assert_eq!(run(&cfg, &[Role::Vm], &lldp, &ports), vec![]);
    }

    #[test]
    fn every_role_needs_mgmt() {
        let cfg = eth0_cfg(vec![att("storage", "eth0", true, None)], 9000);
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", trunk(None, &[20], 9216, None), None)];
        let issues = run(&cfg, &[Role::Transcode], &lldp, &ports);
        assert_eq!(issues, vec![Issue::MissingRequiredNetwork { role: Role::Transcode, network: nid("mgmt") }]);
    }

    #[test]
    fn a_vm_network_with_problems_does_not_count_as_usable() {
        // vm-lan rides a trunk that does not allow VLAN 100
        let cfg = HostNetworkConfig {
            links: vec![Link { name: "eth0".into(), kind: LinkKind::Single(pn("eth0")), mtu: 9000 }],
            attachments: vec![
                att("mgmt", "eth0", false, None),
                att("migration", "eth0", true, None),
                att("vm-lan", "eth0", true, None),
            ],
        };
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", trunk(Some(1), &[30], 9216, None), None)];
        let issues = run(&cfg, &[Role::Vm], &lldp, &ports);
        assert!(issues.contains(&Issue::MissingRequiredNetwork { role: Role::Vm, network: nid(ANY_VM_NETWORK) }), "{issues:?}");
        assert!(unavailable_networks(&issues).contains(&nid(ANY_VM_NETWORK)));
    }

    #[test]
    fn vlan_not_allowed_on_the_switch_port() {
        let cfg = template_cfg("minipc-standard");
        let lldp = [neighbor("eth0", "access-2", "p3")];
        let ports = [observed("access-2", "p3", trunk(Some(1), &[30], 9216, None), None)];
        let issues = run(&cfg, &[Role::Storage], &lldp, &ports);
        assert_eq!(
            issues,
            vec![Issue::VlanNotAllowed {
                network: nid("storage"),
                host_port: pn("eth0"),
                switch: sid("access-2"),
                switch_port: pn("p3"),
                vlan: 20,
            }]
        );
    }

    #[test]
    fn tagged_frames_on_an_access_port_are_not_allowed() {
        let cfg = template_cfg("minipc-standard");
        let lldp = [neighbor("eth0", "access-2", "p3")];
        let ports = [observed("access-2", "p3", access(1, 9216), None)];
        let issues = run(&cfg, &[Role::Storage], &lldp, &ports);
        assert!(matches!(issues.as_slice(), [Issue::VlanNotAllowed { vlan: 20, .. }]), "{issues:?}");
    }

    #[test]
    fn native_vlan_mismatch_for_untagged_attachments() {
        let cfg = eth0_cfg(vec![att("mgmt", "eth0", false, None)], 1500);
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let want = |native| Issue::NativeVlanMismatch {
            network: nid("mgmt"),
            host_port: pn("eth0"),
            switch: sid("access-1"),
            switch_port: pn("p1"),
            vlan: 1,
            native,
        };
        let ports = [observed("access-1", "p1", access(20, 9216), None)];
        assert_eq!(run(&cfg, &[Role::Compute], &lldp, &ports), vec![want(Some(20))]);
        let ports = [observed("access-1", "p1", trunk(None, &[1], 9216, None), None)];
        assert_eq!(run(&cfg, &[Role::Compute], &lldp, &ports), vec![want(None)]);
    }

    #[test]
    fn mtu_too_small_on_the_host_link() {
        let cfg = eth0_cfg(vec![att("mgmt", "eth0", false, None), att("storage", "eth0", true, None)], 1500);
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", trunk(Some(1), &[20], 9216, None), None)];
        let issues = run(&cfg, &[Role::Storage], &lldp, &ports);
        assert_eq!(
            issues,
            vec![Issue::MtuTooSmall { network: nid("storage"), where_: "link eth0".into(), have: 1500, need: 9000 }]
        );
    }

    #[test]
    fn mtu_too_small_on_the_switch_port_but_jumbo_ports_are_fine() {
        let cfg = template_cfg("minipc-standard");
        let lldp = [neighbor("eth0", "access-2", "p3")];
        let small = [observed("access-2", "p3", trunk(Some(1), &[20], 1500, None), None)];
        let issues = run(&cfg, &[Role::Storage], &lldp, &small);
        assert_eq!(
            issues,
            vec![Issue::MtuTooSmall { network: nid("storage"), where_: "access-2 p3".into(), have: 1500, need: 9000 }]
        );
        // 9216 >= 9000
        let jumbo = [observed("access-2", "p3", trunk(Some(1), &[20], 9216, None), None)];
        assert_eq!(run(&cfg, &[Role::Storage], &lldp, &jumbo), vec![]);
    }

    #[test]
    fn bond_split_without_mlag() {
        let (lldp, mut ports) = technician_fixture();
        ports[0].mlag_peer = None;
        ports[1].mlag_peer = None;
        let cfg = template_cfg("ms01-standard");
        let issues = run(&cfg, &[Role::Compute], &lldp, &ports);
        let nets: BTreeSet<_> = ["storage", "migration", "vm-lan", "vm-iot"].map(nid).into();
        assert_eq!(issues, vec![Issue::BondSplitWithoutMlag { link: "bond0".into(), networks: nets }]);
    }

    #[test]
    fn a_bond_on_one_switch_needs_no_mlag() {
        let (mut lldp, mut ports) = technician_fixture();
        lldp[1] = neighbor("sfp1", "core-1", "p2");
        ports[1] = observed("core-1", "p2", trunk(None, &[20, 30, 100, 110], 9216, Some("ms01-01")), None);
        ports[0].mlag_peer = None;
        assert_eq!(run(&template_cfg("ms01-standard"), &[Role::Compute], &lldp, &ports), vec![]);
    }

    #[test]
    fn lacp_not_configured_on_a_member_port() {
        let (lldp, mut ports) = technician_fixture();
        ports[1].cfg.lacp_group = None;
        let issues = run(&template_cfg("ms01-standard"), &[Role::Compute], &lldp, &ports);
        let nets: BTreeSet<_> = ["storage", "migration", "vm-lan", "vm-iot"].map(nid).into();
        assert_eq!(
            issues,
            vec![Issue::LacpNotConfigured { link: "bond0".into(), switch: sid("core-2"), port: pn("p1"), networks: nets }]
        );
    }

    #[test]
    fn lacp_groups_must_match_and_not_be_empty() {
        let (lldp, mut ports) = technician_fixture();
        ports[1].cfg.lacp_group = Some("other".into());
        let issues = run(&template_cfg("ms01-standard"), &[Role::Compute], &lldp, &ports);
        assert!(matches!(issues.as_slice(), [Issue::LacpNotConfigured { switch, .. }] if *switch == sid("core-2")), "{issues:?}");
        // an empty group counts as none, on both members
        ports[0].cfg.lacp_group = Some(String::new());
        ports[1].cfg.lacp_group = Some(String::new());
        let issues = run(&template_cfg("ms01-standard"), &[Role::Compute], &lldp, &ports);
        assert_eq!(issues.len(), 2, "{issues:?}");
    }

    #[test]
    fn active_backup_bonds_need_neither_mlag_nor_lacp() {
        let (lldp, mut ports) = technician_fixture();
        ports[0].mlag_peer = None;
        ports[1].mlag_peer = None;
        ports[0].cfg.lacp_group = None;
        ports[1].cfg.lacp_group = None;
        let mut cfg = template_cfg("ms01-standard");
        cfg.links[0].kind = LinkKind::Bond { members: vec![pn("sfp0"), pn("sfp1")], mode: BondMode::ActiveBackup };
        assert_eq!(run(&cfg, &[Role::Compute], &lldp, &ports), vec![]);
    }

    #[test]
    fn ip_conflict_names_the_other_host() {
        let cfg = eth0_cfg(vec![att("mgmt", "eth0", false, Some("10.0.0.7"))], 1500);
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", access(1, 9216), None)];
        let in_use = BTreeMap::from([(ip("10.0.0.7"), HostId::from("minipc-02")), (ip("10.0.0.8"), HostId::from("x"))]);
        let issues = validate_host(&cfg, &roles(&[Role::Compute]), &seed_networks(), &lldp, &ports, &in_use);
        assert_eq!(
            issues,
            vec![Issue::IpConflict { network: nid("mgmt"), ip: ip("10.0.0.7"), other_host: HostId::from("minipc-02") }]
        );
    }

    #[test]
    fn one_uncabled_bond_leg_is_degraded_not_broken() {
        let (mut lldp, ports) = technician_fixture();
        lldp.remove(1); // sfp1 unplugged
        let r = [Role::Storage, Role::Vm, Role::Compute];
        let issues = run(&template_cfg("ms01-standard"), &r, &lldp, &ports);
        assert_eq!(issues, vec![Issue::PortNotCabled { port: pn("sfp1"), networks: BTreeSet::new() }]);
        assert_eq!(broken_networks(&issues), BTreeSet::new());
        let (active, held) = effective_roles(&roles(&r), &unavailable_networks(&issues));
        assert_eq!((active, held), (roles(&r), BTreeMap::new()));
    }

    #[test]
    fn port_not_cabled_breaks_the_link_when_no_member_is_cabled() {
        let (mut lldp, ports) = technician_fixture();
        lldp.retain(|n| n.local_port == pn("eth0")); // both SFPs unplugged
        let r = [Role::Storage, Role::Vm, Role::Compute];
        let issues = run(&template_cfg("ms01-standard"), &r, &lldp, &ports);
        let nets: BTreeSet<_> = ["storage", "migration", "vm-lan", "vm-iot"].map(nid).into();
        assert_eq!(
            issues[..2],
            [
                Issue::PortNotCabled { port: pn("sfp0"), networks: nets.clone() },
                Issue::PortNotCabled { port: pn("sfp1"), networks: nets.clone() },
            ]
        );
        assert_eq!(broken_networks(&issues), nets);
        let (active, held) = effective_roles(&roles(&r), &broken_networks(&issues));
        assert_eq!(active, roles(&[Role::Compute]));
        assert_eq!(held.keys().copied().collect::<Vec<_>>(), [Role::Storage, Role::Vm]);
    }

    #[test]
    fn a_single_uncabled_link_breaks_its_networks() {
        let cfg = eth0_cfg(vec![att("mgmt", "eth0", false, None)], 1500);
        let issues = run(&cfg, &[Role::Compute], &[], &[]);
        assert_eq!(issues, vec![Issue::PortNotCabled { port: pn("eth0"), networks: [nid("mgmt")].into() }]);
        assert_eq!(broken_networks(&issues), [nid("mgmt")].into());
    }

    #[test]
    fn attachments_on_an_undefined_link_do_not_count_as_attached() {
        let (lldp, ports) = technician_fixture();
        let mut cfg = template_cfg("ms01-standard");
        for a in &mut cfg.attachments {
            if a.network == nid("mgmt") || a.network == nid("vm-lan") || a.network == nid("vm-iot") {
                a.link = "nope".into();
            }
        }
        let issues = run(&cfg, &[Role::Vm], &lldp, &ports);
        assert_eq!(
            issues,
            vec![
                Issue::MissingRequiredNetwork { role: Role::Vm, network: nid("mgmt") },
                Issue::MissingRequiredNetwork { role: Role::Vm, network: nid(ANY_VM_NETWORK) },
            ]
        );
    }

    #[test]
    fn unknown_network_is_reported_once() {
        let cfg = eth0_cfg(
            vec![att("mgmt", "eth0", false, None), att("ghost", "eth0", true, None), att("ghost", "eth0", false, None)],
            1500,
        );
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", access(1, 9216), None)];
        assert_eq!(run(&cfg, &[Role::Compute], &lldp, &ports), vec![Issue::UnknownNetwork { id: nid("ghost") }]);
    }

    #[test]
    fn unobserved_switch_ports_are_not_judged() {
        let cfg = template_cfg("minipc-standard");
        let lldp = [neighbor("eth0", "access-2", "p3")];
        assert_eq!(run(&cfg, &[Role::Storage], &lldp, &[]), vec![]);
    }

    #[test]
    fn broken_networks_collects_traffic_stopping_issues() {
        let issues = vec![
            Issue::MissingRequiredNetwork { role: Role::Storage, network: nid("storage") },
            Issue::VlanNotAllowed { network: nid("migration"), host_port: pn("a"), switch: sid("s"), switch_port: pn("b"), vlan: 30 },
            Issue::NativeVlanMismatch {
                network: nid("mgmt"),
                host_port: pn("a"),
                switch: sid("s"),
                switch_port: pn("b"),
                vlan: 1,
                native: None,
            },
            Issue::MtuTooSmall { network: nid("vm-lan"), where_: "link eth0".into(), have: 1, need: 2 },
            Issue::IpConflict { network: nid("vm-iot"), ip: ip("10.110.0.2"), other_host: HostId::from("h") },
            Issue::UnknownNetwork { id: nid("ghost") },
        ];
        let want: BTreeSet<_> = ["migration", "mgmt", "vm-lan", "vm-iot", "ghost"].map(nid).into();
        assert_eq!(broken_networks(&issues), want, "a missing network is not a broken one");
        let mut with_missing = want;
        with_missing.insert(nid("storage"));
        assert_eq!(unavailable_networks(&issues), with_missing);
    }

    #[test]
    fn link_level_issues_break_every_network_on_the_link() {
        let nets: BTreeSet<_> = ["storage", "migration"].map(nid).into();
        let issues = vec![
            Issue::BondSplitWithoutMlag { link: "bond0".into(), networks: nets.clone() },
            Issue::LacpNotConfigured { link: "bond0".into(), switch: sid("s"), port: pn("p"), networks: ["vm-lan"].map(nid).into() },
            Issue::PortNotCabled { port: pn("eth0"), networks: ["mgmt"].map(nid).into() },
        ];
        let want: BTreeSet<_> = ["storage", "migration", "vm-lan", "mgmt"].map(nid).into();
        assert_eq!(broken_networks(&issues), want);
        assert_eq!(broken_networks(&[]), BTreeSet::new());
    }

    #[test]
    fn storage_role_held_when_vlan20_missing() {
        let cfg = template_cfg("minipc-standard");
        let lldp = [neighbor("eth0", "access-2", "p3")];
        let ports = [observed("access-2", "p3", trunk(Some(1), &[30], 9216, None), None)];
        let r = roles(&[Role::Storage, Role::Compute]);
        let issues = validate_host(&cfg, &r, &seed_networks(), &lldp, &ports, &BTreeMap::new());
        let (active, held) = effective_roles(&r, &broken_networks(&issues));
        assert_eq!(active, roles(&[Role::Compute]));
        assert_eq!(held, BTreeMap::from([(Role::Storage, nid("storage"))]));
    }

    #[test]
    fn effective_roles_follow_the_role_to_network_table() {
        let all = roles(&[Role::ControlPlane, Role::Storage, Role::Compute, Role::Vm, Role::Transcode]);
        let held_for = |broken: &[&str]| effective_roles(&all, &broken.iter().map(|s| nid(s)).collect()).1;
        assert_eq!(held_for(&[]), BTreeMap::new());
        assert_eq!(held_for(&["vm-lan"]), BTreeMap::new(), "one vm network down leaves the role alone");
        assert_eq!(held_for(&["storage"]), BTreeMap::from([(Role::Storage, nid("storage"))]));
        assert_eq!(held_for(&["migration"]), BTreeMap::from([(Role::Vm, nid("migration"))]));
        assert_eq!(held_for(&[ANY_VM_NETWORK]), BTreeMap::from([(Role::Vm, nid(ANY_VM_NETWORK))]));
        // mgmt holds everything
        let held = held_for(&["mgmt"]);
        assert_eq!(held.keys().copied().collect::<BTreeSet<_>>(), all);
        assert!(held.values().all(|n| *n == nid("mgmt")));
        let (active, _) = effective_roles(&all, &[nid("mgmt")].into());
        assert!(active.is_empty());
    }

    #[test]
    fn roles_with_a_missing_network_are_held_via_unavailable_networks() {
        let cfg = eth0_cfg(vec![att("mgmt", "eth0", false, None)], 1500);
        let lldp = [neighbor("eth0", "access-1", "p1")];
        let ports = [observed("access-1", "p1", access(1, 9216), None)];
        let r = roles(&[Role::Storage, Role::Compute]);
        let issues = validate_host(&cfg, &r, &seed_networks(), &lldp, &ports, &BTreeMap::new());
        assert_eq!(broken_networks(&issues), BTreeSet::new());
        let (active, held) = effective_roles(&r, &unavailable_networks(&issues));
        assert_eq!(active, roles(&[Role::Compute]));
        assert_eq!(held, BTreeMap::from([(Role::Storage, nid("storage"))]));
    }

    #[test]
    fn issues_roundtrip_through_json() {
        let i = Issue::MtuTooSmall { network: nid("storage"), where_: "core-1 p3".into(), have: 1500, need: 9000 };
        let back: Issue = serde_json::from_str(&serde_json::to_string(&i).unwrap()).unwrap();
        assert_eq!(back, i);
    }
}
