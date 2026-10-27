//! Rendering a [`HostNetworkConfig`] as systemd-networkd files. Pure, so it
//! is tested here; [`super::LinuxOs`] writes the files and reloads networkd.
//!
//! Every file is named `05-homelab-*`, so it sorts before the `10-netplan-*`
//! files the image's bootstrap leaves in place: networkd uses the first
//! `.network` file (by name) that matches a link.

use model::network::{BondMode, HostNetworkConfig, LinkKind, LogicalNetwork, NetworkPurpose};
use model::NetworkId;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::net::Ipv4Addr;

pub const PREFIX: &str = "05-homelab-";

/// One file networkd reads: its name and contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub name: String,
    pub body: String,
}

impl File {
    fn new(name: String, body: String) -> Self {
        Self { name: format!("{PREFIX}{name}"), body }
    }
}

/// The files for `cfg`. `networks` defines every network it attaches (VLAN,
/// subnet, MTU); `nics` are the host's physical NICs, and those `cfg` leaves
/// out are still brought up (with no addresses) so their carrier and LLDP
/// can be seen. An untagged attachment without an address uses DHCP.
/// Fails if `cfg` attaches a network `networks` does not define.
pub fn render(cfg: &HostNetworkConfig, networks: &BTreeMap<NetworkId, LogicalNetwork>, nics: &[String]) -> Result<Vec<File>, String> {
    let mut files = vec![];
    let mut used = vec![];
    for link in &cfg.links {
        let iface = match &link.kind {
            LinkKind::Single(port) => port.to_string(),
            LinkKind::Bond { members, mode } => {
                files.push(File::new(format!("{}.netdev", link.name), bond_netdev(&link.name, link.mtu, *mode)));
                for m in members {
                    let body = format!(
                        "[Match]\nName={m}\n\n[Link]\nMTUBytes={}\nRequiredForOnline=no\n\n[Network]\nBond={}\nLinkLocalAddressing=no\nLLDP=no\n",
                        link.mtu, link.name
                    );
                    files.push(File::new(format!("{m}.network"), body));
                    used.push(m.to_string());
                }
                link.name.clone()
            }
        };
        used.push(iface.clone());
        let mut net = format!("[Match]\nName={iface}\n\n[Link]\nMTUBytes={}\nRequiredForOnline=no\n\n[Network]\n", link.mtu);
        net.push_str("LinkLocalAddressing=no\nLLDP=no\nConfigureWithoutCarrier=yes\nIgnoreCarrierLoss=yes\n");
        let mut dhcp = false;
        for a in cfg.attachments.iter().filter(|a| a.link == link.name) {
            let def = || networks.get(&a.network).ok_or_else(|| format!("network {} is not defined", a.network));
            if a.tagged {
                let n = def()?;
                let vif = format!("{iface}.{}", n.vlan);
                writeln!(net, "VLAN={vif}").expect("writing to a String");
                let netdev = format!("[NetDev]\nName={vif}\nKind=vlan\nMTUBytes={}\n\n[VLAN]\nId={}\n", n.mtu.min(link.mtu), n.vlan);
                files.push(File::new(format!("{vif}.netdev"), netdev));
                let mut body = format!("[Match]\nName={vif}\n\n[Link]\nRequiredForOnline=no\n\n[Network]\n");
                body.push_str("LinkLocalAddressing=no\nLLDP=no\nConfigureWithoutCarrier=yes\nIgnoreCarrierLoss=yes\n");
                if let Some(ip) = a.ip {
                    address(&mut body, ip, n);
                }
                files.push(File::new(format!("{vif}.network"), body));
            } else {
                match a.ip {
                    Some(ip) => address(&mut net, ip, def()?),
                    None => dhcp = true,
                }
            }
        }
        if dhcp {
            net.push_str("DHCP=ipv4\n\n[DHCPv4]\nClientIdentifier=mac\nUseDNS=no\nUseHostname=no\n");
        }
        files.push(File::new(format!("{iface}.network"), net));
    }
    for nic in nics.iter().filter(|n| !used.contains(n)) {
        let body = format!(
            "[Match]\nName={nic}\n\n[Link]\nRequiredForOnline=no\nActivationPolicy=up\n\n[Network]\nLinkLocalAddressing=no\nLLDP=no\n"
        );
        files.push(File::new(format!("{nic}.network"), body));
    }
    Ok(files)
}

/// `Address=` for `ip` in `n`'s subnet, plus, on the management network,
/// the default route through the router (the subnet's `.1`) and the
/// network's DNS resolvers.
fn address(body: &mut String, ip: Ipv4Addr, n: &LogicalNetwork) {
    writeln!(body, "Address={ip}/{}", n.subnet.prefix_len()).expect("writing to a String");
    if n.purpose == NetworkPurpose::Mgmt {
        let gw = Ipv4Addr::from(u32::from(n.subnet.network()) + 1);
        writeln!(body, "Gateway={gw}").expect("writing to a String");
        for dns in &n.dns {
            writeln!(body, "DNS={dns}").expect("writing to a String");
        }
    }
}

fn bond_netdev(name: &str, mtu: u16, mode: BondMode) -> String {
    let bond = match mode {
        BondMode::Lacp => "Mode=802.3ad\nLACPTransmitRate=fast\nTransmitHashPolicy=layer3+4\nMIIMonitorSec=100ms\n",
        BondMode::ActiveBackup => "Mode=active-backup\nMIIMonitorSec=100ms\n",
    };
    format!("[NetDev]\nName={name}\nKind=bond\nMTUBytes={mtu}\n\n[Bond]\n{bond}")
}

/// The virtual interfaces (bonds, VLANs) `files` create, by name.
pub fn netdevs(files: &[File]) -> BTreeMap<String, String> {
    files
        .iter()
        .filter_map(|f| {
            let name = f.name.strip_prefix(PREFIX)?.strip_suffix(".netdev")?;
            Some((name.to_string(), f.body.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::network::{bootstrap_config, seed_networks};
    use model::{profile, render as render_template, seed_templates, ProfileId};

    fn networks() -> BTreeMap<NetworkId, LogicalNetwork> {
        seed_networks().into_iter().map(|n| (n.id.clone(), n)).collect()
    }

    fn nics(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    fn file<'a>(files: &'a [File], name: &str) -> &'a str {
        let full = format!("{PREFIX}{name}");
        &files.iter().find(|f| f.name == full).unwrap_or_else(|| panic!("no {full} in {:?}", files.iter().map(|f| &f.name).collect::<Vec<_>>())).body
    }

    #[test]
    fn bootstrap_is_dhcp_on_eth0_and_other_nics_come_up_bare() {
        let cfg = bootstrap_config(&profile(ProfileId::Ms01, 0));
        let files = render(&cfg, &networks(), &nics(&["sfp0", "sfp1", "eth0", "eth1"])).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["05-homelab-eth0.network", "05-homelab-sfp0.network", "05-homelab-sfp1.network", "05-homelab-eth1.network"]);
        let eth0 = file(&files, "eth0.network");
        assert!(eth0.contains("DHCP=ipv4") && eth0.contains("MTUBytes=1500"), "{eth0}");
        assert!(!eth0.contains("Address="), "{eth0}");
        assert!(file(&files, "sfp0.network").contains("ActivationPolicy=up"));
    }

    #[test]
    fn the_ms01_template_builds_an_lacp_bond_with_tagged_vlans() {
        let t = seed_templates().into_iter().find(|t| t.id == "ms01-standard").unwrap();
        let ips = [
            (NetworkId::from("mgmt"), "10.0.0.2".parse().unwrap()),
            (NetworkId::from("storage"), "10.20.0.2".parse().unwrap()),
            (NetworkId::from("migration"), "10.30.0.2".parse().unwrap()),
        ]
        .into();
        let cfg = render_template(&t, None, &ips);
        let files = render(&cfg, &networks(), &nics(&["sfp0", "sfp1", "eth0", "eth1"])).unwrap();

        let bond = file(&files, "bond0.netdev");
        assert!(bond.contains("Kind=bond") && bond.contains("Mode=802.3ad") && bond.contains("LACPTransmitRate=fast"), "{bond}");
        assert!(bond.contains("MTUBytes=9000"));
        for m in ["sfp0", "sfp1"] {
            assert!(file(&files, &format!("{m}.network")).contains("Bond=bond0"));
        }
        let bond_net = file(&files, "bond0.network");
        for vif in ["bond0.20", "bond0.30", "bond0.100", "bond0.110"] {
            assert!(bond_net.contains(&format!("VLAN={vif}")), "{bond_net}");
        }
        let storage = file(&files, "bond0.20.netdev");
        assert!(storage.contains("Id=20") && storage.contains("MTUBytes=9000"), "{storage}");
        assert!(file(&files, "bond0.20.network").contains("Address=10.20.0.2/22"));
        assert!(file(&files, "bond0.100.netdev").contains("MTUBytes=1500"), "a VLAN gets its network's MTU");
        assert!(!file(&files, "bond0.100.network").contains("Address="), "VM networks address guests, not the host");
        let eth0 = file(&files, "eth0.network");
        assert!(eth0.contains("Address=10.0.0.2/22") && eth0.contains("Gateway=10.0.0.1"), "{eth0}");
        assert!(eth0.contains("DNS=10.0.0.1"), "the mgmt network's resolver: {eth0}");
        assert!(!eth0.contains("DHCP"), "{eth0}");
        assert!(file(&files, "eth1.network").contains("ActivationPolicy=up"), "the unused NIC still comes up");
        assert_eq!(netdevs(&files).keys().collect::<Vec<_>>(), ["bond0", "bond0.100", "bond0.110", "bond0.20", "bond0.30"]);
    }

    #[test]
    fn a_vlan_never_exceeds_its_links_mtu_and_active_backup_is_rendered() {
        let mut nets = networks();
        nets.get_mut(&NetworkId::from("storage")).unwrap().mtu = 9000;
        let cfg = HostNetworkConfig {
            links: vec![model::network::Link {
                name: "bond1".into(),
                kind: LinkKind::Bond { members: vec!["eth0".into(), "eth1".into()], mode: BondMode::ActiveBackup },
                mtu: 1500,
            }],
            attachments: vec![model::network::Attachment { network: NetworkId::from("storage"), link: "bond1".into(), tagged: true, ip: None }],
        };
        let files = render(&cfg, &nets, &nics(&["eth0", "eth1"])).unwrap();
        assert!(file(&files, "bond1.netdev").contains("Mode=active-backup"));
        assert!(file(&files, "bond1.20.netdev").contains("MTUBytes=1500"));
    }

    #[test]
    fn an_undefined_network_is_refused() {
        let cfg = bootstrap_config(&profile(ProfileId::MiniPc, 0));
        assert!(render(&cfg, &BTreeMap::new(), &nics(&["eth0"])).is_ok(), "DHCP needs no definition");
        let mut cfg = cfg;
        cfg.attachments[0].ip = Some("10.0.0.9".parse().unwrap());
        assert_eq!(render(&cfg, &BTreeMap::new(), &nics(&["eth0"])), Err("network mgmt is not defined".into()));
    }
}
