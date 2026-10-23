//! Network templates: the host network configuration a profile (or one
//! role) gets by default, rendered into a concrete config with addresses.

use crate::hardware::ProfileId;
use crate::ids::{NetworkId, PortName};
use crate::network::{Attachment, BondMode, HostNetworkConfig, Link, LinkKind};
use crate::roles::Role;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use ts_rs::TS;

/// `config` carries no addresses (`ip` is `None` on every attachment); they
/// are allocated per host and filled in by [`render`]. `profile` is the
/// hardware the template was written for; a rule may apply it to a sibling
/// profile with the same NIC names (`sbc-transcode` serves Pi 5 and RK3588).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct NetworkTemplate {
    pub id: String,
    pub profile: ProfileId,
    pub role: Option<Role>,
    pub config: HostNetworkConfig,
}

fn single(name: &str, mtu: u16) -> Link {
    Link { name: name.into(), kind: LinkKind::Single(PortName::from(name)), mtu }
}

fn attach(network: &str, link: &str, tagged: bool) -> Attachment {
    Attachment { network: NetworkId::from(network), link: link.into(), tagged, ip: None }
}

/// The templates every cluster starts with.
pub fn seed_templates() -> Vec<NetworkTemplate> {
    let ms01 = HostNetworkConfig {
        links: vec![
            Link {
                name: "bond0".into(),
                kind: LinkKind::Bond { members: vec![PortName::from("sfp0"), PortName::from("sfp1")], mode: BondMode::Lacp },
                mtu: 9000,
            },
            single("eth0", 1500),
        ],
        attachments: vec![
            attach("mgmt", "eth0", false),
            attach("storage", "bond0", true),
            attach("migration", "bond0", true),
            attach("vm-lan", "bond0", true),
            attach("vm-iot", "bond0", true),
        ],
    };
    let minipc = HostNetworkConfig {
        links: vec![single("eth0", 9000)],
        attachments: vec![attach("mgmt", "eth0", false), attach("storage", "eth0", true)],
    };
    let sbc = HostNetworkConfig { links: vec![single("eth0", 1500)], attachments: vec![attach("mgmt", "eth0", false)] };
    vec![
        NetworkTemplate { id: "ms01-standard".into(), profile: ProfileId::Ms01, role: None, config: ms01 },
        NetworkTemplate { id: "minipc-standard".into(), profile: ProfileId::MiniPc, role: None, config: minipc },
        NetworkTemplate { id: "sbc-transcode".into(), profile: ProfileId::Pi5, role: None, config: sbc },
    ]
}

/// The config a host should run: the host's `override_` if it has one, else
/// the template's, with each attachment's address taken from `ips` (by
/// network). An attachment whose network has no entry keeps the address it
/// already carries.
pub fn render(
    t: &NetworkTemplate,
    override_: Option<&HostNetworkConfig>,
    ips: &BTreeMap<NetworkId, Ipv4Addr>,
) -> HostNetworkConfig {
    let mut cfg = override_.unwrap_or(&t.config).clone();
    for a in &mut cfg.attachments {
        a.ip = ips.get(&a.network).copied().or(a.ip);
    }
    cfg
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template(id: &str) -> NetworkTemplate {
        seed_templates().into_iter().find(|t| t.id == id).unwrap()
    }

    fn carried(cfg: &HostNetworkConfig, link: &str) -> Vec<(String, bool)> {
        cfg.attachments.iter().filter(|a| a.link == link).map(|a| (a.network.to_string(), a.tagged)).collect()
    }

    #[test]
    fn seed_template_ids_and_profiles() {
        let got: Vec<_> = seed_templates().into_iter().map(|t| (t.id, t.profile, t.role)).collect();
        assert_eq!(
            got,
            [
                ("ms01-standard".to_string(), ProfileId::Ms01, None),
                ("minipc-standard".to_string(), ProfileId::MiniPc, None),
                ("sbc-transcode".to_string(), ProfileId::Pi5, None),
            ]
        );
    }

    #[test]
    fn ms01_standard_has_an_lacp_bond_and_a_mgmt_link() {
        let c = template("ms01-standard").config;
        assert_eq!(c.links.len(), 2);
        let bond = c.link("bond0").unwrap();
        assert_eq!(
            bond.kind,
            LinkKind::Bond { members: vec![PortName::from("sfp0"), PortName::from("sfp1")], mode: BondMode::Lacp }
        );
        assert_eq!(bond.mtu, 9000);
        let eth0 = c.link("eth0").unwrap();
        assert_eq!((&eth0.kind, eth0.mtu), (&LinkKind::Single(PortName::from("eth0")), 1500));
        assert_eq!(
            carried(&c, "bond0"),
            [("storage", true), ("migration", true), ("vm-lan", true), ("vm-iot", true)].map(|(n, t)| (n.to_string(), t))
        );
        assert_eq!(carried(&c, "eth0"), [("mgmt".to_string(), false)]);
    }

    #[test]
    fn minipc_and_sbc_templates() {
        let c = template("minipc-standard").config;
        assert_eq!(c.links.len(), 1);
        assert_eq!(c.link("eth0").unwrap().mtu, 9000);
        assert_eq!(carried(&c, "eth0"), [("mgmt".to_string(), false), ("storage".to_string(), true)]);

        let c = template("sbc-transcode").config;
        assert_eq!(c.links.len(), 1);
        assert_eq!(c.link("eth0").unwrap().mtu, 1500);
        assert_eq!(carried(&c, "eth0"), [("mgmt".to_string(), false)]);
    }

    #[test]
    fn seed_templates_carry_no_addresses_and_only_seed_networks() {
        let known: Vec<_> = crate::network::seed_networks().into_iter().map(|n| n.id).collect();
        for t in seed_templates() {
            for a in &t.config.attachments {
                assert_eq!(a.ip, None, "{}", t.id);
                assert!(known.contains(&a.network), "{} -> {}", t.id, a.network);
                assert!(t.config.link(&a.link).is_some(), "{} names a link it lacks", t.id);
            }
        }
    }

    #[test]
    fn render_fills_addresses_by_network() {
        let t = template("minipc-standard");
        let ips = BTreeMap::from([
            (NetworkId::from("mgmt"), "10.0.0.5".parse().unwrap()),
            (NetworkId::from("storage"), "10.20.0.5".parse().unwrap()),
            (NetworkId::from("vm-lan"), "10.100.0.5".parse().unwrap()),
        ]);
        let cfg = render(&t, None, &ips);
        let got: Vec<_> = cfg.attachments.iter().map(|a| (a.network.to_string(), a.ip.unwrap().to_string())).collect();
        assert_eq!(got, [("mgmt", "10.0.0.5"), ("storage", "10.20.0.5")].map(|(n, i)| (n.to_string(), i.to_string())));
        assert_eq!(cfg.links, t.config.links);
        assert!(t.config.attachments.iter().all(|a| a.ip.is_none()), "the template is left alone");
    }

    #[test]
    fn render_without_an_address_leaves_the_attachment_unaddressed() {
        let cfg = render(&template("sbc-transcode"), None, &BTreeMap::new());
        assert_eq!(cfg.attachments[0].ip, None);
    }

    #[test]
    fn render_prefers_the_override_over_the_template() {
        let t = template("ms01-standard");
        let mut ov = template("sbc-transcode").config;
        ov.links[0].mtu = 1400;
        let ips = BTreeMap::from([(NetworkId::from("mgmt"), "10.0.0.9".parse().unwrap())]);
        let cfg = render(&t, Some(&ov), &ips);
        assert_eq!(cfg.links.len(), 1);
        assert_eq!(cfg.links[0].mtu, 1400);
        assert_eq!(cfg.attachments[0].ip, Some("10.0.0.9".parse().unwrap()));
    }
}
