//! Scenarios: the simulator playing a human, e.g. a technician racking new
//! hosts. Unlike faults they may run the simulation while they work.

use crate::engine::Engine;
use crate::presets::{cable_host, capacity, host_id, host_port_cfg, profile_prefix};
use crate::topology::{PortMode, SwitchPortCfg};
use crate::world::HostHw;
use control::agent::HostAgent;
use control::client::{ClientActor, CLIENT};
use control::proto::{ApiOp, ApiResult};
use control::runtime::{Addr, Endpoint, Svc};
use model::ProfileId;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// How long [`Scenario::AddHosts`] waits for the control plane to hand out a join token.
pub const TOKEN_WAIT_MS: u64 = 5000;
/// The port MTU [`Misconfig::MtuTooSmall`] leaves: too small for jumbo networks.
pub const TOO_SMALL_MTU: u16 = 1500;

/// A technician's mistake on the switch ports a new host lands on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Misconfig {
    /// The VLAN is left out of the port's trunk allowed list.
    MissingVlan(u16),
    MtuTooSmall,
    /// Bond member ports get no LACP group.
    BondNoLacp,
}

impl Misconfig {
    /// Applies the mistake to a port configured for the host. Ports where it
    /// does not apply (e.g. a missing VLAN on an access port) are unchanged.
    pub fn apply(self, cfg: &mut SwitchPortCfg) {
        match self {
            Misconfig::MissingVlan(vlan) => {
                if let PortMode::Trunk { allowed, .. } = &mut cfg.mode {
                    allowed.remove(&vlan);
                }
            }
            Misconfig::MtuTooSmall => cfg.mtu = TOO_SMALL_MTU,
            Misconfig::BondNoLacp => cfg.lacp_group = None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Scenario {
    /// Racks `count` new hosts: gets a join token through the client, cables
    /// each host to free switch ports, configures those ports and boots its
    /// agent with the token. Hosts are numbered after the profile's existing ones.
    AddHosts { profile: ProfileId, count: u32, misconfigure: Option<Misconfig> },
}

impl Engine {
    /// Runs `s`, advancing the simulation while it waits on the control plane.
    pub fn scenario(&mut self, s: Scenario) -> Result<(), String> {
        match s {
            Scenario::AddHosts { profile, count, misconfigure } => self.add_hosts(profile, count, misconfigure),
        }
    }

    fn add_hosts(&mut self, profile: ProfileId, count: u32, misconfigure: Option<Misconfig>) -> Result<(), String> {
        let room = capacity(self.fabric().world(), profile);
        if room < count as usize {
            return Err(format!("room for {room} more {profile:?} hosts, not {count}"));
        }
        let token = self.create_join_token()?;
        let prefix = format!("{}-", profile_prefix(profile));
        let w = self.fabric().world();
        let first =
            w.hosts.keys().filter_map(|h| h.as_str().strip_prefix(&prefix)?.parse::<u32>().ok()).max().unwrap_or(0) + 1;
        for n in first..first + count {
            let (id, variant) = (host_id(profile, n), ((n - 1) % 2) as u8);
            let w = self.fabric_mut().world_mut();
            w.hosts.insert(id.clone(), HostHw { join_token: Some(token.clone()), ..HostHw::new(profile, variant) });
            for (s, p) in cable_host(w, &id, profile)?.switch_ports {
                let mut cfg = host_port_cfg();
                if let Some(m) = misconfigure {
                    m.apply(&mut cfg);
                }
                w.topology.set_port(&s, &p, cfg);
            }
            self.spawn(Endpoint::new(Addr::Host(id), Svc::Agent), Box::new(HostAgent::new(Some(token.clone()))));
        }
        Ok(())
    }

    /// Asks the control plane for a join token through the UI client, running
    /// the simulation for up to [`TOKEN_WAIT_MS`] for the answer.
    fn create_join_token(&mut self) -> Result<String, String> {
        if self.actor::<ClientActor>(&CLIENT).is_none() {
            return Err("no client to create a join token with".into());
        }
        let req = self.api(ApiOp::CreateJoinToken);
        let deadline = self.now() + TOKEN_WAIT_MS;
        self.run_until(deadline, |e| e.actor::<ClientActor>(&CLIENT).is_some_and(|c| c.result(req).is_some()));
        match self.take_api_result(req) {
            Some(ApiResult::Ok(serde_json::Value::String(token))) => Ok(token),
            Some(other) => Err(format!("could not create a join token: {other:?}")),
            None => Err(format!("no join token within {} s", TOKEN_WAIT_MS / 1000)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::presets::FleetSpec;
    use crate::topology::Node;
    use model::{HostId, PortName, SwitchId};

    #[test]
    fn misconfigs_break_what_they_name() {
        let trunk = SwitchPortCfg {
            mode: PortMode::Trunk { native: Some(1), allowed: [1, 20].into() },
            mtu: 9216,
            lacp_group: Some("h".into()),
            admin_up: true,
        };
        let mut c = trunk.clone();
        Misconfig::MissingVlan(20).apply(&mut c);
        assert_eq!(c.mode, PortMode::Trunk { native: Some(1), allowed: [1].into() });
        let mut c = trunk.clone();
        Misconfig::MtuTooSmall.apply(&mut c);
        assert_eq!(c.mtu, TOO_SMALL_MTU);
        let mut c = trunk;
        Misconfig::BondNoLacp.apply(&mut c);
        assert_eq!(c.lacp_group, None);
        let mut c = host_port_cfg();
        Misconfig::MissingVlan(20).apply(&mut c);
        assert_eq!(c, host_port_cfg(), "an access port has no allowed list");
    }

    fn engine() -> Engine {
        let mut e = Engine::new(5);
        e.load_fleet(&FleetSpec { ms01: 1, minipc: 0, pi5: 0, rk3588: 0 }, 1);
        e.run_for(5000);
        e
    }

    #[test]
    fn add_hosts_cables_configures_and_boots_with_a_token() {
        let mut e = engine();
        let before = e.now();
        let s = Scenario::AddHosts { profile: ProfileId::Ms01, count: 2, misconfigure: Some(Misconfig::MtuTooSmall) };
        e.scenario(s).unwrap();
        assert!(e.now() > before, "ran the sim while waiting for the token");
        let w = e.fabric().world();
        let (h2, h3) = (HostId::from("ms01-02"), HostId::from("ms01-03"));
        let token = w.hosts[&h2].join_token.clone().expect("booted with a token");
        assert_eq!(w.hosts[&h3].join_token.as_ref(), Some(&token), "one token for the batch");
        assert_eq!((w.hosts[&h2].variant, w.hosts[&h3].variant), (1, 0), "variants keep alternating by number");
        let peer = |h: &HostId, p: &str| w.topology.peer(&Node::HostPort(h.clone(), PortName::from(p))).cloned();
        let sp = |s: &str, p: &str| Some(Node::SwitchPort(SwitchId::from(s), PortName::from(p)));
        assert_eq!(peer(&h2, "eth0"), sp("access-1", "p2"));
        assert_eq!(peer(&h2, "sfp0"), sp("core-1", "p2"));
        assert_eq!(peer(&h3, "sfp1"), sp("core-2", "p3"));
        for (s, p) in [("access-1", "p2"), ("core-1", "p2"), ("core-2", "p2")] {
            assert_eq!(w.topology.switches[&SwitchId::from(s)].port(&PortName::from(p)).unwrap().mtu, TOO_SMALL_MTU);
        }
        assert!(e.actor::<HostAgent>(&Endpoint::new(Addr::Host(h3), Svc::Agent)).is_some());
        e.run_for(1000);
        assert!(e.client_view().state.tokens.contains_key(&token), "the token came from the control plane");
    }

    #[test]
    fn add_hosts_refuses_more_than_fits() {
        let mut e = engine();
        let err = e.scenario(Scenario::AddHosts { profile: ProfileId::Pi5, count: 46, misconfigure: None }).unwrap_err();
        assert!(err.contains("room for 45"), "{err}");
        assert_eq!(e.fabric().world().hosts.len(), 1);
    }

    #[test]
    fn scenarios_are_plain_json() {
        let s = Scenario::AddHosts { profile: ProfileId::MiniPc, count: 1, misconfigure: Some(Misconfig::MissingVlan(20)) };
        let json = serde_json::to_value(&s).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"AddHosts": {"profile": "MiniPc", "count": 1, "misconfigure": {"MissingVlan": 20}}})
        );
        assert_eq!(serde_json::from_value::<Scenario>(json).unwrap(), s);
    }
}
