//! Scenarios: the simulator playing a human, e.g. a technician racking new
//! hosts. Unlike faults they may run the simulation while they work.

use crate::engine::Engine;
use crate::presets::{cable_host, capacity, host_id, profile_prefix};
use crate::topology::{PortMode, SwitchPortCfg};
use crate::traffic::{FlowEnd, FlowKind};
use crate::world::HostHw;
use control::agent::HostAgent;
use control::client::{ClientActor, CLIENT};
use control::proto::{ApiOp, ApiResult};
use control::runtime::{Addr, Endpoint, Svc};
use model::{HostId, NetworkId, ProfileId};
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
    /// each host to free switch ports, configures those ports for the
    /// profile's template (with `misconfigure`'s mistake, if any) and boots
    /// its agent with the token. Hosts are numbered after the profile's existing ones.
    AddHosts { profile: ProfileId, count: u32, misconfigure: Option<Misconfig> },
    /// An iperf run: `streams` parallel flows from `from` to `to` on
    /// `network`, each asking for `mbps`, for `duration_s` seconds.
    Iperf {
        from: HostId,
        to: FlowEnd,
        network: NetworkId,
        mbps: u32,
        duration_s: u32,
        #[serde(default = "one_stream")]
        streams: u32,
    },
}

fn one_stream() -> u32 {
    1
}

impl Engine {
    /// Runs `s`, advancing the simulation while it waits on the control plane.
    pub fn scenario(&mut self, s: Scenario) -> Result<(), String> {
        match s {
            Scenario::AddHosts { profile, count, misconfigure } => self.add_hosts(profile, count, misconfigure),
            Scenario::Iperf { from, to, network, mbps, duration_s, streams } => {
                self.iperf(from, to, network, mbps, duration_s, streams)
            }
        }
    }

    fn iperf(&mut self, from: HostId, to: FlowEnd, network: NetworkId, mbps: u32, duration_s: u32, streams: u32) -> Result<(), String> {
        let w = self.fabric().world();
        let to_host = match &to {
            FlowEnd::Host(h) => Some(h),
            FlowEnd::Router => None,
        };
        if let Some(h) = std::iter::once(&from).chain(to_host).find(|h| !w.hosts.contains_key(*h)) {
            return Err(format!("unknown host {h}"));
        }
        if to == FlowEnd::Host(from.clone()) {
            return Err(format!("{from} cannot iperf itself"));
        }
        if !w.networks.contains_key(&network) {
            return Err(format!("unknown network {network}"));
        }
        if streams == 0 {
            return Err("an iperf needs at least one stream".into());
        }
        let until = self.now() + u64::from(duration_s) * 1000;
        let traffic = self.fabric_mut().traffic_mut();
        for _ in 0..streams {
            traffic.start(FlowEnd::Host(from.clone()), to.clone(), network.clone(), f64::from(mbps), FlowKind::Iperf, Some(until));
        }
        Ok(())
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
            // Cable first: if there is no room, nothing of this host exists yet.
            let cabled = cable_host(w, &id, profile)?;
            w.hosts.insert(id.clone(), HostHw { join_token: Some(token.clone()), ..HostHw::new(profile, variant) });
            if let Some(m) = misconfigure {
                for (s, p) in cabled.switch_ports {
                    let Some(mut cfg) = w.topology.switches.get(&s).and_then(|sw| sw.port(&p)).cloned() else { continue };
                    m.apply(&mut cfg);
                    w.topology.set_port(&s, &p, cfg);
                }
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
    use crate::presets::{host_port_cfg, FleetSpec};
    use crate::topology::Node;
    use crate::traffic::{Flow, FlowKind};
    use model::{HostId, NetworkId, PortName, SwitchId};
    use std::collections::BTreeSet;

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
        let port = |s: &str, p: &str| w.topology.switches[&SwitchId::from(s)].port(&PortName::from(p)).unwrap().clone();
        for (s, p) in [("access-1", "p2"), ("core-1", "p2"), ("core-2", "p2")] {
            assert_eq!(port(s, p).mtu, TOO_SMALL_MTU, "the mistake lands on every port");
        }
        // Otherwise configured for the template: eth0 on mgmt, the SFP+ bond an LACP trunk.
        assert_eq!(port("access-1", "p2").mode, PortMode::Access(1));
        let sfp = port("core-2", "p3");
        assert_eq!((sfp.mode, sfp.lacp_group.as_deref()), (PortMode::Trunk { native: None, allowed: [20, 30, 100, 110].into() }, Some("ms01-03")));
        assert!(e.actor::<HostAgent>(&Endpoint::new(Addr::Host(h3), Svc::Agent)).is_some());
        e.run_for(1000);
        assert!(e.client_view().state.tokens.contains_key(&token), "the token came from the control plane");
    }

    #[test]
    fn add_hosts_configures_ports_for_the_template_bar_the_mistake() {
        let mut e = engine();
        let s = Scenario::AddHosts { profile: ProfileId::MiniPc, count: 1, misconfigure: Some(Misconfig::MissingVlan(20)) };
        e.scenario(s).unwrap();
        let port = |e: &Engine, s: &str, p: &str| e.fabric().world().topology.switches[&SwitchId::from(s)].port(&PortName::from(p)).unwrap().clone();
        let trunk = |allowed: &[u16]| PortMode::Trunk { native: Some(1), allowed: allowed.iter().copied().collect() };
        assert_eq!(port(&e, "access-1", "p2").mode, trunk(&[1]), "VLAN 20 left out");
        e.scenario(Scenario::AddHosts { profile: ProfileId::MiniPc, count: 1, misconfigure: None }).unwrap();
        assert_eq!(port(&e, "access-1", "p3"), SwitchPortCfg { mode: trunk(&[1, 20]), ..host_port_cfg() });
        e.scenario(Scenario::AddHosts { profile: ProfileId::Ms01, count: 1, misconfigure: Some(Misconfig::BondNoLacp) }).unwrap();
        assert_eq!((port(&e, "core-1", "p2").lacp_group, port(&e, "core-2", "p2").lacp_group), (None, None));
        e.scenario(Scenario::AddHosts { profile: ProfileId::Pi5, count: 1, misconfigure: None }).unwrap();
        assert_eq!(port(&e, "access-1", "p5"), host_port_cfg());
    }

    #[test]
    fn add_hosts_refuses_more_than_fits() {
        let mut e = engine();
        let err = e.scenario(Scenario::AddHosts { profile: ProfileId::Pi5, count: 46, misconfigure: None }).unwrap_err();
        assert!(err.contains("room for 45"), "{err}");
        assert_eq!(e.fabric().world().hosts.len(), 1);
    }

    #[test]
    fn iperf_starts_one_flow_per_stream() {
        let mut e = engine();
        let (h, storage) = (HostId::from("ms01-01"), NetworkId::from("storage"));
        let iperf = |from: &str, to: FlowEnd, network: &str, streams| Scenario::Iperf {
            from: HostId::from(from),
            to,
            network: NetworkId::from(network),
            mbps: 100,
            duration_s: 10,
            streams,
        };
        e.scenario(iperf("ms01-01", FlowEnd::Router, "storage", 3)).unwrap();
        let now = e.now();
        let flows: Vec<Flow> = e.fabric().world().traffic.flows().cloned().collect();
        assert_eq!(flows.len(), 3);
        assert_eq!(flows.iter().map(|f| f.id).collect::<BTreeSet<_>>().len(), 3, "distinct ids");
        for f in &flows {
            assert_eq!((&f.src, &f.dst, &f.network, f.demand_mbps), (&FlowEnd::Host(h.clone()), &FlowEnd::Router, &storage, 100.0));
            assert_eq!((f.kind, f.until), (FlowKind::Iperf, Some(now + 10_000)));
        }
        for bad in [
            iperf("nope", FlowEnd::Router, "storage", 1),
            iperf("ms01-01", FlowEnd::Host(HostId::from("nope")), "storage", 1),
            iperf("ms01-01", FlowEnd::Host(h.clone()), "storage", 1),
            iperf("ms01-01", FlowEnd::Router, "nope", 1),
            iperf("ms01-01", FlowEnd::Router, "storage", 0),
        ] {
            assert!(e.scenario(bad.clone()).is_err(), "{bad:?} accepted");
        }
        assert_eq!(e.fabric().world().traffic.flows().count(), 3);
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
        let iperf = serde_json::json!({"Iperf": {"from": "ms01-01", "to": "Router", "network": "storage", "mbps": 10, "duration_s": 5}});
        let Scenario::Iperf { streams, .. } = serde_json::from_value(iperf).unwrap() else { panic!("an iperf") };
        assert_eq!(streams, 1, "one stream unless asked");
    }
}
