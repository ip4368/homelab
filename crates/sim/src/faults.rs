//! Fault injection: what the user (or a test) does to the physical world.
//! Faults change ground truth only; the control plane has to notice.

use crate::engine::Engine;
use crate::topology::{Node, PartitionMember, SwitchPortCfg};
use crate::world::{HostHw, World};
use control::agent::HostAgent;
use control::replica::CpReplica;
use control::runtime::{Addr, Endpoint, Svc};
use model::{HostId, PortName, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Fault {
    /// Powers the host off: its agent, CP replica and workload instances
    /// die, its links go dark. Its disks (stored CP state, volume replicas) survive.
    KillHost { host: HostId },
    /// Powers a killed host back on: it boots its last confirmed network
    /// config, its agent boots as it did originally, and its CP replica
    /// resumes from disk if one ever ran there.
    RestoreHost { host: HostId },
    UnplugPort { host: HostId, port: PortName },
    ReplugPort { host: HostId, port: PortName },
    /// The NIC negotiates `mbps` (e.g. a bad cable); its own speed restores it.
    DegradePort { host: HostId, port: PortName, mbps: u32 },
    KillSwitch { switch: SwitchId },
    RestoreSwitch { switch: SwitchId },
    /// Cuts the listed switches and hosts off from everything else (see
    /// [`crate::topology::Topology::is_cut`]).
    Partition { nodes: Vec<PartitionMember> },
    HealPartitions,
    /// A human editing a switch port's configuration.
    SetSwitchPort { switch: SwitchId, port: PortName, cfg: SwitchPortCfg },
    /// Re-cables the UI client's machine to a free switch port.
    MoveClient { switch: SwitchId, port: PortName },
}

impl Engine {
    /// Applies `f` to the world now. Fails, changing nothing, if it names a
    /// host, switch or port that does not exist (or a port it cannot use).
    pub fn fault(&mut self, f: Fault) -> Result<(), String> {
        match f {
            Fault::KillHost { host } => {
                self.host_hw(&host)?;
                self.fabric_mut().power_off(&host);
                self.kill(&agent(&host));
                self.kill(&cp(&host));
            }
            Fault::RestoreHost { host } => {
                let hw = self.host_hw(&host)?;
                if hw.powered {
                    return Ok(());
                }
                hw.power_on();
                let (token, had_replica) = (hw.join_token.clone(), hw.cp_durable.is_some());
                if had_replica {
                    // The stored state takes precedence over constructor arguments.
                    self.spawn(cp(&host), Box::new(CpReplica::new(host.clone(), None, BTreeSet::new())));
                }
                self.spawn(agent(&host), Box::new(HostAgent::new(token)));
            }
            Fault::UnplugPort { host, port } => self.set_cable(&host, &port, false)?,
            Fault::ReplugPort { host, port } => self.set_cable(&host, &port, true)?,
            Fault::DegradePort { host, port, mbps } => {
                let hw = self.host_hw(&host)?;
                let Some(full) = hw.profile.nics.iter().find(|n| n.name == port.as_str()).map(|n| n.speed_mbps) else {
                    return Err(format!("{host} has no NIC {port}"));
                };
                if !(1..=full).contains(&mbps) {
                    return Err(format!("{host} {port} runs at 1 to {full} Mbps, not {mbps}"));
                }
                if mbps == full {
                    hw.degraded_nics.remove(port.as_str());
                } else {
                    hw.degraded_nics.insert(port.to_string(), mbps);
                }
            }
            Fault::KillSwitch { switch } => self.set_switch_up(&switch, false)?,
            Fault::RestoreSwitch { switch } => self.set_switch_up(&switch, true)?,
            Fault::Partition { nodes } => {
                let w = self.fabric().world();
                if nodes.is_empty() {
                    return Err("a partition needs at least one member".into());
                }
                for n in &nodes {
                    match n {
                        PartitionMember::Host(h) => known_host(w, h)?,
                        PartitionMember::Switch(s) => known_switch(w, s)?,
                    }
                }
                self.fabric_mut().world_mut().topology.partition(nodes.into_iter().collect());
            }
            Fault::HealPartitions => self.fabric_mut().world_mut().topology.heal_partitions(),
            Fault::SetSwitchPort { switch, port, cfg } => {
                known_port(self.fabric().world(), &switch, &port)?;
                self.fabric_mut().world_mut().topology.set_port(&switch, &port, cfg);
            }
            Fault::MoveClient { switch, port } => {
                let w = self.fabric().world();
                known_port(w, &switch, &port)?;
                let to = Node::SwitchPort(switch, port);
                if let Some(other) = w.topology.peer(&to).filter(|n| **n != Node::Client) {
                    return Err(format!("{to:?} is taken by {other:?}"));
                }
                self.fabric_mut().world_mut().topology.connect(Node::Client, to);
            }
        }
        Ok(())
    }

    fn host_hw(&mut self, host: &HostId) -> Result<&mut HostHw, String> {
        self.fabric_mut().host_mut(host).ok_or_else(|| format!("unknown host {host}"))
    }

    fn set_cable(&mut self, host: &HostId, port: &PortName, up: bool) -> Result<(), String> {
        let end = Node::HostPort(host.clone(), port.clone());
        let w = self.fabric().world();
        known_host(w, host)?;
        if w.topology.peer(&end).is_none() {
            return Err(format!("{host} {port} is not cabled"));
        }
        self.fabric_mut().world_mut().topology.set_cable_up(&end, up);
        Ok(())
    }

    fn set_switch_up(&mut self, switch: &SwitchId, up: bool) -> Result<(), String> {
        known_switch(self.fabric().world(), switch)?;
        self.fabric_mut().world_mut().topology.set_switch_up(switch, up);
        Ok(())
    }
}

fn agent(host: &HostId) -> Endpoint {
    Endpoint::new(Addr::Host(host.clone()), Svc::Agent)
}

fn cp(host: &HostId) -> Endpoint {
    Endpoint::new(Addr::Host(host.clone()), Svc::Cp)
}

fn known_host(w: &World, h: &HostId) -> Result<(), String> {
    w.hosts.contains_key(h).then_some(()).ok_or_else(|| format!("unknown host {h}"))
}

fn known_switch(w: &World, s: &SwitchId) -> Result<(), String> {
    w.topology.switches.contains_key(s).then_some(()).ok_or_else(|| format!("unknown switch {s}"))
}

fn known_port(w: &World, s: &SwitchId, p: &PortName) -> Result<(), String> {
    known_switch(w, s)?;
    w.topology.switches[s].ports.contains_key(p).then_some(()).ok_or_else(|| format!("unknown port {s} {p}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::presets::FleetSpec;
    use crate::topology::PortMode;
    use control::client::CLIENT;

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }
    fn sw(s: &str) -> SwitchId {
        SwitchId::from(s)
    }
    fn pn(s: &str) -> PortName {
        PortName::from(s)
    }

    /// One host per profile; ms01-01 is the only CP voter.
    fn engine() -> Engine {
        let mut e = Engine::new(1);
        e.load_fleet(&FleetSpec { ms01: 1, minipc: 1, pi5: 1, rk3588: 1 }, 1);
        e.run_for(5000);
        e
    }

    fn world(e: &Engine) -> &World {
        e.fabric().world()
    }

    #[test]
    fn kill_and_restore_a_cp_host() {
        let mut e = engine();
        let h = hid("ms01-01");
        assert!(e.actor::<CpReplica>(&cp(&h)).unwrap().is_leader(), "a lone voter leads");
        e.fault(Fault::KillHost { host: h.clone() }).unwrap();
        assert!(!world(&e).hosts[&h].powered);
        assert!(e.actor::<CpReplica>(&cp(&h)).is_none() && e.actor::<HostAgent>(&agent(&h)).is_none());
        let disk = world(&e).hosts[&h].cp_durable.clone().expect("the disk survives");

        e.fault(Fault::RestoreHost { host: h.clone() }).unwrap();
        assert!(world(&e).hosts[&h].powered);
        assert!(e.actor::<HostAgent>(&agent(&h)).is_some());
        assert_eq!(e.actor::<CpReplica>(&cp(&h)).unwrap().durable(), &disk);
        e.run_for(5000);
        assert!(e.actor::<CpReplica>(&cp(&h)).unwrap().is_leader(), "leads again");
    }

    #[test]
    fn restore_brings_back_only_what_ran_there() {
        let mut e = engine();
        let h = hid("pi5-01");
        e.fault(Fault::KillHost { host: h.clone() }).unwrap();
        e.fault(Fault::RestoreHost { host: h.clone() }).unwrap();
        assert!(e.actor::<HostAgent>(&agent(&h)).is_some());
        assert!(e.actor::<CpReplica>(&cp(&h)).is_none(), "no replica ever ran on a non-voter");
        assert!(world(&e).hosts[&h].cp_durable.is_none());
    }

    #[test]
    fn port_switch_and_partition_faults_change_the_topology() {
        let mut e = engine();
        let end = Node::HostPort(hid("pi5-01"), pn("eth0"));
        let peer = world(&e).topology.peer(&end).unwrap().clone();
        e.fault(Fault::UnplugPort { host: hid("pi5-01"), port: pn("eth0") }).unwrap();
        assert!(!world(&e).topology.is_cable_up(&end, &peer));
        e.fault(Fault::ReplugPort { host: hid("pi5-01"), port: pn("eth0") }).unwrap();
        assert!(world(&e).topology.is_cable_up(&end, &peer));

        e.fault(Fault::KillSwitch { switch: sw("access-1") }).unwrap();
        assert!(!world(&e).topology.switches[&sw("access-1")].up);
        e.fault(Fault::RestoreSwitch { switch: sw("access-1") }).unwrap();
        assert!(world(&e).topology.switches[&sw("access-1")].up);

        let nodes = vec![PartitionMember::Switch(sw("access-1")), PartitionMember::Host(hid("ms01-01"))];
        e.fault(Fault::Partition { nodes: nodes.clone() }).unwrap();
        assert_eq!(world(&e).topology.partitions, [nodes.into_iter().collect::<BTreeSet<_>>()]);
        e.fault(Fault::HealPartitions).unwrap();
        assert!(world(&e).topology.partitions.is_empty());

        let cfg = SwitchPortCfg { mode: PortMode::Access(20), mtu: 1500, lacp_group: None, admin_up: true };
        e.fault(Fault::SetSwitchPort { switch: sw("access-1"), port: pn("p3"), cfg: cfg.clone() }).unwrap();
        assert_eq!(world(&e).topology.switches[&sw("access-1")].port(&pn("p3")), Some(&cfg));
    }

    #[test]
    fn move_client_recables_it_and_the_client_follows() {
        let mut e = engine();
        e.fault(Fault::MoveClient { switch: sw("access-1"), port: pn("p40") }).unwrap();
        let t = &world(&e).topology;
        assert_eq!(t.peer(&Node::Client), Some(&Node::SwitchPort(sw("access-1"), pn("p40"))));
        assert_eq!(t.peer(&Node::SwitchPort(sw("core-1"), pn("p64"))), None, "the old cable is gone");
        let synced = e.client_view().last_sync_at.unwrap();
        e.run_for(2000);
        assert!(e.client_view().last_sync_at.unwrap() > synced, "the client still reaches the leader");
        assert!(e.actor::<control::client::ClientActor>(&CLIENT).is_some());
    }

    #[test]
    fn degrade_port_sets_the_negotiated_speed_until_set_back() {
        let mut e = engine();
        let h = hid("ms01-01");
        let speed = |e: &Engine| world(e).inventory(&h).unwrap().nics.iter().find(|n| n.name == "sfp0").unwrap().speed_mbps;
        e.fault(Fault::DegradePort { host: h.clone(), port: pn("sfp0"), mbps: 1000 }).unwrap();
        assert_eq!(speed(&e), 1000);
        e.fault(Fault::DegradePort { host: h.clone(), port: pn("sfp0"), mbps: 10_000 }).unwrap();
        assert_eq!(speed(&e), 10_000);
        assert!(world(&e).hosts[&h].degraded_nics.is_empty(), "full speed is no degradation");
    }

    #[test]
    fn bad_targets_are_rejected_without_changes() {
        let mut e = engine();
        let before = world(&e).clone();
        let bad = [
            Fault::KillHost { host: hid("nope") },
            Fault::RestoreHost { host: hid("nope") },
            Fault::UnplugPort { host: hid("ms01-01"), port: pn("eth1") },
            Fault::ReplugPort { host: hid("nope"), port: pn("eth0") },
            Fault::KillSwitch { switch: sw("nope") },
            Fault::Partition { nodes: vec![] },
            Fault::Partition { nodes: vec![PartitionMember::Host(hid("nope"))] },
            Fault::SetSwitchPort { switch: sw("access-1"), port: pn("p99"), cfg: crate::presets::host_port_cfg() },
            Fault::MoveClient { switch: sw("access-1"), port: pn("p1") },
            Fault::DegradePort { host: hid("nope"), port: pn("eth0"), mbps: 100 },
            Fault::DegradePort { host: hid("ms01-01"), port: pn("eth9"), mbps: 100 },
            Fault::DegradePort { host: hid("ms01-01"), port: pn("sfp0"), mbps: 0 },
            Fault::DegradePort { host: hid("ms01-01"), port: pn("sfp0"), mbps: 25_000 },
        ];
        for f in bad {
            assert!(e.fault(f.clone()).is_err(), "{f:?} accepted");
        }
        assert_eq!(world(&e), &before);
    }

    #[test]
    fn faults_are_plain_json() {
        let f = Fault::UnplugPort { host: hid("ms01-01"), port: pn("sfp0") };
        let json = serde_json::to_value(&f).unwrap();
        assert_eq!(json, serde_json::json!({"UnplugPort": {"host": "ms01-01", "port": "sfp0"}}));
        assert_eq!(serde_json::from_value::<Fault>(json).unwrap(), f);
        assert_eq!(serde_json::to_value(Fault::HealPartitions).unwrap(), "HealPartitions");
    }
}
