//! Ground truth: what is really happening in the simulated world, published
//! next to the control plane's view so the UI can show where they disagree.
//! Control logic never reads this.

use crate::engine::Engine;
use crate::topology::{Node, PartitionMember};
use crate::traffic::FlowEnd;
use control::raft::RaftRole;
use control::replica::CpReplica;
use control::runtime::{Addr, Endpoint, SimTime, Svc};
use model::{HostId, NetworkId, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostTruth {
    pub id: HostId,
    pub powered: bool,
    /// The role of the CP replica running on the host, if one is.
    pub cp_role: Option<RaftRole>,
}

/// Throughput through one port at the latest traffic tick, in bits per second.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PortLoad {
    pub port: Node,
    pub rx_bps: f64,
    pub tx_bps: f64,
}

/// A flow on the wire and the rate it got at the latest traffic tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FlowTruth {
    #[ts(type = "number")]
    pub id: u64,
    pub src: FlowEnd,
    pub dst: FlowEnd,
    pub network: NetworkId,
    pub rate_mbps: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct GroundTruth {
    pub hosts: Vec<HostTruth>,
    /// Which host each switch's ARP table says holds the VIP.
    pub vip_owner_by_switch: BTreeMap<SwitchId, HostId>,
    /// The active partition groups.
    pub partitions: Vec<Vec<PartitionMember>>,
    /// VMs running on more than one powered host at once (Task 21).
    pub split_brain_vms: Vec<String>,
    /// Ports that carried traffic at the latest traffic tick.
    pub ports: Vec<PortLoad>,
    pub flows: Vec<FlowTruth>,
    #[ts(type = "number")]
    pub sim_time: SimTime,
}

impl Engine {
    pub fn truth(&self) -> GroundTruth {
        let w = self.fabric().world();
        let hosts = w
            .hosts
            .iter()
            .map(|(id, hw)| HostTruth { id: id.clone(), powered: hw.powered, cp_role: self.replica(id).and_then(CpReplica::raft_role) })
            .collect();
        let traffic = &w.traffic;
        let last = traffic.last();
        let ports = last.port_bps.iter().map(|(port, &(rx_bps, tx_bps))| PortLoad { port: port.clone(), rx_bps, tx_bps });
        let flows = traffic.flows().map(|f| FlowTruth {
            id: f.id,
            src: f.src.clone(),
            dst: f.dst.clone(),
            network: f.network.clone(),
            rate_mbps: last.rate.get(&f.id).copied().unwrap_or(0.0),
        });
        GroundTruth {
            hosts,
            ports: ports.collect(),
            flows: flows.collect(),
            vip_owner_by_switch: self.fabric().vip_arp().clone(),
            partitions: w.topology.partitions.iter().map(|g| g.iter().cloned().collect()).collect(),
            split_brain_vms: vec![],
            sim_time: self.now(),
        }
    }

    /// The real leader: of the running replicas that believe they lead, the
    /// one with the highest term. A deposed leader that has not noticed yet
    /// has a lower term.
    pub fn true_leader(&self) -> Option<HostId> {
        self.fabric()
            .world()
            .hosts
            .keys()
            .filter_map(|id| Some((id, self.replica(id).filter(|r| r.is_leader())?)))
            .max_by_key(|(_, r)| r.durable().term)
            .map(|(id, _)| id.clone())
    }

    fn replica(&self, host: &HostId) -> Option<&CpReplica> {
        self.actor::<CpReplica>(&Endpoint::new(Addr::Host(host.clone()), Svc::Cp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::faults::Fault;
    use crate::presets::FleetSpec;

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    #[test]
    fn truth_reports_power_cp_roles_vip_and_partitions() {
        let mut e = Engine::new(3);
        e.load_fleet(&FleetSpec { ms01: 3, minipc: 1, pi5: 0, rk3588: 0 }, 3);
        assert_eq!(e.true_leader(), None, "nobody leads before the first election");
        assert!(e.run_until(6000, |e| e.true_leader().is_some()));
        let leader = e.true_leader().unwrap();
        e.run_for(1000);

        let t = e.truth();
        assert_eq!(t.sim_time, e.now());
        assert_eq!(t.hosts.len(), 4);
        let role = |t: &GroundTruth, h: &HostId| t.hosts.iter().find(|x| x.id == *h).unwrap().cp_role;
        assert_eq!(role(&t, &leader), Some(RaftRole::Leader));
        let followers = t.hosts.iter().filter(|h| h.cp_role == Some(RaftRole::Follower)).count();
        assert_eq!(followers, 2);
        assert_eq!(role(&t, &hid("minipc-01")), None, "not a voter");
        assert!(t.hosts.iter().all(|h| h.powered));
        let switches: Vec<_> = t.vip_owner_by_switch.keys().map(|s| s.as_str()).collect();
        assert_eq!(switches, ["access-1", "core-1", "core-2"]);
        assert!(t.vip_owner_by_switch.values().all(|h| *h == leader));
        assert!(t.partitions.is_empty() && t.split_brain_vms.is_empty());
        assert!(t.ports.is_empty() && t.flows.is_empty(), "no traffic yet");

        let group = vec![PartitionMember::Host(hid("minipc-01"))];
        e.fault(Fault::Partition { nodes: group.clone() }).unwrap();
        e.fault(Fault::KillHost { host: leader.clone() }).unwrap();
        let t = e.truth();
        assert_eq!(t.partitions, [group]);
        let dead = t.hosts.iter().find(|h| h.id == leader).unwrap();
        assert_eq!((dead.powered, dead.cp_role), (false, None));
        assert_eq!(e.true_leader(), None);
        assert!(e.run_until(e.now() + 6000, |e| e.true_leader().is_some_and(|l| l != leader)));
    }
}
