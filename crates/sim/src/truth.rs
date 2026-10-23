//! Ground truth: what is really happening in the simulated world, published
//! next to the control plane's view so the UI can show where they disagree.
//! Control logic never reads this.

use crate::engine::Engine;
use crate::topology::{Node, PartitionMember};
use crate::traffic::FlowEnd;
use crate::world::World;
use control::raft::RaftRole;
use control::replica::CpReplica;
use control::runtime::{Addr, Endpoint, SimTime, Svc};
use control::state::{Alert, ObjectRef, Severity};
use model::{HostId, NetworkId, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use ts_rs::TS;

/// The kind of the ground-truth alert for a VM running on two hosts at once.
pub const SPLIT_BRAIN: &str = "VmSplitBrain";
/// How many ground-truth alerts the published truth keeps: every open one,
/// and the latest resolved ones up to this many in all.
pub const TRUTH_ALERTS_KEPT: usize = 100;

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
    /// VMs running on more than one powered host at once.
    pub split_brain_vms: Vec<String>,
    /// Ports that carried traffic at the latest traffic tick.
    pub ports: Vec<PortLoad>,
    pub flows: Vec<FlowTruth>,
    /// Alerts the simulator raised from ground truth (`ground_truth: true`),
    /// which the control plane cannot see: split brain. Open and recently resolved.
    pub alerts: Vec<Alert>,
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
            split_brain_vms: w.split_brain().into_keys().collect(),
            alerts: w.truth_alerts.alerts().to_vec(),
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

impl World {
    /// The VMs whose instance runs on more than one powered host, with those hosts.
    pub fn split_brain(&self) -> BTreeMap<String, Vec<HostId>> {
        let mut on: BTreeMap<String, Vec<HostId>> = BTreeMap::new();
        for (id, hw) in self.hosts.iter().filter(|(_, hw)| hw.powered) {
            // A VM still migrating in does not run there yet.
            let running = hw.instances.iter().filter(|(_, i)| i.incoming.is_none());
            for vm in running.filter_map(|(id, _)| id.strip_prefix("vm/")) {
                on.entry(vm.to_string()).or_default().push(id.clone());
            }
        }
        on.retain(|_, hosts| hosts.len() > 1);
        on
    }

    /// Brings the ground-truth alerts up to date with the world at `now`.
    pub fn track_truth_alerts(&mut self, now: SimTime) {
        let split = self.split_brain();
        self.truth_alerts.track_split_brain(&split, now);
    }
}

/// Alerts raised from ground truth, for the UI to show next to the control
/// plane's (labelled as ground truth). One per incident: a split-brain alert
/// opens when a VM starts running on two powered hosts and resolves when it
/// runs on at most one again.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TruthAlerts {
    alerts: Vec<Alert>,
}

impl TruthAlerts {
    /// Oldest first.
    pub fn alerts(&self) -> &[Alert] {
        &self.alerts
    }

    /// Resolves the split-brain alerts of VMs no longer in `split`, raises
    /// one for each VM in it without an open one, and forgets the oldest
    /// resolved alerts beyond [`TRUTH_ALERTS_KEPT`].
    pub fn track_split_brain(&mut self, split: &BTreeMap<String, Vec<HostId>>, now: SimTime) {
        let open = |a: &Alert| a.kind == SPLIT_BRAIN && a.resolved_at.is_none();
        for a in self.alerts.iter_mut().filter(|a| open(a) && !split.contains_key(&a.entity.id)) {
            a.resolved_at = Some(now);
        }
        for (vm, hosts) in split {
            if self.alerts.iter().any(|a| open(a) && a.entity.id == *vm) {
                continue;
            }
            let hosts: Vec<&str> = hosts.iter().map(HostId::as_str).collect();
            self.alerts.push(Alert {
                id: format!("splitbrain-{vm}-{now}"),
                severity: Severity::Critical,
                kind: SPLIT_BRAIN.into(),
                entity: ObjectRef::vm(vm),
                subject: None,
                message: format!("vm {vm} runs on {} at once", hosts.join(" and ")),
                raised_at: now,
                resolved_at: None,
                ground_truth: true,
                action: None,
                version: 0,
            });
        }
        while self.alerts.len() > TRUTH_ALERTS_KEPT {
            let Some(i) = self.alerts.iter().position(|a| a.resolved_at.is_some()) else { break };
            self.alerts.remove(i);
        }
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
        assert!(t.partitions.is_empty() && t.split_brain_vms.is_empty() && t.alerts.is_empty());
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

    fn spec(id: &str) -> control::proto::InstanceSpec {
        let usage = control::proto::UsageProfile { base_pct: 10.0, noise_pct: 0.0, burst_every_s: 0 };
        control::proto::InstanceSpec { id: id.into(), cpu_m: 1000, ram_mb: 1024, usage, flows: vec![] }
    }

    #[test]
    fn a_vm_on_two_powered_hosts_is_split_brain_with_one_ground_truth_alert() {
        // Hosts with no control plane on them, so nothing stops the instances.
        let (a, b, c) = (hid("ms01-01"), hid("ms01-02"), hid("ms01-03"));
        let mut w = World::new();
        for h in [&a, &b, &c] {
            w.hosts.insert(h.clone(), crate::world::HostHw::new(model::ProfileId::Ms01, 0));
        }
        let mut e = Engine::new(4);
        e.set_deliver(Box::new(crate::fabric::Fabric::new(w, 4)));
        let w = e.fabric_mut().world_mut();
        w.start_instance(&a, spec("vm/x"), 1);
        w.start_instance(&b, spec("pod/web/0"), 1);
        assert!(w.split_brain().is_empty());
        w.start_instance(&b, spec("vm/x"), 1);
        w.start_instance(&c, spec("vm/x"), 1);
        assert_eq!(w.split_brain(), [("x".to_string(), vec![a.clone(), b.clone(), c.clone()])].into());
        e.run_for(1000);
        let t = e.truth();
        assert_eq!(t.split_brain_vms, ["x"]);
        let [alert] = t.alerts.as_slice() else { panic!("{:?}", t.alerts) };
        assert_eq!((alert.kind.as_str(), alert.severity, alert.ground_truth), (SPLIT_BRAIN, Severity::Critical, true));
        assert_eq!((alert.entity.clone(), alert.raised_at, alert.resolved_at), (ObjectRef::vm("x"), 1000, None));
        assert_eq!(alert.message, "vm x runs on ms01-01 and ms01-02 and ms01-03 at once");
        e.run_for(3000);
        assert_eq!(e.truth().alerts.len(), 1, "one alert per incident");

        // A powered-off host runs nothing; one copy left is no split brain.
        e.fault(Fault::KillHost { host: b }).unwrap();
        e.fault(Fault::KillHost { host: c }).unwrap();
        assert!(e.truth().split_brain_vms.is_empty());
        e.run_for(1000);
        let t = e.truth();
        assert_eq!(t.alerts.len(), 1);
        assert_eq!(t.alerts[0].resolved_at, Some(5000));
    }

    #[test]
    fn resolved_truth_alerts_are_capped_open_ones_kept() {
        let mut ta = TruthAlerts::default();
        let split = |vm: &str| -> BTreeMap<String, Vec<HostId>> { [(vm.to_string(), vec![hid("a"), hid("b")])].into() };
        ta.track_split_brain(&split("keep"), 0);
        for n in 1..=TRUTH_ALERTS_KEPT as u64 + 5 {
            let mut s = split(&format!("vm{n}"));
            s.extend(split("keep"));
            ta.track_split_brain(&s, n * 10);
            ta.track_split_brain(&split("keep"), n * 10 + 1);
        }
        assert_eq!(ta.alerts().len(), TRUTH_ALERTS_KEPT);
        assert!(ta.alerts().iter().any(|a| a.entity.id == "keep" && a.resolved_at.is_none()), "the open one stays");
        assert_eq!(ta.alerts().last().unwrap().entity.id, format!("vm{}", TRUTH_ALERTS_KEPT + 5), "the newest are kept");
    }
}
