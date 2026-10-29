//! A host made of memory, for running real actors on the real runtime in
//! tests: enough for an agent and a CP replica, with no hardware at all.

use crate::runtime::NodeOs;
use control::proto::LogCmd;
use control::raft::{Durable, DurableOp};
use control::runtime::{HostOs, HostUsage};
use model::network::{HostNetworkConfig, LldpNeighbor};
use model::{HostId, Inventory, ProfileId};
use std::collections::BTreeSet;

/// A host with nothing but memory: enough for an agent and a replica.
pub struct MemOs {
    id: HostId,
    cp: Option<Durable<LogCmd>>,
    net: HostNetworkConfig,
    spawn: Option<BTreeSet<HostId>>,
}

impl HostOs for MemOs {
    fn host_id(&self) -> HostId {
        self.id.clone()
    }
    fn inventory(&self) -> Inventory {
        Inventory::from_profile(ProfileId::Ms01, 0)
    }
    fn lldp(&self) -> Vec<LldpNeighbor> {
        vec![]
    }
    fn apply_network(&mut self, cfg: HostNetworkConfig, _networks: Vec<model::LogicalNetwork>) {
        self.net = cfg;
    }
    fn applied_network(&self) -> HostNetworkConfig {
        self.net.clone()
    }
    fn persist_network(&mut self) {}
    fn usage(&mut self) -> HostUsage {
        HostUsage::default()
    }
    fn load_cp(&self) -> Option<Durable<LogCmd>> {
        self.cp.clone()
    }
    fn store_cp(&mut self, durable: Durable<LogCmd>) {
        self.cp = Some(durable);
    }
    fn persist_cp(&mut self, op: DurableOp<LogCmd>) {
        if let Some(d) = &mut self.cp {
            d.apply(op);
        }
    }
    fn mark_joined(&mut self) {}
    fn spawn_cp_replica(&mut self, voters: BTreeSet<HostId>) {
        if self.cp.is_none() {
            self.cp = Some(Durable::new(voters.clone()));
            self.spawn = Some(voters);
        }
    }
}

impl NodeOs for MemOs {
    fn take_spawn(&mut self) -> Option<BTreeSet<HostId>> {
        self.spawn.take()
    }
}


impl MemOs {
    /// A healthy MS-01 holding `cp` as its stored CP state, if any.
    pub fn new(id: HostId, cp: Option<Durable<LogCmd>>) -> Self {
        Self { id, cp, net: HostNetworkConfig::default(), spawn: None }
    }
}
