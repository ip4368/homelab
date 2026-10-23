//! Simulator ground truth: topology, logical networks and host hardware.

use crate::topology::Topology;
use model::network::{HostNetworkConfig, LogicalNetwork, bootstrap_config, seed_networks};
use model::{HardwareProfile, HostId, NetworkId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct HostHw {
    pub profile: HardwareProfile,
    pub powered: bool,
    /// The network config the host OS has actually applied.
    pub applied_net: HostNetworkConfig,
    /// Names of failed disks.
    pub failed_disks: BTreeSet<String>,
}

impl HostHw {
    /// A powered-on host running its bootstrap network config.
    pub fn new(profile: HardwareProfile) -> Self {
        let applied_net = bootstrap_config(&profile);
        Self { profile, powered: true, applied_net, failed_disks: BTreeSet::new() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct World {
    pub topology: Topology,
    /// VLAN and MTU per network, used to place host attachments on the wire.
    pub networks: BTreeMap<NetworkId, LogicalNetwork>,
    pub hosts: BTreeMap<HostId, HostHw>,
}

impl World {
    /// Empty topology, no hosts, the seed networks.
    pub fn new() -> Self {
        Self {
            topology: Topology::new(),
            networks: seed_networks().into_iter().map(|n| (n.id.clone(), n)).collect(),
            hosts: BTreeMap::new(),
        }
    }
}

impl Default for World {
    fn default() -> Self {
        Self::new()
    }
}
