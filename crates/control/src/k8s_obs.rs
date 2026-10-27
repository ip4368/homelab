//! Observed K3s/Longhorn state, read-only.
//!
//! The leader's homelabd snapshots the K3s API every 30 s (nodes, pods,
//! deployments, Longhorn nodes/disks, volumes, replicas); the snapshot
//! travels leader → client → UI like metrics do: volatile, never Raft.

use crate::runtime::SimTime;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// One K3s node's status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct K8sNode {
    pub name: String,
    pub ready: bool,
    /// From the `node-role.kubernetes.io/` labels (`control-plane`, `etcd`).
    pub roles: Vec<String>,
    pub version: String,
}

/// One K3s deployment's rollout state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct K8sDeployment {
    pub namespace: String,
    pub name: String,
    #[ts(type = "number")]
    pub ready: u32,
    #[ts(type = "number")]
    pub total: u32,
}

/// One K3s pod's placement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct K8sPod {
    pub namespace: String,
    pub name: String,
    pub node: String,
    pub phase: String,
}

/// One Longhorn disk's usage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LhDisk {
    pub path: String,
    #[ts(type = "number")]
    pub total_bytes: u64,
    #[ts(type = "number")]
    pub avail_bytes: u64,
    #[ts(type = "number")]
    pub replicas: u64,
}

/// One Longhorn node's storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LhNode {
    pub name: String,
    pub schedulable: bool,
    pub disks: Vec<LhDisk>,
}

/// One Longhorn replica's placement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LhReplica {
    pub node: String,
    pub running: bool,
}

/// One Longhorn volume and where its replicas are.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LhVolume {
    pub name: String,
    /// The claiming PVC as `namespace/name`, if it has one.
    pub pvc: Option<String>,
    #[ts(type = "number")]
    pub size_bytes: u64,
    pub robustness: String,
    pub state: String,
    pub replicas: Vec<LhReplica>,
}

/// Everything the UI shows about K3s/Longhorn, as of `at` (wall ms).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct K8sSnapshot {
    #[ts(type = "number")]
    pub at: SimTime,
    pub nodes: Vec<K8sNode>,
    pub deployments: Vec<K8sDeployment>,
    pub pods: Vec<K8sPod>,
    pub lh_nodes: Vec<LhNode>,
    pub volumes: Vec<LhVolume>,
}
