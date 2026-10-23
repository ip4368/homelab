//! Workloads the cluster runs: deployments (pods), VMs and replicated
//! volumes, and what is known of where they run.

use crate::ids::{HostId, NetworkId};
use crate::roles::Role;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use ts_rs::TS;

/// Volumes keep this many replicas unless told otherwise.
pub const DEFAULT_VOLUME_REPLICAS: u8 = 3;

/// The most pods a deployment may ask for.
pub const MAX_DEPLOYMENT_REPLICAS: u32 = 1000;
/// The most replicas a volume may ask for.
pub const MAX_VOLUME_REPLICAS: u8 = 5;

/// A deployment whose `capability_prefix` starts with this needs a transcode host.
pub const TRANSCODE_CAPABILITY_PREFIX: &str = "hw-transcode:";

/// Which hosts a deployment's pods may run on. A pod always needs `Compute`;
/// `roles` adds to that, and a transcode capability prefix adds `Transcode`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Selector {
    pub roles: BTreeSet<Role>,
    /// The host must have a capability starting with this.
    pub capability_prefix: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Deployment {
    pub id: String,
    pub image: String,
    pub replicas: u32,
    pub cpu_m: u32,
    pub ram_mb: u32,
    pub selector: Selector,
}

impl Deployment {
    /// The roles a host needs for one of this deployment's pods.
    pub fn required_roles(&self) -> BTreeSet<Role> {
        let mut roles = self.selector.roles.clone();
        roles.insert(Role::Compute);
        if self.selector.capability_prefix.as_deref().is_some_and(|p| p.starts_with(TRANSCODE_CAPABILITY_PREFIX)) {
            roles.insert(Role::Transcode);
        }
        roles
    }

    /// Whether the deployment may be created: 1 to [`MAX_DEPLOYMENT_REPLICAS`] pods.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=MAX_DEPLOYMENT_REPLICAS).contains(&self.replicas) {
            return Err(format!("replicas must be 1 to {MAX_DEPLOYMENT_REPLICAS}, not {}", self.replicas));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Vm {
    pub id: String,
    pub vcpu: u32,
    pub ram_mb: u32,
    pub disk_gb: u32,
    pub networks: Vec<NetworkId>,
    /// The id of the volume holding the VM's disk.
    pub volume: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Volume {
    pub id: String,
    pub size_gb: u32,
    pub replicas: u8,
}

impl Volume {
    /// A volume with the default number of replicas.
    pub fn new(id: impl Into<String>, size_gb: u32) -> Self {
        Self { id: id.into(), size_gb, replicas: DEFAULT_VOLUME_REPLICAS }
    }

    /// Whether the volume may be created: at least 1 GB, 1 to [`MAX_VOLUME_REPLICAS`] replicas.
    pub fn validate(&self) -> Result<(), String> {
        if self.size_gb == 0 {
            return Err("size_gb must be at least 1".into());
        }
        if !(1..=MAX_VOLUME_REPLICAS).contains(&self.replicas) {
            return Err(format!("replicas must be 1 to {MAX_VOLUME_REPLICAS}, not {}", self.replicas));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum PodPhase {
    /// Not running, and why.
    Pending(String),
    Running,
}

/// Where pod `idx` of `deployment` runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PodStatus {
    pub deployment: String,
    pub idx: u32,
    pub host: Option<HostId>,
    pub phase: PodPhase,
}

impl PodStatus {
    /// The id of the pod in the replicated state: `<deployment>/<idx>`.
    pub fn key(deployment: &str, idx: u32) -> String {
        format!("{deployment}/{idx}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum VmPhase {
    /// Not running, and why.
    Pending(String),
    Starting,
    Running,
    Migrating { to: HostId },
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct VmStatus {
    pub host: Option<HostId>,
    pub phase: VmPhase,
    /// Sim time at which `host` or `phase` last changed.
    #[ts(type = "number")]
    pub since: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum ReplicaState {
    /// One of the first replicas of a volume that never held data: created
    /// empty, until its host confirms it. Decided when it was placed.
    Seeding,
    Healthy,
    Rebuilding { pct: u8 },
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ReplicaRef {
    pub host: HostId,
    pub disk: String,
    pub state: ReplicaState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum VolumeHealth {
    Healthy,
    Degraded,
    Rebuilding,
    Faulted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct VolumeStatus {
    pub replicas: Vec<ReplicaRef>,
    pub health: VolumeHealth,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deployment(prefix: Option<&str>, roles: &[Role]) -> Deployment {
        Deployment {
            id: "web".into(),
            image: "nginx".into(),
            replicas: 2,
            cpu_m: 250,
            ram_mb: 256,
            selector: Selector { roles: roles.iter().copied().collect(), capability_prefix: prefix.map(Into::into) },
        }
    }

    #[test]
    fn pods_need_compute_and_transcode_for_hw_transcode() {
        assert_eq!(deployment(None, &[]).required_roles(), [Role::Compute].into());
        assert_eq!(deployment(Some("hw-transcode:"), &[]).required_roles(), [Role::Compute, Role::Transcode].into());
        assert_eq!(deployment(Some("hw-transcode:quicksync"), &[]).required_roles(), [Role::Compute, Role::Transcode].into());
        assert_eq!(deployment(Some("gpu:"), &[Role::Storage]).required_roles(), [Role::Compute, Role::Storage].into());
    }

    #[test]
    fn deployments_need_one_to_a_thousand_replicas() {
        let mut d = deployment(None, &[]);
        assert_eq!(d.validate(), Ok(()));
        for (n, ok) in [(0, false), (1, true), (1000, true), (1001, false)] {
            d.replicas = n;
            assert_eq!(d.validate().is_ok(), ok, "{n}");
        }
        d.replicas = 0;
        assert_eq!(d.validate().unwrap_err(), "replicas must be 1 to 1000, not 0");
    }

    #[test]
    fn volumes_need_a_size_and_one_to_five_replicas() {
        assert_eq!(Volume::new("v", 1).validate(), Ok(()));
        assert_eq!(Volume::new("v", 0).validate().unwrap_err(), "size_gb must be at least 1");
        for (n, ok) in [(0, false), (1, true), (5, true), (6, false)] {
            assert_eq!(Volume { replicas: n, ..Volume::new("v", 10) }.validate().is_ok(), ok, "{n}");
        }
    }

    #[test]
    fn volumes_default_to_three_replicas() {
        assert_eq!(Volume::new("v", 10).replicas, 3);
    }

    #[test]
    fn wire_format_uses_pascal_case_variants() {
        let s = VmStatus { host: Some("h1".into()), phase: VmPhase::Migrating { to: "h2".into() }, since: 5 };
        assert_eq!(serde_json::to_value(&s).unwrap()["phase"], serde_json::json!({"Migrating": {"to": "h2"}}));
        let r = ReplicaRef { host: "h1".into(), disk: "nvme0".into(), state: ReplicaState::Rebuilding { pct: 40 } };
        let back: ReplicaRef = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        assert_eq!(serde_json::to_value(PodPhase::Pending("x".into())).unwrap(), serde_json::json!({"Pending": "x"}));
    }
}
