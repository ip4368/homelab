//! The objects held in the replicated cluster state. These are the wire
//! format the UI receives (via ts-rs), so every object carries the state
//! `version` at which it last changed. `u64` fields are exported as `number`:
//! simulated time and versions stay far below 2^53.

use crate::runtime::SimTime;
use model::network::{LldpNeighbor, ObservedSwitchPort};
// Defined in `model` (the scheduler filters on it); kept importable from here.
pub use model::Lifecycle;
use model::{
    Deployment, HardwareProfile, HostId, HostNetworkConfig, Inventory, Issue, LogicalNetwork, NetworkId, NetworkTemplate,
    PodStatus, ProfileId, Role, SwitchId, Vm, VmStatus, Volume, VolumeStatus,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostObj {
    pub id: HostId,
    pub profile: HardwareProfile,
    pub inventory: Inventory,
    pub location: String,
    /// The cabling the host's agent last reported over LLDP.
    pub lldp: Vec<LldpNeighbor>,
    pub lifecycle: Lifecycle,
    pub roles: BTreeSet<Role>,
    pub labels: BTreeMap<String, String>,
    /// The lifecycle the host was in when it went `NotReady`; `SetLifecycle`
    /// back to `Ready` restores it. `None` unless the host is `NotReady`.
    pub before_not_ready: Option<Lifecycle>,
    /// The network template picked at provisioning; `None` runs the
    /// profile's bootstrap config.
    pub template: Option<String>,
    /// A hand-written network configuration that replaces the template's.
    pub override_: Option<HostNetworkConfig>,
    /// Static addresses allocated at provisioning, by network.
    pub ips: BTreeMap<NetworkId, Ipv4Addr>,
    pub net: HostNet,
    #[ts(type = "number")]
    pub last_transition: SimTime,
    #[ts(type = "number")]
    pub version: u64,
}

/// Where pushing a host's network config stands, for its desired generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum NetState {
    /// Nothing rendered yet.
    #[default]
    Pending,
    /// Pushed (or about to be); the agent has not confirmed it yet.
    Applying,
    Applied,
    /// The agent could not apply it, and kept its previous config.
    Failed,
    /// The agent applied it, lost the control plane, and put its previous config back.
    Reverted,
}

/// What an agent reports after it was told to apply a network config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkResult {
    /// Applied, and this report came over the new config's mgmt.
    Applied,
    Failed(String),
    Reverted,
}

/// A host's network configuration as the network controller manages it.
/// Each change of the rendered config bumps `desired_generation`; the agent
/// is pushed that generation until it applies it, fails, or reverts it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostNet {
    /// The config the host should run (template or override, with its
    /// addresses); `None` until first rendered.
    pub desired: Option<HostNetworkConfig>,
    #[ts(type = "number")]
    pub desired_generation: u64,
    /// The last generation the agent confirmed applying.
    #[ts(type = "number")]
    pub applied_generation: u64,
    /// The config of `applied_generation`: what the host runs as far as the
    /// control plane knows. `None` until a generation applied.
    pub applied: Option<HostNetworkConfig>,
    pub state: NetState,
    /// Why the desired generation `Failed` or was `Reverted`.
    pub error: Option<String>,
    /// What validating the host's config against the observed cabling and
    /// switch ports found: the desired config, or while that `Failed` or was
    /// `Reverted`, the applied one the host went back to.
    pub issues: Vec<Issue>,
    /// The changes that would fix `issues`, switch-side where possible.
    pub fixes: Vec<String>,
    /// When `issues` or `fixes` last changed: one `NetworkMismatch` incident per value.
    #[ts(type = "number")]
    pub issues_since: SimTime,
    /// The host has an override that differs from its template.
    pub drift: bool,
    /// Roles held inactive, each with the first unavailable network it needs.
    pub held_roles: BTreeMap<Role, NetworkId>,
    /// While a rollout has not reached the host's batch: the desired config
    /// it had when the rollout started, which the network controller keeps
    /// instead of rendering the changed template.
    pub pinned_config: Option<HostNetworkConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct JoinToken {
    pub token: String,
    #[ts(type = "number")]
    pub expires_at: SimTime,
    #[ts(type = "number")]
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AutoApproveRule {
    pub id: String,
    pub profile: ProfileId,
    pub roles: BTreeSet<Role>,
    pub template: Option<String>,
    #[ts(type = "number")]
    pub version: u64,
}

/// The NIC every host's mgmt network rides on: its bootstrap link, and the
/// port whose LLDP neighbour is the host's access switch.
pub const MGMT_NIC: &str = "eth0";

/// A logical network with the state version at which it last changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct NetworkObj {
    #[serde(flatten)]
    #[ts(flatten)]
    pub network: LogicalNetwork,
    #[ts(type = "number")]
    pub version: u64,
}

/// A network template with the state version at which it last changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TemplateObj {
    #[serde(flatten)]
    #[ts(flatten)]
    pub template: NetworkTemplate,
    #[ts(type = "number")]
    pub version: u64,
}

/// A switch as the control plane observed it by polling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SwitchObj {
    pub id: SwitchId,
    pub model: String,
    /// Every port, in port-name order.
    pub ports: Vec<ObservedSwitchPort>,
    pub mlag_peer: Option<SwitchId>,
    /// When the switch last answered a poll, as far as the log knows: only
    /// written when the report changes or this value is a minute old.
    #[ts(type = "number")]
    pub last_seen: SimTime,
    #[ts(type = "number")]
    pub version: u64,
}

/// A deployment with the state version at which it last changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DeploymentObj {
    #[serde(flatten)]
    #[ts(flatten)]
    pub deployment: Deployment,
    #[ts(type = "number")]
    pub version: u64,
}

/// A VM with the state version at which it last changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct VmObj {
    #[serde(flatten)]
    #[ts(flatten)]
    pub vm: Vm,
    #[ts(type = "number")]
    pub version: u64,
}

/// A volume with the state version at which it last changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct VolumeObj {
    #[serde(flatten)]
    #[ts(flatten)]
    pub volume: Volume,
    #[ts(type = "number")]
    pub version: u64,
}

/// One pod of a deployment: `deployment` and `idx` identify it (as `<deployment>/<idx>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct PodObj {
    #[serde(flatten)]
    #[ts(flatten)]
    pub status: PodStatus,
    #[ts(type = "number")]
    pub version: u64,
}

/// Where a VM runs; `id` is the VM's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct VmStatusObj {
    pub id: String,
    #[serde(flatten)]
    #[ts(flatten)]
    pub status: VmStatus,
    #[ts(type = "number")]
    pub version: u64,
}

/// Where a volume's replicas are; `id` is the volume's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct VolumeStatusObj {
    pub id: String,
    #[serde(flatten)]
    #[ts(flatten)]
    pub status: VolumeStatus,
    #[ts(type = "number")]
    pub version: u64,
}

/// An edit to a template or a logical network, as previewed and rolled out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Change {
    Template(NetworkTemplate),
    Network(LogicalNetwork),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum RolloutState {
    /// The current batch is being pushed.
    Running,
    /// The current batch applied everywhere; the next starts at `until`.
    Soaking {
        #[ts(type = "number")]
        until: SimTime,
    },
    /// Stopped by a failure or an unready host in the current batch, or by the user.
    Paused { reason: String },
    Done,
    /// Given up: the change was taken back.
    Aborted,
}

/// A change rolled out to its affected hosts batch by batch: batch `k` is
/// `hosts[k * batch_size..]`, at most `batch_size` of them. The change itself
/// applied when the rollout started; hosts whose batch has not started stay
/// on their `pinned_config`. Skipped hosts (not ready at the start, or when
/// the user resumed past them) stay pinned until the rollout finishes, then
/// render whatever the template or network is by then.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Rollout {
    pub id: String,
    pub change: Change,
    pub hosts: Vec<HostId>,
    pub batch_size: usize,
    pub current_batch: usize,
    pub state: RolloutState,
    /// Hosts of the current batch that failed or reverted the new config.
    pub failed: Vec<HostId>,
    /// Hosts the change concerns that the rollout does not wait for: not ready.
    pub skipped: Vec<HostId>,
    /// The template or network the change replaced; `None` if it created it.
    pub previous: Option<Change>,
    /// The overrides of included overridden hosts, dropped by a template change.
    pub dropped_overrides: BTreeMap<HostId, HostNetworkConfig>,
    #[ts(type = "number")]
    pub soak_ms: u64,
    /// Each current-batch host's desired generation when its batch started
    /// (or was resumed); it is done once it applied a later one.
    #[ts(type = "{ [key: string]: number }")]
    pub base_generation: BTreeMap<HostId, u64>,
    #[ts(type = "number")]
    pub version: u64,
}

impl Rollout {
    pub fn batches(&self) -> usize {
        self.hosts.len().div_ceil(self.batch_size.max(1))
    }

    /// The hosts of batch `k`; empty past the last one.
    pub fn batch(&self, k: usize) -> &[HostId] {
        let size = self.batch_size.max(1);
        let start = (k * size).min(self.hosts.len());
        &self.hosts[start..(start + size).min(self.hosts.len())]
    }

    /// Running, soaking or paused: not yet finished.
    pub fn is_active(&self) -> bool {
        !matches!(self.state, RolloutState::Done | RolloutState::Aborted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Alert {
    pub id: String,
    pub severity: Severity,
    pub kind: String,
    pub entity: ObjectRef,
    pub message: String,
    #[ts(type = "number")]
    pub raised_at: SimTime,
    #[ts(type = "number | null")]
    pub resolved_at: Option<SimTime>,
    /// Set when the alert matches an injected fault; lets the UI score detection.
    pub ground_truth: bool,
    /// An operation the UI offers as a button on the alert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub action: Option<crate::proto::ApiOp>,
    #[ts(type = "number")]
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Event {
    #[ts(type = "number")]
    pub seq: u64,
    #[ts(type = "number")]
    pub at: SimTime,
    pub entity: ObjectRef,
    pub message: String,
    #[ts(type = "number")]
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum CpMode {
    Suggest,
    Auto,
}

/// Control-plane membership: the current Raft voters and how replacements happen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CpInfo {
    pub voters: BTreeSet<HostId>,
    pub mode: CpMode,
    #[ts(type = "number")]
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum ObjectKind {
    Host,
    Token,
    Rule,
    Alert,
    Event,
    Cp,
    Network,
    Template,
    Switch,
    Rollout,
    Deployment,
    Vm,
    Volume,
    Pod,
    VmStatus,
    VolumeStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ObjectRef {
    pub kind: ObjectKind,
    pub id: String,
}

impl ObjectRef {
    pub fn host(id: &HostId) -> Self {
        Self { kind: ObjectKind::Host, id: id.to_string() }
    }
    pub fn template(id: &str) -> Self {
        Self { kind: ObjectKind::Template, id: id.into() }
    }
    pub fn rule(id: &str) -> Self {
        Self { kind: ObjectKind::Rule, id: id.into() }
    }
    pub fn switch(id: &SwitchId) -> Self {
        Self { kind: ObjectKind::Switch, id: id.to_string() }
    }
    pub fn deployment(id: &str) -> Self {
        Self { kind: ObjectKind::Deployment, id: id.into() }
    }
    pub fn vm(id: &str) -> Self {
        Self { kind: ObjectKind::Vm, id: id.into() }
    }
    pub fn volume(id: &str) -> Self {
        Self { kind: ObjectKind::Volume, id: id.into() }
    }
    pub fn pod(deployment: &str, idx: u32) -> Self {
        Self { kind: ObjectKind::Pod, id: PodStatus::key(deployment, idx) }
    }
    pub fn cp() -> Self {
        Self { kind: ObjectKind::Cp, id: "cp".into() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum Object {
    Host(HostObj),
    Token(JoinToken),
    Rule(AutoApproveRule),
    Alert(Alert),
    Event(Event),
    Cp(CpInfo),
    Network(NetworkObj),
    Template(TemplateObj),
    Switch(SwitchObj),
    Rollout(Rollout),
    Deployment(DeploymentObj),
    Vm(VmObj),
    Volume(VolumeObj),
    Pod(PodObj),
    VmStatus(VmStatusObj),
    VolumeStatus(VolumeStatusObj),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Changes {
    #[ts(type = "number")]
    pub version: u64,
    pub upserts: Vec<Object>,
    pub removed: Vec<ObjectRef>,
}
