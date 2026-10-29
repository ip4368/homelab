//! Commands replicated through the Raft log, and the errors `apply` can return.

use super::objects::{Change, CpMode, Lifecycle, NetworkResult, ObjectRef};
use model::network::{LldpNeighbor, ObservedSwitchPort};
use model::{
    HardwareProfile, HostId, HostNetworkConfig, Inventory, Issue, LogicalNetwork, NetworkId, NetworkTemplate, ProfileId, Role,
    SwitchId,
};
use crate::runtime::SimTime;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::Ipv4Addr;

/// Everything is decided by the proposer (tokens, ids, ...) so that applying a
/// command is a pure function of the state, the log time `at` and the command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    CreateJoinToken { token: String, ttl_ms: u64 },
    UpsertRule { id: String, profile: ProfileId, roles: BTreeSet<Role>, template: Option<String> },
    DeleteRule { id: String },
    AddPendingHost { id: HostId, profile: HardwareProfile, inventory: Inventory, location: String },
    ApproveHost { id: HostId, roles: BTreeSet<Role> },
    /// Moves an `Approved` host to `Provisioning` with its network template
    /// and addresses. Fails with `InUse` if another host holds one of `ips`.
    ProvisionHost { id: HostId, template: Option<String>, ips: BTreeMap<NetworkId, Ipv4Addr> },
    SetLifecycle { id: HostId, lifecycle: Lifecycle },
    SetRoles { id: HostId, roles: BTreeSet<Role> },
    SetLabels { id: HostId, labels: BTreeMap<String, String> },
    RemoveHost { id: HostId },
    /// Raises an alert. `kind` must be an
    /// [`AlertKind`](crate::controllers::alerts::AlertKind), which sets the
    /// severity. Fails with `AlreadyExists` while an alert with the same
    /// `(kind, entity, subject)` is open.
    RaiseAlert {
        id: String,
        kind: String,
        entity: ObjectRef,
        /// What on the entity the alert is about (a port, a link, a disk), if
        /// the entity can have several alerts of the kind at once.
        #[serde(default)]
        subject: Option<String>,
        message: String,
    },
    ResolveAlert { id: String },
    /// Rewords an open alert, e.g. when what is known about its cause changes.
    SetAlertMessage { id: String, message: String },
    Event { entity: ObjectRef, message: String },
    /// Replaces control-plane membership and replacement mode.
    SetCp { voters: BTreeSet<HostId>, mode: CpMode },
    SetCpMode { mode: CpMode },
    /// Raises a `CpReplacementSuggested` alert on `dead` whose button
    /// replaces it with `with`.
    SuggestCpReplacement { id: String, dead: HostId, with: HostId },
    /// Starts handing voter `from`'s place to `to` (the membership
    /// controller does it). Fails with `AlreadyExists` while one is in progress.
    StartCpHandoff { from: HostId, to: HostId },
    /// Cancels the hand-off in progress (the membership controller undoes
    /// it, then ends it). Fails with `NotFound` while none is in progress.
    CancelCpHandoff,
    /// Ends the hand-off in progress, done or given up, as `message` says.
    EndCpHandoff { message: String },
    /// Creates or replaces a network. Changing the VLAN or MTU of one in use
    /// is allowed; the network controller rolls the change out.
    UpsertNetwork { network: LogicalNetwork },
    /// Fails with `InUse` while a template or a host override attaches the network.
    DeleteNetwork { id: NetworkId },
    UpsertTemplate { template: NetworkTemplate },
    /// Fails with `InUse` while an auto-approve rule names the template.
    DeleteTemplate { id: String },
    /// Sets (or with `None` clears) a host's hand-written network configuration.
    SetHostOverride { host: HostId, config: Option<HostNetworkConfig> },
    /// What a host's agent observed: replaces its LLDP neighbours and
    /// inventory. A host whose location is still unknown learns it from them.
    ObserveHost { id: HostId, lldp: Vec<LldpNeighbor>, inventory: Inventory },
    /// A new rendered network config for the host: the next generation, to be pushed.
    SetDesiredNetwork { id: HostId, config: HostNetworkConfig },
    /// The agent's answer for `generation`, which must be the desired one.
    RecordNetworkResult { id: HostId, generation: u64, result: NetworkResult },
    /// The network controller's latest judgement of the host's desired config.
    AssessNetwork { id: HostId, issues: Vec<Issue>, fixes: Vec<String>, held_roles: BTreeMap<Role, NetworkId>, drift: bool },
    /// Applies `change` (as `UpsertTemplate` or `UpsertNetwork` would) and
    /// starts rolling it out, `batch_pct` percent of the hosts it concerns at
    /// a time with a soak of `soak_ms` after each batch. Which hosts those are
    /// is decided here, at apply time (see `ClusterState::start_rollout`).
    /// Fails with `RolloutInProgress` while another rollout is not finished,
    /// and with `InvalidBatchPct` unless `batch_pct` is 1..=100.
    StartRollout { id: String, change: Change, include_overridden: bool, batch_pct: u8, soak_ms: u64 },
    /// Every host of the current batch, `batch`, applied the change: soak.
    SoakRollout { id: String, batch: usize },
    /// The soak after the current batch, `batch`, is over: start the next batch, or finish.
    AdvanceRollout { id: String, batch: usize },
    /// Stops a running or soaking rollout; `failed` are the current batch's
    /// hosts that failed or reverted the change.
    PauseRollout { id: String, reason: String, failed: Vec<HostId> },
    /// Runs a paused rollout's current batch again: its hosts that failed or
    /// reverted are retried with a new generation, and those not ready are skipped.
    ResumeRollout { id: String },
    /// Gives up a rollout that is not finished: puts back the template or
    /// network it replaced and the overrides it dropped, and unpins every
    /// host, so all of them converge on the old config again.
    AbortRollout { id: String },
    /// What polling a switch showed; `last_seen` is when it answered.
    /// Creates or replaces the switch object.
    ObserveSwitch {
        id: SwitchId,
        model: String,
        ports: Vec<ObservedSwitchPort>,
        mlag_peer: Option<SwitchId>,
        last_seen: SimTime,
    },
    /// Sets the cluster's Kubernetes secret and the host whose K3s server
    /// starts the Kubernetes cluster. Fails with `AlreadyExists` once set.
    SetKube { token: String, init_host: HostId },
    /// What a host's agent last reported of its Kubernetes node.
    RecordKube { id: HostId, status: crate::proto::KubeStatus },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// The host is not in a lifecycle phase that allows this command.
    InvalidLifecycle,
    NotFound,
    AlreadyExists,
    IneligibleRole { role: Role, reason: String },
    /// The object is still referenced by these objects.
    InUse(Vec<ObjectRef>),
    /// A result for a network config generation that is no longer the desired one.
    StaleGeneration,
    /// Only one rollout runs at a time; this one is not done.
    RolloutInProgress(String),
    /// The rollout is not in a state (or at a batch) that allows this command.
    InvalidRolloutState,
    /// A rollout batch must be 1 to 100 percent of the hosts.
    InvalidBatchPct(u8),
    /// The object refers to this one, which does not exist.
    MissingDependency(ObjectRef),
    /// A control-plane voter's host cannot be removed; replace the member first.
    CpVoter,
    /// Not one of the [`AlertKind`](crate::controllers::alerts::AlertKind)s.
    UnknownAlertKind(String),
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLifecycle => f.write_str("host lifecycle does not allow this change"),
            Self::NotFound => f.write_str("not found"),
            Self::AlreadyExists => f.write_str("already exists"),
            Self::IneligibleRole { role, reason } => write!(f, "ineligible for role {role:?}: {reason}"),
            Self::InUse(by) => {
                let by: Vec<String> = by.iter().map(|r| format!("{} {}", format!("{:?}", r.kind).to_lowercase(), r.id)).collect();
                write!(f, "in use by {}", by.join(", "))
            }
            Self::StaleGeneration => f.write_str("not the desired network config generation"),
            Self::RolloutInProgress(id) => write!(f, "rollout {id} in progress"),
            Self::InvalidRolloutState => f.write_str("rollout state does not allow this change"),
            Self::InvalidBatchPct(pct) => write!(f, "batch_pct must be 1 to 100, not {pct}"),
            Self::MissingDependency(r) => {
                write!(f, "needs {} {}, which does not exist", format!("{:?}", r.kind).to_lowercase(), r.id)
            }
            Self::CpVoter => f.write_str("host is a control-plane voter; replace it first"),
            Self::UnknownAlertKind(kind) => write!(f, "unknown alert kind {kind}"),
        }
    }
}

impl std::error::Error for ApplyError {}
