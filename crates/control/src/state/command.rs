//! Commands replicated through the Raft log, and the errors `apply` can return.

use super::objects::{CpMode, Lifecycle, NetworkResult, ObjectRef, Severity};
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
    RaiseAlert {
        id: String,
        severity: Severity,
        kind: String,
        entity: ObjectRef,
        message: String,
        ground_truth: bool,
    },
    ResolveAlert { id: String },
    Event { entity: ObjectRef, message: String },
    /// Replaces control-plane membership and replacement mode.
    SetCp { voters: BTreeSet<HostId>, mode: CpMode },
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
    AssessNetwork { id: HostId, issues: Vec<Issue>, held_roles: BTreeMap<Role, NetworkId>, drift: bool },
    /// What polling a switch showed; `last_seen` is when it answered.
    /// Creates or replaces the switch object.
    ObserveSwitch {
        id: SwitchId,
        model: String,
        ports: Vec<ObservedSwitchPort>,
        mlag_peer: Option<SwitchId>,
        last_seen: SimTime,
    },
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
        }
    }
}

impl std::error::Error for ApplyError {}
