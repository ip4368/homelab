//! Commands replicated through the Raft log, and the errors `apply` can return.

use super::objects::{CpMode, Lifecycle, ObjectRef, Severity};
use model::{HardwareProfile, HostId, HostNetworkConfig, Inventory, LogicalNetwork, NetworkId, NetworkTemplate, ProfileId, Role};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Everything is decided by the proposer (tokens, ids, ...) so that applying a
/// command is a pure function of the state, the log time `at` and the command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    CreateJoinToken { token: String, ttl_ms: u64 },
    UpsertRule { id: String, profile: ProfileId, roles: BTreeSet<Role>, template: Option<String> },
    DeleteRule { id: String },
    AddPendingHost { id: HostId, profile: HardwareProfile, inventory: Inventory, location: String },
    ApproveHost { id: HostId, roles: BTreeSet<Role> },
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
        }
    }
}

impl std::error::Error for ApplyError {}
