//! Commands replicated through the Raft log, and the errors `apply` can return.

use super::objects::{CpMode, Lifecycle, ObjectRef, Severity};
use model::{HardwareProfile, HostId, Inventory, ProfileId, Role};
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// The host is not in a lifecycle phase that allows this command.
    InvalidLifecycle,
    NotFound,
    AlreadyExists,
    IneligibleRole { role: Role, reason: String },
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLifecycle => f.write_str("host lifecycle does not allow this change"),
            Self::NotFound => f.write_str("not found"),
            Self::AlreadyExists => f.write_str("already exists"),
            Self::IneligibleRole { role, reason } => write!(f, "ineligible for role {role:?}: {reason}"),
        }
    }
}

impl std::error::Error for ApplyError {}
