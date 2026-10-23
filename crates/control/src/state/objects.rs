//! The objects held in the replicated cluster state. These are the wire
//! format the UI receives (via ts-rs), so every object carries the state
//! `version` at which it last changed. `u64` fields are exported as `number`:
//! simulated time and versions stay far below 2^53.

use crate::runtime::SimTime;
use model::{HardwareProfile, HostId, HostNetworkConfig, Inventory, LogicalNetwork, NetworkTemplate, ProfileId, Role};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Lifecycle {
    Pending,
    Approved,
    Provisioning,
    Ready,
    NotReady,
    Cordoned,
    Draining,
    Drained,
    Removed,
}

impl Lifecycle {
    /// Transitions allowed through `SetLifecycle`. `Pending -> Approved` only
    /// happens through `ApproveHost` and `Removed` only through `RemoveHost`,
    /// so neither appears as a target here.
    pub fn can_become(self, to: Lifecycle) -> bool {
        use Lifecycle::*;
        matches!(
            (self, to),
            (Approved, Provisioning)
                | (Provisioning, Ready)
                | (Ready, NotReady)
                | (Cordoned, NotReady)
                | (Draining, NotReady)
                | (Drained, NotReady)
                | (Ready, Cordoned)
                | (Ready, Draining)
                | (NotReady, Ready)
                | (NotReady, Cordoned)
                | (NotReady, Draining)
                | (Cordoned, Ready)
                | (Cordoned, Draining)
                | (Draining, Drained)
                | (Drained, Ready)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HostObj {
    pub id: HostId,
    pub profile: HardwareProfile,
    pub inventory: Inventory,
    pub location: String,
    pub lifecycle: Lifecycle,
    pub roles: BTreeSet<Role>,
    pub labels: BTreeMap<String, String>,
    /// The lifecycle the host was in when it went `NotReady`; `SetLifecycle`
    /// back to `Ready` restores it. `None` unless the host is `NotReady`.
    pub before_not_ready: Option<Lifecycle>,
    /// A hand-written network configuration that replaces the template's.
    pub override_: Option<HostNetworkConfig>,
    #[ts(type = "number")]
    pub last_transition: SimTime,
    #[ts(type = "number")]
    pub version: u64,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Changes {
    #[ts(type = "number")]
    pub version: u64,
    pub upserts: Vec<Object>,
    pub removed: Vec<ObjectRef>,
}
