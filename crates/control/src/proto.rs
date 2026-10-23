//! Messages exchanged between actors, and the API the UI client speaks.
//! Everything travels on the `mgmt` network ([`mgmt`]).

use crate::raft::RaftMsg;
use crate::runtime::SimTime;
use crate::state::{Changes, Command};
use model::network::{LldpNeighbor, ObservedSwitchPort};
use model::{HostId, Inventory, NetworkId, ProfileId, Role, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ts_rs::TS;

/// The network control traffic rides on.
pub fn mgmt() -> NetworkId {
    NetworkId::from("mgmt")
}

/// A Raft log entry's payload: the command and the leader's clock when it
/// proposed it. Every replica applies `cmd` at `at`, so applying is
/// deterministic and survives replay after a restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogCmd {
    pub at: SimTime,
    pub cmd: Command,
}

/// What a host's agent observes about its own machine: the cabling it hears
/// over LLDP and its hardware inventory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observed {
    pub lldp: Vec<LldpNeighbor>,
    pub inventory: Inventory,
}

/// One host's resource sample, sent by its agent every metrics interval.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MetricSample {
    #[ts(type = "number")]
    pub at: SimTime,
    pub cpu_pct: f32,
    pub ram_pct: f32,
    pub temp_c: f32,
    /// Per disk name.
    pub disk_pct: BTreeMap<String, f32>,
    /// Per port name.
    pub port_rx_bps: BTreeMap<String, f64>,
    pub port_tx_bps: BTreeMap<String, f64>,
    pub instances: u32,
}

/// A user operation, as submitted through the API.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum ApiOp {
    /// The leader picks the token and its expiry; the result is the token string.
    CreateJoinToken,
    ApproveHost { id: HostId, roles: BTreeSet<Role> },
    SetRoles { id: HostId, roles: BTreeSet<Role> },
    SetLabels { id: HostId, labels: BTreeMap<String, String> },
    Cordon { id: HostId },
    Uncordon { id: HostId },
    RemoveHost { id: HostId },
    UpsertRule { id: String, profile: ProfileId, roles: BTreeSet<Role>, template: Option<String> },
    DeleteRule { id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum ApiResult {
    /// Committed and applied.
    Ok(#[ts(type = "unknown")] serde_json::Value),
    /// Committed, but the state machine rejected it.
    Err(String),
    /// The receiving replica is not the leader; safe to retry elsewhere.
    NotLeader,
    /// No quorum: the write was not accepted, or its outcome is unknown.
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Msg {
    Ping(u64),
    Pong(u64),
    /// Between CP replicas, addressed to `Endpoint { Host(peer), Cp }`.
    Raft(RaftMsg<LogCmd>),
    /// Agent to the VIP. `lifecycle_seen` is the host-object version the agent last saw.
    /// `observed_at` is when the agent last saw its observation change (`None` if
    /// it has none); `observed` carries it only on the first heartbeat, after a
    /// change, or when the leader asked ([`Msg::ReportRequest`]).
    Heartbeat { host: HostId, lifecycle_seen: u64, observed_at: Option<SimTime>, observed: Option<Observed> },
    /// Leader to agent: send your observation, I don't have the one stamped
    /// in your last heartbeat.
    ReportRequest,
    /// Unjoined agent to the VIP, retried until answered `Ok`.
    JoinRequest { token: String, inventory: Inventory },
    /// Leader to agent: `Ok` once the host object exists.
    JoinResp(Result<(), String>),
    /// Leader to agent: the host was approved with these roles.
    Approved { roles: BTreeSet<Role> },
    /// Agent to the VIP, every metrics interval.
    Metrics { host: HostId, sample: MetricSample },
    /// Leader to a switch's management interface, every poll interval.
    SwitchPoll,
    /// Switch to the leader: its own configuration.
    SwitchReport { switch: SwitchId, model: String, ports: Vec<ObservedSwitchPort>, mlag_peer: Option<SwitchId> },
    /// Client to the VIP; `req` is the client's id for matching the reply.
    Api { req: u64, op: ApiOp },
    ApiResp { req: u64, result: ApiResult },
    /// Client to the VIP: everything after state version `since`.
    Sync { since: u64 },
    SyncResp { changes: Changes, leader: HostId, term: u64, metrics: Vec<(HostId, MetricSample)> },
}
