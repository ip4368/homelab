//! Messages exchanged between actors, and the API the UI client speaks.
//! Everything travels on the `mgmt` network ([`mgmt`]).

use crate::raft::RaftMsg;
use crate::runtime::SimTime;
use crate::controllers::rollout::{DEFAULT_BATCH_PCT, DEFAULT_SOAK_S};
use crate::state::{Change, Changes, Command, NetworkResult};
use model::network::{LldpNeighbor, ObservedSwitchPort};
use model::{Deployment, HostId, HostNetworkConfig, Inventory, NetworkId, ProfileId, Role, SwitchId, Vm, Volume};
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

/// Where a flow starts or ends.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum FlowEnd {
    Host(HostId),
    /// The upstream router: VM client traffic and transcode streams end here.
    Router,
}

/// What generates a traffic flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowKind {
    Iperf,
    ReplicaWrite,
    Rebuild,
    Migration,
    VmClient,
    Transcode,
    Pod,
}

/// How an instance's CPU and RAM usage moves, as a percentage of what it
/// requested: `base_pct`, plus up to `noise_pct` of smooth seeded noise, plus
/// a short burst once every `burst_every_s` (0 for none).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct UsageProfile {
    pub base_pct: f32,
    pub noise_pct: f32,
    pub burst_every_s: u32,
}

/// A flow an instance keeps up from its host for as long as it runs. Its
/// demand moves along a seeded curve between `min_mbps` and `max_mbps`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowSpec {
    pub kind: FlowKind,
    pub to: FlowEnd,
    pub network: NetworkId,
    pub min_mbps: f64,
    pub max_mbps: f64,
}

/// What a host runs for a pod or VM.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceSpec {
    /// `vm/<id>` or `pod/<deployment>/<idx>`.
    pub id: String,
    pub cpu_m: u32,
    pub ram_mb: u32,
    pub usage: UsageProfile,
    pub flows: Vec<FlowSpec>,
}

/// The instance id a host runs VM `id` under.
pub fn vm_instance(id: &str) -> String {
    format!("vm/{id}")
}

/// The instance id a host runs pod `idx` of `deployment` under.
pub fn pod_instance(deployment: &str, idx: u32) -> String {
    format!("pod/{deployment}/{idx}")
}

/// A volume replica a host holds: `(volume, disk, pct_complete)`.
pub type ReplicaReport = (String, String, u8);

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
    /// Sets, or with `None` clears, the host's hand-written network config.
    SetHostOverride { id: HostId, config: Option<HostNetworkConfig> },
    UpsertRule { id: String, profile: ProfileId, roles: BTreeSet<Role>, template: Option<String> },
    DeleteRule { id: String },
    /// Refused with the objects using it while a template names it.
    DeleteNetwork { id: NetworkId },
    /// Refused with the rules and hosts using it.
    DeleteTemplate { id: String },
    /// What `change` would do; changes nothing. The result is a `Preview`.
    PreviewChange { change: Change },
    /// Applies `change` and rolls it out to the affected hosts in batches of
    /// `batch_pct` percent, soaking `soak_s` seconds after each. Hosts with an
    /// override are skipped unless `include_overridden`. The result is the rollout id.
    StartRollout {
        change: Change,
        include_overridden: bool,
        #[serde(default = "default_batch_pct")]
        batch_pct: u8,
        #[serde(default = "default_soak_s")]
        soak_s: u32,
    },
    PauseRollout { id: String },
    ResumeRollout { id: String },
    /// Gives a rollout up and takes its change back.
    AbortRollout { id: String },
    /// Refused unless `replicas` is 1 to 1000.
    CreateDeployment { deployment: Deployment },
    DeleteDeployment { id: String },
    /// Refused unless its volume exists.
    CreateVm { vm: Vm },
    DeleteVm { id: String },
    /// Refused unless `size_gb` is at least 1 and `replicas` 1 to 5.
    CreateVolume { volume: Volume },
    /// Refused while a VM uses the volume.
    DeleteVolume { id: String },
}

fn default_batch_pct() -> u8 {
    DEFAULT_BATCH_PCT
}

fn default_soak_s() -> u32 {
    DEFAULT_SOAK_S
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
    /// `net_generation` is the network config generation the host runs (0
    /// when the agent does not know, e.g. after a restart). `instances` and
    /// `replicas` are what the host runs and holds now.
    Heartbeat {
        host: HostId,
        lifecycle_seen: u64,
        observed_at: Option<SimTime>,
        observed: Option<Observed>,
        net_generation: u64,
        instances: Vec<String>,
        replicas: Vec<ReplicaReport>,
    },
    /// Leader to agent: send your observation, I don't have the one stamped
    /// in your last heartbeat.
    ReportRequest,
    /// Unjoined agent to the VIP, retried until answered `Ok`.
    JoinRequest { token: String, inventory: Inventory },
    /// Leader to agent: `Ok` once the host object exists.
    JoinResp(Result<(), String>),
    /// Leader to agent: run this network config. The agent keeps its previous
    /// one and puts it back unless confirmed within its revert timeout.
    ApplyNetwork { generation: u64, config: HostNetworkConfig },
    /// Agent to the VIP: what became of `ApplyNetwork { generation }`. An
    /// `Applied` report is sent over the new config, so its arrival proves mgmt works.
    NetworkStatus { generation: u64, result: NetworkResult },
    /// Leader to agent: keep `generation`.
    ConfirmNetwork { generation: u64 },
    /// Leader to agent: run this instance (a repeat of a running one changes nothing).
    StartInstance(InstanceSpec),
    /// Leader to agent: stop the instance with this id.
    StopInstance(String),
    /// Leader to agent: hold a replica of `volume`, copied from the replica on
    /// `source`, or empty (complete at once) when `None`. A repeat for a
    /// replica the host holds changes nothing.
    CreateReplica { volume: String, size_gb: u32, source: Option<HostId> },
    DeleteReplica { volume: String },
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
