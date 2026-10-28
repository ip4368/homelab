//! Messages exchanged between actors, and the API the UI client speaks.
//! Everything travels on the `mgmt` network ([`mgmt`]).

use crate::metrics::MetricEntity;
use crate::raft::RaftMsg;
use crate::runtime::SimTime;
use crate::controllers::rollout::{DEFAULT_BATCH_PCT, DEFAULT_SOAK_S};
use crate::state::{Change, Changes, Command, CpMode, NetworkResult};
use ipnet::Ipv4Net;
use model::network::{IpRange, LldpNeighbor, ObservedSwitchPort};
use model::{Deployment, HostId, HostNetworkConfig, Inventory, NetworkId, ProfileId, Role, SwitchId, Vm, Volume};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use ts_rs::TS;

/// A live migration not complete this long after it began fails: the
/// control plane puts the VM back on its source.
pub const MIGRATION_TIMEOUT_MS: u64 = 120_000;
/// A migration's target gives up this long before the control plane does
/// ([`MigrateIn`](Msg::MigrateIn)'s `deadline`), so that a migration that
/// completes is always heard of before the control plane would revert it.
pub const MIGRATION_MARGIN_MS: u64 = 10_000;

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

/// A volume replica a host holds, as its agent reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaReport {
    pub volume: String,
    pub disk: String,
    /// How much of it is copied, 0 to 100.
    pub pct: u8,
    /// How much of it is copied, in bytes: progress too fine to move `pct`
    /// (a large volume, a slow link) still shows here.
    pub copied_bytes: u64,
    /// Whether a copy into it is running. An incomplete replica that is not
    /// copying was interrupted (its host or source lost power) and waits for
    /// a `CreateReplica` naming a source to resume.
    pub copying: bool,
}

/// What a host runs of Kubernetes (K3s).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum KubeRole {
    /// An API server with an embedded etcd member.
    Server,
    Agent,
}

/// What the leader asks a host to run of Kubernetes. The agent installs or
/// reinstalls it until what it runs matches, and reports a [`KubeStatus`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubeSpec {
    pub role: KubeRole,
    /// Start a new Kubernetes cluster instead of joining through the VIP.
    /// Only ever true for the first server, the first time.
    pub init: bool,
    /// The cluster's shared secret (`k3s --token`).
    pub token: String,
    /// The address the node registers with: its static mgmt address.
    pub node_ip: Ipv4Addr,
    /// Node labels: the host's profile and roles, and its own labels.
    pub labels: BTreeMap<String, String>,
    /// The data disks Longhorn keeps replicas on; empty on a host without an
    /// active storage role.
    pub storage_disks: Vec<String>,
    /// The network Longhorn replicates over; `None` keeps its traffic on the
    /// pod network. The same for every host.
    #[serde(default)]
    pub storage_network: Option<StorageNetwork>,
    /// The host's static address on the storage network: the interface that
    /// holds it carries Longhorn's attachment. `None` if it has none.
    #[serde(default)]
    pub storage_ip: Option<Ipv4Addr>,
}

/// The storage network as Longhorn's pods use it: each host attaches them
/// (Multus, macvlan) to its own interface on the network, and they take
/// addresses from `pods`, which IPAM never hands to hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageNetwork {
    pub subnet: Ipv4Net,
    pub pods: IpRange,
}

/// What the leader asks one running K3s server to do for the whole cluster,
/// with its admin access. Each message is done once, as it stands when it
/// arrives; the leader sends another while work remains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubeAdmin {
    /// Hosts whose etcd members go: each is down and no control-plane voter.
    /// The server deletes their nodes, so K3s removes the members, and
    /// removes a member with no node, or that never started, through etcd
    /// itself; never one whose node is `Ready`.
    pub remove_etcd_members: BTreeSet<HostId>,
    /// The mgmt address of each host in `remove_etcd_members`, its member's
    /// peer address: how the server finds a member that never started, and
    /// so has no name.
    #[serde(default)]
    pub etcd_peers: BTreeMap<HostId, Ipv4Addr>,
    /// Hosts whose Longhorn disks take no new replicas: they have no active
    /// storage role. Disks this took from a host not listed are given back.
    pub storage_off: BTreeSet<HostId>,
}

/// How a host's Kubernetes install stands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum KubePhase {
    /// Nothing installed, nothing asked for.
    Absent,
    /// Installing, reinstalling, or waiting for the node to register
    /// ([`KubeStatus::detail`] says which).
    Installing,
    Running,
    /// The last attempt failed (it is retried); why.
    Failed(String),
}

/// What a host's agent reports of its Kubernetes node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct KubeStatus {
    /// What is installed; `None` for nothing.
    pub role: Option<KubeRole>,
    pub phase: KubePhase,
    /// Registered and `Ready` in Kubernetes, as the node sees itself.
    pub node_ready: bool,
    /// The labels and Longhorn disks the node has been given.
    pub labels: BTreeMap<String, String>,
    pub storage_disks: Vec<String>,
    /// The installed K3s version.
    pub version: String,
    /// What an `Installing` host is doing or waiting on. Statuses are kept
    /// in the Raft log, so a field added later must default for old entries.
    #[serde(default)]
    pub detail: Option<String>,
    /// On a server: the etcd members, by node name (the host's id).
    #[serde(default)]
    pub etcd_members: BTreeSet<HostId>,
    /// On a server: why it could not read etcd's members, so that
    /// `etcd_members` says nothing of them.
    #[serde(default)]
    pub etcd_unread: Option<String>,
    /// On a server: the etcd members that have not started, which etcd
    /// names only once they do, by peer address (a K3s server's is its
    /// mgmt address).
    #[serde(default)]
    pub etcd_unstarted: BTreeSet<Ipv4Addr>,
    /// On a server: the hosts whose Longhorn disks it last took out of
    /// scheduling ([`KubeAdmin::storage_off`]).
    #[serde(default)]
    pub storage_off: BTreeSet<HostId>,
}

impl KubeStatus {
    /// Whether the host runs what `spec` asks for.
    pub fn satisfies(&self, spec: &KubeSpec) -> bool {
        self.role == Some(spec.role) && self.phase == KubePhase::Running && self.labels == spec.labels && self.storage_disks == spec.storage_disks
    }
}

/// A user operation, as submitted through the API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum ApiOp {
    /// The leader picks the token and its expiry; the result is the token string.
    CreateJoinToken,
    ApproveHost { id: HostId, roles: BTreeSet<Role> },
    SetRoles { id: HostId, roles: BTreeSet<Role> },
    SetLabels { id: HostId, labels: BTreeMap<String, String> },
    Cordon { id: HostId },
    /// Readies the host again; cancels a drain.
    Uncordon { id: HostId },
    /// Empties the host so it can be serviced: its VMs live-migrate away one
    /// at a time, its pods are rescheduled and its volume replicas move off,
    /// then it is `Drained`.
    Drain { host: HostId },
    /// Live-migrates a running VM to `to`, or with `None` to the host the
    /// scheduler picks. The result is the target host.
    MigrateVm { vm: String, to: Option<HostId> },
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
    /// How dead control-plane members are replaced: suggested or automatic.
    SetCpMode { mode: CpMode },
    /// Replaces down CP voter `dead` with eligible host `with`: `with` joins
    /// as a learner, is promoted once caught up, then `dead` is removed.
    /// Answered once `with` is a learner.
    ReplaceCpMember { dead: HostId, with: HostId },
    /// Reads the leader's metrics history (nothing is written). The result
    /// is `{since, series: {metric: [[t, v], ...]}}`, one series per metric
    /// name in [`crate::metrics::METRICS`]; an unknown metric or entity is an `Err`.
    GetSeries { entity: MetricEntity, metrics: Vec<String> },
    /// Reads an admin kubeconfig for the cluster's Kubernetes, pointed at the
    /// VIP, from the leader's own K3s server (every voter runs one). The
    /// result is the kubeconfig's text; `Err` until the leader's server is up.
    GetKubeconfig,
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
    /// `networks` defines the networks it attaches (VLAN, subnet, MTU), which
    /// a real host needs to build them.
    ApplyNetwork {
        generation: u64,
        config: HostNetworkConfig,
        #[serde(default)]
        networks: Vec<model::LogicalNetwork>,
    },
    /// Agent to the VIP: what became of `ApplyNetwork { generation }`. An
    /// `Applied` report is sent over the new config, so its arrival proves mgmt works.
    NetworkStatus { generation: u64, result: NetworkResult },
    /// Leader to agent: keep `generation`.
    ConfirmNetwork { generation: u64 },
    /// Leader to agent: run this instance (a repeat of a running one changes nothing).
    StartInstance(InstanceSpec),
    /// Leader to agent: stop the instance with this id.
    StopInstance(String),
    /// Leader to the agent of a live migration's target: start an incoming
    /// instance of `spec`, copying its memory from `from` over the
    /// `migration` network (a repeat changes nothing). The incoming instance
    /// is given up, never completed, once the clock reaches `deadline`, or
    /// once `from` no longer runs the VM.
    MigrateIn { spec: InstanceSpec, from: HostId, deadline: SimTime },
    /// Leader to agent: give up the incoming instance with this id, if it is
    /// still migrating in (one that runs already is left alone).
    AbortMigration(String),
    /// Agent to the VIP: the incoming instance `instance` took over from its source.
    MigrationDone { instance: String },
    /// Leader to agent: hold a replica of `volume`, copied from the replica on
    /// `source`, or empty (complete at once) when `None`. A repeat for a
    /// replica the host holds changes nothing.
    CreateReplica { volume: String, size_gb: u32, source: Option<HostId> },
    DeleteReplica { volume: String },
    /// Leader to agent: start a CP replica on this host, empty and knowing
    /// the current `voters`, to catch up as a learner. A host that already
    /// runs one keeps it.
    StartCpReplica { voters: BTreeSet<HostId> },
    /// Leader to every member agent: every member host's static addresses,
    /// by network ([`crate::controllers::directory`]). Control logic ignores
    /// it; a real host's runtime resolves `Addr::Host` with it.
    Directory { hosts: BTreeMap<HostId, BTreeMap<NetworkId, std::net::Ipv4Addr>> },
    /// Agent to the VIP, every metrics interval.
    Metrics { host: HostId, sample: MetricSample },
    /// Leader to agent: run this of Kubernetes (a repeat changes nothing).
    Kube(KubeSpec),
    /// Leader to the agent of a host running a K3s server: do this for the cluster.
    KubeAdmin(KubeAdmin),
    /// Agent to the VIP: how its Kubernetes stands, when that changes and
    /// every [`crate::agent::KUBE_REPORT_MS`] regardless.
    KubeStatus { host: HostId, status: KubeStatus },
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
