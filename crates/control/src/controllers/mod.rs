//! Controllers run on the leader only, once per controller period. Each is
//! a pure function of the replicated state, the leader's volatile memory and
//! the time, returning [`Action`]s for the replica to carry out. They must be
//! idempotent across consecutive runs: the replica drops a proposal that is
//! identical to one still in flight (or that failed to apply within the last
//! [`FAILED_BACKOFF_MS`]), so a controller may keep asking for the same command
//! until the state shows it applied. Controllers first run once the leader
//! has applied an entry of its own term, so they never act on a state that
//! lags what earlier leaders committed.

pub mod alerts;
pub mod failover;
pub mod health;
pub mod join;
pub mod membership;
pub mod network;
pub mod observe;
pub mod rollout;
pub mod scheduler;
pub mod storage;

use crate::metrics::MetricsStore;
use crate::proto::{MetricSample, Msg, Observed, ReplicaReport};
use crate::runtime::{Addr, Endpoint, SimTime};
use crate::state::{ClusterState, Command, NetworkResult};
use model::network::ObservedSwitchPort;
use model::{HostId, NetworkId, PortName, SwitchId};
use std::collections::{BTreeMap, BTreeSet};

/// A controller command that applied with an error is not proposed again for this long.
pub const FAILED_BACKOFF_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Propose(Command),
    Send(Endpoint, NetworkId, Msg),
}

/// A client request waiting for its log entry to commit and apply.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingApi {
    pub from: Endpoint,
    pub req: u64,
    /// The term the entry was proposed in; a different term at that index
    /// on commit means the entry was overwritten.
    pub term: u64,
    /// The reply value on success.
    pub ok: serde_json::Value,
    pub proposed_at: SimTime,
}

/// A join waiting for its `AddPendingHost` to commit and apply.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingJoin {
    /// The agent to answer.
    pub from: Endpoint,
    /// As for [`PendingApi::term`].
    pub term: u64,
}

/// The latest observation a host's agent sent this leader.
#[derive(Debug, Clone, PartialEq)]
pub struct HostReport {
    /// The agent's stamp for it, echoed in its heartbeats.
    pub observed_at: SimTime,
    pub observed: Observed,
}

/// What the leader's latest poll of a switch showed.
#[derive(Debug, Clone, PartialEq)]
pub struct SwitchReading {
    /// When the report arrived.
    pub at: SimTime,
    pub model: String,
    pub ports: Vec<ObservedSwitchPort>,
    pub mlag_peer: Option<SwitchId>,
}

/// The leader's memory outside the log. Rebuilt from nothing by every new
/// leader, by design.
#[derive(Debug, Clone, PartialEq)]
pub struct Volatile {
    pub elected_at: SimTime,
    /// The term this leader was elected in.
    pub term: u64,
    /// Whether an entry of `term` (the leader's no-op) has applied, i.e. the
    /// state includes everything committed before this leader.
    pub caught_up: bool,
    pub last_heartbeat: BTreeMap<HostId, SimTime>,
    /// Each host's latest sample.
    pub metrics: BTreeMap<HostId, MetricSample>,
    /// Every host's samples since this leader was elected.
    pub history: MetricsStore,
    pub host_reports: BTreeMap<HostId, HostReport>,
    /// The switch each host port last heard over LLDP, kept after the link
    /// drops: where a port that is down was cabled.
    pub lldp_seen: BTreeMap<HostId, BTreeMap<PortName, SwitchId>>,
    pub switch_reports: BTreeMap<SwitchId, SwitchReading>,
    /// When each switch was last sent a poll.
    pub switch_polled_at: BTreeMap<SwitchId, SimTime>,
    /// The network config generation each host's agent last said it runs.
    pub net_generation: BTreeMap<HostId, u64>,
    /// Each agent's latest report on applying a network config: (generation, result).
    pub net_results: BTreeMap<HostId, (u64, NetworkResult)>,
    /// The network config generation each host was last pushed, and when.
    pub net_pushed: BTreeMap<HostId, (u64, SimTime)>,
    /// Client requests by log index.
    pub pending: BTreeMap<u64, PendingApi>,
    /// Joins by log index.
    pub pending_joins: BTreeMap<u64, PendingJoin>,
    /// When a "join rejected" event was last recorded, per host.
    pub join_rejected_at: BTreeMap<HostId, SimTime>,
    /// The instances each host's agent last said it runs.
    pub instances: BTreeMap<HostId, BTreeSet<String>>,
    /// The volume replicas each host's agent last said it holds.
    pub replicas: BTreeMap<HostId, Vec<ReplicaReport>>,
    /// The host each instance was last sent `StartInstance` to, and when.
    pub instance_started: BTreeMap<String, (HostId, SimTime)>,
    /// When each (volume, host) was last sent `CreateReplica`.
    pub replica_created: BTreeMap<(String, HostId), SimTime>,
    /// When each (host, instance) was last sent `StopInstance`.
    pub instance_stopped: BTreeMap<(HostId, String), SimTime>,
    /// When each (volume, host) was last sent `DeleteReplica`.
    pub replica_deleted: BTreeMap<(String, HostId), SimTime>,
    /// For each copy onto a host, by (volume, host): the percentage its host
    /// last reported, and since when it has stood there. Starts at 0 when the
    /// first `CreateReplica` with a source is sent; dropped once complete.
    pub copy_progress: BTreeMap<(String, HostId), (u8, SimTime)>,
    /// Controller proposals by log index, until they apply.
    pub in_flight: BTreeMap<u64, Command>,
    /// Controller proposals that applied with an error, and when.
    pub failed: Vec<(Command, SimTime)>,
}

impl Volatile {
    pub fn new(elected_at: SimTime, term: u64) -> Self {
        Self {
            elected_at,
            term,
            caught_up: false,
            last_heartbeat: BTreeMap::new(),
            metrics: BTreeMap::new(),
            history: MetricsStore::new(elected_at),
            host_reports: BTreeMap::new(),
            lldp_seen: BTreeMap::new(),
            switch_reports: BTreeMap::new(),
            switch_polled_at: BTreeMap::new(),
            net_generation: BTreeMap::new(),
            net_results: BTreeMap::new(),
            net_pushed: BTreeMap::new(),
            pending: BTreeMap::new(),
            pending_joins: BTreeMap::new(),
            join_rejected_at: BTreeMap::new(),
            instances: BTreeMap::new(),
            replicas: BTreeMap::new(),
            instance_started: BTreeMap::new(),
            replica_created: BTreeMap::new(),
            instance_stopped: BTreeMap::new(),
            replica_deleted: BTreeMap::new(),
            copy_progress: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            failed: Vec::new(),
        }
    }

    /// Whether the replica should propose controller command `cmd` now: not
    /// while an identical proposal is in flight, nor within
    /// [`FAILED_BACKOFF_MS`] of an identical one failing to apply.
    pub fn may_propose(&self, cmd: &Command, now: SimTime) -> bool {
        !self.in_flight.values().any(|c| c == cmd)
            && !self.failed.iter().any(|(c, at)| c == cmd && now < at + FAILED_BACKOFF_MS)
    }

    /// When the leader last heard from `host`. A host not heard from since
    /// the election counts as seen at the election, so a new leader gives
    /// every host a full grace period.
    pub fn last_seen(&self, host: &HostId) -> SimTime {
        self.last_heartbeat.get(host).copied().unwrap_or(self.elected_at)
    }

    /// When the leader last heard from switch `id`. As with hosts, a switch
    /// not heard from since the election counts as seen at the election.
    pub fn switch_last_seen(&self, id: &SwitchId) -> SimTime {
        self.switch_reports.get(id).map_or(self.elected_at, |r| r.at)
    }

    /// Files the observation `host`'s agent sent, noting the switch on the
    /// far end of each port it hears ([`Volatile::lldp_seen`]).
    pub fn note_report(&mut self, host: HostId, report: HostReport) {
        let seen = self.lldp_seen.entry(host.clone()).or_default();
        for n in &report.observed.lldp {
            seen.insert(n.local_port.clone(), n.switch.clone());
        }
        self.host_reports.insert(host, report);
    }

    /// Files a metrics sample from `host`'s agent.
    pub fn note_sample(&mut self, host: HostId, sample: MetricSample) {
        self.history.record(host.clone(), sample.clone());
        self.metrics.insert(host, sample);
    }

    /// Files the replicas `host`'s heartbeat lists, noting when each copy's
    /// reported percentage last moved ([`Volatile::copy_progress`]).
    pub fn note_replicas(&mut self, host: HostId, replicas: Vec<ReplicaReport>, now: SimTime) {
        for r in &replicas {
            let key = (r.volume.clone(), host.clone());
            if r.pct >= 100 {
                self.copy_progress.remove(&key);
            } else if self.copy_progress.get(&key).is_none_or(|(pct, _)| *pct != r.pct) {
                self.copy_progress.insert(key, (r.pct, now));
            }
        }
        self.replicas.insert(host, replicas);
    }

    /// Records that the replica sent `msg` to `to`, for controllers that pace
    /// themselves by when they last sent something.
    pub fn note_sent(&mut self, to: &Endpoint, msg: &Msg, now: SimTime) {
        match (&to.addr, msg) {
            (Addr::Switch(id), Msg::SwitchPoll) => {
                self.switch_polled_at.insert(id.clone(), now);
            }
            (Addr::Host(id), Msg::ApplyNetwork { generation, .. }) => {
                self.net_pushed.insert(id.clone(), (*generation, now));
            }
            (Addr::Host(id), Msg::StartInstance(spec)) => {
                self.instance_started.insert(spec.id.clone(), (id.clone(), now));
            }
            (Addr::Host(id), Msg::CreateReplica { volume, source, .. }) => {
                self.replica_created.insert((volume.clone(), id.clone()), now);
                if source.is_some() {
                    self.copy_progress.entry((volume.clone(), id.clone())).or_insert((0, now));
                }
            }
            (Addr::Host(id), Msg::StopInstance(instance)) => {
                self.instance_stopped.insert((id.clone(), instance.clone()), now);
            }
            (Addr::Host(id), Msg::DeleteReplica { volume }) => {
                self.replica_deleted.insert((volume.clone(), id.clone()), now);
                self.copy_progress.remove(&(volume.clone(), id.clone()));
            }
            _ => {}
        }
    }
}

pub fn run_all(state: &ClusterState, volatile: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = health::run(state, volatile, now);
    out.extend(join::run(state, volatile));
    out.extend(network::run(state, volatile, now));
    out.extend(observe::run(state, volatile, now));
    out.extend(alerts::run(state, volatile));
    out.extend(rollout::run(state, now));
    out.extend(storage::run(state, volatile, now));
    out.extend(scheduler::run(state, volatile, now));
    out.extend(failover::run(state, volatile, now));
    out
}
