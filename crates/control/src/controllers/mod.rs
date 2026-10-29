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
pub mod directory;
pub mod health;
pub mod join;
pub mod kube;
pub mod membership;
pub mod network;
pub mod observe;
pub mod rollout;

use crate::metrics::MetricsStore;
use crate::proto::{KubeStatus, MetricSample, Msg, Observed};
use crate::runtime::{Addr, Endpoint, SimTime};
use crate::state::{ClusterState, Command, NetworkResult};
use model::network::ObservedSwitchPort;
use model::{HostId, NetworkId, PortName, SwitchId};
use std::collections::BTreeMap;

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
    /// When this leader's controllers last ran; `None` before the first run.
    pub last_run: Option<SimTime>,
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
    /// The address directory last sent to the agents, and when.
    pub directory_sent: Option<(directory::Directory, SimTime)>,
    /// Each host's latest Kubernetes status report.
    pub kube_status: BTreeMap<HostId, KubeStatus>,
    /// When each host was last sent its Kubernetes spec.
    pub kube_sent: BTreeMap<HostId, SimTime>,
    /// When a K3s server was last sent work for the cluster ([`crate::proto::KubeAdmin`]).
    pub kube_admin_sent: Option<SimTime>,
    /// When each host was last sent [`Msg::WipeHost`](crate::proto::Msg::WipeHost).
    pub wipe_sent: BTreeMap<HostId, SimTime>,
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
            last_run: None,
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
            directory_sent: None,
            kube_status: BTreeMap::new(),
            kube_sent: BTreeMap::new(),
            kube_admin_sent: None,
            wipe_sent: BTreeMap::new(),
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

    /// A `ForgetHost` for `id` committed: drops this term's executor
    /// reports about it, so a host that rejoins under the id is evicted
    /// and deleted again on its next removal, never skipped on these.
    pub fn note_forget(&mut self, id: &HostId) {
        for s in self.kube_status.values_mut() {
            s.evicted.remove(id);
            s.nodes_deleted.remove(id);
        }
        self.wipe_sent.remove(id);
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
            (Addr::Host(_), Msg::Directory { hosts }) => {
                // One run sends it to every member: record it once, not once per member.
                if self.directory_sent.as_ref().is_none_or(|(sent, at)| *at != now || sent != hosts) {
                    self.directory_sent = Some((hosts.clone(), now));
                }
            }
            (Addr::Host(id), Msg::Kube(_)) => {
                self.kube_sent.insert(id.clone(), now);
            }
            (Addr::Host(_), Msg::KubeAdmin(_)) => {
                self.kube_admin_sent = Some(now);
            }
            (Addr::Host(id), Msg::WipeHost) => {
                self.wipe_sent.insert(id.clone(), now);
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
    out.extend(directory::run(state, volatile, now));
    out.extend(kube::run(state, volatile, now));
    out
}
