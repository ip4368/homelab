//! Controllers run on the leader only, once per controller period. Each is
//! a pure function of the replicated state, the leader's volatile memory and
//! the time, returning [`Action`]s for the replica to carry out. They must be
//! idempotent across consecutive runs: the replica drops a proposal that is
//! identical to one still in flight (or that failed to apply within the last
//! [`FAILED_BACKOFF_MS`]), so a controller may keep asking for the same command
//! until the state shows it applied. Controllers first run once the leader
//! has applied an entry of its own term, so they never act on a state that
//! lags what earlier leaders committed.

pub mod health;
pub mod join;
pub mod observe;

use crate::proto::{MetricSample, Msg, Observed};
use crate::runtime::{Addr, Endpoint, SimTime};
use crate::state::{ClusterState, Command};
use model::network::ObservedSwitchPort;
use model::{HostId, NetworkId, SwitchId};
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
    pub last_heartbeat: BTreeMap<HostId, SimTime>,
    pub metrics: BTreeMap<HostId, MetricSample>,
    pub host_reports: BTreeMap<HostId, HostReport>,
    pub switch_reports: BTreeMap<SwitchId, SwitchReading>,
    /// When each switch was last sent a poll.
    pub switch_polled_at: BTreeMap<SwitchId, SimTime>,
    /// Client requests by log index.
    pub pending: BTreeMap<u64, PendingApi>,
    /// Joins by log index.
    pub pending_joins: BTreeMap<u64, PendingJoin>,
    /// When a "join rejected" event was last recorded, per host.
    pub join_rejected_at: BTreeMap<HostId, SimTime>,
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
            host_reports: BTreeMap::new(),
            switch_reports: BTreeMap::new(),
            switch_polled_at: BTreeMap::new(),
            pending: BTreeMap::new(),
            pending_joins: BTreeMap::new(),
            join_rejected_at: BTreeMap::new(),
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

    /// Records that the replica sent `msg` to `to`, for controllers that pace
    /// themselves by when they last sent something.
    pub fn note_sent(&mut self, to: &Endpoint, msg: &Msg, now: SimTime) {
        if let (Addr::Switch(id), Msg::SwitchPoll) = (&to.addr, msg) {
            self.switch_polled_at.insert(id.clone(), now);
        }
    }
}

pub fn run_all(state: &ClusterState, volatile: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = health::run(state, volatile, now);
    out.extend(join::run(state, volatile));
    out.extend(observe::run(state, volatile, now));
    out
}
