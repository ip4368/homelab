//! Controllers run on the leader only, once per controller period. Each is
//! a pure function of the replicated state, the leader's volatile memory and
//! the time, returning [`Action`]s for the replica to carry out. They must be
//! idempotent across consecutive runs: the replica drops a proposal that is
//! identical to one still in flight, so a controller may keep asking for the
//! same command until the state shows it applied.

pub mod health;

use crate::proto::{MetricSample, Msg};
use crate::runtime::{Endpoint, SimTime};
use crate::state::{ClusterState, Command};
use model::{HostId, NetworkId};
use std::collections::BTreeMap;

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

/// The leader's memory outside the log. Rebuilt from nothing by every new
/// leader, by design.
#[derive(Debug, Clone, PartialEq)]
pub struct Volatile {
    pub elected_at: SimTime,
    pub last_heartbeat: BTreeMap<HostId, SimTime>,
    pub metrics: BTreeMap<HostId, MetricSample>,
    /// Client requests by log index.
    pub pending: BTreeMap<u64, PendingApi>,
    /// Controller proposals by log index, until they apply.
    pub in_flight: BTreeMap<u64, Command>,
}

impl Volatile {
    pub fn new(elected_at: SimTime) -> Self {
        Self {
            elected_at,
            last_heartbeat: BTreeMap::new(),
            metrics: BTreeMap::new(),
            pending: BTreeMap::new(),
            in_flight: BTreeMap::new(),
        }
    }

    /// When the leader last heard from `host`. A host not heard from since
    /// the election counts as seen at the election, so a new leader gives
    /// every host a full grace period.
    pub fn last_seen(&self, host: &HostId) -> SimTime {
        self.last_heartbeat.get(host).copied().unwrap_or(self.elected_at)
    }
}

pub fn run_all(state: &ClusterState, volatile: &Volatile, now: SimTime) -> Vec<Action> {
    health::run(state, volatile, now)
}
