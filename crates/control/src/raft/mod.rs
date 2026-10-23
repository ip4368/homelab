//! Hand-rolled Raft (Ongaro §5) with pre-vote, leader stickiness and
//! check-quorum. [`RaftNode`] is a pure state machine: time arrives only via
//! the `now` arguments, randomness only from its seeded RNG, and every effect
//! (sending, persisting, leadership changes) is returned as a [`RaftOut`] for
//! the caller to carry out.

mod node;

pub use node::RaftNode;

use model::HostId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftConfig {
    pub heartbeat_ms: u64,
    /// Lower bound of the randomized election timeout. Also the check-quorum
    /// window and the leader-stickiness window.
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self { heartbeat_ms: 300, election_min_ms: 1500, election_max_ms: 3000 }
    }
}

/// What a node is doing, as an observer would describe it. A node collecting
/// pre-votes reports `Candidate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum RaftRole {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload<T> {
    Normal(T),
    /// The voter set from this entry onwards.
    Config(BTreeSet<HostId>),
    /// Appended by every new leader so it can commit entries from earlier terms.
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry<T> {
    pub term: u64,
    /// 1-based position in the log.
    pub index: u64,
    pub payload: EntryPayload<T>,
}

/// Everything a node must keep across restarts. Rebuild it by applying the
/// node's [`DurableOp`]s in order to the initial value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Durable<T> {
    pub term: u64,
    pub voted_for: Option<HostId>,
    pub log: Vec<Entry<T>>,
    /// The bootstrap voter set, used until the log holds a `Config` entry.
    pub voters: BTreeSet<HostId>,
}

impl<T> Durable<T> {
    /// A fresh node's state: term 0, no vote, empty log.
    pub fn new(voters: BTreeSet<HostId>) -> Self {
        Self { term: 0, voted_for: None, log: Vec::new(), voters }
    }

    pub fn apply(&mut self, op: DurableOp<T>) {
        match op {
            DurableOp::SetHardState { term, voted_for } => {
                self.term = term;
                self.voted_for = voted_for;
            }
            DurableOp::Append(entries) => {
                for e in entries {
                    debug_assert_eq!(e.index, self.log.len() as u64 + 1, "log append must be contiguous");
                    self.log.push(e);
                }
            }
            DurableOp::TruncateFrom(index) => self.log.truncate(index.saturating_sub(1) as usize),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurableOp<T> {
    SetHardState { term: u64, voted_for: Option<HostId> },
    Append(Vec<Entry<T>>),
    /// Drop the entry at `index` and everything after it.
    TruncateFrom(u64),
}

/// The sender is not in the message; the transport supplies it as `from`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMsg<T> {
    /// With `pre_vote`, `term` is the term the sender would campaign in; the
    /// receiver changes no state.
    RequestVote { term: u64, last_log_index: u64, last_log_term: u64, pre_vote: bool },
    VoteResp { term: u64, granted: bool, pre_vote: bool },
    Append { term: u64, prev_log_index: u64, prev_log_term: u64, entries: Vec<Entry<T>>, leader_commit: u64 },
    /// On success `match_index` is the last index known to match the
    /// leader. On rejection `conflict_index` is where the leader should
    /// resume sending.
    AppendResp { term: u64, success: bool, match_index: u64, conflict_index: u64 },
}

/// Effects for the caller to carry out in order. A `Persist` must be durable
/// before any later `Send` leaves the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftOut<T> {
    Send(HostId, RaftMsg<T>),
    Persist(DurableOp<T>),
    /// This node became leader for the given term.
    BecameLeader(u64),
    /// This node stopped being leader.
    SteppedDown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotLeader {
    pub leader_hint: Option<HostId>,
}
