//! The Raft state machine.
//!
//! Beyond Ongaro §5 it implements three liveness guards:
//! - **Pre-vote:** a node whose election timer fires first asks the voters
//!   whether they would vote for it, and only bumps its term once a quorum
//!   says yes. An isolated node therefore never inflates its term.
//! - **Leader stickiness:** a node that heard from a leader within
//!   `election_min_ms` (or is leader itself) ignores vote requests, so a
//!   rejoining node cannot depose a healthy leader.
//! - **Check-quorum:** a leader that has not heard from a quorum (counting
//!   itself) within `election_min_ms` steps down.

use super::{Durable, DurableOp, Entry, EntryPayload, NotLeader, RaftConfig, RaftMsg, RaftOut};
use crate::runtime::SimTime;
use model::HostId;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug)]
enum Role {
    Follower,
    /// Collecting pre-votes for `term + 1`.
    PreCandidate { votes: BTreeSet<HostId> },
    Candidate { votes: BTreeSet<HostId> },
    Leader(Progress),
}

/// Leader-side view of each peer.
#[derive(Debug)]
struct Progress {
    next_index: BTreeMap<HostId, u64>,
    match_index: BTreeMap<HostId, u64>,
    /// When each peer last answered at the current term (for check-quorum).
    last_ack: BTreeMap<HostId, SimTime>,
    next_heartbeat: SimTime,
}

#[derive(Debug)]
pub struct RaftNode<T> {
    me: HostId,
    cfg: RaftConfig,
    rng: ChaCha8Rng,
    /// Mirrors exactly what the caller has been told to persist.
    durable: Durable<T>,
    /// From the latest `Config` entry in the log, else `durable.voters`.
    voters: BTreeSet<HostId>,
    role: Role,
    leader: Option<HostId>,
    /// When we last accepted an `Append` from the current leader.
    leader_contact: Option<SimTime>,
    /// `None` until the first `tick`/`step` supplies a time.
    election_deadline: Option<SimTime>,
    commit_index: u64,
    /// Highest index handed out by `take_committed`.
    taken_index: u64,
    /// Outputs produced by `propose`, returned by the next `tick`/`step`.
    out: Vec<RaftOut<T>>,
}

impl<T: Clone> RaftNode<T> {
    /// Starts as a follower. Nodes of one cluster need distinct seeds, or
    /// their election timeouts stay in lockstep.
    pub fn new(me: HostId, durable: Durable<T>, cfg: RaftConfig, rand_seed: u64) -> Self {
        let mut node = Self {
            me,
            cfg,
            rng: ChaCha8Rng::seed_from_u64(rand_seed),
            voters: BTreeSet::new(),
            durable,
            role: Role::Follower,
            leader: None,
            leader_contact: None,
            election_deadline: None,
            commit_index: 0,
            taken_index: 0,
            out: Vec::new(),
        };
        node.recompute_voters();
        node
    }

    pub fn tick(&mut self, now: SimTime) -> Vec<RaftOut<T>> {
        if matches!(self.role, Role::Leader(_)) {
            self.leader_tick(now);
        } else {
            if now >= self.election_deadline(now) && self.voters.contains(&self.me) {
                self.start_pre_vote(now);
            }
        }
        std::mem::take(&mut self.out)
    }

    pub fn step(&mut self, now: SimTime, from: HostId, m: RaftMsg<T>) -> Vec<RaftOut<T>> {
        self.election_deadline(now); // arm the timer if this is the first call
        let term = msg_term(&m);
        if term > self.term() {
            match &m {
                // Stickiness: while a leader is fresh, ignore vote requests outright.
                RaftMsg::RequestVote { .. } if self.in_lease(now) => return std::mem::take(&mut self.out),
                // Pre-votes change no state, and a granted pre-vote echoes the
                // term we would campaign in.
                RaftMsg::RequestVote { pre_vote: true, .. } => {}
                RaftMsg::VoteResp { pre_vote: true, granted: true, .. } => {}
                RaftMsg::Append { .. } => self.become_follower(now, term, Some(from.clone())),
                _ => self.become_follower(now, term, None),
            }
        }
        match m {
            RaftMsg::RequestVote { term, last_log_index, last_log_term, pre_vote } => {
                self.on_request_vote(now, from, term, last_log_index, last_log_term, pre_vote)
            }
            RaftMsg::VoteResp { term, granted, pre_vote } => self.on_vote_resp(now, from, term, granted, pre_vote),
            RaftMsg::Append { term, prev_log_index, prev_log_term, entries, leader_commit } => {
                self.on_append(now, from, term, prev_log_index, prev_log_term, entries, leader_commit)
            }
            RaftMsg::AppendResp { term, success, match_index, conflict_index } => {
                self.on_append_resp(now, from, term, success, match_index, conflict_index)
            }
        }
        std::mem::take(&mut self.out)
    }

    /// Appends `t` to the leader's log and returns its index. The resulting
    /// `Persist` and `Send`s come out of the next `tick`/`step`.
    pub fn propose(&mut self, _now: SimTime, t: T) -> Result<u64, NotLeader> {
        if !self.is_leader() {
            return Err(NotLeader { leader_hint: self.leader.clone() });
        }
        let index = self.append_local(EntryPayload::Normal(t));
        self.broadcast_append();
        Ok(index)
    }

    /// Entries committed since the last call, in log order, including `Noop`
    /// and `Config` entries. Apply a call's outputs before calling this.
    pub fn take_committed(&mut self) -> Vec<Entry<T>> {
        let from = self.taken_index as usize;
        let to = self.commit_index as usize;
        self.taken_index = self.commit_index;
        self.durable.log[from..to].to_vec()
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader(_))
    }

    pub fn leader_hint(&self) -> Option<HostId> {
        self.leader.clone()
    }

    /// Whether this node is leader and a quorum (counting itself) has
    /// answered within the check-quorum window. A leader for which this turns
    /// false steps down on its next `tick`.
    pub fn has_quorum(&self, now: SimTime) -> bool {
        let Role::Leader(p) = &self.role else { return false };
        let window = self.cfg.election_min_ms;
        let fresh = 1 + p.last_ack.values().filter(|&&t| now.saturating_sub(t) <= window).count();
        fresh >= self.quorum()
    }

    pub fn term(&self) -> u64 {
        self.durable.term
    }

    pub fn durable(&self) -> &Durable<T> {
        &self.durable
    }

    // ---- timers and role changes ----

    fn draw_timeout(rng: &mut ChaCha8Rng, cfg: &RaftConfig) -> u64 {
        let span = cfg.election_max_ms.saturating_sub(cfg.election_min_ms) + 1;
        cfg.election_min_ms + rng.next_u64() % span
    }

    /// The election deadline, first drawn when a time is first supplied.
    fn election_deadline(&mut self, now: SimTime) -> SimTime {
        *self.election_deadline.get_or_insert_with(|| now + Self::draw_timeout(&mut self.rng, &self.cfg))
    }

    fn reset_election_timer(&mut self, now: SimTime) {
        self.election_deadline = Some(now + Self::draw_timeout(&mut self.rng, &self.cfg));
    }

    /// Whether vote requests should be ignored because a leader is fresh.
    fn in_lease(&self, now: SimTime) -> bool {
        match self.role {
            Role::Leader(_) => true,
            _ => {
                self.leader.is_some()
                    && self.leader_contact.is_some_and(|t| now.saturating_sub(t) < self.cfg.election_min_ms)
            }
        }
    }

    fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    fn peers(&self) -> Vec<HostId> {
        self.voters.iter().filter(|v| **v != self.me).cloned().collect()
    }

    fn become_follower(&mut self, now: SimTime, term: u64, leader: Option<HostId>) {
        if term > self.durable.term {
            self.persist(DurableOp::SetHardState { term, voted_for: None });
        }
        if matches!(self.role, Role::Leader(_)) {
            self.out.push(RaftOut::SteppedDown);
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.reset_election_timer(now);
    }

    fn start_pre_vote(&mut self, now: SimTime) {
        self.role = Role::PreCandidate { votes: BTreeSet::from([self.me.clone()]) };
        self.leader = None;
        self.reset_election_timer(now);
        self.request_votes(true);
        self.check_votes(now);
    }

    fn campaign(&mut self, now: SimTime) {
        let term = self.durable.term + 1;
        self.persist(DurableOp::SetHardState { term, voted_for: Some(self.me.clone()) });
        self.role = Role::Candidate { votes: BTreeSet::from([self.me.clone()]) };
        self.leader = None;
        self.reset_election_timer(now);
        self.request_votes(false);
        self.check_votes(now);
    }

    fn request_votes(&mut self, pre_vote: bool) {
        let term = if pre_vote { self.durable.term + 1 } else { self.durable.term };
        let (last_log_index, last_log_term) = (self.last_index(), self.last_term());
        for peer in self.peers() {
            let m = RaftMsg::RequestVote { term, last_log_index, last_log_term, pre_vote };
            self.out.push(RaftOut::Send(peer, m));
        }
    }

    /// Advances a (pre-)candidate that has a quorum of votes.
    fn check_votes(&mut self, now: SimTime) {
        let won = match &self.role {
            Role::PreCandidate { votes } | Role::Candidate { votes } => votes.len() >= self.quorum(),
            _ => false,
        };
        match self.role {
            Role::PreCandidate { .. } if won => self.campaign(now),
            Role::Candidate { .. } if won => self.become_leader(now),
            _ => {}
        }
    }

    fn become_leader(&mut self, now: SimTime) {
        let peers = self.peers();
        let next = self.last_index() + 1;
        self.role = Role::Leader(Progress {
            next_index: peers.iter().map(|p| (p.clone(), next)).collect(),
            match_index: peers.iter().map(|p| (p.clone(), 0)).collect(),
            // A fresh leader gets a full check-quorum window.
            last_ack: peers.iter().map(|p| (p.clone(), now)).collect(),
            next_heartbeat: now + self.cfg.heartbeat_ms,
        });
        self.leader = Some(self.me.clone());
        self.out.push(RaftOut::BecameLeader(self.durable.term));
        self.append_local(EntryPayload::Noop);
        self.broadcast_append();
        self.advance_commit();
    }

    fn leader_tick(&mut self, now: SimTime) {
        if !self.has_quorum(now) {
            self.become_follower(now, self.durable.term, None);
            return;
        }
        let Role::Leader(p) = &mut self.role else { unreachable!() };
        if now >= p.next_heartbeat {
            p.next_heartbeat = now + self.cfg.heartbeat_ms;
            self.broadcast_append();
        }
        self.advance_commit();
    }

    // ---- message handlers ----

    fn on_request_vote(&mut self, now: SimTime, from: HostId, term: u64, last_index: u64, last_term: u64, pre_vote: bool) {
        let log_ok = (last_term, last_index) >= (self.last_term(), self.last_index());
        let granted = if pre_vote {
            // Requests that arrive while a leader is fresh never get here.
            term > self.durable.term && log_ok
        } else {
            term == self.durable.term
                && log_ok
                && !self.is_leader()
                && self.durable.voted_for.as_ref().is_none_or(|v| *v == from)
        };
        if granted && !pre_vote {
            self.persist(DurableOp::SetHardState { term, voted_for: Some(from.clone()) });
            self.reset_election_timer(now);
        }
        // A granted pre-vote echoes the proposed term; refusals carry ours.
        let term = if granted { term } else { self.durable.term };
        self.out.push(RaftOut::Send(from, RaftMsg::VoteResp { term, granted, pre_vote }));
    }

    fn on_vote_resp(&mut self, now: SimTime, from: HostId, term: u64, granted: bool, pre_vote: bool) {
        if !granted || !self.voters.contains(&from) {
            return;
        }
        let my_term = self.durable.term;
        match &mut self.role {
            Role::PreCandidate { votes } if pre_vote && term == my_term + 1 => {
                votes.insert(from);
            }
            Role::Candidate { votes } if !pre_vote && term == my_term => {
                votes.insert(from);
            }
            _ => return,
        }
        self.check_votes(now);
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append(
        &mut self,
        now: SimTime,
        from: HostId,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry<T>>,
        leader_commit: u64,
    ) {
        let reply = |s: &mut Self, success: bool, match_index: u64, conflict_index: u64| {
            let m = RaftMsg::AppendResp { term: s.durable.term, success, match_index, conflict_index };
            s.out.push(RaftOut::Send(from.clone(), m));
        };
        if term < self.durable.term {
            // A stale leader: our higher term in the reply makes it step down.
            return reply(self, false, 0, 0);
        }
        if self.is_leader() {
            debug_assert!(false, "two leaders in term {term}");
            return;
        }
        if !matches!(self.role, Role::Follower) || self.leader.as_ref() != Some(&from) {
            self.become_follower(now, term, Some(from.clone()));
        }
        self.leader_contact = Some(now);
        self.reset_election_timer(now);

        if prev_log_index > self.last_index() {
            return reply(self, false, 0, self.last_index() + 1);
        }
        if self.term_at(prev_log_index) != prev_log_term {
            // Skip back over the whole conflicting term in one round trip.
            let bad = self.term_at(prev_log_index);
            let first = self.durable.log.iter().position(|e| e.term == bad).map_or(1, |p| p as u64 + 1);
            return reply(self, false, 0, first.max(self.commit_index + 1));
        }
        let last_new = prev_log_index + entries.len() as u64;
        let mut fresh = Vec::new();
        for e in entries {
            if e.index <= self.last_index() {
                if self.term_at(e.index) == e.term {
                    continue;
                }
                debug_assert!(e.index > self.commit_index, "truncating a committed entry");
                self.persist(DurableOp::TruncateFrom(e.index));
            }
            fresh.push(e);
        }
        if !fresh.is_empty() {
            self.persist(DurableOp::Append(fresh));
        }
        self.commit_index = self.commit_index.max(leader_commit.min(last_new));
        reply(self, true, last_new, 0);
    }

    fn on_append_resp(&mut self, now: SimTime, from: HostId, term: u64, success: bool, match_index: u64, conflict_index: u64) {
        let Role::Leader(p) = &mut self.role else { return };
        if term != self.durable.term || !p.next_index.contains_key(&from) {
            return;
        }
        p.last_ack.insert(from.clone(), now);
        let matched = p.match_index[&from];
        let next = p.next_index[&from];
        if success {
            p.match_index.insert(from.clone(), matched.max(match_index));
            p.next_index.insert(from.clone(), next.max(match_index + 1));
            self.advance_commit();
        } else {
            p.next_index.insert(from.clone(), conflict_index.min(next - 1).max(matched + 1));
        }
        let Role::Leader(p) = &self.role else { unreachable!() };
        if p.next_index[&from] <= self.last_index() {
            self.send_append(&from);
        }
    }

    // ---- log ----

    fn last_index(&self) -> u64 {
        self.durable.log.len() as u64
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_index())
    }

    /// Term of the entry at `index`; 0 for index 0.
    fn term_at(&self, index: u64) -> u64 {
        if index == 0 { 0 } else { self.durable.log[index as usize - 1].term }
    }

    fn append_local(&mut self, payload: EntryPayload<T>) -> u64 {
        let index = self.last_index() + 1;
        self.persist(DurableOp::Append(vec![Entry { term: self.durable.term, index, payload }]));
        index
    }

    fn broadcast_append(&mut self) {
        for peer in self.peers() {
            self.send_append(&peer);
        }
    }

    fn send_append(&mut self, peer: &HostId) {
        let Role::Leader(p) = &self.role else { return };
        let Some(&next) = p.next_index.get(peer) else { return };
        let prev_log_index = next - 1;
        let m = RaftMsg::Append {
            term: self.durable.term,
            prev_log_index,
            prev_log_term: self.term_at(prev_log_index),
            entries: self.durable.log[prev_log_index as usize..].to_vec(),
            leader_commit: self.commit_index,
        };
        self.out.push(RaftOut::Send(peer.clone(), m));
    }

    /// Commits the highest current-term index held by a quorum (§5.4.2).
    fn advance_commit(&mut self) {
        let Role::Leader(p) = &self.role else { return };
        let mut matched: Vec<u64> = p.match_index.values().copied().collect();
        matched.push(self.last_index());
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&candidate) = matched.get(self.quorum() - 1) else { return };
        if candidate > self.commit_index && self.term_at(candidate) == self.durable.term {
            self.commit_index = candidate;
        }
    }

    // ---- durability ----

    fn persist(&mut self, op: DurableOp<T>) {
        let voters_changed = match &op {
            DurableOp::Append(es) => es.iter().any(|e| matches!(e.payload, EntryPayload::Config(_))),
            DurableOp::TruncateFrom(_) => true,
            DurableOp::SetHardState { .. } => false,
        };
        self.durable.apply(op.clone());
        if voters_changed {
            self.recompute_voters();
        }
        self.out.push(RaftOut::Persist(op));
    }

    fn recompute_voters(&mut self) {
        self.voters = self
            .durable
            .log
            .iter()
            .rev()
            .find_map(|e| match &e.payload {
                EntryPayload::Config(v) => Some(v.clone()),
                _ => None,
            })
            .unwrap_or_else(|| self.durable.voters.clone());
    }
}

fn msg_term<T>(m: &RaftMsg<T>) -> u64 {
    match m {
        RaftMsg::RequestVote { term, .. }
        | RaftMsg::VoteResp { term, .. }
        | RaftMsg::Append { term, .. }
        | RaftMsg::AppendResp { term, .. } => *term,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{DurableOp, EntryPayload, RaftConfig, RaftMsg, RaftOut};
    use std::collections::{BTreeMap, BTreeSet, VecDeque};

    const STEP_MS: u64 = 10;

    fn hid(i: usize) -> HostId {
        HostId::new(format!("n{i}"))
    }

    fn idx(id: &HostId) -> usize {
        id.as_str()[1..].parse().unwrap()
    }

    /// Nodes exchanging messages instantly through a queue, with manual
    /// time, cuttable links and frozen (crashed or paused) nodes.
    struct Cluster {
        nodes: Vec<RaftNode<u64>>,
        voters: BTreeSet<HostId>,
        now: SimTime,
        cut: BTreeSet<(usize, usize)>,
        /// Frozen nodes neither tick nor send nor receive.
        frozen: BTreeSet<usize>,
        queue: VecDeque<(usize, usize, RaftMsg<u64>)>,
        persisted: Vec<Vec<DurableOp<u64>>>,
        /// Normal payloads each node has taken as committed since it (re)started.
        committed: Vec<Vec<u64>>,
        /// BecameLeader / SteppedDown outputs, with when and who.
        events: Vec<(SimTime, usize, RaftOut<u64>)>,
        leader_of_term: BTreeMap<u64, usize>,
    }

    impl Cluster {
        fn new(n: usize) -> Self {
            let voters: BTreeSet<HostId> = (0..n).map(hid).collect();
            let nodes = (0..n)
                .map(|i| RaftNode::new(hid(i), Durable::new(voters.clone()), RaftConfig::default(), i as u64))
                .collect();
            Self {
                nodes,
                voters,
                now: 0,
                cut: BTreeSet::new(),
                frozen: BTreeSet::new(),
                queue: VecDeque::new(),
                persisted: vec![Vec::new(); n],
                committed: vec![Vec::new(); n],
                events: Vec::new(),
                leader_of_term: BTreeMap::new(),
            }
        }

        fn cut(&mut self, a: usize, b: usize) {
            self.cut.insert((a.min(b), a.max(b)));
        }

        /// Cut every link between `group` and the rest.
        fn isolate(&mut self, group: &[usize]) {
            for &g in group {
                for o in (0..self.nodes.len()).filter(|o| !group.contains(o)) {
                    self.cut(g, o);
                }
            }
        }

        fn heal(&mut self) {
            self.cut.clear();
        }

        fn linked(&self, a: usize, b: usize) -> bool {
            !self.cut.contains(&(a.min(b), a.max(b))) && !self.frozen.contains(&a) && !self.frozen.contains(&b)
        }

        fn handle(&mut self, i: usize, outs: Vec<RaftOut<u64>>) {
            for out in outs {
                match out {
                    RaftOut::Send(to, m) => self.queue.push_back((i, idx(&to), m)),
                    RaftOut::Persist(op) => self.persisted[i].push(op),
                    RaftOut::BecameLeader(term) => {
                        let prev = self.leader_of_term.insert(term, i);
                        assert!(prev.is_none(), "two leaders in term {term}: {prev:?} and {i}");
                        self.events.push((self.now, i, out));
                    }
                    RaftOut::SteppedDown => self.events.push((self.now, i, out)),
                }
            }
            for e in self.nodes[i].take_committed() {
                if let EntryPayload::Normal(v) = e.payload {
                    self.committed[i].push(v);
                }
            }
        }

        fn deliver_all(&mut self) {
            while let Some((from, to, m)) = self.queue.pop_front() {
                if self.linked(from, to) {
                    let outs = self.nodes[to].step(self.now, hid(from), m);
                    self.handle(to, outs);
                }
            }
        }

        fn advance(&mut self) {
            self.now += STEP_MS;
            for i in 0..self.nodes.len() {
                if !self.frozen.contains(&i) {
                    let outs = self.nodes[i].tick(self.now);
                    self.handle(i, outs);
                }
            }
            self.deliver_all();
            self.assert_committed_consistent();
        }

        fn run_for(&mut self, ms: u64) {
            let end = self.now + ms;
            while self.now < end {
                self.advance();
            }
        }

        /// Advances until `pred` holds (true) or `ms` elapse (false).
        fn run_until(&mut self, ms: u64, pred: impl Fn(&Cluster) -> bool) -> bool {
            let end = self.now + ms;
            while self.now < end {
                self.advance();
                if pred(self) {
                    return true;
                }
            }
            false
        }

        fn leaders(&self) -> Vec<usize> {
            (0..self.nodes.len()).filter(|&i| self.nodes[i].is_leader()).collect()
        }

        fn elect(&mut self) -> usize {
            assert!(self.run_until(6000, |c| c.leaders().len() == 1), "no single leader within 6000 ms");
            self.leaders()[0]
        }

        fn propose(&mut self, i: usize, v: u64) -> u64 {
            self.nodes[i].propose(self.now, v).expect("proposer is leader")
        }

        fn ever_committed(&self, v: u64) -> bool {
            self.committed.iter().any(|c| c.contains(&v))
        }

        /// Raft's state-machine safety: committed sequences never diverge.
        fn assert_committed_consistent(&self) {
            for a in &self.committed {
                for b in &self.committed {
                    let n = a.len().min(b.len());
                    assert_eq!(a[..n], b[..n], "committed logs diverge");
                }
            }
        }
    }

    #[test]
    fn elects_single_leader() {
        let mut c = Cluster::new(5);
        assert!(c.run_until(6000, |c| c.leaders().len() == 1));
        let l = c.leaders()[0];
        assert!(c.nodes[l].term() >= 1);
        c.run_for(5000);
        assert_eq!(c.leaders(), vec![l], "a healthy cluster keeps its leader");
        for n in &c.nodes {
            assert_eq!(n.leader_hint(), Some(hid(l)));
        }
    }

    #[test]
    fn replicates_and_commits() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        let indexes: Vec<u64> = [1, 2, 3].into_iter().map(|v| c.propose(l, v)).collect();
        assert!(indexes.windows(2).all(|w| w[1] == w[0] + 1), "indexes {indexes:?}");
        assert!(c.run_until(2000, |c| c.committed.iter().all(|x| x == &[1, 2, 3])), "{:?}", c.committed);
        assert_eq!(c.nodes[(l + 1) % 3].propose(c.now, 4), Err(NotLeader { leader_hint: Some(hid(l)) }));
    }

    #[test]
    fn minority_cannot_commit() {
        let mut c = Cluster::new(5);
        let l = c.elect();
        let f = (l + 1) % 5;
        c.isolate(&[l, f]);
        c.propose(l, 42);
        assert!(
            c.run_until(6000, |c| c.leaders().iter().any(|&i| i != l && i != f)),
            "the majority elects a new leader"
        );
        c.run_for(2000);
        assert!(!c.ever_committed(42));
        let m = c.leaders()[0];
        c.propose(m, 7);
        c.heal();
        assert!(c.run_until(3000, |c| c.committed.iter().all(|x| x == &[7])), "{:?}", c.committed);
        assert!(!c.ever_committed(42), "the minority's entry is overwritten, never committed");
    }

    #[test]
    fn check_quorum_steps_down() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        c.run_for(1000);
        c.isolate(&[l]);
        let t0 = c.now;
        let stepped_down = |c: &Cluster| c.events.iter().any(|(t, i, e)| *t > t0 && *i == l && *e == RaftOut::SteppedDown);
        assert!(c.run_until(1500 + 300, stepped_down), "no SteppedDown within 1800 ms");
        assert!(!c.nodes[l].is_leader());
        assert_eq!(c.nodes[l].propose(c.now, 9), Err(NotLeader { leader_hint: None }));
    }

    #[test]
    fn has_quorum_follows_acks_within_the_window() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        c.run_for(1000);
        assert!(c.nodes[l].has_quorum(c.now));
        assert!((0..3).filter(|&i| i != l).all(|i| !c.nodes[i].has_quorum(c.now)), "followers never have quorum");
        c.isolate(&[l]);
        let t0 = c.now;
        assert!(c.nodes[l].has_quorum(t0 + 1000), "acks from just before the cut are still fresh");
        assert!(!c.nodes[l].has_quorum(t0 + 1500 + STEP_MS + 1), "no acks for a whole window");
    }

    #[test]
    fn restart_from_durable() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        for v in [1, 2, 3] {
            c.propose(l, v);
        }
        assert!(c.run_until(2000, |c| c.committed.iter().all(|x| x == &[1, 2, 3])));

        // Crash the leader; the others carry on and commit 4.
        let before = c.nodes[l].durable().clone();
        c.frozen.insert(l);
        assert!(c.run_until(6000, |c| c.leaders().iter().any(|&i| i != l)));
        let m = *c.leaders().iter().find(|&&i| i != l).unwrap();
        c.propose(m, 4);
        assert!(c.run_until(2000, |c| c.committed[m] == [1, 2, 3, 4]));

        let mut replayed = Durable::new(c.voters.clone());
        for op in c.persisted[l].clone() {
            replayed.apply(op);
        }
        assert_eq!(replayed, before, "replaying Persist ops reproduces the durable state");
        assert!(before.log.len() >= 4, "noop plus three entries");

        c.nodes[l] = RaftNode::new(hid(l), replayed, RaftConfig::default(), 99);
        assert_eq!(c.nodes[l].term(), before.term);
        assert_eq!(c.nodes[l].durable().log, before.log);
        assert!(!c.nodes[l].is_leader(), "a restarted node starts as a follower");
        c.committed[l].clear();
        c.frozen.remove(&l);
        assert!(c.run_until(3000, |c| c.committed.iter().all(|x| x == &[1, 2, 3, 4])), "{:?}", c.committed);
    }

    #[test]
    fn stale_leader_rejected() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        let old_term = c.nodes[l].term();
        // The old leader proposes 99, then pauses; its Appends are held back
        // in the network.
        c.propose(l, 99);
        let outs = c.nodes[l].tick(c.now);
        let (held, rest): (Vec<_>, Vec<_>) = outs.into_iter().partition(|o| matches!(o, RaftOut::Send(..)));
        c.handle(l, rest);
        assert!(!held.is_empty());
        c.frozen.insert(l);

        assert!(c.run_until(6000, |c| c.leaders().iter().any(|&i| i != l)));
        let m = *c.leaders().iter().find(|&&i| i != l).unwrap();
        c.propose(m, 7);
        c.run_for(1000);
        assert!(c.nodes[l].is_leader(), "the paused leader has not noticed yet");

        // The held Appends finally arrive; every reply refuses them.
        for out in held {
            let RaftOut::Send(to, msg) = out else { unreachable!() };
            assert!(matches!(msg, RaftMsg::Append { term, .. } if term == old_term));
            let to = idx(&to);
            let replies = c.nodes[to].step(c.now, hid(l), msg);
            let resp = replies
                .iter()
                .find_map(|o| match o {
                    RaftOut::Send(dst, r @ RaftMsg::AppendResp { .. }) if *dst == hid(l) => Some(r.clone()),
                    _ => None,
                })
                .expect("an AppendResp to the old leader");
            let RaftMsg::AppendResp { term, success, .. } = resp else { unreachable!() };
            assert!(!success && term > old_term, "stale Append must be refused");
            assert!(
                c.nodes[to].durable().log.iter().all(|e| e.payload != EntryPayload::Normal(99)),
                "a refused Append leaves the log untouched"
            );
            let outs = c.nodes[l].step(c.now, hid(to), resp);
            c.handle(l, outs);
        }
        assert!(!c.nodes[l].is_leader());
        assert!(c.events.iter().any(|(_, i, e)| *i == l && *e == RaftOut::SteppedDown));
        assert!(c.nodes[l].term() > old_term);

        c.frozen.remove(&l);
        assert!(c.run_until(3000, |c| c.committed.iter().all(|x| x == &[7])), "{:?}", c.committed);
        assert!(!c.ever_committed(99));
    }

    #[test]
    fn rejoining_node_does_not_disrupt_stable_leader() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        c.run_for(1000);
        let term = c.nodes[l].term();
        let events = c.events.len();
        let r = (l + 1) % 3;
        c.isolate(&[r]);
        c.propose(l, 1);
        c.run_for(10_000);
        c.heal();
        assert!(c.run_until(3000, |c| c.committed[r] == [1]));
        c.run_for(3000);
        assert_eq!(c.leaders(), vec![l]);
        assert_eq!(c.nodes[l].term(), term);
        assert_eq!(c.events.len(), events, "no leadership change");
        assert_eq!(c.nodes[r].term(), term, "the isolated node never inflated its term");
    }

    #[test]
    fn follower_ignores_votes_while_leader_is_fresh() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        c.run_for(500);
        let f = (l + 1) % 3;
        let term = c.nodes[f].term();
        let vote = RaftMsg::RequestVote { term: term + 5, last_log_index: 100, last_log_term: term + 4, pre_vote: false };
        let outs = c.nodes[f].step(c.now, hid((l + 2) % 3), vote);
        assert!(outs.is_empty(), "{outs:?}");
        assert_eq!(c.nodes[f].term(), term);
        assert_eq!(c.nodes[f].leader_hint(), Some(hid(l)));
    }

    #[test]
    fn single_node_cluster_commits_alone() {
        let mut c = Cluster::new(1);
        let l = c.elect();
        c.propose(l, 5);
        assert!(c.run_until(500, |c| c.committed[0] == [5]));
    }
}
