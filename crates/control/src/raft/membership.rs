//! Single-server membership changes (Ongaro §4.1). A new member joins as a
//! learner: it is sent the log but neither votes nor counts towards a
//! quorum. Once its log is within [`CATCH_UP_ENTRIES`] of the leader's it can
//! be promoted to voter. Each change adds or removes one voter, so any old
//! and new majority overlap, and only one change may be uncommitted at a
//! time. A change takes effect on each node as soon as it is appended.
//!
//! A leader also refuses a change until it has committed an entry of its own
//! term, which closes the single-server-change bug Ongaro described on
//! raft-dev in 2015 (a new leader must not stack a change on an uncommitted
//! one from an earlier term).

use super::{NotLeader, RaftNode};
use model::HostId;
use std::fmt;

/// A learner is promoted once its log is at most this many entries behind
/// the leader's.
pub const CATCH_UP_ENTRIES: u64 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipError {
    NotLeader(NotLeader),
    /// A config change is not yet committed, or this leader has not yet
    /// committed an entry of its own term.
    ChangePending,
    /// The host already votes or learns.
    AlreadyMember,
    /// The host neither votes nor learns.
    NotMember,
    /// Only a learner can be promoted.
    NotLearner,
    /// The learner's log is more than [`CATCH_UP_ENTRIES`] behind.
    NotCaughtUp,
    /// The only voter cannot be removed.
    LastVoter,
    /// The learner has not answered within the check-quorum window.
    Unresponsive,
    /// The voters known to be up plus the learner would not make a quorum
    /// of the larger voter set.
    WouldLoseQuorum,
}

impl fmt::Display for MembershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            MembershipError::NotLeader(_) => "not the leader",
            MembershipError::ChangePending => "a membership change is still in progress",
            MembershipError::AlreadyMember => "already a member",
            MembershipError::NotMember => "not a member",
            MembershipError::NotLearner => "not a learner",
            MembershipError::NotCaughtUp => "the learner has not caught up",
            MembershipError::LastVoter => "the last voter cannot be removed",
            MembershipError::Unresponsive => "the learner has not answered recently",
            MembershipError::WouldLoseQuorum => "too few voters are up to grow the voter set safely",
        };
        f.write_str(s)
    }
}

/// One single-server change, as a controller asks for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberChange {
    AddLearner(HostId),
    Promote(HostId),
    Remove(HostId),
}

impl MemberChange {
    pub fn apply<T: Clone>(&self, raft: &mut RaftNode<T>) -> Result<u64, MembershipError> {
        match self {
            MemberChange::AddLearner(id) => raft.add_learner(id.clone()),
            MemberChange::Promote(id) => raft.promote(id.clone()),
            MemberChange::Remove(id) => raft.remove(id.clone()),
        }
    }
}

impl<T: Clone> RaftNode<T> {
    /// Appends a config that adds `id` as a learner and returns its index.
    /// As with `propose`, the effects come out of the next `tick`/`step`.
    pub fn add_learner(&mut self, id: HostId) -> Result<u64, MembershipError> {
        self.may_change()?;
        if self.voters().contains(&id) || self.learners().contains(&id) {
            return Err(MembershipError::AlreadyMember);
        }
        let (voters, mut learners) = (self.voters().clone(), self.learners().clone());
        learners.insert(id);
        Ok(self.append_config(voters, learners))
    }

    /// Appends a config that makes learner `id` a voter. The learner must
    /// have caught up and answered within the check-quorum window, and the
    /// voters known to be up plus the learner must make a quorum of the new
    /// voter set (as etcd's strict reconfiguration check): promoting a
    /// learner that has since died must not leave the cluster without one.
    pub fn promote(&mut self, id: HostId) -> Result<u64, MembershipError> {
        self.may_change()?;
        if !self.learners().contains(&id) {
            return Err(MembershipError::NotLearner);
        }
        if !self.caught_up(&id) {
            return Err(MembershipError::NotCaughtUp);
        }
        if !self.acked_recently(&id) {
            return Err(MembershipError::Unresponsive);
        }
        if self.responsive_voters() + 1 < (self.voters().len() + 1) / 2 + 1 {
            return Err(MembershipError::WouldLoseQuorum);
        }
        let (mut voters, mut learners) = (self.voters().clone(), self.learners().clone());
        learners.remove(&id);
        voters.insert(id);
        Ok(self.append_config(voters, learners))
    }

    /// Appends a config without voter or learner `id`. A leader that removes
    /// itself keeps leading, without counting itself, until the change
    /// commits, then steps down.
    pub fn remove(&mut self, id: HostId) -> Result<u64, MembershipError> {
        self.may_change()?;
        let (mut voters, mut learners) = (self.voters().clone(), self.learners().clone());
        if !voters.remove(&id) && !learners.remove(&id) {
            return Err(MembershipError::NotMember);
        }
        if voters.is_empty() {
            return Err(MembershipError::LastVoter);
        }
        Ok(self.append_config(voters, learners))
    }

    /// Whether the latest config entry in the log is not yet known committed.
    pub fn config_pending(&self) -> bool {
        self.config_index() > self.commit_index()
    }

    /// Whether this leader holds `id`'s log as at most [`CATCH_UP_ENTRIES`]
    /// behind its own. Always false on a follower. Promotion also needs the
    /// learner to be responsive ([`RaftNode::responsive`]).
    pub fn caught_up(&self, id: &HostId) -> bool {
        self.match_index_of(id).is_some_and(|m| m + CATCH_UP_ENTRIES >= self.last_log_index())
    }

    /// Whether this leader heard from `id` within the check-quorum window.
    pub fn responsive(&self, id: &HostId) -> bool {
        self.acked_recently(id)
    }

    fn may_change(&self) -> Result<(), MembershipError> {
        if !self.is_leader() {
            return Err(MembershipError::NotLeader(NotLeader { leader_hint: self.leader_hint() }));
        }
        if self.config_pending() || !self.committed_in_term() {
            return Err(MembershipError::ChangePending);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::node::tests::{hid, Cluster};
    use crate::raft::RaftOut;
    use std::collections::BTreeSet;

    /// The harness's time step.
    const STEP: u64 = 10;

    fn set(ids: &[usize]) -> BTreeSet<HostId> {
        ids.iter().map(|&i| hid(i)).collect()
    }

    /// A 3-voter cluster with a leader whose no-op committed, plus node 3
    /// started empty and added as a learner that has caught up.
    fn with_caught_up_learner() -> (Cluster, usize, usize) {
        let mut c = Cluster::new(3);
        let l = c.elect();
        c.run_for(500);
        let n = c.add_node();
        let index = c.nodes[l].add_learner(hid(n)).expect("leader adds a learner");
        assert!(c.run_until(2000, |c| c.nodes[n].commit_index() >= index), "the learner never caught up");
        assert!(c.nodes[l].caught_up(&hid(n)));
        (c, l, n)
    }

    #[test]
    fn learner_doesnt_vote() {
        let (mut c, l, n) = with_caught_up_learner();
        for node in &c.nodes {
            assert_eq!((node.voters(), node.learners()), (&set(&[0, 1, 2]), &set(&[n])));
        }

        // Cut off, a learner never campaigns.
        let term = c.nodes[n].term();
        c.isolate(&[n]);
        c.run_for(10_000);
        assert_eq!((c.nodes[n].role(), c.nodes[n].term()), (crate::raft::RaftRole::Follower, term));
        c.heal();
        c.run_for(1000);
        assert_eq!(c.leaders(), vec![l]);

        // Its acks neither commit entries nor keep the leader's quorum.
        let others: Vec<usize> = (0..3).filter(|&i| i != l).collect();
        c.frozen.extend(others);
        c.propose(l, 42);
        let t0 = c.now;
        let stepped_down = |c: &Cluster| c.events.iter().any(|(t, i, e)| *t > t0 && *i == l && *e == RaftOut::SteppedDown);
        assert!(c.run_until(1500 + 300, stepped_down), "a leader with only a learner must step down");
        assert!(!c.ever_committed(42), "a learner's ack committed an entry");
    }

    #[test]
    fn promote_then_remove_keeps_quorum() {
        let (mut c, l, n) = with_caught_up_learner();
        let index = c.nodes[l].promote(hid(n)).expect("a caught-up learner is promoted");
        assert_eq!(c.nodes[l].voters(), &set(&[0, 1, 2, n]), "a change takes effect on append");
        assert!(c.run_until(2000, |c| c.nodes.iter().all(|x| x.commit_index() >= index)));
        assert!(c.nodes.iter().all(|x| x.voters().len() == 4 && x.learners().is_empty()));

        // Remove a follower; the removed node is then gone for good.
        let f = (0..3).find(|&i| i != l).unwrap();
        let index = c.nodes[l].remove(hid(f)).expect("removes a voter");
        let live: Vec<usize> = (0..4).filter(|&i| i != f).collect();
        assert!(c.run_until(2000, |c| live.iter().all(|&i| c.nodes[i].commit_index() >= index)));
        c.frozen.insert(f);
        let expected: BTreeSet<HostId> = live.iter().map(|&i| hid(i)).collect();
        assert!(live.iter().all(|&i| c.nodes[i].voters() == &expected));

        // Three voters, one of them frozen: the promoted node completes the quorum.
        let other = *live.iter().find(|&&i| i != l && i != n).unwrap();
        c.frozen.insert(other);
        c.propose(l, 7);
        assert!(c.run_until(2000, |c| c.committed[n].contains(&7)), "no commit with the promoted voter");
        c.run_for(3000);
        assert_eq!(c.leaders(), vec![l], "the leader keeps its quorum with the promoted voter");
    }

    #[test]
    fn second_change_rejected_while_pending() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        let f = (l + 1) % 3;
        assert_eq!(c.nodes[f].add_learner(hid(5)), Err(MembershipError::NotLeader(NotLeader { leader_hint: Some(hid(l)) })));
        c.run_for(500);

        let n = c.add_node();
        let index = c.nodes[l].add_learner(hid(n)).unwrap();
        assert!(c.nodes[l].config_pending());
        assert_eq!(c.nodes[l].add_learner(hid(9)), Err(MembershipError::ChangePending));
        assert_eq!(c.nodes[l].remove(hid(f)), Err(MembershipError::ChangePending));
        assert_eq!(c.nodes[l].promote(hid(n)), Err(MembershipError::ChangePending));

        assert!(c.run_until(2000, |c| c.nodes[l].commit_index() >= index));
        assert!(!c.nodes[l].config_pending());
        assert_eq!(c.nodes[l].add_learner(hid(f)), Err(MembershipError::AlreadyMember));
        assert_eq!(c.nodes[l].add_learner(hid(n)), Err(MembershipError::AlreadyMember));
        assert_eq!(c.nodes[l].promote(hid(f)), Err(MembershipError::NotLearner));
        assert_eq!(c.nodes[l].remove(hid(9)), Err(MembershipError::NotMember));
        assert!(c.run_until(2000, |c| c.nodes[l].caught_up(&hid(n))));
        assert!(c.nodes[l].promote(hid(n)).is_ok(), "the next change goes ahead once the last committed");
    }

    #[test]
    fn a_new_leader_changes_nothing_before_its_noop_commits() {
        use crate::raft::{Durable, RaftConfig, RaftMsg};
        let mut n = RaftNode::<u64>::new(hid(0), Durable::new(set(&[0, 1, 2])), RaftConfig::default(), 1);
        n.tick(0);
        n.tick(RaftConfig::default().election_max_ms);
        n.step(3000, hid(1), RaftMsg::VoteResp { term: 1, granted: true, pre_vote: true });
        n.step(3000, hid(1), RaftMsg::VoteResp { term: 1, granted: true, pre_vote: false });
        assert!(n.is_leader());
        assert_eq!(n.add_learner(hid(3)), Err(MembershipError::ChangePending));
        n.step(3001, hid(1), RaftMsg::AppendResp { term: 1, success: true, match_index: 1, conflict_index: 0 });
        assert_eq!(n.commit_index(), 1);
        assert_eq!(n.add_learner(hid(3)), Ok(2));
    }

    #[test]
    fn a_lagging_learner_is_not_promoted() {
        let (mut c, l, n) = with_caught_up_learner();
        c.isolate(&[n]);
        for v in 0..CATCH_UP_ENTRIES + 1 {
            c.propose(l, v);
        }
        c.run_for(500);
        assert!(!c.nodes[l].caught_up(&hid(n)));
        assert_eq!(c.nodes[l].promote(hid(n)), Err(MembershipError::NotCaughtUp));
        c.heal();
        assert!(c.run_until(2000, |c| c.nodes[l].caught_up(&hid(n))));
        assert!(c.nodes[l].promote(hid(n)).is_ok());
    }

    #[test]
    fn a_caught_up_learner_that_went_silent_is_not_promoted() {
        let (mut c, l, n) = with_caught_up_learner();
        c.frozen.insert(n);
        c.run_for(1500 + 2 * STEP);
        assert!(c.nodes[l].caught_up(&hid(n)), "its log still matches");
        assert!(!c.nodes[l].responsive(&hid(n)));
        assert_eq!(c.nodes[l].promote(hid(n)), Err(MembershipError::Unresponsive));
        c.frozen.remove(&n);
        assert!(c.run_until(1000, |c| c.nodes[l].responsive(&hid(n))));
        assert!(c.nodes[l].promote(hid(n)).is_ok());
    }

    #[test]
    fn promotion_is_refused_unless_the_live_voters_and_learner_make_the_new_quorum() {
        use crate::raft::{Durable, RaftConfig, RaftMsg};
        use crate::runtime::SimTime;
        let ack = |n: &mut RaftNode<u64>, now: SimTime, from: usize, match_index: u64| {
            n.step(now, hid(from), RaftMsg::AppendResp { term: 1, success: true, match_index, conflict_index: 0 });
        };
        // Leader n0 of {n0, n1, n2} with caught-up learner n3.
        let mut n = RaftNode::<u64>::new(hid(0), Durable::new(set(&[0, 1, 2])), RaftConfig::default(), 1);
        n.tick(0);
        n.tick(RaftConfig::default().election_max_ms);
        n.step(3000, hid(1), RaftMsg::VoteResp { term: 1, granted: true, pre_vote: true });
        n.step(3000, hid(1), RaftMsg::VoteResp { term: 1, granted: true, pre_vote: false });
        ack(&mut n, 3001, 1, 1);
        assert_eq!(n.add_learner(hid(3)), Ok(2));
        ack(&mut n, 3002, 1, 2);
        ack(&mut n, 3003, 3, 2);
        assert!(n.caught_up(&hid(3)) && n.responsive(&hid(3)));

        // 1.6 s on only the learner has answered: the leader alone plus the
        // learner are no quorum of four voters.
        ack(&mut n, 4600, 3, 2);
        assert!(n.is_leader(), "check-quorum only runs on the next tick");
        assert_eq!(n.promote(hid(3)), Err(MembershipError::WouldLoseQuorum));
        // With n1 answering again, n0 + n1 + n3 make the quorum of four.
        ack(&mut n, 4601, 1, 2);
        assert_eq!(n.promote(hid(3)), Ok(3));
    }

    #[test]
    fn leader_that_removes_itself_steps_down_once_committed() {
        let mut c = Cluster::new(3);
        let l = c.elect();
        c.run_for(500);
        let index = c.nodes[l].remove(hid(l)).expect("a leader may remove itself");
        assert!(c.nodes[l].is_leader(), "it keeps leading until the change commits");
        let rest: Vec<usize> = (0..3).filter(|&i| i != l).collect();
        assert!(c.run_until(2000, |c| rest.iter().all(|&i| c.nodes[i].commit_index() >= index)));
        assert!(c.run_until(1000, |c| !c.nodes[l].is_leader()), "it steps down once its removal commits");
        assert!(c.run_until(6000, |c| c.leaders().iter().any(|&i| i != l)), "the remaining voters elect a leader");
        let m = *c.leaders().iter().find(|&&i| i != l).unwrap();
        c.propose(m, 3);
        assert!(c.run_until(2000, |c| rest.iter().all(|&i| c.committed[i].contains(&3))));
        c.run_for(10_000);
        assert!(!c.nodes[l].is_leader() && c.nodes[l].role() != crate::raft::RaftRole::Candidate, "a removed node never campaigns");
    }
}
