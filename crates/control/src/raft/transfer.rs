//! Leadership transfer (Ongaro §3.10). The leader stops taking proposals and
//! membership changes, brings the chosen voter's log up to its own, then
//! tells it to campaign at once ([`RaftMsg::TimeoutNow`]). The voter
//! campaigns in the next term without a pre-vote, and marks its vote
//! requests so that voters, the old leader included, answer them despite
//! leader stickiness; the old leader steps down on the first one. Since the
//! voter's log is as long as any, it wins unless it fails on the way. A
//! transfer not done within `election_min_ms` is abandoned and the leader
//! carries on.
//!
//! [`RaftMsg::TimeoutNow`]: super::RaftMsg::TimeoutNow

use super::{NotLeader, RaftNode};
use crate::runtime::SimTime;
use model::HostId;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    NotLeader(NotLeader),
    /// The leader is already handing over.
    InProgress,
    /// The leader already leads.
    ToSelf,
    /// Only a voter can lead.
    NotVoter,
    /// The voter has not answered within the check-quorum window.
    Unresponsive,
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TransferError::NotLeader(_) => "not the leader",
            TransferError::InProgress => "the leadership is already being handed over",
            TransferError::ToSelf => "already the leader",
            TransferError::NotVoter => "not a control-plane voter",
            TransferError::Unresponsive => "the voter has not answered recently",
        };
        f.write_str(s)
    }
}

impl<T: Clone> RaftNode<T> {
    /// Starts handing leadership to voter `to`, which must have answered
    /// within the check-quorum window: a dead voter would only hold up
    /// proposals for the length of the attempt. As with `propose`, the
    /// effects come out of the next `tick`/`step`. Leadership has moved once
    /// this node reports `SteppedDown`; [`RaftNode::transferee`] is `None`
    /// again if the attempt was abandoned.
    pub fn transfer_leadership(&mut self, now: SimTime, to: HostId) -> Result<(), TransferError> {
        if !self.is_leader() {
            return Err(TransferError::NotLeader(NotLeader { leader_hint: self.leader_hint() }));
        }
        if self.transferee().is_some() {
            return Err(TransferError::InProgress);
        }
        if to == *self.id() {
            return Err(TransferError::ToSelf);
        }
        if !self.voters().contains(&to) {
            return Err(TransferError::NotVoter);
        }
        if !self.acked_recently(&to) {
            return Err(TransferError::Unresponsive);
        }
        self.begin_transfer(now, to);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::node::tests::{hid, Cluster};
    use crate::raft::{RaftConfig, RaftOut};

    /// An `n`-voter cluster whose leader has committed its no-op.
    fn settled(n: usize) -> (Cluster, usize) {
        let mut c = Cluster::new(n);
        let l = c.elect();
        c.run_for(500);
        (c, l)
    }

    #[test]
    fn leadership_moves_to_the_chosen_voter_in_one_term() {
        let (mut c, l) = settled(5);
        let to = (l + 2) % 5;
        let term = c.nodes[l].term();
        c.propose(l, 1);
        c.nodes[l].transfer_leadership(c.now, hid(to)).expect("a live voter");
        assert_eq!(c.nodes[l].transferee(), Some(&hid(to)));
        assert!(c.run_until(1000, |c| c.leaders() == [to]), "leaders {:?}", c.leaders());
        assert_eq!(c.nodes[to].term(), term + 1, "no pre-vote, no split vote: one term");
        assert!(c.events.iter().any(|(_, i, e)| *i == l && *e == RaftOut::SteppedDown));
        assert!(c.nodes[l].transferee().is_none());
        c.propose(to, 2);
        assert!(c.run_until(1000, |c| c.committed.iter().all(|x| x == &[1, 2])), "{:?}", c.committed);
    }

    #[test]
    fn a_lagging_voter_is_caught_up_first() {
        let (mut c, l) = settled(3);
        let to = (l + 1) % 3;
        c.isolate(&[to]);
        for v in 0..20 {
            c.propose(l, v);
        }
        c.run_for(500);
        c.nodes[l].transfer_leadership(c.now, hid(to)).expect("its ack is still fresh");
        c.heal();
        assert!(c.run_until(1000, |c| c.leaders() == [to]), "leaders {:?}", c.leaders());
        let all: Vec<u64> = (0..20).collect();
        assert!(c.run_until(1000, |c| c.committed[to] == all), "{:?}", c.committed[to]);
    }

    #[test]
    fn the_leader_takes_no_proposals_or_changes_while_handing_over() {
        let (mut c, l) = settled(3);
        let to = (l + 1) % 3;
        c.frozen.insert(to); // it never campaigns, so the hand-over hangs
        c.nodes[l].transfer_leadership(c.now, hid(to)).unwrap();
        assert_eq!(c.nodes[l].propose(c.now, 9), Err(NotLeader { leader_hint: Some(hid(to)) }));
        assert_eq!(c.nodes[l].remove(hid(to)), Err(crate::raft::MembershipError::Transferring));
        assert_eq!(c.nodes[l].transfer_leadership(c.now, hid(to)), Err(TransferError::InProgress));
        assert!(c.nodes[l].is_leader(), "it leads until someone else does");
    }

    #[test]
    fn a_hand_over_that_does_not_happen_is_abandoned() {
        let (mut c, l) = settled(3);
        let to = (l + 1) % 3;
        let term = c.nodes[l].term();
        c.nodes[l].transfer_leadership(c.now, hid(to)).unwrap();
        c.frozen.insert(to); // it dies before it hears it should campaign
        assert!(
            c.run_until(RaftConfig::default().election_min_ms + 100, |c| c.nodes[l].transferee().is_none()),
            "abandoned within an election timeout"
        );
        assert_eq!((c.leaders(), c.nodes[l].term()), (vec![l], term), "the leader carries on");
        c.propose(l, 3);
        c.frozen.remove(&to);
        assert!(c.run_until(1000, |c| c.committed.iter().all(|x| x == &[3])));
        assert_eq!(c.leaders(), vec![l], "nor does the voter take over once back");
    }

    #[test]
    fn a_lost_timeout_now_is_sent_again() {
        let (mut c, l) = settled(3);
        let to = (l + 1) % 3;
        c.nodes[l].transfer_leadership(c.now, hid(to)).unwrap();
        let outs = c.nodes[l].tick(c.now);
        let is_timeout_now = |o: &RaftOut<u64>| matches!(o, RaftOut::Send(_, crate::raft::RaftMsg::TimeoutNow { .. }));
        assert!(outs.iter().any(is_timeout_now), "a caught-up voter is told at once: {outs:?}");
        c.handle(l, outs.into_iter().filter(|o| !is_timeout_now(o)).collect());
        assert!(c.run_until(1000, |c| c.leaders() == [to]), "told again on its next ack");
    }

    #[test]
    fn transfer_is_refused_unless_to_a_live_voter() {
        let (mut c, l) = settled(3);
        let f = (l + 1) % 3;
        let now = c.now;
        assert_eq!(c.nodes[f].transfer_leadership(now, hid(l)), Err(TransferError::NotLeader(NotLeader { leader_hint: Some(hid(l)) })));
        assert_eq!(c.nodes[l].transfer_leadership(now, hid(l)), Err(TransferError::ToSelf));
        assert_eq!(c.nodes[l].transfer_leadership(now, hid(7)), Err(TransferError::NotVoter));
        let n = c.add_node();
        c.nodes[l].add_learner(hid(n)).unwrap();
        c.run_for(500);
        assert_eq!(c.nodes[l].transfer_leadership(c.now, hid(n)), Err(TransferError::NotVoter), "a learner cannot lead");
        c.frozen.insert(f);
        c.run_for(RaftConfig::default().election_min_ms + 100);
        assert_eq!(c.nodes[l].transfer_leadership(c.now, hid(f)), Err(TransferError::Unresponsive));
    }

    #[test]
    fn a_stale_timeout_now_is_ignored() {
        let (mut c, l) = settled(3);
        let f = (l + 1) % 3;
        let term = c.nodes[f].term();
        let outs = c.nodes[f].step(c.now, hid(l), crate::raft::RaftMsg::TimeoutNow { term: term - 1 });
        c.handle(f, outs);
        c.run_for(1000);
        assert_eq!((c.leaders(), c.nodes[f].term()), (vec![l], term));
    }
}
