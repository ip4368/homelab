//! Applying the rollout commands. Which hosts a rollout concerns is decided
//! when `StartRollout` applies, from the state it applies to.

use super::{ApplyError, Change, ClusterState, Lifecycle, NetState, Rollout, RolloutState};
use crate::controllers::rollout::{batch_size, concerned};
use crate::runtime::SimTime;
use model::HostId;
use std::collections::BTreeMap;

impl ClusterState {
    /// Applies `change` and starts rolling it out.
    ///
    /// The rollout's `hosts` are the ready hosts whose config the change
    /// alters, plus those with an override if `include_overridden` (a template
    /// change drops their override, kept in `dropped_overrides`). Concerned
    /// hosts that are `NotReady` are `skipped`. Every one of them with a
    /// desired config is pinned to it; one with none yet simply renders the
    /// change when it gets there. The first batch starts at once.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn start_rollout(
        &mut self,
        v: u64,
        id: String,
        change: Change,
        include_overridden: bool,
        batch_pct: u8,
        soak_ms: u64,
    ) -> Result<(), ApplyError> {
        if let Some(active) = self.rollouts.values().find(|r| r.is_active()) {
            return Err(ApplyError::RolloutInProgress(active.id.clone()));
        }
        if self.rollouts.contains_key(&id) {
            return Err(ApplyError::AlreadyExists);
        }
        if !(1..=100).contains(&batch_pct) {
            return Err(ApplyError::InvalidBatchPct(batch_pct));
        }
        let c = concerned(self, &change);
        let included: Vec<HostId> = if include_overridden { c.overridden } else { vec![] };
        let ready = |h: &HostId| self.hosts[h].lifecycle != Lifecycle::NotReady;
        let (mut hosts, mut skipped): (Vec<HostId>, Vec<HostId>) = c.affected.into_iter().chain(included.iter().cloned()).partition(ready);
        hosts.sort();
        skipped.sort();
        let previous = match &change {
            Change::Template(t) => self.templates.get(&t.id).map(|o| Change::Template(o.template.clone())),
            Change::Network(n) => self.networks.get(&n.id).map(|o| Change::Network(o.network.clone())),
        };
        let mut dropped_overrides = BTreeMap::new();
        if matches!(change, Change::Template(_)) {
            for h in &included {
                let host = self.hosts.get_mut(h).expect("concerned hosts exist");
                dropped_overrides.extend(host.override_.take().map(|o| (h.clone(), o)));
            }
        }
        match change.clone() {
            Change::Template(t) => self.upsert_template(v, t),
            Change::Network(n) => self.upsert_network(v, n),
        }
        for h in hosts.iter().chain(&skipped) {
            let host = self.hosts.get_mut(h).expect("concerned hosts exist");
            host.net.pinned_config = host.net.desired.clone();
            host.version = v;
        }
        let r = Rollout {
            id: id.clone(),
            change,
            batch_size: batch_size(hosts.len(), batch_pct),
            state: if hosts.is_empty() { RolloutState::Done } else { RolloutState::Running },
            hosts,
            current_batch: 0,
            failed: vec![],
            skipped,
            previous,
            dropped_overrides,
            soak_ms,
            base_generation: BTreeMap::new(),
            version: v,
        };
        let done = r.state == RolloutState::Done;
        self.rollouts.insert(id.clone(), r);
        if done {
            self.finish(v, &id);
        } else {
            self.start_batch(v, &id);
        }
        Ok(())
    }

    pub(super) fn soak_rollout(&mut self, at: SimTime, v: u64, id: &str, batch: usize) -> Result<(), ApplyError> {
        let r = self.rollouts.get_mut(id).ok_or(ApplyError::NotFound)?;
        if r.state != RolloutState::Running || r.current_batch != batch {
            return Err(ApplyError::InvalidRolloutState);
        }
        r.state = RolloutState::Soaking { until: at + r.soak_ms };
        r.version = v;
        Ok(())
    }

    pub(super) fn advance_rollout(&mut self, v: u64, id: &str, batch: usize) -> Result<(), ApplyError> {
        let r = self.rollouts.get_mut(id).ok_or(ApplyError::NotFound)?;
        if !matches!(r.state, RolloutState::Soaking { .. }) || r.current_batch != batch {
            return Err(ApplyError::InvalidRolloutState);
        }
        r.version = v;
        if batch + 1 >= r.batches() {
            r.state = RolloutState::Done;
            self.finish(v, id);
        } else {
            r.current_batch = batch + 1;
            r.state = RolloutState::Running;
            self.start_batch(v, id);
        }
        Ok(())
    }

    pub(super) fn pause_rollout(&mut self, v: u64, id: &str, reason: String, failed: Vec<HostId>) -> Result<(), ApplyError> {
        let r = self.rollouts.get_mut(id).ok_or(ApplyError::NotFound)?;
        if !matches!(r.state, RolloutState::Running | RolloutState::Soaking { .. }) {
            return Err(ApplyError::InvalidRolloutState);
        }
        r.state = RolloutState::Paused { reason };
        r.failed = failed;
        r.version = v;
        Ok(())
    }

    /// Back to `Running` at the current batch. A failed generation is not
    /// pushed again, so a host that failed or reverted is rebased to retry
    /// with the next one. A host that is not ready is skipped: pinned to
    /// the config it last applied until the rollout finishes.
    pub(super) fn resume_rollout(&mut self, v: u64, id: &str) -> Result<(), ApplyError> {
        let r = self.rollouts.get_mut(id).ok_or(ApplyError::NotFound)?;
        if !matches!(r.state, RolloutState::Paused { .. }) {
            return Err(ApplyError::InvalidRolloutState);
        }
        r.state = RolloutState::Running;
        r.failed.clear();
        r.version = v;
        for h in r.batch(r.current_batch).to_vec() {
            let Some(host) = self.hosts.get_mut(&h).filter(|_| !r.skipped.contains(&h)) else { continue };
            if host.lifecycle == Lifecycle::NotReady {
                host.net.pinned_config = host.net.applied.clone();
                host.version = v;
                r.skipped.push(h);
            } else if matches!(host.net.state, NetState::Failed | NetState::Reverted) {
                r.base_generation.insert(h, host.net.desired_generation);
            }
        }
        Ok(())
    }

    /// Takes the change back: the template or network it replaced (one it
    /// created stays), the overrides it dropped, and no host pinned.
    pub(super) fn abort_rollout(&mut self, v: u64, id: &str) -> Result<(), ApplyError> {
        let r = self.rollouts.get_mut(id).ok_or(ApplyError::NotFound)?;
        if !r.is_active() {
            return Err(ApplyError::InvalidRolloutState);
        }
        r.state = RolloutState::Aborted;
        r.version = v;
        let (previous, overrides) = (r.previous.clone(), std::mem::take(&mut r.dropped_overrides));
        match previous {
            Some(Change::Template(t)) => self.upsert_template(v, t),
            Some(Change::Network(n)) => self.upsert_network(v, n),
            None => {}
        }
        for (h, o) in overrides {
            if let Some(host) = self.hosts.get_mut(&h)
                && host.override_.is_none()
            {
                host.override_ = Some(o);
                host.version = v;
            }
        }
        self.finish(v, id);
        Ok(())
    }

    /// Starts rollout `id`'s current batch: its hosts drop their pins, so
    /// the network controller renders the change for them, and their desired
    /// generations are noted to tell when they applied a newer one.
    fn start_batch(&mut self, v: u64, id: &str) {
        let Some(r) = self.rollouts.get_mut(id) else { return };
        r.base_generation.clear();
        for h in r.batch(r.current_batch).to_vec() {
            let Some(host) = self.hosts.get_mut(&h) else { continue };
            host.net.pinned_config = None;
            host.version = v;
            r.base_generation.insert(h, host.net.desired_generation);
        }
    }

    /// The rollout is over (done or aborted): no host of it stays pinned.
    fn finish(&mut self, v: u64, id: &str) {
        let Some(r) = self.rollouts.get_mut(id) else { return };
        r.base_generation.clear();
        for h in r.hosts.iter().chain(&r.skipped) {
            if let Some(host) = self.hosts.get_mut(h)
                && host.net.pinned_config.take().is_some()
            {
                host.version = v;
            }
        }
    }
}
