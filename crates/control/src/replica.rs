//! A control-plane replica: one Raft node plus the cluster state built by
//! applying its committed entries. The leader claims the VIP, answers the API
//! and syncs, and runs the controllers; followers only replicate.

use crate::controllers::join::{self, JoinOutcome};
use crate::controllers::rollout;
use crate::controllers::membership::{self, RaftMembers};
use crate::controllers::{self, Action, HostReport, PendingApi, PendingJoin, SwitchReading, Volatile, FAILED_BACKOFF_MS};
use crate::proto::{mgmt, ApiOp, ApiResult, LogCmd, Msg, Observed};
use crate::raft::{Durable, EntryPayload, RaftConfig, RaftNode, RaftOut, RaftRole};
use crate::runtime::{Actor, Addr, Ctx, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, Lifecycle, NetworkResult};
use model::{HostId, Inventory, Role};
use std::collections::BTreeSet;

pub const RAFT_TICK_MS: u64 = 100;
pub const CONTROLLER_PERIOD_MS: u64 = 1000;
/// A client write not committed this long after proposal gets `Unavailable`.
pub const API_COMMIT_TIMEOUT_MS: u64 = 2000;
pub const JOIN_TOKEN_TTL_MS: u64 = 24 * 3600 * 1000;

const TICK: u64 = 0;
const CONTROLLERS: u64 = 1;

pub struct CpReplica {
    me: HostId,
    /// Consumed by `on_start`, which seeds the Raft node from the runtime RNG.
    /// State stored on the host's disk takes precedence.
    init: Option<Durable<LogCmd>>,
    raft: Option<RaftNode<LogCmd>>,
    state: ClusterState,
    /// `Some` exactly while this replica is leader.
    volatile: Option<Volatile>,
}

impl CpReplica {
    /// `durable` is this replica's persisted Raft state, or `None` for a
    /// fresh member with the bootstrap `voters`. Either is only used when the
    /// host has no stored CP state; otherwise the replica resumes from that.
    pub fn new(me: HostId, durable: Option<Durable<LogCmd>>, voters: BTreeSet<HostId>) -> Self {
        let init = durable.unwrap_or_else(|| Durable::new(voters));
        Self { me, init: Some(init), raft: None, state: ClusterState::new(), volatile: None }
    }

    pub fn is_leader(&self) -> bool {
        self.raft.as_ref().is_some_and(|r| r.is_leader())
    }

    /// Whether this replica leads and has applied an entry of its own term,
    /// so its state includes everything earlier leaders committed: only then
    /// does it answer the API and syncs.
    pub fn caught_up(&self) -> bool {
        self.volatile.as_ref().is_some_and(|v| v.caught_up)
    }

    /// `None` until the replica has started.
    pub fn raft_role(&self) -> Option<RaftRole> {
        self.raft.as_ref().map(RaftNode::role)
    }

    /// Highest committed log index; 0 until the replica has started.
    pub fn commit_index(&self) -> u64 {
        self.raft.as_ref().map_or(0, RaftNode::commit_index)
    }

    pub fn log_len(&self) -> usize {
        self.durable().log.len()
    }

    /// The CP voters as this replica's log has them.
    pub fn voters(&self) -> &BTreeSet<HostId> {
        self.raft.as_ref().map_or(&self.durable().voters, RaftNode::voters)
    }

    /// The CP learners as this replica's log has them.
    pub fn learners(&self) -> BTreeSet<HostId> {
        self.raft.as_ref().map(|r| r.learners().clone()).unwrap_or_default()
    }

    /// The cluster state as this replica has applied it (possibly behind the leader's).
    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    /// The Raft state this replica has persisted.
    pub fn durable(&self) -> &Durable<LogCmd> {
        match &self.raft {
            Some(r) => r.durable(),
            None => self.init.as_ref().expect("replica has either initial or live Raft state"),
        }
    }

    fn raft(&mut self) -> &mut RaftNode<LogCmd> {
        self.raft.as_mut().expect("on_start runs before anything else")
    }

    /// Carries out Raft's effects in order, then applies newly committed entries.
    fn handle(&mut self, ctx: &mut dyn Ctx, outs: Vec<RaftOut<LogCmd>>) {
        for out in outs {
            match out {
                RaftOut::Send(to, m) => ctx.send(Endpoint::new(Addr::Host(to), Svc::Cp), mgmt(), Msg::Raft(m)),
                // On the host's disk before any later Send leaves, so a
                // restarted replica resumes from it.
                RaftOut::Persist(op) => {
                    if let Some(os) = ctx.host() {
                        os.persist_cp(op);
                    }
                }
                RaftOut::BecameLeader(term) => {
                    self.volatile = Some(Volatile::new(ctx.now(), term));
                    ctx.claim_vip();
                }
                RaftOut::SteppedDown => {
                    if let Some(v) = self.volatile.take() {
                        for p in v.pending.into_values() {
                            reply(ctx, &p, ApiResult::Unavailable);
                        }
                    }
                }
            }
        }
        self.apply_committed(ctx);
    }

    fn apply_committed(&mut self, ctx: &mut dyn Ctx) {
        for e in self.raft().take_committed() {
            let result = match e.payload {
                EntryPayload::Normal(LogCmd { at, cmd }) => Some(self.state.apply(at, cmd)),
                EntryPayload::Config { voters, .. } => {
                    self.state.apply_membership(voters);
                    Some(Ok(()))
                }
                EntryPayload::Noop => None,
            };
            let Some(v) = &mut self.volatile else { continue };
            v.caught_up |= e.term == v.term;
            if let (Some(cmd), Some(Err(_))) = (v.in_flight.remove(&e.index), &result) {
                v.failed.push((cmd, ctx.now()));
            }
            // An overwritten join (different term) gets no answer; the agent retries.
            if let (Some(j), Some(r)) = (v.pending_joins.remove(&e.index).filter(|j| j.term == e.term), &result) {
                let answer = r.as_ref().map(|_| ()).map_err(ToString::to_string);
                ctx.send(j.from, mgmt(), Msg::JoinResp(answer));
            }
            let Some(p) = v.pending.remove(&e.index) else { continue };
            let answer = match result {
                _ if p.term != e.term => ApiResult::Unavailable,
                Some(Ok(())) => ApiResult::Ok(p.ok.clone()),
                Some(Err(err)) => ApiResult::Err(err.to_string()),
                None => ApiResult::Unavailable,
            };
            reply(ctx, &p, answer);
        }
    }

    fn on_tick(&mut self, ctx: &mut dyn Ctx) {
        let now = ctx.now();
        let outs = self.raft().tick(ctx.monotonic_ms());
        self.handle(ctx, outs);
        let Some(v) = &mut self.volatile else { return };
        let overdue = |_: &u64, p: &mut PendingApi| now >= p.proposed_at + API_COMMIT_TIMEOUT_MS;
        for (_, p) in v.pending.extract_if(.., overdue) {
            reply(ctx, &p, ApiResult::Unavailable);
        }
    }

    fn run_controllers(&mut self, ctx: &mut dyn Ctx) {
        let (now, mono) = (ctx.now(), ctx.monotonic_ms());
        let Some(raft) = self.raft.as_mut().filter(|r| r.has_quorum(mono)) else { return };
        let Some(v) = self.volatile.as_mut().filter(|v| v.caught_up) else { return };
        v.failed.retain(|(_, at)| now < at + FAILED_BACKOFF_MS);
        v.forget_removed(&self.state);
        let mut actions = controllers::run_all(&self.state, v, now);
        let plan = membership::run(&self.state, v, &RaftMembers::of(raft), now);
        v.last_run = Some(now);
        actions.extend(plan.actions);
        if let Some(change) = plan.change {
            // Refused only if the leader cannot change membership now; the next run asks again.
            let _ = change.apply(raft);
        }
        for action in actions {
            match action {
                Action::Propose(cmd) => {
                    if !v.may_propose(&cmd, now) {
                        continue;
                    }
                    if let Ok(index) = raft.propose(mono, LogCmd { at: now, cmd: cmd.clone() }) {
                        v.in_flight.insert(index, cmd);
                    }
                }
                Action::Send(to, net, msg) => {
                    v.note_sent(&to, &msg, now);
                    ctx.send(to, net, msg);
                }
            }
        }
        // `propose` returns no effects; flush them now rather than at the next tick.
        let outs = raft.tick(mono);
        self.handle(ctx, outs);
    }

    fn on_api(&mut self, ctx: &mut dyn Ctx, from: Endpoint, req: u64, op: ApiOp) {
        let (now, mono) = (ctx.now(), ctx.monotonic_ms());
        let raft = self.raft.as_mut().expect("on_start runs before anything else");
        if !raft.is_leader() {
            return ctx.send(from, mgmt(), Msg::ApiResp { req, result: ApiResult::NotLeader });
        }
        let v = self.volatile.as_ref().expect("leader has volatile state");
        // Without quorum, or before its no-op applied (its state may lag what
        // earlier leaders committed), the leader cannot answer safely.
        if !raft.has_quorum(mono) || !v.caught_up {
            return ctx.send(from, mgmt(), Msg::ApiResp { req, result: ApiResult::Unavailable });
        }
        let term = raft.term();
        let (index, ok) = match handle_op(ctx, &self.state, v, term, op) {
            Handling::Answer(result) => return ctx.send(from, mgmt(), Msg::ApiResp { req, result }),
            Handling::Propose(cmd, ok) => (raft.propose(mono, LogCmd { at: now, cmd: *cmd }).expect("checked leadership above"), ok),
            Handling::ReplaceCpMember { dead, with } => {
                let members = RaftMembers::of(raft);
                let added = membership::check_replace(&self.state, &members, &dead, &with)
                    .and_then(|()| raft.add_learner(with.clone()).map_err(|e| e.to_string()));
                match added {
                    Err(e) => return ctx.send(from, mgmt(), Msg::ApiResp { req, result: ApiResult::Err(e) }),
                    Ok(index) => {
                        let agent = Endpoint::new(Addr::Host(with), Svc::Agent);
                        ctx.send(agent, mgmt(), Msg::StartCpReplica { voters: members.voters });
                        (index, serde_json::Value::Null)
                    }
                }
            }
            Handling::HandOffCp { from: leaving, to } => {
                if let Err(e) = membership::check_handoff(&self.state, &RaftMembers::of(raft), &leaving, &to) {
                    return ctx.send(from, mgmt(), Msg::ApiResp { req, result: ApiResult::Err(e) });
                }
                let cmd = Command::StartCpHandoff { from: leaving, to };
                (raft.propose(mono, LogCmd { at: now, cmd }).expect("checked leadership above"), serde_json::Value::Null)
            }
        };
        let pending = PendingApi { from, req, term, ok, proposed_at: now };
        self.volatile.as_mut().expect("leader has volatile state").pending.insert(index, pending);
        let outs = self.raft().tick(mono);
        self.handle(ctx, outs);
    }

    /// Only a leader with quorum and a caught-up state can judge a token or
    /// know whether the host exists; anyone else stays silent and the agent retries.
    fn on_join(&mut self, ctx: &mut dyn Ctx, from: Endpoint, token: String, inventory: Inventory) {
        let (now, mono) = (ctx.now(), ctx.monotonic_ms());
        let Addr::Host(host) = from.addr.clone() else { return };
        let Some(raft) = self.raft.as_mut().filter(|r| r.has_quorum(mono)) else { return };
        let Some(v) = self.volatile.as_mut().filter(|v| v.caught_up) else { return };
        match join::on_request(&self.state, v, now, &host, &token, inventory) {
            JoinOutcome::Accepted => ctx.send(from, mgmt(), Msg::JoinResp(Ok(()))),
            JoinOutcome::InProgress => {}
            JoinOutcome::Add(cmd) => {
                let term = raft.term();
                let index = raft.propose(mono, LogCmd { at: now, cmd }).expect("has_quorum implies leadership");
                v.pending_joins.insert(index, PendingJoin { from, term });
            }
            JoinOutcome::Rejected { reason, event } => {
                ctx.send(from, mgmt(), Msg::JoinResp(Err(reason)));
                if let Some(cmd) = event {
                    raft.propose(mono, LogCmd { at: now, cmd }).expect("has_quorum implies leadership");
                }
            }
        }
        // `propose` returns no effects; flush them now.
        let outs = self.raft().tick(mono);
        self.handle(ctx, outs);
    }

    /// Notes the heartbeat, the host's network generation and what it runs
    /// and holds, files the host's observation if it came with one, and asks
    /// for it if the one held here is not the one the agent stamped (`None`
    /// means the agent has none).
    #[allow(clippy::too_many_arguments)]
    fn on_heartbeat(
        &mut self,
        ctx: &mut dyn Ctx,
        from: Endpoint,
        host: HostId,
        observed_at: Option<SimTime>,
        observed: Option<Observed>,
        net_generation: u64,
    ) {
        let now = ctx.now();
        let Some(v) = &mut self.volatile else { return };
        // A host that was removed, or never joined, is ignored.
        if !self.state.hosts.contains_key(&host) {
            return;
        }
        v.last_heartbeat.insert(host.clone(), now);
        v.net_generation.insert(host.clone(), net_generation);
        match observed {
            Some(observed) => {
                let observed_at = observed_at.unwrap_or(now);
                v.note_report(host, HostReport { observed_at, observed });
            }
            None if observed_at.is_some() && v.host_reports.get(&host).is_none_or(|r| Some(r.observed_at) != observed_at) => {
                ctx.send(from, mgmt(), Msg::ReportRequest);
            }
            None => {}
        }
    }

    /// Files an agent's network apply report for the network controller. An
    /// `Applied` report travelled over the new config, so mgmt works: confirm
    /// it right away, before the agent's revert timer runs out.
    fn on_network_status(&mut self, ctx: &mut dyn Ctx, from: Endpoint, generation: u64, result: NetworkResult) {
        let Addr::Host(host) = from.addr.clone() else { return };
        let Some(v) = &mut self.volatile else { return };
        if from.svc != Svc::Agent || !self.state.hosts.contains_key(&host) {
            return;
        }
        if result == NetworkResult::Applied {
            ctx.send(from, mgmt(), Msg::ConfirmNetwork { generation });
        }
        v.net_results.insert(host, (generation, result));
    }

    fn on_sync(&mut self, ctx: &mut dyn Ctx, from: Endpoint, since: u64) {
        let mono = ctx.monotonic_ms();
        let raft = self.raft.as_ref().expect("on_start runs before anything else");
        // A leader cut off from its quorum, or not caught up yet, may hold a stale view; stay silent.
        let Some(v) = self.volatile.as_ref().filter(|v| v.caught_up && raft.has_quorum(mono)) else { return };
        let metrics = v.metrics.iter().map(|(h, m)| (h.clone(), m.clone())).collect();
        let changes = self.state.changes_since(since);
        ctx.send(from, mgmt(), Msg::SyncResp { changes, leader: self.me.clone(), term: raft.term(), metrics });
    }
}

/// What the leader does with a client op.
enum Handling {
    /// Answer now; nothing to write.
    Answer(ApiResult),
    /// Propose the command; answer with the value once it applies.
    Propose(Box<Command>, serde_json::Value),
    /// Add `with` as a CP learner, to replace `dead`; answer once that commits.
    ReplaceCpMember { dead: HostId, with: HostId },
    /// Record a hand-off of `from`'s CP place to `to`, if the membership allows it.
    HandOffCp { from: HostId, to: HostId },
}

/// How the leader of `term` handles `op`, given its state.
fn handle_op(ctx: &mut dyn Ctx, state: &ClusterState, v: &Volatile, term: u64, op: ApiOp) -> Handling {
    let cmd = match op {
        ApiOp::CreateJoinToken => {
            let token = format!("{:016x}", ctx.rand_u64());
            let ok = serde_json::Value::String(token.clone());
            return Handling::Propose(Box::new(Command::CreateJoinToken { token, ttl_ms: JOIN_TOKEN_TTL_MS }), ok);
        }
        ApiOp::PreviewChange { change } => {
            if let Err(e) = rollout::check(state, &change) {
                return Handling::Answer(ApiResult::Err(e));
            }
            let preview = serde_json::to_value(rollout::preview(state, &change)).expect("a preview serializes");
            return Handling::Answer(ApiResult::Ok(preview));
        }
        ApiOp::StartRollout { change, include_overridden, batch_pct, soak_s } => {
            if let Err(e) = rollout::check(state, &change) {
                return Handling::Answer(ApiResult::Err(e));
            }
            let id = format!("rollout-{term}-{}", ctx.now());
            let soak_ms = u64::from(soak_s) * 1000;
            let cmd = Command::StartRollout { id: id.clone(), change, include_overridden, batch_pct, soak_ms };
            return Handling::Propose(Box::new(cmd), serde_json::Value::String(id));
        }
        ApiOp::PauseRollout { id } => Command::PauseRollout { id, reason: "paused by the user".into(), failed: vec![] },
        ApiOp::ResumeRollout { id } => Command::ResumeRollout { id },
        ApiOp::AbortRollout { id } => Command::AbortRollout { id },
        ApiOp::ApproveHost { id, roles } | ApiOp::SetRoles { id, roles } if roles.contains(&Role::ControlPlane) != state.cp.voters.contains(&id) => {
            return Handling::Answer(ApiResult::Err(membership::control_plane_role_refused(&id, roles.contains(&Role::ControlPlane))));
        }
        ApiOp::ApproveHost { id, roles } => Command::ApproveHost { id, roles },
        ApiOp::SetRoles { id, roles } => Command::SetRoles { id, roles },
        ApiOp::SetLabels { id, labels } => Command::SetLabels { id, labels },
        ApiOp::Cordon { id } => Command::SetLifecycle { id, lifecycle: Lifecycle::Cordoned },
        ApiOp::Uncordon { id } => Command::SetLifecycle { id, lifecycle: Lifecycle::Ready },
        ApiOp::Drain { host } => Command::SetLifecycle { id: host, lifecycle: Lifecycle::Draining },
        ApiOp::RemoveHost { id } => Command::RemoveHost { id },
        ApiOp::SetHostOverride { id, config } => Command::SetHostOverride { host: id, config },
        ApiOp::UpsertRule { id, profile, roles, template } => Command::UpsertRule { id, profile, roles, template },
        ApiOp::DeleteRule { id } => Command::DeleteRule { id },
        ApiOp::DeleteNetwork { id } => Command::DeleteNetwork { id },
        ApiOp::DeleteTemplate { id } => Command::DeleteTemplate { id },
        ApiOp::SetCpMode { mode } => Command::SetCpMode { mode },
        ApiOp::ReplaceCpMember { dead, with } => return Handling::ReplaceCpMember { dead, with },
        ApiOp::HandOffCp { from, to } => return Handling::HandOffCp { from, to },
        ApiOp::CancelCpHandoff => match membership::check_cancel_handoff(state) {
            Ok(()) => Command::CancelCpHandoff,
            Err(e) => return Handling::Answer(ApiResult::Err(e)),
        },
        ApiOp::GetSeries { entity, metrics } => {
            let result = v.history.query(state, &entity, &metrics).map_or_else(ApiResult::Err, ApiResult::Ok);
            return Handling::Answer(result);
        }
        ApiOp::GetKubeconfig => {
            let result = match ctx.host().and_then(|os| os.kubeconfig()) {
                Some(k) => ApiResult::Ok(serde_json::Value::String(k)),
                None => ApiResult::Err("the leader runs no Kubernetes server yet".into()),
            };
            return Handling::Answer(result);
        }
    };
    Handling::Propose(Box::new(cmd), serde_json::Value::Null)
}

fn reply(ctx: &mut dyn Ctx, p: &PendingApi, result: ApiResult) {
    ctx.send(p.from.clone(), mgmt(), Msg::ApiResp { req: p.req, result });
}

fn arm(ctx: &mut dyn Ctx, token: u64) {
    let after: SimTime = if token == TICK { RAFT_TICK_MS } else { CONTROLLER_PERIOD_MS };
    ctx.set_timer(after, token);
}

impl Actor for CpReplica {
    fn on_start(&mut self, ctx: &mut dyn Ctx) {
        let init = self.init.take().expect("on_start runs once");
        let durable = match ctx.host() {
            Some(os) => os.load_cp().unwrap_or_else(|| {
                os.store_cp(init.clone());
                init
            }),
            None => init,
        };
        self.raft = Some(RaftNode::new(self.me.clone(), durable, RaftConfig::default(), ctx.rand_u64()));
        // A random phase, so replicas started together do not tick in lockstep:
        // election deadlines only fire on ticks, and aligned ticks make
        // simultaneous campaigns (split votes) far more likely.
        let phase = 1 + ctx.rand_u64() % RAFT_TICK_MS;
        ctx.set_timer(phase, TICK);
        arm(ctx, CONTROLLERS);
    }

    fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg) {
        let now = ctx.now();
        match msg {
            Msg::Raft(m) => {
                let Addr::Host(peer) = from.addr else { return };
                let outs = self.raft().step(ctx.monotonic_ms(), peer, m);
                self.handle(ctx, outs);
            }
            Msg::Heartbeat { host, observed_at, observed, net_generation, .. } => {
                self.on_heartbeat(ctx, from, host, observed_at, observed, net_generation)
            }
            Msg::NetworkStatus { generation, result } => self.on_network_status(ctx, from, generation, result),
            Msg::SwitchReport { switch, model, ports, mlag_peer } => {
                // A report only counts from the switch it describes.
                if from.addr == Addr::Switch(switch.clone())
                    && let Some(v) = &mut self.volatile
                {
                    v.switch_reports.insert(switch, SwitchReading { at: now, model, ports, mlag_peer });
                }
            }
            Msg::JoinRequest { token, inventory } => self.on_join(ctx, from, token, inventory),
            Msg::Metrics { host, sample } => {
                if let Some(v) = &mut self.volatile
                    && self.state.hosts.contains_key(&host)
                {
                    v.note_sample(host, sample);
                }
            }
            Msg::KubeStatus { host, status } => {
                if let Some(v) = &mut self.volatile
                    && self.state.hosts.contains_key(&host)
                {
                    v.kube_status.insert(host, status);
                }
            }
            Msg::Api { req, op } => self.on_api(ctx, from, req, op),
            Msg::Sync { since } => self.on_sync(ctx, from, since),
            Msg::Ping(_)
            | Msg::Pong(_)
            | Msg::ApiResp { .. }
            | Msg::SyncResp { .. }
            | Msg::JoinResp(_)
            | Msg::ApplyNetwork { .. }
            | Msg::ConfirmNetwork { .. }
            | Msg::ReportRequest
            | Msg::SwitchPoll
            | Msg::StartCpReplica { .. }
            | Msg::Kube(_)
            | Msg::KubeAdmin(_)
            | Msg::Directory { .. } => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64) {
        match token {
            TICK => self.on_tick(ctx),
            _ => self.run_controllers(ctx),
        }
        arm(ctx, token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::health::NOT_READY_AFTER_MS;
    use crate::raft::RaftMsg;
    use crate::runtime::HostOs;
    use crate::state::ObjectRef;
    use model::{profile, Inventory, NetworkId, ProfileId, Role};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    #[derive(Default)]
    struct FakeCtx {
        /// The monotonic clock.
        now: SimTime,
        /// How far a clock step has put wall time behind `now`.
        wall_behind: SimTime,
        /// Everything sent except Raft traffic.
        sent: Vec<(Endpoint, Msg)>,
        /// (after_ms, token) of every timer set.
        timers: Vec<(u64, u64)>,
        /// What `rand_u64` returns.
        rand: u64,
    }

    impl Ctx for FakeCtx {
        fn now(&self) -> SimTime {
            self.now - self.wall_behind
        }
        fn monotonic_ms(&self) -> SimTime {
            self.now
        }
        fn me(&self) -> Endpoint {
            Endpoint::new(Addr::Host(hid("h1")), Svc::Cp)
        }
        fn send(&mut self, to: Endpoint, _: NetworkId, msg: Msg) {
            if !matches!(msg, Msg::Raft(_)) {
                self.sent.push((to, msg));
            }
        }
        fn set_timer(&mut self, after_ms: u64, token: u64) {
            self.timers.push((after_ms, token));
        }
        fn rand_u64(&mut self) -> u64 {
            self.rand
        }
        fn claim_vip(&mut self) {}
        fn host(&mut self) -> Option<&mut dyn HostOs> {
            None
        }
    }

    /// Replica h1 of {h1, h2, h3}, driven by hand; h2 plays every peer.
    struct Rig {
        r: CpReplica,
        ctx: FakeCtx,
    }

    impl Rig {
        /// Started with `cmds` already applied, as if replayed from earlier terms.
        fn new(cmds: Vec<Command>) -> Self {
            let voters = ["h1", "h2", "h3"].map(hid).into();
            let mut rig = Rig { r: CpReplica::new(hid("h1"), None, voters), ctx: FakeCtx { rand: 1, ..FakeCtx::default() } };
            rig.r.on_start(&mut rig.ctx);
            for c in cmds {
                rig.r.state.apply(0, c).unwrap();
            }
            rig
        }

        fn h2_says(&mut self, m: RaftMsg<LogCmd>) {
            let from = Endpoint::new(Addr::Host(hid("h2")), Svc::Cp);
            self.r.on_message(&mut self.ctx, from, Msg::Raft(m));
        }

        /// Ticks until h1 campaigns, then grants it h2's pre-vote and vote.
        fn elect(&mut self) {
            while self.r.raft().term() == 0 && !self.r.is_leader() {
                self.ctx.now += RAFT_TICK_MS;
                self.r.on_timer(&mut self.ctx, TICK);
                let term = self.r.raft().term();
                self.h2_says(RaftMsg::VoteResp { term: term + 1, granted: true, pre_vote: true });
            }
            let term = self.r.raft().term();
            self.h2_says(RaftMsg::VoteResp { term, granted: true, pre_vote: false });
            assert!(self.r.is_leader());
        }

        /// h2 acknowledges the whole log: everything commits and applies.
        fn ack(&mut self) {
            let (term, last) = (self.r.raft().term(), self.r.log_len() as u64);
            self.h2_says(RaftMsg::AppendResp { term, success: true, match_index: last, conflict_index: 0 });
        }

        fn controllers(&mut self) {
            self.r.on_timer(&mut self.ctx, CONTROLLERS);
        }

        fn advance(&mut self, ms: u64) {
            self.ctx.now += ms;
            self.ack(); // keeps the leader's quorum fresh
        }

        fn proposed(&self) -> Vec<Command> {
            self.r
                .durable()
                .log
                .iter()
                .filter_map(|e| match &e.payload {
                    EntryPayload::Normal(c) => Some(c.cmd.clone()),
                    _ => None,
                })
                .collect()
        }
    }

    /// A removed host whose HostDown alert is still open: health wants to
    /// resolve it on every run until it is.
    fn orphaned_alert() -> Vec<Command> {
        vec![
            Command::AddPendingHost {
                id: hid("h9"),
                profile: profile(ProfileId::MiniPc, 0),
                inventory: Inventory::from_profile(ProfileId::MiniPc, 0),
                location: "rack-1".into(),
            },
            raise("hostdown-h9-0-0"),
            Command::RemoveHost { id: hid("h9") },
        ]
    }

    fn raise(id: &str) -> Command {
        Command::RaiseAlert {
            id: id.into(),
            kind: "HostDown".into(),
            entity: ObjectRef::host(&hid("h9")),
            subject: None,
            message: "down".into(),
        }
    }

    #[test]
    fn join_is_answered_once_the_host_is_added() {
        let mut rig = Rig::new(vec![Command::CreateJoinToken { token: "tok".into(), ttl_ms: 60_000 }]);
        rig.elect();
        rig.ack();
        let m1 = Endpoint::new(Addr::Host(hid("m1")), Svc::Agent);
        let join = |rig: &mut Rig, token: &str| {
            let inventory = Inventory::from_profile(ProfileId::MiniPc, 0);
            let msg = Msg::JoinRequest { token: token.into(), inventory };
            rig.r.on_message(&mut rig.ctx, m1.clone(), msg);
            std::mem::take(&mut rig.ctx.sent)
        };
        assert!(join(&mut rig, "tok").is_empty(), "no answer before the host exists");
        assert!(join(&mut rig, "tok").is_empty(), "a retry while in flight");
        assert!(matches!(rig.proposed().as_slice(), [Command::AddPendingHost { id, .. }] if *id == hid("m1")));
        rig.ack();
        assert_eq!(std::mem::take(&mut rig.ctx.sent), [(m1.clone(), Msg::JoinResp(Ok(())))]);
        assert_eq!(rig.r.state.hosts[&hid("m1")].lifecycle, Lifecycle::Pending);
        assert_eq!(join(&mut rig, "tok"), [(m1.clone(), Msg::JoinResp(Ok(())))], "known host: answered at once");
        assert_eq!(rig.proposed().len(), 1);

        let m2 = Endpoint::new(Addr::Host(hid("m2")), Svc::Agent);
        let msg = Msg::JoinRequest { token: "bad".into(), inventory: Inventory::from_profile(ProfileId::MiniPc, 0) };
        rig.r.on_message(&mut rig.ctx, m2.clone(), msg);
        assert_eq!(rig.ctx.sent, [(m2, Msg::JoinResp(Err("invalid token".into())))]);
        assert!(matches!(rig.proposed().last(), Some(Command::Event { message, .. }) if message.contains("join rejected")));
    }

    #[test]
    fn replicas_tick_at_a_random_phase() {
        let first_tick = |rand: u64| {
            let mut ctx = FakeCtx { rand, ..FakeCtx::default() };
            CpReplica::new(hid("h1"), None, [hid("h1")].into()).on_start(&mut ctx);
            ctx.timers.iter().find(|(_, token)| *token == TICK).unwrap().0
        };
        assert_eq!((first_tick(0), first_tick(41), first_tick(199)), (1, 42, 100));
    }

    #[test]
    fn controllers_wait_for_the_leaders_noop_to_apply() {
        let mut rig = Rig::new(orphaned_alert());
        assert_eq!((rig.r.raft_role(), rig.r.commit_index()), (Some(RaftRole::Follower), 0));
        rig.elect();
        assert_eq!(rig.r.raft_role(), Some(RaftRole::Leader));
        rig.controllers();
        assert!(rig.proposed().is_empty(), "controllers ran before the no-op applied: {:?}", rig.proposed());
        rig.ack();
        assert_eq!(rig.r.commit_index(), 1, "the no-op committed");
        rig.controllers();
        assert_eq!(rig.proposed(), [Command::ResolveAlert { id: "hostdown-h9-0-0".into() }]);
    }

    #[test]
    fn a_wall_clock_stepped_back_does_not_stretch_the_leaders_quorum() {
        let mut rig = Rig::new(vec![]);
        rig.elect();
        rig.ack();
        // No peer answers for a check-quorum window, while chrony steps wall time back as far.
        let window = RaftConfig::default().election_min_ms + RAFT_TICK_MS;
        rig.ctx.now += window;
        rig.ctx.wall_behind = window;
        rig.r.on_timer(&mut rig.ctx, TICK);
        assert_eq!(rig.r.raft_role(), Some(RaftRole::Follower), "check-quorum runs on the monotonic clock");
    }

    #[test]
    fn api_and_syncs_wait_for_the_leaders_noop_to_apply() {
        let mut rig = Rig::new(orphaned_alert()[..1].to_vec());
        rig.elect();
        assert!(rig.r.raft().has_quorum(rig.ctx.now), "a fresh leader has quorum");
        let client = Endpoint::new(Addr::Client, Svc::Client);
        let cordon = |req| Msg::Api { req, op: ApiOp::Cordon { id: hid("h9") } };
        rig.r.on_message(&mut rig.ctx, client.clone(), cordon(1));
        assert_eq!(std::mem::take(&mut rig.ctx.sent), [(client.clone(), Msg::ApiResp { req: 1, result: ApiResult::Unavailable })], "its state may lag");
        rig.r.on_message(&mut rig.ctx, client.clone(), Msg::Sync { since: 0 });
        assert!(rig.ctx.sent.is_empty(), "no sync from a state that may lag: {:?}", rig.ctx.sent);
        assert!(rig.proposed().is_empty());

        rig.ack();
        rig.r.on_message(&mut rig.ctx, client.clone(), Msg::Sync { since: 0 });
        assert!(matches!(rig.ctx.sent.pop(), Some((_, Msg::SyncResp { .. }))));
        rig.r.on_message(&mut rig.ctx, client.clone(), cordon(2));
        assert!(matches!(rig.proposed().as_slice(), [Command::SetLifecycle { .. }]), "{:?}", rig.proposed());
    }

    #[test]
    fn a_leader_without_a_kubernetes_server_has_no_kubeconfig_to_give() {
        let mut rig = Rig::new(vec![]);
        rig.elect();
        rig.ack();
        let (client, before) = (Endpoint::new(Addr::Client, Svc::Client), rig.proposed());
        rig.r.on_message(&mut rig.ctx, client, Msg::Api { req: 1, op: ApiOp::GetKubeconfig });
        let sent = std::mem::take(&mut rig.ctx.sent);
        assert!(matches!(sent.as_slice(), [(_, Msg::ApiResp { req: 1, result: ApiResult::Err(_) })]), "{sent:?}");
        assert_eq!(rig.proposed(), before, "a read proposes nothing");
    }

    fn h9_heartbeat(rig: &mut Rig) {
        let hb = Msg::Heartbeat { host: hid("h9"), lifecycle_seen: 0, observed_at: None, observed: None, net_generation: 0 };
        rig.r.on_message(&mut rig.ctx, host_ep("h9", Svc::Agent), hb);
    }

    #[test]
    fn a_removed_host_is_forgotten_and_its_heartbeats_ignored() {
        let mut rig = Rig::new(orphaned_alert()[..1].to_vec());
        rig.elect();
        rig.ack();
        h9_heartbeat(&mut rig);
        assert!(rig.r.volatile.as_ref().unwrap().last_heartbeat.contains_key(&hid("h9")));
        rig.r.state.apply(0, Command::RemoveHost { id: hid("h9") }).unwrap();
        rig.controllers();
        assert!(!rig.r.volatile.as_ref().unwrap().last_heartbeat.contains_key(&hid("h9")), "forgotten");
        h9_heartbeat(&mut rig);
        assert!(!rig.r.volatile.as_ref().unwrap().last_heartbeat.contains_key(&hid("h9")), "and not heard again");
    }

    #[test]
    fn identical_proposal_is_not_repeated_while_in_flight() {
        let mut rig = Rig::new(orphaned_alert());
        rig.elect();
        rig.ack();
        for _ in 0..3 {
            rig.controllers();
            rig.ctx.now += CONTROLLER_PERIOD_MS;
        }
        assert_eq!(rig.proposed().len(), 1, "one proposal while uncommitted: {:?}", rig.proposed());
        rig.ack();
        rig.controllers();
        assert_eq!(rig.proposed().len(), 1, "applied, so nothing more to do");
    }

    #[test]
    fn command_that_fails_to_apply_backs_off() {
        // h9 is Ready and silent, and an alert with the id health will pick
        // already exists (resolved), so its RaiseAlert keeps failing.
        let mut cmds = orphaned_alert();
        cmds.pop();
        cmds.push(Command::ApproveHost { id: hid("h9"), roles: [Role::Compute].into() });
        cmds.push(Command::SetLifecycle { id: hid("h9"), lifecycle: Lifecycle::Provisioning });
        cmds.push(Command::SetLifecycle { id: hid("h9"), lifecycle: Lifecycle::Ready });
        let mut rig = Rig::new(cmds);
        rig.elect();
        let id = format!("hostdown-h9-1-{}", rig.ctx.now);
        rig.r.state.apply(0, Command::ResolveAlert { id: "hostdown-h9-0-0".into() }).unwrap();
        rig.r.state.apply(0, raise(&id)).unwrap();
        rig.r.state.apply(0, Command::ResolveAlert { id: id.clone() }).unwrap();
        rig.ack();
        rig.advance(NOT_READY_AFTER_MS + 1);

        let raises = |rig: &Rig| rig.proposed().iter().filter(|c| matches!(c, Command::RaiseAlert { .. })).count();
        for _ in 0..(FAILED_BACKOFF_MS / CONTROLLER_PERIOD_MS) {
            rig.controllers();
            rig.advance(CONTROLLER_PERIOD_MS);
        }
        assert_eq!(raises(&rig), 1, "the failing RaiseAlert is retried at most once per backoff period");
        assert_eq!(rig.r.state.hosts[&hid("h9")].lifecycle, Lifecycle::NotReady, "the rest still applies");
        // It failed when it applied, one period after it was proposed.
        rig.advance(CONTROLLER_PERIOD_MS);
        rig.controllers();
        assert_eq!(raises(&rig), 2, "and is retried once the backoff expires");
    }

    fn host_ep(h: &str, svc: Svc) -> Endpoint {
        Endpoint::new(Addr::Host(hid(h)), svc)
    }

    #[test]
    fn series_are_answered_from_the_leaders_memory_without_a_write() {
        use crate::metrics::MetricEntity;
        use crate::proto::MetricSample;
        let mut rig = Rig::new(orphaned_alert()[..1].to_vec());
        let client = Endpoint::new(Addr::Client, Svc::Client);
        let get = |metric: &str| ApiOp::GetSeries { entity: MetricEntity::Host(hid("h9")), metrics: vec![metric.into()] };
        rig.r.on_message(&mut rig.ctx, client.clone(), Msg::Api { req: 1, op: get("cpu_pct") });
        assert_eq!(rig.ctx.sent.pop(), Some((client.clone(), Msg::ApiResp { req: 1, result: ApiResult::NotLeader })));

        rig.elect();
        rig.ack();
        let sample = MetricSample { at: 1234, cpu_pct: 42.0, ram_pct: 1.0, temp_c: Some(30.0), disk_pct: Default::default(), port_rx_bps: Default::default(), port_tx_bps: Default::default() };
        rig.r.on_message(&mut rig.ctx, host_ep("h9", Svc::Agent), Msg::Metrics { host: hid("h9"), sample });
        let log = rig.r.log_len();
        rig.r.on_message(&mut rig.ctx, client.clone(), Msg::Api { req: 2, op: get("cpu_pct") });
        let ok = serde_json::json!({"since": 1234, "series": {"cpu_pct": [[1234, 42.0]]}});
        assert_eq!(rig.ctx.sent.pop(), Some((client.clone(), Msg::ApiResp { req: 2, result: ApiResult::Ok(ok) })));
        rig.r.on_message(&mut rig.ctx, client.clone(), Msg::Api { req: 3, op: get("bogus") });
        assert!(matches!(rig.ctx.sent.pop(), Some((_, Msg::ApiResp { req: 3, result: ApiResult::Err(_) }))));
        assert_eq!(rig.r.log_len(), log, "reads are not written to the log");
    }

    fn observed(link_up: bool) -> Observed {
        let mut inventory = Inventory::from_profile(ProfileId::MiniPc, 0);
        inventory.nics[0].link_up = link_up;
        Observed { lldp: vec![], inventory }
    }

    fn heartbeat(at: Option<SimTime>, observed: Option<Observed>) -> Msg {
        Msg::Heartbeat { host: hid("m1"), lifecycle_seen: 0, observed_at: at, observed, net_generation: 0 }
    }

    #[test]
    fn leader_files_observations_and_asks_for_the_ones_it_lacks() {
        let add = Command::AddPendingHost {
            id: hid("m1"),
            profile: profile(ProfileId::MiniPc, 0),
            inventory: Inventory::from_profile(ProfileId::MiniPc, 0),
            location: "rack-1".into(),
        };
        let mut rig = Rig::new(vec![add]);
        rig.elect();
        rig.ack();
        let agent = host_ep("m1", Svc::Agent);
        let hb = |rig: &mut Rig, m: Msg| {
            rig.r.on_message(&mut rig.ctx, agent.clone(), m);
            std::mem::take(&mut rig.ctx.sent)
        };
        let ask = [(agent.clone(), Msg::ReportRequest)];
        assert_eq!(hb(&mut rig, heartbeat(Some(5), None)), ask, "a stamp the leader holds nothing for");
        assert!(hb(&mut rig, heartbeat(None, None)).is_empty(), "an agent with nothing to report is not asked");
        assert!(hb(&mut rig, heartbeat(Some(5), Some(observed(true)))).is_empty());
        assert!(hb(&mut rig, heartbeat(Some(5), None)).is_empty(), "stamp matches the held report");
        assert_eq!(hb(&mut rig, heartbeat(Some(9), None)), ask, "a newer stamp: the report was lost");
        let held = &rig.r.volatile.as_ref().unwrap().host_reports[&hid("m1")];
        assert_eq!((held.observed_at, &held.observed), (5, &observed(true)));
        hb(&mut rig, heartbeat(Some(9), Some(observed(false))));
        assert_eq!(rig.r.volatile.as_ref().unwrap().host_reports[&hid("m1")].observed, observed(false));

        // Unknown hosts are neither stored nor asked.
        let stranger = Msg::Heartbeat {
            host: hid("zz"),
            lifecycle_seen: 0,
            observed_at: Some(1),
            observed: Some(observed(true)),
            net_generation: 0,
        };
        assert!(hb(&mut rig, stranger).is_empty());
        assert!(!rig.r.volatile.as_ref().unwrap().host_reports.contains_key(&hid("zz")));
        assert!(!rig.r.volatile.as_ref().unwrap().last_heartbeat.contains_key(&hid("zz")));
    }

    #[test]
    fn switch_polls_are_paced_and_reports_only_count_from_their_own_switch() {
        let mut rig = Rig::new(vec![Command::AddPendingHost {
            id: hid("m1"),
            profile: profile(ProfileId::MiniPc, 0),
            inventory: Inventory::from_profile(ProfileId::MiniPc, 0),
            location: "rack-1".into(),
        }]);
        rig.elect();
        rig.ack();
        let sw = |s: &str| Endpoint::new(Addr::Switch(model::SwitchId::from(s)), Svc::Switch);
        // The leader knows access-1 from the host's LLDP.
        let lldp = vec![model::network::LldpNeighbor {
            local_port: model::PortName::from("eth0"),
            switch: model::SwitchId::from("access-1"),
            switch_port: model::PortName::from("p1"),
            mgmt_ip: None,
        }];
        rig.r.state.apply(0, Command::ObserveHost { id: hid("m1"), lldp, inventory: Inventory::from_profile(ProfileId::MiniPc, 0) }).unwrap();
        let polls = |rig: &mut Rig| {
            let sent = std::mem::take(&mut rig.ctx.sent);
            sent.into_iter().filter(|(_, m)| *m == Msg::SwitchPoll).map(|(to, _)| to).collect::<Vec<_>>()
        };
        rig.controllers();
        assert_eq!(polls(&mut rig), [sw("access-1")]);
        for _ in 0..29 {
            rig.advance(CONTROLLER_PERIOD_MS);
            rig.controllers();
        }
        assert!(polls(&mut rig).is_empty(), "not again within 30 s");
        rig.advance(CONTROLLER_PERIOD_MS);
        rig.controllers();
        assert_eq!(polls(&mut rig), [sw("access-1")], "30 s after the last poll");

        let report = |switch: &str| Msg::SwitchReport {
            switch: model::SwitchId::from(switch),
            model: "m".into(),
            ports: vec![],
            mlag_peer: None,
        };
        rig.r.on_message(&mut rig.ctx, sw("access-2"), report("access-1"));
        assert!(rig.r.volatile.as_ref().unwrap().switch_reports.is_empty(), "access-2 cannot speak for access-1");
        rig.r.on_message(&mut rig.ctx, host_ep("m1", Svc::Agent), report("access-1"));
        assert!(rig.r.volatile.as_ref().unwrap().switch_reports.is_empty(), "nor can a host");
        rig.r.on_message(&mut rig.ctx, sw("access-1"), report("access-1"));
        let now = rig.ctx.now;
        assert_eq!(rig.r.volatile.as_ref().unwrap().switch_reports[&model::SwitchId::from("access-1")].at, now);
    }

    #[test]
    fn leader_confirms_applied_network_configs_and_files_reports() {
        let add = Command::AddPendingHost {
            id: hid("m1"),
            profile: profile(ProfileId::MiniPc, 0),
            inventory: Inventory::from_profile(ProfileId::MiniPc, 0),
            location: "rack-1".into(),
        };
        let mut rig = Rig::new(vec![add]);
        rig.elect();
        rig.ack();
        let agent = host_ep("m1", Svc::Agent);
        let status = |generation, result| Msg::NetworkStatus { generation, result };
        rig.r.on_message(&mut rig.ctx, agent.clone(), status(3, NetworkResult::Applied));
        assert_eq!(std::mem::take(&mut rig.ctx.sent), [(agent.clone(), Msg::ConfirmNetwork { generation: 3 })], "confirmed at once");
        rig.r.on_message(&mut rig.ctx, agent.clone(), status(4, NetworkResult::Reverted));
        assert!(rig.ctx.sent.is_empty(), "nothing to confirm");
        let v = rig.r.volatile.as_ref().unwrap();
        assert_eq!(v.net_results[&hid("m1")], (4, NetworkResult::Reverted), "the latest report is kept");

        // Only a known host's agent is listened to.
        rig.r.on_message(&mut rig.ctx, host_ep("m1", Svc::Cp), status(5, NetworkResult::Applied));
        rig.r.on_message(&mut rig.ctx, host_ep("zz", Svc::Agent), status(5, NetworkResult::Applied));
        assert!(rig.ctx.sent.is_empty());
        assert_eq!(rig.r.volatile.as_ref().unwrap().net_results.len(), 1);

        let hb = Msg::Heartbeat {
            host: hid("m1"),
            lifecycle_seen: 0,
            observed_at: None,
            observed: None,
            net_generation: 7,
        };
        rig.r.on_message(&mut rig.ctx, agent, hb);
        let v = rig.r.volatile.as_ref().unwrap();
        assert_eq!(v.net_generation[&hid("m1")], 7, "heartbeats carry the running generation");
    }
}
