//! A control-plane replica: one Raft node plus the cluster state built by
//! applying its committed entries. The leader claims the VIP, answers the API
//! and syncs, and runs the controllers; followers only replicate.

use crate::controllers::{self, Action, PendingApi, Volatile, FAILED_BACKOFF_MS};
use crate::proto::{mgmt, ApiOp, ApiResult, LogCmd, Msg};
use crate::raft::{Durable, EntryPayload, RaftConfig, RaftNode, RaftOut};
use crate::runtime::{Actor, Addr, Ctx, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, Lifecycle};
use model::HostId;
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
    init: Option<Durable<LogCmd>>,
    raft: Option<RaftNode<LogCmd>>,
    state: ClusterState,
    /// `Some` exactly while this replica is leader.
    volatile: Option<Volatile>,
}

impl CpReplica {
    /// `durable` is this replica's persisted Raft state, or `None` for a
    /// fresh member with the bootstrap `voters`.
    pub fn new(me: HostId, durable: Option<Durable<LogCmd>>, voters: BTreeSet<HostId>) -> Self {
        let init = durable.unwrap_or_else(|| Durable::new(voters));
        Self { me, init: Some(init), raft: None, state: ClusterState::new(), volatile: None }
    }

    pub fn is_leader(&self) -> bool {
        self.raft.as_ref().is_some_and(|r| r.is_leader())
    }

    pub fn log_len(&self) -> usize {
        self.durable().log.len()
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
                // The Raft node keeps the durable state in memory for now; the
                // host OS will persist these ops so a restarted replica can reload them.
                RaftOut::Persist(_) => {}
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
                EntryPayload::Noop | EntryPayload::Config(_) => None,
            };
            let Some(v) = &mut self.volatile else { continue };
            v.caught_up |= e.term == v.term;
            if let (Some(cmd), Some(Err(_))) = (v.in_flight.remove(&e.index), &result) {
                v.failed.push((cmd, ctx.now()));
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
        let outs = self.raft().tick(now);
        self.handle(ctx, outs);
        let Some(v) = &mut self.volatile else { return };
        let overdue = |_: &u64, p: &mut PendingApi| now >= p.proposed_at + API_COMMIT_TIMEOUT_MS;
        for (_, p) in v.pending.extract_if(.., overdue) {
            reply(ctx, &p, ApiResult::Unavailable);
        }
    }

    fn run_controllers(&mut self, ctx: &mut dyn Ctx) {
        let now = ctx.now();
        let Some(raft) = self.raft.as_mut().filter(|r| r.has_quorum(now)) else { return };
        let Some(v) = self.volatile.as_mut().filter(|v| v.caught_up) else { return };
        v.failed.retain(|(_, at)| now < at + FAILED_BACKOFF_MS);
        for action in controllers::run_all(&self.state, v, now) {
            match action {
                Action::Propose(cmd) => {
                    if !v.may_propose(&cmd, now) {
                        continue;
                    }
                    if let Ok(index) = raft.propose(now, LogCmd { at: now, cmd: cmd.clone() }) {
                        v.in_flight.insert(index, cmd);
                    }
                }
                Action::Send(to, net, msg) => ctx.send(to, net, msg),
            }
        }
        // `propose` returns no effects; flush them now rather than at the next tick.
        let outs = raft.tick(now);
        self.handle(ctx, outs);
    }

    fn on_api(&mut self, ctx: &mut dyn Ctx, from: Endpoint, req: u64, op: ApiOp) {
        let now = ctx.now();
        let raft = self.raft.as_mut().expect("on_start runs before anything else");
        if !raft.is_leader() {
            return ctx.send(from, mgmt(), Msg::ApiResp { req, result: ApiResult::NotLeader });
        }
        if !raft.has_quorum(now) {
            return ctx.send(from, mgmt(), Msg::ApiResp { req, result: ApiResult::Unavailable });
        }
        let (cmd, ok) = command_for(ctx, op);
        let term = raft.term();
        let index = raft.propose(now, LogCmd { at: now, cmd }).expect("checked leadership above");
        let pending = PendingApi { from, req, term, ok, proposed_at: now };
        self.volatile.as_mut().expect("leader has volatile state").pending.insert(index, pending);
        let outs = self.raft().tick(now);
        self.handle(ctx, outs);
    }

    fn on_sync(&mut self, ctx: &mut dyn Ctx, from: Endpoint, since: u64) {
        let now = ctx.now();
        let raft = self.raft.as_ref().expect("on_start runs before anything else");
        // A leader cut off from its quorum may hold a stale view; stay silent.
        let Some(v) = self.volatile.as_ref().filter(|_| raft.has_quorum(now)) else { return };
        let metrics = v.metrics.iter().map(|(h, m)| (h.clone(), m.clone())).collect();
        let changes = self.state.changes_since(since);
        ctx.send(from, mgmt(), Msg::SyncResp { changes, leader: self.me.clone(), term: raft.term(), metrics });
    }
}

/// The command for a client op and the value to answer with once it applies.
fn command_for(ctx: &mut dyn Ctx, op: ApiOp) -> (Command, serde_json::Value) {
    let cmd = match op {
        ApiOp::CreateJoinToken => {
            let token = format!("{:016x}", ctx.rand_u64());
            let ok = serde_json::Value::String(token.clone());
            return (Command::CreateJoinToken { token, ttl_ms: JOIN_TOKEN_TTL_MS }, ok);
        }
        ApiOp::ApproveHost { id, roles } => Command::ApproveHost { id, roles },
        ApiOp::SetRoles { id, roles } => Command::SetRoles { id, roles },
        ApiOp::SetLabels { id, labels } => Command::SetLabels { id, labels },
        ApiOp::Cordon { id } => Command::SetLifecycle { id, lifecycle: Lifecycle::Cordoned },
        ApiOp::Uncordon { id } => Command::SetLifecycle { id, lifecycle: Lifecycle::Ready },
        ApiOp::RemoveHost { id } => Command::RemoveHost { id },
        ApiOp::UpsertRule { id, profile, roles, template } => Command::UpsertRule { id, profile, roles, template },
        ApiOp::DeleteRule { id } => Command::DeleteRule { id },
    };
    (cmd, serde_json::Value::Null)
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
        let durable = self.init.take().expect("on_start runs once");
        self.raft = Some(RaftNode::new(self.me.clone(), durable, RaftConfig::default(), ctx.rand_u64()));
        arm(ctx, TICK);
        arm(ctx, CONTROLLERS);
    }

    fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg) {
        let now = ctx.now();
        match msg {
            Msg::Raft(m) => {
                let Addr::Host(peer) = from.addr else { return };
                let outs = self.raft().step(now, peer, m);
                self.handle(ctx, outs);
            }
            Msg::Heartbeat { host, .. } => {
                if let Some(v) = &mut self.volatile {
                    v.last_heartbeat.insert(host, now);
                }
            }
            Msg::Api { req, op } => self.on_api(ctx, from, req, op),
            Msg::Sync { since } => self.on_sync(ctx, from, since),
            Msg::Ping(_) | Msg::Pong(_) | Msg::ApiResp { .. } | Msg::SyncResp { .. } => {}
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
    use crate::state::{ObjectRef, Severity};
    use model::{profile, Inventory, NetworkId, ProfileId, Role};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    #[derive(Default)]
    struct FakeCtx {
        now: SimTime,
    }

    impl Ctx for FakeCtx {
        fn now(&self) -> SimTime {
            self.now
        }
        fn me(&self) -> Endpoint {
            Endpoint::new(Addr::Host(hid("h1")), Svc::Cp)
        }
        fn send(&mut self, _: Endpoint, _: NetworkId, _: Msg) {}
        fn set_timer(&mut self, _: u64, _: u64) {}
        fn rand_u64(&mut self) -> u64 {
            1
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
            let mut rig = Rig { r: CpReplica::new(hid("h1"), None, voters), ctx: FakeCtx::default() };
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
            severity: Severity::Critical,
            kind: "HostDown".into(),
            entity: ObjectRef::host(&hid("h9")),
            message: "down".into(),
            ground_truth: false,
        }
    }

    #[test]
    fn controllers_wait_for_the_leaders_noop_to_apply() {
        let mut rig = Rig::new(orphaned_alert());
        rig.elect();
        rig.controllers();
        assert!(rig.proposed().is_empty(), "controllers ran before the no-op applied: {:?}", rig.proposed());
        rig.ack();
        rig.controllers();
        assert_eq!(rig.proposed(), [Command::ResolveAlert { id: "hostdown-h9-0-0".into() }]);
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
        rig.r.state.apply(0, raise(&id)).unwrap();
        rig.r.state.apply(0, Command::ResolveAlert { id: id.clone() }).unwrap();
        rig.r.state.apply(0, Command::ResolveAlert { id: "hostdown-h9-0-0".into() }).unwrap();
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
}
