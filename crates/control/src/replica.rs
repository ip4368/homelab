//! A control-plane replica: one Raft node plus the cluster state built by
//! applying its committed entries. The leader claims the VIP, answers the API
//! and syncs, and runs the controllers; followers only replicate.

use crate::controllers::{self, Action, PendingApi, Volatile};
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
                RaftOut::BecameLeader(_) => {
                    self.volatile = Some(Volatile::new(ctx.now()));
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
            v.in_flight.remove(&e.index);
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
        let Some(v) = &mut self.volatile else { return };
        for action in controllers::run_all(&self.state, v, now) {
            match action {
                Action::Propose(cmd) => {
                    if v.in_flight.values().any(|c| *c == cmd) {
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
