//! The UI's connection to the control plane. It reaches the leader only
//! through the VIP, keeps a [`ClusterView`] fresh by polling `Sync`, and
//! submits API operations on the user's behalf.

use crate::proto::{mgmt, ApiOp, ApiResult, MetricSample, Msg};
use crate::runtime::{Actor, Addr, Ctx, Endpoint, SimTime, Svc};
use crate::state::{Changes, ClusterState, Object, ObjectKind, ObjectRef, EVENT_RING};
use model::{HostId, NetworkId, SwitchId};
use std::collections::BTreeMap;

/// Where the UI's client runs.
pub const CLIENT: Endpoint = Endpoint { addr: Addr::Client, svc: Svc::Client };
const VIP: Endpoint = Endpoint { addr: Addr::Vip, svc: Svc::Cp };

pub const SYNC_PERIOD_MS: u64 = 500;
pub const NOT_LEADER_RETRY_MS: u64 = 300;
/// A request with no final answer after this long yields `Unavailable`.
pub const API_GIVE_UP_MS: u64 = 5000;

/// Timer tokens: `SYNC`, or a request id times two plus `RETRY` or `GIVE_UP`.
const SYNC: u64 = 0;
const RETRY: u64 = 0;
const GIVE_UP: u64 = 1;

/// What the client knows of the cluster, as of its last successful sync.
#[derive(Debug, Clone, Default)]
pub struct ClusterView {
    /// The leader's objects. Only the public maps and `version` are maintained.
    pub state: ClusterState,
    pub version: u64,
    pub leader: Option<HostId>,
    pub term: u64,
    pub last_sync_at: Option<SimTime>,
    /// Latest sample per host.
    pub metrics: BTreeMap<HostId, MetricSample>,
}

impl ClusterView {
    /// Folds in a leader's changes: upserts replace objects by ref, removals
    /// delete them (leaving tombstones, so `state.changes_since` reports them
    /// to readers of the view), and `version` becomes the leader's.
    pub fn merge(&mut self, changes: Changes) {
        let s = &mut self.state;
        for obj in changes.upserts {
            match obj {
                Object::Host(o) => {
                    s.revive(ObjectKind::Host, o.id.as_str());
                    s.hosts.insert(o.id.clone(), o);
                }
                Object::Token(o) => {
                    s.revive(ObjectKind::Token, &o.token);
                    s.tokens.insert(o.token.clone(), o);
                }
                Object::Rule(o) => {
                    s.revive(ObjectKind::Rule, &o.id);
                    s.rules.insert(o.id.clone(), o);
                }
                Object::Alert(o) => {
                    s.revive(ObjectKind::Alert, &o.id);
                    s.alerts.insert(o.id.clone(), o);
                }
                Object::Event(o) => {
                    s.events.insert(o.seq, o);
                }
                Object::Cp(o) => s.cp = o,
                Object::Network(o) => {
                    s.revive(ObjectKind::Network, o.network.id.as_str());
                    s.networks.insert(o.network.id.clone(), o);
                }
                Object::Template(o) => {
                    s.revive(ObjectKind::Template, &o.template.id);
                    s.templates.insert(o.template.id.clone(), o);
                }
                Object::Switch(o) => {
                    s.revive(ObjectKind::Switch, o.id.as_str());
                    s.switches.insert(o.id.clone(), o);
                }
                Object::Rollout(o) => {
                    s.revive(ObjectKind::Rollout, &o.id);
                    s.rollouts.insert(o.id.clone(), o);
                }
            }
        }
        for ObjectRef { kind, id } in changes.removed {
            // Kept so that `changes_since` on the view can tell its own readers what went.
            s.tombstone(changes.version, kind, id.clone());
            match kind {
                ObjectKind::Host => {
                    s.hosts.remove(&HostId::new(id));
                }
                ObjectKind::Token => {
                    s.tokens.remove(&id);
                }
                ObjectKind::Rule => {
                    s.rules.remove(&id);
                }
                ObjectKind::Alert => {
                    s.alerts.remove(&id);
                }
                ObjectKind::Event => {
                    if let Ok(seq) = id.parse::<u64>() {
                        s.events.remove(&seq);
                    }
                }
                ObjectKind::Cp => {}
                ObjectKind::Network => {
                    s.networks.remove(&NetworkId::new(id));
                }
                ObjectKind::Template => {
                    s.templates.remove(&id);
                }
                ObjectKind::Switch => {
                    s.switches.remove(&SwitchId::new(id));
                }
                ObjectKind::Rollout => {
                    s.rollouts.remove(&id);
                }
            }
        }
        // The leader's ring drops old events without tombstones; mirror it.
        while s.events.len() > EVENT_RING {
            s.events.pop_first();
        }
        s.version = changes.version;
        self.version = changes.version;
    }
}

struct Outstanding {
    op: ApiOp,
}

/// How much the client has synced: answers merged and the objects they upserted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncStats {
    pub syncs: u64,
    pub upserts: u64,
}

#[derive(Default)]
pub struct ClientActor {
    view: ClusterView,
    next_req: u64,
    outstanding: BTreeMap<u64, Outstanding>,
    results: Vec<(u64, ApiResult)>,
    sync_stats: SyncStats,
}

impl ClientActor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn view(&self) -> &ClusterView {
        &self.view
    }

    pub fn sync_stats(&self) -> SyncStats {
        self.sync_stats
    }

    /// Sends `op` to the leader via the VIP and returns its request id. The
    /// final answer shows up in [`ClientActor::take_results`].
    pub fn submit(&mut self, ctx: &mut dyn Ctx, op: ApiOp) -> u64 {
        self.next_req += 1;
        let req = self.next_req;
        ctx.send(VIP, mgmt(), Msg::Api { req, op: op.clone() });
        self.outstanding.insert(req, Outstanding { op });
        ctx.set_timer(API_GIVE_UP_MS, req * 2 + GIVE_UP);
        req
    }

    /// Final answers since the last call, in the order they arrived.
    pub fn take_results(&mut self) -> Vec<(u64, ApiResult)> {
        std::mem::take(&mut self.results)
    }

    /// The final answer to `req`, if it has arrived and was not taken yet.
    pub fn result(&self, req: u64) -> Option<&ApiResult> {
        self.results.iter().find(|(r, _)| *r == req).map(|(_, res)| res)
    }

    /// Takes the final answer to `req` only, leaving the others for [`ClientActor::take_results`].
    pub fn take_result(&mut self, req: u64) -> Option<ApiResult> {
        let i = self.results.iter().position(|(r, _)| *r == req)?;
        Some(self.results.remove(i).1)
    }

    fn finish(&mut self, req: u64, result: ApiResult) {
        if self.outstanding.remove(&req).is_some() {
            self.results.push((req, result));
        }
    }
}

impl Actor for ClientActor {
    fn on_start(&mut self, ctx: &mut dyn Ctx) {
        ctx.set_timer(SYNC_PERIOD_MS, SYNC);
    }

    fn on_message(&mut self, ctx: &mut dyn Ctx, _from: Endpoint, msg: Msg) {
        match msg {
            Msg::SyncResp { changes, leader, term, metrics } => {
                self.sync_stats.syncs += 1;
                self.sync_stats.upserts += changes.upserts.len() as u64;
                self.view.merge(changes);
                self.view.leader = Some(leader);
                self.view.term = term;
                self.view.last_sync_at = Some(ctx.now());
                self.view.metrics.extend(metrics);
            }
            // Never proposed, so trying again is safe.
            Msg::ApiResp { req, result: ApiResult::NotLeader } if self.outstanding.contains_key(&req) => {
                ctx.set_timer(NOT_LEADER_RETRY_MS, req * 2 + RETRY);
            }
            Msg::ApiResp { req, result } => self.finish(req, result),
            _ => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64) {
        if token == SYNC {
            ctx.send(VIP, mgmt(), Msg::Sync { since: self.view.version });
            ctx.set_timer(SYNC_PERIOD_MS, SYNC);
            return;
        }
        let req = token / 2;
        match token % 2 {
            RETRY => {
                if let Some(o) = self.outstanding.get(&req) {
                    ctx.send(VIP, mgmt(), Msg::Api { req, op: o.op.clone() });
                }
            }
            _ => self.finish(req, ApiResult::Unavailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Command, Lifecycle};
    use model::{profile, Inventory, ProfileId};

    fn add(id: &str) -> Command {
        Command::AddPendingHost {
            id: HostId::from(id),
            profile: profile(ProfileId::MiniPc, 0),
            inventory: Inventory::from_profile(ProfileId::MiniPc, 0),
            location: "rack-1".into(),
        }
    }

    /// Records what the client sends and which timers it sets.
    #[derive(Default)]
    struct FakeCtx {
        sent: Vec<Msg>,
        timers: Vec<(u64, u64)>,
    }

    impl Ctx for FakeCtx {
        fn now(&self) -> SimTime {
            0
        }
        fn me(&self) -> Endpoint {
            CLIENT
        }
        fn send(&mut self, to: Endpoint, _: model::NetworkId, msg: Msg) {
            assert_eq!(to, VIP, "the client only talks to the VIP");
            self.sent.push(msg);
        }
        fn set_timer(&mut self, after_ms: u64, token: u64) {
            self.timers.push((after_ms, token));
        }
        fn rand_u64(&mut self) -> u64 {
            4
        }
        fn claim_vip(&mut self) {}
        fn host(&mut self) -> Option<&mut dyn crate::runtime::HostOs> {
            None
        }
    }

    #[test]
    fn not_leader_is_retried_until_the_give_up_deadline() {
        let (mut c, mut ctx) = (ClientActor::new(), FakeCtx::default());
        let op = ApiOp::Cordon { id: HostId::from("h1") };
        let req = c.submit(&mut ctx, op.clone());
        let api = Msg::Api { req, op };
        assert_eq!(ctx.sent, std::slice::from_ref(&api));
        let give_up = ctx.timers.pop().unwrap();
        assert_eq!(give_up.0, API_GIVE_UP_MS);

        c.on_message(&mut ctx, VIP, Msg::ApiResp { req, result: ApiResult::NotLeader });
        let retry = ctx.timers.pop().unwrap();
        assert_eq!(retry.0, NOT_LEADER_RETRY_MS);
        assert!(c.take_results().is_empty(), "NotLeader is not a final answer");
        c.on_timer(&mut ctx, retry.1);
        assert_eq!(ctx.sent, [api.clone(), api]);

        c.on_timer(&mut ctx, give_up.1);
        assert_eq!(c.take_results(), [(req, ApiResult::Unavailable)]);
        // A late answer, or a retry timer still pending, changes nothing.
        c.on_message(&mut ctx, VIP, Msg::ApiResp { req, result: ApiResult::Ok(serde_json::Value::Null) });
        c.on_message(&mut ctx, VIP, Msg::ApiResp { req, result: ApiResult::NotLeader });
        c.on_timer(&mut ctx, retry.1);
        assert!(c.take_results().is_empty());
        assert_eq!(ctx.sent.len(), 2);
    }

    #[test]
    fn final_answer_is_reported_once() {
        let (mut c, mut ctx) = (ClientActor::new(), FakeCtx::default());
        let a = c.submit(&mut ctx, ApiOp::CreateJoinToken);
        let b = c.submit(&mut ctx, ApiOp::DeleteRule { id: "r".into() });
        assert_ne!(a, b);
        c.on_message(&mut ctx, VIP, Msg::ApiResp { req: b, result: ApiResult::Err("not found".into()) });
        c.on_message(&mut ctx, VIP, Msg::ApiResp { req: a, result: ApiResult::Unavailable });
        assert_eq!(c.take_results(), [(b, ApiResult::Err("not found".into())), (a, ApiResult::Unavailable)]);
        for (_, token) in ctx.timers.clone() {
            c.on_timer(&mut ctx, token);
        }
        assert!(c.take_results().is_empty(), "the give-up timer of an answered request is a no-op");
    }

    #[test]
    fn one_answer_can_be_taken_leaving_the_rest() {
        let (mut c, mut ctx) = (ClientActor::new(), FakeCtx::default());
        let a = c.submit(&mut ctx, ApiOp::CreateJoinToken);
        let b = c.submit(&mut ctx, ApiOp::CreateJoinToken);
        assert_eq!(c.result(b), None, "not answered yet");
        c.on_message(&mut ctx, VIP, Msg::ApiResp { req: a, result: ApiResult::Unavailable });
        c.on_message(&mut ctx, VIP, Msg::ApiResp { req: b, result: ApiResult::NotLeader });
        c.on_message(&mut ctx, VIP, Msg::ApiResp { req: b, result: ApiResult::Err("x".into()) });
        assert_eq!(c.result(b), Some(&ApiResult::Err("x".into())));
        assert_eq!(c.take_result(b), Some(ApiResult::Err("x".into())));
        assert_eq!(c.take_result(b), None, "taken once");
        assert_eq!(c.take_results(), [(a, ApiResult::Unavailable)]);
    }

    #[test]
    fn merge_tracks_the_leader_state_incrementally() {
        let mut leader = ClusterState::new();
        let mut view = ClusterView::default();
        leader.apply(1, add("h1")).unwrap();
        leader.apply(2, add("h2")).unwrap();
        leader.apply(3, Command::CreateJoinToken { token: "t".into(), ttl_ms: 10 }).unwrap();
        view.merge(leader.changes_since(view.version));
        assert_eq!(view.version, 3);
        assert_eq!(view.state.hosts.len(), 2);
        assert!(view.state.tokens.contains_key("t"));

        let since = view.version;
        let labels = [("a".into(), "b".into())].into();
        leader.apply(4, Command::SetLabels { id: HostId::from("h1"), labels }).unwrap();
        leader.apply(5, Command::RemoveHost { id: HostId::from("h2") }).unwrap();
        leader.apply(6, Command::Event { entity: ObjectRef::cp(), message: "hi".into() }).unwrap();
        let changes = leader.changes_since(since);
        assert_eq!(changes.upserts.len(), 2, "only what changed is sent: {changes:?}");
        view.merge(changes);

        assert_eq!(view.version, 6);
        assert_eq!(view.state.version, 6);
        assert_eq!(view.state.hosts, leader.hosts);
        assert_eq!(view.state.tokens, leader.tokens);
        assert_eq!(view.state.events, leader.events);
        assert_eq!(view.state.hosts[&HostId::from("h1")].lifecycle, Lifecycle::Pending);

        let gone = view.state.changes_since(since);
        assert_eq!(gone.removed, [ObjectRef::host(&HostId::from("h2"))], "removals are reported to readers of the view");
        assert_eq!(view.state.changes_since(view.version).removed, []);

        // Re-merging an overlapping batch is harmless.
        view.merge(leader.changes_since(0));
        assert_eq!(view.state.hosts, leader.hosts);
    }

    #[test]
    fn merge_carries_networks_and_templates() {
        let mut leader = ClusterState::new();
        let mut view = ClusterView::default();
        for n in model::seed_networks() {
            leader.apply(1, Command::UpsertNetwork { network: n }).unwrap();
        }
        for t in model::seed_templates() {
            leader.apply(1, Command::UpsertTemplate { template: t }).unwrap();
        }
        view.merge(leader.changes_since(0));
        assert_eq!((view.state.networks.len(), view.state.templates.len()), (5, 3));
        assert_eq!(view.state.networks, leader.networks);
        assert_eq!(view.state.templates, leader.templates);

        let since = view.version;
        leader.apply(2, Command::DeleteTemplate { id: "ms01-standard".into() }).unwrap();
        leader.apply(2, Command::DeleteNetwork { id: model::NetworkId::from("vm-iot") }).unwrap();
        view.merge(leader.changes_since(since));
        assert_eq!(view.state.networks, leader.networks);
        assert_eq!(view.state.templates, leader.templates);
    }
}
