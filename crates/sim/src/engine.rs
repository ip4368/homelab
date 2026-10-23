//! Discrete-event engine: a time-ordered queue of message deliveries and
//! timers, dispatched to actors one at a time. Events at equal time are
//! delivered in insertion order, so runs are fully deterministic per seed.

use control::proto::Msg;
use control::runtime::{Actor, Ctx, Endpoint, HostOs, SimTime};
use model::NetworkId;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

/// Decides whether and when a message arrives. Task 3 replaces the default
/// with a fabric that owns world state (links, VLANs, faults).
///
/// The engine resolves `Addr::Vip` to the current VIP holder before calling
/// `route`, so the fabric always sees a concrete destination. The returned
/// endpoint is where the message is actually delivered; a fabric may rewrite it.
pub trait Deliver: Send {
    /// Returns the resolved destination and one-way latency in ms, or `None`
    /// to drop the message.
    fn route(&mut self, from: &Endpoint, to: &Endpoint, net: &NetworkId) -> Option<(Endpoint, u64)>;
}

/// Delivers everything to the addressed endpoint with 1 ms latency.
pub struct AlwaysDeliver;

impl Deliver for AlwaysDeliver {
    fn route(&mut self, _from: &Endpoint, to: &Endpoint, _net: &NetworkId) -> Option<(Endpoint, u64)> {
        Some((to.clone(), 1))
    }
}

enum Event {
    Deliver { to: Endpoint, from: Endpoint, msg: Msg },
    /// `actor` is the incarnation that set the timer; a respawned actor at the
    /// same endpoint must not receive its predecessor's timers.
    Timer { to: Endpoint, actor: u64, token: u64 },
}

/// Queue entry ordered by (time, seq) only, so equal-time events keep insertion order.
struct Scheduled {
    at: SimTime,
    seq: u64,
    event: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

/// Everything an actor callback may touch. Kept separate from the actor map
/// so a running actor (taken out of the map) can borrow it mutably.
struct Core {
    now: SimTime,
    seq: u64,
    queue: BinaryHeap<Reverse<Scheduled>>,
    rng: ChaCha8Rng,
    deliver: Box<dyn Deliver>,
    vip_holder: Option<Endpoint>,
}

impl Core {
    fn push(&mut self, at: SimTime, event: Event) {
        self.seq += 1;
        self.queue.push(Reverse(Scheduled { at, seq: self.seq, event }));
    }

    fn resolve_vip(&self, to: Endpoint) -> Option<Endpoint> {
        if to.addr == control::runtime::Addr::Vip { self.vip_holder.clone() } else { Some(to) }
    }

    fn send(&mut self, from: Endpoint, to: Endpoint, net: NetworkId, msg: Msg) {
        let Some(to) = self.resolve_vip(to) else { return };
        let Some((resolved, latency)) = self.deliver.route(&from, &to, &net) else { return };
        let Some(resolved) = self.resolve_vip(resolved) else { return };
        let at = self.now.saturating_add(latency);
        self.push(at, Event::Deliver { to: resolved, from, msg });
    }
}

struct Slot {
    id: u64,
    actor: Box<dyn Actor>,
}

struct EngineCtx<'a> {
    core: &'a mut Core,
    me: Endpoint,
    actor: u64,
}

impl Ctx for EngineCtx<'_> {
    fn now(&self) -> SimTime {
        self.core.now
    }
    fn me(&self) -> Endpoint {
        self.me.clone()
    }
    fn send(&mut self, to: Endpoint, net: NetworkId, msg: Msg) {
        let from = self.me.clone();
        self.core.send(from, to, net, msg);
    }
    fn set_timer(&mut self, after_ms: u64, token: u64) {
        let at = self.core.now.saturating_add(after_ms);
        self.core.push(at, Event::Timer { to: self.me.clone(), actor: self.actor, token });
    }
    fn rand_u64(&mut self) -> u64 {
        self.core.rng.next_u64()
    }
    fn claim_vip(&mut self) {
        self.core.vip_holder = Some(self.me.clone());
    }
    fn host(&mut self) -> Option<&mut dyn HostOs> {
        // No host OS is modelled yet; later tasks attach one per host.
        None
    }
}

pub struct Engine {
    core: Core,
    actors: HashMap<Endpoint, Slot>,
    next_actor: u64,
}

impl Engine {
    pub fn new(seed: u64) -> Self {
        Self {
            core: Core {
                now: 0,
                seq: 0,
                queue: BinaryHeap::new(),
                rng: ChaCha8Rng::seed_from_u64(seed),
                deliver: Box::new(AlwaysDeliver),
                vip_holder: None,
            },
            actors: HashMap::new(),
            next_actor: 0,
        }
    }

    pub fn set_deliver(&mut self, d: Box<dyn Deliver>) {
        self.core.deliver = d;
    }

    /// Registers `actor` at `at` (replacing any actor already there) and runs
    /// its `on_start` at the current time.
    pub fn spawn(&mut self, at: Endpoint, actor: Box<dyn Actor>) {
        self.next_actor += 1;
        let id = self.next_actor;
        self.actors.insert(at.clone(), Slot { id, actor });
        self.with_actor(&at, |actor, ctx| actor.on_start(ctx));
    }

    /// Removes the actor; its pending timers and in-flight messages are
    /// dropped when they come due.
    pub fn kill(&mut self, at: &Endpoint) {
        self.actors.remove(at);
        if self.core.vip_holder.as_ref() == Some(at) {
            self.core.vip_holder = None;
        }
    }

    pub fn now(&self) -> SimTime {
        self.core.now
    }

    /// The endpoint that most recently claimed the VIP and is still alive.
    pub fn vip_holder(&self) -> Option<&Endpoint> {
        self.core.vip_holder.as_ref()
    }

    pub fn run_for(&mut self, ms: u64) {
        let deadline = self.core.now.saturating_add(ms);
        self.run_until(deadline, |_| false);
    }

    /// Dispatches events up to and including `deadline`, checking `pred` before
    /// the first event and after each one. Returns `true` as soon as it holds,
    /// leaving `now()` at the event that satisfied it. Otherwise advances `now()`
    /// to `deadline` and returns `false`.
    pub fn run_until(&mut self, deadline: SimTime, mut pred: impl FnMut(&Engine) -> bool) -> bool {
        if pred(self) {
            return true;
        }
        while let Some(Reverse(next)) = self.core.queue.peek() {
            if next.at > deadline {
                break;
            }
            let Reverse(Scheduled { at, event, .. }) = self.core.queue.pop().expect("peeked");
            self.core.now = at;
            self.dispatch(event);
            if pred(self) {
                return true;
            }
        }
        self.core.now = self.core.now.max(deadline);
        false
    }

    fn dispatch(&mut self, event: Event) {
        match event {
            Event::Deliver { to, from, msg } => {
                self.with_actor(&to, |actor, ctx| actor.on_message(ctx, from, msg));
            }
            Event::Timer { to, actor: id, token } => {
                if self.actors.get(&to).is_some_and(|s| s.id == id) {
                    self.with_actor(&to, |actor, ctx| actor.on_timer(ctx, token));
                }
            }
        }
    }

    /// Runs `f` on the actor at `at` (if any) with a context. The actor is taken
    /// out of the map for the call so the context can borrow the core mutably.
    fn with_actor(&mut self, at: &Endpoint, f: impl FnOnce(&mut dyn Actor, &mut dyn Ctx)) {
        let Some(mut slot) = self.actors.remove(at) else { return };
        let mut ctx = EngineCtx { core: &mut self.core, me: at.clone(), actor: slot.id };
        f(slot.actor.as_mut(), &mut ctx);
        self.actors.insert(at.clone(), slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control::runtime::{Addr, Ctx, Svc};
    use model::HostId;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, PartialEq)]
    enum Seen {
        Timer { token: u64, at: SimTime },
        Msg { msg: Msg, from: Endpoint, at: SimTime },
    }

    type Log = Arc<Mutex<Vec<Seen>>>;

    /// Test double: sets the given timers on start, pings `target` on start,
    /// answers every Ping with a Pong, and logs everything it observes.
    struct PingActor {
        log: Log,
        timers: Vec<(u64, u64)>,
        target: Option<Endpoint>,
        claim_vip: bool,
    }

    impl PingActor {
        fn new(log: &Log) -> Self {
            Self { log: log.clone(), timers: vec![], target: None, claim_vip: false }
        }
        fn timers(mut self, t: &[(u64, u64)]) -> Self {
            self.timers = t.to_vec();
            self
        }
        fn pings(mut self, to: Endpoint) -> Self {
            self.target = Some(to);
            self
        }
        fn claims_vip(mut self) -> Self {
            self.claim_vip = true;
            self
        }
    }

    impl Actor for PingActor {
        fn on_start(&mut self, ctx: &mut dyn Ctx) {
            if self.claim_vip {
                ctx.claim_vip();
            }
            for &(after, token) in &self.timers {
                ctx.set_timer(after, token);
            }
            if let Some(t) = self.target.clone() {
                ctx.send(t, NetworkId::from("mgmt"), Msg::Ping(7));
            }
        }
        fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg) {
            self.log.lock().unwrap().push(Seen::Msg { msg: msg.clone(), from: from.clone(), at: ctx.now() });
            if let Msg::Ping(n) = msg {
                ctx.send(from, NetworkId::from("mgmt"), Msg::Pong(n));
            }
        }
        fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64) {
            self.log.lock().unwrap().push(Seen::Timer { token, at: ctx.now() });
        }
    }

    fn ep(h: &str) -> Endpoint {
        Endpoint::new(Addr::Host(HostId::from(h)), Svc::Agent)
    }

    fn log() -> Log {
        Arc::new(Mutex::new(vec![]))
    }

    #[test]
    fn timers_fire_in_order() {
        let l = log();
        let mut e = Engine::new(1);
        e.spawn(ep("a"), Box::new(PingActor::new(&l).timers(&[(30, 3), (10, 1), (20, 2)])));
        e.run_for(100);
        assert_eq!(
            *l.lock().unwrap(),
            vec![
                Seen::Timer { token: 1, at: 10 },
                Seen::Timer { token: 2, at: 20 },
                Seen::Timer { token: 3, at: 30 },
            ]
        );
        assert_eq!(e.now(), 100);
    }

    #[test]
    fn equal_time_events_fire_in_insertion_order() {
        let l = log();
        let mut e = Engine::new(1);
        e.spawn(ep("a"), Box::new(PingActor::new(&l).timers(&[(5, 9), (5, 4), (5, 6)])));
        e.run_for(5);
        let tokens: Vec<u64> = l
            .lock()
            .unwrap()
            .iter()
            .filter_map(|s| if let Seen::Timer { token, .. } = s { Some(*token) } else { None })
            .collect();
        assert_eq!(tokens, [9, 4, 6]);
    }

    #[test]
    fn message_arrives_after_latency() {
        let (la, lb) = (log(), log());
        let mut e = Engine::new(1);
        e.spawn(ep("b"), Box::new(PingActor::new(&lb)));
        e.spawn(ep("a"), Box::new(PingActor::new(&la).pings(ep("b"))));
        e.run_for(10);
        assert_eq!(*lb.lock().unwrap(), vec![Seen::Msg { msg: Msg::Ping(7), from: ep("a"), at: 1 }]);
        // The Pong makes the return trip: another 1 ms.
        assert_eq!(*la.lock().unwrap(), vec![Seen::Msg { msg: Msg::Pong(7), from: ep("b"), at: 2 }]);
    }

    #[test]
    fn killed_actor_receives_nothing() {
        let (la, lb) = (log(), log());
        let mut e = Engine::new(1);
        e.spawn(ep("b"), Box::new(PingActor::new(&lb).timers(&[(5, 1)])));
        e.kill(&ep("b"));
        e.spawn(ep("a"), Box::new(PingActor::new(&la).pings(ep("b"))));
        e.run_for(10);
        assert!(lb.lock().unwrap().is_empty(), "killed actor saw {:?}", lb.lock().unwrap());
        assert!(la.lock().unwrap().is_empty(), "no Pong expected");
    }

    #[test]
    fn kill_after_send_drops_in_flight_message() {
        let (la, lb) = (log(), log());
        let mut e = Engine::new(1);
        e.spawn(ep("b"), Box::new(PingActor::new(&lb)));
        e.spawn(ep("a"), Box::new(PingActor::new(&la).pings(ep("b"))));
        e.kill(&ep("b")); // ping is already queued, destined for t=1
        e.run_for(10);
        assert!(lb.lock().unwrap().is_empty());
        assert!(la.lock().unwrap().is_empty());
    }

    #[test]
    fn respawn_does_not_inherit_old_timers() {
        let l = log();
        let mut e = Engine::new(1);
        e.spawn(ep("a"), Box::new(PingActor::new(&l).timers(&[(5, 1)])));
        e.kill(&ep("a"));
        e.spawn(ep("a"), Box::new(PingActor::new(&l).timers(&[(8, 2)])));
        e.run_for(20);
        assert_eq!(*l.lock().unwrap(), vec![Seen::Timer { token: 2, at: 8 }]);
    }

    #[test]
    fn run_until_stops_on_predicate() {
        let l = log();
        let mut e = Engine::new(1);
        e.spawn(ep("a"), Box::new(PingActor::new(&l).timers(&[(10, 1), (20, 2), (30, 3)])));
        let l2 = l.clone();
        let hit = e.run_until(100, move |_| l2.lock().unwrap().len() >= 2);
        assert!(hit);
        assert_eq!(e.now(), 20);
        assert_eq!(l.lock().unwrap().len(), 2);
    }

    #[test]
    fn run_until_returns_false_at_deadline() {
        let l = log();
        let mut e = Engine::new(1);
        e.spawn(ep("a"), Box::new(PingActor::new(&l).timers(&[(10, 1), (200, 2)])));
        let hit = e.run_until(50, |_| false);
        assert!(!hit);
        assert!(e.now() <= 50);
        assert_eq!(l.lock().unwrap().len(), 1, "event past the deadline must not fire");
    }

    #[test]
    fn vip_resolves_to_current_claimant() {
        let (la, lb) = (log(), log());
        let mut e = Engine::new(1);
        let vip = Endpoint::new(Addr::Vip, Svc::Cp);
        let leader = Endpoint::new(Addr::Host(HostId::from("cp-1")), Svc::Cp);
        e.spawn(leader.clone(), Box::new(PingActor::new(&lb).claims_vip()));
        e.spawn(ep("a"), Box::new(PingActor::new(&la).pings(vip)));
        e.run_for(10);
        assert_eq!(e.vip_holder(), Some(&leader));
        assert_eq!(*lb.lock().unwrap(), vec![Seen::Msg { msg: Msg::Ping(7), from: ep("a"), at: 1 }]);
        assert_eq!(la.lock().unwrap().len(), 1);
    }

    #[test]
    fn vip_without_claimant_drops() {
        let (la, lb) = (log(), log());
        let mut e = Engine::new(1);
        e.spawn(Endpoint::new(Addr::Host(HostId::from("cp-1")), Svc::Cp), Box::new(PingActor::new(&lb)));
        e.spawn(ep("a"), Box::new(PingActor::new(&la).pings(Endpoint::new(Addr::Vip, Svc::Cp))));
        e.run_for(10);
        assert!(lb.lock().unwrap().is_empty());
    }

    struct RandActor(Arc<Mutex<Vec<u64>>>);
    impl Actor for RandActor {
        fn on_start(&mut self, ctx: &mut dyn Ctx) {
            for _ in 0..4 {
                let v = ctx.rand_u64();
                self.0.lock().unwrap().push(v);
            }
        }
        fn on_message(&mut self, _: &mut dyn Ctx, _: Endpoint, _: Msg) {}
        fn on_timer(&mut self, _: &mut dyn Ctx, _: u64) {}
    }

    fn rolls(seed: u64) -> Vec<u64> {
        let out = Arc::new(Mutex::new(vec![]));
        Engine::new(seed).spawn(ep("a"), Box::new(RandActor(out.clone())));
        let v = out.lock().unwrap().clone();
        v
    }

    #[test]
    fn rng_is_seeded_deterministically() {
        assert_eq!(rolls(42), rolls(42));
        assert_ne!(rolls(42), rolls(43));
    }

    struct DropAll;
    impl Deliver for DropAll {
        fn route(&mut self, _: &Endpoint, _: &Endpoint, _: &NetworkId) -> Option<(Endpoint, u64)> {
            None
        }
    }

    struct Slow;
    impl Deliver for Slow {
        fn route(&mut self, _: &Endpoint, to: &Endpoint, _: &NetworkId) -> Option<(Endpoint, u64)> {
            Some((to.clone(), 25))
        }
    }

    #[test]
    fn custom_deliver_can_drop_or_delay() {
        let (la, lb) = (log(), log());
        let mut e = Engine::new(1);
        e.set_deliver(Box::new(DropAll));
        e.spawn(ep("b"), Box::new(PingActor::new(&lb)));
        e.spawn(ep("a"), Box::new(PingActor::new(&la).pings(ep("b"))));
        e.run_for(100);
        assert!(lb.lock().unwrap().is_empty());

        let (la, lb) = (log(), log());
        let mut e = Engine::new(1);
        e.set_deliver(Box::new(Slow));
        e.spawn(ep("b"), Box::new(PingActor::new(&lb)));
        e.spawn(ep("a"), Box::new(PingActor::new(&la).pings(ep("b"))));
        e.run_for(100);
        assert_eq!(*lb.lock().unwrap(), vec![Seen::Msg { msg: Msg::Ping(7), from: ep("a"), at: 25 }]);
        assert_eq!(*la.lock().unwrap(), vec![Seen::Msg { msg: Msg::Pong(7), from: ep("b"), at: 50 }]);
    }
}
