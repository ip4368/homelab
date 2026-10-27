//! The real runtime: what the simulator's engine is for simulated hosts.
//! A [`Node`] owns the actors of one machine (a host's agent and CP
//! replica, or the UI client) and drives them from one task: messages from
//! the [`Transport`], timers on the monotonic clock, and commands from
//! whoever embeds it. It implements [`Ctx`]; the actors cannot tell it from
//! the simulator.
//!
//! Beyond `Ctx`, the node does what a real host's plumbing would: it holds
//! the VIP exactly while its CP replica leads, keeps the transport's address
//! book from the replica's state and the leader's directory, and starts a CP
//! replica when the agent is asked to.

use crate::transport::{AddressBook, Inbound, Transport};
use control::controllers::directory::{directory, switch_addresses, Directory};
use control::proto::Msg;
use control::replica::CpReplica;
use control::runtime::{Actor, Addr, Ctx, Endpoint, HostOs, SimTime, Svc};
use model::{HostId, NetworkId};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

/// What a node needs from its machine beyond [`HostOs`].
pub trait NodeOs: HostOs + Send {
    /// The voters of a CP replica [`HostOs::spawn_cp_replica`] asked for
    /// since the last call, if it did. The node starts it.
    fn take_spawn(&mut self) -> Option<BTreeSet<HostId>>;
    /// Whether the host's own addresses changed since the last call (a
    /// network config was applied), so open connections are stale.
    fn take_net_changed(&mut self) -> bool {
        false
    }
}

/// Holding the VIP: an address on the mgmt network plus a gratuitous ARP.
pub trait VipControl: Send {
    /// Takes the VIP (adding the address and announcing it) or lets it go.
    fn hold(&mut self, hold: bool);
    /// While holding it: puts the address back if something removed it, and
    /// announces only then. Never announces otherwise: a frozen ex-leader,
    /// thawed, still thinks it holds the VIP until it hears a newer term,
    /// and must not shout over the real leader in that window.
    fn reassert(&mut self) {
        self.hold(true);
    }
}

/// Wall-clock time in Unix milliseconds: what [`Ctx::now`] means on a real host.
pub fn wall_ms() -> SimTime {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as SimTime)
}

/// Milliseconds since this process first asked, on the monotonic clock:
/// what [`Ctx::monotonic_ms`] means on a real host. Unlike [`wall_ms`], a
/// clock step (chrony, an operator) never moves it.
pub fn monotonic_ms() -> SimTime {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as SimTime
}

pub type Command = Box<dyn FnOnce(&mut Node) + Send>;

/// How often a node holding the VIP checks its address is still there.
const VIP_REFRESH: Duration = Duration::from_secs(5);

/// Everything a callback may touch; the actors live beside it so one can be
/// borrowed while its context is.
struct Core {
    me: Addr,
    transport: Transport,
    os: Option<Box<dyn NodeOs>>,
    timers: BinaryHeap<Reverse<(Instant, u64, Svc, u64)>>,
    seq: u64,
    local: VecDeque<Inbound>,
    rng: ChaCha8Rng,
    /// Whether this node holds the VIP: then messages to it stay local.
    vip_held: bool,
}

struct NodeCtx<'a> {
    me: Endpoint,
    core: &'a mut Core,
}

impl Ctx for NodeCtx<'_> {
    fn now(&self) -> SimTime {
        wall_ms()
    }
    fn monotonic_ms(&self) -> SimTime {
        monotonic_ms()
    }
    fn me(&self) -> Endpoint {
        self.me.clone()
    }
    fn send(&mut self, to: Endpoint, net: NetworkId, msg: Msg) {
        if to.addr == self.core.me || (to.addr == Addr::Vip && self.core.vip_held) {
            self.core.local.push_back(Inbound { from: self.me.clone(), to, msg });
        } else {
            self.core.transport.send(self.me.clone(), to, &net, msg);
        }
    }
    fn set_timer(&mut self, after_ms: u64, token: u64) {
        let at = Instant::now() + Duration::from_millis(after_ms);
        self.core.seq += 1;
        self.core.timers.push(Reverse((at, self.core.seq, self.me.svc, token)));
    }
    fn rand_u64(&mut self) -> u64 {
        self.core.rng.next_u64()
    }
    /// The node holds the VIP whenever its replica leads ([`Node::after`]).
    fn claim_vip(&mut self) {}
    fn host(&mut self) -> Option<&mut dyn HostOs> {
        self.core.os.as_deref_mut().map(|os| os as &mut dyn HostOs)
    }
}

pub struct NodeConfig {
    pub me: Addr,
    /// Where to accept connections; `None` for a node that only dials (the UI client).
    pub listen: Option<SocketAddr>,
    /// The port peers listen on.
    pub port: u16,
    pub book: AddressBook,
}

pub struct Node {
    core: Core,
    actors: HashMap<Svc, Box<dyn Actor>>,
    vip: Option<Box<dyn VipControl>>,
    /// The replica state version the address book was last taken from.
    book_version: Option<u64>,
    /// Told every directory the node learns, e.g. to keep it on disk.
    on_directory: Option<Box<dyn FnMut(&Directory) + Send>>,
    last_directory: Directory,
}

impl Node {
    /// A node with no actors yet. Must be called inside a tokio runtime.
    pub fn new(cfg: &NodeConfig, os: Option<Box<dyn NodeOs>>, mut vip: Option<Box<dyn VipControl>>) -> (Self, mpsc::UnboundedReceiver<Inbound>) {
        // A previous process (crashed, or restarted while leading) may have
        // left the VIP's address up: this one holds nothing until it leads.
        if let Some(v) = vip.as_mut() {
            v.hold(false);
        }
        let (transport, inbox) = Transport::new(cfg.me.clone(), cfg.port, cfg.book.clone());
        let core = Core {
            me: cfg.me.clone(),
            transport,
            os,
            timers: BinaryHeap::new(),
            seq: 0,
            local: VecDeque::new(),
            rng: ChaCha8Rng::seed_from_u64(wall_ms() ^ u64::from(std::process::id())),
            vip_held: false,
        };
        let node = Self { core, actors: HashMap::new(), vip, book_version: None, on_directory: None, last_directory: Directory::new() };
        (node, inbox)
    }

    pub fn transport(&self) -> &Transport {
        &self.core.transport
    }

    pub fn on_directory(&mut self, f: impl FnMut(&Directory) + Send + 'static) {
        self.on_directory = Some(Box::new(f));
    }

    /// Starts `actor` as this node's `svc`, replacing any running there.
    pub fn start(&mut self, svc: Svc, actor: Box<dyn Actor>) {
        self.actors.insert(svc, actor);
        self.call(svc, |a, ctx| a.on_start(ctx));
    }

    pub fn actor<A: Actor>(&self, svc: Svc) -> Option<&A> {
        let any: &dyn Any = self.actors.get(&svc)?.as_ref();
        any.downcast_ref()
    }

    /// Runs `f` on actor `svc` (if it is an `A`) with a context, as a callback would.
    pub fn with_actor<A: Actor, R>(&mut self, svc: Svc, f: impl FnOnce(&mut A, &mut dyn Ctx) -> R) -> Option<R> {
        let mut actor = self.actors.remove(&svc)?;
        let any: &mut dyn Any = actor.as_mut();
        let result = any.downcast_mut::<A>().map(|a| {
            let mut ctx = NodeCtx { me: Endpoint::new(self.core.me.clone(), svc), core: &mut self.core };
            f(a, &mut ctx)
        });
        self.actors.insert(svc, actor);
        self.after();
        result
    }

    fn call(&mut self, svc: Svc, f: impl FnOnce(&mut dyn Actor, &mut dyn Ctx)) {
        let Some(mut actor) = self.actors.remove(&svc) else { return };
        let mut ctx = NodeCtx { me: Endpoint::new(self.core.me.clone(), svc), core: &mut self.core };
        f(actor.as_mut(), &mut ctx);
        self.actors.insert(svc, actor);
        self.after();
    }

    /// What follows every callback: start a CP replica the agent was asked
    /// for, drop connections from addresses the host no longer has, hold or
    /// release the VIP, and refresh the address book from the replica.
    fn after(&mut self) {
        if let Some(os) = self.core.os.as_mut() {
            if os.take_net_changed() {
                self.core.transport.reset();
                // Reconfiguring the interface may have dropped the VIP's address.
                if self.core.vip_held
                    && let Some(vip) = &mut self.vip
                {
                    vip.reassert();
                }
            }
            if let Some(voters) = os.take_spawn()
                && !self.actors.contains_key(&Svc::Cp)
                && let Addr::Host(me) = self.core.me.clone()
            {
                self.start(Svc::Cp, Box::new(CpReplica::new(me, None, voters)));
            }
        }
        let replica = self.actor::<CpReplica>(Svc::Cp);
        let leads = replica.is_some_and(CpReplica::is_leader);
        let fresh = replica.map(|r| r.state().version).filter(|v| Some(*v) != self.book_version);
        if let Some(version) = fresh {
            let state = self.actor::<CpReplica>(Svc::Cp).expect("replica checked above").state();
            let (dir, switches) = (directory(state), switch_addresses(state));
            self.book_version = Some(version);
            self.learn_directory(dir);
            self.core.transport.update_book(|b| b.fixed.extend(switches.into_iter().map(|(s, ip)| (Addr::Switch(s), ip))));
        }
        if leads != self.core.vip_held {
            self.core.vip_held = leads;
            eprintln!("node: {} the VIP", if leads { "taking" } else { "releasing" });
            if let Some(vip) = &mut self.vip {
                vip.hold(leads);
            }
        }
    }

    fn learn_directory(&mut self, dir: Directory) {
        if dir.is_empty() || dir == self.last_directory {
            return;
        }
        self.last_directory = dir.clone();
        if let Some(f) = &mut self.on_directory {
            f(&dir);
        }
        self.core.transport.update_book(|b| b.hosts.extend(dir));
    }

    /// Hands an arriving message to the actor it is for: one at this node's
    /// address, or at the VIP. One for the VIP can reach a node not holding
    /// it (a stale ARP entry, a release in flight); its replica answers
    /// `NotLeader`, so the sender retries rather than waits out a timeout.
    fn deliver(&mut self, m: Inbound) {
        if let Msg::Directory { hosts } = &m.msg {
            self.learn_directory(hosts.clone());
        }
        if m.to.addr != self.core.me && m.to.addr != Addr::Vip {
            return;
        }
        let Inbound { from, to, msg } = m;
        self.call(to.svc, |a, ctx| a.on_message(ctx, from, msg));
    }

    fn fire_due(&mut self) {
        let now = Instant::now();
        while let Some(Reverse((at, _, svc, token))) = self.core.timers.peek().copied()
            && at <= now
        {
            self.core.timers.pop();
            self.call(svc, |a, ctx| a.on_timer(ctx, token));
        }
    }

    fn drain_local(&mut self) {
        while let Some(m) = self.core.local.pop_front() {
            self.deliver(m);
        }
    }

    /// Puts the VIP's address back if this node should hold it and
    /// something removed it ([`VipControl::reassert`]).
    fn reassert_vip(&mut self) {
        if self.core.vip_held
            && let Some(vip) = &mut self.vip
        {
            vip.reassert();
        }
    }

    /// Runs the node until its command channel closes.
    pub async fn run(mut self, mut inbox: mpsc::UnboundedReceiver<Inbound>, mut cmds: mpsc::UnboundedReceiver<Command>) {
        let mut vip_refresh = tokio::time::interval(VIP_REFRESH);
        loop {
            self.drain_local();
            let next = self.core.timers.peek().map(|Reverse((at, ..))| *at);
            let sleep = tokio::time::sleep_until(next.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600)));
            tokio::select! {
                Some(m) = inbox.recv() => self.deliver(m),
                cmd = cmds.recv() => match cmd {
                    Some(cmd) => cmd(&mut self),
                    None => return,
                },
                () = sleep => self.fire_due(),
                _ = vip_refresh.tick() => self.reassert_vip(),
            }
        }
    }
}

/// A node running on its own thread, driven through commands.
#[derive(Clone)]
pub struct NodeHandle {
    cmds: mpsc::UnboundedSender<Command>,
    /// Where it accepts connections, if it listens.
    pub listening: Option<SocketAddr>,
}

impl NodeHandle {
    /// Starts a machine-less node (e.g. the UI client) on a new thread with
    /// its own single-threaded runtime; `build` starts its actors.
    pub fn spawn(cfg: NodeConfig, build: impl FnOnce(&mut Node) + Send + 'static) -> std::io::Result<Self> {
        Self::spawn_with(cfg, None, build)
    }

    /// Like [`NodeHandle::spawn`], for a node on machine `os`.
    pub fn spawn_with(cfg: NodeConfig, os: Option<Box<dyn NodeOs>>, build: impl FnOnce(&mut Node) + Send + 'static) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        std::thread::Builder::new().name(format!("node-{:?}", cfg.me)).spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a tokio runtime");
            rt.block_on(async move {
                let (mut node, inbox) = Node::new(&cfg, os, None);
                let mut listening = None;
                if let Some(bind) = cfg.listen {
                    match node.transport().listen(bind).await {
                        Ok(a) => listening = Some(a),
                        Err(e) => {
                            let _ = ready_tx.send(Err(e));
                            return;
                        }
                    }
                }
                build(&mut node);
                let _ = ready_tx.send(Ok(listening));
                node.run(inbox, rx).await;
            });
        })?;
        let listening = ready_rx.recv().map_err(|_| std::io::Error::other("node thread died"))??;
        Ok(Self { cmds: tx, listening })
    }

    /// Runs `f` on the node and returns its result; `None` if the node is gone.
    pub async fn exec<R: Send + 'static>(&self, f: impl FnOnce(&mut Node) -> R + Send + 'static) -> Option<R> {
        let (tx, rx) = oneshot::channel();
        self.cmds.send(Box::new(move |n| {
            let _ = tx.send(f(n));
        })).ok()?;
        rx.await.ok()
    }

    /// Like [`NodeHandle::exec`], for a caller that is not async.
    pub fn exec_blocking<R: Send + 'static>(&self, f: impl FnOnce(&mut Node) -> R + Send + 'static) -> Option<R> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.cmds.send(Box::new(move |n| {
            let _ = tx.send(f(n));
        })).ok()?;
        rx.recv().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::PORT;
    use crate::testing::MemOs;
    use control::agent::HostAgent;
    use control::client::ClientActor;
    use control::proto::{ApiOp, ApiResult};
    use model::{Inventory, ProfileId};
    use std::net::Ipv4Addr;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct VipLog(Arc<Mutex<Vec<bool>>>);

    impl VipControl for VipLog {
        fn hold(&mut self, hold: bool) {
            self.0.lock().unwrap().push(hold);
        }
    }

    /// A one-host cluster on loopback: the host's agent and replica on one
    /// node (VIP at 127.0.0.1 on an ephemeral port), the UI client on another.
    #[tokio::test]
    async fn a_single_host_elects_itself_holds_the_vip_and_answers_the_client() {
        let id = HostId::from("ms01-1");
        let voters: BTreeSet<HostId> = [id.clone()].into();
        let durable = control::bootstrap::seeded_durable(
            &[control::bootstrap::SeedHost {
                id: id.clone(),
                inventory: Inventory::from_profile(ProfileId::Ms01, 0),
                location: "lab".into(),
                running: None,
            }],
            &voters,
            wall_ms(),
        );
        let os = MemOs::new(id.clone(), Some(durable));
        let vip = VipLog::default();
        let cfg = NodeConfig { me: Addr::Host(id.clone()), listen: None, port: PORT, book: AddressBook::default() };
        let (mut host, host_inbox) = Node::new(&cfg, Some(Box::new(os)), Some(Box::new(vip.clone())));
        let port = host.transport().listen("127.0.0.1:0".parse().unwrap()).await.unwrap().port();
        host.start(Svc::Cp, Box::new(CpReplica::new(id.clone(), None, voters)));
        host.start(Svc::Agent, Box::new(HostAgent::new(None)));
        let (_host_cmds, host_rx) = mpsc::unbounded_channel();
        tokio::spawn(host.run(host_inbox, host_rx));

        let mut book = AddressBook::default();
        book.fixed.insert(Addr::Vip, Ipv4Addr::LOCALHOST);
        let ccfg = NodeConfig { me: Addr::Client, listen: None, port, book };
        let (mut client, client_inbox) = Node::new(&ccfg, None, None);
        client.start(Svc::Client, Box::new(ClientActor::new()));
        let (client_cmds, client_rx) = mpsc::unbounded_channel::<Command>();
        tokio::spawn(client.run(client_inbox, client_rx));

        let ask = |f: Box<dyn FnOnce(&mut Node) -> Option<ApiResult> + Send>| {
            let (tx, rx) = oneshot::channel();
            client_cmds.send(Box::new(move |n: &mut Node| { let _ = tx.send(f(n)); })).unwrap();
            rx
        };
        let req = {
            let (tx, rx) = oneshot::channel();
            client_cmds
                .send(Box::new(move |n: &mut Node| {
                    let _ = tx.send(n.with_actor::<ClientActor, _>(Svc::Client, |c, ctx| c.submit(ctx, ApiOp::CreateJoinToken)));
                }))
                .unwrap();
            rx.await.unwrap().unwrap()
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        let answer = loop {
            let got = ask(Box::new(move |n| n.with_actor::<ClientActor, _>(Svc::Client, |c, _| c.take_result(req)).flatten())).await.unwrap();
            if let Some(a) = got {
                break a;
            }
            assert!(Instant::now() < deadline, "no answer within 10 s");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let ApiResult::Ok(token) = answer else { panic!("{answer:?}") };
        assert_eq!(token.as_str().map(str::len), Some(16));
        assert_eq!(*vip.0.lock().unwrap(), [false, true], "released any stale VIP at start, then took it once as leader");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (tx, rx) = oneshot::channel();
            client_cmds.send(Box::new(move |n: &mut Node| { let _ = tx.send(n.actor::<ClientActor>(Svc::Client).unwrap().view().leader.clone()); })).unwrap();
            if rx.await.unwrap() == Some(id.clone()) {
                break;
            }
            assert!(Instant::now() < deadline, "the client's view never named the leader");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
