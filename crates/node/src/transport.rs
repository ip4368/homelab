//! The real network between nodes: TCP connections carrying [`Wire`]
//! frames. It is lossy: `send` never blocks or fails, and a message that
//! cannot be delivered (no address, the peer unreachable, its queue full,
//! the connection dying) is dropped. Raft, heartbeats and the controllers'
//! resends already live with loss.
//!
//! One connection per peer is used in both directions: a reply to an
//! endpoint that dialled in goes back over its connection, which is how a
//! leader answers a host that has no cluster address yet, or the UI client.

use crate::wire::{read_frame, write_frame, Wire};
use control::controllers::directory::Directory;
use control::proto::Msg;
use control::runtime::{Addr, Endpoint};
use model::NetworkId;
use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{timeout, Instant};

/// Messages queued for one peer before newer ones are dropped.
const QUEUE: usize = 4096;
/// How long a dial may keep retrying before its queued messages are dropped.
/// Covers a peer, or this host, whose addresses are being reconfigured.
const DIAL_FOR: Duration = Duration::from_secs(3);
const DIAL_ATTEMPT: Duration = Duration::from_millis(500);
const DIAL_RETRY: Duration = Duration::from_millis(250);
/// An idle connection sends a keepalive this often...
const KEEPALIVE: Duration = Duration::from_secs(1);
/// ...and one that heard nothing for this long is dead. This is what notices
/// a frozen or vanished peer, or a VIP that moved to another host.
const SILENCE: Duration = Duration::from_secs(3);

/// A message that arrived for an actor on this node.
#[derive(Debug, Clone, PartialEq)]
pub struct Inbound {
    pub from: Endpoint,
    pub to: Endpoint,
    pub msg: Msg,
}

/// Where to dial an address.
#[derive(Debug, Clone, Default)]
pub struct AddressBook {
    /// Addresses that are not hosts: the VIP, switch management interfaces.
    pub fixed: HashMap<Addr, Ipv4Addr>,
    /// Hosts' static addresses by network.
    pub hosts: Directory,
}

impl AddressBook {
    pub fn resolve(&self, addr: &Addr, net: &NetworkId) -> Option<Ipv4Addr> {
        match addr {
            Addr::Host(h) => self.hosts.get(h)?.get(net).copied(),
            other => self.fixed.get(other).copied(),
        }
    }
}

struct Conn {
    id: u64,
    tx: mpsc::Sender<Wire>,
}

struct Inner {
    me: Addr,
    port: u16,
    conns: Mutex<HashMap<Addr, Vec<Conn>>>,
    book: Mutex<AddressBook>,
    inbox: mpsc::UnboundedSender<Inbound>,
    next_id: AtomicU64,
}

#[derive(Clone)]
pub struct Transport {
    inner: Arc<Inner>,
}

impl Transport {
    /// A transport for the node at `me`, dialling peers on `port`. Arriving
    /// messages come out of the returned receiver. Must be created inside a
    /// tokio runtime, which runs its connections.
    pub fn new(me: Addr, port: u16, book: AddressBook) -> (Self, mpsc::UnboundedReceiver<Inbound>) {
        let (inbox, rx) = mpsc::unbounded_channel();
        let inner = Inner {
            me,
            port,
            conns: Mutex::new(HashMap::new()),
            book: Mutex::new(book),
            inbox,
            next_id: AtomicU64::new(0),
        };
        (Self { inner: Arc::new(inner) }, rx)
    }

    /// Accepts connections on `bind` until the transport is dropped.
    pub async fn listen(&self, bind: SocketAddr) -> io::Result<SocketAddr> {
        let listener = TcpListener::bind(bind).await?;
        let local = listener.local_addr()?;
        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    // E.g. out of descriptors: back off rather than spin.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                let Some(inner) = weak.upgrade() else { return };
                tokio::spawn(accepted(Transport { inner }, stream));
            }
        });
        Ok(local)
    }

    pub fn update_book(&self, f: impl FnOnce(&mut AddressBook)) {
        f(&mut self.inner.book.lock().expect("book lock"));
    }

    pub fn book(&self) -> AddressBook {
        self.inner.book.lock().expect("book lock").clone()
    }

    /// Sends `msg` to `to` over `net`, or drops it. Never blocks.
    pub fn send(&self, from: Endpoint, to: Endpoint, net: &NetworkId, msg: Msg) {
        let addr = to.addr.clone();
        let frame = Wire::Msg { from, to, msg };
        let mut conns = self.inner.conns.lock().expect("conns lock");
        if let Some(c) = conns.get(&addr).and_then(|cs| cs.last()) {
            let _ = c.tx.try_send(frame);
            return;
        }
        let Some(ip) = self.inner.book.lock().expect("book lock").resolve(&addr, net) else { return };
        let (tx, rx) = mpsc::channel(QUEUE);
        let _ = tx.try_send(frame);
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        conns.entry(addr.clone()).or_default().push(Conn { id, tx });
        let target = SocketAddr::V4(SocketAddrV4::new(ip, self.inner.port));
        tokio::spawn(dialled(self.clone(), addr, id, target, rx));
    }

    /// Drops every connection, e.g. because this host's addresses changed
    /// and connections from the old ones would hang until they time out.
    pub fn reset(&self) {
        self.inner.conns.lock().expect("conns lock").clear();
    }

    /// Whether a connection to `addr` is open or being dialled.
    pub fn connected(&self, addr: &Addr) -> bool {
        self.inner.conns.lock().expect("conns lock").get(addr).is_some_and(|cs| !cs.is_empty())
    }

    fn register(&self, addr: Addr, tx: mpsc::Sender<Wire>) -> u64 {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.conns.lock().expect("conns lock").entry(addr).or_default().push(Conn { id, tx });
        id
    }

    fn forget(&self, addr: &Addr, id: u64) {
        let mut conns = self.inner.conns.lock().expect("conns lock");
        if let Some(cs) = conns.get_mut(addr) {
            cs.retain(|c| c.id != id);
            if cs.is_empty() {
                conns.remove(addr);
            }
        }
    }

    /// Whether connection `id` to `addr` is still wanted (not reset or replaced).
    fn wanted(&self, addr: &Addr, id: u64) -> bool {
        self.inner.conns.lock().expect("conns lock").get(addr).is_some_and(|cs| cs.iter().any(|c| c.id == id))
    }
}

async fn dial(target: SocketAddr) -> io::Result<TcpStream> {
    let deadline = Instant::now() + DIAL_FOR;
    loop {
        match timeout(DIAL_ATTEMPT, TcpStream::connect(target)).await {
            Ok(Ok(s)) => return Ok(s),
            Ok(Err(e)) if Instant::now() + DIAL_RETRY >= deadline => return Err(e),
            Err(_) if Instant::now() >= deadline => return Err(io::ErrorKind::TimedOut.into()),
            _ => tokio::time::sleep(DIAL_RETRY).await,
        }
    }
}

async fn dialled(t: Transport, addr: Addr, id: u64, target: SocketAddr, rx: mpsc::Receiver<Wire>) {
    if let Ok(mut stream) = dial(target).await
        && t.wanted(&addr, id)
    {
        let _ = stream.set_nodelay(true);
        let hello = Wire::Hello { from: t.inner.me.clone() };
        if write_frame(&mut stream, &hello).await.is_ok() {
            run_conn(&t, stream, rx).await;
        }
    }
    t.forget(&addr, id);
}

async fn accepted(t: Transport, mut stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let Ok(Ok(Wire::Hello { from })) = timeout(SILENCE, read_frame(&mut stream)).await else { return };
    let (tx, rx) = mpsc::channel(QUEUE);
    let id = t.register(from.clone(), tx);
    run_conn(&t, stream, rx).await;
    t.forget(&from, id);
}

/// Carries frames both ways until either side fails or falls silent, or the
/// connection is no longer registered (its sender dropped).
async fn run_conn(t: &Transport, stream: TcpStream, mut rx: mpsc::Receiver<Wire>) {
    let (mut r, mut w) = stream.into_split();
    let inbox = t.inner.inbox.clone();
    let mut reader = tokio::spawn(async move {
        loop {
            match timeout(SILENCE, read_frame(&mut r)).await {
                Ok(Ok(Wire::Msg { from, to, msg })) => {
                    if inbox.send(Inbound { from, to, msg }).is_err() {
                        return;
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => return,
            }
        }
    });
    let mut keepalive = tokio::time::interval(KEEPALIVE);
    loop {
        tokio::select! {
            frame = rx.recv() => {
                let Some(frame) = frame else { break };
                if timeout(SILENCE, write_frame(&mut w, &frame)).await.map_or(true, |r| r.is_err()) {
                    break;
                }
            }
            _ = keepalive.tick() => {
                if timeout(SILENCE, write_frame(&mut w, &Wire::Keepalive)).await.map_or(true, |r| r.is_err()) {
                    break;
                }
            }
            _ = &mut reader => break,
        }
    }
    reader.abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use control::proto::mgmt;
    use control::runtime::Svc;
    use model::HostId;

    fn host(s: &str) -> Addr {
        Addr::Host(HostId::from(s))
    }

    fn ep(a: &Addr, svc: Svc) -> Endpoint {
        Endpoint::new(a.clone(), svc)
    }

    async fn recv(rx: &mut mpsc::UnboundedReceiver<Inbound>) -> Inbound {
        timeout(Duration::from_secs(5), rx.recv()).await.expect("a message within 5 s").expect("inbox open")
    }

    /// Two transports on loopback, `a` able to dial `b` (but not the reverse).
    async fn pair() -> (Transport, mpsc::UnboundedReceiver<Inbound>, Transport, mpsc::UnboundedReceiver<Inbound>) {
        let (b, b_rx) = Transport::new(host("b"), 0, AddressBook::default());
        let port = b.listen("127.0.0.1:0".parse().unwrap()).await.unwrap().port();
        let mut book = AddressBook::default();
        book.hosts.insert(HostId::from("b"), [(mgmt(), Ipv4Addr::LOCALHOST)].into());
        let (a, a_rx) = Transport::new(host("a"), port, book);
        (a, a_rx, b, b_rx)
    }

    #[tokio::test]
    async fn a_reply_goes_back_over_the_connection_the_peer_dialled() {
        let (a, mut a_rx, b, mut b_rx) = pair().await;
        a.send(ep(&host("a"), Svc::Agent), ep(&host("b"), Svc::Cp), &mgmt(), Msg::Ping(1));
        let got = recv(&mut b_rx).await;
        assert_eq!((got.from.clone(), got.msg), (ep(&host("a"), Svc::Agent), Msg::Ping(1)));
        // `b` has no address for `a`: only the open connection can carry this.
        b.send(ep(&host("b"), Svc::Cp), got.from, &mgmt(), Msg::Pong(1));
        assert_eq!(recv(&mut a_rx).await.msg, Msg::Pong(1));
    }

    #[tokio::test]
    async fn messages_to_an_unknown_address_are_dropped() {
        let (a, _a_rx) = Transport::new(host("a"), 1, AddressBook::default());
        a.send(ep(&host("a"), Svc::Agent), ep(&host("nobody"), Svc::Cp), &mgmt(), Msg::Ping(1));
        assert!(!a.connected(&host("nobody")));
    }

    #[tokio::test]
    async fn a_dropped_connection_is_noticed_and_redialled() {
        let (a, _a_rx, b, mut b_rx) = pair().await;
        a.send(ep(&host("a"), Svc::Agent), ep(&host("b"), Svc::Cp), &mgmt(), Msg::Ping(1));
        recv(&mut b_rx).await;
        assert!(a.connected(&host("b")));
        // `b` forgets the connection: its side closes, and `a` notices (by EOF here; by silence for a frozen peer).
        b.reset();
        tokio::time::sleep(SILENCE + Duration::from_secs(1)).await;
        assert!(!a.connected(&host("b")), "the silent connection is gone");
        a.send(ep(&host("a"), Svc::Agent), ep(&host("b"), Svc::Cp), &mgmt(), Msg::Ping(2));
        assert_eq!(recv(&mut b_rx).await.msg, Msg::Ping(2), "a fresh dial carries the next message");
    }

    #[tokio::test]
    async fn a_dial_keeps_trying_while_the_peer_comes_up() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let mut book = AddressBook::default();
        book.hosts.insert(HostId::from("b"), [(mgmt(), Ipv4Addr::LOCALHOST)].into());
        let (a, _a_rx) = Transport::new(host("a"), port, book);
        a.send(ep(&host("a"), Svc::Agent), ep(&host("b"), Svc::Cp), &mgmt(), Msg::Ping(9));
        tokio::time::sleep(Duration::from_millis(700)).await;
        let (b, mut b_rx) = Transport::new(host("b"), 0, AddressBook::default());
        b.listen(SocketAddr::from(([127, 0, 0, 1], port))).await.unwrap();
        assert_eq!(recv(&mut b_rx).await.msg, Msg::Ping(9), "queued while dialling, delivered once up");
    }
}
