//! Puts [`Core`] on the wire. One thread polls every port's socket, runs
//! each arriving frame through the core and sends what comes out; it also
//! sends the switches' own LACP and LLDP. The lab changes the switches
//! through [`LabSwitch::with`], and cables a port to a VM NIC (whose QEMU
//! socket frames are sent to) with [`LabSwitch::cable`].

use super::core::{Core, PortId};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const TICK: Duration = Duration::from_millis(100);
const SOCK_BUF: i32 = 4 << 20;

/// How one external port reaches its far end.
pub enum Wire {
    /// A port a VM NIC can be cabled to: QEMU's `dgram` netdev sends here,
    /// and we send to the socket of the NIC the port is cabled to.
    Unix(UnixDatagram),
    /// The lab host's kernel, through a tap interface.
    Tap(File),
}

impl Wire {
    /// Binds the port's socket at `path` (replacing a stale socket file).
    pub fn unix(path: &Path) -> io::Result<Self> {
        let _ = std::fs::remove_file(path);
        let sock = UnixDatagram::bind(path)?;
        sock.set_nonblocking(true)?;
        set_bufs(sock.as_raw_fd());
        Ok(Wire::Unix(sock))
    }

    fn fd(&self) -> RawFd {
        match self {
            Wire::Unix(sock) => sock.as_raw_fd(),
            Wire::Tap(f) => f.as_raw_fd(),
        }
    }

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Wire::Unix(sock) => sock.recv(buf),
            Wire::Tap(f) => f.read(buf),
        }
    }

    /// Sends one frame to `peer` (for a unix port), or drops it: nothing is
    /// cabled, the far end is not listening, or its buffer is full. The wire
    /// is lossy, like a real one under load.
    fn send(&mut self, frame: &[u8], peer: Option<&PathBuf>) {
        let _ = match (self, peer) {
            (Wire::Unix(sock), Some(peer)) => sock.send_to(frame, peer).map(|_| ()),
            (Wire::Unix(_), None) => Ok(()),
            (Wire::Tap(f), _) => f.write(frame).map(|_| ()),
        };
    }
}

fn set_bufs(fd: RawFd) {
    let size = SOCK_BUF;
    for opt in [libc::SO_RCVBUFFORCE, libc::SO_SNDBUFFORCE, libc::SO_RCVBUF, libc::SO_SNDBUF] {
        // SAFETY: setsockopt with a valid fd and a pointer to an i32 of the stated size.
        unsafe {
            libc::setsockopt(fd, libc::SOL_SOCKET, opt, (&raw const size).cast(), size_of::<i32>() as u32);
        }
    }
}

/// Creates (or attaches to) tap interface `name`: Ethernet frames, no
/// packet-info header, non-blocking. Needs CAP_NET_ADMIN.
pub fn open_tap(name: &str) -> io::Result<File> {
    const TUNSETIFF: libc::c_ulong = 0x4004_54ca;
    const IFF_TAP: i16 = 0x0002;
    const IFF_NO_PI: i16 = 0x1000;
    let f = OpenOptions::new().read(true).write(true).open("/dev/net/tun")?;
    let mut ifr = [0u8; 40];
    if name.len() >= 16 {
        return Err(io::Error::other(format!("interface name {name:?} too long")));
    }
    ifr[..name.len()].copy_from_slice(name.as_bytes());
    ifr[16..18].copy_from_slice(&(IFF_TAP | IFF_NO_PI).to_ne_bytes());
    // SAFETY: TUNSETIFF reads and writes a struct ifreq, which `ifr` is large enough to hold.
    if unsafe { libc::ioctl(f.as_raw_fd(), TUNSETIFF, ifr.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fcntl on a valid fd.
    unsafe {
        let flags = libc::fcntl(f.as_raw_fd(), libc::F_GETFL);
        libc::fcntl(f.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    Ok(f)
}

struct Shared {
    core: Core,
    /// The NIC socket each unix port is cabled to.
    peers: HashMap<PortId, PathBuf>,
}

/// The running switches.
pub struct LabSwitch {
    shared: Arc<Mutex<Shared>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl LabSwitch {
    /// Starts forwarding between `wires` (by the port they attach to).
    pub fn start(core: Core, wires: Vec<(PortId, Wire)>) -> io::Result<Self> {
        let shared = Arc::new(Mutex::new(Shared { core, peers: HashMap::new() }));
        let stop = Arc::new(AtomicBool::new(false));
        let (c, s) = (shared.clone(), stop.clone());
        let thread = std::thread::Builder::new().name("labswitch".into()).spawn(move || forward(&c, &s, wires))?;
        Ok(Self { shared, stop, thread: Some(thread) })
    }

    /// Runs `f` on the switches (to read or change them).
    pub fn with<R>(&self, f: impl FnOnce(&mut Core) -> R) -> R {
        f(&mut self.shared.lock().expect("switch lock").core)
    }

    /// Cables unix port `port` to the NIC socket at `nic` (or uncables it).
    pub fn cable(&self, port: PortId, nic: Option<PathBuf>) {
        let mut s = self.shared.lock().expect("switch lock");
        match nic {
            Some(p) => s.peers.insert(port, p),
            None => s.peers.remove(&port),
        };
    }
}

impl Drop for LabSwitch {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn forward(shared: &Mutex<Shared>, stop: &AtomicBool, wires: Vec<(PortId, Wire)>) {
    let (ports, mut wires): (Vec<PortId>, Vec<Wire>) = wires.into_iter().unzip();
    let by_port: HashMap<PortId, usize> = ports.iter().enumerate().map(|(i, p)| (*p, i)).collect();
    let mut fds: Vec<libc::pollfd> = wires.iter().map(|w| libc::pollfd { fd: w.fd(), events: libc::POLLIN, revents: 0 }).collect();
    let mut buf = vec![0u8; 65536];
    let mut last_tick = Instant::now() - TICK;
    while !stop.load(Ordering::Relaxed) {
        // SAFETY: `fds` is a valid array of pollfd of the given length.
        let n = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, TICK.as_millis() as i32) };
        if n > 0 {
            for i in 0..fds.len() {
                if fds[i].revents & libc::POLLIN == 0 {
                    continue;
                }
                // Drain what is waiting, but bounded, so one busy port cannot starve the rest.
                for _ in 0..256 {
                    let Ok(len) = wires[i].recv(&mut buf) else { break };
                    let mut s = shared.lock().expect("switch lock");
                    let out = s.core.ingress(ports[i], &buf[..len], Instant::now());
                    for (p, frame) in out {
                        if let Some(&w) = by_port.get(&p) {
                            wires[w].send(&frame, s.peers.get(&p));
                        }
                    }
                }
            }
        }
        let now = Instant::now();
        if now.duration_since(last_tick) >= TICK {
            last_tick = now;
            let mut s = shared.lock().expect("switch lock");
            let out = s.core.tick(now);
            for (p, frame) in out {
                if let Some(&w) = by_port.get(&p) {
                    wires[w].send(&frame, s.peers.get(&p));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::switch::core::{PortDef, Peer};
    use model::network::{PortMode, SwitchPortCfg};
    use model::{HostId, PortName, SwitchId};

    /// Two "VMs" on unix sockets, one access port each on VLAN 1: a frame
    /// sent by one arrives at the other through the switch thread.
    #[test]
    fn frames_cross_the_switch_between_unix_sockets() {
        let dir = std::env::temp_dir().join(format!("labswitch-io-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = SwitchPortCfg { mode: PortMode::Access(1), mtu: 1500, lacp_group: None, admin_up: true };
        let port = |p: &str, h: &str| PortDef {
            switch: SwitchId::from("sw"),
            name: PortName::from(p),
            speed_mbps: 1000,
            cfg: cfg.clone(),
            peer: Peer::Host { host: HostId::from(h), nic: "eth0".into(), nic_mbps: 1000 },
        };
        let mut core = Core::new(&[(SwitchId::from("sw"), "m".into())], vec![port("p1", "a"), port("p2", "b")], Instant::now());
        core.set_peer_up(0, true);
        core.set_peer_up(1, true);
        let vm = |n: &str| {
            let s = UnixDatagram::bind(dir.join(format!("{n}.vm"))).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            s
        };
        let (a, b) = (vm("a"), vm("b"));
        let wires = vec![(0, Wire::unix(&dir.join("a.sw")).unwrap()), (1, Wire::unix(&dir.join("b.sw")).unwrap())];
        let sw = LabSwitch::start(core, wires).unwrap();
        sw.cable(0, Some(dir.join("a.vm")));
        sw.cable(1, Some(dir.join("b.vm")));
        let mut frame = vec![0xff; 6];
        frame.extend_from_slice(&[0x52, 0x54, 0, 0, 0, 0xa, 0x08, 0x00]);
        frame.extend_from_slice(&[7; 46]);
        a.send_to(&frame, dir.join("a.sw")).unwrap();
        let mut buf = [0u8; 2048];
        // b first hears the switch's LLDP; skip to the data frame.
        let got = loop {
            let n = b.recv(&mut buf).expect("a frame within 2 s");
            if buf[12..14] == [0x08, 0x00] {
                break buf[..n].to_vec();
            }
        };
        assert_eq!(got, frame);
        assert!(sw.with(|c| c.counters(1).tx_frames) >= 1);
    }
}
