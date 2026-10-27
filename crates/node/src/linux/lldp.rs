//! Hearing the switch's LLDP: which switch port is on the far end of each
//! NIC's cable. A thread listens on every interface; a kernel filter passes
//! it LLDP frames only. It listens to all traffic rather than to LLDP's
//! ethertype because the kernel hands a bond member's frames to the bond,
//! and only a tap on all traffic still sees the member they arrived on.
//! Each NIC also joins LLDP's multicast group: without it the NIC's own
//! filter drops the frames before the kernel sees them.

use super::packet::{ifindex, ETH_P_LLDP};
use model::network::LldpNeighbor;
use model::{PortName, SwitchId};
use std::collections::BTreeMap;
use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The nearest-bridge address LLDP is sent to.
const LLDP_MULTICAST: [u8; 6] = [0x01, 0x80, 0xc2, 0x00, 0x00, 0x0e];

/// What one NIC last heard, and until when it holds (the frame's TTL).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Heard {
    switch: SwitchId,
    port: PortName,
    mgmt: Option<Ipv4Addr>,
    until: Instant,
}

/// The switch port each NIC hears, kept fresh by a listening thread.
#[derive(Debug, Clone, Default)]
pub struct Lldp {
    heard: Arc<Mutex<BTreeMap<String, Heard>>>,
}

/// What an LLDP frame says: chassis id, port id, TTL, and the IPv4
/// management address if it has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub chassis: String,
    pub port: String,
    pub ttl: u16,
    pub mgmt: Option<Ipv4Addr>,
}

/// Parses an LLDP frame (a whole Ethernet frame).
pub fn parse(frame: &[u8]) -> Option<Frame> {
    if frame.len() < 14 || u16::from_be_bytes([frame[12], frame[13]]) != ETH_P_LLDP {
        return None;
    }
    let (mut chassis, mut port, mut ttl, mut mgmt) = (None, None, None, None);
    let mut i = 14;
    while i + 2 <= frame.len() {
        let head = u16::from_be_bytes([frame[i], frame[i + 1]]);
        let (kind, len) = (head >> 9, usize::from(head & 0x1ff));
        let value = frame.get(i + 2..i + 2 + len)?;
        match kind {
            0 => break,
            1 if len > 1 => chassis = Some(String::from_utf8_lossy(&value[1..]).into_owned()),
            2 if len > 1 => port = Some(String::from_utf8_lossy(&value[1..]).into_owned()),
            3 if len == 2 => ttl = Some(u16::from_be_bytes([value[0], value[1]])),
            // Address string length (subtype + address), subtype 1 = IPv4.
            8 if len >= 6 && value[0] == 5 && value[1] == 1 && mgmt.is_none() => {
                mgmt = Some(Ipv4Addr::new(value[2], value[3], value[4], value[5]));
            }
            _ => {}
        }
        i += 2 + len;
    }
    Some(Frame { chassis: chassis?, port: port?, ttl: ttl.unwrap_or(120), mgmt })
}

impl Lldp {
    /// Starts listening on every interface of the machine, receiving LLDP's
    /// multicast on each of `nics`.
    pub fn start(nics: &[String]) -> io::Result<Self> {
        let sock = open()?;
        for nic in nics {
            if let Err(e) = join_multicast(&sock, nic) {
                eprintln!("homelabd: LLDP on {nic}: {e}");
            }
        }
        let lldp = Self::default();
        let heard = lldp.heard.clone();
        std::thread::Builder::new().name("lldp".into()).spawn(move || listen(&sock, &heard))?;
        Ok(lldp)
    }

    /// For each of `nics` (in that order) whose link is up per `carrier`, the
    /// switch port it last heard, if that has not expired.
    pub fn neighbours(&self, nics: &[String], carrier: impl Fn(&str) -> bool) -> Vec<LldpNeighbor> {
        let heard = self.heard.lock().expect("lldp lock");
        let now = Instant::now();
        nics.iter()
            .filter(|n| carrier(n))
            .filter_map(|n| {
                let h = heard.get(n).filter(|h| h.until > now)?;
                Some(LldpNeighbor { local_port: PortName::new(n), switch: h.switch.clone(), switch_port: h.port.clone(), mgmt_ip: h.mgmt })
            })
            .collect()
    }
}

/// A raw socket on all interfaces seeing every frame, filtered in the
/// kernel to LLDP (ethertype at offset 12).
fn open() -> io::Result<OwnedFd> {
    // SAFETY: plain socket(2); the result is checked.
    let fd = unsafe { libc::socket(libc::AF_PACKET, libc::SOCK_RAW | libc::SOCK_CLOEXEC, i32::from((libc::ETH_P_ALL as u16).to_be())) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a descriptor we just opened and now own.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut code = [
        libc::sock_filter { code: 0x28, jt: 0, jf: 0, k: 12 },                      // ldh [12]
        libc::sock_filter { code: 0x15, jt: 0, jf: 1, k: u32::from(ETH_P_LLDP) }, // jeq #0x88cc
        libc::sock_filter { code: 0x06, jt: 0, jf: 0, k: 0xffff },                  // ret #65535
        libc::sock_filter { code: 0x06, jt: 0, jf: 0, k: 0 },                       // ret #0
    ];
    let prog = libc::sock_fprog { len: code.len() as u16, filter: code.as_mut_ptr() };
    // SAFETY: attaching a valid classic BPF program of the stated size.
    let r = unsafe { libc::setsockopt(fd.as_raw_fd(), libc::SOL_SOCKET, libc::SO_ATTACH_FILTER, (&raw const prog).cast(), size_of::<libc::sock_fprog>() as u32) };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    let tv = libc::timeval { tv_sec: 1, tv_usec: 0 };
    // SAFETY: a valid timeval of the stated size.
    unsafe { libc::setsockopt(fd.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVTIMEO, (&raw const tv).cast(), size_of::<libc::timeval>() as u32) };
    Ok(fd)
}

/// Has `nic` pass LLDP's multicast up while `sock` is open.
fn join_multicast(sock: &OwnedFd, nic: &str) -> io::Result<()> {
    let mut mr_address = [0u8; 8];
    mr_address[..6].copy_from_slice(&LLDP_MULTICAST);
    let mreq = libc::packet_mreq { mr_ifindex: ifindex(nic)?, mr_type: libc::PACKET_MR_MULTICAST as u16, mr_alen: 6, mr_address };
    // SAFETY: a valid packet_mreq of the stated size.
    let r = unsafe { libc::setsockopt(sock.as_raw_fd(), libc::SOL_PACKET, libc::PACKET_ADD_MEMBERSHIP, (&raw const mreq).cast(), size_of::<libc::packet_mreq>() as u32) };
    if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

fn ifname(index: i32) -> Option<String> {
    let mut buf = [0 as libc::c_char; libc::IF_NAMESIZE];
    // SAFETY: `buf` is IF_NAMESIZE bytes, as if_indextoname requires.
    let p = unsafe { libc::if_indextoname(index as u32, buf.as_mut_ptr()) };
    if p.is_null() {
        return None;
    }
    // SAFETY: if_indextoname wrote a NUL-terminated name into `buf`.
    Some(unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }.to_string_lossy().into_owned())
}

fn listen(sock: &OwnedFd, heard: &Mutex<BTreeMap<String, Heard>>) {
    let mut buf = [0u8; 2048];
    loop {
        // SAFETY: zeroed sockaddr_ll is a valid out-parameter.
        let mut from: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
        let mut len = size_of::<libc::sockaddr_ll>() as u32;
        // SAFETY: `buf` and `from` are valid for writes of the given sizes.
        let n = unsafe { libc::recvfrom(sock.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0, (&raw mut from).cast(), &mut len) };
        if n <= 0 {
            continue;
        }
        // Frames this machine sends (PACKET_OUTGOING) are not the switch talking.
        if from.sll_pkttype == libc::PACKET_OUTGOING as u8 {
            continue;
        }
        let (Some(f), Some(nic)) = (parse(&buf[..n as usize]), ifname(from.sll_ifindex)) else { continue };
        let until = Instant::now() + Duration::from_secs(u64::from(f.ttl));
        heard.lock().expect("lldp lock").insert(nic, Heard { switch: SwitchId::new(f.chassis), port: PortName::new(f.port), mgmt: f.mgmt, until });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An LLDP frame as `labswitch` sends it (see `lab::switch::frame::lldp`).
    fn frame(switch: &str, port: &str) -> Vec<u8> {
        frame_with(switch, port, None)
    }

    fn frame_with(switch: &str, port: &str, mgmt: Option<[u8; 4]>) -> Vec<u8> {
        let tlv = |out: &mut Vec<u8>, kind: u16, v: &[u8]| {
            out.extend_from_slice(&((kind << 9) | v.len() as u16).to_be_bytes());
            out.extend_from_slice(v);
        };
        let mut f = vec![0x01, 0x80, 0xc2, 0, 0, 0x0e, 2, 0, 0, 0, 0, 1, 0x88, 0xcc];
        tlv(&mut f, 1, &[&[7u8][..], switch.as_bytes()].concat());
        tlv(&mut f, 2, &[&[5u8][..], port.as_bytes()].concat());
        tlv(&mut f, 3, &30u16.to_be_bytes());
        if let Some(ip) = mgmt {
            tlv(&mut f, 8, &[&[5u8, 1][..], &ip[..], &[1, 0, 0, 0, 0, 0][..]].concat());
        }
        tlv(&mut f, 0, &[]);
        f
    }

    #[test]
    fn a_switch_frame_names_its_switch_port_and_ttl() {
        let f = |chassis: &str, port: &str, mgmt| Frame { chassis: chassis.into(), port: port.into(), ttl: 30, mgmt };
        assert_eq!(parse(&frame("core-1", "p3")), Some(f("core-1", "p3", None)));
        let with_mgmt = frame_with("core-1", "p3", Some([10, 0, 3, 251]));
        assert_eq!(parse(&with_mgmt), Some(f("core-1", "p3", Some(Ipv4Addr::new(10, 0, 3, 251)))), "the management address TLV");
        let mut not_lldp = frame("core-1", "p3");
        not_lldp[13] = 0x00;
        assert_eq!(parse(&not_lldp), None);
        assert_eq!(parse(&frame("core-1", "p3")[..20]), None, "a truncated frame");
    }

    #[test]
    fn only_live_unexpired_nics_are_reported_in_nic_order() {
        let l = Lldp::default();
        let far = Instant::now() + Duration::from_secs(60);
        let mut h = l.heard.lock().unwrap();
        h.insert("sfp0".into(), Heard { switch: SwitchId::from("core-1"), port: PortName::from("p1"), mgmt: None, until: far });
        h.insert("eth0".into(), Heard { switch: SwitchId::from("access-1"), port: PortName::from("p1"), mgmt: None, until: far });
        h.insert("sfp1".into(), Heard { switch: SwitchId::from("core-1"), port: PortName::from("p2"), mgmt: None, until: far });
        h.insert("eth1".into(), Heard { switch: SwitchId::from("access-1"), port: PortName::from("p9"), mgmt: None, until: Instant::now() });
        drop(h);
        let nics: Vec<String> = ["sfp0", "sfp1", "eth0", "eth1"].map(String::from).into();
        let got: Vec<_> = l.neighbours(&nics, |n| n != "sfp1").iter().map(|n| format!("{}>{}/{}", n.local_port, n.switch, n.switch_port)).collect();
        assert_eq!(got, ["sfp0>core-1/p1", "eth0>access-1/p1"], "sfp1 has no carrier; eth1 expired");
    }
}
