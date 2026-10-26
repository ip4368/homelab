//! Raw Ethernet frames on one interface (`AF_PACKET`), for what the kernel
//! does not do for us: gratuitous ARP when taking the VIP, and listening for
//! LLDP from the switch.

use std::ffi::CString;
use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

pub const ETH_P_ARP: u16 = 0x0806;
pub const ETH_P_LLDP: u16 = 0x88cc;

pub fn ifindex(iface: &str) -> io::Result<i32> {
    let name = CString::new(iface).map_err(io::Error::other)?;
    // SAFETY: `name` is a valid NUL-terminated string for the call's duration.
    match unsafe { libc::if_nametoindex(name.as_ptr()) } {
        0 => Err(io::Error::last_os_error()),
        i => Ok(i as i32),
    }
}

pub fn mac_of(iface: &str) -> io::Result<[u8; 6]> {
    let s = std::fs::read_to_string(format!("/sys/class/net/{iface}/address"))?;
    parse_mac(s.trim()).ok_or_else(|| io::Error::other(format!("bad MAC {s:?}")))
}

pub fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let v: Vec<u8> = s.split(':').map(|b| u8::from_str_radix(b, 16)).collect::<Result<_, _>>().ok()?;
    v.try_into().ok()
}

/// A raw socket bound to one interface and ethertype.
pub struct RawSocket {
    fd: OwnedFd,
    ifindex: i32,
    proto: u16,
}

impl RawSocket {
    /// Opens a socket on `iface` for frames of ethertype `proto`, non-blocking.
    pub fn open(iface: &str, proto: u16) -> io::Result<Self> {
        let ifindex = ifindex(iface)?;
        // SAFETY: plain socket(2) call; the result is checked before use.
        let fd = unsafe { libc::socket(libc::AF_PACKET, libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC, i32::from(proto.to_be())) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a freshly opened descriptor we own.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let sll = sockaddr(ifindex, proto, [0; 6]);
        // SAFETY: `sll` is a valid sockaddr_ll and the length matches it.
        let r = unsafe { libc::bind(fd.as_raw_fd(), (&raw const sll).cast(), size_of::<libc::sockaddr_ll>() as u32) };
        if r < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd, ifindex, proto })
    }

    /// Sends one whole Ethernet frame (destination MAC first).
    pub fn send(&self, frame: &[u8]) -> io::Result<()> {
        let mut dst = [0; 6];
        dst.copy_from_slice(&frame[..6]);
        let sll = sockaddr(self.ifindex, self.proto, dst);
        // SAFETY: `frame` and `sll` are valid for the call; lengths match.
        let r = unsafe {
            libc::sendto(self.fd.as_raw_fd(), frame.as_ptr().cast(), frame.len(), 0, (&raw const sll).cast(), size_of::<libc::sockaddr_ll>() as u32)
        };
        if r < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
    }

    /// The next frame waiting, if any (non-blocking).
    pub fn try_recv(&self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        // SAFETY: `buf` is valid for writes of its length.
        let r = unsafe { libc::recv(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len(), 0) };
        if r >= 0 {
            return Ok(Some(r as usize));
        }
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::WouldBlock { Ok(None) } else { Err(e) }
    }
}

fn sockaddr(ifindex: i32, proto: u16, mac: [u8; 6]) -> libc::sockaddr_ll {
    let mut addr = [0u8; 8];
    addr[..6].copy_from_slice(&mac);
    libc::sockaddr_ll {
        sll_family: libc::AF_PACKET as u16,
        sll_protocol: proto.to_be(),
        sll_ifindex: ifindex,
        sll_hatype: 0,
        sll_pkttype: 0,
        sll_halen: 6,
        sll_addr: addr,
    }
}

/// A gratuitous ARP announcing that `ip` is at `mac`: a broadcast request
/// for `ip` from `ip`, which updates every neighbour's ARP cache and every
/// switch's MAC table.
pub fn gratuitous_arp(mac: [u8; 6], ip: Ipv4Addr) -> Vec<u8> {
    let mut f = Vec::with_capacity(42);
    f.extend_from_slice(&[0xff; 6]);
    f.extend_from_slice(&mac);
    f.extend_from_slice(&ETH_P_ARP.to_be_bytes());
    f.extend_from_slice(&[0, 1, 8, 0, 6, 4, 0, 1]); // Ethernet, IPv4, request
    f.extend_from_slice(&mac);
    f.extend_from_slice(&ip.octets());
    f.extend_from_slice(&[0; 6]);
    f.extend_from_slice(&ip.octets());
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gratuitous_arp_names_the_ip_as_sender_and_target() {
        let mac = parse_mac("52:54:00:00:01:0a").unwrap();
        let f = gratuitous_arp(mac, Ipv4Addr::new(10, 0, 3, 253));
        assert_eq!(f.len(), 42);
        assert_eq!(&f[..6], &[0xff; 6]);
        assert_eq!(&f[6..12], &mac);
        assert_eq!(&f[12..14], &[0x08, 0x06]);
        assert_eq!(&f[20..22], &[0, 1], "a request");
        assert_eq!(&f[28..32], &[10, 0, 3, 253]);
        assert_eq!(&f[38..42], &[10, 0, 3, 253]);
        assert_eq!(parse_mac("zz:00"), None);
    }
}
