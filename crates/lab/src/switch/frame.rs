//! Ethernet frames as the switch sees them: addresses, the optional 802.1Q
//! tag, and the protocols it answers itself (LACP, LLDP).

pub const ETH_P_8021Q: u16 = 0x8100;
pub const ETH_P_SLOW: u16 = 0x8809;
pub const ETH_P_LLDP: u16 = 0x88cc;
pub const LLDP_DST: [u8; 6] = [0x01, 0x80, 0xc2, 0x00, 0x00, 0x0e];
pub const SLOW_DST: [u8; 6] = [0x01, 0x80, 0xc2, 0x00, 0x00, 0x02];

pub type Mac = [u8; 6];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub dst: Mac,
    pub src: Mac,
    /// The 802.1Q VLAN id, if the frame is tagged.
    pub vlan: Option<u16>,
    pub ethertype: u16,
    /// Bytes after the Ethernet header (and tag): what the MTU limits.
    pub payload_len: usize,
}

pub fn parse(f: &[u8]) -> Option<Header> {
    if f.len() < 14 {
        return None;
    }
    let mac = |i: usize| -> Mac { f[i..i + 6].try_into().expect("6 bytes") };
    let t = u16::from_be_bytes([f[12], f[13]]);
    if t == ETH_P_8021Q {
        if f.len() < 18 {
            return None;
        }
        let vlan = u16::from_be_bytes([f[14], f[15]]) & 0x0fff;
        let ethertype = u16::from_be_bytes([f[16], f[17]]);
        Some(Header { dst: mac(0), src: mac(6), vlan: Some(vlan), ethertype, payload_len: f.len() - 18 })
    } else {
        Some(Header { dst: mac(0), src: mac(6), vlan: None, ethertype: t, payload_len: f.len() - 14 })
    }
}

pub fn is_multicast(mac: &Mac) -> bool {
    mac[0] & 1 == 1
}

/// `f` (tagged or not) as it leaves a port: tagged with `vlan`, or untagged.
pub fn retag(f: &[u8], h: &Header, vlan: Option<u16>) -> Vec<u8> {
    let body = if h.vlan.is_some() { &f[16..] } else { &f[12..] };
    let mut out = Vec::with_capacity(f.len() + 4);
    out.extend_from_slice(&f[..12]);
    if let Some(v) = vlan {
        out.extend_from_slice(&ETH_P_8021Q.to_be_bytes());
        out.extend_from_slice(&(v & 0x0fff).to_be_bytes());
    }
    out.extend_from_slice(body);
    out
}

/// A flow hash over addresses, IPv4 addresses and TCP/UDP ports, for picking
/// a LAG member: one flow always takes the same member.
pub fn flow_hash(f: &[u8], h: &Header) -> u64 {
    let mut x: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |b: &[u8]| {
        for &c in b {
            x ^= u64::from(c);
            x = x.wrapping_mul(0x0100_0000_01b3);
        }
    };
    eat(&h.dst);
    eat(&h.src);
    let ip = if h.vlan.is_some() { 18 } else { 14 };
    if h.ethertype == 0x0800 && f.len() >= ip + 20 {
        eat(&f[ip + 12..ip + 20]);
        let ihl = usize::from(f[ip] & 0x0f) * 4;
        if matches!(f[ip + 9], 6 | 17) && f.len() >= ip + ihl + 4 {
            eat(&f[ip + ihl..ip + ihl + 4]);
        }
    }
    x
}

fn tlv(out: &mut Vec<u8>, kind: u8, value: &[u8]) {
    let head = (u16::from(kind) << 9) | u16::try_from(value.len()).expect("short TLV");
    out.extend_from_slice(&head.to_be_bytes());
    out.extend_from_slice(value);
}

/// An LLDP frame from switch port `port` of `switch`: chassis id and system
/// name are the switch id, port id is the port name.
pub fn lldp(src: Mac, switch: &str, port: &str) -> Vec<u8> {
    let mut f = Vec::with_capacity(64);
    f.extend_from_slice(&LLDP_DST);
    f.extend_from_slice(&src);
    f.extend_from_slice(&ETH_P_LLDP.to_be_bytes());
    let mut chassis = vec![7];
    chassis.extend_from_slice(switch.as_bytes());
    tlv(&mut f, 1, &chassis);
    let mut pid = vec![5];
    pid.extend_from_slice(port.as_bytes());
    tlv(&mut f, 2, &pid);
    tlv(&mut f, 3, &120u16.to_be_bytes());
    tlv(&mut f, 5, switch.as_bytes());
    tlv(&mut f, 0, &[]);
    f
}

/// One side's identity and state in a LACPDU.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LacpInfo {
    pub system_priority: u16,
    pub system: Mac,
    pub key: u16,
    pub port_priority: u16,
    pub port: u16,
    pub state: u8,
}

pub const LACP_ACTIVITY: u8 = 0x01;
pub const LACP_TIMEOUT: u8 = 0x02;
pub const LACP_AGGREGATION: u8 = 0x04;
pub const LACP_SYNC: u8 = 0x08;
pub const LACP_COLLECTING: u8 = 0x10;
pub const LACP_DISTRIBUTING: u8 = 0x20;
pub const LACP_DEFAULTED: u8 = 0x40;

fn put_info(out: &mut Vec<u8>, kind: u8, i: &LacpInfo) {
    out.extend_from_slice(&[kind, 20]);
    out.extend_from_slice(&i.system_priority.to_be_bytes());
    out.extend_from_slice(&i.system);
    out.extend_from_slice(&i.key.to_be_bytes());
    out.extend_from_slice(&i.port_priority.to_be_bytes());
    out.extend_from_slice(&i.port.to_be_bytes());
    out.extend_from_slice(&[i.state, 0, 0, 0]);
}

fn get_info(b: &[u8]) -> LacpInfo {
    LacpInfo {
        system_priority: u16::from_be_bytes([b[0], b[1]]),
        system: b[2..8].try_into().expect("6 bytes"),
        key: u16::from_be_bytes([b[8], b[9]]),
        port_priority: u16::from_be_bytes([b[10], b[11]]),
        port: u16::from_be_bytes([b[12], b[13]]),
        state: b[14],
    }
}

pub fn lacpdu(src: Mac, actor: &LacpInfo, partner: &LacpInfo) -> Vec<u8> {
    let mut f = Vec::with_capacity(124);
    f.extend_from_slice(&SLOW_DST);
    f.extend_from_slice(&src);
    f.extend_from_slice(&ETH_P_SLOW.to_be_bytes());
    f.extend_from_slice(&[1, 1]); // LACP, version 1
    put_info(&mut f, 1, actor);
    put_info(&mut f, 2, partner);
    f.extend_from_slice(&[3, 16, 0, 0]); // collector: max delay 0
    f.extend_from_slice(&[0; 12]);
    f.extend_from_slice(&[0, 0]); // terminator
    f.extend_from_slice(&[0; 50]);
    f
}

/// The (actor, partner) of a LACPDU, if `f` is one.
pub fn parse_lacpdu(f: &[u8], h: &Header) -> Option<(LacpInfo, LacpInfo)> {
    let p = if h.vlan.is_some() { 18 } else { 14 };
    let b = f.get(p..p + 2 + 2 * 20)?;
    if h.ethertype != ETH_P_SLOW || b[0] != 1 || b[2] != 1 || b[22] != 2 {
        return None;
    }
    Some((get_info(&b[4..]), get_info(&b[24..])))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn frame(dst: Mac, src: Mac, vlan: Option<u16>, payload: usize) -> Vec<u8> {
        let mut f = vec![];
        f.extend_from_slice(&dst);
        f.extend_from_slice(&src);
        if let Some(v) = vlan {
            f.extend_from_slice(&ETH_P_8021Q.to_be_bytes());
            f.extend_from_slice(&v.to_be_bytes());
        }
        f.extend_from_slice(&0x0800u16.to_be_bytes());
        f.extend(std::iter::repeat_n(0xab, payload));
        f
    }

    #[test]
    fn tags_come_and_go() {
        let f = frame([2; 6], [4; 6], None, 100);
        let h = parse(&f).unwrap();
        assert_eq!((h.vlan, h.ethertype, h.payload_len), (None, 0x0800, 100));
        let t = retag(&f, &h, Some(20));
        let th = parse(&t).unwrap();
        assert_eq!((th.vlan, th.ethertype, th.payload_len, th.dst, th.src), (Some(20), 0x0800, 100, [2; 6], [4; 6]));
        assert_eq!(retag(&t, &th, None), f);
        assert_eq!(parse(&retag(&t, &th, Some(30))).unwrap().vlan, Some(30));
    }

    #[test]
    fn lacpdus_round_trip() {
        let a = LacpInfo { system_priority: 1, system: [2, 0, 0, 0, 0, 1], key: 9, port_priority: 255, port: 3, state: LACP_ACTIVITY | LACP_SYNC };
        let p = LacpInfo { port: 7, ..LacpInfo::default() };
        let f = lacpdu([2, 0, 0, 0, 0, 1], &a, &p);
        assert_eq!(f.len(), 124);
        assert_eq!(parse_lacpdu(&f, &parse(&f).unwrap()), Some((a, p)));
    }

    #[test]
    fn lldp_frames_carry_switch_and_port() {
        let f = lldp([2; 6], "core-1", "p3");
        let h = parse(&f).unwrap();
        assert_eq!((h.dst, h.ethertype), (LLDP_DST, ETH_P_LLDP));
        assert_eq!(&f[14..16], &[0x02, 7], "chassis TLV, 7 bytes");
        assert_eq!(&f[16..23], b"\x07core-1");
        assert_eq!(&f[23..25], &[0x04, 3], "port TLV, 3 bytes");
        assert_eq!(&f[25..28], b"\x05p3");
    }
}
