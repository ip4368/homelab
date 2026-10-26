//! Address allocation inside a logical network's subnet.

use crate::network::LogicalNetwork;
use std::collections::BTreeSet;
use std::net::Ipv4Addr;

/// The lowest address of `net.subnet` not in `used`. The network address,
/// the gateway (the first host address, `.1`), the broadcast address and the
/// network's reserved ranges are never handed out. `None` when the subnet is full.
pub fn allocate(net: &LogicalNetwork, used: &BTreeSet<Ipv4Addr>) -> Option<Ipv4Addr> {
    let gateway = Ipv4Addr::from(u32::from(net.subnet.network()).saturating_add(1));
    net.subnet
        .hosts()
        .find(|ip| *ip != gateway && !used.contains(ip) && !net.reserved.iter().any(|r| r.contains(*ip)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::seed_networks;

    fn net(id: &str) -> LogicalNetwork {
        seed_networks().into_iter().find(|n| n.id.as_str() == id).unwrap()
    }

    fn ip(s: &str) -> Ipv4Addr {
        s.parse().unwrap()
    }

    #[test]
    fn ipam_skips_reserved_and_used() {
        let storage = net("storage");
        assert_eq!(allocate(&storage, &BTreeSet::new()), Some(ip("10.20.0.2")), "network address and gateway are reserved");
        assert_eq!(allocate(&storage, &[ip("10.20.0.2")].into()), Some(ip("10.20.0.3")));
        // lowest free address, even when higher ones are taken
        assert_eq!(allocate(&storage, &[ip("10.20.0.2"), ip("10.20.0.4")].into()), Some(ip("10.20.0.3")));
    }

    #[test]
    fn ipam_runs_out_before_broadcast() {
        let iot = net("vm-iot"); // 10.110.0.0/24: .2 through .254 are usable
        let used: BTreeSet<Ipv4Addr> = (2..=254).map(|i| Ipv4Addr::new(10, 110, 0, i)).collect();
        assert_eq!(used.len(), 253);
        assert_eq!(allocate(&iot, &used), None);
        let mut used = used;
        used.remove(&ip("10.110.0.77"));
        assert_eq!(allocate(&iot, &used), Some(ip("10.110.0.77")));
    }

    #[test]
    fn ipam_crosses_octet_boundaries() {
        let storage = net("storage"); // 10.20.0.0/22 reaches 10.20.3.254
        let used: BTreeSet<Ipv4Addr> = (2..=255).map(|i| Ipv4Addr::new(10, 20, 0, i)).collect();
        assert_eq!(allocate(&storage, &used), Some(ip("10.20.1.0")));
    }

    #[test]
    fn ipam_skips_reserved_ranges() {
        let mut mgmt = net("mgmt");
        assert_eq!(mgmt.reserved, vec![crate::network::MGMT_RESERVED]);
        let first_reserved = ip("10.0.3.100");
        let used: BTreeSet<Ipv4Addr> = mgmt.subnet.hosts().filter(|a| *a < first_reserved).collect();
        assert_eq!(allocate(&mgmt, &used), None, "everything below the reserved block is used, the rest is reserved");
        mgmt.reserved.clear();
        assert_eq!(allocate(&mgmt, &used), Some(first_reserved));
    }
}
