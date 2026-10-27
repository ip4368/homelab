//! The NoCloud seed each VM boots with: what installing Debian on a machine
//! would leave on it. A host name, the technician's SSH key, NIC names
//! fixed by MAC (as udev rules would on real hardware), DHCP on `eth0`, the
//! site's NTP server (the router), and the guest agent the lab watches the
//! machine through (its "BMC"). Nothing about homelab: a technician installs
//! that over SSH afterwards ([`crate::install`]).

use crate::topology::{VmSpec, ROUTER_IP};
use std::fmt::Write;

pub struct Seed {
    pub user_data: String,
    pub meta_data: String,
    pub network_config: String,
}

fn indent(s: &str, n: usize) -> String {
    s.lines().map(|l| format!("{}{l}\n", " ".repeat(n))).collect()
}

pub fn seed(vm: &VmSpec, ssh_key: &str) -> Seed {
    let mut u = String::from("#cloud-config\n");
    writeln!(u, "hostname: {}", vm.id).unwrap();
    writeln!(u, "ssh_authorized_keys:\n  - \"{}\"", ssh_key.trim()).unwrap();
    u.push_str("packages: [qemu-guest-agent, chrony]\n");
    u.push_str("write_files:\n");
    let mut file = |path: &str, perms: &str, body: &str| {
        writeln!(u, "  - path: {path}\n    permissions: '{perms}'\n    content: |").unwrap();
        u.push_str(&indent(body, 6));
    };
    file("/etc/chrony/conf.d/lab.conf", "0644", &format!("server {ROUTER_IP} iburst minpoll -2 maxpoll 0\nmakestep 0.5 -1\n"));
    u.push_str("runcmd:\n");
    u.push_str("  - [sh, -c, \"sed -i 's/^pool/#pool/' /etc/chrony/chrony.conf; rm -f /etc/chrony/sources.d/*; systemctl restart chrony\"]\n");
    u.push_str("  - [systemctl, enable, --now, qemu-guest-agent]\n");

    let mut n = String::from("version: 2\nethernets:\n");
    for nic in &vm.nics {
        writeln!(n, "  {}:\n    match:\n      macaddress: \"{}\"\n    set-name: {}", nic.name, VmSpec::mac_str(&nic.mac), nic.name).unwrap();
        if nic.name == "eth0" {
            n.push_str("    dhcp4: true\n    dhcp-identifier: mac\n");
        } else {
            n.push_str("    dhcp4: false\n    optional: true\n");
        }
    }
    let meta_data = format!("instance-id: {}-{}\nlocal-hostname: {}\n", vm.id, vm.index, vm.id);
    Seed { user_data: u, meta_data, network_config: n }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::LabSpec;
    use model::HostId;

    #[test]
    fn a_machine_gets_plain_debian_with_its_nics_named_and_dhcp_on_eth0_only() {
        let lab = LabSpec::default_lab();
        let vm = lab.vm(&HostId::from("ms01-02")).unwrap();
        let s = seed(vm, "ssh-ed25519 AAAA lab");
        assert!(s.user_data.starts_with("#cloud-config\nhostname: ms01-02\n"));
        assert!(!s.user_data.contains("homelab"), "homelab is installed over SSH: {}", s.user_data);
        assert!(!s.user_data.contains("proxy"), "the lab's uplink needs no proxy settings");
        assert!(s.network_config.contains("  sfp0:\n    match:\n      macaddress: \"52:54:00:1a:02:01\"\n    set-name: sfp0\n    dhcp4: false"));
        assert!(s.network_config.contains("  eth0:\n    match:\n      macaddress: \"52:54:00:1a:02:03\"\n    set-name: eth0\n    dhcp4: true"));
    }
}
