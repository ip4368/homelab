//! The NoCloud seed each VM boots with: what installing a machine with the
//! cluster's image would put on it. It names the NICs by MAC (as udev rules
//! would on real hardware), brings `eth0` up on DHCP, points chrony at the
//! router, installs the guest agent, and installs `homelabd` with the node
//! config (and, for a machine joining an existing cluster, the join token).

use crate::topology::{VmSpec, BIN_PORT, MGMT_NET, ROUTER_IP, VIP};
use model::SwitchId;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::net::Ipv4Addr;

/// What a VM is installed as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// The first machine: it bootstraps a new cluster.
    Bootstrap,
    /// A machine that joins with this token.
    Join(String),
}

pub struct Seed {
    pub user_data: String,
    pub meta_data: String,
    pub network_config: String,
}

/// The `/etc/homelab/node.json` for `vm`.
pub fn node_json(vm: &VmSpec, token: Option<&str>, switches: &BTreeMap<SwitchId, Ipv4Addr>) -> String {
    let v = serde_json::json!({
        "host": vm.id,
        "token": token,
        "vip": VIP,
        "mgmt": MGMT_NET,
        "switches": switches,
    });
    serde_json::to_string_pretty(&v).expect("node config serializes")
}

fn indent(s: &str, n: usize) -> String {
    s.lines().map(|l| format!("{}{l}\n", " ".repeat(n))).collect()
}

const UNIT: &str = "[Unit]
Description=homelab node daemon
After=network-online.target chrony.service
Wants=network-online.target

[Service]
ExecStartPre=/usr/local/sbin/homelabd-fetch
ExecStart=/usr/local/bin/homelabd run
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
";

/// Fetches the newest `homelabd` from the lab; keeps the installed one if
/// the lab is unreachable (the network may be mid-change).
fn fetch_script() -> String {
    format!(
        "#!/bin/sh\nset -e\nif curl -fsS --max-time 20 -o /usr/local/bin/homelabd.new http://{ROUTER_IP}:{BIN_PORT}/homelabd; then\n  chmod +x /usr/local/bin/homelabd.new\n  mv /usr/local/bin/homelabd.new /usr/local/bin/homelabd\nfi\ntest -x /usr/local/bin/homelabd\n"
    )
}

pub fn seed(vm: &VmSpec, role: &Role, ssh_key: &str, switches: &BTreeMap<SwitchId, Ipv4Addr>) -> Seed {
    let token = match role {
        Role::Join(t) => Some(t.as_str()),
        Role::Bootstrap => None,
    };
    let mut u = String::from("#cloud-config\n");
    writeln!(u, "hostname: {}", vm.id).unwrap();
    writeln!(u, "ssh_authorized_keys:\n  - \"{}\"", ssh_key.trim()).unwrap();
    u.push_str("packages: [qemu-guest-agent, chrony]\n");
    u.push_str("write_files:\n");
    let mut file = |path: &str, perms: &str, body: &str| {
        writeln!(u, "  - path: {path}\n    permissions: '{perms}'\n    content: |").unwrap();
        u.push_str(&indent(body, 6));
    };
    file("/etc/homelab/node.json", "0644", &node_json(vm, token, switches));
    file("/etc/chrony/conf.d/lab.conf", "0644", &format!("server {ROUTER_IP} iburst minpoll -2 maxpoll 0\nmakestep 0.5 -1\n"));
    file("/etc/systemd/system/homelabd.service", "0644", UNIT);
    file("/usr/local/sbin/homelabd-fetch", "0755", &fetch_script());
    u.push_str("runcmd:\n");
    u.push_str("  - [sh, -c, \"sed -i 's/^pool/#pool/' /etc/chrony/chrony.conf; rm -f /etc/chrony/sources.d/*; systemctl restart chrony\"]\n");
    u.push_str("  - [systemctl, enable, --now, qemu-guest-agent]\n");
    if *role == Role::Bootstrap {
        u.push_str("  - [sh, -c, \"/usr/local/sbin/homelabd-fetch && /usr/local/bin/homelabd bootstrap\"]\n");
    }
    u.push_str("  - [systemctl, enable, --now, homelabd]\n");

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
    fn a_joining_ms01_gets_its_token_names_and_dhcp_on_eth0_only() {
        let lab = LabSpec::default_lab();
        let vm = lab.vm(&HostId::from("ms01-02")).unwrap();
        let switches = [(SwitchId::from("core-1"), Ipv4Addr::new(10, 0, 3, 251))].into();
        let s = seed(vm, &Role::Join("abc123".into()), "ssh-ed25519 AAAA lab", &switches);
        assert!(s.user_data.starts_with("#cloud-config\nhostname: ms01-02\n"));
        assert!(s.user_data.contains("\"token\": \"abc123\""), "{}", s.user_data);
        assert!(s.user_data.contains("\"vip\": \"10.0.3.253\""));
        assert!(s.user_data.contains("\"core-1\": \"10.0.3.251\""));
        assert!(!s.user_data.contains("homelabd bootstrap"));
        assert!(s.network_config.contains("  sfp0:\n    match:\n      macaddress: \"52:54:00:1a:02:01\"\n    set-name: sfp0\n    dhcp4: false"));
        assert!(s.network_config.contains("  eth0:\n    match:\n      macaddress: \"52:54:00:1a:02:03\"\n    set-name: eth0\n    dhcp4: true"));
        let boot = seed(lab.vm(&HostId::from("ms01-01")).unwrap(), &Role::Bootstrap, "k", &switches);
        assert!(boot.user_data.contains("homelabd bootstrap"));
        assert!(boot.user_data.contains("\"token\": null"));
    }
}
