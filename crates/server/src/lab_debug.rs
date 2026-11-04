//! Debugging a lab VM from its host drawer: its serial console log, its
//! journal over the technician's SSH, and an interactive SSH login. These
//! exist only with a lab attached (docs/plans/2026-11-04-workload-debug-design.md §6);
//! a real host has no shell channel in the homelabd protocol.

use crate::kube_action::valid_namespace;
use std::net::Ipv4Addr;
use std::path::Path;

/// The units whose journal the page may read.
pub const JOURNAL_UNITS: [&str; 3] = ["homelabd", "k3s", "k3s-agent"];
pub const DEFAULT_LINES: u32 = 200;
pub const MAX_LINES: u32 = 1000;
/// The most console log one answer carries.
pub const MAX_CONSOLE_BYTES: usize = 256 << 10;

/// A VM name is a DNS label (the same rule as a namespace).
pub fn valid_vm(vm: &str) -> bool {
    valid_namespace(vm)
}

/// The last `lines` of `unit`'s journal, or why the unit may not be read.
/// Run over SSH as the technician, whose user may sudo.
pub fn journal_command(unit: &str, lines: Option<u32>) -> Result<String, String> {
    if !JOURNAL_UNITS.contains(&unit) {
        return Err(format!("{unit:?} is not a unit this page reads ({})", JOURNAL_UNITS.join(", ")));
    }
    let lines = lines.unwrap_or(DEFAULT_LINES).clamp(1, MAX_LINES);
    Ok(format!("sudo journalctl -u {unit} --no-pager -n {lines} 2>&1"))
}

/// `ssh`'s arguments for an interactive login to `ip` with `key`, as the
/// technician (the image's default user): a forced TTY, no host-key
/// bookkeeping, a dead connection noticed in 30 s.
pub fn ssh_args(key: &Path, ip: Ipv4Addr) -> Vec<String> {
    vec![
        "-tt".into(),
        "-q".into(),
        "-o".into(),
        "StrictHostKeyChecking=no".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "ConnectTimeout=10".into(),
        "-o".into(),
        "ServerAliveInterval=5".into(),
        "-o".into(),
        "ServerAliveCountMax=6".into(),
        "-i".into(),
        key.to_string_lossy().into_owned(),
        format!("debian@{ip}"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_named_units_are_read_and_lines_are_capped() {
        assert_eq!(journal_command("homelabd", None).unwrap(), "sudo journalctl -u homelabd --no-pager -n 200 2>&1");
        assert!(journal_command("k3s", Some(99999)).unwrap().contains("-n 1000"));
        assert!(journal_command("k3s-agent", Some(0)).unwrap().contains("-n 1"));
        for bad in ["ssh", "homelabd; rm -rf /", "", "../x"] {
            assert!(journal_command(bad, None).is_err(), "{bad}");
        }
    }

    #[test]
    fn vm_names_are_dns_labels() {
        assert!(valid_vm("ms01-01"));
        for bad in ["", "../etc", "a/b", "A", "a b", "-x"] {
            assert!(!valid_vm(bad), "{bad}");
        }
    }

    #[test]
    fn ssh_args_force_a_tty_and_log_in_as_the_technician() {
        let args = ssh_args(Path::new("/state/id_ed25519"), Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(args[0], "-tt");
        assert_eq!(args.last().map(String::as_str), Some("debian@10.0.0.2"));
        assert!(args.windows(2).any(|w| w == ["-i", "/state/id_ed25519"]));
        assert!(args.iter().all(|a| !a.contains("sudo")), "a login, not a command");
    }
}
