//! The technician: installs homelab on a racked machine the way the README
//! tells a person to. It logs in over SSH on the address the machine got
//! from DHCP, waits for the OS's first boot to finish, and runs the
//! documented one-liner: `init` on the first machine, `join` with a token on
//! every other.

use crate::topology::{BIN_PORT, ROUTER_IP, VIP};
use std::io;
use std::net::Ipv4Addr;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// How a machine is installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// The first machine: it starts a new cluster.
    Bootstrap,
    /// A machine that joins with this token.
    Join(String),
}

impl Role {
    /// The command the README gives for this role.
    pub fn command(&self) -> String {
        let url = format!("http://{ROUTER_IP}:{BIN_PORT}");
        match self {
            Role::Bootstrap => format!("curl -fsSL {url}/install.sh | sudo sh -s -- init --vip {VIP}"),
            Role::Join(token) => format!("curl -fsSL {url}/install.sh | sudo sh -s -- join {VIP} {token}"),
        }
    }
}

/// The join token in `homelabd init`'s output: the last word of the join
/// command it prints.
pub fn token_in(out: &str) -> Option<String> {
    out.lines().find(|l| l.contains("install.sh") && l.contains(" join "))?.split_whitespace().last().map(String::from)
}

/// `ssh` as the technician: key login as the image's default user, no host
/// key bookkeeping (machines are reinstalled all the time), and a dead
/// connection noticed in 30 s (the machine moves to its static address
/// while homelabd sets it up).
fn ssh_cmd(key: &Path, ip: Ipv4Addr) -> Command {
    let mut c = Command::new("ssh");
    c.args(["-q", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]);
    c.args(["-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=6"]);
    c.arg("-i").arg(key).arg(format!("debian@{ip}"));
    c
}

/// Runs `script` on the machine at `ip`, returning its output; fails with
/// its output if it fails. Retries the login (not the script) until
/// `login_within` has passed, for a machine still booting.
pub fn ssh(key: &Path, ip: Ipv4Addr, script: &str, login_within: Duration) -> io::Result<String> {
    let deadline = Instant::now() + login_within;
    loop {
        let out = ssh_cmd(key, ip).arg(script).output()?;
        let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        match out.status.code() {
            Some(0) => return Ok(text),
            // ssh's own failure (no route, refused, not up yet): try again.
            Some(255) if Instant::now() < deadline => std::thread::sleep(Duration::from_secs(3)),
            code => return Err(io::Error::other(format!("ssh {ip} exited {code:?}: {}", text.trim()))),
        }
    }
}

/// Runs `script` on the machine at `ip` with `input` on its stdin; its
/// output, or an error carrying it.
pub fn ssh_with_input(key: &Path, ip: Ipv4Addr, script: &str, input: &str) -> io::Result<String> {
    use std::io::Write;
    let mut child = ssh_cmd(key, ip).arg(script).stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).spawn()?;
    child.stdin.take().expect("piped").write_all(input.as_bytes())?;
    let out = child.wait_with_output()?;
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    if out.status.success() { Ok(text) } else { Err(io::Error::other(format!("ssh {ip} exited {:?}: {}", out.status.code(), text.trim()))) }
}

/// Installs homelab as `role` on the machine at `ip`: waits for its first
/// boot (cloud-init) to finish, then runs the install command. The output
/// is what the technician would read.
pub fn install(key: &Path, ip: Ipv4Addr, role: &Role) -> io::Result<String> {
    let script = format!("cloud-init status --wait >/dev/null 2>&1; {}", role.command());
    ssh(key, ip, &script, Duration::from_secs(300))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_commands_are_the_readme_one_liners() {
        assert_eq!(Role::Bootstrap.command(), "curl -fsSL http://10.0.0.1:7480/install.sh | sudo sh -s -- init --vip 10.0.3.253");
        assert_eq!(Role::Join("abc".into()).command(), "curl -fsSL http://10.0.0.1:7480/install.sh | sudo sh -s -- join 10.0.3.253 abc");
    }

    #[test]
    fn the_token_is_read_off_init_output() {
        let out = "ms01-01 is a new cluster.\n\nTo join another machine, run on it:\n  curl -fsSL http://10.0.0.1:7480/install.sh | sudo sh -s -- join 10.0.3.253 0123abcd\n";
        assert_eq!(token_in(out).as_deref(), Some("0123abcd"));
        assert_eq!(token_in("nothing"), None);
    }
}
