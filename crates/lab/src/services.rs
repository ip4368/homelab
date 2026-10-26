//! What the homelab's network provides around the machines, all on the lab
//! host behind the router port: the router's addresses, bootstrap DHCP,
//! time, an HTTP proxy to the outside, the `homelabd` download, and each
//! switch's management interface.

use crate::switch::io::LabSwitch;
use crate::topology::{SwitchSpec, BIN_PORT, DHCP_FIRST, DHCP_LAST, RELAY_PORT, ROUTER_IP, ROUTER_TAP, ROUTER_VLAN_IPS, SWITCH_MTU};
use control::proto::Msg;
use control::runtime::{Addr, Endpoint, Svc};
use node::wire::{read_frame, write_frame, Wire, PORT};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub fn sh(cmd: &str, args: &[&str]) -> io::Result<String> {
    let out = Command::new(cmd).args(args).output()?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(io::Error::other(format!("{cmd} {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim())))
    }
}

/// Puts the router's addresses on the router tap: the mgmt gateway, each
/// switch's management address, and a VLAN sub-interface with the gateway
/// of each VM network the router serves.
pub fn configure_router(switches: &[SwitchSpec]) -> io::Result<()> {
    sh("ip", &["link", "set", ROUTER_TAP, "mtu", &SWITCH_MTU.to_string(), "up"])?;
    sh("ip", &["addr", "replace", &format!("{ROUTER_IP}/22"), "dev", ROUTER_TAP])?;
    for s in switches {
        sh("ip", &["addr", "replace", &format!("{}/32", s.mgmt_ip), "dev", ROUTER_TAP])?;
    }
    for (vlan, cidr) in ROUTER_VLAN_IPS {
        let vif = format!("{ROUTER_TAP}.{vlan}");
        let _ = sh("ip", &["link", "add", "link", ROUTER_TAP, "name", &vif, "type", "vlan", "id", &vlan.to_string()]);
        sh("ip", &["addr", "replace", cidr, "dev", &vif])?;
        sh("ip", &["link", "set", &vif, "up"])?;
    }
    Ok(())
}

/// A daemon the lab runs, stopped with the lab.
pub struct Daemon {
    pub name: &'static str,
    pub pid_file: PathBuf,
}

impl Daemon {
    /// Stops it and waits (up to 2 s) for it to go, so a restart can bind
    /// its sockets. A stale pid file's pid, reused by another process, is
    /// left alone.
    pub fn stop(&self) {
        let Ok(pid) = std::fs::read_to_string(&self.pid_file) else { return };
        let Ok(pid) = pid.trim().parse::<i32>() else { return };
        let ours = || std::fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|c| String::from_utf8_lossy(&c).contains(self.name));
        if !ours() {
            return;
        }
        // SAFETY: sending SIGTERM to the daemon we started (checked above).
        unsafe { libc::kill(pid, libc::SIGTERM) };
        for _ in 0..20 {
            if !ours() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

/// Starts dnsmasq handing out the bootstrap pool on the mgmt network (DHCP only, no DNS).
pub fn start_dnsmasq(run: &Path) -> io::Result<Daemon> {
    let pid_file = run.join("dnsmasq.pid");
    let d = Daemon { name: "dnsmasq", pid_file: pid_file.clone() };
    d.stop();
    sh(
        "dnsmasq",
        &[
            &format!("--interface={ROUTER_TAP}"),
            "--bind-interfaces",
            "--except-interface=lo",
            "--port=0",
            "--dhcp-authoritative",
            &format!("--dhcp-range={DHCP_FIRST},{DHCP_LAST},255.255.252.0,12h"),
            &format!("--dhcp-option=3,{ROUTER_IP}"),
            &format!("--dhcp-leasefile={}", run.join("dnsmasq.leases").display()),
            &format!("--pid-file={}", pid_file.display()),
            &format!("--log-facility={}", run.join("dnsmasq.log").display()),
        ],
    )?;
    Ok(d)
}

/// Starts a chronyd that serves the lab host's clock on the router address
/// without touching the host's clock (`-x`).
pub fn start_chronyd(run: &Path) -> io::Result<Daemon> {
    let pid_file = run.join("chronyd.pid");
    let d = Daemon { name: "chronyd", pid_file: pid_file.clone() };
    d.stop();
    let conf = run.join("chrony.conf");
    std::fs::write(
        &conf,
        format!(
            "port 123\nbindaddress {ROUTER_IP}\ncmdport 0\nlocal stratum 8\nallow 10.0.0.0/22\npidfile {}\ndriftfile {}\n",
            pid_file.display(),
            run.join("chrony.drift").display()
        ),
    )?;
    std::thread::sleep(Duration::from_millis(200));
    sh("chronyd", &["-x", "-f", &conf.to_string_lossy()])?;
    Ok(d)
}

/// Relays the guests' proxy connections on the router to `upstream` (the
/// lab host's own HTTP proxy): the lab network has no route out.
pub async fn run_relay(upstream: String) -> io::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from((ROUTER_IP, RELAY_PORT))).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else { continue };
            let upstream = upstream.clone();
            tokio::spawn(async move {
                if let Ok(mut out) = TcpStream::connect(&upstream).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut out).await;
                }
            });
        }
    });
    Ok(())
}

/// Serves `GET /homelabd` with the current build of `binary`, re-read on
/// every request so a rebuild reaches the next machine to boot.
pub async fn run_binary_server(binary: PathBuf) -> io::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from((ROUTER_IP, BIN_PORT))).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else { continue };
            let binary = binary.clone();
            tokio::spawn(async move {
                let mut req = [0u8; 1024];
                let Ok(n) = s.read(&mut req).await else { return };
                let wanted = String::from_utf8_lossy(&req[..n]).starts_with("GET /homelabd ");
                let reply = match (wanted, tokio::fs::read(&binary).await) {
                    (true, Ok(body)) => {
                        let mut r = format!("HTTP/1.0 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n", body.len()).into_bytes();
                        r.extend_from_slice(&body);
                        r
                    }
                    _ => b"HTTP/1.0 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
                };
                let _ = s.write_all(&reply).await;
            });
        }
    });
    Ok(())
}

/// Each switch's management interface: answers the control plane's
/// `SwitchPoll` with the switch's own configuration, over the node wire
/// format, while the switch is up and reachable from the router.
pub async fn run_switch_mgmt(switches: Vec<SwitchSpec>, fabric: Arc<LabSwitch>) -> io::Result<()> {
    for s in switches {
        let listener = TcpListener::bind(SocketAddr::from((s.mgmt_ip, PORT))).await?;
        let fabric = fabric.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { continue };
                tokio::spawn(serve_mgmt(stream, s.clone(), fabric.clone()));
            }
        });
    }
    Ok(())
}

async fn serve_mgmt(stream: TcpStream, s: SwitchSpec, fabric: Arc<LabSwitch>) {
    let _ = stream.set_nodelay(true);
    let (mut r, w) = stream.into_split();
    let w = Arc::new(tokio::sync::Mutex::new(w));
    let ka = w.clone();
    let keepalive = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if write_frame(&mut *ka.lock().await, &Wire::Keepalive).await.is_err() {
                return;
            }
        }
    });
    let me = Endpoint::new(Addr::Switch(s.id.clone()), Svc::Switch);
    while let Ok(Ok(frame)) = tokio::time::timeout(Duration::from_secs(3), read_frame(&mut r)).await {
        let Wire::Msg { from, msg: Msg::SwitchPoll, .. } = frame else { continue };
        let report = fabric.with(|c| {
            (c.switch_up(&s.id) && c.reachable_from_router(&s.id)).then(|| Msg::SwitchReport {
                switch: s.id.clone(),
                model: c.model(&s.id).unwrap_or_default().to_string(),
                ports: c.report(&s.id),
                mlag_peer: None,
            })
        });
        if let Some(msg) = report
            && write_frame(&mut *w.lock().await, &Wire::Msg { from: me.clone(), to: from, msg }).await.is_err()
        {
            break;
        }
    }
    keepalive.abort();
}

/// The lab host's upstream HTTP proxy: `$LAB_UPSTREAM_PROXY`, else the host
/// part of `$https_proxy`/`$http_proxy`, else `fwdproxy:8080`.
pub fn upstream_proxy() -> String {
    if let Ok(p) = std::env::var("LAB_UPSTREAM_PROXY") {
        return p;
    }
    for var in ["https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY"] {
        if let Ok(p) = std::env::var(var) {
            let p = p.trim_start_matches("http://").trim_start_matches("https://").trim_end_matches('/');
            if !p.is_empty() {
                return p.to_string();
            }
        }
    }
    "fwdproxy:8080".into()
}

pub fn router_ip() -> Ipv4Addr {
    ROUTER_IP
}
