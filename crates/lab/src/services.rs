//! What the homelab's network provides around the machines, all on the lab
//! host behind the router port: the router's addresses, bootstrap DHCP, DNS,
//! time, the internet uplink, the `homelabd` download, and each switch's
//! management interface.

use crate::switch::io::LabSwitch;
use crate::topology::{SwitchSpec, BIN_PORT, DHCP_FIRST, DHCP_LAST, ROUTER_IP, ROUTER_TAP, ROUTER_VLAN_IPS, SWITCH_MTU};
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

/// Starts dnsmasq: the bootstrap DHCP pool on the mgmt network, and DNS
/// that answers every name with the router's address (see [`run_egress`]).
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
            "--no-resolv",
            "--no-hosts",
            &format!("--address=/#/{ROUTER_IP}"),
            // Every other record type (AAAA above all) is answered "no
            // data", not refused: Go's resolver (containerd's) fails a whole
            // lookup on a refused AAAA.
            "--local=/#/",
            "--dhcp-authoritative",
            &format!("--dhcp-range={DHCP_FIRST},{DHCP_LAST},255.255.252.0,12h"),
            &format!("--dhcp-option=3,{ROUTER_IP}"),
            &format!("--dhcp-option=6,{ROUTER_IP}"),
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

/// The ports the lab's internet uplink carries: HTTP and HTTPS.
pub const EGRESS_PORTS: [u16; 2] = [80, 443];
/// How long a guest has to send enough of its first request to name the host.
const EGRESS_HELLO: Duration = Duration::from_secs(10);
/// The most of a first request read while looking for the host name.
const EGRESS_PEEK_MAX: usize = 32 * 1024;

/// The lab network's internet uplink, as close to a plain one as the lab
/// host allows (it reaches the internet only through an HTTP proxy, and only
/// over IPv6). Guests need no proxy settings: every name resolves to the
/// router ([`start_dnsmasq`]), and the router relays each connection to
/// port 80 or 443 through `upstream` to the host it names: the HTTP `Host`
/// header, or the TLS ClientHello's server name. Anything else (another
/// port, an address without a name, UDP) has no way out.
pub async fn run_egress(upstream: String) -> io::Result<()> {
    for port in EGRESS_PORTS {
        let listener = TcpListener::bind(SocketAddr::from((ROUTER_IP, port))).await?;
        let upstream = upstream.clone();
        tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else { continue };
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    if let Err(e) = relay(inbound, port, &upstream).await {
                        eprintln!("lab: egress on :{port}: {e}");
                    }
                });
            }
        });
    }
    Ok(())
}

async fn relay(mut inbound: TcpStream, port: u16, upstream: &str) -> io::Result<()> {
    let mut first = Vec::with_capacity(2048);
    let host = tokio::time::timeout(EGRESS_HELLO, async {
        let mut buf = [0u8; 4096];
        loop {
            let n = inbound.read(&mut buf).await?;
            if n == 0 {
                return Err(io::Error::other("closed before naming a host"));
            }
            first.extend_from_slice(&buf[..n]);
            let found = if port == 443 { tls_server_name(&first) } else { http_host(&first) };
            match found {
                Named::Host(h) => return Ok(h),
                Named::Never => return Err(io::Error::other("the request names no host")),
                Named::NeedMore if first.len() >= EGRESS_PEEK_MAX => return Err(io::Error::other("no host name in the first request")),
                Named::NeedMore => {}
            }
        }
    })
    .await
    .map_err(|_| io::Error::other("timed out waiting for the request"))??;
    let mut out = TcpStream::connect(upstream).await?;
    out.write_all(format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n").as_bytes()).await?;
    let mut reply = Vec::new();
    let mut b = [0u8; 1];
    while !reply.ends_with(b"\r\n\r\n") {
        if out.read(&mut b).await? == 0 || reply.len() > 8192 {
            return Err(io::Error::other(format!("the upstream proxy hung up on CONNECT {host}:{port}")));
        }
        reply.push(b[0]);
    }
    let status = String::from_utf8_lossy(&reply);
    if !status.split_whitespace().nth(1).is_some_and(|c| c.starts_with('2')) {
        return Err(io::Error::other(format!("CONNECT {host}:{port}: {}", status.lines().next().unwrap_or(""))));
    }
    out.write_all(&first).await?;
    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut out).await;
    Ok(())
}

/// What the start of a request says about the host it is for.
#[derive(Debug, PartialEq, Eq)]
pub enum Named {
    Host(String),
    /// Not enough of it yet.
    NeedMore,
    /// It will not say.
    Never,
}

fn host_name_ok(h: &str) -> bool {
    !h.is_empty() && h.len() <= 253 && h.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
}

/// The host of an HTTP request's `Host` header (port dropped).
pub fn http_host(req: &[u8]) -> Named {
    let Some(end) = req.windows(4).position(|w| w == b"\r\n\r\n") else { return Named::NeedMore };
    let head = String::from_utf8_lossy(&req[..end]);
    for line in head.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':')
            && k.trim().eq_ignore_ascii_case("host")
        {
            let v = v.trim();
            let host = v.rsplit_once(':').filter(|(_, p)| p.chars().all(|c| c.is_ascii_digit())).map_or(v, |(h, _)| h);
            return if host_name_ok(host) { Named::Host(host.to_ascii_lowercase()) } else { Named::Never };
        }
    }
    Named::Never
}

/// The server name (SNI) of a TLS ClientHello.
pub fn tls_server_name(b: &[u8]) -> Named {
    if b.len() < 5 {
        return Named::NeedMore;
    }
    if b[0] != 0x16 {
        return Named::Never;
    }
    let record = 5 + usize::from(u16::from_be_bytes([b[3], b[4]]));
    if b.len() < record {
        return Named::NeedMore;
    }
    let h = &b[5..record];
    sni(h).map_or(Named::Never, Named::Host)
}

fn sni(h: &[u8]) -> Option<String> {
    let u16_at = |i: usize| Some(usize::from(u16::from_be_bytes([*h.get(i)?, *h.get(i + 1)?])));
    if *h.first()? != 1 {
        return None;
    }
    // Handshake header (4), version (2), random (32).
    let mut i = 4 + 2 + 32;
    i += 1 + usize::from(*h.get(i)?); // session id
    i += 2 + u16_at(i)?; // cipher suites
    i += 1 + usize::from(*h.get(i)?); // compression methods
    let end = i + 2 + u16_at(i)?;
    i += 2;
    while i + 4 <= end.min(h.len()) {
        let (kind, len) = (u16_at(i)?, u16_at(i + 2)?);
        let body = h.get(i + 4..i + 4 + len)?;
        if kind == 0 {
            // Server name list: list length (2), name type (1, 0 = host), name length (2), name.
            let n = usize::from(u16::from_be_bytes([*body.get(3)?, *body.get(4)?]));
            let name = std::str::from_utf8(body.get(5..5 + n)?).ok()?;
            return host_name_ok(name).then(|| name.to_ascii_lowercase());
        }
        i += 4 + len;
    }
    None
}

/// What a machine is installed from: `install.sh`, with its download URL filled in.
pub fn install_script() -> String {
    include_str!("../../node/install.sh").replace("@URL@", &format!("http://{ROUTER_IP}:{BIN_PORT}"))
}

/// Serves `GET /install.sh` and `GET /homelabd` (the current build of
/// `binary`, re-read on every request so a rebuild reaches the next machine
/// installed or upgraded): what a release download site would.
pub async fn run_binary_server(binary: PathBuf) -> io::Result<()> {
    let listener = TcpListener::bind(SocketAddr::from((ROUTER_IP, BIN_PORT))).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = listener.accept().await else { continue };
            let binary = binary.clone();
            tokio::spawn(async move {
                let mut req = [0u8; 1024];
                let Ok(n) = s.read(&mut req).await else { return };
                let req = String::from_utf8_lossy(&req[..n]);
                let body = if req.starts_with("GET /homelabd ") {
                    tokio::fs::read(&binary).await.ok()
                } else if req.starts_with("GET /install.sh ") {
                    Some(install_script().into_bytes())
                } else {
                    None
                };
                let reply = match body {
                    Some(body) => {
                        let mut r = format!("HTTP/1.0 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\n\r\n", body.len()).into_bytes();
                        r.extend_from_slice(&body);
                        r
                    }
                    None => b"HTTP/1.0 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A TLS 1.3 ClientHello as curl sends it, naming `name`.
    fn client_hello(name: &str) -> Vec<u8> {
        let mut sni = vec![];
        sni.extend_from_slice(&((name.len() + 3) as u16).to_be_bytes());
        sni.push(0);
        sni.extend_from_slice(&(name.len() as u16).to_be_bytes());
        sni.extend_from_slice(name.as_bytes());
        let mut ext = vec![0x00, 0x2b, 0x00, 0x03, 0x02, 0x03, 0x04]; // supported_versions first
        ext.extend_from_slice(&[0, 0]);
        ext.extend_from_slice(&(sni.len() as u16).to_be_bytes());
        ext.extend_from_slice(&sni);
        let mut body = vec![0x03, 0x03];
        body.extend_from_slice(&[7; 32]);
        body.extend_from_slice(&[32]);
        body.extend_from_slice(&[9; 32]);
        body.extend_from_slice(&[0, 4, 0x13, 0x01, 0x13, 0x02]);
        body.extend_from_slice(&[1, 0]);
        body.extend_from_slice(&(ext.len() as u16).to_be_bytes());
        body.extend_from_slice(&ext);
        let mut hs = vec![1];
        hs.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
        hs.extend_from_slice(&body);
        let mut rec = vec![0x16, 0x03, 0x01];
        rec.extend_from_slice(&(hs.len() as u16).to_be_bytes());
        rec.extend_from_slice(&hs);
        rec
    }

    #[test]
    fn a_tls_client_hello_names_its_server() {
        let hello = client_hello("registry-1.docker.io");
        assert_eq!(tls_server_name(&hello), Named::Host("registry-1.docker.io".into()));
        assert_eq!(tls_server_name(&hello[..40]), Named::NeedMore, "the record is not all here yet");
        assert_eq!(tls_server_name(b"GET / HTTP/1.1"), Named::Never);
    }

    #[test]
    fn an_http_request_names_its_host() {
        assert_eq!(http_host(b"GET /debian/ HTTP/1.1\r\nHost: deb.debian.org\r\nUser-Agent: apt\r\n\r\n"), Named::Host("deb.debian.org".into()));
        assert_eq!(http_host(b"GET / HTTP/1.1\r\nhost: Example.COM:8080\r\n\r\n"), Named::Host("example.com".into()));
        assert_eq!(http_host(b"GET / HTTP/1.1\r\nHost: deb"), Named::NeedMore);
        assert_eq!(http_host(b"GET / HTTP/1.0\r\n\r\n"), Named::Never);
    }

    #[test]
    fn the_install_script_points_at_the_lab() {
        let s = install_script();
        assert!(s.contains("URL=\"${HOMELAB_URL:-http://10.0.0.1:7480}\""), "{s}");
    }
}
