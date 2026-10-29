//! `homelabd`: the daemon on every host, and the commands that install it.
//!
//! - `homelabd init --vip IP` makes this machine a new cluster of one, like
//!   `k3s server --cluster-init`: it installs itself as a systemd service,
//!   seeds the cluster, starts, and prints the command that joins others.
//! - `homelabd join VIP TOKEN` installs it on a machine that joins the
//!   cluster whose control plane holds `VIP`.
//! - `homelabd token` asks the cluster for a new join token.
//! - `homelabd kubeconfig` prints an admin kubeconfig for the cluster's
//!   Kubernetes, pointed at the VIP (this host's own if it runs a K3s
//!   server, else the control plane's leader's).
//! - `homelabd status` prints what this host is and how its Kubernetes stands.
//! - `homelabd upgrade` installs this binary over the running one and restarts it.
//! - `homelabd run` runs the host's agent, and its CP replica if the host
//!   holds CP state, until killed (what the service runs).
//!
//! All read or write the node config, `/etc/homelab/node.json` unless
//! `--config` says otherwise. Every command but `run` needs root.

use control::agent::HostAgent;
use control::bootstrap::{seeded_durable, SeedHost};
use control::client::ClientActor;
use control::controllers::join::UNKNOWN_LOCATION;
use control::proto::{ApiOp, ApiResult, LogCmd};
use control::raft::{Entry, EntryPayload};
use control::replica::{CpReplica, JOIN_TOKEN_TTL_MS};
use control::state::Command;
use control::runtime::{Addr, HostOs, Svc};
use model::{HostId, SwitchId};
use node::linux::kube::{admin_kubeconfig, status_path};
use node::linux::store::write_synced;
use node::linux::{sh, LinuxOs, LinuxVip, Paths};
use node::runtime::{wall_ms, Node, NodeConfig, NodeHandle};
use node::transport::AddressBook;
use node::wire::PORT;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

const BIN: &str = "/usr/local/bin/homelabd";
const UNIT_PATH: &str = "/etc/systemd/system/homelabd.service";
const UNIT: &str = "[Unit]
Description=homelab node daemon
After=network-online.target time-sync.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/homelabd run
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
";

/// `/etc/homelab/node.json`, written by `init` or `join`.
#[derive(Debug, Serialize, Deserialize)]
struct NodeFile {
    host: HostId,
    /// The join token; ignored once the host has joined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    vip: Ipv4Addr,
    /// The management subnet the VIP lives in (found at install).
    mgmt: ipnet::Ipv4Net,
    /// Where `install.sh` came from, to print join commands with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    /// Switch management addresses for switches that do not advertise one
    /// over LLDP.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    switches: BTreeMap<SwitchId, Ipv4Addr>,
}

const USAGE: &str = "usage:
  homelabd init --vip IP [--host NAME] [--source URL]   start a new cluster on this machine
  homelabd join VIP TOKEN [--host NAME] [--source URL]  join this machine to a cluster
  homelabd token [-q]                                   create a join token (on a member)
  homelabd kubeconfig                                   print an admin kubeconfig (on a member)
  homelabd status                                       what this host is and how its Kubernetes stands
  homelabd upgrade                                      install this binary and restart the service
  homelabd run                                          run the daemon (the service does this)
options: --config PATH (default /etc/homelab/node.json)";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

/// The command line: positional arguments, and `--name value` options
/// (`-q` is a flag).
struct Args {
    pos: Vec<String>,
    opts: BTreeMap<String, String>,
    quiet: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut a = Args { pos: vec![], opts: BTreeMap::new(), quiet: false };
    while let Some(s) = it.next() {
        if s == "-q" || s == "--quiet" {
            a.quiet = true;
        } else if let Some(name) = s.strip_prefix("--") {
            let v = it.next().ok_or(format!("--{name} needs a value"))?;
            a.opts.insert(name.into(), v);
        } else {
            a.pos.push(s);
        }
    }
    Ok(a)
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("homelabd: {e}");
            return usage();
        }
    };
    let config = PathBuf::from(args.opts.get("config").map_or("/etc/homelab/node.json", String::as_str));
    let result = match args.pos.first().map(String::as_str) {
        Some("run") => {
            return match read_config(&config) {
                Ok(cfg) => run(cfg),
                Err(e) => fail(e),
            };
        }
        Some("init") => init(&args, &config),
        Some("join") => join(&args, &config),
        Some("token") => token(&args, &config),
        Some("kubeconfig") => kubeconfig(&config),
        Some("status") => status(&config),
        Some("upgrade") => upgrade(),
        _ => return usage(),
    };
    result.map_or_else(fail, |()| ExitCode::SUCCESS)
}

fn fail(e: String) -> ExitCode {
    eprintln!("homelabd: {e}");
    ExitCode::FAILURE
}

fn read_config(path: &Path) -> Result<NodeFile, String> {
    let b = std::fs::read(path).map_err(|e| format!("read {}: {e} (is this machine installed? see `homelabd init` and `homelabd join`)", path.display()))?;
    serde_json::from_slice(&b).map_err(|e| format!("read {}: {e}", path.display()))
}

fn write_config(path: &Path, cfg: &NodeFile) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    let body = serde_json::to_vec_pretty(cfg).expect("the node config serializes");
    write_synced(path, &body).map_err(|e| format!("write {}: {e}", path.display()))
}

/// This machine's host name: `--host`, else the kernel's host name.
fn host_name(args: &Args) -> Result<HostId, String> {
    let name = match args.opts.get("host") {
        Some(h) => h.clone(),
        None => std::fs::read_to_string("/proc/sys/kernel/hostname").map_err(|e| format!("read the host name: {e}"))?.trim().to_string(),
    };
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(format!("{name:?} is not a usable host name (letters, digits and '-'); pass --host"));
    }
    Ok(HostId::new(name.to_ascii_lowercase()))
}

/// The subnet of the local address the VIP is in: the management network.
fn mgmt_subnet(vip: Ipv4Addr) -> Result<ipnet::Ipv4Net, String> {
    let out = sh("ip", &["-j", "-4", "addr", "show"]).map_err(|e| e.to_string())?;
    let ifaces: serde_json::Value = serde_json::from_str(&out).map_err(|e| format!("ip -j addr: {e}"))?;
    for a in ifaces.as_array().into_iter().flatten().flat_map(|i| i["addr_info"].as_array().into_iter().flatten()) {
        let (Some(local), Some(prefix)) = (a["local"].as_str(), a["prefixlen"].as_u64()) else { continue };
        let Ok(ip) = local.parse::<Ipv4Addr>() else { continue };
        if let Ok(net) = ipnet::Ipv4Net::new(ip, prefix as u8)
            && net.prefix_len() < 32
            && net.trunc().contains(&vip)
            && ip != vip
        {
            return Ok(net.trunc());
        }
    }
    Err(format!("no address of this machine is in the same subnet as the VIP {vip}: the VIP must be on the management network"))
}

/// Copies this binary to [`BIN`] (unless it runs from there), writes the
/// systemd unit and reloads systemd.
fn install_self() -> Result<(), String> {
    let me = std::env::current_exe().map_err(|e| format!("find this binary: {e}"))?;
    let installed = std::fs::canonicalize(BIN).ok();
    if installed.as_deref() != Some(me.as_path()) {
        let tmp = format!("{BIN}.new");
        std::fs::copy(&me, &tmp).map_err(|e| format!("copy to {tmp}: {e}"))?;
        sh("chmod", &["0755", &tmp]).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, BIN).map_err(|e| format!("install {BIN}: {e}"))?;
    }
    std::fs::write(UNIT_PATH, UNIT).map_err(|e| format!("write {UNIT_PATH}: {e}"))?;
    sh("systemctl", &["daemon-reload"]).map_err(|e| e.to_string())?;
    Ok(())
}

fn joined() -> bool {
    Paths::default().state.join("joined").exists()
}

fn join_command(cfg: &NodeFile, token: &str) -> String {
    let source = cfg.source.as_deref().unwrap_or("<url>");
    format!("curl -fsSL {source}/install.sh | sudo sh -s -- join {} {token}", cfg.vip)
}

fn init(args: &Args, config: &Path) -> Result<(), String> {
    if joined() {
        println!("This machine already belongs to a cluster. `sudo homelabd token` prints a join command for another machine.");
        return Ok(());
    }
    let vip: Ipv4Addr = args.opts.get("vip").ok_or("init needs --vip IP: a free address on the management network for the control plane")?.parse().map_err(|_| "--vip must be an IPv4 address")?;
    let host = host_name(args)?;
    let cfg = NodeFile { host: host.clone(), token: None, vip, mgmt: mgmt_subnet(vip)?, source: args.opts.get("source").cloned(), switches: BTreeMap::new() };
    let mut os = LinuxOs::new(host.clone(), Paths::default());
    if os.load_cp().is_some() {
        return Err(format!("{host} holds control-plane state from an earlier cluster; remove /var/lib/homelab to start over"));
    }
    install_self()?;
    write_config(config, &cfg)?;
    let kube_token = format!("{:032x}", rand::random::<u128>());
    // The first join token is part of the new cluster, so it can be printed
    // now: in a moment the machine moves to its static address, and an SSH
    // session to the address it has now goes with it.
    let join_token = format!("{:016x}", rand::random::<u64>());
    let seed = SeedHost { id: host.clone(), inventory: os.inventory(), location: UNKNOWN_LOCATION.into() };
    let now = wall_ms();
    let mut durable = seeded_durable(&[seed], &[host.clone()].into(), &kube_token, now);
    let cmd = Command::CreateJoinToken { token: join_token.clone(), ttl_ms: JOIN_TOKEN_TTL_MS };
    durable.log.push(Entry { term: 1, index: durable.log.len() as u64 + 1, payload: EntryPayload::Normal(LogCmd { at: now, cmd }) });
    os.store_cp(durable);
    os.mark_joined();
    sh("systemctl", &["enable", "--now", "homelabd"]).map_err(|e| e.to_string())?;
    println!("{host} is a new cluster; its control plane holds {vip}.");
    println!("In a moment it moves to its static address (an SSH session to its current address will drop), then installs");
    println!("Kubernetes (K3s) and Longhorn. `sudo homelabd status` on it shows how far it got.");
    println!("\nTo join another machine, run on it (the token is valid for 24 h; `sudo homelabd token` makes a new one):\n  {}", join_command(&cfg, &join_token));
    Ok(())
}

fn join(args: &Args, config: &Path) -> Result<(), String> {
    if joined() {
        println!("This machine already belongs to a cluster.");
        return Ok(());
    }
    let (Some(vip), Some(token)) = (args.pos.get(1), args.pos.get(2)) else { return Err("join needs the cluster's VIP and a join token: homelabd join VIP TOKEN".into()) };
    let vip: Ipv4Addr = vip.parse().map_err(|_| format!("{vip:?} is not an IPv4 address"))?;
    let host = host_name(args)?;
    let cfg = NodeFile { host: host.clone(), token: Some(token.clone()), vip, mgmt: mgmt_subnet(vip)?, source: args.opts.get("source").cloned(), switches: BTreeMap::new() };
    install_self()?;
    write_config(config, &cfg)?;
    sh("systemctl", &["enable", "--now", "homelabd"]).map_err(|e| e.to_string())?;
    println!("{host} is joining the cluster at {vip}. It is approved by its rule (or waits under Pending joins in the UI),");
    println!("moves to its static address (an SSH session to its current address will drop), then joins Kubernetes.");
    println!("`sudo homelabd status` on it shows how far it got; `journalctl -u homelabd` shows why it is stuck.");
    Ok(())
}

/// Asks the control plane, through the VIP, for `op`'s answer: a string.
fn ask(cfg: &NodeFile, op: ApiOp, wait: Duration) -> Result<String, String> {
    let mut book = AddressBook::default();
    book.fixed.insert(Addr::Vip, cfg.vip);
    let me = Addr::Admin(rand::random());
    let client = NodeHandle::spawn(NodeConfig { me, listen: None, port: PORT, book }, |n| n.start(Svc::Client, Box::new(ClientActor::new())))
        .map_err(|e| format!("start a client: {e}"))?;
    let deadline = Instant::now() + wait;
    loop {
        let op = op.clone();
        let req = client.exec_blocking(move |n| n.with_actor::<ClientActor, _>(Svc::Client, |c, ctx| c.submit(ctx, op))).flatten();
        let mut answer = None;
        let asked = Instant::now();
        while let Some(req) = req
            && answer.is_none()
            && asked.elapsed() < Duration::from_secs(6)
        {
            std::thread::sleep(Duration::from_millis(50));
            answer = client.exec_blocking(move |n| n.with_actor::<ClientActor, _>(Svc::Client, |c, _| c.take_result(req)).flatten()).flatten();
        }
        match answer {
            Some(ApiResult::Ok(v)) => return v.as_str().map(String::from).ok_or_else(|| "the control plane's answer is not text".into()),
            Some(ApiResult::Err(e)) => return Err(e),
            _ if Instant::now() >= deadline => return Err(format!("the control plane at {} did not answer", cfg.vip)),
            _ => std::thread::sleep(Duration::from_secs(1)),
        }
    }
}

fn token(args: &Args, config: &Path) -> Result<(), String> {
    let cfg = read_config(config)?;
    let t = ask(&cfg, ApiOp::CreateJoinToken, Duration::from_secs(20))?;
    if args.quiet {
        println!("{t}");
    } else {
        println!("Join token (valid 24 h): {t}\nOn the machine to join, run:\n  {}", join_command(&cfg, &t));
    }
    Ok(())
}

fn kubeconfig(config: &Path) -> Result<(), String> {
    let cfg = read_config(config)?;
    let k = match admin_kubeconfig(cfg.vip) {
        Ok(k) => k,
        Err(_) => ask(&cfg, ApiOp::GetKubeconfig, Duration::from_secs(20))?,
    };
    print!("{k}");
    Ok(())
}

fn status(config: &Path) -> Result<(), String> {
    let cfg = read_config(config)?;
    println!("host: {}\ncluster VIP: {}\nmember: {}", cfg.host, cfg.vip, if joined() { "yes" } else { "no (joining)" });
    match std::fs::read_to_string(status_path(&Paths::default())) {
        Ok(s) => println!("kubernetes: {}", s.trim()),
        Err(_) => println!("kubernetes: nothing yet (it comes once the host is Ready on its network)"),
    }
    Ok(())
}

fn upgrade() -> Result<(), String> {
    install_self()?;
    // An upgrade is often the fix for a daemon that keeps failing, which
    // systemd stops restarting after a while: clear that first.
    let _ = sh("systemctl", &["reset-failed", "homelabd"]);
    sh("systemctl", &["restart", "homelabd"]).map_err(|e| e.to_string())?;
    println!("homelabd upgraded and restarted.");
    Ok(())
}

fn run(cfg: NodeFile) -> ExitCode {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a tokio runtime");
    rt.block_on(async move {
        let paths = Paths::default();
        let dir_file = paths.state.join("directory.json");
        let mut os = LinuxOs::new(cfg.host.clone(), paths);
        os.set_vip(cfg.vip);
        os.boot();
        let mut book = AddressBook::default();
        book.fixed.insert(Addr::Vip, cfg.vip);
        book.fixed.extend(cfg.switches.iter().map(|(id, ip)| (Addr::Switch(id.clone()), *ip)));
        if let Ok(b) = std::fs::read(&dir_file)
            && let Ok(dir) = serde_json::from_slice(&b)
        {
            book.hosts = dir;
        }
        let token = if os.joined() { None } else { cfg.token.clone() };
        let voters = os.load_cp().map(|d| d.voters);
        let node_cfg = NodeConfig {
            me: Addr::Host(cfg.host.clone()),
            listen: Some(SocketAddr::from(([0, 0, 0, 0], PORT))),
            port: PORT,
            book,
        };
        let vip = LinuxVip { vip: cfg.vip, mgmt: cfg.mgmt };
        let (mut node, inbox) = Node::new(&node_cfg, Some(Box::new(os)), Some(Box::new(vip)));
        if let Err(e) = node.transport().listen(node_cfg.listen.expect("set above")).await {
            eprintln!("homelabd: listen on {PORT}: {e}");
            return ExitCode::FAILURE;
        }
        node.on_directory(move |dir| {
            if let Ok(b) = serde_json::to_vec(dir) {
                let _ = write_synced(&dir_file, &b);
            }
        });
        if let Some(voters) = voters {
            eprintln!("homelabd: resuming the CP replica");
            node.start(Svc::Cp, Box::new(CpReplica::new(cfg.host.clone(), None, voters)));
        }
        eprintln!("homelabd: {} starting as {}", cfg.host, if token.is_some() { "a host joining with a token" } else { "a member" });
        node.start(Svc::Agent, Box::new(HostAgent::new(token)));
        let (_cmds, rx) = tokio::sync::mpsc::unbounded_channel();
        node.run(inbox, rx).await;
        ExitCode::SUCCESS
    })
}
