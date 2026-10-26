//! `homelabd`: the daemon on every host.
//!
//! - `homelabd run` runs the host's agent, and its CP replica if the host
//!   holds CP state, until killed.
//! - `homelabd bootstrap` makes this host a new cluster of one (it must not
//!   have joined one), like `k3s server --cluster-init`; `run` then starts it.
//!
//! Both read the node config, `/etc/homelab/node.json` unless `--config` says otherwise.

use control::agent::HostAgent;
use control::bootstrap::{seeded_durable, SeedHost};
use control::controllers::join::UNKNOWN_LOCATION;
use control::replica::CpReplica;
use control::runtime::{Addr, HostOs, Svc};
use model::{HostId, SwitchId};
use node::linux::store::write_synced;
use node::linux::{LinuxOs, LinuxVip, Paths};
use node::runtime::{wall_ms, Node, NodeConfig};
use node::transport::AddressBook;
use node::wire::PORT;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::ExitCode;

/// `/etc/homelab/node.json`, written when the host is installed.
#[derive(Debug, Deserialize)]
struct NodeFile {
    host: HostId,
    /// The join token; ignored once the host has joined.
    #[serde(default)]
    token: Option<String>,
    vip: Ipv4Addr,
    /// The management subnet the VIP lives in.
    mgmt: ipnet::Ipv4Net,
    /// Switch management addresses, as the site's DHCP or install config gives them.
    #[serde(default)]
    switches: BTreeMap<SwitchId, Ipv4Addr>,
}

fn usage() -> ExitCode {
    eprintln!("usage: homelabd run|bootstrap [--config PATH]");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut config = PathBuf::from("/etc/homelab/node.json");
    if let Some(i) = args.iter().position(|a| a == "--config") {
        let Some(p) = args.get(i + 1) else { return usage() };
        config = p.into();
    }
    let cfg: NodeFile = match std::fs::read(&config).map_err(|e| e.to_string()).and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string())) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("homelabd: read {}: {e}", config.display());
            return ExitCode::FAILURE;
        }
    };
    match args.first().map(String::as_str) {
        Some("run") => run(cfg),
        Some("bootstrap") => bootstrap(&cfg),
        _ => usage(),
    }
}

fn bootstrap(cfg: &NodeFile) -> ExitCode {
    let mut os = LinuxOs::new(cfg.host.clone(), Paths::default());
    if os.joined() || os.load_cp().is_some() {
        eprintln!("homelabd: {} already belongs to a cluster", cfg.host);
        return ExitCode::FAILURE;
    }
    let host = SeedHost { id: cfg.host.clone(), inventory: os.inventory(), location: UNKNOWN_LOCATION.into(), running: None };
    os.store_cp(seeded_durable(&[host], &[cfg.host.clone()].into(), wall_ms()));
    os.mark_joined();
    eprintln!("homelabd: {} is a new cluster; `homelabd run` starts it", cfg.host);
    ExitCode::SUCCESS
}

fn run(cfg: NodeFile) -> ExitCode {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a tokio runtime");
    rt.block_on(async move {
        let paths = Paths::default();
        let dir_file = paths.state.join("directory.json");
        let mut os = LinuxOs::new(cfg.host.clone(), paths);
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
