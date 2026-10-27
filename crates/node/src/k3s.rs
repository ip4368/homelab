//! K3s + Longhorn under homelab management.
//!
//! A worker thread (spawned by `homelabd run`, never in tests) converges
//! this host's K3s state: prerequisites, the pinned binary, server (init or
//! join) or agent, and — on the cluster-init host — Longhorn. Everything
//! slow (apt, downloads, `systemctl`, `kubectl`) runs here, off the event
//! loop. The loop only touches the shared [`K3sLink`]: the agent drops the
//! join token in, and reads whether K3s runs.

use crate::linux::store::RaftStore;
use control::k8s_obs::{K8sDeployment, K8sNode, K8sPod, K8sSnapshot, LhDisk, LhNode, LhReplica, LhVolume};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Pinned and proven on the lab (see the k3s bootstrap design).
pub const K3S_VERSION: &str = "v1.36.5+k3s1";
const K3S_SHA256: &str = "d73847bcd3c5fccef0115b372e2f9a91f3032dc84bbf71518a4617565294d313";
const K3S_URL: &str = "https://github.com/k3s-io/k3s/releases/download/v1.36.5%2Bk3s1/k3s";
pub const LONGHORN_VERSION: &str = "v1.13.0";
const LONGHORN_URL: &str = "https://raw.githubusercontent.com/longhorn/longhorn/v1.13.0/deploy/longhorn.yaml";
const K3S_API_PORT: u16 = 6443;
/// K3s's default pod and service CIDRs (what a stock install uses).
const POD_CIDR: &str = "10.42.0.0/16";
const SVC_CIDR: &str = "10.43.0.0/16";

const K3S_BIN: &str = "/usr/local/bin/k3s";
const K3S_ENV: &str = "/etc/systemd/system/k3s.service.env";
const K3S_UNIT: &str = "/etc/systemd/system/k3s.service";
const K3S_CONFIG: &str = "/etc/rancher/k3s/config.yaml";
const K3S_TOKEN: &str = "/var/lib/rancher/k3s/server/node-token";
const K3S_KUBECONFIG: &str = "/etc/rancher/k3s/k3s.yaml";

/// How often the worker converges.
const POLL: Duration = Duration::from_secs(15);
/// How long a member waits before starting a K3s agent: our control plane
/// grows its voters in the first minutes, and a host that is about to
/// become a voter must join as a server, not an agent.
const AGENT_SETTLE: Duration = Duration::from_secs(5 * 60);
/// How long the cluster-init host waits for every voter to be a Ready K3s
/// node before installing Longhorn anyway.
const LONGHORN_WAIT: Duration = Duration::from_secs(15 * 60);
/// How often a server refreshes the observed snapshot (seven `kubectl`
/// queries; the leader serves the file to clients).
const SNAP_EVERY: Duration = Duration::from_secs(30);

/// What the event loop and the worker share.
#[derive(Debug, Default)]
pub struct K3sLink {
    /// The K3s join token the agent last received, until the worker uses it.
    join_token: Mutex<Option<String>>,
    running: AtomicBool,
    /// Whether the host runs a confirmed network config (its address is
    /// final, not bootstrap DHCP): K3s only starts once this holds.
    net_applied: AtomicBool,
}

impl K3sLink {
    pub fn set_join(&self, token: String) {
        *self.join_token.lock().expect("k3s link") = Some(token);
    }

    pub fn join_token(&self) -> Option<String> {
        self.join_token.lock().expect("k3s link").clone()
    }

    pub fn set_running(&self, running: bool) {
        self.running.store(running, Ordering::Relaxed);
    }

    pub fn running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn set_net_applied(&self, applied: bool) {
        self.net_applied.store(applied, Ordering::Relaxed);
    }

    pub fn net_applied(&self) -> bool {
        self.net_applied.load(Ordering::Relaxed)
    }
}

/// The site the worker installs into.
#[derive(Debug, Clone)]
pub struct Site {
    /// The site's HTTP proxy (`http://host:port`), if K3s and friends are
    /// fetched through one (the lab relay); `None` goes direct.
    pub proxy: Option<String>,
    /// The homelab VIP: the K3s API URL everywhere.
    pub vip: Ipv4Addr,
    /// The management subnet (the node's address lives in it).
    pub mgmt: ipnet::Ipv4Net,
}

/// This host's K3s mode, chosen once and kept in
/// `/var/lib/homelab/k3s_mode` (an agent promoted to voter switches once).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    ServerInit,
    ServerJoin,
    Agent,
}

impl Mode {
    fn subcommand(self) -> &'static str {
        match self {
            Mode::ServerInit | Mode::ServerJoin => "server",
            Mode::Agent => "agent",
        }
    }
}

/// Everything the worker needs from the machine, in one place for tests.
#[derive(Debug, Clone)]
pub struct K3sPaths {
    pub state: PathBuf,
    pub run: PathBuf,
    pub bin: PathBuf,
    pub env: PathBuf,
    pub unit: PathBuf,
    pub config: PathBuf,
    pub token: PathBuf,
    pub kubeconfig: PathBuf,
}

impl K3sPaths {
    pub fn system() -> Self {
        Self {
            state: PathBuf::from("/var/lib/homelab"),
            run: PathBuf::from("/run/homelab"),
            bin: PathBuf::from(K3S_BIN),
            env: PathBuf::from(K3S_ENV),
            unit: PathBuf::from(K3S_UNIT),
            config: PathBuf::from(K3S_CONFIG),
            token: PathBuf::from(K3S_TOKEN),
            kubeconfig: PathBuf::from(K3S_KUBECONFIG),
        }
    }

    /// The observed K3s/Longhorn snapshot the leader serves to clients.
    pub fn snapshot_file(&self) -> PathBuf {
        self.run.join("k3s_state.json")
    }

    fn mode_file(&self) -> PathBuf {
        self.state.join("k3s_mode")
    }

    fn prereqs_done(&self) -> PathBuf {
        self.state.join("k3s_prereqs")
    }

    fn longhorn_done(&self) -> PathBuf {
        self.state.join("longhorn_done")
    }
}

/// The proxy variables for K3s, containerd and kubectl: every site subnet
/// plus the pod/service CIDRs bypass the proxy. Empty without a proxy.
pub fn render_env(site: &Site) -> String {
    let Some(proxy) = &site.proxy else { return String::new() };
    let mut no_proxy = vec!["localhost".to_string(), "127.0.0.1".to_string(), POD_CIDR.into(), SVC_CIDR.into()];
    no_proxy.extend(model::seed_networks().iter().map(|n| n.subnet.to_string()));
    no_proxy.push(site.mgmt.to_string());
    no_proxy.sort();
    no_proxy.dedup();
    let no_proxy = no_proxy.join(",");
    format!(
        "HTTP_PROXY={proxy}\nHTTPS_PROXY={proxy}\nhttp_proxy={proxy}\nhttps_proxy={proxy}\nNO_PROXY={no_proxy}\nno_proxy={no_proxy}\n"
    )
}

/// `/etc/rancher/k3s/config.yaml` for `mode`. The token rides in the file
/// (600); the CLI carries only the `server`/`agent` subcommand, so nothing
/// is ever passed twice (kubelet rejects a doubled `--node-ip`).
pub fn render_config(mode: Mode, site: &Site, node_ip: Ipv4Addr, token: Option<&str>) -> String {
    let mut c = String::new();
    match mode {
        Mode::ServerInit => c.push_str("cluster-init: true\n"),
        Mode::ServerJoin | Mode::Agent => {
            c.push_str(&format!("server: https://{}:{K3S_API_PORT}\n", site.vip));
            if let Some(t) = token {
                c.push_str(&format!("token: {t}\n"));
            }
        }
    }
    c.push_str(&format!("node-ip: {node_ip}\n"));
    if mode != Mode::Agent {
        c.push_str(&format!("tls-san:\n  - {}\n", site.vip));
        c.push_str("disable:\n  - traefik\n");
    }
    c
}

/// The `k3s.service` unit (the stock one, minus the install script's
/// argument baking: our config file carries them).
pub fn render_unit(mode: Mode) -> String {
    format!(
        "[Unit]\nDescription=Lightweight Kubernetes\nDocumentation=https://k3s.io\nWants=network-online.target\nAfter=network-online.target\n\n[Install]\nWantedBy=multi-user.target\n\n[Service]\nType=notify\nEnvironmentFile=-{K3S_ENV}\nKillMode=process\nDelegate=yes\nLimitNOFILE=1048576\nLimitNPROC=infinity\nLimitCORE=infinity\nTasksMax=infinity\nTimeoutStartSec=0\nRestart=always\nRestartSec=5s\nExecStartPre=-/sbin/modprobe br_netfilter\nExecStartPre=-/sbin/modprobe overlay\nExecStart={K3S_BIN} {}\n",
        mode.subcommand()
    )
}

fn sh(cmd: &str, args: &[&str]) -> io::Result<String> {
    sh_env(cmd, args, &[])
}

/// Runs `cmd` with `env` (`KEY=value`) added, capturing stdout; a failure
/// carries stderr.
fn sh_env(cmd: &str, args: &[&str], env: &[(&str, &str)]) -> io::Result<String> {
    let out = std::process::Command::new(cmd).args(args).envs(env.iter().copied()).output()?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(io::Error::other(format!("{cmd} {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim())))
    }
}

/// This host's address in `mgmt`, if it has one yet (its network may not be
/// applied). Reads `ip -j -4 addr` like the VIP holder does.
fn node_ip(mgmt: &ipnet::Ipv4Net) -> Option<Ipv4Addr> {
    let out = sh("ip", &["-j", "-4", "addr"]).ok()?;
    let links: serde_json::Value = serde_json::from_str(&out).ok()?;
    links.as_array()?.iter().flat_map(|l| l["addr_info"].as_array().into_iter().flatten()).find_map(|a| {
        let ip: Ipv4Addr = a["local"].as_str()?.parse().ok()?;
        mgmt.contains(&ip).then_some(ip)
    })
}

/// The voters our control plane knows (from this host's Raft state, if it
/// holds any): the hosts that must become K3s servers.
fn voters(state: &Path) -> BTreeSet<model::HostId> {
    RaftStore::new(state.join("raft")).load().ok().flatten().map(|d| d.voters).unwrap_or_default()
}

/// Whether this host holds our control-plane state (it is, or is becoming,
/// a voter): `homelabd bootstrap` and `spawn_cp_replica` both write it.
fn is_voter(state: &Path) -> bool {
    !voters(state).is_empty()
}

/// The names of `Ready` K3s nodes, and which are servers, from `k3s kubectl`.
fn ready_nodes(paths: &K3sPaths, site: &Site) -> io::Result<Vec<(String, bool)>> {
    let env = kubectl_env(site, paths);
    let env = env_ref(&env);
    let out = sh_env(paths.bin.to_str().unwrap_or(K3S_BIN), &["kubectl", "get", "nodes", "--no-headers"], &env)?;
    Ok(out
        .lines()
        .filter_map(|l| {
            let mut w = l.split_whitespace();
            let (name, status, roles) = (w.next()?, w.next()?, w.next()?);
            (status == "Ready").then(|| (name.to_string(), roles.contains("control-plane")))
        })
        .collect())
}

fn kubectl_env(site: &Site, paths: &K3sPaths) -> Vec<(String, String)> {
    let mut env = vec![("KUBECONFIG".to_string(), paths.kubeconfig.to_string_lossy().into_owned())];
    if site.proxy.is_some() {
        env.extend(render_env(site).lines().filter_map(|l| l.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))));
    }
    env
}

fn env_ref(env: &[(String, String)]) -> Vec<(&str, &str)> {
    env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

/// The string at `v[k]`, or `""` (kubectl shapes drift; missing means empty).
fn s<'a>(v: &'a serde_json::Value, k: &str) -> &'a str {
    v.get(k).and_then(serde_json::Value::as_str).unwrap_or("")
}

/// The u64 at `v[k]`, read as a number or a string (Longhorn sizes are strings).
fn n(v: &serde_json::Value, k: &str) -> u64 {
    v.get(k)
        .map(|x| x.as_u64().or_else(|| x.as_str()?.parse().ok()).unwrap_or(0))
        .unwrap_or(0)
}

fn items(doc: &str) -> Vec<serde_json::Value> {
    serde_json::from_str::<serde_json::Value>(doc)
        .ok()
        .and_then(|d| d.get("items").cloned())
        .and_then(|i| serde_json::from_value::<Vec<serde_json::Value>>(i).ok())
        .unwrap_or_default()
}

fn parse_nodes(doc: &str) -> Vec<K8sNode> {
    items(doc)
        .iter()
        .map(|i| {
            let md = &i["metadata"];
            let labels = md.get("labels").and_then(serde_json::Value::as_object);
            let mut roles: Vec<String> = labels
                .into_iter()
                .flat_map(|l| l.keys())
                .filter_map(|k| k.strip_prefix("node-role.kubernetes.io/").map(str::to_string))
                .collect();
            roles.sort();
            let ready = i["status"]["conditions"]
                .as_array()
                .is_some_and(|cs| cs.iter().any(|c| s(c, "type") == "Ready" && s(c, "status") == "True"));
            K8sNode { name: s(md, "name").to_string(), ready, roles, version: s(&i["status"]["nodeInfo"], "kubeletVersion").to_string() }
        })
        .collect()
}

fn parse_deployments(doc: &str) -> Vec<K8sDeployment> {
    items(doc)
        .iter()
        .map(|i| K8sDeployment {
            namespace: s(&i["metadata"], "namespace").to_string(),
            name: s(&i["metadata"], "name").to_string(),
            ready: n(&i["status"], "readyReplicas") as u32,
            total: n(&i["status"], "replicas") as u32,
        })
        .collect()
}

fn parse_pods(doc: &str) -> Vec<K8sPod> {
    items(doc)
        .iter()
        .map(|i| K8sPod {
            namespace: s(&i["metadata"], "namespace").to_string(),
            name: s(&i["metadata"], "name").to_string(),
            node: s(&i["spec"], "nodeName").to_string(),
            phase: s(&i["status"], "phase").to_string(),
        })
        .collect()
}

/// Volume name → `namespace/name` of the claiming PVC.
fn parse_pvcs(doc: &str) -> BTreeMap<String, String> {
    items(doc)
        .iter()
        .filter_map(|i| {
            let vol = s(&i["spec"], "volumeName");
            (!vol.is_empty()).then(|| (vol.to_string(), format!("{}/{}", s(&i["metadata"], "namespace"), s(&i["metadata"], "name"))))
        })
        .collect()
}

fn parse_lh_nodes(doc: &str) -> Vec<LhNode> {
    items(doc)
        .iter()
        .map(|i| {
            let disks = i["spec"]["disks"].as_object().map(|spec| {
                spec.iter()
                    .map(|(uuid, d)| {
                        let st = &i["status"]["diskStatus"][uuid];
                        LhDisk {
                            path: s(d, "path").to_string(),
                            total_bytes: n(st, "storageMaximum"),
                            avail_bytes: n(st, "storageAvailable"),
                            replicas: st.get("scheduledReplica").and_then(serde_json::Value::as_object).map(|m| m.len() as u64).unwrap_or(0),
                        }
                    })
                    .collect()
            });
            // A node whose spec has not arrived yet still reports status.
            let disks = disks.or_else(|| {
                i["status"]["diskStatus"].as_object().map(|st| {
                    st.iter()
                        .map(|(uuid, d)| LhDisk {
                            path: uuid.clone(),
                            total_bytes: n(d, "storageMaximum"),
                            avail_bytes: n(d, "storageAvailable"),
                            replicas: d.get("scheduledReplica").and_then(serde_json::Value::as_object).map(|m| m.len() as u64).unwrap_or(0),
                        })
                        .collect()
                })
            });
            LhNode {
                name: s(&i["metadata"], "name").to_string(),
                schedulable: i["spec"].get("allowScheduling").and_then(serde_json::Value::as_bool).unwrap_or(false),
                disks: disks.unwrap_or_default(),
            }
        })
        .collect()
}

fn parse_volumes(doc: &str, pvcs: &BTreeMap<String, String>, by_volume: &BTreeMap<String, Vec<LhReplica>>) -> Vec<LhVolume> {
    items(doc)
        .iter()
        .map(|i| {
            let name = s(&i["metadata"], "name").to_string();
            LhVolume {
                name: name.clone(),
                pvc: pvcs.get(&name).cloned(),
                size_bytes: n(&i["spec"], "size"),
                robustness: s(&i["status"], "robustness").to_string(),
                state: s(&i["status"], "state").to_string(),
                replicas: by_volume.get(&name).cloned().unwrap_or_default(),
            }
        })
        .collect()
}

/// Groups parsed replicas by volume name.
fn group_replicas(parsed: Vec<(String, LhReplica)>) -> BTreeMap<String, Vec<LhReplica>> {
    let mut by_volume: BTreeMap<String, Vec<LhReplica>> = BTreeMap::new();
    for (vol, r) in parsed {
        by_volume.entry(vol).or_default().push(r);
    }
    for rs in by_volume.values_mut() {
        rs.sort_by(|a, b| a.node.cmp(&b.node));
    }
    by_volume
}

/// (volume, placement) per replica object.
fn parse_replicas(doc: &str) -> Vec<(String, LhReplica)> {
    items(doc)
        .iter()
        .map(|i| {
            let vol = s(&i["spec"], "volumeName");
            let vol = if vol.is_empty() { s(&i["metadata"]["labels"], "longhornvolume") } else { vol };
            (
                vol.to_string(),
                LhReplica {
                    node: s(&i["spec"], "nodeID").to_string(),
                    running: s(&i["status"], "currentState") == "running",
                },
            )
        })
        .collect()
}

pub struct K3sWorker {
    site: Site,
    paths: K3sPaths,
    link: std::sync::Arc<K3sLink>,
    started: Instant,
    snapped: Option<Instant>,
}

impl K3sWorker {
    pub fn new(site: Site, paths: K3sPaths, link: std::sync::Arc<K3sLink>) -> Self {
        Self { site, paths, link, started: Instant::now(), snapped: None }
    }

    /// Converges forever: prerequisites, binary, mode, running K3s, and —
    /// on the cluster-init host — Longhorn. Never panics: every failure is
    /// logged and retried on the next poll.
    pub fn run(mut self) {
        loop {
            if let Err(e) = self.converge() {
                eprintln!("homelabd: k3s: {e} (retrying)");
            }
            std::thread::sleep(POLL);
        }
    }

    fn converge(&mut self) -> io::Result<()> {
        if !self.paths.state.join("joined").exists() {
            return Ok(()); // not a member yet (or never will be)
        }
        let Some(ip) = node_ip(&self.site.mgmt) else {
            return Ok(()); // its network is not applied yet
        };
        self.ensure_prereqs()?;
        self.ensure_installed()?;
        if !self.link.net_applied() {
            return Ok(()); // still on bootstrap DHCP or a trial config
        }
        let mode = self.mode()?;
        let Some(mode) = mode else {
            return Ok(()); // waiting out the agent settle (a voter-to-be)
        };
        let token = self.link.join_token();
        if matches!(mode, Mode::ServerJoin | Mode::Agent) && token.is_none() {
            return Ok(()); // the agent asks the VIP every 30 s until then
        }
        self.ensure_started(mode, ip, token.as_deref())?;
        let running = self.active();
        self.link.set_running(running);
        if running && self.paths.state.join("cluster_init").exists() {
            self.ensure_longhorn()?;
        }
        if running && matches!(mode, Mode::ServerInit | Mode::ServerJoin) && self.snapped.is_none_or(|at| at.elapsed() >= SNAP_EVERY) {
            // A failed snapshot keeps the previous file; the next poll retries.
            match self.write_snapshot() {
                Ok(()) => self.snapped = Some(Instant::now()),
                Err(e) => eprintln!("homelabd: k3s snapshot: {e}"),
            }
        }
        Ok(())
    }

    /// Queries the K3s API (nodes, deployments, pods, PVCs, Longhorn nodes,
    /// volumes, replicas) and writes the observed snapshot for the leader.
    fn write_snapshot(&self) -> io::Result<()> {
        let bin = self.paths.bin.to_string_lossy().into_owned();
        let env = kubectl_env(&self.site, &self.paths);
        let env = env_ref(&env);
        let q = |args: &[&str]| sh_env(&bin, args, &env);
        let nodes = parse_nodes(&q(&["kubectl", "get", "nodes", "-o", "json"])?);
        let deployments = parse_deployments(&q(&["kubectl", "get", "deployments", "-A", "-o", "json"])?);
        let pods = parse_pods(&q(&["kubectl", "get", "pods", "-A", "-o", "json"])?);
        let pvcs = parse_pvcs(&q(&["kubectl", "get", "pvc", "-A", "-o", "json"])?);
        let lh_nodes = parse_lh_nodes(&q(&["kubectl", "-n", "longhorn-system", "get", "nodes.longhorn.io", "-o", "json"])?);
        let by_volume = group_replicas(parse_replicas(&q(&["kubectl", "-n", "longhorn-system", "get", "replicas.longhorn.io", "-o", "json"])?));
        let volumes = parse_volumes(&q(&["kubectl", "-n", "longhorn-system", "get", "volumes.longhorn.io", "-o", "json"])?, &pvcs, &by_volume);
        let snap = K8sSnapshot { at: crate::runtime::wall_ms(), nodes, deployments, pods, lh_nodes, volumes };
        let file = self.paths.snapshot_file();
        if let Some(dir) = file.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&file, serde_json::to_vec(&snap).map_err(io::Error::other)?)?;
        Ok(())
    }

    /// The mode this host converges to, or `None` while it waits to be sure
    /// it is not about to become a voter. A running agent that became a
    /// voter switches to joining as a server (once).
    fn mode(&self) -> io::Result<Option<Mode>> {
        if let Ok(s) = std::fs::read_to_string(self.paths.mode_file()) {
            let mode = match s.trim() {
                "server-init" => Mode::ServerInit,
                "server-join" => Mode::ServerJoin,
                _ => Mode::Agent,
            };
            if mode == Mode::Agent && is_voter(&self.paths.state) {
                eprintln!("homelabd: k3s: promoted to voter; rejoining as a server");
                let _ = sh("systemctl", &["stop", "k3s"]);
                for d in ["server", "agent"] {
                    let _ = std::fs::remove_dir_all(format!("/var/lib/rancher/k3s/{d}"));
                }
                std::fs::write(self.paths.mode_file(), "server-join")?;
                return Ok(Some(Mode::ServerJoin));
            }
            return Ok(Some(mode));
        }
        if self.paths.state.join("cluster_init").exists() {
            std::fs::write(self.paths.mode_file(), "server-init")?;
            return Ok(Some(Mode::ServerInit));
        }
        if is_voter(&self.paths.state) {
            std::fs::write(self.paths.mode_file(), "server-join")?;
            return Ok(Some(Mode::ServerJoin));
        }
        if self.started.elapsed() < AGENT_SETTLE {
            return Ok(None);
        }
        std::fs::write(self.paths.mode_file(), "agent")?;
        Ok(Some(Mode::Agent))
    }

    /// Longhorn's node prerequisites (idempotent; once per install).
    fn ensure_prereqs(&self) -> io::Result<()> {
        if self.paths.prereqs_done().exists() {
            return Ok(());
        }
        sh("apt-get", &["update", "-qq"])?;
        sh_env("apt-get", &["install", "-y", "-qq", "open-iscsi", "nfs-common"], &[("DEBIAN_FRONTEND", "noninteractive")])?;
        sh("systemctl", &["enable", "--now", "iscsid"])?;
        std::fs::write(self.paths.prereqs_done(), LONGHORN_VERSION)?;
        Ok(())
    }

    /// The pinned K3s binary, verified by sha256.
    fn ensure_installed(&self) -> io::Result<()> {
        if let Ok(out) = sh(self.paths.bin.to_str().unwrap_or(K3S_BIN), &["--version"])
            && out.contains(K3S_VERSION)
        {
            return Ok(());
        }
        let tmp = self.paths.state.join("k3s.new");
        let mut env: Vec<(&str, &str)> = vec![];
        let proxy;
        if let Some(p) = &self.site.proxy {
            proxy = p.clone();
            env = vec![("https_proxy", &proxy), ("http_proxy", &proxy)];
        }
        sh_env("curl", &["-fsSL", "--max-time", "600", "-o", &tmp.to_string_lossy(), K3S_URL], &env)?;
        let sum = sh("sha256sum", &[&tmp.to_string_lossy()])?;
        if sum.split_whitespace().next() != Some(K3S_SHA256) {
            let _ = std::fs::remove_file(&tmp);
            return Err(io::Error::other("k3s binary failed its sha256 check"));
        }
        sh("chmod", &["+x", &tmp.to_string_lossy()])?;
        std::fs::rename(&tmp, &self.paths.bin)?;
        Ok(())
    }

    /// The config, env and unit in place and the service started (a start
    /// already done is a no-op; systemd restarts a failed one itself).
    fn ensure_started(&self, mode: Mode, ip: Ipv4Addr, token: Option<&str>) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        if let Some(dir) = self.paths.config.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let config = render_config(mode, &self.site, ip, token);
        let write_config = std::fs::read_to_string(&self.paths.config).ok() != Some(config.clone());
        if write_config {
            std::fs::write(&self.paths.config, config)?;
            std::fs::set_permissions(&self.paths.config, std::fs::Permissions::from_mode(0o600))?;
        }
        let env = render_env(&self.site);
        if !env.is_empty() && std::fs::read_to_string(&self.paths.env).ok() != Some(env.clone()) {
            std::fs::write(&self.paths.env, env)?;
        }
        let unit = render_unit(mode);
        if std::fs::read_to_string(&self.paths.unit).ok() != Some(unit.clone()) {
            std::fs::write(&self.paths.unit, unit)?;
            sh("systemctl", &["daemon-reload"])?;
        }
        if write_config && self.active() {
            sh("systemctl", &["restart", "k3s"])?;
        } else if !self.active() {
            sh("systemctl", &["enable", "--now", "k3s"])?;
        }
        Ok(())
    }

    fn active(&self) -> bool {
        sh("systemctl", &["is-active", "--quiet", "k3s"]).is_ok()
    }

    /// Applies the pinned Longhorn manifest once every voter is a Ready K3s
    /// server (or the wait runs out), then records it done.
    fn ensure_longhorn(&self) -> io::Result<()> {
        if self.paths.longhorn_done().exists() {
            return Ok(());
        }
        let expect: BTreeSet<String> = voters(&self.paths.state).into_iter().map(|h| h.to_string()).collect();
        let ready: BTreeSet<String> = ready_nodes(&self.paths, &self.site)
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, server)| *server)
            .map(|(n, _)| n)
            .collect();
        // Hostnames are the lowercased host ids; compare loosely.
        let ready_lower: BTreeSet<String> = ready.into_iter().map(|n| n.to_lowercase()).collect();
        let missing = expect.iter().filter(|h| !ready_lower.contains(&h.to_lowercase())).count();
        if missing > 0 && self.started.elapsed() < LONGHORN_WAIT {
            return Ok(());
        }
        if missing > 0 {
            eprintln!("homelabd: k3s: {missing} voter(s) not Ready; installing Longhorn anyway");
        }
        let manifest = self.paths.state.join("longhorn.yaml");
        let mut env: Vec<(&str, &str)> = vec![];
        let proxy;
        if let Some(p) = &self.site.proxy {
            proxy = p.clone();
            env = vec![("https_proxy", &proxy), ("http_proxy", &proxy)];
        }
        sh_env("curl", &["-fsSL", "--max-time", "300", "-o", &manifest.to_string_lossy(), LONGHORN_URL], &env)?;
        let bin = self.paths.bin.to_string_lossy().into_owned();
        let env = kubectl_env(&self.site, &self.paths);
        let env = env_ref(&env);
        sh_env(&bin, &["kubectl", "apply", "-f", &manifest.to_string_lossy()], &env)?;
        std::fs::write(self.paths.longhorn_done(), LONGHORN_VERSION)?;
        eprintln!("homelabd: k3s: Longhorn {LONGHORN_VERSION} applied");
        Ok(())
    }
}

/// Spawns the worker on its own thread (it never returns).
pub fn spawn_worker(site: Site, paths: K3sPaths, link: std::sync::Arc<K3sLink>) {
    std::thread::Builder::new()
        .name("k3s-worker".into())
        .spawn(move || K3sWorker::new(site, paths, link).run())
        .expect("a k3s worker thread");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn site() -> Site {
        Site { proxy: Some("http://10.0.0.1:3130".into()), vip: Ipv4Addr::new(10, 0, 3, 253), mgmt: "10.0.0.0/22".parse().unwrap() }
    }

    #[test]
    fn env_proxy_covers_every_subnet_and_nothing_without_a_proxy() {
        let env = render_env(&site());
        assert!(env.contains("HTTP_PROXY=http://10.0.0.1:3130\n"), "{env}");
        assert!(env.contains("http_proxy=http://10.0.0.1:3130\n"), "{env}");
        for cidr in ["10.0.0.0/22", "10.20.0.0/22", "10.30.0.0/22", "10.100.0.0/22", "10.110.0.0/24", "10.42.0.0/16", "10.43.0.0/16", "localhost"] {
            assert!(env.contains(cidr), "{cidr} bypasses the proxy:\n{env}");
        }
        let direct = Site { proxy: None, ..site() };
        assert_eq!(render_env(&direct), "");
    }

    #[test]
    fn configs_carry_the_mode_once_and_pin_vip_and_node_ip() {
        let (vip, ip) = (site().vip, Ipv4Addr::new(10, 0, 0, 6));
        let init = render_config(Mode::ServerInit, &site(), ip, None);
        assert!(init.contains("cluster-init: true\n"), "{init}");
        assert!(!init.contains("token:"), "{init}");
        assert!(init.contains("node-ip: 10.0.0.6\n"), "{init}");
        assert!(init.contains("- 10.0.3.253\n"), "{init}");
        let join = render_config(Mode::ServerJoin, &site(), ip, Some("tok"));
        assert!(join.contains("server: https://10.0.3.253:6443\n"), "{join}");
        assert!(join.contains("token: tok\n"), "{join}");
        assert!(!join.contains("cluster-init"), "{join}");
        let agent = render_config(Mode::Agent, &site(), ip, Some("tok"));
        assert!(agent.contains("server: https://10.0.3.253:6443\n"), "{agent}");
        assert!(!agent.contains("tls-san"), "{agent}");
    }

    #[test]
    fn unit_runs_the_subcommand_with_the_env_file() {
        let server = render_unit(Mode::ServerInit);
        assert!(server.contains("ExecStart=/usr/local/bin/k3s server\n"), "{server}");
        assert!(server.contains("EnvironmentFile=-/etc/systemd/system/k3s.service.env\n"), "{server}");
        assert!(server.contains("Type=notify\n"), "{server}");
        assert!(render_unit(Mode::Agent).contains("ExecStart=/usr/local/bin/k3s agent\n"));
    }

    #[test]
    fn nodes_parse_roles_and_readiness() {
        let doc = r#"{"items": [
            {"metadata": {"name": "ms01-01", "labels": {"node-role.kubernetes.io/control-plane": "true", "node-role.kubernetes.io/etcd": "true", "kubernetes.io/hostname": "ms01-01"}},
             "status": {"conditions": [{"type": "Ready", "status": "True"}], "nodeInfo": {"kubeletVersion": "v1.36.5+k3s1"}}},
            {"metadata": {"name": "ms01-02"}, "status": {"conditions": [{"type": "Ready", "status": "False"}]}}
        ]}"#;
        let nodes = parse_nodes(doc);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].roles, ["control-plane", "etcd"]);
        assert!(nodes[0].ready);
        assert_eq!(nodes[0].version, "v1.36.5+k3s1");
        assert!(nodes[1].roles.is_empty() && !nodes[1].ready);
        assert!(parse_nodes("not json").is_empty());
    }

    #[test]
    fn pods_and_deployments_parse() {
        let pods = parse_pods(r#"{"items": [{"metadata": {"namespace": "default", "name": "w"}, "spec": {"nodeName": "ms01-01"}, "status": {"phase": "Running"}}]}"#);
        assert_eq!(pods, [K8sPod { namespace: "default".into(), name: "w".into(), node: "ms01-01".into(), phase: "Running".into() }]);
        let ds = parse_deployments(r#"{"items": [{"metadata": {"namespace": "default", "name": "web"}, "status": {"readyReplicas": 2, "replicas": 3}}]}"#);
        assert_eq!(ds.len(), 1);
        assert_eq!((ds[0].ready, ds[0].total), (2, 3));
    }

    #[test]
    fn longhorn_parse_joins_disks_volumes_pvcs_and_replicas() {
        let nodes = parse_lh_nodes(r#"{"items": [{"metadata": {"name": "ms01-01"},
            "spec": {"allowScheduling": true, "disks": {"uuid-1": {"path": "/var/lib/longhorn/"}}},
            "status": {"diskStatus": {"uuid-1": {"storageMaximum": 100, "storageAvailable": 40, "scheduledReplica": {"r-a": 1, "r-b": 1}}}}}]}"#);

        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].schedulable);
        assert_eq!(nodes[0].disks.len(), 1);
        assert_eq!(nodes[0].disks[0].path, "/var/lib/longhorn/");
        assert_eq!((nodes[0].disks[0].total_bytes, nodes[0].disks[0].avail_bytes, nodes[0].disks[0].replicas), (100, 40, 2));
        let pvcs = parse_pvcs(r#"{"items": [{"metadata": {"namespace": "default", "name": "data"}, "spec": {"volumeName": "pvc-1"}}]}"#);
        assert_eq!(pvcs.get("pvc-1").map(String::as_str), Some("default/data"));
        let by_volume = group_replicas(parse_replicas(r#"{"items": [
            {"metadata": {"name": "pvc-1-r-a"}, "spec": {"volumeName": "pvc-1", "nodeID": "ms01-02"}, "status": {"currentState": "running"}},
            {"metadata": {"name": "pvc-1-r-b", "labels": {"longhornvolume": "pvc-1"}}, "spec": {"volumeName": "", "nodeID": "ms01-01"}, "status": {"currentState": "stopped"}}
        ]}"#));
        let vols = parse_volumes(
            r#"{"items": [{"metadata": {"name": "pvc-1"}, "spec": {"size": "1073741824"}, "status": {"robustness": "degraded", "state": "attached"}}]}"#,
            &pvcs,
            &by_volume,
        );
        assert_eq!(vols.len(), 1);
        assert_eq!(vols[0].pvc.as_deref(), Some("default/data"));
        assert_eq!(vols[0].size_bytes, 1073741824);
        assert_eq!(vols[0].replicas.len(), 2);
        assert_eq!(vols[0].replicas[0].node, "ms01-01");
        assert!(vols[0].replicas.iter().any(|r| r.running));
    }

    #[test]
    fn mode_choice_prefers_init_then_voter_then_settled_agent() {
        let scratch = std::env::temp_dir().join(format!("k3s-mode-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir_all(&scratch).unwrap();
        let paths = K3sPaths {
            state: scratch.clone(),
            run: scratch.join("run"),
            bin: scratch.join("k3s"),
            env: scratch.join("env"),
            unit: scratch.join("unit"),
            config: scratch.join("config"),
            token: scratch.join("token"),
            kubeconfig: scratch.join("kubeconfig"),
        };
        let worker = K3sWorker::new(site(), paths.clone(), Default::default());
        // Not yet settled, no state: waits rather than risk joining as an
        // agent on a host that is about to become a voter.
        assert_eq!(worker.mode().unwrap(), None);
        std::fs::write(scratch.join("cluster_init"), "").unwrap();
        assert_eq!(worker.mode().unwrap(), Some(Mode::ServerInit));
        assert_eq!(std::fs::read_to_string(paths.mode_file()).unwrap(), "server-init");
        let _ = std::fs::remove_dir_all(&scratch);
    }
}
