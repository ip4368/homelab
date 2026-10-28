//! The host's Kubernetes: K3s, installed and kept in line with the
//! [`KubeSpec`] the leader sends, and Longhorn on the servers. Installing
//! takes minutes (packages, the K3s binary, images), so a worker thread does
//! it all; the event loop only hands it the latest spec and reads its status.
//!
//! What the worker does, every [`EVERY`] or as soon as a new spec arrives:
//!
//! 1. Installs K3s as the spec's role if it is not, with the official
//!    install script, after the packages Longhorn needs. The script (that of
//!    [`K3S_VERSION`]'s release) and the K3s binary are checked against
//!    pinned sha256 sums before anything runs. A host installed as the other role is
//!    uninstalled first; a server deletes its own node before it goes, so
//!    its etcd member leaves the cluster.
//! 2. Starts the service if it stopped, and waits for the node to register.
//! 3. On a server, writes the Multus `HelmChart` manifest, waits for its
//!    DaemonSet, and applies the storage `NetworkAttachmentDefinition`
//!    (a macvlan on `storage0`, VLAN 20); then writes the Longhorn
//!    `HelmChart` manifest (replicating over that network) and, on
//!    upgrades, patches a live `storage-network` setting to match.
//! 4. On a server, deletes the nodes in `spec.etcd_remove` (dead, replaced
//!    servers), which K3s turns into etcd member removals. Never its own
//!    node, never a Ready one.
//! 5. Labels the node, and on a storage host gives Longhorn its data disks
//!    (`node.longhorn.io/default-disks-config`, then the label that tells
//!    Longhorn to use it). A host whose storage role is held gives none.
//!
//! It writes what it found to `/run/homelab/kube.json` for `homelabd status`.

use super::{sh, LinuxVip, Paths};
use control::proto::{KubePhase, KubeRole, KubeSpec, KubeStatus};
use model::HostId;
use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub const K3S_VERSION: &str = "v1.36.5+k3s1";
/// The sha256 of [`K3S_VERSION`]'s amd64 `k3s` binary (the release's
/// `sha256sum-amd64.txt`).
const K3S_SHA256: &str = "d73847bcd3c5fccef0115b372e2f9a91f3032dc84bbf71518a4617565294d313";
/// The sha256 of `install.sh` as tagged [`K3S_VERSION`]. `get.k3s.io` serves
/// whatever the script is today, so it is fetched from the tag instead.
const INSTALL_SCRIPT_SHA256: &str = "46177d4c99440b4c0311b67233823a8e8a2fc09693f6c89af1a7161e152fbfad";
const K3S_BIN: &str = "/usr/local/bin/k3s";
pub const LONGHORN_VERSION: &str = "1.13.0";
/// The `rke2-multus` chart (Multus + Whereabouts) for K3s's CNI paths.
pub const MULTUS_VERSION: &str = "v4.2.410";
/// Longhorn's storage network: pods' second interfaces on VLAN 20.
pub const STORAGE_NAD_NAMESPACE: &str = "longhorn-system";
pub const STORAGE_NAD_NAME: &str = "storage-net";
/// The `storage-network` setting's value: `<namespace>/<name>`.
pub const STORAGE_NETWORK: &str = "longhorn-system/storage-net";
/// Whereabouts range for those interfaces: the last /24 of `storage`
/// (kept out of host IPAM).
pub const STORAGE_POD_RANGE: &str = "10.20.3.0/24";
/// The admin kubeconfig a K3s server writes.
pub const KUBECONFIG: &str = "/etc/rancher/k3s/k3s.yaml";
/// What an agent's kubelet authenticates with: enough to label its own node.
const KUBELET_KUBECONFIG: &str = "/var/lib/rancher/k3s/agent/kubelet.kubeconfig";
const MANIFEST: &str = "/var/lib/rancher/k3s/server/manifests/homelab-longhorn.yaml";
const MULTUS_MANIFEST: &str = "/var/lib/rancher/k3s/server/manifests/homelab-multus.yaml";
/// How often the worker looks again when nothing changed.
const EVERY: Duration = Duration::from_secs(10);
/// How long it waits after a failed step before trying again.
const BACKOFF: Duration = Duration::from_secs(20);
const LONGHORN_DISK_LABEL: &str = "node.longhorn.io/create-default-disk";
const LONGHORN_DISK_ANNOTATION: &str = "node.longhorn.io/default-disks-config";

pub fn status_path(paths: &Paths) -> PathBuf {
    paths.run.join("kube.json")
}

/// The admin kubeconfig this host's K3s server wrote, pointed at `vip`
/// instead of the server itself (only K3s servers have one).
pub fn admin_kubeconfig(vip: Ipv4Addr) -> std::io::Result<String> {
    Ok(at_vip(&std::fs::read_to_string(KUBECONFIG)?, vip))
}

fn at_vip(kubeconfig: &str, vip: Ipv4Addr) -> String {
    kubeconfig.replace("https://127.0.0.1:6443", &format!("https://{vip}:6443"))
}

/// The systemd unit K3s runs as in `role`.
fn unit(role: KubeRole) -> &'static str {
    match role {
        KubeRole::Server => "k3s",
        KubeRole::Agent => "k3s-agent",
    }
}

/// Multus (with the Whereabouts IPAM) for K3s's CNI paths, as a manifest
/// K3s's helm controller installs. Longhorn's storage network rides on it.
pub fn multus_manifest() -> String {
    format!(
        "# Written by homelabd on every K3s server: Multus + Whereabouts for the
# storage network (VLAN 20).
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: multus
  namespace: kube-system
spec:
  repo: https://rke2-charts.rancher.io
  chart: rke2-multus
  version: {MULTUS_VERSION}
  targetNamespace: kube-system
  valuesContent: |-
    config:
      fullnameOverride: multus
      cni_conf:
        confDir: /var/lib/rancher/k3s/agent/etc/cni/net.d
        binDir: /var/lib/rancher/k3s/data/cni/
        kubeconfig: /var/lib/rancher/k3s/agent/etc/cni/net.d/multus.d/multus.kubeconfig
        multusAutoconfigDir: /var/lib/rancher/k3s/agent/etc/cni/net.d
    rke2-whereabouts:
      fullnameOverride: whereabouts
      enabled: true
      cniConf:
        confDir: /var/lib/rancher/k3s/agent/etc/cni/net.d
        binDir: /var/lib/rancher/k3s/data/cni/
"
    )
}

/// The storage network attachment: a macvlan on every storage host's VLAN
/// 20 interface (`storage0`), with pod addresses from [`STORAGE_POD_RANGE`].
/// Applied with `kubectl` once Multus is ready (the CRD comes with it).
pub fn storage_nad_manifest() -> String {
    format!(
        "# Written by homelabd on every K3s server: Longhorn's storage network.
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: {STORAGE_NAD_NAME}
  namespace: {STORAGE_NAD_NAMESPACE}
spec:
  config: |-
    {{
      \"cniVersion\": \"0.3.1\",
      \"name\": \"{STORAGE_NAD_NAME}\",
      \"type\": \"macvlan\",
      \"master\": \"storage0\",
      \"mode\": \"bridge\",
      \"mtu\": 9000,
      \"ipam\": {{
        \"type\": \"whereabouts\",
        \"range\": \"{STORAGE_POD_RANGE}\",
        \"configuration_path\": \"/var/lib/rancher/k3s/agent/etc/cni/net.d/whereabouts.d/whereabouts.conf\"
      }}
    }}
"
    )
}

/// The Longhorn deployment, as a manifest K3s's helm controller installs.
pub fn longhorn_manifest() -> String {
    format!(
        "# Written by homelabd on every K3s server: Longhorn, storing on the data
# disks of the hosts with the storage role, replicating over VLAN 20.
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: longhorn
  namespace: kube-system
spec:
  repo: https://charts.longhorn.io
  chart: longhorn
  version: {LONGHORN_VERSION}
  targetNamespace: longhorn-system
  createNamespace: true
  valuesContent: |-
    defaultSettings:
      createDefaultDiskLabeledNodes: true
      nodeDownPodDeletionPolicy: delete-both-statefulset-and-deployment-pod
      storageNetwork: {STORAGE_NETWORK}
    persistence:
      defaultClass: true
      defaultClassReplicaCount: 3
    longhornUI:
      replicas: 1
"
    )
}

/// The install script's arguments for `spec` on `host`: the role, how it
/// finds the cluster, and the node's name, address, interface and labels.
pub fn install_args(spec: &KubeSpec, host: &HostId, vip: Ipv4Addr, iface: &str) -> Vec<String> {
    let mut a: Vec<String> = vec![];
    let mut push = |s: &str| a.push(s.to_string());
    let join = format!("https://{vip}:6443");
    match spec.role {
        KubeRole::Server => {
            push("server");
            if spec.init {
                push("--cluster-init");
            } else {
                push("--server");
                push(&join);
            }
            push("--tls-san");
            push(&vip.to_string());
            // Longhorn is the cluster's storage.
            push("--disable");
            push("local-storage");
            push("--write-kubeconfig-mode");
            push("0600");
        }
        KubeRole::Agent => {
            push("agent");
            push("--server");
            push(&join);
        }
    }
    for (k, v) in [("--node-name", host.as_str()), ("--node-ip", &spec.node_ip.to_string()), ("--flannel-iface", iface)] {
        push(k);
        push(v);
    }
    for (k, v) in &spec.labels {
        push("--node-label");
        push(&format!("{k}={v}"));
    }
    a
}

/// Longhorn's default-disks annotation for `disks`, mounted under `mnt`.
pub fn disks_annotation(disks: &[String], mnt: &Path) -> String {
    let entries: Vec<serde_json::Value> = disks
        .iter()
        .map(|d| serde_json::json!({"name": d, "path": mnt.join(d).to_string_lossy(), "allowScheduling": true}))
        .collect();
    serde_json::Value::Array(entries).to_string()
}

#[derive(Debug, Default)]
struct Shared {
    want: Option<KubeSpec>,
    /// A new spec arrived since the worker last looked.
    changed: bool,
    status: Option<KubeStatus>,
}

/// The host's Kubernetes supervisor: hand it specs, read its status.
#[derive(Clone)]
pub struct Kube {
    shared: Arc<(Mutex<Shared>, Condvar)>,
}

impl Kube {
    /// Starts the worker for node `host`, joining through `vip`.
    pub fn start(host: HostId, vip: Ipv4Addr, paths: Paths) -> Self {
        let kube = Self { shared: Arc::new((Mutex::new(Shared::default()), Condvar::new())) };
        let k = kube.clone();
        let spawned = std::thread::Builder::new().name("kube".into()).spawn(move || Worker { host, vip, paths, applied: None }.run(&k));
        if let Err(e) = spawned {
            eprintln!("homelabd: start the kubernetes worker: {e}");
        }
        kube
    }

    /// Asks for `spec`; the worker picks it up at once if it is new.
    pub fn want(&self, spec: KubeSpec) {
        let (lock, cv) = &*self.shared;
        let mut s = lock.lock().expect("kube lock");
        if s.want.as_ref() != Some(&spec) {
            s.want = Some(spec);
            s.changed = true;
            cv.notify_one();
        }
    }

    pub fn status(&self) -> Option<KubeStatus> {
        self.shared.0.lock().expect("kube lock").status.clone()
    }

    fn set_status(&self, status: KubeStatus) {
        self.shared.0.lock().expect("kube lock").status = Some(status);
    }

    /// The spec in hand, after waiting up to `wait` for a new one.
    fn next(&self, wait: Duration) -> Option<KubeSpec> {
        let (lock, cv) = &*self.shared;
        let s = lock.lock().expect("kube lock");
        let (mut s, _) = cv.wait_timeout_while(s, wait, |s| !s.changed).expect("kube lock");
        s.changed = false;
        s.want.clone()
    }
}

/// What is on the machine now.
#[derive(Debug, Clone, Default)]
struct Observed {
    role: Option<KubeRole>,
    active: bool,
    /// Why the node does not look registered, if it does not.
    unregistered: Option<String>,
    node_ready: bool,
    version: String,
}

struct Worker {
    host: HostId,
    vip: Ipv4Addr,
    paths: Paths,
    /// The spec whose labels and disks the node has been given.
    applied: Option<KubeSpec>,
}

impl Worker {
    fn run(mut self, kube: &Kube) {
        let mut wait = Duration::ZERO;
        loop {
            let want = kube.next(wait);
            let (status, ok) = self.step(want.as_ref(), kube);
            wait = if ok { EVERY } else { BACKOFF };
            if let Ok(b) = serde_json::to_vec(&status) {
                let _ = std::fs::create_dir_all(&self.paths.run);
                let _ = std::fs::write(status_path(&self.paths), b);
            }
            kube.set_status(status);
        }
    }

    fn status(&self, o: &Observed, phase: KubePhase) -> KubeStatus {
        self.status_with(o, phase, None)
    }

    /// An `Installing` status saying what it is doing.
    fn installing(&self, o: &Observed, detail: String) -> KubeStatus {
        self.status_with(o, KubePhase::Installing, Some(detail))
    }

    fn status_with(&self, o: &Observed, phase: KubePhase, detail: Option<String>) -> KubeStatus {
        let (labels, storage_disks) = match &self.applied {
            Some(a) if phase == KubePhase::Running => (a.labels.clone(), a.storage_disks.clone()),
            _ => (BTreeMap::new(), vec![]),
        };
        KubeStatus { role: o.role, phase, node_ready: o.node_ready, labels, storage_disks, version: o.version.clone(), detail }
    }

    /// One pass toward `want`: the status after it, and whether it went
    /// without error.
    fn step(&mut self, want: Option<&KubeSpec>, kube: &Kube) -> (KubeStatus, bool) {
        let o = self.observe();
        let Some(spec) = want else {
            let status = match (o.role, o.active) {
                (None, _) => self.status(&o, KubePhase::Absent),
                (Some(_), true) => self.status(&o, KubePhase::Running),
                (Some(r), false) => self.installing(&o, format!("{} is not running", unit(r))),
            };
            return (status, true);
        };
        if o.role != Some(spec.role) {
            self.applied = None;
            kube.set_status(self.installing(&o, format!("installing K3s as {:?}", spec.role)));
            if let Err(e) = self.install(spec, o.role) {
                eprintln!("homelabd: kubernetes: {e}");
                return (self.status(&self.observe(), KubePhase::Failed(e)), false);
            }
            return (self.installing(&self.observe(), "installed; starting".into()), true);
        }
        if !o.active {
            // K3s restarts itself; this is for one stopped by hand, or not yet started.
            let _ = sh("systemctl", &["start", "--no-block", unit(spec.role)]);
            return (self.installing(&o, format!("starting {}", unit(spec.role))), true);
        }
        if let Some(why) = &o.unregistered {
            return (self.installing(&o, format!("waiting for the node to register: {why}")), true);
        }
        if spec.role == KubeRole::Server {
            if let Err(e) = write_if_changed(Path::new(MULTUS_MANIFEST), &multus_manifest()) {
                return (self.status(&o, KubePhase::Failed(format!("write the Multus manifest: {e}"))), false);
            }
            match self.multus_ready() {
                Ok(true) => {}
                Ok(false) => return (self.installing(&o, "waiting for Multus".into()), true),
                Err(e) => return (self.installing(&o, format!("waiting for Multus: {e}")), true),
            }
            if let Err(e) = self.apply_storage_nad() {
                eprintln!("homelabd: kubernetes: {e}");
                return (self.status(&o, KubePhase::Failed(e)), false);
            }
            if let Err(e) = write_if_changed(Path::new(MANIFEST), &longhorn_manifest()) {
                return (self.status(&o, KubePhase::Failed(format!("write the Longhorn manifest: {e}"))), false);
            }
            // Upgrades from before the storage network: Longhorn's
            // `defaultSettings` only apply on install, so patch a live
            // setting too. Blocked while volumes are attached; retried every
            // pass without holding up Running.
            self.ensure_storage_network();
            self.remove_etcd_members(spec);
        }
        // `etcd_remove` is a standing task, not state: relabelling on its
        // changes alone would churn `kubectl` every 15 s.
        let labelled = self.applied.as_ref().is_some_and(|a| a.role == spec.role && a.labels == spec.labels && a.storage_disks == spec.storage_disks);
        if !labelled {
            if let Err(e) = self.label(spec) {
                eprintln!("homelabd: kubernetes: {e}");
                return (self.status(&o, KubePhase::Failed(e)), false);
            }
            self.applied = Some(spec.clone());
        }
        (self.status(&o, KubePhase::Running), true)
    }

    fn observe(&self) -> Observed {
        let role = if Path::new("/etc/systemd/system/k3s.service").exists() {
            Some(KubeRole::Server)
        } else if Path::new("/etc/systemd/system/k3s-agent.service").exists() {
            Some(KubeRole::Agent)
        } else {
            None
        };
        let Some(r) = role else { return Observed::default() };
        let active = Command::new("systemctl").args(["is-active", "--quiet", unit(r)]).status().is_ok_and(|s| s.success());
        let version = sh(K3S_BIN, &["--version"]).ok().and_then(|v| v.split_whitespace().nth(2).map(String::from)).unwrap_or_default();
        let ready = if active {
            self.kubectl(r, &["get", "node", self.host.as_str(), "-o", "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}"])
        } else {
            Err("not running".into())
        };
        let node_ready = ready.as_deref().map(str::trim) == Ok("True");
        Observed { role, active, unregistered: ready.err(), node_ready, version }
    }

    fn kubectl(&self, role: KubeRole, args: &[&str]) -> Result<String, String> {
        let mut all = vec!["kubectl", "--request-timeout=10s"];
        if role == KubeRole::Agent {
            all.extend(["--kubeconfig", KUBELET_KUBECONFIG]);
        }
        all.extend(args);
        sh(K3S_BIN, &all).map_err(|e| e.to_string())
    }

    /// Whether Multus's DaemonSet is ready on every node. `Ok(false)` while
    /// the chart has not created it yet.
    fn multus_ready(&self) -> Result<bool, String> {
        match self.kubectl(KubeRole::Server, &["-n", "kube-system", "get", "ds", "multus", "-o", "json"]) {
            Ok(json) => Ok(ds_ready(&json)),
            Err(e) if e.contains("NotFound") => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Creates the storage network's namespace and applies its
    /// `NetworkAttachmentDefinition` (idempotent).
    fn apply_storage_nad(&self) -> Result<(), String> {
        // `create ns` fails when it exists; anything else is real.
        if let Err(e) = self.kubectl(KubeRole::Server, &["create", "ns", STORAGE_NAD_NAMESPACE])
            && !e.contains("AlreadyExists")
        {
            return Err(format!("create namespace {STORAGE_NAD_NAMESPACE}: {e}"));
        }
        let path = self.paths.run.join("storage-nad.yaml");
        std::fs::create_dir_all(&self.paths.run).map_err(|e| format!("write the storage NAD: {e}"))?;
        std::fs::write(&path, storage_nad_manifest()).map_err(|e| format!("write the storage NAD: {e}"))?;
        let path_s = path.to_string_lossy().into_owned();
        self.kubectl(KubeRole::Server, &["apply", "-f", &path_s]).map(|_| ()).map_err(|e| format!("apply the storage NAD: {e}"))
    }

    /// Patches a live Longhorn `storage-network` setting to
    /// [`STORAGE_NETWORK`] (fresh installs get it from the chart's
    /// `defaultSettings`). Blocked while volumes are attached; failures are
    /// logged and retried on the next pass, never fatal.
    fn ensure_storage_network(&self) {
        let current = self.kubectl(KubeRole::Server, &["-n", STORAGE_NAD_NAMESPACE, "get", "setting", "storage-network", "-o", "jsonpath={.value}"]);
        let Ok(value) = current else {
            return;
        };
        if value.trim() == STORAGE_NETWORK {
            return;
        }
        let patch = format!(r#"{{"value":"{STORAGE_NETWORK}"}}"#);
        if let Err(e) = self.kubectl(KubeRole::Server, &["-n", STORAGE_NAD_NAMESPACE, "patch", "setting", "storage-network", "--type", "merge", "-p", &patch]) {
            eprintln!("homelabd: kubernetes: set Longhorn storage-network: {e}");
        } else {
            eprintln!("homelabd: kubernetes: Longhorn storage-network is {STORAGE_NETWORK}");
        }
    }

    /// Installs K3s as `spec` says, uninstalling `installed` first.
    fn install(&self, spec: &KubeSpec, installed: Option<KubeRole>) -> Result<(), String> {
        match installed {
            Some(KubeRole::Server) => {
                eprintln!("homelabd: kubernetes: leaving as a server to rejoin as {:?}", spec.role);
                let _ = self.kubectl(KubeRole::Server, &["delete", "node", self.host.as_str(), "--wait=false"]);
                sh("/usr/local/bin/k3s-uninstall.sh", &[]).map_err(|e| e.to_string())?;
            }
            Some(KubeRole::Agent) => {
                eprintln!("homelabd: kubernetes: leaving as an agent to rejoin as {:?}", spec.role);
                sh("/usr/local/bin/k3s-agent-uninstall.sh", &[]).map_err(|e| e.to_string())?;
            }
            None => {}
        }
        prerequisites()?;
        let iface = LinuxVip::addresses()
            .into_iter()
            .find(|(_, ip)| *ip == spec.node_ip)
            .map(|(name, _)| name)
            .ok_or_else(|| format!("no interface has the node address {}", spec.node_ip))?;
        let script = self.paths.run.join("k3s-install.sh");
        let _ = std::fs::create_dir_all(&self.paths.run);
        let (script_url, bin_url) = k3s_urls();
        fetch_verified(&script_url, INSTALL_SCRIPT_SHA256, &script).map_err(|e| format!("download the K3s installer: {e}"))?;
        fetch_verified(&bin_url, K3S_SHA256, Path::new(K3S_BIN)).map_err(|e| format!("download K3s: {e}"))?;
        sh("chmod", &["0755", K3S_BIN]).map_err(|e| e.to_string())?;
        let args = install_args(spec, &self.host, self.vip, &iface);
        eprintln!("homelabd: kubernetes: installing K3s {K3S_VERSION}: {}", args.join(" "));
        let out = Command::new("sh")
            .arg(&script)
            .args(&args)
            .env("INSTALL_K3S_VERSION", K3S_VERSION)
            .env("K3S_TOKEN", &spec.token)
            // The binary checked above; the script would fetch it unchecked.
            .env("INSTALL_K3S_SKIP_DOWNLOAD", "true")
            // Started below without waiting: a joining server's start lasts
            // until it reaches the cluster, which may take a while.
            .env("INSTALL_K3S_SKIP_START", "true")
            .output()
            .map_err(|e| format!("run the K3s installer: {e}"))?;
        if !out.status.success() {
            let tail: String = String::from_utf8_lossy(&out.stderr).lines().rev().take(5).collect::<Vec<_>>().join(" | ");
            return Err(format!("the K3s installer failed: {tail}"));
        }
        sh("systemctl", &["start", "--no-block", unit(spec.role)]).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Deletes the nodes in `spec.etcd_remove`: dead, replaced servers
    /// whose etcd members must leave the cluster (`kubectl delete node`
    /// removes both). Never its own node, never one that reads Ready (a
    /// live member the control plane misjudged). Failures are logged and
    /// retried on the next pass, never fatal: a removal that cannot commit
    /// (no quorum) must not mark this host Failed.
    fn remove_etcd_members(&self, spec: &KubeSpec) {
        for dead in &spec.etcd_remove {
            if *dead == self.host {
                eprintln!("homelabd: kubernetes: not removing own etcd member {dead}");
                continue;
            }
            match self.kubectl(spec.role, &["get", "node", dead.as_str(), "-o", "json"]) {
                Ok(json) if node_is_ready(&json) => {
                    eprintln!("homelabd: kubernetes: not removing {dead}: its node reads Ready");
                    continue;
                }
                Ok(_) => {}
                Err(e) if e.contains("NotFound") => continue,
                Err(e) => {
                    eprintln!("homelabd: kubernetes: read node {dead}: {e}");
                    continue;
                }
            }
            if let Err(e) = self.kubectl(spec.role, &["delete", "node", dead.as_str(), "--wait=false"]) {
                eprintln!("homelabd: kubernetes: delete node {dead}: {e}");
            } else {
                eprintln!("homelabd: kubernetes: removed dead server {dead} (node and etcd member)");
            }
        }
    }

    /// Gives the node `spec`'s labels (dropping homelab labels it no longer
    /// has) and, on a storage host, Longhorn its disks.
    fn label(&self, spec: &KubeSpec) -> Result<(), String> {
        let node = self.host.as_str();
        let json = self.kubectl(spec.role, &["get", "node", node, "-o", "json"])?;
        let current: serde_json::Value = serde_json::from_str(&json).map_err(|e| format!("read node {node}: {e}"))?;
        let mut args: Vec<String> = vec!["label".into(), "node".into(), node.into(), "--overwrite".into()];
        args.extend(spec.labels.iter().map(|(k, v)| format!("{k}={v}")));
        let stale = current["metadata"]["labels"].as_object().into_iter().flatten().map(|(k, _)| k).filter(|k| k.starts_with(control::controllers::kube::LABEL_PREFIX) && !spec.labels.contains_key(*k));
        args.extend(stale.map(|k| format!("{k}-")));
        self.kubectl(spec.role, &args.iter().map(String::as_str).collect::<Vec<_>>())?;
        if spec.storage_disks.is_empty() {
            return Ok(());
        }
        for d in &spec.storage_disks {
            let dir = self.paths.mnt.join(d);
            if !Command::new("mountpoint").arg("-q").arg(&dir).status().is_ok_and(|s| s.success()) {
                return Err(format!("data disk {d} is not mounted at {} yet", dir.display()));
            }
        }
        let annotation = format!("{LONGHORN_DISK_ANNOTATION}={}", disks_annotation(&spec.storage_disks, &self.paths.mnt));
        self.kubectl(spec.role, &["annotate", "node", node, "--overwrite", &annotation])?;
        self.kubectl(spec.role, &["label", "node", node, "--overwrite", &format!("{LONGHORN_DISK_LABEL}=config")])?;
        Ok(())
    }
}

/// What Longhorn needs on every node that may attach a volume: the iSCSI
/// initiator (running) and the NFS client (for shared volumes).
fn prerequisites() -> Result<(), String> {
    if !Path::new("/usr/sbin/iscsiadm").exists() || !Path::new("/usr/sbin/mount.nfs").exists() {
        eprintln!("homelabd: kubernetes: installing open-iscsi and nfs-common");
        let apt = |args: &[&str]| {
            let out = Command::new("apt-get").args(args).env("DEBIAN_FRONTEND", "noninteractive").output().map_err(|e| format!("apt-get: {e}"))?;
            if out.status.success() { Ok(()) } else { Err(format!("apt-get {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim())) }
        };
        apt(&["update", "-q"])?;
        apt(&["install", "-y", "-q", "open-iscsi", "nfs-common"])?;
    }
    write_if_changed(Path::new("/etc/modules-load.d/homelab-longhorn.conf"), "iscsi_tcp\n").map_err(|e| e.to_string())?;
    sh("modprobe", &["iscsi_tcp"]).map_err(|e| format!("load iscsi_tcp: {e}"))?;
    sh("systemctl", &["enable", "--now", "iscsid"]).map_err(|e| e.to_string())?;
    Ok(())
}

/// Where [`K3S_VERSION`]'s install script and amd64 binary are downloaded from.
fn k3s_urls() -> (String, String) {
    let tag = K3S_VERSION.replace('+', "%2B");
    (format!("https://raw.githubusercontent.com/k3s-io/k3s/{tag}/install.sh"), format!("https://github.com/k3s-io/k3s/releases/download/{tag}/k3s"))
}

/// Downloads `url` to `dest` if its sha256 is `sha256`; a file that does not
/// match never reaches `dest`.
fn fetch_verified(url: &str, sha256: &str, dest: &Path) -> Result<(), String> {
    let tmp = dest.with_extension("download");
    let tmp_s = tmp.to_string_lossy();
    sh("curl", &["-fsSL", "--retry", "3", "-o", &tmp_s, url]).map_err(|e| e.to_string())?;
    let sum = sh("sha256sum", &[&tmp_s]).map_err(|e| e.to_string())?;
    if !sum_matches(&sum, sha256) {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("{url} does not have the expected sha256 {sha256} (got {})", sum.split_whitespace().next().unwrap_or("nothing")));
    }
    std::fs::rename(&tmp, dest).map_err(|e| format!("install {}: {e}", dest.display()))
}

/// Whether `sha256sum`'s output (`<hex>  <file>`) is for `sha256`.
fn sum_matches(sha256sum_out: &str, sha256: &str) -> bool {
    sha256sum_out.split_whitespace().next().is_some_and(|s| s.eq_ignore_ascii_case(sha256))
}

fn write_if_changed(path: &Path, body: &str) -> std::io::Result<()> {
    if std::fs::read_to_string(path).is_ok_and(|b| b == body) {
        return Ok(());
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, body)
}

/// Whether `kubectl get node -o json` shows the node Ready.
fn node_is_ready(node_json: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(node_json).is_ok_and(|v| {
        v["status"]["conditions"].as_array().is_some_and(|cs| cs.iter().any(|c| c["type"] == "Ready" && c["status"] == "True"))
    })
}

/// Whether `kubectl get ds -o json` shows every desired pod ready.
fn ds_ready(ds_json: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(ds_json).is_ok_and(|v| {
        let (ready, desired) = (v["status"]["numberReady"].as_u64().unwrap_or(0), v["status"]["desiredNumberScheduled"].as_u64().unwrap_or(0));
        desired > 0 && ready >= desired
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(role: KubeRole, init: bool) -> KubeSpec {
        KubeSpec {
            role,
            init,
            token: "t".into(),
            node_ip: Ipv4Addr::new(10, 0, 0, 2),
            labels: [("homelab.io/profile".to_string(), "ms01".to_string())].into(),
            storage_disks: vec!["nvme0".into()],
            etcd_remove: vec![],
        }
    }

    #[test]
    fn the_first_server_starts_the_cluster_and_others_join_through_the_vip() {
        let vip = Ipv4Addr::new(10, 0, 3, 253);
        let first = install_args(&spec(KubeRole::Server, true), &HostId::from("ms01-01"), vip, "eth0").join(" ");
        assert_eq!(
            first,
            "server --cluster-init --tls-san 10.0.3.253 --disable local-storage --write-kubeconfig-mode 0600 \
             --node-name ms01-01 --node-ip 10.0.0.2 --flannel-iface eth0 --node-label homelab.io/profile=ms01"
        );
        let second = install_args(&spec(KubeRole::Server, false), &HostId::from("ms01-02"), vip, "eth0").join(" ");
        assert!(second.starts_with("server --server https://10.0.3.253:6443 --tls-san 10.0.3.253"), "{second}");
        let agent = install_args(&spec(KubeRole::Agent, false), &HostId::from("pi5-01"), vip, "bond0").join(" ");
        assert!(agent.starts_with("agent --server https://10.0.3.253:6443 --node-name pi5-01"), "{agent}");
        assert!(agent.contains("--flannel-iface bond0") && !agent.contains("tls-san"), "{agent}");
    }

    #[test]
    fn k3s_downloads_are_those_of_the_pinned_release_and_checked() {
        let (script, bin) = k3s_urls();
        assert_eq!(script, "https://raw.githubusercontent.com/k3s-io/k3s/v1.36.5%2Bk3s1/install.sh");
        assert_eq!(bin, "https://github.com/k3s-io/k3s/releases/download/v1.36.5%2Bk3s1/k3s");
        let sum = format!("{K3S_SHA256}  /usr/local/bin/k3s.download\n");
        assert!(sum_matches(&sum, K3S_SHA256));
        assert!(sum_matches(&sum.to_uppercase(), K3S_SHA256));
        assert!(!sum_matches(&sum, INSTALL_SCRIPT_SHA256));
        assert!(!sum_matches("", K3S_SHA256), "no output is no match");
        assert!([K3S_SHA256, INSTALL_SCRIPT_SHA256].iter().all(|s| s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())));
    }

    #[test]
    fn the_kubeconfig_points_at_the_vip() {
        let k = "clusters:\n- cluster:\n    server: https://127.0.0.1:6443\n";
        assert_eq!(at_vip(k, Ipv4Addr::new(10, 0, 3, 253)), "clusters:\n- cluster:\n    server: https://10.0.3.253:6443\n");
    }

    #[test]
    fn longhorn_gets_each_data_disk_by_its_mount() {
        let a = disks_annotation(&["nvme0".into(), "nvme1".into()], Path::new("/mnt"));
        let v: serde_json::Value = serde_json::from_str(&a).unwrap();
        assert_eq!(v[1]["path"], "/mnt/nvme1");
        assert_eq!(v[0]["allowScheduling"], true);
        assert!(longhorn_manifest().contains(&format!("version: {LONGHORN_VERSION}")));
        assert!(longhorn_manifest().contains("createDefaultDiskLabeledNodes: true"));
        assert!(longhorn_manifest().contains(&format!("storageNetwork: {STORAGE_NETWORK}")), "replication rides VLAN 20");
    }

    #[test]
    fn multus_and_the_storage_attachment_fit_k3s_and_vlan_20() {
        let m = multus_manifest();
        assert!(m.contains(&format!("version: {MULTUS_VERSION}")), "{m}");
        assert!(m.contains("chart: rke2-multus") && m.contains("repo: https://rke2-charts.rancher.io"));
        assert!(m.contains("/var/lib/rancher/k3s/agent/etc/cni/net.d") && m.contains("/var/lib/rancher/k3s/data/cni/"));
        assert!(m.contains("rke2-whereabouts") && m.contains("enabled: true"));
        let nad = storage_nad_manifest();
        assert!(nad.contains("kind: NetworkAttachmentDefinition") && nad.contains(&format!("name: {STORAGE_NAD_NAME}")) && nad.contains(&format!("namespace: {STORAGE_NAD_NAMESPACE}")));
        assert!(nad.contains(r#""type": "macvlan""#) && nad.contains(r#""master": "storage0""#) && nad.contains(r#""mtu": 9000"#));
        assert!(nad.contains(r#""type": "whereabouts""#) && nad.contains(&format!(r#""range": "{STORAGE_POD_RANGE}""#)));
        assert!(nad.contains("whereabouts.d/whereabouts.conf"), "the K3s path Whereabouts needs");
        assert!(ds_ready(r#"{"status":{"numberReady":4,"desiredNumberScheduled":4}}"#));
        assert!(!ds_ready(r#"{"status":{"numberReady":3,"desiredNumberScheduled":4}}"#));
        assert!(!ds_ready(r#"{"status":{"numberReady":0,"desiredNumberScheduled":0}}"#));
        assert!(!ds_ready("not json"));
    }

    #[test]
    fn a_ready_node_is_never_an_etcd_removal() {
        let ready = r#"{"status":{"conditions":[{"type":"Ready","status":"True"}]}}"#;
        let not_ready = r#"{"status":{"conditions":[{"type":"Ready","status":"False"}]}}"#;
        assert!(node_is_ready(ready));
        assert!(!node_is_ready(not_ready));
        assert!(!node_is_ready(r#"{"status":{}}"#));
        assert!(!node_is_ready("not json"));
        // An old spec without the field still parses (rolling upgrades).
        let old = r#"{"role":"Server","init":false,"token":"t","node_ip":"10.0.0.2","labels":{},"storage_disks":[]}"#;
        let spec: KubeSpec = serde_json::from_str(old).unwrap();
        assert!(spec.etcd_remove.is_empty());
    }
}
