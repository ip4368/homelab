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
//! 3. On a server, writes the Longhorn `HelmChart` manifest K3s deploys.
//! 4. Labels the node, and on a storage host gives Longhorn its data disks
//!    (`node.longhorn.io/default-disks-config`, then the label that tells
//!    Longhorn to use it). A host that stores no more loses that label.
//!
//! With a storage network in the spec, Longhorn replicates over it: every
//! K3s server also writes the Multus (and whereabouts) charts and the
//! `NetworkAttachmentDefinition` Longhorn uses, which has no config of its
//! own; each host writes that config, a macvlan on its own interface on the
//! network ([`storage_attachment`]), where Multus looks for it, before
//! anything else.
//!
//! On a server, it also reads etcd's members (K3s's `/db/info`) for the
//! status, and does the work the leader asks of it for the cluster
//! ([`KubeAdmin`]): it deletes the nodes of dead members, so K3s removes
//! them from etcd, keeps Longhorn off the disks of hosts that do not store,
//! and cordons and drains the nodes of hosts being serviced.
//!
//! It writes what it found to `/run/homelab/kube.json` for `homelabd status`.

use super::{sh, LinuxVip, Paths};
use control::proto::{KubeAdmin, KubePhase, KubeRole, KubeSpec, KubeStatus, StorageNetwork};
use control::runtime::SimTime;
use model::HostId;
use std::collections::{BTreeMap, BTreeSet};
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
/// The admin kubeconfig a K3s server writes.
pub const KUBECONFIG: &str = "/etc/rancher/k3s/k3s.yaml";
/// What an agent's kubelet authenticates with: enough to label its own node.
const KUBELET_KUBECONFIG: &str = "/var/lib/rancher/k3s/agent/kubelet.kubeconfig";
const MANIFEST: &str = "/var/lib/rancher/k3s/server/manifests/homelab-longhorn.yaml";
/// How often the worker looks again when nothing changed.
const EVERY: Duration = Duration::from_secs(10);
/// How long it waits after a failed step before trying again.
const BACKOFF: Duration = Duration::from_secs(20);
const LONGHORN_DISK_LABEL: &str = "node.longhorn.io/create-default-disk";
const LONGHORN_DISK_ANNOTATION: &str = "node.longhorn.io/default-disks-config";
/// On a Longhorn node: the disks the control plane took out of scheduling
/// because the host does not store, to give back once it does.
const STORAGE_OFF_ANNOTATION: &str = "homelab.io/storage-off";
/// On a node: the control plane cordoned it, so uncordons it once its host
/// is no longer held. A node cordoned by hand has none, and is left alone.
const CORDONED_ANNOTATION: &str = "homelab.io/cordoned";
/// How long one `kubectl drain` waits for its pods to go; the next request
/// from the leader drains again.
const DRAIN_TIMEOUT: &str = "30s";
/// Longhorn's storage network: the `NetworkAttachmentDefinition` (in
/// `longhorn-system`) and the name of each host's config for it.
pub const STORAGE_ATTACHMENT: &str = "homelab-storage";
/// Where Multus looks for the config of an attachment whose definition has
/// none: each host's own, for its own interface.
const MULTUS_CONF_DIR: &str = "/etc/cni/multus/net.d";
const MULTUS_MANIFEST: &str = "/var/lib/rancher/k3s/server/manifests/homelab-multus.yaml";
/// The rke2-multus chart (with whereabouts) and its CRDs, as K3s documents them.
pub const MULTUS_CHART_VERSION: &str = "v4.3.118";
const MULTUS_CRD_CHART_VERSION: &str = "4.3.118";
/// Where K3s's CNI configs and plugins live, which Multus and whereabouts use.
const K3S_CNI_CONF: &str = "/var/lib/rancher/k3s/agent/etc/cni/net.d";
const K3S_CNI_BIN: &str = "/var/lib/rancher/k3s/data/cni/";
/// A K3s server's certificates: its CA and the admin's client certificate.
const SERVER_TLS: &str = "/var/lib/rancher/k3s/server/tls";

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

/// The Longhorn deployment, as a manifest K3s's helm controller installs.
/// With `storage_network`, Longhorn replicates over the storage network
/// ([`STORAGE_ATTACHMENT`]) and runs only on the hosts attached to it.
pub fn longhorn_manifest(storage_network: bool) -> String {
    let label = control::controllers::kube::STORAGE_NETWORK_LABEL;
    let (settings, manager) = if storage_network {
        (
            format!(
                "      storageNetwork: longhorn-system/{STORAGE_ATTACHMENT}\n      systemManagedComponentsNodeSelector: \"{label}:true\"\n"
            ),
            format!("    longhornManager:\n      nodeSelector:\n        {label}: \"true\"\n"),
        )
    } else {
        (String::new(), String::new())
    };
    format!(
        "# Written by homelabd on every K3s server: Longhorn, storing on the data
# disks of the hosts with the storage role.
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
{settings}    persistence:
      defaultClass: true
      defaultClassReplicaCount: 3
    longhornUI:
      replicas: 1
{manager}"
    )
}

/// Multus (thin, with whereabouts for addresses) installed through K3s's
/// helm controller, and Longhorn's storage network attachment. The
/// attachment has no config: Multus reads each host's own
/// ([`storage_attachment`]). K3s applies it once the CRD chart is in.
pub fn multus_manifest() -> String {
    format!(
        "# Written by homelabd on every K3s server: Multus, which attaches
# Longhorn's pods to the storage network on each host.
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: multus-crd
  namespace: kube-system
spec:
  repo: https://rke2-charts.rancher.io
  chart: rke2-multus-crd
  version: {MULTUS_CRD_CHART_VERSION}
  targetNamespace: kube-system
---
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: multus
  namespace: kube-system
spec:
  repo: https://rke2-charts.rancher.io
  chart: rke2-multus
  version: {MULTUS_CHART_VERSION}
  targetNamespace: kube-system
  valuesContent: |-
    config:
      fullnameOverride: multus
      cni_conf:
        confDir: {K3S_CNI_CONF}
        binDir: {K3S_CNI_BIN}
        kubeconfig: {K3S_CNI_CONF}/multus.d/multus.kubeconfig
        multusAutoconfigDir: {K3S_CNI_CONF}
    rke2-whereabouts:
      fullnameOverride: whereabouts
      enabled: true
      cniConf:
        confDir: {K3S_CNI_CONF}
        binDir: {K3S_CNI_BIN}
---
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata:
  name: {STORAGE_ATTACHMENT}
  namespace: longhorn-system
spec: {{}}
"
    )
}

/// This host's config of Longhorn's storage network attachment: a macvlan
/// on `master` (its interface on the network), with addresses from the
/// network's pod range, network and broadcast addresses left out.
pub fn storage_attachment(master: &str, net: &StorageNetwork) -> String {
    let first = net.pods.first.max(Ipv4Addr::from(u32::from(net.subnet.network()) + 1));
    let last = net.pods.last.min(Ipv4Addr::from(u32::from(net.subnet.broadcast()) - 1));
    let conf = serde_json::json!({
        "cniVersion": "0.3.1",
        "name": STORAGE_ATTACHMENT,
        "type": "macvlan",
        "master": master,
        "mode": "bridge",
        "ipam": {
            "type": "whereabouts",
            "range": net.subnet.trunc().to_string(),
            "range_start": first.to_string(),
            "range_end": last.to_string(),
            "configuration_path": format!("{K3S_CNI_CONF}/whereabouts.d/whereabouts.conf"),
        },
    });
    serde_json::to_string_pretty(&conf).expect("a CNI config serializes") + "\n"
}

/// An etcd member, as K3s's `/db/info` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EtcdMember {
    pub id: u64,
    /// Its node: a member is named `<node>-<8 hex digits>`; `None` for one
    /// that has not started yet, which has no name.
    pub node: Option<HostId>,
    /// The address its peers reach it at (its server's node address).
    pub peer: Option<Ipv4Addr>,
}

/// The etcd members in K3s's `/db/info`.
pub fn etcd_members_in(db_info: &str) -> Result<Vec<EtcdMember>, String> {
    let v: serde_json::Value = serde_json::from_str(db_info).map_err(|e| format!("read etcd's members: {e}"))?;
    let members = v["members"].as_array().ok_or("etcd's members: no list")?;
    let node = |n: &str| match n.rsplit_once('-') {
        Some((node, id)) if id.len() == 8 && id.chars().all(|c| c.is_ascii_hexdigit()) => HostId::from(node),
        _ => HostId::from(n),
    };
    // A peer URL is `https://<address>:2380`.
    let peer = |m: &serde_json::Value| m["peerURLs"][0].as_str()?.split_once("://")?.1.rsplit_once(':')?.0.parse().ok();
    members
        .iter()
        .map(|m| {
            let id = m["ID"].as_u64().ok_or_else(|| format!("etcd's members: no ID in {m}"))?;
            Ok(EtcdMember { id, node: m["name"].as_str().filter(|n| !n.is_empty()).map(node), peer: peer(m) })
        })
        .collect()
}

/// What a status says of `members`: the nodes of those that started, and
/// the peer addresses of those that have not.
fn by_node_and_peer(members: &[EtcdMember]) -> (BTreeSet<HostId>, BTreeSet<Ipv4Addr>) {
    let nodes = members.iter().filter_map(|m| m.node.clone()).collect();
    let unstarted = members.iter().filter(|m| m.node.is_none()).filter_map(|m| m.peer).collect();
    (nodes, unstarted)
}

/// etcd's `Cluster.MemberRemove` request for member `id`, framed for gRPC:
/// uncompressed, the message's length, then the message (field 1, the ID,
/// a varint).
pub fn member_remove_request(id: u64) -> Vec<u8> {
    let mut msg = vec![0x08];
    let mut v = id;
    while v >= 0x80 {
        msg.push((v & 0x7f) as u8 | 0x80);
        v >>= 7;
    }
    msg.push(v as u8);
    let mut out = vec![0];
    out.extend((msg.len() as u32).to_be_bytes());
    out.extend(msg);
    out
}

/// How a server removes a dead host's etcd member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtcdRemoval {
    /// Its node is `Ready`: it is not removed.
    Keep,
    /// Delete its node, and K3s removes the member.
    DeleteNode,
    /// Remove member `id` through etcd itself: it has no node to delete, or
    /// never started, so K3s would not find it by its node.
    ThroughEtcd(u64),
}

/// The member of `members` that is `host`'s, found by its node, or by
/// `peer` (the host's address) for one that never started; and how to
/// remove it, given `host`'s node (`None` for none). A host that runs a
/// K3s agent (`agent`) has a node, the agent's, which stays: its member,
/// left behind by its old server, goes through etcd.
pub fn member_removal(members: &[EtcdMember], host: &HostId, peer: Option<Ipv4Addr>, node: Option<&serde_json::Value>, agent: bool) -> Option<EtcdRemoval> {
    let m = members.iter().find(|m| m.node.as_ref() == Some(host)).or_else(|| members.iter().find(|m| m.node.is_none() && peer.is_some() && m.peer == peer))?;
    Some(match node {
        _ if agent => EtcdRemoval::ThroughEtcd(m.id),
        Some(n) if node_ready(n) => EtcdRemoval::Keep,
        Some(_) if m.node.is_some() => EtcdRemoval::DeleteNode,
        _ => EtcdRemoval::ThroughEtcd(m.id),
    })
}

/// What a status says of etcd's members read as `read`: the members, or
/// none and why.
fn split<T: Default>(read: Result<T, String>) -> (T, Option<String>) {
    match read {
        Ok(members) => (members, None),
        Err(e) => (T::default(), Some(e)),
    }
}

/// The running pods in `pods` (`kubectl get pods -o json`) that asked
/// Multus for the storage network attachment and did not get it: their
/// sandbox was made while K3s's CNI config lacked Multus (K3s reinstalled,
/// or Multus not started yet on a new node), so they run on the pod
/// network alone.
pub fn pods_missing_attachment(pods: &serde_json::Value) -> Vec<String> {
    let want = format!("longhorn-system/{STORAGE_ATTACHMENT}");
    let attached = |p: &serde_json::Value, key: &str, field: &str| -> bool {
        let list: Vec<serde_json::Value> = p["metadata"]["annotations"][key].as_str().and_then(|a| serde_json::from_str(a).ok()).unwrap_or_default();
        list.iter().any(|n| match field {
            "networks" => n["name"] == STORAGE_ATTACHMENT,
            _ => n["name"] == want.as_str(),
        })
    };
    let pods = pods["items"].as_array().into_iter().flatten();
    pods.filter(|p| p["status"]["phase"] == "Running" && p["metadata"]["deletionTimestamp"].is_null())
        .filter(|p| attached(p, "k8s.v1.cni.cncf.io/networks", "networks") && !attached(p, "k8s.v1.cni.cncf.io/network-status", "status"))
        .filter_map(|p| p["metadata"]["name"].as_str().map(String::from))
        .collect()
}

/// Whether a node (`kubectl get node -o json`) is `Ready`.
fn node_ready(node: &serde_json::Value) -> bool {
    node["status"]["conditions"].as_array().into_iter().flatten().any(|c| c["type"] == "Ready" && c["status"] == "True")
}

/// The merge patches, by Longhorn node, that keep Longhorn off the disks of
/// the nodes in `off` and give back those it was kept off before, from the
/// list of Longhorn's nodes: a listed node's schedulable disks are turned
/// off and noted in [`STORAGE_OFF_ANNOTATION`]; the noted disks of a node no
/// longer listed are turned on again, and the note dropped.
pub fn storage_off_patches(nodes: &serde_json::Value, off: &BTreeSet<HostId>) -> Vec<(String, serde_json::Value)> {
    let mut out = vec![];
    for n in nodes["items"].as_array().into_iter().flatten() {
        let Some(name) = n["metadata"]["name"].as_str() else { continue };
        let noted: BTreeSet<String> = n["metadata"]["annotations"][STORAGE_OFF_ANNOTATION].as_str().unwrap_or("").split(',').filter(|d| !d.is_empty()).map(String::from).collect();
        let disks = n["spec"]["disks"].as_object();
        let set = |names: &BTreeSet<String>, on: bool| -> serde_json::Map<String, serde_json::Value> {
            names.iter().map(|d| (d.clone(), serde_json::json!({ "allowScheduling": on }))).collect()
        };
        if off.contains(&HostId::from(name)) {
            let on: BTreeSet<String> = disks.into_iter().flatten().filter(|(_, d)| d["allowScheduling"] == true).map(|(k, _)| k.clone()).collect();
            if on.is_empty() {
                continue;
            }
            let note = noted.union(&on).cloned().collect::<Vec<_>>().join(",");
            out.push((name.to_string(), serde_json::json!({ "metadata": { "annotations": { STORAGE_OFF_ANNOTATION: note } }, "spec": { "disks": set(&on, false) } })));
        } else if !noted.is_empty() {
            let back: BTreeSet<String> = noted.into_iter().filter(|d| disks.is_some_and(|m| m.contains_key(d))).collect();
            out.push((name.to_string(), serde_json::json!({ "metadata": { "annotations": { STORAGE_OFF_ANNOTATION: null } }, "spec": { "disks": set(&back, true) } })));
        }
    }
    out
}

/// The merge patches, by node, that cordon the nodes of the hosts in
/// `cordon` and uncordon those of the hosts in `uncordon` the control plane
/// cordoned before, from the list of nodes (`kubectl get nodes -o json`): a
/// node in `cordon` that is schedulable is cordoned and noted in
/// [`CORDONED_ANNOTATION`]; a noted node in `uncordon` is uncordoned, and
/// the note dropped. A node cordoned by hand is not noted, so stays
/// cordoned; so does the node of a host in neither set (a removed host).
pub fn cordon_patches(nodes: &serde_json::Value, cordon: &BTreeSet<HostId>, uncordon: &BTreeSet<HostId>) -> Vec<(String, serde_json::Value)> {
    let mut out = vec![];
    for n in nodes["items"].as_array().into_iter().flatten() {
        let Some(name) = n["metadata"]["name"].as_str() else { continue };
        let noted = !n["metadata"]["annotations"][CORDONED_ANNOTATION].is_null();
        let unschedulable = n["spec"]["unschedulable"] == true;
        if cordon.contains(&HostId::from(name)) {
            if !unschedulable {
                out.push((name.to_string(), serde_json::json!({ "metadata": { "annotations": { CORDONED_ANNOTATION: "true" } }, "spec": { "unschedulable": true } })));
            }
        } else if noted && uncordon.contains(&HostId::from(name)) {
            out.push((name.to_string(), serde_json::json!({ "metadata": { "annotations": { CORDONED_ANNOTATION: null } }, "spec": { "unschedulable": null } })));
        }
    }
    out
}

/// The pods in `pods` (`kubectl get pods -o json`, those of one node) that
/// a drain must still move: all but DaemonSet pods, static (mirror) pods,
/// and pods that finished.
pub fn pods_left_to_drain(pods: &serde_json::Value) -> Vec<String> {
    let pods = pods["items"].as_array().into_iter().flatten();
    pods.filter(|p| !matches!(p["status"]["phase"].as_str(), Some("Succeeded" | "Failed")))
        .filter(|p| p["metadata"]["annotations"]["kubernetes.io/config.mirror"].is_null())
        .filter(|p| !p["metadata"]["ownerReferences"].as_array().into_iter().flatten().any(|o| o["kind"] == "DaemonSet"))
        .filter_map(|p| Some(format!("{}/{}", p["metadata"]["namespace"].as_str()?, p["metadata"]["name"].as_str()?)))
        .collect()
}

/// Why a `kubectl drain` that failed could not evict everything, from its
/// error: the eviction Kubernetes refused (a PodDisruptionBudget, say), or
/// else the error's last line.
pub fn drain_refusal(err: &str) -> String {
    let lines: Vec<&str> = err.lines().map(str::trim).filter(|l| !l.is_empty()).collect();
    let refused = lines.iter().find(|l| l.contains("Cannot evict"));
    refused.or(lines.last()).map_or_else(|| "kubectl drain failed".into(), |l| l.to_string())
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
    /// Work for the cluster the leader asked for, not done yet.
    admin: Option<KubeAdmin>,
    /// A new spec or work arrived since the worker last looked.
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
        let worker = Worker { host, vip, paths, applied: None, storage_off: BTreeSet::new(), drains: Drains::default() };
        let spawned = std::thread::Builder::new().name("kube".into()).spawn(move || worker.run(&k));
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

    /// Asks for `admin` to be done once, in place of any not done yet.
    pub fn admin(&self, admin: KubeAdmin) {
        let (lock, cv) = &*self.shared;
        let mut s = lock.lock().expect("kube lock");
        s.admin = Some(admin);
        s.changed = true;
        cv.notify_one();
    }

    pub fn status(&self) -> Option<KubeStatus> {
        self.shared.0.lock().expect("kube lock").status.clone()
    }

    fn set_status(&self, status: KubeStatus) {
        self.shared.0.lock().expect("kube lock").status = Some(status);
    }

    /// The spec in hand and the work asked for, after waiting up to `wait`
    /// for something new.
    fn next(&self, wait: Duration) -> (Option<KubeSpec>, Option<KubeAdmin>) {
        let (lock, cv) = &*self.shared;
        let s = lock.lock().expect("kube lock");
        let (mut s, _) = cv.wait_timeout_while(s, wait, |s| !s.changed).expect("kube lock");
        s.changed = false;
        (s.want.clone(), s.admin.take())
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
    /// On a server: etcd's members.
    etcd_members: Vec<EtcdMember>,
    /// On a server: why etcd's members could not be read.
    etcd_unread: Option<String>,
}

struct Worker {
    host: HostId,
    vip: Ipv4Addr,
    paths: Paths,
    /// The spec whose labels and disks the node has been given.
    applied: Option<KubeSpec>,
    /// The hosts whose Longhorn disks this server last kept Longhorn off.
    storage_off: BTreeSet<HostId>,
    /// How the cordons and drains this server was last asked for stand.
    drains: Drains,
}

/// How the cordons and drains a server was asked for stand, for its status.
#[derive(Debug, Clone, Default)]
struct Drains {
    /// The hosts whose nodes it cordoned.
    cordoned: BTreeSet<HostId>,
    /// The hosts whose nodes carry [`CORDONED_ANNOTATION`].
    noted: BTreeSet<HostId>,
    /// The hosts asked to be drained whose nodes are empty, each with the
    /// drain it answers.
    drained_since: BTreeMap<HostId, SimTime>,
    /// Why the others' pods could not all be evicted yet.
    blocked: BTreeMap<HostId, String>,
}

impl Worker {
    fn run(mut self, kube: &Kube) {
        let mut wait = Duration::ZERO;
        loop {
            let (want, admin) = kube.next(wait);
            let (mut status, ok) = self.step(want.as_ref(), kube);
            if let Some(admin) = admin
                && status.role == Some(KubeRole::Server)
                && status.phase == KubePhase::Running
            {
                if let Err(e) = self.admin(&admin) {
                    eprintln!("homelabd: kubernetes: for the cluster: {e}");
                }
                if let Err(e) = self.cordon_and_drain(&admin) {
                    eprintln!("homelabd: kubernetes: cordon and drain: {e}");
                }
                let (members, unread) = split(self.etcd_members());
                (status.etcd_members, status.etcd_unstarted) = by_node_and_peer(&members);
                status.etcd_unread = unread;
                status.storage_off.clone_from(&self.storage_off);
                status.cordoned.clone_from(&self.drains.cordoned);
                status.cordon_noted.clone_from(&self.drains.noted);
                status.drained_since.clone_from(&self.drains.drained_since);
                status.drain_blocked.clone_from(&self.drains.blocked);
            }
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
        let (etcd_members, etcd_unstarted) = by_node_and_peer(&o.etcd_members);
        let server = o.role == Some(KubeRole::Server);
        let drains = if server { self.drains.clone() } else { Drains::default() };
        KubeStatus {
            role: o.role,
            phase,
            node_ready: o.node_ready,
            labels,
            storage_disks,
            version: o.version.clone(),
            detail,
            etcd_members,
            etcd_unread: o.etcd_unread.clone(),
            etcd_unstarted,
            storage_off: if server { self.storage_off.clone() } else { BTreeSet::new() },
            cordoned: drains.cordoned,
            cordon_noted: drains.noted,
            drained_since: drains.drained_since,
            drain_blocked: drains.blocked,
        }
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
        // Before K3s, so Longhorn's pods never start here without it.
        if let Err(e) = self.write_storage_attachment(spec) {
            return (self.status(&o, KubePhase::Failed(e)), false);
        }
        if o.role != Some(spec.role) {
            self.applied = None;
            // The UI's kubeSummary reads this detail to say what the host switches to.
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
            let multus = match spec.storage_network {
                Some(_) => write_if_changed(Path::new(MULTUS_MANIFEST), &multus_manifest()),
                None => Ok(()),
            };
            if let Err(e) = multus.and_then(|()| write_if_changed(Path::new(MANIFEST), &longhorn_manifest(spec.storage_network.is_some()))) {
                return (self.status(&o, KubePhase::Failed(format!("write the Longhorn manifests: {e}"))), false);
            }
        }
        if spec.storage_network.is_some() && spec.storage_ip.is_some() {
            self.reattach_longhorn(spec.role);
        }
        if self.applied.as_ref() != Some(spec) {
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
        let (etcd_members, etcd_unread) = if r == KubeRole::Server && active { split(self.etcd_members()) } else { (vec![], None) };
        Observed { role, active, unregistered: ready.err(), node_ready, version, etcd_members, etcd_unread }
    }

    /// etcd's members, from this host's K3s server (`/db/info` answers the
    /// admin on loopback).
    fn etcd_members(&self) -> Result<Vec<EtcdMember>, String> {
        let tls = |f: &str| format!("{SERVER_TLS}/{f}");
        let (ca, cert, key) = (tls("server-ca.crt"), tls("client-admin.crt"), tls("client-admin.key"));
        let info = sh("curl", &["-fsS", "--max-time", "5", "--cacert", &ca, "--cert", &cert, "--key", &key, "https://127.0.0.1:6443/db/info"]);
        info.map_err(|e| e.to_string()).and_then(|i| etcd_members_in(&i))
    }

    /// Removes etcd member `id` through etcd's own gRPC API on loopback
    /// (K3s enables no JSON gateway), with K3s's etcd client certificate;
    /// then checks that etcd no longer lists it. etcd refuses a removal
    /// that would cost it its quorum.
    fn remove_etcd_member(&self, id: u64) -> Result<(), String> {
        let tls = |f: &str| format!("{SERVER_TLS}/etcd/{f}");
        let (ca, cert, key) = (tls("server-ca.crt"), tls("client.crt"), tls("client.key"));
        let body = self.paths.run.join("etcd-member-remove.grpc");
        let _ = std::fs::create_dir_all(&self.paths.run);
        std::fs::write(&body, member_remove_request(id)).map_err(|e| format!("write {}: {e}", body.display()))?;
        let data = format!("@{}", body.display());
        // A refusal comes in the response's headers; success in its trailers.
        let answer = sh("curl", &[
            "-sS", "--http2", "--max-time", "10", "--cacert", &ca, "--cert", &cert, "--key", &key,
            "-H", "content-type: application/grpc", "-H", "te: trailers", "--data-binary", &data, "-o", "/dev/null",
            "-w", "%{http_code} %header{grpc-status} %header{grpc-message}", "https://127.0.0.1:2379/etcdserverpb.Cluster/MemberRemove",
        ]);
        let _ = std::fs::remove_file(&body);
        let answer = answer.map_err(|e| e.to_string())?;
        if self.etcd_members()?.iter().any(|m| m.id == id) {
            return Err(format!("etcd still lists member {id:x} (it answered {})", answer.trim()));
        }
        Ok(())
    }

    /// Writes this host's config of the storage network attachment where
    /// Multus looks for it, or removes it when the spec has none for it.
    fn write_storage_attachment(&self, spec: &KubeSpec) -> Result<(), String> {
        let path = Path::new(MULTUS_CONF_DIR).join(format!("{STORAGE_ATTACHMENT}.conf"));
        let (Some(net), Some(ip)) = (&spec.storage_network, spec.storage_ip) else {
            return match std::fs::remove_file(&path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(format!("remove {}: {e}", path.display())),
                _ => Ok(()),
            };
        };
        let master = LinuxVip::addresses().into_iter().find(|(_, a)| *a == ip).map(|(name, _)| name);
        let master = master.ok_or_else(|| format!("no interface has the storage address {ip}"))?;
        write_if_changed(&path, &storage_attachment(&master, net)).map_err(|e| format!("write {}: {e}", path.display()))
    }

    /// Deletes this node's Longhorn instance managers that run without the
    /// storage network they asked for ([`pods_missing_attachment`]);
    /// Longhorn makes them again, attached.
    fn reattach_longhorn(&self, role: KubeRole) {
        let selector = format!("spec.nodeName={}", self.host);
        let pods = self.kubectl(role, &["-n", "longhorn-system", "get", "pods", "--field-selector", &selector, "-l", "longhorn.io/component=instance-manager", "-o", "json"]);
        let Ok(pods) = pods.and_then(|j| serde_json::from_str::<serde_json::Value>(&j).map_err(|e| e.to_string())) else { return };
        for pod in pods_missing_attachment(&pods) {
            eprintln!("homelabd: kubernetes: {pod} runs without the storage network: deleting it so Longhorn makes it again");
            if let Err(e) = self.kubectl(role, &["-n", "longhorn-system", "delete", "pod", &pod, "--wait=false"]) {
                eprintln!("homelabd: kubernetes: {e}");
            }
        }
    }

    /// Does `admin` with this server's admin access: removes the dead etcd
    /// members asked ([`member_removal`]), never one whose node is `Ready`;
    /// then keeps Longhorn off the disks of the hosts listed, and gives
    /// back the others'.
    fn admin(&mut self, admin: &KubeAdmin) -> Result<(), String> {
        let members = match admin.remove_etcd_members.is_empty() {
            true => vec![],
            // Nothing to find the members in; the disks need no members.
            false => self.etcd_members().unwrap_or_else(|e| {
                eprintln!("homelabd: kubernetes: no etcd member removed: {e}");
                vec![]
            }),
        };
        for m in admin.remove_etcd_members.iter().filter(|m| **m != self.host) {
            let node = match self.kubectl(KubeRole::Server, &["get", "node", m.as_str(), "-o", "json"]) {
                Ok(json) => Some(serde_json::from_str::<serde_json::Value>(&json).map_err(|e| format!("read node {m}: {e}"))?),
                Err(e) if e.contains("NotFound") => None,
                Err(e) => return Err(e),
            };
            match member_removal(&members, m, admin.etcd_peers.get(m).copied(), node.as_ref(), admin.etcd_agents.contains(m)) {
                None => {}
                Some(EtcdRemoval::Keep) => eprintln!("homelabd: kubernetes: not removing {m}'s etcd member: its node is Ready"),
                Some(EtcdRemoval::DeleteNode) => {
                    eprintln!("homelabd: kubernetes: removing {m}'s etcd member (deleting its node)");
                    self.kubectl(KubeRole::Server, &["delete", "node", m.as_str(), "--wait=false"])?;
                }
                Some(EtcdRemoval::ThroughEtcd(id)) => {
                    eprintln!("homelabd: kubernetes: removing {m}'s etcd member {id:x} through etcd (no node, it never started, or its host runs an agent)");
                    self.remove_etcd_member(id)?;
                }
            }
        }
        let json = self.kubectl(KubeRole::Server, &["-n", "longhorn-system", "get", "nodes.longhorn.io", "-o", "json"])?;
        let nodes: serde_json::Value = serde_json::from_str(&json).map_err(|e| format!("read Longhorn's nodes: {e}"))?;
        for (node, patch) in storage_off_patches(&nodes, &admin.storage_off) {
            eprintln!("homelabd: kubernetes: Longhorn disks of {node}: {patch}");
            self.kubectl(KubeRole::Server, &["-n", "longhorn-system", "patch", "nodes.longhorn.io", &node, "--type", "merge", "-p", &patch.to_string()])?;
        }
        self.storage_off.clone_from(&admin.storage_off);
        Ok(())
    }

    /// Cordons and uncordons the nodes of the hosts `admin` lists
    /// ([`cordon_patches`]), then drains the nodes it asks to (`kubectl
    /// drain`, for up to [`DRAIN_TIMEOUT`]), noting which are empty and why
    /// the others are not. A node whose pods cannot be listed is neither:
    /// the leader asks again.
    fn cordon_and_drain(&mut self, admin: &KubeAdmin) -> Result<(), String> {
        let json = self.kubectl(KubeRole::Server, &["get", "nodes", "-o", "json"])?;
        let nodes: serde_json::Value = serde_json::from_str(&json).map_err(|e| format!("read the nodes: {e}"))?;
        let items = || nodes["items"].as_array().into_iter().flatten();
        let mut noted: BTreeSet<HostId> =
            items().filter(|n| !n["metadata"]["annotations"][CORDONED_ANNOTATION].is_null()).filter_map(|n| Some(HostId::from(n["metadata"]["name"].as_str()?))).collect();
        for (node, patch) in cordon_patches(&nodes, &admin.cordon, &admin.uncordon) {
            eprintln!("homelabd: kubernetes: node {node}: {patch}");
            self.kubectl(KubeRole::Server, &["patch", "node", &node, "--type", "merge", "-p", &patch.to_string()])?;
            if patch["spec"]["unschedulable"] == true {
                noted.insert(HostId::from(node.as_str()));
            } else {
                noted.remove(&HostId::from(node.as_str()));
            }
        }
        let has_node = |h: &HostId| items().any(|n| n["metadata"]["name"] == h.as_str());
        let mut drains = Drains { cordoned: admin.cordon.clone(), noted, ..Drains::default() };
        for (host, since) in &admin.drain_since {
            if !has_node(host) {
                drains.drained_since.insert(host.clone(), *since);
                continue;
            }
            let timeout = format!("--timeout={DRAIN_TIMEOUT}");
            let drained = self.kubectl(KubeRole::Server, &["drain", host.as_str(), "--ignore-daemonsets", "--delete-emptydir-data", &timeout]);
            let pods = match self.pods_on(host) {
                Ok(pods) => pods,
                Err(e) => {
                    eprintln!("homelabd: kubernetes: draining {host}: {e}");
                    continue;
                }
            };
            let left = pods_left_to_drain(&pods);
            if left.is_empty() {
                drains.drained_since.insert(host.clone(), *since);
            } else if let Err(e) = drained {
                eprintln!("homelabd: kubernetes: draining {host}: {} left: {e}", left.join(", "));
                drains.blocked.insert(host.clone(), drain_refusal(&e));
            }
        }
        self.drains = drains;
        Ok(())
    }

    /// The pods on `host`'s node (`kubectl get pods -o json`).
    fn pods_on(&self, host: &HostId) -> Result<serde_json::Value, String> {
        let selector = format!("spec.nodeName={host}");
        let pods = self.kubectl(KubeRole::Server, &["get", "pods", "--all-namespaces", "--field-selector", &selector, "-o", "json"])?;
        serde_json::from_str(&pods).map_err(|e| format!("read {host}'s pods: {e}"))
    }

    fn kubectl(&self, role: KubeRole, args: &[&str]) -> Result<String, String> {
        let mut all = vec!["kubectl", "--request-timeout=10s"];
        if role == KubeRole::Agent {
            all.extend(["--kubeconfig", KUBELET_KUBECONFIG]);
        }
        all.extend(args);
        sh(K3S_BIN, &all).map_err(|e| e.to_string())
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

    /// Gives the node `spec`'s labels (dropping homelab labels it no longer
    /// has) and, on a storage host, Longhorn its disks.
    fn label(&self, spec: &KubeSpec) -> Result<(), String> {
        let node = self.host.as_str();
        let json = self.kubectl(spec.role, &["get", "node", node, "-o", "json"])?;
        let current: serde_json::Value = serde_json::from_str(&json).map_err(|e| format!("read node {node}: {e}"))?;
        let mut args: Vec<String> = vec!["label".into(), "node".into(), node.into(), "--overwrite".into()];
        args.extend(spec.labels.iter().map(|(k, v)| format!("{k}={v}")));
        // Homelab labels the host no longer has, and Longhorn's disk label on a host that stores no more.
        let stale = current["metadata"]["labels"].as_object().into_iter().flatten().map(|(k, _)| k).filter(|k| {
            (k.starts_with(control::controllers::kube::LABEL_PREFIX) && !spec.labels.contains_key(*k)) || (*k == LONGHORN_DISK_LABEL && spec.storage_disks.is_empty())
        });
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
            storage_network: None,
            storage_ip: None,
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
        assert!(longhorn_manifest(false).contains(&format!("version: {LONGHORN_VERSION}")));
        assert!(longhorn_manifest(false).contains("createDefaultDiskLabeledNodes: true"));
    }

    #[test]
    fn longhorn_replicates_over_the_storage_network_only_where_it_is() {
        let off = longhorn_manifest(false);
        assert!(!off.contains("storageNetwork") && !off.contains("nodeSelector"), "{off}");
        let on = longhorn_manifest(true);
        assert!(on.contains("      storageNetwork: longhorn-system/homelab-storage\n"), "{on}");
        assert!(on.contains("      systemManagedComponentsNodeSelector: \"homelab.io/storage-network:true\"\n"), "{on}");
        assert!(on.ends_with("    longhornManager:\n      nodeSelector:\n        homelab.io/storage-network: \"true\"\n"), "{on}");
        assert!(on.contains("    persistence:\n      defaultClass: true\n"), "the settings stay inside defaultSettings: {on}");
        let multus = multus_manifest();
        assert!(multus.contains(&format!("chart: rke2-multus\n  version: {MULTUS_CHART_VERSION}\n")), "{multus}");
        assert!(multus.contains("chart: rke2-multus-crd\n") && multus.contains("      enabled: true\n"), "the CRDs and whereabouts: {multus}");
        assert!(multus.ends_with("kind: NetworkAttachmentDefinition\nmetadata:\n  name: homelab-storage\n  namespace: longhorn-system\nspec: {}\n"), "{multus}");
    }

    #[test]
    fn each_host_attaches_longhorn_to_its_own_storage_interface_with_addresses_from_the_pod_range() {
        let net = StorageNetwork { subnet: "10.20.0.0/22".parse().unwrap(), pods: model::network::STORAGE_RESERVED };
        let conf: serde_json::Value = serde_json::from_str(&storage_attachment("bond0.20", &net)).unwrap();
        assert_eq!((conf["name"].as_str(), conf["type"].as_str(), conf["master"].as_str()), (Some(STORAGE_ATTACHMENT), Some("macvlan"), Some("bond0.20")));
        let ipam = &conf["ipam"];
        assert_eq!(ipam["type"], "whereabouts");
        assert_eq!((ipam["range"].as_str(), ipam["range_start"].as_str()), (Some("10.20.0.0/22"), Some("10.20.3.0")));
        assert_eq!(ipam["range_end"], "10.20.3.254", "not the broadcast address");
        let edge = StorageNetwork { subnet: "10.20.0.0/24".parse().unwrap(), pods: model::network::IpRange { first: "10.20.0.0".parse().unwrap(), last: "10.20.0.9".parse().unwrap() } };
        let conf: serde_json::Value = serde_json::from_str(&storage_attachment("eth0.20", &edge)).unwrap();
        assert_eq!(conf["ipam"]["range_start"], "10.20.0.1", "not the network address");
    }

    #[test]
    fn an_instance_manager_that_runs_without_the_storage_network_it_asked_for_is_found() {
        let pod = |name: &str, phase: &str, asked: bool, got: bool| {
            let mut annotations = serde_json::Map::new();
            if asked {
                annotations.insert("k8s.v1.cni.cncf.io/networks".into(), r#"[{"namespace": "longhorn-system", "name": "homelab-storage", "interface": "lhnet1"}]"#.into());
            }
            let status = if got { r#"[{"name":"cbr0","ips":["10.42.4.9"]},{"name":"longhorn-system/homelab-storage","interface":"lhnet1","ips":["10.20.3.4"]}]"# } else { r#"[{"name":"cbr0","ips":["10.42.4.9"]}]"# };
            annotations.insert("k8s.v1.cni.cncf.io/network-status".into(), status.into());
            serde_json::json!({ "metadata": { "name": name, "annotations": annotations }, "status": { "phase": phase } })
        };
        let pods = serde_json::json!({ "items": [
            pod("im-ok", "Running", true, true),
            pod("im-bare", "Running", true, false),
            pod("im-starting", "Pending", true, false),
            pod("im-before", "Running", false, false),
        ] });
        assert_eq!(pods_missing_attachment(&pods), ["im-bare"], "a pod that never asked, or is still starting, is left alone");
    }

    #[test]
    fn etcd_members_are_named_by_node_and_found_by_peer_address_until_they_start() {
        let info = r#"{"members":[{"ID":17713283624853434543,"name":"ms01-01-da0049ba","peerURLs":["https://10.0.0.2:2380"]},{"ID":2,"name":"ms01-02-5e7f1e24"},{"ID":3,"peerURLs":["https://10.0.0.9:2380"],"isLearner":true},{"ID":4,"name":"odd-name"}]}"#;
        let members = etcd_members_in(info).unwrap();
        let ip = |s: &str| Some(s.parse::<Ipv4Addr>().unwrap());
        assert_eq!(members[0], EtcdMember { id: 17713283624853434543, node: Some(HostId::from("ms01-01")), peer: ip("10.0.0.2") }, "IDs are full 64-bit");
        assert_eq!(members[2], EtcdMember { id: 3, node: None, peer: ip("10.0.0.9") }, "a member not started yet has no name");
        let (nodes, unstarted) = by_node_and_peer(&members);
        assert_eq!(nodes.iter().map(|h| h.as_str()).collect::<Vec<_>>(), ["ms01-01", "ms01-02", "odd-name"]);
        assert_eq!(unstarted, [ip("10.0.0.9").unwrap()].into());
        assert!(etcd_members_in("forbidden").is_err());
        assert!(etcd_members_in(r#"{"members":[{"name":"no-id"}]}"#).is_err());
        let ready = serde_json::json!({"status": {"conditions": [{"type": "MemoryPressure", "status": "False"}, {"type": "Ready", "status": "True"}]}});
        assert!(node_ready(&ready));
        let gone = serde_json::json!({"status": {"conditions": [{"type": "Ready", "status": "Unknown"}]}});
        assert!(!node_ready(&gone));
    }

    #[test]
    fn a_member_remove_request_is_framed_for_grpc() {
        assert_eq!(member_remove_request(1), [0, 0, 0, 0, 2, 0x08, 0x01]);
        assert_eq!(member_remove_request(300), [0, 0, 0, 0, 3, 0x08, 0xac, 0x02]);
        let big = member_remove_request(u64::MAX);
        assert_eq!((big.len(), big[4], big[15]), (16, 11, 0x01), "ten varint bytes, the last holding the top bit");
    }

    #[test]
    fn a_dead_member_goes_with_its_node_or_through_etcd_when_k3s_cannot_find_it_by_one() {
        let info = r#"{"members":[{"ID":1,"name":"ms01-01-da0049ba","peerURLs":["https://10.0.0.2:2380"]},{"ID":2,"name":"ms01-03-5e7f1e24","peerURLs":["https://10.0.0.4:2380"]},{"ID":3,"peerURLs":["https://10.0.0.9:2380"]}]}"#;
        let members = etcd_members_in(info).unwrap();
        let ready = serde_json::json!({"status": {"conditions": [{"type": "Ready", "status": "True"}]}});
        let not_ready = serde_json::json!({"status": {"conditions": [{"type": "Ready", "status": "Unknown"}]}});
        let (dead, peer) = (HostId::from("ms01-03"), Some(Ipv4Addr::new(10, 0, 0, 4)));
        assert_eq!(member_removal(&members, &dead, peer, Some(&not_ready), false), Some(EtcdRemoval::DeleteNode));
        assert_eq!(member_removal(&members, &dead, peer, Some(&ready), false), Some(EtcdRemoval::Keep), "never a Ready node's");
        assert_eq!(member_removal(&members, &dead, peer, Some(&ready), true), Some(EtcdRemoval::ThroughEtcd(2)), "an agent's Ready node stays; the member left behind goes");
        assert_eq!(member_removal(&members, &dead, None, None, false), Some(EtcdRemoval::ThroughEtcd(2)), "its node is gone, the member stays");

        // Died joining: its member has no name, only its host's address.
        let joining = HostId::from("ms01-05");
        assert_eq!(member_removal(&members, &joining, Some(Ipv4Addr::new(10, 0, 0, 9)), Some(&not_ready), false), Some(EtcdRemoval::ThroughEtcd(3)), "its old agent node would not lead K3s to it");
        assert_eq!(member_removal(&members, &joining, Some(Ipv4Addr::new(10, 0, 0, 9)), Some(&ready), false), Some(EtcdRemoval::Keep));
        assert_eq!(member_removal(&members, &joining, None, None, false), None, "no address, nothing to find it by");
        assert_eq!(member_removal(&members, &HostId::from("ms01-01"), Some(Ipv4Addr::new(10, 0, 0, 9)), None, false), Some(EtcdRemoval::ThroughEtcd(1)), "by name first");
        assert_eq!(member_removal(&members, &HostId::from("ms01-07"), Some(Ipv4Addr::new(10, 0, 0, 2)), None, false), None, "a started member is found by its name only");
    }

    #[test]
    fn longhorn_is_kept_off_the_disks_of_hosts_that_do_not_store_and_given_them_back() {
        let node = |name: &str, disks: &[(&str, bool)], noted: Option<&str>| {
            let disks: serde_json::Map<String, serde_json::Value> = disks.iter().map(|(d, on)| (d.to_string(), serde_json::json!({ "allowScheduling": on, "path": format!("/mnt/{d}") }))).collect();
            let annotations = noted.map(|n| serde_json::json!({ STORAGE_OFF_ANNOTATION: n })).unwrap_or(serde_json::json!({}));
            serde_json::json!({ "metadata": { "name": name, "annotations": annotations }, "spec": { "disks": disks } })
        };
        let nodes = serde_json::json!({ "items": [
            node("ms01-01", &[("nvme0", true), ("nvme1", true)], None),
            node("ms01-05", &[("nvme0", true), ("nvme1", false)], None),
            node("ms01-06", &[("nvme0", false)], Some("nvme0")),
            node("minipc-01", &[("nvme0", false)], None),
        ] });
        let off: BTreeSet<HostId> = [HostId::from("ms01-05"), HostId::from("minipc-01")].into();
        let patches = storage_off_patches(&nodes, &off);
        assert_eq!(patches.len(), 2, "{patches:?}");
        assert_eq!(patches[0].0, "ms01-05");
        assert_eq!(patches[0].1, serde_json::json!({ "metadata": { "annotations": { STORAGE_OFF_ANNOTATION: "nvme0" } }, "spec": { "disks": { "nvme0": { "allowScheduling": false } } } }), "a disk turned off by hand is not noted");
        assert_eq!(patches[1].0, "ms01-06", "it stores again: its disk comes back");
        assert_eq!(patches[1].1, serde_json::json!({ "metadata": { "annotations": { STORAGE_OFF_ANNOTATION: null } }, "spec": { "disks": { "nvme0": { "allowScheduling": true } } } }));
        let done = serde_json::json!({ "items": [node("ms01-05", &[("nvme0", false), ("nvme1", false)], Some("nvme0"))] });
        assert!(storage_off_patches(&done, &off).is_empty(), "nothing left to do");
    }

    #[test]
    fn nodes_are_cordoned_for_their_hosts_and_only_those_cordoned_so_are_uncordoned_unless_removed() {
        let node = |name: &str, unschedulable: bool, noted: bool| {
            let annotations = if noted { serde_json::json!({ CORDONED_ANNOTATION: "true" }) } else { serde_json::json!({}) };
            serde_json::json!({ "metadata": { "name": name, "annotations": annotations }, "spec": { "unschedulable": unschedulable } })
        };
        let nodes = serde_json::json!({ "items": [
            node("ms01-01", false, false),
            node("ms01-02", true, true),
            node("ms01-03", true, false),
            node("ms01-04", true, true),
            node("ms01-05", true, true),
        ] });
        let hosts = |ids: &[&str]| -> BTreeSet<HostId> { ids.iter().map(|id| HostId::from(*id)).collect() };
        let patches = cordon_patches(&nodes, &hosts(&["ms01-01", "ms01-04"]), &hosts(&["ms01-02", "ms01-03"]));
        let cordoned = serde_json::json!({ "metadata": { "annotations": { CORDONED_ANNOTATION: "true" } }, "spec": { "unschedulable": true } });
        let uncordoned = serde_json::json!({ "metadata": { "annotations": { CORDONED_ANNOTATION: null } }, "spec": { "unschedulable": null } });
        assert_eq!(
            patches,
            [("ms01-01".to_string(), cordoned), ("ms01-02".to_string(), uncordoned)],
            "ms01-03 was cordoned by hand; ms01-04 is already; ms01-05's host was removed, so it stays cordoned"
        );
    }

    #[test]
    fn a_drain_leaves_daemonset_static_and_finished_pods() {
        let pod = |name: &str, owner: Option<&str>, mirror: bool, phase: &str| {
            let owners: Vec<serde_json::Value> = owner.into_iter().map(|k| serde_json::json!({ "kind": k, "name": "x" })).collect();
            let annotations = if mirror { serde_json::json!({ "kubernetes.io/config.mirror": "abc" }) } else { serde_json::json!({}) };
            serde_json::json!({ "metadata": { "name": name, "namespace": "demo", "ownerReferences": owners, "annotations": annotations }, "status": { "phase": phase } })
        };
        let pods = serde_json::json!({ "items": [
            pod("web-1", Some("ReplicaSet"), false, "Running"),
            pod("longhorn-manager-x", Some("DaemonSet"), false, "Running"),
            pod("static", None, true, "Running"),
            pod("job-1", Some("Job"), false, "Succeeded"),
            pod("postgres-0", Some("StatefulSet"), false, "Pending"),
        ] });
        assert_eq!(pods_left_to_drain(&pods), ["demo/web-1", "demo/postgres-0"]);
        assert!(pods_left_to_drain(&serde_json::json!({ "items": [] })).is_empty());
    }

    #[test]
    fn a_refused_drain_says_which_eviction_kubernetes_refused() {
        let err = "k3s kubectl drain ms01-02: evicting pod demo/postgres-0\nerror when evicting pods/\"postgres-0\" -n \"demo\" (will retry after 5s): Cannot evict pod as it would violate the pod's disruption budget.\nerror: unable to drain node \"ms01-02\" due to error: global timeout reached: 30s";
        assert_eq!(drain_refusal(err), "error when evicting pods/\"postgres-0\" -n \"demo\" (will retry after 5s): Cannot evict pod as it would violate the pod's disruption budget.");
        assert_eq!(drain_refusal("k3s kubectl drain x: \nerror: timed out\n"), "error: timed out");
    }
}
