//! What Kubernetes runs, for the UI's Workloads tab: the workloads, their
//! pods and where they run, and the Longhorn volumes with their replicas.
//! Read from the K3s API through the VIP with an admin kubeconfig, every
//! few seconds, the way `kubectl get` would.

use crate::kube_detail;
use crate::kube_spec::{self, ContainerSpec, ContainerState, Pdb, PodNetwork, Revision};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use ts_rs::TS;

/// The pause between reads.
pub const POLL_EVERY: Duration = Duration::from_secs(3);
/// The pause before asking for a kubeconfig again, when there is none yet.
pub const KUBECONFIG_RETRY: Duration = Duration::from_secs(10);
/// Failed reads in a row after which the kubeconfig is fetched again (the
/// cluster may have been reinstalled, with a new CA).
pub const FAILURES_BEFORE_REFRESH: u32 = 5;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How far back Warning events are shown.
const EVENTS_FOR_MS: u64 = 60 * 60 * 1000;
/// The most Warning events kept.
const MAX_EVENTS: usize = 200;

/// What Kubernetes runs, as of `at`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct KubeView {
    /// When it was last read (Unix ms); 0 if it never was.
    #[ts(type = "number")]
    pub at: u64,
    /// Why the last read failed. The lists are from the last read that worked.
    pub error: Option<String>,
    pub workloads: Vec<KubeWorkload>,
    pub pods: Vec<KubePod>,
    pub volumes: Vec<LonghornVolume>,
    /// Warning events of the last hour on workloads and their pods, newest first.
    pub events: Vec<KubeEvent>,
    /// Why there is no CPU and memory usage: metrics-server did not answer.
    pub metrics_error: Option<String>,
}

/// The kinds of workload the Workloads tab shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    DaemonSet,
}

impl WorkloadKind {
    /// Its resource in the `apps/v1` API.
    pub fn resource(self) -> &'static str {
        match self {
            WorkloadKind::Deployment => "deployments",
            WorkloadKind::StatefulSet => "statefulsets",
            WorkloadKind::DaemonSet => "daemonsets",
        }
    }
}

impl std::fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            WorkloadKind::Deployment => "Deployment",
            WorkloadKind::StatefulSet => "StatefulSet",
            WorkloadKind::DaemonSet => "DaemonSet",
        })
    }
}

/// A Deployment, StatefulSet or DaemonSet.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct KubeWorkload {
    pub namespace: String,
    pub name: String,
    pub kind: WorkloadKind,
    pub desired: u32,
    pub ready: u32,
    /// Pods (or for a DaemonSet, nodes) already on the latest spec.
    pub updated: u32,
    pub health: Health,
    /// Usage summed over the pods metrics-server has measured (a new pod has
    /// no usage yet for a minute); none without it, or before any is measured.
    #[ts(type = "number | null")]
    pub cpu_milli: Option<u64>,
    #[ts(type = "number | null")]
    pub memory_bytes: Option<u64>,
    /// Limits summed over its pods; none if a pod has none.
    #[ts(type = "number | null")]
    pub cpu_limit_milli: Option<u64>,
    #[ts(type = "number | null")]
    pub memory_limit_bytes: Option<u64>,
    /// Its pod template's container images.
    pub images: Vec<String>,
    /// How to reach it, Ingresses first.
    pub endpoints: Vec<Endpoint>,
    /// Its controller's conditions that are not as they should be.
    pub conditions: Vec<KubeCondition>,
    /// Its pod template's containers, init containers first.
    pub containers: Vec<ContainerSpec>,
    /// What its pods share of their host's: `host network`, `host PIDs`.
    pub host_access: Vec<String>,
    /// Its rollout history, newest first.
    pub revisions: Vec<Revision>,
    /// The PodDisruptionBudget over its pods, if any.
    pub pdb: Option<Pdb>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct Health {
    pub state: HealthState,
    /// Why, in a few words: `CrashLoopBackOff (2 pods)`, `1/3 updated`…
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub enum HealthState {
    Healthy,
    RollingOut,
    Degraded,
    Down,
    /// Scaled to 0.
    Idle,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct Endpoint {
    /// `http://10.0.3.253/app`, `10.0.3.253:8096`, `web.demo:80`.
    pub address: String,
    /// The URL to open, when it is likely a web page.
    pub link: Option<String>,
    pub kind: EndpointKind,
    /// What makes it: `Ingress web`, `Service web`.
    pub via: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub enum EndpointKind {
    Ingress,
    LoadBalancer,
    NodePort,
    /// Only from inside the cluster.
    Cluster,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct KubeCondition {
    /// Its type: `Available`, `Progressing`, `ReplicaFailure`.
    pub kind: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
}

/// How a container last ended.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct Termination {
    /// `OOMKilled`, `Error`, `Completed`…
    pub reason: String,
    pub exit_code: i32,
    #[ts(type = "number | null")]
    pub at: Option<u64>,
}

/// A Warning event.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct KubeEvent {
    pub namespace: String,
    /// The workload it concerns, as `<kind>/<name>`, if any.
    pub workload: Option<String>,
    /// What it is about, as `<kind>/<name>`.
    pub object: String,
    pub reason: String,
    pub message: String,
    pub count: u32,
    /// When it last happened (Unix ms).
    #[ts(type = "number | null")]
    pub last: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct KubePod {
    pub namespace: String,
    pub name: String,
    /// The workload it belongs to, as `<kind>/<name>` (a Deployment, not its ReplicaSet).
    pub owner: Option<String>,
    /// The node (= host) it is scheduled on.
    pub node: Option<String>,
    /// As kubectl's STATUS column: the phase, or why it is not running
    /// (`ContainerCreating`, `CrashLoopBackOff`, `Terminating`…).
    pub status: String,
    pub ready: u32,
    pub containers: u32,
    pub restarts: u32,
    /// When it was created (Unix ms).
    #[ts(type = "number | null")]
    pub created: Option<u64>,
    #[ts(type = "number | null")]
    pub cpu_milli: Option<u64>,
    #[ts(type = "number | null")]
    pub memory_bytes: Option<u64>,
    #[ts(type = "number | null")]
    pub cpu_limit_milli: Option<u64>,
    #[ts(type = "number | null")]
    pub memory_limit_bytes: Option<u64>,
    /// Why it is as it is, in Kubernetes' words: the scheduler's message, or a waiting container's.
    pub detail: Option<String>,
    /// The scheduler cannot place it.
    pub unschedulable: bool,
    /// How a container that restarted last ended.
    pub last_termination: Option<Termination>,
    pub ips: Vec<String>,
    /// The networks Multus attached it to, the pod network included; none without Multus.
    pub networks: Vec<PodNetwork>,
    /// `Guaranteed`, `Burstable` or `BestEffort`.
    pub qos: Option<String>,
    /// Each container, init containers first.
    pub container_states: Vec<ContainerState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct LonghornVolume {
    pub name: String,
    /// The claim it backs, as `<namespace>/<name>`.
    pub pvc: Option<String>,
    /// The pods using it.
    pub pods: Vec<String>,
    #[ts(type = "number")]
    pub size_bytes: u64,
    pub replicas_wanted: u32,
    /// `attached`, `detached`, `attaching`…
    pub state: String,
    /// `healthy`, `degraded`, `faulted` or `unknown`.
    pub robustness: String,
    /// The node it is attached to (where its engine runs).
    pub node: Option<String>,
    pub replicas: Vec<LonghornReplica>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct LonghornReplica {
    pub name: String,
    pub node: String,
    pub disk: String,
    /// What the replica's process is doing: `running`, `stopped`, `error`…
    pub state: String,
    /// What the engine makes of it: `RW` (in sync), `WO` (rebuilding) or
    /// `ERR`; none when the volume is detached.
    pub mode: Option<String>,
    /// It failed, and Longhorn will not use its data again.
    pub failed: bool,
}

/// The raw lists one read fetches.
#[derive(Debug)]
pub struct Lists {
    pub pods: Value,
    pub replicasets: Value,
    pub deployments: Value,
    pub statefulsets: Value,
    pub daemonsets: Value,
    pub volumes: Value,
    pub replicas: Value,
    pub engines: Value,
    pub services: Value,
    pub ingresses: Value,
    pub events: Value,
    pub controllerrevisions: Value,
    pub pdbs: Value,
    /// Pod usage from metrics-server, or why there is none.
    pub metrics: Result<Value, String>,
}

impl Default for Lists {
    fn default() -> Self {
        let v = Value::Null;
        Self { pods: v.clone(), replicasets: v.clone(), deployments: v.clone(), statefulsets: v.clone(), daemonsets: v.clone(), volumes: v.clone(), replicas: v.clone(), engines: v.clone(), services: v.clone(), ingresses: v.clone(), events: v.clone(), controllerrevisions: v.clone(), pdbs: v, metrics: Ok(Value::Null) }
    }
}

/// How to reach the API: its URL and a client holding the kubeconfig's CA and client certificate.
pub struct Access {
    server: String,
    client: reqwest::Client,
}

/// A base64 field of the kubeconfig (`homelabd kubeconfig` writes the one
/// cluster and user K3s generates, with the certificates inline).
fn field<'a>(kubeconfig: &'a str, key: &str) -> Option<&'a str> {
    kubeconfig.lines().find_map(|l| l.trim().strip_prefix(key)?.strip_prefix(':')).map(str::trim)
}

fn decoded(kubeconfig: &str, key: &str) -> Result<Vec<u8>, String> {
    let b64 = field(kubeconfig, key).ok_or_else(|| format!("the kubeconfig has no {key}"))?;
    base64::engine::general_purpose::STANDARD.decode(b64).map_err(|e| format!("the kubeconfig's {key}: {e}"))
}

impl Access {
    pub fn from_kubeconfig(kubeconfig: &str) -> Result<Self, String> {
        let server = field(kubeconfig, "server").ok_or("the kubeconfig has no server")?.trim_end_matches('/').to_string();
        let ca = reqwest::Certificate::from_pem(&decoded(kubeconfig, "certificate-authority-data")?).map_err(|e| e.to_string())?;
        let mut identity = decoded(kubeconfig, "client-certificate-data")?;
        identity.push(b'\n');
        identity.extend(decoded(kubeconfig, "client-key-data")?);
        let identity = reqwest::Identity::from_pem(&identity).map_err(|e| e.to_string())?;
        let client = reqwest::Client::builder()
            .tls_certs_only([ca])
            .identity(identity)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self { server, client })
    }

    /// The list at `path`; an empty one if the API does not have it (Longhorn not installed yet).
    async fn list(&self, path: &str) -> Result<Value, ReadError> {
        let res = self.client.get(format!("{}{path}", self.server)).send().await.map_err(|e| ReadError::Failed(format!("{path}: {e}")))?;
        match res.status() {
            reqwest::StatusCode::NOT_FOUND => Ok(Value::Null),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => Err(ReadError::Refused(format!("{path}: {}", res.status()))),
            s if !s.is_success() => Err(ReadError::Failed(format!("{path}: {s}"))),
            _ => res.json().await.map_err(|e| ReadError::Failed(format!("{path}: {e}"))),
        }
    }

    /// Everything the view is built from, fetched at once.
    /// Sends a `method` request to `path` (with `body` as `(content type, JSON)`):
    /// the API's answer, or its own words for why not (a Kubernetes `Status`'s
    /// message, as kubectl prints it).
    pub async fn send(&self, method: reqwest::Method, path: &str, body: Option<(&str, &Value)>) -> Result<Value, SendError> {
        let mut req = self.client.request(method, format!("{}{path}", self.server));
        if let Some((content_type, body)) = body {
            req = req.header(reqwest::header::CONTENT_TYPE, content_type).body(body.to_string());
        }
        let res = req.send().await.map_err(|e| SendError::Unreachable(e.to_string()))?;
        let status = res.status();
        let answer: Value = res.json().await.unwrap_or(Value::Null);
        if status.is_success() {
            return Ok(answer);
        }
        let message = text(&answer["message"]).unwrap_or_else(|| status.to_string());
        Err(if status.is_server_error() { SendError::Unreachable(message) } else { SendError::Refused { status: status.as_u16(), message, status_body: answer } })
    }

    /// The cluster's own address, as the kubeconfig reaches it (the VIP): `10.0.3.253`.
    pub fn host(&self) -> &str {
        let rest = self.server.split_once("://").map_or(self.server.as_str(), |(_, r)| r);
        let authority = rest.split('/').next().unwrap_or(rest);
        authority.rsplit_once(':').map_or(authority, |(h, _)| h)
    }

    pub async fn read(&self) -> Result<Lists, ReadError> {
        // metrics-server is an add-on: without it, everything else still shows.
        let metrics = async {
            match self.client.get(format!("{}/apis/metrics.k8s.io/v1beta1/pods", self.server)).send().await {
                Ok(r) if r.status().is_success() => r.json::<Value>().await.map_err(|e| e.to_string()),
                Ok(r) => Err(format!("metrics-server: {}", r.status())),
                Err(e) => Err(format!("metrics-server: {e}")),
            }
        };
        let (pods, replicasets, deployments, statefulsets, daemonsets, volumes, replicas, engines, services, ingresses, events, controllerrevisions, pdbs, metrics) = tokio::join!(
            self.list("/api/v1/pods"),
            self.list("/apis/apps/v1/replicasets"),
            self.list("/apis/apps/v1/deployments"),
            self.list("/apis/apps/v1/statefulsets"),
            self.list("/apis/apps/v1/daemonsets"),
            self.list("/apis/longhorn.io/v1beta2/namespaces/longhorn-system/volumes"),
            self.list("/apis/longhorn.io/v1beta2/namespaces/longhorn-system/replicas"),
            self.list("/apis/longhorn.io/v1beta2/namespaces/longhorn-system/engines"),
            self.list("/api/v1/services"),
            self.list("/apis/networking.k8s.io/v1/ingresses"),
            self.list("/api/v1/events?fieldSelector=type%3DWarning"),
            self.list("/apis/apps/v1/controllerrevisions"),
            self.list("/apis/policy/v1/poddisruptionbudgets"),
            metrics,
        );
        Ok(Lists {
            pods: pods?,
            replicasets: replicasets?,
            deployments: deployments?,
            statefulsets: statefulsets?,
            daemonsets: daemonsets?,
            volumes: volumes?,
            replicas: replicas?,
            engines: engines?,
            services: services?,
            ingresses: ingresses?,
            events: events?,
            controllerrevisions: controllerrevisions?,
            pdbs: pdbs?,
            metrics,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReadError {
    /// The API turned the credentials down: fetch the kubeconfig again.
    Refused(String),
    Failed(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Refused(e) | ReadError::Failed(e) => f.write_str(e),
        }
    }
}

/// Why the API did not do what was sent.
#[derive(Debug, Clone, PartialEq)]
pub enum SendError {
    /// The API answered no (4xx), saying why: its `Status`'s message, and the
    /// whole `Status` (a conflict's details are in it).
    Refused { status: u16, message: String, status_body: Value },
    /// No answer, or the API failed (5xx).
    Unreachable(String),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Refused { message, .. } | SendError::Unreachable(message) => f.write_str(message),
        }
    }
}

fn items(list: &Value) -> &[Value] {
    list["items"].as_array().map_or(&[], Vec::as_slice)
}

fn text(v: &Value) -> Option<String> {
    v.as_str().filter(|s| !s.is_empty()).map(String::from)
}

fn count(v: &Value) -> u32 {
    v.as_u64().unwrap_or(0) as u32
}

/// Unix ms of a Kubernetes timestamp (RFC 3339 in UTC: `2026-10-27T14:00:45Z`).
fn timestamp_ms(t: &str) -> Option<u64> {
    let b = t.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[10] != b'T' || !t.ends_with('Z') {
        return None;
    }
    let num = |r: std::ops::Range<usize>| t.get(r)?.parse::<i64>().ok();
    let (y, m, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let secs = num(11..13)? * 3600 + num(14..16)? * 60 + num(17..19)?;
    // Days since 1970-01-01, by Howard Hinnant's days_from_civil.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    u64::try_from((days * 86_400 + secs) * 1000).ok()
}

/// The Kubernetes timestamp (RFC 3339 in UTC) of Unix ms `ms`, to the second.
pub fn rfc3339(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Howard Hinnant's civil_from_days, the inverse of `timestamp_ms`'s.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem / 60 % 60, rem % 60)
}

/// kubectl's STATUS for a pod: being deleted, or the first container's
/// reason for not running, or the pod's reason (`Evicted`), or its phase.
fn pod_status(pod: &Value) -> String {
    if pod["metadata"]["deletionTimestamp"].is_string() {
        return "Terminating".into();
    }
    let st = &pod["status"];
    let containers = st["initContainerStatuses"].as_array().into_iter().flatten().chain(st["containerStatuses"].as_array().into_iter().flatten());
    for c in containers {
        let state = &c["state"];
        if let Some(r) = text(&state["waiting"]["reason"]) {
            return r;
        }
        let finished = st["phase"].as_str().is_some_and(|p| p == "Succeeded" || p == "Failed");
        if let Some(r) = text(&state["terminated"]["reason"]).filter(|_| !finished) {
            return r;
        }
    }
    text(&st["reason"]).or_else(|| text(&st["phase"])).unwrap_or_else(|| "Unknown".into())
}

/// The controller in `meta`'s owner references, as `<kind>/<name>`.
fn controller(meta: &Value) -> Option<(String, String)> {
    let refs = meta["ownerReferences"].as_array()?;
    let r = refs.iter().find(|r| r["controller"].as_bool() == Some(true)).or(refs.first())?;
    Some((text(&r["kind"])?, text(&r["name"])?))
}

fn key(meta: &Value) -> (String, String) {
    (text(&meta["namespace"]).unwrap_or_default(), text(&meta["name"]).unwrap_or_default())
}

/// The view `lists` describe, read at `at`; `vip` is the cluster's own address, for endpoints.
pub fn view_of(lists: &Lists, at: u64, vip: &str) -> KubeView {
    // A Deployment's pods belong to a ReplicaSet it owns.
    let rs_owner: BTreeMap<(String, String), (String, String)> =
        items(&lists.replicasets).iter().filter_map(|rs| Some((key(&rs["metadata"]), controller(&rs["metadata"])?))).collect();

    let usage = lists.metrics.as_ref().map(kube_detail::pod_usage).unwrap_or_default();
    let mut pods: Vec<KubePod> = items(&lists.pods)
        .iter()
        .map(|p| {
            let meta = &p["metadata"];
            let (namespace, name) = key(meta);
            let owner = controller(meta).map(|(kind, owner)| match kind.as_str() {
                "ReplicaSet" => match rs_owner.get(&(namespace.clone(), owner.clone())) {
                    Some((k, n)) => format!("{k}/{n}"),
                    None => format!("{kind}/{owner}"),
                },
                _ => format!("{kind}/{owner}"),
            });
            let statuses = p["status"]["containerStatuses"].as_array().map_or(&[][..], Vec::as_slice);
            let containers = p["spec"]["containers"].as_array().map_or(statuses.len(), Vec::len) as u32;
            let scheduled = p["status"]["conditions"].as_array().into_iter().flatten().find(|c| c["type"] == "PodScheduled");
            let unschedulable = scheduled.is_some_and(|c| c["status"] == "False" && c["reason"] == "Unschedulable");
            let waiting = statuses.iter().find_map(|c| text(&c["state"]["waiting"]["message"]));
            let detail = if unschedulable { scheduled.and_then(|c| text(&c["message"])) } else { waiting };
            let last_termination = statuses.iter().find_map(|c| {
                let t = &c["lastState"]["terminated"];
                Some(Termination { reason: text(&t["reason"])?, exit_code: t["exitCode"].as_i64().unwrap_or(0) as i32, at: t["finishedAt"].as_str().and_then(timestamp_ms) })
            });
            let used = usage.get(&(namespace.clone(), name.clone()));
            let (cpu_limit_milli, memory_limit_bytes) = kube_detail::pod_limits(p);
            KubePod {
                owner,
                node: text(&p["spec"]["nodeName"]),
                status: pod_status(p),
                ready: statuses.iter().filter(|c| c["ready"].as_bool() == Some(true)).count() as u32,
                containers,
                restarts: statuses.iter().map(|c| count(&c["restartCount"])).sum(),
                created: meta["creationTimestamp"].as_str().and_then(timestamp_ms),
                cpu_milli: used.map(|u| u.0),
                memory_bytes: used.map(|u| u.1),
                cpu_limit_milli,
                memory_limit_bytes,
                detail,
                unschedulable,
                last_termination,
                ips: kube_spec::pod_ips(p),
                networks: kube_spec::pod_networks(p),
                qos: text(&p["status"]["qosClass"]),
                container_states: kube_spec::container_states(p),
                namespace,
                name,
            }
        })
        .collect();
    pods.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    let metrics_ok = lists.metrics.is_ok();
    let mut workloads: Vec<KubeWorkload> = Vec::new();
    for (list, kind) in [(&lists.deployments, WorkloadKind::Deployment), (&lists.statefulsets, WorkloadKind::StatefulSet), (&lists.daemonsets, WorkloadKind::DaemonSet)] {
        for w in items(list) {
            let (namespace, name) = key(&w["metadata"]);
            let st = &w["status"];
            let (desired, ready, updated) = match kind {
                WorkloadKind::DaemonSet => (count(&st["desiredNumberScheduled"]), count(&st["numberReady"]), count(&st["updatedNumberScheduled"])),
                _ => (w["spec"]["replicas"].as_u64().map_or(1, |n| n as u32), count(&st["readyReplicas"]), count(&st["updatedReplicas"])),
            };
            let conditions: Vec<KubeCondition> = st["conditions"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| KubeCondition { kind: text(&c["type"]).unwrap_or_default(), status: text(&c["status"]).unwrap_or_default(), reason: text(&c["reason"]), message: text(&c["message"]) })
                .filter(|c| if c.kind == "ReplicaFailure" { c.status == "True" } else { c.status != "True" })
                .collect();
            let controller_problem = conditions.iter().find_map(|c| match (c.kind.as_str(), c.reason.as_deref()) {
                ("ReplicaFailure", _) => Some(format!("ReplicaFailure: {}", c.message.as_deref().unwrap_or("pods cannot be created"))),
                ("Progressing", Some("ProgressDeadlineExceeded")) => Some("rollout stuck (ProgressDeadlineExceeded)".into()),
                _ => None,
            });
            let owner = format!("{kind}/{name}");
            let mine: Vec<&KubePod> = pods.iter().filter(|p| p.namespace == namespace && p.owner.as_deref() == Some(owner.as_str())).collect();
            let health = kube_detail::health(&kube_detail::HealthInput {
                desired,
                ready,
                updated,
                spec_seen: st["observedGeneration"].as_u64().unwrap_or(0) >= w["metadata"]["generation"].as_u64().unwrap_or(0),
                controller_problem,
                pods: mine.clone(),
            });
            // Limits need every pod's (one without is unlimited); usage counts the pods measured so far.
            let sum = |f: fn(&KubePod) -> Option<u64>| -> Option<u64> { if mine.is_empty() { None } else { mine.iter().map(|p| f(p)).sum() } };
            let used = |f: fn(&KubePod) -> Option<u64>| -> Option<u64> { mine.iter().filter_map(|p| f(p)).reduce(|a, b| a + b) };
            let template = &w["spec"]["template"];
            workloads.push(KubeWorkload {
                kind,
                desired,
                ready,
                updated,
                health,
                cpu_milli: if metrics_ok { used(|p| p.cpu_milli) } else { None },
                memory_bytes: if metrics_ok { used(|p| p.memory_bytes) } else { None },
                cpu_limit_milli: sum(|p| p.cpu_limit_milli),
                memory_limit_bytes: sum(|p| p.memory_limit_bytes),
                images: template["spec"]["containers"].as_array().into_iter().flatten().filter_map(|c| text(&c["image"])).collect(),
                endpoints: kube_detail::endpoints(&namespace, &template["metadata"]["labels"], &lists.services, &lists.ingresses, vip),
                conditions,
                containers: kube_spec::containers(&template["spec"], &kube_spec::claim_templates(w)),
                host_access: kube_spec::host_access(&template["spec"]),
                revisions: kube_spec::revisions(w, &kind.to_string(), &lists.replicasets, &lists.controllerrevisions, timestamp_ms),
                pdb: kube_spec::pdb(&namespace, &template["metadata"]["labels"], &lists.pdbs),
                namespace,
                name,
            });
        }
    }
    workloads.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    // Warning events, matched to the workload they concern.
    let pod_owner: BTreeMap<(&str, &str), &str> = pods.iter().filter_map(|p| Some(((p.namespace.as_str(), p.name.as_str()), p.owner.as_deref()?))).collect();
    let mut events: Vec<KubeEvent> = items(&lists.events)
        .iter()
        .filter_map(|e| {
            let obj = &e["involvedObject"];
            let (namespace, kind, name) = (text(&obj["namespace"])?, text(&obj["kind"])?, text(&obj["name"])?);
            let workload = match kind.as_str() {
                "Pod" => pod_owner.get(&(namespace.as_str(), name.as_str())).map(|o| o.to_string()),
                "ReplicaSet" => rs_owner.get(&(namespace.clone(), name.clone())).map(|(k, n)| format!("{k}/{n}")),
                "Deployment" | "StatefulSet" | "DaemonSet" => Some(format!("{kind}/{name}")),
                _ => None,
            };
            let last = [&e["lastTimestamp"], &e["series"]["lastObservedTime"], &e["eventTime"], &e["metadata"]["creationTimestamp"]].into_iter().find_map(|t| t.as_str().and_then(timestamp_ms));
            Some(KubeEvent {
                object: format!("{kind}/{name}"),
                namespace,
                workload,
                reason: text(&e["reason"]).unwrap_or_default(),
                message: text(&e["message"]).unwrap_or_default(),
                count: e["count"].as_u64().or_else(|| e["series"]["count"].as_u64()).map_or(1, |n| n as u32),
                last,
            })
        })
        .filter(|e| e.last.is_none_or(|t| t + EVENTS_FOR_MS >= at))
        .collect();
    events.sort_by_key(|e| std::cmp::Reverse(e.last));
    events.truncate(MAX_EVENTS);

    // The engine of each volume knows which replicas it is using, and how.
    let modes: BTreeMap<String, String> = items(&lists.engines)
        .iter()
        .flat_map(|e| e["status"]["replicaModeMap"].as_object().into_iter().flatten())
        .filter_map(|(replica, mode)| Some((replica.clone(), text(mode)?)))
        .collect();
    let mut replicas: BTreeMap<String, Vec<LonghornReplica>> = BTreeMap::new();
    for r in items(&lists.replicas) {
        let Some(volume) = text(&r["spec"]["volumeName"]) else { continue };
        let name = text(&r["metadata"]["name"]).unwrap_or_default();
        replicas.entry(volume).or_default().push(LonghornReplica {
            mode: modes.get(&name).cloned(),
            node: text(&r["spec"]["nodeID"]).unwrap_or_default(),
            disk: text(&r["spec"]["diskPath"]).unwrap_or_default(),
            state: text(&r["status"]["currentState"]).unwrap_or_else(|| "unknown".into()),
            failed: text(&r["spec"]["failedAt"]).is_some(),
            name,
        });
    }
    let mut volumes: Vec<LonghornVolume> = items(&lists.volumes)
        .iter()
        .map(|v| {
            let name = text(&v["metadata"]["name"]).unwrap_or_default();
            let k8s = &v["status"]["kubernetesStatus"];
            let pvc = text(&k8s["pvcName"]).map(|c| format!("{}/{c}", k8s["namespace"].as_str().unwrap_or_default()));
            let mut reps = replicas.remove(&name).unwrap_or_default();
            reps.sort_by(|a, b| a.node.cmp(&b.node));
            LonghornVolume {
                pvc,
                pods: k8s["workloadsStatus"].as_array().into_iter().flatten().filter_map(|w| text(&w["podName"])).collect(),
                size_bytes: v["spec"]["size"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0),
                replicas_wanted: count(&v["spec"]["numberOfReplicas"]),
                state: text(&v["status"]["state"]).unwrap_or_else(|| "unknown".into()),
                robustness: text(&v["status"]["robustness"]).unwrap_or_else(|| "unknown".into()),
                node: text(&v["status"]["currentNodeID"]),
                replicas: reps,
                name,
            }
        })
        .collect();
    volumes.sort_by(|a, b| a.pvc.cmp(&b.pvc).then_with(|| a.name.cmp(&b.name)));

    KubeView { at, error: None, workloads, pods, volumes, events, metrics_error: lists.metrics.as_ref().err().cloned() }
}

/// Keeps `tx` up to date with what Kubernetes runs, reading it with the
/// kubeconfig `kubeconfig` gives (asked again whenever the API refuses it
/// or keeps failing). A failed read keeps the last lists and says why.
/// `now` cuts the pause between reads short (after a change from the UI).
pub async fn watch<F, Fut>(kubeconfig: F, tx: watch::Sender<Arc<KubeView>>, now: Arc<Notify>)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<String, String>>,
{
    let mut access: Option<Access> = None;
    let mut failures = 0;
    loop {
        let Some(a) = &access else {
            match kubeconfig().await.and_then(|kc| Access::from_kubeconfig(&kc)) {
                Ok(a) => access = Some(a),
                Err(e) => {
                    let last = tx.borrow().as_ref().clone();
                    tx.send_replace(Arc::new(KubeView { error: Some(format!("no kubeconfig yet: {e}")), ..last }));
                    tokio::time::sleep(KUBECONFIG_RETRY).await;
                }
            }
            continue;
        };
        match a.read().await {
            Ok(lists) => {
                failures = 0;
                tx.send_replace(Arc::new(view_of(&lists, node::runtime::wall_ms(), a.host())));
            }
            Err(e) => {
                failures += 1;
                if matches!(e, ReadError::Refused(_)) || failures >= FAILURES_BEFORE_REFRESH {
                    access = None;
                    failures = 0;
                }
                let last = tx.borrow().as_ref().clone();
                tx.send_replace(Arc::new(KubeView { error: Some(e.to_string()), ..last }));
            }
        }
        tokio::select! {
            () = tokio::time::sleep(POLL_EVERY) => {}
            () = now.notified() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn list(items: Vec<Value>) -> Value {
        json!({ "items": items })
    }

    #[test]
    fn timestamps_are_read_as_unix_ms() {
        assert_eq!(timestamp_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(timestamp_ms("2026-10-27T14:00:45Z"), Some(1_793_109_645_000));
        assert_eq!(timestamp_ms("2024-02-29T23:59:59Z"), Some(1_709_251_199_000));
        assert_eq!(timestamp_ms("yesterday"), None);
    }

    #[test]
    fn unix_ms_are_written_as_timestamps() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_793_109_645_999), "2026-10-27T14:00:45Z");
        for t in ["2024-02-29T23:59:59Z", "2000-03-01T00:00:00Z", "2026-12-31T08:09:10Z"] {
            assert_eq!(rfc3339(timestamp_ms(t).unwrap()), t);
        }
    }

    #[test]
    fn the_kubeconfig_gives_the_server_and_certificates() {
        let kc = "apiVersion: v1\nclusters:\n- cluster:\n    certificate-authority-data: Q0E=\n    server: https://10.0.3.253:6443\n  name: default\nusers:\n- name: default\n  user:\n    client-certificate-data: Y2VydA==\n    client-key-data: a2V5\n";
        assert_eq!(field(kc, "server"), Some("https://10.0.3.253:6443"));
        assert_eq!(decoded(kc, "certificate-authority-data").unwrap(), b"CA");
        assert_eq!(decoded(kc, "client-key-data").unwrap(), b"key");
        assert!(decoded("apiVersion: v1", "client-key-data").is_err());
        assert!(Access::from_kubeconfig(kc).is_err(), "not real certificates");
    }

    #[test]
    fn a_pod_shows_like_kubectl_get_pods() {
        let pod = |status: Value, deleting: bool| {
            let mut meta = json!({ "namespace": "demo", "name": "p" });
            if deleting {
                meta["deletionTimestamp"] = json!("2026-10-27T14:00:45Z");
            }
            json!({ "metadata": meta, "status": status })
        };
        let running = json!({ "phase": "Running", "containerStatuses": [{ "state": { "running": {} }, "ready": true }] });
        assert_eq!(pod_status(&pod(running.clone(), false)), "Running");
        assert_eq!(pod_status(&pod(running, true)), "Terminating");
        let creating = json!({ "phase": "Pending", "containerStatuses": [{ "state": { "waiting": { "reason": "ContainerCreating" } } }] });
        assert_eq!(pod_status(&pod(creating, false)), "ContainerCreating");
        let crashed = json!({ "phase": "Running", "containerStatuses": [{ "state": { "terminated": { "reason": "Error" } } }] });
        assert_eq!(pod_status(&pod(crashed, false)), "Error");
        let done = json!({ "phase": "Succeeded", "containerStatuses": [{ "state": { "terminated": { "reason": "Completed" } } }] });
        assert_eq!(pod_status(&pod(done, false)), "Succeeded");
        assert_eq!(pod_status(&pod(json!({ "phase": "Failed", "reason": "Evicted" }), false)), "Evicted");
        assert_eq!(pod_status(&pod(json!({ "phase": "Pending" }), false)), "Pending");
    }

    const VIP: &str = "10.0.3.253";

    fn sample() -> Lists {
        Lists {
            pods: list(vec![
                json!({
                    "metadata": { "namespace": "demo", "name": "web-6d4-x2", "creationTimestamp": "1970-01-01T00:00:10Z",
                        "ownerReferences": [{ "kind": "ReplicaSet", "name": "web-6d4", "controller": true }] },
                    "spec": { "nodeName": "ms01-02", "containers": [{}, {}] },
                    "status": { "phase": "Running", "containerStatuses": [{ "ready": true, "restartCount": 2, "state": { "running": {} } }, { "ready": false, "restartCount": 1, "state": { "running": {} } }] }
                }),
                json!({
                    "metadata": { "namespace": "demo", "name": "postgres-0", "ownerReferences": [{ "kind": "StatefulSet", "name": "postgres", "controller": true }] },
                    "spec": { "containers": [{}] },
                    "status": { "phase": "Pending" }
                }),
            ]),
            replicasets: list(vec![json!({ "metadata": { "namespace": "demo", "name": "web-6d4", "ownerReferences": [{ "kind": "Deployment", "name": "web", "controller": true }] } })]),
            deployments: list(vec![json!({ "metadata": { "namespace": "demo", "name": "web" }, "spec": { "replicas": 3 }, "status": { "readyReplicas": 2 } })]),
            statefulsets: list(vec![json!({ "metadata": { "namespace": "demo", "name": "postgres" }, "spec": { "replicas": 1 }, "status": {} })]),
            daemonsets: list(vec![json!({ "metadata": { "namespace": "longhorn-system", "name": "longhorn-manager" }, "status": { "desiredNumberScheduled": 6, "numberReady": 5 } })]),
            volumes: list(vec![json!({
                "metadata": { "name": "pvc-1" },
                "spec": { "size": "2147483648", "numberOfReplicas": 3 },
                "status": { "state": "attached", "robustness": "degraded", "currentNodeID": "ms01-04",
                    "kubernetesStatus": { "namespace": "demo", "pvcName": "data-postgres-0", "workloadsStatus": [{ "podName": "postgres-0" }] } }
            })]),
            replicas: list(vec![
                json!({ "metadata": { "name": "pvc-1-r-b" }, "spec": { "volumeName": "pvc-1", "nodeID": "ms01-03", "diskPath": "/mnt/nvme0", "failedAt": "" }, "status": { "currentState": "running" } }),
                json!({ "metadata": { "name": "pvc-1-r-a" }, "spec": { "volumeName": "pvc-1", "nodeID": "ms01-01", "diskPath": "/mnt/nvme0", "failedAt": "2026-10-27T14:00:45Z" }, "status": { "currentState": "stopped" } }),
            ]),
            engines: list(vec![json!({ "status": { "replicaModeMap": { "pvc-1-r-b": "WO" } } })]),
            services: list(vec![json!({ "metadata": { "namespace": "demo", "name": "web" }, "spec": { "type": "ClusterIP", "selector": { "app": "web" }, "ports": [{ "port": 80 }] } })]),
            ingresses: list(vec![json!({ "metadata": { "namespace": "demo", "name": "web" }, "spec": { "rules": [{ "http": { "paths": [{ "path": "/", "backend": { "service": { "name": "web" } } }] } }] } })]),
            events: list(vec![
                json!({ "involvedObject": { "namespace": "demo", "kind": "Pod", "name": "postgres-0" }, "reason": "FailedScheduling", "message": "0/4 nodes are available", "count": 3, "lastTimestamp": "1970-01-01T01:00:00Z" }),
                json!({ "involvedObject": { "namespace": "demo", "kind": "ReplicaSet", "name": "web-6d4" }, "reason": "FailedCreate", "message": "quota", "lastTimestamp": "1970-01-01T01:30:00Z" }),
                json!({ "involvedObject": { "namespace": "demo", "kind": "Pod", "name": "gone-123" }, "reason": "BackOff", "message": "x", "lastTimestamp": "1970-01-01T01:10:00Z" }),
                json!({ "involvedObject": { "kind": "Node", "name": "ms01-01" }, "reason": "InvalidDiskCapacity", "message": "x" }),
                json!({ "involvedObject": { "namespace": "demo", "kind": "Pod", "name": "web-6d4-x2" }, "reason": "Unhealthy", "message": "old", "lastTimestamp": "1970-01-01T00:00:01Z" }),
            ]),
            controllerrevisions: list(vec![]),
            pdbs: list(vec![json!({ "metadata": { "namespace": "demo", "name": "web" }, "spec": { "selector": { "matchLabels": { "app": "web" } } }, "status": { "disruptionsAllowed": 0 } })]),
            metrics: Ok(json!({ "items": [{ "metadata": { "namespace": "demo", "name": "web-6d4-x2" }, "containers": [{ "usage": { "cpu": "12m", "memory": "30Mi" } }, { "usage": { "cpu": "3m", "memory": "2Mi" } }] }] })),
        }
    }

    /// 1:40, so events from 0:40 on are within the hour.
    const NOW: u64 = 6_000_000;

    #[test]
    fn pods_belong_to_their_workload_and_node() {
        let v = view_of(&sample(), 7, VIP);
        assert_eq!(v.at, 7);
        let names: Vec<_> = v.pods.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["postgres-0", "web-6d4-x2"]);
        let web = &v.pods[1];
        assert_eq!(web.owner.as_deref(), Some("Deployment/web"), "through its ReplicaSet");
        assert_eq!((web.node.as_deref(), web.status.as_str()), (Some("ms01-02"), "Running"));
        assert_eq!((web.ready, web.containers, web.restarts, web.created), (1, 2, 3, Some(10_000)));
        let pg = &v.pods[0];
        assert_eq!((pg.owner.as_deref(), pg.node.as_deref(), pg.ready, pg.containers), (Some("StatefulSet/postgres"), None, 0, 1));
    }

    #[test]
    fn workloads_count_their_ready_replicas() {
        let v = view_of(&sample(), NOW, VIP);
        let rows: Vec<_> = v.workloads.iter().map(|w| (w.kind, w.name.as_str(), w.ready, w.desired)).collect();
        assert_eq!(rows, [(WorkloadKind::StatefulSet, "postgres", 0, 1), (WorkloadKind::Deployment, "web", 2, 3), (WorkloadKind::DaemonSet, "longhorn-manager", 5, 6)]);
    }

    #[test]
    fn volumes_list_their_claim_pods_and_replicas() {
        let v = view_of(&sample(), NOW, VIP);
        let [vol] = v.volumes.as_slice() else { panic!("{:?}", v.volumes) };
        assert_eq!(vol.pvc.as_deref(), Some("demo/data-postgres-0"));
        assert_eq!(vol.pods, ["postgres-0"]);
        assert_eq!((vol.size_bytes, vol.replicas_wanted, vol.node.as_deref()), (2 << 30, 3, Some("ms01-04")));
        assert_eq!((vol.state.as_str(), vol.robustness.as_str()), ("attached", "degraded"));
        let reps: Vec<_> = vol.replicas.iter().map(|r| (r.node.as_str(), r.state.as_str(), r.mode.as_deref(), r.failed)).collect();
        assert_eq!(reps, [("ms01-01", "stopped", None, true), ("ms01-03", "running", Some("WO"), false)]);
    }

    #[test]
    fn workloads_carry_health_usage_images_and_endpoints() {
        let v = view_of(&sample(), NOW, VIP);
        let web = v.workloads.iter().find(|w| w.name == "web").unwrap();
        assert_eq!(web.health.state, HealthState::RollingOut, "0 of 3 updated: {:?}", web.health);
        assert_eq!((web.cpu_milli, web.memory_bytes, web.cpu_limit_milli), (Some(15), Some(32 << 20), None));
        let eps: Vec<_> = web.endpoints.iter().map(|e| e.address.as_str()).collect();
        // The template has no labels in this sample, so no Service selects it.
        assert!(eps.is_empty(), "{eps:?}");
        let pg = v.workloads.iter().find(|w| w.name == "postgres").unwrap();
        assert_eq!((pg.cpu_milli, pg.memory_bytes), (None, None), "its pod has no usage yet");
        assert_eq!(v.metrics_error, None);
    }

    #[test]
    fn a_new_pod_without_usage_leaves_its_workloads_usage_to_the_others() {
        let mut lists = sample();
        let mut fresh = items(&lists.pods).iter().find(|p| p["metadata"]["name"] == "web-6d4-x2").unwrap().clone();
        fresh["metadata"]["name"] = json!("web-6d4-new");
        lists.pods["items"].as_array_mut().unwrap().push(fresh);
        let v = view_of(&lists, NOW, VIP);
        let web = v.workloads.iter().find(|w| w.name == "web").unwrap();
        assert_eq!((web.cpu_milli, web.memory_bytes), (Some(15), Some(32 << 20)), "web-6d4-new is not measured yet");
    }

    #[test]
    fn a_template_labelled_for_a_service_gets_its_endpoints() {
        let mut lists = sample();
        lists.deployments = list(vec![json!({ "metadata": { "namespace": "demo", "name": "web" }, "spec": { "replicas": 1, "template": { "metadata": { "labels": { "app": "web" } }, "spec": { "containers": [{ "image": "nginx:1.29" }] } } }, "status": {} })]);
        let v = view_of(&lists, NOW, VIP);
        let web = v.workloads.iter().find(|w| w.name == "web").unwrap();
        let eps: Vec<_> = web.endpoints.iter().map(|e| (e.address.as_str(), e.link.is_some())).collect();
        assert_eq!(eps, [("http://10.0.3.253/", true), ("web.demo:80", false)]);
        assert_eq!(web.images, ["nginx:1.29"]);
    }

    #[test]
    fn a_workload_carries_its_containers_history_and_budget_and_a_pod_its_networks() {
        let mut lists = sample();
        lists.deployments = list(vec![json!({ "metadata": { "namespace": "demo", "name": "web", "annotations": { "deployment.kubernetes.io/revision": "1" } },
            "spec": { "replicas": 1, "template": { "metadata": { "labels": { "app": "web" } }, "spec": { "hostNetwork": true, "containers": [{ "name": "nginx", "image": "nginx:1.29" }] } } }, "status": {} })]);
        lists.replicasets["items"][0]["metadata"]["annotations"] = json!({ "deployment.kubernetes.io/revision": "1" });
        lists.pods["items"][0]["status"]["podIP"] = json!("10.42.0.9");
        lists.pods["items"][0]["status"]["qosClass"] = json!("BestEffort");
        let v = view_of(&lists, NOW, VIP);
        let web = v.workloads.iter().find(|w| w.name == "web").unwrap();
        assert_eq!(web.containers.iter().map(|c| c.image.as_str()).collect::<Vec<_>>(), ["nginx:1.29"]);
        assert_eq!(web.host_access, ["host network"]);
        assert_eq!(web.revisions.iter().map(|r| (r.number, r.current)).collect::<Vec<_>>(), [(1, true)]);
        assert_eq!(web.pdb.as_ref().map(|p| (p.name.as_str(), p.disruptions_allowed)), Some(("web", 0)));
        let pod = v.pods.iter().find(|p| p.name == "web-6d4-x2").unwrap();
        assert_eq!((pod.ips.clone(), pod.qos.as_deref(), pod.container_states.len()), (vec!["10.42.0.9".to_string()], Some("BestEffort"), 2));
    }

    #[test]
    fn warning_events_of_the_last_hour_are_matched_to_their_workload() {
        let v = view_of(&sample(), NOW, VIP);
        let rows: Vec<_> = v.events.iter().map(|e| (e.reason.as_str(), e.workload.as_deref(), e.count)).collect();
        assert_eq!(
            rows,
            [("FailedCreate", Some("Deployment/web"), 1), ("BackOff", None, 1), ("FailedScheduling", Some("StatefulSet/postgres"), 3)],
            "newest first; a ReplicaSet's are its Deployment's; nodes' and old ones left out"
        );
    }

    #[test]
    fn without_metrics_server_the_rest_still_shows() {
        let lists = Lists { metrics: Err("metrics-server: 503 Service Unavailable".into()), ..sample() };
        let v = view_of(&lists, NOW, VIP);
        assert_eq!(v.metrics_error.as_deref(), Some("metrics-server: 503 Service Unavailable"));
        assert!(v.workloads.iter().all(|w| w.cpu_milli.is_none() && w.memory_bytes.is_none()));
        assert_eq!(v.workloads.len(), 3);
    }

    #[test]
    fn the_vip_is_the_kubeconfigs_server_host() {
        let host = |server: &str| Access { server: server.into(), client: reqwest::Client::new() }.host().to_string();
        assert_eq!(host("https://10.0.3.253:6443"), "10.0.3.253");
        assert_eq!(host("https://k3s.home"), "k3s.home");
    }

    #[test]
    fn missing_lists_are_empty() {
        let v = view_of(&Lists::default(), 1, VIP);
        assert!(v.pods.is_empty() && v.workloads.is_empty() && v.volumes.is_empty());
    }
}
