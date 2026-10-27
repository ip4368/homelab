//! What Kubernetes runs, for the UI's Workloads tab: the workloads, their
//! pods and where they run, and the Longhorn volumes with their replicas.
//! Read from the K3s API through the VIP with an admin kubeconfig, every
//! few seconds, the way `kubectl get` would.

use base64::Engine;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use ts_rs::TS;

/// The pause between reads.
pub const POLL_EVERY: Duration = Duration::from_secs(3);
/// The pause before asking for a kubeconfig again, when there is none yet.
pub const KUBECONFIG_RETRY: Duration = Duration::from_secs(10);
/// Failed reads in a row after which the kubeconfig is fetched again (the
/// cluster may have been reinstalled, with a new CA).
pub const FAILURES_BEFORE_REFRESH: u32 = 5;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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
}

/// A Deployment, StatefulSet or DaemonSet.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct KubeWorkload {
    pub namespace: String,
    pub name: String,
    pub kind: String,
    pub desired: u32,
    pub ready: u32,
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
#[derive(Debug, Default)]
pub struct Lists {
    pub pods: Value,
    pub replicasets: Value,
    pub deployments: Value,
    pub statefulsets: Value,
    pub daemonsets: Value,
    pub volumes: Value,
    pub replicas: Value,
    pub engines: Value,
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
    pub async fn read(&self) -> Result<Lists, ReadError> {
        let (pods, replicasets, deployments, statefulsets, daemonsets, volumes, replicas, engines) = tokio::try_join!(
            self.list("/api/v1/pods"),
            self.list("/apis/apps/v1/replicasets"),
            self.list("/apis/apps/v1/deployments"),
            self.list("/apis/apps/v1/statefulsets"),
            self.list("/apis/apps/v1/daemonsets"),
            self.list("/apis/longhorn.io/v1beta2/namespaces/longhorn-system/volumes"),
            self.list("/apis/longhorn.io/v1beta2/namespaces/longhorn-system/replicas"),
            self.list("/apis/longhorn.io/v1beta2/namespaces/longhorn-system/engines"),
        )?;
        Ok(Lists { pods, replicasets, deployments, statefulsets, daemonsets, volumes, replicas, engines })
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

/// The view `lists` describe, read at `at`.
pub fn view_of(lists: &Lists, at: u64) -> KubeView {
    // A Deployment's pods belong to a ReplicaSet it owns.
    let rs_owner: BTreeMap<(String, String), (String, String)> =
        items(&lists.replicasets).iter().filter_map(|rs| Some((key(&rs["metadata"]), controller(&rs["metadata"])?))).collect();

    let mut workloads: Vec<KubeWorkload> = Vec::new();
    for d in items(&lists.deployments) {
        let (namespace, name) = key(&d["metadata"]);
        let desired = d["spec"]["replicas"].as_u64().map_or(1, |n| n as u32);
        workloads.push(KubeWorkload { namespace, name, kind: "Deployment".into(), desired, ready: count(&d["status"]["readyReplicas"]) });
    }
    for s in items(&lists.statefulsets) {
        let (namespace, name) = key(&s["metadata"]);
        let desired = s["spec"]["replicas"].as_u64().map_or(1, |n| n as u32);
        workloads.push(KubeWorkload { namespace, name, kind: "StatefulSet".into(), desired, ready: count(&s["status"]["readyReplicas"]) });
    }
    for d in items(&lists.daemonsets) {
        let (namespace, name) = key(&d["metadata"]);
        let (desired, ready) = (count(&d["status"]["desiredNumberScheduled"]), count(&d["status"]["numberReady"]));
        workloads.push(KubeWorkload { namespace, name, kind: "DaemonSet".into(), desired, ready });
    }
    workloads.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

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
            KubePod {
                owner,
                node: text(&p["spec"]["nodeName"]),
                status: pod_status(p),
                ready: statuses.iter().filter(|c| c["ready"].as_bool() == Some(true)).count() as u32,
                containers,
                restarts: statuses.iter().map(|c| count(&c["restartCount"])).sum(),
                created: meta["creationTimestamp"].as_str().and_then(timestamp_ms),
                namespace,
                name,
            }
        })
        .collect();
    pods.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

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

    KubeView { at, error: None, workloads, pods, volumes }
}

/// Keeps `tx` up to date with what Kubernetes runs, reading it with the
/// kubeconfig `kubeconfig` gives (asked again whenever the API refuses it
/// or keeps failing). A failed read keeps the last lists and says why.
pub async fn watch<F, Fut>(kubeconfig: F, tx: watch::Sender<Arc<KubeView>>)
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
                tx.send_replace(Arc::new(view_of(&lists, node::runtime::wall_ms())));
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
        tokio::time::sleep(POLL_EVERY).await;
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
        }
    }

    #[test]
    fn pods_belong_to_their_workload_and_node() {
        let v = view_of(&sample(), 7);
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
        let v = view_of(&sample(), 0);
        let rows: Vec<_> = v.workloads.iter().map(|w| (w.kind.as_str(), w.name.as_str(), w.ready, w.desired)).collect();
        assert_eq!(rows, [("StatefulSet", "postgres", 0, 1), ("Deployment", "web", 2, 3), ("DaemonSet", "longhorn-manager", 5, 6)]);
    }

    #[test]
    fn volumes_list_their_claim_pods_and_replicas() {
        let v = view_of(&sample(), 0);
        let [vol] = v.volumes.as_slice() else { panic!("{:?}", v.volumes) };
        assert_eq!(vol.pvc.as_deref(), Some("demo/data-postgres-0"));
        assert_eq!(vol.pods, ["postgres-0"]);
        assert_eq!((vol.size_bytes, vol.replicas_wanted, vol.node.as_deref()), (2 << 30, 3, Some("ms01-04")));
        assert_eq!((vol.state.as_str(), vol.robustness.as_str()), ("attached", "degraded"));
        let reps: Vec<_> = vol.replicas.iter().map(|r| (r.node.as_str(), r.state.as_str(), r.mode.as_deref(), r.failed)).collect();
        assert_eq!(reps, [("ms01-01", "stopped", None, true), ("ms01-03", "running", Some("WO"), false)]);
    }

    #[test]
    fn missing_lists_are_empty() {
        let v = view_of(&Lists::default(), 1);
        assert!(v.pods.is_empty() && v.workloads.is_empty() && v.volumes.is_empty());
    }
}
