//! What a workload's pod template asks for, for the workload drawer: its
//! containers with their resources, ports, probes, environment, mounts and
//! devices; its pods' networks; its rollout history and disruption budget.
//! All from the lists the server already reads, but ControllerRevisions and
//! PodDisruptionBudgets (docs/plans/2026-11-04-workload-insight-logs-terminal-design.md §3).

use crate::kube_detail::{bytes, millicores};
use serde::Serialize;
use serde_json::Value;
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub enum ContainerRole {
    App,
    /// Runs to completion before the app containers start.
    Init,
    /// An init container that keeps running beside the app (`restartPolicy: Always`).
    Sidecar,
}

/// A container of a pod template.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub role: ContainerRole,
    #[ts(type = "number | null")]
    pub cpu_request_milli: Option<u64>,
    #[ts(type = "number | null")]
    pub memory_request_bytes: Option<u64>,
    #[ts(type = "number | null")]
    pub cpu_limit_milli: Option<u64>,
    #[ts(type = "number | null")]
    pub memory_limit_bytes: Option<u64>,
    pub ports: Vec<PortSpec>,
    pub probes: Vec<ProbeSpec>,
    /// Names and where they come from, never values: a literal may be a password.
    pub env: Vec<EnvSource>,
    pub mounts: Vec<Mount>,
    pub devices: Vec<Device>,
    /// What it may do beyond a plain container: `privileged`, `adds NET_ADMIN`, `runs as root`.
    pub security: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct PortSpec {
    pub name: Option<String>,
    pub port: u32,
    /// `TCP`, `UDP` or `SCTP`.
    pub protocol: String,
    /// The port it takes on its host, which ties the pod to a host with that port free.
    pub host_port: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub enum ProbeKind {
    Startup,
    Liveness,
    Readiness,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct ProbeSpec {
    pub kind: ProbeKind,
    /// `HTTP GET :8080/healthz`, `TCP :5432`, `exec cat /tmp/ok`.
    pub check: String,
    pub every_s: u32,
    /// Failures in a row before it counts.
    pub failures: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct EnvSource {
    /// The variable, or `*` for every key of a ConfigMap or Secret (with its prefix, if any).
    pub name: String,
    /// `literal`, `ConfigMap app-config`, `Secret db (password)`, `field metadata.name`.
    pub from: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub enum VolumeKind {
    Claim,
    ConfigMap,
    Secret,
    EmptyDir,
    HostPath,
    Projected,
    DownwardApi,
    Other,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct Mount {
    pub path: String,
    /// The pod's volume.
    pub volume: String,
    pub kind: VolumeKind,
    /// The claim, ConfigMap or Secret's name, or the host path.
    pub source: Option<String>,
    /// More, in a few words: `in memory, at most 1Gi`, `type Directory`, `keys a, b`.
    pub detail: Option<String>,
    pub read_only: bool,
    pub sub_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub enum DeviceKind {
    /// A host path under `/dev`.
    HostPath,
    /// A raw block claim.
    Block,
    /// An extended resource a device plugin hands out.
    Resource,
    /// A privileged container sees every device.
    Privileged,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct Device {
    pub kind: DeviceKind,
    /// `/dev/ttyUSB0`, `gpu.intel.com/i915: 1`, the claim and its device path.
    pub what: String,
}

/// How a container of a pod is, as kubectl describe says.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct ContainerState {
    pub name: String,
    pub role: ContainerRole,
    /// `running`, or why not: `CrashLoopBackOff`, `Completed`…
    pub state: String,
    pub ready: bool,
    pub restarts: u32,
    /// The image it actually runs, by digest (`docker.io/library/nginx@sha256:…`); none before it is pulled.
    pub image_id: Option<String>,
}

/// A network a pod is attached to, as Multus reports it.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct PodNetwork {
    /// `k8s-pod-network`, `longhorn-system/storage`.
    pub name: String,
    pub interface: Option<String>,
    pub ips: Vec<String>,
    pub mac: Option<String>,
    /// The pod network every pod has.
    pub default: bool,
}

/// One revision of a workload's pod template.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct Revision {
    #[ts(type = "number")]
    pub number: u64,
    pub images: Vec<String>,
    #[ts(type = "number | null")]
    pub created: Option<u64>,
    /// Pods on it (a Deployment's ReplicaSet's); none where Kubernetes does not say.
    pub pods: Option<u32>,
    pub current: bool,
}

/// A PodDisruptionBudget over a workload's pods.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct Pdb {
    pub name: String,
    /// Pods that may be evicted now. At 0, a drain of their hosts waits.
    pub disruptions_allowed: u32,
    pub healthy: u32,
    pub desired_healthy: u32,
}

fn text(v: &Value) -> Option<String> {
    v.as_str().filter(|s| !s.is_empty()).map(String::from)
}

fn arr(v: &Value) -> &[Value] {
    v.as_array().map_or(&[], Vec::as_slice)
}

fn num(v: &Value) -> Option<u32> {
    v.as_u64().map(|n| n as u32)
}

/// A probe's port: a number, or the name of a container port.
fn port(v: &Value) -> String {
    v.as_u64().map_or_else(|| v.as_str().unwrap_or("?").to_string(), |n| n.to_string())
}

fn probe(kind: ProbeKind, p: &Value) -> Option<ProbeSpec> {
    if !p.is_object() {
        return None;
    }
    let check = if p["httpGet"].is_object() {
        let h = &p["httpGet"];
        let scheme = if h["scheme"] == "HTTPS" { "HTTPS" } else { "HTTP" };
        format!("{scheme} GET :{}{}", port(&h["port"]), h["path"].as_str().unwrap_or("/"))
    } else if p["tcpSocket"].is_object() {
        format!("TCP :{}", port(&p["tcpSocket"]["port"]))
    } else if p["grpc"].is_object() {
        format!("gRPC :{}", port(&p["grpc"]["port"]))
    } else if p["exec"].is_object() {
        let cmd: Vec<&str> = arr(&p["exec"]["command"]).iter().filter_map(Value::as_str).collect();
        format!("exec {}", cmd.join(" "))
    } else {
        return None;
    };
    Some(ProbeSpec { kind, check, every_s: num(&p["periodSeconds"]).unwrap_or(10), failures: num(&p["failureThreshold"]).unwrap_or(3) })
}

fn env(c: &Value) -> Vec<EnvSource> {
    let mut out: Vec<EnvSource> = arr(&c["env"])
        .iter()
        .map(|e| {
            let f = &e["valueFrom"];
            let from = if let Some(n) = text(&f["configMapKeyRef"]["name"]) {
                format!("ConfigMap {n} ({})", f["configMapKeyRef"]["key"].as_str().unwrap_or("?"))
            } else if let Some(n) = text(&f["secretKeyRef"]["name"]) {
                format!("Secret {n} ({})", f["secretKeyRef"]["key"].as_str().unwrap_or("?"))
            } else if let Some(p) = text(&f["fieldRef"]["fieldPath"]) {
                format!("field {p}")
            } else if let Some(r) = text(&f["resourceFieldRef"]["resource"]) {
                format!("resource {r}")
            } else {
                "literal".into()
            };
            EnvSource { name: text(&e["name"]).unwrap_or_default(), from }
        })
        .collect();
    for e in arr(&c["envFrom"]) {
        let name = format!("{}*", e["prefix"].as_str().unwrap_or(""));
        if let Some(n) = text(&e["configMapRef"]["name"]) {
            out.push(EnvSource { name, from: format!("ConfigMap {n}") });
        } else if let Some(n) = text(&e["secretRef"]["name"]) {
            out.push(EnvSource { name, from: format!("Secret {n}") });
        }
    }
    out
}

/// What a pod volume is, as `(kind, source, detail)`.
fn volume_source(v: &Value) -> (VolumeKind, Option<String>, Option<String>) {
    let keys = |items: &Value| {
        let k: Vec<&str> = arr(items).iter().filter_map(|i| i["key"].as_str()).collect();
        (!k.is_empty()).then(|| format!("keys {}", k.join(", ")))
    };
    if let Some(c) = text(&v["persistentVolumeClaim"]["claimName"]) {
        (VolumeKind::Claim, Some(c), None)
    } else if v["configMap"].is_object() {
        (VolumeKind::ConfigMap, text(&v["configMap"]["name"]), keys(&v["configMap"]["items"]))
    } else if v["secret"].is_object() {
        (VolumeKind::Secret, text(&v["secret"]["secretName"]), keys(&v["secret"]["items"]))
    } else if v["emptyDir"].is_object() {
        let e = &v["emptyDir"];
        let parts: Vec<String> = [(e["medium"] == "Memory").then(|| "in memory".to_string()), text(&e["sizeLimit"]).map(|l| format!("at most {l}"))].into_iter().flatten().collect();
        (VolumeKind::EmptyDir, None, (!parts.is_empty()).then(|| parts.join(", ")))
    } else if v["hostPath"].is_object() {
        (VolumeKind::HostPath, text(&v["hostPath"]["path"]), text(&v["hostPath"]["type"]).map(|t| format!("type {t}")))
    } else if v["projected"].is_object() {
        let sources: Vec<String> = arr(&v["projected"]["sources"])
            .iter()
            .filter_map(|s| {
                let (kind, inner) = s.as_object()?.iter().next()?;
                Some(match text(&inner["name"]) {
                    Some(n) => format!("{kind} {n}"),
                    None => kind.clone(),
                })
            })
            .collect();
        (VolumeKind::Projected, None, (!sources.is_empty()).then(|| sources.join(", ")))
    } else if v["downwardAPI"].is_object() {
        (VolumeKind::DownwardApi, None, None)
    } else {
        let kind = v.as_object().and_then(|o| o.keys().find(|k| *k != "name").cloned());
        (VolumeKind::Other, kind, None)
    }
}

/// Resource names that are not devices.
fn plain_resource(name: &str) -> bool {
    ["cpu", "memory", "ephemeral-storage"].contains(&name) || name.starts_with("hugepages-")
}

fn container(c: &Value, role: ContainerRole, pod: &Value, claim_templates: &[String]) -> ContainerSpec {
    let volumes = arr(&pod["volumes"]);
    let volume = |name: &str| volumes.iter().find(|v| v["name"] == name);
    let res = &c["resources"];
    let mounts: Vec<Mount> = arr(&c["volumeMounts"])
        .iter()
        .map(|m| {
            let name = text(&m["name"]).unwrap_or_default();
            let (kind, source, detail) = match volume(&name) {
                Some(v) => volume_source(v),
                None if claim_templates.contains(&name) => (VolumeKind::Claim, None, Some("a claim per pod".into())),
                None => (VolumeKind::Other, None, None),
            };
            Mount { path: text(&m["mountPath"]).unwrap_or_default(), volume: name, kind, source, detail, read_only: m["readOnly"].as_bool() == Some(true), sub_path: text(&m["subPath"]) }
        })
        .collect();
    let sc = &c["securityContext"];
    let privileged = sc["privileged"].as_bool() == Some(true);
    let mut devices: Vec<Device> = mounts
        .iter()
        .filter(|m| m.kind == VolumeKind::HostPath && m.source.as_deref().is_some_and(|p| p == "/dev" || p.starts_with("/dev/")))
        .map(|m| Device { kind: DeviceKind::HostPath, what: m.source.clone().unwrap_or_default() })
        .collect();
    for d in arr(&c["volumeDevices"]) {
        let claim = text(&d["name"]).and_then(|n| volume(&n).and_then(|v| text(&v["persistentVolumeClaim"]["claimName"]))).unwrap_or_default();
        devices.push(Device { kind: DeviceKind::Block, what: format!("{claim} at {}", d["devicePath"].as_str().unwrap_or("?")) });
    }
    let mut resources: Vec<(&String, &Value)> = res["limits"].as_object().into_iter().chain(res["requests"].as_object()).flatten().filter(|(k, _)| !plain_resource(k)).collect();
    resources.sort_by_key(|(k, _)| *k);
    resources.dedup_by_key(|(k, _)| *k);
    for (name, n) in resources {
        let n = n.as_str().map_or_else(|| n.to_string(), String::from);
        devices.push(Device { kind: DeviceKind::Resource, what: format!("{name}: {n}") });
    }
    if privileged {
        devices.push(Device { kind: DeviceKind::Privileged, what: "every device (privileged)".into() });
    }
    let mut security = vec![];
    if privileged {
        security.push("privileged".to_string());
    }
    let caps: Vec<&str> = arr(&sc["capabilities"]["add"]).iter().filter_map(Value::as_str).collect();
    if !caps.is_empty() {
        security.push(format!("adds {}", caps.join(", ")));
    }
    if sc["runAsUser"].as_u64().or_else(|| pod["securityContext"]["runAsUser"].as_u64()) == Some(0) {
        security.push("runs as root".into());
    }
    let probes = [(ProbeKind::Startup, "startupProbe"), (ProbeKind::Liveness, "livenessProbe"), (ProbeKind::Readiness, "readinessProbe")].into_iter().filter_map(|(k, f)| probe(k, &c[f])).collect();
    ContainerSpec {
        name: text(&c["name"]).unwrap_or_default(),
        image: text(&c["image"]).unwrap_or_default(),
        role,
        cpu_request_milli: millicores(&res["requests"]["cpu"]),
        memory_request_bytes: bytes(&res["requests"]["memory"]),
        cpu_limit_milli: millicores(&res["limits"]["cpu"]),
        memory_limit_bytes: bytes(&res["limits"]["memory"]),
        ports: arr(&c["ports"])
            .iter()
            .map(|p| PortSpec { name: text(&p["name"]), port: num(&p["containerPort"]).unwrap_or(0), protocol: text(&p["protocol"]).unwrap_or_else(|| "TCP".into()), host_port: num(&p["hostPort"]) })
            .collect(),
        probes,
        env: env(c),
        mounts,
        devices,
        security,
    }
}

fn init_role(c: &Value) -> ContainerRole {
    if c["restartPolicy"] == "Always" { ContainerRole::Sidecar } else { ContainerRole::Init }
}

/// The containers of a pod spec, init containers and sidecars first, as they
/// start. `claim_templates` are a StatefulSet's `volumeClaimTemplates`, which
/// its pods mount as volumes their spec does not list.
pub fn containers(pod: &Value, claim_templates: &[String]) -> Vec<ContainerSpec> {
    let init = arr(&pod["initContainers"]).iter().map(|c| container(c, init_role(c), pod, claim_templates));
    init.chain(arr(&pod["containers"]).iter().map(|c| container(c, ContainerRole::App, pod, claim_templates))).collect()
}

/// The names of a StatefulSet's `volumeClaimTemplates`.
pub fn claim_templates(workload: &Value) -> Vec<String> {
    arr(&workload["spec"]["volumeClaimTemplates"]).iter().filter_map(|t| text(&t["metadata"]["name"])).collect()
}

/// What a pod spec gives its pods of their host's: `host network`, `host PIDs`, `host IPC`.
pub fn host_access(pod: &Value) -> Vec<String> {
    [("hostNetwork", "host network"), ("hostPID", "host PIDs"), ("hostIPC", "host IPC")].into_iter().filter(|(f, _)| pod[f].as_bool() == Some(true)).map(|(_, s)| s.to_string()).collect()
}

/// Each container of a pod, as its status says, init containers first.
pub fn container_states(pod: &Value) -> Vec<ContainerState> {
    let spec = &pod["spec"];
    let role = |name: &str| arr(&spec["initContainers"]).iter().find(|c| c["name"] == name).map_or(ContainerRole::App, init_role);
    let statuses = arr(&pod["status"]["initContainerStatuses"]).iter().chain(arr(&pod["status"]["containerStatuses"]));
    statuses
        .map(|s| {
            let name = text(&s["name"]).unwrap_or_default();
            let st = &s["state"];
            let state = if st["running"].is_object() {
                "running".into()
            } else {
                text(&st["waiting"]["reason"]).or_else(|| text(&st["terminated"]["reason"])).unwrap_or_else(|| if st["waiting"].is_object() { "waiting".into() } else { "unknown".into() })
            };
            ContainerState { role: role(&name), state, ready: s["ready"].as_bool() == Some(true), restarts: num(&s["restartCount"]).unwrap_or(0), image_id: text(&s["imageID"]), name }
        })
        .collect()
}

/// A pod's IPs: `status.podIPs`, or the older `podIP`.
pub fn pod_ips(pod: &Value) -> Vec<String> {
    let ips: Vec<String> = arr(&pod["status"]["podIPs"]).iter().filter_map(|i| text(&i["ip"])).collect();
    if ips.is_empty() { text(&pod["status"]["podIP"]).into_iter().collect() } else { ips }
}

/// The networks Multus attached a pod to, from its `network-status` annotation (a JSON string).
pub fn pod_networks(pod: &Value) -> Vec<PodNetwork> {
    let Some(raw) = pod["metadata"]["annotations"]["k8s.v1.cni.cncf.io/network-status"].as_str() else { return vec![] };
    let Ok(Value::Array(nets)) = serde_json::from_str::<Value>(raw) else { return vec![] };
    nets.iter()
        .map(|n| PodNetwork {
            name: text(&n["name"]).unwrap_or_default(),
            interface: text(&n["interface"]),
            ips: arr(&n["ips"]).iter().filter_map(text).collect(),
            mac: text(&n["mac"]),
            default: n["default"].as_bool() == Some(true),
        })
        .collect()
}

/// Whether a label selector (`matchLabels` and `matchExpressions`) picks pods labelled `labels`.
/// An empty selector picks every pod, as a PodDisruptionBudget's does.
pub fn label_selector_matches(selector: &Value, labels: &Value) -> bool {
    let pairs = selector["matchLabels"].as_object().into_iter().flatten().all(|(k, v)| labels.get(k) == Some(v));
    let exprs = arr(&selector["matchExpressions"]).iter().all(|e| {
        let value = labels.get(e["key"].as_str().unwrap_or_default()).and_then(Value::as_str);
        let values: Vec<&str> = arr(&e["values"]).iter().filter_map(Value::as_str).collect();
        match e["operator"].as_str().unwrap_or_default() {
            "In" => value.is_some_and(|v| values.contains(&v)),
            "NotIn" => !value.is_some_and(|v| values.contains(&v)),
            "Exists" => value.is_some(),
            "DoesNotExist" => value.is_none(),
            _ => false,
        }
    });
    selector.is_object() && pairs && exprs
}

/// The first PodDisruptionBudget in `namespace` whose selector picks pods labelled `labels`.
pub fn pdb(namespace: &str, labels: &Value, pdbs: &Value) -> Option<Pdb> {
    arr(&pdbs["items"]).iter().filter(|p| p["metadata"]["namespace"] == namespace).find(|p| label_selector_matches(&p["spec"]["selector"], labels)).map(|p| {
        let st = &p["status"];
        Pdb {
            name: text(&p["metadata"]["name"]).unwrap_or_default(),
            disruptions_allowed: num(&st["disruptionsAllowed"]).unwrap_or(0),
            healthy: num(&st["currentHealthy"]).unwrap_or(0),
            desired_healthy: num(&st["desiredHealthy"]).unwrap_or(0),
        }
    })
}

fn images(template_spec: &Value) -> Vec<String> {
    arr(&template_spec["containers"]).iter().filter_map(|c| text(&c["image"])).collect()
}

fn owned_by(meta: &Value, kind: &str, name: &str) -> bool {
    arr(&meta["ownerReferences"]).iter().any(|r| r["kind"] == kind && r["name"] == name)
}

/// The rollout history of workload `kind/name` in `namespace`, newest first: a Deployment's
/// from its ReplicaSets, a StatefulSet's or DaemonSet's from its ControllerRevisions.
/// `workload` is the workload object; `timestamp` reads Kubernetes' times.
pub fn revisions(workload: &Value, kind: &str, replicasets: &Value, controllerrevisions: &Value, timestamp: fn(&str) -> Option<u64>) -> Vec<Revision> {
    let meta = &workload["metadata"];
    let (namespace, name) = (meta["namespace"].as_str().unwrap_or_default(), meta["name"].as_str().unwrap_or_default());
    let created = |m: &Value| m["creationTimestamp"].as_str().and_then(timestamp);
    let mine = |list: &Value| -> Vec<Value> { arr(&list["items"]).iter().filter(|o| o["metadata"]["namespace"] == namespace && owned_by(&o["metadata"], kind, name)).cloned().collect() };
    let mut out: Vec<Revision> = if kind == "Deployment" {
        let current = meta["annotations"]["deployment.kubernetes.io/revision"].as_str().and_then(|r| r.parse::<u64>().ok());
        mine(replicasets)
            .iter()
            .filter_map(|rs| {
                let number = rs["metadata"]["annotations"]["deployment.kubernetes.io/revision"].as_str()?.parse().ok()?;
                Some(Revision { number, images: images(&rs["spec"]["template"]["spec"]), created: created(&rs["metadata"]), pods: num(&rs["status"]["replicas"]).or(Some(0)), current: current == Some(number) })
            })
            .collect()
    } else {
        let update = workload["status"]["updateRevision"].as_str();
        let crs = mine(controllerrevisions);
        let newest = crs.iter().filter_map(|c| c["revision"].as_u64()).max();
        crs.iter()
            .filter_map(|c| {
                let number = c["revision"].as_u64()?;
                let current = match update {
                    Some(u) => c["metadata"]["name"] == u,
                    None => Some(number) == newest,
                };
                Some(Revision { number, images: images(&c["data"]["spec"]["template"]["spec"]), created: created(&c["metadata"]), pods: None, current })
            })
            .collect()
    };
    out.sort_by_key(|r| std::cmp::Reverse(r.number));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> Value {
        json!({
            "securityContext": { "runAsUser": 1000 },
            "initContainers": [
                { "name": "migrate", "image": "app:2", "command": ["migrate"] },
                { "name": "proxy", "image": "envoy:1", "restartPolicy": "Always" },
            ],
            "containers": [{
                "name": "app", "image": "app:2",
                "resources": { "requests": { "cpu": "100m", "memory": "64Mi" }, "limits": { "cpu": "1", "memory": "256Mi", "gpu.intel.com/i915": "1" } },
                "ports": [{ "name": "http", "containerPort": 8080 }, { "containerPort": 1900, "protocol": "UDP", "hostPort": 1900 }],
                "livenessProbe": { "httpGet": { "path": "/healthz", "port": "http" }, "periodSeconds": 5 },
                "readinessProbe": { "tcpSocket": { "port": 8080 }, "failureThreshold": 1 },
                "startupProbe": { "exec": { "command": ["cat", "/tmp/ok"] } },
                "env": [
                    { "name": "MODE", "value": "prod" },
                    { "name": "DB_PASSWORD", "valueFrom": { "secretKeyRef": { "name": "db", "key": "password" } } },
                    { "name": "POD", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } },
                ],
                "envFrom": [{ "configMapRef": { "name": "app-config" } }, { "prefix": "S_", "secretRef": { "name": "extra" } }],
                "volumeMounts": [
                    { "name": "data", "mountPath": "/data" },
                    { "name": "config", "mountPath": "/etc/app", "readOnly": true },
                    { "name": "cache", "mountPath": "/cache" },
                    { "name": "zigbee", "mountPath": "/dev/ttyUSB0" },
                    { "name": "token", "mountPath": "/var/run/secrets/x", "readOnly": true },
                    { "name": "config", "mountPath": "/etc/app.conf", "subPath": "app.conf" },
                ],
                "volumeDevices": [{ "name": "raw", "devicePath": "/dev/xvda" }],
                "securityContext": { "capabilities": { "add": ["NET_ADMIN"] }, "runAsUser": 0 },
            }],
            "volumes": [
                { "name": "data", "persistentVolumeClaim": { "claimName": "data-app-0" } },
                { "name": "config", "configMap": { "name": "app-config", "items": [{ "key": "app.conf", "path": "app.conf" }] } },
                { "name": "cache", "emptyDir": { "medium": "Memory", "sizeLimit": "1Gi" } },
                { "name": "zigbee", "hostPath": { "path": "/dev/ttyUSB0", "type": "CharDevice" } },
                { "name": "token", "projected": { "sources": [{ "serviceAccountToken": { "path": "token" } }, { "configMap": { "name": "kube-root-ca.crt" } }] } },
                { "name": "raw", "persistentVolumeClaim": { "claimName": "raw-disk" } },
            ],
        })
    }

    #[test]
    fn containers_come_in_start_order_with_their_roles() {
        let cs = containers(&spec(), &[]);
        let rows: Vec<_> = cs.iter().map(|c| (c.name.as_str(), c.role)).collect();
        assert_eq!(rows, [("migrate", ContainerRole::Init), ("proxy", ContainerRole::Sidecar), ("app", ContainerRole::App)]);
        assert!(cs[0].security.is_empty(), "the pod runs as UID 1000: {:?}", cs[0].security);
    }

    #[test]
    fn a_container_has_resources_ports_probes_and_env_without_values() {
        let app = containers(&spec(), &[]).pop().unwrap();
        assert_eq!((app.cpu_request_milli, app.memory_request_bytes, app.cpu_limit_milli, app.memory_limit_bytes), (Some(100), Some(64 << 20), Some(1000), Some(256 << 20)));
        let ports: Vec<_> = app.ports.iter().map(|p| (p.name.as_deref(), p.port, p.protocol.as_str(), p.host_port)).collect();
        assert_eq!(ports, [(Some("http"), 8080, "TCP", None), (None, 1900, "UDP", Some(1900))]);
        let probes: Vec<_> = app.probes.iter().map(|p| (p.kind, p.check.as_str(), p.every_s, p.failures)).collect();
        assert_eq!(probes, [(ProbeKind::Startup, "exec cat /tmp/ok", 10, 3), (ProbeKind::Liveness, "HTTP GET :http/healthz", 5, 3), (ProbeKind::Readiness, "TCP :8080", 10, 1)]);
        let env: Vec<_> = app.env.iter().map(|e| (e.name.as_str(), e.from.as_str())).collect();
        assert_eq!(env, [("MODE", "literal"), ("DB_PASSWORD", "Secret db (password)"), ("POD", "field metadata.name"), ("*", "ConfigMap app-config"), ("S_*", "Secret extra")]);
        assert!(!serde_json::to_string(&app).unwrap().contains("prod"), "a literal's value is never sent");
        assert_eq!(app.security, ["adds NET_ADMIN", "runs as root"]);
    }

    #[test]
    fn mounts_say_what_each_volume_is() {
        let app = containers(&spec(), &[]).pop().unwrap();
        let rows: Vec<_> = app.mounts.iter().map(|m| (m.path.as_str(), m.kind, m.source.as_deref(), m.detail.as_deref(), m.read_only, m.sub_path.as_deref())).collect();
        assert_eq!(
            rows,
            [
                ("/data", VolumeKind::Claim, Some("data-app-0"), None, false, None),
                ("/etc/app", VolumeKind::ConfigMap, Some("app-config"), Some("keys app.conf"), true, None),
                ("/cache", VolumeKind::EmptyDir, None, Some("in memory, at most 1Gi"), false, None),
                ("/dev/ttyUSB0", VolumeKind::HostPath, Some("/dev/ttyUSB0"), Some("type CharDevice"), false, None),
                ("/var/run/secrets/x", VolumeKind::Projected, None, Some("serviceAccountToken, configMap kube-root-ca.crt"), true, None),
                ("/etc/app.conf", VolumeKind::ConfigMap, Some("app-config"), Some("keys app.conf"), false, Some("app.conf")),
            ]
        );
    }

    #[test]
    fn a_statefulsets_claim_templates_are_claims() {
        let sts = json!({ "spec": { "volumeClaimTemplates": [{ "metadata": { "name": "data" } }] } });
        let pod = json!({ "containers": [{ "name": "pg", "volumeMounts": [{ "name": "data", "mountPath": "/var/lib/postgresql" }, { "name": "nowhere", "mountPath": "/x" }] }] });
        let mounts: Vec<_> = containers(&pod, &claim_templates(&sts)).remove(0).mounts.into_iter().map(|m| (m.kind, m.detail)).collect();
        assert_eq!(mounts, [(VolumeKind::Claim, Some("a claim per pod".into())), (VolumeKind::Other, None)]);
    }

    #[test]
    fn devices_are_host_paths_under_dev_block_claims_extended_resources_and_privilege() {
        let app = containers(&spec(), &[]).pop().unwrap();
        let rows: Vec<_> = app.devices.iter().map(|d| (d.kind, d.what.as_str())).collect();
        assert_eq!(rows, [(DeviceKind::HostPath, "/dev/ttyUSB0"), (DeviceKind::Block, "raw-disk at /dev/xvda"), (DeviceKind::Resource, "gpu.intel.com/i915: 1")]);
        let privileged = json!({ "containers": [{ "name": "c", "securityContext": { "privileged": true } }], "hostNetwork": true, "hostPID": true });
        let c = &containers(&privileged, &[])[0];
        assert_eq!(c.devices, [Device { kind: DeviceKind::Privileged, what: "every device (privileged)".into() }]);
        assert_eq!(c.security, ["privileged"]);
        assert_eq!(host_access(&privileged), ["host network", "host PIDs"]);
        assert!(host_access(&spec()).is_empty());
    }

    #[test]
    fn a_pod_has_its_container_states_ips_and_networks() {
        let status = r#"[{"name":"k8s-pod-network","interface":"eth0","ips":["10.42.1.7"],"mac":"aa:bb","default":true},{"name":"longhorn-system/storage","interface":"net1","ips":["10.0.20.31"]}]"#;
        let pod = json!({
            "metadata": { "annotations": { "k8s.v1.cni.cncf.io/network-status": status } },
            "spec": spec(),
            "status": {
                "podIP": "10.42.1.7", "podIPs": [{ "ip": "10.42.1.7" }, { "ip": "fd00::7" }],
                "initContainerStatuses": [{ "name": "migrate", "state": { "terminated": { "reason": "Completed" } }, "ready": false, "restartCount": 0 }],
                "containerStatuses": [{ "name": "app", "state": { "waiting": { "reason": "CrashLoopBackOff" } }, "ready": false, "restartCount": 4, "imageID": "docker.io/library/app@sha256:abc" }],
            }
        });
        let states: Vec<_> = container_states(&pod).iter().map(|s| (s.name.clone(), s.role, s.state.clone(), s.restarts, s.image_id.clone())).collect();
        assert_eq!(
            states,
            [("migrate".into(), ContainerRole::Init, "Completed".into(), 0, None), ("app".into(), ContainerRole::App, "CrashLoopBackOff".into(), 4, Some("docker.io/library/app@sha256:abc".into()))]
        );
        assert_eq!(pod_ips(&pod), ["10.42.1.7", "fd00::7"]);
        let nets: Vec<_> = pod_networks(&pod).iter().map(|n| (n.name.clone(), n.interface.clone(), n.ips.clone(), n.default)).collect();
        assert_eq!(nets, [("k8s-pod-network".into(), Some("eth0".into()), vec!["10.42.1.7".to_string()], true), ("longhorn-system/storage".into(), Some("net1".into()), vec!["10.0.20.31".to_string()], false)]);
        assert!(pod_networks(&json!({ "metadata": { "annotations": { "k8s.v1.cni.cncf.io/network-status": "not json" } } })).is_empty());
    }

    #[test]
    fn label_selectors_match_labels_and_expressions() {
        let labels = json!({ "app": "web", "tier": "front" });
        let m = |s: Value| label_selector_matches(&s, &labels);
        assert!(m(json!({})), "an empty selector picks every pod");
        assert!(m(json!({ "matchLabels": { "app": "web" } })));
        assert!(!m(json!({ "matchLabels": { "app": "db" } })));
        assert!(m(json!({ "matchExpressions": [{ "key": "tier", "operator": "In", "values": ["front", "edge"] }, { "key": "canary", "operator": "DoesNotExist" }] })));
        assert!(!m(json!({ "matchExpressions": [{ "key": "tier", "operator": "NotIn", "values": ["front"] }] })));
        assert!(!m(json!({ "matchExpressions": [{ "key": "canary", "operator": "Exists" }] })));
        assert!(!m(Value::Null));
    }

    #[test]
    fn a_disruption_budget_over_the_pods_is_found() {
        let pdbs = json!({ "items": [
            { "metadata": { "namespace": "other", "name": "web" }, "spec": { "selector": { "matchLabels": { "app": "web" } } }, "status": {} },
            { "metadata": { "namespace": "demo", "name": "web" }, "spec": { "selector": { "matchLabels": { "app": "web" } } }, "status": { "disruptionsAllowed": 0, "currentHealthy": 1, "desiredHealthy": 1 } },
        ] });
        assert_eq!(pdb("demo", &json!({ "app": "web" }), &pdbs), Some(Pdb { name: "web".into(), disruptions_allowed: 0, healthy: 1, desired_healthy: 1 }));
        assert_eq!(pdb("demo", &json!({ "app": "db" }), &pdbs), None);
    }

    fn ts(t: &str) -> Option<u64> {
        t.parse().ok()
    }

    #[test]
    fn a_deployments_history_is_its_replicasets() {
        let deploy = json!({ "metadata": { "namespace": "demo", "name": "web", "annotations": { "deployment.kubernetes.io/revision": "3" } } });
        let rs = |n: u64, image: &str, replicas: u64| {
            json!({ "metadata": { "namespace": "demo", "name": format!("web-{n}"), "creationTimestamp": n.to_string(), "annotations": { "deployment.kubernetes.io/revision": n.to_string() },
                "ownerReferences": [{ "kind": "Deployment", "name": "web" }] },
                "spec": { "template": { "spec": { "containers": [{ "image": image }] } } }, "status": { "replicas": replicas } })
        };
        let other = json!({ "metadata": { "namespace": "demo", "name": "db-1", "annotations": { "deployment.kubernetes.io/revision": "9" }, "ownerReferences": [{ "kind": "Deployment", "name": "db" }] } });
        let rss = json!({ "items": [rs(1, "web:1", 0), rs(3, "web:3", 2), rs(2, "web:2", 0), other] });
        let h = revisions(&deploy, "Deployment", &rss, &Value::Null, ts);
        let rows: Vec<_> = h.iter().map(|r| (r.number, r.images[0].as_str(), r.created, r.pods, r.current)).collect();
        assert_eq!(rows, [(3, "web:3", Some(3), Some(2), true), (2, "web:2", Some(2), Some(0), false), (1, "web:1", Some(1), Some(0), false)]);
    }

    #[test]
    fn a_statefulsets_history_is_its_controller_revisions() {
        let sts = json!({ "metadata": { "namespace": "demo", "name": "pg" }, "status": { "updateRevision": "pg-7c9" } });
        let cr = |name: &str, n: u64, image: &str| {
            json!({ "metadata": { "namespace": "demo", "name": name, "ownerReferences": [{ "kind": "StatefulSet", "name": "pg" }] }, "revision": n,
                "data": { "spec": { "template": { "spec": { "containers": [{ "image": image }] } } } } })
        };
        let crs = json!({ "items": [cr("pg-5f1", 1, "pg:16"), cr("pg-7c9", 2, "pg:17")] });
        let rows: Vec<_> = revisions(&sts, "StatefulSet", &Value::Null, &crs, ts).iter().map(|r| (r.number, r.images.clone(), r.pods, r.current)).collect();
        assert_eq!(rows, [(2, vec!["pg:17".to_string()], None, true), (1, vec!["pg:16".to_string()], None, false)]);
        let ds = json!({ "metadata": { "namespace": "demo", "name": "pg" }, "status": {} });
        let current: Vec<_> = revisions(&ds, "StatefulSet", &Value::Null, &crs, ts).iter().map(|r| r.current).collect();
        assert_eq!(current, [true, false], "without an updateRevision the newest is current");
    }
}
