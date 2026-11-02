//! What the Workloads tab says about a workload beyond its ready count: its
//! health and why, its CPU and memory, and how to reach it. Worked out from
//! the lists the server reads (docs/plans/2026-11-02-workload-details-design.md).

use crate::kube::{Endpoint, EndpointKind, Health, HealthState, KubePod};
use serde_json::Value;
use std::collections::BTreeMap;

/// A Kubernetes quantity in base units: `250m` is 0.25, `18Mi` is 18874368.
pub fn quantity(s: &str) -> Option<f64> {
    let s = s.trim();
    const BINARY: [(&str, f64); 6] = [("Ki", 1024.0), ("Mi", 1048576.0), ("Gi", 1073741824.0), ("Ti", 1099511627776.0), ("Pi", 1125899906842624.0), ("Ei", 1152921504606846976.0)];
    for (suffix, mult) in BINARY {
        if let Some(n) = s.strip_suffix(suffix) {
            return n.parse::<f64>().ok().map(|n| n * mult);
        }
    }
    if let Ok(n) = s.parse::<f64>() {
        return Some(n);
    }
    const DECIMAL: [(char, f64); 9] = [('n', 1e-9), ('u', 1e-6), ('m', 1e-3), ('k', 1e3), ('M', 1e6), ('G', 1e9), ('T', 1e12), ('P', 1e15), ('E', 1e18)];
    let last = s.chars().last()?;
    let (_, mult) = DECIMAL.iter().find(|(c, _)| *c == last)?;
    s[..s.len() - last.len_utf8()].parse::<f64>().ok().map(|n| n * mult)
}

/// A CPU quantity in millicores.
pub fn millicores(v: &Value) -> Option<u64> {
    v.as_str().and_then(quantity).map(|q| (q * 1000.0).round() as u64)
}

/// A memory quantity in bytes.
pub fn bytes(v: &Value) -> Option<u64> {
    v.as_str().and_then(quantity).map(|q| q.round() as u64)
}

/// Each pod's usage by `(namespace, name)`: millicores and bytes, summed over its containers.
pub fn pod_usage(metrics: &Value) -> BTreeMap<(String, String), (u64, u64)> {
    metrics["items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| {
            let key = (m["metadata"]["namespace"].as_str()?.to_string(), m["metadata"]["name"].as_str()?.to_string());
            let containers = m["containers"].as_array()?;
            let cpu = containers.iter().filter_map(|c| millicores(&c["usage"]["cpu"])).sum();
            let mem = containers.iter().filter_map(|c| bytes(&c["usage"]["memory"])).sum();
            Some((key, (cpu, mem)))
        })
        .collect()
}

/// A pod's limits (millicores, bytes): each the sum over its containers, none if one container has none.
pub fn pod_limits(pod: &Value) -> (Option<u64>, Option<u64>) {
    let containers = pod["spec"]["containers"].as_array().map_or(&[][..], Vec::as_slice);
    let sum = |f: &dyn Fn(&Value) -> Option<u64>| -> Option<u64> {
        if containers.is_empty() {
            return None;
        }
        containers.iter().map(f).sum()
    };
    (sum(&|c| millicores(&c["resources"]["limits"]["cpu"])), sum(&|c| bytes(&c["resources"]["limits"]["memory"])))
}

/// Container waiting reasons that will not clear by themselves.
const STUCK: [&str; 6] = ["CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError", "InvalidImageName", "RunContainerError"];
/// Pod statuses on the way to running.
const STARTING: [&str; 4] = ["Pending", "ContainerCreating", "PodInitializing", "Terminating"];

/// Why `p` will not become ready by itself, if it will not.
fn stuck(p: &KubePod) -> Option<String> {
    if STUCK.contains(&p.status.as_str()) {
        return Some(p.status.clone());
    }
    p.unschedulable.then(|| match &p.detail {
        Some(d) => format!("Unschedulable: {d}"),
        None => "Unschedulable".into(),
    })
}

/// What [`health`] needs to know of a workload.
pub struct HealthInput<'a> {
    pub desired: u32,
    pub ready: u32,
    pub updated: u32,
    /// The controller has seen the latest spec (`observedGeneration` ≥ `generation`).
    pub spec_seen: bool,
    /// What the controller itself reports wrong: `ReplicaFailure`, or a rollout past its deadline.
    pub controller_problem: Option<String>,
    pub pods: Vec<&'a KubePod>,
}

/// A workload's health, worst first (the design's §3).
pub fn health(i: &HealthInput) -> Health {
    let at = |state, reason: Option<String>| Health { state, reason };
    if i.desired == 0 {
        return at(HealthState::Idle, Some("scaled to 0".into()));
    }
    // The most common reason among stuck pods, with how many share it.
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    for r in i.pods.iter().filter_map(|p| stuck(p)) {
        *reasons.entry(r).or_default() += 1;
    }
    let stuck = reasons.into_iter().max_by_key(|(_, n)| *n).map(|(r, n)| if n > 1 { format!("{r} ({n} pods)") } else { r });
    if let Some(why) = stuck.or_else(|| i.controller_problem.clone()) {
        return at(if i.ready == 0 { HealthState::Down } else { HealthState::Degraded }, Some(why));
    }
    let starting = i.pods.iter().any(|p| STARTING.contains(&p.status.as_str()) || p.status.starts_with("Init:"));
    let rolling = i.updated < i.desired || !i.spec_seen || starting;
    if i.ready >= i.desired && !rolling {
        let restart = i.pods.iter().filter(|p| p.restarts > 0).find_map(|p| p.last_termination.as_ref()).filter(|t| t.reason == "OOMKilled" || t.reason == "Error");
        return at(HealthState::Healthy, restart.map(|t| format!("last restart: {}", t.reason)));
    }
    if rolling {
        let why = if i.updated < i.desired { format!("{}/{} updated", i.updated, i.desired) } else { "pods starting".into() };
        return at(HealthState::RollingOut, Some(why));
    }
    if i.ready == 0 {
        return at(HealthState::Down, Some("no pod ready".into()));
    }
    at(HealthState::Degraded, Some(format!("{}/{} ready", i.ready, i.desired)))
}

/// Whether `selector` picks pods labelled `labels`: a non-empty selector, every pair of it among the labels.
fn selects(selector: &Value, labels: &Value) -> bool {
    selector.as_object().is_some_and(|s| !s.is_empty() && s.iter().all(|(k, v)| labels.get(k) == Some(v)))
}

/// Whether a port is likely to speak HTTP, and over TLS.
fn http_port(name: Option<&str>, port: u64) -> Option<&'static str> {
    if port == 443 || port == 8443 || name.is_some_and(|n| n.starts_with("https")) {
        return Some("https");
    }
    if [80, 8080].contains(&port) || name.is_some_and(|n| n.starts_with("http") || n.starts_with("web")) {
        return Some("http");
    }
    None
}

/// How to reach the pods of a workload in `namespace` whose template is labelled
/// `labels`, through `services` and `ingresses`; `vip` stands in for the cluster's
/// own address (the design's §5). Ingresses first, in-cluster names last.
pub fn endpoints(namespace: &str, labels: &Value, services: &Value, ingresses: &Value, vip: &str) -> Vec<Endpoint> {
    let items = |list: &Value| list["items"].as_array().cloned().unwrap_or_default();
    let mine: Vec<Value> = items(services)
        .into_iter()
        .filter(|s| s["metadata"]["namespace"] == namespace && selects(&s["spec"]["selector"], labels))
        .collect();
    let names: Vec<&str> = mine.iter().filter_map(|s| s["metadata"]["name"].as_str()).collect();
    let mut out: Vec<Endpoint> = vec![];
    let mut push = |e: Endpoint| {
        if !out.iter().any(|o| o.address == e.address) {
            out.push(e);
        }
    };
    for ing in items(ingresses).iter().filter(|i| i["metadata"]["namespace"] == namespace) {
        let via = format!("Ingress {}", ing["metadata"]["name"].as_str().unwrap_or_default());
        let tls: Vec<&str> = ing["spec"]["tls"].as_array().into_iter().flatten().flat_map(|t| t["hosts"].as_array().into_iter().flatten()).filter_map(Value::as_str).collect();
        let default = ing["spec"]["defaultBackend"]["service"]["name"].as_str();
        if default.is_some_and(|d| names.contains(&d)) {
            let address = format!("http://{vip}/");
            push(Endpoint { link: Some(address.clone()), address, kind: EndpointKind::Ingress, via: via.clone() });
        }
        for rule in ing["spec"]["rules"].as_array().into_iter().flatten() {
            let host = rule["host"].as_str().filter(|h| !h.is_empty());
            let scheme = if host.is_some_and(|h| tls.contains(&h)) { "https" } else { "http" };
            for path in rule["http"]["paths"].as_array().into_iter().flatten() {
                if !path["backend"]["service"]["name"].as_str().is_some_and(|n| names.contains(&n)) {
                    continue;
                }
                let address = format!("{scheme}://{}{}", host.unwrap_or(vip), path["path"].as_str().unwrap_or("/"));
                push(Endpoint { link: Some(address.clone()), address, kind: EndpointKind::Ingress, via: via.clone() });
            }
        }
    }
    for kind in [EndpointKind::LoadBalancer, EndpointKind::NodePort, EndpointKind::Cluster] {
        for s in &mine {
            let name = s["metadata"]["name"].as_str().unwrap_or_default();
            let via = format!("Service {name}");
            let ty = s["spec"]["type"].as_str().unwrap_or("ClusterIP");
            for p in s["spec"]["ports"].as_array().into_iter().flatten() {
                let port_name = p["name"].as_str();
                let (address, external) = match kind {
                    EndpointKind::LoadBalancer if ty == "LoadBalancer" => (format!("{vip}:{}", p["port"]), true),
                    EndpointKind::NodePort if ty == "NodePort" => (format!("{vip}:{}", p["nodePort"]), true),
                    EndpointKind::Cluster if ty == "ClusterIP" => (format!("{name}.{namespace}:{}", p["port"]), false),
                    _ => continue,
                };
                // Whether it speaks HTTP shows in the Service's own port, not the node port it is published on.
                let scheme = p["port"].as_u64().and_then(|n| http_port(port_name, n)).filter(|_| external);
                let link = scheme.map(|scheme| format!("{scheme}://{address}"));
                push(Endpoint { address, link, kind: kind.clone(), via: via.clone() });
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::Termination;
    use serde_json::json;

    #[test]
    fn quantities_are_read_in_base_units() {
        let q = |s| quantity(s).map(|v| (v * 1e9).round() / 1e9);
        assert_eq!(q("250m"), Some(0.25));
        assert_eq!(q("12345678n"), Some(0.012345678));
        assert_eq!(q("0.5"), Some(0.5));
        assert_eq!(q("2"), Some(2.0));
        assert_eq!(q("18Mi"), Some(18.0 * 1048576.0));
        assert_eq!(q("123456Ki"), Some(123456.0 * 1024.0));
        assert_eq!(q("1Gi"), Some(1073741824.0));
        assert_eq!(q("1e3"), Some(1000.0));
        assert_eq!(q("1M"), Some(1e6));
        assert_eq!(q("lots"), None);
        assert_eq!(millicores(&json!("1500m")), Some(1500));
        assert_eq!(bytes(&json!("64Mi")), Some(64 << 20));
        assert_eq!(millicores(&json!(null)), None);
    }

    #[test]
    fn usage_is_summed_over_containers_and_limits_need_every_container() {
        let metrics = json!({ "items": [{ "metadata": { "namespace": "demo", "name": "p" }, "containers": [
            { "usage": { "cpu": "3m", "memory": "10Mi" } }, { "usage": { "cpu": "1500000n", "memory": "1Mi" } } ] }] });
        assert_eq!(pod_usage(&metrics)[&("demo".into(), "p".into())], (5, 11 << 20));
        let limited = json!({ "spec": { "containers": [{ "resources": { "limits": { "cpu": "500m", "memory": "1Gi" } } }, { "resources": { "limits": { "cpu": "1", "memory": "1Gi" } } }] } });
        assert_eq!(pod_limits(&limited), (Some(1500), Some(2 << 30)));
        let partly = json!({ "spec": { "containers": [{ "resources": { "limits": { "cpu": "500m" } } }, { "resources": {} }] } });
        assert_eq!(pod_limits(&partly), (None, None));
    }

    fn pod(status: &str) -> KubePod {
        KubePod {
            namespace: "demo".into(),
            name: "p".into(),
            owner: None,
            node: None,
            status: status.into(),
            ready: u32::from(status == "Running"),
            containers: 1,
            restarts: 0,
            created: None,
            cpu_milli: None,
            memory_bytes: None,
            cpu_limit_milli: None,
            memory_limit_bytes: None,
            detail: None,
            unschedulable: false,
            last_termination: None,
        }
    }

    fn input<'a>(desired: u32, ready: u32, updated: u32, pods: Vec<&'a KubePod>) -> HealthInput<'a> {
        HealthInput { desired, ready, updated, spec_seen: true, controller_problem: None, pods }
    }

    #[test]
    fn health_says_what_is_wrong_worst_first() {
        let (run, crash, pull, creating) = (pod("Running"), pod("CrashLoopBackOff"), pod("ImagePullBackOff"), pod("ContainerCreating"));
        let h = |i: HealthInput| (health(&i).state, health(&i).reason);
        assert_eq!(h(input(0, 0, 0, vec![])), (HealthState::Idle, Some("scaled to 0".into())));
        assert_eq!(h(input(2, 2, 2, vec![&run, &run])), (HealthState::Healthy, None));
        assert_eq!(h(input(1, 0, 1, vec![&pull])), (HealthState::Down, Some("ImagePullBackOff".into())));
        assert_eq!(h(input(3, 1, 3, vec![&run, &crash, &crash])), (HealthState::Degraded, Some("CrashLoopBackOff (2 pods)".into())));
        assert_eq!(h(input(3, 3, 1, vec![&run, &run, &run])), (HealthState::RollingOut, Some("1/3 updated".into())));
        assert_eq!(h(input(2, 1, 2, vec![&run, &creating])), (HealthState::RollingOut, Some("pods starting".into())));
        assert_eq!(h(input(2, 1, 2, vec![&run])), (HealthState::Degraded, Some("1/2 ready".into())));
        assert_eq!(h(input(1, 0, 1, vec![])), (HealthState::Down, Some("no pod ready".into())));
        let stale = HealthInput { spec_seen: false, ..input(1, 1, 1, vec![&run]) };
        assert_eq!(health(&stale).state, HealthState::RollingOut);
        let failing = HealthInput { controller_problem: Some("ReplicaFailure: exceeded quota".into()), ..input(1, 0, 0, vec![]) };
        assert_eq!(h(failing), (HealthState::Down, Some("ReplicaFailure: exceeded quota".into())));
    }

    #[test]
    fn an_unschedulable_pod_says_why_and_a_restart_is_remembered() {
        let mut waiting = pod("Pending");
        waiting.unschedulable = true;
        waiting.detail = Some("0/4 nodes are available: 4 Insufficient memory.".into());
        assert_eq!(health(&input(1, 0, 1, vec![&waiting])).reason.as_deref(), Some("Unschedulable: 0/4 nodes are available: 4 Insufficient memory."));
        let mut oom = pod("Running");
        oom.restarts = 2;
        oom.last_termination = Some(Termination { reason: "OOMKilled".into(), exit_code: 137, at: None });
        let h = health(&input(1, 1, 1, vec![&oom]));
        assert_eq!((h.state, h.reason.as_deref()), (HealthState::Healthy, Some("last restart: OOMKilled")));
    }

    fn services() -> Value {
        json!({ "items": [
            { "metadata": { "namespace": "demo", "name": "web" }, "spec": { "type": "ClusterIP", "selector": { "app": "web" }, "ports": [{ "port": 80 }] } },
            { "metadata": { "namespace": "demo", "name": "web-lb" }, "spec": { "type": "LoadBalancer", "selector": { "app": "web" }, "ports": [{ "port": 8096, "name": "http" }, { "port": 1900 }] } },
            { "metadata": { "namespace": "demo", "name": "web-np" }, "spec": { "type": "NodePort", "selector": { "app": "web", "tier": "front" }, "ports": [{ "port": 80, "nodePort": 30080 }] } },
            { "metadata": { "namespace": "demo", "name": "db" }, "spec": { "type": "ClusterIP", "selector": { "app": "db" }, "ports": [{ "port": 5432 }] } },
            { "metadata": { "namespace": "other", "name": "web" }, "spec": { "type": "ClusterIP", "selector": { "app": "web" }, "ports": [{ "port": 80 }] } },
            { "metadata": { "namespace": "demo", "name": "everything" }, "spec": { "type": "ClusterIP", "selector": {}, "ports": [{ "port": 1 }] } },
        ] })
    }

    fn ingresses() -> Value {
        json!({ "items": [
            { "metadata": { "namespace": "demo", "name": "web" }, "spec": { "rules": [{ "http": { "paths": [{ "path": "/", "backend": { "service": { "name": "web" } } }] } }] } },
            { "metadata": { "namespace": "demo", "name": "named" }, "spec": { "tls": [{ "hosts": ["web.home"] }], "rules": [
                { "host": "web.home", "http": { "paths": [{ "path": "/app", "backend": { "service": { "name": "web" } } }, { "path": "/db", "backend": { "service": { "name": "db" } } }] } } ] } },
        ] })
    }

    #[test]
    fn endpoints_come_from_the_services_that_select_the_pods() {
        let labels = json!({ "app": "web", "tier": "front" });
        let eps = endpoints("demo", &labels, &services(), &ingresses(), "10.0.3.253");
        let rows: Vec<_> = eps.iter().map(|e| (e.kind.clone(), e.address.as_str(), e.link.as_deref(), e.via.as_str())).collect();
        assert_eq!(
            rows,
            [
                (EndpointKind::Ingress, "http://10.0.3.253/", Some("http://10.0.3.253/"), "Ingress web"),
                (EndpointKind::Ingress, "https://web.home/app", Some("https://web.home/app"), "Ingress named"),
                (EndpointKind::LoadBalancer, "10.0.3.253:8096", Some("http://10.0.3.253:8096"), "Service web-lb"),
                (EndpointKind::LoadBalancer, "10.0.3.253:1900", None, "Service web-lb"),
                (EndpointKind::NodePort, "10.0.3.253:30080", Some("http://10.0.3.253:30080"), "Service web-np"),
                (EndpointKind::Cluster, "web.demo:80", None, "Service web"),
            ]
        );
        let db = endpoints("demo", &json!({ "app": "db" }), &services(), &ingresses(), "10.0.3.253");
        let addrs: Vec<_> = db.iter().map(|e| e.address.as_str()).collect();
        assert_eq!(addrs, ["https://web.home/db", "db.demo:5432"]);
        assert!(endpoints("demo", &json!({ "app": "none" }), &services(), &ingresses(), "vip").is_empty(), "an empty selector selects nothing here");
    }
}
