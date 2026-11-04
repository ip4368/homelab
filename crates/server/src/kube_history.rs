//! An hour of pod and workload usage, sampled by the Kubernetes watcher:
//! metrics-server and the kubelet keep no history, so the server does
//! (docs/plans/2026-11-04-workload-debug-design.md §3; step 3 of
//! the 2026-11-02 details design). Volatile, like the leader's host
//! history: `since` says from when this server has data.

use crate::kube::KubeView;
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

/// Samples per series: an hour at the 3 s poll.
pub const HISTORY_POINTS: usize = 1200;
/// The metric names a series can be asked for.
pub const KUBE_METRICS: [&str; 4] = ["cpu_milli", "memory_bytes", "net_rx_bps", "net_tx_bps"];

type Sample = (u64, [Option<f64>; 4]);

#[derive(Default)]
struct Inner {
    since: Option<u64>,
    /// `ns/name` → ring.
    pods: BTreeMap<String, VecDeque<Sample>>,
    /// `ns/Kind/name` → ring.
    workloads: BTreeMap<String, VecDeque<Sample>>,
}

fn push(map: &mut BTreeMap<String, VecDeque<Sample>>, key: String, sample: Sample) {
    let ring = map.entry(key).or_default();
    ring.push_back(sample);
    while ring.len() > HISTORY_POINTS {
        ring.pop_front();
    }
}

/// Pod and workload series, shared between the watcher and the HTTP API.
#[derive(Default)]
pub struct KubeHistory {
    inner: Mutex<Inner>,
}

impl KubeHistory {
    /// Samples every pod and workload in `view` (called after rates are applied).
    pub fn record(&self, view: &KubeView) {
        if view.at == 0 {
            return;
        }
        let f = |v: Option<u64>| v.map(|n| n as f64);
        let mut inner = self.inner.lock().expect("history lock");
        inner.since.get_or_insert(view.at);
        for p in &view.pods {
            push(&mut inner.pods, format!("{}/{}", p.namespace, p.name), (view.at, [f(p.cpu_milli), f(p.memory_bytes), f(p.net_rx_bps), f(p.net_tx_bps)]));
        }
        for w in &view.workloads {
            push(&mut inner.workloads, format!("{}/{}/{}", w.namespace, w.kind, w.name), (view.at, [f(w.cpu_milli), f(w.memory_bytes), f(w.net_rx_bps), f(w.net_tx_bps)]));
        }
        // Gone pods and workloads keep no history.
        let pods: std::collections::BTreeSet<String> = view.pods.iter().map(|p| format!("{}/{}", p.namespace, p.name)).collect();
        inner.pods.retain(|k, _| pods.contains(k));
        let workloads: std::collections::BTreeSet<String> = view.workloads.iter().map(|w| format!("{}/{}/{}", w.namespace, w.kind, w.name)).collect();
        inner.workloads.retain(|k, _| workloads.contains(k));
    }

    /// The answer to a series query: `{since, series: {metric: [[t, v], …]}}`,
    /// as the host metrics answer. `entity` is a pod `ns/name` or a workload
    /// `ns/Kind/name`, as `record` keys them.
    pub fn query(&self, entity: &str, metrics: &[String]) -> Result<Value, String> {
        if metrics.is_empty() {
            return Err(format!("m is required: some of {}", KUBE_METRICS.join(", ")));
        }
        for m in metrics {
            if !KUBE_METRICS.contains(&m.as_str()) {
                return Err(format!("unknown metric {m:?}: expected some of {}", KUBE_METRICS.join(", ")));
            }
        }
        let inner = self.inner.lock().expect("history lock");
        let ring = inner.pods.get(entity).or_else(|| inner.workloads.get(entity)).ok_or_else(|| format!("no history for {entity:?} yet"))?;
        let mut series = serde_json::Map::new();
        for m in metrics {
            let idx = KUBE_METRICS.iter().position(|k| k == m).expect("checked above");
            let points: Vec<(u64, f64)> = ring.iter().filter_map(|(t, vs)| vs[idx].map(|v| (*t, v))).collect();
            series.insert(m.clone(), serde_json::to_value(points).expect("points serialize"));
        }
        Ok(json!({ "since": inner.since.unwrap_or(0), "series": series }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::{Health, HealthState, KubePod, KubeWorkload, WorkloadKind};

    fn view(at: u64, cpu: Option<u64>, rx: Option<u64>) -> KubeView {
        let pod = KubePod {
            namespace: "demo".into(),
            name: "web-1".into(),
            owner: Some("Deployment/web".into()),
            node: None,
            status: "Running".into(),
            ready: 1,
            containers: 1,
            restarts: 0,
            created: None,
            cpu_milli: cpu,
            memory_bytes: Some(1024),
            cpu_limit_milli: None,
            memory_limit_bytes: None,
            cpu_request_milli: None,
            memory_request_bytes: None,
            net_rx_bps: rx,
            net_tx_bps: None,
            fs_used_bytes: None,
            detail: None,
            unschedulable: false,
            last_termination: None,
            uid: None,
            pod_ip: None,
            qos: None,
            service_account: None,
            container_details: vec![],
            init_containers: vec![],
        };
        KubeView {
            at,
            pods: vec![pod],
            workloads: vec![KubeWorkload {
                namespace: "demo".into(),
                name: "web".into(),
                kind: WorkloadKind::Deployment,
                desired: 1,
                ready: 1,
                updated: 1,
                health: Health { state: HealthState::Healthy, reason: None },
                cpu_milli: cpu,
                memory_bytes: Some(1024),
                cpu_limit_milli: None,
                memory_limit_bytes: None,
                cpu_request_milli: None,
                memory_request_bytes: None,
                net_rx_bps: rx,
                net_tx_bps: None,
                images: vec![],
                endpoints: vec![],
                conditions: vec![],
            }],
            ..KubeView::default()
        }
    }

    #[test]
    fn series_skip_unmeasured_points_and_gone_pods_are_forgotten() {
        let h = KubeHistory::default();
        h.record(&view(1000, Some(10), None));
        h.record(&view(4000, None, Some(300)));
        let q = h.query("demo/web-1", &["cpu_milli".into(), "net_rx_bps".into()]).unwrap();
        assert_eq!(q["since"], 1000);
        assert_eq!(q["series"]["cpu_milli"], json!([[1000, 10.0]]));
        assert_eq!(q["series"]["net_rx_bps"], json!([[4000, 300.0]]));
        let w = h.query("demo/Deployment/web", &["cpu_milli".into()]).unwrap();
        assert_eq!(w["series"]["cpu_milli"], json!([[1000, 10.0]]));
        h.record(&KubeView { at: 7000, ..KubeView::default() });
        assert!(h.query("demo/web-1", &["cpu_milli".into()]).is_err(), "the pod is gone");
    }

    #[test]
    fn unknown_entities_and_metrics_are_errors() {
        let h = KubeHistory::default();
        h.record(&view(1000, Some(1), None));
        assert!(h.query("demo/web-1", &["bogus".into()]).is_err());
        assert!(h.query("demo/web-1", &[]).is_err());
        assert!(h.query("demo/nope", &["cpu_milli".into()]).is_err());
        h.record(&view(0, Some(9), None));
        assert_eq!(h.query("demo/web-1", &["cpu_milli".into()]).unwrap()["series"]["cpu_milli"], json!([[1000, 1.0]]), "an unread view records nothing");
    }

    #[test]
    fn rings_cap_at_an_hour_of_points() {
        let h = KubeHistory::default();
        for i in 0..(HISTORY_POINTS as u64 + 5) {
            h.record(&view(1000 + i * 3000, Some(i), None));
        }
        let q = h.query("demo/web-1", &["cpu_milli".into()]).unwrap();
        let pts = q["series"]["cpu_milli"].as_array().unwrap();
        assert_eq!(pts.len(), HISTORY_POINTS);
        assert_eq!(pts[0], json!([1000 + 5 * 3000, 5.0]));
    }
}
