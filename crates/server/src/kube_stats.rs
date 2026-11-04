//! Per-pod network and storage usage from the kubelet Summary API
//! (`/api/v1/nodes/<node>/proxy/stats/summary`): cumulative network counters,
//! ephemeral storage in use, and how much of each volume is written. Rates
//! are worked out between reads, keyed by pod UID so a replacement pod
//! starts fresh (docs/plans/2026-11-04-workload-debug-design.md §3).

use crate::kube::KubeView;
use serde_json::Value;
use std::collections::BTreeMap;

/// One pod in one node's summary.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PodStat {
    pub namespace: String,
    pub name: String,
    pub rx_bytes: Option<u64>,
    pub tx_bytes: Option<u64>,
    /// Its logs and containers' writable layers.
    pub fs_used_bytes: Option<u64>,
    /// `(namespace, claim, used bytes)` for its PVC volumes.
    pub volumes: Vec<(String, String, u64)>,
}

fn num(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
}

fn sum_used(containers: &[Value], field: &str) -> Option<u64> {
    containers.iter().filter_map(|c| num(&c[field]["usedBytes"])).reduce(|a, b| a + b)
}

/// Every pod in `summaries` (node name, summary), by pod UID.
pub fn pod_stats(summaries: &[(String, Value)]) -> BTreeMap<String, PodStat> {
    let mut out = BTreeMap::new();
    for (_, summary) in summaries {
        for p in summary["pods"].as_array().into_iter().flatten() {
            let r = &p["podRef"];
            let (Some(uid), Some(namespace), Some(name)) =
                (r["uid"].as_str(), r["namespace"].as_str(), r["name"].as_str())
            else {
                continue;
            };
            let containers = p["containers"].as_array().map_or(&[][..], Vec::as_slice);
            let fs = match (sum_used(containers, "rootfs"), sum_used(containers, "logs")) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            }
            .or_else(|| num(&p["ephemeral-storage"]["usedBytes"]).or_else(|| num(&p["ephemeralStorage"]["usedBytes"])));
            let volumes = p["volume"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| {
                    let used = num(&v["usedBytes"])?;
                    let ns = v["pvcRef"]["namespace"].as_str()?.to_string();
                    let claim = v["pvcRef"]["name"].as_str()?.to_string();
                    Some((ns, claim, used))
                })
                .collect();
            out.insert(
                uid.to_string(),
                PodStat {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    rx_bytes: num(&p["network"]["rxBytes"]),
                    tx_bytes: num(&p["network"]["txBytes"]),
                    fs_used_bytes: fs,
                    volumes,
                },
            );
        }
    }
    out
}

/// Bytes of each claim in use, by `(namespace, claim)`.
pub fn claim_used(stats: &BTreeMap<String, PodStat>) -> BTreeMap<(String, String), u64> {
    stats.values().flat_map(|s| s.volumes.iter().map(|(ns, c, u)| ((ns.clone(), c.clone()), *u))).collect()
}

/// The previous reading of each pod's counters, between reads.
#[derive(Debug, Default)]
pub struct RateState {
    /// UID → (when, rx bytes, tx bytes).
    prev: BTreeMap<String, (u64, u64, u64)>,
}

fn rate(now: u64, bytes: u64, prev: Option<(u64, u64)>) -> Option<u64> {
    let (at, before) = prev?;
    if now <= at || bytes < before {
        return None;
    }
    Some(((bytes - before) as u128 * 1000 / (now - at) as u128) as u64)
}

impl RateState {
    /// Fills each pod's rates from `stats` read at `at`, then sums each
    /// workload's from the pods measured (as CPU usage is summed). Pods the
    /// stats do not name, and pods seen for the first time, read no rate.
    pub fn apply(&mut self, view: &mut KubeView, stats: &BTreeMap<String, PodStat>, at: u64) {
        let mut next = BTreeMap::new();
        for pod in &mut view.pods {
            let Some(uid) = pod.uid.clone() else { continue };
            let Some(s) = stats.get(&uid) else { continue };
            if pod.fs_used_bytes.is_none() {
                pod.fs_used_bytes = s.fs_used_bytes;
            }
            let before = self.prev.get(&uid).copied();
            if let (Some(rx), Some(tx)) = (s.rx_bytes, s.tx_bytes) {
                pod.net_rx_bps = rate(at, rx, before.map(|(t, r, _)| (t, r)));
                pod.net_tx_bps = rate(at, tx, before.map(|(t, _, x)| (t, x)));
                next.insert(uid, (at, rx, tx));
            }
        }
        self.prev = next;
        for w in &mut view.workloads {
            let owner = format!("{}/{}", w.kind, w.name);
            let mine = view.pods.iter().filter(|p| p.namespace == w.namespace && p.owner.as_deref() == Some(owner.as_str()));
            let (mut rx, mut tx) = (None, None);
            for p in mine {
                if let Some(r) = p.net_rx_bps {
                    rx = Some(rx.unwrap_or(0) + r);
                }
                if let Some(t) = p.net_tx_bps {
                    tx = Some(tx.unwrap_or(0) + t);
                }
            }
            w.net_rx_bps = rx;
            w.net_tx_bps = tx;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::{Health, HealthState, KubePod, KubeWorkload, WorkloadKind};
    use serde_json::json;

    fn summary(rx: u64, tx: u64) -> Vec<(String, Value)> {
        vec![(
            "ms01-01".into(),
            json!({ "pods": [{
                "podRef": { "name": "web-1", "namespace": "demo", "uid": "uid-1" },
                "network": { "rxBytes": rx, "txBytes": tx },
                "containers": [{ "rootfs": { "usedBytes": 100 }, "logs": { "usedBytes": 23 } }],
                "volume": [{ "name": "data", "pvcRef": { "name": "writer", "namespace": "demo" }, "usedBytes": 4096, "capacityBytes": 8192 }]
            }] }),
        )]
    }

    fn view() -> KubeView {
        let pod = |uid: &str, name: &str| KubePod {
            namespace: "demo".into(),
            name: name.into(),
            owner: Some("Deployment/web".into()),
            node: Some("ms01-01".into()),
            status: "Running".into(),
            ready: 1,
            containers: 1,
            restarts: 0,
            created: None,
            cpu_milli: None,
            memory_bytes: None,
            cpu_limit_milli: None,
            memory_limit_bytes: None,
            cpu_request_milli: None,
            memory_request_bytes: None,
            net_rx_bps: None,
            net_tx_bps: None,
            fs_used_bytes: None,
            detail: None,
            unschedulable: false,
            last_termination: None,
            uid: Some(uid.into()),
            pod_ip: None,
            qos: None,
            service_account: None,
            container_details: vec![],
            init_containers: vec![],
        };
        KubeView {
            pods: vec![pod("uid-1", "web-1"), pod("uid-2", "web-2")],
            workloads: vec![KubeWorkload {
                namespace: "demo".into(),
                name: "web".into(),
                kind: WorkloadKind::Deployment,
                desired: 2,
                ready: 2,
                updated: 2,
                health: Health { state: HealthState::Healthy, reason: None },
                cpu_milli: None,
                memory_bytes: None,
                cpu_limit_milli: None,
                memory_limit_bytes: None,
                cpu_request_milli: None,
                memory_request_bytes: None,
                net_rx_bps: None,
                net_tx_bps: None,
                images: vec![],
                endpoints: vec![],
                conditions: vec![],
            }],
            ..KubeView::default()
        }
    }

    #[test]
    fn summaries_give_counters_storage_and_claim_use() {
        let stats = pod_stats(&summary(1000, 500));
        let s = &stats["uid-1"];
        assert_eq!((s.rx_bytes, s.tx_bytes, s.fs_used_bytes), (Some(1000), Some(500), Some(123)));
        assert_eq!(claim_used(&stats)[&("demo".into(), "writer".into())], 4096);
        assert!(pod_stats(&[]).is_empty());
        assert!(pod_stats(&[("n".into(), json!({}))]).is_empty(), "a summary with no pods");
    }

    #[test]
    fn rates_come_from_the_reading_before_and_resets_read_none() {
        let mut state = RateState::default();
        let mut v = view();
        state.apply(&mut v, &pod_stats(&summary(1000, 500)), 10_000);
        assert_eq!((v.pods[0].net_rx_bps, v.pods[0].fs_used_bytes), (None, Some(123)), "first sighting: no rate yet");
        assert_eq!(v.workloads[0].net_rx_bps, None);
        // 6 s later: 3000 more rx bytes, 6000 more tx.
        state.apply(&mut v, &pod_stats(&summary(4000, 6500)), 16_000);
        assert_eq!((v.pods[0].net_rx_bps, v.pods[0].net_tx_bps), (Some(500), Some(1000)));
        assert_eq!((v.workloads[0].net_rx_bps, v.workloads[0].net_tx_bps), (Some(500), Some(1000)), "summed over measured pods");
        assert_eq!(v.pods[1].net_rx_bps, None, "a pod the stats do not name");
        // Counters go backwards (a fresh sandbox under the same UID): no rate.
        state.apply(&mut v, &pod_stats(&summary(10, 10)), 22_000);
        assert_eq!(v.pods[0].net_rx_bps, None);
        // A vanished pod is forgotten.
        state.apply(&mut v, &BTreeMap::new(), 28_000);
        state.apply(&mut v, &pod_stats(&summary(9000, 9000)), 34_000);
        assert_eq!(v.pods[0].net_rx_bps, None, "back after a gap counts as first sighting");
    }
}
