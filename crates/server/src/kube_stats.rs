//! Each pod's network, disk and an hour of history, from the kubelet's
//! stats: `GET /api/v1/nodes/<node>/proxy/stats/summary`, every 15 s, one
//! request per node (docs/plans/2026-11-04-workload-insight-logs-terminal-design.md §4).
//! Kept in the server's memory only, like metrics-server's values.

use crate::kube::Access;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ts_rs::TS;

/// The pause between reads of the kubelets' stats (they refresh about that often).
pub const STATS_EVERY: Duration = Duration::from_secs(15);
/// How far back the history goes.
pub const HISTORY_MS: u64 = 60 * 60 * 1000;
/// The series `/api/kube/metrics` answers.
pub const METRICS: [&str; 4] = ["cpu_milli", "memory_bytes", "rx_bps", "tx_bps"];

/// How full a pod's volume is, as the kubelet sees its filesystem.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct VolumeUse {
    /// The pod's name for the volume.
    pub volume: String,
    /// The claim behind it, if it is one.
    pub claim: Option<String>,
    #[ts(type = "number")]
    pub used_bytes: u64,
    #[ts(type = "number | null")]
    pub capacity_bytes: Option<u64>,
}

/// What one summary says of one pod.
#[derive(Debug, Clone, PartialEq)]
pub struct PodSummary {
    pub namespace: String,
    pub name: String,
    /// When the kubelet took its network counters (Unix ms), if it says.
    pub net_at: Option<u64>,
    /// Bytes received and sent, over every interface.
    pub rx_bytes: Option<u64>,
    pub tx_bytes: Option<u64>,
    pub cpu_milli: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub ephemeral_bytes: Option<u64>,
    pub volumes: Vec<VolumeUse>,
}

/// A pod's figures now, for the view.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PodNow {
    /// Bits per second, as host port throughput.
    pub rx_bps: Option<f64>,
    pub tx_bps: Option<f64>,
    pub ephemeral_bytes: Option<u64>,
    pub volumes: Vec<VolumeUse>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Sample {
    at: u64,
    cpu_milli: Option<u64>,
    memory_bytes: Option<u64>,
    rx_bps: Option<f64>,
    tx_bps: Option<f64>,
}

#[derive(Debug, Default)]
struct PodHistory {
    node: String,
    /// The last counters: when (Unix ms) and bytes received and sent.
    counters: Option<(u64, u64, u64)>,
    samples: VecDeque<Sample>,
    now: PodNow,
}

/// Every pod's history, and which nodes did not answer last time.
#[derive(Debug, Default)]
pub struct Store {
    pods: BTreeMap<(String, String), PodHistory>,
    /// Why a node's kubelet gave no stats on the last read, by node.
    pub errors: BTreeMap<String, String>,
    /// When the history starts (Unix ms); 0 before the first read.
    since: u64,
}

pub type SharedStore = Arc<Mutex<Store>>;

fn text(v: &Value) -> Option<String> {
    v.as_str().filter(|s| !s.is_empty()).map(String::from)
}

/// Unix ms of an RFC 3339 time in UTC with optional fractions: `2026-11-04T10:00:05.123456Z`.
fn time_ms(t: &str) -> Option<u64> {
    let whole = t.get(..19)?;
    let secs = crate::kube::timestamp_ms(&format!("{whole}Z"))?;
    let frac = t[19..].strip_prefix('.').map(|f| f.trim_end_matches('Z')).unwrap_or("");
    let ms = frac.chars().chain("000".chars()).take(3).collect::<String>().parse::<u64>().ok()?;
    Some(secs + ms)
}

/// What a kubelet's summary says of each pod on its node.
pub fn parse_summary(summary: &Value) -> Vec<PodSummary> {
    summary["pods"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| {
            let net = &p["network"];
            let ifaces = net["interfaces"].as_array();
            // The interfaces when listed (Multus adds some); else the default one.
            let total = |field: &str| match ifaces {
                Some(list) if !list.is_empty() => list.iter().map(|i| i[field].as_u64()).sum::<Option<u64>>(),
                _ => net[field].as_u64(),
            };
            let volumes = p["volume"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| {
                    Some(VolumeUse {
                        volume: text(&v["name"])?,
                        claim: text(&v["pvcRef"]["name"]),
                        used_bytes: v["usedBytes"].as_u64()?,
                        capacity_bytes: v["capacityBytes"].as_u64(),
                    })
                })
                .collect();
            Some(PodSummary {
                namespace: text(&p["podRef"]["namespace"])?,
                name: text(&p["podRef"]["name"])?,
                net_at: net["time"].as_str().and_then(time_ms),
                rx_bytes: total("rxBytes"),
                tx_bytes: total("txBytes"),
                cpu_milli: p["cpu"]["usageNanoCores"].as_u64().map(|n| n / 1_000_000),
                memory_bytes: p["memory"]["workingSetBytes"].as_u64(),
                ephemeral_bytes: p["ephemeral-storage"]["usedBytes"].as_u64(),
                volumes,
            })
        })
        .collect()
}

/// Bits per second between two counter readings; none if time did not move or
/// the counter went down (the pod's sandbox was replaced, and counts from 0).
fn rate(then: (u64, u64), now: (u64, u64)) -> Option<f64> {
    let (t0, b0) = then;
    let (t1, b1) = now;
    (t1 > t0 && b1 >= b0).then(|| (b1 - b0) as f64 * 8.0 * 1000.0 / (t1 - t0) as f64)
}

impl Store {
    /// Takes in a read made at `at` (Unix ms): each node's summary, or why there is none.
    pub fn record(&mut self, at: u64, reads: Vec<(String, Result<Value, String>)>) {
        if self.since == 0 {
            self.since = at;
        }
        self.errors.clear();
        for (node, read) in reads {
            let summary = match read {
                Ok(s) => s,
                Err(e) => {
                    // Its pods' figures are unknown now; their history stays.
                    for h in self.pods.values_mut().filter(|h| h.node == node) {
                        h.now = PodNow::default();
                    }
                    self.errors.insert(node, e);
                    continue;
                }
            };
            let seen: BTreeSet<(String, String)> = parse_summary(&summary)
                .into_iter()
                .map(|p| {
                    let key = (p.namespace.clone(), p.name.clone());
                    let h = self.pods.entry(key.clone()).or_default();
                    h.node = node.clone();
                    let t = p.net_at.unwrap_or(at);
                    let (rx_bps, tx_bps) = match (h.counters, p.rx_bytes, p.tx_bytes) {
                        // The kubelet has not taken new counters since: the rate stands.
                        (Some((t0, _, _)), Some(_), Some(_)) if t == t0 => (h.now.rx_bps, h.now.tx_bps),
                        (Some((t0, rx0, tx0)), Some(rx), Some(tx)) => (rate((t0, rx0), (t, rx)), rate((t0, tx0), (t, tx))),
                        _ => (None, None),
                    };
                    if !h.counters.is_some_and(|(t0, _, _)| t0 == t) {
                        h.counters = p.rx_bytes.zip(p.tx_bytes).map(|(rx, tx)| (t, rx, tx));
                    }
                    h.samples.push_back(Sample { at, cpu_milli: p.cpu_milli, memory_bytes: p.memory_bytes, rx_bps, tx_bps });
                    h.now = PodNow { rx_bps, tx_bps, ephemeral_bytes: p.ephemeral_bytes, volumes: p.volumes };
                    key
                })
                .collect();
            // Pods the node no longer runs are gone.
            self.pods.retain(|k, h| h.node != node || seen.contains(k));
        }
        for h in self.pods.values_mut() {
            while h.samples.front().is_some_and(|s| s.at + HISTORY_MS < at) {
                h.samples.pop_front();
            }
        }
        self.pods.retain(|_, h| !h.samples.is_empty());
        self.since = self.since.max(at.saturating_sub(HISTORY_MS));
    }

    /// Pod `namespace/name`'s figures now, if its kubelet answered.
    pub fn now(&self, namespace: &str, name: &str) -> Option<&PodNow> {
        self.pods.get(&(namespace.to_string(), name.to_string())).map(|h| &h.now)
    }

    /// The figures summed over `pods` at each read, as `/api/metrics` answers
    /// host series: `{since, series: {metric: [[t, v], ...]}}`. A read counts
    /// in a series when at least one of the pods has that figure in it.
    pub fn series(&self, namespace: &str, pods: &[String]) -> Value {
        let mut sums: BTreeMap<u64, [Option<f64>; 4]> = BTreeMap::new();
        for name in pods {
            let Some(h) = self.pods.get(&(namespace.to_string(), name.clone())) else { continue };
            for s in &h.samples {
                let row = sums.entry(s.at).or_default();
                let figures = [s.cpu_milli.map(|v| v as f64), s.memory_bytes.map(|v| v as f64), s.rx_bps, s.tx_bps];
                for (sum, v) in row.iter_mut().zip(figures) {
                    if let Some(v) = v {
                        *sum = Some(sum.unwrap_or(0.0) + v);
                    }
                }
            }
        }
        let series: serde_json::Map<String, Value> = METRICS
            .iter()
            .enumerate()
            .map(|(i, m)| (m.to_string(), Value::Array(sums.iter().filter_map(|(t, row)| row[i].map(|v| json!([t, v]))).collect())))
            .collect();
        json!({ "since": self.since, "series": series })
    }
}

/// Reads the stats of every node in `nodes` at once, and records them in `store`.
pub async fn read(access: &Access, nodes: BTreeSet<String>, store: &SharedStore) {
    let mut reads = tokio::task::JoinSet::new();
    for node in nodes {
        let a = access.clone();
        reads.spawn(async move {
            let summary = a.stats(&node).await;
            (node, summary)
        });
    }
    let mut out = vec![];
    while let Some(r) = reads.join_next().await {
        if let Ok(r) = r {
            out.push(r);
        }
    }
    store.lock().expect("the stats store").record(node::runtime::wall_ms(), out);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(t: &str, pods: Vec<Value>) -> Value {
        let pods: Vec<Value> = pods.into_iter().map(|mut p| {
            p["network"]["time"] = json!(t);
            p
        }).collect();
        json!({ "node": { "nodeName": "ms01-01" }, "pods": pods })
    }

    fn pod(name: &str, rx: u64, tx: u64) -> Value {
        json!({
            "podRef": { "namespace": "demo", "name": name },
            "cpu": { "usageNanoCores": 25_000_000 },
            "memory": { "workingSetBytes": 1_000_000 },
            "network": { "interfaces": [{ "name": "eth0", "rxBytes": rx, "txBytes": tx }] },
            "ephemeral-storage": { "usedBytes": 4096 },
            "volume": [
                { "name": "data", "pvcRef": { "name": "data-pg-0", "namespace": "demo" }, "usedBytes": 700, "capacityBytes": 1000 },
                { "name": "kube-api-access-x", "usedBytes": 12 },
            ],
        })
    }

    #[test]
    fn times_are_read_with_their_fractions() {
        assert_eq!(time_ms("1970-01-01T00:00:05Z"), Some(5000));
        assert_eq!(time_ms("1970-01-01T00:00:05.25Z"), Some(5250));
        assert_eq!(time_ms("1970-01-01T00:00:05.123456789Z"), Some(5123));
        assert_eq!(time_ms("soon"), None);
    }

    #[test]
    fn a_summary_gives_each_pods_counters_usage_and_volumes() {
        let mut two = pod("web", 100, 50);
        two["network"]["interfaces"].as_array_mut().unwrap().push(json!({ "name": "net1", "rxBytes": 1, "txBytes": 2 }));
        let [p] = parse_summary(&summary("1970-01-01T00:00:01Z", vec![two])).try_into().unwrap();
        assert_eq!((p.namespace.as_str(), p.name.as_str(), p.net_at), ("demo", "web", Some(1000)));
        assert_eq!((p.rx_bytes, p.tx_bytes), (Some(101), Some(52)), "every interface counts");
        assert_eq!((p.cpu_milli, p.memory_bytes, p.ephemeral_bytes), (Some(25), Some(1_000_000), Some(4096)));
        assert_eq!(p.volumes[0], VolumeUse { volume: "data".into(), claim: Some("data-pg-0".into()), used_bytes: 700, capacity_bytes: Some(1000) });
        assert_eq!(p.volumes[1].claim, None);
        let plain = json!({ "pods": [{ "podRef": { "namespace": "demo", "name": "x" }, "network": { "rxBytes": 9, "txBytes": 8 } }] });
        assert_eq!(parse_summary(&plain)[0].rx_bytes, Some(9), "without interfaces, the pod's own counters");
    }

    #[test]
    fn counters_become_rates_and_a_reset_gives_none() {
        let mut s = Store::default();
        s.record(15_000, vec![("ms01-01".into(), Ok(summary("1970-01-01T00:00:10Z", vec![pod("web", 1000, 0)])))]);
        assert_eq!(s.now("demo", "web").unwrap().rx_bps, None, "one reading is no rate");
        s.record(30_000, vec![("ms01-01".into(), Ok(summary("1970-01-01T00:00:20Z", vec![pod("web", 11_000, 500)])))]);
        let now = s.now("demo", "web").unwrap();
        assert_eq!((now.rx_bps, now.tx_bps), (Some(8000.0), Some(400.0)), "10 kB and 500 B in the kubelet's 10 s");
        assert_eq!(now.ephemeral_bytes, Some(4096));
        s.record(35_000, vec![("ms01-01".into(), Ok(summary("1970-01-01T00:00:20Z", vec![pod("web", 11_000, 500)])))]);
        assert_eq!(s.now("demo", "web").unwrap().rx_bps, Some(8000.0), "no new counters yet: the rate stands");
        s.record(45_000, vec![("ms01-01".into(), Ok(summary("1970-01-01T00:00:35Z", vec![pod("web", 200, 600)])))]);
        assert_eq!(s.now("demo", "web").unwrap().rx_bps, None, "the counter went down");
    }

    #[test]
    fn a_node_that_does_not_answer_leaves_its_pods_unknown_and_says_why() {
        let mut s = Store::default();
        s.record(1, vec![("ms01-01".into(), Ok(summary("1970-01-01T00:00:00Z", vec![pod("web", 0, 0)])))]);
        s.record(2, vec![("ms01-01".into(), Err("503".into()))]);
        assert_eq!(s.now("demo", "web"), Some(&PodNow::default()));
        assert_eq!(s.errors.get("ms01-01").map(String::as_str), Some("503"));
        s.record(3, vec![("ms01-01".into(), Ok(summary("1970-01-01T00:00:00Z", vec![])))]);
        assert_eq!(s.now("demo", "web"), None, "gone from its node");
        assert!(s.errors.is_empty());
    }

    #[test]
    fn a_workloads_series_sums_its_pods_and_keeps_an_hour() {
        let mut s = Store::default();
        let t = |secs: u64| format!("1970-01-01T{:02}:{:02}:{:02}Z", secs / 3600, secs / 60 % 60, secs % 60);
        for (i, at) in [10u64, 25, 3620].into_iter().enumerate() {
            let n = i as u64 + 1;
            let pods = vec![pod("web-a", n * 1000, 0), pod("web-b", n * 2000, 0), pod("db-0", 0, 0)];
            s.record(at * 1000, vec![("ms01-01".into(), Ok(summary(&t(at), pods)))]);
        }
        let v = s.series("demo", &["web-a".into(), "web-b".into(), "gone".into()]);
        assert_eq!(v["since"], json!(20_000), "an hour before the last read");
        assert_eq!(v["series"]["cpu_milli"], json!([[25_000, 50.0], [3_620_000, 50.0]]), "the read at 10 s is over an hour old");
        assert_eq!(v["series"]["memory_bytes"][0], json!([25_000, 2_000_000.0]));
        assert_eq!(v["series"]["rx_bps"], json!([[25_000, (1000.0 + 2000.0) * 8.0 / 15.0], [3_620_000, (1000.0 + 2000.0) * 8.0 / 3595.0]]));
    }
}
