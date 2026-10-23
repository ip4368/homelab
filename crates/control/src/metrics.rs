//! The leader's metrics history: a ring of the last [`HISTORY_POINTS`]
//! samples per host, and rollups over hosts computed on demand. It lives in
//! the leader's volatile memory only, so a new leader starts empty;
//! [`MetricsStore::since`] says from when it has data.
//!
//! Rollups align the hosts' samples on [`METRICS_MS`] buckets. Percentages
//! and temperature are averaged over the hosts with a sample in a bucket;
//! port rates and instance counts are summed.
//!
//! * `fleet`: every host.
//! * `role:<Role>`: the hosts holding the role.
//! * `switch:<id>`: port rates are those of the host ports cabled to the
//!   switch, by LLDP; every other metric is over the hosts whose mgmt port
//!   is cabled to it.

use crate::agent::METRICS_MS;
use crate::proto::MetricSample;
use crate::runtime::SimTime;
use crate::state::{ClusterState, HostObj, MGMT_NIC};
use model::{HostId, Role, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::str::FromStr;
use ts_rs::TS;

/// Points kept per series: one sim-hour at the metrics interval.
pub const HISTORY_POINTS: usize = 360;

/// The metric names a series can be asked for. A host's `disk_pct` is its
/// fullest disk; its port rates are summed over its ports.
pub const METRICS: [&str; 7] = ["cpu_pct", "ram_pct", "temp_c", "disk_pct", "port_rx_bps", "port_tx_bps", "instances"];

/// What a series describes: one host, or a rollup over hosts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum MetricEntity {
    Host(HostId),
    Fleet,
    Role(Role),
    Switch(SwitchId),
}

impl FromStr for MetricEntity {
    type Err = String;

    /// `host:<id>`, `fleet`, `role:<Role>` or `switch:<id>`.
    fn from_str(s: &str) -> Result<Self, String> {
        let entity = match s.split_once(':') {
            None if s == "fleet" => Some(Self::Fleet),
            Some(("host", id)) if !id.is_empty() => Some(Self::Host(HostId::new(id))),
            Some(("switch", id)) if !id.is_empty() => Some(Self::Switch(SwitchId::new(id))),
            Some(("role", role)) => serde_json::from_value(serde_json::Value::String(role.into())).ok().map(Self::Role),
            _ => None,
        };
        entity.ok_or_else(|| format!("unknown entity {s:?}: expected host:<id>, fleet, role:<Role> or switch:<id>"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Metric {
    CpuPct,
    RamPct,
    TempC,
    DiskPct,
    PortRxBps,
    PortTxBps,
    Instances,
}

impl Metric {
    fn parse(name: &str) -> Result<Self, String> {
        Ok(match name {
            "cpu_pct" => Self::CpuPct,
            "ram_pct" => Self::RamPct,
            "temp_c" => Self::TempC,
            "disk_pct" => Self::DiskPct,
            "port_rx_bps" => Self::PortRxBps,
            "port_tx_bps" => Self::PortTxBps,
            "instances" => Self::Instances,
            _ => return Err(format!("unknown metric {name:?}: expected one of {}", METRICS.join(", "))),
        })
    }

    /// Whether a rollup sums this metric over hosts (else it averages).
    fn summed(self) -> bool {
        matches!(self, Self::PortRxBps | Self::PortTxBps | Self::Instances)
    }

    fn is_port_rate(self) -> bool {
        matches!(self, Self::PortRxBps | Self::PortTxBps)
    }

    /// The metric in `s`; port rates only over `ports`, if given.
    fn value(self, s: &MetricSample, ports: Option<&BTreeSet<&str>>) -> f64 {
        let rate = |m: &BTreeMap<String, f64>| m.iter().filter(|(p, _)| ports.is_none_or(|ps| ps.contains(p.as_str()))).map(|(_, v)| v).sum();
        match self {
            Self::CpuPct => f64::from(s.cpu_pct),
            Self::RamPct => f64::from(s.ram_pct),
            Self::TempC => f64::from(s.temp_c),
            Self::DiskPct => s.disk_pct.values().copied().fold(0.0, f32::max).into(),
            Self::PortRxBps => rate(&s.port_rx_bps),
            Self::PortTxBps => rate(&s.port_tx_bps),
            Self::Instances => f64::from(s.instances),
        }
    }
}

/// One host's part in a rollup: its samples, and (for a switch's port
/// rates) the ports that count.
struct Member<'a> {
    ring: &'a VecDeque<MetricSample>,
    ports: Option<BTreeSet<&'a str>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsStore {
    /// When this store was created: the leader's election.
    created_at: SimTime,
    first_sample_at: Option<SimTime>,
    hosts: BTreeMap<HostId, VecDeque<MetricSample>>,
}

impl MetricsStore {
    pub fn new(created_at: SimTime) -> Self {
        Self { created_at, first_sample_at: None, hosts: BTreeMap::new() }
    }

    /// Adds `host`'s sample, dropping its oldest beyond [`HISTORY_POINTS`].
    pub fn record(&mut self, host: HostId, sample: MetricSample) {
        self.first_sample_at.get_or_insert(sample.at);
        let ring = self.hosts.entry(host).or_default();
        ring.push_back(sample);
        while ring.len() > HISTORY_POINTS {
            ring.pop_front();
        }
    }

    /// When this leader's history starts: its first sample, or its election
    /// while it has none.
    pub fn since(&self) -> SimTime {
        self.first_sample_at.unwrap_or(self.created_at)
    }

    /// `metric` of `entity` over time, oldest first, at most [`HISTORY_POINTS`]
    /// points. Membership in rollups (roles, LLDP cabling) is read from `state`.
    /// `Err` for an unknown metric, or a host or switch the state does not know.
    pub fn series(&self, state: &ClusterState, entity: &MetricEntity, metric: &str) -> Result<Vec<(SimTime, f64)>, String> {
        let metric = Metric::parse(metric)?;
        let members: Vec<Member> = match entity {
            MetricEntity::Host(id) => {
                if !state.hosts.contains_key(id) {
                    return Err(format!("unknown host {id}"));
                }
                let ring = self.hosts.get(id).into_iter().flatten();
                return Ok(ring.map(|s| (s.at, metric.value(s, None))).collect());
            }
            MetricEntity::Fleet => state.hosts.keys().filter_map(|id| self.member(id, None)).collect(),
            MetricEntity::Role(role) => {
                state.hosts.values().filter(|h| h.roles.contains(role)).filter_map(|h| self.member(&h.id, None)).collect()
            }
            MetricEntity::Switch(sw) => {
                let known = state.switches.contains_key(sw) || state.hosts.values().any(|h| h.lldp.iter().any(|n| n.switch == *sw));
                if !known {
                    return Err(format!("unknown switch {sw}"));
                }
                state
                    .hosts
                    .values()
                    .filter_map(|h| {
                        let ports = ports_on(h, sw);
                        if metric.is_port_rate() {
                            (!ports.is_empty()).then(|| self.member(&h.id, Some(ports))).flatten()
                        } else {
                            ports.contains(MGMT_NIC).then(|| self.member(&h.id, None)).flatten()
                        }
                    })
                    .collect()
            }
        };
        Ok(rollup(&members, metric))
    }

    fn member<'a>(&'a self, host: &HostId, ports: Option<BTreeSet<&'a str>>) -> Option<Member<'a>> {
        self.hosts.get(host).map(|ring| Member { ring, ports })
    }

    /// The answer to a series query: `{since, series: {metric: [[t, v], ...]}}`.
    pub fn query(&self, state: &ClusterState, entity: &MetricEntity, metrics: &[String]) -> Result<serde_json::Value, String> {
        let mut series = serde_json::Map::new();
        for m in metrics {
            let points = self.series(state, entity, m)?;
            series.insert(m.clone(), serde_json::to_value(points).expect("points serialize"));
        }
        Ok(serde_json::json!({ "since": self.since(), "series": series }))
    }
}

/// The ports of `host` cabled to `switch`, by LLDP.
fn ports_on<'a>(host: &'a HostObj, switch: &SwitchId) -> BTreeSet<&'a str> {
    host.lldp.iter().filter(|n| n.switch == *switch).map(|n| n.local_port.as_str()).collect()
}

/// Each member's samples by [`METRICS_MS`] bucket (its latest per bucket),
/// summed or averaged over the members, for the latest [`HISTORY_POINTS`]
/// buckets. A point is stamped with its bucket's start.
fn rollup(members: &[Member], metric: Metric) -> Vec<(SimTime, f64)> {
    let mut buckets: BTreeMap<u64, (f64, u32)> = BTreeMap::new();
    for m in members {
        let per_bucket: BTreeMap<u64, f64> = m.ring.iter().map(|s| (s.at / METRICS_MS, metric.value(s, m.ports.as_ref()))).collect();
        for (bucket, value) in per_bucket {
            let (sum, n) = buckets.entry(bucket).or_default();
            *sum += value;
            *n += 1;
        }
    }
    let skip = buckets.len().saturating_sub(HISTORY_POINTS);
    buckets
        .into_iter()
        .skip(skip)
        .map(|(bucket, (sum, n))| (bucket * METRICS_MS, if metric.summed() { sum } else { sum / f64::from(n) }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Command;
    use model::network::LldpNeighbor;
    use model::{profile, Inventory, PortName, ProfileId};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn sid(s: &str) -> SwitchId {
        SwitchId::from(s)
    }

    /// A host id, its roles, and its (port, switch) cables.
    type HostSpec<'a> = (&'a str, &'a [Role], &'a [(&'a str, &'a str)]);

    /// MS-01s with the given roles, cabled as listed.
    fn state(hosts: &[HostSpec]) -> ClusterState {
        let mut s = ClusterState::new();
        for (id, roles, cables) in hosts {
            let (id, p) = (hid(id), ProfileId::Ms01);
            let lldp = cables
                .iter()
                .map(|(port, sw)| LldpNeighbor { local_port: PortName::from(*port), switch: sid(sw), switch_port: PortName::from("p1") })
                .collect();
            let cmds = [
                Command::AddPendingHost { id: id.clone(), profile: profile(p, 0), inventory: Inventory::from_profile(p, 0), location: "rack-0".into() },
                Command::ApproveHost { id: id.clone(), roles: roles.iter().copied().collect() },
                Command::ObserveHost { id, lldp, inventory: Inventory::from_profile(p, 0) },
            ];
            for c in cmds {
                s.apply(0, c).unwrap();
            }
        }
        s
    }

    fn sample(at: SimTime, cpu_pct: f32) -> MetricSample {
        MetricSample {
            at,
            cpu_pct,
            ram_pct: 50.0,
            temp_c: 40.0,
            disk_pct: [("nvme0".into(), 10.0), ("nvme1".into(), 30.0)].into(),
            port_rx_bps: BTreeMap::new(),
            port_tx_bps: BTreeMap::new(),
            instances: 2,
        }
    }

    fn rates(s: &mut MetricSample, rx: &[(&str, f64)]) {
        s.port_rx_bps = rx.iter().map(|(p, v)| (p.to_string(), *v)).collect();
        s.port_tx_bps = rx.iter().map(|(p, v)| (p.to_string(), v / 2.0)).collect();
    }

    #[test]
    fn ring_caps_at_360() {
        let s = state(&[("a", &[], &[])]);
        let mut m = MetricsStore::new(500);
        assert_eq!(m.since(), 500, "the election, until there is a sample");
        for i in 0..400u64 {
            m.record(hid("a"), sample(1000 + i * METRICS_MS, i as f32));
        }
        let cpu = m.series(&s, &MetricEntity::Host(hid("a")), "cpu_pct").unwrap();
        assert_eq!(cpu.len(), HISTORY_POINTS);
        assert_eq!(cpu[0], (1000 + 40 * METRICS_MS, 40.0), "the oldest 40 are gone");
        assert_eq!(cpu[359], (1000 + 399 * METRICS_MS, 399.0));
        assert_eq!(m.series(&s, &MetricEntity::Fleet, "cpu_pct").unwrap().len(), HISTORY_POINTS);
        assert_eq!(m.since(), 1000, "the first sample this leader got");
        assert_eq!(m.series(&s, &MetricEntity::Host(hid("a")), "disk_pct").unwrap()[0].1, 30.0, "the fullest disk");
    }

    #[test]
    fn fleet_rollup_is_mean_cpu() {
        let s = state(&[("a", &[Role::Storage], &[]), ("b", &[Role::Compute], &[]), ("c", &[Role::Storage], &[])]);
        let mut m = MetricsStore::new(0);
        // Hosts sample at their own phase within each interval.
        m.record(hid("a"), sample(10_000, 10.0));
        m.record(hid("b"), sample(13_000, 30.0));
        m.record(hid("c"), sample(19_999, 50.0));
        m.record(hid("a"), sample(20_000, 20.0));
        m.record(hid("b"), sample(23_000, 40.0));
        assert_eq!(m.series(&s, &MetricEntity::Fleet, "cpu_pct").unwrap(), [(10_000, 30.0), (20_000, 30.0)]);
        assert_eq!(m.series(&s, &MetricEntity::Fleet, "instances").unwrap(), [(10_000, 6.0), (20_000, 4.0)], "counts add up");
        assert_eq!(m.series(&s, &MetricEntity::Role(Role::Storage), "cpu_pct").unwrap(), [(10_000, 30.0), (20_000, 20.0)]);
        assert_eq!(m.series(&s, &MetricEntity::Role(Role::Vm), "cpu_pct").unwrap(), []);
    }

    #[test]
    fn switch_rollup_sums_port_bps() {
        let s = state(&[
            ("a", &[], &[("sfp0", "core-1"), ("sfp1", "core-2"), ("eth0", "access-1")]),
            ("b", &[], &[("sfp0", "core-1"), ("sfp1", "core-2"), ("eth0", "access-1")]),
            ("c", &[], &[("eth0", "access-2")]),
        ]);
        let mut m = MetricsStore::new(0);
        for (h, rx, cpu) in [
            ("a", [("sfp0", 1000.0), ("sfp1", 3000.0), ("eth0", 10.0)], 10.0),
            ("b", [("sfp0", 2000.0), ("sfp1", 4000.0), ("eth0", 20.0)], 20.0),
            ("c", [("eth0", 5.0), ("eth1", 7.0), ("sfp9", 0.0)], 90.0),
        ] {
            let mut smp = sample(10_000, cpu);
            rates(&mut smp, &rx);
            m.record(hid(h), smp);
        }
        let series = |sw: &str, metric: &str| m.series(&s, &MetricEntity::Switch(sid(sw)), metric).unwrap();
        assert_eq!(series("core-1", "port_rx_bps"), [(10_000, 3000.0)]);
        assert_eq!(series("core-2", "port_tx_bps"), [(10_000, 3500.0)]);
        assert_eq!(series("access-1", "port_rx_bps"), [(10_000, 30.0)], "only the ports cabled to it");
        assert_eq!(series("access-2", "port_rx_bps"), [(10_000, 5.0)], "eth1 is cabled elsewhere, if at all");
        assert_eq!(series("access-1", "cpu_pct"), [(10_000, 15.0)], "the hosts whose mgmt port is on it");
        assert_eq!(series("core-1", "cpu_pct"), [], "no host has its mgmt port on a core");
        assert!(m.series(&s, &MetricEntity::Switch(sid("access-9")), "port_rx_bps").is_err());
    }

    #[test]
    fn unknown_metrics_hosts_and_entities_are_errors() {
        let s = state(&[("a", &[], &[])]);
        let mut m = MetricsStore::new(0);
        m.record(hid("a"), sample(10_000, 10.0));
        assert!(m.series(&s, &MetricEntity::Fleet, "bogus").is_err());
        assert!(m.series(&s, &MetricEntity::Host(hid("zz")), "cpu_pct").is_err());
        let ok = m.query(&s, &MetricEntity::Host(hid("a")), &["cpu_pct".into(), "port_rx_bps".into()]).unwrap();
        assert_eq!(ok, serde_json::json!({"since": 10_000, "series": {"cpu_pct": [[10_000, 10.0]], "port_rx_bps": [[10_000, 0.0]]}}));
        assert!(m.query(&s, &MetricEntity::Fleet, &["cpu_pct".into(), "nope".into()]).is_err());

        let parse = |s: &str| s.parse::<MetricEntity>();
        assert_eq!(parse("host:ms01-01"), Ok(MetricEntity::Host(hid("ms01-01"))));
        assert_eq!(parse("fleet"), Ok(MetricEntity::Fleet));
        assert_eq!(parse("role:ControlPlane"), Ok(MetricEntity::Role(Role::ControlPlane)));
        assert_eq!(parse("switch:core-1"), Ok(MetricEntity::Switch(sid("core-1"))));
        for bad in ["", "host:", "role:Nope", "rack:1", "fleet:x", "switch"] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
    }
}
