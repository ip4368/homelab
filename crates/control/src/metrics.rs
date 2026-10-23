//! The leader's metrics history: a ring of the last [`HISTORY_POINTS`]
//! samples per host, and rollups over hosts computed on demand. It lives in
//! the leader's volatile memory only, so a new leader starts empty;
//! [`MetricsStore::since`] says from when it has data.
//!
//! Rollups align the hosts' samples on [`METRICS_MS`] buckets. Hosts sample
//! at their own phase, so a host with no sample in a bucket counts with its
//! sample from the bucket before (at most one interval old): the newest
//! bucket, still filling, is not short of the hosts yet to report. A bucket
//! older than the oldest sample a full ring still holds is left out, for
//! the same reason. Percentages and temperature are averaged over the
//! hosts; port rates and instance counts are summed.
//!
//! * `fleet`: every host.
//! * `role:<Role>`: the hosts holding the role.
//! * `switch:<id>`: port rates are those of the host ports cabled to the
//!   switch, by LLDP; every other metric is over the hosts whose mgmt port
//!   is cabled to it.
//!
//! Samples are kept compactly (values as `f32`, per-disk and per-port values
//! in one slice indexed by the host's disk and port names), as there are 360
//! per host and up to 500 hosts.

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
}

/// One sample, compactly. `values` holds `disks` disk percentages, then
/// `ports` receive rates, then `ports` transmit rates, each indexed like the
/// ring's names; `NaN` where the sample had no value (e.g. a failed disk).
#[derive(Debug, Clone, PartialEq)]
struct Row {
    at: SimTime,
    cpu_pct: f32,
    ram_pct: f32,
    temp_c: f32,
    instances: u32,
    disks: u16,
    ports: u16,
    values: Box<[f32]>,
}

impl Row {
    /// The metric in this sample; port rates only over the port indices
    /// `ports`, if given.
    fn value(&self, metric: Metric, ports: Option<&BTreeSet<usize>>) -> f64 {
        let (disks, n) = (usize::from(self.disks), usize::from(self.ports));
        let rates = |offset: usize| -> f64 {
            let counted = (0..n).filter(|i| ports.is_none_or(|ps| ps.contains(i)));
            counted.map(|i| self.values[offset + i]).filter(|v| !v.is_nan()).map(f64::from).sum()
        };
        match metric {
            Metric::CpuPct => f64::from(self.cpu_pct),
            Metric::RamPct => f64::from(self.ram_pct),
            Metric::TempC => f64::from(self.temp_c),
            Metric::DiskPct => self.values[..disks].iter().copied().filter(|v| !v.is_nan()).fold(0.0, f32::max).into(),
            Metric::PortRxBps => rates(disks),
            Metric::PortTxBps => rates(disks + n),
            Metric::Instances => f64::from(self.instances),
        }
    }
}

/// One host's history: the disk and port names it has reported, in order of
/// first appearance (only ever appended to, so an index keeps its meaning),
/// and its latest [`HISTORY_POINTS`] samples.
#[derive(Debug, Clone, PartialEq, Default)]
struct HostRing {
    disks: Vec<String>,
    ports: Vec<String>,
    rows: VecDeque<Row>,
}

impl HostRing {
    fn push(&mut self, s: &MetricSample) {
        fn index(names: &mut Vec<String>, name: &str) -> usize {
            names.iter().position(|n| n == name).unwrap_or_else(|| {
                names.push(name.to_string());
                names.len() - 1
            })
        }
        let disks: Vec<(usize, f32)> = s.disk_pct.iter().map(|(d, v)| (index(&mut self.disks, d), *v)).collect();
        let rx: Vec<(usize, f64)> = s.port_rx_bps.iter().map(|(p, v)| (index(&mut self.ports, p), *v)).collect();
        let tx: Vec<(usize, f64)> = s.port_tx_bps.iter().map(|(p, v)| (index(&mut self.ports, p), *v)).collect();
        let (nd, np) = (self.disks.len(), self.ports.len());
        let mut values = vec![f32::NAN; nd + 2 * np];
        for (i, v) in disks {
            values[i] = v;
        }
        for (i, v) in rx {
            values[nd + i] = v as f32;
        }
        for (i, v) in tx {
            values[nd + np + i] = v as f32;
        }
        self.rows.push_back(Row {
            at: s.at,
            cpu_pct: s.cpu_pct,
            ram_pct: s.ram_pct,
            temp_c: s.temp_c,
            instances: s.instances,
            disks: u16::try_from(nd).expect("fewer than 65536 disks"),
            ports: u16::try_from(np).expect("fewer than 65536 ports"),
            values: values.into_boxed_slice(),
        });
        while self.rows.len() > HISTORY_POINTS {
            self.rows.pop_front();
        }
    }
}

/// One host's part in a rollup: its samples, and (for a switch's port
/// rates) the indices of the ports that count.
struct Member<'a> {
    ring: &'a HostRing,
    ports: Option<BTreeSet<usize>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricsStore {
    /// When this store was created: the leader's election.
    created_at: SimTime,
    first_sample_at: Option<SimTime>,
    hosts: BTreeMap<HostId, HostRing>,
}

impl MetricsStore {
    pub fn new(created_at: SimTime) -> Self {
        Self { created_at, first_sample_at: None, hosts: BTreeMap::new() }
    }

    /// Adds `host`'s sample, dropping its oldest beyond [`HISTORY_POINTS`].
    pub fn record(&mut self, host: HostId, sample: MetricSample) {
        self.first_sample_at.get_or_insert(sample.at);
        self.hosts.entry(host).or_default().push(&sample);
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
                let rows = self.hosts.get(id).into_iter().flat_map(|r| &r.rows);
                return Ok(rows.map(|r| (r.at, r.value(metric, None))).collect());
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
                            (!ports.is_empty()).then(|| self.member(&h.id, Some(&ports))).flatten()
                        } else {
                            ports.contains(MGMT_NIC).then(|| self.member(&h.id, None)).flatten()
                        }
                    })
                    .collect()
            }
        };
        Ok(rollup(&members, metric))
    }

    /// `host`'s part in a rollup, counting only the ports named `ports`, if given.
    fn member(&self, host: &HostId, ports: Option<&BTreeSet<&str>>) -> Option<Member<'_>> {
        let ring = self.hosts.get(host)?;
        let ports = ports.map(|names| (0..ring.ports.len()).filter(|i| names.contains(ring.ports[*i].as_str())).collect());
        Some(Member { ring, ports })
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

/// For each [`METRICS_MS`] bucket some member sampled in, each member's
/// latest value in it, or else in the bucket before; summed or averaged
/// over the members. Buckets before the oldest one every full ring still
/// covers are dropped (some members' samples there are gone), then all but
/// the latest [`HISTORY_POINTS`]. A point is stamped with its bucket's start.
fn rollup(members: &[Member], metric: Metric) -> Vec<(SimTime, f64)> {
    let per_member: Vec<BTreeMap<u64, f64>> = members
        .iter()
        .map(|m| m.ring.rows.iter().map(|r| (r.at / METRICS_MS, r.value(metric, m.ports.as_ref()))).collect())
        .collect();
    let complete_from = members
        .iter()
        .filter(|m| m.ring.rows.len() >= HISTORY_POINTS)
        .filter_map(|m| m.ring.rows.front().map(|r| r.at / METRICS_MS))
        .max()
        .unwrap_or(0);
    let buckets: BTreeSet<u64> = per_member.iter().flat_map(|b| b.keys().copied()).filter(|b| *b >= complete_from).collect();
    let skip = buckets.len().saturating_sub(HISTORY_POINTS);
    buckets
        .into_iter()
        .skip(skip)
        .map(|bucket| {
            let held = |samples: &BTreeMap<u64, f64>| {
                samples.get(&bucket).or_else(|| bucket.checked_sub(1).and_then(|before| samples.get(&before))).copied()
            };
            let (sum, n) = per_member.iter().filter_map(held).fold((0.0, 0u32), |(sum, n), v| (sum + v, n + 1));
            (bucket * METRICS_MS, if metric.summed() { sum } else { sum / f64::from(n.max(1)) })
        })
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
        assert_eq!(m.hosts[&hid("a")].rows.len(), HISTORY_POINTS, "the ring itself is capped");
    }

    #[test]
    fn compact_rows_keep_each_disk_and_port_by_name() {
        let s = state(&[("a", &[], &[("eth0", "access-1"), ("sfp0", "core-1")])]);
        let mut m = MetricsStore::new(0);
        let mut first = sample(10_000, 1.0);
        rates(&mut first, &[("eth0", 4.0)]);
        m.record(hid("a"), first);
        // A port appears and a disk fails (no longer reported).
        let mut second = sample(20_000, 2.0);
        rates(&mut second, &[("sfp0", 1000.0), ("eth0", 8.0)]);
        second.disk_pct.remove("nvme1");
        m.record(hid("a"), second);
        let host = |metric: &str| m.series(&s, &MetricEntity::Host(hid("a")), metric).unwrap();
        assert_eq!(host("port_rx_bps"), [(10_000, 4.0), (20_000, 1008.0)]);
        assert_eq!(host("port_tx_bps"), [(10_000, 2.0), (20_000, 504.0)]);
        assert_eq!(host("disk_pct"), [(10_000, 30.0), (20_000, 10.0)], "the failed disk drops out");
        assert_eq!(host("instances"), [(10_000, 2.0), (20_000, 2.0)]);
        let core = m.series(&s, &MetricEntity::Switch(sid("core-1")), "port_rx_bps").unwrap();
        assert_eq!(core, [(10_000, 0.0), (20_000, 1000.0)], "sfp0 had no rate before it appeared");
        assert_eq!(m.hosts[&hid("a")].ports, ["eth0", "sfp0"]);
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
        // c has not reported in the newest bucket yet: it counts with its last sample.
        let newest = (20.0 + 40.0 + 50.0) / 3.0;
        assert_eq!(m.series(&s, &MetricEntity::Fleet, "cpu_pct").unwrap(), [(10_000, 30.0), (20_000, newest)]);
        assert_eq!(m.series(&s, &MetricEntity::Fleet, "instances").unwrap(), [(10_000, 6.0), (20_000, 6.0)], "counts add up");
        assert_eq!(m.series(&s, &MetricEntity::Role(Role::Storage), "cpu_pct").unwrap(), [(10_000, 30.0), (20_000, 35.0)]);
        assert_eq!(m.series(&s, &MetricEntity::Role(Role::Vm), "cpu_pct").unwrap(), []);
        // A sample is held for one interval only: a host gone silent drops out.
        m.record(hid("a"), sample(30_000, 30.0));
        assert_eq!(m.series(&s, &MetricEntity::Fleet, "instances").unwrap()[2], (30_000, 4.0), "b held, c gone");
    }

    #[test]
    fn summed_rollups_count_every_host_in_the_filling_bucket_and_at_the_ring_edge() {
        let s = state(&[("a", &[], &[("eth0", "access-1")]), ("b", &[], &[("eth0", "access-1")])]);
        let mut m = MetricsStore::new(0);
        // a samples early in each interval, b late, and both rings are full.
        // Once a sampled twice in an interval (jitter), so its ring reaches
        // back one interval less than b's.
        for i in 0..400u64 {
            let mut early = sample(i * METRICS_MS + 1000, 0.0);
            rates(&mut early, &[("eth0", 100.0)]);
            m.record(hid("a"), early.clone());
            if i == 200 {
                early.at += 5000;
                m.record(hid("a"), early);
            }
            let mut late = sample(i * METRICS_MS + 9000, 0.0);
            rates(&mut late, &[("eth0", 10.0)]);
            if i > 0 {
                m.record(hid("b"), late);
            }
        }
        // The newest bucket: a has reported, b (sampling later) not yet.
        let mut newest = sample(400 * METRICS_MS + 1000, 0.0);
        rates(&mut newest, &[("eth0", 100.0)]);
        m.record(hid("a"), newest);
        let rx = m.series(&s, &MetricEntity::Switch(sid("access-1")), "port_rx_bps").unwrap();
        assert!(rx.iter().all(|(_, v)| *v == 110.0), "no bucket is short of a host: {:?}", rx.iter().find(|(_, v)| *v != 110.0));
        assert_eq!(rx.last().unwrap().0, 400 * METRICS_MS);
        assert_eq!(rx.len(), HISTORY_POINTS - 1, "as far back as a's ring reaches");
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
