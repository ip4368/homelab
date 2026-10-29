//! What the machine says about itself through `/sys` and `/proc`: its
//! hardware (for the inventory) and its load (for metrics). Every path is
//! under a root, so tests can build a fake tree.

use model::{profile, DiskKind, DiskState, Inventory, NicState, ProfileId};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Sys {
    root: PathBuf,
}

/// A data disk: its name (the serial the machine gives it, e.g. `nvme0`)
/// and its block device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockDisk {
    pub name: String,
    pub dev: String,
    pub size_gb: u32,
}

fn read(path: &Path) -> Option<String> {
    fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

/// The catalogue profile a DMI product name stands for.
pub fn profile_of(product: &str) -> ProfileId {
    let p = product.to_ascii_lowercase();
    if p.contains("ms-01") {
        ProfileId::Ms01
    } else if p.contains("raspberry pi 5") {
        ProfileId::Pi5
    } else if p.contains("rk3588") || p.contains("orange pi 5") {
        ProfileId::Rk3588
    } else {
        ProfileId::MiniPc
    }
}

/// A disk's kind, from the name its serial gives it.
pub fn disk_kind(name: &str) -> DiskKind {
    if name.starts_with("nvme") {
        DiskKind::Nvme
    } else if name.starts_with("emmc") {
        DiskKind::Emmc
    } else if name.starts_with("sd") && !name.starts_with("ssd") {
        DiskKind::Sd
    } else {
        DiskKind::Ssd
    }
}

impl Sys {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, p: &str) -> PathBuf {
        self.root.join(p.trim_start_matches('/'))
    }

    pub fn profile(&self) -> ProfileId {
        profile_of(&read(&self.path("/sys/class/dmi/id/product_name")).unwrap_or_default())
    }

    pub fn cpu_threads(&self) -> u32 {
        let info = read(&self.path("/proc/cpuinfo")).unwrap_or_default();
        info.lines().filter(|l| l.starts_with("processor")).count() as u32
    }

    /// Memory in whole GB, rounded up: the kernel keeps some for itself, so
    /// a 16 GB machine shows a little less.
    pub fn ram_gb(&self) -> u32 {
        let kb = meminfo(&self.path("/proc/meminfo")).get("MemTotal").copied().unwrap_or(0);
        kb.div_ceil(1024 * 1024) as u32
    }

    /// Physical NICs (those backed by a device), in the catalogue's order for
    /// `profile` first, then any others by name.
    pub fn nics(&self, profile: ProfileId) -> Vec<String> {
        let mut found: BTreeSet<String> = fs::read_dir(self.path("/sys/class/net"))
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.path().join("device").exists())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        let mut out = vec![];
        for n in model::profile(profile, 0).nics {
            if found.remove(&n.name) {
                out.push(n.name);
            }
        }
        out.extend(found);
        out
    }

    /// Carrier and speed of `nic`; `None` for each the kernel will not say
    /// (an interface that is down reports neither).
    pub fn nic_state(&self, nic: &str) -> (bool, Option<u32>) {
        let base = format!("/sys/class/net/{nic}");
        let carrier = read(&self.path(&format!("{base}/carrier"))).is_some_and(|c| c == "1");
        let speed = read(&self.path(&format!("{base}/speed"))).and_then(|s| s.parse::<i64>().ok()).filter(|&s| s > 0);
        (carrier, speed.map(|s| s as u32))
    }

    /// Data disks: block devices with a serial (the boot disk has none).
    pub fn disks(&self) -> Vec<BlockDisk> {
        let mut out: Vec<BlockDisk> = fs::read_dir(self.path("/sys/block"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let dev = e.file_name().into_string().ok()?;
                // The serial names its mount point: only plain names.
                let name = read(&e.path().join("serial")).filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'))?;
                let sectors: u64 = read(&e.path().join("size"))?.parse().ok()?;
                let size_gb = (sectors * 512).div_ceil(1 << 30) as u32;
                Some(BlockDisk { name, dev, size_gb })
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Cumulative (total, idle) CPU jiffies.
    pub fn cpu_jiffies(&self) -> (u64, u64) {
        let stat = read(&self.path("/proc/stat")).unwrap_or_default();
        let Some(cpu) = stat.lines().find(|l| l.starts_with("cpu ")) else { return (0, 0) };
        let v: Vec<u64> = cpu.split_whitespace().skip(1).filter_map(|x| x.parse().ok()).collect();
        let idle = v.get(3).copied().unwrap_or(0) + v.get(4).copied().unwrap_or(0);
        (v.iter().sum(), idle)
    }

    pub fn ram_used_pct(&self) -> f32 {
        let m = meminfo(&self.path("/proc/meminfo"));
        let (total, avail) = (m.get("MemTotal").copied().unwrap_or(0), m.get("MemAvailable").copied().unwrap_or(0));
        if total == 0 { 0.0 } else { 100.0 * (total - avail.min(total)) as f32 / total as f32 }
    }

    /// The hottest thermal zone, in °C; `None` on a machine with none (a VM).
    pub fn temp_c(&self) -> Option<f32> {
        let zones = fs::read_dir(self.path("/sys/class/thermal")).ok()?;
        zones
            .flatten()
            .filter(|z| z.file_name().to_string_lossy().starts_with("thermal_zone"))
            .filter_map(|z| read(&z.path().join("temp"))?.parse::<i64>().ok())
            .map(|milli| milli as f32 / 1000.0)
            .reduce(f32::max)
    }

    /// Cumulative (rx, tx) bytes through `nic`.
    pub fn nic_bytes(&self, nic: &str) -> (u64, u64) {
        let stat = |s: &str| read(&self.path(&format!("/sys/class/net/{nic}/statistics/{s}"))).and_then(|v| v.parse().ok()).unwrap_or(0);
        (stat("rx_bytes"), stat("tx_bytes"))
    }
}

fn meminfo(path: &Path) -> BTreeMap<String, u64> {
    read(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let (k, v) = l.split_once(':')?;
            Some((k.to_string(), v.split_whitespace().next()?.parse().ok()?))
        })
        .collect()
}

/// The inventory of a machine: its catalogue profile, its discovered CPU and
/// RAM, its NICs with carrier and negotiated speed, and its disks. A disk in
/// `known` (seen before) that is gone now is reported failed. A NIC whose
/// speed the kernel will not say keeps the last one seen in `speeds`, else
/// the catalogue's.
pub fn inventory(sys: &Sys, known: &BTreeSet<String>, speeds: &mut BTreeMap<String, u32>) -> Inventory {
    let id = sys.profile();
    let catalogue = profile(id, 0);
    let nics = sys
        .nics(id)
        .into_iter()
        .map(|name| {
            let (link_up, speed) = sys.nic_state(&name);
            let nominal = catalogue.nics.iter().find(|n| n.name == name).map_or(1000, |n| n.speed_mbps);
            let speed_mbps = match speed {
                Some(s) => {
                    speeds.insert(name.clone(), s);
                    s
                }
                None => speeds.get(&name).copied().unwrap_or(nominal),
            };
            NicState { name, speed_mbps, link_up }
        })
        .collect();
    let present = sys.disks();
    let mut disks: Vec<DiskState> =
        present.iter().map(|d| DiskState { name: d.name.clone(), kind: disk_kind(&d.name), size_gb: d.size_gb, failed: false }).collect();
    for gone in known.iter().filter(|k| !present.iter().any(|d| &d.name == *k)) {
        disks.push(DiskState { name: gone.clone(), kind: disk_kind(gone), size_gb: 0, failed: true });
    }
    disks.sort_by(|a, b| a.name.cmp(&b.name));
    Inventory { profile: id, variant: 0, cpu_threads: sys.cpu_threads(), ram_gb: sys.ram_gb(), nics, disks }
}

/// Turns cumulative counters into rates between calls.
#[derive(Debug, Default)]
pub struct UsageSampler {
    last: Option<(Instant, (u64, u64), BTreeMap<String, (u64, u64)>)>,
}

impl UsageSampler {
    /// CPU busy percent and per-NIC (rx, tx) bit/s since the previous call
    /// (zeros on the first).
    pub fn sample(&mut self, sys: &Sys, nics: &[String]) -> (f32, BTreeMap<String, (f64, f64)>) {
        let now = Instant::now();
        let cpu = sys.cpu_jiffies();
        let bytes: BTreeMap<String, (u64, u64)> = nics.iter().map(|n| (n.clone(), sys.nic_bytes(n))).collect();
        let mut cpu_pct = 0.0;
        let mut bps = BTreeMap::new();
        if let Some((at, (t0, i0), b0)) = &self.last {
            let (dt, di) = (cpu.0.saturating_sub(*t0), cpu.1.saturating_sub(*i0));
            if dt > 0 {
                cpu_pct = 100.0 * (dt - di.min(dt)) as f32 / dt as f32;
            }
            let secs = now.duration_since(*at).as_secs_f64().max(1e-3);
            for (n, (rx, tx)) in &bytes {
                let (rx0, tx0) = b0.get(n).copied().unwrap_or((*rx, *tx));
                bps.insert(n.clone(), (rx.saturating_sub(rx0) as f64 * 8.0 / secs, tx.saturating_sub(tx0) as f64 * 8.0 / secs));
            }
        } else {
            bps = nics.iter().map(|n| (n.clone(), (0.0, 0.0))).collect();
        }
        self.last = Some((now, cpu, bytes));
        (cpu_pct, bps)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Tree(PathBuf);

    impl Tree {
        fn new(name: &str) -> Self {
            let d = std::env::temp_dir().join(format!("homelab-sys-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&d);
            Self(d)
        }
        fn put(&self, path: &str, body: &str) -> &Self {
            let p = self.0.join(path.trim_start_matches('/'));
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, body).unwrap();
            self
        }
        fn nic(&self, name: &str, carrier: &str, speed: &str) -> &Self {
            self.put(&format!("/sys/class/net/{name}/device/vendor"), "0x1af4")
                .put(&format!("/sys/class/net/{name}/carrier"), carrier)
                .put(&format!("/sys/class/net/{name}/speed"), speed)
        }
        fn sys(&self) -> Sys {
            Sys::new(&self.0)
        }
    }

    fn ms01_vm(t: &Tree) {
        t.put("/sys/class/dmi/id/product_name", "MS-01\n")
            .put("/proc/cpuinfo", &(0..8).map(|i| format!("processor\t: {i}\nmodel name\t: x\n\n")).collect::<String>())
            .put("/proc/meminfo", "MemTotal:       16364112 kB\nMemFree:  100 kB\nMemAvailable:    8182056 kB\n")
            .nic("eth1", "0", "-1")
            .nic("sfp0", "1", "10000")
            .nic("eth0", "1", "2500")
            .nic("sfp1", "1", "10000")
            .put("/sys/class/net/bond0/carrier", "1")
            .put("/sys/class/net/lo/carrier", "1")
            .put("/sys/block/vda/size", "33554432")
            .put("/sys/block/vdb/serial", "nvme0")
            .put("/sys/block/vdb/size", "134217728")
            .put("/sys/block/vdc/serial", "nvme1")
            .put("/sys/block/vdc/size", "134217728");
    }

    #[test]
    fn a_vm_posing_as_an_ms01_reports_what_it_has() {
        let t = Tree::new("ms01");
        ms01_vm(&t);
        let mut speeds = BTreeMap::new();
        let inv = inventory(&t.sys(), &BTreeSet::new(), &mut speeds);
        assert_eq!((inv.profile, inv.cpu_threads, inv.ram_gb), (ProfileId::Ms01, 8, 16));
        let nics: Vec<_> = inv.nics.iter().map(|n| (n.name.as_str(), n.speed_mbps, n.link_up)).collect();
        assert_eq!(nics, [("sfp0", 10_000, true), ("sfp1", 10_000, true), ("eth0", 2_500, true), ("eth1", 2_500, false)]);
        let disks: Vec<_> = inv.disks.iter().map(|d| (d.name.as_str(), d.kind, d.size_gb, d.failed)).collect();
        assert_eq!(disks, [("nvme0", DiskKind::Nvme, 64, false), ("nvme1", DiskKind::Nvme, 64, false)]);
        assert_eq!(t.sys().disks()[0].dev, "vdb");
        assert!((t.sys().ram_used_pct() - 50.0).abs() < 0.1);
    }

    #[test]
    fn temperature_is_the_hottest_thermal_zone_and_none_without_one() {
        let t = Tree::new("thermal");
        ms01_vm(&t);
        assert_eq!(t.sys().temp_c(), None, "a VM has no sensor");
        t.put("/sys/class/thermal/thermal_zone0/temp", "41500\n").put("/sys/class/thermal/thermal_zone1/temp", "57000\n").put("/sys/class/thermal/cooling_device0/cur_state", "3\n");
        assert_eq!(t.sys().temp_c(), Some(57.0));
    }

    #[test]
    fn a_disk_that_vanished_is_failed_and_a_dark_nic_keeps_its_last_speed() {
        let t = Tree::new("failed");
        ms01_vm(&t);
        let mut speeds = BTreeMap::new();
        inventory(&t.sys(), &BTreeSet::new(), &mut speeds);
        fs::remove_dir_all(t.0.join("sys/block/vdc")).unwrap();
        t.put("/sys/class/net/sfp1/carrier", "0").put("/sys/class/net/sfp1/speed", "-1");
        let known: BTreeSet<String> = ["nvme0".into(), "nvme1".into()].into();
        let inv = inventory(&t.sys(), &known, &mut speeds);
        assert_eq!(inv.disks.iter().map(|d| (d.name.as_str(), d.failed)).collect::<Vec<_>>(), [("nvme0", false), ("nvme1", true)]);
        let sfp1 = inv.nics.iter().find(|n| n.name == "sfp1").unwrap();
        assert_eq!((sfp1.link_up, sfp1.speed_mbps), (false, 10_000));
    }

    #[test]
    fn rates_come_from_counter_deltas() {
        let t = Tree::new("rates");
        ms01_vm(&t);
        t.put("/proc/stat", "cpu  100 0 100 800 0 0 0 0 0 0\n")
            .put("/sys/class/net/eth0/statistics/rx_bytes", "1000")
            .put("/sys/class/net/eth0/statistics/tx_bytes", "0");
        let mut s = UsageSampler::default();
        let nics = vec!["eth0".to_string()];
        assert_eq!(s.sample(&t.sys(), &nics).0, 0.0);
        t.put("/proc/stat", "cpu  150 0 150 900 0 0 0 0 0 0\n").put("/sys/class/net/eth0/statistics/rx_bytes", "1000000");
        let (cpu, bps) = s.sample(&t.sys(), &nics);
        assert!((cpu - 50.0).abs() < 0.01, "{cpu}");
        assert!(bps["eth0"].0 > 0.0 && bps["eth0"].1 == 0.0);
    }

    #[test]
    fn profiles_and_disk_kinds_from_names() {
        assert_eq!(profile_of("MS-01"), ProfileId::Ms01);
        assert_eq!(profile_of("Generic mini PC"), ProfileId::MiniPc);
        assert_eq!(profile_of("Raspberry Pi 5 Model B"), ProfileId::Pi5);
        assert_eq!(profile_of("Orange Pi 5 Plus (RK3588)"), ProfileId::Rk3588);
        assert_eq!(disk_kind("nvme1"), DiskKind::Nvme);
        assert_eq!(disk_kind("sd0"), DiskKind::Sd);
        assert_eq!(disk_kind("emmc0"), DiskKind::Emmc);
        assert_eq!(disk_kind("ssd0"), DiskKind::Ssd);
    }
}
