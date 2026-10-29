//! [`HostOs`] over a real Linux machine (a Debian guest in the lab):
//! inventory from `/sys`, network configs through systemd-networkd with
//! `netplan try` semantics, LLDP heard on every NIC, Raft state on disk,
//! data disks mounted under `/mnt/<name>`, and Kubernetes (K3s and
//! Longhorn) run by a worker thread ([`kube`]). Workloads run on
//! Kubernetes, not through the control plane.

pub mod kube;
pub mod lldp;
pub mod networkd;
pub mod packet;
pub mod store;
pub mod sysfs;

use crate::runtime::{NodeOs, VipControl};
use control::proto::{KubeAdmin, KubeSpec, KubeStatus, LogCmd};
use control::raft::{Durable, DurableOp};
use control::runtime::{HostOs, HostUsage};
use model::network::{bootstrap_config, HostNetworkConfig, LldpNeighbor, LogicalNetwork};
use model::{profile, HostId, Inventory, NetworkId};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::JoinHandle;
use store::{write_synced, RaftStore};
use sysfs::{Sys, UsageSampler};

/// Where a host keeps things.
#[derive(Debug, Clone)]
pub struct Paths {
    /// Survives reboots: `/var/lib/homelab`.
    pub state: PathBuf,
    /// Gone after a reboot: `/run/homelab`.
    pub run: PathBuf,
    /// Where networkd looks for runtime files: `/run/systemd/network`.
    pub networkd: PathBuf,
    /// Where data disks are mounted: `/mnt`.
    pub mnt: PathBuf,
    /// The root `/sys` and `/proc` are under: `/`.
    pub sys_root: PathBuf,
}

impl Default for Paths {
    fn default() -> Self {
        Self {
            state: "/var/lib/homelab".into(),
            run: "/run/homelab".into(),
            networkd: "/run/systemd/network".into(),
            mnt: "/mnt".into(),
            sys_root: "/".into(),
        }
    }
}

/// Runs a command, returning its stdout; a failure carries its stderr.
pub fn sh(cmd: &str, args: &[&str]) -> io::Result<String> {
    let out = Command::new(cmd).args(args).output()?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(io::Error::other(format!("{cmd} {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim())))
    }
}

fn load_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn save_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    write_synced(path, &serde_json::to_vec_pretty(value).map_err(io::Error::other)?)
}

pub struct LinuxOs {
    id: HostId,
    paths: Paths,
    sys: Sys,
    nics: Vec<String>,
    /// Every network definition the leader has sent, latest first.
    networks: BTreeMap<NetworkId, LogicalNetwork>,
    applied: HostNetworkConfig,
    /// The virtual interfaces the running config created, and their files.
    netdevs: BTreeMap<String, String>,
    raft: RaftStore,
    spawn: Option<BTreeSet<HostId>>,
    net_changed: bool,
    sampler: UsageSampler,
    /// Data disks ever seen: one missing now has failed.
    known_disks: BTreeSet<String>,
    /// `mkfs` runs off the event loop, by device: a big disk takes long
    /// enough to stall keepalives.
    formatting: BTreeMap<String, JoinHandle<io::Result<String>>>,
    speeds: BTreeMap<String, u32>,
    /// What the NICs hear over LLDP, once [`LinuxOs::boot`] started listening.
    lldp: Option<lldp::Lldp>,
    /// The cluster's VIP, which Kubernetes joins through.
    vip: Option<Ipv4Addr>,
    /// The Kubernetes worker, once [`LinuxOs::boot`] started it (it needs the VIP).
    kube: Option<kube::Kube>,
}

impl LinuxOs {
    pub fn new(id: HostId, paths: Paths) -> Self {
        let sys = Sys::new(&paths.sys_root);
        let nics = sys.nics(sys.profile());
        // The running copies (kept across a restart of this process, not a
        // reboot) first, else those written for the next boot.
        let networks = load_json(&paths.run.join("networks.json")).or_else(|| load_json(&paths.state.join("networks.json"))).unwrap_or_default();
        let known_disks = load_json(&paths.state.join("disks.json")).unwrap_or_default();
        let applied = load_json(&paths.run.join("net.json"))
            .or_else(|| load_json(&paths.state.join("net.json")))
            .unwrap_or_else(|| bootstrap_config(&profile(sys.profile(), 0)));
        let raft = RaftStore::new(paths.state.join("raft"));
        Self {
            id,
            paths,
            sys,
            nics,
            networks,
            applied,
            netdevs: BTreeMap::new(),
            raft,
            spawn: None,
            net_changed: false,
            sampler: UsageSampler::default(),
            known_disks,
            formatting: BTreeMap::new(),
            speeds: BTreeMap::new(),
            lldp: None,
            vip: None,
            kube: None,
        }
    }

    /// Tells the host the cluster's VIP, so [`LinuxOs::boot`] can start its
    /// Kubernetes worker.
    pub fn set_vip(&mut self, vip: Ipv4Addr) {
        self.vip = Some(vip);
    }

    /// Brings the machine to its configured state at boot: the running
    /// network config (the persisted one after a reboot, else the
    /// bootstrap) and the data disks mounted, listening for LLDP. Compacts
    /// the Raft store.
    pub fn boot(&mut self) {
        match lldp::Lldp::start(&self.nics) {
            Ok(l) => self.lldp = Some(l),
            Err(e) => eprintln!("homelabd: listen for LLDP: {e}"),
        }
        let (cfg, networks) = (self.applied.clone(), self.networks.clone());
        if let Err(e) = self.write_network(&cfg, &networks) {
            eprintln!("homelabd: boot network: {e}");
        }
        self.prepare_disks();
        if let Some(vip) = self.vip {
            self.kube = Some(kube::Kube::start(self.id.clone(), vip, self.paths.clone()));
        }
        if let Err(e) = self.raft.compact() {
            eprintln!("homelabd: compact raft store: {e}");
        }
    }

    pub fn joined(&self) -> bool {
        self.paths.state.join("joined").exists()
    }

    /// Renders `cfg`, swaps it in for networkd and reloads. Virtual
    /// interfaces the new config drops or changes are deleted first, since
    /// networkd neither removes nor rebuilds an existing one.
    fn write_network(&mut self, cfg: &HostNetworkConfig, networks: &BTreeMap<NetworkId, LogicalNetwork>) -> io::Result<()> {
        let files = networkd::render(cfg, networks, &self.nics).map_err(io::Error::other)?;
        let netdevs = networkd::netdevs(&files);
        fs::create_dir_all(&self.paths.networkd)?;
        // Already running it (homelabd restarted, the machine did not): a
        // reload would reconfigure the interfaces under everything using
        // them (K3s's flannel panics when its interface flaps).
        let on_disk: BTreeMap<String, String> = fs::read_dir(&self.paths.networkd)?
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with(networkd::PREFIX))
            .filter_map(|e| Some((e.file_name().to_string_lossy().into_owned(), fs::read_to_string(e.path()).ok()?)))
            .collect();
        let wanted: BTreeMap<String, String> = files.iter().map(|f| (f.name.clone(), f.body.clone())).collect();
        if on_disk == wanted {
            self.netdevs = netdevs;
            save_json(&self.paths.run.join("net.json"), cfg)?;
            save_json(&self.paths.run.join("networks.json"), networks)?;
            return Ok(());
        }
        for e in fs::read_dir(&self.paths.networkd)?.flatten() {
            if e.file_name().to_string_lossy().starts_with(networkd::PREFIX) {
                fs::remove_file(e.path())?;
            }
        }
        for f in &files {
            fs::write(self.paths.networkd.join(&f.name), &f.body)?;
        }
        let existing = self.existing_links();
        for (name, body) in &self.netdevs {
            if netdevs.get(name) != Some(body) && existing.contains(name) {
                let _ = sh("ip", &["link", "del", name]);
            }
        }
        // Bonds and VLANs left from before this process started, not in the new config.
        for name in existing.iter().filter(|n| (n.starts_with("bond") || n.contains('.')) && !netdevs.contains_key(*n)) {
            let _ = sh("ip", &["link", "del", name]);
        }
        sh("networkctl", &["reload"])?;
        self.netdevs = netdevs;
        save_json(&self.paths.run.join("net.json"), cfg)?;
        save_json(&self.paths.run.join("networks.json"), networks)?;
        Ok(())
    }

    fn existing_links(&self) -> BTreeSet<String> {
        fs::read_dir(self.sys.root().join("sys/class/net"))
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.file_name().into_string().ok())
            .collect()
    }

    /// Formats (if blank) and mounts every data disk at `/mnt/<name>`, and
    /// remembers it, so it shows as failed if it later disappears. A format
    /// runs on its own thread; the disk is mounted on a call after it ends.
    fn prepare_disks(&mut self) {
        let mounts = fs::read_to_string(self.sys.root().join("proc/mounts")).unwrap_or_default();
        let present = self.sys.disks();
        let mut learned = false;
        // A disk pulled and put back comes back as a new device: let go of the old mount.
        for name in &self.known_disks {
            let dir_s = self.paths.mnt.join(name).to_string_lossy().into_owned();
            let mounted_dev = mounts.lines().find(|l| l.split_whitespace().nth(1) == Some(dir_s.as_str())).and_then(|l| l.split_whitespace().next());
            if let Some(dev) = mounted_dev
                && !present.iter().any(|d| format!("/dev/{}", d.dev) == dev && &d.name == name)
            {
                let _ = sh("umount", &["-l", &dir_s]);
            }
        }
        let mounts = fs::read_to_string(self.sys.root().join("proc/mounts")).unwrap_or_default();
        for d in present {
            let dev = format!("/dev/{}", d.dev);
            let dir = self.paths.mnt.join(&d.name);
            let dir_s = dir.to_string_lossy().into_owned();
            if !mounts.lines().any(|l| l.split_whitespace().nth(1) == Some(dir_s.as_str())) {
                if let Some(f) = self.formatting.get(&dev) {
                    if !f.is_finished() {
                        continue;
                    }
                    let done = self.formatting.remove(&dev).expect("just seen").join();
                    if let Err(e) = done.unwrap_or_else(|_| Err(io::Error::other("mkfs panicked"))) {
                        eprintln!("homelabd: format {dev}: {e}");
                        continue;
                    }
                }
                // Format only a disk blkid calls blank (exit 2); any other
                // failure (I/O, ambiguous signatures) leaves it alone.
                match Command::new("blkid").arg(&dev).status().map(|s| s.code()) {
                    Ok(Some(0)) => {}
                    Ok(Some(2)) => {
                        let (label, target) = (d.name.clone(), dev.clone());
                        let job = std::thread::spawn(move || sh("mkfs.ext4", &["-q", "-L", &label, &target]));
                        self.formatting.insert(dev, job);
                        continue;
                    }
                    other => {
                        eprintln!("homelabd: probe {dev}: {other:?}; not formatting or mounting it");
                        continue;
                    }
                }
                let _ = fs::create_dir_all(&dir);
                if let Err(e) = sh("mount", &[&dev, &dir_s]) {
                    eprintln!("homelabd: mount {dev}: {e}");
                    continue;
                }
            }
            learned |= self.known_disks.insert(d.name);
        }
        if learned && let Err(e) = save_json(&self.paths.state.join("disks.json"), &self.known_disks) {
            eprintln!("homelabd: save disks: {e}");
        }
    }

    fn disk_pct(&self) -> BTreeMap<String, f32> {
        let mut out = BTreeMap::new();
        for d in self.sys.disks() {
            let path = self.paths.mnt.join(&d.name);
            let Ok(c) = std::ffi::CString::new(path.to_string_lossy().as_bytes()) else { continue };
            // SAFETY: `c` is a valid path string and `st` a valid out-pointer.
            let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
            if unsafe { libc::statvfs(c.as_ptr(), &mut st) } == 0 && st.f_blocks > 0 {
                out.insert(d.name, 100.0 * (st.f_blocks - st.f_bavail) as f32 / st.f_blocks as f32);
            }
        }
        out
    }
}

impl HostOs for LinuxOs {
    fn host_id(&self) -> HostId {
        self.id.clone()
    }

    fn inventory(&self) -> Inventory {
        let mut speeds = self.speeds.clone();
        sysfs::inventory(&self.sys, &self.known_disks, &mut speeds)
    }

    fn lldp(&self) -> Vec<LldpNeighbor> {
        let Some(l) = &self.lldp else { return vec![] };
        l.neighbours(&self.nics, |n| self.sys.nic_state(n).0)
    }

    fn apply_network(&mut self, cfg: HostNetworkConfig, networks: Vec<LogicalNetwork>) {
        let mut defs = self.networks.clone();
        defs.extend(networks.into_iter().map(|n| (n.id.clone(), n)));
        if cfg == self.applied && defs == self.networks {
            return;
        }
        match self.write_network(&cfg, &defs) {
            Ok(()) => {
                self.applied = cfg;
                self.networks = defs;
                self.net_changed = true;
            }
            Err(e) => eprintln!("homelabd: apply network: {e}"),
        }
    }

    fn applied_network(&self) -> HostNetworkConfig {
        self.applied.clone()
    }

    fn applied_networks(&self) -> Vec<LogicalNetwork> {
        self.networks.values().cloned().collect()
    }

    /// Writes the running config and the definitions it runs with, together, for the next boot.
    fn persist_network(&mut self) {
        let saved = save_json(&self.paths.state.join("networks.json"), &self.networks).and_then(|()| save_json(&self.paths.state.join("net.json"), &self.applied));
        if let Err(e) = saved {
            eprintln!("homelabd: persist network: {e}");
        }
    }

    fn usage(&mut self) -> HostUsage {
        self.prepare_disks();
        let (cpu_pct, port_bps) = self.sampler.sample(&self.sys, &self.nics);
        HostUsage { cpu_pct, ram_pct: self.sys.ram_used_pct(), disk_pct: self.disk_pct(), port_bps }
    }

    fn load_cp(&self) -> Option<Durable<LogCmd>> {
        self.raft.load().expect("the CP replica's Raft state is readable")
    }

    fn store_cp(&mut self, durable: Durable<LogCmd>) {
        self.raft.store(&durable).expect("the CP replica's Raft state is writable");
    }

    fn persist_cp(&mut self, op: DurableOp<LogCmd>) {
        self.raft.persist(&op).expect("the CP replica's Raft state is writable");
    }

    fn mark_joined(&mut self) {
        if let Err(e) = fs::create_dir_all(&self.paths.state).and_then(|()| write_synced(&self.paths.state.join("joined"), b"")) {
            eprintln!("homelabd: mark joined: {e}");
        }
    }

    fn apply_kube(&mut self, spec: KubeSpec) {
        if let Some(k) = &self.kube {
            k.want(spec);
        }
    }

    fn kube_admin(&mut self, admin: KubeAdmin) {
        if let Some(k) = &self.kube {
            k.admin(admin);
        }
    }

    fn request_wipe(&mut self) {
        if let Some(k) = &self.kube {
            k.request_wipe();
        }
    }

    fn kube_status(&self) -> Option<KubeStatus> {
        self.kube.as_ref()?.status()
    }

    fn kubeconfig(&self) -> Option<String> {
        kube::admin_kubeconfig(self.vip?).ok()
    }

    fn spawn_cp_replica(&mut self, voters: BTreeSet<HostId>) {
        if self.load_cp().is_none() {
            self.store_cp(Durable::new(voters.clone()));
            self.spawn = Some(voters);
        }
    }
}

impl NodeOs for LinuxOs {
    fn take_spawn(&mut self) -> Option<BTreeSet<HostId>> {
        self.spawn.take()
    }

    fn take_net_changed(&mut self) -> bool {
        std::mem::take(&mut self.net_changed)
    }
}

/// The VIP on a real host: a /32 on whichever interface carries an address
/// in the management subnet, announced with gratuitous ARP.
pub struct LinuxVip {
    pub vip: Ipv4Addr,
    pub mgmt: ipnet::Ipv4Net,
}

impl LinuxVip {
    /// Interfaces and their IPv4 addresses, from `ip -j -4 addr`.
    pub fn addresses() -> Vec<(String, Ipv4Addr)> {
        let Ok(out) = sh("ip", &["-j", "-4", "addr"]) else { return vec![] };
        let Ok(serde_json::Value::Array(links)) = serde_json::from_str(&out) else { return vec![] };
        let mut found = vec![];
        for l in links {
            let Some(name) = l["ifname"].as_str() else { continue };
            for a in l["addr_info"].as_array().into_iter().flatten() {
                if let Some(ip) = a["local"].as_str().and_then(|s| s.parse().ok()) {
                    found.push((name.to_string(), ip));
                }
            }
        }
        found
    }
}

impl LinuxVip {
    /// A gratuitous ARP for the VIP from `iface`.
    fn announce(&self, iface: &str) {
        match (packet::mac_of(iface), packet::RawSocket::open(iface, packet::ETH_P_ARP)) {
            (Ok(mac), Ok(sock)) => {
                // A few copies, as one lost frame would leave peers on the old holder.
                for _ in 0..3 {
                    let _ = sock.send(&packet::gratuitous_arp(mac, self.vip));
                }
            }
            (Err(e), _) | (_, Err(e)) => eprintln!("homelabd: announce VIP on {iface}: {e}"),
        }
    }
}

impl VipControl for LinuxVip {
    fn reassert(&mut self) {
        let addrs = Self::addresses();
        if addrs.iter().any(|(_, ip)| *ip == self.vip) {
            return;
        }
        self.hold(true);
    }

    fn hold(&mut self, hold: bool) {
        let addrs = Self::addresses();
        let vip = format!("{}/32", self.vip);
        if !hold {
            for (iface, _) in addrs.iter().filter(|(_, ip)| *ip == self.vip) {
                let _ = sh("ip", &["addr", "del", &vip, "dev", iface]);
            }
            return;
        }
        let Some((iface, _)) = addrs.iter().find(|(_, ip)| *ip != self.vip && self.mgmt.contains(ip)) else { return };
        if !addrs.iter().any(|(i, ip)| i == iface && *ip == self.vip) {
            for (other, _) in addrs.iter().filter(|(i, ip)| *ip == self.vip && i != iface) {
                let _ = sh("ip", &["addr", "del", &vip, "dev", other]);
            }
            if let Err(e) = sh("ip", &["addr", "add", &vip, "dev", iface]) {
                eprintln!("homelabd: take VIP: {e}");
                return;
            }
        }
        self.announce(iface);
    }
}
