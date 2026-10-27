//! The lab as one thing to start, poke and watch: switches, services and
//! VMs. Every operation is something done to hardware (power, a cable, a
//! disk, a switch port) or seen from outside it; none touches the cluster.

use crate::cloudinit::{self, Role};
use crate::qemu::{self, VmFiles};
use crate::services::{self, Daemon};
use crate::switch::core::{Core, Member, Peer, PortCounters, PortId};
use crate::switch::io::{open_tap, LabSwitch, Wire};
use crate::topology::{LabSpec, Mistake, VmSpec, VIP};
use model::network::SwitchPortCfg;
use model::{HostId, PortName, ProfileId, SwitchId};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use ts_rs::TS;

/// How long after the lab boots a VM its guest agent may stay silent
/// because it is still starting.
const BOOT_GRACE: Duration = Duration::from_secs(120);

pub const BASE_IMAGE: &str = "debian-13-genericcloud-amd64.qcow2";
const BASE_URL: &str = "https://cloud.debian.org/images/cloud/trixie/latest/debian-13-genericcloud-amd64.qcow2";

/// Something done to the lab's hardware, as the UI and the e2e run ask for it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum LabFault {
    PowerOn { vm: HostId },
    /// Pulls the plug.
    PowerOff { vm: HostId },
    /// Presses the power button: a clean shutdown.
    Shutdown { vm: HostId },
    /// Presses reset: an instant reboot.
    Reset { vm: HostId },
    /// Stops the VM's CPUs: it sees no time pass until thawed.
    Freeze { vm: HostId },
    Thaw { vm: HostId },
    /// Pulls the data disk (as if it died): the guest sees it vanish.
    FailDisk { vm: HostId, disk: String },
    /// Puts a pulled data disk back, with what was on it.
    RestoreDisk { vm: HostId, disk: String },
    Unplug { vm: HostId, nic: String },
    Replug { vm: HostId, nic: String },
    /// Forces the NIC's switch port down to `mbps`; `None` restores full speed.
    Degrade { vm: HostId, nic: String, mbps: Option<u32> },
    KillSwitch { switch: SwitchId },
    RestoreSwitch { switch: SwitchId },
    Partition { members: Vec<Member> },
    Heal,
    SetSwitchPort { switch: SwitchId, port: PortName, cfg: SwitchPortCfg },
    /// Racks, installs and boots a new machine that joins with a fresh token.
    AddVm { profile: ProfileId, mistake: Option<Mistake> },
}

#[derive(Debug, Clone)]
pub struct LabConfig {
    /// Disks, the base image, the SSH key: `/var/lib/homelab-lab`.
    pub state: PathBuf,
    /// Sockets and pid files: `/run/homelab-lab`.
    pub run: PathBuf,
    /// The `homelabd` build guests download.
    pub homelabd: PathBuf,
    /// The lab host's HTTP proxy, which the guests' relay forwards to.
    pub upstream_proxy: String,
}

/// A VM's power as QEMU reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Power {
    /// No QEMU process (never installed, powered off, or shut down).
    Off,
    Running,
    /// Frozen: QEMU stopped the CPUs; the machine sees no time pass.
    Paused,
    /// QEMU is there but did not answer.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct VmTruth {
    pub id: HostId,
    pub profile: ProfileId,
    pub installed: bool,
    pub power: Power,
    /// NICs whose cable is pulled.
    pub unplugged: Vec<String>,
    /// Its data disks, pulled ones included.
    pub disks: Vec<String>,
    pub failed_disks: Vec<String>,
    /// Whether the guest holds the VIP address (from inside, via the guest agent).
    pub holds_vip: bool,
    /// Workload instances the guest really runs (`homelab-inst-*` units).
    pub instances: Vec<String>,
    /// Guest clock minus lab host clock, when the guest agent answered.
    #[ts(type = "number | null")]
    pub clock_offset_ms: Option<i64>,
    /// Seconds since the guest booted, when the guest agent answered.
    #[ts(type = "number | null")]
    pub uptime_s: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct PortTruth {
    pub switch: SwitchId,
    pub port: PortName,
    /// The host and NIC cabled to it, if any.
    pub host: Option<HostId>,
    pub nic: Option<String>,
    pub link_up: bool,
    pub rx_bps: f64,
    pub tx_bps: f64,
    pub counters: PortCounters,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct SwitchTruth {
    pub id: SwitchId,
    pub up: bool,
}

/// What is really true in the lab, seen from outside the machines.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct LabTruth {
    pub vms: Vec<VmTruth>,
    pub switches: Vec<SwitchTruth>,
    pub ports: Vec<PortTruth>,
    pub partitions: Vec<Vec<Member>>,
    /// Every guest holding the VIP address now: more than one is a split.
    pub vip_holders: Vec<HostId>,
    /// VM instances running on more than one guest at once.
    pub split_brain_vms: Vec<String>,
    #[ts(type = "number")]
    pub at: u64,
}

struct VmState {
    spec: VmSpec,
    files: VmFiles,
    unplugged: BTreeSet<String>,
    failed_disks: BTreeSet<String>,
    /// When the lab last booted it (power on, reset): a guest agent silent
    /// since then is booting, not unreachable.
    booted: Option<Instant>,
}

/// A machine racked after the lab's first four, in `vms/added.json`, so a
/// restarted lab re-racks it (same name, cables and ports) and adopts it.
/// `lab reset` deletes it with the disks.
#[derive(Debug, Serialize, Deserialize)]
struct Added {
    profile: ProfileId,
    mistake: Option<Mistake>,
}

fn added_path(state: &Path) -> PathBuf {
    state.join("vms").join("added.json")
}

fn load_added(state: &Path) -> Vec<Added> {
    std::fs::read(added_path(state)).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default()
}

pub struct Lab {
    cfg: LabConfig,
    spec: Mutex<LabSpec>,
    switch: Arc<LabSwitch>,
    vms: Mutex<BTreeMap<HostId, VmState>>,
    switch_addrs: BTreeMap<SwitchId, Ipv4Addr>,
    ssh_pub: String,
    daemons: Vec<Daemon>,
    /// Held for the whole of each fault, so faults apply one at a time.
    faults: Mutex<()>,
    truth: Mutex<LabTruth>,
    /// Port byte counters at the previous truth sample, for rates.
    last_counters: Mutex<Option<(Instant, Vec<PortCounters>)>>,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn port_sock(run: &Path, switch: &SwitchId, port: &PortName) -> PathBuf {
    run.join("sw").join(format!("{switch}.{port}.sock"))
}

/// The base image, copied from the invoking user's cache or downloaded once.
fn base_image(state: &Path) -> io::Result<PathBuf> {
    let path = state.join(BASE_IMAGE);
    if path.exists() {
        return Ok(path);
    }
    if let Ok(user) = std::env::var("SUDO_USER") {
        let cached = PathBuf::from(format!("/home/{user}/.cache/homelab-lab/{BASE_IMAGE}"));
        if cached.exists() {
            std::fs::copy(&cached, &path)?;
            return Ok(path);
        }
    }
    eprintln!("lab: downloading {BASE_URL}");
    services::sh("curl", &["-fsSL", "-o", &path.to_string_lossy(), BASE_URL])?;
    Ok(path)
}

fn ssh_key(state: &Path) -> io::Result<String> {
    let key = state.join("id_ed25519");
    if !key.exists() {
        services::sh("ssh-keygen", &["-q", "-t", "ed25519", "-N", "", "-C", "homelab-lab", "-f", &key.to_string_lossy()])?;
    }
    std::fs::read_to_string(state.join("id_ed25519.pub"))
}

impl LabConfig {
    /// From `$LAB_STATE`, `$LAB_RUN`, `$LAB_HOMELABD` (defaulting to
    /// `/var/lib/homelab-lab`, `/run/homelab-lab` and this workspace's
    /// release build) and the upstream proxy ([`services::upstream_proxy`]).
    pub fn from_env() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let var = |k: &str, d: PathBuf| std::env::var(k).map_or(d, PathBuf::from);
        Self {
            state: var("LAB_STATE", "/var/lib/homelab-lab".into()),
            run: var("LAB_RUN", "/run/homelab-lab".into()),
            homelabd: var("LAB_HOMELABD", root.join("target/release/homelabd")),
            upstream_proxy: services::upstream_proxy(),
        }
    }
}

impl Lab {
    /// Carries out `fault`, except `AddVm`, which needs a join token from the
    /// cluster (see [`Lab::add_vm`]).
    pub fn apply(&self, fault: LabFault) -> io::Result<()> {
        let _one_at_a_time = self.faults.lock().expect("faults lock");
        match fault {
            LabFault::PowerOn { vm } => self.power_on(&vm),
            LabFault::PowerOff { vm } => self.power_off(&vm),
            LabFault::Shutdown { vm } => self.shutdown(&vm),
            LabFault::Reset { vm } => self.reset(&vm),
            LabFault::Freeze { vm } => self.freeze(&vm),
            LabFault::Thaw { vm } => self.thaw(&vm),
            LabFault::FailDisk { vm, disk } => self.set_disk(&vm, &disk, false),
            LabFault::RestoreDisk { vm, disk } => self.set_disk(&vm, &disk, true),
            LabFault::Unplug { vm, nic } => self.set_cable(&vm, &nic, false),
            LabFault::Replug { vm, nic } => self.set_cable(&vm, &nic, true),
            LabFault::Degrade { vm, nic, mbps } => self.degrade(&vm, &nic, mbps),
            LabFault::KillSwitch { switch } => self.set_switch_up(&switch, false),
            LabFault::RestoreSwitch { switch } => self.set_switch_up(&switch, true),
            LabFault::Partition { members } => self.partition(members.into_iter().collect()),
            LabFault::Heal => {
                self.heal();
                Ok(())
            }
            LabFault::SetSwitchPort { switch, port, cfg } => self.set_port(&switch, &port, cfg),
            LabFault::AddVm { .. } => Err(io::Error::other("AddVm needs a join token: use add_vm")),
        }
    }

    /// Brings up the lab's network and services (switches, router, DHCP,
    /// time, proxy, binary server, switch management) and adopts VMs still
    /// running from before. Boots nothing new. Needs root, and a tokio runtime.
    pub async fn start(cfg: LabConfig) -> io::Result<Arc<Lab>> {
        std::fs::create_dir_all(&cfg.state)?;
        std::fs::create_dir_all(cfg.run.join("sw"))?;
        let ssh_pub = ssh_key(&cfg.state)?;
        let mut spec = LabSpec::default_lab();
        for a in load_added(&cfg.state) {
            if let Err(e) = spec.add_vm(a.profile, a.mistake) {
                eprintln!("lab: re-rack a {:?}: {e}", a.profile);
            }
        }
        let mut core = Core::new(&spec.switches.iter().map(|s| (s.id.clone(), s.model.clone())).collect::<Vec<_>>(), spec.ports.clone(), Instant::now());
        for s in &spec.switches {
            core.set_mgmt_ip(&s.id, s.mgmt_ip);
        }
        let mut wires = vec![];
        let tap = open_tap(crate::topology::ROUTER_TAP)?;
        for (i, p) in spec.ports.iter().enumerate() {
            match &p.peer {
                Peer::Router => wires.push((i, Wire::Tap(tap.try_clone()?))),
                Peer::Switch { .. } => {}
                Peer::None | Peer::Host { .. } => wires.push((i, Wire::unix(&port_sock(&cfg.run, &p.switch, &p.name))?)),
            }
        }
        core.set_peer_up(spec.ports.iter().position(|p| p.peer == Peer::Router).expect("a router port"), true);
        let switch = Arc::new(LabSwitch::start(core, wires)?);
        drop(tap);
        services::configure_router(&spec.switches)?;
        let daemons = vec![services::start_dnsmasq(&cfg.run)?, services::start_chronyd(&cfg.run)?];
        services::run_relay(cfg.upstream_proxy.clone()).await?;
        services::run_binary_server(cfg.homelabd.clone()).await?;
        services::run_switch_mgmt(spec.switches.clone(), switch.clone()).await?;
        let switch_addrs = spec.switches.iter().map(|s| (s.id.clone(), s.mgmt_ip)).collect();
        let vms = spec
            .vms
            .iter()
            .map(|v| {
                let files = VmFiles::new(&cfg.state, &cfg.run, v);
                (v.id.clone(), VmState { spec: v.clone(), files, unplugged: BTreeSet::new(), failed_disks: BTreeSet::new(), booted: None })
            })
            .collect();
        let lab = Arc::new(Lab {
            cfg,
            spec: Mutex::new(spec),
            switch,
            vms: Mutex::new(vms),
            switch_addrs,
            ssh_pub,
            daemons,
            faults: Mutex::new(()),
            truth: Mutex::new(LabTruth { vms: vec![], switches: vec![], ports: vec![], partitions: vec![], vip_holders: vec![], split_brain_vms: vec![], at: 0 }),
            last_counters: Mutex::new(None),
        });
        lab.cable_all();
        lab.adopt();
        lab.refresh_links();
        Ok(lab)
    }

    /// Takes over VMs still running from a previous lab process. Faults it
    /// set end with it, as if the switches were restarted and cables pushed
    /// back in, except pulled disks, which are found missing from QEMU.
    fn adopt(&self) {
        let running: Vec<(HostId, VmFiles)> = self.vms.lock().expect("vms lock").iter().filter(|(_, v)| qemu::running(&v.files)).map(|(id, v)| (id.clone(), v.files.clone())).collect();
        for (id, f) in running {
            let Ok(drives) = qemu::drives(&f) else { continue };
            if let Some(v) = self.vms.lock().expect("vms lock").get_mut(&id) {
                v.failed_disks = v.spec.disks.iter().map(|(d, _)| d.clone()).filter(|d| !drives.contains(&format!("drv-{d}"))).collect();
            }
        }
        self.sync_carrier();
    }

    /// Stops the lab's daemons (VMs keep running until powered off).
    pub fn stop_services(&self) {
        for d in &self.daemons {
            d.stop();
        }
    }

    pub fn config(&self) -> &LabConfig {
        &self.cfg
    }

    pub fn vm_ids(&self) -> Vec<HostId> {
        self.vms.lock().expect("vms lock").keys().cloned().collect()
    }

    pub fn spec(&self, id: &HostId) -> Option<VmSpec> {
        self.vms.lock().expect("vms lock").get(id).map(|v| v.spec.clone())
    }

    fn files(&self, id: &HostId) -> io::Result<VmFiles> {
        self.vms.lock().expect("vms lock").get(id).map(|v| v.files.clone()).ok_or_else(|| io::Error::other(format!("no VM {id}")))
    }

    fn port_of(&self, s: &SwitchId, p: &PortName) -> Option<PortId> {
        self.switch.with(|c| c.port_id(s, p))
    }

    /// Points every switch port cabled to a VM NIC at that NIC's QEMU socket.
    fn cable_all(&self) {
        let vms = self.vms.lock().expect("vms lock");
        for v in vms.values() {
            for nic in &v.spec.nics {
                if let Some((s, p)) = &nic.cable
                    && let Some(port) = self.port_of(s, p)
                {
                    self.switch.cable(port, Some(v.files.nic_vm(&nic.name)));
                }
            }
        }
    }

    /// Sets each running VM NIC's carrier from its cable: in, and the
    /// switch's end up (powered, port admin-up), as a real NIC sees it.
    fn sync_carrier(&self) {
        let nics: Vec<(VmFiles, String, bool)> = {
            let vms = self.vms.lock().expect("vms lock");
            vms.values()
                .flat_map(|v| {
                    v.spec.nics.iter().map(|n| {
                        let switch_end = n.cable.as_ref().and_then(|(s, p)| self.port_of(s, p)).is_some_and(|port| self.switch.with(|c| c.far_end_up(port)));
                        (v.files.clone(), n.name.clone(), switch_end && !v.unplugged.contains(&n.name))
                    })
                })
                .collect()
        };
        for (f, nic, up) in nics.into_iter().filter(|(f, ..)| qemu::running(f)) {
            // Unchanged carrier is a no-op for QEMU: the guest hears nothing.
            if let Err(e) = qemu::qmp(&f, "set_link", json!({"name": format!("nic-{nic}"), "up": up})) {
                eprintln!("lab: set carrier of {nic}: {e}");
            }
        }
    }

    /// Tells the switches which cabled NICs have link: the VM's QEMU runs
    /// (frozen counts: a frozen machine's NIC keeps carrier) and the cable is in.
    pub fn refresh_links(&self) {
        let vms = self.vms.lock().expect("vms lock");
        for v in vms.values() {
            let on = qemu::running(&v.files);
            for nic in &v.spec.nics {
                if let Some((s, p)) = &nic.cable
                    && let Some(port) = self.switch.with(|c| c.port_id(s, p))
                {
                    let up = on && !v.unplugged.contains(&nic.name);
                    self.switch.with(|c| c.set_peer_up(port, up));
                }
            }
        }
    }

    pub fn installed(&self, id: &HostId) -> bool {
        self.files(id).is_ok_and(|f| f.disk("root").exists())
    }

    /// Installs `id` (disks and seed, if it has none) and powers it on.
    pub fn install(&self, id: &HostId, role: &Role) -> io::Result<()> {
        let (spec, files) = {
            let vms = self.vms.lock().expect("vms lock");
            let v = vms.get(id).ok_or_else(|| io::Error::other(format!("no VM {id}")))?;
            (v.spec.clone(), v.files.clone())
        };
        let base = base_image(&self.cfg.state)?;
        let seed = cloudinit::seed(&spec, role, &self.ssh_pub, &self.switch_addrs);
        qemu::install(&spec, &files, &base, &seed)?;
        self.power_on(id)
    }

    // ---- faults -------------------------------------------------------------

    pub fn power_on(&self, id: &HostId) -> io::Result<()> {
        let (spec, files, pulled) = {
            let vms = self.vms.lock().expect("vms lock");
            let v = vms.get(id).ok_or_else(|| io::Error::other(format!("no VM {id}")))?;
            (v.spec.clone(), v.files.clone(), v.failed_disks.clone())
        };
        if !files.disk("root").exists() {
            return Err(io::Error::other(format!("{id} is not installed")));
        }
        if !qemu::running(&files) {
            let run = self.cfg.run.clone();
            let cable: BTreeMap<String, PathBuf> = spec
                .nics
                .iter()
                .map(|n| (n.name.clone(), n.cable.as_ref().map_or_else(|| files.nic_nowhere(&n.name), |(s, p)| port_sock(&run, s, p))))
                .collect();
            // A pulled disk is not in the machine when it boots.
            qemu::launch(&spec, &files, &pulled, |nic| cable[nic].clone())?;
            self.note_booted(id);
        }
        self.sync_carrier();
        self.refresh_links();
        Ok(())
    }

    /// Pulls the plug: QEMU exits at once; the disks keep what was written.
    pub fn power_off(&self, id: &HostId) -> io::Result<()> {
        let f = self.files(id)?;
        if qemu::running(&f) && qemu::qmp(&f, "quit", json!(null)).is_err()
            && let Some(pid) = qemu::pid(&f)
        {
            // SAFETY: killing the QEMU process we started.
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while qemu::running(&f) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        self.refresh_links();
        Ok(())
    }

    /// Presses the power button: the guest shuts down cleanly.
    pub fn shutdown(&self, id: &HostId) -> io::Result<()> {
        qemu::qmp(&self.files(id)?, "system_powerdown", json!(null)).map(|_| ())
    }

    /// Presses reset: an instant reboot, nothing flushed.
    pub fn reset(&self, id: &HostId) -> io::Result<()> {
        qemu::qmp(&self.files(id)?, "system_reset", json!(null))?;
        self.note_booted(id);
        Ok(())
    }

    fn note_booted(&self, id: &HostId) {
        if let Some(v) = self.vms.lock().expect("vms lock").get_mut(id) {
            v.booted = Some(Instant::now());
        }
    }

    pub fn freeze(&self, id: &HostId) -> io::Result<()> {
        qemu::qmp(&self.files(id)?, "stop", json!(null)).map(|_| ())
    }

    pub fn thaw(&self, id: &HostId) -> io::Result<()> {
        qemu::qmp(&self.files(id)?, "cont", json!(null)).map(|_| ())
    }

    /// Pulls (or puts back) the cable of `nic`.
    pub fn set_cable(&self, id: &HostId, nic: &str, plugged: bool) -> io::Result<()> {
        {
            let mut vms = self.vms.lock().expect("vms lock");
            let v = vms.get_mut(id).ok_or_else(|| io::Error::other(format!("no VM {id}")))?;
            let n = v.spec.nics.iter().find(|n| n.name == nic).ok_or_else(|| io::Error::other(format!("{id} has no NIC {nic}")))?;
            if n.cable.is_none() {
                return Err(io::Error::other(format!("{id} {nic} is not cabled")));
            }
            if plugged {
                v.unplugged.remove(nic);
            } else {
                v.unplugged.insert(nic.to_string());
            }
        }
        self.sync_carrier();
        self.refresh_links();
        Ok(())
    }

    /// Pulls (`present = false`) or puts back data disk `disk`. A pulled
    /// disk stays out across power cycles until put back.
    /// The disk is recorded pulled (or back) only once QEMU did it; a
    /// powered-off machine just boots without (or with) it.
    pub fn set_disk(&self, id: &HostId, disk: &str, present: bool) -> io::Result<()> {
        let f = {
            let vms = self.vms.lock().expect("vms lock");
            let v = vms.get(id).ok_or_else(|| io::Error::other(format!("no VM {id}")))?;
            if !v.spec.disks.iter().any(|(n, _)| n == disk) {
                return Err(io::Error::other(format!("{id} has no disk {disk}")));
            }
            if present == !v.failed_disks.contains(disk) {
                return Err(io::Error::other(format!("{id} {disk} is already {}", if present { "in" } else { "out" })));
            }
            v.files.clone()
        };
        if qemu::running(&f) {
            if present { qemu::plug_disk(&f, disk)? } else { qemu::unplug_disk(&f, disk)? }
        }
        let mut vms = self.vms.lock().expect("vms lock");
        let v = vms.get_mut(id).expect("checked above");
        if present {
            v.failed_disks.remove(disk);
        } else {
            v.failed_disks.insert(disk.to_string());
        }
        Ok(())
    }

    pub fn partition(&self, group: BTreeSet<Member>) -> io::Result<()> {
        if group.is_empty() {
            return Err(io::Error::other("an empty partition group"));
        }
        self.switch.with(|c| c.partition(group));
        Ok(())
    }

    pub fn heal(&self) {
        self.switch.with(Core::heal);
    }

    pub fn set_switch_up(&self, s: &SwitchId, up: bool) -> io::Result<()> {
        if !self.switch.with(|c| c.switch_ids().any(|i| i == s)) {
            return Err(io::Error::other(format!("no switch {s}")));
        }
        self.switch.with(|c| c.set_switch_up(s, up));
        self.sync_carrier();
        Ok(())
    }

    pub fn set_port(&self, s: &SwitchId, p: &PortName, cfg: SwitchPortCfg) -> io::Result<()> {
        let port = self.port_of(s, p).ok_or_else(|| io::Error::other(format!("no port {s}/{p}")))?;
        self.switch.with(|c| c.set_cfg(port, cfg));
        self.sync_carrier();
        Ok(())
    }

    /// Forces `nic`'s switch port down to `mbps` (or back to full speed with
    /// `None`), and the guest's NIC with it, as a real NIC renegotiates over
    /// a bad cable. A virtio NIC has no PHY to negotiate with, so the guest
    /// agent sets its speed (`ethtool -s`); the guest kernel forgets that on
    /// reboot or relink, where a real one would renegotiate.
    pub fn degrade(&self, id: &HostId, nic: &str, mbps: Option<u32>) -> io::Result<()> {
        let spec = self.spec(id).ok_or_else(|| io::Error::other(format!("no VM {id}")))?;
        let n = spec.nics.iter().find(|n| n.name == nic).ok_or_else(|| io::Error::other(format!("{id} has no NIC {nic}")))?;
        let (s, p) = n.cable.clone().ok_or_else(|| io::Error::other(format!("{id} {nic} is not cabled")))?;
        let port = self.port_of(&s, &p).expect("cabled to a real port");
        self.switch.with(|c| c.degrade(port, mbps));
        let f = self.files(id)?;
        if qemu::running(&f) {
            let speed = mbps.unwrap_or(n.speed_mbps).to_string();
            qemu::guest_exec(&f, "/usr/sbin/ethtool", &["-s", nic, "speed", &speed, "duplex", "full"], Duration::from_secs(5))?;
        }
        Ok(())
    }

    /// Racks a new machine: cables it, has its ports configured (perhaps
    /// wrongly), installs it with `token` and powers it on.
    pub fn add_vm(&self, profile: ProfileId, mistake: Option<Mistake>, token: String) -> io::Result<HostId> {
        let _one_at_a_time = self.faults.lock().expect("faults lock");
        let vm = self.spec.lock().expect("spec lock").add_vm(profile, mistake).map_err(io::Error::other)?;
        let mut added = load_added(&self.cfg.state);
        added.push(Added { profile, mistake });
        let path = added_path(&self.cfg.state);
        std::fs::create_dir_all(path.parent().expect("under vms/"))?;
        std::fs::write(&path, serde_json::to_vec_pretty(&added).map_err(io::Error::other)?)?;
        for nic in &vm.nics {
            if let Some((s, p)) = &nic.cable {
                let port = self.port_of(s, p).expect("cabled to a real port");
                let cfg = crate::topology::technician_cfg(profile, &vm.id, &nic.name, mistake);
                self.switch.with(|c| {
                    c.set_cfg(port, cfg);
                    c.set_peer(port, Peer::Host { host: vm.id.clone(), nic: nic.name.clone(), nic_mbps: nic.speed_mbps });
                });
            }
        }
        let files = VmFiles::new(&self.cfg.state, &self.cfg.run, &vm);
        let id = vm.id.clone();
        self.vms.lock().expect("vms lock").insert(id.clone(), VmState { spec: vm, files, unplugged: BTreeSet::new(), failed_disks: BTreeSet::new(), booted: None });
        self.cable_all();
        self.install(&id, &Role::Join(token))?;
        Ok(id)
    }

    // ---- truth --------------------------------------------------------------

    pub fn power(&self, id: &HostId) -> Power {
        let Ok(f) = self.files(id) else { return Power::Off };
        if !qemu::running(&f) {
            return Power::Off;
        }
        match qemu::qmp(&f, "query-status", json!(null)) {
            Ok(v) if v["running"].as_bool() == Some(true) => Power::Running,
            Ok(v) if v["status"].as_str() == Some("paused") => Power::Paused,
            Ok(_) => Power::Paused,
            Err(_) => Power::Unknown,
        }
    }

    /// Looks at every VM from outside (QMP, guest agent) and at the
    /// switches, and keeps the result for [`Lab::truth`].
    pub fn sample_truth(&self) {
        self.refresh_links();
        let vms: Vec<(VmSpec, VmFiles, BTreeSet<String>, BTreeSet<String>, Option<Instant>)> = self
            .vms
            .lock()
            .expect("vms lock")
            .values()
            .map(|v| (v.spec.clone(), v.files.clone(), v.unplugged.clone(), v.failed_disks.clone(), v.booted))
            .collect();
        let before: BTreeMap<HostId, VmTruth> = self.truth().vms.into_iter().map(|v| (v.id.clone(), v)).collect();
        let mut out = vec![];
        for (spec, files, unplugged, failed, booted) in vms {
            let power = self.power(&spec.id);
            let (mut holds_vip, mut instances, mut clock_offset_ms, mut uptime_s) = (false, vec![], None, None);
            let carry = |holds_vip: &mut bool, instances: &mut Vec<String>| {
                if let Some(prev) = before.get(&spec.id) {
                    *holds_vip = prev.holds_vip;
                    instances.clone_from(&prev.instances);
                }
            };
            // A frozen machine cannot change, and an unanswered QMP says
            // nothing: what it held when last seen, it holds now.
            if matches!(power, Power::Paused | Power::Unknown) {
                carry(&mut holds_vip, &mut instances);
            }
            if power == Power::Running {
                let t = Duration::from_millis(800);
                let script = "ip -4 -o addr; echo @@; systemctl list-units --plain --no-legend --state=active 'homelab-inst-*'; echo @@; cat /proc/uptime";
                match qemu::guest_exec(&files, "/bin/sh", &["-c", script], t) {
                    Ok(out) => {
                        let mut parts = out.split("@@\n");
                        holds_vip = parts.next().is_some_and(|a| a.contains(&format!(" {VIP}/")));
                        instances = parts.next().unwrap_or("").lines().filter_map(|l| l.split_whitespace().next()).filter_map(node::linux::instance_of_unit).collect();
                        uptime_s = parts.next().and_then(|u| u.split('.').next()?.trim().parse().ok());
                    }
                    // Silent but not booting: busy, not changed.
                    Err(_) if booted.is_none_or(|at| at.elapsed() > BOOT_GRACE) => carry(&mut holds_vip, &mut instances),
                    Err(_) => {}
                }
                let before = now_ms();
                if let Ok(v) = qemu::qga(&files, "guest-get-time", json!(null), t)
                    && let Some(ns) = v.as_i64()
                {
                    let after = now_ms();
                    clock_offset_ms = Some(ns / 1_000_000 - ((before + after) / 2) as i64);
                }
            }
            out.push(VmTruth {
                id: spec.id.clone(),
                profile: spec.profile,
                installed: files.disk("root").exists(),
                power,
                unplugged: unplugged.into_iter().collect(),
                disks: spec.disks.iter().map(|(d, _)| d.clone()).collect(),
                failed_disks: failed.into_iter().collect(),
                holds_vip,
                instances,
                clock_offset_ms,
                uptime_s,
            });
        }
        let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
        for v in &out {
            for i in v.instances.iter().filter(|i| i.starts_with("vm/")) {
                *seen.entry(i.as_str()).or_default() += 1;
            }
        }
        let split_brain_vms = seen.into_iter().filter(|(_, n)| *n > 1).map(|(i, _)| i.to_string()).collect();
        let vip_holders = out.iter().filter(|v| v.holds_vip).map(|v| v.id.clone()).collect();
        let now = Instant::now();
        let (switches, ports, partitions) = self.switch.with(|c| {
            let switches = c.switch_ids().map(|id| SwitchTruth { id: id.clone(), up: c.switch_up(id) }).collect::<Vec<_>>();
            let counters: Vec<PortCounters> = c.ports().map(|(i, _)| c.counters(i)).collect();
            let mut last = self.last_counters.lock().expect("counters lock");
            let ports = c
                .ports()
                .map(|(i, d)| {
                    let (rx_bps, tx_bps) = match &*last {
                        Some((at, prev)) => {
                            let secs = now.duration_since(*at).as_secs_f64().max(0.001);
                            let p = prev[i];
                            ((counters[i].rx_bytes - p.rx_bytes) as f64 * 8.0 / secs, (counters[i].tx_bytes - p.tx_bytes) as f64 * 8.0 / secs)
                        }
                        None => (0.0, 0.0),
                    };
                    let (host, nic) = match &d.peer {
                        Peer::Host { host, nic, .. } => (Some(host.clone()), Some(nic.clone())),
                        _ => (None, None),
                    };
                    PortTruth { switch: d.switch.clone(), port: d.name.clone(), host, nic, link_up: c.link_up(i), rx_bps, tx_bps, counters: counters[i] }
                })
                .collect::<Vec<_>>();
            *last = Some((now, counters));
            (switches, ports, c.partitions().iter().map(|g| g.iter().cloned().collect()).collect())
        });
        *self.truth.lock().expect("truth lock") = LabTruth { vms: out, switches, ports, partitions, vip_holders, split_brain_vms, at: now_ms() };
    }

    pub fn truth(&self) -> LabTruth {
        self.truth.lock().expect("truth lock").clone()
    }
}
