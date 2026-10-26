//! VMs as QEMU processes: their disks and seed, launching them (daemonized,
//! so they outlive a restart of the lab server), and talking to them out of
//! band: QMP to QEMU (power, freeze, link, disks) and the guest agent inside
//! (what the machine really runs, for ground truth). Neither path touches
//! the lab network.

use crate::cloudinit::Seed;
use crate::topology::VmSpec;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

pub const QEMU: &str = "/usr/libexec/qemu-kvm";

/// Where one VM's files live.
#[derive(Debug, Clone)]
pub struct VmFiles {
    /// Disks, seed and console log (survive the lab being stopped).
    pub dir: PathBuf,
    /// Sockets and pid file.
    pub run: PathBuf,
}

impl VmFiles {
    pub fn new(state: &Path, run: &Path, vm: &VmSpec) -> Self {
        Self { dir: state.join("vms").join(vm.id.as_str()), run: run.join(vm.id.as_str()) }
    }
    pub fn qmp(&self) -> PathBuf {
        self.run.join("qmp.sock")
    }
    pub fn qga(&self) -> PathBuf {
        self.run.join("qga.sock")
    }
    pub fn pid(&self) -> PathBuf {
        self.run.join("qemu.pid")
    }
    /// QEMU's end of a NIC's cable.
    pub fn nic_vm(&self, nic: &str) -> PathBuf {
        self.run.join(format!("{nic}.vm.sock"))
    }
    /// Where an uncabled NIC sends: nowhere.
    pub fn nic_nowhere(&self, nic: &str) -> PathBuf {
        self.run.join(format!("{nic}.uncabled"))
    }
    pub fn disk(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.qcow2"))
    }
}

fn run(cmd: &str, args: &[&str]) -> io::Result<String> {
    let out = Command::new(cmd).args(args).output()?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        Err(io::Error::other(format!("{cmd} {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim())))
    }
}

/// Creates the VM's disks (a root overlay on `base`, blank data disks) and
/// its seed ISO, unless they exist: a machine installed once keeps its disks.
pub fn install(vm: &VmSpec, f: &VmFiles, base: &Path, seed: &Seed) -> io::Result<()> {
    fs::create_dir_all(&f.dir)?;
    fs::create_dir_all(&f.run)?;
    let root = f.disk("root");
    if !root.exists() {
        let base = base.to_string_lossy();
        run("qemu-img", &["create", "-q", "-f", "qcow2", "-F", "qcow2", "-b", &base, &root.to_string_lossy(), "16G"])?;
        let seed_dir = f.dir.join("seed");
        fs::create_dir_all(&seed_dir)?;
        fs::write(seed_dir.join("user-data"), &seed.user_data)?;
        fs::write(seed_dir.join("meta-data"), &seed.meta_data)?;
        fs::write(seed_dir.join("network-config"), &seed.network_config)?;
        let iso = f.dir.join("seed.iso");
        let files: Vec<String> = ["user-data", "meta-data", "network-config"].iter().map(|n| seed_dir.join(n).to_string_lossy().into_owned()).collect();
        let mut args = vec!["-quiet", "-output", iso.to_str().expect("utf-8 path"), "-volid", "cidata", "-joliet", "-rock"];
        args.extend(files.iter().map(String::as_str));
        run("genisoimage", &args)?;
    }
    for (name, size) in &vm.disks {
        let d = f.disk(name);
        if !d.exists() {
            run("qemu-img", &["create", "-q", "-f", "qcow2", &d.to_string_lossy(), &format!("{size}G")])?;
        }
    }
    Ok(())
}

/// The DMI manufacturer and product a machine of `vm`'s profile reports.
fn smbios(vm: &VmSpec) -> String {
    match vm.profile {
        model::ProfileId::Ms01 => "type=1,manufacturer=Minisforum,product=MS-01".into(),
        _ => "type=1,manufacturer=Generic,product=Generic mini PC".into(),
    }
}

/// A data disk's root port chassis number: from its name (`nvme1` → 2), so a
/// disk keeps its slot whichever others are present.
fn disk_chassis(name: &str, index: usize) -> usize {
    name.trim_start_matches(|c: char| !c.is_ascii_digit()).parse::<usize>().map_or(index + 1, |n| n + 1)
}

/// Plugs data disk `name` (its image is in place) back into its root port.
/// The guest sees a new block device with the disk's serial. Fails, changing
/// nothing, while the disk's last unplug is still in progress.
pub fn plug_disk(f: &VmFiles, name: &str) -> io::Result<()> {
    if drives(f)?.contains(&format!("drv-{name}")) {
        return Err(io::Error::other(format!("{name} is still being released by the guest; try again")));
    }
    let file = f.disk(name).to_string_lossy().into_owned();
    let add = format!("drive_add 0 file={file},if=none,id=drv-{name},format=qcow2");
    // HMP reports its errors as text in a successful answer.
    let said = qmp(f, "human-monitor-command", json!({"command-line": add}))?;
    let said = said.as_str().unwrap_or("").trim();
    if !(said.is_empty() || said == "OK") {
        return Err(io::Error::other(format!("drive_add {name}: {said}")));
    }
    let dev = json!({"driver": "virtio-blk-pci", "drive": format!("drv-{name}"), "id": format!("dev-{name}"), "serial": name, "bus": format!("rp-{name}")});
    if let Err(e) = qmp(f, "device_add", dev) {
        let _ = qmp(f, "human-monitor-command", json!({"command-line": format!("drive_del drv-{name}")}));
        return Err(e);
    }
    Ok(())
}

/// Starts pulling data disk `name`, and waits (up to 5 s) for the guest to
/// let go of it.
pub fn unplug_disk(f: &VmFiles, name: &str) -> io::Result<()> {
    qmp(f, "device_del", json!({"id": format!("dev-{name}")}))?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while drives(f)?.contains(&format!("drv-{name}")) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

/// The ids of the VM's block drives (`root`, `seed`, `drv-<disk>`).
pub fn drives(f: &VmFiles) -> io::Result<BTreeSet<String>> {
    let blocks = qmp(f, "query-block", Value::Null)?;
    Ok(blocks.as_array().into_iter().flatten().filter_map(|b| b.get("device")?.as_str().map(String::from)).collect())
}

/// The pid of the VM's QEMU process, if it is alive: the pid file's, if
/// that process is this VM's QEMU (a stale file's pid may since have been
/// reused by anything).
pub fn pid(f: &VmFiles) -> Option<i32> {
    let pid = fs::read_to_string(f.pid()).ok()?.trim().parse::<i32>().ok()?;
    let cmdline = fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let qmp = f.qmp().to_string_lossy().into_owned();
    String::from_utf8_lossy(&cmdline).split('\0').any(|a| a.contains(&qmp)).then_some(pid)
}

/// Whether the VM's QEMU process is alive.
pub fn running(f: &VmFiles) -> bool {
    pid(f).is_some()
}

/// Starts the VM (daemonized). Each NIC sends to `switch_port(nic)`, the
/// socket of the switch port its cable goes to; the pcie root ports let its
/// data disks be pulled. A `pulled` disk's port is there, empty, so it can
/// be put back.
pub fn launch(vm: &VmSpec, f: &VmFiles, pulled: &BTreeSet<String>, switch_port: impl Fn(&str) -> PathBuf) -> io::Result<()> {
    fs::create_dir_all(&f.run)?;
    for p in [f.qmp(), f.qga()].into_iter().chain(vm.nics.iter().map(|n| f.nic_vm(&n.name))) {
        let _ = fs::remove_file(p);
    }
    let s = |p: PathBuf| p.to_string_lossy().into_owned();
    let mut a: Vec<String> = vec![];
    let mut arg = |x: &str| a.push(x.to_string());
    for x in ["-name", vm.id.as_str(), "-machine", "q35,accel=kvm", "-cpu", "host", "-display", "none"] {
        arg(x);
    }
    arg("-smp");
    arg(&vm.vcpus.to_string());
    arg("-m");
    arg(&vm.ram_mb.to_string());
    arg("-smbios");
    arg(&smbios(vm));
    arg("-drive");
    arg(&format!("file={},if=none,id=root,format=qcow2", s(f.disk("root"))));
    arg("-device");
    arg("virtio-blk-pci,drive=root,bootindex=0");
    arg("-drive");
    arg(&format!("file={},if=none,id=seed,media=cdrom,readonly=on", s(f.dir.join("seed.iso"))));
    arg("-device");
    arg("ide-cd,drive=seed");
    for (i, (name, _)) in vm.disks.iter().enumerate() {
        arg("-device");
        arg(&format!("pcie-root-port,id=rp-{name},chassis={},slot=0", disk_chassis(name, i)));
        if pulled.contains(name) {
            continue;
        }
        arg("-drive");
        arg(&format!("file={},if=none,id=drv-{name},format=qcow2", s(f.disk(name))));
        arg("-device");
        arg(&format!("virtio-blk-pci,drive=drv-{name},id=dev-{name},serial={name},bus=rp-{name}"));
    }
    for nic in &vm.nics {
        let n = &nic.name;
        arg("-netdev");
        arg(&format!(
            "dgram,id=net-{n},local.type=unix,local.path={},remote.type=unix,remote.path={}",
            s(f.nic_vm(n)),
            s(switch_port(n))
        ));
        arg("-device");
        arg(&format!("virtio-net-pci,netdev=net-{n},id=nic-{n},mac={},speed={},duplex=full", VmSpec::mac_str(&nic.mac), nic.speed_mbps));
    }
    arg("-chardev");
    arg(&format!("socket,id=qga,path={},server=on,wait=off", s(f.qga())));
    arg("-device");
    arg("virtio-serial");
    arg("-device");
    arg("virtserialport,chardev=qga,name=org.qemu.guest_agent.0");
    arg("-qmp");
    arg(&format!("unix:{},server=on,wait=off", s(f.qmp())));
    arg("-serial");
    arg(&format!("file:{}", s(f.dir.join("console.log"))));
    arg("-daemonize");
    arg("-pidfile");
    arg(&s(f.pid()));
    let refs: Vec<&str> = a.iter().map(String::as_str).collect();
    run(QEMU, &refs)?;
    Ok(())
}

/// A JSON line protocol over a unix socket: QMP and the guest agent both speak it.
struct JsonLines {
    r: BufReader<UnixStream>,
    w: UnixStream,
    deadline: Instant,
}

impl JsonLines {
    fn connect(path: &Path, timeout: Duration) -> io::Result<Self> {
        let s = UnixStream::connect(path)?;
        s.set_read_timeout(Some(timeout))?;
        s.set_write_timeout(Some(timeout))?;
        Ok(Self { r: BufReader::new(s.try_clone()?), w: s, deadline: Instant::now() + timeout })
    }

    fn send(&mut self, v: &Value) -> io::Result<()> {
        let mut line = serde_json::to_vec(v).map_err(io::Error::other)?;
        line.push(b'\n');
        self.w.write_all(&line)
    }

    /// The next message that is not an asynchronous event.
    fn recv(&mut self) -> io::Result<Value> {
        loop {
            if Instant::now() > self.deadline {
                return Err(io::ErrorKind::TimedOut.into());
            }
            let mut line = vec![];
            if self.r.read_until(b'\n', &mut line)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            // The guest agent marks a sync reply with a leading 0xFF byte.
            let start = line.iter().position(|&b| b != 0xff).unwrap_or(line.len());
            let Ok(v) = serde_json::from_slice::<Value>(&line[start..]) else { continue };
            if v.get("event").is_none() {
                return Ok(v);
            }
        }
    }

    fn call(&mut self, cmd: &str, args: Value) -> io::Result<Value> {
        let msg = if args.is_null() { json!({"execute": cmd}) } else { json!({"execute": cmd, "arguments": args}) };
        self.send(&msg)?;
        let r = self.recv()?;
        match r.get("return") {
            Some(v) => Ok(v.clone()),
            None => Err(io::Error::other(format!("{cmd}: {}", r.get("error").unwrap_or(&r)))),
        }
    }
}

/// Runs one QMP command against the VM's QEMU.
pub fn qmp(f: &VmFiles, cmd: &str, args: Value) -> io::Result<Value> {
    let mut c = JsonLines::connect(&f.qmp(), Duration::from_secs(5))?;
    c.recv()?; // greeting
    c.call("qmp_capabilities", Value::Null)?;
    c.call(cmd, args)
}

/// Runs one guest agent command inside the VM. The agent only answers a
/// running guest that has booted far enough; a frozen one times out.
pub fn qga(f: &VmFiles, cmd: &str, args: Value, timeout: Duration) -> io::Result<Value> {
    let mut c = JsonLines::connect(&f.qga(), timeout)?;
    // Resynchronise: a previous client may have left a reply unread.
    let id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(1, |d| d.as_nanos() as u64 & 0x7fff_ffff);
    c.send(&json!({"execute": "guest-sync-delimited", "arguments": {"id": id}}))?;
    loop {
        let v = c.recv()?;
        if v.get("return").and_then(Value::as_u64) == Some(id) {
            break;
        }
    }
    c.call(cmd, args)
}

/// Runs `path args` in the guest through its agent and returns stdout.
pub fn guest_exec(f: &VmFiles, path: &str, args: &[&str], timeout: Duration) -> io::Result<String> {
    let started = Instant::now();
    let r = qga(f, "guest-exec", json!({"path": path, "arg": args, "capture-output": true}), timeout)?;
    let pid = r.get("pid").and_then(Value::as_i64).ok_or_else(|| io::Error::other("guest-exec returned no pid"))?;
    loop {
        let left = timeout.saturating_sub(started.elapsed());
        if left.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        let st = qga(f, "guest-exec-status", json!({"pid": pid}), left)?;
        if st.get("exited").and_then(Value::as_bool) == Some(true) {
            let out = st.get("out-data").and_then(Value::as_str).unwrap_or("");
            return Ok(String::from_utf8_lossy(&base64_decode(out)).into_owned());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn base64_decode(s: &str) -> Vec<u8> {
    let val = |c: u8| match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0);
    for v in s.bytes().filter_map(val) {
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_decodes() {
        assert_eq!(base64_decode("aGVsbG8gd29ybGQ="), b"hello world");
        assert_eq!(base64_decode("MQo="), b"1\n");
        assert_eq!(base64_decode(""), b"");
    }
}
