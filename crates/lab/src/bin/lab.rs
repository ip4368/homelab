//! `lab`: drives the real VM lab from the command line (as root).
//!
//! - `lab smoke` brings up the lab, boots the bootstrap host and prints what
//!   the control plane and ground truth say every few seconds.
//! - `lab down` powers off every VM and stops the lab's daemons.
//! - `lab reset` does that and deletes the VMs' disks, so the next start
//!   installs a fresh cluster.
//! - `lab ip VM` prints the VM's management address, asking its guest agent.

use control::client::ClientActor;
use control::runtime::Svc;
use lab::cloudinit::Role;
use lab::manager::{Lab, LabConfig};
use model::HostId;
use std::process::ExitCode;
use std::time::Duration;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("smoke") => smoke().await,
        Some("down") => down(false),
        Some("reset") => down(true),
        Some("ip") => match args.get(1) {
            Some(vm) => ip(vm),
            None => ExitCode::from(2),
        },
        _ => {
            eprintln!("usage: lab smoke|down|reset|ip VM");
            ExitCode::from(2)
        }
    }
}

async fn smoke() -> ExitCode {
    let lab = match Lab::start(LabConfig::from_env()).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("lab: start: {e}");
            return ExitCode::FAILURE;
        }
    };
    let boot = HostId::from("ms01-01");
    let l = lab.clone();
    let b = boot.clone();
    if let Err(e) = tokio::task::spawn_blocking(move || if l.installed(&b) { l.power_on(&b) } else { l.install(&b, &Role::Bootstrap) }).await.expect("join") {
        eprintln!("lab: boot {boot}: {e}");
        return ExitCode::FAILURE;
    }
    let client = lab::client::spawn().expect("client node");
    loop {
        let l = lab.clone();
        tokio::task::spawn_blocking(move || l.sample_truth()).await.expect("join");
        let truth = lab.truth();
        let view = client
            .exec(|n| {
                let v = n.actor::<ClientActor>(Svc::Client).expect("client").view();
                let hosts: Vec<String> = v.state.hosts.values().map(|h| format!("{}:{:?}/{:?}", h.id, h.lifecycle, h.net.state)).collect();
                (v.leader.clone(), v.term, hosts, v.state.cp.voters.len())
            })
            .await
            .expect("client node");
        let vms: Vec<String> = truth.vms.iter().map(|v| format!("{}:{:?}{}{}", v.id, v.power, if v.holds_vip { "+vip" } else { "" }, v.clock_offset_ms.map_or(String::new(), |o| format!(" {o}ms")))).collect();
        println!("leader={:?} term={} voters={} hosts={:?} | truth {:?}", view.0, view.1, view.3, view.2, vms);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Kills the process whose pid is in `file`, if any, and waits for it to go.
fn kill_pid_file(file: &std::path::Path, signal: i32) {
    let Ok(pid) = std::fs::read_to_string(file) else { return };
    let Ok(pid) = pid.trim().parse::<i32>() else { return };
    // SAFETY: signalling a pid the lab recorded for a process it started.
    unsafe { libc::kill(pid, signal) };
    for _ in 0..50 {
        // SAFETY: signal 0 only checks that the process exists.
        if unsafe { libc::kill(pid, 0) } != 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn down(wipe: bool) -> ExitCode {
    let cfg = LabConfig::from_env();
    if let Ok(dirs) = std::fs::read_dir(&cfg.run) {
        for d in dirs.flatten() {
            let files = lab::qemu::VmFiles { dir: cfg.state.join("vms").join(d.file_name()), run: d.path() };
            if lab::qemu::pid(&files).is_some() {
                println!("powering off {}", d.file_name().to_string_lossy());
                kill_pid_file(&files.pid(), 9);
            }
        }
    }
    for daemon in ["dnsmasq.pid", "chronyd.pid"] {
        kill_pid_file(&cfg.run.join(daemon), 15);
    }
    if wipe {
        let vms = cfg.state.join("vms");
        println!("deleting {}", vms.display());
        if let Err(e) = std::fs::remove_dir_all(&vms)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("lab: {e}");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

fn ip(vm: &str) -> ExitCode {
    let cfg = LabConfig::from_env();
    let files = lab::qemu::VmFiles { dir: cfg.state.join("vms").join(vm), run: cfg.run.join(vm) };
    match lab::qemu::guest_exec(&files, "/usr/sbin/ip", &["-4", "-o", "addr", "show", "eth0"], Duration::from_secs(3)) {
        Ok(out) => {
            // The first address on eth0 that is not the VIP.
            let addr = out.split_whitespace().filter_map(|w| w.split_once('/')).map(|(a, _)| a).find(|a| *a != lab::topology::VIP.to_string());
            match addr {
                Some(a) => {
                    println!("{a}");
                    ExitCode::SUCCESS
                }
                None => ExitCode::FAILURE,
            }
        }
        Err(e) => {
            eprintln!("lab: {vm}: {e}");
            ExitCode::FAILURE
        }
    }
}
