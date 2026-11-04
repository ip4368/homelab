//! KubeVirt's VirtualMachines as workloads: how many should run and do, their
//! health from KubeVirt's own status, and what the drawer says of the guest
//! (docs/plans/2026-11-04-workload-insight-logs-terminal-design.md §8).

use crate::kube::{Health, HealthState, KubeCondition};
use serde::Serialize;
use serde_json::Value;
use ts_rs::TS;

/// A VM beyond what every workload has.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct VmDetail {
    /// KubeVirt's word for it: `Running`, `Stopped`, `Starting`, `ErrorUnschedulable`…
    pub status: String,
    /// `Always`, `Halted`, `Manual`, `RerunOnFailure` or `Once`.
    pub run_strategy: String,
    pub cores: Option<u32>,
    /// The guest's memory, as written: `1Gi`.
    pub memory: Option<String>,
    /// Where its instance runs.
    pub node: Option<String>,
    /// From the guest agent, when one runs in the guest.
    pub guest_os: Option<String>,
    pub kernel: Option<String>,
    pub agent_connected: bool,
    pub interfaces: Vec<VmInterface>,
    pub disks: Vec<VmDisk>,
    /// Whether KubeVirt could move it to another host while it runs; none while it does not run.
    pub migratable: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct VmInterface {
    pub name: String,
    pub ips: Vec<String>,
    pub mac: Option<String>,
    /// Its name inside the guest (`enp1s0`), from the guest agent.
    pub guest_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct VmDisk {
    pub name: String,
    /// `containerDisk`, `claim`, `dataVolume`, `cloudInit`, `emptyDisk`…
    pub kind: String,
    /// The image, claim or data volume.
    pub source: Option<String>,
    /// `virtio`, `sata`; `cdrom` for one attached as a CD.
    pub bus: Option<String>,
}

fn text(v: &Value) -> Option<String> {
    v.as_str().filter(|s| !s.is_empty()).map(String::from)
}

fn arr(v: &Value) -> &[Value] {
    v.as_array().map_or(&[], Vec::as_slice)
}

fn condition<'a>(obj: &'a Value, kind: &str) -> Option<&'a Value> {
    arr(&obj["status"]["conditions"]).iter().find(|c| c["type"] == kind)
}

fn is_true(obj: &Value, kind: &str) -> bool {
    condition(obj, kind).is_some_and(|c| c["status"] == "True")
}

/// The run strategy, from `spec.runStrategy` or the older `spec.running`.
pub fn run_strategy(vm: &Value) -> String {
    text(&vm["spec"]["runStrategy"]).unwrap_or_else(|| if vm["spec"]["running"].as_bool() == Some(true) { "Always".into() } else { "Halted".into() })
}

/// How many instances should run (0 or 1), how many are ready, and how many
/// run the latest spec, given the VM and its instance, if any.
pub fn replicas(vm: &Value, vmi: Option<&Value>) -> (u32, u32, u32) {
    let desired = match run_strategy(vm).as_str() {
        "Halted" => 0,
        "Manual" => u32::from(vmi.is_some()),
        _ => 1,
    };
    let ready = u32::from(vmi.is_some_and(|i| is_true(i, "Ready")));
    let updated = if is_true(vm, "RestartRequired") { 0 } else { desired };
    (desired, ready, updated)
}

/// The VM's conditions worth showing: those that are not as they should be.
pub fn conditions(vm: &Value) -> Vec<KubeCondition> {
    arr(&vm["status"]["conditions"])
        .iter()
        .map(|c| KubeCondition { kind: text(&c["type"]).unwrap_or_default(), status: text(&c["status"]).unwrap_or_default(), reason: text(&c["reason"]), message: text(&c["message"]) })
        .filter(|c| match c.kind.as_str() {
            "Failure" | "Paused" | "RestartRequired" => c.status == "True",
            "Ready" => false,
            _ => c.status == "False",
        })
        .collect()
}

/// Statuses on the way to running.
const STARTING: [&str; 5] = ["Provisioning", "Starting", "WaitingForVolumeBinding", "Migrating", "Stopping"];

/// A VM's health, from KubeVirt's printable status (the design's §8.1).
pub fn health(vm: &Value, vmi: Option<&Value>) -> Health {
    let at = |state, reason: &str| Health { state, reason: Some(reason.to_string()) };
    let status = text(&vm["status"]["printableStatus"]).unwrap_or_else(|| "Unknown".into());
    let (desired, ready, _) = replicas(vm, vmi);
    match status.as_str() {
        "Stopped" | "Halted" if desired == 0 => at(HealthState::Idle, "stopped"),
        "Running" if ready == 1 => {
            if is_true(vm, "RestartRequired") {
                at(HealthState::Healthy, "restart to apply changes")
            } else {
                Health { state: HealthState::Healthy, reason: None }
            }
        }
        "Running" => at(HealthState::RollingOut, "booting"),
        "Paused" => at(HealthState::Degraded, "paused"),
        s if STARTING.contains(&s) => at(HealthState::RollingOut, &s.to_lowercase()),
        s => {
            let why = condition(vm, "Failure").and_then(|c| text(&c["message"])).or_else(|| condition(vm, "Ready").and_then(|c| text(&c["message"])));
            Health { state: HealthState::Down, reason: Some(why.map_or_else(|| s.to_string(), |m| format!("{s}: {m}"))) }
        }
    }
}

/// The containerDisk images a VM boots from.
pub fn images(vm: &Value) -> Vec<String> {
    arr(&vm["spec"]["template"]["spec"]["volumes"]).iter().filter_map(|v| text(&v["containerDisk"]["image"])).collect()
}

/// What the drawer says of a VM and its instance, if any.
pub fn detail(vm: &Value, vmi: Option<&Value>) -> VmDetail {
    let spec = &vm["spec"]["template"]["spec"];
    let devices = &spec["domain"]["devices"];
    let disks = arr(&spec["volumes"])
        .iter()
        .map(|v| {
            let name = text(&v["name"]).unwrap_or_default();
            let (kind, inner) = v.as_object().and_then(|o| o.iter().find(|(k, _)| *k != "name")).map_or(("unknown".to_string(), &Value::Null), |(k, inner)| (k.clone(), inner));
            let source = match kind.as_str() {
                "containerDisk" => text(&inner["image"]),
                "persistentVolumeClaim" => text(&inner["claimName"]),
                _ => text(&inner["name"]),
            };
            let kind = match kind.as_str() {
                "persistentVolumeClaim" => "claim".to_string(),
                "cloudInitNoCloud" | "cloudInitConfigDrive" => "cloudInit".to_string(),
                _ => kind,
            };
            let disk = arr(&devices["disks"]).iter().find(|d| d["name"] == name);
            let bus = disk.and_then(|d| if d["cdrom"].is_object() { Some("cdrom".to_string()) } else { text(&d["disk"]["bus"]) });
            VmDisk { name, kind, source, bus }
        })
        .collect();
    let st = vmi.map(|i| &i["status"]);
    let interfaces = st
        .map(|s| arr(&s["interfaces"]))
        .unwrap_or_default()
        .iter()
        .map(|i| {
            let mut ips: Vec<String> = arr(&i["ipAddresses"]).iter().filter_map(text).collect();
            if ips.is_empty() {
                ips.extend(text(&i["ipAddress"]));
            }
            VmInterface { name: text(&i["name"]).unwrap_or_default(), ips, mac: text(&i["mac"]), guest_name: text(&i["interfaceName"]) }
        })
        .collect();
    VmDetail {
        status: text(&vm["status"]["printableStatus"]).unwrap_or_else(|| "Unknown".into()),
        run_strategy: run_strategy(vm),
        cores: spec["domain"]["cpu"]["cores"].as_u64().map(|n| n as u32),
        memory: text(&spec["domain"]["memory"]["guest"]).or_else(|| text(&spec["domain"]["resources"]["requests"]["memory"])),
        node: st.and_then(|s| text(&s["nodeName"])),
        guest_os: st.and_then(|s| text(&s["guestOSInfo"]["prettyName"])),
        kernel: st.and_then(|s| text(&s["guestOSInfo"]["kernelRelease"])),
        agent_connected: vmi.is_some_and(|i| is_true(i, "AgentConnected")),
        interfaces,
        disks,
        migratable: vmi.map(|i| is_true(i, "LiveMigratable")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn vm(strategy: &str, status: &str, conditions: Value) -> Value {
        json!({ "metadata": { "namespace": "demo", "name": "deb" }, "spec": { "runStrategy": strategy,
            "template": { "metadata": { "labels": { "app": "deb" } }, "spec": {
                "domain": { "cpu": { "cores": 2 }, "memory": { "guest": "1Gi" }, "devices": { "disks": [
                    { "name": "root", "disk": { "bus": "virtio" } }, { "name": "data", "disk": { "bus": "virtio" } }, { "name": "init", "cdrom": {} } ] } },
                "volumes": [
                    { "name": "root", "containerDisk": { "image": "quay.io/containerdisks/debian:13" } },
                    { "name": "data", "persistentVolumeClaim": { "claimName": "deb-data" } },
                    { "name": "init", "cloudInitNoCloud": { "userData": "#cloud-config" } } ] } } },
            "status": { "printableStatus": status, "conditions": conditions } })
    }

    fn vmi(ready: bool) -> Value {
        json!({ "status": { "nodeName": "ms01-02", "conditions": [
                { "type": "Ready", "status": if ready { "True" } else { "False" } },
                { "type": "AgentConnected", "status": "True" }, { "type": "LiveMigratable", "status": "False" } ],
            "guestOSInfo": { "prettyName": "Debian GNU/Linux 13 (trixie)", "kernelRelease": "6.12.0-1-amd64" },
            "interfaces": [{ "name": "default", "ipAddress": "10.0.2.2", "ipAddresses": ["10.0.2.2", "fd10::2"], "mac": "52:54:00:aa:bb:cc", "interfaceName": "enp1s0" }] } })
    }

    #[test]
    fn replicas_follow_the_run_strategy_and_the_instance() {
        assert_eq!(replicas(&vm("Always", "Running", json!([])), Some(&vmi(true))), (1, 1, 1));
        assert_eq!(replicas(&vm("Halted", "Stopped", json!([])), None), (0, 0, 0));
        assert_eq!(replicas(&vm("Manual", "Stopped", json!([])), None), (0, 0, 0));
        assert_eq!(replicas(&vm("Manual", "Running", json!([])), Some(&vmi(false))), (1, 0, 1));
        let restart = json!([{ "type": "RestartRequired", "status": "True", "message": "a non-live-updatable field was changed" }]);
        assert_eq!(replicas(&vm("Always", "Running", restart), Some(&vmi(true))), (1, 1, 0));
        let old = json!({ "spec": { "running": true }, "status": {} });
        assert_eq!(run_strategy(&old), "Always", "the older spec.running");
    }

    #[test]
    fn health_reads_kubevirts_status() {
        let h = |v: Value, i: Option<Value>| {
            let h = health(&v, i.as_ref());
            (h.state, h.reason)
        };
        assert_eq!(h(vm("Always", "Running", json!([])), Some(vmi(true))), (HealthState::Healthy, None));
        assert_eq!(h(vm("Halted", "Stopped", json!([])), None), (HealthState::Idle, Some("stopped".into())));
        assert_eq!(h(vm("Always", "Starting", json!([])), Some(vmi(false))), (HealthState::RollingOut, Some("starting".into())));
        assert_eq!(h(vm("Always", "Running", json!([])), Some(vmi(false))), (HealthState::RollingOut, Some("booting".into())));
        assert_eq!(h(vm("Always", "Paused", json!([])), Some(vmi(true))).0, HealthState::Degraded);
        let unschedulable = json!([{ "type": "Ready", "status": "False", "reason": "PodNotExists" }, { "type": "Failure", "status": "True", "message": "0/4 nodes are available: 4 Insufficient devices.kubevirt.io/kvm." }]);
        assert_eq!(
            h(vm("Always", "ErrorUnschedulable", unschedulable), None),
            (HealthState::Down, Some("ErrorUnschedulable: 0/4 nodes are available: 4 Insufficient devices.kubevirt.io/kvm.".into()))
        );
        assert_eq!(h(vm("Always", "ErrImagePull", json!([])), None), (HealthState::Down, Some("ErrImagePull".into())));
        let restart = json!([{ "type": "RestartRequired", "status": "True" }]);
        assert_eq!(h(vm("Always", "Running", restart), Some(vmi(true))), (HealthState::Healthy, Some("restart to apply changes".into())));
    }

    #[test]
    fn conditions_show_what_is_wrong() {
        let cs = json!([{ "type": "Ready", "status": "False" }, { "type": "Paused", "status": "True" }, { "type": "Failure", "status": "False" }, { "type": "LiveMigratable", "status": "False", "message": "cannot migrate" }]);
        let kinds: Vec<_> = conditions(&vm("Always", "Paused", cs)).into_iter().map(|c| c.kind).collect();
        assert_eq!(kinds, ["Paused", "LiveMigratable"]);
    }

    #[test]
    fn the_drawer_has_the_guest_and_its_disks() {
        let d = detail(&vm("Always", "Running", json!([])), Some(&vmi(true)));
        assert_eq!((d.status.as_str(), d.run_strategy.as_str(), d.cores, d.memory.as_deref()), ("Running", "Always", Some(2), Some("1Gi")));
        assert_eq!((d.node.as_deref(), d.guest_os.as_deref(), d.agent_connected, d.migratable), (Some("ms01-02"), Some("Debian GNU/Linux 13 (trixie)"), true, Some(false)));
        assert_eq!(d.interfaces, [VmInterface { name: "default".into(), ips: vec!["10.0.2.2".into(), "fd10::2".into()], mac: Some("52:54:00:aa:bb:cc".into()), guest_name: Some("enp1s0".into()) }]);
        let disks: Vec<_> = d.disks.iter().map(|d| (d.name.as_str(), d.kind.as_str(), d.source.as_deref(), d.bus.as_deref())).collect();
        assert_eq!(disks, [("root", "containerDisk", Some("quay.io/containerdisks/debian:13"), Some("virtio")), ("data", "claim", Some("deb-data"), Some("virtio")), ("init", "cloudInit", None, Some("cdrom"))]);
        assert_eq!(images(&vm("Always", "Running", json!([]))), ["quay.io/containerdisks/debian:13"]);
        let stopped = detail(&vm("Halted", "Stopped", json!([])), None);
        assert_eq!((stopped.node, stopped.migratable, stopped.interfaces.len()), (None, None, 0));
    }
}
