//! Applying the workload commands: creating and deleting deployments, VMs
//! and volumes, and recording where they run.

use super::{
    ApplyError, ClusterState, DeploymentObj, ObjectKind, ObjectRef, PodObj, VmObj, VmStatusObj, VolumeObj, VolumeStatusObj,
};
use crate::runtime::SimTime;
use model::{Deployment, HostId, PodPhase, PodStatus, ReplicaRef, Vm, VmPhase, VmStatus, Volume, VolumeHealth, VolumeStatus};

/// Why a freshly created pod or VM is not running.
pub const UNSCHEDULED: &str = "unscheduled";

impl ClusterState {
    pub(super) fn create_deployment(&mut self, v: u64, deployment: Deployment) -> Result<(), ApplyError> {
        if self.deployments.contains_key(&deployment.id) {
            return Err(ApplyError::AlreadyExists);
        }
        self.revive(ObjectKind::Deployment, &deployment.id);
        for idx in 0..deployment.replicas {
            self.revive(ObjectKind::Pod, &PodStatus::key(&deployment.id, idx));
            let status =
                PodStatus { deployment: deployment.id.clone(), idx, host: None, phase: PodPhase::Pending(UNSCHEDULED.into()) };
            self.pods.insert((deployment.id.clone(), idx), PodObj { status, version: v });
        }
        self.deployments.insert(deployment.id.clone(), DeploymentObj { deployment, version: v });
        Ok(())
    }

    pub(super) fn delete_deployment(&mut self, v: u64, id: &str) -> Result<(), ApplyError> {
        if self.deployments.remove(id).is_none() {
            return Err(ApplyError::NotFound);
        }
        self.tombstone(v, ObjectKind::Deployment, id.to_string());
        let pods: Vec<(String, u32)> = self.pods.keys().filter(|(d, _)| d == id).cloned().collect();
        for key in pods {
            self.pods.remove(&key);
            self.tombstone(v, ObjectKind::Pod, PodStatus::key(&key.0, key.1));
        }
        Ok(())
    }

    pub(super) fn create_vm(&mut self, at: SimTime, v: u64, vm: Vm) -> Result<(), ApplyError> {
        if self.vms.contains_key(&vm.id) {
            return Err(ApplyError::AlreadyExists);
        }
        if !self.volumes.contains_key(&vm.volume) {
            return Err(ApplyError::MissingDependency(ObjectRef::volume(&vm.volume)));
        }
        self.revive(ObjectKind::Vm, &vm.id);
        self.revive(ObjectKind::VmStatus, &vm.id);
        let status = VmStatus { host: None, phase: VmPhase::Pending(UNSCHEDULED.into()), since: at };
        self.vm_status.insert(vm.id.clone(), VmStatusObj { id: vm.id.clone(), status, version: v });
        self.vms.insert(vm.id.clone(), VmObj { vm, version: v });
        Ok(())
    }

    pub(super) fn delete_vm(&mut self, v: u64, id: &str) -> Result<(), ApplyError> {
        if self.vms.remove(id).is_none() {
            return Err(ApplyError::NotFound);
        }
        self.tombstone(v, ObjectKind::Vm, id.to_string());
        if self.vm_status.remove(id).is_some() {
            self.tombstone(v, ObjectKind::VmStatus, id.to_string());
        }
        Ok(())
    }

    pub(super) fn create_volume(&mut self, v: u64, volume: Volume) -> Result<(), ApplyError> {
        if self.volumes.contains_key(&volume.id) {
            return Err(ApplyError::AlreadyExists);
        }
        self.revive(ObjectKind::Volume, &volume.id);
        self.revive(ObjectKind::VolumeStatus, &volume.id);
        let status = VolumeStatus { replicas: Vec::new(), health: VolumeHealth::Faulted };
        self.volume_status.insert(volume.id.clone(), VolumeStatusObj { id: volume.id.clone(), status, version: v });
        self.volumes.insert(volume.id.clone(), VolumeObj { volume, version: v });
        Ok(())
    }

    pub(super) fn delete_volume(&mut self, v: u64, id: &str) -> Result<(), ApplyError> {
        if !self.volumes.contains_key(id) {
            return Err(ApplyError::NotFound);
        }
        let users: Vec<ObjectRef> = self.vms.values().filter(|o| o.vm.volume == id).map(|o| ObjectRef::vm(&o.vm.id)).collect();
        if !users.is_empty() {
            return Err(ApplyError::InUse(users));
        }
        self.volumes.remove(id);
        self.tombstone(v, ObjectKind::Volume, id.to_string());
        if self.volume_status.remove(id).is_some() {
            self.tombstone(v, ObjectKind::VolumeStatus, id.to_string());
        }
        Ok(())
    }

    pub(super) fn set_pod_status(
        &mut self,
        v: u64,
        deployment: String,
        idx: u32,
        host: Option<HostId>,
        phase: PodPhase,
    ) -> Result<(), ApplyError> {
        let pod = self.pods.get_mut(&(deployment, idx)).ok_or(ApplyError::NotFound)?;
        pod.status.host = host;
        pod.status.phase = phase;
        pod.version = v;
        Ok(())
    }

    pub(super) fn set_vm_status(
        &mut self,
        at: SimTime,
        v: u64,
        id: &str,
        host: Option<HostId>,
        phase: VmPhase,
    ) -> Result<(), ApplyError> {
        let vm = self.vm_status.get_mut(id).ok_or(ApplyError::NotFound)?;
        // Migrations are proposed by more than one party (the API, drains):
        // only one may start, and only it may end.
        let allowed = match (&vm.status.phase, &phase) {
            (_, VmPhase::Migrating { to }) => vm.status.phase == VmPhase::Running && vm.status.host == host && host.as_ref() != Some(to),
            (VmPhase::Migrating { to }, VmPhase::Running) => host.as_ref() == Some(to) || host == vm.status.host,
            _ => true,
        };
        if !allowed {
            return Err(ApplyError::InvalidVmPhase);
        }
        if vm.status.host != host || vm.status.phase != phase {
            vm.status.since = at;
        }
        vm.status.host = host;
        vm.status.phase = phase;
        vm.version = v;
        Ok(())
    }

    pub(super) fn set_volume_status(
        &mut self,
        v: u64,
        id: &str,
        replicas: Vec<ReplicaRef>,
        health: VolumeHealth,
    ) -> Result<(), ApplyError> {
        let vol = self.volume_status.get_mut(id).ok_or(ApplyError::NotFound)?;
        vol.status = VolumeStatus { replicas, health };
        vol.version = v;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClusterView;
    use crate::state::{Command, Object};
    use model::{ReplicaState, Selector};

    fn deployment(id: &str, replicas: u32) -> Deployment {
        Deployment { id: id.into(), image: "nginx".into(), replicas, cpu_m: 100, ram_mb: 128, selector: Selector::default() }
    }

    fn vm(id: &str, volume: &str) -> Vm {
        Vm { id: id.into(), vcpu: 2, ram_mb: 4096, disk_gb: 20, networks: vec!["vm-lan".into()], volume: volume.into() }
    }

    fn with_volume_and_vm() -> ClusterState {
        let mut s = ClusterState::new();
        s.apply(1, Command::CreateVolume { volume: Volume::new("disk1", 20) }).unwrap();
        s.apply(2, Command::CreateVm { vm: vm("vm1", "disk1") }).unwrap();
        s
    }

    fn pod_key(d: &str, idx: u32) -> (String, u32) {
        (d.to_string(), idx)
    }

    #[test]
    fn create_deployment_makes_pending_pods() {
        let mut s = ClusterState::new();
        s.apply(10, Command::CreateDeployment { deployment: deployment("web", 3) }).unwrap();
        assert_eq!(s.deployments["web"].version, 1);
        let pods: Vec<_> = s.pods.values().map(|p| (p.status.idx, p.status.host.clone(), p.status.phase.clone())).collect();
        let pending = PodPhase::Pending("unscheduled".into());
        assert_eq!(pods, [(0, None, pending.clone()), (1, None, pending.clone()), (2, None, pending)]);
        let v = s.version;
        assert_eq!(s.apply(11, Command::CreateDeployment { deployment: deployment("web", 1) }), Err(ApplyError::AlreadyExists));
        assert_eq!((s.version, s.pods.len()), (v, 3));
    }

    #[test]
    fn pods_sort_numerically() {
        let mut s = ClusterState::new();
        s.apply(1, Command::CreateDeployment { deployment: deployment("web", 12) }).unwrap();
        let idx: Vec<u32> = s.pods.values().map(|p| p.status.idx).collect();
        assert_eq!(idx, (0..12).collect::<Vec<_>>());
    }

    #[test]
    fn set_pod_status_records_host_and_phase() {
        let mut s = ClusterState::new();
        s.apply(1, Command::CreateDeployment { deployment: deployment("web", 2) }).unwrap();
        let set = |s: &mut ClusterState, idx| {
            s.apply(5, Command::SetPodStatus { deployment: "web".into(), idx, host: Some("h1".into()), phase: PodPhase::Running })
        };
        set(&mut s, 1).unwrap();
        let p = &s.pods[&pod_key("web", 1)];
        assert_eq!((p.status.host.clone(), p.status.phase.clone(), p.version), (Some("h1".into()), PodPhase::Running, 2));
        assert_eq!(s.pods[&pod_key("web", 0)].version, 1, "other pods untouched");
        let v = s.version;
        assert_eq!(set(&mut s, 2), Err(ApplyError::NotFound));
        assert_eq!(s.version, v);
    }

    #[test]
    fn delete_deployment_removes_pods_with_tombstones() {
        let mut s = ClusterState::new();
        s.apply(1, Command::CreateDeployment { deployment: deployment("web", 2) }).unwrap();
        s.apply(1, Command::CreateDeployment { deployment: deployment("db", 1) }).unwrap();
        let v = s.version;
        s.apply(2, Command::DeleteDeployment { id: "web".into() }).unwrap();
        assert_eq!(s.pods.keys().cloned().collect::<Vec<_>>(), [pod_key("db", 0)]);
        let c = s.changes_since(v);
        assert_eq!(c.removed, [ObjectRef::deployment("web"), ObjectRef::pod("web", 0), ObjectRef::pod("web", 1)]);
        assert_eq!(s.apply(3, Command::DeleteDeployment { id: "web".into() }), Err(ApplyError::NotFound));
        // recreating drops the tombstones of what comes back, not of pod web/1
        s.apply(4, Command::CreateDeployment { deployment: deployment("web", 1) }).unwrap();
        assert_eq!(s.changes_since(0).removed, [ObjectRef::pod("web", 1)]);
    }

    #[test]
    fn create_vm_needs_its_volume_and_starts_pending() {
        let mut s = ClusterState::new();
        let err = s.apply(1, Command::CreateVm { vm: vm("vm1", "disk1") }).unwrap_err();
        assert_eq!(err, ApplyError::MissingDependency(ObjectRef::volume("disk1")));
        assert_eq!(err.to_string(), "needs volume disk1, which does not exist");
        assert_eq!((s.version, s.vms.len()), (0, 0));
        let mut s = with_volume_and_vm();
        let st = &s.vm_status["vm1"].status;
        assert_eq!((st.host.clone(), st.phase.clone(), st.since), (None, VmPhase::Pending("unscheduled".into()), 2));
        assert_eq!(s.apply(3, Command::CreateVm { vm: vm("vm1", "disk1") }), Err(ApplyError::AlreadyExists));
    }

    #[test]
    fn vm_status_since_moves_only_when_host_or_phase_changes() {
        let mut s = with_volume_and_vm();
        let set = |s: &mut ClusterState, at, host: Option<&str>, phase| {
            s.apply(at, Command::SetVmStatus { id: "vm1".into(), host: host.map(Into::into), phase }).unwrap()
        };
        set(&mut s, 100, Some("h1"), VmPhase::Starting);
        assert_eq!(s.vm_status["vm1"].status.since, 100);
        set(&mut s, 200, Some("h1"), VmPhase::Starting);
        assert_eq!(s.vm_status["vm1"].status.since, 100, "unchanged");
        let v = s.version;
        set(&mut s, 300, Some("h1"), VmPhase::Running);
        assert_eq!((s.vm_status["vm1"].status.since, s.vm_status["vm1"].version), (300, v + 1));
        set(&mut s, 400, Some("h1"), VmPhase::Migrating { to: "h2".into() });
        set(&mut s, 500, Some("h2"), VmPhase::Running);
        assert_eq!(s.vm_status["vm1"].status.since, 500);
        let unknown = Command::SetVmStatus { id: "zz".into(), host: None, phase: VmPhase::Stopped };
        assert_eq!(s.apply(1, unknown), Err(ApplyError::NotFound));
    }

    #[test]
    fn deleting_a_volume_in_use_lists_the_vms() {
        let mut s = with_volume_and_vm();
        s.apply(3, Command::CreateVm { vm: vm("vm2", "disk1") }).unwrap();
        let v = s.version;
        let err = s.apply(4, Command::DeleteVolume { id: "disk1".into() }).unwrap_err();
        assert_eq!(err, ApplyError::InUse(vec![ObjectRef::vm("vm1"), ObjectRef::vm("vm2")]));
        assert_eq!(err.to_string(), "in use by vm vm1, vm vm2");
        assert_eq!(s.version, v);
        s.apply(5, Command::DeleteVm { id: "vm1".into() }).unwrap();
        s.apply(5, Command::DeleteVm { id: "vm2".into() }).unwrap();
        assert!(s.vm_status.is_empty());
        let v = s.version;
        s.apply(6, Command::DeleteVolume { id: "disk1".into() }).unwrap();
        assert!(s.volumes.is_empty() && s.volume_status.is_empty());
        let status_ref = ObjectRef { kind: ObjectKind::VolumeStatus, id: "disk1".into() };
        assert_eq!(s.changes_since(v).removed, [ObjectRef::volume("disk1"), status_ref]);
        assert_eq!(s.apply(7, Command::DeleteVolume { id: "disk1".into() }), Err(ApplyError::NotFound));
    }

    #[test]
    fn volume_status_starts_faulted_and_is_replaced() {
        let mut s = ClusterState::new();
        s.apply(1, Command::CreateVolume { volume: Volume::new("disk1", 20) }).unwrap();
        assert_eq!(s.volumes["disk1"].volume.replicas, 3);
        let st = &s.volume_status["disk1"].status;
        assert_eq!((st.replicas.len(), st.health), (0, VolumeHealth::Faulted));
        let replicas = vec![
            ReplicaRef { host: "h1".into(), disk: "nvme0".into(), state: ReplicaState::Healthy },
            ReplicaRef { host: "h2".into(), disk: "nvme0".into(), state: ReplicaState::Rebuilding { pct: 30 } },
        ];
        let set = Command::SetVolumeStatus { id: "disk1".into(), replicas: replicas.clone(), health: VolumeHealth::Rebuilding };
        s.apply(2, set).unwrap();
        assert_eq!(s.volume_status["disk1"].status, VolumeStatus { replicas, health: VolumeHealth::Rebuilding });
        let unknown = Command::SetVolumeStatus { id: "x".into(), replicas: vec![], health: VolumeHealth::Healthy };
        assert_eq!(s.apply(3, unknown), Err(ApplyError::NotFound));
    }

    #[test]
    fn workload_objects_roundtrip_and_are_tagged_by_type() {
        let mut s = with_volume_and_vm();
        s.apply(3, Command::CreateDeployment { deployment: deployment("web", 1) }).unwrap();
        let upserts = s.changes_since(0).upserts;
        let types: Vec<String> =
            upserts.iter().map(|o| serde_json::to_value(o).unwrap()["type"].as_str().unwrap().to_string()).collect();
        assert_eq!(types, ["Deployment", "Pod", "Vm", "VmStatus", "Volume", "VolumeStatus"]);
        for o in &upserts {
            let json = serde_json::to_string(o).unwrap();
            assert_eq!(&serde_json::from_str::<Object>(&json).unwrap(), o, "{json}");
        }
        // specs are flat; statuses carry their id
        let v = serde_json::to_value(Object::Vm(s.vms["vm1"].clone())).unwrap();
        assert_eq!(
            (v["type"].as_str(), v["id"].as_str(), v["volume"].as_str(), v["vcpu"].as_u64()),
            (Some("Vm"), Some("vm1"), Some("disk1"), Some(2))
        );
        let v = serde_json::to_value(Object::VmStatus(s.vm_status["vm1"].clone())).unwrap();
        assert_eq!((v["type"].as_str(), v["id"].as_str(), v["host"].is_null()), (Some("VmStatus"), Some("vm1"), true));
        let v = serde_json::to_value(Object::Pod(s.pods[&pod_key("web", 0)].clone())).unwrap();
        assert_eq!((v["type"].as_str(), v["deployment"].as_str(), v["idx"].as_u64()), (Some("Pod"), Some("web"), Some(0)));
    }

    #[test]
    fn a_client_view_that_merges_changes_mirrors_the_state() {
        let mut s = with_volume_and_vm();
        s.apply(3, Command::CreateDeployment { deployment: deployment("web", 2) }).unwrap();
        let mut view = ClusterView::default();
        view.merge(s.changes_since(0));
        assert_eq!(view.state.pods.len(), 2);
        let counts = (view.state.vms.len(), view.state.vm_status.len(), view.state.volumes.len(), view.state.volume_status.len());
        assert_eq!(counts, (1, 1, 1, 1));
        let seen = s.version;
        s.apply(4, Command::DeleteDeployment { id: "web".into() }).unwrap();
        s.apply(4, Command::DeleteVm { id: "vm1".into() }).unwrap();
        s.apply(4, Command::DeleteVolume { id: "disk1".into() }).unwrap();
        view.merge(s.changes_since(seen));
        assert!(view.state.pods.is_empty() && view.state.deployments.is_empty());
        assert!(view.state.vms.is_empty() && view.state.vm_status.is_empty());
        assert!(view.state.volumes.is_empty() && view.state.volume_status.is_empty());
    }

    #[test]
    fn applying_the_same_commands_gives_the_same_state() {
        let cmds = [
            Command::CreateVolume { volume: Volume::new("disk1", 20) },
            Command::CreateVm { vm: vm("vm1", "disk1") },
            Command::CreateDeployment { deployment: deployment("web", 2) },
            Command::SetVmStatus { id: "vm1".into(), host: Some(HostId::from("h1")), phase: VmPhase::Running },
        ];
        let run = || {
            let mut s = ClusterState::new();
            for (i, c) in cmds.iter().enumerate() {
                s.apply(i as u64 * 10, c.clone()).unwrap();
            }
            serde_json::to_string(&s.changes_since(0).upserts).unwrap()
        };
        assert_eq!(run(), run());
    }
}
