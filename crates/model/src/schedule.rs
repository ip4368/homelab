//! Placement: which host runs a pod or VM, and which hosts and disks hold a
//! volume's replicas. Pure functions over views of the hosts that the
//! controller builds from the replicated state; they use the hosts' *active*
//! roles, so a role held inactive by a broken network is never scheduled on.

use crate::hardware::{DiskKind, HardwareProfile};
use crate::ids::{HostId, SwitchId};
use crate::lifecycle::Lifecycle;
use crate::roles::Role;
use crate::workload::{Deployment, Vm};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

/// CPU kept back for the host itself, in millicores.
pub const RESERVED_CPU_M: u32 = 500;
/// RAM kept back for the host itself, in MB.
pub const RESERVED_RAM_MB: u32 = 2048;

/// What a host can give to workloads: `(cpu_m, ram_mb)`. CPU is threads x
/// 1000 m minus a 500 m reserve; RAM is the total minus 2 GB.
pub fn allocatable(p: &HardwareProfile) -> (u32, u32) {
    (
        (p.cpu_threads * 1000).saturating_sub(RESERVED_CPU_M),
        (p.ram_gb * 1024).saturating_sub(RESERVED_RAM_MB),
    )
}

/// What a workload asks of its host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementReq {
    /// Every one of these must be among the host's active roles.
    pub roles: BTreeSet<Role>,
    /// The host needs a capability starting with this.
    pub capability_prefix: Option<String>,
    pub cpu_m: u32,
    pub ram_mb: u32,
}

impl PlacementReq {
    /// One pod of `d`.
    pub fn pod(d: &Deployment) -> Self {
        Self { roles: d.required_roles(), capability_prefix: d.selector.capability_prefix.clone(), cpu_m: d.cpu_m, ram_mb: d.ram_mb }
    }

    /// `vm` needs the VM role and 1000 m per vCPU.
    pub fn vm(vm: &Vm) -> Self {
        Self { roles: [Role::Vm].into(), capability_prefix: None, cpu_m: vm.vcpu.saturating_mul(1000), ram_mb: vm.ram_mb }
    }
}

/// A host as the scheduler sees it. `alloc_*` is what the host can give to
/// workloads (see [`allocatable`]); `used_*` is what the workloads already
/// placed on it ask for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostView {
    pub id: HostId,
    pub lifecycle: Lifecycle,
    pub active_roles: BTreeSet<Role>,
    pub capabilities: Vec<String>,
    pub alloc_cpu_m: u32,
    pub alloc_ram_mb: u32,
    pub used_cpu_m: u32,
    pub used_ram_mb: u32,
    /// The access switch on the host's mgmt port, from its LLDP neighbour.
    pub switch: Option<SwitchId>,
}

/// Picks the host for `req`: among the `Ready` hosts that are not in
/// `exclude`, have every required active role and a matching capability, and
/// have room, the one whose fuller of CPU and RAM is emptiest once `req` is
/// placed; ties go to the lowest id. `Err` says how many hosts each filter
/// turned away (a host counts once, for the first filter it fails), for
/// example `no fit: 3 not ready, 10 missing role vm, 2 insufficient ram`.
pub fn place(req: &PlacementReq, hosts: &[HostView], exclude: &BTreeSet<HostId>) -> Result<HostId, String> {
    let mut why = Rejections::default();
    let mut best: Option<(f64, &HostId)> = None;
    for h in hosts {
        if let Some(reason) = check(req, h, exclude) {
            reason.count(&mut why);
            continue;
        }
        let score = fullness(h.used_cpu_m, req.cpu_m, h.alloc_cpu_m).max(fullness(h.used_ram_mb, req.ram_mb, h.alloc_ram_mb));
        if best.is_none_or(|(s, id)| score < s || (score == s && h.id < *id)) {
            best = Some((score, &h.id));
        }
    }
    best.map(|(_, id)| id.clone()).ok_or_else(|| why.explain(req, hosts.len()))
}

/// Used plus requested, as a fraction of what is allocatable.
fn fullness(used: u32, req: u32, alloc: u32) -> f64 {
    match (u64::from(used) + u64::from(req), alloc) {
        (0, _) => 0.0,
        (_, 0) => f64::INFINITY,
        (total, alloc) => total as f64 / f64::from(alloc),
    }
}

/// The first filter a host fails.
enum Reason {
    NotReady,
    Unschedulable,
    Excluded,
    MissingRole(Role),
    MissingCapability,
    Cpu,
    Ram,
}

/// How many hosts each filter turned away.
#[derive(Default)]
struct Rejections {
    not_ready: usize,
    unschedulable: usize,
    excluded: usize,
    missing_role: BTreeMap<Role, usize>,
    missing_capability: usize,
    cpu: usize,
    ram: usize,
}

impl Reason {
    fn count(self, w: &mut Rejections) {
        match self {
            Reason::NotReady => w.not_ready += 1,
            Reason::Unschedulable => w.unschedulable += 1,
            Reason::Excluded => w.excluded += 1,
            Reason::MissingRole(r) => *w.missing_role.entry(r).or_default() += 1,
            Reason::MissingCapability => w.missing_capability += 1,
            Reason::Cpu => w.cpu += 1,
            Reason::Ram => w.ram += 1,
        }
    }
}

/// Why `h` cannot take `req`, if it cannot.
fn check(req: &PlacementReq, h: &HostView, exclude: &BTreeSet<HostId>) -> Option<Reason> {
    match h.lifecycle {
        Lifecycle::Ready => {}
        Lifecycle::Cordoned | Lifecycle::Draining | Lifecycle::Drained => return Some(Reason::Unschedulable),
        _ => return Some(Reason::NotReady),
    }
    if exclude.contains(&h.id) {
        return Some(Reason::Excluded);
    }
    if let Some(&role) = req.roles.iter().find(|r| !h.active_roles.contains(r)) {
        return Some(Reason::MissingRole(role));
    }
    if let Some(prefix) = &req.capability_prefix
        && !h.capabilities.iter().any(|c| c.starts_with(prefix.as_str()))
    {
        return Some(Reason::MissingCapability);
    }
    if u64::from(h.used_cpu_m) + u64::from(req.cpu_m) > u64::from(h.alloc_cpu_m) {
        return Some(Reason::Cpu);
    }
    if u64::from(h.used_ram_mb) + u64::from(req.ram_mb) > u64::from(h.alloc_ram_mb) {
        return Some(Reason::Ram);
    }
    None
}

impl Rejections {
    fn explain(&self, req: &PlacementReq, hosts: usize) -> String {
        if hosts == 0 {
            return "no fit: no hosts".into();
        }
        let mut parts = Vec::new();
        let mut add = |n: usize, what: String| {
            if n > 0 {
                parts.push(format!("{n} {what}"));
            }
        };
        add(self.not_ready, "not ready".into());
        add(self.unschedulable, "cordoned or draining".into());
        add(self.excluded, "excluded".into());
        for (role, n) in &self.missing_role {
            add(*n, format!("missing role {}", role_name(*role)));
        }
        add(self.missing_capability, format!("missing capability {}", req.capability_prefix.as_deref().unwrap_or_default()));
        add(self.cpu, "insufficient cpu".into());
        add(self.ram, "insufficient ram".into());
        format!("no fit: {}", parts.join(", "))
    }
}

fn role_name(r: Role) -> &'static str {
    match r {
        Role::ControlPlane => "control-plane",
        Role::Storage => "storage",
        Role::Compute => "compute",
        Role::Vm => "vm",
        Role::Transcode => "transcode",
    }
}

/// A disk of a storage host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskView {
    pub name: String,
    pub kind: DiskKind,
    pub free_gb: u32,
    pub failed: bool,
}

/// A host as the volume scheduler sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageHostView {
    pub id: HostId,
    pub lifecycle: Lifecycle,
    pub active_roles: BTreeSet<Role>,
    pub disks: Vec<DiskView>,
    pub switch: Option<SwitchId>,
}

/// Picks up to `n` *new* replicas of a `size_gb` volume as `(host, disk)`
/// pairs. `existing` are the hosts that already hold a replica: they are
/// never picked again, and their switches count as taken. Candidates are
/// `Ready` hosts with the active `Storage` role and a disk that is not
/// SD/eMMC, not failed and has `size_gb` free (the disk with most free space
/// is used). Each pick prefers a host on a switch no replica uses yet, then
/// the most free space, then the lowest host id. Returns fewer than `n`
/// when there are not enough candidates.
pub fn place_replicas(n: u8, size_gb: u32, hosts: &[StorageHostView], existing: &[HostId]) -> Vec<(HostId, String)> {
    let mut taken: BTreeSet<&SwitchId> =
        existing.iter().filter_map(|e| hosts.iter().find(|h| h.id == *e)).filter_map(|h| h.switch.as_ref()).collect();
    let mut candidates: Vec<(&StorageHostView, &DiskView)> = hosts
        .iter()
        .filter(|h| h.lifecycle == Lifecycle::Ready && h.active_roles.contains(&Role::Storage) && !existing.contains(&h.id))
        .filter_map(|h| Some((h, best_disk(h, size_gb)?)))
        .collect();
    let mut picks = Vec::new();
    while picks.len() < usize::from(n) {
        // Fresh switch first, then most free space, then lowest host id.
        let Some(i) = (0..candidates.len()).min_by_key(|&i| {
            let (h, d) = candidates[i];
            (h.switch.as_ref().is_some_and(|s| taken.contains(s)), Reverse(d.free_gb), &h.id)
        }) else {
            break;
        };
        let (h, d) = candidates.swap_remove(i);
        taken.extend(h.switch.as_ref());
        picks.push((h.id.clone(), d.name.clone()));
    }
    picks
}

/// The disk of `h` with most free space (then lowest name) that can hold
/// `size_gb`: not SD or eMMC, not failed.
fn best_disk(h: &StorageHostView, size_gb: u32) -> Option<&DiskView> {
    h.disks
        .iter()
        .filter(|d| !matches!(d.kind, DiskKind::Sd | DiskKind::Emmc) && !d.failed && d.free_gb >= size_gb)
        .min_by_key(|d| (Reverse(d.free_gb), &d.name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hardware::{profile, ProfileId};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn view(id: &str, p: ProfileId, variant: u8, roles: &[Role]) -> HostView {
        let prof = profile(p, variant);
        let (alloc_cpu_m, alloc_ram_mb) = allocatable(&prof);
        HostView {
            id: hid(id),
            lifecycle: Lifecycle::Ready,
            active_roles: roles.iter().copied().collect(),
            capabilities: prof.capabilities,
            alloc_cpu_m,
            alloc_ram_mb,
            used_cpu_m: 0,
            used_ram_mb: 0,
            switch: None,
        }
    }

    fn req(roles: &[Role], cpu_m: u32, ram_mb: u32) -> PlacementReq {
        PlacementReq { roles: roles.iter().copied().collect(), capability_prefix: None, cpu_m, ram_mb }
    }

    fn none() -> BTreeSet<HostId> {
        BTreeSet::new()
    }

    #[test]
    fn allocatable_reserves_half_a_core_and_two_gb() {
        assert_eq!(allocatable(&profile(ProfileId::MiniPc, 0)), (3_500, 16 * 1024 - 2048));
        assert_eq!(allocatable(&profile(ProfileId::Ms01, 0)), (19_500, 96 * 1024 - 2048));
        assert_eq!(allocatable(&profile(ProfileId::Pi5, 0)), (3_500, 6_144));
    }

    #[test]
    fn vm_never_on_pi() {
        let vm = Vm { id: "vm1".into(), vcpu: 2, ram_mb: 2048, disk_gb: 20, networks: vec![], volume: "v".into() };
        // a Pi cannot even hold the role: the controller never lists Vm among its active roles
        let hosts = [view("pi", ProfileId::Pi5, 1, &[Role::Compute]), view("mini", ProfileId::MiniPc, 0, &[Role::Compute, Role::Vm])];
        assert_eq!(place(&PlacementReq::vm(&vm), &hosts, &none()), Ok(hid("mini")));
        let only_pi = &hosts[..1];
        let err = place(&PlacementReq::vm(&vm), only_pi, &none()).unwrap_err();
        assert_eq!(err, "no fit: 1 missing role vm");
    }

    #[test]
    fn vm_request_is_a_thousand_millicores_per_vcpu() {
        let vm = Vm { id: "vm1".into(), vcpu: 4, ram_mb: 8192, disk_gb: 20, networks: vec![], volume: "v".into() };
        let r = PlacementReq::vm(&vm);
        assert_eq!((r.cpu_m, r.ram_mb, r.roles), (4000, 8192, [Role::Vm].into()));
    }

    #[test]
    fn transcode_pod_needs_capability_and_role() {
        let d = Deployment {
            id: "jellyfin".into(),
            image: "jellyfin".into(),
            replicas: 1,
            cpu_m: 500,
            ram_mb: 512,
            selector: crate::workload::Selector { roles: Default::default(), capability_prefix: Some("hw-transcode:".into()) },
        };
        let r = PlacementReq::pod(&d);
        assert_eq!(r.roles, [Role::Compute, Role::Transcode].into());
        // role held but capability missing
        let mut no_cap = view("a-nocap", ProfileId::MiniPc, 0, &[Role::Compute, Role::Transcode]);
        no_cap.capabilities.clear();
        // capability present but role not active (e.g. held)
        let no_role = view("b-norole", ProfileId::MiniPc, 0, &[Role::Compute]);
        let both = view("c-both", ProfileId::Rk3588, 0, &[Role::Compute, Role::Transcode]);
        let err = place(&r, &[no_cap.clone(), no_role.clone()], &none()).unwrap_err();
        assert_eq!(err, "no fit: 1 missing role transcode, 1 missing capability hw-transcode:");
        assert_eq!(place(&r, &[no_cap, no_role, both], &none()), Ok(hid("c-both")));
    }

    #[test]
    fn least_allocated_wins() {
        let mut busy = view("a", ProfileId::MiniPc, 0, &[Role::Compute]);
        busy.used_cpu_m = 2_000;
        let idle = view("b", ProfileId::MiniPc, 0, &[Role::Compute]);
        let mut mid = view("c", ProfileId::MiniPc, 0, &[Role::Compute]);
        mid.used_ram_mb = 4_096;
        let r = req(&[Role::Compute], 500, 512);
        assert_eq!(place(&r, &[busy.clone(), mid.clone(), idle.clone()], &none()), Ok(hid("b")));
        // the fuller of cpu and ram decides: a is 71% cpu, c is 31% ram (cpu 14%)
        assert_eq!(place(&r, &[busy, mid], &none()), Ok(hid("c")));
        // a big host with the same absolute usage is emptier
        let big = view("z", ProfileId::Ms01, 0, &[Role::Compute]);
        let small = view("a", ProfileId::MiniPc, 0, &[Role::Compute]);
        assert_eq!(place(&r, &[small, big], &none()), Ok(hid("z")));
    }

    #[test]
    fn ties_go_to_the_lowest_id() {
        let hosts = [view("m3", ProfileId::MiniPc, 0, &[Role::Compute]), view("m1", ProfileId::MiniPc, 0, &[Role::Compute]), view("m2", ProfileId::MiniPc, 0, &[Role::Compute])];
        assert_eq!(place(&req(&[Role::Compute], 100, 100), &hosts, &none()), Ok(hid("m1")));
    }

    #[test]
    fn exclude_cordoned_and_unready_hosts_are_skipped() {
        let mut cordoned = view("a", ProfileId::MiniPc, 0, &[Role::Compute]);
        cordoned.lifecycle = Lifecycle::Cordoned;
        let mut draining = view("b", ProfileId::MiniPc, 0, &[Role::Compute]);
        draining.lifecycle = Lifecycle::Draining;
        let mut down = view("c", ProfileId::MiniPc, 0, &[Role::Compute]);
        down.lifecycle = Lifecycle::NotReady;
        let ok1 = view("d", ProfileId::MiniPc, 0, &[Role::Compute]);
        let ok2 = view("e", ProfileId::MiniPc, 0, &[Role::Compute]);
        let hosts = [cordoned, draining, down, ok1, ok2];
        let r = req(&[Role::Compute], 100, 100);
        assert_eq!(place(&r, &hosts, &[hid("d")].into()), Ok(hid("e")));
        let err = place(&r, &hosts, &[hid("d"), hid("e")].into()).unwrap_err();
        assert_eq!(err, "no fit: 1 not ready, 2 cordoned or draining, 2 excluded");
    }

    #[test]
    fn no_fit_error_explains() {
        let mut hosts = vec![];
        for (i, l) in [Lifecycle::NotReady, Lifecycle::NotReady, Lifecycle::NotReady].into_iter().enumerate() {
            let mut h = view(&format!("down{i}"), ProfileId::Ms01, 0, &[Role::Vm]);
            h.lifecycle = l;
            hosts.push(h);
        }
        for i in 0..10 {
            hosts.push(view(&format!("pi{i}"), ProfileId::Pi5, 1, &[Role::Compute]));
        }
        for i in 0..2 {
            let mut h = view(&format!("full{i}"), ProfileId::MiniPc, 0, &[Role::Vm]);
            h.used_ram_mb = h.alloc_ram_mb - 1_000;
            hosts.push(h);
        }
        let err = place(&req(&[Role::Vm], 1_000, 4_096), &hosts, &none()).unwrap_err();
        assert_eq!(err, "no fit: 3 not ready, 10 missing role vm, 2 insufficient ram");
        let err = place(&req(&[Role::Vm], 100_000, 4_096), &hosts[13..], &none()).unwrap_err();
        assert_eq!(err, "no fit: 2 insufficient cpu");
        assert_eq!(place(&req(&[Role::Vm], 1, 1), &[], &none()).unwrap_err(), "no fit: no hosts");
    }

    #[test]
    fn exact_fit_is_allowed() {
        let mut h = view("a", ProfileId::MiniPc, 0, &[Role::Compute]);
        h.used_cpu_m = h.alloc_cpu_m - 100;
        h.used_ram_mb = h.alloc_ram_mb - 100;
        assert_eq!(place(&req(&[Role::Compute], 100, 100), &[h.clone()], &none()), Ok(hid("a")));
        assert!(place(&req(&[Role::Compute], 101, 100), &[h], &none()).is_err());
    }

    fn disk(name: &str, kind: DiskKind, free_gb: u32) -> DiskView {
        DiskView { name: name.into(), kind, free_gb, failed: false }
    }

    fn shost(id: &str, switch: Option<&str>, disks: Vec<DiskView>) -> StorageHostView {
        StorageHostView {
            id: hid(id),
            lifecycle: Lifecycle::Ready,
            active_roles: [Role::Storage].into(),
            disks,
            switch: switch.map(SwitchId::from),
        }
    }

    fn nvme(free_gb: u32) -> Vec<DiskView> {
        vec![disk("nvme0", DiskKind::Nvme, free_gb)]
    }

    fn ids(picks: &[(HostId, String)]) -> Vec<&str> {
        picks.iter().map(|(h, _)| h.as_str()).collect()
    }

    #[test]
    fn replicas_distinct_hosts_prefer_distinct_switches() {
        // ids sort a1 a2 b1 b2: plain id order would take a1 a2 b1 and miss nothing,
        // but putting both a-hosts first must not starve switch sw-b either way
        let hosts = [
            shost("h1", Some("sw-a"), nvme(500)),
            shost("h2", Some("sw-a"), nvme(500)),
            shost("h3", Some("sw-b"), nvme(500)),
            shost("h4", Some("sw-b"), nvme(500)),
        ];
        let picks = place_replicas(3, 100, &hosts, &[]);
        assert_eq!(picks.len(), 3);
        let hosts_picked: BTreeSet<_> = picks.iter().map(|(h, _)| h.clone()).collect();
        assert_eq!(hosts_picked.len(), 3, "distinct hosts");
        let switches: BTreeSet<_> =
            picks.iter().map(|(h, _)| hosts.iter().find(|s| s.id == *h).unwrap().switch.clone().unwrap()).collect();
        assert_eq!(switches.len(), 2, "both switches used");
        assert_eq!(ids(&picks), ["h1", "h3", "h2"]);
    }

    #[test]
    fn existing_replicas_are_avoided_and_their_switches_count() {
        let hosts = [
            shost("h1", Some("sw-a"), nvme(500)),
            shost("h2", Some("sw-a"), nvme(500)),
            shost("h3", Some("sw-b"), nvme(500)),
        ];
        let picks = place_replicas(1, 100, &hosts, &[hid("h1")]);
        assert_eq!(ids(&picks), ["h3"], "h1 holds one; sw-a is taken so sw-b wins over h2");
        let picks = place_replicas(2, 100, &hosts, &[hid("h1")]);
        assert_eq!(ids(&picks), ["h3", "h2"]);
        assert_eq!(place_replicas(3, 100, &hosts, &[hid("h1")]).len(), 2, "h1 is never picked twice");
    }

    #[test]
    fn insufficient_storage_hosts_returns_fewer() {
        let hosts = [
            shost("h1", Some("sw-a"), nvme(500)),
            shost("h2", Some("sw-b"), nvme(500)),
            shost("small", Some("sw-b"), nvme(50)),
        ];
        let picks = place_replicas(3, 100, &hosts, &[]);
        assert_eq!(ids(&picks), ["h1", "h2"]);
        assert!(place_replicas(3, 100, &[], &[]).is_empty());
    }

    #[test]
    fn replicas_only_on_eligible_hosts_and_disks() {
        let mut no_role = shost("a-norole", None, nvme(500));
        no_role.active_roles = [Role::Compute].into();
        let mut cordoned = shost("b-cordoned", None, nvme(500));
        cordoned.lifecycle = Lifecycle::Cordoned;
        let sd_only = shost("c-sd", None, vec![disk("sd0", DiskKind::Sd, 500), disk("emmc0", DiskKind::Emmc, 500)]);
        let mut failed = shost("d-failed", None, nvme(500));
        failed.disks[0].failed = true;
        let rk = shost("e-rk", None, vec![disk("emmc0", DiskKind::Emmc, 900), disk("nvme0", DiskKind::Nvme, 300)]);
        let picks = place_replicas(5, 100, &[no_role, cordoned, sd_only, failed, rk], &[]);
        assert_eq!(picks, vec![(hid("e-rk"), "nvme0".to_string())], "emmc is skipped even when it has more room");
    }

    #[test]
    fn the_disk_with_most_free_space_is_used() {
        let h = shost(
            "h1",
            None,
            vec![disk("nvme0", DiskKind::Nvme, 200), disk("nvme1", DiskKind::Ssd, 800), disk("nvme2", DiskKind::Nvme, 800)],
        );
        assert_eq!(place_replicas(1, 100, &[h], &[]), vec![(hid("h1"), "nvme1".to_string())]);
    }

    #[test]
    fn hosts_without_a_known_switch_never_conflict() {
        let hosts = [shost("h1", None, nvme(500)), shost("h2", None, nvme(500)), shost("h3", Some("sw-a"), nvme(500))];
        let picks = place_replicas(3, 100, &hosts, &[]);
        assert_eq!(ids(&picks), ["h1", "h2", "h3"]);
    }

    #[test]
    fn more_free_space_breaks_switch_ties() {
        let hosts = [shost("h1", Some("sw-a"), nvme(200)), shost("h2", Some("sw-b"), nvme(900))];
        assert_eq!(ids(&place_replicas(1, 100, &hosts, &[])), ["h2"]);
    }
}
