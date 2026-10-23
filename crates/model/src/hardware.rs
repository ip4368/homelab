use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Arch {
    X86_64,
    Arm64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum ProfileId {
    Ms01,
    MiniPc,
    Pi5,
    Rk3588,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum NicKind {
    Sfp,
    Rj45,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct NicSpec {
    pub name: String,
    pub speed_mbps: u32,
    pub kind: NicKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum DiskKind {
    Nvme,
    Ssd,
    Sd,
    Emmc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DiskSpec {
    pub name: String,
    pub kind: DiskKind,
    pub size_gb: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct HardwareProfile {
    pub id: ProfileId,
    pub arch: Arch,
    pub cpu_threads: u32,
    pub ram_gb: u32,
    pub nics: Vec<NicSpec>,
    pub disks: Vec<DiskSpec>,
    pub capabilities: Vec<String>,
}

fn nic(name: &str, speed_mbps: u32, kind: NicKind) -> NicSpec {
    NicSpec { name: name.into(), speed_mbps, kind }
}

fn disk(name: &str, kind: DiskKind, size_gb: u32) -> DiskSpec {
    DiskSpec { name: name.into(), kind, size_gb }
}

/// Seed catalogue (spec 6.1). `variant` picks a hardware option per profile:
/// MS-01 RAM (even 96 GB, odd 64 GB), Pi 5 boot disk (even SD, odd NVMe HAT),
/// RK3588 storage (even eMMC only, odd eMMC + NVMe). MiniPc ignores it.
pub fn profile(id: ProfileId, variant: u8) -> HardwareProfile {
    let odd = variant % 2 == 1;
    match id {
        ProfileId::Ms01 => HardwareProfile {
            id,
            arch: Arch::X86_64,
            cpu_threads: 20,
            ram_gb: if odd { 64 } else { 96 },
            nics: vec![
                nic("sfp0", 10_000, NicKind::Sfp),
                nic("sfp1", 10_000, NicKind::Sfp),
                nic("eth0", 2_500, NicKind::Rj45),
                nic("eth1", 2_500, NicKind::Rj45),
            ],
            disks: vec![disk("nvme0", DiskKind::Nvme, 2000), disk("nvme1", DiskKind::Nvme, 2000)],
            capabilities: vec!["hw-transcode:quicksync".into()],
        },
        ProfileId::MiniPc => HardwareProfile {
            id,
            arch: Arch::X86_64,
            cpu_threads: 4,
            ram_gb: 16,
            nics: vec![nic("eth0", 2_500, NicKind::Rj45)],
            disks: vec![disk("nvme0", DiskKind::Nvme, 1000)],
            capabilities: vec!["hw-transcode:quicksync".into()],
        },
        ProfileId::Pi5 => HardwareProfile {
            id,
            arch: Arch::Arm64,
            cpu_threads: 4,
            ram_gb: 8,
            nics: vec![nic("eth0", 1_000, NicKind::Rj45)],
            disks: vec![if odd {
                disk("nvme0", DiskKind::Nvme, 512)
            } else {
                disk("sd0", DiskKind::Sd, 128)
            }],
            capabilities: vec!["hw-transcode:v4l2".into()],
        },
        ProfileId::Rk3588 => {
            let mut disks = vec![disk("emmc0", DiskKind::Emmc, 256)];
            if odd {
                disks.push(disk("nvme0", DiskKind::Nvme, 1000));
            }
            HardwareProfile {
                id,
                arch: Arch::Arm64,
                cpu_threads: 8,
                ram_gb: 16,
                nics: vec![nic("eth0", 2_500, NicKind::Rj45)],
                disks,
                capabilities: vec!["hw-transcode:rkmpp".into()],
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ms01_has_four_nics() {
        let p = profile(ProfileId::Ms01, 0);
        assert_eq!(
            p.nics.iter().map(|n| (n.name.as_str(), n.speed_mbps)).collect::<Vec<_>>(),
            vec![("sfp0", 10_000), ("sfp1", 10_000), ("eth0", 2_500), ("eth1", 2_500)]
        );
        assert!(p.capabilities.contains(&"hw-transcode:quicksync".into()));
    }

    #[test]
    fn variant_selects_ram_and_disks() {
        assert_eq!(profile(ProfileId::Ms01, 0).ram_gb, 96);
        assert_eq!(profile(ProfileId::Ms01, 1).ram_gb, 64);
        assert_eq!(profile(ProfileId::Ms01, 2).ram_gb, 96);
        let kinds = |id, v| profile(id, v).disks.iter().map(|d| d.kind).collect::<Vec<_>>();
        assert_eq!(kinds(ProfileId::Pi5, 0), [DiskKind::Sd]);
        assert_eq!(kinds(ProfileId::Pi5, 1), [DiskKind::Nvme]);
        assert_eq!(kinds(ProfileId::Rk3588, 0), [DiskKind::Emmc]);
        assert_eq!(kinds(ProfileId::Rk3588, 1), [DiskKind::Emmc, DiskKind::Nvme]);
    }

    #[test]
    fn catalogue_matches_spec_table() {
        let m = profile(ProfileId::MiniPc, 0);
        assert_eq!((m.arch, m.cpu_threads, m.ram_gb), (Arch::X86_64, 4, 16));
        let p = profile(ProfileId::Pi5, 0);
        assert_eq!((p.arch, p.cpu_threads, p.ram_gb), (Arch::Arm64, 4, 8));
        assert_eq!(p.nics[0].speed_mbps, 1_000);
        let r = profile(ProfileId::Rk3588, 0);
        assert_eq!((r.arch, r.cpu_threads, r.ram_gb), (Arch::Arm64, 8, 16));
        assert_eq!(profile(ProfileId::Ms01, 0).cpu_threads, 20);
    }
}
