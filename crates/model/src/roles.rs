use crate::hardware::{Arch, DiskKind, HardwareProfile, ProfileId};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Role {
    ControlPlane,
    Storage,
    Compute,
    Vm,
    Transcode,
}

/// Can a host with hardware profile `p` take role `r`? `Err` carries a reason.
pub fn eligible(p: &HardwareProfile, r: Role) -> Result<(), String> {
    match r {
        Role::Compute => Ok(()),
        Role::Storage => {
            if p.disks.iter().any(|d| !matches!(d.kind, DiskKind::Sd | DiskKind::Emmc)) {
                Ok(())
            } else {
                Err("storage needs a non-SD, non-eMMC disk".into())
            }
        }
        Role::ControlPlane => {
            if p.ram_gb < 8 {
                Err(format!("control-plane needs at least 8 GB RAM (has {} GB)", p.ram_gb))
            } else if p.arch != Arch::X86_64 && p.id != ProfileId::Rk3588 {
                Err("control-plane needs an x86_64 host or an RK3588".into())
            } else {
                Ok(())
            }
        }
        Role::Vm => {
            if p.ram_gb >= 16 {
                Ok(())
            } else {
                Err(format!("vm needs at least 16 GB RAM (has {} GB)", p.ram_gb))
            }
        }
        Role::Transcode => {
            if p.capabilities.iter().any(|c| c.starts_with("hw-transcode:")) {
                Ok(())
            } else {
                Err("transcode needs an hw-transcode:* capability".into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hardware::{profile, ProfileId};

    #[test]
    fn sd_card_pi_cannot_store() {
        assert!(eligible(&profile(ProfileId::Pi5, 0), Role::Storage).is_err());
        assert!(eligible(&profile(ProfileId::Pi5, 1), Role::Storage).is_ok());
    }
    #[test]
    fn emmc_only_rk3588_cannot_store() {
        assert!(eligible(&profile(ProfileId::Rk3588, 0), Role::Storage).is_err());
    }
    #[test]
    fn control_plane_rules() {
        assert!(eligible(&profile(ProfileId::Pi5, 1), Role::ControlPlane).is_err());
        assert!(eligible(&profile(ProfileId::Rk3588, 0), Role::ControlPlane).is_ok());
        assert!(eligible(&profile(ProfileId::MiniPc, 0), Role::ControlPlane).is_ok());
    }
    #[test]
    fn vm_needs_16gb() {
        assert!(eligible(&profile(ProfileId::Pi5, 1), Role::Vm).is_err());
    }
    #[test]
    fn transcode_needs_capability() {
        for id in [ProfileId::Ms01, ProfileId::MiniPc, ProfileId::Pi5, ProfileId::Rk3588] {
            assert!(eligible(&profile(id, 0), Role::Transcode).is_ok());
        }
    }

    #[test]
    fn vm_compute_and_failure_reasons() {
        for id in [ProfileId::Ms01, ProfileId::MiniPc, ProfileId::Rk3588] {
            assert!(eligible(&profile(id, 0), Role::Vm).is_ok());
        }
        assert!(eligible(&profile(ProfileId::Pi5, 0), Role::Compute).is_ok());
        assert!(eligible(&profile(ProfileId::Pi5, 1), Role::Vm).unwrap_err().contains("16 GB"));
        assert!(eligible(&profile(ProfileId::Pi5, 0), Role::Storage).unwrap_err().contains("SD"));
    }

    #[test]
    fn transcode_rejects_profile_without_capability() {
        let mut p = profile(ProfileId::MiniPc, 0);
        p.capabilities.clear();
        assert!(eligible(&p, Role::Transcode).is_err());
    }
}
