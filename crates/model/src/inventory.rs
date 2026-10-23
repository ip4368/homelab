//! What a host reports about its own hardware at join time: the catalogue
//! profile plus live state (link status, failed disks).

use crate::hardware::{profile, DiskKind, ProfileId};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct NicState {
    pub name: String,
    pub speed_mbps: u32,
    pub link_up: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DiskState {
    pub name: String,
    pub kind: DiskKind,
    pub size_gb: u32,
    pub failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Inventory {
    pub profile: ProfileId,
    pub variant: u8,
    pub nics: Vec<NicState>,
    pub disks: Vec<DiskState>,
}

impl Inventory {
    /// A healthy machine of the given catalogue profile: all links up, no failed disks.
    pub fn from_profile(id: ProfileId, variant: u8) -> Self {
        let p = profile(id, variant);
        Self {
            profile: id,
            variant,
            nics: p
                .nics
                .into_iter()
                .map(|n| NicState { name: n.name, speed_mbps: n.speed_mbps, link_up: true })
                .collect(),
            disks: p
                .disks
                .into_iter()
                .map(|d| DiskState { name: d.name, kind: d.kind, size_gb: d.size_gb, failed: false })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_profile_is_healthy_and_matches_catalogue() {
        let inv = Inventory::from_profile(ProfileId::Rk3588, 1);
        assert_eq!((inv.profile, inv.variant), (ProfileId::Rk3588, 1));
        assert_eq!(inv.nics.len(), 1);
        assert!(inv.nics.iter().all(|n| n.link_up));
        assert_eq!(
            inv.disks.iter().map(|d| (d.name.as_str(), d.kind)).collect::<Vec<_>>(),
            [("emmc0", DiskKind::Emmc), ("nvme0", DiskKind::Nvme)]
        );
        assert!(inv.disks.iter().all(|d| !d.failed));
        let ms01 = Inventory::from_profile(ProfileId::Ms01, 0);
        assert_eq!(ms01.nics.len(), 4);
        assert_eq!(ms01.nics[0].speed_mbps, 10_000);
    }
}
