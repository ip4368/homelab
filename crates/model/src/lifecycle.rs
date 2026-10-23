//! A host's lifecycle phase. It lives in `model` because the scheduler
//! filters hosts by it.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Lifecycle {
    Pending,
    Approved,
    Provisioning,
    Ready,
    NotReady,
    Cordoned,
    Draining,
    Drained,
    Removed,
}

impl Lifecycle {
    /// Transitions allowed through `SetLifecycle`. `Pending -> Approved` only
    /// happens through `ApproveHost` and `Removed` only through `RemoveHost`,
    /// so neither appears as a target here.
    pub fn can_become(self, to: Lifecycle) -> bool {
        use Lifecycle::*;
        matches!(
            (self, to),
            (Approved, Provisioning)
                | (Provisioning, Ready)
                | (Ready, NotReady)
                | (Cordoned, NotReady)
                | (Draining, NotReady)
                | (Drained, NotReady)
                | (Ready, Cordoned)
                | (Ready, Draining)
                | (NotReady, Ready)
                | (NotReady, Cordoned)
                | (NotReady, Draining)
                | (Cordoned, Ready)
                | (Cordoned, Draining)
                | (Draining, Drained)
                | (Drained, Ready)
        )
    }
}
