//! A host's lifecycle phase. It lives in `model` because control,
//! the node agent and the UI all speak it.

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
    /// Leaving the cluster: the controller drains its Kubernetes workloads,
    /// evicts its Longhorn replicas, deletes its node and wipes the machine,
    /// then forgets it. Only `RemoveHost` moves a host here, and only
    /// `ForgetHost` deletes one from here.
    Removing,
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
                | (Ready, Cordoned)
                | (NotReady, Ready)
                | (NotReady, Cordoned)
                | (Cordoned, Ready)
        )
    }
}
