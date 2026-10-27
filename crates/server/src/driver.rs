//! The driver: the UI's client to the control plane (a [`ClientActor`] on
//! its own node runtime, reaching the leader through the VIP) and the lab
//! it manages. Everything else talks to it through [`DriverHandle`]; what
//! the UI should see comes out on a `watch`, at most 4 times a second.

use control::client::{ClusterView, ClientActor};
use control::proto::{ApiOp, ApiResult};
use control::runtime::Svc;
use lab::cloudinit::Role;
use lab::manager::{Lab, LabFault, LabTruth, Power};
use model::HostId;
use node::runtime::{wall_ms, NodeHandle};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use ts_rs::TS;

/// Wall time between publishes: at most 4 per second.
pub const PUBLISH_EVERY: Duration = Duration::from_millis(250);
/// The pause between samples of the lab's ground truth (each asks every VM's guest agent).
pub const TRUTH_EVERY: Duration = Duration::from_millis(500);
/// How long an operation may wait for its answer. The client gives up by
/// itself after 5 s, so this only bounds a client that went away.
const OP_TIMEOUT: Duration = Duration::from_secs(10);
const OP_POLL: Duration = Duration::from_millis(25);
/// Tries at installing one machine during bring-up, and the pause between them.
const INSTALL_ATTEMPTS: u32 = 3;
const INSTALL_RETRY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub struct ClockInfo {
    /// Wall-clock Unix milliseconds on the lab host.
    #[ts(type = "number")]
    pub now: u64,
}

/// Everything the UI is shown, as of one moment.
#[derive(Debug, Clone)]
pub struct Published {
    /// Bumped if the client is replaced: a view from another epoch shares
    /// nothing with the previous one, whatever its version says.
    pub epoch: u64,
    pub view: ClusterView,
    pub truth: LabTruth,
    pub clock: ClockInfo,
}

/// Why a lab request did not happen.
#[derive(Debug, Clone, PartialEq)]
pub enum LabError {
    /// The server runs without a lab (e.g. in tests).
    NoLab,
    /// The control plane did not hand out a join token.
    NoToken,
    /// The lab refused or could not do it.
    Failed(String),
    /// The lab's code panicked: a bug, not a refusal.
    Panicked(String),
}

#[derive(Clone)]
pub struct DriverHandle {
    client: NodeHandle,
    lab: Option<Arc<Lab>>,
    published: watch::Receiver<Arc<Published>>,
}

fn empty_truth() -> LabTruth {
    LabTruth { vms: vec![], switches: vec![], ports: vec![], partitions: vec![], vip_holders: vec![], split_brain_vms: vec![], at: 0 }
}

impl DriverHandle {
    /// Starts publishing what `client` sees (and `lab`'s truth, sampled in
    /// the background). Must be called inside a tokio runtime.
    pub fn start(client: NodeHandle, lab: Option<Arc<Lab>>) -> Self {
        let first = Published { epoch: 0, view: ClusterView::default(), truth: empty_truth(), clock: ClockInfo { now: wall_ms() } };
        let (tx, published) = watch::channel(Arc::new(first));
        if let Some(lab) = lab.clone() {
            tokio::spawn(async move {
                loop {
                    let l = lab.clone();
                    let _ = tokio::task::spawn_blocking(move || l.sample_truth()).await;
                    tokio::time::sleep(TRUTH_EVERY).await;
                }
            });
        }
        let (c, l) = (client.clone(), lab.clone());
        tokio::spawn(async move {
            loop {
                let Some(view) = c.exec(|n| n.actor::<ClientActor>(Svc::Client).map(|a| a.view().clone())).await.flatten() else { return };
                let truth = l.as_ref().map_or_else(empty_truth, |l| l.truth());
                if tx.send(Arc::new(Published { epoch: 0, view, truth, clock: ClockInfo { now: wall_ms() } })).is_err() {
                    return;
                }
                tokio::time::sleep(PUBLISH_EVERY).await;
            }
        });
        Self { client, lab, published }
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<Published>> {
        self.published.clone()
    }

    /// Submits `op` through the client and waits for its final answer
    /// (`Unavailable` if none comes).
    pub async fn op(&self, op: ApiOp) -> ApiResult {
        let submitted = self.client.exec(move |n| n.with_actor::<ClientActor, _>(Svc::Client, |c, ctx| c.submit(ctx, op))).await.flatten();
        let Some(req) = submitted else { return ApiResult::Unavailable };
        let deadline = tokio::time::Instant::now() + OP_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            let answer = self.client.exec(move |n| n.with_actor::<ClientActor, _>(Svc::Client, |c, _| c.take_result(req)).flatten()).await.flatten();
            if let Some(a) = answer {
                return a;
            }
            tokio::time::sleep(OP_POLL).await;
        }
        ApiResult::Unavailable
    }

    /// A fresh join token from the control plane.
    pub async fn join_token(&self) -> Option<String> {
        match self.op(ApiOp::CreateJoinToken).await {
            ApiResult::Ok(v) => v.as_str().map(String::from),
            _ => None,
        }
    }

    /// Carries out `fault` on the lab. `AddVm` first asks the cluster for a
    /// join token, as an admin racking a machine would; the answer is then
    /// the new machine's id.
    pub async fn fault(&self, fault: LabFault) -> Result<Option<HostId>, LabError> {
        let lab = self.lab.clone().ok_or(LabError::NoLab)?;
        if let LabFault::AddVm { profile, mistake } = fault {
            let token = self.join_token().await.ok_or(LabError::NoToken)?;
            let id = tokio::task::spawn_blocking(move || lab.add_vm(profile, mistake, token)).await.map_err(|e| LabError::Panicked(e.to_string()))?;
            return id.map(Some).map_err(|e| LabError::Failed(e.to_string()));
        }
        tokio::task::spawn_blocking(move || lab.apply(fault))
            .await
            .map_err(|e| LabError::Panicked(e.to_string()))?
            .map(|()| None)
            .map_err(|e| LabError::Failed(e.to_string()))
    }

    /// Brings the lab's machines up, the way an admin installs a cluster:
    /// powers on every installed machine, installs and boots the first one
    /// as a new cluster if it has never been, waits for it to lead, then
    /// installs the rest with a join token. A machine that fails to install
    /// does not stop the others; the error names every one that failed.
    pub async fn bring_up(&self) -> Result<(), String> {
        let lab = self.lab.clone().ok_or("no lab")?;
        let ids = lab.vm_ids();
        for id in ids.iter().filter(|id| lab.installed(id)) {
            let (l, id) = (lab.clone(), id.clone());
            let r = tokio::task::spawn_blocking(move || if l.power(&id) == Power::Off { l.power_on(&id) } else { Ok(()) }).await;
            if let Ok(Err(e)) = r {
                eprintln!("lab: power on: {e}");
            }
        }
        let first = HostId::from("ms01-01");
        if !lab.installed(&first) {
            eprintln!("lab: installing {first} as a new cluster");
            install(&lab, &first, Role::Bootstrap).await?;
        }
        let rest: Vec<HostId> = ids.into_iter().filter(|id| !lab.installed(id)).collect();
        if rest.is_empty() {
            return Ok(());
        }
        let token = loop {
            if let Some(t) = self.join_token().await {
                break t;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        };
        let mut failed = vec![];
        for id in rest {
            eprintln!("lab: installing {id} to join the cluster");
            if let Err(e) = install(&lab, &id, Role::Join(token.clone())).await {
                failed.push(e);
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed.join("; "))
        }
    }
}

/// Installs `id` as `role`, trying again after a failure. A failed attempt
/// leaves the machine not installed (its root disk goes in place last), so
/// the next one starts afresh.
async fn install(lab: &Arc<Lab>, id: &HostId, role: Role) -> Result<(), String> {
    let mut attempt = 1;
    loop {
        let (l, i, r) = (lab.clone(), id.clone(), role.clone());
        let e = match tokio::task::spawn_blocking(move || l.install(&i, &r)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => e.to_string(),
            Err(e) => return Err(format!("install {id}: panicked: {e}")),
        };
        if attempt == INSTALL_ATTEMPTS {
            return Err(format!("install {id}: {e}"));
        }
        eprintln!("lab: install {id} (attempt {attempt} of {INSTALL_ATTEMPTS}): {e}; trying again");
        attempt += 1;
        tokio::time::sleep(INSTALL_RETRY).await;
    }
}
