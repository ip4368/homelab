//! The driver: the UI's client to the control plane (a [`ClientActor`] on
//! its own node runtime, reaching the leader through the VIP) and the lab
//! it manages. Everything else talks to it through [`DriverHandle`]; what
//! the UI should see comes out on a `watch`, at most 4 times a second.

use control::client::{ClusterView, ClientActor};
use control::proto::{ApiOp, ApiResult};
use control::runtime::Svc;
use crate::kube::{self, KubeView};
use lab::install::Role;
use lab::manager::{Lab, LabFault, LabTruth, Power};
use model::HostId;
use node::runtime::{wall_ms, NodeHandle};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use ts_rs::TS;

/// The demo workloads `./lab.sh seed` and the lab panel apply.
pub const DEMO_WORKLOADS: &str = include_str!("../../../deploy/demo-workloads.yaml");

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
    /// What Kubernetes runs; replaced (a new `Arc`) on each read.
    pub kube: Arc<KubeView>,
    pub clock: ClockInfo,
}

/// Why a lab request did not happen.
#[derive(Debug, Clone, PartialEq)]
pub enum LabError {
    /// The server runs without a lab (e.g. in tests).
    NoLab,
    /// The control plane did not hand out a join token.
    NoToken,
    /// No host runs a K3s server yet.
    NoKubeServer,
    /// The lab refused or could not do it.
    Failed(String),
    /// The lab's code panicked: a bug, not a refusal.
    Panicked(String),
}

impl std::fmt::Display for LabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LabError::NoLab => f.write_str("no lab attached"),
            LabError::NoToken => f.write_str("the control plane gave no join token"),
            LabError::NoKubeServer => f.write_str("no Kubernetes server runs yet"),
            LabError::Failed(e) | LabError::Panicked(e) => f.write_str(e),
        }
    }
}

#[derive(Clone)]
pub struct DriverHandle {
    client: NodeHandle,
    lab: Option<Arc<Lab>>,
    published: watch::Receiver<Arc<Published>>,
}

fn empty_truth() -> LabTruth {
    LabTruth { vms: vec![], switches: vec![], ports: vec![], partitions: vec![], vip_holders: vec![], at: 0 }
}

impl DriverHandle {
    /// Starts publishing what `client` sees (and `lab`'s truth, sampled in
    /// the background). Must be called inside a tokio runtime.
    pub fn start(client: NodeHandle, lab: Option<Arc<Lab>>) -> Self {
        let (kube_tx, kube_rx) = watch::channel(Arc::new(KubeView::default()));
        let first = Published { epoch: 0, view: ClusterView::default(), truth: empty_truth(), kube: kube_rx.borrow().clone(), clock: ClockInfo { now: wall_ms() } };
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
                let kube = kube_rx.borrow().clone();
                if tx.send(Arc::new(Published { epoch: 0, view, truth, kube, clock: ClockInfo { now: wall_ms() } })).is_err() {
                    return;
                }
                tokio::time::sleep(PUBLISH_EVERY).await;
            }
        });
        let handle = Self { client, lab, published };
        // Reads Kubernetes with the admin kubeconfig the control plane hands out.
        let h = handle.clone();
        tokio::spawn(kube::watch(move || { let h = h.clone(); async move { h.kubeconfig().await.map_err(|e| e.to_string()) } }, kube_tx));
        handle
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

    /// An admin kubeconfig, from the control plane (its leader runs a K3s
    /// server): no access to a host needed, so it works on real machines too.
    pub async fn kubeconfig(&self) -> Result<String, LabError> {
        match self.op(ApiOp::GetKubeconfig).await {
            ApiResult::Ok(serde_json::Value::String(k)) => Ok(k),
            ApiResult::Ok(v) => Err(LabError::Failed(format!("the control plane answered with no kubeconfig: {v}"))),
            ApiResult::Err(e) => Err(LabError::Failed(e)),
            ApiResult::NotLeader | ApiResult::Unavailable => Err(LabError::Failed("the control plane did not answer".into())),
        }
    }

    /// Applies the demo workloads (`deploy/demo-workloads.yaml`), as an
    /// admin would with `kubectl apply` on a K3s server; kubectl's output.
    pub async fn seed_workloads(&self) -> Result<String, LabError> {
        self.on_kube_server(|lab, id| lab.ssh_with_input(&id, "sudo k3s kubectl apply -f -", DEMO_WORKLOADS)).await
    }

    /// Runs `f` against each host running a K3s server in turn, until one succeeds.
    async fn on_kube_server(&self, f: impl Fn(&Lab, HostId) -> std::io::Result<String> + Send + Sync + Clone + 'static) -> Result<String, LabError> {
        let lab = self.lab.clone().ok_or(LabError::NoLab)?;
        let view = self.published.borrow().view.clone();
        let servers: Vec<HostId> = view
            .state
            .hosts
            .values()
            .filter(|h| h.kube.as_ref().is_some_and(|k| k.role == Some(control::proto::KubeRole::Server) && k.phase == control::proto::KubePhase::Running))
            .map(|h| h.id.clone())
            .collect();
        if servers.is_empty() {
            return Err(LabError::NoKubeServer);
        }
        let mut last = String::new();
        for id in servers {
            let (l, f) = (lab.clone(), f.clone());
            match tokio::task::spawn_blocking(move || f(&l, id)).await.map_err(|e| LabError::Panicked(e.to_string()))? {
                Ok(out) => return Ok(out),
                Err(e) => last = e.to_string(),
            }
        }
        Err(LabError::Failed(last))
    }

    /// Brings the lab's machines up, the way a person installs a cluster
    /// (README, "Setting up a cluster"): powers on every installed machine,
    /// installs Debian on the rest, then has the technician install homelab
    /// over SSH: `init` on the first machine, and `join` on the others with
    /// the token `init` printed (or a fresh one from the control plane, if
    /// the first machine was installed before). A machine that fails does
    /// not stop the others; the error names every one that failed.
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
        let mut failed = vec![];
        for id in ids.iter().filter(|id| !lab.installed(id)) {
            eprintln!("lab: installing Debian on {id}");
            if let Err(e) = retry(&format!("install {id}"), || {
                let (l, id) = (lab.clone(), id.clone());
                async move { tokio::task::spawn_blocking(move || l.install(&id)).await.map_err(|e| e.to_string())?.map_err(|e| e.to_string()) }
            })
            .await
            {
                failed.push(e);
            }
        }
        let first = HostId::from("ms01-01");
        let mut token = None;
        if !lab.homelab_installed(&first) {
            let out = technician(&lab, &first, Role::Bootstrap).await?;
            token = lab::install::token_in(&out);
        }
        let rest: Vec<HostId> = ids.into_iter().filter(|id| *id != first && lab.installed(id) && !lab.homelab_installed(id)).collect();
        if !rest.is_empty() {
            let token = match token {
                Some(t) => t,
                None => loop {
                    if let Some(t) = self.join_token().await {
                        break t;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                },
            };
            let joins = rest.into_iter().map(|id| {
                let (lab, token) = (lab.clone(), token.clone());
                tokio::spawn(async move { technician(&lab, &id, Role::Join(token)).await })
            });
            for j in futures_join(joins).await {
                if let Err(e) = j {
                    failed.push(e);
                }
            }
        }
        if failed.is_empty() { Ok(()) } else { Err(failed.join("; ")) }
    }
}

/// Waits for every task, in order.
async fn futures_join(tasks: impl Iterator<Item = tokio::task::JoinHandle<Result<String, String>>>) -> Vec<Result<String, String>> {
    let mut out = vec![];
    for t in tasks.collect::<Vec<_>>() {
        out.push(t.await.unwrap_or_else(|e| Err(format!("panicked: {e}"))));
    }
    out
}

/// Has the technician install homelab on `id` as `role`, trying again after
/// a failure (the install command is safe to repeat).
async fn technician(lab: &Arc<Lab>, id: &HostId, role: Role) -> Result<String, String> {
    retry(&format!("install homelab on {id}"), || {
        let (l, id, role) = (lab.clone(), id.clone(), role.clone());
        async move { tokio::task::spawn_blocking(move || l.install_homelab(&id, &role)).await.map_err(|e| e.to_string())?.map_err(|e| e.to_string()) }
    })
    .await
}

/// Runs `f` up to [`INSTALL_ATTEMPTS`] times, [`INSTALL_RETRY`] apart, until it succeeds.
async fn retry<T, F, Fut>(what: &str, mut f: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let mut attempt = 1;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt == INSTALL_ATTEMPTS => return Err(format!("{what}: {e}")),
            Err(e) => {
                eprintln!("lab: {what} (attempt {attempt} of {INSTALL_ATTEMPTS}): {e}; trying again");
                attempt += 1;
                tokio::time::sleep(INSTALL_RETRY).await;
            }
        }
    }
}
