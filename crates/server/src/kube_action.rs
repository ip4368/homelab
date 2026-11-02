//! Changes the user makes to Kubernetes from the Workloads tab: scaling,
//! restarting and deleting a workload. The UI's server makes them on the
//! user's behalf with the admin kubeconfig, as kubectl would; homelabd itself
//! never writes workloads (docs/plans/2026-11-02-ui-workload-actions-design.md).

use crate::kube::{rfc3339, Access, SendError, WorkloadKind};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ts_rs::TS;

/// Who the server's changes are recorded as, in each object's managed fields.
pub const FIELD_MANAGER: &str = "homelab-ui";

/// Namespaces the server changes nothing in. K3s, Longhorn and Multus run
/// there, installed and supervised by homelabd; kubectl can still change them.
pub const PROTECTED_NAMESPACES: [&str; 4] = ["kube-system", "kube-public", "kube-node-lease", "longhorn-system"];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct WorkloadRef {
    pub namespace: String,
    pub kind: WorkloadKind,
    pub name: String,
}

/// A change to a workload, as the UI asks for it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum KubeAction {
    /// Sets a Deployment's or StatefulSet's replicas, as `kubectl scale`.
    Scale { workload: WorkloadRef, replicas: u32 },
    /// Replaces the workload's pods, a few at a time, as `kubectl rollout restart`.
    Restart { workload: WorkloadRef },
    /// Deletes the workload and its pods. A StatefulSet's claims, and so its
    /// Longhorn volumes, stay.
    Delete { workload: WorkloadRef },
}

impl KubeAction {
    fn workload(&self) -> &WorkloadRef {
        match self {
            KubeAction::Scale { workload, .. } | KubeAction::Restart { workload } | KubeAction::Delete { workload } => workload,
        }
    }
}

/// Why an action did not happen.
#[derive(Debug, Clone, PartialEq)]
pub enum ActionError {
    /// The server will not do it (it names what to use instead).
    Refused(String),
    /// Kubernetes said no (4xx, its status and words).
    Kube { status: u16, message: String },
    /// No kubeconfig yet, or no answer from the Kubernetes API.
    Unavailable(String),
}

impl std::fmt::Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActionError::Refused(m) | ActionError::Kube { message: m, .. } | ActionError::Unavailable(m) => f.write_str(m),
        }
    }
}

impl From<SendError> for ActionError {
    fn from(e: SendError) -> Self {
        match e {
            SendError::Refused { status, message, .. } => ActionError::Kube { status, message },
            SendError::Unreachable(m) => ActionError::Unavailable(format!("the Kubernetes API: {m}")),
        }
    }
}

/// A Kubernetes object name: a DNS subdomain (lowercase letters, digits,
/// `-` and `.`, at most 253). Anything else could not name an object, and
/// would not be safe in a URL path.
pub fn valid_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 253 && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
}

/// Why the server will not do `action`, if it will not.
pub fn refusal(action: &KubeAction) -> Option<String> {
    let w = action.workload();
    if !valid_name(&w.namespace) || !valid_name(&w.name) {
        return Some(format!("{:?}/{:?} is not a Kubernetes namespace and name", w.namespace, w.name));
    }
    if PROTECTED_NAMESPACES.contains(&w.namespace.as_str()) {
        return Some(format!("{} belongs to K3s and Longhorn, which homelabd installs and supervises: change it with kubectl", w.namespace));
    }
    if matches!(action, KubeAction::Scale { .. }) && w.kind == WorkloadKind::DaemonSet {
        return Some(format!("{} is a DaemonSet: it runs one pod on each node it may, so it has no replicas to scale", w.name));
    }
    None
}

/// One request to the Kubernetes API.
#[derive(Debug, Clone, PartialEq)]
pub struct KubeRequest {
    pub method: Method,
    /// With its query.
    pub path: String,
    /// `(content type, body)`.
    pub body: Option<(&'static str, Value)>,
}

/// The request that carries out `action` at Unix ms `now`, as kubectl sends it.
pub fn request(action: &KubeAction, now: u64) -> KubeRequest {
    let w = action.workload();
    let path = format!("/apis/apps/v1/namespaces/{}/{}/{}", w.namespace, w.kind.resource(), w.name);
    match action {
        KubeAction::Scale { replicas, .. } => KubeRequest {
            method: Method::PATCH,
            path: format!("{path}/scale?fieldManager={FIELD_MANAGER}"),
            body: Some(("application/merge-patch+json", json!({ "spec": { "replicas": replicas } }))),
        },
        KubeAction::Restart { .. } => KubeRequest {
            method: Method::PATCH,
            path: format!("{path}?fieldManager={FIELD_MANAGER}"),
            body: Some((
                "application/merge-patch+json",
                json!({ "spec": { "template": { "metadata": { "annotations": { "kubectl.kubernetes.io/restartedAt": rfc3339(now) } } } } }),
            )),
        },
        KubeAction::Delete { .. } => KubeRequest { method: Method::DELETE, path: format!("{path}?propagationPolicy=Background"), body: None },
    }
}

/// Carries out `action` through `access`, at Unix ms `now`, unless the server refuses it.
pub async fn run(access: &Access, action: &KubeAction, now: u64) -> Result<(), ActionError> {
    if let Some(why) = refusal(action) {
        return Err(ActionError::Refused(why));
    }
    let req = request(action, now);
    access.send(req.method, &req.path, req.body.as_ref().map(|(t, b)| (*t, b))).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(namespace: &str, kind: WorkloadKind, name: &str) -> WorkloadRef {
        WorkloadRef { namespace: namespace.into(), kind, name: name.into() }
    }

    #[test]
    fn the_system_namespaces_and_bad_names_are_refused() {
        let ok = KubeAction::Restart { workload: w("demo", WorkloadKind::Deployment, "web") };
        assert_eq!(refusal(&ok), None);
        for ns in PROTECTED_NAMESPACES {
            let why = refusal(&KubeAction::Delete { workload: w(ns, WorkloadKind::DaemonSet, "x") }).expect(ns);
            assert!(why.contains("kubectl"), "{why}");
        }
        for (ns, name) in [("demo", ""), ("", "web"), ("demo", "web/scale"), ("demo", "Web"), ("../x", "web")] {
            assert!(refusal(&KubeAction::Restart { workload: w(ns, WorkloadKind::Deployment, name) }).is_some(), "{ns:?}/{name:?}");
        }
    }

    #[test]
    fn a_daemonset_cannot_be_scaled() {
        let scale = |kind| refusal(&KubeAction::Scale { workload: w("demo", kind, "x"), replicas: 2 });
        assert!(scale(WorkloadKind::DaemonSet).is_some());
        assert_eq!((scale(WorkloadKind::Deployment), scale(WorkloadKind::StatefulSet)), (None, None));
        assert_eq!(refusal(&KubeAction::Restart { workload: w("demo", WorkloadKind::DaemonSet, "x") }), None);
    }

    #[test]
    fn scaling_patches_the_scale_subresource() {
        let r = request(&KubeAction::Scale { workload: w("demo", WorkloadKind::StatefulSet, "postgres"), replicas: 0 }, 0);
        assert_eq!(r.method, Method::PATCH);
        assert_eq!(r.path, "/apis/apps/v1/namespaces/demo/statefulsets/postgres/scale?fieldManager=homelab-ui");
        assert_eq!(r.body, Some(("application/merge-patch+json", json!({ "spec": { "replicas": 0 } }))));
    }

    #[test]
    fn restarting_stamps_the_pod_template_as_kubectl_does() {
        let r = request(&KubeAction::Restart { workload: w("demo", WorkloadKind::DaemonSet, "agent") }, 1_793_109_645_000);
        assert_eq!(r.method, Method::PATCH);
        assert_eq!(r.path, "/apis/apps/v1/namespaces/demo/daemonsets/agent?fieldManager=homelab-ui");
        let (content_type, body) = r.body.unwrap();
        assert_eq!(content_type, "application/merge-patch+json");
        assert_eq!(body["spec"]["template"]["metadata"]["annotations"]["kubectl.kubernetes.io/restartedAt"], "2026-10-27T14:00:45Z");
    }

    #[test]
    fn deleting_leaves_the_pods_to_the_garbage_collector() {
        let r = request(&KubeAction::Delete { workload: w("demo", WorkloadKind::Deployment, "web") }, 0);
        assert_eq!((r.method, r.path.as_str(), r.body), (Method::DELETE, "/apis/apps/v1/namespaces/demo/deployments/web?propagationPolicy=Background", None));
    }

    #[test]
    fn actions_read_as_the_ui_sends_them() {
        let a: KubeAction = serde_json::from_value(json!({ "Scale": { "workload": { "namespace": "demo", "kind": "Deployment", "name": "web" }, "replicas": 2 } })).unwrap();
        assert_eq!(a, KubeAction::Scale { workload: w("demo", WorkloadKind::Deployment, "web"), replicas: 2 });
        assert!(serde_json::from_value::<KubeAction>(json!({ "Restart": { "workload": { "namespace": "demo", "kind": "Pod", "name": "x" } } })).is_err());
    }
}
