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

/// Namespaces the server changes nothing in. K3s, Longhorn, Multus and
/// KubeVirt run there, installed by homelabd; kubectl can still change them.
pub const PROTECTED_NAMESPACES: [&str; 5] = ["kube-system", "kube-public", "kube-node-lease", "longhorn-system", "kubevirt"];

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
    /// Replaces the workload's pods, a few at a time, as `kubectl rollout
    /// restart`; a VM's guest is restarted, as `virtctl restart`.
    Restart { workload: WorkloadRef },
    /// Deletes the workload and its pods. A StatefulSet's claims, and so its
    /// Longhorn volumes, stay, as do the claims a VM only refers to.
    Delete { workload: WorkloadRef },
    /// Starts a stopped VM, as `virtctl start`.
    Start { workload: WorkloadRef },
    /// Stops a VM, as `virtctl stop`.
    Stop { workload: WorkloadRef },
}

impl KubeAction {
    fn workload(&self) -> &WorkloadRef {
        match self {
            KubeAction::Scale { workload, .. }
            | KubeAction::Restart { workload }
            | KubeAction::Delete { workload }
            | KubeAction::Start { workload }
            | KubeAction::Stop { workload } => workload,
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

/// A DNS label: lowercase letters, digits and `-`, at most 63, starting and
/// ending with a letter or digit.
fn dns_label(s: &str) -> bool {
    let b = s.as_bytes();
    let alnum = |c: &u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    !b.is_empty() && b.len() <= 63 && b.iter().all(|c| alnum(c) || *c == b'-') && alnum(&b[0]) && alnum(&b[b.len() - 1])
}

/// A Kubernetes object name: a DNS subdomain (DNS labels joined by `.`, at
/// most 253). Anything else could not name an object, and would not be safe
/// in a URL path (`..` would climb out of it).
pub fn valid_name(s: &str) -> bool {
    s.len() <= 253 && s.split('.').all(dns_label)
}

/// A namespace name: a DNS label, so no `.` either.
pub fn valid_namespace(s: &str) -> bool {
    dns_label(s)
}

/// A manifest's `apiVersion`: `v1` for the core group, else `<group>/<version>`
/// with the group a DNS subdomain. It becomes a URL path, so nothing else is
/// asked of the API.
pub fn valid_api_version(s: &str) -> bool {
    match s.split_once('/') {
        Some((group, version)) => valid_name(group) && dns_label(version),
        None => dns_label(s),
    }
}

/// Why the server will not do `action`, if it will not.
pub fn refusal(action: &KubeAction) -> Option<String> {
    let w = action.workload();
    if !valid_namespace(&w.namespace) || !valid_name(&w.name) {
        return Some(format!("{:?}/{:?} is not a Kubernetes namespace and name", w.namespace, w.name));
    }
    if PROTECTED_NAMESPACES.contains(&w.namespace.as_str()) {
        return Some(format!("{} belongs to K3s, Longhorn or KubeVirt, which homelabd installs: change it with kubectl", w.namespace));
    }
    match (action, w.kind) {
        (KubeAction::Scale { .. }, WorkloadKind::DaemonSet) => Some(format!("{} is a DaemonSet: it runs one pod on each node it may, so it has no replicas to scale", w.name)),
        (KubeAction::Scale { .. }, WorkloadKind::VirtualMachine) => Some(format!("{} is a VM: start or stop it instead", w.name)),
        (KubeAction::Start { .. } | KubeAction::Stop { .. }, k) if k != WorkloadKind::VirtualMachine => Some(format!("{} is a {k}: only VMs start and stop; scale it instead", w.name)),
        _ => None,
    }
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
    let path = w.kind.path(&w.namespace, &w.name);
    // KubeVirt's verbs are subresources of their own API group.
    let vm_verb = |verb: &str| KubeRequest {
        method: Method::PUT,
        path: format!("/apis/subresources.kubevirt.io/v1/namespaces/{}/virtualmachines/{}/{verb}", w.namespace, w.name),
        body: Some(("application/json", json!({}))),
    };
    match action {
        KubeAction::Restart { .. } if w.kind == WorkloadKind::VirtualMachine => vm_verb("restart"),
        KubeAction::Start { .. } => vm_verb("start"),
        KubeAction::Stop { .. } => vm_verb("stop"),
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
        for (ns, name) in [
            ("demo", ""),
            ("", "web"),
            ("demo", "web/scale"),
            ("demo", "Web"),
            ("../x", "web"),
            ("demo", ".."),
            ("..", "web"),
            ("demo", ".web"),
            ("demo", "web-"),
            ("demo", "a..b"),
            ("de.mo", "web"),
        ] {
            assert!(refusal(&KubeAction::Restart { workload: w(ns, WorkloadKind::Deployment, name) }).is_some(), "{ns:?}/{name:?}");
        }
        assert_eq!(refusal(&KubeAction::Restart { workload: w("demo", WorkloadKind::Deployment, "web.v2-1") }), None);
    }

    #[test]
    fn api_versions_are_a_group_and_version_or_core() {
        for ok in ["v1", "apps/v1", "networking.k8s.io/v1", "example.com/v1beta1"] {
            assert!(valid_api_version(ok), "{ok}");
        }
        for bad in ["", "/v1", "apps/", "apps/v1/x", "../api/v1", "apps/v1?watch=1", "Apps/v1", "apps/../v1"] {
            assert!(!valid_api_version(bad), "{bad}");
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
    fn vms_start_stop_and_restart_through_kubevirts_subresources() {
        let vm = w("demo", WorkloadKind::VirtualMachine, "deb");
        for (action, verb) in [(KubeAction::Start { workload: vm.clone() }, "start"), (KubeAction::Stop { workload: vm.clone() }, "stop"), (KubeAction::Restart { workload: vm.clone() }, "restart")] {
            assert_eq!(refusal(&action), None);
            let r = request(&action, 0);
            assert_eq!((r.method, r.path), (Method::PUT, format!("/apis/subresources.kubevirt.io/v1/namespaces/demo/virtualmachines/deb/{verb}")));
        }
        let r = request(&KubeAction::Delete { workload: vm.clone() }, 0);
        assert_eq!(r.path, "/apis/kubevirt.io/v1/namespaces/demo/virtualmachines/deb?propagationPolicy=Background");
        assert!(refusal(&KubeAction::Scale { workload: vm, replicas: 2 }).unwrap().contains("start or stop"));
        assert!(refusal(&KubeAction::Start { workload: w("demo", WorkloadKind::Deployment, "web") }).is_some());
        assert!(refusal(&KubeAction::Stop { workload: w("kubevirt", WorkloadKind::VirtualMachine, "x") }).unwrap().contains("kubectl"));
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
