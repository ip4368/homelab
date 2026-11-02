//! Apply YAML and Edit YAML for the Workloads tab: manifests the user pastes
//! are applied as `kubectl apply --server-side` would, after a dry-run
//! preview, and a live workload is handed back as YAML to edit and apply
//! again (docs/plans/2026-11-02-ui-workload-actions-design.md §5).

use crate::kube::{Access, SendError};
use crate::kube_action::{valid_name, ActionError, WorkloadRef, FIELD_MANAGER, PROTECTED_NAMESPACES};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use ts_rs::TS;

/// Manifests to apply, as the UI sends them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ApplyRequest {
    /// One or more manifests, `---` between them.
    pub yaml: String,
    /// The namespace of namespaced objects that name none; `default` if empty.
    pub namespace: String,
    /// Only check, with the API's dry run: nothing changes.
    pub dry_run: bool,
    /// Take over fields another field manager owns, as `--force-conflicts`.
    pub force: bool,
}

/// What became of each object, in the order they were applied.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct ApplyReport {
    pub dry_run: bool,
    pub objects: Vec<AppliedObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct AppliedObject {
    pub kind: String,
    /// None for a Namespace.
    pub namespace: Option<String>,
    pub name: String,
    pub outcome: ApplyOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub enum ApplyOutcome {
    /// It did not exist (in a dry run: would be created).
    Created,
    /// It existed and changed (would change).
    Configured,
    /// It existed and the manifest changes nothing.
    Unchanged,
    /// A dry run cannot check it: its namespace is created earlier in the same batch.
    AfterNamespace,
    /// Kubernetes refused it (invalid, forbidden, quota…): its words.
    Failed { message: String },
    /// Another field manager owns fields the manifest sets to other values.
    /// Applying with `force` takes them over.
    Conflict { message: String, conflicts: Vec<FieldConflict> },
    /// Not tried: an object before it failed.
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct FieldConflict {
    /// The field manager that owns it (`kubectl-client-side-apply`, `kubectl-edit`…),
    /// with the subresource it was set through: `homelab-ui (scale)`.
    pub manager: String,
    /// As the API names it: `.spec.replicas`.
    pub field: String,
}

/// The objects in `yaml`: every document, a `List`'s items in its place, empty documents skipped.
pub fn documents(yaml: &str) -> Result<Vec<Value>, String> {
    let mut out = vec![];
    for (i, doc) in serde_yaml_ng::Deserializer::from_str(yaml).enumerate() {
        let v = Value::deserialize(doc).map_err(|e| format!("document {}: {e}", i + 1))?;
        match v {
            Value::Null => {}
            Value::Object(_) if v["kind"] == "List" => out.extend(v["items"].as_array().cloned().unwrap_or_default()),
            Value::Object(_) => out.push(v),
            _ => return Err(format!("document {}: not a Kubernetes object", i + 1)),
        }
    }
    if out.is_empty() {
        return Err("no objects in the YAML".into());
    }
    Ok(out)
}

/// A kind's resource, from discovery: its plural name and whether it is namespaced.
#[derive(Debug, Clone, PartialEq)]
pub struct Resource {
    pub name: String,
    pub namespaced: bool,
}

/// `kind`'s resource in a discovery `APIResourceList` (not a subresource such as `deployments/scale`).
pub fn resource_for(list: &Value, kind: &str) -> Option<Resource> {
    list["resources"].as_array()?.iter().find(|r| r["kind"] == kind && r["name"].as_str().is_some_and(|n| !n.contains('/'))).map(|r| Resource {
        name: r["name"].as_str().unwrap_or_default().to_string(),
        namespaced: r["namespaced"].as_bool().unwrap_or(false),
    })
}

/// Where `apiVersion`'s discovery list is: `/api/v1` for the core group, else `/apis/<group>/<version>`.
fn group_version_path(api_version: &str) -> String {
    if api_version.contains('/') { format!("/apis/{api_version}") } else { format!("/api/{api_version}") }
}

/// One object to apply, and where.
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub obj: Value,
    pub kind: String,
    pub name: String,
    pub namespace: Option<String>,
    /// The object's URL path, without a query.
    pub path: String,
}

/// The objects of `docs` to apply, Namespaces first, each with its URL from
/// `discovered` (each apiVersion's `APIResourceList`, or `Null` if the API
/// does not serve it). An object without a namespace goes in `namespace`.
/// The whole batch is refused if any object is: nothing is applied then.
pub fn targets(docs: Vec<Value>, namespace: &str, discovered: &BTreeMap<String, Value>) -> Result<Vec<Target>, String> {
    let default_ns = if namespace.is_empty() { "default" } else { namespace };
    let mut out = vec![];
    for (i, mut obj) in docs.into_iter().enumerate() {
        let n = i + 1;
        let (Some(api_version), Some(kind)) = (obj["apiVersion"].as_str().map(String::from), obj["kind"].as_str().map(String::from)) else {
            return Err(format!("object {n} has no apiVersion and kind"));
        };
        let Some(name) = obj["metadata"]["name"].as_str().map(String::from) else {
            return Err(format!("{kind} (object {n}) has no metadata.name; generateName is not supported"));
        };
        let what = format!("{kind} {name}");
        if !valid_name(&name) {
            return Err(format!("{what}: {name:?} is not a Kubernetes name"));
        }
        let Some(res) = discovered.get(&api_version).and_then(|list| resource_for(list, &kind)) else {
            return Err(format!("{what}: Kubernetes has no {kind} in {api_version}. If a CRD defines it, apply the CRD with kubectl first"));
        };
        let base = group_version_path(&api_version);
        let (ns, path) = if res.namespaced {
            let ns = obj["metadata"]["namespace"].as_str().filter(|s| !s.is_empty()).unwrap_or(default_ns).to_string();
            if !valid_name(&ns) {
                return Err(format!("{what}: {ns:?} is not a Kubernetes namespace"));
            }
            if PROTECTED_NAMESPACES.contains(&ns.as_str()) {
                return Err(format!("{what}: {ns} belongs to K3s and Longhorn, which homelabd installs and supervises: change it with kubectl"));
            }
            obj["metadata"]["namespace"] = Value::String(ns.clone());
            let path = format!("{base}/namespaces/{ns}/{}/{name}", res.name);
            (Some(ns), path)
        } else if kind == "Namespace" && api_version == "v1" {
            if PROTECTED_NAMESPACES.contains(&name.as_str()) {
                return Err(format!("{what}: it belongs to K3s and Longhorn, which homelabd installs and supervises: change it with kubectl"));
            }
            (None, format!("{base}/{}/{name}", res.name))
        } else {
            return Err(format!("{what}: a {kind} is not in a namespace, and the UI changes nothing cluster-wide but Namespaces: apply it with kubectl"));
        };
        out.push(Target { obj, kind, name, namespace: ns, path });
    }
    // Namespaces first, so their objects find them; otherwise in the order given.
    out.sort_by_key(|t| t.namespace.is_some());
    Ok(out)
}

/// The server-side apply of `t`'s object, as `kubectl apply --server-side`.
pub fn apply_path(t: &Target, dry_run: bool, force: bool) -> String {
    let mut path = format!("{}?fieldManager={FIELD_MANAGER}", t.path);
    if dry_run {
        path.push_str("&dryRun=All");
    }
    if force {
        path.push_str("&force=true");
    }
    path
}

/// `obj` without what Kubernetes keeps for itself: status and bookkeeping.
fn content(obj: &Value) -> Value {
    let mut v = obj.clone();
    if let Some(m) = v.as_object_mut() {
        m.remove("status");
    }
    if let Some(meta) = v["metadata"].as_object_mut() {
        for k in ["managedFields", "resourceVersion", "generation"] {
            meta.remove(k);
        }
    }
    v
}

/// Whether an apply that answered `after` changed the object that was `before`.
pub fn changed(before: &Value, after: &Value) -> bool {
    content(before) != content(after)
}

/// The fields a 409's `Status` says another manager owns.
pub fn conflicts(status: &Value) -> Vec<FieldConflict> {
    status["details"]["causes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["reason"] == "FieldManagerConflict")
        .map(|c| {
            let message = c["message"].as_str().unwrap_or_default();
            // `conflict with "kubectl-client-side-apply" using apps/v1`, or
            // `conflict with "homelab-ui" with subresource "scale" using apps/v1`
            let quoted: Vec<&str> = message.split('"').skip(1).step_by(2).collect();
            let manager = match (quoted.first(), quoted.get(1)) {
                (Some(m), Some(sub)) if message.contains("with subresource") => format!("{m} ({sub})"),
                (Some(m), _) => m.to_string(),
                _ => message.to_string(),
            };
            FieldConflict { manager, field: c["field"].as_str().unwrap_or_default().to_string() }
        })
        .collect()
}

async fn apply_one(access: &Access, t: &Target, dry_run: bool, force: bool) -> ApplyOutcome {
    let before = match access.send(Method::GET, &t.path, None).await {
        Ok(v) => Some(v),
        Err(SendError::Refused { status: 404, .. }) => None,
        Err(e) => return ApplyOutcome::Failed { message: e.to_string() },
    };
    match access.send(Method::PATCH, &apply_path(t, dry_run, force), Some(("application/apply-patch+yaml", &t.obj))).await {
        Ok(after) => match before {
            None => ApplyOutcome::Created,
            Some(b) if changed(&b, &after) => ApplyOutcome::Configured,
            Some(_) => ApplyOutcome::Unchanged,
        },
        Err(SendError::Refused { status: 409, message, status_body }) => ApplyOutcome::Conflict { conflicts: conflicts(&status_body), message },
        Err(e) => ApplyOutcome::Failed { message: e.to_string() },
    }
}

/// Applies (or with `dry_run`, checks) the manifests of `req` one at a
/// time, stopping at the first that fails or conflicts.
pub async fn apply(access: &Access, req: &ApplyRequest) -> Result<ApplyReport, ActionError> {
    let docs = documents(&req.yaml).map_err(ActionError::Refused)?;
    let mut discovered = BTreeMap::new();
    for api_version in docs.iter().filter_map(|d| d["apiVersion"].as_str()) {
        if discovered.contains_key(api_version) {
            continue;
        }
        let list = match access.send(Method::GET, &group_version_path(api_version), None).await {
            Ok(list) => list,
            Err(SendError::Refused { status: 404, .. }) => Value::Null,
            Err(e) => return Err(e.into()),
        };
        discovered.insert(api_version.to_string(), list);
    }
    let targets = targets(docs, &req.namespace, &discovered).map_err(ActionError::Refused)?;
    let (mut objects, mut stopped, mut new_namespaces) = (vec![], false, BTreeSet::new());
    for t in targets {
        let outcome = if stopped {
            ApplyOutcome::Skipped
        } else if req.dry_run && t.namespace.as_ref().is_some_and(|ns| new_namespaces.contains(ns)) {
            ApplyOutcome::AfterNamespace
        } else {
            apply_one(access, &t, req.dry_run, req.force).await
        };
        if t.namespace.is_none() && outcome == ApplyOutcome::Created {
            new_namespaces.insert(t.name.clone());
        }
        stopped |= matches!(outcome, ApplyOutcome::Failed { .. } | ApplyOutcome::Conflict { .. });
        objects.push(AppliedObject { kind: t.kind, namespace: t.namespace, name: t.name, outcome });
    }
    Ok(ApplyReport { dry_run: req.dry_run, objects })
}

/// A live object as a manifest to edit and apply again: without its status
/// and what Kubernetes and controllers keep in its metadata.
pub fn cleaned(obj: &Value) -> Value {
    let mut v = content(obj);
    if let Some(meta) = v["metadata"].as_object_mut() {
        for k in ["uid", "creationTimestamp", "selfLink"] {
            meta.remove(k);
        }
        if let Some(a) = meta.get_mut("annotations").and_then(Value::as_object_mut) {
            a.remove("kubectl.kubernetes.io/last-applied-configuration");
            a.remove("deployment.kubernetes.io/revision");
            if a.is_empty() {
                meta.remove("annotations");
            }
        }
    }
    if let Some(meta) = v["spec"]["template"]["metadata"].as_object_mut() {
        meta.remove("creationTimestamp");
    }
    v
}

/// The workload `w` as YAML to edit (see [`cleaned`]).
pub async fn manifest(access: &Access, w: &WorkloadRef) -> Result<String, ActionError> {
    if !valid_name(&w.namespace) || !valid_name(&w.name) {
        return Err(ActionError::Refused(format!("{:?}/{:?} is not a Kubernetes namespace and name", w.namespace, w.name)));
    }
    let obj = access.send(Method::GET, &format!("/apis/apps/v1/namespaces/{}/{}/{}", w.namespace, w.kind.resource(), w.name), None).await?;
    serde_yaml_ng::to_string(&cleaned(&obj)).map_err(|e| ActionError::Refused(format!("{} {}: {e}", w.kind, w.name)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn discovery() -> BTreeMap<String, Value> {
        let core = json!({ "resources": [
            { "name": "namespaces", "kind": "Namespace", "namespaced": false },
            { "name": "services", "kind": "Service", "namespaced": true },
            { "name": "nodes", "kind": "Node", "namespaced": false },
            { "name": "services/status", "kind": "Service", "namespaced": true },
        ] });
        let apps = json!({ "resources": [
            { "name": "deployments/scale", "kind": "Scale", "namespaced": true },
            { "name": "deployments", "kind": "Deployment", "namespaced": true },
        ] });
        [("v1".to_string(), core), ("apps/v1".to_string(), apps), ("example.com/v1".to_string(), Value::Null)].into()
    }

    const APP: &str = "
# the app
apiVersion: apps/v1
kind: Deployment
metadata: { name: web }
spec: { replicas: 2 }
---
---
apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: other
---
apiVersion: v1
kind: Namespace
metadata: { name: shop }
";

    #[test]
    fn documents_are_split_and_lists_flattened() {
        let docs = documents(APP).unwrap();
        let kinds: Vec<_> = docs.iter().map(|d| d["kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["Deployment", "Service", "Namespace"]);
        let list = documents("{\"kind\": \"List\", \"apiVersion\": \"v1\", \"items\": [{\"kind\": \"Service\"}, {\"kind\": \"Namespace\"}]}").unwrap();
        assert_eq!(list.len(), 2, "JSON is YAML too");
        assert!(documents("---\n# nothing\n---\n").is_err());
        assert!(documents("- a\n- b\n").unwrap_err().contains("document 1"));
        assert!(documents("kind: [unclosed").unwrap_err().contains("document 1"));
    }

    #[test]
    fn targets_go_namespaces_first_each_with_its_url() {
        let ts = targets(documents(APP).unwrap(), "shop", &discovery()).unwrap();
        let rows: Vec<_> = ts.iter().map(|t| (t.kind.as_str(), t.namespace.as_deref(), t.path.as_str())).collect();
        assert_eq!(
            rows,
            [
                ("Namespace", None, "/api/v1/namespaces/shop"),
                ("Deployment", Some("shop"), "/apis/apps/v1/namespaces/shop/deployments/web"),
                ("Service", Some("other"), "/api/v1/namespaces/other/services/web"),
            ]
        );
        assert_eq!(ts[1].obj["metadata"]["namespace"], "shop", "the default namespace is written into the object");
        let ts = targets(documents("apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}").unwrap(), "", &discovery()).unwrap();
        assert_eq!(ts[0].namespace.as_deref(), Some("default"));
    }

    #[test]
    fn the_batch_is_refused_for_system_namespaces_cluster_objects_and_unknown_kinds() {
        let refused = |yaml: &str| targets(documents(&format!("{APP}\n---\n{yaml}")).unwrap(), "", &discovery()).unwrap_err();
        assert!(refused("apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: x, namespace: longhorn-system}").contains("kubectl"));
        assert!(refused("apiVersion: v1\nkind: Namespace\nmetadata: {name: kube-system}").contains("kubectl"));
        assert!(refused("apiVersion: v1\nkind: Node\nmetadata: {name: ms01-01}").contains("not in a namespace"));
        assert!(refused("apiVersion: example.com/v1\nkind: Widget\nmetadata: {name: x}").contains("CRD"));
        assert!(refused("apiVersion: v1\nkind: Service\nmetadata: {generateName: x-}").contains("metadata.name"));
        assert!(refused("kind: Service\nmetadata: {name: x}").contains("apiVersion"));
        assert!(refused("apiVersion: v1\nkind: Service\nmetadata: {name: x, namespace: ../etc}").contains("namespace"));
    }

    #[test]
    fn applying_is_server_side_with_the_field_manager() {
        let t = &targets(documents(APP).unwrap(), "", &discovery()).unwrap()[1];
        assert_eq!(apply_path(t, false, false), "/apis/apps/v1/namespaces/default/deployments/web?fieldManager=homelab-ui");
        assert_eq!(apply_path(t, true, true), "/apis/apps/v1/namespaces/default/deployments/web?fieldManager=homelab-ui&dryRun=All&force=true");
    }

    #[test]
    fn only_a_change_of_content_counts_as_configured() {
        let before = json!({ "metadata": { "name": "web", "resourceVersion": "1", "generation": 1 }, "spec": { "replicas": 2 }, "status": { "readyReplicas": 1 } });
        let touched = json!({ "metadata": { "name": "web", "resourceVersion": "2", "generation": 1, "managedFields": [{}] }, "spec": { "replicas": 2 }, "status": { "readyReplicas": 2 } });
        let scaled = json!({ "metadata": { "name": "web", "resourceVersion": "3", "generation": 2 }, "spec": { "replicas": 3 } });
        assert!(!changed(&before, &touched));
        assert!(changed(&before, &scaled));
    }

    #[test]
    fn conflicts_name_the_owning_manager_and_field() {
        let status = json!({ "kind": "Status", "code": 409, "details": { "causes": [
            { "reason": "FieldManagerConflict", "message": "conflict with \"kubectl-client-side-apply\" using apps/v1", "field": ".spec.replicas" },
            { "reason": "FieldValueInvalid", "message": "x", "field": ".x" },
            { "reason": "FieldManagerConflict", "message": "conflict with \"homelab-ui\" with subresource \"scale\" using apps/v1", "field": ".spec.replicas" },
        ] } });
        assert_eq!(
            conflicts(&status),
            [
                FieldConflict { manager: "kubectl-client-side-apply".into(), field: ".spec.replicas".into() },
                FieldConflict { manager: "homelab-ui (scale)".into(), field: ".spec.replicas".into() },
            ]
        );
        assert_eq!(conflicts(&json!({})), []);
    }

    #[test]
    fn a_live_workload_is_cleaned_into_a_manifest() {
        let live = json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": { "name": "web", "namespace": "demo", "uid": "u", "resourceVersion": "9", "generation": 3, "creationTimestamp": "t", "managedFields": [{}],
                "labels": { "app": "web" },
                "annotations": { "deployment.kubernetes.io/revision": "3", "kubectl.kubernetes.io/last-applied-configuration": "{}" } },
            "spec": { "replicas": 2, "template": { "metadata": { "creationTimestamp": null, "labels": { "app": "web" } } } },
            "status": { "replicas": 2 }
        });
        assert_eq!(
            cleaned(&live),
            json!({
                "apiVersion": "apps/v1", "kind": "Deployment",
                "metadata": { "name": "web", "namespace": "demo", "labels": { "app": "web" } },
                "spec": { "replicas": 2, "template": { "metadata": { "labels": { "app": "web" } } } }
            })
        );
        let yaml = serde_yaml_ng::to_string(&cleaned(&live)).unwrap();
        assert!(yaml.starts_with("apiVersion: apps/v1\nkind: Deployment\nmetadata:"), "{yaml}");
        assert_eq!(documents(&yaml).unwrap(), [cleaned(&live)], "it reads back as it was");
    }
}
