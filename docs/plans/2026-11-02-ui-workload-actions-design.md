# Homelab Cluster Manager — Creating and Managing Workloads from the UI

Date: 2026-11-02
Status: Step 1 (workload actions) built; steps 2–4 not yet.
Builds on: `2026-10-29-homelabd-k3s-responsibilities-design.md` (who owns what) and the Workloads tab, which reads Kubernetes through the VIP with the admin kubeconfig (`crates/server/src/kube.rs`).

## 1. Intent

The Workloads tab shows what Kubernetes runs but cannot change it, so even scaling a Deployment means getting a kubeconfig and running kubectl. This design adds a **limited** set of writes to the UI:

1. **Actions on a workload** (§4): scale, restart, delete.
2. **Apply YAML** (§5): paste one or more manifests, preview them with a server dry run, apply them. **Edit YAML** loads a live workload into the same box.
3. **New from image** (§6): a small form that writes the YAML for a typical homelab app, which then goes through Apply YAML like any other manifest.

kubectl stays the tool for systematic management (manifests in git, Helm, a GitOps controller later). The UI applies the same way kubectl does (server-side apply), so the two mix: a UI change is just another field manager.

### Not in scope

- Storing workload specs anywhere but Kubernetes. homelabd's Raft never holds a workload.
- Our own templating, Helm or app catalogue.
- Cluster-scoped objects other than Namespaces, and anything in the system namespaces (§3).
- Logs, exec, port-forward.
- Authentication. It is still missing everywhere (§8).

## 2. Who writes

The responsibilities design says homelabd reads pods, workloads and volumes and never writes them. That stays true: **homelabd** (the daemon on the hosts) gains no writes. The writes come from the **UI's server**, on the user's behalf, as a K3s API client with the admin kubeconfig. That is exactly what `kubectl` does with the kubeconfig the server already hands out at `/api/lab/kubeconfig`. The README's ownership table gets one line saying so.

## 3. Guard rails (in the server, not just the UI)

The server refuses, with a message naming kubectl:

| Refused | Why |
|---|---|
| Any object in `kube-system`, `kube-public`, `kube-node-lease`, `longhorn-system` | homelabd installs and supervises K3s, Longhorn and Multus. An edit there can break what it repairs. |
| Cluster-scoped objects other than `Namespace` (Nodes, CRDs, ClusterRoles, StorageClasses, PVs…) | Nodes and their `homelab.io/` labels are homelabd's. The rest is cluster administration, not running an app. |
| Deleting a protected namespace | As above. |
| A custom resource whose CRD is not installed yet, in the same batch | Discovery cannot map it. "Apply the CRD with kubectl first." |

The UI hides the actions on rows in the system namespaces, but the server is what enforces this.

## 4. Workload actions

On each row of the Workloads table: a replica count you can edit (Deployment, StatefulSet), **Restart**, **Edit YAML** and **Delete**.

| Action | API call (fieldManager `homelab-ui`) | Note |
|---|---|---|
| Scale | `PATCH …/{deployments,statefulsets}/<name>/scale`, merge patch `{"spec":{"replicas":n}}` | A later `kubectl apply` of a manifest with `replicas:` sets it back, as with `kubectl scale`. |
| Restart | `PATCH` the workload, `spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"] = <now>` | Same as `kubectl rollout restart`, all three kinds. |
| Delete | `DELETE` the workload, `propagationPolicy: Background` | The confirmation says that a StatefulSet's claims (and so its Longhorn volumes and data) are kept, and they stay in the volumes table. |

After any write the server reads Kubernetes at once (§7), so the table changes within a second rather than on the next 3 s poll.

## 5. Apply YAML

An **Apply YAML** button above the Workloads table opens a dialog: a text box, a default namespace (for objects without one), **Preview** and **Apply**.

- **Parsing** is in the server: split on `---`, skip empty documents, flatten `kind: List`, parse each document into JSON. It takes a maintained `serde_yaml` fork (to be picked when building; `serde_yaml` itself is archived). The UI stays free of a YAML dependency, and `curl` can post YAML.
- **Mapping kind to URL** uses discovery. For each `apiVersion` in the batch, `GET /api/v1` or `/apis/<group>/<version>` gives the resource name and whether it is namespaced. Fetched per request; no cache to go stale.
- **Applying**: `PATCH <url>?fieldManager=homelab-ui[&dryRun=All][&force=true]`, `content-type: application/apply-patch+yaml`, the document as JSON. One object at a time, in the order given, stopping at the first failure. Namespaces go first; kubectl keeps the given order, but this saves the usual "namespace not found".
- **Preview** is the same request with `dryRun=All`. It shows each object as *created* or *configured*, or the API's error (validation, admission, quota) next to that object. An object in a namespace the same batch creates cannot be dry-run (the namespace does not exist yet). It shows as "checked when its namespace is created" rather than failing.
- **Conflicts.** If another manager owns a field (typically `kubectl-client-side-apply` on objects made with plain `kubectl apply`), the API answers 409. The dialog lists the conflicting fields and their managers and offers **Apply anyway** (`force=true`), which makes `homelab-ui` the owner of those fields.
- **Edit YAML** on a workload row: `GET` the object and drop `status`, `metadata.managedFields`, `uid`, `resourceVersion`, `generation`, `creationTimestamp` and the `last-applied-configuration` annotation. The server serialises the rest to YAML and the dialog opens with it.

## 6. New from image

A second tab in the same dialog. It generates YAML only; nothing is applied until the user previews and applies it in the first tab.

| Field | Becomes |
|---|---|
| Name, namespace, image, replicas | A Deployment (1–n replicas), with a Namespace object if the namespace is new |
| Container port, "expose" | A ClusterIP Service on that port |
| Ingress path (optional) | An Ingress on K3s's Traefik at `http://<vip><path>`, like the demo web app |
| Volume size and mount path (optional) | A **StatefulSet** instead, 1 replica, with a `volumeClaimTemplate` on `longhorn`, and `nodeSelector: homelab.io/storage-network=true` (Longhorn runs only there) |
| Run on (role, optional) | `nodeSelector: homelab.io/<role>=true` |
| (always) | 20 s tolerations for `node.kubernetes.io/unreachable` and `not-ready`, so pods move quickly off a dead host (as `docs/setup.md` advises) |

A pure function in `ui/src/kube.ts` turns the form into YAML text, with template strings and `JSON.stringify` for scalars (valid YAML double-quoted strings). It is tested with vitest.

## 7. Server changes

- `POST /api/kube` takes a `KubeAction` (ts-rs exported, like `LabFault`): `Apply { yaml, namespace, dry_run, force }`, `Scale { namespace, kind, name, replicas }`, `Restart { … }`, `Delete { … }`. It answers 200 with per-object results, 400 refused by the guard rails or the API, 409 with the conflicts, and 503 with no kubeconfig or no API.
- `GET /api/kube/manifest?namespace=&kind=&name=` returns the cleaned YAML (§5).
- `kube.rs`: `Access` gains `apply`, `patch`, `delete`, `get` and `discover`. Each request builds its `Access` from `DriverHandle::kubeconfig()`: one control-plane round trip, fine for the rate of user writes, and the kubeconfig is always current after a reinstall.
- `kube::watch` takes a `tokio::sync::Notify`, so a write can ask for a read now.
- The paths are under `/api/kube`, not `/api/lab`: none of this needs the lab.

## 8. Security

The API has no authentication, and anyone who can reach the server can already fetch the cluster-admin kubeconfig. So these writes add no new power, but they make it a click away. The server still listens on loopback by default, and `docs/setup.md` keeps recommending an SSH tunnel. Authentication remains its own piece of work. If it comes, these writes are the first thing to put behind it.

## 9. Tests

- **Rust unit tests**: splitting and parsing documents (several documents, empty ones, comments, `List`), the guard rails, kind→URL from a sample discovery list, cleaning a manifest, reading 409 conflicts from the API's `Status`.
- **Rust request tests**: each action and apply builds its requests (method, path with `fieldManager`, `dryRun` and `force`, content type, body) in a pure function, tested directly (`kube_action.rs`). Namespaces are applied first, and the batch stops at the first failure. A fake K3s API would need TLS with client certificates, so the transport is left to the e2e run against the real cluster.
- **UI (vitest)**: the YAML that New from image generates for each combination of fields; which actions a row offers (none in system namespaces, no scale on DaemonSets); how preview results and conflicts render.
- **e2e, new step K10** in `e2e/lab.e2e.ts`. It previews and applies a small Deployment and Service through `POST /api/kube`, waits for it to be Ready, scales it to 2, restarts it (new pods), checks that an apply into `kube-system` is refused, deletes it, and waits for its pods to go.

## 10. Order of work

Each step is a commit (or a few) that leaves everything working:

1. **Workload actions**: `KubeAction::{Scale, Restart, Delete}`, guard rails, `Notify`, the row actions, the e2e scale/restart/delete part of K10.
2. **Apply YAML and Edit YAML**: the YAML crate, discovery, server-side apply with preview and conflicts, the manifest endpoint, the dialog, the rest of K10.
3. **New from image**: the form and its generator, UI only.
4. **Docs**: a "From the UI" paragraph in `docs/setup.md` under *Using the cluster*, the README's tour of the Workloads tab, and the ownership line (§2).
