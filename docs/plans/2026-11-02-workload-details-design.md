# Homelab Cluster Manager — Workload Health, Usage and Endpoints

Date: 2026-11-02
Status: Steps 1–2 built; step 3 (usage history) not yet.
Builds on: `2026-11-02-ui-workload-actions-design.md` and the server's Kubernetes reader (`crates/server/src/kube.rs`).

## 1. Intent

The Workloads tab says how many replicas are ready, and nothing about why not, what a workload costs, or how to reach it. This adds, from what Kubernetes already knows:

1. **Per workload, in the table** (§3–§5): health with a reason, CPU and memory (against limits), endpoints as links.
2. **A workload drawer** (§6): its pods with their usage, restarts and last termination, images, conditions, Warning events of the last hour, volumes and endpoints.
3. **Later: usage history.** metrics-server keeps only current values, so the server would sample them on each read and keep about an hour, like host metrics. Not in this round.

Still read-only, still the server's 3 s read with the admin kubeconfig.

## 2. What the server reads in addition

| List | Path | If missing |
|---|---|---|
| Services | `/api/v1/services` | read fails, as for the others |
| Ingresses | `/apis/networking.k8s.io/v1/ingresses` | read fails |
| Pod metrics | `/apis/metrics.k8s.io/v1beta1/pods` (K3s ships metrics-server) | no usage shown, and the tab says why; the rest of the read stands |
| Warning events | `/api/v1/events?fieldSelector=type%3DWarning` | read fails |

## 3. Health

A state and a reason, from the workload's status, its pods and its conditions, worst first:

| State | When | Reason shown |
|---|---|---|
| Idle | desired 0 | "scaled to 0" |
| Down | none ready, and a pod is stuck or none is starting | the stuck pods' reason: `CrashLoopBackOff`, `ImagePullBackOff`, `Unschedulable: 0/4 nodes…`, `rollout stuck` (`ProgressDeadlineExceeded`) |
| Degraded | some ready but fewer than desired, and not rolling out; or a pod stuck | as above |
| Rolling out | fewer pods updated than desired, the controller has not seen the latest spec, or pods are starting | "1/3 updated" |
| Healthy | all ready and updated | a pod's last restart, if it was `OOMKilled` or `Error` |

A pod is *stuck* when a container waits for `CrashLoopBackOff`, `ImagePullBackOff`, `ErrImagePull`, `CreateContainerConfigError`, `InvalidImageName` or `RunContainerError`, or the scheduler cannot place it (`PodScheduled` false, `Unschedulable`).

## 4. CPU and memory

Pod usage from metrics-server, summed per workload. Quantities are parsed to millicores and bytes (`250m`, `12345678n`, `0.5`; `18Mi`, `1Gi`, `123456Ki`, `1e3`). A pod's limit is the sum of its containers' limits, none if one container has none; a workload's is the sum over its pods, none if one pod has none. The UI shows `usage / limit` when there is a limit.

## 5. Endpoints

A Service belongs to a workload when it has a selector and every pair in it is in the workload's pod template labels. For each such Service:

| Service | Endpoint |
|---|---|
| An Ingress rule's backend | `http://<host or VIP><path>` (`https` if the rule's host is in the Ingress's TLS), a link |
| `LoadBalancer` | `<VIP>:<port>`: K3s's ServiceLB answers on every node, the VIP's holder too |
| `NodePort` | `<VIP>:<nodePort>` |
| `ClusterIP` | `<name>.<namespace>:<port>`, only from inside the cluster (shown greyed) |

The VIP is the host of the kubeconfig's server URL, so this holds on real machines too. `LoadBalancer` and `NodePort` endpoints are links when the port looks like HTTP (named `http…`/`web…`, or 80, 443, 8080, 8443); `https` for 443 and 8443.

## 6. The drawer

Clicking a workload's name opens a drawer like the host drawer (one drawer at a time): health and reason; conditions that are not as they should be; usage; endpoints; images; each pod with its host, status (and why: the scheduler's message, the last termination with its exit code), restarts, CPU and memory; its Longhorn volumes; Warning events of the last hour on it, its ReplicaSets and its pods, newest first, with their count.

Events are matched to workloads by the server: on a Pod, the pod's owner; on a ReplicaSet, its Deployment; on the workload itself.

## 7. Tests

- Rust: quantity parsing; health for each row of §3; Service matching and each kind of endpoint; events matched to workloads; usage and limits summed; a missing metrics API.
- UI (vitest): formatting of CPU and memory, health tones and labels, the drawer's choice of events and volumes.
- e2e K11: applies a Deployment with a Service and an Ingress, and one with an image that does not exist. It checks through the server's WebSocket view that the first is healthy with usage and an Ingress endpoint that answers, and the second down with `ImagePullBackOff` (or `ErrImagePull`) and a Warning event.
