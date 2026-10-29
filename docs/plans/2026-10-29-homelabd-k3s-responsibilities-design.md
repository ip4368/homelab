# Homelab Cluster Manager — What homelabd Owns, What K3s Owns, and Supervising K3s

Date: 2026-10-29
Status: Design; nothing built yet.
Builds on: `2026-10-27-k3s-longhorn-lab-design.md` (K3s and Longhorn on the VM lab) and commit `30dc274`, which removed the simulator and homelabd's own workloads: Kubernetes runs every workload.

## 1. Intent

homelabd keeps its own Raft control plane beside K3s. It does not depend on K3s, so it can still see, report and repair K3s when K3s is the thing that broke. That only works if the two never share a decision. This design:

- states **one owner for every responsibility**: homelabd or K3s, the other side only reading it (§2);
- names the **two kinds of "down"** and what each combination means (§3);
- makes homelabd **supervise K3s**: it watches K3s's health, alerts the user, and repairs what it safely can, saying what it did (§4).

## 2. Who owns what

The rule: **homelabd owns the machines and everything K3s stands on, and supervises K3s. K3s owns everything that runs on it.**

| Area | Owner | The other side |
|---|---|---|
| Hosts: join, approval, roles, labels, lifecycle, removal | homelabd | K3s sees the result: a node with labels |
| Host networking: links, bonds, VLANs, addresses, rollouts; switches | homelabd | K3s never changes host networking |
| The VIP | homelabd's leader | The K3s API answers on it |
| Which hosts run a K3s server or agent; etcd membership | homelabd (servers are the Raft voters) | K3s runs what it is given |
| Installing K3s, Longhorn and Multus; keeping their services running | homelabd's agent | |
| Node labels under `homelab.io/` | homelabd | Other labels belong to users and Kubernetes |
| Which data disks Longhorn may use | homelabd (the storage role) | Longhorn places replicas on them |
| Cordon and drain on a lifecycle change | homelabd starts it | K3s evicts the pods |
| Nodes of removed hosts | homelabd deletes them | |
| Pods, scheduling, restarts, eviction timing, PVCs, Longhorn volumes and replicas | **K3s and Longhorn only** | homelabd reads them (the Workloads tab, §4 alerts) and never writes them |
| Cordoning by hand (`kubectl cordon`) | the user, in Kubernetes | homelabd shows it and leaves it alone (§2.1) |
| Host metrics: CPU, RAM, disks, ports, temperature | homelabd | |
| K3s's health: alerts and repairs | homelabd | |

homelabd writes to Kubernetes only through the K3s worker on each host, and only these things:

- its own node's labels and Longhorn disk annotations (agent credentials);
- the Longhorn and Multus `HelmChart` manifests, on servers (installing them);
- its own node, deleted when a server leaves to become an agent, so its etcd member goes;
- its own Longhorn instance-manager pods that run without the storage network attachment, deleted so Longhorn makes them again attached. This is the one pod homelabd touches: the attachment is host networking, which homelabd owns;
- on one running server with admin credentials, the cluster-wide work the leader asks for (`KubeAdmin`): etcd member removal, Longhorn disk scheduling, cordon and drain, deleting removed hosts' nodes.

Anything else in Kubernetes is K3s's or the user's.

### 2.1 Cordoned by hand

A node a user cordons with kubectl has no `homelab.io/cordoned` annotation. homelabd never uncordons it, and Uncordon in homelabd does not touch it. The host drawer says "node cordoned outside homelabd". homelabd's lifecycle stays what it was: it describes homelabd's intent, not Kubernetes' state.

## 3. Two kinds of "down"

| Name in the UI | Means | Decided by |
|---|---|---|
| **Host NotReady** (lifecycle) | homelabd's leader has heard no heartbeat for 40 s | homelabd |
| **Node not Ready** | the node's kubelet stopped reporting, as Kubernetes sees it | K3s |

The UI never shortens either to just "down". What each combination means and who acts:

| Host | Node | Likely cause | homelabd does | K3s does |
|---|---|---|---|---|
| Ready | Ready | healthy | | |
| Ready | not Ready | K3s on that host is broken (hung, crash-looping, can't reach the API) | `KubeNodeNotReady`; repairs (§4.3) | evicts its pods after its own timeouts |
| NotReady | Ready | homelabd's agent or the mgmt path to the leader broke, K3s didn't | `HostDown`; nothing in Kubernetes | keeps running the pods |
| NotReady | not Ready | the host is off, or cut off | `HostDown`; after 10 min suggests replacing a dead voter | evicts its pods |

homelabd removes a dead host's etcd member only when both are down (it already refuses to delete a `Ready` node). It never evicts pods because a host is NotReady: eviction timing is Kubernetes'. Nor does it change a cordon then: the node of a Cordoned, Draining or Drained host stays cordoned while the host is NotReady, since its kubelet may run on, and a removed host's node stays cordoned until it is deleted.

## 4. Supervising K3s

### 4.1 What homelabd watches

Observations stay in the leader's memory, like metrics: they change too often for the Raft log. Decisions (alerts, repair attempts) go in the log, so a new leader does not reset them.

- **Every host's agent** already reports `KubeStatus`: role, phase (`Failed` with the reason), node Ready, version. It adds how often the K3s service restarted (systemd's `NRestarts`), to tell a crash loop from a one-off.
- **Every running K3s server** sends a `KubeHealth` report every 10 s:
  - `api_ready`: its own API's `/readyz`, on loopback;
  - each node's Ready and unschedulable, and whether homelabd cordoned it;
  - each Longhorn volume's robustness.

  The leader uses the report of its own server for "is the API behind the VIP up", and the first other running server's for the rest.

### 4.2 Alerts

Each alert is raised once its condition has held for its grace period, says what homelabd has done about it so far (§4.3), and resolves when the condition clears.

| Alert | Severity | Condition (grace) |
|---|---|---|
| `KubeApiDown` | Critical | the leader's K3s API is not ready, so the VIP serves no API (60 s) |
| `KubeServerDown` | Warning | a voter's K3s server is not running or its API is not ready, while the host heartbeats (60 s) |
| `KubeNodeNotReady` | Warning | the host is Ready and runs K3s, but its node is not Ready (2 min) |
| `KubeInstallFailed` | Warning | the host's K3s phase has stayed `Failed` (5 min; the worker keeps retrying) |
| `EtcdQuorumAtRisk` | Critical | the etcd members on running servers are exactly a quorum, so one more loss stops Kubernetes (60 s) |
| `LonghornVolumeDegraded` | Warning | a volume is degraded (2 min; Longhorn rebuilds on its own) |
| `LonghornVolumeFaulted` | Critical | a volume is faulted (none) |

Longhorn's alerts are alert-only: Longhorn owns its volumes, and homelabd never touches them.

### 4.3 Repairs

homelabd repairs only what it owns (§2), one step at a time, and stops when a step doesn't help:

| Problem | Repair | Limits |
|---|---|---|
| K3s service stopped | start it | already done by the agent, at once |
| K3s service running but the node not Ready, or a server's API not ready | restart the K3s service on that host | 3 attempts, 10 min apart |
| A dead etcd member | remove it | already done; one at a time, only while etcd keeps a quorum |
| A removed host's node | delete the node | once the host is removed |
| A node whose `homelab.io/` labels or Longhorn disks drifted from what homelabd decided (changed by hand) | put them back | today they are applied only when homelabd's decision changes; checking for drift is new |

The safety rules, which every repair follows:

1. **Only the leader decides,** with a Raft quorum and caught up. A host never repairs on its own beyond starting a stopped service.
2. **Never a workload.** No pod (but §2's instance managers), PVC, Longhorn volume or replica, nor any Kubernetes object outside §2's list.
3. **One server at a time.** A K3s server is restarted only while the other running servers are an etcd quorum without it, and while no other server's repair is still settling.
4. **A budget per problem.** Each attempt is recorded in the Raft log (and as an event) before it is carried out. When the budget is spent, homelabd stops, and the alert says "gave up after 3 restarts at 12:41; needs you". The budget resets when the alert resolves.
5. **Every repair is visible.** Its event names the host, the action and why; the alert's message counts the attempts.

## 5. Changes

- `proto`: `KubeHealth` (server report), `Msg::KubeHealth`; `KubeStatus.restarts`; `Msg::KubeRepair` (leader to agent: restart K3s); `KubeAdmin.delete_nodes`.
- `node::linux::kube`: servers send `KubeHealth` every pass (`kubectl get --raw /readyz`, nodes, Longhorn volumes); agents read `NRestarts`; restart on `KubeRepair`; delete nodes on `delete_nodes`.
- `control`: a `supervise` controller for §4.2's alerts and §4.3's restarts. Its repair log is in the replicated state (`Command::RecordRepair`, cleared with the alert). The kube controller gains `delete_nodes` for removed hosts.
- UI: the host drawer says "Host NotReady" and "Node not Ready", shows a node cordoned outside homelabd, and lists repairs. The README gets this document's §2 table.

## 6. Success criteria (added to the lab's e2e run)

- **K10 Hung node:** stop the K3s process on an agent (SIGSTOP, through the guest agent: the service still looks active). `KubeNodeNotReady` is raised, homelabd restarts K3s, the node is Ready again, and the alert resolves, naming the restart.
- **K11 API behind the VIP:** hang the leader's K3s. `KubeApiDown` is raised, homelabd restarts it, the API answers through the VIP again, and homelabd's own API kept answering throughout.
- **K12 Giving up:** hang K3s on an agent after every restart. homelabd restarts it 3 times, then stops, and the alert says it gave up. No second server is ever restarted while one is.
- **K13 Removed host:** remove a drained host; its node is deleted.
- **K14 Hand cordon:** `kubectl cordon` a node. homelabd reports it, and neither its controllers nor Uncordon undo it.
- **K4 extended:** while the lost replica rebuilds, `LonghornVolumeDegraded` is raised, then resolves.

## 7. Phasing

1. **See and say:** `KubeHealth`, the alerts, the UI's two kinds of down, hand cordons shown. No repairs. K10–K11 checked up to the alert.
2. **Repair:** restarts with budgets and the safety rules, deleting removed hosts' nodes. K10–K14.
3. **Follow-ups** (§9).

## 8. Risks

| Risk | Mitigation |
|---|---|
| A restart makes things worse (etcd churn) | One server at a time, only with quorum to spare; a budget; giving up is loud |
| `/readyz` flaps under load | Grace periods; repair only after the alert has stood |
| Reports from servers that disagree | The leader's own server for the VIP; one chosen server for the cluster view, like etcd's members today |
| The repair log grows the Raft log | Attempts are rare and capped per problem |

## 9. Follow-ups (not in this design)

- **Move the VIP off a leader whose K3s API stays down.** That needs Raft leadership transfer, which homelabd's Raft does not have yet: the leader would hand over to a voter whose API is ready.
- **Reinstall a K3s agent** whose install keeps failing, once it is proven on the lab that Longhorn's data on `/mnt` survives it.
