# homelabd bootstraps real K3s + Longhorn

Date: 2026-10-27
Status: Approved by direction (user: the product runs on K3s; validate it for real).
Builds on: `2026-10-23-homelab-cluster-manager-design.md` §1 (intent) and §11
(production: K3s orchestration, Longhorn storage), `2026-10-26-real-vm-lab-design.md`
(the lab this validates on).

## 1. Intent

The prototype proved our control plane (join, network templates, Raft, UI).
The product it prototypes is a management layer **built on K3s + Longhorn**.
This phase makes `homelabd` bootstrap a real K3s HA cluster and Longhorn on
the fleet through our own join flow, and validates the whole setup on lab VMs.

Proven by spike (2026-10-27, on lab guests through the proxy relay):
K3s v1.36.5 installs and a node goes Ready; Longhorn v1.13.0 installs and a
volume carries data (write/read verified); a second server joins via
`--cluster-init`+token (both `control-plane,etcd`, Ready); a 2-replica volume
spreads one replica per node. Pins: **K3s v1.36.5+k3s1, Longhorn v1.13.0**.

## 2. Division of labour (this phase)

- Ours (unchanged): hosts (join/approve), network (templates, bonds, VLANs,
  switch polling/validation), control-plane membership, UI, ground truth.
- K3s (new, real): workload scheduling, service networking, node health.
- Longhorn (new, real): block storage, replica rebuild.
- Our scheduler/storage/failover controllers are Phase-E stubs (no-ops on
  real hosts), so nothing fights K3s. Workloads are created with `kubectl`
  for validation; driving K8s resources from our API/UI is a later step.
- KubeVirt is out of scope (needs `/dev/kvm` in guests; follow-up).

## 3. Role mapping

| Our host | K3s | Longhorn |
|---|---|---|
| CP voter (the 3 MS-01s) | server (embedded etcd HA) | node (prereqs by homelabd) |
| Other Ready hosts | agent | node (prereqs by homelabd) |

The K3s API URL is `https://<VIP>:6443` everywhere (every voter runs a
server, so wherever our VIP sits, an API serves; joiners retry across moves).
`--tls-san <VIP>` is set on every server. `--node-ip` pins each node's mgmt
IP so flannel and kubelet never pick a VLAN address.

## 4. Bootstrap flow

1. `homelabd bootstrap` (as today) seeds our cluster of one, and records a
   `cluster_init` marker: this host runs the first K3s server.
2. Its worker (§6) installs K3s (pinned binary, sha256-verified, proxy env
   with `NO_PROXY` covering site + pod/service CIDRs — the install script
   must NOT be used: it overwrites the env file), starts
   `k3s server --cluster-init`, waits for Ready.
3. It reads `/var/lib/rancher/k3s/server/node-token` and serves it to
   joiners through the leader (§5).
4. Other voters, once Ready members: fetch join info, install, join as
   servers (`--server https://<VIP>:6443 --token …`).
5. Non-voter Ready hosts: same, but as `k3s agent`.
6. The `cluster_init` host applies the pinned Longhorn manifest once every
   voter is a Ready K3s node, then records a `longhorn_done` marker.
   Prerequisites (`open-iscsi`, `nfs-common`, `iscsid`) are installed by
   every host's worker before K3s starts (apt through the site proxy).

`cluster-init` + token are K3s's own HA mechanism; our control plane only
distributes the token and URL to approved members. A host that loses k3s
state replays the flow (reinstall/rejoin); K3s node identity follows the
hostname, which is stable.

## 5. Control-plane changes (`control`, `model`)

Small, sim-safe additions in the style of §8 of the lab design:

1. `Msg::K3sJoinRequest` (agent → leader) / `Msg::K3sJoinInfo { url, token }`
   (leader → agent). The leader answers from its own server's node-token
   (read via `HostOs`); while its k3s is not up it stays silent and the
   agent retries (the `InProgress` pattern).
2. `HostOs::k3s_managed() -> bool`: false in the sim and `MemOs` (agents
   never ask; zero behaviour change), true in `LinuxOs`.
3. `HostOs::k3s_token() -> Option<String>`: the local server's node-token
   (file read; safe on the event loop). `None` in the sim.
4. No heartbeat or UI change in this phase: `kubectl` is the validation
   interface for K3s/Longhorn state. Surfacing it in our UI comes with the
   API→K8s integration step. No new controller logic.

## 6. Node changes (`node`)

New `node::k3s` module (pure render functions unit-tested; slow shell-outs
only on a worker thread, never on the event loop):

- `ensure_prereqs()`, `ensure_installed(version)`, `ensure_server_init()`,
  `ensure_server_join(url, token)`, `ensure_agent(url, token)`,
  `ensure_longhorn(manifest)`, `local_token()`, `nodes_ready()`.
- `homelabd run` spawns one worker thread owning a `K3sManager`. Every 15 s
  it reads desired state (a snapshot the loop updates: lifecycle, voter?,
  `cluster_init`?, cached join info) and converges. Join info arrives via
  control message into a `Mutex<Option<…>>` the worker drains. Status flows
  back the same way for heartbeats. Tests never spawn the worker.

## 7. Validation (fresh lab, `e2e/k3s.e2e.ts` + kubectl)

1. **Stack up:** 4 homelab-Ready hosts; K3s 3 servers + 1 agent Ready, 3
   etcd members; Longhorn pods Running.
2. **Join:** a 5th VM becomes homelab-Ready AND a Ready K3s node unaided.
3. **Workloads:** nginx Deployment (3 replicas, spread over nodes) +
   3-replica Longhorn PVC with data written and read back.
4. **Faults:** power off a K3s server → API stays up, pods reschedule;
   power off a volume-replica node → volume stays attached, rebuilds;
   thaw/reset → the node rejoins both clusters Ready.
5. **No interference:** our heartbeats, leader and VIP stay stable with K3s
   running (its iptables rules, CPU and RAM present).

Non-goals: KubeVirt; our API creating K8s resources; Longhorn data path on
`/mnt` data disks (default path; follow-up); air-gapped install
(proxy-based; follow-up); auth on the token exchange (transport is already
unauthenticated: known lab limitation).
