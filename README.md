# Homelab cluster manager (prototype)

A management layer for a mixed homelab fleet running K3s and Longhorn: joining hosts, templated
per-port network config checked against VLAN-capable switches, one page for every host and
workload, and staying up when hosts, ports or switches fail.

`homelabd` is the daemon on every host. It joins the cluster, applies the network templates, holds
the control-plane VIP, and **installs and joins K3s** (servers on the control-plane hosts, agents
elsewhere), with **Longhorn** on the storage hosts' data disks. **Kubernetes runs the workloads**:
homelabd places none of its own. Cordoning and draining a host cordon and drain its Kubernetes node.

- **Setting up a cluster on your machines:** [docs/setup.md](docs/setup.md) covers what you need,
  the first machine (`install.sh init`) and every other machine (`install.sh join`).
- **The VM lab** (`./lab.sh up`, Linux with KVM and sudo) runs those same steps on QEMU VMs behind
  userspace VLAN switches. It boots plain Debian, and a "technician" runs the install one-liners
  over SSH. `node e2e/lab.e2e.ts` then checks the whole stack: joins, leader failover, reboots,
  network faults, Kubernetes nodes, a Longhorn volume surviving its host's loss, and draining a
  host. Design and findings:
  [docs/plans/2026-10-26-real-vm-lab-design.md](docs/plans/2026-10-26-real-vm-lab-design.md) and
  [docs/plans/2026-10-27-k3s-longhorn-lab-design.md](docs/plans/2026-10-27-k3s-longhorn-lab-design.md).

There is no simulator any more: every host, NIC, disk, switch port and workload is a real VM, a
real userspace switch or a real Kubernetes object. A lab from before the simulator's workloads were
removed holds Raft log entries this version cannot read: `./lab.sh reset`, then `./lab.sh up`.

## Who owns what

homelabd keeps its own Raft control plane beside K3s on purpose: it does not depend on K3s, so it
can still see, report and repair K3s when K3s breaks. The rule: **homelabd owns the machines and
everything K3s stands on, and supervises K3s; K3s owns everything that runs on it.**

| homelabd | K3s and Longhorn |
|---|---|
| Hosts: join, approval, roles, lifecycle, removal | Pods, scheduling, restarts, eviction timing |
| Host networking and switches | PVCs, Longhorn volumes and replicas |
| The VIP; which hosts are K3s servers; etcd membership | |
| Installing K3s and Longhorn; `homelab.io/` node labels; which disks Longhorn may use | |
| Starting cordon and drain | Evicting the pods |
| Host metrics; K3s's health: alerts and safe repairs | |

The full split, the two kinds of "down" (host NotReady, node not Ready) and how homelabd supervises
K3s: [docs/plans/2026-10-29-homelabd-k3s-responsibilities-design.md](docs/plans/2026-10-29-homelabd-k3s-responsibilities-design.md).

## What runs where

- **homelabd's control plane.** Control-plane replicas on the voters run hand-rolled Raft
  (pre-vote, check-quorum, single-server membership changes, leadership transfer) over the
  management network, with their state on disk. The leader holds the VIP, answers the API and runs the controllers: join
  and approval, network templates, rollouts and validation, switch observation, health, alerts,
  control-plane membership, and what each host runs of Kubernetes.
- **Host agents.** Apply network configs like `netplan try` (reverted unless confirmed), report
  LLDP, inventory and metrics read from `/sys` and `/proc`, and install and run K3s.
- **Kubernetes.** K3s schedules and restarts the workloads; Longhorn places, replicates and
  rebuilds their volumes. A running K3s server does the cluster-wide work the leader asks of it
  with its admin access: removing dead etcd members, keeping Longhorn off the disks of hosts that
  do not store, and cordoning and draining nodes.
- **Metrics.** Real CPU, RAM, disk, port rates and (where the machine has a sensor) temperature.
  The history lives in the leader's memory, about an hour, and resets when the leader fails over.

## Running it

```sh
./lab.sh up              # builds the UI, homelabd and the server, then runs the server and the lab as root
PORT=18080 ./lab.sh up   # listen on another port (8080 is often taken)
./lab.sh seed            # kubectl apply deploy/demo-workloads.yaml
./lab.sh kubectl get pods -A -o wide
./lab.sh reset           # power off the VMs and delete their disks; the next up installs a fresh cluster
```

The server listens on `BIND`:`PORT` (default `127.0.0.1:8080`) and serves the built UI from
`ui/dist`. The API has no authentication and can power off the lab's VMs, so prefer an SSH tunnel
(`ssh -L 8080:localhost:8080 <host>`) to `BIND=0.0.0.0`.

## A tour of the UI

**Top bar:** hosts ready, not ready and pending; the control-plane leader and quorum; open alerts;
the **Ground truth** checkbox; the lab host's clock.

**Views** (tabs; the URL hash opens one directly, e.g. `#topology`)

- **Fleet:** every host as a tile, coloured by health, with CPU/RAM and role badges, grouped by
  rack, switch, role or profile. Click a tile to open the host drawer: inventory, what it runs of
  Kubernetes (and its pods), control-plane membership (hand-off, replacing a down voter, moving the
  VIP), lifecycle (Cordon, Drain, Uncordon, Cancel drain, Remove), live charts, the roles editor and
  the per-host network editor.
- **Topology:** switches, uplinks and host connections, with ports coloured by state and
  utilisation, and VLAN/MTU mismatches highlighted. Click a switch for its port table, port config
  editor and suggested fixes.
- **Networks:** logical networks (VLAN, subnet, MTU), network templates (with staged rollouts) and
  auto-approve rules.
- **Workloads:** what Kubernetes runs, read from the K3s API: workloads, their pods and where they
  run, and Longhorn's volumes with their replicas.

**Right rail:** alerts (with a button for the fix one suggests, such as replacing a dead voter), the event log, and the *Pending joins* queue (approve with roles).

**Ground-truth overlay:** what the lab sees from outside the machines but the control plane does
not, for example a host the control plane still believes Ready while it is powered off, partitions,
or the VIP held by two machines. Add `?truth` to the URL to open the UI with the overlay on.

**Lab panel:** power a machine on or off, shut it down, reset, freeze or thaw it; partition it;
unplug, replug or degrade a NIC; pull or restore a data disk; kill, restore or partition a switch;
rack a new machine (optionally with a switch-port mistake: VLAN 20 missing, MTU too small, a bond
without LACP); seed the demo workloads.

## Tests

```sh
cargo test --workspace                       # all Rust tests
cd ui && npm test                            # UI unit tests (vitest)
cd ui && npx tsc -b && npm run build         # type check and production build
node e2e/lab.e2e.ts [--only K4,K9]           # end to end, against a running lab
```

## Repository layout

| Path | What |
|---|---|
| `crates/model` | Domain model: hardware profiles, networks and templates, network validation, roles. |
| `crates/control` | The control plane: Raft, CP replica, host agent, controllers, API client. Depends only on `model`. |
| `crates/node` | `homelabd`: the host runtime, Linux (`/sys`, systemd-networkd, LLDP, Raft state on disk) and K3s/Longhorn. |
| `crates/lab` | The VM lab: QEMU machines, userspace VLAN switches, the router and the technician. |
| `crates/server` | HTTP + WebSocket server (axum): the UI's client to the control plane, the lab, the Kubernetes view, and the built UI. |
| `ui/` | React + TypeScript + Vite single-page UI. `ui/src/gen` holds wire types generated from Rust by ts-rs. |
| `e2e/` | The end-to-end run against the lab. |
| `deploy/demo-workloads.yaml` | The demo workloads `./lab.sh seed` applies. |

## Known limitations

- **homelabd does not supervise K3s yet.** It shows each host's K3s state, but raises no alert
  when K3s breaks and does not restart a hung K3s (the design above).
- **Metrics history resets on leader failover.** The UI shows "metrics since …".
- **Removing a host does not delete its Kubernetes node.** Drain it first.
- **No real-system concerns.** There is no authentication or multi-user support.
