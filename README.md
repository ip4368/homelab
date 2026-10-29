# Homelab cluster manager

A management layer for a mixed homelab fleet (K3s + Longhorn style): joining hosts,
templated per-port network config checked against VLAN-capable switches, one page for
every host, and staying up when hosts, ports or switches fail.

`homelabd` is the daemon on every host: it joins the cluster, applies the network
templates, holds the control-plane VIP, and **installs and joins K3s** (servers on the
control-plane hosts, agents elsewhere), with **Longhorn** on the storage hosts' data
disks. Workloads run on Kubernetes; the manager configures and observes it.

## Running it

```sh
./run.sh                 # builds the UI, then runs the server and the VM lab; open http://localhost:8080
PORT=18080 ./run.sh      # listen on another port (8080 is often taken)
```

Requirements: Linux with KVM and sudo (the server boots the VM lab), Rust (cargo),
Node and npm. The server runs as root: the lab needs it.

- The first run does a **release** build of the server, which takes a few minutes.
- `npm ci` runs only when `ui/node_modules` is missing or older than `ui/package-lock.json`, and that
  step needs network access.
- The server listens on `BIND`:`PORT` (default `127.0.0.1:8080`) and serves the built UI from `ui/dist`.
  `BIND=0.0.0.0` makes it reachable from other machines; the API has no authentication, so prefer an
  SSH tunnel (`ssh -L 8080:localhost:8080 <host>`).

- **Setting up a cluster on your machines:** [docs/setup.md](docs/setup.md) covers what you need,
  the first machine (`install.sh init`) and every other machine (`install.sh join`).
- **The VM lab** (`./lab.sh up`) runs those same steps on QEMU VMs behind
  userspace VLAN switches. It boots plain Debian, and a "technician" runs the install one-liners
  over SSH. `node e2e/lab.e2e.ts` then checks the whole stack: joins, leader failover, reboots,
  network faults, Kubernetes nodes, and a Longhorn volume surviving its host's loss. Design and
  findings: [docs/plans/2026-10-27-k3s-longhorn-lab-design.md](docs/plans/2026-10-27-k3s-longhorn-lab-design.md).

## What the control plane does

Control-plane replicas run hand-rolled Raft (pre-vote, check-quorum, single-server
membership changes). The leader runs the controllers over the replicated state: join and
approval, network templates, staged rollouts and validation, health, control-plane
membership, K3s install and join, alerts. Host agents and switch pollers talk to the
leader only by messages.

- **Data model and network validation** live in the `model` crate; `control` depends only on it.
- **Metrics** are collected from the agents and the history lives in the leader's memory.
  It resets when the leader fails over. Production would use Prometheus.

## A tour of the UI

**Top bar**

- **Fleet health:** hosts ready, not ready and pending; the control-plane leader and quorum; open alerts.
- **Ground truth** checkbox.

If the control plane has no leader or no quorum, operations return 503 and the UI shows "reconnecting".

**Views** (tabs; the URL hash opens one directly, e.g. `#topology`)

- **Fleet:** every host as a tile, coloured by health, with CPU/RAM and role badges. You can group tiles by
  rack, switch, role or profile. Above 120 hosts the grid switches to a canvas. Click a tile to open
  the host drawer, which has:
  - inventory: NICs (ports no link uses show as grey "unused") and disks;
  - Kubernetes: what K3s runs on the node, and its pods;
  - lifecycle: Cordon, Uncordon, Remove;
  - live charts;
  - the roles editor;
  - the per-host network editor (ports → links → networks, with drift and validation errors).
- **Topology:** switches, uplinks and host connections, with ports coloured by state and utilisation, and
  VLAN/MTU mismatches highlighted. It collapses to per-switch groups at scale. Click a switch to open its port table,
  port config editor and suggested fixes.
- **Networks:** logical networks (VLAN, subnet, MTU), network templates (with staged rollouts) and
  auto-approve rules. All of them are editable.
- **Workloads:** what Kubernetes runs, as the server reads it from the K3s API: workloads, each pod
  with its host, and the Longhorn volumes with where their replicas are and whether they are in
  sync or rebuilding.

**Right rail:** alerts, the event log, and the *Pending joins* queue (approve with roles).

**Ground-truth overlay:** shows what the lab sees from outside the machines but the control plane
does not, for example a host the control plane still believes Ready while it is powered off,
partitions, or who holds the VIP. Add `?truth` to the URL to open the UI with the overlay on.

**Lab panel**

*Selected machine* (select a host first): power on, shutdown, power off, reset, freeze/thaw, partition,
unplug/replug a NIC, degrade a NIC to N Mb/s (like a bad cable), fail/restore a data disk.

*Network:* kill/restore a switch, partition a switch, partition an arbitrary host set as one group,
heal all partitions.

*Rack a machine:* add a VM of a profile through the real join flow.

*Workloads:* seed the demo workloads (`deploy/demo-workloads.yaml`) into the `demo` namespace.

## Tests

```sh
cargo test --workspace                       # all Rust tests, debug build (about a minute including the build)
cd ui && npm test                            # UI unit tests (vitest)
cd ui && npx tsc -b && npm run build         # type check and production build
node e2e/lab.e2e.ts                          # the VM lab end to end (needs ./lab.sh up first)
```

## Repository layout

| Path | What |
|---|---|
| `crates/model` | Domain model: hardware profiles, networks and templates, network validation, roles. |
| `crates/control` | The control plane: Raft, CP replica, host agent, controllers, API client. Depends only on `model`. |
| `crates/node` | `homelabd`: the host daemon (`HostOs` over real Linux, K3s/Longhorn worker, control-plane replica host). |
| `crates/lab` | The VM lab: QEMU machines, userspace VLAN switches, install technician, ground truth. |
| `crates/server` | HTTP + WebSocket server (axum). It drives the lab and serves the UI. |
| `ui/` | React + TypeScript + Vite single-page UI. `ui/src/gen` holds wire types generated from Rust by ts-rs. |
| `docs/plans/2026-10-23-homelab-cluster-manager-design.md` | The original design (goals, architecture, behaviour, success criteria, path to production). |
| `docs/plans/2026-10-23-homelab-cluster-manager.md` | The original implementation plan. |

## Known limitations

- **Metrics history resets on leader failover.** The UI shows "metrics since …".
- **No real-system concerns.** There is no authentication or multi-user support.
