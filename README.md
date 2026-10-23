# Homelab cluster manager (prototype)

A management layer for a mixed homelab fleet (K3s + Longhorn + KubeVirt style), built as a
**prototype against a fully simulated fleet**. It is for judging the control-plane behaviour and the
UI before building the real thing: joining hosts, templated per-port network config checked against
VLAN-capable switches, one page for every host and workload, and staying up when hosts, ports or
switches fail, at up to 500 hosts.

## What is real and what is simulated

Real (this is the code under evaluation):

- **Control plane.** Control-plane replicas run hand-rolled Raft (pre-vote, check-quorum, single-server
  membership changes) over a simulated network. Leader election and failover happen because replicas
  really lose contact, not because a script says so.
- **Controllers.** They run on the leader only, over the replicated state: join and approval, network
  templates, rollouts and validation, health, scheduling, VM failover, drain, storage replicas,
  control-plane membership, alerts.
- **Host agents and switch pollers.** They talk to the leader only by messages through the fabric.
- **Data model and network validation.** These are in the `model` crate, with no dependency on the simulator.
- **The UI.**

Simulated (design §11):

- **Hardware.** There are no real machines, NICs, disks, switches or switch APIs. Hosts, cabling, VLAN trunks,
  MTUs, LACP, link speeds and traffic flows are modelled in the `sim` crate.
- **K3s, KubeVirt and Longhorn.** The scheduler, VM failover and live migration, and volume replica
  placement and rebuild are throwaway stand-ins written only to simulate those systems. In
  production the manager would configure and observe the real ones (§11 of the design).
- **Metrics.** They are synthetic, and the history lives in the leader's memory (about one sim-hour).
  It resets when the leader fails over. Production would use Prometheus.
- **State.** It is in memory only. Every start loads a seeded preset, and nothing persists across restarts.

The control-plane crate never reads simulator ground truth (the compiler enforces this: `control`
depends only on `model`).

## Running it

```sh
./run.sh                 # builds the UI, then runs the server; open http://localhost:8080
PORT=18080 ./run.sh      # listen on another port (8080 is often taken)
```

Requirements: Rust (cargo), Node and npm.

- The first run does a **release** build of the server, which takes a few minutes.
- `npm ci` runs only when `ui/node_modules` is missing or older than `ui/package-lock.json`, and that
  step needs network access.
- The server listens on `PORT` (default 8080) and serves the built UI from `ui/dist`.
- The server starts with the **default** preset at 1×.

### Presets

| Preset | Hosts | Control-plane voters |
|---|---|---|
| `default` | 60: 12 MS-01, 24 mini PC, 12 Pi 5, 12 RK3588 | 3 (growing to 5 automatically) |
| `scale500` | 500: 40 MS-01, 200 mini PC, 120 Pi 5, 140 RK3588 | 5 |

Topology:

- `core-1` and `core-2` are an MLAG pair of 10G SFP+ switches.
- Access switches `access-1..N` are 48 × 2.5G switches with an uplink to each core.
- MS-01s bond two SFP+ ports, one to each core.
- The router and the UI's client attach to `core-1`.

Load a preset from the fault panel. Doing so replaces the whole fleet.

## A tour of the UI

**Top bar**

- **Fleet health:** hosts ready, not ready and pending; the control-plane leader and quorum; degraded volumes; open alerts.
- **Sim clock.**
- **Speed:** Pause, 1×, 5×, 20× or 60×.
- **Ground truth** checkbox.

While the sim is paused:

- Faults still work.
- Operations and scenarios return 409, and their buttons are disabled.
- If the control plane has no leader or no quorum, operations return 503 and the UI shows "reconnecting".

**Views** (tabs; the URL hash opens one directly, e.g. `#topology`)

- **Fleet:** every host as a tile, coloured by health, with CPU/RAM and role badges. You can group tiles by
  rack, switch, role or profile. Above 120 hosts the grid switches to a canvas. Click a tile to open
  the host drawer, which has:
  - inventory: NICs (ports no link uses show as grey "unused") and disks;
  - lifecycle: Cordon, Drain, Uncordon, Cancel drain, Remove (asks for confirmation and names what runs there);
  - live charts;
  - the workloads on the host;
  - the roles editor;
  - the per-host network editor (ports → links → networks, with drift and validation errors).
- **Topology:** switches, uplinks and host connections, with ports coloured by state and utilisation, and
  VLAN/MTU mismatches highlighted. It collapses to per-switch groups at scale. Click a switch to open its port table,
  port config editor and suggested fixes.
- **Networks:** logical networks (VLAN, subnet, MTU), network templates (with staged rollouts) and
  auto-approve rules. All of them are editable.
- **Workloads:** VMs, pods and volumes, with filters, where each runs, and replica placement. There are create
  forms for VMs, deployments and volumes. The workload drawer can live-migrate a VM and shows the volume replica map.

**Right rail:** alerts, the event log, and the *Pending joins* queue (approve with roles).

**Ground-truth overlay:** shows what the simulator knows but the control plane does not, for example a host
the control plane still believes Ready while it is powered off, partitions, or a VM running twice
(split brain). Add `?truth` to the URL to open the UI with the overlay on.

**Faults & scenarios panel**

*Selected host* (select a host first):

| Control | Effect |
|---|---|
| Kill host / Restore host | Powers the host off or on. Its disks keep their data. |
| Partition host | Cuts the host off from everything else (one partition group). |
| Unplug port / Replug port | Pulls or reconnects the cable on the chosen NIC. |
| Fail disk | Fails the chosen disk. Replicas on it are lost. |
| Degrade port | Makes the chosen NIC negotiate at most N Mb/s, like a bad cable. Set it to full speed to undo. |

*Network:*

| Control | Effect |
|---|---|
| Switch id | Pick or type a switch id, e.g. `access-2` or `core-1`. |
| Kill switch / Restore switch | Powers the switch off or on. |
| Partition switch | Puts the switch in its own partition group. |
| Move client | Re-cables the UI client's attachment point to a free switch port. |
| Host set | Lists hosts, separated by commas or spaces. "Add selected" appends the selected host. Tick "with the switch" to put the switch id in the same group, e.g. access-2 plus all its hosts. |
| Partition group | Partitions the host set (and the switch, if ticked) as one group. |
| Heal partitions | Removes every partition. |

*Iperf:* runs N parallel flows of M Mb/s from a host to another host or the router, over a chosen network, for S seconds.

*Scenarios:*

| Control | Effect |
|---|---|
| Add hosts | Racks N hosts of a profile through the real join flow (token, discovery, auto-approve rule, template). The misconfiguration select makes the technician's mistake on their switch ports: VLAN 20 missing, MTU too small, or a bond without LACP. The control plane detects it at join. |
| Seed workloads | Creates a realistic mix of VMs, deployments and volumes, scaled to the fleet. |
| Preset: default / scale500 | Reloads the fleet. |

## Tests

```sh
cargo test --workspace                       # all Rust tests, debug build (about a minute including the build)
cargo test -p sim --release --test scale     # the 500-host tests with their wall-clock budgets checked
cd ui && npm test                            # UI unit tests (vitest)
cd ui && npx tsc -b && npm run build         # type check and production build
```

- The scale tests (500 hosts) also run in the default debug suite, in about 12 s.
- Only a release build checks their wall-clock budgets.
- The integration tests in `crates/sim/tests` drive the full actor system headless and cover the
  design's success criteria 3–6 (leader failover, host failure, bond/VLAN faults, partitions with split
  brain) and the rebuild and storage edge cases.

## Repository layout

| Path | What |
|---|---|
| `crates/model` | Domain model: hardware profiles, networks and templates, network validation, roles, placement. No simulator dependency. |
| `crates/control` | The control plane: Raft, CP replica, host agent, controllers, API client. Depends only on `model`. |
| `crates/sim` | The simulator: discrete-event engine on sim-time, network fabric and topology, traffic model, hardware world, faults, presets, scenarios, ground truth. |
| `crates/server` | HTTP + WebSocket server (axum). It drives the engine on its own thread and serves the UI. |
| `ui/` | React + TypeScript + Vite single-page UI. `ui/src/gen` holds wire types generated from Rust by ts-rs. |
| `docs/plans/2026-10-23-homelab-cluster-manager-design.md` | The design (goals, architecture, behaviour, success criteria, path to production). |
| `docs/plans/2026-10-23-homelab-cluster-manager.md` | The implementation plan. |

## Known limitations

- **A removed host is gone for the session.** Its agent keeps running orphaned and cannot rejoin until the preset is reloaded.
- **Stale replicas hold no modelled data.** Replica contents are not modelled, so a salvaged stale replica is not "older" data.
- **Metrics history resets on leader failover.** The UI shows "metrics since …".
- **A surplus replica waits for a stable volume.** After a drain is cancelled mid-way, the volume's surplus replica is trimmed only once every replica is
  healthy on a Ready host.
- **No real-system concerns.** There is no authentication, multi-user support or persistence (non-goals for the prototype).
