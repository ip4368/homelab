# Homelab Cluster Manager — Prototype Design

Date: 2026-10-23
Status: Draft, awaiting approval

## 1. Intent

A management layer for a homelab fleet built on **K3s** (orchestration), **Longhorn** (distributed block storage) and **KubeVirt** (VMs). It should make it easy to:

- join new machines,
- configure their (often multiple) network ports for traffic distribution, matched against VLAN-capable SFP+ switches,
- see and manage every host and its workloads on one page,
- stay up when hosts, ports or switches fail,
- scale to 100–500 machines, mixing x86 (e.g. Minisforum MS-01 with 4 NICs) and ARM SBCs used for special jobs like hardware video transcoding,
- monitor the whole thing.

**This deliverable is a prototype running against a fully simulated fleet.** No real hardware, no real K3s. The goal is to evaluate the control-plane behaviour and the UI before deciding whether to build the real system. If we do go ahead, this document and the prototype are the starting point (see §11, Path to production).

Prior art: **Harvester** (SUSE) combines RKE2, Longhorn and KubeVirt, but it is heavy, built for x86, and has no story for ARM SBCs or mixed-role fleets. This project's niche is a lightweight, mixed-hardware, role-aware homelab fleet.

## 2. Goals, non-goals, anti-goals

**Goals**
1. **Easy join:** generate a token, the host appears as *pending* with its hardware discovered, then approve it (or auto-approve by rule). A network template and roles are applied automatically.
2. **Per-host network config:** cluster-wide logical networks (VLAN, subnet, MTU). Per host, physical ports are grouped into links (bond or single port), and networks are assigned to links, tagged or untagged. Templates are keyed by hardware model.
3. **Switch simulation:** SFP+/access switches, cabling between host ports and switch ports, VLAN trunk/access config per switch port, inter-switch uplinks. Mismatches are detected.
4. **Single-page UI:** visualise and manage all hosts, switches and workloads, with fault injection built in.
5. **Real resilience behaviour, not scripted:** control-plane replicas elect a leader over the simulated network. Host, port and switch failures and partitions produce emergent outcomes: VM failover, Longhorn replica rebuild, leader failover, control-plane member replacement.
6. **Scale:** 500 simulated hosts with the UI staying responsive.
7. **Monitoring:** live per-host and fleet metrics, per-port throughput, alerts and an event log.

**Non-goals (for the prototype)**
- Talking to real hardware, real K3s, Longhorn, KubeVirt or switch APIs.
- Authentication, multi-user support, RBAC.
- Persistence across process restarts. In-memory only; every run starts from a seeded scenario.
- Long-term metrics retention. Recent history only, about 1 simulated hour per series.
- Production hardening, packaging and installers.

**Anti-goals (would make this a failure even if it "works")**
- **Staged resilience:** if failover happens because the code says "leader died, set leader = X" rather than because replicas actually lost contact and re-elected, the prototype has failed its purpose.
- **A dashboard that falls over at 500 hosts:** if the UI only works at 20 hosts, it doesn't answer the scaling question.
- **Per-host hand configuration:** if setting up networking needs clicking through 500 hosts, the design is wrong. Templates and rules must carry the fleet.
- **Dedicated management machines:** the user does not want machines that only run the control plane. Management runs on ordinary hosts alongside workloads.

## 3. Constraints

- **Backend:** Rust (cargo 1.99 is installed). It contains the simulator and the control plane.
- **Frontend:** TypeScript web UI (Node is installed). It talks to the backend over HTTP plus a WebSocket for live updates.
- **Runs on the user's Mac:** one command to start, no Docker required.
- **Prototype quality:** speed of iteration beats polish, but the boundaries in §5 must hold, because they are what make the resilience real.

## 4. Approaches considered

| # | Approach | Trade-off |
|---|---|---|
| 1 | One process, shared state. The simulator and control plane mutate the same in-memory model. | Fastest to build. Resilience is staged, which hits the anti-goal. |
| **2** | **One process, actor-based.** Every host agent, switch and control-plane replica is an independent async task that communicates only by messages over a simulated network fabric. Replicas run a real consensus/leader election over that fabric. | ~1.5× the effort of #1. Failures are emergent, one command to run, 500 hosts is easy. **Chosen.** |
| 3 | Multiple real processes (Docker Compose): control-plane replicas, simulator and UI as separate programs. | Most realistic, most effort, heavy on a Mac. Proves things a prototype doesn't need. |

**Chosen: #2.** Resilience is one of the six goals and the only one a staged simulation can't evaluate. #2 makes it real without #3's operational overhead.

## 5. Architecture

```
┌───────────────────────────── single Rust process ─────────────────────────────┐
│                                                                               │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                       │
│  │ CP replica A │   │ CP replica B │   │ CP replica C │  ← run on 3 (or 5)    │
│  │ (on host 1)  │   │ (on host 7)  │   │ (on host 12) │    ordinary hosts     │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                       │
│         │  consensus + controllers (leader only)                               │
│  ═══════╪══════════════════╪══════════════════╪═══════  simulated fabric      │
│         │      (ports → cables → switch ports → VLANs → uplinks)               │
│  ┌──────┴──┐ ┌─────────┐ ┌─────────┐      ┌─────────┐   ┌──────────┐         │
│  │ host    │ │ host    │ │ host    │ ...  │ host    │   │ switches │         │
│  │ agent 1 │ │ agent 2 │ │ agent 3 │      │ agent N │   │ (actors) │         │
│  └─────────┘ └─────────┘ └─────────┘      └─────────┘   └──────────┘         │
│                                                                               │
│  ┌──────────────────┐   ┌──────────────────────┐   ┌──────────────────┐      │
│  │ Sim engine       │   │ API gateway ("VIP")  │──▶│ HTTP + WebSocket │──▶ UI │
│  │ clock, faults,   │   │ routes to the current │   └──────────────────┘      │
│  │ traffic model    │   │ leader over the fabric│                             │
│  └──────────────────┘   └──────────────────────┘                             │
└───────────────────────────────────────────────────────────────────────────────┘
```

### 5.1 Hard boundaries (these make resilience real)

1. **Actors share no mutable state.** Host agents, switches and CP replicas communicate only via messages sent through the fabric.
2. **The fabric decides delivery.** A message from A to B is delivered only if a path exists: link up at both ends, the VLAN allowed on every switch port along the way, and the switches up. Delivery adds latency (configurable, small jitter) and can be dropped by injected faults.
3. **CP replicas hold the authoritative cluster state** in a replicated log. Only the leader runs controllers and accepts writes. A replica that loses quorum stops acting.
4. **The UI and API go through a gateway that models a virtual IP** (like kube-vip). The gateway locates the current leader *by asking over the fabric*, not by peeking at replica state. During an election the UI shows "reconnecting / control plane unavailable".
   - *[Builder call, settled]* **The VIP follows the leader's host**, as kube-vip does. It moves when a new leader is elected, and host heartbeats go to the VIP over `mgmt`. **The UI client attaches at a configurable point on the fabric,** defaulting to a port on core switch 1, as if the user's laptop were plugged in there. As a result, which side of a partition the UI sees is decided by the topology, not hard-coded. The client attachment point can itself be partitioned or moved from the fault panel.
5. **The simulator engine may peek at everything,** but only for two things: (a) fault injection, which changes the fabric or kills actors, and (b) "ground truth" overlays in the UI, clearly labelled as such, for example "this VM is actually running on two hosts right now". Control logic never reads ground truth.

### 5.2 Simulation clock

- Simulated time with adjustable speed (pause, 1×, 5×, 20×, 60×). All timers (heartbeats, election timeouts, grace periods, metrics intervals) are in sim-time.
- Seeded randomness for workload noise and jitter. Full determinism under tokio isn't required; best effort is fine.

### 5.3 Consensus

- CP replicas run Raft over the fabric. **Builder's call:** use `openraft` or a compact hand-rolled Raft, as long as it actually elects over the fabric and respects quorum.
- Replica count: 3 by default, 5 when the fleet exceeds a configurable size (default 50 hosts).
- **Stand-in note:** in production this would be K3s's embedded etcd plus the Kubernetes API. See §11.

## 6. Domain model

The model is declarative, Kubernetes style: every object has a **spec** (desired state, written by the user or controllers) and a **status** (observed state, reported by agents). Controllers reconcile the two. This keeps the production path to CRDs and operators straightforward.

### 6.1 Hardware profiles (seed catalogue)

| Profile | Arch | CPU / RAM | NICs | Disks | Notes |
|---|---|---|---|---|---|
| MS-01 | x86_64 | 14C/20T, 64–96 GB | 2× 10G SFP+, 2× 2.5G RJ45 | 1–3 NVMe | Intel QuickSync |
| Generic mini PC (e.g. N100) | x86_64 | 4C, 16 GB | 1× 2.5G | 1 NVMe | QuickSync |
| Raspberry Pi 5 | arm64 | 4C, 8 GB | 1× 1G | SD card or NVMe HAT | weak HW encode |
| RK3588 SBC (e.g. Orange Pi 5) | arm64 | 8C, 16 GB | 1–2× 2.5G | eMMC/NVMe | VPU HW transcode |

Each profile carries *capabilities* (for example `hw-transcode:quicksync`, `hw-transcode:rkmpp`) and storage *suitability*. SD/eMMC-only hosts are never eligible for the storage role.

### 6.2 Host

- Identity, profile, arch, discovered inventory (CPU, RAM, disks, NICs with speed/MAC), location tag (rack/shelf).
- **Lifecycle:** `pending → approved → provisioning → ready ⇄ not-ready → (cordoned / draining) → removed`.
- **Roles (multi-valued):** `control-plane`, `storage` (Longhorn replicas), `compute` (pods), `vm` (KubeVirt), `transcode`. Eligibility is enforced by profile: no `storage` on SD cards, no `control-plane` on hosts below a minimum spec (configurable, default 8 GB RAM, x86 or RK3588).
- Labels and taints for scheduling.

### 6.3 Networking

- **Logical networks (cluster-wide):** name, purpose (`management | storage | migration | vm`), VLAN ID (or untagged), subnet/IPAM range, MTU. Seed set: `mgmt` (untagged, MTU 1500), `storage` (VLAN 20, MTU 9000), `migration` (VLAN 30, MTU 9000), `vm-lan` (VLAN 100), `vm-iot` (VLAN 110).
- **Host links:** a link is either a single port or a bond (`lacp` / `active-backup`) of ports, with an MTU.
- **Attachments:** each network maps to a link, tagged or untagged, with an IP from IPAM.
- **Network templates:** keyed by profile (and optionally by role). Example "MS-01 standard": bond0 = 2×10G LACP carrying storage, migration and VM VLANs tagged; 2.5G port 1 = mgmt untagged; 2.5G port 2 unused. Templates apply on join. Per-host overrides are allowed and shown as drift from the template.
- **Validation before apply:** every role-required network present (storage role needs `storage`, vm role needs at least one `vm-*` and `migration`), MTU consistent end to end (host link ↔ switch port ↔ uplinks), VLAN allowed on the connected switch port, LACP bond members land on the same switch (or an MLAG pair) with an LACP group configured, no IP conflicts.
- **Rollout:** the network config is pushed to the agent, which applies it. If the agent loses management connectivity afterwards, it **auto-reverts after a timeout** (like `netplan try`) and reports failure.
- *[Builder call, settled]* **Editing a template or logical network rolls out to existing hosts only after the user confirms.** The UI first shows a preview: affected host count, validation results, and the switch-side changes needed. The rollout then runs in batches, defaulting to 10% of affected hosts per batch, with a configurable soak time between batches. It **pauses automatically** if any host in a batch fails or auto-reverts. Hosts with per-host overrides are listed separately and skipped unless the user explicitly includes them.

### 6.4 Switches

- Model, port list (speed, SFP+/RJ45), management IP.
- **Per-port config:** mode `access` (one VLAN) or `trunk` (native VLAN plus allowed list), MTU, LACP group membership, admin up/down.
- **Cabling:** an explicit map from host port to switch port, and from switch uplink to switch uplink.
- **Seed topology:** 2 core SFP+ switches (optionally an MLAG pair) with access switches hanging off them. Large fleets add more access switches.
- **Switch config is editable in the UI.** The system flags host↔switch mismatches. It does **not** auto-push switch config in the prototype; it shows the suggested switch-side change. **Builder's call:** whether to offer a one-click "apply suggested change to switch".
- *[Builder call, settled]* **Cabling and switch config are observed, not declared.** Agents report LLDP-style neighbour information (my port → switch X port Y). The control plane polls switch port config over the fabric. The simulator's cabling map is ground truth that the control plane never reads directly.
- *[Builder call, settled]* **Simulated joins play the human technician.** "Add N hosts of profile X" cables each host to free switch ports and configures those ports to match the profile's template, which is what a technician would do before plugging in. A separate scenario, "add N hosts with a switch misconfigured" (a missing VLAN, wrong MTU, or bond members on different switches without MLAG), exercises detection.
- *[Builder call, settled]* **A switch-side mismatch does not block `ready` as long as `mgmt` works.** The host becomes `ready`, an alert is raised, and **any role whose required network is broken is held inactive** until the mismatch is fixed. For example, the storage role gets no new replicas while VLAN 20 is missing. A host without working `mgmt` stays in `provisioning`, and its network config auto-reverts.

### 6.5 Workloads

- **Pods/deployments (K3s-like):** CPU/RAM requests, replica count, node selector/affinity on roles, labels and capabilities. Example: a Jellyfin transcode worker requiring `hw-transcode:*`.
- **VMs (KubeVirt-like):** vCPU, RAM, a root disk backed by a Longhorn volume, attached networks (`vm-*`), run strategy. Supports live migration, which generates traffic on the `migration` network.
- **Volumes (Longhorn-like):** size, replica count (default 3), replicas spread across distinct hosts with the `storage` role (and distinct switches when possible). States: `healthy | degraded | rebuilding | faulted`. Rebuilds generate traffic on the `storage` network.
- **Simulated resource usage:** each workload has a usage curve (baseline plus noise plus occasional bursts) that drives host metrics.

## 7. Control-plane behaviour (controllers, leader only)

| Controller | Behaviour |
|---|---|
| **Join** | Validates the token. Creates a `pending` host from the discovered inventory. Applies auto-approve rules (e.g. "RK3588 → roles [transcode, compute], template 'SBC transcode'"). On approval: assigns roles, applies the template, assigns IPs, moves to `provisioning`, then `ready` once the agent confirms. |
| **Health** | Hosts heartbeat over `mgmt` to the leader (via the gateway/VIP). Missed heartbeats beyond 40 sim-s mark the host `not-ready`. |
| **Scheduler** | Places pods and VMs by resource fit, roles, capabilities, affinity and spread. Never places on cordoned, not-ready or ineligible hosts. |
| **VM failover** | VM on a host that has been `not-ready` for more than 60 sim-s (configurable) is restarted on another eligible host. When the old host reconnects, its stale instances are stopped. **Split-brain is not hidden:** if ground truth shows a VM running twice during a partition, the UI flags it. This is a known real-world hazard (no fencing) and the prototype should make it visible. |
| **Drain** | Cordon, live-migrate VMs, evict pods, wait for volumes to have healthy replicas elsewhere, then mark drained. |
| **Storage** | Keeps every volume at its replica count. Rebuilds a lost replica onto an eligible host. Rebuild speed is bounded by available `storage`-network bandwidth on both ends. |
| **Network** | Renders effective config per host (template plus overrides), validates, pushes, tracks applied/failed/reverted, and reports drift. |
| **CP membership** | If a CP replica's host is gone longer than a threshold (default 10 sim-min), proposes or performs (configurable: `suggest` / `auto`) promotion of an eligible host to replace it, keeping 3 or 5 members. Never drops below quorum by its own action. Also grows from 3 to 5 replicas when fleet size crosses the threshold. |
| **Alerts** | Host down, port down, bond degraded, link speed lower than expected, disk usage > 85%, volume degraded/faulted, VLAN/MTU mismatch, CP quorum at risk, VM split-brain (ground-truth alert, labelled as such). |

## 8. Traffic model

- Traffic is modelled as **flows**, not packets. Each flow has a source, destination, network (VLAN) and demand in Mbit/s. Flow sources: Longhorn replica writes and rebuilds (`storage`), VM live migrations (`migration`), VM/pod client traffic (`vm-*`), transcode streams, heartbeats/metrics (`mgmt`, negligible).
- Each tick, flows are routed over the topology. **Bonds:** LACP hashes per flow across active members; active-backup uses one member and fails over. Links have capacity, contended links share bandwidth fairly, and per-port throughput and utilisation are recorded.
- Unplugging one bond member shifts its flows to the remaining members and raises a "bond degraded" alert. Unplugging a single-port link takes down every network on it.

## 9. UI (single page)

**Layout**
- **Top bar:** fleet health summary (hosts ready / not-ready / pending, CP leader and quorum, volumes degraded, active alerts), sim clock and speed control.
- **Main area, switchable views:**
  - **Fleet grid:** every host as a compact tile coloured by health and showing CPU/RAM/role badges. Group by rack, switch, role or profile. Must render 500 tiles smoothly (canvas or virtualised).
  - **Topology:** switches, uplinks and host connections, with ports coloured by utilisation/state and VLAN/MTU mismatches highlighted. Collapses to per-switch groups at scale.
  - **Workloads:** VMs, pods and volumes with filters. Each row shows where it runs and its replica placement.
- **Side drawer (selection detail):**
  - *Host:* inventory, roles, lifecycle actions (approve, cordon, drain, remove), network editor (ports → links → networks, with drift from template and validation errors), workloads on the host, live charts.
  - *Switch:* port table, port config editor, suggested fixes.
  - *Workload:* placement, migrate/restart, volume replica map.
- **Right rail:** alerts and event log (filterable), *Pending joins* queue.
- **Fault & scenario panel:** kill/restore host, unplug/replug port, fail disk, kill switch, partition (by switch or by host set), misconfigure VLAN on a switch port, add N hosts of profile X (simulated join), load presets (default fleet ~60 hosts, scale fleet 500 hosts).
- **Ground-truth overlay toggle:** shows what's *really* happening versus what the control plane believes. This is useful for spotting stale state and split-brain.

*[Builder call, settled]* **Editable from the UI:** logical networks, network templates, auto-approve rules, host roles and labels, switch port config, and per-host network overrides. Workloads can be created with simple forms: VM (size, networks, disk size), deployment (image name, replicas, requests, role/capability selector) and volume. A **"seed workloads"** scenario button populates a realistic mix scaled to the fleet size, so the user doesn't have to create hundreds by hand.

**Live updates:** the backend pushes batched deltas over WebSocket, coalesced to about 2–4 updates per second regardless of sim speed. Metrics charts pull series on demand for the selected entity. Fleet-wide tiles get summary metrics only.

**Builder's call:** framework (React + Vite is the sensible default), component and chart libraries, visual design.

## 10. Monitoring

- Host agents report metrics every 10 sim-s: CPU, RAM, disk usage per disk, per-port rx/tx, temperature (synthetic), workload counts.
- The leader stores ring buffers (about 1 sim-hour per series) plus fleet rollups (totals, per-role, per-switch).
- Alerts (from §7) carry severity, raised/resolved time and the entity. The event log records every state change and user action.

## 11. Path to production (if we build it for real)

| Prototype piece | Real-world counterpart |
|---|---|
| Raft among CP replicas | K3s embedded etcd (HA, 3–5 servers). The manager runs as a Kubernetes operator with CRDs (`Host`, `NetworkTemplate`, `LogicalNetwork`, `Switch`, …) and uses K8s leader election. |
| Spec/status objects + controllers | CRDs + reconcilers (kube-rs) |
| Host agent actor | A small Rust daemon per host: hardware discovery, K3s join (token), network apply via systemd-networkd/NetworkManager/nmstate with auto-revert, metrics export. |
| Join token / pending queue | K3s node token plus our own approval CRD, possibly PXE/iPXE or cloud-init images per profile. |
| Scheduler / VM failover / storage | Native K3s scheduler, KubeVirt (with real fencing / node health checks), Longhorn. The manager configures and observes rather than reimplementing them. Note that the prototype re-implements these only to simulate them. |
| Logical networks on VMs | Multus + bridge CNI / NetworkAttachmentDefinitions per VLAN. Longhorn storage network setting. |
| Switches | Vendor APIs, SSH/NETCONF or SNMP for read. Config push is optional and gated. |
| Gateway/VIP | kube-vip or keepalived on the CP hosts |
| Metrics/alerts | Prometheus (or VictoriaMetrics for 500 nodes) + Alertmanager. The UI reads from them. |

The prototype's job is to validate the **data model, controller behaviour, network template/validation logic and UI**. Those carry over. The consensus, scheduler and storage simulations are throwaway stand-ins.

*[Builder call, settled]* **"Carry over" means the design carries over, not necessarily the code.** The user is fine with a prototype. Two cheap exceptions that make later reuse easier without slowing the prototype:
- The **domain model and network validation** go in their own crate, with no dependency on the simulator.
- Objects keep a spec/status split plus a version counter.

Controllers can be plain reconcile functions over the replicated state. They do **not** need a kube-rs-shaped client/watch abstraction.

## 12. Success criteria (how we'll judge the prototype)

1. One command starts everything. The UI opens on `localhost` with the default ~60-host fleet.
2. **Join:** adding 20 RK3588 hosts via the scenario panel results in auto-approval, the transcode template applied, and all 20 `ready` with no manual steps.
3. **Leader failover:** killing the leader's host leads to a new leader elected over the fabric within about 2 election timeouts. The UI shows a brief "reconnecting" and then recovers. No controller acts during the gap.
4. **Host failure:** killing a VM host causes its VMs to restart elsewhere after the grace period and its volume replicas to rebuild onto other storage hosts, with rebuild traffic visible on the storage network.
5. **Network:** unplugging one 10G member of an MS-01 bond shifts traffic to the remaining member and raises a "bond degraded" alert. Removing VLAN 20 from a switch port raises a mismatch alert, and the host's storage traffic stops.
6. **Partition:** partitioning one access switch causes the minority side to lose contact. The control plane reacts per §7, and any VM split-brain is flagged in the ground-truth overlay.
7. **Scale:** the 500-host preset runs at 1× with the UI responsive (grid and topology interactive, no multi-second stalls).
8. Automated tests cover the scheduler, network validation and the scenarios in criteria 3–6, run headless against the simulator.

## 13. Phasing (agreed with the user)

Build in this order. Each phase ends with something runnable and demoable.

1. **Resilience core:** sim engine and clock, fabric (management network only is fine here), host agents, CP replicas with Raft leader election, gateway/VIP, join flow, fleet grid, fault panel (kill host, partition), basic alerts and event log. *Demo: kill the leader's host and watch re-election.*
2. **Networking:** logical networks, links/bonds, templates, switches, cabling/LLDP observation, validation, rollout with auto-revert, topology view, traffic/flow model, network editor. *Demo: unplug a bond member; remove a VLAN from a switch port.*
3. **Workloads:** scheduler, pods/VMs/volumes, VM failover, storage rebuild, drain and live migration, ground-truth overlay and split-brain flagging. *Demo: success criteria 4 and 6.*
4. **Scale and monitoring polish:** 500-host preset, UI performance work, CP growth from 3 to 5 replicas, metrics charts and rollups, alert refinement. *Demo: success criterion 7.*

If scope must be cut, cut from the end of this list. Phase 1 is non-negotiable.

## 14. Left to the builder

- Crate choices (tokio, axum, serde, Raft implementation), project layout, and the dev script/Makefile for "one command".
- UI framework, charting library, visual design and layout details within §9.
- Exact synthetic metric curves, latency/jitter values and default timers not specified above.
- Whether the UI's network editor is form-based or a drag-and-drop port/link diagram.
