# Homelab Cluster Manager Prototype — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use 10x-engineer:subagent-driven-development (recommended) or 10x-engineer:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-process Rust simulator and control plane for a K3s/Longhorn/KubeVirt homelab fleet of up to 500 mixed hosts and simulated switches, with a TypeScript single-page UI to manage it and inject faults.

**Architecture:** Every host agent, control-plane (CP) replica and the UI client is an actor: a state machine that communicates only by messages routed through a simulated network fabric. The fabric understands ports, cables, switch ports, VLANs, MTU and partitions. A discrete-event engine drives the actors in simulated time. CP replicas run a compact Raft over the fabric, and only the leader runs controllers. Control logic lives in a crate that cannot see the simulator's world, so the compiler enforces the design's §5.1 boundaries. An axum server drives the engine on its own thread and streams the UI client's view of the cluster, plus ground truth, to a React UI over a WebSocket.

**Tech Stack:**
- Rust 1.99 workspace: tokio 1.54, axum 0.8 (with `ws`), serde/serde_json, ts-rs (exports TS types), rand_chacha (seeded RNG), ipnet.
- UI: Node 22, Vite 8, React 19, TypeScript, vitest, uPlot (charts).

**Spec:** `docs/plans/2026-10-23-homelab-cluster-manager-design.md`. Read it before any task. Section references like "§7" point into it.

### Builder decisions this plan locks in (deviations or refinements of the spec)

- **The engine is a discrete-event scheduler, not one tokio task per actor.** The spec says "independent async task". The scheduler preserves every §5.1 boundary (no shared state, messages only via the fabric) and adds three things: sim-time speed control, deterministic tests, and cheap scaling to 500 hosts.
- **Raft is hand-rolled** (§5.3 builder's call). openraft runs on wall-clock timers, which fights sim-time at 60× speed.
- **Rust types are the single source of truth for the wire format.** ts-rs exports them to `ui/src/gen/`.
- **Flow sources.** Phase 2 adds an `iperf` scenario as a flow source, so the network work can be demoed before workloads exist. Pod traffic rides the `mgmt` network, as flannel does. VM client traffic and transcode streams go to a `Router` endpoint attached to core-1.
- **Metrics history is held only in the leader's memory** and resets on failover. Production would use Prometheus (§11). The UI shows "metrics since <time>".
- **User actions require the sim to be running.** While paused, the API returns 409 and the UI disables actions.
- **The VM role requires at least 16 GB RAM.** The spec is silent on this.

## Global Constraints

- **Backend:** Rust only. **Frontend:** TypeScript. One command (`./run.sh`) builds the UI and starts the server on `http://localhost:8080`. No Docker.
- **The `control` crate must not depend on the `sim` crate.** Control logic never reads ground truth (§5.1.5).
- **Time:** all timers are in sim-time. `SimTime = u64` milliseconds. Sim speed choices: pause, 1×, 5×, 20×, 60×.
- **Default timers:**

  | Timer | Value |
  |---|---|
  | Raft heartbeat | 300 ms |
  | Raft election timeout | random 1500–3000 ms |
  | Raft check-quorum window | 1500 ms |
  | Host heartbeat interval | 5 s |
  | Host not-ready after | 40 s |
  | Metrics interval | 10 s |
  | Metrics history | 360 points (1 sim-hour) |
  | VM failover grace | 60 s |
  | CP replacement | 10 sim-min |
  | Network auto-revert | 30 s |
  | Rollout batch | 10% of affected hosts |
  | Rollout soak | 30 s |
  | Switch poll interval | 30 s |
  | UI push rate | ≤ 4 Hz (wall) |
  | Fabric latency | 0.2 ms per hop + 0–0.1 ms jitter |

- **Seed logical networks:**

  | Network | VLAN | Subnet | MTU |
  |---|---|---|---|
  | `mgmt` | 1 (native/untagged) | 10.0.0.0/22 | 1500 |
  | `storage` | 20 | 10.20.0.0/22 | 9000 |
  | `migration` | 30 | 10.30.0.0/22 | 9000 |
  | `vm-lan` | 100 | 10.100.0.0/22 | 1500 |
  | `vm-iot` | 110 | 10.110.0.0/24 | 1500 |

- **Role eligibility:**
  - `storage` needs a non-SD, non-eMMC disk.
  - `control-plane` needs ≥ 8 GB RAM and either x86_64 or the RK3588 profile.
  - `vm` needs ≥ 16 GB RAM.
  - `transcode` needs an `hw-transcode:*` capability.
- **Role → required networks:**
  - `storage` → `storage`
  - `vm` → `migration` plus at least one `vm-*` network
  - every role → `mgmt`
- **CP replicas:** 3, or 5 when the fleet has more than 50 hosts. CP replacement mode is `suggest` or `auto`, defaulting to `suggest`.
- **Volumes** default to 3 replicas, on distinct hosts with the `storage` role. Prefer distinct switches.
- **Disk usage alert** at > 85%.
- **Seed auto-approve rules (by profile):**

  | Profile | Roles | Template (from Task 12) |
  |---|---|---|
  | Ms01 | {Storage, Vm, Compute} | `ms01-standard` |
  | MiniPc | {Compute, Transcode, Storage} | `minipc-standard` |
  | Pi5 | {Transcode, Compute} | `sbc-transcode` |
  | Rk3588 | {Transcode, Compute} | `sbc-transcode` |

  CP voters also get `ControlPlane`. Before Task 12 the rules carry `template: None`.
- **Host location:** `rack-0` for MS-01s (next to the cores). Every other host is `rack-<n>`, where n is the index of its access switch.
- **Source control:** the project is not under source control yet. Task 1 runs `git init`. Every commit names its files.

## Review Focus

1. **Acting while the control plane is unavailable or the sim is paused.** Expected: the API returns 503 "control plane unavailable" or 409 "simulation paused" within 5 s, and the UI shows it instead of hanging. *(Test in Task 9.)*
2. **Leader failover mid-rollout.** Expected: the rollout state lives in Raft, so the new leader resumes it from the same batch rather than restarting or forgetting it. *(Test in Task 15.)*
3. **Deleting or renaming a logical network or template that is still in use.** Expected: rejected with an error listing the users. *(Test in Task 12.)*
4. **Not enough eligible capacity** (3-replica volume with 2 storage hosts, or VM failover with no room). Expected: volume `degraded` with an "insufficient storage hosts" alert, VM `pending` with a reason, and no busy loop or panic. *(Tests in Tasks 20 and 21.)*
5. **Restoring a killed host after failover.** Expected: its stale VM instances are stopped, its CP replica rejoins from its persisted log and catches up, and its stale volume replicas are discarded. *(Test in Task 21.)*

## File Structure

```
Cargo.toml                    workspace
run.sh                        one command: build UI, run server
crates/model/                 pure domain: ids, hardware, roles, networks, validation, templates, IPAM, workloads, scheduler
  src/{lib,ids,hardware,roles,network,validate,template,ipam,workload,schedule}.rs
crates/control/               software that would run on real hosts; no sim dependency
  src/runtime.rs              SimTime, Addr, Svc, Endpoint, Actor, Ctx, HostOs traits
  src/proto.rs                Msg and all message payloads; ApiOp/ApiResult
  src/raft/{mod,node,membership}.rs    generic RaftNode<T>
  src/state/{mod,objects,command}.rs   ClusterState, Object, Command, apply, changes_since
  src/controllers/{mod,health,join,network,rollout,scheduler,failover,storage,drain,membership,alerts}.rs
  src/metrics.rs              MetricsStore ring buffers and rollups (leader volatile)
  src/replica.rs              CpReplica actor
  src/agent.rs                HostAgent actor
  src/client.rs               ClientActor (the UI's connection, maintains ClusterView)
crates/sim/                   the world: engine, fabric, traffic, faults, scenarios, ground truth
  src/{lib,engine,world,topology,fabric,traffic,faults,scenario,presets,truth}.rs
  tests/{resilience,network,workloads,scale}.rs
crates/server/                axum binary
  src/{main,driver,api,ws}.rs
ui/                           Vite + React + TS
  src/gen/                    ts-rs output (generated, committed)
  src/store.ts, src/api.ts
  src/components/{TopBar,FleetGrid,FleetCanvas,HostDrawer,SwitchDrawer,WorkloadDrawer,RightRail,FaultPanel,Topology,NetworkEditor,NetworksPage,RolloutDialog,WorkloadsView,CreateWorkload,Charts}.tsx
```

---

# Phase 1 — Resilience core

### Task 1: Workspace, hardware catalogue, roles

**Files:**
- Create: `Cargo.toml`, `.gitignore`, `crates/model/Cargo.toml`, `crates/model/src/{lib,ids,hardware,roles}.rs`
- Test: inline `#[cfg(test)]` modules (convention for all Rust tasks)

**Interfaces:**
- Produces:
  - `HostId(String)`, `SwitchId(String)`, `PortName(String)`, `NetworkId(String)`. These are newtypes with `Ord, Hash, Serialize, Deserialize, TS`.
  - `enum Arch { X86_64, Arm64 }`
  - `enum ProfileId { Ms01, MiniPc, Pi5, Rk3588 }`
  - `struct HardwareProfile { id, arch, cpu_threads: u32, ram_gb: u32, nics: Vec<NicSpec{name, speed_mbps, kind: Sfp|Rj45}>, disks: Vec<DiskSpec{name, kind: Nvme|Ssd|Sd|Emmc, size_gb}>, capabilities: Vec<String> }`
  - `fn profile(id: ProfileId, variant: u8) -> HardwareProfile`. `variant` selects:
    - MS-01: 96 GB if variant is even, 64 GB if odd.
    - Pi 5: SD card if variant is even, NVMe HAT if odd.
    - RK3588: eMMC only if variant is even, eMMC plus NVMe if odd.
  - `enum Role { ControlPlane, Storage, Compute, Vm, Transcode }`
  - `fn eligible(p: &HardwareProfile, r: Role) -> Result<(), String>`

- [ ] **Step 1:** `git init`. Create the workspace with members `crates/*`. `.gitignore` covers `target/`, `ui/node_modules/` and `ui/dist/`.
- [ ] **Step 2: Write failing tests** in `hardware.rs` / `roles.rs`:
```rust
#[test] fn ms01_has_four_nics() { let p = profile(ProfileId::Ms01, 0);
  assert_eq!(p.nics.iter().map(|n| (n.name.as_str(), n.speed_mbps)).collect::<Vec<_>>(),
    vec![("sfp0",10_000),("sfp1",10_000),("eth0",2_500),("eth1",2_500)]);
  assert!(p.capabilities.contains(&"hw-transcode:quicksync".into())); }
#[test] fn sd_card_pi_cannot_store() { assert!(eligible(&profile(ProfileId::Pi5,0), Role::Storage).is_err());
  assert!(eligible(&profile(ProfileId::Pi5,1), Role::Storage).is_ok()); }
#[test] fn emmc_only_rk3588_cannot_store() { assert!(eligible(&profile(ProfileId::Rk3588,0), Role::Storage).is_err()); }
#[test] fn control_plane_rules() { assert!(eligible(&profile(ProfileId::Pi5,1), Role::ControlPlane).is_err());
  assert!(eligible(&profile(ProfileId::Rk3588,0), Role::ControlPlane).is_ok());
  assert!(eligible(&profile(ProfileId::MiniPc,0), Role::ControlPlane).is_ok()); }
#[test] fn vm_needs_16gb() { assert!(eligible(&profile(ProfileId::Pi5,1), Role::Vm).is_err()); }
#[test] fn transcode_needs_capability() { for id in [ProfileId::Ms01, ProfileId::MiniPc, ProfileId::Pi5, ProfileId::Rk3588] {
  assert!(eligible(&profile(id,0), Role::Transcode).is_ok()); } }
```
- [ ] **Step 3:** Run `cargo test -p model`. Expected: FAIL to compile.
- [ ] **Step 4:** Implement. Profile values come from spec §6.1:

  | Profile | Threads | RAM | NICs | Disks | Capability |
  |---|---|---|---|---|---|
  | MiniPc | 4 | 16 GB | `eth0` 2.5G | `nvme0` 1 TB | `hw-transcode:quicksync` |
  | Pi5 | 4 | 8 GB | `eth0` 1G | `sd0` 128 GB or `nvme0` 512 GB | `hw-transcode:v4l2` |
  | Rk3588 | 8 | 16 GB | `eth0` 2.5G | `emmc0` 256 GB (+ `nvme0` 1 TB) | `hw-transcode:rkmpp` |
  | Ms01 | 20 | 96 / 64 GB | (see test) | `nvme0`, `nvme1` 2 TB | (see test) |

- [ ] **Step 5:** Run `cargo test -p model`. Expected: PASS.
- [ ] **Step 6:** Commit: `git add Cargo.toml .gitignore crates/model docs && git commit -m "feat(model): workspace, hardware catalogue, role eligibility"`

### Task 2: Runtime interface and discrete-event engine

**Files:**
- Create: `crates/control/{Cargo.toml,src/lib.rs,src/runtime.rs,src/proto.rs}`, `crates/sim/{Cargo.toml,src/lib.rs,src/engine.rs}`

**Interfaces:**
- Produces (in `control::runtime`):
```rust
pub type SimTime = u64;
pub enum Addr { Host(HostId), Vip, Client, Router }
pub enum Svc { Agent, Cp, Client, Router }
pub struct Endpoint { pub addr: Addr, pub svc: Svc }
pub trait Ctx {
  fn now(&self) -> SimTime;
  fn me(&self) -> Endpoint;
  fn send(&mut self, to: Endpoint, net: NetworkId, msg: Msg);
  fn set_timer(&mut self, after_ms: u64, token: u64);
  fn rand_u64(&mut self) -> u64;
  fn claim_vip(&mut self);                    // CP leader only
  fn host(&mut self) -> Option<&mut dyn HostOs>;
}
pub trait HostOs { /* grows per task; Task 2: */ fn host_id(&self) -> HostId; }
pub trait Actor { fn on_start(&mut self, ctx: &mut dyn Ctx);
  fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg);
  fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64); }
```
  `control::proto::Msg` is an enum. Task 2 adds only `Msg::Ping(u64)` and `Msg::Pong(u64)`. Later tasks add variants.
- Produces (in `sim::engine`):
  - `Engine::new(seed: u64)`
  - `spawn(&mut self, at: Endpoint, actor: Box<dyn Actor>)`
  - `kill(&mut self, at: &Endpoint)`
  - `now()`
  - `run_for(ms: u64)`
  - `run_until(deadline: SimTime, pred: impl FnMut(&Engine) -> bool) -> bool`
  - Delivery goes through `trait Deliver { fn route(&mut self, from:&Endpoint, to:&Endpoint, net:&NetworkId) -> Option<(Endpoint /*resolved*/, u64 /*latency ms*/)> }`. The default is `AlwaysDeliver` (latency 1). Task 3 replaces it.

- [ ] **Step 1: Write failing tests** in `engine.rs`, using a `PingActor` test double:
  - `timers_fire_in_order`: tokens set at 30, 10 and 20 ms fire in the order 10, 20, 30, and `now()` equals each token's time when it fires.
  - `message_arrives_after_latency`: A pings B at t=0, and B sees it at t=1.
  - `killed_actor_receives_nothing`: after `kill(B)`, A's ping produces no `Pong`.
  - `run_until_stops_on_predicate`: returns `true` as soon as the predicate holds, and leaves `now()` ≤ the deadline.
- [ ] **Step 2:** Run `cargo test -p sim engine`. Expected: FAIL.
- [ ] **Step 3:** Implement the engine as a `BinaryHeap<Reverse<(SimTime, seq, Event)>>`, where `Event` is `Deliver{to, from, msg}` or `Timer{to, token}`. Actors live in `HashMap<Endpoint, Box<dyn Actor>>`. `EngineCtx` borrows the engine's queue and RNG (`ChaCha8Rng::seed_from_u64`).
- [ ] **Step 4:** Run `cargo test -p sim engine`. Expected: PASS.
- [ ] **Step 5:** Commit `crates/control crates/sim Cargo.toml`. Message: "feat: runtime traits and discrete-event engine".

### Task 3: Topology and fabric reachability (VLAN, MTU, partitions, VIP)

**Files:**
- Create: `crates/model/src/network.rs`, `crates/sim/src/{world,topology,fabric}.rs`

**Interfaces:**
- Produces (in `model::network`):
  - `LogicalNetwork { id: NetworkId, purpose: Mgmt|Storage|Migration|Vm, vlan: u16, subnet: Ipv4Net, mtu: u16 }`
  - `fn seed_networks() -> Vec<LogicalNetwork>`, with the values from Global Constraints
  - `Link { name: String, kind: Single(PortName) | Bond{ members: Vec<PortName>, mode: Lacp|ActiveBackup }, mtu: u16 }`
  - `Attachment { network: NetworkId, link: String, tagged: bool, ip: Option<Ipv4Addr> }`
  - `HostNetworkConfig { links: Vec<Link>, attachments: Vec<Attachment> }`
  - `fn bootstrap_config(p: &HardwareProfile) -> HostNetworkConfig`: `mgmt` untagged on `eth0`, MTU 1500
- Produces (in `sim::topology`):
  - `SwitchPortCfg { mode: Access(u16) | Trunk{ native: Option<u16>, allowed: BTreeSet<u16> }, mtu: u16, lacp_group: Option<String>, admin_up: bool }`
  - `Switch { id, model, ports: BTreeMap<PortName, (speed_mbps, SwitchPortCfg)>, mlag_peer: Option<SwitchId>, up: bool }`
  - `enum Node { HostPort(HostId, PortName), SwitchPort(SwitchId, PortName), Client, Router }`
  - `Topology { switches, cables: BTreeMap<Node, Node> /*symmetric*/, cable_up: BTreeSet<cable key>, partitions: Vec<BTreeSet<NodeGroupKey>>, version: u64 }`
- Produces (in `sim::world`):
  - `World { topology, hosts: BTreeMap<HostId, HostHw{ profile, powered: bool, applied_net: HostNetworkConfig, failed_disks, ... }> }`
- Produces (in `sim::fabric`):
  - `Fabric` implements `Deliver`
  - `fn reachable(&self, a: &Node, b: &Node, vlan: u16, mtu: u16) -> bool`
  - `fn path(&self, a, b, vlan) -> Option<Vec<LinkHop>>`
  - `fn record_vip_claim(&mut self, host: &HostId)`
  - `fn resolve_vip(&self, from_host: &HostId) -> Option<HostId>`

**Algorithm (decided here):**
1. For each (VLAN, topology.version), build a graph whose nodes are host attachments and switches. Cache it.
2. A host attachment for network N with VLAN v connects to the switch port its cabled member port plugs into, if all of the following hold:
   - the host is powered, the cable is up, and the switch is up with the port `admin_up`
   - the frame is accepted:
     - untagged frames take the port's Access VLAN or trunk native VLAN, which must equal v
     - tagged frames need a trunk whose `allowed` set contains v
3. Switch↔switch cables connect if both ends accept v as tagged.
4. A partition group cuts every cable with exactly one end inside the group.
5. MTU check for `reachable(..., mtu)`: every hop's port MTU must be at least `mtu`. The host link MTU must also be at least `mtu`.
6. VIP: `record_vip_claim` sets `arp[s] = host` for every switch reachable from the claimant on VLAN 1. `resolve_vip(from)` returns `arp[access switch of from's mgmt port]`.

- [ ] **Step 1: Write failing tests** in `fabric.rs`. Fixture: 2 switches (`sw-a`, `sw-b`) joined by a trunk allowing VLANs {1, 20}, with hosts `h1` on `sw-a` and `h2` on `sw-b`.
  - `untagged_mgmt_reachable`: both hosts use `bootstrap_config` on access-1 ports. `reachable(h1, h2, 1, 1500)` is true.
  - `tagged_vlan_not_allowed_blocked`: `h2`'s switch port is a trunk allowing {1}. Storage (VLAN 20) is false.
  - `mtu_blackhole`: the trunk MTU is 1500 and the storage network MTU is 9000. `reachable(..., 20, 9000)` is false and `reachable(..., 20, 1400)` is true.
  - `unplug_cable_and_kill_switch`: each of these breaks reachability, and restoring it heals.
  - `partition_by_switch`: partition {sw-b}. h1↔h2 is false and h1↔Client (on sw-a) is true.
  - `vip_follows_last_reachable_claim`: h1 claims, then a partition isolates sw-b, then h2 claims. `resolve_vip(h1)` is h1 and `resolve_vip(h2)` is h2 (stale ARP on each side). After healing, h2 claims again and both resolve to h2.
- [ ] **Step 2:** Run `cargo test -p sim fabric`. Expected: FAIL.
- [ ] **Step 3:** Implement per the algorithm. `route()` uses `reachable` with MTU 1500 for control messages. Latency is 0.2 ms × hop count plus jitter, rounded up to whole milliseconds with a minimum of 1.
- [ ] **Step 4:** Run `cargo test -p sim fabric`. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(sim): topology and VLAN/MTU-aware fabric with partitions and VIP ARP".

### Task 4: Raft core (election, replication, check-quorum, persistence)

**Files:**
- Create: `crates/control/src/raft/{mod,node}.rs`

**Interfaces:**
- Produces:
```rust
pub struct RaftConfig { heartbeat_ms: u64, election_min_ms: u64, election_max_ms: u64 } // 300, 1500, 3000
pub struct Durable<T> { term: u64, voted_for: Option<HostId>, log: Vec<Entry<T>>, voters: BTreeSet<HostId> }
pub struct Entry<T> { term: u64, index: u64, payload: EntryPayload<T> } // Normal(T) | Config(BTreeSet<HostId>) | Noop
pub enum RaftMsg<T> { RequestVote{..}, VoteResp{..}, Append{..}, AppendResp{..} }
pub enum RaftOut<T> { Send(HostId, RaftMsg<T>), Persist(DurableOp<T>), BecameLeader(u64), SteppedDown }
impl<T: Clone> RaftNode<T> {
  pub fn new(me: HostId, durable: Durable<T>, cfg: RaftConfig, rand_seed: u64) -> Self;
  pub fn tick(&mut self, now: SimTime) -> Vec<RaftOut<T>>;
  pub fn step(&mut self, now: SimTime, from: HostId, m: RaftMsg<T>) -> Vec<RaftOut<T>>;
  pub fn propose(&mut self, now: SimTime, t: T) -> Result<u64, NotLeader>;
  pub fn take_committed(&mut self) -> Vec<Entry<T>>;
  pub fn is_leader(&self) -> bool; pub fn leader_hint(&self) -> Option<HostId>; pub fn term(&self) -> u64;
}
```
  `DurableOp` is one of `SetHardState{term, voted_for}`, `Append(Vec<Entry>)` or `TruncateFrom(index)`. On becoming leader, the node appends a `Noop`.

- [ ] **Step 1: Write failing tests** in `node.rs`. They use an in-test `Cluster` harness with 3 to 5 nodes, a message queue, manual time, and a `cut(a, b)` helper.
  - `elects_single_leader`: within 6000 ms there is exactly one leader, with term ≥ 1.
  - `replicates_and_commits`: the leader proposes 1, 2 and 3. Every node's committed payloads equal `[1, 2, 3]`.
  - `minority_cannot_commit`: isolate the leader with one follower out of 5 nodes. Its proposal never commits. The majority elects a new leader within 6000 ms.
  - `check_quorum_steps_down`: an isolated leader returns `SteppedDown` within 1500 + 300 ms and reports `is_leader() == false`.
  - `restart_from_durable`: rebuild a node from replayed `Persist` ops. It keeps its term and log and rejoins without losing committed entries.
  - `stale_leader_rejected`: after healing, the old leader's `Append` with a lower term is refused, and it becomes a follower.
- [ ] **Step 2:** Run `cargo test -p control raft`. Expected: FAIL.
- [ ] **Step 3:** Implement standard Raft (Ongaro §5) with check-quorum. The election timeout is redrawn from the seeded RNG on every reset. Use leader-only commit of current-term entries.
- [ ] **Step 4:** Run `cargo test -p control raft`. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(control): hand-rolled raft with check-quorum".

### Task 5: Cluster state machine (phase-1 commands)

**Files:**
- Create: `crates/control/src/state/{mod,objects,command}.rs`

**Interfaces:**
- Produces:
  - `HostObj { id, profile: HardwareProfile, inventory: Inventory, location: String, lifecycle: Pending|Approved|Provisioning|Ready|NotReady|Cordoned|Draining|Drained|Removed, roles: BTreeSet<Role>, labels: BTreeMap<String,String>, last_transition: SimTime, version: u64 }`
  - `JoinToken { token: String, expires_at: SimTime }`
  - `AutoApproveRule { id, profile: ProfileId, roles: BTreeSet<Role>, template: Option<String> }`
  - `Alert { id, severity: Info|Warning|Critical, kind: String, entity: ObjectRef, message, raised_at, resolved_at: Option<SimTime>, ground_truth: bool }`
  - `Event { seq, at, entity, message }`. Events are a ring of the latest 5000.
  - `enum Object` is serde-tagged by `kind`. `ObjectRef { kind, id }`.
  - `ClusterState { version: u64, hosts, tokens, rules, alerts, events, cp: CpInfo{ voters, mode: Suggest|Auto } ... }`
  - `fn apply(&mut self, at: SimTime, cmd: Command) -> Result<(), ApplyError>`
  - `fn changes_since(&self, v: u64) -> Changes { version, upserts: Vec<Object>, removed: Vec<ObjectRef> }`
  - `Command` (phase 1): `CreateJoinToken`, `UpsertRule`, `DeleteRule`, `AddPendingHost`, `ApproveHost{id, roles}`, `SetLifecycle{id, lifecycle}`, `SetRoles`, `SetLabels`, `RemoveHost`, `RaiseAlert`, `ResolveAlert`, `Event`
  - Every object and the Changes types derive `TS` with `#[ts(export, export_to = "../../ui/src/gen/")]`.

- [ ] **Step 1: Write failing tests:**
  - `apply_bumps_version_and_tracks_changes`: apply `AddPendingHost`. Then `changes_since(0).upserts` contains the host, and `changes_since(state.version)` is empty.
  - `approve_requires_pending`: `ApproveHost` on a `Ready` host returns `Err(ApplyError::InvalidLifecycle)`.
  - `approve_rejects_ineligible_role`: approving an SD-card Pi 5 with `Storage` returns an error that names the role.
  - `remove_host_leaves_tombstone`: `changes_since(v).removed` contains the host's ref.
  - `events_ring_caps_at_5000`.
- [ ] **Step 2:** Run `cargo test -p control state`. Expected: FAIL.
- [ ] **Step 3:** Implement. `apply` must be deterministic: no RNG, and time comes only from `at`.
- [ ] **Step 4:** Run `cargo test -p control state`. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(control): replicated cluster state machine".

### Task 6: CP replica actor, health controller, client actor

**Files:**
- Create: `crates/control/src/{replica,client}.rs`, `crates/control/src/controllers/{mod,health}.rs`
- Modify: `crates/control/src/proto.rs`, `crates/sim/src/engine.rs` (wire `claim_vip` and VIP resolution to the fabric)

**Interfaces:**
- Consumes: Tasks 2, 3, 4 and 5.
- Produces (protocol):
  - `Msg::Raft(RaftMsg<Command>)`
  - `Msg::Heartbeat{host, lifecycle_seen: u64}`
  - `Msg::Api{req: u64, op: ApiOp}`
  - `Msg::ApiResp{req, result: ApiResult}`, where `ApiResult` is `Ok(serde_json::Value)`, `Err(String)`, `NotLeader` or `Unavailable`
  - `Msg::Sync{since: u64}`
  - `Msg::SyncResp{changes: Changes, leader: HostId, term: u64, metrics: Vec<(HostId, MetricSample)>}`
  - `ApiOp` (phase 1): `CreateJoinToken`, `ApproveHost{id, roles}`, `SetRoles`, `SetLabels`, `Cordon`, `Uncordon`, `RemoveHost`, `UpsertRule`, `DeleteRule`
- Produces (replica): `CpReplica::new(me: HostId, durable: Option<Durable<Command>>, voters: BTreeSet<HostId>)`. Its behaviour:
  - It ticks Raft every 100 ms via a timer. On `BecameLeader` it calls `ctx.claim_vip()`.
  - Only the leader runs `controllers::run_all(&state, &volatile, now) -> Vec<Action>` every 1000 ms. An `Action` is `Propose(Command)` or `Send(Endpoint, Msg)`.
  - A non-leader answers `Api` with `NotLeader`. A leader that has lost quorum answers `Unavailable`.
  - API writes are answered after commit and apply. Pending requests are keyed by log index.
- Produces (health controller): it tracks `last_heartbeat` in leader-volatile memory. A host whose last heartbeat is more than 40 s old gets `SetLifecycle NotReady` and a `HostDown` alert. A heartbeat from a `NotReady` host restores `Ready` and resolves the alert. A new leader treats every host as last seen at its election time, so it never mass-fails hosts on takeover.
- Produces (client):
  - `ClientActor` sends `Sync{since}` to the VIP every 500 ms and merges each `SyncResp` into a `ClusterView { state: ClusterState-like maps, version, leader, term, last_sync_at, metrics }`.
  - `submit(op) -> u64` sends `Api` to the VIP. It retries `NotLeader` every 300 ms. After 5000 ms it gives up and yields `Unavailable`.
  - `view() -> &ClusterView`, `take_results() -> Vec<(u64, ApiResult)>`
- Produces (engine): `Engine::fabric()`, `Engine::client_view()` and `Engine::replica_log_len()` are added here, because this task's tests need them. Task 8 lists them again with the other helpers.

- [ ] **Step 1: Write failing tests** in `crates/sim/tests/resilience.rs`. The fixture is 3 hosts on one switch. Each runs a `CpReplica` and a minimal heartbeat-only agent stub. The client is on the same switch.
  - `three_replicas_elect_and_claim_vip`: within 6000 ms, `engine.fabric().resolve_vip(h)` is the leader for all hosts.
  - `client_sync_sees_leader`: within 2000 ms after election, `client.view().leader` equals the true leader.
  - `minority_leader_stops_controllers`: partition the leader alone. After 2000 ms it has proposed no new entries. Assert its log length doesn't grow.
- [ ] **Step 2:** Run `cargo test -p sim --test resilience`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p sim --test resilience`. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: CP replica actor, health controller, client view".

### Task 7: Host agent and join controller

**Files:**
- Create: `crates/control/src/agent.rs`, `crates/control/src/controllers/join.rs`
- Modify: `crates/control/src/runtime.rs` (HostOs), `crates/sim/src/world.rs` (implement HostOs)

**Interfaces:**
- Produces:
  - `HostOs` gains `inventory() -> Inventory{profile, nics with link_up, disks}`, `apply_network(cfg)`, `applied_network() -> HostNetworkConfig`, `usage() -> HostUsage{cpu_pct, ram_pct, disk_pct: BTreeMap, port_bps: BTreeMap}` and `load_cp()/persist_cp(DurableOp)`.
  - Messages: `Msg::JoinRequest{token, inventory}`, `Msg::JoinResp(Result<(), String>)`, `Msg::Approved{roles}`, `Msg::Metrics{host, sample: MetricSample}`.
  - `HostAgent::new(token: Option<String>)`:
    - If the host is unjoined, it boots with `bootstrap_config`. Every 5 s it sends `JoinRequest` to the VIP until it is accepted.
    - Once joined, it sends `Heartbeat` every 5 s and `Metrics` every 10 s.
  - The join controller:
    - Validates the token (exists and not expired).
    - Creates the host via `AddPendingHost`.
    - If an `AutoApproveRule` matches the profile, approves it with the rule's roles. Ineligible roles are dropped and an event is recorded.
    - On approval moves the host to `Provisioning`, then to `Ready` on its first heartbeat. Task 14 inserts the network apply step here.

- [ ] **Step 1: Write failing tests** in `resilience.rs`:
  - `join_pending_then_approve`: spawn a mini PC with a valid token. It is `Pending` within 10 s. After `ApiOp::ApproveHost` it is `Ready` within 15 s.
  - `invalid_token_rejected`: an unknown token produces no host object after 20 s and records a `join rejected` event.
  - `auto_approve_rk3588`: with rule `Rk3588 → {Transcode, Compute}`, the host reaches `Ready` with those roles and no API calls.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: host agent and join controller".

### Task 8: Presets, scenarios, faults, ground truth

**Files:**
- Create: `crates/sim/src/{presets,scenario,faults,truth}.rs`

**Interfaces:**
- Produces:
  - `FleetSpec { ms01: u32, minipc: u32, pi5: u32, rk3588: u32 }`
  - `Preset::Default`, which is `FleetSpec{12, 24, 12, 12}` = 60 hosts.
  - `Preset::Scale500`, which is `FleetSpec{40, 200, 120, 140}`.
  - `fn build_fleet(spec, seed) -> (World, Vec<initial actors>)`:
    - `core-1` and `core-2` are an MLAG pair with 64× 10G SFP+ ports each and a 40G inter-core link.
    - `access-N` switches have 48× 2.5G ports plus 4× 10G uplinks. Ports 1–46 are for hosts. Each access switch has one uplink to each core.
    - The `Router` is on `core-1` port `p63` (trunk allowing {1, 100, 110}). The `Client` is on `core-1` port `p64` (access 1).
    - MS-01: `sfp0`→core-1 and `sfp1`→core-2, both on port index i. `eth0`→the next free access port. `eth1` is left uncabled.
    - Every other host: `eth0`→the next free access port.
    - Initial CP voters are the first 3 MS-01s (5 if the fleet has more than 50 hosts).
    - The initial Raft state is the same seeded `Durable` on every voter. Log entries approve every host with its rule roles and add the seed networks and rules, as if the cluster were already installed.
    - Switch ports for every host's bootstrap mgmt are Access 1. Phase 2 extends this to technician configs.
  - `Engine::load_preset(p)`
  - `Scenario::AddHosts{profile, count, misconfigure: Option<Misconfig>}`. It cables hosts to free access ports, configures those ports, and spawns agents with the current join token (the scenario creates one via the client first).
  - `Fault` variants: `KillHost`, `RestoreHost`, `UnplugPort{host, port}`, `ReplugPort`, `KillSwitch`, `RestoreSwitch`, `Partition{nodes: Vec<PartitionMember: Host|Switch>}`, `HealPartitions`, `SetSwitchPort{switch, port, cfg}`, `MoveClient{switch, port}`.
    - `KillHost` kills both actors on the host.
    - `RestoreHost` re-spawns the agent, and the replica if the host's durable CP state exists.
  - `GroundTruth { hosts: Vec<{id, powered, cp_role: Option<Leader|Follower|Candidate>}>, vip_owner_by_switch, partitions, split_brain_vms: Vec<String> /*Task 21*/, sim_time }`, from `Engine::truth()`
  - Test helpers: `Engine::true_leader() -> Option<HostId>`, `Engine::client_view() -> &ClusterView`, `Engine::fabric() -> &Fabric` and `Engine::replica_log_len(&HostId) -> Option<usize>`. Tests written as `client.view()` mean `engine.client_view()`.
  - Spawns the `ClientActor` at `Endpoint{Addr::Client, Svc::Client}` and attaches it to the topology node `Node::Client`.

- [ ] **Step 1: Write failing tests** in `resilience.rs`:
  - `default_preset_all_ready`: 60 hosts, and `client.view()` shows 60 `Ready` within 30 s.
  - `leader_failover_criterion_3`: record the leader L, then `KillHost(L)`. Within 6000 ms there is a new `true_leader() != L`, and the client view shows the new leader within a further 2000 ms. No entry appended to L's durable log after the kill time is committed on the new leader.
  - `restore_host_rejoins_as_follower`: restore L. Its replica catches up to the leader's commit index within 5000 ms.
  - `kill_host_marks_not_ready`: kill a non-CP host. It is `NotReady` with a `HostDown` alert within 45 s.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(sim): presets, scenarios, faults, ground truth".

### Task 9: Server: engine driver, HTTP API, WebSocket

**Files:**
- Create: `crates/server/{Cargo.toml,src/main.rs,src/driver.rs,src/api.rs,src/ws.rs}`

**Interfaces:**
- Consumes: `Engine` (Task 8) and `ClientActor` (Task 6).
- Produces:
  - **Driver:** a `std::thread` that owns the `Engine`. Every 20 ms of wall time it drains `EngineCmd`s from an mpsc channel and advances the sim by `20 × speed` ms. It publishes `Published { view_version, changes since last publish, truth, sim: SimInfo{now, speed, paused} }` on a `tokio::sync::watch`, at most 4 times per second.
  - **HTTP endpoints:**
    - `POST /api/ops`: body is an `ApiOp` (JSON) and the reply is an `ApiResult`. Status codes: 200 for `Ok`; 400 for `Err`; 503 `{"error":"control plane unavailable"}` for `Unavailable`; 409 `{"error":"simulation paused"}` when paused.
    - `POST /api/sim/speed`: body `{speed: 0|1|5|20|60}`, where 0 means pause.
    - `POST /api/sim/fault`: body is a `Fault`.
    - `POST /api/sim/scenario`: body is a `Scenario` or `{"preset": "default"|"scale500"}`.
    - `GET /api/ws`: the first message is `{type:"snapshot", view, truth, sim}`, then `{type:"delta", upserts, removed, truth, sim}`.
  - Static files are served from `ui/dist` via `tower_http::services::ServeDir`. The port is 8080.

- [ ] **Step 1: Write failing tests** in `api.rs`, using `tower::ServiceExt::oneshot` against `app(DriverHandle)` with the Default preset:
  - `approve_op_roundtrip`: `CreateJoinToken` returns 200 with a token string.
  - `paused_returns_409`: set speed 0, then `POST /api/ops` returns 409 within 1 s.
  - `unavailable_returns_503`: kill 2 of the 3 CP hosts, then `POST /api/ops` returns 503 within 6 s (wall time at speed 20).
  - `ws_first_message_is_snapshot`: connect over the WebSocket. The first message's `type` is `"snapshot"` and `view.hosts` has length 60.
- [ ] **Step 2:** Run `cargo test -p server`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test -p server`. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(server): engine driver, API, websocket".

### Task 10: UI scaffold, store, top bar, run.sh

**Files:**
- Create: `ui/` (Vite React TS), `ui/src/{store.ts,api.ts,App.tsx}`, `ui/src/components/TopBar.tsx`, `ui/src/store.test.ts`, `run.sh`
- Generated: `ui/src/gen/*.ts`, produced by `cargo test -p control` (ts-rs export)

**Interfaces:**
- Produces:
  - **`store.ts`:**
    - `createStore()` returns `{ getState(): UiState, subscribe(fn), applyMessage(msg: WsMessage) }`
    - `UiState = { view: { hosts: Record<string, HostObj>, alerts, events, ... }, truth: GroundTruth, sim: SimInfo, connection: 'connecting'|'live'|'cp-unavailable' }`
    - `useStore(selector)` via `useSyncExternalStore`
    - The connection becomes `cp-unavailable` when `sim.now - view.last_sync_at > 3000`.
  - **`api.ts`:** `op(o: ApiOp)`, `fault(f)`, `scenario(s)`, `setSpeed(n)`. On 409 or 503 they reject with the server's error string.
  - **`run.sh`:**
    1. `(cd ui && npm ci && npm run build)`
    2. `cargo run --release -p server`
    3. Print the URL.

- [ ] **Step 1:** Scaffold with `npm create vite@latest ui -- --template react-ts`. Add `vitest` and the script `"test": "vitest run"`. Configure the dev proxy so `/api` goes to `:8080`.
- [ ] **Step 2: Write failing tests** in `store.test.ts`:
  - `snapshot then delta merges upserts and removes`: after a snapshot with hosts a and b, then a delta that upserts a (with lifecycle `NotReady`) and removes b, `hosts` contains only a, with lifecycle `NotReady`.
  - `stale sync flips connection to cp-unavailable`.
- [ ] **Step 3:** Run `cd ui && npm test`. Expected: FAIL.
- [ ] **Step 4:** Implement the store, the WebSocket client with auto-reconnect, and `TopBar`. The top bar shows:
  - Ready, NotReady and Pending counts
  - the leader host and term
  - quorum status, as live replicas over voters from the view
  - active alert count
  - the sim clock (hh:mm:ss) with speed buttons
  - a red "control plane unavailable — reconnecting" banner when the connection is `cp-unavailable`
- [ ] **Step 5:** Run `npm test && npm run build`. Expected: PASS, and the build succeeds.
- [ ] **Step 6:** Run `./run.sh`, open `localhost:8080`, and confirm the top bar shows 60 Ready hosts.
- [ ] **Step 7:** Commit `ui run.sh`. Message: "feat(ui): scaffold, store, top bar".

### Task 11: UI fleet grid, host drawer, right rail, fault panel

**Files:**
- Create: `ui/src/components/{FleetGrid,HostDrawer,RightRail,FaultPanel}.tsx`, `ui/src/grouping.ts`, `ui/src/grouping.test.ts`

**Interfaces:**
- Consumes: `store.ts` and `api.ts` (Task 10).
- Produces:
  - `groupHosts(hosts, by: 'rack'|'switch'|'role'|'profile') => Array<{key: string, hosts: HostObj[]}>`. Groups are sorted by key, and hosts by id. The `switch` key comes from the LLDP data in Task 13; until then it is `'unknown'`.

- [ ] **Step 1: Write failing tests** in `grouping.test.ts`:
  - `groups by profile sorted`
  - `host with multiple roles appears in each role group`
- [ ] **Step 2:** Run `npm test`. Expected: FAIL.
- [ ] **Step 3:** Implement the components:
  - **FleetGrid:** tiles coloured by lifecycle, with role badges and CPU/RAM bars. Clicking a tile selects the host.
  - **HostDrawer:** inventory, roles editor (`SetRoles`, with ineligible roles disabled and a reason tooltip), Approve/Cordon/Uncordon/Remove buttons, and API errors shown inline.
  - **RightRail:** tabs for Alerts, Events and Pending joins. Pending joins have Approve with a roles picker.
  - **FaultPanel:** kill/restore the selected host, partition the selected switch, heal, the add-hosts scenario (profile and count), and preset buttons.
- [ ] **Step 4:** Run `npm test && npm run build`. Expected: PASS.
- [ ] **Step 5:** Manually check with `./run.sh`. Kill the leader's host from the fault panel. The banner shows briefly, then the new leader appears in the top bar. *(Phase 1 demo.)*
- [ ] **Step 6:** Commit. Message: "feat(ui): fleet grid, host drawer, alerts rail, fault panel".

---

# Phase 2 — Networking

### Task 12: Network validation, templates, IPAM, effective roles

**Files:**
- Create: `crates/model/src/{validate,template,ipam}.rs`
- Modify: `crates/control/src/state/*` (networks and templates as objects, plus commands)

**Interfaces:**
- Produces:
  - **Templates:**
    - `NetworkTemplate { id: String, profile: ProfileId, role: Option<Role>, config: HostNetworkConfig /*ips None*/ }`
    - `fn seed_templates() -> Vec<NetworkTemplate>`:
      - `ms01-standard`: bond0 = [sfp0, sfp1] LACP, MTU 9000, carrying storage, migration, vm-lan and vm-iot (all tagged). eth0 is a single link, MTU 1500, carrying mgmt untagged.
      - `minipc-standard`: eth0, MTU 9000, carrying mgmt (untagged) and storage (tagged).
      - `sbc-transcode`: eth0, MTU 1500, carrying mgmt untagged. Used by Pi 5 and RK3588.
    - `fn render(t: &NetworkTemplate, override_: Option<&HostNetworkConfig>, ips: &BTreeMap<NetworkId, Ipv4Addr>) -> HostNetworkConfig`
  - **Observed switch data:** `ObservedSwitchPort { switch: SwitchId, port: PortName, cfg: SwitchPortCfgView, mlag_peer: Option<SwitchId> }`. `SwitchPortCfgView` mirrors `SwitchPortCfg`; move that type into `model::network` and re-export it from sim.
  - **Validation:**
    - `enum Issue { MissingRequiredNetwork{role, network}, VlanNotAllowed{host_port, switch, switch_port, vlan}, NativeVlanMismatch{..}, MtuTooSmall{where_: String, have, need}, BondSplitWithoutMlag{link}, LacpNotConfigured{link, switch, port}, IpConflict{ip, other_host}, PortNotCabled{port}, UnknownNetwork{id} }`
    - `fn validate_host(cfg: &HostNetworkConfig, roles: &BTreeSet<Role>, networks: &[LogicalNetwork], lldp: &[LldpNeighbor], switch_ports: &[ObservedSwitchPort], ips_in_use: &BTreeMap<Ipv4Addr, HostId>) -> Vec<Issue>`
    - `fn broken_networks(issues: &[Issue]) -> BTreeSet<NetworkId>`
    - `fn effective_roles(roles, broken: &BTreeSet<NetworkId>) -> (BTreeSet<Role> /*active*/, BTreeMap<Role, NetworkId> /*held*/)`
  - **IPAM:** `fn allocate(net: &LogicalNetwork, used: &BTreeSet<Ipv4Addr>) -> Option<Ipv4Addr>`. It skips the network address, the gateway (.1) and broadcast, and returns the lowest free address.
  - **State commands:** `UpsertNetwork`, `DeleteNetwork`, `UpsertTemplate`, `DeleteTemplate`, `SetHostOverride{host, config: Option<HostNetworkConfig>}`.
    - `DeleteNetwork` and `DeleteTemplate` return `Err(InUse(Vec<ObjectRef>))` when referenced.
    - `UpsertNetwork` with a changed VLAN or MTU on a network already in use is allowed. Task 15's rollout carries the change.

- [ ] **Step 1: Write failing tests**, one per `Issue` variant, using small fixtures. Also:
  - `ms01_standard_valid_on_technician_ports`: zero issues.
  - `storage_role_held_when_vlan20_missing`: `effective_roles` holds `Storage` because of `storage`.
  - `ipam_skips_reserved_and_used`: on 10.20.0.0/22 with .2 used, `allocate` returns 10.20.0.3.
  - `delete_in_use_network_rejected` *(Review Focus 3)*: deleting `storage` while `ms01-standard` uses it returns `InUse` listing the template.
- [ ] **Step 2:** Run `cargo test -p model && cargo test -p control state`. Expected: FAIL.
- [ ] **Step 3:** Implement. Validation rules come from §6.3 and §6.4:
  - Bond members must land on the same switch or on an MLAG pair, and all of them need the same `lacp_group`, which must be non-empty.
  - Untagged attachments need the port's native or access VLAN to equal the network's VLAN.
  - The switch port MTU must be at least the network MTU, and the host link MTU must also be at least the network MTU.
  - Uplink MTU along the path is checked by the fabric, not here.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(model): network validation, templates, IPAM".

### Task 13: Observation: LLDP from agents, switch polling

**Files:**
- Modify: `crates/control/src/{agent,proto}.rs`, `crates/control/src/state/*`, `crates/sim/src/{world,engine}.rs`
- Create: `crates/sim/src/switch_actor.rs`

**Interfaces:**
- Produces:
  - `HostOs::lldp() -> Vec<LldpNeighbor{local_port, switch, switch_port}>`. The world derives it from cables where the link is up.
  - Agents include `lldp` and `inventory` in their heartbeat whenever either changes.
  - A `SwitchActor` sits at `Endpoint{addr: Addr::Switch(id), svc: Svc::Switch}`. **Add `Addr::Switch(SwitchId)` and `Svc::Switch` to the runtime.** It answers `Msg::SwitchPoll` with `Msg::SwitchReport{switch, model, ports: Vec<ObservedSwitchPort>, mlag_peer}` by reading its own switch in the world (this is the switch's own config).
  - The leader polls every switch every 30 s over `mgmt`. Switch management ports are untagged on VLAN 1 through their uplink.
  - New state objects: `SwitchObj{id, model, ports, mlag_peer, last_seen}` and `HostObj.lldp`.

- [ ] **Step 1: Write failing tests** in `crates/sim/tests/network.rs`:
  - `lldp_reported`: in the default preset, ms01-01's `HostObj.lldp` contains (`sfp0`, `core-1`, `p1`) within 15 s.
  - `switch_config_observed`: after `Fault::SetSwitchPort` changes access-1 p3 to Access 20, `view.switches["access-1"]` reflects the change within 35 s.
  - `unreachable_switch_goes_stale`: `KillSwitch(access-2)` leaves `last_seen` frozen and raises a `SwitchUnreachable` alert after 90 s.
- [ ] **Step 2:** Run `cargo test -p sim --test network`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: LLDP observation and switch polling".

### Task 14: Network controller, apply with auto-revert, join applies template

**Files:**
- Create: `crates/control/src/controllers/network.rs`
- Modify: `crates/control/src/{agent,controllers/join}.rs`, `crates/sim/src/{presets,scenario}.rs`

**Interfaces:**
- Produces:
  - **Messages:** `Msg::ApplyNetwork{generation: u64, config}` and `Msg::NetworkStatus{generation, result: Applied|Failed(String)|Reverted}`.
  - **Agent apply-with-revert:** the agent applies the config. If it gets no `ConfirmNetwork{generation}` from the CP within 30 s, it re-applies the previous config and reports `Reverted`. The CP sends `ConfirmNetwork` when it receives `NetworkStatus::Applied` over the new config, which proves mgmt still works.
  - **HostObj additions:** `template: Option<String>`, `override_: Option<HostNetworkConfig>`, `ips: BTreeMap<NetworkId, Ipv4Addr>`, `net: { desired_generation, applied_generation, state: Pending|Applying|Applied|Failed|Reverted, issues: Vec<Issue>, drift: bool, held_roles: BTreeMap<Role, NetworkId> }`.
  - **Join flow:** approval picks the template (rule template first, else the first template matching the profile), allocates IPs for mgmt, storage and migration attachments, and moves the host to `Provisioning`. The network controller pushes the config. The host becomes `Ready` on `Applied` with mgmt reachable. A host stuck on `Reverted` stays `Provisioning` with an alert.
  - **Network controller (every 1 s):** for each host it renders, validates against observed data, stores issues and held roles, and raises or resolves `NetworkMismatch` alerts. Each alert includes a suggested switch-side change string, for example `"access-1 p7: add VLAN 20 to trunk allowed"`.
  - **Scenario technician:** `AddHosts` configures switch ports to match the profile's template:
    - SBC ports: Access 1.
    - Mini PC ports: Trunk native 1, allowed {1, 20}, MTU 9216.
    - MS-01: SFP ports are Trunk native none, allowed {20, 30, 100, 110}, MTU 9216, with `lacp_group = host id`. `eth0` is Access 1.
    - `Misconfig` options: `MissingVlan(20)`, `MtuTooSmall`, `BondNoLacp`.
  - **Presets:** they now boot hosts with their template applied (instead of bootstrap) and technician-configured ports.

- [ ] **Step 1: Write failing tests** in `network.rs`:
  - `criterion_2_twenty_rk3588_join`: with rule `Rk3588 → {Transcode, Compute}, template sbc-transcode`, run `AddHosts{Rk3588, 20, None}`. All 20 reach `Ready` with `net.state == Applied` within 60 s and with no API calls.
  - `misconfigured_join_ready_but_role_held`: `AddHosts{MiniPc, 1, Some(MissingVlan(20))}` with roles {Storage, Compute} reaches `Ready`. `held_roles` contains Storage, and a `NetworkMismatch` alert's message contains "add VLAN 20".
  - `mgmt_breaking_override_reverts`: setting an override that moves mgmt to tagged VLAN 1 on an access port gives `Reverted` within 35 s. The host stays reachable and an alert is raised.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: network controller with auto-revert; templated joins".

### Task 15: Template and network rollouts

**Files:**
- Create: `crates/control/src/controllers/rollout.rs`
- Modify: `crates/control/src/state/*`, `crates/control/src/proto.rs`

**Interfaces:**
- Produces:
  - **Preview:** `ApiOp::PreviewChange{change: Change}`, where `Change` is `Template(NetworkTemplate)` or `Network(LogicalNetwork)`. It returns `Preview{ affected: Vec<HostId>, overridden: Vec<HostId>, issues: BTreeMap<HostId, Vec<Issue>>, switch_changes: Vec<String> }`. It has no side effects; it renders against a cloned state.
  - **Rollout ops:** `ApiOp::StartRollout{change, include_overridden: bool, batch_pct: u8 /*default 10*/, soak_s: u32 /*default 30*/}`, `PauseRollout{id}`, `ResumeRollout{id}`.
  - **Rollout object:** `Rollout{ id, change, hosts: Vec<HostId>, batch_size, current_batch: usize, state: Running|Soaking{until}|Paused{reason}|Done, failed: Vec<HostId> }`. It is stored in Raft state.
  - **Rollout controller:**
    1. It applies the change object only when the rollout starts.
    2. It pushes configs batch by batch.
    3. After a batch reaches all `Applied`, it soaks, then moves to the next batch.
    4. Any `Failed` or `Reverted` host means `Paused{reason}`.
    5. Hosts outside the current and done batches keep their previous rendered config. Freeze it with `HostObj.net.pinned_config` until their batch runs.

- [ ] **Step 1: Write failing tests** in `network.rs`:
  - `preview_counts_affected`: changing `ms01-standard`'s bond to active-backup reports 12 affected and 0 overridden in the default preset.
  - `rollout_batches_of_10pct`: changing `minipc-standard`'s MTU to 1500 on 24 hosts gives batch size 3. After the first batch, exactly 3 hosts have the new generation applied until the soak ends.
  - `rollout_pauses_on_revert`: a change that breaks mgmt pauses after batch 1, and the remaining hosts stay unchanged.
  - `rollout_survives_leader_failover` *(Review Focus 2)*: kill the leader mid-rollout. Within 10 s the new leader continues from the same `current_batch` and the rollout reaches `Done`.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: batched network rollouts with preview and auto-pause".

### Task 16: Traffic model, bonds, iperf, port and bond alerts

**Files:**
- Create: `crates/sim/src/traffic.rs`
- Modify: `crates/sim/src/{engine,scenario,world}.rs`, `crates/control/src/controllers/alerts.rs`

**Interfaces:**
- Produces:
  - `Flow { id: u64, src: Node-ish endpoint (HostId|Router), dst, network: NetworkId, demand_mbps: f64, kind: Iperf|ReplicaWrite|Rebuild|Migration|VmClient|Transcode|Pod }`
  - `Traffic::tick(&mut self, world: &World, fabric: &Fabric) -> TickResult { rate: BTreeMap<u64, f64>, port_bps: BTreeMap<Node, (rx, tx)> }`. The engine runs it every 1000 sim-ms.
  - `Scenario::Iperf{from, to, network, mbps, duration_s}`
  - `HostOs::usage().port_bps` is filled from the latest tick.

**Algorithm:**
1. **Routing:** a flow is routable only if `reachable(src, dst, vlan, network.mtu)`. Otherwise its rate is 0 (blackhole).
2. **Member port choice:** pick the source host's member port on the attachment's link.
   - For `Lacp`, use `flow.id % active_members.len()` over members whose link is up.
   - For `ActiveBackup`, use the first up member.
3. **Switch path:** BFS shortest path over up switch↔switch cables that allow the VLAN, ties broken by switch id. Then the destination's member port, chosen the same way.
4. **Rates:** max-min fair share (progressive filling) over the directed capacities of every traversed port and cable, each capped by its demand.

- **Alerts (CP side, from agent inventory and metrics):** `PortDown` (a cabled port with link down), `BondDegraded` (a bond with fewer than all members up) and `LinkSpeedLow` (negotiated speed below the profile speed; the world can set a degraded speed via `Fault::DegradePort{host, port, mbps}`).

- [ ] **Step 1: Write failing tests:**
  - In `traffic.rs`:
    - `fair_share_two_flows_one_link`: two 10 000 Mbps demands over one 10G hop get 5000 each.
    - `lacp_spreads_flows`: 4 flows over a 2×10G bond use both members.
    - `blackhole_on_mtu`: storage at MTU 9000 over a 1500-MTU trunk gives rate 0.
  - In `network.rs`:
    - `criterion_5_bond_member_unplug`: iperf 8 flows ms01-01→ms01-02 on storage. After `UnplugPort(ms01-01, sfp0)`, every flow's port is sfp1, aggregate throughput is ≤ 10 000 Mbps and > 0, and a `BondDegraded` alert appears within 15 s.
    - `criterion_5_vlan_removed`: removing VLAN 20 from ms01-01's core-1 and core-2 ports drops storage iperf to 0 and raises a `NetworkMismatch` alert within 35 s.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(sim): flow traffic model with bonds, MTU blackholes, port alerts".

### Task 17: UI topology view and switch drawer

**Files:**
- Create: `ui/src/components/{Topology,SwitchDrawer}.tsx`, `ui/src/topology.ts`, `ui/src/topology.test.ts`
- Modify: `crates/server` (include `port_bps` and flows in the published truth and metrics)

**Interfaces:**
- Produces: `layoutTopology(switches, hosts, opts: {collapseOver: number}) => { nodes: {id, x, y, kind}[], edges: {a, b, utilisation: number, state: 'up'|'down'|'mismatch'}[] }`.
  - Core switches sit on the top row and access switches on the second row. Hosts sit under the switch their mgmt port plugs into.
  - MS-01 SFP legs are drawn to the cores.
  - Each access switch with more than `collapseOver` (default 24) hosts is shown as a group node with counts.

- [ ] **Step 1: Write failing tests:**
  - `cores above access`
  - `collapses large access switch into group`
  - `mismatch edge flagged when host issues reference that switch port`
- [ ] **Step 2:** Run `npm test`. Expected: FAIL.
- [ ] **Step 3:** Implement:
  - **Topology view:** SVG with utilisation colour bands (0–50%, 50–80% and over 80%) and red dashes for mismatches. Clicking a switch opens SwitchDrawer.
  - **SwitchDrawer:** a port table showing cfg, link, throughput and connected host, plus a port config editor that posts `Fault::SetSwitchPort` (the human editing the switch). It also lists suggested fixes from open `NetworkMismatch` alerts.
  - Load the `dataviz` skill before choosing colours.
- [ ] **Step 4:** Run `npm test && npm run build`. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(ui): topology view and switch drawer".

### Task 18: UI network editor, networks/templates/rules pages, rollout dialog, iperf

**Files:**
- Create: `ui/src/components/{NetworkEditor,NetworksPage,RolloutDialog}.tsx`, `ui/src/netedit.ts`, `ui/src/netedit.test.ts`
- Modify: `HostDrawer.tsx`, `FaultPanel.tsx`

**Interfaces:**
- Produces: pure helpers in `netedit.ts`, each returning a new `HostNetworkConfig`:
  - `addBond(cfg, name, members, mode)`
  - `moveAttachment(cfg, network, link)`
  - `setTagged(cfg, network, tagged)`
  - `removeLink(cfg, name)`, which also drops that link's attachments

- [ ] **Step 1: Write failing tests:**
  - `addBond removes members from single links`
  - `removeLink drops its attachments`
  - `moveAttachment keeps ip`
- [ ] **Step 2:** Run `npm test`. Expected: FAIL.
- [ ] **Step 3:** Implement:
  - **NetworkEditor:** form-based (ports → links → network attachments). It shows template drift and the issues list, and saves via `SetHostOverride`.
  - **NetworksPage:** tabs for Networks, Templates and Auto-approve rules, with create/edit/delete. Deletes show the `InUse` error list.
  - **RolloutDialog:** shows the preview (affected and overridden counts, issues, switch changes), an "include overridden" checkbox, batch % and soak fields, Start, and live progress with Pause/Resume.
  - **FaultPanel:** gains an iperf form and port unplug/replug for the selected host.
- [ ] **Step 4:** Run `npm test && npm run build`. Expected: PASS.
- [ ] **Step 5:** Manually check with `./run.sh`. Unplug ms01-01 sfp0 and watch the topology and alerts. Remove VLAN 20 on a core port and see the suggested fix. *(Phase 2 demo.)*
- [ ] **Step 6:** Commit. Message: "feat(ui): network editor, config pages, rollouts".

---

# Phase 3 — Workloads

### Task 19: Workload model and scheduler

**Files:**
- Create: `crates/model/src/{workload,schedule}.rs`
- Modify: `crates/control/src/state/*`

**Interfaces:**
- Produces:
  - **Deployment:** `Deployment { id, image, replicas: u32, cpu_m: u32, ram_mb: u32, selector: Selector{ roles: BTreeSet<Role>, capability_prefix: Option<String> } }`. Pods need role `Compute`, plus `Transcode` if `capability_prefix` is `hw-transcode:`.
  - **Vm:** `Vm { id, vcpu, ram_mb, disk_gb, networks: Vec<NetworkId>, volume: String }`. The VM role is required.
  - **Volume:** `Volume { id, size_gb, replicas: u8 /*default 3*/ }`.
  - **Status objects:**
    - `PodStatus{deployment, idx, host: Option<HostId>, phase: Pending(reason)|Running}`
    - `VmStatus{host: Option<HostId>, phase: Pending(reason)|Starting|Running|Migrating{to}|Stopped, since}`
    - `VolumeStatus{ replicas: Vec<ReplicaRef{host, disk, state: Healthy|Rebuilding{pct}|Stale}>, health: Healthy|Degraded|Rebuilding|Faulted }`
  - **Scheduler:** `fn place(req: &PlacementReq, hosts: &[HostView], exclude: &BTreeSet<HostId>) -> Result<HostId, String>`, where `HostView { id, lifecycle, active_roles, capabilities, alloc_cpu_m, alloc_ram_mb, used_cpu_m, used_ram_mb, switch: Option<SwitchId> }`.
    - **Filter:** `Ready`, not cordoned or draining, the required active role, the capability, and resource fit.
    - **Score:** lowest `max(cpu_frac, ram_frac)` after placement, ties broken by id.
    - **Errors:** `Err` lists the counts for each failing filter, for example `"no fit: 3 not ready, 10 missing role vm, 2 insufficient ram"`.
  - **Replica placement:** `fn place_replicas(n: u8, size_gb, hosts: &[StorageHostView], existing: &[HostId]) -> Vec<HostId>`. It uses distinct hosts with the active `Storage` role and enough free disk, preferring distinct switches. It may return fewer than `n`.
  - **Allocatable resources:** CPU is threads × 1000 m minus a 500 m reserve. RAM is total minus 2 GB.
  - **Commands:** `Create`/`Delete` for each workload kind, plus status updates.

- [ ] **Step 1: Write failing tests** in `schedule.rs`:
  - `vm_never_on_pi`
  - `transcode_pod_needs_capability_and_role`
  - `least_allocated_wins`
  - `replicas_distinct_hosts_prefer_distinct_switches`: 3 replicas over 4 hosts on 2 switches uses both switches.
  - `insufficient_storage_hosts_returns_fewer` *(Review Focus 4)*: n=3 with 2 eligible hosts returns 2.
  - `no_fit_error_explains`
- [ ] **Step 2:** Run `cargo test -p model schedule`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat(model): workloads and scheduler".

### Task 20: Running workloads: agent instances, scheduling controller, volumes, traffic

**Files:**
- Create: `crates/control/src/controllers/scheduler.rs`
- Modify: `crates/control/src/{agent,proto}.rs`, `crates/sim/src/{world,traffic,scenario}.rs`

**Interfaces:**
- Produces:
  - **HostOs additions:**
    - `start_instance(InstanceSpec{ id: String /* "vm/<id>" or "pod/<dep>/<idx>" */, cpu_m, ram_mb, usage: UsageProfile, flows: Vec<FlowSpec> })`, plus `stop_instance(id)` and `instances() -> Vec<String>`
    - `create_replica(volume, size_gb, source: Option<HostId>) -> disk name`, `replicas() -> Vec<(volume, disk, pct_complete)>` and `delete_replica(volume)`
    - The world turns each `FlowSpec` into a flow (Task 16). For rebuild flows, it advances `pct_complete` by the bytes that flow actually carried.
  - **Messages:**
    - `Msg::StartInstance(InstanceSpec)`, `Msg::StopInstance(id)`
    - `Msg::CreateReplica{volume, size_gb, source}`, `Msg::DeleteReplica{volume}`
    - Heartbeats include `instances` and `replicas`.
  - **Scheduler controller:**
    - Pending pods and VMs are placed via `place`. Each placement commits the status first, then sends `StartInstance`.
    - Volumes without enough replicas get `place_replicas` targets. The first replicas start empty; later ones are rebuilt from a healthy source.
    - A volume stays `Degraded` with an `InsufficientStorageHosts` alert if fewer hosts are available than its replica count.
  - **Instance flows:**
    - A VM with a volume writes 20–200 Mbps (seeded curve) to each remote replica host on `storage` (`ReplicaWrite`). It also sends 5–50 Mbps to the Router on its first `vm-*` network (`VmClient`).
    - A transcode pod streams 15 Mbps to the Router on mgmt.
    - Other pods send 1–10 Mbps on mgmt.
  - **Usage:** instance CPU and RAM usage follows `UsageProfile{base_pct, noise_pct, burst_every_s}`. Host metrics sum their instances plus a 3% baseline.
  - **Seeding:** `Scenario::SeedWorkloads`, scaled per 60 hosts: 30 VMs (2–4 vCPU, 4–8 GB RAM, 40–80 GB disks, on vm-lan or vm-iot), 1 transcode deployment with replicas equal to SBC count / 2, and 6 general deployments × 3 replicas.

- [ ] **Step 1: Write failing tests** in `crates/sim/tests/workloads.rs`:
  - `seed_workloads_all_running`: default preset plus seeding. All VMs and pods are `Running` and all volumes `Healthy` within 120 s.
  - `vm_replicas_on_three_storage_hosts`
  - `transcode_pods_only_on_capable_hosts`
  - `insufficient_storage_hosts_degraded_alert` *(Review Focus 4)*: a custom fleet with 2 storage hosts and one volume gives a `Degraded` volume with the alert, and the CP proposes no more than 5 entries per minute for it (no busy loop).
- [ ] **Step 2:** Run `cargo test -p sim --test workloads`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: run workloads, volumes, workload traffic".

### Task 21: VM failover, storage rebuild, disk failure, split-brain truth

**Files:**
- Create: `crates/control/src/controllers/{failover,storage}.rs`
- Modify: `crates/sim/src/{truth,faults}.rs`

**Interfaces:**
- Produces:
  - **Failover controller:** a VM whose host has been `NotReady` for more than 60 s (`VmStatus.since` / host `last_transition`) is re-placed with the dead host excluded, and its status is set to `Starting` on the new host. If nothing fits, it becomes `Pending("no fit: …")` and is retried every 30 s.
  - **Stale stop:** when a host's heartbeat lists an instance not assigned to it, the CP sends `StopInstance`. Replicas it reports that the volume status marks `Stale` or doesn't list get `DeleteReplica`.
  - **Storage controller:**
    - A replica on a `NotReady` host (after 60 s) or a failed disk is marked `Stale`, and a rebuild target is placed.
    - A rebuild that makes no progress for 60 s is abandoned and re-placed with that target excluded.
    - A volume is `Faulted` if it has no healthy replica.
  - **Faults:** `Fault::FailDisk{host, disk}`. Its replicas are lost and the agent reports the disk failed.
  - **Ground truth:** `GroundTruth.split_brain_vms` lists VMs running on more than one powered host at once. Each starts a ground-truth alert with `ground_truth: true`, which the engine adds to the published truth rather than CP state.

- [ ] **Step 1: Write failing tests** in `workloads.rs`:
  - `criterion_4_host_failure`: kill the VM host H (not a CP host).
    - Within 60 + 40 + 15 s, every VM that was on H is `Running` elsewhere.
    - Within 300 s, every volume that had a replica on H is `Healthy` again with no replica on H.
    - During the rebuild, some storage-network port shows rebuild throughput above 0.
  - `criterion_6_partition_split_brain`: partition access-2 with all its hosts. A VM on an MS-01 whose mgmt port is on access-2 gets restarted elsewhere after the grace period, and `truth.split_brain_vms` contains it while the partition holds. After healing, the stale instance stops within 15 s and `split_brain_vms` is empty.
  - `disk_failure_rebuilds`
  - `rebuild_stall_retargets`: a target whose storage VLAN is blackholed gets re-placed after 60 s.
  - `restore_after_failover` *(Review Focus 5)*: kill H, wait for failover and rebuild, then restore H. Within 30 s, H runs no VM instances, holds no stale replicas, and (if it hosts a CP replica) matches the leader's commit index.
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: VM failover, replica rebuild, split-brain ground truth".

### Task 22: Drain and live migration

**Files:**
- Create: `crates/control/src/controllers/drain.rs`
- Modify: `crates/control/src/{agent,proto}.rs`

**Interfaces:**
- Produces:
  - **Ops:** `ApiOp::Drain{host}` and `ApiOp::MigrateVm{vm, to: Option<HostId>}`.
  - **Migration:** `Msg::MigrateIn{vm spec, from}`. The target agent starts a `Migration` flow of `ram_mb × 8` Mbit on `migration` from the source. When it completes, the target reports and the CP sends `StopInstance` to the source.
    - VM phase: `Migrating{to}` then `Running`.
    - If the migration network is blackholed, the migration fails after 120 s, reverts to `Running` on the source and records an event.
  - **Drain controller:**
    1. Set the host to `Draining`.
    2. Migrate VMs one at a time.
    3. Reschedule pods.
    4. For each replica on the host, place and rebuild a new replica elsewhere, then delete the old one.
    5. Set `Drained` once nothing remains.
    If a step has no target, the drain stays `Draining` with an alert explaining why.

- [ ] **Step 1: Write failing tests** in `workloads.rs`:
  - `drain_empties_host`: drain an MS-01. It reaches `Drained` within 600 s with 0 instances and 0 replicas, and no VM ever goes `Stopped`.
  - `migration_traffic_on_migration_network`: during a migration, the flow's network is `migration` and its rate is above 0.
  - `migration_fails_when_vlan30_blocked`
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: drain with live migration".

### Task 23: UI workloads view, workload drawer, create forms, ground-truth overlay

**Files:**
- Create: `ui/src/components/{WorkloadsView,WorkloadDrawer,CreateWorkload}.tsx`, `ui/src/workloads.ts`, `ui/src/workloads.test.ts`
- Modify: `FleetGrid.tsx`, `HostDrawer.tsx`, `TopBar.tsx`, `FaultPanel.tsx`

**Interfaces:**
- Produces:
  - `filterWorkloads(items, {kind, text, host, health}) => items`
  - `truthDiff(view, truth) => { hostId: string; believed: string; actual: string }[]`, which lists hosts where `view.lifecycle` says Ready but the truth says unpowered (and the reverse), plus split-brain VMs.

- [ ] **Step 1: Write failing tests:**
  - `filter by kind and health`
  - `truthDiff flags powered-off host still Ready`
  - `truthDiff lists split-brain vm`
- [ ] **Step 2:** Run `npm test`. Expected: FAIL.
- [ ] **Step 3:** Implement:
  - **WorkloadsView:** VM, pod and volume tables showing placement and replica hosts.
  - **WorkloadDrawer:** migrate (with a target picker), restart and a replica map.
  - **CreateWorkload:** VM, deployment and volume forms.
  - **Host actions:** Drain button in HostDrawer, and the host's workloads listed there.
  - **FaultPanel:** gains fail-disk and seed-workloads buttons.
  - **Ground-truth overlay:** a toggle in TopBar. Tiles get a hatched overlay where they differ from the truth, and there is a "Split-brain" alert section labelled "ground truth".
- [ ] **Step 4:** Run `npm test && npm run build`. Expected: PASS.
- [ ] **Step 5:** Manually check criteria 4 and 6 in the UI. *(Phase 3 demo.)*
- [ ] **Step 6:** Commit. Message: "feat(ui): workloads, drawer, create forms, ground-truth overlay".

---

# Phase 4 — Scale and monitoring

### Task 24: Raft membership, CP replacement and growth

**Files:**
- Create: `crates/control/src/raft/membership.rs`, `crates/control/src/controllers/membership.rs`
- Modify: `crates/control/src/{raft/node,replica,agent}.rs`, `crates/sim/src/engine.rs`

**Interfaces:**
- Produces:
  - **Raft membership:**
    - `RaftNode::add_learner(id)`, `promote(id) -> Result<u64, MembershipError>` and `remove(id) -> Result<u64, MembershipError>`.
    - Changes are single-server, and only one config change can be uncommitted at a time.
    - Learners receive Append but don't vote or count toward quorum.
    - A learner is promoted once its match index is within 10 entries of the leader's last index.
  - **New replicas:** `Msg::StartCpReplica{voters}` makes the agent call `HostOs::spawn_cp_replica(voters)`.
  - **Membership controller:**
    - **Target size:** 3 replicas, or 5 when there are more than 50 non-removed hosts. Candidates are `Ready` hosts that are CP-eligible, not cordoned, and prefer MS-01. Spreading across access switches is preferred.
    - **Replacement:** a voter whose host has been `NotReady` for more than 10 min triggers either a suggestion (a `CpReplacementSuggested` alert with an `ApiOp::ReplaceCpMember{dead, with}` button) or, in `auto` mode, the full flow: add learner → catch up → promote → remove the dead member.
    - **Growth:** when the fleet crosses the threshold, it adds members the same way.
    - **Quorum alert:** a `CpQuorumAtRisk` alert fires when live voters equal the bare quorum.
  - **Ops:** `ApiOp::SetCpMode{mode}` and `ApiOp::ReplaceCpMember{dead, with}`.
- [ ] **Step 1: Write failing tests:**
  - In `raft/membership.rs` (unit tests):
    - `learner_doesnt_vote`
    - `promote_then_remove_keeps_quorum`
    - `second_change_rejected_while_pending`
  - In `resilience.rs`:
    - `default_fleet_grows_to_5`: 60 hosts give 5 voters within 120 s.
    - `auto_replace_dead_member`: mode `auto`, kill a follower's host. After 10 min plus 60 s, there are 5 voters and none is the dead host.
    - `suggest_mode_only_alerts`
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: raft membership and CP replacement/growth".

### Task 25: Metrics store, rollups, metrics API, alert refinement

**Files:**
- Create: `crates/control/src/metrics.rs`
- Modify: `crates/control/src/{replica,proto}.rs`, `crates/control/src/controllers/alerts.rs`, `crates/server/src/api.rs`

**Interfaces:**
- Produces:
  - `MetricSample { at, cpu_pct, ram_pct, temp_c, disk_pct: BTreeMap<String, f32>, port_rx_bps, port_tx_bps: BTreeMap<String, f64>, instances: u32 }`
  - `MetricsStore::record(host, sample)`, `series(entity: MetricEntity::Host(id)|Fleet|Role(Role)|Switch(id), metric: &str) -> Vec<(SimTime, f64)>` and `since() -> SimTime`. Each series is a ring of 360 points.
  - `ApiOp::GetSeries{entity, metrics: Vec<String>}` is forwarded through the client and fabric to the leader. Server route: `GET /api/metrics?entity=host:ms01-01&m=cpu_pct,ram_pct`.
  - **Alerts:** add `DiskUsageHigh` (> 85%). Make every alert kind one of a defined set with a severity mapping:
    - Critical: `HostDown`, `CpQuorumAtRisk`, `VolumeFaulted`
    - Warning: `BondDegraded`, `PortDown`, `NetworkMismatch`, `VolumeDegraded`, `DiskUsageHigh`, `LinkSpeedLow`, `InsufficientStorageHosts`, `SwitchUnreachable`, `CpReplacementSuggested`
  - Alerts are de-duplicated by (kind, entity).
- [ ] **Step 1: Write failing tests:**
  - In `metrics.rs`:
    - `ring_caps_at_360`
    - `fleet_rollup_is_mean_cpu`
    - `switch_rollup_sums_port_bps`
  - In `resilience.rs`:
    - `disk_usage_alert_over_85`: a volume set fills a host's disk, and the alert fires and resolves on delete.
    - `alerts_deduplicated`
- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Commit. Message: "feat: metrics store, rollups, metrics API, alert set".

### Task 26: Scale to 500: engine perf, canvas grid, charts

**Files:**
- Create: `crates/sim/tests/scale.rs`, `ui/src/components/{FleetCanvas,Charts}.tsx`, `ui/src/canvasLayout.ts`, `ui/src/canvasLayout.test.ts`
- Modify: `FleetGrid.tsx` (switch to the canvas when there are more than 120 hosts), `HostDrawer.tsx` (charts), `crates/sim` and `crates/server` (perf fixes as profiling shows)

**Interfaces:**
- Produces:
  - `tileLayout(groups, width, tile = 18, gap = 3) => { rects: {hostId, x, y, w, h}[], height }`
  - `hitTest(rects, x, y) => hostId | null`
  - `Charts`: uPlot line charts for the selected host (CPU, RAM, port throughput) and fleet rollups. Data comes from `/api/metrics` every 5 s while the host is selected.
- [ ] **Step 1: Write failing tests:**
  - In `scale.rs`, run with `--release`:
    - `scale500_preset_ready`: 500 hosts `Ready` within 60 sim-s.
    - `scale500_runs_faster_than_realtime`: with workloads seeded, simulating 120 sim-s takes less than 40 wall-s. That is 3× headroom for 1× real time.
    - `scale500_sync_payload_bounded`: in steady state, the average `SyncResp` upserts per sync are below 200 objects.
  - In `canvasLayout.test.ts`:
    - `500 hosts fit with wrapping`
    - `hitTest returns host under point`
- [ ] **Step 2:** Run `cargo test -p sim --release --test scale` and `npm test`. Expected: FAIL.
- [ ] **Step 3:** Implement the canvas grid and charts. Load the `dataviz` skill before writing chart code. Profile with `cargo flamegraph` or `samply` if the perf test fails, and fix hot paths: reachability cache misses, traffic tick and sync diffing.
- [ ] **Step 4:** Run both commands. Expected: PASS.
- [ ] **Step 5:** Manually check with `./run.sh`. Load the scale500 preset and seed workloads. At 1×, the grid and topology pan and select without visible stalls, and killing the leader still recovers. *(Phase 4 demo, criterion 7.)*
- [ ] **Step 6:** Commit. Message: "feat: 500-host scale, canvas fleet grid, metrics charts".
