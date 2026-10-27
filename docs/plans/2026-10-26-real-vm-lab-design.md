# Homelab Cluster Manager — Real VM Lab Design

Date: 2026-10-26
Status: Approved 2026-10-26; amended after the phase A spike (§14)
Builds on: `2026-10-23-homelab-cluster-manager-design.md` (the prototype design) and its decisions log.

## 1. Intent

The prototype proved the control plane against a simulated fleet. This phase runs **the same control-plane code on real virtual machines**, so the claims stop depending on the simulator being right. We want to show that:

- real machines join a cluster with a token and are approved, templated and made `ready`;
- the control plane elects a leader over a real network, holds a real virtual IP, and keeps its Raft state on real disks;
- the UI manages the real cluster through that VIP;
- powering off, resetting, freezing, unplugging or partitioning a VM produces the same reconciliation the simulator showed: leader failover, `not-ready`, VM failover, replica rebuild, network alerts and split-brain flagging.

The lab starts at **4 VMs** on one Linux host with KVM.

### Decisions already taken (with the user, 2026-10-26)

| Question | Decision |
|---|---|
| Workloads | **Real stand-ins.** Keep our scheduler, failover and storage controllers. An instance is a real process on the VM, and a volume replica is a real file copied over TCP on the storage network. Real K3s, Longhorn and KubeVirt remain the later production step (prototype design §11). |
| Simulator | **Kept as a test harness only.** The server and UI run only against real VMs: no sim mode, sim scenarios, speed control or presets. The `sim` crate and its integration tests (criteria 3–6, 500-host scale) stay as headless regression coverage for `model` and `control`. |
| Network fidelity | **Real VLAN switching between multi-NIC VMs.** VMs have several NICs and run real bonds, 802.1Q VLANs and MTUs. Originally planned as VLAN-filtering Linux bridges; the spike found the lab host's kernel lacks bridge VLAN filtering, so the switches are a userspace switch in the `lab` crate (§14). |

## 2. Goals, non-goals, anti-goals

**Goals**
1. One command brings up the lab: switches, 4 VMs, a bootstrapped cluster, and the UI on `localhost`.
2. Every success criterion in §10 is checked by an automated end-to-end run against the real VMs, not only by eye in the UI.
3. `model` and `control` are shared unchanged in spirit by the sim harness and the real runtime. A controller change is tested by both.
4. Fault injection works at the VM's "hardware": power, reset, freeze, NIC link, disk, switch port, partition.

**Non-goals**
- Real K3s, Longhorn, KubeVirt, or real switch hardware or APIs.
- Authentication or encryption between nodes. Messages are unauthenticated JSON over TCP; production would use mTLS.
- Raft log compaction or snapshots. The log is replayed on boot (R12), which is fine at lab size.
- More than one physical lab host, or ARM guests (x86 profiles only; ARM emulation is too slow to be useful).
- Running on macOS. The lab needs Linux with KVM and root.

**Anti-goals**
- **The lab manager doing the control plane's job.** The lab may only do what a person with a screwdriver and a power strip could do: cable, power and configure switches, and watch from outside. It never writes cluster state, never tells an agent what to do, and never picks a leader.
- **A second code path for "real".** If a controller behaves differently on real VMs because the code branches on it, the validation is void. Only the `Ctx` and `HostOs` implementations differ.
- **Faked observations.** Inventory, LLDP, link state, port counters, disk usage and replica progress come from the guest's real kernel and files, not from the lab telling the agent.

## 3. What changes and what does not

| Layer | Prototype | Real VM lab |
|---|---|---|
| `model` | Domain model and validation | Unchanged, plus three small additions (§8) |
| `control` | Raft, replica, agent, controllers, client, behind `Ctx` and `HostOs` | Unchanged logic, plus a small set of additions (§8) |
| Runtime (`Ctx`) | Sim engine: sim-time, fabric delivery | **New `node` crate:** tokio, wall-clock timers, TCP transport, real VIP |
| Host OS (`HostOs`) | Simulated `HostHw` | **New `LinuxOs`:** `/proc`, `/sys`, systemd-networkd, systemd units, files on NVMe disks |
| Switches | Simulated topology and fabric | `labswitch`: a userspace VLAN switch (unix-datagram ports to QEMU), with a management API |
| Faults and ground truth | Engine peeks at the world | **New `lab` crate:** QEMU QMP, `labswitch`, qemu-guest-agent |
| Server | Drives the sim engine | Runs the `ClientActor` on the real runtime, plus the lab panel |
| UI | Sim clock, speed, presets, scenarios | Wall clock, lab panel; everything else unchanged |

## 4. Architecture

```
┌──────────────────────────── lab host (Linux, KVM, root) ─────────────────────────────┐
│                                                                                      │
│  server (axum) ── ClientActor on node runtime ──TCP──▶ VIP 10.0.3.253 (the leader)   │
│     │  UI + /api/ops + /ws + /api/lab                                                │
│     └─ lab manager: QEMU/QMP · labswitch · dnsmasq · chronyd · relay · QGA           │
│                                                                                      │
│  labswitch (userspace):  core-1 (10G "SFP+")  ══ trunk ══  access-1 (2.5G)           │
│                            p1..p6 (LACP po1..po3)           p1..p4, p8 → lab-router  │
│                            ▲ unix dgram sockets ▲           tap: 10.0.0.1 + VLAN .1s │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────────┐                │
│  │ ms01-1 (VM)  │ │ ms01-2 (VM)  │ │ ms01-3 (VM)  │ │ minipc-1 (VM) │                │
│  │ homelabd:    │ │ homelabd     │ │ homelabd     │ │ homelabd      │                │
│  │  agent       │ │  agent       │ │  agent       │ │  agent        │                │
│  │  CP replica  │ │  CP replica  │ │  CP replica  │ │  instances    │                │
│  │  instances   │ │  instances   │ │  instances   │ │               │                │
│  │  replicas    │ │  replicas    │ │  replicas    │ │               │                │
│  └──────────────┘ └──────────────┘ └──────────────┘ └───────────────┘                │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.1 Lab hardware (4 VMs)

| VM | Poses as (SMBIOS) | vCPU / RAM | NICs → switch ports | Disks |
|---|---|---|---|---|
| `ms01-1..3` | Minisforum `MS-01` | 8 / 12 GB | `sfp0`, `sfp1` → `core-1` p(2n−1), p(2n), LACP group `po<n>`; `eth0` → `access-1` p<n> (mgmt); `eth1` uncabled | `nvme0`, `nvme1`: 64 GB sparse qcow2, virtio-blk with `serial=nvme0`/`nvme1` |
| `minipc-1` | `Generic mini PC` | 4 / 8 GB | `eth0` → `access-1` p4 | `nvme0`: 64 GB (same) |

- Three MS-01s give the control plane 3 voters, Longhorn 3 storage hosts and KubeVirt 3 VM hosts. The mini PC runs pods and does not qualify for the control plane. That leaves 2 of 3 voters when one MS-01 fails, so quorum holds.
- Total footprint is about 44 GB RAM and 28 vCPU. The lab host has 223 GB and 166 cores.
- **Discovered, not catalogued, capacity.** The agent identifies the catalogue profile from DMI (`/sys/class/dmi/id/product_name`), but reports the CPU, RAM, NICs and disks it actually finds. The join controller merges them into the host's `HardwareProfile` (§8), so the scheduler packs against 12 GB, not the catalogue's 96 GB.
- NIC names (`sfp0`, `eth0`…) are fixed by systemd `.link` files matching the MACs the lab assigns, as udev rules would on real hardware. QEMU's `speed=` property makes `/sys/class/net/*/speed` report 10000 or 2500.
- **Port speed is enforced** by `labswitch`'s per-port token bucket. The userspace switch tops out well below 10 Gbit/s, so in practice the nominal speed is a ceiling, not a guarantee. *Degrade port* lowers the rate.

### 4.2 Addresses

Networks keep the seed definitions (`mgmt` 10.0.0.0/22 untagged, `storage` VLAN 20, `migration` VLAN 30, `vm-lan` VLAN 100, `vm-iot` VLAN 110).

| Address | What |
|---|---|
| `10.0.0.1`, and `.1` of every network | The lab host as the router: VLAN sub-interfaces on `br-access1`'s uplink port. Flows to `Router` end here, in a traffic sink. |
| `10.0.3.100–10.0.3.199` | Bootstrap DHCP pool (dnsmasq on mgmt). A fresh host boots on this. |
| `10.0.3.251`, `10.0.3.252` | `switchd` management addresses for `core-1` and `access-1`. |
| `10.0.3.253` | The control-plane VIP. |
| Everything else | IPAM, as today. IPAM must skip the reserved block (§8). |

Guests get packages through an HTTP proxy relay at `10.0.0.1:3130` (3128 is taken on the lab host), which forwards to the lab host's upstream proxy. The lab host is IPv6-only, so plain NAT would not work.

## 5. The real runtime (`node` crate, binary `homelabd`)

One `homelabd` process per VM hosts that host's actors: always the `HostAgent`, and the `CpReplica` when one is spawned or found on disk. It implements `Ctx` and `HostOs` and nothing else. Control code stays sim-agnostic.

### 5.1 Event loop and time

- A single-threaded tokio loop owns the actors and calls `on_start`, `on_message` and `on_timer` exactly as the engine does. Actor code is not thread-safe and does not need to be.
- `Ctx::now()` returns **wall-clock Unix milliseconds**. It stamps Raft entries (`LogCmd.at`) and the deadlines carried in messages (migration deadline, token TTL), so the VMs' clocks must agree. Guests run chrony against the lab host. `SimTime` stays a `u64`; its doc changes to "milliseconds; Unix time on real hosts".
- `set_timer` uses the **monotonic** clock, so a clock step cannot fire or starve timers.
- **Freeze/thaw** (measured in the spike): the guest's clocks stop while it is frozen. On thaw, its wall clock is behind by the length of the freeze, and its timers carry on as if no time had passed. chrony (against the lab host, `makestep 0.5 -1`, 1–4 s polling) steps the wall clock back within about 2 s. A thawed ex-leader therefore believes it is leader for up to one election timeout of its own time. That is the stale-leader case we want to exercise. Raft safety does not depend on clocks.

### 5.2 Transport

- **TCP, port 7400,** with length-prefixed JSON frames `{from: Endpoint, to: Endpoint, msg: Msg}`. `Msg` already derives serde. `SyncResp` can be large, which rules out UDP.
- `send` is **fire-and-forget and lossy,** like the fabric. Each peer has a bounded queue. If the peer is unreachable, the dial times out (500 ms) and queued messages are dropped. Raft, heartbeats and resends already tolerate loss.
- **One connection per peer, used both ways.** A reply to an endpoint that dialled in goes back on that connection. This is how the leader answers a pending host (which has only a DHCP address) and the UI client.
- **Address resolution** (`Endpoint` → IP on the requested network), in order:
  1. `Addr::Vip` → `10.0.3.253`. `Addr::Switch(id)` → its `switchd` address (from a small static table the runtime is given at boot, as a real host would know its switches' management addresses from DHCP options or config).
  2. An open connection from that endpoint.
  3. The local `CpReplica`'s applied state, when the host runs one: the host object's IPAM address on that network.
  4. The last **directory** the leader pushed (`Msg::Directory`, §8), persisted to `/var/lib/homelab/directory.json` so that a rebooted node can find its peers before it hears from the leader.

### 5.3 The VIP

- After every actor callback, the runtime asks its `CpReplica` whether it is the leader with quorum. If so and it does not hold the VIP, it adds `10.0.3.253/22` to the mgmt interface and sends gratuitous ARP. If not and it holds the VIP, it removes it at once.
- This follows the `claim_vip` contract. The fabric's "last gratuitous ARP wins" becomes real switch FDB and ARP-cache behaviour.
- A thawed ex-leader still has the VIP configured until its first callback tells it it is no longer leader. That window is real and the e2e test measures it (§10, criterion 3b).

### 5.4 `LinuxOs` (the `HostOs` implementation)

| `HostOs` method | Real implementation |
|---|---|
| `inventory` | DMI product name → `ProfileId`; `/proc/cpuinfo`, `/proc/meminfo`; NICs from `/sys/class/net/<n>/{carrier,speed}`; disks named by their virtio serial (`/sys/block/vd*/serial`); a disk that was there at boot and is gone now is `failed`. |
| `lldp` | homelabd listens for LLDP frames (ethertype 0x88cc) on every NIC with an `AF_PACKET` socket and keeps the latest (chassis = switch id, port id = switch port) per NIC, with TTL expiry. |
| `apply_network` | Renders the config as systemd-networkd `.netdev`/`.network` files (bonds with `802.3ad` or `active-backup`, VLAN sub-interfaces, MTUs, static IPs) into **`/run/systemd/network`**, then runs `networkctl reload`. `/run` is tmpfs, so an unconfirmed config is gone after a reboot. |
| `persist_network` | Copies the running files to `/etc/systemd/network`. This is the `netplan try` "accept". |
| `applied_network` | `/run/homelab/net.json` (the running config) falling back to `/var/lib/homelab/net.json` (the persisted one). Bootstrap is DHCP on `eth0`, shipped in the image. |
| `usage` | CPU from `/proc/stat` deltas, RAM from `/proc/meminfo`, disk from `statvfs` of each NVMe mount, per-port bit/s from `/sys/class/net/*/statistics` deltas. Temperature stays synthetic (VMs have no sensors) and is labelled as such. |
| `load_cp`, `store_cp`, `persist_cp` | `/var/lib/homelab/raft/`: a snapshot file written by `store_cp`, plus an append-only JSON-lines op log, `fsync`ed on every op. `load_cp` replays it. |
| `mark_joined` | `/var/lib/homelab/joined`. At boot, homelabd starts the agent with no token if this file exists. |
| `start_instance`, `stop_instance`, `instances` | A transient systemd unit `homelab-inst-<id>.service`, started with `systemd-run` and running `homelabd instance <spec.json>` under `CPUQuota` and `MemoryMax` from the spec. It burns CPU and touches RAM along its usage profile, and **sends its flows as real traffic** (§6). Units survive an agent restart, as containers would, and die with the VM. |
| `migrate_in`, `abort_migration`, `migrations` | §6.2 |
| `create_replica`, `replicas`, `delete_replica` | §6.3 |
| `spawn_cp_replica` | Creates empty Raft state on disk and starts a `CpReplica` actor in the same process. |

### 5.5 Bootstrapping the first node

`homelabd bootstrap` on `ms01-1` plays `k3s server --cluster-init`. It writes a seeded Raft log with:

- `SetCp { voters: {ms01-1} }`;
- the seed networks, templates and auto-approve rules;
- a host object for itself, built from its own inventory: approved with its rule's roles and provisioned on its template;

and then starts normally.

This is `sim::presets::seeded_durable` for a fleet of one, so that function moves into `control` and both use it.

From then on everything is the real flow. The lab asks the cluster for a join token through the VIP (`CreateJoinToken`), as an admin would, and boots the other three VMs with the token in cloud-init. Each joins, is auto-approved by its rule, and is templated and provisioned. Membership grows the control plane from 1 to 3 voters, one at a time, after `GROWTH_SETTLE_MS`.

## 6. Workload stand-ins (real processes, real bytes)

The controllers are unchanged. What each host does when told is now real.

### 6.1 Instances and flows

- Each `FlowSpec` in an instance becomes a real TCP stream from the instance's process, on the flow's network, paced along the same seeded min/max curve.
- `FlowEnd::Router` targets the lab router's sink at that network's `.1`. `FlowEnd::Host(h)` targets `h`'s homelabd sink on that network, resolved as in §5.2.
- Port counters, and therefore metrics, port utilisation in the topology view, and the "storage traffic stops" check, are measured, not modelled.
- Replica-write flows (`ReplicaWrite`) **write real data into the target replica files** through the target homelabd's storage service, so a replica's content is the VM's writes. That is a stand-in for Longhorn's engine, and it fixes the prototype's "stale replicas hold no modelled data" limitation.

### 6.2 Live migration

- On `MigrateIn`, the target homelabd pulls `ram_mb` MiB of the instance's state from the source homelabd over the `migration` network, paced by the link.
- On completion, the target starts the unit and tells the source, which stops its unit.
- The deadline, abort and "source no longer runs it" rules are the ones the sim's `World::advance_migrations` implements, ported to `LinuxOs`, and the agent's existing logic drives them.

### 6.3 Volume replicas

- A replica is a sparse file `<disk-mount>/homelab/replicas/<volume>.img` of the volume's size, plus a small progress sidecar.
- `create_replica(source = None)` creates the empty file. With `source = Some(h)`, it streams `h`'s file over the `storage` network from the recorded offset, so an interrupted copy resumes (R28).
- `ReplicaReport.copied_bytes` and `pct` are real progress. Rebuild speed is bounded by the real links and `labswitch`'s port rates. Disk choice is "most free space", as in the sim.
- Lab volumes are kept small (1–8 GB), so a rebuild takes tens of seconds, not hours.

## 7. The lab (`lab` crate, run by the server)

Everything the lab does is something done to hardware, or seen from outside it.

| Area | How |
|---|---|
| Image | Downloads the Debian 13 `genericcloud` qcow2 once to `~/.cache/homelab-lab/`. Each VM gets a qcow2 overlay and a NoCloud seed ISO (`genisoimage`) with: hostname, `.link` files, bootstrap networkd config, apt proxy, `qemu-guest-agent` and `chrony` packages, the homelabd systemd unit, and the join token (VMs 2–4 and later additions). |
| Binary | The lab serves `target/release/homelabd` over HTTP on `10.0.0.1:7480`. The guest unit fetches it in `ExecStartPre`, so a rebuild reaches a VM on its next reboot without reimaging. |
| VMs | `qemu-kvm` (q35, KVM), one process per VM, with a QMP socket and a QGA virtio-serial socket under `/run/homelab-lab/`. |
| Switches | `labswitch` (§14): `core-1` and `access-1` in one process, joined by an in-process trunk cable. Each switch port is a unix datagram socket that a VM NIC's QEMU `dgram` netdev talks to. Per port: `access`/`trunk native+allowed` VLAN handling, MTU (oversized frames dropped and counted), admin state, a token-bucket rate at the port's speed, rx/tx counters. MAC learning per VLAN. LACP groups run a switch-side 802.3ad actor, so the guest's bond really negotiates and drops a member whose LACPDUs stop. The router port is a host tap (`lab-router`) with `10.0.0.1/22` and VLAN sub-interfaces for every network's `.1`. |
| Switch management | `labswitch` answers `SwitchPoll` on each switch's management address (port 7400, same framing) from its own port table, reports carrier, speed and switch-to-switch LLDP neighbours, and **sends LLDP** out of every port every 5 s. |
| Services | `dnsmasq` for the bootstrap DHCP pool on mgmt, chrony serving time, the proxy relay, and the router's traffic sink. |

### 7.1 Faults (lab panel and API)

| Fault | Mechanism | Prototype equivalent |
|---|---|---|
| Power off (hard) | QMP `quit` (process gone; disks keep their data) | Kill host |
| Shut down (graceful) | QMP `system_powerdown` (ACPI) | new |
| Power on | Start QEMU again on the same disks | Restore host |
| Reset | QMP `system_reset` (instant reboot, no shutdown) | new |
| Freeze / Thaw | QMP `stop` / `cont` (the VM, its clock and its sockets stop dead; peers see silence) | new |
| Unplug / Replug port | QMP `set_link <nic> off/on` (guest sees carrier loss) | Unplug / Replug port |
| Fail disk / Restore disk | QMP `device_del` / `device_add` of the NVMe device | Fail disk |
| Degrade port | Lower the switch port's token-bucket rate | Degrade port |
| Partition host / group / switch | `labswitch` drops frames between ports inside and outside the group; links stay up (R10/R22 semantics) | Partition |
| Heal partitions | Clear `labswitch`'s partition groups | Heal partitions |
| Kill / Restore switch | `labswitch` stops forwarding on the switch, plus `set_link off` on the guest NICs cabled to it | Kill / Restore switch |
| Set switch port | `labswitch` port config, e.g. drop VLAN 20 from a port | Set switch port |
| Add VM | Create and boot another VM of a chosen profile with a fresh join token, optionally with a technician mistake on its switch ports (VLAN 20 missing, MTU 1500 on storage, LACP group missing) | Add hosts scenario |

Kept from the prototype: *seed workloads* (it only submits API operations, so it moves from the `sim` scenario into the server) and *iperf* (the agent runs real paced streams).

Dropped: speed control, presets, *move client*, and every sim-only scenario.

### 7.2 Ground truth

The overlay keeps its meaning: what is really true, which the control plane may not know. It is now gathered **out of band**, never over the cluster network.

| Truth | Source |
|---|---|
| VM power and run state (running / paused / off) | QMP `query-status` |
| Link, partition and switch-port state | `labswitch`'s port table and the lab's QMP link state |
| Instances actually running on each VM (split brain: one VM instance on two hosts) | qemu-guest-agent `guest-exec systemctl list-units 'homelab-inst-*'` |
| Who holds the VIP (and whether two VMs do) | qemu-guest-agent `ip -j addr`, plus `labswitch`'s MAC table for the VIP's gratuitous ARPs |

## 8. Changes to `model` and `control`

Small changes, each tested by the sim harness as well:

1. **`Inventory` gains discovered `cpu_threads` and `ram_gb`** (defaulting to the catalogue values in the sim). The join controller builds the host's `HardwareProfile` from the catalogue entry overlaid with discovered CPU, RAM, NICs and disks.
2. **IPAM skips reserved addresses.** `LogicalNetwork` gains `reserved: Vec<Ipv4Range>`, and the seed `mgmt` reserves `10.0.3.100–10.0.3.255`.
3. **`Msg::Directory { hosts: BTreeMap<HostId, BTreeMap<NetworkId, Ipv4Addr>> }`**: the leader sends it to every member every 30 s and on change. Agents ignore it, and the runtime keeps it (§5.2). The sim delivers and ignores it.
4. **`seeded_durable` moves from `sim::presets` into `control::bootstrap`,** generalised to any voter set, so the sim preset and `homelabd bootstrap` share it.
5. **`SimTime` docs:** "milliseconds; sim-time in the simulator, Unix time on real hosts". No code change.

No controller logic changes. If validation finds a controller bug, the fix lands in `control` with a regression test in the sim harness first, then is confirmed on the VMs.

## 9. Server and UI changes

- The server no longer depends on `sim`. It runs the `ClientActor` on a `node` runtime bound to `10.0.0.1` (the lab host is a client cabled to `access-1`) and the lab manager.
- The WebSocket publishes the client's view as today, plus lab truth instead of sim truth.
- Endpoints:

  | Endpoint | Change |
  |---|---|
  | `/api/ops` and `/api/metrics` | Unchanged |
  | `/api/faults` | Becomes `/api/lab/*` |
  | `/api/speed` and `/api/scenarios` | Removed, except seed workloads |

- UI:
  - The top bar shows wall-clock time and drops the speed control.
  - The fault panel becomes the **lab panel** (§7.1).
  - The fleet grid, topology, networks, workloads, drawers, alerts, events and the ground-truth overlay are unchanged apart from the truth source.
- The server runs as root (the router tap, dnsmasq, chronyd). `./lab.sh up` builds everything as the user and starts the server with sudo. `./lab.sh down` stops VMs and removes the lab's interfaces. `./lab.sh reset` also wipes VM disks.

## 10. Success criteria (validated on real VMs)

Each criterion is a step in the automated e2e run (`node e2e/lab.e2e.ts`, a script with no dependencies beyond Node 24), which talks only to the public API and the lab's fault and truth interfaces, and prints a pass/fail report with timings.

1. **Up:** `./lab.sh up` from a clean checkout ends with 4 `Ready` hosts and 3 CP voters within 10 minutes (the first image download excluded). The UI is on `localhost:8080`.
2. **Join:** *Add VM* (MS-01) boots a fifth VM that is auto-approved, templated and `Ready` with no manual step. With a technician mistake (VLAN 20 missing), it becomes `Ready`, a mismatch alert is raised, and its storage role is held inactive.
3. **Leader failover:**
   - a. Power off or freeze the leader's VM: a new leader is elected within about 2 election timeouts, the VIP moves (truth: exactly one VM holds it), and the UI shows "reconnecting" and then recovers. No controller acts without quorum.
   - b. Thaw the frozen ex-leader: it steps down and drops the VIP, which never stays on two VMs for longer than the e2e's measured bound, and it catches up.
4. **Reboot persistence:** reset a CP voter. It boots its persisted network config, its replica resumes from the on-disk log, and its agent heartbeats as a member without a token.
5. **Host failure:** power off a VM host running a VM workload. After the 60 s grace the VM restarts on another eligible host, and the volume's replica is rebuilt onto another storage host, with rebuild bytes measured on the target's storage VLAN. When the host is powered back on, its stale instances are stopped.
6. **Network:**
   - a. Unplug one bond member of an MS-01: LACP moves traffic to the other member, a "bond degraded" alert is raised, and the host stays `Ready`.
   - b. Remove VLAN 20 from that host's switch ports: a mismatch alert is raised, and its storage traffic drops to zero on the port counters.
7. **Partition:** partition an MS-01 that runs a VM. The majority marks it `not-ready` and restarts the VM elsewhere; the minority's replica cannot write. Ground truth flags the VM running twice (split brain). After healing, the stale instance is stopped and truth shows it once.
8. **Regression:** `cargo test --workspace` still passes. The sim harness covers the same controllers at 60 and 500 hosts.

## 11. Risks and how we will retire them

Each of these is spiked before building on it (phase A).

| Risk | Spike / fallback |
|---|---|
| RHEL's `qemu-kvm` build lacks a device we want (NVMe emulation, PCIe hot-unplug) | **Spiked:** no NVMe; virtio-blk with `serial=` on a `pcie-root-port` hot-unplugs fine. |
| LACP between `labswitch`'s actor and the guest bond does not negotiate, or does not drop a member on `set_link off` | Test first thing in phase B. Fallback: active-backup bonds in the lab template (validation still checks LACP groups). |
| Nested KVM (the lab host is itself a VM) is too slow or unstable | 4 small VMs are well within budget. Measure boot time and steady-state CPU in the spike. |
| Clock skew or jumps between VMs break deadline logic | **Spiked:** chrony to the lab's chronyd keeps guests within about 0.1 ms and recovers from a freeze in about 2 s (`ptp_kvm` is unavailable under nested KVM). The e2e checks skew before running faults. |
| Guest package install through the proxy relay | **Spiked:** works. |
| Running as root | Root is needed only for the `lab-router` tap, dnsmasq and the lab's chronyd. The lab only touches `lab-*` interfaces and `/run/homelab-lab`, and `lab.sh down` removes all of them. QEMU and `labswitch` would run unprivileged if needed. |

## 12. Phasing

Each phase ends with something runnable.

| Phase | Scope | Demo |
|---|---|---|
| **A. Spike** | Install `qemu-kvm`, `dnsmasq`. Boot one Debian guest with cloud-init on a VLAN bridge. Verify NVMe, hot-unplug, QMP stop/cont/set_link/reset, QGA, LACP over taps, apt through the relay. | A guest pinging the router on VLAN 20 over a bond. |
| **B. One-node cluster** | `node` crate (event loop, TCP transport, address resolution, VIP), `LinuxOs` basics (inventory, usage, Raft on disk, networkd apply/try/revert), `homelabd bootstrap`, the lab bringing up switches and `ms01-1`, the server's client over the VIP. | The UI shows one real host, `Ready`, as leader. |
| **C. Join and CP faults** | Join token flow for VMs 2–4, CP growth to 3, `Msg::Directory`, lab panel power/shutdown/reset/freeze, VM truth. e2e criteria 1, 3, 4. | Freeze the leader and watch re-election and the VIP move. |
| **D. Switches and networking** | `labswitch` management (polls and LLDP), port config edits, unplug, set switch port, partitions, *Add VM* with mistakes, IPAM reservation. e2e criteria 2, 6. | Unplug a bond member; drop VLAN 20. |
| **E. Workloads** | Instances as systemd units with real flows, replicas as files with real copy and resume, migration, router sink, QGA instance truth, seed workloads. e2e criteria 5, 7. | Power off a VM host and watch failover and rebuild bytes. |
| **F. Finish** | Remove sim controls from the UI, README, the full e2e run, and a decisions log for this phase. | `./lab.sh up` and `node e2e/lab.e2e.ts` all green. |

If scope must be cut, cut from the end. B and C are the core of "the system actually works".

## 13. Left to the builder

- Frame encoding details, queue sizes and dial timeouts.
- Whether guest-side and host-side setup shells out or uses netlink crates (shelling out first; it is easier to debug).
- Guest image details within §7.
- How instances burn CPU and touch RAM to follow their usage profile.

## 14. Phase A spike findings (2026-10-26)

| Checked | Result | Consequence |
|---|---|---|
| `qemu-kvm` 10.1 (RHEL 9) | KVM works nested; q35. No `-device nvme`. `dgram`, `stream`, `tap` netdevs available. | Disks are virtio-blk with `serial=nvmeN`. NICs use `dgram` netdevs to `labswitch`. |
| Lab host kernel | **No `CONFIG_BRIDGE_VLAN_FILTERING`, no `sch_tbf`, no Open vSwitch.** 802.1Q, bonding, veth, HTB and netem exist. | Linux bridges cannot be VLAN switches here, so the switch is userspace (`labswitch`, §7). Rate limits live in `labswitch`, not `tc`. |
| Lab host network | IPv6-only egress. Port 3128 taken, 8080 taken. | Guests reach apt through a relay on `10.0.0.1:3130` to the upstream proxy. The server keeps `PORT` (use e.g. 18080). |
| Debian 13 genericcloud | Boots to cloud-init done in about 50 s. netplan + systemd-networkd. `bonding`, `8021q` load. DMI product name, virtio serial and NIC `speed=` are visible as expected. | `LinuxOs` renders systemd-networkd files directly (netplan is left with only the bootstrap file). |
| QMP | `stop`/`cont` freeze (SSH times out), `set_link` drops guest carrier, `device_del` removes a disk, `system_reset` works. | Fault table §7.1 stands. |
| qemu-guest-agent | Installs via cloud-init, and `guest-exec` with captured output works. | Out-of-band truth (§7.2) stands. |
| Clocks | Freezing stops the guest clock (it is behind after thaw). `ptp_kvm` is unsupported nested. chrony against a lab chronyd steps it back in about 2 s. | The lab runs its own `chronyd -x` bound to `10.0.0.1`. §5.1 corrected. |

## 15. Phase D findings (2026-10-26)

Things the simulator could not have shown, found on the real VMs:

| Found | Cause | Change |
|---|---|---|
| Hosts heard no LLDP, though `tcpdump` on the guest saw it. | A NIC drops multicast it has not joined unless it is promiscuous, and `tcpdump` turns that on. LLDP goes to `01:80:c2:00:00:0e`. | `homelabd` joins that group on each NIC (`PACKET_ADD_MEMBERSHIP`), as `lldpd` does. |
| A killed switch or an admin-down port left the hosts' NICs with carrier, so no `PortDown` or `BondDegraded`. | Only *Unplug* set guest carrier (QMP `set_link`). | The lab sets each NIC's carrier from both ends of its cable: plugged in, switch powered, port admin-up. |
| *Degrade* throttled the switch port, but the host still saw 2500 Mb/s, so no `LinkSpeedLow`. | virtio NICs have no PHY to renegotiate. | *Degrade* also sets the guest NIC's speed through the guest agent (`ethtool -s`). The guest forgets it on reboot or relink, where a real NIC would renegotiate. |
| A restarted lab forgot VMs racked by *Add VM*. | The lab rebuilt only the first four from code. | Racked machines are listed in `vms/added.json` and re-racked (same names, cables and ports) at start; `lab reset` deletes the list with the disks. |

Behaviour confirmed as designed: a killed core switch raises `SwitchUnreachable`, and the hosts' port alerts name it as the cause. Killing the access switch (mgmt has only `eth0`) loses quorum, so no VM holds the VIP, and the UI shows the control plane as unavailable. It recovers in about 3 s. An isolated leader steps down within one election, and pre-vote keeps it from disrupting the leader when the partition heals.

### 15.1 Left open after the pre-commit review

- The server returns 400 rather than 500 when lab code panics. `bring_up` doesn't retry an install that failed after the root disk was created.
