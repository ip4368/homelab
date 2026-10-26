# Real VM Lab — Implementation Plan

Design: `2026-10-26-real-vm-lab-design.md`. Each task ends green on `cargo test --workspace`. Lab-only behaviour, which needs root and KVM, is checked by the e2e run instead.

## Interfaces fixed up front

- **Wire frame** (TCP, port 7400): a `u32` big-endian length, then JSON `Frame { from: Endpoint, to: Endpoint, msg: Msg }`. The first frame on a connection may be `Frame` with `msg = Ping(0)` as a hello, so the acceptor learns the dialler's endpoint before any real traffic.
- **Node config** `/etc/homelab/node.json`, written by cloud-init: `{ host: HostId, token: Option<String>, vip: Ipv4Addr, switches: {SwitchId: Ipv4Addr}, mgmt_nic: "eth0" }`.
- **Paths in a guest:** `/var/lib/homelab/{joined,net.json,directory.json,raft/}` and `/run/homelab/net.json`. Replicas go under `<disk mount>/homelab/replicas/`, with disks mounted at `/mnt/<name>`.
- **labswitch control:** an in-process API (`LabSwitch` handle) used by the lab manager. The management protocol towards the control plane is the node wire frame on `<switch mgmt ip>:7400`, served through the router tap.

## Phase B: one-node cluster

| # | Task | Done when |
|---|---|---|
| B1 | **model:** `Inventory.{cpu_threads, ram_gb}` (serde default 0 = catalogue), `HardwareProfile::with_discovered(&Inventory)`; the join controller and seeding use it. `LogicalNetwork.reserved: Vec<(Ipv4Addr, Ipv4Addr)>` (serde default), skipped by IPAM; seed `mgmt` reserves `10.0.3.100–10.0.3.255`. Regenerate the ts-rs types. | Unit tests for discovered capacity and reservation; sim tests green. |
| B2 | **control::bootstrap:** `seeded_durable(hosts: &[SeedHost], voters)` moved from `sim::presets`. `SeedHost.running: Option<HostNetworkConfig>`: `Some` = already runs it (sim), `None` = provisioned with IPAM addresses, and the network controller pushes the template (real). | Sim presets use it; a unit test for the `None` path replays to a Provisioning host with IPs. |
| B3 | **control:** `Msg::Directory`, sent by the leader to every member agent every 30 s and when an address changes (in the controller loop, through `Volatile`). Agents ignore it. | Unit test in `replica.rs`. |
| B4 | **node::runtime:** event loop owning actors, monotonic timers, wall-clock `now`, lossy per-peer TCP transport, address book (VIP, switches, connections, local replica state, directory), and VIP hook (add/remove address plus gratuitous ARP through a `VipControl` trait). | Two runtimes on loopback exchange `Ping`/`Pong`. A replica plus agent on one runtime with `FakeOs` elects itself and claims the VIP through a fake `VipControl`. |
| B5 | **node::linux:** `LinuxOs` covering inventory, usage, Raft store, `mark_joined`, and systemd-networkd render/apply/persist. Rendering is a pure function with tests. | Render tests (bond LACP, VLANs, MTU, static IP, DHCP bootstrap); Raft store round-trip test in a temp dir. |
| B6 | **homelabd:** `homelabd run` and `homelabd bootstrap`. | Builds; `--help`. |
| B7 | **lab::switch:** `labswitch` with unix-dgram ports, VLAN access/trunk, MTU, MAC learning, admin state, token bucket, counters, router tap port, inter-switch cable, partition groups. The LACP actor and LLDP come in D. | Unit tests over an in-memory port backend: VLAN tagging and stripping, drops on a disallowed VLAN or oversized frame, learning, partitions. |
| B8 | **lab::vm:** cloud-init seed, overlay disks, QEMU launch, QMP client, QGA client. **lab::services:** dnsmasq, chronyd, proxy relay, the HTTP server for the homelabd binary, and the router tap. | Seed rendering tests; manual boot of `ms01-1`. |
| B9 | **server:** `LabDriver` (client runtime on `10.0.0.1`, lab manager) replaces the sim driver. The WebSocket publishes `LabTruth`. `/api/lab/*` endpoints. The UI top bar shows a wall clock. | Server tests adapted; the UI shows `ms01-1` Ready and leader. |
| B10 | `lab.sh up/down/reset`. | One command brings up phase B's demo. |

## Phase C: join and control-plane faults

| # | Task | Done when |
|---|---|---|
| C1 | Lab orchestration: once the bootstrap host is up, ask for a join token through the API, then boot `ms01-2`, `ms01-3` and `minipc-1` with it. | 4 hosts Ready, 3 voters. |
| C2 | Lab faults: power off, shut down, power on, reset, freeze, thaw. **LabPanel** UI replaces FaultPanel. | Buttons work on the real VMs. |
| C3 | `LabTruth`: VM run state (QMP), VIP holders and instances (QGA), CP role from the leader's view. The overlay is wired up. | The overlay shows a frozen leader as "paused". |
| C4 | `lab e2e` harness with criteria 1, 3a, 3b and 4. | Green on the real lab. |

## Phase D: switches and networking

| # | Task |
|---|---|
| D1 | LLDP sent by `labswitch` and heard by `LinuxOs::lldp` (`AF_PACKET`). |
| D2 | Switch management: `SwitchPoll` → `SwitchReport` from `labswitch`'s port table, with switch-to-switch neighbours. |
| D3 | LACP actor in `labswitch`; bond-member unplug (QMP `set_link`); disk fail and restore (QMP `device_del`/`device_add`). |
| D4 | Set switch port, partitions, kill/restore switch, degrade port from the lab panel. *Add VM* with technician mistakes. |
| D5 | e2e criteria 2, 6a, 6b. |

## Phase E: workloads

| # | Task |
|---|---|
| E1 | `homelabd instance`: a systemd transient unit with CPU and RAM along its usage profile, and real paced TCP flows. A homelabd data service on each host's networks: flow sink, replica write and read, migration pull. A router sink in the lab. |
| E2 | Replicas as files: create, delete, report, copy from a source with resume. |
| E3 | Live migration: pull `ram_mb` from the source, start, then stop the source; deadlines and abort. |
| E4 | QGA instance truth and split-brain alerts in `LabTruth`. Seed workloads in the server. |
| E5 | e2e criteria 5 and 7. |

## Phase F: finish

Remove the sim-only UI (speed, presets, scenarios, move client). Update the README. Run the full e2e. Write a decisions log (L-rulings) for this phase.
