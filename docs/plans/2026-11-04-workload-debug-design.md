# Homelab Cluster Manager — Workload Detail, Metrics, Logs and Terminals

Date: 2026-11-04
Status: Built. Both live transports verified against the lab's K3s v1.36.5
(node-proxy summary answers; `kubectl exec` runs a shell); e2e K12–K14 cover the server's own paths.
Builds on: `2026-11-02-workload-details-design.md` (its deferred step 3, usage history, is §3 here) and `2026-11-02-ui-workload-actions-design.md`.

## 1. Intent

The Workloads tab shows health, CPU/memory and endpoints. Debugging a workload still means
leaving for kubectl. This adds what Kubernetes and the kubelet already know, and the two
interactive tools `kubectl logs` and `kubectl exec` give, plus the lab equivalent for a host
(VM console, journal, SSH terminal). Nothing new is installed in the cluster; homelabd still
writes no workloads — the UI's server acts with the admin kubeconfig, as for the existing
actions.

## 2. Richer detail (§2 of the plan: step 1)

Parsed from the Pod list the server already reads, plus two new lists (`persistentvolumeclaims`,
`nodes`) in the same 3 s read. Soft-fail like Longhorn's lists: a 404 is an empty list.

Per pod: UID, pod IP, QoS class, service account, and its containers (app containers only;
init containers are named separately). Per container: image, ready, restarts, state and reason,
requests and limits (CPU millicores, memory bytes; extended resources such as `nvidia.com/gpu`
as name→quantity maps), ports, probes present (readiness/liveness/startup), last termination,
mounts and volume devices. A mount resolves its volume: `pvc <claim>`, `configMap <name>`,
`secret <name>`, `emptyDir`, `hostPath <path>`, `projected`, else the source's kind. **Env is
never shown**: a literal env value routinely carries a password, and the manifest is one
Edit-YAML click away for whoever wants it.

Per workload: summed CPU/memory requests (over its pods' containers; none if a container
has none, mirroring limits). PVCs are listed with phase, capacity, storage class, access
modes and bound volume; nodes with allocatable CPU/memory and pressure conditions
(`MemoryPressure`, `DiskPressure`, `PIDPressure` true) — context for "how full is the host
this runs on", joining the host drawer's own metrics.

## 3. Network and filesystem metrics, with history (step 2)

metrics-server keeps no history and no network. The kubelet Summary API
(`/api/v1/nodes/<node>/proxy/stats/summary`, through the same admin kubeconfig) gives, per
pod: cumulative network rx/tx bytes, per-volume used/capacity bytes, and ephemeral storage
used bytes (pod log + container writable layers). The watcher fetches every node's summary
alongside each read, concurrently, soft-failing into `stats_error` (the `metrics_error`
precedent: the rest of the view stands).

Counters are cumulative; the server keeps the previous reading per pod **UID** and computes
rx/tx byte rates over the actual elapsed time. A counter that goes backwards (pod restarted
its sandbox) reads as no rate this round. Rates and usage land on `KubePod`
(`net_rx_bps`, `net_tx_bps`, `fs_used_bytes`) and sum onto the workload; volume usage joins
each PVC by `pvcRef` (`used_bytes` next to the claim's capacity).

History: the server keeps rings of 1,200 samples (≈1 h at the 3 s poll) keyed by
pod (`ns/name`) for CPU, memory, rx and tx. `GET /api/kube/metrics?pod=ns/name&m=…`
answers in the host metrics' shape (`{since, series: {metric: [[t, v], …]}}`), so the chart
code is reused with parameterised specs. History lives in the server process and resets
when it restarts — `since` says from when.

## 4. Logs (step 3)

`GET /api/kube/logs?namespace&pod&container&tail&previous&since_seconds&timestamps` proxies
`GET /api/v1/namespaces/{ns}/pods/{pod}/log` and returns text. `tail` defaults to 200 and is
capped at 2,000; the answer is capped at 4 MiB keeping the newest bytes (a truncated first
line is dropped). `GET /api/kube/logs/ws` upgrades to a WebSocket that relays
`follow=true` chunks as text frames; the stream ends when the client closes, the pod's log
ends, or one hour passes.

Logs are **read-only and allowed in every namespace** — debugging K3s and Longhorn
themselves is a core case, and the protected-namespace rule covers writes. Names are
validated as for actions (`valid_namespace`, `valid_name`, container as a DNS label), so
nothing reaches a URL path unchecked.

## 5. Container terminal (step 4)

`GET /api/kube/exec/ws?namespace&pod&container&shell` bridges the browser to the API
server's `pods/exec` over WebSocket (`v4.channel.k8s.io`): binary frames prefixed by a
channel byte (0 stdin, 1 stdout, 2 stderr, 3 status, 4 resize). With a TTY, stdout and
stderr are one stream. The browser speaks the same framing to the server: channel-0 binary
frames are keystrokes, text frames are `{"resize":{"width":…,"height":…}}`. The status
channel's Kubernetes `Status` (exit code or failure) is relayed as text `{"status":…}` and
the sockets close.

Exec is a **write**: the protected namespaces are refused, exactly as scale/restart/delete,
and each session is logged on the server's stdout. Shells offered: `/bin/sh`, `/bin/bash`,
`/bin/ash` — an image without one gets Kubernetes' own error back. Sessions end after
15 minutes at most, and when the browser socket closes. The TLS client for the upstream
WebSocket is built from the kubeconfig's own CA and client certificate (the `Access` the
REST paths use retains them for this). The API has no authentication (see the README's
warning, now naming terminals): exec does not raise that ceiling — Apply YAML with the
admin kubeconfig already grants it — but it makes it interactive.

## 6. VM and host debugging (step 5, lab only)

With a lab attached: `GET /api/lab/console?vm&tail` returns the tail of the VM's serial
log (`console.log`), `GET /api/lab/journal?vm&unit&lines` runs
`journalctl -u <unit> --no-pager -n <lines>` over the technician's SSH (unit allow-list:
`homelabd`, `k3s`, `k3s-agent`; lines capped at 1,000), and `GET /api/lab/terminal/ws?vm`
bridges a browser terminal to an interactive SSH login as `debian` (the image's default
user, which may sudo) under a real PTY on the server. VM names are validated as DNS labels
before they reach the lab or a path. Without a lab these answer 503, like the other
`/api/lab/*` endpoints, and the host drawer's Debug section says SSH is the way in.

A terminal to a **real** host needs a shell/journal channel in the homelabd protocol,
which has none (`ApiOp` has no such op). That is a separate design — credential scope,
audit, and what happens when the leader is not the host being debugged — and is out of
scope here. An interactive QEMU serial console is also out: the serial port is an
output-only file today, and SSH is the terminal.

## 7. Tests

- Rust: container/mount/ports/requests parsing from pod fixtures; PVC/node lists; summary
  parsing and rate maths (elapsed time, counter reset, vanished pods); log query building,
  caps and truncation; exec framing (channel encode/decode, resize, status), shells and
  namespace refusals; lab console tail bounds, journal unit allow-list and command.
- UI (vitest): byte-rate and storage formatting, mount/QoS labels, log-query building,
  exec/resize frame encoding read back as the server writes them, journal-query building.
- e2e: K12 network/volume metrics for the demo `writer`
  and `web-clients`; K13 log tail/follow/previous and an exec round-trip; K14 VM console,
  journal and SSH terminal round-trips.

Spike note: the two live transports were proven against the lab once its K3s recovered
(`kubectl get --raw /api/v1/nodes/ms01-01/proxy/stats/summary` returns per-pod network,
rootfs/logs and volume stats; `kubectl exec` into the demo `web` pods runs `/bin/sh`).
Both are still implemented behind soft-fail surfaces (`stats_error`, the exec status
frame) and covered end to end by K12–K14 against the server's own endpoints.
