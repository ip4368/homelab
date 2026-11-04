# Homelab Cluster Manager — More About a Workload, Its Logs and a Terminal, and VMs as Workloads

Date: 2026-11-04
Status: Steps 1–7 built; the later items of §2 not yet. §7.4 changed when built (see there).
Builds on: `2026-11-02-workload-details-design.md` (health, usage, endpoints, the workload drawer; its step 3, usage history, is folded in here), `2026-11-02-ui-workload-actions-design.md` (writes from the UI's server, guard rails) and `2026-10-29-homelabd-k3s-responsibilities-design.md` (who owns what).

## 1. Intent

The workload drawer says whether a workload is healthy, what it uses and how to reach it. To debug one you still need kubectl: to see its logs, get a shell in it, or find out what it mounts, which devices it has and how much traffic it moves. This design adds:

1. **More about a workload** (§3): from the pod specs already read, its mounts, devices, ports, networks, probes, requests, image digests, rollout history and disruption budget.
2. **Network, disk and history** (§4): from the kubelet's stats, each pod's network rate, ephemeral storage and volume usage, and an hour of history with charts.
3. **Logs** (§5): follow a container's log, or its previous one after a crash.
4. **A terminal** (§6): a shell in a container, in a dock at the bottom of the page.
5. **VMs as workloads** (§7–§9): homelabd installs KubeVirt on hosts with the `vm` role. VirtualMachines show in the Workloads tab like any workload, with start, stop and restart, a serial console in the same terminal, and their console log in the same log viewer.

Still no authentication (§10). homelabd gains no writes to workloads: as before, the UI's server acts on the user's behalf with the admin kubeconfig, as kubectl would.

### Not in scope

- Port-forward. Endpoints (§5 of the details design) cover what is exposed; the rest can wait.
- A shell on a host. This design is about workloads. A host shell would come through homelabd's agent, or SSH, and is its own piece of work.
- Log storage or search across the cluster (Loki and the like). Logs are what the kubelet still has.
- Jobs and CronJobs as workload kinds. They fit the same view and could come next.
- VMs in Raft. A VirtualMachine is a Kubernetes object like a Deployment. homelabd's Raft never holds a workload.

## 2. Order of work

Each step is a commit (or a few) that leaves everything working, and each is useful alone:

| Step | What | New dependencies |
|---|---|---|
| 1 | More from the pod specs (§3) | none |
| 2 | Kubelet stats and usage history (§4) | none |
| 3 | Logs (§5) | none |
| 4 | Terminal (§6) | `tokio-tungstenite` (from dev to normal dependency), `@xterm/xterm` and `@xterm/addon-fit`, loaded on demand |
| 5 | KubeVirt installed by homelabd, nested virtualisation in the lab (§7) | KubeVirt's operator manifest, pinned and vendored |
| 6 | VMs in the view, their actions (§8) | none |
| 7 | Serial console, console log, New VM (§9) | none |
| later | Debug containers (§6.4), VNC (§9.4), CDI disks (§9.5), live migration on the `migration` network and VMs on VM networks (§8.3), logs merged across pods (§5.3) | `@novnc/novnc` for VNC |

Steps 1–4 and 5–7 are independent; 7 needs 4 for the terminal and 3 for logs.

## 3. More from the pod specs

The server already reads every pod and workload. These come from them at no extra request, except where noted.

### 3.1 Per container, from the workload's pod template

| What | From | Shown as |
|---|---|---|
| Image, and the digest it actually runs | `image`; `containerStatuses[].imageID` | `nginx:1.27` and a short digest. A tag that moved shows two digests across the pods |
| Requests and limits | `resources` | CPU and memory, request / limit; the pod's QoS class (`status.qosClass`) |
| Ports | `ports[]` | `http 8080/TCP`; `hostPort` flagged, since it ties the pod to a host |
| Probes | `livenessProbe`, `readinessProbe`, `startupProbe` | `HTTP GET :8080/healthz every 10 s, 3 failures`; `exec cat /tmp/ok`; `TCP :5432` |
| Environment | `env[]`, `envFrom[]` | names and where they come from: `literal`, `ConfigMap app-config`, `Secret db (password)`. **Never a value**: a literal may be a password |
| Security | `securityContext` of pod and container | `privileged`, added capabilities, `runAsUser 0`, `hostNetwork`, `hostPID` |
| Init containers and sidecars | `initContainers[]` (`restartPolicy: Always` is a sidecar) | listed with their state, so a pod stuck in `Init:1/2` says which one |

### 3.2 Mounts

Each `volumeMounts[]` entry, joined to its `volumes[]` entry:

| Volume source | Shown as |
|---|---|
| `persistentVolumeClaim` | the claim, its Longhorn volume (a link to the volumes table) and, from §4, used / size |
| `configMap`, `secret` | the name, and the keys if `items` picks some |
| `emptyDir` | `emptyDir`, `in memory` for `medium: Memory`, the size limit; used bytes from §4 |
| `hostPath` | the path and type, flagged: the pod reads or writes the host's files |
| `projected`, `downwardAPI` | their sources in a few words |

With the mount path, `readOnly` and `subPath`.

### 3.3 Devices

What a homelab app needs from the machine: a Zigbee stick, a GPU for transcoding. A pod has a device when:

- a `hostPath` volume is under `/dev` (`/dev/ttyUSB0`, `/dev/dri`);
- a container has `volumeDevices` (a raw block claim);
- a container requests an extended resource, anything but `cpu`, `memory`, `ephemeral-storage` and `hugepages-*` (`gpu.intel.com/i915: 1`, a device plugin's);
- a container is privileged (it sees every device; shown as such, not listed).

The drawer lists them, and the hosts the pods run on: a device ties a pod to the hosts that have it, which is what the `transcode` role is for.

### 3.4 Networks

Per pod: its IPs (`status.podIPs`), `hostNetwork`, and with Multus its attachments from the `k8s.v1.cni.cncf.io/network-status` annotation: network name, interface, IPs, MAC.

### 3.5 Rollout history

| Kind | Revisions from | Shown |
|---|---|---|
| Deployment | its ReplicaSets (already read), `deployment.kubernetes.io/revision` | revision, images, when, pods; the current one marked |
| StatefulSet, DaemonSet | ControllerRevisions (`/apis/apps/v1/controllerrevisions`, one more list) | revision, images, when |

Rolling back is a later write. Edit YAML can already put an older image back.

### 3.6 Disruption budget

PodDisruptionBudgets (`/apis/policy/v1/poddisruptionbudgets`, one more list) matched by selector, as Services are. The drawer shows `PDB web: 0 disruptions allowed` as a warning: it blocks homelabd's drain of the hosts the pods run on. A drain stuck on a budget can then point at the workload.

### 3.7 Where

`KubeWorkload` gains `containers: Vec<ContainerSpec>` (§3.1–§3.3 from the template), `revisions` and `pdb`. `KubePod` gains `ips`, `networks`, `qos`, `container_states` (image digest, state, restarts per container). Both stay in the WebSocket view: they change only on rollouts, and deltas are per version. The drawer gets **Containers** (per container: image, resources, ports, probes, env, mounts, devices) and **History** sections. The pods table gets an expandable row with the per-container states and networks.

## 4. Network, disk and history

### 4.1 The kubelet's stats

metrics-server gives only CPU and memory. Each node's kubelet has the rest, through the API server: `GET /api/v1/nodes/<node>/proxy/stats/summary`. Per pod:

| Field | Becomes |
|---|---|
| `network.interfaces[].rxBytes`, `txBytes` (counters) | receive and send rate per interface, and in total |
| `ephemeral-storage.usedBytes` | the container layers, logs and emptyDirs it uses |
| `volume[]` with `pvcRef`, `usedBytes`, `capacityBytes` | how full each claim is, which Longhorn's volume list does not say (Longhorn knows blocks written, not the filesystem's use) |
| `containers[].cpu`, `memory.workingSetBytes` | per container usage, where metrics-server gives per pod |

It is read every **15 s** (the kubelet refreshes its stats about that often), one request per node, on its own task beside the 3 s read so a slow or dead node does not hold the view back. A node that does not answer leaves its pods without these figures, and the drawer says which node did not answer. Counters become rates from the previous sample of the same pod; a counter that went down (the pod's sandbox was replaced) gives no rate for that sample, never a negative one.

### 4.2 History

The server keeps an hour of samples per pod (240 at 15 s): CPU, memory, receive and send rates. A workload's series is the sum of its pods' at each sample. In the server's memory only, like metrics-server's values: lost when the server restarts, which is fine for debugging.

`GET /api/kube/metrics?namespace=&kind=&name=` answers the workload's series and each pod's, in the shape of `/api/metrics` (host metrics), so `Charts.tsx` (uPlot) draws them. It is fetched when the drawer opens and every 15 s while it is open; it is not in the WebSocket view.

### 4.3 What the UI shows

- **Workloads table**: a **Network** column, `↓ 1.2 MB/s ↑ 80 kB/s`.
- **Volumes table**: a **Used** column, `7.2 / 10 GiB`, warning at 85 %.
- **Drawer**: charts of CPU, memory and network for the last hour (pods stacked), per pod network and ephemeral storage in the pods table, used bytes beside each mount (§3.2).

## 5. Logs

### 5.1 The server

`GET /api/kube/logs?namespace=&pod=&container=&previous=&tail=&since=&follow=` streams the kubelet's log through the API server's `GET /api/v1/namespaces/<ns>/pods/<pod>/log?container=&follow=&tailLines=&sinceSeconds=&previous=&timestamps=true`, as `text/plain`, chunk by chunk as it arrives (axum's body from reqwest's byte stream).

- **Timestamps are always asked for.** The UI hides them unless asked, and needs them to merge pods' logs (§5.3).
- **Defaults**: `tail=500`, `follow=true`. Without `follow`, the whole log is sent as a download (`content-disposition: attachment`).
- **No 10 s request timeout.** `Access`'s client times out whole requests; streaming uses a second client with only a connect timeout. The stream ends when the browser stops reading, and the server drops its request to Kubernetes then.
- **Every namespace.** Logs are a read, so the guard rails do not apply: the logs of K3s's, Longhorn's and KubeVirt's pods are what you need when homelabd's supervision says something is wrong.
- Errors before the stream starts (no such pod, no previous container) answer 4xx with Kubernetes' words, as the other endpoints. An error after it started ends the stream; the viewer says "stream ended" and offers to reconnect.

### 5.2 The viewer

In the terminal dock (§6.3), as a tab. Opened from a pod row (**Logs**) or the drawer's head (the first pod, or a pod picker).

- A container picker for pods with several, init containers included.
- **Previous**, offered when the container restarted: the log of the run that crashed, which is what `CrashLoopBackOff` needs.
- Follows the end, unless scrolled up; a **Jump to end** button then.
- A filter (case-insensitive text), wrapping, timestamps shown or not, **Download**.
- Keeps the last 5,000 lines. ANSI colour codes are stripped in this round.

Reading the stream is `fetch` with a `ReadableStream` reader and a `TextDecoder`; closing the tab aborts it. The line handling (split across chunks, ring buffer, strip ANSI, filter, timestamp) is a pure module, `ui/src/logs.ts`, tested with vitest.

### 5.3 All pods of a workload (later)

One stream per pod, lines prefixed with a short pod name in its own colour and merged in timestamp order (within a 1 s window, since streams arrive unevenly). `kubectl logs deploy/x` shows only one pod; this is what stern does.

## 6. A terminal

### 6.1 Server to Kubernetes

Exec runs over a WebSocket to `GET /api/v1/namespaces/<ns>/pods/<pod>/exec?container=&stdin=true&stdout=true&tty=true&command=…` with the subprotocol `v5.channel.k8s.io` (`v4.channel.k8s.io` if refused). K3s v1.36 serves exec over WebSockets. Each frame starts with a channel byte:

| Channel | Direction | Carries |
|---|---|---|
| 0 | to the pod | stdin |
| 1 | from the pod | stdout (with a tty, stderr comes here too) |
| 3 | from the pod | a `Status` when the command ends: `Success`, or `NonZeroExitCode` with the exit code |
| 4 | to the pod | resize, `{"Width":120,"Height":40}` |
| 255 | to the pod | stdin closed (v5) |

The command is `sh -c 'command -v bash >/dev/null && exec bash || exec sh'`: bash where there is one. An image without a shell (distroless) fails at once with `executable file not found`; the terminal says so and points at a debug container (§6.4).

The WebSocket client is `tokio-tungstenite` (a dev dependency for the server's tests today), with a `rustls` `ClientConfig` built from the kubeconfig's CA and client certificate (PEM parsing from `rustls-pki-types`, already in the tree through reqwest). `kube.rs` gains `Access::connect_ws(path, subprotocols)`.

### 6.2 Browser to server

`GET /api/kube/exec?namespace=&pod=&container=`, a WebSocket. The browser never sees Kubernetes' channels, so the VM console (§9.1) uses the same protocol:

| Frame | Direction | Means |
|---|---|---|
| binary | both | terminal bytes |
| text `{"resize":{"cols":120,"rows":40}}` | to the server | the terminal's size |
| text `{"exit":{"code":0}}` | to the browser | the shell ended (the server closes after it) |
| text `{"error":"…"}` | to the browser | it could not start, or Kubernetes ended it, in Kubernetes' words |

The server bridges the two sockets, one task per direction; when either side closes, it closes the other. It translates frames in pure functions (`kube_exec.rs`), tested directly, as `kube_action.rs` does for requests. At most 16 sessions at once; a 17th is refused with 503.

**Guard rails.** Exec is refused in the protected namespaces (`kube-system`, `kube-public`, `kube-node-lease`, `longhorn-system`, and `kubevirt` from §7): a shell there is a write in all but name. The server prints a line when a session opens and closes (who is not known, but what and when are), the only trail until there is authentication.

### 6.3 The dock

A panel at the bottom of the page, under every tab, with one tab per log or terminal session and a draggable height. It outlives the workload drawer, so you can watch a log while scaling. A session ends when its tab closes; a terminal whose shell ended stays readable until then.

The terminal is xterm.js (`@xterm/xterm`, `@xterm/addon-fit` to follow the panel's size). It is the UI's first large dependency, so it is loaded with a dynamic `import()` the first time a terminal opens; the rest of the UI does not grow.

### 6.4 Debug containers (later)

For images without a shell, as `kubectl debug -it <pod> --image=busybox --target=<container>`: the server adds an ephemeral container (`PATCH …/pods/<pod>/ephemeralcontainers`, fieldManager `homelab-ui`) with a pinned busybox image, `targetContainerName` (it sees the container's processes), stdin and tty; then attaches to it (`…/attach`, the same channels as exec). Ephemeral containers stay in the pod's spec until the pod is replaced; the button says so. Same guard rails as exec.

## 7. KubeVirt, installed by homelabd

### 7.1 Who owns what

Two lines for the responsibilities table:

| Area | Owner | The other side |
|---|---|---|
| Installing KubeVirt and keeping it deployed | homelabd's agent, on servers, like Longhorn and Multus | |
| VirtualMachines, their instances, disks and consoles | **KubeVirt and the user** | homelabd reads them and never writes them; the UI's server writes them on the user's behalf |

`kubevirt` joins the protected namespaces (§3 of the actions design): homelabd keeps what runs there.

### 7.2 Installing

KubeVirt has no Helm chart of its own, so K3s's `HelmChart` cannot fetch it. homelabd vendors the operator manifest of a pinned release (`crates/node/assets/kubevirt-operator-<version>.yaml`, embedded with `include_str!`, version pinned like `LONGHORN_VERSION`) and writes it to `/var/lib/rancher/k3s/server/manifests/homelab-kubevirt.yaml`, with a `KubeVirt` object:

```yaml
apiVersion: kubevirt.io/v1
kind: KubeVirt
metadata: { name: kubevirt, namespace: kubevirt }
spec:
  workloads:
    nodePlacement:
      nodeSelector: { homelab.io/vm: "true" }
```

so virt-handler, which runs the VMs, runs only on hosts with the `vm` role. The leader decides as it does for the storage network: `KubeSpec` gains `kubevirt: bool`, true once some host has the `vm` role. The manifest is not removed when the last `vm` host goes; that would delete every VM.

### 7.3 KVM

A VM needs `/dev/kvm`. KubeVirt can emulate without it, but too slowly to be useful, so homelabd does not turn that on: it shows the problem instead.

- The agent reports `kvm: bool` in `KubeStatus`.
- A host with the `vm` role and no `/dev/kvm` gets a `NoKvm` warning alert: "enable virtualisation in the firmware". Its `homelab.io/vm` label is withheld, so no VM is scheduled there.
- **The lab**: VMs already run with `-cpu host`, so a guest gets `/dev/kvm` if the lab machine allows nested virtualisation. The lab's preflight reads `/sys/module/kvm_intel/parameters/nested` (or `kvm_amd`) and, if it is off, says how to turn it on. MS-01 VMs (16 GiB, 8 vCPUs) get the `vm` role by default (`control::bootstrap`); test VMs inside them are small (1 vCPU, 1 GiB).

### 7.4 Supervision

`KubeHealth` (from servers) gains the `KubeVirt` object's `status.phase`. A `KubeVirtNotReady` warning is raised when it has not been `Deployed` for 5 min. Alert only, like Longhorn's: homelabd does not repair KubeVirt beyond keeping its manifest written.

*As built:* `KubeHealth` and the supervision of the responsibilities design are not built yet, so there is no `KubeVirtNotReady`. Instead the UI's server reads the `KubeVirt` object with the rest (`KubeView.kubevirt`), and the Workloads tab and the New VM form say how it stands. The alert comes with `KubeHealth`.

### 7.5 Draining

homelabd's drain evicts the virt-launcher pod like any other, which stops the VM, and KubeVirt starts it elsewhere if its run strategy says so. A VM with `evictionStrategy: LiveMigrate` and migratable disks moves instead (§8.3). New VM (§9.3) writes `LiveMigrateIfPossible`.

## 8. VMs in the view

### 8.1 Reading

The 3 s read adds `/apis/kubevirt.io/v1/virtualmachines` and `/virtualmachineinstances` (none when KubeVirt is not installed: a 404 is an empty list, as for Longhorn).

`WorkloadKind` gains `VirtualMachine`. `resource()` assumes `apps/v1` today; it becomes `path()`, giving the group and version too.

| Field | From |
|---|---|
| desired | 1 if the run strategy runs it (`Always`, `RerunOnFailure`, `Once`, or `Manual` with an instance), else 0 |
| ready | 1 if the instance's `Ready` condition is true |
| updated | 0 while the `RestartRequired` condition is true: a spec change waits for a restart |
| pods | the virt-launcher pod. Its owner is the `VirtualMachineInstance`, mapped to `VirtualMachine/<name>` as a ReplicaSet is to its Deployment |
| usage, limits | the launcher pod's, which include QEMU's overhead (the drawer says so) |
| images | containerDisk images |
| endpoints | Services that select the VM template's labels, which KubeVirt copies to the launcher pod; the matching in `kube_detail::endpoints` is unchanged |
| events | on the VM, its instance and the launcher pod |

Health, from `status.printableStatus` and the conditions:

| printableStatus | Health |
|---|---|
| `Stopped`, `Halted` | Idle, "stopped" |
| `Running` and Ready | Healthy (a note if `RestartRequired`: "restart to apply changes") |
| `Provisioning`, `Starting`, `WaitingForVolumeBinding`, `Migrating` | Rolling out, with the status as the reason |
| `Paused` | Degraded, "paused" |
| `ErrorUnschedulable`, `ErrImagePull`, `ImagePullBackOff`, `CrashLoopBackOff`, `DataVolumeError`, `ErrorPvcNotFound`, `Unknown` | Down, with the status and the condition's message |

The drawer gains **Guest**, from the instance: the node, the guest's OS and kernel and its IPs per interface (from the guest agent, when it runs in the guest; "no guest agent" otherwise), and **Disks**: each disk with its source (containerDisk image, claim with its Longhorn volume and §4 usage, cloud-init).

### 8.2 Actions

`KubeAction` gains `Start`, `Stop` and, for a VM, `Restart` means KubeVirt's restart:

| Action | Call |
|---|---|
| Start | `PUT /apis/subresources.kubevirt.io/v1/namespaces/<ns>/virtualmachines/<name>/start` |
| Stop | `…/stop` |
| Restart | `…/restart` |
| Delete | as for the others. The confirmation says that disks from the VM's `dataVolumeTemplates` go with it, and claims it only refers to stay |

Scale is not offered. Edit YAML works as is: the manifest endpoint finds the resource by discovery.

### 8.3 Live migration (later)

`PUT …/virtualmachines/<name>/migrate`, offered when the instance's `LiveMigratable` condition is true. That needs disks every host can open at once: a Longhorn StorageClass with `migratable: "true"`, block mode and `ReadWriteMany`. With it, homelabd's drain moves VMs instead of restarting them (§7.5).

The `vm` role already requires a `migration` network (`model::validate`). homelabd would give it a Multus attachment, as it does for the storage network, and point KubeVirt's `spec.configuration.migrations.network` at it, so migrations do not cross the management network. Likewise, VMs could join the site's VM networks (the role's other required network) through a Multus bridge attachment, rather than only the pod network. Both are later steps.

## 9. Console, console log and New VM

### 9.1 Serial console

`GET /api/kube/console?namespace=&name=`, a WebSocket in the browser protocol of §6.2. The server connects to `/apis/subresources.kubevirt.io/v1/namespaces/<ns>/virtualmachineinstances/<name>/console` with the subprotocol `plain.kubevirt.io`, whose binary frames are the serial port's bytes both ways, and bridges them. A serial port has no window size, so resize frames are dropped.

KubeVirt takes one console connection per VM at a time; a second tab shows its error. The console opens from the drawer's head (**Console**) into the dock, and uses the terminal of §6.3. The same guard rails as exec apply.

### 9.2 Console log

KubeVirt's virt-launcher pod has a `guest-console-log` container that writes the serial console to its log (on unless `disableSerialConsoleLog` is set; to check on the pinned version). **Console log** on a VM opens §5's viewer on that container, so the boot output is there even when nobody was attached. **Logs** on the launcher's `compute` container shows libvirt's and QEMU's side, for VMs that do not start.

### 9.3 New VM

A third tab in the Apply YAML dialog, which, like New from image, only writes YAML (`ui/src/newvm.ts`, a pure function, tested with vitest):

| Field | Becomes |
|---|---|
| Name, namespace | a `VirtualMachine` with `runStrategy: Always`, and the Namespace if it is new |
| Image | a `containerDisk` (default: a pinned Debian image from the containerdisks project). Its root disk resets on restart |
| CPU cores, memory | `domain.cpu.cores`, `domain.memory.guest` |
| User, SSH key, password | `cloudInitNoCloud` user data. A password lets you log in on the serial console; the form warns that it is stored in the VM's spec, readable by anyone with the kubeconfig |
| Data disk (optional) | a PVC on `longhorn` and a second disk; data that survives restarts |
| Port (optional) | a Service on the template's `app: <name>` label |
| (always) | `evictionStrategy: LiveMigrateIfPossible`; the guest agent installed by cloud-init, for §8.1's Guest section |

### 9.4 VNC (later)

`…/virtualmachineinstances/<name>/vnc`, also `plain.kubevirt.io`, carries RFB. In the browser, noVNC (`@novnc/novnc`), loaded on demand like xterm.js. For guests without a serial console: desktops, Windows.

### 9.5 CDI disks (later)

The Containerized Data Importer imports a cloud image from a URL into a Longhorn claim, so a VM's root disk persists: `dataVolumeTemplates` in the VM. homelabd would install it as it installs KubeVirt, `cdi` would become protected, and New VM would offer "root disk from URL".

## 10. Security

Nothing here adds power: the server already hands out the cluster-admin kubeconfig, with which `kubectl exec` and `virtctl console` do the same. But a shell becomes a click away, so:

- the server still listens on loopback by default, and `docs/setup.md` still recommends an SSH tunnel;
- exec and console refuse the protected namespaces, and the server logs each session;
- the drawer never shows Secret contents or environment values;
- when authentication comes, exec, console and debug containers go behind it first, before the other writes.

## 11. Tests

- **Rust unit tests** (server): mounts, devices and probes from sample pod specs (hostPath under `/dev`, `volumeDevices`, extended resources, privileged); Multus network status; PDB matching; revisions from ReplicaSets and ControllerRevisions; summary parsing and rates, a counter that went down giving no rate; history sums per workload and its one-hour bound; the log URL from a query; exec and console URLs; frame translation both ways (stdin, resize, a `Status` to an exit code, a refused command to an error); the session cap; refusals in protected namespaces; VM health for each row of §8.1, its launcher pod's owner, its endpoints; the start, stop and restart requests.
- **Rust unit tests** (node, control): the KubeVirt manifest (version, node placement); `KubeSpec.kubevirt` set once a `vm` host exists; `NoKvm` and the withheld label; `KubeVirtNotReady`.
- **UI (vitest)**: `logs.ts` (lines split across chunks, the ring, ANSI, filter, timestamps); the terminal protocol encoder; the drawer's mounts, devices and network columns; `newvm.ts`'s YAML for each combination of fields; which actions a VM row offers.
- **e2e**, new steps in `e2e/lab.e2e.ts`:
  - **K12 Logs and a shell**: apply a Deployment whose container prints a numbered line every second. `/api/kube/logs` with `tail=5` gives 5 lines, and while following, a new one arrives. Over `/api/kube/exec`, `echo $((6*7)); exit 3` prints `42` and ends with exit code 3. Exec into a `kube-system` pod is refused. Kill the container's process; after the restart, `previous=true` gives the old run's last lines.
  - **K13 Network and volume usage**: request K11's app through its Ingress in a loop; within a minute its receive rate is above zero in `/api/kube/metrics`. Write 100 MiB into a StatefulSet's Longhorn claim; the volume's used bytes grow by about that.
  - **K14 A VM**: KubeVirt reaches `Deployed`, virt-handler runs only on `vm` hosts. Apply the YAML New VM writes (containerDisk, a password); the VM is Healthy with its launcher pod on a `vm` host. Over `/api/kube/console`, send a newline and get `login:`; log in and run `uname -s`, get `Linux`. Its console log has the boot lines. Stop: Idle, and the launcher pod goes. Start: Healthy again. Delete.

## 12. Open questions

1. **History length.** An hour at 15 s, as host metrics keep. Longer would want the samples on disk, which the server has no place for yet.
2. **VM role and KVM on real machines.** The `vm` role asks for 16 GB of RAM today. Should it also need a KVM-capable CPU in the hardware profile (Pi 5s and RK3588s have KVM too, but for arm64 guests only)?
3. **KubeVirt version.** Pinned to the stable release current when step 5 is built; it must support Kubernetes v1.36.
