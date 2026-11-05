# Setting up a cluster

This guide covers installing the homelab cluster on real machines: homelabd, K3s and Longhorn. The VM lab (`./lab.sh up`) follows these same steps, run by its "technician" over SSH, so every lab run checks this guide.

## What you need

**Machines**
- x86_64, with Debian 13 installed (a minimal or cloud install is enough) and SSH access.
- For control-plane hosts (the first three eligible machines; MS-01s are preferred): at least 8 GB RAM.
- For storage hosts: at least one data disk besides the OS disk. homelabd formats a blank data disk (ext4) and mounts it at `/mnt/<name>`, and Longhorn stores volume replicas there. A disk that already holds data is mounted as it is.
- Stable NIC names. The network templates name ports (`eth0`, `sfp0`, `sfp1`…). Pin those names with systemd `.link` files (match on MAC), the same way the lab's image does.

**Network**
- A **management network** (the seed config uses `mgmt` = `10.0.0.0/22`, untagged) with a router at its `.1` that provides internet access and DNS.
- **DHCP** on the management network for machines that have not joined yet. The DHCP pool must sit inside the management network's **reserved range** (the seed config reserves `10.0.3.100–10.0.3.255`), so that the static addresses homelabd hands out never collide with it.
- **One free address for the control plane's VIP**, also inside the reserved range (the lab uses `10.0.3.253`). Whichever host leads the control plane holds it. The UI, `homelabd`, joining machines and the Kubernetes API (`https://<vip>:6443`) all use it.
- Switch ports configured for each machine's template before it is plugged in. For an MS-01: `eth0` access on mgmt; `sfp0`+`sfp1` in an LACP group, trunking VLANs 20 (storage), 30 (migration), 100 and 110 (VMs), MTU 9000 or more. The control plane checks each port against its template and flags mistakes.
- A **storage network** (the seed config uses `storage` = `10.20.0.0/22` on VLAN 20) that the storage hosts reach each other on: Longhorn replicates over it. Its **reserved range** (the seed config reserves `10.20.3.0–10.20.3.255`) holds the addresses Longhorn's pods take there, so no host is given one. Nothing else may use that range.
- Optionally, switches that send LLDP with a management address. homelabd learns each switch's port map from it.

**Where the files come from**
- `install.sh`, the `homelabd` binary and its sha256, served from one URL (`<url>` below). From a checkout: `cargo build --release -p node`, then `(cd target/release && sha256sum homelabd > homelabd.sha256)`, and serve `homelabd`, `homelabd.sha256` and `crates/node/install.sh`. Replace the `@URL@` placeholder in the script with the URL, or set `HOMELAB_URL`.
- `install.sh` runs only a `homelabd` that matches `homelabd.sha256`. The sum comes from the same URL as the binary, so it catches a corrupt or swapped file there, not someone who controls the URL: serve it over HTTPS, or publish the sum elsewhere and pass it as `HOMELAB_SHA256=<sum>` (`curl -fsSL <url>/install.sh | sudo HOMELAB_SHA256=<sum> sh -s -- ...`).
- K3s comes from its GitHub release. homelabd checks the install script and the binary against sha256 sums pinned in the build, so a machine needs to reach `raw.githubusercontent.com`, `github.com` and `release-assets.githubusercontent.com`.

The seed configuration is the one the lab uses: networks, templates, and auto-approve rules per hardware profile. If your subnets differ, change them in the UI after `init`, before joining the other machines.

## The first machine

```sh
curl -fsSL <url>/install.sh | sudo sh -s -- init --vip 10.0.3.253
```

This installs homelabd as a systemd service and creates a cluster of one with this machine as its control plane. Then homelabd:
1. moves the machine from its DHCP address to its static management address, and builds its bonds and VLANs from its template. **An SSH session to the old address drops.** Find the new address in the UI or the router's ARP table;
2. installs a K3s server (`--cluster-init`, with the VIP in its certificate);
3. deploys Longhorn through K3s, and gives it the machine's data disks.

The command prints the join command for the other machines. Its token is valid for 24 hours.

## Every other machine

```sh
curl -fsSL <url>/install.sh | sudo sh -s -- join 10.0.3.253 <token>
```

For a fresh token, run `sudo homelabd token` on any member or click *Create join token* in the UI.

The machine asks to join. If an auto-approve rule matches its hardware it is approved straight away; otherwise it waits under *Pending joins* in the UI. Once approved, it moves to its static address on its template and joins Kubernetes:
- Control-plane hosts join as **K3s servers** (embedded etcd). homelabd keeps 3 control-plane members, or 5 above 50 hosts, and prefers MS-01s.
- Every other host joins as a **K3s agent**.
- Hosts with the storage role give Longhorn their data disks. A host whose storage VLAN is misconfigured gives none until the network is fixed: if it stored already, Longhorn stops placing replicas on its disks until then.

Machines can be joined in any order and at any time.

## Using the cluster

- `sudo homelabd status` on a host shows whether it is a member and how its Kubernetes stands.
- `sudo homelabd kubeconfig` on any member prints an admin kubeconfig that points at the VIP: its own K3s server's on a control-plane host, else the control plane's leader's (every leader runs a K3s server):
  ```sh
  ssh <a member> sudo homelabd kubeconfig > ~/.kube/config
  kubectl get nodes
  ```
  The UI's server hands out the same kubeconfig, so you need no SSH to a member:
  ```sh
  curl -s http://<server>:8080/api/lab/kubeconfig > ~/.kube/homelab
  export KUBECONFIG=~/.kube/homelab
  ```
  The kubeconfig reaches the cluster at the VIP, so it keeps working when the leader fails over. Your machine must be on the management network to use it. The lab host is (as `10.0.0.1`), so in the lab this works as it stands. From any other machine, forward the API over SSH and point the kubeconfig at the tunnel (the API's certificate is valid for `127.0.0.1`):
  ```sh
  ssh -N -L 6443:10.0.3.253:6443 <lab host or a member> &
  kubectl --server https://127.0.0.1:6443 get nodes   # or edit the kubeconfig's server: line
  ```
  In the lab, `./lab.sh kubectl …` also works without a kubeconfig: it runs `k3s kubectl` on a control-plane VM over SSH. A `-f <file>` there names a file on the VM, so pipe local files in: `./lab.sh kubectl apply -f - < app.yaml`.

  The kubeconfig is cluster-admin, and anyone who can reach the server or the management network can fetch it (see [Not covered yet](#not-covered-yet)).
- `longhorn` is the default StorageClass (3 replicas). If a node dies, Longhorn deletes the pods stuck on it so that they restart elsewhere (`nodeDownPodDeletionPolicy`). Give your pods short `node.kubernetes.io/unreachable` and `not-ready` tolerations (for example 30 s) for a fast failover.
- Longhorn's replication runs on the storage network (VLAN 20), not the management network. Multus gives Longhorn's instance managers a second interface (`lhnet1`), a macvlan on each host's own VLAN 20 interface, with an address from the storage network's reserved range. Longhorn runs only on hosts attached to the storage network (node label `homelab.io/storage-network=true`), so a pod with a Longhorn volume must run on one of them; every MS-01 and mini PC is.
- Node labels: `homelab.io/profile=<profile>` and `homelab.io/<role>=true` for each of the host's roles (`control-plane`, `storage`, `compute`, `vm`, `transcode`), and `homelab.io/storage-network=true` on hosts attached to the storage network. Labels you set on a host in the UI are added to its node too, when Kubernetes accepts them.

## Moving the control plane to another machine

The control-plane role is not set by hand. It follows the control-plane voters, so the role checkbox is read-only and the API refuses changes to it. To move a live control-plane host's place to another machine (to retire or service the host), open either host in the UI. Its **Control plane** section offers **Hand off** on a voter and **Take over** on a Ready, eligible machine that is not a voter. Or submit:

```sh
curl -X POST http://<server>:8080/api/ops -H 'content-type: application/json' \
  -d '{"HandOffCp": {"from": "<voter>", "to": "<machine>"}}'
```

The new machine joins the control plane, is promoted to a voter, and reinstalls K3s as a server. Only once that server runs does the old voter leave the control plane, so etcd never runs a member short. The old voter then reinstalls K3s as an agent, deleting its own node first so its etcd member leaves, and stays in the fleet. If the old voter leads the control plane, it steps down and the VIP moves to the new leader. The control plane refuses a hand-off while a voter is down (replace it first) or while another membership change is in progress. It gives up a hand-off, saying so in the events, if the new machine goes down before it joins.

If the new machine's K3s server never starts, the hand-off waits with an extra voter. Cancel it with **Cancel hand-off** in the hand-off's progress (**Show progress** in either host's **Control plane** section), or submit `"CancelCpHandoff"` to `/api/ops`. The new machine leaves the control plane again and reinstalls K3s as an agent, and the old voter keeps its place. Once the old voter has left, the hand-off can no longer be cancelled.

## Moving the VIP to another control-plane host

The VIP follows the control-plane leader. To move it to another voter (say the leader's K3s API is down, or you want to service the leader without a failover), open that voter in the UI and click **Move the VIP here** in its **Control plane** section. Or submit:

```sh
curl -X POST http://<server>:8080/api/ops -H 'content-type: application/json' \
  -d '{"TransferCpLeadership": {"to": "<voter>"}}'
```

The leader sends the voter whatever log it lacks, then tells it to run for leader at once. The voter wins in one election, takes the VIP, and the old leader becomes a follower, normally within a second. While this happens the leader answers other operations `NotLeader`, which clients retry through the VIP. An operation sent in the few tens of milliseconds while the VIP moves between machines can be lost, as with any change of leader. The UI then says the control plane is unavailable after 5 s, and the operation may or may not have been applied, so check before submitting it again. The leader refuses a voter that has not answered in the last 1.5 s, or one whose homelabd predates moving the VIP (upgrade it first). If the voter has not taken over within 1.5 s, the leader keeps the VIP and carries on, and the UI says so.

## When a control-plane machine dies

The control-plane hosts are both the homelabd control plane (Raft) and the K3s servers (etcd). A dead voter is replaced, not handed off: the control plane refuses hand-offs while a voter is down. When one has been down for 10 minutes, the control plane raises a `CpReplacementSuggested` alert naming the best spare machine (MS-01s first), with a **Replace with \<spare\>** button. To replace it sooner, or with another machine, open the dead host in the UI and click **Replace…** in its **Control plane** section (or **Replace \<voter\>…** on the spare). Or submit the operation the alert carries:

```sh
curl -X POST http://<server>:8080/api/ops -H 'content-type: application/json' \
  -d '{"ReplaceCpMember": {"dead": "<dead host>", "with": "<spare>"}}'
```

(`<server>` is the machine the UI's server runs on.) With `{"SetCpMode": {"mode": "Auto"}}` the control plane replaces a member down for 10 minutes without asking.

The replacement goes in etcd's recommended order: once the spare has taken the dead host's place as a control-plane voter, a running K3s server deletes the dead host's node, which removes its etcd member. Only then does the spare reinstall K3s as a server and join etcd. So etcd never counts a dead member while the spare joins. homelabd never removes a member whose node is still `Ready`, removes one at a time, and only while the members on running servers stay a quorum. A member K3s cannot find by a node goes through etcd's own API instead. That covers a dead server whose node was already deleted, and a server that died while joining, whose member never started and so has no name yet (it is found by its address). `sudo homelabd status` on a server lists etcd's members. If no server can read them, nothing is removed and no new server joins until one can, and the host's Kubernetes line in the UI says why.

If the dead machine comes back afterwards, it reinstalls K3s as an agent.

A server that reinstalls as an agent deletes its own node first, so its etcd member leaves. If that delete fails, the member stays behind with nothing to run it, and etcd loses a spare: with 4 members and 3 running, one more failure stops it. homelabd removes such a member through etcd's API (the node, now the agent's, stays), with the same one-at-a-time and quorum rules, and no new server joins until it is gone.

## Trying it out

`deploy/demo-workloads.yaml` is a small homelab-shaped mix in the `demo` namespace:
- PostgreSQL on a Longhorn volume, under a steady pgbench load;
- a writer that rewrites 64 MiB on its volume every 30 s, which keeps Longhorn replicating;
- a 3-replica web app behind K3s's ingress at `http://<vip>/`, with two clients;
- a transcode-style CPU burner on the transcode hosts.

```sh
kubectl apply -f deploy/demo-workloads.yaml      # in the lab: ./lab.sh seed, or "Seed workloads" on the Lab page
kubectl -n demo get pods -o wide
kubectl delete namespace demo                    # remove it all
```

The stateful pods tolerate an unreachable node for only 20 s, so powering off their host moves them quickly.

The UI's Workloads page shows what Kubernetes runs: the workloads, each pod with its host, and the Longhorn volumes with where their replicas are and whether they are in sync or rebuilding. The server asks the control plane for the admin kubeconfig, then reads the Kubernetes API through the VIP every 3 s, so it needs no access to the hosts themselves.

Each workload's row also shows:
- **Health**, with the reason when it is not healthy: down, degraded, rolling out (`1/3 updated`) or idle (scaled to 0). The reason is the stuck pods' (`CrashLoopBackOff`, `ImagePullBackOff`, `Unschedulable: 0/4 nodes are available…`) or the controller's (`ReplicaFailure`, a rollout past its deadline). A healthy workload whose pod last restarted for `OOMKilled` or `Error` says so.
- **CPU and memory**, from metrics-server (K3s ships it), summed over its pods and shown against their limits when every container has one. Without metrics-server the page says why and shows the rest.
- **Network**: what its pods receive and send, from each node's kubelet (`/api/v1/nodes/<node>/proxy/stats/summary`, read every 15 s). A node whose kubelet does not answer is named above the table.
- **Endpoints**: an Ingress rule as `http://<VIP or host><path>`, a `LoadBalancer` Service as `<VIP>:<port>` (K3s's ServiceLB answers on every node, the VIP's holder too), a `NodePort` as `<VIP>:<nodePort>`, and a `ClusterIP` Service as `<name>.<namespace>:<port>`, greyed because only pods reach it. Web ones are links.

The Longhorn volumes table shows how full each volume's filesystem is, as the kubelet of a pod using it sees it (Longhorn itself knows only the blocks written).

Click a workload's row to open it in the detail pane beside the list, in tabs. **Overview**: health and the conditions that are not as they should be, a PodDisruptionBudget that allows no eviction now (draining its pods' hosts waits for it), usage with an hour of CPU, memory and network charts (kept in the server's memory, so they start again when it restarts), every endpoint and its Longhorn volumes. **Containers**: each container's image and the digests its pods run (two digests mean a tag moved), requests and limits, ports, probes, environment variables by name and source (never their values), mounts with what each volume is, devices (host paths under `/dev`, raw block claims, device plugins' resources, privilege) and what it may do beyond a plain container. **Pods**: each pod with its host, why it is as it is (the scheduler's message, how a container last ended), restarts, usage and network; expand a pod for each container's state and digest, its IPs, Multus attachments and how much of its node's disk it uses. **History**: its rollout history (a Deployment's ReplicaSets, a StatefulSet's or DaemonSet's ControllerRevisions). **Events**: the Warning events of the last hour on it, its ReplicaSets and its pods.

**Logs** on a pod's row (or a workload's, for its first pod) opens its log in a dock at the bottom of the page: the last 500 lines, then new ones as they come. It follows the end unless you scroll up, filters, wraps and shows timestamps, and keeps the last 5000 lines; pick another container, or **Previous run** for the log of the run before the last restart (what a `CrashLoopBackOff` needs). **Download** saves the whole log. Logs can be read in every namespace, K3s's and Longhorn's included. The dock keeps its tabs while you move around the page, and hidden ones keep following.

**Shell** on a running pod's row opens a terminal in the dock: bash in the container if it has it, else sh, as `kubectl exec -it` would. An image without a shell says so. There is no shell in the namespaces the UI does not change (below): use `kubectl exec` there. The server logs each terminal's opening and closing, and keeps at most 16 open.

```sh
curl 'http://<server>:8080/api/kube/logs?namespace=demo&pod=web-6d4-x2&tail=100'       # follows; add &follow=false for the log so far
curl 'http://<server>:8080/api/kube/metrics?namespace=demo&kind=Deployment&name=web'   # the last hour, as /api/metrics
```

A workload can also be changed, as kubectl would, from its row's **⋯** menu or the toolbar of its pane: **−** and **+** beside its ready count, then **Scale to N** (Deployments and StatefulSets), **Restart** its pods a few at a time (`kubectl rollout restart`), or **Delete** it. Deleting a StatefulSet keeps its volume claims and their data; delete those with kubectl once you no longer need them. Nothing in `kube-system`, `kube-public`, `kube-node-lease`, `longhorn-system` or `kubevirt` can be changed from the UI: K3s, Longhorn and KubeVirt run there, installed by homelabd, so use kubectl. The same changes can be scripted:

```sh
curl -X POST http://<server>:8080/api/kube -H 'content-type: application/json' \
  -d '{"Scale": {"workload": {"namespace": "demo", "kind": "Deployment", "name": "web"}, "replicas": 5}}'
```

(`Restart` and `Delete` take just `{"workload": …}`.) A later `kubectl apply` of a manifest that sets `replicas:` sets it back, as after `kubectl scale`.

**New / Apply YAML** above the list takes one or more manifests (`---` between them), as `kubectl apply --server-side` does. **Preview** checks them with the API's dry run and says what each object would become: created, changed or unchanged, or Kubernetes' reason for refusing it. **Apply** is offered once a preview of exactly that YAML found no problem. Namespaces are applied first; objects that name no namespace go in the one the dialog names (`default` if none). The same namespaces as above are refused, and so is anything cluster-wide but a Namespace (Nodes, CRDs, ClusterRoles…): use kubectl for those.

Its **New from image** tab writes the YAML for a typical app: a name, an image and replicas; optionally the port it serves on (a Service), a web path (an Ingress on K3s's Traefik at `http://<VIP><path>`; the app sees the path as it is), a Longhorn volume per pod (then a StatefulSet, on hosts on the storage network) and roles its hosts must all have (the form names the hosts that do). Its pods leave a dead host after 20 s. **Write YAML** puts the manifests in the YAML tab, to preview and apply like any other, or to edit first (environment variables, resources and probes are not on the form).

If another tool set a field the YAML sets to another value (plain `kubectl apply`, `kubectl edit`, a Scale from this page), the preview names the field and who set it. Tick **take over** and preview again to apply anyway (`--force-conflicts`). **Edit YAML** (a workload's **⋯** menu or pane) opens its live manifest in the same dialog, without status and bookkeeping, with **take over** already ticked: like `kubectl edit`, what you change is set, whoever set it before.

```sh
curl -X POST http://<server>:8080/api/kube/apply -H 'content-type: application/json' \
  -d "$(jq -n --rawfile yaml app.yaml '{yaml: $yaml, namespace: "demo", dry_run: true, force: false}')"
curl 'http://<server>:8080/api/kube/manifest?namespace=demo&kind=Deployment&name=web'
```

### VMs

Once a host has the vm role (every MS-01 does), homelabd's K3s servers install KubeVirt (v1.9.1, its operator manifest vendored in homelabd). It runs VMs only on the vm hosts. A VM needs `/dev/kvm`: a vm host without it gets a `NoKvm` alert and loses its `homelab.io/vm` label until it has it, so turn on virtualisation in its firmware. In the lab, the VMs have `/dev/kvm` when the lab machine allows nested virtualisation; `./lab.sh up` warns when it does not. The Workloads page says how KubeVirt stands above its list.

A VirtualMachine shows on the Workloads page like any workload: its health from KubeVirt's status (running, stopped, starting, or why not, such as `ErrorUnschedulable`), its launcher pod's usage, its endpoints (Services that select its template's labels), and in its pane's **Guest** tab its guest (OS, kernel and addresses from the guest agent, when the guest runs one), its disks and whether it can move to another host while it runs. **Start**, **Stop** and **Restart** (its row's **⋯** menu or its pane) do what `virtctl` does. **Console** in its pane opens its serial console in the dock (KubeVirt allows one connection at a time), and **Console log** what the console printed, even with nobody attached. A drained host's VMs restart on another vm host, or move while running if their disks allow it.

Apply YAML's **New VM** tab writes a VM: a name, a containerDisk image (its root disk, reset on every restart; Debian 13 by default), cores and memory, a user with an SSH key or a password (to log in on the console; it is stored in the VM's spec), optionally a Longhorn data disk that survives restarts and a port for a Service. The guest agent is installed by cloud-init.

```sh
curl -X POST http://<server>:8080/api/kube -H 'content-type: application/json' \
  -d '{"Stop": {"workload": {"namespace": "demo", "kind": "VirtualMachine", "name": "deb"}}}'
```

## Upgrading homelabd

```sh
curl -fsSL <url>/install.sh | sudo sh -s -- upgrade
```

Upgrade every K3s server before relying on new Kubernetes features: each server writes the Longhorn and KubeVirt manifests from its own version.

Until a voter runs a homelabd that can take over by a VIP move, the leader refuses to move the VIP to it. Upgrading voters one at a time, leader last, works as before: the leader's restart is an ordinary failover.

On a cluster created before Longhorn moved to the storage network, the storage network has no reserved range, so Longhorn stays on the pod network. To move it, give the storage network a reserved range that no host uses, as a network change rollout (for the seed network):

```sh
curl -X POST http://<server>:8080/api/ops -H 'content-type: application/json' -d '{"StartRollout": {"change": {"Network":
  {"id": "storage", "purpose": "Storage", "vlan": 20, "subnet": "10.20.0.0/22", "mtu": 9000,
   "reserved": [{"first": "10.20.3.0", "last": "10.20.3.255"}], "dns": []}}, "include_overridden": false}}'
```

Longhorn switches only while no volume is attached, so scale down the workloads that use volumes until its instance managers have restarted with `lhnet1`.

## Starting over on a machine

```sh
sudo systemctl disable --now homelabd
sudo /usr/local/bin/k3s-uninstall.sh || sudo /usr/local/bin/k3s-agent-uninstall.sh
sudo rm -rf /var/lib/homelab /etc/homelab /run/homelab /etc/cni/multus
```

Remove the host in the UI first, so the cluster forgets it.

## Not covered yet

- ARM machines (Pi 5, RK3588): the installer is x86_64-only for now.
- Authentication: homelabd's API and its node-to-node messages are unauthenticated: anyone who can reach the management network can get a join token or an admin kubeconfig. Keep the management network trusted.
- An unstarted etcd member (a server that died while joining) whose machine was then removed from the cluster is not removed automatically, since homelabd no longer knows the address to find it by. Remove it by hand with an `etcdctl` (K3s does not ship one) and the server's certificates in `/var/lib/rancher/k3s/server/tls/etcd/`.
- A host without the storage network cannot use Longhorn volumes: Longhorn does not run there.
