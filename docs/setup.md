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
- `longhorn` is the default StorageClass (3 replicas). If a node dies, Longhorn deletes the pods stuck on it so that they restart elsewhere (`nodeDownPodDeletionPolicy`). Give your pods short `node.kubernetes.io/unreachable` and `not-ready` tolerations (for example 30 s) for a fast failover.
- Longhorn's replication runs on the storage network (VLAN 20), not the management network. Multus gives Longhorn's instance managers a second interface (`lhnet1`), a macvlan on each host's own VLAN 20 interface, with an address from the storage network's reserved range. Longhorn runs only on hosts attached to the storage network (node label `homelab.io/storage-network=true`), so a pod with a Longhorn volume must run on one of them; every MS-01 and mini PC is.
- Node labels: `homelab.io/profile=<profile>` and `homelab.io/<role>=true` for each of the host's roles (`control-plane`, `storage`, `compute`, `vm`, `transcode`), and `homelab.io/storage-network=true` on hosts attached to the storage network. Labels you set on a host in the UI are added to its node too, when Kubernetes accepts them.

## When a control-plane machine dies

The control-plane hosts are both the homelabd control plane (Raft) and the K3s servers (etcd). When one has been down for 10 minutes, the control plane raises a `CpReplacementSuggested` alert naming the best spare machine (MS-01s first). To take it, or to replace the host sooner, submit the operation the alert carries:

```sh
curl -X POST http://<server>:8080/api/ops -H 'content-type: application/json' \
  -d '{"ReplaceCpMember": {"dead": "<dead host>", "with": "<spare>"}}'
```

(`<server>` is the machine the UI's server runs on.) With `{"SetCpMode": {"mode": "Auto"}}` the control plane replaces a member down for 10 minutes without asking.

The replacement goes in etcd's recommended order: once the spare has taken the dead host's place as a control-plane voter, a running K3s server deletes the dead host's node, which removes its etcd member. Only then does the spare reinstall K3s as a server and join etcd. So etcd never counts a dead member while the spare joins. homelabd never removes a member whose node is still `Ready`, removes one at a time, and only while the members on running servers stay a quorum. `sudo homelabd status` on a server lists etcd's members.

If the dead machine comes back afterwards, it reinstalls K3s as an agent.

## Trying it out

`deploy/demo-workloads.yaml` is a small homelab-shaped mix in the `demo` namespace:
- PostgreSQL on a Longhorn volume, under a steady pgbench load;
- a writer that rewrites 64 MiB on its volume every 30 s, which keeps Longhorn replicating;
- a 3-replica web app behind K3s's ingress at `http://<vip>/`, with two clients;
- a transcode-style CPU burner on the transcode hosts.

```sh
kubectl apply -f deploy/demo-workloads.yaml      # in the lab: ./lab.sh seed, or "Seed workloads" in the lab panel
kubectl -n demo get pods -o wide
kubectl delete namespace demo                    # remove it all
```

The stateful pods tolerate an unreachable node for only 20 s, so powering off their host moves them quickly.

The UI's Workloads tab shows what Kubernetes runs: the workloads, each pod with its host, and the Longhorn volumes with where their replicas are and whether they are in sync or rebuilding. The server asks the control plane for the admin kubeconfig, then reads the Kubernetes API through the VIP every 3 s, so it needs no access to the hosts themselves.

## Upgrading homelabd

```sh
curl -fsSL <url>/install.sh | sudo sh -s -- upgrade
```

Upgrade every K3s server before relying on new Kubernetes features: each server writes the Longhorn manifest from its own version.

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
- An etcd member with no Kubernetes node (a server that died while joining) is not removed automatically: homelabd removes members by deleting their nodes. Remove it by hand with an `etcdctl` (K3s does not ship one) and the server's certificates in `/var/lib/rancher/k3s/server/tls/etcd/`.
- A host without the storage network cannot use Longhorn volumes: Longhorn does not run there.
