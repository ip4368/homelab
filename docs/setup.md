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
- Optionally, switches that send LLDP with a management address. homelabd learns each switch's port map from it.

**Where the files come from**
- `install.sh` and the `homelabd` binary, served from one URL (`<url>` below). From a checkout: `cargo build --release -p node`, then serve `target/release/homelabd` and `crates/node/install.sh`. Replace the `@URL@` placeholder in the script with the URL, or set `HOMELAB_URL`.

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
- Hosts with the storage role give Longhorn their data disks. A host whose storage VLAN is misconfigured gives none until the network is fixed.

Machines can be joined in any order and at any time.

## Using the cluster

- `sudo homelabd status` on a host shows whether it is a member and how its Kubernetes stands.
- `sudo homelabd kubeconfig` on any member prints an admin kubeconfig that points at the VIP: its own K3s server's on a control-plane host, else the control plane's leader's (every leader runs a K3s server):
  ```sh
  ssh <a member> sudo homelabd kubeconfig > ~/.kube/config
  kubectl get nodes
  ```
- `longhorn` is the default StorageClass (3 replicas). If a node dies, Longhorn deletes the pods stuck on it so that they restart elsewhere (`nodeDownPodDeletionPolicy`). Give your pods short `node.kubernetes.io/unreachable` and `not-ready` tolerations (for example 30 s) for a fast failover.
- Node labels: `homelab.io/profile=<profile>` and `homelab.io/<role>=true` for each of the host's roles (`control-plane`, `storage`, `compute`, `vm`, `transcode`). Labels you set on a host in the UI are added to its node too, when Kubernetes accepts them.

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

## Starting over on a machine

```sh
sudo systemctl disable --now homelabd
sudo /usr/local/bin/k3s-uninstall.sh || sudo /usr/local/bin/k3s-agent-uninstall.sh
sudo rm -rf /var/lib/homelab /etc/homelab /run/homelab
```

Remove the host in the UI first, so the cluster forgets it.

## Not covered yet

- Longhorn's traffic uses the pod network (on mgmt), not the storage VLAN. Moving it to VLAN 20 needs Multus.
- ARM machines (Pi 5, RK3588): the installer is x86_64-only for now.
- Authentication: homelabd's API and its node-to-node messages are unauthenticated: anyone who can reach the management network can get a join token or an admin kubeconfig. Keep the management network trusted.
- When a dead control-plane member is replaced, its etcd member is removed only once the replacement is done and the dead host comes back or is removed.
