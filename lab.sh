#!/usr/bin/env bash
# The real VM lab, in one command (needs Linux with KVM, sudo, qemu-kvm,
# dnsmasq, chrony, genisoimage, ssh; see the README). `up` installs Debian on
# new VMs and has a technician run docs/setup.md's install commands on them
# over SSH: homelabd, then K3s and Longhorn.
#
#   ./lab.sh up        build everything and run the server (and the lab) as root
#   ./lab.sh restart   build, then replace a running server: the switches live in the
#                      server, so stopping it first would cut the whole lab off for the build
#   ./lab.sh down      power off every VM and stop the lab's daemons
#   ./lab.sh reset     like down, and delete the VMs' disks (the next up installs a fresh cluster)
#   ./lab.sh ssh VM    log into a VM (by its lab name, e.g. ms01-02)
#   ./lab.sh kubectl ARGS...   kubectl on a K3s server, e.g. ./lab.sh kubectl get pods -A -o wide
#   ./lab.sh seed      apply deploy/demo-workloads.yaml (a database, a writer, a web app, a CPU burner)
#
# PORT sets the UI's port (default 8080). BIND sets the address it listens on
# (default 127.0.0.1; e.g. BIND=0.0.0.0 to reach it from other machines, but
# note the API has no authentication and can power off the VMs).
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$HOME/.cargo/bin:$PATH"
STATE=${LAB_STATE:-/var/lib/homelab-lab}

build() {
  if [ ! -f ui/node_modules/.package-lock.json ] || [ ui/package-lock.json -nt ui/node_modules/.package-lock.json ]; then
    (cd ui && npm ci)
  fi
  (cd ui && npm run build)
  cargo build --release -p node -p lab -p server
}

case "${1:-}" in
  up)
    build
    echo "http://${BIND:-localhost}:${PORT:-8080}"
    exec sudo -E env "PATH=$PATH" "PORT=${PORT:-8080}" "BIND=${BIND:-127.0.0.1}" ./target/release/server
    ;;
  restart)
    build
    sudo pkill -TERM -f 'target/release/server$' || true
    while pgrep -f 'target/release/server$' >/dev/null; do sleep 0.2; done
    echo "http://${BIND:-localhost}:${PORT:-8080}"
    exec sudo -E env "PATH=$PATH" "PORT=${PORT:-8080}" "BIND=${BIND:-127.0.0.1}" ./target/release/server
    ;;
  down)
    cargo build --release -p lab
    sudo -E ./target/release/lab down
    ;;
  reset)
    cargo build --release -p lab
    sudo -E ./target/release/lab reset
    ;;
  ssh)
    vm=${2:?usage: ./lab.sh ssh VM [command]}
    shift 2
    ip=$(sudo -E ./target/release/lab ip "$vm")
    exec sudo ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "$STATE/id_ed25519" "debian@$ip" "$@"
    ;;
  kubectl)
    shift
    args=$(printf ' %q' "$@")
    # Any control-plane host will do: the first one that answers.
    for vm in ms01-01 ms01-02 ms01-03; do
      ip=$(sudo -E ./target/release/lab ip "$vm" 2>/dev/null) || continue
      exec sudo ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -i "$STATE/id_ed25519" "debian@$ip" "sudo k3s kubectl$args"
    done
    echo "lab.sh: no control-plane host answered" >&2
    exit 1
    ;;
  seed)
    "$0" kubectl apply -f - < deploy/demo-workloads.yaml
    ;;
  *)
    sed -n '2,18p' "$0"
    exit 2
    ;;
esac
