#!/usr/bin/env bash
# The real VM lab, in one command (needs Linux with KVM, sudo, qemu-kvm,
# dnsmasq, chrony, genisoimage; see the README).
#
#   ./lab.sh up        build everything and run the server (and the lab) as root
#   ./lab.sh down      power off every VM and stop the lab's daemons
#   ./lab.sh reset     like down, and delete the VMs' disks (the next up installs a fresh cluster)
#   ./lab.sh ssh VM    log into a VM (by its lab name, e.g. ms01-02)
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
  *)
    sed -n '2,12p' "$0"
    exit 2
    ;;
esac
