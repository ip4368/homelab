#!/bin/sh
# Installs homelabd on this machine and runs it. Needs Debian 13 (or another
# systemd distribution with systemd-networkd), x86_64, root, and internet
# access (homelabd later installs K3s and Longhorn).
#
#   curl -fsSL URL/install.sh | sudo sh -s -- init --vip VIP     the first machine: a new cluster
#   curl -fsSL URL/install.sh | sudo sh -s -- join VIP TOKEN     every other machine
#   curl -fsSL URL/install.sh | sudo sh -s -- upgrade            replace homelabd with the latest
#
# URL is where this script, the homelabd binary and its sha256 (homelabd.sha256,
# as `sha256sum homelabd` prints it) are served from ($HOMELAB_URL overrides
# it). A binary that does not match the sha256 is not run. $HOMELAB_SHA256 gives
# the sha256 to expect instead, e.g. one published somewhere other than URL.
set -eu
URL="${HOMELAB_URL:-@URL@}"
usage="usage: install.sh init --vip VIP [--host NAME] | join VIP TOKEN [--host NAME] | upgrade"

[ "$#" -ge 1 ] || { echo "$usage" >&2; exit 2; }
[ "$(id -u)" = 0 ] || { echo "install.sh: run it as root (sudo sh -s -- ...)" >&2; exit 1; }
[ "$(uname -m)" = x86_64 ] || { echo "install.sh: only x86_64 machines are supported for now" >&2; exit 1; }
command -v systemctl >/dev/null || { echo "install.sh: needs systemd" >&2; exit 1; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL --retry 3 -o "$tmp/homelabd" "$URL/homelabd"
want=${HOMELAB_SHA256:-$(curl -fsSL --retry 3 "$URL/homelabd.sha256" | cut -d' ' -f1)}
want=$(printf '%s' "$want" | tr 'A-F' 'a-f')
got=$(sha256sum "$tmp/homelabd" | cut -d' ' -f1)
if [ -z "$want" ] || [ "$got" != "$want" ]; then
  echo "install.sh: the homelabd from $URL has sha256 $got, not ${want:-the one at $URL/homelabd.sha256 (none found)}; not running it" >&2
  exit 1
fi
chmod 0755 "$tmp/homelabd"

cmd=$1
shift
case "$cmd" in
  init|join) "$tmp/homelabd" "$cmd" "$@" --source "$URL" ;;
  upgrade) "$tmp/homelabd" upgrade ;;
  *) echo "$usage" >&2; exit 2 ;;
esac
