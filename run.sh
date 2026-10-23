#!/usr/bin/env bash
# Builds the UI, then runs the server (which also serves the built UI).
# Set PORT to listen somewhere other than 8080.
set -euo pipefail
cd "$(dirname "$0")"

# Install UI dependencies only when they are missing or older than the lockfile
# (npm writes node_modules/.package-lock.json on every install).
if [ ! -f ui/node_modules/.package-lock.json ] || [ ui/package-lock.json -nt ui/node_modules/.package-lock.json ]; then
  (cd ui && npm ci)
fi
(cd ui && npm run build)

echo "http://localhost:${PORT:-8080}"
exec cargo run --release -p server
