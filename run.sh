#!/usr/bin/env bash
# Builds the UI, then runs the server (which also serves the built UI).
# Set PORT to listen somewhere other than 8080.
set -euo pipefail
cd "$(dirname "$0")"

(cd ui && npm ci && npm run build)

echo "http://localhost:${PORT:-8080}"
exec cargo run --release -p server
