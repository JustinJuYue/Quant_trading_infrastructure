#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "[INFO] Root directory: $ROOT_DIR"
echo "[INFO] Starting Julia server..."

# Start Julia server in background
(cd "$ROOT_DIR/julia_app" && julia --project=. server.jl) &
JULIA_PID=$!

cleanup() {
  echo
  echo "[INFO] Stopping Julia server (PID: $JULIA_PID)..."
  kill "$JULIA_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Wait for Julia server warm-up
sleep 1

echo "[INFO] Running Python client..."
(
  cd "$ROOT_DIR/python_app"
  source .venv/bin/activate
  python live_feed_integration.py
)

echo "[INFO] Python client finished."