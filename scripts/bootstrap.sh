#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "[INFO] Root directory: $ROOT_DIR"

echo "[INFO] Setting up Python virtual environment..."
cd "$ROOT_DIR/python_app"

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install --upgrade pip
pip install pyzmq msgpack
pip freeze > requirements.txt

echo "[INFO] Python setup done."

echo "[INFO] Setting up Julia environment..."
cd "$ROOT_DIR/julia_app"
julia --project=. -e "using Pkg; Pkg.resolve(); Pkg.instantiate()"

echo "[INFO] Julia setup done."
echo "[INFO] Bootstrap complete."