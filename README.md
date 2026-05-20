# Quant Trading Infrastructure (Python + Julia + ZMQ)

A minimal dual-engine trading infrastructure:

- **Julia engine** (`julia_app/server.jl`): receives market slices and returns signals
- **Python client** (`python_app/client.py`): sends market slices and reads signals
- IPC transport: **ZeroMQ over TCP** (`tcp://127.0.0.1:5555`)
- Serialization: **MessagePack**

---

## Project Structure

```text
Quant_trading_infrastructure/
├─ julia_app/
│  ├─ Project.toml
│  ├─ Manifest.toml
│  └─ server.jl
├─ python_app/
│  ├─ client.py
│  ├─ requirements.txt
│  └─ .venv/          # local only (gitignored)
├─ scripts/
│  ├─ bootstrap.sh
│  └─ run_local.sh
├─ Makefile
└─ README.md
```

---

## Prerequisites

Python 3.10+
Julia 1.11.x
macOS/Linux shell (Windows users can use Git Bash or WSL)

---

## Quick Start

```
make bootstrap
make run
```

---

## Manual Setup

### 1. Setup Python Environment

```
cd python_app
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install pyzmq msgpack
pip freeze > requirements.txt
```

### 2. Setup Julia Environment

```
cd julia_app
julia --project=. -e "using Pkg; Pkg.instantiate()"
```
If dependency resolution mismatch appears:
```
julia --project=. -e "using Pkg; Pkg.resolve(); Pkg.instantiate()"
```

## Run Manually (Two Terminals)

### Terminal A (Julia server)

```
cd julia_app
julia --project=. server.jl
```
### Terminal B (Python client)

```cd python_app
source .venv/bin/activate
python client.py
```

## Run with Script (Recommended)

```
./scripts/run_local.sh
```

What it does:

- Starts Julia server in background
- Waits 1 second for server warm-up
- Runs Python client
- On exit (or Ctrl+C), auto-stops Julia server