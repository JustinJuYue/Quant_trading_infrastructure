# Quant Trading Infrastructure (Python + Julia, ZMQ)

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
└─ README.md