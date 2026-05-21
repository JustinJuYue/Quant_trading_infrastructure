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

- Python 3.10+
- Julia 1.11.x
- macOS/Linux shell (Windows users can use Git Bash or WSL)

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
### First time
```
chmod +x scripts/run_local.sh
```

What it does:

- Starts Julia server in background
- Waits 1 second for server warm-up
- Runs Python client
- On exit (or Ctrl+C), auto-stops Julia server

markdown_content = """# Quant Trading Infrastructure: Dual-Engine Alpha & Execution System

An enterprise-grade, high-frequency quantitative trading infrastructure that pairs a high-performance mathematical modeling brain (**Julia**) with a robust data ingestion, orchestration, and Order Management System (**Python**). 

This platform supports seamless switching between institutional-grade offline event-driven backtesting and ultra-low latency live execution without modifying core Alpha logic.

---

## Core Architecture

### 1. Event-Driven Backtester (`backtester.jl`)
The virtual matching engine has been upgraded from a basic asset tracker to a rigorous sandboxed simulator that mimics real-world exchange mechanics:
* **Contract Fee Structure:** Native implementation of a Tier-1 exchange fee matrix (e.g., Kraken Futures tier rates: Maker fee of 0.02%, Taker fee of 0.05%).
* **Realistic Slippage Friction:** Integrated a high-frequency market impact and slippage penalty model (0.015% for Taker orders) to ensure backtest equity curves are highly conservative and replicable in live conditions.
* **End-of-Run Liquidation:** Implements mandatory force-closure of active inventory at final mark-to-market prices under structural Taker constraints for precise net-of-fee performance reporting.

### 2. Portfolio Target Weight Execution Engine
Eliminated the rudimentary full-capital "All-In / All-Out" execution loop. The core execution handler now operates on **Delta-Based Position Rebalancing**:
* **Continuous Allocation:** Reads the abstract target asset allocation weights ($Weight \in [0.0, 1.0]$) dispatched by the Alpha plugins.
* **Delta Capital Adjustment:** Dynamically computes the precise monetary variance between `target_exposure` and `current_exposure`. It only executes trades for the marginal delta requirement, eliminating unnecessary trade churn and saving massive transaction overhead (**Fee Drag** mitigation).

### 3. Microstructure Noise Filter & Volatility Protection
To handle 1-minute high-frequency crypto asset series (e.g., `BTC_USDT_1m.parquet`), the signal processing layers have been reinforced:
* **Activation Thresholds:** Built-in trigger barriers (e.g., $\text{Signal Strength} > 0.5$) that prevent the execution engine from whipsawing on sub-minute market noise.
* **Dynamic Bound Constraints (`clamp`):** Hardcoded programmatic guardrails that safely compress un-bounded analytical outputs into valid position sizes, safeguarding the cash tier against accidental over-leveraging during extreme tail-risk events.

### 4. Pluggable Risk Management Subsystem
The strategy risk logic has been completely decoupled from the predictive models through an abstract layer in `src/Core.jl`:
* **AbstractRiskModel Base:** Establishes a polymorphic blueprint for risk filters using Julia's multiple dispatch.
* **PassThroughRisk:** A zero-filter plugin used to evaluate raw alpha signal direction and empirical win rates without capital dampening.
* **ClampRisk:** A dynamic boundary enforcement plugin that handles automated position compression between standardized minimum and maximum exposure thresholds.

---

## 📊 Performance Metrics Engine

The evaluation module processes the continuous equity curve into standard risk-adjusted institutional metrics. Due to the 24/7/365 nature of cryptocurrency markets and high-frequency data inputs, metrics are mathematically scaled using the **High-Frequency Cryptocurrency Annualization Factor**:

$$\text{Annualization Factor} = \sqrt{365 \times 24 \times 60} = \sqrt{525,600} \approx 725.0$$

The reporting engine logs the following key statistics upon run completion:
* **Annualized Sharpe Ratio:** Uses downscaled risk-free rates matching the 1-minute interval dimensions ($\text{Rf}_{\text{min}} = \text{Rf} / 525,600$) to calculate variance accurately.
* **Annualized Sortino Ratio:** Isolates downside deviation ($\sigma_d$) using the full-sample size denominator, filtering out good upside volatility from the risk penalty.
* **Calmar Ratio:** Measures the strategy's annualized rate of return against its maximum peak-to-trough draw-down percentage ($\text{Calmar} = R_a / \text{MaxDD}$).
* **Profit Factor:** Rigorously maps round-trip closed execution loops to evaluate statistical edge:
$$\text{Profit Factor} = \frac{\sum \text{Gross Profits}}{\sum |\text{Gross Losses}|}$$

---

## 📂 Repository Structure