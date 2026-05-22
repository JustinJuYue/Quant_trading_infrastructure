# Quant Trading Infrastructure: Dual-Engine Alpha & Execution System

![Julia](https://img.shields.io/badge/Julia-1.9+-9558B2?style=for-the-badge&logo=julia&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=for-the-badge)

> An enterprise-grade, high-frequency quantitative trading infrastructure designed for
> cryptocurrency markets. Built on a strict **Dual-Engine Architecture** that separates
> mathematical modeling from execution mechanics — enabling seamless transitions from
> offline backtesting to live trading.

---

## Table of Contents

- [Project Structure](#project-structure)
- [System Architecture](#system-architecture)
- [Quick Start](#quick-start)
- [Backtesting and Adjusting Alphas](#backtesting-and-adjusting-alphas)
- [Creating a Custom Alpha](#creating-a-custom-alpha)
- [Live Trading](#live-trading)
- [Operations Manual — Global Interface Control (SOP)](#operations-manual--global-interface-control-sop)
- [Tech Stack](#tech-stack)
- [License](#license)

---

## Project Structure

```plaintext
Quant_trading_infrastructure/
├── julia_app/                      # Core Strategy Engine (Numerical Analysis)
│   ├── src/
│   │   ├── alphas/                 # Alpha logic implementations
│   │   │   ├── Alpha_Template.jl
│   │   │   ├── Alpha001_VolumeMomentum.jl
│   │   │   ├── Alpha002_MeanReversion.jl
│   │   │   └── Alpha003_PriceKinematics.jl
│   │   └── Core.jl                 # Risk management & shared types
│   ├── backtester.jl               # Institutional backtesting suite
│   └── server.jl                   # Live alpha fleet server
├── python_app/                     # Execution & Data Operations
│   ├── data_pipeline.py            # Real-time WebSocket data ingestion
│   ├── history_sync.py             # Historical Parquet dataset downloader
│   ├── order_manager.py            # Exchange execution gateway & risk controls
│   ├── live_feed_integration.py    # Live routing, data gateway & DRY_RUN switch
│   └── client.py                   # Live trading interface
├── data/                           # Historical data storage (.parquet)
├── scripts/
│   └── bootstrap.sh                # Environment setup automation
├── executions.db                   # SQLite trade execution log
└── README.md
```

---

## System Architecture

This infrastructure enforces a strict **separation of concerns** between two engines,
each selected for its domain-specific strengths.

### The Python Engine — Execution Layer

Responsible for all I/O-bound tasks, network communications, and exchange interactions.

| Module | Responsibility |
|---|---|
| `data_pipeline.py` | Ingests real-time WebSocket feeds; normalizes order book and k-line data |
| `order_manager.py` | Translates target weights into live exchange API orders; manages slippage, rate limits, Kelly sizing, and fat-finger risk controls |
| `history_sync.py` | Downloads high-precision Parquet datasets for offline backtesting |
| `live_feed_integration.py` | Routes live market data to the Julia engine; controls the DRY_RUN simulation switch |
| `client.py` | Live trading interface connecting strategy signals to the execution layer |

### The Julia Engine — Strategy Layer

Responsible for all CPU-bound mathematical computations, state memory, and backtesting.

| Module | Responsibility |
|---|---|
| `src/alphas/` | Pure mathematical models that consume price series and output confidence signals |
| `src/Core.jl` | Pluggable risk subsystem that scales raw alpha signals into safe portfolio weights |
| `backtester.jl` | Simulates the Python execution layer natively — models Maker/Taker fees, slippage, and delta-based position rebalancing |
| `server.jl` | Live alpha fleet server; manages state hydration for all registered alphas on startup |

---

## Quick Start

### Step 1: Bootstrap the Environment

```bash
# Navigate to the project root
cd Quant_trading_infrastructure/

# Run the automated setup script for both Julia and Python environments
bash scripts/bootstrap.sh
```

### Step 2: Synchronize Market Data

```bash
# Ingest historical candles into high-performance Parquet storage
python python_app/history_sync.py
```

### Step 3: Provision the Julia Environment

```bash
cd julia_app
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

### Step 4: Run the Backtester

```bash
# From the julia_app/ directory
julia --project=. backtester.jl
```

---

## Backtesting and Adjusting Alphas

The backtester is fully modular. Strategy behavior is controlled by plugging in
different Alpha and Risk configurations at the bottom of `backtester.jl`.

### Adjusting Alpha Hyperparameters

```julia
# Inside backtester.jl — adjusting the lookback window from 15 to 60 minutes
active_strategy = Alpha003_PriceKinematics("Alpha003_Slow", 1.0, 60)
run_backtest(active_strategy, df_history)
```

### Swapping the Risk Module

```julia
# Option A: PassThroughRisk — full allocation, ignores signal strength
my_risk = QuantCore.PassThroughRisk()

# Option B: ClampRisk — caps position size between 10% and 80%
my_risk = QuantCore.ClampRisk(0.1, 0.8)

# Attach the risk module to the active alpha
active_strategy = Alpha003_PriceKinematics("Alpha003_Clamped", 1.0, 60, my_risk)
```

### Running a Named Alpha via CLI

```bash
julia julia_app/backtester.jl --alpha Alpha001_VolumeMomentum
```

---

## Creating a Custom Alpha

New strategies can be introduced with no modifications to the backtester or the
live execution engine. Only the mathematical signal logic needs to be written.

### Step 1: Copy the Template

```bash
cd julia_app/src/alphas/
cp Alpha_Template.jl MyNewStrategy.jl
```

### Step 2: Implement the Signal Logic

```julia
using Dates
using Statistics
using ..QuantCore

# 1. Define the strategy parameters and internal memory state
mutable struct MyCustomAlpha <: AbstractAlpha
    strategy_name::String
    target_weight::Float64
    risk_model::QuantCore.AbstractRiskModel
    price_history::Vector{Float64}   # Rolling memory buffer
end

# 2. Implement the generate_signal interface
function QuantCore.generate_signal(alpha::MyCustomAlpha, data::Dict)::Dict
    ticker        = get(data, "ticker", "UNKNOWN")
    current_price = get(data, "close",  0.0)

    push!(alpha.price_history, current_price)

    action       = "HOLD"
    final_weight = 0.0

    # --- Signal Logic ---
    if length(alpha.price_history) > 10
        recent_return = (current_price - alpha.price_history[end-10]) /
                         alpha.price_history[end-10]

        if recent_return > 0.005          # Momentum breakout threshold: +0.5%
            action       = "BUY"
            raw_strength = recent_return * 100
            final_weight = QuantCore.apply_risk(
                               alpha.risk_model, raw_strength, alpha.target_weight)

        elseif recent_return < -0.002     # Exit threshold: -0.2%
            action       = "SELL"
            final_weight = 0.0
        end
    end
    # --------------------

    # 3. Return the standardized output schema
    return Dict(
        "alpha_id"        => alpha.strategy_name,
        "ticker"          => ticker,
        "action"          => action,
        "weight"          => final_weight,
        "price_at_signal" => current_price,
        "timestamp"       => Dates.datetime2unix(now()) * 1000
    )
end
```

> **Architectural Note:** The standardized output schema (`action`, `weight`) ensures
> that any custom Alpha is immediately compatible with both `backtester.jl` for
> historical simulation and `order_manager.py` for live exchange execution via ZMQ,
> without requiring changes to either system.

---

## Live Trading

```bash
# Start the live execution client
python python_app/client.py
```

All executed trades are automatically persisted to `executions.db` (SQLite).

---

## Operations Manual — Global Interface Control (SOP)

This architecture is intentionally decoupled. During routine operations, each concern
maps to a precise configuration file. The following reference table defines where each
control surface lives and how to operate it correctly.

---

### Section 1 — Strategy and Backtesting (Julia Layer)

**Control: Entry / Exit Signal Logic**

- **File:** `julia_app/src/alphas/Alpha*.jl`
- **Scope:** This layer contains only pure mathematical expressions and threshold
  constants (e.g., fast/slow moving average periods, reflexivity multipliers). Upon
  completion, each alpha must emit a dictionary containing `action` and `weight`.
  No execution logic belongs here.

**Control: Running a Historical Backtest**

- **File:** `julia_app/backtester.jl`
- **Command:**
  ```bash
  julia --project=. backtester.jl
  ```
- **Note:** Upon completion, record the **Profit Factor** and estimated **Win Rate**
  from the output report. These values are required inputs for the Kelly sizer
  in the Python execution layer.

---

### Section 2 — Risk Controls and Capital Management (Python Layer)

**Control: Kelly Criterion — Alpha Statistics Registration**

- **File:** `python_app/order_manager.py`
- **Location:** `ExchangeExecution.__init__` method
- **Operation:** Register or update the statistical profile for each alpha. These
  values feed directly into the Kelly position sizer.

  ```python
  self.kelly_sizer.register_alpha_stats(
      "YourAlphaName",
      win_rate=0.55,        # Win rate recorded from backtest report
      win_loss_ratio=1.8    # Profit Factor recorded from backtest report
  )
  ```

**Control: Fat-Finger Intercept — Maximum Notional Limit**

- **File:** `python_app/order_manager.py`
- **Location:** `RiskManager` class
- **Parameters:**

  | Parameter | Description |
  |---|---|
  | `max_notional_usd` | Maximum allowable order size in USD per trade. Guards against API anomalies causing oversized positions. |
  | `cooldown_seconds` | Mandatory pause between consecutive orders. Prevents cascading erroneous order submissions. |

  ```python
  self.risk_manager.max_notional_usd = 500
  self.risk_manager.cooldown_seconds = 10
  ```

---

### Section 3 — Live Data Routing and Feed Gateway (Python / Julia Interface)

**Control: Adding a New Instrument or Timeframe**

- **File:** `python_app/live_feed_integration.py`
- **Location:** `feed_tasks = [...]` list
- **Operation:** Append an entry for each instrument and timeframe to be monitored.
  The feed manager will automatically loop through all entries, tag each payload,
  and forward it to the Julia engine.

  ```python
  feed_tasks = [
      {"symbol": "BTC/USD",  "interval": "1m"},
      {"symbol": "ETH/USD",  "interval": "1h"},
      {"symbol": "SOL/USD",  "interval": "1d"},  # <-- append new instruments here
  ]
  ```

**Control: Registering a New Alpha on the Live Server**

- **File:** `julia_app/server.jl`
- **Location:** `alpha_fleet = [...]` array at the bottom of the file
- **Operation:** Append the new alpha struct to the fleet array. On server startup,
  the system will automatically perform **state hydration** for every registered alpha,
  replaying recent market history to restore internal memory buffers to a consistent
  operational state.

  ```julia
  alpha_fleet = [
      Alpha001_VolumeMomentum("Alpha001", 1.0),
      Alpha003_PriceKinematics("Alpha003", 1.0, 60),
      MyNewStrategy("MyStrategy", 1.0),   # <-- register new alphas here
  ]
  ```

---

### Section 4 — Simulation vs. Live Execution Switch (Critical)

**Control: DRY_RUN Mode**

- **File:** `python_app/live_feed_integration.py`
- **Location:** Line ~103. Search for `DRY_RUN =`

This is the single most important operational control in the system. It must be
verified before every session.

| State | Value | Behavior |
|---|---|---|
| Simulation Mode | `DRY_RUN = True` | The full pipeline executes — data ingestion, signal computation, and Kelly sizing — but the final order submission is intercepted. No capital is deployed. A confirmation is printed to the console in place of order dispatch. |
| Live Execution Mode | `DRY_RUN = False` | The system connects to the Kraken API and submits real orders. Capital is at risk. Use only after thorough backtesting and simulation validation. |

```python
# python_app/live_feed_integration.py (~line 103)

DRY_RUN = True   # Set to False only when ready for live capital deployment
```

> **Operational Protocol:** Always begin a new alpha's live deployment in
> `DRY_RUN = True` mode. Validate signal output and Kelly-sized weights in the
> console logs for a minimum observation period before switching to live execution.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Strategy and Signal Processing | Julia 1.9+, Statistics.jl |
| Execution and Data Ingestion | Python 3.10+, WebSockets, REST APIs |
| Data Storage | Apache Parquet, SQLite |
| Inter-process Messaging | ZeroMQ (ZMQ) |
| Build Automation | Bash (`bootstrap.sh`) |

---

## License

This project is licensed under the **MIT License**.

---

*Built for performance. Designed for clarity. Architected for production.*