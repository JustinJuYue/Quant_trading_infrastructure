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
- [Tech Stack](#tech-stack)
- [License](#license)

---

## Project Structure

```plaintext
Quant_trading_infrastructure/
├── julia_app/                  # Core Strategy Engine (Numerical Analysis)
│   ├── src/
│   │   ├── alphas/             # Alpha logic implementations
│   │   │   ├── Alpha_Template.jl
│   │   │   ├── Alpha001_VolumeMomentum.jl
│   │   │   ├── Alpha002_MeanReversion.jl
│   │   │   └── Alpha003_PriceKinematics.jl
│   │   └── Core.jl             # Risk management & shared types
│   └── backtester.jl           # Institutional backtesting suite
├── python_app/                 # Execution & Data Operations
│   ├── data_pipeline.py        # Real-time WebSocket data ingestion
│   ├── history_sync.py         # Historical Parquet dataset downloader
│   ├── order_manager.py        # Exchange execution gateway
│   └── client.py               # Live trading interface
├── data/                       # Historical data storage (.parquet)
├── scripts/
│   └── bootstrap.sh            # Environment setup automation
├── executions.db               # SQLite trade execution log
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
| `order_manager.py` | Translates target weights into live exchange API orders; manages slippage and rate limits |
| `history_sync.py` | Downloads high-precision Parquet datasets for offline backtesting |
| `client.py` | Live trading interface connecting strategy signals to the execution layer |

### The Julia Engine — Strategy Layer

Responsible for all CPU-bound mathematical computations, state memory, and backtesting.

| Module | Responsibility |
|---|---|
| `src/alphas/` | Pure mathematical models that consume price series and output confidence signals |
| `src/Core.jl` | Pluggable risk subsystem that scales raw alpha signals into safe portfolio weights |
| `backtester.jl` | Simulates the Python execution layer natively — models Maker/Taker fees, slippage, and delta-based position rebalancing |

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