# ============================================================================
# Offline Event-Driven Backtester
# Strict Constraint: No IPC/Network libraries (No ZMQ, No MsgPack)
# ============================================================================

using DataFrames
using Dates
using Random
# using Parquet2 # Uncomment in production to load real historical data

# Include the standard interface WITHOUT modifying it
include("src/Core.jl")
using .QuantCore

const ALPHAS_DIR = joinpath(@__DIR__, "src", "alphas")

# ----------------------------------------------------------------------------
# STEP 2: Dynamic Plugin Loader with World Age Resolution
# ----------------------------------------------------------------------------
"""
    load_alpha_plugin(filename::String, struct_name::Symbol) -> AbstractAlpha

Dynamically includes the strategy file and safely instantiates it.
Uses `Base.invokelatest` to bypass Julia's World Age restrictions, 
ensuring the newly compiled struct can be immediately executed in the same session.
"""
function load_alpha_plugin(filename::String, struct_name::Symbol)::AbstractAlpha
    filepath = joinpath(ALPHAS_DIR, filename)
    if !isfile(filepath)
        error("Alpha plugin not found at: $filepath")
    end
    
    # 1. Load the new type into the current session
    include(filepath)
    
    # 2. Evaluate the symbol to get the Type/Constructor
    type_constructor = eval(struct_name)
    
    # 3. Use invokelatest to instantiate, solving the world-age problem
    alpha_instance = Base.invokelatest(type_constructor)
    println("[INFO] Successfully loaded and compiled Alpha: $(typeof(alpha_instance))")
    
    return alpha_instance
end

# ----------------------------------------------------------------------------
# STEP 3: Virtual Matching Engine (Event-Driven)
# ----------------------------------------------------------------------------
"""
    run_backtest(alpha::AbstractAlpha, df::DataFrame)

A strict event-driven loop that simulates historical execution row-by-row 
to completely eliminate Look-ahead Bias.
"""
function run_backtest(alpha::AbstractAlpha, df::DataFrame)
    # --- Exchange Parameters ---
    initial_capital = 100_000.0
    usdt_balance = initial_capital
    position = 0.0          # Number of coins held
    
    fee_rate = 0.001        # 0.1% Taker Fee
    slippage_rate = 0.0005  # 0.05% Slippage penalty per trade
    
    # --- Telemetry & State Tracking ---
    equity_curve = Float64[]
    sizehint!(equity_curve, nrow(df))
    total_trades = 0
    
    println("[INFO] Starting Event-Driven Backtest for $(alpha.strategy_name)...")
    
    # ------------------------------------------------------------------------
    # THE CORE LOOP: Iterating strictly row-by-row (No vectorized cheating)
    # ------------------------------------------------------------------------
    for row in eachrow(df)
        # 1. Construct the current market slice (matching the live ZMQ payload)
        market_data = Dict{String, Any}(
            "ticker" => "BTC/USDT",
            "close" => row.close,
            "volume_spike" => row.volume_spike
        )
        
        # 2. Query the isolated Alpha logic
        signal = generate_signal(alpha, market_data)
        action = get(signal, "action", "HOLD")
        
        # 3. Execution Logic with slippage and fees
        # Note: We enforce a minimum USDT balance check to avoid micro-trades (dust)
        if action == "BUY" && usdt_balance > 1.0 
            # Execute at a WORSE price due to slippage
            exec_price = row.close * (1.0 + slippage_rate)
            
            # Calculate how many coins we can buy after fees
            usable_funds = usdt_balance * (1.0 - fee_rate)
            qty_bought = usable_funds / exec_price
            
            position += qty_bought
            usdt_balance = 0.0  # Full allocation
            total_trades += 1
            
        elseif action == "SELL" && position > 0.0
            # Execute at a WORSE price due to slippage
            exec_price = row.close * (1.0 - slippage_rate)
            
            # Calculate funds received after fees
            gross_funds = position * exec_price
            usdt_balance += gross_funds * (1.0 - fee_rate)
            
            position = 0.0      # Full liquidation
            total_trades += 1
        end
        
        # 4. Mark-to-Market (MTM) Valuation for equity curve
        current_equity = usdt_balance + (position * row.close)
        push!(equity_curve, current_equity)
    end
    
    # ------------------------------------------------------------------------
    # STEP 4: End-of-Run Liquidation & Performance Calculation
    # ------------------------------------------------------------------------
    if position > 0.0
        final_price = df[end, :close]
        exec_price = final_price * (1.0 - slippage_rate)
        usdt_balance += (position * exec_price) * (1.0 - fee_rate)
        position = 0.0
        total_trades += 1
        # Update the last point in the equity curve
        equity_curve[end] = usdt_balance 
    end
    
    final_equity = usdt_balance
    
    # Calculate Total Return
    total_return_pct = ((final_equity - initial_capital) / initial_capital) * 100
    
    # Calculate Max Drawdown (Peak-to-Trough)
    peak = initial_capital
    max_dd = 0.0
    for eq in equity_curve
        if eq > peak
            peak = eq
        end
        dd_pct = ((peak - eq) / peak) * 100
        if dd_pct > max_dd
            max_dd = dd_pct
        end
    end
    
    # Print Institutional-Grade Report
    println("\n==================================================")
    println("📊 QUANT DEV BACKTEST REPORT")
    println("🧠 Alpha Engine : $(alpha.strategy_name)")
    println("==================================================")
    println("Initial Capital  : \$$(string(initial_capital))")
    println("Final Equity     : \$$(round(final_equity, digits=2))")
    println("--------------------------------------------------")
    println("Total Return     : $(round(total_return_pct, digits=2)) %")
    println("Max Drawdown     : $(round(max_dd, digits=2)) %")
    println("Total Trades     : $(total_trades)")
    println("==================================================\n")
end

# ----------------------------------------------------------------------------
# Helper: Generate Mock Data for Local Testing
# ----------------------------------------------------------------------------
function generate_mock_data(n_rows::Int=1000)
    Random.seed!(42)
    closes = Float64[]
    current_price = 65000.0
    volume_spikes = Int[]
    
    for i in 1:n_rows
        # Random walk price generation
        current_price *= (1.0 + randn() * 0.002) 
        push!(closes, current_price)
        
        # 1% chance of a massive volume spike triggering a BUY
        # Note: A real Alpha might need a SELL logic. For this test, 
        # we'll randomly trigger a 2 (SELL signal) if we hold a position to see trades.
        spike = rand() < 0.01 ? 1 : (rand() < 0.01 ? 2 : 0)
        push!(volume_spikes, spike)
    end
    
    return DataFrame(close=closes, volume_spike=volume_spikes)
end

# ----------------------------------------------------------------------------
# BOOTSTRAP SEQUENCE
# ----------------------------------------------------------------------------
if abspath(PROGRAM_FILE) == @__FILE__
    # 1. Load the mock historical data (Replace with Parquet read in production)
    df_history = generate_mock_data(5000)
    
    # 2. Dynamically load the Strategy Plugin (solves World Age implicitly)
    active_strategy = load_alpha_plugin("Alpha001_VolumeMomentum.jl", :Alpha001_VolumeMomentum)
    
    # 3. Fire up the matching engine
    run_backtest(active_strategy, df_history)
end