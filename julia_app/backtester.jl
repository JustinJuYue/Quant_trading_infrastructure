# ============================================================================
# Offline Event-Driven Backtester
# Strict Constraint: No IPC/Network libraries (No ZMQ, No MsgPack)
# ============================================================================

using DataFrames
using Statistics
using Dates
using Random
using Parquet2 # Uncomment in production to load real historical data

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
# ----------------------------------------------------------------------------
# STEP 3: Virtual Matching Engine (Event-Driven)
# ----------------------------------------------------------------------------

"""
    run_backtest(alpha::AbstractAlpha, df::DataFrame)

A strict event-driven loop that simulates historical execution row-by-row.
Enhanced with Institutional-Grade performance metrics.
"""
function run_backtest(alpha::AbstractAlpha, df::DataFrame)
    # --- Exchange Parameters ---
    initial_capital = 100_000.0
    usdt_balance = initial_capital
    position = 0.0          
    
    fee_rate = 0.001        # 0.1% Taker Fee
    slippage_rate = 0.0005  # 0.05% Slippage penalty per trade
    
    # --- Telemetry & State Tracking ---
    equity_curve = Float64[]
    sizehint!(equity_curve, nrow(df))
    total_trades = 0
    
    # PnL Tracking for Profit Factor
    entry_cost = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    
    println("[INFO] Starting Event-Driven Backtest for $(alpha.strategy_name)...")
    
    # ------------------------------------------------------------------------
    # THE CORE LOOP: Iterating strictly row-by-row
    # ------------------------------------------------------------------------
    for row in eachrow(df)
        market_data = Dict{String, Any}(
            "ticker" => "BTC/USDT",
            "close" => row.close,
            "volume_spike" => row.volume_spike
        )
        
        signal = generate_signal(alpha, market_data)
        action = get(signal, "action", "HOLD")
        
        # --- Execution Logic ---
        if action == "BUY" && usdt_balance > 1.0 
            exec_price = row.close * (1.0 + slippage_rate)
            usable_funds = usdt_balance * (1.0 - fee_rate)
            qty_bought = usable_funds / exec_price
            
            entry_cost = usdt_balance 
            
            position += qty_bought
            usdt_balance = 0.0  
            total_trades += 1
            
        elseif action == "SELL" && position > 0.0
            exec_price = row.close * (1.0 - slippage_rate)
            gross_funds = position * exec_price
            net_proceeds = gross_funds * (1.0 - fee_rate)
            
            trade_pnl = net_proceeds - entry_cost
            if trade_pnl > 0
                gross_profit += trade_pnl
            else
                gross_loss += abs(trade_pnl)
            end
            
            usdt_balance += net_proceeds
            position = 0.0      
            entry_cost = 0.0    
            total_trades += 1
        end
        
        # Mark-to-Market (MTM) Valuation
        current_equity = usdt_balance + (position * row.close)
        push!(equity_curve, current_equity)
    end
    
    # ------------------------------------------------------------------------
    # End-of-Run Liquidation
    # ------------------------------------------------------------------------
    if position > 0.0
        final_price = df[end, :close]
        exec_price = final_price * (1.0 - slippage_rate)
        net_proceeds = (position * exec_price) * (1.0 - fee_rate)
        
        trade_pnl = net_proceeds - entry_cost
        if trade_pnl > 0
            gross_profit += trade_pnl
        else
            gross_loss += abs(trade_pnl)
        end
        
        usdt_balance += net_proceeds
        position = 0.0
        total_trades += 1
        equity_curve[end] = usdt_balance 
    end
    
    final_equity = usdt_balance
    
    # ------------------------------------------------------------------------
    # STEP 4: Institutional-Grade Performance Evaluation
    # ------------------------------------------------------------------------
    
    # 1. Total Return & Max Drawdown
    total_return_pct = ((final_equity - initial_capital) / initial_capital) * 100
    peak = initial_capital
    max_dd_pct = 0.0
    for eq in equity_curve
        if eq > peak
            peak = eq
        end
        dd_pct = ((peak - eq) / peak) * 100
        if dd_pct > max_dd_pct
            max_dd_pct = dd_pct
        end
    end
    max_dd_decimal = max_dd_pct / 100.0
    
    # 2. Return Series Extraction
    N = length(equity_curve)
    returns = diff(equity_curve) ./ equity_curve[1:end-1]
    
    # 3. Annualized Return (Ra)
    days = N / (24 * 60)
    annualized_return = days > 0 ? (final_equity / initial_capital)^(365.0 / days) - 1.0 : 0.0
    
    # 4. Annualized Sharpe Ratio
    Rf = 0.04
    Rf_min = Rf / 525600.0
    
    mu = length(returns) > 0 ? mean(returns) : 0.0
    sigma = length(returns) > 1 ? std(returns) : 0.0
    
    sharpe_ratio = 0.0
    if sigma > 0.0
        sharpe_ratio = ((mu - Rf_min) / sigma) * sqrt(525600.0)
    end
    
    # 5. Annualized Sortino Ratio
    sortino_ratio = 0.0
    if length(returns) > 0
        downside_sq_sum = sum( r < Rf_min ? (r - Rf_min)^2 : 0.0 for r in returns )
        sigma_d = sqrt(downside_sq_sum / length(returns))
        
        if sigma_d > 0.0
            sortino_ratio = ((mu - Rf_min) / sigma_d) * sqrt(525600.0)
        end
    end
    
    # 6. Calmar Ratio
    calmar_ratio = 0.0
    if max_dd_decimal > 0.0
        calmar_ratio = annualized_return / max_dd_decimal
    end
    
    # 7. Profit Factor
    profit_factor = 0.0
    if gross_loss > 0.0
        profit_factor = gross_profit / gross_loss
    elseif gross_profit > 0.0
        profit_factor = Inf 
    end
    
    # ------------------------------------------------------------------------
    # Console Report Generation
    # ------------------------------------------------------------------------
    println("\n==================================================")
    println("📊 INSTITUTIONAL QUANT BACKTEST REPORT")
    println("🧠 Alpha Engine : $(alpha.strategy_name)")
    println("==================================================")
    println("Initial Capital   : \$$(string(initial_capital))")
    println("Final Equity      : \$$(round(final_equity, digits=2))")
    println("Duration (Days)   : $(round(days, digits=2))")
    println("Total Trades      : $(total_trades)")
    println("--------------------------------------------------")
    println("Total Return      : $(round(total_return_pct, digits=2)) %")
    println("Annualized Return : $(round(annualized_return * 100, digits=2)) %")
    println("Max Drawdown      : $(round(max_dd_pct, digits=2)) %")
    println("--------------------------------------------------")
    println("Sharpe Ratio (Ann): $(round(sharpe_ratio, digits=3))")
    println("Sortino Ratio(Ann): $(round(sortino_ratio, digits=3))")
    println("Calmar Ratio      : $(round(calmar_ratio, digits=3))")
    
    pf_str = profit_factor == Inf ? "Inf (No Losses)" : string(round(profit_factor, digits=3))
    println("Profit Factor     : $pf_str")
    println("==================================================\n")
end

"""
    load_historical_parquet(file_path::String) -> DataFrame

Loads the Parquet file generated by the Python sync script.
Ensures strict temporal ordering and exact type matching to prevent Type Instability 
during the event-driven matching loop.
"""
function load_historical_parquet(file_path::String)::DataFrame
    if !isfile(file_path)
        error("FATAL: Historical data file not found at: $file_path. Did you run history_sync.py?")
    end
    
    println("[INFO] Loading real historical data from: $file_path")
    
    # Load Parquet file efficiently
    ds = Parquet2.Dataset(file_path)
    df = DataFrame(ds)
    
    # 1. Enforce strict temporal ordering (Chronological ascending)
    # This guarantees Look-ahead Bias is structurally impossible
    sort!(df, :timestamp)
    
    # 2. Type casting safety
    # Python's PyArrow might infer numeric types differently based on architecture.
    # We strictly cast them to match our Alpha expectations.
    df.close = convert.(Float64, df.close)
    df.volume_spike = convert.(Int64, df.volume_spike)
    
    # Check for NaN values just in case Python missed them
    if any(isnan, df.close)
        error("FATAL: Corrupted data detected. 'close' column contains NaNs.")
    end
    
    println("[INFO] Data ingestion successful. Total rows (ticks): $(nrow(df))")
    return df
end


# ----------------------------------------------------------------------------
# BOOTSTRAP SEQUENCE
# ----------------------------------------------------------------------------
if abspath(PROGRAM_FILE) == @__FILE__
    # 1. 精准定位你的真实数据文件
    data_path = joinpath(@__DIR__, "..", "data", "BTC_USDT_1m.parquet")
    
    # 🚨 修复Bug：调用 AI 编写的强类型安全加载函数
    df_history = load_historical_parquet(data_path)
    
    # 2. 动态加载你的策略插件
    active_strategy = load_alpha_plugin("Alpha001_VolumeMomentum.jl", :Alpha001_VolumeMomentum)
    
    # 3. 启动回测引擎
    run_backtest(active_strategy, df_history)
end