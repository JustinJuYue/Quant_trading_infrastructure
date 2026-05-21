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
function load_alpha_plugin(filename::String, struct_name::Symbol)::AbstractAlpha
    filepath = joinpath(ALPHAS_DIR, filename)
    if !isfile(filepath)
        error("Alpha plugin not found at: $filepath")
    end
    
    include(filepath)
    type_constructor = eval(struct_name)
    alpha_instance = Base.invokelatest(type_constructor)
    println("[INFO] Successfully loaded and compiled Alpha: $(typeof(alpha_instance))")
    
    return alpha_instance
end

# ----------------------------------------------------------------------------
# STEP 3: Virtual Matching Engine (Event-Driven)
# ----------------------------------------------------------------------------
function run_backtest(alpha::AbstractAlpha, df::DataFrame)
    # --- Exchange Parameters (升级为合约费率体系) ---
    initial_capital = 100_000.0
    usdt_balance = initial_capital
    position = 0.0          
    
    fee_maker = 0.0002       # 0.02% 限价单费率 (Kraken Futures)
    fee_taker = 0.0005       # 0.05% 市价单费率
    slippage_taker = 0.00015 # 0.015% 市价单真实滑点
    
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
            "volume_spike" => get(row, :volume_spike, 0)
        )
        
        signal = generate_signal(alpha, market_data)
        
        # 1. 提取信号与权重
        action = get(signal, "action", "HOLD")
        target_weight = get(signal, "weight", 0.0) 
        order_type = get(signal, "order_type", "TAKER") 
        
        # 2. 动态匹配真实成本
        actual_fee = order_type == "MAKER" ? fee_maker : fee_taker
        actual_slippage = order_type == "MAKER" ? 0.0 : slippage_taker

        # 3. 实时 Mark-to-Market 计算当前总权益
        current_equity = usdt_balance + (position * row.close)
        
        # 4. 计算目标仓位暴露度 与 当前真实仓位暴露度
        target_exposure = current_equity * target_weight
        current_exposure = position * row.close
        
        # --- Execution Logic (目标权重动态调仓) ---
        if action == "BUY" && target_exposure > current_exposure + 1.0 
            # 只买入差额部分
            funds_to_spend = target_exposure - current_exposure
            funds_to_spend = min(funds_to_spend, usdt_balance) # 绝对防止透支
            
            if funds_to_spend > 10.0 # 最小下单金额10 USDT
                # 使用动态滑点和手续费计算
                exec_price = row.close * (1.0 + actual_slippage)
                usable_funds = funds_to_spend * (1.0 - actual_fee)
                qty_bought = usable_funds / exec_price
                
                entry_cost += funds_to_spend 
                position += qty_bought
                usdt_balance -= funds_to_spend  
                total_trades += 1
            end
            
        elseif action == "SELL" && current_exposure > target_exposure + 1.0
            # 只卖出多余的部分
            value_to_sell = current_exposure - target_exposure
            qty_to_sell = value_to_sell / row.close
            qty_to_sell = min(qty_to_sell, position) # 防止超卖
            
            if qty_to_sell > 0.0001 # 最小卖出数量限制
                exec_price = row.close * (1.0 - actual_slippage)
                gross_funds = qty_to_sell * exec_price
                net_proceeds = gross_funds * (1.0 - actual_fee)
                
                # 按比例核算 PnL 成本
                sold_proportion = qty_to_sell / (position + 1e-8)
                cost_of_sold = entry_cost * sold_proportion
                
                trade_pnl = net_proceeds - cost_of_sold
                if trade_pnl > 0
                    gross_profit += trade_pnl
                else
                    gross_loss += abs(trade_pnl)
                end
                
                usdt_balance += net_proceeds
                position -= qty_to_sell      
                entry_cost -= cost_of_sold    
                total_trades += 1
            end
        end
        
        # 记录每步资金曲线
        push!(equity_curve, usdt_balance + (position * row.close))
    end
    
    # ------------------------------------------------------------------------
    # End-of-Run Liquidation
    # ------------------------------------------------------------------------
    if position > 0.0
        final_price = df[end, :close]
        exec_price = final_price * (1.0 - slippage_taker) # 最后清仓强制以 TAKER 计算
        net_proceeds = (position * exec_price) * (1.0 - fee_taker)
        
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
    
    N = length(equity_curve)
    returns = diff(equity_curve) ./ equity_curve[1:end-1]
    
    days = N / (24 * 60)
    annualized_return = days > 0 ? (final_equity / initial_capital)^(365.0 / days) - 1.0 : 0.0
    
    Rf = 0.04
    Rf_min = Rf / 525600.0
    
    mu = length(returns) > 0 ? mean(returns) : 0.0
    sigma = length(returns) > 1 ? std(returns) : 0.0
    
    sharpe_ratio = 0.0
    if sigma > 0.0
        sharpe_ratio = ((mu - Rf_min) / sigma) * sqrt(525600.0)
    end
    
    sortino_ratio = 0.0
    if length(returns) > 0
        downside_sq_sum = sum( r < Rf_min ? (r - Rf_min)^2 : 0.0 for r in returns )
        sigma_d = sqrt(downside_sq_sum / length(returns))
        
        if sigma_d > 0.0
            sortino_ratio = ((mu - Rf_min) / sigma_d) * sqrt(525600.0)
        end
    end
    
    calmar_ratio = 0.0
    if max_dd_decimal > 0.0
        calmar_ratio = annualized_return / max_dd_decimal
    end
    
    profit_factor = 0.0
    if gross_loss > 0.0
        profit_factor = gross_profit / gross_loss
    elseif gross_profit > 0.0
        profit_factor = Inf 
    end
    
    println("\n==================================================")
    println("📊 QUANT BACKTEST REPORT")
    println("Alpha Engine : $(alpha.strategy_name)")
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

function load_historical_parquet(file_path::String)::DataFrame
    if !isfile(file_path)
        error("FATAL: Historical data file not found at: $file_path. Did you run history_sync.py?")
    end
    
    println("[INFO] Loading real historical data from: $file_path")
    ds = Parquet2.Dataset(file_path)
    df = DataFrame(ds)
    sort!(df, :timestamp)
    df.close = convert.(Float64, df.close)
    df.volume_spike = convert.(Int64, df.volume_spike)
    if any(isnan, df.close)
        error("FATAL: Corrupted data detected. 'close' column contains NaNs.")
    end
    println("[INFO] Data ingestion successful. Total rows (ticks): $(nrow(df))")
    return df
end

if abspath(PROGRAM_FILE) == @__FILE__
    data_path = joinpath(@__DIR__, "..", "data", "BTC_USDT_1m.parquet")
    df_history = load_historical_parquet(data_path)
    
    # 已经修改为加载 Alpha002
    active_strategy = load_alpha_plugin("Alpha002_ZScoreReversion.jl", :Alpha002_ZScoreReversion)
    run_backtest(active_strategy, df_history)
end