# ============================================================================
# Offline Event-Driven Multi-Asset Backtester
# Architecture: Strategy-Driven Data Loading (Handshake Protocol) |
#               Nested Asset Routing |
#               Exchange-Grade Fee Modelling |
#               Automated Scientific Dashboard Reporting
# ============================================================================

using DataFrames
using Statistics
using Dates
using Random
using Parquet2
using Plots # 用于生成专业图表

# Include the standard interface without modification.
include("src/Core.jl")
using .QuantCore

const ALPHAS_DIR = joinpath(@__DIR__, "src", "alphas")

# ----------------------------------------------------------------------------
# STEP 1: Dynamic Alpha Plugin Loader
# ----------------------------------------------------------------------------

"""
    load_alpha_plugin(filename, struct_name) -> AbstractAlpha
"""
function load_alpha_plugin(filename::String, struct_name::Symbol)::AbstractAlpha
    filepath = joinpath(ALPHAS_DIR, filename)
    if !isfile(filepath)
        error("Alpha plugin not found at: $filepath")
    end

    include(filepath)
    type_constructor = eval(struct_name)
    alpha_instance   = Base.invokelatest(type_constructor)
    println("[INFO] Successfully loaded and compiled alpha: $(typeof(alpha_instance))")

    return alpha_instance
end

# ----------------------------------------------------------------------------
# STEP 2: Virtual Matching Engine (Multi-Asset Nested Routing)
# ----------------------------------------------------------------------------

function run_backtest(alpha::AbstractAlpha, aligned_df::DataFrame, target_assets::Vector{String})

    # --- Exchange Parameters ---
    initial_capital = 100_000.0
    usdt_balance    = initial_capital

    positions   = Dict{String, Float64}(asset => 0.0 for asset in target_assets)
    entry_costs = Dict{String, Float64}(asset => 0.0 for asset in target_assets)

    fee_maker        = 0.0002   
    fee_taker        = 0.0005   
    slippage_taker   = 0.00015  

    equity_curve = Float64[]
    sizehint!(equity_curve, nrow(aligned_df))
    total_trades = 0
    gross_profit = 0.0
    gross_loss   = 0.0

    println("[INFO] Starting multi-asset data bus event loop...")

    for row in eachrow(aligned_df)
        assets_payload = Dict{String, Dict{String, Any}}()
        for asset in target_assets
            assets_payload[asset] = Dict{String, Any}(
                "close"        => row[Symbol("close_", asset)],
                "volume_spike" => row[Symbol("volume_spike_", asset)]
            )
        end

        market_data = Dict{String, Any}(
            "ticker"    => join(target_assets, "+"),
            "timestamp" => row.timestamp,
            "assets"    => assets_payload
        )

        signal = generate_signal(alpha, market_data)

        action        = get(signal, "action",       "HOLD")
        target_weight = get(signal, "weight",        0.0)
        order_type    = get(signal, "order_type",   "TAKER")
        trade_asset   = get(signal, "trade_asset",   target_assets[1])

        actual_fee      = order_type == "MAKER" ? fee_maker : fee_taker
        actual_slippage = order_type == "MAKER" ? 0.0       : slippage_taker

        asset_value = 0.0
        for asset in target_assets
            asset_value += positions[asset] * row[Symbol("close_", asset)]
        end
        current_equity = usdt_balance + asset_value

        target_exposure  = current_equity * target_weight
        current_exposure = positions[trade_asset] * row[Symbol("close_", trade_asset)]

        if action == "BUY" && target_exposure > current_exposure + 1.0
            funds_to_spend = target_exposure - current_exposure
            funds_to_spend = min(funds_to_spend, usdt_balance)

            if funds_to_spend > 10.0
                exec_price = row[Symbol("close_", trade_asset)] * (1.0 + actual_slippage)
                usable_funds = funds_to_spend * (1.0 - actual_fee)
                qty_bought = usable_funds / exec_price

                entry_costs[trade_asset]  += funds_to_spend
                positions[trade_asset]    += qty_bought
                usdt_balance              -= funds_to_spend
                total_trades              += 1
            end

        elseif action == "SELL" && current_exposure > target_exposure + 1.0
            value_to_sell = current_exposure - target_exposure
            qty_to_sell   = value_to_sell / row[Symbol("close_", trade_asset)]
            qty_to_sell   = min(qty_to_sell, positions[trade_asset])

            if qty_to_sell > 0.0001
                exec_price  = row[Symbol("close_", trade_asset)] * (1.0 - actual_slippage)
                gross_funds = qty_to_sell * exec_price
                net_proceeds = gross_funds * (1.0 - actual_fee)

                sold_proportion  = qty_to_sell / (positions[trade_asset] + 1e-8)
                cost_of_sold     = entry_costs[trade_asset] * sold_proportion

                trade_pnl = net_proceeds - cost_of_sold
                if trade_pnl > 0
                    gross_profit += trade_pnl
                else
                    gross_loss += abs(trade_pnl)
                end

                usdt_balance              += net_proceeds
                positions[trade_asset]    -= qty_to_sell
                entry_costs[trade_asset]  -= cost_of_sold
                total_trades              += 1
            end
        end

        latest_asset_value = 0.0
        for asset in target_assets
            latest_asset_value += positions[asset] * row[Symbol("close_", asset)]
        end
        push!(equity_curve, usdt_balance + latest_asset_value)
    end

    # --- End-of-Run Liquidation ---
    for asset in target_assets
        if positions[asset] > 0.0
            final_price  = aligned_df[end, Symbol("close_", asset)]
            exec_price   = final_price * (1.0 - slippage_taker)
            net_proceeds = (positions[asset] * exec_price) * (1.0 - fee_taker)

            trade_pnl = net_proceeds - entry_costs[asset]
            if trade_pnl > 0
                gross_profit += trade_pnl
            else
                gross_loss += abs(trade_pnl)
            end

            usdt_balance      += net_proceeds
            positions[asset]  = 0.0
            total_trades      += 1
        end
    end

    final_equity        = usdt_balance
    equity_curve[end]   = final_equity

    # --------------------------------------------------------------------------
    # STEP 3: Institutional-Grade Performance Evaluation
    # --------------------------------------------------------------------------
    
    start_time = aligned_df.timestamp[1]
    end_time   = aligned_df.timestamp[end]
    start_str  = Dates.format(start_time, "yyyy-mm-dd HH:MM")
    end_str    = Dates.format(end_time, "yyyy-mm-dd HH:MM")

    total_return_pct = ((final_equity - initial_capital) / initial_capital) * 100

    peak       = initial_capital
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

    N       = length(equity_curve)
    returns = diff(equity_curve) ./ equity_curve[1:end-1]

    tf = alpha.required_timeframe
    minutes_per_bar = 1
    if tf == "1h"
        minutes_per_bar = 60
    elseif tf == "4h"
        minutes_per_bar = 240
    elseif tf == "1d"
        minutes_per_bar = 1440
    end
    days = (N * minutes_per_bar) / (24 * 60)

    annualized_return = days > 0 ? (final_equity / initial_capital)^(365.0 / days) - 1.0 : 0.0

    Rf     = 0.04
    Rf_min = Rf / 525_600.0
    mu     = length(returns) > 0 ? mean(returns)  : 0.0
    sigma  = length(returns) > 1 ? std(returns)   : 0.0

    sharpe_ratio = sigma > 0.0 ? ((mu - Rf_min) / sigma) * sqrt(525_600.0) : 0.0

    sortino_ratio = 0.0
    if length(returns) > 0
        downside_sq_sum = sum(r < Rf_min ? (r - Rf_min)^2 : 0.0 for r in returns)
        sigma_d         = sqrt(downside_sq_sum / length(returns))
        sortino_ratio   = sigma_d > 0.0 ? ((mu - Rf_min) / sigma_d) * sqrt(525_600.0) : 0.0
    end

    calmar_ratio  = max_dd_decimal > 0.0 ? annualized_return / max_dd_decimal : 0.0
    profit_factor = gross_loss > 0.0 ? gross_profit / gross_loss : (gross_profit > 0.0 ? Inf : 0.0)

    # --- Buy & Hold (Benchmark) Calculation ---
    benchmark_asset = target_assets[1]
    initial_price_primary = aligned_df[1, Symbol("close_", benchmark_asset)]
    
    bnh_equity_curve = initial_capital .* (aligned_df[!, Symbol("close_", benchmark_asset)] ./ initial_price_primary)
    
    bnh_return_pct = ((bnh_equity_curve[end] - initial_capital) / initial_capital) * 100
    outperformance = total_return_pct - bnh_return_pct

    println("\n==================================================")
    println("  QUANT MULTI-ASSET BACKTEST REPORT")
    println("  Alpha Engine : $(alpha.strategy_name)")
    println("  Assets       : $(join(target_assets, " | "))")
    println("  Resolution   : $(alpha.required_timeframe)")
    println("  Start Date   : $start_str")
    println("  End Date     : $end_str")
    println("==================================================")
    println("  Initial Capital   : \$$(string(initial_capital))")
    println("  Final Equity      : \$$(round(final_equity, digits=2))")
    println("  Duration (Days)   : $(round(days, digits=2))")
    println("  Total Trades      : $(total_trades)")
    println("--------------------------------------------------")
    println("  Strategy Return   : $(round(total_return_pct, digits=2)) %")
    println("  B&H Return        : $(round(bnh_return_pct, digits=2)) %")
    println("  Outperformance    : $(round(outperformance, digits=2)) %")
    println("  Max Drawdown      : $(round(max_dd_pct, digits=2)) %")
    println("--------------------------------------------------")
    println("  Sharpe Ratio (Ann): $(round(sharpe_ratio, digits=3))")
    println("  Sortino Ratio(Ann): $(round(sortino_ratio, digits=3))")
    println("  Calmar Ratio      : $(round(calmar_ratio, digits=3))")
    println("  Profit Factor     : $(profit_factor == Inf ? "Inf" : string(round(profit_factor, digits=3)))")
    println("==================================================\n")

    # --------------------------------------------------------------------------
    # STEP 4: Automated Scientific Plot Generation (Dashboard UI Style)
    # --------------------------------------------------------------------------
    
    short_name = split(alpha.strategy_name, "_")[1] 
    out_dir = joinpath(@__DIR__, "src", "alphas", "Results")
    mkpath(out_dir)

    plot_title = "$short_name Performance Summary ($(alpha.required_timeframe))"

    dash_y_formatter = y -> begin
        if y >= 1_000_000
            return "\$" * string(round(y / 1_000_000, digits=2)) * "M"
        elseif y >= 1_000
            return "\$" * string(round(y / 1_000, digits=0)) * "K"
        else
            return "\$" * string(round(y, digits=2))
        end
    end

    # 核心资产净值走势主图
    p_main = plot(
        aligned_df.timestamp, equity_curve,
        label="Strategy Equity",
        linewidth=2.5,
        color=RGB(0.27, 0.38, 0.95), 
        title=plot_title,
        titlefont=font(14, "Helvetica"),
        titlealign=:left,
        xlabel="",
        ylabel="Capital",
        yformatter=dash_y_formatter,
        legend=:topleft,
        framestyle=:grid,       
        gridalpha=0.3,          
        foreground_color_grid=:lightgray,
        margin=5Plots.mm
    )
    
    # 🚨 注意：这里移除了 plot!(p_main, ...) 绘制 Buy & Hold 基准曲线的代码，
    # 从而防止大周期的现货数倍涨幅压缩了策略本身的微观波动空间。

    # 创建完全用于展示指标网格的空白子图
    p_metrics = plot(
        framestyle=:none, 
        grid=false, 
        showaxis=false, 
        xticks=false, 
        yticks=false,
        margin=0Plots.mm
    )
    
    c1, c2, c3, c4 = 0.10, 0.35, 0.60, 0.85
    r1, r2 = 0.7, 0.2
    
    # 完美保留 B&H 基准数据在面板网格中
    annotate!(p_metrics, [
        (c1, r1, text("STRATEGY RETURN\n$(round(total_return_pct, digits=2))%", 10, :left, :black)),
        (c2, r1, text("B&H BENCHMARK\n$(round(bnh_return_pct, digits=2))%", 10, :left, :black)),
        (c3, r1, text("OUTPERFORMANCE\n$(round(outperformance, digits=2))%", 10, :left, :black)),
        (c4, r1, text("MAX DRAWDOWN\n$(round(max_dd_pct, digits=2))%", 10, :left, :black)),
        
        (c1, r2, text("SHARPE RATIO\n$(round(sharpe_ratio, digits=2))", 10, :left, :black)),
        (c2, r2, text("SORTINO RATIO\n$(round(sortino_ratio, digits=2))", 10, :left, :black)),
        (c3, r2, text("PROFIT FACTOR\n$(round(profit_factor, digits=2))", 10, :left, :black)),
        (c4, r2, text("TOTAL TRADES\n$total_trades", 10, :left, :black))
    ])

    l = @layout [a; b{0.2h}]
    p_final = plot(p_main, p_metrics, layout=l, size=(950, 700), dpi=300, background_color=:white)

    # 🚨 动态获取当前系统时间（精确到分钟）并拼接到文件名中，防止旧报告被覆盖
    current_time_str = Dates.format(Dates.now(), "yyyymmdd_HHMM")
    plot_file = joinpath(out_dir, "$(short_name)_$(alpha.required_timeframe)_report_$(current_time_str).pdf")
    
    savefig(p_final, plot_file)
    println("[INFO] Generated institutional performance PDF at: $plot_file")
end

# ----------------------------------------------------------------------------
# STEP 5: Strategy-Driven Data Ingestion and Alignment
# ----------------------------------------------------------------------------

function load_and_align_datasets(assets::Vector{String}, timeframe::String)
    base_df = DataFrame()

    for (i, asset) in enumerate(assets)
        data_file = "$(asset)_$(timeframe).parquet"
        file_path = joinpath(@__DIR__, "..", "data", data_file)

        if !isfile(file_path)
            error(
                "Handshake error: strategy requires [$data_file] " *
                "but the file was not found. " *
                "Please run python_app/history_sync.py to download the required data."
            )
        end

        println("[INFO] Loading data file: $data_file")
        ds = Parquet2.Dataset(file_path)
        df = DataFrame(ds)
        sort!(df, :timestamp)

        df_clean = DataFrame(
            :timestamp                      => df.timestamp,
            Symbol("close_", asset)         => convert.(Float64, df.close),
            Symbol("volume_spike_", asset)  => convert.(Int64,   df.volume_spike)
        )

        if i == 1
            base_df = df_clean
        else
            base_df = innerjoin(base_df, df_clean, on=:timestamp)
        end
    end

    sort!(base_df, :timestamp)
    println("[INFO] Data alignment complete. Contiguous aligned rows: $(nrow(base_df))")
    return base_df
end

# ----------------------------------------------------------------------------
# MAIN CONTROLLER
# ----------------------------------------------------------------------------
if abspath(PROGRAM_FILE) == @__FILE__

    alpha_file = "Alpha005_MacroReflexivity.jl"
    alpha_name = :Alpha005_MacroReflexivity

    active_strategy = load_alpha_plugin(alpha_file, alpha_name)

    target_assets    = active_strategy.required_tickers
    target_timeframe = active_strategy.required_timeframe

    println(
        "\n[SYSTEM] Handshake successful. " *
        "Alpha requested: $(join(target_assets, ", ")) " *
        "at $(target_timeframe) resolution."
    )

    aligned_data = load_and_align_datasets(target_assets, target_timeframe)
    run_backtest(active_strategy, aligned_data, target_assets)
end