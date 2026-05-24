# ============================================================================
# Offline Event-Driven Multi-Asset Backtester
# Architecture: Strategy-Driven Data Loading (Handshake Protocol) |
#               Nested Asset Routing | Exchange-Grade Fee Modelling
# ============================================================================

using DataFrames
using Statistics
using Dates
using Random
using Parquet2

# Include the standard interface without modification.
include("src/Core.jl")
using .QuantCore

const ALPHAS_DIR = joinpath(@__DIR__, "src", "alphas")

# ----------------------------------------------------------------------------
# STEP 1: Dynamic Alpha Plugin Loader
# ----------------------------------------------------------------------------

"""
    load_alpha_plugin(filename, struct_name) -> AbstractAlpha

Dynamically load, compile, and instantiate an alpha plugin from the alphas
directory. Raises an error if the specified file does not exist.
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

"""
    run_backtest(alpha, aligned_df, target_assets)

Execute an event-driven backtest over the provided aligned dataset.

Iterates row-by-row, constructing a nested asset payload for each bar,
routing it through the alpha's signal generator, and executing the
resulting order against a simulated exchange with realistic fee and
slippage modelling. Produces a full institutional-grade performance report
upon completion.
"""
function run_backtest(alpha::AbstractAlpha, aligned_df::DataFrame, target_assets::Vector{String})

    # --- Exchange Parameters (Kraken Futures perpetual contract fee schedule) ---
    initial_capital = 100_000.0
    usdt_balance    = initial_capital

    # Per-asset position and cost basis ledgers.
    positions   = Dict{String, Float64}(asset => 0.0 for asset in target_assets)
    entry_costs = Dict{String, Float64}(asset => 0.0 for asset in target_assets)

    fee_maker        = 0.0002   # 0.02% limit order (maker) fee rate
    fee_taker        = 0.0005   # 0.05% market order (taker) fee rate
    slippage_taker   = 0.00015  # 0.015% estimated market order slippage

    # --- Telemetry and State Tracking ---
    equity_curve = Float64[]
    sizehint!(equity_curve, nrow(aligned_df))
    total_trades = 0
    gross_profit = 0.0
    gross_loss   = 0.0

    println("[INFO] Starting multi-asset data bus event loop...")

    # --------------------------------------------------------------------------
    # Core Event Loop: Strict row-by-row iteration (no look-ahead)
    # --------------------------------------------------------------------------
    for row in eachrow(aligned_df)

        # 1. Construct the nested asset routing payload for this bar.
        #    Each asset slice carries its own close price and volume spike flag.
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

        # 2. Feed the market slice into the alpha and retrieve the standardised signal.
        signal = generate_signal(alpha, market_data)

        action        = get(signal, "action",       "HOLD")
        target_weight = get(signal, "weight",        0.0)
        order_type    = get(signal, "order_type",   "TAKER")
        trade_asset   = get(signal, "trade_asset",   target_assets[1])

        actual_fee      = order_type == "MAKER" ? fee_maker      : fee_taker
        actual_slippage = order_type == "MAKER" ? 0.0            : slippage_taker

        # 3. Mark-to-market: compute total portfolio equity across all assets.
        asset_value = 0.0
        for asset in target_assets
            asset_value += positions[asset] * row[Symbol("close_", asset)]
        end
        current_equity = usdt_balance + asset_value

        # 4. Compare target exposure against current exposure for the trade asset.
        target_exposure  = current_equity * target_weight
        current_exposure = positions[trade_asset] * row[Symbol("close_", trade_asset)]

        # --- Order Execution Logic ---
        if action == "BUY" && target_exposure > current_exposure + 1.0
            funds_to_spend = target_exposure - current_exposure
            funds_to_spend = min(funds_to_spend, usdt_balance)

            if funds_to_spend > 10.0
                exec_price    = row[Symbol("close_", trade_asset)] * (1.0 + actual_slippage)
                usable_funds  = funds_to_spend * (1.0 - actual_fee)
                qty_bought    = usable_funds / exec_price

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

                # Attribute cost basis proportionally to the quantity being sold.
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

        # Record the total portfolio equity for this bar.
        latest_asset_value = 0.0
        for asset in target_assets
            latest_asset_value += positions[asset] * row[Symbol("close_", asset)]
        end
        push!(equity_curve, usdt_balance + latest_asset_value)
    end

    # --------------------------------------------------------------------------
    # End-of-Run Liquidation: Force-close all remaining positions at final price.
    # --------------------------------------------------------------------------
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
            positions[asset]   = 0.0
            total_trades      += 1
        end
    end

    final_equity        = usdt_balance
    equity_curve[end]   = final_equity

    # --------------------------------------------------------------------------
    # STEP 3: Institutional-Grade Performance Evaluation
    # --------------------------------------------------------------------------
    total_return_pct = ((final_equity - initial_capital) / initial_capital) * 100

    # Maximum drawdown calculation over the full equity curve.
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

    # Derive the number of calendar days elapsed based on the strategy timeframe.
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

    # Risk-free rate annualised at 4%, scaled to per-minute frequency.
    Rf     = 0.04
    Rf_min = Rf / 525_600.0
    mu     = length(returns) > 0 ? mean(returns)  : 0.0
    sigma  = length(returns) > 1 ? std(returns)   : 0.0

    # Annualised Sharpe ratio scaled to per-minute bar frequency.
    sharpe_ratio = sigma > 0.0 ? ((mu - Rf_min) / sigma) * sqrt(525_600.0) : 0.0

    # Annualised Sortino ratio using downside deviation only.
    sortino_ratio = 0.0
    if length(returns) > 0
        downside_sq_sum = sum(r < Rf_min ? (r - Rf_min)^2 : 0.0 for r in returns)
        sigma_d         = sqrt(downside_sq_sum / length(returns))
        sortino_ratio   = sigma_d > 0.0 ? ((mu - Rf_min) / sigma_d) * sqrt(525_600.0) : 0.0
    end

    calmar_ratio  = max_dd_decimal > 0.0 ? annualized_return / max_dd_decimal : 0.0
    profit_factor = gross_loss > 0.0 ? gross_profit / gross_loss :
                    (gross_profit > 0.0 ? Inf : 0.0)

    println("\n==================================================")
    println("  QUANT MULTI-ASSET BACKTEST REPORT")
    println("  Alpha Engine : $(alpha.strategy_name)")
    println("  Assets       : $(join(target_assets, " | "))")
    println("  Resolution   : $(alpha.required_timeframe)")
    println("==================================================")
    println("  Initial Capital   : \$$(string(initial_capital))")
    println("  Final Equity      : \$$(round(final_equity, digits=2))")
    println("  Duration (Days)   : $(round(days, digits=2))")
    println("  Total Trades      : $(total_trades)")
    println("--------------------------------------------------")
    println("  Total Return      : $(round(total_return_pct, digits=2)) %")
    println("  Annualized Return : $(round(annualized_return * 100, digits=2)) %")
    println("  Max Drawdown      : $(round(max_dd_pct, digits=2)) %")
    println("--------------------------------------------------")
    println("  Sharpe Ratio (Ann): $(round(sharpe_ratio, digits=3))")
    println("  Sortino Ratio(Ann): $(round(sortino_ratio, digits=3))")
    println("  Calmar Ratio      : $(round(calmar_ratio, digits=3))")
    println("  Profit Factor     : $(profit_factor == Inf ? "Inf" : string(round(profit_factor, digits=3)))")
    println("==================================================\n")
end

# ----------------------------------------------------------------------------
# STEP 4: Strategy-Driven Data Ingestion and Alignment
# ----------------------------------------------------------------------------

"""
    load_and_align_datasets(assets, timeframe) -> DataFrame

Load per-asset parquet files as specified by the active strategy's declared
requirements, then inner-join them on the shared timestamp column to produce
a single contiguous aligned dataset.

Raises a fatal error if any required data file is absent, prompting the user
to run the Python history synchronisation script.
"""
function load_and_align_datasets(assets::Vector{String}, timeframe::String)
    base_df = DataFrame()

    for (i, asset) in enumerate(assets)
        # File names are derived entirely from strategy requirements.
        # No hard-coded paths; the handshake protocol drives all data loading.
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

        # Retain only the columns required by the backtester routing engine.
        df_clean = DataFrame(
            :timestamp                      => df.timestamp,
            Symbol("close_", asset)         => convert.(Float64, df.close),
            Symbol("volume_spike_", asset)  => convert.(Int64,   df.volume_spike)
        )

        if i == 1
            base_df = df_clean
        else
            # Inner join ensures only timestamps present in all assets are retained.
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

    # Configuration: specify the alpha plugin file and its struct name.
    # The data loading and alignment pipeline is driven entirely by the
    # strategy's declared requirements via the handshake protocol.
    alpha_file = "Alpha005_1_KellyCriterionMacroReflexivity.jl"
    alpha_name = :Alpha005_1_KellyCriterionMacroReflexivity

    # Step 1: Compile and instantiate the alpha plugin.
    active_strategy = load_alpha_plugin(alpha_file, alpha_name)

    # Step 2: Execute the handshake — read the strategy's data requirements.
    target_assets    = active_strategy.required_tickers
    target_timeframe = active_strategy.required_timeframe

    println(
        "\n[SYSTEM] Handshake successful. " *
        "Alpha requested: $(join(target_assets, ", ")) " *
        "at $(target_timeframe) resolution."
    )

    # Step 3: Load and align all datasets as declared by the strategy.
    aligned_data = load_and_align_datasets(target_assets, target_timeframe)

    # Step 4: Execute the backtest.
    run_backtest(active_strategy, aligned_data, target_assets)
end