using Dates
using Statistics
using ..QuantCore

"""
    Alpha005_MacroReflexivity (Continuous Kelly / Production Ready)

Dual-regime switching strategy with recent extreme extension stop-loss and time stop.
Generates vol_scalar (volatility scalar) and passes it through ZMQ to the Python live risk management system.
"""
mutable struct Alpha005_MacroReflexivity <: AbstractAlpha
    strategy_name::String
    required_tickers::Vector{String}
    required_timeframe::String

    target_weight::Float64
    fast_window::Int
    slow_window::Int

    trend_threshold::Float64       
    reversion_threshold::Float64   

    d_min::Float64                 
    v_smooth_window::Int           
    mr_time_stop::Int              

    position_status::String 
    mr_stop_price::Float64 
    bars_in_trade::Int             

    price_history::Vector{Float64}
    deviation_history::Vector{Float64}

    function Alpha005_MacroReflexivity(
        name::String="Alpha005_Reflexivity",
        weight::Float64=1.0,
        fast::Int=24,
        slow::Int=168,
        trend_thr::Float64=0.00015,     
        reversion_thr::Float64=0.00025, 
        d_min_val::Float64=0.005,       
        v_smooth::Int=2,                
        time_stop::Int=12)              
        
        tickers = ["ETH_USDT"]
        timeframe = "1h"

        new(name, tickers, timeframe, weight, fast, slow, trend_thr, reversion_thr,
            d_min_val, v_smooth, time_stop,
            "FLAT", 0.0, 0, Float64[], Float64[])
    end
end

function QuantCore.generate_signal(alpha::Alpha005_MacroReflexivity, data::Dict)::Dict

    target_token = alpha.required_tickers[1]
    assets_branch = get(data, "assets", Dict())

    if haskey(assets_branch, target_token)
        current_price = assets_branch[target_token]["close"]
    else
        current_price = get(data, "close", 0.0)
    end

    push!(alpha.price_history, current_price)
    if length(alpha.price_history) > alpha.slow_window
        popfirst!(alpha.price_history)
    end

    if alpha.position_status != "FLAT"
        alpha.bars_in_trade += 1
    end

    action = "HOLD"
    final_weight = 0.0
    vol_scalar = 1.0 # Default volatility scalar
    order_type = "MARKET"
    msg = "Data collecting..."

    if length(alpha.price_history) == alpha.slow_window
        fast_ma = mean(alpha.price_history[end - alpha.fast_window + 1 : end])
        slow_ma = mean(alpha.price_history)
        D_t = (fast_ma - slow_ma) / slow_ma

        push!(alpha.deviation_history, D_t)
        hist_req = alpha.v_smooth_window + 1
        if length(alpha.deviation_history) > hist_req
            popfirst!(alpha.deviation_history)
        end

        if length(alpha.deviation_history) == hist_req
            V_t = (alpha.deviation_history[end] - alpha.deviation_history[1]) / alpha.v_smooth_window
            R_t = D_t * V_t

            # =========================================================
            # Compute volatility scalar (shared by backtest and Python live trading)
            # =========================================================
            returns = diff(alpha.price_history) ./ alpha.price_history[1:end-1]
            current_vol = std(returns[end - alpha.fast_window + 1 : end])
            baseline_vol = std(returns)
            if current_vol > 0.0 && baseline_vol > 0.0
                vol_scalar = clamp(baseline_vol / current_vol, 0.3, 1.5)
            end

            # --- State machine exit logic ---
            if alpha.position_status == "LONG_TREND"
                if R_t <= 0 || D_t < 0
                    action = "SELL"
                    alpha.position_status = "FLAT"
                    alpha.bars_in_trade = 0
                    msg = "LONG_TREND exhausted. Flatting."
                else
                    action = "HOLD"
                    msg = "Riding LONG_TREND. R_t: $(round(R_t, digits=6))"
                end

            elseif alpha.position_status == "LONG_REVERSION"
                if D_t >= 0
                    action = "SELL"
                    alpha.position_status = "FLAT"
                    alpha.bars_in_trade = 0
                    msg = "LONG_REVERSION target hit (D_t >= 0)."
                elseif current_price < alpha.mr_stop_price
                    action = "SELL"
                    alpha.position_status = "FLAT"
                    alpha.bars_in_trade = 0
                    msg = "STOP LOSS: Broken below extreme stretch."
                elseif alpha.bars_in_trade >= alpha.mr_time_stop
                    action = "SELL"
                    alpha.position_status = "FLAT"
                    alpha.bars_in_trade = 0
                    msg = "TIME STOP: Mean-reversion trade timeout."
                else
                    action = "HOLD"
                    msg = "Riding LONG_REVERSION. SL: $(round(alpha.mr_stop_price, digits=2))"
                end

            # --- State machine entry logic ---
            elseif alpha.position_status == "FLAT"
                if R_t > alpha.trend_threshold
                    if D_t > 0
                        action = "BUY"
                        alpha.position_status = "LONG_TREND"
                        alpha.bars_in_trade = 0
                        msg = "MODE: LONG_TREND TRIGGERED"
                    end
                elseif R_t < -alpha.reversion_threshold
                    if D_t < 0 && D_t < -alpha.d_min
                        action = "BUY"
                        alpha.position_status = "LONG_REVERSION"
                        alpha.bars_in_trade = 0
                        recent_low = minimum(alpha.price_history[end - alpha.fast_window + 1 : end])
                        alpha.mr_stop_price = recent_low * 0.998
                        msg = "LONG_REVERSION TRIGGERED"
                    end
                end
            end
        end
    end

    # =========================================================
    # Backtest-only local Kelly sizing (overridden by Python Order Manager in live trading)
    # =========================================================
    if action == "BUY" && (alpha.position_status == "LONG_TREND" || alpha.position_status == "LONG_REVERSION")
        p = 0.48
        b = 1.65
        half_kelly_fraction = 0.5
        max_weight = 0.25

        full_kelly = p - ((1.0 - p) / b)
        if full_kelly > 0
            base_safe_kelly = full_kelly * half_kelly_fraction
            final_weight = min(base_safe_kelly * vol_scalar, max_weight)
            msg = msg * " | Vol-Scalar: $(round(vol_scalar, digits=2))x"
        else
            action = "HOLD"
            final_weight = 0.0
            alpha.position_status = "FLAT"
            msg = "Risk intercept: negative expectancy"
        end
    elseif action == "SELL"
        final_weight = 0.0
    end

    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => "MULTIPLE",
        "action" => action,
        "weight" => final_weight,
        "vol_scalar" => vol_scalar, # Key output: pass scalar to Python live trading
        "order_type" => order_type,
        "trade_asset" => target_token,
        "price_at_signal" => current_price,
        "timestamp" => get(data, "timestamp", Dates.datetime2unix(now()) * 1000),
        "msg" => msg
    )
end