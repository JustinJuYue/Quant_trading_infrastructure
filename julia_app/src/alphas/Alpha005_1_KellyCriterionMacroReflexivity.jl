using Dates
using Statistics
using ..QuantCore  

"""
    Alpha005_1_KellyCriterionMacroReflexivity (Regime-Switching Edition)

状态切换型 Alpha：
1. Trend Mode (趋势模式): R_t > θ_trend 触发。顺势而为 (D_t>0 做多，D_t<0 做空)。
2. Mean-Reversion Mode (均值回归模式): R_t < -θ_mr 触发。逆势反弹 (D_t>0 做空，D_t<0 做多)。
* 包含 Extreme Stretch Stop-loss：回归模式下，若价格跌破/突破近期极值则立刻止损。
* 内嵌 Continuous Volatility-Scaled Kelly 用于做多头寸的资金管理。
"""
mutable struct Alpha005_1_KellyCriterionMacroReflexivity <: AbstractAlpha
    strategy_name::String
    required_tickers::Vector{String}
    required_timeframe::String
    
    target_weight::Float64
    fast_window::Int
    slow_window::Int
    
    trend_threshold::Float64       # θ_trend: 趋势模式阈值
    reversion_threshold::Float64   # θ_mr: 均值回归阈值 (绝对值)
    
    position_status::String        # 状态机记忆
    mr_stop_price::Float64         # 均值回归专属：近期极值延伸止损价
    
    price_history::Vector{Float64}
    deviation_history::Vector{Float64}
    
    function Alpha005_1_KellyCriterionMacroReflexivity(
        name::String="Alpha005_1_KellyCriterionMacroReflexivity",
        weight::Float64=1.0,
        fast::Int=24,
        slow::Int=168,
        trend_thr::Float64=0.0001,      # 📉 调低：让趋势更容易触发 (原 0.0002)
        reversion_thr::Float64=0.00015, # 📉 调低：让均值回归更容易触发 (原 0.0002)
        d_min_val::Float64=0.004,       # 📉 减半：0.4% 的极值偏离即可抄底 (原 0.008 容易错过机会)
        v_smooth::Int=2,                # ⚡️ 核心提速：只做 2 小时平滑，极大地减少延迟！(原 5 小时太慢)
        time_stop::Int=12)              # ⏱️ 缩短扛单时间：12小时不反弹立刻走人 (原 24 小时)
        
        tickers = ["ETH_USDT"]
        timeframe = "1h"
        
        new(name, tickers, timeframe, weight, fast, slow, trend_thr, reversion_thr, 
            "FLAT", 0.0, Float64[], Float64[])
    end
end

function QuantCore.generate_signal(alpha::Alpha005_1_KellyCriterionMacroReflexivity, data::Dict)::Dict

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
    
    action = "HOLD"
    final_weight = 0.0
    order_type = "MARKET" 
    msg = "Data collecting..."
    
    if length(alpha.price_history) == alpha.slow_window
        fast_ma = mean(alpha.price_history[end - alpha.fast_window + 1 : end])
        slow_ma = mean(alpha.price_history)
        D_t = (fast_ma - slow_ma) / slow_ma
        
        push!(alpha.deviation_history, D_t)
        if length(alpha.deviation_history) > 3
            popfirst!(alpha.deviation_history)
        end
        
        if length(alpha.deviation_history) == 3
            V_t = alpha.deviation_history[end] - alpha.deviation_history[end-1]
            R_t = D_t * V_t
            
            # =========================================================
            # 🚥 状态机：Regime-Switching 逻辑核心
            # =========================================================
            
            # --- 1. 退场与持仓逻辑 (Exits & Holding) ---
            if alpha.position_status == "LONG_TREND"
                if R_t <= 0 || D_t < 0 
                    action = "SELL"
                    alpha.position_status = "FLAT"
                    msg = "LONG_TREND exhausted. Flatting position."
                else
                    action = "HOLD"
                    msg = "Riding LONG_TREND. R_t: $(round(R_t, digits=6))"
                end
                
            elseif alpha.position_status == "SHORT_TREND"
                if R_t <= 0 || D_t > 0
                    action = "BUY"  # 平空头
                    alpha.position_status = "FLAT"
                    msg = "SHORT_TREND exhausted. Covering position."
                else
                    action = "HOLD"
                    msg = "Riding SHORT_TREND."
                end
                
            elseif alpha.position_status == "LONG_REVERSION"
                if D_t >= 0
                    action = "SELL"
                    alpha.position_status = "FLAT"
                    msg = "LONG_REVERSION Target Hit (D_t >= 0)."
                elseif current_price < alpha.mr_stop_price
                    action = "SELL"
                    alpha.position_status = "FLAT"
                    msg = "🩸 STOP LOSS: Price broke below recent extreme stretch."
                else
                    action = "HOLD"
                    msg = "Riding LONG_REVERSION. Stop at $(round(alpha.mr_stop_price, digits=2))"
                end
                
            elseif alpha.position_status == "SHORT_REVERSION"
                if D_t <= 0
                    action = "BUY" # 平空头
                    alpha.position_status = "FLAT"
                    msg = "SHORT_REVERSION Target Hit (D_t <= 0)."
                elseif current_price > alpha.mr_stop_price
                    action = "BUY"
                    alpha.position_status = "FLAT"
                    msg = "🩸 STOP LOSS: Price broke above recent extreme stretch."
                else
                    action = "HOLD"
                    msg = "Riding SHORT_REVERSION. Stop at $(round(alpha.mr_stop_price, digits=2))"
                end
                
            # --- 2. 进场逻辑 (Entries) ---
            elseif alpha.position_status == "FLAT"
                
                # 模式 A: 趋势跟随 (Trend Mode)
                if R_t > alpha.trend_threshold
                    if D_t > 0
                        action = "BUY"
                        alpha.position_status = "LONG_TREND"
                        msg = "🔥 MODE: LONG_TREND TRIGGERED"
                    elseif D_t < 0
                        action = "SELL" # 试图做空
                        alpha.position_status = "SHORT_TREND"
                        msg = "🩸 MODE: SHORT_TREND TRIGGERED"
                    end
                    
                # 模式 B: 均值回归 (Mean-Reversion Mode)
                elseif R_t < -alpha.reversion_threshold
                    if D_t > 0
                        action = "SELL" # 试图做空
                        alpha.position_status = "SHORT_REVERSION"
                        # 取近期 fast_window 的最高点再加 0.2% 缓冲作为极值止损
                        recent_high = maximum(alpha.price_history[end - alpha.fast_window + 1 : end])
                        alpha.mr_stop_price = recent_high * 1.002
                        msg = "🧲 MODE: SHORT_REVERSION TRIGGERED"
                    elseif D_t < 0
                        action = "BUY"
                        alpha.position_status = "LONG_REVERSION"
                        # 取近期 fast_window 的最低点再减 0.2% 缓冲作为极值止损
                        recent_low = minimum(alpha.price_history[end - alpha.fast_window + 1 : end])
                        alpha.mr_stop_price = recent_low * 0.998
                        msg = "🧲 MODE: LONG_REVERSION TRIGGERED"
                    end
                end
            end
        end
    end

    # =========================================================
    # 🚀 资金管理：波动率凯利模块 (Volatility-Scaled Kelly)
    # =========================================================
    # ⚠️ 逻辑适配说明：由于你现有的 backtester.jl 是【只做多】(Long-Only) 架构
    # 对于 "SELL" 动作，系统会将其视为“平掉多头仓位” (平仓时 weight 置为 0.0)
    # 如果未来需要真实进行合约做空，需在回测器中加入空头会计计算。
    
    if action == "BUY" && (alpha.position_status == "LONG_TREND" || alpha.position_status == "LONG_REVERSION")
        p = 0.45                   
        b = 1.50                   
        half_kelly_fraction = 0.5  
        max_weight = 0.25          
        
        full_kelly = p - ((1.0 - p) / b)
        
        if full_kelly > 0
            base_safe_kelly = full_kelly * half_kelly_fraction
            
            returns = diff(alpha.price_history) ./ alpha.price_history[1:end-1]
            current_vol = std(returns[end - alpha.fast_window + 1 : end])
            baseline_vol = std(returns)
            
            vol_scalar = (current_vol > 0.0 && baseline_vol > 0.0) ? (baseline_vol / current_vol) : 1.0
            vol_scalar = clamp(vol_scalar, 0.3, 1.5) 
            
            final_weight = min(base_safe_kelly * vol_scalar, max_weight)
            msg = msg * " | Kelly W: $(round(final_weight*100, digits=2))%"
        else
            action = "HOLD"
            final_weight = 0.0
            alpha.position_status = "FLAT"
            msg = "Risk Intercept: Strategy expectancy is negative."
        end
        
    elseif action == "SELL" || action == "BUY"
        # 这里的 BUY 是指平空头 (SHORT Cover)，SELL 是指平多头或开空头。
        # 在目前的现货架构下，权重设为 0.0 代表清仓或静默。
        final_weight = 0.0
    end

    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => "MULTIPLE",
        "action" => action,
        "weight" => final_weight,
        "order_type" => order_type,
        "trade_asset" => target_token,  
        "price_at_signal" => current_price,
        "timestamp" => get(data, "timestamp", Dates.datetime2unix(now()) * 1000),
        "msg" => msg
    )
end