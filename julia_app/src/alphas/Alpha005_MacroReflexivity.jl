using Dates
using Statistics
using ..QuantCore  

"""
    Alpha005_MacroReflexivity

基于微分方程与索罗斯反身性（Reflexivity）理论的宏观趋势追踪策略。
适合 1h 或 4h 级别的数据。
"""
mutable struct Alpha005_MacroReflexivity <: AbstractAlpha
    strategy_name::String
    
    # 🤝 数据握手清单：只要 BTC 的 1 小时数据
    required_tickers::Vector{String}
    required_timeframe::String
    
    target_weight::Float64
    fast_window::Int
    slow_window::Int
    reflexivity_threshold::Float64 # 触发交易的反身性阈值
    
    price_history::Vector{Float64}
    deviation_history::Vector{Float64}
    
    function Alpha005_MacroReflexivity(
        name::String="Alpha005_Reflexivity", 
        weight::Float64=1.0, 
        fast::Int=24,   # 1天的短期记忆
        slow::Int=168,  # 1周的长期宏观记忆
        threshold::Float64=0.0002) # 反身性乘数阈值
        
        tickers = ["ETH_USDT"]
        timeframe = "1h"
        
        new(name, tickers, timeframe, weight, fast, slow, threshold, Float64[], Float64[])
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
    
    action = "HOLD"
    final_weight = 0.0
    order_type = "TAKER" 
    
    if length(alpha.price_history) == alpha.slow_window
        # 1. 计算宏观基本面 (Slow) 与 微观情绪 (Fast)
        fast_ma = mean(alpha.price_history[end - alpha.fast_window + 1 : end])
        slow_ma = mean(alpha.price_history)
        
        # 2. 计算偏离度 D(t)
        D_t = (fast_ma - slow_ma) / slow_ma
        
        push!(alpha.deviation_history, D_t)
        if length(alpha.deviation_history) > 3
            popfirst!(alpha.deviation_history)
        end
        
        if length(alpha.deviation_history) == 3
            # 3. 计算偏离速度 (一阶导数近似) V(t)
            # 使用简单的平滑差分
            V_t = alpha.deviation_history[end] - alpha.deviation_history[end-1]
            
            # 4. 核心：计算反身性动力 R(t)
            R_t = D_t * V_t
            
            # 5. 微分方程边界条件判断
            if R_t > alpha.reflexivity_threshold
                if D_t > 0
                    # 价格在均线之上，且正在加速向上（向上的正反馈气泡）
                    action = "BUY"
                    final_weight = alpha.target_weight
                elseif D_t < 0
                    # 价格在均线之下，且正在加速向下（向下的恐慌螺旋）
                    action = "SELL"
                    final_weight = 0.0 # 清仓止损
                end
            elseif R_t < 0
                # 引力大于推力，反身性被破坏，趋势正在衰减
                # 无论之前是做多还是做空，动力丧失时选择清仓观望
                action = "SELL"
                final_weight = 0.0
            end
        end
    end
    
    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => "MULTIPLE",
        "action" => action,
        "weight" => final_weight,
        "order_type" => order_type,
        "trade_asset" => target_token,  
        "price_at_signal" => current_price,
        "timestamp" => get(data, "timestamp", Dates.datetime2unix(now()) * 1000)
    )
end