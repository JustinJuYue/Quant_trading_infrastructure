using Dates
using ..QuantCore  # Import the standard interface

"""
    Alpha003_PriceKinematics

A professional template for institutional quantitative strategies.
"""
mutable struct Alpha003_PriceKinematics <: AbstractAlpha
    strategy_name::String
    target_weight::Float64
    k_window::Int
    price_history::Vector{Float64}
    velocity_history::Vector{Float64}
    
    # Constructor
    function Alpha003_PriceKinematics(name::String="Alpha003_Kinematics", weight::Float64=1.0, k::Int=15)
        new(name, weight, k, Float64[], Float64[])
    end
end


"""
    generate_signal(alpha::Alpha003_PriceKinematics, data::Dict) -> Dict

The core execution block. Called exactly once per tick/candle.
"""
function QuantCore.generate_signal(alpha::Alpha003_PriceKinematics, data::Dict)::Dict

    ticker = get(data, "ticker", "UNKNOWN")
    current_price = get(data, "close", 0.0)
    
    # Update memory buffer (k_window + 3 来提供平滑均线计算空间)
    required_len = alpha.k_window + 3
    push!(alpha.price_history, current_price)
    if length(alpha.price_history) > required_len
        popfirst!(alpha.price_history)
    end
    
    action = "HOLD"
    final_weight = 0.0
    
    if length(alpha.price_history) == required_len
        # 1. 引入平滑：计算均价而不是只用单点价格，过滤1分钟级别的极短期噪音
        smoothed_current = sum(alpha.price_history[end-2:end]) / 3.0
        smoothed_past = sum(alpha.price_history[1:3]) / 3.0
        
        v_t = smoothed_current - smoothed_past

        push!(alpha.velocity_history, v_t)
        if length(alpha.velocity_history) > alpha.k_window + 1
            popfirst!(alpha.velocity_history)
        end

        if length(alpha.velocity_history) == alpha.k_window + 1
            a_t = v_t - alpha.velocity_history[1]

            norm_factor = current_price * 0.001
            
            # 2. 引入交易阈值：预期波动大于手续费磨损(例如 0.3%)时才交易
            cost_threshold = current_price * 0.003 

            if v_t > cost_threshold && a_t > cost_threshold
                action = "BUY"
                signal_strength = a_t / norm_factor
                # 动态计算仓位：最小买入10%，最大买入50%
                calculated_weight = clamp(signal_strength, 0.1, 0.5) 
                final_weight = calculated_weight * alpha.target_weight
                
            elseif v_t < -cost_threshold || (v_t > 0.0 && a_t < -cost_threshold)
                action = "SELL"
                final_weight = 0.0 # 卖出时目标权重设为0，通知回测器清仓
            end
        end
    end
    
    # ------------------------------------------------------------------------
    # STEP 3: Standardized Signal Dispatch
    # DO NOT MODIFY THIS BLOCK. The OMS and Backtester depend on this strict schema.
    # ------------------------------------------------------------------------
    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => ticker,
        "action" => action,
        "weight" => final_weight,  # 这里将目标仓位发送出去
        "price_at_signal" => current_price,
        "timestamp" => Dates.datetime2unix(now()) * 1000
    )
end