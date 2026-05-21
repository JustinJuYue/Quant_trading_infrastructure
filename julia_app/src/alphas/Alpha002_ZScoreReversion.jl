using Dates
using Statistics
using ..QuantCore  # Import the standard interface

"""
    Alpha002_ZScoreReversion

A professional template for institutional quantitative strategies.
"""
mutable struct Alpha002_ZScoreReversion <: AbstractAlpha
    strategy_name::String
    
    target_weight::Float64
    windows_size::Int
    entry_z_score::Float64
    exit_z_score::Float64

    price_history::Vector{Float64}
    
    # Constructor
    function Alpha002_ZScoreReversion(
        name::String="Alpha002_ZReversion", 
        weight::Float64=0.5, 
        window::Int=100, 
        entry_z::Float64=3.5,  # 提高进场门槛，过滤杂音
        exit_z::Float64=0.0)
        new(name, weight, window, entry_z, exit_z, Float64[])
    end
end


"""
    generate_signal(alpha::Alpha002_ZScoreReversion, data::Dict) -> Dict

The core execution block. Called exactly once per tick/candle.
Returns a standardized dictionary consumed by either the Backtester or the Live OMS.
"""
function QuantCore.generate_signal(alpha::Alpha002_ZScoreReversion, data::Dict)::Dict

    ticker = get(data, "ticker", "UNKNOWN")
    current_price = get(data, "close", 0.0)
    
    push!(alpha.price_history, current_price)
    if length(alpha.price_history) > alpha.windows_size
        popfirst!(alpha.price_history)
    end

    action = "HOLD"
    final_weight = 0.0
    order_type = "MAKER" # 默认订单类型
    
    if length(alpha.price_history) == alpha.windows_size
        local_mean = mean(alpha.price_history)
        local_std = std(alpha.price_history)
        
        if local_std > 0
            current_z_score = (current_price - local_mean) / local_std
            
            # 计算回归均线预期的物理利润空间（百分比）
            expected_profit_pct = abs(local_mean - current_price) / current_price
            
            # 定义双边（买入+卖出）的 Taker 摩擦成本红线 (0.05%手续费 + 0.015%滑点) * 2 = 0.13%
            min_required_profit = 2 * (0.0005 + 0.00015)
            
            # 只有预期利润大于摩擦成本的 1.5 倍时，才允许开仓
            if expected_profit_pct > (min_required_profit * 1.5)
                
                if current_z_score < -alpha.entry_z_score
                    action = "BUY"
                    signal_strength = (abs(current_z_score) - alpha.entry_z_score) / (5.0 - alpha.entry_z_score)
                    calculated_weight = clamp(signal_strength, 0.1, 1.0)
                    final_weight = calculated_weight * alpha.target_weight
                    order_type = "TAKER" # 超跌抢反弹使用市价单
                    
                elseif current_z_score > alpha.exit_z_score
                    action = "SELL"
                    final_weight = 0.0
                    order_type = "TAKER" # 止盈或止损离场使用市价单
                end
                
            end
        end
    end
    
    # ------------------------------------------------------------------------
    # STEP 3: Standardized Signal Dispatch
    # ------------------------------------------------------------------------
    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => ticker,
        "action" => action,
        "weight" => final_weight,
        "order_type" => order_type,  # 传输订单类型给回测器
        "price_at_signal" => current_price,
        "timestamp" => Dates.datetime2unix(now()) * 1000
    )
end