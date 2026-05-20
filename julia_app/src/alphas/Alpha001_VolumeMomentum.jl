using Dates
using ..QuantCore  # Import the standard interface

"""
    Alpha001_VolumeMomentum

A pure alpha logic struct. 
Can hold strategy-specific parameters (e.g., weights, thresholds) initialized during loading.
"""
struct Alpha001_VolumeMomentum <: AbstractAlpha
    strategy_name::String
    weight_allocation::Float64
end

# Default constructor for easy plug-and-play
Alpha001_VolumeMomentum() = Alpha001_VolumeMomentum("Alpha001_VolMo", 0.5)

"""
    generate_signal(alpha::Alpha001_VolumeMomentum, data::Dict) -> Dict

Implementation of the standardized interface. 
Evaluates the incoming market slice and outputs an actionable signal dictionary.
"""
function QuantCore.generate_signal(alpha::Alpha001_VolumeMomentum, data::Dict)::Dict
    # 1. Extract features pushed from the Python pipeline
    ticker = get(data, "ticker", "UNKNOWN")
    close_price = get(data, "close", 0.0)
    vol_spike = get(data, "volume_spike", 0)
    
    # 2. Core Alpha Logic: React to volume anomaly
    action = "HOLD"
    target_weight = 0.0
    
    # 在 Alpha001_VolumeMomentum.jl 的 generate_signal 函数里加一点逻辑供回测跑通：
    if vol_spike == 1
        action = "BUY"
        target_weight = alpha.weight_allocation
    elseif vol_spike == 2 # mock数据里随机生成的卖出信号
        action = "SELL"
        target_weight = 0.0
    end
    
    # 3. Standardized Signal Output
    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => ticker,
        "action" => action,
        "weight" => target_weight,
        "price_at_signal" => close_price,
        "timestamp" => Dates.datetime2unix(now()) * 1000
    )
end