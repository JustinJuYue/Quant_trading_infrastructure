using Dates
using ..QuantCore  

"""
    Alpha003_PriceKinematics

升级版：完美兼容多资产数据总线架构，包含降噪与交易成本防火墙。
"""
mutable struct Alpha003_PriceKinematics <: AbstractAlpha
    strategy_name::String
    
    # 🚨 新架构必填参数：策略的“数据握手清单”
    required_tickers::Vector{String}
    required_timeframe::String
    
    target_weight::Float64
    k_window::Int
    price_history::Vector{Float64}
    velocity_history::Vector{Float64}
    
    # Constructor
    function Alpha003_PriceKinematics(
        name::String="Alpha003_Kinematics", 
        weight::Float64=1.0, 
        k::Int=15)
        
        # 声明策略主权：我只要 BTC 的 1 小时级别数据
        tickers = ["BTC_USDT"]
        timeframe = "1h"
        
        new(name, tickers, timeframe, weight, k, Float64[], Float64[])
    end
end


"""
    generate_signal
"""
function QuantCore.generate_signal(alpha::Alpha003_PriceKinematics, data::Dict)::Dict

    # 1. 防御性读取：从多资产总线中精准提取自己需要的数据
    target_token = alpha.required_tickers[1] 
    assets_branch = get(data, "assets", Dict())
    
    if haskey(assets_branch, target_token)
        current_price = assets_branch[target_token]["close"]
    else
        # 兼容性兜底，万一在没有资产树的旧环境运行
        current_price = get(data, "close", 0.0)
    end
    
    # 2. 状态与内存更新 (多预留 3 根 K 线用于后期平滑降噪)
    required_len = alpha.k_window + 3
    push!(alpha.price_history, current_price)
    if length(alpha.price_history) > required_len
        popfirst!(alpha.price_history)
    end
    
    action = "HOLD"
    final_weight = 0.0
    order_type = "TAKER" # 动量突破策略只能市价追进
    
    if length(alpha.price_history) == required_len
        # 3. 降噪过滤：计算均价以过滤单根 K 线的尖刺噪音
        smoothed_current = sum(alpha.price_history[end-2:end]) / 3.0
        smoothed_past = sum(alpha.price_history[1:3]) / 3.0
        
        v_t = smoothed_current - smoothed_past

        push!(alpha.velocity_history, v_t)
        if length(alpha.velocity_history) > alpha.k_window + 1
            popfirst!(alpha.velocity_history)
        end

        if length(alpha.velocity_history) == alpha.k_window + 1
            a_t = v_t - alpha.velocity_history[1]

            # 4. 交易成本防火墙
            # 必须保证加速度带来的预期物理空间，大于完整的 Taker 手续费 + 滑点的双边成本
            min_required_move = current_price * 2 * (0.0005 + 0.00015)

            if v_t > min_required_move && a_t > min_required_move
                action = "BUY"
                # 规范化信号强度
                signal_strength = a_t / (current_price * 0.01) 
                calculated_weight = clamp(signal_strength, 0.1, 1.0)
                final_weight = calculated_weight * alpha.target_weight
                
            elseif v_t < -min_required_move && a_t < -min_required_move
                action = "SELL"
                final_weight = 0.0 # 动力枯竭，通知回测器清仓
            end
        end
    end
    
    # ------------------------------------------------------------------------
    # STEP 3: Standardized Signal Dispatch
    # ------------------------------------------------------------------------
    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => "MULTIPLE",
        "action" => action,
        "weight" => final_weight,
        "order_type" => order_type,
        "trade_asset" => target_token,  # <--- 明确打上路由标签，告诉回测器对谁下单
        "price_at_signal" => current_price,
        "timestamp" => get(data, "timestamp", Dates.datetime2unix(now()) * 1000)
    )
end