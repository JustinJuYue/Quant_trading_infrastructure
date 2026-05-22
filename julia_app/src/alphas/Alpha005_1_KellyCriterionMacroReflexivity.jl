using Dates
using Statistics
using ..QuantCore  

"""
    Alpha005_1_KellyCriterionMacroReflexivity

基于微分方程与索罗斯反身性（Reflexivity）理论的宏观趋势追踪策略。
内嵌用于回测的凯利公式（Kelly Criterion）动态资金管理机制。
"""
mutable struct Alpha005_1_KellyCriterionMacroReflexivity <: AbstractAlpha
    strategy_name::String
    
    # 🤝 数据握手清单：只要 ETH_USDT 的 1 小时数据
    required_tickers::Vector{String}
    required_timeframe::String
    
    target_weight::Float64
    fast_window::Int
    slow_window::Int
    reflexivity_threshold::Float64 # 触发交易的反身性阈值
    
    price_history::Vector{Float64}
    deviation_history::Vector{Float64}
    
    function Alpha005_1_KellyCriterionMacroReflexivity(
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

function QuantCore.generate_signal(alpha::Alpha005_1_KellyCriterionMacroReflexivity, data::Dict)::Dict

    target_token = alpha.required_tickers[1] 
    assets_branch = get(data, "assets", Dict())
    
    if haskey(assets_branch, target_token)
        current_price = assets_branch[target_token]["close"]
    else
        current_price = get(data, "close", 0.0)
    end
    
    # 1. 状态机推入最新价格
    push!(alpha.price_history, current_price)
    if length(alpha.price_history) > alpha.slow_window
        popfirst!(alpha.price_history)
    end
    
    # 默认初始化
    action = "HOLD"
    final_weight = 0.0
    order_type = "MARKET" 
    msg = "Data collecting..."
    
    # 2. 只有当慢周期窗口装满时，才开始进行微分计算
    if length(alpha.price_history) == alpha.slow_window
        fast_ma = mean(alpha.price_history[end - alpha.fast_window + 1 : end])
        slow_ma = mean(alpha.price_history)
        
        # 计算偏离度 D(t)
        D_t = (fast_ma - slow_ma) / slow_ma
        
        push!(alpha.deviation_history, D_t)
        if length(alpha.deviation_history) > 3
            popfirst!(alpha.deviation_history)
        end
        
        if length(alpha.deviation_history) == 3
            # 计算偏离速度 (一阶导数近似) V(t)
            V_t = alpha.deviation_history[end] - alpha.deviation_history[end-1]
            
            # 计算反身性动力 R(t)
            R_t = D_t * V_t
            
            # 微分方程边界条件判断
            if R_t > alpha.reflexivity_threshold
                if D_t > 0
                    # 价格在均线之上，且正在加速向上（向上的正反馈气泡）
                    action = "BUY"
                elseif D_t < 0
                    # 价格在均线之下，且正在加速向下（向下的恐慌螺旋）
                    action = "SELL"
                end
            elseif R_t < 0
                # 引力大于推力，反身性被破坏，趋势正在衰减 -> 平仓
                action = "SELL"
            end
        end
    end

    # ==========================================
    # 🚀 凯利公式回测拦截器 (Kelly Sizer) 🚀
    # 在最终输出信号前，对资金进行动态分配计算
    # ==========================================
    if action == "BUY"
        p = 0.50                   # 历史胜率 (50%)
        b = 3.60                   # 历史盈亏比 (3.6)
        half_kelly_fraction = 0.5  # 半凯利风控打折系数
        max_weight = 0.30          # 胖手指拦截：单次最多投入总资金的 30%
        
        # 凯利核心公式：f = p - (1-p)/b
        full_kelly = p - ((1.0 - p) / b)
        
        if full_kelly > 0
            # 严格限位：取半凯利和最高阈值中较小的一个
            final_weight = min(full_kelly * half_kelly_fraction, max_weight)
            msg = "Kelly Active: Suggested Weight = $(round(final_weight * 100, digits=2))%"
        else
            # 如果凯利公式算出负数，说明该策略数学期望已失效，强制熔断不开仓
            action = "HOLD"
            final_weight = 0.0
            msg = "Risk Intercept: Kelly Negative Expectancy"
        end
        
    elseif action == "SELL"
        # 卖出指令通常是平掉所有多头持仓，故权重归零
        final_weight = 0.0
        msg = "Reflexivity momentum broken, flattening position."
    else
        msg = "No signal triggered."
    end
    
    # 返回标准化的交易信号字典
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