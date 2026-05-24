using Dates
using Statistics
using ..QuantCore  

"""
    Alpha005_1_KellyCriterionMacroReflexivity

基于微分方程与索罗斯反身性（Reflexivity）理论的宏观趋势追踪策略。
内嵌工业级【动态波动率凯利公式 (Dynamic Volatility-Scaled Kelly Criterion)】。
仓位大小 = 基础凯利值 × (宏观基准波动率 / 局部当前波动率)
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
        fast::Int=24,   # 1天的短期微观记忆
        slow::Int=168,  # 1周的长期宏观背景
        threshold::Float64=0.0002) # 反身性乘数阈值
        
        tickers = ["ETH_USDT"]
        timeframe = "1m"
        
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
    
    # 默认状态初始化
    action = "HOLD"
    final_weight = 0.0
    order_type = "MARKET" 
    msg = "Data collecting..."
    
    # 2. 只有当慢周期窗口装满时（168根K线），才开始进行微分方程推演
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
            
            # 微分方程边界条件与反馈气泡判断
            if R_t > alpha.reflexivity_threshold
                if D_t > 0
                    # 价格在均线之上，且正在加速向上（向上的正反馈泡沫确立）
                    action = "BUY"
                elseif D_t < 0
                    # 价格在均线之下，且正在加速向下（向下的恐慌螺旋）
                    action = "SELL"
                end
            elseif R_t < 0
                # 引力大于推力，反身性动能衰竭，趋势崩塌 -> 平仓
                action = "SELL"
            end
        end
    end

    # ========================================================================
    # 🚀 核心升级：动态波动率凯利拦截器 (Dynamic Volatility-Scaled Kelly) 🚀
    # ========================================================================
    if action == "BUY"
        # A. 基础凯利参数设置（来源于全仓无风控大周期的历史统计平均值）
        p = 0.50                   # 基础胜率 50%
        b = 3.60                   # 基础盈亏比 3.6
        half_kelly_fraction = 0.5  # 半凯利风控打折
        max_weight = 0.30          # 胖手指铁律：单次最大绝对仓位顶盖
        
        # 计算静态全凯利值
        full_kelly = p - ((1.0 - p) / b)
        
        if full_kelly > 0
            base_safe_kelly = full_kelly * half_kelly_fraction # 此时为静态常数 18.05%
            
            # B. 计算市场实时波动率标量 (Volatility Scalar)
            # 利用当前 slow_window 内的价格历史计算简单收益率序列
            returns = diff(alpha.price_history) ./ alpha.price_history[1:end-1]
            
            # 局部当前波动率：最近 24 小时（1天）内收益率的标准差
            current_vol = std(returns[end - alpha.fast_window + 1 : end])
            # 宏观基准波动率：整个 168 小时（1周）窗口内收益率的标准差
            baseline_vol = std(returns)
            
            # 安全防线：避免除以 0 导致系统崩溃
            vol_scalar = 1.0
            if current_vol > 0.0 && baseline_vol > 0.0
                # 核心机制：当局部波动率超过长期均值时，缩减仓位；当局部波动率极其平滑时，适度放大仓位
                vol_scalar = baseline_vol / current_vol
            end
            
            # C. 限制波动率乘数的上下边界（防止极端情况下仓位失控）
            # 最多允许在原基础上放大 1.5 倍，缩小至 0.4 倍
            vol_scalar = clamp(vol_scalar, 0.4, 1.5)
            
            # D. 最终注入动态因子
            final_weight = min(base_safe_kelly * vol_scalar, max_weight)
            
            msg = "Dynamic Kelly Active! Vol-Scalar: $(round(vol_scalar, digits=2))x | Target Weight: $(round(final_weight * 100, digits=2))%"
        else
            # 负期望熔断保护
            action = "HOLD"
            final_weight = 0.0
            msg = "Risk Intercept: Strategy base expectancy fell below zero."
        end
        
    elseif action == "SELL"
        # 卖出/平仓信号，强制清空该标的全部头寸
        final_weight = 0.0
        msg = "Reflexivity broken. Liquidation triggered."
    else
        msg = "No trigger conditions met. Holding cash."
    end
    
    # 5. 返回符合 ZMQ 和回测总线的标准化数据报
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