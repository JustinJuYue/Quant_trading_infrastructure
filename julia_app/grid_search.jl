# 文件位置: Quant_trading_infrastructure/julia_app/grid_search.jl
using Dates
using Statistics
using DataFrames
using Parquet2
using Plots
plotly() # 纯净版后端，不需要 WebIO 和 PlotlyJS

# 1. 动态加载你的核心库和 Alpha，绝对不污染你的实盘代码
include("src/Core.jl")
include("src/alphas/Alpha005_1_KellyCriterionMacroReflexivity.jl")
using .QuantCore

# 2. 数据拉取 (复用你的数据加载逻辑)
function load_data(asset::String, timeframe::String)
    file_path = joinpath(@__DIR__, "..", "data", "$(asset)_$(timeframe).parquet")
    ds = Parquet2.Dataset(file_path)
    df = DataFrame(ds)
    sort!(df, :timestamp)
    return df
end

# 3. 极速静默回测引擎 (Silent Fast-Eval Engine)
function fast_eval(alpha, df, target_asset)
    cash = 100000.0
    position = 0.0
    entry_cost = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    
    # 为了极速，跳过复杂的MTM，只算盈亏比
    for i in 1:nrow(df)
        price = Float64(df[i, :close])
        data_dict = Dict(
            "timestamp" => df[i, :timestamp],
            "close" => price,
            "assets" => Dict(target_asset => Dict("close" => price))
        )

        signal = QuantCore.generate_signal(alpha, data_dict)
        action = signal["action"]
        weight = signal["weight"]

        current_equity = cash + position * price
        target_exposure = current_equity * weight
        current_exposure = position * price

        if action == "BUY" && target_exposure > current_exposure + 1.0
            to_spend = min(target_exposure - current_exposure, cash)
            if to_spend > 10.0
                qty = (to_spend * 0.999) / (price * 1.001) # 粗略扣除滑点与手续费
                position += qty
                cash -= to_spend
                entry_cost += to_spend
            end
        elseif action == "SELL" && position > 0.0
            proceeds = position * price * 0.999 * 0.999 
            pnl = proceeds - entry_cost
            if pnl > 0
                gross_profit += pnl
            else
                gross_loss += abs(pnl)
            end
            cash += proceeds
            position = 0.0
            entry_cost = 0.0
        end
    end
    
    # 计算最终 Profit Factor
    pf = gross_loss > 0.0 ? gross_profit / gross_loss : (gross_profit > 0.0 ? 5.0 : 0.0)
    return pf
end

# 4. 主控：网格遍历寻优
function run_grid_search()
    println("🚀 [Grid Search] Starting Parameter Optimization...")
    
    # 载入数据
    target_asset = "ETH_USDT"
    df = load_data(target_asset, "1h")
    
    # 定义遍历网格 (这里是你关心的三维参数)
    v_smooths = 1:1:6                     # 1到6小时平滑
    d_mins = 0.002:0.002:0.010            # 0.2% 到 1.0% 的极值偏离
    trend_thrs = 0.0001:0.0001:0.0005     # 趋势触发阈值
    
    total_runs = length(v_smooths) * length(d_mins) * length(trend_thrs)
    println("📊 Total combinations to test: $total_runs")
    
    xs = Float64[]
    ys = Float64[]
    zs = Float64[]
    pfs = Float64[]
    
    counter = 0
    best_pf = 0.0
    best_params = ()
    
    # 暴力美学：三层嵌套循环
    for v in v_smooths
        for d in d_mins
            for t in trend_thrs
                counter += 1
                if counter % 10 == 0
                    println("... Running $counter / $total_runs")
                end
                
                # 每次循环实例化一个全新的 Alpha 对象，确保状态纯净
                # 参数顺序必须与你的构造函数一致
                alpha = Alpha005_1_KellyCriterionMacroReflexivity(
                    "Alpha_Opt", 1.0, 24, 168, 
                    t, t*1.2, d, v, 12  # reversion_thr 设为 t 的 1.2倍，time_stop 固定12
                )
                
                pf = fast_eval(alpha, df, target_asset)
                
                push!(xs, v)
                push!(ys, d)
                push!(zs, t)
                push!(pfs, pf)
                
                if pf > best_pf
                    best_pf = pf
                    best_params = (v, d, t)
                end
            end
        end
    end
    
    println("\n🏆 [Search Complete] Best Profit Factor: $(round(best_pf, digits=3))")
    println("Optimal Params -> v_smooth: $(best_params[1]), d_min: $(best_params[2]), trend_thr: $(best_params[3])")
    
    # 5. 绘制 3D 盈利高原热力散点图
    println("\n🎨 Generating 3D Heatmap...")
    p = scatter3d(xs, ys, zs, 
        marker_z=pfs, 
        color=:viridis, 
        markersize=5,
        xlabel="v_smooth (Hours)", 
        ylabel="d_min (Stretch)", 
        zlabel="trend_thr",
        title="Alpha005 Profit Factor 3D Plateau",
        camera=(45, 45)
    )
    
    # 6. 保存图片到指定路径
    save_dir = "/Users/JustinJu/Desktop/Quant_Infrastructure/Quant_trading_infrastructure/julia_app/src/alphas/Results"
    mkpath(save_dir) # 如果 Results 文件夹不存在，自动创建
    
    file_name = joinpath(save_dir, "GridSearch_Alpha005_3DHeatmap_$(Dates.format(now(), "mmdd_HHMM")).html")
    # 存为 HTML 可以让你在浏览器里直接打开，用鼠标360度拖拽旋转查看这片 3D 高原！
    savefig(p, file_name) 
    
    println("✅ 3D Heatmap saved successfully to: $file_name")
end

# 运行寻优
run_grid_search()