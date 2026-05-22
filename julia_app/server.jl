# ============================================================================
# Industrial-Grade Real-Time Quant Engine Host
# 架构特性: 状态水合热身机制 (State Hydration) | 多策略路口复用分发
# ============================================================================

using ZMQ
using MsgPack
using Dates
using DataFrames
using Parquet2

# Include the core interface
include("src/Core.jl")
using .QuantCore

const ZMQ_ENDPOINT = "tcp://127.0.0.1:5555"
const ALPHAS_DIR = joinpath(@__DIR__, "src", "alphas")

"""
    load_alpha_plugin
"""
function load_alpha_plugin(filename::String, TypeName::Symbol)::AbstractAlpha
    filepath = joinpath(ALPHAS_DIR, filename)
    if !isfile(filepath)
        error("Alpha plugin not found at: $filepath")
    end
    
    include(filepath)
    alpha_instance = Base.invokelatest(eval(TypeName))
    println("[INFO] Successfully loaded Alpha Plugin: $(typeof(alpha_instance))")
    return alpha_instance
end

"""
    hydrate_alpha_states!(active_alphas::Vector{AbstractAlpha})

🚀 工业级状态水合引擎 (State Hydration Engine)
在启动 ZMQ 监听之前，自动读取本地 Parquet 历史文件，把策略所需的内存状态完全喂满。
"""
function hydrate_alpha_states!(active_alphas::Vector{AbstractAlpha})
    println("\n==================================================")
    println("💧 INITIALIZING STATE HYDRATION (PRE-WARMUP)")
    println("==================================================")
    
    for alpha in active_alphas
        println("[HYDRATE] Preparing memory for: $(alpha.strategy_name)")
        
        target_assets = alpha.required_tickers
        tf = alpha.required_timeframe
        
        # 1. 动态对齐并读取该策略历史文件的最后 200 根 K 线作为热身基底
        # 168小时的慢窗口 + 额外富余空间，200行是单资产低频策略最稳健的水合深度
        warmup_rows = 200
        
        for asset in target_assets
            data_file = "$(asset)_$(tf).parquet"
            file_path = joinpath(@__DIR__, "..", "data", data_file)
            
            if !isfile(file_path)
                println("[WARN] Missing $data_file! Skipping hydration for this asset. Alpha will face Cold Start.")
                continue
            end
            
            # 读取历史 Parquet 文件
            ds = Parquet2.Dataset(file_path)
            df = DataFrame(ds)
            sort!(df, :timestamp)
            
            total_history = nrow(df)
            start_row = max(1, total_history - warmup_rows + 1)
            df_slice = df[start_row:end, :]
            
            println("[HYDRATE] Injecting $(nrow(df_slice)) historical bars from $data_file into $(alpha.strategy_name) memory matrix...")
            
            # 2. 模拟时序演进，静默推入策略状态机
            for row in eachrow(df_slice)
                # 严格构造与实盘完全一致的多资产嵌套数据总线结构
                assets_payload = Dict{String, Dict{String, Any}}()
                assets_payload[asset] = Dict{String, Any}(
                    "close" => convert(Float64, row.close),
                    "volume_spike" => convert(Int64, row.volume_spike)
                )
                
                mock_market_data = Dict{String, Any}(
                    "ticker" => asset,
                    "resolution" => tf,
                    "timestamp" => Dates.datetime2unix(row.timestamp) * 1000,
                    "assets" => assets_payload
                )
                
                # 默默让策略计算状态，丢弃返回的实时信号（不产生历史误下单）
                _ = generate_signal(alpha, mock_market_data)
            end
            println("[SUCCESS] $(alpha.strategy_name) memory cells are fully hydrated and warmed up for $(asset)!")
        end
    end
    println("==================================================")
    println("✅ ALL STRATEGY STATES WARMED UP. READY FOR LIVE FLOW.")
    println("==================================================\n")
end

"""
    run_quant_server
"""
function run_quant_server(active_alphas::Vector{AbstractAlpha})
    ctx = Context()
    sock = Socket(ctx, REP)
    
    # 🚨 实盘启动第一步：扣动状态水合引擎的扳机
    hydrate_alpha_states!(active_alphas)
    
    println("==================================================")
    println("📡 Julia Multi-Agent Quant Engine Host Listening on $ZMQ_ENDPOINT")
    println("==================================================")
    
    try
        ZMQ.bind(sock, ZMQ_ENDPOINT)
        
        while true
            msg_bytes = ZMQ.recv(sock)
            
            try
                market_data = MsgPack.unpack(msg_bytes)
                
                incoming_ticker = get(market_data, "ticker", "")
                incoming_tf = get(market_data, "resolution", "")
                
                # 心跳日志：让终端明确展示 Julia 正在活跃地进行路由分发
                println("[$(now())] Received $incoming_tf candle for $incoming_ticker. Evaluating Alpha Fleet...")
                
                combined_reply = Dict("action" => "HOLD", "msg" => "No strategy triggered.")
                
                # 多策略多路复用并行路由
                for alpha in active_alphas
                    if (incoming_ticker in alpha.required_tickers || incoming_ticker == "MULTIPLE") && 
                       incoming_tf == alpha.required_timeframe
                        
                        signal = generate_signal(alpha, market_data)
                        
                        if signal["action"] != "HOLD"
                            println("[$(now())] [🚨 SIGNAL TRIGGERED] $(signal["action"]) $(signal["trade_asset"]) @ $(signal["price_at_signal"]) (Alpha: $(signal["alpha_id"]))")
                            combined_reply = signal
                            break 
                        end
                    end
                end
                
                ZMQ.send(sock, MsgPack.pack(combined_reply))
                
            catch e
                println(stderr, "[$(now())] [ERROR] Signal Generation Exception: ", e)
                error_reply = MsgPack.pack(Dict("action" => "ERROR", "msg" => string(e)))
                ZMQ.send(sock, error_reply)
            end
        end
    catch e
        if e isa InterruptException
            println("\n[INFO] Gracefully shutting down Julia host...")
        else
            println(stderr, "\n[FATAL ERROR] Host crashed: ", e)
        end
    finally
        close(sock)
        close(ctx)
    end
end

# --- BOOTSTRAP SEQUENCE ---
alpha_fleet = [
    # 挂载 1 小时宏观反身性策略
    load_alpha_plugin("Alpha005_MacroReflexivity.jl", :Alpha005_MacroReflexivity),
    
    # 挂载 1 分钟价格动力学策略 (确保该策略里写了 required_timeframe=\"1m\")
    load_alpha_plugin("Alpha003_PriceKinematics.jl", :Alpha003_PriceKinematics)
]

run_quant_server(alpha_fleet)