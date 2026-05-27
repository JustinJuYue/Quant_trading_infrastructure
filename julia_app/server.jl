# ============================================================================
# Industrial-Grade Real-Time Quant Engine Host
# 架构特性: 状态水合热身机制 (State Hydration) | 多策略路口复用分发
# ============================================================================

using ZMQ
using MsgPack
using Dates
using Parquet2
# [OPTIMIZATION]: Removed `using DataFrames` to save ~200MB memory and skip heavy JIT compilation

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

🚀 工业级状态水合引擎 (State Hydration Engine) - Low-Spec Optimized
Reads historical files using lightweight column access. Completely avoids sorting
to prevent single-core JIT compilation hangs.
"""
function hydrate_alpha_states!(active_alphas::Vector{AbstractAlpha})
    println("\n==================================================")
    println("💧 INITIALIZING STATE HYDRATION (PRE-WARMUP)")
    println("==================================================")
    
    # [OPTIMIZATION]: Force JIT compilation of basic operations before hydration.
    # This prevents the single-core CPU from freezing during initial data processing.
    _ = collect(1:5)
    _ = Float64(1)
    _ = Int64(1)
    
    for alpha in active_alphas
        println("[HYDRATE] Preparing memory for: $(alpha.strategy_name)")
        
        target_assets = alpha.required_tickers
        tf = alpha.required_timeframe
        
        # [OPTIMIZATION]: Reduced warmup rows to 50 (sufficient for Alpha003 & Alpha005 memory)
        warmup_rows = 50 
        
        for asset in target_assets
            try
                data_file = "$(asset)_$(tf).parquet"
                file_path = joinpath(@__DIR__, "..", "data", data_file)
                
                if !isfile(file_path)
                    println("[WARN] Missing $data_file! Skipping hydration for this asset. Alpha will face Cold Start.")
                    continue
                end
                
                # [OPTIMIZATION]: Read only needed columns directly via Parquet2
                ds = Parquet2.Dataset(file_path)
                timestamps = ds[:timestamp]
                closes = ds[:close]
                volume_spikes = ds[:volume_spike]
                
                # [OPTIMIZATION]: history_sync.py guarantees data is chronologically ordered.
                # Removed sortperm() to bypass massive LLVM JIT compilation times on 1 vCPU.
                n = length(timestamps)
                start_i = max(1, n - warmup_rows + 1)
                slice_idx = start_i:n  # Direct range, no sort needed
                
                println("[HYDRATE] Injecting $(length(slice_idx)) historical bars from $data_file into $(alpha.strategy_name) memory matrix...")
                
                # Build mock_market_data directly from column arrays
                for i in slice_idx
                    assets_payload = Dict{String, Dict{String, Any}}(
                        asset => Dict{String, Any}(
                            "close" => Float64(closes[i]),
                            "volume_spike" => Int64(volume_spikes[i])
                        )
                    )
                    
                    # Defensively handle the timestamp in case Parquet2 parses it as a DateTime instead of a Unix integer
                    raw_ts = timestamps[i]
                    ts_val = raw_ts isa DateTime ? Dates.datetime2unix(raw_ts) * 1000 : Float64(raw_ts) * 1000

                    mock_market_data = Dict{String, Any}(
                        "ticker" => asset,
                        "resolution" => tf,
                        "timestamp" => ts_val,
                        "assets" => assets_payload
                    )
                    
                    # Silently push to the state machine
                    _ = generate_signal(alpha, mock_market_data)
                end
                println("[SUCCESS] $(alpha.strategy_name) memory cells are fully hydrated and warmed up for $(asset)!")

            catch e
                println("[ERROR] Hydration failed for $(alpha.strategy_name) on $(asset): ", e)
                println("[WARN] Continuing without full hydration...")
            end
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
    
    # 挂载 1 分钟价格动力学策略 (确保该策略里写了 required_timeframe="1m")
    load_alpha_plugin("Alpha003_PriceKinematics.jl", :Alpha003_PriceKinematics)
]

run_quant_server(alpha_fleet)