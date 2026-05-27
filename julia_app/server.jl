# ============================================================================
# Industrial-Grade Real-Time Quant Engine Host
# 架构特性: 多策略路口复用分发 (State hydration offloaded to Python ZMQ)
# ============================================================================

using ZMQ
using MsgPack
using Dates

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
    run_quant_server
"""
function run_quant_server(active_alphas::Vector{AbstractAlpha})
    ctx = Context()
    sock = Socket(ctx, REP)
    
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
                
                # Check metadata to prevent noisy log spam during Python's ZMQ pre-flight hydration payload delivery
                is_hydrate = get(market_data, "type", "") == "HYDRATE"
                
                if !is_hydrate
                    println("[$(now())] Received $incoming_tf candle for $incoming_ticker. Evaluating Alpha Fleet...")
                end
                
                combined_reply = Dict("action" => "HOLD", "msg" => "No strategy triggered.")
                
                # 多策略多路复用并行路由
                for alpha in active_alphas
                    if (incoming_ticker in alpha.required_tickers || incoming_ticker == "MULTIPLE") && 
                       incoming_tf == alpha.required_timeframe
                        
                        signal = generate_signal(alpha, market_data)
                        
                        if signal["action"] != "HOLD"
                            if !is_hydrate
                                println("[$(now())] [🚨 SIGNAL TRIGGERED] $(signal["action"]) $(signal["trade_asset"]) @ $(signal["price_at_signal"]) (Alpha: $(signal["alpha_id"]))")
                            end
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
    
    # 挂载 1 分钟价格动力学策略
    load_alpha_plugin("Alpha003_PriceKinematics.jl", :Alpha003_PriceKinematics)
]

run_quant_server(alpha_fleet)