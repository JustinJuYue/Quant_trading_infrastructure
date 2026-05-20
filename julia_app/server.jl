using ZMQ
using MsgPack
using Dates

# Include the core interface
include("src/Core.jl")
using .QuantCore

const ZMQ_ENDPOINT = "tcp://127.0.0.1:5555"
const ALPHAS_DIR = joinpath(@__DIR__, "src", "alphas")

"""
    load_alpha_plugin(filename::String, TypeName::Symbol) -> AbstractAlpha

Dynamically includes the alpha script and instantiates the strategy struct.
This decouples the deployment of new strategies from the server codebase.
"""
function load_alpha_plugin(filename::String, TypeName::Symbol)::AbstractAlpha
    filepath = joinpath(ALPHAS_DIR, filename)
    if !isfile(filepath)
        error("Alpha plugin not found at: $filepath")
    end
    
    # Evaluate the file in the current module
    include(filepath)
    
    # Dynamically instantiate the struct using its symbol
    alpha_instance = Base.invokelatest(eval(TypeName))
    println("[INFO] Successfully loaded Alpha Plugin: $(typeof(alpha_instance))")
    return alpha_instance
end

"""
    run_quant_server(active_alpha::AbstractAlpha)

The main event loop. Notice how it takes an `AbstractAlpha` as an argument.
Julia's JIT compiler will specialize this loop for the exact concrete type passed in,
ensuring zero-overhead dynamic dispatch during live trading.
"""
function run_quant_server(active_alpha::AbstractAlpha)
    ctx = Context()
    sock = Socket(ctx, REP)
    
    println("==================================================")
    println("🚀 Julia Quant Engine Host Started")
    println("🧠 Active Alpha: $(active_alpha.strategy_name)")
    println("📡 Listening on $ZMQ_ENDPOINT")
    println("==================================================")
    
    try
        ZMQ.bind(sock, ZMQ_ENDPOINT)
        
        while true
            msg_bytes = ZMQ.recv(sock)
            
            try
                market_data = MsgPack.unpack(msg_bytes)
                
                # --- POLYMORPHIC DISPATCH ---
                # This calls the specific logic defined in Alpha001_VolumeMomentum.jl
                signal = generate_signal(active_alpha, market_data)
                # ----------------------------
                
                # Optionally print BUY/SELL actions to the console for debugging
                if signal["action"] != "HOLD"
                    println("[$(now())] [SIGNAL] $(signal["action"]) $(signal["ticker"]) @ $(signal["price_at_signal"]) (Alpha: $(signal["alpha_id"]))")
                end
                
                ZMQ.send(sock, MsgPack.pack(signal))
                
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
# Easily swap strategies here by changing the filename and struct name
active_strategy = load_alpha_plugin("Alpha001_VolumeMomentum.jl", :Alpha001_VolumeMomentum)

# Launch the server with the injected dependency
run_quant_server(active_strategy)