# Import required packages
using ZMQ
using MsgPack
using Dates

const ZMQ_ENDPOINT = "tcp://127.0.0.1:5555"

"""
    process_market_data(data::Dict) -> Dict

Simulate the core logic of the strategy engine. Receives a dictionary and returns a trading signal.
Keep this function pure to facilitate future Just-In-Time (JIT) compilation optimizations.
"""
function process_market_data(data::Dict)
    # In a live trading environment, this is where your Kalman Filters, 
    # matrix operations, or Stochastic Differential Equations (SDEs) will reside.
    ticker = get(data, "ticker", "UNKNOWN")
    last_price = get(data, "last_price", 0.0)
    
    # Print the features of the received slice (Replace with async logging in production)
    println("[$(now())] Received Data -> Ticker: $ticker, Price: $last_price")
    
    # Construct and return a mocked trading signal
    signal = Dict(
        "action" => "BUY",
        "ticker" => ticker,
        "weight" => 0.5,
        "timestamp" => Dates.datetime2unix(now()) * 1000,
        "engine_status" => "HEALTHY"
    )
    return signal
end

"""
    run_quant_server()

Start the ZMQ REP server, listening for market data from the Python client.
Includes comprehensive exception handling and resource management.
"""
function run_quant_server()
    ctx = Context()
    sock = Socket(ctx, REP)
    
    println("==================================================")
    println("🚀 Julia Quant Math Engine Started")
    println("📡 Listening for Python Market Data on $ZMQ_ENDPOINT")
    println("==================================================")
    
    try
        ZMQ.bind(sock, ZMQ_ENDPOINT)
        
        # Main event loop
        while true
            # Block and wait for binary data from Python
            msg_bytes = ZMQ.recv(sock)
            
            try
                # 1. Deserialize MessagePack to Julia Dict
                market_data = MsgPack.unpack(msg_bytes)
                
                # 2. Strategy computation
                signal = process_market_data(market_data)
                
                # 3. Serialize and send response
                reply_bytes = MsgPack.pack(signal)
                ZMQ.send(sock, reply_bytes)
                
            catch e
                # Catch errors during a single request to prevent the entire engine from crashing
                println(stderr, "[$(now())] [ERROR] Data processing exception: ", e)
                
                # Must return an error response to unblock the Python client
                error_reply = MsgPack.pack(Dict("action" => "ERROR", "msg" => string(e)))
                ZMQ.send(sock, error_reply)
            end
        end
    catch e
        if e isa InterruptException
            println("\n[INFO] Interrupt signal received. Gracefully shutting down Julia engine...")
        else
            println(stderr, "\n[FATAL ERROR] Engine encountered a fatal error: ", e)
        end
    finally
        # Resource cleanup to prevent zombie processes from holding the port
        close(sock)
        close(ctx)
        println("[INFO] ZMQ resources released. Process terminated.")
    end
end

# Run the run_quant_server
run_quant_server()