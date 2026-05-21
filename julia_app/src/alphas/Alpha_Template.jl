using Dates
using ..QuantCore  # Import the standard interface

"""
    Alpha_Template

A professional template for institutional quantitative strategies.
Designed to hold state variables, statistical parameters, or memory arrays 
needed for complex mathematical models (e.g., Kalman filters, moving matrices).
"""
mutable struct Alpha_Template <: AbstractAlpha
    strategy_name::String
    

    target_weight::Float64
    z_score_threshold::Float64

    price_history::Vector{Float64}
    # covariance_matrix::Matrix{Float64}  # Example for more advanced linear algebra models
    
    # Constructor
    function Alpha_Template(name::String="My_First_Alpha", weight::Float64=0.5, threshold::Float64=2.0)
        new(name, weight, threshold, Float64[])
    end
end


"""
    generate_signal(alpha::Alpha_Template, data::Dict) -> Dict

The core execution block. Called exactly once per tick/candle.
Returns a standardized dictionary consumed by either the Backtester or the Live OMS.
"""
function QuantCore.generate_signal(alpha::Alpha_Template, data::Dict)::Dict

    ticker = get(data, "ticker", "UNKNOWN")
    current_price = get(data, "close", 0.0)
    
    # Update memory buffer (e.g., keeping only the last 100 observations)
    push!(alpha.price_history, current_price)
    if length(alpha.price_history) > 100
        popfirst!(alpha.price_history)
    end

    action = "HOLD"
    final_weight = 0.0
    
    # Ensure we have enough data to perform meaningful statistical calculations
    if length(alpha.price_history) == 100
        
        # --- YOUR MATH GOES HERE ---
        pass # Remove this when adding actual logic
        
    end


    
    # ------------------------------------------------------------------------
    # Standardized Signal Dispatch
    # DO NOT MODIFY THIS BLOCK. The OMS and Backtester depend on this strict schema.
    # ------------------------------------------------------------------------
    return Dict(
        "alpha_id" => alpha.strategy_name,
        "ticker" => ticker,
        "action" => action,
        "weight" => final_weight,
        "price_at_signal" => current_price,
        "timestamp" => Dates.datetime2unix(now()) * 1000
    )
end