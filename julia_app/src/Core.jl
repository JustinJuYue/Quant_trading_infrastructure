module QuantCore

export AbstractAlpha, generate_signal

"""
    AbstractAlpha

The abstract base type for all Alpha strategies. 
Any new strategy must be a subtype of AbstractAlpha.
"""
abstract type AbstractAlpha end

"""
    generate_signal(alpha::AbstractAlpha, data::Dict) -> Dict

The standardized interface for signal generation.
Throws a method error if a concrete alpha struct fails to implement this function.
"""
function generate_signal(alpha::AbstractAlpha, data::Dict)::Dict
    error("CRITICAL: generate_signal() is not implemented for $(typeof(alpha))")
end

end # module