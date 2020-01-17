# Base extensions
import Base.startswith, Base.endswith, Base.occursin, Base.(-), Base.(:), 
       Base.show

struct UnitRange{T<:Symbol} <: AbstractUnitRange{T}
    start::T
    stop::T
end

Base.show(io::IO, ::MIME"text/plain", x::UnitRange{<:Symbol}) = 
    println("(:" * string(x.start) * " : :" * string(x.stop), ")")

(:)(start::Symbol, stop::Symbol) = UnitRange(start, stop)

function startswith(b::AbstractString)
    x -> startswith(x, b)
end

function startswith(data::AnyDataFrame, b::AbstractString) 
    startswith.(string.(names(data)), b)
end

function endswith(b::AbstractString)
    x -> endswith(x, b)
end

function endswith(data::AnyDataFrame, b::AbstractString)
    endswith.(string.(names(data)), b)
end

function occursin(b::AbstractString)
    x -> occursin(x, b)
end

function occursin(data::AnyDataFrame, b::AbstractString)
    occursin.(string.(names(data)), b)
end

function (-)(y::Union{AbstractString,AbstractArray{<:AbstractString}})
    x -> x - y
end

function (-)(y::Union{Symbol,AbstractArray{<:Symbol}})
    x -> x - y
end

function (-)(f::Function)
    x -> -(f(x))
end

function (-)(t::typeof(all))
    -all()
end

function (-)(data::AnyDataFrame, y::AbstractArray{<:Symbol})::Array{Int8,1}
    -in(y).(names(data))
end

function (-)(data::AnyDataFrame, y::AbstractArray{<:AbstractString})::Array{Int8,1}
    -in(y).(string.(names(data)))
end

function (-)(data::AnyDataFrame, y::Union{AbstractString,Symbol})::Array{Int8,1}
    data - [y]
end

