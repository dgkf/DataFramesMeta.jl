import Base.startswith, Base.endswith, Base.occursin, Base.(-), Base.(:)

# extend some generics so that they can be partially evaluated to produce 
# selector functions or valid selector inputs

function startswith(b::AbstractString)
    df::AnyDataFrame -> startswith(df, b)
end

function startswith(data::AnyDataFrame, b::AbstractString) 
    startswith.(string.(names(data)), b)
end

function endswith(b::AbstractString)
    df::AnyDataFrame -> endswith(df, b)
end

function endswith(data::AnyDataFrame, b::AbstractString)
    endswith.(string.(names(data)), b)
end

function occursin(b::AbstractString)
    df::AnyDataFrame -> occursin(df, b)
end

function occursin(data::AnyDataFrame, b::AbstractString)
    occursin.(string.(names(data)), b)
end

function (-)(y::Union{AbstractString,AbstractArray{<:AbstractString}})
    df::AnyDataFrame -> df - y
end

function (-)(y::Union{Symbol,AbstractArray{<:Symbol}})
    df::AnyDataFrame -> df - y
end

function (-)(f::Function)
    df::AnyDataFrame -> -(f(df))
end

function (-)(t::typeof(all))
    -all()
end

function (-)(data::AnyDataFrame, y::AbstractArray{<:Symbol})::Array{Int8,1}
    -in(y).(names(data)) .- 1
end

function (-)(data::AnyDataFrame, y::AbstractArray{<:AbstractString})::Array{Int8,1}
    -in(y).(string.(names(data))) .- 1
end

function (-)(data::AnyDataFrame, y::Union{AbstractString,Symbol})::Array{Int8,1}
    data - [y]
end

function (:)(a::Symbol, b::Symbol)
    df::AnyDataFrame -> findfirst(a .== names(df)):findlast(b .== names(df))
end


