import Base.startswith, Base.endswith, Base.occursin, Base.(-), Base.(:)

# extend some generics so that they can be partially evaluated to produce 
# selector functions or valid selector inputs

function startswith(b::AbstractString)
    df::AnyDataFrame -> startswith(df, b)
end

function startswith(data::Union{AbstractDataFrame,GroupedDataFrame}, 
        b::AbstractString) 
    startswith.(string.(names(data)), b)
end

function endswith(b::AbstractString)
    df::AnyDataFrame -> endswith(df, b)
end

function endswith(data::Union{AbstractDataFrame,GroupedDataFrame}, 
        b::AbstractString)
    endswith.(string.(names(data)), b)
end

function occursin(b::AbstractString)
    df::AnyDataFrame -> occursin(df, b)
end

function occursin(data::Union{AbstractDataFrame,GroupedDataFrame}, 
        b::AbstractString)
    occursin.(string.(names(data)), b)
end

function (-)(y::Union{AbstractString,Array{AbstractString,1}})
    df::AnyDataFrame -> df - y
end

function (-)(y::Union{Symbol,Array{Symbol,1}})
    df::AnyDataFrame -> df - y
end

function (-)(f::Function)
    df::AnyDataFrame -> -(f(df))
end

function (-)(data::AnyDataFrame, y::Array{Symbol,1})::Array{Int8,1}
    -in(y).(names(data)) .- 1
end

function (-)(data::AnyDataFrame, 
        y::Array{T,1} where T<:AbstractString)::Array{Int8,1}
    -in(y).(string.(names(data))) .- 1
end

function (-)(data::AnyDataFrame, 
        y::Union{AbstractString,Symbol})::Array{Int8,1}
    data - [y]
end

function (:)(a::Symbol, b::Symbol)
    df::AnyDataFrame -> findfirst(a .== names(df)):findlast(b .== names(df))
end


