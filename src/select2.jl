export select2, @select2, everything
import Base.startswith, Base.(-), Base.(:)

# Approach:
# - Any valid select syntax is handled via `selector` dispatch
# - Dispatch on first argument is used to return either a 
#     (1) result of a selection if data::DataFrame
#     (2) curried function for piping of data
# - Considering declaring new internal types to restrict dispatch of generics 
#     for select-specific usage

# predicate evaluation functions
# - all predicates return an Array{Int8,N} where N = length(names(data))
# - integer values are +1 if column is to be selected, or -1 if column is to 
#   be removed from selection

# methods to extend binary functions as predicate-style function constructors
(-)(y::Union{Symbol,Array{Symbol,1}}) = df -> df - y
(-)(f::Function) = df -> -(f(df))
# (:)(a::Symbol, b::Symbol) = df -> selector(df, findfirst(a .== names(df)):findlast(b .== names(df)))
startswith(b::AbstractString) = df -> startswith(df, b)
endswith(b::AbstractString) = df -> endswith(df, b)
occursin(b::AbstractString) = df -> occursin(df, b)
everything() = df -> repeat([true], length(names(df)))

# extend some generics so that they can be partially evaluated to produce 
# selector functions
startswith(data::Union{AbstractDataFrame,GroupedDataFrame}, b::AbstractString) = 
    startswith.(string.(names(data)), b)
endswith(data::Union{AbstractDataFrame,GroupedDataFrame}, b::AbstractString) = 
    endswith.(string.(names(data)), b)
occursin(data::Union{AbstractDataFrame,GroupedDataFrame}, b::AbstractString) = 
    occursin.(string.(names(data)), b)
(-)(data::Union{AbstractDataFrame,GroupedDataFrame}, y::Array{Symbol,1})::Array{Int8,1} = 
    -in(y).(names(data))
(-)(data::Union{AbstractDataFrame,GroupedDataFrame}, y::Array{T,1} where T<:AbstractString)::Array{Int8,1} = 
    -in(y).(string.(names(data)))
(-)(data::Union{AbstractDataFrame,GroupedDataFrame}, y::Symbol)::Array{Int8,1} =
    data - [y]

# define selector syntax behaviors
selector(data::GroupedDataFrame, arg) = selector(data.parent, arg)
function selector(data::GroupedDataFrame, arg::Function)
    try selector(data, arg(data))  # for group-specific funcs, e.g. groupvars
    catch error
        if (error isa MethodError) selector(data.parent, arg) end
    end
end

# selectors defined on data frames
selector(data::AbstractDataFrame, arg) = arg  # fallthrough
selector(data::AbstractDataFrame, arg::Function) = selector(data, arg(data))
selector(data::AbstractDataFrame, arg::Int) = selector(data, [arg])
selector(data::AbstractDataFrame, arg::UnitRange) = selector(data, [arg...])
selector(data::AbstractDataFrame, arg::Array{Int,1}) = in(arg).(1:length(names(data)))
selector(data::AbstractDataFrame, arg::Union{Symbol,AbstractString}) = selector(data, [arg])
selector(data::AbstractDataFrame, arg::Array{Symbol,1}) = -1 .* (-)(arg)(data)
selector(data::AbstractDataFrame, arg::Array{String,1}) = in(arg).(string.(names(data)))
selector(data::AbstractDataFrame, arg::DataType) = (<:).(eltype.(eachcol(data)), arg)
selector(data::AbstractDataFrame, arg::Regex) = match.(arg, string.(names(data))) .!= nothing

# use multiple dispatch to determine whether function should be "curried"
select2(data::Union{AbstractDataFrame,GroupedDataFrame}, args...) = select2_(data, args...)
select2(args...) = data -> select2_(data, args...)

# helper for converting list to spoken sentence 
function spoken_list(list::AbstractArray, qchar::String="")
    s = string.(list)
    seps = reverse([i == 1 ? " and " : ", " for (i, si) in enumerate(s[2:end])])
    qchar * s[1] * qchar * string((seps .*  qchar .* s[2:end] .* qchar)...)
end

# subsetting helpers
subset_columns(data::AbstractDataFrame, cols) = data[!,cols]
function subset_columns(data::GroupedDataFrame, cols)
    excluded_groupvars = setdiff(groupvars(data), names(data)[cols])
    if (length(excluded_groupvars) > 0)
        @warn("Automatically adding grouping variable" * 
            (length(excluded_groupvars) > 1 ? "s " : " ") *
            spoken_list(excluded_groupvars, "'") * ". " *
            "To avoid warnings, add `groupvars` to selections.")
    end
    map(sdf -> sdf[:,cols], data)
end

# define unexported version to do the heavy lifting
function select2_(data::Union{AbstractDataFrame,GroupedDataFrame}, args...)
    ncol_data = length(names(data))
    selection = repeat([false], ncol_data)
    for (i, arg) in enumerate(args)
        # convert selection argument to column "predicate" array of (-1, 0, 1)
        # run twice, such that any user function which returns a type that 
        # can be handled gets converted to preferred output
        pred = selector(data, arg)

        @assert(maximum(pred) - minimum(pred) <= 1 && maximum(abs.(pred)) <= 1,
            "selectors must either add or remove selected columns, but cannot do both.")

        # handle case where 1st argument is removal (assume initial select all)
        if (i == 1 && minimum(pred) == -1) selection .+= 1 end
        selection = min.(max.(selection .+ pred, 0), 1)
    end
    subset_columns(data, convert(Array{Bool,1}, selection))
end
