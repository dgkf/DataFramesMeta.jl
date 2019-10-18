export filter, filter!, @filter, @filter!
import Base.filter, Base.filter!, DataFrames.deleterows!, DataFrames.names


deleterows!(d::DataFrames.DataFrameRows, x) = deleterows!(parent(d), x)
names(d::DataFrames.DataFrameRows) = names(parent(d))


filter(data::AnyDataFrame, args...; kwargs...) = 
    filter_!(copy(data), args...; kwargs...)
filter(data::AnyDataFrame, x::BitArray) = filter_!(copy(data), x)
filter(data::GroupedDataFrame, args...; kwargs...) = 
    groupby(filter!(copy(parent(data)), args...; kwargs...), data.cols)
filter(data::DataFrames.DataFrameRows, args...; kwargs...) = 
    filter_!(eachrow(copy(parent(data))), args...; kwargs...)

@doc (@doc filter) 
filter!(data::AnyDataFrame, args...; kwargs...) = 
    filter_!(data, args...; kwargs...)
filter!(data::GroupedDataFrame, args...; kwargs...) = 
    groupby(filter_!(parent(data), args...; kwargs...), data.cols)



filter_!(d::AnyDataFrame, predicate::Pair; kwargs...) =
    filter_!(d, names(d)[column_selectors(d, predicate.second)]; kwargs...)
filter_!(d::AnyDataFrame, predicate::Pair, x) =
    filter_!(d, names(d)[column_selectors(d, predicate.second)]; all=x)
function filter_!(d::AnyDataFrame, cols::Array{Symbol}; 
        all::Union{Function,Bool,BitArray,AbstractArray{Bool},Nothing}=nothing, 
        any::Union{Function,Bool,BitArray,AbstractArray{Bool},Nothing}=nothing)

    all_i = predicated_filter_handler(d, cols, all, &)
    any_i = predicated_filter_handler(d, cols, any, |)
    i = (!).(all_i .& any_i)

    if length(i) == 1 (if i deleterows!(d, 1:nrow(d)) end)
    else deleterows!(d, i)
    end
    d
end
function filter_!(d::AnyDataFrame, args...)
    i = reduce((l, r) -> broadcast(&, l, r), filter_handler.([d], args))
    deleterows!(d, convert(Array{Bool}, (!).(i)))
    d
end



predicated_filter_handler(d, cols, x, agg) = x
predicated_filter_handler(d, cols, n::Nothing,  agg::Function) = true
predicated_filter_handler(d::DataFrames.DataFrameRows, cols, n::Nothing,  agg::Function) = true
function predicated_filter_handler(d, cols, f::Function, agg::Function)
    cell_results = (expecting_data(f) ? f(d) : f)(d[!,cols])
    predicated_filter_handler(d, cols, cell_results, agg)
end
function predicated_filter_handler(d::DataFrames.DataFrameRows, cols, f::Function, agg::Function)
    cell_results = (expecting_data(f) ? f(d) : f).([r[c] for r=d, c=cols])
    predicated_filter_handler(d, cols, cell_results, agg)
end
function predicated_filter_handler(d, cols, x::AnyDataFrame, agg::Function) 
    @assert(all((<:).(typeof.(eachcol(x)), AbstractArray{Bool})), 
        "all values of filtering function result must be a subtype of " *
            "AbstractArray{Bool}") 
    reduce((a, b) -> broadcast(agg, a, b), eachcol(x))
end
function predicated_filter_handler(d, cols, x::AbstractArray{Bool,2}, agg::Function) 
    reshape(mapslices(row -> reduce((l, r) -> broadcast(agg, l, r), row), x, dims=2), Val(1))
end

filter_handler(d, a::AbstractArray{Bool}) = a
filter_handler(d, a::Bool) = a
function filter_handler(d::DataFrames.DataFrameRows, a::Function)
    filter_handler(d, a.(d))
end
function filter_handler(d, a::Function)
    filter_handler(d, a(d))
end
filter_handler(d, a) = 
    error("filter conditions must resolve to Bool or a subtype of " *
        "AbstractArray{Bool}")



function filter_macro_helper(args...; inplace::Bool=false)
    a, kw = split_macro_args(args)
    a, predicate, b = split_on_pair(a)    
    :($(inplace ? filter! : filter)(
        data,
        $(predicate...),
        $(map(symbol_context, a)...),
        $(map(symbol_context, b)...);
        $(map(e -> Expr(:kw, e.args[1], symbol_context(e.args[2])), kw)...)))
end

@doc (@doc filter) 
macro filter(args...)
    esc(:(data::$AnyDataFrame -> $(filter_macro_helper(args...))))
end

@doc (@doc filter) 
macro filter!(args...)
    esc(:(data::$AnyDataFrame -> $(filter_macro_helper(args...; inplace = true))))
end