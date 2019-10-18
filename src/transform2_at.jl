export transform2_at, @transform2_at

transform2_at(data::AnyDataFrame, pred, transform::Union{Function,Nothing}, 
        awaiting_data::Bool = false; kwargs...) = 
    transform2_at_(data, pred, awaiting_data ? transform(data) : transform; kwargs...)

transform2_at(f::Function, pred, transform::Union{Function,Nothing}, 
        awaiting_data::Bool = false; kwargs...) = 
    data -> transform2_at(f(data), pred, transform, awaiting_data; kwargs...)

transform2_at(f::Function, pred, awaiting_data::Bool = false; kwargs...) = 
    data -> transform2_at(f(data), pred, nothing, awaiting_data; kwargs...)

transform2_at(pred, transform::Function, awaiting_data::Bool = false; kwargs...) = 
    data -> transform2_at(data, pred, transform, awaiting_data; kwargs...)

transform2_at(pred, awaiting_data::Bool = false; kwargs...) = 
    data -> transform2_at(data, pred, nothing, awaiting_data; kwargs...)

transform_handler(a::AbstractDataFrame, col::Symbol, x) = x
transform_handler(a::AbstractDataFrame, col::Symbol, x::Function) = x(a[!,col])
function transform_handler(g::GroupedDataFrame, col::Symbol, x)
    group_transforms = map(zip(g.starts, g.ends)) do (s, e)
        result = transform_handler(g.parent[s:e,:], col, x)
        isa(result, AbstractArray) ? result : repeat([result], e-s+1)
    end
    @assert all(typeof.(group_transforms) .== typeof(group_transforms[1]))
    @assert sum(length.(group_transforms)) == nrow(g.parent)
    return reduce(vcat, group_transforms)
end

function transform2_at_(d::AbstractDataFrame, pred, x; _sep::String = "_", 
        _namefunc::Function = (column_name, kwarg_name) -> column_name * _sep * kwarg_name, 
        kwargs...)

    cols = names(d)[column_selectors(d, pred)]
    dyn_colnames = _namefunc.(
        repeat(string.(cols), inner = length(kwargs)),
        repeat(string.([k for (k, v) in kwargs]), outer = length(cols)))

    @assert(!any(in(string.(names(d))).(dyn_colnames)), 
        "New column names will overwrite existing columns")

    result = copy(d)
    for col in cols
        if (x != nothing)
            result[!,col] .= transform_handler(d, col, x)
        end
        for (k, v) in kwargs
            new_colname = Symbol(_namefunc(string(col), string(k)))
            result[!,new_colname] .= transform_handler(d, col, transform_handler(d, v))
        end
    end

    return result
end

function transform2_at_(g::GroupedDataFrame, pred, x; _sep::String = "_", 
        _namefunc::Function = (colname, kwname) -> colname * _sep * kwname,
        kwargs...,)

    cols = names(g)[column_selectors(g, protect_bool_array(pred)...)]
    dyn_colnames = _namefunc.(
        repeat(string.(cols), inner = length(kwargs)),
        repeat(string.([k for (k, v) in kwargs]), outer = length(cols)))

    @assert(!any(in(string.(names(g))).(dyn_colnames)), 
        "New column names will overwrite existing columns")
    
    result = copy(g.parent)
    for col in cols
        if (x != nothing)
            result[!,col] = transform_handler(g, col, x)
        end
        for (k, v) in kwargs
            new_colname = Symbol(_namefunc(string(col), string(k)))
            result[!,new_colname] = transform_handler(d, col, transform_handler(g, v))
        end
    end

    return groupby(result, g.cols)
end

function transform2_at_macro_helper(args...)
    a, kw = split_macro_args(args)

    # @transform_at(df, <pred>, x -> x)  # three positional args
    # @transform_at(df, <pred>, x = :x)  # two positional args
    # @transform_at(<pred>, x -> x)      # two positional args
    # @transform_at(<pred>, x = :x)      # one positional arg

    :($transform2_at(
        # data optional, predicate always required
        $(a[1:max(end-1,1)]...), 
        # x function optionally provided (at least 1 other arg)
        $((length(a) > 1 ? [symbol_context(a[end]), true] : [false])...),
        $(map(e -> Expr(:kw, e.args[1], symbol_context(e.args[2])), kw)...)))
end

macro transform2_at(args...)
    esc(:($(transform2_at_macro_helper(args...))))
end
