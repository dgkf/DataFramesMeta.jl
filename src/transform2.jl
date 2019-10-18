export transform2, @transform2

transform2(data::Union{AbstractDataFrame,GroupedDataFrame}, args...; kwargs...) = 
    transform2_(data, args...; kwargs...)
transform2(f::Function, args...; kwargs...) = 
    data -> transform2(f(data), args...; kwargs...)
transform2(args...; kwargs...) = 
    data -> transform2(data, args...; kwargs...)

function transform2_(data::AbstractDataFrame; kwargs...)
    result = copy(data)
    for (k, v) in kwargs
        result[!,k] .= transform_handler(data, v)
    end
    return result
end

function transform2_(data::GroupedDataFrame; kwargs...)
    result = copy(data.parent)
    for (k, v) in kwargs
        result[!,k] = transform_handler(data, v)
    end
    return groupby(result, groupvars(data))
end

function transform2_macro_helper(args...)
    a, kw = split_macro_args(args)
    :($transform2(
        $(a...), 
        $(map(e -> Expr(:kw, e.args[1], symbol_context(e.args[2])), kw)...)))
end

macro transform2(args...)
    esc(:($(transform2_macro_helper(args...))))
end
