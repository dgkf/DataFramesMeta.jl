export transform_pred, @transform_pred

transform_handler(a::AbstractDataFrame, col::Symbol, x) = x
transform_handler(a::AbstractDataFrame, col::Symbol, x::Function) = x(a[:,col])
function transform_handler(g::GroupedDataFrame, col::Symbol, x)
    group_transforms = map(zip(g.starts, g.ends)) do (s, e)
        result = transform_handler(g.parent[s:e,:], col, x)
        isa(result, AbstractArray) ? result : repeat([result], e-s+1)
    end
    @assert all(typeof.(group_transforms) .== typeof(group_transforms[1]))
    @assert sum(length.(group_transforms)) == nrow(g.parent)
    return reduce(vcat, group_transforms)
end

function transform_pred(d::AbstractDataFrame, pred::Union{Array{Bool},BitArray}, 
        args...; sep::String = "_", colf::Function = (c, k) -> c * sep * k, 
        kwargs...)

    cols = names(d)[pred]
    dyn_colnames = colf.(
        repeat(string.(cols), inner = length(kwargs)),
        repeat(string.([k for (k, v) in kwargs]), outer = length(cols)))

    @assert(length(args) <= 1, 
        "Only one unnamed transformation is permitted.")
    @assert(!any(in(string.(names(d))).(dyn_colnames)), 
        "New column names will overwrite existing columns")

    result = copy(d)
    for col in cols
        if (length(args) > 0)
            result[!,col] .= transform_handler(d, col, args[1])
        end
        for (k, v) in kwargs
            new_colname = Symbol(colf(string(col), string(k)))
            result[!,new_colname] .= transform_handler(d, col, v)
        end
    end

    return result
end

function transform_pred(g::GroupedDataFrame, pred::Union{Array{Bool},BitArray}, 
        args...; sep::String = "_", colf::Function = (c, k) -> c * sep * k, 
        kwargs...)

    cols = names(g)[pred]
    dyn_colnames = colf.(
        repeat(string.(cols), inner = length(kwargs)),
        repeat(string.([k for (k, v) in kwargs]), outer = length(cols)))

    @assert(length(args) <= 1, 
        "Only one unnamed transformation is permitted.")
    @assert(!any(in(string.(names(g))).(dyn_colnames)), 
        "New column names will overwrite existing columns")
    
    result = copy(g.parent)
    for col in cols
        if (length(args) > 0)
            result[!,col] = transform_handler(g, col, args[1])
        end
        for (k, v) in kwargs
            new_colname = Symbol(colf(string(col), string(k)))
            result[!,new_colname] = transform_handler(g, col, v)
        end
    end

    return groupby(result, g.cols)
end

function transform_pred_helper(x, pred, args...)
    quote $transform_pred(
        $x, 
        $pred,
        $(map(args) do exp
            if (exp isa Expr && in(exp.head, (:kw, :(=)))) 
                Expr(:kw, exp.args[1], with_helper(x, exp.args[2])) 
            else 
                with_helper(x, exp)
            end
        end...))
    end
end

macro transform_pred(x, pred, args...)
   esc(transform_pred_helper(x, pred, args...))
end
