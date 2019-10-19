walk_expr_for_syms(e, f) = e
walk_expr_for_syms(q::QuoteNode, f) = walk_expr_for_syms(Meta.quot(q.value), f)
walk_expr_for_syms(expr::Expr, symbol_dict::Dict) = 
    walk_expr_for_syms(expr, (e, s) -> get(symbol_dict, s, e))
walk_expr_for_syms(expr::Expr, symbol_func::Function, symbols::Union{Array{Symbol,1}}) = 
    walk_expr_for_syms(expr, (e, s) -> s in symbols ? symbol_func(e, s) : expr)
function walk_expr_for_syms(e::Expr, symbol_func::Function)
    if onearg(e, :^)
        e.args[2]
    elseif onearg(e, :_I_)
        @warn "_I_() for escaping variables is deprecated, use cols() instead"
        symbol_func(e, e.args[2].value)
    elseif onearg(e, :cols)
        symbol_func(e, e.args[2].value)
    elseif e.args[1] == :(:.)
        Expr(e.head, symbol_func(e, e.args[1].value), e.args[2:end]...)
    elseif e.head == :quote
        symbol_func(e, e.args[1])
    else
        mapexpr(ei -> walk_expr_for_syms(ei, symbol_func), e)
    end
end



(::Colon)(d::AnyDataFrame, s::Symbol) = try_accessor(d, s)
try_accessor(d, s::Symbol) = s in names(d) ? accessor(d,s) : s
accessor(d::AnyDataFrame, s::Symbol) = d[!,s]
accessor(g::GroupedDataFrame, s::Symbol) = g.parent[!,s]
accessor(r::DataFrameRow, s::Symbol) = r[s]

symbol_context(expr) = expr
symbol_context(expr::QuoteNode, xsym::Symbol=gensym()) = 
    symbol_context(Meta.quot(expr.value), xsym)
function symbol_context(expr::Expr, x::Symbol=gensym())
    # expand internal macro so that symbol accessor function doesn't get 
    # modified (e.g. by `@.`)
    expr = eval(:(@macroexpand $expr))

    # create new function, with data dependent accessor function
    # e.g. 
    #     :x .* 3
    #   becomes
    #     d -> d:(:x) .* 3
    :($x::$AnyDataFrame -> $(walk_expr_for_syms(expr, (
        e, s) -> s == :. ? :($x) : :($x:$(Meta.quot(s))))
    ))
end


accepts_data(f, type::Type=AnyDataFrame) = 
    any([m.sig<:Tuple{Any,T} where T<:type for m=methods(f)])

expecting_data(f, type::Type=AnyDataFrame) = false
expecting_data(f::Function, type::Type=AnyDataFrame) = 
    startswith(string(nameof(f)), "#") && accepts_data(f, type)

function expecting_data(args...; kwargs...)
    [[expecting_data(a) for a=args]..., 
     [expecting_data(v) for (k,v)=kwargs]...]
end

function provide_expected_data(d, args, kwargs)
    args_exp_data = [expecting_data(a) for a=args]
    kwarg_exp_data = [expecting_data(v) for (k,v)=kwargs]
    args = [e ? a(d) : a for (a,e)=zip(args, args_exp_data)]
    kwargs = [k => e ? v(d) : v for ((k,v),e)=zip(kwargs, kwarg_exp_data)]
    args, kwargs, any(args_exp_data) || any(kwarg_exp_data)
end
