"""
Expression operators
"""
onearg(e, f) = e.head == :call && length(e.args) == 2 && e.args[1] == f


"""
    walk_expr_for_syms(expression, symbol_modification_function)

Walk an AST for symbols and transform them with a given function.
"""
walk_expr_for_syms(e, f) = return e, false
walk_expr_for_syms(q::QuoteNode, f) = walk_expr_for_syms(Meta.quot(q.value), f)
walk_expr_for_syms(expr::Expr, symbol_dict::Dict) = 
    walk_expr_for_syms(expr, (e, s) -> get(symbol_dict, s, e))
walk_expr_for_syms(expr::Expr, symbol_func::Function, symbols::Union{Array{Symbol,1}}) = 
    walk_expr_for_syms(expr, (e, s) -> s in symbols ? symbol_func(e, s) : expr)
function walk_expr_for_syms(e::Expr, symbol_func::Function)
	if onearg(e, :^)
        e.args[2], false
	elseif e.head == :macrocall
		e, false
    elseif e.args[1] == :(:.)
        Expr(e.head, symbol_func(e, e.args[1].value), e.args[2:end]...), true
    elseif e.head == :quote
		symbol_func(e, e.args[1]), true
    else
		walks = (walk_expr_for_syms(ei, symbol_func) for ei=e.args)
		Expr(e.head, (w[1] for w=walks)...), any(w[2] for w=walks)
	end
end


"""
Define a new DataFrame operator which can be used to get DataFramesMeta-relevant
column data from multiple DataFrame-like Types
"""
(d::DataFrame)(x) = try_accessor(d, x)
(d::GroupedDataFrame)(x) = try_accessor(d, x)
(d::DataFrameRow)(x) = try_accessor(d, x)

"""
Try to get Symbol from DataFrame context, returning Symbol if not relevant in 
current context
"""
try_accessor(d, s::Symbol) = s in names(d) ? accessor(d,s) : s
try_accessor(d, x) = accessor(d, x)

"""
Type-dispatched accessors for DataFrame-like Types
"""
accessor(d::AnyDataFrame, s) = d[!,s]
accessor(g::GroupedDataFrame, s) = g.parent[!,s]
accessor(r::DataFrameRow, s) = r[s]



"""
Modify an expression to operate on symbols with a given context

# Examples

```julia
julia> expr = symbol_context(:(:x .* :y), :d)
:(d -> (d:(:x)) .* (d:(:y)))

julia> eval(expr)(DataFrame(x = [1, 2, 3], y = [3, 2, 1]))
3-element Array{Int64,1}:
 3
 4
 3
```
"""
function symbol_context(expr)
    expr
end

function symbol_context(expr::QuoteNode, xsym::Symbol=gensym())
    symbol_context(Meta.quot(expr.value), xsym)
end

function symbol_context(expr::Expr, x::Symbol=gensym())
    # expand internal macro so that symbol accessor function doesn't get 
    # modified (e.g. by `@.`)
    expr = eval(:(@macroexpand $expr))

    # create new function, with data dependent accessor function
    # e.g. 
    #     :x .* 3
    #   becomes
    #     d -> d:(:x) .* 3
	new_expr, any_substitutions = walk_expr_for_syms(expr, 
		(e, s) -> s == :. ? :($x) : :($x($(Meta.quot(s)))))

	any_substitutions ? :($x::$AnyDataFrame -> $(new_expr)) : expr
end


"""
Test for whether a function can accept a DataFrame-like object
"""
accepts_data(f, type::Type=AnyDataFrame) = 
    any([m.sig<:Tuple{Any,T} where T<:type for m=methods(f)])

"""
Tests for whether a function is explicitly expecting a DataFrame-like object
"""
expecting_data(f, type::Type=AnyDataFrame) = false
expecting_data(f::ColumnPredicateFunction, type::Type=Any) = 
	expecting_data(f.f, type)
expecting_data(f::Function, type::Type=AnyDataFrame) = 
    startswith(string(nameof(f)), "#") && accepts_data(f, type)
function expecting_data(args...; kwargs...)
    [[expecting_data(a) for a=args]..., 
     [expecting_data(v) for (k,v)=kwargs]...]
end

"""
Evaluate args and kwargs expecting ddata
"""
function provide_expected_data(d, args, kwargs)
    args_exp_data = [expecting_data(a) for a=args]
    kwarg_exp_data = [expecting_data(v) for (k,v)=kwargs]
    args = [e ? a(d) : a for (a,e)=zip(args, args_exp_data)]
    kwargs = [k => e ? v(d) : v for ((k,v),e)=zip(kwargs, kwarg_exp_data)]
    args, kwargs, any(args_exp_data) || any(kwarg_exp_data)
end
