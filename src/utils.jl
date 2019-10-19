ChildDataFrame = Union{
    GroupedDataFrame,
    DataFrames.DataFrameRows,
    DataFrameRow
}

AnyDataFrame = Union{
    AbstractDataFrame,
    ChildDataFrame
 }

default_naming_func(column_name, keyword) = column_name * "_" * keyword
sym_helper(f) = (args...) -> Symbol(f(string.(args)...))

# helper for converting list to spoken sentence 
function spoken_list(list::AbstractArray, qchar::String="")
    s = string.(list)
    seps = reverse([i == 1 ? " and " : ", " for (i, si) in enumerate(s[2:end])])
    qchar * s[1] * qchar * string((seps .*  qchar .* s[2:end] .* qchar)...)
end

match_arg(a, p) = p == :_
match_arg(arg, pat::Union{Type,DataType}) = 
    match_arg(arg, pat, Val(pat))
match_arg(arg, pat, p::Val{Union{T}} where T<:Any) = 
    match_arg(arg, pat.a, Val(pat.a)) || match_arg(arg, pat.b, Val(pat.b))
match_arg(arg, pat, p::Val{Pair}) = 
    typeof(arg)<:Expr && arg.head == :call && arg.args[1] == :(=>)
match_arg(arg, pat, p::Val{T} where T<:Array) = 
    typeof(arg)<:Expr && arg.head == :vect && typeof(arg.args)<:Array
match_arg(arg, pat, p::Val{Symbol}) = 
    typeof(arg)<:QuoteNode
match_arg(arg, pat::Array) = 
    any(match_arg.([arg], pat))
match_arg(arg::QuoteNode, pat::Union{Symbol,QuoteNode}) = 
    pat == :_ || arg == pat
match_arg(arg::Symbol, pat::Symbol) = 
    pat == :_ || arg == pat
match_arg(arg::Expr, pat::Expr) = 
    arg.head == pat.head && length(arg.args) == length(pat.args) && all(match_arg.(arg.args, pat.args))


function match_args(args, pattern)
    length(args) == 0 && return (repeat([()], length(pattern)+1)..., 0)
    matches = repeat([0], length(args))
    pattern_l = length(pattern)
    pattern_i = 1
    for (i, a) in enumerate(args)
        if length(pattern) == 0
            break
        elseif match_arg(a, pattern[1])
            popfirst!(pattern)
            matches[i] = pattern_i
            pattern_i += 1
        end
    end
    
    matched = (args[matches .== i] for i=[1:pattern_l...,0])
    (matched..., pattern_l - length(pattern))
end

function split_macro_args(args)
    args = [a isa Expr && a.head == :parameters ? a.args : [a] for a in args]
    args = reduce(vcat, args)
    has_kw = [a isa Expr && a.head in (:kw, :(=)) for a in args]
    args[(!).(has_kw)], args[has_kw]
end

function is_lambda(expr)
    typeof(expr)<:Expr && expr.head == :->
end

function at_pair_to_symbol(args)
    is_pair = [match_arg(arg, Pair) for arg in args]
    args = map(zip(args, is_pair)) do (arg, is_arg_pair)
        is_arg_pair && arg.args[2] == :at ? pair_name_to_symbol(arg) : arg
    end
end

function pair_name_to_symbol(pair_expr)
    if (!(typeof(pair_expr.args[2])<:QuoteNode))
        pair_expr.args[2] = Meta.quot(pair_expr.args[2])
    end
    pair_expr
end

function split_pairs(args)
    is_pair = [match_arg(arg, Pair) for arg in args]
    args[is_pair], args[(!).(is_pair)]
end

function split_on_pair(args)
    is_pair = [is_pair_expr(arg) for arg in args]
    args = map(zip(args, is_pair)) do (arg, is_arg_pair)
        is_arg_pair ? pair_name_to_symbol(arg) : arg
    end

    first_pair = findfirst(is_pair)
    last_pair = findlast(is_pair)

    if first_pair == last_pair == nothing 
        args, (), ()
    else
        args[1:first_pair-1], args[first_pair:last_pair], args[last_pair+1:end]
    end
end

function split_reserved(kwargs, re::Regex=r"^_")
    is_res = map(kwargs) do kw
        kw isa Expr && 
        kw.head in (:kw, :(=)) && 
        match(re, string(kw.args[1])) != nothing
    end
    kwargs[is_res], kwargs[(!).(is_res)]
end

function pairs_to_dict(pairs)
    Expr(:Dict, pairs...)
end

function match_macro_args(args; formals::Array{Symbol,1})
    has_kw = [a isa Expr && a.head in (:kw, :(=)) for a in args]    
    kwargs = Dict(map(a -> a.args[1] => a.args[2], args[has_kw])...)
    pargs = [args[(!).(has_kw)]...]

    args = []
    for (f, o) in zip(formals)
        if (f in keys(kwargs)) 
            append!(args, [f => pop!(kwargs, f)])
        elseif (!o && length(pargs) > 0)
            append!(args, [f => pargs[1]])
            deleteat!(pargs, 1)
        end
    end

    append!(args, pargs)
    append!(args, kwargs)
    args
end

function method_types(f::Function, sig::AbstractArray, param::Number)
    [skipmissing(map(methods(f)) do m
        match_sig = all(a<:b for (a,b)=zip(m.sig.parameters[2:min(length(sig),end)], sig))
        if match_sig && length(m.sig.parameters) > param
            m.sig.parameters[param+1]
        else
            missing
        end
    end)...]
end
