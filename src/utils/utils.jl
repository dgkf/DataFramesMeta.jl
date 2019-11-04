ChildDataFrame = Union{
    GroupedDataFrame,
    DataFrames.DataFrameRows,
    DataFrameRow
}

AnyDataFrame = Union{
    AbstractDataFrame,
    ChildDataFrame
 }



"A default naming function for aggregation columns"
default_naming_func(column_name, keyword) = column_name * "_" * keyword



"""
A simple wrapper to convert functions that operate on strings to operate on 
Symbols
"""
sym_helper(f) = (args...) -> Symbol(f(string.(args)...))



"""
    spoken_list(items, quoting_string="")

Helper for converting list to spoken sentence 

# Examples

```
julia> spoken_list(["a", "b", "c"])
"a, b and c"
```
"""
function spoken_list(list::AbstractArray, qchar::String="")
    s = string.(list)
    seps = reverse([i == 1 ? " and " : ", " for (i, si) in enumerate(s[2:end])])
    qchar * s[1] * qchar * string((seps .*  qchar .* s[2:end] .* qchar)...)
end



"""
    match_arg(argument, pattern)

Match an argument against a pattern, returning Bool indicating match
"""
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
    arg.head == pat.head && 
    length(arg.args) == length(pat.args) && 
    all(match_arg.(arg.args, pat.args))



"""
    match_args(args, patterns)

Match a series of arguments against a series of argument patterns

Patterns can either be Types or expressions. `_` is used as a wildcard for
pattern matching.
"""
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



"Split macro arguments into positional and keyword arguments"
function split_macro_args(args)
    args = [a isa Expr && a.head == :parameters ? a.args : [a] for a in args]
    args = reduce(vcat, args)
    has_kw = [a isa Expr && a.head in (:kw, :(=)) for a in args]
    args[(!).(has_kw)], args[has_kw]
end



"Convert a pair specified as `at => _` into `:at => _`"
function at_pair_to_symbol(args)
    is_pair = [match_arg(arg, Pair) for arg in args]
    args = map(zip(args, is_pair)) do (arg, is_arg_pair)
        is_arg_pair && arg.args[2] == :at ? pair_name_to_symbol(arg) : arg
    end
end



"Convert a pair specified as `at => _` into `cols(_)`"
function at_pair_to_cols(args)
    is_pair = [match_arg(arg, Pair) for arg in args]
    args = map(zip(args, is_pair)) do (arg, is_arg_pair)
		is_arg_pair && arg.args[2] == :at ? 
		Expr(:call, :(=>), :(:at), Expr(:call, :cols, arg.args[3])) : 
		arg
    end
end



"Convert a QuoteNode to a Symbol in a Pair (`thing => _` becomes `:thing => _`)"
function pair_name_to_symbol(pair_expr)
    if (!(typeof(pair_expr.args[2])<:QuoteNode))
        pair_expr.args[2] = Meta.quot(pair_expr.args[2])
    end
    pair_expr
end



function verb_arg_handler(args; at_predicate=true, key=true, 
			predicate_pairs=true, args_symbol_context=true, 
			kwargs_symbol_context=true)
	following_at_pred = true
	map(enumerate(args)) do (i, arg)
		if arg isa Expr && arg.head == :macrocall
			return(arg)
		elseif at_predicate && match_arg(arg, :(at => _))
			following_at_pred = true
			return(Expr(:call, :cols, arg.args[3]))
		elseif predicate_pairs && match_arg(arg, :(_ => _))
			arg.args[2] = Expr(:call, :cols, arg.args[2])
		elseif key && following_at_pred && match_arg(arg, Symbol)
		elseif kwargs_symbol_context && arg isa Expr && arg.head in (:kw, :(=))
			arg = Expr(:kw, arg.args[1], symbol_context(arg.args[2]))
		elseif args_symbol_context
        	arg = symbol_context(arg)
		end
		following_at_pred = false
		arg
	end
end



"Split Pair arguments out from a list of arguments"
function split_pairs(args)
    is_pair = [match_arg(arg, Pair) for arg in args]
    args[is_pair], args[(!).(is_pair)]
end



"""
Split argument into arguments before the first pair, the first pair and 
arguments after the first pair
"""
function split_on_pair(args)
    is_pair = [match_arg(arg, Pair) for arg in args]
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



"Get a list of types that are acceptable for a given generic"
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


"Replace single newline characters with a space."
replace_single_newlines(str) = replace(replace(str,
    r"([^\s\n])\n([^\s\n])" => s"\1 \2"), 
    r"\n+$" => s"")
