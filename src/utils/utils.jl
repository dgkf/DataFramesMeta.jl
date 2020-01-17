ChildDataFrame = Union{
    GroupedDataFrame,
    DataFrames.DataFrameRows,
    DataFrameRow
}

AnyDataFrame = Union{
    AbstractDataFrame,
    ChildDataFrame
 }



"""
A default naming function for aggregation columns
"""
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
Test for whether a function can accept a DataFrame-like object
"""
accepts_data(f, type::Type=AnyDataFrame) = 
    any([m.sig<:Tuple{Any,T} where T<:type for m=methods(f)])



"""
Tests for whether a function is explicitly expecting a DataFrame-like object
"""
expecting_data(f, type::Type=AnyDataFrame) = false
expecting_data(f::Function, type::Type=AnyDataFrame) = 
    startswith(string(nameof(f)), "#") && accepts_data(f, type)
function expecting_data(args...; kwargs...)
    [[expecting_data(a) for a=args]..., 
     [expecting_data(v) for (k,v)=kwargs]...]
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
    verb_arg_handler(args; at_predicate, key, predicate_pairs, 
            args_symbol_context, kwargs_symbol_context)

A handler for processing DataFramesMeta verbs

### Arguments

* args: The arguments to a DataFramesMeta verb macro
* at_predicate: Set to `true` if the verb handler should process predicate 
    `at => _` syntax, modifying code to instead read `cols(_)`
* key: Set to `true` if a column key should be accepted following a predicate,
    leaving the key argument untouched if it is a symbol. 
* predicate_pairs: Set to `true` if predicate pairs of the form `_ => _` should
    be allowed, producing `cols(_) => _` pairs.
* args_symbol_context: Set to `true` if additional arguments should be
    interpretted as a symbol context.
* kwargs_symbol_context: Set to `true` if additional keyworded arguments should
    be interpretted as a symbol context.
"""
function verb_arg_handler(args; at_predicate=true, key=true, 
	predicate_pairs=true, args_symbol_context=true, 
	kwargs_symbol_context=true)

    following_at_pred = true
    map(enumerate(args)) do (i, arg)
        if arg isa Expr && arg.head == :macrocall
            return(arg)
        elseif at_predicate && match_arg(arg, :(at => _))
            following_at_pred = true
            return(:(cols($(arg.args[3]))))
        elseif predicate_pairs && match_arg(arg, :(_ => _))
            arg.args[2] = Expr(:call, :cols, arg.args[2])
        elseif key && following_at_pred && match_arg(arg, Symbol)
        elseif kwargs_symbol_context && arg isa Expr && arg.head in (:kw, :(=))
            arg = Expr(:kw, arg.args[1], syms(arg.args[2]))
        elseif args_symbol_context
            arg = syms(arg)
        end
        following_at_pred = false
        arg
    end
end


