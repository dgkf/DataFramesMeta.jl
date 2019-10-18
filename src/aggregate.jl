export aggregate, aggregate_long, aggregate_wide, 
       aggregate!, aggregate_long!, aggregate_wide!, 
       @aggregate, @aggregate_long, @aggregate_wide,
       @aggregate!, @aggregate_long!, @aggregate_wide!

"""
julia> df = DataFrame(x = 1:4, y = repeat([1, 2], 2), z = 'a':'d')
4×2 DataFrame
│ Row │ x     │ y     │ z     |
│     │ Int64 │ Int64 │ Char  |
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ a     |
│ 2   │ 2     │ 2     │ b     |
│ 3   │ 3     │ 1     │ c     |
│ 4   │ 4     │ 2     │ d     |


julia> df |> @aggregate(x_sum = sum(:x))
2×3 DataFrame
│ Row | x_sum │
|     | Int64 |
├─────┼───────┤
│ 1   | 10    │


julia> df |> @aggregate(at => Number, sum)
1×2 DataFrame
│ Row │ x_sum │ y_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 10    │ 6     │


julia> df |> @aggregate(at => Number, sum, mean)
1×4 DataFrame
│ Row │ x_sum │ x_mean  │ y_sum │ y_mean  │
│     │ Int64 │ Float64 │ Int64 │ Float64 │
├─────┼───────┼─────────┼───────┼─────────┤
│ 1   │ 10    │ 2.5     │ 6     │ 1.5     │


julia> df |> @aggregate_wide(at => Number, sum, mean)
2×3 DataFrame
│ Row │ column │ sum   │ mean    │
│     │ Symbol │ Int64 │ Float64 │
├─────┼────────┼───────┼─────────┤
│ 1   │ x      │ 10    │ 2.5     │
│ 2   │ y      │ 6     │ 1.5     │


julia> df |> @aggregate_long(at => Number, sum, mean)
2×3 DataFrame
│ Row │ aggregate │ x       │ y       │
│     │ Symbol    │ Float64 │ Float64 │
├─────┼───────────┼─────────┼─────────┤
│ 1   │ sum       │ 10.0    │ 6.0     │
│ 2   │ mean      │ 2.5     │ 1.5     │


julia> df |> @aggregate_wide(:summary,
    all()  => (col_type = typeof, cell_type = eltype),
    Number => (maximum, minimum),
    Char => (n_unique = x -> length(unique(x)),))
│ Row │ column | datatype | maximum │ minimum  │ n_unique |
│     │ Symbol | DataType | Int64⍰  │ Float64⍰ │ Int64⍰   |
├─────┼────────┼──────────┼─────────┼──────────┼──────────┼
│ 1   │ x      | Real     | 4       │ 1        │ missing  |
│ 2   │ y      │ Real     | 2       │ 1        | missing  |
│ 2   │ z      │ String   | missing │ missing  | 4        |
"""
function aggregate(d::AbstractDataFrame; kwargs...)
    _aggregate(d; kwargs...)
end

function aggregate(d::AbstractDataFrame, x::Function)
    _aggregate(d, names(d), x; _namefunc = (c,k) -> c)
end

function aggregate(d::AbstractDataFrame, args...; kwargs...)
    _aggregate(d, names(d), args...; kwargs...)
end

function aggregate(d::AbstractDataFrame, predicate::Union{Array{Symbol},Pair{Symbol,}}, args...; kwargs...)
    cols = aggregate_predicate(d, predicate)
    _aggregate(d, cols, args...; kwargs...)
end

function aggregate(d::AbstractDataFrame, predicate::Union{Array{Symbol},Pair{Symbol,}}, key::Symbol, args...; kwargs...)
    cols = aggregate_predicate(d, predicate)
    aggregate(d, cols, args...; _key=key, kwargs...)
end

function aggregate(d::AbstractDataFrame, args::Pair{<:Tuple,}...; kwargs...)
    args = map(args) do (a,b); a => (expecting_data(b) ? b(d) : b); end
    _aggregate_flat(d, [args...]; kwargs...)
end

function aggregate(d::AbstractDataFrame, key::Symbol, args::Pair{<:Tuple,}...; kwargs...)
    aggregate_long(d, key, args...; kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, args...; kwargs...)
    aggregate(d, :at => all(), args...; _colkey=:column, kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, predicate::Union{Array{Symbol},Pair{Symbol,}}, args...; kwargs...)
    aggregate(d, predicate, args...; _colkey=:column, kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, predicate::Union{Array{Symbol},Pair{Symbol,}}, key::Symbol, args...; kwargs...)
    aggregate(d, predicate, args...; _colkey=key, kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, key::Symbol, args::Pair{<:Tuple,}...; kwargs...)
    aggregate_wide(d, args...; _colkey=key, kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, args::Pair{<:Tuple,}...; kwargs...) 
    args = map(args) do (a,b); a => (expecting_data(b) ? b(d) : b); end
    _aggregate_wide(d, [args...]; kwargs...)
end 

function aggregate_long(d::AbstractDataFrame, args...; kwargs...)
    aggregate(d, :at => all(), args...; _key=:aggregate, kwargs...)
end

function aggregate_long(d::AbstractDataFrame, predicate::Union{Array{Symbol},Pair{Symbol,}}, args...; kwargs...)
    aggregate(d, predicate, args...; _key=:aggregate, kwargs...)
end

function aggregate_long(d::AbstractDataFrame, predicate::Union{Array{Symbol},Pair{Symbol,}}, key::Symbol, args...; kwargs...)
    aggregate(d, predicate, args...; _key=key, kwargs...)
end

function aggregate_long(d::AbstractDataFrame, key::Symbol, args::Pair{<:Tuple,}...; kwargs...)
    aggregate_long(d, args...; _key=key, kwargs...)
end

function aggregate_long(d::AbstractDataFrame, args::Pair{<:Tuple,}...; kwargs...) 
    args = map(args) do (a,b); a => (expecting_data(b) ? b(d) : b); end
    _aggregate_long(d, [args...]; kwargs...)
end 

function aggregate(g::GroupedDataFrame, args...; kwargs...)
    map(g) do gi; aggregate(gi, args...; kwargs...); end
end

function aggregate(g::GroupedDataFrame, args::Pair{<:Tuple}...; kwargs...)
    args = map(args) do (a,b); aggregate_predicate(g, a) => (expecting_data(b) ? b(g) : b); end
    map(g) do gi; _aggregate_flat(gi, [args...]; kwargs...); end
end

function aggregate(g::GroupedDataFrame, key::Symbol, args::Pair{<:Tuple}...; kwargs...)
    aggregate_long(g, key, args...; kwargs...)
end

function aggregate(g::GroupedDataFrame, predicate::Pair{Symbol,}, args...; kwargs...)
    cols = aggregate_predicate(g, predicate)
    map(g) do gi; aggregate(gi, cols, args...; kwargs...); end
end

function aggregate_long(g::GroupedDataFrame, predicate::Pair{Symbol,}, args...; kwargs...)
    cols = aggregate_predicate(g, predicate)
    map(g) do gi; aggregate_long(gi, cols, args...; kwargs...); end
end

function aggregate_long(g::GroupedDataFrame, args::Pair{<:Tuple,}...; kwargs...)
    args = map(args) do (a,b); (aggregate_predicate(g, a),) => b; end
    map(g) do gi; aggregate_long(gi, args...; kwargs...); end
end

function aggregate_long(g::GroupedDataFrame, key::Symbol, args::Pair{<:Tuple,}...; kwargs...)
    aggregate_long(g, args...; _key=key, kwargs...)
end

function aggregate_wide(g::GroupedDataFrame, predicate::Pair{Symbol,}, args...; kwargs...)
    cols = aggregate_predicate(g, predicate)
    map(g) do gi; aggregate_wide(gi, cols, args...; kwargs...); end
end

function aggregate_wide(g::GroupedDataFrame, key::Symbol, args::Pair{<:Tuple,}...; kwargs...)
    aggregate_wide(g, args...; _key=key, kwargs...)
end

function aggregate_wide(g::GroupedDataFrame, args::Pair{<:Tuple,}...; kwargs...)
    args = map(args) do (a,b); (aggregate_predicate(g, a),) => b; end
    map(g) do gi; aggregate_wide(gi, args...; kwargs...); end
end

function aggregate_long(d::AnyDataFrame, key::Symbol, args...; kwargs...)
    aggregate_long(d, args...; _key = key, kwargs...)
end

function aggregate_wide(d::AnyDataFrame, key::Symbol, args...; kwargs...)
    aggregate_wide(d, args...; _colkey = key, kwargs...)
end

function aggregate(r::DataFrames.DataFrameRows, args...; kwargs...)
    eachrow(aggregate(parent(r), args...; kwargs...))
end

function aggregate_long(r::DataFrames.DataFrameRows, args...; kwargs...)
    eachrow(aggregate(parent(r), args...; kwargs...))
end

function aggregate_wide(r::DataFrames.DataFrameRows, args...; kwargs...)
    eachrow(aggregate(parent(r), args...; kwargs...))
end


aggregate_predicate(d::AnyDataFrame, c::Array{Symbol}) = c
aggregate_predicate(d::AnyDataFrame, p) = 
    names(d)[column_selectors(d, p)]
aggregate_predicate(d::AnyDataFrame, p::Pair) = 
    names(d)[column_selectors(d, p.second)]
aggregate_predicate(g::GroupedDataFrame, p) = 
    setdiff(aggregate_predicate(g.parent, p), names(g)[g.cols])


# what would be the expected behavior of an inplace aggregation?
aggregate!(args...; kwargs...) = aggregate(args...; kwargs...)
aggregate_wide!(args...; kwargs...) = aggregate_wide(args...; kwargs...)
aggregate_long!(args...; kwargs...) = aggregate_long(args...; kwargs...)



function _aggregate(d::AnyDataFrame; kwargs...)
    DataFrame([k => transform_handler(d,k,k,v) for (k,v)=kwargs]...)
end

function _aggregate(d, cols::Array{Symbol}, args...; kwargs...)
    args, kwargs, _ = provide_expected_data(d, args, kwargs)
    __aggregate(d, cols, args...; kwargs...)
end

function __aggregate(d::AnyDataFrame, cols::Array{Symbol}, key::Symbol, args...; kwargs...)
    _aggregate(d, cols, args...; _key=key, kwargs...)
end

function __aggregate(d::AnyDataFrame, cols::Array{Symbol}, args...; 
        _namefunc::Function=default_naming_func, 
        _makeunique::Bool=false,
        _key::Union{Symbol,Missing}=missing,
        _colkey::Union{Symbol,Missing}=missing, kwargs...)

    @assert(length(args) + length(kwargs) > 0, 
        "an aggregating function must be provided.")    
        
    pred_f_pairs = [cols => f for f in [args..., kwargs...]]
    if _colkey !== missing
        _aggregate_wide(d, pred_f_pairs; _colkey=_colkey, _makeunique=_makeunique)
    elseif _key !== missing
        _aggregate_long(d, pred_f_pairs; _key=_key, _makeunique=_makeunique)
    else
        _aggregate_flat(d, pred_f_pairs; _namefunc=_namefunc, _makeunique=_makeunique)
    end
end

function __aggregate_helper(d, predicate_function_pairs::Array{<:Pair})
    # produce list of affecting functions
    # create mask of affected columns per function
    f_cols_pairs = vcat(map(predicate_function_pairs) do (pred,fs)
        cols = column_selectors(d, pred)
        fs isa Union{Tuple,Array} ? [f => cols for f in fs] : 
        fs isa NamedTuple ? [p => cols for p in pairs(fs)] :
        fs => cols
    end...)

    affected_cols_idx = (|).(getindex.(f_cols_pairs, 2)...)
    affected_cols_names = names(d)[affected_cols_idx]

    # calculate results per function for applicable columns
    f_agg_pairs = map(f_cols_pairs) do (fi, cols)
        aff_cols = zip(affected_cols_names, cols[affected_cols_idx])
        f = fi isa Union{Tuple,Pair} ? fi[2] : fi
        agg_res = [cm ? transform_handler(d,c,c,f) : missing for (c,cm)=aff_cols]
        infer_func_name(fi, :agg) => agg_res
    end

    f_agg_pairs, affected_cols_names
end

function _aggregate_wide(d, predicate_function_pairs::Array{<:Pair}; 
        _colkey::Symbol=:column, _makeunique::Bool=false)

    results, cols = __aggregate_helper(d, predicate_function_pairs)
    DataFrame(_colkey => cols, results..., makeunique = _makeunique)
end



function _aggregate_long(d, predicate_function_pairs::Array{<:Pair}; 
        _key::Symbol=:aggregate, _makeunique::Bool=false)

    results, cols = __aggregate_helper(d, predicate_function_pairs)
    df = hcat(
        DataFrame(vcat(reshape.(getfield.(results, :second), 1, :)...), 
            cols, 
            makeunique=_makeunique),
        DataFrame(_key => getfield.(results, :first)),
        makeunique = _makeunique)
    df = df[:,[ncol(df), (1:ncol(df)-1)...]]
end



function _aggregate_flat(d, predicate_function_pairs::Array{<:Pair};
        _namefunc::Union{Function,Missing}=default_naming_func, 
        _makeunique::Bool=false)
    
    _namefunc = _namefunc === missing ? default_naming_func : _namefunc
    results, cols = __aggregate_helper(d, predicate_function_pairs)
    DataFrame([
        sym_helper(_namefunc)(c,n) => v[ci]
        for (ci,c)=enumerate(cols) for (n,v)=results
    ]..., makeunique = _makeunique)
end



infer_func_name(x, default) = default
infer_func_name(x::Pair, default) = x[1] 
infer_func_name(x::NamedTuple, default) = fieldnames(typeof(x))[1]
function infer_func_name(x::Function, default)
    n = nameof(x)
    String(n)[1] == '#' ? :function : n
end



function make_unique_helper(args...; error_replacements, kwargs...)
    try 
        DataFrames.make_unique(args...; kwargs...)
    catch e
        if hasfield(typeof(e), :msg)
            throw(typeof(e)(replace(e.msg, error_replacements...)))
        end
    end
end



function aggregate_predicate_pairs_helper(f, a, kw)
    symbol, a, matched = match_args(a, [Symbol])
    predicate_pairs, a = split_pairs(a)

    :(data::$AnyDataFrame -> $f(
        data,
        $(symbol...),
        $(map(e -> symbol_context(e), a)...),
        $(map(e -> Expr(:call, :(=>), Expr(:tuple, e.args[2]), symbol_context(e.args[3])), predicate_pairs)...),
        $(map(e -> Expr(:kw, e.args[1], symbol_context(e.args[2])), kw)...)
    ))
end

function aggregate_predicate_helepr(f, symbol, predicate, a, kw)
    predicate = at_pair_to_symbol(predicate)

    :(data::$AnyDataFrame -> $f(
        data, 
        $(predicate...),
        $(symbol...),
        $(map(e -> symbol_context(e), a)...),
        $(map(e -> Expr(:kw, e.args[1], symbol_context(e.args[2])), kw)...)))
end

function aggregate_macro_helper(f_inplace, f, args...; inplace::Bool=false)
    f = inplace ? f_inplace : f
    a, kw = split_macro_args(args)
    predicate, symbol, a, matched = match_args(a, [:(at => _), Symbol])

    if matched == 0; aggregate_predicate_pairs_helper(f, a, kw)
    else; aggregate_predicate_helepr(f, symbol, predicate, a, kw)
    end
end

macro aggregate(args...)
    esc(:($(aggregate_macro_helper(aggregate!, aggregate, args...))))
end

macro aggregate_long(args...)
    esc(:($(aggregate_macro_helper(aggregate_long!, aggregate_long, args...))))
end

macro aggregate_wide(args...)
    esc(:($(aggregate_macro_helper(aggregate_wide!, aggregate_wide, args...))))
end
