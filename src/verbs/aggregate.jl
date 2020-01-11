export aggregate, aggregate_long, aggregate_wide, 
       aggregate!, aggregate_long!, aggregate_wide!, 
       @aggregate, @aggregate_long, @aggregate_wide,
       @aggregate!, @aggregate_long!, @aggregate_wide!

"""
    aggregate(data; aggregation..., kwargs...)
    aggregate(data [, :at => predicate] [, key], aggregation...; aggregation..., kwargs...)
    aggregate(data [, key], predicate => aggregation...; kwargs...)

`aggregate` is used to build a `DataFrame` of aggregated values based on entire
columns of values. The form of the resulting DataFrame can take one of
three shapes. When a `key` is provided, aggregate will create a "long-form" 
aggregation DataFrame, using the `key` name as the aggregations column.



# Arguments

* `data`: $(doc_verb_arg_data())

* `predicate`: $(doc_verb_arg_predicate()) Predicates can also be used to apply 
    aggregating function selectively to a subset of columns by specifying
    pairs of predicates and a single aggregator, a `Tuple` of aggregators, or
    a `NamedTuple` of aliased aggregators.

* `aggregation`: A function or aggregation result. If a function is provided, 
    it will be applied to each predicated column (or all columns if no
    predicate is associated) of data. If it is not a function, the value will
    be considered the result of the aggregation. The `Type` and choice of
    aggregation syntax has some slight effects on output formatting. See the
    details section below for more information.

* `key`: A Symbol name of a new column can be provided, which will force the
    return type to a long-form DataFrame with one row per aggregation. 


**Keyword Arguments**

Generally these arguments do not need to be used directly and are primarily
used for controlling functionality internally.

* `_key`: A `Symbol` to use as an aggregation column name for a long-form
    aggregation `DataFrame`.

* `_colkey`: A `Symbol` to use as a column name for a column containing
    original `DataFrame` columns in a wide-form aggregation `DataFrame`.

* `_namefunc`: A `Function` expecting two arguments, the original column name
    and the aggregation name, returning a new column name for the aggregation
    `DataFrame`.

* `_makeunique`: A `Bool` expressing whether to force resulting `DataFrame`
    column names to be unique.



# Details

## Macro Usage

$(doc_verb_macro())

## Aggregation Structures

Although the default aggregations are consistently a single row of
aggregations, there is strong rationale to prefer a multi-row dataset. One
may prefer to have all the aggregations as individual columns which are often
`Array`s of concrete types or to retain only the existing column names with
one row per aggregator.

Both of these structures can be easier to use programmatically, avoiding the 
embedding of information in column names and provides aggregation data in a
friendlier format for downstream use. 

We use the following terminology to refer to these different structures of an
aggregated `DataFrame`.

* A "flat" DataFrame (default) has one row per aggregated `DataFrame` 
    (one row per group for `GroupedDataFrame`s)
* A "wide" DataFrame has a column for each aggregating function 
    (see [`aggregate_wide`](@ref)), used when `_colkey` is provided.
* A "long" DataFrame has a row for each aggregating function 
    (see [`aggregate_long`](@ref)), used when `_key` is provided.

`aggregate` is capable of performing all of these transformations using
keyword arguments, though [`aggregate_long`](@ref) and
[`aggregate_wide`](@ref) are provided as syntactically cleaner shorthands.


## Output Types

The `aggregate` function family is not type stable. Although they will generally
return `DataFrame`s, the structure of those `DataFrame`s will be dependent
on the provided arguments. These input types affect how the output is formatted:

1. The macro version of all core verbs returns an anonymous function

1. When a function is provided to the first argument, a new function is
    returned, awaiting a single argument, the target `DataFrame`.

1. When only a `DataFrame` and a single aggregation function is provided, the 
    aggregation `DataFrame` will contain a single row with unmodified column
    names. If this convenience isn't desired, you can provide a predicate of
    `all()`. When the predicate is specified, new column names are always
    constructed from the original name and aggregator name.

1. Whenever a `key` is provided, a long-form aggregation `DataFrame` is
    returned, containing one row per aggregation function. You can also force
    alternative structures by providing `Symbol` column names to the named
    `_key` (long) or `_colkey` (wide) arguments.

1. When a predicate or more than one aggregator are provided, new column
    names are created using `_namefunc`, containing one column per
    combination of predicate (assuming `all()` as default) and aggregator.

1. When provided with a `GroupedDataFrame`, the resulting aggregations will be 
    applied for each of group.

1. When provided with a `DataFrames.DataFrameRows`, the resulting
    aggregations will be applied over the entirety of the parent `DataFrame`
    and a `DataFrame.DataFrameRows` object is returned.



# Examples

```jdoctest
julia> df = DataFrame(x = 1:4, y = repeat([1, 2], 2), z = 'a':'d')
4×2 DataFrame
│ Row │ x     │ y     │ z     |
│     │ Int64 │ Int64 │ Char  |
├─────┼───────┼───────┼───────┤
│ 1   │ 1     │ 1     │ a     |
│ 2   │ 2     │ 2     │ b     |
│ 3   │ 3     │ 1     │ c     |
│ 4   │ 4     │ 2     │ d     |
```

**Aggregating with a named aggregator**

A common use case where a set of named derived values are created based on
concrete column references. 

```jdoctest
julia> df |> @aggregate(x_sum = sum(:x))  # naming an aggregation value
2×3 DataFrame
│ Row | x_sum │
|     | Int64 |
├─────┼───────┤
│ 1   | 10    │
```

**Aggregating across all columns using a single function**

Aggregating with a single function does not modify column names. If it's
preferred to have modified column names (e.g. "x_maximum", etc), you can 
provide a predicate such as `all()`. Whenever a predicate is provided, column
names will always be inferred based on original name and aggregator name.

```jdoctest
julia> df |> @aggregate(maximum)  # aggregation at all columns
1×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 4     │ 2     │ 'd'  │


julia> df |> @aggregate(maximum, minimum)  # multiple aggregations at all cols
1×6 DataFrame
│ Row │ x_maximum │ x_minimum │ y_maximum │ y_minimum │ z_maximum │ z_minimum │
│     │ Int64     │ Int64     │ Int64     │ Int64     │ Char      │ Char      │
├─────┼───────────┼───────────┼───────────┼───────────┼───────────┼───────────┤
│ 1   │ 4         │ 1         │ 2         │ 1         │ 'd'       │ 'a'       │
```

**Aggregating with a predicate**

A predicate can be provided, limiting the scope columns to which the
aggregations are applied.

```
julia> df |> @aggregate(at => Number, sum)  # predicated aggregation
1×2 DataFrame
│ Row │ x_sum │ y_sum │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 10    │ 6     │


julia> df |> @aggregate(at => Number, sum, mean)  # many predicated aggregations
1×4 DataFrame
│ Row │ x_sum │ x_mean  │ y_sum │ y_mean  │
│     │ Int64 │ Float64 │ Int64 │ Float64 │
├─────┼───────┼─────────┼───────┼─────────┤
│ 1   │ 10    │ 2.5     │ 6     │ 1.5     │
```

**Providing an aggregation column name**

Often it's preferred to maintain the column structure of the incoming
`DataFrame`. For this, a shorthand for [`aggregate_long`](@ref) is provided.
To apply multiple aggregation functions to any predicated columns at once
without perturbing the column names, this can be structured as a long form
(with respect to the number of aggregations) `DataFrame`. Whenever a `key`
(column name of the aggregations column) is provided, `aggregate` will return
a long form `DataFrame`.

```jdoctest
julia> df |> @aggregate(:agg, maximum, minimum)  # long shorthand
2×4 DataFrame
│ Row │ agg     │ x   │ y   │ z   │
│     │ Symbol  │ Any │ Any │ Any │
├─────┼─────────┼─────┼─────┼─────┤
│ 1   │ maximum │ 4   │ 2   │ 'd' │
│ 2   │ minimum │ 1   │ 1   │ 'a' │


julia> df |> @aggregate(at => Number, :agg, sum, mean)  # predicated long
2×3 DataFrame
│ Row │ agg    │ x       │ y       │
│     │ Symbol │ Float64 │ Float64 │
├─────┼────────┼─────────┼─────────┤
│ 1   │ sum    │ 10.0    │ 6.0     │
│ 2   │ mean   │ 2.5     │ 1.5     │
```

**Using Predicate Pairs**

It may not always be desireable to use the same predicate for all functions.
It's common that an aggregation may throw an error when applied to some
columns, or that a result might not be relevant for some columns. For this,
you can define aggregators which are only applied to columns selected with a
given predicate, allowing multiple aggregators to be used which may be error
prone if applied to the same predicated columns.

```jdoctest
julia> df |> @aggregate(:summary,  
    all()  => (col_type = typeof, cell_type = eltype,),
    Number => (maximum, minimum),
    Char   => (n_unique = x -> reduce(*, x),))  
5×4 DataFrame
│ Row │ summary   │ x              │ y              │ z             │
│     │ Symbol    │ Any            │ Any            │ Any           │
├─────┼───────────┼────────────────┼────────────────┼───────────────┤
│ 1   │ col_type  │ Array{Int64,1} │ Array{Int64,1} │ Array{Char,1} │
│ 2   │ cell_type │ Int64          │ Int64          │ Char          │
│ 3   │ maximum   │ 4              │ 2              │ missing       │
│ 4   │ minimum   │ 1              │ 1              │ missing       │
│ 5   │ n_unique  │ missing        │ missing        │ abcd          │
```

**`GroupedDataFrame`s**

Aggregating over a `GroupedDataFrame` will return an aggregate `SubDataFrame`
for each group.

```
julia> gdf = groupby(df, :y)
julia> gdf |> @aggregate(at => Number, sum)  # with GroupedDataFrames
GroupedDataFrame with 2 groups based on key: y
First Group (1 row): y = 1
│ Row │ y     │ x     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 1     │ 3     │ 'c'  │
⋮
Last Group (1 row): y = 2
│ Row │ y     │ x     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 2     │ 4     │ 'd'  │
```

**Use without macros**

```
julia> DataFramesMeta.aggregate(df, :at => all(), maximum, second = x -> x[2])
1×6 DataFrame
│ Row │ x_maximum │ x_second │ y_maximum │ y_second │ z_maximum │ z_second │
│     │ Int64     │ Int64    │ Char      │ Char     │ Bool      │ Bool     │
├─────┼───────────┼──────────┼───────────┼──────────┼───────────┼──────────┤
│ 1   │ 26        │ 2        │ 'z'       │ 'b'      │ 1         │ 0        │
```
"""
function aggregate(d::AbstractDataFrame; kwargs...)
    _aggregate(d; kwargs...)
end

function aggregate(d::AbstractDataFrame, x::Union{Function,SymbolContext})
    _aggregate(d, cols(d, true), x; _namefunc = (c,k) -> c)
end

function aggregate(d::AbstractDataFrame, args...; kwargs...)
    _aggregate(d, cols(d, true), args...; kwargs...)
end

function aggregate(d::AbstractDataFrame, predicate::AnyColumnPredicate, args...; 
        kwargs...)
    col_mask = cols(d, predicate)
    _aggregate(d, col_mask, args...; kwargs...)
end

function aggregate(d::AbstractDataFrame, predicate::AnyColumnPredicate, key::Symbol, 
        args...; kwargs...)
    col_mask = cols(d, predicate)
    aggregate(d, col_mask, args...; _key=key, kwargs...)
end

function aggregate(d::AbstractDataFrame, args::ColumnPredicatedPair...; kwargs...)
    args = map(args) do (a,b); cols(d, a) => (expecting_data(b) ? b(d) : b); end
    _aggregate_flat(d, [args...]; kwargs...)
end

# aggregate GroupedDataFrame
function aggregate(g::GroupedDataFrame; kwargs...)
    map(g) do gi; aggregate(gi; kwargs...); end
end

function aggregate(g::GroupedDataFrame, args...; kwargs...)
    predicate = cols(g, true)
    f = length(args) == 1 ? (c,k) -> c : default_naming_func
    map(g) do gi; aggregate(gi, predicate, args...; _namefunc = f, kwargs...); end
end

function aggregate(g::GroupedDataFrame, arg::ColumnPredicatedPair, 
        args::ColumnPredicatedPair...; kwargs...)
    args = (arg, args...)
    args = map(args) do (a,b); cols(g, a) => (expecting_data(b) ? b(g) : b); end
    map(g) do gi; aggregate(gi, [args...]; kwargs...); end
end

function aggregate(g::GroupedDataFrame, key::Symbol, args...; kwargs...)
    aggregate_long(g, key, args...; kwargs...)
end

function aggregate(g::GroupedDataFrame, predicate::AnyColumnPredicate, 
        args...; kwargs...)
    col_mask = cols(g, predicate)
    map(g) do gi; aggregate(gi, col_mask, args...; kwargs...); end
end

# Aggregate DataFrameRows
function aggregate(r::DataFrames.DataFrameRows, args...; kwargs...)
    eachrow(aggregate(parent(r), args...; kwargs...))
end



# aggregate_wide Function
# aggregate_wide AnyDataFrame
function aggregate_wide(d::AnyDataFrame, key::Symbol, args...; kwargs...)
    aggregate_wide(d, args...; _colkey = key, kwargs...)
end

function aggregate_wide(d::AnyDataFrame, args...; kwargs...)
    aggregate(d, cols(d, true), args...; _colkey=:column, kwargs...)
end

# aggregate_wide AbstractDataFrame
function aggregate_wide(d::AbstractDataFrame, predicate::AnyColumnPredicate, 
        args...; kwargs...)
    aggregate(d, predicate, args...; _colkey=:column, kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, predicate::AnyColumnPredicate, 
        key::Symbol, args...; kwargs...)
    aggregate(d, predicate, args...; _colkey=key, kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, key::Symbol, 
        args::ColumnPredicatedPair...; kwargs...)
    aggregate_wide(d, args...; _colkey=key, kwargs...)
end

function aggregate_wide(d::AbstractDataFrame, args::ColumnPredicatedPair...; kwargs...) 
	args = map(args) do (a,b); cols(d, a) => (expecting_data(b) ? b(d) : b); end
    _aggregate_wide(d, [args...]; kwargs...)
end 

# aggregate_wide GroupedDataFrame
function aggregate_wide(g::GroupedDataFrame, predicate::AnyColumnPredicate, args...; 
        kwargs...)
    col_mask = cols(g, predicate, incl_groups=false)
    map(g) do gi; aggregate_wide(gi, col_mask, args...; kwargs...); end
end

function aggregate_wide(g::GroupedDataFrame, args::ColumnPredicatedPair...; kwargs...)
    args = map(args) do (a,b); cols(g, a, incl_groups=false) => b; end
    map(g) do gi; aggregate_wide(gi, args...; kwargs...); end
end

function aggregate_wide(g::GroupedDataFrame, key::Symbol, args...; kwargs...)
    map(g) do gi; aggregate_wide(gi, args...; _colkey=key, kwargs...); end
end

# aggregate_wide DataFrameRows
function aggregate_wide(r::DataFrames.DataFrameRows, args...; kwargs...)
    eachrow(aggregate(parent(r), args...; kwargs...))
end



# aggregate_long Function
# aggregate_long AnyDataFrame
function aggregate_long(d::AnyDataFrame, key::Symbol, args...; kwargs...)
    aggregate_long(d, args...; _key = key, kwargs...)
end

function aggregate_long(d::AnyDataFrame, args...; kwargs...)
    aggregate_long(d, cols(d, true), args...; _key = :aggregate, kwargs...)
end

# aggregate_long AbstractDataFrame
function aggregate_long(d::AbstractDataFrame, predicate::AnyColumnPredicate, 
        args...; kwargs...)
    aggregate(d, cols(d, predicate), args...; _key = :aggregate, kwargs...)
end

function aggregate_long(d::AbstractDataFrame, predicate::AnyColumnPredicate, 
        key::Symbol, args...; kwargs...)
    aggregate(d, cols(d, predicate), args...; _key = key, kwargs...)
end

function aggregate_long(d::AbstractDataFrame, args::ColumnPredicatedPair...; kwargs...) 
    args = map(args) do (a,b); cols(d, a) => (expecting_data(b) ? b(d) : b); end
    _aggregate_long(d, [args...]; kwargs...)
end 

# aggregate_long GroupedDataFrame
function aggregate_long(g::GroupedDataFrame, predicate::AnyColumnPredicate, args...; 
        kwargs...)
    col_mask = typeof(predicate)<:Function ? predicate(g, incl_groups=false) : predicate
    map(g) do gi; aggregate_long(gi, col_mask, args...; kwargs...); end
end

function aggregate_long(g::GroupedDataFrame, args::ColumnPredicatedPair...; kwargs...)
    args = map(args) do (a,b); cols(g, a, incl_groups=false) => b; end
    map(g) do gi; aggregate_long(gi, args...; kwargs...); end
end

function aggregate_long(g::GroupedDataFrame, key::Symbol, args...; kwargs...)
    map(g) do gi; aggregate_long(gi, args...; _key = key, kwargs...); end
end

# aggregate_long DataFrameRows
function aggregate_long(r::DataFrames.DataFrameRows, args...; kwargs...)
    eachrow(aggregate(parent(r), args...; kwargs...))
end



# what would be the expected behavior of an inplace aggregation?
aggregate!(args...; kwargs...) = aggregate(args...; kwargs...)
aggregate_wide!(args...; kwargs...) = aggregate_wide(args...; kwargs...)
aggregate_long!(args...; kwargs...) = aggregate_long(args...; kwargs...)



function _aggregate(d::AnyDataFrame; kwargs...)
    DataFrame([k => transform_handler(d,k,k,v) for (k,v)=kwargs]...)
end

function _aggregate(d, pred::AnyColumnPredicate, args...; kwargs...)
    args   = [a isa SymbolContext ? a(d) : a for a=args]
    kwargs = [k => v isa SymbolContext ? v(d) : v for (k, v)=kwargs]
    __aggregate(d, pred, args...; kwargs...)
end

function _aggregate(d::AnyDataFrame, pred::AnyColumnPredicate, key::Symbol, 
        args...; kwargs...)
    __aggregate(d, pred, args...; _key=key, kwargs...)
end

function __aggregate(d::AnyDataFrame, pred::AnyColumnPredicate, args...; 
        _namefunc::Union{Function,SymbolContext}=default_naming_func, 
        _makeunique::Bool=false,
        _key::Union{Symbol,Missing}=missing,
        _colkey::Union{Symbol,Missing}=missing, kwargs...)

    @assert(length(args) + length(kwargs) > 0, 
        "an aggregating function must be provided.")    
        
    pred_f_pairs = [colmask(d, pred) => f for f in [args..., kwargs...]]
    if _colkey !== missing
        _aggregate_wide(d, 
            pred_f_pairs; 
            _colkey = _colkey, 
            _makeunique = _makeunique)
    elseif _key !== missing
        _aggregate_long(d, 
            pred_f_pairs; 
            _key = _key, 
            _makeunique = _makeunique)
    else
        _aggregate_flat(d, 
            pred_f_pairs; 
            _namefunc = _namefunc, 
            _makeunique = _makeunique)
    end
end

function __aggregate_helper(d, predicate_function_pairs::Array{<:Pair})
    # produce list of affecting functions
    # create mask of affected columns per function
    f_cols_pairs = vcat(map(predicate_function_pairs) do (pred,fs)
        fs isa Union{Tuple,Array} ? [f => pred for f in fs] : 
        fs isa NamedTuple ? [p => pred for p in pairs(fs)] :
        fs => pred
    end...)

    affected_cols_idx = (|).(getindex.(f_cols_pairs, 2)...)
    affected_cols_names = names(d)[affected_cols_idx]

    # calculate results per function for applicable columns
    f_agg_pairs = map(f_cols_pairs) do (fi, col_mask)
        af_cols = zip(affected_cols_names, col_mask[affected_cols_idx])
        f = fi isa Union{Tuple,Pair} ? fi[2] : fi
        agg_res = [cm ? transform_handler(d,c,c,f) : missing for (c,cm)=af_cols]
        infer_func_name(fi, :agg) => agg_res
    end

    f_agg_pairs, affected_cols_names
end

function _aggregate_wide(d, args...; kwargs...)
    make_unique_handler(__aggregate_wide, d, args...; kwargs..., 
        error_replacements = ["makeunique" => "_makeunique"])
end

function __aggregate_wide(d, predicate_function_pairs::Array{<:Pair}; 
        _colkey::Symbol=:column, _makeunique::Bool=false)
    results, pred = __aggregate_helper(d, predicate_function_pairs)
    if !any_affected_cols(predicate_function_pairs)
        return(DataFrame(_colkey => [], results..., makeunique = _makeunique))
    end

    DataFrame(_colkey => pred, results..., makeunique = _makeunique)
end

function _aggregate_long(d, args...; kwargs...)
    make_unique_handler(__aggregate_long, d, args...; kwargs..., 
        error_replacements = ["makeunique" => "_makeunique"])
end

function __aggregate_long(d, predicate_function_pairs::Array{<:Pair}; 
        _key::Symbol=:aggregate, _makeunique::Bool=false)
    results, pred = __aggregate_helper(d, predicate_function_pairs)
    if !any_affected_cols(predicate_function_pairs)
        return(DataFrame(_key => getfield.(results, :first)))
    end

    df = hcat(
        DataFrame(vcat(reshape.(getfield.(results, :second), 1, :)...), 
            pred, 
            makeunique=_makeunique),
        DataFrame(_key => getfield.(results, :first)),
        makeunique = _makeunique)
    df = df[:,[ncol(df), (1:ncol(df)-1)...]]
end

function _aggregate_flat(d, args...; kwargs...)
    make_unique_handler(__aggregate_flat, d, args...; kwargs..., 
            error_replacements = ["makeunique" => "_makeunique"])
end

function __aggregate_flat(d, predicate_function_pairs::Array{<:Pair};
        _namefunc::Union{Function,SymbolContext,Missing}=default_naming_func, 
        _makeunique::Bool=false)
    if !any_affected_cols(predicate_function_pairs); return(DataFrame()); end
    _namefunc = _namefunc === missing ? default_naming_func : _namefunc
    results, col_syms = __aggregate_helper(d, predicate_function_pairs)
    DataFrame([
        sym_helper(_namefunc)(c,n) => [v[ci]]
        for (ci,c)=enumerate(col_syms) for (n,v)=results
        if v[ci] !== missing
    ]...; makeunique = _makeunique)
end



any_affected_cols(p::Array{<:Pair}) = any(map(any ∘ first, p))



infer_func_name(x, default) = default
infer_func_name(x::Pair, default) = x[1] 
infer_func_name(x::NamedTuple, default) = fieldnames(typeof(x))[1]
function infer_func_name(x::Function, default)
    n = nameof(x)
    String(n)[1] == '#' ? :function : n
end



function make_unique_handler(f, args...; error_replacements, kwargs...)
    try 
        f(args...; kwargs...)
    catch e
        if hasfield(typeof(e), :msg)
            throw(typeof(e)(replace(e.msg, error_replacements...)))
        else
            throw(e)
        end
    end
end



function aggregate_macro_helper(f_inplace, f, args...; inplace::Bool=false)
    f = inplace ? gen(f_inplace) : gen(f)
	args = verb_arg_handler(args)
	:($f($(args...)))
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
