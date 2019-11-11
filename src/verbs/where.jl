export where, where!, @where, @where!
import DataFrames.deleterows!, DataFrames.names

"""
    where(data [, predicate], conditions...)
    where(data, predicate => condition...)

`where` is used for subsetting rows of a `DataFrame` based on a criteria or
`Bool`-like `Array` of rows indices. 



# Arguments

* `data`: $(doc_verb_arg_data())

* `predicate`: $(doc_verb_arg_predicate())

* `condition`: A `Bool`-like `Array` used as a mask for subsetting rows in a
    `DataFrame` or a function, which when applied to a column `Array`, results
    in a `Bool`-like `Array`.



# Details

Most commonly, `where` is used to explicitly evaluate a criteria against
columns explicitly, resulting in a `Bool`-like `Array` which is `true` for
rows to keep.

`where` can also accept functions which will return a `Bool`-like `Array`
subset, which must be satisifed for all columns.

It's also common to want to apply a filter across a subset of columns. For
this, a predicate can be provided to specify a subset of columns, and all
subsetting functions must be satisfied across all predicated columns, though
this can be articulated concretely using [`all`](@ref) or [`any`](@ref)

`where` also has a in-place analog, `where!` and both have macro equivalent
functions, `@where` and `@where!`. All macro verbs return a function
expecting a single `DataFrame`, interpret predicate arguments and symbols
within the context of the provided `DataFrame`.

## Macro Usage

$(doc_verb_macro())



# Examples

```jdoctest
julia> df = DataFrame(x = 1:4, y = repeat([1, 2], 2), z = 'a':'d')
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 1     │ 1     │ 'a'  │
│ 2   │ 2     │ 2     │ 'b'  │
│ 3   │ 3     │ 1     │ 'c'  │
│ 4   │ 4     │ 2     │ 'd'  │
```

Subset columns based on a `Bool`-like `Array`

```jdoctest
julia> df |> @where(:x .>= 3)
2×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 3     │ 1     │ 'c'  │
│ 2   │ 4     │ 2     │ 'd'  │

julia> df |> @where(:x .>= 3, :y .== 2)
1×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 4     │ 2     │ 'd'  │
```

Subset rows when all columns meet satisfy a subsetting function

```jdoctest
julia> df |> @where(x -> x .== maximum(x))
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 4     │ 2     │ 'd'  │
```

Use `all` and `any` to specify column-wise behavior. For example, we can
subset for rows where any value is equal to its column maximum. `all` and `any`
can be nested to build extensive column-wise boolean expressions.

```jdoctest
julia> df |> @where(any(x -> x .== maximum(x)))
2×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 2     │ 2     │ 'b'  │
│ 2   │ 4     │ 2     │ 'd'  │
```

Using a column predicate to apply filtering functions to a subset of columns

```jdoctest
julia> df |> @where(at => Number, x -> x .<= 2)
2×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 1     │ 1     │ 'a'  │
│ 2   │ 2     │ 2     │ 'b'  │

julia> df |> @where(at => Number, any(x -> x .<= 2))
4×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 1     │ 1     │ 'a'  │
│ 2   │ 2     │ 2     │ 'b'  │
│ 3   │ 3     │ 1     │ 'c'  │
│ 4   │ 4     │ 2     │ 'd'  │
```

Specifying multiple predicates 

```jdoctest
julia> df |> @where(
    Number => any(x -> iseven.(x)), 
    Char   => x -> 'd' .>= x .>= 'b')
2×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 2     │ 2     │ 'b'  │
│ 2   │ 4     │ 2     │ 'd'  │
```

Specifying multiple predicates via a columnwise aggregation function

```jdoctest
julia> df |> @where(all(
		any(Number, x -> iseven.(x)),
		Char => x -> 'd' .>= x .>= 'b'
	))
2×3 DataFrame
│ Row │ x     │ y     │ z    │
│     │ Int64 │ Int64 │ Char │
├─────┼───────┼───────┼──────┤
│ 1   │ 2     │ 2     │ 'b'  │
│ 2   │ 4     │ 2     │ 'd'  │
```
"""
function where(args...; kwargs...)
    partial_verb(where, args...; kwargs...)
end

function where(data::AnyDataFrame, args...; kwargs...)
    _where!(copy(data), args...; kwargs...)
end

function where(data::AnyDataFrame, x::AbstractArray{Bool}) 
    _where!(copy(data), x)
end

function where(data::GroupedDataFrame, args...; kwargs...)
    groupby(where!(copy(parent(data)), args...; kwargs...), data.cols)
end

function where(data::GroupedDataFrame, predicate::AnyColumnPredicate, args...)
	groupby(where!(copy(parent(data)), predicate, args...), data.cols)
end

function where(data::DataFrames.DataFrameRows, args...; kwargs...)
    _where!(eachrow(copy(parent(data))), args...; kwargs...)
end

@doc (@doc where) 
function where!(args...; kwargs...)
    partial_verb(where!, args...; kwargs...)
end

function where!(data::AnyDataFrame, args...; kwargs...)
    _where!(data, args...; kwargs...)
end

function where!(data::GroupedDataFrame, args...; kwargs...)
    groupby(_where!(parent(data), args...; kwargs...), data.cols)
end



# Extend DataFrames behaviors for non-standard DataFrame-like Types
deleterows!(d::DataFrames.DataFrameRows, x) = deleterows!(parent(d), x)
names(d::DataFrames.DataFrameRows) = names(parent(d))

function _where!(d::AnyDataFrame, args...)
	_where!(d, [cols(d, true) => (args...,)])
end

function _where!(d::AnyDataFrame, predicate::AnyColumnPredicate, args...)
    _where!(d, [predicate => (args...,)])
end

function _where!(d::AnyDataFrame, predicate_pairs::PredPair...)
    _where!(d, [predicate_pairs...])
end

function _where!(d::AnyDataFrame, predicate_pairs::Array{<:ColumnPredicatedPair})
    i = (!).(reduce((l,r) -> l .& r, map(predicate_pairs) do (pred, arg)
        where_handler(d, cols(d, pred), arg)
    end))

    if length(i) == 1 (if i[1] deleterows!(d, 1:nrow(d)) end)
    else deleterows!(d, i)
    end
    d
end



where_handler(d, col_mask, x) = x
where_handler(d::DataFrames.DataFrameRows, col_mask, n::Nothing) = true

function where_handler(d, col_mask, f::Function)
    if expecting_data(f); where_handler(d, col_mask, f(d[!,col_mask]))
    else; where_handler(d, missing, mapcols(f, d[!,col_mask]))
    end
end

function where_handler(d::DataFrames.DataFrameRows, col_mask, 
        f::Function)
	cell_results = (expecting_data(f) ? f(d) : f).([r[c] for r=d, c=col_mask])
    where_handler(d, cols, cell_results)
end

function where_handler(d, col_mask, t::Union{<:Tuple,<:NamedTuple})
    all.((where_handler(d, col_mask, ti) for ti=t)...)
end

function where_handler(d, col_mask::Missing, x::AnyDataFrame) 
    @assert(all((<:).(typeof.(eachcol(x)), AbstractArray{Bool})), 
        "all values of where function result must be a subtype of " *
            "AbstractArray{Bool}")
    if ncol(x) == 0; return true; end
    reduce((l,r) -> l .& r, eachcol(x))
end

function where_handler(d, col_mask, x::AnyDataFrame)
    @assert(all((<:).(typeof.(eachcol(x)), AbstractArray{Bool})), 
        "all values of where function result must be a subtype of " *
            "AbstractArray{Bool}")
    if isempty(eachcol(x[!,col_mask])); return true; end
    reduce((l,r) -> l .& r, eachcol(x[!,col_mask]))
end



function where_macro_helper(args...; inplace::Bool=false)
    f = inplace ? where! : where
	args = verb_arg_handler(args, key=false)
	:($f($(args...)))
end

@doc (@doc where) 
macro where(args...)
    esc(where_macro_helper(args...))
end

@doc (@doc where) 
macro where!(args...)
    esc(where_macro_helper(args...; inplace = true))
end
