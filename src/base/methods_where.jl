import Base.all, Base.any, Base.Broadcast.broadcasted

is_columnwise_op(f) = nameof(f) == :columnwise_op

# this function feels overly recursive, can probably be improved by separating
# conversion to a common Bool array mask from the reduction of bool masks 
# instead of reducing every leaf of a tree.

function columnwise_comparison(data::AnyDataFrame, fs::Function...; op=&)
    vals = (is_columnwise_op(f) ? f(data) : f(col) for col=eachcol(data), f=fs)
    columnwise_comparison(vals...; op=op)
end

function columnwise_comparison(data::AnyDataFrame, pfs::Pair...; op=&)
    vals = map(pfs) do (p,f)
        col_mask = colmask(data, p)
        if !any(col_mask); return(typeof(op) == typeof(&)); end
        columnwise_comparison(sym(data, col_mask), f; op=op)
    end
    columnwise_comparison(vals...; op=op)
end

function columnwise_comparison(args::Function...; op=&)
    function columnwise_op(x)
        columnwise_comparison(x, args...; op=op)
    end
    columnwise_op
end

function columnwise_comparison(args::Pair...; op=&)
    function columnwise_op(x)
        columnwise_comparison(x, args...; op=op)
    end
    columnwise_op
end

function columnwise_comparison(bs...; op=&)
    reduce((l,r) -> op.(l,r), bs)
end



function all(bs::Array{T} where T<:Union{Bool,AbstractArray{Bool}}) 
    if length(bs) == 0; return(true); end
    reduce((l,r) -> broadcast(&,l,r), bs)
end

function all(bs::Union{Bool,AbstractArray{Bool}}...)
    if length(bs) == 0; return(true); end
    reduce((l,r) -> broadcast(&,l,r), bs)
end

function any(bs::Array{T} where T<:Union{Bool,AbstractArray{Bool}})
    if length(bs) == 0; return(false); end
    reduce((l,r) -> broadcast(|,l,r), bs)
end

function any(bs::Union{Bool,AbstractArray{Bool}}...)
    if length(bs) == 0; return(false); end
    reduce((l,r) -> broadcast(|,l,r), bs)
end



"""
    all()

Return a column selection function which will select all available columns.
"""
all() = x -> convert(Array{Int8,1}, repeat([2], length(names(x))))



"""
    all(fs::Function...)

Wrap multiple filtering functions into a new filtering function, returning 
`true` only when elementwise-all returned values are `true`.

# Details

This flavor of `all` is intended specifically for convenient `DataFrame` row
selection, aggregating multiple filtering functions.

    df |> @where(at => Number, all(x -> x .> 2))

$(doc_see_also("all", doc_family_where()))

# Examples

```julia-repl
julia> df = DataFrame(x = 1:4, y = repeat([1, 2], 2))
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 1     │
│ 4   │ 4     │ 2     │

julia> all(x -> x .> 1, x -> x .<= 3)(df)
4-element BitArray{1}:
 0
 1
 0
 0
```
"""
all(fs::Function...) = columnwise_comparison(fs...; op=&)
all(df::AnyDataFrame) = all(all.(eachcol(df)))

function all(pfs::Union{<:Function,Pair{<:Any,<:Function}}...)
    pfs = (typeof(f)<:Function ? true => f : f for f=pfs)
    function columnwise_op(x)
        columnwise_comparison(x, pfs...; op=&)
    end
    columnwise_op
end

function broadcasted(t::typeof(all), fs::Function...) 
    columnwise_comparison(fs...; op=&)
end


"""
    any(fs::Function...)

Wrap multiple filtering functions into a new filtering function, returning 
`true` only when elementwise-any returned values are `true`.

# Details

This flavor of `any` is intended specifically for convenient `DataFrame` row
selection, aggregating multiple filtering functions.

    df |> @where(at => Number, any(x -> x .> 2))

$(doc_see_also("any", doc_family_where()))

# Examples

```julia-repl
julia> df = DataFrame(x = 1:4, y = repeat([1, 2], 2))
4×2 DataFrame
│ Row │ x     │ y     │
│     │ Int64 │ Int64 │
├─────┼───────┼───────┤
│ 1   │ 1     │ 1     │
│ 2   │ 2     │ 2     │
│ 3   │ 3     │ 1     │
│ 4   │ 4     │ 2     │

julia> any(x -> x .> 1, x -> x .<= 3)(df)
4-element BitArray{1}:
 1
 1
 1
 1
```
"""
any(df::AnyDataFrame) = any(any.(eachcol(df)))

function any(pfs::Union{<:Function,Pair{<:Any,<:Function}}...)
    pfs = (typeof(f)<:Function ? true => f : f for f=pfs)
    function columnwise_op(x)
        columnwise_comparison(x, pfs...; op=|)
    end
    columnwise_op
end

function broadcasted(t::typeof(any), fs::Function...)
    columnwise_comparison(fs...; op=|)
end
