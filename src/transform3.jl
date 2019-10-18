export transform, transform!, @transform, @transform!



"""
    transform([data], [at => <predicate>], [x]; kwargs...)
    transform!([data], [at => <predicate>], [x]; kwargs...)
    @transform([data], [at => <predicate>], [x]; kwargs...)
    @transform!([data], [at => <predicate>], [x]; kwargs...)

`transform` is used to apply column-wise modifications to a DataFrame. A macro
equivalent function is provided which interprets symbols as column names in 
the provided DataFrame. It's possible to apply a transformation function across 
multiple columns by using a predicate column selector.

# Arguments

* `data`: A `DataFrame` or `GroupedDataFrame` on which to perform 
    transformations. If omitted, `transform` returns a function expecting a 
    single argument, `data`.
* `predicate`: A `Pair` with name `:at` (or `at` when using the macro flavors 
    of `transform`). The `Pair` value is passed to `selectors` for making a 
    column selection over which to perform transforms.
* `x`: Only applicable if `predicate` is specified, `x` is an anonymous 
    function which can be applied to columns selected by `predicate`. The 
    function accepts the current column value and returns values which can be 
    broacast over the new column.
* `kwargs`: Named arguments should be new values which can be broadcast into a 
    new column, or a function that can be applied to a target column to produce 
    new column values. When `predicate` is defined, named arguments create a 
    new column for each named argument. The names of these new columns can be 
    defined using a named argument, `_namefunc`
* `_namefunc`: When `predicate` is defined, and named column definitions are 
    provided, a function can be used to build the new column name. `_newfunc` 
    should accept two `String` arguments, the existing column name and the 
    column definition argument name.

# Details

### Composing Transforms

The `transform` function will return a DataFrame if the `data` argument is 
provided, otherwise it will return a function expecting a single argument 
representing the inbound data, allowing for piping of data through a series of 
data operations. 

    julia> df = DataFrame(x = 1:3)

    julia> @transform(y = :x .* 2)
    (generic function with 1 method)

    julia> df |> @transform(y = :x .* 2)
    3×2 DataFrame
    │ Row │ x     │ y     │
    │     │ Int64 │ Int64 │
    ├─────┼───────┼───────┤
    │ 1   │ 1     │ 2     │
    │ 2   │ 2     │ 4     │
    │ 3   │ 3     │ 6     │

### Predicate Selections

Any valid input for `[selectors](@ref)` can be passed as a predicate, by 
providing a value as a Pair with name `:at` (or `at` when macros are used). 
When provided, the predicate will be used to apply function `x` over all 
selected columns. Named arguments will then be applied for each of the 
selected columns, creating a new column per selected column per named argument.
The naming convention of these new columns can be specified by overwriting a 
named argument, `_namefunc::Function`, a function expecting two arguments, the 
selected column name and the keyworded argument keyword.

    julia> df = DataFrame(x = 1:3, y = 'a':'c', z = [true, false, true])

    julia> transform(df, :at => (:x, :y), x -> x .+ 2)
    3×3 DataFrame
    │ Row │ x     │ y    │ z     │
    │     │ Int64 │ Char │ Bool  │
    ├─────┼───────┼──────┼───────┤
    │ 1   │ 3     │ 'c'  │ true  │
    │ 2   │ 4     │ 'd'  │ false │
    │ 3   │ 5     │ 'e'  │ true  │

    julia> transform(df, :at => Number; mean = x -> sum(x)/length(x))
    3×5 DataFrame
    │ Row │ x     │ y    │ z     │ x_mean  │ z_mean   │
    │     │ Int64 │ Char │ Bool  │ Float64 │ Float64  │
    ├─────┼───────┼──────┼───────┼─────────┼──────────┤
    │ 1   │ 1     │ 'a'  │ true  │ 2.0     │ 0.666667 │
    │ 2   │ 2     │ 'b'  │ false │ 2.0     │ 0.666667 │
    │ 3   │ 3     │ 'c'  │ true  │ 2.0     │ 0.666667 │

### Alternative Macro Syntax

The macro versions of the `transform` family of functions are functionally 
equivalent, but have a few slight modifications for simpler call syntax.
    
    - `at` instead of `:at` can be used for defining a predicate, despite 
      being otherwise syntactically invalid, e.g. `at => :x`
    - transformation functions can reference `DataFrame` columns using symbolic 
      column names

# Examples

```jdoctest
julia> df = DataFrame(x = 1:3, y = 'a':'c', z = [true, false, true])

julia> transform(df, a = df[!,:x] .* 3)
3×4 DataFrame
│ Row │ x     │ y    │ z     │ a     │
│     │ Int64 │ Char │ Bool  │ Int64 │
├─────┼───────┼──────┼───────┼───────┤
│ 1   │ 1     │ 'a'  │ true  │ 3     │
│ 2   │ 2     │ 'b'  │ false │ 6     │
│ 3   │ 3     │ 'c'  │ true  │ 9     │

julia> transform(df, :at => all(), x -> string.(x), original = x -> x)
3×6 DataFrame
│ Row │ x      │ y      │ z      │ x_original │ y_original │ z_original │
│     │ String │ String │ String │ Int64      │ Char       │ Bool       │
├─────┼────────┼────────┼────────┼────────────┼────────────┼────────────┤
│ 1   │ 1      │ a      │ true   │ 1          │ 'a'        │ true       │
│ 2   │ 2      │ b      │ false  │ 2          │ 'b'        │ false      │
│ 3   │ 3      │ c      │ true   │ 3          │ 'c'        │ true       │

julia> @transform(df, at => all(), x -> x .+ :x)
3×3 DataFrame
│ Row │ x     │ y    │ z     │
│     │ Int64 │ Char │ Int64 │
├─────┼───────┼──────┼───────┤
│ 1   │ 2     │ 'b'  │ 2     │
│ 2   │ 4     │ 'd'  │ 2     │
│ 3   │ 6     │ 'f'  │ 4     │

julia> df |> @transform(a = :x .* 2)
3×4 DataFrame
│ Row │ x     │ y    │ z     │ a     │
│     │ Int64 │ Char │ Bool  │ Int64 │
├─────┼───────┼──────┼───────┼───────┤
│ 1   │ 1     │ 'a'  │ true  │ 2     │
│ 2   │ 2     │ 'b'  │ false │ 4     │
│ 3   │ 3     │ 'c'  │ true  │ 6     │

julia> df |> @transform(at => Number, x -> x .* 2)
3×3 DataFrame
│ Row │ x     │ y    │ z     │
│     │ Int64 │ Char │ Int64 │
├─────┼───────┼──────┼───────┤
│ 1   │ 2     │ 'a'  │ 2     │
│ 2   │ 4     │ 'b'  │ 0     │
│ 3   │ 6     │ 'c'  │ 2     │
```
"""
function transform(data::AbstractDataFrame, predicate::Union{Pair,Nothing}=nothing,
        x::Union{Function,Nothing}=nothing; kwargs...)
    result = copy(data)
    transform!(result, predicate, x; kwargs...)
    result
end
function transform(data::GroupedDataFrame, predicate::Union{Pair,Nothing}=nothing,
        x::Union{Function,Nothing}=nothing; kwargs...)
    result = groupby(copy(data.parent), data.cols)
    transform!(result, predicate, x; kwargs...)
    result
end
transform(data::AnyDataFrame, predicate::Pair; kwargs...) = 
    transform(data, predicate, nothing; kwargs...)
transform(predicate::Pair, x::Union{Function,Nothing}=nothing; kwargs...) = 
    data::Union{Function,AnyDataFrame} -> transform(data, predicate, x; kwargs...)
transform(;kwargs...) = 
    data::Union{Function,AnyDataFrame} -> transform(data, nothing, nothing; kwargs...)
transform(f::Function, args...; kwargs...) = 
    data::Union{Function,AnyDataFrame} -> transform(f(data), args...; kwargs...)

@doc (@doc transform) 
transform!(data::AnyDataFrame, predicate::Union{Pair,Nothing}=nothing,
        x::Union{Function,Nothing}=nothing; kwargs...) = 
    transform_!(data, predicate, x; kwargs...)
transform!(data::AnyDataFrame, predicate::Pair=(:at => true); kwargs...) = 
    transform!(data, predicate, nothing; kwargs...)
transform!(predicate::Pair, x::Union{Function,Nothing}=nothing; kwargs...) = 
    data::Union{Function,AnyDataFrame} -> transform!(data, predicate, x; kwargs...)
transform!(;kwargs...) = 
    data::Union{Function,AnyDataFrame} -> transform!(data, nothing, nothing; kwargs...)
transform!(f::Function, args...; kwargs...) = 
    data::Union{Function,AnyDataFrame} -> transform!(f(data), args...; kwargs...)



"""
`transform_` operator, used by DataType-specific handlers
"""
function transform_!(d::AbstractDataFrame, predicate::Pair, x; 
        _namefunc::Union{Function,Nothing}=(col, kw)->col*"_"*kw, kwargs...)

    _namefunc = expecting_data(_namefunc) ? _namefunc(d) : _namefunc
    cols = names(d)[column_selectors(d, predicate.second)]
    dyn_cols = Symbol.(_namefunc(c,k) for c in string.(cols) for k in string.(keys(kwargs)))

    @assert(!any(in(names(d)).(dyn_cols)), 
        "New column names will overwrite existing columns")

    for col in cols
        for (k, v) in kwargs
            new_col = Symbol(_namefunc(string(col), string(k)))
            transform_handler!(d, col, new_col, v)
        end
        if (x !== nothing)
            transform_handler!(d, col, col, x)
        end
    end
end

function transform_!(g::GroupedDataFrame, predicate::Pair, x; 
        _namefunc::Union{Function,Nothing}=(col, kw)->col*"_"*kw, kwargs...)

    _namefunc = expecting_data(_namefunc) ? _namefunc(g) : _namefunc
    cols = names(g)[column_selectors(g, predicate.second)]
    dyn_cols = Symbol.(_namefunc(c,k) for c in string.(cols) for k in string.(keys(kwargs)))

    @assert(!any(in(names(g)).(dyn_cols)), 
        "New column names will overwrite existing columns")
    
    @assert((x === nothing || !any(in(groupvars(g)).(cols))) && !any(in(groupvars(g)).(dyn_cols)),
        "transform is attempting to modify a grouping variable. " *
        "To affect grouping variables you must first ungroup.")

    for col in cols
        for (k, v) in kwargs
            new_col = Symbol(_namefunc(string(col), string(k)))
            transform_handler!(g, col, new_col, v)
        end
        if (x !== nothing)
            transform_handler!(g, col, col, x)
        end
    end
end

transform_!(a::AnyDataFrame, predicate::Nothing, x::Nothing; kwargs...) =
    for (k, v) in kwargs; transform_handler!(a, k, k, v); end



transform_handler(a::AbstractDataFrame, col::Symbol, into::Symbol, x) = [x]
transform_handler(a::AbstractDataFrame, col::Symbol, into::Symbol, x::Array) = x
transform_handler(a::AbstractDataFrame, col::Symbol, into::Symbol, x::Function) =
    transform_handler(a, col, into, expecting_data(x) ? x(a) : x(a[!,col]))
transform_handler!(a::AbstractDataFrame, col::Symbol, into::Symbol, x) =
    a[!,into] .= x
transform_handler!(a::AbstractDataFrame, col::Symbol, into::Symbol, x::Function) =
    transform_handler!(a, col, into, expecting_data(x) ? x(a) : x(a[!,col]))
function transform_handler!(g::GroupedDataFrame, col::Symbol, into::Symbol, x)
    g.parent[!,into] = permute!(reduce(vcat, map(zip(g, g.starts, g.ends)) do (gi, s, e)
        out = transform_handler(gi, col, into, x)
        length(out) == 1 ? repeat(out, e-s+1) : out
    end), g.idx)
end



function transform_macro_helper(args...; inplace::Bool=false)
    a, kw = split_macro_args(args)
    data, predicate, x = split_on_pair(a)
    :($(inplace ? transform! : transform)(
        $(data...), 
        $(predicate...),
        $(map(e -> symbol_context(e), x)...);
        $(map(e -> Expr(:kw, e.args[1], symbol_context(e.args[2])), kw)...)))
end

@doc (@doc transform) 
macro transform(args...)
    esc(:($(transform_macro_helper(args...))))
end

@doc (@doc transform) 
macro transform!(args...)
    esc(:($(transform_macro_helper(args...; inplace = true))))
end
