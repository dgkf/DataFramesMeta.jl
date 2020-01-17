export column_selector, cols

import Base.show
import DataFrames.mapcols, DataFrames.convert

# DataFrames extensions
function mapcols(f::Function)
    x -> mapcols(f, x)
end

function (-)(x::AnyDataFrame)
    mapcols(col -> col .* -1, x)
end

# Predicate DataTypes
struct ColumnPredicateFunction <: Function
    f::Function
end

function (pred::ColumnPredicateFunction)(x) 
    pred.f(x)
end

function (pred::ColumnPredicateFunction)(x::GroupedDataFrame; incl_groups::Bool=true)
    group_col_mask = (!in)(x.cols).(1:size(x.parent)[2])
    col_mask = pred.f(x).mask .& (incl_groups .| group_col_mask)
end

struct ColumnMask{T<:Bool} <: AbstractArray{T,1}
    mask::AbstractArray{T,1}
end

function Base.show(io::IO, m::MIME"text/plain", x::ColumnMask{<:Bool})
    print("ColumnMask ")
    show(io, m, x.mask)
end

AnyColumnPredicate = Union{ColumnMask,ColumnPredicateFunction}
ColumnPredicatedPair = Pair{<:AnyColumnPredicate,}


"""
    column_selector(data, value)

`selector` can be used to create an Array{Int,1} with length equal to the number
of columns in `data` indicating whether to add (+1), or subtract (-1) columns
from a selection. 

### Arguments

* `data` : an AbstractDataFrame or GroupedDataFrame to use for column selection
* `value` : one of many types of selection inputs
   - `Bool` : select (true) or remove (false) all columns
   - `Function` : the selection result of the return value of `value::Function` 
        when applied across `data` columns, or to `data`
   - `Integer` : select a column by column index
   - `Symbol` : select a column by column name
   - `AbstractString` : select a column by column name as string
   - `UnitRange` : select columns between two columns by name
   - `DataType` : select all columns of a given DataType
   - `Regex` : select columns by regex match of column names
   - `Array{Bool,1}` or `BitArray{1}` : select columns by Bool mask
   - `Array{Int,1}` : select columns by array of indices, or by integer 
     selection (+1), or removal (0 or -1)
   - `Tuple` or `Array` : Other `Tuple` or `Array` arguments are unpacked and 
        applied in sequence

### Details

`selector` is used to map many mechanisms of communicating column selection
down to a common masking array of -1 (remove) 0 (remove) or 1 (add).
Selectors are only [-2, -1, 0] (for removal) or [0, 1, 2] (for addition) and
will always have a length equal to the number of columns in the data source.
These selection masks can be sequentially applied using `selectors`,
collapsing the `selector` `Array{Int,1}` into a `Array{Bool,1}` mask of
selected columns.

If the absolute value of a selector is 2, it will add (or remove) even if it's
the only selector available, whereas if it is 1 it will assume an initial 
complete or absent selection. As an example, we want `-all()` to remove all
selections, whereas we want `-:x` to assume a complete selection prior to taking
effect.

Also works with any generic which can be partially evaluated to generate a 
function: `startswith`, `endswith`, `occursin`, `everything`

```julia
df = DataFrame(x = 1:26, y = 'a':'z', z = repeat([true, false], 13))
DataFramesMeta.column_selector(df, :x)      # [ 1, 0, 0] 
DataFramesMeta.column_selector(df, -:x)     # [-1, 0, 0] 
DataFramesMeta.column_selector(df, -all())  # [-2,-2,-2]
DataFramesMeta.column_selector(df, -:a)     # [-1,-1,-1] 
DataFramesMeta.column_selector(df, 2:3)     # [ 0, 1, 1] 
```

### Examples

```jldoctest
julia> using DataFrames, DataFramesMeta

julia> df = DataFrame(x = 1:26, y = 'a':'z', z = repeat([true, false], 13));

julia> DataFramesMeta.column_selector(df, :x)  # by symbol

julia> DataFramesMeta.column_selector(df, [1, 3])  # by Int (or Array{Int,1})

julia> DataFramesMeta.column_selector(df, Number)  # by DataType

julia> DataFramesMeta.column_selector(df, false)  # by Bool

julia> DataFramesMeta.column_selector(df, startswith("x"))  # by Function

julia> # by Function applied to columns (default for anonymous functions)
       DataFramesMeta.column_selector(df, x -> length(unique(x)) < length(x))

julia> # by Function applied to DataFrame (when input is strongly typed)
       DataFramesMeta.column_selector(df, df::AbstractDataFrame -> names(df) .= :x)

julia> DataFramesMeta.column_selector(df, :x : :z)  # by Symbol range

julia> DataFramesMeta.column_selector(df, r"[xz]")  # by Regex

julia> DataFramesMeta.column_selector(df, [true, false, true])  # by Array{Bool,1}

julia> DataFramesMeta.column_selector(df, (:x, 3))  # by Tuple or Array{Any,1}
```
"""
column_selector(data::AbstractDataFrame) = 
    column_selector(data, false)

column_selector(data::AbstractDataFrame, arg::ColumnMask) = 
    column_selector(data, arg.mask)

column_selector(data::AbstractDataFrame, arg::typeof(all))::Array{Int8,1} = 
    convert(Array{Int8,1}, repeat([2], length(names(data))))

column_selector(data::AbstractDataFrame, arg::Union{Tuple,Array})::Array{Int8,1} = 
    colmask(data, arg...)
    
column_selector(data::AbstractDataFrame, arg::Bool)::Array{Int8,1} = 
    repeat([arg ? 2 : -2], length(names(data)))

column_selector(data::AbstractDataFrame, arg::Integer)::Array{Int8,1} = 
    column_selector(data, [arg])

column_selector(data::AbstractDataFrame, arg::Symbol)::Array{Int8,1} = 
    in([arg]).(names(data))

column_selector(data::AbstractDataFrame, arg::AbstractString)::Array{Int8,1} = 
    in([arg]).(string.(names(data)))

column_selector(data::AbstractDataFrame, arg::UnitRange{<:Symbol})::Array{Int8,1} =
    column_selector(data, [findfirst(arg.start .== names(data)):findlast(arg.stop .== names(data))...])

column_selector(data::AbstractDataFrame, arg::UnitRange{<:Real})::Array{Int8,1} = 
    column_selector(data, [arg...])

column_selector(data::AbstractDataFrame, arg::DataType)::Array{Int8,1} = 
    (<:).(eltype.(eachcol(data)), arg)

column_selector(data::AbstractDataFrame, arg::Regex)::Array{Int8,1} = 
    match.(arg, string.(names(data))) .!= nothing

column_selector(data::AbstractDataFrame, arg::Function)::Array{Int8,1} = 
    column_selector(data, reshape(convert(Array, arg(data)), :))

function column_selector(data::AbstractDataFrame, arg::Union{Array{Bool,1},BitArray{1}})::Array{Int8,1}
    @assert(length(arg) == length(names(data)),
        "length of $(typeof(arg)) used for $(typeof(data)) column " *
        "selection should match the number of columns in the dataset")
    arg
end

function column_selector(data::AbstractDataFrame, arg::Array{T,1} where T<:Integer)::Array{Int8,1}
    if (!in(0, arg) && length(unique(arg)) == length(arg)) # column indicies
        @assert(maximum(sign.(arg)) - minimum(sign.(arg)) <= 1,
            "column indices for selection should be either all negative or " *
            "all positive, but not both.")
        all(arg .< 0) ? 
            -in(-arg).(1:length(names(data))) .- 1 : 
            in(arg).(1:length(names(data)))
    else # column mask
        @assert(length(arg) == length(names(data)), 
            "column selection by Array{Int} must have length equal to the " *
            "number of columns")
        arg
    end
end

column_selector(data::ChildDataFrame, arg) = 
    column_selector(parent(data), arg)

column_selector(data::ChildDataFrame, arg::Union{Tuple,Array})::Array{Int8,1} = 
    colmask(data, arg...)

column_selector(data::ChildDataFrame,
        arg::Union{Array{Bool,1},BitArray{1},Array{T,1} where T<:Integer}) = 
    column_selector(parent(data), arg)

function column_selector(data::ChildDataFrame, arg::Function)::Array{Int8,1}
    # for group-specific funcs, e.g. groupvars
    try column_selector(data, arg(data)) 
    catch error
        if error isa MethodError; return column_selector(parent(data), arg); end
    end
end



"""
    cols(data, args...)

`selectors` can be used to create an `Array{Bool,1}` selection mask with 
length equal to the number of columns in `data` indicating whether to 
include (`true`), or exclude (`false`) columns.

`args...` are passed to `selector`, which is used to unify selection types 
into a common selection mask.

### Arguments

* `data` : an `AbstractDataFrame` or `GroupedDataFrame` to use for column 
   selection
* `args...` : one of many types of selection inputs
   - `Bool` : select (true) or remove (false) all columns
   - `Function` : the selection result of the return value of `value::Function` 
      when applied to `data`
   - `Integer` : select a column by column index
   - `Symbol` : select a column by column name
   - `AbstractString` : select a column by column name as string
   - `UnitRange` : select columns between two columns by name
   - `DataType` : select all columns of a given DataType
   - `Regex` : select columns by regex match of column names
   - `Array{Bool,1}` or `BitArray{1}`: select columns by Bool mask
   - `Array{Int,1}` : select columns by array of indices, or by integer 
     selection (+1), or removal (0 or -1)
   - `Tuple` or `Array` : Other `Tuple` or `Array` arguments are unpacked and 
      applied in sequence

### Details

`selectors` is used to map many mechanisms of communicating column selection 
down to a common masking array of `true` (include) or `false` (remove). 
The resulting mask will always have a length equal to the number of columns 
in the data source. 

When called without a `data` argument provided, `selectors` creates an 
anonymous function which expects a single remaining value, `data`, to be 
fully evaluated to a selection mask.

```jldoctest
julia> using DataFrames, DataFramesMeta

julia> df = DataFrame(x = 1:26, y = 'a':'z', z = repeat([true, false], 13));

julia> cols(df, :x, 3)

julia> cols(df, [2, 3], -:z, r"x", -startswith("y"))
```
"""
cols(args...) = 
    cols(args)

cols(s::Tuple; kwargs...) = 
    ColumnPredicateFunction(data::AnyDataFrame -> cols(data, s))

cols(data::AnyDataFrame, args...; kwargs...) = 
    cols(data, args)

cols(data::AnyDataFrame, f::Function; kwargs...) = 
    cols(data, column_selector(data, f))

cols(data::AnyDataFrame, f::ColumnPredicateFunction; kwargs...) =
    cols(data, f(data; kwargs...))

cols(data::AnyDataFrame, f::typeof(all); kwargs...) =
    cols(data, (f,))

cols(data::AnyDataFrame, s; kwargs...) = 
    cols(data, (s,))

function cols(data::AnyDataFrame, s::Tuple)
    ncol_data = length(names(data))
    if ncol_data < 1; return(Array{Bool,1}[]); end

    selection = repeat([0], ncol_data)
    for (i, selector_i) in enumerate(s)
        # convert selection argument to column "predicate" array of (-1, 0, 1)
        # run twice, such that any user function which returns a type that 
        # can be handled gets converted to preferred output
        pred = column_selector(data, selector_i)

        @assert(maximum(sign.(pred)) - minimum(sign.(pred)) <= 1,
            "selectors must either add or remove selected columns, but " * 
            "cannot do both.")

        # handle case where 1st argument is removal (assume initial select all)
        if i == 1 && minimum(pred) < 0; selection .= 1
        elseif maximum(abs.(pred)) > 1; pred .-= sign.(pred)
        end

        selection = min.(max.(selection .+ pred, 0), 1)
    end
    ColumnMask(convert(Array{Bool,1}, selection))
end



"""
Return underlying column mask of ColumnMask object returned from cols
"""
colmask(d::AnyDataFrame, args...; kwargs...) = cols(d, args...; kwargs...).mask

