export col_pred

col_pred(g::GroupedDataFrame, x) = col_pred(g[1], x)

col_pred(d::AbstractDataFrame, x::Array{Symbol})::BitArray{1} = 
    in(x).(names(d))

col_pred(d::AbstractDataFrame, x::Array{Int})::BitArray{1} = 
    in(x).(1:_ncol(d))

col_pred(d::AbstractDataFrame, x::Int)::BitArray{1} = 
    col_pred(d, [x])

col_pred(d::AbstractDataFrame, x::Type)::BitArray{1}=
    eltype.(eachcol(d)) .== x

col_pred(d::AbstractDataFrame, x::Symbol)::BitArray{1}= 
    col_pred(d, [x])

col_pred(d::AbstractDataFrame, x::Bool)::BitArray{1}= 
    x ? trues(_ncol(d)) : falses(_ncol(d))

col_pred(d::AbstractDataFrame, x::Union{Array{Bool},BitArray})::BitArray{1}= 
    (@assert length(x) == _ncol(d)) == nothing ? x : nothing

col_pred(d::AbstractDataFrame, x::Regex)::BitArray{1} = 
    match.(x, string.(names(d))) .!= nothing

col_pred(d::AbstractDataFrame, x::Function)::BitArray{1}= 
    map(x, eachcol(d))

col_pred_helper(x, pred) = :($col_pred($x, $pred))