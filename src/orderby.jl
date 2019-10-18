export orderby, orderby!, @orderby, @orderby!



orderby(data, args...) = orderby_!(copy(data), args...)
orderby(g::GroupedDataFrame, args...) =
    groupby(orderby_!(copy(parent(g)), args...), g.cols)

orderby!(data, args...) = orderby_!(data, args...)
orderby!(g::GroupedDataFrame, args...) =
    groupby(orderby_!(parent(g), args...), g.cols)



function orderby_!(data::AnyDataFrame, args...)
    # known to be painfully slow - would appreciate help using DataFrames 
    # internal sorting functions
    order_vecs = hcat([orderby_handler(data, a) for a=args]..., 1:nrow(data))
    order = sortslices(order_vecs, dims=1)[:,size(order_vecs)[2]]
    order = convert(Array{Integer}, order)
    setindex!(data, data, order, 1:ncol(data))
end



orderby_handler(data::AnyDataFrame, x::Function) = orderby_handler(data, x(data))
orderby_handler(data, x::AbstractArray) = x



function orderby_helper(args...; inplace::Bool=false)
    a, kw = split_macro_args(args)
    :($(inplace ? orderby! : orderby)(
        data, 
        $(map(e -> symbol_context(e), a)...),
        $(map(e -> Expr(:kw, e.args[1], symbol_context(e.args[2])), kw)...)))
end

macro orderby(args...)
    esc(:(data -> $(orderby_helper(args...))))
end

macro orderby!(args...)
    esc(:(data -> $(orderby_helper(args...; inplace=true))))
end
