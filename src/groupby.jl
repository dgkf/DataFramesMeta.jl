export @groupby, groupby
import DataFrames.groupby

function groupby_(df::AbstractDataFrame, args::ColumnSelector...; kwargs...)
    DataFrames.groupby(df, names(df)[column_selectors(df, args)]; kwargs...)
end

macro groupby(args...)
    esc(:(data -> $DataFramesMeta.groupby_(data, $(args...))))
end
