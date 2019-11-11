export @groupby, groupby
import DataFrames.groupby

function groupby_(args...; kwargs...)
	partial_verb(groupby_, args...; kwargs...)
end

function groupby_(df::AbstractDataFrame, args::ColumnSelector...; kwargs...)
    DataFrames.groupby(df, names(df)[cols(df, args)]; kwargs...)
end

macro groupby(args...)
    esc(:(data -> $DataFramesMeta.groupby_(data, $(args...))))
end
