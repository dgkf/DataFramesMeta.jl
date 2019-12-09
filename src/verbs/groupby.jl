export @groupby, groupby
import DataFrames.groupby

function groupby_(df::AbstractDataFrame, args...; kwargs...)
    DataFrames.groupby(df, names(df)[colmask(df, args)]; kwargs...)
end

macro groupby(args...)
    esc(:($gen(DataFramesMeta.groupby_)($(args...))))
end
