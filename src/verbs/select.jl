export select, select!, @select, @select!



function select(data::AnyDataFrame, args...) 
    data[!,colmask(data, args)]
end

function select!(data::AnyDataFrame, args...)
    DataFrames.select!(data, colmask(data, args))
end

function select(data::GroupedDataFrame, args...)
    col_mask = validate_groupby_select_cols(data, colmask(data, args))
    groupby(select(parent(data), col_mask), groupvars(data))
end

function select!(data::GroupedDataFrame, args...)
    col_mask = validate_groupby_select_cols(data, colmask(data, args))
    group_syms = groupvars(data)
    DataFrames.select!(parent(data), col_mask)
    data.cols .= (1:ncol(parent(data)))[in(group_syms).(names(data))]
    data
end



function validate_groupby_select_cols(data::GroupedDataFrame, col_mask)
    excluded_groupvars = setdiff(groupvars(data), names(data)[col_mask])
    if length(excluded_groupvars) > 0
        @warn("Automatically adding grouping variable" * 
            (length(excluded_groupvars) > 1 ? "s " : " ") *
            spoken_list(excluded_groupvars, "'") * ". " *
            "To avoid warnings, add `groupvars` to selections.")
    end
    
    col_mask[data.cols] .= true
    col_mask 
end



macro select(args...)
    esc(:($gen(DataFramesMeta.select)($(args...))))
end

macro select!(args...)
    esc(:($gen(DataFramesMeta.select!)($(args...))))
end
