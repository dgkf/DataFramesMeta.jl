export select, @select

import DataFrames.select

# Approach:
# - Any valid select syntax is handled via `selector` dispatch
# - Dispatch on first argument is used to return either a 
#     (1) result of a selection if data::DataFrame
#     (2) curried function for piping of data
# - Considering declaring new internal types to restrict dispatch of generics 
#     for select-specific usage

# predicate evaluation functions
# - all predicates return an Array{Int8,N} where N = length(names(data))
# - integer values are +1 if column is to be selected, or -1 if column is to 
#   be removed from selection

function select(f::Function, args...)
    data -> select(f(data), args...)
end

# overtly mask DataFrames.select method
function select(data::DataFrame, arg::Function)
    _select(data, cols(data, arg))
end

function select(data::AnyDataFrame, args...) 
    _select(data, cols(data, args))
end



function _select(data::AnyDataFrame, col_mask)
    data[!,col_mask]
end 

function _select(data::GroupedDataFrame, col_mask)
    excluded_groupvars = setdiff(groupvars(data), names(data)[col_mask])
    if length(excluded_groupvars) > 0
        @warn("Automatically adding grouping variable" * 
            (length(excluded_groupvars) > 1 ? "s " : " ") *
            spoken_list(excluded_groupvars, "'") * ". " *
            "To avoid warnings, add `groupvars` to selections.")
    end

    groupsyms = groupvars(data)
    colsyms = union(names(data)[col_mask], groupsyms)
    groupby(parent(data)[!,colsyms], groupsyms)
end



macro select(args...)
	data = gensym()
    esc(:($data -> DataFramesMeta.select($data, $(args...))))
end
