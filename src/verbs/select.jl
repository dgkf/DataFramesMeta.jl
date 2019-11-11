export select, select!, @select, @select!

import DataFrames.select, DataFrames.select!

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

function select(args...)
	partial_verb(select, args...)
end

function select!(args...)
	partial_verb(select!, args...)
end

# overtly mask DataFrames.select method
function select(data::DataFrame, arg::Function)
	data[!,cols(data, arg)]
end

function select!(data::DataFrame, arg::Function)
	DataFrames.select!(data, cols(data, arg))
end

# overtly handle ColumnPredicateFunction as first arg so it doesn't get
# mistreated as a data frame transformation function
function select(cpf::ColumnPredicateFunction, args...)
	partial_verb(select, cpf, args...)
end

function select!(cpf::ColumnPredicateFunction, args...)
	partial_verb(select!, cpf, args...)
end

function select(data::AnyDataFrame, args...) 
	data[!,cols(data, args)]
end

function select!(data::AnyDataFrame, args...)
	DataFrames.select!(data, cols(data, args))
end

function select(data::GroupedDataFrame, args...)
	col_mask = validate_groupby_select_cols(data, cols(data, args))
	groupby(select(parent(data), col_mask), groupvars(data))
end

function select!(data::GroupedDataFrame, args...)
	col_mask = validate_groupby_select_cols(data, cols(data, args))
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
	esc(:(select($(args...))))
end

macro select!(args...)
	esc(:(select!($(args...))))
end
