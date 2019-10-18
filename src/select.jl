export select, @select

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

# use multiple dispatch to determine whether function should be "curried"
select(data::AnyDataFrame, args...) = select_(data, column_selectors(data, args))

# subsetting helpers
select_(data::AnyDataFrame, cols) = data[!,cols]
function select_(data::GroupedDataFrame, cols)
    excluded_groupvars = setdiff(groupvars(data), names(data)[cols])
    if (length(excluded_groupvars) > 0)
        @warn("Automatically adding grouping variable" * 
            (length(excluded_groupvars) > 1 ? "s " : " ") *
            spoken_list(excluded_groupvars, "'") * ". " *
            "To avoid warnings, add `groupvars` to selections.")
    end
    colsyms = setdiff(names(data)[cols], groupvars(data))
    map(df -> select_(df, colsyms), data)
end

macro select(args...)
    esc(:(data -> $select(data, $(args...))))
end
