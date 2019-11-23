export case_when, case_when_with

import Base.Broadcast.broadcasted

"""
    case_when(pred, x, rest...)

`case_when` allows a case to be selected dependent on the first `true` case. 
Functionally, this is identical to nesting ifelse functions to build `elseif` 
clauses throughout. If an even number of arguments are provided, the 
fallthrough case will return `nothing`.

### Arguments

* `case` : a Bool passed to `ifelse`
* `x` : the value returned when `pred` is `true`
* `rest...` : the first argument will be the value returned when `pred` is 
    false. If two or more additional arguments are provided they can be 
    considered as `case`, `x` and `rest...` for the next `case_when` clause.

### Returns

* The value of x if `case` is `true` (for more arguments, the first argument 
    following the first `true` value passed to a positionally odd argument. 
    If not satisfied, and an odd number of arguments were provided, then the 
    last value provided. Otherwise, when an even number of arguments were 
    provided, `nothing` is returned.)

### Examples

```jldoctest
julia> using DataFramesMeta

julia> case_when(false, 1, false, 2, true, 3, false, 4)

julia> case_when.(
    [false, false, false, true ], 1,
    [false, true,  false, false], 2,
    true, 3)
```
"""
function case_when(cases...)
	if length(cases) > 0
		ifelse.(cases[1].first, cases[1].second, case_when(cases[2:end]...))
	else
		missing
	end
end



cww_pred(v, pred::Function) = pred(v)
cww_pred(v, pred::Type) = (<:).(typeof(v), pred)
cww_pred(v, pred::Bool) = pred
cww_pred(v, pred::AbstractArray{Bool}) = pred
cww_pred(v, pred::Union{Array,Tuple}) = v in pred
cww_pred(v, pred::Regex) = match.(pred, v) .!== nothing
cww_pred(v, pred) = v == pred

"""
    case_when_with(v, pred, x, rest...)

An analog to `case_when` where each case clause is interpreted as a predicate 
upon the initial value, `v`

### Arguments

* `v` : A value to evaluate `pred` upon for determining cases
* `pred` : A predicate value, which is attempted to be smartly interpretted to
    handle most case-style predicate criteria. This includes:
        * `pred::Function` : `pred` is evaluated upon `v`
        * `pred::Bool` : `pred` is used as case directly
        * `pred::Union{Array,Tuple}` : evaluate whether `v` is in `pred`
        * `pred::Type` : evaluate whether `v` is a subtype of `pred`
        * `pred::Any` : interpretted for equivalence of `v` against `pred`
* `x` : value if resolved `pred` is `true`
* `rest...` : recycled as consecutive `pred` and `x`. If any argument remains, 
    it is used as a fallthrough case.

### Examples

```jldoctest
julia> using DataFramesMeta

julia> case_when_with('a', ==('b') => 1, true => 2)

julia> case_when_with('a', 'a' => 1, true => 2)

julia> case_when_with(1:10,
	(x -> x .< 3) =>  1,  # handle lambdas
    1:10 .< 5     =>  2,  # handle boolean arrays
    [(3, 6, 7)]   =>  3,  # handle iterables with "in". Wrapped for broadcasting.
    9             =>  4,  # handle single values
    Int64         =>  5,  # handle DataTypes 
    true          =>  6)  # optional fallthrough case, return `nothing` otherwise
```
"""
function case_when_with(v, cases::Pair...)
	if length(cases) > 0
		ifelse.(
			cww_pred.(v, cases[1].first), 
			cases[1].second, case_when_with(v, cases[2:end]...))
	else
		missing
	end
end

