export with, @with

function with(args...; kwargs...)
    partial_verb(with, args...; kwargs...)
end

function with(d::AnyDataFrame, f::Function)
	f(d)
end

function with_macro_helper(args...)
	args = verb_arg_handler(args, key=false)
	:($with($(args...)))
end

macro with(args...)
	esc(with_macro_helper(args...))
end

