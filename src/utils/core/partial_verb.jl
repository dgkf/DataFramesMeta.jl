function partial_verb(verb::Function, args...; kwargs...)
	x -> begin
		if typeof(x)<:Function
			y -> verb(x(y), args...; kwargs...)
		else
			verb(x, args...; kwargs...)
		end
	end
end

