"""
    gen(Function)

Generate a new function which will first be partially evaluated with all but the
first argument, producing a new function expecting only the first argument.

This function is used internally by all macro versions of core verbs to create
pipeable functions. 
"""
function gen(f::Function)
    function(args...; kwargs...) 
        x -> f(x, args...; kwargs...) 
    end
end

