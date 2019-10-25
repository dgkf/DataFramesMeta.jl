doc_family_where() = ["where", "any", "all"]


function doc_see_also(f, fs) 
    @assert(all(typeof(@doc eval(fi)) <: typeof(@doc "") for fi=fs), 
        "all fs must have valid documentation")

    """**See Also**: $(join(
        "[`" .* setdiff(fs, [f]) .* "`](@ref)", 
        ", "))"""
end


doc_verb_arg_data() = replace_single_newlines("""
Any `DataFrame`-like object (including any `AbstractDataFrame`,
`DataFrameRows` or `GroupedDataFrame`), or a Function. If a function is
provided, the two are composed producing a function awaiting a single
argument.
""")


doc_verb_arg_predicate() = replace_single_newlines("""
A `Pair` beginning with the symbol `:at` (or `at` when using the
corresponding macro). Predicates are used for selecting a subset of columns
to apply aggregations to, and can be one of many acceptable predicates Types
interetable by [`column_selector`](@ref). When using a predicate argument, it
must be provided as a pair (`:at => <predicate>`, or `at => <predicate>` when
using a verb's accompanying macro).
""")


doc_verb_macro() = replace_single_newlines("""
All verbs have macro analogs that provide slightly more convenient syntax.
Although predominately functionally similar, the macro equivalent functions
have three major differences.

1. The corresponding macro function always returns a function, allowing for 
    better compatibility with the pipe and compose operators. 

1. Symbols passed as part of an expression in most arguments are interpretted
    as the column values within the `DataFrame` passed to the function.

1. Predicates, which are typically passed with a pair starting with an `:at` 
    can instead be passed using a naked `at`. This is primarily for syntactic
    convenience, but also helps to disambiguate predicates from symbols
    referring to columns.
""")
