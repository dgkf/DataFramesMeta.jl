# DataFramesMeta API Experiments

## Installation

```julia
] add https://github.com/dgkf/SymbolContexts.jl
] add https://github.com/dgkf/DataFramesMeta.jl#dev/symbol_contexts
```

## Motivation

`DataFramesMeta` provides a wonderful interface for data manipulation that
adopts a data transformation syntax to a pipeable syntactic shorthand for
columnwise manipulations. 

There are a few key areas where experiments have led to potentially productive
new features or structural changes in the package.

1. Accommodate native `|>` syntax by always returning a data-manipulating 
   unary function from any transformation macro. 
1. Breaking out the handling of symbols in expressions into a new package,
   `SymbolContexts.jl`. Lifting this code out of `DataFramesMeta.jl` allows for
   the core functions to be more easily extended to apply to things like graph
   data, tree structures or data in alternative tabular formats, while still
   using the same syntax for symbols as representing named elements of data.
1. Extending column selections using a repurposing of the `cols()` function.
   This work overlaps heavily with
   [`Selections.jl`](https://github.com/Drvi/Selections.jl), specifying a number
   of different `DataTypes` that can be appropriately assumed to represent a
   mechanism of selecting columns. 
1. An attempt at implementing a syntax for column-selection predicated data
   manipulations, including predicate `transform`, `aggregate` and `where`
   clauses, though a predicated `groupby`, `orderby` and `select` would be
   equally possible to extend through a similar syntax. 

## Experiments Overview

Below are the major structural changes to the package. At this point, after 
quite some time iterating around these core ideas, the package is almost 
unidentifiable with many breaking changes, rebranding of functions for entirely
different purposes, and breaking out code into new packages.

This began largely as a learning exercise, and in the process ended with some 
experiments that might benefit the JuliaData community. This work is framed 
entirely as experimentation because of how far it deviates from the original
package and because this work plays loosely with the core design. Some ideas
feel complete and others leave a lot of room for improvement. 

### 1. Accommodating Native `|>` and `∘` Operators

`DataFramesMeta` relies on the `@linq` macro or `Lazy.jl` threading macro,
`@>`, to support a pipeable syntax. The `@linq` macro in particular felt quite
limited because of how each `DataFramesMeta` function requires a `linq` method
in order to be appropriately interpreted. This barrier to extension felt
unnecessary. To a lesser extent, the `@>` macro, though more transparently
extensible, does require that all manipulations exist in a single expression
that can be transformed by the macro, often requiring encapsulating in a `begin
... end` block. 

To minimize the overhead for a pipeable syntax, all core verb macros (e.g.
`@transform`) return a unary function. The concept maps well to what a domain
specific language like a data manipulation syntax intends to achieve. These
core verbs themselves do not describe a single mapping from an input to output
dataset. Only once parameterized do they fully articulate a function that is to
be applied, and only then will they return a unary function ready to manipulate
data.

From a user's perspective, this change is motivated by one key tradeoff,
assuming that a pipeable syntax and the composition of transformations is more
valuable than single transformations. The merits of this reprioritization
certainly warrant further consideration.

```julia
# prioritized use cases:
transform(df, a = data -> data[!,:b])
df |> @transform(a = :b)

# will throw an error:
@transform(df, a = :b)
```

### 2. Expression Symbol Interpretation moved to `SymbolContexts.jl`

In comparison to R's `dplyr` package, `DataFramesMeta.jl` takes the approach of
using `Symbol`s to represent column names. This is a much appreciated stylistic
change as it largely disambiguates how expressions are interpreted and minimizes
conflicts with object names in the parent scope. The R ecosystem has adopted the
non-standard evaluation principles advanced in `dplyr` with varying degrees of
robustness of implementation. In the worst cases, code which attempts to mimic
the popularized `dplyr` syntax disregards many of `dplyr`'s considerations for
things like scoped variables, nested expressions or alternative modes of column
specification.

All of this is just to say that having easy to reuse code that allows a domain
specific language to propegate is extremely valuable and encourages extension
without duplication of effort.

In the context of `DataFramesMeta`, this also would allow for expressions to 
be interpretted to operate on non-`DataFrame` data structures with the same 
verbs. For example, to modify a graph datastructure, one might imagine a 
syntax that looks like this:

```julia
using MetaGraphs

MetaGraph() |> 
    # initialize some vertices and edges ...  |> 
    @transform(at => edges, weight = :src.mass * :dst.mass) |>
    @where(at => vertices, :mass > 100) |> 
    density
```

`SymbolContexts.jl` exposes only a single function that need to be implemented
for a new `DataType` to extend its symbolic syntax such that these expressions
can be evaluated with the new `DataType`, namely `sym()` and optionally
`syms_in_context()`.

For within `DataFramesMeta`, four methods were added to extend the
`SymbolicContexts.jl` package:

```julia
sym(d::AnyDataFrame, s) = d[!,s]
sym(g::GroupedDataFrame, s) = g.parent[!,s]
sym(r::DataFrameRow, s) = r[s]
syms_in_context(x::AbstractDataFrame, ss) = in(names(x)).(ss)
```

When an expression is intended to be reinterpretted such that the `Symbol`s are
evaluated within the context of an object, the expression only needs to be 
preprocessed by the `@syms` macro:

```julia
df |> @transform(a = :x)

# is functionally equivalent to 
transform(df, a = @syms :x)
```

### 3. Consistent Column Predicates

With the hope of accommodating a wealth of intuitive mechanisms of describing
column selections, the `cols()` function has been redefined as a generic way of
producing a `Array{Bool,1}` mask of selected columns (behind a thin
`ColumnMask` `DataType` for easier dispatch). This common denominator of
column-wise selection is used ubuitously throughout `DataFramesMeta.jl`,
allowing for the same mechanisms of column selection to be reused as a common
paradigm throughout the package. 

This function is used directly by `@select` to interpret a number of column 
selection specifications, but is widely used throughout `DataFramesMeta` as a 
common mechanism of predicating data manipulations.

Using `cols()`, columns can be specified by

- `Array{Bool,1}` (length equal to the number of columns): selects columns
  where for each index that is `true`
- `Bool`: selects all columns if `true` or subtracts all columns if `false`
- `Integer`: adds column at integer index to selection
- `Symbol`: adds column with symbol name to selection
- `String`: adds column with string name to selection
- `Regex`: adds columns matching the regular expression to selection
- `Function`: applies function to the data, the result can be any of the types
  that can be interpretted by `cols()`, but most often is an `Array{Bool,1}`
- `DataType`: adds columns with matching `DataType` to selection
- `UnitRange`, `Tuple`, `Array`: adds elements to range based on
  `cols(<element>)` result

In addition, a number of base functions and operators have been extended to 
produce `Function`s when partially evaluated such that they can be easily used
with `cols()` selections. 

- `startswith`, `endswith`, `occursin`: all extended to return
  `cols()`-compatible functions when called with a single string argument,
  similar to the suggested curriable implementations suggested in
  [`JuliaLang/julia #33193`](https://github.com/JuliaLang/julia/issues/33193)
- `(:)(a::Symbol, b::Symbol)`: defined such that a function is generated,
  creating an `Integer` `UnitRange` between the index of the first symbol and
  the index of the second symbol in the `DataFrame` names.
- `-`: Subtraction is extended to produce `cols()`-compatible functions when
  called with a single string or symbol. Subtracting a function negates the
  result of the function, allowing extensive composition with other column
  selection functions.
- `all` and `-all`: Uniquely handled such that all columns can be easily added
  and removed from the column selection.

With all of these conveniences in place, we can select columns in a flexible and
intuitive manner. 

```julia
df |> @select(-all, :c : :e, Number, r"(d|e|f)", -:e, [13, 14], -endswith("z")) 
```

Like `dplyr`, if the first selection is additive, the implied initial selection
is all columns; whereas if the first selection is subtractive, the implied
initial selection is no columns. For example, with a `DataFrame` with column
names `[:a, :b, :c]`, `select(-:a)` will return a `DataFrame` with columns
`[:b, :c]`, whereas `select(:a)` will return a `DataFrame` with only column
`:a`. This addresses the open issue [`JuliaData/DataFramesMeta.jl
#124`](https://github.com/JuliaData/DataFramesMeta.jl/issues/124). 

### 4. Predicated Columnwise Function Application

One of the biggest gaps in `DataFramesMeta.jl` is the absence of convenient
syntax for applying functions over many columns in a single operation. The same
column selections used by `@select` are also used to predicate inputs to all of
the data manipulation macros. These predicated or subsetted calls have analogs
in `dplyr`. For example, `dplyr`'s `mutate` has analog function calls,
`mutate_at`, `mutate_if`, and `mutate_all`. 

For all `DataFramesMeta` data manipulation macros, these modes of subsetting
affected columns is done through an `at => ` pair, which is used by the
corresponding macro to disambiguate between expressions that may otherwise be
transforming expressions.

>This syntax feels quite non-idiomatic and there's strong rationale to break
>out these predicated functions as their own distinct functions with `_at`,
>`_if` or `_all` suffixes.  
>
>The initial motivation for using the `at => ` predicate was to take advantage
>of multiple dispatch to try to devise an intuitive way of expressing a
>multitude of heavily related operations. However, differentiating when an
>expression should be interpretted as a column of data or as a name of a column
>for selection becomes quite challenging. For example, without the `at => `
>predicate, a call such as `@transform(cols(:a), x -> x .* 2)` would require
>special handling of the `cols()` function call to avoid transforming `:a` into
>a `SymbolicContext`. 
>
>Alternatives to the `at => ` pair syntax might include
>`@transform_at(Number, x -> x .* 2)`
>`@transform(@at(Number), x -> x .* 2)`
>`@transform(:at(Number), x -> x .* 2)`
>`@at(Number, @transform(x -> x .* 2))`

For example, to apply a transformation function to all columns, simply
providing a function to `transform` will default to applying it to all columns:

```julia
df |> @transform(x -> x .* 2)
```

However, to be explicit, you can also specify all columns by passing the
predicate `at => all` as the first argument.

```julia
# analog to dplyr's `mutate_all`
df |> @transform(at => all, x -> x .* 2)
```

This call to `@transform` is functionally equivalent to the macro-less
```
transform(df, cols(all), x -> x .* 2)
```

Any valid column selection can be used, allowing for analogs to `dplyr`'s 
`mutate_at` and `mutate_if` as well. 

```julia
# analog to dplyr's `mutate_at`
df |> @transform(at => (:x, :y), x -> x .* 2)
```

```julia
# analog to dplyr's `mutate_if`
# using a method defined for `mapcols` which awaits an object
df |> @transform(at => mapcols(x -> maximum(x) > 3), x -> x .* 2)
```

>Here `mapcols` is used to create a partially applied function which will
>accept the anticipated `DataFrame` to compute which columns get selected. A
>parallel to `at => ` would be `if => `, which could implicitly wrap functions
>in `mapcols`. Unfortunately, a `if` in this context raises a syntax error,
>preventing its use, and escalates concerns about the syntax itself.

This syntactic sugar is used currently by `@transform`, `@where` and
`@aggregate`, and could be extended to be used as well by `@orderby`,
`@groupby` and `@select`.

#### Operations with Multiple Predicates

Two functions, `@aggregate` and `@where`, need to be handled uniquely as they
are heavily destructive operations, meaning that they cannot be applied
multiple times with different predicates.  As a counterexample, `transpose` can
be applied multiple times in sequence to different predicates, resulting in
sequential transformations.

```julia
df |> 
    @transform(at => Number, x -> x .* 2) |>
    @transform(at => String, x -> x .* "end")
```

Because `@where` and `@aggregate` remove records, some mechanism of applying
multiple predicates to different portions of their opertion is needed. These
use cases are quite fringe but necessary to fully describe complicated
filtering and aggregation steps.

#### Predicated `@where`

The predicated form of `@where` becomes involved because it also necessitates
that logic for collapsing across the predicated columns be described such that a
single filtering mask is produced.

To make that more concrete, if `@where` is called with a predicate:

```julia
df |> @where(at => Number, x -> x .> 2)
```

it is ambiguous whether rows are to be kept if *all* values are greater than 2,
or if *any* values are greater than 2. By default, this will select records
where *all* columns meet the criteria, but a mechanism of expressing the
alternative is needed. Taking inspiration from `dplyr`'s `filter_at` symantics,
the `all_vars` and `any_vars` functions are used to define this logic. 

```R
mtcars %>% filter_if(is.numeric, any_vars(. > 300))
```

Given julia's richer dispatch mechanisms, we can instead define methods for
`all` and `any` which accept an arbitrary number of functions and require that
the condition is met for *all* or *any* of the cells in a record, allowing us
to define quite involved filtering criteria with fairly natural symantics. 

```julia
# select all rows where a value in any Number column is >10 or <5
df |> @where(at => Number, any(x -> x .> 10, x -> x .< 5))
```

Notably, these criteria can be nested to define arbitrarily complicated
selection criteria. In practice, convoluted criteria such as these are probably
best first derived as a column, but that shouldn't impose a restriction on 
how logic can be defined.

```julia
# select all rows where any value in a Number column is >10 and all Number
# column values are >2
df |> @where(at => Number, all(x -> x .> 2, any(x -> x .> 10)))

# multiple arguments are evaluated as `all`, this can also be expressed as
df |> @where(at => Number, x -> x .> 2, any(x -> x .> 10))
```

Not all logic will necessarily share the same predicate, `all` and `any` also
accept **predicated pairs**, where the first value represents a predicate column
selection and the second value is the function that is to be applied to those
columns. 

```julia
# select all rows where any String column value starts with "a" and all Number
# column values are >2
df |> @where(all(Number => x -> x .> 2, any(String => x -> startswith(x, "a"))))
```

These symantics are flexible, compose to be able to represent quite complicated
filtering criteria, and are founded on the same column selection behaviors
itemized above.

#### Predicated `@aggregate`

The symantic notation of pairing a predicate column selection criteria with an
applied function, as used in the last `@where` example is reused for
aggregation. In addition to predicating aggregations using the `at => `
predicate, **predicate pairs** can be used to define functions which are used
for unique sets of column selections. 

This syntax is quite involved and is intended to be a niche, but
critical, use case where aggregating multiple times across incompatible pairs
of predicate criteria and selections would necessitate that multiple copies
of a `DataFrame` are created and aggregated to achieve a similar result. 

```
julia> df |> @aggregate_long(  
    all()  => (col_type = typeof, cell_type = eltype),
    Number => (maximum, minimum),
    Char   => (n_unique = length ∘ unique,))
5×4 DataFrame
│ Row │ aggregate │ x              │ y              │ z             │
│     │ Symbol    │ Any            │ Any            │ Any           │
├─────┼───────────┼────────────────┼────────────────┼───────────────┤
│ 1   │ col_type  │ Array{Int64,1} │ Array{Int64,1} │ Array{Char,1} │
│ 2   │ cell_type │ Int64          │ Int64          │ Char          │
│ 3   │ maximum   │ 4              │ 2              │ missing       │
│ 4   │ minimum   │ 1              │ 1              │ missing       │
│ 5   │ n_unique  │ missing        │ missing        │ 4             │
```

### 5. `@aggregate_long` and `@aggregate_wide`

By default, predicated `@aggregate` will behave similarly to `dplyr`'s
`summarize_at`, creating a column for each affected column, for each
aggregating function. For example, if called such as

```julia
df |> @aggregate(maximum, minimum)

# or equivalently...

df |> @aggregate(at => all, maximum, minimum)
```

We will get two columns for each column in the input `DataFrame`. 

```
1×6 DataFrame
│ Row │ x_maximum │ x_minimum │ y_maximum │ y_minimum │ z_maximum │ z_minimum │
│     │ Int64     │ Int64     │ Int64     │ Int64     │ Char      │ Char      │
├─────┼───────────┼───────────┼───────────┼───────────┼───────────┼───────────┤
│ 1   │ 4         │ 1         │ 2         │ 1         │ 'd'       │ 'a'       │
```

However, encoding information in column names is generally considered poor 
data practice. To more responsibly handle this data, alternative aggregate
macros are provided to produce a long- or wide-form dataset. 

```
julia> df |> @aggregate_long(at => all, maximum, minimum)
2×4 DataFrame
│ Row │ agg     │ x     │ y     │ z    │
│     │ Symbol  │ Int64 │ Int64 │ Char │
├─────┼─────────┼───────┼───────┼──────┤
│ 1   │ maximum │ 4     │ 2     │ 'd'  │
│ 2   │ minimum │ 1     │ 1     │ 'a'  │
```

or

```
julia> df |> @aggregate_wide(at => all, maximum, minimum)
3×3 DataFrame
│ Row │ column  │ maximum │ minimum │
│     │ Symbol  │ Any     │ Any     │
├─────┼─────────┼─────────┼─────────┤
│ 1   │ x       │ 4       │ 1       │
│ 2   │ y       │ 2       │ 1       │
│ 3   │ z       │ 'd'     │ 'a'     │
```

In both cases, a `Symbol` can be passed immediately after the `at => `
predicate to rename the `:agg` or `:column` default column names. 

## Closing

A lot of syntactic and functional space has been explored in an effort to
improve the consistency and flexibility of data minpulation in julia. A lot of
ideas have panned out to varying degrees of success. I hope that my
experimentation can offer a foundation for further discussion of the features
and syntax that was added.  

