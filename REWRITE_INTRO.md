# DataFramesMeta Rewrite

## Motivation

`DataFramesMeta` has been rewritten from the ground up in an attempt to bring it
closer to feature compatibility with similar modern data manipulation packages.
Most notably, this work was inspired by R's `dplyr`, arguably the most 
sophisticated and widely used domain-specific language for data manipulation.

The syntax added was chosen as a balance between cognitive burden of the 
`DataFramesMeta` function footprint and versatility of the core data manipulation
verbs.

## Design Goals and Open Considerations

A number of design decisions have been explored. Naturally, there are arguments
for or against all of them and each API decision comes at a tradeoff among
multiple goals. All critique and input is welcome!

- Consistency and ubiquity
  - `Symbols` as references to columns, using `SymbolContexts.jl`
    >(almost) all macros transform expressions with `Symbol`s to a
    >`SymbolContext` object, a specialized `Function` that handles evaluation
    >of `Symbol`s within the context of an arbitrary data object.
    >
    >```julia
    ># macro calls with symbols ...
    >df |> @transform(z = :x .+ :y)
    >
    ># ... are functionally equivalent to
    >transform(df, z = @syms :x .+ :y)
    >```

  - Column selections handled via `cols()` calls, generating a `ColumnMask`
    object. 
    >Any macros with column selection are handled by transforming expressions
    >to calls to `cols()`, which interprets a number of input `Type`s as
    >selectors, returning a `ColumnMask`
    > 
    >```julia
    ># macro calls with a column selection predicate such as 
    >df |> @transform(at => Number, x -> x .* 2)
    >
    ># are functionally equivalent to
    >transform(df, cols(Number), x -> x .* 2)
    >```

    >**Note**
    >The `at => ` syntax was chosen as a way of using the method dispatch
    >system to differentiate between predicated and non-predicated verb usage.
    >This is certainly a design that warrants scrutiny and could just as easily
    >use the `cols()` calls directly or be broken out as separate, more 
    >type-stable functions such as `transform_at` and `transform_if`.
    
- Native composability with `|>` and `∘` by returning unary functions 
- Flexibility of function inputs to "do the right thing". Most notably, this
  means that named arguments generally create a column of that name, and
  unnamed arguments are general manipulations applied to all columns.
- 

## Features

### 1. Data Manipulations as Function Factories

All manipulation macros (`@transform`, etc) return a unary function, allowing
for native compatibility with the julia `|>` operator. 

```julia
df |> @transform(x = :y)
```

Conceptually, this maps much closer to what is described by each data step.
Each manipulation macro describes a family of possible data manipulation. Taking
for example the `@transform` macro, `transform` does not describe a single 
data manipulation, but rather a family of manipulation that perform column-wise
data transformations. 

A call to the `@transform` macro is to specify a concrete transformation
function, describing what should be done when a `DataFrame` is eventually 
passed to it. 

This reframing of these data steps is fundamental to the rewrite, and has a
number of benefits in julia. First, as each function takes only a single
argument, the expected `DataFrame`, the functions can now be natively piped
with `|>`. Since there's no longer reliance on the `@linq` macro, extending
DataFramesMeta with additional data manipulations is more intuitive. In typical
julia fashion, each function can be individually precompiled and reused.


### 2. API Consistency

One of the core goals of the rewrite was to make the most frequently used syntax 
of data manipulation intuitive, and the complicated edge cases possible. Already
`DataFramesMeta` does a very good job of being transparent about its processing
and balancing that against functionality. Specifically with the consistency and
versatility of `Symbols` to refer to column names, `DataFramesMeta` has set a
strong precedent for how to manipulate data.

To extend this intuition, a number of additional features have been generalized
across the entire package, allowing for use of common paradigms with consistent
function call formatting.

- Consistent column selection spanning a wide range of possible input data
  types and usages. Most directly, this includes use in `select()`, but is used 
  widely to predicate transformations. 
- To improve the consistency and ubiquity in which symbols are handled
  throughout the package, a new package, `SymbolContexts.jl`, was broken out to
  allow symbolic expressions to be easily evaluated in the context of new
  `DataTypes`.

#### 2.1. Flexible Column Selection

With the hope of accommodating a wealth of intuitive mechanisms of describing
column selections, the `cols()` function has been redefined as a generic way of
producing a `Array{Bool,1}` mask of selected columns (behind a thin
`ColumnMask` `DataType` for easier dispatch). This common denominator of
column-wise selection is used ubuitously throughout `DataFramesMeta`, allowing
for the same mechanisms of column selection to be reused as a common paradigm
throughout the package. 

This function is used directly by `@select` to interpret a number of column 
selection specifications, but is widely used throughout `DataFramesMeta` as a 
common mechanism of predicating data manipulations (more below).

Using `cols()`, columns can be specified by

- `Array{Bool,1}` (length equal to the number of columns): selects columns
  where `true`
- `Bool`: selects all columns if `true` or subtracts all columns if `false`
- `Integer`: adds column at integer index to selection
- `Symbol`: adds column with symbol name to selection
- `String`: adds column with string name to selection
- `Regex`: adds columns matching the regular expression to selection
- `Function`: applies function to each column of data, adding columns to
  selection of result is `true`. 
- `DataType`: adds columns with matching `DataType` to selection
- `UnitRange`, `Tuple`, `Array`: adds elements to range based on
  `cols(<element>)` result

In addition, a number of base functions and operators have been extended to 
produce `Function`s when partially evaluated such that they can be easily used
with `cols()` selections. 

- `startswith`, `endswith`, `occursin`: all extended to return
  `cols()`-compatible functions when called with a single string argument.
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
intuitive manner:

```julia
df |> @select(-all, :c : :e, Number, r"(d|e|f)", -:e, [13, 14], -endswith("z")) 
```

#### 2.1.1 General Predicated Manipulations

These same column selections are also used to predicate inputs to all of the
data manipulation macros. These predicated or subsetted calls have analogs
in `dplyr`. For example, `dplyr`'s `mutate` has analog function calls, 
`mutate_at`, `mutate_if`, and `mutate_all`. 

For all `DataFramesMeta` data manipulation macros, these modes of subsetting
affected columns is done through an `at => ` pair, which is used by the
corresponding macro to disambiguate between expressions that may otherwise be
transforming expressions.

For example, to apply a transformation function to all columns, 

```julia
# analog to dplyr's `mutate_all`
df |> @transform(at => all, x -> x .* 2)
```

> This call to `@transform` is functionally equivalent to the macro-less
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
df |> @transform(at => mapcols(x -> maximum(x) > 3), x -> x .* 2)
```

>Here `mapcols` is used to create a partially applied function which will accept
the anticipated `DataFrame` to compute which columns get selected. It's open for
debate whether `if => ` predicates should also be added to accommodate 
column-wise predicate functions, which would simply convert expressions to 
calls to `mapcols` automatically.  

This `at => ` syntax is used to differentiate the predicate argument from 
other keyworded arguments, as is recognized by the `DataFramesMeta` macro 
handler and converted to a call to `cols()`. 

This syntactic sugar is used currently by `@transform`, `@where` and
`@aggregate`, and could be extended to be used as well by `@select`, `@orderby`
and `@groupby` although there were not considered prioirties for this behavior.

#### 2.1.2 `@where` Predicated Column Collapsing

The predicated form of `@where` becomes involved because it also necessitates
that logic for collapsing across the predicated columns be described such that a
single filtering array is produced.

To make that more concrete, if `@where` is called with a predicate:

```julia
df |> @where(at => Number, x -> x .> 2)
```

it is ambiguous whether rows are to be kept if *all* values are greater than 2,
or if *any* values are greater than 2. Taking inspiration from `dplyr`'s
`filter_at` symantics, the `all_vars` and `any_vars` functions are used to
define this logic. 

```R
mtcars %>% filter_if(is.numeric, any_vars(. > 300))
```

Given julia's richer dispatch mechanisms, we can instead define methods for `all`
and `any` which accept an arbitrary number of functions and require that the
condition is met for *all* or *any* of the records, allowing us to define
quite involved selection criteria with fairly natural symantics. 

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
```

Because not all logic will necessarily share the same predicate, `all` and
`any` also accept predicated pairs, where the first value represents a predicate
column selection and the second value is the function that is to be applied 
to those columns. 

```julia
# select all rows where any String column value starts with "a" and all Number
# column values are >2
df |> @where(all(Number => x -> x .> 2, any(String => x -> startswith(x, "a"))))
```

These symantics are flexible and compose to be able to represent quite
complicated filtering criteria, and are founded on the same column selection
behaviors itemized above. 

#### 3.1.3 `@aggregate` With Multiple Predicates

The symantic notation of pairing a predicate column selection criteria with an
applied function, as used in the last `@where` example is reused for
aggregation. In addition to predicating aggregations using the `at => `
predicate, predicate pairs can be used to define functions which are used for
unique sets of column selections. 

This syntax is quite involved and is intended to be a niche, but
critical, use case where aggregating multiple times across incompatible pairs
of predicate criteria and selections would necessitate that multiple copies
of a `DataFrame` are created and aggregated to achieve a similar result. 

```
julia> df |> @aggregate_long(  
    all()  => (col_type = typeof, cell_type = eltype,),
    Number => (maximum, minimum),
    Char   => (n_unique = x -> reduce(*, x),))  
5×4 DataFrame
│ Row │ aggregate │ x              │ y              │ z             │
│     │ Symbol    │ Any            │ Any            │ Any           │
├─────┼───────────┼────────────────┼────────────────┼───────────────┤
│ 1   │ col_type  │ Array{Int64,1} │ Array{Int64,1} │ Array{Char,1} │
│ 2   │ cell_type │ Int64          │ Int64          │ Char          │
│ 3   │ maximum   │ 4              │ 2              │ missing       │
│ 4   │ minimum   │ 1              │ 1              │ missing       │
│ 5   │ n_unique  │ missing        │ missing        │ abcd          │
```

#### 3.1.3 `@aggregate_long` and `@aggregate_wide`

My default, predicated `@aggregate` will behave similarly to `dplyr`'s
`summarize_at`, creating a column for each affected column, for each
aggregating function. For example, if called such as

```julia
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



