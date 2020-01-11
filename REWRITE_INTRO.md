# DataFramesMeta Rewrite

## Motivation

`DataFramesMeta` has been rewritten from the ground up in an attempt to bring it
closer to feature compatibility with similar modern data manipulation packages.
Most notably, this work was inspired by R's `dplyr`, arguably the most 
sophisticated and widely used domain-specific language for data manipulation.

The syntax added was chosen as a balance between cognitive burden of the 
`DataFramesMeta` function footprint and versatility of the core data manipulation
verbs.

## Feature Overview

### 1. Data Manipulations as Function Factories

All manipulation macros (e.g. `@transform`, etc) return a unary function,
allowing for native compatibility with the julia `|>` operator. 

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
DataFramesMeta with additional data manipulations is more intuitive. More
importantly, each function can be individually precompiled and reused.

### 2. Flexible column selection

With the hope of accommodating a wealth of intuitive mechanisms of describing
column selections, the `cols()` function has been redefined as a generic way
of producing a `Array{Bool}` mask of selected columns. This common denominator
of column-wise selection is used ubuitously throughout `DataFramesMeta`, allowing
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

### 3. Predicated Manipulations

These same column selections are also used to predicate inputs to all of the
data manipulation macros. These predicated or subsetted calls have analogs
in `dplyr`. For example, `dplyr`'s `mutate` has analog function calls, 
`mutate_at`, `mutate_if`, and `mutate_all`. 

For all `DataFramesMeta` data manipulation macros, these modes of subsetting
affected columns is done through an `at => ` pair. 

For example, to apply a transformation function to all columns, 

```julia
# analog to dplyr's `mutate_all`
df |> @transform(at => all, x -> x .* 2)
```

Any valid column selection can be used, allowing for analogs to `dplyr`'s 
`mutate_at` and `mutate_if` as well. 

```julia
# analog to dplyr's `mutate_at`
df |> @transform(at => (:x, :y), x -> x .* 2)
```

```julia
# analog to dplyr's `mutate_if`
df |> @transform(at => col -> maximum(col) > 3, x -> .* 2)
```

This `at => ` syntax is used to differentiate the predicate argument from 
other keyworded arguments, as is recognized by the `DataFramesMeta` macro 
handler and converted to a call to `cols()`. 

This syntactic sugar is used currently by `@transform`, `@where` and
`@aggregate`, and could be extended to be used as well by `@select`, `@orderby`
and `@groupby` although there were not considered prioirties for this behavior.

### 4. `@where` predicated column collapsing

The predicated form of `@where` becomes involved because it also necessitates
that logic for collapsing across the predicated columns be described such that a
single filtering array is produced.

To make that more concrete, if `@where` is called with a predicate:

```julia
df |> @where(at => Number, x -> x .> 2)
```

it is ambiguous whether rows are to be kept if all or any values are greater
than 2. Taking inspiration from `dplyr`'s `filter_at` symantics, the `all_vars`
and `any_vars` functions are used to define this logic. 

```R
mtcars %>% filter_if(is.numeric, any_vars(. > 300))
```

Given julia's more rigorous dispatch, we can instead define methods for `all`
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
df |> @where(at => Number, all(x -> x .> 2, any(x -> x .> 10)) 
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

### 5. `@aggregate_long` and `@aggregate_wide`

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

### 6. multiply predicated `@aggregate`

In addition to predicating aggregations using the `at => ` predicate, predicate
pairs can be used to define functions which are used for unique sets of column
selections.

```
julia> df |> @aggregate_long(  
    all()  => (col_type = typeof, cell_type = eltype,),
    Number => (maximum, minimum),
    Char   => (n_unique = x -> reduce(*, x),))  
```

