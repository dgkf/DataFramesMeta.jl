using DataFrames

df = DataFrame(x = 1:26, y = 'a':'z', z = repeat([true, false], 13))
gdf = groupby(df, :z)
rdf = eachrow(df)

@testset "aggregate api testing" begin
    @testset "keyword aggregations" begin
        # aggregating with keywords only produces single aggregate values 
        # in columns with keyword names
        x = df |> 
            @aggregate(a = maximum(:x)) |> 
            (x -> x[1,:a])
        @test x == maximum(df[!,:x])

        # works for multiple keyword arguments
        x = df |> 
            @aggregate(a = maximum(:x), b = minimum(:y)) |> 
            (x -> x[1,:a])
        @test x == maximum(df[!,:x]) 
    end

    @testset "aggregate with only positional args defaults to 'all' predicate" begin
        # aggregate with positional function arguments automatically applies
        # to all columns and produces a single row dataframe of aggregates
        x = df[!,[:x,:y]] |>
            @aggregate(maximum, minimum) |>
            names    
        @test x == [:x_maximum, :x_minimum, :y_maximum, :y_minimum]
        
        # keyword argument names are used as aggregate names
        x = df[!,[:x,:y]] |>
            @aggregate(maximum, test = x -> first(:x)) |>
            names
        @test x == [:x_maximum, :x_test, :y_maximum, :y_test]
    end

    @testset "aggregate with key symbol produces long form dataset" begin
        # aggregate with key produces long form dataset, assumes 'all' 
        # predicate, preserves columns
        x = df |>
            @aggregate(:test, maximum) |>
            names    
        @test  x == [:test, :x, :y, :z]

        # aggregate with a key produces long form datset with 
        # one record per aggregation
        x = df |>
            @aggregate(:test, maximum, minimum) |>
            nrow
        @test x == 2

        # aggregate with a key and a named argument produces an 
        # aggregation of that name
        x = df |>
            @aggregate(:test, maximum, minimum, test = first) |>
            (x -> x[!,:test])
        @test x == [:maximum, :minimum, :test]

        # aggregate producing a long form datset throws an error when 
        # duplicate column names are created
        @test_throws ArgumentError df |> @aggregate(:x, maximum, minimum)
        
        # error no longer exists when _makeunique is set to true
        x = df |> 
            @aggregate(:x, maximum, minimum, _makeunique = true) |>
            typeof
        @test x<:AbstractDataFrame
    end

    @testset "aggregate with predicate pairs" begin
        # aggregate with predicate pairs (predicate => function) produces flat
        # aggregate DataFrame
        x = df |> 
            @aggregate(all() => maximum) |>
            names
        @test x == [:x_maximum, :y_maximum, :z_maximum]

        # aggregate with predicate pairs (predicate => function) filters
        # missing values from columns
        x = df |> 
            @aggregate(all() => maximum, Number => minimum) |>
            names
        @test x == [:x_maximum, :x_minimum, :y_maximum, :z_maximum, :z_minimum]
    end

    @testset "aggregate with key and predicate pairs produces long form DataFrame" begin
        # aggregate with predicate pairs (predicate => function) produces flat
        # aggregate DataFrame
        x = df |> 
            @aggregate(:test, all() => maximum) |>
            names
        @test x == [:test, :x, :y, :z]

        # aggregate with predicate pairs (predicate => function) filters
        # missing values from columns
        x = df |> 
            @aggregate(:test, all() => maximum) |>
            nrow
        @test x == 1

        # works with multiple predicate pairs
        x = df |> 
            @aggregate(:test, all() => maximum, Number => minimum) |>
            nrow
        @test x == 2

        # works with NamedTuple of function in pairs
        x = df |> 
            @aggregate(:test, all() => maximum, Number => (test = minimum,)) |>
            (x -> x[!,:test])
        @test x == [:maximum, :test]
    end
end
