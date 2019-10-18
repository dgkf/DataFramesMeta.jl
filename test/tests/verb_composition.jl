using DataFrames

df = DataFrame(x = 1:4, y = repeat([1, 2], 2))
gdf = groupby(df, :y)

# some generic functions for testing composition
add2(f::Function) = data -> add2(f(data))
add2(d::AbstractDataFrame) = d .+ 2
add2(d::GroupedDataFrame) = groupby(add2(d.parent), groupvars(d))
mul2(f::Function) = data -> mul2(f(data))
mul2(d::AbstractDataFrame) = d .* 2
mul2(d::GroupedDataFrame) = groupby(mul2(d.parent), groupvars(d))

# a generic test for composability
composes(f::Function, data) = 
    (data |> add2 |> f |> mul2) == 
    (add2 |> f |> mul2)(data) == 
    (mul2 ∘ f ∘ add2)(data)

# generic test for implicit curry-ability when data isn't provided
currys(f::Function, data) = all([
    (add2 |> f |> mul2) isa Function, 
    (mul2 ∘ f ∘ add2) isa Function, 
    !((add2 |> f |> mul2)(df) isa Function)])

function composes_and_currys(verb, df)
    @testset "w/ `$(typeof(df))`" begin
        @testset "type stability" begin
            @test typeof(verb)<:Function
            @test typeof(mul2 |> verb)<:Function
            @test typeof(verb(df))<:typeof(df)
        end
        @testset "composes" begin
            @test composes(verb, df)
        end
        @testset "currys" begin
            @test currys(verb, df)
        end
    end
end

@testset "verb composition" begin
    @testset "select" begin
        composes_and_currys(select2(1), df)
        composes_and_currys(@select2(1), df)
        composes_and_currys(select2(2), gdf)
        composes_and_currys(@select2(2), gdf)
    end

    @testset "transform" begin
        composes_and_currys(transform(a = 1), df)
        composes_and_currys(@transform(a = :x .* 2), df)
        composes_and_currys(transform(a = 1), gdf)
        composes_and_currys(@transform(a = :x .* 2), gdf)
    end

    @testset "transform with predicate" begin
        composes_and_currys(transform(:at => :x, x -> x .+ 1, a = 1), df)
        composes_and_currys(@transform(at => :x, a = :x .* 2), df)
        composes_and_currys(transform(:at => :x, x -> x .+ 1, a = 1), gdf)
        composes_and_currys(@transform(at => :x, x -> x .+ 1, a = :x .* 2), gdf)
    end
end
