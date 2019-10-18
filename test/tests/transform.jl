using DataFrames

df = DataFrame(x = 1:26, y = 'a':'z', z = repeat([true, false], 13))

@testset "transform_at api testing" begin
    @testset "single unnamed transform" begin
        @test transform(df, :at => Number, x -> x) == df
        @test @transform(df, at => Number, x -> x) == df
        @test transform(df, :at => Number, x -> x .* 2 ./ 2) == df
        @test @transform(df, at => Number, x -> x .* :x ./ :x) == df
    end

    @testset "only named transforms" begin
        @test @transform(df, x = :x .* 2 ./ 2) == df
        @test @transform(df, a = :x .* 2 ./ 2)[!,:a] == df[!,:x]
        @test @transform(df, a = :x .* 2 ./ 2, b = :x .* 1)[!,:a] == df[!,:x]
        @test @transform(df, a = :x .* 2 ./ 2, b = :x .* 7)[!,:b] == df[!,:x] .* 7
    end
end
