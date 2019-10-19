using DataFrames

df = DataFrame(x = 1:26, y = 'a':'z', z = repeat([true, false], 13))

@testset "transform_at api testing" begin
    @testset "single unnamed transform" begin
        @test transform(df, :at => Number, x -> x) == df
        @test df |> @transform(at => Number, x -> x) == df
        @test transform(df, :at => Number, x -> x .* 2 ./ 2) == df
        @test df |> @transform(at => Number, x -> x .* :x ./ :x) == df
    end

    @testset "only named transforms" begin
        @test df |> @transform(x = :x .* 2 ./ 2) == df
        @test df |> @transform(a = :x .* 2 ./ 2) |> (x -> x[!,:a]) == df[!,:x]
        @test df |> @transform(a = :x .* 2 ./ 2, b = :x .* 1) |> (x -> x[!,:a]) == df[!,:x]
        @test df |> @transform(a = :x .* 2 ./ 2, b = :x .* 7) |> (x -> x[!,:b]) == df[!,:x] .* 7
    end
end
