using DataFrames

df = DataFrame(x = 1:26, y = 'a':'z', z = repeat([true, false], 13))

@testset "DataFrame column selectors" begin
    selector_output_type = Array{T,1} where T<:Integer

    @testset "Selector dispatches to appropriate type handlers" begin
        @testset "Symbol column names" begin
            @test typeof(DataFramesMeta.cols(df, :x))<:selector_output_type
            @test DataFramesMeta.cols(df, :x) == [1, 0, 0]
            @test DataFramesMeta.cols(df, :a) == [0, 0, 0]
        end

        @testset "Array{Symbol} column names" begin
            @test typeof(DataFramesMeta.cols(df, [:x, :y]))<:selector_output_type
            @test DataFramesMeta.cols(df, [:x, :y]) == [1, 1, 0]
        end

        @testset "String column names" begin
            @test typeof(DataFramesMeta.cols(df, "x"))<:selector_output_type
            @test DataFramesMeta.cols(df, "x") == [1, 0, 0]
            @test DataFramesMeta.cols(df, "z") == [0, 0, 1]
        end

        @testset "Array{String} column names" begin
            @test typeof(DataFramesMeta.cols(df, ["x", "y"]))<:selector_output_type
            @test DataFramesMeta.cols(df, ["x", "z"]) == [1, 0, 1]
            @test DataFramesMeta.cols(df, ["z", "y"]) == [0, 1, 1]
        end

        @testset "Subtractive Symbol column names" begin
            @test typeof(DataFramesMeta.cols(df, -:x))<:selector_output_type
            @test DataFramesMeta.cols(df, -:x) == [0, 1, 1]
            @test DataFramesMeta.cols(df, -:a) == [1, 1, 1]
        end

        @testset "Bool column selection" begin
            @test typeof(DataFramesMeta.cols(df, true))<:selector_output_type
            @test DataFramesMeta.cols(df, true) == [1, 1, 1]
            @test DataFramesMeta.cols(df, false) == [0, 0, 0]
        end

        @testset "Array{Bool} column selection" begin
            @test typeof(DataFramesMeta.cols(df, [true, true, true]))<:selector_output_type
            @test DataFramesMeta.cols(df, [true, false, false]) == [1, 0, 0]
            @test_throws AssertionError DataFramesMeta.cols(df, [true, true, true, true])
        end

        @testset "Column selection by index" begin
            @test typeof(DataFramesMeta.cols(df, 1))<:selector_output_type
            @test_throws AssertionError DataFramesMeta.cols(df, [1, -2])
            @test_throws AssertionError DataFramesMeta.cols(df, [1, 0])
            @test DataFramesMeta.cols(df, 1) == [1, 0, 0]
            @test DataFramesMeta.cols(df, 2) == [0, 1, 0]
            @test DataFramesMeta.cols(df, 5) == [0, 0, 0]
            @test DataFramesMeta.cols(df, -2) == [1, 0, 1]
            @test DataFramesMeta.cols(df, -5) == [1, 1, 1]
        end

        @testset "Selecting by column name range" begin
            @test typeof(DataFramesMeta.cols(df, :x : :z))<:selector_output_type
            @test DataFramesMeta.cols(df, :x : :z) == [1, 1, 1]
            @test DataFramesMeta.cols(df, :y : :z) == [0, 1, 1]
            @test DataFramesMeta.cols(df, :y : :y) == [0, 1, 0]
        end

        @testset "Array{Any} column selection" begin
            @test typeof(DataFramesMeta.cols(df, []))<:selector_output_type
            @test typeof(DataFramesMeta.cols(df, [:x, 3]))<:selector_output_type
            @test DataFramesMeta.cols(df, []) == [0, 0, 0]
            @test DataFramesMeta.cols(df, [:x, 3]) == [1, 0, 1]
        end

        @testset "Tuple column selection" begin
            @test typeof(DataFramesMeta.cols(df, ()))<:selector_output_type
            @test typeof(DataFramesMeta.cols(df, (:x, 3)))<:selector_output_type
            @test DataFramesMeta.cols(df, ()) == [0, 0, 0]
            @test DataFramesMeta.cols(df, (:x, 3)) == [1, 0, 1]
        end

        @testset "DataType column selection" begin
            @test typeof(DataFramesMeta.cols(df, Number))<:selector_output_type
            @test DataFramesMeta.cols(df, Number) == [1, 0, 1]
            @test DataFramesMeta.cols(df, Int64) == [1, 0, 0]
            @test DataFramesMeta.cols(df, Bool) == [0, 0, 1]
        end

        @testset "Regex column selection" begin
            @test typeof(DataFramesMeta.cols(df, r"[a-z]"))<:selector_output_type
            @test DataFramesMeta.cols(df, r"[xz]") == [1, 0, 1]
        end

        @testset "Function column selection with DataFrame predicate" begin
            @test typeof(DataFramesMeta.cols(df, x -> 1))<:selector_output_type
            @test DataFramesMeta.cols(df, df::AbstractDataFrame -> names(df)) == [1, 1, 1]
            @test DataFramesMeta.cols(df, df::AbstractDataFrame -> eltype.(eachcol(df))) == [1, 1, 1]
        end

        @testset "Function that returns valid selector" begin
            @test typeof(DataFramesMeta.cols(df, names))<:selector_output_type
            @test DataFramesMeta.cols(df, names) == [1, 1, 1]
            @test DataFramesMeta.cols(df, df -> Integer) == [1, 0, 1]
        end
    end

    @testset "Extension of expected selector generics" begin
        @testset "startswith" begin
            @test typeof(DataFramesMeta.cols(df, startswith("x")))<:selector_output_type
            @test DataFramesMeta.cols(df, startswith("y")) == [0, 1, 0]
        end

        @testset "endswith" begin
            @test typeof(DataFramesMeta.cols(df, endswith("x")))<:selector_output_type
            @test DataFramesMeta.cols(df, endswith("y")) == [0, 1, 0]
        end

        @testset "occursin" begin
            @test typeof(DataFramesMeta.cols(df, occursin("x")))<:selector_output_type
            @test DataFramesMeta.cols(df, occursin("y")) == [0, 1, 0]
        end

        @testset "all" begin
            @test typeof(DataFramesMeta.cols(df, all()))<:selector_output_type
            @test DataFramesMeta.cols(df, all()) == [1, 1, 1]
            @test DataFramesMeta.cols(df, -all()) == [0, 0, 0]
        end
    end

    @testset "Selectors function consolidates selector results" begin
        @test typeof(DataFramesMeta.cols(df,))<:selector_output_type
        @test DataFramesMeta.cols(df, true, -:x, -2) == [0, 0, 1]
        @test DataFramesMeta.cols(df, -:x, Bool, "y") == [0, 1, 1]
    end
end
