using Test
using DataFramesMeta

@testset "DataFramesMeta" begin
    dir = "tests"
	include.(filter(x -> endswith(x, ".jl"), joinpath.(dir, readdir(dir))))
end
