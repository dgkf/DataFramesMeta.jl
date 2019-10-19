using Test
using DataFramesMeta

@testset "DataFramesMeta" begin
    dir = "tests"
    include.(dir .* "/" .* readdir(dir))
end
