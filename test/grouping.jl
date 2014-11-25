

module TestGrouping

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta

d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x)

@test  @where(d, :x .== 3) == where(d, x -> x[:x] .== 3)
@test  DataFrame(@where(g, length(:x) > 5)) == DataFrame(where(g, x -> length(x[:x]) > 5))
@test  DataFrame(@where(g, length(:x) > 5))[:n][1:3] == @data [5, 6, 7]

@test  DataFrame(orderby(g, x -> mean(x[:n]))) == DataFrame(@orderby(g, mean(:n)))
#@test  DataFrames.based_on(@orderby(g, mean(:n)), x -> x[1,:x])[:x1] == [3,1,2]

@test  (@transform(g, y = :n - median(:n)))[1,:y] == -5.0


end # module
