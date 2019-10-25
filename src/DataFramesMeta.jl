module DataFramesMeta

    using DataFrames, Tables

    function include_dir(dir)
        src_path = dirname(pathof(@__MODULE__))
		for (path, dirs, files) in walkdir(joinpath(src_path, dir))
			include.(joinpath.(path, filter(x -> endswith(x, ".jl"), files)))
		end
    end

    include_dir("utils")
    include_dir("verbs")
    include_dir("accessories")

end
