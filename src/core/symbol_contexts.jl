import SymbolContexts.SymbolContext, SymbolContexts.sym, 
       SymbolContexts.syms_in_context

# reexports from SymbolContexts
export SymbolContext, sym

sym(d::AnyDataFrame, s) = d[!,s]
sym(g::GroupedDataFrame, s) = g.parent[!,s]
sym(r::DataFrameRow, s) = r[s]
syms_in_context(x::AbstractDataFrame, ss) = in(names(x)).(ss)

