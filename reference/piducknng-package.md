# piducknng

Thin R and Pi integration package composing DuckDB, ducknng, nanonext,
nanoarrow, and mirai. DuckDB owns native extension loading and host
calls; ducknng and NNG own RPC and transport; nanonext owns the R
socket; nanoarrow owns Arrow IPC conversion; and mirai owns R process
scheduling. The package does not currently export an R runtime API.
