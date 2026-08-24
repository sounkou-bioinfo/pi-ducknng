#' piducknng: DuckNNG-backed runtime composition for Pi
#'
#' The package composes DuckDB, ducknng, nanonext, nanoarrow, and mirai.
#' DuckDB owns native extension loading and host-language calls; ducknng and
#' NNG own RPC and transport; nanonext owns the R socket; nanoarrow owns Arrow
#' IPC conversion; mirai owns R process scheduling.
#' The package does not currently export an R runtime API.
#'
#' @keywords internal
"_PACKAGE"
