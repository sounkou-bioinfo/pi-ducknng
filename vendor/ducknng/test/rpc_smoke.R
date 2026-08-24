suppressWarnings(suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(nanonext)
  library(nanoarrow)
}))

u32le <- function(x) writeBin(as.integer(x), raw(), size = 4L, endian = "little")
u64le <- function(x) {
  x <- as.double(x)
  c(u32le(x %% 2^32), u32le(floor(x / 2^32)))
}
read_u32le <- function(buf, offset) sum(as.double(as.integer(buf[offset + 0:3])) * 256^(0:3))
read_u64le <- function(buf, offset) read_u32le(buf, offset) + 2^32 * read_u32le(buf, offset + 4)
encode_call <- function(name, payload = raw(), flags = 0L) {
  name_raw <- charToRaw(name)
  c(as.raw(1L), as.raw(1L), u32le(flags), u32le(length(name_raw)), u32le(0), u64le(length(payload)), name_raw, payload)
}
encode_manifest_request <- function() {
  c(as.raw(1L), as.raw(0L), u32le(0), u32le(0), u32le(0), u64le(0))
}
encode_exec_request <- function(sql, want_result = FALSE) {
  con <- rawConnection(raw(), open = "r+")
  on.exit(close(con))
  write_nanoarrow(data.frame(sql = sql, want_result = want_result), con)
  encode_call("exec", rawConnectionValue(con))
}
decode_frame <- function(buf) {
  name_len <- read_u32le(buf, 7)
  error_len <- read_u32le(buf, 11)
  payload_len <- read_u64le(buf, 15)
  name_start <- 23L
  error_start <- name_start + name_len
  payload_start <- error_start + error_len
  payload_end <- payload_start + payload_len - 1L
  list(
    version = as.integer(buf[1]),
    type = as.integer(buf[2]),
    flags = read_u32le(buf, 3),
    name = if (name_len > 0) rawToChar(buf[name_start:(error_start - 1L)]) else "",
    error = if (error_len > 0) rawToChar(buf[error_start:(payload_start - 1L)]) else "",
    payload = if (payload_len > 0) buf[payload_start:payload_end] else raw()
  )
}

ext_path <- normalizePath("build/release/ducknng.duckdb_extension")
ipc_path <- tempfile(pattern = "ducknng_rpc_smoke_", tmpdir = "/tmp", fileext = ".ipc")
ipc_url <- paste0("ipc://", ipc_path)

server_job <- parallel::mcparallel({
  drv <- duckdb::duckdb(config = list(allow_unsigned_extensions = "true"))
  con <- DBI::dbConnect(drv, dbdir = ":memory:")
  DBI::dbExecute(con, sprintf("LOAD '%s'", ext_path))
  DBI::dbGetQuery(con, sprintf(
    "SELECT ducknng_start_server('smoke', '%s', 1, 134217728, 300000, 0)",
    ipc_url
  ))
  Sys.sleep(4)
  rows <- tryCatch(DBI::dbGetQuery(con, "SELECT * FROM smoke_table ORDER BY x"), error = function(e) data.frame())
  DBI::dbGetQuery(con, "SELECT ducknng_stop_server('smoke')")
  DBI::dbDisconnect(con, shutdown = TRUE)
  rows
})

Sys.sleep(1)
req <- nanonext::socket("req", dial = ipc_url, autostart = NA)

stopifnot(nanonext::send(req, encode_manifest_request(), mode = "raw", block = 1000L) == 0)
manifest_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
stopifnot(manifest_reply$version == 1L)
stopifnot(manifest_reply$type == 2L)
stopifnot(grepl('"name":"exec"', rawToChar(manifest_reply$payload), fixed = TRUE))
stopifnot(grepl('"name":"manifest"', rawToChar(manifest_reply$payload), fixed = TRUE))

stopifnot(nanonext::send(req, encode_exec_request("CREATE TABLE smoke_table(x INTEGER)", FALSE), mode = "raw", block = 1000L) == 0)
create_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
create_df <- as.data.frame(read_nanoarrow(create_reply$payload))
stopifnot(create_df$statement_type[[1]] == 7L)

stopifnot(nanonext::send(req, encode_exec_request("INSERT INTO smoke_table VALUES (1), (2)", FALSE), mode = "raw", block = 1000L) == 0)
insert_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
insert_df <- as.data.frame(read_nanoarrow(insert_reply$payload))
stopifnot(insert_df$rows_changed[[1]] == 2)

stopifnot(nanonext::send(req, encode_exec_request("SELECT x, x > 1 AS gt_one FROM smoke_table ORDER BY x", TRUE), mode = "raw", block = 1000L) == 0)
select_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
stopifnot(select_reply$type == 2L)
select_df <- as.data.frame(read_nanoarrow(select_reply$payload))
stopifnot(identical(select_df$x, c(1L, 2L)))
stopifnot(identical(select_df$gt_one, c(FALSE, TRUE)))

close(req)
rows <- parallel::mccollect(server_job)[[1]]
stopifnot(identical(rows$x, c(1L, 2L)))
cat("ducknng rpc smoke: OK\n")
