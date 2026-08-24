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
encode_query_open_request <- function(sql, correlation_id = NULL, serialization_mode = NULL) {
  con <- rawConnection(raw(), open = "r+")
  on.exit(close(con))
  write_nanoarrow(
    data.frame(
      sql = sql,
      batch_rows = NA_character_,
      batch_bytes = NA_character_,
      correlation_id = if (is.null(correlation_id)) NA_character_ else correlation_id,
      serialization_mode = if (is.null(serialization_mode)) NA_character_ else serialization_mode,
      stringsAsFactors = FALSE
    ),
    con
  )
  encode_call("query_open", rawConnectionValue(con))
}
encode_session_control <- function(method, session_id, session_token, correlation_id = NULL) {
  suffix <- if (is.null(correlation_id)) "" else paste0(',"correlation_id":"', correlation_id, '"')
  json <- paste0(
    '{"session_id":', format(session_id, scientific = FALSE, trim = TRUE),
    ',"session_token":"', session_token, '"', suffix, "}"
  )
  encode_call(method, charToRaw(json))
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

json_get_string <- function(json, key) {
  m <- regexec(sprintf('"%s":"([^"]*)"', key), json)
  parts <- regmatches(json, m)[[1]]
  if (length(parts) < 2L) NA_character_ else parts[2]
}
json_get_number <- function(json, key) {
  m <- regexec(sprintf('"%s":([0-9]+)', key), json)
  parts <- regmatches(json, m)[[1]]
  if (length(parts) < 2L) NA_real_ else as.numeric(parts[2])
}

dial_req_when_ready <- function(url, timeout_seconds = 10) {
  deadline <- Sys.time() + timeout_seconds
  repeat {
    socket <- tryCatch(
      nanonext::socket("req", dial = url, autostart = NA),
      error = function(e) NULL
    )
    if (!is.null(socket)) return(socket)
    if (Sys.time() >= deadline) {
      stop("ducknng R smoke server did not become ready within ",
           timeout_seconds, " seconds")
    }
    Sys.sleep(0.05)
  }
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
  DBI::dbGetQuery(con, "SELECT ducknng_register_exec_method()")
  Sys.sleep(4)
  rows <- tryCatch(DBI::dbGetQuery(con, "SELECT * FROM smoke_table ORDER BY x"), error = function(e) data.frame())
  DBI::dbGetQuery(con, "SELECT ducknng_stop_server('smoke')")
  DBI::dbDisconnect(con, shutdown = TRUE)
  rows
})

req <- dial_req_when_ready(ipc_url)

stopifnot(nanonext::send(req, encode_manifest_request(), mode = "raw", block = 1000L) == 0)
manifest_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
manifest_text <- rawToChar(manifest_reply$payload)
stopifnot(manifest_reply$version == 1L)
stopifnot(manifest_reply$type == 2L)
stopifnot(grepl('"version":"0.1.2.9000"', manifest_text, fixed = TRUE))
stopifnot(grepl('"name":"exec"', manifest_text, fixed = TRUE))
stopifnot(grepl('"name":"manifest"', manifest_text, fixed = TRUE))
stopifnot(grepl('"name":"handshake"', manifest_text, fixed = TRUE))
stopifnot(grepl('"supported_serialization_modes":["arrow_ipc_stream","ducknng_quack_batch"]', manifest_text, fixed = TRUE))
stopifnot(grepl('"fetch_metadata":{"correlation_id":true,"result_handle":true,"batch_index":true}', manifest_text, fixed = TRUE))
stopifnot(grepl('"parameter_binding":{"encoding":"arrow_struct","positional":true,"methods":["exec","query_open","query_prepare"],"max_parameters":65535}', manifest_text, fixed = TRUE))

handshake_json <- paste0(
  '{"min_protocol_version":1,"max_protocol_version":1,',
  '"preferred_serialization_mode":"arrow_ipc_stream",',
  '"correlation_id":"hs-1"}'
)
stopifnot(nanonext::send(req, encode_call("handshake", charToRaw(handshake_json)), mode = "raw", block = 1000L) == 0)
handshake_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
handshake_text <- rawToChar(handshake_reply$payload)
stopifnot(handshake_reply$type == 2L)
stopifnot(handshake_reply$name == "handshake")
stopifnot(grepl('"server_version":"0.1.2.9000"', handshake_text, fixed = TRUE))
stopifnot(grepl('"selected_serialization_mode":"arrow_ipc_stream"', handshake_text, fixed = TRUE))
stopifnot(grepl('"supported_serialization_modes":["arrow_ipc_stream","ducknng_quack_batch"]', handshake_text, fixed = TRUE))
stopifnot(grepl('"fetch_metadata":{"correlation_id":true,"result_handle":true,"batch_index":true}', handshake_text, fixed = TRUE))
stopifnot(grepl('"parameter_binding":{"encoding":"arrow_struct","positional":true,"methods":["exec","query_open","query_prepare"],"max_parameters":65535}', handshake_text, fixed = TRUE))
stopifnot(grepl('"ducknng_protocol_version":1', handshake_text, fixed = TRUE))
stopifnot(grepl('"row_schema_version":1', handshake_text, fixed = TRUE))
stopifnot(grepl('"default_fetch_batch_chunks":12', handshake_text, fixed = TRUE))
stopifnot(grepl('"correlation_id":"hs-1"', handshake_text, fixed = TRUE))

unsupported_handshake_json <- paste0(
  '{"min_protocol_version":1,"max_protocol_version":1,',
  '"preferred_serialization_mode":"not_a_ducknng_serializer"}'
)
stopifnot(nanonext::send(req, encode_call("handshake", charToRaw(unsupported_handshake_json)), mode = "raw", block = 1000L) == 0)
unsupported_handshake_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
stopifnot(unsupported_handshake_reply$type == 3L)
stopifnot(identical(unsupported_handshake_reply$error, "ducknng: unsupported preferred_serialization_mode"))

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

open_frame <- encode_query_open_request(
  "SELECT 42 AS x UNION ALL SELECT 84 AS x ORDER BY x",
  correlation_id = "open-1"
)
stopifnot(nanonext::send(req, open_frame, mode = "raw", block = 1000L) == 0)
open_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
open_text <- rawToChar(open_reply$payload)
stopifnot(grepl('"correlation_id":"open-1"', open_text, fixed = TRUE))
stopifnot(grepl('"serialization_mode":"arrow_ipc_stream"', open_text, fixed = TRUE))
stopifnot(grepl('"ducknng_protocol_version":1', open_text, fixed = TRUE))
stopifnot(grepl('"row_schema_version":1', open_text, fixed = TRUE))
stopifnot(grepl('"fetch_batch_chunks":12', open_text, fixed = TRUE))
session_id <- json_get_number(open_text, "session_id")
session_token <- json_get_string(open_text, "session_token")
result_handle <- json_get_string(open_text, "result_handle")
stopifnot(!is.na(session_id), !is.na(session_token), !is.na(result_handle))
stopifnot(nchar(result_handle) == 32L)

stopifnot(nanonext::send(req, encode_session_control("fetch", session_id, session_token, "fetch-1"), mode = "raw", block = 1000L) == 0)
fetch_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
stopifnot(fetch_reply$type == 2L)
stopifnot(fetch_reply$name == "fetch")
stopifnot(bitwAnd(fetch_reply$flags, 1L) != 0L)
stopifnot(bitwAnd(fetch_reply$flags, 8L) != 0L)
stopifnot(bitwAnd(fetch_reply$flags, 256L) == 0L)
fetch_df <- as.data.frame(read_nanoarrow(fetch_reply$payload))
stopifnot(identical(fetch_df$x, c(42L, 84L)))

stopifnot(nanonext::send(req, encode_session_control("fetch", session_id, session_token, "fetch-2"), mode = "raw", block = 1000L) == 0)
fetch_eos_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
fetch_eos_text <- rawToChar(fetch_eos_reply$payload)
stopifnot(bitwAnd(fetch_eos_reply$flags, 16L) != 0L)
stopifnot(grepl('"state":"exhausted"', fetch_eos_text, fixed = TRUE))
stopifnot(grepl('"correlation_id":"fetch-2"', fetch_eos_text, fixed = TRUE))
stopifnot(grepl('"batch_index":1', fetch_eos_text, fixed = TRUE))
stopifnot(grepl(paste0('"result_handle":"', result_handle, '"'), fetch_eos_text, fixed = TRUE))

stopifnot(nanonext::send(req, encode_session_control("close", session_id, session_token, "close-1"), mode = "raw", block = 1000L) == 0)
close_reply <- decode_frame(nanonext::recv(req, mode = "raw", block = 1000L))
close_text <- rawToChar(close_reply$payload)
stopifnot(grepl('"state":"closed"', close_text, fixed = TRUE))
stopifnot(grepl('"correlation_id":"close-1"', close_text, fixed = TRUE))

client_drv <- duckdb::duckdb(config = list(allow_unsigned_extensions = "true"))
client_con <- DBI::dbConnect(client_drv, dbdir = ":memory:")
invisible(DBI::dbExecute(client_con, sprintf("LOAD '%s'", ext_path)))
quack_rows <- DBI::dbGetQuery(
  client_con,
  sprintf(
    "SELECT * FROM ducknng_query_rpc_mode('%s', 'SELECT x, x > 1 AS gt_one FROM smoke_table ORDER BY x', 0::UBIGINT, 'ducknng_quack_batch')",
    ipc_url
  )
)
stopifnot(identical(quack_rows$x, c(1L, 2L)))
stopifnot(identical(quack_rows$gt_one, c(FALSE, TRUE)))
for (mode in c("arrow_ipc_stream", "ducknng_quack_batch")) {
  bulk <- DBI::dbGetQuery(
    client_con,
    sprintf(
      "SELECT count(*)::INTEGER AS n, min(x)::INTEGER AS mn, max(x)::INTEGER AS mx FROM ducknng_query_rpc_mode('%s', 'SELECT i::INTEGER AS x FROM range(50000) AS t(i)', 0::UBIGINT, '%s')",
      ipc_url,
      mode
    )
  )
  stopifnot(identical(bulk$n, 50000L))
  stopifnot(identical(bulk$mn, 0L))
  stopifnot(identical(bulk$mx, 49999L))
}
DBI::dbDisconnect(client_con, shutdown = TRUE)

close(req)
rows <- parallel::mccollect(server_job)[[1]]
stopifnot(identical(rows$x, c(1L, 2L)))
cat("ducknng rpc smoke: OK\n")
