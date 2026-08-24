suppressWarnings(suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
}))

sql_quote <- function(x) {
  paste0("'", gsub("'", "''", x, fixed = TRUE), "'")
}

parse_mode <- function(args) {
  if (length(args) == 0L) {
    return(list(mode = "micro", args = args))
  }
  if (args[[1]] %in% c("micro", "direct", "bulk_compare", "bulk-compare", "compare")) {
    mode <- switch(args[[1]],
      "bulk-compare" = "bulk_compare",
      "compare" = "bulk_compare",
      args[[1]]
    )
    return(list(mode = mode, args = args[-1]))
  }
  list(mode = "micro", args = args)
}

open_duckdb <- function(dbdir = ":memory:", allow_unsigned_extensions = FALSE) {
  config <- if (allow_unsigned_extensions) {
    list(allow_unsigned_extensions = "true")
  } else {
    list()
  }
  DBI::dbConnect(duckdb::duckdb(config = config), dbdir = dbdir)
}

run_micro_bench <- function(args) {
  suppressWarnings(suppressPackageStartupMessages({
    library(nanonext)
    library(nanoarrow)
  }))

  iterations <- if (length(args) >= 1L) as.integer(args[[1]]) else 100L
  clients <- if (length(args) >= 2L) as.integer(args[[2]]) else 4L
  stopifnot(iterations > 0L, clients > 0L)

  u32le <- function(x) writeBin(as.integer(x), raw(), size = 4L, endian = "little")
  u64le <- function(x) {
    x <- as.double(x)
    c(u32le(x %% 2^32), u32le(floor(x / 2^32)))
  }
  read_u32le <- function(buf, offset) sum(as.double(as.integer(buf[offset + 0:3])) * 256^(0:3))
  read_u64le <- function(buf, offset) read_u32le(buf, offset) + 2^32 * read_u32le(buf, offset + 4)
  encode_call <- function(name, payload = raw(), flags = 0L) {
    name_raw <- charToRaw(name)
    c(as.raw(1L), as.raw(1L), u32le(flags), u32le(length(name_raw)), u32le(0),
      u64le(length(payload)), name_raw, payload)
  }
  encode_manifest_request <- function() {
    c(as.raw(1L), as.raw(0L), u32le(0), u32le(0), u32le(0), u64le(0))
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
  rpc_roundtrip <- function(sock, frame) {
    stopifnot(nanonext::send(sock, frame, mode = "raw", block = 5000L) == 0)
    decode_frame(nanonext::recv(sock, mode = "raw", block = 5000L))
  }
  bench_case <- function(name, action, iterations) {
    elapsed <- system.time({
      for (i in seq_len(iterations)) action()
    })[["elapsed"]]
    data.frame(
      benchmark = name,
      iterations = iterations,
      clients = 1L,
      total_ms = as.integer(round(elapsed * 1000)),
      per_iter_ms = round((elapsed * 1000) / iterations, 3),
      stringsAsFactors = FALSE
    )
  }
  bench_parallel_sessions <- function(url, iterations, clients) {
    elapsed <- system.time({
      cl <- parallel::makeCluster(clients, type = "PSOCK")
      on.exit(parallel::stopCluster(cl), add = TRUE)
      parallel::clusterEvalQ(cl, suppressWarnings(suppressPackageStartupMessages({
        library(nanonext)
        library(nanoarrow)
      })))
      parallel::clusterExport(cl, c(
        "url", "iterations",
        "u32le", "u64le", "read_u32le", "read_u64le",
        "encode_call", "encode_query_open_request", "encode_session_control",
        "decode_frame", "json_get_string", "json_get_number", "rpc_roundtrip"
      ), envir = environment())
      ok <- parallel::parLapply(cl, seq_len(clients), function(worker_id) {
        sock <- nanonext::socket("req", dial = url, autostart = NA)
        on.exit(close(sock), add = TRUE)
        for (i in seq_len(iterations)) {
          open <- rpc_roundtrip(sock, encode_query_open_request(
            sprintf("SELECT %d AS worker_id, %d AS iter", worker_id, i),
            correlation_id = sprintf("open-%d-%d", worker_id, i)
          ))
          payload_text <- rawToChar(open$payload)
          session_id <- json_get_number(payload_text, "session_id")
          session_token <- json_get_string(payload_text, "session_token")
          fetch <- rpc_roundtrip(sock, encode_session_control(
            "fetch", session_id, session_token,
            sprintf("fetch-%d-%d", worker_id, i)
          ))
          if (fetch$type != 2L) stop("fetch failed")
          invisible(rpc_roundtrip(sock, encode_session_control(
            "close", session_id, session_token,
            sprintf("close-%d-%d", worker_id, i)
          )))
        }
        TRUE
      })
      stopifnot(all(vapply(ok, isTRUE, logical(1))))
    })[["elapsed"]]
    data.frame(
      benchmark = "parallel_sessions_arrow",
      iterations = iterations,
      clients = clients,
      total_ms = as.integer(round(elapsed * 1000)),
      per_iter_ms = round((elapsed * 1000) / (iterations * clients), 3),
      stringsAsFactors = FALSE
    )
  }

  ext_path <- normalizePath(Sys.getenv(
    "DUCKNNG_BENCH_EXT_PATH",
    unset = "build/release/ducknng.duckdb_extension"
  ))
  ipc_path <- tempfile(pattern = "ducknng_bench_", tmpdir = "/tmp", fileext = ".ipc")
  ipc_url <- paste0("ipc://", ipc_path)

  drv <- duckdb::duckdb(config = list(allow_unsigned_extensions = "true"))
  con <- DBI::dbConnect(drv, dbdir = ":memory:")
  sock <- NULL
  on.exit({
    if (!is.null(sock)) {
      try(close(sock), silent = TRUE)
    }
    try(DBI::dbGetQuery(con, "SELECT ducknng_stop_server('bench')"), silent = TRUE)
    try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  }, add = TRUE)
  DBI::dbExecute(con, sprintf("LOAD '%s'", ext_path))
  DBI::dbGetQuery(con, sprintf(
    "SELECT ducknng_start_server('bench', '%s', 1, 134217728, 300000, 0)",
    ipc_url
  ))
  Sys.sleep(1)

  sock <- nanonext::socket("req", dial = ipc_url, autostart = NA)

  results <- list()
  results[[length(results) + 1L]] <- bench_case(
    "manifest_roundtrip",
    function() rpc_roundtrip(sock, encode_manifest_request()),
    iterations
  )
  results[[length(results) + 1L]] <- bench_case(
    "session_roundtrip_arrow",
    function() {
      open <- rpc_roundtrip(sock, encode_query_open_request("SELECT 1 AS x"))
      payload_text <- rawToChar(open$payload)
      session_id <- json_get_number(payload_text, "session_id")
      session_token <- json_get_string(payload_text, "session_token")
      rpc_roundtrip(sock, encode_session_control("fetch", session_id, session_token, "fetch-arrow"))
      rpc_roundtrip(sock, encode_session_control("close", session_id, session_token, "close-arrow"))
      invisible(NULL)
    },
    iterations
  )
  results[[length(results) + 1L]] <- bench_case(
    "session_roundtrip_quack",
    function() {
      open <- rpc_roundtrip(sock, encode_query_open_request(
        "SELECT 1 AS x",
        serialization_mode = "ducknng_quack_batch"
      ))
      payload_text <- rawToChar(open$payload)
      session_id <- json_get_number(payload_text, "session_id")
      session_token <- json_get_string(payload_text, "session_token")
      fetch <- rpc_roundtrip(sock, encode_session_control(
        "fetch", session_id, session_token, "fetch-quack"
      ))
      stopifnot(fetch$type == 2L, bitwAnd(as.integer(fetch$flags), 256L) == 256L)
      rpc_roundtrip(sock, encode_session_control("close", session_id, session_token, "close-quack"))
      invisible(NULL)
    },
    iterations
  )
  results[[length(results) + 1L]] <- bench_parallel_sessions(
    ipc_url,
    max(1L, iterations %/% max(1L, clients)),
    clients
  )

  print(do.call(rbind, results), row.names = FALSE)
}

run_direct_bench <- function(args) {
  source("bench/rpc_bulk_compare_support.R", local = TRUE)
  repetitions <- if (length(args) >= 1L) as.integer(args[[1]]) else 5L
  rows <- if (length(args) >= 2L) {
    as.integer(strsplit(args[[2]], ",", fixed = TRUE)[[1]])
  } else {
    c(100000L, 1000000L)
  }
  db_path <- if (length(args) >= 3L) {
    args[[3]]
  } else {
    file.path(tempdir(), "ducknng_quack_direct.duckdb")
  }
  transport <- Sys.getenv("DUCKNNG_BENCH_TRANSPORT", unset = "ipc")
  ext_path <- ducknng_bench_find_ext_path()
  required_sf <- max(1L, as.integer(ceiling(max(rows) / 6000000)))
  dataset_name <- sprintf("tpch_sf%d.lineitem", required_sf)
  ducknng_bench_ensure_tpch_db(db_path, required_sf, max(rows))
  baseline_con <- ducknng_bench_open_duckdb(
    db_path,
    allow_unsigned_extensions = TRUE
  )
  on.exit(ducknng_bench_safe_disconnect(baseline_con), add = TRUE)
  ducknng_bench_set_single_thread(baseline_con)
  baselines <- setNames(
    lapply(rows, function(n) ducknng_bench_local_baseline(baseline_con, n)),
    as.character(rows)
  )
  result <- ducknng_bench_run_ducknng_rpc_transport(
    transport = transport,
    rows = rows,
    repetitions = repetitions,
    baselines = baselines,
    db_path = db_path,
    ext_path = ext_path,
    dataset_name = dataset_name,
    concurrent_rows = 0L,
    concurrent_iterations = 0L,
    concurrent_clients = 0L
  )
  print(ducknng_bench_machine_details(ext_path), row.names = FALSE)
  print(result$rpc, row.names = FALSE)
}

run_bulk_compare <- function(args) {
  source("bench/rpc_bulk_compare_support.R", local = TRUE)
  repetitions <- if (length(args) >= 1L) as.integer(args[[1]]) else 5L
  rows <- if (length(args) >= 2L) {
    as.integer(strsplit(args[[2]], ",", fixed = TRUE)[[1]])
  } else {
    ducknng_bench_parse_int_csv(Sys.getenv("DUCKNNG_BULK_ROWS", unset = ""),
      c(100000L, 1000000L, 10000000L))
  }
  db_path <- if (length(args) >= 3L) args[[3]] else file.path(tempdir(), "ducknng_quack_tpch.duckdb")
  results <- ducknng_bench_run_bulk_compare(
    repetitions = repetitions,
    rows = rows,
    db_path = db_path
  )
  print(results$metadata, row.names = FALSE)
  print(results$rpc_results, row.names = FALSE)
  print(results$http_vs_quack, row.names = FALSE)
}

parsed <- parse_mode(commandArgs(trailingOnly = TRUE))
switch(parsed$mode,
  micro = run_micro_bench(parsed$args),
  direct = run_direct_bench(parsed$args),
  bulk_compare = run_bulk_compare(parsed$args)
)
