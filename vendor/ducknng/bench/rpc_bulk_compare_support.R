suppressWarnings(suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(parallel)
}))

ducknng_bench_sql_quote <- function(x) {
  paste0("'", gsub("'", "''", x, fixed = TRUE), "'")
}

ducknng_bench_open_duckdb <- function(dbdir = ":memory:", allow_unsigned_extensions = FALSE) {
  config <- if (allow_unsigned_extensions) {
    list(allow_unsigned_extensions = "true")
  } else {
    list()
  }
  DBI::dbConnect(duckdb::duckdb(config = config), dbdir = dbdir)
}

ducknng_bench_open_ducknng_db <- function(dbdir = ":memory:") {
  ducknng_bench_open_duckdb(dbdir, allow_unsigned_extensions = TRUE)
}

ducknng_bench_safe_disconnect <- function(con) {
  if (!is.null(con) && DBI::dbIsValid(con)) {
    suppressWarnings(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE))
  }
  invisible(NULL)
}

ducknng_bench_load_ducknng <- function(con, ext_path) {
  DBI::dbExecute(con, sprintf("LOAD '%s'", ext_path))
  invisible(con)
}

ducknng_bench_load_quack <- function(con) {
  try(DBI::dbExecute(con, "INSTALL quack FROM core_nightly"), silent = TRUE)
  DBI::dbExecute(con, "LOAD quack")
  invisible(con)
}

ducknng_bench_set_single_thread <- function(con) {
  DBI::dbExecute(con, "PRAGMA threads=1")
  DBI::dbExecute(con, "SET enable_progress_bar = false")
  invisible(con)
}

ducknng_bench_ensure_quack_available <- function() {
  con <- ducknng_bench_open_duckdb(":memory:")
  on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
  ducknng_bench_load_quack(con)
  invisible(TRUE)
}

ducknng_bench_ensure_tpch_db <- function(path, sf, target_rows) {
  con <- ducknng_bench_open_duckdb(path, allow_unsigned_extensions = TRUE)
  on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
  count <- NA_real_
  if (DBI::dbExistsTable(con, "lineitem")) {
    count <- as.numeric(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM lineitem")$n[[1]])
  }
  if (!is.finite(count) || count < target_rows) {
    try(DBI::dbExecute(con, "INSTALL tpch"), silent = TRUE)
    DBI::dbExecute(con, "LOAD tpch")
    for (tbl in c("lineitem", "orders", "customer", "partsupp", "supplier", "part", "nation", "region")) {
      if (DBI::dbExistsTable(con, tbl)) {
        DBI::dbExecute(con, sprintf("DROP TABLE %s", tbl))
      }
    }
    DBI::dbExecute(con, sprintf("CALL dbgen(sf=%d)", sf))
    count <- as.numeric(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM lineitem")$n[[1]])
  }
  stopifnot(is.finite(count), count >= target_rows)
  invisible(count)
}

ducknng_bench_aggregate_validation_sql <- function(source_sql) {
  paste(
    "SELECT",
    "count(*) AS row_count,",
    "sum(l_orderkey) AS sum_orderkey,",
    "sum(l_partkey) AS sum_partkey,",
    "sum(l_suppkey) AS sum_suppkey,",
    "sum(l_linenumber) AS sum_linenumber,",
    "sum(l_quantity) AS sum_quantity,",
    "sum(l_extendedprice) AS sum_extendedprice,",
    "sum(l_discount) AS sum_discount,",
    "sum(l_tax) AS sum_tax,",
    "min(l_returnflag) AS min_returnflag,",
    "max(l_linestatus) AS max_linestatus,",
    "min(l_shipdate) AS min_shipdate,",
    "max(l_commitdate) AS max_commitdate,",
    "max(l_receiptdate) AS max_receiptdate,",
    "max(length(l_shipinstruct)) AS shipinstruct_len,",
    "max(length(l_shipmode)) AS shipmode_len,",
    "sum(length(l_comment)) AS comment_len",
    "FROM", source_sql
  )
}

ducknng_bench_normalize_result <- function(df) {
  stopifnot(nrow(df) == 1L)
  out <- vector("list", ncol(df))
  names(out) <- names(df)
  for (i in seq_along(df)) {
    value <- df[[i]][[1]]
    if (inherits(value, "Date")) {
      out[[i]] <- as.character(value)
    } else if (is.numeric(value)) {
      out[[i]] <- sprintf("%.15g", as.numeric(value))
    } else {
      out[[i]] <- as.character(value)
    }
  }
  out
}

ducknng_bench_check_result <- function(actual, expected, expected_rows) {
  actual_row_count <- as.numeric(actual$row_count[[1]])
  stopifnot(identical(actual_row_count, as.numeric(expected_rows)))
  stopifnot(identical(
    ducknng_bench_normalize_result(actual),
    ducknng_bench_normalize_result(expected)
  ))
  invisible(TRUE)
}

ducknng_bench_local_baseline <- function(con, rows) {
  sql <- ducknng_bench_aggregate_validation_sql(
    sprintf("(SELECT * FROM lineitem LIMIT %d) AS lineitem_subset", rows)
  )
  DBI::dbGetQuery(con, sql)
}

ducknng_bench_time_query <- function(con, sql, expected, expected_rows) {
  result <- NULL
  elapsed <- system.time({
    result <- DBI::dbGetQuery(con, sql)
  })[["elapsed"]]
  ducknng_bench_check_result(result, expected, expected_rows)
  elapsed
}

ducknng_bench_ducknng_query_sql <- function(url, rows, serialization_mode = NULL) {
  remote_sql <- sprintf("SELECT * FROM lineitem LIMIT %d", rows)
  if (is.null(serialization_mode) || identical(serialization_mode, "arrow_ipc_stream")) {
    source_sql <- sprintf(
      "ducknng_query_rpc(%s, %s, 0::UBIGINT)",
      ducknng_bench_sql_quote(url), ducknng_bench_sql_quote(remote_sql)
    )
  } else {
    source_sql <- sprintf(
      "ducknng_query_rpc_mode(%s, %s, 0::UBIGINT, %s)",
      ducknng_bench_sql_quote(url),
      ducknng_bench_sql_quote(remote_sql),
      ducknng_bench_sql_quote(serialization_mode)
    )
  }
  ducknng_bench_aggregate_validation_sql(source_sql)
}

ducknng_bench_quack_query_sql <- function(uri, token, rows) {
  remote_sql <- sprintf("SELECT * FROM lineitem LIMIT %d", rows)
  source_sql <- sprintf(
    "quack_query(%s, %s, token=%s)",
    ducknng_bench_sql_quote(uri),
    ducknng_bench_sql_quote(remote_sql),
    ducknng_bench_sql_quote(token)
  )
  ducknng_bench_aggregate_validation_sql(source_sql)
}

ducknng_bench_collect_rpc_timings <- function(system_name, protocol_name, transport_name,
    dataset_name, client_con, sql_builder, rows, repetitions, baselines,
    serialization_mode = NA_character_) {
  results <- vector("list", length(rows))
  for (i in seq_along(rows)) {
    row_count <- rows[[i]]
    message(sprintf(
      "benchmark %s/%s transport=%s serialization=%s rows=%d repetitions=%d",
      system_name, protocol_name, transport_name, serialization_mode, row_count, repetitions
    ))
    sql <- sql_builder(row_count)
    invisible(ducknng_bench_time_query(client_con, sql, baselines[[as.character(row_count)]], row_count))
    timings <- vapply(seq_len(repetitions), function(rep_idx) {
      ducknng_bench_time_query(client_con, sql, baselines[[as.character(row_count)]], row_count)
    }, numeric(1))
    results[[i]] <- data.frame(
      benchmark = "bulk_transfer_lineitem_limit",
      dataset = dataset_name,
      system = system_name,
      protocol = protocol_name,
      transport = transport_name,
      serialization_mode = serialization_mode,
      rows = row_count,
      repetitions = repetitions,
      median_seconds = round(stats::median(timings), 3),
      min_seconds = round(min(timings), 3),
      max_seconds = round(max(timings), 3),
      timings_seconds = paste(sprintf("%.3f", timings), collapse = ","),
      stringsAsFactors = FALSE
    )
  }
  do.call(rbind, results)
}

ducknng_bench_rpc_source_sql <- function(url, remote_sql, serialization_mode = "arrow_ipc_stream") {
  if (identical(serialization_mode, "arrow_ipc_stream")) {
    sprintf(
      "ducknng_query_rpc(%s, %s, 0::UBIGINT)",
      ducknng_bench_sql_quote(url),
      ducknng_bench_sql_quote(remote_sql)
    )
  } else {
    sprintf(
      "ducknng_query_rpc_mode(%s, %s, 0::UBIGINT, %s)",
      ducknng_bench_sql_quote(url),
      ducknng_bench_sql_quote(remote_sql),
      ducknng_bench_sql_quote(serialization_mode)
    )
  }
}

ducknng_bench_concurrent_worker <- function(kind, worker_id, url, serialization_mode,
    rows, iterations, ext_path) {
  con <- ducknng_bench_open_ducknng_db(":memory:")
  on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
  ducknng_bench_set_single_thread(con)
  ducknng_bench_load_ducknng(con, ext_path)
  latencies <- numeric(iterations)
  rows_read <- 0
  rows_written <- 0
  writes <- 0
  for (iter in seq_len(iterations)) {
    if (identical(kind, "reader")) {
      source_sql <- ducknng_bench_rpc_source_sql(
        url,
        sprintf("SELECT l_orderkey FROM lineitem LIMIT %d", rows),
        serialization_mode
      )
      sql <- sprintf("SELECT count(*) AS n FROM %s", source_sql)
      elapsed <- system.time({ result <- DBI::dbGetQuery(con, sql) })[["elapsed"]]
      stopifnot(identical(as.numeric(result$n[[1]]), as.numeric(rows)))
      rows_read <- rows_read + rows
      latencies[[iter]] <- elapsed
    } else {
      sql <- sprintf(
        paste(
          "INSERT INTO ducknng_bench_writes",
          "SELECT %d, %d, i, %s FROM range(%d) AS t(i)"
        ),
        as.integer(worker_id),
        as.integer(iter),
        ducknng_bench_sql_quote(serialization_mode),
        as.integer(rows)
      )
      elapsed <- system.time({
        DBI::dbGetQuery(con, sprintf(
          "SELECT * FROM ducknng_run_rpc(%s, %s, 0::UBIGINT)",
          ducknng_bench_sql_quote(url),
          ducknng_bench_sql_quote(sql)
        ))
      })[["elapsed"]]
      rows_written <- rows_written + rows
      writes <- writes + 1L
      latencies[[iter]] <- elapsed
    }
  }
  data.frame(
    kind = kind,
    worker_id = worker_id,
    operations = iterations,
    rows_read = rows_read,
    rows_written = rows_written,
    write_ops = writes,
    total_worker_seconds = sum(latencies),
    median_latency_seconds = stats::median(latencies),
    p95_latency_seconds = as.numeric(stats::quantile(latencies, 0.95, names = FALSE, type = 7)),
    stringsAsFactors = FALSE
  )
}

ducknng_bench_run_concurrent_scenario <- function(url, transport, serialization_mode,
    ext_path, rows, iterations, readers, writers) {
  worker_plan <- rbind(
    if (readers > 0L) data.frame(kind = "reader", worker_id = seq_len(readers)) else NULL,
    if (writers > 0L) data.frame(kind = "writer", worker_id = seq_len(writers)) else NULL
  )
  if (nrow(worker_plan) == 0L) return(NULL)
  scenario <- if (readers > 0L && writers > 0L) "mixed_read_write" else if (readers > 0L) "readers" else "writers"
  message(sprintf(
    "concurrent benchmark transport=%s serialization=%s scenario=%s readers=%d writers=%d rows=%d iterations=%d",
    transport, serialization_mode, scenario, readers, writers, rows, iterations
  ))
  started <- proc.time()[["elapsed"]]
  worker_fun <- function(i) {
    ducknng_bench_concurrent_worker(
      kind = worker_plan$kind[[i]],
      worker_id = worker_plan$worker_id[[i]],
      url = url,
      serialization_mode = serialization_mode,
      rows = rows,
      iterations = iterations,
      ext_path = ext_path
    )
  }
  worker_results <- if (nrow(worker_plan) > 1L) {
    cl <- parallel::makeCluster(nrow(worker_plan))
    on.exit(parallel::stopCluster(cl), add = TRUE)
    parallel::clusterEvalQ(cl, suppressWarnings(suppressPackageStartupMessages({
      library(DBI)
      library(duckdb)
    })))
    parallel::clusterExport(
      cl,
      varlist = c("worker_plan", "url", "serialization_mode", "rows", "iterations", "ext_path"),
      envir = environment()
    )
    parallel::clusterExport(
      cl,
      varlist = c(
        "ducknng_bench_concurrent_worker", "ducknng_bench_open_ducknng_db",
        "ducknng_bench_open_duckdb", "ducknng_bench_safe_disconnect",
        "ducknng_bench_set_single_thread", "ducknng_bench_load_ducknng",
        "ducknng_bench_rpc_source_sql", "ducknng_bench_sql_quote"
      ),
      envir = .GlobalEnv
    )
    parallel::parLapply(cl, seq_len(nrow(worker_plan)), function(i) {
      ducknng_bench_concurrent_worker(
        kind = worker_plan$kind[[i]],
        worker_id = worker_plan$worker_id[[i]],
        url = url,
        serialization_mode = serialization_mode,
        rows = rows,
        iterations = iterations,
        ext_path = ext_path
      )
    })
  } else {
    lapply(seq_len(nrow(worker_plan)), worker_fun)
  }
  elapsed <- proc.time()[["elapsed"]] - started
  worker_df <- do.call(rbind, worker_results)
  total_rows <- sum(worker_df$rows_read)
  total_rows_written <- sum(worker_df$rows_written)
  total_writes <- sum(worker_df$write_ops)
  operation_seconds <- max(worker_df$total_worker_seconds)
  data.frame(
    benchmark = "concurrent_read_write",
    system = "ducknng",
    protocol = "rpc",
    transport = transport,
    serialization_mode = serialization_mode,
    scenario = scenario,
    readers = readers,
    writers = writers,
    rows_per_reader_operation = rows,
    iterations_per_worker = iterations,
    wall_seconds = round(elapsed, 3),
    operation_seconds = round(operation_seconds, 3),
    rows_read = total_rows,
    rows_written = total_rows_written,
    write_ops = total_writes,
    rows_per_second = round(if (operation_seconds > 0) total_rows / operation_seconds else NA_real_, 1),
    write_rows_per_second = round(if (operation_seconds > 0) total_rows_written / operation_seconds else NA_real_, 1),
    write_ops_per_second = round(if (operation_seconds > 0) total_writes / operation_seconds else NA_real_, 1),
    median_latency_seconds = round(stats::median(worker_df$median_latency_seconds), 3),
    p95_latency_seconds = round(max(worker_df$p95_latency_seconds), 3),
    stringsAsFactors = FALSE
  )
}

ducknng_bench_run_concurrent_ducknng_transport <- function(transport, url, ext_path,
    rows = 100000L, iterations = 2L, clients = 2L,
    serialization_modes = c("arrow_ipc_stream", "ducknng_quack_batch")) {
  stopifnot(rows > 0L, iterations > 0L, clients > 0L)
  out <- list()
  idx <- 1L
  for (mode in serialization_modes) {
    scenarios <- list(
      list(readers = clients, writers = 0L),
      list(readers = 0L, writers = clients),
      list(readers = clients, writers = clients)
    )
    for (scenario in scenarios) {
      out[[idx]] <- ducknng_bench_run_concurrent_scenario(
        url = url,
        transport = transport,
        serialization_mode = mode,
        ext_path = ext_path,
        rows = rows,
        iterations = iterations,
        readers = scenario$readers,
        writers = scenario$writers
      )
      idx <- idx + 1L
    }
  }
  do.call(rbind, out)
}

ducknng_bench_build_rpc_url_template <- function(transport) {
  switch(transport,
    http = "http://127.0.0.1:0/_ducknng",
    tcp = "tcp://127.0.0.1:0",
    ws = "ws://127.0.0.1:0/_ducknng",
    ipc = paste0("ipc://", tempfile(pattern = "ducknng_bulk_rpc_", tmpdir = "/tmp", fileext = ".ipc")),
    stop("unsupported transport: ", transport)
  )
}

ducknng_bench_run_ducknng_rpc_transport <- function(transport, rows, repetitions, baselines,
    db_path, ext_path, dataset_name, concurrent_rows = 0L,
    concurrent_iterations = 0L, concurrent_clients = 0L) {
  service_name <- paste0("bench_bulk_", transport)
  listen_template <- ducknng_bench_build_rpc_url_template(transport)
  server <- ducknng_bench_open_ducknng_db(db_path)
  client <- NULL
  on.exit(ducknng_bench_safe_disconnect(client), add = TRUE)
  on.exit({
    if (!is.null(server) && DBI::dbIsValid(server)) {
      suppressWarnings(try(
        DBI::dbGetQuery(server, sprintf(
          "SELECT ducknng_stop_server(%s) AS ok",
          ducknng_bench_sql_quote(service_name)
        )), silent = TRUE
      ))
    }
    ducknng_bench_safe_disconnect(server)
  }, add = TRUE)
  ducknng_bench_set_single_thread(server)
  ducknng_bench_load_ducknng(server, ext_path)
  service_contexts <- if (identical(transport, "http")) 1L else 8L
  stopifnot(isTRUE(DBI::dbGetQuery(
    server,
    sprintf(
      "SELECT ducknng_start_server(%s, %s, %d, 134217728, 300000, 0::UBIGINT) AS ok",
      ducknng_bench_sql_quote(service_name),
      ducknng_bench_sql_quote(listen_template),
      service_contexts
    )
  )$ok[[1]]))
  actual_url <- DBI::dbGetQuery(
    server,
    sprintf("SELECT listen FROM ducknng_list_servers() WHERE name = %s",
      ducknng_bench_sql_quote(service_name))
  )$listen[[1]]
  Sys.sleep(1)
  client <- ducknng_bench_open_ducknng_db(":memory:")
  ducknng_bench_set_single_thread(client)
  ducknng_bench_load_ducknng(client, ext_path)
  rpc_results <- rbind(
    ducknng_bench_collect_rpc_timings(
      system_name = "ducknng",
      protocol_name = "rpc",
      transport_name = transport,
      dataset_name = dataset_name,
      client_con = client,
      sql_builder = function(n) ducknng_bench_ducknng_query_sql(actual_url, n, "arrow_ipc_stream"),
      rows = rows,
      repetitions = repetitions,
      baselines = baselines,
      serialization_mode = "arrow_ipc_stream"
    ),
    ducknng_bench_collect_rpc_timings(
      system_name = "ducknng",
      protocol_name = "rpc",
      transport_name = transport,
      dataset_name = dataset_name,
      client_con = client,
      sql_builder = function(n) ducknng_bench_ducknng_query_sql(actual_url, n, "ducknng_quack_batch"),
      rows = rows,
      repetitions = repetitions,
      baselines = baselines,
      serialization_mode = "ducknng_quack_batch"
    )
  )
  concurrent_results <- NULL
  if (concurrent_rows > 0L && concurrent_iterations > 0L && concurrent_clients > 0L) {
    DBI::dbExecute(server, "DROP TABLE IF EXISTS ducknng_bench_writes")
    DBI::dbExecute(server, "CREATE TABLE ducknng_bench_writes(worker INTEGER, iter INTEGER, value BIGINT, mode VARCHAR)")
    concurrent_results <- ducknng_bench_run_concurrent_ducknng_transport(
      transport = transport,
      url = actual_url,
      ext_path = ext_path,
      rows = concurrent_rows,
      iterations = concurrent_iterations,
      clients = concurrent_clients
    )
  }
  list(rpc = rpc_results, concurrent = concurrent_results)
}

ducknng_bench_run_quack_http <- function(rows, repetitions, baselines,
    db_path, dataset_name, quack_uri, quack_token) {
  server <- ducknng_bench_open_duckdb(db_path, allow_unsigned_extensions = TRUE)
  client <- NULL
  on.exit(ducknng_bench_safe_disconnect(client), add = TRUE)
  on.exit(ducknng_bench_safe_disconnect(server), add = TRUE)
  ducknng_bench_set_single_thread(server)
  ducknng_bench_load_quack(server)
  DBI::dbExecute(
    server,
    sprintf("CALL quack_serve(%s, token=%s)",
      ducknng_bench_sql_quote(quack_uri),
      ducknng_bench_sql_quote(quack_token))
  )
  Sys.sleep(1)
  client <- ducknng_bench_open_duckdb(":memory:")
  ducknng_bench_set_single_thread(client)
  ducknng_bench_load_quack(client)
  ducknng_bench_collect_rpc_timings(
    system_name = "quack",
    protocol_name = "quack_query",
    transport_name = "http",
    dataset_name = dataset_name,
    client_con = client,
    sql_builder = function(n) ducknng_bench_quack_query_sql(quack_uri, quack_token, n),
    rows = rows,
    repetitions = repetitions,
    baselines = baselines,
    serialization_mode = "application/vnd.duckdb"
  )
}

ducknng_bench_capture_cmd <- function(command) {
  out <- suppressWarnings(try(system2("bash", c("-lc", shQuote(command)), stdout = TRUE, stderr = FALSE), silent = TRUE))
  if (inherits(out, "try-error") || length(out) == 0L) return(NA_character_)
  trimws(paste(out, collapse = "\n"))
}

ducknng_bench_relpath <- function(path, base = getwd()) {
  npath <- normalizePath(path, mustWork = FALSE)
  nbase <- normalizePath(base, mustWork = FALSE)
  prefix <- paste0(nbase, .Platform$file.sep)
  if (startsWith(npath, prefix)) return(substr(npath, nchar(prefix) + 1L, nchar(npath)))
  npath
}

ducknng_bench_machine_details <- function(ext_path) {
  sys <- Sys.info()
  mem_total <- ducknng_bench_capture_cmd("awk '/MemTotal:/ {printf \"%.1f GiB\", $2/1024/1024}' /proc/meminfo")
  cpu_model <- ducknng_bench_capture_cmd("awk -F: '/model name/ {gsub(/^ +/, \"\", $2); print $2; exit}' /proc/cpuinfo")
  if (is.na(cpu_model) || !nzchar(cpu_model)) {
    cpu_model <- ducknng_bench_capture_cmd("sysctl -n machdep.cpu.brand_string 2>/dev/null")
  }
  data.frame(
    generated_at = format(Sys.time(), tz = "UTC", usetz = TRUE),
    hostname = unname(sys[["nodename"]]),
    sysname = unname(sys[["sysname"]]),
    release = unname(sys[["release"]]),
    machine = unname(sys[["machine"]]),
    cpu_model = cpu_model,
    logical_cores = parallel::detectCores(logical = TRUE),
    physical_cores = parallel::detectCores(logical = FALSE),
    memory_total = mem_total,
    r_version = R.version.string,
    duckdb_version = as.character(utils::packageVersion("duckdb")),
    ducknng_extension = ducknng_bench_relpath(ext_path),
    ducknng_git_commit = ducknng_bench_capture_cmd("git rev-parse --short HEAD"),
    quack_install_source = "INSTALL quack FROM core_nightly",
    stringsAsFactors = FALSE
  )
}

ducknng_bench_find_ext_path <- function() {
  override <- Sys.getenv("DUCKNNG_BENCH_EXT_PATH", unset = "")
  candidates <- c(
    if (nzchar(override)) override,
    "build/release/ducknng.duckdb_extension",
    "../build/release/ducknng.duckdb_extension"
  )
  for (path in candidates) {
    if (file.exists(path)) return(normalizePath(path, mustWork = TRUE))
  }
  stop("ducknng benchmark could not find build/release/ducknng.duckdb_extension")
}

ducknng_bench_parse_int_csv <- function(text, default) {
  if (is.null(text) || !nzchar(text)) return(as.integer(default))
  as.integer(strsplit(text, ",", fixed = TRUE)[[1]])
}

ducknng_bench_run_bulk_compare <- function(
    repetitions = 5L,
    rows = c(100000L, 1000000L, 10000000L),
    db_path = file.path(tempdir(), "ducknng_quack_tpch.duckdb"),
    quack_uri = Sys.getenv("DUCKNNG_QUACK_URI", unset = "quack:localhost:19494"),
    quack_token = Sys.getenv("DUCKNNG_QUACK_TOKEN", unset = "asdf"),
    ducknng_transports = c("http", "tcp", "ipc", "ws"),
    concurrent_rows = min(rows),
    concurrent_iterations = 2L,
    concurrent_clients = 2L) {
  stopifnot(repetitions > 0L, length(rows) > 0L, all(rows > 0L))
  ext_path <- ducknng_bench_find_ext_path()
  message(sprintf("using ducknng extension: %s", ext_path))
  max_rows <- max(rows)
  required_sf <- max(1L, as.integer(ceiling(max_rows / 6000000)))
  dataset_name <- sprintf("tpch_sf%d.lineitem", required_sf)

  message("checking Quack extension availability")
  ducknng_bench_ensure_quack_available()
  message(sprintf("ensuring TPC-H data: db_path=%s scale_factor=%d rows_needed=%d", db_path, required_sf, max_rows))
  total_lineitem_rows <- ducknng_bench_ensure_tpch_db(db_path, required_sf, max_rows)

  baseline_con <- ducknng_bench_open_duckdb(db_path, allow_unsigned_extensions = TRUE)
  on.exit(ducknng_bench_safe_disconnect(baseline_con), add = TRUE)
  ducknng_bench_set_single_thread(baseline_con)
  baselines <- setNames(lapply(rows, function(n) ducknng_bench_local_baseline(baseline_con, n)), as.character(rows))

  ducknng_transport_results <- lapply(ducknng_transports, function(transport) {
    message(sprintf("starting ducknng RPC benchmark transport=%s", transport))
    ducknng_bench_run_ducknng_rpc_transport(
      transport = transport,
      rows = rows,
      repetitions = repetitions,
      baselines = baselines,
      db_path = db_path,
      ext_path = ext_path,
      dataset_name = dataset_name,
      concurrent_rows = concurrent_rows,
      concurrent_iterations = concurrent_iterations,
      concurrent_clients = concurrent_clients
    )
  })
  ducknng_rpc_results <- do.call(rbind, lapply(ducknng_transport_results, `[[`, "rpc"))
  concurrent_pieces <- Filter(Negate(is.null), lapply(ducknng_transport_results, `[[`, "concurrent"))
  concurrent_results <- if (length(concurrent_pieces)) do.call(rbind, concurrent_pieces) else data.frame()

  message("starting Quack public HTTP benchmark")
  quack_results <- ducknng_bench_run_quack_http(
    rows = rows,
    repetitions = repetitions,
    baselines = baselines,
    db_path = db_path,
    dataset_name = dataset_name,
    quack_uri = quack_uri,
    quack_token = quack_token
  )

  http_arrow_rows <- ducknng_rpc_results[
    ducknng_rpc_results$transport == "http" & ducknng_rpc_results$serialization_mode == "arrow_ipc_stream",
    c("rows", "median_seconds")
  ]
  names(http_arrow_rows)[2] <- "ducknng_http_arrow_median_seconds"
  http_quack_rows <- ducknng_rpc_results[
    ducknng_rpc_results$transport == "http" & ducknng_rpc_results$serialization_mode == "ducknng_quack_batch",
    c("rows", "median_seconds")
  ]
  names(http_quack_rows)[2] <- "ducknng_http_quack_batch_median_seconds"
  quack_rows <- quack_results[, c("rows", "median_seconds")]
  names(quack_rows)[2] <- "quack_http_median_seconds"
  http_vs_quack <- Reduce(function(x, y) merge(x, y, by = "rows", all = TRUE),
    list(http_arrow_rows, http_quack_rows, quack_rows))
  http_vs_quack$ducknng_arrow_over_quack_ratio <- round(
    http_vs_quack$ducknng_http_arrow_median_seconds / http_vs_quack$quack_http_median_seconds,
    3
  )
  http_vs_quack$ducknng_quack_batch_over_quack_ratio <- round(
    http_vs_quack$ducknng_http_quack_batch_median_seconds / http_vs_quack$quack_http_median_seconds,
    3
  )

  metadata <- cbind(
    ducknng_bench_machine_details(ext_path),
    data.frame(
      dataset = dataset_name,
      lineitem_rows_available = total_lineitem_rows,
      repetitions = repetitions,
      ducknng_transports = paste(ducknng_transports, collapse = ","),
      ducknng_serialization_modes = "arrow_ipc_stream,ducknng_quack_batch",
      concurrent_rows = concurrent_rows,
      concurrent_iterations = concurrent_iterations,
      concurrent_clients = concurrent_clients,
      quack_uri = quack_uri,
      stringsAsFactors = FALSE
    )
  )

  list(
    metadata = metadata,
    rpc_results = rbind(ducknng_rpc_results, quack_results),
    http_vs_quack = http_vs_quack,
    concurrent_results = concurrent_results
  )
}
