# Support functions for the quack-upload-lane vs text-INSERT benchmark
# (issue #12), extended (issue #14) with two calibration arms. This file
# assumes bench/rpc_bulk_compare_support.R has already been sourced in the
# same environment: it reuses ducknng_bench_sql_quote,
# ducknng_bench_open_ducknng_db, ducknng_bench_open_duckdb,
# ducknng_bench_safe_disconnect, ducknng_bench_set_single_thread,
# ducknng_bench_load_ducknng, ducknng_bench_build_rpc_url_template,
# ducknng_bench_machine_details, and ducknng_bench_parse_int_csv from there
# instead of redefining them.
#
# What is compared, per (writer_mode x writer_count) cell. The first two
# writer_modes ship rows already materialized on the client; the last two are
# calibration arms that bound the comparison from either side:
#   text_insert     - client runs the row generator locally, formats every row
#                      as a SQL literal tuple, and sends one
#                      "INSERT INTO t VALUES (...), (...), ..." statement
#                      through ducknng_run_rpc(...).
#   quack_upload    - client hands the SAME row generator query to
#                      ducknng_upload_table(url, source_query, target_table),
#                      which streams rows to the server over the quack upload
#                      lane (upload_open/upload_append/upload_commit).
#   pure_bytes      - internal calibration arm. Ships a payload of the SAME
#                      total byte size quack_upload would ship for this cell
#                      (measured via a one-off untimed probe upload), over
#                      raw ducknng_request_raw(...), with no method dispatch
#                      on the server that touches SQL/appender/transaction
#                      state: the payload is not a valid ducknng wire frame,
#                      so the server's frame decode rejects it immediately and
#                      returns a tiny error reply. This isolates "how fast can
#                      ducknng move N client->server bytes" from "how much of
#                      quack_upload's time is protocol/DB work on top of that."
#                      No correctness gate: no rows land anywhere.
#   quack_official  - external calibration arm against DuckDB's own Quack
#                      extension (`INSTALL quack FROM core_nightly`), used the
#                      same client-materializes-rows way as text_insert:
#                      client fetches the row generator locally, builds one
#                      inlined VALUES INSERT, and executes it directly against
#                      a table attached over `ATTACH 'quack:host:port' AS
#                      remote` (a secret supplies the auth token). Skipped
#                      gracefully (with a clear message) if the quack
#                      extension cannot be installed/loaded in this sandbox.
# Each writer owns its own target table (dst_w<worker_id>) so single-table
# insert contention does not swamp the protocol comparison.

ducknng_bench_default_upload_ext_path <- function() {
  # NOTE: DuckDB derives the extension's init symbol name (e.g.
  # "ducknng_init_c_api") from the loaded file's basename, so a scratch copy
  # must keep the "ducknng.duckdb_extension" filename; only the containing
  # directory may differ. Do not rename the file itself, e.g.:
  #   mkdir -p /tmp/ducknng_bench_ext && \
  #     cp build/release/ducknng.duckdb_extension /tmp/ducknng_bench_ext/
  Sys.getenv("DUCKNNG_BENCH_EXT_PATH", unset = "/tmp/ducknng_bench_ext/ducknng.duckdb_extension")
}

# Deterministic row generator SQL, run by DuckDB on the client side in both
# paths (once fetched into R for text_insert, once passed through as
# source_query for quack_upload), so both paths produce bit-identical rows.
# All arithmetic is integer (promoted to DOUBLE only at the very end), so
# there is no floating-point summation-order risk in the correctness gate.
ducknng_bench_upload_row_sql <- function(rows, worker_id, seed = 42L) {
  stopifnot(rows > 0L)
  sprintf(
    paste(
      "SELECT",
      "i::INTEGER AS id,",
      # seq is deliberately distinct from id (id=i) so the row_md5 gate catches
      # an id<->seq copy/swap column-mapping bug.
      "(i * 2 + 1)::BIGINT AS seq,",
      "((((i + %d) %% 997) * 3) + %d)::DOUBLE AS value,",
      "('w' || %d || '_row_' || i)::VARCHAR AS label",
      "FROM range(%d) AS t(i)"
    ),
    as.integer(seed), as.integer(worker_id), as.integer(worker_id), as.integer(rows)
  )
}

# Independently (no DuckDB involved) computes the expected row count, sum(value),
# and count(distinct id) for ducknng_bench_upload_row_sql(rows, worker_id, seed).
# Kept in exact integer arithmetic so it matches DuckDB's result regardless of
# aggregation/summation order.
ducknng_bench_upload_expected_checksum <- function(rows, worker_id, seed = 42L) {
  stopifnot(rows > 0L)
  i <- 0:(as.integer(rows) - 1L)
  val <- (((i + as.integer(seed)) %% 997L) * 3L) + as.integer(worker_id)
  labels <- sprintf("w%d_row_%d", as.integer(worker_id), i)
  # One content-sensitive digest over EVERY column in id order (id, seq, value,
  # label), so any per-cell corruption -- a swapped seq, a tiny value change, a
  # same-length label edit -- fails the gate, not just aggregate sums. value is a
  # DOUBLE, so it is stringified with %.17g (round-trip-exact: 17 significant
  # digits distinguish any two distinct doubles) rather than a rounding %.6f. R's
  # sprintf and DuckDB's printf both call C printf on the same IEEE-754 bits, so
  # this reproduces DuckDB's
  # md5(string_agg(id||','||seq||','||printf('%.17g',value)||','||label, chr(31) ORDER BY id))
  # byte-for-byte. count(*) and count(DISTINCT id) still guard cardinality.
  rowstrs <- sprintf("%d,%.0f,%.17g,%s", i, 2 * as.numeric(i) + 1, as.numeric(val), labels)
  list(
    row_count = as.numeric(rows),
    distinct_id = as.numeric(rows),
    row_md5 = digest::digest(paste(rowstrs, collapse = intToUtf8(31L)),
      algo = "md5", serialize = FALSE)
  )
}

ducknng_bench_upload_target_ddl <- function(target_table) {
  sprintf("CREATE TABLE %s(id INTEGER, seq BIGINT, value DOUBLE, label VARCHAR)", target_table)
}

# Formats a locally-fetched data.frame of generated rows as one
# "INSERT INTO t VALUES (...), (...), ...;" statement with every row literal
# inlined as SQL text (used by both text_insert and quack_official, which
# both time client-side materialization + an inlined-literal INSERT).
ducknng_bench_build_values_insert_sql <- function(rows_df, target_table) {
  stopifnot(nrow(rows_df) > 0L)
  id_txt <- format(rows_df$id, scientific = FALSE, trim = TRUE)
  seq_txt <- format(rows_df$seq, scientific = FALSE, trim = TRUE)
  value_txt <- sprintf("%.6f", as.numeric(rows_df$value))
  label_txt <- ducknng_bench_sql_quote(as.character(rows_df$label))
  row_literals <- paste0("(", id_txt, ",", seq_txt, ",", value_txt, ",", label_txt, ")")
  paste0("INSERT INTO ", target_table, " VALUES ", paste(row_literals, collapse = ","), ";")
}

# Shared correctness-gate SQL: one content-sensitive digest over every column
# (id, seq, value, label), plus row count and distinct-id cardinality, read
# back FROM source_expr (a table name, or a table-function call producing the
# same columns). Used by text_insert/quack_upload (source_expr =
# ducknng_query_rpc(...)) and quack_official (source_expr = remote.<table>,
# read directly through the ATTACHed catalog).
ducknng_bench_upload_correctness_sql <- function(source_expr) {
  sprintf(
    "SELECT count(*) AS row_count, count(DISTINCT id) AS distinct_id, md5(string_agg(id::VARCHAR || ',' || seq::VARCHAR || ',' || printf('%%.17g', value) || ',' || label, chr(31) ORDER BY id)) AS row_md5 FROM %s",
    source_expr
  )
}

# Tries to make DuckDB's official Quack extension available on `con`:
# core_nightly first (matches the upstream install instructions), falling
# back to the stable `core` repository if core_nightly is unreachable/absent.
# Never raises; returns list(ok = TRUE/FALSE, message = ...) so callers can
# skip the quack_official arm gracefully instead of aborting the whole run.
ducknng_bench_upload_load_quack_official <- function(con) {
  installed <- try(DBI::dbExecute(con, "INSTALL quack FROM core_nightly"), silent = TRUE)
  if (inherits(installed, "try-error")) {
    installed <- try(DBI::dbExecute(con, "INSTALL quack"), silent = TRUE)
  }
  if (inherits(installed, "try-error")) {
    return(list(ok = FALSE, message = paste(
      "could not install quack (tried core_nightly and core):",
      conditionMessage(attr(installed, "condition"))
    )))
  }
  loaded <- try(DBI::dbExecute(con, "LOAD quack"), silent = TRUE)
  if (inherits(loaded, "try-error")) {
    return(list(ok = FALSE, message = paste(
      "quack installed but LOAD failed:", conditionMessage(attr(loaded, "condition"))
    )))
  }
  list(ok = TRUE, message = "ok")
}

# Starts `CALL quack_serve(...)` on a fresh random port (quack_serve does not
# accept port 0 for auto-assignment: "Invalid Input Error: Invalid Port"), so
# this retries a handful of random high ports and treats a bind failure as
# "try another port" rather than a hard error. Returns the "quack:host:port"
# uri (the same uri form used both for quack_serve and for ATTACH, matching
# the convention already used by ducknng_bench_run_quack_http() in
# rpc_bulk_compare_support.R).
ducknng_bench_quack_serve_start <- function(con, token, port_range = 20000:60000, max_tries = 30L) {
  last_err <- NULL
  for (i in seq_len(max_tries)) {
    port <- sample(port_range, 1L)
    uri <- sprintf("quack:127.0.0.1:%d", port)
    ok <- try(DBI::dbExecute(con, sprintf(
      "CALL quack_serve(%s, token=%s)",
      ducknng_bench_sql_quote(uri), ducknng_bench_sql_quote(token)
    )), silent = TRUE)
    if (!inherits(ok, "try-error")) return(uri)
    last_err <- ok
  }
  stop(sprintf(
    "ducknng upload benchmark: could not start quack_serve after %d attempts: %s",
    max_tries,
    if (!is.null(last_err)) conditionMessage(attr(last_err, "condition")) else "unknown error"
  ))
}

# One writer, one (writer_mode x writer_count) cell: repeats "reset own
# table, write rows_per_writer rows, verify" repetitions times, and returns
# one aggregated row of per-writer metrics. Halts with a descriptive error
# (correctness gate) the instant a writer's row count or checksum diverges
# from the analytically expected value. quack_uri/quack_token are only used
# when writer_mode == "quack_official".
ducknng_bench_upload_worker <- function(writer_mode, worker_id, url, rows, repetitions, seed, ext_path,
    quack_uri = NULL, quack_token = NULL) {
  if (identical(writer_mode, "quack_official")) {
    con <- ducknng_bench_open_duckdb(":memory:")
    on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
    ducknng_bench_set_single_thread(con)
    quack_load <- ducknng_bench_upload_load_quack_official(con)
    if (!quack_load$ok) {
      stop("ducknng upload benchmark: worker could not load quack extension: ", quack_load$message)
    }
    DBI::dbExecute(con, sprintf("CREATE SECRET (TYPE quack, TOKEN %s)", ducknng_bench_sql_quote(quack_token)))
    DBI::dbExecute(con, sprintf("ATTACH %s AS remote", ducknng_bench_sql_quote(quack_uri)))
  } else {
    con <- ducknng_bench_open_ducknng_db(":memory:")
    on.exit(ducknng_bench_safe_disconnect(con), add = TRUE)
    ducknng_bench_set_single_thread(con)
    ducknng_bench_load_ducknng(con, ext_path)
  }

  target_table <- sprintf("dst_w%d", as.integer(worker_id))
  remote_target <- sprintf("remote.%s", target_table)
  source_sql <- ducknng_bench_upload_row_sql(rows, worker_id, seed)
  # pure_bytes lands no rows and skips the correctness gate, so don't pay the
  # O(rows) expected-checksum construction (it can dominate/exhaust memory at
  # large DUCKNNG_UPLOAD_ROWS) for that arm.
  expected <- if (identical(writer_mode, "pure_bytes")) {
    NULL
  } else {
    ducknng_bench_upload_expected_checksum(rows, worker_id, seed)
  }

  run_rpc_exec <- function(sql) {
    DBI::dbGetQuery(con, sprintf(
      "SELECT * FROM ducknng_run_rpc(%s, %s, 0::UBIGINT)",
      ducknng_bench_sql_quote(url), ducknng_bench_sql_quote(sql)
    ))
  }

  # pure_bytes calibration: learn the exact byte size quack_upload would ship
  # for this (rows, worker_id, seed) via a one-off untimed probe upload to a
  # scratch table, then drop the probe table. This is done once per worker
  # (not per repetition) since the size does not change across repetitions.
  pure_bytes_target_size <- NA_real_
  if (identical(writer_mode, "pure_bytes")) {
    probe_table <- sprintf("dst_w%d_pure_bytes_probe", as.integer(worker_id))
    run_rpc_exec(sprintf("DROP TABLE IF EXISTS %s", probe_table))
    run_rpc_exec(ducknng_bench_upload_target_ddl(probe_table))
    probe_result <- DBI::dbGetQuery(con, sprintf(
      "SELECT * FROM ducknng_upload_table(%s, %s, %s)",
      ducknng_bench_sql_quote(url), ducknng_bench_sql_quote(source_sql), ducknng_bench_sql_quote(probe_table)
    ))
    pure_bytes_target_size <- as.numeric(probe_result$bytes_uploaded[[1]])
    run_rpc_exec(sprintf("DROP TABLE IF EXISTS %s", probe_table))
    stopifnot(is.finite(pure_bytes_target_size), pure_bytes_target_size > 0)
  }

  latencies <- numeric(repetitions)
  bytes_shipped <- numeric(repetitions)

  for (iter in seq_len(repetitions)) {
    if (writer_mode %in% c("text_insert", "quack_upload")) {
      run_rpc_exec(sprintf("DROP TABLE IF EXISTS %s", target_table))
      run_rpc_exec(ducknng_bench_upload_target_ddl(target_table))
    } else if (identical(writer_mode, "quack_official")) {
      DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", remote_target))
      DBI::dbExecute(con, ducknng_bench_upload_target_ddl(remote_target))
    }
    # pure_bytes: no target table -- nothing lands anywhere.

    if (identical(writer_mode, "text_insert")) {
      # Time the local materialization (fetch + VALUES-string construction) as
      # well as the network send, so this path is comparable to quack_upload,
      # which times its source_query execution + quack encoding + send inside
      # ducknng_upload_table. (system.time evaluates in the caller's frame, so
      # insert_sql/exec_result persist below.)
      elapsed <- system.time({
        rows_df <- DBI::dbGetQuery(con, source_sql)
        insert_sql <- ducknng_bench_build_values_insert_sql(rows_df, target_table)
        exec_result <- run_rpc_exec(insert_sql)
      })[["elapsed"]]
      payload_bytes <- nchar(insert_sql, type = "bytes")
      if (!identical(as.numeric(exec_result$rows_changed[[1]]), as.numeric(rows))) {
        stop(sprintf(
          "CORRECTNESS GATE FAILED (rows_changed) writer_mode=%s worker_id=%d iter=%d: expected %d rows_changed, got %s",
          writer_mode, worker_id, iter, rows, exec_result$rows_changed[[1]]
        ))
      }
    } else if (identical(writer_mode, "quack_upload")) {
      elapsed <- system.time({
        upload_result <- DBI::dbGetQuery(con, sprintf(
          "SELECT * FROM ducknng_upload_table(%s, %s, %s)",
          ducknng_bench_sql_quote(url),
          ducknng_bench_sql_quote(source_sql),
          ducknng_bench_sql_quote(target_table)
        ))
      })[["elapsed"]]
      payload_bytes <- as.numeric(upload_result$bytes_uploaded[[1]])
      if (!identical(as.numeric(upload_result$rows_uploaded[[1]]), as.numeric(rows))) {
        stop(sprintf(
          "CORRECTNESS GATE FAILED (rows_uploaded) writer_mode=%s worker_id=%d iter=%d: expected %d rows_uploaded, got %s",
          writer_mode, worker_id, iter, rows, upload_result$rows_uploaded[[1]]
        ))
      }
    } else if (identical(writer_mode, "quack_official")) {
      # Same client-materialization shape as text_insert (fetch locally,
      # build one inlined VALUES INSERT), executed directly against the
      # ATTACHed remote catalog instead of through ducknng_run_rpc.
      elapsed <- system.time({
        rows_df <- DBI::dbGetQuery(con, source_sql)
        insert_sql <- ducknng_bench_build_values_insert_sql(rows_df, remote_target)
        rows_affected <- DBI::dbExecute(con, insert_sql)
      })[["elapsed"]]
      # bytes_shipped is the inlined-VALUES SQL text length (same proxy as
      # text_insert), not an independently instrumented wire byte count --
      # official Quack does not expose one via SQL.
      payload_bytes <- nchar(insert_sql, type = "bytes")
      if (!identical(as.numeric(rows_affected), as.numeric(rows))) {
        stop(sprintf(
          "CORRECTNESS GATE FAILED (rows_affected) writer_mode=%s worker_id=%d iter=%d: expected %d rows_affected, got %s",
          writer_mode, worker_id, iter, rows, rows_affected
        ))
      }
    } else if (identical(writer_mode, "pure_bytes")) {
      # No SQL/appender/transaction on the server: the payload is not a valid
      # ducknng wire frame, so ducknng_decode_request() rejects it at the
      # O(1) header check and the server replies with a tiny "invalid RPC
      # envelope" error -- the full pure_bytes_target_size bytes still have to
      # cross the wire client->server first, which is exactly what is timed.
      # Ship the target byte total as several capped raw requests (each ~4 MiB,
      # matching quack_upload's per-append frame target) instead of one giant
      # request: this stays under the server's per-message receive limit, scales
      # to any dataset, and is a fair multi-frame comparison to quack_upload.
      # Payloads are prebuilt and replies validated OUTSIDE the timed block, so
      # only the client->server transport is measured.
      pb_cap <- 4L * 1024L * 1024L
      pb_total <- as.integer(pure_bytes_target_size)
      pb_sizes <- rep(pb_cap, pb_total %/% pb_cap)
      if (pb_total %% pb_cap > 0L) pb_sizes <- c(pb_sizes, pb_total %% pb_cap)
      pb_full <- rep_len(as.raw(0:255), pb_cap)
      pb_payloads <- lapply(pb_sizes, function(s) if (s == pb_cap) pb_full else rep_len(as.raw(0:255), s))
      pb_resps <- vector("list", length(pb_payloads))
      elapsed <- system.time({
        for (pk in seq_along(pb_payloads)) {
          pb_resps[[pk]] <- DBI::dbGetQuery(
            con,
            "SELECT ducknng_request_raw(?::VARCHAR, ?::BLOB, ?::INTEGER, ?::UBIGINT) AS resp",
            params = list(url, list(pb_payloads[[pk]]), 60000L, 0)
          )$resp[[1]]
        }
      })[["elapsed"]]
      payload_bytes <- pure_bytes_target_size
      # No correctness gate (no rows land), but confirm each request's full bytes
      # reached the server and were rejected at frame decode ("invalid RPC
      # envelope"), so a transport failure, timeout, or recv-limit rejection is
      # NOT silently counted as pure-bytes throughput (ducknng_request_raw
      # returns a BLOB rather than throwing on such errors).
      for (pb_rb in pb_resps) {
        decoded <- DBI::dbGetQuery(
          con,
          "SELECT error FROM ducknng_decode_frame(?::BLOB)",
          params = list(list(pb_rb))
        )
        if (!isTRUE(grepl("invalid RPC envelope", decoded$error[[1]], fixed = TRUE))) {
          stop(sprintf(
            "pure_bytes send did not get the expected server reject writer_mode=%s worker_id=%d iter=%d: error=%s",
            writer_mode, worker_id, iter, decoded$error[[1]]
          ))
        }
      }
    } else {
      stop("unknown writer_mode: ", writer_mode)
    }

    if (!identical(writer_mode, "pure_bytes")) {
      # Every generated column is checked, so a regression that corrupts only
      # seq or the VARCHAR label (while leaving id/value intact) still fails
      # the gate.
      check_source <- if (identical(writer_mode, "quack_official")) {
        remote_target
      } else {
        sprintf(
          "ducknng_query_rpc(%s, %s, 0::UBIGINT)",
          ducknng_bench_sql_quote(url),
          ducknng_bench_sql_quote(sprintf("SELECT * FROM %s", target_table))
        )
      }
      check_sql <- ducknng_bench_upload_correctness_sql(check_source)
      actual <- DBI::dbGetQuery(con, check_sql)
      gate_ok <- isTRUE(all.equal(as.numeric(actual$row_count[[1]]), expected$row_count)) &&
        isTRUE(all.equal(as.numeric(actual$distinct_id[[1]]), expected$distinct_id)) &&
        identical(as.character(actual$row_md5[[1]]), expected$row_md5)
      if (!gate_ok) {
        stop(sprintf(
          paste(
            "CORRECTNESS GATE FAILED writer_mode=%s worker_id=%d iter=%d rows=%d:",
            "expected(row_count=%s, distinct_id=%s, row_md5=%s)",
            "actual(row_count=%s, distinct_id=%s, row_md5=%s)"
          ),
          writer_mode, worker_id, iter, rows,
          expected$row_count, expected$distinct_id, expected$row_md5,
          actual$row_count[[1]], actual$distinct_id[[1]], actual$row_md5[[1]]
        ))
      }
    }

    latencies[[iter]] <- elapsed
    bytes_shipped[[iter]] <- payload_bytes
  }

  total_seconds <- sum(latencies)
  data.frame(
    writer_mode = writer_mode,
    worker_id = worker_id,
    repetitions = repetitions,
    rows_per_writer = rows,
    rows_written = if (identical(writer_mode, "pure_bytes")) 0 else rows * repetitions,
    bytes_shipped = sum(bytes_shipped),
    total_worker_seconds = total_seconds,
    rows_per_second = round(if (total_seconds > 0 && !identical(writer_mode, "pure_bytes")) (rows * repetitions) / total_seconds else NA_real_, 1),
    median_latency_seconds = stats::median(latencies),
    p95_latency_seconds = as.numeric(stats::quantile(latencies, 0.95, names = FALSE, type = 7)),
    stringsAsFactors = FALSE
  )
}

# Runs `writers` concurrent writers (PSOCK cluster, mirroring
# ducknng_bench_run_concurrent_scenario) for one (writer_mode x writer_count)
# cell and aggregates into one result row. If any worker's correctness gate
# fails, the underlying stop() propagates through parLapply/lapply and this
# function (and the whole benchmark run) aborts with that descriptive error.
ducknng_bench_run_upload_scenario <- function(url, ext_path, writer_mode, writers, rows, repetitions, seed,
    quack_uri = NULL, quack_token = NULL) {
  message(sprintf(
    "upload benchmark writer_mode=%s writers=%d rows_per_writer=%d repetitions=%d",
    writer_mode, writers, rows, repetitions
  ))
  started <- proc.time()[["elapsed"]]
  worker_fun <- function(worker_id) {
    ducknng_bench_upload_worker(
      writer_mode = writer_mode, worker_id = worker_id, url = url,
      rows = rows, repetitions = repetitions, seed = seed, ext_path = ext_path,
      quack_uri = quack_uri, quack_token = quack_token
    )
  }
  worker_results <- if (writers > 1L) {
    cl <- parallel::makeCluster(writers)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    parallel::clusterEvalQ(cl, suppressWarnings(suppressPackageStartupMessages({
      library(DBI)
      library(duckdb)
    })))
    parallel::clusterExport(
      cl,
      varlist = c("writer_mode", "url", "rows", "repetitions", "seed", "ext_path", "quack_uri", "quack_token"),
      envir = environment()
    )
    parallel::clusterExport(
      cl,
      varlist = c(
        "ducknng_bench_upload_worker", "ducknng_bench_open_ducknng_db",
        "ducknng_bench_open_duckdb", "ducknng_bench_safe_disconnect",
        "ducknng_bench_set_single_thread", "ducknng_bench_load_ducknng",
        "ducknng_bench_sql_quote", "ducknng_bench_upload_row_sql",
        "ducknng_bench_upload_expected_checksum", "ducknng_bench_upload_target_ddl",
        "ducknng_bench_build_values_insert_sql", "ducknng_bench_upload_correctness_sql",
        "ducknng_bench_upload_load_quack_official"
      ),
      envir = .GlobalEnv
    )
    parallel::parLapply(cl, seq_len(writers), worker_fun)
  } else {
    lapply(seq_len(writers), worker_fun)
  }
  elapsed <- proc.time()[["elapsed"]] - started
  worker_df <- do.call(rbind, worker_results)

  total_rows_written <- sum(worker_df$rows_written)
  total_bytes_shipped <- sum(worker_df$bytes_shipped)
  operation_seconds <- max(worker_df$total_worker_seconds)

  data.frame(
    benchmark = "upload_lane_compare",
    writer_mode = writer_mode,
    writers = writers,
    rows_per_writer = rows,
    repetitions = repetitions,
    wall_seconds = round(elapsed, 3),
    operation_seconds = round(operation_seconds, 3),
    total_rows_written = total_rows_written,
    total_bytes_shipped = total_bytes_shipped,
    aggregate_rows_per_second = round(if (operation_seconds > 0 && total_rows_written > 0) total_rows_written / operation_seconds else NA_real_, 1),
    aggregate_mb_per_second = round(if (operation_seconds > 0) (total_bytes_shipped / 1048576) / operation_seconds else NA_real_, 3),
    per_writer_rows_per_second_median = round(stats::median(worker_df$rows_per_second), 1),
    median_latency_seconds = round(stats::median(worker_df$median_latency_seconds), 4),
    p95_latency_seconds = round(max(worker_df$p95_latency_seconds), 4),
    correctness_gate = if (identical(writer_mode, "pure_bytes")) "N/A (no rows land)" else "PASS",
    stringsAsFactors = FALSE
  )
}

# Merges every writer_mode's total_bytes_shipped per writer_count into one
# wide payload-size table, plus ratio columns against quack_upload wherever
# both sides of a ratio are present in `results` (so this still works for a
# 2-arm run, a 4-arm run, or anything in between).
ducknng_bench_upload_size_ratio <- function(results) {
  modes <- unique(results$writer_mode)
  pieces <- lapply(modes, function(m) {
    df <- results[results$writer_mode == m, c("writers", "total_bytes_shipped")]
    names(df)[2] <- paste0(m, "_bytes")
    df
  })
  merged <- Reduce(function(x, y) merge(x, y, by = "writers", all = TRUE), pieces)
  merged <- merged[order(merged$writers), ]
  if (all(c("text_insert_bytes", "quack_upload_bytes") %in% names(merged))) {
    merged$text_over_quack_ratio <- round(merged$text_insert_bytes / merged$quack_upload_bytes, 3)
  }
  if (all(c("quack_official_bytes", "quack_upload_bytes") %in% names(merged))) {
    merged$quack_official_over_quack_ratio <- round(merged$quack_official_bytes / merged$quack_upload_bytes, 3)
  }
  if (all(c("pure_bytes_bytes", "quack_upload_bytes") %in% names(merged))) {
    merged$pure_bytes_over_quack_ratio <- round(merged$pure_bytes_bytes / merged$quack_upload_bytes, 3)
  }
  merged
}

# Top-level entry point: starts one ducknng service with the upload lane
# registered (and, if "quack_official" is among writer_modes and the quack
# extension is available, one official Quack server), runs every
# (writer_count x writer_mode) cell, and tears both services down. Returns
# metadata, the per-cell results, and the size-ratio table. If
# "quack_official" is requested but quack cannot be installed/loaded in this
# sandbox, that arm is dropped with a message (metadata$quack_official_status
# records why) rather than aborting the whole run -- unless it was the only
# requested arm, in which case there is nothing left to run and this stops.
ducknng_bench_run_upload_compare <- function(
    writer_counts = c(1L, 2L, 4L, 8L),
    writer_modes = c("text_insert", "quack_upload"),
    rows_per_writer = 20000L,
    repetitions = 3L,
    seed = 42L,
    transport = "tcp",
    ext_path = ducknng_bench_default_upload_ext_path(),
    quack_token = "ducknng-bench-quack-token") {
  supported_modes <- c("text_insert", "quack_upload", "pure_bytes", "quack_official")
  stopifnot(length(writer_counts) > 0L, all(writer_counts > 0L))
  stopifnot(length(writer_modes) > 0L, all(writer_modes %in% supported_modes))
  stopifnot(rows_per_writer > 0L, repetitions > 0L)
  if (!file.exists(ext_path)) {
    stop(sprintf(
      paste(
        "ducknng upload benchmark: extension not found at %s.",
        "Copy build/release/ducknng.duckdb_extension to a stable scratch path",
        "and pass it as ext_path (or set DUCKNNG_BENCH_EXT_PATH) before running."
      ),
      ext_path
    ))
  }
  message(sprintf("using ducknng extension: %s", ext_path))

  # Optional official-Quack server, only stood up if quack_official survives
  # the availability probe below.
  quack_uri <- NA_character_
  quack_server <- NULL
  quack_official_status <- "not_requested"
  if ("quack_official" %in% writer_modes) {
    probe_con <- ducknng_bench_open_duckdb(":memory:")
    probe <- ducknng_bench_upload_load_quack_official(probe_con)
    ducknng_bench_safe_disconnect(probe_con)
    if (!probe$ok) {
      message(sprintf(
        "skipping quack_official arm (could not install/load the quack extension in this sandbox): %s",
        probe$message
      ))
      writer_modes <- setdiff(writer_modes, "quack_official")
      quack_official_status <- sprintf("unavailable: %s", probe$message)
      if (length(writer_modes) == 0L) {
        stop(
          "ducknng upload benchmark: no runnable writer_modes remain ",
          "(quack_official was the only mode requested and is unavailable in this sandbox)"
        )
      }
    } else {
      quack_server <- ducknng_bench_open_duckdb(":memory:")
      ducknng_bench_set_single_thread(quack_server)
      quack_load <- ducknng_bench_upload_load_quack_official(quack_server)
      stopifnot(quack_load$ok)
      quack_uri <- ducknng_bench_quack_serve_start(quack_server, quack_token)
      quack_official_status <- sprintf("available (%s)", quack_uri)
      message(sprintf("quack_official server listening at %s", quack_uri))
    }
  }
  on.exit({
    if (!is.null(quack_server) && DBI::dbIsValid(quack_server)) {
      if (!is.na(quack_uri)) {
        suppressWarnings(try(
          DBI::dbGetQuery(quack_server, sprintf("SELECT * FROM quack_stop(%s)", ducknng_bench_sql_quote(quack_uri))),
          silent = TRUE
        ))
      }
      ducknng_bench_safe_disconnect(quack_server)
    }
  }, add = TRUE)

  service_name <- "bench_upload_lane"
  listen_template <- ducknng_bench_build_rpc_url_template(transport)
  contexts <- max(8L, max(as.integer(writer_counts)))

  server <- ducknng_bench_open_ducknng_db(":memory:")
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
  stopifnot(isTRUE(DBI::dbGetQuery(
    server,
    sprintf(
      "SELECT ducknng_start_server(%s, %s, %d, 134217728, 300000, 0::UBIGINT) AS ok",
      ducknng_bench_sql_quote(service_name),
      ducknng_bench_sql_quote(listen_template),
      contexts
    )
  )$ok[[1]]))
  # exec is opt-in (powers ducknng_run_rpc / ducknng_query_rpc, both used by
  # every cell: to reset each writer's own table and to read it back for the
  # correctness gate), and the upload lane is opt-in and separately gated.
  stopifnot(isTRUE(DBI::dbGetQuery(server, "SELECT ducknng_register_exec_method() AS ok")$ok[[1]]))
  registered <- DBI::dbGetQuery(server, "SELECT ducknng_register_upload_methods() AS n")$n[[1]]
  stopifnot(as.integer(registered) >= 1L)
  actual_url <- DBI::dbGetQuery(
    server,
    sprintf("SELECT listen FROM ducknng_list_servers() WHERE name = %s", ducknng_bench_sql_quote(service_name))
  )$listen[[1]]
  Sys.sleep(1)
  message(sprintf("upload benchmark server listening at %s", actual_url))

  cells <- list()
  idx <- 1L
  for (writers in as.integer(writer_counts)) {
    for (mode in writer_modes) {
      cells[[idx]] <- ducknng_bench_run_upload_scenario(
        url = actual_url,
        ext_path = ext_path,
        writer_mode = mode,
        writers = writers,
        rows = as.integer(rows_per_writer),
        repetitions = as.integer(repetitions),
        seed = as.integer(seed),
        quack_uri = quack_uri,
        quack_token = quack_token
      )
      idx <- idx + 1L
    }
  }
  results <- do.call(rbind, cells)
  size_ratio <- ducknng_bench_upload_size_ratio(results)

  metadata <- cbind(
    ducknng_bench_machine_details(ext_path),
    data.frame(
      benchmark = "upload_lane_compare",
      transport = transport,
      writer_counts = paste(writer_counts, collapse = ","),
      writer_modes = paste(writer_modes, collapse = ","),
      rows_per_writer = rows_per_writer,
      repetitions = repetitions,
      seed = seed,
      quack_official_status = quack_official_status,
      stringsAsFactors = FALSE
    )
  )

  list(metadata = metadata, results = results, size_ratio = size_ratio)
}
