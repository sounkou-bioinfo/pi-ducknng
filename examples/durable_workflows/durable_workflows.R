# Checkpointed workflows backed by one DuckDB process.
#
# This is an R client for schema.sql. Each state mutation is one SQL statement,
# and every worker thread must own its DuckDB connection. External effects are
# at least once; workflow_step() supplies a stable idempotency key.

.workflow_source_dir <- local({
  source_file <- tryCatch(parent.frame()$ofile, error = function(...) NULL)
  if (is.null(source_file)) {
    getwd()
  } else {
    dirname(normalizePath(source_file, mustWork = FALSE))
  }
})

# These private helpers each own one rule repeated by several public verbs:
# classed failures, bounded scalar input, claim provenance, JSON limits, or
# whole-statement conflict retry. Keeping those rules here prevents verb drift.
.workflow_abort <- function(message, class) {
  condition <- structure(
    list(message = message, call = NULL),
    class = c(class, "error", "condition")
  )
  stop(condition)
}

.workflow_check_name <- function(value, label, maximum) {
  message <- sprintf(
    "%s must contain between 1 and %d characters", label, maximum
  )
  if (!is.character(value) || length(value) != 1L) {
    stop(message, call. = FALSE)
  }
  if (is.na(value)) {
    stop(message, call. = FALSE)
  }
  size <- nchar(value, type = "chars")
  if (size < 1L || size > maximum) {
    stop(message, call. = FALSE)
  }
  invisible(value)
}

.workflow_check_number <- function(value, label, lower, upper) {
  if (!is.numeric(value) || length(value) != 1L) {
    stop(sprintf("%s must be one number", label), call. = FALSE)
  }
  if (is.na(value) || !is.finite(value) || value != trunc(value)) {
    stop(sprintf("%s must be one finite integer", label), call. = FALSE)
  }
  if (value < lower || value > upper) {
    stop(
      sprintf("%s must be between %s and %s", label, lower, upper),
      call. = FALSE
    )
  }
  invisible(value)
}

.workflow_check_claim <- function(claim) {
  if (!inherits(claim, "ducknng_workflow_claim")) {
    stop("claim must come from workflow_claim()", call. = FALSE)
  }
  invisible(claim)
}

.workflow_json_encode <- function(value) {
  encoded <- as.character(jsonlite::toJSON(
    value,
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    digits = NA
  ))
  if (nchar(encoded, type = "bytes") > 1048576L) {
    stop("JSON value exceeds 1 MiB", call. = FALSE)
  }
  encoded
}

.workflow_json_decode <- function(value) {
  jsonlite::fromJSON(as.character(value), simplifyVector = FALSE)
}

.workflow_query <- function(con, sql, params = list(), retries = 8L) {
  .workflow_check_number(retries, "retries", 1L, 100L)
  delay <- 0.001
  for (attempt in seq_len(as.integer(retries))) {
    result <- tryCatch(
      DBI::dbGetQuery(con, sql, params = unname(params)),
      error = identity
    )
    if (!inherits(result, "error")) {
      return(result)
    }
    text <- tolower(conditionMessage(result))
    conflict <- grepl("conflict on", text, fixed = TRUE) ||
      grepl("transaction conflict", text, fixed = TRUE)
    if (!conflict || attempt == retries) {
      stop(result)
    }
    Sys.sleep(delay + stats::runif(1L, 0, delay))
    delay <- min(delay * 2, 0.05)
  }
  stop("unreachable workflow retry state", call. = FALSE)
}

#' Install the workflow schema.
#'
#' @param con A DBI connection to DuckDB.
#' @param schema_path Path to schema.sql. By default, use the file beside this
#'   script when sourced, or examples/durable_workflows/schema.sql from the
#'   current working directory.
#' @return `con`, invisibly.
workflow_install <- function(con, schema_path = NULL) {
  if (is.null(schema_path)) {
    candidates <- c(
      file.path(.workflow_source_dir, "schema.sql"),
      file.path(getwd(), "examples", "durable_workflows", "schema.sql")
    )
    schema_path <- candidates[file.exists(candidates)][1L]
  }
  if (length(schema_path) != 1L || is.na(schema_path) || !file.exists(schema_path)) {
    stop("cannot find durable workflow schema.sql", call. = FALSE)
  }
  sql <- paste(readLines(schema_path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  statements <- strsplit(sql, ";", fixed = TRUE)[[1L]]
  for (statement in statements) {
    without_comments <- gsub("--[^\n]*", "", statement)
    if (nzchar(trimws(without_comments))) {
      DBI::dbExecute(con, statement)
    }
  }
  invisible(con)
}

#' Spawn a task, idempotently by task ID.
workflow_spawn <- function(con, queue_name, task_name, input, task_id = NULL,
                           priority = 0L, max_failures = 3L) {
  .workflow_check_name(queue_name, "queue_name", 128L)
  .workflow_check_name(task_name, "task_name", 256L)
  .workflow_check_number(
    priority, "priority", -.Machine$integer.max - 1, .Machine$integer.max
  )
  .workflow_check_number(max_failures, "max_failures", 1L, 1000000L)
  if (is.null(task_id)) {
    task_id <- DBI::dbGetQuery(con, "SELECT uuid()::VARCHAR AS id")$id[[1L]]
  }
  .workflow_check_name(task_id, "task_id", 64L)
  encoded <- .workflow_json_encode(input)
  rows <- .workflow_query(
    con,
    "INSERT INTO ducknng_workflow.tasks(
       task_id, queue_name, task_name, input, priority, max_failures
     )
     VALUES (CAST(? AS UUID), ?, ?, CAST(? AS JSON), ?, ?)
     ON CONFLICT DO NOTHING
     RETURNING 1 AS changed, task_id::VARCHAR AS task_id",
    list(task_id, queue_name, task_name, encoded, priority, max_failures)
  )
  if (nrow(rows) == 1L) {
    return(task_id)
  }

  existing <- .workflow_query(
    con,
    "SELECT queue_name, task_name, input, priority, max_failures
     FROM ducknng_workflow.tasks
     WHERE task_id = CAST(? AS UUID)",
    list(task_id)
  )
  if (nrow(existing) != 1L) {
    stop("spawn lost its insert without an existing task", call. = FALSE)
  }
  same <- identical(existing$queue_name[[1L]], queue_name) &&
    identical(existing$task_name[[1L]], task_name) &&
    identical(
      .workflow_json_decode(existing$input[[1L]]),
      .workflow_json_decode(encoded)
    ) &&
    isTRUE(existing$priority[[1L]] == priority) &&
    isTRUE(existing$max_failures[[1L]] == max_failures)
  if (!same) {
    .workflow_abort(
      sprintf("task %s already has different input", task_id),
      "ducknng_workflow_spawn_conflict"
    )
  }
  task_id
}

#' Mark expired final leases failed.
workflow_reap_expired <- function(con, queue_name, limit = 64L) {
  .workflow_check_name(queue_name, "queue_name", 128L)
  .workflow_check_number(limit, "limit", 1L, 10000L)
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET status = 'failed',
         failure_count = failure_count + 1,
         last_error = 'worker lease expired',
         lease_owner = NULL,
         lease_token = NULL,
         lease_expires_at_ms = NULL,
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id IN (
       SELECT task_id
       FROM ducknng_workflow.tasks
       WHERE queue_name = ?
         AND status = 'running'
         AND lease_expires_at_ms <= epoch_ms(current_timestamp)
         AND failure_count + 1 >= max_failures
       ORDER BY lease_expires_at_ms, task_id
       LIMIT ?
     )
     RETURNING 1 AS changed, task_id::VARCHAR AS task_id",
    list(queue_name, as.integer(limit))
  )
  nrow(rows)
}

#' Claim one eligible task from a queue.
workflow_claim <- function(con, queue_name, worker_id, lease_ms = 30000L,
                           reap_limit = 64L) {
  .workflow_check_name(queue_name, "queue_name", 128L)
  .workflow_check_name(worker_id, "worker_id", 256L)
  .workflow_check_number(lease_ms, "lease_ms", 1L, 86400000L)
  .workflow_check_number(reap_limit, "reap_limit", 1L, 10000L)
  workflow_reap_expired(con, queue_name, reap_limit)
  lease_token <- DBI::dbGetQuery(con, "SELECT uuid()::VARCHAR AS id")$id[[1L]]
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET status = 'running',
         run_count = run_count + 1,
         failure_count = failure_count +
           CASE WHEN status = 'running' THEN 1 ELSE 0 END,
         waiting_event = NULL,
         lease_owner = ?,
         lease_token = CAST(? AS UUID),
         lease_expires_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id = (
       SELECT t.task_id
       FROM ducknng_workflow.tasks AS t
       WHERE t.queue_name = ?
         AND (
           (t.status = 'ready'
            AND t.available_at_ms <= epoch_ms(current_timestamp))
           OR
           (t.status = 'running'
            AND t.lease_expires_at_ms <= epoch_ms(current_timestamp)
            AND t.failure_count + 1 < t.max_failures)
           OR
           (t.status = 'waiting' AND (
             (t.waiting_event IS NULL
              AND t.available_at_ms <= epoch_ms(current_timestamp))
             OR
             (t.waiting_event IS NOT NULL AND EXISTS (
               SELECT 1
               FROM ducknng_workflow.events AS e
               WHERE e.task_id = t.task_id
                 AND e.event_name = t.waiting_event
             ))
           ))
         )
         AND t.failure_count < t.max_failures
       ORDER BY t.priority DESC, t.available_at_ms, t.created_at_ms, t.task_id
       LIMIT 1
     )
     RETURNING 1 AS changed, task_id::VARCHAR AS task_id,
               queue_name, task_name, input, priority, run_count,
               failure_count, max_failures",
    list(worker_id, lease_token, as.numeric(lease_ms), queue_name)
  )
  if (nrow(rows) == 0L) {
    return(NULL)
  }
  structure(
    list(
      task_id = rows$task_id[[1L]],
      queue_name = rows$queue_name[[1L]],
      task_name = rows$task_name[[1L]],
      input = .workflow_json_decode(rows$input[[1L]]),
      priority = rows$priority[[1L]],
      run_count = rows$run_count[[1L]],
      failure_count = rows$failure_count[[1L]],
      max_failures = rows$max_failures[[1L]],
      worker_id = worker_id,
      lease_token = lease_token
    ),
    class = "ducknng_workflow_claim"
  )
}

#' Return a stable external idempotency key for a step.
workflow_step_key <- function(claim, step_name) {
  .workflow_check_claim(claim)
  .workflow_check_name(step_name, "step_name", 256L)
  paste0(claim$task_id, ":", step_name)
}

#' Extend an unexpired task lease.
workflow_heartbeat <- function(con, claim, lease_ms = 30000L) {
  .workflow_check_claim(claim)
  .workflow_check_number(lease_ms, "lease_ms", 1L, 86400000L)
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET lease_expires_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id = CAST(? AS UUID)
       AND status = 'running'
       AND lease_token = CAST(? AS UUID)
       AND lease_expires_at_ms > epoch_ms(current_timestamp)
     RETURNING 1 AS changed, task_id::VARCHAR AS task_id",
    list(as.numeric(lease_ms), claim$task_id, claim$lease_token)
  )
  if (nrow(rows) == 0L) {
    .workflow_abort(
      sprintf("lost lease for task %s", claim$task_id),
      "ducknng_workflow_lost_lease"
    )
  }
  invisible(TRUE)
}

#' Read a named checkpoint.
workflow_checkpoint <- function(con, claim, step_name) {
  .workflow_check_claim(claim)
  .workflow_check_name(step_name, "step_name", 256L)
  rows <- .workflow_query(
    con,
    "SELECT checkpoints
     FROM ducknng_workflow.tasks
     WHERE task_id = CAST(? AS UUID)",
    list(claim$task_id)
  )
  if (nrow(rows) == 0L) {
    return(list(present = FALSE, value = NULL))
  }
  checkpoints <- .workflow_json_decode(rows$checkpoints[[1L]])
  if (!(step_name %in% names(checkpoints))) {
    return(list(present = FALSE, value = NULL))
  }
  list(
    present = TRUE,
    value = .workflow_json_decode(checkpoints[[step_name]]$json)
  )
}

#' Store one checkpoint while the lease remains current.
workflow_put_checkpoint <- function(con, claim, step_name, value) {
  .workflow_check_claim(claim)
  .workflow_check_name(step_name, "step_name", 256L)
  encoded <- .workflow_json_encode(value)
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET checkpoints = json_merge_patch(
           checkpoints, json_object(?, json_object('json', ?))
         ),
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id = CAST(? AS UUID)
       AND status = 'running'
       AND lease_token = CAST(? AS UUID)
       AND lease_expires_at_ms > epoch_ms(current_timestamp)
       AND NOT list_contains(json_keys(checkpoints), ?)
       AND length(CAST(json_merge_patch(
             checkpoints, json_object(?, json_object('json', ?))
           ) AS VARCHAR)) <= 16777216
     RETURNING 1 AS changed, checkpoints",
    list(
      step_name, encoded, claim$task_id, claim$lease_token,
      step_name, step_name, encoded
    )
  )
  if (nrow(rows) == 1L) {
    checkpoints <- .workflow_json_decode(rows$checkpoints[[1L]])
    return(.workflow_json_decode(checkpoints[[step_name]]$json))
  }

  stored <- workflow_checkpoint(con, claim, step_name)
  if (stored$present) {
    return(stored$value)
  }
  lease <- .workflow_query(
    con,
    "SELECT 1 AS current
     FROM ducknng_workflow.tasks
     WHERE task_id = CAST(? AS UUID)
       AND status = 'running'
       AND lease_token = CAST(? AS UUID)
       AND lease_expires_at_ms > epoch_ms(current_timestamp)",
    list(claim$task_id, claim$lease_token)
  )
  if (nrow(lease) == 1L) {
    stop("task checkpoints exceed 16 MiB", call. = FALSE)
  }
  .workflow_abort(
    sprintf("lost lease for task %s", claim$task_id),
    "ducknng_workflow_lost_lease"
  )
}

#' Run a step unless its checkpoint is already present.
workflow_step <- function(con, claim, step_name, operation) {
  if (!is.function(operation)) {
    stop("operation must be a function", call. = FALSE)
  }
  stored <- workflow_checkpoint(con, claim, step_name)
  if (stored$present) {
    return(stored$value)
  }
  value <- operation(workflow_step_key(claim, step_name))
  workflow_put_checkpoint(con, claim, step_name, value)
}

#' Store the first value for a task event.
workflow_emit_event <- function(con, task_id, event_name, value) {
  .workflow_check_name(task_id, "task_id", 64L)
  .workflow_check_name(event_name, "event_name", 256L)
  encoded <- .workflow_json_encode(value)
  rows <- .workflow_query(
    con,
    "INSERT INTO ducknng_workflow.events(task_id, event_name, value)
     SELECT CAST(? AS UUID), ?, CAST(? AS JSON)
     WHERE EXISTS (
       SELECT 1
       FROM ducknng_workflow.tasks
       WHERE task_id = CAST(? AS UUID)
     )
     ON CONFLICT DO NOTHING
     RETURNING 1 AS changed, value",
    list(task_id, event_name, encoded, task_id)
  )
  if (nrow(rows) == 1L) {
    return(.workflow_json_decode(rows$value[[1L]]))
  }
  rows <- .workflow_query(
    con,
    "SELECT value
     FROM ducknng_workflow.events
     WHERE task_id = CAST(? AS UUID) AND event_name = ?",
    list(task_id, event_name)
  )
  if (nrow(rows) == 0L) {
    stop(sprintf("unknown task %s", task_id), call. = FALSE)
  }
  .workflow_json_decode(rows$value[[1L]])
}

#' Read a cached event or suspend the task waiting for it.
workflow_await_event <- function(con, claim, event_name) {
  .workflow_check_claim(claim)
  .workflow_check_name(event_name, "event_name", 256L)
  rows <- .workflow_query(
    con,
    "SELECT value
     FROM ducknng_workflow.events
     WHERE task_id = CAST(? AS UUID) AND event_name = ?",
    list(claim$task_id, event_name)
  )
  if (nrow(rows) == 1L) {
    return(list(ready = TRUE, value = .workflow_json_decode(rows$value[[1L]])))
  }
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET status = 'waiting',
         waiting_event = ?,
         lease_owner = NULL,
         lease_token = NULL,
         lease_expires_at_ms = NULL,
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id = CAST(? AS UUID)
       AND status = 'running'
       AND lease_token = CAST(? AS UUID)
       AND lease_expires_at_ms > epoch_ms(current_timestamp)
     RETURNING 1 AS changed, task_id::VARCHAR AS task_id",
    list(event_name, claim$task_id, claim$lease_token)
  )
  if (nrow(rows) == 0L) {
    .workflow_abort(
      sprintf("lost lease for task %s", claim$task_id),
      "ducknng_workflow_lost_lease"
    )
  }
  list(ready = FALSE, value = NULL)
}

#' Suspend a task for a bounded delay.
workflow_sleep <- function(con, claim, delay_ms) {
  .workflow_check_claim(claim)
  .workflow_check_number(delay_ms, "delay_ms", 0, 31536000000)
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET status = 'waiting',
         waiting_event = NULL,
         available_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
         lease_owner = NULL,
         lease_token = NULL,
         lease_expires_at_ms = NULL,
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id = CAST(? AS UUID)
       AND status = 'running'
       AND lease_token = CAST(? AS UUID)
       AND lease_expires_at_ms > epoch_ms(current_timestamp)
     RETURNING 1 AS changed, task_id::VARCHAR AS task_id",
    list(as.numeric(delay_ms), claim$task_id, claim$lease_token)
  )
  if (nrow(rows) == 0L) {
    .workflow_abort(
      sprintf("lost lease for task %s", claim$task_id),
      "ducknng_workflow_lost_lease"
    )
  }
  invisible(TRUE)
}

#' Complete a task while its lease remains current.
workflow_complete <- function(con, claim, result) {
  .workflow_check_claim(claim)
  encoded <- .workflow_json_encode(result)
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET status = 'completed',
         result = CAST(? AS JSON),
         waiting_event = NULL,
         lease_owner = NULL,
         lease_token = NULL,
         lease_expires_at_ms = NULL,
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id = CAST(? AS UUID)
       AND status = 'running'
       AND lease_token = CAST(? AS UUID)
       AND lease_expires_at_ms > epoch_ms(current_timestamp)
     RETURNING 1 AS changed, task_id::VARCHAR AS task_id",
    list(encoded, claim$task_id, claim$lease_token)
  )
  if (nrow(rows) == 0L) {
    .workflow_abort(
      sprintf("lost lease for task %s", claim$task_id),
      "ducknng_workflow_lost_lease"
    )
  }
  invisible(TRUE)
}

#' Fail a task and either retry or make it terminal.
workflow_fail <- function(con, claim, error, retry_delay_ms = 0) {
  .workflow_check_claim(claim)
  if (!is.character(error) || length(error) != 1L || is.na(error)) {
    stop("error must be one non-missing string", call. = FALSE)
  }
  if (nchar(error, type = "chars") > 8192L) {
    stop("error must be at most 8192 characters", call. = FALSE)
  }
  .workflow_check_number(
    retry_delay_ms, "retry_delay_ms", 0, 31536000000
  )
  rows <- .workflow_query(
    con,
    "UPDATE ducknng_workflow.tasks
     SET status = CASE
           WHEN failure_count + 1 >= max_failures THEN 'failed'
           ELSE 'ready'
         END,
         failure_count = failure_count + 1,
         available_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
         last_error = ?,
         waiting_event = NULL,
         lease_owner = NULL,
         lease_token = NULL,
         lease_expires_at_ms = NULL,
         updated_at_ms = epoch_ms(current_timestamp)
     WHERE task_id = CAST(? AS UUID)
       AND status = 'running'
       AND lease_token = CAST(? AS UUID)
       AND lease_expires_at_ms > epoch_ms(current_timestamp)
     RETURNING 1 AS changed, status",
    list(as.numeric(retry_delay_ms), error, claim$task_id, claim$lease_token)
  )
  if (nrow(rows) == 0L) {
    .workflow_abort(
      sprintf("lost lease for task %s", claim$task_id),
      "ducknng_workflow_lost_lease"
    )
  }
  rows$status[[1L]]
}

#' Inspect one task.
workflow_task <- function(con, task_id) {
  .workflow_check_name(task_id, "task_id", 64L)
  rows <- .workflow_query(
    con,
    "SELECT task_id::VARCHAR AS task_id, queue_name, task_name, input,
            status, priority, run_count, failure_count, max_failures,
            available_at_ms, waiting_event, lease_owner,
            lease_token::VARCHAR AS lease_token, lease_expires_at_ms,
            result, checkpoints, last_error, created_at_ms, updated_at_ms
     FROM ducknng_workflow.tasks
     WHERE task_id = CAST(? AS UUID)",
    list(task_id)
  )
  if (nrow(rows) == 0L) {
    return(NULL)
  }
  task <- lapply(rows[1L, , drop = FALSE], `[[`, 1L)
  task["input"] <- list(.workflow_json_decode(task$input))
  result <- if (is.na(task$result)) {
    NULL
  } else {
    .workflow_json_decode(task$result)
  }
  task["result"] <- list(result)
  task["checkpoints"] <- list(.workflow_json_decode(task$checkpoints))
  task
}
