#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
script_path <- if (length(file_arg)) {
  normalizePath(sub("^--file=", "", file_arg[[1L]]))
} else {
  normalizePath("test/durable_workflows_smoke.R")
}
root <- dirname(dirname(script_path))
source(file.path(root, "examples", "durable_workflows", "durable_workflows.R"))

require_true <- function(condition, message) {
  if (!isTRUE(condition)) {
    stop(message, call. = FALSE)
  }
}

require_condition <- function(expr, class) {
  condition <- tryCatch(
    {
      force(expr)
      NULL
    },
    error = identity
  )
  require_true(inherits(condition, class), sprintf("expected condition %s", class))
  invisible(condition)
}

main <- function() {
  database_path <- tempfile("ducknng-workflows-", fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = database_path)
on.exit({
  DBI::dbDisconnect(con, shutdown = TRUE)
  unlink(database_path)
}, add = TRUE)
workflow_install(con)

input <- list(order_id = "42", amount = 9999)
task_id <- workflow_spawn(con, "orders", "fulfill", input)
require_true(
  identical(workflow_spawn(con, "orders", "fulfill", input, task_id = task_id), task_id),
  "idempotent spawn did not return the existing task"
)
require_condition(
  workflow_spawn(
    con,
    "orders",
    "fulfill",
    list(order_id = "other"),
    task_id = task_id
  ),
  "ducknng_workflow_spawn_conflict"
)

first <- workflow_claim(con, "orders", "worker-a", lease_ms = 5000)
require_true(inherits(first, "ducknng_workflow_claim"), "ready task was not claimed")
calls <- 0L
payment <- workflow_step(con, first, "charge", function(idempotency_key) {
  calls <<- calls + 1L
  list(payment_id = "pay-42", idempotency_key = idempotency_key)
})
require_true(identical(payment$payment_id, "pay-42"), "checkpoint value changed")
replayed <- workflow_step(con, first, "charge", function(idempotency_key) {
  calls <<- calls + 1L
  list(payment_id = "wrong", idempotency_key = idempotency_key)
})
require_true(identical(replayed, payment), "checkpoint was not replayed")
require_true(calls == 1L, "completed step executed twice")
require_true(
  identical(payment$idempotency_key, paste0(task_id, ":charge")),
  "step idempotency key was unstable"
)
require_true(is.null(workflow_put_checkpoint(con, first, "nullable", NULL)),
             "null checkpoint changed")
null_checkpoint <- workflow_checkpoint(con, first, "nullable")
require_true(null_checkpoint$present && is.null(null_checkpoint$value),
             "null checkpoint looked absent")

waiting <- workflow_await_event(con, first, "shipment.packed")
require_true(!waiting$ready && is.null(waiting$value), "missing event did not suspend")
event <- list(tracking = "TRACK123")
emitted <- workflow_emit_event(con, task_id, "shipment.packed", event)
require_true(identical(emitted, event), "event value changed")
require_true(
  identical(
    workflow_emit_event(con, task_id, "shipment.packed", list(tracking = "later")),
    event
  ),
  "event did not keep its first value"
)

resumed <- workflow_claim(con, "orders", "worker-b", lease_ms = 5000)
require_true(inherits(resumed, "ducknng_workflow_claim"), "cached event did not resume task")
waiting <- workflow_await_event(con, resumed, "shipment.packed")
require_true(waiting$ready && identical(waiting$value, event), "cached event was not replayed")
require_condition(
  workflow_complete(con, first, list(wrong = TRUE)),
  "ducknng_workflow_lost_lease"
)
workflow_complete(con, resumed, list(tracking = waiting$value$tracking))
task <- workflow_task(con, task_id)
require_true(identical(task$status, "completed"), "task did not complete")
require_true(identical(task$result$tracking, "TRACK123"), "task result changed")

null_id <- workflow_spawn(con, "null", "null", NULL)
null_claim <- workflow_claim(con, "null", "worker-null")
require_true(is.null(null_claim$input), "JSON null input changed")
workflow_complete(con, null_claim, NULL)
null_task <- workflow_task(con, null_id)
require_true("input" %in% names(null_task) && is.null(null_task$input),
             "null task input field disappeared")
require_true("result" %in% names(null_task) && is.null(null_task$result),
             "null task result field disappeared")

retry_id <- workflow_spawn(
  con, "retry", "retry", list(), max_failures = 2L
)
retry_one <- workflow_claim(con, "retry", "worker-a")
require_true(identical(retry_one$task_id, retry_id), "retry task not claimed")
require_true(identical(workflow_fail(con, retry_one, "first"), "ready"),
             "first failure was terminal")
retry_two <- workflow_claim(con, "retry", "worker-a")
require_true(identical(retry_two$task_id, retry_id), "retry was not scheduled")
require_true(identical(workflow_fail(con, retry_two, "second"), "failed"),
             "failure limit was ignored")

sleep_id <- workflow_spawn(con, "sleep", "sleep", list())
sleeper <- workflow_claim(con, "sleep", "worker-a")
workflow_sleep(con, sleeper, 20)
require_true(is.null(workflow_claim(con, "sleep", "worker-b")),
             "sleeping task resumed too early")
Sys.sleep(0.04)
woken <- workflow_claim(con, "sleep", "worker-b")
require_true(identical(woken$task_id, sleep_id), "sleeping task did not resume")
workflow_complete(con, woken, list(woke = TRUE))

expired_id <- workflow_spawn(
  con, "expired", "expired", list(), max_failures = 2L
)
expired <- workflow_claim(con, "expired", "worker-old", lease_ms = 10)
Sys.sleep(0.03)
recovered <- workflow_claim(con, "expired", "worker-new", lease_ms = 5000)
require_true(identical(recovered$task_id, expired_id), "expired lease was not recovered")
require_true(recovered$failure_count == 1, "lease expiry did not count as failure")
require_condition(
  workflow_heartbeat(con, expired),
  "ducknng_workflow_lost_lease"
)
require_condition(
  workflow_put_checkpoint(con, expired, "stale-step", list(wrong = TRUE)),
  "ducknng_workflow_lost_lease"
)
workflow_complete(con, recovered, list(recovered = TRUE))

exhausted_id <- workflow_spawn(
  con, "exhausted", "exhausted", list(), max_failures = 1L
)
exhausted <- workflow_claim(con, "exhausted", "worker-old", lease_ms = 10)
require_true(identical(exhausted$task_id, exhausted_id), "exhaustion task not claimed")
Sys.sleep(0.03)
require_true(workflow_reap_expired(con, "exhausted") == 1L,
             "expired final lease was not reaped")
require_true(identical(workflow_task(con, exhausted_id)$status, "failed"),
             "reaped task was not terminal")

  cat("R durable workflow smoke: ok\n")
}

main()
