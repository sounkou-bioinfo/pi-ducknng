# The endpoint's R dependencies, with the oldest versions whose interrupt and
# context behaviour it relies on. A missing one stops here with the fix.
local({
  required <- c(jsonlite = "0", mirai = "2.6.0", nanoarrow = "0", nanonext = "1.9.0")
  unusable <- names(required)[vapply(names(required), function(package) {
    !requireNamespace(package, quietly = TRUE) ||
      utils::packageVersion(package) < required[[package]]
  }, logical(1))]
  if (length(unusable) > 0L) {
    wanted <- ifelse(required[unusable] == "0", unusable,
                     paste0(unusable, " (>= ", required[unusable], ")"))
    stop(
      "the persistent R endpoint needs the R packages ", paste(wanted, collapse = ", "),
      "; install them with install.packages(c(",
      paste0('"', unusable, '"', collapse = ", "), "))",
      call. = FALSE
    )
  }
})

script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
if (length(script_argument) != 1L) stop("cannot locate pi-r-endpoint.R")
script_directory <- dirname(normalizePath(sub("^--file=", "", script_argument)))
source(file.path(script_directory, "ducknng-rpc.R"), local = TRUE)

DEFAULT_EVAL_WAIT_MS <- 20000L
MAX_WAIT_MS <- 600000L
MAX_JOBS <- 32L
OUTPUT_CHUNK_BYTES <- 65536L
DEFAULT_SCOPE <- "main"
SCOPE_PATTERN <- "^[A-Za-z0-9_.-]{1,128}$"

now_ms <- function() round(as.numeric(Sys.time()) * 1000)

wait_for_daemon <- function(profile, timeout = 10) {
  deadline <- Sys.time() + timeout
  repeat {
    if (mirai::status(.compute = profile)$connections == 1L) {
      return(invisible(NULL))
    }
    if (Sys.time() >= deadline) stop("timed out waiting for the mirai daemon")
    Sys.sleep(0.05)
  }
}

# The daemon keeps one environment per scope name, created on first use with
# the global environment as parent. Resetting a scope drops its environment.
daemon_scope <- function(name) {
  scope <- .piducknng_scopes[[name]]
  if (is.null(scope)) {
    scope <- new.env(parent = .GlobalEnv)
    assign(name, scope, envir = .piducknng_scopes)
  }
  scope
}

# Lists each scope's objects without forcing active bindings or promises.
daemon_scopes <- function() {
  describe <- function(object, scope) {
    class <- if (bindingIsActive(object, scope)) "active_binding" else class(scope[[object]])
    list(name = object, class = I(class))
  }
  lapply(sort(names(.piducknng_scopes)), function(name) {
    scope <- .piducknng_scopes[[name]]
    objects <- ls(scope, all.names = TRUE, sorted = TRUE)
    list(scope = name, objects = lapply(objects, describe, scope = scope))
  })
}

daemon_reset <- function(name) {
  existed <- exists(name, envir = .piducknng_scopes, inherits = FALSE)
  if (existed) rm(list = name, envir = .piducknng_scopes)
  list(scope = name, existed = existed)
}

# Runs on the mirai daemon. Standard output and messages go to the job's
# output file as they happen; each warning, message, and error is appended to
# its conditions file as one JSON line. Visible values print as they would at
# the console when echo is TRUE.
daemon_runner <- function(code, scope, directory, echo) {
  writeLines(format(round(as.numeric(Sys.time()) * 1000), scientific = FALSE),
             file.path(directory, "started"))
  output <- file(file.path(directory, "output"), open = "w")
  conditions <- file(file.path(directory, "conditions"), open = "w")
  sink(output)
  sink(output, type = "message")
  on.exit({
    sink(type = "message")
    sink()
    close(output)
    close(conditions)
  }, add = TRUE)
  runner_call <- quote(eval(expression, envir = target))
  record <- function(type, condition) {
    call <- conditionCall(condition)
    # A condition signaled at top level carries the runner's own eval call.
    if (identical(call, runner_call)) call <- NULL
    entry <- list(
      type = type,
      class = I(class(condition)),
      message = conditionMessage(condition),
      call = if (is.null(call)) NULL else paste(deparse(call), collapse = "\n")
    )
    writeLines(jsonlite::toJSON(entry, auto_unbox = TRUE, null = "null"), conditions)
    flush(conditions)
    entry
  }
  failure <- NULL
  last <- list(value = invisible(NULL), visible = FALSE)
  tryCatch(
    withCallingHandlers(
      {
        target <- .piducknng_scope(scope)
        for (expression in parse(text = code, keep.source = FALSE)) {
          last <- withVisible(eval(expression, envir = target))
          if (echo && last$visible) print(last$value)
        }
      },
      warning = function(condition) {
        record("warning", condition)
        invokeRestart("muffleWarning")
      },
      message = function(condition) {
        record("message", condition)
        cat(conditionMessage(condition), file = stderr())
        invokeRestart("muffleMessage")
      }
    ),
    error = function(condition) failure <<- record("error", condition)
  )
  value <- last$value
  # Arrow IPC carries a data frame or a non-empty vector without dimensions.
  representable <- is.data.frame(value) ||
    (is.atomic(value) && is.null(dim(value)) && length(value) > 0L)
  list(
    ok = is.null(failure),
    error = failure,
    finished_ms = round(as.numeric(Sys.time()) * 1000),
    visible = last$visible,
    class = class(value),
    length = length(value),
    rows = if (is.data.frame(value)) nrow(value) else NULL,
    representable = representable,
    value = if (is.null(failure) && representable) value else NULL
  )
}

# Installed in the daemon's global environment, where they resolve each other.
daemon_functions <- lapply(
  list(
    .piducknng_scope = daemon_scope,
    .piducknng_list_scopes = daemon_scopes,
    .piducknng_reset = daemon_reset,
    .piducknng_run = daemon_runner
  ),
  `environment<-`,
  globalenv()
)

string_argument <- function(arguments, name, default = NULL) {
  value <- arguments[[name]]
  if (is.null(value)) value <- default
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    stop("invalid_argument: ", name, " must be a string")
  }
  value
}

scope_argument <- function(arguments) {
  scope <- string_argument(arguments, "scope", DEFAULT_SCOPE)
  if (!grepl(SCOPE_PATTERN, scope)) {
    stop("invalid_argument: scope must match ", SCOPE_PATTERN)
  }
  scope
}

integer_argument <- function(arguments, name, default = NULL, minimum = 0L,
                             maximum = .Machine$integer.max) {
  value <- arguments[[name]]
  if (is.null(value)) value <- default
  valid <- is.numeric(value) && length(value) == 1L && !is.na(value) &&
    value == floor(value) && value >= minimum && value <= maximum
  if (!valid) {
    stop("invalid_argument: ", name, " must be an integer from ", minimum, " to ", maximum)
  }
  as.integer(value)
}

only_arguments <- function(arguments, allowed) {
  unknown <- setdiff(names(arguments), allowed)
  if (length(unknown) > 0L) stop("invalid_argument: unknown argument ", unknown[[1L]])
}

# Reads a file from a byte offset, ending on a UTF-8 character boundary so a
# multibyte character that is still being written is left for the next read.
read_from <- function(path, offset, limit) {
  size <- file.size(path)
  if (is.na(size) || size <= offset) return(list(text = "", next_offset = offset, more = FALSE))
  connection <- file(path, open = "rb")
  on.exit(close(connection), add = TRUE)
  seek(connection, offset)
  bytes <- readBin(connection, "raw", n = min(limit, size - offset))
  # Drop at most the three trailing bytes of an incomplete character; text
  # that is invalid for another reason is returned with its bytes escaped.
  lengths <- length(bytes) - seq.int(0L, min(3L, length(bytes) - 1L))
  complete <- Find(function(keep) validUTF8(rawToChar(bytes[seq_len(keep)])), lengths)
  keep <- if (is.null(complete)) length(bytes) else complete
  bytes <- bytes[seq_len(keep)]
  text <- iconv(rawToChar(bytes), "UTF-8", "UTF-8", sub = "byte")
  list(text = text, next_offset = offset + keep, more = offset + keep < size)
}

# Complete JSON lines only; a line still being written is read next time.
read_conditions <- function(path, offset) {
  size <- file.size(path)
  if (is.na(size) || size == 0) return(list())
  text <- rawToChar(readBin(path, "raw", n = size))
  lines <- strsplit(text, "\n", fixed = TRUE)[[1L]]
  if (!endsWith(text, "\n")) lines <- utils::head(lines, -1L)
  lapply(utils::tail(lines, max(0L, length(lines) - offset)),
         jsonlite::fromJSON, simplifyVector = FALSE)
}

new_job_registry <- function(directory, profile) {
  registry <- new.env(parent = emptyenv())
  registry$directory <- directory
  registry$profile <- profile
  registry$next_id <- 1L
  registry$jobs <- list()
  registry
}

job_state <- function(job) {
  if (mirai::unresolved(job$task)) {
    return(if (file.exists(file.path(job$directory, "started"))) "running" else "queued")
  }
  outcome <- job$task$data
  # An interrupted task, or one cancelled by stop_mirai (NNG error 20), is
  # interrupted rather than failed.
  if (job$interrupted || mirai::is_mirai_interrupt(outcome) ||
      (mirai::is_error_value(outcome) && !mirai::is_mirai_error(outcome) &&
       identical(as.integer(outcome), 20L))) {
    return("interrupted")
  }
  if (mirai::is_mirai_error(outcome) || mirai::is_error_value(outcome)) return("failed")
  if (isTRUE(outcome$ok)) "succeeded" else "failed"
}

finished <- function(state) !(state %in% c("queued", "running"))

job_error <- function(job) {
  outcome <- job$task$data
  if (mirai::is_mirai_error(outcome) || mirai::is_error_value(outcome)) {
    return(list(type = "error", class = list("endpoint_error"),
                message = as.character(outcome), call = NULL))
  }
  outcome$error
}

evict_jobs <- function(registry) {
  while (length(registry$jobs) > MAX_JOBS) {
    states <- vapply(registry$jobs, job_state, "")
    done <- names(registry$jobs)[vapply(states, finished, TRUE)]
    if (length(done) == 0L) break
    oldest <- done[[1L]]
    unlink(registry$jobs[[oldest]]$directory, recursive = TRUE)
    registry$jobs[[oldest]] <- NULL
  }
}

submit_job <- function(registry, method, arguments, echo) {
  code <- string_argument(arguments, "code")
  scope <- scope_argument(arguments)
  id <- registry$next_id
  registry$next_id <- id + 1L
  directory <- file.path(registry$directory, paste0("job-", id))
  dir.create(directory, mode = "0700")
  job <- new.env(parent = emptyenv())
  job$id <- id
  job$method <- method
  job$code <- code
  job$scope <- scope
  job$directory <- directory
  job$submitted_ms <- now_ms()
  job$interrupted <- FALSE
  job$interrupted_ms <- NULL
  job$task <- mirai::mirai(
    .piducknng_run(code, scope, directory, echo),
    code = code, scope = scope, directory = directory, echo = echo,
    .compute = registry$profile
  )
  registry$jobs[[as.character(id)]] <- job
  evict_jobs(registry)
  job
}

lookup_job <- function(registry, arguments) {
  id <- integer_argument(arguments, "job_id", minimum = 1L)
  job <- registry$jobs[[as.character(id)]]
  if (is.null(job)) stop("r_unknown_job: job ", id, " does not exist or was discarded")
  job
}

started_ms <- function(job) {
  path <- file.path(job$directory, "started")
  if (!file.exists(path)) return(NULL)
  as.numeric(readLines(path, warn = FALSE))
}

finished_ms <- function(job, state) {
  if (!finished(state)) return(NULL)
  if (!is.null(job$interrupted_ms)) return(job$interrupted_ms)
  outcome <- job$task$data
  if (is.list(outcome)) outcome$finished_ms
}

job_summary <- function(job) {
  state <- job_state(job)
  started <- started_ms(job)
  ended <- finished_ms(job, state)
  list(
    job_id = job$id,
    method = job$method,
    scope = job$scope,
    state = state,
    code = if (nchar(job$code) > 200L) paste0(substr(job$code, 1L, 200L), "...") else job$code,
    submitted_ms = job$submitted_ms,
    elapsed_ms = if (is.null(started)) NULL else (if (is.null(ended)) now_ms() else ended) - started
  )
}

job_report <- function(job, output_offset, conditions_offset) {
  summary <- job_summary(job)
  output <- read_from(file.path(job$directory, "output"), output_offset, OUTPUT_CHUNK_BYTES)
  conditions <- read_conditions(file.path(job$directory, "conditions"), conditions_offset)
  report <- c(summary, list(
    output = output$text,
    output_offset = output$next_offset,
    output_more = output$more,
    conditions = conditions,
    conditions_offset = conditions_offset + length(conditions),
    error = NULL,
    value = NULL
  ))
  if (identical(summary$state, "failed")) report$error <- job_error(job)
  if (identical(summary$state, "succeeded")) {
    outcome <- job$task$data
    report$value <- list(
      class = I(outcome$class),
      visible = outcome$visible,
      length = outcome$length,
      rows = outcome$rows,
      representable = outcome$representable
    )
  }
  report
}

condition_text <- function(condition) {
  call <- if (is.null(condition$call)) "" else paste0(" in ", condition$call)
  classes <- paste(unlist(condition$class), collapse = ", ")
  paste0(condition$message, call, " (class ", classes, ")")
}

arrow_payload <- function(value) {
  table <- if (is.data.frame(value)) value else data.frame(value = value, check.names = FALSE)
  path <- tempfile(fileext = ".arrows")
  on.exit(unlink(path), add = TRUE)
  nanoarrow::write_nanoarrow(table, path)
  readBin(path, "raw", n = file.info(path)$size)
}

value_reply <- function(job, offset = 0L, limit = NULL) {
  state <- job_state(job)
  if (identical(state, "failed")) {
    stop("r_error: job ", job$id, ": ", condition_text(job_error(job)))
  }
  if (identical(state, "interrupted")) stop("r_interrupted: job ", job$id, " was interrupted")
  if (!finished(state)) stop("r_running: job ", job$id, " is ", state)
  outcome <- job$task$data
  if (!isTRUE(outcome$representable)) {
    stop(
      "r_unrepresentable: job ", job$id, " returned an object of class ",
      paste(outcome$class, collapse = "/"),
      "; only data frames and atomic vectors return as Arrow IPC"
    )
  }
  value <- outcome$value
  count <- if (is.data.frame(value)) nrow(value) else length(value)
  if (offset > count) stop("invalid_argument: offset is beyond the ", count, " available rows")
  last <- if (is.null(limit)) count else min(count, offset + limit)
  rows <- if (last > offset) seq.int(offset + 1L, last) else integer()
  value <- if (is.data.frame(value)) value[rows, , drop = FALSE] else value[rows]
  if (is.data.frame(value)) row.names(value) <- NULL
  list(flags = DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM, payload = arrow_payload(value))
}

interrupt_job <- function(job) {
  if (finished(job_state(job))) return(FALSE)
  mirai::stop_mirai(job$task)
  job$interrupted <- TRUE
  job$interrupted_ms <- now_ms()
  TRUE
}

string_schema <- function(description, default = NULL, examples = NULL) {
  schema <- list(type = "string", description = description)
  if (!is.null(default)) schema$default <- default
  if (!is.null(examples)) schema$examples <- examples
  schema
}

integer_schema <- function(description, default = NULL, minimum = 0L, maximum = NULL) {
  schema <- list(type = "integer", description = description, minimum = minimum)
  if (!is.null(maximum)) schema$maximum <- maximum
  if (!is.null(default)) schema$default <- default
  schema
}

object_schema <- function(properties = structure(list(), names = character()),
                          required = NULL) {
  schema <- list(type = "object", properties = properties, additionalProperties = FALSE)
  if (!is.null(required)) schema$required <- I(required)
  schema
}

endpoint_manifest <- function() {
  code <- string_schema(
    "R source evaluated in the selected scope",
    examples = list(paste0(
      "mpg_by_cyl <- aggregate(mpg ~ cyl, ",
      "data = datasets::mtcars, FUN = mean); mpg_by_cyl"
    ))
  )
  scope <- string_schema(
    paste(
      "Name of the persistent environment to evaluate in, created on first use",
      "with the global environment as its parent"
    ),
    default = DEFAULT_SCOPE,
    examples = list(DEFAULT_SCOPE, "analysis")
  )
  wait_ms <- function(description, default) {
    integer_schema(description, default = default, maximum = MAX_WAIT_MS)
  }
  job_id <- integer_schema("Job ID returned by submit or reported by an eval that timed out", minimum = 1L)
  methods <- list(
    rpc_method_descriptor(
      "eval",
      paste(
        "Evaluate R code and return its value as Arrow IPC, optionally its first limit rows.",
        "Waits up to wait_ms; a longer evaluation keeps running as a job and the call",
        "fails with r_timeout naming it"
      ),
      object_schema(list(
        code = code, scope = scope,
        wait_ms = wait_ms(
          "How long to wait for the value; keep it below the caller's RPC timeout",
          DEFAULT_EVAL_WAIT_MS
        ),
        limit = integer_schema("Most rows to return; the job keeps the whole value", minimum = 0L)
      ), required = "code"),
      response_format = "arrow",
      emitted_flags = DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM
    ),
    rpc_method_descriptor(
      "submit",
      "Start evaluating R code as a job and return its job_id at once; visible values print to its output",
      object_schema(list(code = code, scope = scope), required = "code")
    ),
    rpc_method_descriptor(
      "job",
      paste(
        "Report a job's state, its output and conditions since the given offsets,",
        "and its error or value summary. Waits up to wait_ms for the job to finish or produce more"
      ),
      object_schema(list(
        job_id = job_id,
        wait_ms = wait_ms("How long to wait for progress; keep it below the caller's RPC timeout", 0L),
        output_offset = integer_schema("Byte offset in the output already read", default = 0L),
        conditions_offset = integer_schema("Number of conditions already read", default = 0L)
      ), required = "job_id"),
      mutates_state = FALSE, idempotent = TRUE
    ),
    rpc_method_descriptor(
      "result",
      "Return a finished job's value as Arrow IPC, optionally a slice of its rows",
      object_schema(list(
        job_id = job_id,
        offset = integer_schema("Rows to skip", default = 0L),
        limit = integer_schema("Most rows to return", minimum = 0L)
      ), required = "job_id"),
      response_format = "arrow",
      emitted_flags = DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
      mutates_state = FALSE, idempotent = TRUE
    ),
    rpc_method_descriptor(
      "scopes",
      paste(
        "List every scope and the names and classes of its objects, after earlier jobs finish.",
        "Active bindings are reported without being evaluated"
      ),
      object_schema(list(
        wait_ms = wait_ms("How long to wait behind earlier jobs", DEFAULT_EVAL_WAIT_MS)
      )),
      mutates_state = FALSE, idempotent = TRUE
    ),
    rpc_method_descriptor(
      "reset",
      "Discard one scope's environment after earlier jobs finish; the next use starts it empty",
      object_schema(list(
        scope = scope,
        wait_ms = wait_ms("How long to wait behind earlier jobs", DEFAULT_EVAL_WAIT_MS)
      ), required = "scope"),
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "interrupt",
      "Interrupt one queued or running job, or every unfinished job when job_id is omitted; scopes keep their state",
      object_schema(list(job_id = job_id))
    ),
    rpc_method_descriptor(
      "jobs",
      "List recent jobs with their scopes and states",
      object_schema(),
      mutates_state = FALSE, idempotent = TRUE
    ),
    rpc_method_descriptor(
      "close",
      "Stop this persistent R endpoint",
      object_schema(),
      emitted_flags = bitwOr(DUCKNNG_RPC_FLAG_PAYLOAD_JSON, DUCKNNG_RPC_FLAG_SESSION_CLOSED)
    )
  )
  methods[[length(methods)]]$closes_session <- TRUE
  rpc_manifest("piducknng-r", methods)
}

# Scope inspection and reset run on the daemon behind any queued jobs, so they
# observe and change the state those jobs leave. A request still queued at its
# deadline is withdrawn rather than left to run later.
daemon_reply <- function(registry, call, wait_ms) {
  task <- mirai::mirai(.expr = call, .compute = registry$profile)
  deadline <- now_ms() + wait_ms
  rpc_defer(function() {
    if (!mirai::unresolved(task)) {
      if (mirai::is_error_value(task$data)) stop("endpoint_error: ", as.character(task$data))
      return(rpc_json_reply(task$data))
    }
    if (now_ms() < deadline) return(NULL)
    mirai::stop_mirai(task)
    stop("r_busy: earlier jobs are still running; follow them with jobs or interrupt them")
  })
}

endpoint_dispatch <- function(method, arguments, registry) {
  if (identical(method, "eval")) {
    only_arguments(arguments, c("code", "scope", "wait_ms", "limit"))
    wait_ms <- integer_argument(arguments, "wait_ms", DEFAULT_EVAL_WAIT_MS, maximum = MAX_WAIT_MS)
    limit <- if (!is.null(arguments$limit)) integer_argument(arguments, "limit")
    job <- submit_job(registry, "eval", arguments, echo = FALSE)
    deadline <- now_ms() + wait_ms
    return(rpc_defer(function() {
      if (finished(job_state(job))) return(value_reply(job, limit = limit))
      if (now_ms() < deadline) return(NULL)
      stop(
        "r_timeout: evaluation continues as job ", job$id,
        "; call job with job_id ", job$id, " to follow it, result to fetch its value,",
        " or interrupt to stop it"
      )
    }))
  }
  if (identical(method, "submit")) {
    only_arguments(arguments, c("code", "scope"))
    job <- submit_job(registry, "submit", arguments, echo = TRUE)
    return(rpc_json_reply(list(job_id = job$id, state = job_state(job))))
  }
  if (identical(method, "job")) {
    only_arguments(arguments, c("job_id", "wait_ms", "output_offset", "conditions_offset"))
    job <- lookup_job(registry, arguments)
    wait_ms <- integer_argument(arguments, "wait_ms", 0L, maximum = MAX_WAIT_MS)
    output_offset <- integer_argument(arguments, "output_offset", 0L)
    conditions_offset <- integer_argument(arguments, "conditions_offset", 0L)
    deadline <- now_ms() + wait_ms
    progressed <- function() {
      output <- file.size(file.path(job$directory, "output"))
      conditions <- read_conditions(file.path(job$directory, "conditions"), conditions_offset)
      (!is.na(output) && output > output_offset) || length(conditions) > 0L
    }
    return(rpc_defer(function() {
      if (finished(job_state(job)) || progressed() || now_ms() >= deadline) {
        return(rpc_json_reply(job_report(job, output_offset, conditions_offset)))
      }
      NULL
    }))
  }
  if (identical(method, "result")) {
    only_arguments(arguments, c("job_id", "offset", "limit"))
    job <- lookup_job(registry, arguments)
    limit <- if (!is.null(arguments$limit)) integer_argument(arguments, "limit")
    return(value_reply(job, integer_argument(arguments, "offset", 0L), limit))
  }
  if (identical(method, "scopes")) {
    only_arguments(arguments, "wait_ms")
    wait_ms <- integer_argument(arguments, "wait_ms", DEFAULT_EVAL_WAIT_MS, maximum = MAX_WAIT_MS)
    return(daemon_reply(registry, quote(list(scopes = .piducknng_list_scopes())), wait_ms))
  }
  if (identical(method, "reset")) {
    only_arguments(arguments, c("scope", "wait_ms"))
    if (is.null(arguments$scope)) stop("invalid_argument: scope is required")
    scope <- scope_argument(arguments)
    wait_ms <- integer_argument(arguments, "wait_ms", DEFAULT_EVAL_WAIT_MS, maximum = MAX_WAIT_MS)
    return(daemon_reply(registry, bquote(.piducknng_reset(.(scope))), wait_ms))
  }
  if (identical(method, "interrupt")) {
    only_arguments(arguments, "job_id")
    targets <- if (is.null(arguments$job_id)) {
      registry$jobs
    } else {
      list(lookup_job(registry, arguments))
    }
    stopped <- Filter(interrupt_job, targets)
    return(rpc_json_reply(list(
      interrupted = I(vapply(stopped, function(job) job$id, 0L))
    )))
  }
  if (identical(method, "jobs")) {
    only_arguments(arguments, character())
    return(rpc_json_reply(list(jobs = unname(lapply(registry$jobs, job_summary)))))
  }
  if (identical(method, "close")) {
    only_arguments(arguments, character())
    return(list(
      flags = bitwOr(DUCKNNG_RPC_FLAG_PAYLOAD_JSON, DUCKNNG_RPC_FLAG_SESSION_CLOSED),
      payload = charToRaw('{"closed":true}'),
      keep_running = FALSE
    ))
  }
  stop("unknown RPC method")
}

# The default listener is an ipc:// socket beside the locator, so the
# locator directory's permissions decide who may attach.
default_listen <- function(locator) {
  socket <- file.path(normalizePath(dirname(locator)), "r.ipc")
  if (nchar(socket, type = "bytes") > 100L) {
    stop("ipc socket path is too long; pass --listen=URL: ", socket)
  }
  paste0("ipc://", socket)
}

# With --parent=PID the endpoint exits after that process does; without it,
# it runs until close.
main <- function(locator, parent = NULL, listen = default_listen(locator)) {
  if (!is.null(parent) && !grepl("^[1-9][0-9]{0,9}$", parent)) {
    stop("--parent must be a process ID")
  }
  keep_alive <- if (is.null(parent)) function() TRUE else function() rpc_process_alive(parent)
  profile <- paste0("piducknng-endpoint-", Sys.getpid())
  jobs_directory <- file.path(dirname(locator), "jobs")
  dir.create(jobs_directory, showWarnings = FALSE, mode = "0700")
  on.exit(unlink(jobs_directory, recursive = TRUE), add = TRUE)
  mirai::daemons(1L, .compute = profile)
  on.exit(mirai::daemons(0L, .compute = profile), add = TRUE, after = FALSE)
  wait_for_daemon(profile)
  initialized <- mirai::everywhere(
    {
      list2env(functions, envir = .GlobalEnv)
      assign(".piducknng_scopes", new.env(parent = emptyenv()), envir = .GlobalEnv)
    },
    functions = daemon_functions,
    .compute = profile
  )
  mirai::collect_mirai(initialized)
  registry <- new_job_registry(jobs_directory, profile)
  rpc_serve(
    listen,
    locator,
    endpoint_manifest(),
    function(method, arguments) endpoint_dispatch(method, arguments, registry),
    keep_alive = keep_alive
  )
}

usage <- "usage: pi-r-endpoint.R LOCATOR_FILE [--parent=PID] [--listen=URL]"
args <- commandArgs(trailingOnly = TRUE)
options <- grepl("^--(parent|listen)=.", args)
if (sum(!options) != 1L || anyDuplicated(sub("=.*", "", args[options]))) stop(usage)
do.call(main, c(
  list(locator = args[!options]),
  stats::setNames(as.list(sub("^--[a-z]+=", "", args[options])), sub("^--([a-z]+)=.*", "\\1", args[options]))
))
