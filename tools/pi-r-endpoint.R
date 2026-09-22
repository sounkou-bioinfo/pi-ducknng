script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
if (length(script_argument) != 1L) stop("cannot locate pi-r-endpoint.R")
script_directory <- dirname(normalizePath(sub("^--file=", "", script_argument)))
source(file.path(script_directory, "ducknng-rpc.R"), local = TRUE)

DEFAULT_EVAL_WAIT_MS <- 20000L
MAX_WAIT_MS <- 600000L
MAX_JOBS <- 32L
OUTPUT_CHUNK_BYTES <- 65536L

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

# Runs on the mirai daemon. Standard output and messages go to the job's
# output file as they happen; each warning, message, and error is appended to
# its conditions file as one JSON line. Visible values print as they would at
# the console when echo is TRUE.
daemon_runner <- function(code, envir, enclos, directory, echo) {
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
  runner_call <- quote(eval(expression, envir = target, enclos = enclosure))
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
        scope <- .piducknng_session
        target <- eval(parse(text = envir, keep.source = FALSE), envir = scope, enclos = .GlobalEnv)
        enclosure <- eval(parse(text = enclos, keep.source = FALSE), envir = scope, enclos = .GlobalEnv)
        for (expression in parse(text = code, keep.source = FALSE)) {
          last <- withVisible(eval(expression, envir = target, enclos = enclosure))
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
environment(daemon_runner) <- globalenv()

string_argument <- function(arguments, name, default = NULL) {
  value <- arguments[[name]]
  if (is.null(value)) value <- default
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    stop("invalid_argument: ", name, " must be a string")
  }
  value
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
  keep <- length(bytes)
  tail <- 0L
  while (keep > 0L && bitwAnd(as.integer(bytes[[keep]]), 0xC0L) == 0x80L && tail < 3L) {
    keep <- keep - 1L
    tail <- tail + 1L
  }
  if (keep > 0L) {
    lead <- as.integer(bytes[[keep]])
    width <- if (lead >= 0xF0L) 4L else if (lead >= 0xE0L) 3L else if (lead >= 0xC0L) 2L else 1L
    keep <- if (tail + 1L < width) keep - 1L else keep + tail
  }
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
  envir <- string_argument(arguments, "envir", ".piducknng_session")
  enclos <- string_argument(arguments, "enclos", "baseenv()")
  id <- registry$next_id
  registry$next_id <- id + 1L
  directory <- file.path(registry$directory, paste0("job-", id))
  dir.create(directory, mode = "0700")
  job <- new.env(parent = emptyenv())
  job$id <- id
  job$method <- method
  job$code <- code
  job$directory <- directory
  job$submitted_ms <- now_ms()
  job$interrupted <- FALSE
  job$interrupted_ms <- NULL
  job$task <- mirai::mirai(
    .piducknng_run(code, envir, enclos, directory, echo),
    code = code, envir = envir, enclos = enclos, directory = directory, echo = echo,
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
  if (is.list(outcome) && !is.null(outcome$finished_ms)) outcome$finished_ms else NULL
}

job_summary <- function(job) {
  state <- job_state(job)
  started <- started_ms(job)
  ended <- finished_ms(job, state)
  list(
    job_id = job$id,
    method = job$method,
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
    "R source evaluated in the selected persistent environment",
    examples = list(paste0(
      "mpg_by_cyl <- aggregate(mpg ~ cyl, ",
      "data = datasets::mtcars, FUN = mean); mpg_by_cyl"
    ))
  )
  envir <- string_schema(
    "R expression resolving to the evaluation environment",
    default = ".piducknng_session",
    examples = list(".piducknng_session", "analysis")
  )
  enclos <- string_schema(
    "R expression resolving to eval()'s enclosure",
    default = "baseenv()",
    examples = list("baseenv()", "globalenv()")
  )
  job_id <- integer_schema("Job ID returned by submit or reported by an eval that timed out", minimum = 1L)
  methods <- list(
    rpc_method_descriptor(
      "eval",
      paste(
        "Evaluate R code and return its value as Arrow IPC. Waits up to wait_ms;",
        "a longer evaluation keeps running as a job and the call fails with r_timeout naming it"
      ),
      object_schema(list(
        code = code, envir = envir, enclos = enclos,
        wait_ms = integer_schema(
          "How long to wait for the value; keep it below the caller's RPC timeout",
          default = DEFAULT_EVAL_WAIT_MS, maximum = MAX_WAIT_MS
        )
      ), required = "code"),
      response_format = "arrow",
      emitted_flags = DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM
    ),
    rpc_method_descriptor(
      "submit",
      "Start evaluating R code as a job and return its job_id at once; visible values print to its output",
      object_schema(list(code = code, envir = envir, enclos = enclos), required = "code")
    ),
    rpc_method_descriptor(
      "job",
      paste(
        "Report a job's state, its output and conditions since the given offsets,",
        "and its error or value summary. Waits up to wait_ms for the job to finish or produce more"
      ),
      object_schema(list(
        job_id = job_id,
        wait_ms = integer_schema(
          "How long to wait for progress; keep it below the caller's RPC timeout",
          default = 0L, maximum = MAX_WAIT_MS
        ),
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
      "interrupt",
      "Interrupt one queued or running job, or every unfinished job when job_id is omitted; the session keeps its state",
      object_schema(list(job_id = job_id))
    ),
    rpc_method_descriptor(
      "jobs",
      "List recent jobs with their states",
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

endpoint_dispatch <- function(method, arguments, registry) {
  if (identical(method, "eval")) {
    only_arguments(arguments, c("code", "envir", "enclos", "wait_ms"))
    wait_ms <- integer_argument(arguments, "wait_ms", DEFAULT_EVAL_WAIT_MS, maximum = MAX_WAIT_MS)
    job <- submit_job(registry, "eval", arguments, echo = FALSE)
    deadline <- now_ms() + wait_ms
    return(rpc_defer(function() {
      if (finished(job_state(job))) return(value_reply(job))
      if (now_ms() < deadline) return(NULL)
      stop(
        "r_timeout: evaluation continues as job ", job$id,
        "; call job with job_id ", job$id, " to follow it, result to fetch its value,",
        " or interrupt to stop it"
      )
    }))
  }
  if (identical(method, "submit")) {
    only_arguments(arguments, c("code", "envir", "enclos"))
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
    limit <- if (is.null(arguments$limit)) NULL else integer_argument(arguments, "limit")
    return(value_reply(job, integer_argument(arguments, "offset", 0L), limit))
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

# The endpoint lives as long as the process that placed it. Without a parent
# PID it runs until close.
parent_keep_alive <- function() {
  parent <- suppressWarnings(as.integer(Sys.getenv("PI_DUCKNNG_PARENT_PID")))
  if (is.na(parent) || parent <= 0L) return(function() TRUE)
  function() rpc_process_alive(parent)
}

# The default listener is an ipc:// socket beside the locator, so the
# locator directory's permissions decide who may attach.
default_listen <- function(locator) {
  socket <- file.path(normalizePath(dirname(locator)), "r.ipc")
  if (nchar(socket, type = "bytes") > 100L) {
    stop("ipc socket path is too long; pass a listen URL: ", socket)
  }
  paste0("ipc://", socket)
}

main <- function(locator, listen = default_listen(locator)) {
  profile <- paste0("piducknng-endpoint-", Sys.getpid())
  jobs_directory <- file.path(dirname(locator), "jobs")
  dir.create(jobs_directory, showWarnings = FALSE, mode = "0700")
  on.exit(unlink(jobs_directory, recursive = TRUE), add = TRUE)
  mirai::daemons(1L, .compute = profile)
  on.exit(mirai::daemons(0L, .compute = profile), add = TRUE, after = FALSE)
  wait_for_daemon(profile)
  initialized <- mirai::everywhere(
    {
      assign(".piducknng_session", new.env(parent = .GlobalEnv), envir = .GlobalEnv)
      assign(".piducknng_run", runner, envir = .GlobalEnv)
    },
    runner = daemon_runner,
    .compute = profile
  )
  mirai::collect_mirai(initialized)
  registry <- new_job_registry(jobs_directory, profile)
  rpc_serve(
    listen,
    locator,
    endpoint_manifest(),
    function(method, arguments) endpoint_dispatch(method, arguments, registry),
    keep_alive = parent_keep_alive()
  )
}

args <- commandArgs(trailingOnly = TRUE)
if (!length(args) %in% 1:2) stop("usage: pi-r-endpoint.R LOCATOR_FILE [LISTEN_URL]")
do.call(main, as.list(args))
