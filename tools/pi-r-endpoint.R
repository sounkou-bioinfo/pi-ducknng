script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
if (length(script_argument) != 1L) stop("cannot locate pi-r-endpoint.R")
script_directory <- dirname(normalizePath(sub("^--file=", "", script_argument)))
source(file.path(script_directory, "ducknng-rpc.R"), local = TRUE)

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

eval_manifest <- function() {
  eval_schema <- list(
    type = "object",
    required = "code",
    properties = list(
      code = list(
        type = "string",
        description = "R source evaluated in the selected persistent environment",
        examples = list(paste0(
          "mpg_by_cyl <- aggregate(mpg ~ cyl, ",
          "data = datasets::mtcars, FUN = mean); mpg_by_cyl"
        ))
      ),
      envir = list(
        type = "string",
        description = "R expression resolving to the evaluation environment",
        default = ".piducknng_session",
        examples = list(".piducknng_session", "analysis")
      ),
      enclos = list(
        type = "string",
        description = "R expression resolving to eval()'s enclosure",
        default = "baseenv()",
        examples = list("baseenv()", "globalenv()")
      )
    ),
    additionalProperties = FALSE
  )
  close_schema <- list(
    type = "object",
    properties = list(),
    additionalProperties = FALSE
  )
  eval_method <- rpc_method_descriptor(
    "eval",
    "Evaluate R code with explicit envir and enclos expressions",
    eval_schema,
    response_format = "arrow",
    emitted_flags = DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM
  )
  close_method <- rpc_method_descriptor(
    "close",
    "Stop this persistent R endpoint",
    close_schema,
    emitted_flags = bitwOr(
      DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
      DUCKNNG_RPC_FLAG_SESSION_CLOSED
    )
  )
  close_method$closes_session <- TRUE
  rpc_manifest("piducknng-r", list(eval_method, close_method))
}

arrow_payload <- function(value) {
  table <- if (is.data.frame(value)) {
    value
  } else if (is.atomic(value) && is.null(dim(value)) && length(value) > 0L) {
    data.frame(value = value, check.names = FALSE)
  } else {
    stop("R value is not representable by the current Arrow IPC result codec")
  }
  path <- tempfile(fileext = ".arrows")
  on.exit(unlink(path), add = TRUE)
  nanoarrow::write_nanoarrow(table, path)
  readBin(path, "raw", n = file.info(path)$size)
}

evaluate_call <- function(arguments, profile) {
  code <- arguments$code
  envir <- arguments$envir
  enclos <- arguments$enclos
  if (is.null(envir)) envir <- ".piducknng_session"
  if (is.null(enclos)) enclos <- "baseenv()"
  valid <- is.character(code) && length(code) == 1L &&
    is.character(envir) && length(envir) == 1L &&
    is.character(enclos) && length(enclos) == 1L
  if (!valid) stop("invalid eval arguments")

  value <- mirai::mirai(
    {
      scope <- .piducknng_session
      target <- eval(parse(text = .envir), envir = scope, enclos = .GlobalEnv)
      enclosure <- eval(parse(text = .enclos), envir = scope, enclos = .GlobalEnv)
      eval(parse(text = .code), envir = target, enclos = enclosure)
    },
    .code = code,
    .envir = envir,
    .enclos = enclos,
    .compute = profile
  )[]
  if (inherits(value, "miraiError")) stop(as.character(value))
  value
}

endpoint_dispatch <- function(method, arguments, profile) {
  if (identical(method, "eval")) {
    return(list(
      flags = DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
      payload = arrow_payload(evaluate_call(arguments, profile)),
      keep_running = TRUE
    ))
  }
  if (identical(method, "close")) {
    if (length(arguments) != 0L) stop("close takes no arguments")
    return(list(
      flags = bitwOr(
        DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
        DUCKNNG_RPC_FLAG_SESSION_CLOSED
      ),
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

main <- function(locator) {
  profile <- paste0("piducknng-endpoint-", Sys.getpid())
  mirai::daemons(1L, .compute = profile)
  on.exit(mirai::daemons(0L, .compute = profile), add = TRUE)
  wait_for_daemon(profile)
  initialized <- mirai::everywhere(
    assign(
      ".piducknng_session",
      new.env(parent = .GlobalEnv),
      envir = .GlobalEnv
    ),
    .compute = profile
  )
  mirai::collect_mirai(initialized)
  rpc_serve(
    "tcp://127.0.0.1:0",
    locator,
    eval_manifest(),
    function(method, arguments) endpoint_dispatch(method, arguments, profile),
    keep_alive = parent_keep_alive()
  )
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) stop("usage: pi-r-endpoint.R LOCATOR_FILE")
main(args[[1L]])
