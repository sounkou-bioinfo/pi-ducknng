DUCKNNG_WIRE_VERSION <- 1L
DUCKNNG_RPC_MANIFEST <- 0L
DUCKNNG_RPC_CALL <- 1L
DUCKNNG_RPC_RESULT <- 2L
DUCKNNG_RPC_ERROR <- 3L
DUCKNNG_RPC_FLAG_PAYLOAD_JSON <- 4L
DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM <- 8L
DUCKNNG_RPC_FLAG_SESSION_CLOSED <- 64L

wait_for_daemon <- function(profile, timeout = 10) {
  deadline <- Sys.time() + timeout
  repeat {
    if (mirai::status(.compute = profile)$connections == 1L) {
      return(invisible(NULL))
    }
    if (Sys.time() >= deadline) {
      stop("timed out waiting for the mirai daemon")
    }
    Sys.sleep(0.05)
  }
}

send_reply <- function(socket, value) {
  status <- nanonext::send(socket, value, mode = "raw", block = TRUE)
  if (!identical(status, 0L)) stop("failed to send the NNG reply")
}

little_endian_bytes <- function(value, width) {
  exact_unsigned <- length(value) == 1L && is.finite(value) && value >= 0 &&
    value == floor(value) && value <= 2^53
  if (!exact_unsigned) {
    stop("wire integer is outside the exact R numeric range")
  }
  as.raw(floor(value / 256^(0:(width - 1L))) %% 256)
}

read_little_endian <- function(value) {
  sum(as.integer(value) * 256^(seq_along(value) - 1L))
}

encode_frame <- function(type, name = "", flags = 0L, error = "",
                         payload = raw()) {
  name_raw <- charToRaw(enc2utf8(name))
  error_raw <- charToRaw(enc2utf8(error))
  c(
    as.raw(c(DUCKNNG_WIRE_VERSION, type)),
    little_endian_bytes(flags, 4L),
    little_endian_bytes(length(name_raw), 4L),
    little_endian_bytes(length(error_raw), 4L),
    little_endian_bytes(length(payload), 8L),
    name_raw,
    error_raw,
    payload
  )
}

decode_frame <- function(value) {
  if (!is.raw(value) || length(value) < 22L) stop("invalid ducknng frame")
  version <- as.integer(value[[1L]])
  type <- as.integer(value[[2L]])
  flags <- read_little_endian(value[3:6])
  name_length <- read_little_endian(value[7:10])
  error_length <- read_little_endian(value[11:14])
  payload_length <- read_little_endian(value[15:22])
  total <- 22 + name_length + error_length + payload_length
  if (version != DUCKNNG_WIRE_VERSION || total > length(value)) {
    stop("invalid ducknng frame")
  }
  if (type == DUCKNNG_RPC_CALL && error_length != 0) {
    stop("invalid ducknng call frame")
  }

  offset <- 23L
  take <- function(size) {
    if (size == 0) return(raw())
    value[seq.int(offset, length.out = size)]
  }
  name_raw <- take(name_length)
  offset <- offset + name_length
  error_raw <- take(error_length)
  offset <- offset + error_length
  payload <- take(payload_length)
  list(
    version = version,
    type = type,
    flags = flags,
    name = rawToChar(name_raw),
    error = rawToChar(error_raw),
    payload = payload
  )
}

method_descriptor <- function(name, summary, response_format, emitted_flags,
                              request_schema) {
  list(
    name = name,
    family = "r",
    summary = summary,
    transport_pattern = "reqrep",
    request_payload_format = "json",
    response_payload_format = response_format,
    response_mode = "single",
    session_behavior = "persistent_process",
    requires_auth = FALSE,
    requires_session = FALSE,
    opens_session = FALSE,
    closes_session = identical(name, "close"),
    mutates_state = TRUE,
    idempotent = FALSE,
    deprecated = FALSE,
    disabled = FALSE,
    accepted_request_flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    emitted_reply_flags = emitted_flags,
    max_request_bytes = 0L,
    max_reply_bytes = 0L,
    version_introduced = 1L,
    request_schema = request_schema,
    response_schema = NULL
  )
}

manifest_payload <- function() {
  eval_schema <- list(
    type = "object",
    required = "code",
    properties = list(
      code = list(
        type = "string",
        description = "R source evaluated in the selected persistent environment",
        examples = list(
          paste0(
            "mpg_by_cyl <- aggregate(mpg ~ cyl, ",
            "data = datasets::mtcars, FUN = mean); mpg_by_cyl"
          )
        )
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
  charToRaw(jsonlite::toJSON(
    list(
      server = list(
        name = "piducknng-r",
        version = "0.0.0.9000",
        protocol_version = DUCKNNG_WIRE_VERSION
      ),
      methods = list(
        method_descriptor(
          "eval",
          "Evaluate R code with explicit envir and enclos expressions",
          "arrow",
          DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
          eval_schema
        ),
        method_descriptor(
          "close",
          "Stop this persistent R endpoint",
          "json",
          bitwOr(
            DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
            DUCKNNG_RPC_FLAG_SESSION_CLOSED
          ),
          close_schema
        )
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  ))
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

handle_rpc_frame <- function(socket, request, profile) {
  frame <- decode_frame(request)
  if (frame$type == DUCKNNG_RPC_MANIFEST) {
    if (length(frame$payload) != 0L) stop("manifest request has a payload")
    send_reply(
      socket,
      encode_frame(
        DUCKNNG_RPC_RESULT,
        "manifest",
        DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
        payload = manifest_payload()
      )
    )
    return(TRUE)
  }
  if (frame$type != DUCKNNG_RPC_CALL) {
    stop("unsupported ducknng request type")
  }
  if (bitwAnd(
    as.integer(frame$flags),
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON
  ) == 0L) {
    stop("RPC call payload is not JSON")
  }
  arguments <- jsonlite::fromJSON(rawToChar(frame$payload), simplifyVector = FALSE)
  if (!is.list(arguments)) stop("RPC call payload must be a JSON object")

  if (identical(frame$name, "eval")) {
    value <- evaluate_call(arguments, profile)
    send_reply(
      socket,
      encode_frame(
        DUCKNNG_RPC_RESULT,
        "eval",
        DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
        payload = arrow_payload(value)
      )
    )
    return(TRUE)
  }
  if (identical(frame$name, "close")) {
    if (length(arguments) != 0L) stop("close takes no arguments")
    payload <- charToRaw('{"closed":true}')
    send_reply(
      socket,
      encode_frame(
        DUCKNNG_RPC_RESULT,
        "close",
        bitwOr(
          DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
          DUCKNNG_RPC_FLAG_SESSION_CLOSED
        ),
        payload = payload
      )
    )
    return(FALSE)
  }
  stop("unknown RPC method")
}

main <- function(locator) {
  profile <- paste0("piducknng-endpoint-", Sys.getpid())
  mirai::daemons(1L, .compute = profile)
  on.exit(mirai::daemons(0L, .compute = profile), add = TRUE)
  wait_for_daemon(profile)
  initialized <- mirai::everywhere(
    .piducknng_session <<- new.env(parent = .GlobalEnv),
    .compute = profile
  )
  mirai::collect_mirai(initialized)

  socket <- nanonext::socket("rep", listen = "tcp://127.0.0.1:0")
  on.exit(close(socket), add = TRUE)
  listener <- attr(socket, "listener")[[1L]]
  writeLines(attr(listener, "url"), locator)

  repeat {
    request <- nanonext::recv(socket, mode = "raw", block = 30000L)
    if (nanonext::is_error_value(request)) {
      stop("timed out waiting for a ducknng request")
    }
    keep_running <- tryCatch(
      {
        handle_rpc_frame(socket, request, profile)
      },
      error = function(error) {
        send_reply(
          socket,
          encode_frame(
            DUCKNNG_RPC_ERROR,
            error = conditionMessage(error),
            payload = raw()
          )
        )
        TRUE
      }
    )
    if (!keep_running) break
  }
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) stop("usage: pi-r-endpoint.R LOCATOR_FILE")
main(args[[1L]])
