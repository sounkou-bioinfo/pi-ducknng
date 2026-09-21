DUCKNNG_WIRE_VERSION <- 1L
DUCKNNG_RPC_MANIFEST <- 0L
DUCKNNG_RPC_CALL <- 1L
DUCKNNG_RPC_RESULT <- 2L
DUCKNNG_RPC_ERROR <- 3L
DUCKNNG_RPC_FLAG_PAYLOAD_JSON <- 4L
DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM <- 8L
DUCKNNG_RPC_FLAG_SESSION_CLOSED <- 64L

rpc_little_endian_bytes <- function(value, width) {
  exact_unsigned <- length(value) == 1L && is.finite(value) && value >= 0 &&
    value == floor(value) && value <= 2^53
  if (!exact_unsigned) stop("wire integer is outside the exact R numeric range")
  as.raw(floor(value / 256^(0:(width - 1L))) %% 256)
}

rpc_read_little_endian <- function(value) {
  sum(as.integer(value) * 256^(seq_along(value) - 1L))
}

rpc_encode_frame <- function(type, name = "", flags = 0L, error = "",
                             payload = raw()) {
  name_raw <- charToRaw(enc2utf8(name))
  error_raw <- charToRaw(enc2utf8(error))
  c(
    as.raw(c(DUCKNNG_WIRE_VERSION, type)),
    rpc_little_endian_bytes(flags, 4L),
    rpc_little_endian_bytes(length(name_raw), 4L),
    rpc_little_endian_bytes(length(error_raw), 4L),
    rpc_little_endian_bytes(length(payload), 8L),
    name_raw,
    error_raw,
    payload
  )
}

rpc_decode_frame <- function(value) {
  if (!is.raw(value) || length(value) < 22L) stop("invalid ducknng frame")
  version <- as.integer(value[[1L]])
  type <- as.integer(value[[2L]])
  flags <- rpc_read_little_endian(value[3:6])
  name_length <- rpc_read_little_endian(value[7:10])
  error_length <- rpc_read_little_endian(value[11:14])
  payload_length <- rpc_read_little_endian(value[15:22])
  total <- 22 + name_length + error_length + payload_length
  if (version != DUCKNNG_WIRE_VERSION || total != length(value)) {
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

rpc_method_descriptor <- function(name, summary, request_schema,
                                  response_format = "json",
                                  emitted_flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
                                  mutates_state = TRUE, idempotent = FALSE,
                                  session_behavior = "persistent_process",
                                  family = "r", max_request_bytes = 65536L) {
  list(
    name = name,
    family = family,
    summary = summary,
    transport_pattern = "reqrep",
    request_payload_format = "json",
    response_payload_format = response_format,
    response_mode = "single",
    session_behavior = session_behavior,
    requires_auth = FALSE,
    requires_session = FALSE,
    opens_session = FALSE,
    closes_session = FALSE,
    mutates_state = mutates_state,
    idempotent = idempotent,
    deprecated = FALSE,
    disabled = FALSE,
    accepted_request_flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    emitted_reply_flags = emitted_flags,
    max_request_bytes = max_request_bytes,
    max_reply_bytes = 1048576L,
    version_introduced = 1L,
    request_schema = request_schema,
    response_schema = NULL
  )
}

rpc_manifest <- function(server_name, methods, version = "0.0.0.9000",
                         server = list()) {
  list(
    server = c(
      list(
        name = server_name,
        version = version,
        protocol_version = DUCKNNG_WIRE_VERSION
      ),
      server
    ),
    methods = methods
  )
}

rpc_json <- function(value) {
  charToRaw(jsonlite::toJSON(
    value,
    auto_unbox = TRUE,
    null = "null",
    digits = NA
  ))
}

# A dispatch function returns list(flags, payload, keep_running) or, to answer
# later from the same NNG context, list(deferred = TRUE, deadline, retry).
# retry() returns a reply when the wait is satisfied and NULL otherwise; the
# serve loop calls it after each dispatched request and once the deadline
# passes, with final = TRUE.
rpc_deferred <- function(deadline_ms, retry) {
  list(deferred = TRUE, deadline_ms = deadline_ms, retry = retry)
}

rpc_error_reply <- function(error) {
  list(
    frame = rpc_encode_frame(
      DUCKNNG_RPC_ERROR,
      error = conditionMessage(error),
      payload = raw()
    ),
    keep_running = TRUE
  )
}

rpc_result_reply <- function(name, reply) {
  list(
    frame = rpc_encode_frame(
      DUCKNNG_RPC_RESULT,
      name,
      reply$flags,
      payload = reply$payload
    ),
    keep_running = !identical(reply$keep_running, FALSE)
  )
}

rpc_handle_request <- function(request, manifest, manifest_raw, dispatch) {
  frame <- rpc_decode_frame(request)
  if (frame$type == DUCKNNG_RPC_MANIFEST) {
    if (length(frame$payload) != 0L) stop("manifest request has a payload")
    return(list(
      frame = rpc_encode_frame(
        DUCKNNG_RPC_RESULT,
        "manifest",
        DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
        payload = manifest_raw
      ),
      keep_running = TRUE
    ))
  }
  if (frame$type != DUCKNNG_RPC_CALL) {
    stop("unsupported ducknng request type")
  }
  descriptor <- Find(function(method) identical(method$name, frame$name),
                     manifest$methods)
  if (is.null(descriptor)) stop("unknown_method: ", frame$name)
  if (length(frame$payload) > descriptor$max_request_bytes) {
    stop(
      "request_too_large: ", frame$name, " accepts at most ",
      descriptor$max_request_bytes, " payload bytes"
    )
  }
  if (bitwAnd(as.integer(frame$flags), DUCKNNG_RPC_FLAG_PAYLOAD_JSON) == 0L) {
    stop("RPC call payload is not JSON")
  }
  arguments <- jsonlite::fromJSON(rawToChar(frame$payload), simplifyVector = FALSE)
  if (!is.list(arguments) || is.null(names(arguments))) {
    stop("RPC call payload must be a JSON object")
  }
  reply <- dispatch(frame$name, arguments)
  if (isTRUE(reply$deferred)) {
    reply$name <- frame$name
    return(reply)
  }
  rpc_result_reply(frame$name, reply)
}

rpc_now_ms <- function() {
  floor(as.numeric(Sys.time()) * 1000)
}

# Serves ducknng RPC on one NNG REP socket. Each context holds one outstanding
# request, so a deferred reply occupies its context without blocking the
# others. keep_alive() is polled every poll_ms and stops the loop when FALSE.
rpc_serve <- function(listen, locator, manifest, dispatch, contexts = 1L,
                      keep_alive = function() TRUE, poll_ms = 1000L) {
  socket <- nanonext::socket("rep", listen = listen)
  on.exit(close(socket), add = TRUE)
  listener <- attr(socket, "listener")[[1L]]
  writeLines(attr(listener, "url"), locator)

  manifest_raw <- rpc_json(manifest)
  signal <- nanonext::cv()
  handles <- lapply(seq_len(contexts), function(index) nanonext::context(socket))
  receive <- function(index) {
    nanonext::recv_aio(handles[[index]], mode = "raw", cv = signal)
  }
  pending <- lapply(seq_len(contexts), receive)
  parked <- vector("list", contexts)

  # A failed send means the requester is gone; NNG has already dropped it.
  reply <- function(index, outcome) {
    nanonext::send(handles[[index]], outcome$frame, mode = "raw", block = TRUE)
    pending[[index]] <<- receive(index)
    invisible(NULL)
  }

  settle <- function(index, final) {
    waiting <- parked[[index]]
    outcome <- tryCatch(
      {
        value <- waiting$retry(final)
        if (is.null(value)) NULL else rpc_result_reply(waiting$name, value)
      },
      error = rpc_error_reply
    )
    if (is.null(outcome)) return(TRUE)
    parked[index] <<- list(NULL)
    reply(index, outcome)
    outcome$keep_running
  }

  repeat {
    nanonext::until(signal, poll_ms)
    running <- TRUE
    dispatched <- FALSE
    for (index in seq_len(contexts)) {
      aio <- pending[[index]]
      if (is.null(aio) || nanonext::unresolved(aio)) next
      request <- aio$data
      pending[index] <- list(NULL)
      if (nanonext::is_error_value(request)) {
        pending[[index]] <- receive(index)
        next
      }
      outcome <- tryCatch(
        rpc_handle_request(request, manifest, manifest_raw, dispatch),
        error = rpc_error_reply
      )
      dispatched <- TRUE
      if (isTRUE(outcome$deferred)) {
        parked[[index]] <- outcome
        next
      }
      reply(index, outcome)
      running <- running && outcome$keep_running
    }
    now <- rpc_now_ms()
    for (index in seq_len(contexts)) {
      waiting <- parked[[index]]
      if (is.null(waiting)) next
      final <- now >= waiting$deadline_ms
      if (dispatched || final) running <- settle(index, final) && running
    }
    if (!running || !keep_alive()) break
  }
}

rpc_process_alive <- function(pid) {
  isTRUE(suppressWarnings(tools::pskill(pid, 0L)))
}

# Client helper for R callers and executable documentation.
rpc_call <- function(url, method, arguments = structure(list(), names = character()),
                     timeout_ms = 5000L) {
  socket <- nanonext::socket("req", dial = url)
  on.exit(close(socket), add = TRUE)
  frame <- rpc_encode_frame(
    DUCKNNG_RPC_CALL,
    method,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    payload = rpc_json(arguments)
  )
  status <- nanonext::send(socket, frame, mode = "raw", block = timeout_ms)
  if (!identical(status, 0L)) stop("failed to send the ducknng request")
  reply <- nanonext::recv(socket, mode = "raw", block = timeout_ms)
  if (nanonext::is_error_value(reply)) stop("timed out waiting for ", method)
  decoded <- rpc_decode_frame(reply)
  if (decoded$type == DUCKNNG_RPC_ERROR) stop(decoded$error, call. = FALSE)
  jsonlite::fromJSON(rawToChar(decoded$payload), simplifyVector = FALSE)
}
