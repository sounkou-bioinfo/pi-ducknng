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

# A dispatch function returns list(flags, payload, keep_running).
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
  rpc_result_reply(frame$name, dispatch(frame$name, arguments))
}

# Serves ducknng RPC on one NNG REP socket, one request at a time.
# keep_alive() is polled every poll_ms and stops the loop when FALSE.
rpc_serve <- function(listen, locator, manifest, dispatch,
                      keep_alive = function() TRUE, poll_ms = 1000L) {
  socket <- nanonext::socket("rep", listen = listen)
  on.exit(close(socket), add = TRUE)
  listener <- attr(socket, "listener")[[1L]]
  writeLines(attr(listener, "url"), locator)
  manifest_raw <- rpc_json(manifest)

  repeat {
    request <- nanonext::recv(socket, mode = "raw", block = poll_ms)
    if (nanonext::is_error_value(request)) {
      if (!keep_alive()) break
      next
    }
    outcome <- tryCatch(
      rpc_handle_request(request, manifest, manifest_raw, dispatch),
      error = rpc_error_reply
    )
    # A failed send means the requester is gone; NNG has already dropped it.
    nanonext::send(socket, outcome$frame, mode = "raw", block = TRUE)
    if (!outcome$keep_running || !keep_alive()) break
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
