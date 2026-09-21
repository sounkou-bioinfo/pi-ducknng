script_argument <- grep("^--file=", commandArgs(FALSE), value = TRUE)
if (length(script_argument) != 1L) stop("cannot locate pi-coordination-endpoint.R")
script_directory <- dirname(normalizePath(sub("^--file=", "", script_argument)))
source(file.path(script_directory, "ducknng-rpc.R"), local = TRUE)
source(
  file.path(dirname(script_directory), "R", "coordination.R"),
  local = TRUE
)

coordination_schema <- function(required, properties) {
  list(
    type = "object",
    required = required,
    properties = properties,
    additionalProperties = FALSE
  )
}

string_property <- function(description, max_length = 256L) {
  list(type = "string", description = description, maxLength = max_length)
}

integer_property <- function(description, default, minimum, maximum) {
  list(
    type = "integer",
    description = description,
    default = default,
    minimum = minimum,
    maximum = maximum
  )
}

coordination_method <- function(name, summary, schema, mutates_state = TRUE,
                                idempotent = TRUE, max_request_bytes = 16384L) {
  rpc_method_descriptor(
    name, summary, schema,
    mutates_state = mutates_state,
    idempotent = idempotent,
    session_behavior = "endpoint_store",
    family = "coordination",
    max_request_bytes = max_request_bytes
  )
}

coordination_manifest <- function(listen) {
  registration_id <- string_property("Opaque live registration identifier", 64L)
  resource <- string_property("resource: identifier or canonical file:/// URI", 4096L)
  methods <- list(
    coordination_method(
      "register",
      "Create or resume one project-scoped agent instance lease",
      coordination_schema(
        c("project_id", "agent_id", "instance_id", "operation_key"),
        list(
          project_id = string_property("Stable project scope", 128L),
          agent_id = string_property("Stable mailbox owner", 128L),
          instance_id = string_property("Pi session or harness instance", 256L),
          operation_key = string_property("Idempotent registration attempt", 256L),
          display_name = string_property("Human-visible agent name", 256L),
          adapter_kind = string_property("Pi integration boundary", 64L),
          delivery_capability = string_property(
            "Evidence-backed acknowledgement strength", 64L
          ),
          ttl_ms = integer_property("Presence lease duration", 30000L, 5000L, 300000L)
        )
      )
    ),
    coordination_method(
      "heartbeat",
      "Renew one registration lease and replace its bounded status",
      coordination_schema(
        "registration_id",
        list(
          registration_id = registration_id,
          status = list(type = "object", description = "Presence status, at most 8192 bytes")
        )
      )
    ),
    coordination_method(
      "list_agents",
      "List live agent instances in the caller's project",
      coordination_schema("registration_id", list(registration_id = registration_id)),
      mutates_state = FALSE
    ),
    coordination_method(
      "send",
      "Durably enqueue one idempotent message to a stable agent mailbox",
      coordination_schema(
        c("registration_id", "recipient_agent_id", "idempotency_key", "content"),
        list(
          registration_id = registration_id,
          recipient_agent_id = string_property("Stable mailbox owner", 128L),
          idempotency_key = string_property("Sender-scoped message key", 256L),
          content = string_property(
            "UTF-8 message text; tab and line breaks allowed", 65536L
          ),
          content_type = string_property("Message media type", 128L),
          ttl_ms = integer_property(
            "Time before undelivered mail becomes a dead letter",
            604800000L, 60000L, 2592000000
          )
        )
      ),
      max_request_bytes = 262144L
    ),
    coordination_method(
      "receive",
      "Lease a bounded ordered batch from the registered agent mailbox",
      coordination_schema(
        "registration_id",
        list(
          registration_id = registration_id,
          limit = integer_property("Maximum messages", 8L, 1L, 32L),
          visibility_timeout_ms = integer_property(
            "Delivery lease duration", 30000L, 1000L, 300000L
          ),
          wait_ms = integer_property(
            "Hold the reply until mail arrives or this many milliseconds pass",
            0L, 0L, 25000L
          )
        )
      ),
      idempotent = FALSE
    ),
    coordination_method(
      "ack",
      "Idempotently acknowledge a current delivery receipt",
      coordination_schema(
        c("registration_id", "receipt_token"),
        list(
          registration_id = registration_id,
          receipt_token = string_property("Delivery lease receipt", 64L),
          delivery_ref = string_property("Adapter-owned admission evidence", 256L)
        )
      )
    ),
    coordination_method(
      "reserve",
      "Acquire or renew an advisory project-scoped resource lease",
      coordination_schema(
        c("registration_id", "resource"),
        list(
          registration_id = registration_id,
          resource = resource,
          operation_key = string_property("Idempotent acquisition key", 256L),
          lease_id = string_property("Existing lease to renew", 64L),
          ttl_ms = integer_property("Reservation lease duration", 30000L, 5000L, 300000L)
        )
      )
    ),
    coordination_method(
      "list_reservations",
      "List active reservations, optionally only those conflicting with a resource",
      coordination_schema(
        "registration_id",
        list(registration_id = registration_id, resource = resource)
      ),
      mutates_state = FALSE
    ),
    coordination_method(
      "release",
      "Idempotently release an advisory resource lease owned by the caller",
      coordination_schema(
        c("registration_id", "lease_id"),
        list(
          registration_id = registration_id,
          lease_id = string_property("Reservation lease identifier", 64L)
        )
      )
    ),
    coordination_method(
      "unregister",
      "Expire one agent instance without deleting durable mail",
      coordination_schema("registration_id", list(registration_id = registration_id))
    )
  )
  rpc_manifest(
    "piducknng-coordination",
    methods,
    server = list(
      listen_url = listen,
      error_format = "<code>: <detail>",
      error_codes = COORDINATION_ERROR_CODES
    )
  )
}

coordination_json_reply <- function(value) {
  list(flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON, payload = rpc_json(value))
}

# An empty receive with wait_ms is parked on its NNG context and retried after
# each later request, which is when new mail can have been committed.
coordination_reply <- function(store, method, arguments) {
  value <- coordination_dispatch(store, method, arguments)
  wait_ms <- if (identical(method, "receive")) arguments$wait_ms else NULL
  if (length(value$messages) > 0L || is.null(wait_ms) || wait_ms <= 0) {
    return(coordination_json_reply(value))
  }
  rpc_deferred(
    rpc_now_ms() + wait_ms,
    function(final) {
      retried <- coordination_dispatch(store, method, arguments)
      if (length(retried$messages) > 0L || final) {
        coordination_json_reply(retried)
      } else {
        NULL
      }
    }
  )
}

default_listen_url <- function(database) {
  socket <- paste0(tools::file_path_sans_ext(normalizePath(database, mustWork = FALSE)), ".ipc")
  if (nchar(socket, type = "bytes") > 100L) {
    stop("ipc socket path is too long; pass LISTEN_URL explicitly: ", socket)
  }
  paste0("ipc://", socket)
}

main <- function(locator, database, listen) {
  previous_umask <- Sys.umask("0077")
  on.exit(Sys.umask(previous_umask), add = TRUE)
  store <- coordination_open(database)
  on.exit(coordination_close(store), add = TRUE)
  rpc_serve(
    listen,
    locator,
    coordination_manifest(listen),
    function(method, arguments) coordination_reply(store, method, arguments),
    contexts = 64L,
    poll_ms = 250L
  )
}

args <- commandArgs(trailingOnly = TRUE)
if (!length(args) %in% 2:3) {
  stop("usage: pi-coordination-endpoint.R LOCATOR_FILE DATABASE_FILE [LISTEN_URL]")
}
main(
  args[[1L]],
  args[[2L]],
  if (length(args) == 3L) args[[3L]] else default_listen_url(args[[2L]])
)
