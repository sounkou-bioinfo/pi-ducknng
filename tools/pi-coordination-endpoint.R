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

coordination_manifest <- function() {
  registration_id <- string_property("Opaque live registration identifier", 64L)
  methods <- list(
    rpc_method_descriptor(
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
      ),
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "heartbeat",
      "Renew one registration lease and replace its bounded status",
      coordination_schema(
        "registration_id",
        list(
          registration_id = registration_id,
          status = list(type = "object", description = "Bounded presence status")
        )
      ),
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "list_agents",
      "List live agent instances in the caller's project",
      coordination_schema(
        "registration_id",
        list(registration_id = registration_id)
      ),
      mutates_state = FALSE,
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "send",
      "Durably enqueue one idempotent message to a stable agent mailbox",
      coordination_schema(
        c("registration_id", "recipient_agent_id", "idempotency_key", "content"),
        list(
          registration_id = registration_id,
          recipient_agent_id = string_property("Stable mailbox owner", 128L),
          idempotency_key = string_property("Sender-scoped message key", 256L),
          content = string_property("Message content", 65536L),
          content_type = string_property("Message media type", 128L)
        )
      ),
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "receive",
      "Lease a bounded ordered batch from the registered agent mailbox",
      coordination_schema(
        "registration_id",
        list(
          registration_id = registration_id,
          limit = integer_property("Maximum messages", 8L, 1L, 32L),
          visibility_timeout_ms = integer_property(
            "Delivery lease duration", 30000L, 1000L, 300000L
          )
        )
      )
    ),
    rpc_method_descriptor(
      "ack",
      "Idempotently acknowledge a current delivery receipt",
      coordination_schema(
        c("registration_id", "receipt_token"),
        list(
          registration_id = registration_id,
          receipt_token = string_property("Delivery lease receipt", 64L),
          delivery_ref = string_property("Adapter-owned admission evidence", 256L)
        )
      ),
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "reserve",
      "Acquire or renew an advisory project-scoped resource lease",
      coordination_schema(
        c("registration_id", "resource"),
        list(
          registration_id = registration_id,
          resource = string_property("resource: identifier or canonical file:/// URI", 4096L),
          operation_key = string_property("Idempotent acquisition key", 256L),
          lease_id = string_property("Existing lease to renew", 64L),
          ttl_ms = integer_property("Reservation lease duration", 30000L, 5000L, 300000L)
        )
      ),
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "release",
      "Idempotently release an advisory resource lease owned by the caller",
      coordination_schema(
        c("registration_id", "lease_id"),
        list(
          registration_id = registration_id,
          lease_id = string_property("Reservation lease identifier", 64L)
        )
      ),
      idempotent = TRUE
    ),
    rpc_method_descriptor(
      "unregister",
      "Expire one agent instance without deleting durable mail",
      coordination_schema(
        "registration_id",
        list(registration_id = registration_id)
      ),
      idempotent = TRUE
    )
  )
  rpc_manifest_raw("piducknng-coordination", methods)
}

coordination_reply <- function(store, method, arguments) {
  value <- coordination_dispatch(store, method, arguments)
  list(
    flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    payload = charToRaw(jsonlite::toJSON(
      value,
      auto_unbox = TRUE,
      null = "null",
      digits = 16
    )),
    keep_running = TRUE
  )
}

main <- function(locator, database) {
  previous_umask <- Sys.umask("0077")
  on.exit(Sys.umask(previous_umask), add = TRUE)
  store <- coordination_open(database)
  on.exit(coordination_close(store), add = TRUE)
  rpc_serve(
    locator,
    coordination_manifest(),
    function(method, arguments) coordination_reply(store, method, arguments)
  )
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L) {
  stop("usage: pi-coordination-endpoint.R LOCATOR_FILE DATABASE_FILE")
}
main(args[[1L]], args[[2L]])
