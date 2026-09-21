coordination_now_ms <- function() {
  floor(as.numeric(Sys.time()) * 1000)
}

coordination_open <- function(path, now = coordination_now_ms,
                              acknowledged_retention_ms = 2592000000,
                              max_pending_per_mailbox = 10000L) {
  path <- coordination_string(list(path = path), "path", max_bytes = 4096L)
  acknowledged_retention_ms <- coordination_integer(
    list(retention = acknowledged_retention_ms),
    "retention", 2592000000, 86400000, 31536000000
  )
  max_pending_per_mailbox <- coordination_integer(
    list(maximum = max_pending_per_mailbox),
    "maximum", 10000, 1, 1000000
  )
  if (!identical(path, ":memory:")) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  }

  store <- new.env(parent = emptyenv())
  store$connection <- DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE),
    dbdir = path,
    read_only = FALSE,
    bigint = "numeric"
  )
  store$now <- now
  store$acknowledged_retention_ms <- acknowledged_retention_ms
  store$max_pending_per_mailbox <- as.numeric(max_pending_per_mailbox)

  statements <- c(
    paste(
      "CREATE TABLE IF NOT EXISTS coordination_meta (",
      "singleton BOOLEAN PRIMARY KEY, schema_version INTEGER NOT NULL,",
      "CHECK (singleton))"
    ),
    paste(
      "CREATE TABLE IF NOT EXISTS coordination_agents (",
      "project_id VARCHAR NOT NULL, agent_id VARCHAR NOT NULL,",
      "instance_id VARCHAR NOT NULL, registration_id VARCHAR NOT NULL,",
      "operation_key VARCHAR NOT NULL, display_name VARCHAR NOT NULL,",
      "adapter_kind VARCHAR NOT NULL, delivery_capability VARCHAR NOT NULL,",
      "status_json VARCHAR NOT NULL, registered_at_ms BIGINT NOT NULL,",
      "heartbeat_at_ms BIGINT NOT NULL, expires_at_ms BIGINT NOT NULL,",
      "unregistered_at_ms BIGINT, PRIMARY KEY (project_id, instance_id),",
      "UNIQUE (registration_id))"
    ),
    paste(
      "CREATE TABLE IF NOT EXISTS coordination_mailbox_sequences (",
      "project_id VARCHAR NOT NULL, agent_id VARCHAR NOT NULL,",
      "next_sequence BIGINT NOT NULL, PRIMARY KEY (project_id, agent_id))"
    ),
    paste(
      "CREATE TABLE IF NOT EXISTS coordination_messages (",
      "message_id VARCHAR PRIMARY KEY, project_id VARCHAR NOT NULL,",
      "sender_agent_id VARCHAR NOT NULL, recipient_agent_id VARCHAR NOT NULL,",
      "idempotency_key VARCHAR NOT NULL, sequence_number BIGINT NOT NULL,",
      "content VARCHAR NOT NULL, content_type VARCHAR NOT NULL,",
      "created_at_ms BIGINT NOT NULL, state VARCHAR NOT NULL,",
      "receipt_token VARCHAR, lease_expires_at_ms BIGINT,",
      "acked_at_ms BIGINT, delivery_ref VARCHAR,",
      "delivery_capability VARCHAR, ack_instance_id VARCHAR,",
      "UNIQUE (project_id, sender_agent_id, idempotency_key))"
    ),
    paste(
      "CREATE TABLE IF NOT EXISTS coordination_fencing_counters (",
      "project_id VARCHAR PRIMARY KEY, next_value BIGINT NOT NULL)"
    ),
    paste(
      "CREATE TABLE IF NOT EXISTS coordination_reservations (",
      "project_id VARCHAR NOT NULL, owner_agent_id VARCHAR NOT NULL,",
      "owner_instance_id VARCHAR NOT NULL, operation_key VARCHAR NOT NULL,",
      "resource VARCHAR NOT NULL, lease_id VARCHAR NOT NULL,",
      "fencing_value BIGINT NOT NULL, created_at_ms BIGINT NOT NULL,",
      "expires_at_ms BIGINT NOT NULL, released_at_ms BIGINT,",
      "PRIMARY KEY (project_id, owner_instance_id, operation_key),",
      "UNIQUE (lease_id))"
    )
  )
  DBI::dbWithTransaction(store$connection, {
    for (statement in statements) {
      DBI::dbExecute(store$connection, statement)
    }
    DBI::dbExecute(
      store$connection,
      paste(
        "INSERT INTO coordination_meta VALUES (TRUE, 1)",
        "ON CONFLICT (singleton) DO NOTHING"
      )
    )
    versions <- DBI::dbGetQuery(
      store$connection,
      "SELECT schema_version FROM coordination_meta WHERE singleton"
    )
    if (nrow(versions) != 1L || versions$schema_version[[1L]] != 1L) {
      stop("unsupported coordination database schema")
    }
  })
  store
}

coordination_close <- function(store) {
  connection <- store$connection
  if (DBI::dbIsValid(connection)) {
    DBI::dbDisconnect(connection, shutdown = TRUE)
  }
  invisible(NULL)
}

coordination_string <- function(arguments, name, required = TRUE,
                                default = NULL, max_bytes = 256L) {
  value <- arguments[[name]]
  if (is.null(value)) {
    if (required) stop(name, " is required")
    return(default)
  }
  valid <- is.character(value) && length(value) == 1L && !is.na(value) &&
    nzchar(value) && !grepl("[[:cntrl:]]", value) &&
    nchar(value, type = "bytes") <= max_bytes
  if (!valid) stop(name, " must be one bounded non-empty string")
  enc2utf8(value)
}

coordination_integer <- function(arguments, name, default, minimum, maximum) {
  value <- arguments[[name]]
  if (is.null(value)) return(default)
  valid <- is.numeric(value) && length(value) == 1L && is.finite(value) &&
    value == floor(value) && value >= minimum && value <= maximum
  if (!valid) stop(name, " is outside its allowed integer range")
  as.numeric(value)
}

coordination_arguments <- function(arguments, allowed) {
  if (!is.list(arguments) || is.null(names(arguments))) {
    stop("coordination arguments must be a JSON object")
  }
  unexpected <- setdiff(names(arguments), allowed)
  if (length(unexpected) > 0L) {
    stop("unexpected coordination argument: ", unexpected[[1L]])
  }
  arguments
}

coordination_uuid <- function(connection) {
  DBI::dbGetQuery(connection, "SELECT uuid()::VARCHAR AS id")$id[[1L]]
}

coordination_maintain <- function(store, now) {
  cutoff <- now - store$acknowledged_retention_ms
  DBI::dbWithTransaction(store$connection, {
    DBI::dbExecute(
      store$connection,
      paste(
        "DELETE FROM coordination_messages WHERE state = 'acked'",
        "AND acked_at_ms < ?"
      ),
      params = list(cutoff)
    )
    DBI::dbExecute(
      store$connection,
      paste(
        "DELETE FROM coordination_reservations WHERE",
        "(released_at_ms IS NOT NULL AND released_at_ms < ?)",
        "OR (released_at_ms IS NULL AND expires_at_ms < ?)"
      ),
      params = list(cutoff, cutoff)
    )
    DBI::dbExecute(
      store$connection,
      paste(
        "DELETE FROM coordination_agents WHERE",
        "(unregistered_at_ms IS NOT NULL AND unregistered_at_ms < ?)",
        "OR (unregistered_at_ms IS NULL AND expires_at_ms < ?)"
      ),
      params = list(cutoff, cutoff)
    )
  })
  invisible(NULL)
}

coordination_registration <- function(store, registration_id, now) {
  row <- DBI::dbGetQuery(
    store$connection,
    paste(
      "SELECT project_id, agent_id, instance_id, delivery_capability,",
      "expires_at_ms, unregistered_at_ms FROM coordination_agents",
      "WHERE registration_id = ?"
    ),
    params = list(registration_id)
  )
  if (nrow(row) != 1L || !is.na(row$unregistered_at_ms[[1L]]) ||
      row$expires_at_ms[[1L]] <= now) {
    stop("registration is missing or expired")
  }
  row[1L, , drop = FALSE]
}

coordination_register <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, c(
    "project_id", "agent_id", "instance_id", "operation_key",
    "display_name", "adapter_kind", "delivery_capability", "ttl_ms"
  ))
  project_id <- coordination_string(arguments, "project_id", max_bytes = 128L)
  agent_id <- coordination_string(arguments, "agent_id", max_bytes = 128L)
  instance_id <- coordination_string(arguments, "instance_id", max_bytes = 256L)
  operation_key <- coordination_string(arguments, "operation_key", max_bytes = 256L)
  display_name <- coordination_string(
    arguments, "display_name", required = FALSE, default = agent_id,
    max_bytes = 256L
  )
  adapter_kind <- coordination_string(
    arguments, "adapter_kind", required = FALSE,
    default = "unspecified", max_bytes = 64L
  )
  delivery_capability <- coordination_string(
    arguments, "delivery_capability", required = FALSE,
    default = "unspecified", max_bytes = 64L
  )
  ttl_ms <- coordination_integer(arguments, "ttl_ms", 30000, 5000, 300000)
  now <- store$now()
  expires <- now + ttl_ms

  DBI::dbWithTransaction(store$connection, {
    existing <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT agent_id, registration_id, operation_key, registered_at_ms,",
        "expires_at_ms, unregistered_at_ms FROM coordination_agents",
        "WHERE project_id = ? AND instance_id = ?"
      ),
      params = list(project_id, instance_id)
    )
    if (nrow(existing) == 1L && existing$agent_id[[1L]] != agent_id) {
      stop("instance_id is already bound to another agent_id")
    }
    reuse <- nrow(existing) == 1L &&
      identical(existing$operation_key[[1L]], operation_key) &&
      is.na(existing$unregistered_at_ms[[1L]]) &&
      existing$expires_at_ms[[1L]] > now
    registration_id <- if (reuse) {
      existing$registration_id[[1L]]
    } else {
      coordination_uuid(store$connection)
    }
    registered_at <- if (reuse) existing$registered_at_ms[[1L]] else now

    DBI::dbExecute(
      store$connection,
      paste(
        "INSERT INTO coordination_agents VALUES (?, ?, ?, ?, ?, ?, ?, ?,",
        "'{}', ?, ?, ?, NULL) ON CONFLICT (project_id, instance_id) DO UPDATE SET",
        "agent_id = excluded.agent_id, registration_id = excluded.registration_id,",
        "operation_key = excluded.operation_key, display_name = excluded.display_name,",
        "adapter_kind = excluded.adapter_kind,",
        "delivery_capability = excluded.delivery_capability,",
        "status_json = '{}', registered_at_ms = excluded.registered_at_ms,",
        "heartbeat_at_ms = excluded.heartbeat_at_ms,",
        "expires_at_ms = excluded.expires_at_ms, unregistered_at_ms = NULL"
      ),
      params = list(
        project_id, agent_id, instance_id, registration_id, operation_key,
        display_name, adapter_kind, delivery_capability,
        registered_at, now, expires
      )
    )
    list(
      registration_id = registration_id,
      project_id = project_id,
      agent_id = agent_id,
      instance_id = instance_id,
      server_time_ms = now,
      expires_at_ms = expires,
      heartbeat_interval_ms = floor(ttl_ms / 3),
      delivery_capability = delivery_capability,
      replayed = reuse
    )
  })
}

coordination_heartbeat <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, c("registration_id", "status"))
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  status <- arguments$status
  if (is.null(status)) status <- structure(list(), names = character())
  if (!is.list(status) || is.null(names(status))) {
    stop("status must be a JSON object")
  }
  status_json <- jsonlite::toJSON(status, auto_unbox = TRUE, null = "null")
  if (nchar(status_json, type = "bytes") > 8192L) stop("status is too large")
  now <- store$now()
  registration <- coordination_registration(store, registration_id, now)
  previous_ttl <- DBI::dbGetQuery(
    store$connection,
    paste(
      "SELECT expires_at_ms - heartbeat_at_ms AS ttl_ms",
      "FROM coordination_agents WHERE registration_id = ?"
    ),
    params = list(registration_id)
  )$ttl_ms[[1L]]
  ttl_ms <- max(5000, min(300000, previous_ttl))
  expires <- now + ttl_ms
  DBI::dbExecute(
    store$connection,
    paste(
      "UPDATE coordination_agents SET status_json = ?, heartbeat_at_ms = ?,",
      "expires_at_ms = ? WHERE registration_id = ?"
    ),
    params = list(status_json, now, expires, registration_id)
  )
  list(
    registration_id = registration_id,
    server_time_ms = now,
    expires_at_ms = expires,
    project_id = registration$project_id[[1L]],
    agent_id = registration$agent_id[[1L]]
  )
}

coordination_list_agents <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, "registration_id")
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  now <- store$now()
  registration <- coordination_registration(store, registration_id, now)
  rows <- DBI::dbGetQuery(
    store$connection,
    paste(
      "SELECT agent_id, instance_id, display_name, adapter_kind,",
      "delivery_capability, status_json, registered_at_ms, heartbeat_at_ms,",
      "expires_at_ms FROM coordination_agents WHERE project_id = ?",
      "AND unregistered_at_ms IS NULL AND expires_at_ms > ?",
      "ORDER BY agent_id, instance_id"
    ),
    params = list(registration$project_id[[1L]], now)
  )
  agents <- lapply(seq_len(nrow(rows)), function(index) {
    list(
      agent_id = rows$agent_id[[index]],
      instance_id = rows$instance_id[[index]],
      display_name = rows$display_name[[index]],
      adapter_kind = rows$adapter_kind[[index]],
      delivery_capability = rows$delivery_capability[[index]],
      status = jsonlite::fromJSON(rows$status_json[[index]], simplifyVector = FALSE),
      registered_at_ms = rows$registered_at_ms[[index]],
      heartbeat_at_ms = rows$heartbeat_at_ms[[index]],
      expires_at_ms = rows$expires_at_ms[[index]]
    )
  })
  list(server_time_ms = now, agents = agents)
}

coordination_send <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, c(
    "registration_id", "recipient_agent_id", "idempotency_key",
    "content", "content_type"
  ))
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  recipient <- coordination_string(
    arguments, "recipient_agent_id", max_bytes = 128L
  )
  idempotency_key <- coordination_string(
    arguments, "idempotency_key", max_bytes = 256L
  )
  content <- coordination_string(
    arguments, "content", max_bytes = 65536L
  )
  content_type <- coordination_string(
    arguments, "content_type", required = FALSE,
    default = "text/plain", max_bytes = 128L
  )
  now <- store$now()
  registration <- coordination_registration(store, registration_id, now)
  project <- registration$project_id[[1L]]
  sender <- registration$agent_id[[1L]]

  DBI::dbWithTransaction(store$connection, (function() {
    existing <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT message_id, recipient_agent_id, content, content_type,",
        "sequence_number, created_at_ms FROM coordination_messages",
        "WHERE project_id = ? AND sender_agent_id = ? AND idempotency_key = ?"
      ),
      params = list(project, sender, idempotency_key)
    )
    if (nrow(existing) == 1L) {
      same <- identical(existing$recipient_agent_id[[1L]], recipient) &&
        identical(existing$content[[1L]], content) &&
        identical(existing$content_type[[1L]], content_type)
      if (!same) stop("idempotency_key was already used for another message")
      return(list(
        message_id = existing$message_id[[1L]],
        recipient_agent_id = recipient,
        sequence_number = existing$sequence_number[[1L]],
        created_at_ms = existing$created_at_ms[[1L]],
        replayed = TRUE
      ))
    }

    pending <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT count(*) AS count FROM coordination_messages",
        "WHERE project_id = ? AND recipient_agent_id = ? AND state != 'acked'"
      ),
      params = list(project, recipient)
    )$count[[1L]]
    if (pending >= store$max_pending_per_mailbox) {
      stop("recipient mailbox has reached its pending message limit")
    }

    sequence_row <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT next_sequence FROM coordination_mailbox_sequences",
        "WHERE project_id = ? AND agent_id = ?"
      ),
      params = list(project, recipient)
    )
    sequence_number <- if (nrow(sequence_row) == 0L) {
      DBI::dbExecute(
        store$connection,
        "INSERT INTO coordination_mailbox_sequences VALUES (?, ?, 2)",
        params = list(project, recipient)
      )
      1
    } else {
      value <- sequence_row$next_sequence[[1L]]
      DBI::dbExecute(
        store$connection,
        paste(
          "UPDATE coordination_mailbox_sequences SET next_sequence = ?",
          "WHERE project_id = ? AND agent_id = ?"
        ),
        params = list(value + 1, project, recipient)
      )
      value
    }
    message_id <- coordination_uuid(store$connection)
    DBI::dbExecute(
      store$connection,
      paste(
        "INSERT INTO coordination_messages (message_id, project_id,",
        "sender_agent_id, recipient_agent_id, idempotency_key,",
        "sequence_number, content, content_type, created_at_ms, state)",
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued')"
      ),
      params = list(
        message_id, project, sender, recipient, idempotency_key,
        sequence_number, content, content_type, now
      )
    )
    list(
      message_id = message_id,
      recipient_agent_id = recipient,
      sequence_number = sequence_number,
      created_at_ms = now,
      replayed = FALSE
    )
  })())
}

coordination_expire_deliveries <- function(store, project, agent, now) {
  DBI::dbExecute(
    store$connection,
    paste(
      "UPDATE coordination_messages SET state = 'queued', receipt_token = NULL,",
      "lease_expires_at_ms = NULL WHERE project_id = ? AND recipient_agent_id = ?",
      "AND state = 'leased' AND lease_expires_at_ms <= ?"
    ),
    params = list(project, agent, now)
  )
}

coordination_receive <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, c(
    "registration_id", "limit", "visibility_timeout_ms"
  ))
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  limit <- coordination_integer(arguments, "limit", 8, 1, 32)
  visibility <- coordination_integer(
    arguments, "visibility_timeout_ms", 30000, 1000, 300000
  )
  now <- store$now()
  registration <- coordination_registration(store, registration_id, now)
  project <- registration$project_id[[1L]]
  agent <- registration$agent_id[[1L]]
  deadline <- now + visibility

  DBI::dbWithTransaction(store$connection, {
    coordination_expire_deliveries(store, project, agent, now)
    rows <- DBI::dbGetQuery(
      store$connection,
      paste0(
        "SELECT message_id, sender_agent_id, sequence_number, content, ",
        "content_type, created_at_ms FROM coordination_messages ",
        "WHERE project_id = ? AND recipient_agent_id = ? AND state = 'queued' ",
        "ORDER BY sequence_number LIMIT ", as.integer(limit)
      ),
      params = list(project, agent)
    )
    messages <- lapply(seq_len(nrow(rows)), function(index) {
      receipt <- coordination_uuid(store$connection)
      changed <- DBI::dbExecute(
        store$connection,
        paste(
          "UPDATE coordination_messages SET state = 'leased',",
          "receipt_token = ?, lease_expires_at_ms = ?",
          "WHERE message_id = ? AND state = 'queued'"
        ),
        params = list(receipt, deadline, rows$message_id[[index]])
      )
      if (changed != 1L) stop("message lease race")
      list(
        message_id = rows$message_id[[index]],
        receipt_token = receipt,
        sender_agent_id = rows$sender_agent_id[[index]],
        recipient_agent_id = agent,
        sequence_number = rows$sequence_number[[index]],
        content = rows$content[[index]],
        content_type = rows$content_type[[index]],
        created_at_ms = rows$created_at_ms[[index]],
        lease_expires_at_ms = deadline
      )
    })
    list(server_time_ms = now, messages = messages)
  })
}

coordination_ack <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, c(
    "registration_id", "receipt_token", "delivery_ref"
  ))
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  receipt <- coordination_string(arguments, "receipt_token", max_bytes = 64L)
  delivery_ref <- coordination_string(
    arguments, "delivery_ref", required = FALSE,
    default = "unspecified", max_bytes = 256L
  )
  now <- store$now()
  registration <- coordination_registration(store, registration_id, now)
  project <- registration$project_id[[1L]]
  agent <- registration$agent_id[[1L]]

  DBI::dbWithTransaction(store$connection, (function() {
    coordination_expire_deliveries(store, project, agent, now)
    row <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT message_id, state, lease_expires_at_ms, acked_at_ms,",
        "delivery_capability, ack_instance_id FROM coordination_messages",
        "WHERE project_id = ? AND recipient_agent_id = ? AND receipt_token = ?"
      ),
      params = list(project, agent, receipt)
    )
    if (nrow(row) != 1L) stop("receipt is missing, expired, or stale")
    if (identical(row$state[[1L]], "acked")) {
      return(list(
        message_id = row$message_id[[1L]],
        acknowledged = TRUE,
        replayed = TRUE,
        acknowledged_at_ms = row$acked_at_ms[[1L]],
        delivery_capability = row$delivery_capability[[1L]],
        receiver_instance_id = row$ack_instance_id[[1L]]
      ))
    }
    if (!identical(row$state[[1L]], "leased") ||
        row$lease_expires_at_ms[[1L]] <= now) {
      stop("receipt is missing, expired, or stale")
    }
    DBI::dbExecute(
      store$connection,
      paste(
        "UPDATE coordination_messages SET state = 'acked', acked_at_ms = ?,",
        "delivery_ref = ?, delivery_capability = ?, ack_instance_id = ?",
        "WHERE message_id = ?"
      ),
      params = list(
        now, delivery_ref, registration$delivery_capability[[1L]],
        registration$instance_id[[1L]], row$message_id[[1L]]
      )
    )
    list(
      message_id = row$message_id[[1L]],
      acknowledged = TRUE,
      replayed = FALSE,
      acknowledged_at_ms = now,
      delivery_capability = registration$delivery_capability[[1L]],
      receiver_instance_id = registration$instance_id[[1L]]
    )
  })())
}

# Registered agents are distinct producers in one conflict namespace. File URIs
# are normalized lexically for overlap checks; the endpoint never opens the path.
coordination_resource <- function(value) {
  value <- coordination_string(
    list(resource = value), "resource", max_bytes = 4096L
  )
  if (startsWith(value, "resource:")) {
    valid <- grepl(
      "^resource:[A-Za-z0-9][A-Za-z0-9._:/-]*$",
      value,
      perl = TRUE
    ) && !grepl("//|(^|/)\\.\\.?(/|$)", sub("^resource:", "", value), perl = TRUE)
    if (!valid) stop("resource: identifier is not canonical")
    return(value)
  }
  if (!startsWith(value, "file:///")) {
    stop("resource must use resource: or file:/// syntax")
  }
  encoded_path <- sub("^file://", "", value)
  if (grepl("%(?![0-9A-Fa-f]{2})", encoded_path, perl = TRUE)) {
    stop("file resource has invalid URL encoding")
  }
  path <- tryCatch(
    utils::URLdecode(encoded_path),
    error = function(error) stop("file resource has invalid URL encoding")
  )
  parts <- strsplit(path, "/", fixed = TRUE)[[1L]]
  if (!startsWith(path, "/") || any(parts %in% c(".", "..")) ||
      grepl("//", path, fixed = TRUE) || grepl("[[:cntrl:]]", path)) {
    stop("file resource must be an absolute canonical lexical path")
  }
  if (nchar(path) > 1L) path <- sub("/+$", "", path)
  paste0("file://", path)
}

coordination_resources_conflict <- function(left, right) {
  if (identical(left, right)) return(TRUE)
  if (!startsWith(left, "file:///") || !startsWith(right, "file:///")) {
    return(FALSE)
  }
  left_path <- sub("^file://", "", left)
  right_path <- sub("^file://", "", right)
  if (identical(left_path, "/") || identical(right_path, "/")) return(TRUE)
  startsWith(left_path, paste0(right_path, "/")) ||
    startsWith(right_path, paste0(left_path, "/"))
}

coordination_reserve <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, c(
    "registration_id", "resource", "operation_key", "lease_id", "ttl_ms"
  ))
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  resource <- coordination_resource(arguments$resource)
  lease_id <- coordination_string(
    arguments, "lease_id", required = FALSE, default = NULL, max_bytes = 64L
  )
  operation_key <- coordination_string(
    arguments, "operation_key", required = is.null(lease_id),
    default = NULL, max_bytes = 256L
  )
  ttl_ms <- coordination_integer(arguments, "ttl_ms", 30000, 5000, 300000)
  now <- store$now()
  expires <- now + ttl_ms
  registration <- coordination_registration(store, registration_id, now)
  project <- registration$project_id[[1L]]
  owner_agent <- registration$agent_id[[1L]]
  owner_instance <- registration$instance_id[[1L]]

  DBI::dbWithTransaction(store$connection, (function() {
    if (!is.null(lease_id)) {
      row <- DBI::dbGetQuery(
        store$connection,
        paste(
          "SELECT resource, fencing_value, expires_at_ms, released_at_ms",
          "FROM coordination_reservations WHERE lease_id = ? AND project_id = ?",
          "AND owner_instance_id = ?"
        ),
        params = list(lease_id, project, owner_instance)
      )
      valid <- nrow(row) == 1L && is.na(row$released_at_ms[[1L]]) &&
        row$expires_at_ms[[1L]] > now && identical(row$resource[[1L]], resource)
      if (!valid) stop("reservation lease is missing, expired, or stale")
      DBI::dbExecute(
        store$connection,
        "UPDATE coordination_reservations SET expires_at_ms = ? WHERE lease_id = ?",
        params = list(expires, lease_id)
      )
      return(list(
        lease_id = lease_id,
        resource = resource,
        fencing_value = row$fencing_value[[1L]],
        expires_at_ms = expires,
        replayed = FALSE,
        renewed = TRUE
      ))
    }

    prior <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT resource, lease_id, fencing_value, expires_at_ms, released_at_ms",
        "FROM coordination_reservations WHERE project_id = ?",
        "AND owner_instance_id = ? AND operation_key = ?"
      ),
      params = list(project, owner_instance, operation_key)
    )
    if (nrow(prior) == 1L) {
      if (!identical(prior$resource[[1L]], resource)) {
        stop("operation_key was already used for another resource")
      }
      return(list(
        lease_id = prior$lease_id[[1L]],
        resource = resource,
        fencing_value = prior$fencing_value[[1L]],
        expires_at_ms = prior$expires_at_ms[[1L]],
        replayed = TRUE,
        renewed = FALSE,
        active = is.na(prior$released_at_ms[[1L]]) &&
          prior$expires_at_ms[[1L]] > now
      ))
    }

    active <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT resource, owner_agent_id, expires_at_ms",
        "FROM coordination_reservations WHERE project_id = ?",
        "AND released_at_ms IS NULL AND expires_at_ms > ?"
      ),
      params = list(project, now)
    )
    conflict <- vapply(
      active$resource,
      coordination_resources_conflict,
      logical(1L),
      right = resource
    )
    if (any(conflict)) {
      row <- active[which(conflict)[[1L]], , drop = FALSE]
      stop(
        "resource conflicts with active reservation held by ",
        row$owner_agent_id[[1L]], " until ", row$expires_at_ms[[1L]]
      )
    }

    counter <- DBI::dbGetQuery(
      store$connection,
      paste(
        "SELECT next_value FROM coordination_fencing_counters",
        "WHERE project_id = ?"
      ),
      params = list(project)
    )
    fencing_value <- if (nrow(counter) == 0L) {
      DBI::dbExecute(
        store$connection,
        "INSERT INTO coordination_fencing_counters VALUES (?, 2)",
        params = list(project)
      )
      1
    } else {
      value <- counter$next_value[[1L]]
      DBI::dbExecute(
        store$connection,
        paste(
          "UPDATE coordination_fencing_counters SET next_value = ?",
          "WHERE project_id = ?"
        ),
        params = list(value + 1, project)
      )
      value
    }
    lease_id <- coordination_uuid(store$connection)
    DBI::dbExecute(
      store$connection,
      paste(
        "INSERT INTO coordination_reservations VALUES",
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)"
      ),
      params = list(
        project, owner_agent, owner_instance, operation_key, resource,
        lease_id, fencing_value, now, expires
      )
    )
    list(
      lease_id = lease_id,
      resource = resource,
      fencing_value = fencing_value,
      expires_at_ms = expires,
      replayed = FALSE,
      renewed = FALSE,
      active = TRUE
    )
  })())
}

coordination_release <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, c("registration_id", "lease_id"))
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  lease_id <- coordination_string(arguments, "lease_id", max_bytes = 64L)
  now <- store$now()
  registration <- coordination_registration(store, registration_id, now)
  row <- DBI::dbGetQuery(
    store$connection,
    paste(
      "SELECT resource, fencing_value, released_at_ms",
      "FROM coordination_reservations WHERE lease_id = ? AND project_id = ?",
      "AND owner_instance_id = ?"
    ),
    params = list(
      lease_id, registration$project_id[[1L]],
      registration$instance_id[[1L]]
    )
  )
  if (nrow(row) != 1L) stop("reservation lease is missing or belongs to another owner")
  replayed <- !is.na(row$released_at_ms[[1L]])
  if (!replayed) {
    DBI::dbExecute(
      store$connection,
      paste(
        "UPDATE coordination_reservations SET released_at_ms = ?,",
        "expires_at_ms = ? WHERE lease_id = ?"
      ),
      params = list(now, now, lease_id)
    )
  }
  list(
    lease_id = lease_id,
    resource = row$resource[[1L]],
    fencing_value = row$fencing_value[[1L]],
    released = TRUE,
    replayed = replayed
  )
}

coordination_unregister <- function(store, arguments) {
  arguments <- coordination_arguments(arguments, "registration_id")
  registration_id <- coordination_string(
    arguments, "registration_id", max_bytes = 64L
  )
  now <- store$now()
  row <- DBI::dbGetQuery(
    store$connection,
    paste(
      "SELECT unregistered_at_ms FROM coordination_agents",
      "WHERE registration_id = ?"
    ),
    params = list(registration_id)
  )
  if (nrow(row) != 1L) stop("registration is missing")
  replayed <- !is.na(row$unregistered_at_ms[[1L]])
  if (!replayed) {
    DBI::dbExecute(
      store$connection,
      paste(
        "UPDATE coordination_agents SET unregistered_at_ms = ?,",
        "expires_at_ms = ? WHERE registration_id = ?"
      ),
      params = list(now, now, registration_id)
    )
  }
  list(registration_id = registration_id, unregistered = TRUE, replayed = replayed)
}

coordination_dispatch <- function(store, method, arguments) {
  if (method %in% c("register", "send", "receive", "ack", "reserve", "release")) {
    coordination_maintain(store, store$now())
  }
  switch(
    method,
    register = coordination_register(store, arguments),
    heartbeat = coordination_heartbeat(store, arguments),
    list_agents = coordination_list_agents(store, arguments),
    send = coordination_send(store, arguments),
    receive = coordination_receive(store, arguments),
    ack = coordination_ack(store, arguments),
    reserve = coordination_reserve(store, arguments),
    release = coordination_release(store, arguments),
    unregister = coordination_unregister(store, arguments),
    stop("unknown coordination method")
  )
}
