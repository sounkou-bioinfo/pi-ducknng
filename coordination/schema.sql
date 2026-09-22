-- Coordination store served by ducknng SQL methods. The host runs this file
-- on every start; tables persist and macros are replaced.

CREATE TABLE IF NOT EXISTS coordination_meta (
  singleton BOOLEAN PRIMARY KEY CHECK (singleton),
  schema_version INTEGER NOT NULL,
  fixed_now_ms BIGINT,
  maintained_at_ms BIGINT NOT NULL,
  retention_ms BIGINT NOT NULL,
  max_pending_per_mailbox BIGINT NOT NULL,
  -- Wake-up hints: the host's PUB socket, its URL, and the salt that keeps
  -- mailbox topics unguessable without a registration.
  event_socket_id UBIGINT,
  event_url VARCHAR,
  event_salt VARCHAR NOT NULL,
  -- Server-Sent Events for HTTP clients: the in-process PUB socket and URL the
  -- event route's relays subscribe to, and the public /events URL.
  event_relay_socket_id UBIGINT,
  event_relay_url VARCHAR,
  event_sse_url VARCHAR
);

INSERT INTO coordination_meta
  VALUES (TRUE, 1, NULL, 0, 2592000000, 10000, NULL, NULL, uuid()::VARCHAR, NULL, NULL, NULL)
  ON CONFLICT (singleton) DO NOTHING;

-- A verified peer identity may act only as the (project, agent) pairs granted
-- here; agent_id '*' grants every agent in the project.
CREATE TABLE IF NOT EXISTS coordination_grants (
  peer_identity VARCHAR NOT NULL,
  project_id VARCHAR NOT NULL,
  agent_id VARCHAR NOT NULL,
  PRIMARY KEY (peer_identity, project_id, agent_id)
);

CREATE TABLE IF NOT EXISTS coordination_agents (
  project_id VARCHAR NOT NULL,
  agent_id VARCHAR NOT NULL,
  instance_id VARCHAR NOT NULL,
  registration_id VARCHAR NOT NULL,
  operation_key VARCHAR NOT NULL,
  display_name VARCHAR NOT NULL,
  adapter_kind VARCHAR NOT NULL,
  delivery_capability VARCHAR NOT NULL,
  peer_identity VARCHAR,
  status_json VARCHAR NOT NULL,
  registered_at_ms BIGINT NOT NULL,
  heartbeat_at_ms BIGINT NOT NULL,
  expires_at_ms BIGINT NOT NULL,
  unregistered_at_ms BIGINT,
  PRIMARY KEY (project_id, instance_id),
  UNIQUE (registration_id)
);

CREATE TABLE IF NOT EXISTS coordination_mailbox_sequences (
  project_id VARCHAR NOT NULL,
  agent_id VARCHAR NOT NULL,
  next_sequence BIGINT NOT NULL,
  PRIMARY KEY (project_id, agent_id)
);

CREATE TABLE IF NOT EXISTS coordination_messages (
  message_id VARCHAR PRIMARY KEY,
  project_id VARCHAR NOT NULL,
  sender_agent_id VARCHAR NOT NULL,
  recipient_agent_id VARCHAR NOT NULL,
  idempotency_key VARCHAR NOT NULL,
  sequence_number BIGINT NOT NULL,
  content VARCHAR NOT NULL,
  content_type VARCHAR NOT NULL,
  -- One send to several mailboxes shares a broadcast_id.
  broadcast_id VARCHAR NOT NULL,
  recipient_count INTEGER NOT NULL,
  in_reply_to VARCHAR,
  created_at_ms BIGINT NOT NULL,
  expires_at_ms BIGINT NOT NULL,
  state VARCHAR NOT NULL,
  receipt_token VARCHAR,
  lease_batch VARCHAR,
  lease_expires_at_ms BIGINT,
  acked_at_ms BIGINT,
  expired_at_ms BIGINT,
  delivery_ref VARCHAR,
  delivery_capability VARCHAR,
  ack_instance_id VARCHAR,
  UNIQUE (project_id, sender_agent_id, idempotency_key, recipient_agent_id)
);

CREATE TABLE IF NOT EXISTS coordination_fencing_counters (
  project_id VARCHAR PRIMARY KEY,
  next_value BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS coordination_reservations (
  project_id VARCHAR NOT NULL,
  owner_agent_id VARCHAR NOT NULL,
  owner_instance_id VARCHAR NOT NULL,
  operation_key VARCHAR NOT NULL,
  resource VARCHAR NOT NULL,
  lease_id VARCHAR NOT NULL,
  fencing_value BIGINT NOT NULL,
  created_at_ms BIGINT NOT NULL,
  expires_at_ms BIGINT NOT NULL,
  released_at_ms BIGINT,
  PRIMARY KEY (project_id, owner_instance_id, operation_key),
  UNIQUE (lease_id)
);

-- Failures surface as "<code>: <detail>" inside the ducknng error text.
CREATE OR REPLACE MACRO coord_fail(code, detail) AS error(code || ': ' || detail);

-- Server time. fixed_now_ms exists for tests and is NULL in service.
CREATE OR REPLACE MACRO coord_now() AS (
  SELECT coalesce(fixed_now_ms, epoch_ms(now())) FROM coordination_meta
);

CREATE OR REPLACE MACRO coord_cutoff() AS (
  SELECT coord_now() - retention_ms FROM coordination_meta
);

-- The verified caller, or NULL on an unauthenticated listener.
CREATE OR REPLACE MACRO coord_caller() AS (
  SELECT peer_identity FROM ducknng_request_subject()
);

CREATE OR REPLACE MACRO coord_arguments(p, allowed) AS (
  SELECT CASE
    WHEN count(*) > 0 THEN coord_fail('invalid_argument', 'unexpected argument ' || min(k))
  END
  FROM unnest(json_keys(p)) AS t(k)
  WHERE NOT list_contains(allowed, k)
);

-- Identifiers and tokens: one bounded string without control characters.
CREATE OR REPLACE MACRO coord_string(p, name, max_bytes, required, dflt) AS (
  CASE
    WHEN coalesce(json_type(p, '$."' || name || '"'), 'NULL') = 'NULL' THEN
      CASE
        WHEN required THEN coord_fail('invalid_argument', name || ' is required')
        ELSE dflt
      END
    WHEN json_type(p, '$."' || name || '"') <> 'VARCHAR'
      OR (p ->> name) = ''
      OR strlen(p ->> name) > max_bytes
      OR regexp_matches(p ->> name, '[\x00-\x1F\x7F]')
    THEN coord_fail('invalid_argument', name || ' must be one non-empty string of at most '
      || max_bytes || ' bytes without control characters')
    ELSE p ->> name
  END
);

-- Message text: UTF-8 that may contain tabs and line breaks.
CREATE OR REPLACE MACRO coord_text(p, name, max_bytes) AS (
  CASE
    WHEN json_type(p, '$."' || name || '"') IS DISTINCT FROM 'VARCHAR'
      OR (p ->> name) = ''
      OR strlen(p ->> name) > max_bytes
      OR regexp_matches(p ->> name, '[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')
    THEN coord_fail('invalid_argument', name || ' must be non-empty UTF-8 text of at most '
      || max_bytes || ' bytes; only tab, line feed, and carriage return control characters are allowed')
    ELSE p ->> name
  END
);

CREATE OR REPLACE MACRO coord_integer(p, name, dflt, minimum, maximum) AS (
  CASE
    WHEN coalesce(json_type(p, '$."' || name || '"'), 'NULL') = 'NULL' THEN dflt::BIGINT
    WHEN json_type(p, '$."' || name || '"') NOT IN ('BIGINT', 'UBIGINT')
      OR TRY_CAST(p ->> name AS HUGEINT) NOT BETWEEN minimum AND maximum
    THEN coord_fail('invalid_argument', name || ' must be an integer from '
      || minimum || ' to ' || maximum)
    ELSE (p ->> name)::BIGINT
  END
);

-- A live registration called by the identity that created it.
CREATE OR REPLACE MACRO coord_registration(rid, now_ms) AS (
  SELECT CASE
    WHEN count(*) = 0 THEN coord_fail('registration_expired', 'registration is missing or expired')
    WHEN min(peer_identity) IS DISTINCT FROM coord_caller()
      THEN coord_fail('unauthorized', 'registration belongs to another peer identity')
    ELSE min(struct_pack(
      project_id := project_id,
      agent_id := agent_id,
      instance_id := instance_id,
      delivery_capability := delivery_capability
    ))
  END
  FROM coordination_agents
  WHERE registration_id = rid AND unregistered_at_ms IS NULL AND expires_at_ms > now_ms
);

-- File URIs are normalized lexically and percent-encoded per path segment;
-- the endpoint never opens the path.
CREATE OR REPLACE MACRO coord_file_uri(path) AS (
  CASE
    WHEN NOT starts_with(path, '/')
      OR contains(path, '//')
      OR regexp_matches(path, '(^|/)\.\.?(/|$)')
      OR regexp_matches(path, '[\x00-\x1F\x7F]')
    THEN coord_fail('invalid_argument', 'file resource must be an absolute canonical lexical path')
    WHEN path = '/' THEN 'file:///'
    ELSE 'file://' || array_to_string(
      list_transform(string_split(rtrim(path, '/'), '/'), lambda segment: url_encode(segment)),
      '/'
    )
  END
);

CREATE OR REPLACE MACRO coord_resource(v) AS (
  CASE
    WHEN starts_with(v, 'resource:') THEN
      CASE
        WHEN regexp_full_match(v, 'resource:[A-Za-z0-9][A-Za-z0-9._:/-]*')
          AND NOT regexp_matches(substr(v, 10), '//|(^|/)\.\.?(/|$)')
        THEN v
        ELSE coord_fail('invalid_argument', 'resource: identifier is not canonical')
      END
    WHEN NOT starts_with(v, 'file:///')
      THEN coord_fail('invalid_argument', 'resource must use resource: or file:/// syntax')
    WHEN regexp_matches(substr(v, 8), '%($|[^0-9A-Fa-f]|[0-9A-Fa-f]($|[^0-9A-Fa-f]))')
      THEN coord_fail('invalid_argument', 'file resource has invalid URL encoding')
    ELSE coord_file_uri(url_decode(substr(v, 8)))
  END
);

-- File URIs conflict with their ancestors and descendants.
CREATE OR REPLACE MACRO coord_conflict(l, r) AS (
  l = r OR (
    starts_with(l, 'file:///') AND starts_with(r, 'file:///') AND (
      l = 'file:///' OR r = 'file:///'
      OR starts_with(l, r || '/') OR starts_with(r, l || '/')
    )
  )
);

-- Opaque per-mailbox hint topic; a hint carries only this value.
CREATE OR REPLACE MACRO coord_topic(project, agent) AS (
  SELECT left(sha256(event_salt || chr(0) || project || chr(0) || agent), 32)
  FROM coordination_meta
);

CREATE OR REPLACE MACRO coord_events(project, agent) AS (
  SELECT CASE WHEN event_url IS NOT NULL OR event_sse_url IS NOT NULL THEN
    struct_pack(
      url := event_url,
      topic := coord_topic(project, agent),
      sse_url := CASE WHEN event_sse_url IS NOT NULL
        THEN event_sse_url || '?topic=' || coord_topic(project, agent) END
    )
  END
  FROM coordination_meta
);

-- An explicit recipient list: 1 to 256 bounded identifiers, deduplicated.
CREATE OR REPLACE MACRO coord_recipients(p) AS (
  CASE
    WHEN coalesce(json_type(p, '$.recipient_agent_ids'), 'NULL') = 'NULL' THEN NULL
    WHEN json_type(p, '$.recipient_agent_ids') <> 'ARRAY'
      OR json_array_length(p, '$.recipient_agent_ids') NOT BETWEEN 1 AND 256
      OR NOT list_bool_and(list_transform(
        json_extract(p, '$.recipient_agent_ids[*]'),
        lambda entry: json_type(entry) = 'VARCHAR'
          AND (entry ->> '$') <> ''
          AND strlen(entry ->> '$') <= 128
          AND NOT regexp_matches(entry ->> '$', '[\x00-\x1F\x7F]')
      ))
    THEN coord_fail('invalid_argument',
      'recipient_agent_ids must be 1 to 256 identifiers of at most 128 bytes')
    ELSE list_sort(list_distinct(list_transform(
      json_extract(p, '$.recipient_agent_ids[*]'), lambda entry: entry ->> '$')))
  END
);
