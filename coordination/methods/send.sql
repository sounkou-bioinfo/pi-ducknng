-- Durably enqueue one idempotent message to one mailbox, a list of mailboxes,
-- or every other live agent in the project. Each recipient gets its own
-- leased, acknowledged copy; the copies of one send share a broadcast_id.
SELECT coord_arguments($1::JSON, ['registration_id', 'recipient_agent_id',
  'recipient_agent_ids', 'broadcast', 'idempotency_key', 'content', 'content_type',
  'ttl_ms', 'in_reply_to']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SET VARIABLE coord_args = (
  SELECT struct_pack(
    idempotency_key := coord_string(p, 'idempotency_key', 256, true, NULL),
    content := coord_text(p, 'content', 65536),
    content_type := coord_string(p, 'content_type', 128, false, 'text/plain'),
    ttl_ms := coord_integer(p, 'ttl_ms', 604800000, 60000, 2592000000),
    in_reply_to := coord_string(p, 'in_reply_to', 64, false, NULL),
    single := coord_string(p, 'recipient_agent_id', 128, false, NULL),
    listed := coord_recipients(p),
    broadcast := CASE
      WHEN coalesce(json_type(p, '$.broadcast'), 'NULL') = 'NULL' THEN false
      WHEN json_type(p, '$.broadcast') <> 'BOOLEAN'
        THEN coord_fail('invalid_argument', 'broadcast must be a boolean')
      ELSE (p ->> 'broadcast')::BOOLEAN
    END
  )
  FROM (SELECT $1::JSON AS p)
);
SELECT CASE
  WHEN (a.single IS NOT NULL)::INTEGER + (a.listed IS NOT NULL)::INTEGER
    + a.broadcast::INTEGER <> 1
  THEN coord_fail('invalid_argument',
    'give exactly one of recipient_agent_id, recipient_agent_ids, or broadcast')
END
FROM (SELECT unnest(getvariable('coord_args'))) AS a;
SET VARIABLE coord_recipients = (
  SELECT CASE
    WHEN a.single IS NOT NULL THEN [a.single]
    WHEN a.listed IS NOT NULL THEN a.listed
    ELSE coalesce((
      SELECT list_sort(list_distinct(list(agent_id))) FROM coordination_agents
      WHERE project_id = getvariable('coord_reg').project_id
        AND agent_id <> getvariable('coord_reg').agent_id
        AND unregistered_at_ms IS NULL AND expires_at_ms > getvariable('coord_now')
    ), [])
  END
  FROM (SELECT unnest(getvariable('coord_args'))) AS a
);
-- A known idempotency key replays the original send.
SET VARIABLE coord_existing = (
  SELECT struct_pack(
    broadcast_id := min(m.broadcast_id),
    recipients := list_sort(list(m.recipient_agent_id)),
    same := bool_and(m.content = a.content AND m.content_type = a.content_type
      AND m.in_reply_to IS NOT DISTINCT FROM a.in_reply_to),
    created_at_ms := min(m.created_at_ms),
    expires_at_ms := min(m.expires_at_ms)
  )
  FROM coordination_messages m, (SELECT unnest(getvariable('coord_args'))) AS a
  WHERE m.project_id = getvariable('coord_reg').project_id
    AND m.sender_agent_id = getvariable('coord_reg').agent_id
    AND m.idempotency_key = a.idempotency_key
  HAVING count(*) > 0
);
SELECT CASE
  WHEN getvariable('coord_existing') IS NOT NULL AND (
    NOT getvariable('coord_existing').same
    OR (NOT a.broadcast AND getvariable('coord_existing').recipients <> getvariable('coord_recipients')))
  THEN coord_fail('idempotency_conflict', 'idempotency_key was already used for another message')
  WHEN getvariable('coord_existing') IS NULL AND len(getvariable('coord_recipients')) = 0
  THEN coord_fail('invalid_argument', 'broadcast found no other live agent')
END
FROM (SELECT unnest(getvariable('coord_args'))) AS a;
SELECT CASE
  WHEN count(*) > 0 THEN coord_fail('mailbox_full', 'mailbox ' || min(recipient) || ' holds '
    || max(maximum) || ' unacknowledged messages')
END
FROM (
  SELECT
    t.recipient,
    (SELECT max_pending_per_mailbox FROM coordination_meta) AS maximum,
    (SELECT count(*) FROM coordination_messages m
      WHERE m.project_id = getvariable('coord_reg').project_id
        AND m.recipient_agent_id = t.recipient AND m.state IN ('queued', 'leased')) AS pending
  FROM unnest(getvariable('coord_recipients')) AS t(recipient)
  WHERE getvariable('coord_existing') IS NULL
)
WHERE pending >= maximum;
SET VARIABLE coord_broadcast_id = coalesce(getvariable('coord_existing').broadcast_id, uuid()::VARCHAR);
SET VARIABLE coord_targets = (
  SELECT list(struct_pack(
    recipient := t.recipient,
    sequence_number := coalesce(s.next_sequence, 1),
    message_id := uuid()::VARCHAR
  ) ORDER BY t.recipient)
  FROM unnest(getvariable('coord_recipients')) AS t(recipient)
  LEFT JOIN coordination_mailbox_sequences s
    ON s.project_id = getvariable('coord_reg').project_id AND s.agent_id = t.recipient
  WHERE getvariable('coord_existing') IS NULL
);
INSERT INTO coordination_mailbox_sequences
SELECT getvariable('coord_reg').project_id, t.recipient, t.sequence_number + 1
FROM (SELECT unnest(getvariable('coord_targets'), recursive := true)) AS t
ON CONFLICT (project_id, agent_id) DO UPDATE SET next_sequence = excluded.next_sequence;
INSERT INTO coordination_messages (
  message_id, project_id, sender_agent_id, recipient_agent_id, idempotency_key,
  sequence_number, content, content_type, broadcast_id, recipient_count, in_reply_to,
  created_at_ms, expires_at_ms, state
)
SELECT
  t.message_id, getvariable('coord_reg').project_id, getvariable('coord_reg').agent_id,
  t.recipient, a.idempotency_key, t.sequence_number, a.content, a.content_type,
  getvariable('coord_broadcast_id'), len(getvariable('coord_recipients')), a.in_reply_to,
  getvariable('coord_now'), getvariable('coord_now') + a.ttl_ms, 'queued'
FROM (SELECT unnest(getvariable('coord_targets'), recursive := true)) AS t,
  (SELECT unnest(getvariable('coord_args'))) AS a;
-- Wake-up hints carry only an opaque mailbox topic; a missed hint costs
-- latency, never mail. They go to the hint socket and, when Server-Sent
-- Events are served, to the in-process socket their relays subscribe to.
SELECT count(ducknng_send_socket_raw(
  s.socket_id, encode(coord_topic(getvariable('coord_reg').project_id, t.recipient)), 100))
FROM (SELECT unnest(getvariable('coord_targets'), recursive := true)) AS t,
  (SELECT event_socket_id AS socket_id FROM coordination_meta WHERE event_socket_id IS NOT NULL
   UNION ALL
   SELECT event_relay_socket_id FROM coordination_meta WHERE event_relay_socket_id IS NOT NULL) AS s;
SET VARIABLE coord_sent = (
  SELECT list(struct_pack(
    recipient_agent_id := m.recipient_agent_id,
    message_id := m.message_id,
    sequence_number := m.sequence_number,
    recipient_seen := m.recipient_agent_id = getvariable('coord_reg').agent_id OR EXISTS (
      SELECT 1 FROM coordination_agents c
      WHERE c.project_id = m.project_id AND c.agent_id = m.recipient_agent_id)
  ) ORDER BY m.recipient_agent_id)
  FROM coordination_messages m
  WHERE m.project_id = getvariable('coord_reg').project_id
    AND m.broadcast_id = getvariable('coord_broadcast_id')
);
SELECT to_json(struct_pack(
  broadcast_id := getvariable('coord_broadcast_id'),
  project_id := getvariable('coord_reg').project_id,
  recipient_count := len(getvariable('coord_sent')),
  message_id := CASE WHEN len(getvariable('coord_sent')) = 1
    THEN getvariable('coord_sent')[1].message_id END,
  recipient_agent_id := CASE WHEN len(getvariable('coord_sent')) = 1
    THEN getvariable('coord_sent')[1].recipient_agent_id END,
  recipient_seen := CASE WHEN len(getvariable('coord_sent')) = 1
    THEN getvariable('coord_sent')[1].recipient_seen END,
  sequence_number := CASE WHEN len(getvariable('coord_sent')) = 1
    THEN getvariable('coord_sent')[1].sequence_number END,
  messages := getvariable('coord_sent'),
  created_at_ms := coalesce(getvariable('coord_existing').created_at_ms, getvariable('coord_now')),
  expires_at_ms := coalesce(getvariable('coord_existing').expires_at_ms,
    getvariable('coord_now') + a.ttl_ms),
  replayed := getvariable('coord_existing') IS NOT NULL
))
FROM (SELECT unnest(getvariable('coord_args'))) AS a
