-- Durably enqueue one idempotent message to a stable agent mailbox.
SELECT coord_arguments($1::JSON, ['registration_id', 'recipient_agent_id',
  'idempotency_key', 'content', 'content_type', 'ttl_ms']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SET VARIABLE coord_args = (
  SELECT struct_pack(
    recipient := coord_string(p, 'recipient_agent_id', 128, true, NULL),
    idempotency_key := coord_string(p, 'idempotency_key', 256, true, NULL),
    content := coord_text(p, 'content', 65536),
    content_type := coord_string(p, 'content_type', 128, false, 'text/plain'),
    ttl_ms := coord_integer(p, 'ttl_ms', 604800000, 60000, 2592000000)
  )
  FROM (SELECT $1::JSON AS p)
);
SET VARIABLE coord_existing = (
  SELECT struct_pack(
    message_id := m.message_id,
    same := m.recipient_agent_id = a.recipient AND m.content = a.content
      AND m.content_type = a.content_type,
    sequence_number := m.sequence_number,
    created_at_ms := m.created_at_ms,
    expires_at_ms := m.expires_at_ms
  )
  FROM coordination_messages m, (SELECT unnest(getvariable('coord_args'))) AS a
  WHERE m.project_id = getvariable('coord_reg').project_id
    AND m.sender_agent_id = getvariable('coord_reg').agent_id
    AND m.idempotency_key = a.idempotency_key
);
SELECT CASE
  WHEN getvariable('coord_existing') IS NOT NULL AND NOT getvariable('coord_existing').same
    THEN coord_fail('idempotency_conflict', 'idempotency_key was already used for another message')
  WHEN getvariable('coord_existing') IS NULL AND (
    SELECT count(*) FROM coordination_messages m, (SELECT unnest(getvariable('coord_args'))) AS a
    WHERE m.project_id = getvariable('coord_reg').project_id
      AND m.recipient_agent_id = a.recipient AND m.state IN ('queued', 'leased')
  ) >= (SELECT max_pending_per_mailbox FROM coordination_meta)
    THEN coord_fail('mailbox_full', 'recipient mailbox holds '
      || (SELECT max_pending_per_mailbox FROM coordination_meta) || ' unacknowledged messages')
END;
SET VARIABLE coord_sequence = (
  SELECT coalesce(max(s.next_sequence), 1)
  FROM coordination_mailbox_sequences s, (SELECT unnest(getvariable('coord_args'))) AS a
  WHERE s.project_id = getvariable('coord_reg').project_id AND s.agent_id = a.recipient
);
INSERT INTO coordination_mailbox_sequences
SELECT getvariable('coord_reg').project_id, a.recipient, getvariable('coord_sequence') + 1
FROM (SELECT unnest(getvariable('coord_args'))) AS a
WHERE getvariable('coord_existing') IS NULL
ON CONFLICT (project_id, agent_id) DO UPDATE SET next_sequence = excluded.next_sequence;
SET VARIABLE coord_message_id = coalesce(getvariable('coord_existing').message_id, uuid()::VARCHAR);
INSERT INTO coordination_messages (
  message_id, project_id, sender_agent_id, recipient_agent_id, idempotency_key,
  sequence_number, content, content_type, created_at_ms, expires_at_ms, state
)
SELECT
  getvariable('coord_message_id'), getvariable('coord_reg').project_id,
  getvariable('coord_reg').agent_id, a.recipient, a.idempotency_key,
  getvariable('coord_sequence'), a.content, a.content_type,
  getvariable('coord_now'), getvariable('coord_now') + a.ttl_ms, 'queued'
FROM (SELECT unnest(getvariable('coord_args'))) AS a
WHERE getvariable('coord_existing') IS NULL;
SELECT to_json(struct_pack(
  message_id := getvariable('coord_message_id'),
  project_id := getvariable('coord_reg').project_id,
  recipient_agent_id := a.recipient,
  recipient_seen := a.recipient = getvariable('coord_reg').agent_id OR EXISTS (
    SELECT 1 FROM coordination_agents c
    WHERE c.project_id = getvariable('coord_reg').project_id AND c.agent_id = a.recipient
  ),
  sequence_number := coalesce(getvariable('coord_existing').sequence_number,
    getvariable('coord_sequence')),
  created_at_ms := coalesce(getvariable('coord_existing').created_at_ms, getvariable('coord_now')),
  expires_at_ms := coalesce(getvariable('coord_existing').expires_at_ms,
    getvariable('coord_now') + a.ttl_ms),
  replayed := getvariable('coord_existing') IS NOT NULL
))
FROM (SELECT unnest(getvariable('coord_args'))) AS a
