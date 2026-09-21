-- Lease a bounded ordered batch from the registered agent mailbox. The reply
-- never waits; callers poll.
SELECT coord_arguments($1::JSON, ['registration_id', 'limit', 'visibility_timeout_ms']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SET VARIABLE coord_args = (
  SELECT struct_pack(
    batch_limit := coord_integer(p, 'limit', 8, 1, 32),
    visibility_ms := coord_integer(p, 'visibility_timeout_ms', 30000, 1000, 300000)
  )
  FROM (SELECT $1::JSON AS p)
);
UPDATE coordination_messages
  SET state = 'queued', receipt_token = NULL, lease_batch = NULL, lease_expires_at_ms = NULL
  WHERE project_id = getvariable('coord_reg').project_id
    AND recipient_agent_id = getvariable('coord_reg').agent_id
    AND state = 'leased' AND lease_expires_at_ms <= getvariable('coord_now');
SET VARIABLE coord_batch = uuid()::VARCHAR;
UPDATE coordination_messages SET
  state = 'leased',
  receipt_token = uuid()::VARCHAR,
  lease_batch = getvariable('coord_batch'),
  lease_expires_at_ms = getvariable('coord_now') + getvariable('coord_args').visibility_ms
WHERE message_id IN (
  SELECT message_id FROM (
    SELECT message_id, row_number() OVER (ORDER BY sequence_number) AS position
    FROM coordination_messages
    WHERE project_id = getvariable('coord_reg').project_id
      AND recipient_agent_id = getvariable('coord_reg').agent_id
      AND state = 'queued' AND expires_at_ms > getvariable('coord_now')
  )
  WHERE position <= getvariable('coord_args').batch_limit
);
SELECT to_json(struct_pack(
  server_time_ms := getvariable('coord_now'),
  messages := coalesce((
    SELECT list(struct_pack(
      message_id := message_id,
      receipt_token := receipt_token,
      project_id := project_id,
      sender_agent_id := sender_agent_id,
      recipient_agent_id := recipient_agent_id,
      sequence_number := sequence_number,
      content := content,
      content_type := content_type,
      broadcast_id := broadcast_id,
      recipient_count := recipient_count,
      in_reply_to := in_reply_to,
      created_at_ms := created_at_ms,
      expires_at_ms := expires_at_ms,
      lease_expires_at_ms := lease_expires_at_ms
    ) ORDER BY sequence_number)
    FROM coordination_messages WHERE lease_batch = getvariable('coord_batch')
  ), [])
))
