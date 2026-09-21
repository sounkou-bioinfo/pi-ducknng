-- Idempotently acknowledge a current delivery receipt.
SELECT coord_arguments($1::JSON, ['registration_id', 'receipt_token', 'delivery_ref']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SET VARIABLE coord_args = (
  SELECT struct_pack(
    receipt := coord_string(p, 'receipt_token', 64, true, NULL),
    delivery_ref := coord_string(p, 'delivery_ref', 256, false, 'unspecified')
  )
  FROM (SELECT $1::JSON AS p)
);
UPDATE coordination_messages
  SET state = 'queued', receipt_token = NULL, lease_batch = NULL, lease_expires_at_ms = NULL
  WHERE project_id = getvariable('coord_reg').project_id
    AND recipient_agent_id = getvariable('coord_reg').agent_id
    AND state = 'leased' AND lease_expires_at_ms <= getvariable('coord_now');
SET VARIABLE coord_message = (
  SELECT struct_pack(message_id := message_id, state := state,
    lease_expires_at_ms := lease_expires_at_ms)
  FROM coordination_messages
  WHERE project_id = getvariable('coord_reg').project_id
    AND recipient_agent_id = getvariable('coord_reg').agent_id
    AND receipt_token = getvariable('coord_args').receipt
);
SELECT CASE
  WHEN getvariable('coord_message') IS NULL
    OR (getvariable('coord_message').state <> 'acked' AND (
      getvariable('coord_message').state <> 'leased'
      OR getvariable('coord_message').lease_expires_at_ms <= getvariable('coord_now')))
  THEN coord_fail('receipt_invalid', 'receipt is missing, expired, or stale')
END;
UPDATE coordination_messages SET
  state = 'acked',
  acked_at_ms = getvariable('coord_now'),
  delivery_ref = getvariable('coord_args').delivery_ref,
  delivery_capability = getvariable('coord_reg').delivery_capability,
  ack_instance_id = getvariable('coord_reg').instance_id
WHERE message_id = getvariable('coord_message').message_id AND state = 'leased';
SELECT to_json(struct_pack(
  message_id := message_id,
  acknowledged := true,
  replayed := getvariable('coord_message').state = 'acked',
  acknowledged_at_ms := acked_at_ms,
  delivery_capability := delivery_capability,
  receiver_instance_id := ack_instance_id
))
FROM coordination_messages WHERE message_id = getvariable('coord_message').message_id
