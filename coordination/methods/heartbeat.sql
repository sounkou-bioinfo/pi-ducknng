-- Renew one registration lease and replace its bounded status.
SELECT coord_arguments($1::JSON, ['registration_id', 'status']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_rid = coord_string($1::JSON, 'registration_id', 64, true, NULL);
SET VARIABLE coord_reg = coord_registration(getvariable('coord_rid'), getvariable('coord_now'));
SET VARIABLE coord_status = (
  SELECT CASE
    WHEN coalesce(json_type(p, '$.status'), 'NULL') = 'NULL' THEN '{}'
    WHEN json_type(p, '$.status') <> 'OBJECT'
      THEN coord_fail('invalid_argument', 'status must be a JSON object')
    WHEN strlen((p -> 'status')::VARCHAR) > 8192
      THEN coord_fail('invalid_argument', 'status is larger than 8192 bytes')
    ELSE (p -> 'status')::VARCHAR
  END
  FROM (SELECT $1::JSON AS p)
);
UPDATE coordination_agents SET
  status_json = getvariable('coord_status'),
  heartbeat_at_ms = getvariable('coord_now'),
  expires_at_ms = getvariable('coord_now')
    + least(300000, greatest(5000, expires_at_ms - heartbeat_at_ms))
WHERE registration_id = getvariable('coord_rid');
SELECT to_json(struct_pack(
  registration_id := registration_id,
  server_time_ms := getvariable('coord_now'),
  expires_at_ms := expires_at_ms,
  project_id := project_id,
  agent_id := agent_id
))
FROM coordination_agents WHERE registration_id = getvariable('coord_rid')
