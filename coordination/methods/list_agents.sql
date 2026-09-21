-- List live agent instances in the caller's project.
SELECT coord_arguments($1::JSON, ['registration_id']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SELECT to_json(struct_pack(
  server_time_ms := getvariable('coord_now'),
  agents := coalesce((
    SELECT list(struct_pack(
      agent_id := agent_id,
      instance_id := instance_id,
      display_name := display_name,
      adapter_kind := adapter_kind,
      delivery_capability := delivery_capability,
      authenticated := peer_identity IS NOT NULL,
      status := status_json::JSON,
      registered_at_ms := registered_at_ms,
      heartbeat_at_ms := heartbeat_at_ms,
      expires_at_ms := expires_at_ms
    ) ORDER BY agent_id, instance_id)
    FROM coordination_agents
    WHERE project_id = getvariable('coord_reg').project_id
      AND unregistered_at_ms IS NULL AND expires_at_ms > getvariable('coord_now')
  ), [])
))
