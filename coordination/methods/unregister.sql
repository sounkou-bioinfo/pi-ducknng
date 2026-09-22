-- End one agent instance without deleting durable mail. Its reservations end
-- with it; a holder that stops without unregistering keeps them until TTL.
SELECT coord_arguments($1::JSON, ['registration_id']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_rid = coord_string($1::JSON, 'registration_id', 64, true, NULL);
SET VARIABLE coord_target = (
  SELECT struct_pack(
    unregistered_at_ms := unregistered_at_ms, peer_identity := peer_identity,
    project_id := project_id, instance_id := instance_id)
  FROM coordination_agents WHERE registration_id = getvariable('coord_rid')
);
SELECT CASE
  WHEN getvariable('coord_target') IS NULL
    THEN coord_fail('registration_expired', 'registration is missing')
  WHEN getvariable('coord_target').peer_identity IS DISTINCT FROM coord_caller()
    THEN coord_fail('unauthorized', 'registration belongs to another peer identity')
END;
SET VARIABLE coord_released = (
  SELECT count(*) FROM coordination_reservations
  WHERE getvariable('coord_target').unregistered_at_ms IS NULL
    AND project_id = getvariable('coord_target').project_id
    AND owner_instance_id = getvariable('coord_target').instance_id
    AND released_at_ms IS NULL AND expires_at_ms > getvariable('coord_now')
);
UPDATE coordination_reservations
  SET released_at_ms = getvariable('coord_now'), expires_at_ms = getvariable('coord_now')
  WHERE getvariable('coord_target').unregistered_at_ms IS NULL
    AND project_id = getvariable('coord_target').project_id
    AND owner_instance_id = getvariable('coord_target').instance_id
    AND released_at_ms IS NULL AND expires_at_ms > getvariable('coord_now');
UPDATE coordination_agents
  SET unregistered_at_ms = getvariable('coord_now'), expires_at_ms = getvariable('coord_now')
  WHERE registration_id = getvariable('coord_rid') AND unregistered_at_ms IS NULL;
SELECT to_json(struct_pack(
  registration_id := getvariable('coord_rid'),
  unregistered := true,
  released_reservations := getvariable('coord_released'),
  replayed := getvariable('coord_target').unregistered_at_ms IS NOT NULL
))
