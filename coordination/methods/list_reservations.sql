-- List active reservations, optionally only those conflicting with a resource.
-- Lease IDs are returned only to their owner.
SELECT coord_arguments($1::JSON, ['registration_id', 'resource']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SET VARIABLE coord_filter = (
  SELECT CASE
    WHEN coalesce(json_type(p, '$.resource'), 'NULL') = 'NULL' THEN NULL
    ELSE coord_resource(coord_string(p, 'resource', 4096, true, NULL))
  END
  FROM (SELECT $1::JSON AS p)
);
SELECT to_json(struct_pack(
  server_time_ms := getvariable('coord_now'),
  resource := getvariable('coord_filter'),
  reservations := coalesce((
    SELECT list(struct_pack(
      resource := resource,
      owner_agent_id := owner_agent_id,
      owned_by_caller := owner_instance_id = getvariable('coord_reg').instance_id,
      fencing_value := fencing_value,
      expires_at_ms := expires_at_ms,
      lease_id := CASE
        WHEN owner_instance_id = getvariable('coord_reg').instance_id THEN lease_id
      END
    ) ORDER BY resource, fencing_value)
    FROM coordination_reservations
    WHERE project_id = getvariable('coord_reg').project_id
      AND released_at_ms IS NULL AND expires_at_ms > getvariable('coord_now')
      AND (getvariable('coord_filter') IS NULL
        OR coord_conflict(resource, getvariable('coord_filter')))
  ), [])
))
