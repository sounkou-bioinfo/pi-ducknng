-- Acquire, replay, or renew an advisory project-scoped resource lease.
SELECT coord_arguments($1::JSON, ['registration_id', 'resource', 'operation_key',
  'lease_id', 'ttl_ms']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SET VARIABLE coord_args = (
  SELECT struct_pack(
    resource := coord_resource(coord_string(p, 'resource', 4096, true, NULL)),
    lease_id := coord_string(p, 'lease_id', 64, false, NULL),
    operation_key := coord_string(p, 'operation_key', 256, false, NULL),
    ttl_ms := coord_integer(p, 'ttl_ms', 30000, 5000, 300000)
  )
  FROM (SELECT $1::JSON AS p)
);
SELECT CASE
  WHEN getvariable('coord_args').lease_id IS NULL AND getvariable('coord_args').operation_key IS NULL
    THEN coord_fail('invalid_argument', 'operation_key is required')
END;
-- Renewal presents a lease this instance holds.
SET VARIABLE coord_renew = (
  SELECT struct_pack(
    lease_id := lease_id,
    fencing_value := fencing_value,
    valid := released_at_ms IS NULL AND expires_at_ms > getvariable('coord_now')
      AND resource = getvariable('coord_args').resource
  )
  FROM coordination_reservations
  WHERE getvariable('coord_args').lease_id IS NOT NULL
    AND lease_id = getvariable('coord_args').lease_id
    AND project_id = getvariable('coord_reg').project_id
    AND owner_instance_id = getvariable('coord_reg').instance_id
);
SELECT CASE
  WHEN getvariable('coord_args').lease_id IS NOT NULL
    AND coalesce(getvariable('coord_renew').valid, false) = false
  THEN coord_fail('lease_invalid', 'reservation lease is missing, expired, or stale')
END;
UPDATE coordination_reservations
  SET expires_at_ms = getvariable('coord_now') + getvariable('coord_args').ttl_ms
  WHERE lease_id = getvariable('coord_renew').lease_id;
-- An acquisition with a known operation key replays its original result.
SET VARIABLE coord_prior = (
  SELECT struct_pack(
    resource := resource,
    lease_id := lease_id,
    fencing_value := fencing_value,
    expires_at_ms := expires_at_ms,
    active := released_at_ms IS NULL AND expires_at_ms > getvariable('coord_now')
  )
  FROM coordination_reservations
  WHERE getvariable('coord_args').lease_id IS NULL
    AND project_id = getvariable('coord_reg').project_id
    AND owner_instance_id = getvariable('coord_reg').instance_id
    AND operation_key = getvariable('coord_args').operation_key
);
SELECT CASE
  WHEN getvariable('coord_prior') IS NOT NULL
    AND getvariable('coord_prior').resource <> getvariable('coord_args').resource
  THEN coord_fail('idempotency_conflict', 'operation_key was already used for another resource')
END;
SET VARIABLE coord_acquire = getvariable('coord_args').lease_id IS NULL
  AND getvariable('coord_prior') IS NULL;
SELECT CASE
  WHEN count(*) > 0 THEN coord_fail('resource_conflict', arg_min(resource, fencing_value)
    || ' is reserved by ' || arg_min(owner_agent_id, fencing_value)
    || ' until ' || arg_min(expires_at_ms, fencing_value))
END
FROM coordination_reservations
WHERE getvariable('coord_acquire')
  AND project_id = getvariable('coord_reg').project_id
  AND released_at_ms IS NULL AND expires_at_ms > getvariable('coord_now')
  AND coord_conflict(resource, getvariable('coord_args').resource);
SET VARIABLE coord_fencing = (
  SELECT coalesce(max(next_value), 1) FROM coordination_fencing_counters
  WHERE project_id = getvariable('coord_reg').project_id
);
INSERT INTO coordination_fencing_counters
SELECT getvariable('coord_reg').project_id, getvariable('coord_fencing') + 1
WHERE getvariable('coord_acquire')
ON CONFLICT (project_id) DO UPDATE SET next_value = excluded.next_value;
SET VARIABLE coord_new_lease = CASE WHEN getvariable('coord_acquire') THEN uuid()::VARCHAR END;
INSERT INTO coordination_reservations
SELECT
  getvariable('coord_reg').project_id, getvariable('coord_reg').agent_id,
  getvariable('coord_reg').instance_id, getvariable('coord_args').operation_key,
  getvariable('coord_args').resource, getvariable('coord_new_lease'),
  getvariable('coord_fencing'), getvariable('coord_now'),
  getvariable('coord_now') + getvariable('coord_args').ttl_ms, NULL
WHERE getvariable('coord_acquire');
SELECT to_json(CASE
  WHEN getvariable('coord_renew') IS NOT NULL THEN struct_pack(
    lease_id := getvariable('coord_renew').lease_id,
    resource := getvariable('coord_args').resource,
    fencing_value := getvariable('coord_renew').fencing_value,
    expires_at_ms := getvariable('coord_now') + getvariable('coord_args').ttl_ms,
    replayed := false, renewed := true, active := true)
  WHEN getvariable('coord_prior') IS NOT NULL THEN struct_pack(
    lease_id := getvariable('coord_prior').lease_id,
    resource := getvariable('coord_args').resource,
    fencing_value := getvariable('coord_prior').fencing_value,
    expires_at_ms := getvariable('coord_prior').expires_at_ms,
    replayed := true, renewed := false, active := getvariable('coord_prior').active)
  ELSE struct_pack(
    lease_id := getvariable('coord_new_lease'),
    resource := getvariable('coord_args').resource,
    fencing_value := getvariable('coord_fencing'),
    expires_at_ms := getvariable('coord_now') + getvariable('coord_args').ttl_ms,
    replayed := false, renewed := false, active := true)
END)
