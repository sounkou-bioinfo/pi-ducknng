-- Idempotently release an advisory resource lease owned by the caller.
SELECT coord_arguments($1::JSON, ['registration_id', 'lease_id']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_reg = coord_registration(
  coord_string($1::JSON, 'registration_id', 64, true, NULL), getvariable('coord_now'));
SET VARIABLE coord_lease_id = coord_string($1::JSON, 'lease_id', 64, true, NULL);
SET VARIABLE coord_lease = (
  SELECT struct_pack(resource := resource, fencing_value := fencing_value,
    released_at_ms := released_at_ms)
  FROM coordination_reservations
  WHERE lease_id = getvariable('coord_lease_id')
    AND project_id = getvariable('coord_reg').project_id
    AND owner_instance_id = getvariable('coord_reg').instance_id
);
SELECT CASE
  WHEN getvariable('coord_lease') IS NULL
    THEN coord_fail('lease_invalid', 'reservation lease is missing or belongs to another owner')
END;
UPDATE coordination_reservations
  SET released_at_ms = getvariable('coord_now'), expires_at_ms = getvariable('coord_now')
  WHERE lease_id = getvariable('coord_lease_id') AND released_at_ms IS NULL;
SELECT to_json(struct_pack(
  lease_id := getvariable('coord_lease_id'),
  resource := getvariable('coord_lease').resource,
  fencing_value := getvariable('coord_lease').fencing_value,
  released := true,
  replayed := getvariable('coord_lease').released_at_ms IS NOT NULL
))
