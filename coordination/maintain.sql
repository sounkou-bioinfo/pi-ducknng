-- Prepended to write methods. Runs at most once per second: returns expired
-- delivery leases to the queue, turns overdue queued mail into dead letters,
-- and deletes records whose retention ended.
SET VARIABLE coord_maintain = (
  SELECT coord_now() - maintained_at_ms >= 1000 FROM coordination_meta
);
UPDATE coordination_meta SET maintained_at_ms = coord_now()
  WHERE getvariable('coord_maintain');
UPDATE coordination_messages
  SET state = 'queued', receipt_token = NULL, lease_batch = NULL, lease_expires_at_ms = NULL
  WHERE getvariable('coord_maintain') AND state = 'leased' AND lease_expires_at_ms <= coord_now();
UPDATE coordination_messages SET state = 'expired', expired_at_ms = coord_now()
  WHERE getvariable('coord_maintain') AND state = 'queued' AND expires_at_ms <= coord_now();
DELETE FROM coordination_messages
  WHERE getvariable('coord_maintain') AND (
    (state = 'acked' AND acked_at_ms < coord_cutoff())
    OR (state = 'expired' AND expired_at_ms < coord_cutoff())
  );
DELETE FROM coordination_reservations
  WHERE getvariable('coord_maintain') AND (
    (released_at_ms IS NOT NULL AND released_at_ms < coord_cutoff())
    OR (released_at_ms IS NULL AND expires_at_ms < coord_cutoff())
  );
DELETE FROM coordination_agents
  WHERE getvariable('coord_maintain') AND (
    (unregistered_at_ms IS NOT NULL AND unregistered_at_ms < coord_cutoff())
    OR (unregistered_at_ms IS NULL AND expires_at_ms < coord_cutoff())
  );
