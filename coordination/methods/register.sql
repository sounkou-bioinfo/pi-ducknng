-- Create or resume one project-scoped agent instance lease. A verified caller
-- needs a grant for the (project, agent) pair, and an instance stays bound to
-- the identity that first registered it.
SELECT coord_arguments($1::JSON, ['project_id', 'agent_id', 'instance_id', 'operation_key',
  'display_name', 'adapter_kind', 'delivery_capability', 'ttl_ms']);
SET VARIABLE coord_now = coord_now();
SET VARIABLE coord_args = (
  SELECT struct_pack(
    project_id := coord_string(p, 'project_id', 128, true, NULL),
    agent_id := coord_string(p, 'agent_id', 128, true, NULL),
    instance_id := coord_string(p, 'instance_id', 256, true, NULL),
    operation_key := coord_string(p, 'operation_key', 256, true, NULL),
    display_name := coord_string(p, 'display_name', 256, false, NULL),
    adapter_kind := coord_string(p, 'adapter_kind', 64, false, 'unspecified'),
    delivery_capability := coord_string(p, 'delivery_capability', 64, false, 'unspecified'),
    ttl_ms := coord_integer(p, 'ttl_ms', 30000, 5000, 300000),
    caller := coord_caller()
  )
  FROM (SELECT $1::JSON AS p)
);
SELECT CASE
  WHEN a.caller IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM coordination_grants g
    WHERE g.peer_identity = a.caller AND g.project_id = a.project_id
      AND (g.agent_id = a.agent_id OR g.agent_id = '*')
  ) THEN coord_fail('unauthorized', a.caller || ' has no grant for '
    || a.project_id || '/' || a.agent_id)
  WHEN EXISTS (
    SELECT 1 FROM coordination_agents c
    WHERE c.project_id = a.project_id AND c.instance_id = a.instance_id
      AND c.agent_id <> a.agent_id
  ) THEN coord_fail('invalid_argument', 'instance_id is already bound to another agent_id')
  WHEN EXISTS (
    SELECT 1 FROM coordination_agents c
    WHERE c.project_id = a.project_id AND c.instance_id = a.instance_id
      AND c.peer_identity IS DISTINCT FROM a.caller
  ) THEN coord_fail('unauthorized', 'instance_id is bound to another peer identity')
END
FROM (SELECT unnest(getvariable('coord_args'))) AS a;
SET VARIABLE coord_reuse = (
  SELECT c.registration_id
  FROM coordination_agents c, (SELECT unnest(getvariable('coord_args'))) AS a
  WHERE c.project_id = a.project_id AND c.instance_id = a.instance_id
    AND c.operation_key = a.operation_key AND c.unregistered_at_ms IS NULL
    AND c.expires_at_ms > getvariable('coord_now')
);
SET VARIABLE coord_registration_id = coalesce(getvariable('coord_reuse'), uuid()::VARCHAR);
INSERT INTO coordination_agents (
  project_id, agent_id, instance_id, registration_id, operation_key, display_name,
  adapter_kind, delivery_capability, peer_identity, status_json, registered_at_ms,
  heartbeat_at_ms, expires_at_ms, unregistered_at_ms
)
SELECT
  a.project_id, a.agent_id, a.instance_id, getvariable('coord_registration_id'),
  a.operation_key, coalesce(a.display_name, a.agent_id), a.adapter_kind,
  a.delivery_capability, a.caller, '{}',
  coalesce(
    (SELECT registered_at_ms FROM coordination_agents
      WHERE registration_id = getvariable('coord_reuse')),
    getvariable('coord_now')
  ),
  getvariable('coord_now'), getvariable('coord_now') + a.ttl_ms, NULL
FROM (SELECT unnest(getvariable('coord_args'))) AS a
ON CONFLICT (project_id, instance_id) DO UPDATE SET
  agent_id = excluded.agent_id,
  registration_id = excluded.registration_id,
  operation_key = excluded.operation_key,
  display_name = excluded.display_name,
  adapter_kind = excluded.adapter_kind,
  delivery_capability = excluded.delivery_capability,
  peer_identity = excluded.peer_identity,
  status_json = '{}',
  registered_at_ms = excluded.registered_at_ms,
  heartbeat_at_ms = excluded.heartbeat_at_ms,
  expires_at_ms = excluded.expires_at_ms,
  unregistered_at_ms = NULL;
SELECT to_json(struct_pack(
  registration_id := getvariable('coord_registration_id'),
  project_id := a.project_id,
  agent_id := a.agent_id,
  instance_id := a.instance_id,
  server_time_ms := getvariable('coord_now'),
  expires_at_ms := getvariable('coord_now') + a.ttl_ms,
  heartbeat_interval_ms := a.ttl_ms // 3,
  delivery_capability := a.delivery_capability,
  replayed := getvariable('coord_reuse') IS NOT NULL
))
FROM (SELECT unnest(getvariable('coord_args'))) AS a
