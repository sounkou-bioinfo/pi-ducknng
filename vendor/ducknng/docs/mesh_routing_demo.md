# Mesh routing demo sketch

This is a design sketch for a future demo, not a sealed public routing API.

`ducknng` now has enough low-level telemetry to demonstrate a small DuckDB service mesh without adding a new wire protocol. The demo should compose existing surfaces:

- `ducknng_list_servers()` for local service state, limits, active pipe count, and in-flight request count.
- `ducknng_list_pipes(name)` for the current active NNG pipe snapshot.
- `ducknng_read_monitor(name, after_seq, max_events)` for ordered pipe add/remove/admission events.
- `ducknng_monitor_status(name)` for event ring health, dropped-event detection, and current limits.
- generic NNG `req` sockets for forwarding one framed request at a time.
- framed RPC helpers for manifest discovery and session/query calls.

## Demo topology

A minimal demo can use three roles:

1. **workers** expose framed RPC services over `tcp://` or `tls+tcp://`.
2. **router** keeps a SQL route table and forwards client requests to workers.
3. **client** talks only to the router.

The router should not invent a fake protocol. It should either:

- forward complete `ducknng` frames over real NNG REQ sockets, or
- use existing client helper calls to selected worker URLs.

## Suggested SQL state

A demo route table can be ordinary DuckDB state:

```sql
CREATE TABLE mesh_workers (
  worker_id VARCHAR PRIMARY KEY,
  url VARCHAR NOT NULL,
  last_seen_ms UBIGINT,
  active_pipes UBIGINT DEFAULT 0,
  inflight_requests UBIGINT DEFAULT 0,
  healthy BOOLEAN DEFAULT true
);
```

A monitor cursor table can remember per-service event progress:

```sql
CREATE TABLE mesh_monitor_cursors (
  service_name VARCHAR PRIMARY KEY,
  last_seq UBIGINT DEFAULT 0
);
```

## Membership loop

The router periodically reads monitor events:

```sql
SELECT *
FROM ducknng_read_monitor('router_rpc', last_seq, 1000::UBIGINT);
```

For each batch it should:

- advance `last_seq` only after processing the batch;
- treat `add_post` as a possible new member/presence signal;
- treat `rem_post` as a disconnect/churn signal;
- record denied `add_pre` events and their `reason` for admission audit;
- compare `last_seq` with `ducknng_monitor_status()` so dropped events trigger a full reconciliation.

If `dropped_events` increases, the demo should rebuild current membership from `ducknng_list_pipes()` and worker health checks rather than trusting a partial event stream.

## Routing policy

A simple first policy can route to the worker with the lowest observed pressure:

```sql
ORDER BY healthy DESC,
         inflight_requests ASC,
         active_pipes ASC,
         last_seen_ms DESC
LIMIT 1
```

This policy intentionally uses live telemetry outside the manifest. The manifest remains capability and policy metadata; routing decisions use `ducknng_list_servers()`, monitor status, and application-owned worker tables.

## Backpressure

The demo should configure conservative service limits before accepting client traffic:

```sql
SELECT ducknng_set_service_limits(
  'router_rpc',
  64::UBIGINT,   -- max_open_sessions
  256::UBIGINT,  -- max_active_pipes
  16::UBIGINT,   -- max_inflight_requests
  4::UBIGINT     -- max_sessions_per_peer_identity
);
```

Workers can use smaller caps. A rejected request with `ducknng: max inflight requests exceeded` should make the router retry another healthy worker only when the method is safe to retry. Sessionful methods must preserve session affinity once `query_open` succeeds.

## Session affinity

For query sessions, the router must remember which worker owns each session:

```sql
CREATE TABLE mesh_sessions (
  client_session_id UBIGINT,
  worker_id VARCHAR NOT NULL,
  worker_session_id UBIGINT NOT NULL,
  session_token VARCHAR NOT NULL,
  PRIMARY KEY (client_session_id)
);
```

`fetch`, `close`, and `cancel` must go back to the same worker. `session_id` is a lookup key only; `session_token` remains the bearer capability. If mTLS is used, worker-side identity binding still applies.

## Non-goals for the demo

- no fake HELLO/PING wire messages;
- no HTTP-specific copies of RPC methods;
- no manifest mutation with live counters;
- no sealed routing API until the demo proves the shape;
- no forwarding of unsafe `exec` unless explicitly enabled and deployment-gated.

## Open questions

- whether to expose a dedicated SQL helper for frame forwarding, or keep forwarding in demo SQL/client code;
- how to represent per-worker method capability differences if workers have different manifests;
- how to bound retry fan-out and prevent thundering herds;
- whether a future router should use SQL authorizer principals as quota owners.
