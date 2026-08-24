# ducknng API sealing checklist

This checklist tracks what still blocks calling the current public API sealed or stable enough that public names, contracts, and examples should stop changing casually. It is narrower than `docs/design_review_checklist.md`.

## Current position

`ducknng` has a real, tested public surface covering:

- service lifecycle control
- method registry administration
- raw request/reply helpers
- generic NNG socket patterns (pair, push/pull, pub/sub, surveyor/respondent, bus, poly)
- raw AIO handles for send, recv, and unary RPC
- query-session control helpers (open, fetch, close, cancel) — synchronous and AIO variants
- TLS config handles (self-signed and file-backed)
- scheme-routed server entrypoint covering NNG and HTTP/HTTPS families
- URL-routed synchronous request, RPC, and session helpers
- NNG WebSocket transport schemes through `ws://` and `wss://`
- low-level HTTP/HTTPS client helper (`ducknng_ncurl`) with AIO variant
- low-level HTTP route framework beside the framed RPC mount
- built-in content-type driven body codec helpers (raw, text, JSON, Arrow IPC, frame bodies; CSV/TSV/Parquet use the generic `body BLOB` fallback)
- user-extensible body codec hooks

## Resolved items

### 1. Session ownership and execution-lane policy

The public contract is stable: `query_open` returns a bearer `session_token`; `fetch`, `close`, and `cancel` must present it with the session id. mTLS-verified peer identity is an additional owner constraint when present. The default execution model is `shared_serialized_connection`; `service_serialized_connection` and `request_connection` are available through `ducknng_set_service_execution_model(...)`. Deployment profiles (local, trusted mesh, shared client, public) are the operator's responsibility and are documented in `docs/security.md`. **Resolved.**

### 2. Resource quotas for multi-client services

The stable baseline is in place: listener recv-size limits, descriptor request/reply-size limits, `max_open_sessions`, `max_active_pipes`, `max_inflight_requests`, and `max_sessions_per_peer_identity` set with `ducknng_set_service_limits(...)`. Per-principal in-flight caps, cumulative byte limits, and session-open rate limits are explicitly deferred — they are additive hardening and do not block sealing. **Resolved.**

### 3. Fetch payload decoding stance

`ducknng_fetch_query(...)` returns the Arrow IPC batch as `payload`. `ducknng_fetch_query_table(...)` decodes a single-fetch row path directly into a DuckDB table. Both reuse the same shared Arrow IPC decoder. **Resolved.**

### 4. HTTP async and web-server framework scope

`ducknng_start_server(...)` covers `http://` and `https://` listeners. Synchronous and AIO request/RPC/session helpers route over both carriers. `ducknng_ncurl_aio(...)` and `ducknng_ncurl_aio_collect(...)` provide async HTTP/HTTPS client. The low-level HTTP route framework (`ducknng_register_http_route`, `ducknng_register_http_route_pattern`, etc.) is part of the public SQL surface. Static assets, HTTP-carrier streaming, route-local authentication policy, and worker lifecycle management remain explicitly deferred as additive features. **Resolved.**

### 5. Transport matrix

The stable scheme matrix is documented in `docs/transports.md`: shared server surface accepts both NNG and HTTP families; generic socket surfaces are NNG-only; synchronous RPC/session helpers and raw RPC AIO helpers route across both families; TLS handles are accepted only on TLS-capable schemes (`tls+tcp://`, `wss://`, `https://`). **Resolved.**

### 6. Async surface scope

The stable async contract is raw-result-first. NNG and RPC AIO helpers collect raw frames through `ducknng_aio_collect(...)`. `ducknng_aio_collect_decoded(...)` layers structured convenience over the same substrate. Session-family AIO launchers cover open, fetch, close, and cancel. HTTP AIO helpers collect raw HTTP rows through `ducknng_ncurl_aio_collect(...)`. Synchronous raw session twins (`ducknng_open_query_raw`, `ducknng_fetch_query_raw`, `ducknng_close_query_raw`, `ducknng_cancel_query_raw`) return one reply frame directly. Scalar frame accessors (`ducknng_frame_version`, `ducknng_frame_type`, `ducknng_frame_flags`, `ducknng_frame_type_name`, `ducknng_frame_name`, `ducknng_frame_payload`, `ducknng_frame_payload_text`, `ducknng_frame_error_text`, `ducknng_frame_end_of_stream`) provide the route-safe decode path. **Resolved.**

### 7. Arrow type contract

The normative type set, emit-only projections, and explicitly deferred encodings are documented in `docs/types.md` and covered by `test/sql/ducknng_rpc_client_smoke.test`. The 0.1.0 contract covers: all scalar integer and float types, `VARCHAR`, `BLOB`, `DATE`, `TIME`, `TIME_NS`, all four timestamp units, `DECIMAL`/`HUGEINT` as `decimal128`, `UUID` as `utf8`, `TIMESTAMP_TZ` as timezone-free `timestamp[us]`, `ENUM` as `utf8`, `LIST`, `STRUCT`, `MAP` (emit-only), and `UNION` (emit-only; input decoder not yet implemented). Dictionary-preserving roundtrips, extension types, run-end encoding, `UNION` input decoding, and per-principal rate limits are all explicitly deferred and documented as such. **Resolved.**

### 8. Representative protocol examples and tests

All NNG socket patterns — pair, push/pull, pub/sub, surveyor/respondent, bus — have dedicated test coverage. The bus pattern specifically is covered in `test/sql/ducknng_bus_mesh.test` with both hub-and-spoke and fully-connected all-pairs topologies. RPC, session, AIO, TLS, HTTP, body codecs, execution models, pipe monitor, mTLS, IP allowlist, peer allowlist, SQL authorizer, service limits, negative paths, and lifecycle races all have dedicated test files. **Resolved.**

## Not sealing blockers

These remain important but do not need to be finished before the API is considered sealed:

- richer HTTP route-framework features (static assets, HTTP-carrier streaming, route-local authentication policy, worker lifecycle management, packaged gateway products)
- scalarfs-style in-memory filesystem/provider for CSV/TSV/Parquet body parsing; the generic `body BLOB` fallback is acceptable stable behaviour until a clean provider exists
- `UNION` input decoding — the emit path is stable; adding a decoder is an additive improvement
- per-principal in-flight caps, cumulative byte limits, and session-open rate limits — service-level limits are the stable contract; per-principal hardening is additive
- large UTF-8 / large binary / fixed-size binary / duration / timezone-aware timestamp — expansion-tier work
- a future DuckDB-native Arrow re-plumb using `unstable_new_arrow_functions` if that API stabilises

## Seal status

All must-resolve items above are resolved. The 0.1.0 public API is **sealed**: names, contracts, and method schemas in the current implementation and documentation are stable and should not change casually. Additive features, deferred encodings, and hardening work proceed without breaking the sealed surface.
