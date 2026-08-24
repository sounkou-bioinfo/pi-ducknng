# News

## ducknng 0.1.1 — community submission release

Community-extension submission to duckdb/community-extensions (#1904).

- Vendored NNG 1.11.0 transport layer: inproc://, ipc://, tcp://, tls+tcp://, ws://, wss://.
- HTTP carrier over http:// and https:// via NNG HTTP framework.
- Scoped outbound HTTP profiles for `ducknng_ncurl(...)`, `ducknng_ncurl_aio(...)`, and `ducknng_ncurl_table(...)`: profile credentials are resolved inside the HTTP client path after fail-closed scheme/host/port/segment-aware-path/method/TLS checks, caller auth-header collisions and control-character header values are rejected, Unix-epoch expiry is enforced consistently across platforms, and `ducknng_list_http_profiles()` redacts secret values.
- The self-published release workflow now smoke-checks the outbound HTTP profile path and compatibility arities on backport release binaries.

- The generated SQL catalog spans Server, NNG Sockets, Framed RPC, HTTP Client,
  HTTP Routes, HTTP Workers, HTTP Helpers, Body Codecs, TLS, Service, Admission,
  Monitoring, Async I/O, Method Registry, Frame Helpers, SQL Macros.
- Cross-platform Linux/macOS; Windows requires MinGW/Rtools (MSVC excluded for MbedTLS).
- All 20+ sqllogictest integration tests pass.
- Full README rendered at https://github.com/sounkou-bioinfo/ducknng

## ducknng 0.1.0 — first sealed release

The 0.1.0 public API is sealed. Names, contracts, and method schemas in the current implementation and documentation are stable and should not change casually. All must-resolve items in `docs/api_sealing_checklist.md` are resolved.

### What is included and stable

**Transport.** `ducknng_start_server(...)` accepts `inproc://`, `ipc://`, `tcp://`, `tls+tcp://`, `ws://`, `wss://`, `http://`, and `https://`. The URL scheme selects the transport family; NNG and HTTP/HTTPS share one server entrypoint with `contexts = 1` for the HTTP carrier. Synchronous and AIO request, RPC, and session helpers route by scheme automatically.

**Socket patterns.** All six NNG socket families are supported and tested with dedicated coverage: pair, push/pull, pub/sub, surveyor/respondent, bus (hub-and-spoke broadcast and fully-connected all-pairs in `test/sql/ducknng_bus_mesh.test`), and poly.

**AIO.** Raw request, unary RPC, session (open/fetch/close/cancel), and HTTP AIO helpers share one handle lifecycle: launch returns a handle, `ducknng_aio_collect(...)` collects or polls, `ducknng_aio_drop(...)` releases. `ducknng_aio_collect_decoded(...)` layers structured convenience. `ducknng_aio_status(...)` exposes terminal state. `ducknng_aio_wait(...)` waits without collecting.

**Query sessions.** `query_open` returns a bearer `session_token`; `fetch`, `close`, and `cancel` must present it with the session id. mTLS-verified peer identity is an additional owner constraint when present. `ducknng_fetch_query_table(...)` is the one-fetch decoded-table convenience path. Raw session variants (`ducknng_open_query_raw`, `ducknng_fetch_query_raw`, `ducknng_close_query_raw`, `ducknng_cancel_query_raw`) return one reply frame for use inside HTTP route handlers.

**Execution models.** `ducknng_set_service_execution_model(...)` switches between `shared_serialized_connection` (default), `service_serialized_connection`, and `request_connection`.

**TLS.** `ducknng_self_signed_tls_config(...)` and `ducknng_tls_config_from_files(...)` accept PEM material from paths or in-memory strings. Handles are reusable across servers and client calls.

**HTTP route framework.** `ducknng_register_http_route(...)` and `ducknng_register_http_route_pattern(...)` mount exact, prefix, and template routes beside the framed RPC endpoint. Strict header/query/cookie/path parsers, named request accessors, and one-row response-builder helpers are part of the public surface. Routes are not manifest methods.

**Body codecs.** `ducknng_parse_body(blob, content_type)` dispatches to built-in decoders for JSON, Arrow IPC, ducknng frames, text, and raw. CSV/TSV/Parquet are recognised but use the `body BLOB` fallback. `ducknng_register_codec(...)` and `ducknng_unregister_codec(...)` extend the set at runtime.

**Method registry and manifest.** Methods are registered through descriptors and appear in the manifest exported by `ducknng_get_rpc_manifest(...)` and the built-in `manifest` RPC method.

**Admission and security.** Fast C admission: required mTLS, exact peer-identity allowlists, IP/CIDR allowlists, and service limits (`max_open_sessions`, `max_active_pipes`, `max_inflight_requests`, `max_sessions_per_peer_identity`). SQL authorizer callbacks via `ducknng_set_service_sql_authorizer(...)`. Per-principal in-flight caps, cumulative byte limits, and session-open rate limits are explicitly deferred — service-level limits are the stable admission contract.

**Arrow IPC type contract.** Normative two-way types: `BOOLEAN`, `TINYINT`–`BIGINT`, `UTINYINT`–`UBIGINT`, `FLOAT`, `DOUBLE`, `VARCHAR`, `BLOB`, `DATE`, `TIME`, `TIME_NS`, `TIMESTAMP_S`/`MS`/`US`/`NS`, `DECIMAL`, `LIST`, `STRUCT`. Emit-only projections: `HUGEINT` as `decimal128(38,0)`, `UUID` as `utf8`, `TIMESTAMP_TZ` as timezone-free `timestamp[us]`, `ENUM` as resolved `utf8`, `MAP` as Arrow `map`, `UNION` as Arrow `dense_union`. Explicitly deferred: dictionary-preserving roundtrips, extension types, run-end encoding, `UNION` input decoding, per-principal rate limits, large UTF-8/binary, fixed-size binary, duration, timezone-aware timestamp semantics. See `docs/types.md`.

**Pipe monitor.** `ducknng_read_monitor(...)` and `ducknng_list_pipes(...)` expose per-service NNG pipe event telemetry.

**Tests.** 19 SQL test files: server start/stop, socket protocols, bus mesh, mTLS auth, pipe monitor, HTTP client contract, HTTP framework, HTTP server routing, service limits, body codecs, SQL authorizer, RPC client smoke (full Arrow type roundtrip), public surface, negative paths, lifecycle races, execution models, IP allowlist, peer allowlist, and WebSocket transports.

**README.** Rendered using [duckknit](https://github.com/rundel/duckknit) for persistent inter-chunk DuckDB sessions. All `{duckdb}` chunks use a `document` hook that rewrites fences to ` ```sql ` for GitHub syntax highlighting.

### What is explicitly deferred (not blocking 0.1.0)

- Dictionary-preserving Arrow roundtrips, extension types, run-end encoding, `UNION` input decoding
- Per-principal in-flight caps, cumulative byte limits, session-open rate limits
- Large UTF-8/binary, fixed-size binary, duration, timezone-aware timestamps
- Static assets, HTTP-carrier streaming, route-local authentication policy, worker lifecycle management
- CSV/TSV/Parquet body parsing via a memory-backed reader (generic `body BLOB` fallback is stable)
- Future DuckDB-native Arrow re-plumb using `unstable_new_arrow_functions` if that API stabilises

---

### Detailed change history

- Made synchronous low-level socket helpers return one in-band result struct with `ok`, `error`, nullable `nng_error`, nullable `nng_error_message`, `socket_id`, `payload`, and `url`, so validation failures and NNG failures no longer have to surface as DuckDB statement errors in normal socket workflows.
- Made framed raw RPC/session helpers return local `ducknng` error frames for request setup and transport failures instead of collapsing them to `NULL`, and taught `ducknng_decode_frame(...)` to surface frame error text in its `error` column with `ok = false` for error frames.
- Made AIO launch helpers return immediate terminal error handles for expected setup and launch failures, so callers can inspect those failures through `ducknng_aio_status(...)`, `ducknng_aio_collect(...)`, or `ducknng_ncurl_aio_collect(...)` instead of catching a DuckDB exception at launch time.
- Reworked community-extension submission generation to follow the `duckhts` pattern: `function_catalog/functions.yaml` now carries a `community_extension` block, `function_catalog/generate_function_catalog.py` renders both the local catalog and the DuckDB community descriptor from one manifest plus the repo `description.yml` version, and the rendered submission lands in `community-extensions/extensions/ducknng/description.yml` through a checked-in template.
- Added public `ducknng_aio_wait(...)` as the AIO lifecycle primitive for waiting on one of several handles without collecting or dropping the result, and removed direct test reliance on the internal macro helper that previously backed collection.
- Tightened HTTP header JSON handling so request/response header adapters reject trailing garbage and non-token header names consistently with the SQL header-builder contract.
- Sealed the small HTTP route toolkit surface around strict header/query/cookie/path accessors plus one-row response-builder macros, updated the generated function catalog to match the loaded SQL surface, and removed the unused destructive internal `ducknng__aio_mark_collected` helper from registration.
- Updated extension metadata to describe the current product shape: DuckDB SQL and manifest-declared RPC over NNG and HTTP with Arrow IPC payloads.
- Renamed the multi-process topology docs and demo from the earlier branded wording to the generic subscriber-gateway shape: `docs/subscriber_gateway_demo.md`, `demo/subscriber_gateway.py`, `make subscriber_gateway_demo`, and `make subscriber_gateway_rdm`.
- Split the subscriber gateway demo into dedicated support modules while keeping `demo/subscriber_gateway.py` as the stable public entrypoint for `make subscriber_gateway_demo` and the rendered Rmd walkthrough.
- Added service-side DuckDB execution models via `ducknng_set_service_execution_model(...)`: the default `shared_serialized_connection`, service-local `service_serialized_connection`, and per-handler `request_connection`.
- Added additive low-level HTTP route patterns with `ducknng_register_http_route_pattern(...)` and `ducknng_unregister_http_route_pattern(...)`, covering `exact`, `prefix`, and `template` matching, plus `route_match_kind` and `path_params_json` in `ducknng_http_request()` and `match_kind` in `ducknng_list_http_routes()`.
- Added `ducknng_aio_collect_decoded(...)` as the first structured async convenience wrapper over the existing raw-frame aio substrate, projecting decoded envelope columns directly without changing the underlying one-operation aio contract.
- Centralized the remaining duplicated ASCII case-folding helpers and introduced an explicit internal runtime execution-lane abstraction over the current `shared_serialized_connection` model, so the code no longer hard-codes `init_con` as the only future execution-policy boundary.
- Added `make check_news` plus `scripts/check_news.py` so user-visible changes in `src/`, `docs/`, `demo/`, `README.Rmd`, and the main build workflow now fail locally unless `NEWS.md` is updated too; `BASE=<ref>` makes the same check usable on commit ranges in CI.
- Added `make docs` as the umbrella documentation target over the rendered README and the dedicated subscriber-gateway walkthrough, instead of keeping those render paths as separate ad hoc commands.
- Removed the temporary `ducknng_start_http_server(...)` alias and made `ducknng_start_server(...)` the single scheme-routed server entrypoint across NNG and HTTP/HTTPS, with `contexts = 1` as the explicit HTTP carrier rule.
- Made the NNG request aio path honest end to end: URL-launched raw request aio no longer blocks on the initial dial, raw RPC aio helpers route by URL scheme, and HTTP/HTTPS now shares the same raw aio request family instead of living on a separate dual track.
- Unified Arrow row decoding into a shared internal layer, rebased `ducknng_query_rpc(...)` onto the explicit session lifecycle, added `ducknng_fetch_query_table(...)` as the one-fetch decoded-table convenience path, and added raw async session launchers for `open`, `fetch`, `close`, and `cancel`.
- Froze and documented the current service execution contract as `shared_serialized_connection`, exported it in `ducknng_list_servers()` and the `manifest` server metadata, and clarified that multi-process gateway or worker topologies are the honest scaling and isolation path on the stable DuckDB C API boundary.
- Centralized the SQL helper and registration scaffolding: shared argument/null/blob helpers, scalar and table-function registration, logical-type registration, manifest metadata projection, session-control parsing, and service execution-lane helpers now live in shared internal layers instead of being re-open-coded across the SQL modules.
- Landed the low-level exact-path HTTP route framework as a real public SQL surface with `ducknng_register_http_route(...)`, `ducknng_unregister_http_route(...)`, `ducknng_list_http_routes()`, `ducknng_http_request()`, and `ducknng_http_request_body()`, while keeping it explicitly separate from the manifest-derived RPC method surface.
- Added raw session route primitives `ducknng_open_query_raw(...)`, `ducknng_fetch_query_raw(...)`, `ducknng_close_query_raw(...)`, `ducknng_cancel_query_raw(...)`, plus `ducknng_frame_payload(...)`, `ducknng_frame_payload_text(...)`, and `ducknng_frame_error_text(...)` so route SQL can drive per-request gateway session control without bind-time table-function problems.
- Added a real multi-process subscriber gateway demo with a public HTTP edge, bearer-auth tenant resolution, table-driven subscriber discovery, private backend query sessions, cross-tenant close rejection, and dedicated private workers behind the gateway. The live helper now lives in `demo/subscriber_gateway.py`, and `make subscriber_gateway_rdm` renders a dedicated `demo/subscriber_gateway.Rmd` walkthrough beside the end-to-end `make subscriber_gateway_demo` check.
- Implemented the first real HTTP/HTTPS server slice over the existing registry-backed framed RPC surface, including adapter-level `405` / `415` / `400` handling and `200 OK` frame replies for protocol-level success and failure. This now lives under the scheme-routed `ducknng_start_server(...)` entrypoint.
- Added `ducknng_ncurl_aio(...)` and `ducknng_ncurl_aio_collect(...)` as the nanonext-style asynchronous HTTP/HTTPS client slice, preserving the raw `ducknng_ncurl(...)` status/header/body contract while using the same future-like aio handle lifecycle as other async helpers.
- Taught the synchronous request, RPC, and session helper family to route by URL scheme so `ducknng_request(...)`, `ducknng_request_raw(...)`, `ducknng_get_rpc_manifest(...)`, `ducknng_get_rpc_manifest_raw(...)`, `ducknng_run_rpc(...)`, `ducknng_run_rpc_raw(...)`, `ducknng_query_rpc(...)`, `ducknng_open_query(...)`, `ducknng_fetch_query(...)`, `ducknng_close_query(...)`, and `ducknng_cancel_query(...)` now work over `http://` and `https://` without minting a second RPC surface.
- Enabled the vendored NNG `ws://` and `wss://` transports and documented them as part of the NNG transport family rather than as part of the HTTP carrier layer.
- Extended `ducknng_dial_socket(...)` to take `tls_config_id` and applied the same reusable TLS-handle model to generic socket dialing, including `wss://`.
- Changed synchronous `ducknng_request_socket(...)` to use the already-connected req socket handle directly instead of silently re-dialing from the stored URL, so socket-handle dialing and TLS settings actually carry through to the request path.
- Added sqllogictest coverage for the new HTTP server/routed-helper surface and for `ws://` / `wss://` transport use through both service and socket-handle paths.
- Hardened client socket lifetime management with runtime-owned retain/release tracking so close/destroy waits for in-flight users, pending socket-bound aio operations keep their socket alive while they are actually pending, and terminal aio handles no longer block later socket close just because the caller has not dropped the collected aio row yet.
- Hardened server-side query session lifetime tracking so `fetch` now runs against an acquired session reference instead of an unlocked borrowed pointer, close/cancel/prune/stop detach sessions before destroy and wait for in-flight users to drain, service introspection reads a published session-count snapshot instead of re-locking service state from inside service-owned SQL, and `query_open` now refuses to publish a fresh session while shutdown is already in progress.
- Hardened service-owned SQL execution and HTTP shutdown around `init_con`: service-side `exec` / `query_open` now serialize through a runtime-owned init-connection gate, HTTP shutdown waits for handler finalization before freeing handler-owned state, and self-stop from a service's own request path is rejected instead of deadlocking or tearing the service out from under the active request.
- Expanded Arrow IPC row mapping for unary `ducknng_query_rpc(...)` / `exec(..., want_result = true)` paths beyond the initial scalar subset to include `DATE`, `TIME`, timezone-free `TIMESTAMP` units, DuckDB `DECIMAL` via Arrow `decimal128`, Arrow lists as DuckDB lists, and Arrow structs as DuckDB structs, with sqllogictest coverage for scalar temporal/decimal and nested list/struct roundtrips.
- Added the first content-type driven body codec provider layer: `ducknng_list_codecs()`, `ducknng_parse_body(body, content_type)`, and `ducknng_ncurl_table(...)` now expose built-in raw/text/JSON/Arrow IPC/frame parsing while keeping `ducknng_ncurl(...)` raw and keeping the HTTP server RPC endpoint frame-only. JSON parsing now stays in memory through DuckDB JSON functions; CSV/TSV/Parquet media types are recognized but use the generic `body BLOB` fallback until a scalarfs-style memory filesystem/provider path lands. The HTTP frame endpoint now also accepts the frame media type case-insensitively with parameters.
- Bound query sessions to generated bearer capabilities: `query_open` / `ducknng_open_query(...)` now return `session_token`, and `fetch`, `close`, and `cancel` reject calls that present a matching `session_id` without the matching token. `query_open` replies also expose the server-owned effective `idle_timeout_ms` so clients can see the session cleanup policy without controlling it.
- Extended `ducknng_register_exec_method(...)` with an optional `requires_auth` boolean so deployments can register `exec` with descriptor-level verified peer identity enforcement instead of relying only on listener-wide mTLS policy.
- Added `ducknng_set_method_auth(name, requires_auth)` so deployments can protect registry-backed RPC methods such as `manifest` through the same descriptor-level verified-peer-identity policy, and made unregistration refuse to remove sessionful methods or families while sessions are open.
- Added transport-derived mTLS caller identity for `tls+tcp://`, `wss://`, and `https://` service requests. TLS authentication mode `2` now requires a verified peer certificate identity before dispatch; sessions opened over verified mTLS are bound to that identity in addition to the bearer `session_token`, and `ducknng_list_servers()` exposes `tls_enabled`, `tls_auth_mode`, and `peer_identity_required`.
- Added dynamic exact peer-identity allowlists with `ducknng_set_tls_peer_allowlist(...)` for TLS handles and `ducknng_set_service_peer_allowlist(...)` for running services. NNG listeners use `NNG_PIPE_EV_ADD_PRE` pipe notifications to close non-admitted new pipes before they reach the socket; HTTP/HTTPS returns `403` before RPC dispatch.
- Added service-level IP/CIDR admission through optional `ip_allowlist_json` startup arguments and `ducknng_set_service_ip_allowlist(...)`. NNG services use `NNG_OPT_REMADDR` on pipes for efficient parsed-CIDR checks during `ADD_PRE` and dispatch; HTTP/HTTPS checks the connection remote address before framed RPC dispatch and returns `403` for non-admitted addresses.
- Added service-level SQL authorization callbacks through `ducknng_set_service_authorizer(...)` and `ducknng_auth_context()`. Fast C denials for mTLS, peer identity allowlists, and IP/CIDR allowlists remain the low-latency path; SQL callbacks run uniformly at the request/dispatch boundary for NNG and HTTP/HTTPS framed RPC where they can inspect transport, remote address, HTTP metadata, and RPC method context.
- Continued splitting SQL registration out of `src/ducknng_sql_api.c`: SQL authorization lives in `src/ducknng_sql_auth.c`, monitor/pipe telemetry in `src/ducknng_sql_monitor.c`, service introspection/limits in `src/ducknng_sql_service.c`, TLS SQL bindings in `src/ducknng_sql_tls.c`, generic socket scalar/listing bindings in `src/ducknng_sql_socket.c`, AIO launch/status/collect bindings in `src/ducknng_sql_aio.c`, and method registry administration/introspection in `src/ducknng_sql_registry.c`, query-session SQL helpers in `src/ducknng_sql_session.c`, body/codec/frame decoding helpers in `src/ducknng_sql_body.c`, and one-shot RPC/client bindings in `src/ducknng_sql_rpc.c`.
- Added lifecycle and negative-path coverage for stopping services with live sessions, rejecting same-service stop while a service-owned SQL authorizer request is active, unsupported URL schemes across RPC/session/HTTP client helpers, and supplied TLS configuration on non-TLS URL schemes.
- Hardened NNG client URL validation so one-shot request/RPC/session helpers, raw RPC AIO launch, and generic socket dialing reject TLS configuration unless the URL uses `tls+tcp://` or `wss://`.
- Clarified the remote SQL security contract: arbitrary SQL execution is a deployment-owned capability, while `ducknng`'s responsibility is safe internal SQL construction, explicit auth/admission/resource controls, and documented exposure profiles.
- Sealed the current query-session ownership contract around `session_token` plus optional verified mTLS owner identity; future envelope-level application authentication is treated as additive rather than a prerequisite for the current session family.
- Clarified resource-quota ownership: current built-in quotas are service-wide plus verified-peer-identity session caps; SQL-authorizer principals remain deployment policy/audit metadata until a future principal-owned quota model lands.
- Documented and tested SQL-side decoding of `ducknng_fetch_query(...)` Arrow IPC payloads through `ducknng_parse_body(payload, 'application/vnd.apache.arrow.stream')`.
- Froze the async contract as raw-result-first: NNG/RPC AIO collection returns frames, HTTP AIO collection returns HTTP-shaped rows, and structured async wrappers must remain layered conveniences rather than a second job protocol.
- Added a stable transport scheme matrix covering which SQL surfaces accept NNG schemes, HTTP/HTTPS schemes, and TLS handles, and expanded AIO lifecycle/family-mismatch regression coverage.
- Extended `ducknng_set_service_limits(name, max_open_sessions[, max_active_pipes[, max_inflight_requests[, max_sessions_per_peer_identity]]])`; the manifest continues to expose server-owned session policy, while `ducknng_list_servers()` exposes live service limits. Exceeding the service-wide session cap makes `query_open` fail with `ducknng: max open sessions exceeded`, the per-verified-peer-identity session cap fails with `ducknng: max sessions per peer identity exceeded`, the active-pipe cap rejects excess NNG pipes at `ADD_PRE`, and the in-flight cap rejects excess requests before SQL authorizers and dispatch.
- Added `ducknng_read_monitor(name, after_seq, max_events)` for bounded per-service NNG pipe event streams based on `nng_pipe_notify(..., ADD_PRE/ADD_POST/REM_POST, ...)`, including sequence, timestamp, pipe id, transport, admission result, denial reason, remote address, and verified peer identity when available. Added `ducknng_monitor_status(name)` for ring/counter metadata and `ducknng_list_pipes(name)` for the current active NNG pipe snapshot, plus `active_pipes`/`max_active_pipes` and `inflight_requests`/`max_inflight_requests` in `ducknng_list_servers()`.
- Added sqllogictest coverage for mTLS manifest roundtrips over NNG TLS, WSS, and the HTTPS frame carrier, plus query-session identity binding over NNG TLS, wrong-identity `fetch` / `close` / `cancel` failures, required-mTLS no-certificate rejection, and optional-auth token-only versus identity-bound session behavior.
- Added the first raw unary RPC aio wrappers: `ducknng_get_rpc_manifest_raw_aio(...)` and `ducknng_run_rpc_raw_aio(...)`.
- Added a documented local NNG patch under `patches/nng/` so the vendored Windows clock fallback for DuckDB CI's Rtools42 MinGW environment is explicit rather than an undocumented edit inside `third_party/nng/`.
- Fixed the next Windows MinGW portability blocker after the vendored NNG gate: self-signed TLS material generation now uses portable temp-directory helpers instead of calling `mkdtemp()` directly from `ducknng_sql_api.c`.
- Replaced the old no-op Windows stubs in `ducknng_util.c` with real Win32 implementations for time, sleep, threads, mutexes, and condition variables so runtime-owned services and aio state no longer depend on POSIX-only helper behavior on Windows builds.
- Fixed several concrete teardown and lifetime hazards: `ducknng_runtime_destroy(...)` now disconnects the init connection, removes the runtime from the global registry before teardown, cleans up services before client sockets, and fully frees its owned structures; service stop/teardown now releases per-context aio state safely and frees restartable allocations; SQL registration no longer shares one static cross-database context pointer; method error replies no longer read DuckDB result error text after destroying the result object; manifest JSON now uses DuckDB allocators consistently; and async send teardown now drains pending `nng_msg` ownership instead of leaving messages attached to freed aio objects.
- Added `docs/api_sealing_checklist.md` to track what still blocks calling the current public API sealed, including session ownership and execution-isolation questions. Added `docs/mesh_routing_demo.md` as a non-sealed design sketch for composing monitor, pipe, and service telemetry into a future routing demo.
- Expanded the README with runnable `push` / `pull`, `pub` / `sub`, and `surveyor` / `respondent` raw messaging examples and matched them with sqllogictest coverage instead of leaving the broader protocol family only implied by the function list.
- Retitled the project in the README and package metadata as a DuckDB extension exposing DuckDB SQL and manifest-declared RPC over NNG and HTTP with Arrow IPC payloads, instead of the older too-narrow REQ/REP-only framing.
- Added a short getting-started section near the top of the README and wrapped the generated function catalog in a foldable block so the README no longer opens with a long wall of generated catalog content.
- Made the README getting-started SQL actually execute during rendering instead of showing only static unevaluated snippets.
- Stopped the rendered README from leaking machine-local absolute extension paths; the examples now use relative extension paths instead.
- Added `docs/lifetime.md` and a matching README section to document the current low-level manual-lifecycle contract: DuckDB gives destroy callbacks for extension-internal function/bind/init state, but not a nanonext-style GC/finalizer model for long-lived SQL handles, so servers, sockets, aio handles, TLS configs, and query sessions still need explicit cleanup.
- Cleaned README wording so the lifetime section states the contract directly without clunky meta-labels, clarified that the HTTPS hello example is a `ducknng_ncurl()` call against a local `nanonext` HTTPS server, made that README HTTPS setup more robust by polling the local server before the SQL example runs instead of relying on a fixed one-second sleep, and reformatted the visible README code so long lines do not force unnecessary horizontal scrolling.
- Removed the stale `docs/design_review.md` snapshot instead of leaving it in the repo as if it were current documentation, and updated `docs/design_review_checklist.md` to stand on its own.
- Clarified the README and protocol docs so the layering is explicit: the generic socket layer is the transport substrate, higher-level RPC helpers wrap manifest-declared request/reply methods, session helpers wrap the fixed `query_open` / `fetch` / `close` / `cancel` lifecycle, and aio launch `timeout_ms` is distinct from later `ducknng_aio_collect(..., wait_ms)` polling.
- Reframed the README and transport docs so `ws://` and `wss://` are documented as enabled NNG transports, while browser-style or HTTP-carrier WebSocket work remains explicitly deferred.
- Switched the README HTTP client illustration to a visible local `nanonext` HTTPS server so the example shows the real carrier and TLS story rather than hiding it behind generic setup prose.
- Implemented `ducknng_ncurl(...)` as the first low-level HTTP/HTTPS client slice, returning in-band `ok`, `status`, `error`, `headers_json`, `body`, and `body_text` columns.
- Added `make http_smoke` and a local Python-stdlib smoke harness to validate real HTTP GET and POST roundtrips without depending on the public internet.
- Added `docs/http.md` to pin the first HTTP transport contract: scheme-routed server startup through `ducknng_start_server(...)`, `ducknng_ncurl(...)`, frame-over-HTTP carriage, and the invariant that session methods and Arrow record batches keep the same protocol semantics under HTTP. Added `docs/http_server_framework.md` as the route-layer contract beside, not instead of, the framed RPC endpoint.
- Added a transport-family URL parser above the NNG shim so `http://` and `https://` route through the HTTP carrier adapter for synchronous helpers, while generic NNG socket/listener paths reject those schemes instead of treating them as malformed NNG endpoints.
- Added a runtime-owned aio registry and SQL-visible raw aio helpers for both request/reply and generic socket operations: `ducknng_request_raw_aio()`, `ducknng_request_socket_raw_aio()`, `ducknng_send_socket_raw_aio()`, `ducknng_recv_socket_raw_aio()`, `ducknng_aio_ready()`, `ducknng_aio_status()`, `ducknng_aio_collect()`, `ducknng_aio_cancel()`, and `ducknng_aio_drop()`.
- `ducknng_aio_collect()` and `ducknng_aio_status()` are now exposed as SQL macros over internal scalar helpers so dynamic arguments can work without relying on lateral-capable stable-C-API table-function parameters.
- Expanded the generic socket surface to the broader nanonext-style NNG protocol family: `bus`, `pair`, `poly`, `push`, `pull`, `pub`, `sub`, `req`, `rep`, `surveyor`, and `respondent`.
- Added generic raw socket verbs `ducknng_listen_socket()`, `ducknng_send_socket_raw()`, `ducknng_recv_socket_raw()`, `ducknng_subscribe_socket()`, and `ducknng_unsubscribe_socket()`.
- Added SQL-visible session wrappers `ducknng_open_query()`, `ducknng_fetch_query()`, `ducknng_close_query()`, and `ducknng_cancel_query()` over the existing `query_open` / `fetch` / `close` / `cancel` RPC family.
- The docs contract for the session query family now fixes the intended lifecycle: `query_open` returns JSON control metadata and a session id, `fetch` is the only row-bearing method, `close` is the normal cleanup path, and `cancel` is best-effort.

### Earlier 0.1.0 groundwork

- Added the client-side SQL helper family now exposed as `ducknng_get_rpc_manifest(...)`, `ducknng_run_rpc(...)`, `ducknng_query_rpc(...)`, plus raw-frame variants for callers that need explicit frame handling.
- Added in-band result-table request helpers `ducknng_request(...)` and `ducknng_request_socket(...)` so client-side transport and protocol failures can be handled as rows, alongside raw scalar variants when a bare reply frame is specifically needed.
- Added SQL-native req-style client socket helpers now exposed as `ducknng_open_socket(protocol)`, `ducknng_dial_socket(socket_id, url, timeout_ms, tls_config_id)`, `ducknng_request_socket(...)`, `ducknng_close_socket(socket_id)`, and `ducknng_list_sockets()`.
- Added `ducknng_query_rpc(url, sql, tls_config_id)` as the unary row-reply client table function, exposing Arrow IPC row replies as DuckDB tables for the supported unary row subset.
- `exec` request payloads are now Arrow IPC tables containing `sql` and `want_result` fields.
- `manifest` replies are now JSON exported from the runtime method registry.
- `exec` metadata replies are now Arrow IPC generated with vendored nanoarrow C.
- Unary `exec(..., want_result = true)` initially returned Arrow IPC row payloads for the first scalar subset: BOOLEAN, signed/unsigned integers, FLOAT/DOUBLE, VARCHAR, and BLOB.
- Removed unstable and deprecated DuckDB Arrow entrypoints from the implementation and kept the row-result Arrow path on explicit nanoarrow-based schema and batch mapping.
- Removed the dead deprecated Arrow-wrapper compatibility layer so the tree no longer carries unused `duckdb_query_arrow*` scaffolding.
- Added a registry-backed built-in method surface with `manifest` and `exec` descriptors.
- `manifest` now remains the only always-on built-in RPC method; `exec` must be registered explicitly with `ducknng_register_exec_method()`.
- Added SQL-visible method registry administration with `ducknng_register_exec_method()`, `ducknng_set_method_auth(name, requires_auth)`, `ducknng_unregister_method(name)`, `ducknng_unregister_family(family)`, and `ducknng_list_methods()`.
- Added `docs/security.md` and `docs/registry.md` as binding design and implementation contracts.
- Added a project-local Pi skill at `.pi/skills/ducknng-rpc-framework/` for protocol, registry, session, and security work in this repo.
- Added `test/rpc_smoke.R` plus `make rpc_smoke` to validate manifest discovery and Arrow-metadata `exec` replies over real NNG REQ/REP.
- Added a versioned RPC envelope with a method name, flags, error field, and payload length instead of the older ad hoc opcode frame.
- Added initial `function_catalog/functions.yaml` metadata plus generated markdown and TSV catalogs.
- Added initial SQLLogicTest coverage in `test/sql/ducknng_server_start.test`.
- Added extension metadata file `description.yml`.
- Added excluded platform metadata for wasm and Windows targets while native Linux development is underway.
- Added `README.Rmd` and `make rdm` for generated documentation.
- Vendored `nng` and `nanoarrow` as third-party dependencies.
- Added `ducknng_list_servers()` table-function introspection over the per-database runtime service registry.
- `ducknng_start_server(...)` now creates the requested number of REP contexts on one REP socket instead of hard-coding a single worker.
- Added phase-1 SQL control functions that evolved into the current names:
  - `ducknng_start_server(...)`
  - `ducknng_stop_server(name)`
- Added a real NNG REP listener lifecycle with one context, one AIO, and one worker thread.
- Added a phase-1 pure C runtime keyed by DuckDB database handle.
- Renamed the template extension to `ducknng`.

## Planned next steps

- Keep future async additions on the current aio substrate: add lifecycle, wait, collect, and decode conveniences only when they preserve the one-pending-operation handle model.
- Continue Arrow type coverage beyond the current practical core only where the mapping can be documented and tested precisely.
- Keep the current session identity and shared serialized execution-lane contracts explicit, while leaving harder per-session/per-request DuckDB isolation to deployment topologies or future opt-in work.
- Keep tightening lifetime and concurrency behavior around runtime-owned sockets, sessions, and aio handles now that the transport matrix is broader.
