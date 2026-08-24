# ducknng HTTP and HTTPS transport adapter contract

This document defines the first concrete HTTP and HTTPS transport design for `ducknng`. It is binding for the HTTP/HTTPS carrier exposed through `ducknng_start_server(...)`, for the low-level `ducknng_ncurl(...)` helpers, and for the URL-routed synchronous request, RPC, and session helpers that now use the same carrier automatically when the URL scheme is `http` or `https`. The purpose of the document is to keep that surface aligned with the manifest, session, and Arrow contracts instead of letting the project grow a second RPC API that drifts away from the existing method model.

The governing rule is the same one stated in `docs/protocol.md` and `docs/transports.md`: HTTP is a transport adapter, not a second protocol. It changes how the bytes travel. It does not change the registry-backed method set, the query-session lifecycle, or the Arrow-versus-JSON payload contract.

## Scope

The initial HTTP adapter is deliberately narrow. Its primary job is still framed RPC carriage for the existing `ducknng` envelope and method registry. It is not a generic web framework, not a browser asset server, not a WebSocket toolkit, and not an excuse to create path-specific copies of `manifest`, `exec`, `query_open`, `fetch`, `close`, or `cancel`. A separate low-level route layer now exists beside that framed RPC mount, covering exact, prefix, and template patterns while remaining deliberately small and documented separately in `docs/http_server_framework.md`.

The generic socket surface remains NNG-only. `ducknng_open_socket(...)`, `ducknng_listen_socket(...)`, `ducknng_send_socket_raw(...)`, `ducknng_recv_socket_raw(...)`, and the corresponding socket AIO helpers model NNG socket patterns and do not generalize to HTTP. The synchronous low-level socket helpers return a single struct-shaped result with `ok`, `error`, nullable `nng_error`, nullable `nng_error_message`, `socket_id`, `payload`, and `url`, so expected NNG failures can be handled in SQL without tearing down the DuckDB statement. The HTTP family instead gets its own low-level client helper, `ducknng_ncurl(...)`, while `ducknng_start_server(...)` mounts the HTTP/HTTPS carrier when the listen URL uses those schemes and the existing request, RPC, and session helpers route by URL scheme on top of that adapter.

## Current SQL surface

The shipped low-level synchronous client entry point is:

```sql
ducknng_ncurl(url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id])
```

Its asynchronous companion is:

```sql
ducknng_ncurl_aio(url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id])
ducknng_ncurl_aio_collect(aio_ids, wait_ms)
```

They are modeled ergonomically on `nanonext::ncurl()` / `ncurl_aio()` while staying faithful to DuckDB SQL conventions and the project preference for in-band error tables.

The implemented return shape is:

```text
TABLE(
  ok BOOLEAN,
  status INTEGER,
  error VARCHAR,
  headers_json VARCHAR,
  body BLOB,
  body_text VARCHAR
)
```

`ok` means the HTTP transport operation completed and a response was received. It does not mean the response status was 2xx. `status` is the HTTP status code when present. `error` is reserved for local client, connection, timeout, TLS, cancellation, or adapter failures. `headers_json` is the response header block in a canonical JSON form that preserves order and duplicates. `body` is the raw response body. `body_text` is a best-effort UTF-8 decoding of `body` and is `NULL` when the body is absent or not valid text. The raw `ducknng_ncurl(...)` helper is registered through a volatile execution path, so constant arguments are not folded into one cached HTTP response across recursive CTE iterations or repeated row execution. If a retry loop must avoid speculative calls after a stop condition, make the HTTP expression depend on the filtered recursive row, for example by including the attempt number in a query parameter or request body. `ducknng_ncurl_aio_collect(...)` returns the same raw result columns plus `aio_id` for terminal handles launched by `ducknng_ncurl_aio(...)`; expected launch failures such as unsupported schemes or invalid TLS handles are represented as immediate terminal error aio handles and are inspected through the same collect/status path. Those handles are inspected with `ducknng_aio_status(...)` and released with `ducknng_aio_drop(...)` like other aio handles.

The request-side `headers_json` argument uses the same canonical JSON shape for symmetry. The preferred contract is an array of objects such as `[{"name":"Content-Type","value":"application/json"}]` rather than a plain JSON object, because HTTP header names may repeat and order sometimes matters operationally.

Outbound HTTP credential profiles are runtime-local records resolved inside the HTTP client path rather than in caller SQL. They are managed with:

```sql
ducknng_register_http_profile(profile_id, scheme, host, port, path_prefix,
                              method, tls_required,
                              auth_header_name, auth_header_value[, expires_at_ms])
ducknng_drop_http_profile(profile_id)
ducknng_list_http_profiles()
```

When `profile_id` is supplied to `ducknng_ncurl(...)`, `ducknng_ncurl_aio(...)`, or `ducknng_ncurl_table(...)`, ducknng looks up the profile, checks the request scope, and injects the profile's authentication header before the request is sent. Scope checks are fail-closed and cover scheme, exact host, optional exact port, segment-aware path prefix, HTTP method, and whether TLS is required. A prefix such as `/auth` matches `/auth` and `/auth/...` but not `/authz`; use `/` or a trailing slash for deliberately broad scopes. The current implementation deliberately rejects a caller-supplied header that collides with the profile auth header, including `Authorization`, instead of letting caller SQL override the credential. This collision policy applies consistently across the sync, table, and AIO helpers. `ducknng_list_http_profiles()` is redacted: it exposes profile id, scope, auth header names, version, Unix-epoch-millisecond timestamps, and expiry, but never raw credential values. These profiles are ducknng runtime credentials; the vendored DuckDB C API in this repository does not expose a stable Secret Manager registration/lookup path, so the resolver is shaped to allow a future Secret Manager or C++ bridge without pretending that integration exists today.

The raw helper deliberately does not parse response bodies by default. Provider-driven parsing is opt-in through two table helpers:

```sql
ducknng_list_codecs()
ducknng_parse_body(body, content_type)
ducknng_ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id])
```

`ducknng_list_codecs()` lists the built-in body serialization/deserialization providers. `ducknng_parse_body(...)` takes an existing `BLOB` plus a `Content-Type` string and returns provider-specific table output. `ducknng_ncurl_table(...)` performs one HTTP/HTTPS request, requires a 2xx status, reads the response `Content-Type`, and returns the parsed body as a DuckDB table. Because its output schema depends on the response body and `Content-Type`, it is a dynamic-schema table function whose request is resolved during binding; it is not the right primitive for per-row retry loops or lateral chunk fanout. Use raw `ducknng_ncurl(...)` plus an explicit parse step when row-by-row HTTP execution is required. Missing or unknown content types fall back to a raw `body BLOB` column.

The initial built-in providers are content-type driven: raw bytes, UTF-8 text, JSON, Arrow IPC stream, and `application/vnd.ducknng.frame`. CSV, TSV, and Parquet media types are recognized, but they currently use the generic `body BLOB` fallback rather than temporary files. JSON is parsed in memory through DuckDB JSON functions and then reuses the same Arrow IPC row mapping that powers `ducknng_query_rpc(...)`. Arrow IPC stream bytes are decoded with nanoarrow and mapped into DuckDB vectors through the stable manual mapping layer. `application/vnd.ducknng.frame` returns the same envelope columns as `ducknng_decode_frame(...)`.

These parsed helpers are intentionally transport-local conveniences. They do not change the framed RPC method surface and they do not make the HTTP server accept arbitrary JSON, CSV, Parquet, or Arrow bodies at the RPC endpoint.

The shipped server entry point is:

```sql
ducknng_start_server(name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id[, ip_allowlist_json])
```

For `http://` and `https://` listeners, `contexts` must be `1` because the HTTP carrier does not expose the NNG REP-context model. Starting the server is non-blocking from SQL's point of view: it installs an NNG HTTP handler, starts the NNG HTTP server, and returns. Requests are accepted by the NNG HTTP server through its asynchronous handler path. The current handler still collects each request body up to `recv_max_bytes` and dispatches the framed RPC synchronously into the `ducknng` dispatcher; service-owned DuckDB SQL remains serialized through the runtime execution lane. In other words, the listener is non-blocking, but long-running handler work can still occupy server-side execution resources.

`name` is the runtime service name. `listen` is a full HTTP or HTTPS endpoint URL such as `http://127.0.0.1:8080/_ducknng` or `https://127.0.0.1:8443/_ducknng`. For the HTTP adapter, the path component is semantically meaningful: it is the RPC mount path. `recv_max_bytes`, `session_idle_ms`, and `tls_config_id` retain their current meanings, and `ip_allowlist_json` is the optional startup allowlist copied into the service before it starts. `tls_config_id = 0::UBIGINT` means plaintext for `http://`. HTTPS listeners require an explicit TLS handle because the server needs certificate material to terminate TLS correctly. If that TLS handle uses authentication mode `2`, the HTTPS server requires a verified client certificate and passes the derived `tls:san:<value>` or `tls:cn:<common-name>` caller identity into the same dispatcher path used by NNG TLS transports.

The matching stop and introspection path remains generic rather than adding HTTP-specific variants. `ducknng_stop_server(name)` stops a named service regardless of transport family, and `ducknng_list_servers()` reports the currently registered services without minting transport-specific lifecycle names. That keeps the public surface compact.

`ducknng_ncurl(...)` and `ducknng_ncurl_aio(...)` are transport-local and not manifest-derived. They are meant for generic HTTP interactions, adapter debugging, and future interoperability helpers. They are not the only route to `ducknng` RPC over HTTP because the higher-level synchronous helpers already use the same carrier automatically.

## Companion route framework

HTTP and HTTPS services can now also register additional routes beside the framed RPC mount:

```sql
ducknng_register_http_route(service_name, method, path, handler_sql[, request_max_bytes])
ducknng_register_http_route_pattern(service_name, method, match_kind, path_pattern, handler_sql[, request_max_bytes])
ducknng_unregister_http_route(service_name, method, path)
ducknng_unregister_http_route_pattern(service_name, method, match_kind, path_pattern)
ducknng_list_http_routes()
ducknng_list_http_workers()
ducknng_http_request()
ducknng_http_request_body()
ducknng_http_headers_get(headers_json, name)
ducknng_http_headers_build(names, values)
ducknng_http_query_param_get(query_string, name)
ducknng_http_cookie_get(cookie_header, name)
ducknng_http_path_params_get(path_params_json, name)
ducknng_http_header(name)
ducknng_http_query_param(name)
ducknng_http_cookie(name)
ducknng_http_path_param(name)
ducknng_http_response(status, headers_json, content_type, body, body_text)
ducknng_http_text(status, body_text)
ducknng_http_json(status, body_text)
ducknng_http_binary(status, body)
```

These helpers are transport-local service tooling, not manifest-derived RPC methods. A route handler is one SQL query that returns exactly one response row, with optional `status`, `headers_json`, `content_type`, `body`, and `body_text` columns. Request context comes from `ducknng_http_request()` and `ducknng_http_request_body()` while that handler runs. The named header, query, cookie, and path helpers are small accessors over those same context rows, and the response macros build the same one-row route response shape.

When a route needs per-request dynamic backend session control, use the raw synchronous session helpers:

- `ducknng_open_query_raw(...)`
- `ducknng_fetch_query_raw(...)`
- `ducknng_close_query_raw(...)`
- `ducknng_cancel_query_raw(...)`

and inspect the returned frames with:

- `ducknng_frame_version(...)`
- `ducknng_frame_type(...)`
- `ducknng_frame_flags(...)`
- `ducknng_frame_type_name(...)`
- `ducknng_frame_name(...)`
- `ducknng_frame_payload(...)`
- `ducknng_frame_payload_text(...)`
- `ducknng_frame_error_text(...)`
- `ducknng_frame_end_of_stream(...)`

The structured table helpers remain the ergonomic client-facing surface. The raw scalar helpers are the correct route-layer surface when parameters come from request-body columns or continuation-token columns.

The important boundary stays the same:

- the framed RPC mount still lives exactly at the path encoded in the service listen URL
- registered routes must not conflict with that mount path
- routes are application routes beside the frame carrier, not a second RPC namespace
- routes inherit the same admission stack and the configured service execution model as the rest of the service

For the precise route contract, request/response shape, and execution-model caveats, see `docs/http_server_framework.md`.

## Operation-oriented routing by URL scheme

The existing synchronous request, RPC, and session helpers remain operation-oriented and route by URL scheme instead of forcing callers to learn a second RPC client API.

That means URLs like `http://127.0.0.1:8080/_ducknng` and `https://127.0.0.1:8443/_ducknng` are accepted by the existing synchronous helpers:

- `ducknng_request(...)`
- `ducknng_request_raw(...)`
- `ducknng_get_rpc_manifest(...)`
- `ducknng_get_rpc_manifest_raw(...)`
- `ducknng_run_rpc(...)`
- `ducknng_run_rpc_raw(...)`
- `ducknng_open_query_raw(...)`
- `ducknng_fetch_query_raw(...)`
- `ducknng_close_query_raw(...)`
- `ducknng_cancel_query_raw(...)`
- `ducknng_query_rpc(...)`
- `ducknng_open_query(...)`
- `ducknng_fetch_query(...)`
- `ducknng_close_query(...)`
- `ducknng_cancel_query(...)`
- `ducknng_request_raw_aio(...)`
- `ducknng_get_rpc_manifest_raw_aio(...)`
- `ducknng_run_rpc_raw_aio(...)`

In other words, `ducknng_ncurl(...)` is the generic HTTP primitive, while the higher-level `ducknng` RPC and session helpers keep their current names and use the HTTP carrier automatically when the URL scheme is `http` or `https`. Structured helpers report carrier failures as `ok = false` rows. Framed raw helpers report those same local setup or carrier failures as `ducknng` error frames so dynamic SQL can decode the error without forcing a DuckDB exception.

## Initial HTTP carrier contract

The first HTTP adapter should expose one RPC endpoint at the exact path encoded in the `listen` URL. If the server is started on `http://127.0.0.1:8080/_ducknng`, then the RPC endpoint is `POST /_ducknng`. The same rule applies to HTTPS.

The initial binding is frame-over-HTTP. The request body is one complete `ducknng` frame. The response body is one complete `ducknng` frame whenever the adapter successfully reaches the registry-backed dispatcher and obtains a protocol-level reply.

The normative media type for both request and response is:

```text
application/vnd.ducknng.frame
```

The HTTP adapter is therefore a carrier for the existing `ducknng` envelope, not a replacement for it. The HTTP body is not raw SQL text, not ad hoc JSON RPC, and not a path-based method binding. It is the same versioned frame that today travels over NNG.

The initial method contract is intentionally narrow. `POST` is the only accepted method on the RPC endpoint. The adapter should reject other methods with `405 Method Not Allowed`. It should reject an unsupported request `Content-Type` with `415 Unsupported Media Type`. It should reject an oversized request with `413 Payload Too Large`. It should reject a malformed frame with `400 Bad Request`. Path mismatches are `404 Not Found`.

Once the adapter has accepted a request as a valid `ducknng` frame and handed it to the dispatcher, protocol-level success and protocol-level failure both travel back as ordinary `ducknng` frames. In that state, HTTP status code `200 OK` is the correct outer status even when the inner `ducknng` reply frame is an error frame. HTTP 4xx and 5xx responses are reserved for adapter-level failures that occur before a valid `ducknng` reply frame exists.

This distinction is essential. It keeps application errors inside the `ducknng` protocol, where existing clients already know how to decode them, instead of spreading method failure semantics across two unrelated status systems.

## Sessions and Arrow record batches over HTTP

The HTTP adapter does not alter the session contract. It carries the same methods and payloads that already exist over the NNG carrier.

`query_open` still accepts an Arrow IPC payload containing exactly one logical request row with `sql` and optional batch controls. Over HTTP, that Arrow IPC payload remains the payload inside a `ducknng` request frame, and that frame becomes the HTTP request body. Its JSON reply includes the same `session_id`, `session_token`, state metadata, and server-owned effective `idle_timeout_ms` as the NNG carrier.

`fetch` still accepts JSON control metadata keyed by `session_id` and `session_token`. Over HTTP, that JSON control payload remains the payload inside a `ducknng` request frame, and that frame becomes the HTTP request body.

`fetch` still returns either Arrow IPC row data or JSON control metadata. When rows are returned, the Arrow IPC bytes remain inside the `ducknng` reply frame payload exactly as they do over NNG. The HTTP response body is therefore still one `ducknng` frame whose payload contains Arrow IPC record-batch bytes. When only control metadata is returned, the HTTP response body is one `ducknng` frame whose payload contains JSON.

`close` and `cancel` retain the same JSON control request and response shapes. They do not become path-specialized HTTP endpoints and they do not gain alternate payload encodings just because the outer carrier is HTTP.

This means Arrow record batches remain Arrow record batches. They do not become JSON arrays, text tables, or bespoke HTTP chunking formats in the first HTTP adapter. Session ids and session tokens remain frame payload fields. They do not migrate into path segments, query parameters, or HTTP cookies. The same state machine in `docs/protocol.md` continues to govern `query_open`, `fetch`, `close`, and `cancel`.

## TLS and security

The HTTP adapter inherits the same trust model and ownership rules described in `docs/security.md`. It does not introduce a second authentication model. HTTPS uses the same TLS handle model already established for the NNG transport direction, and session ownership remains a protocol-level concern rather than a carrier-local shortcut.

A successful HTTPS deployment therefore still needs deliberate TLS configuration. Session ownership is proven with the same `session_token` bearer capability used over NNG, and HTTPS protects that token from network observers. When HTTPS mTLS is enabled with authentication mode `2`, the adapter also requires a verified peer certificate, derives the caller identity from the first available SAN or, if no SAN is available, the certificate common name, and binds query sessions opened by that caller to the same identity in addition to the bearer token. If a service peer allowlist is active, the HTTPS adapter checks that verified identity before RPC dispatch and returns HTTP `403` for non-admitted peers. If a service IP/CIDR allowlist is active, the HTTP/HTTPS adapter checks the connection remote address before RPC dispatch and returns HTTP `403` for non-admitted addresses. If a service SQL authorizer is installed, the HTTP/HTTPS adapter evaluates the same `ducknng_auth_context()` callback used by NNG framed RPC before method dispatch; callback denials return HTTP `403` or the callback-provided status. This keeps HTTPS as a carrier for the frame protocol while still allowing transport-level authorization.

## Deferred items

The following are explicitly not part of the first HTTP adapter contract.

Human-friendly convenience routes such as `GET /manifest` are deferred. They may later be added as transport conveniences that internally map onto the same registry-backed methods, but they are not part of the first binding.

HTTP-carrier WebSocket, SSE, NDJSON, browser asset serving, and mixed HTTP-plus-static routing are deferred. They belong to broader web-toolkit work and should not be smuggled into the first RPC carrier implementation. This does not conflict with the separate NNG `ws://` and `wss://` transport schemes, which remain part of the NNG adapter rather than this HTTP carrier.

The low-level route layer is now part of the public SQL surface, including additive exact, prefix, and template matching plus small request-accessor and response-builder helpers. Richer web-toolkit features are still deferred. Static asset serving, route-local authentication policy, HTTP-carrier WebSocket/SSE/NDJSON work, worker lifecycle management, and application gateway products must remain additive layers beside the frame carrier. They must not mint method copies such as `http_exec`, `http_query_open`, or `http_fetch`, and they must not change the frame endpoint's `POST application/vnd.ducknng.frame` contract.
