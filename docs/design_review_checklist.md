# ducknng design review checklist

This checklist tracks the implementation status of the main architecture, transport, Arrow, session, and async hardening items for `ducknng`. It separates work that is already landed, work completed in this running line of development, and items blocked by deeper architectural replacement work.

## Completed before this pass

- [x] Make `exec` opt-in instead of always-on.
  - `manifest` remains always registered.
  - `exec` must be enabled explicitly with `ducknng_register_exec_method()`, or `ducknng_register_exec_method(true)` when the `exec` descriptor should require verified transport-derived peer identity.
- [x] Add SQL-visible method registry administration.
  - `ducknng_register_exec_method()`
  - `ducknng_unregister_method(name)`
  - `ducknng_unregister_family(family)`
  - `ducknng_list_methods()`
- [x] Make service teardown ownership explicit.
  - `ducknng_stop_server(name)` already removes the service from the runtime table and destroys it.
- [x] Keep server runtime introspection real.
  - `ducknng_list_servers()` reflects the live runtime registry rather than a fake catalog.
- [x] Add reusable TLS config handles.
  - `ducknng_tls_config_from_files()`
  - `ducknng_tls_config_from_pem()`
  - `ducknng_self_signed_tls_config()`
  - `ducknng_drop_tls_config()`
  - `ducknng_list_tls_configs()`

## Completed in this pass

- [x] Collapse `ducknng_start_server` to one public signature.
  - New public signature: `ducknng_start_server(name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id)`.
  - Plaintext now uses `tls_config_id = 0`.
  - Inline file-argument TLS startup was removed from the public SQL surface.
- [x] Collapse `ducknng_request_raw` to one public signature.
  - New public signature: `ducknng_request_raw(url, payload, timeout_ms, tls_config_id)`.
  - Plaintext now uses `tls_config_id = 0::UBIGINT`.
- [x] Give structured one-shot RPC helpers the same TLS reach as their raw counterparts.
  - `ducknng_request(url, payload, timeout_ms, tls_config_id)`
  - `ducknng_get_rpc_manifest(url, tls_config_id)`
  - `ducknng_run_rpc(url, sql, tls_config_id)`
  - `ducknng_query_rpc(url, sql, tls_config_id)`
  - raw companions now also take `tls_config_id` consistently.
- [x] Unify allocator discipline for resolved listen URLs.
  - `ducknng_listener_resolve_url()` now allocates with `duckdb_malloc()`.
  - service teardown now frees resolved URLs with `duckdb_free()`.
- [x] Make `ducknng_open_socket()` honest by implementing the broader NNG protocol family.
  - `bus`, `pair`, `pair0`, `pair1`, `poly`, `push`, `pull`, `pub`, `sub`, `req`, `rep`, `surveyor`, and `respondent` now open through the public socket surface.
- [x] Add generic raw socket verbs on top of that broader protocol family.
  - `ducknng_listen_socket()`
  - `ducknng_send_socket_raw()`
  - `ducknng_recv_socket_raw()`
  - `ducknng_subscribe_socket()`
  - `ducknng_unsubscribe_socket()`
- [x] Add SQL-visible aio handles for both request/reply and generic socket operations.
  - `ducknng_request_raw_aio()`
  - `ducknng_request_socket_raw_aio()`
  - `ducknng_send_socket_raw_aio()`
  - `ducknng_recv_socket_raw_aio()`
  - `ducknng_aio_ready()`
  - `ducknng_aio_wait()`
  - `ducknng_aio_status()`
  - `ducknng_aio_collect()`
  - `ducknng_aio_cancel()`
  - `ducknng_aio_drop()`
- [x] Add real SQLLogicTest coverage for the widened socket and aio surface.
  - `test/sql/ducknng_socket_protocols.test`
- [x] Make the docs/examples/catalog match the widened socket and aio surface.
  - `README.Rmd`
  - generated `README.md` / `README.html`
  - `function_catalog/functions.yaml`
  - generated `function_catalog/functions.md` / `functions.tsv`
- [x] Keep the current implementation off unstable and deprecated DuckDB Arrow entrypoints.
  - Unstable and deprecated DuckDB Arrow entrypoints were removed from implementation code.
  - The row-bearing RPC export path remains on explicit nanoarrow schema and batch mapping.
- [x] Add a transport-family boundary for HTTP / `ncurl` adapter work.
  - URL-family parsing now lives in `src/ducknng_transport.c`.
  - NNG-specific socket/listener/TLS behavior remains isolated in `src/ducknng_nng_compat.c`.
  - HTTP-specific client/server behavior remains isolated in `src/ducknng_http_compat.c`.
  - `http://` and `https://` route through the HTTP adapter for synchronous request/RPC/session helpers, while generic NNG socket/listener paths reject those schemes instead of treating them as malformed NNG endpoints.
- [x] Add HTTP/HTTPS aio client helpers without changing the framed RPC carrier contract.
  - `ducknng_ncurl_aio(...)` launches one unary asynchronous HTTP/HTTPS request.
  - `ducknng_ncurl_aio_collect(...)` returns complete HTTP-shaped terminal results instead of raw frame rows.
  - The ncurl stream open/receive/close family exposes response heads before body completion and cancellable de-framed body reads without changing the unary contract.
- [x] Land the low-level HTTP route framework beside the framed RPC mount.
  - `ducknng_register_http_route(...)`, `ducknng_unregister_http_route(...)`, and `ducknng_list_http_routes()` expose service-local route registration and introspection for `http://` and `https://` services.
  - `ducknng_http_request()` and `ducknng_http_request_body()` expose route-local request context and body bytes while handler SQL runs.
  - Route handlers return one bounded response row and share the same admission stack and configured service execution model as framed RPC.
- [x] Surface NNG pipe events and live pipe snapshots.
  - `ducknng_read_monitor(...)` exposes bounded pipe monitor events with sequence cursors and denial reasons.
  - `ducknng_monitor_status(...)` exposes monitor ring counters and drop counts.
  - `ducknng_list_pipes(...)` exposes active pipe snapshots.
- [x] Split SQL registration into focused modules.
  - `src/ducknng_sql_api.c` is now the small top-level registration orchestrator.
  - Focused modules cover service, auth, monitor, TLS, socket, AIO, registry, session, body/codec, and RPC/client bindings.
- [x] Add lifecycle and URL-policy regression coverage.
  - Tests now cover stopping services with live sessions, rejecting same-service stop while a service-owned authorizer request is active, unsupported URL schemes across major helpers, and TLS configuration on non-TLS URL schemes.

## Partial / clarified but not fully solved

- [x] Keep structured-vs-raw helper duplication under explicit review.
  - Resolved: signatures and transport reach are unified, and the raw/structured twins are kept intentionally as the current 0.1.0 surface. The raw helpers feed `ducknng_decode_frame()` / aio collection paths and the structured helpers wrap them as ergonomic table forms; deleting either side would force every caller through the other style without removing real complexity.
- [x] Prepare HTTP / HTTPS transport adapters without inventing a second RPC surface.
  - Current state: `docs/transports.md` and `docs/http.md` now fix the intended boundary, `ducknng_start_server(...)` covers `http://` and `https://` listeners with `contexts = 1`, `ducknng_ncurl(...)`, unary `ducknng_ncurl_aio(...)`, and the incremental ncurl stream family provide the low-level HTTP/HTTPS client slices, synchronous request/RPC/session helpers route by URL scheme, and the low-level route layer now covers exact, prefix, and template matching while staying explicitly beside rather than inside the manifest-derived RPC surface.

## Blocked by larger architectural replacement work

- [~] Replace the hand-rolled Arrow encode/decode path with DuckDB-native Arrow C API plumbing.
  - Current state: the row-bearing RPC export path stays on manual nanoarrow schema and batch mapping.
  - Remaining work:
    - decide whether a future non-deprecated DuckDB Arrow seam exists that would justify another re-plumb
    - until then, keep the manual mappings careful, documented, and fully tested instead of mixing in unstable or deprecated DuckDB entrypoints
- [~] Harden the session query family now that SQL helpers exist.
  - Current state:
    - server-side `query_open` / `fetch` / `close` / `cancel` methods are registered
    - SQL-visible wrappers now exist as `ducknng_open_query()`, `ducknng_fetch_query()`, `ducknng_close_query()`, and `ducknng_cancel_query()`
    - `ducknng_fetch_query_table()` now exposes one fetched Arrow IPC batch directly as a DuckDB table
    - `ducknng_query_rpc()` now runs on top of the session family and closes the session automatically after end-of-stream
    - raw synchronous session helpers now exist as `ducknng_open_query_raw()`, `ducknng_fetch_query_raw()`, `ducknng_close_query_raw()`, and `ducknng_cancel_query_raw()`
    - frame-field scalar accessors now exist for envelope metadata, flags, payload, text payloads, protocol error text, and end-of-stream state, which makes per-request route SQL practical without bind-time table decoding
    - raw async session launchers now exist as `ducknng_open_query_raw_aio()`, `ducknng_fetch_query_raw_aio()`, `ducknng_close_query_raw_aio()`, and `ducknng_cancel_query_raw_aio()`
    - query sessions now carry a generated `session_token` bearer capability; `fetch`, `close`, and `cancel` reject token mismatches instead of accepting bare `session_id`
    - mTLS-authenticated transports now attach verified peer identity to requests and bind sessions to that identity when present
  - Remaining work:
    - keep envelope-level RPC authentication as an optional future additive layer rather than a blocker for the current session contract
    - document the current single serialized DuckDB execution lane as an intentional deployment mode, and add isolated per-session or per-request DuckDB execution resources only for deployments that need hard state isolation
    - decide later whether any higher-level structured async session wrappers are worth adding on top of the raw-frame aio contract
- [~] Add the codec framework for body and Arrow extension serde.
  - Current state:
    - `ducknng_list_codecs()` lists built-in content-type driven providers and registered user hooks side by side, with a `kind` column and a `function_name` column for user codecs
    - `ducknng_parse_body(body, content_type)` parses raw bodies through built-in providers and user hooks
    - `ducknng_ncurl_table(...)` performs HTTP/HTTPS and parses successful response bodies by `Content-Type`
    - JSON uses DuckDB JSON functions in memory; Arrow IPC still uses nanoarrow plus the stable manual mapping layer
    - CSV, TSV, and Parquet are recognized but use the generic `body BLOB` fallback until a scalarfs-style memory filesystem/provider path is available
    - `ducknng_register_codec(content_type, function_name)` and `ducknng_unregister_codec(content_type)` install user-defined `BLOB → VARCHAR` codecs that take precedence over built-ins; identifiers are gated by a plain-identifier allowlist and dispatched through a fixed `SELECT <fn>(?::BLOB) AS value` shape, so the sealed contract is single-row, single-column `value VARCHAR`
  - Remaining work:
    - research a scalarfs-style in-memory filesystem/provider path for CSV/TSV/Parquet body parsing, including whether community-extension designs such as `duckdb_scalarfs` can be copied or adapted without pulling core `ducknng` onto unstable or C++ DuckDB APIs
    - keep the generic `body BLOB` fallback as the safe default whenever a media type is recognized but no parsing provider is enabled
    - keep user-defined Arrow extension serde blocked until ownership, security, and the future DuckDB Arrow seam are clear enough not to freeze an unsafe callback ABI

## Validation for this checklist pass

- [x] `make release`
- [x] `make test_release`

## Current blockers to report upstream

1. **Current DuckDB-facing Arrow work stays on manual nanoarrow mappings.** The implementation no longer compiles unstable or deprecated DuckDB Arrow entrypoints, so any future Arrow re-plumb must wait for a non-deprecated seam or be abandoned in favor of maintaining the explicit mappings.
2. **Session-family ownership is sealed around token plus optional mTLS identity.** Query sessions now have an explicit `session_token` bearer capability and optional mTLS owner-identity binding, so the bare-`session_id` ownership hole is closed. The higher-level row ergonomics are also in place: `ducknng_fetch_query_table(...)` gives the one-fetch table path, `ducknng_query_rpc()` now sits on the session family, raw synchronous session helpers plus frame-field scalar accessors cover dynamic route SQL, raw session aio launchers collect through the same raw-frame async contract as the rest of RPC, and `ducknng_aio_collect_decoded(...)` now provides the first additive structured collect wrapper over that same substrate. Remaining work is optional isolation and any further structured async wrappers.
3. **HTTP / HTTPS transport adapters are landed, including the raw async client slice and the low-level route layer.** The transport-family boundary is explicit in docs and code, `ducknng_start_server(...)` covers HTTP and HTTPS listeners, synchronous request/RPC/session helpers and raw RPC AIO helpers route over HTTP and HTTPS, `ducknng_ncurl_aio(...)` / `ducknng_ncurl_aio_collect(...)` cover unary raw asynchronous HTTP and the ncurl stream family covers incremental response bodies, and the route helpers now cover exact, prefix, and template matching as an explicitly separate surface beside the framed RPC carrier. Remaining HTTP work is about richer web-toolkit features, not about inventing a second RPC namespace.
4. **Codec work should not be built on undocumented mapping behavior.** If the project continues using the current manual nanoarrow route, codec decisions should sit on top of explicit tested mappings rather than implicit assumptions about a future Arrow helper path.
5. **Generic client socket TLS dialing and the supported transport matrix are documented.** Listener-side TLS, one-shot req/rep TLS, and socket-handle dialing share the same TLS-config handle model, including `wss://`; non-TLS URL schemes reject supplied TLS configuration; and `docs/transports.md` plus the README summarize which surfaces accept NNG schemes, HTTP schemes, and TLS handles.
