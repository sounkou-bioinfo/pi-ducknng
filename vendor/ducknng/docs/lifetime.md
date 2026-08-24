# Lifetime and manual resource management

`ducknng` is a low-level DuckDB binding to NNG plus an Arrow IPC-based RPC framework. That means lifetime rules matter, both inside the extension and at the SQL surface.

This document separates the two.

## 1. What DuckDB does give the extension

DuckDB's C extension API does provide destroy callbacks for extension-owned state that is tied to function registration or statement execution.

Examples include:

- scalar-function extra info via `duckdb_scalar_function_set_extra_info(..., destroy)`
- scalar-function bind data via `duckdb_scalar_function_set_bind_data(..., destroy)`
- table-function extra info via `duckdb_table_function_set_extra_info(..., destroy)`
- table-function bind data via `duckdb_bind_set_bind_data(..., destroy)`
- table-function init data via `duckdb_init_set_init_data(..., destroy)`
- aggregate destructors and similar callback-driven internal cleanup points

That is enough for extension-internal lifetime management of:

- registered function context objects
- bind-time objects
- init-time objects
- statement-local execution state

`ducknng` already uses those callback hooks for internal cleanup.

## 2. What DuckDB does not give us at the SQL-handle level

DuckDB SQL does **not** give `ducknng` a general-purpose GC/finalizer model for user-visible handles like R's external pointer finalizers.

In particular, there is no general SQL-level hook that says:

- this returned `UBIGINT` handle is no longer referenced anywhere
- this handle should be finalized automatically at end of user scope
- this object should be dropped when a variable or temp table cell becomes unreachable

So while DuckDB helps with extension-internal callback-owned state, it does **not** give `ducknng` a `nanonext`-style automatic finalizer story for long-lived SQL-visible runtime handles.

That is why `ducknng` has explicit cleanup functions.

## 3. Manual cleanup the user should do

If you create long-lived runtime objects from SQL, you should clean them up explicitly.

### Servers

If you start a server with `ducknng_start_server(...)`, stop it with:

- `ducknng_stop_server(name)`

Do not rely on database shutdown as the normal cleanup path for long-lived sessions or tests.

### Sockets

If you open a socket with `ducknng_open_socket(...)`, close it with:

- `(ducknng_close_socket(socket_id)).ok`

`ducknng_open_socket(...)` returns the handle as the `socket_id` field of its result struct. This applies to `req`, `rep`, `pair`, `poly`, `push`, `pull`, `pub`, `sub`, `surveyor`, `respondent`, and the other exposed generic protocols. Socket close failures are in-band result rows; they do not normally throw a DuckDB exception.

### AIO handles

If you launch an aio with:

- `ducknng_request_raw_aio(...)`
- `ducknng_request_socket_raw_aio(...)`
- `ducknng_send_socket_raw_aio(...)`
- `ducknng_recv_socket_raw_aio(...)`
- `ducknng_get_rpc_manifest_raw_aio(...)`
- `ducknng_run_rpc_raw_aio(...)`
- `ducknng_open_query_raw_aio(...)`
- `ducknng_fetch_query_raw_aio(...)`
- `ducknng_close_query_raw_aio(...)`
- `ducknng_cancel_query_raw_aio(...)`
- `ducknng_ncurl_aio(...)`
- `ducknng_ncurl_stream_open_aio(...)`
- `ducknng_ncurl_stream_recv_aio(...)`

then you should eventually release the aio handle explicitly with:

- `ducknng_aio_drop(aio_id)`

Important:

- `ducknng_aio_cancel(aio_id)` requests cancellation, but it is **not** the destructor
- the aio collect helpers, including the unary and streaming ncurl collectors, collect terminal results, but they do **not** auto-free the aio slot
- dropping the aio handle is still the explicit cleanup step

### HTTP response streams

A successful `ducknng_ncurl_stream_open_aio_collect(...)` call returns a separate `stream_id`. Close it with:

- `ducknng_ncurl_stream_close(stream_id)`

The open aio and stream are different resources. Before successful open collection, the stream is unclaimed and dropping the open aio closes it. Collection claims the stream, after which dropping the aio does not close the stream. Every claimed stream needs explicit close, including after `end_of_stream = true`. Closing aborts a pending body receive; the receive aio still needs collection or waiting followed by `ducknng_aio_drop(...)`.

### TLS config handles

If you create TLS config handles with:

- `ducknng_tls_config_from_files(...)`
- `ducknng_tls_config_from_pem(...)`
- `ducknng_self_signed_tls_config(...)`

then drop them when done with:

- `ducknng_drop_tls_config(tls_config_id)`

Note that `tls_config_id = 0` is the public sentinel for plaintext/no TLS config. It is not an allocated handle and does not need dropping.

### Query sessions

If you open a query session with:

- `ducknng_open_query(...)`

keep both the returned `session_id` and `session_token`. The `idle_timeout_ms` column is the server-owned effective idle cleanup policy for that session; it is not a client-authoritative lease extension. Close the session explicitly with:

- `ducknng_close_query(...)`

If the session was opened over verified mTLS, the server also records the transport-derived peer identity and requires later `fetch`, `close`, and `cancel` calls to arrive with the same verified identity.

If you need to stop work early, use:

- `ducknng_cancel_query(...)`

but treat `cancel` as best-effort control, not as a universal destructor guarantee. The normal session cleanup path is still explicit close unless the server clearly reports otherwise.

### Execution connection pool

Sessionful query methods and `request_connection` execution scopes use a runtime-owned pool of DuckDB connections. The pool opens a warm minimum eagerly, grows on demand up to a soft ceiling, and waits only a bounded time at the ceiling before returning a visible "pool exhausted" error. The ceiling is controlled through SQL:

- `ducknng_set_execution_pool_max(n)` sets the ceiling, clamped to the compiled range, and returns the effective value.
- `ducknng_execution_pool_max()` returns the current ceiling.

Raising the ceiling lets more concurrent query sessions or nested `query_rpc` table functions hold connections at once. Lowering it caps future growth without dropping connections already in use. A released pooled connection is reset before reuse by dropping connection-local temporary tables and views. This prevents the common temp-object leakage between sessions while keeping the connection handle stable; temp macros and session settings are not reset by this cleanup pass.

## 4. What runtime teardown still does for you

`ducknng` runtime teardown is still important, and recent hardening work improved it substantially.

When the runtime is destroyed, it now cleans up its owned structures rather than leaving destruction partial. That includes runtime-owned registries and transport/service state.

But that is fallback/runtime teardown behavior. It is **not** a substitute for explicit user-level cleanup in a long-lived database process, test suite, notebook, or interactive session.

## 5. Practical rule of thumb

Treat `ducknng` like a low-level systems binding:

- if you **start** a server, you should **stop** it
- if you **open** a socket, you should **close** it
- if you **launch** an aio, you should **drop** it
- if you **open** an HTTP response stream, you should **close** it
- if you **create** a TLS config, you should **drop** it
- if you **open** a query session, you should **close** it

That explicit lifecycle is the honest current API contract.

## 6. Relation to API hardening

The recent lifetime pass improved internal destruction and ownership correctness, and the session ownership model is now explicit: `session_id` is a lookup key, `session_token` is the bearer capability, and verified mTLS identity is an additional owner constraint when present. Envelope-level RPC authentication may be added later as an application-auth layer, but it is not a prerequisite for the current session lifetime contract.

The main remaining lifetime-adjacent sealing blockers are therefore narrower:

- any remaining borrowed-pointer or concurrent-handle edge cases that should be eliminated before calling the API sealed
- clear documentation of which resources are user-managed and which are statement-local/internal

That is why this document exists and why the README now links to it directly.
