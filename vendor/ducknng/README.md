
<!-- README.md is generated from README.Rmd using [duckknit](https://github.com/rundel/duckknit). Please edit that file -->

# ducknng

`ducknng` is a pure C DuckDB extension providing a binding to the
[NNG](https://nng.nanomsg.org/) scalability protocols. It is inspired by
[`nanonext`](https://github.com/r-lib/nanonext) and follows its
ergonomic model: transport is chosen by URL scheme, aio handles wrap
pending NNG operations as SQL-visible futures, and TLS material may come
from filesystem paths or in-memory PEM. On top of the raw socket and
transport layer, `ducknng` adds a framed RPC envelope, Arrow IPC tabular
payloads, manifest-declared methods, and an HTTP/HTTPS carrier so DuckDB
sessions can reach one another over `inproc://`, `ipc://`, `tcp://`,
`tls+tcp://`, `ws://`, `wss://`, `http://`, or `https://`. Long-lived
handles — servers, sockets, AIO futures, TLS configs, query sessions —
are manually managed at the SQL surface.

It also draws on [`mangoro`](https://github.com/sounkou-bioinfo/mangoro)
for the thin versioned RPC envelope design. The optional
`ducknng_quack_batch` row serializer is informed by DuckDB’s
experimental [`quack`](https://github.com/duckdb/duckdb-quack) extension
and related Quack client work such as
[`@quack-protocol/sdk`](https://github.com/tobilg/quack-protocol) and
[`adbc-driver-quack`](https://github.com/gizmodata/adbc-driver-quack).
In ducknng today, that Quack-derived encoding is a row-payload mode
inside the ducknng RPC/session protocol; it is not yet a full Quack
`POST /quack` / `application/vnd.duckdb` server or client.

## Layer map

| Layer          | What it does                                                                                                                                                                                                                                                               |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Transport**  | NNG sockets and listeners. Synchronous send/recv. One-shot AIO handles. Pipe-event telemetry.                                                                                                                                                                              |
| **Framed RPC** | Versioned envelope carrying Arrow IPC payloads or JSON control text. `manifest` is always on; `exec` is opt-in. `http://`/`https://` mount the same methods at the URL path.                                                                                               |
| **Policy**     | Fast C admission: mTLS, exact peer-identity allowlists, IP/CIDR allowlists, per-service and per-principal resource limits. Optional SQL authorizer at the request boundary.                                                                                                |
| **Codecs**     | `ducknng_parse_body(...)` and `ducknng_ncurl_table(...)` parse content-type-tagged BLOBs. Built-in providers cover JSON, Arrow IPC, ducknng frames, the ducknng `ducknng_quack_batch` batch media type, and text/raw fallback. User-registered codec hooks extend the set. |

Raw protocol clients now also have a built-in `handshake` control method
for capability negotiation. It advertises
`supported_serialization_modes` plus `fetch_metadata` and
`session_schema` capability objects covering control-plane
`correlation_id` echo, stable query-session `result_handle` values,
terminal `batch_index` metadata, negotiated protocol/schema versions,
and effective fetch chunk counts. Today the advertised row payload modes
are `arrow_ipc_stream` and `ducknng_quack_batch`: Arrow IPC remains the
broad default, while `ducknng_quack_batch` is the Quack-derived DuckDB
BinarySerializer batch path used for the new comparative benchmarks. The
runnable wire-level example lives in `test/rpc_smoke.R`, and the
contract is pinned in `docs/protocol.md`.

## Quick tour

Build the extension first (see **Development** below), then load it:

Start a TLS server. Port 0 lets the OS assign a free port; a self-signed
certificate is generated in memory — no files needed.

``` sql
-- Self-signed dev cert: key and certificate live only in DuckDB memory.
SET VARIABLE tour_tls = ducknng_self_signed_tls_config('127.0.0.1', 365, 0);
SELECT ducknng_start_server(
  'tour',                              -- service name
  'tls+tcp://127.0.0.1:0',             -- TLS; port 0 = OS-assigned
  1,                                   -- REP contexts
  134217728,                           -- recv_max_bytes (128 MiB)
  300000,                              -- session_idle_ms
  getvariable('tour_tls')::UBIGINT     -- TLS config handle
);
SET VARIABLE tour_url = (
  SELECT listen FROM ducknng_list_servers() WHERE name = 'tour'
);
SELECT getvariable('tour_url') AS listen_url;
-- Open a REQ socket and dial immediately.
SET VARIABLE req_id = (ducknng_open_socket('req')).socket_id;
SELECT (ducknng_dial_socket(
  getvariable('req_id')::UBIGINT,
  getvariable('tour_url'),
  1000, getvariable('tour_tls')::UBIGINT
)).ok AS dialed;

+-------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('tour', 'tls+tcp://127.0.0.1:0', 1, 134217728, 300000, CAST(getvariable('tour_tls') AS "UBIGINT")) |
+-------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                    |
+-------------------------------------------------------------------------------------------------------------------------+

+---------------------------+
|        listen_url         |
+---------------------------+
| tls+tcp://127.0.0.1:40785 |
+---------------------------+

+--------+
| dialed |
+--------+
| true   |
+--------+
```

**Raw socket send/receive.** Send the manifest request frame and decode
the reply envelope.

``` sql
-- Send the 22-byte manifest call frame and decode the reply.
SELECT ok, type_name, name,
       json_array_length(json_extract(payload_text::JSON, '$.methods')) AS method_count
FROM ducknng_decode_frame(
  ducknng_request_socket_raw(
    getvariable('req_id')::UBIGINT,
    from_hex('01000000000000000000000000000000000000000000'),
    1000
  )
);
+------+-----------+----------+--------------+
|  ok  | type_name |   name   | method_count |
+------+-----------+----------+--------------+
| true | result    | manifest | 7            |
+------+-----------+----------+--------------+
```

**One-shot URL form** — same call without holding a socket handle.

``` sql
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_request_raw(
    getvariable('tour_url'),
    from_hex('01000000000000000000000000000000000000000000'),
    1000, getvariable('tour_tls')::UBIGINT
  )
);
+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
```

**AIO: launch two requests in parallel, collect both later.**
`ducknng_request_raw_aio(...)` returns immediately with an integer AIO
handle. `ducknng_aio_collect(...)` blocks until all handles are ready.

``` sql
CREATE TEMP TABLE tour_aios AS
SELECT
  ducknng_request_raw_aio(
    getvariable('tour_url'),
    from_hex('01000000000000000000000000000000000000000000'),
    1000, getvariable('tour_tls')::UBIGINT
  ) AS aio1,
  ducknng_request_raw_aio(
    getvariable('tour_url'),
    from_hex('01000000000000000000000000000000000000000000'),
    1000, getvariable('tour_tls')::UBIGINT
  ) AS aio2;
-- aio_collect blocks until both frames are ready then returns one row per handle.
SELECT aio_id, ok, octet_length(frame) > 0 AS has_frame
FROM ducknng_aio_collect((SELECT list_value(aio1, aio2) FROM tour_aios), 1000)
ORDER BY aio_id;

+--------+------+-----------+
| aio_id |  ok  | has_frame |
+--------+------+-----------+
| 1      | true | true      |
| 2      | true | true      |
+--------+------+-----------+
```

**`ducknng_ncurl` — HTTP client primitive.** The framed RPC mount
accepts POST only. Mount a server on an `http://` URL and POST the
manifest call frame to demonstrate that the framed RPC surface is
identical over HTTP.

``` sql
SELECT ducknng_start_server(
  'tour_http', 'http://127.0.0.1:18440/_ducknng', 1, 134217728, 300000, 0::UBIGINT
);
-- The RPC mount accepts POST. POST the 22-byte manifest frame and decode the reply.
SET VARIABLE tour_http_reply = (
  SELECT body FROM ducknng_ncurl(
    'http://127.0.0.1:18440/_ducknng',
    'POST', '[{"name":"Content-Type","value":"application/vnd.ducknng.frame"}]',
    from_hex('01000000000000000000000000000000000000000000'),
    2000, 0::UBIGINT
  )
);
SELECT ok, type_name, name,
       json_array_length(json_extract(payload_text::JSON, '$.methods')) AS method_count
FROM ducknng_decode_frame(getvariable('tour_http_reply')::BLOB);
+------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('tour_http', 'http://127.0.0.1:18440/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+------------------------------------------------------------------------------------------------------------------+
| true                                                                                                             |
+------------------------------------------------------------------------------------------------------------------+

+------+-----------+----------+--------------+
|  ok  | type_name |   name   | method_count |
+------+-----------+----------+--------------+
| true | result    | manifest | 7            |
+------+-----------+----------+--------------+
```

**Outbound HTTP profile.** Caller SQL passes a profile id and ordinary
request fields. `ducknng` checks the profile scope, injects the auth
header inside the HTTP client path, and keeps profile introspection
redacted.

``` sql
SELECT CASE WHEN ducknng_register_http_route(
  'tour_http', 'GET', '/profile-auth',
  'SELECT * FROM ducknng_http_text(
     200,
     CASE
       WHEN ducknng_http_header(''Authorization'') = ''Bearer readme-secret''
       THEN ''authorized''
       ELSE ''denied''
     END
   )'
) THEN 1 ELSE 0 END AS route_registered;
SELECT CASE WHEN ducknng_register_http_profile(
  'tour-profile', 'http', '127.0.0.1', 18440,
  '/profile-auth', 'GET', false,
  'Authorization', 'Bearer readme-secret'
) THEN 1 ELSE 0 END AS profile_registered;
SELECT ok, status, body_text
FROM ducknng_ncurl(
  'http://127.0.0.1:18440/profile-auth',
  'GET', NULL, NULL, 2000, 0::UBIGINT,
  'tour-profile'
);
SELECT profile_id, path_prefix, method, auth_header_names_json,
       position('readme-secret' IN auth_header_names_json) = 0 AS redacted
FROM ducknng_list_http_profiles()
WHERE profile_id = 'tour-profile';
SELECT CASE WHEN ducknng_drop_http_profile('tour-profile')
  THEN 1 ELSE 0 END AS profile_dropped;
+------------------+
| route_registered |
+------------------+
| 1                |
+------------------+
+--------------------+
| profile_registered |
+--------------------+
| 1                  |
+--------------------+
+------+--------+------------+
|  ok  | status | body_text  |
+------+--------+------------+
| true | 200    | authorized |
+------+--------+------------+
+--------------+---------------+--------+------------------------+----------+
|  profile_id  |  path_prefix  | method | auth_header_names_json | redacted |
+--------------+---------------+--------+------------------------+----------+
| tour-profile | /profile-auth | GET    | ["Authorization"]      | true     |
+--------------+---------------+--------+------------------------+----------+
+-----------------+
| profile_dropped |
+-----------------+
| 1               |
+-----------------+
```

**Query session: open, fetch Arrow IPC, parse the batch.**
`ducknng_open_query` returns a `session_id` + `session_token`.
`ducknng_fetch_query_table` decodes the first Arrow batch directly as
rows. The raw fetch frame carries the same Arrow bytes as a BLOB for
callers that need to forward or inspect the payload.

``` sql
CREATE TEMP TABLE tour_session AS
SELECT *
FROM ducknng_open_query(
  getvariable('tour_url'),
  'SELECT i, i * i AS sq FROM range(1, 6) AS t(i)',
  0::UBIGINT, 0::UBIGINT, getvariable('tour_tls')::UBIGINT
);
SET VARIABLE tour_sid   = (SELECT session_id    FROM tour_session);
SET VARIABLE tour_token = (SELECT session_token FROM tour_session);
-- Decoded rows directly:
SELECT *
FROM ducknng_fetch_query_table(
  getvariable('tour_url'),
  getvariable('tour_sid')::UBIGINT,
  getvariable('tour_token')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, getvariable('tour_tls')::UBIGINT
);



+---+----+
| i | sq |
+---+----+
| 1 | 1  |
| 2 | 4  |
| 3 | 9  |
| 4 | 16 |
| 5 | 25 |
+---+----+
```

Raw fetch gives the Arrow IPC BLOB. `ducknng_parse_body` decodes it as a
table function.

``` sql
-- Re-open a fresh session to show the raw path.
CREATE TEMP TABLE tour_session2 AS
SELECT *
FROM ducknng_open_query(
  getvariable('tour_url'),
  'SELECT i, i * i AS sq FROM range(1, 6) AS t(i)',
  0::UBIGINT, 0::UBIGINT, getvariable('tour_tls')::UBIGINT
);
SET VARIABLE tour_raw_frame = ducknng_fetch_query_raw(
  getvariable('tour_url'),
  (SELECT session_id    FROM tour_session2)::UBIGINT,
  (SELECT session_token FROM tour_session2)::VARCHAR,
  0::UBIGINT, 0::UBIGINT, getvariable('tour_tls')::UBIGINT
);
-- Inspect envelope fields, then decode the Arrow payload.
SELECT ducknng_frame_type_name(getvariable('tour_raw_frame')::BLOB) AS type_name,
       ducknng_frame_end_of_stream(getvariable('tour_raw_frame')::BLOB) AS end_of_stream;
SELECT *
FROM ducknng_parse_body(
  ducknng_frame_payload(getvariable('tour_raw_frame')::BLOB),
  'application/vnd.apache.arrow.stream'
);


+-----------+---------------+
| type_name | end_of_stream |
+-----------+---------------+
| result    | false         |
+-----------+---------------+
+---+----+
| i | sq |
+---+----+
| 1 | 1  |
| 2 | 4  |
| 3 | 9  |
| 4 | 16 |
| 5 | 25 |
+---+----+
```

**SSE streaming route.** `ducknng_add_stream_route` registers a
chunked-streaming route. The SQL must return a `chunk` column; each
non-null row is written to the client as a separate HTTP chunk.
`ducknng_format_sse` formats a Server-Sent Events event string.

``` sql
SELECT ducknng_add_stream_route(
  'tour_http', 'GET', '/events',
  'SELECT ducknng_format_sse(''tick '' || i::VARCHAR) AS chunk
   FROM generate_series(1, 3) t(i)'
);
-- ncurl transparently reassembles the chunked body.
SELECT ok, status, body_text
FROM ducknng_ncurl('http://127.0.0.1:18440/events', 'GET', NULL, NULL, 2000, 0::UBIGINT);
+--------------------------------------+
| ducknng_add_stream_route('tour_http', 'GET', '/events', 'SELECT ducknng_format_sse(''tick '' || i::VARCHAR) AS chunk
   FROM generate_series(1, 3) t(i)') |
+--------------------------------------+
| true                                 |
+--------------------------------------+
+------+--------+-----------+
|  ok  | status | body_text |
+------+--------+-----------+
| true | 200    | data: tick 1

data: tick 2

data: tick 3

          |
+------+--------+-----------+
```

Explicit cleanup — everything that was opened must be explicitly
stopped, closed, or dropped.

``` sql
SELECT ducknng_aio_drop(aio1) AND ducknng_aio_drop(aio2) AS aios_dropped FROM tour_aios;
DROP TABLE tour_aios;
DROP TABLE tour_session;
DROP TABLE tour_session2;
SELECT (ducknng_close_socket(getvariable('req_id')::UBIGINT)).ok AS socket_closed;
SELECT ducknng_stop_server('tour_http');
SELECT ducknng_stop_server('tour');
SELECT ducknng_drop_tls_config(getvariable('tour_tls')::UBIGINT);
+--------------+
| aios_dropped |
+--------------+
| true         |
+--------------+



+---------------+
| socket_closed |
+---------------+
| true          |
+---------------+
+----------------------------------+
| ducknng_stop_server('tour_http') |
+----------------------------------+
| true                             |
+----------------------------------+
+-----------------------------+
| ducknng_stop_server('tour') |
+-----------------------------+
| true                        |
+-----------------------------+
+---------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tour_tls') AS "UBIGINT")) |
+---------------------------------------------------------------------+
| true                                                                |
+---------------------------------------------------------------------+
```

## Development

Prerequisites: a C compiler, CMake, Python 3, and R with the `rmarkdown`
and [`duckknit`](https://github.com/rundel/duckknit) packages.

``` sh
# Download DuckDB headers and set up the build tree. Run once per checkout.
make configure

# Build the release extension binary.
# Output: build/release/ducknng.duckdb_extension
make release

# Run the SQL test suite against the release build.
make test_release

# Run the ducknng-only RPC microbench harness.
make rpc_bench

# Render the bulk benchmark report to bench/rpc_bulk_compare.md.
# This compares ducknng RPC over http/tcp/ipc/ws in both arrow_ipc_stream
# and ducknng_quack_batch modes, plus Quack over its public client path.
# Requires network access to INSTALL quack FROM core_nightly.
make rpc_bulk_compare

# Render README.md and demo/subscriber_gateway.md from their Rmd sources.
make docs
```

`make configure` downloads the DuckDB C API headers at the version
pinned in the Makefile (`DUCKDB_HEADER_VERSION`) and sets up CMake. The
extension targets `TARGET_DUCKDB_VERSION`. It builds with
`USE_UNSTABLE_C_API=1` to access DuckDB’s native Arrow conversion API
(`duckdb_to_arrow_schema`, `duckdb_data_chunk_to_arrow`) in the
result-to-IPC emit path. The only deprecated-entrypoint exception is the
pending-result streaming pair, isolated in
`src/ducknng_duckdb_streaming_compat.c`, because DuckDB v1.5.2 does not
yet expose an undeprecated C API for pending execution with incremental
chunk delivery. Query sessions do not silently fall back to materialized
results; when DuckDB provides a replacement streaming API, that
compatibility boundary is the single place to change. `make rpc_bench`
keeps the existing raw-RPC microbench path. `make rpc_bulk_compare` now
renders `bench/rpc_bulk_compare.md`, a benchmark report that records
machine details, compares ducknng RPC over `http`, `tcp`, `ipc`, and
`ws` in both `arrow_ipc_stream` and `ducknng_quack_batch` modes,
compares Quack over its public client/server path, and includes
concurrent reader, writer, and mixed reader/writer RPC slices. The
writer slice is a set-oriented remote SQL write dispatch benchmark, not
a client-side bulk append protocol. The report generates a local TPC-H
dataset if needed and installs `quack` from `core_nightly`. The unstable
functions in use are tracked by `tools/used_duckdb_unstable_api.R`; the
table below is generated fresh on every README render:

| ABI group                                      | Functions used                                                                                                                                                                                                                                                                                                                          | Count |
|------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------:|
| `unstable_deprecated`                          | `duckdb_pending_prepared_streaming`, `duckdb_stream_fetch_chunk`                                                                                                                                                                                                                                                                        |     2 |
| `unstable_new_arrow_functions`                 | `duckdb_data_chunk_to_arrow`, `duckdb_to_arrow_schema`                                                                                                                                                                                                                                                                                  |     2 |
| `unstable_new_config_options_functions`        | `duckdb_client_context_get_config_option`, `duckdb_config_option_set_default_scope`, `duckdb_config_option_set_default_value`, `duckdb_config_option_set_description`, `duckdb_config_option_set_name`, `duckdb_config_option_set_type`, `duckdb_create_config_option`, `duckdb_destroy_config_option`, `duckdb_register_config_option` |     9 |
| `unstable_new_error_data_functions`            | `duckdb_destroy_error_data`, `duckdb_error_data_has_error`, `duckdb_error_data_message`                                                                                                                                                                                                                                                 |     3 |
| `unstable_new_logger_functions`                | `duckdb_create_log_storage`, `duckdb_destroy_log_storage`, `duckdb_log_storage_set_extra_data`, `duckdb_log_storage_set_name`, `duckdb_log_storage_set_write_log_entry`, `duckdb_register_log_storage`                                                                                                                                  |     6 |
| `unstable_new_open_connect_functions`          | `duckdb_connection_get_arrow_options`, `duckdb_destroy_arrow_options`                                                                                                                                                                                                                                                                   |     2 |
| `unstable_new_prepared_statement_functions`    | `duckdb_prepared_statement_column_count`, `duckdb_prepared_statement_column_logical_type`, `duckdb_prepared_statement_column_name`                                                                                                                                                                                                      |     3 |
| `unstable_new_query_execution_functions`       | `duckdb_result_get_arrow_options`                                                                                                                                                                                                                                                                                                       |     1 |
| `unstable_new_scalar_function_functions`       | `duckdb_scalar_function_set_bind`                                                                                                                                                                                                                                                                                                       |     1 |
| `unstable_new_scalar_function_state_functions` | `duckdb_scalar_function_set_init`                                                                                                                                                                                                                                                                                                       |     1 |
| `unstable_new_string_functions`                | `duckdb_valid_utf8_check`                                                                                                                                                                                                                                                                                                               |     1 |
| `unstable_new_vector_functions`                | `duckdb_unsafe_vector_assign_string_element_len`                                                                                                                                                                                                                                                                                        |     1 |

`make test_release` runs the SQL test suite with DuckDB’s test runner.
All tests live under `test/sql/` as `.test` files. The public function
surface is verified by `test/sql/ducknng_public_surface.test`, which
reads `function_catalog/functions.yaml` and checks that every declared
name appears in the loaded extension catalog.

To regenerate the function catalog markdown (embedded in this README)
after adding or renaming public functions:

``` sh
make function_catalog  # regenerates function_catalog/functions.md
```

Every new public SQL function must have a corresponding entry in
`function_catalog/functions.yaml` or the surface test will fail.

## Lifetime and manual cleanup

Long-lived handles are manually managed at the SQL surface. Stop, close,
or drop them explicitly; runtime teardown is fallback cleanup, not the
primary lifecycle model.

- Stop servers with `ducknng_stop_server(...)`
- Close sockets with `ducknng_close_socket(...)`
- Drop AIO handles with `ducknng_aio_drop(...)`
- Drop TLS config handles with `ducknng_drop_tls_config(...)`
- Close query sessions with `ducknng_close_query(...)`

`ducknng_aio_cancel(...)` is cancellation control, not the destructor.
`ducknng_cancel_query(...)` is best-effort session control, not a
replacement for the explicit close path.

## Function catalog

The generated catalog is the public SQL surface. Any loaded `ducknng__*`
helper names are internal macro implementation details required by
DuckDB’s stable C API path and are not supported user APIs.

<details>
<summary>
<strong>Expand the full generated function catalog</strong>
</summary>

# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

## Service Control

| name                                  | kind   | arguments                                                                                     | returns   | description                                                                                                                                        |
|---------------------------------------|--------|-----------------------------------------------------------------------------------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `ducknng_start_server`                | scalar | `name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id[, ip_allowlist_json]` | `BOOLEAN` | Start a named ducknng service and choose the carrier from the listen URL scheme.                                                                   |
| `ducknng_stop_server`                 | scalar | `name`                                                                                        | `BOOLEAN` | Stop a named ducknng service.                                                                                                                      |
| `ducknng_service_inflight`            | scalar | `name`                                                                                        | `UBIGINT` | Return the current in-flight request count for a named service. Re-evaluated on every call, making it suitable for use inside recursive poll CTEs. |
| `ducknng_set_service_execution_model` | scalar | `name, model`                                                                                 | `BOOLEAN` | Set the DuckDB connection execution model used by service-side SQL.                                                                                |
| `ducknng_set_execution_pool_max`      | scalar | `n`                                                                                           | `UBIGINT` | Set the runtime execution-connection pool grow ceiling and return the effective value.                                                             |
| `ducknng_execution_pool_max`          | scalar |                                                                                               | `UBIGINT` | Return the current execution-connection pool grow ceiling.                                                                                         |

## Introspection

| name                          | kind   | arguments                          | returns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | description                                                                                                                                                                                                                                             |
|-------------------------------|--------|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ducknng_list_servers`        | table  |                                    | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, execution_model VARCHAR, sessions UBIGINT, active_pipes UBIGINT, max_open_sessions UBIGINT, max_active_pipes UBIGINT, inflight_requests UBIGINT, max_inflight_requests UBIGINT, max_sessions_per_peer_identity UBIGINT, max_inflight_per_principal UBIGINT, max_reply_bytes_per_principal UBIGINT, max_session_open_rate_per_principal UBIGINT, tls_enabled BOOLEAN, tls_auth_mode INTEGER, peer_identity_required BOOLEAN, peer_allowlist_active BOOLEAN, ip_allowlist_active BOOLEAN, sql_authorizer_active BOOLEAN, peer_allowlist_count UBIGINT, ip_allowlist_count UBIGINT)` | List registered ducknng services.                                                                                                                                                                                                                       |
| `ducknng_read_monitor`        | table  | `name, after_seq, max_events`      | `TABLE(seq UBIGINT, ts_ms UBIGINT, pipe_id UBIGINT, service_name VARCHAR, listen VARCHAR, transport_family VARCHAR, scheme VARCHAR, event VARCHAR, admitted BOOLEAN, reason VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, peer_identity VARCHAR)`                                                                                                                                                                                                                                                                                                                                                                                                     | Read the bounded per-service NNG pipe monitor event stream.                                                                                                                                                                                             |
| `ducknng_monitor_status`      | table  | `name`                             | `TABLE(service_name VARCHAR, event_capacity UBIGINT, event_count UBIGINT, oldest_seq UBIGINT, newest_seq UBIGINT, dropped_events UBIGINT, active_pipes UBIGINT, max_active_pipes UBIGINT)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Return pipe monitor ring status and active-pipe counters for a running service.                                                                                                                                                                         |
| `ducknng_list_pipes`          | table  | `name`                             | `TABLE(pipe_id UBIGINT, opened_ms UBIGINT, service_name VARCHAR, listen VARCHAR, transport_family VARCHAR, scheme VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, peer_identity VARCHAR)`                                                                                                                                                                                                                                                                                                                                                                                                                                                               | List currently active NNG pipes for a running service.                                                                                                                                                                                                  |
| `ducknng_nng_stats`           | table  |                                    | `TABLE(scope VARCHAR, name VARCHAR, type VARCHAR, unit VARCHAR, value UBIGINT, svalue VARCHAR, description VARCHAR)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Snapshot of NNG’s native statistics tree, flattened into rows.                                                                                                                                                                                          |
| `ducknng_monitor_socket`      | scalar | `socket_id`                        | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Opt a client socket into pipe-event monitoring. Subsequent pipe add/remove events are captured into a per-socket ring readable with ducknng_read_socket_monitor().                                                                                      |
| `ducknng_read_socket_monitor` | table  | `socket_id, after_seq, max_events` | `TABLE(seq UBIGINT, ts_ms UBIGINT, pipe_id UBIGINT, event VARCHAR, dropped UBIGINT)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Read captured pipe events for a monitored client socket. event is ‘add’ or ‘remove’; dropped is the running count of events evicted from the ring.                                                                                                      |
| `ducknng_socket_monitor_wait` | scalar | `socket_id, after_seq, timeout_ms` | `UBIGINT`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Block until a monitored socket records a pipe event newer than after_seq, or until timeout_ms elapses; returns the current newest event seq.                                                                                                            |
| `ducknng_log_entries`         | table  |                                    | `TABLE(ts TIMESTAMP, level VARCHAR, log_type VARCHAR, message VARCHAR)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Return a snapshot of the most recent DuckDB log entries captured by the ducknng log ring.                                                                                                                                                               |
| `ducknng_enable_log_capture`  | scalar |                                    | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Wire the ducknng log ring into DuckDB’s internal logger so that DuckDB log entries are captured and visible through ducknng_log_entries(). Returns TRUE if capture is active after the call, FALSE if registration failed. Safe to call multiple times. |

## Method Registry

| name                           | kind   | arguments             | returns                                                                                                                                                                                                                                                                       | description                                                                            |
|--------------------------------|--------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `ducknng_register_exec_method` | scalar | `[requires_auth]`     | `BOOLEAN`                                                                                                                                                                                                                                                                     | Register the built-in exec RPC method explicitly.                                      |
| `ducknng_set_method_auth`      | scalar | `name, requires_auth` | `BOOLEAN`                                                                                                                                                                                                                                                                     | Set descriptor-level verified-peer-identity authorization for a registered RPC method. |
| `ducknng_unregister_method`    | scalar | `name`                | `BOOLEAN`                                                                                                                                                                                                                                                                     | Unregister a method from the runtime registry.                                         |
| `ducknng_unregister_family`    | scalar | `family`              | `UBIGINT`                                                                                                                                                                                                                                                                     | Unregister all methods in a family and return the number removed.                      |
| `ducknng_list_methods`         | table  |                       | `TABLE(name VARCHAR, family VARCHAR, summary VARCHAR, transport_pattern VARCHAR, request_payload_format VARCHAR, response_payload_format VARCHAR, response_mode VARCHAR, request_schema_json VARCHAR, response_schema_json VARCHAR, requires_auth BOOLEAN, disabled BOOLEAN)` | List the currently registered RPC methods in the runtime registry.                     |

## Primitive Transport

| name                          | kind   | arguments                                       | returns                                                                                                                                                         | description                                                                                         |
|-------------------------------|--------|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `ducknng_open_socket`         | scalar | `protocol`                                      | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Open a client socket handle for a supported NNG protocol.                                           |
| `ducknng_dial_socket`         | scalar | `socket_id, url, timeout_ms, tls_config_id`     | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Dial a URL using an opened socket handle.                                                           |
| `ducknng_listen_socket`       | scalar | `socket_id, url, recv_max_bytes, tls_config_id` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Bind a socket handle to a listen URL and start its NNG listener.                                    |
| `ducknng_close_socket`        | scalar | `socket_id`                                     | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Close a client socket handle.                                                                       |
| `ducknng_send_socket_raw`     | scalar | `socket_id, frame, timeout_ms`                  | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Send one raw frame through an active socket handle.                                                 |
| `ducknng_recv_socket_raw`     | scalar | `socket_id, timeout_ms`                         | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Receive one raw frame from an active socket handle.                                                 |
| `ducknng_subscribe_socket`    | scalar | `socket_id, topic`                              | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Register a raw topic prefix on a sub socket.                                                        |
| `ducknng_unsubscribe_socket`  | scalar | `socket_id, topic`                              | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)`                                 | Remove a raw topic prefix from a sub socket.                                                        |
| `ducknng_list_sockets`        | table  |                                                 | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, listening BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | List client socket handles in the runtime.                                                          |
| `ducknng_request`             | table  | `url, payload, timeout_ms, tls_config_id`       | `TABLE(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, payload BLOB)`                                                                  | Perform a one-shot raw request and return a structured result row.                                  |
| `ducknng_request_socket`      | table  | `socket_id, payload, timeout_ms`                | `TABLE(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, payload BLOB)`                                                                  | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_request_raw`         | scalar | `url, payload, timeout_ms, tls_config_id`       | `BLOB`                                                                                                                                                          | Perform a one-shot raw request and return the raw reply frame bytes.                                |
| `ducknng_request_socket_raw`  | scalar | `socket_id, payload, timeout_ms`                | `BLOB`                                                                                                                                                          | Perform a raw request through a dialed socket handle and return the raw reply frame bytes.          |
| `ducknng_decode_frame`        | table  | `frame`                                         | `TABLE(ok BOOLEAN, error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)`        | Decode a raw ducknng frame into envelope fields and extracted payload columns.                      |
| `ducknng_frame_payload`       | scalar | `frame`                                         | `BLOB`                                                                                                                                                          | Extract the payload bytes from one raw ducknng frame.                                               |
| `ducknng_frame_payload_text`  | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the payload as UTF-8 text when a raw ducknng frame carries a textual payload.               |
| `ducknng_frame_error_text`    | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the protocol-level error text from a raw ducknng error frame.                               |
| `ducknng_frame_version`       | scalar | `frame`                                         | `UTINYINT`                                                                                                                                                      | Extract the protocol version field from one raw ducknng frame.                                      |
| `ducknng_frame_type`          | scalar | `frame`                                         | `UTINYINT`                                                                                                                                                      | Extract the numeric reply type field from one raw ducknng frame.                                    |
| `ducknng_frame_flags`         | scalar | `frame`                                         | `UINTEGER`                                                                                                                                                      | Extract the reply flags bitset from one raw ducknng frame.                                          |
| `ducknng_frame_type_name`     | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the symbolic reply type name from one raw ducknng frame.                                    |
| `ducknng_frame_name`          | scalar | `frame`                                         | `VARCHAR`                                                                                                                                                       | Extract the method or reply name field from one raw ducknng frame.                                  |
| `ducknng_frame_end_of_stream` | scalar | `frame`                                         | `BOOLEAN`                                                                                                                                                       | Report whether one raw ducknng frame carries the end-of-stream reply flag.                          |

## Transport Security

| name                                 | kind   | arguments                                                                                                                                                                                                      | returns                                                                                                                                                                                                                                                                                                                                                                                                                                                    | description                                                                                                                     |
|--------------------------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| `ducknng_list_tls_configs`           | table  |                                                                                                                                                                                                                | `TABLE(tls_config_id UBIGINT, source VARCHAR, enabled BOOLEAN, has_cert_key_file BOOLEAN, has_ca_file BOOLEAN, has_cert_pem BOOLEAN, has_key_pem BOOLEAN, has_ca_pem BOOLEAN, has_password BOOLEAN, auth_mode INTEGER, peer_allowlist_active BOOLEAN, peer_allowlist_count UBIGINT, peer_allowlist_json VARCHAR)`                                                                                                                                          | List registered TLS config handles and the kinds of material they contain.                                                      |
| `ducknng_drop_tls_config`            | scalar | `tls_config_id`                                                                                                                                                                                                | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Remove a registered TLS config handle from the runtime.                                                                         |
| `ducknng_set_tls_peer_allowlist`     | scalar | `tls_config_id, identities_json`                                                                                                                                                                               | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Set the default exact peer-identity allowlist on a TLS config handle.                                                           |
| `ducknng_set_service_peer_allowlist` | scalar | `name, identities_json`                                                                                                                                                                                        | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Dynamically set the exact peer-identity allowlist for a running service.                                                        |
| `ducknng_set_service_ip_allowlist`   | scalar | `name, cidrs_json`                                                                                                                                                                                             | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Dynamically set the IP/CIDR remote-address allowlist for a running service.                                                     |
| `ducknng_set_service_limits`         | scalar | `name, max_open_sessions[, max_active_pipes[, max_inflight_requests[, max_sessions_per_peer_identity[, max_inflight_per_principal[, max_reply_bytes_per_principal[, max_session_open_rate_per_principal]]]]]]` | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Set service resource limits.                                                                                                    |
| `ducknng_auth_context`               | table  |                                                                                                                                                                                                                | `TABLE(phase VARCHAR, service_name VARCHAR, transport_family VARCHAR, scheme VARCHAR, listen VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, tls_verified BOOLEAN, peer_identity VARCHAR, peer_allowlist_active BOOLEAN, ip_allowlist_active BOOLEAN, sql_authorizer_active BOOLEAN, http_method VARCHAR, http_path VARCHAR, content_type VARCHAR, body_bytes UBIGINT, rpc_method VARCHAR, rpc_type VARCHAR, payload_bytes UBIGINT)` | Expose the current request context to a SQL authorization callback.                                                             |
| `ducknng_set_service_authorizer`     | scalar | `name, authorizer_sql`                                                                                                                                                                                         | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Install or clear a service-level SQL authorization callback evaluated uniformly for framed RPC requests before method dispatch. |
| `ducknng_self_signed_tls_config`     | scalar | `common_name, valid_days, auth_mode`                                                                                                                                                                           | `UBIGINT`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Generate a self-signed development certificate and register it as a TLS config handle.                                          |
| `ducknng_tls_config_from_pem`        | scalar | `cert_pem, key_pem, ca_pem, password, auth_mode`                                                                                                                                                               | `UBIGINT`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Register a TLS config handle from in-memory PEM material.                                                                       |
| `ducknng_tls_config_from_files`      | scalar | `cert_key_file, ca_file, password, auth_mode`                                                                                                                                                                  | `UBIGINT`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Register a TLS config handle from file-backed certificate material.                                                             |

## HTTP Transport

| name                            | kind   | arguments                                                                                                                 | returns                                                                                                                                                                                                                                                              | description                                                                                                                         |
|---------------------------------|--------|---------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `ducknng_ncurl`                 | table  | `url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]`                                                | `TABLE(ok BOOLEAN, status INTEGER, error VARCHAR, headers_json VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                               | Perform one HTTP or HTTPS request and return an in-band result row.                                                                 |
| `ducknng_ncurl_aio`             | scalar | `url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]`                                                | `UBIGINT`                                                                                                                                                                                                                                                            | Launch one asynchronous HTTP or HTTPS request and return a future-like aio handle id.                                               |
| `ducknng_ncurl_aio_collect`     | table  | `aio_ids, wait_ms`                                                                                                        | `TABLE(aio_id UBIGINT, ok BOOLEAN, status INTEGER, error VARCHAR, headers_json VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                               | Wait for asynchronous ncurl handles and return one raw HTTP result row per newly collected terminal operation.                      |
| `ducknng_ncurl_table`           | table  | `url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]`                                                | `TABLE(dynamic by response Content-Type)`                                                                                                                                                                                                                            | Perform one HTTP or HTTPS request and parse a successful response body into a DuckDB table using the built-in body codec providers. |
| `ducknng_register_http_profile` | scalar | `profile_id, scheme, host, port, path_prefix, method, tls_required, auth_header_name, auth_header_value[, expires_at_ms]` | `BOOLEAN`                                                                                                                                                                                                                                                            | Register or replace an outbound HTTP credential profile with fail-closed request scope.                                             |
| `ducknng_drop_http_profile`     | scalar | `profile_id`                                                                                                              | `BOOLEAN`                                                                                                                                                                                                                                                            | Drop a registered outbound HTTP credential profile.                                                                                 |
| `ducknng_list_http_profiles`    | table  |                                                                                                                           | `TABLE(profile_id VARCHAR, scheme VARCHAR, host VARCHAR, port INTEGER, has_port BOOLEAN, path_prefix VARCHAR, method VARCHAR, tls_required BOOLEAN, auth_header_names_json VARCHAR, version UBIGINT, created_ms UBIGINT, updated_ms UBIGINT, expires_at_ms UBIGINT)` | List registered outbound HTTP profiles with redacted credential metadata.                                                           |

## Body Codecs

| name                       | kind   | arguments                     | returns                                                                                                            | description                                                                                                              |
|----------------------------|--------|-------------------------------|--------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| `ducknng_list_codecs`      | table  |                               | `TABLE(provider VARCHAR, media_types VARCHAR, kind VARCHAR, function_name VARCHAR, output VARCHAR, notes VARCHAR)` | List built-in body serialization/deserialization providers and any registered user codec hooks.                          |
| `ducknng_register_codec`   | scalar | `content_type, function_name` | `BOOLEAN`                                                                                                          | Register a user body codec that decodes a BLOB body into a single VARCHAR value through an existing scalar SQL function. |
| `ducknng_unregister_codec` | scalar | `content_type`                | `BOOLEAN`                                                                                                          | Remove a previously registered user body codec for a content type.                                                       |
| `ducknng_parse_body`       | table  | `body, content_type`          | `TABLE(dynamic by provider)`                                                                                       | Parse one response/request body BLOB according to its content type.                                                      |

## HTTP Routes

| name                                    | kind        | arguments                                                                          | returns                                                                                                                                                                                                                                                                                                                                                                                 | description                                                                                                            |
|-----------------------------------------|-------------|------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `ducknng_register_http_route`           | scalar      | `service_name, method, path, handler_sql[, request_max_bytes]`                     | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Register one exact-path HTTP route beside the framed RPC mount of an existing <http://> or <https://> service.         |
| `ducknng_register_http_route_pattern`   | scalar      | `service_name, method, match_kind, path_pattern, handler_sql[, request_max_bytes]` | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Register one low-level HTTP route pattern beside the framed RPC mount using exact, prefix, or template matching.       |
| `ducknng_unregister_http_route`         | scalar      | `service_name, method, path`                                                       | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Remove one previously registered exact-path HTTP route from a service.                                                 |
| `ducknng_unregister_http_route_pattern` | scalar      | `service_name, method, match_kind, path_pattern`                                   | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Remove one previously registered prefix, template, or explicit exact route pattern from a service.                     |
| `ducknng_list_http_routes`              | table       |                                                                                    | `TABLE(service_id UBIGINT, route_id UBIGINT, request_max_bytes UBIGINT, service_name VARCHAR, method VARCHAR, match_kind VARCHAR, path VARCHAR, handler_sql VARCHAR, auth_require_identity BOOLEAN, static_dir_path VARCHAR, auth_allow_identities_json VARCHAR)`                                                                                                                       | List the currently registered HTTP routes across running services, including their match kind and stored path pattern. |
| `ducknng_set_http_route_auth`           | scalar      | `service_name, method, path[, require_identity[, allow_identities_json]]`          | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Set authentication requirements on a registered HTTP route.                                                            |
| `ducknng_register_http_static`          | scalar      | `service_name, path_prefix, dir_path`                                              | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Register a prefix route that serves static files from a directory on the server filesystem.                            |
| `ducknng_register_http_worker`          | scalar      | `service_name, worker_name, sql, interval_ms`                                      | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Register a background SQL worker that runs on a recurring interval while the HTTP service is active.                   |
| `ducknng_unregister_http_worker`        | scalar      | `service_name, worker_name`                                                        | `BOOLEAN`                                                                                                                                                                                                                                                                                                                                                                               | Unregister a previously registered HTTP background worker.                                                             |
| `ducknng_list_http_workers`             | table       |                                                                                    | `TABLE(service_name VARCHAR, worker_name VARCHAR, sql VARCHAR, interval_ms UBIGINT)`                                                                                                                                                                                                                                                                                                    | List all currently registered HTTP background workers across running services.                                         |
| `ducknng_http_ndjson`                   | table_macro | `status, body_text`                                                                | `TABLE(status INTEGER, content_type VARCHAR, body VARCHAR)`                                                                                                                                                                                                                                                                                                                             | Convenience table macro for returning an NDJSON HTTP response from a route handler.                                    |
| `ducknng_http_sse`                      | table_macro | `status, body_text`                                                                | `TABLE(status INTEGER, content_type VARCHAR, body VARCHAR)`                                                                                                                                                                                                                                                                                                                             | Convenience table macro for returning a Server-Sent Events HTTP response from a route handler.                         |
| `ducknng_http_request`                  | table       |                                                                                    | `TABLE(service_name VARCHAR, listen VARCHAR, scheme VARCHAR, method VARCHAR, path VARCHAR, query_string VARCHAR, content_type VARCHAR, headers_json VARCHAR, caller_identity VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, route_method VARCHAR, route_match_kind VARCHAR, route_path VARCHAR, path_params_json VARCHAR, body_bytes UBIGINT, route_id UBIGINT, remote_port INTEGER)` | Expose the current HTTP request context while SQL runs inside an active route handler.                                 |
| `ducknng_http_request_body`             | table       |                                                                                    | `TABLE(body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                                                                                   | Expose the current HTTP request body while SQL runs inside an active route handler.                                    |
| `ducknng_http_headers_get`              | scalar      | `headers_json, name`                                                               | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one header value from ducknng’s canonical HTTP header JSON.                                                     |
| `ducknng_http_headers_build`            | scalar      | `names, values`                                                                    | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Build ducknng’s canonical HTTP header JSON from parallel name and value lists.                                         |
| `ducknng_http_query_param_get`          | scalar      | `query_string, name`                                                               | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one decoded query-string parameter value.                                                                       |
| `ducknng_http_cookie_get`               | scalar      | `cookie_header, name`                                                              | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one cookie value from a Cookie header string.                                                                   |
| `ducknng_http_path_params_get`          | scalar      | `path_params_json, name`                                                           | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Return one template-route path parameter from path_params_json.                                                        |
| `ducknng_http_header`                   | scalar      | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one request header by name.                                                           |
| `ducknng_http_query_param`              | scalar      | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one decoded query parameter by name.                                                  |
| `ducknng_http_cookie`                   | scalar      | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one request cookie by name.                                                           |
| `ducknng_http_path_param`               | scalar      | `name`                                                                             | `VARCHAR`                                                                                                                                                                                                                                                                                                                                                                               | Route-local shortcut for reading one template path parameter by name.                                                  |
| `ducknng_http_response`                 | table       | `status, headers_json, content_type, body, body_text`                              | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build the one-row response shape expected by a route handler.                                                          |
| `ducknng_http_text`                     | table       | `status, body_text`                                                                | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build a one-row plain-text HTTP route response.                                                                        |
| `ducknng_http_json`                     | table       | `status, body_text`                                                                | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build a one-row JSON HTTP route response from a text body.                                                             |
| `ducknng_http_binary`                   | table       | `status, body`                                                                     | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)`                                                                                                                                                                                                                                                                                       | Build a one-row binary HTTP route response.                                                                            |

## Async I/O

| name                               | kind   | arguments                                                                            | returns                                                                                                                                                                                                                                                             | description                                                                                                        |
|------------------------------------|--------|--------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `ducknng_request_raw_aio`          | scalar | `url, frame, timeout_ms, tls_config_id`                                              | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one raw req/rep roundtrip asynchronously and return a future-like aio handle id.                            |
| `ducknng_get_rpc_manifest_raw_aio` | scalar | `url, timeout_ms, tls_config_id`                                                     | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one asynchronous manifest RPC request and return an aio handle id for the raw reply frame.                  |
| `ducknng_run_rpc_raw_aio`          | scalar | `url, sql, timeout_ms, tls_config_id`                                                | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one asynchronous metadata-only exec RPC request and return an aio handle id for the raw reply frame.        |
| `ducknng_open_query_raw_aio`       | scalar | `url, sql, batch_rows, batch_bytes, timeout_ms, tls_config_id`                       | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one asynchronous query_open request and return an aio handle id for the raw reply frame.                    |
| `ducknng_fetch_query_raw_aio`      | scalar | `url, session_id, session_token, batch_rows, batch_bytes, timeout_ms, tls_config_id` | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one asynchronous fetch request and return an aio handle id for the raw reply frame.                         |
| `ducknng_close_query_raw_aio`      | scalar | `url, session_id, session_token, timeout_ms, tls_config_id`                          | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one asynchronous close request and return an aio handle id for the raw reply frame.                         |
| `ducknng_cancel_query_raw_aio`     | scalar | `url, session_id, session_token, timeout_ms, tls_config_id`                          | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one asynchronous cancel request and return an aio handle id for the raw reply frame.                        |
| `ducknng_request_socket_raw_aio`   | scalar | `socket_id, frame, timeout_ms`                                                       | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one raw req/rep roundtrip asynchronously on an existing req socket handle and return an aio handle id.      |
| `ducknng_send_socket_raw_aio`      | scalar | `socket_id, frame, timeout_ms`                                                       | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one raw socket send asynchronously and return an aio handle id.                                             |
| `ducknng_recv_socket_raw_aio`      | scalar | `socket_id, timeout_ms`                                                              | `UBIGINT`                                                                                                                                                                                                                                                           | Launch one raw socket receive asynchronously and return an aio handle id.                                          |
| `ducknng_aio_ready`                | scalar | `aio_id`                                                                             | `BOOLEAN`                                                                                                                                                                                                                                                           | Return whether an aio handle has reached a terminal state.                                                         |
| `ducknng_aio_wait`                 | scalar | `aio_ids, wait_ms`                                                                   | `BOOLEAN`                                                                                                                                                                                                                                                           | Wait until any requested aio handle reaches a terminal state without collecting or dropping it.                    |
| `ducknng_aio_status`               | table  | `aio_id`                                                                             | `TABLE(aio_id UBIGINT, exists BOOLEAN, kind VARCHAR, state VARCHAR, phase VARCHAR, terminal BOOLEAN, send_done BOOLEAN, send_ok BOOLEAN, recv_done BOOLEAN, recv_ok BOOLEAN, has_reply_frame BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR)` | Inspect the current or terminal status of one aio handle, including send-phase and recv-phase completion.          |
| `ducknng_aio_collect`              | table  | `aio_ids, wait_ms`                                                                   | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame BLOB, nng_error INTEGER, nng_error_message VARCHAR)`                                                                                                                                                        | Wait for any requested aio handles to finish and return one row per newly collected terminal result.               |
| `ducknng_aio_collect_decoded`      | table  | `aio_ids, wait_ms`                                                                   | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame_ok BOOLEAN, frame_error VARCHAR, version UTINYINT, type UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR, nng_error INTEGER, nng_error_message VARCHAR)`       | Wait for framed aio handles, collect their terminal frame rows, and project the decoded envelope columns directly. |
| `ducknng_aio_cancel`               | scalar | `aio_id`                                                                             | `BOOLEAN`                                                                                                                                                                                                                                                           | Request cancellation of a pending aio handle.                                                                      |
| `ducknng_aio_drop`                 | scalar | `aio_id`                                                                             | `BOOLEAN`                                                                                                                                                                                                                                                           | Release a terminal aio handle from the runtime registry.                                                           |

## RPC Helper

| name                           | kind   | arguments                 | returns                                                                                               | description                                                                                                                 |
|--------------------------------|--------|---------------------------|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `ducknng_get_rpc_manifest`     | table  | `url, tls_config_id`      | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)`                                                  | Request the RPC manifest and return a structured result row.                                                                |
| `ducknng_get_rpc_manifest_raw` | scalar | `url, tls_config_id`      | `BLOB`                                                                                                | Request the RPC manifest and return the raw reply frame as BLOB.                                                            |
| `ducknng_run_rpc`              | table  | `url, sql, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute a metadata-oriented RPC call and return a structured result row.                                                    |
| `ducknng_run_rpc_raw`          | scalar | `url, sql, tls_config_id` | `BLOB`                                                                                                | Execute the exec RPC and return the raw reply frame as BLOB.                                                                |
| `ducknng_query_rpc`            | table  | `url, sql, tls_config_id` | `table`                                                                                               | Execute a row-returning RPC query as a session convenience wrapper and expose the fetched Arrow IPC rows as a DuckDB table. |

## RPC Session

| name                        | kind   | arguments                                                                | returns                                                                                                                                                                                               | description                                                                                                    |
|-----------------------------|--------|--------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| `ducknng_open_query`        | table  | `url, sql, batch_rows, batch_bytes, tls_config_id`                       | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)`                                      | Open a server-side query session and return the JSON control metadata as a structured row.                     |
| `ducknng_fetch_query`       | table  | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT, payload BLOB, end_of_stream BOOLEAN)` | Fetch the next session reply and return either JSON control metadata or an Arrow IPC batch payload.            |
| `ducknng_fetch_query_table` | table  | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `TABLE(dynamic from Arrow IPC batch)`                                                                                                                                                                 | Fetch one session row batch and decode the returned Arrow IPC payload directly into a DuckDB table.            |
| `ducknng_close_query`       | table  | `url, session_id, session_token, tls_config_id`                          | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)`                                      | Close a server-side query session and return the JSON control metadata as a structured row.                    |
| `ducknng_cancel_query`      | table  | `url, session_id, session_token, tls_config_id`                          | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)`                                      | Request cancellation for a server-side query session and return the JSON control metadata as a structured row. |
| `ducknng_open_query_raw`    | scalar | `url, sql, batch_rows, batch_bytes, tls_config_id`                       | `BLOB`                                                                                                                                                                                                | Open a server-side query session and return the raw reply frame as BLOB.                                       |
| `ducknng_fetch_query_raw`   | scalar | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `BLOB`                                                                                                                                                                                                | Fetch the next session reply and return the raw reply frame as BLOB.                                           |
| `ducknng_close_query_raw`   | scalar | `url, session_id, session_token, tls_config_id`                          | `BLOB`                                                                                                                                                                                                | Close a server-side query session and return the raw reply frame as BLOB.                                      |
| `ducknng_cancel_query_raw`  | scalar | `url, session_id, session_token, tls_config_id`                          | `BLOB`                                                                                                                                                                                                | Cancel a server-side query session and return the raw reply frame as BLOB.                                     |

</details>

## Transport layer

### NNG socket patterns

NNG exposes seven socket patterns. All share the same
`ducknng_open_socket(kind)` / `ducknng_listen_socket(...)` /
`ducknng_dial_socket(...)` entry points and the same synchronous/AIO
send-recv helpers.

| Pattern           | `kind`                    | Direction         | Typical use                                 |
|-------------------|---------------------------|-------------------|---------------------------------------------|
| Request/reply     | `req` / `rep`             | 1:1 round-trip    | RPC, sync calls                             |
| Pipeline          | `push` / `pull`           | 1-way fan-out     | Task queues, load distribution              |
| Publish/subscribe | `pub` / `sub`             | 1:N broadcast     | Event fan-out; subscribers filter by prefix |
| Survey            | `surveyor` / `respondent` | 1:N with replies  | Health checks, membership queries           |
| Bus               | `bus`                     | N:N mesh          | Fully-connected peer broadcast              |
| Pair              | `pair`                    | 1:1 bidirectional | Point-to-point channels                     |

The push/pull example below shows the pattern shared by all of them: arm
an async receiver, send synchronously, collect the AIO result.

``` sql
SET VARIABLE pull_s = (ducknng_open_socket('pull')).socket_id;
SET VARIABLE pull_ok = (ducknng_listen_socket(
  getvariable('pull_s')::UBIGINT,
  'ipc:///tmp/ducknng_readme_pushpull.ipc',
  134217728, 0::UBIGINT
)).ok;
SET VARIABLE push_s = (ducknng_open_socket('push')).socket_id;
SET VARIABLE push_ok = (ducknng_dial_socket(
  getvariable('push_s')::UBIGINT,
  'ipc:///tmp/ducknng_readme_pushpull.ipc',
  1000, 0::UBIGINT
)).ok;
CREATE TEMP TABLE pp_recv AS
SELECT ducknng_recv_socket_raw_aio(getvariable('pull_s')::UBIGINT, 1000) AS recv_aio;
SET VARIABLE pp_sent = (ducknng_send_socket_raw(
  getvariable('push_s')::UBIGINT, from_hex('6865796f'), 1000
)).ok;
SELECT getvariable('pp_sent')::BOOLEAN AS sent,
       ok, hex(frame) = '6865796F' AS got_payload
FROM ducknng_aio_collect((SELECT list_value(recv_aio) FROM pp_recv), 1000);
SELECT ducknng_aio_drop((SELECT recv_aio FROM pp_recv)) AS dropped;
DROP TABLE pp_recv;
SELECT (ducknng_close_socket(getvariable('push_s')::UBIGINT)).ok AS closed_push,
       (ducknng_close_socket(getvariable('pull_s')::UBIGINT)).ok AS closed_pull;






+------+------+-------------+
| sent |  ok  | got_payload |
+------+------+-------------+
| true | true | true        |
+------+------+-------------+
+---------+
| dropped |
+---------+
| true    |
+---------+

+-------------+-------------+
| closed_push | closed_pull |
+-------------+-------------+
| true        | true        |
+-------------+-------------+
```

For pub/sub, call `ducknng_subscribe_socket(socket_id, prefix_blob)`
before dialing. For surveyor, send then arm multiple respondent
receivers. For bus, every peer dials the same listener so each send
reaches all other peers.

### TLS (`tls+tcp://`, `wss://`, `https://`)

TLS handles are created once and reused across services and clients.
Material can come from the filesystem or from in-memory PEM content.

``` sql
-- Self-signed development cert: no files needed.
SET VARIABLE tls_self = ducknng_self_signed_tls_config('127.0.0.1', 365, 0);
SELECT ducknng_start_server(
  'tls_demo', 'tls+tcp://127.0.0.1:45453',
  1, 134217728, 300000,
  getvariable('tls_self')::UBIGINT
);
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45453',
    from_hex('01000000000000000000000000000000000000000000'),
    1000,
    getvariable('tls_self')::UBIGINT
  )
);
SELECT ducknng_stop_server('tls_demo');
SELECT ducknng_drop_tls_config(getvariable('tls_self')::UBIGINT);

+---------------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('tls_demo', 'tls+tcp://127.0.0.1:45453', 1, 134217728, 300000, CAST(getvariable('tls_self') AS "UBIGINT")) |
+---------------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                            |
+---------------------------------------------------------------------------------------------------------------------------------+
+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
+---------------------------------+
| ducknng_stop_server('tls_demo') |
+---------------------------------+
| true                            |
+---------------------------------+
+---------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tls_self') AS "UBIGINT")) |
+---------------------------------------------------------------------+
| true                                                                |
+---------------------------------------------------------------------+
```

File-backed material uses
`ducknng_tls_config_from_files(cert_key_file, ca_file, password, auth_mode)`.
Set `auth_mode = 2` to require client certificates (mTLS). The
dispatcher derives caller identity from the first verified SAN
(`tls:san:<value>`) falling back to CN (`tls:cn:<name>`). Peer identity
and IP/CIDR allowlists narrow admission before SQL dispatch.

``` sql
SET VARIABLE tls_files = ducknng_tls_config_from_files(
  'test/certs/loopback-cert-key.pem',
  'test/certs/loopback-ca.pem',
  NULL, 0
);
SELECT ducknng_start_server(
  'tls_files_demo', 'tls+tcp://127.0.0.1:45454',
  1, 134217728, 300000,
  getvariable('tls_files')::UBIGINT
);
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'tls+tcp://127.0.0.1:45454',
    from_hex('01000000000000000000000000000000000000000000'),
    1000,
    getvariable('tls_files')::UBIGINT
  )
);
SELECT ducknng_stop_server('tls_files_demo');
SELECT ducknng_drop_tls_config(getvariable('tls_files')::UBIGINT);

+----------------------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('tls_files_demo', 'tls+tcp://127.0.0.1:45454', 1, 134217728, 300000, CAST(getvariable('tls_files') AS "UBIGINT")) |
+----------------------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                                   |
+----------------------------------------------------------------------------------------------------------------------------------------+
+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
+---------------------------------------+
| ducknng_stop_server('tls_files_demo') |
+---------------------------------------+
| true                                  |
+---------------------------------------+
+----------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tls_files') AS "UBIGINT")) |
+----------------------------------------------------------------------+
| true                                                                 |
+----------------------------------------------------------------------+
```

### WebSocket (`ws://`, `wss://`)

`ws://` and `wss://` are NNG transport schemes. They use
`ducknng_start_server(...)`, the full framed RPC surface, and the same
TLS handle model as `tls+tcp://`. They are not the HTTP carrier
described in `docs/http.md`.

``` sql
SELECT ducknng_start_server(
  'ws_demo', 'ws://127.0.0.1:45455/_ducknng', 1, 134217728, 300000, 0::UBIGINT
);
SELECT ok, type_name, name
FROM ducknng_decode_frame(
  ducknng_request_raw(
    'ws://127.0.0.1:45455/_ducknng',
    from_hex('01000000000000000000000000000000000000000000'),
    1000, 0::UBIGINT
  )
);
SELECT ducknng_stop_server('ws_demo');
+--------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('ws_demo', 'ws://127.0.0.1:45455/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+--------------------------------------------------------------------------------------------------------------+
| true                                                                                                         |
+--------------------------------------------------------------------------------------------------------------+
+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
+--------------------------------+
| ducknng_stop_server('ws_demo') |
+--------------------------------+
| true                           |
+--------------------------------+
```

## Framed RPC

### Registry: manifest and exec

Every server starts with a `manifest` method and nothing else. `exec` is
registered explicitly.

``` sql
SELECT ducknng_start_server(
  'rpc_demo', 'ipc:///tmp/ducknng_readme_rpc.ipc', 1, 134217728, 300000, 0
);
-- Default registry: manifest only.
SELECT name, family, response_mode, requires_auth, disabled
FROM ducknng_list_methods()
ORDER BY name;
-- Register exec (false = no auth required).
SELECT ducknng_register_exec_method(false) AS registered_exec;
-- Manifest now reports two methods.
WITH m AS (
  SELECT manifest
  FROM ducknng_get_rpc_manifest('ipc:///tmp/ducknng_readme_rpc.ipc', 0::UBIGINT)
  WHERE ok
)
SELECT json_extract_string(manifest::JSON, '$.server.name') AS server_name,
       json_array_length(json_extract(manifest::JSON, '$.methods')) AS method_count,
       position('"name":"exec"' IN manifest) > 0 AS has_exec
FROM m;
+------------------------------------------------------------------------------------------------+
| ducknng_start_server('rpc_demo', 'ipc:///tmp/ducknng_readme_rpc.ipc', 1, 134217728, 300000, 0) |
+------------------------------------------------------------------------------------------------+
| true                                                                                           |
+------------------------------------------------------------------------------------------------+
+---------------+---------+---------------+---------------+----------+
|     name      | family  | response_mode | requires_auth | disabled |
+---------------+---------+---------------+---------------+----------+
| cancel        | query   | metadata_only | false         | false    |
| close         | query   | metadata_only | false         | false    |
| fetch         | query   | rows          | false         | false    |
| handshake     | control | metadata_only | false         | false    |
| manifest      | control | metadata_only | false         | false    |
| query_open    | query   | session_open  | false         | false    |
| query_prepare | query   | rows          | false         | false    |
+---------------+---------+---------------+---------------+----------+
+-----------------+
| registered_exec |
+-----------------+
| true            |
+-----------------+
+-------------+--------------+----------+
| server_name | method_count | has_exec |
+-------------+--------------+----------+
| ducknng     | 8            | true     |
+-------------+--------------+----------+
```

### Synchronous helpers

``` sql
-- DDL statement over RPC.
SELECT * FROM ducknng_run_rpc(
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  'CREATE TABLE rpc_demo_t(i INTEGER)',
  0::UBIGINT
);
-- DML over RPC.
SELECT * FROM ducknng_run_rpc(
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  'INSERT INTO rpc_demo_t VALUES (10),(11),(12)',
  0::UBIGINT
);
-- Query rows over RPC — decoded as a table.
SELECT *
FROM ducknng_query_rpc(
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  'SELECT i, i > 10 AS gt_10 FROM rpc_demo_t ORDER BY i',
  0::UBIGINT
);
+------+-------+--------------+----------------+-------------+
|  ok  | error | rows_changed | statement_type | result_type |
+------+-------+--------------+----------------+-------------+
| true | NULL  | 0            | 7              | 2           |
+------+-------+--------------+----------------+-------------+
+------+-------+--------------+----------------+-------------+
|  ok  | error | rows_changed | statement_type | result_type |
+------+-------+--------------+----------------+-------------+
| true | NULL  | 3            | 2              | 1           |
+------+-------+--------------+----------------+-------------+
+----+-------+
| i  | gt_10 |
+----+-------+
| 10 | false |
| 11 | true  |
| 12 | true  |
+----+-------+
```

You can also request the Quack-derived batch serializer over the same
session contract. Unlike upstream
[Quack](https://github.com/duckdb/duckdb-quack), ducknng keeps transport
choice separate from row serialization, so the same
`ducknng_quack_batch` mode can ride over `http`, `tcp`, `ipc`, or `ws`.
Version and schema compatibility belong to the negotiated ducknng
session contract rather than to a suffix on the serializer token; full
Quack protocol traffic uses `application/vnd.duckdb` messages and
negotiates `quackVersion` during the connection exchange, but this fetch
payload is not wrapped in that Quack connection envelope.
`ducknng.fetch_batch_chunks` controls the default number of DuckDB
chunks requested per fetch; it defaults to 12 and applies to Arrow IPC
and `ducknng_quack_batch` alike unless the client sends an explicit
`batch_rows` hint.

``` sql
SELECT *
FROM ducknng_query_rpc_mode(
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  'SELECT i, i > 10 AS gt_10 FROM rpc_demo_t ORDER BY i',
  0::UBIGINT,
  'ducknng_quack_batch'
);
+----+-------+
| i  | gt_10 |
+----+-------+
| 10 | false |
| 11 | true  |
| 12 | true  |
+----+-------+
```

The Quack-derived mode also carries nested DuckDB values through the
same RPC/session lifecycle.

``` sql
SELECT st.a = 7 AS struct_ok,
       st.xs = [1, 2, 3]::INTEGER[] AS nested_list_ok
FROM ducknng_query_rpc_mode(
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  'SELECT {''a'': 7::INTEGER, ''xs'': [1,2,3]::INTEGER[]} AS st',
  0::UBIGINT,
  'ducknng_quack_batch'
);
+-----------+----------------+
| struct_ok | nested_list_ok |
+-----------+----------------+
| true      | true           |
+-----------+----------------+
```

Arrow IPC carries temporal, decimal, list, struct, union, large-string,
large-binary, fixed-size-binary, and duration values through the same
path. Supported mappings are documented in `docs/types.md`.

``` sql
SELECT d = DATE '2024-01-02' AS date_ok,
       ts = TIMESTAMP '2024-01-02 03:04:05.123456' AS ts_ok,
       dec = 123.45::DECIMAL(10,2) AS decimal_ok,
       xs[2] IS NULL AND xs[3] = 3 AS list_ok,
       st.a = 7 AND st.b = 'bee' AS struct_ok
FROM ducknng_query_rpc(
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  'SELECT DATE ''2024-01-02'' AS d,
          TIMESTAMP ''2024-01-02 03:04:05.123456'' AS ts,
          123.45::DECIMAL(10,2) AS dec,
          [1, NULL, 3]::INTEGER[] AS xs,
          {''a'': 7::INTEGER, ''b'': ''bee''} AS st',
  0::UBIGINT
);
+---------+-------+------------+---------+-----------+
| date_ok | ts_ok | decimal_ok | list_ok | struct_ok |
+---------+-------+------------+---------+-----------+
| true    | true  | true       | true    | true      |
+---------+-------+------------+---------+-----------+
```

`ducknng_parse_body(...)` decodes content-type-tagged BLOBs. The
built-in providers cover Arrow IPC, JSON, ducknng frames, the ducknng
`application/vnd.ducknng.quack-batch` batch media type, and a text/raw
fallback.

``` sql
SELECT provider, output FROM ducknng_list_codecs() ORDER BY provider;
SELECT a, b
FROM ducknng_parse_body(
  '[{"a":1,"b":"x"},{"a":2,"b":"y"}]'::BLOB,
  'application/json; charset=utf-8'
)
ORDER BY a;
+---------------------+-----------------------------+
|      provider       |           output            |
+---------------------+-----------------------------+
| arrow_ipc           | dynamic table               |
| csv                 | dynamic table               |
| ducknng_frame       | decoded frame columns       |
| ducknng_quack_batch | dynamic table               |
| form                | name VARCHAR, value VARCHAR |
| json                | dynamic table               |
| ndjson              | dynamic table               |
| parquet             | dynamic table               |
| raw                 | body BLOB                   |
| text                | body_text VARCHAR           |
| tsv                 | dynamic table               |
+---------------------+-----------------------------+
+---+---+
| a | b |
+---+---+
| 1 | x |
| 2 | y |
+---+---+
```

### Raw frame helpers

The raw scalar forms return reply bytes as BLOBs.
`ducknng_decode_frame(...)` projects all envelope fields plus the text
payload.

``` sql
-- Raw bytes from a socket-scoped request (show first 14 bytes as hex).
SET VARIABLE req2 = (ducknng_open_socket('req')).socket_id;
SET VARIABLE dialed2 = (ducknng_dial_socket(
  getvariable('req2')::UBIGINT,
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  1000, 0::UBIGINT
)).ok;
SELECT substr(
  hex(ducknng_request_socket_raw(
    getvariable('req2')::UBIGINT,
    from_hex('01000000000000000000000000000000000000000000'),
    1000
  )),
  1, 28
) AS first_14_bytes_hex;
-- Decode a manifest reply directly.
SELECT ok, version, type_name, name,
       position('"name":"exec"' IN payload_text) > 0 AS has_exec
FROM ducknng_decode_frame(
  ducknng_get_rpc_manifest_raw('ipc:///tmp/ducknng_readme_rpc.ipc', 0::UBIGINT)
);
SELECT (ducknng_close_socket(getvariable('req2')::UBIGINT)).ok AS closed;


+------------------------------+
|      first_14_bytes_hex      |
+------------------------------+
| 0102040000000800000000000000 |
+------------------------------+
+------+---------+-----------+----------+----------+
|  ok  | version | type_name |   name   | has_exec |
+------+---------+-----------+----------+----------+
| true | 1       | result    | manifest | true     |
+------+---------+-----------+----------+----------+
+--------+
| closed |
+--------+
| true   |
+--------+
```

### AIO helpers

`ducknng_request_raw_aio(...)` and
`ducknng_get_rpc_manifest_raw_aio(...)` return integer AIO handles
immediately. `ducknng_aio_collect(...)` blocks until all handles are
ready and returns one decoded-frame row per handle.
`ducknng_aio_wait(...)` waits without consuming — the handle stays
inspectable and droppable afterward. `ducknng_aio_collect_decoded(...)`
projects envelope columns directly.

``` sql
-- Launch manifest and exec requests in parallel.
SET VARIABLE aio_manifest = ducknng_get_rpc_manifest_raw_aio(
  'ipc:///tmp/ducknng_readme_rpc.ipc', 1000, 0::UBIGINT
);
SET VARIABLE aio_exec = ducknng_run_rpc_raw_aio(
  'ipc:///tmp/ducknng_readme_rpc.ipc',
  'CREATE TABLE IF NOT EXISTS rpc_aio_t(i INTEGER)',
  1000, 0::UBIGINT
);
SELECT getvariable('aio_manifest') > 0 AS manifest_launched,
       getvariable('aio_exec') > 0    AS exec_launched;
-- Collect both frames when ready.
CREATE TEMP TABLE aio_results AS
SELECT * FROM ducknng_aio_collect(
  list_value(getvariable('aio_manifest'), getvariable('aio_exec')), 1000
);
SET VARIABLE frame_manifest = (SELECT frame FROM aio_results WHERE aio_id = getvariable('aio_manifest'));
SET VARIABLE frame_exec     = (SELECT frame FROM aio_results WHERE aio_id = getvariable('aio_exec'));
SELECT ok, type_name, name,
       position('"name":"exec"' IN payload_text) > 0 AS has_exec
FROM ducknng_decode_frame(getvariable('frame_manifest'));
SELECT ok, type_name, name
FROM ducknng_decode_frame(getvariable('frame_exec'));
-- collect_decoded projects envelope columns directly without a second decode call.
SET VARIABLE aio_manifest2 = ducknng_get_rpc_manifest_raw_aio(
  'ipc:///tmp/ducknng_readme_rpc.ipc', 1000, 0::UBIGINT
);
SELECT aio_id, ok, frame_ok, type_name, name
FROM ducknng_aio_collect_decoded(list_value(getvariable('aio_manifest2')), 1000);
SELECT ducknng_aio_drop(getvariable('aio_manifest')) AND
       ducknng_aio_drop(getvariable('aio_exec')) AND
       ducknng_aio_drop(getvariable('aio_manifest2')) AS dropped;
DROP TABLE aio_results;
SELECT ducknng_stop_server('rpc_demo');


+-------------------+---------------+
| manifest_launched | exec_launched |
+-------------------+---------------+
| true              | true          |
+-------------------+---------------+



+------+-----------+----------+----------+
|  ok  | type_name |   name   | has_exec |
+------+-----------+----------+----------+
| true | result    | manifest | true     |
+------+-----------+----------+----------+
+------+-----------+------+
|  ok  | type_name | name |
+------+-----------+------+
| true | result    | exec |
+------+-----------+------+

+--------+------+----------+-----------+----------+
| aio_id |  ok  | frame_ok | type_name |   name   |
+--------+------+----------+-----------+----------+
| 6      | true | true     | result    | manifest |
+--------+------+----------+-----------+----------+
+---------+
| dropped |
+---------+
| true    |
+---------+

+---------------------------------+
| ducknng_stop_server('rpc_demo') |
+---------------------------------+
| true                            |
+---------------------------------+
```

## Query sessions

Query sessions decouple opening a query from fetching its result
batches. The server holds the cursor; the client drives the fetch loop
using `session_id` + `session_token`. The token is the bearer capability
that authorizes fetch, close, and cancel.

### High-level (open / fetch_table / close)

``` sql
SELECT ducknng_start_server(
  'session_demo', 'ipc:///tmp/ducknng_readme_session.ipc', 1, 134217728, 300000, 0
);
CREATE TEMP TABLE sess AS
SELECT *
FROM ducknng_open_query(
  'ipc:///tmp/ducknng_readme_session.ipc',
  'SELECT 1 AS id UNION ALL SELECT 2 AS id ORDER BY id',
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
SET VARIABLE sid   = (SELECT session_id    FROM sess);
SET VARIABLE stok  = (SELECT session_token FROM sess);
-- Decode first batch as rows directly.
SELECT * FROM ducknng_fetch_query_table(
  'ipc:///tmp/ducknng_readme_session.ipc',
  getvariable('sid')::UBIGINT,
  getvariable('stok')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
-- Next fetch returns the exhausted control reply.
SELECT ok, state, end_of_stream
FROM ducknng_fetch_query(
  'ipc:///tmp/ducknng_readme_session.ipc',
  getvariable('sid')::UBIGINT,
  getvariable('stok')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, 0::UBIGINT
);
SELECT ok, state
FROM ducknng_close_query(
  'ipc:///tmp/ducknng_readme_session.ipc',
  getvariable('sid')::UBIGINT,
  getvariable('stok')::VARCHAR,
  0::UBIGINT
);
DROP TABLE sess;
SELECT ducknng_stop_server('session_demo');
+--------------------------------------------------------------------------------------------------------+
| ducknng_start_server('session_demo', 'ipc:///tmp/ducknng_readme_session.ipc', 1, 134217728, 300000, 0) |
+--------------------------------------------------------------------------------------------------------+
| true                                                                                                   |
+--------------------------------------------------------------------------------------------------------+



+----+
| id |
+----+
| 1  |
| 2  |
+----+
+------+-----------+---------------+
|  ok  |   state   | end_of_stream |
+------+-----------+---------------+
| true | exhausted | true          |
+------+-----------+---------------+
+------+--------+
|  ok  | state  |
+------+--------+
| true | closed |
+------+--------+

+-------------------------------------+
| ducknng_stop_server('session_demo') |
+-------------------------------------+
| true                                |
+-------------------------------------+
```

### Raw frames (open_raw / fetch_raw / close_raw)

The raw helpers return the reply envelope as a BLOB.
`ducknng_frame_payload(...)` extracts the Arrow IPC bytes;
`ducknng_parse_body(...)` turns them into rows.

``` sql
SET VARIABLE session_raw_tls = ducknng_self_signed_tls_config('127.0.0.1', 365, 0);
SELECT ducknng_start_server(
  'session_raw', 'tls+tcp://127.0.0.1:0', 1, 134217728, 300000,
  getvariable('session_raw_tls')::UBIGINT
);
SET VARIABLE session_raw_url = (
  SELECT listen FROM ducknng_list_servers() WHERE name = 'session_raw'
);
SET VARIABLE raw_open = ducknng_open_query_raw(
  getvariable('session_raw_url'),
  'SELECT i, i * i AS sq FROM range(1, 4) AS t(i)',
  0::UBIGINT, 0::UBIGINT, getvariable('session_raw_tls')::UBIGINT
);
SET VARIABLE raw_sid = (
  SELECT json_extract(
    ducknng_frame_payload_text(getvariable('raw_open')::BLOB)::JSON,
    '$.session_id'
  )::UBIGINT
);
SET VARIABLE raw_tok = (
  SELECT json_extract_string(
    ducknng_frame_payload_text(getvariable('raw_open')::BLOB)::JSON,
    '$.session_token'
  )
);
SET VARIABLE raw_fetch = ducknng_fetch_query_raw(
  getvariable('session_raw_url'),
  getvariable('raw_sid')::UBIGINT,
  getvariable('raw_tok')::VARCHAR,
  0::UBIGINT, 0::UBIGINT, getvariable('session_raw_tls')::UBIGINT
);
SELECT ducknng_frame_type_name(getvariable('raw_fetch')::BLOB)      AS type_name,
       ducknng_frame_end_of_stream(getvariable('raw_fetch')::BLOB)  AS end_of_stream;
SELECT *
FROM ducknng_parse_body(
  ducknng_frame_payload(getvariable('raw_fetch')::BLOB),
  'application/vnd.apache.arrow.stream'
);
SET VARIABLE raw_close = ducknng_close_query_raw(
  getvariable('session_raw_url'),
  getvariable('raw_sid')::UBIGINT,
  getvariable('raw_tok')::VARCHAR,
  getvariable('session_raw_tls')::UBIGINT
);
SELECT position('"state":"closed"' IN
  ducknng_frame_payload_text(getvariable('raw_close')::BLOB)
) > 0 AS is_closed;
SELECT ducknng_stop_server('session_raw');
SELECT ducknng_drop_tls_config(getvariable('session_raw_tls')::UBIGINT);

+---------------------------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('session_raw', 'tls+tcp://127.0.0.1:0', 1, 134217728, 300000, CAST(getvariable('session_raw_tls') AS "UBIGINT")) |
+---------------------------------------------------------------------------------------------------------------------------------------+
| true                                                                                                                                  |
+---------------------------------------------------------------------------------------------------------------------------------------+





+-----------+---------------+
| type_name | end_of_stream |
+-----------+---------------+
| result    | false         |
+-----------+---------------+
+---+----+
| i | sq |
+---+----+
| 1 | 1  |
| 2 | 4  |
| 3 | 9  |
+---+----+

+-----------+
| is_closed |
+-----------+
| true      |
+-----------+
+------------------------------------+
| ducknng_stop_server('session_raw') |
+------------------------------------+
| true                               |
+------------------------------------+
+----------------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('session_raw_tls') AS "UBIGINT")) |
+----------------------------------------------------------------------------+
| true                                                                       |
+----------------------------------------------------------------------------+
```

### Async (open_aio / fetch_aio / close_aio / cancel_aio)

Each session lifecycle step has a `_raw_aio` variant. The AIO handle is
collected through `ducknng_aio_collect(...)` just like any other AIO
handle.

``` sql
SELECT ducknng_start_server(
  'session_aio', 'ipc:///tmp/ducknng_readme_session_aio.ipc', 1, 134217728, 300000, 0
);
SET VARIABLE open_aio = ducknng_open_query_raw_aio(
  'ipc:///tmp/ducknng_readme_session_aio.ipc',
  'SELECT 99 AS id',
  0::UBIGINT, 0::UBIGINT, 1000, 0::UBIGINT
);
CREATE TEMP TABLE open_collect AS
SELECT * FROM ducknng_aio_collect(list_value(getvariable('open_aio')::UBIGINT), 1000);
SET VARIABLE open_frame = (SELECT frame FROM open_collect);
SET VARIABLE aio_sid = (
  SELECT json_extract(payload_text::JSON, '$.session_id')::UBIGINT
  FROM ducknng_decode_frame(getvariable('open_frame'))
);
SET VARIABLE aio_tok = (
  SELECT json_extract_string(payload_text::JSON, '$.session_token')
  FROM ducknng_decode_frame(getvariable('open_frame'))
);
SET VARIABLE close_aio = ducknng_close_query_raw_aio(
  'ipc:///tmp/ducknng_readme_session_aio.ipc',
  getvariable('aio_sid')::UBIGINT,
  getvariable('aio_tok')::VARCHAR,
  1000, 0::UBIGINT
);
CREATE TEMP TABLE close_collect AS
SELECT * FROM ducknng_aio_collect(list_value(getvariable('close_aio')::UBIGINT), 1000);
SET VARIABLE close_frame = (SELECT frame FROM close_collect);
SELECT ok, type_name, name,
       position('"state":"closed"' IN payload_text) > 0 AS is_closed
FROM ducknng_decode_frame(getvariable('close_frame'));
SELECT ducknng_aio_drop(getvariable('open_aio')::UBIGINT) AND
       ducknng_aio_drop(getvariable('close_aio')::UBIGINT) AS dropped;
DROP TABLE open_collect; DROP TABLE close_collect;
SELECT ducknng_stop_server('session_aio');
+-----------------------------------------------------------------------------------------------------------+
| ducknng_start_server('session_aio', 'ipc:///tmp/ducknng_readme_session_aio.ipc', 1, 134217728, 300000, 0) |
+-----------------------------------------------------------------------------------------------------------+
| true                                                                                                      |
+-----------------------------------------------------------------------------------------------------------+








+------+-----------+-------+-----------+
|  ok  | type_name | name  | is_closed |
+------+-----------+-------+-----------+
| true | result    | close | true      |
+------+-----------+-------+-----------+
+---------+
| dropped |
+---------+
| true    |
+---------+

+------------------------------------+
| ducknng_stop_server('session_aio') |
+------------------------------------+
| true                               |
+------------------------------------+
```

## HTTP carrier

### `ducknng_ncurl` — HTTP client primitive

`ducknng_ncurl(url, method, headers_json, body, timeout_ms, tls_config_id)`
is the low-level HTTP/HTTPS transport primitive. A local `nanonext`
HTTPS server is started during README rendering for the TLS example; the
server definition is shown because it is part of the example rather than
hidden setup.

``` r
library(nanonext)

cert <- write_cert(cn = '127.0.0.1')
writeLines(cert$client[[1]], "/tmp/ducknng_readme_http_demo_ca.pem")

server <- http_server(
  url = 'https://127.0.0.1:18443',
  handlers = list(
    handler('/hello', function(req) {
      list(status = 200L,
           headers = c('Content-Type' = 'text/plain', 'X-Test' = 'hello'),
           body = 'hello from nanonext https server')
    }),
    handler('/echo', function(req) {
      list(status = 200L,
           headers = c('Content-Type' = (
             req$headers[['Content-Type']] %||% 'application/octet-stream'),
             'X-Test' = 'echo'),
           body = req$body)
    }, method = 'POST')
  ),
  tls = tls_config(server = cert$server)
)
```

``` sql
-- Register a client TLS handle that trusts the self-signed demo CA.
SET VARIABLE tls_http = ducknng_tls_config_from_files(
  NULL, '/tmp/ducknng_readme_http_demo_ca.pem', NULL,
  2  -- auth_mode = require certificate validation
);
-- GET request over HTTPS.
SELECT ok, status, error, body_text
FROM ducknng_ncurl(
  'https://127.0.0.1:18443/hello',
  NULL, NULL, NULL, 2000,
  getvariable('tls_http')::UBIGINT
);
-- POST with raw bytes; inspect echoed body and response headers.
SET VARIABLE echo_hdrs = '[{"name":"Content-Type","value":"application/octet-stream"}]';
SELECT ok, status, hex(body) AS body_hex,
       position('X-Test' IN headers_json) > 0 AS has_x_test
FROM ducknng_ncurl(
  'https://127.0.0.1:18443/echo',
  'POST',
  getvariable('echo_hdrs'),
  from_hex('01020304'),
  2000,
  getvariable('tls_http')::UBIGINT
);
SELECT ducknng_drop_tls_config(getvariable('tls_http')::UBIGINT);

+------+--------+-------+----------------------------------+
|  ok  | status | error |            body_text             |
+------+--------+-------+----------------------------------+
| true | 200    | NULL  | hello from nanonext https server |
+------+--------+-------+----------------------------------+

+------+--------+----------+------------+
|  ok  | status | body_hex | has_x_test |
+------+--------+----------+------------+
| true | 200    | 01020304 | true       |
+------+--------+----------+------------+
+---------------------------------------------------------------------+
| ducknng_drop_tls_config(CAST(getvariable('tls_http') AS "UBIGINT")) |
+---------------------------------------------------------------------+
| true                                                                |
+---------------------------------------------------------------------+
```

`ducknng_ncurl_aio(...)` is the async form; `ducknng_ncurl_table(...)`
parses the response body through the codec layer and returns a table
directly.

### `ducknng_start_server` on `http://` and `https://`

Pointing `ducknng_start_server(...)` at an `http://` or `https://` URL
mounts the framed RPC surface at the URL path. The higher-level
synchronous request, RPC, and session helpers switch carriers
automatically from the scheme; `contexts = 1` is required for the HTTP
carrier.

``` sql
SELECT ducknng_start_server(
  'http_rpc', 'http://127.0.0.1:18444/_ducknng', 1, 134217728, 300000, 0::UBIGINT
);
WITH m AS (
  SELECT manifest
  FROM ducknng_get_rpc_manifest('http://127.0.0.1:18444/_ducknng', 0::UBIGINT)
  WHERE ok
)
SELECT json_extract_string(manifest::JSON, '$.server.name') AS server_name,
       json_array_length(json_extract(manifest::JSON, '$.methods')) AS method_count
FROM m;
-- ducknng_ncurl can call the framed RPC path directly too.
SET VARIABLE http_frame = (
  SELECT body FROM ducknng_ncurl(
    'http://127.0.0.1:18444/_ducknng', 'POST',
    '[{"name":"Content-Type","value":"application/vnd.ducknng.frame"}]',
    from_hex('01000000000000000000000000000000000000000000'),
    2000, 0::UBIGINT
  )
);
SELECT ok, type_name, name FROM ducknng_decode_frame(getvariable('http_frame'));
SELECT ducknng_stop_server('http_rpc');
+-----------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('http_rpc', 'http://127.0.0.1:18444/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+-----------------------------------------------------------------------------------------------------------------+
| true                                                                                                            |
+-----------------------------------------------------------------------------------------------------------------+
+-------------+--------------+
| server_name | method_count |
+-------------+--------------+
| ducknng     | 8            |
+-------------+--------------+

+------+-----------+----------+
|  ok  | type_name |   name   |
+------+-----------+----------+
| true | result    | manifest |
+------+-----------+----------+
+---------------------------------+
| ducknng_stop_server('http_rpc') |
+---------------------------------+
| true                            |
+---------------------------------+
```

### Custom HTTP routes

HTTP and HTTPS services can register additional exact, prefix, or
template routes beside the framed RPC mount. Route handlers are ordinary
SQL queries that return one response row and can inspect the in-flight
request through `ducknng_http_request()` and
`ducknng_http_request_body()`.

``` sql
SELECT ducknng_start_server(
  'http_routes', 'http://127.0.0.1:18445/_ducknng', 1, 134217728, 300000, 0::UBIGINT
);
SELECT ducknng_register_http_route(
  'http_routes', 'GET', '/healthz',
  'SELECT * FROM ducknng_http_text(200, ''ok'')'
);
SELECT ducknng_register_http_route(
  'http_routes', 'POST', '/echo',
  'SELECT * FROM ducknng_http_text(
     201,
     (SELECT method || '' '' || path || '' x='' ||
             coalesce(ducknng_http_query_param(''x''), '''') ||
             '' body='' || coalesce(body_text, '''')
      FROM ducknng_http_request(), ducknng_http_request_body()))'
);
SELECT ducknng_register_http_route_pattern(
  'http_routes', 'GET', 'template',
  '/tenant/{tenant_id}/items/{item_id}',
  'SELECT * FROM ducknng_http_text(
     200,
     (SELECT ducknng_http_path_param(''tenant_id'') || '':'' ||
             ducknng_http_path_param(''item_id'')
      FROM ducknng_http_request()))'
);
+--------------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('http_routes', 'http://127.0.0.1:18445/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+--------------------------------------------------------------------------------------------------------------------+
| true                                                                                                               |
+--------------------------------------------------------------------------------------------------------------------+
+---------------------------------------------------------------------------------------------------------------+
| ducknng_register_http_route('http_routes', 'GET', '/healthz', 'SELECT * FROM ducknng_http_text(200, ''ok'')') |
+---------------------------------------------------------------------------------------------------------------+
| true                                                                                                          |
+---------------------------------------------------------------------------------------------------------------+
+--------------------------------------------------------------------+
| ducknng_register_http_route('http_routes', 'POST', '/echo', 'SELECT * FROM ducknng_http_text(
     201,
     (SELECT method || '' '' || path || '' x='' ||
             coalesce(ducknng_http_query_param(''x''), '''') ||
             '' body='' || coalesce(body_text, '''')
      FROM ducknng_http_request(), ducknng_http_request_body()))') |
+--------------------------------------------------------------------+
| true                                                               |
+--------------------------------------------------------------------+
+---------------------------------------+
| ducknng_register_http_route_pattern('http_routes', 'GET', 'template', '/tenant/{tenant_id}/items/{item_id}', 'SELECT * FROM ducknng_http_text(
     200,
     (SELECT ducknng_http_path_param(''tenant_id'') || '':'' ||
             ducknng_http_path_param(''item_id'')
      FROM ducknng_http_request()))') |
+---------------------------------------+
| true                                  |
+---------------------------------------+
```

``` sql
SELECT ok, status, body_text
FROM ducknng_ncurl('http://127.0.0.1:18445/healthz', 'GET', NULL, NULL, 2000, 0::UBIGINT);
SELECT ok, status, body_text
FROM ducknng_ncurl(
  'http://127.0.0.1:18445/echo?x=1', 'POST',
  '[{"name":"Content-Type","value":"text/plain"}]',
  'hello'::BLOB, 2000, 0::UBIGINT
);
SELECT ok, status, body_text
FROM ducknng_ncurl(
  'http://127.0.0.1:18445/tenant/alice/items/42', 'GET', NULL, NULL, 2000, 0::UBIGINT
);
SELECT service_name, method, match_kind, path
FROM ducknng_list_http_routes()
WHERE service_name = 'http_routes'
ORDER BY match_kind, path;
SELECT ducknng_stop_server('http_routes');
+------+--------+-----------+
|  ok  | status | body_text |
+------+--------+-----------+
| true | 200    | ok        |
+------+--------+-----------+
+------+--------+---------------------------+
|  ok  | status |         body_text         |
+------+--------+---------------------------+
| true | 201    | POST /echo x=1 body=hello |
+------+--------+---------------------------+
+------+--------+-----------+
|  ok  | status | body_text |
+------+--------+-----------+
| true | 200    | alice:42  |
+------+--------+-----------+
+--------------+--------+------------+-------------------------------------+
| service_name | method | match_kind |                path                 |
+--------------+--------+------------+-------------------------------------+
| http_routes  | POST   | exact      | /echo                               |
| http_routes  | GET    | exact      | /healthz                            |
| http_routes  | GET    | template   | /tenant/{tenant_id}/items/{item_id} |
+--------------+--------+------------+-------------------------------------+
+------------------------------------+
| ducknng_stop_server('http_routes') |
+------------------------------------+
| true                               |
+------------------------------------+
```

Route handlers run on the `shared_serialized_connection` lane by
default. Switch to `service_serialized_connection` or
`request_connection` with `ducknng_set_service_execution_model(...)`
before traffic arrives. The subscriber gateway walkthrough in
`demo/subscriber_gateway.Rmd` (and `docs/subscriber_gateway_demo.md`)
shows all three services sharing one DuckDB runtime without deadlocking
because each uses `service_serialized_connection`.

The runtime execution-connection pool opens a warm minimum and grows on
demand up to a deterministic ceiling. Use the SQL controls below when a
workload intentionally holds many query sessions or nested `query_rpc`
scans at once.

``` sql
SELECT ducknng_execution_pool_max() AS current_pool_max;
SELECT ducknng_set_execution_pool_max(96) AS effective_pool_max;
SELECT ducknng_execution_pool_max() AS updated_pool_max;
SELECT ducknng_set_execution_pool_max(64) AS reset_pool_max;
+------------------+
| current_pool_max |
+------------------+
| 64               |
+------------------+
+--------------------+
| effective_pool_max |
+--------------------+
| 96                 |
+--------------------+
+------------------+
| updated_pool_max |
+------------------+
| 96               |
+------------------+
+----------------+
| reset_pool_max |
+----------------+
| 64             |
+----------------+
```

### Chunked streaming routes (SSE)

`ducknng_add_stream_route` registers a route that responds with HTTP
chunked transfer encoding. The SQL query must return a `chunk` column;
each non-null row is written to the client as a separate chunk before
the terminator. `ducknng_format_sse(data, event, id, retry)` formats a
single Server-Sent Events event string.

``` sql
SELECT ducknng_start_server(
  'http_sse', 'http://127.0.0.1:18446/_ducknng', 1, 134217728, 300000, 0::UBIGINT
);
SELECT ducknng_add_stream_route(
  'http_sse', 'GET', '/events',
  'SELECT ducknng_format_sse(''row '' || i::VARCHAR) AS chunk
   FROM generate_series(1, 3) t(i)'
);
SELECT is_stream, stream_content_type
FROM ducknng_list_http_routes()
WHERE service_name = 'http_sse' AND path = '/events';
+-----------------------------------------------------------------------------------------------------------------+
| ducknng_start_server('http_sse', 'http://127.0.0.1:18446/_ducknng', 1, 134217728, 300000, CAST(0 AS "UBIGINT")) |
+-----------------------------------------------------------------------------------------------------------------+
| true                                                                                                            |
+-----------------------------------------------------------------------------------------------------------------+
+--------------------------------------+
| ducknng_add_stream_route('http_sse', 'GET', '/events', 'SELECT ducknng_format_sse(''row '' || i::VARCHAR) AS chunk
   FROM generate_series(1, 3) t(i)') |
+--------------------------------------+
| true                                 |
+--------------------------------------+
+-----------+----------------------------------+
| is_stream |       stream_content_type        |
+-----------+----------------------------------+
| true      | text/event-stream; charset=utf-8 |
+-----------+----------------------------------+
```

`ducknng_ncurl` transparently reassembles the chunked response. The body
is the concatenation of all SSE events written by the server.

``` sql
SELECT ok, status, headers_json, body_text
FROM ducknng_ncurl('http://127.0.0.1:18446/events', 'GET', NULL, NULL, 2000, 0::UBIGINT);
SELECT ducknng_stop_server('http_sse');
+------+--------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
|  ok  | status |                                                                          headers_json                                                                           | body_text |
+------+--------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| true | 200    | [{"name":"Transfer-Encoding","value":"chunked"},{"name":"Content-Type","value":"text/event-stream; charset=utf-8"},{"name":"Cache-Control","value":"no-cache"}] | data: row 1

data: row 2

data: row 3

          |
+------+--------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
+---------------------------------+
| ducknng_stop_server('http_sse') |
+---------------------------------+
| true                            |
+---------------------------------+
```

## Client interop: nanonext REQ/REP

This example shows a `nanonext` R client talking to a `ducknng` server
using the versioned wire envelope directly. It covers frame encoding and
decoding at the R level so the wire contract is explicit.

### Frame helpers

``` r
write_u32le <- function(x) writeBin(as.integer(x), raw(), size = 4L, endian = "little")
write_u64le <- function(x) {
  x <- as.double(x)
  c(write_u32le(x %% 2^32), write_u32le(floor(x / 2^32)))
}
read_u32le_bytes <- function(x) sum(as.double(as.integer(x)) * 256^(0:3))
read_u64le_bytes <- function(x) read_u32le_bytes(x[1:4]) + 2^32 * read_u32le_bytes(x[5:8])
read_u32le <- function(buf, offset) read_u32le_bytes(buf[offset + 0:3])
read_u64le <- function(buf, offset) read_u64le_bytes(buf[offset + 0:7])

encode_ducknng_exec_payload <- function(sql, want_result = FALSE) {
  raw_con <- rawConnection(raw(), open = "r+")
  on.exit(close(raw_con), add = TRUE)
  nanoarrow::write_nanoarrow(data.frame(sql = sql, want_result = want_result), raw_con)
  rawConnectionValue(raw_con)
}

encode_ducknng_call_frame <- function(method, payload, flags = 0L) {
  m <- charToRaw(method)
  c(as.raw(1L), as.raw(1L), write_u32le(flags),
    write_u32le(length(m)), write_u32le(0),
    write_u64le(length(payload)), m, payload)
}

encode_ducknng_exec_request <- function(sql, want_result = FALSE) {
  encode_ducknng_call_frame("exec", encode_ducknng_exec_payload(sql, want_result))
}

decode_ducknng_exec_reply <- function(buf) {
  if (!is.raw(buf) || length(buf) < 22L)
    stop("ducknng reply frame was empty or truncated", call. = FALSE)
  name_len    <- read_u32le(buf, 7)
  error_len   <- read_u32le(buf, 11)
  payload_len <- read_u64le(buf, 15)
  ns <- 23L; es <- ns + name_len; ps <- es + error_len
  payload <- if (payload_len > 0) buf[ps:(ps + payload_len - 1L)] else raw()
  list(
    version = as.integer(buf[1]), type = as.integer(buf[2]),
    method  = rawToChar(buf[ns:(es - 1L)]),
    error   = if (error_len > 0) rawToChar(buf[es:(ps - 1L)]) else "",
    data    = if (payload_len > 0) as.data.frame(nanoarrow::read_nanoarrow(payload)) else NULL
  )
}
```

### Start a server, dial a REQ socket, run remote SQL

``` r
ext_path <- normalizePath("build/release/ducknng.duckdb_extension")
ipc_path <- "/tmp/ducknng_readme_exec.ipc"
ipc_url  <- paste0("ipc://", ipc_path)
unlink(ipc_path)

db_con <- DBI::dbConnect(duckdb::duckdb(config = list(allow_unsigned_extensions = "true", allow_extensions_metadata_mismatch = "true")))
DBI::dbExecute(db_con, sprintf("LOAD '%s'", ext_path))
[1] 0
DBI::dbGetQuery(db_con, sprintf(
  "SELECT ducknng_start_server('sql_exec', '%s', 1, 134217728, 300000, 0::UBIGINT)", ipc_url
))
  ducknng_start_server('sql_exec', 'ipc:///tmp/ducknng_readme_exec.ipc', 1, 134217728, 300000, CAST(0 AS "UBIGINT"))
1                                                                                                               TRUE
DBI::dbGetQuery(db_con, "SELECT ducknng_register_exec_method()")
  ducknng_register_exec_method()
1                           TRUE

deadline <- Sys.time() + 5
while (!file.exists(ipc_path) && Sys.time() < deadline) Sys.sleep(0.1)
req <- nanonext::socket("req", dial = ipc_url)
```

``` r
send_recv <- function(sock, frame, timeout_ms = 1000L) {
  nanonext::send(sock, frame, mode = "raw", block = timeout_ms)
  decode_ducknng_exec_reply(nanonext::recv(sock, mode = "raw", block = timeout_ms))
}

# DDL.
send_recv(req, encode_ducknng_exec_request(
  "CREATE TABLE exec_demo(i INTEGER)"
))$data
  rows_changed statement_type result_type
1            0              7           2

# DML.
send_recv(req, encode_ducknng_exec_request(
  "INSERT INTO exec_demo VALUES (42),(99)"
))$data
  rows_changed statement_type result_type
1            2              2           1

# Query with want_result = TRUE returns Arrow-decoded rows.
send_recv(req, encode_ducknng_exec_request(
  "SELECT i, i > 50 AS gt_50 FROM exec_demo ORDER BY i",
  want_result = TRUE
))$data
   i gt_50
1 42 FALSE
2 99  TRUE
```

The same table is visible locally through the DuckDB connection that
hosts the server:

``` r
DBI::dbGetQuery(db_con, "SELECT * FROM exec_demo ORDER BY i")
   i
1 42
2 99
```

``` r
DBI::dbGetQuery(db_con, "SELECT ducknng_stop_server('sql_exec')")
  ducknng_stop_server('sql_exec')
1                            TRUE
close(req)
DBI::dbDisconnect(db_con)
unlink(ipc_path)
```

## Deployment and admission policy

`ducknng` defaults to default-open transport behavior. The application
or deployment decides where the trust boundary is. For anything beyond
process-local, filesystem-local, or loopback-only development, set the
boundary deliberately before exposing a service.

A strong direct-exposure baseline:

1.  Use `https://`, `wss://`, or `tls+tcp://` rather than plaintext
    network carriers.
2.  Create the TLS handle with `auth_mode = 2` so peers must present a
    verified client certificate.
3.  Configure a private CA or otherwise narrow certificate issuance so
    the trust root is not broader than the application.
4.  Set exact peer-identity and/or IP/CIDR allowlists before exposure
    with `ducknng_set_tls_peer_allowlist(...)` and the optional
    `ip_allowlist_json` startup argument.
5.  Set basic service resource limits with
    `ducknng_set_service_limits(name, max_open_sessions[, ...])` before
    exposing query sessions.
6.  Install a service-level SQL authorizer with
    `ducknng_set_service_authorizer(...)` when admission depends on
    deployment tables, tenants, method names, or HTTP metadata.
7.  Rotate policy dynamically with
    `ducknng_set_service_peer_allowlist(...)`,
    `ducknng_set_service_ip_allowlist(...)`,
    `ducknng_set_service_limits(...)`, and
    `ducknng_set_service_authorizer(...)`.

`exec` and query sessions run DuckDB SQL as the service process. Do not
expose them to callers who are not already authorized to run that SQL.
Profiles by use case:

- **Local development / single-user IPC**: `inproc://`, loopback, or
  `ipc://` under a private directory; `exec` may be enabled for
  convenience.
- **Trusted service mesh**: `tls+tcp://`, `wss://`, or `https://` with
  mTLS, exact peer allowlists, IP/CIDR allowlists, and service limits.
- **Shared or semi-trusted clients**: prefer query sessions plus a short
  SQL authorizer; use deployment-level DuckDB/process controls for
  filesystem, extension loading, outbound network, and attachments.
- **Public/untrusted internet**: do not expose raw DuckDB SQL directly.
  Put `ducknng` behind a gateway that authenticates, rate-limits, and
  maps users to fixed application operations.

``` sql
-- Set a peer allowlist on the TLS handle before starting services with it.
SELECT ducknng_set_tls_peer_allowlist(
  1::UBIGINT,
  '["tls:san:client-a.example","tls:san:client-b.example"]'
);

-- Start with an IP/CIDR allowlist installed before the listener accepts traffic.
SELECT ducknng_start_server(
  'sql_http', 'http://127.0.0.1:18444/_ducknng',
  1, 134217728, 300000, 0::UBIGINT,
  '["127.0.0.1/32"]'
);

-- Set a basic resource limit for query sessions.
SELECT ducknng_set_service_limits('sql_http', 16::UBIGINT);

-- Add a flexible SQL callback for policy that depends on request context.
SELECT ducknng_set_service_authorizer(
  'sql_http',
  'SELECT remote_ip = ''127.0.0.1'' AND rpc_method = ''manifest'' AS allow,
          403 AS status, ''not authorized'' AS reason
   FROM ducknng_auth_context()'
);
```

Transport/carrier scheme reference:

| Surface                         | Accepted schemes                                                                        | TLS handle accepted on             |
|---------------------------------|-----------------------------------------------------------------------------------------|------------------------------------|
| `ducknng_start_server(...)`     | `inproc://`, `ipc://`, `tcp://`, `tls+tcp://`, `ws://`, `wss://`, `http://`, `https://` | `tls+tcp://`, `wss://`, `https://` |
| synchronous RPC/session helpers | NNG schemes + `http://`, `https://`                                                     | `tls+tcp://`, `wss://`, `https://` |
| raw RPC AIO helpers             | NNG schemes + `http://`, `https://`                                                     | `tls+tcp://`, `wss://`, `https://` |
| generic socket APIs             | `inproc://`, `ipc://`, `tcp://`, `tls+tcp://`, `ws://`, `wss://`                        | `tls+tcp://`, `wss://`             |
| HTTP client helpers             | `http://`, `https://`                                                                   | `https://`                         |

Supplying a non-zero TLS handle on a non-TLS URL is rejected rather than
silently ignored.

For deeper write-ups see `docs/protocol.md`, `docs/manifest.md`,
`docs/security.md`, `docs/registry.md`, `docs/transports.md`, and
`docs/http.md`. `NEWS.md` summarizes landed changes.

## Introspection examples

### DuckDB log entries

`ducknng_log_entries()` returns a snapshot of the most recent DuckDB log
entries captured by the extension’s in-memory ring buffer (capacity
512). The ring is populated via the DuckDB logger API. Log capture is
not active by default; call `ducknng_enable_log_capture()` once to
register the ring with DuckDB’s logger before querying the ring. Note:
`ducknng_enable_log_capture()` is unsafe to call under DuckDB v1.5.2 due
to a crash in the logger registration API; the example below is shown
but not executed.

``` sql
SELECT ducknng_enable_log_capture();

SELECT ts, level, log_type, LEFT(message, 80) AS message
FROM ducknng_log_entries()
ORDER BY ts
LIMIT 5;
```

### NNG statistics

`ducknng_nng_stats()` exposes NNG’s process-wide statistics tree as
rows. It is a low-level observability primitive: filter by statistic
name, unit, or scope to inspect socket counters without making NNG types
part of the public RPC method surface.

``` sql
SELECT name, unit, count(*) AS rows
FROM ducknng_nng_stats()
WHERE name IN ('tx_bytes', 'rx_bytes', 'tx_msgs', 'rx_msgs')
GROUP BY name, unit
ORDER BY name, unit;
+----------+----------+------+
|   name   |   unit   | rows |
+----------+----------+------+
| rx_bytes | bytes    | 1    |
| rx_msgs  | messages | 1    |
| tx_bytes | bytes    | 1    |
| tx_msgs  | messages | 1    |
+----------+----------+------+
```

### Pipe-event monitor

Each running service maintains a bounded ring of NNG pipe events
(ADD_PRE, ADD_POST, REM_POST).
`ducknng_read_monitor(name, after_seq, max_events)` pages through the
ring. `ducknng_monitor_status(name)` returns ring and active-pipe
counters.

``` sql
SELECT ducknng_start_server('monitor_demo', 'tcp://127.0.0.1:0', 1, 134217728, 30000, 0::UBIGINT);
SET VARIABLE monitor_url = (SELECT listen FROM ducknng_list_servers() WHERE name = 'monitor_demo');
+------------------------------------------------------------------------------------------------------+
| ducknng_start_server('monitor_demo', 'tcp://127.0.0.1:0', 1, 134217728, 30000, CAST(0 AS "UBIGINT")) |
+------------------------------------------------------------------------------------------------------+
| true                                                                                                 |
+------------------------------------------------------------------------------------------------------+
```

``` sql
-- Dial a REQ socket to generate pipe-connect events.
SET VARIABLE monitor_req = (ducknng_open_socket('req')).socket_id;
SELECT (ducknng_dial_socket(getvariable('monitor_req')::UBIGINT, getvariable('monitor_url'), 0, 0::UBIGINT)).ok AS dialed;

+--------+
| dialed |
+--------+
| true   |
+--------+
```

``` sql
SELECT event_capacity, event_count, oldest_seq, newest_seq,
       dropped_events, active_pipes, max_active_pipes
FROM ducknng_monitor_status('monitor_demo');
+----------------+-------------+------------+------------+----------------+--------------+------------------+
| event_capacity | event_count | oldest_seq | newest_seq | dropped_events | active_pipes | max_active_pipes |
+----------------+-------------+------------+------------+----------------+--------------+------------------+
| 1024           | 2           | 1          | 2          | 0              | 1            | 0                |
+----------------+-------------+------------+------------+----------------+--------------+------------------+
```

`ducknng_list_pipes(name)` lists currently open NNG pipes for a service.

``` sql
SELECT pipe_id, opened_ms, remote_addr, peer_identity
FROM ducknng_list_pipes('monitor_demo');
+------------+---------------+-----------------+---------------+
|  pipe_id   |   opened_ms   |   remote_addr   | peer_identity |
+------------+---------------+-----------------+---------------+
| 1340834191 | 1783177957954 | 127.0.0.1:36520 | NULL          |
+------------+---------------+-----------------+---------------+
```

``` sql
SELECT ducknng_close_socket(getvariable('monitor_req')::UBIGINT);
SELECT ducknng_stop_server('monitor_demo');
+-------------------------------------------------------------------------------------------------------------------------+
|                           ducknng_close_socket(CAST(getvariable('monitor_req') AS "UBIGINT"))                           |
+-------------------------------------------------------------------------------------------------------------------------+
| {'ok': true, 'error': NULL, 'nng_error': NULL, 'nng_error_message': NULL, 'socket_id': 5, 'payload': NULL, 'url': NULL} |
+-------------------------------------------------------------------------------------------------------------------------+
+-------------------------------------+
| ducknng_stop_server('monitor_demo') |
+-------------------------------------+
| true                                |
+-------------------------------------+
```

Client sockets can also opt into their own pipe-event ring. Enable
monitoring before the socket listens or dials if you need to capture the
first add event.

``` sql
SELECT ducknng_monitor_socket(getvariable('socket_mon_a')::UBIGINT) AS monitoring_enabled;
+--------------------+
| monitoring_enabled |
+--------------------+
| true               |
+--------------------+
```

``` sql
SELECT event, count(*) AS events
FROM ducknng_read_socket_monitor(getvariable('socket_mon_a')::UBIGINT,
                                 0::UBIGINT,
                                 0::UBIGINT)
GROUP BY event
ORDER BY event;
SELECT ducknng_socket_monitor_wait(getvariable('socket_mon_a')::UBIGINT,
                                   0::UBIGINT,
                                   1000::UBIGINT) >= 1 AS saw_event;
+-------+--------+
| event | events |
+-------+--------+
| add   | 1      |
+-------+--------+
+-----------+
| saw_event |
+-----------+
| true      |
+-----------+
```

## Browser and WebAssembly

ducknng builds as a duckdb-wasm side module, but a browser sandbox
cannot offer raw sockets, so the transport story is bounded by what the
platform allows rather than by the extension. The release-supported
browser lane is the `wasm_eh` duckdb-wasm runtime: extension load and
scalar SQL, browser HTTP(S) client calls through the Emscripten-only
synchronous-XHR bridge, terminal HTTP AIO handles, table parsing,
HTTPS/CORS, and framed HTTP RPC/session helper routing. Raw `tcp://`,
`ipc://`, native POSIX-style `tls+tcp://`, and browser WebSocket
adapters are not supported.

The browser checks are not run during README rendering because they
require Docker, a staged duckdb-wasm site, and real Chromium. They are
still literate, executable chunks; run them from the repository root
when validating a wasm/browser change.

``` bash
DUCKDB_WASM_PLATFORM=wasm_eh DUCKNNG_WASM_SERVE=0 \
  scripts/start_duckdb_wasm_local_test.sh
```

``` bash
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site \
  --probes=load,inproc,http-sync,http-aio,http-table,https-cors,http-rpc
```

The threaded runtime remains diagnostic rather than release-blocking. It
can build and individual HTTP probes can pass, but repeated headless
runs still expose extension-load, `inproc://` progress, and DuckDB-wasm
JSON-autoload instability.

``` bash
DUCKDB_WASM_PLATFORM=wasm_threads DUCKNNG_WASM_SERVE=0 \
  scripts/start_duckdb_wasm_local_test.sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site \
  --probes=load,http-sync,http-aio,http-table,https-cors,http-rpc
```

Self-published unsigned release assets are built by the dedicated
release workflow. Push release tags one at a time using
`v<ducknng-version>-duckdb<duckdb-version>` so GitHub runs the binary
matrix for each pinned DuckDB runtime.

``` bash
gh workflow run ducknng-release-binaries.yml --ref main
gh run list --workflow "ducknng release binaries" --limit 5
```

The browser transport matrix, the COOP/COEP requirements for the
threaded runtime, and the smoke harness are tracked in `docs/wasm.md`,
`docs/wasm_browser_transport_checklist.md`, and
`test/browser/README.md`.

## Status

Version 0.1.0. Every DuckDB chunk shown as runnable in this README is
implemented, tested, and runs at render time. Browser/WebAssembly and
release-matrix checks require external runtimes, so their exact commands
are shown above and covered by the Playwright and GitHub Actions
harnesses. The extension builds against DuckDB v1.5.2 using the C API
only.

## References

- [NNG](https://nng.nanomsg.org/) — underlying messaging library and
  transport family.
- [`nanonext`](https://github.com/r-lib/nanonext) — main client/server
  ergonomics reference.
- [`mangoro`](https://github.com/sounkou-bioinfo/mangoro) —
  thin-envelope + Arrow IPC RPC client.
- [`quack`](https://github.com/duckdb/duckdb-quack) — DuckDB’s
  experimental Quack HTTP protocol extension.
- [`@quack-protocol/sdk`](https://github.com/tobilg/quack-protocol) and
  [`adbc-driver-quack`](https://github.com/gizmodata/adbc-driver-quack)
  — related Quack client/codec implementations used as protocol
  references.
- [DuckDB C API](https://duckdb.org/docs/stable/clients/c/api) —
  extension and SQL integration boundary.
- [Apache Arrow
  IPC](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
  — tabular payload format.
