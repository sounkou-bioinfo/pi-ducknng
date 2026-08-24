## Function reference

### Server

#### `ducknng_start_server(name, listen_url, rep_contexts, recv_max_bytes, session_idle_ms, tls_config_id)`

Start a named NNG REP server. Supports inproc://, ipc://, tcp://, tls+tcp://, ws://, wss://, http://, and https:// URL schemes.

#### `ducknng_stop_server(name)`

Stop a named server, close all its pipes, and release resources.

#### `ducknng_list_servers()`

List all running servers with listen URL, TLS mode/peering mode, pipe count.

### NNG Sockets

#### `ducknng_open_socket(kind)`

Open a raw NNG socket: req, rep, pub, sub, push, pull, surveyor, respondent, bus, pair.

#### `ducknng_dial_socket(socket_id, url, timeout_ms, tls_config_id)`

Dial a socket to a remote listener.

#### `ducknng_listen_socket(socket_id, url, flags, tls_config_id)`

Start listening on a socket at the given URL.

#### `ducknng_send_socket_raw(socket_id, data, timeout_ms)`

Send raw bytes on a connected socket. Negative timeout = non-blocking.

#### `ducknng_recv_socket_raw(socket_id, timeout_ms)`

Receive raw bytes from a connected socket.

#### `ducknng_close_socket(socket_id)`

Close a socket and release resources.

#### `ducknng_subscribe_socket(socket_id, prefix)`

Subscribe a SUB socket to a topic prefix.

#### `ducknng_unsubscribe_socket(socket_id, prefix)`

Unsubscribe a SUB socket from a topic prefix.

#### `ducknng_list_sockets()`

List all open NNG sockets with kind and state.

### Framed RPC

#### `ducknng_get_rpc_manifest(url, tls_config_id)`

Request the RPC method manifest from a remote server.

#### `ducknng_query_rpc(url, sql, tls_config_id)`

Execute SQL on a remote server and return result rows. For SELECT: opens session + auto-fetch. For DML: returns rows_changed.

#### `ducknng_run_rpc(url, sql, tls_config_id)`

Execute SQL via the exec method. Returns rows_changed + metadata.

#### `ducknng_open_query(url, sql, batch_rows, batch_bytes, tls_config_id)`

Open a remote query session. Returns session_id + session_token needed for fetch/close/cancel.

#### `ducknng_fetch_query_table(url, session_id, session_token, batch_rows, batch_bytes, tls_config_id)`

Fetch the next batch of rows from an open query session. Returns decoded Arrow IPC rows.

#### `ducknng_close_query(url, session_id, session_token, tls_config_id)`

Close a remote query session and release server-side resources.

#### `ducknng_cancel_query(url, session_id, session_token, tls_config_id)`

Cancel a running query on a remote session.

#### `ducknng_request_raw(url, data, timeout_ms, tls_config_id)`

Send a raw NNG request and return the reply as a BLOB frame.

#### `ducknng_request_socket_raw(socket_id, data, timeout_ms)`

Send a raw request on a connected socket and return the reply frame.

#### `ducknng_get_rpc_manifest_raw(url, tls_config_id)`

Request the manifest and return the raw reply frame.

#### `ducknng_open_query_raw(url, sql, batch_rows, batch_bytes, tls_config_id)`

Open a query session and return the raw reply frame.

#### `ducknng_fetch_query_raw(url, session_id, session_token, batch_rows, batch_bytes, tls_config_id)`

Fetch next batch and return the raw Arrow IPC frame.

#### `ducknng_close_query_raw(url, session_id, session_token, tls_config_id)`

Close a session and return the raw reply frame.

#### `ducknng_cancel_query_raw(url, session_id, session_token, tls_config_id)`

Cancel a query and return the raw reply frame.

### HTTP Client

#### `ducknng_ncurl(url, method, headers_json, body, timeout_ms, tls_config_id)`

Perform an HTTP request. Returns status, headers_json, body BLOB, body_text VARCHAR.

#### `ducknng_ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id)`

HTTP request with auto-parsed response body via Content-Type codec dispatch. Requires 2xx status.

### HTTP Routes

#### `ducknng_register_http_route(service_name, method, path, handler_sql)`

Register an exact-match HTTP route. Handler SQL must return status, content_type, body or body_text columns.

#### `ducknng_register_http_route_pattern(service_name, method, match_kind, path, handler_sql)`

Register prefix or template-matched route. match_kind: 'prefix' or 'template'. {param} accessible via ducknng_http_path_param.

#### `ducknng_add_stream_route(service_name, method, path, handler_sql, content_type)`

Register a chunked-streaming route. Handler SQL must return 'chunk' column. Each row written as HTTP chunk. Default content-type: text/event-stream.

#### `ducknng_register_http_static(service_name, url_prefix, directory_path)`

Serve static files from a directory under a URL prefix.

#### `ducknng_unregister_http_route(service_name, method, path)`

Unregister a previously registered HTTP route.

#### `ducknng_list_http_routes()`

List all registered HTTP routes with method, path, match kind, auth policy, streaming mode.

### HTTP Workers

#### `ducknng_register_http_worker(service_name, worker_name, sql, interval_ms)`

Register a background worker that executes SQL on a recurring interval (milliseconds).

#### `ducknng_unregister_http_worker(service_name, worker_name)`

Unregister a background worker.

#### `ducknng_list_http_workers()`

List all registered HTTP workers with their interval and last-run timestamp.

### Async I/O

#### `ducknng_send_socket_raw_aio(socket_id, data, timeout_ms)`

Launch an async raw socket send. Returns aio_id to collect with ducknng_aio_collect.

#### `ducknng_recv_socket_raw_aio(socket_id, timeout_ms)`

Launch an async raw socket recv. Returns aio_id.

#### `ducknng_request_raw_aio(url, data, timeout_ms, tls_config_id)`

Launch an async raw RPC request. Returns aio_id.

#### `ducknng_request_socket_raw_aio(socket_id, data, timeout_ms)`

Launch an async request on a connected socket. Returns aio_id.

#### `ducknng_run_rpc_raw_aio(url, sql, timeout_ms, tls_config_id)`

Launch an async exec RPC. Returns aio_id.

#### `ducknng_get_rpc_manifest_raw_aio(url, timeout_ms, tls_config_id)`

Launch an async manifest request. Returns aio_id.

#### `ducknng_open_query_raw_aio(url, sql, batch_rows, batch_bytes, timeout_ms, tls_config_id)`

Launch an async query session open. Returns aio_id.

#### `ducknng_fetch_query_raw_aio(url, session_id, session_token, batch_rows, batch_bytes, timeout_ms, tls_config_id)`

Launch an async batch fetch. Returns aio_id.

#### `ducknng_close_query_raw_aio(url, session_id, session_token, timeout_ms, tls_config_id)`

Launch an async session close. Returns aio_id.

#### `ducknng_cancel_query_raw_aio(url, session_id, session_token, timeout_ms, tls_config_id)`

Launch an async cancel. Returns aio_id.

#### `ducknng_ncurl_aio(url, method, headers_json, body, timeout_ms, tls_config_id)`

Launch an async HTTP request. Returns aio_id.

#### `ducknng_aio_ready(aio_id)`

Check if an async operation has completed.

#### `ducknng_aio_cancel(aio_id)`

Cancel a pending async operation.

#### `ducknng_aio_drop(aio_id)`

Drop an aio handle and release its resources.

#### `ducknng_aio_wait(aio_ids, timeout_ms)`

Block until all aio_ids complete or timeout. aio_ids: LIST(UBIGINT).

#### `ducknng_aio_collect(aio_ids, wait_ms)`

Collect async RPC results into decoded frame table.

#### `ducknng_aio_collect_decoded(aio_ids, wait_ms)`

Collect and decode ASYNC RPC aio results into a decoded frame rows table.

#### `ducknng_ncurl_aio_collect(aio_ids, wait_ms)`

Collect async HTTP results into ncurl-style table.

### Body Codecs

#### `ducknng_parse_body(body, content_type)`

Parse a BLOB using the supplied content type. Supports JSON, NDJSON, CSV, TSV, Parquet, Arrow IPC, form-urlencoded, ducknng frames.

#### `ducknng_parse_csv(body)`

Parse a CSV BLOB via DuckDB read_csv_auto through a tempfile round-trip.

#### `ducknng_parse_tsv(body)`

Parse a TSV BLOB via DuckDB read_csv_auto(delim='	') through a tempfile round-trip.

#### `ducknng_parse_parquet(body)`

Parse a Parquet BLOB via DuckDB read_parquet through a tempfile round-trip.

#### `ducknng_list_codecs()`

List all built-in and user-registered body codec providers.

#### `ducknng_register_codec(content_type, function_name)`

Register a scalar SQL function as a custom body codec for the given content type.

#### `ducknng_unregister_codec(content_type)`

Unregister a user-registered body codec, restoring the built-in behavior.

### TLS

#### `ducknng_self_signed_tls_config(host, days, auth_mode)`

Generate a self-signed TLS cert in memory. auth_mode: 0=none, 1=server, 2=mutual. Returns TLS config handle.

#### `ducknng_tls_config_from_files(cert_path, key_path, ca_path, auth_mode)`

Create TLS config from PEM file paths.

#### `ducknng_tls_config_from_pem(cert_pem, key_pem, ca_pem, hostname, auth_mode)`

Create TLS config from in-memory PEM text (no file I/O).

#### `ducknng_drop_tls_config(tls_config_id)`

Drop a TLS config and release its certificate memory.

#### `ducknng_list_tls_configs()`

List all created TLS configs with auth mode.

#### `ducknng_set_tls_peer_allowlist(tls_config_id, identity_regex)`

Set peer identity allowlist on a TLS config for server-side mTLS filtering.

### Service

#### `ducknng_set_service_limits(name, max_memory, max_sessions, max_result_bytes, max_send_bytes, max_recv_bytes, max_pipes, query_timeout_ms)`

Set resource limits for a service. All params after name are UBIGINT with 0 = no limit.

#### `ducknng_set_service_execution_model(name, model)`

Set execution model: 'shared_serialized_connection', 'service_serialized_connection', or 'request_connection'.

### Admission

#### `ducknng_set_service_peer_allowlist(name, identity_regex)`

Restrict a service to mTLS peer identities matching a regex.

#### `ducknng_set_service_ip_allowlist(name, cidr_list)`

Restrict a service to clients matching IP/CIDR patterns. Semicolon-separated list.

#### `ducknng_set_http_route_auth(service_name, method, path, require_auth, auth_allow_identities)`

Set route-level auth policy. require_auth: BOOLEAN. allow_identities: comma-separated peer identity regexes.

#### `ducknng_set_service_authorizer(name, handler_sql)`

Register a SQL authorizer callback for a service. The handler receives query text and returns allow/deny.

#### `ducknng_auth_context()`

Return the current request's auth context: peer_identity, peer_addr, authenticated columns.

### Monitoring

#### `ducknng_read_monitor(name, after_seq, max_events)`

Read pipe events from a service's event monitor ring buffer.

#### `ducknng_monitor_status(name)`

Return monitor ring buffer capacity, event counts, active/max pipes.

#### `ducknng_list_pipes(name)`

List currently open NNG pipes for a service.

#### `ducknng_log_entries()`

Read the DuckDB log ring buffer (requires ducknng_enable_log_capture()).

#### `ducknng_enable_log_capture()`

Enable DuckDB log ring capture for the current session.

### Method Registry

#### `ducknng_list_methods()`

List all registered RPC methods with auth requirements.

#### `ducknng_register_exec_method(enable_default)`

Register (or re-register) the default exec method. Pass TRUE to enable by default.

#### `ducknng_set_method_auth(method_name, require_auth)`

Set auth requirement for an RPC method.

#### `ducknng_unregister_method(method_name)`

Unregister a single RPC method.

#### `ducknng_unregister_family(family_name)`

Unregister all methods in a family. Returns count of unregistered methods.

### Frame Helpers

#### `ducknng_decode_frame(data)`

Decode a ducknng protocol frame into type, name, payload_text, error_text, and raw payload.

#### `ducknng_frame_payload(data)`

Extract the frame payload BLOB from a ducknng protocol frame.

#### `ducknng_frame_payload_text(data)`

Decode frame payload as UTF-8 text.

#### `ducknng_frame_error_text(data)`

Extract the error message from a ducknng error frame.

#### `ducknng_frame_version(data)`

Extract protocol version from a ducknng frame.

#### `ducknng_frame_type(data)`

Extract numeric type code from a ducknng frame.

#### `ducknng_frame_flags(data)`

Extract flags field from a ducknng frame.

#### `ducknng_frame_type_name(data)`

Decode the human-readable type name from a ducknng frame.

#### `ducknng_frame_name(data)`

Extract the method name from a ducknng query/call frame.

#### `ducknng_frame_end_of_stream(data)`

Check if a ducknng frame is an end-of-stream marker.

### SQL Macros

#### `ducknng_format_sse(data, event := NULL, id := NULL, retry := NULL)`

Format a Server-Sent Events event string with optional event type, id, retry fields.

#### `ducknng_http_response(status, headers_json, content_type, body, body_text)`

Build an HTTP route response row with explicit status, headers, content-type, and body.

#### `ducknng_http_text(status, body_text)`

Build a text/plain HTTP route response.

#### `ducknng_http_json(status, body_text)`

Build an application/json HTTP route response.

#### `ducknng_http_binary(status, body)`

Build an application/octet-stream HTTP route response.

#### `ducknng_http_ndjson(status, body_text)`

Build an application/x-ndjson HTTP route response.
