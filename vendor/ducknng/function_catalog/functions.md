# Function Catalog

This file is generated from `function_catalog/functions.yaml`.

## Service Control

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_start_server` | scalar | `name, listen, contexts, recv_max_bytes, session_idle_ms, tls_config_id[, ip_allowlist_json]` | `BOOLEAN` | Start a named ducknng service and choose the carrier from the listen URL scheme. |
| `ducknng_stop_server` | scalar | `name` | `BOOLEAN` | Stop a named ducknng service. |
| `ducknng_service_inflight` | scalar | `name` | `UBIGINT` | Return the current in-flight request count for a named service. Re-evaluated on every call, making it suitable for use inside recursive poll CTEs. |
| `ducknng_set_service_execution_model` | scalar | `name, model` | `BOOLEAN` | Set the DuckDB connection execution model used by service-side SQL. |
| `ducknng_transport_capabilities` | scalar |  | `VARCHAR` | Return the active net backend's capability descriptor as a JSON object. |
| `ducknng_list_transport_capabilities` | table |  | `TABLE(target VARCHAR, active BOOLEAN, http VARCHAR, https VARCHAR, http_response_stream VARCHAR, inproc VARCHAR, tcp VARCHAR, ipc VARCHAR, tls_tcp VARCHAR, websocket VARCHAR, async_is_real BOOLEAN, honors_timeout BOOLEAN, honors_cancel BOOLEAN, tls_owner VARCHAR)` | List the capability contract for every build target; exactly one row is active. |
| `ducknng_set_execution_pool_max` | scalar | `n` | `UBIGINT` | Set the runtime execution-connection pool grow ceiling and return the effective value. |
| `ducknng_execution_pool_max` | scalar |  | `UBIGINT` | Return the current execution-connection pool grow ceiling. |

## Introspection

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_list_servers` | table |  | `TABLE(service_id UBIGINT, name VARCHAR, listen VARCHAR, contexts INTEGER, running BOOLEAN, execution_model VARCHAR, sessions UBIGINT, active_pipes UBIGINT, max_open_sessions UBIGINT, max_active_pipes UBIGINT, inflight_requests UBIGINT, max_inflight_requests UBIGINT, max_sessions_per_peer_identity UBIGINT, max_inflight_per_peer_identity UBIGINT, max_reply_bytes_per_peer_identity UBIGINT, max_session_open_rate_per_peer_identity UBIGINT, tls_enabled BOOLEAN, tls_auth_mode INTEGER, peer_identity_required BOOLEAN, peer_allowlist_active BOOLEAN, ip_allowlist_active BOOLEAN, sql_authorizer_active BOOLEAN, peer_allowlist_count UBIGINT, ip_allowlist_count UBIGINT)` | List registered ducknng services. |
| `ducknng_read_monitor` | table | `name, after_seq, max_events` | `TABLE(seq UBIGINT, ts_ms UBIGINT, pipe_id UBIGINT, service_name VARCHAR, listen VARCHAR, transport_family VARCHAR, scheme VARCHAR, event VARCHAR, admitted BOOLEAN, reason VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, peer_identity VARCHAR)` | Read the bounded per-service NNG pipe monitor event stream. |
| `ducknng_monitor_status` | table | `name` | `TABLE(service_name VARCHAR, event_capacity UBIGINT, event_count UBIGINT, oldest_seq UBIGINT, newest_seq UBIGINT, dropped_events UBIGINT, active_pipes UBIGINT, max_active_pipes UBIGINT)` | Return pipe monitor ring status and active-pipe counters for a running service. |
| `ducknng_list_pipes` | table | `name` | `TABLE(pipe_id UBIGINT, opened_ms UBIGINT, service_name VARCHAR, listen VARCHAR, transport_family VARCHAR, scheme VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, peer_identity VARCHAR)` | List currently active NNG pipes for a running service. |
| `ducknng_nng_stats` | table |  | `TABLE(scope VARCHAR, name VARCHAR, type VARCHAR, unit VARCHAR, value UBIGINT, svalue VARCHAR, description VARCHAR)` | Snapshot of NNG's native statistics tree, flattened into rows. |
| `ducknng_monitor_socket` | scalar | `socket_id` | `BOOLEAN` | Opt a client socket into pipe-event monitoring. Subsequent pipe add/remove events are captured into a per-socket ring readable with ducknng_read_socket_monitor(). |
| `ducknng_read_socket_monitor` | table | `socket_id, after_seq, max_events` | `TABLE(seq UBIGINT, ts_ms UBIGINT, pipe_id UBIGINT, event VARCHAR, dropped UBIGINT)` | Read captured pipe events for a monitored client socket. event is 'add' or 'remove'; dropped is the running count of events evicted from the ring. |
| `ducknng_socket_monitor_wait` | scalar | `socket_id, after_seq, timeout_ms` | `UBIGINT` | Block until a monitored socket records a pipe event newer than after_seq, or until timeout_ms elapses; returns the current newest event seq. |
| `ducknng_log_entries` | table |  | `TABLE(ts TIMESTAMP, level VARCHAR, log_type VARCHAR, message VARCHAR)` | Return a snapshot of the most recent DuckDB log entries captured by the ducknng log ring. |
| `ducknng_enable_log_capture` | scalar |  | `BOOLEAN` | Wire the ducknng log ring into DuckDB's internal logger so that DuckDB log entries are captured and visible through ducknng_log_entries(). Returns TRUE if capture is active after the call, FALSE if registration failed. Safe to call multiple times. |

## Method Registry

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_register_exec_method` | scalar | `[requires_auth]` | `BOOLEAN` | Register the built-in exec RPC method explicitly. |
| `ducknng_register_upload_methods` | scalar | `[requires_auth]` | `BOOLEAN` | Register the built-in upload lane RPC methods (upload_open/append/commit/abort) explicitly. |
| `ducknng_set_method_auth` | scalar | `name, requires_auth` | `BOOLEAN` | Set descriptor-level verified-peer-identity authorization for a registered RPC method. |
| `ducknng_unregister_method` | scalar | `name` | `BOOLEAN` | Unregister a method from the runtime registry. |
| `ducknng_unregister_family` | scalar | `family` | `UBIGINT` | Unregister all methods in a family and return the number removed. |
| `ducknng_list_methods` | table |  | `TABLE(name VARCHAR, family VARCHAR, summary VARCHAR, transport_pattern VARCHAR, request_payload_format VARCHAR, response_payload_format VARCHAR, response_mode VARCHAR, session_behavior VARCHAR, request_schema_json VARCHAR, response_schema_json VARCHAR, requires_auth BOOLEAN, requires_session BOOLEAN, opens_session BOOLEAN, closes_session BOOLEAN, mutates_state BOOLEAN, idempotent BOOLEAN, deprecated BOOLEAN, disabled BOOLEAN, accepted_request_flags UINTEGER, emitted_reply_flags UINTEGER, max_request_bytes UBIGINT, max_reply_bytes UBIGINT, version_introduced INTEGER)` | List the currently registered RPC methods in the runtime registry. |

## Primitive Transport

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_open_socket` | scalar | `protocol` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Open a client socket handle for a supported NNG protocol. |
| `ducknng_dial_socket` | scalar | `socket_id, url, timeout_ms, tls_config_id` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Dial a URL using an opened socket handle. |
| `ducknng_listen_socket` | scalar | `socket_id, url, recv_max_bytes, tls_config_id` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Bind a socket handle to a listen URL and start its NNG listener. |
| `ducknng_close_socket` | scalar | `socket_id` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Close a client socket handle. |
| `ducknng_send_socket_raw` | scalar | `socket_id, frame, timeout_ms` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Send one raw frame through an active socket handle. |
| `ducknng_recv_socket_raw` | scalar | `socket_id, timeout_ms` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Receive one raw frame from an active socket handle. |
| `ducknng_subscribe_socket` | scalar | `socket_id, topic` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Register a raw topic prefix on a sub socket. |
| `ducknng_unsubscribe_socket` | scalar | `socket_id, topic` | `STRUCT(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, socket_id UBIGINT, payload BLOB, url VARCHAR)` | Remove a raw topic prefix from a sub socket. |
| `ducknng_list_sockets` | table |  | `TABLE(socket_id UBIGINT, protocol VARCHAR, url VARCHAR, open BOOLEAN, connected BOOLEAN, listening BOOLEAN, send_timeout_ms INTEGER, recv_timeout_ms INTEGER)` | List client socket handles in the runtime. |
| `ducknng_request` | table | `url, payload, timeout_ms, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, payload BLOB)` | Perform a one-shot raw request and return a structured result row. |
| `ducknng_request_socket` | table | `socket_id, payload, timeout_ms` | `TABLE(ok BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR, payload BLOB)` | Perform a raw request through a previously dialed socket handle and return a structured result row. |
| `ducknng_request_raw` | scalar | `url, payload, timeout_ms, tls_config_id` | `BLOB` | Perform a one-shot raw request and return the raw reply frame bytes. |
| `ducknng_request_socket_raw` | scalar | `socket_id, payload, timeout_ms` | `BLOB` | Perform a raw request through a dialed socket handle and return the raw reply frame bytes. |
| `ducknng_decode_frame` | table | `frame` | `TABLE(ok BOOLEAN, error VARCHAR, version UTINYINT, type UTINYINT, status UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR)` | Decode a raw ducknng frame into envelope fields and extracted payload columns. |
| `ducknng_frame_payload` | scalar | `frame` | `BLOB` | Extract the payload bytes from one raw ducknng frame. |
| `ducknng_frame_payload_text` | scalar | `frame` | `VARCHAR` | Extract the payload as UTF-8 text when a raw ducknng frame carries a textual payload. |
| `ducknng_frame_error_text` | scalar | `frame` | `VARCHAR` | Extract the protocol-level error text from a raw ducknng error frame. |
| `ducknng_frame_version` | scalar | `frame` | `UTINYINT` | Extract the protocol version field from one raw ducknng frame. |
| `ducknng_frame_type` | scalar | `frame` | `UTINYINT` | Extract the numeric reply type field from one raw ducknng frame. |
| `ducknng_frame_status` | scalar | `frame` | `UTINYINT` | Extract the protocol status from one raw ducknng frame. |
| `ducknng_frame_flags` | scalar | `frame` | `UINTEGER` | Extract the reply flags bitset from one raw ducknng frame. |
| `ducknng_frame_type_name` | scalar | `frame` | `VARCHAR` | Extract the symbolic reply type name from one raw ducknng frame. |
| `ducknng_frame_name` | scalar | `frame` | `VARCHAR` | Extract the method or reply name field from one raw ducknng frame. |
| `ducknng_frame_end_of_stream` | scalar | `frame` | `BOOLEAN` | Report whether one raw ducknng frame carries the end-of-stream reply flag. |

## Transport Security

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_list_tls_configs` | table |  | `TABLE(tls_config_id UBIGINT, source VARCHAR, enabled BOOLEAN, has_cert_key_file BOOLEAN, has_ca_file BOOLEAN, has_cert_pem BOOLEAN, has_key_pem BOOLEAN, has_ca_pem BOOLEAN, has_password BOOLEAN, auth_mode INTEGER, peer_allowlist_active BOOLEAN, peer_allowlist_count UBIGINT, peer_allowlist_json VARCHAR)` | List registered TLS config handles and the kinds of material they contain. |
| `ducknng_drop_tls_config` | scalar | `tls_config_id` | `BOOLEAN` | Remove a registered TLS config handle from the runtime. |
| `ducknng_set_tls_peer_allowlist` | scalar | `tls_config_id, identities_json` | `BOOLEAN` | Set the default exact peer-identity allowlist on a TLS config handle. |
| `ducknng_set_service_peer_allowlist` | scalar | `name, identities_json` | `BOOLEAN` | Dynamically set the exact peer-identity allowlist for a running service. |
| `ducknng_set_service_ip_allowlist` | scalar | `name, cidrs_json` | `BOOLEAN` | Dynamically set the IP/CIDR remote-address allowlist for a running service. |
| `ducknng_set_service_limits` | scalar | `name, max_open_sessions[, max_active_pipes[, max_inflight_requests[, max_sessions_per_peer_identity[, max_inflight_per_peer_identity[, max_reply_bytes_per_peer_identity[, max_session_open_rate_per_peer_identity]]]]]]` | `BOOLEAN` | Set service resource limits. |
| `ducknng_auth_context` | table |  | `TABLE(phase VARCHAR, service_name VARCHAR, transport_family VARCHAR, scheme VARCHAR, listen VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, remote_port INTEGER, tls_verified BOOLEAN, peer_identity VARCHAR, peer_allowlist_active BOOLEAN, ip_allowlist_active BOOLEAN, sql_authorizer_active BOOLEAN, http_method VARCHAR, http_path VARCHAR, content_type VARCHAR, body_bytes UBIGINT, rpc_method VARCHAR, rpc_type VARCHAR, payload_bytes UBIGINT)` | Expose the current request context to a SQL authorization callback. |
| `ducknng_set_service_authorizer` | scalar | `name, authorizer_sql` | `BOOLEAN` | Install or clear a service-level SQL authorization callback evaluated uniformly for framed RPC requests before method dispatch. |
| `ducknng_self_signed_tls_config` | scalar | `common_name, valid_days, auth_mode` | `UBIGINT` | Generate a self-signed development certificate and register it as a TLS config handle. |
| `ducknng_tls_config_from_pem` | scalar | `cert_pem, key_pem, ca_pem, password, auth_mode` | `UBIGINT` | Register a TLS config handle from in-memory PEM material. |
| `ducknng_tls_config_from_files` | scalar | `cert_key_file, ca_file, password, auth_mode` | `UBIGINT` | Register a TLS config handle from file-backed certificate material. |

## HTTP Transport

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_ncurl` | table | `url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]` | `TABLE(ok BOOLEAN, status INTEGER, error VARCHAR, headers_json VARCHAR, body BLOB, body_text VARCHAR)` | Perform one HTTP or HTTPS request and return an in-band result row. |
| `ducknng_ncurl_aio` | scalar | `url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]` | `UBIGINT` | Launch one asynchronous HTTP or HTTPS request and return a future-like aio handle id. |
| `ducknng_ncurl_aio_collect` | table | `aio_ids, wait_ms` | `TABLE(aio_id UBIGINT, ok BOOLEAN, status INTEGER, error VARCHAR, headers_json VARCHAR, body BLOB, body_text VARCHAR)` | Wait for asynchronous ncurl handles and return one raw HTTP result row per newly collected terminal operation. |
| `ducknng_ncurl_stream_open_aio` | scalar | `url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]` | `UBIGINT` | Open an HTTP or HTTPS response stream asynchronously and return an aio handle for the response headers. |
| `ducknng_ncurl_stream_open_aio_collect` | table | `aio_ids, wait_ms` | `TABLE(aio_id UBIGINT, ok BOOLEAN, stream_id UBIGINT, status INTEGER, error VARCHAR, headers_json VARCHAR)` | Wait for streaming HTTP open aios and return response status, headers, and a readable stream handle. |
| `ducknng_ncurl_stream_recv_aio` | scalar | `stream_id, max_bytes, timeout_ms` | `UBIGINT` | Launch one cancellable asynchronous raw-body receive from an opened HTTP response stream. |
| `ducknng_ncurl_stream_recv_aio_collect` | table | `aio_ids, wait_ms` | `TABLE(aio_id UBIGINT, ok BOOLEAN, stream_id UBIGINT, error VARCHAR, body BLOB, end_of_stream BOOLEAN)` | Wait for streaming HTTP receive aios and return one raw body slice or explicit end-of-stream result per ready handle. |
| `ducknng_ncurl_stream_close` | scalar | `stream_id` | `BOOLEAN` | Close and release an HTTP response stream handle. |
| `ducknng_ncurl_table` | table | `url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]` | `TABLE(dynamic by response Content-Type)` | Perform one HTTP or HTTPS request and parse a successful response body into a DuckDB table using the built-in body codec providers. |
| `ducknng_register_http_profile` | scalar | `profile_id, scheme, host, port, path_prefix, method, tls_required, auth_header_name, auth_header_value[, expires_at_ms[, allow_subjects_json]]` | `BOOLEAN` | Register or replace an outbound HTTP credential profile with fail-closed request scope. |
| `ducknng_drop_http_profile` | scalar | `profile_id` | `BOOLEAN` | Drop a registered outbound HTTP credential profile. |
| `ducknng_list_http_profiles` | table |  | `TABLE(profile_id VARCHAR, scheme VARCHAR, host VARCHAR, port INTEGER, has_port BOOLEAN, path_prefix VARCHAR, method VARCHAR, tls_required BOOLEAN, auth_header_names_json VARCHAR, version UBIGINT, created_ms UBIGINT, updated_ms UBIGINT, expires_at_ms UBIGINT, allow_subjects_json VARCHAR)` | List registered outbound HTTP profiles with redacted credential metadata. |

## Body Codecs

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_list_codecs` | table |  | `TABLE(provider VARCHAR, media_types VARCHAR, kind VARCHAR, function_name VARCHAR, output VARCHAR, notes VARCHAR)` | List built-in body serialization/deserialization providers and any registered user codec hooks. |
| `ducknng_register_codec` | scalar | `content_type, function_name` | `BOOLEAN` | Register a user body codec that decodes a BLOB body into a single VARCHAR value through an existing scalar SQL function. |
| `ducknng_unregister_codec` | scalar | `content_type` | `BOOLEAN` | Remove a previously registered user body codec for a content type. |
| `ducknng_parse_body` | table | `body, content_type` | `TABLE(dynamic by provider)` | Parse one response/request body BLOB according to its content type. |

## HTTP Routes

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_register_http_route` | scalar | `service_name, method, path, handler_sql[, request_max_bytes]` | `BOOLEAN` | Register one exact-path HTTP route beside the framed RPC mount of an existing http:// or https:// service. |
| `ducknng_register_http_route_pattern` | scalar | `service_name, method, match_kind, path_pattern, handler_sql[, request_max_bytes]` | `BOOLEAN` | Register one low-level HTTP route pattern beside the framed RPC mount using exact, prefix, or template matching. |
| `ducknng_unregister_http_route` | scalar | `service_name, method, path` | `BOOLEAN` | Remove one previously registered exact-path HTTP route from a service. |
| `ducknng_unregister_http_route_pattern` | scalar | `service_name, method, match_kind, path_pattern` | `BOOLEAN` | Remove one previously registered prefix, template, or explicit exact route pattern from a service. |
| `ducknng_list_http_routes` | table |  | `TABLE(service_id UBIGINT, route_id UBIGINT, request_max_bytes UBIGINT, service_name VARCHAR, method VARCHAR, match_kind VARCHAR, path VARCHAR, handler_sql VARCHAR, auth_require_identity BOOLEAN, static_dir_path VARCHAR, auth_allow_identities_json VARCHAR)` | List the currently registered HTTP routes across running services, including their match kind and stored path pattern. |
| `ducknng_set_http_route_auth` | scalar | `service_name, method, path[, require_identity[, allow_identities_json]]` | `BOOLEAN` | Set authentication requirements on a registered HTTP route. |
| `ducknng_register_http_static` | scalar | `service_name, path_prefix, dir_path` | `BOOLEAN` | Register a prefix route that serves static files from a directory on the server filesystem. |
| `ducknng_register_http_worker` | scalar | `service_name, worker_name, sql, interval_ms` | `BOOLEAN` | Register a background SQL worker that runs on a recurring interval while the HTTP service is active. |
| `ducknng_unregister_http_worker` | scalar | `service_name, worker_name` | `BOOLEAN` | Unregister a previously registered HTTP background worker. |
| `ducknng_list_http_workers` | table |  | `TABLE(service_name VARCHAR, worker_name VARCHAR, sql VARCHAR, interval_ms UBIGINT)` | List all currently registered HTTP background workers across running services. |
| `ducknng_http_ndjson` | table_macro | `status, body_text` | `TABLE(status INTEGER, content_type VARCHAR, body VARCHAR)` | Convenience table macro for returning an NDJSON HTTP response from a route handler. |
| `ducknng_http_sse` | table_macro | `status, body_text` | `TABLE(status INTEGER, content_type VARCHAR, body VARCHAR)` | Convenience table macro for returning a Server-Sent Events HTTP response from a route handler. |
| `ducknng_http_request` | table |  | `TABLE(service_name VARCHAR, listen VARCHAR, scheme VARCHAR, method VARCHAR, path VARCHAR, query_string VARCHAR, content_type VARCHAR, headers_json VARCHAR, caller_identity VARCHAR, remote_addr VARCHAR, remote_ip VARCHAR, route_method VARCHAR, route_match_kind VARCHAR, route_path VARCHAR, path_params_json VARCHAR, body_bytes UBIGINT, route_id UBIGINT, remote_port INTEGER)` | Expose the current HTTP request context while SQL runs inside an active route handler. |
| `ducknng_http_request_body` | table |  | `TABLE(body BLOB, body_text VARCHAR)` | Expose the current HTTP request body while SQL runs inside an active route handler. |
| `ducknng_http_headers_get` | scalar | `headers_json, name` | `VARCHAR` | Return one header value from ducknng's canonical HTTP header JSON. |
| `ducknng_http_headers_build` | scalar | `names, values` | `VARCHAR` | Build ducknng's canonical HTTP header JSON from parallel name and value lists. |
| `ducknng_http_query_param_get` | scalar | `query_string, name` | `VARCHAR` | Return one decoded query-string parameter value. |
| `ducknng_http_cookie_get` | scalar | `cookie_header, name` | `VARCHAR` | Return one cookie value from a Cookie header string. |
| `ducknng_http_path_params_get` | scalar | `path_params_json, name` | `VARCHAR` | Return one template-route path parameter from path_params_json. |
| `ducknng_http_header` | scalar | `name` | `VARCHAR` | Route-local shortcut for reading one request header by name. |
| `ducknng_http_query_param` | scalar | `name` | `VARCHAR` | Route-local shortcut for reading one decoded query parameter by name. |
| `ducknng_http_cookie` | scalar | `name` | `VARCHAR` | Route-local shortcut for reading one request cookie by name. |
| `ducknng_http_path_param` | scalar | `name` | `VARCHAR` | Route-local shortcut for reading one template path parameter by name. |
| `ducknng_http_response` | table | `status, headers_json, content_type, body, body_text` | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)` | Build the one-row response shape expected by a route handler. |
| `ducknng_http_text` | table | `status, body_text` | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)` | Build a one-row plain-text HTTP route response. |
| `ducknng_http_json` | table | `status, body_text` | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)` | Build a one-row JSON HTTP route response from a text body. |
| `ducknng_http_binary` | table | `status, body` | `TABLE(status INTEGER, headers_json VARCHAR, content_type VARCHAR, body BLOB, body_text VARCHAR)` | Build a one-row binary HTTP route response. |

## Async I/O

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_request_raw_aio` | scalar | `url, frame, timeout_ms, tls_config_id` | `UBIGINT` | Launch one raw req/rep roundtrip asynchronously and return a future-like aio handle id. |
| `ducknng_get_rpc_manifest_raw_aio` | scalar | `url, timeout_ms, tls_config_id` | `UBIGINT` | Launch one asynchronous manifest RPC request and return an aio handle id for the raw reply frame. |
| `ducknng_run_rpc_raw_aio` | scalar | `url, sql, timeout_ms, tls_config_id` | `UBIGINT` | Launch one asynchronous metadata-only exec RPC request and return an aio handle id for the raw reply frame. |
| `ducknng_open_query_raw_aio` | scalar | `url, sql, batch_rows, batch_bytes, timeout_ms, tls_config_id` | `UBIGINT` | Launch one asynchronous query_open request and return an aio handle id for the raw reply frame. |
| `ducknng_fetch_query_raw_aio` | scalar | `url, session_id, session_token, batch_rows, batch_bytes, timeout_ms, tls_config_id` | `UBIGINT` | Launch one asynchronous fetch request and return an aio handle id for the raw reply frame. |
| `ducknng_close_query_raw_aio` | scalar | `url, session_id, session_token, timeout_ms, tls_config_id` | `UBIGINT` | Launch one asynchronous close request and return an aio handle id for the raw reply frame. |
| `ducknng_cancel_query_raw_aio` | scalar | `url, session_id, session_token, timeout_ms, tls_config_id` | `UBIGINT` | Launch one asynchronous cancel request and return an aio handle id for the raw reply frame. |
| `ducknng_request_socket_raw_aio` | scalar | `socket_id, frame, timeout_ms` | `UBIGINT` | Launch one raw req/rep roundtrip asynchronously on an existing req socket handle and return an aio handle id. |
| `ducknng_send_socket_raw_aio` | scalar | `socket_id, frame, timeout_ms` | `UBIGINT` | Launch one raw socket send asynchronously and return an aio handle id. |
| `ducknng_recv_socket_raw_aio` | scalar | `socket_id, timeout_ms` | `UBIGINT` | Launch one raw socket receive asynchronously and return an aio handle id. |
| `ducknng_aio_ready` | scalar | `aio_id` | `BOOLEAN` | Return whether an aio handle has reached a terminal state. |
| `ducknng_aio_wait` | scalar | `aio_ids, wait_ms` | `BOOLEAN` | Wait until any requested aio handle reaches a terminal state without collecting or dropping it. |
| `ducknng_aio_status` | table | `aio_id` | `TABLE(aio_id UBIGINT, exists BOOLEAN, kind VARCHAR, state VARCHAR, phase VARCHAR, terminal BOOLEAN, send_done BOOLEAN, send_ok BOOLEAN, recv_done BOOLEAN, recv_ok BOOLEAN, has_reply_frame BOOLEAN, error VARCHAR, nng_error INTEGER, nng_error_message VARCHAR)` | Inspect the current or terminal status of one aio handle, including send-phase and recv-phase completion. |
| `ducknng_aio_collect` | table | `aio_ids, wait_ms` | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame BLOB, nng_error INTEGER, nng_error_message VARCHAR)` | Wait for any requested aio handles to finish and return one row per newly collected terminal result. |
| `ducknng_aio_collect_decoded` | table | `aio_ids, wait_ms` | `TABLE(aio_id UBIGINT, ok BOOLEAN, error VARCHAR, frame_ok BOOLEAN, frame_error VARCHAR, version UTINYINT, type UTINYINT, status UTINYINT, flags UINTEGER, type_name VARCHAR, name VARCHAR, payload BLOB, payload_text VARCHAR, nng_error INTEGER, nng_error_message VARCHAR)` | Wait for framed aio handles, collect their terminal frame rows, and project the decoded envelope columns directly. |
| `ducknng_aio_cancel` | scalar | `aio_id` | `BOOLEAN` | Request cancellation of a pending aio handle. |
| `ducknng_aio_drop` | scalar | `aio_id` | `BOOLEAN` | Release a terminal aio handle from the runtime registry. |

## RPC Helper

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_get_rpc_manifest` | table | `url, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, manifest VARCHAR)` | Request the RPC manifest and return a structured result row. |
| `ducknng_get_rpc_manifest_raw` | scalar | `url, tls_config_id` | `BLOB` | Request the RPC manifest and return the raw reply frame as BLOB. |
| `ducknng_run_rpc` | table | `url, sql, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute a metadata-oriented RPC call and return a structured result row. |
| `ducknng_run_rpc_params` | table | `url, sql, params, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, rows_changed UBIGINT, statement_type INTEGER, result_type INTEGER)` | Execute one parameterized metadata-oriented exec RPC and return a structured result row. |
| `ducknng_run_rpc_raw` | scalar | `url, sql, tls_config_id` | `BLOB` | Execute the exec RPC and return the raw reply frame as BLOB. |
| `ducknng_query_rpc` | table | `url, sql, tls_config_id` | `table` | Execute a row-returning RPC query as a session convenience wrapper and expose the fetched Arrow IPC rows as a DuckDB table. |
| `ducknng_query_rpc_params` | table | `url, sql, params, tls_config_id` | `table` | Execute a parameterized row-returning query through the session family and expose its Arrow rows as a DuckDB table. |
| `ducknng_prepare_query` | table | `url, sql, tls_config_id` | `table (zero rows with the prepared remote schema)` | Prepare exactly one remote SQL statement without executing it and expose its result schema. |
| `ducknng_prepare_query_params` | table | `url, sql, params, tls_config_id` | `table (zero rows with the prepared remote schema)` | Bind a typed parameter tuple, prepare exactly one remote SQL statement without executing it, and expose its result schema. |
| `ducknng_upload_table` | table | `url, source_query, target_table[, tls_config_id]` | `TABLE(rows_uploaded BIGINT, bytes_uploaded BIGINT)` | Run source_query locally and stream its result rows into a remote table over the quack-batch upload lane; returns rows_uploaded and client-sent bytes_uploaded. |

## RPC Session

| name | kind | arguments | returns | description |
|---|---|---|---|---|
| `ducknng_open_query` | table | `url, sql, batch_rows, batch_bytes, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)` | Open a server-side query session and return the JSON control metadata as a structured row. |
| `ducknng_fetch_query` | table | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT, payload BLOB, end_of_stream BOOLEAN)` | Fetch the next session reply and return either JSON control metadata or an Arrow IPC batch payload. |
| `ducknng_fetch_query_table` | table | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `TABLE(dynamic from Arrow IPC batch)` | Fetch one session row batch and decode the returned Arrow IPC payload directly into a DuckDB table. |
| `ducknng_close_query` | table | `url, session_id, session_token, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)` | Close a server-side query session and return the JSON control metadata as a structured row. |
| `ducknng_cancel_query` | table | `url, session_id, session_token, tls_config_id` | `TABLE(ok BOOLEAN, error VARCHAR, session_id UBIGINT, session_token VARCHAR, state VARCHAR, next_method VARCHAR, control_json VARCHAR, idle_timeout_ms UBIGINT)` | Request cancellation for a server-side query session and return the JSON control metadata as a structured row. |
| `ducknng_open_query_raw` | scalar | `url, sql, batch_rows, batch_bytes, tls_config_id` | `BLOB` | Open a server-side query session and return the raw reply frame as BLOB. |
| `ducknng_fetch_query_raw` | scalar | `url, session_id, session_token, batch_rows, batch_bytes, tls_config_id` | `BLOB` | Fetch the next session reply and return the raw reply frame as BLOB. |
| `ducknng_close_query_raw` | scalar | `url, session_id, session_token, tls_config_id` | `BLOB` | Close a server-side query session and return the raw reply frame as BLOB. |
| `ducknng_cancel_query_raw` | scalar | `url, session_id, session_token, tls_config_id` | `BLOB` | Cancel a server-side query session and return the raw reply frame as BLOB. |
