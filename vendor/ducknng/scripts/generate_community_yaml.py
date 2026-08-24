#!/usr/bin/env python3
"""
Generate description.yml and functions.yaml for DuckDB community-extension submission.
Also generates function reference markdown for README.Rmd inclusion.

Usage:
  python3 scripts/generate_community_yaml.py          # YAML+JSON output
  python3 scripts/generate_community_yaml.py --docs   # also produce docs markdown
  python3 scripts/generate_community_yaml.py --render # run SQL through DuckDB for output
"""

import json, os, subprocess, sys, textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
EXT_BIN = REPO_ROOT / "build" / "release" / "ducknng.duckdb_extension"
COMMUNITY_DIR = REPO_ROOT / "community-extensions" / "extensions" / "ducknng"
DOCS_DIR = REPO_ROOT / "docs"
EXTENSION = {
    "name": "ducknng",
    "description": "Pure C DuckDB extension exposing a DuckDB-backed SQL and RPC server over NNG using Arrow IPC — with framed RPC, custom HTTP routes, TLS support, body codec layer, and admission controls",
    "version": "0.1.2.9000",
    "language": "C",
    "build": "cmake",
    "license": "MIT",
    "requires_toolchains": "python3",
    "excluded_platforms": "wasm_mvp;wasm_eh;wasm_threads;windows_amd64;windows_arm64",
    "maintainers": ["sounkou-bioinfo"],
}


def af(name, kind, cat, sig, ret, desc, examples=None):
    """Build a function entry dict."""
    return {
        "name": name,
        "kind": kind,
        "category": cat,
        "signature": sig,
        "returns": ret,
        "description": desc,
        "examples": examples or [],
    }


SRV = "Server"
SKT = "NNG Sockets"
RPC = "Framed RPC"
HTTP = "HTTP Client"
HTR = "HTTP Routes"
HTW = "HTTP Workers"
HTH = "HTTP Helpers"
BDY = "Body Codecs"
TLS = "TLS"
SVC = "Service"
ATH = "Admission"
MON = "Monitoring"
AIO = "Async I/O"
REG = "Method Registry"
FRM = "Frame Helpers"
MAC = "SQL Macros"

FUNCTIONS = [
    # ---- Server ----
    af(
        "ducknng_start_server",
        "scalar",
        SRV,
        "start_server(name, listen_url, rep_contexts, recv_max_bytes, session_idle_ms, tls_config_id)",
        "BOOLEAN",
        "Start a named NNG REP server. Supports inproc://, ipc://, tcp://, tls+tcp://, ws://, wss://, http://, and https:// URL schemes.",
    ),
    af(
        "ducknng_stop_server",
        "scalar",
        SRV,
        "stop_server(name)",
        "BOOLEAN",
        "Stop a named server, close all its pipes, and release resources.",
    ),
    af(
        "ducknng_list_servers",
        "table",
        SRV,
        "list_servers()",
        "table",
        "List all running servers with listen URL, TLS mode/peering mode, pipe count.",
    ),
    af(
        "ducknng_nng_version",
        "scalar",
        SRV,
        "nng_version()",
        "VARCHAR",
        "Return the vendored NNG library version string.",
    ),
    # ---- NNG Sockets ----
    af(
        "ducknng_open_socket",
        "scalar",
        SKT,
        "open_socket(kind)",
        "STRUCT(ok,socket_id UBIGINT,error,...)",
        "Open a raw NNG socket: req, rep, pub, sub, push, pull, surveyor, respondent, bus, pair.",
    ),
    af(
        "ducknng_dial_socket",
        "scalar",
        SKT,
        "dial_socket(socket_id, url, timeout_ms, tls_config_id)",
        "STRUCT(ok,error,...)",
        "Dial a socket to a remote listener.",
    ),
    af(
        "ducknng_listen_socket",
        "scalar",
        SKT,
        "listen_socket(socket_id, url, flags, tls_config_id)",
        "STRUCT(ok,error,...)",
        "Start listening on a socket at the given URL.",
    ),
    af(
        "ducknng_send_socket_raw",
        "scalar",
        SKT,
        "send_socket_raw(socket_id, data, timeout_ms)",
        "STRUCT(ok,error,...)",
        "Send raw bytes on a connected socket. Negative timeout = non-blocking.",
    ),
    af(
        "ducknng_recv_socket_raw",
        "scalar",
        SKT,
        "recv_socket_raw(socket_id, timeout_ms)",
        "STRUCT(ok,payload BLOB,error,...)",
        "Receive raw bytes from a connected socket.",
    ),
    af(
        "ducknng_close_socket",
        "scalar",
        SKT,
        "close_socket(socket_id)",
        "STRUCT(ok,error,...)",
        "Close a socket and release resources.",
    ),
    af(
        "ducknng_subscribe_socket",
        "scalar",
        SKT,
        "subscribe_socket(socket_id, prefix)",
        "STRUCT(ok,error,...)",
        "Subscribe a SUB socket to a topic prefix.",
    ),
    af(
        "ducknng_unsubscribe_socket",
        "scalar",
        SKT,
        "unsubscribe_socket(socket_id, prefix)",
        "STRUCT(ok,error,...)",
        "Unsubscribe a SUB socket from a topic prefix.",
    ),
    af(
        "ducknng_list_sockets",
        "table",
        SKT,
        "list_sockets()",
        "table",
        "List all open NNG sockets with kind and state.",
    ),
    # ---- Framed RPC ----
    af(
        "ducknng_get_rpc_manifest",
        "table",
        RPC,
        "get_rpc_manifest(url, tls_config_id)",
        "table",
        "Request the RPC method manifest from a remote server.",
    ),
    af(
        "ducknng_query_rpc",
        "table",
        RPC,
        "query_rpc(url, sql, tls_config_id)",
        "table",
        "Run read-only or idempotent remote SQL through a query session and return its rows.",
    ),
    af(
        "ducknng_query_rpc_params",
        "table",
        RPC,
        "query_rpc_params(url, sql, params, tls_config_id)",
        "table",
        "Run one parameterized read-only or idempotent remote statement and return its rows.",
    ),
    af(
        "ducknng_prepare_query",
        "table",
        RPC,
        "prepare_query(url, sql, tls_config_id)",
        "table",
        "Prepare one remote statement without executing it and expose its result schema.",
    ),
    af(
        "ducknng_prepare_query_params",
        "table",
        RPC,
        "prepare_query_params(url, sql, params, tls_config_id)",
        "table",
        "Bind a typed parameter tuple and expose the prepared remote result schema without execution.",
    ),
    af(
        "ducknng_upload_table",
        "table",
        RPC,
        "upload_table(url, source_query, target_table[, tls_config_id])",
        "table",
        "Run source_query locally and stream its rows into a remote table over the quack upload lane (upload_open/append/commit, abort on error). Returns rows_uploaded and bytes_uploaded.",
    ),
    af(
        "ducknng_run_rpc",
        "table",
        RPC,
        "run_rpc(url, sql, tls_config_id)",
        "table",
        "Execute SQL via the exec method. Returns rows_changed + metadata.",
    ),
    af(
        "ducknng_run_rpc_params",
        "table",
        RPC,
        "run_rpc_params(url, sql, params, tls_config_id)",
        "table",
        "Execute one parameterized statement through exec and return rows_changed plus metadata.",
    ),
    af(
        "ducknng_open_query",
        "table",
        RPC,
        "open_query(url, sql, batch_rows, batch_bytes, tls_config_id)",
        "table",
        "Open a remote query session. Returns session_id + session_token needed for fetch/close/cancel.",
    ),
    af(
        "ducknng_fetch_query_table",
        "table",
        RPC,
        "fetch_query_table(url, session_id, session_token, batch_rows, batch_bytes, tls_config_id)",
        "table",
        "Fetch the next batch of rows from an open query session. Returns decoded Arrow IPC rows.",
    ),
    af(
        "ducknng_close_query",
        "table",
        RPC,
        "close_query(url, session_id, session_token, tls_config_id)",
        "table",
        "Close a remote query session and release server-side resources.",
    ),
    af(
        "ducknng_cancel_query",
        "table",
        RPC,
        "cancel_query(url, session_id, session_token, tls_config_id)",
        "table",
        "Cancel a running query on a remote session.",
    ),
    af(
        "ducknng_request_raw",
        "scalar",
        RPC,
        "request_raw(url, data, timeout_ms, tls_config_id)",
        "BLOB",
        "Send a raw NNG request and return the reply as a BLOB frame.",
    ),
    af(
        "ducknng_request_socket_raw",
        "scalar",
        RPC,
        "request_socket_raw(socket_id, data, timeout_ms)",
        "BLOB",
        "Send a raw request on a connected socket and return the reply frame.",
    ),
    af(
        "ducknng_get_rpc_manifest_raw",
        "scalar",
        RPC,
        "get_rpc_manifest_raw(url, tls_config_id)",
        "BLOB",
        "Request the manifest and return the raw reply frame.",
    ),
    af(
        "ducknng_open_query_raw",
        "scalar",
        RPC,
        "open_query_raw(url, sql, batch_rows, batch_bytes, tls_config_id)",
        "BLOB",
        "Open a query session and return the raw reply frame.",
    ),
    af(
        "ducknng_fetch_query_raw",
        "scalar",
        RPC,
        "fetch_query_raw(url, session_id, session_token, batch_rows, batch_bytes, tls_config_id)",
        "BLOB",
        "Fetch next batch and return the raw Arrow IPC frame.",
    ),
    af(
        "ducknng_close_query_raw",
        "scalar",
        RPC,
        "close_query_raw(url, session_id, session_token, tls_config_id)",
        "BLOB",
        "Close a session and return the raw reply frame.",
    ),
    af(
        "ducknng_cancel_query_raw",
        "scalar",
        RPC,
        "cancel_query_raw(url, session_id, session_token, tls_config_id)",
        "BLOB",
        "Cancel a query and return the raw reply frame.",
    ),
    # ---- HTTP Client ----
    af(
        "ducknng_ncurl",
        "table",
        HTTP,
        "ncurl(url, method, headers_json, body, timeout_ms, tls_config_id)",
        "table",
        "Perform an HTTP request. Returns status, headers_json, body BLOB, body_text VARCHAR.",
    ),
    af(
        "ducknng_ncurl_table",
        "table",
        HTTP,
        "ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id)",
        "table",
        "HTTP request with auto-parsed response body via Content-Type codec dispatch. Requires 2xx status.",
    ),
    # ---- HTTP Routes ----
    af(
        "ducknng_register_http_route",
        "scalar",
        HTR,
        "register_http_route(service_name, method, path, handler_sql)",
        "BOOLEAN",
        "Register an exact-match HTTP route. Handler SQL must return status, content_type, body or body_text columns.",
    ),
    af(
        "ducknng_register_http_route_pattern",
        "scalar",
        HTR,
        "register_http_route_pattern(service_name, method, match_kind, path, handler_sql)",
        "BOOLEAN",
        "Register prefix or template-matched route. match_kind: 'prefix' or 'template'. {param} accessible via ducknng_http_path_param.",
    ),
    af(
        "ducknng_add_stream_route",
        "scalar",
        HTR,
        "add_stream_route(service_name, method, path, handler_sql, content_type)",
        "BOOLEAN",
        "Register a chunked-streaming route. Handler SQL must return 'chunk' column. Each row written as HTTP chunk. Default content-type: text/event-stream.",
    ),
    af(
        "ducknng_register_http_static",
        "scalar",
        HTR,
        "register_http_static(service_name, url_prefix, directory_path)",
        "BOOLEAN",
        "Serve static files from a directory under a URL prefix.",
    ),
    af(
        "ducknng_unregister_http_route",
        "scalar",
        HTR,
        "unregister_http_route(service_name, method, path)",
        "BOOLEAN",
        "Unregister a previously registered HTTP route.",
    ),
    af(
        "ducknng_list_http_routes",
        "table",
        HTR,
        "list_http_routes()",
        "table",
        "List all registered HTTP routes with method, path, match kind, auth policy, streaming mode.",
    ),
    # ---- HTTP Workers ----
    af(
        "ducknng_register_http_worker",
        "scalar",
        HTW,
        "register_http_worker(service_name, worker_name, sql, interval_ms)",
        "BOOLEAN",
        "Register a background worker that executes SQL on a recurring interval (milliseconds).",
    ),
    af(
        "ducknng_unregister_http_worker",
        "scalar",
        HTW,
        "unregister_http_worker(service_name, worker_name)",
        "BOOLEAN",
        "Unregister a background worker.",
    ),
    af(
        "ducknng_list_http_workers",
        "table",
        HTW,
        "list_http_workers()",
        "table",
        "List all registered HTTP workers with their interval and last-run timestamp.",
    ),
    # ---- Body Codecs ----
    af(
        "ducknng_parse_body",
        "table",
        BDY,
        "parse_body(body, content_type)",
        "table",
        "Parse a BLOB using the supplied content type. Supports JSON, NDJSON, CSV, TSV, Parquet, Arrow IPC, form-urlencoded, ducknng frames.",
    ),
    af(
        "ducknng_parse_csv",
        "table",
        BDY,
        "parse_csv(body)",
        "table",
        "Parse a CSV BLOB via DuckDB read_csv_auto through a tempfile round-trip.",
    ),
    af(
        "ducknng_parse_tsv",
        "table",
        BDY,
        "parse_tsv(body)",
        "table",
        "Parse a TSV BLOB via DuckDB read_csv_auto(delim='\t') through a tempfile round-trip.",
    ),
    af(
        "ducknng_parse_parquet",
        "table",
        BDY,
        "parse_parquet(body)",
        "table",
        "Parse a Parquet BLOB via DuckDB read_parquet through a tempfile round-trip.",
    ),
    af(
        "ducknng_list_codecs",
        "table",
        BDY,
        "list_codecs()",
        "table",
        "List all built-in and user-registered body codec providers.",
    ),
    af(
        "ducknng_register_codec",
        "scalar",
        BDY,
        "register_codec(content_type, function_name)",
        "BOOLEAN",
        "Register a scalar SQL function as a custom body codec for the given content type.",
    ),
    af(
        "ducknng_unregister_codec",
        "scalar",
        BDY,
        "unregister_codec(content_type)",
        "BOOLEAN",
        "Unregister a user-registered body codec, restoring the built-in behavior.",
    ),
    # ---- TLS ----
    af(
        "ducknng_self_signed_tls_config",
        "scalar",
        TLS,
        "self_signed_tls_config(host, days, auth_mode)",
        "UBIGINT",
        "Generate a self-signed TLS cert in memory. In client mode auth_mode 0 verifies the remote server; on listeners auth_mode 2 requires mTLS. Returns TLS config handle.",
    ),
    af(
        "ducknng_tls_config_from_files",
        "scalar",
        TLS,
        "tls_config_from_files(cert_path, key_path, ca_path, auth_mode)",
        "UBIGINT",
        "Create TLS config from PEM file paths.",
    ),
    af(
        "ducknng_tls_config_from_pem",
        "scalar",
        TLS,
        "tls_config_from_pem(cert_pem, key_pem, ca_pem, hostname, auth_mode)",
        "UBIGINT",
        "Create TLS config from in-memory PEM text (no file I/O).",
    ),
    af(
        "ducknng_drop_tls_config",
        "scalar",
        TLS,
        "drop_tls_config(tls_config_id)",
        "BOOLEAN",
        "Drop a TLS config and release its certificate memory.",
    ),
    af(
        "ducknng_list_tls_configs",
        "table",
        TLS,
        "list_tls_configs()",
        "table",
        "List all created TLS configs with auth mode.",
    ),
    # ---- Service ----
    af(
        "ducknng_set_service_limits",
        "scalar",
        SVC,
        "set_service_limits(name, max_memory, max_sessions, max_result_bytes, max_send_bytes, max_recv_bytes, max_pipes, query_timeout_ms)",
        "BOOLEAN",
        "Set resource limits for a service. All params after name are UBIGINT with 0 = no limit.",
    ),
    af(
        "ducknng_set_service_execution_model",
        "scalar",
        SVC,
        "set_service_execution_model(name, model)",
        "BOOLEAN",
        "Set execution model: 'shared_serialized_connection', 'service_serialized_connection', or 'request_connection'.",
    ),
    # ---- Admission ----
    af(
        "ducknng_set_service_peer_allowlist",
        "scalar",
        ATH,
        "set_service_peer_allowlist(name, identity_regex)",
        "BOOLEAN",
        "Restrict a service to mTLS peer identities matching a regex.",
    ),
    af(
        "ducknng_set_service_ip_allowlist",
        "scalar",
        ATH,
        "set_service_ip_allowlist(name, cidr_list)",
        "BOOLEAN",
        "Restrict a service to clients matching IP/CIDR patterns. Semicolon-separated list.",
    ),
    af(
        "ducknng_set_http_route_auth",
        "scalar",
        ATH,
        "set_http_route_auth(service_name, method, path, require_auth, auth_allow_identities)",
        "BOOLEAN",
        "Set route-level auth policy. require_auth: BOOLEAN. allow_identities: comma-separated peer identity regexes.",
    ),
    af(
        "ducknng_set_service_authorizer",
        "scalar",
        ATH,
        "set_service_authorizer(name, handler_sql)",
        "BOOLEAN",
        "Register a SQL authorizer callback for a service. The handler receives query text and returns allow/deny.",
    ),
    af(
        "ducknng_auth_context",
        "table",
        ATH,
        "auth_context()",
        "table",
        "Return the current request's auth context: peer_identity, peer_addr, authenticated columns.",
    ),
    # ---- Monitoring ----
    af(
        "ducknng_read_monitor",
        "table",
        MON,
        "read_monitor(name, after_seq, max_events)",
        "table",
        "Read pipe events from a service's event monitor ring buffer.",
    ),
    af(
        "ducknng_monitor_status",
        "table",
        MON,
        "monitor_status(name)",
        "table",
        "Return monitor ring buffer capacity, event counts, active/max pipes.",
    ),
    af(
        "ducknng_list_pipes",
        "table",
        MON,
        "list_pipes(name)",
        "table",
        "List currently open NNG pipes for a service.",
    ),
    af(
        "ducknng_log_entries",
        "table",
        MON,
        "log_entries()",
        "table",
        "Read the DuckDB log ring buffer (requires ducknng_enable_log_capture()).",
    ),
    af(
        "ducknng_enable_log_capture",
        "scalar",
        MON,
        "enable_log_capture()",
        "BOOLEAN",
        "Enable DuckDB log ring capture for the current session.",
    ),
    # ---- Async I/O ----
    af(
        "ducknng_send_socket_raw_aio",
        "scalar",
        AIO,
        "send_socket_raw_aio(socket_id, data, timeout_ms)",
        "UBIGINT",
        "Launch an async raw socket send. Returns aio_id to collect with ducknng_aio_collect.",
    ),
    af(
        "ducknng_recv_socket_raw_aio",
        "scalar",
        AIO,
        "recv_socket_raw_aio(socket_id, timeout_ms)",
        "UBIGINT",
        "Launch an async raw socket recv. Returns aio_id.",
    ),
    af(
        "ducknng_request_raw_aio",
        "scalar",
        AIO,
        "request_raw_aio(url, data, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async raw RPC request. Returns aio_id.",
    ),
    af(
        "ducknng_request_socket_raw_aio",
        "scalar",
        AIO,
        "request_socket_raw_aio(socket_id, data, timeout_ms)",
        "UBIGINT",
        "Launch an async request on a connected socket. Returns aio_id.",
    ),
    af(
        "ducknng_run_rpc_raw_aio",
        "scalar",
        AIO,
        "run_rpc_raw_aio(url, sql, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async exec RPC. Returns aio_id.",
    ),
    af(
        "ducknng_get_rpc_manifest_raw_aio",
        "scalar",
        AIO,
        "get_rpc_manifest_raw_aio(url, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async manifest request. Returns aio_id.",
    ),
    af(
        "ducknng_open_query_raw_aio",
        "scalar",
        AIO,
        "open_query_raw_aio(url, sql, batch_rows, batch_bytes, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async query session open. Returns aio_id.",
    ),
    af(
        "ducknng_fetch_query_raw_aio",
        "scalar",
        AIO,
        "fetch_query_raw_aio(url, session_id, session_token, batch_rows, batch_bytes, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async batch fetch. Returns aio_id.",
    ),
    af(
        "ducknng_close_query_raw_aio",
        "scalar",
        AIO,
        "close_query_raw_aio(url, session_id, session_token, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async session close. Returns aio_id.",
    ),
    af(
        "ducknng_cancel_query_raw_aio",
        "scalar",
        AIO,
        "cancel_query_raw_aio(url, session_id, session_token, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async cancel. Returns aio_id.",
    ),
    af(
        "ducknng_ncurl_aio",
        "scalar",
        AIO,
        "ncurl_aio(url, method, headers_json, body, timeout_ms, tls_config_id)",
        "UBIGINT",
        "Launch an async HTTP request. Returns aio_id.",
    ),
    af(
        "ducknng_aio_ready",
        "scalar",
        AIO,
        "aio_ready(aio_id)",
        "BOOLEAN",
        "Check if an async operation has completed.",
    ),
    af(
        "ducknng_aio_cancel",
        "scalar",
        AIO,
        "aio_cancel(aio_id)",
        "BOOLEAN",
        "Cancel a pending async operation.",
    ),
    af(
        "ducknng_aio_drop",
        "scalar",
        AIO,
        "aio_drop(aio_id)",
        "BOOLEAN",
        "Drop an aio handle and release its resources.",
    ),
    af(
        "ducknng_aio_wait",
        "scalar",
        AIO,
        "aio_wait(aio_ids, timeout_ms)",
        "BOOLEAN",
        "Block until all aio_ids complete or timeout. aio_ids: LIST(UBIGINT).",
    ),
    af(
        "ducknng_aio_collect",
        "table_macro",
        AIO,
        "aio_collect(aio_ids, wait_ms)",
        "table",
        "Collect async RPC results into decoded frame table.",
    ),
    af(
        "ducknng_aio_collect_decoded",
        "table_macro",
        AIO,
        "aio_collect_decoded(aio_ids, wait_ms)",
        "table",
        "Collect and decode ASYNC RPC aio results into a decoded frame rows table.",
    ),
    af(
        "ducknng_ncurl_aio_collect",
        "table_macro",
        AIO,
        "ncurl_aio_collect(aio_ids, wait_ms)",
        "table",
        "Collect async HTTP results into ncurl-style table.",
    ),
    # ---- Method Registry ----
    af(
        "ducknng_list_methods",
        "table",
        REG,
        "list_methods()",
        "table",
        "List all registered RPC methods with auth requirements.",
    ),
    af(
        "ducknng_register_exec_method",
        "scalar",
        REG,
        "register_exec_method(enable_default)",
        "BOOLEAN",
        "Register (or re-register) the default exec method. Pass TRUE to enable by default.",
    ),
    af(
        "ducknng_register_upload_methods",
        "scalar",
        REG,
        "register_upload_methods(require_auth)",
        "BOOLEAN",
        "Register (or re-register) the upload lane methods (upload_open/append/commit/abort). Pass TRUE to require auth.",
    ),
    af(
        "ducknng_set_method_auth",
        "scalar",
        REG,
        "set_method_auth(method_name, require_auth)",
        "BOOLEAN",
        "Set auth requirement for an RPC method.",
    ),
    af(
        "ducknng_unregister_method",
        "scalar",
        REG,
        "unregister_method(method_name)",
        "BOOLEAN",
        "Unregister a single RPC method.",
    ),
    af(
        "ducknng_unregister_family",
        "scalar",
        REG,
        "unregister_family(family_name)",
        "UBIGINT",
        "Unregister all methods in a family. Returns count of unregistered methods.",
    ),
    # ---- Frame Helpers ----
    af(
        "ducknng_decode_frame",
        "table",
        FRM,
        "decode_frame(data)",
        "table",
        "Decode a ducknng protocol frame into type, name, payload_text, error_text, and raw payload.",
    ),
    af(
        "ducknng_frame_payload",
        "scalar",
        FRM,
        "frame_payload(data)",
        "BLOB",
        "Extract the frame payload BLOB from a ducknng protocol frame.",
    ),
    af(
        "ducknng_frame_payload_text",
        "scalar",
        FRM,
        "frame_payload_text(data)",
        "VARCHAR",
        "Decode frame payload as UTF-8 text.",
    ),
    af(
        "ducknng_frame_error_text",
        "scalar",
        FRM,
        "frame_error_text(data)",
        "VARCHAR",
        "Extract the error message from a ducknng error frame.",
    ),
    af(
        "ducknng_frame_version",
        "scalar",
        FRM,
        "frame_version(data)",
        "UTINYINT",
        "Extract protocol version from a ducknng frame.",
    ),
    af(
        "ducknng_frame_type",
        "scalar",
        FRM,
        "frame_type(data)",
        "UTINYINT",
        "Extract numeric type code from a ducknng frame.",
    ),
    af(
        "ducknng_frame_flags",
        "scalar",
        FRM,
        "frame_flags(data)",
        "UINTEGER",
        "Extract flags field from a ducknng frame.",
    ),
    af(
        "ducknng_frame_type_name",
        "scalar",
        FRM,
        "frame_type_name(data)",
        "VARCHAR",
        "Decode the human-readable type name from a ducknng frame.",
    ),
    af(
        "ducknng_frame_name",
        "scalar",
        FRM,
        "frame_name(data)",
        "VARCHAR",
        "Extract the method name from a ducknng query/call frame.",
    ),
    af(
        "ducknng_frame_end_of_stream",
        "scalar",
        FRM,
        "frame_end_of_stream(data)",
        "BOOLEAN",
        "Check if a ducknng frame is an end-of-stream marker.",
    ),
    # ---- SQL Macros ----
    af(
        "ducknng_format_sse",
        "scalar_macro",
        MAC,
        "format_sse(data, event := NULL, id := NULL, retry := NULL)",
        "VARCHAR",
        "Format a Server-Sent Events event string with optional event type, id, retry fields.",
    ),
    af(
        "ducknng_http_response",
        "table_macro",
        MAC,
        "http_response(status, headers_json, content_type, body, body_text)",
        "table",
        "Build an HTTP route response row with explicit status, headers, content-type, and body.",
    ),
    af(
        "ducknng_http_text",
        "table_macro",
        MAC,
        "http_text(status, body_text)",
        "table",
        "Build a text/plain HTTP route response.",
    ),
    af(
        "ducknng_http_json",
        "table_macro",
        MAC,
        "http_json(status, body_text)",
        "table",
        "Build an application/json HTTP route response.",
    ),
    af(
        "ducknng_http_binary",
        "table_macro",
        MAC,
        "http_binary(status, body)",
        "table",
        "Build an application/octet-stream HTTP route response.",
    ),
    af(
        "ducknng_http_ndjson",
        "table_macro",
        MAC,
        "http_ndjson(status, body_text)",
        "table",
        "Build an application/x-ndjson HTTP route response.",
    ),
    af(
        "ducknng_http_headers_get",
        "scalar",
        HTH,
        "http_headers_get(headers_json, name)",
        "VARCHAR",
        "Extract a header value from an ncurl-style headers JSON array.",
    ),
    af(
        "ducknng_http_header",
        "scalar_macro",
        HTH,
        "http_header(name)",
        "VARCHAR",
        "Extract a request header inside an HTTP route handler.",
    ),
    af(
        "ducknng_http_query_param",
        "scalar_macro",
        HTH,
        "http_query_param(name)",
        "VARCHAR",
        "Extract a URL query parameter inside an HTTP route handler.",
    ),
    af(
        "ducknng_http_cookie",
        "scalar_macro",
        HTH,
        "http_cookie(name)",
        "VARCHAR",
        "Extract a cookie value inside an HTTP route handler.",
    ),
    af(
        "ducknng_http_path_param",
        "scalar_macro",
        HTH,
        "http_path_param(name)",
        "VARCHAR",
        "Extract a template path parameter inside an HTTP route handler.",
    ),
    af(
        "ducknng_http_query_param_get",
        "scalar",
        HTH,
        "http_query_param_get(headers_or_query, name)",
        "VARCHAR",
        "Extract a query parameter from a URL query string.",
    ),
    af(
        "ducknng_http_cookie_get",
        "scalar",
        HTH,
        "http_cookie_get(cookie_str, name)",
        "VARCHAR",
        "Extract a cookie value from a Cookie header string.",
    ),
    af(
        "ducknng_http_path_params_get",
        "scalar",
        HTH,
        "http_path_params_get(path_params_json, name)",
        "VARCHAR",
        "Extract a path parameter from a JSON object.",
    ),
    af(
        "ducknng_http_headers_build",
        "scalar",
        HTH,
        "http_headers_build(names, values)",
        "VARCHAR",
        "Build an ncurl-style headers JSON array from two LIST(VARCHAR) columns.",
    ),
    # ---- Service (additional) ----
    af(
        "ducknng_set_tls_peer_allowlist",
        "scalar",
        TLS,
        "set_tls_peer_allowlist(tls_config_id, identity_regex)",
        "BOOLEAN",
        "Set peer identity allowlist on a TLS config for server-side mTLS filtering.",
    ),
]


def hello_world():
    sql = (
        "    -- Load the extension\n    LOAD ducknng;\n\n"
        "    -- Start an inproc REP server and run remote SQL\n"
        "    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);\n\n"
        "    -- ducknng_query_rpc returns the actual result rows\n"
        "    SELECT *\n"
        "    FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);\n\n"
        "    SELECT ducknng_stop_server('demo');"
    )
    if not EXT_BIN.exists() or "--render" not in sys.argv:
        return sql
    try:
        sql_script = (
            f"LOAD '{EXT_BIN}';\nSELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);\n"
            f"SELECT * FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);\n"
            f"SELECT ducknng_stop_server('demo');"
        )
        result = subprocess.run(
            ["/usr/local/bin/duckdb152", "-unsigned", "-c", sql_script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        raw = result.stdout
    except Exception:
        return sql
    tables = []
    current = []
    in_table = False
    for line in raw.splitlines():
        if line.startswith("┌"):
            in_table = True
            current = [line]
        elif line.startswith("└") and in_table:
            current.append(line)
            tables.append("\n".join(current))
            current = []
            in_table = False
        elif in_table:
            current.append(line)
    out = []
    out.append("    -- Load the extension")
    out.append("    LOAD ducknng;")
    out.append("")
    out.append("    -- Start an inproc REP server and run remote SQL")
    out.append(
        "    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);"
    )
    out.append("")
    if len(tables) > 0:
        out.extend(f"    {l}" for l in tables[0].splitlines())
    out.append("")
    out.append("    -- ducknng_query_rpc returns the actual result rows")
    out.append("    SELECT *")
    out.append(
        "    FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);"
    )
    out.append("")
    if len(tables) > 1:
        out.extend(f"    {l}" for l in tables[1].splitlines())
    out.append("")
    out.append("    SELECT ducknng_stop_server('demo');")
    out.append("")
    if len(tables) > 2:
        out.extend(f"    {l}" for l in tables[2].splitlines())
    return "\n".join(out)


def dump_yaml(val, indent=0):
    sp = "  " * indent
    if isinstance(val, bool):
        return f"{sp}{str(val).lower()}"
    if isinstance(val, str):
        if "\n" in val:
            return f"{sp}|\n" + "\n".join(f"{sp}{l}" for l in val.splitlines())
        if ":" in val or any(c in val for c in "{}[]&*?|>!%@`,"):
            return f'{sp}"{val}"'
        return f"{sp}{val}"
    if isinstance(val, list):
        items = []
        for v in val:
            if isinstance(v, dict):
                items.append(f"{sp}-")
                items.append(dump_yaml(v, indent + 1))
            elif isinstance(v, str) and "\n" not in v:
                items.append(f'{sp}- "{v}"')
            else:
                items.append(f"{sp}- {v}")
        return "\n".join(items)
    if isinstance(val, dict):
        items = []
        for k, v in val.items():
            prefix = f"{sp}{k}:"
            child = dump_yaml(v, indent + 1)
            if child.strip():
                items.append(f"{prefix}\n{child}")
            else:
                items.append(prefix)
        return "\n".join(items)
    return f"{sp}{val}" if val is not None else f"{sp}~"


def build_description_yml():
    desc = {
        "extension": EXTENSION,
        # Pin the community source to the advertised release tag, not a volatile
        # dev HEAD: a committed file cannot contain its own commit SHA (committing
        # changes HEAD), so a tag is the only stable, non-stale ref.
        "repo": {"github": f"{EXTENSION['maintainers'][0]}/ducknng", "ref": f"v{EXTENSION['version']}"},
        "docs": {
            "hello_world": hello_world(),
            "extended_description": textwrap.dedent("""\
                ducknng is a pure C DuckDB extension that exposes a DuckDB-backed SQL and
                RPC server over NNG (Nanomsg Next Generation) using Arrow IPC with nanoarrow C
                for payload encoding and decoding.

                **Transport layer (NNG)**
                Supports inproc://, ipc://, tcp://, and tls+tcp:// URLs. TLS certificates
                can be loaded from file paths or in-memory PEM content; self-signed dev
                certificates are generated entirely inside the extension (no file I/O).

                **Framed RPC**
                Versioned request/reply envelope with manifest, exec, query session
                (open/fetch/close/cancel), and raw unary operations. All tabular data
                is encoded as Arrow IPC streams.

                **HTTP carrier**
                Start a server on an http:// or https:// URL for the framed RPC mount.
                Register custom HTTP routes (exact, prefix, or template matching) backed
                by SQL queries. Streaming chunked routes for Server-Sent Events are
                supported via ducknng_add_stream_route. Static asset serving, route-local
                auth policies, and background workers are also available.

                **Body codec layer**
                Parse HTTP response bodies by content type: JSON, NDJSON, CSV, TSV,
                Parquet, Arrow IPC, form-urlencoded, and ducknng frames. Standalone
                ducknng_parse_csv(body), ducknng_parse_tsv(body), and
                ducknng_parse_parquet(body) functions use DuckDB's standard readers
                via a tempfile round-trip. User-registered codec hooks extend the set.

                **Admission & security**
                mTLS peer-identity extraction, exact identity allowlists, IP/CIDR
                allowlists, per-service and per-principal resource limits (max memory,
                max sessions, max result bytes), and SQL authorizer callbacks.

                **Development & testing**
                Built against the DuckDB C API (no C++). Uses DuckDB's stable and
                unstable C extensions API for Arrow conversion. 20+ SQL integration
                tests run via sqllogictest. Cross-platform (Linux, macOS, Windows)
                via the extension-ci-tools CMake build system.

                Project details and examples: https://github.com/sounkou-bioinfo/ducknng

                Community package excludes WASM targets (NNG threading requirement) and Windows MSVC (use MinGW/Rtools).""").strip(),
        },
    }
    return dump_yaml(desc) + "\n"


def build_functions_json():
    """functions.yaml is JSON (despite .yaml ext) — same format as duckhts."""
    doc = {
        "manifest_version": 1,
        "community_extension": {
            "extension": EXTENSION,
            "repo": {
                "github": f"{EXTENSION['maintainers'][0]}/ducknng",
                "ref_source": "git_head",
            },
            "docs": {
                "hello_world_lines": [
                    "LOAD ducknng;",
                    "",
                    "SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);",
                    "",
                    "SELECT * FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);",
                    "",
                    "SELECT ducknng_stop_server('demo');",
                ],
                "extended_intro": [
                    "ducknng is a pure C DuckDB extension that exposes a DuckDB-backed SQL and RPC server over NNG (Nanomsg Next Generation) using Arrow IPC with nanoarrow C for payload encoding and decoding.",
                    "All features shown in this README are implemented, tested, and runnable.",
                ],
                "feature_notes": [
                    "Only linux_amd64, osx_amd64, and osx_arm64 are currently tested and supported; WASM targets cannot use NNG threading; Windows MSVC builds fail on the MbedTLS dependency (use MinGW/Rtools on Windows).",
                    "The extension uses both stable and unstable DuckDB C extension APIs for Arrow conversion.",
                    "TLS configs accept PEM text in-memory (no file I/O) or file paths.",
                    "HTTP transport does not yet support http/2.",
                ],
            },
        },
        "functions": FUNCTIONS,
    }
    return json.dumps(doc, indent=2) + "\n"


def build_docs_markdown():
    """Generate a markdown function reference section from FUNCTIONS catalog."""
    categories = {}
    for fn in FUNCTIONS:
        cat = fn["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(fn)
    lines = []
    lines.append("## Function reference")
    lines.append("")
    for cat in [
        "Server",
        "NNG Sockets",
        "Framed RPC",
        "HTTP Client",
        "HTTP Routes",
        "HTTP Workers",
        "Async I/O",
        "Body Codecs",
        "TLS",
        "Service",
        "Admission",
        "Monitoring",
        "Method Registry",
        "Frame Helpers",
        "SQL Macros",
    ]:
        if cat not in categories:
            continue
        lines.append(f"### {cat}")
        lines.append("")
        for fn in categories[cat]:
            kind_icon = {
                "table": "📋",
                "scalar": "🔢",
                "table_macro": "📋",
                "scalar_macro": "🔢",
            }.get(fn["kind"], "🔢")
            sig = fn["signature"].split("(", 1)
            fn_name = sig[0]
            fn_args = sig[1] if len(sig) > 1 else ""
            # Remove ducknng_ prefix for link target
            anchor = fn["name"].replace("_", "-")
            lines.append(f"#### `{fn['name']}({fn_args}`")
            lines.append("")
            lines.append(f"{fn['description']}")
            lines.append("")
            if fn["examples"]:
                for ex in fn["examples"][:1]:
                    lines.append(f"```sql")
                    lines.append(f"{ex}")
                    lines.append(f"```")
                    lines.append("")
    return "\n".join(lines)


def main():
    # Report stats
    categories = {}
    for fn in FUNCTIONS:
        categories.setdefault(fn["category"], []).append(fn)
    total_fns = len(FUNCTIONS)
    by_kind = {}
    for fn in FUNCTIONS:
        by_kind[fn["kind"]] = by_kind.get(fn["kind"], 0) + 1

    # Build the community-extensions descriptor (nested submission format). It is
    # written only to the community-extensions + PR dirs below, NOT to repo-root
    # description.yml: that root file is the minimal build/version descriptor
    # parsed by function_catalog/generate_function_catalog.py (flat key: value),
    # and overwriting it with this nested format breaks that parser.
    desc = build_description_yml()
    print(
        f"description.yml (community): {len(FUNCTIONS)} functions across {len(categories)} categories: "
        + ", ".join(f"{k}={len(v)}" for k, v in sorted(categories.items()))
    )

    # Write functions.yaml (JSON)
    fn_json = build_functions_json()
    (REPO_ROOT / "functions.yaml").write_text(fn_json)
    print(
        f"functions.yaml: {len(fn_json)} bytes, {total_fns} entries "
        + f"({', '.join(f'{k}={v}' for k, v in sorted(by_kind.items()))})"
    )

    # Write to community-extensions dir
    COMMUNITY_DIR.mkdir(parents=True, exist_ok=True)
    (COMMUNITY_DIR / "description.yml").write_text(desc)
    print(f"-> community-extensions/extensions/ducknng/description.yml")

    # Write to PR dir
    pr_dir = Path("/tmp/community-extensions/extensions/ducknng")
    pr_dir.mkdir(parents=True, exist_ok=True)
    (pr_dir / "description.yml").write_text(desc)
    print(f"-> /tmp/community-extensions/extensions/ducknng/description.yml (for PR)")

    # Optionally write docs markdown
    if "--docs" in sys.argv:
        md = build_docs_markdown()
        DOCS_DIR.mkdir(parents=True, exist_ok=True)
        (DOCS_DIR / "function_reference.md").write_text(md)
        print(f"-> docs/function_reference.md ({len(md)} bytes)")


if __name__ == "__main__":
    main()
