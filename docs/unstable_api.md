# DuckDB Unstable API Audit

This document records findings from a systematic review of all `unstable_*` API groups in
`duckdb_extension.h` and `duckdb.h`, with a recommendation per group on whether and how
`ducknng` should use each one. Because these APIs pin binaries to one exact DuckDB version, the operational branch and binary-release workflow is documented in `docs/release_branches.md`.

---

## `unstable_new_arrow_functions`

**Functions:** `duckdb_to_arrow_schema`, `duckdb_data_chunk_to_arrow`,
`duckdb_schema_from_arrow`, `duckdb_data_chunk_from_arrow`,
`duckdb_destroy_arrow_converted_schema`

**What it does.** Converts between `duckdb_data_chunk` / `duckdb_logical_type` and
nanoarrow `ArrowSchema` / `ArrowArray` using DuckDB's own type mapping rather than a
hand-written translation table.

**Current situation.** `ducknng_ipc_out.c` previously built Arrow IPC from DuckDB result chunks using a manually maintained `ducknng_set_arrow_schema_type` switch (~530 lines of hand-written per-type dispatch). That path has been replaced: `duckdb_to_arrow_schema` generates the Arrow schema from the DuckDB result's logical types, and `duckdb_data_chunk_to_arrow` converts each fetched chunk. `ducknng_ipc_in.c` and `ducknng_sql_arrow.c` handle the receive direction and continue to use nanoarrow directly (the receive-side rewrite requires a `duckdb_connection` inside scan callbacks, which is not available without additional architectural changes).

**Status: ADOPTED (emit side).** `duckdb_to_arrow_schema` and `duckdb_data_chunk_to_arrow` are in use in `src/ducknng_ipc_out.c`. `duckdb_schema_from_arrow` and `duckdb_data_chunk_from_arrow` are deferred pending an architectural solution for threading a connection into TVF scan callbacks.

---

## `unstable_new_string_functions`

**Functions:** `duckdb_valid_utf8_check(str, len)`, `duckdb_value_to_string`,
`duckdb_unsafe_vector_assign_string_element_len` (in the vector group)

**What it does.** `duckdb_valid_utf8_check` validates UTF-8 in-place.
`duckdb_unsafe_vector_assign_string_element_len` assigns a string element with a known
length, avoiding an internal `strlen` call.

**Status: ADOPTED.**

`ducknng_sql_bytes_look_text` in `src/ducknng_sql_api.c` now uses `duckdb_valid_utf8_check`
as the UTF-8 gate. The function retains a fast pre-pass that rejects null bytes and
disallowed control characters (below U+0020 except tab, line feed, carriage return) before
delegating the multi-byte UTF-8 validation to DuckDB.

`duckdb_unsafe_vector_assign_string_element_len` is now used in all vector string-assignment
sites across the codebase:

- `src/ducknng_sql_arrow.c`: Arrow IPC `STRING` and `LARGE_STRING` scan paths (Arrow IPC
  guarantees UTF-8 for these column types, so skipping the internal UTF-8 check is correct
  by the Arrow specification).
- `src/ducknng_sql_api.c` and `src/ducknng_sql_arrow.c`: BLOB assignments (BLOB is opaque
  binary; UTF-8 validation is inappropriate).
- All remaining `duckdb_vector_assign_string_element` call sites across
  `ducknng_sql_http.c`, `ducknng_sql_registry.c`, `ducknng_sql_tls.c`,
  `ducknng_sql_service.c`, `ducknng_sql_body.c`, `ducknng_sql_aio.c`,
  `ducknng_sql_session.c`, `ducknng_sql_socket.c`, `ducknng_sql_monitor.c`,
  `ducknng_sql_rpc.c`, and `ducknng_sql_auth.c`: these assign either C string literals,
  internal struct fields populated from C code, or values extracted from DuckDB VARCHAR
  inputs (already validated as UTF-8 by DuckDB). All are replaced with
  `duckdb_unsafe_vector_assign_string_element_len` paired with `strlen`.

---

## `unstable_new_table_function_functions`

**Functions:** `duckdb_table_function_get_client_context(bind_info, out_ctx)`

**What it does.** Retrieves the DuckDB `duckdb_client_context` from inside a
table-function bind callback.

**Current situation.** All TVF bind callbacks in `ducknng` receive a
`ducknng_sql_context *` (the extension's own runtime/service state) via
`duckdb_bind_get_extra_info`. That struct carries the service, session map, TLS
config, and codec connection — none of which come from a DuckDB `duckdb_client_context`.
`duckdb_table_function_get_client_context` returns a DuckDB client context handle, not
the extension's state, so it cannot replace `extra_info` for `ducknng`'s purposes.

**Recommendation: NOT applicable.** The `extra_info` pattern must remain for
`ducknng_sql_context *` propagation. `duckdb_table_function_get_client_context` is only
useful if a TVF bind callback needs to call back into DuckDB's catalog or connection APIs
using the originating client context, which no current `ducknng` bind callback does.

---

## `unstable_new_scalar_function_functions`

**Functions:** `duckdb_scalar_function_set_bind`, `duckdb_scalar_function_bind_get_argument`,
`duckdb_scalar_function_bind_get_argument_count`, `duckdb_scalar_function_set_bind_data`,
`duckdb_scalar_function_set_bind_data_copy`, `duckdb_scalar_function_get_bind_data`,
`duckdb_scalar_function_get_client_context`, `duckdb_scalar_function_bind_set_error`,
`duckdb_scalar_function_bind_get_extra_info`

**What it does.** Adds a bind phase to scalar functions so they can inspect constant
arguments at planning time and constant-fold or specialise their execution path.

**Adopted.** The four HTTP lookup functions (`ducknng_http_headers_get`,
`ducknng_http_query_param_get`, `ducknng_http_cookie_get`,
`ducknng_http_path_params_get`) now register a bind callback
(`ducknng_http_lookup_bind_cb`) via `duckdb_scalar_function_set_bind`.
When the second argument (the lookup name) is a constant at planning time, the bind
callback folds it once using the expression fold API and stores the result in a
`ducknng_http_lookup_bind_data` struct attached with `duckdb_scalar_function_set_bind_data`.
The execute callbacks retrieve this with `duckdb_scalar_function_get_bind_data` and skip
the per-row `duckdb_malloc`/`memcpy` for the name when a pre-folded value is available.
Copy and destroy callbacks are registered for the bind data so it is correctly managed
across parallel plans.

The internal registration helper `ducknng_sql_register_scalar_logical_types_ex` now
accepts a `duckdb_scalar_function_bind_t bind_fn` parameter (NULL for all existing
functions). A new public function `ducknng_sql_register_volatile_scalar_with_bind` and
macro `DUCKNNG_REGISTER_VOLATILE_SCALAR_WITH_BIND` are exposed in
`src/include/ducknng_sql_shared.h`.

Pairs with `unstable_new_expression_functions` (`duckdb_expression_is_foldable`,
`duckdb_expression_fold`, `duckdb_destroy_expression`) which are used inside the bind
callback to test and fold constant arguments.

---

## `unstable_new_open_connect_functions`

**Functions:** `duckdb_connection_get_client_context`,
`duckdb_connection_get_arrow_options`, `duckdb_client_context_get_connection_id`,
`duckdb_destroy_client_context`, `duckdb_destroy_arrow_options`, `duckdb_get_table_names`

**What it does.** Exposes the internal client context from a `duckdb_connection` handle,
provides a connection-level unique ID, retrieves per-connection Arrow options, and lists
table names reachable from a connection.

**Current situation.** `ducknng` assigns `session_id` values from a `uint64_t` counter
starting at 1 (`svc->next_session_id`). `session_id` is part of the public wire protocol
— it appears in `query_open` JSON replies and is echoed back in `fetch`, `close`, and
`cancel` requests. Tests rely on sequential predictable values.

`duckdb_client_context_get_connection_id` returns the DuckDB-internal connection ID, which
is non-sequential and not predictable from the client's perspective. Using it as the
public `session_id` would break the protocol contract and require updating every test and
documented example. It could be stored as a private diagnostic field on the session
struct for log correlation without affecting the wire ID.

**Recommendation: NOT applicable for public session_id.** The counter is correct and
cheap. `duckdb_client_context_get_connection_id` could be stored as `session_duckdb_conn_id`
on `ducknng_session` for diagnostic use, but that is a low-value addition given the
counter already provides unique session identification.

---

## `unstable_new_vector_functions`

**Functions:** `duckdb_create_vector`, `duckdb_destroy_vector`, `duckdb_slice_vector`,
`duckdb_vector_reference_value`, `duckdb_vector_reference_vector`,
`duckdb_vector_copy_sel`, `duckdb_create_selection_vector`,
`duckdb_destroy_selection_vector`, `duckdb_selection_vector_get_data_ptr`,
`duckdb_unsafe_vector_assign_string_element_len`

**What it does.** Provides lower-level vector manipulation: zero-copy vector references,
selection vectors for filtering without copying, string assignment with explicit length,
and standalone vector allocation with arbitrary capacity.

`duckdb_create_vector` / `duckdb_destroy_vector` allocate standalone `duckdb_vector`
objects outside a data chunk. `duckdb_slice_vector` reindexes a vector through a
selection vector. `duckdb_vector_reference_value` fills a vector with a single scalar
value. `duckdb_vector_copy_sel` copies elements with per-element selection.

**Update:** `duckdb_unsafe_vector_assign_string_element_len` is adopted — see the
string functions entry above. The Quack-derived decoder also uses
`duckdb_create_vector`, `duckdb_destroy_vector`,
`duckdb_create_selection_vector`, `duckdb_selection_vector_get_data_ptr`,
`duckdb_vector_copy_sel`, and `duckdb_destroy_selection_vector` to materialize
constant and dictionary vectors into flat output while retaining DuckDB's
recursive-value ownership semantics. Vector references and `duckdb_slice_vector`
remain unused.

---

## `unstable_new_file_system_api`

**Functions:** `duckdb_file_system_open`, `duckdb_file_handle_read`,
`duckdb_file_handle_write`, `duckdb_file_handle_seek`, `duckdb_file_handle_tell`,
`duckdb_file_handle_get_file_size`, `duckdb_file_handle_sync`, `duckdb_file_handle_close`

**What it does.** Provides a consumer interface for opening and operating on files through
DuckDB's virtual file system abstraction. `duckdb_file_system` is an opaque
`{ void *internal_ptr }` wrapping a C++ `FileSystem *`.

**Important limitation.** There is no `duckdb_register_file_system` or equivalent
registration callback in the C API. The C++ side has
`DatabaseInstance::AddFileSystem(unique_ptr<FileSystem>)` and a virtual `FileSystem` base
class, but neither is bridged to C. Implementing a custom `ducknng://body/...` path that
DuckDB dispatches to would require constructing a C++ vtable with the correct layout and
ABI — platform- and version-specific, and not viable from pure C.

**What the API is good for.** The consumer-side functions — `duckdb_file_system_open` and
`duckdb_file_handle_write` — replace OS-level file I/O in the Parquet temp-file path
(`ducknng_body_parse_run_tempfile_reader`), routing through DuckDB's FS abstraction for
portability. The write path was migrated in an earlier pass. The remaining POSIX dependency
was `mkstemp`+`close` in `ducknng_body_make_tempfile_path`, used only to generate a unique
file name before DuckDB FS opens it. That is now replaced by an atomic counter that
constructs `/tmp/ducknng_body_<hex16>.tmp` (POSIX) or `%TEMP%\ducknng_body_<hex16>.tmp`
(Windows) without touching the OS file system at all.

**Status: ADOPTED.** The write path uses `duckdb_file_system_open`, `duckdb_file_handle_write`,
and `duckdb_destroy_file_handle`. The path-generation helper no longer calls `mkstemp` or
`_tempnam`; it uses `atomic_fetch_add_explicit` over a module-static counter.

---

## `unstable_new_copy_functions_api`

**Functions:** Full COPY TO / COPY FROM extension API with bind, sink, and finalize
callbacks, plus `duckdb_copy_function_set_copy_from_function`.

**What it does.** Lets extensions implement custom `COPY ... TO/FROM 'file' (FORMAT
myformat)` dialects integrated into DuckDB's query planner.

**Recommendation: NOT applicable.** `ducknng` parses body bytes in a TVF bind callback,
not through `COPY`. No benefit from this group.

---

## `unstable_new_expression_functions`

**Functions:** `duckdb_expression_is_foldable`, `duckdb_expression_fold`

**What it does.** Lets the extension ask the planner whether a given expression is
constant and fold it to a value at bind time.

**Adopted.** Used inside the scalar bind callback `ducknng_http_lookup_bind_cb`
(`src/ducknng_sql_http.c`). `duckdb_expression_is_foldable` tests whether arg 1 is
a constant, and `duckdb_expression_fold` collapses it to a `duckdb_value` that is
then extracted with `duckdb_get_varchar`. The expression and value are destroyed with
`duckdb_destroy_expression` and `duckdb_destroy_value` respectively.

---

## `unstable_new_catalog_interface`

**Functions:** `duckdb_client_context_get_catalog`, `duckdb_catalog_get_entry`,
`duckdb_catalog_get_type_name`

**What it does.** Exposes the DuckDB catalog for looking up types and objects by name
from within an extension.

**Recommendation: NOT applicable now.** `ducknng` does not inspect the user catalog.

---

## `unstable_new_logger_functions`

**Functions:** `duckdb_create_log_storage`, `duckdb_destroy_log_storage`,
`duckdb_log_storage_set_write_log_entry`, `duckdb_log_storage_set_extra_data`,
`duckdb_log_storage_set_name`, `duckdb_register_log_storage`

**What it does.** Lets extensions register a named custom log sink that receives DuckDB's
internal log entries. `duckdb_log_storage_set_write_log_entry` installs the write
callback. `duckdb_log_storage_set_extra_data` attaches extension state with a destroy
callback. `duckdb_log_storage_set_name` labels the sink for diagnostics.

**Status: ADOPTED (deferred registration).** The log ring buffer (`ducknng_log_ring`,
capacity 512) is in place and `ducknng_log_write_entry` (`src/ducknng_runtime.c`) is the
registered callback. However, calling `duckdb_register_log_storage` from the extension
entry point triggers a DuckDB v1.5.2 internal assertion failure ("Attempted to
dereference unique_ptr that is NULL"). The fix is to defer registration to query time
via `ducknng_enable_log_capture()`, a volatile scalar that calls
`duckdb_create_log_storage` + `duckdb_log_storage_set_*` + `duckdb_register_log_storage`
from a normal SQL execution context where the DuckDB logger subsystem is fully
initialized. The function is idempotent and sets `rt->log_capture_enabled` on success.
Ownership of the log storage object transfers to DuckDB after `duckdb_register_log_storage`
succeeds; `duckdb_destroy_log_storage` is called only on failure.

---

## `unstable_new_config_options_functions`

**Functions:** `duckdb_create_config_option`, `duckdb_register_config_option`,
`duckdb_client_context_get_config_option`

**What it does.** Lets extensions declare named configuration options visible through
`SET ducknng.option = value` and readable in SQL via `duckdb_client_context_get_config_option`.

**Status: ADOPTED.** `ducknng.csv_max_columns` (UBIGINT, default 1024, SESSION scope) is
registered at extension load time via `ducknng_register_config_options` in
`src/ducknng_sql_api.c` using `duckdb_create_config_option` / `duckdb_config_option_set_*`
/ `duckdb_register_config_option`. It is read in the CSV/TSV body-parse bind callback
(`src/ducknng_sql_body.c`) via `ducknng_sql_get_config_ubigint(client_ctx, ...)`, which
calls `duckdb_client_context_get_config_option` and falls back to 1024 when no override
is set. Users can raise the limit for wide CSV inputs with
`SET ducknng.csv_max_columns = 4096`.

---

## `unstable_deprecated`

**Functions:** `duckdb_query_arrow`, `duckdb_arrow_array_scan`,
`duckdb_arrow_rows_changed`, `duckdb_pending_prepared_streaming`,
`duckdb_stream_fetch_chunk`, etc.

The original Arrow result-set APIs are deprecated in favour of
`unstable_new_arrow_functions` and remain forbidden. The pending-result streaming
pair is different: DuckDB v1.5.2 has no undeprecated C API that combines pending
execution with incremental chunk delivery. `ducknng` therefore uses
`duckdb_pending_prepared_streaming` and `duckdb_stream_fetch_chunk` only through
`src/ducknng_duckdb_streaming_compat.c`. There is no materialized-result fallback
inside query sessions because that would change the internal session contract and
reintroduce the performance path this boundary exists to avoid.

**Recommendation: EXCEPTION ONLY.** Keep the pending-result streaming exception
isolated in the compatibility boundary and avoid all other deprecated entrypoints.
When DuckDB provides a replacement, update the boundary rather than adding branches
at query, session, or codec call sites.

---

## `unstable_new_error_data_functions`

**Functions:** `duckdb_create_error_data`, `duckdb_error_data_message`,
`duckdb_error_data_has_error`

**What it does.** Richer structured error objects with type codes and messages.

**Recommendation: LOW priority.** Current string-based error propagation is adequate.
Adopt if `ducknng` exposes typed error codes through the protocol.

**Update:** adopted as a side-effect of the emit-side Arrow rewrite. `duckdb_error_data_has_error`, `duckdb_error_data_message`, and `duckdb_destroy_error_data` are now used in `src/ducknng_ipc_out.c` to check and extract errors from `duckdb_to_arrow_schema` and `duckdb_data_chunk_to_arrow` return values.

---

## `unstable_instance_cache`

**Functions:** `duckdb_create_instance_cache`, `duckdb_get_or_create_from_cache`

**What it does.** Shared database instance caching for multi-process use.

**Recommendation: NOT applicable.** `ducknng` manages its own database handle lifetime.

---

## `unstable_new_append_functions`

**Functions:** `duckdb_appender_create_query`, `duckdb_appender_error_data`,
`duckdb_appender_clear`

**What it does.** Extensions to the `duckdb_appender` API.

**Recommendation: NOT applicable.** `ducknng` does not use the appender API.

---

## `unstable_new_geo_functions`

**Functions:** `duckdb_geometry_type_get_crs(type) -> char *`

**What it does.** Returns the CRS (coordinate reference system) string for a DuckDB
`GEOMETRY` logical type. Useful for extensions that introspect geometry column metadata.

**Recommendation: NOT applicable.** `ducknng` does not handle geometry types. The
transport layer encodes unsupported types as Arrow binary blobs; no CRS metadata is
exposed.

---

## `unstable_new_prepared_statement_functions`

**Functions:** `duckdb_prepared_statement_column_count`,
`duckdb_prepared_statement_column_name`, `duckdb_prepared_statement_column_logical_type`,
`duckdb_prepared_statement_column_type`

**What it does.** Exposes output column metadata (count, name, logical type, type enum)
from a `duckdb_prepared_statement` before it is executed. Previously, column metadata was
only available after calling `duckdb_execute_prepared` or its variants.

**Status: ADOPTED.** Used to implement the `query_prepare` RPC method. The helper
`ducknng_prepared_schema_to_ipc` in `src/ducknng_ipc_out.c` calls
`duckdb_prepared_statement_column_count`, `duckdb_prepared_statement_column_name`, and
`duckdb_prepared_statement_column_logical_type` to build the Arrow IPC schema for a
prepared statement and serialize a zero-batch Arrow stream. The `query_prepare` method
handler in `src/ducknng_methods.c` prepares the final statement in a multi-statement
query and returns this schema stream without executing the query. This gives clients the
output column names and types before committing to execution.

---

## `unstable_new_query_execution_functions`

**Functions:** `duckdb_result_get_arrow_options(result) -> duckdb_arrow_options`

**What it does.** Retrieves the Arrow options associated with a `duckdb_result`. Arrow
options control encoding details such as large string offsets and timezone handling.

**Pairing.** `duckdb_connection_get_arrow_options` (in `unstable_new_open_connect_functions`)
gets the per-connection options that were in effect when the result was produced.
`duckdb_result_get_arrow_options` retrieves those options from the result object itself.
Both must be destroyed with `duckdb_destroy_arrow_options`.

**Status: ADOPTED.** Both functions are used in `src/ducknng_ipc_out.c`:
`duckdb_result_get_arrow_options` is called in the main `ducknng_result_to_ipc_stream`
path to retrieve Arrow options from the result, and `duckdb_connection_get_arrow_options`
is called in `ducknng_prepared_schema_to_ipc` where a result object is not available
(schema-only path). The options are passed to `duckdb_to_arrow_schema` and
`duckdb_data_chunk_to_arrow` to ensure correct timezone and large-string encoding.

---

## `unstable_new_scalar_function_state_functions`

**Functions:** `duckdb_scalar_function_get_state`,
`duckdb_scalar_function_set_init`, `duckdb_scalar_function_init_set_error`,
`duckdb_scalar_function_init_set_state`, `duckdb_scalar_function_init_get_client_context`,
`duckdb_scalar_function_init_get_bind_data`, `duckdb_scalar_function_init_get_extra_info`

**What it does.** Adds a per-execution-thread local state to scalar functions. An `init`
callback is called once per execution context to allocate thread-local state (e.g. a
compiled regex, a connection handle, a reusable buffer). The execute callback retrieves
that state with `duckdb_scalar_function_get_state`.

**Status: ADOPTED.** Used in `src/ducknng_sql_http.c` for `ducknng_http_headers_build`.
The init callback `ducknng_headers_build_init_cb` allocates a
`ducknng_headers_build_state` struct containing pointer arrays for header name/value
pairs and their escaped counterparts. The struct is registered via
`duckdb_scalar_function_init_set_state` with a corresponding destroy callback. The
execute callback retrieves it with `duckdb_scalar_function_get_state` and reuses the
arrays across rows, growing them on demand. If state is NULL (fallback), the function
falls back to per-row allocation. This eliminates repeated malloc/free cycles during
batch execution of `ducknng_http_headers_build`.

---

## `unstable_new_table_description_functions`

**Functions:** `duckdb_table_description_get_column_count`,
`duckdb_table_description_get_column_type`

**What it does.** Extends the `duckdb_table_description` API (opened with
`duckdb_table_description_create` / `duckdb_table_description_create_ext`) with column
count and type introspection. A table description gives schema metadata for a named table
without executing a query.

**Recommendation: NOT applicable.** `ducknng` does not introspect named tables.

---

## `unstable_new_value_functions`

**Functions:** `duckdb_create_map_value`, `duckdb_create_union_value`,
`duckdb_create_time_ns`, `duckdb_get_time_ns`

**What it does.** Constructs `duckdb_value` objects for MAP, UNION, and TIME_NS types.
Useful in TVF bind callbacks and scalar bind callbacks where values must be returned
without executing a query.

**Status: ADOPTED.** Arrow parameter tuples are decoded into `duckdb_value` objects and
bound to prepared statements. MAP, UNION, ARRAY, TIME_NS, nested containers, and the
other supported Arrow types therefore use the value constructors in this group. The
end-to-end contract is covered by `test/sql/ducknng_parameter_binding.test`.

---

## Summary table

| Group | Priority | Action |
|---|---|---|
| `unstable_new_arrow_functions` | **DONE** | Emit and receive paths are implemented in `ducknng_ipc_out.c` and `ducknng_sql_arrow.c` |
| `unstable_new_error_data_functions` | **DONE** | Adopted in `ducknng_ipc_out.c` alongside Arrow rewrite |
| `unstable_new_string_functions` / vector string | **DONE** | `duckdb_valid_utf8_check` in `ducknng_sql_bytes_look_text`; `duckdb_unsafe_vector_assign_string_element_len` at all vector string-assign sites |
| `unstable_new_scalar_function_functions` | **DONE** | Bind phase adopted for the four HTTP lookup scalar functions; bind data pre-folds constant name argument |
| `unstable_new_expression_functions` | **DONE** | Used inside scalar bind callbacks (`duckdb_expression_is_foldable`, `duckdb_expression_fold`) |
| `unstable_new_table_function_functions` | NOT applicable | `duckdb_table_function_get_client_context` returns a DuckDB context, not `ducknng_sql_context`; `extra_info` pattern must remain |
| `unstable_new_file_system_api` | **DONE** | Write path uses `duckdb_file_system_open` + `duckdb_file_handle_write`; path generation uses atomic counter (no mkstemp) |
| `unstable_new_open_connect_functions` | NOT applicable | `duckdb_client_context_get_connection_id` would change the public wire session_id; counter is correct |
| `unstable_new_logger_functions` | **DONE** | `ducknng_enable_log_capture()` scalar defers `duckdb_register_log_storage` to query time to avoid v1.5.2 load-time crash |
| `unstable_new_config_options_functions` | **DONE** | `ducknng.csv_max_columns` registered at load time; read in CSV/TSV body-parse bind callback |
| `unstable_new_prepared_statement_functions` | **DONE** | `query_prepare` RPC method; `ducknng_prepared_schema_to_ipc` in `ducknng_ipc_out.c` |
| `unstable_new_query_execution_functions` | **DONE** | `duckdb_result_get_arrow_options` in result path; `duckdb_connection_get_arrow_options` in prepared-schema path |
| `unstable_new_scalar_function_state_functions` | **DONE** | Per-thread scratch buffer for `ducknng_http_headers_build` in `ducknng_sql_http.c` |
| `unstable_new_value_functions` | **DONE** | Arrow parameter decoding constructs MAP, UNION, ARRAY, TIME_NS, and other typed DuckDB values before prepared-statement binding |
| `unstable_new_vector_functions` | PARTIAL | Length-aware string assignment, temporary vectors, and selection-copy materialization are adopted; reference/slice functions remain unused |
| `unstable_new_geo_functions` | NOT applicable | GEOMETRY CRS metadata; `ducknng` does not handle geometry columns |
| `unstable_new_table_description_functions` | NOT applicable | Named-table schema introspection not used |
| `unstable_new_copy_functions_api` | NOT applicable | — |
| `unstable_new_catalog_interface` | NOT applicable | — |
| `unstable_instance_cache` | NOT applicable | — |
| `unstable_new_append_functions` | NOT applicable | — |
| `unstable_deprecated` | EXCEPTION | Pending-result streaming is isolated in `ducknng_duckdb_streaming_compat.c`; avoid all other deprecated entrypoints |

---

## DuckDB async query surface

This section covers DuckDB's pending-query API plus the single deprecated streaming
exception that `ducknng` uses for incremental session fetches. It is documented here
because it intersects directly with `ducknng`'s session lifecycle and async dispatch
model.

### Pending query API

**Functions (stable):** `duckdb_pending_prepared`, `duckdb_destroy_pending`,
`duckdb_pending_error`, `duckdb_pending_execute_task`,
`duckdb_pending_execute_check_state`, `duckdb_execute_pending`,
`duckdb_pending_execution_is_finished`

**Functions (deprecated streaming exception):** `duckdb_pending_prepared_streaming`,
`duckdb_stream_fetch_chunk`

**Task execution (stable):** `duckdb_execute_tasks`, `duckdb_create_task_state`,
`duckdb_execute_tasks_state`, `duckdb_execute_n_tasks_state`, `duckdb_finish_execution`,
`duckdb_destroy_task_state`

**What it does.** `duckdb_pending_prepared` starts a prepared statement asynchronously,
returning a `duckdb_pending_result` instead of blocking. The caller then calls
`duckdb_pending_execute_task` in a loop, yielding between calls, until the state
transitions to `DUCKDB_PENDING_RESULT_READY`. At that point `duckdb_execute_pending`
collects the final `duckdb_result`. `duckdb_pending_execute_check_state` probes state
without advancing execution; `duckdb_pending_execution_is_finished` maps a state enum
to a boolean.

`duckdb_execute_tasks` drives DuckDB background work from external threads — this is the
multi-threaded execution model where DuckDB task work is pumped by a thread pool that the
caller controls.

**Status: ADOPTED in session query lifecycle, with streaming isolated behind a compatibility boundary.**

`ducknng_methods.c` uses this API in the `query_open` / `fetch` session flow:

- `query_open` calls `ducknng_pending_prepared_for_session(...)`, implemented in
  `src/ducknng_duckdb_streaming_compat.c`. The wrapper calls
  `duckdb_pending_prepared_streaming` so fetches can read rows incrementally.
- The first `fetch` call drives execution by looping `duckdb_pending_execute_task` until
  `DUCKDB_PENDING_RESULT_READY`, then calls `duckdb_execute_pending` to obtain the
  `duckdb_result`. Subsequent `fetch` calls read chunks through
  `ducknng_result_fetch_session_chunk(...)`, which calls `duckdb_stream_fetch_chunk`.

This is the one deliberate exception to the repository's normal avoidance of deprecated
DuckDB C entrypoints. Profiling showed the materialized pending-result path spending a
large share of query-session time in `MaterializedQueryResult::FetchInternal` and
`ColumnDataCollection::Scan` for 10M-row RPC fetches. After switching session fetches to
the compatibility wrapper, the sampled path moves through `StreamQueryResult::FetchInternal`
and DuckDB buffered/scan work instead. DuckDB v1.5.2 does not expose an undeprecated C API
that combines pending execution with incremental result chunks, so the compatibility
wrapper centralizes this version-sensitive choice and keeps the rest of the session code
independent of the deprecated symbol.

**Outstanding design question: cooperative vs. preemptive task dispatch.**

The current `fetch` handler drives the pending result to completion in a tight loop on
whichever NNG worker thread picks up the fetch request. This is simple and correct, but
it monopolises an NNG thread for the duration of the query, preventing that thread from
handling other requests.

A more cooperative model would pump `duckdb_pending_execute_task` in a dedicated task
thread or use `duckdb_execute_tasks` with a thread pool, signalling the NNG layer when
the result is ready via an NNG AIO completion. The NNG AIO (`nng_aio`) primitive in
`ducknng` is already used for raw send/receive futures. Bridging it to the DuckDB pending
result would require a dedicated poller thread or integration with NNG's task scheduler,
but would allow NNG worker threads to remain unblocked during long queries.

This is a concrete future improvement path but not a blocking issue today: the server
is already functional and correct; the cost is reduced concurrency under heavy parallel
query load.
