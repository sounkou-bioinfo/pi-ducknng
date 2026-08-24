# TigerStyle review for ducknng

This note records how to read TigerBeetle's `TIGER_STYLE.md` against `ducknng`. It is not a mandate to copy TigerBeetle's rules literally. `ducknng` is an embedded DuckDB extension and transport server, so crashing the host process is usually the wrong failure mode. The useful translation is tighter bounds, clearer invariants, smaller state machines, explicit ownership, and error messages that remain visible to SQL callers.

## Project-local interpretation

The `ducknng` production rule is errors as values with messages. Runtime checks that can be triggered by malformed user input, malformed wire input, resource exhaustion, unsupported browser/runtime behavior, or ordinary operating errors must return a DuckDB error, an `ok = false` result row, or a terminal aio error handle. They must not abort the DuckDB process.

Compile-time assertions are acceptable and useful. `_Static_assert(...)` should be used for design invariants such as fixed header lengths, enum assumptions, integer widths, and compile-time relationships between constants. These fail the build rather than a user session.

Runtime aborting assertions should be reserved for opt-in developer, property-test, sanitizer, or fuzz builds. If a check is valuable in production, express it as a normal branch that preserves the existing error-as-value surface. For callback paths that cannot directly set a DuckDB bind/scalar error, record the error in the runtime, service, or aio state and surface it through the existing status or collect APIs.

## What already aligns well

The architectural split is strong. Transport-family parsing is isolated in `src/ducknng_transport.c`; NNG-specific behavior is behind `src/ducknng_nng_compat.c`; HTTP-specific behavior is behind `src/ducknng_http_compat.c`; wire framing is in `src/ducknng_wire.c`; DuckDB streaming compatibility is isolated in `src/ducknng_duckdb_streaming_compat.c`; the method registry and manifest are first-class protocol contracts.

The protocol favors bounded public contracts rather than vague behavior. Method names have a maximum length, services and routes carry request size limits, fetch batching is capped, HTTP route body limits are tied back to service receive limits, and unsupported transport schemes are rejected explicitly.

The code uses explicit cleanup paths and in-band error tables in many public SQL helpers. This matches the project policy better than aborting on failures. Property tests now exist for wire, transport, and Quack-adjacent invariants, and sanitizer targets are available for that native property-test slice.

## Current mechanical gaps

A quick scan of `src/*.c` and `src/include/*.h` found roughly:

```text
C/H lines scanned:             28,823
functions detected:               806
functions longer than 70 lines:    53
lines longer than 100 columns:  1,380
runtime/static assertion refs:       0
allocation/free refs in src:     1,613
compound boolean refs:           1,426
```

These numbers are only heuristics, but they identify the main review pressure points.

### Invariants are checked, but not centralized

The code has many defensive `if (...) return ...` checks, but it does not have a shared vocabulary for internal design invariants, size arithmetic, or positive/negative-space validation. This makes it harder to distinguish user-facing validation from programmer-error checks and harder to audit whether overflow checks are complete.

Recommended direction:

- add `_Static_assert` checks for fixed wire layout and constant relationships;
- add non-aborting helper checks for size arithmetic and bounds;
- keep any aborting `DUCKNNG_ASSERT`-style macro debug-only and disabled in production;
- prefer helpers that set `char **errmsg` or return existing `ok = false` result rows.

### Size arithmetic needs a common checked path

There are multiple growable buffers that compute `want = len + add` and then double capacity until it fits. Examples include `ducknng_quack_writer_reserve()` in `src/ducknng_quack.c` and `ducknng_buf_append()` in `src/ducknng_util.c`. These should use a common checked addition and capacity-growth helper so overflow behavior is identical everywhere and returns a clear error.

The production rule should be:

```c
if (ducknng_size_add(*len, src_len, &want) != 0) {
    /* errors-as-values: set the caller's errmsg / return -1, never abort */
    return -1;
}
```

not an assertion and not unchecked arithmetic.

Landed: `ducknng_size_add`, `ducknng_size_mul`, and `ducknng_grow_capacity`
in `src/ducknng_util.c` (declared in `src/include/ducknng_util.h`) are the
shared checked-arithmetic and capacity-growth helpers. Each returns `0` on
success and `-1` on overflow without writing a wrapped value, so callers keep
the existing errors-as-values surface. `ducknng_buf_append()` in
`src/ducknng_util.c` and `ducknng_quack_writer_reserve()` in
`src/ducknng_quack.c` now route their `len + add` and doubling-growth math
through these helpers. Boundary regressions live in the
`size_checked_properties` suite in `test/property/ducknng_prop.c` and run under
`make prop`, `make prop-asan`, and `make prop-ubsan`.

### Arrow schema and value handling uses recursion

`src/ducknng_sql_arrow.c` recursively walks nested Arrow schemas and nested Arrow values in:

- `ducknng_sql_arrow_set_nested_null()`;
- `ducknng_sql_arrow_schema_to_logical_type()`;
- `ducknng_sql_arrow_assign_column_at()`.

That is the clearest conflict with the TigerStyle preference for bounded control flow. Because Arrow schemas are external input, this should not be unbounded. The near-term fix does not have to be a full iterative rewrite; adding an explicit nesting depth limit and passing `depth + 1` through recursive helpers would already turn the failure into a precise DuckDB-facing error.

Recommended direction:

- define a documented maximum Arrow nesting depth for decoded remote results;
- enforce it in schema binding and value assignment;
- add invalid/deep nested schema tests;
- consider an iterative stack later if the supported nested subset grows.

Landed: `DUCKNNG_ARROW_MAX_NESTING_DEPTH` (64) in `src/ducknng_sql_arrow.c` is
the documented bound, pinned with a `_Static_assert`. The three recursive walks
— `ducknng_sql_arrow_schema_to_logical_type` (via an internal `_depth` variant
behind the unchanged public signature), `ducknng_sql_arrow_assign_column_at`,
and `ducknng_sql_arrow_set_nested_null` — now thread a `depth` counter.
Type-binding and value assignment return a DuckDB-visible
"Arrow … nesting exceeds supported depth" error past the bound instead of
recursing; nested-null marking stops descending defensively. An iterative
stack is still the longer-term option if the supported nested subset grows.
The normal (in-bounds) nested path — lists, structs, maps, unions, list of
struct, struct of list, recursive list of list, and nulls nested inside each —
is round-trip tested end-to-end over both serialization modes in
`test/sql/ducknng_type_roundtrip.test`. Remaining: a fixture that feeds an
*over-depth* (> `DUCKNNG_ARROW_MAX_NESTING_DEPTH`) nested Arrow IPC schema to
assert the depth guard fires (the native property harness does not link the
DuckDB Arrow API, so this needs an sqllogictest fixture or a dedicated target).

### Long functions hide state transitions

The longest functions are concentrated in Arrow conversion, method handlers, SQL binding/scanning, runtime init, and aio callbacks. The biggest examples from the scan were:

```text
418  src/ducknng_sql_arrow.c:316   ducknng_sql_arrow_assign_column_at
191  src/ducknng_sql_arrow.c:98    ducknng_sql_arrow_schema_to_logical_type
179  src/ducknng_methods.c:446     ducknng_method_query_open_handler
165  src/ducknng_methods.c:629     ducknng_method_fetch_handler
163  src/ducknng_sql_http.c:490    ducknng_http_headers_build_scalar
151  src/ducknng_sql_service.c:286 ducknng_servers_scan
127  src/ducknng_registry.c:369    ducknng_method_registry_manifest_json
126  src/ducknng_runtime.c:64      ducknng_runtime_init
119  src/ducknng_sql_monitor.c:173 ducknng_read_monitor_bind
119  src/ducknng_methods.c:901     ducknng_method_query_prepare_handler
118  src/ducknng_session.c:285     ducknng_service_add_session
114  src/ducknng_sql_aio.c:348     ducknng_client_aio_cb
```

The project does not need a hard 70-line law, but these functions should be treated as review hotspots. The highest-value splits are ones that separate parsing, validation, state mutation, and output construction without creating fake object systems.

### AIO callbacks perform too much work

`ducknng_client_aio_cb()` handles phase transitions, NNG error extraction, HTTP response copying, frame response copying, state mutation, socket reference release, and condition-variable notification. It is understandable, but it is too much responsibility for an async completion callback.

Recommended direction:

- keep the callback as a small state transition entry point;
- move HTTP response harvesting and framed reply harvesting into phase-specific helpers;
- make lock ownership obvious in helper names or comments;
- avoid doing more work under the runtime mutex than needed;
- preserve terminal aio errors as values, not thrown or aborting errors.

### Dynamic allocation is inherent, but should be bounded and named

A DuckDB extension cannot follow TigerBeetle's no-post-init-allocation rule literally. DuckDB vectors, Arrow IPC payloads, HTTP bodies, manifest JSON, and query results are data-dependent. The `ducknng` adaptation should be bounded dynamic allocation:

- every public path that can allocate from input should have an explicit size limit;
- every growable buffer should use checked arithmetic;
- ownership should be obvious from names and cleanup blocks;
- long-lived runtime pools should be allocated at runtime initialization where practical;
- browser wasm paths should remain conservative because memory and thread resources are constrained.

### Compound validation should be split at trust boundaries

The code has many compact conditions such as `if (!rt || !slot || ...) return ...`. These are common C style, but at trust boundaries they obscure which error was detected. For user-facing validation, split conditions when doing so improves messages. For internal leaf helpers, compact guard clauses are acceptable if the error is not externally ambiguous.

Preferred boundary shape:

```c
if (!url || !url[0]) {
    ducknng_set_error(errmsg, "ducknng: transport URL is required");
    return -1;
}
if (tls_config_id != 0 && !parsed.uses_tls) {
    ducknng_set_error(errmsg, "ducknng: TLS config requires a TLS-capable scheme");
    return -1;
}
```

## Prioritized cleanup queue

1. Done. Non-aborting checked arithmetic helpers (`ducknng_size_add`, `ducknng_size_mul`, `ducknng_grow_capacity`) landed; no production `assert()` added. A broader `_Static_assert` pass over wire constants is still open.
2. Done. `ducknng_quack_writer_reserve()` and `ducknng_buf_append()` now use the checked helpers.
3. Mostly done. Arrow nesting limit (`DUCKNNG_ARROW_MAX_NESTING_DEPTH`) enforced in schema binding and value assignment; an over-depth end-to-end fixture test is still open.
4. Split `ducknng_sql_arrow_assign_column_at()` into type-family assignment helpers while preserving the current supported type subset.
5. Split `ducknng_client_aio_cb()` into phase-specific completion helpers and document mutex ownership.
6. Review `ducknng_method_query_open_handler()` and `ducknng_method_fetch_handler()` for clearer parse/validate/execute/reply phases.
7. Add property or regression tests for buffer growth overflow, oversized payload rejection, and deeply nested Arrow schemas.
8. Consider a stricter warning target for core extension code once the current build matrix can support it without churn.

## Checklist for future patches

- [ ] Does malformed input return a DuckDB-visible error value/message instead of crashing?
- [ ] Are size additions and multiplications checked before allocation or `memcpy`?
- [ ] Is there an explicit maximum for loops over external input, queues, buffers, or nested structures?
- [ ] Are recursive walks bounded by a documented depth limit or replaced by an explicit stack?
- [ ] Does the function separate parsing, validation, state mutation, and output construction clearly enough for review?
- [ ] Is ownership obvious at every allocation and cleanup point?
- [ ] Is callback work minimized and are lock boundaries auditable?
- [ ] Are transport-family decisions made before adapter-specific code runs?
- [ ] Are public behavior changes reflected in protocol, transport, HTTP, manifest, type, or wasm docs as appropriate?
- [ ] Are tests covering both valid and invalid inputs, including values that become invalid at a boundary?
