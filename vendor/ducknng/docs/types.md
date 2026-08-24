# ducknng type support and schema policy

This document is binding for how `ducknng` declares and supports types in method inputs and outputs. The project does not want a vague promise that it uses Arrow. It needs an explicit, stable contract describing which Arrow logical types are accepted, how they map to DuckDB logical types, which subset is considered canonical for output, and which advanced or exotic types are intentionally deferred.

The guiding principle is that Arrow IPC is the transport-level tabular encoding and DuckDB logical types are the execution-level types. The protocol contract sits between them. That contract must be narrow enough to be reliable and broad enough to be useful. Early versions of the project should therefore prefer a strong and explicit supported subset over a theoretically complete but poorly specified compatibility claim.

## Normative type mappings

The following mappings are part of the public 0.1.0 contract. Each type listed here has test coverage in `test/sql/ducknng_rpc_client_smoke.test` and is safe to rely on.

Arrow `bool` maps to DuckDB `BOOLEAN`. Arrow `int8`, `int16`, `int32`, and `int64` map to DuckDB `TINYINT`, `SMALLINT`, `INTEGER`, and `BIGINT`. Arrow `uint8`, `uint16`, `uint32`, and `uint64` map to DuckDB `UTINYINT`, `USMALLINT`, `UINTEGER`, and `UBIGINT`. Arrow `float32` and `float64` map to DuckDB `FLOAT` and `DOUBLE`. Arrow `utf8` maps to DuckDB `VARCHAR`, and Arrow `binary` maps to DuckDB `BLOB`. Arrow `date32` maps to DuckDB `DATE`. Arrow `time64[us]` maps to DuckDB `TIME` and Arrow `time64[ns]` maps to DuckDB `TIME_NS`. Timezone-free Arrow timestamps map to the corresponding DuckDB timestamp unit: `timestamp[s]` to `TIMESTAMP_S`, `timestamp[ms]` to `TIMESTAMP_MS`, `timestamp[us]` to `TIMESTAMP`, and `timestamp[ns]` to `TIMESTAMP_NS`. DuckDB `DECIMAL` values are emitted as Arrow `decimal128` with the same precision and scale. Arrow `struct` maps to DuckDB `STRUCT` and Arrow `list` maps to DuckDB list values. Nesting of `struct` and `list` is supported to arbitrary depth.

## Emit-only projections (stable, receive-side only)

The following DuckDB types are emitted as Arrow but are not accepted back on the input side or roundtripped to the same DuckDB type. Clients receive the projected Arrow type and are responsible for any re-interpretation they need.

`HUGEINT` is projected as Arrow `decimal128(38, 0)`. `UUID` is projected as Arrow `utf8` in canonical 36-character form. `TIMESTAMP WITH TIME ZONE` is projected as a timezone-free Arrow `timestamp[us]`; timezone metadata is dropped on the wire and the microsecond value is preserved as-is. `ENUM` is projected as the resolved Arrow `utf8` label, not as a dictionary-encoded Arrow array. `MAP` is projected as Arrow `map` with key and value children matching the DuckDB key and value types; roundtripping a result back to a DuckDB `MAP` column is a caller responsibility. `UNION` (DuckDB dense union) is projected as Arrow `dense_union` on the emit side only; the DuckDB-facing decoder for `dense_union` vectors is not yet implemented, so queries returning `UNION` values through `ducknng_query_rpc` currently return an error.

## Explicitly deferred

The following features are intentionally outside the current contract. They are not ruled out permanently, but they must not be advertised as supported until they have dedicated implementation, tests, and documentation.

**Dictionary-preserving roundtrips.** The server does not emit Arrow dictionary arrays and does not accept them. `ENUM` is always projected as plain `utf8`. Dictionary-encoded inputs from third-party clients are not currently decoded.

**Extension types.** The server emits no Arrow extension metadata and ignores extension metadata on incoming schemas. Extension type claims require an explicit registry, a defined name, and test coverage before they can be part of the contract.

**Run-end encoded arrays.** Not implemented on either the emit or accept side.

**Large UTF-8 / large binary / fixed-size binary.** Not implemented. The server emits ordinary `utf8` and `binary` regardless of value size.

**Duration.** DuckDB `INTERVAL` falls through to an error at the Arrow IPC encoder boundary. No `duration` Arrow type is emitted.

**UNION input decoding.** DuckDB `UNION` values can be emitted as Arrow `dense_union` (emit-only, see above) but the DuckDB vector decoder for `dense_union` is not implemented. Queries returning `UNION` columns through the RPC surface return an error until a decoder is added.

**Timezone semantics for TIMESTAMP WITH TIME ZONE.** The server emits `TIMESTAMP_TZ` as a timezone-free `timestamp[us]` with no UTC adjustment. Proper timezone-aware handling is deferred.

## Scan-phase Arrow conversion

The scan callbacks for `ducknng_fetch_query_table` and `ducknng_parse_body` use a hand-rolled nanoarrow-to-DuckDB-vector conversion (739-line `ducknng_sql_arrow_assign_column_at` switch statement) instead of DuckDB's unstable C API functions `duckdb_schema_from_arrow` and `duckdb_data_chunk_from_arrow`. Both of those functions require a `duckdb_connection` handle that is not available in scan callbacks.

Two approaches exist to remove the hand-rolled path:

1. **Bind/init-phase conversion** — convert Arrow data to DuckDB data chunks in the bind or init callback (where a `duckdb_client_context` is available via `duckdb_table_function_get_client_context`, from which a connection can be opened), cache the chunks in bind_data, and serve them from the cache in scan. Simple, synchronous, adequate for single-batch results. This is the planned path.

2. **Dedicated decode thread** — a background thread owns a connection, converts Arrow chunks as they arrive, and pushes to a bounded queue consumed by the scan callback. Adds pipeline parallelism and streaming memory but requires thread lifecycle, queue synchronization, backpressure, and error forwarding. Noted here for future investigation.

## Expansion tier

These are reasonable next additions once the core contract is stable, but they are not blocked on: large UTF-8 / large binary, fixed-size binary, duration, timezone-aware timestamps, and accepting dictionary-encoded inputs by decoding to plain values. None of these require implementation before 0.1.0 is sealed.

## Nullability

Nullability is part of the contract rather than an afterthought. Methods must declare whether fields are nullable, and implementations must not silently weaken or strengthen nullability without updating the manifest and this document. The server preserves null semantics faithfully for all supported types.

## Nested types

Structs and lists are in scope and tested. Nesting depth is unrestricted in principle. Deeply nested `MAP` or `UNION` combinations are not tested beyond single-level examples and should not be claimed as supported until tests exist.

## Method schemas

Method schemas declare concrete field names and types rather than informal descriptions. `exec` declares a request schema with non-null `sql: utf8` and non-null `want_result: bool`. Its metadata reply declares non-null `rows_changed: uint64`, non-null `statement_type: int32`, and non-null `result_type: int32`. `query_open` declares a request schema with non-null `sql: utf8`, nullable `batch_rows: uint64`, and nullable `batch_bytes: uint64`. Its JSON control reply declares non-null `session_id: uint64`, non-null `session_token: string`, non-null `state: string`, non-null `next_method: string`, and non-null `idle_timeout_ms: uint64`. `fetch`, `close`, and `cancel` carry JSON request objects with non-null `session_id: uint64` and non-null `session_token: string`.

The dynamic row schema returned by `fetch` is fixed for the life of the session. `fetch` must not change column names or logical types across batches of the same session. End-of-stream, cancellation acknowledgement, and already-closed status are communicated through JSON control metadata, not through empty Arrow tables with sentinel columns.

## Compatibility decisions

If the server normalizes an input type, that normalization must be written here. Canonical output is part of the contract: the server emits ordinary `utf8` rather than `large_utf8`, emits `binary` rather than `large_binary`, and preserves DuckDB timestamp unit variants as the matching Arrow timestamp unit. If a client sends `large_utf8`, behaviour is currently unspecified and should not be relied upon.

## Checklist for new type claims

Before a new type or encoding is added to the contract: the Arrow logical type must be named here, the DuckDB mapping must be declared here or in the manifest, nullability behaviour must be explicit, canonical output behaviour must be explicit, and test coverage must exist for both successful roundtrip and failure on unsupported types. If those conditions are not met, the claim is not yet part of the public contract.
