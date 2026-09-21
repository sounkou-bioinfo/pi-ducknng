# ducknng type support and schema policy

This document defines the Arrow IPC and DuckDB type contract used by framed RPC. DuckDB logical types are the execution types; Arrow IPC is the portable request and row encoding. Unsupported or malformed encodings fail before row materialization.

## Arrow IPC mappings

The receive path constructs these DuckDB types:

| Arrow storage | DuckDB |
| --- | --- |
| `null` | `SQLNULL` |
| `bool` | `BOOLEAN` |
| signed and unsigned 8/16/32/64-bit integers | matching DuckDB integer |
| `float32`, `float64` | `FLOAT`, `DOUBLE` |
| `utf8`, `large_utf8` | `VARCHAR` |
| `binary`, `large_binary`, `fixed_size_binary` | `BLOB` |
| `date32`, `date64` | `DATE` |
| `time32`, `time64` | `TIME`, or `TIME_NS` for nanoseconds |
| timestamp seconds/milliseconds/microseconds/nanoseconds | matching DuckDB timestamp unit |
| timestamp with timezone metadata | `TIMESTAMP WITH TIME ZONE` |
| decimal32/64/128 | `DECIMAL(precision, scale)` |
| duration and Arrow interval variants | `INTERVAL` |
| list and large list | `LIST` |
| fixed-size list | `ARRAY` |
| struct | `STRUCT` |
| map | `MAP` |
| dense and sparse union | `UNION` |

List, array, struct, map, and union values may be nested recursively. Parent and child nulls are preserved. The decoder validates offsets, lengths, type ids, and child bounds before assigning DuckDB vectors.

DuckDB's Arrow producer supplies response schemas and batches. Some DuckDB values therefore have canonical wire projections rather than identical round trips:

- `HUGEINT` and `UHUGEINT` use `decimal128(38,0)` and return as `DECIMAL(38,0)`.
- `UUID` and `ENUM` use UTF-8 and return as `VARCHAR`.
- `TIME WITH TIME ZONE` returns as `TIME`; its offset is not preserved.
- `TIMESTAMP WITH TIME ZONE` uses an Arrow timezone timestamp and returns as DuckDB `TIMESTAMP WITH TIME ZONE`; the instant is preserved, not an original named-zone identity.
- DuckDB strings and blobs are emitted as ordinary `utf8` and `binary`, although the receiver also accepts large and fixed-size variants.

These normalizations are part of the contract and are covered by `test/sql/ducknng_rpc_client_smoke.test`.

## SQL parameter tuples

`exec`, `query_open`, and `query_prepare` accept an optional one-row Arrow `STRUCT` field named `params`. Its children form a positional tuple: child order binds SQL `?` parameters, while child names are descriptive only. The protocol limit is 65,535 parameters. A missing or null `params` row means no parameters.

Parameters use the same Arrow-to-DuckDB mappings, including nested list, array, struct, map, and union values. The server passes each value to `duckdb_bind_value`; values are never interpolated into SQL text. DuckDB's C value API represents a top-level null as untyped `SQLNULL`, so the SQL must provide type context, for example `?::BIGINT`. Nulls inside typed containers retain their container type.

Parameterized `exec` and `query_open` accept exactly one SQL statement. `query_prepare` also accepts exactly one statement and returns its schema without executing it. These contracts are exercised by `test/sql/ducknng_parameter_binding.test`.

## Method schemas

`exec` declares non-null `sql: utf8`, non-null `want_result: bool`, and nullable `params: struct`. Its metadata reply contains `rows_changed: uint64`, `statement_type: int32`, and `result_type: int32`.

`query_open` and `query_prepare` declare non-null `sql: utf8`, nullable `batch_rows: uint64`, nullable `batch_bytes: uint64`, and nullable `params: struct`. `query_open` returns JSON session control metadata. `query_prepare` returns a zero-row Arrow stream carrying only the prepared result schema.

The dynamic row schema returned by `fetch` is fixed for the session. End-of-stream and cancellation are control metadata, not sentinel row schemas.

## Bounds and unsupported encodings

The Arrow vector decoder caps recursive conversion at 64 levels and validates IPC through nanoarrow. Unknown or malformed layouts fail with a DuckDB-visible error.

Dictionary-preserving round trips, Arrow extension-type semantics, and run-end encoded arrays are not supported. `ENUM` is deliberately normalized to UTF-8; extension metadata is not treated as an executable type registry.
