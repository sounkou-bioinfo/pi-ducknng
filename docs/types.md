# ducknng type contract

`ducknng` moves tables through two negotiated row codecs. Arrow IPC is the
portable default. `ducknng_quack_batch` preserves DuckDB chunks for DuckDB-aware
peers. The codec is independent of the NNG, HTTP, or WebSocket carrier.

The executable contract is the end-to-end matrix in
`test/sql/ducknng_type_roundtrip.test`. It sends values through a running server
and compares the decoded values on the client over both codecs. Parameter
binding is covered separately by `test/sql/ducknng_parameter_binding.test`.

## Arrow IPC

The receive path accepts these Arrow storage types and constructs the listed
DuckDB logical types:

| Arrow | DuckDB |
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

Nested combinations of list, array, struct, map, and union are decoded
recursively. Null parent values and null children are preserved.

The DuckDB-to-Arrow producer uses DuckDB's Arrow conversion, so a few DuckDB
types have canonical wire projections rather than identical logical types:

- `HUGEINT` and `UHUGEINT` are emitted as `decimal128(38,0)` and return as
  `DECIMAL(38,0)`. Values outside that decimal domain are not representable.
- `UUID` and `ENUM` are emitted as UTF-8 and return as `VARCHAR`.
- `TIME WITH TIME ZONE` returns as `TIME`; its offset is not preserved.
- `TIMESTAMP WITH TIME ZONE` uses an Arrow timezone timestamp and returns as
  DuckDB `TIMESTAMP WITH TIME ZONE`; the instant is preserved, not an original
  named-zone identity.
- Ordinary DuckDB strings and blobs are emitted as `utf8` and `binary` even
  though the receiver also accepts large and fixed-size variants.

These are declared normalizations, not silent claims of identity.

## Quack-derived batches

`ducknng_quack_batch` is not Arrow. It carries DuckDB logical types and data
chunks through the ducknng session protocol. The round-trip matrix covers the
scalar numeric, string/blob, temporal, decimal, interval, huge integer, UUID,
list, array, struct, map, and union families, including recursive nesting and
nested nulls. Flat vectors are canonical output. On input, constant and
dictionary vectors are materialized through DuckDB's selection-copy C API, so
the same scalar and recursive type families remain available; sequence vectors
are accepted for the signed and unsigned integer logical types that DuckDB emits
as sequences. FSST vectors remain unsupported and fail closed. A client
selecting this codec must implement the ducknng Quack batch format; selecting it
does not change session ownership, fetch, cancel, or end-of-stream semantics.

DuckDB v1.5.2 also defines `GEOMETRY` (logical id 60) and `VARIANT`
(logical id 109), but neither is part of this codec contract. `GEOMETRY` adds a
versioned vector-format field and optional CRS type metadata that this codec does
not yet preserve. `VARIANT` is a struct-like internal logical type, but v1.5.2's
C API does not expose a `DUCKDB_TYPE_VARIANT` constructor or vector contract.
Both fail closed rather than being silently normalized to `BLOB` or `STRUCT`.
Support must be gated and tested against each pinned DuckDB serializer/C API
version; the presence of an internal logical id alone is not a public C contract.

## SQL parameter tuples

`exec`, `query_open`, and `query_prepare` may receive an optional one-row Arrow
`STRUCT` named `params`. Its children form a positional tuple: child order binds
the SQL `?` parameters, while child names are descriptive only. The protocol
limit is 65,535 parameters. A missing or null `params` row means no parameters.

Parameters use the same Arrow-to-DuckDB value mappings above, including nested
list, array, struct, map, and union values. They are bound with
`duckdb_bind_value`; values are never interpolated into SQL text. DuckDB's C
value API represents a top-level null as untyped `SQLNULL`, so null parameters
need type context in the SQL, for example `?::BIGINT`. Nulls inside a typed
list, struct, map, array, or union retain their container type.

Parameterized `exec` and parameterized `query_open` accept exactly one SQL
statement. `query_prepare` also accepts exactly one statement and returns its
schema without executing it.

## Bounds and unsupported encodings

The Arrow producer rejects logical nesting deeper than 16. The receiver caps
recursive schema and value conversion at 64, in addition to nanoarrow IPC
validation. The Quack decoder has its own checked length arithmetic and nesting
bound. Unknown or malformed types fail with a DuckDB-visible error before row
materialization.

Dictionary-preserving round trips, Arrow extension-type semantics, and run-end
encoded arrays are not supported. `ENUM` is deliberately normalized to UTF-8;
arbitrary extension metadata is not treated as an executable type registry.
