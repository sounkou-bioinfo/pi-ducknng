# Quack protocol and serializer study

This note studies Quack from the upstream C++ implementation rather than from a
client library. The goal is to make the wire format concrete enough that
`ducknng` can later grow a real C implementation that decodes, encodes, and
converts Quack messages to and from Arrow through nanoarrow C.

The upstream sources inspected for this note are under `/tmp/duckdb-quack` at
commit `1693647` on branch `v1.5-variegata`: `src/quack_message.cpp`,
`src/include/quack_message.hpp`, `src/serialize_quack_message.cpp`,
`src/quack_server.cpp`, `src/quack_client.cpp`, `src/quack_http_server.cpp`,
`src/quack_scan.cpp`, `src/storage/quack_insert.cpp`,
`src/quack_start_stop.cpp`, and `src/quack_extension.cpp`. The serializer itself
is DuckDB core code, not Quack code, so this note also uses
`/tmp/duckdb-quack/duckdb/src/common/serializer/`,
`duckdb/src/common/types/data_chunk.cpp`, `duckdb/src/common/types/vector.cpp`,
`duckdb/src/common/types.cpp`, and
`duckdb/src/storage/serialization/serialize_types.cpp`.

## What Quack is

Quack is an HTTP-carried DuckDB RPC protocol. The current upstream server exposes
`POST /quack` and returns `application/vnd.duckdb` response bodies. The body is a
DuckDB `BinarySerializer` byte stream containing one Quack message. There is no
JSON inside the RPC path, no Arrow IPC inside the RPC path, and no outer Quack
length prefix on HTTP: the HTTP request or response body is the message. The
inspected C++ server does not validate the request content type before reading the
body.

The endpoint is connection-oriented at the message layer. A client first sends a
connection request with an authentication string. The server creates a
server-side DuckDB `Connection`, assigns a random connection id, and returns that
id. Later prepare, fetch, append, and disconnect messages carry this connection
id in the message header. The server rejects non-connection messages whose
connection id is unknown.

The row payload is DuckDB `DataChunk` serialization. A `PrepareResponseMessage`
contains result column names, result logical types, an initial list of
`DataChunkWrapper` values, a `needs_more_fetch` flag, and a random `result_uuid`.
A `FetchRequestMessage` carries the `result_uuid`; a `FetchResponseMessage`
returns more `DataChunkWrapper` values and a batch index. An `AppendRequestMessage`
carries one `DataChunkWrapper` to append into a remote table.

## Transport behavior in the C++ implementation

`HttpQuackServer` constructs a `duckdb_httplib::Server`, registers `POST /quack`,
reads the full request body into a DuckDB `MemoryStream`, calls
`QuackServer::HandleMessage`, serializes the response into the same stream, and
sets the response content type to `application/vnd.duckdb`.

The server also registers `GET /` as a human-readable informational endpoint and
`OPTIONS /quack` for CORS. The server uses a 128-thread httplib pool,
`set_keep_alive_max_count(128)`, `set_keep_alive_timeout(10)`, and TCP no-delay.
Errors are usually protocol-level `ERROR_RESPONSE` messages rather than HTTP
status errors.

The client uses DuckDB `HTTPUtil`, builds the URL as `uri.Http() + "/quack"`,
serializes the request to a `MemoryStream`, posts it with default HTTP headers in
the inspected code, and deserializes the response body from the returned HTTP
buffer. `QuackClientConnection` caches HTTP clients for a Quack connection and
sends a `DISCONNECT_MESSAGE` from its destructor when possible.

Quack URIs have the `quack:` scheme and default to port 9494. The URI parser
also tolerates `quack://` by normalizing it to `quack:`. A server listens without
SSL; clients enable HTTPS for non-local URIs unless disabled. The HTTP URL
materialized from a Quack URI is `http://host:port` or `https://host:port`.
`quack_serve` now throws `NotImplementedException` on `__EMSCRIPTEN__`, so the
upstream extension currently supports connecting to existing endpoints on wasm but
not starting a Quack server there.

## Message byte layout

`QuackMessage::ToMemoryStream` is the authoritative framing code. It rewinds the
stream, creates a DuckDB `BinarySerializer`, sets
`SerializationOptions::serialization_compatibility = SerializationCompatibility::FromIndex(7)`,
and serializes two top-level objects back to back:

1. the `MessageHeader` object;
2. the concrete message body object selected by `header.type`.

Each object is a DuckDB `BinarySerializer` object. Each property starts with a
fixed `field_id_t`, which is `uint16_t`, written directly as two bytes. DuckDB
writes this in host order; the supported platforms here are little-endian, so a
portable C implementation should treat field ids as little-endian unsigned 16-bit
values. An object ends with field id `0xffff`.

There is no type tag attached to a property. The decoder must know the object
schema from the message type, field id, and expected C++ type. Because there is
no generic length for an arbitrary property, unknown fields are not generally
skippable unless the decoder has type knowledge for that field. Compatibility
therefore depends on fixed field order plus optional/default fields, not on a
self-describing TLV format.

The top-level message shape is:

```text
message := object(MessageHeader) object(MessageBody(header.type)) EOF
object  := (field_id payload)* 0xffff
field_id := uint16 little-endian in practice
```

The C++ deserializer reads the header object first, then begins a second object
and dispatches body decoding based on the header type. A C decoder should reject
trailing bytes after the second object unless it deliberately implements message
framing for a non-HTTP transport.

## DuckDB BinarySerializer primitives

Quack inherits DuckDB's BinarySerializer encoding rules.

Field ids are fixed 16-bit values. Lists are encoded as an unsigned LEB128 count
followed by each element. Nullable pointers are encoded as a one-byte boolean
present flag followed by the pointed-to object when present. `WriteProperty` is
required and always emits its field. `WritePropertyWithDefault` omits the field
when DuckDB considers the value the default.

Primitive payloads are:

| C++ type | Encoding |
|---|---|
| `bool` | one byte, 0 or 1 |
| unsigned integers up to 64-bit | unsigned LEB128 |
| signed integers up to 64-bit | signed LEB128 |
| enums | underlying integer, then the corresponding integer encoding; `BinarySerializer` forces `serialize_enum_as_string = false` |
| `hugeint_t` | signed LEB128 upper 64 bits, then unsigned LEB128 lower 64 bits |
| `uhugeint_t` | unsigned LEB128 upper 64 bits, then unsigned LEB128 lower 64 bits |
| `float` | raw 32-bit float bytes, little-endian in practice |
| `double` | raw 64-bit float bytes, little-endian in practice |
| `string` / `string_t` | unsigned LEB128 byte length, then UTF-8/string bytes |
| raw data pointer | unsigned LEB128 byte length, then raw bytes |
| `optional_idx` | valid index as unsigned integer, or DuckDB invalid index (`uint64_t` max) |

DuckDB's LEB128 implementation is in `EncodingUtil`. Unsigned LEB128 emits seven
payload bits per byte and sets bit 7 while more bytes follow. Signed LEB128 emits
seven payload bits per byte and stops once the remaining value is all sign bits,
using bit 6 of the final byte as the sign bit.

A C implementation should not call these DuckDB internals. It should implement
its own bounded LEB128 routines with explicit overflow checks, byte-count caps,
and an input cursor that reports truncation without undefined behavior.

## Message types

The message type enum is serialized as its underlying `uint8_t` through LEB128.
The upstream enum is:

| Id | Message type | Direction |
|---:|---|---|
| 0 | `INVALID` | never valid |
| 1 | `CONNECTION_REQUEST` | client to server |
| 2 | `CONNECTION_RESPONSE` | server to client |
| 3 | `PREPARE_REQUEST` | client to server |
| 4 | `PREPARE_RESPONSE` | server to client |
| 7 | `FETCH_REQUEST` | client to server |
| 8 | `FETCH_RESPONSE` | server to client |
| 9 | `APPEND_REQUEST` | client to server |
| 10 | `SUCCESS_RESPONSE` | server to client |
| 11 | `DISCONNECT_MESSAGE` | client to server |
| 100 | `ERROR_RESPONSE` | server to client |

The server accepts only connection, prepare, fetch, append, and disconnect
requests. It rejects response messages received from clients.

## Message header

The header object is generated from `MessageHeader::Serialize`:

| Field id | Name | C++ type | Required? | Meaning |
|---:|---|---|---|---|
| 1 | `type` | `MessageType` | yes | Message type id |
| 2 | `connection_id` | `string` with default | omitted when empty | Server-assigned connection/session id |
| 3 | `client_query_id` | `optional_idx` | yes | DuckDB client query id, invalid index when absent |

`client_query_id` is always serialized by the generated C++ code. The client code
attempts to derive it from the active DuckDB transaction for logging, although in
`HttpsQuackClient::RequestInternal` the call to `ToMemoryStream` currently occurs
before the logging block that sets the query id. A C implementation should follow
the serialized format, not the logging intent.

## Message body fields

The generated C++ serializer file `serialize_quack_message.cpp` is the source of
truth for body fields.

### `CONNECTION_REQUEST`

| Field id | Name | Type | Encoding notes |
|---:|---|---|---|
| 1 | `auth_string` | `string` default | token or auth material |
| 2 | `client_duckdb_version` | `string` default | populated by C++ constructor |
| 3 | `client_platform` | `string` default | populated by C++ constructor |
| 4 | `min_supported_quack_version` | `idx_t` default | current constructor uses 1 |
| 5 | `max_supported_quack_version` | `idx_t` default | current constructor uses 1 |

The server rejects the request if `min_supported_quack_version > 1`. It does not
perform a full range intersection check in the inspected code. Authentication is
delegated to the configured DuckDB scalar function
`quack_authentication_function`, called as `SELECT function(session_id,
auth_string, server_token)`.

### `CONNECTION_RESPONSE`

| Field id | Name | Type | Encoding notes |
|---:|---|---|---|
| 1 | `server_duckdb_version` | `string` default | populated by C++ constructor |
| 2 | `server_platform` | `string` default | populated by C++ constructor |
| 3 | `quack_version` | `idx_t` default | current server version 1 |

The connection id is not in this body; it is the header `connection_id` field.

### `PREPARE_REQUEST`

| Field id | Name | Type | Encoding notes |
|---:|---|---|---|
| 1 | `sql_query` | `string` default | SQL text to execute |

The server authorizes prepare requests through `quack_authorization_function`,
called as `SELECT function(connection_id, sql_query)`. It then uses the
connection's DuckDB `Connection::SendQuery`. The resulting `QueryResult` remains
open on the server-side Quack connection.

### `PREPARE_RESPONSE`

| Field id | Name | Type | Encoding notes |
|---:|---|---|---|
| 1 | `result_types` | `vector<LogicalType>` default | result schema |
| 2 | `result_names` | `vector<string>` default | result column names |
| 3 | `needs_more_fetch` | `bool` default | true when the initial batch hit the chunk limit |
| 4 | `results` | `vector<unique_ptr<DataChunkWrapper>>` default | initial chunks |
| 5 | `result_uuid` | `hugeint_t` | server-side result id |

`quack_fetch_batch_chunks` controls the maximum number of DuckDB chunks returned
per prepare or fetch response. The upstream default is 12. `needs_more_fetch` is
computed as `results.size() == max_chunks_per_batch`; if the result has exactly
that many chunks remaining, the next fetch may still return no rows.

### `FETCH_REQUEST`

| Field id | Name | Type | Encoding notes |
|---:|---|---|---|
| 1 | `uuid` | `hugeint_t` | must match the connection's current result UUID |

If the UUID does not match, the server responds with `ERROR_RESPONSE` saying the
result has been closed.

### `FETCH_RESPONSE`

| Field id | Name | Type | Encoding notes |
|---:|---|---|---|
| 1 | `results` | `vector<unique_ptr<DataChunkWrapper>>` default | fetched chunks; omitted or empty at end |
| 2 | `batch_index` | `optional_idx` | assigned for fetch batches |

For fetches with an active result, the server increments `next_batch_index` and
returns it. `QuackScan` uses this as partition/order metadata. A default
constructed `FetchResponseMessage` has an invalid `batch_index`.

### `APPEND_REQUEST`

| Field id | Name | Type | Encoding notes |
|---:|---|---|---|
| 1 | `schema_name` | `string` default | target schema |
| 2 | `table_name` | `string` default | target table |
| 3 | `append_chunk` | `unique_ptr<DataChunkWrapper>` default | chunk to append |

Append authorizes a dummy insert string, locates the target table, wraps the
incoming chunk in a `ColumnDataCollection`, and calls `Connection::Append`.

### `SUCCESS_RESPONSE`, `DISCONNECT_MESSAGE`, and `ERROR_RESPONSE`

`SUCCESS_RESPONSE` and `DISCONNECT_MESSAGE` have empty bodies. `ERROR_RESPONSE`
has field 1, `message`, encoded as a default string. Error responses carry the
error in the body, not as an HTTP error payload.

## DataChunkWrapper and DataChunk encoding

`DataChunkWrapper` exists because upstream Quack wants to serialize DuckDB
`DataChunk` values through generated message code. Its `Serialize` method writes
one object field:

| Field id | Name | Type |
|---:|---|---|
| 300 | `chunk` | nested `DataChunk` object |

When a `DataChunkWrapper` is stored in a `unique_ptr`, DuckDB serializes a
nullable pointer first. In a `vector<unique_ptr<DataChunkWrapper>>`, each list
element is therefore:

```text
present: bool
if present:
  object(DataChunkWrapper) = field 300 object(DataChunk) 0xffff
```

A DuckDB `DataChunk` serializes as:

| Field id | Name | Type | Meaning |
|---:|---|---|---|
| 100 | `rows` | `sel_t` (`uint32_t`) | row count |
| 101 | `types` | list of `LogicalType` | one type per column |
| 102 | `columns` | list of vector objects | one vector per column |

Each DataChunk includes its logical types even when the containing
`PrepareResponseMessage` already has `result_types`. A full upstream-compatible C
implementation must decode the per-chunk types and should verify that they match
the response schema for query results.

DuckDB asserts that it should not serialize empty data chunks. End of stream is
represented by an empty `results` list or omitted/default results, not by a
zero-row `DataChunk`.

## LogicalType encoding

`LogicalType::Serialize` writes:

| Field id | Name | Type | Meaning |
|---:|---|---|---|
| 100 | `id` | `LogicalTypeId` | logical type id enum |
| 101 | `type_info` | `shared_ptr<ExtraTypeInfo>` default | type parameters, children, alias, extension info |

Important `LogicalTypeId` numeric values from the DuckDB source include:

| Id | Type |
|---:|---|
| 10 | `BOOLEAN` |
| 11 | `TINYINT` |
| 12 | `SMALLINT` |
| 13 | `INTEGER` |
| 14 | `BIGINT` |
| 15 | `DATE` |
| 16 | `TIME` |
| 17 | `TIMESTAMP_SEC` |
| 18 | `TIMESTAMP_MS` |
| 19 | `TIMESTAMP` |
| 20 | `TIMESTAMP_NS` |
| 21 | `DECIMAL` |
| 22 | `FLOAT` |
| 23 | `DOUBLE` |
| 24 | `CHAR` |
| 25 | `VARCHAR` |
| 26 | `BLOB` |
| 27 | `INTERVAL` |
| 28 | `UTINYINT` |
| 29 | `USMALLINT` |
| 30 | `UINTEGER` |
| 31 | `UBIGINT` |
| 32 | `TIMESTAMP_TZ` |
| 34 | `TIME_TZ` |
| 35 | `TIME_NS` |
| 49 | `UHUGEINT` |
| 50 | `HUGEINT` |
| 54 | `UUID` |
| 100 | `STRUCT` |
| 101 | `LIST` |
| 102 | `MAP` |
| 104 | `ENUM` |
| 107 | `UNION` |
| 108 | `ARRAY` |
| 109 | `VARIANT` |

`ExtraTypeInfo` starts with these common fields:

| Field id | Name | Type |
|---:|---|---|
| 100 | `type` | `ExtraTypeInfoType` |
| 101 | `alias` | `string` default |
| 103 | `extension_info` | `unique_ptr<ExtensionTypeInfo>` default |

Specific type info then adds fields. The most important for Arrow conversion are:

| Extra type | Numeric id | Extra fields |
|---|---:|---|
| `DECIMAL_TYPE_INFO` | 2 | field 200 `width` (`uint8_t` default), field 201 `scale` (`uint8_t` default) |
| `STRING_TYPE_INFO` | 3 | field 200 `collation` (`string` default) |
| `LIST_TYPE_INFO` | 4 | field 200 `child_type` (`LogicalType`) |
| `STRUCT_TYPE_INFO` | 5 | field 200 `child_types` (`child_list_t<LogicalType>`, a list of name/type pairs) |
| `ENUM_TYPE_INFO` | 6 | field 200 `values_count`, field 201 `values` list of strings, after common fields |
| `ARRAY_TYPE_INFO` | 9 | field 200 `child_type`, field 201 `size` (`uint32_t` default) |

A minimal C library does not need to implement every DuckDB logical type on day
one. It should decode enough of the type object to either map supported types to
Arrow exactly or reject unsupported types with a precise error before reading the
corresponding vectors.

## Vector encoding

Each column vector is serialized as a DuckDB vector object. For normal flat
vectors, field 90 (`vector_type`) is absent because DuckDB reads it with an
explicit default of `FLAT_VECTOR`. Compressed/vector-special cases may write field
90.

The vector type enum values are:

| Id | Vector type | Upstream behavior |
|---:|---|---|
| 0 | `FLAT_VECTOR` | default when field 90 absent |
| 1 | `FSST_VECTOR` | enum exists; current serialization code has a TODO rather than emitting it here |
| 2 | `CONSTANT_VECTOR` | field 90 then a recursive vector of count 1 |
| 3 | `DICTIONARY_VECTOR` | field 90, selection vector, dictionary count, then dictionary vector |
| 4 | `SEQUENCE_VECTOR` | field 90, sequence start, sequence increment; no validity/data fields |

### Flat vectors

Flat vectors write:

| Field id | Name | Type | Meaning |
|---:|---|---|---|
| 99 | `geometry_format` | enum | only for `GEOMETRY` in compatible versions |
| 100 | `has_validity_mask` | `bool` | whether field 101 follows |
| 101 | `validity` | raw bytes | DuckDB validity mask, only when not all valid |
| 102 | `data` | raw bytes or list | fixed-size physical data, strings, blobs, or geometry |
| 103 | `children` | list of vector objects | struct children |
| 104 | `list_size` or `array_size` | integer | list child cardinality or fixed array size |
| 105 | `entries` | list of offset/length objects | list entries |
| 106 | `child` | vector object | list child vector |

For constant-size physical types, field 102 is a raw data pointer whose byte
length must equal `GetTypeIdSize(physical_type) * row_count`. DuckDB obtains the
bytes via `VectorOperations::WriteToStorage`. Values are in DuckDB physical
storage representation. Examples: integers and floats are fixed little-endian
machine values, dates are integer days, timestamps are integer epoch counts in
the type's unit, intervals are DuckDB `interval_t`, and hugeints/UUIDs use
DuckDB's 128-bit layout.

For `VARCHAR` and `BLOB` physical storage, field 102 is a list with one string
payload per row. Null rows still have list entries in the serialized stream; the
validity mask determines whether the string should be used. A decoder should
consume every row's string payload even when the row is null.

For `STRUCT`, field 103 is a list of child vector objects, one per struct child.
Each child vector is serialized for the same row count as the parent.

For `LIST`, field 104 is the total child list size. Field 105 is a list of
per-row objects with field 100 `offset` and field 101 `length`. Field 106 is the
child vector serialized with `list_size` rows. Null parent rows have offset and
length zero in the serialization code.

For `ARRAY`, field 103 is the fixed array size and field 104 is the child vector
serialized with `array_size * row_count` rows.

### Constant, dictionary, and sequence vectors

When compressed serialization is enabled and the serializer compatibility
supports it, DuckDB may emit non-flat vectors:

* `CONSTANT_VECTOR`: field 90 = 2, followed by a recursive vector serialized with
  count 1. A decoder materializes that one value across the requested row count.
* `DICTIONARY_VECTOR`: field 90 = 3, field 91 raw `sel_t[count]` selection vector,
  field 92 dictionary count, followed by the dictionary vector serialized for the
  dictionary count. A decoder applies the selection vector to the decoded
  dictionary.
* `SEQUENCE_VECTOR`: field 90 = 4, field 91 signed sequence start, field 92 signed
  sequence increment. A decoder materializes `start + row * increment` for the
  requested row count. There are no validity/data fields in this case.

The current upstream `Vector::Serialize` has a TODO for other compressed vector
types such as FSST in this path. A C implementation should reject `FSST_VECTOR`
until actual upstream emission is verified with fixtures.

### Versioned logical types outside the current codec

“Full type spectrum” must always be qualified by a pinned DuckDB version and a
usable public API. DuckDB v1.5.2 defines `GEOMETRY` logical id 60 and `VARIANT`
logical id 109. Geometry vectors add field 99 to select WKB or older spatial
storage, and geometry type information may include a CRS. Variant uses a
struct-like physical layout, but that release's C API has no
`DUCKDB_TYPE_VARIANT` constructor or documented variant-vector accessors. The
current pure-C codec rejects both. Treating geometry as an ordinary blob or
variant as an ordinary struct would discard logical semantics and is not
compatibility.

## Result batching semantics

The helper `CreateBatch` repeatedly calls `query_result->Fetch()` until it has
`max_chunks` chunks, reaches end of stream, or observes an error. End of stream
resets the stored `QueryResult`. The default `quack_fetch_batch_chunks` setting
is 12.

`PREPARE_RESPONSE` includes the first batch of chunks. `FETCH_RESPONSE` contains
subsequent batches. The protocol does not expose a row count in the response
outside the chunks themselves. The client detects completion when a fetch response
contains no chunks.

The C++ scan path uses initial chunks from bind, then allows parallel local
states to fetch more chunks with cached HTTP clients. `FetchResponseMessage`'s
`batch_index` is surfaced through DuckDB partition data so order-preserving
operators can avoid reordering parallel fetch batches.

## How this differs from `ducknng_quack_batch`

`ducknng_quack_batch` is a Quack-derived batch codec inside the `ducknng` RPC
session contract. It is not byte-compatible with upstream Quack messages.
Notable differences are:

* upstream Quack serializes a header object plus a typed message body; `ducknng`
  RPC has its own method envelope;
* upstream Quack and canonical `ducknng_quack_batch` chunks both use DuckDB
  `DataChunk` fields 100/101/102. ducknng emits flat vectors and materializes
  flat, constant, dictionary, and integer sequence vectors on decode; FSST
  remains rejected because this serializer path does not emit it;
* upstream Quack repeats chunk types inside every `DataChunk`; canonical ducknng
  output now does the same and validates those types against the result schema.
  `ducknng_quack_batch` may omit the outer schema header in later fetch payloads
  under the negotiated ducknng session contract;
* upstream Quack has connection, fetch, append, and disconnect messages with
  `result_uuid`; `ducknng` RPC uses `query_open`, `fetch`, `close`, and
  `session_id` / `session_token`.

A future `ducknng_quackurl(...)` surface should therefore be treated as a separate
Quack-compatible adapter, not as a rename of `ducknng_quack_batch`.

## C library design for decode, encode, and Arrow conversion

A real C implementation should mirror the C++ layering while keeping DuckDB C++
objects out of the core codec.

### Layer 1: binary reader/writer

Implement a small bounded reader/writer for the DuckDB BinarySerializer subset:

* byte cursor with explicit length and error state;
* fixed little-endian `uint16_t` field ids;
* object begin/end helpers and required/optional field helpers;
* unsigned and signed LEB128 with overflow checks;
* string/blob read and write helpers;
* list count helpers with configured maximums;
* nullable pointer helpers.

This layer must not allocate based on untrusted lengths without checking size
limits. It should report truncation, overflow, unexpected field id, missing
field, duplicate field if relevant, unsupported value, and trailing bytes as
distinct errors.

### Layer 2: message model

Define C structs for:

* `ducknng_quack_header`;
* connection request/response;
* prepare request/response;
* fetch request/response;
* append request;
* success, disconnect, and error response.

The message decoder should parse the header first, validate the message type,
then parse the matching body. For `DataChunkWrapper` lists, expose chunks either
as decoded objects or as borrowed slices for lazy decoding. Lazy decoding is
valuable because a client may want only schema or only error metadata.

### Layer 3: logical types

Define a compact C representation of supported DuckDB logical types with child
arrays for list, struct, map, and array. Map DuckDB logical types to Arrow schema
using nanoarrow:

* integers, booleans, floats, dates, times, timestamps, decimals, strings, blobs,
  intervals, hugeints, UUIDs;
* lists and structs after flat scalar support is proven;
* enums either as dictionary or as resolved UTF-8, depending on the public
  contract for the new surface;
* reject union, variant, aggregate state, geometry, extension/user types until a
  precise mapping is documented and tested.

The decoder should preserve original DuckDB logical type ids and decimal width /
scale even when Arrow uses a more general physical representation.

### Layer 4: DataChunk/vector decoder

Decode `DataChunk` into an internal batch object that can export an
`ArrowArray`/`ArrowSchema`. The first supported vector set should be:

* flat vectors for fixed-size scalar physical types;
* validity masks;
* flat `VARCHAR` and `BLOB` vectors;
* decimal, date, time, timestamp, interval, hugeint, and UUID physical layouts
  once their exact Arrow mappings are documented;
* constant and sequence vectors by materializing or by constructing Arrow buffers
  directly;
* dictionary vectors after selection-vector handling is tested.

For fixed-size flat vectors, the decoder can borrow from the message buffer when
exporting Arrow only if the Arrow release callback owns or references the message
buffer. Otherwise it must copy into Arrow-owned buffers. For strings and blobs,
DuckDB serializes row-wise length-prefixed values, so the decoder must build Arrow
offsets and a contiguous value buffer.

### Layer 5: Arrow to Quack encoder

The reverse direction should accept a nanoarrow `ArrowArray` + `ArrowSchema` and
produce Quack `DataChunk` serialization. Start with the same supported subset as
the decoder. The encoder must choose DuckDB logical types explicitly; it cannot
infer all DuckDB type details from Arrow alone. Decimal width/scale, timestamp
unit/timezone, UUID-vs-fixed-binary, blob-vs-string, and enum handling must be
provided by schema metadata or an explicit type mapping object.

For append support, the encoder wraps one encoded `DataChunk` in
`AppendRequestMessage` field 3. For query responses, it wraps chunks in
`PrepareResponseMessage` or `FetchResponseMessage` lists.

### Layer 6: Quack HTTP adapter

The protocol codec should stay transport-independent. A small HTTP adapter can
then implement upstream-compatible `POST /quack` and return `application/vnd.duckdb`
responses while mapping decoded messages to `ducknng` server/session operations,
or implement a client helper that sends Quack messages to an upstream Quack server.

## Compatibility and version policy

Quack protocol version is currently 1 and is negotiated in the connection
messages. The BinarySerializer compatibility in the upstream C++ code is fixed to
`SerializationCompatibility::FromIndex(7)`. That serializer compatibility is a
separate concern from the Quack protocol version and is not independently
negotiated by the inspected code.

The C library should therefore make both version values explicit:

* Quack protocol version 1 in connection messages;
* DuckDB BinarySerializer compatibility index 7 for message encoding;
* a hard failure for unsupported future message fields or type encodings until
  fixtures prove compatibility.

Because DuckDB BinarySerializer is not a generic stable external serialization
spec, byte-for-byte tests against upstream Quack are mandatory. Compatibility
claims should be limited to the DuckDB/Quack version range covered by fixtures.

## Security and robustness requirements

A C decoder for this format must be defensive. Required limits include:

* maximum message bytes;
* maximum object nesting depth;
* maximum list length;
* maximum column count;
* maximum row count per chunk;
* maximum total vector data bytes;
* maximum string/blob bytes per value and per chunk;
* maximum number of chunks per message.

The decoder must check all arithmetic before multiplying row counts by physical
widths. It must reject overlong LEB128 encodings, truncated fields, invalid field
order, unexpected object terminators, invalid vector type ids, invalid logical
child counts, malformed list entries, and validity masks with inconsistent sizes.

Authentication and authorization are not properties of the serializer. Upstream
Quack delegates them to DuckDB scalar functions and relies on the server token and
transport security around HTTP/HTTPS. A `ducknng` Quack-compatible surface should
keep transport security, authn, authz, and serialization as separate layers.

## Test fixture plan

Before implementing broad support, capture fixtures from the upstream C++ code:

1. connection request and response;
2. prepare request for a simple query;
3. prepare response for scalar columns: bool, signed/unsigned integers, floats,
   date/time/timestamp, decimal, varchar, blob, interval, hugeint, UUID;
4. fetch response with multiple chunks and batch index;
5. empty fetch response at end of stream;
6. append request for a small chunk;
7. error response;
8. constant vector, sequence vector, dictionary vector where upstream emits them;
9. list, struct, array, enum fixtures after scalar support is stable.

Each fixture should include the SQL that generated it, the DuckDB and Quack
versions, the platform, a hex dump or binary file, expected decoded JSON metadata,
and expected Arrow schema/batch checksums. Tests should cover decode, encode,
round-trip byte identity for messages we generate, Arrow conversion, and
interop with an upstream Quack server/client.

## Implementation sequence for `ducknng`

A safe implementation order is:

1. Add a standalone C binary serializer module with no DuckDB connection or NNG
   dependencies.
2. Decode and encode message headers and scalar-only control messages.
3. Decode `LogicalType` for a narrow scalar subset and map it to nanoarrow schema.
4. Decode flat scalar `DataChunk` values to nanoarrow arrays.
5. Encode the same scalar subset from nanoarrow arrays to `DataChunk` bytes.
6. Add `PrepareResponse` and `FetchResponse` decode to an Arrow array stream.
7. Add `AppendRequest` encode from Arrow batches.
8. Add upstream-compatible HTTP `POST /quack` client/server paths as a separate
   surface from `ducknng` RPC.
9. Expand to nested and compressed vector types only with upstream fixtures and
   documented Arrow mappings.

This keeps the project honest: the serializer is real DuckDB BinarySerializer /
DataChunk compatibility, not Arrow IPC wrapped in a different name, and Quack
compatibility remains a separate public surface from the existing `ducknng` RPC
contract.
