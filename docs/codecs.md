# ducknng body codec providers

`ducknng` keeps transport bytes, protocol frames, and method payloads separate. The body codec provider layer is the opt-in serialization/deserialization layer for raw HTTP bodies and other content-type-tagged `BLOB` values. It is inspired by `nanonext` serialization providers, but it is keyed by media type instead of R object class and it is not part of the framed RPC method registry.

The raw HTTP primitives remain `ducknng_ncurl(...)` and its async companion `ducknng_ncurl_aio(...)` / `ducknng_ncurl_aio_collect(...)`. They return status, headers, `body BLOB`, and a best-effort UTF-8 `body_text` column without interpreting the response by `Content-Type`. Parsed body behavior is explicit:

```sql
SELECT * FROM ducknng_list_codecs();
SELECT * FROM ducknng_parse_body(body, content_type);
SELECT * FROM ducknng_ncurl_table(url, method, headers_json, body, timeout_ms, tls_config_id[, profile_id]);
```

`ducknng_parse_body(...)` parses an existing `BLOB` using the supplied content type. `ducknng_ncurl_table(...)` performs one HTTP/HTTPS request, requires a 2xx response status, extracts the response `Content-Type`, and parses the response body into a DuckDB table. When `profile_id` is supplied, it uses the same scoped outbound HTTP profile resolver as `ducknng_ncurl(...)` and sends the request only after scope checks pass and caller headers do not collide with the profile's injected auth header. Its schema is inferred from the response at bind time, so it cannot be used as a lateral per-row HTTP call or as the retry primitive inside a recursive CTE. Use `ducknng_ncurl(...)` instead when you need volatile row-by-row HTTP execution, non-2xx status inspection, response headers, or raw bytes, then parse the body explicitly once the response you want has been selected.

Format-specific functions are also available for formats where DuckDB's own readers provide better type inference and dialect detection than the built-in codec:

```sql
SELECT * FROM ducknng_parse_csv(body);       -- DuckDB read_csv_auto via tempfile
SELECT * FROM ducknng_parse_tsv(body);       -- DuckDB read_csv_auto(delim='\t') via tempfile
SELECT * FROM ducknng_parse_parquet(body);   -- DuckDB read_parquet via tempfile
```

These write the body bytes to a cross-platform temporary file and delegate to DuckDB's standard CSV and Parquet SQL readers, then decode the result through the Arrow IPC path. They exist alongside the content-type dispatch in `ducknng_parse_body` as an explicit opt-in for callers who want DuckDB's full reader capabilities.

The initial built-in providers are deliberately conservative. Unknown or missing content types fall back to raw `BLOB` output. `text/*` bodies are exposed as `VARCHAR` only when the bytes are valid UTF-8 text. `application/vnd.ducknng.frame` is decoded with the same envelope shape as `ducknng_decode_frame(...)`. Arrow IPC stream bytes are decoded through nanoarrow and the same stable manual DuckDB vector mapping used by `ducknng_query_rpc(...)`. `application/vnd.ducknng.quack-batch` decodes a standalone ducknng `ducknng_quack_batch` body using the native C Quack-derived codec. The body may contain one or more DuckDB `DataChunk` results, but it is intentionally not registered for upstream Quack's `application/vnd.duckdb` envelope. Malformed Quack-derived bodies fail closed when schema column counts, length arithmetic, fixed-width byte counts, row counts, nested child sizes, dictionary selections, compressed-vector fields, repeated
chunk types, cumulative materialized values, or object terminators would overflow,
disagree, or exceed supported decoder bounds. Constant and dictionary vectors are materialized to flat output;
DuckDB integer sequence vectors are generated directly; FSST is rejected. The
dependency-free `tools/quack_compressed_fixtures.R` generates the executable
v1.5.2 literals; `make quack-fixtures` checks them against the SQL test.

JSON bodies are parsed in memory through DuckDB's JSON scalar functions (`json_structure(...)` and `from_json(...)`) and then serialized through the existing Arrow IPC row mapping before returning rows to the caller. This avoids temporary files while still letting DuckDB own JSON type inference and conversion.

CSV, TSV, and Parquet body parsing is provided through two independent paths. The content-type dispatch in `ducknng_parse_body` uses a direct C parser for CSV/TSV (type inference: INT64, DOUBLE, VARCHAR with nanoarrow encoding) and a tempfile-backed DuckDB `read_parquet()` call for Parquet. The standalone `ducknng_parse_csv(body)`, `ducknng_parse_tsv(body)`, and `ducknng_parse_parquet(body)` functions always write to a temporary file and delegate to DuckDB's standard readers (`read_csv_auto`, `read_parquet`), which provide better dialect detection and type inference at the cost of a tempfile round-trip.

JSON parsing runs its nested DuckDB query on the runtime's dedicated codec connection, which is independent of the per-session pool connection executing a remote request. Because the codec does not re-enter the request's own connection, JSON parsing works the same inside service-owned SQL — SQL executed through remote `exec` / `ducknng_query_rpc(...)` — as it does in a local client query. Raw, text, CSV/TSV/Parquet fallback, frame, direct Arrow IPC decoding, and direct `ducknng_quack_batch` decoding do not need a nested DuckDB query at all.

User-extensible body codecs are part of the 0.1.0 surface through `ducknng_register_codec(content_type, function_name)` and `ducknng_unregister_codec(content_type)`. The contract is deliberately narrow: `function_name` must be a plain SQL identifier (ASCII letter/underscore start, then alphanumeric/underscore/dot only — no semicolons, parentheses, or expression syntax) naming a single-argument scalar function whose argument type is `BLOB` and whose return type is `VARCHAR`. The codec layer dispatches by splicing the identifier into a fixed `SELECT <fn>(?::BLOB) AS value` shape, so the parsed table for a user codec is always a single-row, single-column `value VARCHAR` result. Returning `NULL` is allowed and surfaces as a `NULL` row.

User codecs take precedence over built-ins when their content type matches, so registering a hook for `application/json` overrides the built-in JSON parser for as long as it is registered. Content-type matching is case-insensitive and tolerates parameters such as `; charset=utf-8`. Unregistering an unknown content type returns `FALSE` rather than erroring; unregistering a registered hook restores the built-in behavior. `ducknng_list_codecs()` reports the registered user hook with `kind = 'user'` and the SQL function name in the `function_name` column alongside the built-in providers.

User codecs run their `SELECT <fn>(?::BLOB)` query on the same dedicated codec connection as the built-in JSON provider, so they work inside service-owned SQL such as `ducknng_query_rpc(...)` execution just as they do locally. The dispatch path stays predictable because the codec connection is separate from both the runtime init connection and the per-session pool connection executing the request, so there is no re-entrancy on the request thread's connection. This is verified end-to-end in `test/sql/ducknng_codec_in_rpc.test`.

The HTTP server RPC endpoint does not use this provider layer to accept arbitrary web payloads. An HTTP/HTTPS listener started through `ducknng_start_server(...)` still accepts only `POST` requests whose body is one complete `application/vnd.ducknng.frame` envelope. The provider layer is for local parsing and client-side HTTP convenience, not a second server protocol.
