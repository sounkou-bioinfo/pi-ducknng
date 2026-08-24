# Security review: ducknng extension (whole codebase)

_Reviewed surfaces: outbound HTTP + credential profiles, inbound HTTP server, TLS/auth, NNG wire/RPC/IPC parsing and C memory safety, and the SQL-exposed body/arrow/RPC deserialization._

## Status (latest)

**All five findings are now fixed and re-verified.** Findings 1–4 were fixed in
commit `5258a9b` (see "Pass 2 — fix verification"); finding 5 was fixed in commit
`4b1d9fd` (see "Finding 5 — fix verification" below). The most recent pass also
re-swept the body **codec** surface (percent-decode / form-urlencoded, hex, and
the user-codec dispatch) and the **local NNG patches** in `patches/nng/` (see
"Vendored NNG patches" below), and found nothing exploitable. No open issues
remain in the reviewed surfaces.

Scope note: upstream `third_party/nng/` itself is out of scope; only our local
divergence (the two files under `patches/nng/`) is reviewed here.

Line-number note: every `file:line` below is as of the revision that was
audited, not a live pointer. The Quack core extraction moved several of these
checks into `src/ducknng_quack_core.c` — finding 2's counted-blob bound, cited
as `src/ducknng_quack.c:222`, is now `ducknng_qk_read_counted()`, reached
through `ducknng_quack_read_blob_view()`. Locate a finding by the function name
given with it rather than by the line number, and treat the numbers as
historical record. The fixes themselves are covered by the native property
suite and the fuzz corpus, which is what actually guards against regression.

A later addition: `ducknng_quack_read_type_info()` accepted ExtraTypeInfo
fields 200 and 201 in either order, letting an ENUM publish an `enum_count`
that its `enum_labels` allocation never matched — a heap over-read plus wild
`duckdb_free()` reachable from any untrusted schema. The count is now assigned
only where the labels are allocated, the reversed field order is rejected, and
both shapes are tracked as property regressions and corpus seeds.

Summary:

| # | Severity | Category | Location | Status |
| --- | --- | --- | --- | --- |
| 1 | HIGH | cert-validation-bypass | `src/ducknng_nng_compat.c:320`, `src/ducknng_http_compat.c:32` | ✅ fixed |
| 2 | HIGH | integer-overflow / oob-read | `src/ducknng_quack.c:222` (sink `:1553`) | ✅ fixed |
| 3 | HIGH | integer-overflow / oob-read | `src/ducknng_quack.c:1648` | ✅ fixed |
| 4 | MEDIUM | http-header-injection | `src/ducknng_http_compat.c:362` (decoder `:216`) | ✅ fixed |
| 5 | HIGH | integer-overflow / **oob-write + arbitrary-free** | `src/ducknng_quack.c:1897` (`payload_bind_columns`) | ✅ fixed (`4b1d9fd`) |

---

## HIGH 5 (new) — Unbounded column count → heap OOB write in quack schema bind

`integer-overflow / oob-write` · `src/ducknng_quack.c:1897`, write at `:1909`
(also the NAMES loop `:1919`)

`ducknng_quack_payload_bind_columns` reads the top-level column count `ncols`
from the untrusted payload as a ULEB128 and, **with no upper bound**, allocates
`duckdb_malloc(sizeof(*out_schema->cols) * (size_t)ncols)`. Every other
count-driven allocation in this decoder is explicitly bounded
(`DUCKNNG_QUACK_MAX_ENUM_VALUES` at `:1117,1141`, `DUCKNNG_QUACK_MAX_STRUCT_MEMBERS`
at `:1101`, array size capped to `uint32`); this top-level count is the lone
exception. On a 64-bit target the multiply wraps `size_t`, so a crafted `ncols`
yields a small allocation, after which the loop `out_schema->cols[i] = *node`
writes full `ducknng_quack_column_schema` structs — which contain heap pointers
(`name`, `enum_labels`, `children`, `child_names`) — past the end of the buffer.

This is reachable from untrusted remote data: the payload is the RPC reply for
`ducknng_query_rpc(url, sql, tls)` / `ducknng_query_rpc_mode(...)`
(`src/ducknng_sql_rpc.c:1522`, decoded when the reply's
`serialization_mode = "ducknng_quack_batch"`) and a body decode
(`src/ducknng_sql_body.c:1801`). A malicious or MITM server the client queries
controls every field.

- **Exploit (write path):** reply whose outer result-types section declares
  `ncols = 2^61 + 1` (chosen so `sizeof(col) * ncols` wraps to exactly one slot),
  followed by two or more minimal valid type nodes. `duckdb_malloc` returns a
  1-slot buffer; the loop then writes `cols[1]`, `cols[2]`, … out of bounds with
  attacker-influenced struct contents (including pointer fields) — remote heap
  corruption, a stronger primitive than the OOB *reads* in findings 2–3.
- **Amplification (cleanup path — worse and easier to trigger):**
  `out_schema->ncols` is set to the huge `ncols` **before** the wrapped
  allocation (`:1897`). The `fail:` path (`:1970`) calls
  `ducknng_quack_schema_reset`, which loops `for i < schema->ncols` running
  `ducknng_quack_node_free_contents(&cols[i])` (`:1825`). So *any* failure after
  the allocation — trivially, declare a huge `ncols` and truncate the payload so
  the first `read_type_node` fails — walks billions of entries over the one-slot
  buffer, doing OOB reads of `cols[i].name`/`.children`/… and `duckdb_free()` on
  those garbage pointers. That is a remote **arbitrary-free / OOB-free** reachable
  with a tiny payload and no completed writes (only the `wrapped-to-small-nonzero`
  case; a wrap to exactly 0 yields `duckdb_malloc(0)`, and if that returns NULL
  the `!cols` guard makes the NULL case safe).
- **Fix:** bound `ncols` before the multiply/allocation — reject
  `ncols > DUCKNNG_QUACK_MAX_STRUCT_MEMBERS` (or a dedicated column max), mirroring
  the existing enum/struct guards — and/or size the allocation with the checked
  `ducknng_size_mul(sizeof(*out_schema->cols), ncols, &bytes)`. Also set
  `out_schema->ncols` only *after* a successful allocation so the cleanup loop can
  never walk an unallocated count. The NAMES loop (`:1919`) and `node_to_duckdb`
  loop inherit the bound once `ncols` is capped.
- **Confidence:** 0.9

---

## Original findings (Pass 1)

The originally-pending diff (`ducknng_http_profile.c` scope/expiry/header
changes) was a strict tightening and introduced no vulnerability; findings 1–4
were pre-existing.

| # | Severity | Category | Location |
| --- | --- | --- | --- |
| 1 | HIGH | cert-validation-bypass | `src/ducknng_nng_compat.c:320`, `src/ducknng_http_compat.c:32`, default `src/ducknng_sql_tls.c:164,212,261` |
| 2 | HIGH | integer-overflow / oob-read | `src/ducknng_quack.c:222` (sink `:1553`) |
| 3 | HIGH | integer-overflow / oob-read | `src/ducknng_quack.c:1648` |
| 4 | MEDIUM | http-header-injection | `src/ducknng_http_compat.c:362` (decoder `:216`) |

---

## HIGH 1 — Outbound TLS clients skip server-certificate verification by default

`cert-validation-bypass` · `src/ducknng_nng_compat.c:320` and
`src/ducknng_http_compat.c:32`, default set at `src/ducknng_sql_tls.c:164,212,261`

`auth_mode` is one field applied to both server and client TLS configs, and it
defaults to `0`, which maps to `NNG_TLS_AUTH_MODE_NONE`. On a **client** config
(`NNG_TLS_MODE_CLIENT`, used by `ducknng_socket_apply_tls` and the HTTPS client),
`NONE` disables verification of the *server's* certificate. The code still calls
`nng_tls_config_server_name()`, which makes it look like hostname validation is
happening. The catalog documents `auth_mode` purely from a server/peer angle
(`0 = no peer authentication`), so a user configuring an *outbound* client
naturally leaves it at `0`. Registration even requires a cert or CA when
`auth_mode==0` (`ducknng_sql_tls.c:166,214`), so the exact trap is: supply a CA
to pin the server, leave `auth_mode` default, and get a connection that accepts
**any** certificate.

- **Exploit:** `ducknng_tls_config_from_pem(cert, key, ca_pem, pwd /*auth_mode defaults to 0*/)`
  then `ducknng_dial_socket('tls+tcp://host', …)` or `ducknng_ncurl('https://host/…', …)`.
  A network MITM presents any certificate; the client accepts it and the
  "TLS-protected" session tokens and payloads are fully interceptable.
- **Fix:** For `NNG_TLS_MODE_CLIENT`, do not map `auth_mode==0` to `NONE` — treat
  it as `REQUIRED` (at least whenever a CA is present), or split the field into
  distinct server-verify vs. client-verify semantics with clients defaulting to
  `REQUIRED`.
- **Confidence:** 0.8

## HIGH 2 — Integer overflow in quack reader bounds check → heap OOB read

`integer-overflow / oob-read` · `src/ducknng_quack.c:222` (sink at `:1553`)

The one bounds gate for every read is `if (!r || r->off + n > r->len)`, all
`size_t`. `n` is an attacker-supplied ULEB128 (up to 2^64−1) cast straight to
`size_t` in `ducknng_quack_read_blob_view`. `r->off + n` wraps modulo 2^64, so
an enormous length passes the check. For a VARCHAR/BLOB column the resulting
`(data, len)` view goes directly to
`duckdb_unsafe_vector_assign_string_element_len(out_vec, …, data, (idx_t)len)` —
copying `len` bytes from a pointer near the end of the payload. The quack decoder
parses the **RPC reply body received over the NNG socket**
(`serialization_mode = "ducknng_quack_batch"`), so a malicious or MITM peer
controls every field.

- **Exploit:** reply declaring one VARCHAR column, one row, one string whose
  ULEB128 length is `0xFFFFFFFFFFFFFFF0`; `off + n` wraps below `len`, the guard
  passes, and the assign reads ~2^64 bytes → crash or heap info disclosure. (The
  sibling `read_string_dup` path is safe only because it `duckdb_malloc(len+1)`
  first; the vector-assign path has no such guard.)
- **Fix:** make the gate overflow-safe — `if (!r || n > r->len - r->off)` with an
  `off <= len` invariant, or use the project's existing
  `ducknng_size_add(r->off, n, &end)` and compare `end > r->len`. This decoder is
  the one place that hand-rolls `+`/`*` instead of those checked helpers.
- **Confidence:** 0.85

## HIGH 3 — Integer overflow in fixed-width column size check → heap OOB read

`integer-overflow / oob-read` · `src/ducknng_quack.c:1648`

For fixed-width columns the decoder validates `data_len != width * (size_t)src_rows`,
then `memcpy(dst, data_blob + width*offset, width*out_count)`. `src_rows` comes
from the attacker-controlled chunk row count `rows_u64` cast to `idx_t` with
**no upper bound** on this sliced (non-nested) path (only the nested branch
bounds it against `duckdb_vector_size()`). `width * src_rows` is unchecked, so a
huge `src_rows` wraps to a small value matching a small real `data_len`;
`out_count` is then clamped to the vector size (2048) and the memcpy reads
`width * 2048` bytes from a much smaller blob.

- **Exploit:** one BIGINT column (`width=8`), chunk `rows = 2^61+1`, an 8-byte
  data blob. `8 * (2^61+1) mod 2^64 = 8 == data_len`, guard passes, memcpy reads
  `8*2048 = 16 KB` from an 8-byte source → OOB read into the output vector.
- **Fix:** reject `rows_u64 > duckdb_vector_size()` on the sliced path (as the
  nested branch already does) and compute the size with `ducknng_size_mul`,
  erroring on overflow.
- **Confidence:** 0.8

## MEDIUM — Header value from `headers_json` is not CRLF-validated (request smuggling)

`http-header-injection` · `src/ducknng_http_compat.c:362` (decoder at `:216`,
wire emission in vendored `http_msg.c`)

`ducknng_http_apply_headers_json_common` validates each header **name** as an
HTTP token (`:368`) but applies **no validation to the value**. The JSON string
decoder produces raw bytes including `\n` (0x0A) and `\r` (0x0D) — and the
`\uXXXX` branch explicitly allows code points ≤ 0x7f (`:242`), so control
characters pass. The value is stored verbatim by `nng_http_req_add_header` and
serialized as `"%s: %s\r\n"`. Notably, the pending diff just *added* exactly this
control-char check for **profile** auth values
(`ducknng_http_profile_header_value_valid`, `ducknng_http_profile.c:107`), so
caller-supplied header values are now the inconsistent gap.

- **Exploit:** an application that builds `headers_json` from untrusted data,
  e.g. a value `"a\r\n\r\nGET /admin HTTP/1.1\r\nHost: …"`, terminates the header
  block early and smuggles a second request. Combined with a profile, the
  credential `Authorization` header is merged *after* caller headers, so injected
  CRLFs can relocate the credential-bearing bytes or inject an overriding `Host:`.
- **Fix:** reject values containing bytes `< 0x20` or `0x7f` right after the
  name-token check in `ducknng_http_apply_headers_json_common` (covers both
  request and response header paths), and constrain the `\uXXXX` decoder to reject
  control code points — reusing the predicate the profile path already uses.
- **Confidence:** 0.8

---

## Lower-severity hardening notes (not blocking)

From the inbound-HTTP audit — worth fixing but below the HIGH/MEDIUM bar:

- **Admission runs after route lookup / body-limit / frame-decode.** In
  `ducknng_http_route_handler` the 404 and 413 responses
  (`ducknng_http_compat.c:1281,1286`) and in `ducknng_http_rpc_handler` the 400
  frame-decode rejection (`:1492`) happen *before*
  `ducknng_service_network_admission_check` (`:1309,:1517`). A client outside the
  IP allowlist / lacking an mTLS identity can enumerate which routes exist and
  probe size limits. The NNG transport rejects earlier (at pipe-connect). Move the
  admission check ahead of route lookup and body inspection.
- **Route-local identity allowlist uses `strstr`, not JSON parsing**
  (`ducknng_http_compat.c:1358`). The quoting and server-added
  `tls:cn:`/`tls:san:` prefix make a practical bypass hard, and the authoritative
  peer allowlist (`ducknng_nng_compat.c:243`) uses exact `strcmp`, but this
  secondary filter should parse and compare array elements exactly.
- **Response header values aren't CRLF-checked**
  (`ducknng_http_apply_headers_json_common`, response path) — same helper as the
  MEDIUM above, so one shared value-charset check fixes both directions.

## Verified clean

Profile credential scoping (parse-vs-wire path identity, scheme/host/port/path-
boundary matching, no redirect re-attachment, `tls_required` enforcement,
auth-value never exposed via list/introspection/errors); session token generation
(128-bit from `nng_random`) and constant-time token comparison; central
fail-closed admission for NNG and HTTP; `openssl` CN interpolation guarded by
`ducknng_cn_is_safe`; the frame decoder `ducknng_decode_frame_bytes` and
percent-decoder bounds; SQL-injection paths in the JSON/codec/tempfile readers
(quoted literals + prepared params + internal-only paths); Arrow decode
backstopped by nanoarrow `VALIDATION_LEVEL_FULL`; and AIO reply-message ownership
(no UAF/double-free found).

## Priority

Fix the two quack overflows and the TLS client default first — the quack bugs are
remotely triggerable memory corruption from any server a ducknng client talks to,
and the TLS default silently voids server authentication for every outbound TLS
user who doesn't override `auth_mode`.

---

## Pass 2 — fix verification

Re-reviewed the working-tree fixes for findings 1–4; all correct and complete.

- **Finding 1 (TLS):** `ducknng_tls_auth_mode_map` now takes `nng_tls_mode` and
  maps `auth_mode==0 → NNG_TLS_AUTH_MODE_REQUIRED` for `NNG_TLS_MODE_CLIENT`
  (`ducknng_nng_compat.c:320`, `ducknng_http_compat.c:32`); both call sites
  updated; server semantics unchanged. New regression test
  `test/sql/ducknng_tls_client_verification.test` asserts a client with the wrong
  CA is rejected.
- **Finding 2 (reader OOB read):** the gate is now overflow-safe
  (`r->off > r->len || n > r->len - r->off`, `ducknng_quack.c:223`),
  `read_blob_view` rejects `len > SIZE_MAX`, and the string sink converts length
  via the checked `ducknng_quack_size_to_idx`.
- **Finding 3 (fixed-width OOB read):** the size check uses `ducknng_size_mul`,
  the memcpy slice is now bounds-checked (`copy_offset`/`copy_len` vs `data_len`),
  and the `rows > duckdb_vector_size()` cap was hoisted to cover all (not just
  nested) paths in `decode_data_chunk_slice`. New checked `idx_add`/`idx_mul`
  helpers are used consistently for row-count math.
- **Finding 4 (CRLF header injection):** a shared `ducknng_http_header_value_is_valid`
  (`ducknng_util.c`) rejects control bytes; `ducknng_http_parse_json_string` now
  rejects `\b \f \n \r \t` escapes, control `\uXXXX` code points, and raw control
  bytes at parse time; the check is applied to request headers, response
  `Content-Type` (both buffered and chunked paths), `headers_build`, the stream
  route content type, and the profile header-collision path. (Minor, non-security
  behavior change: HTAB `0x09` in a header value, technically legal per RFC 7230,
  is now rejected too — acceptable and conservative.)

No regressions were introduced by these fixes.

## Finding 5 — fix verification

Fixed in commit `4b1d9fd` ("Bound Quack schema column counts"), verified correct
and complete:

- `ncols` is rejected when `> DUCKNNG_QUACK_MAX_STRUCT_MEMBERS` **before** any
  allocation (`ducknng_quack.c:1902`), then converted with the checked
  `ducknng_quack_uint64_to_idx` and sized with `ducknng_size_mul` — the wrapping
  multiply is gone.
- `out_schema->ncols` is now assigned **after** a successful allocation
  (`:1922`), so the cleanup path (`ducknng_quack_schema_reset` freeing
  `cols[i]` for `i < ncols`) can never walk an unallocated/oversized count — the
  amplified remote arbitrary-free is closed.
- The write loop bound (`i < ncols`) and the allocation are now consistent
  (both ≤ 65536). A matching bound was also added to the encode side (`:1361`).
- New property tests (`test/property/ducknng_prop.c`) and a SQL test
  (`test/sql/ducknng_body_codecs.test`) cover the bound.

No open issues remain in the reviewed surfaces.

---

## Vendored NNG patches (our divergence only)

Scope: only the local delta recorded under `patches/nng/`; upstream NNG is not
reviewed. Both patches are clean — no security finding.

**`0001-windows-rtools42-timespec-fallback.patch`** — relaxes the Windows CMake
gate (drops the `timespec_get` requirement) and adds a
`GetSystemTimeAsFileTime()` fallback for `nni_time_get` on MinGW/Rtools42. No
attacker-controlled input; it is a platform clock source. Not security-relevant.

**`0002-emscripten-wasm-inproc-progress.patch`** — entirely `__EMSCRIPTEN__`-gated
(browser wasm side-module only); compiles to no-ops on native/release builds.
Verified:

- The `ducknng_nng_wasm_trace` points emit only **static string literals** (no
  payloads, tokens, or PII) and are further gated behind `DUCKNNG_WASM_TRACE`
  (off by default) — no information disclosure even when enabled.
- `nni_plat_init` skipping pollq/resolver/`atfork` under `DUCKNNG_WASM_INPROC_ONLY`
  is browser-only and scoped to transports absent from the sandbox;
  `nni_plat_fini` skips the matching teardown, so init/fini stay balanced (no
  double-free / use-of-uninitialized). Worst case if a skipped subsystem were
  ever reached is a crash (browser-only), not memory corruption.
- The comma-operator trace insertions in `init.c`'s `||` init chain preserve the
  exact short-circuit logic and return values — no behavior change.

Consistency note (non-security): the trace helper's guarded `#include <stdio.h>`
appears in `aio.c` / `reap.c` / `taskq.c` / `posix_thread.c` because those TUs
lack stdio otherwise; `init.c` relies on its pre-existing **unconditional**
`#include <stdio.h>`, so it compiles in every configuration and needs no added
include.
