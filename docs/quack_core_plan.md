# Quack core and deterministic-testing plan

This is the ordered implementation plan for making the Quack-derived codec fast,
independently testable, and measurably covered. It also records the smaller
SQLite/TigerBeetle-style test seams that are justified elsewhere in ducknng.
The binding contracts remain `docs/protocol.md`, `docs/types.md`,
`docs/codecs.md`, and `docs/lifetime.md`; this file does not override them.
Completed plan statements must graduate into code, executable tests, and those
binding documents. Delete this file when every surviving item has done so.

## Rules for every phase

- Deterministic parsing, validation, sizing, and state transitions are pure C.
- A function-pointer table is introduced only for a real external effect with a
  production implementation and a scripted test implementation.
- Hostile input is rejected by checked production branches. `assert()` is only
  for internal invariants in debug, sanitizer, fuzz, and test builds.
- Input-driven addition, multiplication, narrowing, allocation, nesting, and
  materialization are checked before use.
- Borrowed DuckDB vectors, `duckdb_string_t` data, wire slices, and `nng_msg`
  bodies never outlive their owner.
- No benchmark or coverage claim is made without a recorded tool report and a
  reproducible command.
- Each phase lands its focused tests before the next architectural change.

## Baseline

The pre-change native property suite passed on 2026-08-04 with deterministic
seed `0xd17c0ffee1234567`: 23 tests passed under both ASan and UBSan, with 2,000
trials for each generated property. The output is retained by the Pi session as
`.pi/tasks/session-1290620-1290620/be8c57f02.output`. This baseline covers
checked sizes, dotted paths, JSON subject arrays, random and valid frames,
upload prefixes, transport URLs, and bounded Quack parser entry points. It is
not a complete Quack round-trip, fuzz, coverage, or MC/DC result.

## Phase 1 — Preserve current malformed-input and status regressions

**Status:** complete.

Implemented focused native regressions before restructuring:

- one frame consumes exactly its counted payload and rejects every trailing
  suffix;
- every emitted error status survives encode/decode;
- legacy error frames with an unassigned status byte decode explicitly as
  `DUCKNNG_STATUS_UNSPECIFIED`;
- status bits are rejected on non-error frames;
- semantic flags remain confined to their assigned bits;
- invalid boolean presence markers, incomplete nested type metadata, mismatched
  extra-info kinds, invalid decimal shapes, malformed arrays/maps/unions, and
  input-driven count/allocation bounds fail closed;
- existing compressed Quack fixtures remain accepted.

The binding protocol and SQL frame projection now expose status explicitly.
Evidence: 26 native tests passed under ASan and UBSan with 2,000 generated
trials per property; compressed fixtures were current; and all 32 release SQL
tests passed, including type round trips, uploads, body codecs, negative paths,
and mTLS. One first full-suite run observed a transient mTLS HTTP test failure;
the same test passed alone and the complete 32-test rerun passed.

## Phase 2 — Add coverage-guided hostile-byte fuzzing

**Status:** complete.

Created dependency-minimal libFuzzer entry points for:

- complete frame decoding;
- upload-prefix decoding;
- Quack schema and chunk traversal.

The tracked corpus is seeded from the Rducks Quack corpus, ducknng compressed
fixtures, canonical current/legacy frames, and an upload prefix. The dictionary,
ASan/UBSan make targets, temporary writable corpora, and requested CI workflow
are present. Both sanitizer harnesses passed smoke campaigns and the final extracted-core
build passed 250,000 inputs each (`ASan`: 608 covered edges/1,118 features,
388 MiB peak; `UBSan`: 1,087 covered edges/2,068 features, 38 MiB peak). The workflow remains to
be observed in GitHub after push, but its local commands pass. Accepted frames
are canonicalized and decoded again; accepted Quack schemas are
traversed through the strict row-count path. Quack canonical re-encoding remains
a Phase 4/5 property because the current monolith has no independent encoder.

## Phase 3 — Finish strict frame/status behavior

**Status:** complete.

Keep the 22-byte envelope layout. Assign the high byte of the existing 32-bit
flags word to the status enum and the low 24 bits to semantic flags. Error
frames carry a nonzero documented status; pre-assignment error frames decode as
`UNSPECIFIED`. Calls, results, manifests, and events require status zero. The
service dispatcher must preserve `ducknng_method_reply.status` when constructing
the reply.

The counted frame measure/encode/decode functions use no NNG operation; the NNG
wrapper allocates the final message body once and delegates to the counted
encoder. `ducknng_decode_frame`, `ducknng_frame_status`, and decoded AIO rows
expose status, and the protocol documentation distinguishes it from NNG errors.
A later source split will place these functions in their own translation unit
so the fuzz harness no longer needs aborting link stubs for retained adapter
functions.

## Phase 4 — Extract a dependency-free Quack core

**Status:** in progress.

The dependency-free counted frame implementation is now
`ducknng_wire_core.[ch]`, with `ducknng_wire.c` limited to the NNG allocation
adapter. `ducknng_quack_core.[ch]` now owns the checked fixed/measure writer,
reader, fields, booleans, ULEB128/SLEB128, counted-byte primitives, wire logical
IDs, dependency-free type-tree representation, type classification/equality,
and structural type-shape validation. The existing DuckDB parser/encoder
consumes those authorities and only duplicates adapter error ownership. The release build,
all 32 SQL tests, and ASan/UBSan properties pass after that integration.

The type/schema parser, structural validation, borrowed vector views, and
DuckDB conversion are not yet separated from `ducknng_quack.c`; therefore this
phase is not complete and no whole-codec pure-core claim is made.

Create:

```text
src/ducknng_quack_core.c
src/include/ducknng_quack_core.h
src/ducknng_quack_duckdb.c
```

`ducknng_quack_core` owns byte cursors, BinarySerializer field parsing, type and
schema structural validation, exact encoded-size calculation, fixed-buffer
encoding, and borrowed vector wire views. It includes no DuckDB, NNG, HTTP, R,
SQL, mutex, or thread API.

`ducknng_quack_duckdb` owns DuckDB logical-type conversion, borrowed vector
views, short/long `duckdb_string_t` handling, flat-vector materialization, and
compressed-vector materialization. `src/ducknng_quack.c` is reduced to result,
session, upload, and table-function integration.

The core consumes contiguous input spans and caller-owned output spans. It does
not place function-pointer dispatch in per-value loops. Schema/type ownership
has one destroy path, and borrowed wire views remain valid only while their
payload owner remains alive.

## Phase 5 — Add structured shrinking properties

**Status:** in progress behind Phase 4.

The suite now generates shrinking Quack byte-core integer cases and nested
schema payloads, checks strict arbitrary-byte parsing, and carries deterministic
edge tests for overflow, truncation, null arguments, exact frame sizing, and
status shapes. Full canonical batch/type/value round trips remain blocked on
the rest of the Phase 4 split.

Add theft generators and shrinkers for supported type trees, schemas, chunks,
validity patterns, scalar values, nested values, and compressed-vector forms.
Required properties include:

- accepted batch -> decode -> encode -> equivalent canonical batch;
- every proper prefix of a canonical batch is rejected;
- exact sizing equals bytes written;
- an undersized destination never writes out of bounds and reports required or
  failed size consistently;
- schema/type trees survive canonical round-trip;
- row, child, selection, dictionary, and cumulative materialization bounds hold;
- short strings at DuckDB inline-size boundaries and long strings preserve
  bytes including embedded NULs;
- arbitrary bytes are rejected or canonicalized without leaks or undefined
  behavior.

Failing seeds and minimized bytes become tracked regressions.

## Phase 6 — Enforce measured core coverage and MC/DC

**Status:** initial extracted-core gate complete; expand with Phases 4/5.

Add debug/test-only verification macros without changing production validation:

- `DUCKNNG_ASSERT` for internal invariants in debug/test builds;
- `DUCKNNG_TESTCASE` for boundary values, significant mask bits, and shared
  switch destinations;
- `DUCKNNG_TEST_ONLY` for declarations needed only by verification code.

Use Clang 18 source-based coverage with:

```text
-fprofile-instr-generate
-fcoverage-mapping
-fcoverage-mcdc
```

Add `make quack-coverage`, `make quack-mcdc`, and
`make quack-mcdc-check`. The first 100% MC/DC gate covers only the declared pure
set: Quack core, frame core, checked arithmetic, and any pure lifecycle
transition modules already extracted. LLVM's `--show-mcdc-summary` report is the
authority. gcov branch percentages are not MC/DC, and no whole-extension claim
is permitted until the measured file set actually expands to the whole
extension.

Defensive branches reachable from malformed input are never hidden with
`ALWAYS()` or `NEVER()`. Any genuinely unreachable internal condition requires
a debug assertion and an explicit coverage rationale.

`make quack-mcdc-check` now instruments only `ducknng_wire_core.c` and
`ducknng_quack_core.c`; linked DuckDB adapters and test/vendor code are
uninstrumented. Clang/LLVM 18 measured 93/93 MC/DC conditions (100%): 2/2 in the
checked-arithmetic core, 36/36 in the frame core, and 55/55 in the extracted
Quack byte/type-tree core. The same report measured 100% functions, 98.70%
lines, and 91.02% branches in those three files. The executable gate parses
LLVM's JSON summary and fails for zero
conditions, missing files, or any uncovered MC/DC condition. This is explicitly
not a whole-codec or whole-extension claim. Every newly extracted pure schema,
sizing, validation, or lifecycle module must join the enforced file set.

## Phase 7 — Encode directly into the final NNG message

**Status:** in progress; fetch reply path complete.

The Quack fetch path now measures while retaining its fetched DuckDB chunks,
allocates one `nng_msg` for the complete frame prefix plus payload, encodes the
payload directly at its final offset, then fills the counted frame prefix in
place. `ducknng_method_reply` owns that prebuilt message until the dispatcher
validates flags and size and transfers it to NNG. Reset/error paths free either
the old DuckDB-allocated payload or the prebuilt message, never both. The 32 SQL
tests, including Quack scalar/nested type round trips and empty results, pass on
this path. Arrow replies and Quack upload requests retain their existing
ownership paths; the upload request's final frame copy remains to be removed or
justified by measurement before this phase is complete.

Expose an exact two-pass core API equivalent to:

```c
int ducknng_quack_measure_batch(
    const ducknng_quack_batch_view *batch, size_t *required);
int ducknng_quack_encode_batch(
    const ducknng_quack_batch_view *batch,
    uint8_t *destination, size_t capacity, size_t *written);
```

Production measures the complete envelope plus payload, allocates or resizes one
final `nng_msg`, and writes the header and Quack payload directly into that
message body. Remove the growing intermediate Quack writer and the subsequent
`nng_msg_append` payload copy from the native path.

A `duckdb_string_t_data()` pointer is borrowed only for the immediate bounded
copy while its data chunk is alive. Inline short-string bytes live in the
`duckdb_string_t` object itself; no retained iovec or asynchronous callback may
reference them. Long strings follow the same immediate-copy rule. Fixed-width
vectors and validity regions are copied directly to their final wire ranges.

Received Quack scans retain the owning `nng_msg` while wire views point into its
body. Materializing output into DuckDB vectors remains an ownership-required
copy.

## Phase 8 — Benchmark the claimed production path

**Status:** in progress; bounded IPC comparison recorded.

On 2026-08-04, the uncommitted working tree based on `437f295` was built
against exact DuckDB v1.5.3 headers solely to match the installed R DuckDB
runtime. On Linux 6.8 x86-64, an Intel i5-13500 (20 logical cores), 62.6 GiB
RAM, and one DuckDB thread, five correctness-checked IPC repetitions over
`tpch_sf1.lineitem` measured these medians:

| rows | Arrow IPC stream | direct-message Quack |
|---:|---:|---:|
| 100,000 | 0.075 s | 0.055 s |
| 1,000,000 | 0.685 s | 0.571 s |

The benchmark mode is reproducible with an exact-ABI artifact:

```sh
DUCKNNG_BENCH_EXT_PATH=/path/to/ducknng.duckdb_extension \
  Rscript bench/rpc_quack_bench.R direct 5 100000,1000000 /tmp/ducknng-direct.duckdb
```

The same exact v1.5.3 artifact exported only `ducknng_init_c_api` and co-loaded
with nanonext in R; a 20-iteration one-row RPC smoke measured 2.80 ms per Arrow
session and 2.30 ms per Quack session. These are bounded regression/smoke
measurements, not the full Phase 8 result: they do not record payload bytes or
allocation counts, do not cover the required type/transport matrix, and do not
compare against the previous copied Quack path or the row-byte baseline.

Record revision, DuckDB version, transport, machine, thread count, batch size,
input bytes, output bytes, rows, types, and correctness check. Benchmark at
least:

- inline strings below, at, and above DuckDB's inline threshold;
- long strings and blobs;
- fixed-width numeric columns;
- null-heavy values;
- lists, structs, maps, arrays, and unions;
- upload and fetch over inproc, IPC, TCP, and HTTP where supported;
- Arrow IPC, optimized Quack, and the existing row-byte baseline.

Report throughput and allocations/copies per payload. Do not call the redesign
faster until the real session/upload paths show it.

## Phase 9 — Introduce only justified deterministic effect seams

**Status:** pending the codec work unless a narrower phase needs one sooner.

### Clock and entropy

Add a small runtime environment table for monotonic time and random bytes.
Production uses the current platform implementation. Tests use scripted time and
fixed entropy to prove session tokens, idle pruning, and retry deadlines.

### Carrier operations

Evolve transport implementations behind one carrier operation contract only
after writing its current NNG, HTTP, and browser consumers down precisely. Use
opaque adapter handles above the native adapter so NNG types do not leak into
protocol/session logic. Add a scripted carrier for deterministic completion,
error, cancellation, and ordering tests. Do not route Quack's per-value hot path
through this table.

### Process and file effects

Introduce file/process operations only around existing TLS-file and temporary
CSV/TSV/Parquet effects. Provide an in-memory or scripted test implementation
for read/write/temporary/unlink failures. Do not create a generic fake
filesystem for code without file effects.

### Method handlers

Use the existing registry handler callback as the plugin seam. Register scripted
methods in native tests to prove descriptor lookup, admission, request/reply
limits, flags, status preservation, and reply validation without starting SQL
execution. Real DuckDB execution and Arrow conversion retain integration tests.

## Phase 10 — Extract deterministic lifecycle transitions

**Status:** pending Phase 9.

Extract pure, exhaustive transition functions only for state machines already
present in production: sessions, uploads, AIO, and service request/reply phases.
Native tests enumerate every state/event pair. Locks, condition variables, and
OS threads stay in runtime adapters; they are not mocked.

Debug/test builds track lock ownership and assert internal lock-order and state
invariants. Production paths continue returning explicit errors for malformed
requests, cancellation races, exhaustion, and operating failures.

Test-only APIs may expose fail-at-N allocation, scripted time/entropy, forced
carrier completion order, session snapshots, transition functions, parser
cursors, and exact sizing. They compile only with `DUCKNNG_TEST` and must not be
exported from release extension artifacts.

## Phase 11 — Rducknng admission gate

**Status:** deferred.

Do not scaffold Rducknng until Phases 1–8 pass and the relevant deterministic
runtime seams needed by its execution overlay are identified from real code.
Rducks remains the behavior and performance comparison baseline. Rducknng must
not introduce a second NNG, TLS, nanoarrow, Quack, or ducknng runtime.

Rducknng must apply the same private-static dependency isolation discipline as
Rducks so it can be loaded in an R process that also has nanonext loaded:

- compile bundled NNG, Mbed TLS, nanoarrow, and ducknng implementation objects
  with hidden visibility;
- hide static-archive members at the final link (`--exclude-libs,ALL` where the
  linker supports it) and bind internal ELF calls locally
  (`-Bsymbolic-functions`);
- use an exported-symbol allowlist on macOS, exclude bundled symbols on Windows,
  and explicitly export only `rducknng_init_c_api` on wasm;
- audit every built artifact and fail if an `nng_*`, `nni_*`, `mbedtls_*`,
  `psa_*`, or `Arrow*` implementation symbol is externally visible;
- run a real R co-load test with nanonext and Rducknng, exercising each runtime
  independently before and after unloading/teardown.

The standalone native ducknng extension now applies the same discipline before
Rducknng exists: hidden target visibility, an ELF version script that exports
only `ducknng_init_c_api`, `--exclude-libs,ALL`, `-Bsymbolic-functions`, and a
macOS exported-symbol allowlist. The Linux release artifact's defined dynamic
symbol table contains exactly that one entry; the full SQL suite passes with the
sealed artifact. Rducknng still needs its own final-link audit and real nanonext
co-load test because embedding changes the final link product.

Rducks currently achieves this with static linkage, hidden visibility, archive
symbol exclusion, local ELF binding, and platform export allowlists; it does
not currently rewrite every dependency symbol to a textual prefix. If artifact
and co-load tests show that linker-level isolation is insufficient on a target,
add generated symbol-prefix rewriting to both packages from one audited symbol
manifest rather than creating an Rducknng-only naming convention.
