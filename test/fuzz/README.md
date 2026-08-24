# Native hostile-byte fuzzing

`ducknng_fuzz.c` drives four dependency-minimal trust-boundary surfaces without
starting DuckDB or a network service:

- complete ducknng frame decode plus canonical encode/decode;
- upload-append control-prefix decode;
- Quack counted bytes and integer primitives;
- Quack schema decode followed by bounded result traversal when the schema is
  accepted.

The harness installs only `malloc` and `free` in the test DuckDB extension API
vtable and builds with `DUCKNNG_CORE_ONLY`, which excludes the final-NNG-message
encoder. Linker section garbage collection removes other adapter paths not
reached by these parser entry points. One aborting streaming-fetch definition
remains until schema/chunk traversal leaves the monolithic DuckDB adapter.

Run bounded ASan and UBSan campaigns with:

```sh
make fuzz-asan
make fuzz-ubsan
make fuzz
```

Useful overrides:

```sh
make fuzz FUZZ_RUNS=1000000 FUZZ_MAX_LEN=65536
make fuzz-asan FUZZ_RUNS=10000 FUZZ_SEED=1234
make fuzz-clean
```

Each target copies the tracked seeds into a temporary writable corpus so local
campaigns do not modify repository files. The corpus includes canonical frames,
an upload prefix, Quack compressed-vector fixtures derived from
`tools/quack_compressed_fixtures.R`, and ENUM schema seeds covering the
serializer field order, the reversed order, and a declared/actual label-count
mismatch. Minimized failures must be added as named tracked regression seeds
and, where their invariant is structural, as a native property or SQL
regression too.

## Known coverage gap

These entry points reach schema decode, row-count reads, and bounded traversal.
They do not reach the vector-materializing path
(`ducknng_quack_payload_scan_next`, and the dictionary-selection, fixed-width
slice, and cumulative-value bounds it enforces), because that path writes into
a real `duckdb_data_chunk` and the harness installs only `malloc`/`free` in the
extension API vtable. Stubbing DuckDB's vector functions here would fuzz the
stubs rather than the decoder, so the gap stays open and recorded instead.

Closing it is Phase 4 work in `docs/quack_core_plan.md`: once vector decode
emits borrowed wire views into a caller-owned destination, the materializing
bounds become reachable from a dependency-free harness. Until then, that path's
coverage comes from SQL type round trips and native properties only.

A corpus with no example of a type family gives the mutator nothing to reorder
or truncate within it. The ENUM field-reordering defect fixed in
`ducknng_quack_read_type_info` sat behind exactly that gap: the schema decoder
was fuzzed, but no seed contained an ENUM. Adding one seed per supported type
family is worth more here than more runs on the seeds already present.

Fuzz acceptance is not an interoperability claim. SQL type round trips, upload,
real carriers, sanitizers, shrinking properties, and measured MC/DC remain
separate gates.
