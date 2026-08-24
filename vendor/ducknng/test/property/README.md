# Native property tests

This directory contains C properties and deterministic edge tests for parser and
protocol code that can run without starting a DuckDB extension server. The
runner uses vendored [`greatest`](../vendor/greatest/LICENSE) for assertions and
[`theft`](../vendor/theft/LICENSE) for generated inputs and shrinking.

Run the default bounded suite with:

```sh
make prop
```

Useful variants:

```sh
make prop-quick          # fewer generated trials for rapid iteration
make prop-asan           # AddressSanitizer build of the same suite
make prop-ubsan          # UndefinedBehaviorSanitizer build
make prop-sanitize       # ASan then UBSan
make prop-clean
```

Generation knobs:

```sh
make prop PROP_TRIALS=10000
make prop PROP_SEED=0x1234
DUCKNNG_PROP_FORK=1 make prop
```

The suite covers checked arithmetic, Quack byte primitives, random and canonical
frames, status encoding, upload prefixes, transport URLs, hostile Quack bytes,
and generated nested Quack schemas. Fuzzing is separate; see
[`../fuzz/README.md`](../fuzz/README.md).

## Measured MC/DC

Clang/LLVM source-based coverage instruments only the currently extracted
pure-C checked arithmetic, frame core, and Quack byte/type-tree core. Adapter,
DuckDB, NNG, test-runner, and
vendored theft code is linked but not instrumented.

```sh
make quack-coverage
make quack-mcdc
make quack-mcdc-check
make quack-mcdc-clean
```

`quack-mcdc-check` reads LLVM's JSON summary and fails unless both
`src/ducknng_checked.c`, `src/ducknng_wire_core.c`, and
`src/ducknng_quack_core.c` have at least one MC/DC
condition and every measured condition is covered. This is not a whole-codec or
whole-extension coverage claim. The enforced file set must expand when more
schema, validation, sizing, or lifecycle logic moves into dependency-free core
modules.

The targets require Clang 18 or newer plus matching `llvm-cov` and
`llvm-profdata`. Override `MCDC_CC`, `MCDC_LLVM_COV`, or
`MCDC_LLVM_PROFDATA` when the tools are not on `PATH`.
