# Release branches and self-published binaries

`ducknng` uses `main` for forward development. Because the extension uses DuckDB's unstable C extension vtable, each built `.duckdb_extension` is pinned to one exact DuckDB version. A fix that downstreams need on an older DuckDB version must therefore be backported to a branch named for that DuckDB runtime, for example `release/duckdb-1.5.2`.

A release branch pins three things together in `Makefile`: `TARGET_DUCKDB_VERSION`, `DUCKDB_TEST_VERSION`, and `DUCKDB_HEADER_VERSION`. The checked-in `duckdb_capi/duckdb.h` and `duckdb_capi/duckdb_extension.h` must come from the same DuckDB tag. Do not change one of these without the others.

The backport workflow is: land the fix on `main`, cherry-pick it to every supported `release/duckdb-X.Y.Z` branch, update the version pins and C API headers for that branch if the branch is newly created, then build and test with the matching DuckDB runtime:

```sh
make configure
make update_duckdb_headers
make release -j2
configure/venv/bin/python3 test/http_smoke.py build/release/ducknng.duckdb_extension
```

The DuckDB community extension repository is not the backport distribution channel. It builds the community submission line and does not carry per-DuckDB-version fixes. Backport binaries are published from this repository as unsigned GitHub Release assets. Tag release-branch commits with a tag that pins both versions, using:

```text
v<ducknng-version>-duckdb<duckdb-version>
```

For example, `v0.1.1-duckdb1.5.2` names the `ducknng` 0.1.1 backport line built for DuckDB 1.5.2. Pushing such a tag runs `.github/workflows/ducknng-release-binaries.yml`, builds the branch's pinned release matrix, and attaches the `.duckdb_extension` artifacts, raw native libraries or wasm side module, checksums, and short load notes to the GitHub Release. The current self-published matrix is `linux_amd64`, `linux_arm64`, `osx_amd64`, `osx_arm64`, `windows_amd64_mingw` built with Rtools/MinGW, and `wasm_eh`.

The release workflow runs the HTTP smoke test, verifies the side-effecting transport/service/profile scalars are `VOLATILE`, checks the compatibility arities for `ducknng__ncurl_row(...)` and `ducknng_register_http_profile(...)`, and runs a scoped outbound HTTP profile smoke on native platforms where the matching DuckDB Python runtime can load the built artifact directly: Linux x86_64 and Linux arm64. The Windows MinGW/Rtools artifact is smoke-loaded through the matching R `duckdb` package, which uses the Rtools/MinGW runtime and can load the `windows_amd64_mingw` extension; that smoke includes the same volatility and compatibility-arity checks. The macOS artifacts are target-architecture builds on `macos-latest` with explicit `CMAKE_OSX_ARCHITECTURES`; they are build-validated in this self-published workflow rather than smoke-loaded because one runner architecture cannot reliably execute both target artifacts. The `wasm_eh` artifact is a duckdb-wasm side module and is build-validated here; browser runtime proof remains covered by the wasm/browser workflow and tests rather than by native `LOAD`.

These release assets are unsigned. A consumer must open the host DuckDB connection with `allow_unsigned_extensions = true` and then load the downloaded file explicitly:

```sql
LOAD '/path/to/ducknng-v0.1.1-duckdb1.5.2-linux_amd64.duckdb_extension';
```

Community-signed builds remain loadable through the usual community extension flow when the consumer can use the community line's DuckDB target. The self-published assets are for pinned runtimes that need a backport before, or instead of, a community submission refresh. When a platform is added to or removed from the self-published matrix, update this file and re-tag the affected release branches so the release assets are regenerated from commits containing the matching workflow.
