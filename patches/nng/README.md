# Local NNG patches

These patch files document local divergence from the vendored `third_party/nng/` tree.

## Upstream pin

- Project: `nanomsg/nng`
- Vendored line in this repo: `1.11.x` (`third_party/nng/` currently reports `1.11.0` during configure)

## Patch ledger

### `0001-windows-rtools42-timespec-fallback.patch`

Reason:

- DuckDB extension CI currently drives `windows_amd64_mingw` through the DuckDB reusable workflow's Rtools 42 MinGW environment.
- Vendored NNG's Windows CMake gate rejects that environment because `timespec_get()` is missing there, even though the rest of the Windows API surface needed by `ducknng` is present.
- `ducknng` intends to keep supporting `windows_amd64_mingw`, so the vendored copy carries a minimal Windows clock fallback rather than dropping the target.

Files touched:

- `third_party/nng/src/platform/windows/CMakeLists.txt`
- `third_party/nng/src/platform/windows/win_clock.c`

Behavior:

- relax the fatal Windows configure check so missing `timespec_get()` alone does not abort the build
- use `GetSystemTimeAsFileTime()` as a Windows fallback when `timespec_get()` is unavailable

Refresh command:

```sh
git diff -- third_party/nng/src/platform/windows/CMakeLists.txt \
  third_party/nng/src/platform/windows/win_clock.c \
  > patches/nng/0001-windows-rtools42-timespec-fallback.patch
```

### `0002-emscripten-wasm-inproc-progress.patch`

Reason:

- Browser `wasm_threads` runs NNG pthreads as Emscripten Web Workers, and local
  `inproc://` probes can hang when NNG creates task/reap/expire workers or when
  the side module touches POSIX poller/resolver initialization that is irrelevant
  to `inproc://`.
- `ducknng` uses browser wasm as a duckdb-wasm side module, where `ipc://`, raw
  `tcp://`, and `tls+tcp://` are not supported browser transports. The wasm
  smoke path therefore needs a narrow, auditable NNG `inproc://` compatibility
  slice rather than pretending the full native NNG footprint works in a browser.
- `DUCKNNG_WASM_TRACE=1` must remain build-time gated and disabled by default,
  but targeted NNG trace points are needed to diagnose thread/task progress.

Files touched:

- `third_party/nng/src/core/aio.c`
- `third_party/nng/src/core/init.c`
- `third_party/nng/src/core/reap.c`
- `third_party/nng/src/core/taskq.c`
- `third_party/nng/src/platform/posix/posix_thread.c`

Behavior:

- add Emscripten-only, build-time-gated NNG trace points prefixed with
  `[ducknng nng wasm trace]`
- in `DUCKNNG_WASM_INPROC_ONLY` builds, skip POSIX poller/resolver startup and
  `pthread_atfork()` from NNG platform initialization
- yield briefly around Emscripten `pthread_create()` in wasm inproc-only builds
  so browser worker startup can make progress before later NNG calls depend on
  task/reap/expire workers
- keep native and non-Emscripten builds unchanged

Refresh command:

```sh
git diff -- third_party/nng/src/core/aio.c \
  third_party/nng/src/core/init.c \
  third_party/nng/src/core/reap.c \
  third_party/nng/src/core/taskq.c \
  third_party/nng/src/platform/posix/posix_thread.c \
  > patches/nng/0002-emscripten-wasm-inproc-progress.patch
```
