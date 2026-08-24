# Wasm and webR feasibility notes

`ducknng` has two wasm targets to keep separate. The first is a DuckDB wasm
side-module loaded by `duckdb-wasm`. The second is a future `Rducknng` package
built for webR/rwasm, where the package install process builds or bundles the
extension payload. This repository now has initial infrastructure for the first
path only. The review checklist for the remaining browser transport matrix lives
in `docs/wasm_browser_transport_checklist.md`.

The nanonext wasm reference point is commit
`4391bb31cf6aafe34f976feda1b03be0f9ca4acf`, which adds Emscripten link-time
settings for pthread-enabled wasm builds: `PTHREAD_POOL_SIZE=16`,
`ALLOW_MEMORY_GROWTH=1`, and `INITIAL_MEMORY=33554432`. `ducknng` applies the
same settings when `DUCKDB_PLATFORM=wasm_threads` is used. Emscripten currently
warns that `PTHREAD_POOL_SIZE` is only meaningful when producing JavaScript, so
that value is mainly a compatibility marker for the future webR/R package path;
`-pthread` and the wasm ABI flags are the relevant side-module checks. For
`wasm_eh` and `wasm_threads`, the extension also links with
`-fwasm-exceptions` and `-sWASM_BIGINT` so the side module matches duckdb-wasm's
native exception and i64 ABI. The wasm build also force-includes
`src/include/ducknng_wasm_compat.h`, following the DuckHTS pattern for
`duckdb-wasm` side modules: libc functions with 64-bit arguments or returns,
such as `strtoll`, `strtoull`, `lseek`, `ftruncate`, and `time`, are remapped to
duckdb-wasm's native `orig$*` exports to avoid dynamic-link signature
mismatches. During extension load on Emscripten, ducknng also sets NNG's init
parameters to small worker pools: two task threads, one expire thread, one
poller thread, and one resolver thread. This does not make browser transports
portable by itself; it keeps the local wasm probes from spending the browser's
pthread capacity on large default NNG pools before testing `inproc://` progress.

## Local duckdb-wasm smoke page

The local smoke workflow builds a wasm side-module in a Docker container, stages
a small static site, downloads a matching duckdb-wasm runtime, and serves a
browser page that loads the extension and runs a few SQL checks. With
`DUCKDB_WASM_PLATFORM=wasm_eh` it serves DuckDB's EH runtime. With
`DUCKDB_WASM_PLATFORM=wasm_threads` it serves DuckDB's COI pthread runtime and
adds the COOP/COEP headers required for `crossOriginIsolated` browser pthread
execution.

```sh
scripts/start_duckdb_wasm_local_test.sh
```

Useful knobs:

```sh
DUCKDB_WASM_PLATFORM=wasm_eh scripts/start_duckdb_wasm_local_test.sh
DUCKDB_WASM_PLATFORM=wasm_threads scripts/start_duckdb_wasm_local_test.sh
DUCKDB_WASM_NPM_VERSION=1.33.1-dev55.0 scripts/start_duckdb_wasm_local_test.sh
DUCKDB_WASM_DUCKDB_VERSION=v1.5.3 scripts/start_duckdb_wasm_local_test.sh
DUCKNNG_WASM_BUILD_ONLY=1 scripts/start_duckdb_wasm_local_test.sh
DUCKNNG_WASM_TRACE=1 DUCKDB_WASM_PLATFORM=wasm_threads scripts/start_duckdb_wasm_local_test.sh
DUCKNNG_WASM_INPROC_ONLY=0 DUCKDB_WASM_PLATFORM=wasm_threads scripts/start_duckdb_wasm_local_test.sh
DOCKER_REBUILD_IMAGE=1 scripts/start_duckdb_wasm_local_test.sh
```

The script pins `DUCKDB_WASM_NPM_VERSION` and `DUCKDB_WASM_DUCKDB_VERSION` by
default instead of using npm's moving `latest` tag or the native extension's
current DuckDB target. A DuckDB wasm runtime built from a different DuckDB or
Emscripten ABI can fail at extension load with opaque dynamic-link signature
mismatches. The local server also sends `Cache-Control: no-store` for every
served file so repeated runs do not usually need browser cache clearing.

By default, browser wasm builds set `DUCKNNG_WASM_INPROC_ONLY=1`. That keeps
NNG's SP transport registration to `inproc://` for the side module and skips the
POSIX poller/resolver initialization that is not needed for same-process NNG
transport probes. Native builds are unchanged. On `wasm_threads`, ducknng also
warms up NNG during extension load by opening and closing a dummy REP socket so
worker creation is not deferred to the first real socket query. The vendored NNG
delta for this mode is recorded in
`patches/nng/0002-emscripten-wasm-inproc-progress.patch` and summarized in
`patches/nng/README.md`. Set `DUCKNNG_WASM_INPROC_ONLY=0` only when deliberately
testing a broader wasm NNG footprint; browser `ipc://`, raw `tcp://`, and
`tls+tcp://` still are not considered supported by that flag.

The generated artifacts live under `.duckdb-wasm-local-artifacts/`, and the
Docker work tree lives under `.duckdb_wasm_docker_work/`. Both are ignored by
Git. Set `DUCKNNG_WASM_SERVE=0` to stage the static site and exit instead of
starting the local no-cache server; this is the mode used by the GitHub Pages
workflow.

## GitHub Pages demo

The workflow `.github/workflows/ducknng-wasm-pages.yml` builds the same static
`duckdb-wasm` demo site, publishes a rolling prerelease named
`ducknng-wasm-demo-latest`, and force-publishes the site to the `gh-pages`
branch from the CI-built artifact. It is separate from
`.github/workflows/MainDistributionPipeline.yml` and does not change the DuckDB
community-extension binary distribution path. The release assets include the
`ducknng` wasm side module, `ducknng-wasm-config.json`, a tarball of the staged
Pages site, and SHA-256 sums. The Pages site contains the matching
`duckdb-wasm` runtime files, the config, the same wasm side module mirrored into
same-origin Pages storage, a small index page, and the browser smoke page with
an interactive SQL shell.

The default Pages build uses `DUCKDB_WASM_PLATFORM=wasm_threads`,
`DUCKNNG_WASM_INPROC_ONLY=1`, and the same pinned `duckdb-wasm` / DuckDB version
pair as the local script. Because ordinary GitHub Pages cannot set COOP/COEP
headers from branch content, the static site also includes a same-origin COI
service worker. On first load, the smoke page may register that worker and
reload once before `crossOriginIsolated` becomes true. The Pages demo proves the
CI-built side module can load in `duckdb-wasm`, run scalar extension calls, and
support the interactive SQL shell. The full `inproc://` transport proof remains
attached to the local smoke page or another host that sends real COOP/COEP
headers; on GitHub Pages the page skips that transport proof because
service-worker-provided COI has proven insufficiently reliable for the current
NNG pthread progress path. Before publication, a separate `wasm_eh` artifact
passes the capability-driven Playwright gate, including browser HTTP and
asynchronous WS/WSS frame carriers. A passing Pages demo is not a release promise
for browser `ipc://`, raw `tcp://`, native `tls+tcp://`, generic socket handles,
or listeners.

The side-module wasm file on `gh-pages` is a same-origin mirror of the CI-built
demo release asset, not a stable binary release channel. The mirror is deliberate:
the threaded browser runtime needs cross-origin isolation, and direct
`github.com/releases/download/...` asset fetches may not provide the CORS/CORP
headers required under COEP. If the project later needs durable downloadable
binaries, add a separate stable release workflow or package repository
deliberately rather than silently treating either the Pages branch or the rolling
demo prerelease as an API-stable artifact host.

The browser page deliberately exercises a narrow load path: `LOAD` the extension,
query `ducknng_nng_version()`, decode one synthetic frame, list body codecs, and
then walk upward through the transport stack before trying the RPC manifest. The
`inproc://` section first opens a raw `rep` socket and tests URL-launched raw
request AIO bytes, then starts a ducknng service and collects the framed manifest
through `ducknng_get_rpc_manifest_raw_aio(...)` plus
`ducknng_aio_collect_decoded(...)`. This keeps wasm transport failures from
being misdiagnosed as manifest or dispatcher failures while avoiding the known
blocking `ducknng_dial_socket(...)` path. With the EH runtime, the `inproc://`
probes are allowed to report unavailable because NNG worker/task threads require
a pthread-capable runtime. With the COI pthread runtime, an `inproc://` failure
is treated as a smoke failure. Passing this page proves only the tested runtime
path; browser HTTP and WebSocket client claims come from the separate Playwright
probes below, and the page alone does not prove those carriers or any browser
TCP, IPC, native TLS socket, generic socket-handle, or listener support.

When `DUCKNNG_WASM_TRACE=1` is set for the local build, the extension compiles
extra Emscripten-only trace points around the NNG client launch boundary and the
vendored NNG initialization/thread boundaries. Extension messages are prefixed
with `[ducknng wasm trace]`; vendored NNG messages are prefixed with
`[ducknng nng wasm trace]`. The trace marks entry/return around blocking socket
dial, nonblocking URL dial, context creation, runtime AIO registration,
`ctx_send_aio`, NNG platform initialization, taskq/reap/expire worker creation,
and wasm inproc-only poller/resolver suppression. The trace switch is build-time
only and is intended for the local wasm smoke page, not as part of the public
protocol.

## Browser smoke test (Playwright)

`test/browser/run_smoke.mjs` drives a staged site in real headless Chromium via
Playwright. It serves the site with the COOP/COEP headers the threaded runtime
needs for `crossOriginIsolated` (a plain static server does not send these),
asserts cross-origin isolation, loads the extension, runs an ad-hoc
`SELECT ducknng_nng_version()` through the SQL shell, and then exercises selected
browser transport probes against local test endpoints.

```sh
cd test/browser
npm ci
cd ../..
DUCKDB_WASM_PLATFORM=wasm_eh DUCKNNG_WASM_SERVE=0 \
  scripts/start_duckdb_wasm_local_test.sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site \
  --probes=conformance
```

Exit code 0 on pass; `BROWSER_DEBUG=1` mirrors the page console. Conformance
reads the active capability descriptor and runs every supported probe as a hard
gate. The `wasm_eh` lane covers extension load, whole-response HTTP, real
asynchronous Fetch with cancellation and timeout, body parsing, HTTPS/CORS,
framed HTTP RPC/session helpers, and asynchronous WS/WSS framed RPC including
persistent reuse, cancellation, timeout, abnormal close, synchronous-call
rejection, and browser-managed WSS TLS. `inproc://` is reported unsupported on
EH and skipped cleanly.

The threaded runtime remains diagnostic rather than release-blocking:

```sh
DUCKDB_WASM_PLATFORM=wasm_threads DUCKNNG_WASM_SERVE=0 \
  scripts/start_duckdb_wasm_local_test.sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site \
  --probes=load
```

Recent `wasm_threads` runs have shown the expected instability: the side module
builds, and individual `load`, `http-sync`, and `http-aio` runs can pass, but
repeated headless runs still expose extension-load timeouts and DuckDB-wasm JSON
autoload failures such as `json_duckdb_cpp_init` mismatches during
`ducknng_ncurl_table(...)`. Treat threaded `inproc://` and threaded HTTP table
coverage as diagnostic evidence, not as a stable browser support promise. The
harness skips capabilities declared unsupported, including browser `ipc://`, raw
`tcp://`, native `tls+tcp://`, generic socket handles, and listeners.

## Browser HTTP(S) adapters

Raw `tcp://`, `ipc://`, and `tls+tcp://` sockets cannot exist in a browser
sandbox, so the native NNG HTTP client path does not work there. Under
`__EMSCRIPTEN__`, `ducknng_http_transact()` routes `http://` and `https://`
requests through `src/ducknng_wasm_http_fetch.c`, which performs a synchronous
`XMLHttpRequest` via `EM_ASM` — allowed inside the duckdb-wasm Web Worker, the
same approach duckhts uses in `src/wasm_http_hfile.c`. The shim returns the
status, the raw response-header block, and the body; the existing native code
converts the header block to the canonical `headers_json`, so the
`ok/status/error/headers_json/body/body_text` result shape is the same as the
native client. The whole translation unit is gated on `__EMSCRIPTEN__` and
compiles to nothing on native builds.

The proven `wasm_eh` browser HTTP slice now includes same-origin
`ducknng_ncurl(...)` GET and POST, request-header propagation, response-header
exposure, non-2xx statuses returned as completed HTTP rows, invalid methods and
invalid `headers_json` returning `ok = false` in-band, browser network/CORS
failures returned as `ok = false`, real asynchronous
`ducknng_ncurl_aio(...)` pending/collect/cancel/timeout behavior,
`ducknng_ncurl_table(...)` JSON/text/CSV body parsing, raw local HTTPS CORS,
explicit TLS-handle rejection because browser HTTPS trust is browser-managed,
local HTTPS CORS table parsing, and framed raw/RPC/session helpers routed over
browser HTTP. Browser constraints apply: same-origin URLs work directly;
cross-origin requests need permissive CORS on the remote. TLS is
browser-managed, so an explicit ducknng TLS configuration is rejected with an
error rather than silently ignored. Synchronous whole-response calls retain the
XHR implementation; asynchronous calls use Fetch plus `AbortController` and
settle through the runtime's operation table. An optional
`Module.ducknngWasmHttpConfig`, mirroring duckhts's
`Module.duckhtsWasmHttpConfig`, can supply allowlist-gated extra request
headers, a hard host allowlist (`enforceHostAllowlist`), and `withCredentials`.
The same hard hostname allowlist applies to browser WS/WSS launches.

## Current `inproc://` status

The pthread wasm `inproc://` smoke path is a diagnostic path, not a stable
browser support claim. Individual local/header-hosted proofs have passed the
AIO-based contract tested by the page: extension load, scalar calls, raw
URL-launched AIO request/reply bytes, `ducknng_start_server(...)`, and framed
manifest collection through `ducknng_get_rpc_manifest_raw_aio(...)`. Repeated
real-browser Playwright runs have also exposed extension-load timeouts and NNG
progress timeouts, especially without tracing. The wasm NNG build keeps only the
`inproc://` SP transport registered, skips POSIX poller/resolver startup in that
mode, and yields briefly around Emscripten `pthread_create(...)` while NNG
creates task/reap/expire workers. A native Linux run still covers the broader
synchronous helper path.

The lowest-level page probes have narrowed the failure below the RPC framework.
The first raw-socket probe originally reached raw socket setup and then timed
out in the blocking client dial call:

```sql
SELECT (ducknng_open_socket('pair')).socket_id;
SELECT (ducknng_listen_socket(
  1,
  'inproc://ducknng_wasm_pair_...',
  1048576,
  0::UBIGINT
)).ok;
SELECT (ducknng_open_socket('pair')).socket_id;
SELECT (ducknng_dial_socket(
  2,
  'inproc://ducknng_wasm_pair_...',
  2000,
  0::UBIGINT
)).ok;
```

That timeout happened before `ducknng_recv_socket_raw_aio(...)`,
`ducknng_send_socket_raw_aio(...)`, or `ducknng_aio_collect(...)` could run. It
placed that specific failure at the NNG blocking dial boundary rather than in the
RPC manifest method. The wasm smoke page therefore treats URL-launched AIO as the
browser-compatible NNG path and does not use `ducknng_dial_socket(...)` as a
required browser proof.

Before opening any real socket, the page also calls `ducknng_listen_socket(...)`
with a missing socket id. That negative check verifies that the SQL binding and
argument path for the listen scalar can run without touching NNG listener state.
The page then uses a URL-launched raw request AIO probe that avoids explicit
`ducknng_dial_socket(...)`. These launch probes pass `0` as the NNG operation
timeout so the first question is whether the operation can be submitted at all;
the page-level collect/query timeouts remain responsible for bounding the smoke
page. NNG setup calls use a longer JavaScript guard than pure scalar checks so
slow browser pthread startup is not mistaken for an immediate logic failure. In
the trace build, this path has progressed through
`nng_dial(..., NNG_FLAG_NONBLOCK)`, `nng_ctx_open`, runtime AIO registration, and
`ctx_send_aio`, returned an AIO id, and delivered the raw request bytes to a raw
`rep` socket. The reply is also launched through `ducknng_send_socket_raw_aio(...)`.
The framed manifest path uses the same nonblocking URL-launched request AIO
pattern. The JavaScript timeout wrapper is only a diagnostic guard around
`conn.query(...)`; it does not cancel the running DuckDB wasm query. After a
timeout, the page intentionally avoids follow-up SQL on the same connection
because status or cleanup queries would usually just queue behind the hung
operation and obscure the first blocking boundary.

## Current wasm boundary

Browser wasm is not a POSIX host. `ipc://` and raw TCP listeners do not map to a
normal browser runtime. NNG may be buildable through Emscripten, as nanonext
shows for pthread wasm, but each transport still needs runtime-specific testing.

The per-target transport capability contract is compiled into the extension and
queryable as `SELECT * FROM ducknng_list_transport_capabilities()` (the active
target's descriptor is `ducknng_transport_capabilities()`). The table below is
rendered from that contract by `make wasm_matrix`; edit
`src/ducknng_net_backend.c`, not this block.

<!-- BEGIN GENERATED TRANSPORT MATRIX (make wasm_matrix) -->
| target | http | https | response stream | inproc | tcp | ipc | tls+tcp | ws/wss | real async | timeout | cancel | TLS owner |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `native` | supported | supported | supported | supported | supported | supported | supported | supported | yes | yes | yes | native |
| `wasm_eh` | supported | supported | unsupported | unsupported | unsupported | unsupported | unsupported | supported | yes | yes | yes | browser_managed |
| `wasm_threads` | supported | supported | unsupported | experimental | unsupported | unsupported | unsupported | supported | yes | yes | yes | browser_managed |
<!-- END GENERATED TRANSPORT MATRIX -->

The evidence log below records what has been proven in real browsers, which is
a stricter statement than the contract above:

| Runtime / carrier | Current status |
| --- | --- |
| `duckdb-wasm` `wasm_eh`: load, scalar SQL, codecs | Proven locally in real Chromium. |
| `duckdb-wasm` `wasm_eh`: same-origin `ducknng_ncurl(...)` GET/POST, request/response headers, non-2xx status rows, invalid input rows, and no-CORS network/CORS errors | Proven locally in real Chromium through the browser HTTP bridge. |
| `duckdb-wasm` `wasm_eh`: `ducknng_ncurl_aio(...)` real-async handles: pending in flight, poll-style wait, cancel aborts the fetch, timeout_ms aborts via a JS timer, collect/drop for success and launch-error cases | Proven locally in real Chromium through the fetch completion bridge; completions settle when the DB worker event loop runs between queries, so a blocking wait inside one query cannot make progress and degrades to a poll. |
| Browser incremental HTTP response streams | Unsupported and reported as `http_response_stream = unsupported`. Future work should follow `r-lib/nanonext#329`: return response headers with a session, receive chunks through ordinary AIO receives, and use a zero-length chunk for EOF; `ReadableStream.getReader()` supplies the browser transport. |
| `duckdb-wasm` `wasm_eh`: `ducknng_ncurl_table(...)` JSON/text/CSV plus raw/parsed HTTPS CORS and explicit TLS-handle rejection | Proven locally in real Chromium. |
| `duckdb-wasm` `wasm_eh`: framed raw/RPC/session helpers over `http://` | Proven locally against a Playwright local ducknng-frame responder. |
| `duckdb-wasm` `wasm_eh`: `inproc://` | Not supported; the probe may report unavailable and still pass because this runtime has no pthread worker support for NNG progress. |
| `duckdb-wasm` `wasm_threads`: load, scalar SQL, codecs | Proven in individual runs and on the Pages demo, but repeated headless local runs can time out during `LOAD`; treat as resource-sensitive. |
| `duckdb-wasm` `wasm_threads`: `inproc://` AIO/raw/framed manifest | Diagnostic only. Individual local COOP/COEP proofs have passed; repeated real-browser runs still expose progress timeouts. |
| `duckdb-wasm` `wasm_threads`: browser HTTP probes | Diagnostic only. Individual `load,http-sync,http-aio` runs can pass; repeated runs have exposed extension-load and DuckDB-wasm JSON autoload failures. |
| Browser HTTPS remote-origin HTTP | Local HTTPS CORS is proven in Chromium; a durable public remote-origin target is not part of the release gate. |
| Browser `ipc://`, raw `tcp://`, and native POSIX-style `tls+tcp://` | Unsupported by the browser sandbox. |
| Browser `ws://` / `wss://` | Implemented and proven in headless Chromium as an async-only ducknng-frame-over-WebSocket carrier: persistent actor reuse, FIFO replies, cancel, timeout, abnormal close, synchronous-call rejection, WS/WSS replies, and browser-managed WSS TLS. The server endpoint shares frame authorization and dispatch with HTTP. `caps.websocket = supported`, so conformance is a release gate. |
| webR / R package wasm | Separate future runtime; no support should be inferred from `duckdb-wasm`. |

Do not infer webR support from duckdb-wasm support. They are different runtimes
and artifacts.

## Future `Rducknng` webR path

A future `Rducknng` layout should follow the DuckHTS pattern: keep the native
extension contract at the repository root, then stage a self-contained R package
under `r/Rducknng/` with explicit bootstrap, configure, and package tests. The
webR build should use `ghcr.io/r-wasm/webr:main` and the `rwasm::build()` path,
then inspect the built `Rducknng_<version>.tgz` rather than host build
directories.

When that package exists, its `configure` script should carry the nanonext-style
pthread wasm link tuning for emcc builds, preserve Emscripten `LDFLAGS` in the
final extension link, and keep wasm-only patches gated on real Emscripten target
detection. If browser networking requires adapter code, keep it behind wasm
transport boundaries rather than leaking browser details into the SQL method
surface.
