# Browser wasm transport checklist

This checklist closes the release-supported `duckdb-wasm` browser-client scope. The compiled capability descriptor is the behavior authority. `docs/wasm.md` records build details and evidence; `test/browser/run_smoke.mjs --probes=conformance` is the executable gate.

## Artifact and loading

- [x] Build a `wasm_eh` duckdb-wasm side module.
- [x] Load the extension in real Chromium and run scalar SQL.
- [x] Gate demo publication on the capability-driven Chromium conformance run.
- [x] Publish the static Pages demo from the CI-built artifact.
- [x] Treat Pages and the rolling prerelease as demo provenance, not a stable binary channel.
- [x] Keep the `wasm_threads` artifact and `inproc://` path diagnostic rather than release-blocking.

## HTTP and HTTPS client

- [x] Route synchronous browser HTTP through the network backend's XHR implementation.
- [x] Preserve the native whole-response result shape: `ok`, `status`, `error`, `headers_json`, `body`, and `body_text`.
- [x] Cover GET, POST, request and response headers, non-2xx status-as-data, invalid input, and no-CORS failure mapping.
- [x] Implement genuinely asynchronous unary Fetch AIO with observable pending state.
- [x] Abort Fetch on timeout and cancellation without leaking SQL-visible handles.
- [x] Preserve terminal error handles for expected launch failures.
- [x] Parse JSON, text, and CSV through `ducknng_ncurl_table(...)`.
- [x] Prove HTTPS/CORS in Chromium.
- [x] Treat HTTPS trust as browser-managed and reject explicit ducknng TLS handles.
- [x] Apply the same hard hostname allowlist and credential policy to synchronous and asynchronous HTTP.

Incremental browser response bodies remain unsupported. Future work should align with `r-lib/nanonext#329`: a session returning response headers, ordinary receive AIOs for chunks, zero-length EOF, and an explicit close. Do not add a separate browser-only stream API.

## Framed HTTP RPC and sessions

- [x] Route raw and structured RPC helpers over the HTTP frame carrier without changing method names.
- [x] Preserve the ducknng frame, Arrow IPC, Quack-derived payload, and session contracts.
- [x] Prove raw manifest, structured manifest, AIO manifest, exec, query open, fetch, cancel, and close against the local frame responder.
- [x] Keep browser HTTP listeners unsupported.

## WebSocket frame carrier

- [x] Limit the browser slice to URL-launched framed RPC AIO rather than pretending generic NNG socket compatibility.
- [x] Implement a persistent JavaScript `WebSocket` actor per URL.
- [x] Correlate replies FIFO and close the actor when cancellation or timeout would invalidate correlation.
- [x] Implement the server-side raw-WebSocket sibling endpoint and pass every frame through the shared service authorization and dispatch path.
- [x] Keep native `ws://` and `wss://` on NNG SP-over-WebSocket; use ducknng-frame-over-WebSocket only in the browser backend.
- [x] Treat WSS trust as browser-managed and reject explicit ducknng TLS handles.
- [x] Apply the embedding host's hard hostname allowlist to WS/WSS launches.
- [x] Reject synchronous browser WS/WSS RPC with an explicit direction to AIO.
- [x] Prove WS and WSS manifest/exec replies in Chromium.
- [x] Prove persistent actor reuse, cancellation, timeout, abnormal close, and TLS-handle rejection.
- [x] Gate the WebSocket probe from `caps.websocket`; a supported claim without passing behavior fails CI.
- [x] Keep browser WebSocket listeners unsupported.

## Capability and test contract

- [x] Compile target descriptors for native, `wasm_eh`, and `wasm_threads`.
- [x] Expose scalar JSON and a table form with one active row.
- [x] Fail conformance when scalar and table descriptors disagree.
- [x] Run supported probes as hard gates, skip unsupported probes, and run experimental probes report-only.
- [x] Generate the `docs/wasm.md` matrix from the descriptor with `make wasm_matrix`.
- [x] Pin Node, Playwright, duckdb-wasm, DuckDB, and Emscripten inputs used by the browser proof.
- [x] Run JavaScript syntax checks and real Chromium conformance before claiming support.

## Explicit non-goals

- [x] Browser `ipc://` remains unsupported.
- [x] Browser raw `tcp://` remains unsupported.
- [x] Browser native POSIX-style `tls+tcp://` remains unsupported.
- [x] Browser HTTP and WebSocket listeners remain unsupported.
- [x] Generic browser NNG socket handles remain unsupported.
- [x] Incremental browser HTTP response streams remain unsupported.
- [x] `wasm_threads` `inproc://` remains experimental.
- [x] webR support is not inferred from duckdb-wasm support.

## Separate future work

A future webR/R package artifact needs its own build, install, runtime, and transport proof. A future browser incremental-response implementation needs the session-plus-receive-AIO contract above. Neither is required to call the current duckdb-wasm browser-client scope complete.
