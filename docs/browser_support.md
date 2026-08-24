# Browser support contract

`ducknng` supports browser clients through the `duckdb-wasm` `wasm_eh` side-module target. Support is capability-driven: the active network backend reports one compiled descriptor through `ducknng_transport_capabilities()` and `ducknng_list_transport_capabilities()`, and the Chromium conformance runner executes every capability reported as `supported`. A capability reported as `unsupported` is skipped rather than simulated. `experimental` capabilities run as non-gating diagnostics.

The release-supported browser client scope is extension loading, scalar SQL, whole-response HTTP and HTTPS, real asynchronous unary Fetch with timeout and cancellation, body-codec table helpers, framed RPC and query sessions over HTTP, and asynchronous ducknng-frame-over-WebSocket RPC over `ws://` and `wss://`. Browser HTTPS and WSS use the browser trust store. Supplying a ducknng TLS handle is rejected rather than silently ignored.

Incremental HTTP response bodies, browser listeners, generic browser socket handles, `ipc://`, raw `tcp://`, and native POSIX-style `tls+tcp://` are unsupported. The pthread `wasm_threads` `inproc://` path is experimental and does not gate a release. webR is a separate runtime and artifact contract; duckdb-wasm evidence does not establish webR support.

## Network backend

Shared SQL, RPC, and AIO code calls `ducknng_net_backend`. Target selection happens once at build time. Native code binds that interface to NNG HTTP and socket operations. Browser code binds it to synchronous XHR for the synchronous whole-response client, Fetch for asynchronous HTTP, and the browser WebSocket frame carrier for asynchronous WS/WSS RPC.

The descriptor records three-state support for HTTP, HTTPS, incremental response streams, inproc, TCP, IPC, TLS-over-TCP, and WebSocket. It also records whether AIO is genuinely asynchronous, whether timeout and cancellation are honored, and whether TLS is native or browser-managed. `make wasm_matrix` renders the descriptor into `docs/wasm.md`; generated matrix prose is not a second authority.

No `__EMSCRIPTEN__` transport choice belongs in the method registry or RPC method implementation. Browser-specific bridge code remains in the network adapter and wasm bridge translation units. The manifest and method state machines remain carrier-independent.

## Asynchronous completion

Browser HTTP AIO uses `fetch()` plus `AbortController`. JavaScript records pending operations in a runtime-owned table; the C runtime pumps settled operations through the same SQL-visible AIO state machine used by native operations. Timeout and cancellation abort the fetch. Because the DuckDB wasm worker event loop runs between queries, SQL wait operations poll rather than block the worker waiting for a callback that cannot run.

Browser WebSocket AIO uses the same operation table. One persistent `WebSocket` actor is retained per URL. Requests and replies are correlated FIFO because the server processes one frame at a time per connection and the version-1 ducknng frame has no transport request identifier. Cancelling or timing out one request closes that actor and fails every outstanding request on it, preventing a late reply from being assigned to the wrong operation. Abnormal closes become terminal AIO errors.

The browser Fetch, XHR, and WebSocket paths enforce the embedding host's hard hostname allowlist. Browser WebSocket does not expose arbitrary handshake-header control, so configured HTTP headers do not apply to WS/WSS. Cookies and browser credentials remain subject to browser policy.

## Frame carriers

HTTP carries one `application/vnd.ducknng.frame` request and one framed response per POST. The browser WebSocket carrier sends one binary ducknng frame per WebSocket message and receives one binary reply message.

The server half of the browser WebSocket carrier lives in `src/ducknng_ws_frame.c`. An HTTP or HTTPS ducknng service starts a raw-WebSocket sibling endpoint at `<mount>/ws`, using `ws://` or `wss://` respectively. Each frame passes through `ducknng_service_authorize_and_dispatch_frame()`, the same admission, authorization, accounting, execution-subject, and session checks as the HTTP frame endpoint.

This browser carrier is not NNG SP-over-WebSocket. Native `ws://` and `wss://` continue to use NNG's SP transport. Browsers use ducknng-frame-over-WebSocket because the JavaScript WebSocket API cannot reproduce NNG's SP handshake and stream framing. Synchronous browser WS/WSS helpers reject the operation and direct callers to the raw AIO helper family.

## Conformance gate

`node test/browser/run_smoke.mjs <site> --probes=conformance` first compares the scalar capability JSON with the active row returned by the capability table function. It then applies the three-state policy to each carrier probe.

The `wasm_eh` gate covers:

- extension loading and scalar SQL;
- same-origin HTTP GET/POST, headers, status-as-data, invalid input, and CORS failures;
- asynchronous Fetch pending, collect, cancellation, timeout, and cleanup;
- JSON, text, and CSV table decoding;
- HTTPS/CORS and explicit TLS-handle rejection;
- raw, structured, AIO, and session RPC over HTTP;
- WS and WSS framed RPC, persistent actor reuse, cancellation, timeout, abnormal close, synchronous-call rejection, and explicit WSS TLS-handle rejection.

The Pages workflow runs this conformance gate against the CI-built `wasm_eh` artifact before publishing any demo artifact. The `wasm_threads` build remains a report-only load check because repeated browser runs still expose runtime load and NNG pthread progress instability.

## Incremental HTTP bodies

The capability descriptor reports browser response streaming as `unsupported`. Future implementation should follow the direction of [`r-lib/nanonext#329`](https://github.com/r-lib/nanonext/issues/329): open an HTTP session that returns status and headers, then receive body chunks through the ordinary AIO receive model, with a zero-length chunk marking EOF. Browser `ReadableStream.getReader()` can implement that transport later. Do not expand the public API with another browser-specific streaming method family.

## Hosting and artifacts

The local harness serves COOP/COEP and no-cache headers. GitHub Pages uses a same-origin mirror of the CI-built side module because direct release assets may not provide the CORS/CORP response headers required by a cross-origin-isolated threaded runtime. The rolling Pages artifact is demo provenance, not a stable binary release channel.

The detailed build commands, target matrix, and evidence log are in `docs/wasm.md`. `docs/wasm_browser_transport_checklist.md` records the completed browser-client scope and explicit non-goals. `test/browser/README.md` documents the executable probes.
