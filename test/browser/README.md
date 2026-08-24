# Browser wasm conformance

`run_smoke.mjs` serves a staged duckdb-wasm site with COOP/COEP headers and drives it in real Chromium. The release gate is capability-driven: it reads the extension's active network descriptor, checks scalar/table agreement, and runs every supported probe as a hard assertion.

## Setup

```sh
cd test/browser
npm ci
npx playwright install chromium
cd ../..
```

Stage the release-supported EH artifact:

```sh
DUCKNNG_WASM_SERVE=0 DUCKDB_WASM_PLATFORM=wasm_eh \
  scripts/start_duckdb_wasm_local_test.sh
```

Run the complete gate:

```sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site \
  --probes=conformance
```

A successful run ends with:

```text
BROWSER SMOKE: PASS
```

The same command gates the wasm Pages workflow before demo artifacts are published.

## Probes

`load` loads the extension, checks cross-origin isolation, and runs scalar SQL.

`inproc` exercises the page's scalar, codec, raw-AIO, and manifest path. It is unsupported on `wasm_eh` and experimental on `wasm_threads`, so it does not gate the release lane.

`http-sync` covers same-origin GET/POST, request and response headers, status-as-data, invalid input, and no-CORS failure mapping.

`http-aio` covers pending state, collection, poll-style wait, cancellation, timeout, terminal launch errors, and cleanup through the Fetch/AbortController bridge.

`http-table` decodes JSON, text, and CSV responses.

`https-cors` covers a separate local HTTPS origin, exposed headers, table decoding, and rejection of explicit ducknng TLS handles under browser-managed TLS.

`http-rpc` covers raw, structured, AIO, and query-session helpers over the HTTP frame carrier.

`ws-rpc` covers asynchronous `ws://` and `wss://` framed RPC, persistent actor reuse, cancellation, timeout, abnormal close, synchronous-call rejection, and explicit WSS TLS-handle rejection. It uses a bounded local WebSocket frame responder from the pinned `ws` development dependency.

`conformance` reads `ducknng_transport_capabilities()`, verifies the active `ducknng_list_transport_capabilities()` row, then runs supported probes, skips unsupported probes, and treats experimental failures as report-only. It is the preferred command; explicit probe lists are for diagnosis.

## Diagnostic threaded runtime

```sh
DUCKNNG_WASM_SERVE=0 DUCKDB_WASM_PLATFORM=wasm_threads \
  scripts/start_duckdb_wasm_local_test.sh
node test/browser/run_smoke.mjs .duckdb-wasm-local-artifacts/site \
  --probes=load
```

Repeated `wasm_threads` runs can expose extension-load and NNG pthread progress timeouts. Do not promote those paths from `experimental` without repeatable browser evidence.

`BROWSER_DEBUG=1` mirrors page console output. `BROWSER_HEADFUL=1` opens Chromium visibly. Screenshots are not proof; retain the textual passing log.
