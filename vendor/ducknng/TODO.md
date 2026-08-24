# ducknng TODO

## Error API Consistency

All items resolved. The decisions and rules are documented in the "Error surface contract" section of `docs/protocol.md`.

- [x] AIO result rows now expose `nng_error` / `nng_error_message` in `ducknng_aio_status`, `ducknng_aio_collect`, and `ducknng_aio_collect_decoded`.

- [x] URL-path `ducknng_request` now threads `nng_error` through `ducknng_client_roundtrip_raw_tls` and populates it in the result row.

- [x] Local ducknng error frames intentionally do not carry NNG numeric codes. The numeric transport code belongs in the collect row that delivered the frame, not inside the frame payload. Documented.

- [x] High-level RPC/session helpers (`ducknng_get_rpc_manifest`, `ducknng_run_rpc`, `ducknng_open_query`, `ducknng_fetch_query`, `ducknng_close_query`, `ducknng_cancel_query`) expose `ok, error` only. Multi-step transport sequences make a single NNG error code attribution unreliable. Documented as intentional.

- [x] HTTP client helpers use `status INTEGER` as the carrier-level code. Pre-connection failures remain text-only. No `nng_error` column on HTTP result rows. Documented as intentional.

- [x] Lifecycle/admin mutators (service, TLS, registry, method-auth, allowlist, authorizer) throw on missing IDs or invalid arguments. These are programmer/configuration errors. Documented as intentional.

- [x] Dynamic-schema table helpers throw at bind time when DuckDB requires a schema and none can be produced. Documented as intentional.

- [x] Scalar frame accessors return NULL for invalid or absent frames. Correct SQL behavior for projection/filtering. Documented as intentional.

- [x] Parser/accessor helpers throw for malformed syntax, return NULL for missing names. Rule named and documented.

- [x] Docs and catalog updated: `docs/protocol.md` has the Error Surface Contract section; `function_catalog/functions.yaml` updated for `ducknng_aio_status`, `ducknng_aio_collect`, and `ducknng_aio_collect_decoded`.

- [x] Shared internal error-result helper: deferred. The surfaces differ enough in shape (struct vs table vs frame) that a single helper would be awkward. Revisit if a fourth or fifth call site emerges.
