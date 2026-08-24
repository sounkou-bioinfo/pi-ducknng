#pragma once
#include "duckdb_extension.h"
#include <stddef.h>
#include <stdint.h>

/*
 * Browser HTTP(S) client bridge for the Emscripten/duckdb-wasm build.
 *
 * Raw tcp://, ipc://, and tls+tcp:// sockets cannot exist in a browser sandbox,
 * so the native NNG HTTP client path in ducknng_http_compat.c does not work
 * there. Under __EMSCRIPTEN__, ducknng_http_transact() routes http:// and
 * https:// requests through this shim, which performs a synchronous
 * XMLHttpRequest (allowed inside the duckdb-wasm Web Worker) via EM_ASM and
 * copies the status, response headers, and body back into extension memory.
 * TLS is browser-managed; callers reject any explicit TLS configuration before
 * dispatching here.
 *
 * The whole implementation is compiled only when __EMSCRIPTEN__ is defined; on
 * native targets this translation unit is empty.
 */

#ifdef __EMSCRIPTEN__

/*
 * Perform one synchronous browser HTTP request.
 *
 * headers_json is the request header set as a JSON array of {"name","value"}
 * objects (the same shape ducknng_ncurl already accepts). On success the
 * response headers are returned as a raw header block ("Name: value\r\n" lines,
 * matching nni_http_res_headers) via out_header_block, which the caller converts
 * to the public JSON form with the existing helper. timeout_ms is accepted for
 * signature parity but ignored: synchronous XHR does not support a timeout.
 *
 * Returns 0 on success (a completed HTTP exchange, regardless of status code) or
 * -1 on a transport/setup failure, allowlist denial, or out-of-memory, writing
 * an owned message into *errmsg. All out_* buffers are duckdb_malloc-allocated
 * and owned by the caller.
 */
int ducknng_wasm_http_fetch_perform(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, size_t body_len, int timeout_ms,
    uint16_t *out_status, char **out_header_block, uint8_t **out_body,
    size_t *out_body_len, char **errmsg);

#endif /* __EMSCRIPTEN__ */
