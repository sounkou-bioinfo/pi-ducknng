#pragma once
#include "duckdb_extension.h"
#include <stddef.h>
#include <stdint.h>

/*
 * Browser fetch -> AIO completion bridge (docs/browser_support.md step 4).
 *
 * In the browser, AIO handles used to be terminal at launch because the only
 * HTTP primitive was a synchronous XHR. This bridge makes browser AIO real:
 * launch registers a fetch() with an AbortController in a JS-side op table
 * keyed by op id and returns immediately; the fetch settles when the DB
 * worker's event loop spins (i.e. between SQL calls); readers pump settled
 * ops into the aio slot; cancel aborts the controller and is synchronously
 * observable; timeout_ms arms a JS timer that aborts the fetch.
 *
 * The op table is transport-agnostic on purpose: a future WebSocket frame
 * carrier settles ops through the same table and the same pump.
 *
 * One consequence of the single-threaded browser event loop is that no
 * blocking wait can make progress while SQL is executing, so wait/collect
 * loops must poll once and return instead of sleeping; they query
 * ducknng_wasm_aio_wait_can_block() for that.
 *
 * On native targets everything here is an inline no-op, so callers need no
 * target forks.
 */

struct ducknng_client_aio;

#ifdef __EMSCRIPTEN__

/* Start one browser fetch. Returns a non-zero op id, or 0 with *errmsg set.
 * body bytes are copied on the JS side before returning. timeout_ms <= 0
 * means no timeout. */
uint64_t ducknng_wasm_fetch_launch(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, size_t body_len,
    int timeout_ms, char **errmsg);

/* Send one ducknng frame over a persistent browser WebSocket to url (ws:// or
 * wss://). Returns a non-zero op id, or 0 with *errmsg set. The frame bytes are
 * copied on the JS side before returning. The reply settles the same op table
 * the fetch bridge uses, so ducknng_wasm_fetch_pump() completes the slot as a
 * KIND_REQUEST reply. timeout_ms <= 0 means no timeout. */
uint64_t ducknng_wasm_ws_launch(const char *url, const uint8_t *frame,
    size_t frame_len, int timeout_ms, char **errmsg);

/* If the slot's op has settled, complete the slot in place (status, headers,
 * body / reply_msg, error, state, finished_ms) and forget the op. Returns 1
 * when the slot changed state, 0 otherwise. Call with rt->mu held, like the
 * native completion callback. */
int ducknng_wasm_fetch_pump(struct ducknng_client_aio *slot);

/* Abort a pending op; synchronously marks it cancelled so the same query can
 * observe the state. Safe on settled or unknown ids. */
void ducknng_wasm_fetch_abort(uint64_t op_id);

/* Forget an op whose slot is being destroyed. Safe on unknown ids. */
void ducknng_wasm_fetch_forget(uint64_t op_id);

/* 0: waiting cannot make progress on this target (single-threaded event
 * loop); poll once and return. */
static inline int ducknng_wasm_aio_wait_can_block(void) { return 0; }

#else /* native no-ops */

static inline uint64_t ducknng_wasm_fetch_launch(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, size_t body_len,
    int timeout_ms, char **errmsg) {
    (void)url; (void)method; (void)headers_json; (void)body; (void)body_len;
    (void)timeout_ms; (void)errmsg;
    return 0;
}

static inline uint64_t ducknng_wasm_ws_launch(const char *url, const uint8_t *frame,
    size_t frame_len, int timeout_ms, char **errmsg) {
    (void)url; (void)frame; (void)frame_len; (void)timeout_ms; (void)errmsg;
    return 0;
}

static inline int ducknng_wasm_fetch_pump(struct ducknng_client_aio *slot) {
    (void)slot;
    return 0;
}

static inline void ducknng_wasm_fetch_abort(uint64_t op_id) { (void)op_id; }
static inline void ducknng_wasm_fetch_forget(uint64_t op_id) { (void)op_id; }
static inline int ducknng_wasm_aio_wait_can_block(void) { return 1; }

#endif /* __EMSCRIPTEN__ */
