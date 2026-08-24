/*
 * Browser fetch -> AIO completion bridge implementation. See the header for
 * the execution model. Compiled only under __EMSCRIPTEN__; empty on native.
 *
 * JS-side op record states: 0 pending, 1 http-complete, 2 failed, 3 cancelled.
 * Ops are settled by fetch handlers that run when the DB worker's event loop
 * spins between SQL calls; abort/cancel is marked synchronously from C so the
 * launching query can already observe it.
 */

#ifdef __EMSCRIPTEN__

#include "ducknng_wasm_fetch_bridge.h"
#include "ducknng_http_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_shared.h"
#include "ducknng_util.h"
#include <emscripten.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

EM_JS(int, ducknng_js_fetch_launch, (const char *url_ptr, const char *method_ptr,
    const char *headers_ptr, const uint8_t *body_ptr, int body_len, int timeout_ms), {
    if (!Module.ducknngFetchOps) {
        Module.ducknngFetchOps = new Map();
        Module.ducknngFetchNext = 1;
    }
    var url = UTF8ToString(url_ptr);
    var method = method_ptr ? UTF8ToString(method_ptr) : "";
    var headersJson = headers_ptr ? UTF8ToString(headers_ptr) : "";
    var cfg = Module.ducknngWasmHttpConfig || null;
    var id = Module.ducknngFetchNext++;
    var op = { state: 0, status: 0, headers: "", body: null, error: "",
        ctrl: new AbortController(), timer: 0, timedOut: false };
    Module.ducknngFetchOps.set(id, op);

    // Same host-allowlist / configured-header policy the synchronous XHR
    // bridge (ducknng_wasm_http_fetch.c) enforces, so async fetches cannot
    // bypass a host's browser HTTP config.
    function hostMatchesAllowlist(targetUrl, allowHosts) {
        var host = "";
        var allow = allowHosts;
        if (!allow) return false;
        if (typeof allow === "string") allow = allow.split(",");
        try { host = new URL(targetUrl).hostname.toLowerCase(); } catch (e) { return false; }
        if (!Array.isArray(allow)) return false;
        for (var i = 0; i < allow.length; i++) {
            var raw = allow[i];
            if (raw === null || raw === undefined) continue;
            var rule = String(raw).trim().toLowerCase();
            if (!rule) continue;
            if (rule.charAt(0) === ".") {
                if (host.length > rule.length && host.endsWith(rule)) return true;
            } else if (host === rule) {
                return true;
            }
        }
        return false;
    }
    if (cfg && cfg.enforceHostAllowlist === true && !hostMatchesAllowlist(url, cfg.allowHosts)) {
        op.state = 2;
        op.error = "ducknng: browser HTTP host allowlist denied the request";
        return id;
    }

    var headers = [];
    if (headersJson) {
        try {
            headers = JSON.parse(headersJson).map(function(h) { return [h.name, h.value]; });
        } catch (e) {
            op.state = 2;
            op.error = "ducknng: invalid headers_json";
            return id;
        }
    }
    // Configured headers and credentials are both scoped to the host
    // allowlist, matching the synchronous XHR bridge, so browser credentials
    // are never attached to origins outside the configured allowlist.
    var configHostAllowed = !!(cfg && hostMatchesAllowlist(url, cfg.allowHosts));
    if (cfg && cfg.headers && configHostAllowed) {
        var isHttps = url.toLowerCase().startsWith("https://");
        var allowInsecureAuth = !!(cfg.allowInsecureAuth);
        for (var keyName in cfg.headers) {
            if (!Object.prototype.hasOwnProperty.call(cfg.headers, keyName)) continue;
            if (cfg.headers[keyName] === null || cfg.headers[keyName] === undefined) continue;
            if (!allowInsecureAuth && keyName.toLowerCase() === "authorization" && !isHttps) continue;
            headers.push([String(keyName), String(cfg.headers[keyName])]);
        }
    }
    var withCreds = !!(cfg && cfg.withCredentials === true && configHostAllowed);
    var init = { method: method || "GET", headers: headers,
        signal: op.ctrl.signal, credentials: withCreds ? "include" : "same-origin" };
    if (body_len > 0) init.body = HEAPU8.slice(body_ptr, body_ptr + body_len);
    if (timeout_ms > 0) {
        op.timer = setTimeout(function() { op.timedOut = true; op.ctrl.abort(); }, timeout_ms);
    }
    try {
        fetch(url, init).then(async function(resp) {
            var buf = new Uint8Array(await resp.arrayBuffer());
            if (op.state !== 0) return;
            var blk = "";
            resp.headers.forEach(function(v, k) { blk += k + ": " + v + "\r\n"; });
            op.status = resp.status;
            op.headers = blk;
            op.body = buf;
            op.state = 1;
        }).catch(function(e) {
            if (op.state !== 0) return;
            op.state = 2;
            op.error = op.timedOut ? "ducknng: browser fetch timed out"
                : "ducknng: browser fetch failed: " + (e && e.message ? e.message : String(e));
        }).finally(function() {
            if (op.timer) { clearTimeout(op.timer); op.timer = 0; }
        });
    } catch (e) {
        op.state = 2;
        op.error = "ducknng: browser fetch launch failed: " + (e && e.message ? e.message : String(e));
    }
    return id;
});

EM_JS(void, ducknng_js_fetch_abort, (int id), {
    var op = Module.ducknngFetchOps ? Module.ducknngFetchOps.get(id) : undefined;
    if (!op || op.state !== 0) return;
    op.state = 3;
    if (op.timer) { clearTimeout(op.timer); op.timer = 0; }
    // A WebSocket op shares a persistent socket whose replies are matched by
    // FIFO order (issue #11); cancelling one op would misalign the rest, so tear
    // the whole socket down. Module.ducknngWsFailActor is installed by the ws
    // bridge and only reachable for op.ws records.
    if (op.ws) { if (Module.ducknngWsFailActor) Module.ducknngWsFailActor(op.actorUrl, "ducknng: aio cancelled"); return; }
    try { op.ctrl.abort(); } catch (e) {}
});

EM_JS(void, ducknng_js_fetch_forget, (int id), {
    if (!Module.ducknngFetchOps) return;
    var op = Module.ducknngFetchOps.get(id);
    if (!op) return;
    if (op.timer) { clearTimeout(op.timer); op.timer = 0; }
    if (op.ws) {
        if (op.state === 0 && Module.ducknngWsFailActor) Module.ducknngWsFailActor(op.actorUrl, "ducknng: aio forgotten");
        Module.ducknngFetchOps.delete(id);
        return;
    }
    if (op.state === 0) { try { op.ctrl.abort(); } catch (e) {} }
    Module.ducknngFetchOps.delete(id);
});

EM_JS(int, ducknng_js_fetch_query, (int id, int *status_out, int *hlen_out,
    int *blen_out, int *elen_out), {
    var op = Module.ducknngFetchOps ? Module.ducknngFetchOps.get(id) : undefined;
    if (!op) return -1;
    if (op.state === 0) return 0;
    setValue(status_out, op.status | 0, "i32");
    setValue(hlen_out, op.headers ? lengthBytesUTF8(op.headers) : 0, "i32");
    setValue(blen_out, op.body ? op.body.length : 0, "i32");
    setValue(elen_out, op.error ? lengthBytesUTF8(op.error) : 0, "i32");
    return op.state;
});

EM_JS(void, ducknng_js_fetch_copy, (int id, char *hbuf, int hlen, uint8_t *bbuf,
    char *ebuf, int elen), {
    var op = Module.ducknngFetchOps.get(id);
    if (op.headers) stringToUTF8(op.headers, hbuf, hlen + 1);
    if (op.body && op.body.length) HEAPU8.set(op.body, bbuf);
    if (op.error) stringToUTF8(op.error, ebuf, elen + 1);
    Module.ducknngFetchOps.delete(id);
});


uint64_t ducknng_wasm_fetch_launch(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, size_t body_len,
    int timeout_ms, char **errmsg) {
    int op_id;

    if (errmsg) *errmsg = NULL;
    if (ducknng_validate_http_url(url, errmsg) != 0) return 0;
    if (method && method[0] && !ducknng_http_token_is_valid(method)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP method must be an HTTP token");
        return 0;
    }
    if (headers_json && headers_json[0] &&
        ducknng_http_validate_headers_json(headers_json, errmsg) != 0) {
        return 0;
    }
    op_id = ducknng_js_fetch_launch(url, method, headers_json, body, (int)body_len, timeout_ms);
    return (uint64_t)(uint32_t)op_id;
}

void ducknng_wasm_fetch_abort(uint64_t op_id) {
    if (op_id == 0) return;
    ducknng_js_fetch_abort((int)op_id);
}

void ducknng_wasm_fetch_forget(uint64_t op_id) {
    if (op_id == 0) return;
    ducknng_js_fetch_forget((int)op_id);
}

static char *ducknng_wasm_dup_text(const uint8_t *src, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, src, len);
    out[len] = (char)0;
    return out;
}

static void ducknng_wasm_fetch_pump_fail(struct ducknng_client_aio *slot,
    char *owned_error, int cancelled) {
    if (slot->error) duckdb_free(slot->error);
    slot->error = owned_error ? owned_error :
        ducknng_strdup("ducknng: browser fetch failed");
    slot->state = cancelled ? DUCKNNG_CLIENT_AIO_CANCELLED : DUCKNNG_CLIENT_AIO_ERROR;
}

int ducknng_wasm_fetch_pump(struct ducknng_client_aio *slot) {
    int op_state;
    int status = 0;
    int headers_len = 0;
    int body_len = 0;
    int error_len = 0;
    char *headers_block = NULL;
    uint8_t *body = NULL;
    char *error_text = NULL;

    if (!slot || slot->wasm_op_id == 0 || slot->state != DUCKNNG_CLIENT_AIO_PENDING) return 0;
    op_state = ducknng_js_fetch_query((int)slot->wasm_op_id, &status, &headers_len,
        &body_len, &error_len);
    if (op_state == 0) return 0;
    if (op_state < 0) {
        ducknng_wasm_fetch_pump_fail(slot,
            ducknng_strdup("ducknng: browser fetch op was lost"), 0);
        goto done;
    }
    headers_block = (char *)duckdb_malloc((size_t)headers_len + 1);
    body = body_len > 0 ? (uint8_t *)duckdb_malloc((size_t)body_len) : NULL;
    error_text = (char *)duckdb_malloc((size_t)error_len + 1);
    if (!headers_block || (body_len > 0 && !body) || !error_text) {
        if (headers_block) { duckdb_free(headers_block); headers_block = NULL; }
        if (body) { duckdb_free(body); body = NULL; }
        if (error_text) { duckdb_free(error_text); error_text = NULL; }
        ducknng_wasm_fetch_forget(slot->wasm_op_id);
        ducknng_wasm_fetch_pump_fail(slot,
            ducknng_strdup("ducknng: out of memory copying browser fetch result"), 0);
        goto done;
    }
    headers_block[0] = '\0';
    error_text[0] = '\0';
    ducknng_js_fetch_copy((int)slot->wasm_op_id, headers_block, headers_len, body,
        error_text, error_len);
    if (op_state == 3) {
        ducknng_wasm_fetch_pump_fail(slot, ducknng_strdup("ducknng: aio cancelled"), 1);
        goto done;
    }
    if (op_state == 2) {
        char *msg = error_text;
        error_text = NULL;
        ducknng_wasm_fetch_pump_fail(slot, msg, 0);
        goto done;
    }
    slot->send_done = 1;
    slot->recv_done = 1;
    slot->send_result = 0;
    slot->recv_result = 0;
    if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_NCURL) {
        char *convert_err = NULL;
        char *headers_json = ducknng_http_headers_block_to_json(headers_block, &convert_err);
        if (!headers_json) {
            if (body) { duckdb_free(body); body = NULL; }
            ducknng_wasm_fetch_pump_fail(slot, convert_err ? convert_err :
                ducknng_strdup("ducknng: failed to convert browser response headers"), 0);
            goto done;
        }
        slot->http_status = (uint16_t)status;
        slot->http_headers_json = headers_json;
        slot->http_body = body;
        slot->http_body_len = (size_t)body_len;
        body = NULL;
        if (slot->http_body && slot->http_body_len > 0 &&
            ducknng_sql_bytes_look_text(slot->http_body, slot->http_body_len)) {
            slot->http_body_text = ducknng_wasm_dup_text(slot->http_body, slot->http_body_len);
            if (!slot->http_body_text) {
                ducknng_wasm_fetch_pump_fail(slot,
                    ducknng_strdup("ducknng: out of memory copying HTTP response text"), 0);
                goto done;
            }
        }
        slot->state = DUCKNNG_CLIENT_AIO_READY;
    } else if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_REQUEST) {
        if (status != 200) {
            ducknng_wasm_fetch_pump_fail(slot,
                ducknng_http_status_error_message((uint16_t)status, body, (size_t)body_len), 0);
            goto done;
        }
        if (nng_msg_alloc(&slot->reply_msg, (size_t)body_len) != 0) {
            ducknng_wasm_fetch_pump_fail(slot,
                ducknng_strdup("ducknng: out of memory copying HTTP frame reply"), 0);
            goto done;
        }
        if (body_len > 0) memcpy(nng_msg_body(slot->reply_msg), body, (size_t)body_len);
        slot->state = DUCKNNG_CLIENT_AIO_READY;
    } else {
        ducknng_wasm_fetch_pump_fail(slot,
            ducknng_strdup("ducknng: unsupported browser fetch aio kind"), 0);
    }
done:
    if (headers_block) duckdb_free(headers_block);
    if (body) duckdb_free(body);
    if (error_text) duckdb_free(error_text);
    slot->wasm_op_id = 0;
    slot->finished_ms = ducknng_now_ms();
    return 1;
}

#endif /* __EMSCRIPTEN__ */
