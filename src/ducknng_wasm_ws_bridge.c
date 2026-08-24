/*
 * Browser WebSocket frame carrier -> AIO completion bridge (issue #11).
 *
 * The browser-side companion to the server ducknng-frame-over-WebSocket
 * endpoint. It keeps one persistent JS WebSocket per URL and rides the exact
 * same op-record table + pump as the fetch bridge (ducknng_wasm_fetch_bridge.c):
 * a launch registers an op in Module.ducknngFetchOps and returns immediately;
 * the socket settles the op when the DB worker's event loop spins; readers pump
 * settled ops into the aio slot as a KIND_REQUEST reply. Because the server
 * processes one frame per connection at a time and replies in order, replies are
 * correlated to requests by FIFO order per socket (the wire frame carries no
 * request id). Cancel/timeout tears the socket down so a late reply can never
 * resolve the wrong op.
 *
 * Compiled only under __EMSCRIPTEN__; the launch entry is a no-op on native.
 */

#ifdef __EMSCRIPTEN__

#include "ducknng_wasm_fetch_bridge.h"
#include "ducknng_util.h"
#include <emscripten.h>
#include <limits.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

EM_JS(int, ducknng_js_ws_launch, (const char *url_ptr, const uint8_t *frame_ptr,
    int frame_len, int timeout_ms), {
    if (!Module.ducknngFetchOps) {
        Module.ducknngFetchOps = new Map();
        Module.ducknngFetchNext = 1;
    }
    if (!Module.ducknngWsActors) {
        Module.ducknngWsActors = new Map();
    }
    // Single teardown helper, shared with the fetch bridge's abort/forget path
    // (which reaches it as Module.ducknngWsFailActor). Cancelling any one
    // in-flight op breaks the socket's FIFO reply correlation, so we close the
    // socket and fail every op still on it.
    if (!Module.ducknngWsFailActor) {
        Module.ducknngWsFailActor = function(actorUrl, message) {
            var actor = Module.ducknngWsActors.get(actorUrl);
            if (!actor) return;
            Module.ducknngWsActors.delete(actorUrl);
            var ids = actor.sendQueue.map(function(e) { return e[0]; }).concat(actor.pending);
            for (var i = 0; i < ids.length; i++) {
                var o = Module.ducknngFetchOps.get(ids[i]);
                if (o && o.state === 0) {
                    o.state = 2;
                    o.error = message;
                    if (o.timer) { clearTimeout(o.timer); o.timer = 0; }
                }
            }
            actor.sendQueue = [];
            actor.pending = [];
            try { if (actor.ws) actor.ws.close(); } catch (e) {}
        };
    }

    var url = UTF8ToString(url_ptr);
    var id = Module.ducknngFetchNext++;
    // ws:true marks the op so the shared abort/forget path tears the socket down
    // instead of aborting a fetch AbortController.
    var op = { state: 0, status: 0, headers: "", body: null, error: "",
        ws: true, actorUrl: url, timer: 0, timedOut: false };
    Module.ducknngFetchOps.set(id, op);

    // Apply the same hard host allowlist as the browser Fetch/XHR paths. A
    // WebSocket frame carries the same RPC authority and must not become an
    // alternate route around an embedding host's outbound policy. Custom HTTP
    // headers are intentionally irrelevant here: browser WebSocket does not
    // expose handshake-header control.
    var cfg = Module.ducknngWasmHttpConfig || null;
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
        op.error = "ducknng: browser WebSocket host allowlist denied the request";
        return id;
    }

    // Copy the frame out of the wasm heap now; it is not stable after return.
    var payload = HEAPU8.slice(frame_ptr, frame_ptr + frame_len);

    var actor = Module.ducknngWsActors.get(url);
    if (!actor) {
        actor = { url: url, ws: null, ready: false, sendQueue: [], pending: [] };
        Module.ducknngWsActors.set(url, actor);
        try {
            actor.ws = new WebSocket(url);
        } catch (e) {
            Module.ducknngWsActors.delete(url);
            op.state = 2;
            op.error = "ducknng: browser WebSocket open failed: " + (e && e.message ? e.message : String(e));
            return id;
        }
        actor.ws.binaryType = "arraybuffer";
        actor.ws.onopen = function() {
            actor.ready = true;
            var q = actor.sendQueue;
            actor.sendQueue = [];
            for (var i = 0; i < q.length; i++) {
                try {
                    actor.ws.send(q[i][1]);
                    actor.pending.push(q[i][0]);
                } catch (e) {
                    var o = Module.ducknngFetchOps.get(q[i][0]);
                    if (o && o.state === 0) { o.state = 2; o.error = "ducknng: browser WebSocket send failed"; }
                }
            }
        };
        actor.ws.onmessage = function(ev) {
            var oid = actor.pending.shift();
            if (oid === undefined) return; /* unsolicited frame */
            var o = Module.ducknngFetchOps.get(oid);
            if (!o || o.state !== 0) return;
            o.status = 200;
            o.body = new Uint8Array(ev.data);
            o.state = 1;
            if (o.timer) { clearTimeout(o.timer); o.timer = 0; }
        };
        actor.ws.onerror = function() { Module.ducknngWsFailActor(url, "ducknng: browser WebSocket error"); };
        actor.ws.onclose = function() { Module.ducknngWsFailActor(url, "ducknng: browser WebSocket closed"); };
    }

    if (actor.ready) {
        try {
            actor.ws.send(payload);
            actor.pending.push(id);
        } catch (e) {
            op.state = 2;
            op.error = "ducknng: browser WebSocket send failed: " + (e && e.message ? e.message : String(e));
        }
    } else {
        actor.sendQueue.push([id, payload]);
    }

    if (timeout_ms > 0) {
        op.timer = setTimeout(function() {
            var o = Module.ducknngFetchOps.get(id);
            if (!o || o.state !== 0) return;
            o.timedOut = true;
            // A timed-out request breaks FIFO correlation, so tear the socket
            // down; every in-flight op on it (this one included) fails.
            Module.ducknngWsFailActor(o.actorUrl, "ducknng: browser WebSocket timed out");
        }, timeout_ms);
    }
    return id;
});

uint64_t ducknng_wasm_ws_launch(const char *url, const uint8_t *frame,
    size_t frame_len, int timeout_ms, char **errmsg) {
    int op_id;
    if (errmsg) *errmsg = NULL;
    if (!url || !url[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing WebSocket URL");
        return 0;
    }
    if (frame_len > (size_t)INT_MAX) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: WebSocket frame exceeds browser bridge size limit");
        return 0;
    }
    op_id = ducknng_js_ws_launch(url, frame, (int)frame_len, timeout_ms);
    return (uint64_t)(uint32_t)op_id;
}

#endif /* __EMSCRIPTEN__ */
