#include "ducknng_wasm_http_fetch.h"

#ifdef __EMSCRIPTEN__

#include "ducknng_util.h"
#include <emscripten.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/*
 * Phase 1: issue the synchronous XHR and stash the result on the Module object.
 *
 * Request headers arrive as a JSON array of {name,value} objects; an optional
 * Module.ducknngWasmHttpConfig contributes allowlist-gated extra headers and an
 * optional hard host allowlist, mirroring duckhts's Module.duckhtsWasmHttpConfig.
 *
 * Return codes (kept distinct so the C side can produce a precise error):
 *    >=100  the HTTP status of a completed exchange
 *        0  network/CORS failure (xhr.send threw)
 *       -1  request blocked by the configured host allowlist
 *       -2  setup error (bad URL, header JSON, or open() threw)
 */
static int ducknng_wasm_http_fetch_send(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, int body_len) {
    return EM_ASM_INT({
        var url = UTF8ToString($0);
        var method = $1 ? UTF8ToString($1) : "GET";
        var headersJson = $2 ? UTF8ToString($2) : "";
        var bodyPtr = $3;
        var bodyLen = $4;
        var cfg = Module.ducknngWasmHttpConfig || null;
        Module.ducknngWasmHttpLast = null;

        function hostMatchesAllowlist(targetUrl, allowHosts) {
            var host = "";
            var allow = allowHosts;
            var i;
            if (!allow) return false;
            if (typeof allow === "string") allow = allow.split(",");
            try {
                host = new URL(targetUrl).hostname.toLowerCase();
            } catch (e) {
                return false;
            }
            if (!Array.isArray(allow)) return false;
            for (i = 0; i < allow.length; i++) {
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

        function shouldAllowRequest(targetUrl) {
            if (!cfg || cfg.enforceHostAllowlist !== true) return true;
            return hostMatchesAllowlist(targetUrl, cfg.allowHosts);
        }

        function applyConfiguredHeaders(xhrObj, targetUrl) {
            var headers;
            var keyName;
            var isHttps = (typeof targetUrl === "string") &&
                targetUrl.toLowerCase().startsWith("https://");
            var allowInsecureAuth = !!(cfg && cfg.allowInsecureAuth);
            if (!cfg || !cfg.headers) return;
            if (!hostMatchesAllowlist(targetUrl, cfg.allowHosts)) return;
            headers = cfg.headers;
            for (keyName in headers) {
                if (!Object.prototype.hasOwnProperty.call(headers, keyName)) continue;
                if (headers[keyName] === null || headers[keyName] === undefined) continue;
                if (!allowInsecureAuth && keyName.toLowerCase() === "authorization" && !isHttps) continue;
                try {
                    xhrObj.setRequestHeader(String(keyName), String(headers[keyName]));
                } catch (e) {
                }
            }
            if (cfg.withCredentials === true) xhrObj.withCredentials = true;
        }

        if (!shouldAllowRequest(url)) return -1;

        var xhr = new XMLHttpRequest();
        try {
            xhr.open(method, url, false);   /* false = synchronous; allowed in Workers */
        } catch (e) {
            return -2;
        }
        xhr.responseType = "arraybuffer";

        if (headersJson) {
            var arr = null;
            try {
                arr = JSON.parse(headersJson);
            } catch (e) {
                return -2;
            }
            if (Array.isArray(arr)) {
                for (var i = 0; i < arr.length; i++) {
                    var h = arr[i];
                    if (h && h.name !== null && h.name !== undefined &&
                        h.value !== null && h.value !== undefined) {
                        try {
                            xhr.setRequestHeader(String(h.name), String(h.value));
                        } catch (e) {
                        }
                    }
                }
            }
        }
        applyConfiguredHeaders(xhr, url);

        var payload = null;
        if (bodyPtr && bodyLen > 0) payload = HEAPU8.slice(bodyPtr, bodyPtr + bodyLen);
        try {
            xhr.send(payload);
        } catch (e) {
            return 0;
        }

        var data = new Uint8Array(xhr.response || new ArrayBuffer(0));
        var hdrs = "";
        try {
            hdrs = xhr.getAllResponseHeaders() || "";
        } catch (e) {
            hdrs = "";
        }
        /* Assign fields one at a time: a {a,b} object literal would expose the
         * commas to the C preprocessor, which only protects commas inside (). */
        var result = {};
        result.status = xhr.status;
        result.body = data;
        result.headers = hdrs;
        Module.ducknngWasmHttpLast = result;
        return xhr.status | 0;
    }, url, method, headers_json, body, body_len);
}

static int ducknng_wasm_http_fetch_body_len(void) {
    return EM_ASM_INT({
        var r = Module.ducknngWasmHttpLast;
        return (r && r.body) ? r.body.length : 0;
    });
}

static void ducknng_wasm_http_fetch_copy_body(uint8_t *dst, int len) {
    EM_ASM({
        var r = Module.ducknngWasmHttpLast;
        if (r && r.body && $1 > 0) HEAPU8.set(r.body.subarray(0, $1), $0);
    }, dst, len);
}

static int ducknng_wasm_http_fetch_headers_len(void) {
    return EM_ASM_INT({
        var r = Module.ducknngWasmHttpLast;
        return (r && r.headers) ? lengthBytesUTF8(r.headers) : 0;
    });
}

static void ducknng_wasm_http_fetch_copy_headers(char *dst, int cap) {
    EM_ASM({
        var r = Module.ducknngWasmHttpLast;
        var s = (r && r.headers) ? r.headers : "";
        stringToUTF8(s, $0, $1);
    }, dst, cap);
}

static void ducknng_wasm_http_fetch_clear(void) {
    EM_ASM({ Module.ducknngWasmHttpLast = null; });
}

int ducknng_wasm_http_fetch_perform(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, size_t body_len, int timeout_ms,
    uint16_t *out_status, char **out_header_block, uint8_t **out_body,
    size_t *out_body_len, char **errmsg) {
    int status;
    int blen;
    int hlen;
    int send_body_len;
    uint8_t *body_buf = NULL;
    char *header_buf = NULL;

    (void)timeout_ms; /* synchronous XHR cannot honor a timeout; ignored by design */
    if (out_status) *out_status = 0;
    if (out_header_block) *out_header_block = NULL;
    if (out_body) *out_body = NULL;
    if (out_body_len) *out_body_len = 0;
    if (errmsg) *errmsg = NULL;
    if (!url || !url[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing URL for browser HTTP request");
        return -1;
    }
    if (body_len > (size_t)INT_MAX) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: browser HTTP request body is too large");
        return -1;
    }
    send_body_len = (body && body_len > 0) ? (int)body_len : 0;

    status = ducknng_wasm_http_fetch_send(url, method ? method : "GET", headers_json,
        body, send_body_len);
    if (status == -1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: browser HTTP request blocked by host allowlist");
        ducknng_wasm_http_fetch_clear();
        return -1;
    }
    if (status == -2) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start browser HTTP request (bad URL or headers)");
        ducknng_wasm_http_fetch_clear();
        return -1;
    }
    if (status == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: browser HTTP request failed (network or CORS error)");
        ducknng_wasm_http_fetch_clear();
        return -1;
    }
    if (status < 100 || status > 599) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: browser HTTP request returned an invalid status");
        ducknng_wasm_http_fetch_clear();
        return -1;
    }

    blen = ducknng_wasm_http_fetch_body_len();
    if (blen < 0) blen = 0;
    body_buf = (uint8_t *)duckdb_malloc((size_t)blen + 1u);
    if (!body_buf) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying browser HTTP body");
        ducknng_wasm_http_fetch_clear();
        return -1;
    }
    if (blen > 0) ducknng_wasm_http_fetch_copy_body(body_buf, blen);

    hlen = ducknng_wasm_http_fetch_headers_len();
    if (hlen < 0) hlen = 0;
    header_buf = (char *)duckdb_malloc((size_t)hlen + 1u);
    if (!header_buf) {
        duckdb_free(body_buf);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying browser HTTP headers");
        ducknng_wasm_http_fetch_clear();
        return -1;
    }
    ducknng_wasm_http_fetch_copy_headers(header_buf, hlen + 1);
    header_buf[hlen] = '\0';

    if (out_status) *out_status = (uint16_t)status;
    if (out_body_len) *out_body_len = (size_t)blen;
    if (out_body) {
        *out_body = body_buf;
        body_buf = NULL;
    }
    if (out_header_block) {
        *out_header_block = header_buf;
        header_buf = NULL;
    }
    if (body_buf) duckdb_free(body_buf);
    if (header_buf) duckdb_free(header_buf);
    ducknng_wasm_http_fetch_clear();
    return 0;
}

#endif /* __EMSCRIPTEN__ */
