#pragma once
#include "ducknng_nng_compat.h"
#include "ducknng_transport.h"
#include <stddef.h>
#include <stdint.h>

/*
 * Net backend boundary (docs/browser_support.md, issue #8).
 *
 * Shared code above this line must not fork on __EMSCRIPTEN__. The target is
 * selected once, at build time, and everything target-specific lives behind
 * this vtable plus a machine-readable capability descriptor. Step 1 routes
 * the synchronous HTTP exchange; AIO submission and the frame carrier join
 * the boundary in later steps.
 */

typedef enum ducknng_net_cap {
    DUCKNNG_NET_CAP_UNSUPPORTED = 0,
    DUCKNNG_NET_CAP_EXPERIMENTAL = 1,
    DUCKNNG_NET_CAP_SUPPORTED = 2
} ducknng_net_cap;

typedef enum ducknng_net_tls_owner {
    DUCKNNG_NET_TLS_OWNER_NATIVE = 0,
    DUCKNNG_NET_TLS_OWNER_BROWSER_MANAGED = 1
} ducknng_net_tls_owner;

typedef struct ducknng_net_caps {
    const char *backend_name;   /* "native" or "browser" */
    /* per-transport support level */
    ducknng_net_cap http;       /* http://  */
    ducknng_net_cap https;      /* https:// (TLS ownership below) */
    ducknng_net_cap http_response_stream; /* incremental client response body */
    ducknng_net_cap inproc;     /* inproc:// */
    ducknng_net_cap tcp;        /* tcp:// */
    ducknng_net_cap ipc;        /* ipc:// */
    ducknng_net_cap tls_tcp;    /* tls+tcp:// */
    ducknng_net_cap websocket;  /* ws:// wss:// */
    /* async semantics */
    int async_is_real;          /* 0 => AIO handles are terminal at launch */
    int honors_timeout;
    int honors_cancel;
    ducknng_net_tls_owner tls_owner;
} ducknng_net_caps;

struct ducknng_http_frame_client;

typedef struct ducknng_net_backend {
    const ducknng_net_caps *(*capabilities)(void);
    /* One synchronous HTTP exchange; the contract of ducknng_http_transact. */
    int (*http_transact)(const char *url, const char *method, const char *headers_json,
        const uint8_t *body, size_t body_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
        uint16_t *out_status, char **out_headers_json, uint8_t **out_body, size_t *out_body_len,
        char **errmsg);
    /* HTTP frame carrier: send one framed request, await the reply frame.
     * Contracts of the ducknng_http_frame_client_* functions. */
    int (*frame_client_open)(const char *url, const ducknng_tls_opts *tls_opts,
        struct ducknng_http_frame_client **out_client, char **errmsg);
    int (*frame_client_transact)(struct ducknng_http_frame_client *client,
        const uint8_t *frame, size_t frame_len, int timeout_ms,
        uint8_t **out_frame, size_t *out_frame_len, char **errmsg);
    int (*frame_client_transact_msg)(struct ducknng_http_frame_client *client,
        const uint8_t *frame, size_t frame_len, int timeout_ms,
        nng_msg **out_msg, char **errmsg);
} ducknng_net_backend;

/* The build-target backend; never NULL after extension load. */
const ducknng_net_backend *ducknng_net_backend_get(void);

/* Whether the active backend reaches a URL of this scheme through the frame
 * carrier (return 1) rather than an nng socket (return 0). HTTP/HTTPS are always
 * carrier; ws/wss are carrier only on the browser backend (no nng). Shared
 * client dispatch routes on this instead of on ducknng_transport_url_is_http so
 * the browser can carry ducknng frames over ws/wss. */
int ducknng_net_backend_carrier_scheme(ducknng_transport_scheme scheme);

/* All build targets' capability descriptors, as data, in every build; the
 * docs matrix and conformance harness render from this instead of prose. */
const ducknng_net_caps *ducknng_net_caps_all(size_t *out_count);

const char *ducknng_net_cap_name(ducknng_net_cap cap);
const char *ducknng_net_tls_owner_name(ducknng_net_tls_owner owner);

/* Render a capability descriptor as a JSON object (duckdb_malloc; caller
 * frees). Returns NULL on allocation failure. */
char *ducknng_net_caps_to_json(const ducknng_net_caps *caps);
