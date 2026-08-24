#pragma once
#include "ducknng_transport.h"
#include <stddef.h>

/*
 * Server-side ducknng-frame-over-raw-WebSocket endpoint (issue #11).
 *
 * This is the browser-facing companion to the HTTP POST frame mount. It listens
 * with an nng raw-WebSocket (message-mode) stream listener that shares the same
 * underlying HTTP server as the RPC POST handler (nng refcounts servers per
 * address), so it truly sits *beside* the RPC mount at a sibling path. Each
 * accepted connection is persistent: a dedicated thread reads one binary WS
 * message as a ducknng frame, dispatches it through the identical
 * ducknng_service_authorize_and_dispatch_frame() gate the HTTP handler uses
 * (admission, accounting, authorization, execution subject, session binding all
 * reused), and replies with one binary WS message, then loops for the next.
 *
 * NON-goal: NNG SP-over-WebSocket. Native ws:// already speaks SP through an nng
 * socket; this is the ducknng-native frame carrier a browser JS WebSocket can
 * drive without reproducing SP framing.
 */

struct ducknng_service;
typedef struct ducknng_ws_frame_endpoint ducknng_ws_frame_endpoint;

/*
 * Start the endpoint. ws_url is a fully-resolved ws://host:port/path or
 * wss://host:port/path (its address must match the already-started HTTP
 * server's so nng shares that server). scheme is DUCKNNG_TRANSPORT_SCHEME_WS or
 * _WSS (recorded for the authorizer). recv_max bounds one message's bytes.
 *
 * On success *out_ep owns the running endpoint and, when out_resolved_ws_url is
 * non-NULL, *out_resolved_ws_url is a duckdb_malloc'd copy of ws_url (caller
 * frees). On failure returns -1 with *errmsg set (duckdb_malloc, caller frees).
 */
int ducknng_ws_frame_endpoint_start(struct ducknng_service *svc,
    const char *ws_url, ducknng_transport_scheme scheme, size_t recv_max,
    ducknng_ws_frame_endpoint **out_ep, char **out_resolved_ws_url, char **errmsg);

/* Stop and free: close the listener, then close, join and free every live
 * connection. Safe on NULL. */
void ducknng_ws_frame_endpoint_stop(ducknng_ws_frame_endpoint *ep);
