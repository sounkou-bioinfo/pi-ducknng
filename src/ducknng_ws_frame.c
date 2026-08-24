#include "ducknng_ws_frame.h"
#include "ducknng_nng_compat.h"
#include "ducknng_service.h"
#include "ducknng_thread.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/*
 * Raw-WebSocket ducknng-frame endpoint. See the header for the model. It rides
 * a thread-per-connection with blocking (callback-less) aios, matching the
 * synchronous style ducknng already uses for hijacked HTTP stream routes, which
 * keeps connection teardown simple: an aio is only ever freed by the thread
 * that owns it, never from inside its own completion callback.
 *
 * NNI_OPT_WS_MSGMODE ("ws:msgmode") puts the stream in message mode: recv
 * delivers one whole WS message as an nng_msg via nng_aio_get_msg, and send
 * ships one nng_msg as one WS message. In message mode the caller owns the
 * message across the operation, so both the received request and the sent reply
 * are freed here after the op completes.
 */

#define DUCKNNG_WS_MSGMODE_OPT "ws:msgmode"

typedef struct ducknng_ws_conn {
    ducknng_ws_frame_endpoint *ep;
    nng_stream *stream;
    ducknng_thread thread;
    int thread_started;
    int finished; /* set (under ep->mu) just before the thread returns */
    struct ducknng_ws_conn *next;
} ducknng_ws_conn;

struct ducknng_ws_frame_endpoint {
    struct ducknng_service *svc;
    nng_stream_listener *listener;
    nng_aio *accept_aio;
    ducknng_thread accept_thread;
    int accept_started;
    ducknng_transport_scheme scheme;
    char *path;
    size_t recv_max;
    ducknng_mutex mu;
    int mu_initialized;
    int stopping;
    ducknng_ws_conn *conns;
};

static int32_t ducknng_ws_status_to_code(uint16_t status) {
    switch (status) {
    case 400: return DUCKNNG_STATUS_INVALID;
    case 403: return DUCKNNG_STATUS_UNAUTHORIZED;
    case 503: return DUCKNNG_STATUS_BUSY;
    default: return DUCKNNG_STATUS_INTERNAL;
    }
}

/* Release one connection whose thread has already returned (or never started). */
static void ducknng_ws_conn_free(ducknng_ws_conn *conn) {
    if (!conn) return;
    if (conn->thread_started) ducknng_thread_join(conn->thread);
    if (conn->stream) {
        nng_stream_close(conn->stream);
        nng_stream_free(conn->stream);
    }
    duckdb_free(conn);
}

/* Unlink and free every connection that has finished. Called only by the accept
 * thread during normal operation, so it is the single reaper of naturally
 * closed connections; ducknng_thread_join here waits on a thread other than the
 * caller's. */
static void ducknng_ws_reap_finished(ducknng_ws_frame_endpoint *ep) {
    ducknng_ws_conn *dead = NULL;
    ducknng_ws_conn **pp;
    ducknng_ws_conn *cur;
    ducknng_mutex_lock(&ep->mu);
    pp = &ep->conns;
    while ((cur = *pp) != NULL) {
        if (cur->finished) {
            *pp = cur->next;
            cur->next = dead;
            dead = cur;
        } else {
            pp = &cur->next;
        }
    }
    ducknng_mutex_unlock(&ep->mu);
    while (dead) {
        ducknng_ws_conn *next = dead->next;
        ducknng_ws_conn_free(dead);
        dead = next;
    }
}

static void *ducknng_ws_conn_thread(void *arg) {
    ducknng_ws_conn *conn = (ducknng_ws_conn *)arg;
    ducknng_ws_frame_endpoint *ep = conn->ep;
    nng_aio *rio = NULL;
    nng_aio *sio = NULL;

    if (nng_aio_alloc(&rio, NULL, NULL) != 0 || nng_aio_alloc(&sio, NULL, NULL) != 0) {
        goto done;
    }
    for (;;) {
        nng_msg *msg = NULL;
        nng_msg *reply = NULL;
        nng_msg *sent = NULL;
        uint16_t status = 500;
        char *err_text = NULL;
        nng_sockaddr addr;
        int have_addr;
        int stop;

        ducknng_mutex_lock(&ep->mu);
        stop = ep->stopping;
        ducknng_mutex_unlock(&ep->mu);
        if (stop) break;

        nng_stream_recv(conn->stream, rio);
        nng_aio_wait(rio);
        if (nng_aio_result(rio) != 0) break; /* EOF, peer close, or error */
        msg = nng_aio_get_msg(rio);
        nng_aio_set_msg(rio, NULL);
        if (!msg) break;

        have_addr = nng_stream_get_addr(conn->stream, NNG_OPT_REMADDR, &addr) == 0;
        reply = ducknng_service_authorize_and_dispatch_frame(ep->svc,
            (const uint8_t *)nng_msg_body(msg), nng_msg_len(msg), NULL,
            have_addr ? &addr : NULL, ep->scheme, "GET", ep->path, NULL,
            &status, &err_text);
        nng_msg_free(msg);
        if (!reply) {
            /* Rejected before or during dispatch: reply with an error frame so
             * the WS client sees the reason instead of a bare connection drop,
             * then keep the connection open for the next request. */
            reply = ducknng_error_msg(NULL, ducknng_ws_status_to_code(status),
                err_text ? err_text : "ducknng: request failed");
            if (err_text) duckdb_free(err_text);
            if (!reply) break; /* out of memory: drop the connection */
        }

        nng_aio_set_msg(sio, reply);
        nng_stream_send(conn->stream, sio);
        nng_aio_wait(sio);
        /* Message mode leaves the message on the aio; the caller owns it. */
        sent = nng_aio_get_msg(sio);
        if (sent) {
            nng_msg_free(sent);
            nng_aio_set_msg(sio, NULL);
        }
        if (nng_aio_result(sio) != 0) break; /* send failed: drop the connection */
    }
done:
    if (rio) nng_aio_free(rio);
    if (sio) nng_aio_free(sio);
    if (conn->stream) nng_stream_close(conn->stream);
    ducknng_mutex_lock(&ep->mu);
    conn->finished = 1;
    ducknng_mutex_unlock(&ep->mu);
    return NULL;
}

static void *ducknng_ws_accept_thread(void *arg) {
    ducknng_ws_frame_endpoint *ep = (ducknng_ws_frame_endpoint *)arg;

    for (;;) {
        nng_stream *s;
        ducknng_ws_conn *conn;
        int stop;

        ducknng_mutex_lock(&ep->mu);
        stop = ep->stopping;
        ducknng_mutex_unlock(&ep->mu);
        if (stop) break;

        ducknng_ws_reap_finished(ep);
        nng_stream_listener_accept(ep->listener, ep->accept_aio);
        nng_aio_wait(ep->accept_aio);
        if (nng_aio_result(ep->accept_aio) != 0) break; /* listener closed or stopped */
        s = (nng_stream *)nng_aio_get_output(ep->accept_aio, 0);
        if (!s) continue;

        ducknng_mutex_lock(&ep->mu);
        stop = ep->stopping;
        ducknng_mutex_unlock(&ep->mu);
        if (stop) {
            nng_stream_close(s);
            nng_stream_free(s);
            break;
        }
        conn = (ducknng_ws_conn *)duckdb_malloc(sizeof(*conn));
        if (!conn) {
            nng_stream_close(s);
            nng_stream_free(s);
            continue;
        }
        memset(conn, 0, sizeof(*conn));
        conn->ep = ep;
        conn->stream = s;
        /* Publish before starting the worker so a concurrent stop sees it. */
        ducknng_mutex_lock(&ep->mu);
        conn->next = ep->conns;
        ep->conns = conn;
        ducknng_mutex_unlock(&ep->mu);
        if (ducknng_thread_create(&conn->thread, ducknng_ws_conn_thread, conn) != 0) {
            /* Could not start: unlink and drop it. */
            ducknng_ws_conn **pp;
            ducknng_mutex_lock(&ep->mu);
            pp = &ep->conns;
            while (*pp && *pp != conn) pp = &(*pp)->next;
            if (*pp == conn) *pp = conn->next;
            ducknng_mutex_unlock(&ep->mu);
            nng_stream_close(s);
            nng_stream_free(s);
            duckdb_free(conn);
            continue;
        }
        conn->thread_started = 1;
    }
    return NULL;
}

int ducknng_ws_frame_endpoint_start(struct ducknng_service *svc,
    const char *ws_url, ducknng_transport_scheme scheme, size_t recv_max,
    ducknng_ws_frame_endpoint **out_ep, char **out_resolved_ws_url, char **errmsg) {
    ducknng_ws_frame_endpoint *ep = NULL;
    nng_url *up = NULL;
    int rv;

    if (out_ep) *out_ep = NULL;
    if (out_resolved_ws_url) *out_resolved_ws_url = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc || !ws_url || !ws_url[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing WebSocket endpoint state");
        return -1;
    }
    ep = (ducknng_ws_frame_endpoint *)duckdb_malloc(sizeof(*ep));
    if (!ep) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating WebSocket endpoint");
        return -1;
    }
    memset(ep, 0, sizeof(*ep));
    ep->svc = svc;
    ep->scheme = scheme;
    ep->recv_max = recv_max;
    if (ducknng_mutex_init(&ep->mu) != 0) {
        duckdb_free(ep);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize WebSocket endpoint mutex");
        return -1;
    }
    ep->mu_initialized = 1;
    if (nng_url_parse(&up, ws_url) == 0 && up && up->u_path) {
        ep->path = ducknng_strdup(up->u_path);
    }
    if (up) nng_url_free(up);
    if (!ep->path) ep->path = ducknng_strdup("/");
    if (!ep->path) {
        rv = NNG_ENOMEM;
        goto fail;
    }
    rv = nng_stream_listener_alloc(&ep->listener, ws_url);
    if (rv != 0) goto fail;
    rv = nng_stream_listener_set_bool(ep->listener, DUCKNNG_WS_MSGMODE_OPT, true);
    if (rv != 0) goto fail;
    if (recv_max > 0) {
        rv = nng_stream_listener_set_size(ep->listener, NNG_OPT_RECVMAXSZ, recv_max);
        if (rv != 0) goto fail;
    }
    rv = nng_stream_listener_listen(ep->listener);
    if (rv != 0) goto fail;
    rv = nng_aio_alloc(&ep->accept_aio, NULL, NULL);
    if (rv != 0) goto fail;
    rv = ducknng_thread_create(&ep->accept_thread, ducknng_ws_accept_thread, ep);
    if (rv != 0) {
        rv = NNG_ENOMEM;
        goto fail;
    }
    ep->accept_started = 1;
    if (out_resolved_ws_url) {
        *out_resolved_ws_url = ducknng_strdup(ws_url);
        if (!*out_resolved_ws_url) {
            ducknng_ws_frame_endpoint_stop(ep);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying WebSocket URL");
            return -1;
        }
    }
    if (out_ep) *out_ep = ep;
    return 0;
fail:
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(rv != 0 ? ducknng_nng_strerror(rv) :
        "ducknng: failed to start WebSocket endpoint");
    if (ep->accept_aio) nng_aio_free(ep->accept_aio);
    if (ep->listener) nng_stream_listener_free(ep->listener);
    if (ep->path) duckdb_free(ep->path);
    if (ep->mu_initialized) ducknng_mutex_destroy(&ep->mu);
    duckdb_free(ep);
    return -1;
}

void ducknng_ws_frame_endpoint_stop(ducknng_ws_frame_endpoint *ep) {
    ducknng_ws_conn *conn;

    if (!ep) return;
    if (ep->mu_initialized) {
        ducknng_mutex_lock(&ep->mu);
        ep->stopping = 1;
        ducknng_mutex_unlock(&ep->mu);
    }
    /* The ws listener's close does not finish a queued accept aio, so stop it
     * explicitly to unblock the accept thread's nng_aio_wait; then join it
     * before touching the connection list so this thread becomes the list's
     * sole owner. */
    if (ep->listener) nng_stream_listener_close(ep->listener);
    if (ep->accept_aio) nng_aio_stop(ep->accept_aio);
    if (ep->accept_started) ducknng_thread_join(ep->accept_thread);

    if (ep->mu_initialized) ducknng_mutex_lock(&ep->mu);
    conn = ep->conns;
    ep->conns = NULL;
    if (ep->mu_initialized) ducknng_mutex_unlock(&ep->mu);
    while (conn) {
        ducknng_ws_conn *next = conn->next;
        /* Unblock a worker parked in recv/send, then join and free it. */
        if (conn->stream) nng_stream_close(conn->stream);
        ducknng_ws_conn_free(conn);
        conn = next;
    }
    if (ep->accept_aio) nng_aio_free(ep->accept_aio);
    if (ep->listener) nng_stream_listener_free(ep->listener);
    if (ep->path) duckdb_free(ep->path);
    if (ep->mu_initialized) ducknng_mutex_destroy(&ep->mu);
    duckdb_free(ep);
}
