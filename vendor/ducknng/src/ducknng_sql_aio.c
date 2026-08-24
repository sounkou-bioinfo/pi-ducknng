#include "ducknng_sql_shared.h"
#include "ducknng_http_compat.h"
#include "ducknng_ipc_out.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(__EMSCRIPTEN__)
#include <emscripten/threading.h>
#endif

DUCKDB_EXTENSION_EXTERN

typedef struct {
    uint64_t aio_id;
    bool ok;
    char *error;
    uint8_t *frame;
    idx_t frame_len;
    int has_nng_error;
    int32_t nng_error;
} ducknng_aio_collect_row;

typedef struct {
    ducknng_sql_context *ctx;
    uint64_t *aio_ids;
    idx_t aio_id_count;
    int32_t wait_ms;
    ducknng_aio_collect_row *rows;
    idx_t row_count;
    int materialized;
} ducknng_aio_collect_bind_data;

typedef struct {
    ducknng_aio_collect_bind_data *bind;
    idx_t offset;
} ducknng_aio_collect_init_data;

static char *ducknng_dup_bytes(const uint8_t *data, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, data, len);
    out[len] = '\0';
    return out;
}

static int execute_sql(duckdb_connection con, const char *sql) {
    duckdb_result result;
    memset(&result, 0, sizeof(result));
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return 0;
    }
    duckdb_destroy_result(&result);
    return 1;
}

static void ducknng_destroy_logical_types(duckdb_logical_type *types, idx_t count) {
    idx_t i;
    if (!types) return;
    for (i = 0; i < count; i++) duckdb_destroy_logical_type(&types[i]);
}

static int ducknng_register_struct_row_scalar_named(duckdb_connection con,
    ducknng_sql_context *ctx, const char *name, idx_t nparams, const duckdb_type *param_type_ids,
    duckdb_scalar_function_t fn, idx_t nfields, const duckdb_type *field_type_ids,
    const char **field_names) {
    duckdb_logical_type *param_types = NULL;
    duckdb_logical_type *fields = NULL;
    duckdb_logical_type return_type;
    idx_t i;
    int ok;
    if (!ctx || !ctx->rt || !name || !fn || (!param_type_ids && nparams > 0) ||
        (!field_type_ids && nfields > 0) || (!field_names && nfields > 0)) return 0;
    param_types = nparams ? (duckdb_logical_type *)duckdb_malloc(sizeof(*param_types) * nparams) : NULL;
    fields = nfields ? (duckdb_logical_type *)duckdb_malloc(sizeof(*fields) * nfields) : NULL;
    if ((nparams > 0 && !param_types) || (nfields > 0 && !fields)) {
        if (param_types) duckdb_free(param_types);
        if (fields) duckdb_free(fields);
        return 0;
    }
    for (i = 0; i < nparams; i++) param_types[i] = duckdb_create_logical_type(param_type_ids[i]);
    for (i = 0; i < nfields; i++) fields[i] = duckdb_create_logical_type(field_type_ids[i]);
    return_type = duckdb_create_struct_type(fields, field_names, nfields);
    ok = DUCKNNG_REGISTER_VOLATILE_SCALAR_LOGICAL_TYPES(con, name, nparams, fn, ctx,
        param_types, return_type);
    ducknng_destroy_logical_types(param_types, nparams);
    ducknng_destroy_logical_types(fields, nfields);
    if (param_types) duckdb_free(param_types);
    if (fields) duckdb_free(fields);
    duckdb_destroy_logical_type(&return_type);
    return ok;
}

static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (out_id) *out_id = 0;
    if (out_source) *out_source = NULL;
    if (out_opts) memset(out_opts, 0, sizeof(*out_opts));
    if (!ctx || !ctx->rt || tls_config_id == 0) return 0;
    ducknng_mutex_lock(&ctx->rt->mu);
    for (i = 0; i < ctx->rt->tls_config_count; i++) {
        if (ctx->rt->tls_configs[i] && ctx->rt->tls_configs[i]->tls_config_id == tls_config_id) {
            cfg = ctx->rt->tls_configs[i];
            break;
        }
    }
    if (!cfg) {
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: tls config not found");
        return -1;
    }
    if (out_id) *out_id = cfg->tls_config_id;
    if (out_source && cfg->source) {
        *out_source = ducknng_strdup(cfg->source);
        if (!*out_source) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS source");
            return -1;
        }
    }
    if (out_opts && ducknng_tls_opts_copy(out_opts, &cfg->opts) != 0) {
        if (out_source && *out_source) {
            duckdb_free(*out_source);
            *out_source = NULL;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS options");
        return -1;
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    return 0;
}

static int ducknng_lookup_tls_opts(ducknng_sql_context *ctx, uint64_t tls_config_id,
    const ducknng_tls_opts **out_opts, char **errmsg) {
    static _Thread_local ducknng_tls_opts tls_copy;
    static _Thread_local int tls_copy_valid = 0;
    if (out_opts) *out_opts = NULL;
    if (tls_copy_valid) {
        ducknng_tls_opts_reset(&tls_copy);
        tls_copy_valid = 0;
    }
    if (tls_config_id == 0) return 0;
    if (ducknng_lookup_tls_config_copy(ctx, tls_config_id, NULL, NULL, &tls_copy, errmsg) != 0) return -1;
    tls_copy_valid = 1;
    if (out_opts) *out_opts = &tls_copy;
    return 0;
}

static int ducknng_socket_is_active(const ducknng_client_socket *sock) {
    return sock && sock->open && (sock->connected || sock->has_listener);
}

static int ducknng_socket_is_req_protocol(const ducknng_client_socket *sock) {
    return sock && sock->protocol && strcmp(sock->protocol, "req") == 0;
}

static nng_msg *ducknng_client_manifest_request(void) {
    return ducknng_build_reply(DUCKNNG_RPC_MANIFEST, NULL, 0, NULL, NULL, 0);
}

static nng_msg *ducknng_client_exec_request(const char *sql, int want_result, char **errmsg) {
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    nng_msg *msg;
    if (ducknng_exec_request_to_ipc(sql, want_result, &payload, &payload_len, errmsg) != 0) return NULL;
    msg = ducknng_build_reply(DUCKNNG_RPC_CALL, "exec", 0, NULL, payload, (uint64_t)payload_len);
    duckdb_free(payload);
    if (!msg && errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: failed to allocate exec request message");
    return msg;
}

static int ducknng_wait_any_for_ids(ducknng_runtime *rt, const uint64_t *aio_ids, idx_t aio_id_count, int32_t wait_ms);

static void destroy_aio_collect_bind_data(void *ptr) {
    ducknng_aio_collect_bind_data *data = (ducknng_aio_collect_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    if (data->aio_ids) duckdb_free(data->aio_ids);
    if (data->rows) {
        for (i = 0; i < data->row_count; i++) {
            if (data->rows[i].error) duckdb_free(data->rows[i].error);
            if (data->rows[i].frame) duckdb_free(data->rows[i].frame);
        }
        duckdb_free(data->rows);
    }
    duckdb_free(data);
}

static void destroy_aio_collect_init_data(void *ptr) {
    ducknng_aio_collect_init_data *data = (ducknng_aio_collect_init_data *)ptr;
    if (data) duckdb_free(data);
}

#if defined(__EMSCRIPTEN__) && defined(DUCKNNG_WASM_TRACE) && DUCKNNG_WASM_TRACE
static void ducknng_wasm_trace(const char *where) {
    fprintf(stderr, "[ducknng wasm trace] %s\n", where ? where : "(null)");
    fflush(stderr);
}
#else
static void ducknng_wasm_trace(const char *where) {
    (void)where;
#if defined(__EMSCRIPTEN__)
    __asm__ __volatile__("" ::: "memory");
    fflush(stderr);
#if defined(DUCKNNG_WASM_INPROC_ONLY) && DUCKNNG_WASM_INPROC_ONLY
    emscripten_thread_sleep(1.0);
#endif
#endif
}
#endif

static int ducknng_client_open_req_socket_tls(const char *url, int timeout_ms, const ducknng_tls_opts *tls_opts, nng_socket *out, char **errmsg) {
    int rv;
    if (!url || !url[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: client URL is required");
        return -1;
    }
    ducknng_wasm_trace("open_req_socket_tls: validate begin");
    if (ducknng_socket_validate_client_url(url, tls_opts, errmsg) != 0) return -1;
    ducknng_wasm_trace("open_req_socket_tls: req open begin");
    rv = ducknng_req_socket_open(out);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    ducknng_wasm_trace("open_req_socket_tls: set timeout begin");
    rv = ducknng_socket_set_timeout_ms(*out, timeout_ms, timeout_ms);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    ducknng_wasm_trace("open_req_socket_tls: apply tls begin");
    rv = ducknng_socket_apply_tls(*out, url, tls_opts);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    /* Launch must not block on the first dial attempt. The request AIO itself
     * remains the one user-visible future, while NNG retries the initial
     * connection attempt in the background. */
    ducknng_wasm_trace("open_req_socket_tls: nonblocking dial begin");
    rv = ducknng_socket_dial_nonblocking(*out, url);
    ducknng_wasm_trace("open_req_socket_tls: nonblocking dial returned");
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    return 0;
}


static ducknng_client_aio *ducknng_find_client_aio_locked(ducknng_runtime *rt, uint64_t aio_id) {
    size_t i;
    if (!rt || aio_id == 0) return NULL;
    for (i = 0; i < rt->client_aio_count; i++) {
        if (rt->client_aios[i] && rt->client_aios[i]->aio_id == aio_id) return rt->client_aios[i];
    }
    return NULL;
}

static nng_msg *ducknng_client_raw_request_message(const uint8_t *payload, size_t payload_len, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return req;
}

static nng_msg *ducknng_client_method_request(const char *method_name, const void *payload,
    size_t payload_len, char **errmsg) {
    nng_msg *msg = ducknng_build_reply(DUCKNNG_RPC_CALL, method_name, 0, NULL,
        payload, (uint64_t)payload_len);
    if (!msg && errmsg && !*errmsg) {
        *errmsg = ducknng_strdup("ducknng: failed to allocate RPC request message");
    }
    return msg;
}

static void ducknng_client_aio_clear_http_handles(ducknng_client_aio *slot) {
    if (!slot) return;
    if (slot->http_res) {
        nng_http_res_free(slot->http_res);
        slot->http_res = NULL;
    }
    if (slot->http_req) {
        nng_http_req_free(slot->http_req);
        slot->http_req = NULL;
    }
    if (slot->http_client) {
        nng_http_client_free(slot->http_client);
        slot->http_client = NULL;
    }
    if (slot->http_url) {
        nng_url_free(slot->http_url);
        slot->http_url = NULL;
    }
}

static int ducknng_client_aio_copy_http_reply_msg(ducknng_client_aio *slot, char **errmsg) {
    uint16_t status = 0;
    uint8_t *body = NULL;
    size_t body_len = 0;
    nng_msg *reply_msg = NULL;
    int rv;
    if (errmsg) *errmsg = NULL;
    if (!slot || !slot->http_res) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP response state");
        return -1;
    }
    if (ducknng_http_response_copy(slot->http_res, &status, NULL, &body, &body_len, errmsg) != 0) return -1;
    slot->http_status = status;
    if (status != 200) {
        if (errmsg && !*errmsg) *errmsg = ducknng_http_status_error_message(status, body, body_len);
        if (body) duckdb_free(body);
        return -1;
    }
    if (!body && body_len > 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: HTTP transport returned an invalid empty body");
        return -1;
    }
    rv = nng_msg_alloc(&reply_msg, body_len);
    if (rv != 0) {
        if (body) duckdb_free(body);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    if (body_len > 0) memcpy(nng_msg_body(reply_msg), body, body_len);
    if (body) duckdb_free(body);
    if (slot->reply_msg) nng_msg_free(slot->reply_msg);
    slot->reply_msg = reply_msg;
    return 0;
}

static void ducknng_client_aio_cb(void *arg) {
    ducknng_client_aio *slot = (ducknng_client_aio *)arg;
    ducknng_runtime *rt;
    int rv;
    ducknng_wasm_trace("client_aio_cb: entered");
    if (!slot || !(rt = slot->rt) || !slot->aio) return;
    rv = ducknng_aio_result(slot->aio);
    ducknng_mutex_lock(&rt->mu);
    if (slot->state != DUCKNNG_CLIENT_AIO_PENDING) {
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_HTTP) {
        slot->send_done = 1;
        slot->recv_done = 1;
        slot->send_result = rv;
        slot->recv_result = rv;
    } else if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_SEND) {
        slot->send_done = 1;
        slot->send_result = rv;
    } else if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_RECV) {
        slot->recv_done = 1;
        slot->recv_result = rv;
    }
    if (rv != 0) {
        nng_msg *pending_msg = ducknng_aio_get_msg(slot->aio);
        if (pending_msg) {
            nng_msg_free(pending_msg);
            ducknng_aio_set_msg(slot->aio, NULL);
        }
        if (slot->error) duckdb_free(slot->error);
        slot->error = ducknng_strdup(ducknng_nng_strerror(rv));
        slot->state = rv == NNG_ECANCELED ? DUCKNNG_CLIENT_AIO_CANCELLED : DUCKNNG_CLIENT_AIO_ERROR;
        slot->finished_ms = ducknng_now_ms();
        if (slot->socket_ref) {
            ducknng_runtime_release_client_socket(slot->socket_ref);
            slot->socket_ref = NULL;
        }
        if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_HTTP) ducknng_client_aio_clear_http_handles(slot);
        ducknng_cond_broadcast(&rt->aio_cv);
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_HTTP) {
        if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_NCURL) {
            uint16_t status = 0;
            char *headers_json = NULL;
            uint8_t *body = NULL;
            size_t body_len = 0;
            char *errmsg = NULL;
            if (ducknng_http_response_copy(slot->http_res, &status, &headers_json, &body, &body_len, &errmsg) != 0) {
                if (slot->error) duckdb_free(slot->error);
                slot->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to copy HTTP response");
                slot->state = DUCKNNG_CLIENT_AIO_ERROR;
            } else {
                slot->http_status = status;
                slot->http_headers_json = headers_json;
                slot->http_body = body;
                slot->http_body_len = body_len;
                if (body && body_len > 0 && ducknng_sql_bytes_look_text(body, body_len)) {
                    slot->http_body_text = ducknng_dup_bytes(body, body_len);
                    if (!slot->http_body_text) {
                        slot->error = ducknng_strdup("ducknng: out of memory copying HTTP response text");
                        slot->state = DUCKNNG_CLIENT_AIO_ERROR;
                    } else {
                        slot->state = DUCKNNG_CLIENT_AIO_READY;
                    }
                } else {
                    slot->state = DUCKNNG_CLIENT_AIO_READY;
                }
            }
        } else if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_REQUEST) {
            char *errmsg = NULL;
            if (ducknng_client_aio_copy_http_reply_msg(slot, &errmsg) != 0) {
                if (slot->error) duckdb_free(slot->error);
                slot->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to copy HTTP frame response");
                slot->state = DUCKNNG_CLIENT_AIO_ERROR;
            } else {
                slot->state = DUCKNNG_CLIENT_AIO_READY;
            }
        } else {
            if (slot->error) duckdb_free(slot->error);
            slot->error = ducknng_strdup("ducknng: unsupported HTTP aio kind");
            slot->state = DUCKNNG_CLIENT_AIO_ERROR;
        }
        ducknng_client_aio_clear_http_handles(slot);
        slot->finished_ms = ducknng_now_ms();
        if (slot->socket_ref) {
            ducknng_runtime_release_client_socket(slot->socket_ref);
            slot->socket_ref = NULL;
        }
        ducknng_cond_broadcast(&rt->aio_cv);
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    if (slot->kind == DUCKNNG_CLIENT_AIO_KIND_REQUEST && slot->phase == DUCKNNG_CLIENT_AIO_PHASE_SEND) {
        slot->phase = DUCKNNG_CLIENT_AIO_PHASE_RECV;
        ducknng_mutex_unlock(&rt->mu);
        ducknng_ctx_recv_aio(slot->ctx, slot->aio);
        return;
    }
    if (slot->phase == DUCKNNG_CLIENT_AIO_PHASE_RECV) {
        slot->reply_msg = ducknng_aio_get_msg(slot->aio);
        ducknng_aio_set_msg(slot->aio, NULL);
    }
    slot->state = DUCKNNG_CLIENT_AIO_READY;
    slot->finished_ms = ducknng_now_ms();
    if (slot->socket_ref) {
        ducknng_runtime_release_client_socket(slot->socket_ref);
        slot->socket_ref = NULL;
    }
    ducknng_cond_broadcast(&rt->aio_cv);
    ducknng_mutex_unlock(&rt->mu);
}

static ducknng_client_aio *ducknng_client_aio_alloc_slot(ducknng_runtime *rt, int timeout_ms, char **errmsg) {
    ducknng_client_aio *slot;
    if (!rt) return NULL;
    slot = (ducknng_client_aio *)duckdb_malloc(sizeof(*slot));
    if (!slot) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating aio slot");
        return NULL;
    }
    memset(slot, 0, sizeof(*slot));
    slot->rt = rt;
    slot->timeout_ms = timeout_ms;
    slot->state = DUCKNNG_CLIENT_AIO_PENDING;
    slot->send_result = -1;
    slot->recv_result = -1;
    slot->started_ms = ducknng_now_ms();
    ducknng_wasm_trace("client_aio_alloc_slot: nng aio alloc begin");
    if (ducknng_aio_alloc(&slot->aio, ducknng_client_aio_cb, slot, timeout_ms) != 0) {
        ducknng_wasm_trace("client_aio_alloc_slot: nng aio alloc failed");
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: failed to allocate nng aio");
        duckdb_free(slot);
        return NULL;
    }
    ducknng_wasm_trace("client_aio_alloc_slot: nng aio alloc returned");
    return slot;
}

static int ducknng_client_add_terminal_error_aio(ducknng_sql_context *ctx, int kind, int phase,
    int timeout_ms, const char *message, uint64_t *out_aio_id, char **errmsg) {
    ducknng_client_aio *slot;
    if (out_aio_id) *out_aio_id = 0;
    if (!ctx || !ctx->rt) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing aio runtime");
        return -1;
    }
    slot = ducknng_client_aio_alloc_slot(ctx->rt, timeout_ms, errmsg);
    if (!slot) return -1;
    slot->kind = kind;
    slot->phase = phase;
    slot->state = DUCKNNG_CLIENT_AIO_ERROR;
    slot->finished_ms = ducknng_now_ms();
    slot->error = ducknng_strdup(message ? message : "ducknng: aio launch failed");
    if (!slot->error) {
        ducknng_client_aio_destroy(slot);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying aio error");
        return -1;
    }
    if (ducknng_runtime_add_client_aio(ctx->rt, slot, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    if (out_aio_id) *out_aio_id = slot->aio_id;
    return 0;
}

static int ducknng_set_terminal_error_aio_or_throw(duckdb_function_info info, ducknng_sql_context *ctx,
    duckdb_vector output, idx_t row, int kind, int phase, int timeout_ms, const char *message) {
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    char *errmsg = NULL;
    if (ducknng_client_add_terminal_error_aio(ctx, kind, phase, timeout_ms, message,
            &out[row], &errmsg) != 0) {
        duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to create terminal error aio");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    if (errmsg) duckdb_free(errmsg);
    return 0;
}

static ducknng_client_aio *ducknng_client_prepare_url_aio(ducknng_runtime *rt, const char *url,
    int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    ducknng_client_aio *slot = ducknng_client_aio_alloc_slot(rt, timeout_ms, errmsg);
    int rv;
    if (!slot) return NULL;
    ducknng_wasm_trace("prepare_url_aio: open req socket begin");
    if (ducknng_client_open_req_socket_tls(url, timeout_ms, tls_opts, &slot->sock, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return NULL;
    }
    ducknng_wasm_trace("prepare_url_aio: open req socket returned");
    slot->owns_socket = 1;
    slot->open = 1;
    ducknng_wasm_trace("prepare_url_aio: ctx open begin");
    rv = ducknng_ctx_open(&slot->ctx, slot->sock);
    ducknng_wasm_trace("prepare_url_aio: ctx open returned");
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_client_aio_destroy(slot);
        return NULL;
    }
    slot->has_ctx = 1;
    return slot;
}

static ducknng_client_aio *ducknng_client_prepare_socket_request_aio(ducknng_runtime *rt, uint64_t socket_id,
    int timeout_ms, char **errmsg) {
    ducknng_client_socket *sock;
    ducknng_client_aio *slot;
    int rv;
    if (!rt || socket_id == 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: connected req client socket is required for aio request");
        return NULL;
    }
    sock = ducknng_runtime_acquire_client_socket(rt, socket_id);
    if (!sock || !sock->open || !sock->connected || !ducknng_socket_is_req_protocol(sock)) {
        if (sock) ducknng_runtime_release_client_socket(sock);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: connected req client socket is required for aio request");
        return NULL;
    }
    slot = ducknng_client_aio_alloc_slot(rt, timeout_ms, errmsg);
    if (!slot) {
        ducknng_runtime_release_client_socket(sock);
        return NULL;
    }
    slot->socket_ref = sock;
    slot->sock = sock->sock;
    slot->socket_id = sock->socket_id;
    rv = ducknng_ctx_open(&slot->ctx, sock->sock);
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_client_aio_destroy(slot);
        return NULL;
    }
    slot->has_ctx = 1;
    return slot;
}

static ducknng_client_aio *ducknng_client_prepare_socket_raw_aio(ducknng_runtime *rt, uint64_t socket_id,
    int timeout_ms, char **errmsg) {
    ducknng_client_socket *sock;
    ducknng_client_aio *slot;
    if (!rt || socket_id == 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: active client socket is required for aio socket operation");
        return NULL;
    }
    ducknng_wasm_trace("prepare_socket_raw_aio: acquire socket begin");
    sock = ducknng_runtime_acquire_client_socket(rt, socket_id);
    ducknng_wasm_trace("prepare_socket_raw_aio: acquire socket returned");
    if (!ducknng_socket_is_active(sock)) {
        if (sock) ducknng_runtime_release_client_socket(sock);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: active client socket is required for aio socket operation");
        return NULL;
    }
    ducknng_wasm_trace("prepare_socket_raw_aio: aio slot alloc begin");
    slot = ducknng_client_aio_alloc_slot(rt, timeout_ms, errmsg);
    if (!slot) {
        ducknng_runtime_release_client_socket(sock);
        return NULL;
    }
    ducknng_wasm_trace("prepare_socket_raw_aio: aio slot alloc returned");
    slot->socket_ref = sock;
    slot->sock = sock->sock;
    slot->socket_id = sock->socket_id;
    return slot;
}

static int ducknng_client_launch_request_aio(ducknng_runtime *rt, ducknng_client_aio *slot, nng_msg *req, char **errmsg) {
    if (!rt || !slot || !req) {
        if (req) nng_msg_free(req);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing aio launch state");
        return -1;
    }
    ducknng_wasm_trace("launch_request_aio: add runtime aio begin");
    if (ducknng_runtime_add_client_aio(rt, slot, errmsg) != 0) {
        nng_msg_free(req);
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    ducknng_wasm_trace("launch_request_aio: add runtime aio returned");
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_REQUEST;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_SEND;
    ducknng_wasm_trace("launch_request_aio: set msg begin");
    ducknng_aio_set_msg(slot->aio, req);
    ducknng_wasm_trace("launch_request_aio: ctx send aio begin");
    ducknng_ctx_send_aio(slot->ctx, slot->aio);
    ducknng_wasm_trace("launch_request_aio: ctx send aio returned");
    return 0;
}

static int ducknng_client_launch_socket_send_aio(ducknng_runtime *rt, ducknng_client_aio *slot, nng_msg *msg, char **errmsg) {
    if (!rt || !slot || !msg) {
        if (msg) nng_msg_free(msg);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing aio send state");
        return -1;
    }
    ducknng_wasm_trace("launch_socket_send_aio: add runtime aio begin");
    if (ducknng_runtime_add_client_aio(rt, slot, errmsg) != 0) {
        nng_msg_free(msg);
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    ducknng_wasm_trace("launch_socket_send_aio: add runtime aio returned");
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_SEND;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_SEND;
    ducknng_wasm_trace("launch_socket_send_aio: set msg begin");
    ducknng_aio_set_msg(slot->aio, msg);
    ducknng_wasm_trace("launch_socket_send_aio: socket send aio begin");
    ducknng_socket_send_aio(slot->sock, slot->aio);
    ducknng_wasm_trace("launch_socket_send_aio: socket send aio returned");
    return 0;
}

static int ducknng_client_launch_socket_recv_aio(ducknng_runtime *rt, ducknng_client_aio *slot, char **errmsg) {
    if (!rt || !slot) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing aio recv state");
        return -1;
    }
    ducknng_wasm_trace("launch_socket_recv_aio: add runtime aio begin");
    if (ducknng_runtime_add_client_aio(rt, slot, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    ducknng_wasm_trace("launch_socket_recv_aio: add runtime aio returned");
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_RECV;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_RECV;
    ducknng_wasm_trace("launch_socket_recv_aio: socket recv aio begin");
    ducknng_socket_recv_aio(slot->sock, slot->aio);
    ducknng_wasm_trace("launch_socket_recv_aio: socket recv aio returned");
    return 0;
}

static int ducknng_client_launch_url_request_aio(ducknng_sql_context *ctx, const char *url,
    int32_t timeout_ms, uint64_t tls_config_id, nng_msg *req, uint64_t *out_aio_id, char **errmsg) {
    const ducknng_tls_opts *tls_opts = NULL;
    ducknng_transport_url parsed;
    ducknng_client_aio *slot = NULL;
    if (out_aio_id) *out_aio_id = 0;
    ducknng_wasm_trace("launch_url_request_aio: begin");
    if (!ctx || !ctx->rt || !url || !req) {
        if (req) nng_msg_free(req);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing async RPC request state");
        return -1;
    }
    ducknng_wasm_trace("launch_url_request_aio: parse url begin");
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) {
        nng_msg_free(req);
        return -1;
    }
    ducknng_wasm_trace("launch_url_request_aio: lookup tls begin");
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, errmsg) != 0) {
        nng_msg_free(req);
        return -1;
    }
    ducknng_wasm_trace("launch_url_request_aio: transport dispatch begin");
    if (ducknng_transport_url_is_http(&parsed)) {
        slot = ducknng_client_aio_alloc_slot(ctx->rt, timeout_ms, errmsg);
        if (!slot) {
            nng_msg_free(req);
            return -1;
        }
        slot->kind = DUCKNNG_CLIENT_AIO_KIND_REQUEST;
        slot->phase = DUCKNNG_CLIENT_AIO_PHASE_HTTP;
        if (ducknng_http_frame_transact_aio_prepare(url, (const uint8_t *)nng_msg_body(req), nng_msg_len(req),
                tls_opts, &slot->http_url, &slot->http_client, &slot->http_req, &slot->http_res, errmsg) != 0) {
            nng_msg_free(req);
            ducknng_client_aio_destroy(slot);
            return -1;
        }
        nng_msg_free(req);
        if (ducknng_runtime_add_client_aio(ctx->rt, slot, errmsg) != 0) {
            ducknng_client_aio_destroy(slot);
            return -1;
        }
        nng_http_client_transact(slot->http_client, slot->http_req, slot->http_res, slot->aio);
        if (out_aio_id) *out_aio_id = slot->aio_id;
        return 0;
    }
    slot = ducknng_client_prepare_url_aio(ctx->rt, url, timeout_ms, tls_opts, errmsg);
    if (!slot) {
        nng_msg_free(req);
        return -1;
    }
    if (ducknng_client_launch_request_aio(ctx->rt, slot, req, errmsg) != 0) {
        return -1;
    }
    if (out_aio_id) *out_aio_id = slot->aio_id;
    return 0;
}

static int ducknng_client_launch_url_method_aio(ducknng_sql_context *ctx, const char *url,
    const char *method_name, const uint8_t *payload, size_t payload_len, int32_t timeout_ms,
    uint64_t tls_config_id, uint64_t *out_aio_id, char **errmsg) {
    nng_msg *req = ducknng_client_method_request(method_name, payload, payload_len, errmsg);
    if (!req) return -1;
    return ducknng_client_launch_url_request_aio(ctx, url, timeout_ms, tls_config_id,
        req, out_aio_id, errmsg);
}

static int ducknng_client_launch_ncurl_aio(ducknng_sql_context *ctx, const char *url,
    const char *method, const char *headers_json, const uint8_t *body, size_t body_len,
    int32_t timeout_ms, uint64_t tls_config_id, uint64_t *out_aio_id, char **errmsg) {
    const ducknng_tls_opts *tls_opts = NULL;
    ducknng_client_aio *slot = NULL;
    if (out_aio_id) *out_aio_id = 0;
    if (!ctx || !ctx->rt || !url || !url[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: ncurl_aio URL must not be NULL or empty");
        return -1;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, errmsg) != 0) return -1;
    slot = ducknng_client_aio_alloc_slot(ctx->rt, timeout_ms, errmsg);
    if (!slot) return -1;
    slot->kind = DUCKNNG_CLIENT_AIO_KIND_NCURL;
    slot->phase = DUCKNNG_CLIENT_AIO_PHASE_HTTP;
    if (ducknng_http_transact_aio_prepare(url, method, headers_json, body, body_len, tls_opts,
            &slot->http_url, &slot->http_client, &slot->http_req, &slot->http_res, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    if (ducknng_runtime_add_client_aio(ctx->rt, slot, errmsg) != 0) {
        ducknng_client_aio_destroy(slot);
        return -1;
    }
    nng_http_client_transact(slot->http_client, slot->http_req, slot->http_res, slot->aio);
    if (out_aio_id) *out_aio_id = slot->aio_id;
    return 0;
}


static void ducknng_request_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_wasm_trace("request_raw_aio: function entered");
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        ducknng_wasm_trace("request_raw_aio: row begin");
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        nng_msg *req = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || !url || (!payload && payload_len > 0)) {
            if (url) duckdb_free(url);
            if (payload) duckdb_free(payload);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: request_raw_aio requires url and payload") != 0) return;
            continue;
        }
        ducknng_wasm_trace("request_raw_aio: raw message build begin");
        req = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        ducknng_wasm_trace("request_raw_aio: raw message build returned");
        if (payload) duckdb_free(payload);
        payload = NULL;
        if (!req) {
            duckdb_free(url);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to build request frame") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        ducknng_wasm_trace("request_raw_aio: launch url aio begin");
        if (ducknng_client_launch_url_request_aio(ctx, url, timeout_ms, tls_config_id, req, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        ducknng_wasm_trace("request_raw_aio: launch url aio returned");
        duckdb_free(url);
    }
}

static void ducknng_get_rpc_manifest_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        nng_msg *req = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || !url) {
            if (url) duckdb_free(url);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: get_rpc_manifest_raw_aio requires url") != 0) return;
            continue;
        }
        req = ducknng_client_manifest_request();
        if (!req) {
            duckdb_free(url);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: failed to build manifest request frame") != 0) return;
            continue;
        }
        if (ducknng_client_launch_url_request_aio(ctx, url, timeout_ms, tls_config_id, req, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch manifest aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        duckdb_free(url);
    }
}

static void ducknng_run_rpc_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        nng_msg *req = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || !url || !sql) {
            if (url) duckdb_free(url);
            if (sql) duckdb_free(sql);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: run_rpc_raw_aio requires url and sql") != 0) return;
            continue;
        }
        req = ducknng_client_exec_request(sql, 0, &errmsg);
        duckdb_free(sql);
        if (!req) {
            duckdb_free(url);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to build exec request frame") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        if (ducknng_client_launch_url_request_aio(ctx, url, timeout_ms, tls_config_id, req, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch exec aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        duckdb_free(url);
    }
}

static void ducknng_open_query_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        uint64_t batch_rows = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        uint64_t batch_bytes = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 4), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0);
        uint8_t *payload = NULL;
        size_t payload_len = 0;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || !url || !sql || !url[0] || !sql[0]) {
            if (url) duckdb_free(url);
            if (sql) duckdb_free(sql);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: open_query_raw_aio requires url and sql") != 0) return;
            continue;
        }
        if (ducknng_query_open_request_to_ipc(sql, batch_rows, batch_bytes, &payload, &payload_len, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_free(sql);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to encode query_open request payload") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        if (ducknng_client_launch_url_method_aio(ctx, url, "query_open", payload, payload_len,
                timeout_ms, tls_config_id, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            duckdb_free(sql);
            duckdb_free(payload);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch query_open aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        duckdb_free(url);
        duckdb_free(sql);
        duckdb_free(payload);
    }
}

static void ducknng_fetch_query_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t session_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        char *session_token = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        uint64_t batch_rows = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        uint64_t batch_bytes = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 5), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 6), row, 0);
        char *payload = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || !url || !url[0] || session_id == 0 || !session_token || !session_token[0]) {
            if (url) duckdb_free(url);
            if (session_token) duckdb_free(session_token);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: fetch_query_raw_aio requires url, session_id, and session_token") != 0) return;
            continue;
        }
        payload = ducknng_session_request_json(session_id, session_token, batch_rows, batch_bytes);
        if (!payload) {
            duckdb_free(url);
            duckdb_free(session_token);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: failed to build fetch request payload") != 0) return;
            continue;
        }
        if (ducknng_client_launch_url_method_aio(ctx, url, "fetch", (const uint8_t *)payload, strlen(payload),
                timeout_ms, tls_config_id, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            duckdb_free(session_token);
            duckdb_free(payload);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch fetch aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        duckdb_free(url);
        duckdb_free(session_token);
        duckdb_free(payload);
    }
}

static void ducknng_close_query_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t session_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        char *session_token = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 3), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0);
        char *payload = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || !url || !url[0] || session_id == 0 || !session_token || !session_token[0]) {
            if (url) duckdb_free(url);
            if (session_token) duckdb_free(session_token);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: close_query_raw_aio requires url, session_id, and session_token") != 0) return;
            continue;
        }
        payload = ducknng_session_request_json(session_id, session_token, 0, 0);
        if (!payload) {
            duckdb_free(url);
            duckdb_free(session_token);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: failed to build close request payload") != 0) return;
            continue;
        }
        if (ducknng_client_launch_url_method_aio(ctx, url, "close", (const uint8_t *)payload, strlen(payload),
                timeout_ms, tls_config_id, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            duckdb_free(session_token);
            duckdb_free(payload);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch close aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        duckdb_free(url);
        duckdb_free(session_token);
        duckdb_free(payload);
    }
}

static void ducknng_cancel_query_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t session_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        char *session_token = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 3), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0);
        char *payload = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || !url || !url[0] || session_id == 0 || !session_token || !session_token[0]) {
            if (url) duckdb_free(url);
            if (session_token) duckdb_free(session_token);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: cancel_query_raw_aio requires url, session_id, and session_token") != 0) return;
            continue;
        }
        payload = ducknng_session_request_json(session_id, session_token, 0, 0);
        if (!payload) {
            duckdb_free(url);
            duckdb_free(session_token);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: failed to build cancel request payload") != 0) return;
            continue;
        }
        if (ducknng_client_launch_url_method_aio(ctx, url, "cancel", (const uint8_t *)payload, strlen(payload),
                timeout_ms, tls_config_id, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            duckdb_free(session_token);
            duckdb_free(payload);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch cancel aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        duckdb_free(url);
        duckdb_free(session_token);
        duckdb_free(payload);
    }
}

static void ducknng_ncurl_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *headers_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *profile_id = NULL;
        char *effective_headers_json = NULL;
        idx_t body_len = 0;
        uint8_t *body = arg_blob_dup(duckdb_data_chunk_get_vector(input, 3), row, &body_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 4), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0);
        char *errmsg = NULL;
        out[row] = 0;
        if (ncols > 6 && !arg_is_null(duckdb_data_chunk_get_vector(input, 6), row)) {
            profile_id = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 6), row);
        }
        if (!ctx || !ctx->rt || !url || (!body && body_len > 0)) {
            if (url) duckdb_free(url);
            if (method) duckdb_free(method);
            if (headers_json) duckdb_free(headers_json);
            if (profile_id) duckdb_free(profile_id);
            if (body) duckdb_free(body);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_NCURL, DUCKNNG_CLIENT_AIO_PHASE_HTTP,
                    timeout_ms, "ducknng: ncurl_aio requires url") != 0) return;
            continue;
        }
        if (profile_id && profile_id[0] && ducknng_runtime_resolve_http_profile_headers(ctx->rt,
                profile_id, url, method, headers_json, &effective_headers_json, &errmsg) != 0) {
            duckdb_free(url);
            if (method) duckdb_free(method);
            if (headers_json) duckdb_free(headers_json);
            if (profile_id) duckdb_free(profile_id);
            if (body) duckdb_free(body);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_NCURL, DUCKNNG_CLIENT_AIO_PHASE_HTTP,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to resolve HTTP profile") != 0) {
                if (effective_headers_json) duckdb_free(effective_headers_json);
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (effective_headers_json) duckdb_free(effective_headers_json);
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        if (ducknng_client_launch_ncurl_aio(ctx, url, method,
                effective_headers_json ? effective_headers_json : headers_json,
                body, (size_t)body_len, timeout_ms, tls_config_id, &out[row], &errmsg) != 0) {
            duckdb_free(url);
            if (method) duckdb_free(method);
            if (headers_json) duckdb_free(headers_json);
            if (profile_id) duckdb_free(profile_id);
            if (effective_headers_json) duckdb_free(effective_headers_json);
            if (body) duckdb_free(body);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_NCURL, DUCKNNG_CLIENT_AIO_PHASE_HTTP,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch ncurl aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        duckdb_free(url);
        if (method) duckdb_free(method);
        if (headers_json) duckdb_free(headers_json);
        if (profile_id) duckdb_free(profile_id);
        if (effective_headers_json) duckdb_free(effective_headers_json);
        if (body) duckdb_free(body);
    }
}

static void ducknng_request_socket_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_aio *slot = NULL;
        nng_msg *req = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: request_socket_raw_aio requires socket id and payload") != 0) return;
            continue;
        }
        req = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (payload) duckdb_free(payload);
        payload = NULL;
        if (!req) {
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to build request frame") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        slot = ducknng_client_prepare_socket_request_aio(ctx->rt, socket_id, timeout_ms, &errmsg);
        if (!slot) {
            nng_msg_free(req);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to prepare socket aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        if (ducknng_client_launch_request_aio(ctx->rt, slot, req, &errmsg) != 0) {
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_REQUEST, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch socket aio request") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        out[row] = slot->aio_id;
    }
}

static void ducknng_send_socket_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_aio *slot = NULL;
        nng_msg *msg = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_SEND, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, "ducknng: send_socket_raw_aio requires socket id and payload") != 0) return;
            continue;
        }
        msg = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (payload) duckdb_free(payload);
        if (!msg) {
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_SEND, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to build send message") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        slot = ducknng_client_prepare_socket_raw_aio(ctx->rt, socket_id, timeout_ms, &errmsg);
        if (!slot) {
            nng_msg_free(msg);
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_SEND, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to prepare socket send aio") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        if (ducknng_client_launch_socket_send_aio(ctx->rt, slot, msg, &errmsg) != 0) {
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_SEND, DUCKNNG_CLIENT_AIO_PHASE_SEND,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch socket send aio") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        out[row] = slot->aio_id;
    }
}

static void ducknng_recv_socket_raw_aio_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        ducknng_client_aio *slot = NULL;
        char *errmsg = NULL;
        out[row] = 0;
        if (!ctx || !ctx->rt || socket_id == 0) {
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_RECV, DUCKNNG_CLIENT_AIO_PHASE_RECV,
                    timeout_ms, "ducknng: recv_socket_raw_aio requires socket id") != 0) return;
            continue;
        }
        slot = ducknng_client_prepare_socket_raw_aio(ctx->rt, socket_id, timeout_ms, &errmsg);
        if (!slot) {
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_RECV, DUCKNNG_CLIENT_AIO_PHASE_RECV,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to prepare socket recv aio") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        if (ducknng_client_launch_socket_recv_aio(ctx->rt, slot, &errmsg) != 0) {
            if (ducknng_set_terminal_error_aio_or_throw(info, ctx, output, row,
                    DUCKNNG_CLIENT_AIO_KIND_RECV, DUCKNNG_CLIENT_AIO_PHASE_RECV,
                    timeout_ms, errmsg ? errmsg : "ducknng: failed to launch socket recv aio") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        out[row] = slot->aio_id;
    }
}

static void ducknng_aio_ready_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->state != DUCKNNG_CLIENT_AIO_PENDING) out[row] = true;
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_aio_cancel_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->state == DUCKNNG_CLIENT_AIO_PENDING && slot->aio) {
            ducknng_aio_cancel(slot->aio);
            out[row] = true;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_aio_drop_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot = NULL;
        int droppable = 0;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        droppable = slot && slot->state != DUCKNNG_CLIENT_AIO_PENDING;
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (!droppable) continue;
        slot = ducknng_runtime_remove_client_aio(ctx->rt, aio_id);
        if (!slot) continue;
        ducknng_client_aio_destroy(slot);
        out[row] = true;
    }
}

static const char *ducknng_aio_kind_name(int kind) {
    switch (kind) {
        case DUCKNNG_CLIENT_AIO_KIND_REQUEST: return "request";
        case DUCKNNG_CLIENT_AIO_KIND_SEND: return "send";
        case DUCKNNG_CLIENT_AIO_KIND_RECV: return "recv";
        case DUCKNNG_CLIENT_AIO_KIND_NCURL: return "ncurl";
        default: return NULL;
    }
}

static const char *ducknng_aio_state_name(int state) {
    switch (state) {
        case DUCKNNG_CLIENT_AIO_PENDING: return "pending";
        case DUCKNNG_CLIENT_AIO_READY: return "ready";
        case DUCKNNG_CLIENT_AIO_ERROR: return "error";
        case DUCKNNG_CLIENT_AIO_CANCELLED: return "cancelled";
        case DUCKNNG_CLIENT_AIO_COLLECTED: return "collected";
        default: return NULL;
    }
}

static const char *ducknng_aio_phase_name(int phase) {
    switch (phase) {
        case DUCKNNG_CLIENT_AIO_PHASE_SEND: return "send";
        case DUCKNNG_CLIENT_AIO_PHASE_RECV: return "recv";
        case DUCKNNG_CLIENT_AIO_PHASE_HTTP: return "http";
        default: return NULL;
    }
}

static void ducknng_aio_status_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_id_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector child_vecs[14];
    uint64_t *out_aio_id;
    bool *out_exists;
    bool *out_terminal;
    bool *out_send_done;
    bool *out_recv_done;
    bool *out_has_reply_frame;
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (int i = 0; i < 14; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    out_aio_id = (uint64_t *)duckdb_vector_get_data(child_vecs[0]);
    out_exists = (bool *)duckdb_vector_get_data(child_vecs[1]);
    out_terminal = (bool *)duckdb_vector_get_data(child_vecs[5]);
    out_send_done = (bool *)duckdb_vector_get_data(child_vecs[6]);
    out_recv_done = (bool *)duckdb_vector_get_data(child_vecs[8]);
    out_has_reply_frame = (bool *)duckdb_vector_get_data(child_vecs[10]);
    for (idx_t row = 0; row < row_count; row++) {
        uint64_t aio_id = arg_u64(aio_id_vec, row, 0);
        ducknng_client_aio snapshot;
        char *error_copy = NULL;
        const char *kind_name = NULL;
        const char *state_name = NULL;
        const char *phase_name = NULL;
        int found = 0;
        memset(&snapshot, 0, sizeof(snapshot));
        out_aio_id[row] = aio_id;
        out_exists[row] = false;
        out_terminal[row] = false;
        out_send_done[row] = false;
        out_recv_done[row] = false;
        out_has_reply_frame[row] = false;
        if (!ctx || !ctx->rt || arg_is_null(aio_id_vec, row) || aio_id == 0) {
            set_null(child_vecs[2], row);
            set_null(child_vecs[3], row);
            set_null(child_vecs[4], row);
            set_null(child_vecs[7], row);
            set_null(child_vecs[9], row);
            set_null(child_vecs[11], row);
            set_null(child_vecs[12], row);
            set_null(child_vecs[13], row);
            continue;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        {
            ducknng_client_aio *slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
            if (slot) {
                snapshot = *slot;
                if (slot->error) error_copy = ducknng_strdup(slot->error);
                found = 1;
            }
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (!found) {
            set_null(child_vecs[2], row);
            set_null(child_vecs[3], row);
            set_null(child_vecs[4], row);
            set_null(child_vecs[7], row);
            set_null(child_vecs[9], row);
            set_null(child_vecs[11], row);
            set_null(child_vecs[12], row);
            set_null(child_vecs[13], row);
            continue;
        }
        out_exists[row] = true;
        out_terminal[row] = snapshot.state != DUCKNNG_CLIENT_AIO_PENDING;
        out_send_done[row] = snapshot.send_done != 0;
        out_recv_done[row] = snapshot.recv_done != 0;
        out_has_reply_frame[row] = snapshot.reply_msg != NULL;
        kind_name = ducknng_aio_kind_name(snapshot.kind);
        state_name = ducknng_aio_state_name(snapshot.state);
        phase_name = ducknng_aio_phase_name(snapshot.phase);
        if (kind_name) duckdb_unsafe_vector_assign_string_element_len(child_vecs[2], row, kind_name, (idx_t)strlen(kind_name)); else set_null(child_vecs[2], row);
        if (state_name) duckdb_unsafe_vector_assign_string_element_len(child_vecs[3], row, state_name, (idx_t)strlen(state_name)); else set_null(child_vecs[3], row);
        if (phase_name) duckdb_unsafe_vector_assign_string_element_len(child_vecs[4], row, phase_name, (idx_t)strlen(phase_name)); else set_null(child_vecs[4], row);
        if (snapshot.send_done) {
            bool *send_ok = (bool *)duckdb_vector_get_data(child_vecs[7]);
            send_ok[row] = snapshot.send_result == 0;
        } else {
            set_null(child_vecs[7], row);
        }
        if (snapshot.recv_done) {
            bool *recv_ok = (bool *)duckdb_vector_get_data(child_vecs[9]);
            recv_ok[row] = snapshot.recv_result == 0;
        } else {
            set_null(child_vecs[9], row);
        }
        if (error_copy) duckdb_unsafe_vector_assign_string_element_len(child_vecs[11], row, error_copy, (idx_t)strlen(error_copy));
        else set_null(child_vecs[11], row);
        if (error_copy) duckdb_free(error_copy);
        {
            int32_t nng_err = 0;
            if (snapshot.send_result != 0 && snapshot.send_result != -1)
                nng_err = (int32_t)snapshot.send_result;
            else if (snapshot.recv_result != 0 && snapshot.recv_result != -1)
                nng_err = (int32_t)snapshot.recv_result;
            if (nng_err != 0) {
                int32_t *nng_errors = (int32_t *)duckdb_vector_get_data(child_vecs[12]);
                nng_errors[row] = nng_err;
                duckdb_unsafe_vector_assign_string_element_len(child_vecs[13], row, ducknng_nng_strerror(nng_err), (idx_t)strlen(ducknng_nng_strerror(nng_err)));
            } else {
                set_null(child_vecs[12], row);
                set_null(child_vecs[13], row);
            }
        }
    }
}

static void ducknng_aio_wait_any_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_ids_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector wait_vec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector child_vec = duckdb_list_vector_get_child(aio_ids_vec);
    duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(aio_ids_vec);
    uint64_t *child_ids = (uint64_t *)duckdb_vector_get_data(child_vec);
    bool *out = (bool *)duckdb_vector_get_data(output);
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (idx_t row = 0; row < row_count; row++) {
        duckdb_list_entry entry;
        uint64_t *ids = NULL;
        idx_t count = 0;
        idx_t write_idx = 0;
        out[row] = false;
        if (!ctx || !ctx->rt || arg_is_null(aio_ids_vec, row) || arg_is_null(wait_vec, row)) continue;
        entry = entries[row];
        if (entry.length == 0) continue;
        ids = (uint64_t *)duckdb_malloc(sizeof(*ids) * (size_t)entry.length);
        if (!ids) continue;
        for (idx_t i = 0; i < (idx_t)entry.length; i++) {
            idx_t child_index = (idx_t)entry.offset + i;
            if (arg_is_null(child_vec, child_index)) continue;
            ids[write_idx++] = child_ids[child_index];
        }
        count = write_idx;
        if (count > 0) out[row] = ducknng_wait_any_for_ids(ctx->rt, ids, count, arg_int32(wait_vec, row, 0)) != 0;
        duckdb_free(ids);
    }
}

static void ducknng_aio_collect_row_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_id_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector wait_vec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector child_vecs[6];
    uint64_t *out_aio_id;
    bool *out_ok;
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (int i = 0; i < 6; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    out_aio_id = (uint64_t *)duckdb_vector_get_data(child_vecs[0]);
    out_ok = (bool *)duckdb_vector_get_data(child_vecs[1]);
    for (idx_t row = 0; row < row_count; row++) {
        uint64_t aio_id;
        int32_t wait_ms;
        ducknng_client_aio *slot;
        if (!ctx || !ctx->rt || arg_is_null(aio_id_vec, row) || arg_is_null(wait_vec, row)) {
            set_null(output, row);
            for (int i = 0; i < 6; i++) set_null(child_vecs[i], row);
            continue;
        }
        aio_id = arg_u64(aio_id_vec, row, 0);
        wait_ms = arg_int32(wait_vec, row, 0);
        if (aio_id == 0 || wait_ms < 0) {
            set_null(output, row);
            for (int i = 0; i < 6; i++) set_null(child_vecs[i], row);
            continue;
        }
        if (!ducknng_wait_any_for_ids(ctx->rt, &aio_id, 1, wait_ms) && wait_ms > 0) {
            set_null(output, row);
            for (int i = 0; i < 6; i++) set_null(child_vecs[i], row);
            continue;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (!slot || !(slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED || slot->state == DUCKNNG_CLIENT_AIO_COLLECTED)) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            set_null(output, row);
            for (int i = 0; i < 6; i++) set_null(child_vecs[i], row);
            continue;
        }
        out_aio_id[row] = slot->aio_id;
        out_ok[row] = slot->state == DUCKNNG_CLIENT_AIO_READY ||
            (slot->state == DUCKNNG_CLIENT_AIO_COLLECTED && !slot->error);
        if (slot->error) duckdb_unsafe_vector_assign_string_element_len(child_vecs[2], row, slot->error, (idx_t)strlen(slot->error));
        else set_null(child_vecs[2], row);
        if ((slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_COLLECTED) && slot->reply_msg) {
            assign_blob(child_vecs[3], row, (const uint8_t *)nng_msg_body(slot->reply_msg), (idx_t)nng_msg_len(slot->reply_msg));
            nng_msg_free(slot->reply_msg);
            slot->reply_msg = NULL;
        } else {
            set_null(child_vecs[3], row);
        }
        {
            int32_t nng_err = 0;
            if (slot->send_result != 0 && slot->send_result != -1)
                nng_err = (int32_t)slot->send_result;
            else if (slot->recv_result != 0 && slot->recv_result != -1)
                nng_err = (int32_t)slot->recv_result;
            if (nng_err != 0) {
                int32_t *nng_errors = (int32_t *)duckdb_vector_get_data(child_vecs[4]);
                nng_errors[row] = nng_err;
                duckdb_unsafe_vector_assign_string_element_len(child_vecs[5], row, ducknng_nng_strerror(nng_err), (idx_t)strlen(ducknng_nng_strerror(nng_err)));
            } else {
                set_null(child_vecs[4], row);
                set_null(child_vecs[5], row);
            }
        }
        if (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED) {
            slot->state = DUCKNNG_CLIENT_AIO_COLLECTED;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_ncurl_aio_collect_row_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector aio_id_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector wait_vec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector child_vecs[7];
    uint64_t *out_aio_id;
    bool *out_ok;
    int32_t *out_status;
    idx_t row_count = duckdb_data_chunk_get_size(input);
    for (int i = 0; i < 7; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    out_aio_id = (uint64_t *)duckdb_vector_get_data(child_vecs[0]);
    out_ok = (bool *)duckdb_vector_get_data(child_vecs[1]);
    out_status = (int32_t *)duckdb_vector_get_data(child_vecs[2]);
    for (idx_t row = 0; row < row_count; row++) {
        uint64_t aio_id;
        int32_t wait_ms;
        ducknng_client_aio *slot;
        if (!ctx || !ctx->rt || arg_is_null(aio_id_vec, row) || arg_is_null(wait_vec, row)) {
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        aio_id = arg_u64(aio_id_vec, row, 0);
        wait_ms = arg_int32(wait_vec, row, 0);
        if (aio_id == 0 || wait_ms < 0) {
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        if (!ducknng_wait_any_for_ids(ctx->rt, &aio_id, 1, wait_ms) && wait_ms > 0) {
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (!slot || slot->kind != DUCKNNG_CLIENT_AIO_KIND_NCURL ||
                !(slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED || slot->state == DUCKNNG_CLIENT_AIO_COLLECTED)) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            set_null(output, row);
            for (int i = 0; i < 7; i++) set_null(child_vecs[i], row);
            continue;
        }
        out_aio_id[row] = slot->aio_id;
        out_ok[row] = slot->state == DUCKNNG_CLIENT_AIO_READY ||
            (slot->state == DUCKNNG_CLIENT_AIO_COLLECTED && !slot->error);
        if (out_ok[row]) out_status[row] = (int32_t)slot->http_status; else set_null(child_vecs[2], row);
        if (slot->error) duckdb_unsafe_vector_assign_string_element_len(child_vecs[3], row, slot->error, (idx_t)strlen(slot->error));
        else set_null(child_vecs[3], row);
        if (slot->http_headers_json) duckdb_unsafe_vector_assign_string_element_len(child_vecs[4], row, slot->http_headers_json, (idx_t)strlen(slot->http_headers_json));
        else set_null(child_vecs[4], row);
        if (slot->http_body) assign_blob(child_vecs[5], row, slot->http_body, (idx_t)slot->http_body_len);
        else set_null(child_vecs[5], row);
        if (slot->http_body_text) duckdb_unsafe_vector_assign_string_element_len(child_vecs[6], row, slot->http_body_text, (idx_t)strlen(slot->http_body_text));
        else set_null(child_vecs[6], row);
        if (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED) {
            slot->state = DUCKNNG_CLIENT_AIO_COLLECTED;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_aio_collectable_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->kind != DUCKNNG_CLIENT_AIO_KIND_NCURL &&
                (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED)) {
            out[row] = true;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static void ducknng_ncurl_aio_collectable_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t aio_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_aio *slot;
        out[row] = false;
        if (!ctx || !ctx->rt || aio_id == 0) continue;
        ducknng_mutex_lock(&ctx->rt->mu);
        slot = ducknng_find_client_aio_locked(ctx->rt, aio_id);
        if (slot && slot->kind == DUCKNNG_CLIENT_AIO_KIND_NCURL &&
                (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED)) {
            out[row] = true;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
    }
}

static idx_t ducknng_count_collectable_aios_locked(ducknng_runtime *rt, const uint64_t *aio_ids, idx_t aio_id_count) {
    idx_t i;
    idx_t count = 0;
    if (!rt || !aio_ids) return 0;
    for (i = 0; i < aio_id_count; i++) {
        ducknng_client_aio *slot = ducknng_find_client_aio_locked(rt, aio_ids[i]);
        if (!slot) continue;
        if (slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
            slot->state == DUCKNNG_CLIENT_AIO_CANCELLED) {
            count++;
        }
    }
    return count;
}

static int ducknng_wait_any_for_ids(ducknng_runtime *rt, const uint64_t *aio_ids, idx_t aio_id_count, int32_t wait_ms) {
    uint64_t deadline_ms;
    idx_t ready_count;
    if (!rt || !aio_ids || aio_id_count == 0) return 0;
    deadline_ms = ducknng_now_ms() + (wait_ms > 0 ? (uint64_t)wait_ms : 0ULL);
    ducknng_mutex_lock(&rt->mu);
    for (;;) {
        ready_count = ducknng_count_collectable_aios_locked(rt, aio_ids, aio_id_count);
        if (ready_count > 0) {
            ducknng_mutex_unlock(&rt->mu);
            return 1;
        }
        if (wait_ms == 0 || ducknng_now_ms() >= deadline_ms) {
            ducknng_mutex_unlock(&rt->mu);
            return 0;
        }
        if (ducknng_cond_timedwait_ms(&rt->aio_cv, &rt->mu, deadline_ms - ducknng_now_ms()) != 0) {
            ducknng_mutex_unlock(&rt->mu);
            return 0;
        }
    }
}

static void ducknng_aio_collect_materialize(ducknng_aio_collect_bind_data *bind) {
    ducknng_runtime *rt;
    idx_t ready_count;
    idx_t row_index = 0;
    idx_t out_index = 0;
    if (!bind || bind->materialized || !bind->ctx || !(rt = bind->ctx->rt)) return;
    bind->materialized = 1;
    if (!ducknng_wait_any_for_ids(rt, bind->aio_ids, bind->aio_id_count, bind->wait_ms) && bind->wait_ms > 0) {
        return;
    }
    ducknng_mutex_lock(&rt->mu);
    ready_count = ducknng_count_collectable_aios_locked(rt, bind->aio_ids, bind->aio_id_count);
    if (ready_count > 0) {
        bind->rows = (ducknng_aio_collect_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)ready_count);
        if (bind->rows) memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)ready_count);
    }
    if (ready_count > 0 && !bind->rows) {
        ducknng_mutex_unlock(&rt->mu);
        return;
    }
    bind->row_count = ready_count;
    for (row_index = 0; row_index < bind->aio_id_count && bind->rows; row_index++) {
        ducknng_client_aio *slot = ducknng_find_client_aio_locked(rt, bind->aio_ids[row_index]);
        ducknng_aio_collect_row *out_row;
        nng_msg *reply_msg;
        size_t frame_len;
        if (!slot) continue;
        if (!(slot->state == DUCKNNG_CLIENT_AIO_READY || slot->state == DUCKNNG_CLIENT_AIO_ERROR ||
                slot->state == DUCKNNG_CLIENT_AIO_CANCELLED)) {
            continue;
        }
        out_row = &bind->rows[out_index++];
        out_row->aio_id = slot->aio_id;
        out_row->ok = slot->state == DUCKNNG_CLIENT_AIO_READY;
        if (slot->error) out_row->error = ducknng_strdup(slot->error);
        {
            int32_t nng_err = 0;
            if (slot->send_result != 0 && slot->send_result != -1)
                nng_err = (int32_t)slot->send_result;
            else if (slot->recv_result != 0 && slot->recv_result != -1)
                nng_err = (int32_t)slot->recv_result;
            if (nng_err != 0) {
                out_row->has_nng_error = 1;
                out_row->nng_error = nng_err;
            }
        }
        if (slot->state == DUCKNNG_CLIENT_AIO_READY && (reply_msg = slot->reply_msg) != NULL) {
            frame_len = nng_msg_len(reply_msg);
            out_row->frame = (uint8_t *)duckdb_malloc(frame_len);
            if (out_row->frame || frame_len == 0) {
                out_row->frame_len = (idx_t)frame_len;
                if (frame_len) memcpy(out_row->frame, nng_msg_body(reply_msg), frame_len);
            }
            nng_msg_free(reply_msg);
            slot->reply_msg = NULL;
        }
        slot->state = DUCKNNG_CLIENT_AIO_COLLECTED;
    }
    ducknng_mutex_unlock(&rt->mu);
}

static void ducknng_aio_collect_bind(duckdb_bind_info info) {
    ducknng_aio_collect_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value aio_ids_val;
    duckdb_value wait_val;
    idx_t i;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_aio_collect_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    aio_ids_val = duckdb_bind_get_parameter(info, 0);
    wait_val = duckdb_bind_get_parameter(info, 1);
    bind->wait_ms = duckdb_get_int32(wait_val);
    if (bind->wait_ms < 0) {
        duckdb_destroy_value(&aio_ids_val);
        duckdb_destroy_value(&wait_val);
        duckdb_free(bind);
        duckdb_bind_set_error(info, "ducknng: aio_collect wait_ms must be >= 0");
        return;
    }
    bind->aio_id_count = duckdb_get_list_size(aio_ids_val);
    if (bind->aio_id_count > 0) {
        bind->aio_ids = (uint64_t *)duckdb_malloc(sizeof(*bind->aio_ids) * (size_t)bind->aio_id_count);
        if (!bind->aio_ids) {
            duckdb_destroy_value(&aio_ids_val);
            duckdb_destroy_value(&wait_val);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        for (i = 0; i < bind->aio_id_count; i++) {
            duckdb_value child = duckdb_get_list_child(aio_ids_val, i);
            bind->aio_ids[i] = (uint64_t)duckdb_get_uint64(child);
            duckdb_destroy_value(&child);
        }
    }
    duckdb_destroy_value(&aio_ids_val);
    duckdb_destroy_value(&wait_val);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "aio_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "frame", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "nng_error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "nng_error_message", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_aio_collect_bind_data);
}

static void ducknng_aio_collect_init(duckdb_init_info info) {
    ducknng_aio_collect_bind_data *bind = (ducknng_aio_collect_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_aio_collect_init_data *init = (ducknng_aio_collect_init_data *)duckdb_malloc(sizeof(*init));
    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing aio collect bind data");
        return;
    }
    ducknng_aio_collect_materialize(bind);
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_aio_collect_init_data);
}

static void ducknng_aio_collect_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_aio_collect_init_data *init = (ducknng_aio_collect_init_data *)duckdb_function_get_init_data(info);
    ducknng_aio_collect_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    uint64_t *aio_ids;
    bool *ok;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    aio_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    ok = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
    for (i = 0; i < chunk_size; i++) {
        ducknng_aio_collect_row *row = &bind->rows[init->offset + i];
        aio_ids[i] = row->aio_id;
        ok[i] = row->ok;
        if (row->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 2), i, row->error, (idx_t)strlen(row->error));
        else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        if (row->frame) assign_blob(duckdb_data_chunk_get_vector(output, 3), i, row->frame, row->frame_len);
        else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->has_nng_error) {
            int32_t *nng_errors = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
            nng_errors[i] = row->nng_error;
            duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), i, ducknng_nng_strerror(row->nng_error), (idx_t)strlen(ducknng_nng_strerror(row->nng_error)));
        } else {
            set_null(duckdb_data_chunk_get_vector(output, 4), i);
            set_null(duckdb_data_chunk_get_vector(output, 5), i);
        }
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}



static int register_aio_wait_any_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_logical_type list_child_type;
    duckdb_logical_type param_types[2];
    duckdb_logical_type return_type;
    int ok;
    if (!ctx || !ctx->rt) return 0;
    list_child_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    param_types[0] = duckdb_create_list_type(list_child_type);
    param_types[1] = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    return_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    ok = DUCKNNG_REGISTER_VOLATILE_SCALAR_LOGICAL_TYPES(con, name, 2,
        ducknng_aio_wait_any_scalar, ctx, param_types, return_type);
    duckdb_destroy_logical_type(&list_child_type);
    ducknng_destroy_logical_types(param_types, 2);
    duckdb_destroy_logical_type(&return_type);
    return ok;
}

static int register_aio_collect_row_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    static const duckdb_type param_type_ids[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER};
    static const duckdb_type field_type_ids[6] = {
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB,
        DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR
    };
    const char *field_names[6] = {"aio_id", "ok", "error", "frame", "nng_error", "nng_error_message"};
    return ducknng_register_struct_row_scalar_named(con, ctx, name, 2, param_type_ids,
        ducknng_aio_collect_row_scalar, 6, field_type_ids, field_names);
}

static int register_ncurl_aio_collect_row_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    static const duckdb_type param_type_ids[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER};
    static const duckdb_type field_type_ids[7] = {
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_VARCHAR
    };
    const char *field_names[7] = {"aio_id", "ok", "status", "error", "headers_json", "body", "body_text"};
    return ducknng_register_struct_row_scalar_named(con, ctx, name, 2, param_type_ids,
        ducknng_ncurl_aio_collect_row_scalar, 7, field_type_ids, field_names);
}

static int register_aio_status_scalar_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    static const duckdb_type param_type_ids[1] = {DUCKDB_TYPE_UBIGINT};
    static const duckdb_type field_type_ids[14] = {
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_BOOLEAN,
        DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR
    };
    const char *field_names[14] = {
        "aio_id", "exists", "kind", "state", "phase", "terminal",
        "send_done", "send_ok", "recv_done", "recv_ok", "has_reply_frame", "error",
        "nng_error", "nng_error_message"
    };
    return ducknng_register_struct_row_scalar_named(con, ctx, name, 1, param_type_ids,
        ducknng_aio_status_scalar, 14, field_type_ids, field_names);
}

static int register_aio_status_macro(duckdb_connection con) {
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_aio_status(aio_id) AS TABLE "
        "SELECT struct_extract(s, 'aio_id') AS aio_id, "
        "       struct_extract(s, 'exists') AS exists, "
        "       struct_extract(s, 'kind') AS kind, "
        "       struct_extract(s, 'state') AS state, "
        "       struct_extract(s, 'phase') AS phase, "
        "       struct_extract(s, 'terminal') AS terminal, "
        "       struct_extract(s, 'send_done') AS send_done, "
        "       struct_extract(s, 'send_ok') AS send_ok, "
        "       struct_extract(s, 'recv_done') AS recv_done, "
        "       struct_extract(s, 'recv_ok') AS recv_ok, "
        "       struct_extract(s, 'has_reply_frame') AS has_reply_frame, "
        "       struct_extract(s, 'error') AS error, "
        "       struct_extract(s, 'nng_error') AS nng_error, "
        "       struct_extract(s, 'nng_error_message') AS nng_error_message "
        "FROM (SELECT ducknng__aio_status_row(aio_id) AS s)";
    return execute_sql(con, sql);
}

static int register_aio_collect_macro(duckdb_connection con) {
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_aio_collect(aio_ids, wait_ms) AS TABLE "
        "WITH _input AS (SELECT aio_ids AS aio_ids, ducknng_aio_wait(aio_ids, wait_ms) AS have_ready) "
        "SELECT struct_extract(r, 'aio_id') AS aio_id, "
        "       struct_extract(r, 'ok') AS ok, "
        "       struct_extract(r, 'error') AS error, "
        "       struct_extract(r, 'frame') AS frame, "
        "       struct_extract(r, 'nng_error') AS nng_error, "
        "       struct_extract(r, 'nng_error_message') AS nng_error_message "
        "FROM _input, "
        "UNNEST(list_transform("
        "  list_filter(aio_ids, lambda x: ducknng__aio_collectable(x)), "
        "  lambda x: ducknng__aio_collect_row(x, 0)"
        ")) AS t(r) "
        "WHERE have_ready";
    return execute_sql(con, sql);
}

static int register_aio_collect_decoded_macro(duckdb_connection con) {
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_aio_collect_decoded(aio_ids, wait_ms) AS TABLE "
        "WITH collected AS (SELECT * FROM ducknng_aio_collect(aio_ids, wait_ms)) "
        "SELECT c.aio_id AS aio_id, "
        "       c.ok AS ok, "
        "       c.error AS error, "
        "       c.nng_error AS nng_error, "
        "       c.nng_error_message AS nng_error_message, "
        "       ducknng_frame_version(c.frame) IS NOT NULL AS frame_ok, "
        "       ducknng_frame_error_text(c.frame) AS frame_error, "
        "       ducknng_frame_version(c.frame) AS version, "
        "       ducknng_frame_type(c.frame) AS type, "
        "       ducknng_frame_flags(c.frame) AS flags, "
        "       ducknng_frame_type_name(c.frame) AS type_name, "
        "       ducknng_frame_name(c.frame) AS name, "
        "       ducknng_frame_payload(c.frame) AS payload, "
        "       ducknng_frame_payload_text(c.frame) AS payload_text "
        "FROM collected AS c";
    return execute_sql(con, sql);
}

static int register_ncurl_aio_collect_macro(duckdb_connection con) {
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_ncurl_aio_collect(aio_ids, wait_ms) AS TABLE "
        "WITH _input AS (SELECT aio_ids AS aio_ids, ducknng_aio_wait(aio_ids, wait_ms) AS have_ready) "
        "SELECT struct_extract(r, 'aio_id') AS aio_id, "
        "       struct_extract(r, 'ok') AS ok, "
        "       struct_extract(r, 'status') AS status, "
        "       struct_extract(r, 'error') AS error, "
        "       struct_extract(r, 'headers_json') AS headers_json, "
        "       struct_extract(r, 'body') AS body, "
        "       struct_extract(r, 'body_text') AS body_text "
        "FROM _input, "
        "UNNEST(list_transform("
        "  list_filter(aio_ids, lambda x: ducknng__ncurl_aio_collectable(x)), "
        "  lambda x: ducknng__ncurl_aio_collect_row(x, 0)"
        ")) AS t(r) "
        "WHERE have_ready";
    return execute_sql(con, sql);
}


int ducknng_register_sql_aio(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type rpc_exec_raw_aio_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type rpc_manifest_raw_aio_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type open_query_raw_aio_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type fetch_query_raw_aio_types[7] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type session_control_raw_aio_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type ncurl_aio_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type ncurl_aio_profile_types[7] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    duckdb_type request_socket_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    duckdb_type recv_socket_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER};
    duckdb_type request_tls_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type aio_id_types[1] = {DUCKDB_TYPE_UBIGINT};
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_run_rpc_raw_aio", 4, ducknng_run_rpc_raw_aio_scalar, ctx, rpc_exec_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_get_rpc_manifest_raw_aio", 3, ducknng_get_rpc_manifest_raw_aio_scalar, ctx, rpc_manifest_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_open_query_raw_aio", 6, ducknng_open_query_raw_aio_scalar, ctx, open_query_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_fetch_query_raw_aio", 7, ducknng_fetch_query_raw_aio_scalar, ctx, fetch_query_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_close_query_raw_aio", 5, ducknng_close_query_raw_aio_scalar, ctx, session_control_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_cancel_query_raw_aio", 5, ducknng_cancel_query_raw_aio_scalar, ctx, session_control_raw_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_ncurl_aio", 6, ducknng_ncurl_aio_scalar, ctx, ncurl_aio_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_ncurl_aio", 7, ducknng_ncurl_aio_scalar, ctx, ncurl_aio_profile_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_send_socket_raw_aio", 3, ducknng_send_socket_raw_aio_scalar, ctx, request_socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_recv_socket_raw_aio", 2, ducknng_recv_socket_raw_aio_scalar, ctx, recv_socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_request_socket_raw_aio", 3, ducknng_request_socket_raw_aio_scalar, ctx, request_socket_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_request_raw_aio", 4, ducknng_request_raw_aio_scalar, ctx, request_tls_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_aio_ready", 1, ducknng_aio_ready_scalar, ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_aio_cancel", 1, ducknng_aio_cancel_scalar, ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_aio_drop", 1, ducknng_aio_drop_scalar, ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_aio_wait_any_scalar_named(con, ctx, "ducknng_aio_wait")) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng__aio_collectable", 1, ducknng_aio_collectable_scalar, ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng__ncurl_aio_collectable", 1, ducknng_ncurl_aio_collectable_scalar, ctx, aio_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_aio_collect_row_scalar_named(con, ctx, "ducknng__aio_collect_row")) return 0;
    if (!register_ncurl_aio_collect_row_scalar_named(con, ctx, "ducknng__ncurl_aio_collect_row")) return 0;
    if (!register_aio_status_scalar_named(con, ctx, "ducknng__aio_status_row")) return 0;
    if (!register_aio_status_macro(con)) return 0;
    if (!register_aio_collect_macro(con)) return 0;
    if (!register_aio_collect_decoded_macro(con)) return 0;
    if (!register_ncurl_aio_collect_macro(con)) return 0;
    return 1;
}
