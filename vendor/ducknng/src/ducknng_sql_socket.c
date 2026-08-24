#include "ducknng_sql_shared.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(__EMSCRIPTEN__)
#include <emscripten/threading.h>
#endif

DUCKDB_EXTENSION_EXTERN

#if defined(__EMSCRIPTEN__) && defined(DUCKNNG_WASM_TRACE) && DUCKNNG_WASM_TRACE
static void ducknng_socket_wasm_trace(const char *where) {
    fprintf(stderr, "[ducknng wasm trace] %s\n", where ? where : "(null)");
    fflush(stderr);
}
#else
static void ducknng_socket_wasm_trace(const char *where) {
    (void)where;
    /* Browser pthread side modules need small progress points around NNG
     * socket/listener operations even when diagnostic trace text is disabled. */
#if defined(__EMSCRIPTEN__)
    __asm__ __volatile__("" ::: "memory");
    fflush(stderr);
#if defined(DUCKNNG_WASM_INPROC_ONLY) && DUCKNNG_WASM_INPROC_ONLY
    emscripten_thread_sleep(1.0);
#endif
#endif
}
#endif

typedef struct {
    uint64_t socket_id;
    char *protocol;
    char *url;
    bool open;
    bool connected;
    bool listening;
    int32_t send_timeout_ms;
    int32_t recv_timeout_ms;
} ducknng_socket_row;

typedef struct {
    ducknng_socket_row *rows;
    idx_t row_count;
} ducknng_sockets_bind_data;

typedef struct {
    ducknng_sockets_bind_data *bind;
    idx_t offset;
} ducknng_sockets_init_data;

typedef struct {
    bool ok;
    char *error;
    int has_nng_error;
    int32_t nng_error;
    uint64_t socket_id;
    int has_payload;
    uint8_t *payload;
    idx_t payload_len;
    char *url;
} ducknng_socket_result;

static void ducknng_socket_result_init(ducknng_socket_result *res) {
    if (!res) return;
    memset(res, 0, sizeof(*res));
}

static void ducknng_socket_result_reset(ducknng_socket_result *res) {
    if (!res) return;
    if (res->error) duckdb_free(res->error);
    if (res->payload) duckdb_free(res->payload);
    if (res->url) duckdb_free(res->url);
    memset(res, 0, sizeof(*res));
}

static void ducknng_socket_result_set_error(ducknng_socket_result *res, const char *message) {
    if (!res) return;
    if (res->error) {
        duckdb_free(res->error);
        res->error = NULL;
    }
    res->ok = false;
    res->error = ducknng_strdup(message ? message : "ducknng: socket operation failed");
}

static void ducknng_socket_result_take_error(ducknng_socket_result *res, char *message, const char *fallback) {
    if (!res) {
        if (message) duckdb_free(message);
        return;
    }
    if (res->error) {
        duckdb_free(res->error);
        res->error = NULL;
    }
    res->ok = false;
    res->error = message ? message : ducknng_strdup(fallback ? fallback : "ducknng: socket operation failed");
}

static void ducknng_socket_result_set_nng_error(ducknng_socket_result *res, int rv) {
    if (!res) return;
    res->ok = false;
    res->has_nng_error = 1;
    res->nng_error = (int32_t)rv;
    ducknng_socket_result_set_error(res, ducknng_nng_strerror(rv));
    res->has_nng_error = 1;
    res->nng_error = (int32_t)rv;
}

static void ducknng_socket_result_set_payload(ducknng_socket_result *res, const uint8_t *data, idx_t len) {
    if (!res) return;
    if (res->payload) {
        duckdb_free(res->payload);
        res->payload = NULL;
    }
    res->payload_len = 0;
    res->has_payload = 1;
    if (len > 0) {
        res->payload = (uint8_t *)duckdb_malloc((size_t)len);
        if (!res->payload) {
            res->has_payload = 0;
            ducknng_socket_result_set_error(res, "ducknng: out of memory copying socket payload");
            return;
        }
        memcpy(res->payload, data, (size_t)len);
    }
    res->payload_len = len;
}

static void ducknng_socket_result_set_url(ducknng_socket_result *res, const char *url) {
    if (!res) return;
    if (res->url) {
        duckdb_free(res->url);
        res->url = NULL;
    }
    if (url) res->url = ducknng_strdup(url);
}

static void ducknng_socket_result_emit(duckdb_vector output, idx_t row,
    const ducknng_socket_result *res) {
    duckdb_vector child_vecs[7];
    bool *ok_data;
    int32_t *nng_errors;
    uint64_t *socket_ids;
    const char *nng_message = NULL;
    int i;
    for (i = 0; i < 7; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    ok_data = (bool *)duckdb_vector_get_data(child_vecs[0]);
    nng_errors = (int32_t *)duckdb_vector_get_data(child_vecs[2]);
    socket_ids = (uint64_t *)duckdb_vector_get_data(child_vecs[4]);
    ok_data[row] = res && res->ok;
    if (res && res->error) duckdb_unsafe_vector_assign_string_element_len(child_vecs[1], row, res->error, (idx_t)strlen(res->error));
    else set_null(child_vecs[1], row);
    if (res && res->has_nng_error) {
        nng_errors[row] = res->nng_error;
        nng_message = ducknng_nng_strerror(res->nng_error);
        if (nng_message) duckdb_unsafe_vector_assign_string_element_len(child_vecs[3], row, nng_message, (idx_t)strlen(nng_message));
        else set_null(child_vecs[3], row);
    } else {
        set_null(child_vecs[2], row);
        set_null(child_vecs[3], row);
    }
    if (res && res->socket_id != 0) socket_ids[row] = res->socket_id;
    else set_null(child_vecs[4], row);
    if (res && res->has_payload) {
        const uint8_t *payload = res->payload ? res->payload : (const uint8_t *)"";
        assign_blob(child_vecs[5], row, payload, res->payload_len);
    } else {
        set_null(child_vecs[5], row);
    }
    if (res && res->url) duckdb_unsafe_vector_assign_string_element_len(child_vecs[6], row, res->url, (idx_t)strlen(res->url));
    else set_null(child_vecs[6], row);
}

static void ducknng_socket_destroy_logical_types(duckdb_logical_type *types, idx_t count) {
    idx_t i;
    if (!types) return;
    for (i = 0; i < count; i++) duckdb_destroy_logical_type(&types[i]);
}

static int ducknng_register_socket_result_scalar_named(duckdb_connection con,
    ducknng_sql_context *ctx, const char *name, idx_t nparams, const duckdb_type *param_type_ids,
    duckdb_scalar_function_t fn) {
    static const duckdb_type field_type_ids[7] = {
        DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_VARCHAR
    };
    const char *field_names[7] = {
        "ok", "error", "nng_error", "nng_error_message", "socket_id", "payload", "url"
    };
    duckdb_logical_type *param_types = NULL;
    duckdb_logical_type fields[7];
    duckdb_logical_type return_type;
    idx_t i;
    int ok;
    if (!ctx || !ctx->rt || !name || !fn || (!param_type_ids && nparams > 0)) return 0;
    param_types = nparams ? (duckdb_logical_type *)duckdb_malloc(sizeof(*param_types) * nparams) : NULL;
    if (nparams > 0 && !param_types) return 0;
    for (i = 0; i < nparams; i++) param_types[i] = duckdb_create_logical_type(param_type_ids[i]);
    for (i = 0; i < 7; i++) fields[i] = duckdb_create_logical_type(field_type_ids[i]);
    return_type = duckdb_create_struct_type(fields, field_names, 7);
    ok = DUCKNNG_REGISTER_VOLATILE_SCALAR_LOGICAL_TYPES(con, name, nparams, fn, ctx,
        param_types, return_type);
    ducknng_socket_destroy_logical_types(param_types, nparams);
    ducknng_socket_destroy_logical_types(fields, 7);
    if (param_types) duckdb_free(param_types);
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

static int ducknng_socket_is_active(const ducknng_client_socket *sock) {
    return sock && sock->open && (sock->connected || sock->has_listener);
}

static int ducknng_socket_is_req_protocol(const ducknng_client_socket *sock) {
    return sock && sock->protocol && strcmp(sock->protocol, "req") == 0;
}

static void destroy_sockets_bind_data(void *ptr) {
    ducknng_sockets_bind_data *data = (ducknng_sockets_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].protocol) duckdb_free(data->rows[i].protocol);
        if (data->rows[i].url) duckdb_free(data->rows[i].url);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_sockets_init_data(void *ptr) {
    ducknng_sockets_init_data *data = (ducknng_sockets_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void ducknng_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *protocol = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        int rv;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        if (!ctx || !ctx->rt || !protocol) {
            ducknng_socket_result_set_error(&result, "ducknng: socket protocol is required");
            goto emit;
        }
        sock = (ducknng_client_socket *)duckdb_malloc(sizeof(*sock));
        if (!sock) {
            duckdb_free(protocol);
            protocol = NULL;
            ducknng_socket_result_set_error(&result, "ducknng: out of memory allocating client socket");
            goto emit;
        }
        memset(sock, 0, sizeof(*sock));
        sock->protocol = protocol;
        protocol = NULL;
        sock->send_timeout_ms = 5000;
        sock->recv_timeout_ms = 5000;
        if (ducknng_mutex_init(&sock->mu) != 0) {
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            ducknng_socket_result_set_error(&result, "ducknng: failed to initialize client socket mutex");
            goto emit;
        }
        sock->mu_initialized = 1;
        if (ducknng_cond_init(&sock->cv) != 0) {
            ducknng_mutex_destroy(&sock->mu);
            sock->mu_initialized = 0;
            duckdb_free(sock->protocol);
            duckdb_free(sock);
            ducknng_socket_result_set_error(&result, "ducknng: failed to initialize client socket condition variable");
            goto emit;
        }
        sock->cv_initialized = 1;
        ducknng_socket_wasm_trace("open_socket: nng socket open begin");
        if (ducknng_socket_open_protocol(sock->protocol, &sock->sock, &errmsg) != 0) {
            ducknng_socket_wasm_trace("open_socket: nng socket open failed");
            ducknng_client_socket_destroy(sock);
            ducknng_socket_result_take_error(&result, errmsg, "ducknng: failed to open socket protocol");
            errmsg = NULL;
            goto emit;
        }
        ducknng_socket_wasm_trace("open_socket: nng socket open returned");
        ducknng_socket_wasm_trace("open_socket: ctx open begin");
        rv = ducknng_ctx_open(&sock->ctx, sock->sock);
        ducknng_socket_wasm_trace("open_socket: ctx open returned");
        if (rv == 0) {
            sock->has_ctx = 1;
        } else if (rv != NNG_ENOTSUP) {
            ducknng_socket_close(sock->sock);
            sock->open = 0;
            ducknng_client_socket_destroy(sock);
            ducknng_socket_result_set_nng_error(&result, rv);
            goto emit;
        }
        sock->open = 1;
        ducknng_socket_wasm_trace("open_socket: runtime add socket begin");
        if (ducknng_runtime_add_client_socket(ctx->rt, sock, &errmsg) != 0) {
            if (sock->has_ctx) {
                ducknng_ctx_close(sock->ctx);
                sock->has_ctx = 0;
            }
            if (sock->open) {
                ducknng_socket_close(sock->sock);
                sock->open = 0;
            }
            ducknng_client_socket_destroy(sock);
            ducknng_socket_result_take_error(&result, errmsg, "ducknng: failed to register client socket");
            errmsg = NULL;
            goto emit;
        }
        ducknng_socket_wasm_trace("open_socket: runtime add socket returned");
        result.ok = true;
        result.socket_id = sock->socket_id;
emit:
        if (protocol) duckdb_free(protocol);
        if (errmsg) duckdb_free(errmsg);
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}

static void ducknng_dial_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        int rv;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        result.socket_id = socket_id;
        if (!ctx || !ctx->rt || socket_id == 0 || !url) {
            ducknng_socket_result_set_error(&result, "ducknng: socket id and URL are required");
            goto emit;
        }
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            ducknng_socket_result_take_error(&result, errmsg, "ducknng: tls config not found");
            errmsg = NULL;
            goto emit;
        }
        if (ducknng_socket_validate_client_url(url, tls_opts, &errmsg) != 0) {
            ducknng_socket_result_take_error(&result, errmsg, "ducknng: invalid transport URL");
            errmsg = NULL;
            goto emit;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock) {
            ducknng_socket_result_set_error(&result, "ducknng: client socket not found");
            goto emit;
        }
        ducknng_mutex_lock(&sock->mu);
        if (!sock->open) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: client socket not found");
            goto emit;
        }
        if (sock->connected) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: socket is already dialed");
            goto emit;
        }
        ducknng_socket_wasm_trace("dial_socket: set timeout begin");
        rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
        if (rv == 0) {
            ducknng_socket_wasm_trace("dial_socket: apply tls begin");
            rv = ducknng_socket_apply_tls(sock->sock, url, tls_opts);
        }
        if (rv == 0) {
            ducknng_socket_wasm_trace("dial_socket: blocking dial begin");
            rv = ducknng_socket_dial(sock->sock, url);
            ducknng_socket_wasm_trace("dial_socket: blocking dial returned");
        }
        if (rv == 0) {
            if (sock->url) duckdb_free(sock->url);
            sock->url = url;
            sock->connected = 1;
            sock->send_timeout_ms = timeout_ms;
            sock->recv_timeout_ms = timeout_ms;
            result.ok = true;
            result.socket_id = sock->socket_id;
            ducknng_socket_result_set_url(&result, sock->url);
            url = NULL;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            ducknng_socket_result_set_nng_error(&result, rv);
            goto emit;
        }
emit:
        if (url) duckdb_free(url);
        if (errmsg) duckdb_free(errmsg);
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}

static void ducknng_listen_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    ducknng_socket_wasm_trace("listen_socket: function entered");
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        ducknng_socket_wasm_trace("listen_socket: row begin");
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_socket_wasm_trace("listen_socket: arg socket id read");
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_socket_wasm_trace("listen_socket: arg url read");
        uint64_t recv_max_bytes = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        ducknng_socket_wasm_trace("listen_socket: arg recvmax read");
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        ducknng_socket_wasm_trace("listen_socket: arg tls id read");
        const ducknng_tls_opts *tls_opts = NULL;
        ducknng_client_socket *sock;
        char *errmsg = NULL;
        nng_listener lst;
        char *resolved_url = NULL;
        int rv;
        int listener_created = 0;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        result.socket_id = socket_id;
        memset(&lst, 0, sizeof(lst));
        ducknng_socket_wasm_trace("listen_socket: scalar begin");
        if (!ctx || !ctx->rt || socket_id == 0 || !url) {
            ducknng_socket_result_set_error(&result, "ducknng: socket id and URL are required");
            goto emit;
        }
        ducknng_socket_wasm_trace("listen_socket: tls lookup begin");
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            ducknng_socket_result_take_error(&result, errmsg, "ducknng: tls config not found");
            errmsg = NULL;
            goto emit;
        }
        ducknng_socket_wasm_trace("listen_socket: tls lookup returned");
        ducknng_socket_wasm_trace("listen_socket: validate url begin");
        if (ducknng_listener_validate_startup_url(url, tls_opts, &errmsg) != 0) {
            ducknng_socket_result_take_error(&result, errmsg, "ducknng: invalid listen URL");
            errmsg = NULL;
            goto emit;
        }
        ducknng_socket_wasm_trace("listen_socket: validate url returned");
        ducknng_socket_wasm_trace("listen_socket: acquire socket begin");
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        ducknng_socket_wasm_trace("listen_socket: acquire socket returned");
        if (!sock) {
            ducknng_socket_result_set_error(&result, "ducknng: client socket not found");
            goto emit;
        }
        ducknng_socket_wasm_trace("listen_socket: socket mutex lock begin");
        ducknng_mutex_lock(&sock->mu);
        ducknng_socket_wasm_trace("listen_socket: socket mutex lock returned");
        if (!sock->open) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: client socket not found");
            goto emit;
        }
        if (sock->has_listener) {
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: socket is already listening");
            goto emit;
        }
        ducknng_socket_wasm_trace("listen_socket: listener create begin");
        rv = ducknng_listener_create(&lst, sock->sock, url);
        ducknng_socket_wasm_trace("listen_socket: listener create returned");
        if (rv == 0) listener_created = 1;
        if (rv == 0 && recv_max_bytes > 0) {
            ducknng_socket_wasm_trace("listen_socket: set recvmax begin");
            rv = ducknng_listener_set_recvmaxsz(lst, (size_t)recv_max_bytes);
            ducknng_socket_wasm_trace("listen_socket: set recvmax returned");
        }
        if (rv == 0) {
            ducknng_socket_wasm_trace("listen_socket: apply tls begin");
            rv = ducknng_listener_apply_tls(lst, tls_opts);
            ducknng_socket_wasm_trace("listen_socket: apply tls returned");
        }
        if (rv == 0) {
            ducknng_socket_wasm_trace("listen_socket: listener start begin");
            rv = ducknng_listener_start(lst);
            ducknng_socket_wasm_trace("listen_socket: listener start returned");
        }
        if (rv == 0) {
            resolved_url = ducknng_listener_resolve_url(lst, url);
            if (sock->listen_url) duckdb_free(sock->listen_url);
            sock->listen_url = resolved_url ? resolved_url : url;
            if (resolved_url) duckdb_free(url);
            url = NULL;
            sock->listener = lst;
            sock->has_listener = 1;
            result.ok = true;
            result.socket_id = sock->socket_id;
            ducknng_socket_result_set_url(&result, sock->listen_url);
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            if (listener_created) ducknng_listener_close(lst);
            ducknng_socket_result_set_nng_error(&result, rv);
            goto emit;
        }
emit:
        if (url) duckdb_free(url);
        if (errmsg) duckdb_free(errmsg);
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}

static void ducknng_close_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_client_socket *sock;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        result.socket_id = socket_id;
        if (!ctx || !ctx->rt || socket_id == 0) {
            ducknng_socket_result_set_error(&result, "ducknng: socket id is required");
            goto emit;
        }
        sock = ducknng_runtime_remove_client_socket(ctx->rt, socket_id);
        if (!sock) {
            ducknng_socket_result_set_error(&result, "ducknng: client socket not found");
            goto emit;
        }
        ducknng_client_socket_destroy(sock);
        result.ok = true;
emit:
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}

static void ducknng_send_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *msg = NULL;
        char *errmsg = NULL;
        int rv;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        result.socket_id = socket_id;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            ducknng_socket_result_set_error(&result, "ducknng: socket id, payload, and timeout are required");
            goto emit;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!ducknng_socket_is_active(sock)) {
            if (sock) ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: active client socket not found");
            goto emit;
        }
        msg = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
        if (!msg) {
            ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_take_error(&result, errmsg, "ducknng: failed to build socket send message");
            errmsg = NULL;
            goto emit;
        }
        ducknng_mutex_lock(&sock->mu);
        ducknng_socket_wasm_trace("send_socket_raw: set timeout begin");
        rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, sock->recv_timeout_ms);
        if (rv == 0) {
            ducknng_socket_wasm_trace("send_socket_raw: send begin");
            rv = ducknng_socket_send(sock->sock, msg);
            ducknng_socket_wasm_trace("send_socket_raw: send returned");
            if (rv == 0) msg = NULL;
        }
        if (rv == 0) {
            sock->send_timeout_ms = timeout_ms;
            result.ok = true;
            result.socket_id = sock->socket_id;
        }
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            if (msg) nng_msg_free(msg);
            ducknng_socket_result_set_nng_error(&result, rv);
            goto emit;
        }
emit:
        if (payload) duckdb_free(payload);
        if (errmsg) duckdb_free(errmsg);
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}

static void ducknng_recv_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 5000);
        ducknng_client_socket *sock;
        nng_msg *msg = NULL;
        int rv;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        result.socket_id = socket_id;
        if (!ctx || !ctx->rt || socket_id == 0) {
            ducknng_socket_result_set_error(&result, "ducknng: socket id is required");
            goto emit;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!ducknng_socket_is_active(sock)) {
            if (sock) ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: active client socket not found");
            goto emit;
        }
        ducknng_mutex_lock(&sock->mu);
        ducknng_socket_wasm_trace("recv_socket_raw: set timeout begin");
        rv = ducknng_socket_set_timeout_ms(sock->sock, sock->send_timeout_ms, timeout_ms);
        if (rv == 0) {
            ducknng_socket_wasm_trace("recv_socket_raw: recv begin");
            rv = ducknng_socket_recv(sock->sock, &msg);
            ducknng_socket_wasm_trace("recv_socket_raw: recv returned");
        }
        if (rv == 0) sock->recv_timeout_ms = timeout_ms;
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            ducknng_socket_result_set_nng_error(&result, rv);
            goto emit;
        }
        result.ok = true;
        ducknng_socket_result_set_payload(&result, (const uint8_t *)nng_msg_body(msg), (idx_t)nng_msg_len(msg));
        nng_msg_free(msg);
emit:
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}

static void ducknng_subscribe_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t topic_len = 0;
        uint8_t *topic = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &topic_len);
        ducknng_client_socket *sock;
        int rv;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        result.socket_id = socket_id;
        if (!ctx || !ctx->rt || socket_id == 0 || (!topic && topic_len > 0)) {
            ducknng_socket_result_set_error(&result, "ducknng: subscribe_socket requires socket id and topic blob");
            goto emit;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->protocol || strcmp(sock->protocol, "sub") != 0) {
            if (sock) ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: sub socket not found");
            goto emit;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_subscribe(sock->sock, topic, (size_t)topic_len);
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            ducknng_socket_result_set_nng_error(&result, rv);
            goto emit;
        }
        result.ok = true;
emit:
        if (topic) duckdb_free(topic);
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}

static void ducknng_unsubscribe_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        uint64_t socket_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        idx_t topic_len = 0;
        uint8_t *topic = arg_blob_dup(duckdb_data_chunk_get_vector(input, 1), row, &topic_len);
        ducknng_client_socket *sock;
        int rv;
        ducknng_socket_result result;
        ducknng_socket_result_init(&result);
        result.socket_id = socket_id;
        if (!ctx || !ctx->rt || socket_id == 0 || (!topic && topic_len > 0)) {
            ducknng_socket_result_set_error(&result, "ducknng: unsubscribe_socket requires socket id and topic blob");
            goto emit;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->protocol || strcmp(sock->protocol, "sub") != 0) {
            if (sock) ducknng_runtime_release_client_socket(sock);
            ducknng_socket_result_set_error(&result, "ducknng: sub socket not found");
            goto emit;
        }
        ducknng_mutex_lock(&sock->mu);
        rv = ducknng_socket_unsubscribe(sock->sock, topic, (size_t)topic_len);
        ducknng_mutex_unlock(&sock->mu);
        ducknng_runtime_release_client_socket(sock);
        if (rv != 0) {
            ducknng_socket_result_set_nng_error(&result, rv);
            goto emit;
        }
        result.ok = true;
emit:
        if (topic) duckdb_free(topic);
        ducknng_socket_result_emit(output, row, &result);
        ducknng_socket_result_reset(&result);
    }
}


static void ducknng_sockets_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_sockets_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_sockets_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->client_socket_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_socket_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            ducknng_client_socket *sock = ctx->rt->client_sockets[i];
            bind->rows[i].socket_id = sock ? sock->socket_id : 0;
            bind->rows[i].protocol = sock && sock->protocol ? ducknng_strdup(sock->protocol) : NULL;
            bind->rows[i].url = sock ? ducknng_strdup(sock->url ? sock->url : sock->listen_url) : NULL;
            bind->rows[i].open = sock ? (bool)sock->open : false;
            bind->rows[i].connected = sock ? (bool)sock->connected : false;
            bind->rows[i].listening = sock ? (bool)sock->has_listener : false;
            bind->rows[i].send_timeout_ms = sock ? sock->send_timeout_ms : 0;
            bind->rows[i].recv_timeout_ms = sock ? sock->recv_timeout_ms : 0;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "socket_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "protocol", type);
    duckdb_bind_add_result_column(info, "url", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "open", type);
    duckdb_bind_add_result_column(info, "connected", type);
    duckdb_bind_add_result_column(info, "listening", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "send_timeout_ms", type);
    duckdb_bind_add_result_column(info, "recv_timeout_ms", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_sockets_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_sockets_init(duckdb_init_info info) {
    ducknng_sockets_bind_data *bind = (ducknng_sockets_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_sockets_init_data *init = (ducknng_sockets_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_sockets_init_data);
}

static void ducknng_sockets_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sockets_init_data *init = (ducknng_sockets_init_data *)duckdb_function_get_init_data(info);
    ducknng_sockets_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    duckdb_vector vec_socket_id;
    duckdb_vector vec_protocol;
    duckdb_vector vec_url;
    duckdb_vector vec_open;
    duckdb_vector vec_connected;
    duckdb_vector vec_listening;
    duckdb_vector vec_send_timeout_ms;
    duckdb_vector vec_recv_timeout_ms;
    uint64_t *socket_ids;
    bool *open;
    bool *connected;
    bool *listening;
    int32_t *send_timeout_ms;
    int32_t *recv_timeout_ms;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    vec_socket_id = duckdb_data_chunk_get_vector(output, 0);
    vec_protocol = duckdb_data_chunk_get_vector(output, 1);
    vec_url = duckdb_data_chunk_get_vector(output, 2);
    vec_open = duckdb_data_chunk_get_vector(output, 3);
    vec_connected = duckdb_data_chunk_get_vector(output, 4);
    vec_listening = duckdb_data_chunk_get_vector(output, 5);
    vec_send_timeout_ms = duckdb_data_chunk_get_vector(output, 6);
    vec_recv_timeout_ms = duckdb_data_chunk_get_vector(output, 7);
    socket_ids = (uint64_t *)duckdb_vector_get_data(vec_socket_id);
    open = (bool *)duckdb_vector_get_data(vec_open);
    connected = (bool *)duckdb_vector_get_data(vec_connected);
    listening = (bool *)duckdb_vector_get_data(vec_listening);
    send_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_send_timeout_ms);
    recv_timeout_ms = (int32_t *)duckdb_vector_get_data(vec_recv_timeout_ms);
    for (i = 0; i < chunk_size; i++) {
        ducknng_socket_row *row = &bind->rows[init->offset + i];
        socket_ids[i] = row->socket_id;
        open[i] = row->open;
        connected[i] = row->connected;
        listening[i] = row->listening;
        send_timeout_ms[i] = row->send_timeout_ms;
        recv_timeout_ms[i] = row->recv_timeout_ms;
        if (row->protocol) duckdb_unsafe_vector_assign_string_element_len(vec_protocol, i, row->protocol, (idx_t)strlen(row->protocol));
        else set_null(vec_protocol, i);
        if (row->url) duckdb_unsafe_vector_assign_string_element_len(vec_url, i, row->url, (idx_t)strlen(row->url));
        else set_null(vec_url, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}


static int register_named_sockets_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 0, NULL, ducknng_sockets_bind,
        ducknng_sockets_init, ducknng_sockets_scan);
}

int ducknng_register_sql_socket(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type socket_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type close_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type dial_types[4] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type listen_types[4] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type request_socket_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    duckdb_type recv_socket_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_INTEGER};
    duckdb_type subscribe_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB};
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_open_socket", 1, socket_types, ducknng_socket_scalar)) return 0;
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_dial_socket", 4, dial_types, ducknng_dial_scalar)) return 0;
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_listen_socket", 4, listen_types, ducknng_listen_scalar)) return 0;
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_close_socket", 1, close_types, ducknng_close_scalar)) return 0;
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_send_socket_raw", 3, request_socket_types, ducknng_send_scalar)) return 0;
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_recv_socket_raw", 2, recv_socket_types, ducknng_recv_scalar)) return 0;
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_subscribe_socket", 2, subscribe_types, ducknng_subscribe_scalar)) return 0;
    if (!ducknng_register_socket_result_scalar_named(con, ctx, "ducknng_unsubscribe_socket", 2, subscribe_types, ducknng_unsubscribe_scalar)) return 0;
    if (!register_named_sockets_table(con, ctx, "ducknng_list_sockets")) return 0;
    return 1;
}
