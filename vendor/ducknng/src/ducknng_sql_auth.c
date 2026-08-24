#include "ducknng_sql_shared.h"
#include "ducknng_service.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    idx_t emitted;
} ducknng_sql_auth_single_row_init_data;

typedef struct {
    idx_t row_count;
    char *phase;
    char *service_name;
    char *transport_family;
    char *scheme;
    char *listen;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    bool tls_verified;
    char *peer_identity;
    bool peer_allowlist_active;
    bool ip_allowlist_active;
    bool sql_authorizer_active;
    char *http_method;
    char *http_path;
    char *content_type;
    uint64_t body_bytes;
    char *rpc_method;
    char *rpc_type;
    uint64_t payload_bytes;
} ducknng_auth_context_bind_data;

static char *ducknng_dup_bytes_local(const uint8_t *data, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, data, len);
    out[len] = '\0';
    return out;
}

static void ducknng_sql_auth_single_row_init(duckdb_init_info info) {
    ducknng_sql_auth_single_row_init_data *init =
        (ducknng_sql_auth_single_row_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, duckdb_free);
}

static void destroy_auth_context_bind_data(void *ptr) {
    ducknng_auth_context_bind_data *data = (ducknng_auth_context_bind_data *)ptr;
    if (!data) return;
    if (data->phase) duckdb_free(data->phase);
    if (data->service_name) duckdb_free(data->service_name);
    if (data->transport_family) duckdb_free(data->transport_family);
    if (data->scheme) duckdb_free(data->scheme);
    if (data->listen) duckdb_free(data->listen);
    if (data->remote_addr) duckdb_free(data->remote_addr);
    if (data->remote_ip) duckdb_free(data->remote_ip);
    if (data->peer_identity) duckdb_free(data->peer_identity);
    if (data->http_method) duckdb_free(data->http_method);
    if (data->http_path) duckdb_free(data->http_path);
    if (data->content_type) duckdb_free(data->content_type);
    if (data->rpc_method) duckdb_free(data->rpc_method);
    if (data->rpc_type) duckdb_free(data->rpc_type);
    duckdb_free(data);
}

static char *ducknng_sql_frame_method_dup(const ducknng_frame *frame) {
    if (!frame) return NULL;
    if (frame->type == DUCKNNG_RPC_MANIFEST) return ducknng_strdup("manifest");
    if (frame->type == DUCKNNG_RPC_CALL && frame->name && frame->name_len > 0) {
        return ducknng_dup_bytes_local(frame->name, frame->name_len);
    }
    return NULL;
}

static char *ducknng_sql_frame_type_dup(const ducknng_frame *frame) {
    if (!frame) return NULL;
    switch (frame->type) {
    case DUCKNNG_RPC_MANIFEST: return ducknng_strdup("manifest");
    case DUCKNNG_RPC_CALL: return ducknng_strdup("call");
    case DUCKNNG_RPC_RESULT: return ducknng_strdup("result");
    case DUCKNNG_RPC_ERROR: return ducknng_strdup("error");
    case DUCKNNG_RPC_EVENT: return ducknng_strdup("event");
    default: return ducknng_strdup("unknown");
    }
}

static void ducknng_auth_context_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    const ducknng_authorizer_context *auth_ctx = NULL;
    ducknng_auth_context_bind_data *bind;
    duckdb_logical_type type;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_auth_context_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    auth_ctx = ducknng_runtime_current_thread_authorizer_context_get(ctx->rt);
    if (auth_ctx && auth_ctx->svc) {
        ducknng_transport_url parsed;
        char *parse_err = NULL;
        ducknng_transport_family family = auth_ctx->transport_family;
        ducknng_transport_scheme scheme = auth_ctx->scheme;
        ducknng_transport_url_init(&parsed);
        if ((family == DUCKNNG_TRANSPORT_FAMILY_UNKNOWN || scheme == DUCKNNG_TRANSPORT_SCHEME_UNKNOWN) &&
            auth_ctx->svc->listen_url && ducknng_transport_url_parse(auth_ctx->svc->listen_url, &parsed, &parse_err) == 0) {
            if (family == DUCKNNG_TRANSPORT_FAMILY_UNKNOWN) family = parsed.family;
            if (scheme == DUCKNNG_TRANSPORT_SCHEME_UNKNOWN) scheme = parsed.scheme;
        }
        if (parse_err) duckdb_free(parse_err);
        bind->row_count = 1;
        bind->phase = ducknng_strdup(auth_ctx->phase ? auth_ctx->phase : "rpc_request");
        bind->service_name = auth_ctx->svc->name ? ducknng_strdup(auth_ctx->svc->name) : NULL;
        bind->transport_family = ducknng_strdup(ducknng_transport_family_name(family));
        bind->scheme = ducknng_strdup(ducknng_transport_scheme_name(scheme));
        bind->listen = ducknng_service_resolved_listen(auth_ctx->svc) ? ducknng_strdup(ducknng_service_resolved_listen(auth_ctx->svc)) : NULL;
        bind->remote_addr = ducknng_sql_sockaddr_addr_dup(auth_ctx->remote_addr, &bind->remote_ip, &bind->remote_port);
        bind->tls_verified = auth_ctx->caller_identity && auth_ctx->caller_identity[0];
        bind->peer_identity = auth_ctx->caller_identity ? ducknng_strdup(auth_ctx->caller_identity) : NULL;
        bind->peer_allowlist_active = ducknng_service_peer_allowlist_active(auth_ctx->svc) ? true : false;
        bind->ip_allowlist_active = ducknng_service_ip_allowlist_active(auth_ctx->svc) ? true : false;
        bind->sql_authorizer_active = ducknng_service_authorizer_active(auth_ctx->svc) ? true : false;
        bind->http_method = auth_ctx->http_method ? ducknng_strdup(auth_ctx->http_method) : NULL;
        bind->http_path = auth_ctx->http_path ? ducknng_strdup(auth_ctx->http_path) : NULL;
        bind->content_type = auth_ctx->content_type ? ducknng_strdup(auth_ctx->content_type) : NULL;
        bind->body_bytes = auth_ctx->body_bytes;
        bind->rpc_method = ducknng_sql_frame_method_dup(auth_ctx->frame);
        bind->rpc_type = ducknng_sql_frame_type_dup(auth_ctx->frame);
        bind->payload_bytes = auth_ctx->frame ? auth_ctx->frame->payload_len : 0;
    }

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "phase", type);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "transport_family", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "tls_verified", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_identity", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "peer_allowlist_active", type);
    duckdb_bind_add_result_column(info, "ip_allowlist_active", type);
    duckdb_bind_add_result_column(info, "sql_authorizer_active", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "http_method", type);
    duckdb_bind_add_result_column(info, "http_path", type);
    duckdb_bind_add_result_column(info, "content_type", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "body_bytes", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "rpc_method", type);
    duckdb_bind_add_result_column(info, "rpc_type", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "payload_bytes", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_auth_context_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_auth_context_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sql_auth_single_row_init_data *init =
        (ducknng_sql_auth_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_auth_context_bind_data *bind = (ducknng_auth_context_bind_data *)duckdb_function_get_bind_data(info);
    if (!init || !bind || init->emitted || bind->row_count == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
#define ASSIGN_AUTH_STRING(IDX, VALUE) do { \
        if ((VALUE)) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, (IDX)), 0, (VALUE), (idx_t)strlen((VALUE))); \
        else set_null(duckdb_data_chunk_get_vector(output, (IDX)), 0); \
    } while (0)
    ASSIGN_AUTH_STRING(0, bind->phase);
    ASSIGN_AUTH_STRING(1, bind->service_name);
    ASSIGN_AUTH_STRING(2, bind->transport_family);
    ASSIGN_AUTH_STRING(3, bind->scheme);
    ASSIGN_AUTH_STRING(4, bind->listen);
    ASSIGN_AUTH_STRING(5, bind->remote_addr);
    ASSIGN_AUTH_STRING(6, bind->remote_ip);
    ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7)))[0] = bind->remote_port;
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8)))[0] = bind->tls_verified;
    ASSIGN_AUTH_STRING(9, bind->peer_identity);
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10)))[0] = bind->peer_allowlist_active;
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11)))[0] = bind->ip_allowlist_active;
    ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 12)))[0] = bind->sql_authorizer_active;
    ASSIGN_AUTH_STRING(13, bind->http_method);
    ASSIGN_AUTH_STRING(14, bind->http_path);
    ASSIGN_AUTH_STRING(15, bind->content_type);
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 16)))[0] = bind->body_bytes;
    ASSIGN_AUTH_STRING(17, bind->rpc_method);
    ASSIGN_AUTH_STRING(18, bind->rpc_type);
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 19)))[0] = bind->payload_bytes;
#undef ASSIGN_AUTH_STRING
    init->emitted = 1;
    duckdb_data_chunk_set_size(output, 1);
}

static void ducknng_set_service_authorizer_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ctx && ctx->rt && ducknng_runtime_current_thread_authorizer_context_get(ctx->rt)) {
        duckdb_scalar_function_set_error(info,
            "ducknng: ducknng client and lifecycle functions cannot run inside a SQL authorizer callback");
        return;
    }
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *authorizer_sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            if (authorizer_sql) duckdb_free(authorizer_sql);
            duckdb_scalar_function_set_error(info, "ducknng: service name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        for (i = 0; i < ctx->rt->service_count; i++) {
            if (ctx->rt->services[i] && ctx->rt->services[i]->name && strcmp(ctx->rt->services[i]->name, name) == 0) {
                svc = ctx->rt->services[i];
                break;
            }
        }
        if (!svc) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (authorizer_sql) duckdb_free(authorizer_sql);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_authorizer(svc, authorizer_sql, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (authorizer_sql) duckdb_free(authorizer_sql);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service SQL authorizer");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (authorizer_sql) duckdb_free(authorizer_sql);
        out[row] = true;
    }
}

static int register_auth_context_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 0, NULL, ducknng_auth_context_bind,
        ducknng_sql_auth_single_row_init, ducknng_auth_context_scan);
}

int ducknng_register_sql_auth(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type service_authorizer_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_authorizer", 2,
            ducknng_set_service_authorizer_scalar, ctx, service_authorizer_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!register_auth_context_table_named(con, ctx, "ducknng_auth_context")) return 0;
    return 1;
}
