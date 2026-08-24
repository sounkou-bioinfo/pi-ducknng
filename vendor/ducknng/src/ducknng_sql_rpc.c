#include "ducknng_sql_api.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_http_compat.h"
#include "ducknng_manifest.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_arrow.h"
#include "ducknng_transport.h"
#include "ducknng_service.h"
#include "ducknng_sql_shared.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <unistd.h>
#endif

DUCKDB_EXTENSION_EXTERN

static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg);

typedef struct {
    ducknng_sql_context *ctx;
    char *url;
    uint64_t tls_config_id;
    uint64_t session_id;
    char *session_token;
    int session_open;
    int close_attempted;
    int end_of_stream;
    struct ArrowSchema schema;
    struct ArrowArray array;
    idx_t row_count;
} ducknng_query_rpc_bind_data;

typedef struct {
    ducknng_query_rpc_bind_data *bind;
    idx_t offset;
} ducknng_query_rpc_init_data;

typedef struct {
    bool ok;
    char *error;
    char *manifest;
} ducknng_manifest_result_bind_data;

typedef struct {
    bool ok;
    char *error;
    uint64_t rows_changed;
    int32_t statement_type;
    int32_t result_type;
} ducknng_exec_result_bind_data;

typedef struct {
    bool ok;
    char *error;
    int has_nng_error;
    int32_t nng_error;
    uint8_t *payload;
    idx_t payload_len;
} ducknng_request_bind_data;

typedef struct {
    idx_t emitted;
} ducknng_single_row_init_data;
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

static const char *ducknng_rpc_type_name(uint8_t type) {
    switch (type) {
    case DUCKNNG_RPC_MANIFEST: return "manifest";
    case DUCKNNG_RPC_CALL: return "call";
    case DUCKNNG_RPC_RESULT: return "result";
    case DUCKNNG_RPC_ERROR: return "error";
    case DUCKNNG_RPC_EVENT: return "event";
    default: return "unknown";
    }
}

static char *ducknng_normalize_media_type(const char *content_type) {
    const char *start;
    const char *end;
    char *out;
    size_t len;
    size_t i;
    if (!content_type) return ducknng_strdup("");
    start = content_type;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') start++;
    end = start;
    while (*end && *end != ';' && *end != ' ' && *end != '\t' && *end != '\r' && *end != '\n') end++;
    while (end > start && (end[-1] == ' ' || end[-1] == '\t')) end--;
    len = (size_t)(end - start);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    for (i = 0; i < len; i++) out[i] = (char)ducknng_ascii_tolower_int((unsigned char)start[i]);
    out[len] = '\0';
    return out;
}

static const char *ducknng_json_find_key(const char *json, const char *key) {
    char needle[128];
    const char *p;
    if (!json || !key) return NULL;
    if (snprintf(needle, sizeof(needle), "\"%s\"", key) >= (int)sizeof(needle)) return NULL;
    p = strstr(json, needle);
    if (!p) return NULL;
    p = strchr(p + strlen(needle), ':');
    if (!p) return NULL;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

static int ducknng_json_extract_u64_value(const char *json, const char *key, uint64_t *out) {
    const char *p = ducknng_json_find_key(json, key);
    char *end = NULL;
    if (out) *out = 0;
    if (!p || !out) return -1;
    if (*p == '"') p++;
    *out = (uint64_t)strtoull(p, &end, 10);
    return end == p ? -1 : 0;
}

static char *ducknng_json_extract_string_dup(const char *json, const char *key) {
    const char *p = ducknng_json_find_key(json, key);
    const char *end;
    char *out;
    size_t len;
    if (!p || *p != '"') return NULL;
    p++;
    end = strchr(p, '"');
    if (!end) return NULL;
    len = (size_t)(end - p);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, p, len);
    out[len] = '\0';
    return out;
}

static void ducknng_query_rpc_reset_result(ducknng_query_rpc_bind_data *bind);
static int ducknng_query_rpc_close_session(ducknng_query_rpc_bind_data *bind);

static void destroy_query_rpc_bind_data(void *ptr) {
    ducknng_query_rpc_bind_data *data = (ducknng_query_rpc_bind_data *)ptr;
    if (!data) return;
    (void)ducknng_query_rpc_close_session(data);
    ducknng_query_rpc_reset_result(data);
    if (data->url) duckdb_free(data->url);
    if (data->session_token) duckdb_free(data->session_token);
    duckdb_free(data);
}

static void destroy_query_rpc_init_data(void *ptr) {
    ducknng_query_rpc_init_data *data = (ducknng_query_rpc_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void destroy_manifest_result_bind_data(void *ptr) {
    ducknng_manifest_result_bind_data *data = (ducknng_manifest_result_bind_data *)ptr;
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    if (data->manifest) duckdb_free(data->manifest);
    duckdb_free(data);
}

static void destroy_exec_result_bind_data(void *ptr) {
    ducknng_exec_result_bind_data *data = (ducknng_exec_result_bind_data *)ptr;
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    duckdb_free(data);
}

static void destroy_request_bind_data(void *ptr) {
    ducknng_request_bind_data *data = (ducknng_request_bind_data *)ptr;
    if (!data) return;
    if (data->error) duckdb_free(data->error);
    if (data->payload) duckdb_free(data->payload);
    duckdb_free(data);
}

static void destroy_single_row_init_data(void *ptr) {
    ducknng_single_row_init_data *data = (ducknng_single_row_init_data *)ptr;
    if (data) duckdb_free(data);
}

static int ducknng_start_service_with_tls_opts(ducknng_sql_context *ctx, const char *name, const char *listen,
    int contexts, uint64_t recv_max, uint64_t idle_ms, uint64_t tls_config_id,
    const char *tls_config_source, const ducknng_tls_opts *tls_opts,
    const char *ip_allowlist_json, char **errmsg) {
    ducknng_service *svc;
    if (!ctx || !ctx->rt || !name || !listen || !name[0] || !listen[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service name and listen URL must be non-empty");
        return -1;
    }
    if (contexts < 1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: contexts must be >= 1");
        return -1;
    }
    if (recv_max == 0 || idle_ms == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: recv_max_bytes and session_idle_ms must be > 0");
        return -1;
    }
    if (tls_opts && (tls_opts->auth_mode < 0 || tls_opts->auth_mode > 2)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: tls_auth_mode must be 0, 1, or 2");
        return -1;
    }
    svc = ducknng_service_create(ctx->rt, name, listen, contexts, (size_t)recv_max, idle_ms,
        tls_config_id, tls_config_source, tls_opts);
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to allocate service");
        return -1;
    }
    if (ip_allowlist_json && ip_allowlist_json[0] &&
        ducknng_service_set_ip_allowlist(svc, ip_allowlist_json, errmsg) != 0) {
        ducknng_service_destroy(svc);
        return -1;
    }
    if (ducknng_runtime_add_service(ctx->rt, svc, errmsg) != 0) {
        ducknng_service_destroy(svc);
        return -1;
    }
    if (ducknng_service_start(svc, errmsg) != 0) {
        ducknng_runtime_remove_service(ctx->rt, svc->name);
        ducknng_service_destroy(svc);
        return -1;
    }
    return 0;
}

static int ducknng_validate_service_start_url(const char *listen, const ducknng_tls_opts *tls_opts, char **errmsg) {
    ducknng_transport_url parsed;
    if (!listen || !listen[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: listen URL must be non-empty");
        return -1;
    }
    if (ducknng_transport_url_parse(listen, &parsed, errmsg) != 0) return -1;
    if (ducknng_transport_url_is_http(&parsed)) {
        return ducknng_validate_http_server_url(listen, tls_opts, errmsg);
    }
    return ducknng_listener_validate_startup_url(listen, tls_opts, errmsg);
}

static int ducknng_start_server_row(ducknng_sql_context *ctx, const char *name, const char *listen,
    int contexts, uint64_t recv_max, uint64_t idle_ms, uint64_t tls_config_id,
    const char *ip_allowlist_json, char **errmsg) {
    ducknng_transport_url parsed;
    uint64_t copied_tls_id = 0;
    char *tls_source = NULL;
    ducknng_tls_opts tls_opts_copy;
    int rc = -1;
    ducknng_tls_opts_init(&tls_opts_copy);
    if (tls_config_id != 0 &&
        ducknng_lookup_tls_config_copy(ctx, tls_config_id, &copied_tls_id, &tls_source, &tls_opts_copy, errmsg) != 0) {
        goto done;
    }
    if (ducknng_transport_url_parse(listen, &parsed, errmsg) != 0) goto done;
    if (ducknng_transport_url_is_http(&parsed) && contexts != 1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: contexts must be 1 for http:// and https:// services");
        goto done;
    }
    if (ducknng_validate_service_start_url(listen, tls_config_id != 0 ? &tls_opts_copy : NULL, errmsg) != 0) {
        goto done;
    }
    rc = ducknng_start_service_with_tls_opts(ctx, name, listen, contexts, recv_max, idle_ms,
        copied_tls_id, tls_source, tls_config_id != 0 ? &tls_opts_copy : NULL,
        ip_allowlist_json, errmsg);
done:
    ducknng_tls_opts_reset(&tls_opts_copy);
    if (tls_source) duckdb_free(tls_source);
    return rc;
}

static void ducknng_server_start_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *listen = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        int contexts = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 1);
        uint64_t recv_max = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 134217728ULL);
        uint64_t idle_ms = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 300000ULL);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0);
        char *ip_allowlist_json = ncols > 6 ? arg_varchar_dup(duckdb_data_chunk_get_vector(input, 6), row) : NULL;
        char *errmsg = NULL;
        if (ducknng_start_server_row(ctx, name, listen, contexts, recv_max, idle_ms, tls_config_id,
                ip_allowlist_json, &errmsg) != 0) {
            if (name) duckdb_free(name);
            if (listen) duckdb_free(listen);
            if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to start service");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (name) duckdb_free(name);
        if (listen) duckdb_free(listen);
        if (ip_allowlist_json) duckdb_free(ip_allowlist_json);
        out[row] = true;
    }
}

static void ducknng_server_stop_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        ducknng_service *svc;
        if (!ctx || !ctx->rt || !name) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: invalid stop arguments");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, name);
        if (!svc) {
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        /* Programming error: stop called from within this service's own
         * request handler thread (e.g. an authorizer calling stop on
         * itself).  This is always a thrown error. */
        if (ducknng_runtime_current_thread_request_service_get(ctx->rt) == svc) {
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: cannot stop a service from its own request handler");
            return;
        }
        /* Transient condition: some other thread has an inflight request or
         * is currently authorizing one (current_request_service_ptr is set
         * for the full duration of enter_request_sql, which covers both the
         * authorizer SQL and the method SQL).  Return false so the caller
         * can drain and retry rather than throwing an unrecoverable error. */
        if (ducknng_runtime_current_request_service_get(ctx->rt) == svc) {
            duckdb_free(name);
            out[row] = false;
            continue;
        }
        /* Belt-and-suspenders: also catch the window between begin_request
         * and the first enter_request_sql call (TLS handshake / NNG
         * dispatch latency before the authorizer acquires the DuckDB lane). */
        {
            int has_inflight = 0;
            if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
            has_inflight = (svc->inflight_request_count > 0);
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            if (has_inflight) {
                duckdb_free(name);
                out[row] = false;
                continue;
            }
        }
        svc = ducknng_runtime_remove_service(ctx->rt, name);
        duckdb_free(name);
        if (!svc) {
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        ducknng_service_stop(svc, NULL);
        ducknng_service_destroy(svc);
        out[row] = true;
    }
}

static void ducknng_set_service_peer_allowlist_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *identities_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            if (identities_json) duckdb_free(identities_json);
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
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_peer_allowlist(svc, identities_json, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service peer allowlist");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (identities_json) duckdb_free(identities_json);
        out[row] = true;
    }
}

static void ducknng_set_service_ip_allowlist_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *cidrs_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            if (cidrs_json) duckdb_free(cidrs_json);
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
            if (cidrs_json) duckdb_free(cidrs_json);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_ip_allowlist(svc, cidrs_json, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            if (cidrs_json) duckdb_free(cidrs_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service IP allowlist");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (cidrs_json) duckdb_free(cidrs_json);
        out[row] = true;
    }
}

static int ducknng_client_open_req_socket_tls(const char *url, int timeout_ms, const ducknng_tls_opts *tls_opts, nng_socket *out, char **errmsg);
static int ducknng_lookup_tls_opts(ducknng_sql_context *ctx, uint64_t tls_config_id, const ducknng_tls_opts **out_opts, char **errmsg);
static int ducknng_socket_is_active(const ducknng_client_socket *sock);
static int ducknng_socket_is_req_protocol(const ducknng_client_socket *sock);

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

static char *ducknng_frame_error_detail(const ducknng_frame *frame, const char *fallback) {
    char *detail;
    if (!frame || !frame->error || frame->error_len == 0) return ducknng_strdup(fallback);
    detail = (char *)duckdb_malloc((size_t)frame->error_len + 1);
    if (!detail) return ducknng_strdup("ducknng: out of memory decoding remote error");
    memcpy(detail, frame->error, (size_t)frame->error_len);
    detail[frame->error_len] = '\0';
    return detail;
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

static int ducknng_client_open_req_socket_tls(const char *url, int timeout_ms, const ducknng_tls_opts *tls_opts, nng_socket *out, char **errmsg) {
    int rv;
    if (!url || !url[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: client URL is required");
        return -1;
    }
    if (ducknng_socket_validate_client_url(url, tls_opts, errmsg) != 0) return -1;
    rv = ducknng_req_socket_open(out);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    rv = ducknng_socket_set_timeout_ms(*out, timeout_ms, timeout_ms);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    rv = ducknng_socket_apply_tls(*out, url, tls_opts);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    rv = ducknng_socket_dial(*out, url);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    return 0;
}

static int ducknng_client_open_req_socket(const char *url, int timeout_ms, nng_socket *out, char **errmsg) {
    return ducknng_client_open_req_socket_tls(url, timeout_ms, NULL, out, errmsg);
}

static nng_msg *ducknng_client_roundtrip_tls(const char *url, nng_msg *req, int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg, int *out_nng_error) {
    ducknng_transport_url parsed;
    nng_socket sock;
    nng_msg *resp = NULL;
    uint8_t *reply_frame = NULL;
    size_t reply_frame_len = 0;
    int rv;
    memset(&sock, 0, sizeof(sock));
    if (out_nng_error) *out_nng_error = 0;
    if (!req) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: request message is required");
        return NULL;
    }
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    if (ducknng_transport_url_is_http(&parsed)) {
        if (ducknng_http_frame_transact(url, (const uint8_t *)nng_msg_body(req), nng_msg_len(req),
                timeout_ms, tls_opts, &reply_frame, &reply_frame_len, errmsg) != 0) {
            nng_msg_free(req);
            return NULL;
        }
        nng_msg_free(req);
        rv = nng_msg_alloc(&resp, reply_frame_len);
        if (rv != 0) {
            if (reply_frame) duckdb_free(reply_frame);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
            return NULL;
        }
        if (reply_frame_len) memcpy(nng_msg_body(resp), reply_frame, reply_frame_len);
        if (reply_frame) duckdb_free(reply_frame);
        return resp;
    }
    if (ducknng_client_open_req_socket_tls(url, timeout_ms, tls_opts, &sock, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    rv = ducknng_req_transact(sock, req, &resp);
    ducknng_socket_close(sock);
    if (rv != 0) {
        if (out_nng_error) *out_nng_error = rv;
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    return resp;
}

static nng_msg *ducknng_client_roundtrip(const char *url, nng_msg *req, int timeout_ms, char **errmsg) {
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, NULL, errmsg, NULL);
}

static nng_msg *ducknng_client_roundtrip_raw_tls(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg, int *out_nng_error) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg, out_nng_error);
}

static nng_msg *ducknng_client_roundtrip_raw(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip(url, req, timeout_ms, errmsg);
}

static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (out_id) *out_id = 0;
    if (out_source) *out_source = NULL;
    if (out_opts) ducknng_tls_opts_init(out_opts);
    if (tls_config_id == 0) return 0;
    if (!ctx || !ctx->rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for TLS lookup");
        return -1;
    }
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
    if (ducknng_lookup_tls_config_copy(ctx, tls_config_id, NULL, NULL, &tls_copy, errmsg) != 0) {
        return -1;
    }
    tls_copy_valid = 1;
    if (out_opts) *out_opts = &tls_copy;
    return 0;
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

static nng_msg *ducknng_client_method_roundtrip_tls(const char *url, const char *method_name,
    const void *payload, size_t payload_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    char **errmsg) {
    nng_msg *req = ducknng_client_method_request(method_name, payload, payload_len, errmsg);
    if (!req) return NULL;
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg, NULL);
}

static void ducknng_query_rpc_reset_result(ducknng_query_rpc_bind_data *bind) {
    if (!bind) return;
    if (bind->array.release) ArrowArrayRelease(&bind->array);
    if (bind->schema.release) ArrowSchemaRelease(&bind->schema);
    memset(&bind->array, 0, sizeof(bind->array));
    memset(&bind->schema, 0, sizeof(bind->schema));
    bind->row_count = 0;
}

static int ducknng_query_rpc_open_session(ducknng_query_rpc_bind_data *bind, const char *sql,
    char **errmsg) {
    const ducknng_tls_opts *tls_opts = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    nng_msg *resp_msg = NULL;
    ducknng_frame frame;
    char *json = NULL;
    uint64_t session_id = 0;
    char *session_token = NULL;
    int rc = -1;
    if (!bind || !bind->ctx || !bind->url || !sql || !sql[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: query_rpc requires non-empty url and sql");
        return -1;
    }
    if (ducknng_lookup_tls_opts(bind->ctx, bind->tls_config_id, &tls_opts, errmsg) != 0) goto cleanup;
    if (ducknng_query_open_request_to_ipc(sql, 0, 0, &payload, &payload_len, errmsg) != 0) goto cleanup;
    resp_msg = ducknng_client_method_roundtrip_tls(bind->url, "query_open", payload, payload_len,
        5000, tls_opts, errmsg);
    if (!resp_msg) goto cleanup;
    if (ducknng_decode_request(resp_msg, &frame) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid query_open response envelope");
        goto cleanup;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        if (errmsg && !*errmsg) *errmsg = ducknng_frame_error_detail(&frame, "ducknng: query_open failed");
        goto cleanup;
    }
    if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON)) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: query_open did not return a JSON control reply");
        goto cleanup;
    }
    json = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
    if (!json) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying query_open reply");
        goto cleanup;
    }
    if (ducknng_json_extract_u64_value(json, "session_id", &session_id) != 0 || session_id == 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: query_open reply did not include session_id");
        goto cleanup;
    }
    session_token = ducknng_json_extract_string_dup(json, "session_token");
    if (!session_token || !session_token[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: query_open reply did not include session_token");
        goto cleanup;
    }
    if (bind->session_token) duckdb_free(bind->session_token);
    bind->session_id = session_id;
    bind->session_token = session_token;
    bind->session_open = 1;
    bind->close_attempted = 0;
    session_token = NULL;
    rc = 0;
cleanup:
    if (session_token) duckdb_free(session_token);
    if (json) duckdb_free(json);
    if (resp_msg) nng_msg_free(resp_msg);
    if (payload) duckdb_free(payload);
    return rc;
}

static int ducknng_query_rpc_fetch_batch(ducknng_query_rpc_bind_data *bind, char **errmsg) {
    const ducknng_tls_opts *tls_opts = NULL;
    char *json = NULL;
    nng_msg *resp_msg = NULL;
    ducknng_frame frame;
    int rc = -1;
    if (!bind || !bind->ctx || !bind->url || bind->session_id == 0 ||
        !bind->session_token || !bind->session_token[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing query_rpc session state");
        return -1;
    }
    if (ducknng_lookup_tls_opts(bind->ctx, bind->tls_config_id, &tls_opts, errmsg) != 0) goto cleanup;
    json = ducknng_session_request_json(bind->session_id, bind->session_token, 0, 0);
    if (!json) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: failed to build fetch request payload");
        goto cleanup;
    }
    resp_msg = ducknng_client_method_roundtrip_tls(bind->url, "fetch", json, strlen(json),
        5000, tls_opts, errmsg);
    if (!resp_msg) goto cleanup;
    if (ducknng_decode_request(resp_msg, &frame) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid fetch response envelope");
        goto cleanup;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        if (errmsg && !*errmsg) *errmsg = ducknng_frame_error_detail(&frame, "ducknng: fetch failed");
        goto cleanup;
    }
    ducknng_query_rpc_reset_result(bind);
    bind->end_of_stream = (frame.flags & DUCKNNG_RPC_FLAG_END_OF_STREAM) != 0;
    if ((frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM) &&
        (frame.flags & DUCKNNG_RPC_FLAG_RESULT_ROWS)) {
        if (ducknng_decode_ipc_table_payload(frame.payload, (size_t)frame.payload_len,
                &bind->schema, &bind->array, errmsg) != 0) {
            goto cleanup;
        }
        bind->row_count = (idx_t)bind->array.length;
        if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
            ducknng_query_rpc_reset_result(bind);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid fetch Arrow row schema");
            goto cleanup;
        }
        rc = 0;
        goto cleanup;
    }
    if ((frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON) && bind->end_of_stream) {
        rc = 0;
        goto cleanup;
    }
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: fetch returned an unexpected reply shape");
cleanup:
    if (resp_msg) nng_msg_free(resp_msg);
    if (json) duckdb_free(json);
    return rc;
}

static int ducknng_query_rpc_close_session(ducknng_query_rpc_bind_data *bind) {
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    char *json = NULL;
    nng_msg *resp_msg = NULL;
    if (!bind || bind->close_attempted) return 0;
    bind->close_attempted = 1;
    if (!bind->session_open || !bind->ctx || !bind->url || bind->session_id == 0 ||
        !bind->session_token || !bind->session_token[0]) {
        bind->session_open = 0;
        return 0;
    }
    if (ducknng_lookup_tls_opts(bind->ctx, bind->tls_config_id, &tls_opts, &errmsg) == 0) {
        json = ducknng_session_request_json(bind->session_id, bind->session_token, 0, 0);
        if (json) {
            resp_msg = ducknng_client_method_roundtrip_tls(bind->url, "close", json, strlen(json),
                5000, tls_opts, &errmsg);
        }
    }
    bind->session_open = 0;
    if (resp_msg) nng_msg_free(resp_msg);
    if (json) duckdb_free(json);
    if (errmsg) duckdb_free(errmsg);
    return 0;
}

static int ducknng_assign_local_error_frame(duckdb_function_info info, duckdb_vector output,
    idx_t row, const char *name, const char *message);

static void ducknng_get_rpc_manifest_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        char *errmsg = NULL;
        nng_msg *resp_msg;
        if (!url || ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            if (url) duckdb_free(url);
            if (ducknng_assign_local_error_frame(info, output, row, "manifest",
                    errmsg ? errmsg : "ducknng: manifest URL must not be NULL or empty") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        resp_msg = ducknng_client_roundtrip_tls(url, ducknng_client_manifest_request(), 5000, tls_opts, &errmsg, NULL);
        duckdb_free(url);
        if (!resp_msg) {
            if (ducknng_assign_local_error_frame(info, output, row, "manifest",
                    errmsg ? errmsg : "ducknng: manifest request failed") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp_msg), (idx_t)nng_msg_len(resp_msg));
        nng_msg_free(resp_msg);
    }
}

static void ducknng_run_rpc_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        char *errmsg = NULL;
        nng_msg *req = NULL;
        nng_msg *resp_msg;
        if (!url || !sql || ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            if (url) duckdb_free(url);
            if (sql) duckdb_free(sql);
            if (ducknng_assign_local_error_frame(info, output, row, "exec",
                    errmsg ? errmsg : "ducknng: run_rpc_raw requires non-null url and sql") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        req = ducknng_client_exec_request(sql, 0, &errmsg);
        duckdb_free(sql);
        if (!req) {
            duckdb_free(url);
            if (ducknng_assign_local_error_frame(info, output, row, "exec",
                    errmsg ? errmsg : "ducknng: failed to build exec request frame") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        resp_msg = ducknng_client_roundtrip_tls(url, req, 5000, tls_opts, &errmsg, NULL);
        duckdb_free(url);
        if (!resp_msg) {
            if (ducknng_assign_local_error_frame(info, output, row, "exec",
                    errmsg ? errmsg : "ducknng: remote exec request failed") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp_msg), (idx_t)nng_msg_len(resp_msg));
        nng_msg_free(resp_msg);
    }
}

static int ducknng_assign_method_roundtrip_blob(duckdb_function_info info, duckdb_vector output, idx_t row,
    ducknng_sql_context *ctx, const char *url, const char *method_name,
    const void *payload, size_t payload_len, uint64_t tls_config_id) {
    const ducknng_tls_opts *tls_opts = NULL;
    nng_msg *resp_msg = NULL;
    char *errmsg = NULL;
    if (!ctx || !ctx->rt || !url || !method_name ||
        ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        if (ducknng_assign_local_error_frame(info, output, row, method_name,
                errmsg ? errmsg : "ducknng: raw RPC helper requires valid runtime, URL, and method") != 0) {
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    resp_msg = ducknng_client_method_roundtrip_tls(url, method_name, payload, payload_len,
        5000, tls_opts, &errmsg);
    if (!resp_msg) {
        if (ducknng_assign_local_error_frame(info, output, row, method_name,
                errmsg ? errmsg : "ducknng: raw RPC request failed") != 0) {
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    assign_blob(output, row, (const uint8_t *)nng_msg_body(resp_msg), (idx_t)nng_msg_len(resp_msg));
    nng_msg_free(resp_msg);
    if (errmsg) duckdb_free(errmsg);
    return 0;
}

static void ducknng_open_query_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        uint64_t batch_rows = arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0);
        uint64_t batch_bytes = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0);
        uint8_t *payload = NULL;
        size_t payload_len = 0;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !url || !sql || !url[0] || !sql[0]) {
            if (url) duckdb_free(url);
            if (sql) duckdb_free(sql);
            if (ducknng_assign_local_error_frame(info, output, row, "query_open",
                    "ducknng: open_query_raw requires non-empty url and sql") != 0) return;
            continue;
        }
        if (ducknng_query_open_request_to_ipc(sql, batch_rows, batch_bytes, &payload, &payload_len, &errmsg) != 0) {
            duckdb_free(url);
            duckdb_free(sql);
            if (payload) duckdb_free(payload);
            if (ducknng_assign_local_error_frame(info, output, row, "query_open",
                    errmsg ? errmsg : "ducknng: failed to encode query_open request payload") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        (void)ducknng_assign_method_roundtrip_blob(info, output, row, ctx, url, "query_open",
            payload, payload_len, tls_config_id);
        duckdb_free(url);
        duckdb_free(sql);
        duckdb_free(payload);
        if (errmsg) duckdb_free(errmsg);
    }
}

static void ducknng_fetch_query_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t session_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        char *session_token = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        uint64_t batch_rows = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        uint64_t batch_bytes = arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0);
        char *payload = NULL;
        if (!ctx || !ctx->rt || !url || !url[0] || session_id == 0 || !session_token || !session_token[0]) {
            if (url) duckdb_free(url);
            if (session_token) duckdb_free(session_token);
            if (ducknng_assign_local_error_frame(info, output, row, "fetch",
                    "ducknng: fetch_query_raw requires non-empty url, session_id, and session_token") != 0) return;
            continue;
        }
        payload = ducknng_session_request_json(session_id, session_token, batch_rows, batch_bytes);
        if (!payload) {
            duckdb_free(url);
            duckdb_free(session_token);
            if (ducknng_assign_local_error_frame(info, output, row, "fetch",
                    "ducknng: failed to build fetch request payload") != 0) return;
            continue;
        }
        (void)ducknng_assign_method_roundtrip_blob(info, output, row, ctx, url, "fetch",
            payload, strlen(payload), tls_config_id);
        duckdb_free(url);
        duckdb_free(session_token);
        duckdb_free(payload);
    }
}

static void ducknng_close_query_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t session_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        char *session_token = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        char *payload = NULL;
        if (!ctx || !ctx->rt || !url || !url[0] || session_id == 0 || !session_token || !session_token[0]) {
            if (url) duckdb_free(url);
            if (session_token) duckdb_free(session_token);
            if (ducknng_assign_local_error_frame(info, output, row, "close",
                    "ducknng: close_query_raw requires non-empty url, session_id, and session_token") != 0) return;
            continue;
        }
        payload = ducknng_session_request_json(session_id, session_token, 0, 0);
        if (!payload) {
            duckdb_free(url);
            duckdb_free(session_token);
            if (ducknng_assign_local_error_frame(info, output, row, "close",
                    "ducknng: failed to build close request payload") != 0) return;
            continue;
        }
        (void)ducknng_assign_method_roundtrip_blob(info, output, row, ctx, url, "close",
            payload, strlen(payload), tls_config_id);
        duckdb_free(url);
        duckdb_free(session_token);
        duckdb_free(payload);
    }
}

static void ducknng_cancel_query_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *url = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t session_id = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        char *session_token = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        char *payload = NULL;
        if (!ctx || !ctx->rt || !url || !url[0] || session_id == 0 || !session_token || !session_token[0]) {
            if (url) duckdb_free(url);
            if (session_token) duckdb_free(session_token);
            if (ducknng_assign_local_error_frame(info, output, row, "cancel",
                    "ducknng: cancel_query_raw requires non-empty url, session_id, and session_token") != 0) return;
            continue;
        }
        payload = ducknng_session_request_json(session_id, session_token, 0, 0);
        if (!payload) {
            duckdb_free(url);
            duckdb_free(session_token);
            if (ducknng_assign_local_error_frame(info, output, row, "cancel",
                    "ducknng: failed to build cancel request payload") != 0) return;
            continue;
        }
        (void)ducknng_assign_method_roundtrip_blob(info, output, row, ctx, url, "cancel",
            payload, strlen(payload), tls_config_id);
        duckdb_free(url);
        duckdb_free(session_token);
        duckdb_free(payload);
    }
}

static int ducknng_socket_is_active(const ducknng_client_socket *sock) {
    return sock && sock->open && (sock->connected || sock->has_listener);
}

static int ducknng_socket_is_req_protocol(const ducknng_client_socket *sock) {
    return sock && sock->protocol && strcmp(sock->protocol, "req") == 0;
}

static int ducknng_assign_local_error_frame(duckdb_function_info info, duckdb_vector output,
    idx_t row, const char *name, const char *message) {
    nng_msg *err = ducknng_error_msg(name ? name : "transport", DUCKNNG_STATUS_INTERNAL,
        message ? message : "ducknng: request failed");
    if (!err) {
        duckdb_scalar_function_set_error(info, "ducknng: failed to build local error frame");
        return -1;
    }
    assign_blob(output, row, (const uint8_t *)nng_msg_body(err), (idx_t)nng_msg_len(err));
    nng_msg_free(err);
    return 0;
}

static void ducknng_request_raw_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        duckdb_vector url_vec = duckdb_data_chunk_get_vector(input, 0);
        duckdb_vector payload_vec = duckdb_data_chunk_get_vector(input, 1);
        int url_is_null = ducknng_sql_arg_is_null(url_vec, row);
        int payload_is_null = ducknng_sql_arg_is_null(payload_vec, row);
        char *url = arg_varchar_dup(url_vec, row);
        idx_t payload_len = 0;
        uint8_t *payload = arg_blob_dup(payload_vec, row, &payload_len);
        int32_t timeout_ms = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 5000);
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0);
        const ducknng_tls_opts *tls_opts = NULL;
        nng_msg *resp = NULL;
        char *errmsg = NULL;
        if (url_is_null || payload_is_null || !url || !url[0]) {
            if (url) duckdb_free(url);
            if (payload) duckdb_free(payload);
            if (ducknng_assign_local_error_frame(info, output, row, "transport",
                    "ducknng: request_raw requires non-null url and payload") != 0) return;
            continue;
        }
        if (!payload && payload_len > 0) {
            duckdb_free(url);
            if (ducknng_assign_local_error_frame(info, output, row, "transport",
                    "ducknng: out of memory copying request payload") != 0) return;
            continue;
        }
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
            duckdb_free(url);
            if (payload) duckdb_free(payload);
            if (ducknng_assign_local_error_frame(info, output, row, "transport",
                    errmsg ? errmsg : "ducknng: tls config not found") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        resp = ducknng_client_roundtrip_raw_tls(url, payload, (size_t)payload_len, timeout_ms, tls_opts, &errmsg, NULL);
        duckdb_free(url);
        if (payload) duckdb_free(payload);
        if (!resp) {
            if (ducknng_assign_local_error_frame(info, output, row, "transport",
                    errmsg ? errmsg : "ducknng: request failed") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp), (idx_t)nng_msg_len(resp));
        nng_msg_free(resp);
    }
}

static void ducknng_request_socket_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
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
        nng_msg *resp = NULL;
        char *errmsg = NULL;
        int rv;
        if (!ctx || !ctx->rt || socket_id == 0 || (!payload && payload_len > 0)) {
            if (payload) duckdb_free(payload);
            if (ducknng_assign_local_error_frame(info, output, row, "transport",
                    "ducknng: request_socket_raw requires socket id and payload") != 0) return;
            continue;
        }
        sock = ducknng_runtime_acquire_client_socket(ctx->rt, socket_id);
        if (!sock || !sock->open || !sock->connected || !ducknng_socket_is_req_protocol(sock)) {
            if (payload) duckdb_free(payload);
            if (sock) ducknng_runtime_release_client_socket(sock);
            if (ducknng_assign_local_error_frame(info, output, row, "transport",
                    "ducknng: connected req client socket not found") != 0) return;
            continue;
        }
        {
            nng_msg *req = ducknng_client_raw_request_message(payload, (size_t)payload_len, &errmsg);
            int transact_called = 0;
            if (payload) duckdb_free(payload);
            payload = NULL;
            if (!req) {
                if (sock) ducknng_runtime_release_client_socket(sock);
                if (ducknng_assign_local_error_frame(info, output, row, "transport",
                        errmsg ? errmsg : "ducknng: failed to build socket request frame") != 0) {
                    if (errmsg) duckdb_free(errmsg);
                    return;
                }
                if (errmsg) duckdb_free(errmsg);
                continue;
            }
            ducknng_mutex_lock(&sock->mu);
            rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
            if (rv == 0) {
                transact_called = 1;
                rv = ducknng_req_transact(sock->sock, req, &resp);
            }
            if (rv == 0) {
                sock->send_timeout_ms = timeout_ms;
                sock->recv_timeout_ms = timeout_ms;
            }
            ducknng_mutex_unlock(&sock->mu);
            ducknng_runtime_release_client_socket(sock);
            if (rv != 0) {
                if (!transact_called) nng_msg_free(req);
                if (ducknng_assign_local_error_frame(info, output, row, "transport",
                        ducknng_nng_strerror(rv)) != 0) return;
                continue;
            }
        }
        if (!resp) {
            if (ducknng_assign_local_error_frame(info, output, row, "transport",
                    errmsg ? errmsg : "ducknng: request failed") != 0) {
                if (errmsg) duckdb_free(errmsg);
                return;
            }
            if (errmsg) duckdb_free(errmsg);
            continue;
        }
        assign_blob(output, row, (const uint8_t *)nng_msg_body(resp), (idx_t)nng_msg_len(resp));
        nng_msg_free(resp);
    }
}

static void ducknng_query_rpc_bind(duckdb_bind_info info) {
    ducknng_query_rpc_bind_data *bind;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value tls_val;
    char *sql;
    char *errmsg = NULL;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    if (duckdb_bind_get_parameter_count(info) != 3) {
        duckdb_bind_set_error(info, "ducknng: ducknng_query_rpc(url, sql, tls_config_id) requires exactly three parameters");
        return;
    }
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    tls_val = duckdb_bind_get_parameter(info, 2);
    bind = (ducknng_query_rpc_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_destroy_value(&url_val);
        duckdb_destroy_value(&sql_val);
        duckdb_destroy_value(&tls_val);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    bind->url = duckdb_get_varchar(url_val);
    sql = duckdb_get_varchar(sql_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !sql || !bind->url[0] || !sql[0]) {
        if (sql) duckdb_free(sql);
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: ducknng_query_rpc(url, sql, tls_config_id) requires non-empty url and sql");
        return;
    }
    if (ducknng_query_rpc_open_session(bind, sql, &errmsg) != 0) {
        duckdb_free(sql);
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: query_open failed");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    duckdb_free(sql);
    if (ducknng_query_rpc_fetch_batch(bind, &errmsg) != 0) {
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: fetch failed");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    if (!bind->schema.release || bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: query_rpc could not infer result columns from the first fetch reply");
        return;
    }
    if (ducknng_sql_arrow_bind_result_columns(info, &bind->schema, &errmsg) != 0) {
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported remote Arrow type");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    duckdb_bind_set_bind_data(info, bind, destroy_query_rpc_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_query_rpc_init(duckdb_init_info info) {
    ducknng_query_rpc_bind_data *bind = (ducknng_query_rpc_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_query_rpc_init_data *init = (ducknng_query_rpc_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_query_rpc_init_data);
}

static void ducknng_query_rpc_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_query_rpc_init_data *init = (ducknng_query_rpc_init_data *)duckdb_function_get_init_data(info);
    ducknng_query_rpc_bind_data *bind;
    char *errmsg = NULL;
    if (!init || !init->bind) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    while (init->offset >= bind->row_count) {
        if (bind->end_of_stream) {
            (void)ducknng_query_rpc_close_session(bind);
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
        if (ducknng_query_rpc_fetch_batch(bind, &errmsg) != 0) {
            duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to fetch the next query batch");
            if (errmsg) duckdb_free(errmsg);
            (void)ducknng_query_rpc_close_session(bind);
            return;
        }
        init->offset = 0;
        if (bind->row_count == 0 && bind->end_of_stream) {
            (void)ducknng_query_rpc_close_session(bind);
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
    }
    if (ducknng_sql_arrow_scan_table(output, &bind->schema, &bind->array, bind->row_count,
            &init->offset, &errmsg) != 0) {
        duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to decode remote Arrow row payload");
        if (errmsg) duckdb_free(errmsg);
        (void)ducknng_query_rpc_close_session(bind);
        return;
    }
    if (init->offset >= bind->row_count && bind->end_of_stream) {
        (void)ducknng_query_rpc_close_session(bind);
    }
}

static void ducknng_single_row_init(duckdb_init_info info) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_single_row_init_data);
}

static void ducknng_get_rpc_manifest_bind(duckdb_bind_info info) {
    ducknng_manifest_result_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value tls_val;
    char *url;
    uint64_t tls_config_id;
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    ducknng_frame frame;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_manifest_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    url_val = duckdb_bind_get_parameter(info, 0);
    tls_val = duckdb_bind_get_parameter(info, 1);
    url = duckdb_get_varchar(url_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&tls_val);
    if (!url || !url[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: remote manifest URL must not be NULL or empty");
    } else if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        errmsg = NULL;
    } else {
        resp_msg = ducknng_client_roundtrip_tls(url, ducknng_client_manifest_request(), 5000, tls_opts, &errmsg, NULL);
        if (!resp_msg) {
            bind->ok = false;
            bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: manifest request failed");
            errmsg = NULL;
        } else if (ducknng_decode_request(resp_msg, &frame) != 0) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: invalid manifest response envelope");
        } else if (frame.type == DUCKNNG_RPC_ERROR) {
            bind->ok = false;
            bind->error = ducknng_frame_error_detail(&frame, "ducknng: manifest request failed");
        } else if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON)) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: manifest response was not JSON result payload");
        } else {
            bind->ok = true;
            bind->manifest = (char *)duckdb_malloc((size_t)frame.payload_len + 1);
            if (!bind->manifest) {
                bind->ok = false;
                bind->error = ducknng_strdup("ducknng: out of memory copying manifest payload");
            } else {
                memcpy(bind->manifest, frame.payload, (size_t)frame.payload_len);
                bind->manifest[frame.payload_len] = '\0';
            }
        }
    }
    if (resp_msg) nng_msg_free(resp_msg);
    if (url) duckdb_free(url);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_bind_add_result_column(info, "manifest", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_manifest_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_get_rpc_manifest_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_manifest_result_bind_data *bind = (ducknng_manifest_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, bind->error, (idx_t)strlen(bind->error));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    if (bind->manifest) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 2), 0, bind->manifest, (idx_t)strlen(bind->manifest));
    else set_null(duckdb_data_chunk_get_vector(output, 2), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_run_rpc_bind(duckdb_bind_info info) {
    ducknng_exec_result_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value tls_val;
    char *url;
    char *sql;
    uint64_t tls_config_id;
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    ducknng_frame frame;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_exec_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    tls_val = duckdb_bind_get_parameter(info, 2);
    url = duckdb_get_varchar(url_val);
    sql = duckdb_get_varchar(sql_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&tls_val);
    if (!url || !url[0] || !sql || !sql[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: remote exec URL and SQL must not be NULL or empty");
    } else if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        errmsg = NULL;
    } else {
        resp_msg = ducknng_client_roundtrip_tls(url, ducknng_client_exec_request(sql, 0, &errmsg), 5000, tls_opts, &errmsg, NULL);
        if (!resp_msg) {
            bind->ok = false;
            bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: remote exec request failed");
            errmsg = NULL;
        } else if (ducknng_decode_request(resp_msg, &frame) != 0) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: invalid remote exec response envelope");
        } else if (frame.type == DUCKNNG_RPC_ERROR) {
            bind->ok = false;
            bind->error = ducknng_frame_error_detail(&frame, "ducknng: remote exec request failed");
        } else if (frame.type != DUCKNNG_RPC_RESULT || !(frame.flags & DUCKNNG_RPC_FLAG_RESULT_METADATA)) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: remote exec expected metadata reply");
        } else if (ducknng_decode_exec_metadata_payload(frame.payload, (size_t)frame.payload_len,
                &bind->rows_changed, (uint32_t *)&bind->statement_type, (uint32_t *)&bind->result_type, &errmsg) != 0) {
            bind->ok = false;
            bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to decode remote exec metadata");
            errmsg = NULL;
        } else {
            bind->ok = true;
        }
    }
    if (resp_msg) nng_msg_free(resp_msg);
    if (url) duckdb_free(url);
    if (sql) duckdb_free(sql);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "rows_changed", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "statement_type", type);
    duckdb_bind_add_result_column(info, "result_type", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_exec_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_run_rpc_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_exec_result_bind_data *bind = (ducknng_exec_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    uint64_t *rows_changed;
    int32_t *statement_type;
    int32_t *result_type;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    rows_changed = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    statement_type = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    result_type = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, bind->error, (idx_t)strlen(bind->error));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    rows_changed[0] = bind->rows_changed;
    statement_type[0] = bind->statement_type;
    result_type[0] = bind->result_type;
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_request_bind_common(ducknng_request_bind_data *bind, const char *url,
    const uint8_t *payload, size_t payload_len, int32_t timeout_ms, const ducknng_tls_opts *tls_opts) {
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    int nng_err = 0;
    if (!url || !url[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: request URL must not be NULL or empty");
        return;
    }
    resp_msg = ducknng_client_roundtrip_raw_tls(url, payload, payload_len, timeout_ms, tls_opts, &errmsg, &nng_err);
    if (!resp_msg) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: request failed");
        if (nng_err != 0) {
            bind->has_nng_error = 1;
            bind->nng_error = (int32_t)nng_err;
        }
        return;
    }
    bind->payload_len = (idx_t)nng_msg_len(resp_msg);
    bind->payload = (uint8_t *)duckdb_malloc((size_t)bind->payload_len);
    if (!bind->payload && bind->payload_len > 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: out of memory copying reply payload");
        nng_msg_free(resp_msg);
        return;
    }
    if (bind->payload_len) memcpy(bind->payload, nng_msg_body(resp_msg), (size_t)bind->payload_len);
    bind->ok = true;
    nng_msg_free(resp_msg);
}

static void ducknng_request_bind_common_socket(ducknng_request_bind_data *bind, ducknng_client_socket *sock,
    const uint8_t *payload, size_t payload_len, int32_t timeout_ms) {
    char *errmsg = NULL;
    nng_msg *req_msg = NULL;
    nng_msg *resp_msg = NULL;
    int rv;
    if (!bind || !sock || !sock->open || !sock->connected || !ducknng_socket_is_req_protocol(sock)) {
        if (bind) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: connected req client socket not found");
        }
        return;
    }
    req_msg = ducknng_client_raw_request_message(payload, payload_len, &errmsg);
    if (!req_msg) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to build socket request frame");
        return;
    }
    rv = ducknng_socket_set_timeout_ms(sock->sock, timeout_ms, timeout_ms);
    if (rv != 0) {
        nng_msg_free(req_msg);
        bind->ok = false;
        bind->has_nng_error = 1;
        bind->nng_error = (int32_t)rv;
        bind->error = ducknng_strdup(ducknng_nng_strerror(rv));
        return;
    }
    rv = ducknng_req_transact(sock->sock, req_msg, &resp_msg);
    if (rv != 0) {
        bind->ok = false;
        bind->has_nng_error = 1;
        bind->nng_error = (int32_t)rv;
        bind->error = ducknng_strdup(ducknng_nng_strerror(rv));
        return;
    }
    bind->payload_len = (idx_t)nng_msg_len(resp_msg);
    bind->payload = (uint8_t *)duckdb_malloc((size_t)bind->payload_len);
    if (!bind->payload && bind->payload_len > 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: out of memory copying reply payload");
        nng_msg_free(resp_msg);
        return;
    }
    if (bind->payload_len) memcpy(bind->payload, nng_msg_body(resp_msg), (size_t)bind->payload_len);
    sock->send_timeout_ms = timeout_ms;
    sock->recv_timeout_ms = timeout_ms;
    bind->ok = true;
    nng_msg_free(resp_msg);
}

static void ducknng_request_bind(duckdb_bind_info info) {
    ducknng_request_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value payload_val;
    duckdb_value timeout_val;
    duckdb_value tls_val;
    duckdb_blob blob;
    char *url;
    int32_t timeout_ms;
    uint64_t tls_config_id;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    bind = (ducknng_request_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    url_val = duckdb_bind_get_parameter(info, 0);
    payload_val = duckdb_bind_get_parameter(info, 1);
    timeout_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    url = duckdb_get_varchar(url_val);
    blob = duckdb_get_blob(payload_val);
    timeout_ms = duckdb_get_int32(timeout_val);
    tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&payload_val);
    duckdb_destroy_value(&timeout_val);
    duckdb_destroy_value(&tls_val);
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
    } else {
        ducknng_request_bind_common(bind, url, (const uint8_t *)blob.data, (size_t)blob.size, timeout_ms, tls_opts);
    }
    if (url) duckdb_free(url);
    if (blob.data) duckdb_free(blob.data);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "nng_error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "nng_error_message", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_request_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_request_socket_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    ducknng_request_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value socket_val;
    duckdb_value payload_val;
    duckdb_value timeout_val;
    duckdb_blob blob;
    uint64_t socket_id;
    int32_t timeout_ms;
    ducknng_client_socket *sock;
    bind = (ducknng_request_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    socket_val = duckdb_bind_get_parameter(info, 0);
    payload_val = duckdb_bind_get_parameter(info, 1);
    timeout_val = duckdb_bind_get_parameter(info, 2);
    socket_id = duckdb_get_int64(socket_val);
    blob = duckdb_get_blob(payload_val);
    timeout_ms = duckdb_get_int32(timeout_val);
    duckdb_destroy_value(&socket_val);
    duckdb_destroy_value(&payload_val);
    duckdb_destroy_value(&timeout_val);
    sock = ctx && ctx->rt ? ducknng_runtime_acquire_client_socket(ctx->rt, socket_id) : NULL;
    if (!sock || !sock->open || !sock->connected) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: connected client socket not found");
    } else {
        ducknng_request_bind_common_socket(bind, sock, (const uint8_t *)blob.data, (size_t)blob.size, timeout_ms);
    }
    if (sock) ducknng_runtime_release_client_socket(sock);
    if (blob.data) duckdb_free(blob.data);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "nng_error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "nng_error_message", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_request_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_request_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_request_bind_data *bind = (ducknng_request_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, bind->error, (idx_t)strlen(bind->error));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    if (bind->has_nng_error) {
        int32_t *nng_errors = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
        nng_errors[0] = bind->nng_error;
        duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), 0, ducknng_nng_strerror(bind->nng_error), (idx_t)strlen(ducknng_nng_strerror(bind->nng_error)));
    } else {
        set_null(duckdb_data_chunk_get_vector(output, 2), 0);
        set_null(duckdb_data_chunk_get_vector(output, 3), 0);
    }
    if (bind->payload) assign_blob(duckdb_data_chunk_get_vector(output, 4), 0, bind->payload, bind->payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 4), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_ncurl_row_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    duckdb_vector url_vec;
    duckdb_vector method_vec;
    duckdb_vector headers_vec;
    duckdb_vector body_vec;
    duckdb_vector timeout_vec;
    duckdb_vector tls_vec;
    duckdb_vector profile_vec = NULL;
    duckdb_vector child_vecs[6];
    bool *ok_data;
    int32_t *status_data;
    idx_t row_count;
    idx_t row;
    int i;
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    url_vec = duckdb_data_chunk_get_vector(input, 0);
    method_vec = duckdb_data_chunk_get_vector(input, 1);
    headers_vec = duckdb_data_chunk_get_vector(input, 2);
    body_vec = duckdb_data_chunk_get_vector(input, 3);
    timeout_vec = duckdb_data_chunk_get_vector(input, 4);
    tls_vec = duckdb_data_chunk_get_vector(input, 5);
    if (duckdb_data_chunk_get_column_count(input) > 6) profile_vec = duckdb_data_chunk_get_vector(input, 6);
    row_count = duckdb_data_chunk_get_size(input);
    for (i = 0; i < 6; i++) child_vecs[i] = duckdb_struct_vector_get_child(output, (idx_t)i);
    ok_data = (bool *)duckdb_vector_get_data(child_vecs[0]);
    status_data = (int32_t *)duckdb_vector_get_data(child_vecs[1]);
    for (row = 0; row < row_count; row++) {
        char *url = NULL;
        char *method = NULL;
        char *headers_json = NULL;
        uint8_t *body = NULL;
        idx_t body_len = 0;
        int32_t timeout_ms;
        uint64_t tls_config_id;
        char *profile_id = NULL;
        char *effective_headers_json = NULL;
        const ducknng_tls_opts *tls_opts = NULL;
        char *errmsg = NULL;
        char *headers_out = NULL;
        uint8_t *body_out = NULL;
        size_t body_out_len = 0;
        uint16_t status = 0;
        ok_data[row] = false;
        if (!ctx || !ctx->rt || arg_is_null(url_vec, row) || arg_is_null(timeout_vec, row) ||
            arg_is_null(tls_vec, row)) {
            errmsg = ducknng_strdup("ducknng: missing HTTP request state");
            goto emit_error;
        }
        url = arg_varchar_dup(url_vec, row);
        method = arg_is_null(method_vec, row) ? NULL : arg_varchar_dup(method_vec, row);
        headers_json = arg_is_null(headers_vec, row) ? NULL : arg_varchar_dup(headers_vec, row);
        if (!arg_is_null(body_vec, row)) body = arg_blob_dup(body_vec, row, &body_len);
        timeout_ms = arg_int32(timeout_vec, row, 0);
        tls_config_id = arg_u64(tls_vec, row, 0);
        if (profile_vec && !arg_is_null(profile_vec, row)) profile_id = arg_varchar_dup(profile_vec, row);
        if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) goto emit_error;
        if (profile_id && profile_id[0] && ducknng_runtime_resolve_http_profile_headers(ctx->rt,
                profile_id, url, method, headers_json, &effective_headers_json, &errmsg) != 0) {
            goto emit_error;
        }
        if (ducknng_http_transact(url, method,
                effective_headers_json ? effective_headers_json : headers_json,
                body, (size_t)body_len, timeout_ms, tls_opts, &status,
                &headers_out, &body_out, &body_out_len, &errmsg) != 0) {
            if (!errmsg) errmsg = ducknng_strdup("ducknng: HTTP request failed");
            goto emit_error;
        }
        ok_data[row] = true;
        status_data[row] = (int32_t)status;
        set_null(child_vecs[2], row);
        if (headers_out) duckdb_unsafe_vector_assign_string_element_len(child_vecs[3], row,
            headers_out, (idx_t)strlen(headers_out));
        else set_null(child_vecs[3], row);
        if (body_out) assign_blob(child_vecs[4], row, body_out, (idx_t)body_out_len);
        else set_null(child_vecs[4], row);
        if (body_out && body_out_len > 0 && ducknng_sql_bytes_look_text(body_out, body_out_len)) {
            char *body_text = ducknng_dup_bytes(body_out, body_out_len);
            if (body_text) {
                duckdb_unsafe_vector_assign_string_element_len(child_vecs[5], row,
                    body_text, (idx_t)strlen(body_text));
                duckdb_free(body_text);
            } else {
                ok_data[row] = false;
                set_null(child_vecs[1], row);
                errmsg = ducknng_strdup("ducknng: out of memory copying HTTP response text");
                goto emit_error;
            }
        } else {
            set_null(child_vecs[5], row);
        }
        goto cleanup_row;
emit_error:
        ok_data[row] = false;
        set_null(child_vecs[1], row);
        if (errmsg) duckdb_unsafe_vector_assign_string_element_len(child_vecs[2], row,
            errmsg, (idx_t)strlen(errmsg));
        else set_null(child_vecs[2], row);
        set_null(child_vecs[3], row);
        set_null(child_vecs[4], row);
        set_null(child_vecs[5], row);
cleanup_row:
        if (url) duckdb_free(url);
        if (method) duckdb_free(method);
        if (headers_json) duckdb_free(headers_json);
        if (profile_id) duckdb_free(profile_id);
        if (effective_headers_json) duckdb_free(effective_headers_json);
        if (body) duckdb_free(body);
        if (headers_out) duckdb_free(headers_out);
        if (body_out) duckdb_free(body_out);
        if (errmsg) duckdb_free(errmsg);
    }
}

static int register_remote_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 3, param_types, ducknng_query_rpc_bind,
        ducknng_query_rpc_init, ducknng_query_rpc_scan);
}

static int register_manifest_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 2, param_types,
        ducknng_get_rpc_manifest_bind, ducknng_single_row_init, ducknng_get_rpc_manifest_scan);
}

static int register_exec_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 3, param_types, ducknng_run_rpc_bind,
        ducknng_single_row_init, ducknng_run_rpc_scan);
}

static int register_request_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER,
        DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 4, param_types, ducknng_request_bind,
        ducknng_single_row_init, ducknng_request_scan);
}

static int register_request_socket_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 3, param_types,
        ducknng_request_socket_bind, ducknng_single_row_init, ducknng_request_scan);
}

static int register_http_result_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type profile_param_types[7] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    duckdb_type field_types[6] = {DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_VARCHAR};
    const char *field_names[6] = {"ok", "status", "error", "headers_json", "body", "body_text"};
    const char *sql =
        "CREATE OR REPLACE MACRO ducknng_ncurl(url, method, headers_json, body, timeout_ms, tls_config_id, profile_id := NULL) AS TABLE "
        "WITH _row AS (SELECT ducknng__ncurl_row(url, method, headers_json, body, timeout_ms, tls_config_id, profile_id) AS r) "
        "SELECT struct_extract(r, 'ok') AS ok, "
        "       struct_extract(r, 'status') AS status, "
        "       struct_extract(r, 'error') AS error, "
        "       struct_extract(r, 'headers_json') AS headers_json, "
        "       struct_extract(r, 'body') AS body, "
        "       struct_extract(r, 'body_text') AS body_text "
        "FROM _row";
    (void)name;
    if (!ctx || !ctx->rt) return 0;
    if (!ducknng_register_struct_row_scalar_named(con, ctx, "ducknng__ncurl_row", 6,
            param_types, ducknng_ncurl_row_scalar, 6, field_types, field_names)) return 0;
    if (!ducknng_register_struct_row_scalar_named(con, ctx, "ducknng__ncurl_row", 7,
            profile_param_types, ducknng_ncurl_row_scalar, 6, field_types, field_names)) return 0;
    return execute_sql(con, sql);
}

int ducknng_register_sql_rpc(duckdb_connection connection, ducknng_sql_context *ctx) {
    duckdb_type start_tls_config_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type start_tls_config_ip_types[7] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    duckdb_type stop_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type service_allowlist_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type rpc_exec_raw_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type rpc_manifest_raw_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type open_query_raw_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type fetch_query_raw_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type session_control_raw_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type request_tls_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_UBIGINT};
    duckdb_type request_socket_types[3] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_BLOB, DUCKDB_TYPE_INTEGER};
    if (!ctx) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_start_server", 6, ducknng_server_start_scalar, ctx, start_tls_config_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_start_server", 7, ducknng_server_start_scalar, ctx, start_tls_config_ip_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_stop_server", 1, ducknng_server_stop_scalar, ctx, stop_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_set_service_peer_allowlist", 2, ducknng_set_service_peer_allowlist_scalar, ctx, service_allowlist_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_set_service_ip_allowlist", 2, ducknng_set_service_ip_allowlist_scalar, ctx, service_allowlist_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_run_rpc_raw", 3, ducknng_run_rpc_raw_scalar, ctx, rpc_exec_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_get_rpc_manifest_raw", 2, ducknng_get_rpc_manifest_raw_scalar, ctx, rpc_manifest_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_open_query_raw", 5, ducknng_open_query_raw_scalar, ctx, open_query_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_fetch_query_raw", 6, ducknng_fetch_query_raw_scalar, ctx, fetch_query_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_close_query_raw", 4, ducknng_close_query_raw_scalar, ctx, session_control_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_cancel_query_raw", 4, ducknng_cancel_query_raw_scalar, ctx, session_control_raw_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_request_raw", 4, ducknng_request_raw_scalar, ctx, request_tls_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(connection, "ducknng_request_socket_raw", 3, ducknng_request_socket_scalar, ctx, request_socket_types, DUCKDB_TYPE_BLOB)) return 0;
    if (!register_remote_table_named(connection, ctx, "ducknng_query_rpc")) return 0;
    if (!register_manifest_result_table_named(connection, ctx, "ducknng_get_rpc_manifest")) return 0;
    if (!register_exec_result_table_named(connection, ctx, "ducknng_run_rpc")) return 0;
    if (!register_request_result_table_named(connection, ctx, "ducknng_request")) return 0;
    if (!register_request_socket_result_table_named(connection, ctx, "ducknng_request_socket")) return 0;
    if (!register_http_result_table_named(connection, ctx, "ducknng_ncurl")) return 0;
    return 1;
}
