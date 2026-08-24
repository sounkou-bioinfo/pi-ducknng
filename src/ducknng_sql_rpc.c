#include "ducknng_sql_api.h"
#include "ducknng_net_backend.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_http_compat.h"
#include "ducknng_manifest.h"
#include "ducknng_nng_compat.h"
#include "ducknng_quack.h"
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
    char *requested_serialization_mode;
    uint64_t fetch_batch_chunks;
    int row_payload_format;
    int transport_parsed;
    ducknng_transport_url transport;
    nng_socket req_sock;
    int req_sock_open;
    ducknng_http_frame_client *http_client;
    uint64_t session_id;
    char *session_token;
    int session_open;
    int close_attempted;
    int end_of_stream;
    ducknng_arrow_batches arrow_batches;
    ducknng_quack_schema quack_schema;
    size_t quack_scan_offset;
    uint64_t quack_chunks_remaining;
    int quack_scan_started;
    nng_msg *payload_msg;
    uint8_t *payload;
    size_t payload_len;
    idx_t row_count;
} ducknng_query_rpc_bind_data;

typedef struct {
    ducknng_query_rpc_bind_data *bind;
    idx_t offset;
    idx_t array_index;
} ducknng_query_rpc_init_data;

typedef struct {
    bool ok;
    char *error;
    char *manifest;
} ducknng_manifest_result_bind_data;

typedef struct {
    ducknng_sql_context *ctx;
    char *url;
    uint64_t tls_config_id;
    uint8_t *request_payload;
    size_t request_payload_len;
    int executed;
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
    duckdb_scalar_function_t fn, duckdb_scalar_function_bind_t bind_fn, idx_t nfields,
    const duckdb_type *field_type_ids, const char **field_names) {
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
    ok = ducknng_sql_register_volatile_scalar_logical_types_with_bind(con, name, nparams, fn,
        bind_fn, ctx, param_types, return_type);
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

static int ducknng_query_rpc_parse_row_payload_format(const char *value, int *out) {
    if (out) *out = DUCKNNG_PAYLOAD_ARROW_IPC_STREAM;
    if (!value || !value[0] || strcmp(value, "arrow_ipc_stream") == 0) return 0;
    if (strcmp(value, "ducknng_quack_batch") == 0) {
        if (out) *out = DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH;
        return 0;
    }
    return -1;
}

static void ducknng_query_rpc_reset_result(ducknng_query_rpc_bind_data *bind);
static int ducknng_query_rpc_close_session(ducknng_query_rpc_bind_data *bind);

static void destroy_query_rpc_bind_data(void *ptr) {
    ducknng_query_rpc_bind_data *data = (ducknng_query_rpc_bind_data *)ptr;
    if (!data) return;
    (void)ducknng_query_rpc_close_session(data);
    ducknng_query_rpc_reset_result(data);
    ducknng_quack_schema_reset(&data->quack_schema);
    if (data->url) duckdb_free(data->url);
    if (data->requested_serialization_mode) duckdb_free(data->requested_serialization_mode);
    if (data->session_token) duckdb_free(data->session_token);
    if (data->http_client) ducknng_http_frame_client_close(data->http_client);
    if (data->req_sock_open) ducknng_socket_close(data->req_sock);
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
    if (data->url) duckdb_free(data->url);
    if (data->request_payload) duckdb_free(data->request_payload);
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
    if (ducknng_net_backend_carrier_scheme(parsed.scheme)) {
        if (parsed.scheme == DUCKNNG_TRANSPORT_SCHEME_WS ||
            parsed.scheme == DUCKNNG_TRANSPORT_SCHEME_WSS) {
            /* Reached only on a backend where ws/wss is a frame-carrier scheme
             * (the browser). Browsers have no synchronous WebSocket receive, so
             * the sync client cannot drive it; the async path can. */
            nng_msg_free(req);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(
                "ducknng: synchronous WebSocket RPC is unavailable in the browser "
                "(no synchronous WebSocket receive); use the async *_rpc_aio path");
            return NULL;
        }
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

static nng_msg *ducknng_query_rpc_method_roundtrip(ducknng_query_rpc_bind_data *bind,
    const char *method_name, const void *payload, size_t payload_len, int timeout_ms,
    const ducknng_tls_opts *tls_opts, char **errmsg) {
    nng_msg *req = NULL;
    nng_msg *resp = NULL;
    int rv;
    if (!bind || !bind->url || !method_name) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing query_rpc transport state");
        return NULL;
    }
    if (!bind->transport_parsed) {
        if (ducknng_transport_url_parse(bind->url, &bind->transport, errmsg) != 0) return NULL;
        bind->transport_parsed = 1;
    }
    req = ducknng_client_method_request(method_name, payload, payload_len, errmsg);
    if (!req) return NULL;
    if (ducknng_net_backend_carrier_scheme(bind->transport.scheme)) {
        if (bind->transport.scheme == DUCKNNG_TRANSPORT_SCHEME_WS ||
            bind->transport.scheme == DUCKNNG_TRANSPORT_SCHEME_WSS) {
            /* Browser-only (see above): no synchronous WebSocket receive. */
            nng_msg_free(req);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(
                "ducknng: synchronous WebSocket RPC is unavailable in the browser "
                "(no synchronous WebSocket receive); use the async *_rpc_aio path");
            return NULL;
        }
        if (!bind->http_client && ducknng_http_frame_client_open(bind->url, tls_opts,
                &bind->http_client, errmsg) != 0) {
            nng_msg_free(req);
            return NULL;
        }
        if (ducknng_http_frame_client_transact_msg(bind->http_client,
                (const uint8_t *)nng_msg_body(req), nng_msg_len(req), timeout_ms,
                &resp, errmsg) != 0) {
            nng_msg_free(req);
            return NULL;
        }
        nng_msg_free(req);
        return resp;
    }
    if (!bind->req_sock_open) {
        if (ducknng_client_open_req_socket_tls(bind->url, timeout_ms, tls_opts,
                &bind->req_sock, errmsg) != 0) {
            nng_msg_free(req);
            return NULL;
        }
        bind->req_sock_open = 1;
    }
    rv = ducknng_req_transact(bind->req_sock, req, &resp);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    return resp;
}

static void ducknng_query_rpc_reset_result(ducknng_query_rpc_bind_data *bind) {
    if (!bind) return;
    ducknng_arrow_batches_reset(&bind->arrow_batches);
    if (bind->payload_msg) nng_msg_free(bind->payload_msg);
    else if (bind->payload) duckdb_free(bind->payload);
    bind->payload_msg = NULL;
    bind->payload = NULL;
    bind->payload_len = 0;
    bind->row_count = 0;
    bind->quack_scan_offset = 0;
    bind->quack_chunks_remaining = 0;
    bind->quack_scan_started = 0;
}

static int ducknng_query_rpc_encode_query_request(
    ducknng_query_rpc_bind_data *bind, const char *sql, duckdb_value params,
    uint64_t batch_rows, uint64_t batch_bytes, uint8_t **payload,
    size_t *payload_len, char **errmsg) {
    if (!bind || !bind->ctx || !bind->ctx->rt) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(
            "ducknng: missing runtime for query request encoding");
        return -1;
    }
    if (!params) {
        return ducknng_query_open_request_to_ipc_ex(sql, batch_rows,
            batch_bytes, NULL, bind->requested_serialization_mode, payload,
            payload_len, errmsg);
    }
    {
        duckdb_connection codec_con = ducknng_runtime_codec_connection(bind->ctx->rt);
        int rc;
        if (!codec_con) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(
                "ducknng: no local connection is available to encode query parameters");
            return -1;
        }
        ducknng_runtime_codec_connection_lock(bind->ctx->rt);
        rc = ducknng_query_open_request_with_params_to_ipc(codec_con, sql,
            batch_rows, batch_bytes, NULL, bind->requested_serialization_mode,
            params, payload, payload_len, errmsg);
        ducknng_runtime_codec_connection_unlock(bind->ctx->rt);
        return rc;
    }
}

static int ducknng_query_rpc_open_session(ducknng_query_rpc_bind_data *bind, const char *sql,
    duckdb_value params, char **errmsg) {
    const ducknng_tls_opts *tls_opts = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    nng_msg *resp_msg = NULL;
    ducknng_frame frame;
    char *json = NULL;
    uint64_t session_id = 0;
    char *session_token = NULL;
    char *serialization_mode = NULL;
    int row_payload_format = DUCKNNG_PAYLOAD_ARROW_IPC_STREAM;
    int rc = -1;
    if (!bind || !bind->ctx || !bind->url || !sql || !sql[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: query_rpc requires non-empty url and sql");
        return -1;
    }
    if (ducknng_lookup_tls_opts(bind->ctx, bind->tls_config_id, &tls_opts, errmsg) != 0) goto cleanup;
    {
        uint64_t batch_rows = bind->fetch_batch_chunks ?
            bind->fetch_batch_chunks * (uint64_t)duckdb_vector_size() : 0;
        if (ducknng_query_rpc_encode_query_request(bind, sql, params, batch_rows,
                0, &payload, &payload_len, errmsg) != 0) {
            goto cleanup;
        }
    }
    resp_msg = ducknng_query_rpc_method_roundtrip(bind, "query_open", payload, payload_len,
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
    serialization_mode = ducknng_json_extract_string_dup(json, "serialization_mode");
    if (ducknng_query_rpc_parse_row_payload_format(serialization_mode, &row_payload_format) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: query_open reply returned an unsupported serialization_mode");
        goto cleanup;
    }
    if (bind->session_token) duckdb_free(bind->session_token);
    bind->session_id = session_id;
    bind->session_token = session_token;
    bind->row_payload_format = row_payload_format;
    bind->session_open = 1;
    bind->close_attempted = 0;
    session_token = NULL;
    rc = 0;
cleanup:
    if (serialization_mode) duckdb_free(serialization_mode);
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
    resp_msg = ducknng_query_rpc_method_roundtrip(bind, "fetch", json, strlen(json),
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
        bind->row_payload_format = DUCKNNG_PAYLOAD_ARROW_IPC_STREAM;
        if (ducknng_decode_ipc_batches_payload(frame.payload, (size_t)frame.payload_len,
                &bind->arrow_batches, errmsg) != 0) {
            goto cleanup;
        }
        bind->row_count = bind->arrow_batches.row_count;
        if (bind->arrow_batches.schema.n_children < 0) {
            ducknng_query_rpc_reset_result(bind);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid fetch Arrow row schema");
            goto cleanup;
        }
        rc = 0;
        goto cleanup;
    }
    if ((frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_QUACK_BATCH) &&
        (frame.flags & DUCKNNG_RPC_FLAG_RESULT_ROWS)) {
        bind->row_payload_format = DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH;
        bind->payload_msg = resp_msg;
        bind->payload = (uint8_t *)frame.payload;
        bind->payload_len = (size_t)frame.payload_len;
        resp_msg = NULL;
        bind->row_count = 0;
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
            resp_msg = ducknng_query_rpc_method_roundtrip(bind, "close", json, strlen(json),
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

static void ducknng_query_rpc_bind_impl(duckdb_bind_info info, int with_params) {
    ducknng_query_rpc_bind_data *bind;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value tls_val;
    duckdb_value mode_val;
    duckdb_value params_val;
    char *sql;
    char *errmsg = NULL;
    idx_t param_count;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    param_count = duckdb_bind_get_parameter_count(info);
    memset(&params_val, 0, sizeof(params_val));
    if ((!with_params && param_count != 3 && param_count != 4) ||
        (with_params && param_count != 4)) {
        duckdb_bind_set_error(info, with_params
            ? "ducknng: ducknng_query_rpc_params(url, sql, params, tls_config_id) requires exactly four parameters"
            : "ducknng: ducknng_query_rpc(url, sql, tls_config_id) requires exactly three parameters and ducknng_query_rpc_mode(url, sql, tls_config_id, serialization_mode) requires exactly four");
        return;
    }
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    if (with_params) {
        params_val = duckdb_bind_get_parameter(info, 2);
        tls_val = duckdb_bind_get_parameter(info, 3);
    } else {
        tls_val = duckdb_bind_get_parameter(info, 2);
    }
    if (!with_params && param_count == 4) mode_val = duckdb_bind_get_parameter(info, 3);
    else memset(&mode_val, 0, sizeof(mode_val));
    bind = (ducknng_query_rpc_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_destroy_value(&url_val);
        duckdb_destroy_value(&sql_val);
        duckdb_destroy_value(&tls_val);
        if (with_params) duckdb_destroy_value(&params_val);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    bind->url = duckdb_get_varchar(url_val);
    sql = duckdb_get_varchar(sql_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->requested_serialization_mode = (!with_params && param_count == 4)
        ? duckdb_get_varchar(mode_val) : NULL;
    {
        duckdb_client_context client_ctx = NULL;
        duckdb_table_function_get_client_context(info, &client_ctx);
        bind->fetch_batch_chunks = ducknng_sql_get_config_ubigint(client_ctx,
            "ducknng.fetch_batch_chunks", DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS);
        if (client_ctx) duckdb_destroy_client_context(&client_ctx);
        if (bind->fetch_batch_chunks == 0) bind->fetch_batch_chunks = DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS;
        if (bind->fetch_batch_chunks > DUCKNNG_MAX_FETCH_BATCH_CHUNKS)
            bind->fetch_batch_chunks = DUCKNNG_MAX_FETCH_BATCH_CHUNKS;
    }
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&tls_val);
    if (!with_params && param_count == 4) duckdb_destroy_value(&mode_val);
    if (!bind->url || !sql || !bind->url[0] || !sql[0] ||
        (!with_params && param_count == 4 &&
            (!bind->requested_serialization_mode || !bind->requested_serialization_mode[0]))) {
        if (sql) duckdb_free(sql);
        if (with_params) duckdb_destroy_value(&params_val);
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, !with_params && param_count == 4
            ? "ducknng: ducknng_query_rpc_mode(url, sql, tls_config_id, serialization_mode) requires non-empty url, sql, and serialization_mode"
            : (with_params
                ? "ducknng: ducknng_query_rpc_params(url, sql, params, tls_config_id) requires non-empty url and sql"
                : "ducknng: ducknng_query_rpc(url, sql, tls_config_id) requires non-empty url and sql"));
        return;
    }
    if (ducknng_query_rpc_open_session(bind, sql,
            with_params ? params_val : NULL, &errmsg) != 0) {
        duckdb_free(sql);
        if (with_params) duckdb_destroy_value(&params_val);
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: query_open failed");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    if (with_params) duckdb_destroy_value(&params_val);
    duckdb_free(sql);
    if (ducknng_query_rpc_fetch_batch(bind, &errmsg) != 0) {
        destroy_query_rpc_bind_data(bind);
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: fetch failed");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    if (bind->row_payload_format == DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH) {
        if (!bind->payload || bind->payload_len == 0 ||
            ducknng_quack_payload_bind_columns(info, bind->payload, bind->payload_len,
                &bind->quack_schema, &bind->row_count, &errmsg) != 0) {
            destroy_query_rpc_bind_data(bind);
            duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported remote quack row payload");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
    } else {
        if (!bind->arrow_batches.schema.release || bind->arrow_batches.schema.n_children < 0) {
            destroy_query_rpc_bind_data(bind);
            duckdb_bind_set_error(info, "ducknng: query_rpc could not infer result columns from the first fetch reply");
            return;
        }
        if (ducknng_sql_arrow_bind_result_columns(info, &bind->arrow_batches.schema, &errmsg) != 0) {
            destroy_query_rpc_bind_data(bind);
            duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported remote Arrow type");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
    }
    duckdb_bind_set_bind_data(info, bind, destroy_query_rpc_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_query_rpc_bind(duckdb_bind_info info) {
    ducknng_query_rpc_bind_impl(info, 0);
}

static void ducknng_query_rpc_params_bind(duckdb_bind_info info) {
    ducknng_query_rpc_bind_impl(info, 1);
}

static void ducknng_prepare_query_bind_impl(duckdb_bind_info info,
    int with_params) {
    ducknng_query_rpc_bind_data *bind = NULL;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    duckdb_value url_val = NULL;
    duckdb_value sql_val = NULL;
    duckdb_value params_val = NULL;
    duckdb_value tls_val = NULL;
    const ducknng_tls_opts *tls_opts = NULL;
    char *sql = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    nng_msg *response = NULL;
    ducknng_frame frame;
    char *errmsg = NULL;
    idx_t param_count = duckdb_bind_get_parameter_count(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    if ((!with_params && param_count != 3) || (with_params && param_count != 4)) {
        duckdb_bind_set_error(info, with_params
            ? "ducknng: ducknng_prepare_query_params(url, sql, params, tls_config_id) requires four parameters"
            : "ducknng: ducknng_prepare_query(url, sql, tls_config_id) requires three parameters");
        return;
    }
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    if (with_params) {
        params_val = duckdb_bind_get_parameter(info, 2);
        tls_val = duckdb_bind_get_parameter(info, 3);
    } else {
        tls_val = duckdb_bind_get_parameter(info, 2);
    }
    bind = (ducknng_query_rpc_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        goto cleanup;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    bind->url = duckdb_get_varchar(url_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    sql = duckdb_get_varchar(sql_val);
    if (!bind->url || !bind->url[0] || !sql || !sql[0]) {
        duckdb_bind_set_error(info,
            with_params
                ? "ducknng: ducknng_prepare_query_params requires non-empty url and sql"
                : "ducknng: ducknng_prepare_query requires non-empty url and sql");
        goto cleanup;
    }
    if (ducknng_query_rpc_encode_query_request(bind, sql,
            with_params ? params_val : NULL, 0, 0, &payload, &payload_len,
            &errmsg) != 0 ||
        ducknng_lookup_tls_opts(ctx, bind->tls_config_id, &tls_opts, &errmsg) != 0) {
        duckdb_bind_set_error(info, errmsg ? errmsg :
            "ducknng: failed to encode query_prepare request");
        goto cleanup;
    }
    response = ducknng_query_rpc_method_roundtrip(bind, "query_prepare",
        payload, payload_len, 5000, tls_opts, &errmsg);
    if (!response || ducknng_decode_request(response, &frame) != 0) {
        duckdb_bind_set_error(info, errmsg ? errmsg :
            "ducknng: query_prepare transport failed");
        goto cleanup;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        errmsg = ducknng_frame_error_detail(&frame,
            "ducknng: query_prepare failed");
        duckdb_bind_set_error(info, errmsg ? errmsg :
            "ducknng: query_prepare failed");
        goto cleanup;
    }
    if (!(frame.flags & DUCKNNG_RPC_FLAG_RESULT_ROWS) ||
        !(frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM) ||
        ducknng_decode_ipc_batches_payload(frame.payload,
            (size_t)frame.payload_len, &bind->arrow_batches, &errmsg) != 0 ||
        ducknng_sql_arrow_bind_result_columns(info, &bind->arrow_batches.schema,
            &errmsg) != 0) {
        duckdb_bind_set_error(info, errmsg ? errmsg :
            "ducknng: invalid query_prepare Arrow schema reply");
        goto cleanup;
    }
    duckdb_bind_set_bind_data(info, bind, destroy_query_rpc_bind_data);
    duckdb_bind_set_cardinality(info, 0, true);
    bind = NULL;
cleanup:
    if (response) nng_msg_free(response);
    if (payload) duckdb_free(payload);
    if (sql) duckdb_free(sql);
    if (url_val) duckdb_destroy_value(&url_val);
    if (sql_val) duckdb_destroy_value(&sql_val);
    if (params_val) duckdb_destroy_value(&params_val);
    if (tls_val) duckdb_destroy_value(&tls_val);
    if (errmsg) duckdb_free(errmsg);
    if (bind) destroy_query_rpc_bind_data(bind);
}

static void ducknng_prepare_query_bind(duckdb_bind_info info) {
    ducknng_prepare_query_bind_impl(info,
        duckdb_bind_get_parameter_count(info) == 4);
}

static void ducknng_prepare_query_scan(duckdb_function_info info,
    duckdb_data_chunk output) {
    (void)info;
    duckdb_data_chunk_set_size(output, 0);
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
    init->array_index = 0;
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
    if (bind->row_payload_format == DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH) {
        for (;;) {
            if (bind->payload && bind->payload_len > 0 && !bind->quack_scan_started) {
                if (ducknng_quack_payload_scan_begin(bind->payload, bind->payload_len,
                        &bind->quack_schema, &bind->quack_scan_offset,
                        &bind->quack_chunks_remaining, &errmsg) != 0) {
                    duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to start remote quack row payload scan");
                    if (errmsg) duckdb_free(errmsg);
                    (void)ducknng_query_rpc_close_session(bind);
                    return;
                }
                bind->quack_scan_started = 1;
            }
            if (bind->quack_chunks_remaining > 0) break;
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
        }
        if (ducknng_quack_payload_scan_next(output, bind->payload, bind->payload_len,
                &bind->quack_schema, &bind->quack_scan_offset,
                &bind->quack_chunks_remaining, &errmsg) != 0) {
            duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to decode remote quack row payload");
            if (errmsg) duckdb_free(errmsg);
            (void)ducknng_query_rpc_close_session(bind);
            return;
        }
        return;
    }
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
        init->array_index = 0;
        if (bind->row_count == 0 && bind->end_of_stream) {
            (void)ducknng_query_rpc_close_session(bind);
            duckdb_data_chunk_set_size(output, 0);
            return;
        }
    }
    if (init->array_index >= bind->arrow_batches.array_count) {
        duckdb_data_chunk_set_size(output, 0);
        init->offset = bind->row_count;
    } else {
        struct ArrowArray *arr = &bind->arrow_batches.arrays[init->array_index];
        idx_t arr_rows = (idx_t)arr->length;
        if (ducknng_sql_arrow_scan_table(output, &bind->arrow_batches.schema, arr, arr_rows,
                &init->offset, &errmsg) != 0) {
            duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to decode remote Arrow row payload");
            if (errmsg) duckdb_free(errmsg);
            (void)ducknng_query_rpc_close_session(bind);
            return;
        }
        if (init->offset >= arr_rows) {
            init->array_index++;
            init->offset = 0;
            if (init->array_index >= bind->arrow_batches.array_count) init->offset = bind->row_count;
        }
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

static void ducknng_run_rpc_bind_impl(duckdb_bind_info info, int with_params) {
    ducknng_exec_result_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val = NULL;
    duckdb_value sql_val = NULL;
    duckdb_value params_val = NULL;
    duckdb_value tls_val = NULL;
    char *sql = NULL;
    char *errmsg = NULL;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    if ((!with_params && duckdb_bind_get_parameter_count(info) != 3) ||
        (with_params && duckdb_bind_get_parameter_count(info) != 4)) {
        duckdb_bind_set_error(info, with_params
            ? "ducknng: ducknng_run_rpc_params(url, sql, params, tls_config_id) requires four parameters"
            : "ducknng: ducknng_run_rpc(url, sql, tls_config_id) requires three parameters");
        return;
    }
    bind = (ducknng_exec_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    if (with_params) {
        params_val = duckdb_bind_get_parameter(info, 2);
        tls_val = duckdb_bind_get_parameter(info, 3);
    } else {
        tls_val = duckdb_bind_get_parameter(info, 2);
    }
    bind->url = duckdb_get_varchar(url_val);
    sql = duckdb_get_varchar(sql_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    if (!bind->url || !bind->url[0] || !sql || !sql[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: remote exec URL and SQL must not be NULL or empty");
    } else if (with_params) {
        duckdb_connection codec_con = ctx && ctx->rt
            ? ducknng_runtime_codec_connection(ctx->rt) : NULL;
        int encode_rc;
        if (!codec_con) {
            bind->error = ducknng_strdup(
                "ducknng: no local connection is available to encode exec parameters");
        } else {
            ducknng_runtime_codec_connection_lock(ctx->rt);
            encode_rc = ducknng_exec_request_with_params_to_ipc(codec_con, sql,
                0, params_val, &bind->request_payload,
                &bind->request_payload_len, &errmsg);
            ducknng_runtime_codec_connection_unlock(ctx->rt);
            if (encode_rc != 0) {
                bind->error = errmsg ? errmsg : ducknng_strdup(
                    "ducknng: failed to encode parameterized exec request");
                errmsg = NULL;
            }
        }
    } else {
        if (ducknng_exec_request_to_ipc(sql, 0, &bind->request_payload,
                &bind->request_payload_len, &errmsg) != 0) {
            bind->error = errmsg ? errmsg : ducknng_strdup(
                "ducknng: failed to encode exec request");
            errmsg = NULL;
        }
    }
    if (sql) duckdb_free(sql);
    if (url_val) duckdb_destroy_value(&url_val);
    if (sql_val) duckdb_destroy_value(&sql_val);
    if (params_val) duckdb_destroy_value(&params_val);
    if (tls_val) duckdb_destroy_value(&tls_val);
    if (errmsg) duckdb_free(errmsg);
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

static void ducknng_run_rpc_bind(duckdb_bind_info info) {
    ducknng_run_rpc_bind_impl(info, 0);
}

static void ducknng_run_rpc_params_bind(duckdb_bind_info info) {
    ducknng_run_rpc_bind_impl(info, 1);
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
    if (!bind->executed) {
        const ducknng_tls_opts *tls_opts = NULL;
        char *errmsg = NULL;
        nng_msg *resp_msg = NULL;
        ducknng_frame frame;
        bind->executed = 1;
        if (!bind->error && ducknng_lookup_tls_opts(bind->ctx,
                bind->tls_config_id, &tls_opts, &errmsg) != 0) {
            bind->error = errmsg ? errmsg : ducknng_strdup(
                "ducknng: tls config not found");
            errmsg = NULL;
        }
        if (!bind->error) {
            resp_msg = ducknng_client_method_roundtrip_tls(bind->url, "exec",
                bind->request_payload, bind->request_payload_len, 5000,
                tls_opts, &errmsg);
            if (!resp_msg) {
                bind->error = errmsg ? errmsg : ducknng_strdup(
                    "ducknng: remote exec request failed");
                errmsg = NULL;
            } else if (ducknng_decode_request(resp_msg, &frame) != 0) {
                bind->error = ducknng_strdup(
                    "ducknng: invalid remote exec response envelope");
            } else if (frame.type == DUCKNNG_RPC_ERROR) {
                bind->error = ducknng_frame_error_detail(&frame,
                    "ducknng: remote exec request failed");
            } else if (frame.type != DUCKNNG_RPC_RESULT ||
                !(frame.flags & DUCKNNG_RPC_FLAG_RESULT_METADATA)) {
                bind->error = ducknng_strdup(
                    "ducknng: remote exec expected metadata reply");
            } else if (ducknng_decode_exec_metadata_payload(frame.payload,
                    (size_t)frame.payload_len, &bind->rows_changed,
                    (uint32_t *)&bind->statement_type,
                    (uint32_t *)&bind->result_type, &errmsg) != 0) {
                bind->error = errmsg ? errmsg : ducknng_strdup(
                    "ducknng: failed to decode remote exec metadata");
                errmsg = NULL;
            }
        }
        bind->ok = bind->error == NULL;
        if (resp_msg) nng_msg_free(resp_msg);
        if (errmsg) duckdb_free(errmsg);
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
        if (profile_id && profile_id[0]) {
            uint64_t connection_id = 0;
            int has_connection_id = ducknng_sql_scalar_connection_id(info, &connection_id);
            if (ducknng_runtime_resolve_http_profile_headers(ctx->rt,
                    profile_id, url, method, headers_json, has_connection_id, connection_id,
                    &effective_headers_json, &errmsg) != 0) {
                goto emit_error;
            }
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

/* ---- ducknng_upload_table: client-to-server quack-batch upload lane (#9) ----
 * Runs source_query on the runtime's pooled codec connection, then streams its
 * result chunks to a remote table via upload_open -> upload_append* -> upload_commit,
 * rolling the server transaction back with upload_abort on any error. It reuses
 * the ducknng_query_rpc transport bind purely for the socket/session/roundtrip
 * plumbing (never the query-family "close"). Returns one row: rows_uploaded and
 * client-sent bytes_uploaded. v1: source_query runs on a fresh connection, so it
 * sees committed/persistent data, not the caller's temp/uncommitted state. */
typedef struct {
    ducknng_query_rpc_bind_data transport;
    char *source_query;
    char *target_table;
    uint64_t rows_uploaded;
    uint64_t bytes_uploaded;
} ducknng_upload_table_bind_data;

typedef struct {
    ducknng_upload_table_bind_data *bind;
    int done; /* the upload runs once, on the first scan (never in bind: DuckDB calls bind repeatedly) */
} ducknng_upload_table_init_data;

static void destroy_upload_table_bind_data(void *ptr) {
    ducknng_upload_table_bind_data *d = (ducknng_upload_table_bind_data *)ptr;
    if (!d) return;
    /* Transport teardown only -- the upload lane has no query-family close. */
    if (d->transport.http_client) ducknng_http_frame_client_close(d->transport.http_client);
    if (d->transport.req_sock_open) ducknng_socket_close(d->transport.req_sock);
    if (d->transport.url) duckdb_free(d->transport.url);
    if (d->transport.session_token) duckdb_free(d->transport.session_token);
    if (d->source_query) duckdb_free(d->source_query);
    if (d->target_table) duckdb_free(d->target_table);
    duckdb_free(d);
}

static void destroy_upload_table_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

/* Build the upload_append frame body: the counted control prefix
 * [session_id u64 LE][token_len u16 LE][token] followed by one quack batch. */
static uint8_t *ducknng_upload_append_body(uint64_t session_id, const char *token,
    const uint8_t *quack, size_t quack_len, size_t *out_len) {
    size_t token_len = token ? strlen(token) : 0;
    size_t body_len;
    uint8_t *body;
    size_t off = 0;
    int i;
    if (token_len == 0 || token_len > DUCKNNG_UPLOAD_TOKEN_MAX) return NULL;
    body_len = 8u + 2u + token_len + quack_len;
    body = (uint8_t *)duckdb_malloc(body_len);
    if (!body) return NULL;
    for (i = 0; i < 8; i++) body[off++] = (uint8_t)((session_id >> (8 * i)) & 0xffu);
    body[off++] = (uint8_t)(token_len & 0xffu);
    body[off++] = (uint8_t)((token_len >> 8) & 0xffu);
    memcpy(body + off, token, token_len);
    off += token_len;
    if (quack_len) memcpy(body + off, quack, quack_len);
    *out_len = body_len;
    return body;
}

/* One timeout for the whole upload conversation. The NNG req socket is opened by
 * the first (upload_open) roundtrip and reused, and ducknng_query_rpc_method_roundtrip
 * only applies timeout_ms when it opens the socket -- so every upload roundtrip
 * must pass the same value or later appends/commits would silently keep the open
 * timeout. HTTP applies it per transact, so a single constant is correct there too. */
#define DUCKNNG_UPLOAD_TIMEOUT_MS 30000

/* Batch several chunks per upload_append to cut roundtrips. The per-frame chunk
 * count adapts from the observed bytes/chunk so each frame targets ~4 MiB and
 * stays well under the server's 16 MiB upload_append max_request_bytes for any
 * row width (wide rows shrink the count; a hard cap bounds narrow-row frames). */
#define DUCKNNG_UPLOAD_APPEND_TARGET_BYTES (4u * 1024u * 1024u)
#define DUCKNNG_UPLOAD_APPEND_MAX_CHUNKS 32u
/* Hard per-frame cap (prefix + quack body), with margin under the server's
 * 16 MiB upload_append max_request_bytes. The adaptive estimate can undershoot
 * when later chunks are much wider than earlier ones, so a batch exceeding this
 * is re-encoded with a halved chunk count before sending. */
#define DUCKNNG_UPLOAD_APPEND_MAX_BYTES (14u * 1024u * 1024u)

/* v1 upload targets are simple [[catalog.]schema.]table identifiers. Mirror the
 * server-side parser (ducknng_upload_split_target/ident_ok) client-side so a
 * malformed target fails fast -- before source_query runs -- rather than after,
 * and so a quote/backslash cannot break out of the upload_open control JSON.
 * Rules: 1..3 dot-separated segments, each a non-empty identifier whose first
 * char is [A-Za-z_] and rest [A-Za-z0-9_$]. The server re-validates authoritatively. */
static int ducknng_upload_table_target_ok(const char *target) {
    size_t seg_start = 0;
    size_t i;
    int nparts = 0;
    if (!target || !target[0]) return 0;
    for (i = 0;; i++) {
        if (target[i] == '.' || target[i] == '\0') {
            size_t len = i - seg_start;
            size_t j;
            char first;
            if (len == 0) return 0; /* empty segment: leading/trailing/double dot */
            first = target[seg_start];
            if (!((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z') || first == '_')) return 0;
            for (j = 1; j < len; j++) {
                char c = target[seg_start + j];
                if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                      (c >= '0' && c <= '9') || c == '_' || c == '$')) return 0;
            }
            if (++nparts > 3) return 0; /* more than [[catalog.]schema.]table */
            if (target[i] == '\0') break;
            seg_start = i + 1;
        }
    }
    return 1;
}

/* Build and send one upload_append (counted prefix + quack payload), decode the
 * reply, and (when out_running is non-NULL) read the server's running row count.
 * Returns 0 on success, -1 with *errmsg on any failure. */
static int ducknng_upload_append_one(ducknng_query_rpc_bind_data *t, const uint8_t *quack,
    size_t quack_len, const ducknng_tls_opts *tls_opts, uint64_t *out_running, char **errmsg) {
    uint8_t *body;
    size_t body_len = 0;
    nng_msg *resp;
    ducknng_frame frame;
    char *reply_json;
    body = ducknng_upload_append_body(t->session_id, t->session_token, quack, quack_len, &body_len);
    if (!body) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building upload_append frame");
        return -1;
    }
    resp = ducknng_query_rpc_method_roundtrip(t, "upload_append", body, body_len,
        DUCKNNG_UPLOAD_TIMEOUT_MS, tls_opts, errmsg);
    duckdb_free(body);
    if (!resp) return -1;
    if (ducknng_decode_request(resp, &frame) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid upload_append response envelope");
        nng_msg_free(resp);
        return -1;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        if (errmsg && !*errmsg) *errmsg = ducknng_frame_error_detail(&frame, "ducknng: upload_append failed");
        nng_msg_free(resp);
        return -1;
    }
    reply_json = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
    if (reply_json && out_running) (void)ducknng_json_extract_u64_value(reply_json, "rows", out_running);
    if (reply_json) duckdb_free(reply_json);
    nng_msg_free(resp);
    return 0;
}

/* Send a JSON-keyed upload session control method (upload_commit / upload_abort). */
static nng_msg *ducknng_upload_control(ducknng_query_rpc_bind_data *t, const char *method,
    const ducknng_tls_opts *tls_opts, char **errmsg) {
    char *json = ducknng_session_request_json(t->session_id, t->session_token, 0, 0);
    nng_msg *resp;
    if (!json) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building upload control request");
        return NULL;
    }
    resp = ducknng_query_rpc_method_roundtrip(t, method, json, strlen(json),
        DUCKNNG_UPLOAD_TIMEOUT_MS, tls_opts, errmsg);
    duckdb_free(json);
    return resp;
}

static int ducknng_upload_table_run(ducknng_upload_table_bind_data *ub, const char *source_query,
    char **errmsg) {
    ducknng_query_rpc_bind_data *t = &ub->transport;
    const ducknng_tls_opts *tls_opts = NULL;
    duckdb_result result;
    int have_result = 0;
    char *open_json = NULL;
    size_t open_json_len;
    nng_msg *resp = NULL;
    ducknng_frame frame;
    char *reply_json = NULL;
    idx_t chunk_index = 0;
    int sent_any = 0;
    int rc = -1;

    /* Validate the target before running source_query, so a malformed target
     * fails fast without executing any (possibly side-effecting) local query. */
    if (!ducknng_upload_table_target_ok(ub->target_table)) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(
            "ducknng: upload target_table must be a simple [[catalog.]schema.]table identifier");
        goto done;
    }
    if (ducknng_lookup_tls_opts(t->ctx, t->tls_config_id, &tls_opts, errmsg) != 0) goto done;

    /* Run source_query on the runtime's pre-opened codec connection. The stable
     * C API does not permit opening a fresh connection mid-request (the pooled /
     * codec connections exist for exactly this). duckdb_query materializes the
     * whole result, so the codec lock is held only for the query itself, not for
     * the network-bound upload that follows. v1: this connection sees
     * committed/persistent data, not the caller's temp/uncommitted state. */
    {
        duckdb_connection cc = ducknng_runtime_codec_connection(t->ctx->rt);
        duckdb_state qs;
        if (!cc) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: upload_table has no local connection");
            goto done;
        }
        ducknng_runtime_codec_connection_lock(t->ctx->rt);
        qs = duckdb_query(cc, source_query, &result);
        ducknng_runtime_codec_connection_unlock(t->ctx->rt);
        if (qs == DuckDBError) {
            const char *m = duckdb_result_error(&result);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(m && m[0] ? m : "ducknng: upload_table source_query failed");
            duckdb_destroy_result(&result);
            goto done;
        }
        have_result = 1;
    }

    open_json_len = strlen(ub->target_table) + 48u;
    open_json = (char *)duckdb_malloc(open_json_len);
    if (!open_json) { if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory"); goto done; }
    snprintf(open_json, open_json_len, "{\"target_table\":\"%s\",\"mode\":\"append\"}", ub->target_table);
    resp = ducknng_query_rpc_method_roundtrip(t, "upload_open", open_json, strlen(open_json),
        DUCKNNG_UPLOAD_TIMEOUT_MS, tls_opts, errmsg);
    if (!resp) goto done;
    if (ducknng_decode_request(resp, &frame) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid upload_open response envelope");
        goto done;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        if (errmsg && !*errmsg) *errmsg = ducknng_frame_error_detail(&frame, "ducknng: upload_open failed");
        goto done;
    }
    reply_json = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
    if (!reply_json || ducknng_json_extract_u64_value(reply_json, "session_id", &t->session_id) != 0 ||
            t->session_id == 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: upload_open reply did not include session_id");
        goto done;
    }
    t->session_token = ducknng_json_extract_string_dup(reply_json, "session_token");
    if (!t->session_token || !t->session_token[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: upload_open reply did not include session_token");
        goto done;
    }
    t->session_open = 1;
    duckdb_free(reply_json); reply_json = NULL;
    nng_msg_free(resp); resp = NULL;

    {
        /* Adaptive chunk batching: start at one chunk, then size the next frame
         * from the observed bytes/chunk so each upload_append targets ~4 MiB and
         * stays under the server's 16 MiB max_request_bytes for any row width. */
        idx_t batch_chunks = 1;
        size_t token_len = t->session_token ? strlen(t->session_token) : 0;
        for (chunk_index = 0;;) {
            uint8_t *qbytes = NULL;
            size_t qlen = 0;
            int has_chunk = 0;
            uint64_t running = 0;
            idx_t before = chunk_index;
            idx_t n_in_batch;
            /* Encode a batch, shrinking the chunk count and re-encoding if the
             * framed body (prefix + quack) would exceed the server's limit -- the
             * adaptive estimate can undershoot when later chunks are far wider
             * than earlier ones. A single chunk that still exceeds it is sent as
             * is (an inherent per-chunk limit the server reports, no worse than
             * the pre-batching one-chunk-per-frame behavior). */
            for (;;) {
                chunk_index = before;
                if (ducknng_result_materialized_chunks_to_quack_payload(result, &chunk_index,
                        (uint64_t)batch_chunks, 1, &qbytes, &qlen, &has_chunk, errmsg) != 0)
                    goto do_abort;
                if (!has_chunk) break;
                if (batch_chunks > 1 &&
                    (uint64_t)qlen + 10u + (uint64_t)token_len > DUCKNNG_UPLOAD_APPEND_MAX_BYTES) {
                    duckdb_free(qbytes);
                    qbytes = NULL;
                    batch_chunks /= 2;
                    continue;
                }
                break;
            }
            if (!has_chunk) { if (qbytes) duckdb_free(qbytes); break; }
            if (ducknng_upload_append_one(t, qbytes, qlen, tls_opts, &running, errmsg) != 0) {
                duckdb_free(qbytes);
                goto do_abort;
            }
            n_in_batch = chunk_index - before;
            if (n_in_batch > 0 && qlen > 0) {
                size_t per = qlen / (size_t)n_in_batch;
                size_t want = per ? (DUCKNNG_UPLOAD_APPEND_TARGET_BYTES / per) : DUCKNNG_UPLOAD_APPEND_MAX_CHUNKS;
                if (want < 1) want = 1;
                if (want > DUCKNNG_UPLOAD_APPEND_MAX_CHUNKS) want = DUCKNNG_UPLOAD_APPEND_MAX_CHUNKS;
                batch_chunks = (idx_t)want;
            }
            duckdb_free(qbytes);
            ub->rows_uploaded = running;
            ub->bytes_uploaded += (uint64_t)qlen;
            sent_any = 1;
        }
    }
    if (!sent_any) {
        /* A zero-row source still sends one schema-only batch so the server
         * validates the source's column names/count/types against the target
         * (catching schema drift) instead of committing 0 rows unchecked. */
        uint8_t *qbytes = NULL;
        size_t qlen = 0;
        if (ducknng_result_empty_quack_payload(result, &qbytes, &qlen, errmsg) != 0) goto do_abort;
        if (ducknng_upload_append_one(t, qbytes, qlen, tls_opts, NULL, errmsg) != 0) {
            duckdb_free(qbytes);
            goto do_abort;
        }
        ub->bytes_uploaded += (uint64_t)qlen;
        duckdb_free(qbytes);
    }

    resp = ducknng_upload_control(t, "upload_commit", tls_opts, errmsg);
    if (!resp) goto do_abort;
    if (ducknng_decode_request(resp, &frame) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid upload_commit response envelope");
        goto do_abort;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        if (errmsg && !*errmsg) *errmsg = ducknng_frame_error_detail(&frame, "ducknng: upload_commit failed");
        goto do_abort;
    }
    reply_json = ducknng_dup_bytes(frame.payload, (size_t)frame.payload_len);
    {
        uint64_t committed_rows = 0;
        if (reply_json && ducknng_json_extract_u64_value(reply_json, "rows", &committed_rows) == 0)
            ub->rows_uploaded = committed_rows;
    }
    if (reply_json) { duckdb_free(reply_json); reply_json = NULL; }
    nng_msg_free(resp); resp = NULL;
    t->session_open = 0; /* a successful commit closes the session server-side */
    rc = 0;
    goto done;

do_abort:
    /* Best-effort abort so the server rolls back partial rows; keep the original
     * error in *errmsg (do not let the abort roundtrip overwrite it). */
    if (t->session_open && t->session_id && t->session_token) {
        nng_msg *ar = ducknng_upload_control(t, "upload_abort", tls_opts, NULL);
        if (ar) nng_msg_free(ar);
        t->session_open = 0;
    }
done:
    if (resp) nng_msg_free(resp);
    if (reply_json) duckdb_free(reply_json);
    if (open_json) duckdb_free(open_json);
    if (have_result) duckdb_destroy_result(&result);
    return rc;
}

static void ducknng_upload_table_bind(duckdb_bind_info info) {
    ducknng_upload_table_bind_data *ub;
    duckdb_value url_val;
    duckdb_value query_val;
    duckdb_value target_val;
    duckdb_value tls_val;
    idx_t param_count;
    duckdb_logical_type bigint;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    param_count = duckdb_bind_get_parameter_count(info);
    if (param_count != 3 && param_count != 4) {
        duckdb_bind_set_error(info, "ducknng: ducknng_upload_table(url, source_query, target_table[, tls_config_id]) requires three or four parameters");
        return;
    }
    url_val = duckdb_bind_get_parameter(info, 0);
    query_val = duckdb_bind_get_parameter(info, 1);
    target_val = duckdb_bind_get_parameter(info, 2);
    if (param_count == 4) tls_val = duckdb_bind_get_parameter(info, 3);
    else memset(&tls_val, 0, sizeof(tls_val));
    ub = (ducknng_upload_table_bind_data *)duckdb_malloc(sizeof(*ub));
    if (!ub) {
        duckdb_destroy_value(&url_val);
        duckdb_destroy_value(&query_val);
        duckdb_destroy_value(&target_val);
        if (param_count == 4) duckdb_destroy_value(&tls_val);
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(ub, 0, sizeof(*ub));
    ub->transport.ctx = ctx;
    ub->transport.url = duckdb_get_varchar(url_val);
    ub->source_query = duckdb_get_varchar(query_val);
    ub->target_table = duckdb_get_varchar(target_val);
    ub->transport.tls_config_id = (param_count == 4) ? (uint64_t)duckdb_get_uint64(tls_val) : 0;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&query_val);
    duckdb_destroy_value(&target_val);
    if (param_count == 4) duckdb_destroy_value(&tls_val);
    if (!ub->transport.url || !ub->source_query || !ub->target_table ||
        !ub->transport.url[0] || !ub->source_query[0] || !ub->target_table[0]) {
        destroy_upload_table_bind_data(ub);
        duckdb_bind_set_error(info, "ducknng: ducknng_upload_table requires non-empty url, source_query, and target_table");
        return;
    }
    /* The upload is a write side effect, so it must run exactly once. DuckDB may
     * invoke a table function's bind multiple times during planning, so the
     * upload is deferred to the first scan (execution) rather than done here. */
    bigint = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_bind_add_result_column(info, "rows_uploaded", bigint);
    duckdb_bind_add_result_column(info, "bytes_uploaded", bigint);
    duckdb_destroy_logical_type(&bigint);
    duckdb_bind_set_bind_data(info, ub, destroy_upload_table_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_upload_table_init(duckdb_init_info info) {
    ducknng_upload_table_bind_data *ub = (ducknng_upload_table_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_upload_table_init_data *init = (ducknng_upload_table_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = ub;
    init->done = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_upload_table_init_data);
}

static void ducknng_upload_table_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_upload_table_init_data *init = (ducknng_upload_table_init_data *)duckdb_function_get_init_data(info);
    ducknng_upload_table_bind_data *ub;
    char *errmsg = NULL;
    int64_t *rows_col;
    int64_t *bytes_col;
    if (!init || !init->bind || init->done) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ub = init->bind;
    init->done = 1; /* set before running so a failure cannot re-trigger the upload */
    /* Run the upload once, here at execution time (not in the repeatedly-invoked bind). */
    if (ducknng_upload_table_run(ub, ub->source_query, &errmsg) != 0) {
        duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: upload_table failed");
        if (errmsg) duckdb_free(errmsg);
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    rows_col = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    bytes_col = (int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
    rows_col[0] = (int64_t)ub->rows_uploaded;
    bytes_col[0] = (int64_t)ub->bytes_uploaded;
    duckdb_data_chunk_set_size(output, 1);
}

static int register_upload_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type p3[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type p4[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, name, ctx, 3, p3, ducknng_upload_table_bind,
            ducknng_upload_table_init, ducknng_upload_table_scan)) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 4, p4, ducknng_upload_table_bind,
        ducknng_upload_table_init, ducknng_upload_table_scan);
}

static int register_remote_table_named_mode(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 4, param_types, ducknng_query_rpc_bind,
        ducknng_query_rpc_init, ducknng_query_rpc_scan);
}

static int register_remote_table_params(duckdb_connection con,
    ducknng_sql_context *ctx) {
    duckdb_type param_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_ANY, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_query_rpc_params", ctx, 4,
        param_types, ducknng_query_rpc_params_bind, ducknng_query_rpc_init,
        ducknng_query_rpc_scan);
}

static int register_prepare_query_tables(duckdb_connection con,
    ducknng_sql_context *ctx) {
    duckdb_type plain[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT};
    duckdb_type parameterized[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_ANY, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_prepare_query", ctx, 3, plain,
            ducknng_prepare_query_bind, ducknng_query_rpc_init,
            ducknng_prepare_query_scan)) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_prepare_query_params", ctx, 4,
        parameterized, ducknng_prepare_query_bind, ducknng_query_rpc_init,
        ducknng_prepare_query_scan);
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

static int register_exec_params_result_table(duckdb_connection con,
    ducknng_sql_context *ctx) {
    duckdb_type param_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_ANY, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, "ducknng_run_rpc_params", ctx, 4,
        param_types, ducknng_run_rpc_params_bind, ducknng_single_row_init,
        ducknng_run_rpc_scan);
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
            param_types, ducknng_ncurl_row_scalar, NULL, 6, field_types, field_names)) return 0;
    if (!ducknng_register_struct_row_scalar_named(con, ctx, "ducknng__ncurl_row", 7,
            profile_param_types, ducknng_ncurl_row_scalar, ducknng_sql_connection_bind_cb,
            6, field_types, field_names)) return 0;
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
    if (!register_remote_table_params(connection, ctx)) return 0;
    if (!register_prepare_query_tables(connection, ctx)) return 0;
    if (!register_upload_table_named(connection, ctx, "ducknng_upload_table")) return 0;
    if (!register_remote_table_named_mode(connection, ctx, "ducknng_query_rpc_mode")) return 0;
    if (!register_manifest_result_table_named(connection, ctx, "ducknng_get_rpc_manifest")) return 0;
    if (!register_exec_result_table_named(connection, ctx, "ducknng_run_rpc")) return 0;
    if (!register_exec_params_result_table(connection, ctx)) return 0;
    if (!register_request_result_table_named(connection, ctx, "ducknng_request")) return 0;
    if (!register_request_socket_result_table_named(connection, ctx, "ducknng_request_socket")) return 0;
    if (!register_http_result_table_named(connection, ctx, "ducknng_ncurl")) return 0;
    return 1;
}
