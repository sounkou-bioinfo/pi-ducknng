#include "ducknng_sql_shared.h"
#include "ducknng_net_backend.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <nng/nng.h>
#include <stdatomic.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    uint64_t service_id;
    char *name;
    char *listen;
    int32_t contexts;
    bool running;
    char *execution_model;
    uint64_t sessions;
    uint64_t active_pipes;
    uint64_t max_open_sessions;
    uint64_t max_active_pipes;
    uint64_t inflight_requests;
    uint64_t max_inflight_requests;
    uint64_t max_sessions_per_peer_identity;
    uint64_t max_inflight_per_peer_identity;
    uint64_t max_reply_bytes_per_peer_identity;
    uint64_t max_session_open_rate_per_peer_identity;
    bool tls_enabled;
    int32_t tls_auth_mode;
    bool peer_identity_required;
    bool peer_allowlist_active;
    uint64_t peer_allowlist_count;
    bool ip_allowlist_active;
    uint64_t ip_allowlist_count;
    bool sql_authorizer_active;
} ducknng_server_row;

typedef struct {
    ducknng_server_row *rows;
    idx_t row_count;
    ducknng_runtime *rt;
} ducknng_servers_bind_data;

typedef struct {
    ducknng_servers_bind_data *bind;
    idx_t offset;
} ducknng_servers_init_data;

static void destroy_servers_bind_data(void *ptr) {
    ducknng_servers_bind_data *data = (ducknng_servers_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].name) duckdb_free(data->rows[i].name);
        if (data->rows[i].listen) duckdb_free(data->rows[i].listen);
        if (data->rows[i].execution_model) duckdb_free(data->rows[i].execution_model);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_servers_init_data(void *ptr) {
    ducknng_servers_init_data *data = (ducknng_servers_init_data *)ptr;
    if (data) duckdb_free(data);
}

static ducknng_service *ducknng_sql_find_service_by_name(ducknng_runtime *rt, const char *name) {
    size_t i;
    if (!rt || !name) return NULL;
    for (i = 0; i < rt->service_count; i++) {
        if (rt->services[i] && rt->services[i]->name && strcmp(rt->services[i]->name, name) == 0) return rt->services[i];
    }
    return NULL;
}

static void ducknng_set_service_execution_model_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *model = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !name || !name[0] || !model || !model[0]) {
            if (name) duckdb_free(name);
            if (model) duckdb_free(model);
            duckdb_scalar_function_set_error(info, "ducknng: service name and execution model are required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        svc = ducknng_sql_find_service_by_name(ctx->rt, name);
        if (!svc) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            duckdb_free(model);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        if (ducknng_service_set_execution_model(svc, model, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            duckdb_free(model);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service execution model");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        duckdb_free(model);
        out[row] = true;
    }
}

static void ducknng_set_service_limits_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t max_open_sessions = arg_u64(duckdb_data_chunk_get_vector(input, 1), row, 0);
        uint64_t max_active_pipes = 0;
        uint64_t max_inflight_requests = 0;
        uint64_t max_sessions_per_peer_identity = 0;
        uint64_t max_inflight_per_peer_identity = 0;
        uint64_t max_reply_bytes_per_peer_identity = 0;
        uint64_t max_session_open_rate_per_peer_identity = 0;
        ducknng_service *svc = NULL;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: service name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        svc = ducknng_sql_find_service_by_name(ctx->rt, name);
        if (!svc) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        max_active_pipes = ncols > 2 ? arg_u64(duckdb_data_chunk_get_vector(input, 2), row, 0) : ducknng_service_max_active_pipes(svc);
        max_inflight_requests = ncols > 3 ? arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 0) : ducknng_service_max_inflight_requests(svc);
        max_sessions_per_peer_identity = ncols > 4 ? arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0) : ducknng_service_max_sessions_per_peer_identity(svc);
        max_inflight_per_peer_identity = ncols > 5 ? arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0) : ducknng_service_max_inflight_per_peer_identity(svc);
        max_reply_bytes_per_peer_identity = ncols > 6 ? arg_u64(duckdb_data_chunk_get_vector(input, 6), row, 0) : ducknng_service_max_reply_bytes_per_peer_identity(svc);
        max_session_open_rate_per_peer_identity = ncols > 7 ? arg_u64(duckdb_data_chunk_get_vector(input, 7), row, 0) : ducknng_service_max_session_open_rate_per_peer_identity(svc);
        if (ducknng_service_set_limits(svc, max_open_sessions, max_active_pipes, max_inflight_requests, max_sessions_per_peer_identity, max_inflight_per_peer_identity, max_reply_bytes_per_peer_identity, max_session_open_rate_per_peer_identity, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set service limits");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        out[row] = true;
    }
}

static void ducknng_servers_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_servers_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_servers_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->rt = ctx->rt;

    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->service_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_server_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            ducknng_service *svc = ctx->rt->services[i];
            bind->rows[i].service_id = svc ? svc->service_id : 0;
            bind->rows[i].name = svc && svc->name ? ducknng_strdup(svc->name) : NULL;
            bind->rows[i].listen = svc && ducknng_service_resolved_listen(svc) ? ducknng_strdup(ducknng_service_resolved_listen(svc)) : NULL;
            bind->rows[i].contexts = svc ? svc->ncontexts : 0;
            bind->rows[i].running = svc ? (bool)svc->running : false;
            bind->rows[i].execution_model = svc ? ducknng_strdup(ducknng_service_execution_model(svc)) : NULL;
            bind->rows[i].sessions = svc ? (uint64_t)atomic_load_explicit(&svc->session_count_visible, memory_order_acquire) : 0;
            bind->rows[i].active_pipes = svc ? (uint64_t)ducknng_service_active_pipe_count(svc) : 0;
            bind->rows[i].max_open_sessions = svc ? ducknng_service_max_open_sessions(svc) : 0;
            bind->rows[i].max_active_pipes = svc ? ducknng_service_max_active_pipes(svc) : 0;
            bind->rows[i].inflight_requests = svc ? (uint64_t)ducknng_service_inflight_request_count(svc) : 0;
            bind->rows[i].max_inflight_requests = svc ? ducknng_service_max_inflight_requests(svc) : 0;
            bind->rows[i].max_sessions_per_peer_identity = svc ? ducknng_service_max_sessions_per_peer_identity(svc) : 0;
            bind->rows[i].max_inflight_per_peer_identity = svc ? ducknng_service_max_inflight_per_peer_identity(svc) : 0;
            bind->rows[i].max_reply_bytes_per_peer_identity = svc ? ducknng_service_max_reply_bytes_per_peer_identity(svc) : 0;
            bind->rows[i].max_session_open_rate_per_peer_identity = svc ? ducknng_service_max_session_open_rate_per_peer_identity(svc) : 0;
            bind->rows[i].tls_enabled = svc ? (bool)svc->tls_enabled : false;
            bind->rows[i].tls_auth_mode = svc ? svc->tls_opts.auth_mode : 0;
            bind->rows[i].peer_identity_required = svc ? (bool)ducknng_service_requires_peer_identity(svc) : false;
            bind->rows[i].peer_allowlist_active = svc ? (bool)ducknng_service_peer_allowlist_active(svc) : false;
            bind->rows[i].peer_allowlist_count = svc ? (uint64_t)ducknng_service_peer_allowlist_count(svc) : 0;
            bind->rows[i].ip_allowlist_active = svc ? (bool)ducknng_service_ip_allowlist_active(svc) : false;
            bind->rows[i].ip_allowlist_count = svc ? (uint64_t)ducknng_service_ip_allowlist_count(svc) : 0;
            bind->rows[i].sql_authorizer_active = svc ? (bool)ducknng_service_authorizer_active(svc) : false;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "service_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "contexts", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "running", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "execution_model", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "sessions", type);
    duckdb_bind_add_result_column(info, "active_pipes", type);
    duckdb_bind_add_result_column(info, "max_open_sessions", type);
    duckdb_bind_add_result_column(info, "max_active_pipes", type);
    duckdb_bind_add_result_column(info, "inflight_requests", type);
    duckdb_bind_add_result_column(info, "max_inflight_requests", type);
    duckdb_bind_add_result_column(info, "max_sessions_per_peer_identity", type);
    duckdb_bind_add_result_column(info, "max_inflight_per_peer_identity", type);
    duckdb_bind_add_result_column(info, "max_reply_bytes_per_peer_identity", type);
    duckdb_bind_add_result_column(info, "max_session_open_rate_per_peer_identity", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "tls_enabled", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "tls_auth_mode", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "peer_identity_required", type);
    duckdb_bind_add_result_column(info, "peer_allowlist_active", type);
    duckdb_bind_add_result_column(info, "ip_allowlist_active", type);
    duckdb_bind_add_result_column(info, "sql_authorizer_active", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "peer_allowlist_count", type);
    duckdb_bind_add_result_column(info, "ip_allowlist_count", type);
    duckdb_destroy_logical_type(&type);

    duckdb_bind_set_bind_data(info, bind, destroy_servers_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_servers_init(duckdb_init_info info) {
    ducknng_servers_bind_data *bind = (ducknng_servers_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_servers_init_data *init = (ducknng_servers_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_servers_init_data);
}

static void ducknng_servers_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_servers_init_data *init = (ducknng_servers_init_data *)duckdb_function_get_init_data(info);
    ducknng_servers_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    duckdb_vector vec_service_id;
    duckdb_vector vec_name;
    duckdb_vector vec_listen;
    duckdb_vector vec_contexts;
    duckdb_vector vec_running;
    duckdb_vector vec_execution_model;
    duckdb_vector vec_sessions;
    duckdb_vector vec_active_pipes;
    duckdb_vector vec_max_open_sessions;
    duckdb_vector vec_max_active_pipes;
    duckdb_vector vec_inflight_requests;
    duckdb_vector vec_max_inflight_requests;
    duckdb_vector vec_max_sessions_per_peer_identity;
    duckdb_vector vec_max_inflight_per_peer_identity;
    duckdb_vector vec_max_reply_bytes_per_peer_identity;
    duckdb_vector vec_max_session_open_rate_per_peer_identity;
    duckdb_vector vec_tls_enabled;
    duckdb_vector vec_tls_auth_mode;
    duckdb_vector vec_peer_identity_required;
    duckdb_vector vec_peer_allowlist_active;
    duckdb_vector vec_ip_allowlist_active;
    duckdb_vector vec_sql_authorizer_active;
    duckdb_vector vec_peer_allowlist_count;
    duckdb_vector vec_ip_allowlist_count;
    uint64_t *service_ids;
    int32_t *contexts;
    bool *running;
    uint64_t *sessions;
    uint64_t *active_pipes;
    uint64_t *max_open_sessions;
    uint64_t *max_active_pipes;
    uint64_t *inflight_requests;
    uint64_t *max_inflight_requests;
    uint64_t *max_sessions_per_peer_identity;
    uint64_t *max_inflight_per_peer_identity;
    uint64_t *max_reply_bytes_per_peer_identity;
    uint64_t *max_session_open_rate_per_peer_identity;
    bool *tls_enabled;
    int32_t *tls_auth_mode;
    bool *peer_identity_required;
    bool *peer_allowlist_active;
    bool *ip_allowlist_active;
    bool *sql_authorizer_active;
    uint64_t *peer_allowlist_count;
    uint64_t *ip_allowlist_count;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;

    vec_service_id = duckdb_data_chunk_get_vector(output, 0);
    vec_name = duckdb_data_chunk_get_vector(output, 1);
    vec_listen = duckdb_data_chunk_get_vector(output, 2);
    vec_contexts = duckdb_data_chunk_get_vector(output, 3);
    vec_running = duckdb_data_chunk_get_vector(output, 4);
    vec_execution_model = duckdb_data_chunk_get_vector(output, 5);
    vec_sessions = duckdb_data_chunk_get_vector(output, 6);
    vec_active_pipes = duckdb_data_chunk_get_vector(output, 7);
    vec_max_open_sessions = duckdb_data_chunk_get_vector(output, 8);
    vec_max_active_pipes = duckdb_data_chunk_get_vector(output, 9);
    vec_inflight_requests = duckdb_data_chunk_get_vector(output, 10);
    vec_max_inflight_requests = duckdb_data_chunk_get_vector(output, 11);
    vec_max_sessions_per_peer_identity = duckdb_data_chunk_get_vector(output, 12);
    vec_max_inflight_per_peer_identity = duckdb_data_chunk_get_vector(output, 13);
    vec_max_reply_bytes_per_peer_identity = duckdb_data_chunk_get_vector(output, 14);
    vec_max_session_open_rate_per_peer_identity = duckdb_data_chunk_get_vector(output, 15);
    vec_tls_enabled = duckdb_data_chunk_get_vector(output, 16);
    vec_tls_auth_mode = duckdb_data_chunk_get_vector(output, 17);
    vec_peer_identity_required = duckdb_data_chunk_get_vector(output, 18);
    vec_peer_allowlist_active = duckdb_data_chunk_get_vector(output, 19);
    vec_ip_allowlist_active = duckdb_data_chunk_get_vector(output, 20);
    vec_sql_authorizer_active = duckdb_data_chunk_get_vector(output, 21);
    vec_peer_allowlist_count = duckdb_data_chunk_get_vector(output, 22);
    vec_ip_allowlist_count = duckdb_data_chunk_get_vector(output, 23);

    service_ids = (uint64_t *)duckdb_vector_get_data(vec_service_id);
    contexts = (int32_t *)duckdb_vector_get_data(vec_contexts);
    running = (bool *)duckdb_vector_get_data(vec_running);
    sessions = (uint64_t *)duckdb_vector_get_data(vec_sessions);
    active_pipes = (uint64_t *)duckdb_vector_get_data(vec_active_pipes);
    max_open_sessions = (uint64_t *)duckdb_vector_get_data(vec_max_open_sessions);
    max_active_pipes = (uint64_t *)duckdb_vector_get_data(vec_max_active_pipes);
    inflight_requests = (uint64_t *)duckdb_vector_get_data(vec_inflight_requests);
    max_inflight_requests = (uint64_t *)duckdb_vector_get_data(vec_max_inflight_requests);
    max_sessions_per_peer_identity = (uint64_t *)duckdb_vector_get_data(vec_max_sessions_per_peer_identity);
    max_inflight_per_peer_identity = (uint64_t *)duckdb_vector_get_data(vec_max_inflight_per_peer_identity);
    max_reply_bytes_per_peer_identity = (uint64_t *)duckdb_vector_get_data(vec_max_reply_bytes_per_peer_identity);
    max_session_open_rate_per_peer_identity = (uint64_t *)duckdb_vector_get_data(vec_max_session_open_rate_per_peer_identity);
    tls_enabled = (bool *)duckdb_vector_get_data(vec_tls_enabled);
    tls_auth_mode = (int32_t *)duckdb_vector_get_data(vec_tls_auth_mode);
    peer_identity_required = (bool *)duckdb_vector_get_data(vec_peer_identity_required);
    peer_allowlist_active = (bool *)duckdb_vector_get_data(vec_peer_allowlist_active);
    ip_allowlist_active = (bool *)duckdb_vector_get_data(vec_ip_allowlist_active);
    sql_authorizer_active = (bool *)duckdb_vector_get_data(vec_sql_authorizer_active);
    peer_allowlist_count = (uint64_t *)duckdb_vector_get_data(vec_peer_allowlist_count);
    ip_allowlist_count = (uint64_t *)duckdb_vector_get_data(vec_ip_allowlist_count);

    for (i = 0; i < chunk_size; i++) {
        ducknng_server_row *row = &bind->rows[init->offset + i];
        uint64_t live_inflight = row->inflight_requests;
        if (bind->rt && row->service_id > 0) {
            size_t si;
            ducknng_mutex_lock(&bind->rt->mu);
            for (si = 0; si < bind->rt->service_count; si++) {
                if (bind->rt->services[si] && bind->rt->services[si]->service_id == row->service_id) {
                    live_inflight = (uint64_t)ducknng_service_inflight_request_count(bind->rt->services[si]);
                    break;
                }
            }
            ducknng_mutex_unlock(&bind->rt->mu);
        }
        service_ids[i] = row->service_id;
        contexts[i] = row->contexts;
        running[i] = row->running;
        sessions[i] = row->sessions;
        active_pipes[i] = row->active_pipes;
        max_open_sessions[i] = row->max_open_sessions;
        max_active_pipes[i] = row->max_active_pipes;
        inflight_requests[i] = live_inflight;
        max_inflight_requests[i] = row->max_inflight_requests;
        max_sessions_per_peer_identity[i] = row->max_sessions_per_peer_identity;
        max_inflight_per_peer_identity[i] = row->max_inflight_per_peer_identity;
        max_reply_bytes_per_peer_identity[i] = row->max_reply_bytes_per_peer_identity;
        max_session_open_rate_per_peer_identity[i] = row->max_session_open_rate_per_peer_identity;
        tls_enabled[i] = row->tls_enabled;
        tls_auth_mode[i] = row->tls_auth_mode;
        peer_identity_required[i] = row->peer_identity_required;
        peer_allowlist_active[i] = row->peer_allowlist_active;
        ip_allowlist_active[i] = row->ip_allowlist_active;
        sql_authorizer_active[i] = row->sql_authorizer_active;
        peer_allowlist_count[i] = row->peer_allowlist_count;
        ip_allowlist_count[i] = row->ip_allowlist_count;
        if (row->name) duckdb_unsafe_vector_assign_string_element_len(vec_name, i, row->name, (idx_t)strlen(row->name));
        else set_null(vec_name, i);
        if (row->listen) duckdb_unsafe_vector_assign_string_element_len(vec_listen, i, row->listen, (idx_t)strlen(row->listen));
        else set_null(vec_listen, i);
        if (row->execution_model) duckdb_unsafe_vector_assign_string_element_len(vec_execution_model, i, row->execution_model, (idx_t)strlen(row->execution_model));
        else set_null(vec_execution_model, i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_service_inflight_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        uint64_t inflight = 0;
        if (ctx && ctx->rt && name && name[0]) {
            ducknng_service *svc;
            ducknng_mutex_lock(&ctx->rt->mu);
            svc = ducknng_sql_find_service_by_name(ctx->rt, name);
            if (svc) inflight = (uint64_t)ducknng_service_inflight_request_count(svc);
            ducknng_mutex_unlock(&ctx->rt->mu);
        }
        if (name) duckdb_free(name);
        out[row] = inflight;
    }
}

static void ducknng_nng_version_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    (void)info;
    idx_t count = duckdb_data_chunk_get_size(input);
    const char *ver = nng_version();
    for (idx_t row = 0; row < count; row++) {
        duckdb_vector_assign_string_element(output, row, ver);
    }
}

static void ducknng_transport_capabilities_scalar(duckdb_function_info info,
    duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    char *json = ducknng_net_caps_to_json(ducknng_net_backend_get()->capabilities());
    idx_t row;
    if (!json) {
        duckdb_scalar_function_set_error(info, "ducknng: out of memory rendering transport capabilities");
        return;
    }
    for (row = 0; row < count; row++) {
        duckdb_vector_assign_string_element(output, row, json);
    }
    duckdb_free(json);
}

typedef struct {
    idx_t offset;
} ducknng_caps_init_data;

static void destroy_caps_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static void ducknng_list_transport_capabilities_bind(duckdb_bind_info info) {
    duckdb_logical_type type;
    size_t count = 0;
    (void)ducknng_net_caps_all(&count);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "target", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "active", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "http", type);
    duckdb_bind_add_result_column(info, "https", type);
    duckdb_bind_add_result_column(info, "http_response_stream", type);
    duckdb_bind_add_result_column(info, "inproc", type);
    duckdb_bind_add_result_column(info, "tcp", type);
    duckdb_bind_add_result_column(info, "ipc", type);
    duckdb_bind_add_result_column(info, "tls_tcp", type);
    duckdb_bind_add_result_column(info, "websocket", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "async_is_real", type);
    duckdb_bind_add_result_column(info, "honors_timeout", type);
    duckdb_bind_add_result_column(info, "honors_cancel", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "tls_owner", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_cardinality(info, (idx_t)count, true);
}

static void ducknng_list_transport_capabilities_init(duckdb_init_info info) {
    ducknng_caps_init_data *init =
        (ducknng_caps_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_caps_init_data);
}

static void ducknng_list_transport_capabilities_scan(duckdb_function_info info,
    duckdb_data_chunk output) {
    ducknng_caps_init_data *init =
        (ducknng_caps_init_data *)duckdb_function_get_init_data(info);
    size_t count = 0;
    const ducknng_net_caps *all = ducknng_net_caps_all(&count);
    const ducknng_net_caps *active = ducknng_net_backend_get()->capabilities();
    bool *actives;
    bool *async_real;
    bool *timeouts;
    bool *cancels;
    idx_t emitted = 0;
    idx_t i;

    if (!init || !all || init->offset >= (idx_t)count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    actives = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
    async_real = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10));
    timeouts = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11));
    cancels = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 12));
    for (i = init->offset; i < (idx_t)count && emitted < duckdb_vector_size(); i++, emitted++) {
        const ducknng_net_caps *row = &all[i];
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 0), emitted,
            row->backend_name ? row->backend_name : "unknown");
        actives[emitted] = row == active;
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 2), emitted,
            ducknng_net_cap_name(row->http));
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 3), emitted,
            ducknng_net_cap_name(row->https));
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 4), emitted,
            ducknng_net_cap_name(row->http_response_stream));
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 5), emitted,
            ducknng_net_cap_name(row->inproc));
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 6), emitted,
            ducknng_net_cap_name(row->tcp));
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 7), emitted,
            ducknng_net_cap_name(row->ipc));
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 8), emitted,
            ducknng_net_cap_name(row->tls_tcp));
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 9), emitted,
            ducknng_net_cap_name(row->websocket));
        async_real[emitted] = row->async_is_real != 0;
        timeouts[emitted] = row->honors_timeout != 0;
        cancels[emitted] = row->honors_cancel != 0;
        duckdb_vector_assign_string_element(duckdb_data_chunk_get_vector(output, 13), emitted,
            ducknng_net_tls_owner_name(row->tls_owner));
    }
    init->offset = i;
    duckdb_data_chunk_set_size(output, emitted);
}

static void ducknng_set_execution_pool_max_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: runtime is not available");
        return;
    }
    for (row = 0; row < count; row++) {
        uint64_t requested = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        uint64_t effective = 0;
        char *errmsg = NULL;
        if (ducknng_runtime_set_execution_pool_max(ctx->rt, requested, &effective, &errmsg) != 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set execution pool max");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = effective;
    }
}

static void ducknng_execution_pool_max_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: runtime is not available");
        return;
    }
    for (row = 0; row < count; row++) out[row] = ducknng_runtime_execution_pool_max(ctx->rt);
}

int ducknng_register_sql_service(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type service_limits_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type service_limits_extended_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type service_limits_full_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type service_limits_identity_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type service_limits_peer_identity6_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type service_limits_peer_identity7_types[7] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type service_limits_peer_identity8_types[8] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    duckdb_type execution_model_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_nng_version", 0, ducknng_nng_version_scalar, ctx, NULL, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_transport_capabilities", 0, ducknng_transport_capabilities_scalar, ctx, NULL, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_list_transport_capabilities", ctx, 0, NULL,
            ducknng_list_transport_capabilities_bind, ducknng_list_transport_capabilities_init,
            ducknng_list_transport_capabilities_scan)) return 0;
    {
        duckdb_type inflight_types[1] = {DUCKDB_TYPE_VARCHAR};
        if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_service_inflight", 1, ducknng_service_inflight_scalar, ctx, inflight_types, DUCKDB_TYPE_UBIGINT)) return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_limits", 2, ducknng_set_service_limits_scalar, ctx, service_limits_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_limits", 3, ducknng_set_service_limits_scalar, ctx, service_limits_extended_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_limits", 4, ducknng_set_service_limits_scalar, ctx, service_limits_full_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_limits", 5, ducknng_set_service_limits_scalar, ctx, service_limits_identity_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_limits", 6, ducknng_set_service_limits_scalar, ctx, service_limits_peer_identity6_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_limits", 7, ducknng_set_service_limits_scalar, ctx, service_limits_peer_identity7_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_limits", 8, ducknng_set_service_limits_scalar, ctx, service_limits_peer_identity8_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_service_execution_model", 2, ducknng_set_service_execution_model_scalar, ctx, execution_model_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    {
        duckdb_type pool_max_types[1] = {DUCKDB_TYPE_UBIGINT};
        if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_execution_pool_max", 1, ducknng_set_execution_pool_max_scalar, ctx, pool_max_types, DUCKDB_TYPE_UBIGINT)) return 0;
        if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_execution_pool_max", 0, ducknng_execution_pool_max_scalar, ctx, NULL, DUCKDB_TYPE_UBIGINT)) return 0;
    }
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_list_servers", ctx, 0, NULL,
            ducknng_servers_bind, ducknng_servers_init, ducknng_servers_scan)) return 0;
    return 1;
}
