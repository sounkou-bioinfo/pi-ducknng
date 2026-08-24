#include "ducknng_sql_shared.h"
#include "ducknng_manifest.h"
#include "ducknng_registry.h"
#include "ducknng_runtime.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <stdatomic.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    char *name;
    char *family;
    char *summary;
    char *transport_pattern;
    char *request_payload_format;
    char *response_payload_format;
    char *response_mode;
    char *session_behavior;
    bool requires_auth;
    bool requires_session;
    bool opens_session;
    bool closes_session;
    bool mutates_state;
    bool idempotent;
    bool deprecated;
    bool disabled;
    uint32_t accepted_request_flags;
    uint32_t emitted_reply_flags;
    uint64_t max_request_bytes;
    uint64_t max_reply_bytes;
    int32_t version_introduced;
    char *request_schema_json;
    char *response_schema_json;
} ducknng_method_row;

typedef struct {
    ducknng_method_row *rows;
    idx_t row_count;
} ducknng_methods_bind_data;

typedef struct {
    ducknng_methods_bind_data *bind;
    idx_t offset;
} ducknng_methods_init_data;

static void destroy_methods_bind_data(void *ptr) {
    ducknng_methods_bind_data *data = (ducknng_methods_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].name) duckdb_free(data->rows[i].name);
        if (data->rows[i].family) duckdb_free(data->rows[i].family);
        if (data->rows[i].summary) duckdb_free(data->rows[i].summary);
        if (data->rows[i].transport_pattern) duckdb_free(data->rows[i].transport_pattern);
        if (data->rows[i].request_payload_format) duckdb_free(data->rows[i].request_payload_format);
        if (data->rows[i].response_payload_format) duckdb_free(data->rows[i].response_payload_format);
        if (data->rows[i].response_mode) duckdb_free(data->rows[i].response_mode);
        if (data->rows[i].session_behavior) duckdb_free(data->rows[i].session_behavior);
        if (data->rows[i].request_schema_json) duckdb_free(data->rows[i].request_schema_json);
        if (data->rows[i].response_schema_json) duckdb_free(data->rows[i].response_schema_json);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_methods_init_data(void *ptr) {
    ducknng_methods_init_data *data = (ducknng_methods_init_data *)ptr;
    if (data) duckdb_free(data);
}

static int ducknng_strdup_field(char **out, const char *value) {
    if (!out) return 0;
    *out = NULL;
    if (!value) return 1;
    *out = ducknng_strdup(value);
    return *out != NULL;
}

static int ducknng_method_row_copy(ducknng_method_row *row,
    const ducknng_method_projection *view) {
    if (!row || !view) return 0;

    if (!ducknng_strdup_field(&row->name, view->name)) return 0;
    if (!ducknng_strdup_field(&row->family, view->family)) return 0;
    if (!ducknng_strdup_field(&row->summary, view->summary)) return 0;
    if (!ducknng_strdup_field(&row->transport_pattern, view->transport_pattern)) return 0;
    if (!ducknng_strdup_field(&row->request_payload_format, view->request_payload_format)) return 0;
    if (!ducknng_strdup_field(&row->response_payload_format, view->response_payload_format)) return 0;
    if (!ducknng_strdup_field(&row->response_mode, view->response_mode)) return 0;
    if (!ducknng_strdup_field(&row->session_behavior, view->session_behavior)) return 0;
    if (!ducknng_strdup_field(&row->request_schema_json, view->request_schema_json)) return 0;
    if (!ducknng_strdup_field(&row->response_schema_json, view->response_schema_json)) return 0;

    row->requires_auth = view->requires_auth ? true : false;
    row->requires_session = view->requires_session ? true : false;
    row->opens_session = view->opens_session ? true : false;
    row->closes_session = view->closes_session ? true : false;
    row->mutates_state = view->mutates_state ? true : false;
    row->idempotent = view->idempotent ? true : false;
    row->deprecated = view->deprecated ? true : false;
    row->disabled = view->disabled ? true : false;
    row->accepted_request_flags = view->accepted_request_flags;
    row->emitted_reply_flags = view->emitted_reply_flags;
    row->max_request_bytes = (uint64_t)view->max_request_bytes;
    row->max_reply_bytes = (uint64_t)view->max_reply_bytes;
    row->version_introduced = (int32_t)view->version_introduced;
    return 1;
}

static void ducknng_methods_assign_string(duckdb_vector vec, idx_t row,
    const char *value) {
    if (value) {
        duckdb_unsafe_vector_assign_string_element_len(vec, row, value,
            (idx_t)strlen(value));
    } else {
        set_null(vec, row);
    }
}

static void ducknng_register_exec_method_scalar(duckdb_function_info info,
    duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx;
    bool *out;
    char *errmsg = NULL;
    idx_t count;
    idx_t ncols;
    idx_t row;

    ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;

    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: missing runtime");
        return;
    }

    out = (bool *)duckdb_vector_get_data(output);
    count = duckdb_data_chunk_get_size(input);
    ncols = duckdb_data_chunk_get_column_count(input);

    for (row = 0; row < count; row++) {
        int requires_auth = 0;

        if (ncols > 0) {
            requires_auth = arg_bool(duckdb_data_chunk_get_vector(input, 0),
                row, false) ? 1 : 0;
        }

        ducknng_mutex_lock(&ctx->rt->mu);
        if (!ducknng_register_exec_method_with_auth(ctx->rt, requires_auth,
                &errmsg)) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_scalar_function_set_error(info,
                errmsg ? errmsg : "ducknng: failed to register exec method");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        out[row] = true;
    }
}

static void ducknng_register_upload_methods_scalar(duckdb_function_info info,
    duckdb_data_chunk input, duckdb_vector output) {
    ducknng_sql_context *ctx;
    bool *out;
    char *errmsg = NULL;
    idx_t count;
    idx_t ncols;
    idx_t row;

    ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;

    if (!ctx || !ctx->rt) {
        duckdb_scalar_function_set_error(info, "ducknng: missing runtime");
        return;
    }

    out = (bool *)duckdb_vector_get_data(output);
    count = duckdb_data_chunk_get_size(input);
    ncols = duckdb_data_chunk_get_column_count(input);

    for (row = 0; row < count; row++) {
        int requires_auth = 0;

        if (ncols > 0) {
            requires_auth = arg_bool(duckdb_data_chunk_get_vector(input, 0),
                row, false) ? 1 : 0;
        }

        ducknng_mutex_lock(&ctx->rt->mu);
        if (!ducknng_register_upload_methods_with_auth(ctx->rt, requires_auth, &errmsg)) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_scalar_function_set_error(info,
                errmsg ? errmsg : "ducknng: failed to register upload methods");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        out[row] = true;
    }
}

static int ducknng_method_descriptor_sessionful(const ducknng_method_descriptor *method) {
    if (!method) return 0;
    return method->session_behavior != DUCKNNG_SESSION_STATELESS ||
        method->requires_session || method->opens_session || method->closes_session;
}

static size_t ducknng_runtime_visible_session_count_locked(ducknng_runtime *rt) {
    size_t i;
    size_t total = 0;
    if (!rt) return 0;
    for (i = 0; i < rt->service_count; i++) {
        ducknng_service *svc = rt->services[i];
        if (svc) total += atomic_load_explicit(&svc->session_count_visible, memory_order_acquire);
    }
    return total;
}

static int ducknng_registry_family_sessionful_locked(const ducknng_method_registry *registry, const char *family) {
    size_t i;
    if (!registry || !family || !family[0]) return 0;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (method && method->family && strcmp(method->family, family) == 0 &&
            ducknng_method_descriptor_sessionful(method)) return 1;
    }
    return 0;
}

static void ducknng_set_method_auth_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int requires_auth = arg_bool(duckdb_data_chunk_get_vector(input, 1), row, false) ? 1 : 0;
        char *errmsg = NULL;
        int ok;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: method name is required");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        ok = ducknng_method_registry_set_requires_auth(&ctx->rt->registry, name, requires_auth, &errmsg);
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (!ok) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to update method auth policy");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = true;
    }
}

static void ducknng_unregister_method_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int ok;
        if (!ctx || !ctx->rt || !name || !name[0]) {
            if (name) duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: method name is required");
            return;
        }
        if (strcmp(name, "manifest") == 0) {
            duckdb_free(name);
            duckdb_scalar_function_set_error(info, "ducknng: manifest cannot be unregistered");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        {
            const ducknng_method_descriptor *method = ducknng_method_registry_find(&ctx->rt->registry,
                (const uint8_t *)name, (uint32_t)strlen(name));
            if (!method) {
                ducknng_mutex_unlock(&ctx->rt->mu);
                duckdb_free(name);
                duckdb_scalar_function_set_error(info, "ducknng: method not found");
                return;
            }
            if (ducknng_method_descriptor_sessionful(method) &&
                ducknng_runtime_visible_session_count_locked(ctx->rt) > 0) {
                ducknng_mutex_unlock(&ctx->rt->mu);
                duckdb_free(name);
                duckdb_scalar_function_set_error(info,
                    "ducknng: cannot unregister sessionful method while sessions are open");
                return;
            }
        }
        ok = ducknng_method_registry_unregister(&ctx->rt->registry, name);
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(name);
        if (!ok) {
            duckdb_scalar_function_set_error(info, "ducknng: method not found");
            return;
        }
        out[row] = true;
    }
}

static void ducknng_unregister_family_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        char *family = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        size_t removed;
        if (!ctx || !ctx->rt || !family || !family[0]) {
            if (family) duckdb_free(family);
            duckdb_scalar_function_set_error(info, "ducknng: method family is required");
            return;
        }
        if (strcmp(family, "control") == 0) {
            duckdb_free(family);
            duckdb_scalar_function_set_error(info, "ducknng: control family cannot be unregistered");
            return;
        }
        ducknng_mutex_lock(&ctx->rt->mu);
        if (ducknng_registry_family_sessionful_locked(&ctx->rt->registry, family) &&
            ducknng_runtime_visible_session_count_locked(ctx->rt) > 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(family);
            duckdb_scalar_function_set_error(info,
                "ducknng: cannot unregister sessionful method family while sessions are open");
            return;
        }
        removed = ducknng_method_registry_unregister_family(&ctx->rt->registry, family);
        ducknng_mutex_unlock(&ctx->rt->mu);
        duckdb_free(family);
        out[row] = (uint64_t)removed;
    }
}


static void ducknng_methods_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_methods_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_methods_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));

    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->registry.method_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_method_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            const ducknng_method_descriptor *m = ctx->rt->registry.methods[i];
            ducknng_method_projection view;

            ducknng_method_descriptor_project(m, &view);
            if (!ducknng_method_row_copy(&bind->rows[i], &view)) {
                ducknng_mutex_unlock(&ctx->rt->mu);
                destroy_methods_bind_data(bind);
                duckdb_bind_set_error(info,
                    "ducknng: out of memory copying method descriptor");
                return;
            }
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "name", type);
    duckdb_bind_add_result_column(info, "family", type);
    duckdb_bind_add_result_column(info, "summary", type);
    duckdb_bind_add_result_column(info, "transport_pattern", type);
    duckdb_bind_add_result_column(info, "request_payload_format", type);
    duckdb_bind_add_result_column(info, "response_payload_format", type);
    duckdb_bind_add_result_column(info, "response_mode", type);
    duckdb_bind_add_result_column(info, "session_behavior", type);
    duckdb_bind_add_result_column(info, "request_schema_json", type);
    duckdb_bind_add_result_column(info, "response_schema_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "requires_auth", type);
    duckdb_bind_add_result_column(info, "requires_session", type);
    duckdb_bind_add_result_column(info, "opens_session", type);
    duckdb_bind_add_result_column(info, "closes_session", type);
    duckdb_bind_add_result_column(info, "mutates_state", type);
    duckdb_bind_add_result_column(info, "idempotent", type);
    duckdb_bind_add_result_column(info, "deprecated", type);
    duckdb_bind_add_result_column(info, "disabled", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);
    duckdb_bind_add_result_column(info, "accepted_request_flags", type);
    duckdb_bind_add_result_column(info, "emitted_reply_flags", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "max_request_bytes", type);
    duckdb_bind_add_result_column(info, "max_reply_bytes", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "version_introduced", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_methods_bind_data);
}

static void ducknng_methods_init(duckdb_init_info info) {
    ducknng_methods_bind_data *bind = (ducknng_methods_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_methods_init_data *init = (ducknng_methods_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_methods_init_data);
}

static void ducknng_methods_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_methods_init_data *init = (ducknng_methods_init_data *)duckdb_function_get_init_data(info);
    ducknng_methods_bind_data *bind;
    duckdb_vector name_col;
    duckdb_vector family_col;
    duckdb_vector summary_col;
    duckdb_vector transport_pattern_col;
    duckdb_vector request_payload_format_col;
    duckdb_vector response_payload_format_col;
    duckdb_vector response_mode_col;
    duckdb_vector session_behavior_col;
    duckdb_vector request_schema_json_col;
    duckdb_vector response_schema_json_col;
    bool *requires_auth;
    bool *requires_session;
    bool *opens_session;
    bool *closes_session;
    bool *mutates_state;
    bool *idempotent;
    bool *deprecated;
    bool *disabled;
    uint32_t *accepted_request_flags;
    uint32_t *emitted_reply_flags;
    uint64_t *max_request_bytes;
    uint64_t *max_reply_bytes;
    int32_t *version_introduced;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;

    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }

    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;

    name_col = duckdb_data_chunk_get_vector(output, 0);
    family_col = duckdb_data_chunk_get_vector(output, 1);
    summary_col = duckdb_data_chunk_get_vector(output, 2);
    transport_pattern_col = duckdb_data_chunk_get_vector(output, 3);
    request_payload_format_col = duckdb_data_chunk_get_vector(output, 4);
    response_payload_format_col = duckdb_data_chunk_get_vector(output, 5);
    response_mode_col = duckdb_data_chunk_get_vector(output, 6);
    session_behavior_col = duckdb_data_chunk_get_vector(output, 7);
    request_schema_json_col = duckdb_data_chunk_get_vector(output, 8);
    response_schema_json_col = duckdb_data_chunk_get_vector(output, 9);
    requires_auth = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10));
    requires_session = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11));
    opens_session = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 12));
    closes_session = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 13));
    mutates_state = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 14));
    idempotent = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 15));
    deprecated = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 16));
    disabled = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 17));
    accepted_request_flags = (uint32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 18));
    emitted_reply_flags = (uint32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 19));
    max_request_bytes = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 20));
    max_reply_bytes = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 21));
    version_introduced = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 22));

    for (i = 0; i < chunk_size; i++) {
        ducknng_method_row *row = &bind->rows[init->offset + i];

        ducknng_methods_assign_string(name_col, i, row->name);
        ducknng_methods_assign_string(family_col, i, row->family);
        ducknng_methods_assign_string(summary_col, i, row->summary);
        ducknng_methods_assign_string(transport_pattern_col, i, row->transport_pattern);
        ducknng_methods_assign_string(request_payload_format_col, i,
            row->request_payload_format);
        ducknng_methods_assign_string(response_payload_format_col, i,
            row->response_payload_format);
        ducknng_methods_assign_string(response_mode_col, i, row->response_mode);
        ducknng_methods_assign_string(session_behavior_col, i, row->session_behavior);
        ducknng_methods_assign_string(request_schema_json_col, i,
            row->request_schema_json);
        ducknng_methods_assign_string(response_schema_json_col, i,
            row->response_schema_json);
        requires_auth[i] = row->requires_auth;
        requires_session[i] = row->requires_session;
        opens_session[i] = row->opens_session;
        closes_session[i] = row->closes_session;
        mutates_state[i] = row->mutates_state;
        idempotent[i] = row->idempotent;
        deprecated[i] = row->deprecated;
        disabled[i] = row->disabled;
        accepted_request_flags[i] = row->accepted_request_flags;
        emitted_reply_flags[i] = row->emitted_reply_flags;
        max_request_bytes[i] = row->max_request_bytes;
        max_reply_bytes[i] = row->max_reply_bytes;
        version_introduced[i] = row->version_introduced;
    }

    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_named_methods_table(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 0, NULL, ducknng_methods_bind,
        ducknng_methods_init, ducknng_methods_scan);
}



int ducknng_register_sql_registry(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type method_name_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type method_auth_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN};
    duckdb_type method_family_types[1] = {DUCKDB_TYPE_VARCHAR};
    duckdb_type register_exec_auth_types[1] = {DUCKDB_TYPE_BOOLEAN};

    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_exec_method", 0,
            ducknng_register_exec_method_scalar, ctx, NULL, DUCKDB_TYPE_BOOLEAN)) {
        return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_exec_method", 1,
            ducknng_register_exec_method_scalar, ctx, register_exec_auth_types,
            DUCKDB_TYPE_BOOLEAN)) {
        return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_upload_methods", 0,
            ducknng_register_upload_methods_scalar, ctx, NULL, DUCKDB_TYPE_BOOLEAN)) {
        return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_upload_methods", 1,
            ducknng_register_upload_methods_scalar, ctx, register_exec_auth_types,
            DUCKDB_TYPE_BOOLEAN)) {
        return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_method_auth", 2,
            ducknng_set_method_auth_scalar, ctx, method_auth_types,
            DUCKDB_TYPE_BOOLEAN)) {
        return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_method", 1,
            ducknng_unregister_method_scalar, ctx, method_name_types,
            DUCKDB_TYPE_BOOLEAN)) {
        return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_family", 1,
            ducknng_unregister_family_scalar, ctx, method_family_types,
            DUCKDB_TYPE_UBIGINT)) {
        return 0;
    }
    if (!register_named_methods_table(con, ctx, "ducknng_list_methods")) {
        return 0;
    }

    return 1;
}
