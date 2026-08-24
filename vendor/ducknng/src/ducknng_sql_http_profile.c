#include "ducknng_sql_shared.h"
#include "ducknng_util.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct ducknng_http_profiles_bind_data {
    ducknng_http_profile_info *profiles;
    idx_t profile_count;
} ducknng_http_profiles_bind_data;

typedef struct ducknng_http_profiles_init_data {
    ducknng_http_profiles_bind_data *bind;
    idx_t offset;
} ducknng_http_profiles_init_data;

static void
destroy_http_profiles_bind_data(void *ptr)
{
    ducknng_http_profiles_bind_data *data = (ducknng_http_profiles_bind_data *)ptr;

    if (!data) return;
    if (data->profiles) {
        ducknng_runtime_http_profiles_snapshot_free(data->profiles, (size_t)data->profile_count);
    }
    duckdb_free(data);
}

static void
destroy_http_profiles_init_data(void *ptr)
{
    if (ptr) duckdb_free(ptr);
}

/* Inside service-handled SQL the listing is subject-scoped: only unrestricted
 * profiles and profiles admitted to the current execution subject are visible,
 * and the allowlist column is withheld. Host/local SQL keeps the full redacted
 * listing. Returns the number of visible profiles. */
static idx_t
ducknng_http_profiles_scope_to_subject(ducknng_runtime *rt, duckdb_bind_info info,
    ducknng_http_profile_info *profiles, idx_t count)
{
    char *subject = NULL;
    const ducknng_execution_subject *subject_ctx;
    int in_request;
    idx_t kept = 0;
    idx_t i;

    if (!rt || !profiles || count == 0) return count;
    in_request = ducknng_runtime_current_thread_request_service_get(rt) != NULL;
    /* Connection binding first: a session connection carries its opener's
     * subject, which governs visibility even when another caller's fetch
     * request is driving execution on this thread. */
    {
        duckdb_client_context client_ctx = NULL;
        duckdb_table_function_get_client_context(info, &client_ctx);
        if (client_ctx) {
            subject = ducknng_runtime_execution_subject_for_connection_dup(rt,
                (uint64_t)duckdb_client_context_get_connection_id(client_ctx));
            duckdb_destroy_client_context(&client_ctx);
            if (subject) in_request = 1;
        }
    }
    if (!subject) {
        subject_ctx = ducknng_runtime_current_thread_execution_subject_get(rt);
        if (subject_ctx && subject_ctx->subject && subject_ctx->subject[0]) {
            subject = ducknng_strdup(subject_ctx->subject);
            in_request = 1;
        }
    }
    if (!in_request) {
        if (subject) duckdb_free(subject);
        return count;
    }
    for (i = 0; i < count; i++) {
        ducknng_http_profile_info *row = &profiles[i];
        int visible = !row->allow_subjects_json || !row->allow_subjects_json[0] ||
            (subject && ducknng_json_string_array_contains(row->allow_subjects_json,
                subject, NULL, NULL) == 1);
        if (!visible) {
            ducknng_http_profile_info_reset(row);
            continue;
        }
        if (row->allow_subjects_json) {
            duckdb_free(row->allow_subjects_json);
            row->allow_subjects_json = NULL;
        }
        if (kept != i) {
            profiles[kept] = *row;
            memset(row, 0, sizeof(*row));
        }
        kept++;
    }
    if (subject) duckdb_free(subject);
    return kept;
}

static void
ducknng_register_http_profile_scalar(duckdb_function_info info,
    duckdb_data_chunk input, duckdb_vector output)
{
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);

    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *profile_id = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *scheme = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *host = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        int has_port = !arg_is_null(duckdb_data_chunk_get_vector(input, 3), row);
        int32_t port = has_port ? arg_int32(duckdb_data_chunk_get_vector(input, 3), row, 0) : 0;
        char *path_prefix = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 4), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 5), row);
        bool tls_required = arg_bool(duckdb_data_chunk_get_vector(input, 6), row, false);
        char *auth_header_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 7), row);
        char *auth_header_value = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 8), row);
        char *allow_subjects_json = NULL;
        uint64_t expires_at_ms = 0;
        char *errmsg = NULL;

        if (ncols > 9 && !arg_is_null(duckdb_data_chunk_get_vector(input, 9), row)) {
            expires_at_ms = arg_u64(duckdb_data_chunk_get_vector(input, 9), row, 0);
        }
        if (ncols > 10) {
            allow_subjects_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 10), row);
        }
        if (!ctx || !ctx->rt) {
            duckdb_scalar_function_set_error(info, "ducknng: runtime not initialized");
            goto fail;
        }
        if (ducknng_runtime_upsert_http_profile(ctx->rt, profile_id, scheme, host,
                port, has_port, path_prefix, method, tls_required ? 1 : 0,
                auth_header_name, auth_header_value, expires_at_ms,
                allow_subjects_json, &errmsg) != 0) {
            duckdb_scalar_function_set_error(info,
                errmsg ? errmsg : "ducknng: failed to register HTTP profile");
            if (errmsg) duckdb_free(errmsg);
            goto fail;
        }
        out[row] = true;
        if (profile_id) duckdb_free(profile_id);
        if (scheme) duckdb_free(scheme);
        if (host) duckdb_free(host);
        if (path_prefix) duckdb_free(path_prefix);
        if (method) duckdb_free(method);
        if (auth_header_name) duckdb_free(auth_header_name);
        if (auth_header_value) duckdb_free(auth_header_value);
        if (allow_subjects_json) duckdb_free(allow_subjects_json);
        continue;
fail:
        if (profile_id) duckdb_free(profile_id);
        if (scheme) duckdb_free(scheme);
        if (host) duckdb_free(host);
        if (path_prefix) duckdb_free(path_prefix);
        if (method) duckdb_free(method);
        if (auth_header_name) duckdb_free(auth_header_name);
        if (auth_header_value) duckdb_free(auth_header_value);
        if (allow_subjects_json) duckdb_free(allow_subjects_json);
        return;
    }
}

static void
ducknng_drop_http_profile_scalar(duckdb_function_info info,
    duckdb_data_chunk input, duckdb_vector output)
{
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);

    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    for (row = 0; row < count; row++) {
        char *profile_id = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);

        if (!ctx || !ctx->rt || !profile_id || !profile_id[0]) {
            if (profile_id) duckdb_free(profile_id);
            duckdb_scalar_function_set_error(info, "ducknng: HTTP profile id is required");
            return;
        }
        out[row] = ducknng_runtime_drop_http_profile(ctx->rt, profile_id) ? true : false;
        duckdb_free(profile_id);
    }
}

static void
ducknng_list_http_profiles_bind(duckdb_bind_info info)
{
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_http_profiles_bind_data *bind;
    duckdb_logical_type type;
    char *errmsg = NULL;
    size_t count = 0;

    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_http_profiles_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    if (ducknng_runtime_http_profiles_snapshot(ctx->rt, &bind->profiles,
            &count, &errmsg) != 0) {
        destroy_http_profiles_bind_data(bind);
        duckdb_bind_set_error(info,
            errmsg ? errmsg : "ducknng: failed to list HTTP profiles");
        if (errmsg) duckdb_free(errmsg);
        return;
    }
    bind->profile_count = ducknng_http_profiles_scope_to_subject(ctx->rt, info,
        bind->profiles, (idx_t)count);

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "profile_id", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "host", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "has_port", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "path_prefix", type);
    duckdb_bind_add_result_column(info, "method", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "tls_required", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "auth_header_names_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "version", type);
    duckdb_bind_add_result_column(info, "created_ms", type);
    duckdb_bind_add_result_column(info, "updated_ms", type);
    duckdb_bind_add_result_column(info, "expires_at_ms", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "allow_subjects_json", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_profiles_bind_data);
    duckdb_bind_set_cardinality(info, bind->profile_count, true);
}

static void
ducknng_list_http_profiles_init(duckdb_init_info info)
{
    ducknng_http_profiles_bind_data *bind =
        (ducknng_http_profiles_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_http_profiles_init_data *init =
        (ducknng_http_profiles_init_data *)duckdb_malloc(sizeof(*init));

    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_http_profiles_init_data);
}

static void
ducknng_list_http_profiles_scan(duckdb_function_info info, duckdb_data_chunk output)
{
    ducknng_http_profiles_init_data *init =
        (ducknng_http_profiles_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_profiles_bind_data *bind = init ? init->bind : NULL;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    int32_t *ports;
    bool *has_ports;
    bool *tls_required;
    uint64_t *versions;
    uint64_t *created;
    uint64_t *updated;
    uint64_t *expires;
    (void)info;

    if (!init || !bind || init->offset >= bind->profile_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    remaining = bind->profile_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    ports = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    has_ports = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    tls_required = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    versions = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 9));
    created = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10));
    updated = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11));
    expires = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 12));
    for (i = 0; i < chunk_size; i++) {
        ducknng_http_profile_info *row = &bind->profiles[init->offset + i];
        if (row->profile_id) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 0), i, row->profile_id, (idx_t)strlen(row->profile_id));
        else set_null(duckdb_data_chunk_get_vector(output, 0), i);
        if (row->scheme) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), i, row->scheme, (idx_t)strlen(row->scheme));
        else set_null(duckdb_data_chunk_get_vector(output, 1), i);
        if (row->host) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 2), i, row->host, (idx_t)strlen(row->host));
        else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        if (row->has_port) ports[i] = (int32_t)row->port;
        else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        has_ports[i] = row->has_port ? true : false;
        if (row->path_prefix) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), i, row->path_prefix, (idx_t)strlen(row->path_prefix));
        else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->method) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), i, row->method, (idx_t)strlen(row->method));
        else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        tls_required[i] = row->tls_required ? true : false;
        if (row->auth_header_names_json) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 8), i, row->auth_header_names_json, (idx_t)strlen(row->auth_header_names_json));
        else set_null(duckdb_data_chunk_get_vector(output, 8), i);
        versions[i] = row->version;
        created[i] = row->created_ms;
        updated[i] = row->updated_ms;
        expires[i] = row->expires_at_ms;
        if (row->allow_subjects_json) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 13), i, row->allow_subjects_json, (idx_t)strlen(row->allow_subjects_json));
        else set_null(duckdb_data_chunk_get_vector(output, 13), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

int
ducknng_register_sql_http_profiles(duckdb_connection con, ducknng_sql_context *ctx)
{
    duckdb_type register_types[9] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR};
    duckdb_type register_expiry_types[10] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type register_subjects_types[11] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    duckdb_type profile_id_types[1] = {DUCKDB_TYPE_VARCHAR};

    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_profile", 9,
            ducknng_register_http_profile_scalar, ctx, register_types,
            DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_profile", 10,
            ducknng_register_http_profile_scalar, ctx, register_expiry_types,
            DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_profile", 11,
            ducknng_register_http_profile_scalar, ctx, register_subjects_types,
            DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_drop_http_profile", 1,
            ducknng_drop_http_profile_scalar, ctx, profile_id_types,
            DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_list_http_profiles", ctx, 0, NULL,
            ducknng_list_http_profiles_bind, ducknng_list_http_profiles_init,
            ducknng_list_http_profiles_scan)) return 0;
    return 1;
}
