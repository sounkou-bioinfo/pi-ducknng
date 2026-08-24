#include "ducknng_sql_api.h"
#include "ducknng_runtime.h"
#include "ducknng_session.h"
#include "ducknng_sql_shared.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

DUCKDB_EXTENSION_EXTERN

static void destroy_sql_context_extra(void *data) {
    if (data) duckdb_free(data);
}

static ducknng_sql_context *ducknng_dup_sql_context(const ducknng_sql_context *ctx) {
    ducknng_sql_context *copy;
    if (!ctx) return NULL;
    copy = (ducknng_sql_context *)duckdb_malloc(sizeof(*copy));
    if (!copy) return NULL;
    memcpy(copy, ctx, sizeof(*copy));
    return copy;
}

int ducknng_sql_arg_is_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity = duckdb_vector_get_validity(vec);
    return validity && !duckdb_validity_row_is_valid(validity, row);
}

char *ducknng_sql_arg_varchar_dup(duckdb_vector vec, idx_t row) {
    duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
    const char *src;
    uint32_t len;
    char *out;
    if (ducknng_sql_arg_is_null(vec, row)) return NULL;
    src = duckdb_string_t_data(&data[row]);
    len = duckdb_string_t_length(data[row]);
    out = (char *)duckdb_malloc((size_t)len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

uint8_t *ducknng_sql_arg_blob_dup(duckdb_vector vec, idx_t row, idx_t *out_len) {
    duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
    const char *src;
    uint32_t len;
    uint8_t *out;
    if (out_len) *out_len = 0;
    if (ducknng_sql_arg_is_null(vec, row)) return NULL;
    src = duckdb_string_t_data(&data[row]);
    len = duckdb_string_t_length(data[row]);
    out = (uint8_t *)duckdb_malloc((size_t)len);
    if (!out && len > 0) return NULL;
    if (len > 0) memcpy(out, src, len);
    if (out_len) *out_len = (idx_t)len;
    return out;
}

int32_t ducknng_sql_arg_int32(duckdb_vector vec, idx_t row, int32_t dflt) {
    int32_t *data = (int32_t *)duckdb_vector_get_data(vec);
    if (ducknng_sql_arg_is_null(vec, row)) return dflt;
    return data[row];
}

uint64_t ducknng_sql_arg_u64(duckdb_vector vec, idx_t row, uint64_t dflt) {
    uint64_t *data = (uint64_t *)duckdb_vector_get_data(vec);
    if (ducknng_sql_arg_is_null(vec, row)) return dflt;
    return data[row];
}

bool ducknng_sql_arg_bool(duckdb_vector vec, idx_t row, bool dflt) {
    bool *data = (bool *)duckdb_vector_get_data(vec);
    if (ducknng_sql_arg_is_null(vec, row)) return dflt;
    return data[row];
}

void ducknng_sql_set_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity;
    duckdb_vector_ensure_validity_writable(vec);
    validity = duckdb_vector_get_validity(vec);
    duckdb_validity_set_row_invalid(validity, row);
}

void ducknng_sql_assign_blob(duckdb_vector vec, idx_t row, const uint8_t *data, idx_t len) {
    /* BLOB is opaque binary; no UTF-8 validation needed. */
    duckdb_unsafe_vector_assign_string_element_len(vec, row, (const char *)data, len);
}

int ducknng_sql_bytes_look_text(const uint8_t *data, size_t len) {
    size_t i;
    duckdb_error_data err;
    if (len == 0) return 1;
    if (!data) return 0;
    /* Reject null bytes and disallowed control characters (below U+0020 except
     * horizontal tab, line feed, carriage return). This cannot be delegated to
     * duckdb_valid_utf8_check, which only validates UTF-8 encoding. */
    for (i = 0; i < len; i++) {
        uint8_t b = data[i];
        if (b == 0) return 0;
        if (b < 0x20 && b != '\t' && b != '\n' && b != '\r') return 0;
    }
    /* Use the unstable API to validate UTF-8 encoding. */
    err = duckdb_valid_utf8_check((const char *)data, (idx_t)len);
    if (duckdb_error_data_has_error(err)) {
        duckdb_destroy_error_data(&err);
        return 0;
    }
    duckdb_destroy_error_data(&err);
    return 1;
}

char *ducknng_sql_sockaddr_addr_dup(const nng_sockaddr *addr, char **out_ip, int32_t *out_port) {
    char ipbuf[INET6_ADDRSTRLEN];
    char addrbuf[INET6_ADDRSTRLEN + 32];
    const char *ip = NULL;
    int32_t port = 0;
    if (out_ip) *out_ip = NULL;
    if (out_port) *out_port = 0;
    if (!addr) return NULL;
    memset(ipbuf, 0, sizeof(ipbuf));
    memset(addrbuf, 0, sizeof(addrbuf));
    if (addr->s_family == NNG_AF_INET) {
        ip = inet_ntop(AF_INET, &addr->s_in.sa_addr, ipbuf, sizeof(ipbuf));
        port = (int32_t)ntohs(addr->s_in.sa_port);
        if (!ip) return NULL;
        snprintf(addrbuf, sizeof(addrbuf), "%s:%d", ipbuf, (int)port);
        if (out_ip) *out_ip = ducknng_strdup(ipbuf);
        if (out_port) *out_port = port;
        return ducknng_strdup(addrbuf);
    }
    if (addr->s_family == NNG_AF_INET6) {
        ip = inet_ntop(AF_INET6, addr->s_in6.sa_addr, ipbuf, sizeof(ipbuf));
        port = (int32_t)ntohs(addr->s_in6.sa_port);
        if (!ip) return NULL;
        snprintf(addrbuf, sizeof(addrbuf), "[%s]:%d", ipbuf, (int)port);
        if (out_ip) *out_ip = ducknng_strdup(ipbuf);
        if (out_port) *out_port = port;
        return ducknng_strdup(addrbuf);
    }
    if (addr->s_family == NNG_AF_IPC) return ducknng_strdup(addr->s_ipc.sa_path);
    if (addr->s_family == NNG_AF_INPROC) return ducknng_strdup(addr->s_inproc.sa_name);
    snprintf(addrbuf, sizeof(addrbuf), "nng-family:%u", (unsigned)addr->s_family);
    return ducknng_strdup(addrbuf);
}

static int ducknng_sql_register_scalar_logical_types_ex(duckdb_connection con, const char *name,
    idx_t nparams, duckdb_scalar_function_t fn, duckdb_scalar_function_bind_t bind_fn,
    ducknng_sql_context *ctx, duckdb_logical_type *param_types, duckdb_logical_type return_type,
    int is_volatile) {
    duckdb_scalar_function f = duckdb_create_scalar_function();
    idx_t i;
    if (!f) return 0;
    duckdb_scalar_function_set_name(f, name);
    for (i = 0; i < nparams; i++) duckdb_scalar_function_add_parameter(f, param_types[i]);
    duckdb_scalar_function_set_return_type(f, return_type);
    duckdb_scalar_function_set_function(f, fn);
    if (bind_fn) duckdb_scalar_function_set_bind(f, bind_fn);
    duckdb_scalar_function_set_special_handling(f);
    if (is_volatile) duckdb_scalar_function_set_volatile(f);
    if (!ducknng_set_scalar_sql_context(f, ctx)) {
        duckdb_destroy_scalar_function(&f);
        return 0;
    }
    if (duckdb_register_scalar_function(con, f) == DuckDBError) {
        duckdb_destroy_scalar_function(&f);
        return 0;
    }
    duckdb_destroy_scalar_function(&f);
    return 1;
}

static int ducknng_sql_register_scalar_ex(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id, int is_volatile) {
    duckdb_logical_type *logical_param_types = NULL;
    duckdb_logical_type ret_type;
    idx_t i;
    int ok;
    if (nparams > 0) {
        logical_param_types = (duckdb_logical_type *)duckdb_malloc(sizeof(*logical_param_types) * nparams);
        if (!logical_param_types) return 0;
    }
    for (i = 0; i < nparams; i++) {
        logical_param_types[i] = duckdb_create_logical_type(param_types[i]);
    }
    ret_type = duckdb_create_logical_type(return_type_id);
    ok = ducknng_sql_register_scalar_logical_types_ex(con, name, nparams, fn, NULL, ctx,
        logical_param_types, ret_type, is_volatile);
    for (i = 0; i < nparams; i++) duckdb_destroy_logical_type(&logical_param_types[i]);
    if (logical_param_types) duckdb_free(logical_param_types);
    duckdb_destroy_logical_type(&ret_type);
    return ok;
}

int ducknng_sql_register_scalar(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id) {
    return ducknng_sql_register_scalar_ex(con, name, nparams, fn, ctx, param_types,
        return_type_id, 0);
}

int ducknng_sql_register_volatile_scalar(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id) {
    return ducknng_sql_register_scalar_ex(con, name, nparams, fn, ctx, param_types,
        return_type_id, 1);
}

int ducknng_sql_register_scalar_logical_types(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_logical_type *param_types,
    duckdb_logical_type return_type) {
    return ducknng_sql_register_scalar_logical_types_ex(con, name, nparams, fn, NULL, ctx,
        param_types, return_type, 0);
}

int ducknng_sql_register_volatile_scalar_logical_types(duckdb_connection con, const char *name,
    idx_t nparams, duckdb_scalar_function_t fn, ducknng_sql_context *ctx,
    duckdb_logical_type *param_types, duckdb_logical_type return_type) {
    return ducknng_sql_register_scalar_logical_types_ex(con, name, nparams, fn, NULL, ctx,
        param_types, return_type, 1);
}

int ducknng_sql_register_volatile_scalar_logical_types_with_bind(duckdb_connection con,
    const char *name, idx_t nparams, duckdb_scalar_function_t fn,
    duckdb_scalar_function_bind_t bind_fn, ducknng_sql_context *ctx,
    duckdb_logical_type *param_types, duckdb_logical_type return_type) {
    return ducknng_sql_register_scalar_logical_types_ex(con, name, nparams, fn, bind_fn, ctx,
        param_types, return_type, 1);
}

static void ducknng_sql_connection_bind_data_destroy(void *data) {
    if (data) duckdb_free(data);
}

static void *ducknng_sql_connection_bind_data_copy(void *data) {
    ducknng_sql_connection_bind_data *src = (ducknng_sql_connection_bind_data *)data;
    ducknng_sql_connection_bind_data *dst;
    if (!src) return NULL;
    dst = (ducknng_sql_connection_bind_data *)duckdb_malloc(sizeof(*dst));
    if (!dst) return NULL;
    *dst = *src;
    return dst;
}

void ducknng_sql_connection_bind_cb(duckdb_bind_info info) {
    ducknng_sql_connection_bind_data *bd;
    duckdb_client_context client_ctx = NULL;
    bd = (ducknng_sql_connection_bind_data *)duckdb_malloc(sizeof(*bd));
    if (!bd) {
        duckdb_scalar_function_bind_set_error(info, "ducknng: out of memory in bind");
        return;
    }
    memset(bd, 0, sizeof(*bd));
    duckdb_scalar_function_get_client_context(info, &client_ctx);
    if (client_ctx) {
        bd->connection_id = (uint64_t)duckdb_client_context_get_connection_id(client_ctx);
        bd->has_connection_id = 1;
        duckdb_destroy_client_context(&client_ctx);
    }
    duckdb_scalar_function_set_bind_data(info, bd, ducknng_sql_connection_bind_data_destroy);
    duckdb_scalar_function_set_bind_data_copy(info, ducknng_sql_connection_bind_data_copy);
}

int ducknng_sql_scalar_connection_id(duckdb_function_info info, uint64_t *out_connection_id) {
    ducknng_sql_connection_bind_data *bd =
        (ducknng_sql_connection_bind_data *)duckdb_scalar_function_get_bind_data(info);
    if (out_connection_id) *out_connection_id = 0;
    if (!bd || !bd->has_connection_id) return 0;
    if (out_connection_id) *out_connection_id = bd->connection_id;
    return 1;
}

int ducknng_sql_register_volatile_scalar_with_bind(duckdb_connection con, const char *name,
    idx_t nparams, duckdb_scalar_function_t fn, duckdb_scalar_function_bind_t bind_fn,
    ducknng_sql_context *ctx, duckdb_type *param_types, duckdb_type return_type_id) {
    duckdb_logical_type *logical_param_types = NULL;
    duckdb_logical_type ret_type;
    idx_t i;
    int ok;
    if (nparams > 0) {
        logical_param_types = (duckdb_logical_type *)duckdb_malloc(sizeof(*logical_param_types) * nparams);
        if (!logical_param_types) return 0;
    }
    for (i = 0; i < nparams; i++) {
        logical_param_types[i] = duckdb_create_logical_type(param_types[i]);
    }
    ret_type = duckdb_create_logical_type(return_type_id);
    ok = ducknng_sql_register_scalar_logical_types_ex(con, name, nparams, fn, bind_fn, ctx,
        logical_param_types, ret_type, 1);
    for (i = 0; i < nparams; i++) duckdb_destroy_logical_type(&logical_param_types[i]);
    if (logical_param_types) duckdb_free(logical_param_types);
    duckdb_destroy_logical_type(&ret_type);
    return ok;
}

int ducknng_sql_register_table(duckdb_connection con, const char *name, ducknng_sql_context *ctx,
    idx_t nparams, duckdb_type *param_types, duckdb_table_function_bind_t bind_fn,
    duckdb_table_function_init_t init_fn, duckdb_table_function_t scan_fn) {
    duckdb_table_function tf;
    idx_t i;
    if (!con || !name || !bind_fn || !scan_fn) return 0;
    tf = duckdb_create_table_function();
    if (!tf) return 0;
    duckdb_table_function_set_name(tf, name);
    for (i = 0; i < nparams; i++) {
        duckdb_logical_type t = duckdb_create_logical_type(param_types[i]);
        duckdb_table_function_add_parameter(tf, t);
        duckdb_destroy_logical_type(&t);
    }
    if (ctx && !ducknng_set_table_sql_context(tf, ctx)) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_table_function_set_bind(tf, bind_fn);
    if (init_fn) duckdb_table_function_set_init(tf, init_fn);
    duckdb_table_function_set_function(tf, scan_fn);
    if (duckdb_register_table_function(con, tf) == DuckDBError) {
        duckdb_destroy_table_function(&tf);
        return 0;
    }
    duckdb_destroy_table_function(&tf);
    return 1;
}

int ducknng_set_table_sql_context(duckdb_table_function tf, const ducknng_sql_context *ctx) {
    ducknng_sql_context *copy = ducknng_dup_sql_context(ctx);
    if (!copy) return 0;
    duckdb_table_function_set_extra_info(tf, copy, destroy_sql_context_extra);
    return 1;
}

int ducknng_set_scalar_sql_context(duckdb_scalar_function fn, const ducknng_sql_context *ctx) {
    ducknng_sql_context *copy = ducknng_dup_sql_context(ctx);
    if (!copy) return 0;
    duckdb_scalar_function_set_extra_info(fn, copy, destroy_sql_context_extra);
    return 1;
}

int ducknng_sql_inside_authorizer(ducknng_sql_context *ctx) {
    return ctx && ctx->rt && ducknng_runtime_current_thread_authorizer_context_get(ctx->rt) != NULL;
}

int ducknng_reject_table_inside_authorizer(duckdb_bind_info info, ducknng_sql_context *ctx) {
    if (!ducknng_sql_inside_authorizer(ctx)) return 0;
    duckdb_bind_set_error(info, "ducknng: ducknng client and lifecycle functions cannot run inside a SQL authorizer callback");
    return 1;
}

int ducknng_reject_scalar_inside_authorizer(duckdb_function_info info, ducknng_sql_context *ctx) {
    if (!ducknng_sql_inside_authorizer(ctx)) return 0;
    duckdb_scalar_function_set_error(info, "ducknng: ducknng client and lifecycle functions cannot run inside a SQL authorizer callback");
    return 1;
}

/* ---------------------------------------------------------------------------
 * Config option registration — called once at extension load time.
 * Options are registered with SESSION scope so callers can override per
 * connection with SET ducknng.option = value while the extension-wide
 * default applies when no per-session override is set.
 * --------------------------------------------------------------------------- */
static int ducknng_register_one_config_option(duckdb_connection con,
    const char *name, const char *description, uint64_t default_ubigint) {
    duckdb_config_option opt;
    duckdb_logical_type type;
    duckdb_value val;
    int ok;
    opt = duckdb_create_config_option();
    if (!opt) return 0;
    duckdb_config_option_set_name(opt, name);
    duckdb_config_option_set_description(opt, description);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_config_option_set_type(opt, type);
    duckdb_destroy_logical_type(&type);
    val = duckdb_create_uint64(default_ubigint);
    duckdb_config_option_set_default_value(opt, val);
    duckdb_destroy_value(&val);
    duckdb_config_option_set_default_scope(opt, DUCKDB_CONFIG_OPTION_SCOPE_SESSION);
    ok = (duckdb_register_config_option(con, opt) == DuckDBSuccess);
    duckdb_destroy_config_option(&opt);
    return ok;
}

static int ducknng_register_config_options(duckdb_connection con) {
    if (!ducknng_register_one_config_option(con, "ducknng.csv_max_columns",
            "Maximum number of columns allowed when parsing CSV or TSV body payloads "
            "via ducknng_parse_body. Increase for wide CSV inputs.", 1024))
        return 0;
    if (!ducknng_register_one_config_option(con, "ducknng.fetch_batch_chunks",
            "Default number of DuckDB data chunks requested per query-session fetch. "
            "Applies to all row serializers and transports unless a client sends an explicit batch_rows hint.",
            DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS))
        return 0;
    return 1;
}

/* Helper: read a UBIGINT config option from a client context.
 * Returns fallback when the context is NULL or the option is absent. */
uint64_t ducknng_sql_get_config_ubigint(duckdb_client_context ctx,
    const char *name, uint64_t fallback) {
    duckdb_config_option_scope scope;
    duckdb_value val;
    uint64_t result;
    if (!ctx) return fallback;
    val = duckdb_client_context_get_config_option(ctx, name, &scope);
    if (!val) return fallback;
    result = duckdb_get_uint64(val);
    duckdb_destroy_value(&val);
    return result;
}

int ducknng_register_sql_api(duckdb_connection connection, ducknng_runtime *rt) {
    ducknng_sql_context ctx;
    ctx.rt = rt;
    ctx.is_init_connection = rt && connection == ducknng_runtime_execution_connection(rt);
    if (!ducknng_register_config_options(connection)) return 0;
    if (!ducknng_register_sql_service(connection, &ctx)) return 0;
    if (!ducknng_register_sql_http(connection, &ctx)) return 0;
    if (!ducknng_register_sql_http_profiles(connection, &ctx)) return 0;
    if (!ducknng_register_sql_auth(connection, &ctx)) return 0;
    if (!ducknng_register_sql_monitor(connection, &ctx)) return 0;
    if (!ducknng_register_sql_tls(connection, &ctx)) return 0;
    if (!ducknng_register_sql_socket(connection, &ctx)) return 0;
    if (!ducknng_register_sql_rpc(connection, &ctx)) return 0;
    if (!ducknng_register_sql_aio(connection, &ctx)) return 0;
    if (!ducknng_register_sql_registry(connection, &ctx)) return 0;
    if (!ducknng_register_sql_session(connection, &ctx)) return 0;
    if (!ducknng_register_sql_body(connection, &ctx)) return 0;
    return 1;
}
