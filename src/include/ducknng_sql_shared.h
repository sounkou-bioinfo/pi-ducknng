#pragma once
#include "duckdb_extension.h"
#include "ducknng_runtime.h"

typedef struct {
    ducknng_runtime *rt;
    int is_init_connection;
} ducknng_sql_context;

int ducknng_sql_arg_is_null(duckdb_vector vec, idx_t row);
char *ducknng_sql_arg_varchar_dup(duckdb_vector vec, idx_t row);
uint8_t *ducknng_sql_arg_blob_dup(duckdb_vector vec, idx_t row, idx_t *out_len);
int32_t ducknng_sql_arg_int32(duckdb_vector vec, idx_t row, int32_t dflt);
uint64_t ducknng_sql_arg_u64(duckdb_vector vec, idx_t row, uint64_t dflt);
bool ducknng_sql_arg_bool(duckdb_vector vec, idx_t row, bool dflt);
void ducknng_sql_set_null(duckdb_vector vec, idx_t row);
void ducknng_sql_assign_blob(duckdb_vector vec, idx_t row, const uint8_t *data, idx_t len);
int ducknng_sql_bytes_look_text(const uint8_t *data, size_t len);
char *ducknng_sql_sockaddr_addr_dup(const nng_sockaddr *addr, char **out_ip, int32_t *out_port);
int ducknng_sql_register_scalar(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id);
int ducknng_sql_register_volatile_scalar(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_type *param_types,
    duckdb_type return_type_id);
int ducknng_sql_register_scalar_logical_types(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_logical_type *param_types,
    duckdb_logical_type return_type);
int ducknng_sql_register_volatile_scalar_logical_types(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, ducknng_sql_context *ctx, duckdb_logical_type *param_types,
    duckdb_logical_type return_type);
int ducknng_sql_register_volatile_scalar_with_bind(duckdb_connection con, const char *name, idx_t nparams,
    duckdb_scalar_function_t fn, duckdb_scalar_function_bind_t bind_fn, ducknng_sql_context *ctx,
    duckdb_type *param_types, duckdb_type return_type_id);
int ducknng_sql_register_table(duckdb_connection con, const char *name, ducknng_sql_context *ctx,
    idx_t nparams, duckdb_type *param_types, duckdb_table_function_bind_t bind_fn,
    duckdb_table_function_init_t init_fn, duckdb_table_function_t scan_fn);

/* Read a UBIGINT config option registered by ducknng from a client context.
 * Returns fallback if ctx is NULL or the option is absent. */
uint64_t ducknng_sql_get_config_ubigint(duckdb_client_context ctx,
    const char *name, uint64_t fallback);

#define DUCKNNG_REGISTER_SCALAR(...) ducknng_sql_register_scalar(__VA_ARGS__)
#define DUCKNNG_REGISTER_VOLATILE_SCALAR(...) ducknng_sql_register_volatile_scalar(__VA_ARGS__)
#define DUCKNNG_REGISTER_SCALAR_LOGICAL_TYPES(...) ducknng_sql_register_scalar_logical_types(__VA_ARGS__)
#define DUCKNNG_REGISTER_VOLATILE_SCALAR_LOGICAL_TYPES(...) ducknng_sql_register_volatile_scalar_logical_types(__VA_ARGS__)
#define DUCKNNG_REGISTER_VOLATILE_SCALAR_WITH_BIND(...) ducknng_sql_register_volatile_scalar_with_bind(__VA_ARGS__)
#define DUCKNNG_REGISTER_TABLE(...) ducknng_sql_register_table(__VA_ARGS__)

#define arg_is_null ducknng_sql_arg_is_null
#define arg_varchar_dup ducknng_sql_arg_varchar_dup
#define arg_blob_dup ducknng_sql_arg_blob_dup
#define arg_int32 ducknng_sql_arg_int32
#define arg_u64 ducknng_sql_arg_u64
#define arg_bool ducknng_sql_arg_bool
#define set_null ducknng_sql_set_null
#define assign_blob ducknng_sql_assign_blob

int ducknng_set_table_sql_context(duckdb_table_function tf, const ducknng_sql_context *ctx);
int ducknng_set_scalar_sql_context(duckdb_scalar_function fn, const ducknng_sql_context *ctx);
int ducknng_sql_inside_authorizer(ducknng_sql_context *ctx);
int ducknng_reject_table_inside_authorizer(duckdb_bind_info info, ducknng_sql_context *ctx);
int ducknng_reject_scalar_inside_authorizer(duckdb_function_info info, ducknng_sql_context *ctx);

int ducknng_register_sql_auth(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_monitor(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_service(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_http(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_http_profiles(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_tls(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_socket(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_aio(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_registry(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_session(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_body(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_rpc(duckdb_connection con, ducknng_sql_context *ctx);
