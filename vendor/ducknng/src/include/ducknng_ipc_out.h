#pragma once
#include "duckdb_extension.h"
#include <stddef.h>
#include <stdint.h>

int ducknng_exec_request_to_ipc(const char *sql, int want_result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);
int ducknng_exec_request_with_params_to_ipc(duckdb_connection con,
    const char *sql, int want_result, duckdb_value params,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);
int ducknng_query_open_request_to_ipc(const char *sql, uint64_t batch_rows,
    uint64_t batch_bytes, uint8_t **out_bytes, size_t *out_len, char **errmsg);
int ducknng_query_open_request_to_ipc_ex(const char *sql, uint64_t batch_rows,
    uint64_t batch_bytes, const char *correlation_id, const char *serialization_mode,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);
int ducknng_query_open_request_with_params_to_ipc(duckdb_connection con,
    const char *sql, uint64_t batch_rows, uint64_t batch_bytes,
    const char *correlation_id, const char *serialization_mode,
    duckdb_value params, uint8_t **out_bytes, size_t *out_len, char **errmsg);
char *ducknng_session_request_json(uint64_t session_id, const char *session_token,
    uint64_t batch_rows, uint64_t batch_bytes);
char *ducknng_session_request_json_ex(uint64_t session_id, const char *session_token,
    uint64_t batch_rows, uint64_t batch_bytes, const char *correlation_id);
int ducknng_result_to_ipc_stream(duckdb_prepared_statement stmt, duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);
int ducknng_query_to_ipc_stream(duckdb_connection con, const char *sql,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);
int ducknng_result_next_chunk_to_ipc(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg);
int ducknng_result_next_chunks_to_ipc(duckdb_result result, uint64_t max_chunks,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg);
int ducknng_exec_metadata_to_ipc(uint64_t rows_changed,
    uint32_t statement_type, uint32_t result_type, uint8_t **out_bytes,
    size_t *out_len, char **errmsg);
int ducknng_prepared_schema_to_ipc(duckdb_connection con, duckdb_prepared_statement stmt,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);
