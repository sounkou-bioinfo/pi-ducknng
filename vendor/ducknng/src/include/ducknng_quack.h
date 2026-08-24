#pragma once
#include "duckdb_extension.h"
#include "ducknng_quack_core.h"
#include <nng/nng.h>
#include <stddef.h>
#include <stdint.h>

/* Type-tree nodes are dependency-free and owned by ducknng_quack_core.h.
 * Top-level columns remain a DuckDB-sized flat array in this adapter schema. */
typedef struct ducknng_quack_schema {
    idx_t ncols;
    ducknng_quack_column_schema *cols;
} ducknng_quack_schema;

void ducknng_quack_schema_reset(ducknng_quack_schema *schema);

int ducknng_result_next_chunk_to_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg);
int ducknng_result_next_chunks_to_quack_payload(duckdb_result result,
    uint64_t max_chunks, int include_schema, uint8_t **out_bytes, size_t *out_len,
    int *has_chunk, char **errmsg);
/* Allocate one final NNG message with prefix_size bytes reserved before the
 * Quack payload and encode directly into its body. The caller owns *out_msg. */
int ducknng_result_next_chunks_to_quack_message(duckdb_result result,
    uint64_t max_chunks, int include_schema, size_t prefix_size,
    nng_msg **out_msg, size_t *out_payload_len, int *has_chunk, char **errmsg);
/* Materialized-result variant of ducknng_result_next_chunks_to_quack_payload:
 * encodes up to max_chunks batches starting at *inout_chunk_index (advancing it)
 * using duckdb_result_get_chunk, for results produced by duckdb_query. */
int ducknng_result_materialized_chunks_to_quack_payload(duckdb_result result,
    idx_t *inout_chunk_index, uint64_t max_chunks, int include_schema,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg);
int ducknng_result_empty_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg);
int ducknng_result_empty_quack_message(duckdb_result result,
    size_t prefix_size, nng_msg **out_msg, size_t *out_payload_len,
    char **errmsg);

/* Parse the self-describing type + name header of a quack batch into a schema
 * without a bind_info. Fills *out_schema (caller frees with
 * ducknng_quack_schema_reset). This is the shared parse used by both the table
 * function bind path and the server-side upload append path, so it must remain
 * fail-closed against arbitrary bytes. */
int ducknng_quack_payload_parse_schema(const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, char **errmsg);
int ducknng_quack_payload_bind_columns(duckdb_bind_info info,
    const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, idx_t *out_row_count, char **errmsg);
/* Decode a quack batch and append its rows to an open appender whose column
 * types must match the batch schema. Adds the appended row count to
 * *inout_rows. Fail-closed: any malformed byte, schema/appender column
 * mismatch, or append error returns -1 with *errmsg and appends nothing
 * further. The appender is neither flushed nor destroyed here. */
/* expected_names (when non-NULL) are the target table's column names in order;
 * the batch column names must match them exactly, since the appender appends
 * by ordinal and would otherwise misassign same-typed columns. */
int ducknng_quack_payload_append_to_appender(duckdb_appender appender,
    const uint8_t *payload, size_t payload_len,
    const char *const *expected_names, idx_t expected_name_count,
    uint64_t *inout_rows, char **errmsg);
int ducknng_quack_payload_read_row_count(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *out_row_count, char **errmsg);
int ducknng_quack_payload_scan_begin(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, size_t *inout_offset, uint64_t *out_remaining,
    char **errmsg);
int ducknng_quack_payload_scan_next(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len, const ducknng_quack_schema *schema,
    size_t *inout_offset, uint64_t *inout_remaining, char **errmsg);
int ducknng_quack_payload_scan(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *inout_offset, char **errmsg);
