#pragma once
#include <stddef.h>
#include <stdint.h>
#include "duckdb_extension.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"

typedef struct ducknng_exec_request {
    char *sql;
    int want_result;
    duckdb_value *parameters;
    idx_t parameter_count;
} ducknng_exec_request;

typedef struct ducknng_query_open_request {
    char *sql;
    uint64_t batch_rows;
    uint64_t batch_bytes;
    char *correlation_id;
    char *serialization_mode;
    duckdb_value *parameters;
    idx_t parameter_count;
} ducknng_query_open_request;

typedef struct ducknng_arrow_batches {
    struct ArrowSchema schema;
    struct ArrowArray *arrays;
    int64_t array_count;
    int64_t row_count;
} ducknng_arrow_batches;

int ducknng_decode_exec_request_payload(const uint8_t *payload, size_t payload_len,
    ducknng_exec_request *out, char **errmsg);
int ducknng_decode_exec_metadata_payload(const uint8_t *payload, size_t payload_len,
    uint64_t *rows_changed, uint32_t *statement_type, uint32_t *result_type, char **errmsg);
int ducknng_decode_query_open_payload(const uint8_t *payload, size_t payload_len,
    ducknng_query_open_request *out, char **errmsg);
int ducknng_decode_ipc_table_payload(const uint8_t *payload, size_t payload_len,
    struct ArrowSchema *schema, struct ArrowArray *array, char **errmsg);
int ducknng_decode_ipc_batches_payload(const uint8_t *payload, size_t payload_len,
    ducknng_arrow_batches *out, char **errmsg);
void ducknng_arrow_batches_reset(ducknng_arrow_batches *batches);
void ducknng_exec_request_destroy(ducknng_exec_request *req);
void ducknng_query_open_request_destroy(ducknng_query_open_request *req);
