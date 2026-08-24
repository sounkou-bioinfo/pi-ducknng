#pragma once
#include <stddef.h>
#include <stdint.h>
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"

typedef struct ducknng_exec_request {
    char *sql;
    int want_result;
} ducknng_exec_request;

typedef struct ducknng_query_open_request {
    char *sql;
    uint64_t batch_rows;
    uint64_t batch_bytes;
} ducknng_query_open_request;

int ducknng_decode_exec_request_payload(const uint8_t *payload, size_t payload_len,
    ducknng_exec_request *out, char **errmsg);
int ducknng_decode_exec_metadata_payload(const uint8_t *payload, size_t payload_len,
    uint64_t *rows_changed, uint32_t *statement_type, uint32_t *result_type, char **errmsg);
int ducknng_decode_query_open_payload(const uint8_t *payload, size_t payload_len,
    ducknng_query_open_request *out, char **errmsg);
int ducknng_decode_ipc_table_payload(const uint8_t *payload, size_t payload_len,
    struct ArrowSchema *schema, struct ArrowArray *array, char **errmsg);
void ducknng_exec_request_destroy(ducknng_exec_request *req);
void ducknng_query_open_request_destroy(ducknng_query_open_request *req);
