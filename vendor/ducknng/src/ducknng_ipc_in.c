#include "ducknng_ipc_in.h"
#include "ducknng_util.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static char *ducknng_copy_string_view(struct ArrowStringView view) {
    char *out;
    if (!view.data || view.size_bytes < 0) return NULL;
    out = (char *)duckdb_malloc((size_t)view.size_bytes + 1);
    if (!out) return NULL;
    memcpy(out, view.data, (size_t)view.size_bytes);
    out[view.size_bytes] = '\0';
    return out;
}

int ducknng_decode_exec_request_payload(const uint8_t *payload, size_t payload_len,
    ducknng_exec_request *out, char **errmsg) {
    struct ArrowBuffer input_buf;
    struct ArrowIpcInputStream input_stream;
    struct ArrowArrayStream stream;
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayView view;
    struct ArrowError error;
    struct ArrowStringView sql_view;
    int rc = -1;
    memset(&input_buf, 0, sizeof(input_buf));
    memset(&input_stream, 0, sizeof(input_stream));
    memset(&stream, 0, sizeof(stream));
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&view, 0, sizeof(view));
    memset(&error, 0, sizeof(error));
    if (out) memset(out, 0, sizeof(*out));

    if (!payload || payload_len == 0 || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing exec payload");
        return -1;
    }

    ArrowBufferInit(&input_buf);
    if (ArrowBufferAppend(&input_buf, payload, (int64_t)payload_len) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy Arrow IPC payload");
        goto cleanup;
    }
    if (ArrowIpcInputStreamInitBuffer(&input_stream, &input_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow IPC input stream");
        input_buf.data = NULL;
        goto cleanup;
    }
    memset(&input_buf, 0, sizeof(input_buf));
    if (ArrowIpcArrayStreamReaderInit(&stream, &input_stream, NULL) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow IPC reader");
        memset(&input_stream, 0, sizeof(input_stream));
        goto cleanup;
    }
    memset(&input_stream, 0, sizeof(input_stream));
    if (ArrowArrayStreamGetSchema(&stream, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (schema.n_children != 2 || !schema.children ||
        !schema.children[0] || !schema.children[1] ||
        !schema.children[0]->name || strcmp(schema.children[0]->name, "sql") != 0 ||
        !schema.children[1]->name || strcmp(schema.children[1]->name, "want_result") != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: exec payload schema must be {sql, want_result}");
        goto cleanup;
    }
    if (ArrowArrayStreamGetNext(&stream, &array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (!array.release || array.length != 1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: exec payload must contain exactly one row");
        goto cleanup;
    }
    if (ArrowArrayViewInitFromSchema(&view, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayViewSetArray(&view, &array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayViewIsNull(view.children[0], 0)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: exec payload sql must not be NULL");
        goto cleanup;
    }
    sql_view = ArrowArrayViewGetStringUnsafe(view.children[0], 0);
    out->sql = ducknng_copy_string_view(sql_view);
    if (!out->sql) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy sql from exec payload");
        goto cleanup;
    }
    out->want_result = (!ArrowArrayViewIsNull(view.children[1], 0) &&
        ArrowArrayViewGetIntUnsafe(view.children[1], 0) != 0) ? 1 : 0;
    rc = 0;

cleanup:
    if (rc != 0 && out) ducknng_exec_request_destroy(out);
    ArrowArrayViewReset(&view);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (input_stream.release) input_stream.release(&input_stream);
    if (input_buf.data) ArrowBufferReset(&input_buf);
    return rc;
}

int ducknng_decode_exec_metadata_payload(const uint8_t *payload, size_t payload_len,
    uint64_t *rows_changed, uint32_t *statement_type, uint32_t *result_type, char **errmsg) {
    struct ArrowBuffer input_buf;
    struct ArrowIpcInputStream input_stream;
    struct ArrowArrayStream stream;
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayView view;
    struct ArrowError error;
    int rc = -1;
    memset(&input_buf, 0, sizeof(input_buf));
    memset(&input_stream, 0, sizeof(input_stream));
    memset(&stream, 0, sizeof(stream));
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&view, 0, sizeof(view));
    memset(&error, 0, sizeof(error));
    if (!payload || payload_len == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing exec metadata payload");
        return -1;
    }
    ArrowBufferInit(&input_buf);
    if (ArrowBufferAppend(&input_buf, payload, (int64_t)payload_len) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy exec metadata payload");
        goto cleanup;
    }
    if (ArrowIpcInputStreamInitBuffer(&input_stream, &input_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize exec metadata IPC reader");
        input_buf.data = NULL;
        goto cleanup;
    }
    memset(&input_buf, 0, sizeof(input_buf));
    if (ArrowIpcArrayStreamReaderInit(&stream, &input_stream, NULL) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize exec metadata Arrow reader");
        memset(&input_stream, 0, sizeof(input_stream));
        goto cleanup;
    }
    memset(&input_stream, 0, sizeof(input_stream));
    if (ArrowArrayStreamGetSchema(&stream, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (schema.n_children != 3 || !schema.children || !schema.children[0] || !schema.children[1] || !schema.children[2]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid exec metadata schema");
        goto cleanup;
    }
    if (ArrowArrayStreamGetNext(&stream, &array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (!array.release || array.length != 1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: exec metadata payload must contain exactly one row");
        goto cleanup;
    }
    if (ArrowArrayViewInitFromSchema(&view, &schema, &error) != NANOARROW_OK ||
        ArrowArrayViewSetArray(&view, &array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (rows_changed) *rows_changed = ArrowArrayViewGetUIntUnsafe(view.children[0], 0);
    if (statement_type) *statement_type = (uint32_t)ArrowArrayViewGetIntUnsafe(view.children[1], 0);
    if (result_type) *result_type = (uint32_t)ArrowArrayViewGetIntUnsafe(view.children[2], 0);
    rc = 0;
cleanup:
    ArrowArrayViewReset(&view);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (input_stream.release) input_stream.release(&input_stream);
    if (input_buf.data) ArrowBufferReset(&input_buf);
    return rc;
}

int ducknng_decode_ipc_table_payload(const uint8_t *payload, size_t payload_len,
    struct ArrowSchema *schema, struct ArrowArray *array, char **errmsg) {
    struct ArrowBuffer input_buf;
    struct ArrowIpcInputStream input_stream;
    struct ArrowArrayStream stream;
    struct ArrowError error;
    int rc = -1;
    memset(&input_buf, 0, sizeof(input_buf));
    memset(&input_stream, 0, sizeof(input_stream));
    memset(&stream, 0, sizeof(stream));
    memset(&error, 0, sizeof(error));
    if (!payload || payload_len == 0 || !schema || !array) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing Arrow table payload");
        return -1;
    }
    memset(schema, 0, sizeof(*schema));
    memset(array, 0, sizeof(*array));
    ArrowBufferInit(&input_buf);
    if (ArrowBufferAppend(&input_buf, payload, (int64_t)payload_len) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy Arrow table payload");
        goto cleanup;
    }
    if (ArrowIpcInputStreamInitBuffer(&input_stream, &input_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow IPC input stream");
        input_buf.data = NULL;
        goto cleanup;
    }
    memset(&input_buf, 0, sizeof(input_buf));
    if (ArrowIpcArrayStreamReaderInit(&stream, &input_stream, NULL) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow IPC reader");
        memset(&input_stream, 0, sizeof(input_stream));
        goto cleanup;
    }
    memset(&input_stream, 0, sizeof(input_stream));
    if (ArrowArrayStreamGetSchema(&stream, schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStreamGetNext(&stream, array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (!array->release) {
        /* EOS immediately after schema: zero-row result.  Build an empty
         * array that matches the schema so callers can inspect columns. */
        if (ArrowArrayInitFromSchema(array, schema, &error) != NANOARROW_OK ||
            ArrowArrayFinishBuildingDefault(array, &error) != NANOARROW_OK) {
            if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message
                                      : "ducknng: failed to build empty Arrow array for zero-row result");
            goto cleanup;
        }
    }
    rc = 0;
cleanup:
    if (rc != 0) {
        if (array->release) ArrowArrayRelease(array);
        if (schema->release) ArrowSchemaRelease(schema);
    }
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (input_stream.release) input_stream.release(&input_stream);
    if (input_buf.data) ArrowBufferReset(&input_buf);
    return rc;
}

int ducknng_decode_query_open_payload(const uint8_t *payload, size_t payload_len,
    ducknng_query_open_request *out, char **errmsg) {
    struct ArrowBuffer input_buf;
    struct ArrowIpcInputStream input_stream;
    struct ArrowArrayStream stream;
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayView view;
    struct ArrowError error;
    struct ArrowStringView sql_view;
    int rc = -1;
    memset(&input_buf, 0, sizeof(input_buf));
    memset(&input_stream, 0, sizeof(input_stream));
    memset(&stream, 0, sizeof(stream));
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&view, 0, sizeof(view));
    memset(&error, 0, sizeof(error));
    if (out) memset(out, 0, sizeof(*out));
    if (!payload || payload_len == 0 || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing query_open payload");
        return -1;
    }
    ArrowBufferInit(&input_buf);
    if (ArrowBufferAppend(&input_buf, payload, (int64_t)payload_len) != NANOARROW_OK ||
        ArrowIpcInputStreamInitBuffer(&input_stream, &input_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize query_open Arrow payload reader");
        goto cleanup;
    }
    input_buf.data = NULL;
    if (ArrowIpcArrayStreamReaderInit(&stream, &input_stream, NULL) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize query_open Arrow stream reader");
        goto cleanup;
    }
    input_stream.release = NULL;
    if (ArrowArrayStreamGetSchema(&stream, &schema, &error) != NANOARROW_OK ||
        ArrowArrayStreamGetNext(&stream, &array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message : "ducknng: failed to decode query_open payload");
        goto cleanup;
    }
    if (!array.release || array.length != 1 || schema.n_children < 1 || !schema.children || !schema.children[0] ||
        !schema.children[0]->name || strcmp(schema.children[0]->name, "sql") != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: query_open payload must contain exactly one row with sql as the first field");
        goto cleanup;
    }
    if (ArrowArrayViewInitFromSchema(&view, &schema, &error) != NANOARROW_OK ||
        ArrowArrayViewSetArray(&view, &array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message : "ducknng: failed to view query_open payload");
        goto cleanup;
    }
    if (ArrowArrayViewIsNull(view.children[0], 0)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: query_open payload sql must not be NULL");
        goto cleanup;
    }
    sql_view = ArrowArrayViewGetStringUnsafe(view.children[0], 0);
    out->sql = ducknng_copy_string_view(sql_view);
    if (!out->sql) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy sql from query_open payload");
        goto cleanup;
    }
    if (schema.n_children > 1 && schema.children[1] && schema.children[1]->name && strcmp(schema.children[1]->name, "batch_rows") == 0 && !ArrowArrayViewIsNull(view.children[1], 0)) {
        out->batch_rows = ArrowArrayViewGetUIntUnsafe(view.children[1], 0);
    }
    if (schema.n_children > 2 && schema.children[2] && schema.children[2]->name && strcmp(schema.children[2]->name, "batch_bytes") == 0 && !ArrowArrayViewIsNull(view.children[2], 0)) {
        out->batch_bytes = ArrowArrayViewGetUIntUnsafe(view.children[2], 0);
    }
    rc = 0;
cleanup:
    if (rc != 0 && out) ducknng_query_open_request_destroy(out);
    ArrowArrayViewReset(&view);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (input_stream.release) input_stream.release(&input_stream);
    if (input_buf.data) ArrowBufferReset(&input_buf);
    return rc;
}

void ducknng_exec_request_destroy(ducknng_exec_request *req) {
    if (!req) return;
    if (req->sql) duckdb_free(req->sql);
    req->sql = NULL;
    req->want_result = 0;
}

void ducknng_query_open_request_destroy(ducknng_query_open_request *req) {
    if (!req) return;
    if (req->sql) duckdb_free(req->sql);
    req->sql = NULL;
    req->batch_rows = 0;
    req->batch_bytes = 0;
}
