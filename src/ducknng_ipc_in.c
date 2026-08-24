#include "ducknng_ipc_in.h"
#include "ducknng_sql_arrow.h"
#include "ducknng_util.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

#define DUCKNNG_MAX_QUERY_PARAMETERS 65535

static char *ducknng_copy_string_view(struct ArrowStringView view) {
    char *out;
    if (!view.data || view.size_bytes < 0) return NULL;
    out = (char *)duckdb_malloc((size_t)view.size_bytes + 1);
    if (!out) return NULL;
    memcpy(out, view.data, (size_t)view.size_bytes);
    out[view.size_bytes] = '\0';
    return out;
}

static int ducknng_decode_parameter_struct(const struct ArrowSchema *schema,
    struct ArrowArrayView *view, idx_t field_index, duckdb_value **out_values,
    idx_t *out_count, char **errmsg) {
    struct ArrowSchema *params_schema;
    struct ArrowArrayView *params_view;
    struct ArrowSchemaView params_schema_view;
    struct ArrowError error;
    duckdb_value *values = NULL;
    idx_t count;
    idx_t i;
    if (out_values) *out_values = NULL;
    if (out_count) *out_count = 0;
    memset(&params_schema_view, 0, sizeof(params_schema_view));
    memset(&error, 0, sizeof(error));
    if (!schema || !view || !out_values || !out_count || field_index >= (idx_t)schema->n_children ||
        field_index >= (idx_t)view->n_children) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing SQL parameter tuple");
        return -1;
    }
    params_schema = schema->children[field_index];
    params_view = view->children[field_index];
    if (!params_schema || !params_view || !params_schema->name ||
        strcmp(params_schema->name, "params") != 0 ||
        ArrowSchemaViewInit(&params_schema_view, params_schema, &error) != NANOARROW_OK ||
        params_schema_view.type != NANOARROW_TYPE_STRUCT) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: params must be a struct");
        return -1;
    }
    if (ArrowArrayViewIsNull(params_view, 0)) return 0;
    if (params_schema->n_children < 0 ||
        (uint64_t)params_schema->n_children > DUCKNNG_MAX_QUERY_PARAMETERS) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: SQL parameter count exceeds the protocol limit");
        return -1;
    }
    if (params_view->n_children != params_schema->n_children ||
        (params_schema->n_children > 0 &&
            (!params_schema->children || !params_view->children))) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: params schema and array children do not match");
        return -1;
    }
    count = (idx_t)params_schema->n_children;
    values = (duckdb_value *)duckdb_malloc(sizeof(*values) *
        (size_t)(count > 0 ? count : 1));
    if (!values) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: out of memory decoding SQL parameters");
        return -1;
    }
    memset(values, 0, sizeof(*values) * (size_t)(count > 0 ? count : 1));
    for (i = 0; i < count; i++) {
        if (ducknng_sql_arrow_value_at(params_schema->children[i],
                params_view->children[i], 0, &values[i], errmsg) != 0) {
            idx_t j;
            for (j = 0; j < count; j++) {
                if (values[j]) duckdb_destroy_value(&values[j]);
            }
            duckdb_free(values);
            return -1;
        }
    }
    *out_values = values;
    *out_count = count;
    return 0;
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
    if ((schema.n_children != 2 && schema.n_children != 3) || !schema.children ||
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
    if (schema.n_children == 3 && ducknng_decode_parameter_struct(&schema,
            &view, 2, &out->parameters, &out->parameter_count, errmsg) != 0) {
        goto cleanup;
    }
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

void ducknng_arrow_batches_reset(ducknng_arrow_batches *batches) {
    idx_t i;
    if (!batches) return;
    if (batches->arrays) {
        for (i = 0; i < batches->array_count; i++) {
            if (batches->arrays[i].release) ArrowArrayRelease(&batches->arrays[i]);
        }
        free(batches->arrays);
    }
    if (batches->schema.release) ArrowSchemaRelease(&batches->schema);
    memset(batches, 0, sizeof(*batches));
}

int ducknng_decode_ipc_batches_payload(const uint8_t *payload, size_t payload_len,
    ducknng_arrow_batches *out, char **errmsg) {
    struct ArrowBuffer input_buf;
    struct ArrowIpcInputStream input_stream;
    struct ArrowArrayStream stream;
    struct ArrowError error;
    idx_t cap = 0;
    int rc = -1;
    memset(&input_buf, 0, sizeof(input_buf));
    memset(&input_stream, 0, sizeof(input_stream));
    memset(&stream, 0, sizeof(stream));
    memset(&error, 0, sizeof(error));
    if (!payload || payload_len == 0 || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing Arrow batches payload");
        return -1;
    }
    memset(out, 0, sizeof(*out));
    ArrowBufferInit(&input_buf);
    if (ArrowBufferAppend(&input_buf, payload, (int64_t)payload_len) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy Arrow batches payload");
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
    if (ArrowArrayStreamGetSchema(&stream, &out->schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    for (;;) {
        struct ArrowArray arr;
        memset(&arr, 0, sizeof(arr));
        if (ArrowArrayStreamGetNext(&stream, &arr, &error) != NANOARROW_OK) {
            if (errmsg) *errmsg = ducknng_strdup(error.message);
            goto cleanup;
        }
        if (!arr.release) break;
        if (out->array_count >= cap) {
            idx_t newcap = cap == 0 ? 4 : cap * 2;
            struct ArrowArray *next = (struct ArrowArray *)realloc(out->arrays,
                (size_t)newcap * sizeof(*out->arrays));
            if (!next) {
                ArrowArrayRelease(&arr);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory collecting Arrow IPC batches");
                goto cleanup;
            }
            out->arrays = next;
            cap = newcap;
        }
        out->row_count += (idx_t)arr.length;
        out->arrays[out->array_count++] = arr;
    }
    rc = 0;
cleanup:
    if (rc != 0) ducknng_arrow_batches_reset(out);
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
    if (schema.n_children > 3 && schema.children[3] && schema.children[3]->name && strcmp(schema.children[3]->name, "correlation_id") == 0 && !ArrowArrayViewIsNull(view.children[3], 0)) {
        struct ArrowStringView correlation_view = ArrowArrayViewGetStringUnsafe(view.children[3], 0);
        out->correlation_id = ducknng_copy_string_view(correlation_view);
        if (!out->correlation_id) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy correlation_id from query_open payload");
            goto cleanup;
        }
    }
    if (schema.n_children > 4 && schema.children[4] && schema.children[4]->name && strcmp(schema.children[4]->name, "serialization_mode") == 0 && !ArrowArrayViewIsNull(view.children[4], 0)) {
        struct ArrowStringView mode_view = ArrowArrayViewGetStringUnsafe(view.children[4], 0);
        out->serialization_mode = ducknng_copy_string_view(mode_view);
        if (!out->serialization_mode) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy serialization_mode from query_open payload");
            goto cleanup;
        }
    }
    if (schema.n_children > 5) {
        if (ducknng_decode_parameter_struct(&schema, &view, 5,
                &out->parameters, &out->parameter_count, errmsg) != 0) goto cleanup;
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
    idx_t i;
    if (!req) return;
    if (req->sql) duckdb_free(req->sql);
    if (req->parameters) {
        for (i = 0; i < req->parameter_count; i++) {
            if (req->parameters[i]) duckdb_destroy_value(&req->parameters[i]);
        }
        duckdb_free(req->parameters);
    }
    req->sql = NULL;
    req->want_result = 0;
    req->parameters = NULL;
    req->parameter_count = 0;
}

void ducknng_query_open_request_destroy(ducknng_query_open_request *req) {
    idx_t i;
    if (!req) return;
    if (req->sql) duckdb_free(req->sql);
    if (req->correlation_id) duckdb_free(req->correlation_id);
    if (req->serialization_mode) duckdb_free(req->serialization_mode);
    if (req->parameters) {
        for (i = 0; i < req->parameter_count; i++) {
            if (req->parameters[i]) duckdb_destroy_value(&req->parameters[i]);
        }
        duckdb_free(req->parameters);
    }
    req->sql = NULL;
    req->correlation_id = NULL;
    req->serialization_mode = NULL;
    req->parameters = NULL;
    req->parameter_count = 0;
    req->batch_rows = 0;
    req->batch_bytes = 0;
}
