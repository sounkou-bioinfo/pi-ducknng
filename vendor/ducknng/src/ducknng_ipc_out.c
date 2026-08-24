#include "ducknng_ipc_out.h"
#include "ducknng_util.h"
#include "nanoarrow/nanoarrow.h"
#include "nanoarrow/nanoarrow_ipc.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* -------------------------------------------------------------------------
 * Fixed-schema control-message helpers (unchanged from previous version)
 * ---------------------------------------------------------------------- */

static int ducknng_build_exec_request_schema(struct ArrowSchema *schema, struct ArrowError *error) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 2) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[0], "sql") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[1], "want_result") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_BOOL) != NANOARROW_OK) return -1;
    (void)error;
    return 0;
}

static int ducknng_build_query_open_request_schema(struct ArrowSchema *schema, struct ArrowError *error) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 3) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[0], "sql") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[1], "batch_rows") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_UINT64) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[2], "batch_bytes") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[2], NANOARROW_TYPE_UINT64) != NANOARROW_OK) return -1;
    (void)error;
    return 0;
}

static int ducknng_build_metadata_schema(struct ArrowSchema *schema, struct ArrowError *error) {
    ArrowSchemaInit(schema);
    if (ArrowSchemaSetTypeStruct(schema, 3) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[0], "rows_changed") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_UINT64) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[1], "statement_type") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_INT32) != NANOARROW_OK) return -1;
    if (ArrowSchemaSetName(schema->children[2], "result_type") != NANOARROW_OK) return -1;
    if (ArrowSchemaSetType(schema->children[2], NANOARROW_TYPE_INT32) != NANOARROW_OK) return -1;
    (void)error;
    return 0;
}

/* -------------------------------------------------------------------------
 * IPC serialisation: schema + N Arrow arrays → raw IPC bytes
 *
 * Ownership transfer: on success, schema and arrays[0..n-1] are consumed by
 * the stream and must not be released by the caller. On failure, all are
 * still owned by the caller (and still need releasing).
 * ---------------------------------------------------------------------- */

static int ducknng_ipc_arrays_to_bytes(struct ArrowSchema *schema,
    struct ArrowArray *arrays, int64_t n,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    uint8_t *copy = NULL;
    int64_t i;
    int rc = -1;

    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));

    if (ArrowBasicArrayStreamInit(&stream, schema, n) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow IPC stream");
        return -1;
    }
    memset(schema, 0, sizeof(*schema)); /* stream owns it now */
    for (i = 0; i < n; i++) {
        ArrowBasicArrayStreamSetArray(&stream, i, &arrays[i]);
        memset(&arrays[i], 0, sizeof(arrays[i])); /* stream owns it now */
    }

    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow IPC output stream");
        goto cleanup;
    }
    if (ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize Arrow IPC writer");
        goto cleanup;
    }
    if (ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message : "ducknng: failed to write Arrow IPC stream");
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying Arrow IPC payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;

cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    return rc;
}

/* -------------------------------------------------------------------------
 * Build an Arrow schema from a DuckDB result using DuckDB's own conversion.
 * ---------------------------------------------------------------------- */

static int ducknng_result_to_arrow_schema(duckdb_result *result,
    duckdb_arrow_options opts, struct ArrowSchema *out_schema, char **errmsg) {
    idx_t ncols = duckdb_column_count(result);
    idx_t col;
    duckdb_logical_type *types = NULL;
    const char **names = NULL;
    duckdb_error_data err = NULL;
    int rc = -1;

    types = (duckdb_logical_type *)malloc(sizeof(*types) * (size_t)ncols);
    names = (const char **)malloc(sizeof(*names) * (size_t)ncols);
    if (!types || !names) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building Arrow schema arrays");
        goto cleanup;
    }
    memset(types, 0, sizeof(*types) * (size_t)ncols);
    for (col = 0; col < ncols; col++) {
        types[col] = duckdb_column_logical_type(result, col);
        names[col] = duckdb_column_name(result, col);
    }
    err = duckdb_to_arrow_schema(opts, types, names, ncols, out_schema);
    if (duckdb_error_data_has_error(err)) {
        if (errmsg) *errmsg = ducknng_strdup(duckdb_error_data_message(err));
        goto cleanup;
    }
    rc = 0;

cleanup:
    if (err) duckdb_destroy_error_data(&err);
    if (types) {
        for (col = 0; col < ncols; col++)
            if (types[col]) duckdb_destroy_logical_type(&types[col]);
        free(types);
    }
    if (names) free(names);
    return rc;
}

/* -------------------------------------------------------------------------
 * Flatten dictionary-encoded columns in an ArrowArray / ArrowSchema pair.
 *
 * DuckDB encodes ENUM columns as dictionary-encoded Arrow arrays (integer
 * indices + a string dictionary).  The nanoarrow IPC writer does not support
 * dictionary batches, so we resolve any dictionary-encoded top-level column
 * to a plain UTF-8 string column before serialisation.  The types.md contract
 * says ENUM is transported as resolved UTF-8 strings anyway.
 * ---------------------------------------------------------------------- */

static int ducknng_flatten_one_dict_child(struct ArrowSchema *schema,
    struct ArrowArray *array, int64_t col, char **errmsg) {
    struct ArrowArrayView idx_view;
    struct ArrowArrayView dict_view;
    struct ArrowSchema *child_schema = schema->children[col];
    struct ArrowArray *child_array  = array->children[col];
    struct ArrowArray flat;
    struct ArrowError aerr;
    const char *col_name = NULL;
    int64_t i, n;
    int rc = -1;

    memset(&idx_view,  0, sizeof(idx_view));
    memset(&dict_view, 0, sizeof(dict_view));
    memset(&flat, 0, sizeof(flat));
    memset(&aerr, 0, sizeof(aerr));

    /* Initialise views for index array and dictionary array. */
    if (ArrowArrayViewInitFromSchema(&idx_view, child_schema, &aerr) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(aerr.message);
        goto done;
    }
    if (ArrowArrayViewSetArray(&idx_view, child_array, &aerr) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(aerr.message);
        goto done;
    }
    if (ArrowArrayViewInitFromSchema(&dict_view, child_schema->dictionary, &aerr) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(aerr.message);
        goto done;
    }
    if (ArrowArrayViewSetArray(&dict_view, child_array->dictionary, &aerr) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(aerr.message);
        goto done;
    }

    /* Build a plain UTF-8 string array. */
    if (ArrowArrayInitFromType(&flat, NANOARROW_TYPE_STRING) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to init flat string array for ENUM column");
        goto done;
    }
    n = child_array->length;
    if (ArrowArrayStartAppending(&flat) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start appending to flat array");
        goto done;
    }
    for (i = 0; i < n; i++) {
        if (ArrowArrayViewIsNull(&idx_view, i)) {
            if (ArrowArrayAppendNull(&flat, 1) != NANOARROW_OK) goto oom;
        } else {
            int64_t idx = ArrowArrayViewGetIntUnsafe(&idx_view, i);
            struct ArrowStringView sv = ArrowArrayViewGetStringUnsafe(&dict_view, idx);
            if (ArrowArrayAppendString(&flat, sv) != NANOARROW_OK) goto oom;
        }
    }
    if (ArrowArrayFinishBuildingDefault(&flat, &aerr) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(aerr.message);
        if (flat.release) ArrowArrayRelease(&flat);
        goto done;
    }

    /* Replace child schema: point to plain UTF-8, keep name. */
    col_name = child_schema->name ? child_schema->name : "";
    ArrowSchemaRelease(child_schema);
    ArrowSchemaInit(child_schema);
    if (ArrowSchemaSetType(child_schema, NANOARROW_TYPE_STRING) != NANOARROW_OK ||
        ArrowSchemaSetName(child_schema, col_name) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to reset schema for flattened ENUM column");
        if (flat.release) ArrowArrayRelease(&flat);
        goto done;
    }

    /* Replace child array. */
    if (child_array->release) ArrowArrayRelease(child_array);
    *child_array = flat;
    memset(&flat, 0, sizeof(flat)); /* transferred */
    rc = 0;
    goto done;

oom:
    if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory flattening ENUM column");
    if (flat.release) ArrowArrayRelease(&flat);
done:
    ArrowArrayViewReset(&idx_view);
    ArrowArrayViewReset(&dict_view);
    return rc;
}

/* Flatten all dictionary-encoded top-level columns in schema/array in place. */
static int ducknng_flatten_dict_columns(struct ArrowSchema *schema,
    struct ArrowArray *array, char **errmsg) {
    int64_t i;
    for (i = 0; i < schema->n_children; i++) {
        if (schema->children[i] && schema->children[i]->dictionary) {
            if (ducknng_flatten_one_dict_child(schema, array, i, errmsg) != 0)
                return -1;
        }
    }
    return 0;
}

/* -------------------------------------------------------------------------
 * Public API: result → IPC stream (all chunks)
 * ---------------------------------------------------------------------- */

int ducknng_result_to_ipc_stream(duckdb_prepared_statement stmt, duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray *arrays = NULL;
    int64_t nchunks = 0;
    int64_t cap = 0;
    duckdb_data_chunk chunk = NULL;
    duckdb_arrow_options opts = NULL;
    duckdb_error_data err = NULL;
    int rc = -1;
    (void)stmt;

    memset(&schema, 0, sizeof(schema));

    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid result output pointers");
        return -1;
    }

    opts = duckdb_result_get_arrow_options(&result);
    if (!opts) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to get Arrow options from result");
        return -1;
    }

    if (ducknng_result_to_arrow_schema(&result, opts, &schema, errmsg) != 0)
        goto cleanup;

    for (;;) {
        struct ArrowArray arr;
        memset(&arr, 0, sizeof(arr));

        chunk = duckdb_fetch_chunk(result);
        if (!chunk) break;

        err = duckdb_data_chunk_to_arrow(opts, chunk, &arr);
        duckdb_destroy_data_chunk(&chunk);
        chunk = NULL;

        if (duckdb_error_data_has_error(err)) {
            if (errmsg) *errmsg = ducknng_strdup(duckdb_error_data_message(err));
            if (arr.release) ArrowArrayRelease(&arr);
            duckdb_destroy_error_data(&err);
            err = NULL;
            goto cleanup;
        }
        duckdb_destroy_error_data(&err);
        err = NULL;

        if (ducknng_flatten_dict_columns(&schema, &arr, errmsg) != 0) {
            if (arr.release) ArrowArrayRelease(&arr);
            goto cleanup;
        }

        if (nchunks >= cap) {
            int64_t newcap = cap == 0 ? 4 : cap * 2;
            struct ArrowArray *tmp = (struct ArrowArray *)realloc(arrays,
                (size_t)newcap * sizeof(*arrays));
            if (!tmp) {
                if (arr.release) ArrowArrayRelease(&arr);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory collecting Arrow chunks");
                goto cleanup;
            }
            arrays = tmp;
            cap = newcap;
        }
        arrays[nchunks++] = arr;
    }

    /* Empty result: still need a 0-batch stream. */
    rc = ducknng_ipc_arrays_to_bytes(&schema, arrays, nchunks, out_bytes, out_len, errmsg);
    arrays = NULL; /* consumed by stream on success path; released in cleanup loop otherwise */
    goto done;

cleanup:
    rc = -1;
done:
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    if (err) duckdb_destroy_error_data(&err);
    if (opts) duckdb_destroy_arrow_options(&opts);
    if (arrays) {
        int64_t i;
        for (i = 0; i < nchunks; i++)
            if (arrays[i].release) ArrowArrayRelease(&arrays[i]);
        free(arrays);
    }
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

/* -------------------------------------------------------------------------
 * Public API: prepared statement schema → IPC stream (0 data batches)
 * Uses unstable_new_prepared_statement_functions to read column metadata
 * before execution, and unstable_new_open_connect_functions Arrow options
 * for encoding fidelity.
 * ---------------------------------------------------------------------- */

int ducknng_prepared_schema_to_ipc(duckdb_connection con, duckdb_prepared_statement stmt,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    idx_t ncols;
    idx_t col;
    duckdb_logical_type *types = NULL;
    const char **names = NULL;
    duckdb_arrow_options opts = NULL;
    duckdb_error_data err = NULL;
    struct ArrowSchema schema;
    int rc = -1;

    memset(&schema, 0, sizeof(schema));

    if (!con || !stmt || !out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid arguments to ducknng_prepared_schema_to_ipc");
        return -1;
    }

    ncols = duckdb_prepared_statement_column_count(stmt);
    if (ncols > 0) {
        types = (duckdb_logical_type *)malloc(sizeof(*types) * (size_t)ncols);
        names = (const char **)malloc(sizeof(*names) * (size_t)ncols);
        if (!types || !names) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building prepared schema arrays");
            goto cleanup;
        }
        memset(types, 0, sizeof(*types) * (size_t)ncols);
        for (col = 0; col < ncols; col++) {
            types[col] = duckdb_prepared_statement_column_logical_type(stmt, col);
            names[col] = duckdb_prepared_statement_column_name(stmt, col);
        }
    }

    duckdb_connection_get_arrow_options(con, &opts);
    if (!opts) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to get Arrow options for prepared schema");
        goto cleanup;
    }

    err = duckdb_to_arrow_schema(opts, types, names, ncols, &schema);
    if (duckdb_error_data_has_error(err)) {
        if (errmsg) *errmsg = ducknng_strdup(duckdb_error_data_message(err));
        goto cleanup;
    }

    /* Serialize schema with 0 data batches. */
    rc = ducknng_ipc_arrays_to_bytes(&schema, NULL, 0, out_bytes, out_len, errmsg);
    memset(&schema, 0, sizeof(schema)); /* consumed on success */
    goto done;

cleanup:
    rc = -1;
done:
    if (err) duckdb_destroy_error_data(&err);
    if (opts) duckdb_destroy_arrow_options(&opts);
    if (types) {
        for (col = 0; col < ncols; col++)
            if (types[col]) duckdb_destroy_logical_type(&types[col]);
        free(types);
    }
    if (names) free(names);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}


int ducknng_result_next_chunk_to_ipc(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray arr;
    duckdb_data_chunk chunk = NULL;
    duckdb_arrow_options opts = NULL;
    duckdb_error_data err = NULL;
    int rc = -1;

    memset(&schema, 0, sizeof(schema));
    memset(&arr, 0, sizeof(arr));
    if (has_chunk) *has_chunk = 0;

    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid next chunk output pointers");
        return -1;
    }

    chunk = duckdb_fetch_chunk(result);
    if (!chunk) {
        if (has_chunk) *has_chunk = 0;
        return 0;
    }

    opts = duckdb_result_get_arrow_options(&result);
    if (!opts) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to get Arrow options from result");
        goto cleanup;
    }

    if (ducknng_result_to_arrow_schema(&result, opts, &schema, errmsg) != 0)
        goto cleanup;

    err = duckdb_data_chunk_to_arrow(opts, chunk, &arr);
    duckdb_destroy_data_chunk(&chunk);
    chunk = NULL;

    if (duckdb_error_data_has_error(err)) {
        if (errmsg) *errmsg = ducknng_strdup(duckdb_error_data_message(err));
        goto cleanup;
    }
    duckdb_destroy_error_data(&err);
    err = NULL;

    if (ducknng_flatten_dict_columns(&schema, &arr, errmsg) != 0)
        goto cleanup;

    /* ducknng_ipc_arrays_to_bytes takes ownership on success. */
    if (ducknng_ipc_arrays_to_bytes(&schema, &arr, 1, out_bytes, out_len, errmsg) != 0) {
        /* arr still needs releasing — schema was cleared inside, arr was cleared inside on success only */
        goto cleanup;
    }
    if (has_chunk) *has_chunk = 1;
    return 0;

cleanup:
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    if (err) duckdb_destroy_error_data(&err);
    if (opts) duckdb_destroy_arrow_options(&opts);
    if (arr.release) ArrowArrayRelease(&arr);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

/* -------------------------------------------------------------------------
 * Public API: run SQL query → IPC stream
 * ---------------------------------------------------------------------- */

int ducknng_query_to_ipc_stream(duckdb_connection con, const char *sql,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    duckdb_result result;
    int rc;
    memset(&result, 0, sizeof(result));
    if (!con || !sql || !sql[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid query_to_ipc_stream arguments");
        return -1;
    }
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        const char *detail = duckdb_result_error(&result);
        if (errmsg) *errmsg = ducknng_strdup(detail && detail[0] ? detail : "ducknng: query failed");
        duckdb_destroy_result(&result);
        return -1;
    }
    if (duckdb_result_return_type(result) != DUCKDB_RESULT_TYPE_QUERY_RESULT) {
        duckdb_destroy_result(&result);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: query_to_ipc_stream requires a row-returning query");
        return -1;
    }
    rc = ducknng_result_to_ipc_stream(NULL, result, out_bytes, out_len, errmsg);
    duckdb_destroy_result(&result);
    return rc;
}

/* -------------------------------------------------------------------------
 * Public API: exec request → IPC
 * ---------------------------------------------------------------------- */

int ducknng_exec_request_to_ipc(const char *sql, int want_result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    struct ArrowStringView sql_view;
    uint8_t *copy = NULL;
    int rc = -1;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));
    if (!sql || !out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid exec request arguments");
        return -1;
    }
    if (ducknng_build_exec_request_schema(&schema, &error) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to build exec request Arrow schema");
        goto cleanup;
    }
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start exec request Arrow append");
        goto cleanup;
    }
    sql_view.data = sql;
    sql_view.size_bytes = (int64_t)strlen(sql);
    if (ArrowArrayAppendString(array.children[0], sql_view) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[1], want_result ? 1 : 0) != NANOARROW_OK ||
        ArrowArrayFinishElement(&array) != NANOARROW_OK ||
        ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to build exec request Arrow payload");
        goto cleanup;
    }
    if (ArrowBasicArrayStreamInit(&stream, &schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize exec request Arrow stream");
        goto cleanup;
    }
    memset(&schema, 0, sizeof(schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);
    memset(&array, 0, sizeof(array));
    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK ||
        ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK ||
        ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to encode exec request Arrow IPC payload");
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying exec request payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;
cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

/* -------------------------------------------------------------------------
 * Public API: query_open request → IPC
 * ---------------------------------------------------------------------- */

int ducknng_query_open_request_to_ipc(const char *sql, uint64_t batch_rows,
    uint64_t batch_bytes, uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    struct ArrowStringView sql_view;
    uint8_t *copy = NULL;
    int rc = -1;
    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));
    if (!sql || !sql[0] || !out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid query_open request arguments");
        return -1;
    }
    if (ducknng_build_query_open_request_schema(&schema, &error) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to build query_open request Arrow schema");
        goto cleanup;
    }
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start query_open request Arrow append");
        goto cleanup;
    }
    sql_view.data = sql;
    sql_view.size_bytes = (int64_t)strlen(sql);
    if (ArrowArrayAppendString(array.children[0], sql_view) != NANOARROW_OK ||
        (batch_rows > 0 ? ArrowArrayAppendUInt(array.children[1], batch_rows) : ArrowArrayAppendNull(array.children[1], 1)) != NANOARROW_OK ||
        (batch_bytes > 0 ? ArrowArrayAppendUInt(array.children[2], batch_bytes) : ArrowArrayAppendNull(array.children[2], 1)) != NANOARROW_OK ||
        ArrowArrayFinishElement(&array) != NANOARROW_OK ||
        ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to build query_open request Arrow payload");
        goto cleanup;
    }
    if (ArrowBasicArrayStreamInit(&stream, &schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize query_open request Arrow stream");
        goto cleanup;
    }
    memset(&schema, 0, sizeof(schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);
    memset(&array, 0, sizeof(array));
    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK ||
        ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK ||
        ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to encode query_open request Arrow IPC payload");
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying query_open request payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;
cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}

/* -------------------------------------------------------------------------
 * Public API: session request JSON
 * ---------------------------------------------------------------------- */

char *ducknng_session_request_json(uint64_t session_id, const char *session_token,
    uint64_t batch_rows, uint64_t batch_bytes) {
    char buf[320];
    int n;
    if (session_id == 0 || !session_token || !session_token[0]) return NULL;
    n = snprintf(buf, sizeof(buf), "{\"session_id\":%llu,\"session_token\":\"%s\"",
        (unsigned long long)session_id, session_token);
    if (n < 0 || (size_t)n >= sizeof(buf)) return NULL;
    if (batch_rows > 0) {
        n += snprintf(buf + n, sizeof(buf) - (size_t)n, ",\"batch_rows\":%llu",
            (unsigned long long)batch_rows);
        if ((size_t)n >= sizeof(buf)) return NULL;
    }
    if (batch_bytes > 0) {
        n += snprintf(buf + n, sizeof(buf) - (size_t)n, ",\"batch_bytes\":%llu",
            (unsigned long long)batch_bytes);
        if ((size_t)n >= sizeof(buf)) return NULL;
    }
    n += snprintf(buf + n, sizeof(buf) - (size_t)n, "}");
    if ((size_t)n >= sizeof(buf)) return NULL;
    return ducknng_strdup(buf);
}

/* -------------------------------------------------------------------------
 * Public API: exec metadata → IPC
 * ---------------------------------------------------------------------- */

int ducknng_exec_metadata_to_ipc(uint64_t rows_changed,
    uint32_t statement_type, uint32_t result_type, uint8_t **out_bytes,
    size_t *out_len, char **errmsg) {
    struct ArrowSchema schema;
    struct ArrowArray array;
    struct ArrowArrayStream stream;
    struct ArrowIpcOutputStream output_stream;
    struct ArrowIpcWriter writer;
    struct ArrowBuffer output_buf;
    struct ArrowError error;
    uint8_t *copy = NULL;
    int rc = -1;

    memset(&schema, 0, sizeof(schema));
    memset(&array, 0, sizeof(array));
    memset(&stream, 0, sizeof(stream));
    memset(&output_stream, 0, sizeof(output_stream));
    memset(&writer, 0, sizeof(writer));
    memset(&output_buf, 0, sizeof(output_buf));
    memset(&error, 0, sizeof(error));

    if (!out_bytes || !out_len) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid metadata output pointers");
        return -1;
    }
    if (ducknng_build_metadata_schema(&schema, &error) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to build metadata Arrow schema");
        goto cleanup;
    }
    if (ArrowArrayInitFromSchema(&array, &schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start metadata Arrow array append");
        goto cleanup;
    }
    if (ArrowArrayAppendUInt(array.children[0], rows_changed) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[1], (int64_t)statement_type) != NANOARROW_OK ||
        ArrowArrayAppendInt(array.children[2], (int64_t)result_type) != NANOARROW_OK ||
        ArrowArrayFinishElement(&array) != NANOARROW_OK ||
        ArrowArrayFinishBuildingDefault(&array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to build metadata Arrow array");
        goto cleanup;
    }
    if (ArrowBasicArrayStreamInit(&stream, &schema, 1) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow stream");
        goto cleanup;
    }
    memset(&schema, 0, sizeof(schema));
    ArrowBasicArrayStreamSetArray(&stream, 0, &array);
    memset(&array, 0, sizeof(array));
    ArrowBufferInit(&output_buf);
    if (ArrowIpcOutputStreamInitBuffer(&output_stream, &output_buf) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow IPC output stream");
        goto cleanup;
    }
    if (ArrowIpcWriterInit(&writer, &output_stream) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize metadata Arrow IPC writer");
        goto cleanup;
    }
    if (ArrowIpcWriterWriteArrayStream(&writer, &stream, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        goto cleanup;
    }
    copy = (uint8_t *)duckdb_malloc((size_t)output_buf.size_bytes);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying metadata Arrow IPC payload");
        goto cleanup;
    }
    memcpy(copy, output_buf.data, (size_t)output_buf.size_bytes);
    *out_bytes = copy;
    *out_len = (size_t)output_buf.size_bytes;
    copy = NULL;
    rc = 0;

cleanup:
    if (copy) duckdb_free(copy);
    if (writer.private_data) ArrowIpcWriterReset(&writer);
    if (output_stream.release) output_stream.release(&output_stream);
    if (output_buf.data) ArrowBufferReset(&output_buf);
    if (stream.release) ArrowArrayStreamRelease(&stream);
    if (array.release) ArrowArrayRelease(&array);
    if (schema.release) ArrowSchemaRelease(&schema);
    return rc;
}
