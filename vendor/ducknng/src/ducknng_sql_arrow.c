#include "ducknng_sql_arrow.h"
#include "ducknng_util.h"
#include <stdint.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/*
 * Decoded remote Arrow schemas and arrays are external input. Bound the nesting
 * depth of type conversion, value assignment, and nested-null marking so a
 * hostile or pathological deeply nested schema produces a DuckDB-visible error
 * instead of unbounded recursion (a stack-exhaustion crash). DuckDB's practical
 * nesting is far below this; the limit only rejects abuse. It is a design
 * invariant, hence the compile-time check.
 */
#define DUCKNNG_ARROW_MAX_NESTING_DEPTH 64
_Static_assert(DUCKNNG_ARROW_MAX_NESTING_DEPTH > 0 &&
    DUCKNNG_ARROW_MAX_NESTING_DEPTH <= 1024,
    "ducknng Arrow nesting depth limit must be a small positive bound");

static void ducknng_sql_arrow_set_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity;
    duckdb_vector_ensure_validity_writable(vec);
    validity = duckdb_vector_get_validity(vec);
    duckdb_validity_set_row_invalid(validity, row);
}

static void ducknng_sql_arrow_assign_blob(duckdb_vector vec, idx_t row,
    const uint8_t *data, idx_t len) {
    /* BLOB is opaque binary; no UTF-8 validation needed. */
    duckdb_unsafe_vector_assign_string_element_len(vec, row, (const char *)data, len);
}

static int64_t ducknng_sql_arrow_floor_div_i64(int64_t value, int64_t divisor) {
    int64_t q = value / divisor;
    int64_t r = value % divisor;
    return (r != 0 && ((r < 0) != (divisor < 0))) ? q - 1 : q;
}

static int ducknng_sql_arrow_assign_column_at(duckdb_vector vec,
    struct ArrowArrayView *col_view, const struct ArrowSchema *col_schema,
    idx_t src_offset, idx_t dst_offset, idx_t count, int depth, char **errmsg);

static int ducknng_sql_arrow_schema_to_logical_type_depth(const struct ArrowSchema *schema,
    duckdb_logical_type *out_type, int depth, char **errmsg);

static int ducknng_sql_arrow_set_nested_null(duckdb_vector vec,
    const struct ArrowSchema *schema, idx_t index, int depth) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    idx_t child;
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    ducknng_sql_arrow_set_null(vec, index);
    /* Best-effort null marking: stop descending past the bound rather than
     * recursing without limit. Over-deep schemas are already rejected at bind
     * time by the type-conversion path, so this branch is defensive. */
    if (depth > DUCKNNG_ARROW_MAX_NESTING_DEPTH) return 0;
    if (!schema || ArrowSchemaViewInit(&schema_view, schema, &error) != NANOARROW_OK) return 0;
    if (schema_view.type == NANOARROW_TYPE_STRUCT) {
        for (child = 0; child < (idx_t)schema->n_children; child++) {
            duckdb_vector child_vec = duckdb_struct_vector_get_child(vec, child);
            (void)ducknng_sql_arrow_set_nested_null(child_vec, schema->children[child], index, depth + 1);
        }
    } else if (schema_view.type == NANOARROW_TYPE_DENSE_UNION ||
               schema_view.type == NANOARROW_TYPE_SPARSE_UNION) {
        /* null the tag child and all member children */
        for (child = 0; child < (idx_t)schema->n_children + 1; child++) {
            duckdb_vector child_vec = duckdb_struct_vector_get_child(vec, child);
            ducknng_sql_arrow_set_null(child_vec, index);
        }
    } else if (schema_view.type == NANOARROW_TYPE_LIST ||
               schema_view.type == NANOARROW_TYPE_LARGE_LIST) {
        duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(vec);
        entries[index].offset = duckdb_list_vector_get_size(vec);
        entries[index].length = 0;
    }
    return 0;
}

static int ducknng_sql_arrow_assign_decimal(duckdb_vector vec, idx_t dst_index,
    const struct ArrowSchemaView *schema_view, const struct ArrowArrayView *col_view,
    int64_t src_index) {
    duckdb_logical_type logical_type = duckdb_vector_get_column_type(vec);
    duckdb_type internal_type;
    struct ArrowDecimal decimal;
    if (!logical_type) return -1;
    internal_type = duckdb_decimal_internal_type(logical_type);
    duckdb_destroy_logical_type(&logical_type);
    ArrowDecimalInit(&decimal, schema_view->decimal_bitwidth,
        schema_view->decimal_precision, schema_view->decimal_scale);
    ArrowArrayViewGetDecimalUnsafe(col_view, src_index, &decimal);
    switch (internal_type) {
    case DUCKDB_TYPE_SMALLINT:
        ((int16_t *)duckdb_vector_get_data(vec))[dst_index] =
            (int16_t)ArrowDecimalGetIntUnsafe(&decimal);
        return 0;
    case DUCKDB_TYPE_INTEGER:
        ((int32_t *)duckdb_vector_get_data(vec))[dst_index] =
            (int32_t)ArrowDecimalGetIntUnsafe(&decimal);
        return 0;
    case DUCKDB_TYPE_BIGINT:
        ((int64_t *)duckdb_vector_get_data(vec))[dst_index] =
            (int64_t)ArrowDecimalGetIntUnsafe(&decimal);
        return 0;
    case DUCKDB_TYPE_HUGEINT: {
        duckdb_hugeint value;
        value.lower = decimal.words[decimal.low_word_index];
        value.upper = (int64_t)decimal.words[decimal.high_word_index];
        ((duckdb_hugeint *)duckdb_vector_get_data(vec))[dst_index] = value;
        return 0;
    }
    default:
        return -1;
    }
}

static int ducknng_sql_arrow_schema_to_logical_type_depth(const struct ArrowSchema *schema,
    duckdb_logical_type *out_type, int depth, char **errmsg) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    if (out_type) *out_type = NULL;
    if (!schema || !out_type) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing Arrow schema");
        return -1;
    }
    if (depth > DUCKNNG_ARROW_MAX_NESTING_DEPTH) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: Arrow schema nesting exceeds supported depth");
        return -1;
    }
    if (ArrowSchemaViewInit(&schema_view, schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        return -1;
    }
    switch (schema_view.type) {
    case NANOARROW_TYPE_NA:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_SQLNULL);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_BOOL:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_INT8:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TINYINT);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_INT16:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_INT32:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_INT64:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_UINT8:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UTINYINT);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_UINT16:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_USMALLINT);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_UINT32:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_UINT64:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_FLOAT:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_DOUBLE:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_STRING:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_BINARY:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_DATE32:
    case NANOARROW_TYPE_DATE64:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DATE);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64:
        if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME_NS);
        } else {
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME);
        }
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_TIMESTAMP:
        if (schema_view.timezone && schema_view.timezone[0]) {
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_TZ);
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND) {
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_S);
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI) {
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_MS);
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_NS);
        } else {
            *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP);
        }
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_DECIMAL32:
    case NANOARROW_TYPE_DECIMAL64:
    case NANOARROW_TYPE_DECIMAL128:
        *out_type = duckdb_create_decimal_type(
            (uint8_t)schema_view.decimal_precision,
            (uint8_t)schema_view.decimal_scale);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_DENSE_UNION:
    case NANOARROW_TYPE_SPARSE_UNION: {
        idx_t nmembers = (idx_t)schema->n_children;
        duckdb_logical_type *member_types = NULL;
        const char **member_names = NULL;
        idx_t i2;
        int ok = 1;
        if (nmembers > 0) {
            member_types = (duckdb_logical_type *)duckdb_malloc(sizeof(*member_types) * (size_t)nmembers);
            member_names = (const char **)duckdb_malloc(sizeof(*member_names) * (size_t)nmembers);
            if (!member_types || !member_names) ok = 0;
            if (member_types) memset(member_types, 0, sizeof(*member_types) * (size_t)nmembers);
            if (member_names) memset(member_names, 0, sizeof(*member_names) * (size_t)nmembers);
        }
        for (i2 = 0; ok && i2 < nmembers; i2++) {
            member_names[i2] = schema->children[i2] && schema->children[i2]->name
                ? schema->children[i2]->name : "";
            ok = schema->children[i2] &&
                ducknng_sql_arrow_schema_to_logical_type_depth(schema->children[i2], &member_types[i2], depth + 1, errmsg) == 0;
        }
        if (ok) *out_type = duckdb_create_union_type(member_types, member_names, nmembers);
        ok = ok && *out_type;
        if (member_types) {
            for (i2 = 0; i2 < nmembers; i2++) {
                if (member_types[i2]) duckdb_destroy_logical_type(&member_types[i2]);
            }
            duckdb_free(member_types);
        }
        if (member_names) duckdb_free(member_names);
        return ok ? 0 : -1;
    }
    case NANOARROW_TYPE_LARGE_STRING:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_LARGE_BINARY:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_DURATION:
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    case NANOARROW_TYPE_INTERVAL_DAY_TIME:
    case NANOARROW_TYPE_INTERVAL_MONTHS:
        *out_type = duckdb_create_logical_type(DUCKDB_TYPE_INTERVAL);
        return *out_type ? 0 : -1;
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST: {
        duckdb_logical_type child_type = NULL;
        int ok = schema->n_children == 1 && schema->children && schema->children[0] &&
            ducknng_sql_arrow_schema_to_logical_type_depth(schema->children[0], &child_type, depth + 1, errmsg) == 0 &&
            child_type;
        if (ok) *out_type = duckdb_create_list_type(child_type);
        if (child_type) duckdb_destroy_logical_type(&child_type);
        return ok && *out_type ? 0 : -1;
    }
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
        duckdb_logical_type child_type = NULL;
        int ok = schema_view.fixed_size > 0 && schema->n_children == 1 &&
            schema->children && schema->children[0] &&
            ducknng_sql_arrow_schema_to_logical_type_depth(schema->children[0],
                &child_type, depth + 1, errmsg) == 0 && child_type;
        if (ok) *out_type = duckdb_create_array_type(child_type,
            (idx_t)schema_view.fixed_size);
        if (child_type) duckdb_destroy_logical_type(&child_type);
        return ok && *out_type ? 0 : -1;
    }
    case NANOARROW_TYPE_MAP: {
        duckdb_logical_type key_type = NULL;
        duckdb_logical_type value_type = NULL;
        int ok = schema->n_children == 1 && schema->children && schema->children[0] &&
            schema->children[0]->n_children == 2 &&
            ducknng_sql_arrow_schema_to_logical_type_depth(schema->children[0]->children[0], &key_type, depth + 1, errmsg) == 0 &&
            key_type &&
            ducknng_sql_arrow_schema_to_logical_type_depth(schema->children[0]->children[1], &value_type, depth + 1, errmsg) == 0 &&
            value_type;
        if (ok) *out_type = duckdb_create_map_type(key_type, value_type);
        if (key_type) duckdb_destroy_logical_type(&key_type);
        if (value_type) duckdb_destroy_logical_type(&value_type);
        return ok && *out_type ? 0 : -1;
    }
    case NANOARROW_TYPE_STRUCT: {
        idx_t nchildren = (idx_t)schema->n_children;
        duckdb_logical_type *child_types = NULL;
        const char **child_names = NULL;
        idx_t i;
        int ok = 1;
        if (nchildren > 0) {
            child_types = (duckdb_logical_type *)duckdb_malloc(sizeof(*child_types) * (size_t)nchildren);
            child_names = (const char **)duckdb_malloc(sizeof(*child_names) * (size_t)nchildren);
            if (!child_types || !child_names) ok = 0;
            if (child_types) memset(child_types, 0, sizeof(*child_types) * (size_t)nchildren);
            if (child_names) memset(child_names, 0, sizeof(*child_names) * (size_t)nchildren);
        }
        for (i = 0; ok && i < nchildren; i++) {
            child_names[i] = schema->children[i] && schema->children[i]->name ? schema->children[i]->name : "";
            ok = schema->children[i] &&
                ducknng_sql_arrow_schema_to_logical_type_depth(schema->children[i], &child_types[i], depth + 1, errmsg) == 0;
        }
        if (ok) *out_type = duckdb_create_struct_type(child_types, child_names, nchildren);
        ok = ok && *out_type;
        if (child_types) {
            for (i = 0; i < nchildren; i++) {
                if (child_types[i]) duckdb_destroy_logical_type(&child_types[i]);
            }
            duckdb_free(child_types);
        }
        if (child_names) duckdb_free(child_names);
        return ok ? 0 : -1;
    }
    default:
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: unsupported Arrow type in remote row schema");
        return -1;
    }
}

int ducknng_sql_arrow_schema_to_logical_type(const struct ArrowSchema *schema,
    duckdb_logical_type *out_type, char **errmsg) {
    return ducknng_sql_arrow_schema_to_logical_type_depth(schema, out_type, 0, errmsg);
}

static void ducknng_sql_arrow_destroy_values(duckdb_value *values, idx_t count) {
    idx_t i;
    if (!values) return;
    for (i = 0; i < count; i++) {
        if (values[i]) duckdb_destroy_value(&values[i]);
    }
    duckdb_free(values);
}

static duckdb_value *ducknng_sql_arrow_alloc_values(idx_t count) {
    duckdb_value *values = (duckdb_value *)duckdb_malloc(
        sizeof(*values) * (size_t)(count > 0 ? count : 1));
    if (values) memset(values, 0, sizeof(*values) * (size_t)(count > 0 ? count : 1));
    return values;
}

static int ducknng_sql_arrow_value_at_depth(const struct ArrowSchema *schema,
    struct ArrowArrayView *view, idx_t index, int depth,
    duckdb_value *out_value, char **errmsg) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    duckdb_value value = NULL;
    if (out_value) *out_value = NULL;
    if (!schema || !view || !out_value) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing Arrow parameter value");
        return -1;
    }
    if (view->length < 0 || index > INT64_MAX || index >= (idx_t)view->length) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: Arrow parameter index is outside the decoded array");
        return -1;
    }
    if (depth > DUCKNNG_ARROW_MAX_NESTING_DEPTH) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: Arrow parameter nesting exceeds supported depth");
        return -1;
    }
    if (ArrowArrayViewIsNull(view, (int64_t)index)) {
        value = duckdb_create_null_value();
        if (!value) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to create NULL parameter value");
            return -1;
        }
        *out_value = value;
        return 0;
    }
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    if (ArrowSchemaViewInit(&schema_view, schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        return -1;
    }
    switch (schema_view.type) {
    case NANOARROW_TYPE_NA:
        value = duckdb_create_null_value();
        break;
    case NANOARROW_TYPE_BOOL:
        value = duckdb_create_bool(ArrowArrayViewGetIntUnsafe(view, index) != 0);
        break;
    case NANOARROW_TYPE_INT8:
        value = duckdb_create_int8((int8_t)ArrowArrayViewGetIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_INT16:
        value = duckdb_create_int16((int16_t)ArrowArrayViewGetIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_INT32:
        value = duckdb_create_int32((int32_t)ArrowArrayViewGetIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_INT64:
        value = duckdb_create_int64((int64_t)ArrowArrayViewGetIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_UINT8:
        value = duckdb_create_uint8((uint8_t)ArrowArrayViewGetUIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_UINT16:
        value = duckdb_create_uint16((uint16_t)ArrowArrayViewGetUIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_UINT32:
        value = duckdb_create_uint32((uint32_t)ArrowArrayViewGetUIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_UINT64:
        value = duckdb_create_uint64((uint64_t)ArrowArrayViewGetUIntUnsafe(view, index));
        break;
    case NANOARROW_TYPE_FLOAT:
        value = duckdb_create_float((float)ArrowArrayViewGetDoubleUnsafe(view, index));
        break;
    case NANOARROW_TYPE_DOUBLE:
        value = duckdb_create_double(ArrowArrayViewGetDoubleUnsafe(view, index));
        break;
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING: {
        struct ArrowStringView text = ArrowArrayViewGetStringUnsafe(view, index);
        value = duckdb_create_varchar_length(text.data, (idx_t)text.size_bytes);
        break;
    }
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
        struct ArrowBufferView bytes = ArrowArrayViewGetBytesUnsafe(view, index);
        value = duckdb_create_blob(bytes.data.data, (idx_t)bytes.size_bytes);
        break;
    }
    case NANOARROW_TYPE_DATE32: {
        duckdb_date date;
        date.days = (int32_t)ArrowArrayViewGetIntUnsafe(view, index);
        value = duckdb_create_date(date);
        break;
    }
    case NANOARROW_TYPE_DATE64: {
        duckdb_date date;
        date.days = (int32_t)ducknng_sql_arrow_floor_div_i64(
            ArrowArrayViewGetIntUnsafe(view, index), 86400000LL);
        value = duckdb_create_date(date);
        break;
    }
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64:
        if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
            duckdb_time_ns time_ns;
            time_ns.nanos = ArrowArrayViewGetIntUnsafe(view, index);
            value = duckdb_create_time_ns(time_ns);
        } else {
            duckdb_time time;
            int64_t mul = schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND ? 1000000LL :
                (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI ? 1000LL : 1LL);
            time.micros = ArrowArrayViewGetIntUnsafe(view, index) * mul;
            value = duckdb_create_time(time);
        }
        break;
    case NANOARROW_TYPE_TIMESTAMP: {
        int64_t raw = ArrowArrayViewGetIntUnsafe(view, index);
        if (schema_view.timezone && schema_view.timezone[0]) {
            duckdb_timestamp ts;
            int64_t mul = schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND ? 1000000LL :
                (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI ? 1000LL :
                (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO ? 0LL : 1LL));
            ts.micros = schema_view.time_unit == NANOARROW_TIME_UNIT_NANO ? raw / 1000LL : raw * mul;
            value = duckdb_create_timestamp_tz(ts);
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND) {
            duckdb_timestamp_s ts;
            ts.seconds = raw;
            value = duckdb_create_timestamp_s(ts);
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI) {
            duckdb_timestamp_ms ts;
            ts.millis = raw;
            value = duckdb_create_timestamp_ms(ts);
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
            duckdb_timestamp_ns ts;
            ts.nanos = raw;
            value = duckdb_create_timestamp_ns(ts);
        } else {
            duckdb_timestamp ts;
            ts.micros = raw;
            value = duckdb_create_timestamp(ts);
        }
        break;
    }
    case NANOARROW_TYPE_DECIMAL32:
    case NANOARROW_TYPE_DECIMAL64:
    case NANOARROW_TYPE_DECIMAL128: {
        struct ArrowDecimal decimal;
        duckdb_decimal out;
        ArrowDecimalInit(&decimal, schema_view.decimal_bitwidth,
            schema_view.decimal_precision, schema_view.decimal_scale);
        ArrowArrayViewGetDecimalUnsafe(view, index, &decimal);
        memset(&out, 0, sizeof(out));
        out.width = (uint8_t)schema_view.decimal_precision;
        out.scale = (uint8_t)schema_view.decimal_scale;
        out.value.lower = decimal.words[decimal.low_word_index];
        out.value.upper = (int64_t)decimal.words[decimal.high_word_index];
        value = duckdb_create_decimal(out);
        break;
    }
    case NANOARROW_TYPE_DURATION: {
        int64_t raw = ArrowArrayViewGetIntUnsafe(view, index);
        duckdb_interval interval;
        interval.months = 0;
        interval.days = 0;
        if (schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND) interval.micros = raw * 1000000LL;
        else if (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI) interval.micros = raw * 1000LL;
        else if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) interval.micros = raw / 1000LL;
        else interval.micros = raw;
        value = duckdb_create_interval(interval);
        break;
    }
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    case NANOARROW_TYPE_INTERVAL_DAY_TIME:
    case NANOARROW_TYPE_INTERVAL_MONTHS: {
        struct ArrowInterval arrow_interval;
        duckdb_interval interval;
        memset(&arrow_interval, 0, sizeof(arrow_interval));
        memset(&interval, 0, sizeof(interval));
        ArrowArrayViewGetIntervalUnsafe(view, (int64_t)index, &arrow_interval);
        if (schema_view.type == NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO) {
            interval.months = arrow_interval.months;
            interval.days = arrow_interval.days;
            interval.micros = arrow_interval.ns / 1000LL;
        } else if (schema_view.type == NANOARROW_TYPE_INTERVAL_DAY_TIME) {
            interval.days = arrow_interval.days;
            interval.micros = (int64_t)arrow_interval.ms * 1000LL;
        } else {
            interval.months = arrow_interval.months;
        }
        value = duckdb_create_interval(interval);
        break;
    }
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
        int64_t list_index;
        int64_t child_start;
        int64_t child_end;
        idx_t child_count;
        idx_t i;
        duckdb_value *children;
        duckdb_logical_type child_type = NULL;
        if (view->offset < 0 || (int64_t)index > INT64_MAX - view->offset) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: invalid Arrow list parameter range");
            return -1;
        }
        list_index = (int64_t)index + view->offset;
        if (schema_view.type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
            if (schema_view.fixed_size <= 0 || list_index >
                    INT64_MAX / schema_view.fixed_size) {
                if (errmsg) *errmsg = ducknng_strdup(
                    "ducknng: invalid Arrow fixed-size-list parameter range");
                return -1;
            }
            child_start = list_index * schema_view.fixed_size;
            if (child_start > INT64_MAX - schema_view.fixed_size) {
                if (errmsg) *errmsg = ducknng_strdup(
                    "ducknng: Arrow fixed-size-list parameter range overflows");
                return -1;
            }
            child_end = child_start + schema_view.fixed_size;
        } else {
            child_start = ArrowArrayViewListChildOffset(view, list_index);
            child_end = ArrowArrayViewListChildOffset(view, list_index + 1);
        }
        if (child_start < 0 || child_end < child_start) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid Arrow list parameter offsets");
            return -1;
        }
        child_count = (idx_t)(child_end - child_start);
        if (!view->children || !view->children[0] ||
            view->children[0]->length < 0 ||
            (uint64_t)child_start > (uint64_t)view->children[0]->length ||
            child_count > (idx_t)view->children[0]->length -
                (idx_t)child_start) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: Arrow list parameter child range exceeds the child array");
            return -1;
        }
        children = ducknng_sql_arrow_alloc_values(child_count);
        if (!children || ducknng_sql_arrow_schema_to_logical_type_depth(
                schema->children[0], &child_type, depth + 1, errmsg) != 0) {
            ducknng_sql_arrow_destroy_values(children, child_count);
            return -1;
        }
        for (i = 0; i < child_count; i++) {
            if (ducknng_sql_arrow_value_at_depth(schema->children[0], view->children[0],
                    (idx_t)child_start + i, depth + 1, &children[i], errmsg) != 0) {
                duckdb_destroy_logical_type(&child_type);
                ducknng_sql_arrow_destroy_values(children, child_count);
                return -1;
            }
        }
        value = schema_view.type == NANOARROW_TYPE_FIXED_SIZE_LIST
            ? duckdb_create_array_value(child_type, children, child_count)
            : duckdb_create_list_value(child_type, children, child_count);
        duckdb_destroy_logical_type(&child_type);
        ducknng_sql_arrow_destroy_values(children, child_count);
        break;
    }
    case NANOARROW_TYPE_STRUCT: {
        idx_t child_count = (idx_t)schema->n_children;
        idx_t i;
        duckdb_value *children = ducknng_sql_arrow_alloc_values(child_count);
        duckdb_logical_type struct_type = NULL;
        if (!children || ducknng_sql_arrow_schema_to_logical_type_depth(
                schema, &struct_type, depth + 1, errmsg) != 0) {
            ducknng_sql_arrow_destroy_values(children, child_count);
            return -1;
        }
        for (i = 0; i < child_count; i++) {
            if (ducknng_sql_arrow_value_at_depth(schema->children[i], view->children[i],
                    index, depth + 1, &children[i], errmsg) != 0) {
                duckdb_destroy_logical_type(&struct_type);
                ducknng_sql_arrow_destroy_values(children, child_count);
                return -1;
            }
        }
        value = duckdb_create_struct_value(struct_type, children);
        duckdb_destroy_logical_type(&struct_type);
        ducknng_sql_arrow_destroy_values(children, child_count);
        break;
    }
    case NANOARROW_TYPE_MAP: {
        int64_t list_index = (int64_t)index + view->offset;
        int64_t child_start = ArrowArrayViewListChildOffset(view, list_index);
        int64_t child_end = ArrowArrayViewListChildOffset(view, list_index + 1);
        idx_t entry_count;
        idx_t i;
        duckdb_value *keys;
        duckdb_value *values;
        duckdb_logical_type map_type = NULL;
        if (child_start < 0 || child_end < child_start || !schema->children ||
            !schema->children[0] || schema->children[0]->n_children != 2 ||
            !view->children || !view->children[0] || view->children[0]->n_children != 2) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid Arrow map parameter");
            return -1;
        }
        entry_count = (idx_t)(child_end - child_start);
        keys = ducknng_sql_arrow_alloc_values(entry_count);
        values = ducknng_sql_arrow_alloc_values(entry_count);
        if (!keys || !values || ducknng_sql_arrow_schema_to_logical_type_depth(
                schema, &map_type, depth + 1, errmsg) != 0) {
            ducknng_sql_arrow_destroy_values(keys, entry_count);
            ducknng_sql_arrow_destroy_values(values, entry_count);
            return -1;
        }
        for (i = 0; i < entry_count; i++) {
            if (ducknng_sql_arrow_value_at_depth(schema->children[0]->children[0],
                    view->children[0]->children[0], (idx_t)child_start + i,
                    depth + 1, &keys[i], errmsg) != 0 ||
                ducknng_sql_arrow_value_at_depth(schema->children[0]->children[1],
                    view->children[0]->children[1], (idx_t)child_start + i,
                    depth + 1, &values[i], errmsg) != 0) {
                duckdb_destroy_logical_type(&map_type);
                ducknng_sql_arrow_destroy_values(keys, entry_count);
                ducknng_sql_arrow_destroy_values(values, entry_count);
                return -1;
            }
        }
        value = duckdb_create_map_value(map_type, keys, values, entry_count);
        duckdb_destroy_logical_type(&map_type);
        ducknng_sql_arrow_destroy_values(keys, entry_count);
        ducknng_sql_arrow_destroy_values(values, entry_count);
        break;
    }
    case NANOARROW_TYPE_DENSE_UNION:
    case NANOARROW_TYPE_SPARSE_UNION: {
        int8_t child_index = ArrowArrayViewUnionChildIndex(view, (int64_t)index);
        int64_t child_offset = ArrowArrayViewUnionChildOffset(view, (int64_t)index);
        duckdb_value child_value = NULL;
        duckdb_logical_type union_type = NULL;
        if (child_index < 0 || child_index >= schema->n_children || child_offset < 0 ||
            ducknng_sql_arrow_schema_to_logical_type_depth(schema, &union_type,
                depth + 1, errmsg) != 0 ||
            ducknng_sql_arrow_value_at_depth(schema->children[child_index],
                view->children[child_index], (idx_t)child_offset, depth + 1,
                &child_value, errmsg) != 0) {
            if (union_type) duckdb_destroy_logical_type(&union_type);
            if (child_value) duckdb_destroy_value(&child_value);
            return -1;
        }
        value = duckdb_create_union_value(union_type, (idx_t)child_index, child_value);
        duckdb_destroy_logical_type(&union_type);
        duckdb_destroy_value(&child_value);
        break;
    }
    default:
        if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported Arrow type in SQL parameter tuple");
        return -1;
    }
    if (!value) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to construct SQL parameter value");
        return -1;
    }
    *out_value = value;
    return 0;
}

int ducknng_sql_arrow_value_at(const struct ArrowSchema *schema,
    struct ArrowArrayView *view, idx_t index, duckdb_value *out_value,
    char **errmsg) {
    return ducknng_sql_arrow_value_at_depth(schema, view, index, 0,
        out_value, errmsg);
}

int ducknng_sql_arrow_bind_result_columns(duckdb_bind_info info,
    const struct ArrowSchema *schema, char **errmsg) {
    idx_t col;
    if (!schema) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing Arrow schema");
        return -1;
    }
    if (schema->n_children < 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid Arrow schema");
        return -1;
    }
    for (col = 0; col < (idx_t)schema->n_children; col++) {
        duckdb_logical_type type = NULL;
        const char *name = schema->children[col] && schema->children[col]->name ?
            schema->children[col]->name : "";
        if (ducknng_sql_arrow_schema_to_logical_type(schema->children[col], &type, errmsg) != 0) {
            return -1;
        }
        duckdb_bind_add_result_column(info, name, type);
        duckdb_destroy_logical_type(&type);
    }
    return 0;
}

static int ducknng_sql_arrow_assign_column_at(duckdb_vector vec,
    struct ArrowArrayView *col_view, const struct ArrowSchema *col_schema,
    idx_t src_offset, idx_t dst_offset, idx_t count, int depth, char **errmsg) {
    struct ArrowSchemaView schema_view;
    struct ArrowError error;
    idx_t i;
    memset(&schema_view, 0, sizeof(schema_view));
    memset(&error, 0, sizeof(error));
    if (!vec || !col_view || !col_schema || col_view->length < 0 ||
        src_offset > INT64_MAX || src_offset > (idx_t)col_view->length ||
        count > (idx_t)col_view->length - src_offset) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: Arrow source range is outside the decoded array");
        return -1;
    }
    if (depth > DUCKNNG_ARROW_MAX_NESTING_DEPTH) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: Arrow value nesting exceeds supported depth");
        return -1;
    }
    if (ArrowSchemaViewInit(&schema_view, col_schema, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message);
        return -1;
    }
    switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL: {
        bool *out = (bool *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) != 0;
        }
        return 0;
    }
    case NANOARROW_TYPE_INT8: {
        int8_t *out = (int8_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (int8_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_INT16: {
        int16_t *out = (int16_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (int16_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_INT32: {
        int32_t *out = (int32_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (int32_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_INT64: {
        int64_t *out = (int64_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_UINT8: {
        uint8_t *out = (uint8_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (uint8_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_UINT16: {
        uint16_t *out = (uint16_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (uint16_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_UINT32: {
        uint32_t *out = (uint32_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (uint32_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_UINT64: {
        uint64_t *out = (uint64_t *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (uint64_t)ArrowArrayViewGetUIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_FLOAT: {
        float *out = (float *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = (float)ArrowArrayViewGetDoubleUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_DOUBLE: {
        double *out = (double *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst] = ArrowArrayViewGetDoubleUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_STRING: {
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            struct ArrowStringView value;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                value = ArrowArrayViewGetStringUnsafe(col_view, src_offset + i);
                /* Arrow IPC guarantees UTF-8 for STRING columns; skip internal validation. */
                duckdb_unsafe_vector_assign_string_element_len(vec, dst, value.data, (idx_t)value.size_bytes);
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_BINARY: {
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            struct ArrowBufferView value;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                value = ArrowArrayViewGetBytesUnsafe(col_view, src_offset + i);
                ducknng_sql_arrow_assign_blob(vec, dst, value.data.data, (idx_t)value.size_bytes);
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_DATE32: {
        duckdb_date *out = (duckdb_date *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst].days = (int32_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
        }
        return 0;
    }
    case NANOARROW_TYPE_DATE64: {
        duckdb_date *out = (duckdb_date *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else out[dst].days = (int32_t)ducknng_sql_arrow_floor_div_i64(
                ArrowArrayViewGetIntUnsafe(col_view, src_offset + i), 86400000LL);
        }
        return 0;
    }
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64: {
        if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
            duckdb_time_ns *out = (duckdb_time_ns *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
                else out[dst].nanos = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
        } else {
            duckdb_time *out = (duckdb_time *)duckdb_vector_get_data(vec);
            int64_t mul = schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND ? 1000000LL :
                (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI ? 1000LL : 1LL);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
                else out[dst].micros = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) * mul;
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_TIMESTAMP: {
        int64_t mul = schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND ? 1000000LL :
            (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI ? 1000LL : 1LL);
        if (schema_view.timezone && schema_view.timezone[0]) {
            duckdb_timestamp *out = (duckdb_timestamp *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
                else out[dst].micros = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) * mul;
            }
            return 0;
        }
        if (schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND) {
            duckdb_timestamp_s *out = (duckdb_timestamp_s *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
                else out[dst].seconds = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI) {
            duckdb_timestamp_ms *out = (duckdb_timestamp_ms *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
                else out[dst].millis = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
        } else if (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO) {
            duckdb_timestamp_ns *out = (duckdb_timestamp_ns *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
                else out[dst].nanos = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
            }
        } else {
            duckdb_timestamp *out = (duckdb_timestamp *)duckdb_vector_get_data(vec);
            for (i = 0; i < count; i++) {
                idx_t dst = dst_offset + i;
                if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
                else out[dst].micros = (int64_t)ArrowArrayViewGetIntUnsafe(col_view, src_offset + i) * mul;
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_DECIMAL32:
    case NANOARROW_TYPE_DECIMAL64:
    case NANOARROW_TYPE_DECIMAL128:
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else if (ducknng_sql_arrow_assign_decimal(vec, dst, &schema_view, col_view, src_offset + i) != 0) {
                return -1;
            }
        }
        return 0;
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_MAP: {
        duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(vec);
        duckdb_vector child_vec = duckdb_list_vector_get_child(vec);
        idx_t child_size = duckdb_list_vector_get_size(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            int64_t list_index = (int64_t)(src_offset + i) + col_view->offset;
            int64_t child_start = ArrowArrayViewListChildOffset(col_view, list_index);
            int64_t child_end = ArrowArrayViewListChildOffset(col_view, list_index + 1);
            idx_t child_len = child_end >= child_start ? (idx_t)(child_end - child_start) : 0;
            entries[dst].offset = child_size;
            entries[dst].length = 0;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) {
                ducknng_sql_arrow_set_null(vec, dst);
                continue;
            }
            if (duckdb_list_vector_reserve(vec, child_size + child_len) == DuckDBError ||
                duckdb_list_vector_set_size(vec, child_size + child_len) == DuckDBError) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to reserve DuckDB list/map child vector");
                return -1;
            }
            entries[dst].offset = child_size;
            entries[dst].length = child_len;
            if (child_len > 0 && ducknng_sql_arrow_assign_column_at(child_vec, col_view->children[0],
                    col_schema->children[0], (idx_t)child_start, child_size, child_len, depth + 1, errmsg) != 0) {
                return -1;
            }
            child_size += child_len;
        }
        return 0;
    }
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
        duckdb_vector child_vec = duckdb_array_vector_get_child(vec);
        idx_t fixed_size = (idx_t)schema_view.fixed_size;
        if (schema_view.fixed_size <= 0 || !child_vec || !col_view->children ||
            !col_view->children[0] || !col_schema->children ||
            !col_schema->children[0]) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: invalid fixed-size-list Arrow column");
            return -1;
        }
        for (i = 0; i < count; i++) {
            idx_t src = src_offset + i;
            idx_t dst = dst_offset + i;
            int64_t parent_src;
            idx_t child_src;
            idx_t child_dst = dst * fixed_size;
            idx_t child;
            if (src > INT64_MAX || col_view->offset < 0 ||
                (int64_t)src > INT64_MAX - col_view->offset ||
                (parent_src = (int64_t)src + col_view->offset) >
                    INT64_MAX / schema_view.fixed_size) {
                if (errmsg) *errmsg = ducknng_strdup(
                    "ducknng: invalid fixed-size-list Arrow source range");
                return -1;
            }
            child_src = (idx_t)(parent_src * schema_view.fixed_size);
            if (col_view->children[0]->length < 0 ||
                child_src > (idx_t)col_view->children[0]->length ||
                fixed_size > (idx_t)col_view->children[0]->length - child_src) {
                if (errmsg) *errmsg = ducknng_strdup(
                    "ducknng: fixed-size-list child range exceeds Arrow child array");
                return -1;
            }
            if (ArrowArrayViewIsNull(col_view, src)) {
                ducknng_sql_arrow_set_null(vec, dst);
                for (child = 0; child < fixed_size; child++) {
                    ducknng_sql_arrow_set_null(child_vec, child_dst + child);
                }
            } else if (ducknng_sql_arrow_assign_column_at(child_vec,
                    col_view->children[0], col_schema->children[0], child_src,
                    child_dst, fixed_size, depth + 1, errmsg) != 0) {
                return -1;
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_STRUCT: {
        idx_t nchildren = (idx_t)col_view->n_children;
        idx_t child;
        for (child = 0; child < nchildren; child++) {
            duckdb_vector child_vec = duckdb_struct_vector_get_child(vec, child);
            if (ducknng_sql_arrow_assign_column_at(child_vec, col_view->children[child],
                    col_schema->children[child], src_offset, dst_offset, count, depth + 1, errmsg) != 0) {
                return -1;
            }
        }
        for (i = 0; i < count; i++) {
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) {
                (void)ducknng_sql_arrow_set_nested_null(vec, col_schema, dst_offset + i, depth + 1);
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_DENSE_UNION:
    case NANOARROW_TYPE_SPARSE_UNION: {
        /*
         * DuckDB UNION is stored as a STRUCT: child[0] is the UTINYINT tag
         * (0-based member index), and child[k+1] holds member k values.
         * Arrow union buffers[0] carries type_ids; dense union buffers[1]
         * carries per-element child offsets; sparse union uses the parent index.
         * ArrowArrayViewUnionChildIndex maps type_id -> child index.
         * ArrowArrayViewUnionChildOffset returns the index into that child
         * (handles both dense and sparse transparently).
         */
        idx_t nchildren = (idx_t)col_view->n_children;
        duckdb_vector tag_vec = duckdb_struct_vector_get_child(vec, 0);
        uint8_t *tag_out = (uint8_t *)duckdb_vector_get_data(tag_vec);
        idx_t child;
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            int64_t src = (int64_t)(src_offset + i);
            if (ArrowArrayViewIsNull(col_view, src)) {
                ducknng_sql_arrow_set_null(vec, dst);
                for (child = 0; child < nchildren; child++)
                    ducknng_sql_arrow_set_null(duckdb_struct_vector_get_child(vec, child + 1), dst);
                continue;
            }
            int8_t child_index = ArrowArrayViewUnionChildIndex(col_view, src);
            int64_t child_offset = ArrowArrayViewUnionChildOffset(col_view, src);
            tag_out[dst] = (uint8_t)child_index;
            /* write the active member; null out all others */
            for (child = 0; child < nchildren; child++) {
                duckdb_vector member_vec = duckdb_struct_vector_get_child(vec, child + 1);
                if ((int8_t)child == child_index) {
                    if (ducknng_sql_arrow_assign_column_at(member_vec,
                            col_view->children[child_index],
                            col_schema->children[child_index],
                            (idx_t)child_offset, dst, 1, depth + 1, errmsg) != 0)
                        return -1;
                } else {
                    ducknng_sql_arrow_set_null(member_vec, dst);
                }
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_LARGE_STRING: {
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            struct ArrowStringView value;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                value = ArrowArrayViewGetStringUnsafe(col_view, src_offset + i);
                /* Arrow IPC guarantees UTF-8 for LARGE_STRING columns; skip internal validation. */
                duckdb_unsafe_vector_assign_string_element_len(vec, dst, value.data, (idx_t)value.size_bytes);
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            struct ArrowBufferView value;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                value = ArrowArrayViewGetBytesUnsafe(col_view, src_offset + i);
                ducknng_sql_arrow_assign_blob(vec, dst, value.data.data, (idx_t)value.size_bytes);
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_DURATION: {
        duckdb_interval *out = (duckdb_interval *)duckdb_vector_get_data(vec);
        int64_t to_micros = schema_view.time_unit == NANOARROW_TIME_UNIT_SECOND ? 1000000LL :
            (schema_view.time_unit == NANOARROW_TIME_UNIT_MILLI ? 1000LL :
            (schema_view.time_unit == NANOARROW_TIME_UNIT_NANO  ? 0LL   : 1LL));
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                int64_t raw = ArrowArrayViewGetIntUnsafe(col_view, src_offset + i);
                out[dst].months = 0;
                out[dst].days   = 0;
                /* NANO: truncate to micros (1 ns = 0.001 us) */
                out[dst].micros = schema_view.time_unit == NANOARROW_TIME_UNIT_NANO
                    ? raw / 1000LL
                    : raw * to_micros;
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO: {
        duckdb_interval *out = (duckdb_interval *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                struct ArrowInterval iv;
                ArrowArrayViewGetIntervalUnsafe(col_view, (int64_t)(src_offset + i), &iv);
                out[dst].months = iv.months;
                out[dst].days   = iv.days;
                out[dst].micros = iv.ns / 1000LL;
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_INTERVAL_DAY_TIME: {
        duckdb_interval *out = (duckdb_interval *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                struct ArrowInterval iv;
                ArrowArrayViewGetIntervalUnsafe(col_view, (int64_t)(src_offset + i), &iv);
                out[dst].months = 0;
                out[dst].days   = iv.days;
                out[dst].micros = (int64_t)iv.ms * 1000LL;
            }
        }
        return 0;
    }
    case NANOARROW_TYPE_INTERVAL_MONTHS: {
        duckdb_interval *out = (duckdb_interval *)duckdb_vector_get_data(vec);
        for (i = 0; i < count; i++) {
            idx_t dst = dst_offset + i;
            if (ArrowArrayViewIsNull(col_view, src_offset + i)) ducknng_sql_arrow_set_null(vec, dst);
            else {
                struct ArrowInterval iv;
                ArrowArrayViewGetIntervalUnsafe(col_view, (int64_t)(src_offset + i), &iv);
                out[dst].months = iv.months;
                out[dst].days   = 0;
                out[dst].micros = 0;
            }
        }
        return 0;
    }
    default:
        if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported Arrow type in remote row reply");
        return -1;
    }
}

int ducknng_sql_arrow_assign_column(duckdb_vector vec, struct ArrowArrayView *col_view,
    const struct ArrowSchema *col_schema, idx_t src_offset, idx_t count, char **errmsg) {
    return ducknng_sql_arrow_assign_column_at(vec, col_view, col_schema, src_offset, 0, count, 0, errmsg);
}

int ducknng_sql_arrow_scan_table(duckdb_data_chunk output, const struct ArrowSchema *schema,
    const struct ArrowArray *array, idx_t row_count, idx_t *offset, char **errmsg) {
    struct ArrowArrayView view;
    struct ArrowError error;
    idx_t remaining;
    idx_t chunk_size;
    idx_t col;
    if (errmsg) *errmsg = NULL;
    if (!output || !schema || !array || !offset || *offset >= row_count) {
        if (output) duckdb_data_chunk_set_size(output, 0);
        return 0;
    }
    memset(&view, 0, sizeof(view));
    memset(&error, 0, sizeof(error));
    if (ArrowArrayViewInitFromSchema(&view, schema, &error) != NANOARROW_OK ||
        ArrowArrayViewSetArray(&view, array, &error) != NANOARROW_OK) {
        if (errmsg) *errmsg = ducknng_strdup(error.message[0] ? error.message :
            "ducknng: failed to view Arrow row payload");
        ArrowArrayViewReset(&view);
        return -1;
    }
    remaining = row_count - *offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (col = 0; col < (idx_t)schema->n_children; col++) {
        duckdb_vector vec = duckdb_data_chunk_get_vector(output, col);
        if (ducknng_sql_arrow_assign_column(vec, view.children[col], schema->children[col],
                *offset, chunk_size, errmsg) != 0) {
            ArrowArrayViewReset(&view);
            return -1;
        }
    }
    *offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
    ArrowArrayViewReset(&view);
    return 0;
}
