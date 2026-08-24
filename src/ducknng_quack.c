#include "ducknng_quack.h"
#include "ducknng_duckdb_streaming_compat.h"
#include "ducknng_quack_core.h"
#include "ducknng_util.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

#define DUCKNNG_QUACK_FIELD_END 0xffffu
#define DUCKNNG_QUACK_OUTER_RESULT_TYPES 1u
#define DUCKNNG_QUACK_OUTER_RESULT_NAMES 2u
#define DUCKNNG_QUACK_OUTER_RESULTS 4u
#define DUCKNNG_QUACK_TYPE_ID 100u
#define DUCKNNG_QUACK_TYPE_INFO 101u
#define DUCKNNG_QUACK_EXTRA_INFO_KIND 100u
#define DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH 200u
#define DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE 201u
#define DUCKNNG_QUACK_CHUNK_WRAPPER 300u
#define DUCKNNG_QUACK_CHUNK_ROWS 100u
#define DUCKNNG_QUACK_CHUNK_TYPES 101u
#define DUCKNNG_QUACK_CHUNK_COLUMNS 102u
#define DUCKNNG_QUACK_VECTOR_TYPE 90u
#define DUCKNNG_QUACK_VECTOR_SELECTION 91u
#define DUCKNNG_QUACK_VECTOR_DICTIONARY_COUNT 92u
#define DUCKNNG_QUACK_VECTOR_SEQUENCE_START 91u
#define DUCKNNG_QUACK_VECTOR_SEQUENCE_INCREMENT 92u
#define DUCKNNG_QUACK_VECTOR_HAS_VALIDITY 100u
#define DUCKNNG_QUACK_VECTOR_VALIDITY 101u
#define DUCKNNG_QUACK_VECTOR_DATA 102u
#define DUCKNNG_QUACK_VECTOR_FLAT 0u
#define DUCKNNG_QUACK_VECTOR_FSST 1u
#define DUCKNNG_QUACK_VECTOR_CONSTANT 2u
#define DUCKNNG_QUACK_VECTOR_DICTIONARY 3u
#define DUCKNNG_QUACK_VECTOR_SEQUENCE 4u

/* ExtraTypeInfo / nested vector field ids and bounds. */
#define DUCKNNG_QUACK_EXTRA_CHILD 200
#define DUCKNNG_QUACK_EXTRA_ARRAY_SIZE 201
#define DUCKNNG_QUACK_EXTRA_STRUCT_COUNT 200
#define DUCKNNG_QUACK_EXTRA_ENUM_COUNT 200
#define DUCKNNG_QUACK_EXTRA_ENUM_VALUES 201
#define DUCKNNG_QUACK_PAIR_FIRST 0
#define DUCKNNG_QUACK_PAIR_SECOND 1
#define DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN 103
#define DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE 104
#define DUCKNNG_QUACK_VECTOR_ARRAY_SIZE 103
#define DUCKNNG_QUACK_VECTOR_LIST_ENTRIES 105
#define DUCKNNG_QUACK_VECTOR_LIST_CHILD 106
#define DUCKNNG_QUACK_VECTOR_ARRAY_CHILD 104
#define DUCKNNG_QUACK_ENTRY_OFFSET 100
#define DUCKNNG_QUACK_ENTRY_LENGTH 101

typedef ducknng_qk_writer ducknng_quack_writer;

typedef ducknng_qk_reader ducknng_quack_reader;

typedef struct ducknng_quack_type_meta {
    int logical_type_id;
    uint8_t decimal_width;
    uint8_t decimal_scale;
} ducknng_quack_type_meta;

/* Bytes a DuckDB validity mask occupies on the wire: ceil(rows/64) uint64 words.
 * SIZE_MAX means the count cannot be represented safely. */
static size_t ducknng_quack_validity_bytes(idx_t rows) {
    return ducknng_qk_validity_bytes((uint64_t)rows);
}

/* Read a validity bit byte-wise so the wire blob need not be 8-byte aligned and
 * the result is endianness-independent. On little-endian this is bit-identical
 * to indexing the mask as uint64 words (bit row%64 of word row/64). */
static int ducknng_quack_vector_row_is_valid(const uint8_t *validity, idx_t row) {
    if (!validity) return 1;
    return (validity[row >> 3] >> (row & 7u)) & 1u;
}

static void ducknng_quack_set_error(char **errmsg, const char *message) {
    if (!errmsg || *errmsg) return;
    *errmsg = ducknng_strdup(message ? message : "ducknng: quack codec error");
}

static int ducknng_quack_writer_result(int rc, ducknng_quack_writer *writer,
    char **errmsg) {
    if (rc == 0) return 0;
    switch (ducknng_qk_writer_status(writer)) {
    case DUCKNNG_QK_OVERFLOW:
        ducknng_quack_set_error(errmsg, "ducknng: quack payload size overflow");
        break;
    case DUCKNNG_QK_NO_SPACE:
        ducknng_quack_set_error(errmsg, "ducknng: measured quack output buffer is too small");
        break;
    default:
        ducknng_quack_set_error(errmsg, "ducknng: invalid quack encoder input");
        break;
    }
    return -1;
}

static int ducknng_quack_write_byte(ducknng_quack_writer *w,
    uint8_t value, char **errmsg) {
    return ducknng_quack_writer_result(
        ducknng_qk_write_u8(w, value), w, errmsg);
}

static int ducknng_quack_write_field_id(ducknng_quack_writer *w,
    uint16_t field_id, char **errmsg) {
    return ducknng_quack_writer_result(
        ducknng_qk_write_field(w, field_id), w, errmsg);
}

static int ducknng_quack_write_field_end(ducknng_quack_writer *w,
    char **errmsg) {
    return ducknng_quack_write_field_id(w, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_write_uleb128(ducknng_quack_writer *w,
    uint64_t value, char **errmsg) {
    return ducknng_quack_writer_result(
        ducknng_qk_write_uleb128(w, value), w, errmsg);
}

static int ducknng_quack_write_sleb128(ducknng_quack_writer *w,
    int64_t value, char **errmsg) {
    return ducknng_quack_writer_result(
        ducknng_qk_write_sleb128(w, value), w, errmsg);
}

static int ducknng_quack_write_string_len(ducknng_quack_writer *w,
    const uint8_t *data, size_t len, char **errmsg) {
    return ducknng_quack_writer_result(
        ducknng_qk_write_counted(w, data, len), w, errmsg);
}

static int ducknng_quack_write_string(ducknng_quack_writer *w,
    const char *text, char **errmsg) {
    size_t len = text ? strlen(text) : 0;
    return ducknng_quack_write_string_len(w,
        (const uint8_t *)(text ? text : ""), len, errmsg);
}

static int ducknng_quack_write_blob(ducknng_quack_writer *w,
    const uint8_t *data, size_t len, char **errmsg) {
    return ducknng_quack_writer_result(
        ducknng_qk_write_counted(w, data, len), w, errmsg);
}

static int ducknng_quack_reader_result(int rc,
    const ducknng_quack_reader *reader, const char *invalid_message,
    char **errmsg) {
    if (rc == 0) return 0;
    switch (ducknng_qk_reader_status(reader)) {
    case DUCKNNG_QK_TRUNCATED:
        ducknng_quack_set_error(errmsg, "ducknng: truncated quack payload");
        break;
    case DUCKNNG_QK_OVERFLOW:
        ducknng_quack_set_error(errmsg,
            "ducknng: quack integer or byte length exceeds supported size");
        break;
    default:
        ducknng_quack_set_error(errmsg, invalid_message ? invalid_message :
            "ducknng: invalid quack payload");
        break;
    }
    return -1;
}

static int ducknng_quack_uint64_to_idx(uint64_t value, idx_t *out,
    const char *message, char **errmsg) {
    idx_t converted = (idx_t)value;
    if ((uint64_t)converted != value) {
        ducknng_quack_set_error(errmsg, message ? message : "ducknng: quack integer exceeds supported size");
        return -1;
    }
    if (out) *out = converted;
    return 0;
}

static int ducknng_quack_size_to_idx(size_t value, idx_t *out,
    const char *message, char **errmsg) {
    return ducknng_quack_uint64_to_idx((uint64_t)value, out, message, errmsg);
}

/* idx_t row/element math keeps its own entry points because it attaches a
 * caller-supplied error message and narrows to idx_t, neither of which the
 * plain checked helpers do. The overflow test itself is not restated here: it
 * comes from ducknng_checked at the matching 64-bit width, so a fix there
 * reaches this path too. */
static int ducknng_quack_idx_narrow(uint64_t value, idx_t *out,
    const char *message, char **errmsg) {
    if ((uint64_t)(idx_t)value != value) {
        ducknng_quack_set_error(errmsg, message ? message :
            "ducknng: quack row count overflows supported size");
        return -1;
    }
    if (out) *out = (idx_t)value;
    return 0;
}

static int ducknng_quack_idx_add(idx_t a, idx_t b, idx_t *out,
    const char *message, char **errmsg) {
    uint64_t sum;
    if (ducknng_u64_add((uint64_t)a, (uint64_t)b, &sum) != 0) {
        ducknng_quack_set_error(errmsg, message ? message :
            "ducknng: quack row count overflows supported size");
        return -1;
    }
    return ducknng_quack_idx_narrow(sum, out, message, errmsg);
}

static int ducknng_quack_idx_mul(idx_t a, idx_t b, idx_t *out,
    const char *message, char **errmsg) {
    uint64_t product;
    if (ducknng_u64_mul((uint64_t)a, (uint64_t)b, &product) != 0) {
        ducknng_quack_set_error(errmsg, message ? message :
            "ducknng: quack row count overflows supported size");
        return -1;
    }
    return ducknng_quack_idx_narrow(product, out, message, errmsg);
}

static int ducknng_quack_read_boolean(ducknng_quack_reader *r, uint8_t *out,
    const char *message, char **errmsg) {
    return ducknng_quack_reader_result(ducknng_qk_read_boolean(r, out), r,
        message ? message : "ducknng: quack boolean field is not 0 or 1",
        errmsg);
}

static int ducknng_quack_peek_u16le(ducknng_quack_reader *r,
    uint16_t *out, char **errmsg) {
    return ducknng_quack_reader_result(ducknng_qk_peek_u16le(r, out), r,
        NULL, errmsg);
}

static int ducknng_quack_read_u16le(ducknng_quack_reader *r,
    uint16_t *out, char **errmsg) {
    return ducknng_quack_reader_result(ducknng_qk_read_u16le(r, out), r,
        NULL, errmsg);
}

static int ducknng_quack_read_expect_field(ducknng_quack_reader *r, uint16_t expected, char **errmsg) {
    uint16_t field_id = 0;
    if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id != expected) {
        char buf[128];
        snprintf(buf, sizeof(buf), "ducknng: unexpected quack field id %u (expected %u)",
            (unsigned int)field_id, (unsigned int)expected);
        ducknng_quack_set_error(errmsg, buf);
        return -1;
    }
    return 0;
}

static int ducknng_quack_read_uleb128(ducknng_quack_reader *r,
    uint64_t *out, char **errmsg) {
    return ducknng_quack_reader_result(ducknng_qk_read_uleb128(r, out), r,
        "ducknng: invalid quack uleb128 integer", errmsg);
}

static int ducknng_quack_read_sleb128(ducknng_quack_reader *r,
    int64_t *out, char **errmsg) {
    return ducknng_quack_reader_result(ducknng_qk_read_sleb128(r, out), r,
        "ducknng: invalid quack sleb128 integer", errmsg);
}

static int ducknng_quack_read_blob_view(ducknng_quack_reader *r,
    const uint8_t **out_data, size_t *out_len, char **errmsg) {
    return ducknng_quack_reader_result(
        ducknng_qk_read_counted(r, out_data, out_len), r,
        "ducknng: invalid quack counted byte field", errmsg);
}

static int ducknng_quack_read_string_dup(ducknng_quack_reader *r, char **out, char **errmsg) {
    const uint8_t *data = NULL;
    size_t len = 0;
    char *dup;
    if (out) *out = NULL;
    if (ducknng_quack_read_blob_view(r, &data, &len, errmsg) != 0) return -1;
    dup = (char *)duckdb_malloc(len + 1);
    if (!dup) {
        ducknng_quack_set_error(errmsg, "ducknng: out of memory copying quack string");
        return -1;
    }
    if (len) memcpy(dup, data, len);
    dup[len] = '\0';
    /* This reader backs only identifier-like fields (column names, struct/union
     * child names, enum labels) that are later compared with strcmp. An
     * embedded NUL would make a counted name like "a\0x" compare equal to "a",
     * bypassing the exact column-name match on the upload path (and corrupting
     * nested-type equality). Reject it here: our encoder never emits NUL in
     * these fields, so this only rejects malformed/hostile payloads. String
     * and blob *data values* go through read_blob_view and keep their NULs. */
    if (strlen(dup) != len) {
        duckdb_free(dup);
        ducknng_quack_set_error(errmsg, "ducknng: quack identifier contains an embedded NUL");
        return -1;
    }
    if (out) *out = dup;
    else duckdb_free(dup);
    return 0;
}

static int ducknng_quack_skip_string(ducknng_quack_reader *r, char **errmsg) {
    return ducknng_quack_read_blob_view(r, NULL, NULL, errmsg);
}

static int ducknng_quack_duckdb_type_to_meta(duckdb_logical_type type,
    ducknng_quack_type_meta *out, char **errmsg) {
    duckdb_type id;
    if (!type || !out) {
        ducknng_quack_set_error(errmsg, "ducknng: missing DuckDB logical type for quack encoding");
        return -1;
    }
    memset(out, 0, sizeof(*out));
    id = duckdb_get_type_id(type);
    switch (id) {
    case DUCKDB_TYPE_BOOLEAN: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_BOOLEAN; return 0;
    case DUCKDB_TYPE_TINYINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TINYINT; return 0;
    case DUCKDB_TYPE_SMALLINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_SMALLINT; return 0;
    case DUCKDB_TYPE_INTEGER: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_INTEGER; return 0;
    case DUCKDB_TYPE_BIGINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_BIGINT; return 0;
    case DUCKDB_TYPE_DATE: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_DATE; return 0;
    case DUCKDB_TYPE_TIME: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIME; return 0;
    case DUCKDB_TYPE_TIMESTAMP_S: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC; return 0;
    case DUCKDB_TYPE_TIMESTAMP_MS: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS; return 0;
    case DUCKDB_TYPE_TIMESTAMP: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP; return 0;
    case DUCKDB_TYPE_TIMESTAMP_NS: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS; return 0;
    case DUCKDB_TYPE_DECIMAL:
        out->logical_type_id = DUCKNNG_QUACK_LOGICAL_DECIMAL;
        out->decimal_width = duckdb_decimal_width(type);
        out->decimal_scale = duckdb_decimal_scale(type);
        return 0;
    case DUCKDB_TYPE_FLOAT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_FLOAT; return 0;
    case DUCKDB_TYPE_DOUBLE: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_DOUBLE; return 0;
    case DUCKDB_TYPE_VARCHAR: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_VARCHAR; return 0;
    case DUCKDB_TYPE_BLOB: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_BLOB; return 0;
    case DUCKDB_TYPE_INTERVAL: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_INTERVAL; return 0;
    case DUCKDB_TYPE_UTINYINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UTINYINT; return 0;
    case DUCKDB_TYPE_USMALLINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_USMALLINT; return 0;
    case DUCKDB_TYPE_UINTEGER: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UINTEGER; return 0;
    case DUCKDB_TYPE_UBIGINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UBIGINT; return 0;
    case DUCKDB_TYPE_TIMESTAMP_TZ: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ; return 0;
    case DUCKDB_TYPE_TIME_TZ: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIME_TZ; return 0;
    case DUCKDB_TYPE_TIME_NS: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_TIME_NS; return 0;
    case DUCKDB_TYPE_UHUGEINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UHUGEINT; return 0;
    case DUCKDB_TYPE_HUGEINT: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_HUGEINT; return 0;
    case DUCKDB_TYPE_UUID: out->logical_type_id = DUCKNNG_QUACK_LOGICAL_UUID; return 0;
    default:
        ducknng_quack_set_error(errmsg,
            "ducknng: ducknng_quack_batch currently supports scalar bool/integer/float/date/time/timestamp/decimal/varchar/blob/interval/hugeint/uuid columns only");
        return -1;
    }
}

static int ducknng_quack_meta_to_duckdb_type(const ducknng_quack_type_meta *meta,
    duckdb_logical_type *out_type, char **errmsg) {
    if (out_type) *out_type = NULL;
    if (!meta || !out_type) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack logical type metadata");
        return -1;
    }
    switch (meta->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_BOOLEAN: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN); break;
    case DUCKNNG_QUACK_LOGICAL_TINYINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TINYINT); break;
    case DUCKNNG_QUACK_LOGICAL_SMALLINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT); break;
    case DUCKNNG_QUACK_LOGICAL_INTEGER: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER); break;
    case DUCKNNG_QUACK_LOGICAL_BIGINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT); break;
    case DUCKNNG_QUACK_LOGICAL_DATE: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DATE); break;
    case DUCKNNG_QUACK_LOGICAL_TIME: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_S); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_MS); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_NS); break;
    case DUCKNNG_QUACK_LOGICAL_DECIMAL:
        *out_type = duckdb_create_decimal_type(meta->decimal_width, meta->decimal_scale);
        break;
    case DUCKNNG_QUACK_LOGICAL_FLOAT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_FLOAT); break;
    case DUCKNNG_QUACK_LOGICAL_DOUBLE: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE); break;
    case DUCKNNG_QUACK_LOGICAL_CHAR:
    case DUCKNNG_QUACK_LOGICAL_VARCHAR: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR); break;
    case DUCKNNG_QUACK_LOGICAL_BLOB: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB); break;
    case DUCKNNG_QUACK_LOGICAL_INTERVAL: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_INTERVAL); break;
    case DUCKNNG_QUACK_LOGICAL_UTINYINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UTINYINT); break;
    case DUCKNNG_QUACK_LOGICAL_USMALLINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_USMALLINT); break;
    case DUCKNNG_QUACK_LOGICAL_UINTEGER: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UINTEGER); break;
    case DUCKNNG_QUACK_LOGICAL_UBIGINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT); break;
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIMESTAMP_TZ); break;
    case DUCKNNG_QUACK_LOGICAL_TIME_TZ: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME_TZ); break;
    case DUCKNNG_QUACK_LOGICAL_TIME_NS: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_TIME_NS); break;
    case DUCKNNG_QUACK_LOGICAL_UHUGEINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UHUGEINT); break;
    case DUCKNNG_QUACK_LOGICAL_HUGEINT: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_HUGEINT); break;
    case DUCKNNG_QUACK_LOGICAL_UUID: *out_type = duckdb_create_logical_type(DUCKDB_TYPE_UUID); break;
    default:
        ducknng_quack_set_error(errmsg,
            "ducknng: ducknng_quack_batch decode encountered an unsupported logical type");
        return -1;
    }
    if (!*out_type) {
        ducknng_quack_set_error(errmsg, "ducknng: failed to allocate DuckDB logical type for quack payload");
        return -1;
    }
    return 0;
}

/* ===== Nested type-node codec (recursive; ported from Rducks quack_core.c) ===== */

static void ducknng_quack_node_free(ducknng_quack_column_schema *node);
static int ducknng_quack_node_from_duckdb(duckdb_logical_type type, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg);
static int ducknng_quack_node_to_duckdb(const ducknng_quack_column_schema *node, unsigned depth,
    duckdb_logical_type *out_type, char **errmsg);
static int ducknng_quack_write_type_node(ducknng_quack_writer *w,
    const ducknng_quack_column_schema *node, unsigned depth, char **errmsg);
static int ducknng_quack_read_type_node(ducknng_quack_reader *r, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg);

static ducknng_quack_column_schema *ducknng_quack_node_new(int logical_type_id) {
    ducknng_quack_column_schema *n = (ducknng_quack_column_schema *)duckdb_malloc(sizeof(*n));
    if (!n) return NULL;
    memset(n, 0, sizeof(*n));
    n->logical_type_id = logical_type_id;
    return n;
}

/* Append a child node (transfers ownership of `child`) with a member name. */
static int ducknng_quack_node_add_child(ducknng_quack_column_schema *node,
    ducknng_quack_column_schema *child, const char *name) {
    ducknng_quack_column_schema **children;
    char **names;
    char *name_dup;
    uint32_t n;
    if (!node || !child) return -1;
    n = node->nchildren;
    children = (ducknng_quack_column_schema **)duckdb_malloc(sizeof(*children) * ((size_t)n + 1));
    if (!children) return -1;
    names = (char **)duckdb_malloc(sizeof(*names) * ((size_t)n + 1));
    if (!names) { duckdb_free(children); return -1; }
    name_dup = ducknng_strdup(name ? name : "");
    if (!name_dup) { duckdb_free(children); duckdb_free(names); return -1; }
    if (node->children && n) memcpy(children, node->children, sizeof(*children) * n);
    if (node->child_names && n) memcpy(names, node->child_names, sizeof(*names) * n);
    children[n] = child;
    names[n] = name_dup;
    if (node->children) duckdb_free(node->children);
    if (node->child_names) duckdb_free(node->child_names);
    node->children = children;
    node->child_names = names;
    node->nchildren = n + 1;
    return 0;
}

static int ducknng_quack_node_is_nested(
    const ducknng_quack_column_schema *node) {
    return ducknng_qk_type_is_nested(node);
}

static int ducknng_quack_node_is_varlen(
    const ducknng_quack_column_schema *node) {
    return ducknng_qk_type_is_varlen(node);
}

static size_t ducknng_quack_node_fixed_width(
    const ducknng_quack_column_schema *node) {
    return ducknng_qk_type_fixed_width(node);
}

static int ducknng_quack_node_is_sequence_integer(
    const ducknng_quack_column_schema *node) {
    return ducknng_qk_type_is_sequence_integer(node);
}

static int ducknng_quack_node_type_equal(
    const ducknng_quack_column_schema *a,
    const ducknng_quack_column_schema *b) {
    return ducknng_qk_type_equal(a, b);
}

static int ducknng_quack_write_type_meta(ducknng_quack_writer *w,
    const ducknng_quack_type_meta *meta, char **errmsg) {
    if (!w || !meta) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack type metadata");
        return -1;
    }
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_ID, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)meta->logical_type_id, errmsg) != 0) return -1;
    if (meta->logical_type_id == DUCKNNG_QUACK_LOGICAL_DECIMAL) {
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_INFO, errmsg) != 0 ||
            ducknng_quack_write_byte(w, 1, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_INFO_KIND, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, meta->decimal_width, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, meta->decimal_scale, errmsg) != 0 ||
            ducknng_quack_write_field_end(w, errmsg) != 0) return -1;
    }
    return ducknng_quack_write_field_end(w, errmsg);
}

static int ducknng_quack_read_type_meta(ducknng_quack_reader *r,
    ducknng_quack_type_meta *out, char **errmsg) {
    uint16_t field_id = 0;
    uint64_t logical_id = 0;
    if (!r || !out) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack logical type decoder state");
        return -1;
    }
    memset(out, 0, sizeof(*out));
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_TYPE_ID, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &logical_id, errmsg) != 0) return -1;
    out->logical_type_id = (int)logical_id;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_TYPE_INFO) {
        uint8_t present = 0;
        uint64_t kind = 0;
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_boolean(r, &present,
                "ducknng: quack type-info presence flag is not boolean",
                errmsg) != 0) return -1;
        if (present) {
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_EXTRA_INFO_KIND, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &kind, errmsg) != 0) return -1;
            if (kind == DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL) {
                uint64_t width = 0;
                uint64_t scale = 0;
                if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH, errmsg) != 0 ||
                    ducknng_quack_read_uleb128(r, &width, errmsg) != 0 ||
                    ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE, errmsg) != 0 ||
                    ducknng_quack_read_uleb128(r, &scale, errmsg) != 0) return -1;
                out->decimal_width = (uint8_t)width;
                out->decimal_scale = (uint8_t)scale;
            }
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
        }
    }
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_skip_type_meta(ducknng_quack_reader *r, char **errmsg) {
    ducknng_quack_column_schema *node = NULL;
    if (ducknng_quack_read_type_node(r, 0, &node, errmsg) != 0) return -1;
    ducknng_quack_node_free(node);
    return 0;
}

static int ducknng_quack_skip_string_list(ducknng_quack_reader *r, uint64_t expected, char **errmsg) {
    uint64_t n = 0;
    uint64_t i;
    if (ducknng_quack_read_uleb128(r, &n, errmsg) != 0) return -1;
    if (expected != UINT64_MAX && n != expected) {
        ducknng_quack_set_error(errmsg, "ducknng: quack string list length mismatch");
        return -1;
    }
    for (i = 0; i < n; i++) if (ducknng_quack_skip_string(r, errmsg) != 0) return -1;
    return 0;
}

static int ducknng_quack_skip_type_list(ducknng_quack_reader *r, uint64_t expected, char **errmsg) {
    uint64_t n = 0;
    uint64_t i;
    if (ducknng_quack_read_uleb128(r, &n, errmsg) != 0) return -1;
    if (expected != UINT64_MAX && n != expected) {
        ducknng_quack_set_error(errmsg, "ducknng: quack logical type list length mismatch");
        return -1;
    }
    for (i = 0; i < n; i++) if (ducknng_quack_skip_type_meta(r, errmsg) != 0) return -1;
    return 0;
}

static int ducknng_quack_node_from_duckdb(duckdb_logical_type type, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg) {
    duckdb_type id;
    ducknng_quack_column_schema *node = NULL;
    if (out) *out = NULL;
    if (!type) {
        ducknng_quack_set_error(errmsg, "ducknng: missing DuckDB logical type for quack encoding");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    id = duckdb_get_type_id(type);
    switch (id) {
    case DUCKDB_TYPE_LIST:
    case DUCKDB_TYPE_ARRAY: {
        duckdb_logical_type child_t = (id == DUCKDB_TYPE_LIST)
            ? duckdb_list_type_child_type(type) : duckdb_array_type_child_type(type);
        ducknng_quack_column_schema *child = NULL;
        int rc;
        node = ducknng_quack_node_new(id == DUCKDB_TYPE_LIST
            ? DUCKNNG_QUACK_LOGICAL_LIST : DUCKNNG_QUACK_LOGICAL_ARRAY);
        if (!node) { if (child_t) duckdb_destroy_logical_type(&child_t); goto oom; }
        if (id == DUCKDB_TYPE_ARRAY) node->array_size = (uint32_t)duckdb_array_type_array_size(type);
        rc = ducknng_quack_node_from_duckdb(child_t, depth + 1, &child, errmsg);
        if (child_t) duckdb_destroy_logical_type(&child_t);
        if (rc != 0) { ducknng_quack_node_free(node); return -1; }
        if (ducknng_quack_node_add_child(node, child, "") != 0) {
            ducknng_quack_node_free(child); ducknng_quack_node_free(node); goto oom;
        }
        break;
    }
    case DUCKDB_TYPE_STRUCT:
    case DUCKDB_TYPE_UNION: {
        idx_t count = (id == DUCKDB_TYPE_STRUCT)
            ? duckdb_struct_type_child_count(type) : duckdb_union_type_member_count(type);
        idx_t i;
        node = ducknng_quack_node_new(id == DUCKDB_TYPE_STRUCT
            ? DUCKNNG_QUACK_LOGICAL_STRUCT : DUCKNNG_QUACK_LOGICAL_UNION);
        if (!node) goto oom;
        if (id == DUCKDB_TYPE_UNION) {
            /* DuckDB UNION physical layout: leading UTINYINT tag (name "") then members. */
            ducknng_quack_column_schema *tag = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_UTINYINT);
            if (!tag) { ducknng_quack_node_free(node); goto oom; }
            if (ducknng_quack_node_add_child(node, tag, "") != 0) {
                ducknng_quack_node_free(tag); ducknng_quack_node_free(node); goto oom;
            }
        }
        for (i = 0; i < count; i++) {
            duckdb_logical_type child_t = (id == DUCKDB_TYPE_STRUCT)
                ? duckdb_struct_type_child_type(type, i) : duckdb_union_type_member_type(type, i);
            char *child_name = (id == DUCKDB_TYPE_STRUCT)
                ? duckdb_struct_type_child_name(type, i) : duckdb_union_type_member_name(type, i);
            ducknng_quack_column_schema *child = NULL;
            int rc = ducknng_quack_node_from_duckdb(child_t, depth + 1, &child, errmsg);
            if (child_t) duckdb_destroy_logical_type(&child_t);
            if (rc != 0) { if (child_name) duckdb_free(child_name); ducknng_quack_node_free(node); return -1; }
            if (ducknng_quack_node_add_child(node, child, child_name ? child_name : "") != 0) {
                if (child_name) duckdb_free(child_name);
                ducknng_quack_node_free(child); ducknng_quack_node_free(node); goto oom;
            }
            if (child_name) duckdb_free(child_name);
        }
        break;
    }
    case DUCKDB_TYPE_MAP: {
        duckdb_logical_type key_t = duckdb_map_type_key_type(type);
        duckdb_logical_type value_t = duckdb_map_type_value_type(type);
        ducknng_quack_column_schema *entry = NULL, *key_node = NULL, *value_node = NULL;
        int rc;
        node = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_MAP);
        entry = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_STRUCT);
        if (!node || !entry) {
            if (key_t) duckdb_destroy_logical_type(&key_t);
            if (value_t) duckdb_destroy_logical_type(&value_t);
            ducknng_quack_node_free(node); ducknng_quack_node_free(entry); goto oom;
        }
        rc = ducknng_quack_node_from_duckdb(key_t, depth + 2, &key_node, errmsg);
        if (key_t) duckdb_destroy_logical_type(&key_t);
        if (rc == 0) rc = ducknng_quack_node_from_duckdb(value_t, depth + 2, &value_node, errmsg);
        if (value_t) duckdb_destroy_logical_type(&value_t);
        if (rc != 0) { ducknng_quack_node_free(key_node); ducknng_quack_node_free(entry); ducknng_quack_node_free(node); return -1; }
        if (ducknng_quack_node_add_child(entry, key_node, "key") != 0) {
            ducknng_quack_node_free(key_node); ducknng_quack_node_free(value_node);
            ducknng_quack_node_free(entry); ducknng_quack_node_free(node); goto oom;
        }
        if (ducknng_quack_node_add_child(entry, value_node, "value") != 0) {
            ducknng_quack_node_free(value_node); ducknng_quack_node_free(entry); ducknng_quack_node_free(node); goto oom;
        }
        if (ducknng_quack_node_add_child(node, entry, "") != 0) {
            ducknng_quack_node_free(entry); ducknng_quack_node_free(node); goto oom;
        }
        break;
    }
    case DUCKDB_TYPE_ENUM: {
        idx_t size = duckdb_enum_dictionary_size(type);
        idx_t i;
        node = ducknng_quack_node_new(DUCKNNG_QUACK_LOGICAL_ENUM);
        if (!node) goto oom;
        node->enum_count = (uint32_t)size;
        node->enum_labels = (char **)duckdb_malloc(sizeof(char *) * (size ? (size_t)size : 1));
        if (!node->enum_labels) { ducknng_quack_node_free(node); goto oom; }
        memset(node->enum_labels, 0, sizeof(char *) * (size ? (size_t)size : 1));
        for (i = 0; i < size; i++) {
            char *label = duckdb_enum_dictionary_value(type, i);
            node->enum_labels[i] = ducknng_strdup(label ? label : "");
            if (label) duckdb_free(label);
            if (!node->enum_labels[i]) { ducknng_quack_node_free(node); goto oom; }
        }
        break;
    }
    default: {
        ducknng_quack_type_meta meta;
        if (ducknng_quack_duckdb_type_to_meta(type, &meta, errmsg) != 0) return -1;
        node = ducknng_quack_node_new(meta.logical_type_id);
        if (!node) goto oom;
        node->decimal_width = meta.decimal_width;
        node->decimal_scale = meta.decimal_scale;
        break;
    }
    }
    *out = node;
    return 0;
oom:
    ducknng_quack_set_error(errmsg, "ducknng: out of memory building quack type node");
    return -1;
}

static int ducknng_quack_node_to_duckdb(const ducknng_quack_column_schema *node, unsigned depth,
    duckdb_logical_type *out_type, char **errmsg) {
    if (out_type) *out_type = NULL;
    if (!node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack type node");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_LIST:
    case DUCKNNG_QUACK_LOGICAL_ARRAY: {
        duckdb_logical_type child = NULL;
        if (node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/array type requires exactly one child");
            return -1;
        }
        if (ducknng_quack_node_to_duckdb(node->children[0], depth + 1, &child, errmsg) != 0) return -1;
        *out_type = (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST)
            ? duckdb_create_list_type(child) : duckdb_create_array_type(child, node->array_size);
        duckdb_destroy_logical_type(&child);
        break;
    }
    case DUCKNNG_QUACK_LOGICAL_STRUCT:
    case DUCKNNG_QUACK_LOGICAL_UNION: {
        uint32_t start = (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) ? 1u : 0u;
        uint32_t n = (node->nchildren > start) ? node->nchildren - start : 0u;
        duckdb_logical_type *types = NULL;
        const char **names = NULL;
        uint32_t i;
        int ok = 1;
        if (n == 0) { ducknng_quack_set_error(errmsg, "ducknng: quack struct/union type has no members"); return -1; }
        types = (duckdb_logical_type *)duckdb_malloc(sizeof(*types) * n);
        names = (const char **)duckdb_malloc(sizeof(*names) * n);
        if (!types || !names) { if (types) duckdb_free(types); if (names) duckdb_free(names);
            ducknng_quack_set_error(errmsg, "ducknng: out of memory building struct/union type"); return -1; }
        memset(types, 0, sizeof(*types) * n);
        for (i = 0; i < n; i++) {
            names[i] = node->child_names ? node->child_names[start + i] : "";
            if (ducknng_quack_node_to_duckdb(node->children[start + i], depth + 1, &types[i], errmsg) != 0) { ok = 0; break; }
        }
        if (ok) {
            *out_type = (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT)
                ? duckdb_create_struct_type(types, names, n)
                : duckdb_create_union_type(types, names, n);
        }
        for (i = 0; i < n; i++) if (types[i]) duckdb_destroy_logical_type(&types[i]);
        duckdb_free(types);
        duckdb_free(names);
        if (!ok) return -1;
        break;
    }
    case DUCKNNG_QUACK_LOGICAL_MAP: {
        duckdb_logical_type key_t = NULL, value_t = NULL;
        const ducknng_quack_column_schema *entry;
        if (node->nchildren != 1 || !node->children[0] ||
            node->children[0]->logical_type_id != DUCKNNG_QUACK_LOGICAL_STRUCT ||
            node->children[0]->nchildren != 2) {
            ducknng_quack_set_error(errmsg, "ducknng: malformed quack map type");
            return -1;
        }
        entry = node->children[0];
        if (ducknng_quack_node_to_duckdb(entry->children[0], depth + 2, &key_t, errmsg) != 0) return -1;
        if (ducknng_quack_node_to_duckdb(entry->children[1], depth + 2, &value_t, errmsg) != 0) {
            duckdb_destroy_logical_type(&key_t); return -1;
        }
        *out_type = duckdb_create_map_type(key_t, value_t);
        duckdb_destroy_logical_type(&key_t);
        duckdb_destroy_logical_type(&value_t);
        break;
    }
    case DUCKNNG_QUACK_LOGICAL_ENUM: {
        *out_type = duckdb_create_enum_type((const char **)node->enum_labels, node->enum_count);
        break;
    }
    default: {
        ducknng_quack_type_meta meta;
        memset(&meta, 0, sizeof(meta));
        meta.logical_type_id = node->logical_type_id;
        meta.decimal_width = node->decimal_width;
        meta.decimal_scale = node->decimal_scale;
        return ducknng_quack_meta_to_duckdb_type(&meta, out_type, errmsg);
    }
    }
    if (!*out_type) {
        ducknng_quack_set_error(errmsg, "ducknng: failed to allocate DuckDB logical type for quack node");
        return -1;
    }
    return 0;
}

static uint64_t ducknng_quack_expected_info_kind(int logical_type_id) {
    return ducknng_qk_type_expected_info_kind(logical_type_id);
}

static int ducknng_quack_type_needs_info(
    const ducknng_quack_column_schema *node) {
    return ducknng_qk_type_needs_info(node);
}

static int ducknng_quack_validate_type_shape(
    const ducknng_quack_column_schema *node, char **errmsg) {
    const char *message = NULL;
    if (ducknng_qk_type_shape_validate(node, &message) != 0) {
        ducknng_quack_set_error(errmsg, message);
        return -1;
    }
    return 0;
}

static int ducknng_quack_write_child_pair(ducknng_quack_writer *w, const char *name,
    const ducknng_quack_column_schema *child, unsigned depth, char **errmsg) {
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_PAIR_FIRST, errmsg) != 0 ||
        ducknng_quack_write_string(w, name ? name : "", errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_PAIR_SECOND, errmsg) != 0) return -1;
    if (ducknng_quack_write_type_node(w, child, depth, errmsg) != 0) return -1;
    return ducknng_quack_write_field_end(w, errmsg);
}

static int ducknng_quack_write_type_node(ducknng_quack_writer *w,
    const ducknng_quack_column_schema *node, unsigned depth, char **errmsg) {
    uint32_t i;
    if (!w || !node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack type node");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_ID, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)node->logical_type_id, errmsg) != 0) return -1;
    if (ducknng_quack_type_needs_info(node)) {
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_TYPE_INFO, errmsg) != 0 ||
            ducknng_quack_write_byte(w, 1, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_INFO_KIND, errmsg) != 0) return -1;
        switch (node->logical_type_id) {
        case DUCKNNG_QUACK_LOGICAL_DECIMAL:
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL, errmsg) != 0) return -1;
            if (node->decimal_width &&
                (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_WIDTH, errmsg) != 0 ||
                 ducknng_quack_write_uleb128(w, node->decimal_width, errmsg) != 0)) return -1;
            if (node->decimal_scale &&
                (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_DECIMAL_SCALE, errmsg) != 0 ||
                 ducknng_quack_write_uleb128(w, node->decimal_scale, errmsg) != 0)) return -1;
            break;
        case DUCKNNG_QUACK_LOGICAL_LIST:
        case DUCKNNG_QUACK_LOGICAL_MAP:
            if (node->nchildren != 1) {
                ducknng_quack_set_error(errmsg, "ducknng: quack list/map type requires one child");
                return -1;
            }
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_LIST, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_CHILD, errmsg) != 0 ||
                ducknng_quack_write_type_node(w, node->children[0], depth + 1, errmsg) != 0) return -1;
            break;
        case DUCKNNG_QUACK_LOGICAL_STRUCT:
        case DUCKNNG_QUACK_LOGICAL_UNION:
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_STRUCT, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_STRUCT_COUNT, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, node->nchildren, errmsg) != 0) return -1;
            for (i = 0; i < node->nchildren; i++) {
                const char *nm = (node->child_names && node->child_names[i]) ? node->child_names[i] : "";
                if (ducknng_quack_write_child_pair(w, nm, node->children[i], depth + 1, errmsg) != 0) return -1;
            }
            break;
        case DUCKNNG_QUACK_LOGICAL_ENUM:
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_ENUM, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_ENUM_COUNT, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, node->enum_count, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_ENUM_VALUES, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, node->enum_count, errmsg) != 0) return -1;
            for (i = 0; i < node->enum_count; i++) {
                const char *label = (node->enum_labels && node->enum_labels[i]) ? node->enum_labels[i] : "";
                if (ducknng_quack_write_string(w, label, errmsg) != 0) return -1;
            }
            break;
        case DUCKNNG_QUACK_LOGICAL_ARRAY:
            if (node->nchildren != 1) {
                ducknng_quack_set_error(errmsg, "ducknng: quack array type requires one child");
                return -1;
            }
            if (ducknng_quack_write_uleb128(w, DUCKNNG_QUACK_EXTRA_TYPE_ARRAY, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_CHILD, errmsg) != 0 ||
                ducknng_quack_write_type_node(w, node->children[0], depth + 1, errmsg) != 0) return -1;
            if (node->array_size &&
                (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_EXTRA_ARRAY_SIZE, errmsg) != 0 ||
                 ducknng_quack_write_uleb128(w, node->array_size, errmsg) != 0)) return -1;
            break;
        default:
            ducknng_quack_set_error(errmsg, "ducknng: internal error writing quack type info");
            return -1;
        }
        if (ducknng_quack_write_field_end(w, errmsg) != 0) return -1; /* end type_info */
    }
    return ducknng_quack_write_field_end(w, errmsg); /* end LogicalType */
}

static int ducknng_quack_read_child_pair(ducknng_quack_reader *r, char **name_out,
    ducknng_quack_column_schema **type_out, unsigned depth, char **errmsg) {
    uint16_t field = 0;
    int seen_name = 0, seen_type = 0;
    char *name = NULL;
    *name_out = NULL;
    *type_out = NULL;
    for (;;) {
        if (ducknng_quack_read_u16le(r, &field, errmsg) != 0) goto fail;
        if (field == DUCKNNG_QUACK_FIELD_END) break;
        if (field == DUCKNNG_QUACK_PAIR_FIRST) {
            if (seen_name) { ducknng_quack_set_error(errmsg, "ducknng: duplicate name in quack struct member"); goto fail; }
            seen_name = 1;
            if (ducknng_quack_read_string_dup(r, &name, errmsg) != 0) goto fail;
        } else if (field == DUCKNNG_QUACK_PAIR_SECOND) {
            if (seen_type) { ducknng_quack_set_error(errmsg, "ducknng: duplicate type in quack struct member"); goto fail; }
            seen_type = 1;
            if (ducknng_quack_read_type_node(r, depth, type_out, errmsg) != 0) goto fail;
        } else {
            ducknng_quack_set_error(errmsg, "ducknng: unexpected field in quack struct member");
            goto fail;
        }
    }
    if (!*type_out) { ducknng_quack_set_error(errmsg, "ducknng: quack struct member missing its type"); goto fail; }
    if (!name) { name = ducknng_strdup(""); if (!name) goto fail; }
    *name_out = name;
    return 0;
fail:
    if (name) duckdb_free(name);
    if (*type_out) { ducknng_quack_node_free(*type_out); *type_out = NULL; }
    return -1;
}

static int ducknng_quack_read_type_info(ducknng_quack_reader *r,
    ducknng_quack_column_schema *t, unsigned depth, char **errmsg) {
    uint16_t field = 0;
    uint64_t value = 0;
    uint64_t i, count;
    uint32_t seen = 0;
    /* Field 200 only records how many enum labels the payload claims. The
     * count is copied into the node in the field 201 branch, once an array of
     * exactly that many slots exists, so t->enum_count can never exceed the
     * allocation that ducknng_quack_node_free_contents later walks. */
    uint64_t declared_enum_count = 0;
    for (;;) {
        if (ducknng_quack_read_u16le(r, &field, errmsg) != 0) return -1;
        if (field == DUCKNNG_QUACK_FIELD_END) {
            uint32_t required = 1u; /* field 100: extra-info kind */
            switch (t->logical_type_id) {
            case DUCKNNG_QUACK_LOGICAL_DECIMAL:
            case DUCKNNG_QUACK_LOGICAL_LIST:
            case DUCKNNG_QUACK_LOGICAL_MAP:
            case DUCKNNG_QUACK_LOGICAL_STRUCT:
            case DUCKNNG_QUACK_LOGICAL_UNION:
                required |= 1u << 5; /* field 200 */
                break;
            case DUCKNNG_QUACK_LOGICAL_ENUM:
            case DUCKNNG_QUACK_LOGICAL_ARRAY:
                required |= (1u << 5) | (1u << 6); /* fields 200 and 201 */
                break;
            default:
                break;
            }
            if ((seen & required) != required) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: quack type info is missing required fields");
                return -1;
            }
            return ducknng_quack_validate_type_shape(t, errmsg);
        }
        if (field == 100 || field == 101 || field == 103 ||
            field == 200 || field == 201) {
            uint32_t bit = (field < 200)
                ? (1u << (field - 100)) : (1u << (field - 200 + 5));
            if (seen & bit) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: duplicate field in quack type info");
                return -1;
            }
            seen |= bit;
        }
        switch (field) {
        case 100: /* extra-info kind */
            if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
            if (value != ducknng_quack_expected_info_kind(t->logical_type_id)) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: quack logical type and extra-info kind disagree");
                return -1;
            }
            break;
        case 101: /* alias */
            if (ducknng_quack_skip_string(r, errmsg) != 0) return -1;
            break;
        case 103: { /* extension info pointer */
            uint8_t present = 0;
            if (ducknng_quack_read_boolean(r, &present,
                    "ducknng: quack extension-info presence flag is not boolean",
                    errmsg) != 0) return -1;
            if (present) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: extension type info is not supported");
                return -1;
            }
            break;
        }
        case DUCKNNG_QUACK_EXTRA_CHILD: /* 200 */
            switch (t->logical_type_id) {
            case DUCKNNG_QUACK_LOGICAL_DECIMAL:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value == 0 || value > 38) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack decimal width must be between 1 and 38");
                    return -1;
                }
                t->decimal_width = (uint8_t)value;
                break;
            case DUCKNNG_QUACK_LOGICAL_LIST:
            case DUCKNNG_QUACK_LOGICAL_MAP:
            case DUCKNNG_QUACK_LOGICAL_ARRAY: {
                ducknng_quack_column_schema *child = NULL;
                if (ducknng_quack_read_type_node(r, depth + 1,
                        &child, errmsg) != 0) return -1;
                if (ducknng_quack_node_add_child(t, child, "") != 0) {
                    ducknng_quack_node_free(child);
                    ducknng_quack_set_error(errmsg,
                        "ducknng: out of memory decoding nested quack type");
                    return -1;
                }
                break;
            }
            case DUCKNNG_QUACK_LOGICAL_STRUCT:
            case DUCKNNG_QUACK_LOGICAL_UNION: {
                size_t children_bytes = 0;
                size_t names_bytes = 0;
                if (ducknng_quack_read_uleb128(r, &count, errmsg) != 0) return -1;
                if (count > DUCKNNG_QUACK_MAX_STRUCT_MEMBERS ||
                    count > (uint64_t)ducknng_qk_reader_remaining(r) ||
                    count > SIZE_MAX) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack struct member count exceeds its payload bound");
                    return -1;
                }
                if (ducknng_size_mul(sizeof(*t->children), (size_t)count,
                        &children_bytes) != 0 ||
                    ducknng_size_mul(sizeof(*t->child_names), (size_t)count,
                        &names_bytes) != 0) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack struct member allocation overflows");
                    return -1;
                }
                if (count > 0) {
                    t->children = (ducknng_quack_column_schema **)
                        duckdb_malloc(children_bytes);
                    t->child_names = (char **)duckdb_malloc(names_bytes);
                    if (!t->children || !t->child_names) {
                        ducknng_quack_set_error(errmsg,
                            "ducknng: out of memory decoding quack struct members");
                        return -1;
                    }
                    memset(t->children, 0, children_bytes);
                    memset(t->child_names, 0, names_bytes);
                }
                for (i = 0; i < count; i++) {
                    char *name = NULL;
                    ducknng_quack_column_schema *child = NULL;
                    if (ducknng_quack_read_child_pair(r, &name, &child,
                            depth + 1, errmsg) != 0) return -1;
                    t->children[i] = child;
                    t->child_names[i] = name;
                    t->nchildren++;
                }
                break;
            }
            case DUCKNNG_QUACK_LOGICAL_ENUM:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value > DUCKNNG_QUACK_MAX_ENUM_VALUES) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack enum size exceeds limit");
                    return -1;
                }
                declared_enum_count = value;
                break;
            default:
                ducknng_quack_set_error(errmsg,
                    "ducknng: unexpected field 200 in quack type info");
                return -1;
            }
            break;
        case DUCKNNG_QUACK_EXTRA_ARRAY_SIZE: /* 201 */
            /* DuckDB's serializer emits field 200 before field 201, and every
             * type carrying both reads 201 against state established by 200.
             * Reject the reverse order rather than validating against a
             * default-initialized count. */
            if (!(seen & (1u << 5))) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: quack type info field 201 precedes field 200");
                return -1;
            }
            switch (t->logical_type_id) {
            case DUCKNNG_QUACK_LOGICAL_DECIMAL:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value > 38) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack decimal scale exceeds 38");
                    return -1;
                }
                t->decimal_scale = (uint8_t)value;
                break;
            case DUCKNNG_QUACK_LOGICAL_ARRAY:
                if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) return -1;
                if (value == 0 || value > UINT32_MAX) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack array size is invalid");
                    return -1;
                }
                t->array_size = (uint32_t)value;
                break;
            case DUCKNNG_QUACK_LOGICAL_ENUM: {
                uint64_t n = 0;
                size_t labels_bytes = 0;
                if (ducknng_quack_read_uleb128(r, &n, errmsg) != 0) return -1;
                if (n != declared_enum_count) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack enum values disagree with count");
                    return -1;
                }
                if (n > DUCKNNG_QUACK_MAX_ENUM_VALUES ||
                    n > (uint64_t)ducknng_qk_reader_remaining(r) ||
                    n > SIZE_MAX ||
                    ducknng_size_mul(sizeof(*t->enum_labels),
                        n ? (size_t)n : 1u, &labels_bytes) != 0) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: quack enum size exceeds its payload bound");
                    return -1;
                }
                t->enum_labels = (char **)duckdb_malloc(labels_bytes);
                if (!t->enum_labels) {
                    ducknng_quack_set_error(errmsg,
                        "ducknng: out of memory decoding quack enum labels");
                    return -1;
                }
                memset(t->enum_labels, 0, labels_bytes);
                /* Publish the count only now that enum_labels holds exactly
                 * this many zeroed slots. A later label-read failure leaves the
                 * unfilled tail NULL, which the free path skips. */
                t->enum_count = (uint32_t)n;
                for (i = 0; i < n; i++) {
                    if (ducknng_quack_read_string_dup(r,
                            &t->enum_labels[i], errmsg) != 0) return -1;
                }
                break;
            }
            default:
                ducknng_quack_set_error(errmsg,
                    "ducknng: unexpected field 201 in quack type info");
                return -1;
            }
            break;
        default:
            ducknng_quack_set_error(errmsg,
                "ducknng: unknown field in quack type info");
            return -1;
        }
    }
}

static int ducknng_quack_read_type_node(ducknng_quack_reader *r, unsigned depth,
    ducknng_quack_column_schema **out, char **errmsg) {
    ducknng_quack_column_schema *t = NULL;
    uint16_t field = 0;
    uint64_t value = 0;
    int seen_id = 0, seen_info = 0, info_present = 0;
    if (out) *out = NULL;
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack type nesting exceeds supported depth");
        return -1;
    }
    t = ducknng_quack_node_new(0);
    if (!t) {
        ducknng_quack_set_error(errmsg,
            "ducknng: out of memory decoding quack type");
        return -1;
    }
    for (;;) {
        if (ducknng_quack_read_u16le(r, &field, errmsg) != 0) goto fail;
        if (field == DUCKNNG_QUACK_FIELD_END) break;
        switch (field) {
        case DUCKNNG_QUACK_TYPE_ID: /* 100 */
            if (seen_id) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: duplicate type id in quack logical type");
                goto fail;
            }
            if (ducknng_quack_read_uleb128(r, &value, errmsg) != 0) goto fail;
            if (value > INT32_MAX) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: quack logical type id exceeds supported range");
                goto fail;
            }
            t->logical_type_id = (int)value;
            seen_id = 1;
            break;
        case DUCKNNG_QUACK_TYPE_INFO: { /* 101 */
            uint8_t present = 0;
            if (!seen_id) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: quack type info precedes its id");
                goto fail;
            }
            if (seen_info) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: duplicate type info in quack logical type");
                goto fail;
            }
            seen_info = 1;
            if (ducknng_quack_read_boolean(r, &present,
                    "ducknng: quack type-info presence flag is not boolean",
                    errmsg) != 0) goto fail;
            info_present = present != 0;
            if (info_present &&
                ducknng_quack_read_type_info(r, t, depth, errmsg) != 0) goto fail;
            break;
        }
        default:
            ducknng_quack_set_error(errmsg,
                "ducknng: unknown field in quack logical type");
            goto fail;
        }
    }
    if (!seen_id) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack logical type missing its id");
        goto fail;
    }
    if (ducknng_quack_type_needs_info(t) && (!seen_info || !info_present)) {
        ducknng_quack_set_error(errmsg,
            "ducknng: nested or parameterized quack type is missing its type info");
        goto fail;
    }
    /* ducknng_quack_read_type_info already validated the shape, and the node is
     * unmodified since. Only a type with no type-info still needs the check,
     * which keeps struct member and enum label arrays from being walked twice
     * on every schema decode. */
    if (!info_present && ducknng_quack_validate_type_shape(t, errmsg) != 0) {
        goto fail;
    }
    *out = t;
    return 0;
fail:
    ducknng_quack_node_free(t);
    return -1;
}

static int ducknng_quack_encode_validity_blob(ducknng_quack_writer *w,
    const uint64_t *validity, idx_t rows, char **errmsg) {
    /* Same overflow-checked sizing the decode path compares against, so encode
     * and decode cannot disagree about a mask's byte length. */
    size_t validity_len = ducknng_quack_validity_bytes(rows);
    if (validity_len == SIZE_MAX) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack validity mask length overflows");
        return -1;
    }
    return ducknng_quack_write_blob(w, (const uint8_t *)validity, validity_len, errmsg);
}

static int ducknng_quack_encode_vector(ducknng_quack_writer *w, duckdb_vector vec,
    const ducknng_quack_column_schema *node, idx_t rows, unsigned depth, char **errmsg) {
    uint64_t *validity = NULL;
    int has_validity = 0;
    size_t width = 0;
    if (!w || !vec || !node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack vector encoder state");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack vector nesting exceeds supported depth");
        return -1;
    }
    validity = duckdb_vector_get_validity(vec);
    has_validity = validity != NULL;
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_HAS_VALIDITY, errmsg) != 0 ||
        ducknng_quack_write_byte(w, has_validity ? 1u : 0u, errmsg) != 0) return -1;
    if (has_validity) {
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_encode_validity_blob(w, validity, rows, errmsg) != 0) return -1;
    }
    if (ducknng_quack_node_is_varlen(node)) {
        duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vec);
        idx_t row;
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, (uint64_t)rows, errmsg) != 0) return -1;
        for (row = 0; row < rows; row++) {
            const uint8_t *ptr = NULL;
            size_t len = 0;
            if (ducknng_quack_vector_row_is_valid((const uint8_t *)validity, row)) {
                duckdb_string_t *value = &data[row];
                len = (size_t)duckdb_string_t_length(*value);
                ptr = (const uint8_t *)duckdb_string_t_data(value);
            }
            if (ducknng_quack_write_string_len(w, ptr, len, errmsg) != 0) return -1;
        }
        return ducknng_quack_write_field_end(w, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) {
        uint32_t i;
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, node->nchildren, errmsg) != 0) return -1;
        for (i = 0; i < node->nchildren; i++) {
            duckdb_vector child = duckdb_struct_vector_get_child(vec, i);
            if (ducknng_quack_encode_vector(w, child, node->children[i], rows, depth + 1, errmsg) != 0) return -1;
        }
        return ducknng_quack_write_field_end(w, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_MAP) {
        idx_t child_size = duckdb_list_vector_get_size(vec);
        duckdb_list_entry *entries = (duckdb_list_entry *)duckdb_vector_get_data(vec);
        duckdb_vector child = duckdb_list_vector_get_child(vec);
        idx_t row;
        if (node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/map vector has no child type");
            return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, (uint64_t)child_size, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_LIST_ENTRIES, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, (uint64_t)rows, errmsg) != 0) return -1;
        for (row = 0; row < rows; row++) {
            uint64_t off = 0, len = 0;
            if (!validity || ducknng_quack_vector_row_is_valid((const uint8_t *)validity, row)) {
                off = entries[row].offset;
                len = entries[row].length;
            }
            if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_ENTRY_OFFSET, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, off, errmsg) != 0 ||
                ducknng_quack_write_field_id(w, DUCKNNG_QUACK_ENTRY_LENGTH, errmsg) != 0 ||
                ducknng_quack_write_uleb128(w, len, errmsg) != 0 ||
                ducknng_quack_write_field_end(w, errmsg) != 0) return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_LIST_CHILD, errmsg) != 0) return -1;
        if (ducknng_quack_encode_vector(w, child, node->children[0], child_size, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_write_field_end(w, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ARRAY) {
        duckdb_vector child = duckdb_array_vector_get_child(vec);
        idx_t child_rows;
        if (node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector has no child type");
            return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_ARRAY_SIZE, errmsg) != 0 ||
            ducknng_quack_write_uleb128(w, node->array_size, errmsg) != 0 ||
            ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_ARRAY_CHILD, errmsg) != 0) return -1;
        if (ducknng_quack_idx_mul(rows, (idx_t)node->array_size, &child_rows,
                "ducknng: quack array row count overflows supported size", errmsg) != 0) return -1;
        if (ducknng_quack_encode_vector(w, child, node->children[0], child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_write_field_end(w, errmsg);
    }
    width = ducknng_quack_node_fixed_width(node);
    if (width == 0) {
        ducknng_quack_set_error(errmsg, "ducknng: unsupported column type for ducknng_quack_batch encoding");
        return -1;
    }
    {
        size_t data_len = 0;
        if (ducknng_size_mul(width, (size_t)rows, &data_len) != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: fixed-size quack vector payload overflows supported size");
            return -1;
        }
        if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_write_blob(w, (const uint8_t *)duckdb_vector_get_data(vec), data_len, errmsg) != 0) return -1;
    }
    return ducknng_quack_write_field_end(w, errmsg);
}

static int ducknng_quack_encode_one_chunk(ducknng_quack_writer *w, duckdb_data_chunk chunk,
    idx_t ncols, char **errmsg) {
    ducknng_quack_column_schema **nodes = NULL;
    idx_t rows = duckdb_data_chunk_get_size(chunk);
    idx_t col;
    size_t nodes_bytes = 0;
    int rc = -1;
    if (ncols > 0) {
        if (ducknng_size_mul(sizeof(*nodes), (size_t)ncols, &nodes_bytes) != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: quack chunk schema allocation overflows supported size");
            return -1;
        }
        nodes = (ducknng_quack_column_schema **)duckdb_malloc(nodes_bytes);
        if (!nodes) {
            ducknng_quack_set_error(errmsg, "ducknng: out of memory allocating quack chunk schema");
            return -1;
        }
        memset(nodes, 0, nodes_bytes);
    }
    for (col = 0; col < ncols; col++) {
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, col);
        duckdb_logical_type logical_type = duckdb_vector_get_column_type(vec);
        int node_rc = ducknng_quack_node_from_duckdb(logical_type, 0, &nodes[col], errmsg);
        if (logical_type) duckdb_destroy_logical_type(&logical_type);
        if (node_rc != 0) goto done;
    }
    if (ducknng_quack_write_byte(w, 1, errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)rows, errmsg) != 0 ||
        ducknng_quack_write_field_id(w, DUCKNNG_QUACK_CHUNK_TYPES, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)ncols, errmsg) != 0) goto done;
    for (col = 0; col < ncols; col++) {
        if (ducknng_quack_write_type_node(w, nodes[col], 0, errmsg) != 0) goto done;
    }
    if (ducknng_quack_write_field_id(w, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_write_uleb128(w, (uint64_t)ncols, errmsg) != 0) goto done;
    for (col = 0; col < ncols; col++) {
        duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, col);
        if (ducknng_quack_encode_vector(w, vec, nodes[col], rows, 0, errmsg) != 0) goto done;
    }
    /* Close DataChunk, then its containing DataChunkWrapper. Each result list
     * element is a nullable unique_ptr<DataChunkWrapper> in DuckDB's generated
     * message encoding, so both object terminators are required per chunk. */
    if (ducknng_quack_write_field_end(w, errmsg) != 0 ||
        ducknng_quack_write_field_end(w, errmsg) != 0) goto done;
    rc = 0;
done:
    if (nodes) {
        for (col = 0; col < ncols; col++) ducknng_quack_node_free(nodes[col]);
        duckdb_free(nodes);
    }
    return rc;
}

static int ducknng_quack_encode_result_payload_pass(
    ducknng_quack_writer *writer, duckdb_result result,
    duckdb_data_chunk *chunks, idx_t chunk_count, int include_schema,
    char **errmsg) {
    idx_t ncols = duckdb_column_count(&result);
    idx_t col;
    idx_t chunk_idx;
    if ((uint64_t)ncols > DUCKNNG_QUACK_MAX_STRUCT_MEMBERS) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack column count exceeds supported limit");
        return -1;
    }
    if (include_schema) {
        if (ducknng_quack_write_field_id(writer,
                DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_write_uleb128(writer, (uint64_t)ncols,
                errmsg) != 0) return -1;
        for (col = 0; col < ncols; col++) {
            duckdb_logical_type logical_type =
                duckdb_column_logical_type(&result, col);
            ducknng_quack_column_schema *node = NULL;
            int rc = ducknng_quack_node_from_duckdb(logical_type, 0,
                &node, errmsg);
            if (logical_type) duckdb_destroy_logical_type(&logical_type);
            if (rc != 0) return -1;
            rc = ducknng_quack_write_type_node(writer, node, 0, errmsg);
            ducknng_quack_node_free(node);
            if (rc != 0) return -1;
        }
        if (ducknng_quack_write_field_id(writer,
                DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_write_uleb128(writer, (uint64_t)ncols,
                errmsg) != 0) return -1;
        for (col = 0; col < ncols; col++) {
            if (ducknng_quack_write_string(writer,
                    duckdb_column_name(&result, col), errmsg) != 0) return -1;
        }
    }
    if (chunk_count > 0) {
        if (ducknng_quack_write_field_id(writer,
                DUCKNNG_QUACK_OUTER_RESULTS, errmsg) != 0 ||
            ducknng_quack_write_uleb128(writer, (uint64_t)chunk_count,
                errmsg) != 0) return -1;
        for (chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
            if (!chunks[chunk_idx]) {
                ducknng_quack_set_error(errmsg,
                    "ducknng: missing quack chunk while encoding");
                return -1;
            }
            if (ducknng_quack_encode_one_chunk(writer, chunks[chunk_idx],
                    ncols, errmsg) != 0) return -1;
        }
    }
    return ducknng_quack_write_field_end(writer, errmsg);
}

static int ducknng_quack_encode_result_payload(duckdb_result result,
    duckdb_data_chunk *chunks, idx_t chunk_count, int include_schema,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    ducknng_quack_writer writer;
    uint8_t *bytes;
    size_t required;
    if (out_bytes) *out_bytes = NULL;
    if (out_len) *out_len = 0;
    if (!out_bytes || !out_len) {
        ducknng_quack_set_error(errmsg,
            "ducknng: missing quack payload output pointers");
        return -1;
    }
    ducknng_qk_writer_init_measure(&writer);
    if (ducknng_quack_encode_result_payload_pass(&writer, result, chunks,
            chunk_count, include_schema, errmsg) != 0) return -1;
    required = ducknng_qk_writer_size(&writer);
    bytes = (uint8_t *)duckdb_malloc(required ? required : 1u);
    if (!bytes) {
        ducknng_quack_set_error(errmsg,
            "ducknng: out of memory allocating measured quack payload");
        return -1;
    }
    ducknng_qk_writer_init_fixed(&writer, bytes, required);
    if (ducknng_quack_encode_result_payload_pass(&writer, result, chunks,
            chunk_count, include_schema, errmsg) != 0 ||
        ducknng_qk_writer_size(&writer) != required) {
        duckdb_free(bytes);
        return -1;
    }
    *out_bytes = bytes;
    *out_len = required;
    return 0;
}

#ifndef DUCKNNG_CORE_ONLY
static int ducknng_quack_encode_result_message(duckdb_result result,
    duckdb_data_chunk *chunks, idx_t chunk_count, int include_schema,
    size_t prefix_size, nng_msg **out_msg, size_t *out_payload_len,
    char **errmsg) {
    ducknng_quack_writer writer;
    nng_msg *msg = NULL;
    uint8_t *payload;
    size_t payload_len;
    size_t message_len;
    int rv;
    if (out_msg) *out_msg = NULL;
    if (out_payload_len) *out_payload_len = 0;
    if (!out_msg || !out_payload_len) {
        ducknng_quack_set_error(errmsg,
            "ducknng: missing quack message output pointers");
        return -1;
    }
    ducknng_qk_writer_init_measure(&writer);
    if (ducknng_quack_encode_result_payload_pass(&writer, result, chunks,
            chunk_count, include_schema, errmsg) != 0) return -1;
    payload_len = ducknng_qk_writer_size(&writer);
    if (ducknng_size_add(prefix_size, payload_len, &message_len) != 0) {
        ducknng_quack_set_error(errmsg,
            "ducknng: measured quack message size overflows supported size");
        return -1;
    }
    rv = nng_msg_alloc(&msg, message_len);
    if (rv != 0) {
        ducknng_quack_set_error(errmsg,
            "ducknng: out of memory allocating final quack message");
        return -1;
    }
    payload = (uint8_t *)nng_msg_body(msg) + prefix_size;
    ducknng_qk_writer_init_fixed(&writer, payload, payload_len);
    if (ducknng_quack_encode_result_payload_pass(&writer, result, chunks,
            chunk_count, include_schema, errmsg) != 0 ||
        ducknng_qk_writer_size(&writer) != payload_len) {
        nng_msg_free(msg);
        return -1;
    }
    *out_msg = msg;
    *out_payload_len = payload_len;
    return 0;
}
#endif

/* Read a Vector object's optional field-90 type. Flat is the explicit default
 * when the field is absent. Compressed vectors carry their own fields and do
 * not have the flat validity/data prologue. */
static int ducknng_quack_read_vector_type(ducknng_quack_reader *r,
    uint64_t *out_vector_type, char **errmsg) {
    uint16_t field_id = 0;
    uint64_t vector_type = DUCKNNG_QUACK_VECTOR_FLAT;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_VECTOR_TYPE) {
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &vector_type, errmsg) != 0) return -1;
    }
    if (vector_type > DUCKNNG_QUACK_VECTOR_SEQUENCE) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid quack vector type");
        return -1;
    }
    if (out_vector_type) *out_vector_type = vector_type;
    return 0;
}

static int ducknng_quack_read_flat_vector_prologue(ducknng_quack_reader *r,
    const uint8_t **out_validity, size_t *out_validity_len, char **errmsg) {
    uint8_t has_validity = 0;
    if (out_validity) *out_validity = NULL;
    if (out_validity_len) *out_validity_len = 0;
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_HAS_VALIDITY, errmsg) != 0 ||
        ducknng_quack_read_boolean(r, &has_validity,
            "ducknng: quack validity presence flag is not boolean",
            errmsg) != 0) return -1;
    if (has_validity) {
        const uint8_t *blob = NULL;
        size_t blob_len = 0;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_VALIDITY, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &blob, &blob_len, errmsg) != 0) return -1;
        if (out_validity) *out_validity = blob;
        if (out_validity_len) *out_validity_len = blob_len;
    }
    return 0;
}

static int ducknng_quack_read_dictionary_header(ducknng_quack_reader *r,
    idx_t rows, const uint8_t **out_selection, idx_t *out_dictionary_count,
    char **errmsg) {
    const uint8_t *selection = NULL;
    size_t selection_len = 0;
    size_t expected_len = 0;
    uint64_t dictionary_count_u64 = 0;
    idx_t dictionary_count = 0;
    idx_t row;
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_SELECTION, errmsg) != 0 ||
        ducknng_quack_read_blob_view(r, &selection, &selection_len, errmsg) != 0 ||
        ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DICTIONARY_COUNT, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &dictionary_count_u64, errmsg) != 0) return -1;
    if (ducknng_size_mul(sizeof(sel_t), (size_t)rows, &expected_len) != 0) {
        ducknng_quack_set_error(errmsg, "ducknng: quack dictionary selection size overflows supported size");
        return -1;
    }
    if (selection_len != expected_len) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid quack dictionary selection payload");
        return -1;
    }
    if (ducknng_quack_uint64_to_idx(dictionary_count_u64, &dictionary_count,
            "ducknng: quack dictionary count exceeds supported size", errmsg) != 0) return -1;
    if (rows > 0 && dictionary_count == 0) {
        ducknng_quack_set_error(errmsg, "ducknng: non-empty quack dictionary has no values");
        return -1;
    }
    if (dictionary_count > rows) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack dictionary count exceeds its selected row count");
        return -1;
    }
    for (row = 0; row < rows; row++) {
        sel_t selected = 0;
        memcpy(&selected, selection + (size_t)row * sizeof(selected), sizeof(selected));
        if ((uint64_t)selected >= (uint64_t)dictionary_count) {
            ducknng_quack_set_error(errmsg, "ducknng: quack dictionary selection index is out of range");
            return -1;
        }
    }
    if (out_selection) *out_selection = selection;
    if (out_dictionary_count) *out_dictionary_count = dictionary_count;
    return 0;
}

/* Recursively skip a Vector object of the given node type without materializing
 * it, so the reader lands exactly past the FIELD_END that closes the vector. */
static int ducknng_quack_skip_vector_node(ducknng_quack_reader *r,
    const ducknng_quack_column_schema *node, idx_t rows, unsigned depth, char **errmsg) {
    const uint8_t *validity = NULL;
    size_t validity_len = 0;
    uint64_t vector_type = DUCKNNG_QUACK_VECTOR_FLAT;
    if (!node) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack vector type while skipping");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack vector nesting exceeds supported depth");
        return -1;
    }
    if (ducknng_quack_read_vector_type(r, &vector_type, errmsg) != 0) return -1;
    if (vector_type == DUCKNNG_QUACK_VECTOR_FSST) {
        ducknng_quack_set_error(errmsg, "ducknng: FSST quack vectors are unsupported");
        return -1;
    }
    if (vector_type == DUCKNNG_QUACK_VECTOR_CONSTANT) {
        return ducknng_quack_skip_vector_node(r, node, 1, depth + 1, errmsg);
    }
    if (vector_type == DUCKNNG_QUACK_VECTOR_DICTIONARY) {
        idx_t dictionary_count = 0;
        if (ducknng_quack_read_dictionary_header(r, rows, NULL, &dictionary_count, errmsg) != 0) return -1;
        return ducknng_quack_skip_vector_node(r, node, dictionary_count, depth + 1, errmsg);
    }
    if (vector_type == DUCKNNG_QUACK_VECTOR_SEQUENCE) {
        int64_t start = 0;
        int64_t increment = 0;
        if (!ducknng_quack_node_is_sequence_integer(node)) {
            ducknng_quack_set_error(errmsg,
                "ducknng: sequence quack vector requires an integer logical type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_SEQUENCE_START, errmsg) != 0 ||
            ducknng_quack_read_sleb128(r, &start, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_SEQUENCE_INCREMENT, errmsg) != 0 ||
            ducknng_quack_read_sleb128(r, &increment, errmsg) != 0) return -1;
        (void)start;
        (void)increment;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (ducknng_quack_read_flat_vector_prologue(r, &validity, &validity_len, errmsg) != 0) return -1;
    if (validity && validity_len != ducknng_quack_validity_bytes(rows)) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid quack validity mask size");
        return -1;
    }
    if (ducknng_quack_node_is_varlen(node)) {
        uint64_t n_items = 0;
        idx_t row;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_items, errmsg) != 0) return -1;
        if (n_items != (uint64_t)rows) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid variable-width quack vector payload");
            return -1;
        }
        for (row = 0; row < rows; row++) if (ducknng_quack_skip_string(r, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) {
        uint64_t nchild = 0;
        uint32_t i;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &nchild, errmsg) != 0) return -1;
        if (nchild != node->nchildren || !node->children) {
            ducknng_quack_set_error(errmsg, "ducknng: quack struct vector child count mismatch");
            return -1;
        }
        for (i = 0; i < node->nchildren; i++) {
            if (ducknng_quack_skip_vector_node(r, node->children[i], rows, depth + 1, errmsg) != 0) return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_MAP) {
        uint64_t child_size = 0;
        uint64_t n_entries = 0;
        idx_t child_rows = 0;
        uint64_t row;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/map vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &child_size, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_ENTRIES, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_entries, errmsg) != 0) return -1;
        if (n_entries != (uint64_t)rows) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list vector entry count mismatch");
            return -1;
        }
        if (ducknng_quack_uint64_to_idx(child_size, &child_rows,
                "ducknng: quack list child size exceeds supported size", errmsg) != 0) return -1;
        for (row = 0; row < n_entries; row++) {
            uint64_t off = 0, len = 0;
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_OFFSET, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &off, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_LENGTH, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &len, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
            if (off > child_size || len > child_size - off) {
                ducknng_quack_set_error(errmsg, "ducknng: quack list entry exceeds child vector size");
                return -1;
            }
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD, errmsg) != 0) return -1;
        if (ducknng_quack_skip_vector_node(r, node->children[0], child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ARRAY) {
        uint64_t asize = 0;
        idx_t child_rows;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &asize, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_CHILD, errmsg) != 0) return -1;
        if (asize != node->array_size) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector fixed-size mismatch");
            return -1;
        }
        if (ducknng_quack_idx_mul(rows, (idx_t)asize, &child_rows,
                "ducknng: quack array row count overflows supported size", errmsg) != 0) return -1;
        if (ducknng_quack_skip_vector_node(r, node->children[0], child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    {
        const uint8_t *blob = NULL;
        size_t blob_len = 0;
        size_t width = ducknng_quack_node_fixed_width(node);
        size_t expected_len = 0;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &blob, &blob_len, errmsg) != 0) return -1;
        if (width == 0 || ducknng_size_mul(width, (size_t)rows, &expected_len) != 0 ||
            blob_len != expected_len) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector payload");
            return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
}

static int ducknng_quack_validate_optional_chunk_types(ducknng_quack_reader *r,
    const ducknng_quack_schema *schema, char **errmsg) {
    uint16_t field_id = 0;
    uint64_t count = 0;
    uint64_t i;
    if (!r || !schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack chunk type state");
        return -1;
    }
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_CHUNK_TYPES) return 0;
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_TYPES, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &count, errmsg) != 0) return -1;
    if (count != (uint64_t)schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack chunk type count does not match the result schema");
        return -1;
    }
    for (i = 0; i < count; i++) {
        ducknng_quack_column_schema *chunk_type = NULL;
        int equal;
        if (ducknng_quack_read_type_node(r, 0, &chunk_type, errmsg) != 0) return -1;
        equal = ducknng_quack_node_type_equal(&schema->cols[i], chunk_type);
        ducknng_quack_node_free(chunk_type);
        if (!equal) {
            ducknng_quack_set_error(errmsg, "ducknng: quack chunk type does not match the result schema");
            return -1;
        }
    }
    return 0;
}

static int ducknng_quack_consume_chunk_wrapper_end(ducknng_quack_reader *r,
    int allow_legacy_omission, char **errmsg) {
    uint16_t field_id = 0;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_FIELD_END) {
        return ducknng_quack_read_u16le(r, &field_id, errmsg);
    }
    if (allow_legacy_omission) return 0;
    ducknng_quack_set_error(errmsg, "ducknng: missing quack data chunk wrapper terminator");
    return -1;
}

static int ducknng_quack_skip_data_chunk(ducknng_quack_reader *r,
    const ducknng_quack_schema *schema, idx_t *out_rows, char **errmsg) {
    uint64_t rows_u64 = 0;
    uint64_t ncols = 0;
    idx_t rows = 0;
    idx_t col;
    if (!schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack schema while skipping data chunk");
        return -1;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &rows_u64, errmsg) != 0) return -1;
    if (ducknng_quack_uint64_to_idx(rows_u64, &rows,
            "ducknng: quack row count exceeds supported size", errmsg) != 0) return -1;
    if (ducknng_quack_validate_optional_chunk_types(r, schema, errmsg) != 0 ||
        ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
    if (ncols != (uint64_t)schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
        return -1;
    }
    for (col = 0; col < schema->ncols; col++) {
        if (ducknng_quack_skip_vector_node(r, &schema->cols[col], rows, 0, errmsg) != 0) return -1;
    }
    if (out_rows) *out_rows = rows;
    return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
}

static int ducknng_quack_check_materialized_values(ducknng_quack_reader *r,
    idx_t count, int commit, char **errmsg) {
    uint64_t value_count = (uint64_t)count;
    if (!r || r->materialized_values > DUCKNNG_QUACK_MAX_MATERIALIZED_VALUES ||
        value_count > DUCKNNG_QUACK_MAX_MATERIALIZED_VALUES - r->materialized_values) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack materialized value count exceeds supported limit");
        return -1;
    }
    if (commit) r->materialized_values += value_count;
    return 0;
}

static void ducknng_quack_copy_validity_slice(duckdb_vector out_vec,
    const uint8_t *src_validity, idx_t src_offset, idx_t count) {
    uint64_t *dst_validity;
    idx_t row;
    if (!out_vec || !src_validity || count == 0) return;
    duckdb_vector_ensure_validity_writable(out_vec);
    dst_validity = duckdb_vector_get_validity(out_vec);
    for (row = 0; row < count; row++) {
        duckdb_validity_set_row_validity(dst_validity, row,
            ducknng_quack_vector_row_is_valid(src_validity, src_offset + row) ? true : false);
    }
}

static int ducknng_quack_decode_vector_into(ducknng_quack_reader *r,
    const ducknng_quack_column_schema *node, idx_t src_rows, idx_t offset,
    duckdb_vector out_vec, idx_t out_count, unsigned depth, char **errmsg);

static int ducknng_quack_decode_compressed_copy(ducknng_quack_reader *r,
    const ducknng_quack_column_schema *node, idx_t source_count,
    const uint8_t *dictionary_selection, idx_t offset,
    duckdb_vector out_vec, idx_t out_count, unsigned depth, char **errmsg) {
    duckdb_logical_type child_type = NULL;
    duckdb_vector source_vec = NULL;
    duckdb_selection_vector selection = NULL;
    sel_t *selection_data;
    idx_t row;
    int rc = -1;
    if (ducknng_quack_check_materialized_values(r, source_count, 0, errmsg) != 0 ||
        ducknng_quack_node_to_duckdb(node, depth, &child_type, errmsg) != 0) goto done;
    source_vec = duckdb_create_vector(child_type, source_count ? source_count : 1);
    if (!source_vec) {
        ducknng_quack_set_error(errmsg, "ducknng: failed to allocate quack compressed-vector source");
        goto done;
    }
    if (ducknng_quack_decode_vector_into(r, node, source_count, 0, source_vec,
            source_count, depth + 1, errmsg) != 0) goto done;
    if (out_count == 0) {
        rc = 0;
        goto done;
    }
    selection = duckdb_create_selection_vector(out_count);
    if (!selection) {
        ducknng_quack_set_error(errmsg, "ducknng: failed to allocate quack materialization selection");
        goto done;
    }
    selection_data = duckdb_selection_vector_get_data_ptr(selection);
    if (!selection_data) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack materialization selection data");
        goto done;
    }
    for (row = 0; row < out_count; row++) {
        if (dictionary_selection) {
            memcpy(&selection_data[row], dictionary_selection +
                (size_t)(offset + row) * sizeof(selection_data[row]), sizeof(selection_data[row]));
        } else {
            selection_data[row] = 0;
        }
        if ((uint64_t)selection_data[row] >= (uint64_t)source_count) {
            ducknng_quack_set_error(errmsg, "ducknng: quack materialization selection is out of range");
            goto done;
        }
    }
    duckdb_vector_copy_sel(source_vec, out_vec, selection, out_count, 0, 0);
    rc = 0;
done:
    if (selection) duckdb_destroy_selection_vector(selection);
    if (source_vec) duckdb_destroy_vector(&source_vec);
    if (child_type) duckdb_destroy_logical_type(&child_type);
    return rc;
}

static int ducknng_quack_decode_sequence(const ducknng_quack_column_schema *node,
    int64_t start, int64_t increment, idx_t offset, duckdb_vector out_vec,
    idx_t out_count, char **errmsg) {
    void *out_data;
    idx_t row;
    if (!node || !out_vec) return -1;
    if (!ducknng_quack_node_is_sequence_integer(node)) {
        ducknng_quack_set_error(errmsg,
            "ducknng: sequence quack vector requires an integer logical type");
        return -1;
    }
    out_data = duckdb_vector_get_data(out_vec);
    if (!out_data && out_count > 0) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack sequence output storage");
        return -1;
    }
    for (row = 0; row < out_count; row++) {
        uint64_t bits = (uint64_t)start + (uint64_t)increment * (uint64_t)(offset + row);
        int64_t value;
        memcpy(&value, &bits, sizeof(value));
        switch (node->logical_type_id) {
        case DUCKNNG_QUACK_LOGICAL_TINYINT:
            if (value < INT8_MIN || value > INT8_MAX) goto range_error;
            ((int8_t *)out_data)[row] = (int8_t)value;
            break;
        case DUCKNNG_QUACK_LOGICAL_SMALLINT:
            if (value < INT16_MIN || value > INT16_MAX) goto range_error;
            ((int16_t *)out_data)[row] = (int16_t)value;
            break;
        case DUCKNNG_QUACK_LOGICAL_INTEGER:
            if (value < INT32_MIN || value > INT32_MAX) goto range_error;
            ((int32_t *)out_data)[row] = (int32_t)value;
            break;
        case DUCKNNG_QUACK_LOGICAL_BIGINT:
            ((int64_t *)out_data)[row] = value;
            break;
        case DUCKNNG_QUACK_LOGICAL_UTINYINT:
            if (value < 0 || (uint64_t)value > UINT8_MAX) goto range_error;
            ((uint8_t *)out_data)[row] = (uint8_t)value;
            break;
        case DUCKNNG_QUACK_LOGICAL_USMALLINT:
            if (value < 0 || (uint64_t)value > UINT16_MAX) goto range_error;
            ((uint16_t *)out_data)[row] = (uint16_t)value;
            break;
        case DUCKNNG_QUACK_LOGICAL_UINTEGER:
            if (value < 0 || (uint64_t)value > UINT32_MAX) goto range_error;
            ((uint32_t *)out_data)[row] = (uint32_t)value;
            break;
        case DUCKNNG_QUACK_LOGICAL_UBIGINT:
            if (value < 0) goto range_error;
            ((uint64_t *)out_data)[row] = (uint64_t)value;
            break;
        default:
            return -1; /* guarded by ducknng_quack_node_is_sequence_integer */
        }
    }
    return 0;
range_error:
    ducknng_quack_set_error(errmsg, "ducknng: quack sequence value is out of range for its logical type");
    return -1;
}

/* Recursively decode a Vector object of the given node type into out_vec. The
 * decoder copies the source rows [offset, offset+out_count) into output rows
 * [0, out_count). Compressed vectors are materialized into flat destination
 * vectors through DuckDB's C selection-copy API. */
static int ducknng_quack_decode_vector_into(ducknng_quack_reader *r,
    const ducknng_quack_column_schema *node, idx_t src_rows, idx_t offset,
    duckdb_vector out_vec, idx_t out_count, unsigned depth, char **errmsg) {
    const uint8_t *src_validity = NULL;
    const uint8_t *dictionary_selection = NULL;
    size_t validity_len = 0;
    uint64_t vector_type = DUCKNNG_QUACK_VECTOR_FLAT;
    idx_t dictionary_count = 0;
    int64_t sequence_start = 0;
    int64_t sequence_increment = 0;
    if (!node || !out_vec) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack vector decoder state");
        return -1;
    }
    if (depth > DUCKNNG_QUACK_MAX_NESTING) {
        ducknng_quack_set_error(errmsg, "ducknng: quack vector nesting exceeds supported depth");
        return -1;
    }
    if (offset > src_rows || out_count > src_rows - offset) {
        ducknng_quack_set_error(errmsg, "ducknng: quack vector slice is out of range");
        return -1;
    }
    if (ducknng_quack_check_materialized_values(r, src_rows, 1, errmsg) != 0) return -1;
    if (ducknng_quack_read_vector_type(r, &vector_type, errmsg) != 0) return -1;
    if (vector_type == DUCKNNG_QUACK_VECTOR_FSST) {
        ducknng_quack_set_error(errmsg, "ducknng: FSST quack vectors are unsupported");
        return -1;
    }
    if (vector_type == DUCKNNG_QUACK_VECTOR_CONSTANT) {
        return ducknng_quack_decode_compressed_copy(r, node, 1, NULL,
            offset, out_vec, out_count, depth, errmsg);
    }
    if (vector_type == DUCKNNG_QUACK_VECTOR_DICTIONARY) {
        if (ducknng_quack_read_dictionary_header(r, src_rows, &dictionary_selection,
                &dictionary_count, errmsg) != 0) return -1;
        return ducknng_quack_decode_compressed_copy(r, node, dictionary_count,
            dictionary_selection, offset, out_vec, out_count, depth, errmsg);
    }
    if (vector_type == DUCKNNG_QUACK_VECTOR_SEQUENCE) {
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_SEQUENCE_START, errmsg) != 0 ||
            ducknng_quack_read_sleb128(r, &sequence_start, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_SEQUENCE_INCREMENT, errmsg) != 0 ||
            ducknng_quack_read_sleb128(r, &sequence_increment, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
        return ducknng_quack_decode_sequence(node, sequence_start, sequence_increment,
            offset, out_vec, out_count, errmsg);
    }
    if (ducknng_quack_read_flat_vector_prologue(r, &src_validity, &validity_len, errmsg) != 0) return -1;
    if (src_validity && validity_len != ducknng_quack_validity_bytes(src_rows)) {
        ducknng_quack_set_error(errmsg, "ducknng: invalid quack validity mask size");
        return -1;
    }

    if (ducknng_quack_node_is_varlen(node)) {
        uint64_t n_items = 0;
        idx_t row;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_items, errmsg) != 0) return -1;
        if (n_items != (uint64_t)src_rows) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid variable-width quack vector payload");
            return -1;
        }
        for (row = 0; row < src_rows; row++) {
            const uint8_t *data = NULL;
            size_t len = 0;
            if (ducknng_quack_read_blob_view(r, &data, &len, errmsg) != 0) return -1;
            if (row < offset || row >= offset + out_count) continue;
            if (src_validity && !ducknng_quack_vector_row_is_valid(src_validity, row)) {
                duckdb_vector_ensure_validity_writable(out_vec);
                duckdb_validity_set_row_invalid(duckdb_vector_get_validity(out_vec), row - offset);
            } else {
                idx_t len_idx = 0;
                if (ducknng_quack_size_to_idx(len, &len_idx,
                        "ducknng: quack string value exceeds supported size", errmsg) != 0) return -1;
                duckdb_unsafe_vector_assign_string_element_len(out_vec, row - offset,
                    (const char *)data, len_idx);
            }
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_STRUCT ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_UNION) {
        uint64_t nchild = 0;
        uint32_t i;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_STRUCT_CHILDREN, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &nchild, errmsg) != 0) return -1;
        if (nchild != node->nchildren || !node->children) {
            ducknng_quack_set_error(errmsg, "ducknng: quack struct vector child count mismatch");
            return -1;
        }
        if (src_validity && out_count > 0) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        for (i = 0; i < node->nchildren; i++) {
            duckdb_vector child = duckdb_struct_vector_get_child(out_vec, i);
            if (ducknng_quack_decode_vector_into(r, node->children[i], src_rows, offset, child, out_count, depth + 1, errmsg) != 0) return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_LIST ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_MAP) {
        uint64_t child_size = 0;
        uint64_t n_entries = 0;
        idx_t child_rows = 0;
        uint64_t row;
        duckdb_list_entry *out_entries = NULL;
        duckdb_vector child = NULL;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list/map vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &child_size, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_ENTRIES, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_entries, errmsg) != 0) return -1;
        if (n_entries != (uint64_t)src_rows) {
            ducknng_quack_set_error(errmsg, "ducknng: quack list vector entry count mismatch");
            return -1;
        }
        if (ducknng_quack_uint64_to_idx(child_size, &child_rows,
                "ducknng: quack list child size exceeds supported size", errmsg) != 0 ||
            ducknng_quack_check_materialized_values(r, child_rows, 0, errmsg) != 0) return -1;
        if (duckdb_list_vector_reserve(out_vec, child_rows) != DuckDBSuccess) {
            ducknng_quack_set_error(errmsg, "ducknng: failed to reserve quack list child capacity");
            return -1;
        }
        out_entries = (duckdb_list_entry *)duckdb_vector_get_data(out_vec);
        if (src_validity && out_count > 0) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        for (row = 0; row < n_entries; row++) {
            uint64_t off = 0, len = 0;
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_OFFSET, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &off, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_ENTRY_LENGTH, errmsg) != 0 ||
                ducknng_quack_read_uleb128(r, &len, errmsg) != 0 ||
                ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
            if (off > child_size || len > child_size - off) {
                ducknng_quack_set_error(errmsg, "ducknng: quack list entry exceeds child vector size");
                return -1;
            }
            if (row >= offset && row < offset + out_count) {
                out_entries[row - offset].offset = off;
                out_entries[row - offset].length = len;
            }
        }
        if (duckdb_list_vector_set_size(out_vec, child_rows) != DuckDBSuccess) {
            ducknng_quack_set_error(errmsg, "ducknng: failed to size quack list child vector");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_LIST_CHILD, errmsg) != 0) return -1;
        child = duckdb_list_vector_get_child(out_vec);
        if (ducknng_quack_decode_vector_into(r, node->children[0], child_rows, 0, child,
                child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ARRAY) {
        uint64_t asize = 0;
        duckdb_vector child = NULL;
        idx_t array_size = 0;
        idx_t child_rows;
        if (!node->children || node->nchildren != 1) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector has no child type");
            return -1;
        }
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_SIZE, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &asize, errmsg) != 0) return -1;
        if (asize != node->array_size) {
            ducknng_quack_set_error(errmsg, "ducknng: quack array vector fixed-size mismatch");
            return -1;
        }
        if (ducknng_quack_uint64_to_idx(asize, &array_size,
                "ducknng: quack array size exceeds supported size", errmsg) != 0) return -1;
        if (src_validity && out_count > 0) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_ARRAY_CHILD, errmsg) != 0) return -1;
        child = duckdb_array_vector_get_child(out_vec);
        if (ducknng_quack_idx_mul(src_rows, array_size, &child_rows,
                "ducknng: quack array row count overflows supported size", errmsg) != 0) return -1;
        if (ducknng_quack_decode_vector_into(r, node->children[0], child_rows, 0, child,
                child_rows, depth + 1, errmsg) != 0) return -1;
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    {
        const uint8_t *data_blob = NULL;
        size_t data_len = 0;
        size_t width = ducknng_quack_node_fixed_width(node);
        size_t expected_len = 0;
        size_t copy_offset = 0;
        size_t copy_len = 0;
        if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_VECTOR_DATA, errmsg) != 0 ||
            ducknng_quack_read_blob_view(r, &data_blob, &data_len, errmsg) != 0) return -1;
        if (width == 0 || ducknng_size_mul(width, (size_t)src_rows, &expected_len) != 0 ||
            data_len != expected_len) {
            ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector payload");
            return -1;
        }
        if (out_count > 0) {
            if (ducknng_size_mul(width, (size_t)offset, &copy_offset) != 0 ||
                ducknng_size_mul(width, (size_t)out_count, &copy_len) != 0 ||
                copy_offset > data_len || copy_len > data_len - copy_offset) {
                ducknng_quack_set_error(errmsg, "ducknng: invalid fixed-size quack vector slice");
                return -1;
            }
            memcpy(duckdb_vector_get_data(out_vec), data_blob + copy_offset, copy_len);
            if (src_validity) ducknng_quack_copy_validity_slice(out_vec, src_validity, offset, out_count);
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
}

/* True when any top-level column is a nested type, in which case the body scan
 * must decode whole source chunks rather than slicing mid-chunk. */
static int ducknng_quack_schema_has_nested(const ducknng_quack_schema *schema) {
    idx_t i;
    if (!schema) return 0;
    for (i = 0; i < schema->ncols; i++) {
        if (ducknng_quack_node_is_nested(&schema->cols[i])) return 1;
    }
    return 0;
}

static void ducknng_quack_node_free(ducknng_quack_column_schema *node);

/* Free a node's owned contents (name, enum labels, child names, and recursively
 * its child node structs) but NOT the node struct itself, so it serves both
 * heap-allocated child nodes and array-embedded top-level columns. */
static void ducknng_quack_node_free_contents(ducknng_quack_column_schema *node) {
    uint32_t i;
    if (!node) return;
    if (node->name) { duckdb_free(node->name); node->name = NULL; }
    if (node->enum_labels) {
        for (i = 0; i < node->enum_count; i++) {
            if (node->enum_labels[i]) duckdb_free(node->enum_labels[i]);
        }
        duckdb_free(node->enum_labels);
        node->enum_labels = NULL;
    }
    if (node->child_names) {
        for (i = 0; i < node->nchildren; i++) {
            if (node->child_names[i]) duckdb_free(node->child_names[i]);
        }
        duckdb_free(node->child_names);
        node->child_names = NULL;
    }
    if (node->children) {
        for (i = 0; i < node->nchildren; i++) ducknng_quack_node_free(node->children[i]);
        duckdb_free(node->children);
        node->children = NULL;
    }
    node->nchildren = 0;
    node->enum_count = 0;
}

static void ducknng_quack_node_free(ducknng_quack_column_schema *node) {
    if (!node) return;
    ducknng_quack_node_free_contents(node);
    duckdb_free(node);
}

void ducknng_quack_schema_reset(ducknng_quack_schema *schema) {
    idx_t i;
    if (!schema) return;
    if (schema->cols) {
        for (i = 0; i < schema->ncols; i++) ducknng_quack_node_free_contents(&schema->cols[i]);
        duckdb_free(schema->cols);
    }
    memset(schema, 0, sizeof(*schema));
}

typedef struct ducknng_quack_chunk_set {
    duckdb_data_chunk *chunks;
    idx_t count;
} ducknng_quack_chunk_set;

static void ducknng_quack_chunk_set_reset(ducknng_quack_chunk_set *set) {
    idx_t i;
    if (!set) return;
    if (set->chunks) {
        for (i = 0; i < set->count; i++) {
            if (set->chunks[i]) duckdb_destroy_data_chunk(&set->chunks[i]);
        }
        duckdb_free(set->chunks);
    }
    memset(set, 0, sizeof(*set));
}

/* Allocate and zero a chunk array holding *inout_max_chunks entries,
 * normalizing a zero request to one. The streaming collector and the
 * materialized-result path size their arrays identically and differ only in
 * how they fill them, so the checked sizing lives here once. */
static int ducknng_quack_alloc_chunk_array(uint64_t *inout_max_chunks,
    duckdb_data_chunk **out_chunks, char **errmsg) {
    size_t allocation_size;
    if (!inout_max_chunks || !out_chunks) {
        ducknng_quack_set_error(errmsg,
            "ducknng: missing quack chunk array output");
        return -1;
    }
    *out_chunks = NULL;
    if (*inout_max_chunks == 0) *inout_max_chunks = 1;
#if SIZE_MAX < UINT64_MAX
    if (*inout_max_chunks > SIZE_MAX) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack chunk collection size overflows supported size");
        return -1;
    }
#endif
    if (ducknng_size_mul(sizeof(**out_chunks), (size_t)*inout_max_chunks,
            &allocation_size) != 0) {
        ducknng_quack_set_error(errmsg,
            "ducknng: quack chunk collection size overflows supported size");
        return -1;
    }
    *out_chunks = (duckdb_data_chunk *)duckdb_malloc(allocation_size);
    if (!*out_chunks) {
        ducknng_quack_set_error(errmsg,
            "ducknng: out of memory collecting quack chunks");
        return -1;
    }
    memset(*out_chunks, 0, allocation_size);
    return 0;
}

static int ducknng_quack_collect_next_chunks(duckdb_result result,
    uint64_t max_chunks, ducknng_quack_chunk_set *out, char **errmsg) {
    uint64_t i;
    if (!out) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack chunk set output");
        return -1;
    }
    memset(out, 0, sizeof(*out));
    if (ducknng_quack_alloc_chunk_array(&max_chunks, &out->chunks,
            errmsg) != 0) return -1;
    for (i = 0; i < max_chunks; i++) {
        out->chunks[out->count] = ducknng_result_fetch_session_chunk(result);
        if (!out->chunks[out->count]) break;
        out->count++;
    }
    return 0;
}

int ducknng_result_next_chunks_to_quack_payload(duckdb_result result,
    uint64_t max_chunks, int include_schema, uint8_t **out_bytes, size_t *out_len,
    int *has_chunk, char **errmsg) {
    ducknng_quack_chunk_set set;
    int rc;
    if (has_chunk) *has_chunk = 0;
    if (ducknng_quack_collect_next_chunks(result, max_chunks, &set,
            errmsg) != 0) return -1;
    if (set.count == 0) {
        ducknng_quack_chunk_set_reset(&set);
        return 0;
    }
    rc = ducknng_quack_encode_result_payload(result, set.chunks, set.count,
        include_schema, out_bytes, out_len, errmsg);
    ducknng_quack_chunk_set_reset(&set);
    if (rc == 0 && has_chunk) *has_chunk = 1;
    return rc;
}

#ifndef DUCKNNG_CORE_ONLY
int ducknng_result_next_chunks_to_quack_message(duckdb_result result,
    uint64_t max_chunks, int include_schema, size_t prefix_size,
    nng_msg **out_msg, size_t *out_payload_len, int *has_chunk, char **errmsg) {
    ducknng_quack_chunk_set set;
    int rc;
    if (out_msg) *out_msg = NULL;
    if (out_payload_len) *out_payload_len = 0;
    if (has_chunk) *has_chunk = 0;
    if (!out_msg || !out_payload_len) {
        ducknng_quack_set_error(errmsg,
            "ducknng: missing quack message output pointers");
        return -1;
    }
    if (ducknng_quack_collect_next_chunks(result, max_chunks, &set,
            errmsg) != 0) return -1;
    if (set.count == 0) {
        ducknng_quack_chunk_set_reset(&set);
        return 0;
    }
    rc = ducknng_quack_encode_result_message(result, set.chunks, set.count,
        include_schema, prefix_size, out_msg, out_payload_len, errmsg);
    ducknng_quack_chunk_set_reset(&set);
    if (rc == 0 && has_chunk) *has_chunk = 1;
    return rc;
}
#endif

/* Materialized-result variant of the above: encodes up to max_chunks starting at
 * *inout_chunk_index using the non-deprecated duckdb_result_get_chunk /
 * duckdb_result_chunk_count, advancing the index. The streaming variant relies on
 * duckdb_stream_fetch_chunk, which yields nothing for a materialized duckdb_query
 * result; the upload client runs source_query with duckdb_query and iterates here.
 * Sets *has_chunk when a batch was produced; returns 0 with *has_chunk=0 once the
 * result is exhausted. */
int ducknng_result_materialized_chunks_to_quack_payload(duckdb_result result,
    idx_t *inout_chunk_index, uint64_t max_chunks, int include_schema,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg) {
    duckdb_data_chunk *chunks = NULL;
    idx_t chunk_count = 0;
    idx_t total = duckdb_result_chunk_count(result);
    uint64_t i;
    int rc;
    if (has_chunk) *has_chunk = 0;
    if (!inout_chunk_index) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack chunk index");
        return -1;
    }
    if (*inout_chunk_index >= total) return 0; /* exhausted */
    if (ducknng_quack_alloc_chunk_array(&max_chunks, &chunks, errmsg) != 0) {
        return -1;
    }
    for (i = 0; i < max_chunks && *inout_chunk_index < total; i++) {
        chunks[chunk_count] = duckdb_result_get_chunk(result, *inout_chunk_index);
        (*inout_chunk_index)++;
        if (!chunks[chunk_count]) continue;
        chunk_count++;
    }
    if (chunk_count == 0) {
        duckdb_free(chunks);
        return 0;
    }
    rc = ducknng_quack_encode_result_payload(result, chunks, chunk_count, include_schema,
        out_bytes, out_len, errmsg);
    for (i = 0; i < (uint64_t)chunk_count; i++) {
        if (chunks[i]) duckdb_destroy_data_chunk(&chunks[i]);
    }
    duckdb_free(chunks);
    if (rc == 0 && has_chunk) *has_chunk = 1;
    return rc;
}

int ducknng_result_next_chunk_to_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, int *has_chunk, char **errmsg) {
    return ducknng_result_next_chunks_to_quack_payload(result, 1, 1,
        out_bytes, out_len, has_chunk, errmsg);
}

int ducknng_result_empty_quack_payload(duckdb_result result,
    uint8_t **out_bytes, size_t *out_len, char **errmsg) {
    return ducknng_quack_encode_result_payload(result, NULL, 0, 1,
        out_bytes, out_len, errmsg);
}

#ifndef DUCKNNG_CORE_ONLY
int ducknng_result_empty_quack_message(duckdb_result result,
    size_t prefix_size, nng_msg **out_msg, size_t *out_payload_len,
    char **errmsg) {
    return ducknng_quack_encode_result_message(result, NULL, 0, 1,
        prefix_size, out_msg, out_payload_len, errmsg);
}
#endif

/* Parse the OUTER_RESULT_TYPES + OUTER_RESULT_NAMES sections into out_schema,
 * advancing the reader to the start of the results section. Shared by the
 * table-function bind path and the server upload append path. */
static int ducknng_quack_reader_parse_schema(ducknng_quack_reader *r,
    ducknng_quack_schema *out_schema, char **errmsg) {
    uint64_t ncols = 0;
    idx_t ncols_idx = 0;
    uint64_t i;
    size_t cols_bytes = 0;
    memset(out_schema, 0, sizeof(*out_schema));
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) goto fail;
    if (ncols > DUCKNNG_QUACK_MAX_STRUCT_MEMBERS) {
        ducknng_quack_set_error(errmsg, "ducknng: quack column count exceeds supported limit");
        goto fail;
    }
    if (ducknng_quack_uint64_to_idx(ncols, &ncols_idx,
            "ducknng: quack column count exceeds supported size", errmsg) != 0) goto fail;
    if (ncols_idx > 0) {
        if (ducknng_size_mul(sizeof(*out_schema->cols), (size_t)ncols_idx,
                &cols_bytes) != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: quack schema column allocation overflows supported size");
            goto fail;
        }
        out_schema->cols = (ducknng_quack_column_schema *)duckdb_malloc(cols_bytes);
        if (!out_schema->cols) {
            ducknng_quack_set_error(errmsg, "ducknng: out of memory allocating quack schema columns");
            goto fail;
        }
        memset(out_schema->cols, 0, cols_bytes);
    }
    out_schema->ncols = ncols_idx;
    for (i = 0; i < ncols; i++) {
        ducknng_quack_column_schema *node = NULL;
        if (ducknng_quack_read_type_node(r, 0, &node, errmsg) != 0) goto fail;
        /* Move the heap node's contents into the flat top-level column slot and
         * free the wrapper struct only (children/labels are now owned by cols[i]).
         * name is set from the separate NAMES section below. */
        out_schema->cols[i] = *node;
        duckdb_free(node);
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) goto fail;
    if (ncols != (uint64_t)out_schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack payload name/type count mismatch");
        goto fail;
    }
    for (i = 0; i < ncols; i++) {
        if (ducknng_quack_read_string_dup(r, &out_schema->cols[i].name, errmsg) != 0) goto fail;
    }
    return 0;
fail:
    ducknng_quack_schema_reset(out_schema);
    return -1;
}

int ducknng_quack_payload_parse_schema(const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, char **errmsg) {
    ducknng_quack_reader r;
    if (!payload || payload_len == 0 || !out_schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload for schema parse");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    return ducknng_quack_reader_parse_schema(&r, out_schema, errmsg);
}

/* From a reader positioned just after the schema names section, consume the
 * OUTER_RESULTS section (skipping chunk data), the results and top-level
 * FIELD_END terminators, and require the reader to be exactly at end of
 * payload. Returns the total row count. This is the strict structural tail
 * shared by the bind path and the upload append path, so both reject a
 * missing/replaced terminator or trailing bytes identically. */
static int ducknng_quack_reader_validate_results(ducknng_quack_reader *r,
    const ducknng_quack_schema *schema, idx_t max_chunk_rows,
    idx_t *out_row_count, char **errmsg) {
    uint16_t field_id = 0;
    idx_t row_count = 0;
    if (out_row_count) *out_row_count = 0;
    if (ducknng_quack_peek_u16le(r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULTS) {
        uint64_t n_results = 0;
        uint64_t chunk_idx;
        if (ducknng_quack_read_u16le(r, &field_id, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &n_results, errmsg) != 0) return -1;
        for (chunk_idx = 0; chunk_idx < n_results; chunk_idx++) {
            uint8_t present = 0;
            idx_t chunk_rows = 0;
            if (ducknng_quack_read_boolean(r, &present,
                    "ducknng: quack chunk presence flag is not boolean",
                    errmsg) != 0) return -1;
            if (!present) {
                ducknng_quack_set_error(errmsg, "ducknng: quack payload contained a NULL chunk pointer");
                return -1;
            }
            if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
                ducknng_quack_skip_data_chunk(r, schema, &chunk_rows, errmsg) != 0 ||
                ducknng_quack_consume_chunk_wrapper_end(r, chunk_idx + 1 < n_results,
                    errmsg) != 0) return -1;
            /* The skip path does not enforce the decode-time per-chunk vector
             * capacity that scan_next imposes. Callers that will decode the
             * chunks (the upload append path) pass max_chunk_rows so an
             * oversized later chunk fails preflight instead of causing a
             * partial append; the bind/row-count path passes 0 (no cap). */
            if (max_chunk_rows > 0 && chunk_rows > max_chunk_rows) {
                ducknng_quack_set_error(errmsg, "ducknng: quack chunk exceeds the supported vector size");
                return -1;
            }
            if (ducknng_quack_idx_add(row_count, chunk_rows, &row_count,
                    "ducknng: quack row count overflows supported size", errmsg) != 0) return -1;
        }
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    if (r->off != r->len) {
        ducknng_quack_set_error(errmsg, "ducknng: trailing bytes in quack payload");
        return -1;
    }
    if (out_row_count) *out_row_count = row_count;
    return 0;
}

int ducknng_quack_payload_bind_columns(duckdb_bind_info info,
    const uint8_t *payload, size_t payload_len,
    ducknng_quack_schema *out_schema, idx_t *out_row_count, char **errmsg) {
    ducknng_quack_reader r;
    uint64_t i;
    idx_t row_count = 0;
    if (out_row_count) *out_row_count = 0;
    if (!info || !payload || payload_len == 0 || !out_schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload for bind");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    if (ducknng_quack_reader_parse_schema(&r, out_schema, errmsg) != 0) return -1;
    for (i = 0; i < (uint64_t)out_schema->ncols; i++) {
        duckdb_logical_type bind_type = NULL;
        if (ducknng_quack_node_to_duckdb(&out_schema->cols[i], 0, &bind_type, errmsg) != 0) goto fail;
        duckdb_bind_add_result_column(info, out_schema->cols[i].name ? out_schema->cols[i].name : "", bind_type);
        duckdb_destroy_logical_type(&bind_type);
    }
    if (ducknng_quack_reader_validate_results(&r, out_schema, 0, &row_count, errmsg) != 0) goto fail;
    if (out_row_count) *out_row_count = row_count;
    return 0;
fail:
    ducknng_quack_schema_reset(out_schema);
    return -1;
}

/* Deep structural equality of two DuckDB logical types: same type id plus, for
 * parameterized and nested types, matching decimal precision/scale, list/array
 * child, array size, struct/union child names and types, map key/value, and
 * enum labels. The upload append path requires this so a batch column is only
 * appended into a target column of exactly the same type, never silently cast
 * (e.g. BIGINT into INTEGER, or DECIMAL(9,2) into DECIMAL(18,4)). */
static int ducknng_quack_logical_type_equal(duckdb_logical_type a, duckdb_logical_type b) {
    duckdb_type ta;
    duckdb_type tb;
    if (!a || !b) return 0;
    ta = duckdb_get_type_id(a);
    tb = duckdb_get_type_id(b);
    if (ta != tb) return 0;
    switch (ta) {
    case DUCKDB_TYPE_DECIMAL:
        return duckdb_decimal_width(a) == duckdb_decimal_width(b) &&
               duckdb_decimal_scale(a) == duckdb_decimal_scale(b);
    case DUCKDB_TYPE_LIST: {
        duckdb_logical_type ca = duckdb_list_type_child_type(a);
        duckdb_logical_type cb = duckdb_list_type_child_type(b);
        int eq = ducknng_quack_logical_type_equal(ca, cb);
        if (ca) duckdb_destroy_logical_type(&ca);
        if (cb) duckdb_destroy_logical_type(&cb);
        return eq;
    }
    case DUCKDB_TYPE_ARRAY: {
        duckdb_logical_type ca = duckdb_array_type_child_type(a);
        duckdb_logical_type cb = duckdb_array_type_child_type(b);
        int eq = duckdb_array_type_array_size(a) == duckdb_array_type_array_size(b) &&
                 ducknng_quack_logical_type_equal(ca, cb);
        if (ca) duckdb_destroy_logical_type(&ca);
        if (cb) duckdb_destroy_logical_type(&cb);
        return eq;
    }
    case DUCKDB_TYPE_MAP: {
        duckdb_logical_type ka = duckdb_map_type_key_type(a);
        duckdb_logical_type kb = duckdb_map_type_key_type(b);
        duckdb_logical_type va = duckdb_map_type_value_type(a);
        duckdb_logical_type vb = duckdb_map_type_value_type(b);
        int eq = ducknng_quack_logical_type_equal(ka, kb) &&
                 ducknng_quack_logical_type_equal(va, vb);
        if (ka) duckdb_destroy_logical_type(&ka);
        if (kb) duckdb_destroy_logical_type(&kb);
        if (va) duckdb_destroy_logical_type(&va);
        if (vb) duckdb_destroy_logical_type(&vb);
        return eq;
    }
    case DUCKDB_TYPE_STRUCT: {
        idx_t n = duckdb_struct_type_child_count(a);
        idx_t i;
        if (n != duckdb_struct_type_child_count(b)) return 0;
        for (i = 0; i < n; i++) {
            char *na = duckdb_struct_type_child_name(a, i);
            char *nb = duckdb_struct_type_child_name(b, i);
            duckdb_logical_type ca = duckdb_struct_type_child_type(a, i);
            duckdb_logical_type cb = duckdb_struct_type_child_type(b, i);
            int eq = na && nb && strcmp(na, nb) == 0 &&
                     ducknng_quack_logical_type_equal(ca, cb);
            if (na) duckdb_free(na);
            if (nb) duckdb_free(nb);
            if (ca) duckdb_destroy_logical_type(&ca);
            if (cb) duckdb_destroy_logical_type(&cb);
            if (!eq) return 0;
        }
        return 1;
    }
    case DUCKDB_TYPE_UNION: {
        idx_t n = duckdb_union_type_member_count(a);
        idx_t i;
        if (n != duckdb_union_type_member_count(b)) return 0;
        for (i = 0; i < n; i++) {
            char *na = duckdb_union_type_member_name(a, i);
            char *nb = duckdb_union_type_member_name(b, i);
            duckdb_logical_type ca = duckdb_union_type_member_type(a, i);
            duckdb_logical_type cb = duckdb_union_type_member_type(b, i);
            int eq = na && nb && strcmp(na, nb) == 0 &&
                     ducknng_quack_logical_type_equal(ca, cb);
            if (na) duckdb_free(na);
            if (nb) duckdb_free(nb);
            if (ca) duckdb_destroy_logical_type(&ca);
            if (cb) duckdb_destroy_logical_type(&cb);
            if (!eq) return 0;
        }
        return 1;
    }
    case DUCKDB_TYPE_ENUM: {
        uint32_t n = duckdb_enum_dictionary_size(a);
        uint32_t i;
        if (n != duckdb_enum_dictionary_size(b)) return 0;
        for (i = 0; i < n; i++) {
            char *la = duckdb_enum_dictionary_value(a, i);
            char *lb = duckdb_enum_dictionary_value(b, i);
            int eq = la && lb && strcmp(la, lb) == 0;
            if (la) duckdb_free(la);
            if (lb) duckdb_free(lb);
            if (!eq) return 0;
        }
        return 1;
    }
    default:
        /* Same top-level id and no parameters to compare. */
        return 1;
    }
}

/* Record a duckdb_appender error into *errmsg via the non-deprecated
 * error-data API (duckdb_appender_error is deprecated and absent under
 * DUCKDB_API_NO_DEPRECATED). */
static void ducknng_quack_set_appender_error(duckdb_appender appender,
    const char *fallback, char **errmsg) {
    duckdb_error_data ed = duckdb_appender_error_data(appender);
    const char *msg = ed ? duckdb_error_data_message(ed) : NULL;
    ducknng_quack_set_error(errmsg, msg && msg[0] ? msg : fallback);
    if (ed) duckdb_destroy_error_data(&ed);
}

int ducknng_quack_payload_append_to_appender(duckdb_appender appender,
    const uint8_t *payload, size_t payload_len,
    const char *const *expected_names, idx_t expected_name_count,
    uint64_t *inout_rows, char **errmsg) {
    ducknng_quack_schema schema;
    duckdb_logical_type *types = NULL;
    duckdb_data_chunk chunk = NULL;
    ducknng_quack_reader r;
    size_t offset = 0;
    uint64_t remaining = 0;
    uint64_t appended = 0;
    idx_t expected_rows = 0;
    idx_t appender_cols;
    idx_t i;
    int rc = -1;
    if (!appender || !payload || payload_len == 0) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack append inputs");
        return -1;
    }
    memset(&schema, 0, sizeof(schema));
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    /* Parse the schema, then require the whole payload to be structurally
     * valid (terminators + no trailing bytes) before touching the appender. */
    if (ducknng_quack_reader_parse_schema(&r, &schema, errmsg) != 0) return -1;
    if (schema.ncols == 0) {
        ducknng_quack_set_error(errmsg, "ducknng: quack append payload has no columns");
        goto done;
    }
    if (ducknng_quack_reader_validate_results(&r, &schema, (idx_t)duckdb_vector_size(),
            &expected_rows, errmsg) != 0) goto done;
    /* The batch schema must match the target appender exactly: same column
     * count and same top-level type per column. DuckDB's append-time cast
     * would otherwise silently coerce e.g. BIGINT batch data into an INTEGER
     * column, so reject mismatches here rather than relying on the append. */
    appender_cols = duckdb_appender_column_count(appender);
    if ((uint64_t)appender_cols != (uint64_t)schema.ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack append column count does not match the target table");
        goto done;
    }
    /* When target column names are supplied, the batch column names must match
     * them exactly and in order. DuckDB's appender appends by ordinal, so
     * without this a same-typed but differently-named/ordered batch (e.g.
     * target (a,b) with batch (b,a)) would silently write values into the wrong
     * columns. */
    if (expected_names) {
        if ((uint64_t)expected_name_count != (uint64_t)schema.ncols) {
            ducknng_quack_set_error(errmsg, "ducknng: quack append column count does not match the target table");
            goto done;
        }
        for (i = 0; i < schema.ncols; i++) {
            const char *batch_name = schema.cols[i].name ? schema.cols[i].name : "";
            const char *want = expected_names[i] ? expected_names[i] : "";
            if (strcmp(batch_name, want) != 0) {
                ducknng_quack_set_error(errmsg, "ducknng: quack append column names do not match the target table");
                goto done;
            }
        }
    }
    types = (duckdb_logical_type *)duckdb_malloc(sizeof(*types) * (size_t)schema.ncols);
    if (!types) {
        ducknng_quack_set_error(errmsg, "ducknng: out of memory allocating quack append column types");
        goto done;
    }
    memset(types, 0, sizeof(*types) * (size_t)schema.ncols);
    for (i = 0; i < schema.ncols; i++) {
        duckdb_logical_type target;
        int eq;
        if (ducknng_quack_node_to_duckdb(&schema.cols[i], 0, &types[i], errmsg) != 0) goto done;
        target = duckdb_appender_column_type(appender, i);
        eq = ducknng_quack_logical_type_equal(types[i], target);
        if (target) duckdb_destroy_logical_type(&target);
        if (!eq) {
            ducknng_quack_set_error(errmsg, "ducknng: quack append column type does not match the target table");
            goto done;
        }
    }
    chunk = duckdb_create_data_chunk(types, (idx_t)schema.ncols);
    if (!chunk) {
        ducknng_quack_set_error(errmsg, "ducknng: failed to allocate data chunk for quack append");
        goto done;
    }
    if (ducknng_quack_payload_scan_begin(payload, payload_len, &schema, &offset, &remaining, errmsg) != 0) goto done;
    while (remaining > 0) {
        idx_t chunk_size;
        duckdb_data_chunk_reset(chunk);
        if (ducknng_quack_payload_scan_next(chunk, payload, payload_len, &schema,
                &offset, &remaining, errmsg) != 0) goto done;
        chunk_size = duckdb_data_chunk_get_size(chunk);
        if (chunk_size == 0) continue;
        if (duckdb_append_data_chunk(appender, chunk) == DuckDBError) {
            ducknng_quack_set_appender_error(appender, "ducknng: quack append to table failed", errmsg);
            goto done;
        }
        appended += (uint64_t)chunk_size;
    }
    if (appended != (uint64_t)expected_rows) {
        ducknng_quack_set_error(errmsg, "ducknng: quack append row count did not match the payload");
        goto done;
    }
    if (inout_rows) *inout_rows += appended;
    rc = 0;
done:
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    if (types) {
        for (i = 0; i < schema.ncols; i++) {
            if (types[i]) duckdb_destroy_logical_type(&types[i]);
        }
        duckdb_free(types);
    }
    ducknng_quack_schema_reset(&schema);
    return rc;
}

int ducknng_quack_payload_read_row_count(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *out_row_count, char **errmsg) {
    ducknng_quack_reader r;
    uint16_t field_id = 0;
    uint64_t n_results = 0;
    uint64_t chunk_idx;
    idx_t row_count = 0;
    if (out_row_count) *out_row_count = 0;
    if (!payload || payload_len == 0 || !schema) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload row-count inputs");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULT_TYPES) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_skip_type_list(&r, (uint64_t)schema->ncols, errmsg) != 0 ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_skip_string_list(&r, (uint64_t)schema->ncols, errmsg) != 0) return -1;
    }
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_OUTER_RESULTS) {
        if (out_row_count) *out_row_count = 0;
        return 0;
    }
    if (ducknng_quack_read_u16le(&r, &field_id, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &n_results, errmsg) != 0) return -1;
    for (chunk_idx = 0; chunk_idx < n_results; chunk_idx++) {
        uint8_t present = 0;
        idx_t chunk_rows = 0;
        if (ducknng_quack_read_boolean(&r, &present,
                "ducknng: quack chunk presence flag is not boolean",
                errmsg) != 0 || !present ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
            ducknng_quack_skip_data_chunk(&r, schema, &chunk_rows, errmsg) != 0 ||
            ducknng_quack_consume_chunk_wrapper_end(&r, chunk_idx + 1 < n_results,
                errmsg) != 0) return -1;
        if (ducknng_quack_idx_add(row_count, chunk_rows, &row_count,
                "ducknng: quack row count overflows supported size", errmsg) != 0) return -1;
    }
    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    if (r.off != r.len) {
        ducknng_quack_set_error(errmsg, "ducknng: trailing bytes in quack payload");
        return -1;
    }
    if (out_row_count) *out_row_count = row_count;
    return 0;
}

static int ducknng_quack_decode_data_chunk_slice(ducknng_quack_reader *r,
    duckdb_data_chunk output, const ducknng_quack_schema *schema,
    idx_t offset, idx_t *out_rows, char **errmsg) {
    uint64_t rows_u64 = 0;
    uint64_t ncols = 0;
    idx_t rows;
    idx_t copy_count;
    idx_t col;
    int has_nested;
    if (out_rows) *out_rows = 0;
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_ROWS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &rows_u64, errmsg) != 0) return -1;
    if (ducknng_quack_uint64_to_idx(rows_u64, &rows,
            "ducknng: quack row count exceeds supported size", errmsg) != 0) return -1;
    if (rows > duckdb_vector_size()) {
        ducknng_quack_set_error(errmsg, "ducknng: quack chunk exceeds output vector capacity");
        return -1;
    }
    if (out_rows) *out_rows = rows;
    has_nested = ducknng_quack_schema_has_nested(schema);
    if (offset >= rows) {
        if (ducknng_quack_validate_optional_chunk_types(r, schema, errmsg) != 0 ||
            ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
            ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
        if (ncols != (uint64_t)schema->ncols) {
            ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
            return -1;
        }
        for (col = 0; col < schema->ncols; col++) {
            if (ducknng_quack_skip_vector_node(r, &schema->cols[col], rows, 0, errmsg) != 0) return -1;
        }
        return ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg);
    }
    copy_count = rows - offset;
    if (copy_count > duckdb_vector_size()) copy_count = duckdb_vector_size();
    if (has_nested) {
        /* Nested vectors cannot be sliced mid-chunk; the scan path advances one
         * whole source chunk per call, so a nested chunk must start at row 0 and
         * fit in one output chunk (source chunks are already <= vector size). */
        if (offset != 0) {
            ducknng_quack_set_error(errmsg, "ducknng: nested quack chunk cannot resume mid-chunk");
            return -1;
        }
        copy_count = rows;
    }
    if (ducknng_quack_validate_optional_chunk_types(r, schema, errmsg) != 0 ||
        ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_CHUNK_COLUMNS, errmsg) != 0 ||
        ducknng_quack_read_uleb128(r, &ncols, errmsg) != 0) return -1;
    if (ncols != (uint64_t)schema->ncols) {
        ducknng_quack_set_error(errmsg, "ducknng: quack data chunk column count mismatch");
        return -1;
    }
    for (col = 0; col < schema->ncols; col++) {
        duckdb_vector out_vec = duckdb_data_chunk_get_vector(output, col);
        if (ducknng_quack_decode_vector_into(r, &schema->cols[col], rows, offset, out_vec, copy_count, 0, errmsg) != 0) return -1;
    }
    if (ducknng_quack_read_expect_field(r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    duckdb_data_chunk_set_size(output, copy_count);
    return 0;
}

int ducknng_quack_payload_scan_begin(const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, size_t *inout_offset, uint64_t *out_remaining,
    char **errmsg) {
    ducknng_quack_reader r;
    uint16_t field_id = 0;
    uint64_t n_results = 0;
    if (inout_offset) *inout_offset = 0;
    if (out_remaining) *out_remaining = 0;
    if (!payload || payload_len == 0 || !schema || !inout_offset || !out_remaining) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload scan state");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULT_TYPES) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_skip_type_list(&r, (uint64_t)schema->ncols, errmsg) != 0 ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_skip_string_list(&r, (uint64_t)schema->ncols, errmsg) != 0) return -1;
    }
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_OUTER_RESULTS) {
        *inout_offset = r.off;
        *out_remaining = 0;
        return 0;
    }
    if (ducknng_quack_read_u16le(&r, &field_id, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &n_results, errmsg) != 0) return -1;
    *inout_offset = r.off;
    *out_remaining = n_results;
    return 0;
}

int ducknng_quack_payload_scan_next(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len, const ducknng_quack_schema *schema,
    size_t *inout_offset, uint64_t *inout_remaining, char **errmsg) {
    ducknng_quack_reader r;
    uint8_t present = 0;
    idx_t chunk_rows = 0;
    if (!output || !payload || payload_len == 0 || !schema || !inout_offset || !inout_remaining) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload scan inputs");
        return -1;
    }
    if (*inout_remaining == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return 0;
    }
    if (*inout_offset > payload_len) {
        ducknng_quack_set_error(errmsg, "ducknng: quack payload scan offset is out of range");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    r.off = *inout_offset;
    if (ducknng_quack_read_boolean(&r, &present,
            "ducknng: quack chunk presence flag is not boolean",
            errmsg) != 0 || !present ||
        ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0 ||
        ducknng_quack_decode_data_chunk_slice(&r, output, schema, 0, &chunk_rows, errmsg) != 0 ||
        ducknng_quack_consume_chunk_wrapper_end(&r, *inout_remaining > 1, errmsg) != 0) return -1;
    *inout_offset = r.off;
    (*inout_remaining)--;
    if (*inout_remaining == 0) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
        *inout_offset = r.off;
    }
    (void)chunk_rows;
    return 0;
}

int ducknng_quack_payload_scan(duckdb_data_chunk output,
    const uint8_t *payload, size_t payload_len,
    const ducknng_quack_schema *schema, idx_t *inout_offset, char **errmsg) {
    ducknng_quack_reader r;
    uint16_t field_id = 0;
    uint64_t n_results = 0;
    uint64_t chunk_idx;
    idx_t global_offset = inout_offset ? *inout_offset : 0;
    idx_t seen_rows = 0;
    if (!output || !payload || payload_len == 0 || !schema || !inout_offset) {
        ducknng_quack_set_error(errmsg, "ducknng: missing quack payload scan inputs");
        return -1;
    }
    memset(&r, 0, sizeof(r));
    r.data = payload;
    r.len = payload_len;
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id == DUCKNNG_QUACK_OUTER_RESULT_TYPES) {
        if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_TYPES, errmsg) != 0 ||
            ducknng_quack_skip_type_list(&r, (uint64_t)schema->ncols, errmsg) != 0 ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_OUTER_RESULT_NAMES, errmsg) != 0 ||
            ducknng_quack_skip_string_list(&r, (uint64_t)schema->ncols, errmsg) != 0) return -1;
    }
    if (ducknng_quack_peek_u16le(&r, &field_id, errmsg) != 0) return -1;
    if (field_id != DUCKNNG_QUACK_OUTER_RESULTS) {
        duckdb_data_chunk_set_size(output, 0);
        return 0;
    }
    if (ducknng_quack_read_u16le(&r, &field_id, errmsg) != 0 ||
        ducknng_quack_read_uleb128(&r, &n_results, errmsg) != 0) return -1;
    for (chunk_idx = 0; chunk_idx < n_results; chunk_idx++) {
        uint8_t present = 0;
        idx_t chunk_rows = 0;
        idx_t local_offset;
        idx_t next_seen = 0;
        idx_t next_offset = 0;
        if (ducknng_quack_read_boolean(&r, &present,
                "ducknng: quack chunk presence flag is not boolean",
                errmsg) != 0 || !present ||
            ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_CHUNK_WRAPPER, errmsg) != 0) return -1;
        if (global_offset >= seen_rows) local_offset = global_offset - seen_rows;
        else local_offset = 0;
        if (ducknng_quack_decode_data_chunk_slice(&r, output, schema, local_offset, &chunk_rows, errmsg) != 0 ||
            ducknng_quack_consume_chunk_wrapper_end(&r, chunk_idx + 1 < n_results,
                errmsg) != 0) return -1;
        if (ducknng_quack_idx_add(seen_rows, chunk_rows, &next_seen,
                "ducknng: quack row count overflows supported size", errmsg) != 0) return -1;
        if (global_offset < next_seen) {
            if (ducknng_quack_idx_add(global_offset, duckdb_data_chunk_get_size(output), &next_offset,
                    "ducknng: quack scan offset overflows supported size", errmsg) != 0) return -1;
            *inout_offset = next_offset;
            return 0;
        }
        seen_rows = next_seen;
    }
    if (ducknng_quack_read_expect_field(&r, DUCKNNG_QUACK_FIELD_END, errmsg) != 0) return -1;
    duckdb_data_chunk_set_size(output, 0);
    return 0;
}
