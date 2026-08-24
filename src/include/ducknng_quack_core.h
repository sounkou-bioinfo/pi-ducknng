#pragma once
#include <stddef.h>
#include <stdint.h>

/* Dependency-free byte kernel for the DuckDB BinarySerializer subset used by
 * ducknng Quack batches. It owns no memory and reports only stable error kinds;
 * DuckDB adapters add user-facing messages and allocation policy. */
typedef enum ducknng_qk_status {
    DUCKNNG_QK_OK = 0,
    DUCKNNG_QK_INVALID = 1,
    DUCKNNG_QK_OVERFLOW = 2,
    DUCKNNG_QK_NO_SPACE = 3,
    DUCKNNG_QK_TRUNCATED = 4
} ducknng_qk_status;

/* DuckDB LogicalTypeId values serialized by the Quack wire subset. These are
 * wire identifiers, not DuckDB C API enum dependencies. The DuckDB adapter
 * verifies and converts them explicitly. */
#define DUCKNNG_QUACK_LOGICAL_BOOLEAN 10
#define DUCKNNG_QUACK_LOGICAL_TINYINT 11
#define DUCKNNG_QUACK_LOGICAL_SMALLINT 12
#define DUCKNNG_QUACK_LOGICAL_INTEGER 13
#define DUCKNNG_QUACK_LOGICAL_BIGINT 14
#define DUCKNNG_QUACK_LOGICAL_DATE 15
#define DUCKNNG_QUACK_LOGICAL_TIME 16
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC 17
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS 18
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP 19
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS 20
#define DUCKNNG_QUACK_LOGICAL_DECIMAL 21
#define DUCKNNG_QUACK_LOGICAL_FLOAT 22
#define DUCKNNG_QUACK_LOGICAL_DOUBLE 23
#define DUCKNNG_QUACK_LOGICAL_CHAR 24
#define DUCKNNG_QUACK_LOGICAL_VARCHAR 25
#define DUCKNNG_QUACK_LOGICAL_BLOB 26
#define DUCKNNG_QUACK_LOGICAL_INTERVAL 27
#define DUCKNNG_QUACK_LOGICAL_UTINYINT 28
#define DUCKNNG_QUACK_LOGICAL_USMALLINT 29
#define DUCKNNG_QUACK_LOGICAL_UINTEGER 30
#define DUCKNNG_QUACK_LOGICAL_UBIGINT 31
#define DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ 32
#define DUCKNNG_QUACK_LOGICAL_TIME_TZ 34
#define DUCKNNG_QUACK_LOGICAL_TIME_NS 35
#define DUCKNNG_QUACK_LOGICAL_UHUGEINT 49
#define DUCKNNG_QUACK_LOGICAL_HUGEINT 50
#define DUCKNNG_QUACK_LOGICAL_UUID 54
#define DUCKNNG_QUACK_LOGICAL_STRUCT 100
#define DUCKNNG_QUACK_LOGICAL_LIST 101
#define DUCKNNG_QUACK_LOGICAL_MAP 102
#define DUCKNNG_QUACK_LOGICAL_ENUM 104
#define DUCKNNG_QUACK_LOGICAL_UNION 107
#define DUCKNNG_QUACK_LOGICAL_ARRAY 108

#define DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL 2u
#define DUCKNNG_QUACK_EXTRA_TYPE_LIST 4u
#define DUCKNNG_QUACK_EXTRA_TYPE_STRUCT 5u
#define DUCKNNG_QUACK_EXTRA_TYPE_ENUM 6u
#define DUCKNNG_QUACK_EXTRA_TYPE_ARRAY 9u
#define DUCKNNG_QUACK_MAX_NESTING 64u
#define DUCKNNG_QUACK_MAX_STRUCT_MEMBERS 65536u
#define DUCKNNG_QUACK_MAX_ENUM_VALUES (1u << 24)
#define DUCKNNG_QUACK_MAX_MATERIALIZED_VALUES (1u << 22)

typedef struct ducknng_quack_column_schema {
    char *name;
    int logical_type_id;
    uint8_t decimal_width;
    uint8_t decimal_scale;
    uint32_t array_size;
    uint32_t enum_count;
    char **enum_labels;
    uint32_t nchildren;
    struct ducknng_quack_column_schema **children;
    char **child_names;
} ducknng_quack_column_schema;

typedef enum ducknng_qk_writer_mode {
    DUCKNNG_QK_WRITER_MEASURE = 1,
    DUCKNNG_QK_WRITER_FIXED = 2
} ducknng_qk_writer_mode;

typedef struct ducknng_qk_writer {
    uint8_t *data;
    size_t capacity;
    size_t position;
    ducknng_qk_writer_mode mode;
    ducknng_qk_status status;
} ducknng_qk_writer;

void ducknng_qk_writer_init_measure(ducknng_qk_writer *writer);
void ducknng_qk_writer_init_fixed(ducknng_qk_writer *writer,
    uint8_t *destination, size_t capacity);
size_t ducknng_qk_writer_size(const ducknng_qk_writer *writer);
ducknng_qk_status ducknng_qk_writer_status(
    const ducknng_qk_writer *writer);
int ducknng_qk_write_bytes(ducknng_qk_writer *writer,
    const void *source, size_t size);
int ducknng_qk_write_u8(ducknng_qk_writer *writer, uint8_t value);
int ducknng_qk_write_u16le(ducknng_qk_writer *writer, uint16_t value);
int ducknng_qk_write_field(ducknng_qk_writer *writer, uint16_t field);
int ducknng_qk_write_uleb128(ducknng_qk_writer *writer, uint64_t value);
int ducknng_qk_write_sleb128(ducknng_qk_writer *writer, int64_t value);
int ducknng_qk_write_counted(ducknng_qk_writer *writer,
    const void *source, size_t size);

typedef struct ducknng_qk_reader {
    const uint8_t *data;
    size_t len;
    size_t off;
    uint64_t materialized_values;
    ducknng_qk_status status;
} ducknng_qk_reader;

void ducknng_qk_reader_init(ducknng_qk_reader *reader,
    const uint8_t *data, size_t size);
size_t ducknng_qk_reader_remaining(const ducknng_qk_reader *reader);
ducknng_qk_status ducknng_qk_reader_status(
    const ducknng_qk_reader *reader);
int ducknng_qk_read_u8(ducknng_qk_reader *reader, uint8_t *out);
int ducknng_qk_read_boolean(ducknng_qk_reader *reader, uint8_t *out);
int ducknng_qk_peek_u16le(ducknng_qk_reader *reader, uint16_t *out);
int ducknng_qk_read_u16le(ducknng_qk_reader *reader, uint16_t *out);
int ducknng_qk_read_uleb128(ducknng_qk_reader *reader, uint64_t *out);
int ducknng_qk_read_sleb128(ducknng_qk_reader *reader, int64_t *out);
int ducknng_qk_read_counted(ducknng_qk_reader *reader,
    const uint8_t **out_data, size_t *out_size);
int ducknng_qk_skip(ducknng_qk_reader *reader, size_t size);

/* Dependency-free type-tree classification and structural validation. Error
 * text, when requested, points to a static string owned by the core. */
size_t ducknng_qk_validity_bytes(uint64_t rows);
int ducknng_qk_type_is_nested(const ducknng_quack_column_schema *node);
int ducknng_qk_type_is_varlen(const ducknng_quack_column_schema *node);
size_t ducknng_qk_type_fixed_width(const ducknng_quack_column_schema *node);
int ducknng_qk_type_is_sequence_integer(
    const ducknng_quack_column_schema *node);
int ducknng_qk_type_equal(const ducknng_quack_column_schema *a,
    const ducknng_quack_column_schema *b);
uint64_t ducknng_qk_type_expected_info_kind(int logical_type_id);
int ducknng_qk_type_needs_info(const ducknng_quack_column_schema *node);
int ducknng_qk_type_shape_validate(const ducknng_quack_column_schema *node,
    const char **error_message);
