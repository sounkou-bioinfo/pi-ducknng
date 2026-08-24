#include "ducknng_quack_core.h"
#include "ducknng_checked.h"
#include <string.h>

static int ducknng_qk_writer_fail(ducknng_qk_writer *writer,
    ducknng_qk_status status) {
    writer->status = status;
    return -1;
}

void ducknng_qk_writer_init_measure(ducknng_qk_writer *writer) {
    if (!writer) return;
    memset(writer, 0, sizeof(*writer));
    writer->mode = DUCKNNG_QK_WRITER_MEASURE;
}

void ducknng_qk_writer_init_fixed(ducknng_qk_writer *writer,
    uint8_t *destination, size_t capacity) {
    if (!writer) return;
    memset(writer, 0, sizeof(*writer));
    writer->data = destination;
    writer->capacity = capacity;
    writer->mode = DUCKNNG_QK_WRITER_FIXED;
    if (capacity > 0 && !destination) writer->status = DUCKNNG_QK_INVALID;
}

size_t ducknng_qk_writer_size(const ducknng_qk_writer *writer) {
    return writer ? writer->position : 0;
}

ducknng_qk_status ducknng_qk_writer_status(
    const ducknng_qk_writer *writer) {
    return writer ? writer->status : DUCKNNG_QK_INVALID;
}

int ducknng_qk_write_bytes(ducknng_qk_writer *writer,
    const void *source, size_t size) {
    size_t next;
    if (!writer || writer->status != DUCKNNG_QK_OK) return -1;
    if (size > 0 && !source) return ducknng_qk_writer_fail(writer,
        DUCKNNG_QK_INVALID);
    if (ducknng_size_add(writer->position, size, &next) != 0) {
        return ducknng_qk_writer_fail(writer, DUCKNNG_QK_OVERFLOW);
    }
    if (writer->mode == DUCKNNG_QK_WRITER_FIXED) {
        if (next > writer->capacity) return ducknng_qk_writer_fail(writer,
            DUCKNNG_QK_NO_SPACE);
        if (size > 0) memcpy(writer->data + writer->position, source, size);
    } else if (writer->mode != DUCKNNG_QK_WRITER_MEASURE) {
        return ducknng_qk_writer_fail(writer, DUCKNNG_QK_INVALID);
    }
    writer->position = next;
    return 0;
}

int ducknng_qk_write_u8(ducknng_qk_writer *writer, uint8_t value) {
    return ducknng_qk_write_bytes(writer, &value, sizeof(value));
}

int ducknng_qk_write_u16le(ducknng_qk_writer *writer, uint16_t value) {
    uint8_t bytes[2];
    bytes[0] = (uint8_t)(value & UINT16_C(0xff));
    bytes[1] = (uint8_t)(value >> 8);
    return ducknng_qk_write_bytes(writer, bytes, sizeof(bytes));
}

int ducknng_qk_write_field(ducknng_qk_writer *writer, uint16_t field) {
    return ducknng_qk_write_u16le(writer, field);
}

int ducknng_qk_write_uleb128(ducknng_qk_writer *writer, uint64_t value) {
    do {
        uint8_t byte = (uint8_t)(value & UINT64_C(0x7f));
        value >>= 7;
        if (value != 0) byte |= UINT8_C(0x80);
        if (ducknng_qk_write_u8(writer, byte) != 0) return -1;
    } while (value != 0);
    return 0;
}

int ducknng_qk_write_sleb128(ducknng_qk_writer *writer, int64_t value) {
    int more = 1;
    while (more) {
        int64_t next = value / 128;
        uint8_t byte;
        int sign;
        if (value < 0 && value % 128 != 0) next--;
        byte = (uint8_t)(value - next * 128);
        sign = (byte & UINT8_C(0x40)) != 0;
        more = !((next == 0 && !sign) || (next == -1 && sign));
        if (more) byte |= UINT8_C(0x80);
        if (ducknng_qk_write_u8(writer, byte) != 0) return -1;
        value = next;
    }
    return 0;
}

int ducknng_qk_write_counted(ducknng_qk_writer *writer,
    const void *source, size_t size) {
    if (ducknng_qk_write_uleb128(writer, (uint64_t)size) != 0) return -1;
    return ducknng_qk_write_bytes(writer, source, size);
}

static int ducknng_qk_reader_fail(ducknng_qk_reader *reader,
    ducknng_qk_status status) {
    reader->status = status;
    return -1;
}

void ducknng_qk_reader_init(ducknng_qk_reader *reader,
    const uint8_t *data, size_t size) {
    if (!reader) return;
    memset(reader, 0, sizeof(*reader));
    reader->data = data;
    reader->len = size;
    if (size > 0 && !data) reader->status = DUCKNNG_QK_INVALID;
}

size_t ducknng_qk_reader_remaining(const ducknng_qk_reader *reader) {
    if (!reader || reader->off > reader->len) return 0;
    return reader->len - reader->off;
}

ducknng_qk_status ducknng_qk_reader_status(
    const ducknng_qk_reader *reader) {
    return reader ? reader->status : DUCKNNG_QK_INVALID;
}

static int ducknng_qk_reader_need(ducknng_qk_reader *reader, size_t size) {
    if (!reader || reader->status != DUCKNNG_QK_OK) return -1;
    if (reader->off > reader->len ||
        size > reader->len - reader->off) {
        return ducknng_qk_reader_fail(reader, DUCKNNG_QK_TRUNCATED);
    }
    return 0;
}

int ducknng_qk_read_u8(ducknng_qk_reader *reader, uint8_t *out) {
    if (ducknng_qk_reader_need(reader, 1) != 0) return -1;
    if (out) *out = reader->data[reader->off];
    reader->off++;
    return 0;
}

int ducknng_qk_read_boolean(ducknng_qk_reader *reader, uint8_t *out) {
    uint8_t value = 0;
    if (ducknng_qk_read_u8(reader, &value) != 0) return -1;
    if (value > 1u) return ducknng_qk_reader_fail(reader,
        DUCKNNG_QK_INVALID);
    if (out) *out = value;
    return 0;
}

int ducknng_qk_peek_u16le(ducknng_qk_reader *reader, uint16_t *out) {
    if (ducknng_qk_reader_need(reader, 2) != 0) return -1;
    if (out) {
        *out = (uint16_t)reader->data[reader->off] |
            (uint16_t)((uint16_t)reader->data[reader->off + 1] << 8);
    }
    return 0;
}

int ducknng_qk_read_u16le(ducknng_qk_reader *reader, uint16_t *out) {
    if (ducknng_qk_peek_u16le(reader, out) != 0) return -1;
    reader->off += 2;
    return 0;
}

int ducknng_qk_read_uleb128(ducknng_qk_reader *reader, uint64_t *out) {
    uint64_t value = 0;
    unsigned shift = 0;
    for (;;) {
        uint8_t byte = 0;
        if (shift >= 64) return ducknng_qk_reader_fail(reader,
            DUCKNNG_QK_OVERFLOW);
        if (ducknng_qk_read_u8(reader, &byte) != 0) return -1;
        if (shift == 63 && (byte & UINT8_C(0x7e)) != 0) {
            return ducknng_qk_reader_fail(reader, DUCKNNG_QK_OVERFLOW);
        }
        value |= (uint64_t)(byte & UINT8_C(0x7f)) << shift;
        if ((byte & UINT8_C(0x80)) == 0) break;
        shift += 7;
    }
    if (out) *out = value;
    return 0;
}

int ducknng_qk_read_sleb128(ducknng_qk_reader *reader, int64_t *out) {
    uint64_t bits = 0;
    unsigned shift = 0;
    uint8_t byte = 0;
    for (;;) {
        uint8_t payload;
        if (shift >= 64) return ducknng_qk_reader_fail(reader,
            DUCKNNG_QK_OVERFLOW);
        if (ducknng_qk_read_u8(reader, &byte) != 0) return -1;
        payload = (uint8_t)(byte & UINT8_C(0x7f));
        if (shift == 63 && payload != 0 && payload != UINT8_C(0x7f)) {
            return ducknng_qk_reader_fail(reader, DUCKNNG_QK_OVERFLOW);
        }
        bits |= (uint64_t)payload << shift;
        if ((byte & UINT8_C(0x80)) == 0) break;
        shift += 7;
    }
    if (shift < 63 && (byte & UINT8_C(0x40)) != 0) {
        bits |= UINT64_MAX << (shift + 7);
    }
    if (out) memcpy(out, &bits, sizeof(bits));
    return 0;
}

int ducknng_qk_read_counted(ducknng_qk_reader *reader,
    const uint8_t **out_data, size_t *out_size) {
    uint64_t length = 0;
    size_t size;
    if (out_data) *out_data = NULL;
    if (out_size) *out_size = 0;
    if (ducknng_qk_read_uleb128(reader, &length) != 0) return -1;
    if (length > SIZE_MAX) return ducknng_qk_reader_fail(reader,
        DUCKNNG_QK_OVERFLOW);
    size = (size_t)length;
    if (ducknng_qk_reader_need(reader, size) != 0) return -1;
    if (out_data) *out_data = reader->data + reader->off;
    if (out_size) *out_size = size;
    reader->off += size;
    return 0;
}

int ducknng_qk_skip(ducknng_qk_reader *reader, size_t size) {
    if (ducknng_qk_reader_need(reader, size) != 0) return -1;
    reader->off += size;
    return 0;
}

/* Deliberately not routed through ducknng_size_mul. The row count is uint64_t,
 * which size_t cannot express on a 32-bit target, and after the rounding
 * divide `words` is at most 2^58, so the multiply cannot overflow a 64-bit
 * size_t. An unconditional checked multiply would add an MC/DC condition whose
 * false branch is unreachable wherever size_t is 64 bits, failing the coverage
 * gate. The preprocessor guard keeps the check only where it can fire. */
size_t ducknng_qk_validity_bytes(uint64_t rows) {
    uint64_t words;
    if (rows > UINT64_MAX - 63u) return SIZE_MAX;
    words = (rows + 63u) / 64u;
#if SIZE_MAX < UINT64_MAX
    if (words > SIZE_MAX / 8u) return SIZE_MAX;
#endif
    return (size_t)words * 8u;
}

int ducknng_qk_type_is_nested(const ducknng_quack_column_schema *node) {
    if (!node) return 0;
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_LIST:
    case DUCKNNG_QUACK_LOGICAL_MAP:
    case DUCKNNG_QUACK_LOGICAL_STRUCT:
    case DUCKNNG_QUACK_LOGICAL_UNION:
    case DUCKNNG_QUACK_LOGICAL_ARRAY:
        return 1;
    default:
        return 0;
    }
}

int ducknng_qk_type_is_varlen(const ducknng_quack_column_schema *node) {
    if (!node) return 0;
    return node->logical_type_id == DUCKNNG_QUACK_LOGICAL_VARCHAR ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_CHAR ||
        node->logical_type_id == DUCKNNG_QUACK_LOGICAL_BLOB;
}

size_t ducknng_qk_type_fixed_width(const ducknng_quack_column_schema *node) {
    if (!node) return 0;
    if (node->logical_type_id == DUCKNNG_QUACK_LOGICAL_ENUM) {
        if (node->enum_count <= 0xffu) return 1;
        if (node->enum_count <= 0xffffu) return 2;
        return 4;
    }
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_BOOLEAN:
    case DUCKNNG_QUACK_LOGICAL_TINYINT:
    case DUCKNNG_QUACK_LOGICAL_UTINYINT:
        return 1;
    case DUCKNNG_QUACK_LOGICAL_SMALLINT:
    case DUCKNNG_QUACK_LOGICAL_USMALLINT:
        return 2;
    case DUCKNNG_QUACK_LOGICAL_INTEGER:
    case DUCKNNG_QUACK_LOGICAL_UINTEGER:
    case DUCKNNG_QUACK_LOGICAL_DATE:
    case DUCKNNG_QUACK_LOGICAL_FLOAT:
        return 4;
    case DUCKNNG_QUACK_LOGICAL_BIGINT:
    case DUCKNNG_QUACK_LOGICAL_UBIGINT:
    case DUCKNNG_QUACK_LOGICAL_TIME:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_SEC:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_MS:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_NS:
    case DUCKNNG_QUACK_LOGICAL_TIMESTAMP_TZ:
    case DUCKNNG_QUACK_LOGICAL_TIME_TZ:
    case DUCKNNG_QUACK_LOGICAL_TIME_NS:
    case DUCKNNG_QUACK_LOGICAL_DOUBLE:
        return 8;
    case DUCKNNG_QUACK_LOGICAL_INTERVAL:
    case DUCKNNG_QUACK_LOGICAL_HUGEINT:
    case DUCKNNG_QUACK_LOGICAL_UHUGEINT:
    case DUCKNNG_QUACK_LOGICAL_UUID:
        return 16;
    case DUCKNNG_QUACK_LOGICAL_DECIMAL:
        if (node->decimal_width <= 4) return 2;
        if (node->decimal_width <= 9) return 4;
        if (node->decimal_width <= 18) return 8;
        if (node->decimal_width <= 38) return 16;
        return 0;
    default:
        return 0;
    }
}

int ducknng_qk_type_is_sequence_integer(
    const ducknng_quack_column_schema *node) {
    if (!node) return 0;
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_TINYINT:
    case DUCKNNG_QUACK_LOGICAL_SMALLINT:
    case DUCKNNG_QUACK_LOGICAL_INTEGER:
    case DUCKNNG_QUACK_LOGICAL_BIGINT:
    case DUCKNNG_QUACK_LOGICAL_UTINYINT:
    case DUCKNNG_QUACK_LOGICAL_USMALLINT:
    case DUCKNNG_QUACK_LOGICAL_UINTEGER:
    case DUCKNNG_QUACK_LOGICAL_UBIGINT:
        return 1;
    default:
        return 0;
    }
}

int ducknng_qk_type_equal(const ducknng_quack_column_schema *a,
    const ducknng_quack_column_schema *b) {
    uint32_t i;
    if (!a) return 0;
    if (!b) return 0;
    if (a->logical_type_id != b->logical_type_id) return 0;
    if (a->decimal_width != b->decimal_width) return 0;
    if (a->decimal_scale != b->decimal_scale) return 0;
    if (a->array_size != b->array_size) return 0;
    if (a->enum_count != b->enum_count) return 0;
    if (a->nchildren != b->nchildren) return 0;
    if (a->enum_count > 0 && !a->enum_labels) return 0;
    if (b->enum_count > 0 && !b->enum_labels) return 0;
    for (i = 0; i < a->enum_count; i++) {
        const char *al = a->enum_labels[i] ? a->enum_labels[i] : "";
        const char *bl = b->enum_labels[i] ? b->enum_labels[i] : "";
        if (strcmp(al, bl) != 0) return 0;
    }
    if (a->nchildren > 0 && !a->children) return 0;
    if (b->nchildren > 0 && !b->children) return 0;
    for (i = 0; i < a->nchildren; i++) {
        const char *an = a->child_names && a->child_names[i]
            ? a->child_names[i] : "";
        const char *bn = b->child_names && b->child_names[i]
            ? b->child_names[i] : "";
        if (strcmp(an, bn) != 0) return 0;
        if (!ducknng_qk_type_equal(a->children[i], b->children[i])) return 0;
    }
    return 1;
}

uint64_t ducknng_qk_type_expected_info_kind(int logical_type_id) {
    switch (logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_DECIMAL:
        return DUCKNNG_QUACK_EXTRA_TYPE_DECIMAL;
    case DUCKNNG_QUACK_LOGICAL_LIST:
    case DUCKNNG_QUACK_LOGICAL_MAP:
        return DUCKNNG_QUACK_EXTRA_TYPE_LIST;
    case DUCKNNG_QUACK_LOGICAL_STRUCT:
    case DUCKNNG_QUACK_LOGICAL_UNION:
        return DUCKNNG_QUACK_EXTRA_TYPE_STRUCT;
    case DUCKNNG_QUACK_LOGICAL_ENUM:
        return DUCKNNG_QUACK_EXTRA_TYPE_ENUM;
    case DUCKNNG_QUACK_LOGICAL_ARRAY:
        return DUCKNNG_QUACK_EXTRA_TYPE_ARRAY;
    default:
        return UINT64_MAX;
    }
}

int ducknng_qk_type_needs_info(const ducknng_quack_column_schema *node) {
    if (!node) return 0;
    return ducknng_qk_type_expected_info_kind(node->logical_type_id) !=
        UINT64_MAX;
}

static int ducknng_qk_type_shape_error(const char **error_message,
    const char *message) {
    if (error_message) *error_message = message;
    return -1;
}

int ducknng_qk_type_shape_validate(const ducknng_quack_column_schema *node,
    const char **error_message) {
    uint32_t i;
    if (error_message) *error_message = NULL;
    if (!node) return ducknng_qk_type_shape_error(error_message,
        "ducknng: missing quack logical type");
    switch (node->logical_type_id) {
    case DUCKNNG_QUACK_LOGICAL_DECIMAL:
        if (node->decimal_width == 0 || node->decimal_width > 38 ||
            node->decimal_scale > node->decimal_width) {
            return ducknng_qk_type_shape_error(error_message,
                "ducknng: quack decimal type has invalid width or scale");
        }
        return 0;
    case DUCKNNG_QUACK_LOGICAL_LIST:
        if (node->nchildren != 1) goto malformed_list;
        if (!node->children) goto malformed_list;
        if (!node->children[0]) goto malformed_list;
        return 0;
    malformed_list:
        return ducknng_qk_type_shape_error(error_message,
            "ducknng: quack list type is missing its child");
    case DUCKNNG_QUACK_LOGICAL_MAP:
        if (node->nchildren != 1) goto malformed_map;
        if (!node->children) goto malformed_map;
        if (!node->children[0]) goto malformed_map;
        if (node->children[0]->logical_type_id !=
            DUCKNNG_QUACK_LOGICAL_STRUCT) goto malformed_map;
        if (node->children[0]->nchildren != 2) goto malformed_map;
        if (!node->children[0]->children) goto malformed_map;
        if (!node->children[0]->children[0]) goto malformed_map;
        if (!node->children[0]->children[1]) goto malformed_map;
        return 0;
    malformed_map:
        return ducknng_qk_type_shape_error(error_message,
            "ducknng: malformed quack map type");
    case DUCKNNG_QUACK_LOGICAL_STRUCT:
        if (node->nchildren == 0 || !node->children || !node->child_names) {
            return ducknng_qk_type_shape_error(error_message,
                "ducknng: quack struct type is missing its members");
        }
        break;
    case DUCKNNG_QUACK_LOGICAL_UNION:
        if (node->nchildren < 2) goto malformed_union;
        if (!node->children) goto malformed_union;
        if (!node->child_names) goto malformed_union;
        if (!node->children[0]) goto malformed_union;
        if (node->children[0]->logical_type_id !=
            DUCKNNG_QUACK_LOGICAL_UTINYINT) goto malformed_union;
        break;
    malformed_union:
        return ducknng_qk_type_shape_error(error_message,
            "ducknng: quack union type is missing its tag or members");
    case DUCKNNG_QUACK_LOGICAL_ENUM:
        if (node->enum_count > DUCKNNG_QUACK_MAX_ENUM_VALUES ||
            (node->enum_count > 0 && !node->enum_labels)) {
            return ducknng_qk_type_shape_error(error_message,
                "ducknng: quack enum type is missing its dictionary");
        }
        for (i = 0; i < node->enum_count; i++) {
            if (!node->enum_labels[i]) return ducknng_qk_type_shape_error(
                error_message,
                "ducknng: quack enum type has a missing label");
        }
        return 0;
    case DUCKNNG_QUACK_LOGICAL_ARRAY:
        if (node->array_size == 0) goto malformed_array;
        if (node->nchildren != 1) goto malformed_array;
        if (!node->children) goto malformed_array;
        if (!node->children[0]) goto malformed_array;
        return 0;
    malformed_array:
        return ducknng_qk_type_shape_error(error_message,
            "ducknng: quack array type is missing its child or size");
    default:
        if (!ducknng_qk_type_is_varlen(node) &&
            ducknng_qk_type_fixed_width(node) == 0) {
            return ducknng_qk_type_shape_error(error_message,
                "ducknng: quack payload uses an unsupported logical type");
        }
        return 0;
    }
    for (i = 0; i < node->nchildren; i++) {
        if (!node->children[i] || !node->child_names[i]) {
            return ducknng_qk_type_shape_error(error_message,
                "ducknng: quack struct/union type has an incomplete member");
        }
    }
    return 0;
}
