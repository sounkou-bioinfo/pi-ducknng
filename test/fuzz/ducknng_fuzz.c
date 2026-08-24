#include "duckdb_extension.h"
DUCKDB_EXTENSION_GLOBAL
#undef duckdb_malloc
#undef duckdb_free

#include "ducknng_quack.h"
#include "ducknng_quack_core.h"
#include "ducknng_wire_core.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* Schema/chunk traversal still shares a translation unit with the streaming
 * DuckDB adapter. libFuzzer's export-dynamic link retains that unreachable
 * function, so provide one aborting core-only definition. The harness never
 * calls it; the remaining parser split removes this definition. */
duckdb_data_chunk ducknng_result_fetch_session_chunk(duckdb_result result) {
    (void)result;
    abort();
}

static void ducknng_fuzz_init_api(void) {
    static int initialized = 0;
    if (initialized) return;
    duckdb_ext_api.duckdb_malloc = malloc;
    duckdb_ext_api.duckdb_free = free;
    initialized = 1;
}

static void ducknng_fuzz_frame(const uint8_t *data, size_t size) {
    ducknng_frame frame;
    ducknng_frame_parts parts;
    ducknng_frame decoded;
    uint8_t *encoded = NULL;
    size_t required = 0;
    size_t written = 0;

    if (ducknng_decode_frame_bytes(data, size, &frame) != 0) return;
    if (frame.name < data || frame.error < frame.name ||
        frame.payload < frame.error || frame.payload_len > SIZE_MAX ||
        (size_t)(frame.payload - data) > size ||
        (size_t)frame.payload_len != size - (size_t)(frame.payload - data)) abort();

    memset(&parts, 0, sizeof(parts));
    parts.type = frame.type;
    parts.status = frame.status == DUCKNNG_STATUS_UNSPECIFIED
        ? DUCKNNG_STATUS_INVALID : frame.status;
    parts.flags = frame.flags;
    parts.name = frame.name;
    parts.name_len = frame.name_len;
    parts.error = frame.error;
    parts.error_len = frame.error_len;
    parts.payload = frame.payload;
    parts.payload_len = (size_t)frame.payload_len;
    if (ducknng_frame_measure(&parts, &required) != 0 || required != size) abort();
    encoded = (uint8_t *)malloc(required ? required : 1u);
    if (!encoded) abort();
    if (ducknng_encode_frame_bytes(&parts, encoded, required, &written) != 0 ||
        written != required ||
        ducknng_decode_frame_bytes(encoded, written, &decoded) != 0 ||
        decoded.type != frame.type || decoded.flags != frame.flags ||
        decoded.name_len != frame.name_len || decoded.error_len != frame.error_len ||
        decoded.payload_len != frame.payload_len ||
        decoded.status != (uint8_t)parts.status ||
        memcmp(decoded.name, frame.name, frame.name_len) != 0 ||
        memcmp(decoded.error, frame.error, frame.error_len) != 0 ||
        memcmp(decoded.payload, frame.payload, (size_t)frame.payload_len) != 0) abort();
    free(encoded);
}

static void ducknng_fuzz_upload_prefix(const uint8_t *data, size_t size) {
    uint64_t session_id = 0;
    const uint8_t *token = NULL;
    size_t token_len = 0;
    size_t quack_offset = 0;
    if (ducknng_upload_append_parse_prefix(data, size, &session_id, &token,
            &token_len, &quack_offset) != 0) return;
    (void)session_id;
    if (!token || token != data + 10u || token_len == 0 ||
        token_len > DUCKNNG_UPLOAD_TOKEN_MAX || quack_offset > size ||
        quack_offset != 10u + token_len) abort();
}

static void ducknng_fuzz_quack_bytes(const uint8_t *data, size_t size) {
    ducknng_qk_writer writer;
    ducknng_qk_reader reader;
    const uint8_t *decoded = NULL;
    uint8_t *encoded;
    size_t required;
    size_t decoded_size = 0;

    ducknng_qk_writer_init_measure(&writer);
    if (ducknng_qk_write_counted(&writer, data, size) != 0) abort();
    required = ducknng_qk_writer_size(&writer);
    encoded = (uint8_t *)malloc(required ? required : 1u);
    if (!encoded) abort();
    ducknng_qk_writer_init_fixed(&writer, encoded, required);
    if (ducknng_qk_write_counted(&writer, data, size) != 0 ||
        ducknng_qk_writer_size(&writer) != required) abort();
    ducknng_qk_reader_init(&reader, encoded, required);
    if (ducknng_qk_read_counted(&reader, &decoded, &decoded_size) != 0 ||
        decoded_size != size || memcmp(decoded, data, size) != 0 ||
        ducknng_qk_reader_remaining(&reader) != 0) abort();
    free(encoded);

    ducknng_qk_reader_init(&reader, data, size);
    switch (size > 0 ? data[0] & 3u : 0u) {
    case 0: {
        uint64_t value = 0;
        (void)ducknng_qk_read_uleb128(&reader, &value);
        break;
    }
    case 1: {
        int64_t value = 0;
        (void)ducknng_qk_read_sleb128(&reader, &value);
        break;
    }
    case 2: {
        uint8_t value = 0;
        (void)ducknng_qk_read_boolean(&reader, &value);
        break;
    }
    default:
        (void)ducknng_qk_read_counted(&reader, &decoded, &decoded_size);
        break;
    }
}

static void ducknng_fuzz_quack(const uint8_t *data, size_t size) {
    ducknng_quack_schema schema;
    idx_t rows = 0;
    char *errmsg = NULL;
    int rc;

    memset(&schema, 0, sizeof(schema));
    rc = ducknng_quack_payload_parse_schema(data, size, &schema, &errmsg);
    if (rc != 0) {
        if (errmsg) free(errmsg);
        ducknng_quack_schema_reset(&schema);
        return;
    }
    if (errmsg || (schema.ncols > 0 && !schema.cols) ||
        (schema.ncols == 0 && schema.cols)) abort();
    rc = ducknng_quack_payload_read_row_count(data, size, &schema, &rows, &errmsg);
    if (rc == 0 && errmsg) abort();
    if (errmsg) free(errmsg);
    ducknng_quack_schema_reset(&schema);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    ducknng_fuzz_init_api();
    ducknng_fuzz_frame(data, size);
    ducknng_fuzz_upload_prefix(data, size);
    ducknng_fuzz_quack_bytes(data, size);
    ducknng_fuzz_quack(data, size);
    return 0;
}
