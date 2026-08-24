#pragma once
#include <stddef.h>
#include <stdint.h>

#define DUCKNNG_WIRE_VERSION 1u
#define DUCKNNG_MAX_METHOD_NAME_LEN 128u
#define DUCKNNG_WIRE_HEADER_LEN 22u

/* Protocol v1 reserves the high byte of the little-endian control word for the
 * protocol status. Semantic flags occupy the low 24 bits. */
#define DUCKNNG_RPC_STATUS_SHIFT 24u
#define DUCKNNG_RPC_STATUS_MASK UINT32_C(0xff000000)
#define DUCKNNG_RPC_FLAGS_MASK UINT32_C(0x00ffffff)

enum ducknng_rpc_type {
    DUCKNNG_RPC_MANIFEST = 0,
    DUCKNNG_RPC_CALL = 1,
    DUCKNNG_RPC_RESULT = 2,
    DUCKNNG_RPC_ERROR = 3,
    DUCKNNG_RPC_EVENT = 4
};

enum ducknng_status {
    DUCKNNG_STATUS_OK = 0,
    DUCKNNG_STATUS_INVALID = 1,
    DUCKNNG_STATUS_NOT_FOUND = 2,
    DUCKNNG_STATUS_BUSY = 3,
    DUCKNNG_STATUS_SQL_ERROR = 4,
    DUCKNNG_STATUS_ARROW_ERROR = 5,
    DUCKNNG_STATUS_INTERNAL = 6,
    DUCKNNG_STATUS_CANCELLED = 7,
    DUCKNNG_STATUS_TLS_ERROR = 8,
    DUCKNNG_STATUS_UNAUTHORIZED = 9,
    DUCKNNG_STATUS_DISABLED = 10,
    /* Decode-only representation of legacy error frames with a zero status. */
    DUCKNNG_STATUS_UNSPECIFIED = 255
};

enum ducknng_rpc_flags {
    DUCKNNG_RPC_FLAG_NONE = 0u,
    DUCKNNG_RPC_FLAG_RESULT_ROWS = 1u,
    DUCKNNG_RPC_FLAG_RESULT_METADATA = 2u,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON = 4u,
    DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM = 8u,
    DUCKNNG_RPC_FLAG_END_OF_STREAM = 16u,
    DUCKNNG_RPC_FLAG_SESSION_OPEN = 32u,
    DUCKNNG_RPC_FLAG_SESSION_CLOSED = 64u,
    DUCKNNG_RPC_FLAG_CANCELLED = 128u,
    DUCKNNG_RPC_FLAG_PAYLOAD_QUACK_BATCH = 256u
};

typedef struct ducknng_frame {
    uint8_t version;
    uint8_t type;
    uint8_t status;
    uint32_t flags;
    const uint8_t *name;
    uint32_t name_len;
    const uint8_t *error;
    uint32_t error_len;
    const uint8_t *payload;
    uint64_t payload_len;
} ducknng_frame;

/* Counted input for the dependency-free frame encoder. All byte slices are
 * borrowed for the duration of the call. Error frames require a documented,
 * nonzero status and nonempty error text; other frame types require status zero
 * and no error text. */
typedef struct ducknng_frame_parts {
    uint8_t type;
    int32_t status;
    uint32_t flags;
    const uint8_t *name;
    size_t name_len;
    const uint8_t *error;
    size_t error_len;
    const uint8_t *payload;
    size_t payload_len;
} ducknng_frame_parts;

/* On an insufficient destination, *written receives the exact required size.
 * On invalid input it remains zero. */
int ducknng_frame_measure(const ducknng_frame_parts *parts, size_t *required);
/* Encode only header, name, and error bytes while counting payload_len in the
 * header. The payload pointer may be NULL because payload bytes are not read.
 * This lets an adapter reserve one final message and encode its payload in
 * place after the returned prefix. */
int ducknng_encode_frame_prefix(const ducknng_frame_parts *parts,
    uint8_t *destination, size_t capacity, size_t *written);
int ducknng_encode_frame_bytes(const ducknng_frame_parts *parts,
    uint8_t *destination, size_t capacity, size_t *written);
int ducknng_decode_frame_bytes(const uint8_t *data, size_t len,
    ducknng_frame *out);
int ducknng_frame_name_equals(const ducknng_frame *frame, const char *name);

/* Upload-append body prefix. Returned token points into payload. */
#define DUCKNNG_UPLOAD_TOKEN_MAX 256u
int ducknng_upload_append_parse_prefix(const uint8_t *payload,
    size_t payload_len, uint64_t *out_session_id, const uint8_t **out_token,
    size_t *out_token_len, size_t *out_quack_offset);
