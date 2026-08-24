#pragma once
#include <stddef.h>
#include <stdint.h>
#include <nng/nng.h>

#define DUCKNNG_WIRE_VERSION 1u
#define DUCKNNG_MAX_METHOD_NAME_LEN 128u
#define DUCKNNG_WIRE_HEADER_LEN 22u

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
    DUCKNNG_STATUS_DISABLED = 10
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
    DUCKNNG_RPC_FLAG_CANCELLED = 128u
};

typedef struct ducknng_frame {
    uint8_t version;
    uint8_t type;
    uint32_t flags;
    const uint8_t *name;
    uint32_t name_len;
    const uint8_t *error;
    uint32_t error_len;
    const uint8_t *payload;
    uint64_t payload_len;
} ducknng_frame;

int ducknng_decode_frame_bytes(const uint8_t *data, size_t len, ducknng_frame *out);
int ducknng_decode_request(nng_msg *msg, ducknng_frame *out);
int ducknng_frame_name_equals(const ducknng_frame *frame, const char *name);
nng_msg *ducknng_build_reply(uint8_t type, const char *name, uint32_t flags,
    const char *error, const void *payload, uint64_t payload_len);
nng_msg *ducknng_error_msg(const char *name, int32_t code, const char *message);
