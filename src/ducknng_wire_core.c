#include "ducknng_wire_core.h"
#include "ducknng_checked.h"
#include <string.h>

/*
 * Wire header layout (little-endian):
 *   [0] version u8; [1] type u8; [2] control u32;
 *   [6] name_len u32; [10] error_len u32; [14] payload_len u64.
 */
_Static_assert(DUCKNNG_WIRE_HEADER_LEN == 1u + 1u + 4u + 4u + 4u + 8u,
    "ducknng wire header length must match its field layout");
_Static_assert(sizeof(uint8_t) == 1 && sizeof(uint32_t) == 4 &&
    sizeof(uint64_t) == 8,
    "ducknng wire encoding assumes 8/32/64-bit fixed-width integers");
_Static_assert(DUCKNNG_WIRE_VERSION <= UINT8_MAX &&
    DUCKNNG_RPC_EVENT <= UINT8_MAX && DUCKNNG_STATUS_UNSPECIFIED <= UINT8_MAX,
    "ducknng wire version/type/status ids must fit in one byte");
_Static_assert(DUCKNNG_RPC_FLAG_PAYLOAD_QUACK_BATCH <= DUCKNNG_RPC_FLAGS_MASK,
    "ducknng semantic flags must fit below the reserved status byte");

static uint16_t ducknng_wire_le16_read(const uint8_t *p) {
    return (uint16_t)p[0] | (uint16_t)((uint16_t)p[1] << 8);
}

static uint32_t ducknng_wire_le32_read(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
        ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t ducknng_wire_le64_read(const uint8_t *p) {
    return (uint64_t)ducknng_wire_le32_read(p) |
        ((uint64_t)ducknng_wire_le32_read(p + 4) << 32);
}

static void ducknng_wire_le32_write(uint8_t *p, uint32_t value) {
    p[0] = (uint8_t)(value & UINT32_C(0xff));
    p[1] = (uint8_t)((value >> 8) & UINT32_C(0xff));
    p[2] = (uint8_t)((value >> 16) & UINT32_C(0xff));
    p[3] = (uint8_t)((value >> 24) & UINT32_C(0xff));
}

static void ducknng_wire_le64_write(uint8_t *p, uint64_t value) {
    ducknng_wire_le32_write(p, (uint32_t)value);
    ducknng_wire_le32_write(p + 4, (uint32_t)(value >> 32));
}

static int ducknng_frame_parts_validate(const ducknng_frame_parts *parts,
    size_t *required, int require_payload_pointer) {
    size_t total = DUCKNNG_WIRE_HEADER_LEN;
    if (required) *required = 0;
    if (!parts) return -1;
    if (!required) return -1;
    if (parts->type > DUCKNNG_RPC_EVENT) return -1;
    if (parts->name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return -1;
#if SIZE_MAX > UINT32_MAX
    if (parts->error_len > UINT32_MAX) return -1;
#endif
    if ((parts->flags & ~DUCKNNG_RPC_FLAGS_MASK) != 0) return -1;
    if (parts->name_len > 0 && !parts->name) return -1;
    if (parts->error_len > 0 && !parts->error) return -1;
    if (require_payload_pointer && parts->payload_len > 0 && !parts->payload) return -1;
    if (parts->type == DUCKNNG_RPC_ERROR) {
        if (parts->status <= DUCKNNG_STATUS_OK ||
            parts->status > DUCKNNG_STATUS_DISABLED ||
            parts->error_len == 0) return -1;
    } else if (parts->status != DUCKNNG_STATUS_OK || parts->error_len != 0) {
        return -1;
    }
    if (ducknng_size_add(total, parts->name_len, &total) != 0) return -1;
    if (ducknng_size_add(total, parts->error_len, &total) != 0) return -1;
    if (ducknng_size_add(total, parts->payload_len, &total) != 0) return -1;
    *required = total;
    return 0;
}

int ducknng_frame_measure(const ducknng_frame_parts *parts, size_t *required) {
    return ducknng_frame_parts_validate(parts, required, 1);
}

/* Write the fixed header followed by the counted name and error strings, and
 * return the offset just past them. The prefix-only and whole-frame encoders
 * share this so a header layout change cannot land in one and miss the other.
 * Callers validate parts and check the destination bound first. */
static size_t ducknng_wire_write_prefix(const ducknng_frame_parts *parts,
    uint8_t *destination) {
    size_t offset = DUCKNNG_WIRE_HEADER_LEN;
    uint32_t wire_flags;
    memset(destination, 0, DUCKNNG_WIRE_HEADER_LEN);
    destination[0] = DUCKNNG_WIRE_VERSION;
    destination[1] = parts->type;
    wire_flags = parts->flags |
        ((uint32_t)parts->status << DUCKNNG_RPC_STATUS_SHIFT);
    ducknng_wire_le32_write(destination + 2, wire_flags);
    ducknng_wire_le32_write(destination + 6, (uint32_t)parts->name_len);
    ducknng_wire_le32_write(destination + 10, (uint32_t)parts->error_len);
    ducknng_wire_le64_write(destination + 14, (uint64_t)parts->payload_len);
    if (parts->name_len > 0) {
        memcpy(destination + offset, parts->name, parts->name_len);
        offset += parts->name_len;
    }
    if (parts->error_len > 0) {
        memcpy(destination + offset, parts->error, parts->error_len);
        offset += parts->error_len;
    }
    return offset;
}

int ducknng_encode_frame_prefix(const ducknng_frame_parts *parts,
    uint8_t *destination, size_t capacity, size_t *written) {
    size_t required = 0;
    size_t prefix_size;
    if (written) *written = 0;
    if (!written) return -1;
    if (ducknng_frame_parts_validate(parts, &required, 0) != 0) return -1;
    prefix_size = required - parts->payload_len;
    *written = prefix_size;
    if (!destination || capacity < prefix_size) return -1;
    (void)ducknng_wire_write_prefix(parts, destination);
    return 0;
}

int ducknng_encode_frame_bytes(const ducknng_frame_parts *parts,
    uint8_t *destination, size_t capacity, size_t *written) {
    size_t required = 0;
    size_t offset;
    if (written) *written = 0;
    if (!written || ducknng_frame_parts_validate(parts, &required, 1) != 0) return -1;
    *written = required;
    if (!destination || capacity < required) return -1;
    offset = ducknng_wire_write_prefix(parts, destination);
    if (parts->payload_len > 0) {
        memcpy(destination + offset, parts->payload, parts->payload_len);
    }
    return 0;
}

int ducknng_decode_frame_bytes(const uint8_t *data, size_t len,
    ducknng_frame *out) {
    uint32_t name_len;
    uint32_t wire_flags;
    uint32_t error_len;
    uint32_t status;
    uint64_t payload_len;
    size_t fixed_len = DUCKNNG_WIRE_HEADER_LEN;
    size_t body_len;
    if (!out || !data || len < DUCKNNG_WIRE_HEADER_LEN) return -1;
    memset(out, 0, sizeof(*out));
    out->version = data[0];
    out->type = data[1];
    if (out->version != DUCKNNG_WIRE_VERSION ||
        out->type > DUCKNNG_RPC_EVENT) return -1;
    wire_flags = ducknng_wire_le32_read(data + 2);
    name_len = ducknng_wire_le32_read(data + 6);
    error_len = ducknng_wire_le32_read(data + 10);
    payload_len = ducknng_wire_le64_read(data + 14);
    status = (wire_flags & DUCKNNG_RPC_STATUS_MASK) >>
        DUCKNNG_RPC_STATUS_SHIFT;
    if (name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return -1;
    if (ducknng_size_add(fixed_len, (size_t)name_len, &fixed_len) != 0) return -1;
    if (ducknng_size_add(fixed_len, (size_t)error_len, &fixed_len) != 0) return -1;
    if (len < fixed_len) return -1;
    body_len = len - fixed_len;
    if (payload_len != (uint64_t)body_len) return -1;
    if (out->type == DUCKNNG_RPC_ERROR) {
        if (status > DUCKNNG_STATUS_DISABLED || error_len == 0) return -1;
        out->status = status ? (uint8_t)status :
            DUCKNNG_STATUS_UNSPECIFIED;
    } else {
        if (status != DUCKNNG_STATUS_OK || error_len != 0) return -1;
        out->status = DUCKNNG_STATUS_OK;
    }
    out->flags = wire_flags & DUCKNNG_RPC_FLAGS_MASK;
    out->name_len = name_len;
    out->name = data + DUCKNNG_WIRE_HEADER_LEN;
    out->error_len = error_len;
    out->error = out->name + name_len;
    out->payload_len = payload_len;
    out->payload = out->error + error_len;
    return 0;
}

int ducknng_frame_name_equals(const ducknng_frame *frame, const char *name) {
    size_t n;
    if (!frame || !name) return 0;
    n = strlen(name);
    return frame->name_len == (uint32_t)n &&
        memcmp(frame->name, name, n) == 0;
}

int ducknng_upload_append_parse_prefix(const uint8_t *payload,
    size_t payload_len, uint64_t *out_session_id, const uint8_t **out_token,
    size_t *out_token_len, size_t *out_quack_offset) {
    uint16_t token_len;
    const size_t token_offset = 10u;
    if (out_session_id) *out_session_id = 0;
    if (out_token) *out_token = NULL;
    if (out_token_len) *out_token_len = 0;
    if (out_quack_offset) *out_quack_offset = 0;
    if (!payload || payload_len < token_offset) return -1;
    token_len = ducknng_wire_le16_read(payload + 8);
    if (token_len == 0 || token_len > DUCKNNG_UPLOAD_TOKEN_MAX ||
        (size_t)token_len > payload_len - token_offset) return -1;
    if (out_session_id) *out_session_id = ducknng_wire_le64_read(payload);
    if (out_token) *out_token = payload + token_offset;
    if (out_token_len) *out_token_len = (size_t)token_len;
    if (out_quack_offset) *out_quack_offset = token_offset + token_len;
    return 0;
}
