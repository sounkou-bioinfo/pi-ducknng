#include "ducknng_wire.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

int ducknng_decode_frame_bytes(const uint8_t *data, size_t len, ducknng_frame *out) {
    uint32_t name_len;
    uint32_t flags;
    uint32_t error_len;
    uint64_t payload_len;
    if (!out || !data || len < DUCKNNG_WIRE_HEADER_LEN) return -1;
    memset(out, 0, sizeof(*out));
    out->version = data[0];
    out->type = data[1];
    if (out->version != DUCKNNG_WIRE_VERSION) return -1;
    flags = ducknng_le32_read(data + 2);
    name_len = ducknng_le32_read(data + 6);
    error_len = ducknng_le32_read(data + 10);
    payload_len = ducknng_le64_read(data + 14);
    if (name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return -1;
    if (len < DUCKNNG_WIRE_HEADER_LEN + (size_t)name_len + (size_t)error_len) return -1;
    if ((uint64_t)(len - DUCKNNG_WIRE_HEADER_LEN - (size_t)name_len - (size_t)error_len) < payload_len) return -1;
    if (out->type == DUCKNNG_RPC_CALL && error_len != 0) return -1;
    out->flags = flags;
    out->name_len = name_len;
    out->name = data + DUCKNNG_WIRE_HEADER_LEN;
    out->error_len = error_len;
    out->error = out->name + name_len;
    out->payload_len = payload_len;
    out->payload = out->error + error_len;
    return 0;
}

int ducknng_decode_request(nng_msg *msg, ducknng_frame *out) {
    const uint8_t *data = (const uint8_t *)nng_msg_body(msg);
    size_t len = nng_msg_len(msg);
    return ducknng_decode_frame_bytes(data, len, out);
}

int ducknng_frame_name_equals(const ducknng_frame *frame, const char *name) {
    size_t n;
    if (!frame || !name) return 0;
    n = strlen(name);
    return frame->name_len == (uint32_t)n && memcmp(frame->name, name, n) == 0;
}

nng_msg *ducknng_build_reply(uint8_t type, const char *name, uint32_t flags,
    const char *error, const void *payload, uint64_t payload_len) {
    nng_msg *msg = NULL;
    uint8_t hdr[DUCKNNG_WIRE_HEADER_LEN];
    uint32_t name_len = name ? (uint32_t)strlen(name) : 0;
    uint32_t error_len = error ? (uint32_t)strlen(error) : 0;
    if (name_len > DUCKNNG_MAX_METHOD_NAME_LEN) return NULL;
    if (nng_msg_alloc(&msg, 0) != 0) return NULL;
    memset(hdr, 0, sizeof(hdr));
    hdr[0] = DUCKNNG_WIRE_VERSION;
    hdr[1] = type;
    ducknng_le32_write(hdr + 2, flags);
    ducknng_le32_write(hdr + 6, name_len);
    ducknng_le32_write(hdr + 10, error_len);
    ducknng_le64_write(hdr + 14, payload_len);
    if (nng_msg_append(msg, hdr, sizeof(hdr)) != 0) goto fail;
    if (name_len > 0 && nng_msg_append(msg, name, name_len) != 0) goto fail;
    if (error_len > 0 && nng_msg_append(msg, error, error_len) != 0) goto fail;
    if (payload_len > 0 && payload && nng_msg_append(msg, payload, (size_t)payload_len) != 0) goto fail;
    return msg;
fail:
    nng_msg_free(msg);
    return NULL;
}

nng_msg *ducknng_error_msg(const char *name, int32_t code, const char *message) {
    char errbuf[64];
    snprintf(errbuf, sizeof(errbuf), "%d", (int)code);
    (void)errbuf;
    return ducknng_build_reply(DUCKNNG_RPC_ERROR, name, 0,
        message ? message : "ducknng: unspecified error", NULL, 0);
}
