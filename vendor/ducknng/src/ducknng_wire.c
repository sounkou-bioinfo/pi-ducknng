#include "ducknng_wire.h"
#include <string.h>

int ducknng_decode_request(nng_msg *msg, ducknng_frame *out) {
    const uint8_t *data;
    size_t len;
    if (!msg) return -1;
    data = (const uint8_t *)nng_msg_body(msg);
    len = nng_msg_len(msg);
    return ducknng_decode_frame_bytes(data, len, out);
}

nng_msg *ducknng_build_reply_status(uint8_t type, const char *name,
    uint32_t flags, int32_t status, const char *error,
    const void *payload, uint64_t payload_len) {
    nng_msg *msg = NULL;
    ducknng_frame_parts parts;
    size_t required = 0;
    size_t written = 0;
    if (payload_len > SIZE_MAX) return NULL;
    memset(&parts, 0, sizeof(parts));
    parts.type = type;
    parts.status = status;
    parts.flags = flags;
    parts.name = (const uint8_t *)name;
    parts.name_len = name ? strlen(name) : 0;
    parts.error = (const uint8_t *)error;
    parts.error_len = error ? strlen(error) : 0;
    parts.payload = (const uint8_t *)payload;
    parts.payload_len = (size_t)payload_len;
    if (ducknng_frame_measure(&parts, &required) != 0 ||
        nng_msg_alloc(&msg, required) != 0) return NULL;
    if (ducknng_encode_frame_bytes(&parts, (uint8_t *)nng_msg_body(msg),
            nng_msg_len(msg), &written) != 0 || written != required) {
        nng_msg_free(msg);
        return NULL;
    }
    return msg;
}

nng_msg *ducknng_build_reply(uint8_t type, const char *name, uint32_t flags,
    const char *error, const void *payload, uint64_t payload_len) {
    return ducknng_build_reply_status(type, name, flags, DUCKNNG_STATUS_OK,
        error, payload, payload_len);
}

nng_msg *ducknng_error_msg(const char *name, int32_t code,
    const char *message) {
    /* `code` became the validated frame status, so a caller passing one
     * outside the assigned error range would otherwise get NULL and be
     * reported as an allocation failure by callers that drop the connection on
     * NULL. Report the error with a generic status instead: NULL from here
     * means only that the message could not be built. */
    if (code <= DUCKNNG_STATUS_OK || code > DUCKNNG_STATUS_DISABLED) {
        code = DUCKNNG_STATUS_INTERNAL;
    }
    return ducknng_build_reply_status(DUCKNNG_RPC_ERROR, name, 0, code,
        message ? message : "ducknng: unspecified error", NULL, 0);
}
