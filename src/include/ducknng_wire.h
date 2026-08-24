#pragma once
#include "ducknng_wire_core.h"
#include <nng/nng.h>

#define DUCKNNG_VERSION "0.1.2.9000"

/* NNG adapter over the counted frame core. The returned frame borrows the
 * nng_msg body and is valid only while that message remains alive. */
int ducknng_decode_request(nng_msg *msg, ducknng_frame *out);
nng_msg *ducknng_build_reply(uint8_t type, const char *name, uint32_t flags,
    const char *error, const void *payload, uint64_t payload_len);
nng_msg *ducknng_build_reply_status(uint8_t type, const char *name,
    uint32_t flags, int32_t status, const char *error,
    const void *payload, uint64_t payload_len);
nng_msg *ducknng_error_msg(const char *name, int32_t code,
    const char *message);
