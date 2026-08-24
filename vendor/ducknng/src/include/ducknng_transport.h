#pragma once
#include <stddef.h>

typedef enum ducknng_transport_family {
    DUCKNNG_TRANSPORT_FAMILY_UNKNOWN = 0,
    DUCKNNG_TRANSPORT_FAMILY_NNG = 1,
    DUCKNNG_TRANSPORT_FAMILY_HTTP = 2
} ducknng_transport_family;

typedef enum ducknng_transport_scheme {
    DUCKNNG_TRANSPORT_SCHEME_UNKNOWN = 0,
    DUCKNNG_TRANSPORT_SCHEME_INPROC = 1,
    DUCKNNG_TRANSPORT_SCHEME_IPC = 2,
    DUCKNNG_TRANSPORT_SCHEME_TCP = 3,
    DUCKNNG_TRANSPORT_SCHEME_TLS_TCP = 4,
    DUCKNNG_TRANSPORT_SCHEME_WS = 5,
    DUCKNNG_TRANSPORT_SCHEME_WSS = 6,
    DUCKNNG_TRANSPORT_SCHEME_HTTP = 7,
    DUCKNNG_TRANSPORT_SCHEME_HTTPS = 8
} ducknng_transport_scheme;

typedef struct ducknng_transport_url {
    ducknng_transport_family family;
    ducknng_transport_scheme scheme;
    int uses_tls;
} ducknng_transport_url;

void ducknng_transport_url_init(ducknng_transport_url *out);
int ducknng_transport_url_parse(const char *url, ducknng_transport_url *out, char **errmsg);
int ducknng_transport_url_is_nng(const ducknng_transport_url *url);
int ducknng_transport_url_is_http(const ducknng_transport_url *url);
const char *ducknng_transport_family_name(ducknng_transport_family family);
const char *ducknng_transport_scheme_name(ducknng_transport_scheme scheme);
