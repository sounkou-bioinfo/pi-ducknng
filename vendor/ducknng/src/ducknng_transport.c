#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <ctype.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

void ducknng_transport_url_init(ducknng_transport_url *out) {
    if (!out) return;
    memset(out, 0, sizeof(*out));
}

static int ducknng_scheme_matches(const char *url, size_t len, const char *want) {
    size_t i;
    size_t want_len;
    if (!url || !want) return 0;
    want_len = strlen(want);
    if (len != want_len) return 0;
    for (i = 0; i < len; i++) {
        if ((char)tolower((unsigned char)url[i]) != want[i]) return 0;
    }
    return 1;
}

int ducknng_transport_url_parse(const char *url, ducknng_transport_url *out, char **errmsg) {
    const char *sep;
    size_t scheme_len;
    ducknng_transport_url parsed;
    if (errmsg) *errmsg = NULL;
    ducknng_transport_url_init(&parsed);
    if (!url || !url[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: transport URL is required");
        return -1;
    }
    sep = strstr(url, "://");
    if (!sep || sep == url) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: transport URL must include a scheme such as ipc:// or https://");
        return -1;
    }
    scheme_len = (size_t)(sep - url);
    if (ducknng_scheme_matches(url, scheme_len, "inproc")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_NNG;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_INPROC;
    } else if (ducknng_scheme_matches(url, scheme_len, "ipc")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_NNG;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_IPC;
    } else if (ducknng_scheme_matches(url, scheme_len, "tcp")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_NNG;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_TCP;
    } else if (ducknng_scheme_matches(url, scheme_len, "tls+tcp")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_NNG;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_TLS_TCP;
        parsed.uses_tls = 1;
    } else if (ducknng_scheme_matches(url, scheme_len, "ws")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_NNG;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_WS;
    } else if (ducknng_scheme_matches(url, scheme_len, "wss")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_NNG;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_WSS;
        parsed.uses_tls = 1;
    } else if (ducknng_scheme_matches(url, scheme_len, "http")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_HTTP;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_HTTP;
    } else if (ducknng_scheme_matches(url, scheme_len, "https")) {
        parsed.family = DUCKNNG_TRANSPORT_FAMILY_HTTP;
        parsed.scheme = DUCKNNG_TRANSPORT_SCHEME_HTTPS;
        parsed.uses_tls = 1;
    } else {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported transport URL scheme");
        return -1;
    }
    if (out) *out = parsed;
    return 0;
}

int ducknng_transport_url_is_nng(const ducknng_transport_url *url) {
    return url && url->family == DUCKNNG_TRANSPORT_FAMILY_NNG;
}

int ducknng_transport_url_is_http(const ducknng_transport_url *url) {
    return url && url->family == DUCKNNG_TRANSPORT_FAMILY_HTTP;
}

const char *ducknng_transport_family_name(ducknng_transport_family family) {
    switch (family) {
    case DUCKNNG_TRANSPORT_FAMILY_NNG: return "nng";
    case DUCKNNG_TRANSPORT_FAMILY_HTTP: return "http";
    default: return "unknown";
    }
}

const char *ducknng_transport_scheme_name(ducknng_transport_scheme scheme) {
    switch (scheme) {
    case DUCKNNG_TRANSPORT_SCHEME_INPROC: return "inproc";
    case DUCKNNG_TRANSPORT_SCHEME_IPC: return "ipc";
    case DUCKNNG_TRANSPORT_SCHEME_TCP: return "tcp";
    case DUCKNNG_TRANSPORT_SCHEME_TLS_TCP: return "tls+tcp";
    case DUCKNNG_TRANSPORT_SCHEME_WS: return "ws";
    case DUCKNNG_TRANSPORT_SCHEME_WSS: return "wss";
    case DUCKNNG_TRANSPORT_SCHEME_HTTP: return "http";
    case DUCKNNG_TRANSPORT_SCHEME_HTTPS: return "https";
    default: return "unknown";
    }
}
