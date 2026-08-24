#include "ducknng_http_compat.h"
#include "ducknng_net_backend.h"
#include "ducknng_runtime.h"
#include "ducknng_service.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include "ducknng_wasm_http_fetch.h"
#include "ducknng_ws_frame.h"
#include <ctype.h>
#include <nng/supplemental/http/http.h>
#include "../third_party/nng/src/core/defs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* Vendored NNG internal helpers kept isolated in the HTTP compat layer. */
extern char *nni_http_req_headers(nng_http_req *req);
extern char *nni_http_res_headers(nng_http_res *res);
extern int nni_http_conn_getopt(nng_http_conn *conn, const char *name, void *buf, size_t *szp, nni_type type);
extern void nni_strfree(char *s);

int ducknng_http_tls_requested(const ducknng_tls_opts *opts) {
    return opts && (opts->enabled ||
        (opts->cert_key_file && opts->cert_key_file[0]) ||
        (opts->ca_file && opts->ca_file[0]) ||
        (opts->cert_pem && opts->cert_pem[0]) ||
        (opts->key_pem && opts->key_pem[0]) ||
        (opts->ca_pem && opts->ca_pem[0]) ||
        (opts->password && opts->password[0]) ||
        opts->auth_mode != 0);
}

static int ducknng_http_tls_auth_mode_map(nng_tls_mode mode, int auth_mode,
    nng_tls_auth_mode *out) {
    if (!out) return NNG_EINVAL;
    switch (auth_mode) {
    case 0:
        *out = (mode == NNG_TLS_MODE_CLIENT) ? NNG_TLS_AUTH_MODE_REQUIRED : NNG_TLS_AUTH_MODE_NONE;
        return 0;
    case 1: *out = NNG_TLS_AUTH_MODE_OPTIONAL; return 0;
    case 2: *out = NNG_TLS_AUTH_MODE_REQUIRED; return 0;
    default: return NNG_EINVAL;
    }
}

static char *ducknng_http_tls_identity_from_value(const char *kind, const char *value) {
    static const char prefix[] = "tls:";
    size_t kind_len;
    size_t value_len;
    size_t need;
    char *out;
    if (!kind || !kind[0] || !value || !value[0]) return NULL;
    kind_len = strlen(kind);
    value_len = strlen(value);
    need = sizeof(prefix) - 1 + kind_len + 1 + value_len + 1;
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    memcpy(out, prefix, sizeof(prefix) - 1);
    memcpy(out + sizeof(prefix) - 1, kind, kind_len);
    out[sizeof(prefix) - 1 + kind_len] = ':';
    memcpy(out + sizeof(prefix) + kind_len, value, value_len + 1);
    return out;
}

static char *ducknng_http_tls_identity_from_alt_names(char **alt_names) {
    size_t i;
    if (!alt_names) return NULL;
    for (i = 0; alt_names[i]; i++) {
        if (alt_names[i][0]) return ducknng_http_tls_identity_from_value("san", alt_names[i]);
    }
    return NULL;
}

static void ducknng_http_tls_alt_names_free(char **alt_names) {
    size_t i;
    if (!alt_names) return;
    for (i = 0; alt_names[i]; i++) nng_strfree(alt_names[i]);
    free(alt_names);
}

static int ducknng_http_conn_remote_addr(nng_http_conn *conn, nng_sockaddr *out) {
    size_t size = sizeof(*out);
    if (!conn || !out) return -1;
    memset(out, 0, sizeof(*out));
    return nni_http_conn_getopt(conn, NNG_OPT_REMADDR, out, &size, NNI_TYPE_SOCKADDR) == 0 ? 0 : -1;
}

static char *ducknng_http_conn_verified_peer_identity(nng_http_conn *conn) {
    bool verified = false;
    char **alt_names = NULL;
    char *cn = NULL;
    char *identity = NULL;
    if (!conn) return NULL;
    if (nni_http_conn_getopt(conn, NNG_OPT_TLS_VERIFIED, &verified, NULL,
            NNI_TYPE_BOOL) != 0 || !verified) {
        return NULL;
    }
    if (nni_http_conn_getopt(conn, NNG_OPT_TLS_PEER_ALT_NAMES, &alt_names, NULL,
            NNI_TYPE_POINTER) == 0 && alt_names) {
        identity = ducknng_http_tls_identity_from_alt_names(alt_names);
    }
    ducknng_http_tls_alt_names_free(alt_names);
    if (!identity && nni_http_conn_getopt(conn, NNG_OPT_TLS_PEER_CN, &cn, NULL,
            NNI_TYPE_STRING) == 0 && cn && cn[0]) {
        identity = ducknng_http_tls_identity_from_value("cn", cn);
    }
    if (cn) nng_strfree(cn);
    return identity;
}

static int ducknng_http_tls_config_build(nng_tls_config **out, nng_tls_mode mode,
    const char *url, const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    nng_tls_auth_mode auth_mode;
    nng_url *up = NULL;
    int rv;
    if (!out) return NNG_EINVAL;
    *out = NULL;
    if (!opts || !ducknng_http_tls_requested(opts)) return 0;
    rv = nng_tls_config_alloc(&cfg, mode);
    if (rv != 0) return rv;
    rv = ducknng_http_tls_auth_mode_map(mode, opts->auth_mode, &auth_mode);
    if (rv != 0) goto fail;
    rv = nng_tls_config_auth_mode(cfg, auth_mode);
    if (rv != 0) goto fail;
    if (opts->ca_file && opts->ca_file[0]) {
        rv = nng_tls_config_ca_file(cfg, opts->ca_file);
        if (rv != 0) goto fail;
    } else if (opts->ca_pem && opts->ca_pem[0]) {
        rv = nng_tls_config_ca_chain(cfg, opts->ca_pem, NULL);
        if (rv != 0) goto fail;
    }
    if (opts->cert_key_file && opts->cert_key_file[0]) {
        rv = nng_tls_config_cert_key_file(cfg, opts->cert_key_file, opts->password);
        if (rv != 0) goto fail;
    } else if (opts->cert_pem && opts->cert_pem[0] && opts->key_pem && opts->key_pem[0]) {
        rv = nng_tls_config_own_cert(cfg, opts->cert_pem, opts->key_pem, opts->password);
        if (rv != 0) goto fail;
    }
    if (mode == NNG_TLS_MODE_CLIENT && url && nng_url_parse(&up, url) == 0 && up && up->u_hostname && up->u_hostname[0]) {
        rv = nng_tls_config_server_name(cfg, up->u_hostname);
        if (rv != 0) goto fail;
    }
    if (up) nng_url_free(up);
    *out = cfg;
    return 0;
fail:
    if (up) nng_url_free(up);
    if (cfg) nng_tls_config_free(cfg);
    return rv;
}

static void ducknng_http_skip_ws(const char **p) {
    while (p && *p && **p && isspace((unsigned char)**p)) (*p)++;
}

static int ducknng_http_buf_append(char **buf, size_t *len, size_t *cap, const char *src, size_t src_len) {
    char *new_buf;
    size_t new_cap;
    if (!buf || !len || !cap) return -1;
    if (*cap < *len + src_len + 1) {
        new_cap = *cap ? *cap * 2 : 128;
        while (new_cap < *len + src_len + 1) new_cap *= 2;
        new_buf = (char *)duckdb_malloc(new_cap);
        if (!new_buf) return -1;
        if (*buf && *len) memcpy(new_buf, *buf, *len);
        if (*buf) duckdb_free(*buf);
        *buf = new_buf;
        *cap = new_cap;
    }
    if (src_len) memcpy(*buf + *len, src, src_len);
    *len += src_len;
    (*buf)[*len] = '\0';
    return 0;
}

static int ducknng_http_append_json_string(char **buf, size_t *len, size_t *cap, const char *src) {
    size_t i;
    if (ducknng_http_buf_append(buf, len, cap, "\"", 1) != 0) return -1;
    if (!src) src = "";
    for (i = 0; src[i]; i++) {
        unsigned char c = (unsigned char)src[i];
        switch (c) {
        case '"': if (ducknng_http_buf_append(buf, len, cap, "\\\"", 2) != 0) return -1; break;
        case '\\': if (ducknng_http_buf_append(buf, len, cap, "\\\\", 2) != 0) return -1; break;
        case '\b': if (ducknng_http_buf_append(buf, len, cap, "\\b", 2) != 0) return -1; break;
        case '\f': if (ducknng_http_buf_append(buf, len, cap, "\\f", 2) != 0) return -1; break;
        case '\n': if (ducknng_http_buf_append(buf, len, cap, "\\n", 2) != 0) return -1; break;
        case '\r': if (ducknng_http_buf_append(buf, len, cap, "\\r", 2) != 0) return -1; break;
        case '\t': if (ducknng_http_buf_append(buf, len, cap, "\\t", 2) != 0) return -1; break;
        default:
            if (c < 0x20) {
                char esc[7];
                snprintf(esc, sizeof(esc), "\\u%04x", (unsigned)c);
                if (ducknng_http_buf_append(buf, len, cap, esc, 6) != 0) return -1;
            } else {
                char one = (char)c;
                if (ducknng_http_buf_append(buf, len, cap, &one, 1) != 0) return -1;
            }
        }
    }
    if (ducknng_http_buf_append(buf, len, cap, "\"", 1) != 0) return -1;
    return 0;
}

static char *ducknng_http_parse_json_string(const char **p, char **errmsg) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    if (!p || !*p || **p != '"') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected JSON string");
        return NULL;
    }
    (*p)++;
    while (**p) {
        char c = *(*p)++;
        if (c == '"') return buf ? buf : ducknng_strdup("");
        if (c == '\\') {
            char esc = *(*p)++;
            char out;
            switch (esc) {
            case '"': out = '"'; break;
            case '\\': out = '\\'; break;
            case '/': out = '/'; break;
            case 'b':
            case 'f':
            case 'n':
            case 'r':
            case 't':
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: control characters are not allowed in headers_json strings");
                return NULL;
            case 'u': {
                int i;
                unsigned value = 0;
                for (i = 0; i < 4; i++) {
                    char h = *(*p)++;
                    if (h >= '0' && h <= '9') value = (value << 4) | (unsigned)(h - '0');
                    else if (h >= 'a' && h <= 'f') value = (value << 4) | (unsigned)(h - 'a' + 10);
                    else if (h >= 'A' && h <= 'F') value = (value << 4) | (unsigned)(h - 'A' + 10);
                    else {
                        if (buf) duckdb_free(buf);
                        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid JSON unicode escape in headers_json");
                        return NULL;
                    }
                }
                if (value > 0x7f) {
                    if (buf) duckdb_free(buf);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: only ASCII JSON unicode escapes are supported in headers_json");
                    return NULL;
                }
                if (value < 0x20 || value == 0x7f) {
                    if (buf) duckdb_free(buf);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: control characters are not allowed in headers_json strings");
                    return NULL;
                }
                out = (char)value;
                break;
            }
            default:
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported JSON escape in headers_json");
                return NULL;
            }
            if (ducknng_http_buf_append(&buf, &len, &cap, &out, 1) != 0) {
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing headers_json");
                return NULL;
            }
        } else {
            unsigned char uc = (unsigned char)c;
            if (uc < 0x20 || uc == 0x7f) {
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: control characters are not allowed in headers_json strings");
                return NULL;
            }
            if (ducknng_http_buf_append(&buf, &len, &cap, &c, 1) != 0) {
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing headers_json");
                return NULL;
            }
        }
    }
    if (buf) duckdb_free(buf);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: unterminated JSON string in headers_json");
    return NULL;
}

static int ducknng_http_apply_headers_json_common(const char *headers_json,
    void *target, int (*add_header)(void *, const char *, const char *), char **errmsg) {
    const char *p = headers_json;
    if (errmsg) *errmsg = NULL;
    if (!headers_json || !headers_json[0] || !add_header) return 0;
    ducknng_http_skip_ws(&p);
    if (*p != '[') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: headers_json must be a JSON array of {name,value} objects");
        return -1;
    }
    p++;
    ducknng_http_skip_ws(&p);
    if (*p == ']') {
        p++;
        ducknng_http_skip_ws(&p);
        if (*p != '\0') {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after headers_json");
            return -1;
        }
        return 0;
    }
    for (;;) {
        char *key = NULL;
        char *value = NULL;
        char *name = NULL;
        char *header_value = NULL;
        int rv;
        ducknng_http_skip_ws(&p);
        if (*p != '{') {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: expected header object in headers_json");
            return -1;
        }
        p++;
        for (;;) {
            ducknng_http_skip_ws(&p);
            key = ducknng_http_parse_json_string(&p, errmsg);
            if (!key) return -1;
            ducknng_http_skip_ws(&p);
            if (*p != ':') {
                duckdb_free(key);
                if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: expected ':' in headers_json");
                return -1;
            }
            p++;
            ducknng_http_skip_ws(&p);
            value = ducknng_http_parse_json_string(&p, errmsg);
            if (!value) {
                duckdb_free(key);
                return -1;
            }
            if (strcmp(key, "name") == 0) {
                if (name) {
                    duckdb_free(key);
                    duckdb_free(value);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: duplicate header name field in headers_json");
                    if (header_value) duckdb_free(header_value);
                    return -1;
                }
                name = value;
            } else if (strcmp(key, "value") == 0) {
                if (header_value) {
                    duckdb_free(key);
                    duckdb_free(value);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: duplicate header value field in headers_json");
                    if (name) duckdb_free(name);
                    return -1;
                }
                header_value = value;
            } else {
                duckdb_free(value);
                duckdb_free(key);
                if (name) duckdb_free(name);
                if (header_value) duckdb_free(header_value);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: headers_json objects may contain only name and value fields");
                return -1;
            }
            duckdb_free(key);
            ducknng_http_skip_ws(&p);
            if (*p == ',') {
                p++;
                continue;
            }
            if (*p == '}') {
                p++;
                break;
            }
            if (name) duckdb_free(name);
            if (header_value) duckdb_free(header_value);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or '}' in headers_json");
            return -1;
        }
        if (!name || !name[0] || !header_value) {
            if (name) duckdb_free(name);
            if (header_value) duckdb_free(header_value);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: each headers_json object must contain non-empty name and value strings");
            return -1;
        }
        if (!ducknng_http_token_is_valid(name)) {
            duckdb_free(name);
            duckdb_free(header_value);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP header name must be an HTTP token");
            return -1;
        }
        if (!ducknng_http_header_value_is_valid(header_value)) {
            duckdb_free(name);
            duckdb_free(header_value);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP header value must not contain control characters");
            return -1;
        }
        rv = add_header(target, name, header_value);
        duckdb_free(name);
        duckdb_free(header_value);
        if (rv != 0) {
            if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
            return -1;
        }
        ducknng_http_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            ducknng_http_skip_ws(&p);
            if (*p != '\0') {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after headers_json");
                return -1;
            }
            return 0;
        }
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or ']' in headers_json");
        return -1;
    }
}

static int ducknng_http_req_add_header_adapter(void *target, const char *name, const char *value) {
    return nng_http_req_add_header((nng_http_req *)target, name, value);
}

static int ducknng_http_res_add_header_adapter(void *target, const char *name, const char *value) {
    return nng_http_res_add_header((nng_http_res *)target, name, value);
}

static int ducknng_http_validate_header_adapter(void *target, const char *name, const char *value) {
    (void)target;
    (void)name;
    (void)value;
    return 0;
}

int ducknng_http_validate_headers_json(const char *headers_json, char **errmsg) {
    return ducknng_http_apply_headers_json_common(headers_json, NULL,
        ducknng_http_validate_header_adapter, errmsg);
}

static int ducknng_http_apply_headers_json(nng_http_req *req, const char *headers_json, char **errmsg) {
    return ducknng_http_apply_headers_json_common(headers_json, req,
        ducknng_http_req_add_header_adapter, errmsg);
}

static int ducknng_http_apply_response_headers_json(nng_http_res *res, const char *headers_json,
    char **errmsg) {
    return ducknng_http_apply_headers_json_common(headers_json, res,
        ducknng_http_res_add_header_adapter, errmsg);
}

static char *ducknng_http_dup_bytes(const uint8_t *data, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, data, len);
    out[len] = '\0';
    return out;
}

static int ducknng_http_bytes_look_text(const uint8_t *data, size_t len) {
    size_t i;
    if (!data) return 0;
    for (i = 0; i < len; i++) {
        uint8_t c = data[i];
        if (c == 9 || c == 10 || c == 13) continue;
        if (c < 32 || c == 127) return 0;
    }
    return 1;
}

char *ducknng_http_headers_block_to_json(const char *headers_block, char **errmsg) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    const char *line = headers_block;
    int first = 1;
    if (errmsg) *errmsg = NULL;
    if (ducknng_http_buf_append(&buf, &len, &cap, "[", 1) != 0) goto oom;
    if (!headers_block || !headers_block[0]) {
        if (ducknng_http_buf_append(&buf, &len, &cap, "]", 1) != 0) goto oom;
        return buf;
    }
    while (*line) {
        const char *line_end = strstr(line, "\r\n");
        const char *colon;
        const char *value;
        size_t name_len;
        size_t value_len;
        char *name_copy;
        char *value_copy;
        if (!line_end) line_end = line + strlen(line);
        if (line_end == line) break;
        colon = memchr(line, ':', (size_t)(line_end - line));
        if (!colon) {
            if (line_end[0] == '\0') break;
            line = line_end[0] ? line_end + 2 : line_end;
            continue;
        }
        name_len = (size_t)(colon - line);
        value = colon + 1;
        while (value < line_end && (*value == ' ' || *value == '\t')) value++;
        value_len = (size_t)(line_end - value);
        name_copy = (char *)duckdb_malloc(name_len + 1);
        value_copy = (char *)duckdb_malloc(value_len + 1);
        if (!name_copy || !value_copy) {
            if (name_copy) duckdb_free(name_copy);
            if (value_copy) duckdb_free(value_copy);
            goto oom;
        }
        memcpy(name_copy, line, name_len);
        name_copy[name_len] = '\0';
        memcpy(value_copy, value, value_len);
        value_copy[value_len] = '\0';
        if (!first && ducknng_http_buf_append(&buf, &len, &cap, ",", 1) != 0) {
            duckdb_free(name_copy);
            duckdb_free(value_copy);
            goto oom;
        }
        first = 0;
        if (ducknng_http_buf_append(&buf, &len, &cap, "{\"name\":", 8) != 0 ||
            ducknng_http_append_json_string(&buf, &len, &cap, name_copy) != 0 ||
            ducknng_http_buf_append(&buf, &len, &cap, ",\"value\":", 9) != 0 ||
            ducknng_http_append_json_string(&buf, &len, &cap, value_copy) != 0 ||
            ducknng_http_buf_append(&buf, &len, &cap, "}", 1) != 0) {
            duckdb_free(name_copy);
            duckdb_free(value_copy);
            goto oom;
        }
        duckdb_free(name_copy);
        duckdb_free(value_copy);
        line = line_end[0] ? line_end + 2 : line_end;
    }
    if (ducknng_http_buf_append(&buf, &len, &cap, "]", 1) != 0) goto oom;
    return buf;
oom:
    if (buf) duckdb_free(buf);
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building headers_json");
    return NULL;
}

static char *ducknng_http_request_headers_json_dup(nng_http_req *req, char **errmsg) {
    char *headers_block;
    char *headers_json;
    if (errmsg) *errmsg = NULL;
    if (!req) return ducknng_strdup("[]");
    headers_block = nni_http_req_headers(req);
    headers_json = ducknng_http_headers_block_to_json(headers_block, errmsg);
    if (headers_block) nni_strfree(headers_block);
    return headers_json;
}

int ducknng_validate_http_url(const char *url, char **errmsg) {
    ducknng_transport_url parsed;
    char *parse_err = NULL;
    if (errmsg) *errmsg = NULL;
    if (ducknng_transport_url_parse(url, &parsed, &parse_err) != 0) {
        if (errmsg) *errmsg = parse_err;
        else if (parse_err) duckdb_free(parse_err);
        return -1;
    }
    if (!ducknng_transport_url_is_http(&parsed)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: http:// or https:// URL is required");
        return -1;
    }
    return 0;
}

int ducknng_http_response_copy(nng_http_res *res, uint16_t *out_status,
    char **out_headers_json, uint8_t **out_body, size_t *out_body_len, char **errmsg) {
    char *header_block = NULL;
    void *resp_body = NULL;
    size_t resp_body_len = 0;
    int rc = -1;
    if (out_status) *out_status = 0;
    if (out_headers_json) *out_headers_json = NULL;
    if (out_body) *out_body = NULL;
    if (out_body_len) *out_body_len = 0;
    if (errmsg) *errmsg = NULL;
    if (!res) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP response state");
        return -1;
    }
    if (out_status) *out_status = nng_http_res_get_status(res);
    header_block = nni_http_res_headers(res);
    if (out_headers_json) {
        *out_headers_json = ducknng_http_headers_block_to_json(header_block, errmsg);
        if (!*out_headers_json && header_block) goto cleanup;
    }
    nng_http_res_get_data(res, &resp_body, &resp_body_len);
    if (out_body_len) *out_body_len = resp_body_len;
    if (out_body && resp_body_len > 0) {
        *out_body = (uint8_t *)duckdb_malloc(resp_body_len);
        if (!*out_body) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP response body");
            goto cleanup;
        }
        memcpy(*out_body, resp_body, resp_body_len);
    }
    rc = 0;
cleanup:
    if (header_block) nni_strfree(header_block);
    if (rc != 0) {
        if (out_headers_json && *out_headers_json) {
            duckdb_free(*out_headers_json);
            *out_headers_json = NULL;
        }
        if (out_body && *out_body) {
            duckdb_free(*out_body);
            *out_body = NULL;
        }
        if (out_body_len) *out_body_len = 0;
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy HTTP response");
    }
    return rc;
}

int ducknng_http_transact_aio_prepare(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, const ducknng_tls_opts *tls_opts,
    nng_url **out_url, nng_http_client **out_client, nng_http_req **out_req,
    nng_http_res **out_res, char **errmsg) {
    ducknng_transport_url parsed;
    nng_url *parsed_url = NULL;
    nng_http_client *client = NULL;
    nng_http_req *req = NULL;
    nng_http_res *res = NULL;
    nng_tls_config *tls_cfg = NULL;
    int rv;
    if (out_url) *out_url = NULL;
    if (out_client) *out_client = NULL;
    if (out_req) *out_req = NULL;
    if (out_res) *out_res = NULL;
    if (errmsg) *errmsg = NULL;
    if (!out_url || !out_client || !out_req || !out_res) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP aio state");
        return -1;
    }
    if (ducknng_validate_http_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (ducknng_http_tls_requested(tls_opts) && !parsed.uses_tls) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires an https:// URL");
        return -1;
    }
    rv = nng_url_parse(&parsed_url, url);
    if (rv != 0) goto fail;
    rv = nng_http_client_alloc(&client, parsed_url);
    if (rv != 0) goto fail;
    rv = nng_http_req_alloc(&req, parsed_url);
    if (rv != 0) goto fail;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) goto fail;
    if (method && method[0]) {
        rv = nng_http_req_set_method(req, method);
        if (rv != 0) goto fail;
    }
    if (headers_json && headers_json[0]) {
        if (ducknng_http_apply_headers_json(req, headers_json, errmsg) != 0) {
            rv = NNG_EINVAL;
            goto fail;
        }
    }
    if (body && body_len > 0) {
        rv = nng_http_req_copy_data(req, body, body_len);
        if (rv != 0) goto fail;
    }
    if (parsed.uses_tls && ducknng_http_tls_requested(tls_opts)) {
        rv = ducknng_http_tls_config_build(&tls_cfg, NNG_TLS_MODE_CLIENT, url, tls_opts);
        if (rv != 0) goto fail;
        if (tls_cfg) {
            rv = nng_http_client_set_tls(client, tls_cfg);
            nng_tls_config_free(tls_cfg);
            tls_cfg = NULL;
            if (rv != 0) goto fail;
        }
    }
    *out_url = parsed_url;
    *out_client = client;
    *out_req = req;
    *out_res = res;
    return 0;
fail:
    if (tls_cfg) nng_tls_config_free(tls_cfg);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    if (client) nng_http_client_free(client);
    if (parsed_url) nng_url_free(parsed_url);
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    return -1;
}

int ducknng_http_frame_transact_aio_prepare(const char *url, const uint8_t *frame, size_t frame_len,
    const ducknng_tls_opts *tls_opts, nng_url **out_url, nng_http_client **out_client,
    nng_http_req **out_req, nng_http_res **out_res, char **errmsg) {
    return ducknng_http_transact_aio_prepare(url, "POST",
        "[{\"name\":\"Content-Type\",\"value\":\"application/vnd.ducknng.frame\"}]",
        frame, frame_len, tls_opts, out_url, out_client, out_req, out_res, errmsg);
}

int ducknng_http_transact(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    uint16_t *out_status, char **out_headers_json, uint8_t **out_body, size_t *out_body_len,
    char **errmsg) {
    return ducknng_net_backend_get()->http_transact(url, method, headers_json,
        body, body_len, timeout_ms, tls_opts, out_status, out_headers_json,
        out_body, out_body_len, errmsg);
}

#ifdef __EMSCRIPTEN__
/* In the browser there is no native socket layer for the NNG HTTP client.
 * Route http:// and https:// through the synchronous XHR bridge.
 * TLS is browser-managed, so an explicit TLS configuration is rejected. */
int ducknng_http_transact_browser(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    uint16_t *out_status, char **out_headers_json, uint8_t **out_body, size_t *out_body_len,
    char **errmsg) {
    ducknng_transport_url parsed;
    char *header_block = NULL;
    int frc;
    if (out_status) *out_status = 0;
    if (out_headers_json) *out_headers_json = NULL;
    if (out_body) *out_body = NULL;
    if (out_body_len) *out_body_len = 0;
    if (errmsg) *errmsg = NULL;
    if (ducknng_validate_http_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (ducknng_http_tls_requested(tls_opts)) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: explicit TLS configuration is unsupported in the browser; https uses browser-managed TLS");
        return -1;
    }
    if (method && method[0] && !ducknng_http_token_is_valid(method)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP method must be an HTTP token");
        return -1;
    }
    if (headers_json && headers_json[0] &&
        ducknng_http_validate_headers_json(headers_json, errmsg) != 0) {
        return -1;
    }
    frc = ducknng_wasm_http_fetch_perform(url, method, headers_json, body, body_len,
        timeout_ms, out_status, &header_block, out_body, out_body_len, errmsg);
    if (frc != 0) {
        if (header_block) duckdb_free(header_block);
        return -1;
    }
    if (out_headers_json) {
        *out_headers_json = ducknng_http_headers_block_to_json(header_block, errmsg);
        if (!*out_headers_json) {
            if (header_block) duckdb_free(header_block);
            if (out_body && *out_body) {
                duckdb_free(*out_body);
                *out_body = NULL;
            }
            if (out_body_len) *out_body_len = 0;
            return -1;
        }
    }
    if (header_block) duckdb_free(header_block);
    return 0;
}
#endif

int ducknng_http_transact_native(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    uint16_t *out_status, char **out_headers_json, uint8_t **out_body, size_t *out_body_len,
    char **errmsg) {
    ducknng_transport_url parsed;
    nng_url *parsed_url = NULL;
    nng_http_client *client = NULL;
    nng_http_req *req = NULL;
    nng_http_res *res = NULL;
    nng_aio *aio = NULL;
    nng_tls_config *tls_cfg = NULL;
    char *header_block = NULL;
    void *resp_body = NULL;
    size_t resp_body_len = 0;
    int rv;
    if (out_status) *out_status = 0;
    if (out_headers_json) *out_headers_json = NULL;
    if (out_body) *out_body = NULL;
    if (out_body_len) *out_body_len = 0;
    if (errmsg) *errmsg = NULL;
    if (ducknng_validate_http_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (ducknng_http_tls_requested(tls_opts) && !parsed.uses_tls) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires an https:// URL");
        return -1;
    }
    rv = nng_url_parse(&parsed_url, url);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    rv = nng_http_client_alloc(&client, parsed_url);
    if (rv != 0) goto fail;
    rv = nng_http_req_alloc(&req, parsed_url);
    if (rv != 0) goto fail;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) goto fail;
    if (method && method[0]) {
        rv = nng_http_req_set_method(req, method);
        if (rv != 0) goto fail;
    }
    if (headers_json && headers_json[0]) {
        if (ducknng_http_apply_headers_json(req, headers_json, errmsg) != 0) {
            rv = NNG_EINVAL;
            goto fail;
        }
    }
    if (body && body_len > 0) {
        rv = nng_http_req_copy_data(req, body, body_len);
        if (rv != 0) goto fail;
    }
    if (parsed.uses_tls && ducknng_http_tls_requested(tls_opts)) {
        rv = ducknng_http_tls_config_build(&tls_cfg, NNG_TLS_MODE_CLIENT, url, tls_opts);
        if (rv != 0) goto fail;
        if (tls_cfg) {
            rv = nng_http_client_set_tls(client, tls_cfg);
            nng_tls_config_free(tls_cfg);
            tls_cfg = NULL;
            if (rv != 0) goto fail;
        }
    }
    rv = ducknng_aio_alloc(&aio, NULL, NULL, timeout_ms);
    if (rv != 0) goto fail;
    nng_http_client_transact(client, req, res, aio);
    ducknng_aio_wait(aio);
    rv = ducknng_aio_result(aio);
    if (rv != 0) goto fail;
    if (out_status) *out_status = nng_http_res_get_status(res);
    header_block = nni_http_res_headers(res);
    if (out_headers_json) {
        *out_headers_json = ducknng_http_headers_block_to_json(header_block, errmsg);
        if (!*out_headers_json && header_block) {
            rv = NNG_ENOMEM;
            goto fail;
        }
    }
    nng_http_res_get_data(res, &resp_body, &resp_body_len);
    if (out_body_len) *out_body_len = resp_body_len;
    if (out_body) {
        *out_body = (uint8_t *)duckdb_malloc(resp_body_len);
        if (!*out_body && resp_body_len > 0) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP response body");
            rv = NNG_ENOMEM;
            goto fail;
        }
        if (resp_body_len) memcpy(*out_body, resp_body, resp_body_len);
    }
    if (header_block) nni_strfree(header_block);
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    if (client) nng_http_client_free(client);
    if (parsed_url) nng_url_free(parsed_url);
    return 0;
fail:
    if (tls_cfg) nng_tls_config_free(tls_cfg);
    if (header_block) nni_strfree(header_block);
    if (out_headers_json && *out_headers_json) {
        duckdb_free(*out_headers_json);
        *out_headers_json = NULL;
    }
    if (out_body && *out_body) {
        duckdb_free(*out_body);
        *out_body = NULL;
    }
    if (out_body_len) *out_body_len = 0;
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    if (client) nng_http_client_free(client);
    if (parsed_url) nng_url_free(parsed_url);
    return -1;
}

typedef struct ducknng_http_server_state {
    struct ducknng_service *svc;
    nng_http_server *server;
    nng_http_handler *rpc_handler;
    nng_http_handler *route_handler;
    char *path;
    /* Browser-facing ducknng-frame-over-WebSocket endpoint sharing this server
     * (issue #11); NULL when the endpoint did not start. */
    ducknng_ws_frame_endpoint *ws_endpoint;
    char *ws_url;
    ducknng_mutex mu;
    ducknng_cond cv;
    int stopping;
    int rpc_handler_finalized;
    int rpc_handler_data_installed;
    int route_handler_finalized;
    int route_handler_data_installed;
    size_t active_streams;
    int mu_initialized;
    int cv_initialized;
} ducknng_http_server_state;

typedef struct ducknng_http_handler_data {
    ducknng_http_server_state *state;
    int is_route_handler;
} ducknng_http_handler_data;

static const char *DUCKNNG_HTTP_FRAME_MEDIA_TYPE = "application/vnd.ducknng.frame";
static const char *DUCKNNG_HTTP_FRAME_HEADERS_JSON =
    "[{\"name\":\"Content-Type\",\"value\":\"application/vnd.ducknng.frame\"}]";

const char *ducknng_http_frame_headers_json(void) {
    return DUCKNNG_HTTP_FRAME_HEADERS_JSON;
}

static void ducknng_http_server_state_handler_dtor(void *arg) {
    ducknng_http_handler_data *data = (ducknng_http_handler_data *)arg;
    ducknng_http_server_state *state = data ? data->state : NULL;
    int is_route_handler = data ? data->is_route_handler : 0;
    if (!state || !state->mu_initialized) {
        if (data) duckdb_free(data);
        return;
    }
    ducknng_mutex_lock(&state->mu);
    if (is_route_handler) state->route_handler_finalized = 1;
    else state->rpc_handler_finalized = 1;
    if (state->cv_initialized) ducknng_cond_broadcast(&state->cv);
    ducknng_mutex_unlock(&state->mu);
    duckdb_free(data);
}

static int ducknng_http_server_stream_begin(ducknng_http_server_state *state) {
    if (!state || !state->mu_initialized) return -1;
    ducknng_mutex_lock(&state->mu);
    if (state->stopping) {
        ducknng_mutex_unlock(&state->mu);
        return -1;
    }
    state->active_streams++;
    ducknng_mutex_unlock(&state->mu);
    return 0;
}

static void ducknng_http_server_stream_end(ducknng_http_server_state *state) {
    if (!state || !state->mu_initialized) return;
    ducknng_mutex_lock(&state->mu);
    if (state->active_streams > 0) state->active_streams--;
    if (state->cv_initialized) ducknng_cond_broadcast(&state->cv);
    ducknng_mutex_unlock(&state->mu);
}

static uint16_t ducknng_http_be16_to_host(uint16_t value) {
    return (uint16_t)(((value & 0x00ffu) << 8) | ((value & 0xff00u) >> 8));
}

static char *ducknng_http_url_with_port(const nng_url *up, uint16_t port) {
    char port_buf[32];
    size_t need;
    char *out;
    if (!up || !up->u_scheme || !up->u_hostname) return NULL;
    snprintf(port_buf, sizeof(port_buf), "%u", (unsigned)port);
    need = strlen(up->u_scheme) + strlen(up->u_hostname) + strlen(port_buf) + 8;
    if (up->u_path) need += strlen(up->u_path);
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    snprintf(out, need, "%s://%s:%s%s", up->u_scheme, up->u_hostname, port_buf,
        up->u_path ? up->u_path : "");
    return out;
}

static char *ducknng_http_server_resolve_url(nng_http_server *server, const char *url) {
    nng_url *up = NULL;
    nng_sockaddr addr;
    uint16_t port = 0;
    char *resolved = NULL;
    if (!server || !url) return NULL;
    memset(&addr, 0, sizeof(addr));
    if (nng_url_parse(&up, url) != 0 || !up) goto done;
    if (!up->u_port || strcmp(up->u_port, "0") != 0) goto done;
    if (nng_http_server_get_addr(server, &addr) != 0) goto done;
    if (addr.s_family == NNG_AF_INET) port = ducknng_http_be16_to_host(addr.s_in.sa_port);
    else if (addr.s_family == NNG_AF_INET6) port = ducknng_http_be16_to_host(addr.s_in6.sa_port);
    if (port == 0) goto done;
    resolved = ducknng_http_url_with_port(up, port);
done:
    if (up) nng_url_free(up);
    return resolved;
}

static int ducknng_http_ascii_tolower_int(int c) {
    return (c >= 'A' && c <= 'Z') ? c + ('a' - 'A') : c;
}

static int ducknng_http_content_type_is_frame(const char *content_type) {
    size_t want_len = strlen(DUCKNNG_HTTP_FRAME_MEDIA_TYPE);
    size_t i;
    if (!content_type) return 0;
    while (*content_type == ' ' || *content_type == '\t' || *content_type == '\r' || *content_type == '\n') content_type++;
    for (i = 0; i < want_len; i++) {
        if (!content_type[i] ||
            ducknng_http_ascii_tolower_int((unsigned char)content_type[i]) !=
                ducknng_http_ascii_tolower_int((unsigned char)DUCKNNG_HTTP_FRAME_MEDIA_TYPE[i])) return 0;
    }
    content_type += want_len;
    while (*content_type == ' ' || *content_type == '\t') content_type++;
    return *content_type == '\0' || *content_type == ';';
}

static int ducknng_http_alloc_text_response(nng_http_res **out, uint16_t status, const char *body_text) {
    nng_http_res *res = NULL;
    int rv;
    if (!out) return NNG_EINVAL;
    *out = NULL;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) return rv;
    rv = nng_http_res_set_status(res, status);
    if (rv != 0) goto fail;
    rv = nng_http_res_set_header(res, "Content-Type", "text/plain; charset=utf-8");
    if (rv != 0) goto fail;
    if (body_text && body_text[0]) {
        rv = nng_http_res_copy_data(res, body_text, strlen(body_text));
        if (rv != 0) goto fail;
    }
    *out = res;
    return 0;
fail:
    if (res) nng_http_res_free(res);
    return rv;
}

static int ducknng_http_alloc_frame_response(nng_http_res **out, const void *frame, size_t frame_len) {
    nng_http_res *res = NULL;
    int rv;
    if (!out) return NNG_EINVAL;
    *out = NULL;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) return rv;
    rv = nng_http_res_set_status(res, 200);
    if (rv != 0) goto fail;
    rv = nng_http_res_set_header(res, "Content-Type", DUCKNNG_HTTP_FRAME_MEDIA_TYPE);
    if (rv != 0) goto fail;
    rv = nng_http_res_copy_data(res, frame, frame_len);
    if (rv != 0) goto fail;
    *out = res;
    return 0;
fail:
    if (res) nng_http_res_free(res);
    return rv;
}

static void ducknng_http_finish_response(nng_aio *aio, nng_http_res *res, int rv) {
    if (!aio) {
        if (res) nng_http_res_free(res);
        return;
    }
    if (rv == 0 && res) {
        nng_aio_set_output(aio, 0, res);
        nng_aio_finish(aio, 0);
        return;
    }
    if (res) nng_http_res_free(res);
    nng_aio_finish(aio, rv != 0 ? rv : NNG_EINVAL);
}

static ducknng_http_server_state *ducknng_http_handler_server_state(nng_http_handler *handler) {
    ducknng_http_handler_data *data =
        handler ? (ducknng_http_handler_data *)nng_http_handler_get_data(handler) : NULL;
    return data ? data->state : NULL;
}

static int ducknng_http_route_response_alloc(nng_http_res **out,
    const ducknng_http_route_reply *reply, char **errmsg) {
    nng_http_res *res = NULL;
    int rv;
    if (!out || !reply) return NNG_EINVAL;
    if (errmsg) *errmsg = NULL;
    *out = NULL;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) return rv;
    rv = nng_http_res_set_status(res, (uint16_t)reply->status);
    if (rv != 0) goto fail;
    if (reply->headers_json && reply->headers_json[0] &&
        ducknng_http_apply_response_headers_json(res, reply->headers_json, errmsg) != 0) {
        goto fail;
    }
    if (reply->content_type && reply->content_type[0]) {
        if (!ducknng_http_header_value_is_valid(reply->content_type)) {
            rv = NNG_EINVAL;
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP header value must not contain control characters");
            goto fail;
        }
        rv = nng_http_res_set_header(res, "Content-Type", reply->content_type);
        if (rv != 0) goto fail;
    }
    if (reply->body && reply->body_len > 0) {
        rv = nng_http_res_copy_data(res, reply->body, reply->body_len);
        if (rv != 0) goto fail;
    } else if (reply->body_text && reply->body_text[0]) {
        rv = nng_http_res_copy_data(res, reply->body_text, strlen(reply->body_text));
        if (rv != 0) goto fail;
    }
    *out = res;
    return 0;
fail:
    if (res) nng_http_res_free(res);
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    return rv;
}

/* ---- Chunked streaming helpers ---- */

typedef struct {
    nng_http_conn *conn;
    nng_aio *write_aio;
    int error;
} ducknng_stream_write_ctx;

static int ducknng_http_stream_write_headers(nng_http_conn *conn, nng_aio *write_aio,
    uint16_t status, const char *content_type) {
    nng_http_res *res = NULL;
    int rv;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) return rv;
    rv = nng_http_res_set_status(res, status);
    if (rv != 0) goto fail;
    rv = nng_http_res_set_header(res, "Transfer-Encoding", "chunked");
    if (rv != 0) goto fail;
    if (content_type && content_type[0]) {
        if (!ducknng_http_header_value_is_valid(content_type)) {
            rv = NNG_EINVAL;
            goto fail;
        }
        rv = nng_http_res_set_header(res, "Content-Type", content_type);
        if (rv != 0) goto fail;
    }
    rv = nng_http_res_set_header(res, "Cache-Control", "no-cache");
    if (rv != 0) goto fail;
    nng_http_conn_write_res(conn, res, write_aio);
    nng_aio_wait(write_aio);
    rv = nng_aio_result(write_aio);
fail:
    nng_http_res_free(res);
    return rv;
}

static int ducknng_http_stream_write_chunk(nng_http_conn *conn, nng_aio *write_aio,
    const void *data, size_t len) {
    char header[24];
    int header_len;
    size_t chunk_len;
    uint8_t *buf;
    nng_iov iov;
    int rv;
    if (len == 0) return 0;
    header_len = snprintf(header, sizeof(header), "%zx\r\n", len);
    if (header_len <= 0) return NNG_EINVAL;
    chunk_len = (size_t)header_len + len + 2;
    buf = (uint8_t *)duckdb_malloc(chunk_len);
    if (!buf) return NNG_ENOMEM;
    memcpy(buf, header, (size_t)header_len);
    memcpy(buf + header_len, data, len);
    memcpy(buf + header_len + len, "\r\n", 2);
    iov.iov_buf = buf;
    iov.iov_len = chunk_len;
    rv = nng_aio_set_iov(write_aio, 1, &iov);
    if (rv == 0) {
        nng_http_conn_write_all(conn, write_aio);
        nng_aio_wait(write_aio);
        rv = nng_aio_result(write_aio);
    }
    duckdb_free(buf);
    return rv;
}

static int ducknng_http_stream_write_terminator(nng_http_conn *conn, nng_aio *write_aio) {
    static const char term[] = "0\r\n\r\n";
    nng_iov iov;
    int rv;
    iov.iov_buf = (void *)term;
    iov.iov_len = 5;
    rv = nng_aio_set_iov(write_aio, 1, &iov);
    if (rv == 0) {
        nng_http_conn_write_all(conn, write_aio);
        nng_aio_wait(write_aio);
        rv = nng_aio_result(write_aio);
    }
    return rv;
}

static int ducknng_stream_on_chunk(const void *data, size_t len, void *user_data) {
    ducknng_stream_write_ctx *wctx = (ducknng_stream_write_ctx *)user_data;
    int rv = ducknng_http_stream_write_chunk(wctx->conn, wctx->write_aio, data, len);
    if (rv != 0) wctx->error = rv;
    return rv;
}

static void ducknng_http_serve_stream_route(nng_http_conn *conn, nng_aio *handler_aio,
    ducknng_http_server_state *state, nng_http_req *req,
    const ducknng_http_request_context *request_ctx,
    const char *caller_identity) {
    nng_aio *write_aio = NULL;
    ducknng_stream_write_ctx wctx;
    char *stream_err = NULL;
    const char *ct;
    int rv;
    (void)caller_identity;
    /*
     * Hijack: NNG hands connection and request ownership to us.  The handler
     * AIO must be finished, but ducknng state is protected separately by the
     * active-stream guard held by the caller until after service cleanup.
     */
    rv = nng_http_hijack(conn);
    if (rv != 0) {
        nng_aio_finish(handler_aio, rv);
        return;
    }
    nng_aio_finish(handler_aio, 0);
    ct = request_ctx->route.stream_content_type;
    if (!ct || !ct[0]) ct = "text/event-stream; charset=utf-8";
    rv = nng_aio_alloc(&write_aio, NULL, NULL);
    if (rv != 0) goto done;
    nng_aio_set_timeout(write_aio, 30000); /* 30 s per write */
    rv = ducknng_http_stream_write_headers(conn, write_aio, 200, ct);
    if (rv != 0) goto done;
    memset(&wctx, 0, sizeof(wctx));
    wctx.conn = conn;
    wctx.write_aio = write_aio;
    ducknng_service_execute_stream_route(state->svc, request_ctx,
        ducknng_stream_on_chunk, &wctx, &stream_err);
    if (stream_err) duckdb_free(stream_err);
    ducknng_http_stream_write_terminator(conn, write_aio);
done:
    if (write_aio) nng_aio_free(write_aio);
    if (req) nng_http_req_free(req);
    nng_http_conn_close(conn);
}

static void ducknng_http_split_uri(const char *uri, char **out_path, const char **out_query) {
    const char *path_end;
    const char *query = NULL;
    char *path = NULL;
    size_t path_len;
    if (out_path) *out_path = NULL;
    if (out_query) *out_query = NULL;
    if (!uri) return;
    path_end = uri;
    while (*path_end && *path_end != '?' && *path_end != '#') path_end++;
    if (*path_end == '?') query = path_end + 1;
    path_len = (size_t)(path_end - uri);
    path = (char *)duckdb_malloc(path_len + 1);
    if (!path) return;
    if (path_len) memcpy(path, uri, path_len);
    path[path_len] = '\0';
    if (out_path) *out_path = path;
    else duckdb_free(path);
    if (out_query) *out_query = query;
}

static void ducknng_http_route_handler(nng_aio *aio) {
    nng_http_req *req;
    nng_http_handler *handler;
    nng_http_conn *conn;
    ducknng_http_server_state *state;
    const char *content_type;
    const char *uri;
    const char *query_string = NULL;
    const char *method;
    void *body = NULL;
    size_t body_len = 0;
    char *request_path = NULL;
    char *headers_json = NULL;
    char *path_params_json = NULL;
    char *caller_identity = NULL;
    nng_sockaddr remote_addr;
    int have_remote_addr = 0;
    nng_http_res *res = NULL;
    int rv = 0;
    int handler_rc = 0;
    int stopping = 0;
    int service_stopping = 0;
    ducknng_http_route route;
    ducknng_http_request_context request_ctx;
    ducknng_http_route_reply route_reply;
    if (!aio) return;
    memset(&route, 0, sizeof(route));
    memset(&request_ctx, 0, sizeof(request_ctx));
    ducknng_http_route_reply_init(&route_reply);
    req = (nng_http_req *)nng_aio_get_input(aio, 0);
    handler = (nng_http_handler *)nng_aio_get_input(aio, 1);
    conn = (nng_http_conn *)nng_aio_get_input(aio, 2);
    state = ducknng_http_handler_server_state(handler);
    if (!req || !state || !state->svc) {
        rv = ducknng_http_alloc_text_response(&res, 500, "ducknng: missing HTTP server state");
        ducknng_http_finish_response(aio, res, rv);
        return;
    }
    if (state->mu_initialized) {
        ducknng_mutex_lock(&state->mu);
        stopping = state->stopping;
        ducknng_mutex_unlock(&state->mu);
    }
    if (state->svc->mu_initialized) {
        ducknng_mutex_lock(&state->svc->mu);
        service_stopping = state->svc->shutting_down;
        ducknng_mutex_unlock(&state->svc->mu);
    } else {
        service_stopping = state->svc->shutting_down;
    }
    if (stopping || service_stopping) {
        rv = ducknng_http_alloc_text_response(&res, 503, "ducknng: HTTP server is stopping");
        ducknng_http_finish_response(aio, res, rv);
        return;
    }
    uri = nng_http_req_get_uri(req);
    method = nng_http_req_get_method(req);
    ducknng_http_split_uri(uri ? uri : "/", &request_path, &query_string);
    if (!request_path) {
        rv = ducknng_http_alloc_text_response(&res, 500, "ducknng: failed to copy HTTP request path");
        ducknng_http_finish_response(aio, res, rv);
        return;
    }
    if (state->path && strcmp(request_path, state->path) == 0) {
        rv = ducknng_http_alloc_text_response(&res, 405, "ducknng: framed RPC mount accepts POST only");
        goto done;
    }
    nng_http_req_get_data(req, &body, &body_len);
    headers_json = ducknng_http_request_headers_json_dup(req, NULL);
    if (!headers_json) {
        rv = ducknng_http_alloc_text_response(&res, 500, "ducknng: failed to read HTTP request headers");
        goto done;
    }
    content_type = nng_http_req_get_header(req, "Content-Type");
    caller_identity = ducknng_http_conn_verified_peer_identity(conn);
    have_remote_addr = ducknng_http_conn_remote_addr(conn, &remote_addr) == 0;
    {
        char *route_err = NULL;
        int route_found = ducknng_service_lookup_http_route(state->svc,
            method ? method : "GET", request_path, &route, &path_params_json, &route_err);
        if (route_found < 0) {
            rv = ducknng_http_alloc_text_response(&res, 500,
                route_err ? route_err : "ducknng: failed to resolve HTTP route");
            if (route_err) duckdb_free(route_err);
            goto done;
        }
        if (route_err) duckdb_free(route_err);
        if (route_found == 0) {
            rv = ducknng_http_alloc_text_response(&res, 404, "ducknng: HTTP route not found");
            goto done;
        }
    }
    if (route.request_max_bytes > 0 && body_len > route.request_max_bytes) {
        rv = ducknng_http_alloc_text_response(&res, 413, "ducknng: HTTP route request body too large");
        goto done;
    }
    {
        ducknng_authorizer_context auth_ctx;
        ducknng_authorizer_decision decision;
        char *admission_err = NULL;
        char *limit_err = NULL;
        char *handler_err = NULL;
        memset(&auth_ctx, 0, sizeof(auth_ctx));
        auth_ctx.svc = state->svc;
        auth_ctx.frame = NULL;
        auth_ctx.phase = "http_route";
        auth_ctx.transport_family = DUCKNNG_TRANSPORT_FAMILY_HTTP;
        auth_ctx.scheme = state->svc && state->svc->tls_enabled ? DUCKNNG_TRANSPORT_SCHEME_HTTPS : DUCKNNG_TRANSPORT_SCHEME_HTTP;
        auth_ctx.caller_identity = caller_identity;
        auth_ctx.remote_addr = have_remote_addr ? &remote_addr : NULL;
        auth_ctx.http_method = method ? method : "GET";
        auth_ctx.http_path = request_path;
        auth_ctx.content_type = content_type;
        auth_ctx.body_bytes = (uint64_t)body_len;
        ducknng_authorizer_decision_init(&decision);
        if (ducknng_service_network_admission_check(state->svc, caller_identity,
                have_remote_addr ? &remote_addr : NULL, &admission_err) != 0) {
            rv = ducknng_http_alloc_text_response(&res, 403,
                admission_err ? admission_err : "ducknng: peer is not admitted");
            if (admission_err) duckdb_free(admission_err);
            ducknng_authorizer_decision_reset(&decision);
            goto done;
        }
        if (ducknng_service_begin_request(state->svc, caller_identity, &limit_err) != 0) {
            rv = ducknng_http_alloc_text_response(&res, 503,
                limit_err ? limit_err : "ducknng: max inflight requests exceeded");
            if (limit_err) duckdb_free(limit_err);
            ducknng_authorizer_decision_reset(&decision);
            goto done;
        }
        if (ducknng_service_authorize_request(state->svc, &auth_ctx, &decision, NULL) != 0) {
            uint16_t status = (decision.http_status >= 100 && decision.http_status <= 599) ?
                (uint16_t)decision.http_status : 403;
            rv = ducknng_http_alloc_text_response(&res, status,
                decision.reason ? decision.reason : "ducknng: request is not authorized");
            ducknng_authorizer_decision_reset(&decision);
            ducknng_service_end_request(state->svc, caller_identity, 0);
            goto done;
        }
        request_ctx.svc = state->svc;
        request_ctx.scheme = auth_ctx.scheme;
        request_ctx.method = auth_ctx.http_method;
        request_ctx.path = request_path;
        request_ctx.query_string = query_string;
        request_ctx.content_type = content_type;
        request_ctx.headers_json = headers_json;
        request_ctx.body = (const uint8_t *)body;
        request_ctx.body_len = body_len;
        request_ctx.path_params_json = path_params_json;
        request_ctx.caller_identity = caller_identity;
        request_ctx.remote_addr = have_remote_addr ? &remote_addr : NULL;
        if (ducknng_http_route_copy(&request_ctx.route, &route) != 0) {
            ducknng_authorizer_decision_reset(&decision);
            ducknng_service_end_request(state->svc, caller_identity, 0);
            rv = ducknng_http_alloc_text_response(&res, 500, "ducknng: failed to copy HTTP route context");
            goto done;
        }
        /* route-local auth check */
        if (route.auth_require_identity && (!caller_identity || !caller_identity[0])) {
            ducknng_authorizer_decision_reset(&decision);
            ducknng_service_end_request(state->svc, caller_identity, 0);
            rv = ducknng_http_alloc_text_response(&res, 401, "ducknng: route requires caller identity");
            goto done;
        }
        if (route.auth_allow_identities_json && route.auth_allow_identities_json[0] &&
            caller_identity && caller_identity[0]) {
            /* exact-match membership; malformed allowlists fail closed */
            int id_allowed = ducknng_json_string_array_contains(
                route.auth_allow_identities_json, caller_identity, NULL, NULL) == 1;
            if (!id_allowed) {
                ducknng_authorizer_decision_reset(&decision);
                ducknng_service_end_request(state->svc, caller_identity, 0);
                rv = ducknng_http_alloc_text_response(&res, 403, "ducknng: caller identity not allowed for this route");
                goto done;
            }
        }
        /* streaming route: hijack connection and stream chunked response */
        if (route.response_mode == DUCKNNG_HTTP_ROUTE_RESPONSE_STREAM) {
            if (ducknng_http_server_stream_begin(state) != 0) {
                ducknng_authorizer_decision_reset(&decision);
                ducknng_service_end_request(state->svc, caller_identity, 0);
                rv = ducknng_http_alloc_text_response(&res, 503, "ducknng: HTTP server is stopping");
                goto done;
            }
            {
                ducknng_execution_subject subject_ctx;
                ducknng_service_execution_subject_begin(state->svc, &subject_ctx,
                    caller_identity, &decision);
                /* serve_stream_route calls nng_http_hijack + nng_aio_finish(aio,0) internally */
                ducknng_http_serve_stream_route(conn, aio, state, req, &request_ctx, caller_identity);
                ducknng_service_execution_subject_end(state->svc);
            }
            ducknng_authorizer_decision_reset(&decision);
            ducknng_service_end_request(state->svc, caller_identity, 0);
            if (request_path) duckdb_free(request_path);
            if (headers_json) duckdb_free(headers_json);
            if (path_params_json) duckdb_free(path_params_json);
            if (caller_identity) duckdb_free(caller_identity);
            ducknng_http_route_reset(&request_ctx.route);
            ducknng_http_route_reset(&route);
            ducknng_http_route_reply_reset(&route_reply);
            ducknng_http_server_stream_end(state);
            return;
        }
        {
            ducknng_execution_subject subject_ctx;
            ducknng_service_execution_subject_begin(state->svc, &subject_ctx,
                caller_identity, &decision);
            handler_rc = ducknng_service_handle_http_route(state->svc, &request_ctx,
                &route_reply, &handler_err);
            ducknng_service_execution_subject_end(state->svc);
        }
        if (handler_rc != 0) {
            rv = ducknng_http_alloc_text_response(&res, 500,
                handler_err ? handler_err : "ducknng: HTTP route handler failed");
            if (handler_err) duckdb_free(handler_err);
            ducknng_authorizer_decision_reset(&decision);
            {
                size_t reply_bytes = route_reply.body ? route_reply.body_len :
                    (route_reply.body_text ? strlen(route_reply.body_text) : 0);
                ducknng_service_end_request(state->svc, caller_identity, reply_bytes);
            }
            goto done;
        }
        ducknng_authorizer_decision_reset(&decision);
        {
            size_t reply_bytes = route_reply.body ? route_reply.body_len :
                (route_reply.body_text ? strlen(route_reply.body_text) : 0);
            ducknng_service_end_request(state->svc, caller_identity, reply_bytes);
        }
    }
    {
        char *reply_err = NULL;
        rv = ducknng_http_route_response_alloc(&res, &route_reply, &reply_err);
        if (rv != 0) {
            rv = ducknng_http_alloc_text_response(&res, 500,
                reply_err ? reply_err : "ducknng: failed to build HTTP route response");
            if (reply_err) duckdb_free(reply_err);
        }
    }
done:
    if (request_path) duckdb_free(request_path);
    if (headers_json) duckdb_free(headers_json);
    if (path_params_json) duckdb_free(path_params_json);
    if (caller_identity) duckdb_free(caller_identity);
    ducknng_http_route_reset(&request_ctx.route);
    ducknng_http_route_reset(&route);
    ducknng_http_route_reply_reset(&route_reply);
    ducknng_http_finish_response(aio, res, rv);
}

static void ducknng_http_rpc_handler(nng_aio *aio) {
    nng_http_req *req;
    nng_http_handler *handler;
    nng_http_conn *conn;
    ducknng_http_server_state *state;
    const char *content_type;
    void *body = NULL;
    size_t body_len = 0;
    nng_msg *reply_msg = NULL;
    nng_http_res *res = NULL;
    char *caller_identity = NULL;
    nng_sockaddr remote_addr;
    int have_remote_addr = 0;
    int rv = 0;
    int stopping = 0;
    int service_stopping = 0;
    if (!aio) return;
    req = (nng_http_req *)nng_aio_get_input(aio, 0);
    handler = (nng_http_handler *)nng_aio_get_input(aio, 1);
    conn = (nng_http_conn *)nng_aio_get_input(aio, 2);
    state = ducknng_http_handler_server_state(handler);
    if (!req || !state || !state->svc) {
        rv = ducknng_http_alloc_text_response(&res, 500, "ducknng: missing HTTP server state");
        ducknng_http_finish_response(aio, res, rv);
        return;
    }
    if (state->mu_initialized) {
        ducknng_mutex_lock(&state->mu);
        stopping = state->stopping;
        ducknng_mutex_unlock(&state->mu);
    }
    if (state->svc->mu_initialized) {
        ducknng_mutex_lock(&state->svc->mu);
        service_stopping = state->svc->shutting_down;
        ducknng_mutex_unlock(&state->svc->mu);
    } else {
        service_stopping = state->svc->shutting_down;
    }
    if (stopping || service_stopping) {
        rv = ducknng_http_alloc_text_response(&res, 503, "ducknng: HTTP server is stopping");
        ducknng_http_finish_response(aio, res, rv);
        return;
    }
    content_type = nng_http_req_get_header(req, "Content-Type");
    if (!ducknng_http_content_type_is_frame(content_type)) {
        rv = ducknng_http_alloc_text_response(&res, 415,
            "ducknng: expected Content-Type application/vnd.ducknng.frame");
        ducknng_http_finish_response(aio, res, rv);
        return;
    }
    nng_http_req_get_data(req, &body, &body_len);
    caller_identity = ducknng_http_conn_verified_peer_identity(conn);
    have_remote_addr = ducknng_http_conn_remote_addr(conn, &remote_addr) == 0;
    {
        uint16_t status = 500;
        char *err_text = NULL;
        reply_msg = ducknng_service_authorize_and_dispatch_frame(state->svc,
            (const uint8_t *)body, body_len, caller_identity,
            have_remote_addr ? &remote_addr : NULL,
            state->svc->tls_enabled ? DUCKNNG_TRANSPORT_SCHEME_HTTPS : DUCKNNG_TRANSPORT_SCHEME_HTTP,
            "POST", state->path, content_type, &status, &err_text);
        if (!reply_msg) {
            rv = ducknng_http_alloc_text_response(&res, status,
                err_text ? err_text : "ducknng: failed to dispatch request");
            if (err_text) duckdb_free(err_text);
            if (caller_identity) duckdb_free(caller_identity);
            ducknng_http_finish_response(aio, res, rv);
            return;
        }
    }
    if (caller_identity) duckdb_free(caller_identity);
    rv = ducknng_http_alloc_frame_response(&res, nng_msg_body(reply_msg), nng_msg_len(reply_msg));
    nng_msg_free(reply_msg);
    ducknng_http_finish_response(aio, res, rv);
}

char *ducknng_http_status_error_message(uint16_t status, const uint8_t *body, size_t body_len) {
    char *text = NULL;
    char *msg = NULL;
    size_t need;
    if (body && body_len > 0 && ducknng_http_bytes_look_text(body, body_len)) {
        text = ducknng_http_dup_bytes(body, body_len);
    }
    if (text && text[0]) {
        need = strlen(text) + 64;
        msg = (char *)duckdb_malloc(need);
        if (msg) snprintf(msg, need, "ducknng: HTTP transport returned status %u: %s",
            (unsigned)status, text);
    } else {
        msg = (char *)duckdb_malloc(64);
        if (msg) snprintf(msg, 64, "ducknng: HTTP transport returned status %u", (unsigned)status);
    }
    if (text) duckdb_free(text);
    return msg ? msg : ducknng_strdup("ducknng: HTTP transport request failed");
}

int ducknng_validate_http_server_url(const char *url, const ducknng_tls_opts *tls_opts, char **errmsg) {
    ducknng_transport_url parsed;
    nng_url *up = NULL;
    int tls_requested = ducknng_http_tls_requested(tls_opts);
    int rc = -1;
    if (errmsg) *errmsg = NULL;
    if (ducknng_validate_http_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (nng_url_parse(&up, url) != 0 || !up) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid HTTP listen URL");
        goto done;
    }
    if (!up->u_path || up->u_path[0] != '/') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP listen URL must include an absolute path such as /_ducknng");
        goto done;
    }
    if (parsed.scheme == DUCKNNG_TRANSPORT_SCHEME_HTTPS) {
        if (!tls_requested) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: https listeners require TLS configuration");
            goto done;
        }
    } else if (tls_requested) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires an https:// listen URL");
        goto done;
    }
    rc = 0;
done:
    if (up) nng_url_free(up);
    return rc;
}

int ducknng_http_frame_transact(const char *url, const uint8_t *frame, size_t frame_len,
    int timeout_ms, const ducknng_tls_opts *tls_opts, uint8_t **out_frame, size_t *out_frame_len,
    char **errmsg) {
    uint16_t status = 0;
    uint8_t *body = NULL;
    size_t body_len = 0;
    if (out_frame) *out_frame = NULL;
    if (out_frame_len) *out_frame_len = 0;
    if (ducknng_http_transact(url, "POST", DUCKNNG_HTTP_FRAME_HEADERS_JSON, frame, frame_len,
            timeout_ms, tls_opts, &status, NULL, &body, &body_len, errmsg) != 0) {
        return -1;
    }
    if (status != 200) {
        if (errmsg && !*errmsg) *errmsg = ducknng_http_status_error_message(status, body, body_len);
        if (body) duckdb_free(body);
        return -1;
    }
    if (!body && body_len > 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: HTTP transport returned an invalid empty body");
        return -1;
    }
    if (out_frame) *out_frame = body;
    else if (body) duckdb_free(body);
    if (out_frame_len) *out_frame_len = body_len;
    return 0;
}

struct ducknng_http_frame_client {
    nng_url *url;
    nng_http_client *client;
    char *url_text;
};

int ducknng_http_frame_client_open(const char *url, const ducknng_tls_opts *tls_opts,
    ducknng_http_frame_client **out_client, char **errmsg) {
    return ducknng_net_backend_get()->frame_client_open(url, tls_opts, out_client, errmsg);
}

#ifdef __EMSCRIPTEN__
int ducknng_http_frame_client_open_browser(const char *url, const ducknng_tls_opts *tls_opts,
    ducknng_http_frame_client **out_client, char **errmsg) {
    ducknng_http_frame_client *fc = NULL;
    ducknng_transport_url parsed;
    if (out_client) *out_client = NULL;
    if (!out_client) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame client output");
        return -1;
    }
    if (ducknng_validate_http_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (ducknng_http_tls_requested(tls_opts)) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: explicit TLS configuration is unsupported in the browser; https uses browser-managed TLS");
        return -1;
    }
    fc = (ducknng_http_frame_client *)duckdb_malloc(sizeof(*fc));
    if (!fc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating HTTP frame client");
        return -1;
    }
    memset(fc, 0, sizeof(*fc));
    fc->url_text = ducknng_strdup(url);
    if (!fc->url_text) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying browser HTTP frame URL");
        ducknng_http_frame_client_close(fc);
        return -1;
    }
    *out_client = fc;
    return 0;
}
#endif

int ducknng_http_frame_client_open_native(const char *url, const ducknng_tls_opts *tls_opts,
    ducknng_http_frame_client **out_client, char **errmsg) {
    ducknng_http_frame_client *fc = NULL;
    ducknng_transport_url parsed;
    nng_tls_config *tls_cfg = NULL;
    int rv;
    if (out_client) *out_client = NULL;
    if (!out_client) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame client output");
        return -1;
    }
    if (ducknng_validate_http_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (ducknng_http_tls_requested(tls_opts) && !parsed.uses_tls) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires an https:// URL");
        return -1;
    }
    fc = (ducknng_http_frame_client *)duckdb_malloc(sizeof(*fc));
    if (!fc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating HTTP frame client");
        return -1;
    }
    memset(fc, 0, sizeof(*fc));
    rv = nng_url_parse(&fc->url, url);
    if (rv != 0) goto fail;
    rv = nng_http_client_alloc(&fc->client, fc->url);
    if (rv != 0) goto fail;
    if (parsed.uses_tls && ducknng_http_tls_requested(tls_opts)) {
        rv = ducknng_http_tls_config_build(&tls_cfg, NNG_TLS_MODE_CLIENT, url, tls_opts);
        if (rv != 0) goto fail;
        if (tls_cfg) {
            rv = nng_http_client_set_tls(fc->client, tls_cfg);
            nng_tls_config_free(tls_cfg);
            tls_cfg = NULL;
            if (rv != 0) goto fail;
        }
    }
    *out_client = fc;
    return 0;
fail:
    if (tls_cfg) nng_tls_config_free(tls_cfg);
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    ducknng_http_frame_client_close(fc);
    return -1;
}

int ducknng_http_frame_client_transact(ducknng_http_frame_client *client,
    const uint8_t *frame, size_t frame_len, int timeout_ms,
    uint8_t **out_frame, size_t *out_frame_len, char **errmsg) {
    return ducknng_net_backend_get()->frame_client_transact(client, frame, frame_len,
        timeout_ms, out_frame, out_frame_len, errmsg);
}

#ifdef __EMSCRIPTEN__
int ducknng_http_frame_client_transact_browser(ducknng_http_frame_client *client,
    const uint8_t *frame, size_t frame_len, int timeout_ms,
    uint8_t **out_frame, size_t *out_frame_len, char **errmsg) {
    if (out_frame) *out_frame = NULL;
    if (out_frame_len) *out_frame_len = 0;
    if (!client || !client->url_text) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame client");
        return -1;
    }
    return ducknng_http_frame_transact(client->url_text, frame, frame_len, timeout_ms,
        NULL, out_frame, out_frame_len, errmsg);
}
#endif

int ducknng_http_frame_client_transact_native(ducknng_http_frame_client *client,
    const uint8_t *frame, size_t frame_len, int timeout_ms,
    uint8_t **out_frame, size_t *out_frame_len, char **errmsg) {
    nng_http_req *req = NULL;
    nng_http_res *res = NULL;
    nng_aio *aio = NULL;
    void *resp_body = NULL;
    size_t resp_body_len = 0;
    uint16_t status = 0;
    int rv;
    if (out_frame) *out_frame = NULL;
    if (out_frame_len) *out_frame_len = 0;
    if (!client || !client->client || !client->url) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame client");
        return -1;
    }
    rv = nng_http_req_alloc(&req, client->url);
    if (rv != 0) goto fail;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) goto fail;
    rv = nng_http_req_set_method(req, "POST");
    if (rv != 0) goto fail;
    if (ducknng_http_apply_headers_json(req, DUCKNNG_HTTP_FRAME_HEADERS_JSON, errmsg) != 0) {
        rv = NNG_EINVAL;
        goto fail;
    }
    if (frame && frame_len > 0) {
        rv = nng_http_req_copy_data(req, frame, frame_len);
        if (rv != 0) goto fail;
    }
    rv = ducknng_aio_alloc(&aio, NULL, NULL, timeout_ms);
    if (rv != 0) goto fail;
    nng_http_client_transact(client->client, req, res, aio);
    ducknng_aio_wait(aio);
    rv = ducknng_aio_result(aio);
    if (rv != 0) goto fail;
    status = nng_http_res_get_status(res);
    nng_http_res_get_data(res, &resp_body, &resp_body_len);
    if (status != 200) {
        if (errmsg && !*errmsg) *errmsg = ducknng_http_status_error_message(status,
            (const uint8_t *)resp_body, resp_body_len);
        rv = NNG_EPROTO;
        goto fail;
    }
    if (out_frame_len) *out_frame_len = resp_body_len;
    if (out_frame) {
        *out_frame = (uint8_t *)duckdb_malloc(resp_body_len);
        if (!*out_frame && resp_body_len > 0) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP frame response");
            rv = NNG_ENOMEM;
            goto fail;
        }
        if (resp_body_len) memcpy(*out_frame, resp_body, resp_body_len);
    }
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    return 0;
fail:
    if (out_frame && *out_frame) {
        duckdb_free(*out_frame);
        *out_frame = NULL;
    }
    if (out_frame_len) *out_frame_len = 0;
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    return -1;
}

int ducknng_http_frame_client_transact_msg(ducknng_http_frame_client *client,
    const uint8_t *frame, size_t frame_len, int timeout_ms,
    nng_msg **out_msg, char **errmsg) {
    return ducknng_net_backend_get()->frame_client_transact_msg(client, frame, frame_len,
        timeout_ms, out_msg, errmsg);
}

#ifdef __EMSCRIPTEN__
int ducknng_http_frame_client_transact_msg_browser(ducknng_http_frame_client *client,
    const uint8_t *frame, size_t frame_len, int timeout_ms,
    nng_msg **out_msg, char **errmsg) {
    nng_msg *msg = NULL;
    uint8_t *out_frame = NULL;
    size_t out_frame_len = 0;
    int rv;
    if (out_msg) *out_msg = NULL;
    if (!out_msg) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame message output");
        return -1;
    }
    if (!client || !client->url_text) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame client");
        return -1;
    }
    if (ducknng_http_frame_client_transact_browser(client, frame, frame_len, timeout_ms,
            &out_frame, &out_frame_len, errmsg) != 0) {
        return -1;
    }
    rv = nng_msg_alloc(&msg, out_frame_len);
    if (rv != 0) {
        if (out_frame) duckdb_free(out_frame);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    if (out_frame_len) memcpy(nng_msg_body(msg), out_frame, out_frame_len);
    if (out_frame) duckdb_free(out_frame);
    *out_msg = msg;
    return 0;
}
#endif

int ducknng_http_frame_client_transact_msg_native(ducknng_http_frame_client *client,
    const uint8_t *frame, size_t frame_len, int timeout_ms,
    nng_msg **out_msg, char **errmsg) {
    nng_http_req *req = NULL;
    nng_http_res *res = NULL;
    nng_aio *aio = NULL;
    nng_msg *msg = NULL;
    void *resp_body = NULL;
    size_t resp_body_len = 0;
    uint16_t status = 0;
    int rv;
    if (out_msg) *out_msg = NULL;
    if (!out_msg) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame message output");
        return -1;
    }
    if (!client || !client->client || !client->url) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP frame client");
        return -1;
    }
    rv = nng_http_req_alloc(&req, client->url);
    if (rv != 0) goto fail;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) goto fail;
    rv = nng_http_req_set_method(req, "POST");
    if (rv != 0) goto fail;
    if (ducknng_http_apply_headers_json(req, DUCKNNG_HTTP_FRAME_HEADERS_JSON, errmsg) != 0) {
        rv = NNG_EINVAL;
        goto fail;
    }
    if (frame && frame_len > 0) {
        rv = nng_http_req_copy_data(req, frame, frame_len);
        if (rv != 0) goto fail;
    }
    rv = ducknng_aio_alloc(&aio, NULL, NULL, timeout_ms);
    if (rv != 0) goto fail;
    nng_http_client_transact(client->client, req, res, aio);
    ducknng_aio_wait(aio);
    rv = ducknng_aio_result(aio);
    if (rv != 0) goto fail;
    status = nng_http_res_get_status(res);
    nng_http_res_get_data(res, &resp_body, &resp_body_len);
    if (status != 200) {
        if (errmsg && !*errmsg) *errmsg = ducknng_http_status_error_message(status,
            (const uint8_t *)resp_body, resp_body_len);
        rv = NNG_EPROTO;
        goto fail;
    }
    rv = nng_msg_alloc(&msg, resp_body_len);
    if (rv != 0) goto fail;
    if (resp_body_len) memcpy(nng_msg_body(msg), resp_body, resp_body_len);
    *out_msg = msg;
    msg = NULL;
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    return 0;
fail:
    if (msg) nng_msg_free(msg);
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    return -1;
}

void ducknng_http_frame_client_close(ducknng_http_frame_client *client) {
    if (!client) return;
    if (client->client) nng_http_client_free(client->client);
    if (client->url) nng_url_free(client->url);
    if (client->url_text) duckdb_free(client->url_text);
    duckdb_free(client);
}

/* Derive the WebSocket frame endpoint URL from the (resolved) HTTP mount URL:
 * swap http->ws / https->wss and append a "/ws" path segment so it never
 * collides with the RPC POST handler's exact path on the shared server. Caller
 * frees. Returns NULL on parse/allocation failure. */
static char *ducknng_http_frame_ws_url(const char *base_url, int tls) {
    nng_url *up = NULL;
    char *out = NULL;
    const char *scheme = tls ? "wss" : "ws";
    if (!base_url || nng_url_parse(&up, base_url) != 0 || !up) {
        if (up) nng_url_free(up);
        return NULL;
    }
    if (up->u_hostname && up->u_port && up->u_port[0]) {
        const char *path = (up->u_path && up->u_path[0]) ? up->u_path : "/";
        size_t plen = strlen(path);
        const char *sep = (plen > 0 && path[plen - 1] == '/') ? "ws" : "/ws";
        size_t need = strlen(scheme) + strlen(up->u_hostname) + strlen(up->u_port) + plen + 8;
        out = (char *)duckdb_malloc(need);
        if (out) snprintf(out, need, "%s://%s:%s%s%s", scheme, up->u_hostname, up->u_port, path, sep);
    }
    nng_url_free(up);
    return out;
}

int ducknng_http_server_start(struct ducknng_service *svc, ducknng_http_server_state **out_state,
    char **out_resolved_url, char **errmsg) {
    ducknng_http_server_state *state = NULL;
    ducknng_http_handler_data *rpc_handler_data = NULL;
    ducknng_http_handler_data *route_handler_data = NULL;
    nng_url *up = NULL;
    nng_tls_config *tls_cfg = NULL;
    int rv;
    if (out_state) *out_state = NULL;
    if (out_resolved_url) *out_resolved_url = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc || !svc->listen_url) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP service state");
        return -1;
    }
    if (ducknng_validate_http_server_url(svc->listen_url, &svc->tls_opts, errmsg) != 0) return -1;
    rv = nng_url_parse(&up, svc->listen_url);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    state = (ducknng_http_server_state *)duckdb_malloc(sizeof(*state));
    if (!state) {
        nng_url_free(up);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating HTTP server state");
        return -1;
    }
    memset(state, 0, sizeof(*state));
    state->svc = svc;
    state->path = ducknng_strdup(up->u_path ? up->u_path : "/");
    if (!state->path) {
        nng_url_free(up);
        duckdb_free(state);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP path");
        return -1;
    }
    if (ducknng_mutex_init(&state->mu) != 0) {
        nng_url_free(up);
        if (state->path) duckdb_free(state->path);
        duckdb_free(state);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize HTTP server mutex");
        return -1;
    }
    state->mu_initialized = 1;
    if (ducknng_cond_init(&state->cv) != 0) {
        ducknng_mutex_destroy(&state->mu);
        nng_url_free(up);
        if (state->path) duckdb_free(state->path);
        duckdb_free(state);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize HTTP server condition variable");
        return -1;
    }
    state->cv_initialized = 1;
    rv = nng_http_server_hold(&state->server, up);
    if (rv != 0) goto fail;
    rv = nng_http_handler_alloc(&state->rpc_handler, up->u_path, ducknng_http_rpc_handler);
    if (rv != 0) goto fail;
    rv = nng_http_handler_set_method(state->rpc_handler, "POST");
    if (rv != 0) goto fail;
    rv = nng_http_handler_collect_body(state->rpc_handler, true, svc->recv_max_bytes);
    if (rv != 0) goto fail;
    rpc_handler_data = (ducknng_http_handler_data *)duckdb_malloc(sizeof(*rpc_handler_data));
    if (!rpc_handler_data) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating HTTP RPC handler state");
        rv = NNG_ENOMEM;
        goto fail;
    }
    rpc_handler_data->state = state;
    rpc_handler_data->is_route_handler = 0;
    rv = nng_http_handler_set_data(state->rpc_handler, rpc_handler_data,
        ducknng_http_server_state_handler_dtor);
    if (rv != 0) goto fail;
    rpc_handler_data = NULL;
    state->rpc_handler_data_installed = 1;
    rv = nng_http_server_add_handler(state->server, state->rpc_handler);
    if (rv != 0) goto fail;
    rv = nng_http_handler_alloc(&state->route_handler, "/", ducknng_http_route_handler);
    if (rv != 0) goto fail;
    rv = nng_http_handler_set_method(state->route_handler, NULL);
    if (rv != 0) goto fail;
    rv = nng_http_handler_set_tree(state->route_handler);
    if (rv != 0) goto fail;
    rv = nng_http_handler_collect_body(state->route_handler, true, svc->recv_max_bytes);
    if (rv != 0) goto fail;
    route_handler_data = (ducknng_http_handler_data *)duckdb_malloc(sizeof(*route_handler_data));
    if (!route_handler_data) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating HTTP route handler state");
        rv = NNG_ENOMEM;
        goto fail;
    }
    route_handler_data->state = state;
    route_handler_data->is_route_handler = 1;
    rv = nng_http_handler_set_data(state->route_handler, route_handler_data,
        ducknng_http_server_state_handler_dtor);
    if (rv != 0) goto fail;
    route_handler_data = NULL;
    state->route_handler_data_installed = 1;
    rv = nng_http_server_add_handler(state->server, state->route_handler);
    if (rv != 0) goto fail;
    if (ducknng_http_tls_requested(&svc->tls_opts)) {
        rv = ducknng_http_tls_config_build(&tls_cfg, NNG_TLS_MODE_SERVER, NULL, &svc->tls_opts);
        if (rv != 0) goto fail;
        rv = nng_http_server_set_tls(state->server, tls_cfg);
        nng_tls_config_free(tls_cfg);
        tls_cfg = NULL;
        if (rv != 0) goto fail;
    }
    rv = nng_http_server_start(state->server);
    if (rv != 0) goto fail;
    {
        /* Bring up the browser-facing ducknng-frame-over-WebSocket endpoint on
         * the same (now-bound) server, at a "/ws" sibling of the RPC path. */
        char *resolved = ducknng_http_server_resolve_url(state->server, svc->listen_url);
        const char *base = resolved ? resolved : svc->listen_url;
        int tls = ducknng_http_tls_requested(&svc->tls_opts);
        char *ws_url = ducknng_http_frame_ws_url(base, tls);
        char *ws_err = NULL;
        if (!ws_url) {
            if (resolved) duckdb_free(resolved);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building WebSocket endpoint URL");
            rv = NNG_ENOMEM;
            goto fail;
        }
        if (ducknng_ws_frame_endpoint_start(svc, ws_url,
                tls ? DUCKNNG_TRANSPORT_SCHEME_WSS : DUCKNNG_TRANSPORT_SCHEME_WS,
                svc->recv_max_bytes, &state->ws_endpoint, &state->ws_url, &ws_err) != 0) {
            duckdb_free(ws_url);
            if (resolved) duckdb_free(resolved);
            if (errmsg) *errmsg = ws_err ? ws_err : ducknng_strdup("ducknng: failed to start WebSocket endpoint");
            else if (ws_err) duckdb_free(ws_err);
            rv = NNG_EINTERNAL;
            goto fail;
        }
        duckdb_free(ws_url);
        if (out_resolved_url) *out_resolved_url = resolved;
        else if (resolved) duckdb_free(resolved);
    }
    if (out_state) *out_state = state;
    nng_url_free(up);
    return 0;
fail:
    if (rpc_handler_data) duckdb_free(rpc_handler_data);
    if (route_handler_data) duckdb_free(route_handler_data);
    if (tls_cfg) nng_tls_config_free(tls_cfg);
    if (state) {
        if (state->server && state->rpc_handler) (void)nng_http_server_del_handler(state->server, state->rpc_handler);
        if (state->server && state->route_handler) (void)nng_http_server_del_handler(state->server, state->route_handler);
        if (state->rpc_handler) {
            nng_http_handler_free(state->rpc_handler);
            state->rpc_handler = NULL;
        }
        if (state->route_handler) {
            nng_http_handler_free(state->route_handler);
            state->route_handler = NULL;
        }
        if (state->mu_initialized && state->rpc_handler_data_installed) {
            ducknng_mutex_lock(&state->mu);
            while (!state->rpc_handler_finalized && state->cv_initialized) {
                ducknng_cond_wait(&state->cv, &state->mu);
            }
            ducknng_mutex_unlock(&state->mu);
        }
        if (state->mu_initialized && state->route_handler_data_installed) {
            ducknng_mutex_lock(&state->mu);
            while (!state->route_handler_finalized && state->cv_initialized) {
                ducknng_cond_wait(&state->cv, &state->mu);
            }
            ducknng_mutex_unlock(&state->mu);
        }
        if (state->server) {
            nng_http_server_stop(state->server);
            nng_http_server_release(state->server);
        }
        if (state->ws_endpoint) ducknng_ws_frame_endpoint_stop(state->ws_endpoint);
        if (state->ws_url) duckdb_free(state->ws_url);
        if (state->cv_initialized) ducknng_cond_destroy(&state->cv);
        if (state->mu_initialized) ducknng_mutex_destroy(&state->mu);
        if (state->path) duckdb_free(state->path);
        duckdb_free(state);
    }
    if (up) nng_url_free(up);
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    return -1;
}

void ducknng_http_server_stop(ducknng_http_server_state *state) {
    if (!state) return;
    if (state->mu_initialized) {
        ducknng_mutex_lock(&state->mu);
        state->stopping = 1;
        ducknng_mutex_unlock(&state->mu);
    }
    /* Tear down the WebSocket endpoint (its own listener + connection threads)
     * before the shared HTTP server is stopped and released. */
    if (state->ws_endpoint) {
        ducknng_ws_frame_endpoint_stop(state->ws_endpoint);
        state->ws_endpoint = NULL;
    }
    if (state->ws_url) {
        duckdb_free(state->ws_url);
        state->ws_url = NULL;
    }
    if (state->server && state->rpc_handler) (void)nng_http_server_del_handler(state->server, state->rpc_handler);
    if (state->server && state->route_handler) (void)nng_http_server_del_handler(state->server, state->route_handler);
    if (state->server) nng_http_server_stop(state->server);
    if (state->rpc_handler) {
        nng_http_handler_free(state->rpc_handler);
        state->rpc_handler = NULL;
    } else if (state->mu_initialized) {
        ducknng_mutex_lock(&state->mu);
        state->rpc_handler_finalized = 1;
        ducknng_mutex_unlock(&state->mu);
    }
    if (state->route_handler) {
        nng_http_handler_free(state->route_handler);
        state->route_handler = NULL;
    } else if (state->mu_initialized) {
        ducknng_mutex_lock(&state->mu);
        state->route_handler_finalized = 1;
        ducknng_mutex_unlock(&state->mu);
    }
    if (state->mu_initialized) {
        ducknng_mutex_lock(&state->mu);
        while ((!state->rpc_handler_finalized || !state->route_handler_finalized ||
                state->active_streams > 0) && state->cv_initialized) {
            ducknng_cond_wait(&state->cv, &state->mu);
        }
        ducknng_mutex_unlock(&state->mu);
    }
    if (state->server) nng_http_server_release(state->server);
    if (state->cv_initialized) ducknng_cond_destroy(&state->cv);
    if (state->mu_initialized) ducknng_mutex_destroy(&state->mu);
    if (state->path) duckdb_free(state->path);
    duckdb_free(state);
}
