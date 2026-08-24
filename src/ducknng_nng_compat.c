#include "ducknng_nng_compat.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <nng/transport/tls/tls.h>

DUCKDB_EXTENSION_EXTERN

void ducknng_tls_opts_init(ducknng_tls_opts *opts) {
    if (!opts) return;
    memset(opts, 0, sizeof(*opts));
}

static void ducknng_tls_opts_clear_peer_allowlist(ducknng_tls_opts *opts) {
    size_t i;
    if (!opts) return;
    if (opts->peer_allowlist) {
        for (i = 0; i < opts->peer_allowlist_count; i++) {
            if (opts->peer_allowlist[i]) duckdb_free(opts->peer_allowlist[i]);
        }
        duckdb_free(opts->peer_allowlist);
    }
    if (opts->peer_allowlist_json) duckdb_free(opts->peer_allowlist_json);
    opts->peer_allowlist = NULL;
    opts->peer_allowlist_count = 0;
    opts->peer_allowlist_json = NULL;
    opts->peer_allowlist_active = 0;
}

static void ducknng_json_skip_ws(const char **p) {
    while (p && *p && (**p == ' ' || **p == '\t' || **p == '\r' || **p == '\n')) (*p)++;
}

static int ducknng_json_hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static char *ducknng_json_parse_string(const char **p, char **errmsg) {
    const char *s;
    char *out;
    size_t len = 0;
    size_t cap = 32;
    if (!p || !*p || **p != '"') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: peer allowlist must be a JSON array of strings");
        return NULL;
    }
    s = ++(*p);
    out = (char *)duckdb_malloc(cap);
    if (!out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing peer allowlist");
        return NULL;
    }
    while (*s) {
        unsigned char ch = (unsigned char)*s++;
        if (ch == '"') {
            out[len] = '\0';
            *p = s;
            return out;
        }
        if (ch == '\\') {
            char esc = *s++;
            if (!esc) break;
            switch (esc) {
                case '"': ch = '"'; break;
                case '\\': ch = '\\'; break;
                case '/': ch = '/'; break;
                case 'b': ch = '\b'; break;
                case 'f': ch = '\f'; break;
                case 'n': ch = '\n'; break;
                case 'r': ch = '\r'; break;
                case 't': ch = '\t'; break;
                case 'u': {
                    int h0, h1, h2, h3;
                    if (!s[0] || !s[1] || !s[2] || !s[3]) goto invalid_escape;
                    h0 = ducknng_json_hex_value(s[0]);
                    h1 = ducknng_json_hex_value(s[1]);
                    h2 = ducknng_json_hex_value(s[2]);
                    h3 = ducknng_json_hex_value(s[3]);
                    if (h0 != 0 || h1 != 0 || h2 < 0 || h3 < 0) goto invalid_escape;
                    ch = (unsigned char)((h2 << 4) | h3);
                    if (ch == 0) goto invalid_escape;
                    s += 4;
                    break;
                }
                default:
invalid_escape:
                    duckdb_free(out);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported JSON escape in peer allowlist");
                    return NULL;
            }
        } else if (ch < 0x20) {
            duckdb_free(out);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid control character in peer allowlist");
            return NULL;
        }
        if (len + 2 > cap) {
            char *next;
            cap *= 2;
            next = (char *)duckdb_malloc(cap);
            if (!next) {
                duckdb_free(out);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing peer allowlist");
                return NULL;
            }
            memcpy(next, out, len);
            duckdb_free(out);
            out = next;
        }
        out[len++] = (char)ch;
    }
    duckdb_free(out);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: unterminated JSON string in peer allowlist");
    return NULL;
}

static void ducknng_peer_allowlist_free(char **items, size_t count) {
    size_t i;
    if (!items) return;
    for (i = 0; i < count; i++) {
        if (items[i]) duckdb_free(items[i]);
    }
    duckdb_free(items);
}

static int ducknng_peer_allowlist_parse(const char *json, char ***out_items,
    size_t *out_count, char **out_json, char **errmsg) {
    const char *p = json;
    char **items = NULL;
    size_t count = 0;
    size_t cap = 0;
    if (out_items) *out_items = NULL;
    if (out_count) *out_count = 0;
    if (out_json) *out_json = NULL;
    if (errmsg) *errmsg = NULL;
    ducknng_json_skip_ws(&p);
    if (!p || *p != '[') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: peer allowlist must be a JSON array of strings");
        return -1;
    }
    p++;
    ducknng_json_skip_ws(&p);
    if (*p == ']') {
        p++;
        ducknng_json_skip_ws(&p);
        if (*p) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after peer allowlist JSON");
            return -1;
        }
        if (out_json) {
            *out_json = ducknng_strdup(json);
            if (!*out_json) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying peer allowlist");
                return -1;
            }
        }
        return 0;
    }
    for (;;) {
        char *item;
        ducknng_json_skip_ws(&p);
        item = ducknng_json_parse_string(&p, errmsg);
        if (!item) goto fail;
        if (!item[0]) {
            duckdb_free(item);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: peer allowlist identities must be non-empty strings");
            goto fail;
        }
        if (count == cap) {
            char **next;
            size_t new_cap = cap ? cap * 2 : 4;
            next = (char **)duckdb_malloc(sizeof(*next) * new_cap);
            if (!next) {
                duckdb_free(item);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing peer allowlist");
                goto fail;
            }
            if (items && count) memcpy(next, items, sizeof(*items) * count);
            if (items) duckdb_free(items);
            items = next;
            cap = new_cap;
        }
        items[count++] = item;
        ducknng_json_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            break;
        }
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or ']' in peer allowlist JSON");
        goto fail;
    }
    ducknng_json_skip_ws(&p);
    if (*p) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after peer allowlist JSON");
        goto fail;
    }
    if (out_json) {
        *out_json = ducknng_strdup(json);
        if (!*out_json) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying peer allowlist");
            goto fail;
        }
    }
    if (out_items) *out_items = items;
    else ducknng_peer_allowlist_free(items, count);
    if (out_count) *out_count = count;
    return 0;
fail:
    ducknng_peer_allowlist_free(items, count);
    return -1;
}

int ducknng_tls_opts_set_peer_allowlist(ducknng_tls_opts *opts, const char *identities_json, char **errmsg) {
    char **items = NULL;
    size_t count = 0;
    char *json = NULL;
    if (errmsg) *errmsg = NULL;
    if (!opts) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing TLS config for peer allowlist");
        return -1;
    }
    if (!identities_json || !identities_json[0]) {
        ducknng_tls_opts_clear_peer_allowlist(opts);
        return 0;
    }
    if (ducknng_peer_allowlist_parse(identities_json, &items, &count, &json, errmsg) != 0) return -1;
    ducknng_tls_opts_clear_peer_allowlist(opts);
    opts->peer_allowlist_active = 1;
    opts->peer_allowlist = items;
    opts->peer_allowlist_count = count;
    opts->peer_allowlist_json = json;
    return 0;
}

int ducknng_tls_opts_peer_allowed(const ducknng_tls_opts *opts, const char *identity) {
    size_t i;
    if (!opts || !opts->peer_allowlist_active) return 1;
    if (!identity || !identity[0]) return 0;
    for (i = 0; i < opts->peer_allowlist_count; i++) {
        if (opts->peer_allowlist[i] && strcmp(opts->peer_allowlist[i], identity) == 0) return 1;
    }
    return 0;
}

int ducknng_tls_opts_copy(ducknng_tls_opts *dst, const ducknng_tls_opts *src) {
    if (!dst) return -1;
    ducknng_tls_opts_init(dst);
    if (!src) return 0;
    dst->enabled = src->enabled;
    dst->auth_mode = src->auth_mode;
    dst->cert_key_file = ducknng_strdup(src->cert_key_file);
    dst->ca_file = ducknng_strdup(src->ca_file);
    dst->cert_pem = ducknng_strdup(src->cert_pem);
    dst->key_pem = ducknng_strdup(src->key_pem);
    dst->ca_pem = ducknng_strdup(src->ca_pem);
    dst->password = ducknng_strdup(src->password);
    if ((src->cert_key_file && !dst->cert_key_file) ||
        (src->ca_file && !dst->ca_file) ||
        (src->cert_pem && !dst->cert_pem) ||
        (src->key_pem && !dst->key_pem) ||
        (src->ca_pem && !dst->ca_pem) ||
        (src->password && !dst->password)) {
        ducknng_tls_opts_reset(dst);
        return -1;
    }
    if (src->peer_allowlist_active) {
        if (ducknng_tls_opts_set_peer_allowlist(dst, src->peer_allowlist_json ? src->peer_allowlist_json : "[]", NULL) != 0) {
            ducknng_tls_opts_reset(dst);
            return -1;
        }
    }
    return 0;
}

void ducknng_tls_opts_reset(ducknng_tls_opts *opts) {
    if (!opts) return;
    if (opts->cert_key_file) duckdb_free(opts->cert_key_file);
    if (opts->ca_file) duckdb_free(opts->ca_file);
    if (opts->cert_pem) duckdb_free(opts->cert_pem);
    if (opts->key_pem) duckdb_free(opts->key_pem);
    if (opts->ca_pem) duckdb_free(opts->ca_pem);
    if (opts->password) duckdb_free(opts->password);
    ducknng_tls_opts_clear_peer_allowlist(opts);
    memset(opts, 0, sizeof(*opts));
}

static char *ducknng_url_with_port(const nng_url *up, int port) {
    char port_buf[32];
    size_t need;
    char *out;
    if (!up || !up->u_scheme || !up->u_hostname) return NULL;
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    need = strlen(up->u_scheme) + strlen(up->u_hostname) + strlen(port_buf) + 8;
    if (up->u_path) need += strlen(up->u_path);
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    snprintf(out, need, "%s://%s:%s%s", up->u_scheme, up->u_hostname, port_buf, up->u_path ? up->u_path : "");
    return out;
}

static int ducknng_tls_requested(const ducknng_tls_opts *opts) {
    return opts && (opts->enabled ||
        (opts->cert_key_file && opts->cert_key_file[0]) ||
        (opts->ca_file && opts->ca_file[0]) ||
        (opts->cert_pem && opts->cert_pem[0]) ||
        (opts->key_pem && opts->key_pem[0]) ||
        (opts->ca_pem && opts->ca_pem[0]) ||
        (opts->password && opts->password[0]) ||
        opts->auth_mode != 0);
}

static int ducknng_tls_auth_mode_map(int auth_mode, nng_tls_auth_mode *out) {
    if (!out) return NNG_EINVAL;
    switch (auth_mode) {
    case 0: *out = NNG_TLS_AUTH_MODE_NONE; return 0;
    case 1: *out = NNG_TLS_AUTH_MODE_OPTIONAL; return 0;
    case 2: *out = NNG_TLS_AUTH_MODE_REQUIRED; return 0;
    default: return NNG_EINVAL;
    }
}

static char *ducknng_tls_identity_from_value(const char *kind, const char *value) {
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

static char *ducknng_tls_identity_from_alt_names(char **alt_names) {
    size_t i;
    if (!alt_names) return NULL;
    for (i = 0; alt_names[i]; i++) {
        if (alt_names[i][0]) return ducknng_tls_identity_from_value("san", alt_names[i]);
    }
    return NULL;
}

static void ducknng_tls_alt_names_free(char **alt_names) {
    size_t i;
    if (!alt_names) return;
    for (i = 0; alt_names[i]; i++) nng_strfree(alt_names[i]);
    free(alt_names);
}

static int ducknng_pipe_tls_verified(nng_pipe pipe, bool *out_verified) {
    bool verified = false;
    int rv;
    if (out_verified) *out_verified = false;
    if (pipe.id <= 0) return NNG_EINVAL;
    rv = nng_pipe_get_bool(pipe, NNG_OPT_TLS_VERIFIED, &verified);
    if (rv != 0) return rv;
    if (out_verified) *out_verified = verified;
    return 0;
}

char *ducknng_pipe_verified_peer_identity(nng_pipe pipe) {
    bool verified = false;
    char **alt_names = NULL;
    char *cn = NULL;
    char *identity = NULL;
    if (ducknng_pipe_tls_verified(pipe, &verified) != 0 || !verified) return NULL;
    if (nng_pipe_get_ptr(pipe, NNG_OPT_TLS_PEER_ALT_NAMES, (void **)&alt_names) == 0 && alt_names) {
        identity = ducknng_tls_identity_from_alt_names(alt_names);
    }
    ducknng_tls_alt_names_free(alt_names);
    if (!identity && nng_pipe_get_string(pipe, NNG_OPT_TLS_PEER_CN, &cn) == 0 && cn && cn[0]) {
        identity = ducknng_tls_identity_from_value("cn", cn);
    }
    if (cn) nng_strfree(cn);
    return identity;
}

char *ducknng_msg_verified_peer_identity(nng_msg *msg) {
    if (!msg) return NULL;
    return ducknng_pipe_verified_peer_identity(nng_msg_get_pipe(msg));
}

static int ducknng_tls_config_build(nng_tls_config **out, nng_tls_mode mode, const char *url,
    const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    nng_tls_auth_mode auth_mode;
    int rv;
    nng_url *up = NULL;
    if (!out) return NNG_EINVAL;
    *out = NULL;
    if (!opts || !ducknng_tls_requested(opts)) return 0;
    rv = nng_tls_config_alloc(&cfg, mode);
    if (rv != 0) return rv;
    rv = ducknng_tls_auth_mode_map(opts->auth_mode, &auth_mode);
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

int ducknng_rep_socket_open(nng_socket *out) { return nng_rep0_open(out); }
int ducknng_req_socket_open(nng_socket *out) { return nng_req0_open(out); }
int ducknng_validate_nng_url(const char *url, char **errmsg) {
    ducknng_transport_url parsed;
    char *parse_err = NULL;
    if (errmsg) *errmsg = NULL;
    if (ducknng_transport_url_parse(url, &parsed, &parse_err) != 0) {
        if (errmsg) *errmsg = parse_err;
        else if (parse_err) duckdb_free(parse_err);
        return -1;
    }
    if (!ducknng_transport_url_is_nng(&parsed)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: http:// and https:// are supported by the HTTP carrier, not the NNG socket/service layer");
        return -1;
    }
    return 0;
}
int ducknng_socket_validate_client_url(const char *url, const ducknng_tls_opts *opts, char **errmsg) {
    ducknng_transport_url parsed;
    int tls_requested = ducknng_tls_requested(opts);
    if (errmsg) *errmsg = NULL;
    if (ducknng_validate_nng_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (tls_requested && parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_TLS_TCP && parsed.scheme != DUCKNNG_TRANSPORT_SCHEME_WSS) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires a tls+tcp:// or wss:// URL");
        return -1;
    }
    return 0;
}

int ducknng_socket_open_protocol(const char *protocol, nng_socket *out, char **errmsg) {
    int rv = NNG_EINVAL;
    if (errmsg) *errmsg = NULL;
    if (!protocol || !protocol[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: socket protocol is required");
        return -1;
    }
    if (strcmp(protocol, "bus") == 0) rv = nng_bus0_open(out);
    else if (strcmp(protocol, "pair") == 0 || strcmp(protocol, "pair0") == 0) rv = nng_pair0_open(out);
    else if (strcmp(protocol, "poly") == 0) rv = nng_pair1_open_poly(out);
    else if (strcmp(protocol, "pair1") == 0) rv = nng_pair1_open(out);
    else if (strcmp(protocol, "push") == 0) rv = nng_push0_open(out);
    else if (strcmp(protocol, "pull") == 0) rv = nng_pull0_open(out);
    else if (strcmp(protocol, "pub") == 0) rv = nng_pub0_open(out);
    else if (strcmp(protocol, "sub") == 0) rv = nng_sub0_open(out);
    else if (strcmp(protocol, "req") == 0) rv = nng_req0_open(out);
    else if (strcmp(protocol, "rep") == 0) rv = nng_rep0_open(out);
    else if (strcmp(protocol, "surveyor") == 0) rv = nng_surveyor0_open(out);
    else if (strcmp(protocol, "respondent") == 0) rv = nng_respondent0_open(out);
    else {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: protocol must be one of bus, pair, pair0, pair1, poly, push, pull, pub, sub, req, rep, surveyor, respondent");
        return -1;
    }
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    return 0;
}
int ducknng_socket_set_timeout_ms(nng_socket sock, int send_timeout_ms, int recv_timeout_ms) {
    int rv;
    if (send_timeout_ms > 0) {
        rv = nng_socket_set_ms(sock, NNG_OPT_SENDTIMEO, send_timeout_ms);
        if (rv != 0) return rv;
    }
    if (recv_timeout_ms > 0) {
        rv = nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, recv_timeout_ms);
        if (rv != 0) return rv;
    }
    return 0;
}
int ducknng_socket_dial(nng_socket sock, const char *url) { return nng_dial(sock, url, NULL, 0); }
int ducknng_socket_dial_nonblocking(nng_socket sock, const char *url) {
    return nng_dial(sock, url, NULL, NNG_FLAG_NONBLOCK);
}
int ducknng_socket_send(nng_socket sock, nng_msg *msg) { return nng_sendmsg(sock, msg, 0); }
int ducknng_socket_recv(nng_socket sock, nng_msg **msg) { return nng_recvmsg(sock, msg, 0); }
int ducknng_socket_subscribe(nng_socket sock, const void *topic, size_t len) {
    return nng_socket_set(sock, NNG_OPT_SUB_SUBSCRIBE, topic, len);
}
int ducknng_socket_unsubscribe(nng_socket sock, const void *topic, size_t len) {
    return nng_socket_set(sock, NNG_OPT_SUB_UNSUBSCRIBE, topic, len);
}
int ducknng_ctx_send(nng_ctx ctx, nng_msg *msg) { return nng_ctx_sendmsg(ctx, msg, 0); }
int ducknng_ctx_recv(nng_ctx ctx, nng_msg **msg) { return nng_ctx_recvmsg(ctx, msg, 0); }
int ducknng_req_dial(nng_socket sock, const char *url, int timeout_ms) {
    int rv = ducknng_socket_set_timeout_ms(sock, timeout_ms, timeout_ms);
    if (rv != 0) return rv;
    return ducknng_socket_dial(sock, url);
}
int ducknng_socket_apply_tls(nng_socket sock, const char *url, const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    int rv = ducknng_tls_config_build(&cfg, NNG_TLS_MODE_CLIENT, url, opts);
    if (rv != 0) return rv;
    if (!cfg) return 0;
    rv = nng_socket_set_ptr(sock, NNG_OPT_TLS_CONFIG, cfg);
    nng_tls_config_free(cfg);
    return rv;
}
int ducknng_req_transact(nng_socket sock, nng_msg *req, nng_msg **resp) {
    int rv;
    rv = ducknng_socket_send(sock, req);
    if (rv != 0) {
        nng_msg_free(req);
        return rv;
    }
    return ducknng_socket_recv(sock, resp);
}
int ducknng_listener_create(nng_listener *out, nng_socket sock, const char *url) { return nng_listener_create(out, sock, url); }
int ducknng_listener_validate_startup_url(const char *url, const ducknng_tls_opts *opts, char **errmsg) {
    ducknng_transport_url parsed;
    nng_url *up = NULL;
    int tls_requested = ducknng_tls_requested(opts);
    int rc = 0;
    if (errmsg) *errmsg = NULL;
    if (!url || !url[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: listen URL is required");
        return -1;
    }
    if (ducknng_validate_nng_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (nng_url_parse(&up, url) != 0 || !up || !up->u_scheme || !up->u_scheme[0]) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: invalid listen URL");
        rc = -1;
        goto done;
    }
    if (parsed.scheme == DUCKNNG_TRANSPORT_SCHEME_TLS_TCP || parsed.scheme == DUCKNNG_TRANSPORT_SCHEME_WSS) {
        if (!tls_requested) {
            if (errmsg) {
                *errmsg = ducknng_strdup(parsed.scheme == DUCKNNG_TRANSPORT_SCHEME_WSS
                    ? "ducknng: wss listeners require TLS configuration"
                    : "ducknng: tls+tcp listeners require TLS configuration");
            }
            rc = -1;
            goto done;
        }
    } else if (tls_requested) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires a tls+tcp:// or wss:// listen URL");
        rc = -1;
        goto done;
    }

done:
    if (up) nng_url_free(up);
    return rc;
}
int ducknng_listener_set_recvmaxsz(nng_listener lst, size_t bytes) { return nng_listener_set_size(lst, NNG_OPT_RECVMAXSZ, bytes); }
int ducknng_listener_apply_tls(nng_listener lst, const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    int rv = ducknng_tls_config_build(&cfg, NNG_TLS_MODE_SERVER, NULL, opts);
    if (rv != 0) return rv;
    if (!cfg) return 0;
    rv = nng_listener_set_ptr(lst, NNG_OPT_TLS_CONFIG, cfg);
    nng_tls_config_free(cfg);
    return rv;
}
int ducknng_listener_start(nng_listener lst) { return nng_listener_start(lst, 0); }
int ducknng_listener_close(nng_listener lst) { return nng_listener_close(lst); }
char *ducknng_listener_resolve_url(nng_listener lst, const char *url) {
    nng_url *up = NULL;
    char *resolved = NULL;
    int port = 0;
    if (!url) return NULL;
    if (nng_url_parse(&up, url) != 0 || !up) goto done;
    if (!up->u_port || strcmp(up->u_port, "0") != 0) goto done;
    if (strcmp(up->u_scheme, "tcp") != 0 && strcmp(up->u_scheme, "tls+tcp") != 0 &&
        strcmp(up->u_scheme, "ws") != 0 && strcmp(up->u_scheme, "wss") != 0) goto done;
    if (nng_listener_get_int(lst, NNG_OPT_TCP_BOUND_PORT, &port) != 0 || port <= 0) goto done;
    resolved = ducknng_url_with_port(up, port);
done:
    if (up) nng_url_free(up);
    return resolved;
}
int ducknng_socket_close(nng_socket sock) { return nng_close(sock); }
int ducknng_ctx_open(nng_ctx *out, nng_socket sock) { return nng_ctx_open(out, sock); }
int ducknng_ctx_close(nng_ctx ctx) { return nng_ctx_close(ctx); }
void ducknng_ctx_recv_aio(nng_ctx ctx, nng_aio *aio) { nng_ctx_recv(ctx, aio); }
void ducknng_ctx_send_aio(nng_ctx ctx, nng_aio *aio) { nng_ctx_send(ctx, aio); }
void ducknng_socket_recv_aio(nng_socket sock, nng_aio *aio) { nng_recv_aio(sock, aio); }
void ducknng_socket_send_aio(nng_socket sock, nng_aio *aio) { nng_send_aio(sock, aio); }
int ducknng_aio_alloc(nng_aio **out, void (*cb)(void *), void *arg, int timeout_ms) {
    int rv = nng_aio_alloc(out, cb, arg);
    if (rv == 0 && timeout_ms > 0) {
        nng_aio_set_timeout(*out, timeout_ms);
    }
    return rv;
}
void ducknng_aio_free(nng_aio *aio) { nng_aio_free(aio); }
int ducknng_aio_result(nng_aio *aio) { return nng_aio_result(aio); }
void ducknng_aio_cancel(nng_aio *aio) { nng_aio_cancel(aio); }
void ducknng_aio_wait(nng_aio *aio) { nng_aio_wait(aio); }
void ducknng_aio_set_msg(nng_aio *aio, nng_msg *msg) { nng_aio_set_msg(aio, msg); }
nng_msg *ducknng_aio_get_msg(nng_aio *aio) { return nng_aio_get_msg(aio); }
const char *ducknng_nng_strerror(int err) { return nng_strerror(err); }
