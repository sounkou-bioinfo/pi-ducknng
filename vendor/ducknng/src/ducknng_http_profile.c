#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <nng/supplemental/http/http.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static void
ducknng_http_profile_reset(ducknng_http_profile *p)
{
    if (!p) return;
    if (p->profile_id) duckdb_free(p->profile_id);
    if (p->scheme) duckdb_free(p->scheme);
    if (p->host) duckdb_free(p->host);
    if (p->path_prefix) duckdb_free(p->path_prefix);
    if (p->method) duckdb_free(p->method);
    if (p->auth_header_name) duckdb_free(p->auth_header_name);
    if (p->auth_header_value) duckdb_free(p->auth_header_value);
    if (p->allow_subjects_json) duckdb_free(p->allow_subjects_json);
    memset(p, 0, sizeof(*p));
}

static int
ducknng_http_profile_string_clean(const char *s)
{
    const unsigned char *p;

    if (!s || !s[0]) return 0;
    for (p = (const unsigned char *)s; *p; p++) {
        if (*p < 0x20 || *p == 0x7f) return 0;
    }
    return 1;
}

static char *
ducknng_http_profile_dup_trim_lower(const char *s)
{
    const char *start;
    const char *end;
    char *out;
    size_t len;
    size_t i;

    if (!s) return NULL;
    start = s;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') start++;
    end = start + strlen(start);
    while (end > start && (end[-1] == ' ' || end[-1] == '\t' ||
        end[-1] == '\r' || end[-1] == '\n')) end--;
    len = (size_t)(end - start);
    if (len == 0) return NULL;
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)start[i];
        out[i] = (char)ducknng_ascii_tolower_int(c);
    }
    out[len] = '\0';
    return out;
}

static char *
ducknng_http_profile_dup_trim_upper(const char *s)
{
    const char *start;
    const char *end;
    char *out;
    size_t len;
    size_t i;

    if (!s) return NULL;
    start = s;
    while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n') start++;
    end = start + strlen(start);
    while (end > start && (end[-1] == ' ' || end[-1] == '\t' ||
        end[-1] == '\r' || end[-1] == '\n')) end--;
    len = (size_t)(end - start);
    if (len == 0) return NULL;
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)start[i];
        if (c >= 'a' && c <= 'z') out[i] = (char)(c - ('a' - 'A'));
        else out[i] = (char)c;
    }
    out[len] = '\0';
    return out;
}

static char *
ducknng_http_profile_dup_or_default(const char *s, const char *dflt)
{
    if (!s || !s[0]) return ducknng_strdup(dflt);
    return ducknng_strdup(s);
}

static int
ducknng_http_profile_method_valid(const char *method)
{
    if (!method || !method[0]) return 0;
    if (strcmp(method, "*") == 0) return 1;
    return ducknng_http_token_is_valid(method);
}

static int
ducknng_http_profile_header_value_valid(const char *value)
{
    return value && value[0] && ducknng_http_header_value_is_valid(value);
}

static int
ducknng_http_profile_path_in_scope(const char *path, const char *prefix)
{
    size_t prefix_len;

    if (!path || !prefix || prefix[0] != '/') return 0;
    prefix_len = strlen(prefix);
    if (prefix_len == 0) return 0;
    if (strcmp(prefix, "/") == 0) return 1;
    if (strncmp(path, prefix, prefix_len) != 0) return 0;
    if (path[prefix_len] == '\0') return 1;
    if (prefix[prefix_len - 1] == '/') return 1;
    return path[prefix_len] == '/';
}

static int
ducknng_http_profile_copy(ducknng_http_profile *dst, const ducknng_http_profile *src)
{
    if (!dst || !src) return -1;
    memset(dst, 0, sizeof(*dst));
    dst->profile_id = src->profile_id ? ducknng_strdup(src->profile_id) : NULL;
    dst->scheme = src->scheme ? ducknng_strdup(src->scheme) : NULL;
    dst->host = src->host ? ducknng_strdup(src->host) : NULL;
    dst->path_prefix = src->path_prefix ? ducknng_strdup(src->path_prefix) : NULL;
    dst->method = src->method ? ducknng_strdup(src->method) : NULL;
    dst->auth_header_name = src->auth_header_name ? ducknng_strdup(src->auth_header_name) : NULL;
    dst->auth_header_value = src->auth_header_value ? ducknng_strdup(src->auth_header_value) : NULL;
    dst->allow_subjects_json = src->allow_subjects_json ? ducknng_strdup(src->allow_subjects_json) : NULL;
    if ((src->profile_id && !dst->profile_id) || (src->scheme && !dst->scheme) ||
        (src->host && !dst->host) || (src->path_prefix && !dst->path_prefix) ||
        (src->method && !dst->method) || (src->auth_header_name && !dst->auth_header_name) ||
        (src->auth_header_value && !dst->auth_header_value) ||
        (src->allow_subjects_json && !dst->allow_subjects_json)) {
        ducknng_http_profile_reset(dst);
        return -1;
    }
    dst->port = src->port;
    dst->has_port = src->has_port;
    dst->tls_required = src->tls_required;
    dst->version = src->version;
    dst->created_ms = src->created_ms;
    dst->updated_ms = src->updated_ms;
    dst->expires_at_ms = src->expires_at_ms;
    return 0;
}

void
ducknng_http_profile_info_reset(ducknng_http_profile_info *info)
{
    if (!info) return;
    if (info->profile_id) duckdb_free(info->profile_id);
    if (info->scheme) duckdb_free(info->scheme);
    if (info->host) duckdb_free(info->host);
    if (info->path_prefix) duckdb_free(info->path_prefix);
    if (info->method) duckdb_free(info->method);
    if (info->auth_header_names_json) duckdb_free(info->auth_header_names_json);
    if (info->allow_subjects_json) duckdb_free(info->allow_subjects_json);
    memset(info, 0, sizeof(*info));
}

static int
ducknng_http_profile_public_copy(ducknng_http_profile_info *dst,
    const ducknng_http_profile *src)
{
    static const char prefix[] = "[\"";
    static const char suffix[] = "\"]";
    size_t header_len;
    size_t need;

    if (!dst || !src) return -1;
    memset(dst, 0, sizeof(*dst));
    dst->profile_id = src->profile_id ? ducknng_strdup(src->profile_id) : NULL;
    dst->scheme = src->scheme ? ducknng_strdup(src->scheme) : NULL;
    dst->host = src->host ? ducknng_strdup(src->host) : NULL;
    dst->path_prefix = src->path_prefix ? ducknng_strdup(src->path_prefix) : NULL;
    dst->method = src->method ? ducknng_strdup(src->method) : NULL;
    if ((src->profile_id && !dst->profile_id) || (src->scheme && !dst->scheme) ||
        (src->host && !dst->host) || (src->path_prefix && !dst->path_prefix) ||
        (src->method && !dst->method)) {
        goto fail;
    }
    header_len = src->auth_header_name ? strlen(src->auth_header_name) : 0;
    need = sizeof(prefix) - 1 + header_len + sizeof(suffix);
    dst->auth_header_names_json = (char *)duckdb_malloc(need);
    if (!dst->auth_header_names_json) goto fail;
    snprintf(dst->auth_header_names_json, need, "[\"%s\"]",
        src->auth_header_name ? src->auth_header_name : "");
    if (src->allow_subjects_json) {
        dst->allow_subjects_json = ducknng_strdup(src->allow_subjects_json);
        if (!dst->allow_subjects_json) goto fail;
    }
    dst->port = src->port;
    dst->has_port = src->has_port;
    dst->tls_required = src->tls_required;
    dst->version = src->version;
    dst->created_ms = src->created_ms;
    dst->updated_ms = src->updated_ms;
    dst->expires_at_ms = src->expires_at_ms;
    return 0;
fail:
    ducknng_http_profile_info_reset(dst);
    return -1;
}

static int
ducknng_http_profiles_reserve(ducknng_runtime *rt, size_t want, char **errmsg)
{
    ducknng_http_profile *next;
    size_t cap;

    if (!rt) return -1;
    if (rt->http_profile_cap >= want) return 0;
    cap = rt->http_profile_cap ? rt->http_profile_cap * 2 : 4;
    while (cap < want) cap *= 2;
    next = (ducknng_http_profile *)duckdb_malloc(sizeof(*next) * cap);
    if (!next) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing HTTP profile registry");
        return -1;
    }
    memset(next, 0, sizeof(*next) * cap);
    if (rt->http_profiles && rt->http_profile_count > 0) {
        memcpy(next, rt->http_profiles, sizeof(*next) * rt->http_profile_count);
        duckdb_free(rt->http_profiles);
    }
    rt->http_profiles = next;
    rt->http_profile_cap = cap;
    return 0;
}

int
ducknng_runtime_upsert_http_profile(ducknng_runtime *rt, const char *profile_id,
    const char *scheme, const char *host, int32_t port, int has_port,
    const char *path_prefix, const char *method, int tls_required,
    const char *auth_header_name, const char *auth_header_value,
    uint64_t expires_at_ms, const char *allow_subjects_json, char **errmsg)
{
    ducknng_http_profile next;
    size_t i;
    uint64_t now;
    int rc = -1;

    if (errmsg) *errmsg = NULL;
    memset(&next, 0, sizeof(next));
    if (!rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: runtime not initialized");
        return -1;
    }
    if (allow_subjects_json && allow_subjects_json[0]) {
        size_t subject_count = 0;
        char *parse_err = NULL;
        if (ducknng_json_string_array_contains(allow_subjects_json, NULL,
                &subject_count, &parse_err) < 0) {
            if (parse_err) duckdb_free(parse_err);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile allow_subjects_json must be a JSON array of strings");
            return -1;
        }
        if (subject_count == 0) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile allow_subjects_json must list at least one subject; pass NULL for an unrestricted profile");
            return -1;
        }
    }
    if (!ducknng_http_profile_string_clean(profile_id)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile id must be a non-empty string without control characters");
        return -1;
    }
    if (!ducknng_http_profile_string_clean(host)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile host must be a non-empty host name");
        return -1;
    }
    if (!path_prefix || !path_prefix[0] || path_prefix[0] != '/') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile path_prefix must start with '/'");
        return -1;
    }
    if (!ducknng_http_token_is_valid(auth_header_name)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile auth header name must be an HTTP token");
        return -1;
    }
    if (!ducknng_http_profile_header_value_valid(auth_header_value)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile auth header value must be non-empty and must not contain control characters");
        return -1;
    }
    if (has_port && (port <= 0 || port > 65535)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile port must be between 1 and 65535");
        return -1;
    }
    next.profile_id = ducknng_strdup(profile_id);
    next.scheme = ducknng_http_profile_dup_trim_lower(scheme);
    next.host = ducknng_http_profile_dup_trim_lower(host);
    next.path_prefix = ducknng_strdup(path_prefix);
    next.method = ducknng_http_profile_dup_trim_upper(method && method[0] ? method : "*");
    next.auth_header_name = ducknng_strdup(auth_header_name);
    next.auth_header_value = ducknng_strdup(auth_header_value);
    next.allow_subjects_json = allow_subjects_json && allow_subjects_json[0] ?
        ducknng_strdup(allow_subjects_json) : NULL;
    if (!next.profile_id || !next.scheme || !next.host || !next.path_prefix ||
        !next.method || !next.auth_header_name || !next.auth_header_value ||
        (allow_subjects_json && allow_subjects_json[0] && !next.allow_subjects_json)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP profile");
        goto done;
    }
    if (!(strcmp(next.scheme, "http") == 0 || strcmp(next.scheme, "https") == 0)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile scheme must be http or https");
        goto done;
    }
    if (tls_required && strcmp(next.scheme, "https") != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile tls_required requires scheme = 'https'");
        goto done;
    }
    if (!ducknng_http_profile_method_valid(next.method)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile method must be an HTTP token or '*'");
        goto done;
    }
    next.port = has_port ? (uint16_t)port : 0;
    next.has_port = has_port ? 1 : 0;
    next.tls_required = tls_required ? 1 : 0;
    next.expires_at_ms = expires_at_ms;
    now = ducknng_wall_clock_ms();
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->http_profile_count; i++) {
        if (rt->http_profiles[i].profile_id && strcmp(rt->http_profiles[i].profile_id, profile_id) == 0) {
            next.created_ms = rt->http_profiles[i].created_ms;
            next.updated_ms = now;
            next.version = rt->next_http_profile_version++;
            if (next.version == 0) next.version = rt->next_http_profile_version++;
            ducknng_http_profile_reset(&rt->http_profiles[i]);
            rt->http_profiles[i] = next;
            memset(&next, 0, sizeof(next));
            rc = 0;
            goto unlock;
        }
    }
    if (ducknng_http_profiles_reserve(rt, rt->http_profile_count + 1, errmsg) != 0) goto unlock;
    next.created_ms = now;
    next.updated_ms = now;
    next.version = rt->next_http_profile_version++;
    if (next.version == 0) next.version = rt->next_http_profile_version++;
    rt->http_profiles[rt->http_profile_count++] = next;
    memset(&next, 0, sizeof(next));
    rc = 0;
unlock:
    ducknng_mutex_unlock(&rt->mu);
done:
    ducknng_http_profile_reset(&next);
    return rc;
}

int
ducknng_runtime_drop_http_profile(ducknng_runtime *rt, const char *profile_id)
{
    size_t i;
    int removed = 0;

    if (!rt || !profile_id || !profile_id[0]) return 0;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->http_profile_count; i++) {
        if (rt->http_profiles[i].profile_id && strcmp(rt->http_profiles[i].profile_id, profile_id) == 0) {
            ducknng_http_profile_reset(&rt->http_profiles[i]);
            for (; i + 1 < rt->http_profile_count; i++) rt->http_profiles[i] = rt->http_profiles[i + 1];
            memset(&rt->http_profiles[rt->http_profile_count - 1], 0, sizeof(rt->http_profiles[0]));
            rt->http_profile_count--;
            removed = 1;
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return removed;
}

int
ducknng_runtime_http_profiles_snapshot(ducknng_runtime *rt,
    ducknng_http_profile_info **out_profiles, size_t *out_count, char **errmsg)
{
    ducknng_http_profile_info *out = NULL;
    size_t i;

    if (out_profiles) *out_profiles = NULL;
    if (out_count) *out_count = 0;
    if (errmsg) *errmsg = NULL;
    if (!rt || !out_profiles || !out_count) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP profile registry");
        return -1;
    }
    ducknng_mutex_lock(&rt->mu);
    if (rt->http_profile_count > 0) {
        out = (ducknng_http_profile_info *)duckdb_malloc(sizeof(*out) * rt->http_profile_count);
        if (!out) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory snapshotting HTTP profiles");
            return -1;
        }
        memset(out, 0, sizeof(*out) * rt->http_profile_count);
        for (i = 0; i < rt->http_profile_count; i++) {
            if (ducknng_http_profile_public_copy(&out[i], &rt->http_profiles[i]) != 0) {
                ducknng_mutex_unlock(&rt->mu);
                ducknng_runtime_http_profiles_snapshot_free(out, rt->http_profile_count);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP profile snapshot");
                return -1;
            }
        }
    }
    *out_profiles = out;
    *out_count = rt->http_profile_count;
    ducknng_mutex_unlock(&rt->mu);
    return 0;
}

void
ducknng_runtime_http_profiles_snapshot_free(ducknng_http_profile_info *profiles, size_t count)
{
    size_t i;

    if (!profiles) return;
    for (i = 0; i < count; i++) ducknng_http_profile_info_reset(&profiles[i]);
    duckdb_free(profiles);
}

void
ducknng_runtime_http_profiles_reset(ducknng_runtime *rt)
{
    size_t i;

    if (!rt || !rt->http_profiles) return;
    for (i = 0; i < rt->http_profile_count; i++) {
        ducknng_http_profile_reset(&rt->http_profiles[i]);
    }
    duckdb_free(rt->http_profiles);
    rt->http_profiles = NULL;
    rt->http_profile_count = 0;
    rt->http_profile_cap = 0;
}

static int
ducknng_http_profile_find_copy(ducknng_runtime *rt, const char *profile_id,
    ducknng_http_profile *out, char **errmsg)
{
    size_t i;

    if (out) memset(out, 0, sizeof(*out));
    if (!rt || !profile_id || !profile_id[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile id is required");
        return -1;
    }
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->http_profile_count; i++) {
        if (rt->http_profiles[i].profile_id && strcmp(rt->http_profiles[i].profile_id, profile_id) == 0) {
            if (ducknng_http_profile_copy(out, &rt->http_profiles[i]) != 0) {
                ducknng_mutex_unlock(&rt->mu);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP profile");
                return -1;
            }
            ducknng_mutex_unlock(&rt->mu);
            return 0;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile not found");
    return -1;
}

static int
ducknng_http_profile_parse_port(const char *s, uint16_t dflt, uint16_t *out)
{
    char *end = NULL;
    unsigned long value;

    if (!out) return -1;
    if (!s || !s[0]) {
        *out = dflt;
        return 0;
    }
    value = strtoul(s, &end, 10);
    if (end == s || *end != '\0' || value == 0 || value > 65535UL) return -1;
    *out = (uint16_t)value;
    return 0;
}

static char *
ducknng_http_profile_json_escape_dup(const char *s)
{
    char *out;
    char *p;
    size_t len = 0;
    size_t i;

    if (!s) s = "";
    for (i = 0; s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
        case '"':
        case '\\':
        case '\n':
        case '\r':
        case '\t':
            len += 2;
            break;
        default:
            if (c < 0x20) len += 6;
            else len++;
            break;
        }
    }
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    p = out;
    for (i = 0; s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
        case '"': *p++ = '\\'; *p++ = '"'; break;
        case '\\': *p++ = '\\'; *p++ = '\\'; break;
        case '\n': *p++ = '\\'; *p++ = 'n'; break;
        case '\r': *p++ = '\\'; *p++ = 'r'; break;
        case '\t': *p++ = '\\'; *p++ = 't'; break;
        default:
            if (c < 0x20) {
                snprintf(p, 7, "\\u%04x", (unsigned)c);
                p += 6;
            } else {
                *p++ = (char)c;
            }
            break;
        }
    }
    *p = '\0';
    return out;
}

static char *
ducknng_http_profile_auth_header_object(const char *name, const char *value)
{
    char *ename = NULL;
    char *evalue = NULL;
    char *out = NULL;
    size_t need;

    ename = ducknng_http_profile_json_escape_dup(name);
    evalue = ducknng_http_profile_json_escape_dup(value);
    if (!ename || !evalue) goto done;
    need = strlen(ename) + strlen(evalue) + strlen("{\"name\":\"\",\"value\":\"\"}") + 1;
    out = (char *)duckdb_malloc(need);
    if (!out) goto done;
    snprintf(out, need, "{\"name\":\"%s\",\"value\":\"%s\"}", ename, evalue);
done:
    if (ename) duckdb_free(ename);
    if (evalue) duckdb_free(evalue);
    return out;
}

static int
ducknng_http_profile_headers_empty_array(const char *headers_json, const char *end_bracket)
{
    const char *p = headers_json;

    if (!headers_json || !end_bracket) return 1;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p != '[') return 0;
    p++;
    while (p < end_bracket && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;
    return p == end_bracket;
}

static char *
ducknng_http_profile_merge_headers(const char *headers_json,
    const char *auth_header_name, const char *auth_header_value, char **errmsg)
{
    char *auth = NULL;
    char *out = NULL;
    const char *end;
    size_t prefix_len;
    size_t need;
    int have_headers;

    if (errmsg) *errmsg = NULL;
    auth = ducknng_http_profile_auth_header_object(auth_header_name, auth_header_value);
    if (!auth) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building HTTP profile headers");
        return NULL;
    }
    have_headers = headers_json && headers_json[0];
    if (!have_headers) {
        need = strlen(auth) + 3;
        out = (char *)duckdb_malloc(need);
        if (!out) goto oom;
        snprintf(out, need, "[%s]", auth);
        duckdb_free(auth);
        return out;
    }
    end = headers_json + strlen(headers_json);
    while (end > headers_json && (end[-1] == ' ' || end[-1] == '\t' ||
        end[-1] == '\r' || end[-1] == '\n')) end--;
    if (end == headers_json || end[-1] != ']') {
        duckdb_free(auth);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid headers_json");
        return NULL;
    }
    end--;
    if (ducknng_http_profile_headers_empty_array(headers_json, end)) {
        need = strlen(auth) + 3;
        out = (char *)duckdb_malloc(need);
        if (!out) goto oom;
        snprintf(out, need, "[%s]", auth);
        duckdb_free(auth);
        return out;
    }
    prefix_len = (size_t)(end - headers_json);
    need = prefix_len + strlen(auth) + 3;
    out = (char *)duckdb_malloc(need);
    if (!out) goto oom;
    memcpy(out, headers_json, prefix_len);
    out[prefix_len] = ',';
    memcpy(out + prefix_len + 1, auth, strlen(auth));
    out[prefix_len + 1 + strlen(auth)] = ']';
    out[prefix_len + 2 + strlen(auth)] = '\0';
    duckdb_free(auth);
    return out;
oom:
    if (auth) duckdb_free(auth);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory merging HTTP profile headers");
    return NULL;
}

int
ducknng_runtime_resolve_http_profile_headers(ducknng_runtime *rt,
    const char *profile_id, const char *url, const char *method,
    const char *headers_json, int has_connection_id, uint64_t connection_id,
    char **out_headers_json, char **errmsg)
{
    ducknng_http_profile profile;
    ducknng_transport_url transport;
    nng_url *parsed_url = NULL;
    char *req_method = NULL;
    char *caller_auth = NULL;
    const char *path;
    uint16_t want_port;
    uint16_t got_port;
    int rc = -1;

    if (out_headers_json) *out_headers_json = NULL;
    if (errmsg) *errmsg = NULL;
    memset(&profile, 0, sizeof(profile));
    if (!profile_id || !profile_id[0]) {
        if (out_headers_json) *out_headers_json = headers_json && headers_json[0] ? ducknng_strdup(headers_json) : NULL;
        return 0;
    }
    if (!rt || !url || !url[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile resolution requires runtime and URL");
        return -1;
    }
    if (ducknng_http_profile_find_copy(rt, profile_id, &profile, errmsg) != 0) return -1;
    if (profile.expires_at_ms != 0 && ducknng_wall_clock_ms() > profile.expires_at_ms) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile expired");
        goto done;
    }
    if (profile.allow_subjects_json && profile.allow_subjects_json[0]) {
        const char *subject = NULL;
        char *connection_subject = NULL;
        int admitted;
        /* The connection binding takes precedence over the thread-local
         * subject: a session connection carries its opener's subject, which
         * must govern the session's SQL even when a different caller's fetch
         * request is driving execution on this thread. */
        if (has_connection_id) {
            connection_subject =
                ducknng_runtime_execution_subject_for_connection_dup(rt, connection_id);
            subject = connection_subject;
        }
        if (!subject || !subject[0]) {
            const ducknng_execution_subject *subject_ctx =
                ducknng_runtime_current_thread_execution_subject_get(rt);
            subject = subject_ctx ? subject_ctx->subject : NULL;
        }
        admitted = subject && subject[0] &&
            ducknng_json_string_array_contains(profile.allow_subjects_json,
                subject, NULL, NULL) == 1;
        if (connection_subject) duckdb_free(connection_subject);
        if (!admitted) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile admission rejected request subject");
            goto done;
        }
    }
    if (ducknng_transport_url_parse(url, &transport, errmsg) != 0) goto done;
    if (!ducknng_transport_url_is_http(&transport)) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: http:// or https:// URL is required");
        goto done;
    }
    if (profile.tls_required && !transport.uses_tls) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile requires a TLS URL");
        goto done;
    }
    if (nng_url_parse(&parsed_url, url) != 0 || !parsed_url) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: failed to parse HTTP URL for profile scope");
        goto done;
    }
    if (!parsed_url->u_scheme || !ducknng_ascii_ieq(parsed_url->u_scheme, profile.scheme)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile scope rejected URL scheme");
        goto done;
    }
    if (!parsed_url->u_hostname || !ducknng_ascii_ieq(parsed_url->u_hostname, profile.host)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile scope rejected URL host");
        goto done;
    }
    want_port = profile.port;
    if (ducknng_http_profile_parse_port(parsed_url->u_port,
            transport.uses_tls ? 443 : 80, &got_port) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile scope rejected invalid URL port");
        goto done;
    }
    if (profile.has_port && got_port != want_port) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile scope rejected URL port");
        goto done;
    }
    path = parsed_url->u_path && parsed_url->u_path[0] ? parsed_url->u_path : "/";
    if (!ducknng_http_profile_path_in_scope(path, profile.path_prefix)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile scope rejected URL path");
        goto done;
    }
    req_method = ducknng_http_profile_dup_trim_upper(method && method[0] ? method : "GET");
    if (!req_method) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP method");
        goto done;
    }
    if (!ducknng_http_token_is_valid(req_method)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP method must be an HTTP token");
        goto done;
    }
    if (strcmp(profile.method, "*") != 0 && strcmp(profile.method, req_method) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP profile scope rejected HTTP method");
        goto done;
    }
    if (headers_json && headers_json[0]) {
        int found = ducknng_http_headers_json_get_header(headers_json,
            profile.auth_header_name, 0, &caller_auth, errmsg);
        if (found < 0) goto done;
        if (found > 0) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: caller header collides with HTTP profile auth header");
            goto done;
        }
    }
    if (out_headers_json) {
        *out_headers_json = ducknng_http_profile_merge_headers(headers_json,
            profile.auth_header_name, profile.auth_header_value, errmsg);
        if (!*out_headers_json) goto done;
    }
    rc = 0;
done:
    if (caller_auth) duckdb_free(caller_auth);
    if (req_method) duckdb_free(req_method);
    if (parsed_url) nng_url_free(parsed_url);
    ducknng_http_profile_reset(&profile);
    return rc;
}
