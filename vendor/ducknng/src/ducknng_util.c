#include "ducknng_thread.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef _WIN32
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#else
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

DUCKDB_EXTENSION_EXTERN

char *ducknng_strdup(const char *s) {
    size_t n;
    char *out;
    if (!s) return NULL;
    n = strlen(s);
    out = (char *)duckdb_malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, s, n + 1);
    return out;
}

char *ducknng_make_temp_dir(const char *prefix) {
#ifndef _WIN32
    const char *base;
    const char *stem;
    size_t len;
    char *templ;
    base = getenv("TMPDIR");
    if (!base || !base[0]) base = "/tmp";
    stem = (prefix && prefix[0]) ? prefix : "ducknng-";
    len = strlen(base) + 1 + strlen(stem) + 6 + 1;
    templ = (char *)duckdb_malloc(len);
    if (!templ) return NULL;
    snprintf(templ, len, "%s/%sXXXXXX", base, stem);
    if (!mkdtemp(templ)) {
        duckdb_free(templ);
        return NULL;
    }
    return templ;
#else
    char temp_path[MAX_PATH];
    char file_path[MAX_PATH];
    const char *tag = "dng";
    UINT n;
    (void)prefix;
    n = GetTempPathA((DWORD)sizeof(temp_path), temp_path);
    if (n == 0 || n >= sizeof(temp_path)) return NULL;
    if (GetTempFileNameA(temp_path, tag, 0, file_path) == 0) return NULL;
    DeleteFileA(file_path);
    if (!CreateDirectoryA(file_path, NULL)) return NULL;
    return ducknng_strdup(file_path);
#endif
}

int ducknng_remove_file(const char *path) {
    if (!path || !path[0]) return -1;
#ifndef _WIN32
    return unlink(path);
#else
    return DeleteFileA(path) ? 0 : -1;
#endif
}

int ducknng_remove_dir(const char *path) {
    if (!path || !path[0]) return -1;
#ifndef _WIN32
    return rmdir(path);
#else
    return RemoveDirectoryA(path) ? 0 : -1;
#endif
}

uint64_t ducknng_now_ms(void) {
#ifndef _WIN32
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t)ts.tv_sec * 1000ULL) + (uint64_t)(ts.tv_nsec / 1000000ULL);
#else
    return (uint64_t)GetTickCount64();
#endif
}

uint64_t ducknng_wall_clock_ms(void) {
#ifndef _WIN32
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t)ts.tv_sec * 1000ULL) + (uint64_t)(ts.tv_nsec / 1000000ULL);
#else
    FILETIME ft;
    ULARGE_INTEGER value;
    const uint64_t windows_to_unix_100ns = 116444736000000000ULL;

    GetSystemTimeAsFileTime(&ft);
    value.LowPart = ft.dwLowDateTime;
    value.HighPart = ft.dwHighDateTime;
    if (value.QuadPart < windows_to_unix_100ns) return 0;
    return (uint64_t)((value.QuadPart - windows_to_unix_100ns) / 10000ULL);
#endif
}

void ducknng_sleep_ms(uint64_t ms) {
#ifndef _WIN32
    usleep((useconds_t)(ms * 1000ULL));
#else
    Sleep((DWORD)ms);
#endif
}

int ducknng_ascii_tolower_int(int c) {
    return (c >= 'A' && c <= 'Z') ? c + ('a' - 'A') : c;
}

int ducknng_ascii_ieq(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        if (ducknng_ascii_tolower_int((unsigned char)*a) !=
                ducknng_ascii_tolower_int((unsigned char)*b)) {
            return 0;
        }
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

int ducknng_ascii_istarts_with(const char *s, const char *prefix) {
    if (!s || !prefix) return 0;
    while (*prefix) {
        if (!*s || ducknng_ascii_tolower_int((unsigned char)*s) !=
                ducknng_ascii_tolower_int((unsigned char)*prefix)) {
            return 0;
        }
        s++;
        prefix++;
    }
    return 1;
}

int ducknng_ascii_iends_with(const char *s, const char *suffix) {
    size_t slen;
    size_t suffix_len;
    if (!s || !suffix) return 0;
    slen = strlen(s);
    suffix_len = strlen(suffix);
    if (suffix_len > slen) return 0;
    return ducknng_ascii_ieq(s + slen - suffix_len, suffix);
}

int ducknng_http_token_is_valid(const char *s) {
    const unsigned char *p = (const unsigned char *)s;
    if (!s || !s[0]) return 0;
    while (*p) {
        unsigned char ch = *p++;
        if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
            (ch >= '0' && ch <= '9') || ch == '!' || ch == '#' ||
            ch == '$' || ch == '%' || ch == '&' || ch == '\'' ||
            ch == '*' || ch == '+' || ch == '-' || ch == '.' ||
            ch == '^' || ch == '_' || ch == '`' || ch == '|' ||
            ch == '~') {
            continue;
        }
        return 0;
    }
    return 1;
}

int ducknng_http_header_value_is_valid(const char *s) {
    const unsigned char *p = (const unsigned char *)s;
    if (!s) return 0;
    while (*p) {
        unsigned char ch = *p++;
        if (ch < 0x20 || ch == 0x7f) return 0;
    }
    return 1;
}

static char *ducknng_dup_bytes(const char *src, size_t len) {
    char *out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

static void ducknng_json_skip_ws(const char **p) {
    while (p && *p && (**p == ' ' || **p == '\t' || **p == '\r' || **p == '\n')) (*p)++;
}

static int ducknng_hex_value(int c) {
    if (c >= '0' && c <= '9') return c - '0';
    c = ducknng_ascii_tolower_int(c);
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    return -1;
}

int ducknng_grow_capacity(size_t need, size_t current_cap, size_t min_cap, size_t *out_cap) {
    size_t cap;
    if (!out_cap) return -1;
    cap = current_cap ? current_cap : (min_cap ? min_cap : 1);
    while (cap < need) {
        if (cap > SIZE_MAX / 2) {
            cap = need;
            break;
        }
        cap *= 2;
    }
    *out_cap = cap;
    return 0;
}

char *ducknng_join_dotted_path(const char *prefix, const char *name) {
    size_t plen = prefix ? strlen(prefix) : 0;
    size_t nlen = name ? strlen(name) : 0;
    size_t need;
    char *out;
    if (plen == 0) {
        out = (char *)duckdb_malloc(nlen + 1);
        if (!out) return NULL;
        if (nlen) memcpy(out, name, nlen);
        out[nlen] = '\0';
        return out;
    }
    if (ducknng_size_add(plen, nlen, &need) != 0 ||
        ducknng_size_add(need, 2, &need) != 0) return NULL;
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    memcpy(out, prefix, plen);
    out[plen] = '.';
    if (nlen) memcpy(out + plen + 1, name, nlen);
    out[plen + 1 + nlen] = '\0';
    return out;
}

static int ducknng_buf_append(char **buf, size_t *len, size_t *cap, const char *src, size_t src_len) {
    char *next;
    size_t want;
    size_t new_cap;
    if (!buf || !len || !cap) return -1;
    if (ducknng_size_add(*len, src_len, &want) != 0 ||
        ducknng_size_add(want, 1, &want) != 0) return -1;
    if (*cap < want) {
        if (ducknng_grow_capacity(want, *cap, 32, &new_cap) != 0) return -1;
        next = (char *)duckdb_malloc(new_cap);
        if (!next) return -1;
        if (*buf && *len) memcpy(next, *buf, *len);
        if (*buf) duckdb_free(*buf);
        *buf = next;
        *cap = new_cap;
    }
    if (src_len) memcpy(*buf + *len, src, src_len);
    *len += src_len;
    (*buf)[*len] = '\0';
    return 0;
}

static char *ducknng_format_name_error(const char *fmt, const char *name) {
    size_t need;
    char *out;
    if (!fmt || !name) return NULL;
    need = (size_t)snprintf(NULL, 0, fmt, name) + 1;
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    snprintf(out, need, fmt, name);
    return out;
}

static char *ducknng_json_parse_string_dup(const char **p, char **errmsg) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    if (errmsg) *errmsg = NULL;
    if (!p || !*p || **p != '"') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected JSON string");
        return NULL;
    }
    (*p)++;
    while (**p) {
        char c = *(*p)++;
        if (c == '"') return buf ? buf : ducknng_strdup("");
        if ((unsigned char)c < 0x20) {
            if (buf) duckdb_free(buf);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid control character in JSON string");
            return NULL;
        }
        if (c == '\\') {
            char esc = *(*p)++;
            switch (esc) {
            case '"': c = '"'; break;
            case '\\': c = '\\'; break;
            case '/': c = '/'; break;
            case 'b': c = '\b'; break;
            case 'f': c = '\f'; break;
            case 'n': c = '\n'; break;
            case 'r': c = '\r'; break;
            case 't': c = '\t'; break;
            case 'u': {
                int i;
                unsigned value = 0;
                for (i = 0; i < 4; i++) {
                    char h = *(*p)++;
                    int v = ducknng_hex_value((unsigned char)h);
                    if (v < 0) {
                        if (buf) duckdb_free(buf);
                        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid JSON unicode escape");
                        return NULL;
                    }
                    value = (value << 4) | (unsigned)v;
                }
                if (value == 0) {
                    if (buf) duckdb_free(buf);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: JSON strings may not contain NUL");
                    return NULL;
                }
                if (value > 0x7f) {
                    if (buf) duckdb_free(buf);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: only ASCII JSON unicode escapes are supported");
                    return NULL;
                }
                c = (char)value;
                break;
            }
            case '\0':
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: unterminated JSON escape");
                return NULL;
            default:
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported JSON escape");
                return NULL;
            }
        }
        if (ducknng_buf_append(&buf, &len, &cap, &c, 1) != 0) {
            if (buf) duckdb_free(buf);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing JSON string");
            return NULL;
        }
    }
    if (buf) duckdb_free(buf);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: unterminated JSON string");
    return NULL;
}

int ducknng_json_object_get_string(const char *json, const char *wanted_name,
    int ascii_case_insensitive, int reject_duplicates, char **out_value, char **errmsg) {
    const char *p = json;
    char *found = NULL;
    int found_match = 0;
    if (out_value) *out_value = NULL;
    if (errmsg) *errmsg = NULL;
    if (!json || !wanted_name || !wanted_name[0]) return 0;
    ducknng_json_skip_ws(&p);
    if (*p != '{') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected JSON object");
        return -1;
    }
    p++;
    ducknng_json_skip_ws(&p);
    if (*p == '}') {
        p++;
        ducknng_json_skip_ws(&p);
        if (*p != '\0') {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after JSON object");
            return -1;
        }
        return 0;
    }
    for (;;) {
        char *key = NULL;
        char *value = NULL;
        int is_match;
        key = ducknng_json_parse_string_dup(&p, errmsg);
        if (!key) goto fail;
        ducknng_json_skip_ws(&p);
        if (*p != ':') {
            duckdb_free(key);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: expected ':' in JSON object");
            goto fail;
        }
        p++;
        ducknng_json_skip_ws(&p);
        value = ducknng_json_parse_string_dup(&p, errmsg);
        if (!value) {
            duckdb_free(key);
            goto fail;
        }
        is_match = ascii_case_insensitive ? ducknng_ascii_ieq(key, wanted_name) : strcmp(key, wanted_name) == 0;
        if (is_match) {
            if (found_match && reject_duplicates) {
                duckdb_free(key);
                duckdb_free(value);
                if (errmsg) *errmsg = ducknng_format_name_error("ducknng: duplicate JSON field '%s'", wanted_name);
                goto fail;
            }
            if (found) duckdb_free(found);
            found = value;
            value = NULL;
            found_match = 1;
        }
        duckdb_free(key);
        if (value) duckdb_free(value);
        ducknng_json_skip_ws(&p);
        if (*p == ',') {
            p++;
            ducknng_json_skip_ws(&p);
            continue;
        }
        if (*p == '}') {
            p++;
            break;
        }
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or '}' in JSON object");
        goto fail;
    }
    ducknng_json_skip_ws(&p);
    if (*p != '\0') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after JSON object");
        goto fail;
    }
    if (out_value && found) *out_value = found;
    else if (found) duckdb_free(found);
    return found_match ? 1 : 0;
fail:
    if (found) duckdb_free(found);
    return -1;
}

int ducknng_json_string_array_contains(const char *json, const char *wanted,
    size_t *out_count, char **errmsg) {
    const char *p = json;
    size_t count = 0;
    int found = 0;
    if (out_count) *out_count = 0;
    if (errmsg) *errmsg = NULL;
    if (!json) return 0;
    ducknng_json_skip_ws(&p);
    if (*p != '[') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected JSON array of strings");
        return -1;
    }
    p++;
    ducknng_json_skip_ws(&p);
    if (*p != ']') {
        for (;;) {
            char *entry = ducknng_json_parse_string_dup(&p, errmsg);
            if (!entry) return -1;
            count++;
            if (wanted && strcmp(entry, wanted) == 0) found = 1;
            duckdb_free(entry);
            ducknng_json_skip_ws(&p);
            if (*p == ',') {
                p++;
                ducknng_json_skip_ws(&p);
                continue;
            }
            if (*p == ']') break;
            if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or ']' in JSON array");
            return -1;
        }
    }
    p++;
    ducknng_json_skip_ws(&p);
    if (*p != '\0') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after JSON array");
        return -1;
    }
    if (out_count) *out_count = count;
    return found;
}

int ducknng_http_headers_json_get_header(const char *headers_json, const char *wanted_name,
    int reject_duplicates, char **out_value, char **errmsg) {
    const char *p = headers_json;
    char *found = NULL;
    char *key = NULL;
    char *value = NULL;
    char *name = NULL;
    char *header_value = NULL;
    int found_match = 0;
    if (out_value) *out_value = NULL;
    if (errmsg) *errmsg = NULL;
    if (!headers_json || !wanted_name || !wanted_name[0]) return 0;
    ducknng_json_skip_ws(&p);
    if (*p != '[') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: headers_json must be a JSON array of {name,value} objects");
        return -1;
    }
    p++;
    ducknng_json_skip_ws(&p);
    if (*p == ']') {
        p++;
        ducknng_json_skip_ws(&p);
        if (*p != '\0') {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after headers_json");
            return -1;
        }
        return 0;
    }
    for (;;) {
        key = NULL;
        value = NULL;
        name = NULL;
        header_value = NULL;
        ducknng_json_skip_ws(&p);
        if (*p != '{') {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: expected header object in headers_json");
            goto fail;
        }
        p++;
        for (;;) {
            ducknng_json_skip_ws(&p);
            key = ducknng_json_parse_string_dup(&p, errmsg);
            if (!key) goto fail;
            ducknng_json_skip_ws(&p);
            if (*p != ':') {
                duckdb_free(key);
                if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: expected ':' in headers_json");
                goto fail;
            }
            p++;
            ducknng_json_skip_ws(&p);
            value = ducknng_json_parse_string_dup(&p, errmsg);
            if (!value) {
                duckdb_free(key);
                goto fail;
            }
            if (strcmp(key, "name") == 0) {
                if (name) {
                    duckdb_free(key);
                    duckdb_free(value);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: duplicate header name field in headers_json");
                    goto fail;
                }
                name = value;
                value = NULL;
            } else if (strcmp(key, "value") == 0) {
                if (header_value) {
                    duckdb_free(key);
                    duckdb_free(value);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: duplicate header value field in headers_json");
                    goto fail;
                }
                header_value = value;
                value = NULL;
            } else {
                duckdb_free(key);
                duckdb_free(value);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: headers_json objects may contain only name and value fields");
                goto fail;
            }
            duckdb_free(key);
            key = NULL;
            if (value) {
                duckdb_free(value);
                value = NULL;
            }
            ducknng_json_skip_ws(&p);
            if (*p == ',') {
                p++;
                continue;
            }
            if (*p == '}') {
                p++;
                break;
            }
            if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or '}' in headers_json");
            goto fail;
        }
        if (!name || !name[0] || !header_value) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: each headers_json object must contain non-empty name and a string value");
            goto fail;
        }
        if (!ducknng_http_header_value_is_valid(header_value)) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP header value must not contain control characters");
            goto fail;
        }
        if (ducknng_ascii_ieq(name, wanted_name)) {
            if (found_match && reject_duplicates) {
                if (errmsg) *errmsg = ducknng_format_name_error("ducknng: duplicate HTTP header '%s'", wanted_name);
                goto fail;
            }
            if (found) duckdb_free(found);
            found = ducknng_strdup(header_value);
            if (!found) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying header value");
                goto fail;
            }
            found_match = 1;
        }
        duckdb_free(name);
        duckdb_free(header_value);
        name = NULL;
        header_value = NULL;
        ducknng_json_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') {
            p++;
            break;
        }
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or ']' in headers_json");
        goto fail;
    }
    ducknng_json_skip_ws(&p);
    if (*p != '\0') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after headers_json");
        goto fail;
    }
    if (out_value && found) *out_value = found;
    else if (found) duckdb_free(found);
    return found_match ? 1 : 0;
fail:
    if (key) duckdb_free(key);
    if (value) duckdb_free(value);
    if (name) duckdb_free(name);
    if (header_value) duckdb_free(header_value);
    if (found) duckdb_free(found);
    return -1;
}

static char *ducknng_percent_decode_dup(const char *src, size_t len, int plus_as_space,
    const char *label, char **errmsg) {
    char *out;
    size_t i;
    size_t j = 0;
    if (errmsg) *errmsg = NULL;
    if (!src) return NULL;
    out = (char *)duckdb_malloc(len + 1);
    if (!out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory decoding URL component");
        return NULL;
    }
    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == '%') {
            int hi;
            int lo;
            if (i + 2 >= len) {
                duckdb_free(out);
                if (errmsg) *errmsg = ducknng_format_name_error("ducknng: invalid percent escape in %s", label ? label : "URL component");
                return NULL;
            }
            hi = ducknng_hex_value((unsigned char)src[i + 1]);
            lo = ducknng_hex_value((unsigned char)src[i + 2]);
            if (hi < 0 || lo < 0) {
                duckdb_free(out);
                if (errmsg) *errmsg = ducknng_format_name_error("ducknng: invalid percent escape in %s", label ? label : "URL component");
                return NULL;
            }
            c = (unsigned char)((hi << 4) | lo);
            if (c == 0) {
                duckdb_free(out);
                if (errmsg) *errmsg = ducknng_format_name_error("ducknng: %s contains NUL after percent decoding", label ? label : "URL component");
                return NULL;
            }
            i += 2;
        } else if (plus_as_space && c == '+') {
            c = ' ';
        }
        out[j++] = (char)c;
    }
    out[j] = '\0';
    return out;
}

int ducknng_query_string_get_param(const char *query_string, const char *wanted_name,
    int reject_duplicates, char **out_value, char **errmsg) {
    const char *p = query_string;
    char *found = NULL;
    int found_match = 0;
    if (out_value) *out_value = NULL;
    if (errmsg) *errmsg = NULL;
    if (!query_string || !wanted_name || !wanted_name[0]) return 0;
    while (p && *p) {
        const char *segment_end = strchr(p, '&');
        const char *eq;
        size_t name_len;
        char *name;
        char *value = NULL;
        char *decode_err = NULL;
        if (!segment_end) segment_end = p + strlen(p);
        eq = memchr(p, '=', (size_t)(segment_end - p));
        name_len = eq ? (size_t)(eq - p) : (size_t)(segment_end - p);
        name = ducknng_percent_decode_dup(p, name_len, 1, "query parameter name", &decode_err);
        if (!name) {
            if (errmsg) *errmsg = decode_err;
            goto fail;
        }
        if (strcmp(name, wanted_name) == 0) {
            size_t value_len = eq ? (size_t)(segment_end - eq - 1) : 0;
            if (found_match && reject_duplicates) {
                duckdb_free(name);
                if (errmsg) *errmsg = ducknng_format_name_error("ducknng: duplicate query parameter '%s'", wanted_name);
                goto fail;
            }
            value = ducknng_percent_decode_dup(eq ? eq + 1 : "", value_len, 1, "query parameter value", &decode_err);
            if (!value) {
                duckdb_free(name);
                if (errmsg) *errmsg = decode_err;
                goto fail;
            }
            if (found) duckdb_free(found);
            found = value;
            value = NULL;
            found_match = 1;
        }
        duckdb_free(name);
        if (value) duckdb_free(value);
        p = *segment_end ? segment_end + 1 : segment_end;
    }
    if (out_value && found) *out_value = found;
    else if (found) duckdb_free(found);
    return found_match ? 1 : 0;
fail:
    if (found) duckdb_free(found);
    return -1;
}

int ducknng_cookie_header_get_value(const char *cookie_header, const char *wanted_name,
    int reject_duplicates, char **out_value, char **errmsg) {
    const char *p = cookie_header;
    char *found = NULL;
    int found_match = 0;
    if (out_value) *out_value = NULL;
    if (errmsg) *errmsg = NULL;
    if (!cookie_header || !wanted_name || !wanted_name[0]) return 0;
    while (p && *p) {
        const char *segment_end = strchr(p, ';');
        const char *eq;
        const char *name_start;
        const char *name_end;
        const char *value_start;
        const char *value_end;
        char *name;
        char *value;
        if (!segment_end) segment_end = p + strlen(p);
        while (p < segment_end && (*p == ' ' || *p == '\t')) p++;
        eq = memchr(p, '=', (size_t)(segment_end - p));
        if (!eq) {
            p = *segment_end ? segment_end + 1 : segment_end;
            continue;
        }
        name_start = p;
        name_end = eq;
        while (name_end > name_start && (name_end[-1] == ' ' || name_end[-1] == '\t')) name_end--;
        value_start = eq + 1;
        while (value_start < segment_end && (*value_start == ' ' || *value_start == '\t')) value_start++;
        value_end = segment_end;
        while (value_end > value_start && (value_end[-1] == ' ' || value_end[-1] == '\t')) value_end--;
        name = ducknng_dup_bytes(name_start, (size_t)(name_end - name_start));
        if (!name) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing Cookie header");
            goto fail;
        }
        if ((size_t)(value_end - value_start) >= 2 && value_start[0] == '"' && value_end[-1] == '"') {
            value_start++;
            value_end--;
        }
        value = ducknng_dup_bytes(value_start, (size_t)(value_end - value_start));
        if (!value) {
            duckdb_free(name);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing Cookie header");
            goto fail;
        }
        if (strcmp(name, wanted_name) == 0) {
            if (found_match && reject_duplicates) {
                duckdb_free(name);
                duckdb_free(value);
                if (errmsg) *errmsg = ducknng_format_name_error("ducknng: duplicate cookie '%s'", wanted_name);
                goto fail;
            }
            if (found) duckdb_free(found);
            found = value;
            value = NULL;
            found_match = 1;
        }
        duckdb_free(name);
        if (value) duckdb_free(value);
        p = *segment_end ? segment_end + 1 : segment_end;
    }
    if (out_value && found) *out_value = found;
    else if (found) duckdb_free(found);
    return found_match ? 1 : 0;
fail:
    if (found) duckdb_free(found);
    return -1;
}

uint16_t ducknng_le16_read(const uint8_t *p) { return (uint16_t)(p[0] | ((uint16_t)p[1] << 8)); }
uint32_t ducknng_le32_read(const uint8_t *p) { return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }
uint64_t ducknng_le64_read(const uint8_t *p) { return (uint64_t)ducknng_le32_read(p) | ((uint64_t)ducknng_le32_read(p + 4) << 32); }
void ducknng_le16_write(uint8_t *p, uint16_t v) { p[0] = (uint8_t)(v & 0xff); p[1] = (uint8_t)((v >> 8) & 0xff); }
void ducknng_le32_write(uint8_t *p, uint32_t v) { p[0] = (uint8_t)(v & 0xff); p[1] = (uint8_t)((v >> 8) & 0xff); p[2] = (uint8_t)((v >> 16) & 0xff); p[3] = (uint8_t)((v >> 24) & 0xff); }
void ducknng_le64_write(uint8_t *p, uint64_t v) { ducknng_le32_write(p, (uint32_t)(v & 0xffffffffu)); ducknng_le32_write(p + 4, (uint32_t)(v >> 32)); }

#ifdef _WIN32
typedef struct {
    void *(*fn)(void *);
    void *arg;
} ducknng_thread_start;

static DWORD WINAPI ducknng_thread_main(LPVOID arg) {
    ducknng_thread_start *start = (ducknng_thread_start *)arg;
    if (start) {
        void *(*fn)(void *) = start->fn;
        void *fn_arg = start->arg;
        duckdb_free(start);
        if (fn) fn(fn_arg);
    }
    return 0;
}
#endif

int ducknng_thread_create(ducknng_thread *thread, void *(*fn)(void *), void *arg) {
#ifndef _WIN32
    return pthread_create(thread, NULL, fn, arg);
#else
    ducknng_thread_start *start;
    HANDLE handle;
    if (!thread || !fn) return -1;
    start = (ducknng_thread_start *)duckdb_malloc(sizeof(*start));
    if (!start) return -1;
    start->fn = fn;
    start->arg = arg;
    handle = CreateThread(NULL, 0, ducknng_thread_main, start, 0, NULL);
    if (!handle) {
        duckdb_free(start);
        return -1;
    }
    *thread = (ducknng_thread)handle;
    return 0;
#endif
}
void ducknng_thread_join(ducknng_thread thread) {
#ifndef _WIN32
    pthread_join(thread, NULL);
#else
    HANDLE handle = (HANDLE)thread;
    if (!handle) return;
    WaitForSingleObject(handle, INFINITE);
    CloseHandle(handle);
#endif
}
int ducknng_mutex_init(ducknng_mutex *mu) {
#ifndef _WIN32
    return pthread_mutex_init(mu, NULL);
#else
    CRITICAL_SECTION *cs;
    if (!mu) return -1;
    cs = (CRITICAL_SECTION *)duckdb_malloc(sizeof(*cs));
    if (!cs) return -1;
    InitializeCriticalSection(cs);
    *mu = (ducknng_mutex)cs;
    return 0;
#endif
}
void ducknng_mutex_lock(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_lock(mu);
#else
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cs) return;
    EnterCriticalSection(cs);
#endif
}
void ducknng_mutex_unlock(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_unlock(mu);
#else
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cs) return;
    LeaveCriticalSection(cs);
#endif
}
void ducknng_mutex_destroy(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_destroy(mu);
#else
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cs) return;
    DeleteCriticalSection(cs);
    duckdb_free(cs);
    *mu = NULL;
#endif
}
int ducknng_cond_init(ducknng_cond *cv) {
#ifndef _WIN32
    return pthread_cond_init(cv, NULL);
#else
    CONDITION_VARIABLE *cond;
    if (!cv) return -1;
    cond = (CONDITION_VARIABLE *)duckdb_malloc(sizeof(*cond));
    if (!cond) return -1;
    InitializeConditionVariable(cond);
    *cv = (ducknng_cond)cond;
    return 0;
#endif
}
void ducknng_cond_wait(ducknng_cond *cv, ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_cond_wait(cv, mu);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    if (!cond || !cs) return;
    SleepConditionVariableCS(cond, cs, INFINITE);
#endif
}
int ducknng_cond_timedwait_ms(ducknng_cond *cv, ducknng_mutex *mu, uint64_t timeout_ms) {
#ifndef _WIN32
    struct timespec ts;
    uint64_t ns_total;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) return -1;
    ns_total = (uint64_t)ts.tv_nsec + (timeout_ms % 1000ULL) * 1000000ULL;
    ts.tv_sec += (time_t)(timeout_ms / 1000ULL) + (time_t)(ns_total / 1000000000ULL);
    ts.tv_nsec = (long)(ns_total % 1000000000ULL);
    if (pthread_cond_timedwait(cv, mu, &ts) == ETIMEDOUT) return 1;
    return 0;
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    CRITICAL_SECTION *cs = mu ? (CRITICAL_SECTION *)(*mu) : NULL;
    DWORD rc;
    DWORD wait_ms = timeout_ms > (uint64_t)INFINITE - 1 ? (INFINITE - 1) : (DWORD)timeout_ms;
    if (!cond || !cs) return -1;
    rc = SleepConditionVariableCS(cond, cs, wait_ms);
    if (rc) return 0;
    return GetLastError() == ERROR_TIMEOUT ? 1 : -1;
#endif
}
void ducknng_cond_signal(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_signal(cv);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    if (!cond) return;
    WakeConditionVariable(cond);
#endif
}
void ducknng_cond_broadcast(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_broadcast(cv);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    if (!cond) return;
    WakeAllConditionVariable(cond);
#endif
}
void ducknng_cond_destroy(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_destroy(cv);
#else
    CONDITION_VARIABLE *cond = cv ? (CONDITION_VARIABLE *)(*cv) : NULL;
    if (!cond) return;
    duckdb_free(cond);
    *cv = NULL;
#endif
}
