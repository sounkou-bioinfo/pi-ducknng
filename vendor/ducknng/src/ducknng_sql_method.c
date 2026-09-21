/* SQL-defined RPC methods.
 *
 * A SQL method is a registry descriptor whose handler runs server-owned SQL.
 * The caller supplies only a JSON object payload; it never supplies SQL. The
 * statements run in one transaction on the service's request connection, and
 * each statement may bind the payload text through its single parameter. The
 * first column of the first row of the last statement becomes the JSON reply.
 *
 * Entries are owned by the runtime and are freed only when the runtime is
 * destroyed. Dispatch copies a descriptor under rt->mu and calls the handler
 * outside the lock, so an entry must outlive every snapshot that can point at
 * it, including snapshots taken before the method was replaced or
 * unregistered. */
#include "ducknng_sql_method.h"
#include "ducknng_registry.h"
#include "ducknng_runtime.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

struct ducknng_sql_method {
    ducknng_method_descriptor descriptor;
    char *name;
    char *summary;
    char *request_schema_json;
    char *handler_sql;
    struct ducknng_sql_method *next;
};

#define DUCKNNG_SQL_METHOD_MAX_REQUEST_BYTES (1024 * 1024)
#define DUCKNNG_SQL_METHOD_MAX_REPLY_BYTES (16 * 1024 * 1024)
#define DUCKNNG_JSON_MAX_DEPTH 256

static _Thread_local const ducknng_request_context *g_sql_method_request = NULL;

const ducknng_request_context *ducknng_sql_method_current_request(void) {
    return g_sql_method_request;
}

static const char *ducknng_sql_method_response_schema =
    "{\"type\":\"json\",\"source\":\"first column of the first row of the last statement\"}";

/* Minimal RFC 8259 syntax check used for registered schemas and request
 * payloads. It validates structure only; it does not build values. */
typedef struct {
    const unsigned char *p;
    const unsigned char *end;
    int depth;
} ducknng_json_cursor;

static void json_skip_ws(ducknng_json_cursor *c) {
    while (c->p < c->end && (*c->p == ' ' || *c->p == '\t' || *c->p == '\n' || *c->p == '\r')) c->p++;
}

static int json_hex(unsigned char ch) {
    return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
}

static int json_string(ducknng_json_cursor *c) {
    if (c->p >= c->end || *c->p != '"') return 0;
    c->p++;
    while (c->p < c->end) {
        unsigned char ch = *c->p++;
        if (ch == '"') return 1;
        if (ch < 0x20) return 0;
        if (ch == '\\') {
            if (c->p >= c->end) return 0;
            ch = *c->p++;
            if (ch == 'u') {
                int i;
                for (i = 0; i < 4; i++) {
                    if (c->p >= c->end || !json_hex(*c->p)) return 0;
                    c->p++;
                }
            } else if (!strchr("\"\\/bfnrt", ch)) {
                return 0;
            }
        }
    }
    return 0;
}

static int json_digits(ducknng_json_cursor *c) {
    const unsigned char *start = c->p;
    while (c->p < c->end && *c->p >= '0' && *c->p <= '9') c->p++;
    return c->p > start;
}

static int json_number(ducknng_json_cursor *c) {
    if (c->p < c->end && *c->p == '-') c->p++;
    if (c->p < c->end && *c->p == '0') {
        c->p++;
    } else if (!json_digits(c)) {
        return 0;
    }
    if (c->p < c->end && *c->p == '.') {
        c->p++;
        if (!json_digits(c)) return 0;
    }
    if (c->p < c->end && (*c->p == 'e' || *c->p == 'E')) {
        c->p++;
        if (c->p < c->end && (*c->p == '+' || *c->p == '-')) c->p++;
        if (!json_digits(c)) return 0;
    }
    return 1;
}

static int json_literal(ducknng_json_cursor *c, const char *word) {
    size_t n = strlen(word);
    if ((size_t)(c->end - c->p) < n || memcmp(c->p, word, n) != 0) return 0;
    c->p += n;
    return 1;
}

static int json_value(ducknng_json_cursor *c);

static int json_container(ducknng_json_cursor *c, unsigned char close, int object) {
    if (++c->depth > DUCKNNG_JSON_MAX_DEPTH) return 0;
    c->p++;
    json_skip_ws(c);
    if (c->p < c->end && *c->p == close) {
        c->p++;
        c->depth--;
        return 1;
    }
    for (;;) {
        if (object) {
            if (!json_string(c)) return 0;
            json_skip_ws(c);
            if (c->p >= c->end || *c->p != ':') return 0;
            c->p++;
        }
        if (!json_value(c)) return 0;
        json_skip_ws(c);
        if (c->p >= c->end) return 0;
        if (*c->p == ',') {
            c->p++;
            json_skip_ws(c);
            continue;
        }
        if (*c->p != close) return 0;
        c->p++;
        c->depth--;
        return 1;
    }
}

static int json_value(ducknng_json_cursor *c) {
    json_skip_ws(c);
    if (c->p >= c->end) return 0;
    switch (*c->p) {
    case '{': return json_container(c, '}', 1);
    case '[': return json_container(c, ']', 0);
    case '"': return json_string(c);
    case 't': return json_literal(c, "true");
    case 'f': return json_literal(c, "false");
    case 'n': return json_literal(c, "null");
    default: return json_number(c);
    }
}

int ducknng_json_is_object(const char *text, size_t len) {
    ducknng_json_cursor c;
    if (!text) return 0;
    c.p = (const unsigned char *)text;
    c.end = c.p + len;
    c.depth = 0;
    json_skip_ws(&c);
    if (c.p >= c.end || *c.p != '{') return 0;
    if (!json_value(&c)) return 0;
    json_skip_ws(&c);
    return c.p == c.end;
}

static int ducknng_utf8_valid(const unsigned char *s, size_t len) {
    size_t i = 0;
    while (i < len) {
        unsigned char ch = s[i];
        size_t need;
        uint32_t cp;
        size_t k;
        if (ch < 0x80) {
            if (ch == 0) return 0;
            i++;
            continue;
        }
        if ((ch & 0xE0) == 0xC0) { need = 1; cp = ch & 0x1F; }
        else if ((ch & 0xF0) == 0xE0) { need = 2; cp = ch & 0x0F; }
        else if ((ch & 0xF8) == 0xF0) { need = 3; cp = ch & 0x07; }
        else return 0;
        if (i + need >= len) return 0;
        for (k = 1; k <= need; k++) {
            if ((s[i + k] & 0xC0) != 0x80) return 0;
            cp = (cp << 6) | (s[i + k] & 0x3F);
        }
        if ((need == 1 && cp < 0x80) || (need == 2 && cp < 0x800) ||
            (need == 3 && (cp < 0x10000 || cp > 0x10FFFF)) ||
            (cp >= 0xD800 && cp <= 0xDFFF)) return 0;
        i += need + 1;
    }
    return 1;
}

static char *ducknng_json_quote(const char *text, size_t len, size_t *out_len) {
    static const char hex[] = "0123456789abcdef";
    size_t cap = len * 6 + 3;
    char *out = (char *)duckdb_malloc(cap);
    size_t n = 0;
    size_t i;
    if (!out) return NULL;
    out[n++] = '"';
    for (i = 0; i < len; i++) {
        unsigned char ch = (unsigned char)text[i];
        switch (ch) {
        case '"': out[n++] = '\\'; out[n++] = '"'; break;
        case '\\': out[n++] = '\\'; out[n++] = '\\'; break;
        case '\n': out[n++] = '\\'; out[n++] = 'n'; break;
        case '\r': out[n++] = '\\'; out[n++] = 'r'; break;
        case '\t': out[n++] = '\\'; out[n++] = 't'; break;
        default:
            if (ch < 0x20) {
                out[n++] = '\\'; out[n++] = 'u'; out[n++] = '0'; out[n++] = '0';
                out[n++] = hex[ch >> 4]; out[n++] = hex[ch & 0xF];
            } else {
                out[n++] = (char)ch;
            }
        }
    }
    out[n++] = '"';
    *out_len = n;
    return out;
}

static char *ducknng_sql_error_message(const char *prefix, const char *detail) {
    size_t need = strlen(prefix) + (detail ? strlen(detail) : 0) + 3;
    char *msg = (char *)duckdb_malloc(need);
    if (!msg) return NULL;
    snprintf(msg, need, "%s%s%s", prefix, detail && detail[0] ? ": " : "", detail ? detail : "");
    return msg;
}

static int ducknng_sql_method_run(duckdb_connection con, const char *handler_sql,
    const char *payload, duckdb_result *last, int *has_last, char **errmsg) {
    duckdb_extracted_statements extracted = NULL;
    duckdb_result tx;
    idx_t count;
    idx_t i;
    int in_transaction = 0;

    *has_last = 0;
    count = duckdb_extract_statements(con, handler_sql, &extracted);
    if (count == 0) {
        const char *detail = extracted ? duckdb_extract_statements_error(extracted) : NULL;
        *errmsg = ducknng_sql_error_message("ducknng: SQL method has no executable statement", detail);
        if (extracted) duckdb_destroy_extracted(&extracted);
        return -1;
    }
    memset(&tx, 0, sizeof(tx));
    if (duckdb_query(con, "BEGIN TRANSACTION", &tx) == DuckDBError) {
        *errmsg = ducknng_sql_error_message("ducknng: SQL method could not begin a transaction",
            duckdb_result_error(&tx));
        duckdb_destroy_result(&tx);
        duckdb_destroy_extracted(&extracted);
        return -1;
    }
    duckdb_destroy_result(&tx);
    in_transaction = 1;

    for (i = 0; i < count; i++) {
        duckdb_prepared_statement stmt = NULL;
        duckdb_result result;
        idx_t nparams;
        memset(&result, 0, sizeof(result));
        if (duckdb_prepare_extracted_statement(con, extracted, i, &stmt) == DuckDBError) {
            *errmsg = ducknng_sql_error_message("ducknng: SQL method statement failed to prepare",
                stmt ? duckdb_prepare_error(stmt) : NULL);
            if (stmt) duckdb_destroy_prepare(&stmt);
            goto fail;
        }
        nparams = duckdb_nparams(stmt);
        if (nparams > 1) {
            *errmsg = ducknng_strdup("ducknng: SQL method statements accept at most one parameter, the JSON payload");
            duckdb_destroy_prepare(&stmt);
            goto fail;
        }
        if (nparams == 1 && duckdb_bind_varchar(stmt, 1, payload) == DuckDBError) {
            *errmsg = ducknng_strdup("ducknng: failed to bind the SQL method payload");
            duckdb_destroy_prepare(&stmt);
            goto fail;
        }
        if (duckdb_execute_prepared(stmt, &result) == DuckDBError) {
            *errmsg = ducknng_sql_error_message("ducknng: SQL method failed", duckdb_result_error(&result));
            duckdb_destroy_result(&result);
            duckdb_destroy_prepare(&stmt);
            goto fail;
        }
        duckdb_destroy_prepare(&stmt);
        if (*has_last) duckdb_destroy_result(last);
        *last = result;
        *has_last = 1;
    }
    duckdb_destroy_extracted(&extracted);
    memset(&tx, 0, sizeof(tx));
    if (duckdb_query(con, "COMMIT", &tx) == DuckDBError) {
        *errmsg = ducknng_sql_error_message("ducknng: SQL method commit failed", duckdb_result_error(&tx));
        duckdb_destroy_result(&tx);
        in_transaction = 0;
        {
            duckdb_result rollback;
            memset(&rollback, 0, sizeof(rollback));
            duckdb_query(con, "ROLLBACK", &rollback);
            duckdb_destroy_result(&rollback);
        }
        if (*has_last) duckdb_destroy_result(last);
        *has_last = 0;
        return -1;
    }
    duckdb_destroy_result(&tx);
    return 0;

fail:
    if (extracted) duckdb_destroy_extracted(&extracted);
    if (in_transaction) {
        memset(&tx, 0, sizeof(tx));
        duckdb_query(con, "ROLLBACK", &tx);
        duckdb_destroy_result(&tx);
    }
    if (*has_last) duckdb_destroy_result(last);
    *has_last = 0;
    return -1;
}

/* Encodes the first column of the first row: JSON passes through, VARCHAR
 * becomes a JSON string, and no row or NULL becomes null. */
static int ducknng_sql_method_encode_reply(duckdb_result *result, uint8_t **out,
    size_t *out_len, char **errmsg) {
    duckdb_logical_type type;
    duckdb_type id;
    char *alias = NULL;
    int is_json = 0;
    duckdb_data_chunk chunk;
    duckdb_vector vec;
    uint64_t *validity;
    duckdb_string_t *data;
    const char *src;
    uint32_t len;

    if (duckdb_result_return_type(*result) != DUCKDB_RESULT_TYPE_QUERY_RESULT ||
        duckdb_column_count(result) == 0) {
        *errmsg = ducknng_strdup("ducknng: the last SQL method statement must return a JSON or VARCHAR column");
        return -1;
    }
    type = duckdb_column_logical_type(result, 0);
    id = duckdb_get_type_id(type);
    alias = duckdb_logical_type_get_alias(type);
    is_json = alias && strcmp(alias, "JSON") == 0;
    if (alias) duckdb_free(alias);
    duckdb_destroy_logical_type(&type);
    if (id != DUCKDB_TYPE_VARCHAR) {
        *errmsg = ducknng_strdup("ducknng: the SQL method result column must be JSON or VARCHAR; wrap other values with to_json()");
        return -1;
    }

    chunk = duckdb_fetch_chunk(*result);
    if (!chunk || duckdb_data_chunk_get_size(chunk) == 0) {
        if (chunk) duckdb_destroy_data_chunk(&chunk);
        *out = (uint8_t *)ducknng_strdup("null");
        *out_len = 4;
        return *out ? 0 : -1;
    }
    vec = duckdb_data_chunk_get_vector(chunk, 0);
    validity = duckdb_vector_get_validity(vec);
    if (validity && !duckdb_validity_row_is_valid(validity, 0)) {
        duckdb_destroy_data_chunk(&chunk);
        *out = (uint8_t *)ducknng_strdup("null");
        *out_len = 4;
        return *out ? 0 : -1;
    }
    data = (duckdb_string_t *)duckdb_vector_get_data(vec);
    src = duckdb_string_t_data(&data[0]);
    len = duckdb_string_t_length(data[0]);
    if (is_json) {
        char *copy = (char *)duckdb_malloc((size_t)len + 1);
        if (copy) {
            memcpy(copy, src, len);
            copy[len] = '\0';
        }
        *out = (uint8_t *)copy;
        *out_len = len;
    } else {
        *out = (uint8_t *)ducknng_json_quote(src, len, out_len);
    }
    duckdb_destroy_data_chunk(&chunk);
    if (!*out) {
        *errmsg = ducknng_strdup("ducknng: out of memory encoding the SQL method reply");
        return -1;
    }
    return 0;
}

static int ducknng_sql_method_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    const struct ducknng_sql_method *entry;
    ducknng_service_sql_scope scope;
    duckdb_result last;
    int has_last = 0;
    char *payload = NULL;
    size_t payload_len;
    uint8_t *body = NULL;
    size_t body_len = 0;
    char *errmsg = NULL;
    int rc;

    entry = method ? (const struct ducknng_sql_method *)method->handler_data : NULL;
    if (!svc || !svc->rt || !req || !req->frame || !reply || !entry || !entry->handler_sql) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing SQL method execution context");
        return -1;
    }
    payload_len = (size_t)req->frame->payload_len;
    if (payload_len == 0) {
        payload = ducknng_strdup("{}");
        payload_len = 2;
    } else {
        payload = (char *)duckdb_malloc(payload_len + 1);
        if (payload) {
            memcpy(payload, req->frame->payload, payload_len);
            payload[payload_len] = '\0';
        }
    }
    if (!payload) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: out of memory copying the SQL method payload");
        return -1;
    }
    if (!ducknng_utf8_valid((const unsigned char *)payload, payload_len) ||
        !ducknng_json_is_object(payload, payload_len)) {
        duckdb_free(payload);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: SQL method payload must be a UTF-8 JSON object");
        return -1;
    }

    memset(&last, 0, sizeof(last));
    if (ducknng_service_enter_request_sql(svc, &scope, &errmsg) != 0) {
        duckdb_free(payload);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            errmsg ? errmsg : "ducknng: missing execution context");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    g_sql_method_request = req;
    rc = ducknng_sql_method_run(scope.con, entry->handler_sql, payload, &last, &has_last, &errmsg);
    g_sql_method_request = NULL;
    if (rc != 0) {
        ducknng_service_leave_request_sql(&scope);
        duckdb_free(payload);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            errmsg ? errmsg : "ducknng: SQL method failed");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    ducknng_service_leave_request_sql(&scope);
    duckdb_free(payload);

    if (ducknng_sql_method_encode_reply(&last, &body, &body_len, &errmsg) != 0) {
        duckdb_destroy_result(&last);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            errmsg ? errmsg : "ducknng: failed to encode the SQL method reply");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    duckdb_destroy_result(&last);
    ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
        DUCKNNG_RPC_FLAG_PAYLOAD_JSON, body, body_len);
    return 0;
}

static int ducknng_sql_method_name_valid(const char *name) {
    const char *p;
    if (!name || !name[0] || strlen(name) > 128) return 0;
    for (p = name; *p; p++) {
        unsigned char ch = (unsigned char)*p;
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
              (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' || ch == '-')) return 0;
    }
    return 1;
}

int ducknng_runtime_register_sql_method(ducknng_runtime *rt, const char *name,
    const char *handler_sql, const char *request_schema_json, int requires_auth,
    char **errmsg) {
    struct ducknng_sql_method *entry;
    const ducknng_method_descriptor *existing;
    struct ducknng_sql_method *previous = NULL;
    int previous_auth = 0;

    if (!rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for SQL method registration");
        return 0;
    }
    if (!ducknng_sql_method_name_valid(name)) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: SQL method name must be 1 to 128 characters from [A-Za-z0-9_.-]");
        return 0;
    }
    if (!handler_sql || !handler_sql[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: SQL method handler_sql is required");
        return 0;
    }
    if (!request_schema_json ||
        !ducknng_utf8_valid((const unsigned char *)request_schema_json, strlen(request_schema_json)) ||
        !ducknng_json_is_object(request_schema_json, strlen(request_schema_json))) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: SQL method request_schema_json must be a JSON object");
        return 0;
    }
    existing = ducknng_method_registry_find(&rt->registry, (const uint8_t *)name, (uint32_t)strlen(name));
    if (existing) {
        if (existing->handler != ducknng_sql_method_handler) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: a built-in method with this name is already registered");
            return 0;
        }
        /* The registry slot may be an owned auth-policy copy that unregister
         * frees, so keep the stable entry and its effective auth flag. */
        previous = (struct ducknng_sql_method *)existing->handler_data;
        previous_auth = existing->requires_auth;
    }

    entry = (struct ducknng_sql_method *)duckdb_malloc(sizeof(*entry));
    if (!entry) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory registering SQL method");
        return 0;
    }
    memset(entry, 0, sizeof(*entry));
    entry->name = ducknng_strdup(name);
    entry->summary = ducknng_strdup("SQL-defined method");
    entry->request_schema_json = ducknng_strdup(request_schema_json);
    entry->handler_sql = ducknng_strdup(handler_sql);
    if (!entry->name || !entry->summary || !entry->request_schema_json || !entry->handler_sql) {
        if (entry->name) duckdb_free(entry->name);
        if (entry->summary) duckdb_free(entry->summary);
        if (entry->request_schema_json) duckdb_free(entry->request_schema_json);
        if (entry->handler_sql) duckdb_free(entry->handler_sql);
        duckdb_free(entry);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory registering SQL method");
        return 0;
    }
    entry->descriptor.name = entry->name;
    entry->descriptor.family = "sql_method";
    entry->descriptor.summary = entry->summary;
    entry->descriptor.transport_pattern = DUCKNNG_TRANSPORT_REQREP;
    entry->descriptor.request_payload_format = DUCKNNG_PAYLOAD_JSON;
    entry->descriptor.response_payload_format = DUCKNNG_PAYLOAD_JSON;
    entry->descriptor.response_mode = DUCKNNG_RESPONSE_METADATA_ONLY;
    entry->descriptor.session_behavior = DUCKNNG_SESSION_STATELESS;
    entry->descriptor.accepted_request_flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON;
    entry->descriptor.emitted_reply_flags = DUCKNNG_RPC_FLAG_PAYLOAD_JSON;
    entry->descriptor.max_request_bytes = DUCKNNG_SQL_METHOD_MAX_REQUEST_BYTES;
    entry->descriptor.max_reply_bytes = DUCKNNG_SQL_METHOD_MAX_REPLY_BYTES;
    entry->descriptor.requires_auth = requires_auth ? 1 : 0;
    entry->descriptor.mutates_state = 1;
    entry->descriptor.version_introduced = 1;
    entry->descriptor.request_schema_json = entry->request_schema_json;
    entry->descriptor.response_schema_json = ducknng_sql_method_response_schema;
    entry->descriptor.handler = ducknng_sql_method_handler;
    entry->descriptor.handler_data = entry;

    if (previous) ducknng_method_registry_unregister(&rt->registry, name);
    if (!ducknng_method_registry_register(&rt->registry, &entry->descriptor, errmsg)) {
        /* The replaced entry stays in rt->sql_methods, so it can be restored
         * with its previous auth policy; only this new entry is released. */
        if (previous) {
            char *restore_err = NULL;
            if (ducknng_method_registry_register(&rt->registry, &previous->descriptor, &restore_err) &&
                previous_auth != previous->descriptor.requires_auth) {
                ducknng_method_registry_set_requires_auth(&rt->registry, name, previous_auth, &restore_err);
            }
            if (restore_err) duckdb_free(restore_err);
        }
        duckdb_free(entry->name);
        duckdb_free(entry->summary);
        duckdb_free(entry->request_schema_json);
        duckdb_free(entry->handler_sql);
        duckdb_free(entry);
        return 0;
    }
    entry->next = rt->sql_methods;
    rt->sql_methods = entry;
    return 1;
}

void ducknng_runtime_sql_methods_destroy(ducknng_runtime *rt) {
    struct ducknng_sql_method *entry;
    if (!rt) return;
    entry = rt->sql_methods;
    while (entry) {
        struct ducknng_sql_method *next = entry->next;
        duckdb_free(entry->name);
        duckdb_free(entry->summary);
        duckdb_free(entry->request_schema_json);
        duckdb_free(entry->handler_sql);
        duckdb_free(entry);
        entry = next;
    }
    rt->sql_methods = NULL;
}
