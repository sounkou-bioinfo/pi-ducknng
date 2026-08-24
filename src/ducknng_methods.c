#include "ducknng_manifest.h"
#include "ducknng_duckdb_streaming_compat.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_quack.h"
#include "ducknng_service.h"
#include "ducknng_session.h"
#include "ducknng_runtime.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_method_manifest_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    char *json = NULL;
    size_t payload_len;
    uint8_t *payload;
    char *errmsg = NULL;
    (void)method;
    if (!svc || !svc->rt || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing runtime for manifest request");
        return -1;
    }
    if (req && req->frame && req->frame->payload_len != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: manifest request does not accept a payload");
        return -1;
    }
    {
        ducknng_manifest_security security;
        ducknng_service_manifest_security(svc, &security);
        ducknng_mutex_lock(&svc->rt->mu);
        json = ducknng_method_registry_manifest_json(&svc->rt->registry, "ducknng", DUCKNNG_VERSION,
            DUCKNNG_WIRE_VERSION, &security, &errmsg);
        ducknng_mutex_unlock(&svc->rt->mu);
    }
    if (!json) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            errmsg ? errmsg : "ducknng: failed to generate manifest json");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    payload_len = strlen(json);
    payload = (uint8_t *)duckdb_malloc(payload_len);
    if (!payload) {
        duckdb_free(json);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: out of memory preparing manifest reply");
        return -1;
    }
    memcpy(payload, json, payload_len);
    duckdb_free(json);
    ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
        DUCKNNG_RPC_FLAG_PAYLOAD_JSON, payload, payload_len);
    return 0;
}

static char *ducknng_copy_payload_json(const ducknng_request_context *req) {
    char *json;
    if (!req || !req->frame || !req->frame->payload || req->frame->payload_len == 0) return NULL;
    json = (char *)duckdb_malloc((size_t)req->frame->payload_len + 1);
    if (!json) return NULL;
    memcpy(json, req->frame->payload, (size_t)req->frame->payload_len);
    json[req->frame->payload_len] = '\0';
    return json;
}

static const char *ducknng_json_find_key(const char *json, const char *key) {
    char needle[128];
    const char *p;
    if (!json || !key) return NULL;
    if (snprintf(needle, sizeof(needle), "\"%s\"", key) >= (int)sizeof(needle)) return NULL;
    p = strstr(json, needle);
    if (!p) return NULL;
    p = strchr(p + strlen(needle), ':');
    if (!p) return NULL;
    p++;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

static int ducknng_json_extract_u64(const char *json, const char *key, uint64_t *out) {
    const char *p = ducknng_json_find_key(json, key);
    char *end = NULL;
    if (out) *out = 0;
    if (!p || !out) return -1;
    if (*p == '"') p++;
    *out = (uint64_t)strtoull(p, &end, 10);
    return end == p ? -1 : 0;
}

static char *ducknng_json_extract_string_dup(const char *json, const char *key) {
    const char *p = ducknng_json_find_key(json, key);
    const char *end;
    char *out;
    size_t len;
    if (!p || *p != '"') return NULL;
    p++;
    end = strchr(p, '"');
    if (!end) return NULL;
    len = (size_t)(end - p);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, p, len);
    out[len] = '\0';
    return out;
}

static int ducknng_json_reply(ducknng_method_reply *reply, const char *method_name, uint32_t flags,
    const char *json_text) {
    size_t payload_len;
    uint8_t *payload;
    (void)method_name;
    if (!reply || !json_text) return -1;
    payload_len = strlen(json_text);
    payload = (uint8_t *)duckdb_malloc(payload_len);
    if (!payload) return -1;
    memcpy(payload, json_text, payload_len);
    return ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
        flags | DUCKNNG_RPC_FLAG_PAYLOAD_JSON, payload, payload_len) ? 0 : -1;
}

static int ducknng_json_append_text(char **buf, size_t *len, size_t *cap, const char *text) {
    size_t add = text ? strlen(text) : 0;
    char *next;
    size_t want;
    if (!text || add == 0) return 1;
    want = *len + add + 1;
    if (*cap >= want) {
        memcpy(*buf + *len, text, add + 1);
        *len += add;
        return 1;
    }
    *cap = *cap ? *cap * 2 : 256;
    while (*cap < want) *cap *= 2;
    next = (char *)duckdb_malloc(*cap);
    if (!next) return 0;
    if (*buf && *len) memcpy(next, *buf, *len);
    if (*buf) duckdb_free(*buf);
    *buf = next;
    memcpy(*buf + *len, text, add + 1);
    *len += add;
    return 1;
}

static int ducknng_json_append_string(char **buf, size_t *len, size_t *cap, const char *text) {
    const char *p = text ? text : "";
    if (!ducknng_json_append_text(buf, len, cap, "\"")) return 0;
    while (*p) {
        char esc[3] = {0, 0, 0};
        if (*p == '\\' || *p == '"') {
            esc[0] = '\\';
            esc[1] = *p;
            if (!ducknng_json_append_text(buf, len, cap, esc)) return 0;
        } else if ((unsigned char)*p < 0x20) {
            char hex[7];
            snprintf(hex, sizeof(hex), "\\u%04x", (unsigned int)(unsigned char)*p);
            if (!ducknng_json_append_text(buf, len, cap, hex)) return 0;
        } else {
            char one[2] = {*p, 0};
            if (!ducknng_json_append_text(buf, len, cap, one)) return 0;
        }
        p++;
    }
    return ducknng_json_append_text(buf, len, cap, "\"");
}

typedef struct ducknng_session_reply_view {
    uint64_t session_id;
    const char *session_token;
    const char *state;
    const char *next_method;
    int has_idle_timeout_ms;
    uint64_t idle_timeout_ms;
    const char *result_handle;
    int has_batch_index;
    uint64_t batch_index;
    int has_protocol_version;
    uint64_t protocol_version;
    int has_row_schema_version;
    uint64_t row_schema_version;
    int has_fetch_batch_chunks;
    uint64_t fetch_batch_chunks;
    const char *serialization_mode;
    const char *correlation_id;
} ducknng_session_reply_view;

static const char *ducknng_session_row_payload_format_name(int value) {
    switch (value) {
    case DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH:
        return "ducknng_quack_batch";
    case DUCKNNG_PAYLOAD_ARROW_IPC_STREAM:
    default:
        return "arrow_ipc_stream";
    }
}

static int ducknng_parse_row_payload_format(const char *value, int *out) {
    if (out) *out = DUCKNNG_PAYLOAD_ARROW_IPC_STREAM;
    if (!value || !value[0] || strcmp(value, "arrow_ipc_stream") == 0) return 0;
    if (strcmp(value, "ducknng_quack_batch") == 0) {
        if (out) *out = DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH;
        return 0;
    }
    return -1;
}

static const char *ducknng_server_platform_name(void) {
#if defined(_WIN32)
    return "windows";
#elif defined(__APPLE__)
    return "macos";
#elif defined(__linux__)
    return "linux";
#else
    return "unknown";
#endif
}

static char *ducknng_build_session_reply_json(const ducknng_session_reply_view *view) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    char numbuf[64];
    if (!view) return NULL;
    if (!ducknng_json_append_text(&buf, &len, &cap, "{\"session_id\":")) goto oom;
    snprintf(numbuf, sizeof(numbuf), "%llu", (unsigned long long)view->session_id);
    if (!ducknng_json_append_text(&buf, &len, &cap, numbuf)) goto oom;
    if (view->session_token) {
        if (!ducknng_json_append_text(&buf, &len, &cap, ",\"session_token\":")) goto oom;
        if (!ducknng_json_append_string(&buf, &len, &cap, view->session_token)) goto oom;
    }
    if (view->state) {
        if (!ducknng_json_append_text(&buf, &len, &cap, ",\"state\":")) goto oom;
        if (!ducknng_json_append_string(&buf, &len, &cap, view->state)) goto oom;
    }
    if (view->next_method) {
        if (!ducknng_json_append_text(&buf, &len, &cap, ",\"next_method\":")) goto oom;
        if (!ducknng_json_append_string(&buf, &len, &cap, view->next_method)) goto oom;
    }
    if (view->has_idle_timeout_ms) {
        snprintf(numbuf, sizeof(numbuf), ",\"idle_timeout_ms\":%llu",
            (unsigned long long)view->idle_timeout_ms);
        if (!ducknng_json_append_text(&buf, &len, &cap, numbuf)) goto oom;
    }
    if (view->result_handle) {
        if (!ducknng_json_append_text(&buf, &len, &cap, ",\"result_handle\":")) goto oom;
        if (!ducknng_json_append_string(&buf, &len, &cap, view->result_handle)) goto oom;
    }
    if (view->has_batch_index) {
        snprintf(numbuf, sizeof(numbuf), ",\"batch_index\":%llu",
            (unsigned long long)view->batch_index);
        if (!ducknng_json_append_text(&buf, &len, &cap, numbuf)) goto oom;
    }
    if (view->has_protocol_version) {
        snprintf(numbuf, sizeof(numbuf), ",\"ducknng_protocol_version\":%llu",
            (unsigned long long)view->protocol_version);
        if (!ducknng_json_append_text(&buf, &len, &cap, numbuf)) goto oom;
    }
    if (view->has_row_schema_version) {
        snprintf(numbuf, sizeof(numbuf), ",\"row_schema_version\":%llu",
            (unsigned long long)view->row_schema_version);
        if (!ducknng_json_append_text(&buf, &len, &cap, numbuf)) goto oom;
    }
    if (view->has_fetch_batch_chunks) {
        snprintf(numbuf, sizeof(numbuf), ",\"fetch_batch_chunks\":%llu",
            (unsigned long long)view->fetch_batch_chunks);
        if (!ducknng_json_append_text(&buf, &len, &cap, numbuf)) goto oom;
    }
    if (view->serialization_mode) {
        if (!ducknng_json_append_text(&buf, &len, &cap, ",\"serialization_mode\":")) goto oom;
        if (!ducknng_json_append_string(&buf, &len, &cap, view->serialization_mode)) goto oom;
    }
    if (view->correlation_id) {
        if (!ducknng_json_append_text(&buf, &len, &cap, ",\"correlation_id\":")) goto oom;
        if (!ducknng_json_append_string(&buf, &len, &cap, view->correlation_id)) goto oom;
    }
    if (!ducknng_json_append_text(&buf, &len, &cap, "}")) goto oom;
    return buf;
oom:
    if (buf) duckdb_free(buf);
    return NULL;
}

static int ducknng_session_state_reply(ducknng_method_reply *reply, const char *method_name,
    uint32_t flags, const ducknng_session_reply_view *view) {
    char *control;
    int rc;
    if (!reply || !method_name || !view) return -1;
    control = ducknng_build_session_reply_json(view);
    if (!control) return -1;
    rc = ducknng_json_reply(reply, method_name, flags, control);
    duckdb_free(control);
    return rc;
}

typedef struct ducknng_session_control_request {
    uint64_t session_id;
    char *session_token;
    char *correlation_id;
} ducknng_session_control_request;

static void ducknng_session_control_request_reset(ducknng_session_control_request *request) {
    if (!request) return;
    if (request->session_token) duckdb_free(request->session_token);
    if (request->correlation_id) duckdb_free(request->correlation_id);
    memset(request, 0, sizeof(*request));
}

static int ducknng_parse_session_control_request(const ducknng_request_context *req,
    const char *method_name, ducknng_session_control_request *request,
    ducknng_method_reply *reply) {
    char *json;
    char errmsg[160];
    if (!request) return -1;
    ducknng_session_control_request_reset(request);
    if (!req || !reply || !method_name) return -1;
    json = ducknng_copy_payload_json(req);
    if (!json || ducknng_json_extract_u64(json, "session_id", &request->session_id) != 0) {
        if (json) duckdb_free(json);
        snprintf(errmsg, sizeof(errmsg),
            "ducknng: %s requires JSON payload with session_id and session_token", method_name);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID, errmsg);
        return -1;
    }
    request->session_token = ducknng_json_extract_string_dup(json, "session_token");
    request->correlation_id = ducknng_json_extract_string_dup(json, "correlation_id");
    duckdb_free(json);
    if (!request->session_token || !request->session_token[0]) {
        snprintf(errmsg, sizeof(errmsg),
            "ducknng: %s requires JSON payload with session_id and session_token", method_name);
        ducknng_session_control_request_reset(request);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID, errmsg);
        return -1;
    }
    return 0;
}

static const char *ducknng_session_auth_error_message(int auth) {
    return auth == DUCKNNG_SESSION_AUTH_IDENTITY_MISMATCH ?
        "ducknng: session owner identity mismatch" :
        "ducknng: session owner token mismatch";
}

/* Non-destructively verify a (session_id, token) names a session of the
 * expected family (upload vs query) owned by this caller, before a
 * destructive remove. Query controls (fetch/close/cancel) and upload controls
 * (upload_append/commit/abort) share one session table, so each family must
 * reject the other's sessions rather than detach and destroy a live handle of
 * the wrong kind. Sets an error reply and returns -1 on missing, unauthorized,
 * or wrong-family; the session is left intact in every case. */
static int ducknng_session_check_kind(ducknng_service *svc, uint64_t session_id,
    const char *session_token, const char *caller_identity, int require_upload,
    ducknng_method_reply *reply) {
    int unauthorized = 0;
    int is_upload;
    ducknng_session *session = ducknng_service_acquire_session(svc, session_id,
        session_token, caller_identity, &unauthorized);
    if (!session) {
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: session not found");
        return -1;
    }
    ducknng_mutex_lock(&session->mu);
    is_upload = session->is_upload;
    ducknng_mutex_unlock(&session->mu);
    ducknng_session_release(session);
    if (require_upload && !is_upload) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID, "ducknng: session is not an upload session");
        return -1;
    }
    if (!require_upload && is_upload) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: session is an upload session; use the upload_* methods");
        return -1;
    }
    return 0;
}

static int ducknng_bind_sql_parameters(duckdb_prepared_statement stmt,
    duckdb_value *parameters, idx_t parameter_count, char **errmsg) {
    idx_t expected;
    idx_t i;
    char detail[192];
    if (!stmt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing prepared statement parameter context");
        return -1;
    }
    expected = duckdb_nparams(stmt);
    if (expected != parameter_count) {
        snprintf(detail, sizeof(detail),
            "ducknng: SQL expects %llu parameter(s), request supplied %llu",
            (unsigned long long)expected,
            (unsigned long long)parameter_count);
        if (errmsg) *errmsg = ducknng_strdup(detail);
        return -1;
    }
    for (i = 0; i < expected; i++) {
        if (!parameters || !parameters[i] ||
            duckdb_bind_value(stmt, i + 1, parameters[i]) == DuckDBError) {
            snprintf(detail, sizeof(detail),
                "ducknng: failed to bind SQL parameter %llu",
                (unsigned long long)(i + 1));
            if (errmsg) *errmsg = ducknng_strdup(detail);
            return -1;
        }
    }
    return 0;
}

static int ducknng_bind_query_parameters(duckdb_prepared_statement stmt,
    const ducknng_query_open_request *request, char **errmsg) {
    if (!request) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: missing prepared statement parameter context");
        return -1;
    }
    return ducknng_bind_sql_parameters(stmt, request->parameters,
        request->parameter_count, errmsg);
}

static int ducknng_method_handshake_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    char *json = NULL;
    char *correlation_id = NULL;
    char *preferred_mode = NULL;
    char *response = NULL;
    uint64_t min_protocol_version = DUCKNNG_WIRE_VERSION;
    uint64_t max_protocol_version = DUCKNNG_WIRE_VERSION;
    int row_payload_format = DUCKNNG_PAYLOAD_ARROW_IPC_STREAM;
    char *duckdb_version = NULL;
    char *errmsg = NULL;
    size_t len = 0;
    size_t cap = 0;
    size_t i;
    char numbuf[160];
    static const char *parameter_methods[] = {"exec", "query_open", "query_prepare"};
    (void)method;
    if (!svc || !svc->rt || !reply) return -1;
    if (req && req->frame && req->frame->payload_len > 0) {
        json = ducknng_copy_payload_json(req);
        if (!json) {
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
                "ducknng: out of memory copying handshake payload");
            return -1;
        }
        if (ducknng_json_extract_u64(json, "min_protocol_version", &min_protocol_version) != 0) {
            min_protocol_version = DUCKNNG_WIRE_VERSION;
        }
        if (ducknng_json_extract_u64(json, "max_protocol_version", &max_protocol_version) != 0) {
            max_protocol_version = DUCKNNG_WIRE_VERSION;
        }
        correlation_id = ducknng_json_extract_string_dup(json, "correlation_id");
        preferred_mode = ducknng_json_extract_string_dup(json, "preferred_serialization_mode");
    }
    if (min_protocol_version > DUCKNNG_WIRE_VERSION || max_protocol_version < DUCKNNG_WIRE_VERSION) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: handshake protocol version range does not include the current wire version");
        goto done;
    }
    if (preferred_mode && ducknng_parse_row_payload_format(preferred_mode, &row_payload_format) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: unsupported preferred_serialization_mode");
        goto done;
    }
    duckdb_version = ducknng_strdup(duckdb_library_version());
    if (!duckdb_version) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: out of memory preparing handshake reply");
        goto done;
    }
    if (!ducknng_json_append_text(&response, &len, &cap, "{")) goto oom;
    if (!ducknng_json_append_text(&response, &len, &cap, "\"server_name\":")) goto oom;
    if (!ducknng_json_append_string(&response, &len, &cap, "ducknng")) goto oom;
    if (!ducknng_json_append_text(&response, &len, &cap, ",\"server_version\":")) goto oom;
    if (!ducknng_json_append_string(&response, &len, &cap, DUCKNNG_VERSION)) goto oom;
    if (!ducknng_json_append_text(&response, &len, &cap, ",\"duckdb_version\":")) goto oom;
    if (!ducknng_json_append_string(&response, &len, &cap, duckdb_version)) goto oom;
    if (!ducknng_json_append_text(&response, &len, &cap, ",\"server_platform\":")) goto oom;
    if (!ducknng_json_append_string(&response, &len, &cap, ducknng_server_platform_name())) goto oom;
    snprintf(numbuf, sizeof(numbuf), ",\"protocol_version\":%u,\"selected_protocol_version\":%u,\"ducknng_protocol_version\":1,\"row_schema_version\":1,\"default_fetch_batch_chunks\":%u",
        (unsigned int)DUCKNNG_WIRE_VERSION, (unsigned int)DUCKNNG_WIRE_VERSION,
        (unsigned int)DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS);
    if (!ducknng_json_append_text(&response, &len, &cap, numbuf)) goto oom;
    if (!ducknng_json_append_text(&response, &len, &cap, ",\"selected_serialization_mode\":")) goto oom;
    if (!ducknng_json_append_string(&response, &len, &cap,
            ducknng_session_row_payload_format_name(row_payload_format))) goto oom;
    if (!ducknng_json_append_text(&response, &len, &cap,
            ",\"supported_serialization_modes\":[\"arrow_ipc_stream\",\"ducknng_quack_batch\"],\"capabilities\":{\"fetch_metadata\":{\"correlation_id\":true,\"result_handle\":true,\"batch_index\":true},\"session_schema\":{\"ducknng_protocol_version\":true,\"row_schema_version\":true,\"fetch_batch_chunks\":true},\"parameter_binding\":{\"encoding\":\"arrow_struct\",\"positional\":true,\"methods\":[")) goto oom;
    {
        int emitted = 0;
        int append_ok = 1;
        ducknng_mutex_lock(&svc->rt->mu);
        for (i = 0; i < sizeof(parameter_methods) / sizeof(parameter_methods[0]); i++) {
            const char *name = parameter_methods[i];
            if (!ducknng_method_registry_find(&svc->rt->registry,
                    (const uint8_t *)name, (uint32_t)strlen(name))) continue;
            if (emitted && !ducknng_json_append_text(&response, &len, &cap, ",")) {
                append_ok = 0;
                break;
            }
            if (!ducknng_json_append_string(&response, &len, &cap, name)) {
                append_ok = 0;
                break;
            }
            emitted = 1;
        }
        ducknng_mutex_unlock(&svc->rt->mu);
        if (!append_ok) goto oom;
    }
    if (!ducknng_json_append_text(&response, &len, &cap,
            "],\"max_parameters\":65535}}")) goto oom;
    if (correlation_id) {
        if (!ducknng_json_append_text(&response, &len, &cap, ",\"correlation_id\":")) goto oom;
        if (!ducknng_json_append_string(&response, &len, &cap, correlation_id)) goto oom;
    }
    if (!ducknng_json_append_text(&response, &len, &cap, "}")) goto oom;
    if (ducknng_json_reply(reply, "handshake", 0, response) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: failed to build handshake reply");
    }
    goto done;
oom:
    errmsg = ducknng_strdup("ducknng: out of memory preparing handshake reply");
    ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, errmsg ? errmsg : "ducknng: out of memory preparing handshake reply");
done:
    if (errmsg) duckdb_free(errmsg);
    if (response) duckdb_free(response);
    if (json) duckdb_free(json);
    if (correlation_id) duckdb_free(correlation_id);
    if (preferred_mode) duckdb_free(preferred_mode);
    if (duckdb_version) duckdb_free(duckdb_version);
    return reply->type == DUCKNNG_RPC_ERROR ? -1 : 0;
}

static int ducknng_method_query_open_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_query_open_request open_req;
    duckdb_connection session_con = NULL;
    size_t session_pool_index = (size_t)-1;
    duckdb_prepared_statement stmt = NULL;
    duckdb_pending_result pending = NULL;
    uint64_t session_id = 0;
    int row_payload_format = DUCKNNG_PAYLOAD_ARROW_IPC_STREAM;
    uint64_t fetch_batch_chunks = DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS;
    char *owner_token = NULL;
    char *result_handle = NULL;
    char *errmsg = NULL;
    (void)method;
    memset(&open_req, 0, sizeof(open_req));
    if (!svc || !svc->rt || !req || !req->frame || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: missing query_open execution context");
        return -1;
    }
    if (ducknng_decode_query_open_payload(req->frame->payload, (size_t)req->frame->payload_len, &open_req, &errmsg) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID, errmsg ? errmsg : "ducknng: invalid query_open payload");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    if (ducknng_parse_row_payload_format(open_req.serialization_mode, &row_payload_format) != 0) {
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: unsupported serialization_mode in query_open request");
        return -1;
    }
    if (open_req.batch_rows > 0) {
        uint64_t vector_rows = (uint64_t)duckdb_vector_size();
        fetch_batch_chunks = (open_req.batch_rows + vector_rows - 1) / vector_rows;
        if (fetch_batch_chunks == 0) fetch_batch_chunks = DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS;
        if (fetch_batch_chunks > DUCKNNG_MAX_FETCH_BATCH_CHUNKS) fetch_batch_chunks = DUCKNNG_MAX_FETCH_BATCH_CHUNKS;
    }
    if (ducknng_service_acquire_session_connection(svc, &session_con, &session_pool_index, &errmsg) != 0) {
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            errmsg ? errmsg : "ducknng: no connection available for query session");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    /* set service context so table-function bind callbacks (e.g. ducknng_parse_body) detect
     * service-owned SQL during the duckdb_prepare bind phase */
    if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, svc);
    {
        /* Extract statements so multi-statement SQL (e.g. SET x=y; SELECT ...) is handled
         * correctly: duckdb_prepare rejects strings with more than one statement.
         * We execute all but the last statement eagerly on session_con, then prepare+pending
         * the final statement. */
        duckdb_extracted_statements extracted = NULL;
        idx_t n_stmts = duckdb_extract_statements(session_con, open_req.sql, &extracted);
        if (n_stmts == 0) {
            /* extraction error or empty SQL */
            const char *ext_err = extracted ? duckdb_extract_statements_error(extracted) : NULL;
            char *detail = ducknng_strdup(ext_err ? ext_err : "ducknng: query_open failed to extract statements");
            if (extracted) duckdb_destroy_extracted(&extracted);
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_service_release_session_connection(svc, session_pool_index);
            ducknng_query_open_request_destroy(&open_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                detail && detail[0] ? detail : "ducknng: query_open: empty or invalid SQL");
            if (detail) duckdb_free(detail);
            return -1;
        }
        if (open_req.parameter_count > 0 && n_stmts != 1) {
            duckdb_destroy_extracted(&extracted);
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_service_release_session_connection(svc, session_pool_index);
            ducknng_query_open_request_destroy(&open_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
                "ducknng: parameterized query_open requires exactly one SQL statement");
            return -1;
        }
        /* Execute leading statements (all but last) eagerly */
        {
            idx_t i;
            for (i = 0; i + 1 < n_stmts; i++) {
                duckdb_prepared_statement lead_stmt = NULL;
                duckdb_result lead_result;
                memset(&lead_result, 0, sizeof(lead_result));
                if (duckdb_prepare_extracted_statement(session_con, extracted, i, &lead_stmt) == DuckDBError) {
                    char *detail = ducknng_strdup(lead_stmt ? duckdb_prepare_error(lead_stmt) : NULL);
                    if (lead_stmt) duckdb_destroy_prepare(&lead_stmt);
                    duckdb_destroy_extracted(&extracted);
                    if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
                    ducknng_service_release_session_connection(svc, session_pool_index);
                    ducknng_query_open_request_destroy(&open_req);
                    ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                        detail && detail[0] ? detail : "ducknng: query_open: failed to prepare leading statement");
                    if (detail) duckdb_free(detail);
                    return -1;
                }
                if (duckdb_execute_prepared(lead_stmt, &lead_result) == DuckDBError) {
                    char *detail = ducknng_strdup(duckdb_result_error(&lead_result));
                    duckdb_destroy_result(&lead_result);
                    duckdb_destroy_prepare(&lead_stmt);
                    duckdb_destroy_extracted(&extracted);
                    if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
                    ducknng_service_release_session_connection(svc, session_pool_index);
                    ducknng_query_open_request_destroy(&open_req);
                    ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                        detail && detail[0] ? detail : "ducknng: query_open: failed to execute leading statement");
                    if (detail) duckdb_free(detail);
                    return -1;
                }
                duckdb_destroy_result(&lead_result);
                duckdb_destroy_prepare(&lead_stmt);
            }
        }
        /* Prepare the final statement */
        if (duckdb_prepare_extracted_statement(session_con, extracted, n_stmts - 1, &stmt) == DuckDBError) {
            char *detail = ducknng_strdup(stmt ? duckdb_prepare_error(stmt) : NULL);
            if (stmt) duckdb_destroy_prepare(&stmt);
            stmt = NULL;
            duckdb_destroy_extracted(&extracted);
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_service_release_session_connection(svc, session_pool_index);
            ducknng_query_open_request_destroy(&open_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                detail && detail[0] ? detail : "ducknng: query_open prepare failed");
            if (detail) duckdb_free(detail);
            return -1;
        }
        duckdb_destroy_extracted(&extracted);
    }
    if (ducknng_bind_query_parameters(stmt, &open_req, &errmsg) != 0) {
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        duckdb_destroy_prepare(&stmt);
        ducknng_service_release_session_connection(svc, session_pool_index);
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            errmsg ? errmsg : "ducknng: failed to bind query parameters");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    if (ducknng_pending_prepared_for_session(stmt, &pending) == DuckDBError) {
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        if (pending) duckdb_destroy_pending(&pending);
        duckdb_destroy_prepare(&stmt);
        ducknng_service_release_session_connection(svc, session_pool_index);
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            "ducknng: query_open failed to start streaming execution");
        return -1;
    }
    if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
    {
        int add_rc = ducknng_service_add_session_streaming(svc, session_con, session_pool_index,
            stmt, pending, req->caller_identity, row_payload_format, fetch_batch_chunks,
            &session_id, &owner_token, &result_handle, &errmsg);
        if (add_rc != 0) {
            /* session was not created; release resources here */
            duckdb_destroy_pending(&pending);
            duckdb_destroy_prepare(&stmt);
            ducknng_service_release_session_connection(svc, session_pool_index);
            ducknng_query_open_request_destroy(&open_req);
            ducknng_method_reply_set_error(reply,
                add_rc > 0 ? DUCKNNG_STATUS_BUSY : DUCKNNG_STATUS_INTERNAL,
                errmsg ? errmsg : "ducknng: failed to register query session");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
    }
    {
        ducknng_session_reply_view view;
        memset(&view, 0, sizeof(view));
        view.session_id = session_id;
        view.session_token = owner_token;
        view.state = "open";
        view.next_method = "fetch";
        view.has_idle_timeout_ms = 1;
        view.idle_timeout_ms = svc->session_idle_ms;
        view.result_handle = result_handle;
        view.has_protocol_version = 1;
        view.protocol_version = 1;
        view.has_row_schema_version = 1;
        view.row_schema_version = 1;
        view.has_fetch_batch_chunks = 1;
        view.fetch_batch_chunks = fetch_batch_chunks;
        view.serialization_mode = ducknng_session_row_payload_format_name(row_payload_format);
        view.correlation_id = open_req.correlation_id;
        if (ducknng_session_state_reply(reply, "query_open", DUCKNNG_RPC_FLAG_SESSION_OPEN, &view) != 0) {
            ducknng_session *session = ducknng_service_remove_session(svc, session_id, owner_token,
                req->caller_identity, NULL);
            if (session) ducknng_session_destroy(session);
            ducknng_query_open_request_destroy(&open_req);
            if (owner_token) duckdb_free(owner_token);
            if (result_handle) duckdb_free(result_handle);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: failed to build query_open reply");
            return -1;
        }
    }
    ducknng_query_open_request_destroy(&open_req);
    if (owner_token) duckdb_free(owner_token);
    if (result_handle) duckdb_free(result_handle);
    return 0;
}

/* Assign one fetch row reply for the negotiated payload format. `extra_flags`
 * adds end-of-stream on the terminal batch; everything else is identical
 * between the batch and empty-result paths, which previously carried two
 * copies of this branch. Takes ownership of prebuilt_msg on the Quack path. */
static int ducknng_fetch_set_row_reply(ducknng_method_reply *reply,
    int row_payload_format, uint32_t extra_flags, nng_msg *prebuilt_msg,
    size_t reply_prefix_size, uint8_t *payload, size_t payload_len) {
    if (row_payload_format == DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH) {
        if (!ducknng_method_reply_set_prebuilt_message(reply,
                DUCKNNG_RPC_RESULT,
                DUCKNNG_RPC_FLAG_RESULT_ROWS |
                    DUCKNNG_RPC_FLAG_PAYLOAD_QUACK_BATCH | extra_flags,
                prebuilt_msg, reply_prefix_size, payload_len)) {
            nng_msg_free(prebuilt_msg);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
                "ducknng: failed to assign prebuilt fetch reply");
            return -1;
        }
        return 0;
    }
    ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
        DUCKNNG_RPC_FLAG_RESULT_ROWS |
            DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM | extra_flags,
        payload, payload_len);
    return 0;
}

static int ducknng_method_fetch_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    ducknng_session_reply_view view;
    uint64_t session_id = 0;
    uint64_t batch_index = 0;
    int row_payload_format = DUCKNNG_PAYLOAD_ARROW_IPC_STREAM;
    int unauthorized = 0;
    ducknng_session *session;
    uint8_t *payload = NULL;
    nng_msg *prebuilt_msg = NULL;
    size_t payload_len = 0;
    size_t reply_prefix_size = 0;
    int has_batch = 0;
    char *result_handle = NULL;
    char *errmsg = NULL;
    if (!svc || !method || !req || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: missing fetch context");
        return -1;
    }
    memset(&control_req, 0, sizeof(control_req));
    memset(&view, 0, sizeof(view));
    if (ducknng_size_add(DUCKNNG_WIRE_HEADER_LEN, strlen(method->name),
            &reply_prefix_size) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: fetch reply prefix size overflows");
        return -1;
    }
    if (ducknng_parse_session_control_request(req, "fetch", &control_req, reply) != 0) {
        return -1;
    }
    session_id = control_req.session_id;
    session = ducknng_service_acquire_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    if (!session) {
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: session not found");
        return -1;
    }
    /* fetch is a query-family control; reject an upload session (is_upload is
     * set once before the session is published, so this read is race-free). */
    if (session->is_upload) {
        ducknng_session_release(session);
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: session is an upload session; use the upload_* methods");
        return -1;
    }
    ducknng_mutex_lock(&session->mu);
    session->last_touch_ms = ducknng_now_ms();
    if (session->cancelled) {
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_CANCELLED, "ducknng: session was cancelled");
        return -1;
    }
    /* drive pending execution to READY on first fetch call */
    if (!session->pending_ready && session->pending_open) {
        duckdb_pending_state pstate;
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, svc);
        do {
            pstate = duckdb_pending_execute_task(session->session_pending);
        } while (pstate == DUCKDB_PENDING_RESULT_NOT_READY || pstate == DUCKDB_PENDING_NO_TASKS_AVAILABLE);
        if (pstate == DUCKDB_PENDING_ERROR) {
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_release(session);
            ducknng_session_control_request_reset(&control_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR, "ducknng: fetch execution failed");
            return -1;
        }
        if (duckdb_execute_pending(session->session_pending, &session->result) == DuckDBError) {
            char *detail = ducknng_strdup(duckdb_result_error(&session->result));
            duckdb_destroy_result(&session->result);
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_release(session);
            ducknng_session_control_request_reset(&control_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                detail && detail[0] ? detail : "ducknng: fetch failed to obtain streaming result");
            if (detail) duckdb_free(detail);
            return -1;
        }
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        session->result_open = 1;
        session->pending_ready = 1;
        /* pending result consumed; mark closed so destroy doesn't double-free */
        session->pending_open = 0;
    }
    row_payload_format = session->row_payload_format;
    if (!session->result_open ||
        (row_payload_format == DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH
            ? ducknng_result_next_chunks_to_quack_message(session->result,
                session->fetch_batch_chunks, !session->row_schema_sent,
                reply_prefix_size, &prebuilt_msg, &payload_len, &has_batch,
                &errmsg)
            : ducknng_result_next_chunks_to_ipc(session->result,
                session->fetch_batch_chunks, &payload, &payload_len, &has_batch, &errmsg)) != 0) {
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            errmsg ? errmsg : "ducknng: fetch failed to encode row batch");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    if (!has_batch && session->result_handle) {
        result_handle = ducknng_strdup(session->result_handle);
        if (!result_handle) {
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_release(session);
            ducknng_session_control_request_reset(&control_req);
            if (payload) duckdb_free(payload);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
                "ducknng: out of memory copying result handle");
            return -1;
        }
    }
    if (has_batch) {
        session->batch_no++;
        session->row_schema_sent = 1;
        batch_index = session->batch_no;
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_session_control_request_reset(&control_req);
        return ducknng_fetch_set_row_reply(reply, row_payload_format, 0u,
            prebuilt_msg, reply_prefix_size, payload, payload_len);
    }
    if (session->batch_no == 0) {
        if ((row_payload_format == DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH
                ? ducknng_result_empty_quack_message(session->result,
                    reply_prefix_size, &prebuilt_msg, &payload_len, &errmsg)
                : ducknng_result_to_ipc_stream(NULL, session->result,
                    &payload, &payload_len, &errmsg)) != 0) {
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_release(session);
            ducknng_session_control_request_reset(&control_req);
            if (result_handle) duckdb_free(result_handle);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
                errmsg ? errmsg : "ducknng: fetch failed to encode empty row batch");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        session->eos = 1;
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_session_control_request_reset(&control_req);
        if (result_handle) duckdb_free(result_handle);
        return ducknng_fetch_set_row_reply(reply, row_payload_format,
            DUCKNNG_RPC_FLAG_END_OF_STREAM, prebuilt_msg, reply_prefix_size,
            payload, payload_len);
    }
    session->eos = 1;
    batch_index = session->batch_no;
    ducknng_mutex_unlock(&session->mu);
    ducknng_session_release(session);
    view.session_id = session_id;
    view.state = "exhausted";
    view.result_handle = result_handle;
    view.has_batch_index = 1;
    view.batch_index = batch_index;
    view.correlation_id = control_req.correlation_id;
    if (ducknng_session_state_reply(reply, "fetch", DUCKNNG_RPC_FLAG_END_OF_STREAM, &view) != 0) {
        ducknng_session_control_request_reset(&control_req);
        if (result_handle) duckdb_free(result_handle);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: failed to build fetch control reply");
        return -1;
    }
    ducknng_session_control_request_reset(&control_req);
    if (result_handle) duckdb_free(result_handle);
    return 0;
}

static int ducknng_method_close_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    ducknng_session_reply_view view;
    uint64_t session_id = 0;
    int unauthorized = 0;
    ducknng_session *session;
    char *result_handle = NULL;
    uint64_t batch_index = 0;
    (void)method;
    memset(&control_req, 0, sizeof(control_req));
    memset(&view, 0, sizeof(view));
    if (ducknng_parse_session_control_request(req, "close", &control_req, reply) != 0) {
        return -1;
    }
    /* Reject upload sessions non-destructively before removing: close must not
     * detach and destroy a live upload session (which would roll its upload
     * back) while returning a query-session success. */
    if (ducknng_session_check_kind(svc, control_req.session_id, control_req.session_token,
            req->caller_identity, 0, reply) != 0) {
        ducknng_session_control_request_reset(&control_req);
        return -1;
    }
    session_id = control_req.session_id;
    session = ducknng_service_remove_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    if (!session) {
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: session not found");
        return -1;
    }
    batch_index = session->batch_no;
    if (session->result_handle) result_handle = ducknng_strdup(session->result_handle);
    ducknng_session_destroy(session);
    view.session_id = session_id;
    view.state = "closed";
    view.result_handle = result_handle;
    view.has_batch_index = 1;
    view.batch_index = batch_index;
    view.correlation_id = control_req.correlation_id;
    if (ducknng_session_state_reply(reply, "close", DUCKNNG_RPC_FLAG_SESSION_CLOSED, &view) != 0) {
        ducknng_session_control_request_reset(&control_req);
        if (result_handle) duckdb_free(result_handle);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: failed to build close control reply");
        return -1;
    }
    ducknng_session_control_request_reset(&control_req);
    if (result_handle) duckdb_free(result_handle);
    return 0;
}

static int ducknng_method_cancel_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    ducknng_session_reply_view view;
    uint64_t session_id = 0;
    int unauthorized = 0;
    ducknng_session *session;
    char *result_handle = NULL;
    uint64_t batch_index = 0;
    (void)method;
    memset(&control_req, 0, sizeof(control_req));
    memset(&view, 0, sizeof(view));
    if (ducknng_parse_session_control_request(req, "cancel", &control_req, reply) != 0) {
        return -1;
    }
    /* Reject upload sessions non-destructively: cancel is a query-family
     * control and must not consume a live upload session. */
    if (ducknng_session_check_kind(svc, control_req.session_id, control_req.session_token,
            req->caller_identity, 0, reply) != 0) {
        ducknng_session_control_request_reset(&control_req);
        return -1;
    }
    session_id = control_req.session_id;
    session = ducknng_service_remove_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    if (!session) {
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: session not found");
        return -1;
    }
    batch_index = session->batch_no;
    if (session->result_handle) result_handle = ducknng_strdup(session->result_handle);
    if (session->mu_initialized) {
        ducknng_mutex_lock(&session->mu);
        session->cancelled = 1;
        ducknng_mutex_unlock(&session->mu);
    }
    if (session->session_con) {
        duckdb_interrupt(session->session_con);
    }
    ducknng_session_destroy(session);
    view.session_id = session_id;
    view.state = "cancelled";
    view.result_handle = result_handle;
    view.has_batch_index = 1;
    view.batch_index = batch_index;
    view.correlation_id = control_req.correlation_id;
    if (ducknng_session_state_reply(reply, "cancel",
            DUCKNNG_RPC_FLAG_CANCELLED | DUCKNNG_RPC_FLAG_SESSION_CLOSED, &view) != 0) {
        ducknng_session_control_request_reset(&control_req);
        if (result_handle) duckdb_free(result_handle);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: failed to build cancel control reply");
        return -1;
    }
    ducknng_session_control_request_reset(&control_req);
    if (result_handle) duckdb_free(result_handle);
    return 0;
}

static int ducknng_method_query_prepare_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_query_open_request open_req;
    ducknng_service_sql_scope scope;
    duckdb_extracted_statements extracted = NULL;
    duckdb_prepared_statement stmt = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    char *errmsg = NULL;
    idx_t n_stmts = 0;
    (void)method;

    memset(&open_req, 0, sizeof(open_req));
    memset(&scope, 0, sizeof(scope));

    if (!svc || !svc->rt || !req || !req->frame || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing query_prepare execution context");
        return -1;
    }
    if (ducknng_decode_query_open_payload(req->frame->payload, (size_t)req->frame->payload_len,
            &open_req, &errmsg) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            errmsg ? errmsg : "ducknng: invalid query_prepare payload");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    if (ducknng_service_enter_request_sql(svc, &scope, &errmsg) != 0) {
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            errmsg ? errmsg : "ducknng: no connection available for query_prepare");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, svc);

    n_stmts = duckdb_extract_statements(scope.con, open_req.sql, &extracted);
    if (n_stmts == 0) {
        const char *ext_err = extracted ? duckdb_extract_statements_error(extracted) : NULL;
        char *detail = ducknng_strdup(ext_err ? ext_err : "ducknng: query_prepare: empty or invalid SQL");
        if (extracted) duckdb_destroy_extracted(&extracted);
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_service_leave_request_sql(&scope);
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            detail && detail[0] ? detail : "ducknng: query_prepare: empty or invalid SQL");
        if (detail) duckdb_free(detail);
        return -1;
    }
    if (n_stmts != 1) {
        duckdb_destroy_extracted(&extracted);
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_service_leave_request_sql(&scope);
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: query_prepare requires exactly one SQL statement");
        return -1;
    }
    if (duckdb_prepare_extracted_statement(scope.con, extracted, 0, &stmt) == DuckDBError) {
        char *detail = ducknng_strdup(stmt ? duckdb_prepare_error(stmt) : NULL);
        if (stmt) duckdb_destroy_prepare(&stmt);
        stmt = NULL;
        duckdb_destroy_extracted(&extracted);
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_service_leave_request_sql(&scope);
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            detail && detail[0] ? detail : "ducknng: query_prepare: prepare failed");
        if (detail) duckdb_free(detail);
        return -1;
    }
    duckdb_destroy_extracted(&extracted);
    extracted = NULL;

    if (ducknng_bind_query_parameters(stmt, &open_req, &errmsg) != 0) {
        duckdb_destroy_prepare(&stmt);
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_service_leave_request_sql(&scope);
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            errmsg ? errmsg : "ducknng: failed to bind query_prepare parameters");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }

    if (ducknng_prepared_schema_to_ipc(scope.con, stmt, &payload, &payload_len, &errmsg) != 0) {
        duckdb_destroy_prepare(&stmt);
        if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_service_leave_request_sql(&scope);
        ducknng_query_open_request_destroy(&open_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
            errmsg ? errmsg : "ducknng: query_prepare: failed to encode schema as Arrow IPC");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    duckdb_destroy_prepare(&stmt);
    if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
    ducknng_service_leave_request_sql(&scope);
    ducknng_query_open_request_destroy(&open_req);
    ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
        DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM |
            DUCKNNG_RPC_FLAG_END_OF_STREAM,
        payload, payload_len);
    return 0;
}

static int ducknng_execute_parameterized_sql(duckdb_connection con,
    const char *sql, duckdb_value *parameters, idx_t parameter_count,
    duckdb_prepared_statement *out_stmt, duckdb_result *out_result,
    char **errmsg) {
    duckdb_extracted_statements extracted = NULL;
    duckdb_prepared_statement stmt = NULL;
    idx_t n_stmts;
    if (out_stmt) *out_stmt = NULL;
    if (!con || !sql || !sql[0] || !out_stmt || !out_result) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: invalid parameterized exec context");
        return -1;
    }
    n_stmts = duckdb_extract_statements(con, sql, &extracted);
    if (n_stmts != 1) {
        if (errmsg) {
            const char *detail = n_stmts == 0 && extracted
                ? duckdb_extract_statements_error(extracted) : NULL;
            *errmsg = ducknng_strdup(detail && detail[0] ? detail :
                "ducknng: parameterized exec requires exactly one SQL statement");
        }
        if (extracted) duckdb_destroy_extracted(&extracted);
        return -1;
    }
    if (duckdb_prepare_extracted_statement(con, extracted, 0, &stmt) == DuckDBError) {
        if (errmsg) *errmsg = ducknng_strdup(stmt && duckdb_prepare_error(stmt)
            ? duckdb_prepare_error(stmt) :
            "ducknng: failed to prepare parameterized exec statement");
        if (stmt) duckdb_destroy_prepare(&stmt);
        duckdb_destroy_extracted(&extracted);
        return -1;
    }
    duckdb_destroy_extracted(&extracted);
    if (ducknng_bind_sql_parameters(stmt, parameters, parameter_count,
            errmsg) != 0 ||
        duckdb_execute_prepared(stmt, out_result) == DuckDBError) {
        if (errmsg && !*errmsg) {
            const char *detail = duckdb_result_error(out_result);
            *errmsg = ducknng_strdup(detail && detail[0] ? detail :
                "ducknng: parameterized exec failed");
        }
        if (out_result->internal_data) duckdb_destroy_result(out_result);
        memset(out_result, 0, sizeof(*out_result));
        duckdb_destroy_prepare(&stmt);
        return -1;
    }
    *out_stmt = stmt;
    return 0;
}


static int ducknng_method_exec_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_exec_request exec_req;
    duckdb_result result;
    duckdb_prepared_statement parameterized_stmt = NULL;
    ducknng_service_sql_scope scope;
    duckdb_statement_type stmt_type;
    duckdb_result_type result_type;
    idx_t rows_changed;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    char *errmsg = NULL;
    (void)method;

    memset(&exec_req, 0, sizeof(exec_req));
    memset(&result, 0, sizeof(result));
    if (!svc || !svc->rt || !req || !req->frame || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing execution context");
        return -1;
    }
    if (ducknng_decode_exec_request_payload(req->frame->payload, (size_t)req->frame->payload_len,
            &exec_req, &errmsg) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            errmsg ? errmsg : "ducknng: invalid exec payload");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }

    if (exec_req.parameter_count > 0) {
        if (ducknng_service_enter_request_sql(svc, &scope, &errmsg) != 0) {
            ducknng_exec_request_destroy(&exec_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
                errmsg ? errmsg : "ducknng: missing execution context");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        if (ducknng_execute_parameterized_sql(scope.con, exec_req.sql,
                exec_req.parameters, exec_req.parameter_count,
                &parameterized_stmt, &result, &errmsg) != 0) {
            ducknng_service_leave_request_sql(&scope);
            ducknng_exec_request_destroy(&exec_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                errmsg ? errmsg : "ducknng: parameterized exec failed");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        if (exec_req.want_result) {
            if (duckdb_result_return_type(result) != DUCKDB_RESULT_TYPE_QUERY_RESULT ||
                ducknng_result_to_ipc_stream(parameterized_stmt, result, &payload,
                    &payload_len, &errmsg) != 0) {
                ducknng_service_leave_request_sql(&scope);
                duckdb_destroy_result(&result);
                duckdb_destroy_prepare(&parameterized_stmt);
                ducknng_exec_request_destroy(&exec_req);
                ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
                    errmsg ? errmsg :
                    "ducknng: failed to encode parameterized query result as Arrow IPC");
                if (errmsg) duckdb_free(errmsg);
                return -1;
            }
            ducknng_service_leave_request_sql(&scope);
            duckdb_destroy_result(&result);
            duckdb_destroy_prepare(&parameterized_stmt);
            ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
                DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
                payload, payload_len);
            payload = NULL;
        } else {
            ducknng_service_leave_request_sql(&scope);
            stmt_type = duckdb_result_statement_type(result);
            result_type = duckdb_result_return_type(result);
            if (result_type == DUCKDB_RESULT_TYPE_QUERY_RESULT) {
                duckdb_destroy_result(&result);
                duckdb_destroy_prepare(&parameterized_stmt);
                ducknng_exec_request_destroy(&exec_req);
                ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                    "ducknng: EXEC result requires want_result = true");
                return -1;
            }
            rows_changed = duckdb_rows_changed(&result);
            duckdb_destroy_result(&result);
            duckdb_destroy_prepare(&parameterized_stmt);
            memset(&result, 0, sizeof(result));
            if (ducknng_exec_metadata_to_ipc((uint64_t)rows_changed,
                    (uint32_t)stmt_type, (uint32_t)result_type, &payload,
                    &payload_len, &errmsg) != 0) {
                ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
                    errmsg ? errmsg :
                    "ducknng: failed to encode parameterized exec metadata as Arrow IPC");
            } else {
                ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
                    DUCKNNG_RPC_FLAG_RESULT_METADATA |
                        DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
                    payload, payload_len);
                payload = NULL;
            }
        }
    } else if (exec_req.want_result) {
        if (ducknng_service_enter_request_sql(svc, &scope, &errmsg) != 0) {
            ducknng_exec_request_destroy(&exec_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
                errmsg ? errmsg : "ducknng: missing execution context");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        if (ducknng_query_to_ipc_stream(scope.con,
                exec_req.sql, &payload, &payload_len, &errmsg) != 0) {
            ducknng_service_leave_request_sql(&scope);
            ducknng_exec_request_destroy(&exec_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
                errmsg ? errmsg : "ducknng: failed to encode query result as Arrow IPC");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        ducknng_service_leave_request_sql(&scope);
        ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
            DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
            payload, payload_len);
        payload = NULL;
    } else {
        if (ducknng_service_enter_request_sql(svc, &scope, &errmsg) != 0) {
            ducknng_exec_request_destroy(&exec_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
                errmsg ? errmsg : "ducknng: missing execution context");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        if (duckdb_query(scope.con, exec_req.sql, &result) == DuckDBError) {
            char *exec_err = ducknng_strdup(duckdb_result_error(&result));
            ducknng_service_leave_request_sql(&scope);
            duckdb_destroy_result(&result);
            ducknng_exec_request_destroy(&exec_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                exec_err && exec_err[0] ? exec_err : "ducknng: exec failed");
            if (exec_err) duckdb_free(exec_err);
            return -1;
        }
        ducknng_service_leave_request_sql(&scope);

        stmt_type = duckdb_result_statement_type(result);
        result_type = duckdb_result_return_type(result);
        if (result_type == DUCKDB_RESULT_TYPE_QUERY_RESULT) {
            duckdb_destroy_result(&result);
            ducknng_exec_request_destroy(&exec_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                "ducknng: EXEC result requires want_result = true");
            return -1;
        }
        rows_changed = duckdb_rows_changed(&result);
        duckdb_destroy_result(&result);
        memset(&result, 0, sizeof(result));
        if (ducknng_exec_metadata_to_ipc((uint64_t)rows_changed,
                (uint32_t)stmt_type, (uint32_t)result_type, &payload, &payload_len, &errmsg) != 0) {
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
                errmsg ? errmsg : "ducknng: failed to encode exec metadata as Arrow IPC");
        } else {
            ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
                DUCKNNG_RPC_FLAG_RESULT_METADATA | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
                payload, payload_len);
            payload = NULL;
        }
    }
    if (payload) duckdb_free(payload);
    if (errmsg) duckdb_free(errmsg);
    ducknng_exec_request_destroy(&exec_req);
    return reply->type == DUCKNNG_RPC_ERROR ? -1 : 0;
}

/* --- Upload lane (issue #9): client-to-server tuple streams --- */

static int ducknng_upload_run_stmt(duckdb_connection con, const char *sql, char **errmsg) {
    duckdb_result res;
    memset(&res, 0, sizeof(res));
    if (duckdb_query(con, sql, &res) == DuckDBError) {
        if (errmsg && !*errmsg) {
            const char *m = duckdb_result_error(&res);
            *errmsg = ducknng_strdup(m && m[0] ? m : "ducknng: upload transaction statement failed");
        }
        duckdb_destroy_result(&res);
        return -1;
    }
    duckdb_destroy_result(&res);
    return 0;
}

/* A v1 upload target identifier part: an unquoted SQL identifier. Restricting
 * the charset keeps both duckdb_appender_create_ext (literal parts) and the
 * quoted name-capture query injection-safe. Quoted/exotic identifiers are a
 * documented v1 limitation. */
static int ducknng_upload_ident_ok(const char *s) {
    size_t i;
    if (!s || !s[0]) return 0;
    if (!((s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= 'a' && s[0] <= 'z') || s[0] == '_')) return 0;
    for (i = 1; s[i]; i++) {
        char c = s[i];
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '$')) return 0;
    }
    return 1;
}

/* Split "[[catalog.]schema.]table" into duckdb_strdup'd parts (schema/catalog
 * NULL when absent). Fails closed on empty, >3 parts, or any part that is not
 * a simple identifier. */
static int ducknng_upload_split_target(const char *target, char **out_catalog,
    char **out_schema, char **out_table, char **errmsg) {
    char *copy;
    char *parts[3];
    int nparts = 0;
    char *p;
    int i;
    *out_catalog = NULL;
    *out_schema = NULL;
    *out_table = NULL;
    copy = ducknng_strdup(target ? target : "");
    if (!copy) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing upload target");
        return -1;
    }
    p = copy;
    for (;;) {
        char *dot = strchr(p, '.');
        if (nparts >= 3) { nparts = 4; break; }
        if (dot) { *dot = '\0'; parts[nparts++] = p; p = dot + 1; }
        else { parts[nparts++] = p; break; }
    }
    if (nparts < 1 || nparts > 3) {
        duckdb_free(copy);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(
            "ducknng: upload target must be a simple [[catalog.]schema.]table identifier");
        return -1;
    }
    for (i = 0; i < nparts; i++) {
        if (!ducknng_upload_ident_ok(parts[i])) {
            duckdb_free(copy);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(
                "ducknng: upload target must be a simple [[catalog.]schema.]table identifier");
            return -1;
        }
    }
    if (nparts == 1) {
        *out_table = ducknng_strdup(parts[0]);
    } else if (nparts == 2) {
        *out_schema = ducknng_strdup(parts[0]);
        *out_table = ducknng_strdup(parts[1]);
    } else {
        *out_catalog = ducknng_strdup(parts[0]);
        *out_schema = ducknng_strdup(parts[1]);
        *out_table = ducknng_strdup(parts[2]);
    }
    duckdb_free(copy);
    if (!*out_table || (nparts >= 2 && !*out_schema) || (nparts == 3 && !*out_catalog)) {
        if (*out_catalog) duckdb_free(*out_catalog);
        if (*out_schema) duckdb_free(*out_schema);
        if (*out_table) duckdb_free(*out_table);
        *out_catalog = *out_schema = *out_table = NULL;
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing upload target");
        return -1;
    }
    return 0;
}

/* Capture the target's column names in ordinal order via a LIMIT 0 select on
 * the fully-quoted identifier (parts are validated identifiers, so quoting is
 * injection-safe). Returns a duckdb_malloc'd array of duckdb_strdup'd names. */
static int ducknng_upload_capture_col_names(duckdb_connection con,
    const char *catalog, const char *schema, const char *table,
    char ***out_names, idx_t *out_count, char **errmsg) {
    char *sql = NULL;
    size_t cap = 64;
    duckdb_result res;
    idx_t ncols;
    idx_t i;
    char **names = NULL;
    int rc = -1;
    *out_names = NULL;
    *out_count = 0;
    if (catalog) cap += strlen(catalog);
    if (schema) cap += strlen(schema);
    cap += strlen(table) + 32;
    sql = (char *)duckdb_malloc(cap);
    if (!sql) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building upload schema query");
        return -1;
    }
    /* Parts are validated identifiers (no quotes), so "%s" quoting is safe. */
    if (catalog) {
        snprintf(sql, cap, "SELECT * FROM \"%s\".\"%s\".\"%s\" LIMIT 0", catalog, schema, table);
    } else if (schema) {
        snprintf(sql, cap, "SELECT * FROM \"%s\".\"%s\" LIMIT 0", schema, table);
    } else {
        snprintf(sql, cap, "SELECT * FROM \"%s\" LIMIT 0", table);
    }
    memset(&res, 0, sizeof(res));
    if (duckdb_query(con, sql, &res) == DuckDBError) {
        if (errmsg && !*errmsg) {
            const char *m = duckdb_result_error(&res);
            *errmsg = ducknng_strdup(m && m[0] ? m : "ducknng: upload_open could not resolve the target table");
        }
        duckdb_destroy_result(&res);
        duckdb_free(sql);
        return -1;
    }
    duckdb_free(sql);
    ncols = duckdb_column_count(&res);
    if (ncols > 0) {
        names = (char **)duckdb_malloc(sizeof(*names) * (size_t)ncols);
        if (!names) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying upload column names");
            goto done;
        }
        memset(names, 0, sizeof(*names) * (size_t)ncols);
        for (i = 0; i < ncols; i++) {
            const char *cn = duckdb_column_name(&res, i);
            names[i] = ducknng_strdup(cn ? cn : "");
            if (!names[i]) {
                if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying upload column names");
                goto done;
            }
        }
    }
    *out_names = names;
    *out_count = ncols;
    names = NULL;
    rc = 0;
done:
    if (names) {
        for (i = 0; i < ncols; i++) if (names[i]) duckdb_free(names[i]);
        duckdb_free(names);
    }
    duckdb_destroy_result(&res);
    return rc;
}

static int ducknng_method_upload_open_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    char *json = NULL;
    char *target = NULL;
    char *mode = NULL;
    char *correlation_id = NULL;
    duckdb_connection con = NULL;
    size_t pool_index = (size_t)-1;
    duckdb_appender appender = NULL;
    int txn_open = 0;
    uint64_t session_id = 0;
    char *owner_token = NULL;
    char *result_handle = NULL;
    char *errmsg = NULL;
    char *cat = NULL;
    char *sch = NULL;
    char *tbl = NULL;
    char **col_names = NULL;
    idx_t col_count = 0;
    ducknng_session_reply_view view;
    (void)method;
    if (!svc || !svc->rt || !req || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: missing upload_open context");
        return -1;
    }
    json = ducknng_copy_payload_json(req);
    if (!json) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: upload_open requires a JSON payload with target_table");
        return -1;
    }
    target = ducknng_json_extract_string_dup(json, "target_table");
    mode = ducknng_json_extract_string_dup(json, "mode");
    correlation_id = ducknng_json_extract_string_dup(json, "correlation_id");
    duckdb_free(json);
    if (!target || !target[0]) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: upload_open requires target_table");
        goto fail;
    }
    if (mode && mode[0] && strcmp(mode, "append") != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: upload_open v1 supports only mode 'append' to an existing table");
        goto fail;
    }
    if (ducknng_upload_split_target(target, &cat, &sch, &tbl, &errmsg) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            errmsg ? errmsg : "ducknng: invalid upload target");
        goto fail;
    }
    if (ducknng_service_acquire_session_connection(svc, &con, &pool_index, &errmsg) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            errmsg ? errmsg : "ducknng: upload_open could not acquire a session connection");
        goto fail;
    }
    ducknng_runtime_current_request_service_set(svc->rt, svc);
    if (ducknng_upload_run_stmt(con, "BEGIN TRANSACTION", &errmsg) != 0) {
        ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            errmsg ? errmsg : "ducknng: upload_open failed to begin a transaction");
        goto fail;
    }
    txn_open = 1;
    if (duckdb_appender_create_ext(con, cat, sch, tbl, &appender) == DuckDBError) {
        duckdb_error_data ed = duckdb_appender_error_data(appender);
        const char *m = ed ? duckdb_error_data_message(ed) : NULL;
        if (!errmsg) errmsg = ducknng_strdup(m && m[0] ? m :
            "ducknng: upload_open could not open an appender on the target table");
        if (ed) duckdb_destroy_error_data(&ed);
        if (appender) { duckdb_appender_destroy(&appender); appender = NULL; }
        ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR, errmsg);
        goto fail;
    }
    if (ducknng_upload_capture_col_names(con, cat, sch, tbl, &col_names, &col_count, &errmsg) != 0) {
        ducknng_runtime_current_request_service_set(svc->rt, NULL);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            errmsg ? errmsg : "ducknng: upload_open could not resolve the target columns");
        goto fail;
    }
    ducknng_runtime_current_request_service_set(svc->rt, NULL);
    {
        int add_rc = ducknng_service_add_upload_session(svc, con, pool_index, appender, target,
            col_names, col_count, req->caller_identity, &session_id, &owner_token, &result_handle, &errmsg);
        if (add_rc != 0) {
            ducknng_method_reply_set_error(reply,
                add_rc > 0 ? DUCKNNG_STATUS_BUSY : DUCKNNG_STATUS_INTERNAL,
                errmsg ? errmsg : "ducknng: upload_open failed to register the upload session");
            goto fail;
        }
    }
    /* The session now owns the connection, appender, transaction, and the
     * column-name array. */
    con = NULL;
    appender = NULL;
    txn_open = 0;
    pool_index = (size_t)-1;
    col_names = NULL;
    col_count = 0;
    memset(&view, 0, sizeof(view));
    view.session_id = session_id;
    view.session_token = owner_token;
    view.state = "open";
    view.next_method = "upload_append";
    view.has_idle_timeout_ms = 1;
    view.idle_timeout_ms = svc->session_idle_ms;
    /* Upload sessions have no fetchable result, so no result_handle is emitted;
     * correlation_id is echoed when the caller supplied one. */
    view.correlation_id = correlation_id;
    if (ducknng_session_state_reply(reply, "upload_open", DUCKNNG_RPC_FLAG_SESSION_OPEN, &view) != 0) {
        ducknng_session *dead = ducknng_service_remove_session(svc, session_id, owner_token,
            req->caller_identity, NULL);
        if (dead) ducknng_session_destroy(dead);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: failed to build upload_open reply");
        goto fail;
    }
    if (target) duckdb_free(target);
    if (mode) duckdb_free(mode);
    if (correlation_id) duckdb_free(correlation_id);
    if (owner_token) duckdb_free(owner_token);
    if (result_handle) duckdb_free(result_handle);
    if (cat) duckdb_free(cat);
    if (sch) duckdb_free(sch);
    if (tbl) duckdb_free(tbl);
    if (errmsg) duckdb_free(errmsg);
    return 0;
fail:
    if (appender) duckdb_appender_destroy(&appender);
    if (txn_open && con) {
        char *rollback_err = NULL;
        ducknng_upload_run_stmt(con, "ROLLBACK", &rollback_err);
        if (rollback_err) duckdb_free(rollback_err);
    }
    if (con && pool_index != (size_t)-1) ducknng_service_release_session_connection(svc, pool_index);
    if (col_names) {
        idx_t ci;
        for (ci = 0; ci < col_count; ci++) if (col_names[ci]) duckdb_free(col_names[ci]);
        duckdb_free(col_names);
    }
    if (target) duckdb_free(target);
    if (mode) duckdb_free(mode);
    if (correlation_id) duckdb_free(correlation_id);
    if (owner_token) duckdb_free(owner_token);
    if (result_handle) duckdb_free(result_handle);
    if (cat) duckdb_free(cat);
    if (sch) duckdb_free(sch);
    if (tbl) duckdb_free(tbl);
    if (errmsg) duckdb_free(errmsg);
    return -1;
}

static int ducknng_method_upload_append_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    uint64_t session_id = 0;
    const uint8_t *token = NULL;
    size_t token_len = 0;
    size_t quack_off = 0;
    char *token_str = NULL;
    ducknng_session *session = NULL;
    int unauthorized = 0;
    uint64_t rows_now = 0;
    char *errmsg = NULL;
    char control[96];
    (void)method;
    if (!svc || !req || !req->frame || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: missing upload_append context");
        return -1;
    }
    if (ducknng_upload_append_parse_prefix(req->frame->payload, (size_t)req->frame->payload_len,
            &session_id, &token, &token_len, &quack_off) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: upload_append payload must be [session_id][token_len][token][quack batch]");
        return -1;
    }
    token_str = (char *)duckdb_malloc(token_len + 1);
    if (!token_str) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: out of memory copying upload token");
        return -1;
    }
    memcpy(token_str, token, token_len);
    token_str[token_len] = '\0';
    /* The prefix token is counted bytes but acquire_session compares a C
     * string; an embedded NUL would truncate the comparison and let a byte
     * sequence other than the counted token authenticate. Owner tokens never
     * contain NUL, so reject any counted token that does. */
    if (strlen(token_str) != token_len) {
        duckdb_free(token_str);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: upload session token contains an embedded NUL");
        return -1;
    }
    session = ducknng_service_acquire_session(svc, session_id, token_str, req->caller_identity, &unauthorized);
    duckdb_free(token_str);
    if (!session) {
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: upload session not found");
        return -1;
    }
    ducknng_mutex_lock(&session->mu);
    session->last_touch_ms = ducknng_now_ms();
    if (!session->is_upload || !session->upload_appender_open) {
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID, "ducknng: session is not an open upload session");
        return -1;
    }
    if (ducknng_quack_payload_append_to_appender(session->upload_appender,
            req->frame->payload + quack_off, (size_t)req->frame->payload_len - quack_off,
            (const char *const *)session->upload_col_names, session->upload_col_count,
            &session->upload_rows, &errmsg) != 0) {
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            errmsg ? errmsg : "ducknng: upload_append failed to append the batch");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    rows_now = session->upload_rows;
    ducknng_mutex_unlock(&session->mu);
    ducknng_session_release(session);
    snprintf(control, sizeof(control), "{\"state\":\"open\",\"rows\":%llu}", (unsigned long long)rows_now);
    if (ducknng_json_reply(reply, "upload_append", 0, control) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: failed to build upload_append reply");
        return -1;
    }
    return 0;
}

static int ducknng_method_upload_commit_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    ducknng_session *session = NULL;
    int unauthorized = 0;
    uint64_t rows = 0;
    char *errmsg = NULL;
    char control[96];
    (void)method;
    memset(&control_req, 0, sizeof(control_req));
    if (ducknng_parse_session_control_request(req, "upload_commit", &control_req, reply) != 0) {
        return -1;
    }
    /* Confirm this is an upload session non-destructively before removing it, so
     * a mistyped control request against a live query session's id+token cannot
     * detach and destroy that other family's session. */
    if (ducknng_session_check_kind(svc, control_req.session_id, control_req.session_token,
            req->caller_identity, 1, reply) != 0) {
        ducknng_session_control_request_reset(&control_req);
        return -1;
    }
    session = ducknng_service_remove_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    if (!session) {
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: upload session not found");
        return -1;
    }
    if (!session->is_upload) {
        /* Raced from upload session to query session between the check and the
         * remove is not possible (owner token is single-holder), but guard
         * anyway: a non-upload session here is detached, so destroy it. */
        ducknng_session_destroy(session);
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID, "ducknng: session is not an upload session");
        return -1;
    }
    /* Take the session lock for the whole appender/commit critical section.
     * upload_append holds this same lock while it appends, so a concurrent
     * append either completes before this runs or, if it was waiting, observes
     * the cleared upload_appender_open below and bails without touching the
     * destroyed appender. The session is already detached, so no new append can
     * acquire it. */
    ducknng_mutex_lock(&session->mu);
    rows = session->upload_rows;
    /* Flush and close the appender (writing buffered rows into the transaction),
     * then commit. Clearing the flags means teardown will not roll back. */
    if (session->upload_appender_open) {
        if (duckdb_appender_flush(session->upload_appender) == DuckDBError) {
            duckdb_error_data ed = duckdb_appender_error_data(session->upload_appender);
            const char *m = ed ? duckdb_error_data_message(ed) : NULL;
            errmsg = ducknng_strdup(m && m[0] ? m : "ducknng: upload_commit failed to flush the appender");
            if (ed) duckdb_destroy_error_data(&ed);
            /* A failed flush invalidates the appender. Destroy and clear it under
             * the lock (as the close-failure path does) so a concurrent
             * upload_append waiting on session->mu observes upload_appender_open
             * == 0 and bails instead of calling into the invalidated appender
             * before teardown completes. Leave upload_txn_open set so
             * ducknng_session_destroy rolls the transaction back. */
            duckdb_appender_destroy(&session->upload_appender);
            session->upload_appender_open = 0;
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_destroy(session);
            ducknng_session_control_request_reset(&control_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR, errmsg);
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        /* Close the appender (flush intermediate states + finalize) and check
         * the result BEFORE destroying it: destroy also finalizes and can fail
         * on a deferred constraint violation, but it invalidates the error
         * message, so close is the only place we can both detect the failure
         * and recover its text. On failure, clear only the appender flag and
         * leave upload_txn_open set so ducknng_session_destroy rolls the
         * transaction back instead of committing a half-finalized upload. */
        if (duckdb_appender_close(session->upload_appender) == DuckDBError) {
            duckdb_error_data ed = duckdb_appender_error_data(session->upload_appender);
            const char *m = ed ? duckdb_error_data_message(ed) : NULL;
            errmsg = ducknng_strdup(m && m[0] ? m : "ducknng: upload_commit failed to close the appender");
            if (ed) duckdb_destroy_error_data(&ed);
            duckdb_appender_destroy(&session->upload_appender);
            session->upload_appender_open = 0;
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_destroy(session);
            ducknng_session_control_request_reset(&control_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                errmsg ? errmsg : "ducknng: upload_commit failed to close the appender");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        duckdb_appender_destroy(&session->upload_appender);
        session->upload_appender_open = 0;
    }
    if (session->upload_txn_open && session->session_con) {
        if (ducknng_upload_run_stmt(session->session_con, "COMMIT", &errmsg) != 0) {
            /* Commit failed; roll back so nothing partially persists. */
            char *rollback_err = NULL;
            ducknng_upload_run_stmt(session->session_con, "ROLLBACK", &rollback_err);
            if (rollback_err) duckdb_free(rollback_err);
            session->upload_txn_open = 0;
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_destroy(session);
            ducknng_session_control_request_reset(&control_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                errmsg ? errmsg : "ducknng: upload_commit failed to commit the transaction");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        session->upload_txn_open = 0;
    }
    ducknng_mutex_unlock(&session->mu);
    ducknng_session_destroy(session);
    snprintf(control, sizeof(control), "{\"state\":\"committed\",\"rows\":%llu}", (unsigned long long)rows);
    if (ducknng_json_reply(reply, "upload_commit", DUCKNNG_RPC_FLAG_SESSION_CLOSED, control) != 0) {
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: failed to build upload_commit reply");
        return -1;
    }
    ducknng_session_control_request_reset(&control_req);
    return 0;
}

static int ducknng_method_upload_abort_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    ducknng_session *session = NULL;
    int unauthorized = 0;
    (void)method;
    memset(&control_req, 0, sizeof(control_req));
    if (ducknng_parse_session_control_request(req, "upload_abort", &control_req, reply) != 0) {
        return -1;
    }
    if (ducknng_session_check_kind(svc, control_req.session_id, control_req.session_token,
            req->caller_identity, 1, reply) != 0) {
        ducknng_session_control_request_reset(&control_req);
        return -1;
    }
    session = ducknng_service_remove_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    if (!session) {
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: upload session not found");
        return -1;
    }
    /* Teardown destroys the appender and rolls the transaction back. */
    ducknng_session_destroy(session);
    if (ducknng_json_reply(reply, "upload_abort", DUCKNNG_RPC_FLAG_SESSION_CLOSED,
            "{\"state\":\"aborted\"}") != 0) {
        ducknng_session_control_request_reset(&control_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: failed to build upload_abort reply");
        return -1;
    }
    ducknng_session_control_request_reset(&control_req);
    return 0;
}

const ducknng_method_descriptor ducknng_method_query_prepare = {
    "query_prepare",
    "query",
    "Prepare a SQL statement and return the output schema without executing",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_RESPONSE_ROWS,
    DUCKNNG_SESSION_STATELESS,
    0,
    DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM | DUCKNNG_RPC_FLAG_END_OF_STREAM,
    16 * 1024 * 1024,
    16 * 1024 * 1024,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    "{\"fields\":[{\"name\":\"sql\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"batch_rows\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"batch_bytes\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true},{\"name\":\"serialization_mode\",\"type\":\"string\",\"nullable\":true},{\"name\":\"params\",\"type\":\"struct\",\"nullable\":true,\"semantics\":\"positional_sql_parameters\",\"max_children\":65535}]}",
    "{\"mode\":\"schema_only\",\"note\":\"Arrow IPC stream with schema and 0 data batches\"}",
    ducknng_method_query_prepare_handler
};

const ducknng_method_descriptor ducknng_method_upload_open = {
    "upload_open",
    "upload",
    "Open a client-to-server table upload session",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_SESSION_OPEN,
    DUCKNNG_SESSION_OPENS,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON | DUCKNNG_RPC_FLAG_SESSION_OPEN,
    1024 * 1024,
    1024 * 1024,
    0,
    0,
    1,
    0,
    1,
    0,
    0,
    0,
    1,
    "{\"fields\":[{\"name\":\"target_table\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"mode\",\"type\":\"utf8\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    "{\"type\":\"json\",\"session_open\":true,\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"next_method\",\"type\":\"string\",\"nullable\":false},{\"name\":\"idle_timeout_ms\",\"type\":\"uint64\",\"nullable\":false,\"policy\":\"server_effective\"},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    ducknng_method_upload_open_handler
};

const ducknng_method_descriptor ducknng_method_upload_append = {
    "upload_append",
    "upload",
    "Append one quack batch to an open upload session",
    DUCKNNG_TRANSPORT_REQREP,
    /* The request body is a custom upload-append frame (session prefix + an
     * embedded quack batch), advertised with its own payload format so
     * manifest-driven clients build the prefix (per request_schema_json below)
     * rather than send a bare quack batch or assume no payload. */
    DUCKNNG_PAYLOAD_UPLOAD_APPEND,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_REQUIRES,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    16 * 1024 * 1024,
    1024 * 1024,
    0,
    1,
    0,
    0,
    1,
    0,
    0,
    0,
    1,
    "{\"layout\":\"ducknng_upload_append\",\"prefix\":[{\"name\":\"session_id\",\"type\":\"uint64_le\"},{\"name\":\"token_len\",\"type\":\"uint16_le\"},{\"name\":\"token\",\"type\":\"bytes\"}],\"body\":\"ducknng_quack_batch\"}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"rows\",\"type\":\"uint64\",\"nullable\":false}]}",
    ducknng_method_upload_append_handler
};

const ducknng_method_descriptor ducknng_method_upload_commit = {
    "upload_commit",
    "upload",
    "Commit an upload session and return the appended row count",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_CLOSES,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON | DUCKNNG_RPC_FLAG_SESSION_CLOSED,
    1024 * 1024,
    1024 * 1024,
    0,
    1,
    0,
    1,
    1,
    0,
    0,
    0,
    1,
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"rows\",\"type\":\"uint64\",\"nullable\":false}]}",
    ducknng_method_upload_commit_handler
};

const ducknng_method_descriptor ducknng_method_upload_abort = {
    "upload_abort",
    "upload",
    "Abort an upload session and roll back its transaction",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_CLOSES,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON | DUCKNNG_RPC_FLAG_SESSION_CLOSED,
    1024 * 1024,
    1024 * 1024,
    0,
    1,
    0,
    1,
    1,
    0,
    0,
    0,
    1,
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"state\",\"type\":\"string\",\"nullable\":false}]}",
    ducknng_method_upload_abort_handler
};

const ducknng_method_descriptor ducknng_method_exec = {
    "exec",
    "sql",
    "Execute SQL and return metadata or rows",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_RESPONSE_METADATA_OR_ROWS,
    DUCKNNG_SESSION_STATELESS,
    0,
    DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_RESULT_METADATA | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
    16 * 1024 * 1024,
    16 * 1024 * 1024,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    1,
    "{\"fields\":[{\"name\":\"sql\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"want_result\",\"type\":\"bool\",\"nullable\":false},{\"name\":\"params\",\"type\":\"struct\",\"nullable\":true,\"semantics\":\"positional_sql_parameters\",\"max_children\":65535}]}",
    "{\"mode\":\"metadata_or_rows\"}",
    ducknng_method_exec_handler
};

const ducknng_method_descriptor ducknng_method_query_open = {
    "query_open",
    "query",
    "Open a server-side query session",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_SESSION_OPEN,
    DUCKNNG_SESSION_OPENS,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON | DUCKNNG_RPC_FLAG_SESSION_OPEN,
    16 * 1024 * 1024,
    1024 * 1024,
    0,
    0,
    1,
    0,
    1,
    0,
    0,
    0,
    1,
    "{\"fields\":[{\"name\":\"sql\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"batch_rows\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"batch_bytes\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true},{\"name\":\"serialization_mode\",\"type\":\"string\",\"nullable\":true},{\"name\":\"params\",\"type\":\"struct\",\"nullable\":true,\"semantics\":\"positional_sql_parameters\",\"max_children\":65535}]}",
    "{\"type\":\"json\",\"session_open\":true,\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"next_method\",\"type\":\"string\",\"nullable\":false},{\"name\":\"idle_timeout_ms\",\"type\":\"uint64\",\"nullable\":false,\"policy\":\"server_effective\"},{\"name\":\"result_handle\",\"type\":\"string\",\"nullable\":true},{\"name\":\"ducknng_protocol_version\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"row_schema_version\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"fetch_batch_chunks\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"serialization_mode\",\"type\":\"string\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    ducknng_method_query_open_handler
};

const ducknng_method_descriptor ducknng_method_fetch = {
    "fetch",
    "query",
    "Fetch the next row batch from an open session",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_RESPONSE_ROWS,
    DUCKNNG_SESSION_REQUIRES,
    0,
    DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM | DUCKNNG_RPC_FLAG_PAYLOAD_QUACK_BATCH | DUCKNNG_RPC_FLAG_END_OF_STREAM | DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    1024 * 1024,
    16 * 1024 * 1024,
    0,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    "{\"mode\":\"rows_or_control\",\"rows\":{\"default\":\"arrow_ipc_stream\",\"supported\":[\"arrow_ipc_stream\",\"ducknng_quack_batch\"],\"max_chunks_field\":\"fetch_batch_chunks\"},\"control\":{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"result_handle\",\"type\":\"string\",\"nullable\":true},{\"name\":\"batch_index\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}],\"ducknng_quack_batch\":{\"layout\":\"ducknng_quack_batch\",\"first_payload_fields\":[\"result_types\",\"result_names\",\"results\"],\"later_payload_fields\":[\"results\"]}}}",
    ducknng_method_fetch_handler
};

const ducknng_method_descriptor ducknng_method_close = {
    "close",
    "query",
    "Close an open or exhausted session",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_CLOSES,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON | DUCKNNG_RPC_FLAG_SESSION_CLOSED,
    1024 * 1024,
    1024 * 1024,
    0,
    1,
    0,
    1,
    1,
    1,
    0,
    0,
    1,
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"result_handle\",\"type\":\"string\",\"nullable\":true},{\"name\":\"batch_index\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    ducknng_method_close_handler
};

const ducknng_method_descriptor ducknng_method_cancel = {
    "cancel",
    "query",
    "Cancel and close an open session",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_CANCELS,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON | DUCKNNG_RPC_FLAG_CANCELLED | DUCKNNG_RPC_FLAG_SESSION_CLOSED,
    1024 * 1024,
    1024 * 1024,
    0,
    1,
    0,
    1,
    1,
    0,
    0,
    0,
    1,
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"result_handle\",\"type\":\"string\",\"nullable\":true},{\"name\":\"batch_index\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    ducknng_method_cancel_handler
};

const ducknng_method_descriptor ducknng_method_handshake = {
    "handshake",
    "control",
    "Negotiate protocol capabilities and preferred row serialization mode",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_STATELESS,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    1024 * 1024,
    1024 * 1024,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    1,
    "{\"type\":\"json\",\"fields\":[{\"name\":\"min_protocol_version\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"max_protocol_version\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"preferred_serialization_mode\",\"type\":\"string\",\"nullable\":true},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}]}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"server_name\",\"type\":\"string\",\"nullable\":false},{\"name\":\"server_version\",\"type\":\"string\",\"nullable\":false},{\"name\":\"duckdb_version\",\"type\":\"string\",\"nullable\":false},{\"name\":\"server_platform\",\"type\":\"string\",\"nullable\":false},{\"name\":\"protocol_version\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"selected_protocol_version\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"selected_serialization_mode\",\"type\":\"string\",\"nullable\":false},{\"name\":\"supported_serialization_modes\",\"type\":\"array<string>\",\"nullable\":false},{\"name\":\"correlation_id\",\"type\":\"string\",\"nullable\":true}],\"capabilities\":{\"fetch_metadata\":{\"correlation_id\":true,\"result_handle\":true,\"batch_index\":true},\"parameter_binding\":{\"encoding\":\"arrow_struct\",\"positional\":true,\"methods\":[\"exec\",\"query_open\",\"query_prepare\"],\"max_parameters\":65535}},\"note\":\"future reply metadata should be negotiated as a generic capability rather than a fake row serializer\"}",
    ducknng_method_handshake_handler
};

const ducknng_method_descriptor ducknng_method_manifest = {
    "manifest",
    "control",
    "Return the registry-derived manifest JSON",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_NONE,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_STATELESS,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    0,
    1024 * 1024,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    1,
    NULL,
    "{\"type\":\"json\"}",
    ducknng_method_manifest_handler
};

int ducknng_register_builtin_methods(ducknng_runtime *rt, char **errmsg) {
    const ducknng_method_descriptor *methods[] = {
        &ducknng_method_manifest,
        &ducknng_method_handshake,
        &ducknng_method_query_open,
        &ducknng_method_fetch,
        &ducknng_method_close,
        &ducknng_method_cancel,
        &ducknng_method_query_prepare
    };
    if (!rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for method registration");
        return 0;
    }
    return ducknng_method_registry_register_many(&rt->registry, methods,
        sizeof(methods) / sizeof(methods[0]), errmsg);
}

int ducknng_register_exec_method_with_auth(ducknng_runtime *rt, int requires_auth, char **errmsg) {
    if (!rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for exec method registration");
        return 0;
    }
    if (!ducknng_method_registry_register(&rt->registry, &ducknng_method_exec, errmsg)) return 0;
    if (!requires_auth) return 1;
    if (ducknng_method_registry_set_requires_auth(&rt->registry, ducknng_method_exec.name, 1, errmsg)) {
        return 1;
    }
    ducknng_method_registry_unregister(&rt->registry, ducknng_method_exec.name);
    return 0;
}

int ducknng_register_exec_method(ducknng_runtime *rt, char **errmsg) {
    return ducknng_register_exec_method_with_auth(rt, 0, errmsg);
}

int ducknng_register_upload_methods_with_auth(ducknng_runtime *rt, int requires_auth, char **errmsg) {
    const ducknng_method_descriptor *methods[] = {
        &ducknng_method_upload_open,
        &ducknng_method_upload_append,
        &ducknng_method_upload_commit,
        &ducknng_method_upload_abort
    };
    size_t i;
    unsigned char present[sizeof(methods) / sizeof(methods[0])] = {0};
    unsigned char orig_auth[sizeof(methods) / sizeof(methods[0])] = {0};
    if (!rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for upload method registration");
        return 0;
    }
    /* Idempotent, atomic (re-)registration. First snapshot each method's
     * original state -- whether it is already present, and its current auth flag
     * -- BEFORE any fallible operation, so a failure at any later point can
     * restore the original state exactly. Then register whichever methods are
     * missing (handles a fresh registry, a full re-register / auth flip, and a
     * partial family from an individual ducknng_unregister_method) and apply the
     * requested auth policy to all four. On any failure -- e.g. an OOM in a
     * register or an auth copy -- unregister the methods that were absent
     * originally (we added them) and restore the original auth flag of the ones
     * that were already present, so a failed call never leaves a partial family,
     * unauthenticated methods, or a downgraded/half-applied auth state exposed.
     * It never unregisters a method that was already present (which would bypass
     * the open-session guard), and set_requires_auth is copy-on-write, so it
     * never mutates the shared static descriptors. */
    for (i = 0; i < sizeof(methods) / sizeof(methods[0]); i++) {
        const ducknng_method_descriptor *m = ducknng_method_registry_find(&rt->registry,
            (const uint8_t *)methods[i]->name, (uint32_t)strlen(methods[i]->name));
        present[i] = m ? 1u : 0u;
        orig_auth[i] = (m && m->requires_auth) ? 1u : 0u;
    }
    for (i = 0; i < sizeof(methods) / sizeof(methods[0]); i++) {
        if (!present[i]) {
            if (!ducknng_method_registry_register(&rt->registry, methods[i], errmsg)) {
                goto rollback;
            }
        }
    }
    for (i = 0; i < sizeof(methods) / sizeof(methods[0]); i++) {
        if (!ducknng_method_registry_set_requires_auth(&rt->registry, methods[i]->name,
                requires_auth ? 1 : 0, errmsg)) {
            goto rollback;
        }
    }
    return 1;
rollback:
    for (i = 0; i < sizeof(methods) / sizeof(methods[0]); i++) {
        if (!present[i]) {
            /* Absent originally: remove it (a no-op if its own register failed). */
            ducknng_method_registry_unregister(&rt->registry, methods[i]->name);
        } else {
            /* Present originally: restore its original auth flag in place,
             * allocation-free. This cannot OOM (so rollback is truly atomic) and
             * never unregisters the method -- important because upload methods
             * are sessionful, and removing one out from under a live upload
             * session would bypass the open-session unregister guard. Any method
             * this call flipped is an owned copy (restored in place); one it left
             * untouched is already at its original value (no-op). */
            (void)ducknng_method_registry_restore_auth_inplace(&rt->registry,
                methods[i]->name, orig_auth[i]);
        }
    }
    return 0;
}

int ducknng_register_upload_methods(ducknng_runtime *rt, char **errmsg) {
    return ducknng_register_upload_methods_with_auth(rt, 0, errmsg);
}
