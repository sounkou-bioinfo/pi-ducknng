#include "ducknng_registry.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_registry_reserve(ducknng_method_registry *registry, size_t want, char **errmsg) {
    const ducknng_method_descriptor **next_methods;
    unsigned char *next_owned;
    size_t next_cap = registry->method_cap ? registry->method_cap * 2 : 8;
    if (registry->method_cap >= want) return 1;
    while (next_cap < want) next_cap *= 2;
    next_methods = (const ducknng_method_descriptor **)duckdb_malloc(sizeof(*next_methods) * next_cap);
    next_owned = (unsigned char *)duckdb_malloc(sizeof(*next_owned) * next_cap);
    if (!next_methods || !next_owned) {
        if (next_methods) duckdb_free(next_methods);
        if (next_owned) duckdb_free(next_owned);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing method registry");
        return 0;
    }
    memset(next_methods, 0, sizeof(*next_methods) * next_cap);
    memset(next_owned, 0, sizeof(*next_owned) * next_cap);
    if (registry->methods && registry->method_count) {
        memcpy(next_methods, registry->methods, sizeof(*next_methods) * registry->method_count);
        duckdb_free(registry->methods);
    }
    if (registry->owned && registry->method_count) {
        memcpy(next_owned, registry->owned, sizeof(*next_owned) * registry->method_count);
        duckdb_free(registry->owned);
    }
    registry->methods = next_methods;
    registry->owned = next_owned;
    registry->method_cap = next_cap;
    return 1;
}

static int ducknng_method_name_exists(const ducknng_method_registry *registry, const char *name) {
    size_t i;
    if (!registry || !name) return 0;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (method && method->name && strcmp(method->name, name) == 0) return 1;
    }
    return 0;
}

static int ducknng_method_descriptor_valid(const ducknng_method_descriptor *method) {
    return method && method->name && method->name[0] && method->handler;
}

static int ducknng_method_name_exists_in_batch(
    const ducknng_method_descriptor *const *methods, size_t n_methods, size_t upto, const char *name) {
    size_t i;
    if (!methods || !name) return 0;
    for (i = 0; i < upto; i++) {
        if (methods[i] && methods[i]->name && strcmp(methods[i]->name, name) == 0) return 1;
    }
    return 0;
}

static void ducknng_registry_remove_at(ducknng_method_registry *registry, size_t idx) {
    if (!registry || idx >= registry->method_count) return;
    if (registry->owned && registry->owned[idx] && registry->methods && registry->methods[idx]) {
        duckdb_free((void *)registry->methods[idx]);
    }
    for (; idx + 1 < registry->method_count; idx++) {
        registry->methods[idx] = registry->methods[idx + 1];
        if (registry->owned) registry->owned[idx] = registry->owned[idx + 1];
    }
    registry->method_count--;
    registry->methods[registry->method_count] = NULL;
    if (registry->owned) registry->owned[registry->method_count] = 0;
}

int ducknng_method_registry_init(ducknng_method_registry *registry) {
    if (!registry) return 0;
    memset(registry, 0, sizeof(*registry));
    return 1;
}

void ducknng_method_registry_destroy(ducknng_method_registry *registry) {
    size_t i;
    if (!registry) return;
    if (registry->methods && registry->owned) {
        for (i = 0; i < registry->method_count; i++) {
            if (registry->owned[i] && registry->methods[i]) duckdb_free((void *)registry->methods[i]);
        }
    }
    if (registry->methods) duckdb_free(registry->methods);
    if (registry->owned) duckdb_free(registry->owned);
    memset(registry, 0, sizeof(*registry));
}

int ducknng_method_registry_register(ducknng_method_registry *registry,
    const ducknng_method_descriptor *method, char **errmsg) {
    if (!registry || !ducknng_method_descriptor_valid(method)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid method descriptor");
        return 0;
    }
    if (ducknng_method_name_exists(registry, method->name)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: method already registered");
        return 0;
    }
    if (!ducknng_registry_reserve(registry, registry->method_count + 1, errmsg)) return 0;
    registry->methods[registry->method_count] = method;
    if (registry->owned) registry->owned[registry->method_count] = 0;
    registry->method_count++;
    return 1;
}

int ducknng_method_registry_register_many(ducknng_method_registry *registry,
    const ducknng_method_descriptor *const *methods, size_t n_methods, char **errmsg) {
    size_t i;
    if (!registry || (!methods && n_methods > 0)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid bulk method registration request");
        return 0;
    }
    for (i = 0; i < n_methods; i++) {
        if (!ducknng_method_descriptor_valid(methods[i])) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid method descriptor in bulk registration");
            return 0;
        }
        if (ducknng_method_name_exists(registry, methods[i]->name) ||
            ducknng_method_name_exists_in_batch(methods, n_methods, i, methods[i]->name)) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: duplicate method name in bulk registration");
            return 0;
        }
    }
    if (!ducknng_registry_reserve(registry, registry->method_count + n_methods, errmsg)) return 0;
    for (i = 0; i < n_methods; i++) {
        registry->methods[registry->method_count] = methods[i];
        if (registry->owned) registry->owned[registry->method_count] = 0;
        registry->method_count++;
    }
    return 1;
}

int ducknng_method_registry_unregister(ducknng_method_registry *registry, const char *name) {
    size_t i;
    if (!registry || !name || !name[0]) return 0;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (method && method->name && strcmp(method->name, name) == 0) {
            ducknng_registry_remove_at(registry, i);
            return 1;
        }
    }
    return 0;
}

size_t ducknng_method_registry_unregister_family(ducknng_method_registry *registry, const char *family) {
    size_t i;
    size_t removed = 0;
    if (!registry || !family || !family[0]) return 0;
    for (i = 0; i < registry->method_count;) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (method && method->family && strcmp(method->family, family) == 0) {
            ducknng_registry_remove_at(registry, i);
            removed++;
            continue;
        }
        i++;
    }
    return removed;
}

int ducknng_method_registry_set_requires_auth(ducknng_method_registry *registry,
    const char *name, int requires_auth, char **errmsg) {
    size_t i;
    ducknng_method_descriptor *copy;
    if (!registry || !name || !name[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: method name is required");
        return 0;
    }
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (!method || !method->name || strcmp(method->name, name) != 0) continue;
        requires_auth = requires_auth ? 1 : 0;
        if (method->requires_auth == requires_auth) return 1;
        copy = (ducknng_method_descriptor *)duckdb_malloc(sizeof(*copy));
        if (!copy) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory updating method auth policy");
            return 0;
        }
        memcpy(copy, method, sizeof(*copy));
        copy->requires_auth = requires_auth;
        if (registry->owned && registry->owned[i] && registry->methods[i]) duckdb_free((void *)registry->methods[i]);
        registry->methods[i] = copy;
        if (registry->owned) registry->owned[i] = 1;
        return 1;
    }
    if (errmsg) *errmsg = ducknng_strdup("ducknng: method not found");
    return 0;
}

/* Allocation-free auth restore for rollback. If the slot is an owned heap copy,
 * flip requires_auth in place (no malloc, no descriptor swap, no unregister); if
 * it already holds the target value it is a no-op. This lets a failed
 * multi-method (re-)registration restore each preexisting method's original auth
 * policy without risking a second OOM and without removing a method that may
 * have live sessions. A static (unowned) slot cannot be mutated, but a static
 * slot was never flipped by the caller, so it already holds its original value
 * and the early no-op path covers it. Returns 1 when the flag is at the target,
 * 0 only if the name is absent or a static slot genuinely differs (which the
 * caller's pre-registration snapshot guarantees will not happen). */
int ducknng_method_registry_restore_auth_inplace(ducknng_method_registry *registry,
    const char *name, int requires_auth) {
    size_t i;
    if (!registry || !name || !name[0]) return 0;
    requires_auth = requires_auth ? 1 : 0;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (!method || !method->name || strcmp(method->name, name) != 0) continue;
        if (method->requires_auth == requires_auth) return 1;
        if (registry->owned && registry->owned[i]) {
            ((ducknng_method_descriptor *)method)->requires_auth = requires_auth;
            return 1;
        }
        return 0;
    }
    return 0;
}

const ducknng_method_descriptor *ducknng_method_registry_find(
    const ducknng_method_registry *registry, const uint8_t *name, uint32_t name_len) {
    size_t i;
    if (!registry || !name) return NULL;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        size_t n;
        if (!method || !method->name) continue;
        n = strlen(method->name);
        if ((uint32_t)n == name_len && memcmp(method->name, name, n) == 0) return method;
    }
    return NULL;
}

void ducknng_method_reply_init(ducknng_method_reply *reply) {
    if (!reply) return;
    memset(reply, 0, sizeof(*reply));
    reply->type = DUCKNNG_RPC_RESULT;
    reply->status = DUCKNNG_STATUS_OK;
}

static void ducknng_method_reply_release_body(ducknng_method_reply *reply) {
    if (reply->payload) duckdb_free(reply->payload);
    if (reply->prebuilt_msg) nng_msg_free(reply->prebuilt_msg);
    reply->payload = NULL;
    reply->prebuilt_msg = NULL;
    reply->payload_len = 0;
    reply->prefix_size = 0;
}

void ducknng_method_reply_reset(ducknng_method_reply *reply) {
    if (!reply) return;
    ducknng_method_reply_release_body(reply);
    if (reply->error) duckdb_free(reply->error);
    ducknng_method_reply_init(reply);
}

int ducknng_method_reply_set_payload(ducknng_method_reply *reply, uint8_t type, uint32_t flags,
    uint8_t *payload, size_t payload_len) {
    if (!reply) return 0;
    ducknng_method_reply_release_body(reply);
    reply->type = type;
    reply->flags = flags;
    reply->payload = payload;
    reply->payload_len = payload_len;
    reply->status = DUCKNNG_STATUS_OK;
    return 1;
}

int ducknng_method_reply_set_prebuilt_message(ducknng_method_reply *reply,
    uint8_t type, uint32_t flags, nng_msg *msg, size_t prefix_size,
    size_t payload_len) {
    if (!reply || !msg) return 0;
    ducknng_method_reply_release_body(reply);
    reply->type = type;
    reply->flags = flags;
    reply->prebuilt_msg = msg;
    reply->payload_len = payload_len;
    reply->prefix_size = prefix_size;
    reply->status = DUCKNNG_STATUS_OK;
    return 1;
}

int ducknng_method_reply_set_error(ducknng_method_reply *reply, int32_t status, const char *message) {
    if (!reply) return 0;
    ducknng_method_reply_release_body(reply);
    if (reply->error) {
        duckdb_free(reply->error);
        reply->error = NULL;
    }
    reply->type = DUCKNNG_RPC_ERROR;
    reply->flags = 0;
    reply->status = status;
    reply->error = ducknng_strdup(message ? message : "ducknng: unspecified error");
    return reply->error != NULL;
}

const char *ducknng_transport_pattern_name(ducknng_transport_pattern value) {
    switch (value) {
        case DUCKNNG_TRANSPORT_REQREP: return "reqrep";
        case DUCKNNG_TRANSPORT_PUBSUB: return "pubsub";
        default: return "unknown";
    }
}

const char *ducknng_payload_format_name(ducknng_payload_format value) {
    switch (value) {
        case DUCKNNG_PAYLOAD_NONE: return "none";
        case DUCKNNG_PAYLOAD_JSON: return "json";
        case DUCKNNG_PAYLOAD_ARROW_IPC_STREAM: return "arrow_ipc_stream";
        case DUCKNNG_PAYLOAD_DUCKNNG_QUACK_BATCH: return "ducknng_quack_batch";
        case DUCKNNG_PAYLOAD_UPLOAD_APPEND: return "ducknng_upload_append";
        default: return "unknown";
    }
}

const char *ducknng_response_mode_name(ducknng_response_mode value) {
    switch (value) {
        case DUCKNNG_RESPONSE_NONE: return "none";
        case DUCKNNG_RESPONSE_METADATA_ONLY: return "metadata_only";
        case DUCKNNG_RESPONSE_ROWS: return "rows";
        case DUCKNNG_RESPONSE_METADATA_OR_ROWS: return "metadata_or_rows";
        case DUCKNNG_RESPONSE_SESSION_OPEN: return "session_open";
        case DUCKNNG_RESPONSE_EVENT: return "event";
        default: return "unknown";
    }
}

const char *ducknng_session_behavior_name(ducknng_session_behavior value) {
    switch (value) {
        case DUCKNNG_SESSION_STATELESS: return "stateless";
        case DUCKNNG_SESSION_OPENS: return "opens_session";
        case DUCKNNG_SESSION_REQUIRES: return "requires_session";
        case DUCKNNG_SESSION_CLOSES: return "closes_session";
        case DUCKNNG_SESSION_CANCELS: return "cancels_session";
        default: return "unknown";
    }
}

void ducknng_method_descriptor_project(
    const ducknng_method_descriptor *method, ducknng_method_projection *projection) {
    if (!projection) return;
    memset(projection, 0, sizeof(*projection));
    if (!method) return;
    projection->name = method->name ? method->name : "";
    projection->family = method->family ? method->family : "";
    projection->summary = method->summary ? method->summary : "";
    projection->transport_pattern = ducknng_transport_pattern_name(method->transport_pattern);
    projection->request_payload_format = ducknng_payload_format_name(method->request_payload_format);
    projection->response_payload_format = ducknng_payload_format_name(method->response_payload_format);
    projection->response_mode = ducknng_response_mode_name(method->response_mode);
    projection->session_behavior = ducknng_session_behavior_name(method->session_behavior);
    projection->request_schema_json = method->request_schema_json;
    projection->response_schema_json = method->response_schema_json;
    projection->requires_auth = method->requires_auth;
    projection->requires_session = method->requires_session;
    projection->opens_session = method->opens_session;
    projection->closes_session = method->closes_session;
    projection->mutates_state = method->mutates_state;
    projection->idempotent = method->idempotent;
    projection->deprecated = method->deprecated;
    projection->disabled = method->disabled;
    projection->accepted_request_flags = method->accepted_request_flags;
    projection->emitted_reply_flags = method->emitted_reply_flags;
    projection->max_request_bytes = method->max_request_bytes;
    projection->max_reply_bytes = method->max_reply_bytes;
    projection->version_introduced = method->version_introduced;
}

static int append_text(char **buf, size_t *len, size_t *cap, const char *text) {
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
    *cap = *cap ? *cap * 2 : 1024;
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

static int append_json_string(char **buf, size_t *len, size_t *cap, const char *text) {
    const char *p = text ? text : "";
    if (!append_text(buf, len, cap, "\"")) return 0;
    while (*p) {
        char esc[3] = {0, 0, 0};
        if (*p == '\\' || *p == '"') {
            esc[0] = '\\';
            esc[1] = *p;
            if (!append_text(buf, len, cap, esc)) return 0;
        } else if ((unsigned char)*p < 0x20) {
            char hex[7];
            snprintf(hex, sizeof(hex), "\\u%04x", (unsigned int)(unsigned char)*p);
            if (!append_text(buf, len, cap, hex)) return 0;
        } else {
            char one[2] = {*p, 0};
            if (!append_text(buf, len, cap, one)) return 0;
        }
        p++;
    }
    return append_text(buf, len, cap, "\"");
}

static int append_parameter_binding_capability(char **buf, size_t *len,
    size_t *cap, const ducknng_method_registry *registry) {
    static const char *method_names[] = {"exec", "query_open", "query_prepare"};
    size_t i;
    int emitted = 0;
    if (!append_text(buf, len, cap,
            "\"parameter_binding\":{\"encoding\":\"arrow_struct\",\"positional\":true,\"methods\":[")) return 0;
    for (i = 0; i < sizeof(method_names) / sizeof(method_names[0]); i++) {
        const char *name = method_names[i];
        if (!ducknng_method_registry_find(registry, (const uint8_t *)name,
                (uint32_t)strlen(name))) continue;
        if (emitted && !append_text(buf, len, cap, ",")) return 0;
        if (!append_json_string(buf, len, cap, name)) return 0;
        emitted = 1;
    }
    return append_text(buf, len, cap, "],\"max_parameters\":65535}");
}

char *ducknng_method_registry_manifest_json(const ducknng_method_registry *registry,
    const char *server_name, const char *server_version, int protocol_version,
    const ducknng_manifest_security *security, char **errmsg) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    size_t i;
    char numbuf[512];
    if (!registry) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing method registry");
        return NULL;
    }
    if (!append_text(&buf, &len, &cap, "{\"server\":{\"name\":")) goto oom;
    if (!append_json_string(&buf, &len, &cap, server_name ? server_name : "ducknng")) goto oom;
    if (!append_text(&buf, &len, &cap, ",\"version\":")) goto oom;
    if (!append_json_string(&buf, &len, &cap, server_version ? server_version : DUCKNNG_VERSION)) goto oom;
    if (!append_text(&buf, &len, &cap, ",\"protocol_version\":")) goto oom;
    snprintf(numbuf, sizeof(numbuf), "%d", protocol_version);
    if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
    if (security) {
        if (!append_text(&buf, &len, &cap, ",\"security\":{\"tls_enabled\":")) goto oom;
        if (!append_text(&buf, &len, &cap, security->tls_enabled ? "true" : "false")) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"tls_auth_mode\":%d", security->tls_auth_mode);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"peer_identity_required\":")) goto oom;
        if (!append_text(&buf, &len, &cap, security->peer_identity_required ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"peer_allowlist_active\":")) goto oom;
        if (!append_text(&buf, &len, &cap, security->peer_allowlist_active ? "true" : "false")) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"peer_allowlist_count\":%llu",
            (unsigned long long)security->peer_allowlist_count);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"ip_allowlist_active\":")) goto oom;
        if (!append_text(&buf, &len, &cap, security->ip_allowlist_active ? "true" : "false")) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"ip_allowlist_count\":%llu",
            (unsigned long long)security->ip_allowlist_count);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"sql_authorizer_active\":")) goto oom;
        if (!append_text(&buf, &len, &cap, security->sql_authorizer_active ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"peer_identity_format\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap,
                security->peer_identity_format ? security->peer_identity_format : DUCKNNG_PEER_IDENTITY_FORMAT_TLS)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"sessions_bind_peer_identity_when_present\":")) goto oom;
        if (!append_text(&buf, &len, &cap,
                security->sessions_bind_peer_identity_when_present ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, "}")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"execution\":{\"model\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap,
                security->execution_model ? security->execution_model : DUCKNNG_EXECUTION_MODEL_SHARED_SERIALIZED_CONNECTION)) goto oom;
        if (!append_text(&buf, &len, &cap, "}")) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"session_policy\":{\"idle_timeout_ms\":%llu,\"idle_timeout_owner\":\"server\",\"max_open_sessions\":%llu,\"max_open_sessions_owner\":\"server\",\"max_sessions_per_peer_identity\":%llu,\"max_sessions_per_peer_identity_owner\":\"server\"}",
            (unsigned long long)security->session_idle_timeout_ms,
            (unsigned long long)security->max_open_sessions,
            (unsigned long long)security->max_sessions_per_peer_identity);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
    }
    if (!append_text(&buf, &len, &cap,
            "},\"capabilities\":{\"handshake\":true,\"supported_serialization_modes\":[\"arrow_ipc_stream\",\"ducknng_quack_batch\"],\"fetch_metadata\":{\"correlation_id\":true,\"result_handle\":true,\"batch_index\":true},\"session_schema\":{\"ducknng_protocol_version\":true,\"row_schema_version\":true,\"fetch_batch_chunks\":true},")) goto oom;
    if (!append_parameter_binding_capability(&buf, &len, &cap, registry)) goto oom;
    if (!append_text(&buf, &len, &cap, "},\"methods\":[")) goto oom;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *m = registry->methods[i];
        ducknng_method_projection view;
        if (!m) continue;
        ducknng_method_descriptor_project(m, &view);
        if (i > 0 && !append_text(&buf, &len, &cap, ",")) goto oom;
        if (!append_text(&buf, &len, &cap, "{")) goto oom;
        if (!append_text(&buf, &len, &cap, "\"name\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.name)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"family\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.family)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"summary\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.summary)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"transport_pattern\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.transport_pattern)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"request_payload_format\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.request_payload_format)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"response_payload_format\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.response_payload_format)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"response_mode\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.response_mode)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"session_behavior\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, view.session_behavior)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"requires_auth\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.requires_auth ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"requires_session\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.requires_session ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"opens_session\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.opens_session ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"closes_session\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.closes_session ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"mutates_state\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.mutates_state ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"idempotent\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.idempotent ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"deprecated\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.deprecated ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"disabled\":")) goto oom;
        if (!append_text(&buf, &len, &cap, view.disabled ? "true" : "false")) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"accepted_request_flags\":%u",
            (unsigned int)view.accepted_request_flags);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"emitted_reply_flags\":%u",
            (unsigned int)view.emitted_reply_flags);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"max_request_bytes\":%llu",
            (unsigned long long)view.max_request_bytes);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"max_reply_bytes\":%llu",
            (unsigned long long)view.max_reply_bytes);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"version_introduced\":%d",
            view.version_introduced);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"request_schema\":")) goto oom;
        if (view.request_schema_json) {
            if (!append_text(&buf, &len, &cap, view.request_schema_json)) goto oom;
        } else if (!append_text(&buf, &len, &cap, "null")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"response_schema\":")) goto oom;
        if (view.response_schema_json) {
            if (!append_text(&buf, &len, &cap, view.response_schema_json)) goto oom;
        } else if (!append_text(&buf, &len, &cap, "null")) goto oom;
        if (!append_text(&buf, &len, &cap, "}")) goto oom;
    }
    if (!append_text(&buf, &len, &cap, "]}")) goto oom;
    return buf;

oom:
    if (buf) duckdb_free(buf);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory generating manifest json");
    return NULL;
}
