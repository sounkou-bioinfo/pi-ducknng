#pragma once
#include "ducknng_wire.h"
#include <stddef.h>
#include <stdint.h>

struct ducknng_service;

typedef enum ducknng_transport_pattern {
    DUCKNNG_TRANSPORT_REQREP = 1,
    DUCKNNG_TRANSPORT_PUBSUB = 2
} ducknng_transport_pattern;

typedef enum ducknng_payload_format {
    DUCKNNG_PAYLOAD_NONE = 0,
    DUCKNNG_PAYLOAD_JSON = 1,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM = 2
} ducknng_payload_format;

typedef enum ducknng_response_mode {
    DUCKNNG_RESPONSE_NONE = 0,
    DUCKNNG_RESPONSE_METADATA_ONLY = 1,
    DUCKNNG_RESPONSE_ROWS = 2,
    DUCKNNG_RESPONSE_METADATA_OR_ROWS = 3,
    DUCKNNG_RESPONSE_SESSION_OPEN = 4,
    DUCKNNG_RESPONSE_EVENT = 5
} ducknng_response_mode;

typedef enum ducknng_session_behavior {
    DUCKNNG_SESSION_STATELESS = 0,
    DUCKNNG_SESSION_OPENS = 1,
    DUCKNNG_SESSION_REQUIRES = 2,
    DUCKNNG_SESSION_CLOSES = 3,
    DUCKNNG_SESSION_CANCELS = 4
} ducknng_session_behavior;

typedef struct ducknng_request_context {
    const ducknng_frame *frame;
    const char *caller_identity;
    const char *auth_principal;
    const char *auth_claims_json;
    void *session;
} ducknng_request_context;

typedef struct ducknng_method_reply {
    uint8_t type;
    uint32_t flags;
    uint8_t *payload;
    size_t payload_len;
    char *error;
    int32_t status;
} ducknng_method_reply;

struct ducknng_method_descriptor;
typedef int (*ducknng_method_handler)(struct ducknng_service *svc,
    const struct ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply);

typedef struct ducknng_method_descriptor {
    const char *name;
    const char *family;
    const char *summary;
    ducknng_transport_pattern transport_pattern;
    ducknng_payload_format request_payload_format;
    ducknng_payload_format response_payload_format;
    ducknng_response_mode response_mode;
    ducknng_session_behavior session_behavior;
    uint32_t accepted_request_flags;
    uint32_t emitted_reply_flags;
    size_t max_request_bytes;
    size_t max_reply_bytes;
    int requires_auth;
    int requires_session;
    int opens_session;
    int closes_session;
    int mutates_state;
    int idempotent;
    int deprecated;
    int disabled;
    int version_introduced;
    const char *request_schema_json;
    const char *response_schema_json;
    ducknng_method_handler handler;
} ducknng_method_descriptor;

typedef struct ducknng_method_registry {
    const ducknng_method_descriptor **methods;
    unsigned char *owned;
    size_t method_count;
    size_t method_cap;
} ducknng_method_registry;

#define DUCKNNG_EXECUTION_MODEL_SHARED_SERIALIZED_CONNECTION "shared_serialized_connection"
#define DUCKNNG_PEER_IDENTITY_FORMAT_TLS "tls:san:<value>|tls:cn:<common-name>"

typedef struct ducknng_manifest_security {
    int tls_enabled;
    int tls_auth_mode;
    int peer_identity_required;
    int peer_allowlist_active;
    uint64_t peer_allowlist_count;
    int ip_allowlist_active;
    uint64_t ip_allowlist_count;
    int sql_authorizer_active;
    int sessions_bind_peer_identity_when_present;
    uint64_t session_idle_timeout_ms;
    uint64_t max_open_sessions;
    uint64_t max_sessions_per_peer_identity;
    const char *peer_identity_format;
    const char *execution_model;
} ducknng_manifest_security;

typedef struct ducknng_method_projection {
    const char *name;
    const char *family;
    const char *summary;
    const char *transport_pattern;
    const char *request_payload_format;
    const char *response_payload_format;
    const char *response_mode;
    const char *session_behavior;
    const char *request_schema_json;
    const char *response_schema_json;
    int requires_auth;
    int requires_session;
    int opens_session;
    int closes_session;
    int mutates_state;
    int idempotent;
    int deprecated;
    int disabled;
    uint32_t accepted_request_flags;
    uint32_t emitted_reply_flags;
    size_t max_request_bytes;
    size_t max_reply_bytes;
    int version_introduced;
} ducknng_method_projection;

int ducknng_method_registry_init(ducknng_method_registry *registry);
void ducknng_method_registry_destroy(ducknng_method_registry *registry);
int ducknng_method_registry_register(ducknng_method_registry *registry,
    const ducknng_method_descriptor *method, char **errmsg);
int ducknng_method_registry_register_many(ducknng_method_registry *registry,
    const ducknng_method_descriptor *const *methods, size_t n_methods, char **errmsg);
int ducknng_method_registry_unregister(ducknng_method_registry *registry, const char *name);
size_t ducknng_method_registry_unregister_family(ducknng_method_registry *registry, const char *family);
int ducknng_method_registry_set_requires_auth(ducknng_method_registry *registry,
    const char *name, int requires_auth, char **errmsg);
const ducknng_method_descriptor *ducknng_method_registry_find(
    const ducknng_method_registry *registry, const uint8_t *name, uint32_t name_len);
char *ducknng_method_registry_manifest_json(const ducknng_method_registry *registry,
    const char *server_name, const char *server_version, int protocol_version,
    const ducknng_manifest_security *security, char **errmsg);
void ducknng_method_descriptor_project(
    const ducknng_method_descriptor *method, ducknng_method_projection *projection);
void ducknng_method_reply_init(ducknng_method_reply *reply);
void ducknng_method_reply_reset(ducknng_method_reply *reply);
int ducknng_method_reply_set_payload(ducknng_method_reply *reply, uint8_t type, uint32_t flags,
    uint8_t *payload, size_t payload_len);
int ducknng_method_reply_set_error(ducknng_method_reply *reply, int32_t status, const char *message);
const char *ducknng_transport_pattern_name(ducknng_transport_pattern value);
const char *ducknng_payload_format_name(ducknng_payload_format value);
const char *ducknng_response_mode_name(ducknng_response_mode value);
const char *ducknng_session_behavior_name(ducknng_session_behavior value);
