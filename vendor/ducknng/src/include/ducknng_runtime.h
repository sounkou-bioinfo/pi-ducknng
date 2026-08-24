#pragma once
#include "duckdb_extension.h"
#include "ducknng_service.h"
#include "ducknng_thread.h"
#include "ducknng_registry.h"
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <nng/supplemental/http/http.h>

/* ---------------------------------------------------------------------------
 * DuckDB log entry ring buffer
 * --------------------------------------------------------------------------- */
#define DUCKNNG_LOG_RING_CAP 512

typedef struct ducknng_log_entry {
    duckdb_timestamp ts;
    char *level;
    char *log_type;
    char *message;
} ducknng_log_entry;

typedef struct ducknng_log_ring {
    ducknng_log_entry entries[DUCKNNG_LOG_RING_CAP];
    size_t head;   /* index of oldest entry */
    size_t count;  /* number of valid entries (0..DUCKNNG_LOG_RING_CAP) */
    ducknng_mutex mu;
    int mu_initialized;
} ducknng_log_ring;

typedef struct ducknng_client_socket {
    uint64_t socket_id;
    char *protocol;
    char *url;
    char *listen_url;
    nng_socket sock;
    nng_ctx ctx;
    nng_listener listener;
    int open;
    int connected;
    int has_ctx;
    int has_listener;
    int send_timeout_ms;
    int recv_timeout_ms;
    ducknng_mutex mu;
    ducknng_cond cv;
    uint32_t refcount;
    int closing;
    int mu_initialized;
    int cv_initialized;
    uint8_t *pending_request;
    size_t pending_request_len;
    uint8_t *pending_reply;
    size_t pending_reply_len;
} ducknng_client_socket;

typedef struct ducknng_tls_config {
    uint64_t tls_config_id;
    char *source;
    ducknng_tls_opts opts;
} ducknng_tls_config;

enum ducknng_client_aio_phase {
    DUCKNNG_CLIENT_AIO_PHASE_SEND = 1,
    DUCKNNG_CLIENT_AIO_PHASE_RECV = 2,
    DUCKNNG_CLIENT_AIO_PHASE_HTTP = 3
};

enum ducknng_client_aio_kind {
    DUCKNNG_CLIENT_AIO_KIND_REQUEST = 1,
    DUCKNNG_CLIENT_AIO_KIND_SEND = 2,
    DUCKNNG_CLIENT_AIO_KIND_RECV = 3,
    DUCKNNG_CLIENT_AIO_KIND_NCURL = 4
};

enum ducknng_client_aio_state {
    DUCKNNG_CLIENT_AIO_PENDING = 0,
    DUCKNNG_CLIENT_AIO_READY = 1,
    DUCKNNG_CLIENT_AIO_ERROR = 2,
    DUCKNNG_CLIENT_AIO_CANCELLED = 3,
    DUCKNNG_CLIENT_AIO_COLLECTED = 4
};

typedef struct ducknng_client_aio {
    struct ducknng_runtime *rt;
    uint64_t aio_id;
    uint64_t socket_id;
    ducknng_client_socket *socket_ref;
    nng_socket sock;
    nng_ctx ctx;
    nng_aio *aio;
    nng_msg *reply_msg;
    nng_url *http_url;
    nng_http_client *http_client;
    nng_http_req *http_req;
    nng_http_res *http_res;
    uint16_t http_status;
    char *http_headers_json;
    uint8_t *http_body;
    size_t http_body_len;
    char *http_body_text;
    int owns_socket;
    int open;
    int has_ctx;
    int kind;
    int phase;
    int state;
    int timeout_ms;
    int send_done;
    int recv_done;
    int send_result;
    int recv_result;
    uint64_t started_ms;
    uint64_t finished_ms;
    char *error;
} ducknng_client_aio;

typedef struct ducknng_user_codec {
    char *content_type; /* lower-cased, trimmed */
    char *function_name; /* DuckDB scalar function name */
} ducknng_user_codec;

typedef struct ducknng_http_profile {
    char *profile_id;
    char *scheme;
    char *host;
    uint16_t port;
    int has_port;
    char *path_prefix;
    char *method;
    int tls_required;
    char *auth_header_name;
    char *auth_header_value; /* secret: never exposed by introspection */
    uint64_t version;
    uint64_t created_ms;
    uint64_t updated_ms;
    uint64_t expires_at_ms;
} ducknng_http_profile;

typedef struct ducknng_http_profile_info {
    char *profile_id;
    char *scheme;
    char *host;
    uint16_t port;
    int has_port;
    char *path_prefix;
    char *method;
    int tls_required;
    char *auth_header_names_json;
    uint64_t version;
    uint64_t created_ms;
    uint64_t updated_ms;
    uint64_t expires_at_ms;
} ducknng_http_profile_info;

typedef struct ducknng_runtime {
    duckdb_database db;
    duckdb_connection init_con;
    duckdb_connection codec_con;   /* dedicated connection for body codec SQL */
    ducknng_mutex mu;
    ducknng_mutex init_con_mu;
    ducknng_mutex codec_con_mu;    /* serializes codec_con queries */
    ducknng_mutex execution_pool_mu;
    ducknng_cond aio_cv;
    int aio_cv_initialized;
    int init_con_mu_initialized;
    int codec_con_mu_initialized;  /* 1 after codec_con_mu is ready */
    int execution_pool_mu_initialized;
    duckdb_connection *execution_pool;
    int *execution_pool_busy;
    size_t execution_pool_count;
    ducknng_service **services;
    size_t service_count;
    size_t service_cap;
    ducknng_client_socket **client_sockets;
    size_t client_socket_count;
    size_t client_socket_cap;
    ducknng_client_aio **client_aios;
    size_t client_aio_count;
    size_t client_aio_cap;
    ducknng_tls_config **tls_configs;
    size_t tls_config_count;
    size_t tls_config_cap;
    ducknng_user_codec *user_codecs;
    size_t user_codec_count;
    size_t user_codec_cap;
    ducknng_http_profile *http_profiles;
    size_t http_profile_count;
    size_t http_profile_cap;
    uint64_t next_service_id;
    uint64_t next_client_socket_id;
    uint64_t next_client_aio_id;
    uint64_t next_tls_config_id;
    uint64_t next_http_profile_version;
    int shutting_down;
    int log_capture_enabled; /* 1 after ducknng_enable_log_capture() succeeds */
    atomic_uintptr_t current_request_service_ptr;
    ducknng_method_registry registry;
    ducknng_log_ring log_ring;
} ducknng_runtime;

/* User codec helpers. Registry is keyed by lower-cased trimmed content_type.
 * register: upserts (content_type -> function_name); returns 1 on success.
 * unregister: removes; returns 1 if an entry was removed.
 * find: returns a strdup-ed function_name (caller frees) or NULL. */
int ducknng_runtime_register_user_codec(ducknng_runtime *rt, const char *content_type,
    const char *function_name, char **errmsg);
int ducknng_runtime_unregister_user_codec(ducknng_runtime *rt, const char *content_type);
char *ducknng_runtime_find_user_codec(ducknng_runtime *rt, const char *content_type);

int ducknng_runtime_upsert_http_profile(ducknng_runtime *rt, const char *profile_id,
    const char *scheme, const char *host, int32_t port, int has_port,
    const char *path_prefix, const char *method, int tls_required,
    const char *auth_header_name, const char *auth_header_value,
    uint64_t expires_at_ms, char **errmsg);
int ducknng_runtime_drop_http_profile(ducknng_runtime *rt, const char *profile_id);
int ducknng_runtime_http_profiles_snapshot(ducknng_runtime *rt,
    ducknng_http_profile_info **out_profiles, size_t *out_count, char **errmsg);
void ducknng_runtime_http_profiles_snapshot_free(ducknng_http_profile_info *profiles, size_t count);
void ducknng_runtime_http_profiles_reset(ducknng_runtime *rt);
int ducknng_runtime_resolve_http_profile_headers(ducknng_runtime *rt,
    const char *profile_id, const char *url, const char *method,
    const char *headers_json, char **out_headers_json, char **errmsg);

int ducknng_runtime_init(duckdb_connection connection, duckdb_extension_info info,
    struct duckdb_extension_access *access, ducknng_runtime **out_rt, int *out_created);
void ducknng_runtime_destroy(ducknng_runtime *rt);
ducknng_service *ducknng_runtime_find_service(ducknng_runtime *rt, const char *name);
int ducknng_runtime_add_service(ducknng_runtime *rt, ducknng_service *svc, char **errmsg);
ducknng_service *ducknng_runtime_remove_service(ducknng_runtime *rt, const char *name);
ducknng_client_socket *ducknng_runtime_find_client_socket(ducknng_runtime *rt, uint64_t socket_id);
ducknng_client_socket *ducknng_runtime_acquire_client_socket(ducknng_runtime *rt, uint64_t socket_id);
void ducknng_runtime_release_client_socket(ducknng_client_socket *sock);
void ducknng_client_socket_destroy(ducknng_client_socket *sock);
int ducknng_runtime_add_client_socket(ducknng_runtime *rt, ducknng_client_socket *sock, char **errmsg);
ducknng_client_socket *ducknng_runtime_remove_client_socket(ducknng_runtime *rt, uint64_t socket_id);
int ducknng_runtime_add_client_aio(ducknng_runtime *rt, ducknng_client_aio *aio, char **errmsg);
ducknng_client_aio *ducknng_runtime_remove_client_aio(ducknng_runtime *rt, uint64_t aio_id);
void ducknng_client_aio_destroy(ducknng_client_aio *aio);
ducknng_tls_config *ducknng_runtime_find_tls_config(ducknng_runtime *rt, uint64_t tls_config_id);
int ducknng_runtime_add_tls_config(ducknng_runtime *rt, ducknng_tls_config *cfg, char **errmsg);
ducknng_tls_config *ducknng_runtime_remove_tls_config(ducknng_runtime *rt, uint64_t tls_config_id);
duckdb_connection ducknng_runtime_execution_connection(ducknng_runtime *rt);
const char *ducknng_runtime_execution_model(ducknng_runtime *rt);
void ducknng_runtime_execution_lane_lock(ducknng_runtime *rt);
void ducknng_runtime_execution_lane_unlock(ducknng_runtime *rt);
void ducknng_runtime_init_con_lock(ducknng_runtime *rt);
void ducknng_runtime_init_con_unlock(ducknng_runtime *rt);
duckdb_connection ducknng_runtime_codec_connection(ducknng_runtime *rt);
void ducknng_runtime_codec_connection_lock(ducknng_runtime *rt);
void ducknng_runtime_codec_connection_unlock(ducknng_runtime *rt);
void ducknng_runtime_current_request_service_set(ducknng_runtime *rt, ducknng_service *svc);
ducknng_service *ducknng_runtime_current_request_service_get(ducknng_runtime *rt);
ducknng_service *ducknng_runtime_current_thread_request_service_get(ducknng_runtime *rt);
void ducknng_runtime_current_http_request_context_set(ducknng_runtime *rt,
    const ducknng_http_request_context *request_ctx);
const ducknng_http_request_context *ducknng_runtime_current_thread_http_request_context_get(
    ducknng_runtime *rt);
void ducknng_runtime_current_authorizer_context_set(ducknng_runtime *rt,
    const ducknng_authorizer_context *auth_ctx);
const ducknng_authorizer_context *ducknng_runtime_current_thread_authorizer_context_get(ducknng_runtime *rt);

/* Log ring helpers */
void ducknng_log_ring_init(ducknng_log_ring *ring);
void ducknng_log_ring_destroy(ducknng_log_ring *ring);
void ducknng_log_ring_append(ducknng_log_ring *ring, const duckdb_timestamp *ts,
    const char *level, const char *log_type, const char *message);
/* DuckDB log storage write callback (duckdb_logger_write_log_entry_t-compatible).
 * extra_data must be a ducknng_runtime *. */
void ducknng_log_write_entry(void *extra_data, duckdb_timestamp *timestamp,
    const char *level, const char *log_type, const char *log_message);
/* Snapshot: caller provides arrays of at least DUCKNNG_LOG_RING_CAP entries.
 * Returns the number of entries written. Caller must free each non-NULL
 * level/log_type/message string with duckdb_free. */
size_t ducknng_log_ring_snapshot(ducknng_log_ring *ring,
    duckdb_timestamp *out_ts, char **out_level, char **out_log_type, char **out_message);
