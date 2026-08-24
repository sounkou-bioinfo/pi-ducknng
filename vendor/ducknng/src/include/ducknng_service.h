#pragma once
#include "ducknng_http_compat.h"
#include "ducknng_nng_compat.h"
#include "ducknng_session.h"
#include "ducknng_thread.h"
#include "ducknng_transport.h"
#include "ducknng_wire.h"
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

typedef struct ducknng_runtime ducknng_runtime;
typedef struct ducknng_service ducknng_service;
typedef struct ducknng_rep_ctx ducknng_rep_ctx;
typedef struct ducknng_manifest_security ducknng_manifest_security;

typedef struct ducknng_ip_allow_rule {
    int family;
    uint8_t addr[16];
    uint8_t prefix_bits;
} ducknng_ip_allow_rule;

typedef struct ducknng_authorizer_context {
    ducknng_service *svc;
    const ducknng_frame *frame;
    ducknng_transport_family transport_family;
    ducknng_transport_scheme scheme;
    const char *phase;
    const char *caller_identity;
    const nng_sockaddr *remote_addr;
    const char *http_method;
    const char *http_path;
    const char *content_type;
    uint64_t body_bytes;
} ducknng_authorizer_context;

typedef struct ducknng_authorizer_decision {
    int allow;
    int http_status;
    char *reason;
    char *principal;
    char *claims_json;
    uint64_t cache_ttl_ms;
} ducknng_authorizer_decision;

typedef struct ducknng_pipe_event {
    uint64_t seq;
    uint64_t ts_ms;
    uint64_t pipe_id;
    char *event;
    int admitted;
    char *reason;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    char *peer_identity;
} ducknng_pipe_event;

typedef struct ducknng_pipe_state {
    uint64_t pipe_id;
    uint64_t opened_ms;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    char *peer_identity;
} ducknng_pipe_state;

typedef struct ducknng_pipe_monitor_stats {
    uint64_t event_capacity;
    uint64_t event_count;
    uint64_t oldest_seq;
    uint64_t newest_seq;
    uint64_t dropped_events;
    uint64_t active_pipes;
    uint64_t max_active_pipes;
} ducknng_pipe_monitor_stats;

enum {
    DUCKNNG_HTTP_ROUTE_MATCH_EXACT = 1,
    DUCKNNG_HTTP_ROUTE_MATCH_PREFIX = 2,
    DUCKNNG_HTTP_ROUTE_MATCH_TEMPLATE = 3
};

enum {
    DUCKNNG_HTTP_ROUTE_RESPONSE_ONESHOT = 0,  /* buffer full body then send (default) */
    DUCKNNG_HTTP_ROUTE_RESPONSE_STREAM = 1    /* hijack conn, write chunked rows */
};

enum {
    DUCKNNG_EXECUTION_SHARED_SERIALIZED_CONNECTION = 1,
    DUCKNNG_EXECUTION_SERVICE_SERIALIZED_CONNECTION = 2,
    DUCKNNG_EXECUTION_REQUEST_CONNECTION = 3
};

typedef struct ducknng_http_route {
    uint64_t route_id;
    uint8_t match_kind;
    uint8_t response_mode;           /* DUCKNNG_HTTP_ROUTE_RESPONSE_* */
    char *method;
    char *path;
    char *handler_sql;
    uint64_t request_max_bytes;
    char *static_dir_path;           /* non-NULL: serve files from this directory */
    char *stream_content_type;       /* non-NULL when response_mode==STREAM */
    int auth_require_identity;       /* 1: require non-empty caller_identity */
    char *auth_allow_identities_json; /* JSON array of allowed identities; NULL = any */
} ducknng_http_route;

typedef struct ducknng_principal_state {
    char *identity;
    size_t inflight_count;
    uint64_t cumulative_reply_bytes;
    /* sliding-window ring buffer for session-open rate */
    uint64_t *session_open_times;  /* allocated ring buffer of ms timestamps */
    size_t session_open_head;      /* index of oldest entry */
    size_t session_open_count;     /* number of valid entries */
    size_t session_open_cap;       /* allocated capacity */
} ducknng_principal_state;

typedef struct ducknng_http_worker {
    char *name;
    char *sql;
    uint64_t interval_ms;
    ducknng_thread thread;
    ducknng_mutex mu;
    ducknng_cond cv;
    int stopping;
    int mu_initialized;
    int cv_initialized;
    int thread_started;
    struct ducknng_service *svc;
} ducknng_http_worker;

typedef struct ducknng_http_request_context {
    ducknng_service *svc;
    ducknng_transport_scheme scheme;
    const char *method;
    const char *path;
    const char *query_string;
    const char *content_type;
    const char *headers_json;
    const uint8_t *body;
    size_t body_len;
    const char *path_params_json;
    const char *caller_identity;
    const nng_sockaddr *remote_addr;
    ducknng_http_route route;
} ducknng_http_request_context;

typedef struct ducknng_http_route_reply {
    int status;
    char *headers_json;
    char *content_type;
    uint8_t *body;
    size_t body_len;
    char *body_text;
} ducknng_http_route_reply;

typedef struct ducknng_service_sql_scope {
    ducknng_service *svc;
    duckdb_connection con;
    int owns_connection;
    int locked_runtime;
    int locked_service;
    size_t pool_index;
} ducknng_service_sql_scope;

struct ducknng_rep_ctx {
    ducknng_service *svc;
    nng_ctx ctx;
    nng_aio *aio;
    ducknng_thread thread;
    ducknng_mutex mu;
    ducknng_cond cv;
    int stopping;
    int phase;
    int event;
    int last_nng_err;
    int mu_initialized;
    int cv_initialized;
    int thread_started;
    nng_msg *request_msg;
};

struct ducknng_service {
    uint64_t service_id;
    char *name;
    char *listen_url;
    char *resolved_listen_url;
    int tls_enabled;
    uint64_t tls_config_id;
    char *tls_config_source;
    ducknng_tls_opts tls_opts;
    int ip_allowlist_active;
    ducknng_ip_allow_rule *ip_allowlist;
    size_t ip_allowlist_count;
    char *ip_allowlist_json;
    int authorizer_active;
    char *authorizer_sql;
    ducknng_pipe_event *pipe_events;
    size_t pipe_event_start;
    size_t pipe_event_count;
    size_t pipe_event_cap;
    uint64_t next_pipe_event_seq;
    uint64_t pipe_event_dropped;
    ducknng_pipe_state *pipe_states;
    size_t pipe_state_count;
    atomic_size_t pipe_state_count_visible;
    size_t pipe_state_cap;
    size_t inflight_request_count;
    atomic_size_t inflight_request_count_visible;
    nng_socket rep_sock;
    nng_listener listener;
    ducknng_rep_ctx *ctxs;
    ducknng_http_server_state *http_state;
    ducknng_http_route *http_routes;
    size_t http_route_count;
    size_t http_route_cap;
    uint64_t next_http_route_id;
    int ncontexts;
    ducknng_mutex mu;
    ducknng_mutex execution_mu;
    int mu_initialized;
    int execution_mu_initialized;
    int execution_model;
    duckdb_connection execution_con;
    size_t execution_pool_index;
    ducknng_session **sessions;
    size_t session_count;
    atomic_size_t session_count_visible;
    size_t session_cap;
    uint64_t next_session_id;
    uint64_t session_idle_ms;
    uint64_t max_open_sessions;
    uint64_t max_active_pipes;
    uint64_t max_inflight_requests;
    uint64_t max_sessions_per_peer_identity;
    uint64_t max_inflight_per_principal;
    uint64_t max_reply_bytes_per_principal;
    uint64_t max_session_open_rate_per_principal;
    ducknng_principal_state *principals;
    size_t principal_count;
    size_t principal_cap;
    ducknng_http_worker **http_workers;
    size_t http_worker_count;
    size_t http_worker_cap;
    size_t recv_max_bytes;
    int running;
    int shutting_down;
    ducknng_runtime *rt;
};

enum {
    DUCKNNG_PHASE_RECV = 1,
    DUCKNNG_PHASE_SEND = 2
};

enum {
    DUCKNNG_EVT_NONE = 0,
    DUCKNNG_EVT_REQUEST_READY = 1,
    DUCKNNG_EVT_SEND_DONE = 2,
    DUCKNNG_EVT_NNG_ERROR = 3
};

ducknng_service *ducknng_service_create(ducknng_runtime *rt, const char *name, const char *listen_url,
    int contexts, size_t recv_max_bytes, uint64_t session_idle_ms,
    uint64_t tls_config_id, const char *tls_config_source, const ducknng_tls_opts *tls_opts);
void ducknng_service_destroy(ducknng_service *svc);
int ducknng_service_start(ducknng_service *svc, char **errmsg);
int ducknng_service_stop(ducknng_service *svc, char **errmsg);
nng_msg *ducknng_handle_request(ducknng_service *svc, nng_msg *req);
nng_msg *ducknng_handle_request_with_identity(ducknng_service *svc, nng_msg *req,
    const char *caller_identity);
int ducknng_service_requires_peer_identity(const ducknng_service *svc);
int ducknng_service_peer_allowlist_active(const ducknng_service *svc);
size_t ducknng_service_peer_allowlist_count(const ducknng_service *svc);
int ducknng_service_peer_admission_check(ducknng_service *svc, const char *caller_identity, char **errmsg);
int ducknng_service_network_admission_check(ducknng_service *svc, const char *caller_identity,
    const nng_sockaddr *remote_addr, char **errmsg);
int ducknng_service_set_peer_allowlist(ducknng_service *svc, const char *identities_json, char **errmsg);
int ducknng_service_set_ip_allowlist(ducknng_service *svc, const char *cidrs_json, char **errmsg);
int ducknng_service_ip_allowlist_active(const ducknng_service *svc);
size_t ducknng_service_ip_allowlist_count(const ducknng_service *svc);
int ducknng_service_set_authorizer(ducknng_service *svc, const char *authorizer_sql, char **errmsg);
int ducknng_service_authorizer_active(const ducknng_service *svc);
int ducknng_service_set_limits(ducknng_service *svc, uint64_t max_open_sessions,
    uint64_t max_active_pipes, uint64_t max_inflight_requests,
    uint64_t max_sessions_per_peer_identity,
    uint64_t max_inflight_per_principal,
    uint64_t max_reply_bytes_per_principal,
    uint64_t max_session_open_rate_per_principal, char **errmsg);
int ducknng_service_set_execution_model(ducknng_service *svc, const char *model, char **errmsg);
const char *ducknng_execution_model_name(int model);
uint64_t ducknng_service_max_open_sessions(const ducknng_service *svc);
uint64_t ducknng_service_max_active_pipes(const ducknng_service *svc);
uint64_t ducknng_service_max_inflight_requests(const ducknng_service *svc);
uint64_t ducknng_service_max_sessions_per_peer_identity(const ducknng_service *svc);
uint64_t ducknng_service_max_inflight_per_principal(const ducknng_service *svc);
uint64_t ducknng_service_max_reply_bytes_per_principal(const ducknng_service *svc);
uint64_t ducknng_service_max_session_open_rate_per_principal(const ducknng_service *svc);
const char *ducknng_service_execution_model(const ducknng_service *svc);
const char *ducknng_service_peer_identity_format(const ducknng_service *svc);
void ducknng_service_manifest_security(const ducknng_service *svc, ducknng_manifest_security *security);
size_t ducknng_service_active_pipe_count(const ducknng_service *svc);
size_t ducknng_service_inflight_request_count(const ducknng_service *svc);
int ducknng_service_enter_request_sql(ducknng_service *svc, ducknng_service_sql_scope *scope, char **errmsg);
void ducknng_service_leave_request_sql(ducknng_service_sql_scope *scope);
int ducknng_service_acquire_session_connection(ducknng_service *svc,
    duckdb_connection *out_con, size_t *out_index, char **errmsg);
void ducknng_service_release_session_connection(ducknng_service *svc, size_t index);
int ducknng_service_enter_http_route_sql(ducknng_service *svc,
    const ducknng_http_request_context *request_ctx, ducknng_service_sql_scope *scope, char **errmsg);
void ducknng_service_leave_http_route_sql(ducknng_service_sql_scope *scope);
int ducknng_service_enter_authorizer_sql(ducknng_service *svc,
    const ducknng_authorizer_context *auth_ctx, ducknng_service_sql_scope *scope, char **errmsg);
void ducknng_service_leave_authorizer_sql(ducknng_service_sql_scope *scope);
void ducknng_http_route_reset(ducknng_http_route *route);
int ducknng_http_route_copy(ducknng_http_route *dst, const ducknng_http_route *src);
const char *ducknng_http_route_match_kind_name(uint8_t match_kind);
void ducknng_http_route_reply_init(ducknng_http_route_reply *reply);
void ducknng_http_route_reply_reset(ducknng_http_route_reply *reply);
int ducknng_service_register_http_route(ducknng_service *svc, const char *method, const char *path,
    const char *handler_sql, uint64_t request_max_bytes, char **errmsg);
int ducknng_service_register_http_route_pattern(ducknng_service *svc, const char *method,
    const char *match_kind, const char *path_pattern, const char *handler_sql,
    uint64_t request_max_bytes, char **errmsg);
int ducknng_service_unregister_http_route(ducknng_service *svc, const char *method,
    const char *path, char **errmsg);
int ducknng_service_unregister_http_route_pattern(ducknng_service *svc, const char *method,
    const char *match_kind, const char *path_pattern, char **errmsg);
int ducknng_service_lookup_http_route(ducknng_service *svc, const char *method,
    const char *path, ducknng_http_route *out_route, char **out_path_params_json, char **errmsg);
int ducknng_service_http_routes_snapshot(ducknng_service *svc, ducknng_http_route **out_routes,
    size_t *out_count, char **errmsg);
void ducknng_service_http_routes_free(ducknng_http_route *routes, size_t count);
int ducknng_service_handle_http_route(ducknng_service *svc,
    const ducknng_http_request_context *request_ctx, ducknng_http_route_reply *reply, char **errmsg);
int ducknng_service_begin_request(ducknng_service *svc, const char *caller_identity, char **errmsg);
void ducknng_service_end_request(ducknng_service *svc, const char *caller_identity, size_t reply_bytes);
int ducknng_service_pipe_monitor_stats(ducknng_service *svc,
    ducknng_pipe_monitor_stats *out_stats, char **errmsg);
int ducknng_service_pipe_events_snapshot(ducknng_service *svc, uint64_t after_seq, uint64_t max_events,
    ducknng_pipe_event **out_events, size_t *out_count, char **errmsg);
void ducknng_service_pipe_events_free(ducknng_pipe_event *events, size_t count);
int ducknng_service_pipe_states_snapshot(ducknng_service *svc,
    ducknng_pipe_state **out_states, size_t *out_count, char **errmsg);
void ducknng_service_pipe_states_free(ducknng_pipe_state *states, size_t count);
void ducknng_authorizer_decision_init(ducknng_authorizer_decision *decision);
void ducknng_authorizer_decision_reset(ducknng_authorizer_decision *decision);
int ducknng_service_authorize_request(ducknng_service *svc, const ducknng_authorizer_context *auth_ctx,
    ducknng_authorizer_decision *decision, char **errmsg);
nng_msg *ducknng_handle_decoded_request(ducknng_service *svc, const ducknng_frame *frame,
    const char *caller_identity, const ducknng_authorizer_decision *decision);
int ducknng_service_set_http_route_auth(ducknng_service *svc, const char *method,
    const char *path, int require_identity, const char *allow_identities_json, char **errmsg);
int ducknng_service_register_http_worker(ducknng_service *svc, const char *name,
    const char *sql, uint64_t interval_ms, char **errmsg);
int ducknng_service_unregister_http_worker(ducknng_service *svc, const char *name,
    char **errmsg);
int ducknng_service_http_workers_snapshot(ducknng_service *svc,
    ducknng_http_worker **out_workers, size_t *out_count, char **errmsg);
void ducknng_service_http_workers_free(ducknng_http_worker *workers, size_t count);
/* Called with svc->mu already held. Checks and records a session-open event for
   the given identity. Returns 0 on success, -1 if the rate limit is exceeded
   (sets *errmsg). Pass now_ms from ducknng_now_ms(). */
int ducknng_service_check_and_record_session_open_locked(ducknng_service *svc,
    const char *identity, uint64_t now_ms, char **errmsg);
const char *ducknng_service_resolved_listen(const ducknng_service *svc);
int ducknng_service_register_http_stream_route(ducknng_service *svc, const char *method,
    const char *path, const char *handler_sql, const char *content_type,
    uint64_t request_max_bytes, char **errmsg);
int ducknng_service_execute_stream_route(ducknng_service *svc,
    const ducknng_http_request_context *request_ctx,
    int (*on_chunk)(const void *data, size_t len, void *user_data),
    void *user_data, char **errmsg);
