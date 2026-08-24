#include "ducknng_service.h"
#include "ducknng_registry.h"
#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

DUCKDB_EXTENSION_EXTERN

static void ducknng_service_clear_ip_allowlist(ducknng_service *svc) {
    if (!svc) return;
    if (svc->ip_allowlist) duckdb_free(svc->ip_allowlist);
    if (svc->ip_allowlist_json) duckdb_free(svc->ip_allowlist_json);
    svc->ip_allowlist = NULL;
    svc->ip_allowlist_count = 0;
    svc->ip_allowlist_json = NULL;
    svc->ip_allowlist_active = 0;
}

static void ducknng_service_clear_authorizer(ducknng_service *svc) {
    if (!svc) return;
    if (svc->authorizer_sql) duckdb_free(svc->authorizer_sql);
    svc->authorizer_sql = NULL;
    svc->authorizer_active = 0;
}

void ducknng_http_route_reset(ducknng_http_route *route) {
    if (!route) return;
    if (route->method) duckdb_free(route->method);
    if (route->path) duckdb_free(route->path);
    if (route->handler_sql) duckdb_free(route->handler_sql);
    if (route->static_dir_path) duckdb_free(route->static_dir_path);
    if (route->stream_content_type) duckdb_free(route->stream_content_type);
    if (route->auth_allow_identities_json) duckdb_free(route->auth_allow_identities_json);
    memset(route, 0, sizeof(*route));
}

int ducknng_http_route_copy(ducknng_http_route *dst, const ducknng_http_route *src) {
    if (!dst || !src) return -1;
    memset(dst, 0, sizeof(*dst));
    dst->route_id = src->route_id;
    dst->match_kind = src->match_kind;
    dst->response_mode = src->response_mode;
    dst->request_max_bytes = src->request_max_bytes;
    dst->auth_require_identity = src->auth_require_identity;
    dst->method = src->method ? ducknng_strdup(src->method) : NULL;
    dst->path = src->path ? ducknng_strdup(src->path) : NULL;
    dst->handler_sql = src->handler_sql ? ducknng_strdup(src->handler_sql) : NULL;
    dst->static_dir_path = src->static_dir_path ? ducknng_strdup(src->static_dir_path) : NULL;
    dst->stream_content_type = src->stream_content_type ? ducknng_strdup(src->stream_content_type) : NULL;
    dst->auth_allow_identities_json = src->auth_allow_identities_json ?
        ducknng_strdup(src->auth_allow_identities_json) : NULL;
    if ((src->method && !dst->method) || (src->path && !dst->path) ||
        (src->handler_sql && !dst->handler_sql) ||
        (src->static_dir_path && !dst->static_dir_path) ||
        (src->stream_content_type && !dst->stream_content_type) ||
        (src->auth_allow_identities_json && !dst->auth_allow_identities_json)) {
        ducknng_http_route_reset(dst);
        return -1;
    }
    return 0;
}

const char *ducknng_http_route_match_kind_name(uint8_t match_kind) {
    switch (match_kind) {
    case DUCKNNG_HTTP_ROUTE_MATCH_PREFIX:
        return "prefix";
    case DUCKNNG_HTTP_ROUTE_MATCH_TEMPLATE:
        return "template";
    case DUCKNNG_HTTP_ROUTE_MATCH_EXACT:
    default:
        return "exact";
    }
}

void ducknng_http_route_reply_init(ducknng_http_route_reply *reply) {
    if (!reply) return;
    memset(reply, 0, sizeof(*reply));
    reply->status = 200;
}

void ducknng_http_route_reply_reset(ducknng_http_route_reply *reply) {
    if (!reply) return;
    if (reply->headers_json) duckdb_free(reply->headers_json);
    if (reply->content_type) duckdb_free(reply->content_type);
    if (reply->body) duckdb_free(reply->body);
    if (reply->body_text) duckdb_free(reply->body_text);
    ducknng_http_route_reply_init(reply);
}

static void ducknng_service_clear_http_routes(ducknng_service *svc) {
    size_t i;
    if (!svc || !svc->http_routes) return;
    for (i = 0; i < svc->http_route_count; i++) ducknng_http_route_reset(&svc->http_routes[i]);
    duckdb_free(svc->http_routes);
    svc->http_routes = NULL;
    svc->http_route_count = 0;
    svc->http_route_cap = 0;
}

const char *ducknng_execution_model_name(int model) {
    switch (model) {
    case DUCKNNG_EXECUTION_SERVICE_SERIALIZED_CONNECTION:
        return "service_serialized_connection";
    case DUCKNNG_EXECUTION_REQUEST_CONNECTION:
        return "request_connection";
    case DUCKNNG_EXECUTION_SHARED_SERIALIZED_CONNECTION:
    default:
        return "shared_serialized_connection";
    }
}

static int ducknng_execution_model_parse(const char *model, int *out_model) {
    if (!model || !model[0] || !out_model) return 0;
    if (strcmp(model, "shared_serialized_connection") == 0 || strcmp(model, "shared") == 0) {
        *out_model = DUCKNNG_EXECUTION_SHARED_SERIALIZED_CONNECTION;
        return 1;
    }
    if (strcmp(model, "service_serialized_connection") == 0 || strcmp(model, "service") == 0) {
        *out_model = DUCKNNG_EXECUTION_SERVICE_SERIALIZED_CONNECTION;
        return 1;
    }
    if (strcmp(model, "request_connection") == 0 || strcmp(model, "request") == 0) {
        *out_model = DUCKNNG_EXECUTION_REQUEST_CONNECTION;
        return 1;
    }
    return 0;
}

#define DUCKNNG_EXECUTION_POOL_ACQUIRE_TIMEOUT_MS 10000u

static int ducknng_service_acquire_execution_connection(ducknng_service *svc,
    duckdb_connection *out_con, size_t *out_index, char **errmsg) {
    ducknng_runtime *rt;
    size_t i;
    uint64_t start;
    uint64_t deadline;
    if (out_con) *out_con = NULL;
    if (out_index) *out_index = (size_t)-1;
    if (!svc || !svc->rt || !out_con || !out_index || !svc->rt->execution_pool_mu_initialized) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing DuckDB execution connection pool");
        return -1;
    }
    rt = svc->rt;
    start = ducknng_now_ms();
    deadline = DUCKNNG_EXECUTION_POOL_ACQUIRE_TIMEOUT_MS > UINT64_MAX - start ?
        UINT64_MAX : start + DUCKNNG_EXECUTION_POOL_ACQUIRE_TIMEOUT_MS;
    ducknng_mutex_lock(&rt->execution_pool_mu);
    for (;;) {
        uint64_t now;
        for (i = 0; i < rt->execution_pool_count; i++) {
            if (!rt->execution_pool_busy[i] && rt->execution_pool[i]) {
                rt->execution_pool_busy[i] = 1;
                *out_con = rt->execution_pool[i];
                *out_index = i;
                ducknng_mutex_unlock(&rt->execution_pool_mu);
                return 0;
            }
        }
        if (rt->execution_pool_count < rt->execution_pool_max && rt->db) {
            duckdb_connection con = NULL;
            size_t idx = rt->execution_pool_count;
            if (duckdb_connect(rt->db, &con) != DuckDBError && con) {
                rt->execution_pool[idx] = con;
                rt->execution_pool_busy[idx] = 1;
                rt->execution_pool_count++;
                *out_con = con;
                *out_index = idx;
                ducknng_mutex_unlock(&rt->execution_pool_mu);
                return 0;
            }
        }
        if (!rt->execution_pool_cv_initialized) break;
        now = ducknng_now_ms();
        if (now >= deadline) break;
        if (ducknng_cond_timedwait_ms(&rt->execution_pool_cv, &rt->execution_pool_mu,
                deadline - now) < 0) break;
    }
    ducknng_mutex_unlock(&rt->execution_pool_mu);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: DuckDB execution connection pool exhausted");
    return -1;
}

static void ducknng_service_reset_pool_connection(ducknng_runtime *rt, size_t index) {
    static const char *build_sql =
        "SELECT coalesce(string_agg(stmt, '; '), '') FROM ("
        "SELECT 'DROP TABLE IF EXISTS \"' || replace(schema_name,'\"','\"\"') || "
        "'\".\"' || replace(table_name,'\"','\"\"') || '\"' AS stmt "
        "FROM duckdb_tables() WHERE temporary AND NOT internal "
        "UNION ALL "
        "SELECT 'DROP VIEW IF EXISTS \"' || replace(schema_name,'\"','\"\"') || "
        "'\".\"' || replace(view_name,'\"','\"\"') || '\"' "
        "FROM duckdb_views() WHERE temporary AND NOT internal)";
    duckdb_connection con;
    duckdb_result res;
    char *script = NULL;
    if (!rt || index >= rt->execution_pool_count) return;
    con = rt->execution_pool[index];
    if (!con) return;
    memset(&res, 0, sizeof(res));
    if (duckdb_query(con, build_sql, &res) == DuckDBError) {
        duckdb_destroy_result(&res);
        return;
    }
    {
        duckdb_data_chunk chunk = duckdb_fetch_chunk(res);
        if (chunk) {
            if (duckdb_data_chunk_get_size(chunk) >= 1 &&
                duckdb_data_chunk_get_column_count(chunk) >= 1) {
                duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, 0);
                uint64_t *validity = duckdb_vector_get_validity(vec);
                if (!validity || duckdb_validity_row_is_valid(validity, 0)) {
                    duckdb_string_t *strs = (duckdb_string_t *)duckdb_vector_get_data(vec);
                    const char *src = duckdb_string_t_data(&strs[0]);
                    uint32_t len = duckdb_string_t_length(strs[0]);
                    if (len) {
                        script = (char *)duckdb_malloc((size_t)len + 1);
                        if (script) {
                            memcpy(script, src, len);
                            script[len] = '\0';
                        }
                    }
                }
            }
            duckdb_destroy_data_chunk(&chunk);
        }
    }
    duckdb_destroy_result(&res);
    if (script && script[0]) {
        duckdb_result drop_res;
        memset(&drop_res, 0, sizeof(drop_res));
        (void)duckdb_query(con, script, &drop_res);
        duckdb_destroy_result(&drop_res);
    }
    if (script) duckdb_free(script);
}

static void ducknng_service_release_execution_connection(ducknng_service *svc, size_t index) {
    if (!svc || !svc->rt || index == (size_t)-1 || !svc->rt->execution_pool_mu_initialized) return;
    ducknng_service_reset_pool_connection(svc->rt, index);
    ducknng_mutex_lock(&svc->rt->execution_pool_mu);
    if (index < svc->rt->execution_pool_count) svc->rt->execution_pool_busy[index] = 0;
    if (svc->rt->execution_pool_cv_initialized) ducknng_cond_signal(&svc->rt->execution_pool_cv);
    ducknng_mutex_unlock(&svc->rt->execution_pool_mu);
}

int ducknng_service_enter_request_sql(ducknng_service *svc, ducknng_service_sql_scope *scope, char **errmsg) {
    int model;
    if (errmsg) *errmsg = NULL;
    if (!scope) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing SQL execution scope");
        return -1;
    }
    memset(scope, 0, sizeof(*scope));
    scope->pool_index = (size_t)-1;
    if (!svc || !svc->rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing SQL execution service");
        return -1;
    }
    scope->svc = svc;
    model = svc->execution_model ? svc->execution_model : DUCKNNG_EXECUTION_SHARED_SERIALIZED_CONNECTION;
    if (model == DUCKNNG_EXECUTION_SHARED_SERIALIZED_CONNECTION) {
        ducknng_runtime_execution_lane_lock(svc->rt);
        scope->locked_runtime = 1;
        scope->con = ducknng_runtime_execution_connection(svc->rt);
    } else if (model == DUCKNNG_EXECUTION_SERVICE_SERIALIZED_CONNECTION) {
        if (!svc->execution_mu_initialized) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: missing service execution mutex");
            return -1;
        }
        ducknng_mutex_lock(&svc->execution_mu);
        scope->locked_service = 1;
        if (!svc->execution_con && ducknng_service_acquire_execution_connection(svc,
                &svc->execution_con, &svc->execution_pool_index, errmsg) != 0) {
            ducknng_mutex_unlock(&svc->execution_mu);
            scope->locked_service = 0;
            return -1;
        }
        scope->con = svc->execution_con;
    } else if (model == DUCKNNG_EXECUTION_REQUEST_CONNECTION) {
        if (ducknng_service_acquire_execution_connection(svc, &scope->con, &scope->pool_index, errmsg) != 0) return -1;
        scope->owns_connection = 1;
    } else {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid service execution model");
        return -1;
    }
    if (!scope->con) {
        ducknng_service_leave_request_sql(scope);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing DuckDB execution connection");
        return -1;
    }
    ducknng_runtime_current_request_service_set(svc->rt, svc);
    /* Bind the current request's execution subject to the connection about to
     * run its SQL, so profile admission still sees it when DuckDB executes
     * pipelines on worker threads. */
    {
        const ducknng_execution_subject *subject_ctx =
            ducknng_runtime_current_thread_execution_subject_get(svc->rt);
        if (subject_ctx && subject_ctx->subject && subject_ctx->subject[0]) {
            duckdb_client_context client_ctx = NULL;
            duckdb_connection_get_client_context(scope->con, &client_ctx);
            if (client_ctx) {
                scope->subject_connection_id =
                    (uint64_t)duckdb_client_context_get_connection_id(client_ctx);
                duckdb_destroy_client_context(&client_ctx);
                if (ducknng_runtime_execution_subject_bind_connection(svc->rt,
                        scope->subject_connection_id, subject_ctx->subject) == 0) {
                    scope->subject_bound = 1;
                }
            }
        }
    }
    return 0;
}

void ducknng_service_leave_request_sql(ducknng_service_sql_scope *scope) {
    ducknng_service *svc;
    if (!scope || !scope->svc) return;
    svc = scope->svc;
    if (scope->subject_bound && svc->rt) {
        ducknng_runtime_execution_subject_unbind_connection(svc->rt,
            scope->subject_connection_id);
    }
    if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
    if (scope->owns_connection) ducknng_service_release_execution_connection(svc, scope->pool_index);
    if (scope->locked_service && svc->execution_mu_initialized) ducknng_mutex_unlock(&svc->execution_mu);
    if (scope->locked_runtime && svc->rt) ducknng_runtime_execution_lane_unlock(svc->rt);
    memset(scope, 0, sizeof(*scope));
}

int ducknng_service_acquire_session_connection(ducknng_service *svc,
    duckdb_connection *out_con, size_t *out_index, char **errmsg) {
    if (ducknng_service_acquire_execution_connection(svc, out_con, out_index, errmsg) != 0) return -1;
    /* Bind the opener's execution subject to the session connection so
     * restricted HTTP profiles keep resolving during later fetches of this
     * session, on whatever thread DuckDB executes them. Every release path
     * funnels through ducknng_service_release_session_connection, which
     * removes the binding before the pooled connection can be reused. */
    {
        const ducknng_execution_subject *subject_ctx =
            ducknng_runtime_current_thread_execution_subject_get(svc->rt);
        if (subject_ctx && subject_ctx->subject && subject_ctx->subject[0] && *out_con) {
            duckdb_client_context client_ctx = NULL;
            duckdb_connection_get_client_context(*out_con, &client_ctx);
            if (client_ctx) {
                uint64_t connection_id =
                    (uint64_t)duckdb_client_context_get_connection_id(client_ctx);
                duckdb_destroy_client_context(&client_ctx);
                (void)ducknng_runtime_execution_subject_bind_connection(svc->rt,
                    connection_id, subject_ctx->subject);
            }
        }
    }
    return 0;
}

void ducknng_service_release_session_connection(ducknng_service *svc, size_t index) {
    if (svc && svc->rt && index != (size_t)-1 && svc->rt->execution_pool_mu_initialized) {
        duckdb_connection con = NULL;
        ducknng_mutex_lock(&svc->rt->execution_pool_mu);
        if (index < svc->rt->execution_pool_count) con = svc->rt->execution_pool[index];
        ducknng_mutex_unlock(&svc->rt->execution_pool_mu);
        if (con) {
            duckdb_client_context client_ctx = NULL;
            duckdb_connection_get_client_context(con, &client_ctx);
            if (client_ctx) {
                ducknng_runtime_execution_subject_unbind_connection(svc->rt,
                    (uint64_t)duckdb_client_context_get_connection_id(client_ctx));
                duckdb_destroy_client_context(&client_ctx);
            }
        }
    }
    ducknng_service_release_execution_connection(svc, index);
}

int ducknng_service_enter_http_route_sql(ducknng_service *svc,
    const ducknng_http_request_context *request_ctx, ducknng_service_sql_scope *scope, char **errmsg) {
    if (ducknng_service_enter_request_sql(svc, scope, errmsg) != 0) return -1;
    ducknng_runtime_current_http_request_context_set(svc->rt, request_ctx);
    return 0;
}

void ducknng_service_leave_http_route_sql(ducknng_service_sql_scope *scope) {
    ducknng_service *svc = scope ? scope->svc : NULL;
    if (svc && svc->rt) ducknng_runtime_current_http_request_context_set(svc->rt, NULL);
    ducknng_service_leave_request_sql(scope);
}

int ducknng_service_enter_authorizer_sql(ducknng_service *svc,
    const ducknng_authorizer_context *auth_ctx, ducknng_service_sql_scope *scope, char **errmsg) {
    if (ducknng_service_enter_request_sql(svc, scope, errmsg) != 0) return -1;
    ducknng_runtime_current_authorizer_context_set(svc->rt, auth_ctx);
    return 0;
}

void ducknng_service_leave_authorizer_sql(ducknng_service_sql_scope *scope) {
    ducknng_service *svc = scope ? scope->svc : NULL;
    if (svc && svc->rt) ducknng_runtime_current_authorizer_context_set(svc->rt, NULL);
    ducknng_service_leave_request_sql(scope);
}

void ducknng_service_execution_subject_begin(ducknng_service *svc,
    struct ducknng_execution_subject *subject_ctx, const char *caller_identity,
    const ducknng_authorizer_decision *auth_decision) {
    if (!subject_ctx) return;
    memset(subject_ctx, 0, sizeof(*subject_ctx));
    subject_ctx->peer_identity = caller_identity;
    subject_ctx->principal = auth_decision ? auth_decision->principal : NULL;
    subject_ctx->subject = (subject_ctx->principal && subject_ctx->principal[0])
        ? subject_ctx->principal : caller_identity;
    subject_ctx->claims_json = auth_decision ? auth_decision->claims_json : NULL;
    if (svc && svc->rt) ducknng_runtime_current_execution_subject_set(svc->rt, subject_ctx);
}

void ducknng_service_execution_subject_end(ducknng_service *svc) {
    if (svc && svc->rt) ducknng_runtime_current_execution_subject_set(svc->rt, NULL);
}

static void ducknng_pipe_event_reset(ducknng_pipe_event *event) {
    if (!event) return;
    if (event->event) duckdb_free(event->event);
    if (event->reason) duckdb_free(event->reason);
    if (event->remote_addr) duckdb_free(event->remote_addr);
    if (event->remote_ip) duckdb_free(event->remote_ip);
    if (event->peer_identity) duckdb_free(event->peer_identity);
    memset(event, 0, sizeof(*event));
}

static void ducknng_pipe_state_reset(ducknng_pipe_state *state) {
    if (!state) return;
    if (state->remote_addr) duckdb_free(state->remote_addr);
    if (state->remote_ip) duckdb_free(state->remote_ip);
    if (state->peer_identity) duckdb_free(state->peer_identity);
    memset(state, 0, sizeof(*state));
}

void ducknng_service_pipe_states_free(ducknng_pipe_state *states, size_t count) {
    size_t i;
    if (!states) return;
    for (i = 0; i < count; i++) ducknng_pipe_state_reset(&states[i]);
    duckdb_free(states);
}

void ducknng_service_pipe_events_free(ducknng_pipe_event *events, size_t count) {
    size_t i;
    if (!events) return;
    for (i = 0; i < count; i++) ducknng_pipe_event_reset(&events[i]);
    duckdb_free(events);
}

static void ducknng_service_clear_pipe_events(ducknng_service *svc) {
    size_t i;
    if (!svc) return;
    if (svc->pipe_events) {
        for (i = 0; i < svc->pipe_event_cap; i++) ducknng_pipe_event_reset(&svc->pipe_events[i]);
        duckdb_free(svc->pipe_events);
    }
    svc->pipe_events = NULL;
    svc->pipe_event_start = 0;
    svc->pipe_event_count = 0;
    svc->pipe_event_cap = 0;
    svc->pipe_event_dropped = 0;
}

static void ducknng_service_clear_pipe_states(ducknng_service *svc) {
    size_t i;
    if (!svc) return;
    if (svc->pipe_states) {
        for (i = 0; i < svc->pipe_state_count; i++) ducknng_pipe_state_reset(&svc->pipe_states[i]);
        duckdb_free(svc->pipe_states);
    }
    svc->pipe_states = NULL;
    svc->pipe_state_count = 0;
    atomic_store_explicit(&svc->pipe_state_count_visible, 0, memory_order_release);
    svc->pipe_state_cap = 0;
}

static void ducknng_json_skip_ws(const char **p) {
    while (p && *p && (**p == ' ' || **p == '\t' || **p == '\r' || **p == '\n')) (*p)++;
}

static char *ducknng_json_parse_ascii_string(const char **p, char **errmsg) {
    const char *s;
    char *out;
    size_t len = 0;
    size_t cap = 32;
    if (!p || !*p || **p != '"') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist must be a JSON array of strings");
        return NULL;
    }
    s = ++(*p);
    out = (char *)duckdb_malloc(cap);
    if (!out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing IP allowlist");
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
                default:
                    duckdb_free(out);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported JSON escape in IP allowlist");
                    return NULL;
            }
        } else if (ch < 0x20) {
            duckdb_free(out);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid control character in IP allowlist");
            return NULL;
        }
        if (len + 2 > cap) {
            char *next;
            cap *= 2;
            next = (char *)duckdb_malloc(cap);
            if (!next) {
                duckdb_free(out);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing IP allowlist");
                return NULL;
            }
            memcpy(next, out, len);
            duckdb_free(out);
            out = next;
        }
        out[len++] = (char)ch;
    }
    duckdb_free(out);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: unterminated JSON string in IP allowlist");
    return NULL;
}

static int ducknng_parse_u8_decimal(const char *s, int max_value, int *out) {
    int v = 0;
    if (!s || !s[0]) return -1;
    while (*s) {
        if (*s < '0' || *s > '9') return -1;
        v = v * 10 + (*s - '0');
        if (v > max_value) return -1;
        s++;
    }
    if (out) *out = v;
    return 0;
}

static int ducknng_parse_ip_rule(const char *text, ducknng_ip_allow_rule *rule, char **errmsg) {
    char host[128];
    const char *slash;
    size_t host_len;
    int prefix = -1;
    if (!text || !text[0] || !rule) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist entries must be non-empty strings");
        return -1;
    }
    slash = strchr(text, '/');
    host_len = slash ? (size_t)(slash - text) : strlen(text);
    if (host_len == 0 || host_len >= sizeof(host)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid IP allowlist address");
        return -1;
    }
    memcpy(host, text, host_len);
    host[host_len] = '\0';
    memset(rule, 0, sizeof(*rule));
    if (inet_pton(AF_INET, host, rule->addr) == 1) {
        rule->family = NNG_AF_INET;
        prefix = slash ? -1 : 32;
        if (slash && ducknng_parse_u8_decimal(slash + 1, 32, &prefix) != 0) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid IPv4 CIDR prefix in IP allowlist");
            return -1;
        }
    } else if (inet_pton(AF_INET6, host, rule->addr) == 1) {
        rule->family = NNG_AF_INET6;
        prefix = slash ? -1 : 128;
        if (slash && ducknng_parse_u8_decimal(slash + 1, 128, &prefix) != 0) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid IPv6 CIDR prefix in IP allowlist");
            return -1;
        }
    } else {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist entries must be IPv4/IPv6 literals or CIDR blocks");
        return -1;
    }
    rule->prefix_bits = (uint8_t)prefix;
    return 0;
}

static void ducknng_ip_rules_free(ducknng_ip_allow_rule *rules) {
    if (rules) duckdb_free(rules);
}

static int ducknng_parse_ip_allowlist_json(const char *json, ducknng_ip_allow_rule **out_rules,
    size_t *out_count, char **out_json, char **errmsg) {
    const char *p = json;
    ducknng_ip_allow_rule *rules = NULL;
    size_t count = 0;
    size_t cap = 0;
    if (out_rules) *out_rules = NULL;
    if (out_count) *out_count = 0;
    if (out_json) *out_json = NULL;
    if (errmsg) *errmsg = NULL;
    ducknng_json_skip_ws(&p);
    if (!p || *p != '[') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist must be a JSON array of strings");
        return -1;
    }
    p++;
    ducknng_json_skip_ws(&p);
    if (*p == ']') {
        p++;
        ducknng_json_skip_ws(&p);
        if (*p) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after IP allowlist JSON");
            return -1;
        }
        if (out_json) {
            *out_json = ducknng_strdup(json);
            if (!*out_json) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying IP allowlist");
                return -1;
            }
        }
        return 0;
    }
    for (;;) {
        char *item;
        ducknng_ip_allow_rule rule;
        ducknng_json_skip_ws(&p);
        item = ducknng_json_parse_ascii_string(&p, errmsg);
        if (!item) goto fail;
        if (ducknng_parse_ip_rule(item, &rule, errmsg) != 0) {
            duckdb_free(item);
            goto fail;
        }
        duckdb_free(item);
        if (count == cap) {
            ducknng_ip_allow_rule *next;
            size_t new_cap = cap ? cap * 2 : 4;
            next = (ducknng_ip_allow_rule *)duckdb_malloc(sizeof(*next) * new_cap);
            if (!next) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing IP allowlist");
                goto fail;
            }
            if (rules && count) memcpy(next, rules, sizeof(*rules) * count);
            if (rules) duckdb_free(rules);
            rules = next;
            cap = new_cap;
        }
        rules[count++] = rule;
        ducknng_json_skip_ws(&p);
        if (*p == ',') { p++; continue; }
        if (*p == ']') { p++; break; }
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or ']' in IP allowlist JSON");
        goto fail;
    }
    ducknng_json_skip_ws(&p);
    if (*p) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after IP allowlist JSON");
        goto fail;
    }
    if (out_json) {
        *out_json = ducknng_strdup(json);
        if (!*out_json) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying IP allowlist");
            goto fail;
        }
    }
    if (out_rules) *out_rules = rules;
    else ducknng_ip_rules_free(rules);
    if (out_count) *out_count = count;
    return 0;
fail:
    ducknng_ip_rules_free(rules);
    return -1;
}

static int ducknng_ip_rule_matches_bytes(const ducknng_ip_allow_rule *rule, const uint8_t *addr) {
    int full;
    int rem;
    uint8_t mask;
    if (!rule || !addr) return 0;
    full = rule->prefix_bits / 8;
    rem = rule->prefix_bits % 8;
    if (full > 0 && memcmp(rule->addr, addr, (size_t)full) != 0) return 0;
    if (rem == 0) return 1;
    mask = (uint8_t)(0xffu << (8 - rem));
    return (rule->addr[full] & mask) == (addr[full] & mask);
}

static int ducknng_remote_addr_matches_ip_allowlist(const ducknng_service *svc, const nng_sockaddr *addr) {
    uint8_t ip[16];
    size_t i;
    if (!svc || !svc->ip_allowlist_active) return 1;
    if (!addr) return 0;
    memset(ip, 0, sizeof(ip));
    if (addr->s_family == NNG_AF_INET) {
        memcpy(ip, &addr->s_in.sa_addr, 4);
    } else if (addr->s_family == NNG_AF_INET6) {
        memcpy(ip, addr->s_in6.sa_addr, 16);
    } else {
        return 0;
    }
    for (i = 0; i < svc->ip_allowlist_count; i++) {
        const ducknng_ip_allow_rule *rule = &svc->ip_allowlist[i];
        if (rule->family == addr->s_family && ducknng_ip_rule_matches_bytes(rule, ip)) return 1;
    }
    return 0;
}

static int ducknng_pipe_remote_addr(nng_pipe pipe, nng_sockaddr *out) {
    if (!out || pipe.id <= 0) return -1;
    memset(out, 0, sizeof(*out));
    return nng_pipe_get_addr(pipe, NNG_OPT_REMADDR, out) == 0 ? 0 : -1;
}

static char *ducknng_sockaddr_addr_dup(const nng_sockaddr *addr, char **out_ip, int32_t *out_port) {
    char ipbuf[INET6_ADDRSTRLEN];
    char addrbuf[INET6_ADDRSTRLEN + 32];
    const char *ip = NULL;
    int32_t port = 0;
    if (out_ip) *out_ip = NULL;
    if (out_port) *out_port = 0;
    if (!addr) return NULL;
    memset(ipbuf, 0, sizeof(ipbuf));
    memset(addrbuf, 0, sizeof(addrbuf));
    if (addr->s_family == NNG_AF_INET) {
        ip = inet_ntop(AF_INET, &addr->s_in.sa_addr, ipbuf, sizeof(ipbuf));
        port = (int32_t)ntohs(addr->s_in.sa_port);
        if (!ip) return NULL;
        snprintf(addrbuf, sizeof(addrbuf), "%s:%d", ipbuf, (int)port);
        if (out_ip) *out_ip = ducknng_strdup(ipbuf);
        if (out_port) *out_port = port;
        return ducknng_strdup(addrbuf);
    }
    if (addr->s_family == NNG_AF_INET6) {
        ip = inet_ntop(AF_INET6, addr->s_in6.sa_addr, ipbuf, sizeof(ipbuf));
        port = (int32_t)ntohs(addr->s_in6.sa_port);
        if (!ip) return NULL;
        snprintf(addrbuf, sizeof(addrbuf), "[%s]:%d", ipbuf, (int)port);
        if (out_ip) *out_ip = ducknng_strdup(ipbuf);
        if (out_port) *out_port = port;
        return ducknng_strdup(addrbuf);
    }
    if (addr->s_family == NNG_AF_IPC) return ducknng_strdup(addr->s_ipc.sa_path);
    if (addr->s_family == NNG_AF_INPROC) return ducknng_strdup(addr->s_inproc.sa_name);
    snprintf(addrbuf, sizeof(addrbuf), "nng-family:%u", (unsigned)addr->s_family);
    return ducknng_strdup(addrbuf);
}

static int ducknng_pipe_event_copy(ducknng_pipe_event *dst, const ducknng_pipe_event *src) {
    if (!dst || !src) return -1;
    memset(dst, 0, sizeof(*dst));
    *dst = *src;
    dst->event = src->event ? ducknng_strdup(src->event) : NULL;
    dst->reason = src->reason ? ducknng_strdup(src->reason) : NULL;
    dst->remote_addr = src->remote_addr ? ducknng_strdup(src->remote_addr) : NULL;
    dst->remote_ip = src->remote_ip ? ducknng_strdup(src->remote_ip) : NULL;
    dst->peer_identity = src->peer_identity ? ducknng_strdup(src->peer_identity) : NULL;
    if ((src->event && !dst->event) || (src->reason && !dst->reason) ||
        (src->remote_addr && !dst->remote_addr) || (src->remote_ip && !dst->remote_ip) ||
        (src->peer_identity && !dst->peer_identity)) {
        ducknng_pipe_event_reset(dst);
        return -1;
    }
    return 0;
}

static int ducknng_pipe_state_copy(ducknng_pipe_state *dst, const ducknng_pipe_state *src) {
    if (!dst || !src) return -1;
    memset(dst, 0, sizeof(*dst));
    *dst = *src;
    dst->remote_addr = src->remote_addr ? ducknng_strdup(src->remote_addr) : NULL;
    dst->remote_ip = src->remote_ip ? ducknng_strdup(src->remote_ip) : NULL;
    dst->peer_identity = src->peer_identity ? ducknng_strdup(src->peer_identity) : NULL;
    if ((src->remote_addr && !dst->remote_addr) || (src->remote_ip && !dst->remote_ip) ||
        (src->peer_identity && !dst->peer_identity)) {
        ducknng_pipe_state_reset(dst);
        return -1;
    }
    return 0;
}

static int ducknng_service_pipe_state_find_locked(ducknng_service *svc, uint64_t pipe_id) {
    size_t i;
    if (!svc || pipe_id == 0) return -1;
    for (i = 0; i < svc->pipe_state_count; i++) {
        if (svc->pipe_states[i].pipe_id == pipe_id) return (int)i;
    }
    return -1;
}

static void ducknng_service_pipe_state_add_locked(ducknng_service *svc, nng_pipe pipe,
    const nng_sockaddr *remote_addr, const char *identity) {
    uint64_t pipe_id = pipe.id > 0 ? (uint64_t)pipe.id : 0;
    int existing;
    ducknng_pipe_state *state;
    if (!svc || pipe_id == 0) return;
    existing = ducknng_service_pipe_state_find_locked(svc, pipe_id);
    if (existing >= 0) {
        ducknng_pipe_state_reset(&svc->pipe_states[existing]);
        state = &svc->pipe_states[existing];
    } else {
        if (svc->pipe_state_count == svc->pipe_state_cap) {
            size_t new_cap = svc->pipe_state_cap ? svc->pipe_state_cap * 2 : 16;
            ducknng_pipe_state *next = (ducknng_pipe_state *)duckdb_malloc(sizeof(*next) * new_cap);
            if (!next) return;
            memset(next, 0, sizeof(*next) * new_cap);
            if (svc->pipe_states && svc->pipe_state_count) {
                memcpy(next, svc->pipe_states, sizeof(*next) * svc->pipe_state_count);
                duckdb_free(svc->pipe_states);
            }
            svc->pipe_states = next;
            svc->pipe_state_cap = new_cap;
        }
        state = &svc->pipe_states[svc->pipe_state_count++];
        atomic_store_explicit(&svc->pipe_state_count_visible, svc->pipe_state_count, memory_order_release);
    }
    memset(state, 0, sizeof(*state));
    state->pipe_id = pipe_id;
    state->opened_ms = ducknng_now_ms();
    state->remote_addr = ducknng_sockaddr_addr_dup(remote_addr, &state->remote_ip, &state->remote_port);
    state->peer_identity = identity && identity[0] ? ducknng_strdup(identity) : NULL;
}

static void ducknng_service_pipe_state_remove_locked(ducknng_service *svc, nng_pipe pipe) {
    uint64_t pipe_id = pipe.id > 0 ? (uint64_t)pipe.id : 0;
    int idx;
    size_t i;
    if (!svc || pipe_id == 0) return;
    idx = ducknng_service_pipe_state_find_locked(svc, pipe_id);
    if (idx < 0) return;
    ducknng_pipe_state_reset(&svc->pipe_states[idx]);
    for (i = (size_t)idx; i + 1 < svc->pipe_state_count; i++) svc->pipe_states[i] = svc->pipe_states[i + 1];
    svc->pipe_state_count--;
    atomic_store_explicit(&svc->pipe_state_count_visible, svc->pipe_state_count, memory_order_release);
    memset(&svc->pipe_states[svc->pipe_state_count], 0, sizeof(svc->pipe_states[svc->pipe_state_count]));
}

static void ducknng_service_pipe_event_append(ducknng_service *svc, nng_pipe pipe,
    const char *event_name, const nng_sockaddr *remote_addr, const char *identity,
    int admitted, const char *reason) {
    ducknng_pipe_event event;
    size_t idx;
    int is_add_post;
    int is_rem_post;
    if (!svc || !event_name) return;
    memset(&event, 0, sizeof(event));
    event.ts_ms = ducknng_now_ms();
    event.pipe_id = pipe.id > 0 ? (uint64_t)pipe.id : 0;
    event.event = ducknng_strdup(event_name);
    event.admitted = admitted;
    event.reason = reason && reason[0] ? ducknng_strdup(reason) : NULL;
    event.remote_addr = ducknng_sockaddr_addr_dup(remote_addr, &event.remote_ip, &event.remote_port);
    event.peer_identity = identity && identity[0] ? ducknng_strdup(identity) : NULL;
    is_add_post = strcmp(event_name, "add_post") == 0;
    is_rem_post = strcmp(event_name, "rem_post") == 0;
    if (!event.event || (reason && reason[0] && !event.reason) ||
        (identity && identity[0] && !event.peer_identity)) {
        ducknng_pipe_event_reset(&event);
        return;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    if (!svc->pipe_events) {
        svc->pipe_event_cap = 1024;
        svc->pipe_events = (ducknng_pipe_event *)duckdb_malloc(sizeof(*svc->pipe_events) * svc->pipe_event_cap);
        if (!svc->pipe_events) {
            svc->pipe_event_cap = 0;
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            ducknng_pipe_event_reset(&event);
            return;
        }
        memset(svc->pipe_events, 0, sizeof(*svc->pipe_events) * svc->pipe_event_cap);
    }
    if (svc->pipe_event_count < svc->pipe_event_cap) {
        idx = (svc->pipe_event_start + svc->pipe_event_count) % svc->pipe_event_cap;
        svc->pipe_event_count++;
    } else {
        idx = svc->pipe_event_start;
        ducknng_pipe_event_reset(&svc->pipe_events[idx]);
        svc->pipe_event_start = (svc->pipe_event_start + 1) % svc->pipe_event_cap;
        svc->pipe_event_dropped++;
    }
    if (is_add_post) ducknng_service_pipe_state_add_locked(svc, pipe, remote_addr, identity);
    if (is_rem_post) ducknng_service_pipe_state_remove_locked(svc, pipe);
    event.seq = ++svc->next_pipe_event_seq;
    svc->pipe_events[idx] = event;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
}

void ducknng_authorizer_decision_init(ducknng_authorizer_decision *decision) {
    if (!decision) return;
    memset(decision, 0, sizeof(*decision));
    decision->allow = 1;
    decision->http_status = 200;
}

void ducknng_authorizer_decision_reset(ducknng_authorizer_decision *decision) {
    if (!decision) return;
    if (decision->reason) duckdb_free(decision->reason);
    if (decision->principal) duckdb_free(decision->principal);
    if (decision->claims_json) duckdb_free(decision->claims_json);
    ducknng_authorizer_decision_init(decision);
}

static int ducknng_ascii_name_equal(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        unsigned char ca = (unsigned char)*a++;
        unsigned char cb = (unsigned char)*b++;
        if (ca >= 'A' && ca <= 'Z') ca = (unsigned char)(ca + ('a' - 'A'));
        if (cb >= 'A' && cb <= 'Z') cb = (unsigned char)(cb + ('a' - 'A'));
        if (ca != cb) return 0;
    }
    return *a == '\0' && *b == '\0';
}

static int ducknng_result_column_index(duckdb_result *result, const char *name) {
    idx_t i;
    idx_t ncols;
    if (!result || !name) return -1;
    ncols = duckdb_column_count(result);
    for (i = 0; i < ncols; i++) {
        const char *col = duckdb_column_name(result, i);
        if (ducknng_ascii_name_equal(col, name)) return (int)i;
    }
    return -1;
}

static int ducknng_chunk_value_is_null(duckdb_vector vec, idx_t row) {
    uint64_t *validity;
    if (!vec) return 1;
    validity = duckdb_vector_get_validity(vec);
    return validity && !duckdb_validity_row_is_valid(validity, row);
}

static char *ducknng_chunk_varchar_dup(duckdb_data_chunk chunk, int col, idx_t row) {
    duckdb_vector vec;
    duckdb_string_t *data;
    const char *src;
    uint32_t len;
    char *out;
    if (!chunk || col < 0) return NULL;
    vec = duckdb_data_chunk_get_vector(chunk, (idx_t)col);
    if (ducknng_chunk_value_is_null(vec, row)) return NULL;
    data = (duckdb_string_t *)duckdb_vector_get_data(vec);
    src = duckdb_string_t_data(&data[row]);
    len = duckdb_string_t_length(data[row]);
    out = (char *)duckdb_malloc((size_t)len + 1);
    if (!out) return NULL;
    if (len) memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

static int ducknng_chunk_bool_value(duckdb_data_chunk chunk, int col, idx_t row, int *out) {
    duckdb_vector vec;
    bool *data;
    if (!chunk || col < 0 || !out) return -1;
    vec = duckdb_data_chunk_get_vector(chunk, (idx_t)col);
    if (ducknng_chunk_value_is_null(vec, row)) { *out = 0; return 0; }
    data = (bool *)duckdb_vector_get_data(vec);
    *out = data[row] ? 1 : 0;
    return 0;
}

static int ducknng_chunk_int32_value(duckdb_data_chunk chunk, int col, idx_t row, int32_t *out) {
    duckdb_vector vec;
    int32_t *data;
    if (!chunk || col < 0 || !out) return -1;
    vec = duckdb_data_chunk_get_vector(chunk, (idx_t)col);
    if (ducknng_chunk_value_is_null(vec, row)) return -1;
    data = (int32_t *)duckdb_vector_get_data(vec);
    *out = data[row];
    return 0;
}

static int ducknng_chunk_uint64_value(duckdb_data_chunk chunk, int col, idx_t row, uint64_t *out) {
    duckdb_vector vec;
    uint64_t *data;
    if (!chunk || col < 0 || !out) return -1;
    vec = duckdb_data_chunk_get_vector(chunk, (idx_t)col);
    if (ducknng_chunk_value_is_null(vec, row)) return -1;
    data = (uint64_t *)duckdb_vector_get_data(vec);
    *out = data[row];
    return 0;
}

static uint8_t *ducknng_chunk_blob_dup(duckdb_data_chunk chunk, int col, idx_t row, size_t *out_len) {
    duckdb_vector vec;
    duckdb_string_t *data;
    const char *src;
    uint32_t len;
    uint8_t *out;
    if (out_len) *out_len = 0;
    if (!chunk || col < 0) return NULL;
    vec = duckdb_data_chunk_get_vector(chunk, (idx_t)col);
    if (ducknng_chunk_value_is_null(vec, row)) return NULL;
    data = (duckdb_string_t *)duckdb_vector_get_data(vec);
    src = duckdb_string_t_data(&data[row]);
    len = duckdb_string_t_length(data[row]);
    out = (uint8_t *)duckdb_malloc((size_t)len);
    if (!out && len > 0) return NULL;
    if (len > 0) memcpy(out, src, len);
    if (out_len) *out_len = (size_t)len;
    return out;
}

static int ducknng_parse_authorizer_result(duckdb_result *result,
    ducknng_authorizer_decision *decision, char **errmsg) {
    duckdb_data_chunk chunk = NULL;
    duckdb_data_chunk extra = NULL;
    idx_t rows;
    int allow_col;
    int reason_col;
    int status_col;
    int principal_col;
    int claims_col;
    int ttl_col;
    int allowed = 0;
    int status = 403;
    int32_t status_value = 0;
    uint64_t ttl_value = 0;
    int rc = -1;
    if (errmsg) *errmsg = NULL;
    if (!result || !decision) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing SQL authorizer result");
        return -1;
    }
    chunk = duckdb_fetch_chunk(*result);
    rows = chunk ? duckdb_data_chunk_get_size(chunk) : 0;
    if (rows != 1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: SQL authorizer must return exactly one row");
        goto done;
    }
    extra = duckdb_fetch_chunk(*result);
    if (extra && duckdb_data_chunk_get_size(extra) > 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: SQL authorizer must return exactly one row");
        goto done;
    }
    allow_col = ducknng_result_column_index(result, "allow");
    if (allow_col < 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: SQL authorizer must return an allow column");
        goto done;
    }
    if (duckdb_column_type(result, (idx_t)allow_col) != DUCKDB_TYPE_BOOLEAN ||
        ducknng_chunk_bool_value(chunk, allow_col, 0, &allowed) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: SQL authorizer allow column must be BOOLEAN");
        goto done;
    }
    status_col = ducknng_result_column_index(result, "status");
    if (status_col >= 0 && duckdb_column_type(result, (idx_t)status_col) == DUCKDB_TYPE_INTEGER &&
        ducknng_chunk_int32_value(chunk, status_col, 0, &status_value) == 0 &&
        status_value >= 100 && status_value <= 599) {
        status = (int)status_value;
    }
    reason_col = ducknng_result_column_index(result, "reason");
    principal_col = ducknng_result_column_index(result, "principal");
    claims_col = ducknng_result_column_index(result, "claims_json");
    ttl_col = ducknng_result_column_index(result, "cache_ttl_ms");

    decision->allow = allowed;
    decision->http_status = allowed ? 200 : status;
    if (decision->reason) { duckdb_free(decision->reason); decision->reason = NULL; }
    if (reason_col >= 0) decision->reason = ducknng_chunk_varchar_dup(chunk, reason_col, 0);
    if (!allowed && (!decision->reason || !decision->reason[0])) {
        if (decision->reason) duckdb_free(decision->reason);
        decision->reason = ducknng_strdup("ducknng: SQL authorizer denied request");
    }
    if (decision->principal) { duckdb_free(decision->principal); decision->principal = NULL; }
    if (principal_col >= 0) decision->principal = ducknng_chunk_varchar_dup(chunk, principal_col, 0);
    if (decision->claims_json) { duckdb_free(decision->claims_json); decision->claims_json = NULL; }
    if (claims_col >= 0) decision->claims_json = ducknng_chunk_varchar_dup(chunk, claims_col, 0);
    if (ttl_col >= 0 && duckdb_column_type(result, (idx_t)ttl_col) == DUCKDB_TYPE_UBIGINT &&
        ducknng_chunk_uint64_value(chunk, ttl_col, 0, &ttl_value) == 0) {
        decision->cache_ttl_ms = ttl_value;
    }
    rc = allowed ? 0 : -1;
done:
    if (extra) duckdb_destroy_data_chunk(&extra);
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    return rc;
}

static int ducknng_service_run_sql_authorizer(ducknng_service *svc,
    const ducknng_authorizer_context *auth_ctx, const char *authorizer_sql,
    ducknng_authorizer_decision *decision, char **errmsg) {
    duckdb_result result;
    ducknng_service_sql_scope scope;
    int rc = -1;
    char *parse_err = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc || !svc->rt || !authorizer_sql || !decision) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing SQL authorizer state");
        return -1;
    }
    memset(&result, 0, sizeof(result));
    if (ducknng_service_enter_authorizer_sql(svc, auth_ctx, &scope, errmsg) != 0) {
        decision->allow = 0;
        decision->http_status = 500;
        if (decision->reason) duckdb_free(decision->reason);
        decision->reason = ducknng_strdup(errmsg && *errmsg ? *errmsg : "ducknng: missing SQL authorizer state");
        return -1;
    }
    if (duckdb_query(scope.con, authorizer_sql, &result) == DuckDBError) {
        const char *detail = duckdb_result_error(&result);
        char *msg = NULL;
        if (detail && detail[0]) {
            size_t need = strlen(detail) + 32;
            msg = (char *)duckdb_malloc(need);
            if (msg) snprintf(msg, need, "ducknng: SQL authorizer error: %s", detail);
        }
        if (!msg) msg = ducknng_strdup("ducknng: SQL authorizer error");
        decision->allow = 0;
        decision->http_status = 500;
        if (decision->reason) duckdb_free(decision->reason);
        decision->reason = ducknng_strdup(msg);
        if (errmsg) *errmsg = msg;
        else if (msg) duckdb_free(msg);
        ducknng_service_leave_authorizer_sql(&scope);
        duckdb_destroy_result(&result);
        return -1;
    }
    ducknng_service_leave_authorizer_sql(&scope);
    rc = ducknng_parse_authorizer_result(&result, decision, &parse_err);
    duckdb_destroy_result(&result);
    if (rc != 0 && errmsg) *errmsg = parse_err ? parse_err : ducknng_strdup(decision->reason ? decision->reason : "ducknng: SQL authorizer denied request");
    else if (parse_err) duckdb_free(parse_err);
    return rc;
}

static int ducknng_parse_http_route_result(duckdb_result *result,
    ducknng_http_route_reply *reply, char **errmsg) {
    duckdb_data_chunk chunk = NULL;
    duckdb_data_chunk extra = NULL;
    idx_t rows;
    int status_col;
    int headers_col;
    int content_type_col;
    int body_col;
    int body_text_col;
    int32_t status_value = 0;
    int rc = -1;
    if (errmsg) *errmsg = NULL;
    if (!result || !reply) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP route result");
        return -1;
    }
    chunk = duckdb_fetch_chunk(*result);
    rows = chunk ? duckdb_data_chunk_get_size(chunk) : 0;
    if (rows != 1) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route handler must return exactly one row");
        goto done;
    }
    extra = duckdb_fetch_chunk(*result);
    if (extra && duckdb_data_chunk_get_size(extra) > 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route handler must return exactly one row");
        goto done;
    }
    status_col = ducknng_result_column_index(result, "status");
    if (status_col >= 0) {
        if (duckdb_column_type(result, (idx_t)status_col) != DUCKDB_TYPE_INTEGER ||
            ducknng_chunk_int32_value(chunk, status_col, 0, &status_value) != 0 ||
            status_value < 100 || status_value > 599) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route status column must be INTEGER between 100 and 599");
            goto done;
        }
        reply->status = status_value;
    }
    headers_col = ducknng_result_column_index(result, "headers_json");
    if (headers_col >= 0) {
        if (duckdb_column_type(result, (idx_t)headers_col) != DUCKDB_TYPE_VARCHAR) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route headers_json column must be VARCHAR");
            goto done;
        }
        reply->headers_json = ducknng_chunk_varchar_dup(chunk, headers_col, 0);
        if (!reply->headers_json && !ducknng_chunk_value_is_null(duckdb_data_chunk_get_vector(chunk, (idx_t)headers_col), 0)) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP route headers_json");
            goto done;
        }
    }
    content_type_col = ducknng_result_column_index(result, "content_type");
    if (content_type_col >= 0) {
        if (duckdb_column_type(result, (idx_t)content_type_col) != DUCKDB_TYPE_VARCHAR) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route content_type column must be VARCHAR");
            goto done;
        }
        reply->content_type = ducknng_chunk_varchar_dup(chunk, content_type_col, 0);
        if (!reply->content_type && !ducknng_chunk_value_is_null(duckdb_data_chunk_get_vector(chunk, (idx_t)content_type_col), 0)) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP route content_type");
            goto done;
        }
    }
    body_col = ducknng_result_column_index(result, "body");
    if (body_col >= 0) {
        if (duckdb_column_type(result, (idx_t)body_col) != DUCKDB_TYPE_BLOB) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route body column must be BLOB");
            goto done;
        }
        reply->body = ducknng_chunk_blob_dup(chunk, body_col, 0, &reply->body_len);
        if (!reply->body && reply->body_len > 0) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP route body");
            goto done;
        }
    }
    body_text_col = ducknng_result_column_index(result, "body_text");
    if (body_text_col >= 0) {
        if (duckdb_column_type(result, (idx_t)body_text_col) != DUCKDB_TYPE_VARCHAR) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route body_text column must be VARCHAR");
            goto done;
        }
        reply->body_text = ducknng_chunk_varchar_dup(chunk, body_text_col, 0);
        if (!reply->body_text && !ducknng_chunk_value_is_null(duckdb_data_chunk_get_vector(chunk, (idx_t)body_text_col), 0)) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP route body_text");
            goto done;
        }
    }
    if (reply->body && reply->body_text) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route must return either body or body_text, not both");
        goto done;
    }
    if (!reply->content_type || !reply->content_type[0]) {
        if (reply->content_type) {
            duckdb_free(reply->content_type);
            reply->content_type = NULL;
        }
        if (reply->body_text) reply->content_type = ducknng_strdup("text/plain; charset=utf-8");
        else if (reply->body) reply->content_type = ducknng_strdup("application/octet-stream");
        if ((reply->body_text || reply->body) && !reply->content_type) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory assigning HTTP route content type");
            goto done;
        }
    }
    rc = 0;
done:
    if (extra) duckdb_destroy_data_chunk(&extra);
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    return rc;
}

static const char *ducknng_static_mime_for_ext(const char *ext) {
    if (!ext) return "application/octet-stream";
    if (strcmp(ext, "html") == 0 || strcmp(ext, "htm") == 0) return "text/html";
    if (strcmp(ext, "css") == 0) return "text/css";
    if (strcmp(ext, "js") == 0) return "application/javascript";
    if (strcmp(ext, "json") == 0) return "application/json";
    if (strcmp(ext, "txt") == 0) return "text/plain";
    if (strcmp(ext, "png") == 0) return "image/png";
    if (strcmp(ext, "jpg") == 0 || strcmp(ext, "jpeg") == 0) return "image/jpeg";
    if (strcmp(ext, "gif") == 0) return "image/gif";
    if (strcmp(ext, "svg") == 0) return "image/svg+xml";
    if (strcmp(ext, "ico") == 0) return "image/x-icon";
    if (strcmp(ext, "wasm") == 0) return "application/wasm";
    if (strcmp(ext, "xml") == 0) return "application/xml";
    return "application/octet-stream";
}

static int ducknng_path_has_traversal(const char *rel_path) {
    const char *p = rel_path;
    if (!p) return 0;
    while (*p) {
        if (p[0] == '.' && p[1] == '.' && (p[2] == '/' || p[2] == '\\' || p[2] == '\0')) return 1;
        while (*p && *p != '/' && *p != '\\') p++;
        if (*p) p++;
    }
    return 0;
}

static int ducknng_serve_static_file(const char *static_dir, const char *route_prefix,
    const char *request_path, ducknng_http_route_reply *reply, char **errmsg) {
    const char *rel;
    size_t dir_len, rel_len, path_len;
    char *full_path;
    const char *ext;
    const char *mime;
    FILE *f;
    long file_size;
    uint8_t *buf;
    size_t nread;
    if (errmsg) *errmsg = NULL;
    if (!static_dir || !request_path || !reply) return -1;
    /* compute relative path: strip route_prefix from request_path */
    rel = request_path;
    if (route_prefix && route_prefix[0]) {
        size_t prefix_len = strlen(route_prefix);
        if (strncmp(request_path, route_prefix, prefix_len) == 0)
            rel = request_path + prefix_len;
    }
    while (*rel == '/' || *rel == '\\') rel++;
    if (!rel[0]) rel = "index.html";
    if (ducknng_path_has_traversal(rel)) {
        reply->status = 403;
        reply->body_text = ducknng_strdup("ducknng: path traversal not permitted");
        return 0;
    }
    dir_len = strlen(static_dir);
    rel_len = strlen(rel);
    path_len = dir_len + 1 + rel_len + 1;
    full_path = (char *)duckdb_malloc(path_len);
    if (!full_path) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building static path");
        return -1;
    }
    memcpy(full_path, static_dir, dir_len);
    full_path[dir_len] = '/';
    memcpy(full_path + dir_len + 1, rel, rel_len + 1);
    /* find extension */
    ext = NULL;
    {
        const char *dot = NULL;
        const char *p = full_path;
        while (*p) { if (*p == '.') dot = p + 1; p++; }
        if (dot && dot > full_path) ext = dot;
    }
    mime = ducknng_static_mime_for_ext(ext);
    f = fopen(full_path, "rb");
    duckdb_free(full_path);
    if (!f) {
        reply->status = 404;
        reply->body_text = ducknng_strdup("Not Found");
        return 0;
    }
    fseek(f, 0, SEEK_END);
    file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (file_size < 0) {
        fclose(f);
        reply->status = 500;
        reply->body_text = ducknng_strdup("ducknng: failed to stat static file");
        return 0;
    }
    buf = (uint8_t *)duckdb_malloc((size_t)file_size + 1);
    if (!buf) {
        fclose(f);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory reading static file");
        return -1;
    }
    nread = fread(buf, 1, (size_t)file_size, f);
    fclose(f);
    buf[nread] = 0;
    reply->status = 200;
    reply->content_type = ducknng_strdup(mime);
    reply->body = buf;
    reply->body_len = nread;
    return 0;
}

int ducknng_service_handle_http_route(ducknng_service *svc,
    const ducknng_http_request_context *request_ctx, ducknng_http_route_reply *reply, char **errmsg) {
    duckdb_result result;
    ducknng_service_sql_scope scope;
    int rc = -1;
    if (errmsg) *errmsg = NULL;
    if (!svc || !request_ctx || !reply) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP route execution state");
        return -1;
    }
    /* static file serving: bypass SQL */
    if (request_ctx->route.static_dir_path && request_ctx->route.static_dir_path[0]) {
        return ducknng_serve_static_file(
            request_ctx->route.static_dir_path, request_ctx->route.path,
            request_ctx->path, reply, errmsg);
    }
    if (!svc->rt || !request_ctx->route.handler_sql || !request_ctx->route.handler_sql[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP route execution state");
        return -1;
    }
    memset(&result, 0, sizeof(result));
    if (ducknng_service_enter_http_route_sql(svc, request_ctx, &scope, errmsg) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP route execution state");
        return -1;
    }
    if (duckdb_query(scope.con, request_ctx->route.handler_sql, &result) == DuckDBError) {
        const char *detail = duckdb_result_error(&result);
        size_t need = detail && detail[0] ? strlen(detail) + 40 : 0;
        if (detail && detail[0] && errmsg) {
            *errmsg = (char *)duckdb_malloc(need);
            if (*errmsg) snprintf(*errmsg, need, "ducknng: HTTP route handler error: %s", detail);
        }
        ducknng_service_leave_http_route_sql(&scope);
        duckdb_destroy_result(&result);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route handler error");
        return -1;
    }
    ducknng_service_leave_http_route_sql(&scope);
    rc = ducknng_parse_http_route_result(&result, reply, errmsg);
    duckdb_destroy_result(&result);
    return rc;
}

int ducknng_service_execute_stream_route(ducknng_service *svc,
    const ducknng_http_request_context *request_ctx,
    int (*on_chunk)(const void *data, size_t len, void *user_data),
    void *user_data, char **errmsg) {
    duckdb_result result;
    ducknng_service_sql_scope scope;
    duckdb_data_chunk chunk = NULL;
    int chunk_col = -1;
    int rc = -1;
    duckdb_type chunk_col_type = DUCKDB_TYPE_INVALID;
    if (errmsg) *errmsg = NULL;
    if (!svc || !request_ctx || !on_chunk) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing stream route execution state");
        return -1;
    }
    if (!svc->rt || !request_ctx->route.handler_sql || !request_ctx->route.handler_sql[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing stream route SQL");
        return -1;
    }
    memset(&result, 0, sizeof(result));
    if (ducknng_service_enter_http_route_sql(svc, request_ctx, &scope, errmsg) != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing stream route execution state");
        return -1;
    }
    if (duckdb_query(scope.con, request_ctx->route.handler_sql, &result) == DuckDBError) {
        const char *detail = duckdb_result_error(&result);
        size_t need = detail && detail[0] ? strlen(detail) + 40 : 0;
        if (detail && detail[0] && errmsg) {
            *errmsg = (char *)duckdb_malloc(need);
            if (*errmsg) snprintf(*errmsg, need, "ducknng: stream route handler error: %s", detail);
        }
        ducknng_service_leave_http_route_sql(&scope);
        duckdb_destroy_result(&result);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: stream route handler error");
        return -1;
    }
    ducknng_service_leave_http_route_sql(&scope);
    chunk_col = ducknng_result_column_index(&result, "chunk");
    if (chunk_col >= 0) {
        chunk_col_type = duckdb_column_type(&result, (idx_t)chunk_col);
        if (chunk_col_type != DUCKDB_TYPE_VARCHAR && chunk_col_type != DUCKDB_TYPE_BLOB) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: stream route chunk column must be VARCHAR or BLOB");
            duckdb_destroy_result(&result);
            return -1;
        }
    }
    rc = 0;
    while ((chunk = duckdb_fetch_chunk(result)) != NULL) {
        idx_t rows = duckdb_data_chunk_get_size(chunk);
        idx_t row;
        for (row = 0; row < rows && rc == 0; row++) {
            if (chunk_col < 0) continue;
            if (ducknng_chunk_value_is_null(duckdb_data_chunk_get_vector(chunk, (idx_t)chunk_col), row)) continue;
            if (chunk_col_type == DUCKDB_TYPE_VARCHAR) {
                char *val = ducknng_chunk_varchar_dup(chunk, chunk_col, row);
                if (val) {
                    rc = on_chunk(val, strlen(val), user_data);
                    duckdb_free(val);
                }
            } else {
                size_t blob_len = 0;
                uint8_t *blob = ducknng_chunk_blob_dup(chunk, chunk_col, row, &blob_len);
                if (blob) {
                    rc = on_chunk(blob, blob_len, user_data);
                    duckdb_free(blob);
                }
            }
        }
        duckdb_destroy_data_chunk(&chunk);
        if (rc != 0) break;
    }
    duckdb_destroy_result(&result);
    return rc;
}

int ducknng_service_authorize_request(ducknng_service *svc, const ducknng_authorizer_context *auth_ctx,
    ducknng_authorizer_decision *decision, char **errmsg) {
    char *authorizer_sql = NULL;
    char *local_err = NULL;
    int authorizer_was_active = 0;
    int rc;
    if (errmsg) *errmsg = NULL;
    if (!decision) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing authorizer decision");
        return -1;
    }
    decision->allow = 1;
    decision->http_status = 200;
    rc = ducknng_service_network_admission_check(svc,
        auth_ctx ? auth_ctx->caller_identity : NULL,
        auth_ctx ? auth_ctx->remote_addr : NULL,
        &local_err);
    if (rc != 0) {
        decision->allow = 0;
        decision->http_status = 403;
        if (decision->reason) duckdb_free(decision->reason);
        decision->reason = local_err ? local_err : ducknng_strdup("ducknng: peer is not admitted");
        if (errmsg) *errmsg = ducknng_strdup(decision->reason ? decision->reason : "ducknng: peer is not admitted");
        return -1;
    }
    if (!svc) {
        decision->allow = 0;
        decision->http_status = 500;
        if (decision->reason) duckdb_free(decision->reason);
        decision->reason = ducknng_strdup("ducknng: missing service for authorization");
        if (errmsg) *errmsg = ducknng_strdup(decision->reason);
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    authorizer_was_active = svc->authorizer_active && svc->authorizer_sql && svc->authorizer_sql[0];
    if (authorizer_was_active) {
        authorizer_sql = ducknng_strdup(svc->authorizer_sql);
    }
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (authorizer_was_active && !authorizer_sql) {
        decision->allow = 0;
        decision->http_status = 500;
        if (decision->reason) duckdb_free(decision->reason);
        decision->reason = ducknng_strdup("ducknng: failed to copy SQL authorizer");
        if (errmsg) *errmsg = ducknng_strdup(decision->reason);
        return -1;
    }
    if (!authorizer_sql) return 0;
    rc = ducknng_service_run_sql_authorizer(svc, auth_ctx, authorizer_sql, decision, &local_err);
    duckdb_free(authorizer_sql);
    if (rc != 0) {
        decision->allow = 0;
        if (decision->http_status == 200) decision->http_status = 403;
        if (local_err && (!decision->reason || !decision->reason[0])) {
            if (decision->reason) duckdb_free(decision->reason);
            decision->reason = ducknng_strdup(local_err);
        }
        if (errmsg) *errmsg = local_err ? local_err : ducknng_strdup(decision->reason ? decision->reason : "ducknng: SQL authorizer denied request");
        else if (local_err) duckdb_free(local_err);
        return -1;
    }
    if (local_err) duckdb_free(local_err);
    return 0;
}

static nng_msg *ducknng_dispatch_request(ducknng_service *svc, const ducknng_frame *frame,
    const char *caller_identity, const ducknng_authorizer_decision *auth_decision) {
    const ducknng_method_descriptor *method = NULL;
    ducknng_method_descriptor method_snapshot;
    ducknng_request_context req_ctx;
    ducknng_method_reply reply;
    nng_msg *msg;

    memset(&req_ctx, 0, sizeof(req_ctx));
    ducknng_method_reply_init(&reply);

    if (!svc || !svc->rt || !frame) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing dispatcher state");
    }
    {
        char *admission_err = NULL;
        if (ducknng_service_peer_admission_check(svc, caller_identity, &admission_err) != 0) {
            nng_msg *err = ducknng_error_msg(NULL, DUCKNNG_STATUS_UNAUTHORIZED,
                admission_err ? admission_err : "ducknng: peer identity is not admitted");
            if (admission_err) duckdb_free(admission_err);
            return err;
        }
    }
    ducknng_service_prune_idle_sessions(svc, ducknng_now_ms());
    memset(&method_snapshot, 0, sizeof(method_snapshot));
    ducknng_mutex_lock(&svc->rt->mu);
    if (frame->type == DUCKNNG_RPC_MANIFEST) {
        method = ducknng_method_registry_find(&svc->rt->registry,
            (const uint8_t *)"manifest", (uint32_t)strlen("manifest"));
    } else if (frame->type == DUCKNNG_RPC_CALL) {
        method = ducknng_method_registry_find(&svc->rt->registry, frame->name, frame->name_len);
    } else {
        ducknng_mutex_unlock(&svc->rt->mu);
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INVALID,
            "ducknng: unsupported message type");
    }
    if (method) {
        method_snapshot = *method;
        method = &method_snapshot;
    }
    ducknng_mutex_unlock(&svc->rt->mu);
    if (!method) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_NOT_FOUND,
            "ducknng: unknown RPC method");
    }
    if (method->disabled) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_DISABLED,
            "ducknng: RPC method is disabled");
    }
    if (frame->flags & ~method->accepted_request_flags) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INVALID,
            "ducknng: request used unsupported flag bits for this method");
    }
    if (method->max_request_bytes > 0 && frame->payload_len > (uint64_t)method->max_request_bytes) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INVALID,
            "ducknng: request payload exceeds method limit");
    }
    if (method->requires_auth && (!caller_identity || !caller_identity[0])) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_UNAUTHORIZED,
            "ducknng: method requires verified peer identity");
    }

    req_ctx.frame = frame;
    req_ctx.caller_identity = caller_identity;
    req_ctx.auth_principal = auth_decision ? auth_decision->principal : NULL;
    req_ctx.auth_claims_json = auth_decision ? auth_decision->claims_json : NULL;
    {
        ducknng_execution_subject subject_ctx;
        int handler_rc;
        ducknng_service_execution_subject_begin(svc, &subject_ctx,
            caller_identity, auth_decision);
        handler_rc = method->handler(svc, method, &req_ctx, &reply);
        ducknng_service_execution_subject_end(svc);
        if (handler_rc != 0 && reply.type != DUCKNNG_RPC_ERROR) {
            ducknng_method_reply_reset(&reply);
            return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
                "ducknng: method handler failed without structured error reply");
        }
    }
    if (reply.type == DUCKNNG_RPC_RESULT && (reply.flags & ~method->emitted_reply_flags)) {
        ducknng_method_reply_reset(&reply);
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
            "ducknng: method emitted unsupported reply flags");
    }
    if (method->max_reply_bytes > 0 && reply.payload_len > method->max_reply_bytes) {
        ducknng_method_reply_reset(&reply);
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_BUSY,
            "ducknng: reply payload exceeds method limit");
    }

    if (reply.prebuilt_msg) {
        ducknng_frame_parts parts;
        size_t expected_prefix = DUCKNNG_WIRE_HEADER_LEN;
        size_t message_size = 0;
        size_t prefix_written = 0;
        memset(&parts, 0, sizeof(parts));
        if (ducknng_size_add(expected_prefix, strlen(method->name),
                &expected_prefix) != 0 ||
            ducknng_size_add(expected_prefix,
                reply.error ? strlen(reply.error) : 0, &expected_prefix) != 0 ||
            ducknng_size_add(expected_prefix, reply.payload_len,
                &message_size) != 0 ||
            reply.prefix_size != expected_prefix ||
            nng_msg_len(reply.prebuilt_msg) != message_size || reply.payload) {
            ducknng_method_reply_reset(&reply);
            return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
                "ducknng: prebuilt reply message has inconsistent sizing");
        }
        parts.type = reply.type;
        parts.status = reply.status;
        parts.flags = reply.flags;
        parts.name = (const uint8_t *)method->name;
        parts.name_len = strlen(method->name);
        parts.error = (const uint8_t *)reply.error;
        parts.error_len = reply.error ? strlen(reply.error) : 0;
        parts.payload_len = reply.payload_len;
        if (ducknng_encode_frame_prefix(&parts,
                (uint8_t *)nng_msg_body(reply.prebuilt_msg),
                reply.prefix_size, &prefix_written) != 0 ||
            prefix_written != reply.prefix_size) {
            ducknng_method_reply_reset(&reply);
            return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
                "ducknng: failed to encode prebuilt reply prefix");
        }
        msg = reply.prebuilt_msg;
        reply.prebuilt_msg = NULL;
    } else {
        msg = ducknng_build_reply_status(reply.type, method->name, reply.flags,
            reply.status, reply.error, reply.payload,
            (uint64_t)reply.payload_len);
    }
    ducknng_method_reply_reset(&reply);
    if (!msg) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
            "ducknng: failed to allocate reply message");
    }
    return msg;
}

static void ducknng_rep_aio_cb(void *arg) {
    ducknng_rep_ctx *rc = (ducknng_rep_ctx *)arg;
    int rv = nng_aio_result(rc->aio);
    ducknng_mutex_lock(&rc->mu);
    if (rc->stopping) {
        ducknng_mutex_unlock(&rc->mu);
        return;
    }
    if (rv != 0) {
        nng_msg *pending_msg = nng_aio_get_msg(rc->aio);
        if (pending_msg) {
            nng_msg_free(pending_msg);
            nng_aio_set_msg(rc->aio, NULL);
        }
        rc->last_nng_err = rv;
        rc->event = DUCKNNG_EVT_NNG_ERROR;
        ducknng_cond_signal(&rc->cv);
        ducknng_mutex_unlock(&rc->mu);
        return;
    }
    if (rc->phase == DUCKNNG_PHASE_RECV) {
        rc->request_msg = nng_aio_get_msg(rc->aio);
        nng_aio_set_msg(rc->aio, NULL);
        rc->event = DUCKNNG_EVT_REQUEST_READY;
        ducknng_cond_signal(&rc->cv);
        ducknng_mutex_unlock(&rc->mu);
        return;
    }
    rc->event = DUCKNNG_EVT_SEND_DONE;
    ducknng_cond_signal(&rc->cv);
    ducknng_mutex_unlock(&rc->mu);
}

static void ducknng_rep_ctx_teardown(ducknng_rep_ctx *rc) {
    if (!rc) return;
    if (rc->mu_initialized) {
        ducknng_mutex_lock(&rc->mu);
        rc->stopping = 1;
        ducknng_cond_signal(&rc->cv);
        ducknng_mutex_unlock(&rc->mu);
    }
    if (rc->aio) ducknng_aio_cancel(rc->aio);
    if (rc->thread_started) {
        ducknng_thread_join(rc->thread);
        rc->thread_started = 0;
    }
    if (rc->aio) {
        ducknng_aio_wait(rc->aio);
        if (nng_aio_get_msg(rc->aio)) {
            nng_msg_free(nng_aio_get_msg(rc->aio));
            nng_aio_set_msg(rc->aio, NULL);
        }
    }
    if (rc->request_msg) {
        nng_msg_free(rc->request_msg);
        rc->request_msg = NULL;
    }
    if (rc->aio) {
        ducknng_aio_free(rc->aio);
        rc->aio = NULL;
    }
    if (rc->ctx.id != 0) {
        ducknng_ctx_close(rc->ctx);
        memset(&rc->ctx, 0, sizeof(rc->ctx));
    }
    if (rc->cv_initialized) {
        ducknng_cond_destroy(&rc->cv);
        rc->cv_initialized = 0;
    }
    if (rc->mu_initialized) {
        ducknng_mutex_destroy(&rc->mu);
        rc->mu_initialized = 0;
    }
}

nng_msg *ducknng_handle_decoded_request(ducknng_service *svc, const ducknng_frame *frame,
    const char *caller_identity, const ducknng_authorizer_decision *decision) {
    return ducknng_dispatch_request(svc, frame, caller_identity, decision);
}

/*
 * Decode one framed request payload and run it through the full server-side
 * gate: network admission, inflight accounting, authorization, and dispatch.
 * Shared by the HTTP POST frame handler and the WebSocket frame handler so peer
 * identity, authorizer principals, subject-restricted profiles, request limits,
 * and session binding are byte-identical across both carriers (issue #11).
 *
 * On success returns the reply message (caller frees). On any pre-dispatch
 * rejection or a dispatch failure, returns NULL and sets *out_status to an
 * HTTP-style status and *out_error to a duckdb_malloc'd message (caller frees;
 * may be NULL). transport_family and the request phase match the HTTP handler;
 * scheme/method/path/content_type identify the specific carrier for policy.
 */
nng_msg *ducknng_service_authorize_and_dispatch_frame(ducknng_service *svc,
    const uint8_t *body, size_t body_len, const char *caller_identity,
    const nng_sockaddr *remote_addr, ducknng_transport_scheme scheme,
    const char *http_method, const char *http_path, const char *content_type,
    uint16_t *out_status, char **out_error) {
    ducknng_frame frame;
    ducknng_authorizer_context auth_ctx;
    ducknng_authorizer_decision decision;
    char *admission_err = NULL;
    char *limit_err = NULL;
    nng_msg *reply_msg = NULL;

    if (out_status) *out_status = 500;
    if (out_error) *out_error = NULL;
    if (!svc) {
        if (out_error) *out_error = ducknng_strdup("ducknng: missing HTTP server state");
        return NULL;
    }
    if (ducknng_decode_frame_bytes(body, body_len, &frame) != 0) {
        if (out_status) *out_status = 400;
        if (out_error) *out_error = ducknng_strdup("ducknng: invalid frame envelope");
        return NULL;
    }
    memset(&auth_ctx, 0, sizeof(auth_ctx));
    auth_ctx.svc = svc;
    auth_ctx.frame = &frame;
    auth_ctx.phase = "rpc_request";
    auth_ctx.transport_family = DUCKNNG_TRANSPORT_FAMILY_HTTP;
    auth_ctx.scheme = scheme;
    auth_ctx.caller_identity = caller_identity;
    auth_ctx.remote_addr = remote_addr;
    auth_ctx.http_method = http_method;
    auth_ctx.http_path = http_path;
    auth_ctx.content_type = content_type;
    auth_ctx.body_bytes = (uint64_t)body_len;
    ducknng_authorizer_decision_init(&decision);
    if (ducknng_service_network_admission_check(svc, caller_identity, remote_addr, &admission_err) != 0) {
        if (out_status) *out_status = 403;
        if (out_error) *out_error = admission_err ? admission_err :
            ducknng_strdup("ducknng: peer is not admitted");
        else if (admission_err) duckdb_free(admission_err);
        ducknng_authorizer_decision_reset(&decision);
        return NULL;
    }
    if (ducknng_service_begin_request(svc, caller_identity, &limit_err) != 0) {
        if (out_status) *out_status = 503;
        if (out_error) *out_error = limit_err ? limit_err :
            ducknng_strdup("ducknng: max inflight requests exceeded");
        else if (limit_err) duckdb_free(limit_err);
        ducknng_authorizer_decision_reset(&decision);
        return NULL;
    }
    if (ducknng_service_authorize_request(svc, &auth_ctx, &decision, NULL) != 0) {
        if (out_status) *out_status = (decision.http_status >= 100 && decision.http_status <= 599) ?
            (uint16_t)decision.http_status : 403;
        if (out_error) *out_error = ducknng_strdup(decision.reason ? decision.reason :
            "ducknng: request is not authorized");
        ducknng_authorizer_decision_reset(&decision);
        ducknng_service_end_request(svc, caller_identity, 0);
        return NULL;
    }
    reply_msg = ducknng_handle_decoded_request(svc, &frame, caller_identity, &decision);
    ducknng_authorizer_decision_reset(&decision);
    ducknng_service_end_request(svc, caller_identity, reply_msg ? nng_msg_len(reply_msg) : 0);
    if (!reply_msg) {
        if (out_status) *out_status = 500;
        if (out_error) *out_error = ducknng_strdup("ducknng: failed to dispatch request");
        return NULL;
    }
    if (out_status) *out_status = 200;
    return reply_msg;
}

nng_msg *ducknng_handle_request_with_identity(ducknng_service *svc, nng_msg *req,
    const char *caller_identity) {
    ducknng_frame frame;
    ducknng_authorizer_context auth_ctx;
    ducknng_authorizer_decision decision;
    nng_msg *reply;
    char *admission_err = NULL;
    char *limit_err = NULL;
    if (ducknng_decode_request(req, &frame) != 0) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INVALID, "invalid RPC envelope");
    }
    memset(&auth_ctx, 0, sizeof(auth_ctx));
    auth_ctx.svc = svc;
    auth_ctx.frame = &frame;
    auth_ctx.phase = "rpc_request";
    auth_ctx.transport_family = DUCKNNG_TRANSPORT_FAMILY_NNG;
    auth_ctx.caller_identity = caller_identity;
    ducknng_authorizer_decision_init(&decision);
    if (ducknng_service_network_admission_check(svc, caller_identity, NULL, &admission_err) != 0) {
        reply = ducknng_error_msg(NULL, DUCKNNG_STATUS_UNAUTHORIZED,
            admission_err ? admission_err : "ducknng: peer is not admitted");
        if (admission_err) duckdb_free(admission_err);
        ducknng_authorizer_decision_reset(&decision);
        return reply;
    }
    if (ducknng_service_begin_request(svc, caller_identity, &limit_err) != 0) {
        reply = ducknng_error_msg(NULL, DUCKNNG_STATUS_BUSY,
            limit_err ? limit_err : "ducknng: max inflight requests exceeded");
        if (limit_err) duckdb_free(limit_err);
        ducknng_authorizer_decision_reset(&decision);
        return reply;
    }
    if (ducknng_service_authorize_request(svc, &auth_ctx, &decision, NULL) != 0) {
        reply = ducknng_error_msg(NULL, DUCKNNG_STATUS_UNAUTHORIZED,
            decision.reason ? decision.reason : "ducknng: request is not authorized");
        ducknng_authorizer_decision_reset(&decision);
        ducknng_service_end_request(svc, caller_identity, 0);
        return reply;
    }
    reply = ducknng_dispatch_request(svc, &frame, caller_identity, &decision);
    ducknng_authorizer_decision_reset(&decision);
    ducknng_service_end_request(svc, caller_identity, reply ? nng_msg_len(reply) : 0);
    return reply;
}

nng_msg *ducknng_handle_request(ducknng_service *svc, nng_msg *req) {
    nng_pipe pipe;
    nng_sockaddr remote_addr;
    ducknng_frame frame;
    ducknng_authorizer_context auth_ctx;
    ducknng_authorizer_decision decision;
    int have_remote_addr;
    char *caller_identity;
    char *admission_err = NULL;
    char *limit_err = NULL;
    nng_msg *reply;
    memset(&pipe, 0, sizeof(pipe));
    memset(&auth_ctx, 0, sizeof(auth_ctx));
    if (req) pipe = nng_msg_get_pipe(req);
    have_remote_addr = ducknng_pipe_remote_addr(pipe, &remote_addr) == 0;
    caller_identity = ducknng_msg_verified_peer_identity(req);
    if (ducknng_decode_request(req, &frame) != 0) {
        if (caller_identity) duckdb_free(caller_identity);
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INVALID, "invalid RPC envelope");
    }
    auth_ctx.svc = svc;
    auth_ctx.frame = &frame;
    auth_ctx.phase = "rpc_request";
    auth_ctx.transport_family = DUCKNNG_TRANSPORT_FAMILY_NNG;
    auth_ctx.remote_addr = have_remote_addr ? &remote_addr : NULL;
    auth_ctx.caller_identity = caller_identity;
    ducknng_authorizer_decision_init(&decision);
    if (ducknng_service_network_admission_check(svc, caller_identity,
            have_remote_addr ? &remote_addr : NULL, &admission_err) != 0) {
        reply = ducknng_error_msg(NULL, DUCKNNG_STATUS_UNAUTHORIZED,
            admission_err ? admission_err : "ducknng: peer is not admitted");
        if (admission_err) duckdb_free(admission_err);
        ducknng_authorizer_decision_reset(&decision);
        if (caller_identity) duckdb_free(caller_identity);
        return reply;
    }
    if (ducknng_service_begin_request(svc, caller_identity, &limit_err) != 0) {
        reply = ducknng_error_msg(NULL, DUCKNNG_STATUS_BUSY,
            limit_err ? limit_err : "ducknng: max inflight requests exceeded");
        if (limit_err) duckdb_free(limit_err);
        ducknng_authorizer_decision_reset(&decision);
        if (caller_identity) duckdb_free(caller_identity);
        return reply;
    }
    if (ducknng_service_authorize_request(svc, &auth_ctx, &decision, NULL) != 0) {
        reply = ducknng_error_msg(NULL, DUCKNNG_STATUS_UNAUTHORIZED,
            decision.reason ? decision.reason : "ducknng: request is not authorized");
        ducknng_authorizer_decision_reset(&decision);
        ducknng_service_end_request(svc, caller_identity, 0);
        if (caller_identity) duckdb_free(caller_identity);
        return reply;
    }
    reply = ducknng_dispatch_request(svc, &frame, caller_identity, &decision);
    ducknng_authorizer_decision_reset(&decision);
    ducknng_service_end_request(svc, caller_identity, reply ? nng_msg_len(reply) : 0);
    if (caller_identity) duckdb_free(caller_identity);
    return reply;
}

static void *ducknng_rep_worker_main(void *arg) {
    ducknng_rep_ctx *rc = (ducknng_rep_ctx *)arg;
    ducknng_mutex_lock(&rc->mu);
    rc->phase = DUCKNNG_PHASE_RECV;
    ducknng_mutex_unlock(&rc->mu);
    nng_ctx_recv(rc->ctx, rc->aio);
    for (;;) {
        int event, nng_err, stopping;
        nng_msg *req = NULL;
        ducknng_mutex_lock(&rc->mu);
        while (rc->event == DUCKNNG_EVT_NONE && !rc->stopping) {
            ducknng_cond_wait(&rc->cv, &rc->mu);
        }
        event = rc->event;
        rc->event = DUCKNNG_EVT_NONE;
        nng_err = rc->last_nng_err;
        rc->last_nng_err = 0;
        req = rc->request_msg;
        rc->request_msg = NULL;
        stopping = rc->stopping;
        ducknng_mutex_unlock(&rc->mu);
        if (stopping) break;
        if (event == DUCKNNG_EVT_NNG_ERROR) {
            if (nng_err == NNG_ECLOSED) break;
            ducknng_mutex_lock(&rc->mu);
            rc->phase = DUCKNNG_PHASE_RECV;
            ducknng_mutex_unlock(&rc->mu);
            nng_ctx_recv(rc->ctx, rc->aio);
            continue;
        }
        if (event == DUCKNNG_EVT_REQUEST_READY) {
            nng_msg *resp = ducknng_handle_request(rc->svc, req);
            nng_msg_free(req);
            ducknng_mutex_lock(&rc->mu);
            rc->phase = DUCKNNG_PHASE_SEND;
            ducknng_mutex_unlock(&rc->mu);
            nng_aio_set_msg(rc->aio, resp);
            nng_ctx_send(rc->ctx, rc->aio);
            continue;
        }
        if (event == DUCKNNG_EVT_SEND_DONE) {
            ducknng_mutex_lock(&rc->mu);
            rc->phase = DUCKNNG_PHASE_RECV;
            ducknng_mutex_unlock(&rc->mu);
            nng_ctx_recv(rc->ctx, rc->aio);
            continue;
        }
    }
    return NULL;
}

ducknng_service *ducknng_service_create(ducknng_runtime *rt, const char *name, const char *listen_url,
    int contexts, size_t recv_max_bytes, uint64_t session_idle_ms,
    uint64_t tls_config_id, const char *tls_config_source, const ducknng_tls_opts *tls_opts) {
    ducknng_service *svc = (ducknng_service *)duckdb_malloc(sizeof(*svc));
    if (!svc) return NULL;
    memset(svc, 0, sizeof(*svc));
    atomic_store_explicit(&svc->session_count_visible, 0, memory_order_release);
    atomic_store_explicit(&svc->pipe_state_count_visible, 0, memory_order_release);
    atomic_store_explicit(&svc->inflight_request_count_visible, 0, memory_order_release);
    ducknng_tls_opts_init(&svc->tls_opts);
    svc->rt = rt;
    svc->name = ducknng_strdup(name);
    svc->listen_url = ducknng_strdup(listen_url);
    svc->ncontexts = contexts > 0 ? contexts : 1;
    svc->recv_max_bytes = recv_max_bytes ? recv_max_bytes : 134217728u;
    svc->session_idle_ms = session_idle_ms ? session_idle_ms : 300000u;
    svc->tls_config_id = tls_config_id;
    svc->next_session_id = 1;
    svc->next_http_route_id = 1;
    svc->execution_model = DUCKNNG_EXECUTION_SHARED_SERIALIZED_CONNECTION;
    svc->execution_pool_index = (size_t)-1;
    svc->tls_config_source = ducknng_strdup(tls_config_source);
    if ((tls_config_source && !svc->tls_config_source) || ducknng_tls_opts_copy(&svc->tls_opts, tls_opts) != 0) {
        ducknng_service_destroy(svc);
        return NULL;
    }
    if (ducknng_mutex_init(&svc->mu) != 0) {
        ducknng_service_destroy(svc);
        return NULL;
    }
    svc->mu_initialized = 1;
    if (ducknng_mutex_init(&svc->execution_mu) != 0) {
        ducknng_service_destroy(svc);
        return NULL;
    }
    svc->execution_mu_initialized = 1;
    return svc;
}

static void ducknng_service_clear_peer_identity_states(ducknng_service *svc);
static ducknng_peer_identity_state *ducknng_service_find_or_create_peer_identity_locked(ducknng_service *svc, const char *identity);

static void ducknng_http_worker_destroy(ducknng_http_worker *w) {
    if (!w) return;
    if (w->mu_initialized) ducknng_mutex_lock(&w->mu);
    w->stopping = 1;
    if (w->cv_initialized) ducknng_cond_broadcast(&w->cv);
    if (w->mu_initialized) ducknng_mutex_unlock(&w->mu);
    if (w->thread_started) {
        ducknng_thread_join(w->thread);
        w->thread_started = 0;
    }
    if (w->cv_initialized) {
        ducknng_cond_destroy(&w->cv);
        w->cv_initialized = 0;
    }
    if (w->mu_initialized) {
        ducknng_mutex_destroy(&w->mu);
        w->mu_initialized = 0;
    }
    if (w->name) duckdb_free(w->name);
    if (w->sql) duckdb_free(w->sql);
    duckdb_free(w);
}

static void ducknng_service_stop_http_workers(ducknng_service *svc) {
    ducknng_http_worker **workers;
    size_t count;
    size_t i;
    if (!svc || !svc->http_workers) return;
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    workers = svc->http_workers;
    count = svc->http_worker_count;
    svc->http_workers = NULL;
    svc->http_worker_count = 0;
    svc->http_worker_cap = 0;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    for (i = 0; i < count; i++) ducknng_http_worker_destroy(workers[i]);
    duckdb_free(workers);
}

void ducknng_service_destroy(ducknng_service *svc) {
    ducknng_session **sessions = NULL;
    size_t session_count = 0;
    size_t i;
    if (!svc) return;
    if (svc->running || svc->http_state || svc->ctxs || svc->listener.id != 0 || svc->rep_sock.id != 0) {
        (void)ducknng_service_stop(svc, NULL);
    }
    ducknng_service_stop_http_workers(svc);
    sessions = ducknng_service_detach_all_sessions(svc, &session_count);
    if (svc->name) duckdb_free(svc->name);
    if (svc->listen_url) duckdb_free(svc->listen_url);
    if (svc->resolved_listen_url) duckdb_free(svc->resolved_listen_url);
    if (svc->tls_config_source) duckdb_free(svc->tls_config_source);
    ducknng_tls_opts_reset(&svc->tls_opts);
    ducknng_service_clear_ip_allowlist(svc);
    ducknng_service_clear_authorizer(svc);
    ducknng_service_clear_http_routes(svc);
    ducknng_service_clear_pipe_events(svc);
    ducknng_service_clear_pipe_states(svc);
    ducknng_service_clear_peer_identity_states(svc);
    for (i = 0; i < session_count; i++) {
        if (sessions && sessions[i]) ducknng_session_destroy(sessions[i]);
    }
    if (sessions) duckdb_free(sessions);
    if (svc->ctxs) duckdb_free(svc->ctxs);
    if (svc->execution_con) {
        ducknng_service_release_execution_connection(svc, svc->execution_pool_index);
        svc->execution_con = NULL;
        svc->execution_pool_index = (size_t)-1;
    }
    if (svc->execution_mu_initialized) {
        ducknng_mutex_destroy(&svc->execution_mu);
        svc->execution_mu_initialized = 0;
    }
    if (svc->mu_initialized) {
        ducknng_mutex_destroy(&svc->mu);
        svc->mu_initialized = 0;
    }
    duckdb_free(svc);
}

/* --- HTTP worker lifecycle --- */

static void *ducknng_http_worker_thread_main(void *arg) {
    ducknng_http_worker *w = (ducknng_http_worker *)arg;
    ducknng_service *svc = w->svc;
    ducknng_mutex_lock(&w->mu);
    while (!w->stopping) {
        char *sql;
        ducknng_cond_timedwait_ms(&w->cv, &w->mu, w->interval_ms);
        if (w->stopping) break;
        sql = w->sql ? ducknng_strdup(w->sql) : NULL;
        ducknng_mutex_unlock(&w->mu);
        if (sql && svc) {
            ducknng_service_sql_scope scope;
            memset(&scope, 0, sizeof(scope));
            if (ducknng_service_enter_request_sql(svc, &scope, NULL) == 0) {
                duckdb_result result;
                memset(&result, 0, sizeof(result));
                (void)duckdb_query(scope.con, sql, &result);
                duckdb_destroy_result(&result);
                ducknng_service_leave_request_sql(&scope);
            }
        }
        if (sql) duckdb_free(sql);
        ducknng_mutex_lock(&w->mu);
    }
    ducknng_mutex_unlock(&w->mu);
    return NULL;
}

int ducknng_service_register_http_worker(ducknng_service *svc, const char *name,
    const char *sql, uint64_t interval_ms, char **errmsg) {
    ducknng_http_worker *w = NULL;
    ducknng_http_worker **new_workers;
    size_t new_cap;
    size_t i;
    if (errmsg) *errmsg = NULL;
    if (!svc || !name || !name[0] || !sql || !sql[0] || interval_ms == 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: name, sql, and interval_ms are required for http worker");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->http_worker_count; i++) {
        if (svc->http_workers[i] && svc->http_workers[i]->name &&
            strcmp(svc->http_workers[i]->name, name) == 0) {
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: http worker with that name already exists");
            return -1;
        }
    }
    if (svc->http_worker_count == svc->http_worker_cap) {
        new_cap = svc->http_worker_cap ? svc->http_worker_cap * 2 : 4;
        new_workers = (ducknng_http_worker **)duckdb_malloc(sizeof(*new_workers) * new_cap);
        if (!new_workers) {
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing worker table");
            return -1;
        }
        memset(new_workers, 0, sizeof(*new_workers) * new_cap);
        if (svc->http_workers && svc->http_worker_count)
            memcpy(new_workers, svc->http_workers, sizeof(*new_workers) * svc->http_worker_count);
        if (svc->http_workers) duckdb_free(svc->http_workers);
        svc->http_workers = new_workers;
        svc->http_worker_cap = new_cap;
    }
    w = (ducknng_http_worker *)duckdb_malloc(sizeof(*w));
    if (!w) {
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating http worker");
        return -1;
    }
    memset(w, 0, sizeof(*w));
    w->svc = svc;
    w->name = ducknng_strdup(name);
    w->sql = ducknng_strdup(sql);
    w->interval_ms = interval_ms;
    if (!w->name || !w->sql) {
        if (w->name) duckdb_free(w->name);
        if (w->sql) duckdb_free(w->sql);
        duckdb_free(w);
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for http worker name/sql");
        return -1;
    }
    if (ducknng_mutex_init(&w->mu) != 0) {
        if (w->name) duckdb_free(w->name);
        if (w->sql) duckdb_free(w->sql);
        duckdb_free(w);
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to init http worker mutex");
        return -1;
    }
    w->mu_initialized = 1;
    if (ducknng_cond_init(&w->cv) != 0) {
        ducknng_mutex_destroy(&w->mu);
        if (w->name) duckdb_free(w->name);
        if (w->sql) duckdb_free(w->sql);
        duckdb_free(w);
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to init http worker condvar");
        return -1;
    }
    w->cv_initialized = 1;
    if (ducknng_thread_create(&w->thread, ducknng_http_worker_thread_main, w) != 0) {
        ducknng_cond_destroy(&w->cv);
        ducknng_mutex_destroy(&w->mu);
        if (w->name) duckdb_free(w->name);
        if (w->sql) duckdb_free(w->sql);
        duckdb_free(w);
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to start http worker thread");
        return -1;
    }
    w->thread_started = 1;
    svc->http_workers[svc->http_worker_count++] = w;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return 0;
}

int ducknng_service_unregister_http_worker(ducknng_service *svc, const char *name, char **errmsg) {
    size_t i;
    ducknng_http_worker *w = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc || !name || !name[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service and name are required");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->http_worker_count; i++) {
        if (svc->http_workers[i] && svc->http_workers[i]->name &&
            strcmp(svc->http_workers[i]->name, name) == 0) {
            w = svc->http_workers[i];
            svc->http_workers[i] = svc->http_workers[--svc->http_worker_count];
            svc->http_workers[svc->http_worker_count] = NULL;
            break;
        }
    }
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (!w) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: http worker not found");
        return -1;
    }
    ducknng_http_worker_destroy(w);
    return 0;
}

int ducknng_service_http_workers_snapshot(ducknng_service *svc,
    ducknng_http_worker **out_workers, size_t *out_count, char **errmsg) {
    ducknng_http_worker *snap = NULL;
    size_t i;
    if (out_workers) *out_workers = NULL;
    if (out_count) *out_count = 0;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    if (svc->http_worker_count > 0) {
        snap = (ducknng_http_worker *)duckdb_malloc(sizeof(*snap) * svc->http_worker_count);
        if (!snap) {
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory snapshotting workers");
            return -1;
        }
        for (i = 0; i < svc->http_worker_count; i++) {
            memset(&snap[i], 0, sizeof(snap[i]));
            snap[i].name = svc->http_workers[i]->name ? ducknng_strdup(svc->http_workers[i]->name) : NULL;
            snap[i].sql = svc->http_workers[i]->sql ? ducknng_strdup(svc->http_workers[i]->sql) : NULL;
            snap[i].interval_ms = svc->http_workers[i]->interval_ms;
        }
    }
    if (out_count) *out_count = svc->http_worker_count;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (out_workers) *out_workers = snap;
    return 0;
}

void ducknng_service_http_workers_free(ducknng_http_worker *workers, size_t count) {
    size_t i;
    if (!workers) return;
    for (i = 0; i < count; i++) {
        if (workers[i].name) duckdb_free(workers[i].name);
        if (workers[i].sql) duckdb_free(workers[i].sql);
    }
    duckdb_free(workers);
}

/* --- Route-local auth --- */

int ducknng_service_set_http_route_auth(ducknng_service *svc, const char *method,
    const char *path, int require_identity, const char *allow_identities_json, char **errmsg) {
    size_t i;
    if (errmsg) *errmsg = NULL;
    if (!svc || !method || !path) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service, method, and path are required");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->http_route_count; i++) {
        ducknng_http_route *r = &svc->http_routes[i];
        if (r->method && strcmp(r->method, method) == 0 &&
            r->path && strcmp(r->path, path) == 0) {
            r->auth_require_identity = require_identity;
            if (r->auth_allow_identities_json) {
                duckdb_free(r->auth_allow_identities_json);
                r->auth_allow_identities_json = NULL;
            }
            if (allow_identities_json && allow_identities_json[0]) {
                r->auth_allow_identities_json = ducknng_strdup(allow_identities_json);
                if (!r->auth_allow_identities_json) {
                    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory for identities JSON");
                    return -1;
                }
            }
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            return 0;
        }
    }
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route not found");
    return -1;
}

static const char *ducknng_pipe_event_name(nng_pipe_ev ev) {
    switch (ev) {
    case NNG_PIPE_EV_ADD_PRE: return "add_pre";
    case NNG_PIPE_EV_ADD_POST: return "add_post";
    case NNG_PIPE_EV_REM_POST: return "rem_post";
    default: return "unknown";
    }
}

static void ducknng_service_pipe_event_cb(nng_pipe pipe, nng_pipe_ev ev, void *arg) {
    ducknng_service *svc = (ducknng_service *)arg;
    char *identity = NULL;
    char *admission_err = NULL;
    nng_sockaddr remote_addr;
    int have_remote_addr = 0;
    int allowed = ev == NNG_PIPE_EV_ADD_PRE ? 1 : -1;
    if (!svc) return;
    identity = ducknng_pipe_verified_peer_identity(pipe);
    have_remote_addr = ducknng_pipe_remote_addr(pipe, &remote_addr) == 0;
    if (ev == NNG_PIPE_EV_ADD_PRE) {
        allowed = ducknng_service_network_admission_check(svc, identity,
            have_remote_addr ? &remote_addr : NULL, &admission_err) == 0;
        if (allowed && svc->mu_initialized) {
            ducknng_mutex_lock(&svc->mu);
            if (svc->max_active_pipes > 0 && svc->pipe_state_count >= (size_t)svc->max_active_pipes) {
                allowed = 0;
                admission_err = ducknng_strdup("ducknng: max active pipes exceeded");
            }
            ducknng_mutex_unlock(&svc->mu);
        }
    }
    ducknng_service_pipe_event_append(svc, pipe, ducknng_pipe_event_name(ev),
        have_remote_addr ? &remote_addr : NULL, identity, allowed, admission_err);
    if (identity) duckdb_free(identity);
    if (admission_err) duckdb_free(admission_err);
    if (ev == NNG_PIPE_EV_ADD_PRE && !allowed) (void)nng_pipe_close(pipe);
}

int ducknng_service_start(ducknng_service *svc, char **errmsg) {
    int rv;
    int i;
    ducknng_tls_opts tls_opts;
    ducknng_transport_url transport;
    char *parse_err = NULL;
    if (!svc) return -1;

    ducknng_tls_opts_init(&tls_opts);
    if (ducknng_tls_opts_copy(&tls_opts, &svc->tls_opts) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy TLS configuration");
        return -1;
    }
    tls_opts.enabled = tls_opts.enabled ||
        (tls_opts.cert_key_file && tls_opts.cert_key_file[0]) ||
        (tls_opts.ca_file && tls_opts.ca_file[0]) ||
        (tls_opts.cert_pem && tls_opts.cert_pem[0]) ||
        (tls_opts.key_pem && tls_opts.key_pem[0]) ||
        (tls_opts.ca_pem && tls_opts.ca_pem[0]) ||
        tls_opts.auth_mode != 0;
    svc->tls_enabled = tls_opts.enabled ? 1 : 0;

    if (svc->running) {
        ducknng_tls_opts_reset(&tls_opts);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service is already running");
        return -1;
    }
    if (ducknng_transport_url_parse(svc->listen_url, &transport, &parse_err) != 0) {
        ducknng_tls_opts_reset(&tls_opts);
        if (errmsg) *errmsg = parse_err;
        else if (parse_err) duckdb_free(parse_err);
        return -1;
    }
    if (svc->mu_initialized) {
        ducknng_mutex_lock(&svc->mu);
        svc->shutting_down = 0;
        ducknng_mutex_unlock(&svc->mu);
    } else {
        svc->shutting_down = 0;
    }

    if (ducknng_transport_url_is_http(&transport)) {
        char *resolved_url = NULL;
        rv = ducknng_http_server_start(svc, &svc->http_state, &resolved_url, errmsg);
        ducknng_tls_opts_reset(&tls_opts);
        if (rv != 0) return -1;
        if (svc->resolved_listen_url) duckdb_free(svc->resolved_listen_url);
        svc->resolved_listen_url = resolved_url;
        svc->running = 1;
        return 0;
    }

    if (ducknng_listener_validate_startup_url(svc->listen_url, &tls_opts, errmsg) != 0) {
        ducknng_tls_opts_reset(&tls_opts);
        return -1;
    }

    rv = ducknng_rep_socket_open(&svc->rep_sock);
    if (rv != 0) goto fail;
    rv = nng_pipe_notify(svc->rep_sock, NNG_PIPE_EV_ADD_PRE, ducknng_service_pipe_event_cb, svc);
    if (rv != 0) goto fail;
    rv = nng_pipe_notify(svc->rep_sock, NNG_PIPE_EV_ADD_POST, ducknng_service_pipe_event_cb, svc);
    if (rv != 0) goto fail;
    rv = nng_pipe_notify(svc->rep_sock, NNG_PIPE_EV_REM_POST, ducknng_service_pipe_event_cb, svc);
    if (rv != 0) goto fail;
    rv = ducknng_listener_create(&svc->listener, svc->rep_sock, svc->listen_url);
    if (rv != 0) goto fail;
    rv = ducknng_listener_set_recvmaxsz(svc->listener, svc->recv_max_bytes);
    if (rv != 0) goto fail;
    rv = ducknng_listener_apply_tls(svc->listener, tls_opts.enabled ? &tls_opts : NULL);
    if (rv != 0) goto fail;
    rv = ducknng_listener_start(svc->listener);
    if (rv != 0) goto fail;

    svc->resolved_listen_url = ducknng_listener_resolve_url(svc->listener, svc->listen_url);
    svc->ctxs = (ducknng_rep_ctx *)duckdb_malloc(sizeof(*svc->ctxs) * (size_t)svc->ncontexts);
    if (!svc->ctxs) {
        if (errmsg) *errmsg = ducknng_strdup("out of memory");
        rv = NNG_ENOMEM;
        goto fail;
    }
    memset(svc->ctxs, 0, sizeof(*svc->ctxs) * (size_t)svc->ncontexts);

    for (i = 0; i < svc->ncontexts; i++) {
        ducknng_rep_ctx *rc = &svc->ctxs[i];
        rc->svc = svc;
        rv = ducknng_ctx_open(&rc->ctx, svc->rep_sock);
        if (rv != 0) goto fail;
        rv = ducknng_aio_alloc(&rc->aio, ducknng_rep_aio_cb, rc, 30000);
        if (rv != 0) goto fail;
        if (ducknng_mutex_init(&rc->mu) != 0) {
            rv = NNG_EINTERNAL;
            goto fail;
        }
        rc->mu_initialized = 1;
        if (ducknng_cond_init(&rc->cv) != 0) {
            rv = NNG_EINTERNAL;
            goto fail;
        }
        rc->cv_initialized = 1;
        rv = ducknng_thread_create(&rc->thread, ducknng_rep_worker_main, rc);
        if (rv != 0) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("failed to create worker thread");
            goto fail;
        }
        rc->thread_started = 1;
    }

    svc->running = 1;
    ducknng_tls_opts_reset(&tls_opts);
    return 0;

fail:
    ducknng_tls_opts_reset(&tls_opts);
    if (svc->ctxs) {
        for (i = 0; i < svc->ncontexts; i++) {
            ducknng_rep_ctx_teardown(&svc->ctxs[i]);
        }
        duckdb_free(svc->ctxs);
        svc->ctxs = NULL;
    }
    if (svc->listener.id != 0) {
        ducknng_listener_close(svc->listener);
        memset(&svc->listener, 0, sizeof(svc->listener));
    }
    if (svc->rep_sock.id != 0) {
        ducknng_socket_close(svc->rep_sock);
        memset(&svc->rep_sock, 0, sizeof(svc->rep_sock));
    }
    if (svc->resolved_listen_url) {
        duckdb_free(svc->resolved_listen_url);
        svc->resolved_listen_url = NULL;
    }
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    return -1;
}

int ducknng_service_requires_peer_identity(const ducknng_service *svc) {
    return svc && svc->tls_enabled && svc->tls_opts.auth_mode == 2;
}

int ducknng_service_peer_allowlist_active(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->tls_opts.peer_allowlist_active ? 1 : 0;
}

size_t ducknng_service_peer_allowlist_count(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->tls_opts.peer_allowlist_count;
}

int ducknng_service_peer_admission_check(ducknng_service *svc, const char *caller_identity, char **errmsg) {
    int require_identity;
    int allowlist_active;
    int allowed;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing service for peer admission");
        return -1;
    }
    require_identity = ducknng_service_requires_peer_identity(svc);
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    allowlist_active = svc->tls_opts.peer_allowlist_active ? 1 : 0;
    allowed = ducknng_tls_opts_peer_allowed(&svc->tls_opts, caller_identity);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (require_identity && (!caller_identity || !caller_identity[0])) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: mTLS peer identity is required");
        return -1;
    }
    if (allowlist_active && !allowed) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: peer identity is not admitted");
        return -1;
    }
    return 0;
}

int ducknng_service_network_admission_check(ducknng_service *svc, const char *caller_identity,
    const nng_sockaddr *remote_addr, char **errmsg) {
    int rc;
    int ip_active;
    int ip_allowed;
    if (errmsg) *errmsg = NULL;
    rc = ducknng_service_peer_admission_check(svc, caller_identity, errmsg);
    if (rc != 0) return rc;
    if (!svc) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing service for network admission");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    ip_active = svc->ip_allowlist_active ? 1 : 0;
    ip_allowed = ducknng_remote_addr_matches_ip_allowlist(svc, remote_addr);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (ip_active && !ip_allowed) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: remote address is not admitted");
        return -1;
    }
    return 0;
}

int ducknng_service_set_peer_allowlist(ducknng_service *svc, const char *identities_json, char **errmsg) {
    int rc;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    rc = ducknng_tls_opts_set_peer_allowlist(&svc->tls_opts, identities_json, errmsg);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return rc;
}

int ducknng_service_set_ip_allowlist(ducknng_service *svc, const char *cidrs_json, char **errmsg) {
    ducknng_ip_allow_rule *rules = NULL;
    size_t count = 0;
    char *json = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (!cidrs_json || !cidrs_json[0]) {
        if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
        ducknng_service_clear_ip_allowlist(svc);
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        return 0;
    }
    if (ducknng_parse_ip_allowlist_json(cidrs_json, &rules, &count, &json, errmsg) != 0) return -1;
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    ducknng_service_clear_ip_allowlist(svc);
    svc->ip_allowlist_active = 1;
    svc->ip_allowlist = rules;
    svc->ip_allowlist_count = count;
    svc->ip_allowlist_json = json;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return 0;
}

int ducknng_service_ip_allowlist_active(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->ip_allowlist_active ? 1 : 0;
}

size_t ducknng_service_ip_allowlist_count(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->ip_allowlist_count;
}

int ducknng_service_set_authorizer(ducknng_service *svc, const char *authorizer_sql, char **errmsg) {
    char *copy = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (!authorizer_sql || !authorizer_sql[0]) {
        if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
        ducknng_service_clear_authorizer(svc);
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        return 0;
    }
    copy = ducknng_strdup(authorizer_sql);
    if (!copy) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying SQL authorizer");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    ducknng_service_clear_authorizer(svc);
    svc->authorizer_sql = copy;
    svc->authorizer_active = 1;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return 0;
}

static char *ducknng_http_route_method_normalize_dup(const char *method, char **errmsg) {
    size_t i;
    size_t len;
    char *out;
    if (errmsg) *errmsg = NULL;
    if (!method || !method[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route method is required");
        return NULL;
    }
    len = strlen(method);
    out = (char *)duckdb_malloc(len + 1);
    if (!out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory normalizing HTTP route method");
        return NULL;
    }
    for (i = 0; i < len; i++) {
        unsigned char ch = (unsigned char)method[i];
        if ((ch >= 'a' && ch <= 'z')) ch = (unsigned char)(ch - ('a' - 'A'));
        out[i] = (char)ch;
    }
    out[len] = '\0';
    if (!ducknng_http_token_is_valid(out)) {
        duckdb_free(out);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route method must be an HTTP token");
        return NULL;
    }
    return out;
}

static int ducknng_http_route_path_valid(const char *path) {
    const char *p = path;
    if (!path || path[0] != '/') return 0;
    while (*p) {
        if (*p == '?' || *p == '#') return 0;
        p++;
    }
    return 1;
}

static const char *ducknng_service_http_mount_path(const ducknng_service *svc) {
    static _Thread_local char mount_path[1024];
    nng_url *up = NULL;
    if (!svc || !svc->listen_url) return NULL;
    if (nng_url_parse(&up, svc->listen_url) != 0 || !up || !up->u_path || up->u_path[0] != '/') {
        if (up) nng_url_free(up);
        return NULL;
    }
    snprintf(mount_path, sizeof(mount_path), "%s", up->u_path);
    nng_url_free(up);
    return mount_path;
}

static int ducknng_http_route_match_kind_parse(const char *match_kind, uint8_t *out_kind, char **errmsg) {
    if (errmsg) *errmsg = NULL;
    if (!out_kind) return -1;
    if (!match_kind || !match_kind[0] || ducknng_ascii_ieq(match_kind, "exact")) {
        *out_kind = DUCKNNG_HTTP_ROUTE_MATCH_EXACT;
        return 0;
    }
    if (ducknng_ascii_ieq(match_kind, "prefix")) {
        *out_kind = DUCKNNG_HTTP_ROUTE_MATCH_PREFIX;
        return 0;
    }
    if (ducknng_ascii_ieq(match_kind, "template")) {
        *out_kind = DUCKNNG_HTTP_ROUTE_MATCH_TEMPLATE;
        return 0;
    }
    if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route match_kind must be exact, prefix, or template");
    return -1;
}

static int ducknng_http_route_template_param_name_valid(const char *name, size_t len) {
    size_t i;
    if (!name || len == 0) return 0;
    if (!((name[0] >= 'a' && name[0] <= 'z') ||
            (name[0] >= 'A' && name[0] <= 'Z') || name[0] == '_')) {
        return 0;
    }
    for (i = 1; i < len; i++) {
        char ch = name[i];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                (ch >= '0' && ch <= '9') || ch == '_')) {
            return 0;
        }
    }
    return 1;
}

static int ducknng_http_route_template_valid(const char *path, char **errmsg) {
    const char *seg = path + 1;
    int saw_param = 0;
    if (errmsg) *errmsg = NULL;
    while (1) {
        const char *slash = strchr(seg, '/');
        size_t seg_len = slash ? (size_t)(slash - seg) : strlen(seg);
        if (seg_len == 0) {
            if (!slash) break;
            seg = slash + 1;
            continue;
        }
        if (seg[0] == '{' || seg[seg_len - 1] == '}') {
            const char *close = memchr(seg, '}', seg_len);
            if (seg[0] != '{' || !close || close != seg + seg_len - 1 || seg_len < 3 ||
                memchr(seg + 1, '{', seg_len - 2) || memchr(seg + 1, '}', seg_len - 2)) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route template segments must use {name} and occupy a whole path segment");
                return 0;
            }
            if (!ducknng_http_route_template_param_name_valid(seg + 1, seg_len - 2)) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route template parameter names must match [A-Za-z_][A-Za-z0-9_]*");
                return 0;
            }
            saw_param = 1;
        }
        if (!slash) break;
        seg = slash + 1;
    }
    if (!saw_param) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: template routes must include at least one {name} segment");
        return 0;
    }
    return 1;
}

static int ducknng_http_route_pattern_valid(uint8_t match_kind, const char *path, char **errmsg) {
    if (errmsg) *errmsg = NULL;
    if (!ducknng_http_route_path_valid(path)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route path must be an absolute path without query or fragment");
        return 0;
    }
    if (match_kind == DUCKNNG_HTTP_ROUTE_MATCH_TEMPLATE) {
        return ducknng_http_route_template_valid(path, errmsg);
    }
    return 1;
}

static int ducknng_http_route_prefix_matches(const char *pattern, const char *path) {
    size_t len;
    if (!pattern || !path) return 0;
    len = strlen(pattern);
    if (len == 0) return 0;
    if (strncmp(pattern, path, len) != 0) return 0;
    if (path[len] == '\0') return 1;
    if (pattern[len - 1] == '/') return 1;
    return path[len] == '/';
}

static int ducknng_http_route_json_buf_append(char **buf, size_t *len, size_t *cap,
    const char *src, size_t src_len) {
    char *next;
    size_t new_cap;
    if (!buf || !len || !cap) return -1;
    if (*cap < *len + src_len + 1) {
        new_cap = *cap ? *cap * 2 : 64;
        while (new_cap < *len + src_len + 1) new_cap *= 2;
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

static int ducknng_http_route_json_append_string(char **buf, size_t *len, size_t *cap,
    const char *src, size_t src_len) {
    size_t i;
    if (ducknng_http_route_json_buf_append(buf, len, cap, "\"", 1) != 0) return -1;
    for (i = 0; i < src_len; i++) {
        unsigned char ch = (unsigned char)src[i];
        switch (ch) {
        case '"':
            if (ducknng_http_route_json_buf_append(buf, len, cap, "\\\"", 2) != 0) return -1;
            break;
        case '\\':
            if (ducknng_http_route_json_buf_append(buf, len, cap, "\\\\", 2) != 0) return -1;
            break;
        case '\b':
            if (ducknng_http_route_json_buf_append(buf, len, cap, "\\b", 2) != 0) return -1;
            break;
        case '\f':
            if (ducknng_http_route_json_buf_append(buf, len, cap, "\\f", 2) != 0) return -1;
            break;
        case '\n':
            if (ducknng_http_route_json_buf_append(buf, len, cap, "\\n", 2) != 0) return -1;
            break;
        case '\r':
            if (ducknng_http_route_json_buf_append(buf, len, cap, "\\r", 2) != 0) return -1;
            break;
        case '\t':
            if (ducknng_http_route_json_buf_append(buf, len, cap, "\\t", 2) != 0) return -1;
            break;
        default:
            if (ch < 0x20) {
                char esc[7];
                snprintf(esc, sizeof(esc), "\\u%04x", (unsigned)ch);
                if (ducknng_http_route_json_buf_append(buf, len, cap, esc, 6) != 0) return -1;
            } else if (ducknng_http_route_json_buf_append(buf, len, cap, (const char *)&src[i], 1) != 0) {
                return -1;
            }
        }
    }
    if (ducknng_http_route_json_buf_append(buf, len, cap, "\"", 1) != 0) return -1;
    return 0;
}

static int ducknng_http_route_template_matches(const char *pattern, const char *path,
    char **out_params_json) {
    const char *pat = pattern;
    const char *req = path;
    char *params_json = NULL;
    size_t json_len = 0;
    size_t json_cap = 0;
    int first = 1;
    if (out_params_json) *out_params_json = NULL;
    if (!pattern || !path) return 0;
    while (1) {
        const char *pat_next;
        const char *req_next;
        size_t pat_len;
        size_t req_len;
        if (*pat == '/') pat++;
        if (*req == '/') req++;
        pat_next = strchr(pat, '/');
        req_next = strchr(req, '/');
        pat_len = pat_next ? (size_t)(pat_next - pat) : strlen(pat);
        req_len = req_next ? (size_t)(req_next - req) : strlen(req);
        if (pat_len == 0 && req_len == 0) break;
        if (pat_len == 0 || req_len == 0) goto fail_nomatch;
        if (pat[0] == '{' && pat[pat_len - 1] == '}') {
            if (!params_json &&
                ducknng_http_route_json_buf_append(&params_json, &json_len, &json_cap, "{", 1) != 0) {
                goto fail_error;
            }
            if (!first &&
                ducknng_http_route_json_buf_append(&params_json, &json_len, &json_cap, ",", 1) != 0) {
                goto fail_error;
            }
            if (ducknng_http_route_json_append_string(&params_json, &json_len, &json_cap,
                    pat + 1, pat_len - 2) != 0 ||
                ducknng_http_route_json_buf_append(&params_json, &json_len, &json_cap, ":", 1) != 0 ||
                ducknng_http_route_json_append_string(&params_json, &json_len, &json_cap,
                    req, req_len) != 0) {
                goto fail_error;
            }
            first = 0;
        } else if (pat_len != req_len || strncmp(pat, req, pat_len) != 0) {
            goto fail_nomatch;
        }
        pat = pat_next ? pat_next : pat + pat_len;
        req = req_next ? req_next : req + req_len;
        if (!pat_next && !req_next) break;
        if (!pat_next || !req_next) goto fail_nomatch;
    }
    if (params_json) {
        if (ducknng_http_route_json_buf_append(&params_json, &json_len, &json_cap, "}", 1) != 0) {
            goto fail_error;
        }
    } else {
        params_json = ducknng_strdup("{}");
        if (!params_json) goto fail_error;
    }
    if (out_params_json) *out_params_json = params_json;
    else duckdb_free(params_json);
    return 1;
fail_error:
    if (params_json) duckdb_free(params_json);
    return -1;
fail_nomatch:
    if (params_json) duckdb_free(params_json);
    return 0;
}

static int ducknng_http_route_pattern_matches(uint8_t match_kind, const char *pattern,
    const char *path, char **out_params_json) {
    if (out_params_json) *out_params_json = NULL;
    switch (match_kind) {
    case DUCKNNG_HTTP_ROUTE_MATCH_PREFIX:
        return ducknng_http_route_prefix_matches(pattern, path);
    case DUCKNNG_HTTP_ROUTE_MATCH_TEMPLATE:
        return ducknng_http_route_template_matches(pattern, path, out_params_json);
    case DUCKNNG_HTTP_ROUTE_MATCH_EXACT:
    default:
        return (pattern && path && strcmp(pattern, path) == 0) ? 1 : 0;
    }
}

static int ducknng_service_http_routes_reserve(ducknng_service *svc, size_t want, char **errmsg) {
    ducknng_http_route *next;
    size_t new_cap = svc && svc->http_route_cap ? svc->http_route_cap * 2 : 4;
    if (errmsg) *errmsg = NULL;
    if (!svc) return -1;
    if (svc->http_route_cap >= want) return 0;
    while (new_cap < want) new_cap *= 2;
    next = (ducknng_http_route *)duckdb_malloc(sizeof(*next) * new_cap);
    if (!next) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing HTTP route registry");
        return -1;
    }
    memset(next, 0, sizeof(*next) * new_cap);
    if (svc->http_routes && svc->http_route_count > 0) {
        memcpy(next, svc->http_routes, sizeof(*next) * svc->http_route_count);
        duckdb_free(svc->http_routes);
    }
    svc->http_routes = next;
    svc->http_route_cap = new_cap;
    return 0;
}

static int ducknng_service_register_http_route_inner(ducknng_service *svc, const char *method,
    uint8_t match_kind, const char *path, const char *handler_sql, uint64_t request_max_bytes,
    char **errmsg) {
    size_t i;
    char *norm_method = NULL;
    ducknng_http_route route;
    const char *mount_path;
    int mount_match_rc;
    ducknng_transport_url parsed;
    char *parse_err = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc || !method || !path || !handler_sql) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route requires service, method, path, and handler_sql");
        return -1;
    }
    if (ducknng_transport_url_parse(svc->listen_url, &parsed, &parse_err) != 0 ||
        !ducknng_transport_url_is_http(&parsed)) {
        if (parse_err && errmsg) *errmsg = parse_err;
        else if (parse_err) duckdb_free(parse_err);
        else if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP routes require an http:// or https:// service");
        return -1;
    }
    if (parse_err) duckdb_free(parse_err);
    if (!ducknng_http_route_pattern_valid(match_kind, path, errmsg)) {
        return -1;
    }
    mount_path = ducknng_service_http_mount_path(svc);
    mount_match_rc = mount_path ?
        ducknng_http_route_pattern_matches(match_kind, path, mount_path, NULL) : 0;
    if (mount_match_rc < 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory matching HTTP route pattern");
        return -1;
    }
    if (mount_match_rc > 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route path conflicts with the framed RPC mount");
        return -1;
    }
    if (request_max_bytes > svc->recv_max_bytes) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route request_max_bytes cannot exceed the service recv_max_bytes");
        return -1;
    }
    norm_method = ducknng_http_route_method_normalize_dup(method, errmsg);
    if (!norm_method) return -1;
    memset(&route, 0, sizeof(route));
    route.route_id = svc->next_http_route_id++;
    route.match_kind = match_kind;
    route.request_max_bytes = request_max_bytes;
    route.method = norm_method;
    route.path = ducknng_strdup(path);
    route.handler_sql = ducknng_strdup(handler_sql);
    if (!route.path || !route.handler_sql) {
        ducknng_http_route_reset(&route);
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory registering HTTP route");
        return -1;
    }
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->http_route_count; i++) {
        if (svc->http_routes[i].match_kind == route.match_kind &&
            strcmp(svc->http_routes[i].method, route.method) == 0 &&
            strcmp(svc->http_routes[i].path, route.path) == 0) {
            ducknng_mutex_unlock(&svc->mu);
            ducknng_http_route_reset(&route);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route already registered");
            return -1;
        }
    }
    if (ducknng_service_http_routes_reserve(svc, svc->http_route_count + 1, errmsg) != 0) {
        ducknng_mutex_unlock(&svc->mu);
        ducknng_http_route_reset(&route);
        return -1;
    }
    svc->http_routes[svc->http_route_count++] = route;
    ducknng_mutex_unlock(&svc->mu);
    return 0;
}

int ducknng_service_register_http_route(ducknng_service *svc, const char *method, const char *path,
    const char *handler_sql, uint64_t request_max_bytes, char **errmsg) {
    return ducknng_service_register_http_route_inner(svc, method,
        DUCKNNG_HTTP_ROUTE_MATCH_EXACT, path, handler_sql, request_max_bytes, errmsg);
}

int ducknng_service_register_http_stream_route(ducknng_service *svc, const char *method,
    const char *path, const char *handler_sql, const char *content_type,
    uint64_t request_max_bytes, char **errmsg) {
    int rc;
    char *ct_copy = NULL;
    size_t i;
    if (errmsg) *errmsg = NULL;
    rc = ducknng_service_register_http_route_inner(svc, method,
        DUCKNNG_HTTP_ROUTE_MATCH_EXACT, path, handler_sql, request_max_bytes, errmsg);
    if (rc != 0) return rc;
    ct_copy = ducknng_strdup(content_type && content_type[0] ? content_type : "text/event-stream; charset=utf-8");
    if (!ct_copy) {
        ducknng_service_unregister_http_route(svc, method, path, NULL);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory registering stream route content type");
        return -1;
    }
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->http_route_count; i++) {
        if (strcmp(svc->http_routes[i].method, method) == 0 &&
            strcmp(svc->http_routes[i].path, path) == 0 &&
            svc->http_routes[i].match_kind == DUCKNNG_HTTP_ROUTE_MATCH_EXACT) {
            svc->http_routes[i].response_mode = DUCKNNG_HTTP_ROUTE_RESPONSE_STREAM;
            if (svc->http_routes[i].stream_content_type) duckdb_free(svc->http_routes[i].stream_content_type);
            svc->http_routes[i].stream_content_type = ct_copy;
            ct_copy = NULL;
            break;
        }
    }
    ducknng_mutex_unlock(&svc->mu);
    if (ct_copy) duckdb_free(ct_copy); /* route vanished between register and lock — shouldn't happen */
    return 0;
}

int ducknng_service_register_http_route_pattern(ducknng_service *svc, const char *method,
    const char *match_kind, const char *path_pattern, const char *handler_sql,
    uint64_t request_max_bytes, char **errmsg) {
    uint8_t parsed_kind = DUCKNNG_HTTP_ROUTE_MATCH_EXACT;
    if (ducknng_http_route_match_kind_parse(match_kind, &parsed_kind, errmsg) != 0) return -1;
    return ducknng_service_register_http_route_inner(svc, method, parsed_kind,
        path_pattern, handler_sql, request_max_bytes, errmsg);
}

static int ducknng_service_unregister_http_route_inner(ducknng_service *svc, const char *method,
    uint8_t match_kind, const char *path, char **errmsg) {
    size_t i;
    char *norm_method = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc || !method || !path) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: HTTP route unregister requires service, method, and path");
        return -1;
    }
    norm_method = ducknng_http_route_method_normalize_dup(method, errmsg);
    if (!norm_method) return -1;
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->http_route_count; i++) {
        if (svc->http_routes[i].match_kind == match_kind &&
            strcmp(svc->http_routes[i].method, norm_method) == 0 &&
            strcmp(svc->http_routes[i].path, path) == 0) {
            ducknng_http_route_reset(&svc->http_routes[i]);
            for (; i + 1 < svc->http_route_count; i++) svc->http_routes[i] = svc->http_routes[i + 1];
            svc->http_route_count--;
            if (svc->http_route_count < svc->http_route_cap) {
                memset(&svc->http_routes[svc->http_route_count], 0, sizeof(*svc->http_routes));
            }
            ducknng_mutex_unlock(&svc->mu);
            duckdb_free(norm_method);
            return 1;
        }
    }
    ducknng_mutex_unlock(&svc->mu);
    duckdb_free(norm_method);
    return 0;
}

int ducknng_service_unregister_http_route(ducknng_service *svc, const char *method,
    const char *path, char **errmsg) {
    return ducknng_service_unregister_http_route_inner(svc, method,
        DUCKNNG_HTTP_ROUTE_MATCH_EXACT, path, errmsg);
}

int ducknng_service_unregister_http_route_pattern(ducknng_service *svc, const char *method,
    const char *match_kind, const char *path_pattern, char **errmsg) {
    uint8_t parsed_kind = DUCKNNG_HTTP_ROUTE_MATCH_EXACT;
    if (ducknng_http_route_match_kind_parse(match_kind, &parsed_kind, errmsg) != 0) return -1;
    return ducknng_service_unregister_http_route_inner(svc, method, parsed_kind,
        path_pattern, errmsg);
}

int ducknng_service_lookup_http_route(ducknng_service *svc, const char *method,
    const char *path, ducknng_http_route *out_route, char **out_path_params_json, char **errmsg) {
    size_t i;
    char *norm_method = NULL;
    int rc = 0;
    uint64_t best_score = 0;
    char *best_path_params_json = NULL;
    if (errmsg) *errmsg = NULL;
    if (out_path_params_json) *out_path_params_json = NULL;
    if (!svc || !method || !path || !out_route) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP route lookup arguments");
        return -1;
    }
    norm_method = ducknng_http_route_method_normalize_dup(method, errmsg);
    if (!norm_method) return -1;
    memset(out_route, 0, sizeof(*out_route));
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->http_route_count; i++) {
        char *candidate_path_params_json = NULL;
        int matched;
        uint64_t score;
        if (strcmp(svc->http_routes[i].method, norm_method) != 0) continue;
        matched = ducknng_http_route_pattern_matches(svc->http_routes[i].match_kind,
            svc->http_routes[i].path, path, &candidate_path_params_json);
        if (matched < 0) {
            ducknng_mutex_unlock(&svc->mu);
            duckdb_free(norm_method);
            if (best_path_params_json) duckdb_free(best_path_params_json);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory matching HTTP route");
            return -1;
        }
        if (!matched) continue;
        score = ((uint64_t)(
            svc->http_routes[i].match_kind == DUCKNNG_HTTP_ROUTE_MATCH_EXACT ? 3 :
            svc->http_routes[i].match_kind == DUCKNNG_HTTP_ROUTE_MATCH_TEMPLATE ? 2 : 1
        ) << 32) + (uint64_t)strlen(svc->http_routes[i].path);
        if (score >= best_score) {
            ducknng_http_route_reset(out_route);
            if (best_path_params_json) {
                duckdb_free(best_path_params_json);
                best_path_params_json = NULL;
            }
            rc = ducknng_http_route_copy(out_route, &svc->http_routes[i]) == 0 ? 1 : -1;
            if (rc < 0) {
                if (candidate_path_params_json) duckdb_free(candidate_path_params_json);
                break;
            }
            best_path_params_json = candidate_path_params_json;
            best_score = score;
        } else if (candidate_path_params_json) {
            duckdb_free(candidate_path_params_json);
        }
    }
    ducknng_mutex_unlock(&svc->mu);
    duckdb_free(norm_method);
    if (rc < 0 && errmsg && !*errmsg) {
        if (best_path_params_json) duckdb_free(best_path_params_json);
        *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP route");
    } else if (rc > 0 && out_path_params_json) {
        *out_path_params_json = best_path_params_json;
    } else if (best_path_params_json) {
        duckdb_free(best_path_params_json);
    }
    return rc;
}

int ducknng_service_http_routes_snapshot(ducknng_service *svc, ducknng_http_route **out_routes,
    size_t *out_count, char **errmsg) {
    ducknng_http_route *routes = NULL;
    size_t i;
    if (errmsg) *errmsg = NULL;
    if (out_routes) *out_routes = NULL;
    if (out_count) *out_count = 0;
    if (!svc || !out_routes || !out_count) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing HTTP route snapshot outputs");
        return -1;
    }
    ducknng_mutex_lock(&svc->mu);
    if (svc->http_route_count > 0) {
        routes = (ducknng_http_route *)duckdb_malloc(sizeof(*routes) * svc->http_route_count);
        if (!routes) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP routes");
            return -1;
        }
        memset(routes, 0, sizeof(*routes) * svc->http_route_count);
        for (i = 0; i < svc->http_route_count; i++) {
            if (ducknng_http_route_copy(&routes[i], &svc->http_routes[i]) != 0) {
                size_t j;
                ducknng_mutex_unlock(&svc->mu);
                for (j = 0; j <= i; j++) ducknng_http_route_reset(&routes[j]);
                duckdb_free(routes);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP routes");
                return -1;
            }
        }
    }
    *out_routes = routes;
    *out_count = svc->http_route_count;
    ducknng_mutex_unlock(&svc->mu);
    return 0;
}

void ducknng_service_http_routes_free(ducknng_http_route *routes, size_t count) {
    size_t i;
    if (!routes) return;
    for (i = 0; i < count; i++) ducknng_http_route_reset(&routes[i]);
    duckdb_free(routes);
}

int ducknng_service_authorizer_active(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->authorizer_active ? 1 : 0;
}

int ducknng_service_set_limits(ducknng_service *svc, uint64_t max_open_sessions,
    uint64_t max_active_pipes, uint64_t max_inflight_requests,
    uint64_t max_sessions_per_peer_identity,
    uint64_t max_inflight_per_peer_identity,
    uint64_t max_reply_bytes_per_peer_identity,
    uint64_t max_session_open_rate_per_peer_identity, char **errmsg) {
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    svc->max_open_sessions = max_open_sessions;
    svc->max_active_pipes = max_active_pipes;
    svc->max_inflight_requests = max_inflight_requests;
    svc->max_sessions_per_peer_identity = max_sessions_per_peer_identity;
    svc->max_inflight_per_peer_identity = max_inflight_per_peer_identity;
    svc->max_reply_bytes_per_peer_identity = max_reply_bytes_per_peer_identity;
    svc->max_session_open_rate_per_peer_identity = max_session_open_rate_per_peer_identity;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return 0;
}

int ducknng_service_set_execution_model(ducknng_service *svc, const char *model, char **errmsg) {
    int parsed;
    duckdb_connection old_con = NULL;
    size_t old_index = (size_t)-1;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (!ducknng_execution_model_parse(model, &parsed)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: execution model must be shared_serialized_connection, service_serialized_connection, or request_connection");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    if (svc->inflight_request_count > 0 || svc->session_count > 0) {
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: execution model cannot change while requests or sessions are active");
        return -1;
    }
    if (svc->execution_model == parsed) {
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        return 0;
    }
    if (svc->execution_mu_initialized) ducknng_mutex_lock(&svc->execution_mu);
    old_con = svc->execution_con;
    old_index = svc->execution_pool_index;
    svc->execution_con = NULL;
    svc->execution_pool_index = (size_t)-1;
    svc->execution_model = parsed;
    if (svc->execution_mu_initialized) ducknng_mutex_unlock(&svc->execution_mu);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (old_con) ducknng_service_release_execution_connection(svc, old_index);
    return 0;
}

uint64_t ducknng_service_max_open_sessions(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->max_open_sessions;
}

uint64_t ducknng_service_max_active_pipes(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->max_active_pipes;
}

uint64_t ducknng_service_max_inflight_requests(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->max_inflight_requests;
}

uint64_t ducknng_service_max_sessions_per_peer_identity(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->max_sessions_per_peer_identity;
}

uint64_t ducknng_service_max_inflight_per_peer_identity(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->max_inflight_per_peer_identity;
}

uint64_t ducknng_service_max_reply_bytes_per_peer_identity(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->max_reply_bytes_per_peer_identity;
}

uint64_t ducknng_service_max_session_open_rate_per_peer_identity(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->max_session_open_rate_per_peer_identity;
}

const char *ducknng_service_execution_model(const ducknng_service *svc) {
    return ducknng_execution_model_name(svc ? svc->execution_model : DUCKNNG_EXECUTION_SHARED_SERIALIZED_CONNECTION);
}

const char *ducknng_service_peer_identity_format(const ducknng_service *svc) {
    (void)svc;
    return DUCKNNG_PEER_IDENTITY_FORMAT_TLS;
}

void ducknng_service_manifest_security(const ducknng_service *svc, ducknng_manifest_security *security) {
    if (!security) return;
    memset(security, 0, sizeof(*security));
    if (!svc) return;
    security->tls_enabled = svc->tls_enabled;
    security->tls_auth_mode = svc->tls_opts.auth_mode;
    security->peer_identity_required = ducknng_service_requires_peer_identity(svc);
    security->peer_allowlist_active = ducknng_service_peer_allowlist_active(svc);
    security->peer_allowlist_count = (uint64_t)ducknng_service_peer_allowlist_count(svc);
    security->ip_allowlist_active = ducknng_service_ip_allowlist_active(svc);
    security->ip_allowlist_count = (uint64_t)ducknng_service_ip_allowlist_count(svc);
    security->sql_authorizer_active = ducknng_service_authorizer_active(svc);
    security->sessions_bind_peer_identity_when_present = 1;
    security->session_idle_timeout_ms = svc->session_idle_ms;
    security->max_open_sessions = ducknng_service_max_open_sessions(svc);
    security->max_sessions_per_peer_identity = ducknng_service_max_sessions_per_peer_identity(svc);
    security->peer_identity_format = ducknng_service_peer_identity_format(svc);
    security->execution_model = ducknng_service_execution_model(svc);
}

size_t ducknng_service_active_pipe_count(const ducknng_service *svc) {
    if (!svc) return 0;
    return atomic_load_explicit(&svc->pipe_state_count_visible, memory_order_acquire);
}

size_t ducknng_service_inflight_request_count(const ducknng_service *svc) {
    if (!svc) return 0;
    return atomic_load_explicit(&svc->inflight_request_count_visible, memory_order_acquire);
}

/* --- Per-peer-identity state helpers (caller must hold svc->mu) --- */

static ducknng_peer_identity_state *ducknng_service_find_or_create_peer_identity_locked(
    ducknng_service *svc, const char *identity) {
    size_t i;
    ducknng_peer_identity_state *p;
    ducknng_peer_identity_state *new_peer_identity_states;
    size_t new_cap;
    if (!svc || !identity || !identity[0]) return NULL;
    for (i = 0; i < svc->peer_identity_state_count; i++) {
        if (svc->peer_identity_states[i].identity && strcmp(svc->peer_identity_states[i].identity, identity) == 0)
            return &svc->peer_identity_states[i];
    }
    /* not found – allocate a new slot */
    if (svc->peer_identity_state_count == svc->peer_identity_state_cap) {
        new_cap = svc->peer_identity_state_cap ? svc->peer_identity_state_cap * 2 : 8;
        new_peer_identity_states = (ducknng_peer_identity_state *)duckdb_malloc(
            sizeof(*new_peer_identity_states) * new_cap);
        if (!new_peer_identity_states) return NULL;
        memset(new_peer_identity_states, 0, sizeof(*new_peer_identity_states) * new_cap);
        if (svc->peer_identity_states && svc->peer_identity_state_count)
            memcpy(new_peer_identity_states, svc->peer_identity_states,
                sizeof(*new_peer_identity_states) * svc->peer_identity_state_count);
        if (svc->peer_identity_states) duckdb_free(svc->peer_identity_states);
        svc->peer_identity_states = new_peer_identity_states;
        svc->peer_identity_state_cap = new_cap;
    }
    p = &svc->peer_identity_states[svc->peer_identity_state_count];
    memset(p, 0, sizeof(*p));
    p->identity = ducknng_strdup(identity);
    if (!p->identity) return NULL;
    svc->peer_identity_state_count++;
    return p;
}

static void ducknng_service_clear_peer_identity_states(ducknng_service *svc) {
    size_t i;
    if (!svc || !svc->peer_identity_states) return;
    for (i = 0; i < svc->peer_identity_state_count; i++) {
        if (svc->peer_identity_states[i].identity) duckdb_free(svc->peer_identity_states[i].identity);
        if (svc->peer_identity_states[i].session_open_times) duckdb_free(svc->peer_identity_states[i].session_open_times);
    }
    duckdb_free(svc->peer_identity_states);
    svc->peer_identity_states = NULL;
    svc->peer_identity_state_count = 0;
    svc->peer_identity_state_cap = 0;
}

int ducknng_service_check_and_record_session_open_locked(ducknng_service *svc,
    const char *identity, uint64_t now_ms, char **errmsg) {
    ducknng_peer_identity_state *ps;
    uint64_t *new_times;
    size_t new_cap;
    size_t valid;
    size_t i;
    if (errmsg) *errmsg = NULL;
    if (!svc || !identity || !identity[0]) return 0;
    if (svc->max_session_open_rate_per_peer_identity == 0) return 0;
    ps = ducknng_service_find_or_create_peer_identity_locked(svc, identity);
    if (!ps) return 0; /* OOM: silently allow */
    /* Expire entries older than 1 second */
    valid = 0;
    for (i = 0; i < ps->session_open_count; i++) {
        size_t idx = (ps->session_open_head + i) % (ps->session_open_cap ? ps->session_open_cap : 1);
        if (ps->session_open_times[idx] + 1000 > now_ms) {
            ps->session_open_times[valid] = ps->session_open_times[idx];
            valid++;
        }
    }
    ps->session_open_count = valid;
    ps->session_open_head = 0;
    /* Check rate */
    if (ps->session_open_count >= (size_t)svc->max_session_open_rate_per_peer_identity) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: session open rate per peer identity exceeded");
        return -1;
    }
    /* Record this open */
    if (ps->session_open_count == ps->session_open_cap) {
        new_cap = ps->session_open_cap ? ps->session_open_cap * 2 : 8;
        new_times = (uint64_t *)duckdb_malloc(sizeof(uint64_t) * new_cap);
        if (!new_times) return 0; /* OOM: silently allow */
        if (ps->session_open_times && ps->session_open_count)
            memcpy(new_times, ps->session_open_times, sizeof(uint64_t) * ps->session_open_count);
        if (ps->session_open_times) duckdb_free(ps->session_open_times);
        ps->session_open_times = new_times;
        ps->session_open_cap = new_cap;
    }
    ps->session_open_times[ps->session_open_count] = now_ms;
    ps->session_open_count++;
    return 0;
}

int ducknng_service_begin_request(ducknng_service *svc, const char *caller_identity, char **errmsg) {
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    if (svc->shutting_down) {
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service is shutting down");
        return -1;
    }
    if (svc->max_inflight_requests > 0 && svc->inflight_request_count >= (size_t)svc->max_inflight_requests) {
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: max inflight requests exceeded");
        return -1;
    }
    if (caller_identity && caller_identity[0]) {
        ducknng_peer_identity_state *ps = ducknng_service_find_or_create_peer_identity_locked(svc, caller_identity);
        if (ps) {
            if (svc->max_inflight_per_peer_identity > 0 &&
                ps->inflight_count >= (size_t)svc->max_inflight_per_peer_identity) {
                if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: max inflight requests per peer identity exceeded");
                return -1;
            }
            if (svc->max_reply_bytes_per_peer_identity > 0 &&
                ps->cumulative_reply_bytes >= svc->max_reply_bytes_per_peer_identity) {
                if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: reply byte quota for peer identity exceeded");
                return -1;
            }
            ps->inflight_count++;
        }
    }
    svc->inflight_request_count++;
    atomic_store_explicit(&svc->inflight_request_count_visible, svc->inflight_request_count, memory_order_release);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return 0;
}

void ducknng_service_end_request(ducknng_service *svc, const char *caller_identity, size_t reply_bytes) {
    if (!svc) return;
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    if (svc->inflight_request_count > 0) svc->inflight_request_count--;
    atomic_store_explicit(&svc->inflight_request_count_visible, svc->inflight_request_count, memory_order_release);
    if (caller_identity && caller_identity[0]) {
        size_t i;
        for (i = 0; i < svc->peer_identity_state_count; i++) {
            if (svc->peer_identity_states[i].identity && strcmp(svc->peer_identity_states[i].identity, caller_identity) == 0) {
                if (svc->peer_identity_states[i].inflight_count > 0) svc->peer_identity_states[i].inflight_count--;
                svc->peer_identity_states[i].cumulative_reply_bytes += (uint64_t)reply_bytes;
                break;
            }
        }
    }
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
}

int ducknng_service_pipe_monitor_stats(ducknng_service *svc,
    ducknng_pipe_monitor_stats *out_stats, char **errmsg) {
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (!out_stats) return -1;
    memset(out_stats, 0, sizeof(*out_stats));
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    out_stats->event_capacity = (uint64_t)svc->pipe_event_cap;
    out_stats->event_count = (uint64_t)svc->pipe_event_count;
    out_stats->dropped_events = svc->pipe_event_dropped;
    out_stats->active_pipes = (uint64_t)svc->pipe_state_count;
    out_stats->max_active_pipes = svc->max_active_pipes;
    if (svc->pipe_event_count > 0 && svc->pipe_events && svc->pipe_event_cap > 0) {
        size_t newest_idx = (svc->pipe_event_start + svc->pipe_event_count - 1) % svc->pipe_event_cap;
        out_stats->oldest_seq = svc->pipe_events[svc->pipe_event_start].seq;
        out_stats->newest_seq = svc->pipe_events[newest_idx].seq;
    }
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return 0;
}

int ducknng_service_pipe_events_snapshot(ducknng_service *svc, uint64_t after_seq, uint64_t max_events,
    ducknng_pipe_event **out_events, size_t *out_count, char **errmsg) {
    ducknng_pipe_event *rows = NULL;
    size_t i;
    size_t n = 0;
    size_t cap = 0;
    if (out_events) *out_events = NULL;
    if (out_count) *out_count = 0;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (!out_events || !out_count) return -1;
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->pipe_event_count; i++) {
        size_t idx = (svc->pipe_event_start + i) % svc->pipe_event_cap;
        ducknng_pipe_event *src = &svc->pipe_events[idx];
        ducknng_pipe_event *next;
        if (src->seq <= after_seq) continue;
        if (max_events > 0 && n >= (size_t)max_events) break;
        if (n == cap) {
            size_t new_cap = cap ? cap * 2 : 16;
            while (new_cap <= n) new_cap *= 2;
            next = (ducknng_pipe_event *)duckdb_malloc(sizeof(*next) * new_cap);
            if (!next) {
                if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
                ducknng_service_pipe_events_free(rows, n);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying pipe monitor events");
                return -1;
            }
            memset(next, 0, sizeof(*next) * new_cap);
            if (rows && n) memcpy(next, rows, sizeof(*rows) * n);
            if (rows) duckdb_free(rows);
            rows = next;
            cap = new_cap;
        }
        if (ducknng_pipe_event_copy(&rows[n], src) != 0) {
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            ducknng_service_pipe_events_free(rows, n);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying pipe monitor event");
            return -1;
        }
        n++;
    }
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    *out_events = rows;
    *out_count = n;
    return 0;
}

int ducknng_service_pipe_states_snapshot(ducknng_service *svc,
    ducknng_pipe_state **out_states, size_t *out_count, char **errmsg) {
    ducknng_pipe_state *rows = NULL;
    size_t i;
    if (out_states) *out_states = NULL;
    if (out_count) *out_count = 0;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (!out_states || !out_count) return -1;
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    if (svc->pipe_state_count > 0) {
        rows = (ducknng_pipe_state *)duckdb_malloc(sizeof(*rows) * svc->pipe_state_count);
        if (!rows) {
            if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying active pipe states");
            return -1;
        }
        memset(rows, 0, sizeof(*rows) * svc->pipe_state_count);
        for (i = 0; i < svc->pipe_state_count; i++) {
            if (ducknng_pipe_state_copy(&rows[i], &svc->pipe_states[i]) != 0) {
                if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
                ducknng_service_pipe_states_free(rows, i);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying active pipe state");
                return -1;
            }
        }
    }
    *out_count = svc->pipe_state_count;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    *out_states = rows;
    return 0;
}

int ducknng_service_stop(ducknng_service *svc, char **errmsg) {
    int i;
    ducknng_session **sessions = NULL;
    size_t session_count = 0;
    size_t j;
    (void)errmsg;
    if (!svc) return -1;
    if (svc->mu_initialized) {
        ducknng_mutex_lock(&svc->mu);
        svc->shutting_down = 1;
        ducknng_mutex_unlock(&svc->mu);
    } else {
        svc->shutting_down = 1;
    }
    ducknng_service_stop_http_workers(svc);
    if (svc->listener.id != 0) {
        ducknng_listener_close(svc->listener);
        memset(&svc->listener, 0, sizeof(svc->listener));
    }
    /*
     * NNG socket shutdown marks socket-owned contexts closed and may destroy
     * protocol-private context state for contexts whose public refcount is
     * zero.  A ducknng REP worker keeps an nng_ctx id plus an outstanding AIO,
     * not a public ctx reference.  Closing the socket before canceling and
     * joining those AIO workers can therefore leave nng_ctx_recv/send
     * completions running against freed context state.  Stop accepting first,
     * drain every worker/AIO/context, and close the REP socket last.
     */
    if (svc->ctxs) {
        for (i = 0; i < svc->ncontexts; i++) {
            ducknng_rep_ctx_teardown(&svc->ctxs[i]);
        }
        duckdb_free(svc->ctxs);
        svc->ctxs = NULL;
    }
    if (svc->rep_sock.id != 0) {
        ducknng_socket_close(svc->rep_sock);
        memset(&svc->rep_sock, 0, sizeof(svc->rep_sock));
    }
    if (svc->http_state) {
        ducknng_http_server_stop(svc->http_state);
        svc->http_state = NULL;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    ducknng_service_clear_pipe_states(svc);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    sessions = ducknng_service_detach_all_sessions(svc, &session_count);
    if (svc->resolved_listen_url) {
        duckdb_free(svc->resolved_listen_url);
        svc->resolved_listen_url = NULL;
    }
    svc->running = 0;
    for (j = 0; j < session_count; j++) {
        if (sessions && sessions[j]) ducknng_session_destroy(sessions[j]);
    }
    if (sessions) duckdb_free(sessions);
    return 0;
}

const char *ducknng_service_resolved_listen(const ducknng_service *svc) {
    if (!svc) return NULL;
    return svc->resolved_listen_url ? svc->resolved_listen_url : svc->listen_url;
}
