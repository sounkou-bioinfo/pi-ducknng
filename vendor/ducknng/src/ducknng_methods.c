#include "ducknng_manifest.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
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
        json = ducknng_method_registry_manifest_json(&svc->rt->registry, "ducknng", "0.1.1",
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

static int ducknng_session_state_reply(ducknng_method_reply *reply, const char *method_name,
    uint32_t flags, uint64_t session_id, const char *state) {
    char control[256];
    if (!reply || !method_name || !state) return -1;
    snprintf(control, sizeof(control), "{\"session_id\":%llu,\"state\":\"%s\"}",
        (unsigned long long)session_id, state);
    return ducknng_json_reply(reply, method_name, flags, control);
}

typedef struct ducknng_session_control_request {
    uint64_t session_id;
    char *session_token;
} ducknng_session_control_request;

static void ducknng_session_control_request_reset(ducknng_session_control_request *request) {
    if (!request) return;
    if (request->session_token) duckdb_free(request->session_token);
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
    char *owner_token = NULL;
    char json[384];
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
    if (duckdb_pending_prepared(stmt, &pending) == DuckDBError) {
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
            stmt, pending, req->caller_identity, &session_id, &owner_token, &errmsg);
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
    ducknng_query_open_request_destroy(&open_req);
    snprintf(json, sizeof(json), "{\"session_id\":%llu,\"session_token\":\"%s\",\"state\":\"open\",\"next_method\":\"fetch\",\"idle_timeout_ms\":%llu}",
        (unsigned long long)session_id, owner_token ? owner_token : "",
        (unsigned long long)svc->session_idle_ms);
    if (ducknng_json_reply(reply, "query_open", DUCKNNG_RPC_FLAG_SESSION_OPEN, json) != 0) {
        ducknng_session *session = ducknng_service_remove_session(svc, session_id, owner_token,
            req->caller_identity, NULL);
        if (session) ducknng_session_destroy(session);
        if (owner_token) duckdb_free(owner_token);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: failed to build query_open reply");
        return -1;
    }
    if (owner_token) duckdb_free(owner_token);
    return 0;
}

static int ducknng_method_fetch_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    uint64_t session_id = 0;
    int unauthorized = 0;
    ducknng_session *session;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    int has_batch = 0;
    char *errmsg = NULL;
    (void)method;
    if (!svc || !req || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL, "ducknng: missing fetch context");
        return -1;
    }
    memset(&control_req, 0, sizeof(control_req));
    if (ducknng_parse_session_control_request(req, "fetch", &control_req, reply) != 0) {
        return -1;
    }
    session_id = control_req.session_id;
    session = ducknng_service_acquire_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    ducknng_session_control_request_reset(&control_req);
    if (!session) {
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: session not found");
        return -1;
    }
    ducknng_mutex_lock(&session->mu);
    session->last_touch_ms = ducknng_now_ms();
    if (session->cancelled) {
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
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
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR, "ducknng: fetch execution failed");
            return -1;
        }
        if (duckdb_execute_pending(session->session_pending, &session->result) == DuckDBError) {
            char *detail = ducknng_strdup(duckdb_result_error(&session->result));
            duckdb_destroy_result(&session->result);
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_release(session);
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
    if (!session->result_open || ducknng_result_next_chunk_to_ipc(session->result, &payload, &payload_len, &has_batch, &errmsg) != 0) {
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR, errmsg ? errmsg : "ducknng: fetch failed to encode Arrow batch");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    if (has_batch) {
        session->batch_no++;
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
            DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
            payload, payload_len);
        return 0;
    }
    if (session->batch_no == 0) {
        if (ducknng_result_to_ipc_stream(NULL, session->result, &payload, &payload_len, &errmsg) != 0) {
            ducknng_mutex_unlock(&session->mu);
            ducknng_session_release(session);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
                errmsg ? errmsg : "ducknng: fetch failed to encode empty Arrow batch");
            if (errmsg) duckdb_free(errmsg);
            return -1;
        }
        session->eos = 1;
        ducknng_mutex_unlock(&session->mu);
        ducknng_session_release(session);
        ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
            DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM |
                DUCKNNG_RPC_FLAG_END_OF_STREAM,
            payload, payload_len);
        return 0;
    }
    session->eos = 1;
    ducknng_mutex_unlock(&session->mu);
    ducknng_session_release(session);
    return ducknng_session_state_reply(reply, "fetch", DUCKNNG_RPC_FLAG_END_OF_STREAM,
        session_id, "exhausted");
}

static int ducknng_method_close_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    uint64_t session_id = 0;
    int unauthorized = 0;
    ducknng_session *session;
    (void)method;
    memset(&control_req, 0, sizeof(control_req));
    if (ducknng_parse_session_control_request(req, "close", &control_req, reply) != 0) {
        return -1;
    }
    session_id = control_req.session_id;
    session = ducknng_service_remove_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    ducknng_session_control_request_reset(&control_req);
    if (!session) {
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: session not found");
        return -1;
    }
    ducknng_session_destroy(session);
    return ducknng_session_state_reply(reply, "close", DUCKNNG_RPC_FLAG_SESSION_CLOSED,
        session_id, "closed");
}

static int ducknng_method_cancel_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_session_control_request control_req;
    uint64_t session_id = 0;
    int unauthorized = 0;
    ducknng_session *session;
    (void)method;
    memset(&control_req, 0, sizeof(control_req));
    if (ducknng_parse_session_control_request(req, "cancel", &control_req, reply) != 0) {
        return -1;
    }
    session_id = control_req.session_id;
    session = ducknng_service_remove_session(svc, control_req.session_id, control_req.session_token,
        req->caller_identity, &unauthorized);
    ducknng_session_control_request_reset(&control_req);
    if (!session) {
        ducknng_method_reply_set_error(reply, unauthorized ? DUCKNNG_STATUS_UNAUTHORIZED : DUCKNNG_STATUS_NOT_FOUND,
            unauthorized ? ducknng_session_auth_error_message(unauthorized) : "ducknng: session not found");
        return -1;
    }
    if (session->mu_initialized) {
        ducknng_mutex_lock(&session->mu);
        session->cancelled = 1;
        ducknng_mutex_unlock(&session->mu);
    }
    if (session->session_con) {
        duckdb_interrupt(session->session_con);
    }
    ducknng_session_destroy(session);
    return ducknng_session_state_reply(reply, "cancel",
        DUCKNNG_RPC_FLAG_CANCELLED | DUCKNNG_RPC_FLAG_SESSION_CLOSED,
        session_id, "cancelled");
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
    idx_t i;
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
    /* Execute leading statements eagerly (same pattern as query_open). */
    for (i = 0; i + 1 < n_stmts; i++) {
        duckdb_prepared_statement lead_stmt = NULL;
        duckdb_result lead_result;
        memset(&lead_result, 0, sizeof(lead_result));
        if (duckdb_prepare_extracted_statement(scope.con, extracted, i, &lead_stmt) == DuckDBError) {
            char *detail = ducknng_strdup(lead_stmt ? duckdb_prepare_error(lead_stmt) : NULL);
            if (lead_stmt) duckdb_destroy_prepare(&lead_stmt);
            duckdb_destroy_extracted(&extracted);
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_service_leave_request_sql(&scope);
            ducknng_query_open_request_destroy(&open_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                detail && detail[0] ? detail : "ducknng: query_prepare: failed to prepare leading statement");
            if (detail) duckdb_free(detail);
            return -1;
        }
        if (duckdb_execute_prepared(lead_stmt, &lead_result) == DuckDBError) {
            char *detail = ducknng_strdup(duckdb_result_error(&lead_result));
            duckdb_destroy_result(&lead_result);
            duckdb_destroy_prepare(&lead_stmt);
            duckdb_destroy_extracted(&extracted);
            if (svc->rt) ducknng_runtime_current_request_service_set(svc->rt, NULL);
            ducknng_service_leave_request_sql(&scope);
            ducknng_query_open_request_destroy(&open_req);
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
                detail && detail[0] ? detail : "ducknng: query_prepare: failed to execute leading statement");
            if (detail) duckdb_free(detail);
            return -1;
        }
        duckdb_destroy_result(&lead_result);
        duckdb_destroy_prepare(&lead_stmt);
    }
    /* Prepare the final statement. */
    if (duckdb_prepare_extracted_statement(scope.con, extracted, n_stmts - 1, &stmt) == DuckDBError) {
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


static int ducknng_method_exec_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_exec_request exec_req;
    duckdb_result result;
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

    if (exec_req.want_result) {
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
    "{\"fields\":[{\"name\":\"sql\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"batch_rows\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"batch_bytes\",\"type\":\"uint64\",\"nullable\":true}]}",
    "{\"mode\":\"schema_only\",\"note\":\"Arrow IPC stream with schema and 0 data batches\"}",
    ducknng_method_query_prepare_handler
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
    "{\"fields\":[{\"name\":\"sql\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"want_result\",\"type\":\"bool\",\"nullable\":false}]}",
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
    0,
    0,
    0,
    0,
    1,
    "{\"fields\":[{\"name\":\"sql\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"batch_rows\",\"type\":\"uint64\",\"nullable\":true},{\"name\":\"batch_bytes\",\"type\":\"uint64\",\"nullable\":true}]}",
    "{\"type\":\"json\",\"session_open\":true,\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false},{\"name\":\"next_method\",\"type\":\"string\",\"nullable\":false},{\"name\":\"idle_timeout_ms\",\"type\":\"uint64\",\"nullable\":false,\"policy\":\"server_effective\"}]}",
    ducknng_method_query_open_handler
};

const ducknng_method_descriptor ducknng_method_fetch = {
    "fetch",
    "query",
    "Fetch the next Arrow batch from an open session",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_RESPONSE_ROWS,
    DUCKNNG_SESSION_REQUIRES,
    0,
    DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM | DUCKNNG_RPC_FLAG_END_OF_STREAM | DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
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
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false}]}",
    "{\"mode\":\"rows_or_control\",\"control\":{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false}]}}",
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
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false}]}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false}]}",
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
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"session_token\",\"type\":\"string\",\"nullable\":false}]}",
    "{\"type\":\"json\",\"fields\":[{\"name\":\"session_id\",\"type\":\"uint64\",\"nullable\":false},{\"name\":\"state\",\"type\":\"string\",\"nullable\":false}]}",
    ducknng_method_cancel_handler
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
