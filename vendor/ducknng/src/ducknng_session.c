#include "ducknng_session.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <nng/nng.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static void ducknng_session_mark_closing(ducknng_session *session) {
    if (!session || !session->mu_initialized) return;
    ducknng_mutex_lock(&session->mu);
    session->closing = 1;
    if (session->refcount == 0 && session->cv_initialized) ducknng_cond_broadcast(&session->cv);
    ducknng_mutex_unlock(&session->mu);
}

static int ducknng_session_try_acquire(ducknng_session *session) {
    int ok = 0;
    if (!session || !session->mu_initialized) return 0;
    ducknng_mutex_lock(&session->mu);
    if (!session->closing) {
        session->refcount++;
        ok = 1;
    }
    ducknng_mutex_unlock(&session->mu);
    return ok;
}

static void ducknng_service_publish_session_count(ducknng_service *svc) {
    if (!svc) return;
    atomic_store_explicit(&svc->session_count_visible, svc->session_count, memory_order_release);
}

static void ducknng_session_generate_hex_token(char out[33]) {
    static const char hex[] = "0123456789abcdef";
    size_t pos = 0;
    int word;
    if (!out) return;
    for (word = 0; word < 4; word++) {
        uint32_t value = nng_random();
        int shift;
        for (shift = 28; shift >= 0; shift -= 4) {
            out[pos++] = hex[(value >> shift) & 0x0fu];
        }
    }
    out[pos] = '\0';
}

static void ducknng_session_generate_owner_token(char out[33]) {
    ducknng_session_generate_hex_token(out);
}

static void ducknng_session_generate_result_handle(char out[33]) {
    ducknng_session_generate_hex_token(out);
}

static int ducknng_session_token_equal(const char *a, const char *b) {
    size_t la, lb, max_len, i;
    unsigned char diff;
    if (!a || !b) return 0;
    la = strlen(a);
    lb = strlen(b);
    max_len = la > lb ? la : lb;
    diff = (unsigned char)(la ^ lb);
    for (i = 0; i < max_len; i++) {
        unsigned char ca = i < la ? (unsigned char)a[i] : 0;
        unsigned char cb = i < lb ? (unsigned char)b[i] : 0;
        diff |= (unsigned char)(ca ^ cb);
    }
    return diff == 0;
}

static int ducknng_session_owner_auth(const ducknng_session *session, const char *owner_token,
    const char *caller_identity) {
    if (!session || !session->owner_token || !owner_token || !owner_token[0] ||
        !ducknng_session_token_equal(session->owner_token, owner_token)) {
        return DUCKNNG_SESSION_AUTH_TOKEN_MISMATCH;
    }
    if (session->owner_identity && session->owner_identity[0] &&
        (!caller_identity || strcmp(session->owner_identity, caller_identity) != 0)) {
        return DUCKNNG_SESSION_AUTH_IDENTITY_MISMATCH;
    }
    return DUCKNNG_SESSION_AUTH_OK;
}

static ducknng_session *ducknng_service_detach_session_locked(ducknng_service *svc, size_t idx) {
    ducknng_session *session;
    size_t i;
    if (!svc || idx >= svc->session_count) return NULL;
    session = svc->sessions[idx];
    for (i = idx; i + 1 < svc->session_count; i++) svc->sessions[i] = svc->sessions[i + 1];
    svc->session_count--;
    if (svc->sessions) svc->sessions[svc->session_count] = NULL;
    ducknng_service_publish_session_count(svc);
    ducknng_session_mark_closing(session);
    return session;
}

ducknng_session *ducknng_session_create(duckdb_result *result, uint64_t session_id,
    const char *owner_token, const char *owner_identity, char **errmsg) {
    ducknng_session *session = (ducknng_session *)duckdb_malloc(sizeof(*session));
    if (!session) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating session");
        if (result) duckdb_destroy_result(result);
        return NULL;
    }
    memset(session, 0, sizeof(*session));
    if (!owner_token || !owner_token[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing session owner token");
        if (result) duckdb_destroy_result(result);
        duckdb_free(session);
        return NULL;
    }
    session->session_id = session_id;
    session->owner_token = ducknng_strdup(owner_token);
    if (!session->owner_token) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying session owner token");
        if (result) duckdb_destroy_result(result);
        duckdb_free(session);
        return NULL;
    }
    if (owner_identity && owner_identity[0]) {
        session->owner_identity = ducknng_strdup(owner_identity);
        if (!session->owner_identity) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying session owner identity");
            if (result) duckdb_destroy_result(result);
            if (session->owner_token) duckdb_free(session->owner_token);
            duckdb_free(session);
            return NULL;
        }
    }
    if (result) {
        session->result = *result;
        memset(result, 0, sizeof(*result));
        session->result_open = 1;
    }
    session->fetch_batch_chunks = DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS;
    session->last_touch_ms = ducknng_now_ms();
    if (ducknng_mutex_init(&session->mu) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize session mutex");
        if (session->result_open) duckdb_destroy_result(&session->result);
        if (session->owner_token) duckdb_free(session->owner_token);
        if (session->owner_identity) duckdb_free(session->owner_identity);
        duckdb_free(session);
        return NULL;
    }
    session->mu_initialized = 1;
    if (ducknng_cond_init(&session->cv) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize session condition variable");
        if (session->result_open) duckdb_destroy_result(&session->result);
        if (session->owner_token) duckdb_free(session->owner_token);
        if (session->owner_identity) duckdb_free(session->owner_identity);
        ducknng_mutex_destroy(&session->mu);
        duckdb_free(session);
        return NULL;
    }
    session->cv_initialized = 1;
    return session;
}

ducknng_session *ducknng_session_create_streaming(
    ducknng_service *svc, duckdb_connection session_con, size_t pool_index,
    duckdb_prepared_statement stmt, duckdb_pending_result pending,
    uint64_t session_id, const char *owner_token, const char *owner_identity,
    char **errmsg) {
    ducknng_session *session = (ducknng_session *)duckdb_malloc(sizeof(*session));
    if (!session) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating session");
        return NULL;
    }
    memset(session, 0, sizeof(*session));
    if (!owner_token || !owner_token[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing session owner token");
        duckdb_free(session);
        return NULL;
    }
    session->session_id = session_id;
    session->owner_token = ducknng_strdup(owner_token);
    if (!session->owner_token) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying session owner token");
        duckdb_free(session);
        return NULL;
    }
    if (owner_identity && owner_identity[0]) {
        session->owner_identity = ducknng_strdup(owner_identity);
        if (!session->owner_identity) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying session owner identity");
            duckdb_free(session->owner_token);
            duckdb_free(session);
            return NULL;
        }
    }
    session->session_svc = svc;
    session->session_con = session_con;
    session->session_pool_index = pool_index;
    session->session_stmt = stmt;
    session->session_pending = pending;
    session->stmt_open = (stmt != NULL) ? 1 : 0;
    session->pending_open = (pending != NULL) ? 1 : 0;
    session->pending_ready = 0;
    session->fetch_batch_chunks = DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS;
    session->last_touch_ms = ducknng_now_ms();
    if (ducknng_mutex_init(&session->mu) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize session mutex");
        if (session->owner_token) duckdb_free(session->owner_token);
        if (session->owner_identity) duckdb_free(session->owner_identity);
        duckdb_free(session);
        return NULL;
    }
    session->mu_initialized = 1;
    if (ducknng_cond_init(&session->cv) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to initialize session condition variable");
        if (session->owner_token) duckdb_free(session->owner_token);
        if (session->owner_identity) duckdb_free(session->owner_identity);
        ducknng_mutex_destroy(&session->mu);
        duckdb_free(session);
        return NULL;
    }
    session->cv_initialized = 1;
    return session;
}

void ducknng_session_destroy(ducknng_session *session) {
    if (!session) return;
    if (session->mu_initialized) {
        ducknng_mutex_lock(&session->mu);
        session->closing = 1;
        while (session->refcount > 0 && session->cv_initialized) {
            ducknng_cond_wait(&session->cv, &session->mu);
        }
        ducknng_mutex_unlock(&session->mu);
    }
    if (session->result_open) {
        duckdb_destroy_result(&session->result);
        session->result_open = 0;
    }
    if (session->pending_open) {
        duckdb_destroy_pending(&session->session_pending);
        session->pending_open = 0;
    }
    if (session->stmt_open) {
        duckdb_destroy_prepare(&session->session_stmt);
        session->stmt_open = 0;
    }
    /* Upload session: destroying an appender flushes its buffered rows into the
     * open transaction, so we destroy it first, then roll the transaction back
     * on the session connection. An upload that reached commit already cleared
     * both flags, so this only fires for abort/prune/shutdown and guarantees
     * partially uploaded rows never persist. */
    if (session->upload_appender_open) {
        duckdb_appender_destroy(&session->upload_appender);
        session->upload_appender_open = 0;
    }
    if (session->upload_txn_open && session->session_con) {
        duckdb_result rollback_res;
        memset(&rollback_res, 0, sizeof(rollback_res));
        /* duckdb_query populates the result (including the error state) on both
         * success and failure, so it must be destroyed unconditionally. */
        (void)duckdb_query(session->session_con, "ROLLBACK", &rollback_res);
        duckdb_destroy_result(&rollback_res);
        session->upload_txn_open = 0;
    }
    if (session->upload_target) {
        duckdb_free(session->upload_target);
        session->upload_target = NULL;
    }
    if (session->upload_col_names) {
        idx_t ci;
        for (ci = 0; ci < session->upload_col_count; ci++) {
            if (session->upload_col_names[ci]) duckdb_free(session->upload_col_names[ci]);
        }
        duckdb_free(session->upload_col_names);
        session->upload_col_names = NULL;
        session->upload_col_count = 0;
    }
    if (session->session_svc && session->session_pool_index != (size_t)-1) {
        ducknng_service_release_session_connection(session->session_svc, session->session_pool_index);
        session->session_pool_index = (size_t)-1;
        session->session_con = NULL;
    }
    if (session->owner_token) {
        duckdb_free(session->owner_token);
        session->owner_token = NULL;
    }
    if (session->owner_identity) {
        duckdb_free(session->owner_identity);
        session->owner_identity = NULL;
    }
    if (session->result_handle) {
        duckdb_free(session->result_handle);
        session->result_handle = NULL;
    }
    if (session->cv_initialized) {
        ducknng_cond_destroy(&session->cv);
        session->cv_initialized = 0;
    }
    if (session->mu_initialized) {
        ducknng_mutex_destroy(&session->mu);
        session->mu_initialized = 0;
    }
    duckdb_free(session);
}

void ducknng_session_release(ducknng_session *session) {
    if (!session || !session->mu_initialized) return;
    ducknng_mutex_lock(&session->mu);
    if (session->refcount > 0) session->refcount--;
    if (session->closing && session->refcount == 0 && session->cv_initialized) {
        ducknng_cond_broadcast(&session->cv);
    }
    ducknng_mutex_unlock(&session->mu);
}

int ducknng_service_add_session(ducknng_service *svc, duckdb_result *result,
    const char *owner_identity, int row_payload_format,
    uint64_t *out_session_id, char **out_owner_token, char **out_result_handle, char **errmsg) {
    ducknng_session **new_sessions;
    size_t new_cap;
    ducknng_session *session;
    uint64_t session_id;
    char owner_token[33];
    char result_handle[33];
    char *owner_token_copy = NULL;
    char *result_handle_copy = NULL;
    size_t i;
    size_t owner_session_count = 0;
    if (out_session_id) *out_session_id = 0;
    if (out_owner_token) *out_owner_token = NULL;
    if (out_result_handle) *out_result_handle = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing service for session add");
        if (result) duckdb_destroy_result(result);
        return -1;
    }
    ducknng_mutex_lock(&svc->mu);
    if (svc->shutting_down) {
        ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service is stopping");
        if (result) duckdb_destroy_result(result);
        return 1;
    }
    if (svc->max_open_sessions > 0 && svc->session_count >= (size_t)svc->max_open_sessions) {
        ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: max open sessions exceeded");
        if (result) duckdb_destroy_result(result);
        return 1;
    }
    if (svc->max_sessions_per_peer_identity > 0 && owner_identity && owner_identity[0]) {
        for (i = 0; i < svc->session_count; i++) {
            if (svc->sessions[i] && svc->sessions[i]->owner_identity &&
                strcmp(svc->sessions[i]->owner_identity, owner_identity) == 0) {
                owner_session_count++;
            }
        }
        if (owner_session_count >= (size_t)svc->max_sessions_per_peer_identity) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: max sessions per peer identity exceeded");
            if (result) duckdb_destroy_result(result);
            return 1;
        }
    }
    {
        char *rate_errmsg = NULL;
        uint64_t now_ms = ducknng_now_ms();
        if (ducknng_service_check_and_record_session_open_locked(svc, owner_identity, now_ms, &rate_errmsg) != 0) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = rate_errmsg;
            else if (rate_errmsg) duckdb_free(rate_errmsg);
            if (result) duckdb_destroy_result(result);
            return 1;
        }
    }
    if (svc->session_count == svc->session_cap) {
        new_cap = svc->session_cap ? svc->session_cap * 2 : 4;
        new_sessions = (ducknng_session **)duckdb_malloc(sizeof(*new_sessions) * new_cap);
        if (!new_sessions) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing session table");
            if (result) duckdb_destroy_result(result);
            return -1;
        }
        memset(new_sessions, 0, sizeof(*new_sessions) * new_cap);
        if (svc->sessions && svc->session_count) {
            memcpy(new_sessions, svc->sessions, sizeof(*new_sessions) * svc->session_count);
        }
        if (svc->sessions) duckdb_free(svc->sessions);
        svc->sessions = new_sessions;
        svc->session_cap = new_cap;
    }
    session_id = svc->next_session_id;
    ducknng_session_generate_owner_token(owner_token);
    ducknng_session_generate_result_handle(result_handle);
    session = ducknng_session_create(result, session_id, owner_token, owner_identity, errmsg);
    if (!session) {
        ducknng_mutex_unlock(&svc->mu);
        return -1;
    }
    session->result_handle = ducknng_strdup(result_handle);
    if (!session->result_handle) {
        ducknng_mutex_unlock(&svc->mu);
        ducknng_session_destroy(session);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying result handle");
        return -1;
    }
    session->row_payload_format = row_payload_format;
    if (out_owner_token) {
        owner_token_copy = ducknng_strdup(owner_token);
        if (!owner_token_copy) {
            ducknng_mutex_unlock(&svc->mu);
            ducknng_session_destroy(session);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying session owner token");
            return -1;
        }
    }
    if (out_result_handle) {
        result_handle_copy = ducknng_strdup(result_handle);
        if (!result_handle_copy) {
            ducknng_mutex_unlock(&svc->mu);
            ducknng_session_destroy(session);
            if (owner_token_copy) duckdb_free(owner_token_copy);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying result handle");
            return -1;
        }
    }
    svc->next_session_id++;
    svc->sessions[svc->session_count++] = session;
    ducknng_service_publish_session_count(svc);
    ducknng_mutex_unlock(&svc->mu);
    if (out_session_id) *out_session_id = session_id;
    if (out_owner_token) *out_owner_token = owner_token_copy;
    if (out_result_handle) *out_result_handle = result_handle_copy;
    return 0;
}

ducknng_session *ducknng_service_acquire_session(ducknng_service *svc, uint64_t session_id,
    const char *owner_token, const char *caller_identity, int *out_unauthorized) {
    size_t i;
    ducknng_session *session = NULL;
    if (out_unauthorized) *out_unauthorized = DUCKNNG_SESSION_AUTH_OK;
    if (!svc || session_id == 0) return NULL;
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->session_count; i++) {
        if (svc->sessions[i] && svc->sessions[i]->session_id == session_id) {
            int auth = ducknng_session_owner_auth(svc->sessions[i], owner_token, caller_identity);
            if (auth != DUCKNNG_SESSION_AUTH_OK) {
                if (out_unauthorized) *out_unauthorized = auth;
            } else if (ducknng_session_try_acquire(svc->sessions[i])) {
                session = svc->sessions[i];
            }
            break;
        }
    }
    ducknng_mutex_unlock(&svc->mu);
    return session;
}

ducknng_session *ducknng_service_remove_session(ducknng_service *svc, uint64_t session_id,
    const char *owner_token, const char *caller_identity, int *out_unauthorized) {
    size_t i;
    ducknng_session *session = NULL;
    if (out_unauthorized) *out_unauthorized = DUCKNNG_SESSION_AUTH_OK;
    if (!svc || session_id == 0) return NULL;
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->session_count; i++) {
        if (svc->sessions[i] && svc->sessions[i]->session_id == session_id) {
            int auth = ducknng_session_owner_auth(svc->sessions[i], owner_token, caller_identity);
            if (auth != DUCKNNG_SESSION_AUTH_OK) {
                if (out_unauthorized) *out_unauthorized = auth;
            } else {
                session = ducknng_service_detach_session_locked(svc, i);
            }
            break;
        }
    }
    ducknng_mutex_unlock(&svc->mu);
    return session;
}

ducknng_session **ducknng_service_detach_all_sessions(ducknng_service *svc, size_t *out_count) {
    ducknng_session **sessions;
    size_t count;
    size_t i;
    if (out_count) *out_count = 0;
    if (!svc) return NULL;
    if (!svc->mu_initialized) {
        sessions = svc->sessions;
        count = svc->session_count;
        svc->sessions = NULL;
        svc->session_count = 0;
        svc->session_cap = 0;
        ducknng_service_publish_session_count(svc);
        if (out_count) *out_count = count;
        return sessions;
    }
    ducknng_mutex_lock(&svc->mu);
    sessions = svc->sessions;
    count = svc->session_count;
    svc->sessions = NULL;
    svc->session_count = 0;
    svc->session_cap = 0;
    ducknng_service_publish_session_count(svc);
    for (i = 0; i < count; i++) {
        ducknng_session_mark_closing(sessions ? sessions[i] : NULL);
    }
    ducknng_mutex_unlock(&svc->mu);
    if (out_count) *out_count = count;
    return sessions;
}

size_t ducknng_service_prune_idle_sessions(ducknng_service *svc, uint64_t now_ms) {
    size_t i = 0;
    size_t removed = 0;
    if (!svc || svc->session_idle_ms == 0) return 0;
    ducknng_mutex_lock(&svc->mu);
    while (i < svc->session_count) {
        ducknng_session *session = svc->sessions[i];
        int should_remove = 0;
        if (session && session->mu_initialized) {
            ducknng_mutex_lock(&session->mu);
            should_remove = !session->closing && session->refcount == 0 && !session->cancelled && !session->eos &&
                now_ms >= session->last_touch_ms && (now_ms - session->last_touch_ms) > svc->session_idle_ms;
            ducknng_mutex_unlock(&session->mu);
        }
        if (should_remove) {
            session = ducknng_service_detach_session_locked(svc, i);
            removed++;
            ducknng_mutex_unlock(&svc->mu);
            ducknng_session_destroy(session);
            ducknng_mutex_lock(&svc->mu);
            continue;
        }
        i++;
    }
    ducknng_mutex_unlock(&svc->mu);
    return removed;
}

/* Shared session registrar for both query (streaming) and upload sessions.
 * For upload sessions (is_upload) the appender/target/transaction ownership is
 * installed on the session while svc->mu is still held and before the session
 * is inserted into svc->sessions, so a concurrent stop/detach can never see a
 * bare session and release its pinned connection without destroying the
 * appender and rolling the transaction back. */
static int ducknng_service_add_session_full(ducknng_service *svc,
    duckdb_connection session_con, size_t pool_index,
    duckdb_prepared_statement stmt, duckdb_pending_result pending,
    duckdb_appender appender, int is_upload, const char *target,
    char **col_names, idx_t col_count,
    const char *owner_identity, int row_payload_format, uint64_t fetch_batch_chunks,
    uint64_t *out_session_id, char **out_owner_token, char **out_result_handle, char **errmsg) {
    ducknng_session **new_sessions;
    size_t new_cap;
    ducknng_session *session;
    uint64_t session_id;
    char owner_token[33];
    char result_handle[33];
    char *owner_token_copy = NULL;
    char *result_handle_copy = NULL;
    size_t i;
    size_t owner_session_count = 0;
    if (out_session_id) *out_session_id = 0;
    if (out_owner_token) *out_owner_token = NULL;
    if (out_result_handle) *out_result_handle = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing service for session add");
        return -1;
    }
    ducknng_mutex_lock(&svc->mu);
    if (svc->shutting_down) {
        ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service is stopping");
        return 1;
    }
    if (svc->max_open_sessions > 0 && svc->session_count >= (size_t)svc->max_open_sessions) {
        ducknng_mutex_unlock(&svc->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: max open sessions exceeded");
        return 1;
    }
    if (svc->max_sessions_per_peer_identity > 0 && owner_identity && owner_identity[0]) {
        for (i = 0; i < svc->session_count; i++) {
            if (svc->sessions[i] && svc->sessions[i]->owner_identity &&
                strcmp(svc->sessions[i]->owner_identity, owner_identity) == 0) {
                owner_session_count++;
            }
        }
        if (owner_session_count >= (size_t)svc->max_sessions_per_peer_identity) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: max sessions per peer identity exceeded");
            return 1;
        }
    }
    {
        char *rate_errmsg = NULL;
        uint64_t now_ms = ducknng_now_ms();
        if (ducknng_service_check_and_record_session_open_locked(svc, owner_identity, now_ms, &rate_errmsg) != 0) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = rate_errmsg;
            else if (rate_errmsg) duckdb_free(rate_errmsg);
            return 1;
        }
    }
    if (svc->session_count == svc->session_cap) {
        new_cap = svc->session_cap ? svc->session_cap * 2 : 4;
        new_sessions = (ducknng_session **)duckdb_malloc(sizeof(*new_sessions) * new_cap);
        if (!new_sessions) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing session table");
            return -1;
        }
        memset(new_sessions, 0, sizeof(*new_sessions) * new_cap);
        if (svc->sessions && svc->session_count) {
            memcpy(new_sessions, svc->sessions, sizeof(*new_sessions) * svc->session_count);
        }
        if (svc->sessions) duckdb_free(svc->sessions);
        svc->sessions = new_sessions;
        svc->session_cap = new_cap;
    }
    session_id = svc->next_session_id;
    ducknng_session_generate_owner_token(owner_token);
    ducknng_session_generate_result_handle(result_handle);
    session = ducknng_session_create_streaming(svc, session_con, pool_index, stmt, pending,
        session_id, owner_token, owner_identity, errmsg);
    if (!session) {
        ducknng_mutex_unlock(&svc->mu);
        return -1;
    }
    session->result_handle = ducknng_strdup(result_handle);
    if (!session->result_handle) {
        ducknng_mutex_unlock(&svc->mu);
        /* detach streaming resources before destroy to avoid double-release */
        session->session_svc = NULL;
        session->session_pool_index = (size_t)-1;
        session->stmt_open = 0;
        session->pending_open = 0;
        ducknng_session_destroy(session);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying result handle");
        return -1;
    }
    session->row_payload_format = row_payload_format;
    if (fetch_batch_chunks == 0) fetch_batch_chunks = DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS;
    if (fetch_batch_chunks > DUCKNNG_MAX_FETCH_BATCH_CHUNKS) fetch_batch_chunks = DUCKNNG_MAX_FETCH_BATCH_CHUNKS;
    session->fetch_batch_chunks = fetch_batch_chunks;
    if (out_owner_token) {
        owner_token_copy = ducknng_strdup(owner_token);
        if (!owner_token_copy) {
            ducknng_mutex_unlock(&svc->mu);
            /* detach streaming resources before destroy to avoid double-release */
            session->session_svc = NULL;
            session->session_pool_index = (size_t)-1;
            session->stmt_open = 0;
            session->pending_open = 0;
            ducknng_session_destroy(session);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying session owner token");
            return -1;
        }
    }
    if (out_result_handle) {
        result_handle_copy = ducknng_strdup(result_handle);
        if (!result_handle_copy) {
            ducknng_mutex_unlock(&svc->mu);
            if (owner_token_copy) duckdb_free(owner_token_copy);
            /* detach streaming resources before destroy to avoid double-release */
            session->session_svc = NULL;
            session->session_pool_index = (size_t)-1;
            session->stmt_open = 0;
            session->pending_open = 0;
            ducknng_session_destroy(session);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying result handle");
            return -1;
        }
    }
    /* Install upload ownership under the lock and before insertion: past all
     * pre-insert error paths (so a failure never destroys the caller's
     * appender/transaction) and before the session is visible to any concurrent
     * stop/detach (so teardown always sees a full upload session). */
    if (is_upload) {
        session->is_upload = 1;
        session->upload_appender = appender;
        session->upload_appender_open = 1;
        session->upload_txn_open = 1;
        session->upload_target = (target && target[0]) ? ducknng_strdup(target) : NULL;
        session->upload_col_names = col_names;
        session->upload_col_count = col_count;
    }
    svc->next_session_id++;
    svc->sessions[svc->session_count++] = session;
    ducknng_service_publish_session_count(svc);
    ducknng_mutex_unlock(&svc->mu);
    if (out_session_id) *out_session_id = session_id;
    if (out_owner_token) *out_owner_token = owner_token_copy;
    if (out_result_handle) *out_result_handle = result_handle_copy;
    return 0;
}

int ducknng_service_add_session_streaming(ducknng_service *svc,
    duckdb_connection session_con, size_t pool_index,
    duckdb_prepared_statement stmt, duckdb_pending_result pending,
    const char *owner_identity, int row_payload_format, uint64_t fetch_batch_chunks,
    uint64_t *out_session_id, char **out_owner_token, char **out_result_handle, char **errmsg) {
    return ducknng_service_add_session_full(svc, session_con, pool_index, stmt, pending,
        NULL, 0, NULL, NULL, 0, owner_identity, row_payload_format, fetch_batch_chunks,
        out_session_id, out_owner_token, out_result_handle, errmsg);
}

int ducknng_service_add_upload_session(ducknng_service *svc,
    duckdb_connection session_con, size_t pool_index, duckdb_appender appender,
    const char *target, char **col_names, idx_t col_count, const char *owner_identity,
    uint64_t *out_session_id, char **out_owner_token, char **out_result_handle, char **errmsg) {
    if (out_session_id) *out_session_id = 0;
    if (out_owner_token) *out_owner_token = NULL;
    if (out_result_handle) *out_result_handle = NULL;
    if (!appender) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing upload session appender");
        return -1;
    }
    return ducknng_service_add_session_full(svc, session_con, pool_index, NULL, NULL,
        appender, 1, target, col_names, col_count, owner_identity, 0, 0,
        out_session_id, out_owner_token, out_result_handle, errmsg);
}
