#pragma once
#include "duckdb_extension.h"
#include "ducknng_thread.h"
#include <stdint.h>

#define DUCKNNG_DEFAULT_FETCH_BATCH_CHUNKS 12u
#define DUCKNNG_MAX_FETCH_BATCH_CHUNKS 256u

typedef struct ducknng_schema_cache {
    idx_t ncols;
    char **names;
    duckdb_logical_type *types;
} ducknng_schema_cache;

typedef struct ducknng_session {
    uint64_t session_id;
    char *owner_token;
    char *owner_identity;
    char *result_handle;
    int row_payload_format;
    uint64_t fetch_batch_chunks;
    int row_schema_sent;
    duckdb_result result;
    int result_open;
    int eos;
    int cancelled;
    uint64_t batch_no;
    uint64_t last_touch_ms;
    ducknng_mutex mu;
    ducknng_cond cv;
    uint32_t refcount;
    int closing;
    int mu_initialized;
    int cv_initialized;
    /* streaming session fields — set by ducknng_session_create_streaming */
    struct ducknng_service *session_svc; /* weak ref, for pool release on destroy */
    duckdb_connection session_con;
    size_t session_pool_index;
    duckdb_prepared_statement session_stmt;
    duckdb_pending_result session_pending;
    int pending_ready;  /* 1 after duckdb_execute_pending has been called */
    int stmt_open;
    int pending_open;
    /* upload session fields — set by ducknng_service_add_upload_session. An
     * upload session pins its connection like a query session, but instead of
     * a prepared statement it holds an open appender inside an open
     * transaction on session_con; commit flushes+commits, abort/prune/destroy
     * rolls the transaction back so partially uploaded rows never persist. */
    int is_upload;
    duckdb_appender upload_appender;
    int upload_appender_open;
    int upload_txn_open;
    char *upload_target;
    uint64_t upload_rows;
    /* Target table column names in ordinal order, captured at open, so
     * upload_append can reject batches whose column names/order differ from
     * the target rather than let the ordinal appender misassign them. */
    char **upload_col_names;
    idx_t upload_col_count;
} ducknng_session;

typedef struct ducknng_service ducknng_service;

enum {
    DUCKNNG_SESSION_AUTH_OK = 0,
    DUCKNNG_SESSION_AUTH_TOKEN_MISMATCH = 1,
    DUCKNNG_SESSION_AUTH_IDENTITY_MISMATCH = 2
};

ducknng_session *ducknng_session_create(duckdb_result *result, uint64_t session_id,
    const char *owner_token, const char *owner_identity, char **errmsg);
ducknng_session *ducknng_session_create_streaming(
    struct ducknng_service *svc, duckdb_connection session_con, size_t pool_index,
    duckdb_prepared_statement stmt, duckdb_pending_result pending,
    uint64_t session_id, const char *owner_token, const char *owner_identity,
    char **errmsg);
void ducknng_session_destroy(ducknng_session *session);
void ducknng_session_release(ducknng_session *session);
int ducknng_service_add_session(ducknng_service *svc, duckdb_result *result,
    const char *owner_identity, int row_payload_format,
    uint64_t *out_session_id, char **out_owner_token, char **out_result_handle, char **errmsg);
int ducknng_service_add_session_streaming(ducknng_service *svc,
    duckdb_connection session_con, size_t pool_index,
    duckdb_prepared_statement stmt, duckdb_pending_result pending,
    const char *owner_identity, int row_payload_format, uint64_t fetch_batch_chunks,
    uint64_t *out_session_id, char **out_owner_token, char **out_result_handle, char **errmsg);
/* Register an upload session: session_con is pinned for the session's life
 * with an open appender inside an open transaction (both created by the
 * caller). target is the target table name (copied, for diagnostics). On
 * success the session owns the appender and transaction; on failure the
 * caller retains them. */
/* col_names (col_count entries, ordinal order) is the target's column-name
 * list; ownership of the array and its strings transfers to the session on
 * success and they are freed on teardown. */
int ducknng_service_add_upload_session(ducknng_service *svc,
    duckdb_connection session_con, size_t pool_index, duckdb_appender appender,
    const char *target, char **col_names, idx_t col_count, const char *owner_identity,
    uint64_t *out_session_id, char **out_owner_token, char **out_result_handle, char **errmsg);
ducknng_session *ducknng_service_acquire_session(ducknng_service *svc, uint64_t session_id,
    const char *owner_token, const char *caller_identity, int *out_unauthorized);
ducknng_session *ducknng_service_remove_session(ducknng_service *svc, uint64_t session_id,
    const char *owner_token, const char *caller_identity, int *out_unauthorized);
ducknng_session **ducknng_service_detach_all_sessions(ducknng_service *svc, size_t *out_count);
size_t ducknng_service_prune_idle_sessions(ducknng_service *svc, uint64_t now_ms);
