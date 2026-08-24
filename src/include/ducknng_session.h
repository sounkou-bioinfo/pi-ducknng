#pragma once
#include "duckdb_extension.h"
#include "ducknng_thread.h"
#include <stdint.h>

typedef struct ducknng_schema_cache {
    idx_t ncols;
    char **names;
    duckdb_logical_type *types;
} ducknng_schema_cache;

typedef struct ducknng_session {
    uint64_t session_id;
    char *owner_token;
    char *owner_identity;
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
    uint64_t session_id, const char *owner_token, const char *owner_identity, char **errmsg);
void ducknng_session_destroy(ducknng_session *session);
void ducknng_session_release(ducknng_session *session);
int ducknng_service_add_session(ducknng_service *svc, duckdb_result *result,
    const char *owner_identity, uint64_t *out_session_id, char **out_owner_token, char **errmsg);
int ducknng_service_add_session_streaming(ducknng_service *svc,
    duckdb_connection session_con, size_t pool_index,
    duckdb_prepared_statement stmt, duckdb_pending_result pending,
    const char *owner_identity, uint64_t *out_session_id, char **out_owner_token, char **errmsg);
ducknng_session *ducknng_service_acquire_session(ducknng_service *svc, uint64_t session_id,
    const char *owner_token, const char *caller_identity, int *out_unauthorized);
ducknng_session *ducknng_service_remove_session(ducknng_service *svc, uint64_t session_id,
    const char *owner_token, const char *caller_identity, int *out_unauthorized);
ducknng_session **ducknng_service_detach_all_sessions(ducknng_service *svc, size_t *out_count);
size_t ducknng_service_prune_idle_sessions(ducknng_service *svc, uint64_t now_ms);
