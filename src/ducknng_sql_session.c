#include "ducknng_sql_shared.h"
#include "ducknng_http_compat.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_arrow.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include "ducknng_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    ducknng_sql_context *ctx;
    char *url;
    char *sql;
    char *method_name;
    uint64_t session_id;
    char *session_token;
    uint64_t batch_rows;
    uint64_t batch_bytes;
    uint64_t tls_config_id;
    int expect_json_only;
    int executed;
    bool ok;
    char *error;
    char *state;
    char *next_method;
    char *control_json;
    uint64_t idle_timeout_ms;
    int has_idle_timeout_ms;
    uint8_t *payload;
    idx_t payload_len;
    bool end_of_stream;
} ducknng_session_result_bind_data;

typedef struct {
    idx_t emitted;
} ducknng_single_row_init_data;

typedef struct {
    ducknng_sql_context *ctx;
    char *url;
    uint64_t session_id;
    char *session_token;
    uint64_t batch_rows;
    uint64_t batch_bytes;
    uint64_t tls_config_id;
    struct ArrowSchema schema;
    struct ArrowArray array;
    idx_t row_count;
    uint64_t rebind_cache_generation;
} ducknng_fetch_query_table_bind_data;

typedef struct {
    ducknng_fetch_query_table_bind_data *bind;
    idx_t offset;
} ducknng_fetch_query_table_init_data;

typedef struct {
    uint64_t generation;
    char *url;
    uint64_t session_id;
    char *session_token;
    uint64_t batch_rows;
    uint64_t batch_bytes;
    uint64_t tls_config_id;
    uint8_t *payload;
    idx_t payload_len;
    bool end_of_stream;
    uint64_t expires_at_ms;
} ducknng_fetch_query_table_rebind_cache;

static _Thread_local ducknng_fetch_query_table_rebind_cache ducknng_fetch_query_table_cache;
static _Thread_local uint64_t ducknng_fetch_query_table_cache_generation;

static void destroy_single_row_init_data(void *ptr) {
    ducknng_single_row_init_data *data = (ducknng_single_row_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void ducknng_session_result_bind_data_reset(ducknng_session_result_bind_data *data) {
    if (!data) return;
    if (data->url) duckdb_free(data->url);
    if (data->sql) duckdb_free(data->sql);
    if (data->method_name) duckdb_free(data->method_name);
    if (data->session_token) duckdb_free(data->session_token);
    if (data->error) duckdb_free(data->error);
    if (data->state) duckdb_free(data->state);
    if (data->next_method) duckdb_free(data->next_method);
    if (data->control_json) duckdb_free(data->control_json);
    if (data->payload) duckdb_free(data->payload);
    memset(data, 0, sizeof(*data));
}

static void destroy_session_result_bind_data(void *ptr) {
    ducknng_session_result_bind_data *data = (ducknng_session_result_bind_data *)ptr;
    if (!data) return;
    ducknng_session_result_bind_data_reset(data);
    duckdb_free(data);
}

static void destroy_fetch_query_table_bind_data(void *ptr) {
    ducknng_fetch_query_table_bind_data *data = (ducknng_fetch_query_table_bind_data *)ptr;
    if (!data) return;
    if (data->url) duckdb_free(data->url);
    if (data->session_token) duckdb_free(data->session_token);
    if (data->array.release) ArrowArrayRelease(&data->array);
    if (data->schema.release) ArrowSchemaRelease(&data->schema);
    duckdb_free(data);
}

static void destroy_fetch_query_table_init_data(void *ptr) {
    ducknng_fetch_query_table_init_data *data = (ducknng_fetch_query_table_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void ducknng_fetch_query_table_cache_reset(void) {
    if (ducknng_fetch_query_table_cache.url) duckdb_free(ducknng_fetch_query_table_cache.url);
    if (ducknng_fetch_query_table_cache.session_token) duckdb_free(ducknng_fetch_query_table_cache.session_token);
    if (ducknng_fetch_query_table_cache.payload) duckdb_free(ducknng_fetch_query_table_cache.payload);
    memset(&ducknng_fetch_query_table_cache, 0, sizeof(ducknng_fetch_query_table_cache));
}

static int ducknng_fetch_query_table_cache_matches(
    const ducknng_fetch_query_table_bind_data *bind) {
    if (!bind || ducknng_fetch_query_table_cache.generation == 0 ||
        !ducknng_fetch_query_table_cache.url ||
        !ducknng_fetch_query_table_cache.session_token ||
        !ducknng_fetch_query_table_cache.payload) {
        return 0;
    }
    if (ducknng_fetch_query_table_cache.expires_at_ms != 0 &&
        ducknng_now_ms() > ducknng_fetch_query_table_cache.expires_at_ms) {
        ducknng_fetch_query_table_cache_reset();
        return 0;
    }
    return ducknng_fetch_query_table_cache.session_id == bind->session_id &&
        ducknng_fetch_query_table_cache.batch_rows == bind->batch_rows &&
        ducknng_fetch_query_table_cache.batch_bytes == bind->batch_bytes &&
        ducknng_fetch_query_table_cache.tls_config_id == bind->tls_config_id &&
        strcmp(ducknng_fetch_query_table_cache.url, bind->url) == 0 &&
        strcmp(ducknng_fetch_query_table_cache.session_token, bind->session_token) == 0;
}

static int ducknng_fetch_query_table_cache_store(
    ducknng_fetch_query_table_bind_data *bind,
    const ducknng_session_result_bind_data *resp) {
    uint8_t *payload_copy = NULL;
    char *url_copy = NULL;
    char *token_copy = NULL;
    if (!bind || !resp || !resp->payload) return -1;
    if (resp->payload_len > 0) {
        payload_copy = (uint8_t *)duckdb_malloc((size_t)resp->payload_len);
        if (!payload_copy) return -1;
        memcpy(payload_copy, resp->payload, (size_t)resp->payload_len);
    }
    url_copy = ducknng_strdup(bind->url);
    token_copy = ducknng_strdup(bind->session_token);
    if (!url_copy || !token_copy) {
        if (payload_copy) duckdb_free(payload_copy);
        if (url_copy) duckdb_free(url_copy);
        if (token_copy) duckdb_free(token_copy);
        return -1;
    }
    ducknng_fetch_query_table_cache_reset();
    ducknng_fetch_query_table_cache_generation++;
    if (ducknng_fetch_query_table_cache_generation == 0) ducknng_fetch_query_table_cache_generation++;
    ducknng_fetch_query_table_cache.generation = ducknng_fetch_query_table_cache_generation;
    ducknng_fetch_query_table_cache.url = url_copy;
    ducknng_fetch_query_table_cache.session_id = bind->session_id;
    ducknng_fetch_query_table_cache.session_token = token_copy;
    ducknng_fetch_query_table_cache.batch_rows = bind->batch_rows;
    ducknng_fetch_query_table_cache.batch_bytes = bind->batch_bytes;
    ducknng_fetch_query_table_cache.tls_config_id = bind->tls_config_id;
    ducknng_fetch_query_table_cache.payload = payload_copy;
    ducknng_fetch_query_table_cache.payload_len = resp->payload_len;
    ducknng_fetch_query_table_cache.end_of_stream = resp->end_of_stream;
    ducknng_fetch_query_table_cache.expires_at_ms = ducknng_now_ms() + 250;
    bind->rebind_cache_generation = ducknng_fetch_query_table_cache.generation;
    return 0;
}

static int ducknng_fetch_query_table_cache_copy_response(
    ducknng_fetch_query_table_bind_data *bind,
    ducknng_session_result_bind_data *resp) {
    uint8_t *payload_copy = NULL;
    if (!bind || !resp || !ducknng_fetch_query_table_cache_matches(bind)) return -1;
    if (ducknng_fetch_query_table_cache.payload_len > 0) {
        payload_copy = (uint8_t *)duckdb_malloc((size_t)ducknng_fetch_query_table_cache.payload_len);
        if (!payload_copy) return -1;
        memcpy(payload_copy, ducknng_fetch_query_table_cache.payload,
            (size_t)ducknng_fetch_query_table_cache.payload_len);
    }
    memset(resp, 0, sizeof(*resp));
    resp->ok = true;
    resp->session_id = bind->session_id;
    resp->payload = payload_copy;
    resp->payload_len = ducknng_fetch_query_table_cache.payload_len;
    resp->end_of_stream = ducknng_fetch_query_table_cache.end_of_stream;
    bind->rebind_cache_generation = ducknng_fetch_query_table_cache.generation;
    return 0;
}

static int ducknng_fetch_query_table_result_set(ducknng_fetch_query_table_bind_data *bind,
    ducknng_session_result_bind_data *resp, duckdb_bind_info info) {
    char *errmsg = NULL;
    if (!bind || !resp) return -1;
    if (!resp->ok) {
        duckdb_bind_set_error(info, resp->error ? resp->error : "ducknng: fetch_query_table request failed");
        return -1;
    }
    if (!resp->payload) {
        duckdb_bind_set_error(info,
            "ducknng: fetch_query_table expected an Arrow row payload; use ducknng_fetch_query(...) for control-only session replies");
        return -1;
    }
    if (ducknng_decode_ipc_table_payload(resp->payload, (size_t)resp->payload_len,
            &bind->schema, &bind->array, &errmsg) != 0) {
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: failed to decode fetch_query_table Arrow payload");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    bind->row_count = (idx_t)bind->array.length;
    if (bind->schema.n_children < 0 || bind->schema.n_children != bind->array.n_children) {
        duckdb_bind_set_error(info, "ducknng: invalid fetch_query_table Arrow schema");
        return -1;
    }
    if (ducknng_sql_arrow_bind_result_columns(info, &bind->schema, &errmsg) != 0) {
        duckdb_bind_set_error(info, errmsg ? errmsg : "ducknng: unsupported fetch_query_table Arrow type");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    return 0;
}

static char *ducknng_frame_error_detail(const ducknng_frame *frame, const char *fallback) {
    char *detail;
    if (!frame || !frame->error || frame->error_len == 0) return ducknng_strdup(fallback);
    detail = (char *)duckdb_malloc((size_t)frame->error_len + 1);
    if (!detail) return ducknng_strdup("ducknng: out of memory decoding remote error");
    memcpy(detail, frame->error, (size_t)frame->error_len);
    detail[frame->error_len] = '\0';
    return detail;
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

static int ducknng_json_extract_u64_value(const char *json, const char *key, uint64_t *out) {
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

static nng_msg *ducknng_client_method_request(const char *method_name, const void *payload,
    size_t payload_len, char **errmsg) {
    nng_msg *msg;
    msg = ducknng_build_reply(DUCKNNG_RPC_CALL, method_name, 0, NULL, payload, (uint64_t)payload_len);
    if (!msg && errmsg && !*errmsg) {
        *errmsg = ducknng_strdup("ducknng: failed to allocate RPC request message");
    }
    return msg;
}

static nng_msg *ducknng_client_roundtrip_tls(const char *url, nng_msg *req, int timeout_ms,
    const ducknng_tls_opts *tls_opts, char **errmsg);

static nng_msg *ducknng_client_method_roundtrip_tls(const char *url, const char *method_name,
    const void *payload, size_t payload_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    char **errmsg) {
    nng_msg *req = ducknng_client_method_request(method_name, payload, payload_len, errmsg);
    if (!req) return NULL;
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg);
}

static void ducknng_session_result_fill_from_response(ducknng_session_result_bind_data *bind,
    uint64_t fallback_session_id, nng_msg *resp_msg) {
    ducknng_frame frame;
    if (!bind) return;
    bind->session_id = fallback_session_id;
    if (!resp_msg) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: missing session response");
        return;
    }
    if (ducknng_decode_request(resp_msg, &frame) != 0) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: invalid session response envelope");
        return;
    }
    if (frame.type == DUCKNNG_RPC_ERROR) {
        bind->ok = false;
        bind->error = ducknng_frame_error_detail(&frame, "ducknng: session request failed");
        return;
    }
    if (frame.type != DUCKNNG_RPC_RESULT) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: session response was not an RPC result");
        return;
    }
    bind->end_of_stream = (frame.flags & DUCKNNG_RPC_FLAG_END_OF_STREAM) != 0;
    if (frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON) {
        bind->control_json = (char *)duckdb_malloc((size_t)frame.payload_len + 1);
        if (!bind->control_json) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: out of memory copying session control payload");
            return;
        }
        if (frame.payload_len) memcpy(bind->control_json, frame.payload, (size_t)frame.payload_len);
        bind->control_json[frame.payload_len] = '\0';
        (void)ducknng_json_extract_u64_value(bind->control_json, "session_id", &bind->session_id);
        if (!bind->session_token) bind->session_token = ducknng_json_extract_string_dup(bind->control_json, "session_token");
        bind->state = ducknng_json_extract_string_dup(bind->control_json, "state");
        bind->next_method = ducknng_json_extract_string_dup(bind->control_json, "next_method");
        if (ducknng_json_extract_u64_value(bind->control_json, "idle_timeout_ms", &bind->idle_timeout_ms) == 0) {
            bind->has_idle_timeout_ms = 1;
        }
        bind->ok = true;
        return;
    }
    if ((frame.flags & DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM) && (frame.flags & DUCKNNG_RPC_FLAG_RESULT_ROWS)) {
        bind->payload_len = (idx_t)frame.payload_len;
        bind->payload = (uint8_t *)duckdb_malloc((size_t)bind->payload_len);
        if (!bind->payload && bind->payload_len > 0) {
            bind->ok = false;
            bind->error = ducknng_strdup("ducknng: out of memory copying session row payload");
            return;
        }
        if (bind->payload_len) memcpy(bind->payload, frame.payload, (size_t)bind->payload_len);
        bind->ok = true;
        return;
    }
    bind->ok = false;
    bind->error = ducknng_strdup("ducknng: unexpected session response payload kind");
}


static int ducknng_client_open_req_socket_tls(const char *url, int timeout_ms, const ducknng_tls_opts *tls_opts, nng_socket *out, char **errmsg) {
    int rv;
    if (!url || !url[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: client URL is required");
        return -1;
    }
    if (ducknng_socket_validate_client_url(url, tls_opts, errmsg) != 0) return -1;
    rv = ducknng_req_socket_open(out);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    rv = ducknng_socket_set_timeout_ms(*out, timeout_ms, timeout_ms);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    rv = ducknng_socket_apply_tls(*out, url, tls_opts);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    rv = ducknng_socket_dial(*out, url);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_socket_close(*out);
        return -1;
    }
    return 0;
}

static int ducknng_client_open_req_socket(const char *url, int timeout_ms, nng_socket *out, char **errmsg) {
    return ducknng_client_open_req_socket_tls(url, timeout_ms, NULL, out, errmsg);
}

static nng_msg *ducknng_client_roundtrip_tls(const char *url, nng_msg *req, int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    ducknng_transport_url parsed;
    nng_socket sock;
    nng_msg *resp = NULL;
    uint8_t *reply_frame = NULL;
    size_t reply_frame_len = 0;
    int rv;
    memset(&sock, 0, sizeof(sock));
    if (!req) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: request message is required");
        return NULL;
    }
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    if (ducknng_transport_url_is_http(&parsed)) {
        if (ducknng_http_frame_transact(url, (const uint8_t *)nng_msg_body(req), nng_msg_len(req),
                timeout_ms, tls_opts, &reply_frame, &reply_frame_len, errmsg) != 0) {
            nng_msg_free(req);
            return NULL;
        }
        nng_msg_free(req);
        rv = nng_msg_alloc(&resp, reply_frame_len);
        if (rv != 0) {
            if (reply_frame) duckdb_free(reply_frame);
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
            return NULL;
        }
        if (reply_frame_len) memcpy(nng_msg_body(resp), reply_frame, reply_frame_len);
        if (reply_frame) duckdb_free(reply_frame);
        return resp;
    }
    if (ducknng_client_open_req_socket_tls(url, timeout_ms, tls_opts, &sock, errmsg) != 0) {
        nng_msg_free(req);
        return NULL;
    }
    rv = ducknng_req_transact(sock, req, &resp);
    ducknng_socket_close(sock);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    return resp;
}

static nng_msg *ducknng_client_roundtrip(const char *url, nng_msg *req, int timeout_ms, char **errmsg) {
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, NULL, errmsg);
}

static nng_msg *ducknng_client_roundtrip_raw_tls(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, const ducknng_tls_opts *tls_opts, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip_tls(url, req, timeout_ms, tls_opts, errmsg);
}

static nng_msg *ducknng_client_roundtrip_raw(const char *url, const uint8_t *payload, size_t payload_len,
    int timeout_ms, char **errmsg) {
    nng_msg *req = NULL;
    int rv = nng_msg_alloc(&req, payload_len);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return NULL;
    }
    if (payload_len) memcpy(nng_msg_body(req), payload, payload_len);
    return ducknng_client_roundtrip(url, req, timeout_ms, errmsg);
}


static int ducknng_lookup_tls_config_copy(ducknng_sql_context *ctx, uint64_t tls_config_id,
    uint64_t *out_id, char **out_source, ducknng_tls_opts *out_opts, char **errmsg) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (out_id) *out_id = 0;
    if (out_source) *out_source = NULL;
    if (out_opts) ducknng_tls_opts_init(out_opts);
    if (tls_config_id == 0) return 0;
    if (!ctx || !ctx->rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for TLS lookup");
        return -1;
    }
    ducknng_mutex_lock(&ctx->rt->mu);
    for (i = 0; i < ctx->rt->tls_config_count; i++) {
        if (ctx->rt->tls_configs[i] && ctx->rt->tls_configs[i]->tls_config_id == tls_config_id) {
            cfg = ctx->rt->tls_configs[i];
            break;
        }
    }
    if (!cfg) {
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: tls config not found");
        return -1;
    }
    if (out_id) *out_id = cfg->tls_config_id;
    if (out_source && cfg->source) {
        *out_source = ducknng_strdup(cfg->source);
        if (!*out_source) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS source");
            return -1;
        }
    }
    if (out_opts && ducknng_tls_opts_copy(out_opts, &cfg->opts) != 0) {
        if (out_source && *out_source) {
            duckdb_free(*out_source);
            *out_source = NULL;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying TLS options");
        return -1;
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    return 0;
}

static int ducknng_lookup_tls_opts(ducknng_sql_context *ctx, uint64_t tls_config_id,
    const ducknng_tls_opts **out_opts, char **errmsg) {
    static _Thread_local ducknng_tls_opts tls_copy;
    static _Thread_local int tls_copy_valid = 0;
    if (out_opts) *out_opts = NULL;
    if (tls_copy_valid) {
        ducknng_tls_opts_reset(&tls_copy);
        tls_copy_valid = 0;
    }
    if (tls_config_id == 0) return 0;
    if (ducknng_lookup_tls_config_copy(ctx, tls_config_id, NULL, NULL, &tls_copy, errmsg) != 0) {
        return -1;
    }
    tls_copy_valid = 1;
    if (out_opts) *out_opts = &tls_copy;
    return 0;
}

static void ducknng_session_bind_add_control_columns(duckdb_bind_info info) {
    duckdb_logical_type type;
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "ok", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "error", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "session_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "session_token", type);
    duckdb_bind_add_result_column(info, "state", type);
    duckdb_bind_add_result_column(info, "next_method", type);
    duckdb_bind_add_result_column(info, "control_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "idle_timeout_ms", type);
    duckdb_destroy_logical_type(&type);
}

static void ducknng_session_bind_common(ducknng_session_result_bind_data *bind,
    ducknng_sql_context *ctx, const char *url, const char *method_name,
    const uint8_t *payload, size_t payload_len, uint64_t tls_config_id,
    uint64_t fallback_session_id, int expect_json_only) {
    const ducknng_tls_opts *tls_opts = NULL;
    char *errmsg = NULL;
    nng_msg *resp_msg = NULL;
    if (!bind) return;
    if (!url || !url[0]) {
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: session URL must not be NULL or empty");
        return;
    }
    if (ducknng_lookup_tls_opts(ctx, tls_config_id, &tls_opts, &errmsg) != 0) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: tls config not found");
        return;
    }
    resp_msg = ducknng_client_method_roundtrip_tls(url, method_name, payload, payload_len, 5000, tls_opts, &errmsg);
    if (!resp_msg) {
        bind->ok = false;
        bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: session request failed");
        return;
    }
    ducknng_session_result_fill_from_response(bind, fallback_session_id, resp_msg);
    nng_msg_free(resp_msg);
    if (expect_json_only && bind->ok && !bind->control_json) {
        if (bind->payload) {
            duckdb_free(bind->payload);
            bind->payload = NULL;
            bind->payload_len = 0;
        }
        bind->ok = false;
        bind->error = ducknng_strdup("ducknng: session control request expected a JSON control reply");
    }
}

static void ducknng_open_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_value url_val;
    duckdb_value sql_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    sql_val = duckdb_bind_get_parameter(info, 1);
    batch_rows_val = duckdb_bind_get_parameter(info, 2);
    batch_bytes_val = duckdb_bind_get_parameter(info, 3);
    tls_val = duckdb_bind_get_parameter(info, 4);
    bind->url = duckdb_get_varchar(url_val);
    bind->sql = duckdb_get_varchar(sql_val);
    bind->method_name = ducknng_strdup("query_open");
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&sql_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || !bind->sql || !bind->sql[0]) {
        bind->error = ducknng_strdup("ducknng: open_query requires non-empty url and sql");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing open_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_close_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("close");
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->error = ducknng_strdup("ducknng: close_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing close_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_cancel_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    tls_val = duckdb_bind_get_parameter(info, 3);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("cancel");
    bind->expect_json_only = 1;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->error = ducknng_strdup("ducknng: cancel_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing cancel_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_fetch_query_bind(duckdb_bind_info info) {
    ducknng_session_result_bind_data *bind;
    duckdb_logical_type type;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_session_result_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    batch_rows_val = duckdb_bind_get_parameter(info, 3);
    batch_bytes_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    bind->method_name = ducknng_strdup("fetch");
    bind->expect_json_only = 0;
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 || !bind->session_token || !bind->session_token[0]) {
        bind->error = ducknng_strdup("ducknng: fetch_query requires non-empty url, session_id, and session_token");
    } else if (!bind->method_name) {
        bind->error = ducknng_strdup("ducknng: out of memory preparing fetch_query call");
    }
    ducknng_session_bind_add_control_columns(info);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "payload", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "end_of_stream", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_session_result_bind_data);
    duckdb_bind_set_cardinality(info, 1, true);
}

static void ducknng_session_result_init(duckdb_init_info info) {
    ducknng_session_result_bind_data *bind = (ducknng_session_result_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_malloc(sizeof(*init));
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    char *json = NULL;
    char *errmsg = NULL;
    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing session bind data");
        return;
    }
    if (!bind->executed && !bind->error) {
        if (bind->method_name && strcmp(bind->method_name, "query_open") == 0) {
            if (ducknng_query_open_request_to_ipc(bind->sql, bind->batch_rows, bind->batch_bytes, &payload, &payload_len, &errmsg) != 0) {
                bind->error = errmsg ? errmsg : ducknng_strdup("ducknng: failed to encode query_open payload");
            } else {
                ducknng_session_bind_common(bind, bind->ctx, bind->url, bind->method_name, payload, payload_len, bind->tls_config_id, 0, bind->expect_json_only);
                if (bind->ok && (bind->session_id == 0 || !bind->session_token || !bind->session_token[0])) {
                    bind->ok = false;
                    if (!bind->error) bind->error = ducknng_strdup("ducknng: query_open reply did not include session_id and session_token");
                }
            }
        } else {
            json = ducknng_session_request_json(bind->session_id, bind->session_token, bind->batch_rows, bind->batch_bytes);
            if (!json) {
                bind->error = ducknng_strdup("ducknng: failed to build session request payload");
            } else {
                ducknng_session_bind_common(bind, bind->ctx, bind->url, bind->method_name, (const uint8_t *)json, strlen(json), bind->tls_config_id, bind->session_id, bind->expect_json_only);
            }
        }
        if (payload) duckdb_free(payload);
        if (json) duckdb_free(json);
        bind->executed = 1;
    }
    if (bind->error) bind->ok = false;
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_single_row_init_data);
}

static void ducknng_session_control_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_session_result_bind_data *bind = (ducknng_session_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    uint64_t *session_ids;
    uint64_t *idle_timeout_ms;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    session_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    idle_timeout_ms = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, bind->error, (idx_t)strlen(bind->error));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    session_ids[0] = bind->session_id;
    if (bind->session_token) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), 0, bind->session_token, (idx_t)strlen(bind->session_token));
    else set_null(duckdb_data_chunk_get_vector(output, 3), 0);
    if (bind->state) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 4), 0, bind->state, (idx_t)strlen(bind->state));
    else set_null(duckdb_data_chunk_get_vector(output, 4), 0);
    if (bind->next_method) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), 0, bind->next_method, (idx_t)strlen(bind->next_method));
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    if (bind->control_json) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), 0, bind->control_json, (idx_t)strlen(bind->control_json));
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (bind->has_idle_timeout_ms) idle_timeout_ms[0] = bind->idle_timeout_ms;
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_fetch_query_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_single_row_init_data *init = (ducknng_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_session_result_bind_data *bind = (ducknng_session_result_bind_data *)duckdb_function_get_bind_data(info);
    bool *ok_data;
    uint64_t *session_ids;
    uint64_t *idle_timeout_ms;
    bool *end_of_stream;
    if (!init || !bind || init->emitted) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    ok_data = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    session_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    idle_timeout_ms = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    end_of_stream = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 9));
    ok_data[0] = bind->ok;
    if (bind->error) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, bind->error, (idx_t)strlen(bind->error));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    session_ids[0] = bind->session_id;
    if (bind->session_token) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), 0, bind->session_token, (idx_t)strlen(bind->session_token));
    else set_null(duckdb_data_chunk_get_vector(output, 3), 0);
    if (bind->state) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 4), 0, bind->state, (idx_t)strlen(bind->state));
    else set_null(duckdb_data_chunk_get_vector(output, 4), 0);
    if (bind->next_method) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), 0, bind->next_method, (idx_t)strlen(bind->next_method));
    else set_null(duckdb_data_chunk_get_vector(output, 5), 0);
    if (bind->control_json) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), 0, bind->control_json, (idx_t)strlen(bind->control_json));
    else set_null(duckdb_data_chunk_get_vector(output, 6), 0);
    if (bind->has_idle_timeout_ms) idle_timeout_ms[0] = bind->idle_timeout_ms;
    else set_null(duckdb_data_chunk_get_vector(output, 7), 0);
    if (bind->payload) assign_blob(duckdb_data_chunk_get_vector(output, 8), 0, bind->payload, bind->payload_len);
    else set_null(duckdb_data_chunk_get_vector(output, 8), 0);
    end_of_stream[0] = bind->end_of_stream;
    duckdb_data_chunk_set_size(output, 1);
    init->emitted = 1;
}

static void ducknng_fetch_query_table_bind(duckdb_bind_info info) {
    ducknng_fetch_query_table_bind_data *bind;
    ducknng_session_result_bind_data resp;
    char *json = NULL;
    duckdb_value url_val;
    duckdb_value session_val;
    duckdb_value token_val;
    duckdb_value batch_rows_val;
    duckdb_value batch_bytes_val;
    duckdb_value tls_val;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    bind = (ducknng_fetch_query_table_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    memset(&resp, 0, sizeof(resp));
    bind->ctx = ctx;
    url_val = duckdb_bind_get_parameter(info, 0);
    session_val = duckdb_bind_get_parameter(info, 1);
    token_val = duckdb_bind_get_parameter(info, 2);
    batch_rows_val = duckdb_bind_get_parameter(info, 3);
    batch_bytes_val = duckdb_bind_get_parameter(info, 4);
    tls_val = duckdb_bind_get_parameter(info, 5);
    bind->url = duckdb_get_varchar(url_val);
    bind->session_id = (uint64_t)duckdb_get_uint64(session_val);
    bind->session_token = duckdb_get_varchar(token_val);
    bind->batch_rows = (uint64_t)duckdb_get_uint64(batch_rows_val);
    bind->batch_bytes = (uint64_t)duckdb_get_uint64(batch_bytes_val);
    bind->tls_config_id = (uint64_t)duckdb_get_uint64(tls_val);
    duckdb_destroy_value(&url_val);
    duckdb_destroy_value(&session_val);
    duckdb_destroy_value(&token_val);
    duckdb_destroy_value(&batch_rows_val);
    duckdb_destroy_value(&batch_bytes_val);
    duckdb_destroy_value(&tls_val);
    if (!bind->url || !bind->url[0] || bind->session_id == 0 ||
        !bind->session_token || !bind->session_token[0]) {
        destroy_fetch_query_table_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: fetch_query_table requires non-empty url, session_id, and session_token");
        return;
    }
    json = ducknng_session_request_json(bind->session_id, bind->session_token,
        bind->batch_rows, bind->batch_bytes);
    if (!json) {
        destroy_fetch_query_table_bind_data(bind);
        duckdb_bind_set_error(info, "ducknng: failed to build fetch_query_table request payload");
        return;
    }
    if (ducknng_fetch_query_table_cache_copy_response(bind, &resp) != 0) {
        ducknng_session_bind_common(&resp, bind->ctx, bind->url, "fetch",
            (const uint8_t *)json, strlen(json), bind->tls_config_id, bind->session_id, 0);
        if (resp.ok && resp.payload &&
            ducknng_fetch_query_table_cache_store(bind, &resp) != 0) {
            resp.ok = false;
            if (!resp.error) resp.error = ducknng_strdup("ducknng: out of memory caching fetch_query_table payload");
        }
    }
    duckdb_free(json);
    json = NULL;
    if (ducknng_fetch_query_table_result_set(bind, &resp, info) != 0) {
        ducknng_session_result_bind_data_reset(&resp);
        destroy_fetch_query_table_bind_data(bind);
        return;
    }
    ducknng_session_result_bind_data_reset(&resp);
    duckdb_bind_set_bind_data(info, bind, destroy_fetch_query_table_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_fetch_query_table_init(duckdb_init_info info) {
    ducknng_fetch_query_table_bind_data *bind =
        (ducknng_fetch_query_table_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_fetch_query_table_init_data *init =
        (ducknng_fetch_query_table_init_data *)duckdb_malloc(sizeof(*init));
    if (!bind) {
        duckdb_init_set_error(info, "ducknng: missing fetch_query_table bind data");
        return;
    }
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    if (bind->rebind_cache_generation != 0 &&
        ducknng_fetch_query_table_cache.generation == bind->rebind_cache_generation) {
        ducknng_fetch_query_table_cache_reset();
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_fetch_query_table_init_data);
}

static void ducknng_fetch_query_table_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_fetch_query_table_init_data *init =
        (ducknng_fetch_query_table_init_data *)duckdb_function_get_init_data(info);
    ducknng_fetch_query_table_bind_data *bind;
    char *errmsg = NULL;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    if (ducknng_sql_arrow_scan_table(output, &bind->schema, &bind->array,
            bind->row_count, &init->offset, &errmsg) != 0) {
        duckdb_function_set_error(info, errmsg ? errmsg : "ducknng: failed to decode fetch_query_table Arrow payload");
        if (errmsg) duckdb_free(errmsg);
    }
}


static int register_open_query_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 5, param_types, ducknng_open_query_bind,
        ducknng_session_result_init, ducknng_session_control_scan);
}

static int register_session_control_table_named(duckdb_connection con, ducknng_sql_context *ctx,
    const char *name, duckdb_table_function_bind_t bind_fn) {
    duckdb_type param_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 4, param_types, bind_fn,
        ducknng_session_result_init, ducknng_session_control_scan);
}

static int register_fetch_query_table_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 6, param_types, ducknng_fetch_query_bind,
        ducknng_session_result_init, ducknng_fetch_query_scan);
}

static int register_fetch_query_table_rows_named(duckdb_connection con, ducknng_sql_context *ctx, const char *name) {
    duckdb_type param_types[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR,
        DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    return DUCKNNG_REGISTER_TABLE(con, name, ctx, 6, param_types,
        ducknng_fetch_query_table_bind, ducknng_fetch_query_table_init,
        ducknng_fetch_query_table_scan);
}



int ducknng_register_sql_session(duckdb_connection con, ducknng_sql_context *ctx) {
    if (!register_open_query_table_named(con, ctx, "ducknng_open_query")) return 0;
    if (!register_fetch_query_table_named(con, ctx, "ducknng_fetch_query")) return 0;
    if (!register_fetch_query_table_rows_named(con, ctx, "ducknng_fetch_query_table")) return 0;
    if (!register_session_control_table_named(con, ctx, "ducknng_close_query", ducknng_close_query_bind)) return 0;
    if (!register_session_control_table_named(con, ctx, "ducknng_cancel_query", ducknng_cancel_query_bind)) return 0;
    return 1;
}
