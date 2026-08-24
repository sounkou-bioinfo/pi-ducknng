#pragma once

#include "ducknng_http_compat.h"
#include <stddef.h>
#include <stdint.h>

#define DUCKNNG_HTTP_STREAM_READ_BUFFER_SIZE 8192
#define DUCKNNG_HTTP_STREAM_LINE_MAX 8192
#define DUCKNNG_HTTP_STREAM_MAX_RECV_BYTES (64U * 1024U * 1024U)

typedef enum ducknng_http_stream_action {
    DUCKNNG_HTTP_STREAM_ACTION_NONE = 0,
    DUCKNNG_HTTP_STREAM_ACTION_CONNECT = 1,
    DUCKNNG_HTTP_STREAM_ACTION_WRITE_REQUEST = 2,
    DUCKNNG_HTTP_STREAM_ACTION_READ_HEADERS = 3,
    DUCKNNG_HTTP_STREAM_ACTION_READ_BODY = 4
} ducknng_http_stream_action;

typedef enum ducknng_http_stream_open_phase {
    DUCKNNG_HTTP_STREAM_OPEN_CONNECT = 1,
    DUCKNNG_HTTP_STREAM_OPEN_WRITE_REQUEST = 2,
    DUCKNNG_HTTP_STREAM_OPEN_READ_HEADERS = 3,
    DUCKNNG_HTTP_STREAM_OPEN_DONE = 4
} ducknng_http_stream_open_phase;

typedef enum ducknng_http_stream_body_mode {
    DUCKNNG_HTTP_STREAM_BODY_NONE = 0,
    DUCKNNG_HTTP_STREAM_BODY_CONTENT_LENGTH = 1,
    DUCKNNG_HTTP_STREAM_BODY_CHUNKED = 2,
    DUCKNNG_HTTP_STREAM_BODY_UNTIL_CLOSE = 3
} ducknng_http_stream_body_mode;

typedef enum ducknng_http_stream_chunk_state {
    DUCKNNG_HTTP_STREAM_CHUNK_SIZE = 1,
    DUCKNNG_HTTP_STREAM_CHUNK_DATA = 2,
    DUCKNNG_HTTP_STREAM_CHUNK_DATA_CRLF = 3,
    DUCKNNG_HTTP_STREAM_CHUNK_TRAILERS = 4
} ducknng_http_stream_chunk_state;

typedef struct ducknng_http_client_stream {
    uint64_t stream_id;
    uint64_t recv_aio_id;
    uint32_t refcount;
    nng_url *url;
    nng_http_client *client;
    nng_http_req *req;
    nng_http_res *res;
    nng_http_conn *conn;
    uint16_t status;
    char *headers_json;
    uint64_t content_remaining;
    uint64_t chunk_remaining;
    size_t raw_pos;
    size_t raw_len;
    size_t line_len;
    unsigned data_crlf_pos;
    int line_saw_cr;
    int open_phase;
    int body_mode;
    int chunk_state;
    int open;
    int eof;
    int failed;
    int closing;
    uint8_t raw_buf[DUCKNNG_HTTP_STREAM_READ_BUFFER_SIZE];
    char line_buf[DUCKNNG_HTTP_STREAM_LINE_MAX + 1];
} ducknng_http_client_stream;

int ducknng_http_client_stream_prepare(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, size_t body_len,
    const ducknng_tls_opts *tls_opts, ducknng_http_client_stream **out_stream,
    char **errmsg);

/* Complete one open-phase aio and report the next action.  A return of 1 means
 * response status/headers are ready, 0 means submit *out_action, and -1 is a
 * terminal error. */
int ducknng_http_client_stream_open_advance(ducknng_http_client_stream *stream,
    nng_aio *aio, int aio_result, ducknng_http_stream_action *out_action,
    char **errmsg);

/* Consume buffered response bytes before starting another network read.
 * A return of 1 means one user receive is complete (body bytes or explicit
 * EOF), 0 means submit *out_action, and -1 is a terminal stream error. */
int ducknng_http_client_stream_recv_begin(ducknng_http_client_stream *stream,
    uint8_t *out, size_t out_cap, size_t *out_len, int *out_eof,
    ducknng_http_stream_action *out_action, char **errmsg);

/* Complete one raw connection read, de-frame it, and either finish the user
 * receive or request another raw read. */
int ducknng_http_client_stream_recv_advance(ducknng_http_client_stream *stream,
    int aio_result, size_t aio_count, uint8_t *out, size_t out_cap,
    size_t *out_len, int *out_eof, ducknng_http_stream_action *out_action,
    char **errmsg);

int ducknng_http_client_stream_submit(ducknng_http_client_stream *stream,
    nng_aio *aio, ducknng_http_stream_action action, char **errmsg);
void ducknng_http_client_stream_destroy(ducknng_http_client_stream *stream);
