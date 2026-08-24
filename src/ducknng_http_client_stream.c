#include "ducknng_http_client_stream.h"
#include "ducknng_util.h"
#include <limits.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_http_stream_header_ends_with_token(const char *value,
    const char *wanted) {
    const char *p = value;
    size_t wanted_len = strlen(wanted);
    int saw_token = 0;
    int last_matches = 0;
    if (!value || !wanted || wanted_len == 0) return 0;
    while (*p) {
        const char *start;
        const char *end;
        size_t i;
        while (*p == ' ' || *p == '\t' || *p == ',') p++;
        start = p;
        while (*p && *p != ',') p++;
        end = p;
        while (end > start && (end[-1] == ' ' || end[-1] == '\t')) end--;
        if (end == start) {
            if (*p == ',') p++;
            continue;
        }
        saw_token = 1;
        last_matches = (size_t)(end - start) == wanted_len;
        for (i = 0; last_matches && i < wanted_len; i++) {
            unsigned char a = (unsigned char)start[i];
            unsigned char b = (unsigned char)wanted[i];
            if (a >= 'A' && a <= 'Z') a = (unsigned char)(a + ('a' - 'A'));
            if (b >= 'A' && b <= 'Z') b = (unsigned char)(b + ('a' - 'A'));
            if (a != b) last_matches = 0;
        }
        if (*p == ',') p++;
    }
    return saw_token && last_matches;
}

static int ducknng_http_stream_parse_content_length(const char *value,
    uint64_t *out, char **errmsg) {
    uint64_t parsed = 0;
    const unsigned char *p = (const unsigned char *)value;
    if (out) *out = 0;
    if (!value || !value[0]) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: empty Content-Length in streaming HTTP response");
        return -1;
    }
    while (*p) {
        unsigned digit;
        if (*p < '0' || *p > '9') {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: invalid Content-Length in streaming HTTP response");
            return -1;
        }
        digit = (unsigned)(*p - '0');
        if (parsed > (UINT64_MAX - digit) / 10) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: Content-Length overflows the streaming HTTP response counter");
            return -1;
        }
        parsed = parsed * 10 + digit;
        p++;
    }
    if (out) *out = parsed;
    return 0;
}

static int ducknng_http_stream_ascii_equal(const char *a, const char *b) {
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

static int ducknng_http_stream_response_has_no_body(
    ducknng_http_client_stream *stream) {
    const char *method;
    if (!stream || !stream->req) return 1;
    method = nng_http_req_get_method(stream->req);
    if (method && ducknng_http_stream_ascii_equal(method, "HEAD")) return 1;
    if (stream->status >= 100 && stream->status < 200) return 1;
    return stream->status == 204 || stream->status == 205 ||
        stream->status == 304;
}

static int ducknng_http_stream_configure_body(
    ducknng_http_client_stream *stream, char **errmsg) {
    const char *transfer_encoding;
    const char *content_length;
    if (!stream || !stream->res) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: missing streaming HTTP response headers");
        return -1;
    }
    stream->status = nng_http_res_get_status(stream->res);
    if (ducknng_http_response_copy(stream->res, NULL,
            &stream->headers_json, NULL, NULL, errmsg) != 0) {
        return -1;
    }
    if (ducknng_http_stream_response_has_no_body(stream)) {
        stream->body_mode = DUCKNNG_HTTP_STREAM_BODY_NONE;
        stream->eof = 1;
        return 0;
    }
    transfer_encoding = nng_http_res_get_header(stream->res,
        "Transfer-Encoding");
    if (transfer_encoding && transfer_encoding[0]) {
        if (!ducknng_http_stream_header_ends_with_token(transfer_encoding,
                "chunked")) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: unsupported Transfer-Encoding in streaming HTTP response");
            return -1;
        }
        stream->body_mode = DUCKNNG_HTTP_STREAM_BODY_CHUNKED;
        stream->chunk_state = DUCKNNG_HTTP_STREAM_CHUNK_SIZE;
        return 0;
    }
    content_length = nng_http_res_get_header(stream->res, "Content-Length");
    if (content_length) {
        if (ducknng_http_stream_parse_content_length(content_length,
                &stream->content_remaining, errmsg) != 0) {
            return -1;
        }
        stream->body_mode = DUCKNNG_HTTP_STREAM_BODY_CONTENT_LENGTH;
        if (stream->content_remaining == 0) stream->eof = 1;
        return 0;
    }
    stream->body_mode = DUCKNNG_HTTP_STREAM_BODY_UNTIL_CLOSE;
    return 0;
}

int ducknng_http_client_stream_prepare(const char *url, const char *method,
    const char *headers_json, const uint8_t *body, size_t body_len,
    const ducknng_tls_opts *tls_opts, ducknng_http_client_stream **out_stream,
    char **errmsg) {
    ducknng_http_client_stream *stream;
    int rv;
    if (out_stream) *out_stream = NULL;
    if (errmsg) *errmsg = NULL;
    if (!out_stream) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: missing streaming HTTP client output");
        return -1;
    }
    stream = (ducknng_http_client_stream *)duckdb_malloc(sizeof(*stream));
    if (!stream) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: out of memory allocating streaming HTTP client");
        return -1;
    }
    memset(stream, 0, sizeof(*stream));
    if (ducknng_http_transact_aio_prepare(url, method, headers_json, body,
            body_len, tls_opts, &stream->url, &stream->client, &stream->req,
            &stream->res, errmsg) != 0) {
        ducknng_http_client_stream_destroy(stream);
        return -1;
    }
    /* A streaming response owns one connection until explicit close.  Asking
     * the peer to close prevents it from treating the connection as reusable
     * after a close-delimited body or early client cancellation. */
    rv = nng_http_req_set_header(stream->req, "Connection", "close");
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg =
            ducknng_strdup(ducknng_nng_strerror(rv));
        ducknng_http_client_stream_destroy(stream);
        return -1;
    }
    stream->open_phase = DUCKNNG_HTTP_STREAM_OPEN_CONNECT;
    *out_stream = stream;
    return 0;
}

int ducknng_http_client_stream_open_advance(ducknng_http_client_stream *stream,
    nng_aio *aio, int aio_result, ducknng_http_stream_action *out_action,
    char **errmsg) {
    if (out_action) *out_action = DUCKNNG_HTTP_STREAM_ACTION_NONE;
    if (errmsg) *errmsg = NULL;
    if (!stream || !aio || !out_action) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: missing streaming HTTP open state");
        return -1;
    }
    if (aio_result != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(aio_result));
        stream->failed = 1;
        return -1;
    }
    switch (stream->open_phase) {
    case DUCKNNG_HTTP_STREAM_OPEN_CONNECT:
        stream->conn = (nng_http_conn *)nng_aio_get_output(aio, 0);
        if (!stream->conn) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: HTTP stream connect returned no connection");
            stream->failed = 1;
            return -1;
        }
        stream->open_phase = DUCKNNG_HTTP_STREAM_OPEN_WRITE_REQUEST;
        *out_action = DUCKNNG_HTTP_STREAM_ACTION_WRITE_REQUEST;
        return 0;
    case DUCKNNG_HTTP_STREAM_OPEN_WRITE_REQUEST:
        stream->open_phase = DUCKNNG_HTTP_STREAM_OPEN_READ_HEADERS;
        *out_action = DUCKNNG_HTTP_STREAM_ACTION_READ_HEADERS;
        return 0;
    case DUCKNNG_HTTP_STREAM_OPEN_READ_HEADERS:
        if (ducknng_http_stream_configure_body(stream, errmsg) != 0) {
            stream->failed = 1;
            return -1;
        }
        stream->open_phase = DUCKNNG_HTTP_STREAM_OPEN_DONE;
        stream->open = 1;
        return 1;
    default:
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: invalid streaming HTTP open phase");
        stream->failed = 1;
        return -1;
    }
}

static int ducknng_http_stream_read_line(ducknng_http_client_stream *stream,
    int *out_complete, char **errmsg) {
    if (out_complete) *out_complete = 0;
    while (stream->raw_pos < stream->raw_len) {
        unsigned char c = stream->raw_buf[stream->raw_pos++];
        if (stream->line_saw_cr) {
            stream->line_saw_cr = 0;
            if (c != '\n') {
                if (errmsg) *errmsg = ducknng_strdup(
                    "ducknng: malformed chunked HTTP line ending");
                return -1;
            }
            stream->line_buf[stream->line_len] = '\0';
            if (out_complete) *out_complete = 1;
            return 0;
        }
        if (c == '\r') {
            stream->line_saw_cr = 1;
            continue;
        }
        if (c == '\n') {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: malformed chunked HTTP line ending");
            return -1;
        }
        if ((c < 0x20 && c != '\t') || c == 0x7f) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: control character in chunked HTTP framing");
            return -1;
        }
        if (stream->line_len >= DUCKNNG_HTTP_STREAM_LINE_MAX) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: chunked HTTP size or trailer line is too long");
            return -1;
        }
        stream->line_buf[stream->line_len++] = (char)c;
    }
    return 0;
}

static int ducknng_http_stream_parse_chunk_size(
    ducknng_http_client_stream *stream, char **errmsg) {
    const unsigned char *p = (const unsigned char *)stream->line_buf;
    uint64_t size = 0;
    size_t digits = 0;
    while (*p && *p != ';') {
        unsigned digit;
        if (*p >= '0' && *p <= '9') digit = (unsigned)(*p - '0');
        else if (*p >= 'a' && *p <= 'f') digit = (unsigned)(*p - 'a' + 10);
        else if (*p >= 'A' && *p <= 'F') digit = (unsigned)(*p - 'A' + 10);
        else {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: invalid chunk size in streaming HTTP response");
            return -1;
        }
        if (size > (UINT64_MAX - digit) / 16) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: chunk size overflows the streaming HTTP response counter");
            return -1;
        }
        size = size * 16 + digit;
        digits++;
        p++;
    }
    if (digits == 0) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: empty chunk size in streaming HTTP response");
        return -1;
    }
    stream->chunk_remaining = size;
    stream->line_len = 0;
    if (size == 0) {
        stream->chunk_state = DUCKNNG_HTTP_STREAM_CHUNK_TRAILERS;
    } else {
        stream->chunk_state = DUCKNNG_HTTP_STREAM_CHUNK_DATA;
    }
    return 0;
}

static size_t ducknng_http_stream_min3(size_t a, size_t b, uint64_t c) {
    size_t n = a < b ? a : b;
    if (c < (uint64_t)n) n = (size_t)c;
    return n;
}

static int ducknng_http_stream_process(ducknng_http_client_stream *stream,
    uint8_t *out, size_t out_cap, size_t *out_len, int *out_eof,
    char **errmsg) {
    if (out_eof) *out_eof = 0;
    if (!stream || !out_len || !out_eof || (!out && out_cap > 0)) return -1;
    if (stream->eof) {
        *out_eof = 1;
        return 1;
    }
    if (stream->body_mode == DUCKNNG_HTTP_STREAM_BODY_CONTENT_LENGTH) {
        size_t available = stream->raw_len - stream->raw_pos;
        size_t room = out_cap - *out_len;
        size_t take = ducknng_http_stream_min3(available, room,
            stream->content_remaining);
        if (take > 0) {
            memcpy(out + *out_len, stream->raw_buf + stream->raw_pos, take);
            stream->raw_pos += take;
            stream->content_remaining -= (uint64_t)take;
            *out_len += take;
        }
        if (stream->content_remaining == 0) stream->eof = 1;
        if (*out_len > 0) return 1;
        if (stream->eof) {
            *out_eof = 1;
            return 1;
        }
        return 0;
    }
    if (stream->body_mode == DUCKNNG_HTTP_STREAM_BODY_UNTIL_CLOSE) {
        size_t available = stream->raw_len - stream->raw_pos;
        size_t room = out_cap - *out_len;
        size_t take = available < room ? available : room;
        if (take > 0) {
            memcpy(out + *out_len, stream->raw_buf + stream->raw_pos, take);
            stream->raw_pos += take;
            *out_len += take;
            return 1;
        }
        return 0;
    }
    if (stream->body_mode != DUCKNNG_HTTP_STREAM_BODY_CHUNKED) {
        stream->eof = 1;
        *out_eof = 1;
        return 1;
    }
    for (;;) {
        if (stream->chunk_state == DUCKNNG_HTTP_STREAM_CHUNK_SIZE) {
            int complete = 0;
            if (ducknng_http_stream_read_line(stream, &complete, errmsg) != 0)
                return -1;
            if (!complete) return 0;
            if (ducknng_http_stream_parse_chunk_size(stream, errmsg) != 0)
                return -1;
            continue;
        }
        if (stream->chunk_state == DUCKNNG_HTTP_STREAM_CHUNK_DATA) {
            size_t available = stream->raw_len - stream->raw_pos;
            size_t room = out_cap - *out_len;
            size_t take = ducknng_http_stream_min3(available, room,
                stream->chunk_remaining);
            if (take > 0) {
                memcpy(out + *out_len, stream->raw_buf + stream->raw_pos,
                    take);
                stream->raw_pos += take;
                stream->chunk_remaining -= (uint64_t)take;
                *out_len += take;
            }
            if (stream->chunk_remaining == 0) {
                stream->chunk_state = DUCKNNG_HTTP_STREAM_CHUNK_DATA_CRLF;
                stream->data_crlf_pos = 0;
            }
            if (*out_len > 0) return 1;
            if (stream->raw_pos == stream->raw_len) return 0;
            continue;
        }
        if (stream->chunk_state == DUCKNNG_HTTP_STREAM_CHUNK_DATA_CRLF) {
            static const unsigned char crlf[2] = {'\r', '\n'};
            while (stream->data_crlf_pos < 2 &&
                    stream->raw_pos < stream->raw_len) {
                if (stream->raw_buf[stream->raw_pos++] !=
                        crlf[stream->data_crlf_pos++]) {
                    if (errmsg) *errmsg = ducknng_strdup(
                        "ducknng: malformed chunk data terminator in streaming HTTP response");
                    return -1;
                }
            }
            if (stream->data_crlf_pos < 2) return 0;
            stream->chunk_state = DUCKNNG_HTTP_STREAM_CHUNK_SIZE;
            continue;
        }
        if (stream->chunk_state == DUCKNNG_HTTP_STREAM_CHUNK_TRAILERS) {
            int complete = 0;
            if (ducknng_http_stream_read_line(stream, &complete, errmsg) != 0)
                return -1;
            if (!complete) return 0;
            if (stream->line_len == 0) {
                stream->eof = 1;
                *out_eof = 1;
                return 1;
            }
            stream->line_len = 0;
            continue;
        }
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: invalid chunked HTTP decoder state");
        return -1;
    }
}

int ducknng_http_client_stream_recv_begin(ducknng_http_client_stream *stream,
    uint8_t *out, size_t out_cap, size_t *out_len, int *out_eof,
    ducknng_http_stream_action *out_action, char **errmsg) {
    int rc;
    if (out_len) *out_len = 0;
    if (out_eof) *out_eof = 0;
    if (out_action) *out_action = DUCKNNG_HTTP_STREAM_ACTION_NONE;
    if (errmsg) *errmsg = NULL;
    if (!stream || !out_len || !out_eof || !out_action || out_cap == 0 ||
            out_cap > DUCKNNG_HTTP_STREAM_MAX_RECV_BYTES) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: HTTP stream receive max_bytes must be between 1 and 67108864");
        return -1;
    }
    if (!stream->open || stream->failed || stream->closing) {
        if (errmsg) *errmsg = ducknng_strdup(
            stream->closing ? "ducknng: HTTP stream is closing" :
            "ducknng: HTTP stream is not readable");
        return -1;
    }
    rc = ducknng_http_stream_process(stream, out, out_cap, out_len,
        out_eof, errmsg);
    if (rc != 0) {
        if (rc < 0) stream->failed = 1;
        return rc;
    }
    *out_action = DUCKNNG_HTTP_STREAM_ACTION_READ_BODY;
    return 0;
}

int ducknng_http_client_stream_recv_advance(ducknng_http_client_stream *stream,
    int aio_result, size_t aio_count, uint8_t *out, size_t out_cap,
    size_t *out_len, int *out_eof, ducknng_http_stream_action *out_action,
    char **errmsg) {
    int rc;
    if (out_action) *out_action = DUCKNNG_HTTP_STREAM_ACTION_NONE;
    if (errmsg) *errmsg = NULL;
    if (!stream || !out || out_cap == 0 ||
            out_cap > DUCKNNG_HTTP_STREAM_MAX_RECV_BYTES ||
            !out_len || !out_eof || !out_action) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: missing or invalid streaming HTTP receive state");
        return -1;
    }
    if (aio_result != 0) {
        if (stream->body_mode == DUCKNNG_HTTP_STREAM_BODY_UNTIL_CLOSE &&
                !stream->closing &&
                (aio_result == NNG_ECONNSHUT || aio_result == NNG_ECLOSED)) {
            stream->eof = 1;
            *out_eof = 1;
            return 1;
        }
        stream->failed = 1;
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(aio_result));
        return -1;
    }
    if (aio_count == 0 || aio_count > sizeof(stream->raw_buf)) {
        stream->failed = 1;
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: streaming HTTP connection returned an empty or oversized read");
        return -1;
    }
    stream->raw_pos = 0;
    stream->raw_len = aio_count;
    rc = ducknng_http_stream_process(stream, out, out_cap, out_len,
        out_eof, errmsg);
    if (rc != 0) {
        if (rc < 0) stream->failed = 1;
        return rc;
    }
    *out_action = DUCKNNG_HTTP_STREAM_ACTION_READ_BODY;
    return 0;
}

int ducknng_http_client_stream_submit(ducknng_http_client_stream *stream,
    nng_aio *aio, ducknng_http_stream_action action, char **errmsg) {
    nng_iov iov;
    int rv;
    if (errmsg) *errmsg = NULL;
    if (!stream || !aio) {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: missing streaming HTTP submission state");
        return -1;
    }
    switch (action) {
    case DUCKNNG_HTTP_STREAM_ACTION_CONNECT:
        nng_http_client_connect(stream->client, aio);
        return 0;
    case DUCKNNG_HTTP_STREAM_ACTION_WRITE_REQUEST:
        nng_http_conn_write_req(stream->conn, stream->req, aio);
        return 0;
    case DUCKNNG_HTTP_STREAM_ACTION_READ_HEADERS:
        nng_http_conn_read_res(stream->conn, stream->res, aio);
        return 0;
    case DUCKNNG_HTTP_STREAM_ACTION_READ_BODY:
        if (!stream->conn || stream->raw_pos != stream->raw_len) {
            if (errmsg) *errmsg = ducknng_strdup(
                "ducknng: invalid streaming HTTP raw-read state");
            return -1;
        }
        stream->raw_pos = 0;
        stream->raw_len = 0;
        iov.iov_buf = stream->raw_buf;
        iov.iov_len = sizeof(stream->raw_buf);
        rv = nng_aio_set_iov(aio, 1, &iov);
        if (rv != 0) {
            if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
            return -1;
        }
        nng_http_conn_read(stream->conn, aio);
        return 0;
    default:
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: invalid streaming HTTP submission action");
        return -1;
    }
}

void ducknng_http_client_stream_destroy(ducknng_http_client_stream *stream) {
    if (!stream) return;
    if (stream->conn) nng_http_conn_close(stream->conn);
    if (stream->res) nng_http_res_free(stream->res);
    if (stream->req) nng_http_req_free(stream->req);
    if (stream->client) nng_http_client_free(stream->client);
    if (stream->url) nng_url_free(stream->url);
    if (stream->headers_json) duckdb_free(stream->headers_json);
    duckdb_free(stream);
}
