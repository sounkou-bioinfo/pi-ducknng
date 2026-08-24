#pragma once
#include "ducknng_nng_compat.h"
#include <stddef.h>
#include <stdint.h>
#include <nng/supplemental/http/http.h>

struct ducknng_service;
typedef struct ducknng_http_server_state ducknng_http_server_state;

int ducknng_validate_http_url(const char *url, char **errmsg);
int ducknng_validate_http_server_url(const char *url, const ducknng_tls_opts *tls_opts, char **errmsg);
int ducknng_http_transact(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    uint16_t *out_status, char **out_headers_json, uint8_t **out_body, size_t *out_body_len,
    char **errmsg);
int ducknng_http_transact_aio_prepare(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, const ducknng_tls_opts *tls_opts,
    nng_url **out_url, nng_http_client **out_client, nng_http_req **out_req,
    nng_http_res **out_res, char **errmsg);
int ducknng_http_response_copy(nng_http_res *res, uint16_t *out_status,
    char **out_headers_json, uint8_t **out_body, size_t *out_body_len, char **errmsg);
char *ducknng_http_status_error_message(uint16_t status, const uint8_t *body, size_t body_len);
int ducknng_http_frame_transact_aio_prepare(const char *url, const uint8_t *frame, size_t frame_len,
    const ducknng_tls_opts *tls_opts, nng_url **out_url, nng_http_client **out_client,
    nng_http_req **out_req, nng_http_res **out_res, char **errmsg);
int ducknng_http_frame_transact(const char *url, const uint8_t *frame, size_t frame_len,
    int timeout_ms, const ducknng_tls_opts *tls_opts, uint8_t **out_frame, size_t *out_frame_len,
    char **errmsg);
int ducknng_http_server_start(struct ducknng_service *svc, ducknng_http_server_state **out_state,
    char **out_resolved_url, char **errmsg);
void ducknng_http_server_stop(ducknng_http_server_state *state);
