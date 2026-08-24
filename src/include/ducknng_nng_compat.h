#pragma once
#include <stddef.h>
#include <stdint.h>
#include <nng/nng.h>
#include <nng/protocol/bus0/bus.h>
#include <nng/protocol/pair0/pair.h>
#include <nng/protocol/pair1/pair.h>
#include <nng/protocol/pipeline0/pull.h>
#include <nng/protocol/pipeline0/push.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>
#include <nng/protocol/reqrep0/req.h>
#include <nng/protocol/reqrep0/rep.h>
#include <nng/protocol/survey0/respond.h>
#include <nng/protocol/survey0/survey.h>
#include <nng/supplemental/tls/tls.h>

typedef struct ducknng_tls_opts {
    int enabled;
    char *cert_key_file;
    char *ca_file;
    char *cert_pem;
    char *key_pem;
    char *ca_pem;
    char *password;
    int auth_mode;
    int peer_allowlist_active;
    char **peer_allowlist;
    size_t peer_allowlist_count;
    char *peer_allowlist_json;
} ducknng_tls_opts;

void ducknng_tls_opts_init(ducknng_tls_opts *opts);
int ducknng_tls_opts_copy(ducknng_tls_opts *dst, const ducknng_tls_opts *src);
void ducknng_tls_opts_reset(ducknng_tls_opts *opts);
int ducknng_tls_opts_set_peer_allowlist(ducknng_tls_opts *opts, const char *identities_json, char **errmsg);
int ducknng_tls_opts_peer_allowed(const ducknng_tls_opts *opts, const char *identity);

int ducknng_rep_socket_open(nng_socket *out);
int ducknng_req_socket_open(nng_socket *out);
int ducknng_socket_open_protocol(const char *protocol, nng_socket *out, char **errmsg);
int ducknng_validate_nng_url(const char *url, char **errmsg);
int ducknng_socket_validate_client_url(const char *url, const ducknng_tls_opts *opts, char **errmsg);
int ducknng_socket_set_timeout_ms(nng_socket sock, int send_timeout_ms, int recv_timeout_ms);
int ducknng_socket_dial(nng_socket sock, const char *url);
int ducknng_socket_dial_nonblocking(nng_socket sock, const char *url);
int ducknng_socket_send(nng_socket sock, nng_msg *msg);
int ducknng_socket_recv(nng_socket sock, nng_msg **msg);
int ducknng_socket_subscribe(nng_socket sock, const void *topic, size_t len);
int ducknng_socket_unsubscribe(nng_socket sock, const void *topic, size_t len);
int ducknng_ctx_send(nng_ctx ctx, nng_msg *msg);
int ducknng_ctx_recv(nng_ctx ctx, nng_msg **msg);
char *ducknng_pipe_verified_peer_identity(nng_pipe pipe);
char *ducknng_msg_verified_peer_identity(nng_msg *msg);
int ducknng_req_dial(nng_socket sock, const char *url, int timeout_ms);
int ducknng_req_transact(nng_socket sock, nng_msg *req, nng_msg **resp);
int ducknng_socket_apply_tls(nng_socket sock, const char *url, const ducknng_tls_opts *opts);
int ducknng_listener_create(nng_listener *out, nng_socket sock, const char *url);
int ducknng_listener_validate_startup_url(const char *url, const ducknng_tls_opts *opts, char **errmsg);
int ducknng_listener_set_recvmaxsz(nng_listener lst, size_t bytes);
int ducknng_listener_apply_tls(nng_listener lst, const ducknng_tls_opts *opts);
int ducknng_listener_start(nng_listener lst);
int ducknng_listener_close(nng_listener lst);
char *ducknng_listener_resolve_url(nng_listener lst, const char *url);
int ducknng_socket_close(nng_socket sock);
int ducknng_ctx_open(nng_ctx *out, nng_socket sock);
int ducknng_ctx_close(nng_ctx ctx);
void ducknng_ctx_recv_aio(nng_ctx ctx, nng_aio *aio);
void ducknng_ctx_send_aio(nng_ctx ctx, nng_aio *aio);
void ducknng_socket_recv_aio(nng_socket sock, nng_aio *aio);
void ducknng_socket_send_aio(nng_socket sock, nng_aio *aio);
int ducknng_aio_alloc(nng_aio **out, void (*cb)(void *), void *arg, int timeout_ms);
void ducknng_aio_free(nng_aio *aio);
int ducknng_aio_result(nng_aio *aio);
void ducknng_aio_cancel(nng_aio *aio);
void ducknng_aio_wait(nng_aio *aio);
void ducknng_aio_set_msg(nng_aio *aio, nng_msg *msg);
nng_msg *ducknng_aio_get_msg(nng_aio *aio);
const char *ducknng_nng_strerror(int err);
