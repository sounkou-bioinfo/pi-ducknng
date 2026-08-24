#include "ducknng_runtime.h"
#include "ducknng_manifest.h"
#include "ducknng_nng_compat.h"
#include "ducknng_util.h"
#include <stdatomic.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

#define DUCKNNG_EXECUTION_POOL_SIZE 8

typedef struct {
    duckdb_database db;
    ducknng_runtime *rt;
} ducknng_registry_entry;

static ducknng_registry_entry *g_entries = NULL;
static size_t g_entry_count = 0;
static size_t g_entry_cap = 0;
static atomic_flag g_registry_lock = ATOMIC_FLAG_INIT;
static _Thread_local ducknng_runtime *g_thread_request_runtime = NULL;
static _Thread_local ducknng_service *g_thread_request_service = NULL;
static _Thread_local ducknng_runtime *g_thread_http_request_runtime = NULL;
static _Thread_local const ducknng_http_request_context *g_thread_http_request_context = NULL;
static _Thread_local ducknng_runtime *g_thread_authorizer_runtime = NULL;
static _Thread_local const ducknng_authorizer_context *g_thread_authorizer_context = NULL;

static void reg_lock(void) { while (atomic_flag_test_and_set_explicit(&g_registry_lock, memory_order_acquire)) {} }
static void reg_unlock(void) { atomic_flag_clear_explicit(&g_registry_lock, memory_order_release); }
static long reg_find(duckdb_database db) {
    size_t i;
    for (i = 0; i < g_entry_count; i++) if (g_entries[i].db == db) return (long)i;
    return -1;
}
static int reg_reserve(size_t want) {
    ducknng_registry_entry *new_entries;
    size_t new_cap = g_entry_cap ? g_entry_cap * 2 : 4;
    if (g_entry_cap >= want) return 1;
    while (new_cap < want) new_cap *= 2;
    new_entries = (ducknng_registry_entry *)duckdb_malloc(sizeof(*new_entries) * new_cap);
    if (!new_entries) return 0;
    memset(new_entries, 0, sizeof(*new_entries) * new_cap);
    if (g_entries && g_entry_count) memcpy(new_entries, g_entries, sizeof(*new_entries) * g_entry_count);
    if (g_entries) duckdb_free(g_entries);
    g_entries = new_entries;
    g_entry_cap = new_cap;
    return 1;
}
static void reg_remove(duckdb_database db) {
    long idx;
    if (!db) return;
    idx = reg_find(db);
    if (idx < 0) return;
    for (; (size_t)idx + 1 < g_entry_count; idx++) g_entries[idx] = g_entries[idx + 1];
    g_entry_count--;
    if (g_entry_count == 0 && g_entries) {
        duckdb_free(g_entries);
        g_entries = NULL;
        g_entry_cap = 0;
    }
}

int ducknng_runtime_init(duckdb_connection connection, duckdb_extension_info info,
    struct duckdb_extension_access *access, ducknng_runtime **out_rt, int *out_created) {
    duckdb_database *db_ptr = NULL;
    duckdb_database db = NULL;
    ducknng_runtime *rt = NULL;
    long idx;
    char *errmsg = NULL;
    if (!access || !info || !out_rt) return 0;
    if (out_created) *out_created = 0;
    db_ptr = access->get_database(info);
    if (!db_ptr || !*db_ptr) {
        access->set_error(info, "ducknng: missing database handle");
        return 0;
    }
    db = *db_ptr;
    reg_lock();
    idx = reg_find(db);
    if (idx >= 0) {
        *out_rt = g_entries[idx].rt;
        reg_unlock();
        return 1;
    }
    if (!reg_reserve(g_entry_count + 1)) {
        reg_unlock();
        access->set_error(info, "ducknng: failed to grow runtime registry");
        return 0;
    }
    rt = (ducknng_runtime *)duckdb_malloc(sizeof(*rt));
    if (!rt) {
        reg_unlock();
        access->set_error(info, "ducknng: out of memory");
        return 0;
    }
    memset(rt, 0, sizeof(*rt));
    rt->db = db;
    rt->init_con = connection;
    rt->next_service_id = 1;
    rt->next_client_socket_id = 1;
    rt->next_client_aio_id = 1;
    rt->next_tls_config_id = 1;
    rt->next_http_profile_version = 1;
    atomic_store_explicit(&rt->current_request_service_ptr, (uintptr_t)0, memory_order_release);
    if (ducknng_mutex_init(&rt->mu) != 0) {
        duckdb_free(rt);
        reg_unlock();
        access->set_error(info, "ducknng: failed to initialize runtime mutex");
        return 0;
    }
    if (ducknng_mutex_init(&rt->init_con_mu) != 0) {
        ducknng_mutex_destroy(&rt->mu);
        duckdb_free(rt);
        reg_unlock();
        access->set_error(info, "ducknng: failed to initialize runtime init connection mutex");
        return 0;
    }
    rt->init_con_mu_initialized = 1;
    if (ducknng_mutex_init(&rt->codec_con_mu) != 0) {
        ducknng_mutex_destroy(&rt->init_con_mu);
        ducknng_mutex_destroy(&rt->mu);
        duckdb_free(rt);
        reg_unlock();
        access->set_error(info, "ducknng: failed to initialize runtime codec connection mutex");
        return 0;
    }
    rt->codec_con_mu_initialized = 1;
    if (duckdb_connect(db, &rt->codec_con) == DuckDBError || !rt->codec_con) {
        ducknng_mutex_destroy(&rt->codec_con_mu);
        ducknng_mutex_destroy(&rt->init_con_mu);
        ducknng_mutex_destroy(&rt->mu);
        duckdb_free(rt);
        reg_unlock();
        access->set_error(info, "ducknng: failed to open codec connection");
        return 0;
    }
    if (ducknng_mutex_init(&rt->execution_pool_mu) != 0) {
        ducknng_mutex_destroy(&rt->init_con_mu);
        ducknng_mutex_destroy(&rt->mu);
        duckdb_free(rt);
        reg_unlock();
        access->set_error(info, "ducknng: failed to initialize runtime execution pool mutex");
        return 0;
    }
    rt->execution_pool_mu_initialized = 1;
    rt->execution_pool = (duckdb_connection *)duckdb_malloc(sizeof(*rt->execution_pool) * DUCKNNG_EXECUTION_POOL_SIZE);
    rt->execution_pool_busy = (int *)duckdb_malloc(sizeof(*rt->execution_pool_busy) * DUCKNNG_EXECUTION_POOL_SIZE);
    if (rt->execution_pool && rt->execution_pool_busy) {
        size_t pi;
        memset(rt->execution_pool, 0, sizeof(*rt->execution_pool) * DUCKNNG_EXECUTION_POOL_SIZE);
        memset(rt->execution_pool_busy, 0, sizeof(*rt->execution_pool_busy) * DUCKNNG_EXECUTION_POOL_SIZE);
        for (pi = 0; pi < DUCKNNG_EXECUTION_POOL_SIZE; pi++) {
            duckdb_connection pool_con = NULL;
            if (duckdb_connect(db, &pool_con) == DuckDBError || !pool_con) break;
            rt->execution_pool[rt->execution_pool_count++] = pool_con;
        }
    }
    if (ducknng_cond_init(&rt->aio_cv) == 0) rt->aio_cv_initialized = 1;
    ducknng_log_ring_init(&rt->log_ring);
    ducknng_method_registry_init(&rt->registry);
    if (!ducknng_register_builtin_methods(rt, &errmsg)) {
        ducknng_method_registry_destroy(&rt->registry);
        if (rt->execution_pool) {
            size_t pi;
            for (pi = 0; pi < rt->execution_pool_count; pi++) {
                if (rt->execution_pool[pi]) duckdb_disconnect(&rt->execution_pool[pi]);
            }
            duckdb_free(rt->execution_pool);
        }
        if (rt->execution_pool_busy) duckdb_free(rt->execution_pool_busy);
        if (rt->aio_cv_initialized) ducknng_cond_destroy(&rt->aio_cv);
        if (rt->execution_pool_mu_initialized) ducknng_mutex_destroy(&rt->execution_pool_mu);
        if (rt->codec_con) duckdb_disconnect(&rt->codec_con);
        if (rt->codec_con_mu_initialized) ducknng_mutex_destroy(&rt->codec_con_mu);
        if (rt->init_con_mu_initialized) ducknng_mutex_destroy(&rt->init_con_mu);
        ducknng_mutex_destroy(&rt->mu);
        duckdb_free(rt);
        reg_unlock();
        access->set_error(info, errmsg ? errmsg : "ducknng: failed to register builtin methods");
        if (errmsg) duckdb_free(errmsg);
        return 0;
    }
    g_entries[g_entry_count].db = db;
    g_entries[g_entry_count].rt = rt;
    g_entry_count++;
    *out_rt = rt;
    if (out_created) *out_created = 1;
    reg_unlock();
    return 1;
}

void ducknng_runtime_release_client_socket(ducknng_client_socket *sock) {
    if (!sock || !sock->mu_initialized) return;
    ducknng_mutex_lock(&sock->mu);
    if (sock->refcount > 0) sock->refcount--;
    if (sock->closing && sock->refcount == 0 && sock->cv_initialized) {
        ducknng_cond_broadcast(&sock->cv);
    }
    ducknng_mutex_unlock(&sock->mu);
}

void ducknng_client_socket_destroy(ducknng_client_socket *sock) {
    if (!sock) return;
    if (sock->mu_initialized) {
        ducknng_mutex_lock(&sock->mu);
        sock->closing = 1;
        while (sock->refcount > 0 && sock->cv_initialized) {
            ducknng_cond_wait(&sock->cv, &sock->mu);
        }
        ducknng_mutex_unlock(&sock->mu);
    }
    if (sock->has_listener) ducknng_listener_close(sock->listener);
    if (sock->has_ctx) ducknng_ctx_close(sock->ctx);
    if (sock->open) ducknng_socket_close(sock->sock);
    if (sock->protocol) duckdb_free(sock->protocol);
    if (sock->url) duckdb_free(sock->url);
    if (sock->listen_url) duckdb_free(sock->listen_url);
    if (sock->pending_request) duckdb_free(sock->pending_request);
    if (sock->pending_reply) duckdb_free(sock->pending_reply);
    if (sock->cv_initialized) ducknng_cond_destroy(&sock->cv);
    if (sock->mu_initialized) ducknng_mutex_destroy(&sock->mu);
    duckdb_free(sock);
}

void ducknng_client_aio_destroy(ducknng_client_aio *aio) {
    if (!aio) return;
    if (aio->aio) {
        if (aio->state == DUCKNNG_CLIENT_AIO_PENDING) {
            ducknng_aio_cancel(aio->aio);
            ducknng_aio_wait(aio->aio);
        }
        if (ducknng_aio_get_msg(aio->aio)) {
            nng_msg_free(ducknng_aio_get_msg(aio->aio));
            ducknng_aio_set_msg(aio->aio, NULL);
        }
        ducknng_aio_free(aio->aio);
    }
    if (aio->reply_msg) nng_msg_free(aio->reply_msg);
    if (aio->http_res) nng_http_res_free(aio->http_res);
    if (aio->http_req) nng_http_req_free(aio->http_req);
    if (aio->http_client) nng_http_client_free(aio->http_client);
    if (aio->http_url) nng_url_free(aio->http_url);
    if (aio->http_headers_json) duckdb_free(aio->http_headers_json);
    if (aio->http_body) duckdb_free(aio->http_body);
    if (aio->http_body_text) duckdb_free(aio->http_body_text);
    if (aio->has_ctx) ducknng_ctx_close(aio->ctx);
    if (aio->owns_socket && aio->open) ducknng_socket_close(aio->sock);
    if (aio->socket_ref) ducknng_runtime_release_client_socket(aio->socket_ref);
    if (aio->error) duckdb_free(aio->error);
    duckdb_free(aio);
}

void ducknng_runtime_destroy(ducknng_runtime *rt) {
    size_t i;
    duckdb_database db;
    if (!rt) return;
    db = rt->db;
    reg_lock();
    reg_remove(db);
    reg_unlock();
    if (rt->services) {
        for (i = 0; i < rt->service_count; i++) {
            ducknng_service *svc = rt->services[i];
            if (!svc) continue;
            ducknng_service_stop(svc, NULL);
            ducknng_service_destroy(svc);
        }
        duckdb_free(rt->services);
        rt->services = NULL;
        rt->service_count = 0;
        rt->service_cap = 0;
    }
    if (rt->client_aios) {
        for (i = 0; i < rt->client_aio_count; i++) {
            ducknng_client_aio_destroy(rt->client_aios[i]);
        }
        duckdb_free(rt->client_aios);
        rt->client_aios = NULL;
        rt->client_aio_count = 0;
        rt->client_aio_cap = 0;
    }
    if (rt->client_sockets) {
        for (i = 0; i < rt->client_socket_count; i++) {
            ducknng_client_socket *sock = rt->client_sockets[i];
            if (!sock) continue;
            ducknng_client_socket_destroy(sock);
        }
        duckdb_free(rt->client_sockets);
        rt->client_sockets = NULL;
        rt->client_socket_count = 0;
        rt->client_socket_cap = 0;
    }
    if (rt->tls_configs) {
        for (i = 0; i < rt->tls_config_count; i++) {
            ducknng_tls_config *cfg = rt->tls_configs[i];
            if (!cfg) continue;
            if (cfg->source) duckdb_free(cfg->source);
            ducknng_tls_opts_reset(&cfg->opts);
            duckdb_free(cfg);
        }
        duckdb_free(rt->tls_configs);
        rt->tls_configs = NULL;
        rt->tls_config_count = 0;
        rt->tls_config_cap = 0;
    }
    if (rt->user_codecs) {
        for (i = 0; i < rt->user_codec_count; i++) {
            if (rt->user_codecs[i].content_type) duckdb_free(rt->user_codecs[i].content_type);
            if (rt->user_codecs[i].function_name) duckdb_free(rt->user_codecs[i].function_name);
        }
        duckdb_free(rt->user_codecs);
        rt->user_codecs = NULL;
        rt->user_codec_count = 0;
        rt->user_codec_cap = 0;
    }
    ducknng_runtime_http_profiles_reset(rt);
    ducknng_method_registry_destroy(&rt->registry);
    ducknng_log_ring_destroy(&rt->log_ring);
    if (rt->execution_pool) {
        for (i = 0; i < rt->execution_pool_count; i++) {
            if (rt->execution_pool[i]) duckdb_disconnect(&rt->execution_pool[i]);
        }
        duckdb_free(rt->execution_pool);
        rt->execution_pool = NULL;
    }
    if (rt->execution_pool_busy) {
        duckdb_free(rt->execution_pool_busy);
        rt->execution_pool_busy = NULL;
    }
    if (rt->aio_cv_initialized) ducknng_cond_destroy(&rt->aio_cv);
    if (rt->init_con) duckdb_disconnect(&rt->init_con);
    if (rt->codec_con) duckdb_disconnect(&rt->codec_con);
    if (rt->execution_pool_mu_initialized) ducknng_mutex_destroy(&rt->execution_pool_mu);
    if (rt->codec_con_mu_initialized) ducknng_mutex_destroy(&rt->codec_con_mu);
    if (rt->init_con_mu_initialized) ducknng_mutex_destroy(&rt->init_con_mu);
    ducknng_mutex_destroy(&rt->mu);
    duckdb_free(rt);
}

duckdb_connection ducknng_runtime_execution_connection(ducknng_runtime *rt) {
    return rt ? rt->init_con : NULL;
}

const char *ducknng_runtime_execution_model(ducknng_runtime *rt) {
    (void)rt;
    return DUCKNNG_EXECUTION_MODEL_SHARED_SERIALIZED_CONNECTION;
}

void ducknng_runtime_execution_lane_lock(ducknng_runtime *rt) {
    if (!rt || !rt->init_con_mu_initialized) return;
    ducknng_mutex_lock(&rt->init_con_mu);
}

void ducknng_runtime_execution_lane_unlock(ducknng_runtime *rt) {
    if (!rt || !rt->init_con_mu_initialized) return;
    ducknng_runtime_current_request_service_set(rt, NULL);
    ducknng_mutex_unlock(&rt->init_con_mu);
}

void ducknng_runtime_init_con_lock(ducknng_runtime *rt) {
    ducknng_runtime_execution_lane_lock(rt);
}

void ducknng_runtime_init_con_unlock(ducknng_runtime *rt) {
    ducknng_runtime_execution_lane_unlock(rt);
}

duckdb_connection ducknng_runtime_codec_connection(ducknng_runtime *rt) {
    return rt ? rt->codec_con : NULL;
}

void ducknng_runtime_codec_connection_lock(ducknng_runtime *rt) {
    if (!rt || !rt->codec_con_mu_initialized) return;
    ducknng_mutex_lock(&rt->codec_con_mu);
}

void ducknng_runtime_codec_connection_unlock(ducknng_runtime *rt) {
    if (!rt || !rt->codec_con_mu_initialized) return;
    ducknng_mutex_unlock(&rt->codec_con_mu);
}

void ducknng_runtime_current_request_service_set(ducknng_runtime *rt, ducknng_service *svc) {
    if (!rt) return;
    atomic_store_explicit(&rt->current_request_service_ptr, (uintptr_t)svc, memory_order_release);
    if (svc) {
        g_thread_request_runtime = rt;
        g_thread_request_service = svc;
    } else if (g_thread_request_runtime == rt) {
        g_thread_request_runtime = NULL;
        g_thread_request_service = NULL;
    }
}

ducknng_service *ducknng_runtime_current_request_service_get(ducknng_runtime *rt) {
    if (!rt) return NULL;
    return (ducknng_service *)atomic_load_explicit(&rt->current_request_service_ptr, memory_order_acquire);
}

ducknng_service *ducknng_runtime_current_thread_request_service_get(ducknng_runtime *rt) {
    if (!rt || g_thread_request_runtime != rt) return NULL;
    return g_thread_request_service;
}

void ducknng_runtime_current_http_request_context_set(ducknng_runtime *rt,
    const ducknng_http_request_context *request_ctx) {
    if (!rt) return;
    if (request_ctx) {
        g_thread_http_request_runtime = rt;
        g_thread_http_request_context = request_ctx;
    } else if (g_thread_http_request_runtime == rt) {
        g_thread_http_request_runtime = NULL;
        g_thread_http_request_context = NULL;
    }
}

const ducknng_http_request_context *ducknng_runtime_current_thread_http_request_context_get(
    ducknng_runtime *rt) {
    if (!rt || g_thread_http_request_runtime != rt) return NULL;
    return g_thread_http_request_context;
}

void ducknng_runtime_current_authorizer_context_set(ducknng_runtime *rt,
    const ducknng_authorizer_context *auth_ctx) {
    if (!rt) return;
    if (auth_ctx) {
        g_thread_authorizer_runtime = rt;
        g_thread_authorizer_context = auth_ctx;
    } else if (g_thread_authorizer_runtime == rt) {
        g_thread_authorizer_runtime = NULL;
        g_thread_authorizer_context = NULL;
    }
}

const ducknng_authorizer_context *ducknng_runtime_current_thread_authorizer_context_get(ducknng_runtime *rt) {
    if (!rt || g_thread_authorizer_runtime != rt) return NULL;
    return g_thread_authorizer_context;
}

ducknng_service *ducknng_runtime_find_service(ducknng_runtime *rt, const char *name) {
    size_t i;
    ducknng_service *svc = NULL;
    if (!rt || !name) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->service_count; i++) {
        if (rt->services[i] && rt->services[i]->name && strcmp(rt->services[i]->name, name) == 0) {
            svc = rt->services[i];
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return svc;
}

int ducknng_runtime_add_service(ducknng_runtime *rt, ducknng_service *svc, char **errmsg) {
    ducknng_service **new_services;
    size_t new_cap;
    size_t i;
    if (!rt || !svc) return -1;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->service_count; i++) {
        if (rt->services[i] && strcmp(rt->services[i]->name, svc->name) == 0) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("service already exists");
            return -1;
        }
    }
    if (rt->service_count == rt->service_cap) {
        new_cap = rt->service_cap ? rt->service_cap * 2 : 4;
        new_services = (ducknng_service **)duckdb_malloc(sizeof(*new_services) * new_cap);
        if (!new_services) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("out of memory");
            return -1;
        }
        memset(new_services, 0, sizeof(*new_services) * new_cap);
        if (rt->services && rt->service_count) memcpy(new_services, rt->services, sizeof(*new_services) * rt->service_count);
        if (rt->services) duckdb_free(rt->services);
        rt->services = new_services;
        rt->service_cap = new_cap;
    }
    svc->service_id = rt->next_service_id++;
    rt->services[rt->service_count++] = svc;
    ducknng_mutex_unlock(&rt->mu);
    return 0;
}

ducknng_service *ducknng_runtime_remove_service(ducknng_runtime *rt, const char *name) {
    size_t i;
    ducknng_service *svc = NULL;
    if (!rt || !name) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->service_count; i++) {
        if (rt->services[i] && strcmp(rt->services[i]->name, name) == 0) {
            svc = rt->services[i];
            for (; i + 1 < rt->service_count; i++) rt->services[i] = rt->services[i + 1];
            rt->service_count--;
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return svc;
}

ducknng_client_socket *ducknng_runtime_find_client_socket(ducknng_runtime *rt, uint64_t socket_id) {
    size_t i;
    ducknng_client_socket *sock = NULL;
    if (!rt || socket_id == 0) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->client_socket_count; i++) {
        if (rt->client_sockets[i] && rt->client_sockets[i]->socket_id == socket_id) {
            sock = rt->client_sockets[i];
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return sock;
}

ducknng_client_socket *ducknng_runtime_acquire_client_socket(ducknng_runtime *rt, uint64_t socket_id) {
    size_t i;
    ducknng_client_socket *sock = NULL;
    if (!rt || socket_id == 0) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->client_socket_count; i++) {
        if (rt->client_sockets[i] && rt->client_sockets[i]->socket_id == socket_id) {
            sock = rt->client_sockets[i];
            break;
        }
    }
    if (sock && sock->mu_initialized) {
        ducknng_mutex_lock(&sock->mu);
        if (sock->closing) {
            ducknng_mutex_unlock(&sock->mu);
            sock = NULL;
        } else {
            sock->refcount++;
            ducknng_mutex_unlock(&sock->mu);
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return sock;
}

int ducknng_runtime_add_client_socket(ducknng_runtime *rt, ducknng_client_socket *sock, char **errmsg) {
    ducknng_client_socket **new_sockets;
    size_t new_cap;
    if (!rt || !sock) return -1;
    ducknng_mutex_lock(&rt->mu);
    if (rt->client_socket_count == rt->client_socket_cap) {
        new_cap = rt->client_socket_cap ? rt->client_socket_cap * 2 : 4;
        new_sockets = (ducknng_client_socket **)duckdb_malloc(sizeof(*new_sockets) * new_cap);
        if (!new_sockets) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("out of memory");
            return -1;
        }
        memset(new_sockets, 0, sizeof(*new_sockets) * new_cap);
        if (rt->client_sockets && rt->client_socket_count) {
            memcpy(new_sockets, rt->client_sockets, sizeof(*new_sockets) * rt->client_socket_count);
        }
        if (rt->client_sockets) duckdb_free(rt->client_sockets);
        rt->client_sockets = new_sockets;
        rt->client_socket_cap = new_cap;
    }
    sock->socket_id = rt->next_client_socket_id++;
    rt->client_sockets[rt->client_socket_count++] = sock;
    ducknng_mutex_unlock(&rt->mu);
    return 0;
}

ducknng_client_socket *ducknng_runtime_remove_client_socket(ducknng_runtime *rt, uint64_t socket_id) {
    size_t i;
    ducknng_client_socket *sock = NULL;
    if (!rt || socket_id == 0) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->client_socket_count; i++) {
        if (rt->client_sockets[i] && rt->client_sockets[i]->socket_id == socket_id) {
            sock = rt->client_sockets[i];
            for (; i + 1 < rt->client_socket_count; i++) rt->client_sockets[i] = rt->client_sockets[i + 1];
            rt->client_socket_count--;
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return sock;
}

int ducknng_runtime_add_client_aio(ducknng_runtime *rt, ducknng_client_aio *aio, char **errmsg) {
    ducknng_client_aio **new_aios;
    size_t new_cap;
    if (!rt || !aio) return -1;
    ducknng_mutex_lock(&rt->mu);
    if (rt->client_aio_count == rt->client_aio_cap) {
        new_cap = rt->client_aio_cap ? rt->client_aio_cap * 2 : 4;
        new_aios = (ducknng_client_aio **)duckdb_malloc(sizeof(*new_aios) * new_cap);
        if (!new_aios) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("out of memory");
            return -1;
        }
        memset(new_aios, 0, sizeof(*new_aios) * new_cap);
        if (rt->client_aios && rt->client_aio_count) {
            memcpy(new_aios, rt->client_aios, sizeof(*new_aios) * rt->client_aio_count);
        }
        if (rt->client_aios) duckdb_free(rt->client_aios);
        rt->client_aios = new_aios;
        rt->client_aio_cap = new_cap;
    }
    aio->aio_id = rt->next_client_aio_id++;
    rt->client_aios[rt->client_aio_count++] = aio;
    ducknng_mutex_unlock(&rt->mu);
    return 0;
}

ducknng_client_aio *ducknng_runtime_remove_client_aio(ducknng_runtime *rt, uint64_t aio_id) {
    size_t i;
    ducknng_client_aio *aio = NULL;
    if (!rt || aio_id == 0) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->client_aio_count; i++) {
        if (rt->client_aios[i] && rt->client_aios[i]->aio_id == aio_id) {
            aio = rt->client_aios[i];
            for (; i + 1 < rt->client_aio_count; i++) rt->client_aios[i] = rt->client_aios[i + 1];
            rt->client_aio_count--;
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return aio;
}

ducknng_tls_config *ducknng_runtime_find_tls_config(ducknng_runtime *rt, uint64_t tls_config_id) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (!rt || tls_config_id == 0) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->tls_config_count; i++) {
        if (rt->tls_configs[i] && rt->tls_configs[i]->tls_config_id == tls_config_id) {
            cfg = rt->tls_configs[i];
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return cfg;
}

int ducknng_runtime_add_tls_config(ducknng_runtime *rt, ducknng_tls_config *cfg, char **errmsg) {
    ducknng_tls_config **new_configs;
    size_t new_cap;
    if (!rt || !cfg) return -1;
    ducknng_mutex_lock(&rt->mu);
    if (rt->tls_config_count == rt->tls_config_cap) {
        new_cap = rt->tls_config_cap ? rt->tls_config_cap * 2 : 4;
        new_configs = (ducknng_tls_config **)duckdb_malloc(sizeof(*new_configs) * new_cap);
        if (!new_configs) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("out of memory");
            return -1;
        }
        memset(new_configs, 0, sizeof(*new_configs) * new_cap);
        if (rt->tls_configs && rt->tls_config_count) {
            memcpy(new_configs, rt->tls_configs, sizeof(*new_configs) * rt->tls_config_count);
        }
        if (rt->tls_configs) duckdb_free(rt->tls_configs);
        rt->tls_configs = new_configs;
        rt->tls_config_cap = new_cap;
    }
    cfg->tls_config_id = rt->next_tls_config_id++;
    rt->tls_configs[rt->tls_config_count++] = cfg;
    ducknng_mutex_unlock(&rt->mu);
    return 0;
}

ducknng_tls_config *ducknng_runtime_remove_tls_config(ducknng_runtime *rt, uint64_t tls_config_id) {
    size_t i;
    ducknng_tls_config *cfg = NULL;
    if (!rt || tls_config_id == 0) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->tls_config_count; i++) {
        if (rt->tls_configs[i] && rt->tls_configs[i]->tls_config_id == tls_config_id) {
            cfg = rt->tls_configs[i];
            for (; i + 1 < rt->tls_config_count; i++) rt->tls_configs[i] = rt->tls_configs[i + 1];
            rt->tls_config_count--;
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return cfg;
}

static char *ducknng_codec_normalize(const char *content_type) {
    /* Lower-case ASCII and trim trailing whitespace; codec keys are case-insensitive
     * and we don't strip parameters here so e.g. "text/plain" and "Text/Plain" share
     * one slot but "text/plain;charset=utf-8" remains a distinct key. */
    size_t len;
    size_t i;
    char *out;
    while (content_type && (*content_type == ' ' || *content_type == '\t')) content_type++;
    if (!content_type || !*content_type) return NULL;
    len = strlen(content_type);
    while (len > 0 && (content_type[len - 1] == ' ' || content_type[len - 1] == '\t')) len--;
    if (len == 0) return NULL;
    out = (char *)duckdb_malloc(len + 1);
    if (!out) return NULL;
    for (i = 0; i < len; i++) {
        unsigned char c = (unsigned char)content_type[i];
        out[i] = (c >= 'A' && c <= 'Z') ? (char)(c + 32) : (char)c;
    }
    out[len] = '\0';
    return out;
}

int ducknng_runtime_register_user_codec(ducknng_runtime *rt, const char *content_type,
    const char *function_name, char **errmsg) {
    char *key;
    char *fn_copy;
    size_t i;
    int ok = 0;
    if (!rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: runtime not initialized");
        return 0;
    }
    if (!function_name || !*function_name) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: codec function name must be a non-empty identifier");
        return 0;
    }
    key = ducknng_codec_normalize(content_type);
    if (!key) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: codec content_type must be a non-empty media type");
        return 0;
    }
    fn_copy = ducknng_strdup(function_name);
    if (!fn_copy) {
        duckdb_free(key);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory registering codec");
        return 0;
    }
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->user_codec_count; i++) {
        if (rt->user_codecs[i].content_type && strcmp(rt->user_codecs[i].content_type, key) == 0) {
            duckdb_free(rt->user_codecs[i].function_name);
            rt->user_codecs[i].function_name = fn_copy;
            duckdb_free(key);
            ok = 1;
            goto done;
        }
    }
    if (rt->user_codec_count == rt->user_codec_cap) {
        size_t new_cap = rt->user_codec_cap ? rt->user_codec_cap * 2 : 4;
        ducknng_user_codec *grown = (ducknng_user_codec *)duckdb_malloc(sizeof(*grown) * new_cap);
        if (!grown) {
            duckdb_free(key);
            duckdb_free(fn_copy);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing codec registry");
            goto done;
        }
        memset(grown, 0, sizeof(*grown) * new_cap);
        if (rt->user_codecs && rt->user_codec_count) {
            memcpy(grown, rt->user_codecs, sizeof(*grown) * rt->user_codec_count);
        }
        if (rt->user_codecs) duckdb_free(rt->user_codecs);
        rt->user_codecs = grown;
        rt->user_codec_cap = new_cap;
    }
    rt->user_codecs[rt->user_codec_count].content_type = key;
    rt->user_codecs[rt->user_codec_count].function_name = fn_copy;
    rt->user_codec_count++;
    ok = 1;
done:
    ducknng_mutex_unlock(&rt->mu);
    return ok;
}

int ducknng_runtime_unregister_user_codec(ducknng_runtime *rt, const char *content_type) {
    char *key;
    size_t i;
    int removed = 0;
    if (!rt) return 0;
    key = ducknng_codec_normalize(content_type);
    if (!key) return 0;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->user_codec_count; i++) {
        if (rt->user_codecs[i].content_type && strcmp(rt->user_codecs[i].content_type, key) == 0) {
            duckdb_free(rt->user_codecs[i].content_type);
            duckdb_free(rt->user_codecs[i].function_name);
            for (; i + 1 < rt->user_codec_count; i++) rt->user_codecs[i] = rt->user_codecs[i + 1];
            memset(&rt->user_codecs[rt->user_codec_count - 1], 0, sizeof(rt->user_codecs[0]));
            rt->user_codec_count--;
            removed = 1;
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    duckdb_free(key);
    return removed;
}

char *ducknng_runtime_find_user_codec(ducknng_runtime *rt, const char *content_type) {
    char *key;
    char *result = NULL;
    size_t i;
    if (!rt) return NULL;
    key = ducknng_codec_normalize(content_type);
    if (!key) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->user_codec_count; i++) {
        if (rt->user_codecs[i].content_type && strcmp(rt->user_codecs[i].content_type, key) == 0) {
            result = ducknng_strdup(rt->user_codecs[i].function_name);
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    duckdb_free(key);
    return result;
}

/* ---------------------------------------------------------------------------
 * DuckDB log entry ring buffer
 * --------------------------------------------------------------------------- */

void ducknng_log_ring_init(ducknng_log_ring *ring) {
    if (!ring) return;
    memset(ring, 0, sizeof(*ring));
    if (ducknng_mutex_init(&ring->mu) == 0) ring->mu_initialized = 1;
}

void ducknng_log_ring_destroy(ducknng_log_ring *ring) {
    size_t i;
    if (!ring) return;
    for (i = 0; i < DUCKNNG_LOG_RING_CAP; i++) {
        if (ring->entries[i].level) { duckdb_free(ring->entries[i].level); ring->entries[i].level = NULL; }
        if (ring->entries[i].log_type) { duckdb_free(ring->entries[i].log_type); ring->entries[i].log_type = NULL; }
        if (ring->entries[i].message) { duckdb_free(ring->entries[i].message); ring->entries[i].message = NULL; }
    }
    if (ring->mu_initialized) ducknng_mutex_destroy(&ring->mu);
}

void ducknng_log_ring_append(ducknng_log_ring *ring, const duckdb_timestamp *ts,
    const char *level, const char *log_type, const char *message) {
    size_t slot;
    ducknng_log_entry *e;
    if (!ring || !ring->mu_initialized) return;
    ducknng_mutex_lock(&ring->mu);
    if (ring->count < DUCKNNG_LOG_RING_CAP) {
        slot = (ring->head + ring->count) % DUCKNNG_LOG_RING_CAP;
        ring->count++;
    } else {
        /* overwrite oldest */
        slot = ring->head;
        ring->head = (ring->head + 1) % DUCKNNG_LOG_RING_CAP;
        e = &ring->entries[slot];
        if (e->level) { duckdb_free(e->level); e->level = NULL; }
        if (e->log_type) { duckdb_free(e->log_type); e->log_type = NULL; }
        if (e->message) { duckdb_free(e->message); e->message = NULL; }
    }
    e = &ring->entries[slot];
    if (ts) e->ts = *ts; else memset(&e->ts, 0, sizeof(e->ts));
    e->level = level ? ducknng_strdup(level) : NULL;
    e->log_type = log_type ? ducknng_strdup(log_type) : NULL;
    e->message = message ? ducknng_strdup(message) : NULL;
    ducknng_mutex_unlock(&ring->mu);
}

size_t ducknng_log_ring_snapshot(ducknng_log_ring *ring,
    duckdb_timestamp *out_ts, char **out_level, char **out_log_type, char **out_message) {
    size_t i, n;
    if (!ring || !ring->mu_initialized || !out_ts || !out_level || !out_log_type || !out_message) return 0;
    ducknng_mutex_lock(&ring->mu);
    n = ring->count;
    for (i = 0; i < n; i++) {
        size_t idx = (ring->head + i) % DUCKNNG_LOG_RING_CAP;
        ducknng_log_entry *e = &ring->entries[idx];
        out_ts[i] = e->ts;
        out_level[i] = e->level ? ducknng_strdup(e->level) : NULL;
        out_log_type[i] = e->log_type ? ducknng_strdup(e->log_type) : NULL;
        out_message[i] = e->message ? ducknng_strdup(e->message) : NULL;
    }
    ducknng_mutex_unlock(&ring->mu);
    return n;
}

/* DuckDB log storage write callback — wires the DuckDB logger into the ring.
 * Signature matches duckdb_logger_write_log_entry_t.
 * extra_data is a ducknng_runtime *. */
void ducknng_log_write_entry(void *extra_data, duckdb_timestamp *timestamp,
    const char *level, const char *log_type, const char *log_message) {
    ducknng_runtime *rt = (ducknng_runtime *)extra_data;
    if (!rt) return;
    ducknng_log_ring_append(&rt->log_ring, timestamp, level, log_type, log_message);
}
