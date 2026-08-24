#include "duckdb_extension.h"
#include "ducknng_nng_compat.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_api.h"
#ifdef __EMSCRIPTEN__
#include <stdio.h>
#if defined(DUCKNNG_WASM_THREADS_RUNTIME)
#include <emscripten/threading.h>
#endif
#endif

#if defined(__EMSCRIPTEN__) && defined(DUCKNNG_WASM_TRACE) && DUCKNNG_WASM_TRACE
static void ducknng_extension_wasm_trace(const char *where) {
    fprintf(stderr, "[ducknng wasm trace] %s\n", where ? where : "(null)");
    fflush(stderr);
}
#else
static void ducknng_extension_wasm_trace(const char *where) {
    (void)where;
#if defined(__EMSCRIPTEN__)
    __asm__ __volatile__("" ::: "memory");
    fflush(stderr);
#endif
}
#endif

static void ducknng_wasm_nng_init_tuning(void) {
#ifdef __EMSCRIPTEN__
    ducknng_extension_wasm_trace("extension: nng init tuning begin");
    /* duckdb-wasm side modules cannot set Emscripten's pthread pool size.
     * Keep NNG's internal worker pools small so local transport probes do not
     * exhaust browser worker startup capacity before the first inproc round trip. */
    nng_init_set_parameter(NNG_INIT_NUM_TASK_THREADS, 2);
    nng_init_set_parameter(NNG_INIT_MAX_TASK_THREADS, 2);
    nng_init_set_parameter(NNG_INIT_NUM_EXPIRE_THREADS, 1);
    nng_init_set_parameter(NNG_INIT_MAX_EXPIRE_THREADS, 1);
    nng_init_set_parameter(NNG_INIT_NUM_POLLER_THREADS, 1);
    nng_init_set_parameter(NNG_INIT_MAX_POLLER_THREADS, 1);
    nng_init_set_parameter(NNG_INIT_NUM_RESOLVER_THREADS, 1);
    ducknng_extension_wasm_trace("extension: nng init tuning returned");
#else
    return;
#endif
}

static void ducknng_wasm_nng_runtime_warmup(void) {
#if defined(__EMSCRIPTEN__) && defined(DUCKNNG_WASM_INPROC_ONLY) && \
    DUCKNNG_WASM_INPROC_ONLY && defined(DUCKNNG_WASM_THREADS_RUNTIME)
    nng_socket sock;
    int rv;

    ducknng_extension_wasm_trace("extension: nng warmup begin");
    rv = ducknng_rep_socket_open(&sock);
    if (rv == 0) {
        emscripten_thread_sleep(25.0);
        (void)ducknng_socket_close(sock);
        emscripten_thread_sleep(25.0);
    }
    ducknng_extension_wasm_trace("extension: nng warmup returned");
#else
    return;
#endif
}

DUCKDB_EXTENSION_ENTRYPOINT_CUSTOM(duckdb_extension_info info, struct duckdb_extension_access *access) {
    duckdb_connection connection = NULL;
    ducknng_runtime *rt = NULL;
    duckdb_database *db = NULL;
    int created = 0;
    ducknng_extension_wasm_trace("extension: entrypoint begin");
    if (!access || !info) {
        return false;
    }
    ducknng_wasm_nng_init_tuning();
    ducknng_wasm_nng_runtime_warmup();
    ducknng_extension_wasm_trace("extension: get database begin");
    db = access->get_database(info);
    if (!db || !*db) {
        access->set_error(info, "ducknng: failed to get database handle");
        return false;
    }
    ducknng_extension_wasm_trace("extension: connect begin");
    if (duckdb_connect(*db, &connection) == DuckDBError || !connection) {
        access->set_error(info, "ducknng: failed to open init connection");
        return false;
    }
    ducknng_extension_wasm_trace("extension: runtime init begin");
    if (!ducknng_runtime_init(connection, info, access, &rt, &created)) {
        duckdb_disconnect(&connection);
        return false;
    }
    ducknng_extension_wasm_trace("extension: register sql api begin");
    if (!ducknng_register_sql_api(connection, rt)) {
        access->set_error(info, "ducknng: failed to register sql api");
        if (created) {
            ducknng_runtime_destroy(rt);
        } else {
            duckdb_disconnect(&connection);
        }
        return false;
    }
    /* Log capture is enabled at runtime via SELECT ducknng_enable_log_capture()
     * rather than here. Calling duckdb_register_log_storage during extension
     * load triggers a DuckDB v1.5.2 internal assertion failure. */
    if (!created) {
        ducknng_extension_wasm_trace("extension: disconnect init connection begin");
        duckdb_disconnect(&connection);
    }
    ducknng_extension_wasm_trace("extension: entrypoint returned");
    return true;
}
