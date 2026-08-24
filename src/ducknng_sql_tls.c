#include "ducknng_sql_shared.h"
#include "ducknng_runtime.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    uint64_t tls_config_id;
    char *source;
    bool enabled;
    bool has_cert_key_file;
    bool has_ca_file;
    bool has_cert_pem;
    bool has_key_pem;
    bool has_ca_pem;
    bool has_password;
    int32_t auth_mode;
    bool peer_allowlist_active;
    uint64_t peer_allowlist_count;
    char *peer_allowlist_json;
} ducknng_tls_config_row;

typedef struct {
    ducknng_tls_config_row *rows;
    idx_t row_count;
} ducknng_tls_configs_bind_data;

typedef struct {
    ducknng_tls_configs_bind_data *bind;
    idx_t offset;
} ducknng_tls_configs_init_data;

static int ducknng_cn_is_safe(const char *cn) {
    size_t i;
    if (!cn || !cn[0]) return 0;
    for (i = 0; cn[i]; i++) {
        char c = cn[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '.' || c == ':' || c == '-' || c == '_')) {
            return 0;
        }
    }
    return 1;
}
static char *ducknng_read_text_file(const char *path) {
    FILE *fp;
    long len;
    char *buf;
    if (!path || !path[0]) return NULL;
    fp = fopen(path, "rb");
    if (!fp) return NULL;
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }
    len = ftell(fp);
    if (len < 0 || fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }
    buf = (char *)duckdb_malloc((size_t)len + 1);
    if (!buf) {
        fclose(fp);
        return NULL;
    }
    if (len > 0 && fread(buf, 1, (size_t)len, fp) != (size_t)len) {
        fclose(fp);
        duckdb_free(buf);
        return NULL;
    }
    buf[len] = '\0';
    fclose(fp);
    return buf;
}
static int ducknng_generate_self_signed_material(const char *cn, int32_t valid_days,
    char **cert_pem, char **key_pem, char **ca_pem, char **errmsg) {
    char cmd[2048];
    char cert_path[512];
    char key_path[512];
    char *dir;
    int status;
#ifdef _WIN32
    const char *path_sep = "\\";
    const char *null_sink = "NUL";
#else
    const char *path_sep = "/";
    const char *null_sink = "/dev/null";
#endif
    if (cert_pem) *cert_pem = NULL;
    if (key_pem) *key_pem = NULL;
    if (ca_pem) *ca_pem = NULL;
    if (!ducknng_cn_is_safe(cn)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: common name must use only letters, digits, dot, colon, dash, or underscore");
        return -1;
    }
    if (valid_days <= 0) valid_days = 365;
    dir = ducknng_make_temp_dir("ducknng-cert-");
    if (!dir) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to create temp directory for certificate generation");
        return -1;
    }
    snprintf(cert_path, sizeof(cert_path), "%s%scert.pem", dir, path_sep);
    snprintf(key_path, sizeof(key_path), "%s%skey.pem", dir, path_sep);
    snprintf(cmd, sizeof(cmd), "openssl req -x509 -newkey rsa:2048 -keyout '%s' -out '%s' -sha256 -days %d -nodes -subj '/CN=%s' >%s 2>&1", key_path, cert_path, (int)valid_days, cn, null_sink);
    status = system(cmd);
    if (status != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: openssl certificate generation failed");
        ducknng_remove_file(cert_path);
        ducknng_remove_file(key_path);
        ducknng_remove_dir(dir);
        duckdb_free(dir);
        return -1;
    }
    if (cert_pem) *cert_pem = ducknng_read_text_file(cert_path);
    if (key_pem) *key_pem = ducknng_read_text_file(key_path);
    if (ca_pem) *ca_pem = ducknng_read_text_file(cert_path);
    ducknng_remove_file(cert_path);
    ducknng_remove_file(key_path);
    ducknng_remove_dir(dir);
    duckdb_free(dir);
    if ((cert_pem && !*cert_pem) || (key_pem && !*key_pem) || (ca_pem && !*ca_pem)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to read generated certificate material");
        if (cert_pem && *cert_pem) duckdb_free(*cert_pem);
        if (key_pem && *key_pem) duckdb_free(*key_pem);
        if (ca_pem && *ca_pem) duckdb_free(*ca_pem);
        if (cert_pem) *cert_pem = NULL;
        if (key_pem) *key_pem = NULL;
        if (ca_pem) *ca_pem = NULL;
        return -1;
    }
    return 0;
}

static void destroy_tls_configs_bind_data(void *ptr) {
    ducknng_tls_configs_bind_data *data = (ducknng_tls_configs_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].source) duckdb_free(data->rows[i].source);
        if (data->rows[i].peer_allowlist_json) duckdb_free(data->rows[i].peer_allowlist_json);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_tls_configs_init_data(void *ptr) {
    ducknng_tls_configs_init_data *data = (ducknng_tls_configs_init_data *)ptr;
    if (data) duckdb_free(data);
}

static void ducknng_tls_config_from_files_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        ducknng_tls_config *cfg;
        char *cert_key_file = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *ca_file = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *password = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        int auth_mode = arg_int32(duckdb_data_chunk_get_vector(input, 3), row, 0);
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || auth_mode < 0 || auth_mode > 2 || ((!cert_key_file || !cert_key_file[0]) && (!ca_file || !ca_file[0]) && auth_mode == 0)) {
            if (cert_key_file) duckdb_free(cert_key_file);
            if (ca_file) duckdb_free(ca_file);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: invalid tls config file arguments");
            return;
        }
        cfg = (ducknng_tls_config *)duckdb_malloc(sizeof(*cfg));
        if (!cfg) {
            if (cert_key_file) duckdb_free(cert_key_file);
            if (ca_file) duckdb_free(ca_file);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating tls config");
            return;
        }
        memset(cfg, 0, sizeof(*cfg));
        cfg->source = ducknng_strdup("files");
        ducknng_tls_opts_init(&cfg->opts);
        cfg->opts.enabled = 1;
        cfg->opts.cert_key_file = cert_key_file;
        cfg->opts.ca_file = ca_file;
        cfg->opts.password = password;
        cfg->opts.auth_mode = auth_mode;
        if (!cfg->source || ducknng_runtime_add_tls_config(ctx->rt, cfg, &errmsg) != 0) {
            if (cfg->source) duckdb_free(cfg->source);
            ducknng_tls_opts_reset(&cfg->opts);
            duckdb_free(cfg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register tls config");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = cfg->tls_config_id;
    }
}

static void ducknng_tls_config_from_pem_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        ducknng_tls_config *cfg;
        char *cert_pem = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *key_pem = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *ca_pem = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *password = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        int auth_mode = arg_int32(duckdb_data_chunk_get_vector(input, 4), row, 0);
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || auth_mode < 0 || auth_mode > 2 || ((!cert_pem || !cert_pem[0]) && (!key_pem || !key_pem[0]) && (!ca_pem || !ca_pem[0]) && auth_mode == 0)) {
            if (cert_pem) duckdb_free(cert_pem);
            if (key_pem) duckdb_free(key_pem);
            if (ca_pem) duckdb_free(ca_pem);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: invalid tls pem arguments");
            return;
        }
        cfg = (ducknng_tls_config *)duckdb_malloc(sizeof(*cfg));
        if (!cfg) {
            if (cert_pem) duckdb_free(cert_pem);
            if (key_pem) duckdb_free(key_pem);
            if (ca_pem) duckdb_free(ca_pem);
            if (password) duckdb_free(password);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating tls config");
            return;
        }
        memset(cfg, 0, sizeof(*cfg));
        cfg->source = ducknng_strdup("pem");
        ducknng_tls_opts_init(&cfg->opts);
        cfg->opts.enabled = 1;
        cfg->opts.cert_pem = cert_pem;
        cfg->opts.key_pem = key_pem;
        cfg->opts.ca_pem = ca_pem;
        cfg->opts.password = password;
        cfg->opts.auth_mode = auth_mode;
        if (!cfg->source || ducknng_runtime_add_tls_config(ctx->rt, cfg, &errmsg) != 0) {
            if (cfg->source) duckdb_free(cfg->source);
            ducknng_tls_opts_reset(&cfg->opts);
            duckdb_free(cfg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register tls config");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = cfg->tls_config_id;
    }
}

static void ducknng_self_signed_tls_config_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    uint64_t *out = (uint64_t *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        ducknng_tls_config *cfg;
        char *cn = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        int32_t valid_days = arg_int32(duckdb_data_chunk_get_vector(input, 1), row, 365);
        int auth_mode = arg_int32(duckdb_data_chunk_get_vector(input, 2), row, 0);
        char *cert_pem = NULL, *key_pem = NULL, *ca_pem = NULL, *errmsg = NULL;
        if (!ctx || !ctx->rt || !cn || auth_mode < 0 || auth_mode > 2) {
            if (cn) duckdb_free(cn);
            duckdb_scalar_function_set_error(info, "ducknng: invalid self-signed tls config arguments");
            return;
        }
        if (ducknng_generate_self_signed_material(cn, valid_days, &cert_pem, &key_pem, &ca_pem, &errmsg) != 0) {
            duckdb_free(cn);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to generate self-signed certificate");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(cn);
        cfg = (ducknng_tls_config *)duckdb_malloc(sizeof(*cfg));
        if (!cfg) {
            if (cert_pem) duckdb_free(cert_pem);
            if (key_pem) duckdb_free(key_pem);
            if (ca_pem) duckdb_free(ca_pem);
            duckdb_scalar_function_set_error(info, "ducknng: out of memory allocating tls config");
            return;
        }
        memset(cfg, 0, sizeof(*cfg));
        cfg->source = ducknng_strdup("self_signed");
        ducknng_tls_opts_init(&cfg->opts);
        cfg->opts.enabled = 1;
        cfg->opts.cert_pem = cert_pem;
        cfg->opts.key_pem = key_pem;
        cfg->opts.ca_pem = ca_pem;
        cfg->opts.auth_mode = auth_mode;
        if (!cfg->source || ducknng_runtime_add_tls_config(ctx->rt, cfg, &errmsg) != 0) {
            if (cfg->source) duckdb_free(cfg->source);
            ducknng_tls_opts_reset(&cfg->opts);
            duckdb_free(cfg);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register tls config");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        out[row] = cfg->tls_config_id;
    }
}

static void ducknng_drop_tls_config_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        ducknng_tls_config *cfg;
        if (!ctx || !ctx->rt || tls_config_id == 0) {
            duckdb_scalar_function_set_error(info, "ducknng: tls config id is required");
            return;
        }
        cfg = ducknng_runtime_remove_tls_config(ctx->rt, tls_config_id);
        if (!cfg) {
            duckdb_scalar_function_set_error(info, "ducknng: tls config not found");
            return;
        }
        if (cfg->source) duckdb_free(cfg->source);
        ducknng_tls_opts_reset(&cfg->opts);
        duckdb_free(cfg);
        out[row] = true;
    }
}

static void ducknng_set_tls_peer_allowlist_scalar(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    bool *out = (bool *)duckdb_vector_get_data(output);
    for (row = 0; row < count; row++) {
        uint64_t tls_config_id = arg_u64(duckdb_data_chunk_get_vector(input, 0), row, 0);
        char *identities_json = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_tls_config *cfg = NULL;
        char *errmsg = NULL;
        size_t i;
        if (!ctx || !ctx->rt || tls_config_id == 0) {
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: tls config id is required");
            return;
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
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: tls config not found");
            return;
        }
        if (ducknng_tls_opts_set_peer_allowlist(&cfg->opts, identities_json, &errmsg) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            if (identities_json) duckdb_free(identities_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set TLS peer allowlist");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        ducknng_mutex_unlock(&ctx->rt->mu);
        if (identities_json) duckdb_free(identities_json);
        out[row] = true;
    }
}

static void ducknng_tls_configs_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_tls_configs_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_tls_configs_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    bind->row_count = (idx_t)ctx->rt->tls_config_count;
    if (bind->row_count > 0) {
        bind->rows = (ducknng_tls_config_row *)duckdb_malloc(sizeof(*bind->rows) * (size_t)bind->row_count);
        if (!bind->rows) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            duckdb_free(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        memset(bind->rows, 0, sizeof(*bind->rows) * (size_t)bind->row_count);
        for (i = 0; i < (size_t)bind->row_count; i++) {
            ducknng_tls_config *cfg = ctx->rt->tls_configs[i];
            bind->rows[i].tls_config_id = cfg ? cfg->tls_config_id : 0;
            bind->rows[i].source = cfg && cfg->source ? ducknng_strdup(cfg->source) : NULL;
            bind->rows[i].enabled = cfg ? (bool)cfg->opts.enabled : false;
            bind->rows[i].has_cert_key_file = cfg && cfg->opts.cert_key_file && cfg->opts.cert_key_file[0];
            bind->rows[i].has_ca_file = cfg && cfg->opts.ca_file && cfg->opts.ca_file[0];
            bind->rows[i].has_cert_pem = cfg && cfg->opts.cert_pem && cfg->opts.cert_pem[0];
            bind->rows[i].has_key_pem = cfg && cfg->opts.key_pem && cfg->opts.key_pem[0];
            bind->rows[i].has_ca_pem = cfg && cfg->opts.ca_pem && cfg->opts.ca_pem[0];
            bind->rows[i].has_password = cfg && cfg->opts.password && cfg->opts.password[0];
            bind->rows[i].auth_mode = cfg ? cfg->opts.auth_mode : 0;
            bind->rows[i].peer_allowlist_active = cfg ? (bool)cfg->opts.peer_allowlist_active : false;
            bind->rows[i].peer_allowlist_count = cfg ? (uint64_t)cfg->opts.peer_allowlist_count : 0;
            bind->rows[i].peer_allowlist_json = cfg && cfg->opts.peer_allowlist_json ? ducknng_strdup(cfg->opts.peer_allowlist_json) : NULL;
        }
    }
    ducknng_mutex_unlock(&ctx->rt->mu);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "tls_config_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "source", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "enabled", type);
    duckdb_bind_add_result_column(info, "has_cert_key_file", type);
    duckdb_bind_add_result_column(info, "has_ca_file", type);
    duckdb_bind_add_result_column(info, "has_cert_pem", type);
    duckdb_bind_add_result_column(info, "has_key_pem", type);
    duckdb_bind_add_result_column(info, "has_ca_pem", type);
    duckdb_bind_add_result_column(info, "has_password", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "auth_mode", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "peer_allowlist_active", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "peer_allowlist_count", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "peer_allowlist_json", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_tls_configs_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_tls_configs_init(duckdb_init_info info) {
    ducknng_tls_configs_bind_data *bind = (ducknng_tls_configs_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_tls_configs_init_data *init = (ducknng_tls_configs_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_tls_configs_init_data);
}

static void ducknng_tls_configs_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_tls_configs_init_data *init = (ducknng_tls_configs_init_data *)duckdb_function_get_init_data(info);
    ducknng_tls_configs_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    uint64_t *ids;
    bool *enabled;
    bool *has_cert_key_file;
    bool *has_ca_file;
    bool *has_cert_pem;
    bool *has_key_pem;
    bool *has_ca_pem;
    bool *has_password;
    int32_t *auth_mode;
    bool *peer_allowlist_active;
    uint64_t *peer_allowlist_count;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    enabled = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    has_cert_key_file = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3));
    has_ca_file = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 4));
    has_cert_pem = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 5));
    has_key_pem = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 6));
    has_ca_pem = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 7));
    has_password = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8));
    auth_mode = (int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 9));
    peer_allowlist_active = (bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 10));
    peer_allowlist_count = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11));
    for (i = 0; i < chunk_size; i++) {
        ducknng_tls_config_row *row = &bind->rows[init->offset + i];
        ids[i] = row->tls_config_id;
        enabled[i] = row->enabled;
        has_cert_key_file[i] = row->has_cert_key_file;
        has_ca_file[i] = row->has_ca_file;
        has_cert_pem[i] = row->has_cert_pem;
        has_key_pem[i] = row->has_key_pem;
        has_ca_pem[i] = row->has_ca_pem;
        has_password[i] = row->has_password;
        auth_mode[i] = row->auth_mode;
        peer_allowlist_active[i] = row->peer_allowlist_active;
        peer_allowlist_count[i] = row->peer_allowlist_count;
        if (row->source) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), i, row->source, (idx_t)strlen(row->source));
        else set_null(duckdb_data_chunk_get_vector(output, 1), i);
        if (row->peer_allowlist_json) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 12), i, row->peer_allowlist_json, (idx_t)strlen(row->peer_allowlist_json));
        else set_null(duckdb_data_chunk_get_vector(output, 12), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

int ducknng_register_sql_tls(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type tls_files_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER};
    duckdb_type tls_pem_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER};
    duckdb_type tls_self_signed_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_INTEGER};
    duckdb_type tls_id_types[1] = {DUCKDB_TYPE_UBIGINT};
    duckdb_type tls_allowlist_types[2] = {DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_VARCHAR};
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_tls_config_from_files", 4, ducknng_tls_config_from_files_scalar, ctx, tls_files_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_tls_config_from_pem", 5, ducknng_tls_config_from_pem_scalar, ctx, tls_pem_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_self_signed_tls_config", 3, ducknng_self_signed_tls_config_scalar, ctx, tls_self_signed_types, DUCKDB_TYPE_UBIGINT)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_drop_tls_config", 1, ducknng_drop_tls_config_scalar, ctx, tls_id_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_tls_peer_allowlist", 2, ducknng_set_tls_peer_allowlist_scalar, ctx, tls_allowlist_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_list_tls_configs", ctx, 0, NULL,
            ducknng_tls_configs_bind, ducknng_tls_configs_init, ducknng_tls_configs_scan)) return 0;
    return 1;
}
