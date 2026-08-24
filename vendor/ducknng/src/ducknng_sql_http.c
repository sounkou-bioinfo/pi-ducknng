#include "ducknng_sql_shared.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    uint64_t service_id;
    char *service_name;
    uint64_t route_id;
    char *method;
    char *match_kind;
    char *path;
    uint64_t request_max_bytes;
    char *handler_sql;
    char *static_dir_path;
    int auth_require_identity;
    char *auth_allow_identities_json;
    uint8_t response_mode;
    char *stream_content_type;
} ducknng_http_route_row;

typedef struct {
    ducknng_http_route_row *rows;
    idx_t row_count;
    idx_t row_cap;
} ducknng_http_routes_bind_data;

typedef struct {
    ducknng_http_routes_bind_data *bind;
    idx_t offset;
} ducknng_http_routes_init_data;

typedef struct {
    idx_t row_count;
    char *service_name;
    char *listen;
    char *scheme;
    char *method;
    char *path;
    char *query_string;
    char *content_type;
    char *headers_json;
    uint64_t body_bytes;
    char *caller_identity;
    char *remote_addr;
    char *remote_ip;
    int32_t remote_port;
    uint64_t route_id;
    char *route_method;
    char *route_match_kind;
    char *route_path;
    char *path_params_json;
} ducknng_http_request_bind_data;

typedef struct {
    idx_t row_count;
    uint8_t *body;
    idx_t body_len;
    char *body_text;
} ducknng_http_request_body_bind_data;

typedef struct {
    int emitted;
} ducknng_sql_http_single_row_init_data;

static int execute_sql(duckdb_connection con, const char *sql) {
    duckdb_result result;
    if (duckdb_query(con, sql, &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return 0;
    }
    duckdb_destroy_result(&result);
    return 1;
}

static void ducknng_http_destroy_logical_types(duckdb_logical_type *types, idx_t count) {
    idx_t i;
    if (!types) return;
    for (i = 0; i < count; i++) duckdb_destroy_logical_type(&types[i]);
}

static size_t ducknng_http_json_escaped_len(const char *s) {
    size_t len = 0;
    size_t i;
    if (!s) return 0;
    for (i = 0; s[i]; i++) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
        case '\"':
        case '\\':
        case '\b':
        case '\f':
        case '\n':
        case '\r':
        case '\t':
            len += 2;
            break;
        default:
            if (c < 0x20) len += 6;
            else len += 1;
            break;
        }
    }
    return len;
}

static char *ducknng_http_json_escape_dup(const char *s) {
    static const char hex[] = "0123456789abcdef";
    char *out;
    size_t in_len;
    size_t out_len;
    size_t i;
    size_t j = 0;
    if (!s) return ducknng_strdup("");
    in_len = strlen(s);
    out_len = ducknng_http_json_escaped_len(s);
    out = (char *)duckdb_malloc(out_len + 1);
    if (!out) return NULL;
    for (i = 0; i < in_len; i++) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
        case '\"': out[j++] = '\\'; out[j++] = '\"'; break;
        case '\\': out[j++] = '\\'; out[j++] = '\\'; break;
        case '\b': out[j++] = '\\'; out[j++] = 'b'; break;
        case '\f': out[j++] = '\\'; out[j++] = 'f'; break;
        case '\n': out[j++] = '\\'; out[j++] = 'n'; break;
        case '\r': out[j++] = '\\'; out[j++] = 'r'; break;
        case '\t': out[j++] = '\\'; out[j++] = 't'; break;
        default:
            if (c < 0x20) {
                out[j++] = '\\';
                out[j++] = 'u';
                out[j++] = '0';
                out[j++] = '0';
                out[j++] = hex[(c >> 4) & 0x0f];
                out[j++] = hex[c & 0x0f];
            } else {
                out[j++] = (char)c;
            }
            break;
        }
    }
    out[j] = '\0';
    return out;
}

static void destroy_http_routes_bind_data(void *ptr) {
    ducknng_http_routes_bind_data *data = (ducknng_http_routes_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].service_name) duckdb_free(data->rows[i].service_name);
        if (data->rows[i].method) duckdb_free(data->rows[i].method);
        if (data->rows[i].match_kind) duckdb_free(data->rows[i].match_kind);
        if (data->rows[i].path) duckdb_free(data->rows[i].path);
        if (data->rows[i].handler_sql) duckdb_free(data->rows[i].handler_sql);
        if (data->rows[i].static_dir_path) duckdb_free(data->rows[i].static_dir_path);
        if (data->rows[i].auth_allow_identities_json) duckdb_free(data->rows[i].auth_allow_identities_json);
        if (data->rows[i].stream_content_type) duckdb_free(data->rows[i].stream_content_type);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_http_routes_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static int ducknng_http_routes_bind_reserve(ducknng_http_routes_bind_data *bind, idx_t want) {
    ducknng_http_route_row *next;
    idx_t new_cap = bind && bind->row_cap ? bind->row_cap * 2 : 8;
    if (!bind) return -1;
    if (!bind->rows) new_cap = want > 8 ? want : 8;
    if (bind->rows && bind->row_cap >= want) return 0;
    while (new_cap < want) new_cap *= 2;
    next = (ducknng_http_route_row *)duckdb_malloc(sizeof(*next) * (size_t)new_cap);
    if (!next) return -1;
    memset(next, 0, sizeof(*next) * (size_t)new_cap);
    if (bind->rows && bind->row_count > 0) {
        memcpy(next, bind->rows, sizeof(*next) * (size_t)bind->row_count);
        duckdb_free(bind->rows);
    }
    bind->rows = next;
    bind->row_cap = new_cap;
    return 0;
}

static void destroy_http_request_bind_data(void *ptr) {
    ducknng_http_request_bind_data *data = (ducknng_http_request_bind_data *)ptr;
    if (!data) return;
    if (data->service_name) duckdb_free(data->service_name);
    if (data->listen) duckdb_free(data->listen);
    if (data->scheme) duckdb_free(data->scheme);
    if (data->method) duckdb_free(data->method);
    if (data->path) duckdb_free(data->path);
    if (data->query_string) duckdb_free(data->query_string);
    if (data->content_type) duckdb_free(data->content_type);
    if (data->headers_json) duckdb_free(data->headers_json);
    if (data->caller_identity) duckdb_free(data->caller_identity);
    if (data->remote_addr) duckdb_free(data->remote_addr);
    if (data->remote_ip) duckdb_free(data->remote_ip);
    if (data->route_method) duckdb_free(data->route_method);
    if (data->route_match_kind) duckdb_free(data->route_match_kind);
    if (data->route_path) duckdb_free(data->route_path);
    if (data->path_params_json) duckdb_free(data->path_params_json);
    duckdb_free(data);
}

static void destroy_http_request_body_bind_data(void *ptr) {
    ducknng_http_request_body_bind_data *data = (ducknng_http_request_body_bind_data *)ptr;
    if (!data) return;
    if (data->body) duckdb_free(data->body);
    if (data->body_text) duckdb_free(data->body_text);
    duckdb_free(data);
}

static void destroy_sql_http_single_row_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static int ducknng_http_sql_reject_inside_request_handler(duckdb_function_info info, ducknng_sql_context *ctx,
    const char *what) {
    if (ctx && ctx->rt &&
        (ducknng_runtime_current_thread_request_service_get(ctx->rt) != NULL ||
         ducknng_runtime_current_request_service_get(ctx->rt) != NULL)) {
        duckdb_scalar_function_set_error(info, what);
        return 1;
    }
    return 0;
}

static int ducknng_http_sql_reject_table_inside_request_handler(duckdb_bind_info info,
    ducknng_sql_context *ctx, const char *what) {
    if (ctx && ctx->rt &&
        (ducknng_runtime_current_thread_request_service_get(ctx->rt) != NULL ||
         ducknng_runtime_current_request_service_get(ctx->rt) != NULL)) {
        duckdb_bind_set_error(info, what);
        return 1;
    }
    return 0;
}

typedef struct {
    char *const_name; /* pre-folded lookup name at bind time, or NULL if not constant */
} ducknng_http_lookup_bind_data;

static void ducknng_http_lookup_bind_data_destroy(void *data) {
    ducknng_http_lookup_bind_data *bd = (ducknng_http_lookup_bind_data *)data;
    if (!bd) return;
    if (bd->const_name) duckdb_free(bd->const_name);
    duckdb_free(bd);
}

static void *ducknng_http_lookup_bind_data_copy(void *data) {
    ducknng_http_lookup_bind_data *src = (ducknng_http_lookup_bind_data *)data;
    ducknng_http_lookup_bind_data *dst;
    if (!src) return NULL;
    dst = (ducknng_http_lookup_bind_data *)duckdb_malloc(sizeof(*dst));
    if (!dst) return NULL;
    dst->const_name = src->const_name ? ducknng_strdup(src->const_name) : NULL;
    return dst;
}

static void ducknng_http_lookup_bind_cb(duckdb_bind_info info) {
    ducknng_http_lookup_bind_data *bd;
    duckdb_expression name_expr;
    bd = (ducknng_http_lookup_bind_data *)duckdb_malloc(sizeof(*bd));
    if (!bd) { duckdb_scalar_function_bind_set_error(info, "ducknng: out of memory in bind"); return; }
    bd->const_name = NULL;
    name_expr = duckdb_scalar_function_bind_get_argument(info, 1);
    if (name_expr && duckdb_expression_is_foldable(name_expr)) {
        duckdb_client_context client_ctx = NULL;
        duckdb_value folded = NULL;
        duckdb_error_data err;
        duckdb_scalar_function_get_client_context(info, &client_ctx);
        err = duckdb_expression_fold(client_ctx, name_expr, &folded);
        duckdb_destroy_client_context(&client_ctx);
        if (err == NULL && folded != NULL) {
            char *name_str = duckdb_get_varchar(folded);
            if (name_str) {
                bd->const_name = ducknng_strdup(name_str);
                duckdb_free(name_str);
            }
        }
        if (err) duckdb_destroy_error_data(&err);
        if (folded) duckdb_destroy_value(&folded);
    }
    if (name_expr) duckdb_destroy_expression(&name_expr);
    duckdb_scalar_function_set_bind_data(info, bd, ducknng_http_lookup_bind_data_destroy);
    duckdb_scalar_function_set_bind_data_copy(info, ducknng_http_lookup_bind_data_copy);
}

static void ducknng_http_headers_get_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector headers_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector name_vec = duckdb_data_chunk_get_vector(input, 1);
    ducknng_http_lookup_bind_data *bd = (ducknng_http_lookup_bind_data *)duckdb_scalar_function_get_bind_data(info);
    int name_is_const = bd && bd->const_name;
    for (row = 0; row < count; row++) {
        char *headers_json = arg_varchar_dup(headers_vec, row);
        char *name = name_is_const ? bd->const_name : arg_varchar_dup(name_vec, row);
        char *value = NULL;
        char *errmsg = NULL;
        int rc;
        if (!headers_json || !name) {
            if (headers_json) duckdb_free(headers_json);
            if (!name_is_const && name) duckdb_free(name);
            set_null(output, row);
            continue;
        }
        rc = ducknng_http_headers_json_get_header(headers_json, name, 1, &value, &errmsg);
        if (rc < 0) {
            duckdb_free(headers_json);
            if (!name_is_const) duckdb_free(name);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid headers_json");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (value) {
            duckdb_unsafe_vector_assign_string_element_len(output, row, value, (idx_t)strlen(value));
            duckdb_free(value);
        } else {
            set_null(output, row);
        }
        if (errmsg) duckdb_free(errmsg);
        duckdb_free(headers_json);
        if (!name_is_const) duckdb_free(name);
    }
}

static void ducknng_http_query_param_get_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector query_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector name_vec = duckdb_data_chunk_get_vector(input, 1);
    ducknng_http_lookup_bind_data *bd = (ducknng_http_lookup_bind_data *)duckdb_scalar_function_get_bind_data(info);
    int name_is_const = bd && bd->const_name;
    for (row = 0; row < count; row++) {
        char *query_string = arg_varchar_dup(query_vec, row);
        char *name = name_is_const ? bd->const_name : arg_varchar_dup(name_vec, row);
        char *value = NULL;
        char *errmsg = NULL;
        int rc;
        if (!query_string || !name) {
            if (query_string) duckdb_free(query_string);
            if (!name_is_const && name) duckdb_free(name);
            set_null(output, row);
            continue;
        }
        rc = ducknng_query_string_get_param(query_string, name, 1, &value, &errmsg);
        if (rc < 0) {
            duckdb_free(query_string);
            if (!name_is_const) duckdb_free(name);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid query string");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (value) {
            duckdb_unsafe_vector_assign_string_element_len(output, row, value, (idx_t)strlen(value));
            duckdb_free(value);
        } else {
            set_null(output, row);
        }
        if (errmsg) duckdb_free(errmsg);
        duckdb_free(query_string);
        if (!name_is_const) duckdb_free(name);
    }
}

static void ducknng_http_cookie_get_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector cookie_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector name_vec = duckdb_data_chunk_get_vector(input, 1);
    ducknng_http_lookup_bind_data *bd = (ducknng_http_lookup_bind_data *)duckdb_scalar_function_get_bind_data(info);
    int name_is_const = bd && bd->const_name;
    for (row = 0; row < count; row++) {
        char *cookie_header = arg_varchar_dup(cookie_vec, row);
        char *name = name_is_const ? bd->const_name : arg_varchar_dup(name_vec, row);
        char *value = NULL;
        char *errmsg = NULL;
        int rc;
        if (!cookie_header || !name) {
            if (cookie_header) duckdb_free(cookie_header);
            if (!name_is_const && name) duckdb_free(name);
            set_null(output, row);
            continue;
        }
        rc = ducknng_cookie_header_get_value(cookie_header, name, 1, &value, &errmsg);
        if (rc < 0) {
            duckdb_free(cookie_header);
            if (!name_is_const) duckdb_free(name);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid Cookie header");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (value) {
            duckdb_unsafe_vector_assign_string_element_len(output, row, value, (idx_t)strlen(value));
            duckdb_free(value);
        } else {
            set_null(output, row);
        }
        if (errmsg) duckdb_free(errmsg);
        duckdb_free(cookie_header);
        if (!name_is_const) duckdb_free(name);
    }
}

static void ducknng_http_path_params_get_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    duckdb_vector params_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector name_vec = duckdb_data_chunk_get_vector(input, 1);
    ducknng_http_lookup_bind_data *bd = (ducknng_http_lookup_bind_data *)duckdb_scalar_function_get_bind_data(info);
    int name_is_const = bd && bd->const_name;
    for (row = 0; row < count; row++) {
        char *path_params_json = arg_varchar_dup(params_vec, row);
        char *name = name_is_const ? bd->const_name : arg_varchar_dup(name_vec, row);
        char *value = NULL;
        char *errmsg = NULL;
        int rc;
        if (!path_params_json || !name) {
            if (path_params_json) duckdb_free(path_params_json);
            if (!name_is_const && name) duckdb_free(name);
            set_null(output, row);
            continue;
        }
        rc = ducknng_json_object_get_string(path_params_json, name, 0, 1, &value, &errmsg);
        if (rc < 0) {
            duckdb_free(path_params_json);
            if (!name_is_const) duckdb_free(name);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: invalid path_params_json");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (value) {
            duckdb_unsafe_vector_assign_string_element_len(output, row, value, (idx_t)strlen(value));
            duckdb_free(value);
        } else {
            set_null(output, row);
        }
        if (errmsg) duckdb_free(errmsg);
        duckdb_free(path_params_json);
        if (!name_is_const) duckdb_free(name);
    }
}

/* Per-thread scratch state for ducknng_http_headers_build_scalar.
 * Holds reusable pointer arrays that grow on demand, avoiding per-row malloc/free
 * of the four pointer arrays themselves.  The strings they point to are still
 * allocated and freed per-row. */
typedef struct {
    char **names;
    char **values;
    char **escaped_names;
    char **escaped_values;
    idx_t cap;
} ducknng_headers_build_state;

static void ducknng_headers_build_state_destroy(void *ptr) {
    ducknng_headers_build_state *s = (ducknng_headers_build_state *)ptr;
    if (!s) return;
    if (s->names) duckdb_free(s->names);
    if (s->values) duckdb_free(s->values);
    if (s->escaped_names) duckdb_free(s->escaped_names);
    if (s->escaped_values) duckdb_free(s->escaped_values);
    duckdb_free(s);
}

static void ducknng_headers_build_init_cb(duckdb_init_info info) {
    ducknng_headers_build_state *s =
        (ducknng_headers_build_state *)duckdb_malloc(sizeof(*s));
    if (!s) {
        duckdb_scalar_function_init_set_error(info, "ducknng: out of memory in headers_build init");
        return;
    }
    memset(s, 0, sizeof(*s));
    duckdb_scalar_function_init_set_state(info, s, ducknng_headers_build_state_destroy);
}

static void ducknng_http_headers_build_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t row_count = duckdb_data_chunk_get_size(input);
    duckdb_vector names_vec = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector values_vec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_list_entry *name_entries = (duckdb_list_entry *)duckdb_vector_get_data(names_vec);
    duckdb_list_entry *value_entries = (duckdb_list_entry *)duckdb_vector_get_data(values_vec);
    duckdb_vector name_child = duckdb_list_vector_get_child(names_vec);
    duckdb_vector value_child = duckdb_list_vector_get_child(values_vec);
    ducknng_headers_build_state *state =
        (ducknng_headers_build_state *)duckdb_scalar_function_get_state(info);
    idx_t row;
    for (row = 0; row < row_count; row++) {
        duckdb_list_entry name_entry = name_entries[row];
        duckdb_list_entry value_entry = value_entries[row];
        char *json = NULL;
        size_t total_len = 2;
        idx_t i;
        char **names = NULL;
        char **values = NULL;
        char **escaped_names = NULL;
        char **escaped_values = NULL;
        int using_state = 0;
        char *out = NULL;
        size_t off = 0;
        int failed = 0;
        if (arg_is_null(names_vec, row) || arg_is_null(values_vec, row)) {
            set_null(output, row);
            continue;
        }
        if (name_entry.length != value_entry.length) {
            duckdb_scalar_function_set_error(info, "ducknng: headers_build requires names and values lists of equal length");
            return;
        }
        if (name_entry.length > 0) {
            if (state) {
                /* Grow the per-thread scratch arrays if needed. */
                if (state->cap < name_entry.length) {
                    idx_t newcap = name_entry.length + 8;
                    char **nn = (char **)duckdb_malloc(sizeof(*nn) * (size_t)newcap);
                    char **vv = (char **)duckdb_malloc(sizeof(*vv) * (size_t)newcap);
                    char **en = (char **)duckdb_malloc(sizeof(*en) * (size_t)newcap);
                    char **ev = (char **)duckdb_malloc(sizeof(*ev) * (size_t)newcap);
                    if (!nn || !vv || !en || !ev) {
                        if (nn) duckdb_free(nn);
                        if (vv) duckdb_free(vv);
                        if (en) duckdb_free(en);
                        if (ev) duckdb_free(ev);
                        duckdb_scalar_function_set_error(info, "ducknng: out of memory growing headers_build state");
                        return;
                    }
                    if (state->names) duckdb_free(state->names);
                    if (state->values) duckdb_free(state->values);
                    if (state->escaped_names) duckdb_free(state->escaped_names);
                    if (state->escaped_values) duckdb_free(state->escaped_values);
                    state->names = nn;
                    state->values = vv;
                    state->escaped_names = en;
                    state->escaped_values = ev;
                    state->cap = newcap;
                }
                names = state->names;
                values = state->values;
                escaped_names = state->escaped_names;
                escaped_values = state->escaped_values;
                using_state = 1;
                memset(names, 0, sizeof(*names) * (size_t)name_entry.length);
                memset(values, 0, sizeof(*values) * (size_t)name_entry.length);
                memset(escaped_names, 0, sizeof(*escaped_names) * (size_t)name_entry.length);
                memset(escaped_values, 0, sizeof(*escaped_values) * (size_t)name_entry.length);
            } else {
                /* Fallback: allocate per-row if no per-thread state. */
                names = (char **)duckdb_malloc(sizeof(*names) * (size_t)name_entry.length);
                values = (char **)duckdb_malloc(sizeof(*values) * (size_t)name_entry.length);
                escaped_names = (char **)duckdb_malloc(sizeof(*escaped_names) * (size_t)name_entry.length);
                escaped_values = (char **)duckdb_malloc(sizeof(*escaped_values) * (size_t)name_entry.length);
                if (!names || !values || !escaped_names || !escaped_values) {
                    if (names) duckdb_free(names);
                    if (values) duckdb_free(values);
                    if (escaped_names) duckdb_free(escaped_names);
                    if (escaped_values) duckdb_free(escaped_values);
                    duckdb_scalar_function_set_error(info, "ducknng: out of memory");
                    return;
                }
                memset(names, 0, sizeof(*names) * (size_t)name_entry.length);
                memset(values, 0, sizeof(*values) * (size_t)name_entry.length);
                memset(escaped_names, 0, sizeof(*escaped_names) * (size_t)name_entry.length);
                memset(escaped_values, 0, sizeof(*escaped_values) * (size_t)name_entry.length);
            }
        }
        for (i = 0; i < name_entry.length; i++) {
            idx_t name_idx = (idx_t)name_entry.offset + i;
            idx_t value_idx = (idx_t)value_entry.offset + i;
            names[i] = arg_varchar_dup(name_child, name_idx);
            values[i] = arg_varchar_dup(value_child, value_idx);
            if (!names[i] || !values[i]) {
                duckdb_scalar_function_set_error(info, "ducknng: headers_build requires non-null header names and values");
                failed = 1;
                goto cleanup_headers_build;
            }
            if (!ducknng_http_token_is_valid(names[i])) {
                duckdb_scalar_function_set_error(info, "ducknng: headers_build requires header names to be HTTP tokens");
                failed = 1;
                goto cleanup_headers_build;
            }
            if (!ducknng_http_header_value_is_valid(values[i])) {
                duckdb_scalar_function_set_error(info, "ducknng: headers_build requires header values without control characters");
                failed = 1;
                goto cleanup_headers_build;
            }
            escaped_names[i] = ducknng_http_json_escape_dup(names[i]);
            escaped_values[i] = ducknng_http_json_escape_dup(values[i]);
            if (!escaped_names[i] || !escaped_values[i]) {
                duckdb_scalar_function_set_error(info, "ducknng: out of memory");
                failed = 1;
                goto cleanup_headers_build;
            }
            total_len += strlen("{\"name\":\"\",\"value\":\"\"}") +
                strlen(escaped_names[i]) + strlen(escaped_values[i]);
            if (i + 1 < name_entry.length) total_len += 1;
        }
        out = (char *)duckdb_malloc(total_len + 1);
        if (!out) {
            duckdb_scalar_function_set_error(info, "ducknng: out of memory");
            failed = 1;
            goto cleanup_headers_build;
        }
        out[off++] = '[';
        for (i = 0; i < name_entry.length; i++) {
            int wrote = snprintf(out + off, total_len + 1 - off,
                "%s{\"name\":\"%s\",\"value\":\"%s\"}",
                i ? "," : "", escaped_names[i], escaped_values[i]);
            if (wrote < 0) {
                duckdb_free(out);
                out = NULL;
                duckdb_scalar_function_set_error(info, "ducknng: failed to build headers JSON");
                failed = 1;
                goto cleanup_headers_build;
            }
            off += (size_t)wrote;
        }
        out[off++] = ']';
        out[off] = '\0';
        json = out;
cleanup_headers_build:
        /* Always free the per-header strings; only free the pointer arrays if not using state. */
        if (names) {
            for (i = 0; i < name_entry.length; i++) {
                if (names[i]) duckdb_free(names[i]);
                if (values[i]) duckdb_free(values[i]);
                if (escaped_names[i]) duckdb_free(escaped_names[i]);
                if (escaped_values[i]) duckdb_free(escaped_values[i]);
            }
            if (!using_state) {
                duckdb_free(names);
                duckdb_free(values);
                duckdb_free(escaped_names);
                duckdb_free(escaped_values);
            }
        }
        if (failed) return;
        if (json) {
            duckdb_unsafe_vector_assign_string_element_len(output, row, json, (idx_t)strlen(json));
            duckdb_free(json);
        } else {
            duckdb_unsafe_vector_assign_string_element_len(output, row, "[]", (idx_t)strlen("[]"));
        }
    }
}

static void ducknng_register_http_route_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot register HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *path = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *handler_sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        uint64_t request_max_bytes = ncols > 4 ? arg_u64(duckdb_data_chunk_get_vector(input, 4), row, 0) : 0;
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !method || !path || !handler_sql) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, method, path, and handler_sql are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_register_http_route(svc, method, path, handler_sql,
                request_max_bytes, &errmsg) != 0) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register HTTP route");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(path);
        duckdb_free(handler_sql);
        out[row] = true;
    }
}

static void ducknng_add_stream_route_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot register HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *path = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *handler_sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        char *content_type = ncols > 4 ? arg_varchar_dup(duckdb_data_chunk_get_vector(input, 4), row) : NULL;
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !method || !path || !handler_sql) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (handler_sql) duckdb_free(handler_sql);
            if (content_type) duckdb_free(content_type);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, method, path, and handler_sql are required");
            return;
        }
        if (content_type && !ducknng_http_header_value_is_valid(content_type)) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (handler_sql) duckdb_free(handler_sql);
            if (content_type) duckdb_free(content_type);
            duckdb_scalar_function_set_error(info, "ducknng: stream content_type must not contain control characters");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_register_http_stream_route(svc, method, path, handler_sql,
                content_type, 0, &errmsg) != 0) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (handler_sql) duckdb_free(handler_sql);
            if (content_type) duckdb_free(content_type);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register stream route");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(path);
        duckdb_free(handler_sql);
        if (content_type) duckdb_free(content_type);
        out[row] = true;
    }
}

static void ducknng_unregister_http_route_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot unregister HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *path = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        ducknng_service *svc;
        char *errmsg = NULL;
        int removed;
        if (!ctx || !ctx->rt || !service_name || !method || !path) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, method, and path are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc) {
            duckdb_free(service_name);
            duckdb_free(method);
            duckdb_free(path);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        removed = ducknng_service_unregister_http_route(svc, method, path, &errmsg);
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(path);
        if (removed < 0) {
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to unregister HTTP route");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (errmsg) duckdb_free(errmsg);
        out[row] = removed ? true : false;
    }
}

static void ducknng_register_http_route_pattern_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot register HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *match_kind = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *path_pattern = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        char *handler_sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 4), row);
        uint64_t request_max_bytes = ncols > 5 ? arg_u64(duckdb_data_chunk_get_vector(input, 5), row, 0) : 0;
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !method || !match_kind ||
            !path_pattern || !handler_sql) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (match_kind) duckdb_free(match_kind);
            if (path_pattern) duckdb_free(path_pattern);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info,
                "ducknng: service_name, method, match_kind, path_pattern, and handler_sql are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_register_http_route_pattern(svc, method, match_kind,
                path_pattern, handler_sql, request_max_bytes, &errmsg) != 0) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (match_kind) duckdb_free(match_kind);
            if (path_pattern) duckdb_free(path_pattern);
            if (handler_sql) duckdb_free(handler_sql);
            duckdb_scalar_function_set_error(info,
                errmsg ? errmsg : "ducknng: failed to register HTTP route pattern");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(match_kind);
        duckdb_free(path_pattern);
        duckdb_free(handler_sql);
        out[row] = true;
    }
}

static void ducknng_unregister_http_route_pattern_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot unregister HTTP routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *match_kind = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        char *path_pattern = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 3), row);
        ducknng_service *svc;
        char *errmsg = NULL;
        int removed;
        if (!ctx || !ctx->rt || !service_name || !method || !match_kind || !path_pattern) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (match_kind) duckdb_free(match_kind);
            if (path_pattern) duckdb_free(path_pattern);
            duckdb_scalar_function_set_error(info,
                "ducknng: service_name, method, match_kind, and path_pattern are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc) {
            duckdb_free(service_name);
            duckdb_free(method);
            duckdb_free(match_kind);
            duckdb_free(path_pattern);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        removed = ducknng_service_unregister_http_route_pattern(svc, method, match_kind,
            path_pattern, &errmsg);
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(match_kind);
        duckdb_free(path_pattern);
        if (removed < 0) {
            duckdb_scalar_function_set_error(info,
                errmsg ? errmsg : "ducknng: failed to unregister HTTP route pattern");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (errmsg) duckdb_free(errmsg);
        out[row] = removed ? true : false;
    }
}

static void ducknng_list_http_routes_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_http_routes_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    idx_t row = 0;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_table_inside_request_handler(info, ctx,
            "ducknng: ducknng_list_http_routes() cannot run inside a request handler")) return;
    bind = (ducknng_http_routes_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    for (i = 0; i < ctx->rt->service_count; i++) {
        ducknng_service *svc = ctx->rt->services[i];
        ducknng_http_route *routes = NULL;
        size_t route_count = 0;
        size_t j;
        if (!svc) continue;
        if (ducknng_service_http_routes_snapshot(svc, &routes, &route_count, NULL) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            destroy_http_routes_bind_data(bind);
            duckdb_bind_set_error(info, "ducknng: failed to snapshot HTTP routes");
            return;
        }
        if (route_count > 0 && ducknng_http_routes_bind_reserve(bind, row + (idx_t)route_count) != 0) {
            ducknng_mutex_unlock(&ctx->rt->mu);
            ducknng_service_http_routes_free(routes, route_count);
            destroy_http_routes_bind_data(bind);
            duckdb_bind_set_error(info, "ducknng: out of memory");
            return;
        }
        for (j = 0; j < route_count; j++, row++) {
            bind->rows[row].service_id = svc->service_id;
            bind->rows[row].route_id = routes[j].route_id;
            bind->rows[row].request_max_bytes = routes[j].request_max_bytes;
            bind->rows[row].service_name = svc->name ? ducknng_strdup(svc->name) : NULL;
            bind->rows[row].method = routes[j].method ? ducknng_strdup(routes[j].method) : NULL;
            bind->rows[row].match_kind = ducknng_strdup(
                ducknng_http_route_match_kind_name(routes[j].match_kind));
            bind->rows[row].path = routes[j].path ? ducknng_strdup(routes[j].path) : NULL;
            bind->rows[row].handler_sql = routes[j].handler_sql ? ducknng_strdup(routes[j].handler_sql) : NULL;
            bind->rows[row].static_dir_path = routes[j].static_dir_path ? ducknng_strdup(routes[j].static_dir_path) : NULL;
            bind->rows[row].auth_require_identity = routes[j].auth_require_identity;
            bind->rows[row].auth_allow_identities_json = routes[j].auth_allow_identities_json ? ducknng_strdup(routes[j].auth_allow_identities_json) : NULL;
            bind->rows[row].response_mode = routes[j].response_mode;
            bind->rows[row].stream_content_type = routes[j].stream_content_type ? ducknng_strdup(routes[j].stream_content_type) : NULL;
            if ((svc->name && !bind->rows[row].service_name) ||
                (routes[j].method && !bind->rows[row].method) ||
                !bind->rows[row].match_kind ||
                (routes[j].path && !bind->rows[row].path) ||
                (routes[j].handler_sql && !bind->rows[row].handler_sql)) {
                ducknng_mutex_unlock(&ctx->rt->mu);
                ducknng_service_http_routes_free(routes, route_count);
                destroy_http_routes_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: out of memory");
                return;
            }
        }
        ducknng_service_http_routes_free(routes, route_count);
    }
    bind->row_count = row;
    ducknng_mutex_unlock(&ctx->rt->mu);

    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "service_id", type);
    duckdb_bind_add_result_column(info, "route_id", type);
    duckdb_bind_add_result_column(info, "request_max_bytes", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "method", type);
    duckdb_bind_add_result_column(info, "match_kind", type);
    duckdb_bind_add_result_column(info, "path", type);
    duckdb_bind_add_result_column(info, "handler_sql", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
    duckdb_bind_add_result_column(info, "auth_require_identity", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
     duckdb_bind_add_result_column(info, "static_dir_path", type);
     duckdb_bind_add_result_column(info, "auth_allow_identities_json", type);
     duckdb_destroy_logical_type(&type);
     type = duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);
     duckdb_bind_add_result_column(info, "is_stream", type);
     duckdb_destroy_logical_type(&type);
     type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
     duckdb_bind_add_result_column(info, "stream_content_type", type);
     duckdb_destroy_logical_type(&type);
     duckdb_bind_set_bind_data(info, bind, destroy_http_routes_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_list_http_routes_init(duckdb_init_info info) {
    ducknng_http_routes_bind_data *bind =
        (ducknng_http_routes_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_http_routes_init_data *init =
        (ducknng_http_routes_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_http_routes_init_data);
}

static void ducknng_list_http_routes_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_http_routes_init_data *init =
        (ducknng_http_routes_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_routes_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    uint64_t *service_ids;
    uint64_t *route_ids;
    uint64_t *request_max_bytes;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    service_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 0));
    route_ids = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 1));
    request_max_bytes = (uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 2));
    for (i = 0; i < chunk_size; i++) {
        ducknng_http_route_row *row = &bind->rows[init->offset + i];
        service_ids[i] = row->service_id;
        route_ids[i] = row->route_id;
        request_max_bytes[i] = row->request_max_bytes;
        if (row->service_name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 3), i, row->service_name, (idx_t)strlen(row->service_name));
        else set_null(duckdb_data_chunk_get_vector(output, 3), i);
        if (row->method) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 4), i, row->method, (idx_t)strlen(row->method));
        else set_null(duckdb_data_chunk_get_vector(output, 4), i);
        if (row->match_kind) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 5), i, row->match_kind, (idx_t)strlen(row->match_kind));
        else set_null(duckdb_data_chunk_get_vector(output, 5), i);
        if (row->path) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 6), i, row->path, (idx_t)strlen(row->path));
        else set_null(duckdb_data_chunk_get_vector(output, 6), i);
        if (row->handler_sql) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 7), i, row->handler_sql, (idx_t)strlen(row->handler_sql));
        else set_null(duckdb_data_chunk_get_vector(output, 7), i);
        ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 8)))[i] = (bool)row->auth_require_identity;
        if (row->static_dir_path) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 9), i, row->static_dir_path, (idx_t)strlen(row->static_dir_path));
        else set_null(duckdb_data_chunk_get_vector(output, 9), i);
        if (row->auth_allow_identities_json) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 10), i, row->auth_allow_identities_json, (idx_t)strlen(row->auth_allow_identities_json));
        else set_null(duckdb_data_chunk_get_vector(output, 10), i);
        ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 11)))[i] =
            (bool)(row->response_mode == DUCKNNG_HTTP_ROUTE_RESPONSE_STREAM);
        if (row->stream_content_type) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 12), i, row->stream_content_type, (idx_t)strlen(row->stream_content_type));
        else set_null(duckdb_data_chunk_get_vector(output, 12), i);
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static void ducknng_http_request_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    const ducknng_http_request_context *request_ctx = NULL;
    ducknng_http_request_bind_data *bind;
    duckdb_logical_type type;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_http_request_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    request_ctx = ducknng_runtime_current_thread_http_request_context_get(ctx->rt);
    if (request_ctx && request_ctx->svc) {
        bind->row_count = 1;
        bind->service_name = request_ctx->svc->name ? ducknng_strdup(request_ctx->svc->name) : NULL;
        bind->listen = ducknng_service_resolved_listen(request_ctx->svc) ?
            ducknng_strdup(ducknng_service_resolved_listen(request_ctx->svc)) : NULL;
        bind->scheme = ducknng_strdup(ducknng_transport_scheme_name(request_ctx->scheme));
        bind->method = request_ctx->method ? ducknng_strdup(request_ctx->method) : NULL;
        bind->path = request_ctx->path ? ducknng_strdup(request_ctx->path) : NULL;
        bind->query_string = request_ctx->query_string ? ducknng_strdup(request_ctx->query_string) : NULL;
        bind->content_type = request_ctx->content_type ? ducknng_strdup(request_ctx->content_type) : NULL;
        bind->headers_json = request_ctx->headers_json ? ducknng_strdup(request_ctx->headers_json) : NULL;
        bind->body_bytes = (uint64_t)request_ctx->body_len;
        bind->caller_identity = request_ctx->caller_identity ? ducknng_strdup(request_ctx->caller_identity) : NULL;
        bind->remote_addr = ducknng_sql_sockaddr_addr_dup(request_ctx->remote_addr, &bind->remote_ip, &bind->remote_port);
        bind->route_id = request_ctx->route.route_id;
        bind->route_method = request_ctx->route.method ? ducknng_strdup(request_ctx->route.method) : NULL;
        bind->route_match_kind = ducknng_strdup(
            ducknng_http_route_match_kind_name(request_ctx->route.match_kind));
        bind->route_path = request_ctx->route.path ? ducknng_strdup(request_ctx->route.path) : NULL;
        bind->path_params_json = request_ctx->path_params_json ?
            ducknng_strdup(request_ctx->path_params_json) : NULL;
    }
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "listen", type);
    duckdb_bind_add_result_column(info, "scheme", type);
    duckdb_bind_add_result_column(info, "method", type);
    duckdb_bind_add_result_column(info, "path", type);
    duckdb_bind_add_result_column(info, "query_string", type);
    duckdb_bind_add_result_column(info, "content_type", type);
    duckdb_bind_add_result_column(info, "headers_json", type);
    duckdb_bind_add_result_column(info, "caller_identity", type);
    duckdb_bind_add_result_column(info, "remote_addr", type);
    duckdb_bind_add_result_column(info, "remote_ip", type);
    duckdb_bind_add_result_column(info, "route_method", type);
    duckdb_bind_add_result_column(info, "route_match_kind", type);
    duckdb_bind_add_result_column(info, "route_path", type);
    duckdb_bind_add_result_column(info, "path_params_json", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "body_bytes", type);
    duckdb_bind_add_result_column(info, "route_id", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
    duckdb_bind_add_result_column(info, "remote_port", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_request_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_http_request_body_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    const ducknng_http_request_context *request_ctx = NULL;
    ducknng_http_request_body_bind_data *bind;
    duckdb_logical_type type;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    bind = (ducknng_http_request_body_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    request_ctx = ducknng_runtime_current_thread_http_request_context_get(ctx->rt);
    if (request_ctx) {
        bind->row_count = 1;
        bind->body_len = (idx_t)request_ctx->body_len;
        if (request_ctx->body_len > 0) {
            bind->body = (uint8_t *)duckdb_malloc(request_ctx->body_len);
            if (!bind->body) {
                destroy_http_request_body_bind_data(bind);
                duckdb_bind_set_error(info, "ducknng: out of memory");
                return;
            }
            memcpy(bind->body, request_ctx->body, request_ctx->body_len);
            if (ducknng_sql_bytes_look_text(request_ctx->body, request_ctx->body_len)) {
                bind->body_text = (char *)duckdb_malloc(request_ctx->body_len + 1);
                if (!bind->body_text) {
                    destroy_http_request_body_bind_data(bind);
                    duckdb_bind_set_error(info, "ducknng: out of memory");
                    return;
                }
                memcpy(bind->body_text, request_ctx->body, request_ctx->body_len);
                bind->body_text[request_ctx->body_len] = '\0';
            }
        }
    }
    type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
    duckdb_bind_add_result_column(info, "body", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "body_text", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_request_body_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_sql_http_single_row_init(duckdb_init_info info) {
    ducknng_sql_http_single_row_init_data *init =
        (ducknng_sql_http_single_row_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->emitted = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_sql_http_single_row_init_data);
}

static void ducknng_http_request_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sql_http_single_row_init_data *init =
        (ducknng_sql_http_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_request_bind_data *bind =
        (ducknng_http_request_bind_data *)duckdb_function_get_bind_data(info);
    if (!init || !bind || init->emitted || bind->row_count == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
#define ASSIGN_REQ_STRING(IDX, VALUE) do { \
        if ((VALUE)) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, (IDX)), 0, (VALUE), (idx_t)strlen((VALUE))); \
        else set_null(duckdb_data_chunk_get_vector(output, (IDX)), 0); \
    } while (0)
    ASSIGN_REQ_STRING(0, bind->service_name);
    ASSIGN_REQ_STRING(1, bind->listen);
    ASSIGN_REQ_STRING(2, bind->scheme);
    ASSIGN_REQ_STRING(3, bind->method);
    ASSIGN_REQ_STRING(4, bind->path);
    ASSIGN_REQ_STRING(5, bind->query_string);
    ASSIGN_REQ_STRING(6, bind->content_type);
    ASSIGN_REQ_STRING(7, bind->headers_json);
    ASSIGN_REQ_STRING(8, bind->caller_identity);
    ASSIGN_REQ_STRING(9, bind->remote_addr);
    ASSIGN_REQ_STRING(10, bind->remote_ip);
    ASSIGN_REQ_STRING(11, bind->route_method);
    ASSIGN_REQ_STRING(12, bind->route_match_kind);
    ASSIGN_REQ_STRING(13, bind->route_path);
    ASSIGN_REQ_STRING(14, bind->path_params_json);
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 15)))[0] = bind->body_bytes;
    ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 16)))[0] = bind->route_id;
    ((int32_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 17)))[0] = bind->remote_port;
#undef ASSIGN_REQ_STRING
    init->emitted = 1;
    duckdb_data_chunk_set_size(output, 1);
}

static void ducknng_http_request_body_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_sql_http_single_row_init_data *init =
        (ducknng_sql_http_single_row_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_request_body_bind_data *bind =
        (ducknng_http_request_body_bind_data *)duckdb_function_get_bind_data(info);
    if (!init || !bind || init->emitted || bind->row_count == 0) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    assign_blob(duckdb_data_chunk_get_vector(output, 0), 0,
        bind->body ? bind->body : (const uint8_t *)"", bind->body_len);
    if (bind->body_text) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), 0, bind->body_text, (idx_t)strlen(bind->body_text));
    else set_null(duckdb_data_chunk_get_vector(output, 1), 0);
    init->emitted = 1;
    duckdb_data_chunk_set_size(output, 1);
}

static int register_http_headers_build_scalar(duckdb_connection con, ducknng_sql_context *ctx,
    const char *name) {
    duckdb_logical_type child_type;
    duckdb_logical_type param_types[2];
    duckdb_logical_type return_type;
    duckdb_scalar_function f;
    int ok = 0;
    child_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    param_types[0] = duckdb_create_list_type(child_type);
    param_types[1] = duckdb_create_list_type(child_type);
    return_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    f = duckdb_create_scalar_function();
    if (f) {
        duckdb_scalar_function_set_name(f, name);
        duckdb_scalar_function_add_parameter(f, param_types[0]);
        duckdb_scalar_function_add_parameter(f, param_types[1]);
        duckdb_scalar_function_set_return_type(f, return_type);
        duckdb_scalar_function_set_function(f, ducknng_http_headers_build_scalar);
        duckdb_scalar_function_set_init(f, ducknng_headers_build_init_cb);
        duckdb_scalar_function_set_special_handling(f);
        duckdb_scalar_function_set_volatile(f);
        if (ducknng_set_scalar_sql_context(f, ctx)) {
            ok = (duckdb_register_scalar_function(con, f) != DuckDBError);
        }
        duckdb_destroy_scalar_function(&f);
    }
    duckdb_destroy_logical_type(&child_type);
    ducknng_http_destroy_logical_types(param_types, 2);
    duckdb_destroy_logical_type(&return_type);
    return ok;
}

static int register_http_response_macros(duckdb_connection con) {
    const char *response_sql =
        "CREATE OR REPLACE MACRO ducknng_http_response(status, headers_json, content_type, body, body_text) AS TABLE "
        "SELECT status AS status, headers_json AS headers_json, content_type AS content_type, body AS body, body_text AS body_text";
    const char *text_sql =
        "CREATE OR REPLACE MACRO ducknng_http_text(status, body_text) AS TABLE "
        "SELECT * FROM ducknng_http_response(status, NULL::VARCHAR, 'text/plain; charset=utf-8', NULL::BLOB, body_text)";
    const char *json_sql =
        "CREATE OR REPLACE MACRO ducknng_http_json(status, body_text) AS TABLE "
        "SELECT * FROM ducknng_http_response(status, NULL::VARCHAR, 'application/json; charset=utf-8', NULL::BLOB, body_text)";
    const char *binary_sql =
        "CREATE OR REPLACE MACRO ducknng_http_binary(status, body) AS TABLE "
        "SELECT * FROM ducknng_http_response(status, NULL::VARCHAR, 'application/octet-stream', body, NULL::VARCHAR)";
    const char *ndjson_sql =
        "CREATE OR REPLACE MACRO ducknng_http_ndjson(status, body_text) AS TABLE "
        "SELECT * FROM ducknng_http_response(status, NULL::VARCHAR, 'application/x-ndjson; charset=utf-8', NULL::BLOB, body_text)";
    /* ducknng_format_sse: format a server-sent event string for use as a stream route chunk.
       All arguments except data are optional (pass NULL to omit). */
    const char *sse_fmt_sql =
        "CREATE OR REPLACE MACRO ducknng_format_sse(data, event := NULL, id := NULL, retry := NULL) AS "
        "coalesce('event: ' || event || chr(10), '') || "
        "coalesce('id: ' || id || chr(10), '') || "
        "coalesce('retry: ' || retry || chr(10), '') || "
        "'data: ' || coalesce(data, '') || chr(10) || chr(10)";
    return execute_sql(con, response_sql) &&
        execute_sql(con, text_sql) &&
        execute_sql(con, json_sql) &&
        execute_sql(con, binary_sql) &&
        execute_sql(con, ndjson_sql) &&
        execute_sql(con, sse_fmt_sql);
}

static void ducknng_register_http_static_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot register static routes from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *path_prefix = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *dir_path = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !path_prefix || !dir_path) {
            if (service_name) duckdb_free(service_name);
            if (path_prefix) duckdb_free(path_prefix);
            if (dir_path) duckdb_free(dir_path);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, path_prefix, and dir_path are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc) {
            duckdb_free(service_name);
            duckdb_free(path_prefix);
            duckdb_free(dir_path);
            duckdb_scalar_function_set_error(info, "ducknng: service not found");
            return;
        }
        /* Register a prefix route with empty handler SQL; static_dir_path is set after */
        if (ducknng_service_register_http_route_pattern(svc, "GET", "prefix", path_prefix, "", 0, &errmsg) != 0) {
            duckdb_free(service_name);
            duckdb_free(path_prefix);
            duckdb_free(dir_path);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register static route");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        if (ducknng_service_set_http_route_auth(svc, "GET", path_prefix, 0, NULL, &errmsg) == 0) {
            /* set static_dir_path by finding the route we just registered */
            size_t j;
            ducknng_mutex_lock(&svc->mu);
            for (j = 0; j < svc->http_route_count; j++) {
                if (svc->http_routes[j].match_kind == DUCKNNG_HTTP_ROUTE_MATCH_PREFIX &&
                    svc->http_routes[j].method &&
                    strcmp(svc->http_routes[j].method, "GET") == 0 &&
                    svc->http_routes[j].path &&
                    strcmp(svc->http_routes[j].path, path_prefix) == 0) {
                    if (svc->http_routes[j].static_dir_path) duckdb_free(svc->http_routes[j].static_dir_path);
                    svc->http_routes[j].static_dir_path = dir_path;
                    dir_path = NULL;
                    break;
                }
            }
            ducknng_mutex_unlock(&svc->mu);
        }
        if (dir_path) duckdb_free(dir_path);
        duckdb_free(service_name);
        duckdb_free(path_prefix);
        out[row] = true;
    }
}

static void ducknng_set_http_route_auth_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t ncols = duckdb_data_chunk_get_column_count(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot set route auth from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *method = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *path = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        bool require_identity = ncols > 3 ? ((bool *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(input, 3)))[row] : false;
        char *allow_identities_json = ncols > 4 ? arg_varchar_dup(duckdb_data_chunk_get_vector(input, 4), row) : NULL;
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !method || !path) {
            if (service_name) duckdb_free(service_name);
            if (method) duckdb_free(method);
            if (path) duckdb_free(path);
            if (allow_identities_json) duckdb_free(allow_identities_json);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, method, and path are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_set_http_route_auth(svc, method, path,
                (int)require_identity, allow_identities_json, &errmsg) != 0) {
            duckdb_free(service_name);
            duckdb_free(method);
            duckdb_free(path);
            if (allow_identities_json) duckdb_free(allow_identities_json);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to set route auth");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(method);
        duckdb_free(path);
        if (allow_identities_json) duckdb_free(allow_identities_json);
        out[row] = true;
    }
}

static void ducknng_register_http_worker_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot register HTTP workers from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *worker_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        char *sql = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 2), row);
        uint64_t interval_ms = arg_u64(duckdb_data_chunk_get_vector(input, 3), row, 1000);
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !worker_name || !sql) {
            if (service_name) duckdb_free(service_name);
            if (worker_name) duckdb_free(worker_name);
            if (sql) duckdb_free(sql);
            duckdb_scalar_function_set_error(info, "ducknng: service_name, worker_name, and sql are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_register_http_worker(svc, worker_name, sql, interval_ms, &errmsg) != 0) {
            duckdb_free(service_name);
            duckdb_free(worker_name);
            duckdb_free(sql);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to register HTTP worker");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(worker_name);
        duckdb_free(sql);
        out[row] = true;
    }
}

static void ducknng_unregister_http_worker_scalar(duckdb_function_info info, duckdb_data_chunk input,
    duckdb_vector output) {
    idx_t count = duckdb_data_chunk_get_size(input);
    idx_t row;
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_scalar_function_get_extra_info(info);
    bool *out = (bool *)duckdb_vector_get_data(output);
    if (ducknng_reject_scalar_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_inside_request_handler(info, ctx,
            "ducknng: cannot unregister HTTP workers from a request handler")) return;
    for (row = 0; row < count; row++) {
        char *service_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 0), row);
        char *worker_name = arg_varchar_dup(duckdb_data_chunk_get_vector(input, 1), row);
        ducknng_service *svc;
        char *errmsg = NULL;
        if (!ctx || !ctx->rt || !service_name || !worker_name) {
            if (service_name) duckdb_free(service_name);
            if (worker_name) duckdb_free(worker_name);
            duckdb_scalar_function_set_error(info, "ducknng: service_name and worker_name are required");
            return;
        }
        svc = ducknng_runtime_find_service(ctx->rt, service_name);
        if (!svc || ducknng_service_unregister_http_worker(svc, worker_name, &errmsg) != 0) {
            duckdb_free(service_name);
            duckdb_free(worker_name);
            duckdb_scalar_function_set_error(info, errmsg ? errmsg : "ducknng: failed to unregister HTTP worker");
            if (errmsg) duckdb_free(errmsg);
            return;
        }
        duckdb_free(service_name);
        duckdb_free(worker_name);
        out[row] = true;
    }
}

typedef struct {
    char *service_name;
    char *worker_name;
    char *sql;
    uint64_t interval_ms;
} ducknng_http_worker_row;

typedef struct {
    ducknng_http_worker_row *rows;
    idx_t row_count;
    idx_t row_cap;
} ducknng_http_workers_bind_data;

typedef struct {
    ducknng_http_workers_bind_data *bind;
    idx_t offset;
} ducknng_http_workers_init_data;

static void destroy_http_workers_bind_data(void *ptr) {
    ducknng_http_workers_bind_data *data = (ducknng_http_workers_bind_data *)ptr;
    idx_t i;
    if (!data) return;
    for (i = 0; i < data->row_count; i++) {
        if (data->rows[i].service_name) duckdb_free(data->rows[i].service_name);
        if (data->rows[i].worker_name) duckdb_free(data->rows[i].worker_name);
        if (data->rows[i].sql) duckdb_free(data->rows[i].sql);
    }
    if (data->rows) duckdb_free(data->rows);
    duckdb_free(data);
}

static void destroy_http_workers_init_data(void *ptr) {
    if (ptr) duckdb_free(ptr);
}

static void ducknng_list_http_workers_bind(duckdb_bind_info info) {
    ducknng_sql_context *ctx = (ducknng_sql_context *)duckdb_bind_get_extra_info(info);
    ducknng_http_workers_bind_data *bind;
    duckdb_logical_type type;
    size_t i;
    if (!ctx || !ctx->rt) {
        duckdb_bind_set_error(info, "ducknng: missing runtime");
        return;
    }
    if (ducknng_reject_table_inside_authorizer(info, ctx)) return;
    if (ducknng_http_sql_reject_table_inside_request_handler(info, ctx,
            "ducknng: ducknng_list_http_workers() cannot run inside a request handler")) return;
    bind = (ducknng_http_workers_bind_data *)duckdb_malloc(sizeof(*bind));
    if (!bind) {
        duckdb_bind_set_error(info, "ducknng: out of memory");
        return;
    }
    memset(bind, 0, sizeof(*bind));
    ducknng_mutex_lock(&ctx->rt->mu);
    for (i = 0; i < ctx->rt->service_count; i++) {
        ducknng_service *svc = ctx->rt->services[i];
        ducknng_http_worker *workers = NULL;
        size_t worker_count = 0;
        size_t j;
        if (!svc) continue;
        if (ducknng_service_http_workers_snapshot(svc, &workers, &worker_count, NULL) != 0) continue;
        if (worker_count > 0) {
            ducknng_http_worker_row *new_rows;
            idx_t new_cap = bind->row_cap ? bind->row_cap * 2 : 8;
            while (new_cap < bind->row_count + (idx_t)worker_count) new_cap *= 2;
            if (!bind->rows || bind->row_cap < bind->row_count + (idx_t)worker_count) {
                new_rows = (ducknng_http_worker_row *)duckdb_malloc(sizeof(*new_rows) * (size_t)new_cap);
                if (!new_rows) {
                    ducknng_service_http_workers_free(workers, worker_count);
                    break;
                }
                memset(new_rows, 0, sizeof(*new_rows) * (size_t)new_cap);
                if (bind->rows) {
                    memcpy(new_rows, bind->rows, sizeof(*new_rows) * (size_t)bind->row_count);
                    duckdb_free(bind->rows);
                }
                bind->rows = new_rows;
                bind->row_cap = new_cap;
            }
        }
        for (j = 0; j < worker_count; j++) {
            bind->rows[bind->row_count].service_name = svc->name ? ducknng_strdup(svc->name) : NULL;
            bind->rows[bind->row_count].worker_name = workers[j].name ? ducknng_strdup(workers[j].name) : NULL;
            bind->rows[bind->row_count].sql = workers[j].sql ? ducknng_strdup(workers[j].sql) : NULL;
            bind->rows[bind->row_count].interval_ms = workers[j].interval_ms;
            bind->row_count++;
        }
        ducknng_service_http_workers_free(workers, worker_count);
    }
    ducknng_mutex_unlock(&ctx->rt->mu);

    type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_bind_add_result_column(info, "service_name", type);
    duckdb_bind_add_result_column(info, "worker_name", type);
    duckdb_bind_add_result_column(info, "sql", type);
    duckdb_destroy_logical_type(&type);
    type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_bind_add_result_column(info, "interval_ms", type);
    duckdb_destroy_logical_type(&type);
    duckdb_bind_set_bind_data(info, bind, destroy_http_workers_bind_data);
    duckdb_bind_set_cardinality(info, bind->row_count, true);
}

static void ducknng_list_http_workers_init(duckdb_init_info info) {
    ducknng_http_workers_bind_data *bind =
        (ducknng_http_workers_bind_data *)duckdb_init_get_bind_data(info);
    ducknng_http_workers_init_data *init =
        (ducknng_http_workers_init_data *)duckdb_malloc(sizeof(*init));
    if (!init) {
        duckdb_init_set_error(info, "ducknng: out of memory");
        return;
    }
    init->bind = bind;
    init->offset = 0;
    duckdb_init_set_max_threads(info, 1);
    duckdb_init_set_init_data(info, init, destroy_http_workers_init_data);
}

static void ducknng_list_http_workers_scan(duckdb_function_info info, duckdb_data_chunk output) {
    ducknng_http_workers_init_data *init =
        (ducknng_http_workers_init_data *)duckdb_function_get_init_data(info);
    ducknng_http_workers_bind_data *bind;
    idx_t remaining;
    idx_t chunk_size;
    idx_t i;
    if (!init || !init->bind || init->offset >= init->bind->row_count) {
        duckdb_data_chunk_set_size(output, 0);
        return;
    }
    bind = init->bind;
    remaining = bind->row_count - init->offset;
    chunk_size = remaining > duckdb_vector_size() ? duckdb_vector_size() : remaining;
    for (i = 0; i < chunk_size; i++) {
        ducknng_http_worker_row *row = &bind->rows[init->offset + i];
        if (row->service_name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 0), i, row->service_name, (idx_t)strlen(row->service_name));
        else set_null(duckdb_data_chunk_get_vector(output, 0), i);
        if (row->worker_name) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 1), i, row->worker_name, (idx_t)strlen(row->worker_name));
        else set_null(duckdb_data_chunk_get_vector(output, 1), i);
        if (row->sql) duckdb_unsafe_vector_assign_string_element_len(duckdb_data_chunk_get_vector(output, 2), i, row->sql, (idx_t)strlen(row->sql));
        else set_null(duckdb_data_chunk_get_vector(output, 2), i);
        ((uint64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(output, 3)))[i] = row->interval_ms;
    }
    init->offset += chunk_size;
    duckdb_data_chunk_set_size(output, chunk_size);
}

static int register_http_request_accessor_macros(duckdb_connection con) {
    const char *header_sql =
        "CREATE OR REPLACE MACRO ducknng_http_header(name) AS "
        "(SELECT ducknng_http_headers_get(headers_json, name) FROM ducknng_http_request())";
    const char *query_sql =
        "CREATE OR REPLACE MACRO ducknng_http_query_param(name) AS "
        "(SELECT ducknng_http_query_param_get(query_string, name) FROM ducknng_http_request())";
    const char *cookie_sql =
        "CREATE OR REPLACE MACRO ducknng_http_cookie(name) AS "
        "(SELECT ducknng_http_cookie_get(ducknng_http_headers_get(headers_json, 'Cookie'), name) FROM ducknng_http_request())";
    const char *path_sql =
        "CREATE OR REPLACE MACRO ducknng_http_path_param(name) AS "
        "(SELECT ducknng_http_path_params_get(path_params_json, name) FROM ducknng_http_request())";
    return execute_sql(con, header_sql) &&
        execute_sql(con, query_sql) &&
        execute_sql(con, cookie_sql) &&
        execute_sql(con, path_sql);
}

int ducknng_register_sql_http(duckdb_connection con, ducknng_sql_context *ctx) {
    duckdb_type route_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_types_with_limit[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type unregister_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_pattern_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_pattern_types_with_limit[6] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    duckdb_type unregister_pattern_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type two_varchar_types[2] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type three_varchar_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_auth3_types[3] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
    duckdb_type route_auth4_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN};
    duckdb_type route_auth5_types[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_VARCHAR};
    duckdb_type worker_types[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_UBIGINT};
    if (!ctx || !ctx->rt) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route", 4,
            ducknng_register_http_route_scalar, ctx, route_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route", 5,
            ducknng_register_http_route_scalar, ctx, route_types_with_limit, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route_pattern", 5,
            ducknng_register_http_route_pattern_scalar, ctx, route_pattern_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_route_pattern", 6,
            ducknng_register_http_route_pattern_scalar, ctx, route_pattern_types_with_limit, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_http_route", 3,
            ducknng_unregister_http_route_scalar, ctx, unregister_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_http_route_pattern", 4,
            ducknng_unregister_http_route_pattern_scalar, ctx, unregister_pattern_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    {
        /* ducknng_add_stream_route(service, method, path, sql [, content_type]) */
        duckdb_type stream_route_types4[4] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
        duckdb_type stream_route_types5[5] = {DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR, DUCKDB_TYPE_VARCHAR};
        if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_add_stream_route", 4,
                ducknng_add_stream_route_scalar, ctx, stream_route_types4, DUCKDB_TYPE_BOOLEAN)) return 0;
        if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_add_stream_route", 5,
                ducknng_add_stream_route_scalar, ctx, stream_route_types5, DUCKDB_TYPE_BOOLEAN)) return 0;
    }
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_static", 3,
            ducknng_register_http_static_scalar, ctx, three_varchar_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_http_route_auth", 3,
            ducknng_set_http_route_auth_scalar, ctx, route_auth3_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_http_route_auth", 4,
            ducknng_set_http_route_auth_scalar, ctx, route_auth4_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_set_http_route_auth", 5,
            ducknng_set_http_route_auth_scalar, ctx, route_auth5_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_register_http_worker", 4,
            ducknng_register_http_worker_scalar, ctx, worker_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR(con, "ducknng_unregister_http_worker", 2,
            ducknng_unregister_http_worker_scalar, ctx, two_varchar_types, DUCKDB_TYPE_BOOLEAN)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR_WITH_BIND(con, "ducknng_http_headers_get", 2,
            ducknng_http_headers_get_scalar, ducknng_http_lookup_bind_cb, ctx, two_varchar_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR_WITH_BIND(con, "ducknng_http_query_param_get", 2,
            ducknng_http_query_param_get_scalar, ducknng_http_lookup_bind_cb, ctx, two_varchar_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR_WITH_BIND(con, "ducknng_http_cookie_get", 2,
            ducknng_http_cookie_get_scalar, ducknng_http_lookup_bind_cb, ctx, two_varchar_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!DUCKNNG_REGISTER_VOLATILE_SCALAR_WITH_BIND(con, "ducknng_http_path_params_get", 2,
            ducknng_http_path_params_get_scalar, ducknng_http_lookup_bind_cb, ctx, two_varchar_types, DUCKDB_TYPE_VARCHAR)) return 0;
    if (!register_http_headers_build_scalar(con, ctx, "ducknng_http_headers_build")) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_list_http_routes", ctx, 0, NULL,
            ducknng_list_http_routes_bind, ducknng_list_http_routes_init, ducknng_list_http_routes_scan)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_list_http_workers", ctx, 0, NULL,
            ducknng_list_http_workers_bind, ducknng_list_http_workers_init, ducknng_list_http_workers_scan)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_http_request", ctx, 0, NULL,
            ducknng_http_request_bind, ducknng_sql_http_single_row_init, ducknng_http_request_scan)) return 0;
    if (!DUCKNNG_REGISTER_TABLE(con, "ducknng_http_request_body", ctx, 0, NULL,
            ducknng_http_request_body_bind, ducknng_sql_http_single_row_init, ducknng_http_request_body_scan)) return 0;
    if (!register_http_request_accessor_macros(con)) return 0;
    if (!register_http_response_macros(con)) return 0;
    return 1;
}
