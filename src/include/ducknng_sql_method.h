#pragma once
#include "ducknng_registry.h"
#include <stddef.h>

struct ducknng_runtime;

/* Registers or replaces a SQL-defined RPC method. The caller holds rt->mu.
 * A built-in method with the same name is never replaced. */
int ducknng_runtime_register_sql_method(struct ducknng_runtime *rt, const char *name,
    const char *handler_sql, const char *request_schema_json, int requires_auth,
    char **errmsg);

/* Frees every SQL method entry. Called only while destroying the runtime,
 * after its services have stopped. */
void ducknng_runtime_sql_methods_destroy(struct ducknng_runtime *rt);

/* Returns 1 when text is exactly one syntactically valid JSON object. */
int ducknng_json_is_object(const char *text, size_t len);

/* The request whose SQL method handler is running on this thread, or NULL.
 * The handler publishes it only while its statements execute. */
const ducknng_request_context *ducknng_sql_method_current_request(void);
