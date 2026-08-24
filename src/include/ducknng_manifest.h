#pragma once
#include "ducknng_runtime.h"
#include "ducknng_registry.h"

extern const ducknng_method_descriptor ducknng_method_exec;
extern const ducknng_method_descriptor ducknng_method_manifest;
extern const ducknng_method_descriptor ducknng_method_upload_open;
extern const ducknng_method_descriptor ducknng_method_upload_append;
extern const ducknng_method_descriptor ducknng_method_upload_commit;
extern const ducknng_method_descriptor ducknng_method_upload_abort;
int ducknng_register_builtin_methods(ducknng_runtime *rt, char **errmsg);
int ducknng_register_exec_method(ducknng_runtime *rt, char **errmsg);
int ducknng_register_exec_method_with_auth(ducknng_runtime *rt, int requires_auth, char **errmsg);
/* Register the upload lane methods (upload_open/append/commit/abort). Gated
 * like the exec method: hosts opt in, so services expose no upload surface
 * unless registered. requires_auth mirrors ducknng_register_exec_method. */
int ducknng_register_upload_methods(ducknng_runtime *rt, char **errmsg);
int ducknng_register_upload_methods_with_auth(ducknng_runtime *rt, int requires_auth, char **errmsg);
