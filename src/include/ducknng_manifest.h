#pragma once
#include "ducknng_runtime.h"
#include "ducknng_registry.h"

extern const ducknng_method_descriptor ducknng_method_exec;
extern const ducknng_method_descriptor ducknng_method_manifest;
int ducknng_register_builtin_methods(ducknng_runtime *rt, char **errmsg);
int ducknng_register_exec_method(ducknng_runtime *rt, char **errmsg);
int ducknng_register_exec_method_with_auth(ducknng_runtime *rt, int requires_auth, char **errmsg);
