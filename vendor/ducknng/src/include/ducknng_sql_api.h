#pragma once
#include "duckdb_extension.h"
#include "ducknng_runtime.h"

int ducknng_register_sql_api(duckdb_connection connection, ducknng_runtime *rt);
