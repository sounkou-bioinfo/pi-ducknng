#pragma once
#include "duckdb_extension.h"

/*
 * DuckDB's current C API exposes incremental result delivery for prepared
 * statements through the pending-streaming entrypoint. Keep that compatibility
 * decision in one small boundary so session and codec code have one internal
 * result-fetch contract. When DuckDB replaces the pending-streaming entrypoint,
 * update this file rather than adding branches at call sites.
 */
int ducknng_pending_prepared_for_session(duckdb_prepared_statement stmt,
    duckdb_pending_result *out_pending);
duckdb_data_chunk ducknng_result_fetch_session_chunk(duckdb_result result);
