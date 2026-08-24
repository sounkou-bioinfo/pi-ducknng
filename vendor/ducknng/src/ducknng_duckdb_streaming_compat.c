#include "ducknng_duckdb_streaming_compat.h"

DUCKDB_EXTENSION_EXTERN

int ducknng_pending_prepared_for_session(duckdb_prepared_statement stmt,
    duckdb_pending_result *out_pending) {
#if defined(DUCKDB_API_NO_DEPRECATED)
#error "ducknng query sessions require DuckDB pending-result streaming; update ducknng_duckdb_streaming_compat.c for the replacement API"
#endif
    return duckdb_pending_prepared_streaming(stmt, out_pending);
}

duckdb_data_chunk ducknng_result_fetch_session_chunk(duckdb_result result) {
#if defined(DUCKDB_API_NO_DEPRECATED)
#error "ducknng query sessions require DuckDB stream fetch; update ducknng_duckdb_streaming_compat.c for the replacement API"
#endif
    return duckdb_stream_fetch_chunk(result);
}
