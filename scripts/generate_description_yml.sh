#!/usr/bin/env bash
set -euo pipefail
# Generate description.yml for DuckDB community-extension submission.
# Run from the repo root:
#   bash scripts/generate_description_yml.sh > description.yml
#
# With --render, run the hello_world SQL through DuckDB and include output:
#   make release && bash scripts/generate_description_yml.sh --render > description.yml

EXTENSION_NAME="ducknng"
GIT_REF=$(git rev-parse HEAD)
DESCRIPTION="Pure C DuckDB extension exposing a DuckDB-backed SQL and RPC server over NNG using Arrow IPC — with framed RPC, custom HTTP routes, TLS support, and a body codec layer"
VERSION="0.1.1"
LANGUAGE="C"
BUILD="cmake"
LICENSE="MIT"
MAINTAINER="sounkou-bioinfo"

HELLO_SQL=$(cat <<'SQLEOF'
LOAD ducknng;
SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);
SELECT * FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);
SELECT ducknng_stop_server('demo');
SQLEOF
)

render_hello() {
    local ext_path="$(pwd)/build/release/ducknng.duckdb_extension"
    if [ ! -f "$ext_path" ]; then
        non_render_hello
        return
    fi
    # Write SQL to temp file, run once, then parse output line by line
    local tmpf
    tmpf=$(mktemp)
    printf '%s\n' "${HELLO_SQL}" > "$tmpf"
    /usr/local/bin/duckdb152 -unsigned -c "$(cat "$tmpf")" 2>&1 > "${tmpf}.out"
    local output
    output=$(cat "${tmpf}.out")
    rm -f "$tmpf" "${tmpf}.out"

    # Extract the three box-drawing tables
    local t1 t2 t3
    t1=$(echo "$output" | awk '/^┌/{found++; if(found==1) print; next} found==1{print; if(/^└/) exit}')
    t2=$(echo "$output" | awk '/^┌/{found++; if(found==2) print; next} found==2{print; if(/^└/) exit}')
    t3=$(echo "$output" | awk '/^┌/{found++; if(found==3) print; next} found==3{print; if(/^└/) exit}')

    echo "    -- Load the extension"
    echo "    LOAD ducknng;"
    echo ""
    echo "    -- Start an inproc REP server and run SQL"
    echo "    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);"
    echo ""
    echo "$t1" | sed 's/^/    /'
    echo ""
    echo "    -- ducknng_query_rpc returns the actual result rows"
    echo "    SELECT *"
    echo "    FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);"
    echo ""
    echo "$t2" | sed 's/^/    /'
    echo ""
    echo "    SELECT ducknng_stop_server('demo');"
    echo ""
    echo "$t3" | sed 's/^/    /'
}

non_render_hello() {
    echo "    -- Load the extension"
    echo "    LOAD ducknng;"
    echo ""
    echo "    -- Start an inproc REP server and run SQL"
    echo "    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);"
    echo ""
    echo "    -- ducknng_query_rpc returns the actual result rows"
    echo "    SELECT *"
    echo "    FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);"
    echo ""
    echo "    SELECT ducknng_stop_server('demo');"
}

HELLO_TEXT=""
if [ "${1:-}" = "--render" ]; then
    HELLO_TEXT=$(render_hello)
else
    HELLO_TEXT="    -- Load the extension
    LOAD ducknng;

    -- Start an inproc REP server and run SQL
    SELECT ducknng_start_server('demo', 'inproc://ducknng_demo', 1, 134217728, 30000, 0::UBIGINT);

    -- ducknng_query_rpc returns the actual result rows
    SELECT *
    FROM ducknng_query_rpc('inproc://ducknng_demo', 'SELECT 42 AS answer', 0::UBIGINT);

    SELECT ducknng_stop_server('demo');"
fi

cat <<YAML
extension:
  name: ${EXTENSION_NAME}
  description: ${DESCRIPTION}
  version: ${VERSION}
  language: ${LANGUAGE}
  build: ${BUILD}
  license: ${LICENSE}
  requires_toolchains: "python3"
  excluded_platforms: wasm_mvp;wasm_eh;wasm_threads
  maintainers:
    - "${MAINTAINER}"

repo:
  github: ${MAINTAINER}/${EXTENSION_NAME}
  ref: ${GIT_REF}

docs:
  hello_world: |
${HELLO_TEXT}
  extended_description: |
    ducknng is a pure C DuckDB extension that exposes a DuckDB-backed SQL and
    RPC server over NNG (Nanomsg Next Generation) using Arrow IPC with nanoarrow C
    for payload encoding and decoding.

    **Transport layer (NNG)**
    Supports inproc://, ipc://, tcp://, and tls+tcp:// URLs. TLS certificates
    can be loaded from file paths or in-memory PEM content; self-signed dev
    certificates are generated entirely inside the extension (no file I/O).

    **Framed RPC**
    Versioned request/reply envelope with manifest, exec, query session
    (open/fetch/close/cancel), and raw unary operations. All tabular data
    is encoded as Arrow IPC streams.

    **HTTP carrier**
    Start a server on an http:// or https:// URL for the framed RPC mount.
    Register custom HTTP routes (exact, prefix, or template matching) backed
    by SQL queries. Streaming chunked routes for Server-Sent Events are
    supported via ducknng_add_stream_route. Static asset serving, route-local
    auth policies, and background workers are also available.

    **Body codec layer**
    Parse HTTP response bodies by content type: JSON, NDJSON, CSV, TSV,
    Parquet, Arrow IPC, form-urlencoded, and ducknng frames. Standalone
    ducknng_parse_csv(body), ducknng_parse_tsv(body), and
    ducknng_parse_parquet(body) functions use DuckDB's standard readers
    via a tempfile round-trip. User-registered codec hooks extend the set.

    **Admission & security**
    mTLS peer-identity extraction, exact identity allowlists, IP/CIDR
    allowlists, per-service and per-principal resource limits (max memory,
    max sessions, max result bytes), and SQL authorizer callbacks.

    **Development & testing**
    Built against the DuckDB C API (no C++). Uses DuckDB's stable and
    unstable C extensions API for Arrow conversion. 20+ SQL integration
    tests run via sqllogictest. Cross-platform (Linux, macOS, Windows)
    via the extension-ci-tools CMake build system.

    Project details and examples: https://github.com/sounkou-bioinfo/ducknng

    Community package excludes WASM targets (NNG threading requirement).
YAML
