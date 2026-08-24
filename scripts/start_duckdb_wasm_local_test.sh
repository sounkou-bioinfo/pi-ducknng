#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PORT=${PORT:-8002}
HOST=${HOST:-127.0.0.1}
LOCAL_WASM_IMAGE=${LOCAL_WASM_IMAGE:-ducknng/duckdb-wasm-local:latest}
LOCAL_WASM_DOCKERFILE=${LOCAL_WASM_DOCKERFILE:-scripts/docker/duckdb-wasm-local.Dockerfile}
DUCKDB_WASM_PLATFORM=${DUCKDB_WASM_PLATFORM:-wasm_eh}
# Keep these pinned. The npm "latest" tag can move independently from the
# DuckDB C API version used to build the extension and can produce opaque wasm
# dynamic-link errors such as env.* signature mismatches. The current pinned
# duckdb-wasm dev package reports DuckDB v1.5.3 in the browser runtime.
DUCKDB_WASM_NPM_VERSION=${DUCKDB_WASM_NPM_VERSION:-1.33.1-dev55.0}
DUCKDB_WASM_DUCKDB_VERSION=${DUCKDB_WASM_DUCKDB_VERSION:-v1.5.3}
DUCKDB_WASM_DUCKDB_PY_VERSION=${DUCKDB_WASM_DUCKDB_VERSION#v}
ARTIFACT_ROOT=${ARTIFACT_ROOT:-${ROOT_DIR}/.duckdb-wasm-local-artifacts}
SITE_ROOT=${SITE_ROOT:-${ARTIFACT_ROOT}/site}
WASM_BUILD_DIR=${WASM_BUILD_DIR:-${ARTIFACT_ROOT}/build/${DUCKDB_WASM_PLATFORM}/extension/ducknng}
DOCKER_WORK_ROOT=${DOCKER_WORK_ROOT:-${ROOT_DIR}/.duckdb_wasm_docker_work}
DOCKER_REBUILD_IMAGE=${DOCKER_REBUILD_IMAGE:-0}
DUCKNNG_WASM_BUILD_ONLY=${DUCKNNG_WASM_BUILD_ONLY:-0}
DUCKNNG_WASM_TRACE=${DUCKNNG_WASM_TRACE:-0}
DUCKNNG_WASM_INPROC_ONLY=${DUCKNNG_WASM_INPROC_ONLY:-1}
DUCKNNG_WASM_SERVE=${DUCKNNG_WASM_SERVE:-1}
DUCKNNG_WASM_EXTENSION_RELEASE_URL=${DUCKNNG_WASM_EXTENSION_RELEASE_URL:-}
DUCKNNG_WASM_EXTENSION_VERSION=${DUCKNNG_WASM_EXTENSION_VERSION:-}
DUCKNNG_SOURCE_COMMIT=${DUCKNNG_SOURCE_COMMIT:-}
if [ -z "${DUCKNNG_SOURCE_COMMIT}" ]; then
  DUCKNNG_SOURCE_COMMIT=$(git -C "${ROOT_DIR}" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)
fi

case "${DUCKDB_WASM_PLATFORM}" in
  wasm_eh|wasm_threads) ;;
  *)
    echo "Unsupported DUCKDB_WASM_PLATFORM=${DUCKDB_WASM_PLATFORM}" >&2
    exit 1
    ;;
esac

case "${DUCKDB_WASM_PLATFORM}" in
  wasm_eh)
    DUCKDB_WASM_RUNTIME_WORKER="duckdb-browser-eh.worker.js"
    DUCKDB_WASM_RUNTIME_WASM="duckdb-eh.wasm"
    DUCKDB_WASM_RUNTIME_PTHREAD_WORKER=""
    DUCKDB_WASM_REQUIRES_COI=0
    ;;
  wasm_threads)
    DUCKDB_WASM_RUNTIME_WORKER="duckdb-browser-coi.worker.js"
    DUCKDB_WASM_RUNTIME_WASM="duckdb-coi.wasm"
    DUCKDB_WASM_RUNTIME_PTHREAD_WORKER="duckdb-browser-coi.pthread.worker.js"
    DUCKDB_WASM_REQUIRES_COI=1
    ;;
esac

echo "Building ducknng ${DUCKDB_WASM_PLATFORM} extension for duckdb-wasm..."

if [ "${DOCKER_REBUILD_IMAGE}" = "1" ] || \
   ! docker image inspect "${LOCAL_WASM_IMAGE}" >/dev/null 2>&1; then
  docker build -f "${ROOT_DIR}/${LOCAL_WASM_DOCKERFILE}" \
    -t "${LOCAL_WASM_IMAGE}" "${ROOT_DIR}"
fi

mkdir -p "${DOCKER_WORK_ROOT}"
rsync -a --delete \
  --exclude '.git/' \
  --exclude 'build/' \
  --exclude 'cmake_build/' \
  --exclude 'configure/' \
  --exclude '.duckdb-wasm-local-artifacts/' \
  --exclude '.duckdb_wasm_docker_work/' \
  --exclude 'test/bin/' \
  "${ROOT_DIR}/" "${DOCKER_WORK_ROOT}/"
rm -rf "${DOCKER_WORK_ROOT}/.git"
git -C "${DOCKER_WORK_ROOT}" init -q
git -C "${DOCKER_WORK_ROOT}" config user.email docker@example.invalid
git -C "${DOCKER_WORK_ROOT}" config user.name docker
git -C "${DOCKER_WORK_ROOT}" add -A
git -C "${DOCKER_WORK_ROOT}" commit -qm docker-context
rm -rf \
  "${DOCKER_WORK_ROOT}/configure" \
  "${DOCKER_WORK_ROOT}/build" \
  "${DOCKER_WORK_ROOT}/cmake_build"

docker run --rm -v "${DOCKER_WORK_ROOT}:/work" -w /work \
  "${LOCAL_WASM_IMAGE}" bash -lc "
set -euo pipefail
source /opt/emsdk/emsdk_env.sh
git config --global --add safe.directory /work
make DUCKDB_PLATFORM=${DUCKDB_WASM_PLATFORM} \
  DUCKNNG_WASM_TRACE=${DUCKNNG_WASM_TRACE} \
  DUCKNNG_WASM_INPROC_ONLY=${DUCKNNG_WASM_INPROC_ONLY} \
  TARGET_DUCKDB_VERSION=${DUCKDB_WASM_DUCKDB_VERSION} \
  DUCKDB_HEADER_VERSION=${DUCKDB_WASM_DUCKDB_VERSION} \
  DUCKDB_TEST_VERSION=${DUCKDB_WASM_DUCKDB_PY_VERSION} \
  EXTENSION_VERSION=${DUCKNNG_WASM_EXTENSION_VERSION} \
  venv update_duckdb_headers platform extension_version release move_wasm_extension
/opt/emsdk/upstream/bin/wasm-dis \
  build/${DUCKDB_WASM_PLATFORM}/extension/ducknng/ducknng.duckdb_extension.wasm \
  > /tmp/ducknng-extension.wat
grep -q ducknng_init_c_api /tmp/ducknng-extension.wat
if grep -Eq '\(import \"env\" \"(nng_|nni_|mbedtls_|mbedx509_|mbedcrypto_)' \
    /tmp/ducknng-extension.wat; then
  echo 'ducknng wasm extension still imports bundled dependency symbols' >&2
  grep -E '\(import \"env\" \"(nng_|nni_|mbedtls_|mbedx509_|mbedcrypto_)' \
    /tmp/ducknng-extension.wat >&2
  exit 1
fi
"

mkdir -p "${WASM_BUILD_DIR}"
cp -f "${DOCKER_WORK_ROOT}/build/${DUCKDB_WASM_PLATFORM}/extension/ducknng/ducknng.duckdb_extension.wasm" \
      "${WASM_BUILD_DIR}/ducknng.duckdb_extension.wasm"

WASM_EXT="${WASM_BUILD_DIR}/ducknng.duckdb_extension.wasm"
if [ ! -f "${WASM_EXT}" ]; then
  echo "Expected wasm extension not found: ${WASM_EXT}" >&2
  exit 1
fi

if [ "${DUCKNNG_WASM_BUILD_ONLY}" = "1" ]; then
  echo "Built: ${WASM_EXT}"
  exit 0
fi

rm -rf "${SITE_ROOT}"
mkdir -p "${SITE_ROOT}/scripts" "${SITE_ROOT}/duckdb-wasm"

RUNTIME_BASE="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${DUCKDB_WASM_NPM_VERSION}/dist"
curl -fsSL "${RUNTIME_BASE}/duckdb-browser.mjs" -o "${SITE_ROOT}/duckdb-browser.mjs"
curl -fsSL "${RUNTIME_BASE}/${DUCKDB_WASM_RUNTIME_WORKER}" \
  -o "${SITE_ROOT}/${DUCKDB_WASM_RUNTIME_WORKER}"
curl -fsSL "${RUNTIME_BASE}/${DUCKDB_WASM_RUNTIME_WASM}" \
  -o "${SITE_ROOT}/${DUCKDB_WASM_RUNTIME_WASM}"
curl -fsSL "${RUNTIME_BASE}/${DUCKDB_WASM_RUNTIME_WORKER}.map" \
  -o "${SITE_ROOT}/${DUCKDB_WASM_RUNTIME_WORKER}.map" || true
if [ -n "${DUCKDB_WASM_RUNTIME_PTHREAD_WORKER}" ]; then
  curl -fsSL "${RUNTIME_BASE}/${DUCKDB_WASM_RUNTIME_PTHREAD_WORKER}" \
    -o "${SITE_ROOT}/${DUCKDB_WASM_RUNTIME_PTHREAD_WORKER}"
  curl -fsSL "${RUNTIME_BASE}/${DUCKDB_WASM_RUNTIME_PTHREAD_WORKER}.map" \
    -o "${SITE_ROOT}/${DUCKDB_WASM_RUNTIME_PTHREAD_WORKER}.map" || true
  DUCKDB_WASM_PTHREAD_WORKER_JSON="\"${DUCKDB_WASM_RUNTIME_PTHREAD_WORKER}\""
else
  DUCKDB_WASM_PTHREAD_WORKER_JSON="null"
fi
cat > "${SITE_ROOT}/ducknng-wasm-config.json" <<EOF
{
  "platform": "${DUCKDB_WASM_PLATFORM}",
  "worker": "${DUCKDB_WASM_RUNTIME_WORKER}",
  "wasm": "${DUCKDB_WASM_RUNTIME_WASM}",
  "pthreadWorker": ${DUCKDB_WASM_PTHREAD_WORKER_JSON},
  "requiresCrossOriginIsolation": ${DUCKDB_WASM_REQUIRES_COI},
  "coiServiceWorker": "ducknng-coi-service-worker.js",
  "duckdbWasmNpmVersion": "${DUCKDB_WASM_NPM_VERSION}",
  "duckdbVersion": "${DUCKDB_WASM_DUCKDB_VERSION}",
  "nngInprocOnly": ${DUCKNNG_WASM_INPROC_ONLY},
  "sourceCommit": "${DUCKNNG_SOURCE_COMMIT}",
  "extensionReleaseUrl": "${DUCKNNG_WASM_EXTENSION_RELEASE_URL}"
}
EOF
touch "${SITE_ROOT}/.nojekyll"
printf '%s\n' '<!doctype html><title>ducknng</title>' > "${SITE_ROOT}/favicon.ico"

cat > "${SITE_ROOT}/index.html" <<'EOF'
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ducknng browser wasm demo</title>
  <style>
    body { margin: 0; font-family: system-ui, sans-serif; background: #f3efe4; color: #1a1a1a; }
    main { max-width: 760px; margin: 0 auto; padding: 40px 20px; }
    a.button { display: inline-block; margin: 12px 0; padding: 10px 16px; border-radius: 999px; background: #0f766e; color: white; text-decoration: none; }
    code { background: rgba(15, 118, 110, 0.08); padding: 1px 5px; border-radius: 6px; }
  </style>
</head>
<body>
  <main>
    <h1>ducknng browser wasm demo</h1>
    <p>
      This static site loads the ducknng DuckDB extension as a duckdb-wasm side module
      and runs the browser smoke page. The current public demo proves extension load,
      codec/registry calls, and the <code>inproc://</code> AIO RPC manifest path when
      the pthread runtime has cross-origin isolation.
    </p>
    <p>
      It does not claim browser support for <code>ipc://</code>, raw <code>tcp://</code>,
      native <code>tls+tcp://</code>, HTTP, or WebSocket transports.
    </p>
    <p><a class="button" href="scripts/duckdb-wasm-local-test.html">Open the smoke page</a></p>
  </main>
</body>
</html>
EOF

cat > "${SITE_ROOT}/ducknng-coi-service-worker.js" <<'JS'
self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (event) => {
  if (event.request.cache === "only-if-cached" && event.request.mode !== "same-origin") {
    return;
  }

  event.respondWith((async () => {
    const response = await fetch(event.request);
    if (response.type === "opaque") {
      return response;
    }

    const headers = new Headers(response.headers);
    headers.set("Cross-Origin-Opener-Policy", "same-origin");
    headers.set("Cross-Origin-Embedder-Policy", "require-corp");
    if (new URL(event.request.url).origin === self.location.origin) {
      headers.set("Cross-Origin-Resource-Policy", "same-origin");
    }

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  })());
});
JS

cp -f "${ROOT_DIR}/scripts/duckdb-wasm-local-test.html" \
  "${SITE_ROOT}/scripts/duckdb-wasm-local-test.html"
cp -f "${WASM_EXT}" "${SITE_ROOT}/duckdb-wasm/ducknng.duckdb_extension.wasm"

cat > "${SITE_ROOT}/serve_no_cache.py" <<'PY'
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

class NoCacheHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        if os.environ.get("DUCKNNG_WASM_COI") == "1":
            self.send_header("Cross-Origin-Opener-Policy", "same-origin")
            self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
            self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        super().end_headers()

if __name__ == "__main__":
    import os

    host = os.environ.get("DUCKNNG_WASM_HOST", "127.0.0.1")
    port = int(os.environ.get("DUCKNNG_WASM_PORT", "8002"))
    ThreadingHTTPServer((host, port), NoCacheHandler).serve_forever()
PY

cat <<EOF
Built:
  ${WASM_EXT}

Using duckdb-wasm npm package:
  @duckdb/duckdb-wasm@${DUCKDB_WASM_NPM_VERSION}

Building extension against DuckDB:
  ${DUCKDB_WASM_DUCKDB_VERSION}

Open in a browser:
  http://${HOST}:${PORT}/scripts/duckdb-wasm-local-test.html

Artifact roots:
  ${ARTIFACT_ROOT}
  ${DOCKER_WORK_ROOT}

Served files:
  /index.html
  /duckdb-browser.mjs
  /${DUCKDB_WASM_RUNTIME_WORKER}
  /${DUCKDB_WASM_RUNTIME_WASM}
  /ducknng-wasm-config.json
  /ducknng-coi-service-worker.js
  /duckdb-wasm/ducknng.duckdb_extension.wasm

The local server sends Cache-Control: no-store for all files.
If DUCKDB_WASM_PLATFORM=wasm_threads, it also sends COOP/COEP headers.
EOF

if [ "${DUCKNNG_WASM_SERVE}" = "0" ]; then
  echo "DUCKNNG_WASM_SERVE=0; static site staged at ${SITE_ROOT}"
  exit 0
fi

cd "${SITE_ROOT}"
DUCKNNG_WASM_HOST="${HOST}" DUCKNNG_WASM_PORT="${PORT}" \
  DUCKNNG_WASM_COI="${DUCKDB_WASM_REQUIRES_COI}" \
  exec python3 serve_no_cache.py
