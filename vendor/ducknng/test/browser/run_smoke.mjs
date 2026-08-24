// Real-browser smoke test runner for the ducknng duckdb-wasm side module.
//
// The runner serves a staged duckdb-wasm site with COOP/COEP headers, drives the
// smoke page in headless Chromium through Playwright, and then runs explicit
// probes through the page's test API and SQL shell. Keep probes explicit: the
// default proves only extension load + one shell query. Transport probes must be
// requested by name.
//
// Usage:
//   node test/browser/run_smoke.mjs [siteDir]
//   node test/browser/run_smoke.mjs [siteDir] --probes=conformance
//
// Environment:
//   DUCKNNG_BROWSER_PROBES=load,http-sync
//   BROWSER_DEBUG=1

import { createServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { readFile, stat } from "node:fs/promises";
import { dirname, resolve, extname, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { WebSocketServer } from "ws";

const DEFAULT_SITE_DIR = ".duckdb-wasm-local-artifacts/site";
const SMOKE_PATH = "/scripts/duckdb-wasm-local-test.html";
const KNOWN_PROBES = new Set([
  "load",
  "inproc",
  "http-sync",
  "http-aio",
  "http-table",
  "https-cors",
  "http-rpc",
  "ws-rpc",
  "conformance",
]);
const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
  ".map": "application/json; charset=utf-8",
  ".css": "text/css; charset=utf-8",
};

function parseArgs(argv) {
  let siteDir = null;
  let probes = process.env.DUCKNNG_BROWSER_PROBES || "load";

  for (const arg of argv) {
    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }
    if (arg.startsWith("--probes=")) {
      probes = arg.slice("--probes=".length);
      continue;
    }
    if (arg.startsWith("--site-dir=")) {
      siteDir = arg.slice("--site-dir=".length);
      continue;
    }
    if (arg.startsWith("--")) {
      throw new Error(`unknown option: ${arg}`);
    }
    if (siteDir) throw new Error(`unexpected extra argument: ${arg}`);
    siteDir = arg;
  }

  return {
    siteDir: resolve(siteDir || DEFAULT_SITE_DIR),
    probes: parseProbes(probes),
  };
}

function parseProbes(raw) {
  const probes = new Set();
  for (const item of String(raw || "load").split(",")) {
    const probe = item.trim();
    if (!probe) continue;
    if (probe === "baseline") {
      probes.add("load");
      probes.add("inproc");
      continue;
    }
    if (!KNOWN_PROBES.has(probe)) {
      throw new Error(`unknown browser probe: ${probe}`);
    }
    probes.add(probe);
  }
  probes.add("load");
  return probes;
}

function printUsage() {
  console.log(`Usage: node test/browser/run_smoke.mjs [siteDir] [--probes=load,inproc,http-sync,http-aio,http-table,https-cors,http-rpc,ws-rpc]

Probes:
  load       Load DuckDB wasm, LOAD the ducknng extension, verify local
             COOP/COEP isolation, and run one SQL shell query. Enabled by
             default and required before all transport probes.
  inproc     Run the smoke page's built-in scalar/codec/inproc:// AIO proof.
             A false inproc result is accepted for non-threaded runtimes. For
             wasm_threads, a failure fails this diagnostic probe, but repeated
             headless runs are currently known to expose progress flakiness.
  http-sync  Exercise browser HTTP(S) sync client support through ducknng_ncurl
             against same-origin local GET/POST/header/status test endpoints,
             invalid method and headers_json error cases, and a cross-origin
             no-CORS failure. Enable only for artifacts expected to contain the
             browser HTTP bridge.
  http-aio   Launch ducknng_ncurl_aio against the same-origin endpoint, then
             verify aio_status, aio_wait, ducknng_ncurl_aio_collect, cancel,
             and drop behavior, plus pending/cancel/timeout semantics for the
             browser fetch completion bridge (real async handles).
  http-table Exercise ducknng_ncurl_table over same-origin JSON, text, and CSV
             response bodies.
  https-cors Exercise ducknng_ncurl and ducknng_ncurl_table against a separate
             HTTPS origin with permissive CORS headers. The runner uses a local
             test certificate and launches Chromium with HTTPS errors ignored
             for this probe.
  http-rpc   Exercise framed raw/RPC/session helper routing over browser HTTP
             against a small local ducknng-frame responder.
  ws-rpc     Exercise the async browser ws:// and wss:// frame carrier,
             including persistent reuse, timeout, cancellation, abnormal close,
             synchronous-call rejection, and browser-managed WSS TLS.
  conformance
             Read ducknng_transport_capabilities(), assert the scalar and the
             active ducknng_list_transport_capabilities() row agree, then gate
             each transport probe on its capability: run supported, skip
             unsupported, run experimental report-only. The capability-gated
             replacement for hand-maintained per-target probe lists.

Alias:
  baseline   Expands to load,inproc.
`);
}

function setIsolationHeaders(res) {
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
  res.setHeader("Cross-Origin-Resource-Policy", "same-origin");
  res.setHeader("Cache-Control", "no-store");
}

function setCorsProbeHeaders(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Ducknng-Probe");
  res.setHeader("Access-Control-Expose-Headers", "X-Ducknng-Reply");
  res.setHeader("Cross-Origin-Resource-Policy", "cross-origin");
  res.setHeader("Cache-Control", "no-store");
}

function writeU32LE(buf, offset, value) {
  buf.writeUInt32LE(value >>> 0, offset);
}

function buildDucknngFrame(type, name, flags, payload) {
  const nameBuf = Buffer.from(name || "", "utf8");
  const payloadBuf = Buffer.from(payload || "", "utf8");
  const header = Buffer.alloc(22);
  header[0] = 1;
  header[1] = type & 0xff;
  writeU32LE(header, 2, flags >>> 0);
  writeU32LE(header, 6, nameBuf.length);
  writeU32LE(header, 10, 0);
  header.writeBigUInt64LE(BigInt(payloadBuf.length), 14);
  return Buffer.concat([header, nameBuf, payloadBuf]);
}

function decodeDucknngFrame(buf) {
  if (!buf || buf.length < 22 || buf[0] !== 1) return null;
  const nameLen = buf.readUInt32LE(6);
  const errorLen = buf.readUInt32LE(10);
  const payloadLen = Number(buf.readBigUInt64LE(14));
  const nameStart = 22;
  const errorStart = nameStart + nameLen;
  const payloadStart = errorStart + errorLen;
  if (nameLen > 128 || payloadStart + payloadLen > buf.length) return null;
  return {
    type: buf[1],
    flags: buf.readUInt32LE(2),
    name: buf.subarray(nameStart, errorStart).toString("utf8"),
    payload: buf.subarray(payloadStart, payloadStart + payloadLen),
  };
}

function ducknngFrameResponseFor(reqBody) {
  const frame = decodeDucknngFrame(reqBody);
  const payloadJson = (text) => buildDucknngFrame(2, frame?.name || "manifest", 4, text);
  if (!frame) {
    return buildDucknngFrame(3, "transport", 0, "");
  }
  if (frame.type === 0) {
    return buildDucknngFrame(2, "manifest", 4, '{"browser_manifest":"ok","methods":[]}');
  }
  switch (frame.name) {
    case "manifest":
      return payloadJson('{"browser_manifest":"ok","methods":[]}');
    case "exec":
      return buildDucknngFrame(2, "exec", 4, '{"rows_changed":0,"statement_type":0,"result_type":0}');
    case "query_open":
      return buildDucknngFrame(2, "query_open", 4 | 32,
        '{"session_id":1,"session_token":"browser-token","result_handle":"browser-result",' +
        '"state":"open","next_method":"fetch","serialization_mode":"arrow_ipc_stream",' +
        '"ducknng_protocol_version":1,"row_schema_version":1,"fetch_batch_chunks":1,' +
        '"idle_timeout_ms":300000}');
    case "fetch":
      return buildDucknngFrame(2, "fetch", 4 | 16, '{"state":"exhausted","batch_index":0}');
    case "close":
      return buildDucknngFrame(2, "close", 4 | 64, '{"state":"closed"}');
    case "cancel":
      return buildDucknngFrame(2, "cancel", 4 | 128, '{"state":"cancelled"}');
    default:
      return buildDucknngFrame(3, frame.name || "unknown", 0, "");
  }
}

function readBody(req) {
  return new Promise((resolveBody) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolveBody(Buffer.concat(chunks)));
    req.on("error", () => resolveBody(Buffer.alloc(0)));
  });
}

async function handleProbe(req, res, pathname) {
  setIsolationHeaders(res);

  if (pathname === "/probe/slow" && req.method === "GET") {
    setTimeout(() => {
      res.setHeader("content-type", "text/plain");
      res.end("slow ok");
    }, 1500);
    return;
  }
  if (pathname === "/probe/hello" && req.method === "GET") {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end('{"hello":"ducknng-http-ok"}');
    return;
  }

  if (pathname === "/probe/json" && req.method === "GET") {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.end('[{"a":1,"b":"x"},{"a":2,"b":"y"}]');
    return;
  }

  if (pathname === "/probe/text" && req.method === "GET") {
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.end("ducknng text table ok");
    return;
  }

  if (pathname === "/probe/csv" && req.method === "GET") {
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.end("a,b\n1,x\n2,y\n");
    return;
  }

  if (pathname === "/probe/echo" && req.method === "POST") {
    const body = await readBody(req);
    res.setHeader("Content-Type", "application/octet-stream");
    res.end(body);
    return;
  }

  if (pathname === "/probe/headers" && req.method === "GET") {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("X-Ducknng-Reply", "browser-response-header-ok");
    res.end(JSON.stringify({
      requestHeader: req.headers["x-ducknng-probe"] || null,
      method: req.method,
    }));
    return;
  }

  if (pathname === "/probe/rpc" && req.method === "POST") {
    const body = await readBody(req);
    res.setHeader("Content-Type", "application/vnd.ducknng.frame");
    res.end(ducknngFrameResponseFor(body));
    return;
  }

  res.statusCode = 404;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.end("probe not found");
}

function safeFilePath(root, pathname) {
  const decoded = decodeURIComponent(pathname);
  const relative = decoded === "/" ? "index.html" : decoded.replace(/^\/+/, "");
  const filePath = resolve(root, relative);
  if (filePath !== root && !filePath.startsWith(root + sep)) {
    throw new Error("path escapes site root");
  }
  return filePath;
}

function attachWebSocketProbe(server) {
  const wsServer = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });
  const paths = new Set(["/probe/rpc/ws", "/probe/rpc/ws-hang", "/probe/rpc/ws-close"]);

  server.on("upgrade", (req, socket, head) => {
    let pathname;
    try {
      pathname = new URL(req.url || "/", "http://localhost").pathname;
    } catch {
      socket.destroy();
      return;
    }
    if (!paths.has(pathname)) {
      socket.destroy();
      return;
    }
    wsServer.handleUpgrade(req, socket, head, (ws) => {
      wsServer.emit("connection", ws, req);
    });
  });

  wsServer.on("connection", (ws, req) => {
    const pathname = new URL(req.url || "/", "http://localhost").pathname;
    ws.on("message", (data, isBinary) => {
      if (!isBinary) {
        ws.close(1003, "binary ducknng frames required");
        return;
      }
      if (pathname === "/probe/rpc/ws-hang") return;
      if (pathname === "/probe/rpc/ws-close") {
        ws.close(1011, "intentional probe close");
        return;
      }
      ws.send(ducknngFrameResponseFor(Buffer.from(data)), { binary: true });
    });
  });
  return wsServer;
}

function startServer(root) {
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url || "/", "http://localhost");
      if (url.pathname.startsWith("/probe/")) {
        await handleProbe(req, res, url.pathname);
        return;
      }

      const filePath = safeFilePath(root, url.pathname);
      const info = await stat(filePath);
      if (!info.isFile()) throw new Error("not a file");
      const body = await readFile(filePath);

      setIsolationHeaders(res);
      res.setHeader("Content-Type", MIME[extname(filePath)] || "application/octet-stream");
      res.end(body);
    } catch {
      setIsolationHeaders(res);
      res.statusCode = 404;
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      res.end("not found");
    }
  });

  const wsServer = attachWebSocketProbe(server);
  return new Promise((resolveServer) => {
    server.listen(0, "127.0.0.1", () => {
      resolveServer({ server, wsServer, port: server.address().port });
    });
  });
}


async function handleHttpsCorsProbe(req, res, pathname) {
  setCorsProbeHeaders(res);
  if (req.method === "OPTIONS") {
    res.statusCode = 204;
    res.end();
    return;
  }
  if (pathname === "/probe/https-json" && req.method === "GET") {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("X-Ducknng-Reply", "browser-https-response-header-ok");
    res.end('[{"a":10},{"a":32}]');
    return;
  }
  res.statusCode = 404;
  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.end("https probe not found");
}

async function startHttpsProbeServer() {
  const [key, cert] = await Promise.all([
    readFile(resolve(REPO_ROOT, "test/certs/loopback-key.pem")),
    readFile(resolve(REPO_ROOT, "test/certs/loopback-cert.pem")),
  ]);
  const server = createHttpsServer({ key, cert }, async (req, res) => {
    try {
      const url = new URL(req.url || "/", "https://localhost");
      await handleHttpsCorsProbe(req, res, url.pathname);
    } catch {
      setCorsProbeHeaders(res);
      res.statusCode = 500;
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      res.end("https probe failure");
    }
  });
  const wsServer = attachWebSocketProbe(server);
  return new Promise((resolveServer) => {
    server.listen(0, "127.0.0.1", () => {
      resolveServer({ server, wsServer, port: server.address().port });
    });
  });
}

function startNoCorsProbeServer() {
  const server = createServer(async (req, res) => {
    const url = new URL(req.url || "/", "http://localhost");
    res.setHeader("Cache-Control", "no-store");
    if (url.pathname === "/probe/no-cors" && req.method === "GET") {
      res.setHeader("Content-Type", "text/plain; charset=utf-8");
      res.end("this response intentionally has no CORS headers");
      return;
    }
    res.statusCode = 404;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.end("no-cors probe not found");
  });
  return new Promise((resolveServer) => {
    server.listen(0, "127.0.0.1", () => {
      resolveServer({ server, port: server.address().port });
    });
  });
}

const RECORDED_FAILURE = "__ducknng_browser_smoke_failure_recorded__";

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exitCode = 1;
  throw new Error(RECORDED_FAILURE);
}

async function waitForSmokeApi(page) {
  await page.waitForFunction(() => !!globalThis.ducknngWasmSmoke?.setup, undefined, {
    timeout: 30000,
  });
}

async function runShell(page, sql, timeout = 60000) {
  await page.click("#clear-result");
  await page.fill("#sql", sql);
  await page.click("#run-sql");
  await page.waitForFunction(() => {
    const meta = document.getElementById("result-meta");
    const text = (meta && meta.textContent) || "";
    return text !== "No query has run yet." && /row|Error/i.test(text);
  }, undefined, { timeout });

  return await page.evaluate(() => ({
    meta: document.getElementById("result-meta")?.textContent || "",
    table: document.getElementById("result-table")?.textContent || "",
  }));
}

async function runLoadProbe(page) {
  const isolated = await page.evaluate(() => self.crossOriginIsolated === true);
  if (!isolated) fail("page is not crossOriginIsolated; local server did not apply COOP/COEP");
  console.log("ok: crossOriginIsolated");

  await page.evaluate(async () => await globalThis.ducknngWasmSmoke.setup());
  await page.waitForFunction(() => document.getElementById("run-sql")?.disabled === false, undefined, {
    timeout: 120000,
  });
  console.log("ok: extension loaded and SQL shell enabled");

  const shell = await runShell(page, "SELECT ducknng_nng_version() AS v");
  if (/error/i.test(shell.meta)) {
    fail(`shell query errored: ${shell.table}`);
  } else if (!/\d+\.\d+/.test(shell.table)) {
    fail(`shell query returned no version: "${shell.table}"`);
  } else {
    console.log(`ok: shell query returned a version (${shell.table.trim().slice(0, 40)})`);
  }
}

async function readCapabilities(page) {
  const scalarRows = await page.evaluate(
    async (sql) => await globalThis.ducknngWasmSmoke.query(sql),
    "SELECT ducknng_transport_capabilities() AS c");
  if (!scalarRows || !scalarRows[0] || typeof scalarRows[0].c !== "string") {
    fail("conformance: ducknng_transport_capabilities() returned no descriptor");
  }
  let caps;
  try {
    caps = JSON.parse(scalarRows[0].c);
  } catch (e) {
    fail(`conformance: capability descriptor is not JSON: ${scalarRows[0].c}`);
  }
  // The active row of the table function must agree with the scalar: a claim
  // the two disagree on is drift in the contract itself.
  const listRows = await page.evaluate(
    async (sql) => await globalThis.ducknngWasmSmoke.query(sql),
    "SELECT target, http, https, http_response_stream, inproc, tcp, ipc, tls_tcp, websocket, " +
      "async_is_real, honors_timeout, honors_cancel, tls_owner " +
      "FROM ducknng_list_transport_capabilities() WHERE active");
  if (!listRows || listRows.length !== 1) {
    fail(`conformance: expected exactly one active capability row, got ${listRows ? listRows.length : 0}`);
  }
  const active = listRows[0];
  const bool = (v) => v === true || v === "true" || v === 1 || v === 1n;
  if (active.target !== caps.backend || active.http !== caps.http || active.https !== caps.https ||
      active.http_response_stream !== caps.http_response_stream || active.inproc !== caps.inproc || active.tcp !== caps.tcp || active.ipc !== caps.ipc ||
      active.tls_tcp !== caps.tls_tcp || active.websocket !== caps.websocket ||
      active.tls_owner !== caps.tls_owner || bool(active.async_is_real) !== (caps.async_is_real === true) ||
      bool(active.honors_timeout) !== (caps.honors_timeout === true) ||
      bool(active.honors_cancel) !== (caps.honors_cancel === true)) {
    fail(`conformance: scalar descriptor and active table row disagree: ` +
      `${JSON.stringify(caps)} vs ${JSON.stringify(active)}`);
  }
  console.log(`ok: conformance descriptor consistent (backend=${caps.backend}, ` +
    `async_is_real=${caps.async_is_real}, http=${caps.http}, websocket=${caps.websocket})`);
  return caps;
}

// Run a capability-gated assertion: supported -> run and hard-fail on error;
// experimental -> run report-only (a failure does not fail the suite);
// unsupported -> clean skip. `available` lets the caller skip when a probe
// dependency (e.g. an HTTPS origin) was not started.
async function conformanceGate(name, capability, fn, available = true) {
  if (capability === "unsupported") {
    console.log(`skip: ${name} (capability unsupported)`);
    return;
  }
  if (!available) {
    console.log(`skip: ${name} (probe dependency not started)`);
    return;
  }
  if (capability === "experimental") {
    const priorExit = process.exitCode;
    try {
      await fn();
      console.log(`ok (experimental): ${name}`);
    } catch (error) {
      if ((error?.message ?? String(error)) === RECORDED_FAILURE) {
        process.exitCode = priorExit;
        console.log(`report-only: ${name} did not pass (experimental, non-gating)`);
        return;
      }
      throw error;
    }
    return;
  }
  await fn();
}

async function runConformanceProbe(page, base, httpsBase, noCorsBase, wsBase, wssBase) {
  const caps = await readCapabilities(page);
  // HTTP client family, including the frame carrier, gates on http support.
  await conformanceGate("http sync client", caps.http,
    () => runHttpSyncProbe(page, base, noCorsBase));
  await conformanceGate("http table client", caps.http,
    () => runHttpTableProbe(page, base));
  await conformanceGate("http aio (fetch completion bridge)", caps.http,
    () => runHttpAioProbe(page, base));
  await conformanceGate("http frame carrier (raw/rpc/session)", caps.http,
    () => runHttpRpcProbe(page, base));
  await conformanceGate("https client", caps.https,
    () => runHttpsCorsProbe(page, httpsBase), httpsBase != null);
  await conformanceGate("websocket frame carrier", caps.websocket,
    () => runWebSocketRpcProbe(page, wsBase, wssBase), wsBase != null && wssBase != null);
  // inproc is drift-checked: the runInprocProbe already fails for wasm_threads
  // and reports unavailable otherwise, so map it to the same three-way rule.
  await conformanceGate("inproc transport", caps.inproc,
    () => runInprocProbe(page));
  console.log(`ok: conformance run complete for ${caps.backend}`);
}

async function runInprocProbe(page) {
  const runtime = await page.evaluate(() => globalThis.ducknngWasmSmoke.runtimeConfig());
  const platform = runtime?.platform || "unknown";
  let result = false;

  try {
    result = await page.evaluate(async () => await globalThis.ducknngWasmSmoke.runSmokeTests());
  } catch (error) {
    fail(`inproc smoke threw for ${platform}: ${error?.message ?? String(error)}`);
  }

  if (result === true) {
    console.log(`ok: inproc smoke passed for ${platform}`);
    return;
  }
  if (platform !== "wasm_threads") {
    console.log(`ok: inproc smoke unavailable for ${platform}`);
    return;
  }
  fail("inproc smoke failed for wasm_threads");
}

async function runHttpSyncProbe(page, base, noCorsBase) {
  const getSql =
    `SELECT ok, status, body_text FROM ducknng_ncurl('${base}/probe/hello', ` +
    `'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const get = await runShell(page, getSql);
  if (/error/i.test(get.meta)) {
    fail(`http-sync GET errored: ${get.table}`);
  } else if (!/200/.test(get.table) || !/ducknng-http-ok/.test(get.table)) {
    fail(`http-sync GET did not return expected 200 body: "${get.table}"`);
  } else {
    console.log("ok: http-sync GET returned 200 + expected body");
  }

  const postSql =
    `SELECT ok, status, body_text FROM ducknng_ncurl('${base}/probe/echo', ` +
    `'POST', NULL, 'ducknng-post-roundtrip'::BLOB, 5000, 0::UBIGINT)`;
  const post = await runShell(page, postSql);
  if (/error/i.test(post.meta)) {
    fail(`http-sync POST errored: ${post.table}`);
  } else if (!/200/.test(post.table) || !/ducknng-post-roundtrip/.test(post.table)) {
    fail(`http-sync POST did not echo request body: "${post.table}"`);
  } else {
    console.log("ok: http-sync POST round-tripped the request body");
  }

  const headerSql =
    `SELECT ok, status, headers_json, body_text FROM ducknng_ncurl('${base}/probe/headers', ` +
    `'GET', '[{"name":"X-Ducknng-Probe","value":"browser-header-ok"}]', NULL, 5000, 0::UBIGINT)`;
  const header = await runShell(page, headerSql);
  if (/error/i.test(header.meta)) {
    fail(`http-sync header probe errored: ${header.table}`);
  } else if (!/browser-header-ok/.test(header.table) || !/browser-response-header-ok/i.test(header.table)) {
    fail(`http-sync header probe did not round-trip request/response headers: "${header.table}"`);
  } else {
    console.log("ok: http-sync sent request headers and exposed response headers");
  }

  const notFoundSql =
    `SELECT ok, status, body_text FROM ducknng_ncurl('${base}/probe/not-found', ` +
    `'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const notFound = await runShell(page, notFoundSql);
  if (/error/i.test(notFound.meta)) {
    fail(`http-sync 404 probe raised a SQL error: ${notFound.table}`);
  } else if (!/true/i.test(notFound.table) || !/404/.test(notFound.table) || !/probe not found/.test(notFound.table)) {
    fail(`http-sync 404 probe did not return completed HTTP status/body: "${notFound.table}"`);
  } else {
    console.log("ok: http-sync treats HTTP 404 as a completed HTTP exchange");
  }

  const badMethodSql =
    `SELECT ok, error FROM ducknng_ncurl('${base}/probe/hello', ` +
    `'BAD METHOD', NULL, NULL, 5000, 0::UBIGINT)`;
  const badMethod = await runShell(page, badMethodSql);
  if (/error/i.test(badMethod.meta)) {
    fail(`http-sync invalid method raised a SQL error: ${badMethod.table}`);
  } else if (!/false/i.test(badMethod.table) || !/method/i.test(badMethod.table)) {
    fail(`http-sync invalid method did not return the expected in-band error: "${badMethod.table}"`);
  } else {
    console.log("ok: http-sync invalid method returned an in-band error");
  }

  const badHeadersSql =
    `SELECT ok, error FROM ducknng_ncurl('${base}/probe/hello', ` +
    `'GET', '{"bad":true}', NULL, 5000, 0::UBIGINT)`;
  const badHeaders = await runShell(page, badHeadersSql);
  if (/error/i.test(badHeaders.meta)) {
    fail(`http-sync invalid headers_json raised a SQL error: ${badHeaders.table}`);
  } else if (!/false/i.test(badHeaders.table) || !/headers_json/i.test(badHeaders.table)) {
    fail(`http-sync invalid headers_json did not return the expected in-band error: "${badHeaders.table}"`);
  } else {
    console.log("ok: http-sync invalid headers_json returned an in-band error");
  }

  const noCorsSql =
    `SELECT ok, status, error FROM ducknng_ncurl('${noCorsBase}/probe/no-cors', ` +
    `'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const noCors = await runShell(page, noCorsSql);
  if (/error/i.test(noCors.meta)) {
    fail(`http-sync no-CORS probe raised a SQL error: ${noCors.table}`);
  } else if (!/false/i.test(noCors.table) || !/(CORS|network)/i.test(noCors.table)) {
    fail(`http-sync no-CORS probe did not return expected in-band browser error: "${noCors.table}"`);
  } else {
    console.log("ok: http-sync cross-origin no-CORS failure returned an in-band error");
  }
}


async function runHttpAioProbe(page, base) {
  await runShell(page, "DROP TABLE IF EXISTS browser_http_aio", 10000);
  const launch = await runShell(page,
    `CREATE TEMP TABLE browser_http_aio AS ` +
    `SELECT ducknng_ncurl_aio('${base}/probe/hello', 'GET', NULL, NULL, 5000, 0::UBIGINT) AS aio`,
    30000);
  if (/error/i.test(launch.meta)) fail(`http-aio launch errored: ${launch.table}`);

  const waited = await runShell(page,
    `SELECT ducknng_aio_cancel(aio) AS cancelled, ` +
    `ducknng_aio_wait(list_value(aio), 5000) AS waited FROM browser_http_aio`,
    30000);
  if (/error/i.test(waited.meta) || !/false/i.test(waited.table) || !/true/i.test(waited.table)) {
    fail(`http-aio wait/cancel did not match an already-ready browser handle: "${waited.table}"`);
  }

  const status = await runShell(page,
    `SELECT kind, state, phase, terminal ` +
    `FROM ducknng_aio_status((SELECT aio FROM browser_http_aio))`,
    30000);
  if (/error/i.test(status.meta) || !/ncurl/i.test(status.table) || !/ready/i.test(status.table) ||
      !/http/i.test(status.table) || !/true/i.test(status.table)) {
    fail(`http-aio status did not report a ready ncurl/http handle: "${status.table}"`);
  }

  const collected = await runShell(page,
    `SELECT ok, status, body_text FROM ducknng_ncurl_aio_collect(` +
    `(SELECT list_value(aio) FROM browser_http_aio), 5000)`,
    30000);
  if (/error/i.test(collected.meta) || !/true/i.test(collected.table) ||
      !/200/.test(collected.table) || !/ducknng-http-ok/.test(collected.table)) {
    fail(`http-aio collect did not return expected HTTP result: "${collected.table}"`);
  }

  const dropped = await runShell(page,
    `SELECT ducknng_aio_drop(aio) AS dropped FROM browser_http_aio`, 30000);
  if (/error/i.test(dropped.meta) || !/true/i.test(dropped.table)) {
    fail(`http-aio drop failed: "${dropped.table}"`);
  }
  await runShell(page, "DROP TABLE IF EXISTS browser_http_aio", 10000);

  await runShell(page, "DROP TABLE IF EXISTS browser_http_bad_aio", 10000);
  const badLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_http_bad_aio AS ` +
    `SELECT ducknng_ncurl_aio('${base}/probe/hello', 'GET', '{"bad":true}', NULL, 5000, 0::UBIGINT) AS aio`,
    30000);
  if (/error/i.test(badLaunch.meta)) fail(`http-aio bad-header launch raised SQL error: ${badLaunch.table}`);
  const badStatus = await runShell(page,
    `SELECT kind, state, terminal, error FROM ducknng_aio_status((SELECT aio FROM browser_http_bad_aio))`,
    30000);
  if (/error/i.test(badStatus.meta) || !/ncurl/i.test(badStatus.table) || !/error/i.test(badStatus.table) ||
      !/true/i.test(badStatus.table) || !/headers_json/i.test(badStatus.table)) {
    fail(`http-aio bad-header handle was not a terminal error handle: "${badStatus.table}"`);
  }
  const badCollected = await runShell(page,
    `SELECT ok, error FROM ducknng_ncurl_aio_collect(` +
    `(SELECT list_value(aio) FROM browser_http_bad_aio), 0)`,
    30000);
  if (/error/i.test(badCollected.meta) || !/false/i.test(badCollected.table) || !/headers_json/i.test(badCollected.table)) {
    fail(`http-aio bad-header collect did not return expected terminal error row: "${badCollected.table}"`);
  }
  const badDropped = await runShell(page,
    `SELECT ducknng_aio_drop(aio) AS dropped FROM browser_http_bad_aio`, 30000);
  if (/error/i.test(badDropped.meta) || !/true/i.test(badDropped.table)) {
    fail(`http-aio bad-header drop failed: "${badDropped.table}"`);
  }
  await runShell(page, "DROP TABLE IF EXISTS browser_http_bad_aio", 10000);

  // Real-async semantics: an in-flight fetch is observably pending, waiting
  // polls without hanging the worker, and cancel aborts the request.
  await runShell(page, "DROP TABLE IF EXISTS browser_http_slow_aio", 10000);
  const slowLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_http_slow_aio AS ` +
    `SELECT ducknng_ncurl_aio('${base}/probe/slow', 'GET', NULL, NULL, 0, 0::UBIGINT) AS aio`,
    30000);
  if (/error/i.test(slowLaunch.meta)) fail(`http-aio slow launch errored: ${slowLaunch.table}`);
  const pendingStatus = await runShell(page,
    `SELECT state, terminal FROM ducknng_aio_status((SELECT aio FROM browser_http_slow_aio))`,
    30000);
  if (/error/i.test(pendingStatus.meta) || !/pending/i.test(pendingStatus.table) ||
      !/false/i.test(pendingStatus.table)) {
    fail(`http-aio slow handle was not pending in flight: "${pendingStatus.table}"`);
  }
  const pollWait = await runShell(page,
    `SELECT ducknng_aio_wait(list_value(aio), 100) AS waited FROM browser_http_slow_aio`,
    10000);
  if (/error/i.test(pollWait.meta) || !/false/i.test(pollWait.table)) {
    fail(`http-aio wait on a pending browser handle did not poll-and-return: "${pollWait.table}"`);
  }
  const slowCancel = await runShell(page,
    `SELECT ducknng_aio_cancel(aio) AS cancelled FROM browser_http_slow_aio`, 10000);
  if (/error/i.test(slowCancel.meta) || !/true/i.test(slowCancel.table)) {
    fail(`http-aio cancel of an in-flight fetch failed: "${slowCancel.table}"`);
  }
  const cancelledStatus = await runShell(page,
    `SELECT state, terminal FROM ducknng_aio_status((SELECT aio FROM browser_http_slow_aio))`,
    30000);
  if (/error/i.test(cancelledStatus.meta) || !/cancelled/i.test(cancelledStatus.table) ||
      !/true/i.test(cancelledStatus.table)) {
    fail(`http-aio cancelled handle did not report cancelled: "${cancelledStatus.table}"`);
  }
  await runShell(page,
    `SELECT ducknng_aio_drop(aio) FROM browser_http_slow_aio`, 10000);
  await runShell(page, "DROP TABLE IF EXISTS browser_http_slow_aio", 10000);

  // timeout_ms arms a JS timer that aborts the fetch once the event loop runs.
  await runShell(page, "DROP TABLE IF EXISTS browser_http_timeout_aio", 10000);
  const timeoutLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_http_timeout_aio AS ` +
    `SELECT ducknng_ncurl_aio('${base}/probe/slow', 'GET', NULL, NULL, 200, 0::UBIGINT) AS aio`,
    30000);
  if (/error/i.test(timeoutLaunch.meta)) fail(`http-aio timeout launch errored: ${timeoutLaunch.table}`);
  await new Promise((resolve) => setTimeout(resolve, 700));
  const timedOut = await runShell(page,
    `SELECT state, error FROM ducknng_aio_status((SELECT aio FROM browser_http_timeout_aio))`,
    30000);
  if (/error(?!\s*\|)/i.test(timedOut.meta) || !/timed out/i.test(timedOut.table)) {
    fail(`http-aio timeout did not abort the fetch: "${timedOut.table}"`);
  }
  await runShell(page,
    `SELECT ducknng_aio_drop(aio) FROM browser_http_timeout_aio`, 10000);
  await runShell(page, "DROP TABLE IF EXISTS browser_http_timeout_aio", 10000);

  console.log("ok: http-aio pending/cancel/timeout semantics held and handles collected cleanly");
}

async function runHttpTableProbe(page, base) {
  const jsonSql =
    `SELECT sum(a) AS sum_a, string_agg(b, ',' ORDER BY b) AS bs ` +
    `FROM ducknng_ncurl_table('${base}/probe/json', 'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const json = await runShell(page, jsonSql);
  if (/error/i.test(json.meta) || !/3/.test(json.table) || !/x,y/.test(json.table)) {
    fail(`http-table JSON proof failed: ${json.table}`);
  }

  const textSql =
    `SELECT body_text FROM ducknng_ncurl_table('${base}/probe/text', ` +
    `'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const text = await runShell(page, textSql);
  if (/error/i.test(text.meta) || !/ducknng text table ok/.test(text.table)) {
    fail(`http-table text proof failed: ${text.table}`);
  }

  const csvSql =
    `SELECT sum(a) AS sum_a, string_agg(b, ',' ORDER BY b) AS bs ` +
    `FROM ducknng_ncurl_table('${base}/probe/csv', 'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const csv = await runShell(page, csvSql);
  if (/error/i.test(csv.meta) || !/3/.test(csv.table) || !/x,y/.test(csv.table)) {
    fail(`http-table CSV proof failed: ${csv.table}`);
  }
  console.log("ok: http-table parsed JSON, text, and CSV responses");
}

async function runHttpsCorsProbe(page, httpsBase) {
  const rawSql =
    `SELECT ok, status, headers_json, body_text FROM ducknng_ncurl('${httpsBase}/probe/https-json', ` +
    `'GET', '[{"name":"X-Ducknng-Probe","value":"browser-https-header-ok"}]', NULL, 5000, 0::UBIGINT)`;
  const raw = await runShell(page, rawSql);
  if (/error/i.test(raw.meta)) {
    fail(`https-cors raw proof raised a SQL error: ${raw.table}`);
  } else if (!/true/i.test(raw.table) || !/200/.test(raw.table) || !/browser-https-response-header-ok/i.test(raw.table) ||
      !/"a":10/.test(raw.table)) {
    fail(`https-cors raw proof did not expose expected status/header/body: ${raw.table}`);
  }

  const tlsRejectSql = `WITH cfg AS (
      SELECT ducknng_tls_config_from_pem('browser-test-cert', NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, 0) AS tls_id
    )
    SELECT ok, error
    FROM cfg, ducknng_ncurl('${httpsBase}/probe/https-json', 'GET', NULL, NULL, 5000, cfg.tls_id)`;
  const tlsReject = await runShell(page, tlsRejectSql);
  if (/error/i.test(tlsReject.meta)) {
    fail(`https-cors explicit TLS rejection raised a SQL error: ${tlsReject.table}`);
  } else if (!/false/i.test(tlsReject.table) || !/(browser-managed TLS|explicit TLS)/i.test(tlsReject.table)) {
    fail(`https-cors explicit TLS rejection did not return expected in-band error: ${tlsReject.table}`);
  }

  const httpsSql =
    `SELECT sum(a) AS sum_a FROM ducknng_ncurl_table('${httpsBase}/probe/https-json', ` +
    `'GET', NULL, NULL, 5000, 0::UBIGINT)`;
  const https = await runShell(page, httpsSql);
  if (/error/i.test(https.meta) || !/42/.test(https.table)) {
    fail(`https-cors table proof failed: ${https.table}`);
  }
  console.log("ok: https-cors returned raw/parsed results and rejected explicit browser TLS handles");
}

async function runHttpRpcProbe(page, base) {
  const rawSql =
    `SELECT ok, type_name, name, payload_text ` +
    `FROM ducknng_decode_frame(ducknng_get_rpc_manifest_raw('${base}/probe/rpc', 0::UBIGINT))`;
  const raw = await runShell(page, rawSql);
  if (/error/i.test(raw.meta) || !/true/i.test(raw.table) || !/manifest/.test(raw.table) ||
      !/browser_manifest/.test(raw.table)) {
    fail(`http-rpc raw manifest proof failed: ${raw.table}`);
  }

  const manifestSql = `SELECT ok, manifest FROM ducknng_get_rpc_manifest('${base}/probe/rpc', 0::UBIGINT)`;
  const manifest = await runShell(page, manifestSql);
  if (/error/i.test(manifest.meta) || !/true/i.test(manifest.table) || !/browser_manifest/.test(manifest.table)) {
    fail(`http-rpc structured manifest proof failed: ${manifest.table}`);
  }

  await runShell(page, "DROP TABLE IF EXISTS browser_rpc_aio", 10000);
  const aioLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_rpc_aio AS ` +
    `SELECT ducknng_get_rpc_manifest_raw_aio('${base}/probe/rpc', 5000, 0::UBIGINT) AS aio`,
    30000);
  if (/error/i.test(aioLaunch.meta)) fail(`http-rpc aio launch failed: ${aioLaunch.table}`);
  const aio = await runShell(page,
    `WITH collected AS (` +
    `  SELECT * FROM ducknng_aio_collect_decoded((SELECT list_value(aio) FROM browser_rpc_aio), 5000)` +
    `), dropped AS (` +
    `  SELECT c.*, ducknng_aio_drop(c.aio_id) AS dropped FROM collected c` +
    `) SELECT ok, type_name, name, payload_text, dropped FROM dropped`,
    30000);
  if (/error/i.test(aio.meta) || !/true/i.test(aio.table) || !/manifest/.test(aio.table) ||
      !/browser_manifest/.test(aio.table)) {
    fail(`http-rpc aio manifest proof failed: ${aio.table}`);
  }
  await runShell(page, "DROP TABLE IF EXISTS browser_rpc_aio", 10000);

  const sessionSql = `WITH opened AS (
      SELECT ducknng_open_query_raw('${base}/probe/rpc', 'SELECT 1', 0::UBIGINT, 0::UBIGINT, 0::UBIGINT) AS frame
    ), open_dec AS (
      SELECT ducknng_frame_error_text(frame) IS NULL AS ok,
             ducknng_frame_payload_text(frame) AS payload_text
      FROM opened
    ), session AS (
      SELECT json_extract(payload_text::JSON, '$.session_id')::UBIGINT AS session_id,
             json_extract_string(payload_text::JSON, '$.session_token') AS token
      FROM open_dec
    ), fetched AS (
      SELECT ducknng_fetch_query_raw('${base}/probe/rpc', session_id, token, 0::UBIGINT, 0::UBIGINT, 0::UBIGINT) AS frame
      FROM session
    ), fetch_dec AS (
      SELECT ducknng_frame_error_text(frame) IS NULL AS ok,
             ducknng_frame_payload_text(frame) AS payload_text
      FROM fetched
    ), cancelled AS (
      SELECT ducknng_cancel_query_raw('${base}/probe/rpc', session_id, token, 0::UBIGINT) AS frame
      FROM session
    ), cancel_dec AS (
      SELECT ducknng_frame_error_text(frame) IS NULL AS ok,
             ducknng_frame_payload_text(frame) AS payload_text
      FROM cancelled
    ), closed AS (
      SELECT ducknng_close_query_raw('${base}/probe/rpc', session_id, token, 0::UBIGINT) AS frame
      FROM session
    ), close_dec AS (
      SELECT ducknng_frame_error_text(frame) IS NULL AS ok,
             ducknng_frame_payload_text(frame) AS payload_text
      FROM closed
    )
    SELECT open_dec.ok AS open_ok,
           position('"state":"open"' IN open_dec.payload_text) > 0 AS open_state,
           fetch_dec.ok AS fetch_ok,
           position('"state":"exhausted"' IN fetch_dec.payload_text) > 0 AS fetch_state,
           cancel_dec.ok AS cancel_ok,
           position('"state":"cancelled"' IN cancel_dec.payload_text) > 0 AS cancel_state,
           close_dec.ok AS close_ok,
           position('"state":"closed"' IN close_dec.payload_text) > 0 AS close_state
    FROM open_dec, fetch_dec, cancel_dec, close_dec`;
  const session = await runShell(page, sessionSql);
  const trueCount = (session.table.match(/true/gi) || []).length;
  if (/error/i.test(session.meta) || trueCount < 8) {
    fail(`http-rpc raw session helper proof failed: ${session.table}`);
  }
  console.log("ok: http-rpc proved raw, structured, aio, and session helper routing over browser HTTP");
}

async function collectWebSocketAio(page, tableName, launchSql, expectedName, expectedPayload) {
  await runShell(page, `DROP TABLE IF EXISTS ${tableName}`, 10000);
  const launch = await runShell(page,
    `CREATE TEMP TABLE ${tableName} AS SELECT ${launchSql} AS aio`, 30000);
  if (/error/i.test(launch.meta)) fail(`websocket aio launch failed for ${tableName}: ${launch.table}`);
  // Let the DB worker event loop deliver the browser WebSocket callback before
  // entering a collect query; browser waits are deliberately poll-style.
  await new Promise((resolve) => setTimeout(resolve, 100));
  const collected = await runShell(page,
    `WITH c AS (` +
    `  SELECT * FROM ducknng_aio_collect_decoded((SELECT list_value(aio) FROM ${tableName}), 5000)` +
    `), d AS (` +
    `  SELECT c.*, ducknng_aio_drop(c.aio_id) AS dropped FROM c` +
    `) SELECT ok AND dropped AS lifecycle_ok, type_name, name, payload_text FROM d`,
    30000);
  if (/error/i.test(collected.meta) || !/true/i.test(collected.table) ||
      !new RegExp(expectedName, "i").test(collected.table) ||
      !new RegExp(expectedPayload, "i").test(collected.table)) {
    fail(`websocket aio collect failed for ${tableName}: ${collected.table}`);
  }
  await runShell(page, `DROP TABLE IF EXISTS ${tableName}`, 10000);
}

async function runWebSocketRpcProbe(page, wsBase, wssBase) {
  const sync = await runShell(page,
    `SELECT ok, error FROM ducknng_get_rpc_manifest('${wsBase}', 0::UBIGINT)`, 30000);
  if (/error/i.test(sync.meta) || !/false/i.test(sync.table) ||
      !/(synchronous WebSocket|async.*aio)/i.test(sync.table)) {
    fail(`websocket synchronous helper was not rejected explicitly: ${sync.table}`);
  }

  await collectWebSocketAio(page, "browser_ws_manifest_aio",
    `ducknng_get_rpc_manifest_raw_aio('${wsBase}', 5000, 0::UBIGINT)`,
    "manifest", "browser_manifest");
  // A second operation on the same URL reuses the persistent actor and proves
  // that request/reply FIFO remains aligned after the first op is collected.
  await collectWebSocketAio(page, "browser_ws_exec_aio",
    `ducknng_run_rpc_raw_aio('${wsBase}', 'SELECT 1', 5000, 0::UBIGINT)`,
    "exec", "rows_changed");
  await collectWebSocketAio(page, "browser_wss_manifest_aio",
    `ducknng_get_rpc_manifest_raw_aio('${wssBase}', 5000, 0::UBIGINT)`,
    "manifest", "browser_manifest");

  await runShell(page, "DROP TABLE IF EXISTS browser_ws_cancel_aio", 10000);
  const cancelLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_ws_cancel_aio AS ` +
    `SELECT ducknng_get_rpc_manifest_raw_aio('${wsBase}-hang', 0, 0::UBIGINT) AS aio`, 30000);
  if (/error/i.test(cancelLaunch.meta)) fail(`websocket cancel launch failed: ${cancelLaunch.table}`);
  await new Promise((resolve) => setTimeout(resolve, 100));
  const pending = await runShell(page,
    `SELECT state, terminal FROM ducknng_aio_status((SELECT aio FROM browser_ws_cancel_aio))`, 30000);
  if (/error/i.test(pending.meta) || !/pending/i.test(pending.table) || !/false/i.test(pending.table)) {
    fail(`websocket hanging operation was not pending: ${pending.table}`);
  }
  const cancelled = await runShell(page,
    `SELECT ducknng_aio_cancel(aio) AS cancelled FROM browser_ws_cancel_aio`, 10000);
  if (/error/i.test(cancelled.meta) || !/true/i.test(cancelled.table)) {
    fail(`websocket pending operation did not cancel: ${cancelled.table}`);
  }
  const cancelledStatus = await runShell(page,
    `SELECT state, terminal FROM ducknng_aio_status((SELECT aio FROM browser_ws_cancel_aio))`, 30000);
  if (/error/i.test(cancelledStatus.meta) || !/cancelled/i.test(cancelledStatus.table) ||
      !/true/i.test(cancelledStatus.table)) {
    fail(`websocket cancelled operation reported the wrong state: ${cancelledStatus.table}`);
  }
  await runShell(page, `SELECT ducknng_aio_drop(aio) FROM browser_ws_cancel_aio`, 10000);
  await runShell(page, "DROP TABLE IF EXISTS browser_ws_cancel_aio", 10000);

  await runShell(page, "DROP TABLE IF EXISTS browser_ws_timeout_aio", 10000);
  const timeoutLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_ws_timeout_aio AS ` +
    `SELECT ducknng_get_rpc_manifest_raw_aio('${wsBase}-hang', 200, 0::UBIGINT) AS aio`, 30000);
  if (/error/i.test(timeoutLaunch.meta)) fail(`websocket timeout launch failed: ${timeoutLaunch.table}`);
  await new Promise((resolve) => setTimeout(resolve, 700));
  const timedOut = await runShell(page,
    `SELECT state, error FROM ducknng_aio_status((SELECT aio FROM browser_ws_timeout_aio))`, 30000);
  if (/error(?!\s*\|)/i.test(timedOut.meta) || !/timed out/i.test(timedOut.table)) {
    fail(`websocket timeout did not tear down the actor: ${timedOut.table}`);
  }
  await runShell(page, `SELECT ducknng_aio_drop(aio) FROM browser_ws_timeout_aio`, 10000);
  await runShell(page, "DROP TABLE IF EXISTS browser_ws_timeout_aio", 10000);

  await runShell(page, "DROP TABLE IF EXISTS browser_ws_close_aio", 10000);
  const closeLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_ws_close_aio AS ` +
    `SELECT ducknng_get_rpc_manifest_raw_aio('${wsBase}-close', 5000, 0::UBIGINT) AS aio`, 30000);
  if (/error/i.test(closeLaunch.meta)) fail(`websocket abnormal-close launch failed: ${closeLaunch.table}`);
  await new Promise((resolve) => setTimeout(resolve, 200));
  const closed = await runShell(page,
    `SELECT state, error FROM ducknng_aio_status((SELECT aio FROM browser_ws_close_aio))`, 30000);
  if (/error(?!\s*\|)/i.test(closed.meta) || !/browser WebSocket (error|closed)/i.test(closed.table)) {
    fail(`websocket abnormal close was not surfaced: ${closed.table}`);
  }
  await runShell(page, `SELECT ducknng_aio_drop(aio) FROM browser_ws_close_aio`, 10000);
  await runShell(page, "DROP TABLE IF EXISTS browser_ws_close_aio", 10000);

  await runShell(page, "DROP TABLE IF EXISTS browser_wss_tls_aio", 10000);
  const tlsLaunch = await runShell(page,
    `CREATE TEMP TABLE browser_wss_tls_aio AS WITH cfg AS (` +
    `  SELECT ducknng_tls_config_from_pem('browser-wss-test', NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, 0) AS tls_id` +
    `) SELECT tls_id, ducknng_get_rpc_manifest_raw_aio('${wssBase}', 5000, tls_id) AS aio FROM cfg`, 30000);
  if (/error/i.test(tlsLaunch.meta)) fail(`wss explicit-TLS rejection launch failed: ${tlsLaunch.table}`);
  const tlsStatus = await runShell(page,
    `SELECT state, error FROM ducknng_aio_status((SELECT aio FROM browser_wss_tls_aio))`, 30000);
  if (/error(?!\s*\|)/i.test(tlsStatus.meta) ||
      !/(browser-managed TLS|explicit TLS configuration)/i.test(tlsStatus.table)) {
    fail(`wss explicit TLS handle was not rejected: ${tlsStatus.table}`);
  }
  await runShell(page, `SELECT ducknng_aio_drop(aio) FROM browser_wss_tls_aio`, 10000);
  await runShell(page, `SELECT ducknng_drop_tls_config(tls_id) FROM browser_wss_tls_aio`, 10000);
  await runShell(page, "DROP TABLE IF EXISTS browser_wss_tls_aio", 10000);

  console.log("ok: websocket carrier passed ws/wss RPC, persistence, cancel, timeout, close, and TLS semantics");
}

let options;
try {
  options = parseArgs(process.argv.slice(2));
  await stat(options.siteDir).catch(() => {
    throw new Error(`site directory does not exist: ${options.siteDir}`);
  });
} catch (error) {
  console.error(`FAIL: ${error?.message ?? String(error)}`);
  printUsage();
  process.exit(1);
}

const { siteDir, probes } = options;
const { server, wsServer, port } = await startServer(siteDir);
const wantsHttps = probes.has("https-cors") || probes.has("ws-rpc") || probes.has("conformance");
const wantsNoCors = probes.has("http-sync") || probes.has("conformance");
const httpsProbe = wantsHttps ? await startHttpsProbeServer() : null;
const noCorsProbe = wantsNoCors ? await startNoCorsProbeServer() : null;
const base = `http://127.0.0.1:${port}`;
const httpsBase = httpsProbe ? `https://127.0.0.1:${httpsProbe.port}` : null;
const wsBase = `ws://127.0.0.1:${port}/probe/rpc/ws`;
const wssBase = httpsProbe ? `wss://127.0.0.1:${httpsProbe.port}/probe/rpc/ws` : null;
const noCorsBase = noCorsProbe ? `http://127.0.0.1:${noCorsProbe.port}` : null;
console.log(`serving ${siteDir} at ${base} (COOP/COEP enabled)`);
if (httpsBase) console.log(`serving HTTPS CORS probe at ${httpsBase}`);
if (noCorsBase) console.log(`serving no-CORS failure probe at ${noCorsBase}`);
console.log(`browser probes: ${Array.from(probes).join(",")}`);

let browser = null;
let context = null;
try {
  browser = await chromium.launch({ headless: process.env.BROWSER_HEADFUL !== "1" });
  context = await browser.newContext({ ignoreHTTPSErrors: wantsHttps });
  const page = await context.newPage();
  page.on("pageerror", (error) => console.error(`[pageerror] ${error.message}`));
  if (process.env.BROWSER_DEBUG === "1") {
    page.on("console", (message) => console.log(`[browser:${message.type()}] ${message.text()}`));
  }

  await page.goto(`${base}${SMOKE_PATH}?local_browser_smoke=${Date.now()}`, {
    waitUntil: "domcontentloaded",
    timeout: 30000,
  });
  await waitForSmokeApi(page);

  await runLoadProbe(page);
  if (probes.has("inproc")) await runInprocProbe(page);
  if (probes.has("http-sync")) await runHttpSyncProbe(page, base, noCorsBase);
  if (probes.has("http-aio")) await runHttpAioProbe(page, base);
  if (probes.has("http-table")) await runHttpTableProbe(page, base);
  if (probes.has("https-cors")) await runHttpsCorsProbe(page, httpsBase);
  if (probes.has("http-rpc")) await runHttpRpcProbe(page, base);
  if (probes.has("ws-rpc")) await runWebSocketRpcProbe(page, wsBase, wssBase);
  if (probes.has("conformance")) {
    await runConformanceProbe(page, base, httpsBase, noCorsBase, wsBase, wssBase);
  }
} catch (error) {
  if ((error?.message ?? String(error)) !== RECORDED_FAILURE) {
    console.error(`FAIL: ${error?.message ?? String(error)}`);
    process.exitCode = 1;
  }
} finally {
  if (context) await context.close();
  if (browser) await browser.close();
  if (httpsProbe) {
    for (const client of httpsProbe.wsServer.clients) client.terminate();
    httpsProbe.wsServer.close();
    httpsProbe.server.close();
  }
  if (noCorsProbe) noCorsProbe.server.close();
  for (const client of wsServer.clients) client.terminate();
  wsServer.close();
  server.close();
}

console.log(process.exitCode === 1 ? "BROWSER SMOKE: FAIL" : "BROWSER SMOKE: PASS");
