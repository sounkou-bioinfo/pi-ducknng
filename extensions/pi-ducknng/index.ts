import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  DuckDBInstance,
  type DuckDBConnection,
} from "@duckdb/node-api";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import {
  type CoordinationClient,
  registerCoordinationAdapter,
} from "./coordination.ts";
import { resolveDucknngExtension } from "./ducknng-binary.ts";
import {
  MAX_SQL_ROWS,
  type SqlWorkspace,
  WORKSPACE_FILE,
  openSqlWorkspace,
} from "./workspace.ts";

export const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const DUCKNNG_RPC_FLAG_PAYLOAD_JSON = 4;
const DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM = 8;
const DEFAULT_CALL_TIMEOUT_MS = 30_000;
const MANIFEST_SQL = `
  SELECT type_name, name, flags, error AS error_text, payload_text
  FROM ducknng_decode_frame(
    ducknng_get_rpc_manifest_raw($url, $tls_config_id::UBIGINT)
  )`;
// ducknng builds the call frame; this client never assembles envelope bytes.
const RPC_CALL_SQL = `
  SELECT type_name, name, flags, error AS error_text, payload
  FROM ducknng_decode_frame(
    ducknng_request_raw(
      $url,
      ducknng_encode_rpc_call($method, $payload),
      $timeout,
      $tls_config_id::UBIGINT
    )
  )`;

export { resolveDucknngExtension };

export function closeDucknngConnection(
  instance: DuckDBInstance,
  connection?: DuckDBConnection,
): void {
  try {
    connection?.closeSync();
  } finally {
    instance.closeSync();
  }
}

export async function openDucknngConnection(
  extensionPath: string,
  database = ":memory:",
): Promise<{ instance: DuckDBInstance; connection: DuckDBConnection }> {
  const instance = await DuckDBInstance.create(database, {
    allow_unsigned_extensions: "true",
  });
  let connection: DuckDBConnection | undefined;
  try {
    connection = await instance.connect();
    const escapedPath = extensionPath.replaceAll("'", "''");
    await connection.run(`LOAD '${escapedPath}'`);
    return { instance, connection };
  } catch (error) {
    closeDucknngConnection(instance, connection);
    throw error;
  }
}

function environment(name: string): string | null {
  return process.env[name]?.trim() || null;
}

/**
 * Client TLS material is injected at the process boundary as PEM file paths:
 * PI_DUCKNNG_TLS_CA_FILE verifies the server, and PI_DUCKNNG_TLS_CERT_KEY_FILE,
 * a combined certificate and key, is presented for mutual TLS. The paths are
 * bound as SQL parameters and never appear in tool schemas or results.
 */
export async function clientTlsConfigId(
  connection: DuckDBConnection,
  url: string,
): Promise<number> {
  if (!/^(tls\+tcp|wss):\/\//i.test(url)) return 0;
  const caFile = environment("PI_DUCKNNG_TLS_CA_FILE");
  if (!caFile) {
    throw new Error("TLS endpoints require PI_DUCKNNG_TLS_CA_FILE to verify the server");
  }
  const reader = await connection.runAndReadAll(
    "SELECT ducknng_tls_config_from_files($cert_key_file, $ca_file, NULL, 2)::UBIGINT AS id",
    { cert_key_file: environment("PI_DUCKNNG_TLS_CERT_KEY_FILE"), ca_file: caFile },
  );
  const [row] = reader.getRowObjects();
  const id = Number(row?.id);
  if (!Number.isSafeInteger(id) || id <= 0) {
    throw new Error("ducknng did not create a client TLS configuration");
  }
  return id;
}

function blobBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (
    typeof value === "object" &&
    value !== null &&
    "bytes" in value &&
    value.bytes instanceof Uint8Array
  ) {
    return value.bytes;
  }
  throw new Error("ducknng returned a non-BLOB payload");
}

function collectProcessOutput(child: ChildProcess): () => string {
  let output = "";
  const append = (chunk: Buffer | string) => {
    output = `${output}${chunk.toString()}`.slice(-1024 * 1024);
  };
  child.stdout?.on("data", append);
  child.stderr?.on("data", append);
  return () => output.trim();
}

type ProcessOutcome = {
  code: number | null;
  signal: NodeJS.Signals | null;
};

type ProcessObserver = {
  termination: Promise<ProcessOutcome>;
  outcome: () => ProcessOutcome | undefined;
  error: () => Error | undefined;
};

function observeChild(child: ChildProcess): ProcessObserver {
  let outcome: ProcessOutcome | undefined;
  let processError: Error | undefined;
  const termination = new Promise<ProcessOutcome>((resolveExit, rejectExit) => {
    child.once("error", rejectExit);
    child.once("exit", (code, signal) => resolveExit({ code, signal }));
  });
  termination.then(
    (value) => {
      outcome = value;
    },
    (error) => {
      processError = error instanceof Error ? error : new Error(String(error));
    },
  );
  return {
    termination,
    outcome: () => outcome,
    error: () => processError,
  };
}

// A missing Rscript is the most common setup problem; say what to install.
function spawnFailure(error: Error): Error {
  if ((error as NodeJS.ErrnoException).code !== "ENOENT") return error;
  return new Error(
    "persistent_r_start needs R, but Rscript is not on PATH: install R, then " +
    "install.packages(c(\"jsonlite\", \"mirai\", \"nanoarrow\", \"nanonext\"))",
    { cause: error },
  );
}

async function waitForLocator(
  locator: string,
  observer: ProcessObserver,
  processOutput: () => string,
): Promise<string> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const url = (await readFile(locator, "utf8")).trim();
      if (url) return url;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "ENOENT") throw error;
    }
    const failed = observer.error();
    if (failed) throw spawnFailure(failed);
    const outcome = observer.outcome();
    if (outcome) {
      throw new Error(
        `R endpoint exited with status ${outcome.code ?? outcome.signal}: ${processOutput()}`,
      );
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
  }
  throw new Error(`timed out waiting for the R endpoint: ${processOutput()}`);
}

async function waitForExit(
  observer: ProcessObserver,
  timeoutMs: number,
): Promise<ProcessOutcome> {
  return await new Promise<ProcessOutcome>((resolveExit, rejectExit) => {
    const timer = setTimeout(
      () => rejectExit(new Error("timed out waiting for the R endpoint to exit")),
      timeoutMs,
    );
    observer.termination.then(
      (outcome) => {
        clearTimeout(timer);
        resolveExit(outcome);
      },
      (error) => {
        clearTimeout(timer);
        rejectExit(error);
      },
    );
  });
}

async function terminateChild(
  child: ChildProcess,
  observer: ProcessObserver,
): Promise<void> {
  if (observer.outcome() || observer.error()) return;
  child.kill();
  try {
    await waitForExit(observer, 2000);
  } catch {
    if (observer.outcome() || observer.error()) return;
    child.kill("SIGKILL");
    await waitForExit(observer, 2000);
  }
}

type REndpoint = {
  extensionPath: string;
  work: string;
  url: string;
  child: ChildProcess;
  observer: ProcessObserver;
  processOutput: () => string;
  sharedLocator?: string;
};

function hasExited(endpoint: REndpoint): boolean {
  return Boolean(
    endpoint.observer.outcome() ||
    endpoint.observer.error() ||
    endpoint.child.exitCode !== null ||
    endpoint.child.signalCode !== null,
  );
}

/**
 * PI_DUCKNNG_R_LOCATOR names a file that receives the endpoint URL, so another
 * local client can attach to the same R session. The URL is an ipc:// socket
 * in a directory only this user can enter.
 */
async function shareLocator(url: string): Promise<string | undefined> {
  const path = environment("PI_DUCKNNG_R_LOCATOR");
  if (!path) return undefined;
  const staged = `${path}.${process.pid}.tmp`;
  await writeFile(staged, `${url}\n`, { mode: 0o600 });
  await rename(staged, path);
  return path;
}

async function unshareLocator(endpoint: REndpoint): Promise<void> {
  if (!endpoint.sharedLocator) return;
  const current = await readFile(endpoint.sharedLocator, "utf8").catch(() => "");
  if (current.trim() === endpoint.url) await rm(endpoint.sharedLocator, { force: true });
}

type EndpointMethod = {
  [key: string]: unknown;
  name: string;
  summary: string;
  request_schema: Record<string, unknown> | null;
};

type EndpointManifest = {
  server: {
    [key: string]: unknown;
    name: string;
    version: string;
    protocol_version: number;
  };
  methods: EndpointMethod[];
};

// The endpoint exits after this process does, so it never outlives Pi.
async function startREndpoint(root: string): Promise<REndpoint> {
  const extensionPath = await resolveDucknngExtension(root);
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-endpoint-"));
  const locator = resolve(work, "endpoint.url");
  const child = spawn(
    "Rscript",
    ["--vanilla", resolve(root, "tools/pi-r-endpoint.R"), locator, `--parent=${process.pid}`],
    { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
  );
  const processOutput = collectProcessOutput(child);
  const observer = observeChild(child);
  try {
    const url = await waitForLocator(locator, observer, processOutput);
    const sharedLocator = await shareLocator(url);
    return { extensionPath, work, url, child, observer, processOutput, sharedLocator };
  } catch (error) {
    try {
      await terminateChild(child, observer);
      await rm(work, { recursive: true, force: true });
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "R endpoint startup failed and its process could not be stopped",
      );
    }
    throw error;
  }
}

async function disposeREndpoint(endpoint: REndpoint): Promise<void> {
  if (!hasExited(endpoint)) {
    try {
      await rpcCallThroughDucknng(
        endpoint.extensionPath,
        endpoint.url,
        "close",
        {},
      );
      await waitForExit(endpoint.observer, 2000);
    } catch {
      await terminateChild(endpoint.child, endpoint.observer);
    }
  }
  await terminateChild(endpoint.child, endpoint.observer);
  await unshareLocator(endpoint);
  await rm(endpoint.work, { recursive: true, force: true });
}

function parseManifest(payload: string): EndpointManifest {
  const value: unknown = JSON.parse(payload);
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("endpoint returned a non-object ducknng manifest");
  }
  const record = value as Record<string, unknown>;
  if (
    typeof record.server !== "object" ||
    record.server === null ||
    Array.isArray(record.server) ||
    !Array.isArray(record.methods)
  ) {
    throw new Error("endpoint returned an invalid ducknng manifest header");
  }
  const server = record.server as Record<string, unknown>;
  if (
    typeof server.name !== "string" ||
    typeof server.version !== "string" ||
    typeof server.protocol_version !== "number"
  ) {
    throw new Error("endpoint returned invalid ducknng server metadata");
  }
  const methods = record.methods.map((method): EndpointMethod => {
    if (typeof method !== "object" || method === null || Array.isArray(method)) {
      throw new Error("endpoint returned an invalid ducknng method descriptor");
    }
    const descriptor = method as Record<string, unknown>;
    const schema = descriptor.request_schema;
    if (
      typeof descriptor.name !== "string" ||
      typeof descriptor.summary !== "string" ||
      !(
        schema === null ||
        (typeof schema === "object" && !Array.isArray(schema))
      )
    ) {
      throw new Error("endpoint returned an invalid ducknng method descriptor");
    }
    return {
      ...descriptor,
      name: descriptor.name,
      summary: descriptor.summary,
      request_schema: schema as Record<string, unknown> | null,
    };
  });
  return {
    server: {
      ...server,
      name: server.name,
      version: server.version,
      protocol_version: server.protocol_version,
    },
    methods,
  };
}

async function manifestThroughDucknng(
  extensionPath: string,
  url: string,
): Promise<string> {
  const { instance, connection } = await openDucknngConnection(extensionPath);
  try {
    const reader = await connection.runAndReadAll(MANIFEST_SQL, {
      url,
      tls_config_id: await clientTlsConfigId(connection, url),
    });
    const [row] = reader.getRowObjects();
    if (!row || row.type_name !== "result" || row.name !== "manifest") {
      throw new Error(String(row?.error_text ?? "invalid ducknng manifest reply"));
    }
    if (typeof row.payload_text !== "string") {
      throw new Error("ducknng manifest reply is not JSON text");
    }
    return row.payload_text;
  } finally {
    closeDucknngConnection(instance, connection);
  }
}

async function rpcCallThroughDucknng(
  extensionPath: string,
  url: string,
  method: string,
  args: Record<string, unknown>,
  timeoutMs = 5000,
): Promise<unknown> {
  if (!method) throw new Error("ducknng RPC method is empty");
  const { instance, connection } = await openDucknngConnection(extensionPath);
  try {
    const reader = await connection.runAndReadAll(RPC_CALL_SQL, {
      url,
      method,
      payload: JSON.stringify(args),
      timeout: timeoutMs,
      tls_config_id: await clientTlsConfigId(connection, url),
    });
    const [row] = reader.getRowObjects();
    if (!row || row.type_name !== "result") {
      throw new Error(String(row?.error_text ?? "ducknng RPC call failed"));
    }
    if (row.name !== method) {
      throw new Error(`ducknng replied for ${String(row.name)}, not ${method}`);
    }
    const flags = Number(row.flags);
    const payload = blobBytes(row.payload);
    if ((flags & DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM) !== 0) {
      const ipcHex = Buffer.from(payload).toString("hex");
      const parsed = await connection.runAndReadAll(
        `SELECT * FROM ducknng_parse_body(
          from_hex($ipc_hex),
          'application/vnd.apache.arrow.stream'
        )`,
        { ipc_hex: ipcHex },
      );
      const rows = parsed.getRowObjects();
      if (rows.length === 1) {
        const entries = Object.entries(rows[0]);
        if (entries.length === 1 && entries[0][0] === "value") {
          return entries[0][1];
        }
      }
      return rows;
    }
    if ((flags & DUCKNNG_RPC_FLAG_PAYLOAD_JSON) !== 0) {
      return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(payload));
    }
    throw new Error(`ducknng RPC reply uses unsupported flags: ${flags}`);
  } finally {
    closeDucknngConnection(instance, connection);
  }
}

const StartParameters = Type.Object({}, { additionalProperties: false });
const SqlParameters = Type.Object(
  {
    sql: Type.String({ description: "DuckDB SQL; several statements may be separated by semicolons" }),
    max_rows: Type.Optional(Type.Integer({
      minimum: 1,
      maximum: MAX_SQL_ROWS,
      description: "Most rows of the last statement to return, 100 by default",
    })),
  },
  { additionalProperties: false },
);
const DescribeParameters = Type.Object(
  {
    url: Type.String({ description: "NNG endpoint URL" }),
  },
  { additionalProperties: false },
);
const CallParameters = Type.Object(
  {
    url: Type.String({ description: "NNG endpoint URL passed to ducknng_describe" }),
    method: Type.String({ description: "Method name returned by ducknng_describe" }),
    arguments: Type.Optional(
      Type.Record(Type.String(), Type.Unknown(), {
        description: "JSON arguments declared by the endpoint manifest",
      }),
    ),
    timeout_ms: Type.Optional(Type.Integer({
      minimum: 1000,
      maximum: 660_000,
      description:
        "How long to wait for the reply, 30000 by default. A method that waits, such as an eval with wait_ms, needs a longer timeout than its wait.",
    })),
  },
  { additionalProperties: false },
);

let localEndpoint: REndpoint | undefined;
let localEndpointStart: Promise<REndpoint> | undefined;
const manifests = new Map<string, EndpointManifest>();
const endpointTurns = new Map<string, Promise<void>>();
const adapterOwnedUrls = new Set<string>();

function refuseAdapterOwnedUrl(url: string): void {
  if (adapterOwnedUrls.has(url)) {
    throw new Error(
      "this URL is owned by the coordination adapter; use the coordination_* tools",
    );
  }
}

async function withEndpointTurn<T>(
  url: string,
  operation: () => Promise<T>,
): Promise<T> {
  const previous = endpointTurns.get(url) ?? Promise.resolve();
  let release = () => {};
  const gate = new Promise<void>((resolveGate) => {
    release = resolveGate;
  });
  const tail = previous.catch(() => undefined).then(() => gate);
  endpointTurns.set(url, tail);
  await previous.catch(() => undefined);
  try {
    return await operation();
  } finally {
    release();
    if (endpointTurns.get(url) === tail) endpointTurns.delete(url);
  }
}

// Drops an endpoint that is gone: its cached manifest, shared locator, and files.
async function forgetEndpoint(endpoint: REndpoint): Promise<void> {
  manifests.delete(endpoint.url);
  if (localEndpoint === endpoint) localEndpoint = undefined;
  await unshareLocator(endpoint);
  await rm(endpoint.work, { recursive: true, force: true });
}

async function startLocalREndpoint(): Promise<REndpoint> {
  if (localEndpoint) {
    if (!hasExited(localEndpoint)) return localEndpoint;
    await forgetEndpoint(localEndpoint);
  }
  localEndpointStart ??= startREndpoint(PACKAGE_ROOT);
  try {
    localEndpoint = await localEndpointStart;
    return localEndpoint;
  } finally {
    localEndpointStart = undefined;
  }
}

async function describeEndpoint(url: string): Promise<EndpointManifest> {
  return await withEndpointTurn(url, async () => {
    const extensionPath = await resolveDucknngExtension(PACKAGE_ROOT);
    const manifest = parseManifest(
      await manifestThroughDucknng(extensionPath, url),
    );
    manifests.set(url, manifest);
    return manifest;
  });
}

function declaredJsonMethod(url: string, method: string): EndpointMethod {
  const manifest = manifests.get(url);
  if (!manifest) {
    throw new Error("call ducknng_describe for this URL before invoking a method");
  }
  const descriptor = manifest.methods.find(
    (candidate) => candidate.name === method,
  );
  if (!descriptor) {
    throw new Error(`method is not declared by the endpoint manifest: ${method}`);
  }
  if (descriptor.request_payload_format !== "json") {
    throw new Error(
      `ducknng_call does not support request format: ${String(descriptor.request_payload_format)}`,
    );
  }
  return descriptor;
}

// The coordination adapter's own requests bypass the per-URL turn that orders
// model calls, so background polling never delays a model tool call.
async function callCoordinationEndpoint(
  url: string,
  method: string,
  args: Record<string, unknown>,
  options: { timeoutMs?: number } = {},
): Promise<unknown> {
  declaredJsonMethod(url, method);
  const extensionPath = await resolveDucknngExtension(PACKAGE_ROOT);
  return await rpcCallThroughDucknng(
    extensionPath,
    url,
    method,
    args,
    options.timeoutMs,
  );
}

async function callEndpointNow(
  url: string,
  method: string,
  args: Record<string, unknown> | undefined,
  timeoutMs: number,
): Promise<Record<string, unknown>> {
  declaredJsonMethod(url, method);
  const extensionPath = await resolveDucknngExtension(PACKAGE_ROOT);
  const response = await rpcCallThroughDucknng(
    extensionPath,
    url,
    method,
    args ?? {},
    timeoutMs,
  );

  const endpoint = localEndpoint?.url === url ? localEndpoint : undefined;
  const result: Record<string, unknown> = { method, result: response };
  if (endpoint?.child.pid !== undefined) result.endpoint_process = endpoint.child.pid;
  if (method !== "close") return result;

  manifests.delete(url);
  if (!endpoint) return result;
  try {
    const outcome = await waitForExit(endpoint.observer, 5000);
    if (outcome.code !== 0 || outcome.signal !== null) {
      throw new Error(
        `R endpoint exited with status ${outcome.code ?? outcome.signal}: ${endpoint.processOutput()}`,
      );
    }
  } catch (error) {
    try {
      await disposeREndpoint(endpoint);
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "R endpoint close failed and its process could not be stopped",
      );
    }
    throw error;
  } finally {
    if (localEndpoint === endpoint) localEndpoint = undefined;
  }
  await forgetEndpoint(endpoint);
  return result;
}

/**
 * Aborting a call to the R endpoint this extension placed interrupts that
 * endpoint's unfinished jobs, so the waiting call returns promptly and the R
 * session keeps its state. The interrupt bypasses the per-URL turn.
 */
function interruptOnAbort(url: string, signal: AbortSignal | undefined): () => void {
  const endpoint = localEndpoint?.url === url ? localEndpoint : undefined;
  if (!signal || !endpoint) return () => {};
  const interrupt = () => {
    if (!manifests.get(url)?.methods.some(({ name }) => name === "interrupt")) return;
    void rpcCallThroughDucknng(endpoint.extensionPath, url, "interrupt", {}).catch(() => undefined);
  };
  signal.addEventListener("abort", interrupt, { once: true });
  return () => signal.removeEventListener("abort", interrupt);
}

async function callEndpoint(
  url: string,
  method: string,
  args: Record<string, unknown> | undefined,
  options: { timeoutMs?: number; signal?: AbortSignal } = {},
): Promise<Record<string, unknown>> {
  return await withEndpointTurn(url, async () => {
    options.signal?.throwIfAborted();
    const release = interruptOnAbort(url, options.signal);
    try {
      return await callEndpointNow(url, method, args, options.timeoutMs ?? DEFAULT_CALL_TIMEOUT_MS);
    } catch (error) {
      const endpoint = localEndpoint?.url === url ? localEndpoint : undefined;
      if (endpoint && hasExited(endpoint)) await forgetEndpoint(endpoint);
      throw error;
    } finally {
      release();
    }
  });
}

async function disposeLocalEndpoint(): Promise<void> {
  let endpoint = localEndpoint;
  if (!endpoint && localEndpointStart) {
    endpoint = await localEndpointStart.catch(() => undefined);
  }
  localEndpoint = undefined;
  localEndpointStart = undefined;
  if (endpoint) {
    manifests.delete(endpoint.url);
    await withEndpointTurn(
      endpoint.url,
      async () => await disposeREndpoint(endpoint),
    );
  }
}

let sqlWorkspace: Promise<SqlWorkspace> | undefined;

// The workspace opens on first use in the session's working directory.
function workspaceFor(cwd: string): Promise<SqlWorkspace> {
  if (!sqlWorkspace) {
    const opening = openSqlWorkspace(cwd, async (database) => {
      const extensionPath = await resolveDucknngExtension(PACKAGE_ROOT);
      const { instance, connection } = await openDucknngConnection(extensionPath, database);
      return { connection, close: () => closeDucknngConnection(instance, connection) };
    });
    sqlWorkspace = opening;
    opening.catch(() => {
      if (sqlWorkspace === opening) sqlWorkspace = undefined;
    });
  }
  return sqlWorkspace;
}

async function closeSqlWorkspace(): Promise<void> {
  const opening = sqlWorkspace;
  sqlWorkspace = undefined;
  (await opening?.catch(() => undefined))?.close();
}

// The complete reply is parsed first; only the model-facing serialization is
// capped. BIGINT values from DuckDB become decimal strings.
function toolResult(value: unknown) {
  const serialized = JSON.stringify(
    value,
    (_key, item) => typeof item === "bigint" ? item.toString() : item,
  );
  const bytes = Buffer.byteLength(serialized, "utf8");
  const details = bytes <= 64 * 1024 ? JSON.parse(serialized) : {
    truncated: true,
    serialized_bytes: bytes,
    preview: new TextDecoder().decode(Buffer.from(serialized, "utf8").subarray(0, 60 * 1024)),
  };
  return {
    content: [{ type: "text" as const, text: JSON.stringify(details, null, 2) }],
    details,
  };
}

/**
 * Subscribes to one mailbox's wake-up hints. The subscriber owns one DuckDB
 * instance and one NNG SUB socket for its lifetime; hints are opaque topics
 * and carry no mail, so a lost hint only delays delivery until the next poll.
 */
export async function subscribeCoordinationEvents(
  url: string,
  topic: string,
  onHint: () => void,
): Promise<{ close(): Promise<void> }> {
  const extensionPath = await resolveDucknngExtension(PACKAGE_ROOT);
  const { instance, connection } = await openDucknngConnection(extensionPath);
  const socketCall = async (call: string, values: Record<string, unknown>) => {
    const reader = await connection.runAndReadAll(
      `SELECT s.ok, s.error, s.socket_id FROM (SELECT ${call} AS s)`,
      values as never,
    );
    const [row] = reader.getRowObjects();
    if (!row?.ok) throw new Error(String(row?.error ?? "ducknng socket call failed"));
    return row.socket_id;
  };
  let socket: unknown;
  try {
    const tls = await clientTlsConfigId(connection, url);
    socket = await socketCall("ducknng_open_socket('sub')", {});
    await socketCall("ducknng_subscribe_socket($id::UBIGINT, encode($topic))", { id: socket, topic });
    await socketCall("ducknng_dial_socket($id::UBIGINT, $url, 2000, $tls::UBIGINT)", { id: socket, url, tls });
  } catch (error) {
    closeDucknngConnection(instance, connection);
    throw error;
  }
  let open = true;
  const loop = (async () => {
    while (open) {
      const reader = await connection.runAndReadAll(
        "SELECT (ducknng_recv_socket_raw($id::UBIGINT, 500)).ok AS ok",
        { id: socket } as never,
      );
      if (open && reader.getRowObjects()[0]?.ok === true) onHint();
    }
  })();
  return {
    async close() {
      open = false;
      await loop.catch(() => undefined);
      try {
        await connection.run("SELECT ducknng_close_socket($id::UBIGINT)", { id: socket } as never);
      } finally {
        closeDucknngConnection(instance, connection);
      }
    },
  };
}

/**
 * The DuckDB and ducknng client used by the Pi adapter, for host processes
 * that attach an `AgentHarness` lane with `attachCoordinationLane`.
 */
export const ducknngCoordinationClient: CoordinationClient = {
  describe: describeEndpoint,
  call: callCoordinationEndpoint,
  subscribe: subscribeCoordinationEvents,
};

export default function piDucknngExtension(pi: ExtensionAPI): void {
  pi.registerTool({
    name: "persistent_r_start",
    label: "Start persistent R endpoint",
    description:
      "Start the local persistent R adapter and return its NNG URL. Describe that URL to obtain endpoint-owned methods, schemas, and examples before calling it. Code runs in named scopes that persist between calls and can be listed or reset; long evaluations run as jobs whose output can be followed and interrupted.",
    parameters: StartParameters,
    execute: async (_toolCallId, _params, signal) => {
      signal?.throwIfAborted();
      let endpoint: REndpoint;
      try {
        endpoint = await startLocalREndpoint();
        signal?.throwIfAborted();
      } catch (error) {
        if (signal?.aborted) await disposeLocalEndpoint();
        throw error;
      }
      return toolResult({ url: endpoint.url, endpoint_process: endpoint.child.pid });
    },
  });

  pi.registerTool({
    name: "ducknng_describe",
    label: "Describe NNG endpoint",
    description:
      "Fetch a compatible endpoint's ducknng RPC manifest through DuckDB and ducknng. Call this before ducknng_call for the same URL.",
    parameters: DescribeParameters,
    execute: async (_toolCallId, params, signal) => {
      signal?.throwIfAborted();
      if (!params.url) throw new Error("url is required");
      refuseAdapterOwnedUrl(params.url);
      const manifest = await describeEndpoint(params.url);
      signal?.throwIfAborted();
      const result: Record<string, unknown> = { manifest };
      if (localEndpoint?.url === params.url) result.endpoint_process = localEndpoint.child.pid;
      return toolResult(result);
    },
  });

  pi.registerTool({
    name: "ducknng_call",
    label: "Call NNG endpoint",
    description:
      "Invoke a method declared by ducknng_describe for the same URL through a fresh DuckDB client and ducknng RPC call.",
    parameters: CallParameters,
    execute: async (_toolCallId, params, signal) => {
      signal?.throwIfAborted();
      if (!params.url) throw new Error("url is required");
      if (!params.method) throw new Error("method is required");
      refuseAdapterOwnedUrl(params.url);
      const result = await callEndpoint(
        params.url,
        params.method,
        params.arguments,
        { timeoutMs: params.timeout_ms, signal },
      );
      signal?.throwIfAborted();
      return toolResult(result);
    },
  });

  pi.registerTool({
    name: "duckdb_sql",
    label: "DuckDB SQL workspace",
    description:
      `Run DuckDB SQL in this project's persistent workspace, ${WORKSPACE_FILE}, with ducknng loaded, and return the last statement's first rows. Tables persist across sessions: keep results that matter in tables and read back only what you need. After persistent_r_start, FROM r_eval('<R code>', scope := 'main') returns an R value as rows, so CREATE TABLE name AS FROM r_eval(...) saves it.`,
    parameters: SqlParameters,
    execute: async (_toolCallId, params, signal, _onUpdate, ctx) => {
      signal?.throwIfAborted();
      if (!params.sql) throw new Error("sql is required");
      const workspace = await workspaceFor(ctx.cwd);
      const endpoint = localEndpoint && !hasExited(localEndpoint) ? localEndpoint : undefined;
      const reply = await workspace.query(params.sql, {
        maxRows: params.max_rows,
        rUrl: endpoint?.url,
        signal,
      });
      return toolResult({ database: WORKSPACE_FILE, ...reply });
    },
  });

  registerCoordinationAdapter(pi, {
    describe: describeEndpoint,
    call: callCoordinationEndpoint,
    subscribe: subscribeCoordinationEvents,
    claim: (url) => adapterOwnedUrls.add(url),
    release: (url) => adapterOwnedUrls.delete(url),
  });

  pi.on("session_shutdown", async () => {
    await closeSqlWorkspace();
    await disposeLocalEndpoint();
  });
}
