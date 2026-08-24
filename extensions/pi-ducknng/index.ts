import { execFile, spawn, type ChildProcess } from "node:child_process";
import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  DuckDBInstance,
  type DuckDBConnection,
} from "@duckdb/node-api";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";

const execFileAsync = promisify(execFile);
const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const DUCKNNG_WIRE_VERSION = 1;
const DUCKNNG_RPC_CALL = 1;
const DUCKNNG_RPC_FLAG_PAYLOAD_JSON = 4;
const DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM = 8;
const extensionBuilds = new Map<string, Promise<void>>();
const MANIFEST_SQL = `
  SELECT type_name, name, flags, error AS error_text, payload_text
  FROM ducknng_decode_frame(
    ducknng_get_rpc_manifest_raw($url, $tls_config_id::UBIGINT)
  )`;
const RPC_CALL_SQL = `
  SELECT type_name, name, flags, error AS error_text, payload
  FROM ducknng_decode_frame(
    ducknng_request_raw(
      $url,
      from_hex($frame_hex),
      $timeout,
      $tls_config_id::UBIGINT
    )
  )`;

async function ensureDucknngExtension(root: string): Promise<string> {
  const configured = process.env.DUCKNNG_EXTENSION_PATH;
  const extensionPath = configured
    ? resolve(configured)
    : resolve(root, "vendor/ducknng/build/release/ducknng.duckdb_extension");
  try {
    await access(extensionPath);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== "ENOENT") throw error;
    if (configured) {
      throw new Error(`DUCKNNG_EXTENSION_PATH does not exist: ${extensionPath}`);
    }
    let build = extensionBuilds.get(extensionPath);
    if (!build) {
      build = (async () => {
        await execFileAsync("make", ["ducknng-extension"], {
          cwd: root,
          encoding: "utf8",
          maxBuffer: 8 * 1024 * 1024,
          timeout: 10 * 60 * 1000,
        });
        await access(extensionPath);
      })();
      extensionBuilds.set(extensionPath, build);
    }
    try {
      await build;
    } finally {
      if (extensionBuilds.get(extensionPath) === build) {
        extensionBuilds.delete(extensionPath);
      }
    }
  }
  return extensionPath;
}

function closeDucknngConnection(
  instance: DuckDBInstance,
  connection?: DuckDBConnection,
): void {
  try {
    connection?.closeSync();
  } finally {
    instance.closeSync();
  }
}

async function openDucknngConnection(
  extensionPath: string,
): Promise<{ instance: DuckDBInstance; connection: DuckDBConnection }> {
  const instance = await DuckDBInstance.create(":memory:", {
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
    if (observer.error()) throw observer.error();
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
};

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

async function startREndpoint(root: string): Promise<REndpoint> {
  const extensionPath = await ensureDucknngExtension(root);
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-endpoint-"));
  const locator = resolve(work, "endpoint.url");
  const endpointScript = resolve(root, "tools/pi-r-endpoint.R");
  const child = spawn(
    process.env.RSCRIPT ?? "Rscript",
    ["--vanilla", endpointScript, locator],
    { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
  );
  const processOutput = collectProcessOutput(child);
  const observer = observeChild(child);
  try {
    const url = await waitForLocator(locator, observer, processOutput);
    return { extensionPath, work, url, child, observer, processOutput };
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
  if (!endpoint.observer.outcome() && !endpoint.observer.error()) {
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

function buildRpcCallFrame(
  method: string,
  args: Record<string, unknown>,
): Uint8Array {
  const name = Buffer.from(method, "utf8");
  const payload = Buffer.from(JSON.stringify(args), "utf8");
  if (name.length === 0) throw new Error("ducknng RPC method is empty");
  const header = Buffer.alloc(22);
  header.writeUInt8(DUCKNNG_WIRE_VERSION, 0);
  header.writeUInt8(DUCKNNG_RPC_CALL, 1);
  header.writeUInt32LE(DUCKNNG_RPC_FLAG_PAYLOAD_JSON, 2);
  header.writeUInt32LE(name.length, 6);
  header.writeUInt32LE(0, 10);
  header.writeBigUInt64LE(BigInt(payload.length), 14);
  return Buffer.concat([header, name, payload]);
}

async function manifestThroughDucknng(
  extensionPath: string,
  url: string,
): Promise<string> {
  const { instance, connection } = await openDucknngConnection(extensionPath);
  try {
    const reader = await connection.runAndReadAll(MANIFEST_SQL, {
      url,
      tls_config_id: 0,
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
): Promise<unknown> {
  const frameHex = Buffer.from(buildRpcCallFrame(method, args)).toString("hex");
  const { instance, connection } = await openDucknngConnection(extensionPath);
  try {
    const reader = await connection.runAndReadAll(RPC_CALL_SQL, {
      url,
      frame_hex: frameHex,
      timeout: 5000,
      tls_config_id: 0,
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
  },
  { additionalProperties: false },
);

let localEndpoint: REndpoint | undefined;
let localEndpointStart: Promise<REndpoint> | undefined;
const manifests = new Map<string, EndpointManifest>();
const endpointTurns = new Map<string, Promise<void>>();

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

async function startLocalREndpoint(): Promise<REndpoint> {
  if (localEndpoint) {
    const exited =
      localEndpoint.observer.outcome() ||
      localEndpoint.observer.error() ||
      localEndpoint.child.exitCode !== null ||
      localEndpoint.child.signalCode !== null;
    if (!exited) return localEndpoint;
    const stale = localEndpoint;
    localEndpoint = undefined;
    manifests.delete(stale.url);
    await rm(stale.work, { recursive: true, force: true });
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
    const extensionPath = await ensureDucknngExtension(PACKAGE_ROOT);
    const manifest = parseManifest(
      await manifestThroughDucknng(extensionPath, url),
    );
    manifests.set(url, manifest);
    return manifest;
  });
}

async function callEndpointNow(
  url: string,
  method: string,
  args: Record<string, unknown> | undefined,
): Promise<Record<string, unknown>> {
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

  const extensionPath = await ensureDucknngExtension(PACKAGE_ROOT);
  const response = await rpcCallThroughDucknng(
    extensionPath,
    url,
    method,
    args ?? {},
  );

  const endpoint = localEndpoint?.url === url ? localEndpoint : undefined;
  const result: Record<string, unknown> = {
    method,
    result: response,
    duckdb_client: "fresh in-memory instance closed after reply",
  };
  if (endpoint?.child.pid !== undefined) {
    result.endpoint_process = endpoint.child.pid;
  }

  if (method === "close") {
    manifests.delete(url);
    if (endpoint) {
      try {
        const outcome = await waitForExit(endpoint.observer, 5000);
        if (outcome.code !== 0 || outcome.signal !== null) {
          throw new Error(
            `R endpoint exited with status ${outcome.code ?? outcome.signal}: ${endpoint.processOutput()}`,
          );
        }
        await rm(endpoint.work, { recursive: true, force: true });
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
    }
  }
  return result;
}

async function callEndpoint(
  url: string,
  method: string,
  args: Record<string, unknown> | undefined,
): Promise<Record<string, unknown>> {
  return await withEndpointTurn(url, async () => {
    try {
      return await callEndpointNow(url, method, args);
    } catch (error) {
      const endpoint = localEndpoint?.url === url ? localEndpoint : undefined;
      const exited = endpoint && (
        endpoint.observer.outcome() ||
        endpoint.observer.error() ||
        endpoint.child.exitCode !== null ||
        endpoint.child.signalCode !== null
      );
      if (endpoint && exited) {
        manifests.delete(url);
        localEndpoint = undefined;
        await rm(endpoint.work, { recursive: true, force: true });
      }
      throw error;
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

function normalizeToolResult(value: unknown): unknown {
  return JSON.parse(
    JSON.stringify(
      value,
      (_key, item) => typeof item === "bigint" ? item.toString() : item,
    ),
  );
}

function toolResultDetails(value: unknown): unknown {
  const normalized = normalizeToolResult(value);
  const serialized = JSON.stringify(normalized);
  if (Buffer.byteLength(serialized, "utf8") <= 64 * 1024) return normalized;
  const preview = new TextDecoder().decode(
    Buffer.from(serialized, "utf8").subarray(0, 60 * 1024),
  );
  return {
    truncated: true,
    serialized_bytes: Buffer.byteLength(serialized, "utf8"),
    preview,
  };
}

function stringifyToolResult(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export default function piDucknngExtension(pi: ExtensionAPI): void {
  pi.registerTool({
    name: "persistent_r_start",
    label: "Start persistent R endpoint",
    description:
      "Start the local persistent R adapter and return its NNG URL. Describe that URL to obtain endpoint-owned methods, schemas, and examples before calling it.",
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
      const result = {
        url: endpoint.url,
        endpoint_process: endpoint.child.pid,
      };
      const details = toolResultDetails(result);
      return {
        content: [{ type: "text", text: stringifyToolResult(details) }],
        details,
      };
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
      const manifest = await describeEndpoint(params.url);
      signal?.throwIfAborted();
      const result: Record<string, unknown> = {
        manifest,
        request_path: "@duckdb/node-api -> DuckDB -> ducknng -> NNG",
        duckdb_client: "fresh in-memory instance closed after manifest reply",
      };
      if (localEndpoint?.url === params.url) {
        result.endpoint_process = localEndpoint.child.pid;
      }
      const details = toolResultDetails(result);
      return {
        content: [{ type: "text", text: stringifyToolResult(details) }],
        details,
      };
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
      const result = await callEndpoint(
        params.url,
        params.method,
        params.arguments,
      );
      signal?.throwIfAborted();
      const details = toolResultDetails(result);
      return {
        content: [{ type: "text", text: stringifyToolResult(details) }],
        details,
      };
    },
  });

  pi.on("session_shutdown", async () => {
    await disposeLocalEndpoint();
  });

}
