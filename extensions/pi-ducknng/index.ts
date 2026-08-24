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

const execFileAsync = promisify(execFile);
const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const REQUEST_SQL = `
  SELECT ok, error, payload
  FROM ducknng_request(
    $url,
    encode($payload),
    $timeout,
    $tls_config_id::UBIGINT
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
    await execFileAsync("make", ["ducknng-extension"], {
      cwd: root,
      encoding: "utf8",
      maxBuffer: 8 * 1024 * 1024,
      timeout: 10 * 60 * 1000,
    });
    await access(extensionPath);
  }
  return extensionPath;
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
    connection?.closeSync();
    instance.closeSync();
    throw error;
  }
}

async function requestThroughDucknng(
  extensionPath: string,
  url: string,
  payload: string,
): Promise<string> {
  const { instance, connection } = await openDucknngConnection(extensionPath);
  try {
    const reader = await connection.runAndReadAll(REQUEST_SQL, {
      url,
      payload,
      timeout: 5000,
      tls_config_id: 0,
    });
    const [row] = reader.getRowObjects();
    if (!row || row.ok !== true) {
      throw new Error(String(row?.error ?? "ducknng returned no result"));
    }
    const blob = row.payload as
      | Uint8Array
      | { bytes?: Uint8Array }
      | null;
    const bytes = blob instanceof Uint8Array ? blob : blob?.bytes;
    if (!bytes) throw new Error("ducknng returned a non-BLOB payload");
    return Buffer.from(bytes).toString("utf8");
  } finally {
    connection.closeSync();
    instance.closeSync();
  }
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
    if (!observer.outcome() && !observer.error()) child.kill("SIGKILL");
    await waitForExit(observer, 2000).catch(() => undefined);
  }
}

export async function runDucknngProof(root = PACKAGE_ROOT): Promise<string> {
  const extensionPath = await ensureDucknngExtension(root);
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-proof-"));
  const locator = resolve(work, "endpoint.url");
  const endpointScript = resolve(root, "tools/pi-r-endpoint.R");
  const child = spawn(
    process.env.RSCRIPT ?? "Rscript",
    ["--vanilla", endpointScript, locator],
    { cwd: root, stdio: ["ignore", "pipe", "pipe"] },
  );
  const processOutput = collectProcessOutput(child);
  const observer = observeChild(child);
  let url: string | undefined;
  let stopped = false;

  try {
    url = await waitForLocator(locator, observer, processOutput);
    const setReply = await requestThroughDucknng(
      extensionPath,
      url,
      "set:x=41",
    );
    if (setReply !== "set:x=41") {
      throw new Error(`unexpected set reply: ${setReply}`);
    }

    const evalReply = await requestThroughDucknng(
      extensionPath,
      url,
      "eval:x+1",
    );
    if (evalReply !== "42") {
      throw new Error(`unexpected evaluation reply: ${evalReply}`);
    }

    const stopReply = await requestThroughDucknng(extensionPath, url, "stop");
    if (stopReply !== "stopped") {
      throw new Error(`unexpected stop reply: ${stopReply}`);
    }
    const outcome = await waitForExit(observer, 5000);
    if (outcome.code !== 0 || outcome.signal !== null) {
      throw new Error(
        `R endpoint exited with status ${outcome.code ?? outcome.signal}: ${processOutput()}`,
      );
    }
    stopped = true;

    return [
      "Pi extension: loaded DuckDB API and ducknng",
      "Pi client A: set x = 41 through DuckDB -> ducknng -> NNG -> R",
      "Pi client A: disconnected",
      "Pi client B: reconnected through a fresh DuckDB instance",
      "Pi client B: evaluated x + 1 = 42 in the same persistent R session",
      "R endpoint: stopped",
    ].join("\n");
  } finally {
    if (!stopped && url && !observer.outcome() && !observer.error()) {
      try {
        await requestThroughDucknng(extensionPath, url, "stop");
        await waitForExit(observer, 2000);
      } catch {
        await terminateChild(child, observer);
      }
    }
    if (!stopped) await terminateChild(child, observer);
    await rm(work, { recursive: true, force: true });
  }
}

export default function piDucknngExtension(pi: ExtensionAPI): void {
  pi.registerCommand("ducknng-proof", {
    description: "Prove Pi reaches persistent R through DuckDB and ducknng",
    handler: async (_args, ctx) => {
      if (ctx.hasUI) {
        ctx.ui.notify("Running the Pi to persistent-R ducknng proof...", "info");
      }
      try {
        const output = await runDucknngProof();
        if (ctx.hasUI) ctx.ui.notify(output, "info");
      } catch (error) {
        if (ctx.hasUI) {
          ctx.ui.notify(
            error instanceof Error ? error.message : String(error),
            "error",
          );
        }
        throw error;
      }
    },
  });
}
