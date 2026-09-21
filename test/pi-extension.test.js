import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";

import piDucknngExtension from "../extensions/pi-ducknng/index.ts";

function loadTools() {
  const tools = new Map();
  const handlers = new Map();
  piDucknngExtension({
    registerTool(definition) {
      tools.set(definition.name, definition);
    },
    registerCommand() {},
    registerFlag() {},
    getFlag() {},
    on(event, handler) {
      const registered = handlers.get(event) ?? [];
      registered.push(handler);
      handlers.set(event, registered);
    },
  });
  return {
    tools,
    shutdown: async () => {
      for (const handler of handlers.get("session_shutdown") ?? []) {
        await handler();
      }
    },
  };
}

test("package manifest declares the Pi extension", async () => {
  const manifest = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  );
  assert.ok(manifest.keywords.includes("pi-package"));
  assert.deepEqual(manifest.pi.extensions, ["./extensions/pi-ducknng/index.ts"]);
});

test("R adapter spawn failure returns promptly", async () => {
  const previous = process.env.RSCRIPT;
  process.env.RSCRIPT = "pi-ducknng-missing-rscript";
  const { tools, shutdown } = loadTools();
  const started = Date.now();
  try {
    await assert.rejects(
      tools.get("persistent_r_start").execute(
        "start",
        {},
        new AbortController().signal,
      ),
      /ENOENT|spawn/,
    );
    assert.ok(Date.now() - started < 5000);
  } finally {
    await shutdown();
    if (previous === undefined) delete process.env.RSCRIPT;
    else process.env.RSCRIPT = previous;
  }
});

test("model tools discover and call the ducknng manifest", async () => {
  const { tools, shutdown } = loadTools();
  assert.deepEqual(
    [...tools.keys()],
    [
      "persistent_r_start",
      "ducknng_describe",
      "ducknng_call",
      "coordination_send",
      "coordination_inbox",
      "coordination_agents",
      "coordination_reserve",
      "coordination_release",
    ],
  );
  const signal = new AbortController().signal;
  try {
    const started = await tools.get("persistent_r_start").execute(
      "start",
      {},
      signal,
    );
    const { url } = started.details;
    const described = await tools.get("ducknng_describe").execute(
      "describe",
      { url },
      signal,
    );
    const manifestReceipt = JSON.parse(described.content[0].text);
    assert.equal(manifestReceipt.manifest.server.protocol_version, 1);
    assert.deepEqual(
      manifestReceipt.manifest.methods.map(({ name }) => name),
      ["eval", "close"],
    );
    const evalMethod = manifestReceipt.manifest.methods.find(
      ({ name }) => name === "eval",
    );
    assert.match(
      evalMethod.request_schema.properties.code.examples[0],
      /datasets::mtcars/,
    );

    const set = await tools.get("ducknng_call").execute(
      "set",
      { url, method: "eval", arguments: { code: "x <- 41L" } },
      signal,
    );
    const bound = await tools.get("ducknng_call").execute(
      "bind",
      {
        url,
        method: "eval",
        arguments: {
          code: [
            "e <- new.env(parent = baseenv())",
            "makeActiveBinding('answer', function(value) {",
            "  if (!missing(value)) stop('answer is read-only')",
            "  x + 1L",
            "}, e)",
            "identical(parent.env(e), baseenv())",
          ].join("; "),
        },
      },
      signal,
    );
    const evaluated = await tools.get("ducknng_call").execute(
      "eval",
      { url, method: "eval", arguments: { code: "answer", envir: "e" } },
      signal,
    );
    assert.equal(set.details.result, 41);
    assert.equal(bound.details.result, true);
    assert.equal(evaluated.details.result, 42);
    assert.equal(set.details.endpoint_process, evaluated.details.endpoint_process);
    assert.match(evaluated.details.duckdb_client, /fresh in-memory instance closed/);

    const table = await tools.get("ducknng_call").execute(
      "table",
      {
        url,
        method: "eval",
        arguments: {
          code: [
            "mpg_by_cyl <- aggregate(mpg ~ cyl,",
            "data = datasets::mtcars, FUN = mean); mpg_by_cyl",
          ].join(" "),
        },
      },
      signal,
    );
    assert.deepEqual(
      table.details.result.map(({ cyl }) => cyl),
      [4, 6, 8],
    );
    assert.ok(Math.abs(table.details.result[0].mpg - 26.66364) < 1e-5);

    const transformed = await tools.get("ducknng_call").execute(
      "transform",
      {
        url,
        method: "eval",
        arguments: {
          code: [
            "transform(mpg_by_cyl,",
            "  delta_from_4cyl = mpg - mpg[cyl == 4])",
          ].join(" "),
        },
      },
      signal,
    );
    assert.equal(transformed.details.result[0].delta_from_4cyl, 0);
    assert.ok(
      Math.abs(transformed.details.result[2].delta_from_4cyl + 11.56364) < 1e-5,
    );

    const increments = await Promise.all([
      tools.get("ducknng_call").execute(
        "increment-a",
        {
          url,
          method: "eval",
          arguments: {
            code: "current <- x; Sys.sleep(0.1); x <- current + 1L; x",
          },
        },
        signal,
      ),
      tools.get("ducknng_call").execute(
        "increment-b",
        {
          url,
          method: "eval",
          arguments: {
            code: "current <- x; Sys.sleep(0.1); x <- current + 1L; x",
          },
        },
        signal,
      ),
    ]);
    assert.deepEqual(increments.map(({ details }) => details.result), [42, 43]);

    await assert.rejects(
      tools.get("ducknng_call").execute(
        "missing",
        { url, method: "missing", arguments: {} },
        signal,
      ),
      /not declared by the endpoint manifest/,
    );

    const closed = await tools.get("ducknng_call").execute(
      "close",
      { url, method: "close", arguments: {} },
      signal,
    );
    assert.deepEqual(closed.details.result, { closed: true });
  } finally {
    await shutdown();
  }
});

function exited(child) {
  return new Promise((resolveExit) => {
    if (child.exitCode !== null || child.signalCode !== null) resolveExit();
    else child.once("exit", resolveExit);
  });
}

test("R endpoint outlives idle polls and exits with its parent", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-parent-test-"));
  const parent = spawn("sleep", ["60"], { stdio: "ignore" });
  const endpoint = spawn(
    process.env.RSCRIPT ?? "Rscript",
    ["--vanilla", "tools/pi-r-endpoint.R", resolve(work, "endpoint.url")],
    {
      cwd: resolve(import.meta.dirname, ".."),
      env: { ...process.env, PI_DUCKNNG_PARENT_PID: String(parent.pid) },
      stdio: "ignore",
    },
  );
  try {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      const url = await readFile(resolve(work, "endpoint.url"), "utf8").catch(() => "");
      if (url.trim()) break;
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 3000));
    assert.equal(endpoint.exitCode, null, "endpoint exited while its parent lived");
    parent.kill();
    await exited(parent);
    const stopped = Date.now();
    await Promise.race([
      exited(endpoint),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("endpoint outlived its parent")), 5000)),
    ]);
    assert.ok(Date.now() - stopped < 5000);
  } finally {
    parent.kill();
    endpoint.kill("SIGKILL");
    await rm(work, { recursive: true, force: true });
  }
});
