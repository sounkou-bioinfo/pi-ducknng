import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import piDucknngExtension from "../extensions/pi-ducknng/index.ts";

function loadTools() {
  const tools = new Map();
  let shutdown;
  piDucknngExtension({
    registerTool(definition) {
      tools.set(definition.name, definition);
    },
    registerCommand() {},
    on(event, handler) {
      if (event === "session_shutdown") shutdown = handler;
    },
  });
  return { tools, shutdown: () => shutdown?.() };
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
    ["persistent_r_start", "ducknng_describe", "ducknng_call"],
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
        arguments: { code: "data.frame(a = 1:2, b = c('x', 'y'))" },
      },
      signal,
    );
    assert.deepEqual(table.details.result, [
      { a: 1, b: "x" },
      { a: 2, b: "y" },
    ]);

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
