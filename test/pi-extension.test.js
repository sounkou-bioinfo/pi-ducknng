import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";

import piDucknngExtension, {
  runDucknngProof,
} from "../extensions/pi-ducknng/index.ts";

const root = fileURLToPath(new URL("..", import.meta.url));

test("package manifest declares the Pi extension", async () => {
  const manifest = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  );
  assert.ok(manifest.keywords.includes("pi-package"));
  assert.deepEqual(manifest.pi.extensions, ["./extensions/pi-ducknng/index.ts"]);
});

test("proof traverses DuckDB and ducknng into persistent R", async () => {
  const output = await runDucknngProof(root);
  assert.match(output, /Pi client A: set x = 41 through DuckDB -> ducknng -> NNG -> R/);
  assert.match(output, /Pi client B: evaluated x \+ 1 = 42 in the same persistent R session/);
});

test("proof fails promptly when Rscript cannot spawn", async () => {
  const previous = process.env.RSCRIPT;
  process.env.RSCRIPT = "pi-ducknng-missing-rscript";
  const started = Date.now();
  try {
    await assert.rejects(runDucknngProof(root), /ENOENT|spawn/);
    assert.ok(Date.now() - started < 5000);
  } finally {
    if (previous === undefined) delete process.env.RSCRIPT;
    else process.env.RSCRIPT = previous;
  }
});

test("Pi extension registers an executable proof command", async () => {
  let command;
  const notifications = [];
  piDucknngExtension({
    registerCommand(name, definition) {
      command = { name, definition };
    },
  });

  assert.equal(command.name, "ducknng-proof");
  await command.definition.handler("", {
    hasUI: true,
    ui: {
      notify(message, level) {
        notifications.push({ message, level });
      },
    },
  });

  assert.match(
    notifications.at(-1).message,
    /Pi client B: evaluated x \+ 1 = 42 in the same persistent R session/,
  );
  assert.equal(notifications.at(-1).level, "info");
});
