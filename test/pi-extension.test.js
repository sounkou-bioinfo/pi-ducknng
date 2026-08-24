import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";

import piDucknngExtension, {
  runPersistentRProof,
} from "../extensions/pi-ducknng/index.ts";

const root = fileURLToPath(new URL("..", import.meta.url));

test("package manifest declares the Pi extension", async () => {
  const manifest = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  );
  assert.ok(manifest.keywords.includes("pi-package"));
  assert.deepEqual(manifest.pi.extensions, ["./extensions/pi-ducknng/index.ts"]);
});

test("persistent R proof runner uses its requested package root", async () => {
  const output = await runPersistentRProof(root);
  assert.match(output, /persistent R daemon reconnected with x \+ 1 = 42/);
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
    /persistent R daemon reconnected with x \+ 1 = 42/,
  );
  assert.equal(notifications.at(-1).level, "info");
});
