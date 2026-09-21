import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";

import piDucknngExtension from "../extensions/pi-ducknng/index.ts";

async function waitForLocator(path, child) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`coordination endpoint exited with ${child.exitCode}`);
    }
    try {
      const url = (await readFile(path, "utf8")).trim();
      if (url) return url;
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
  }
  throw new Error("timed out waiting for coordination endpoint");
}

function loadTools() {
  const tools = new Map();
  piDucknngExtension({
    registerTool(definition) {
      tools.set(definition.name, definition);
    },
    registerCommand() {},
    registerFlag() {},
    getFlag() {},
    on() {},
  });
  return tools;
}

async function call(tools, url, method, args) {
  const response = await tools.get("ducknng_call").execute(
    method,
    { url, method, arguments: args },
    new AbortController().signal,
  );
  return response.details.result;
}

test("coordination endpoint exposes durable manifested mail", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-coordination-test-"));
  const locator = resolve(work, "endpoint.url");
  const database = resolve(work, "coordination.duckdb");
  const child = spawn(
    process.env.RSCRIPT ?? "Rscript",
    ["--vanilla", "tools/pi-coordination-endpoint.R", locator, database],
    { cwd: resolve(import.meta.dirname, ".."), stdio: "ignore" },
  );
  try {
    const url = await waitForLocator(locator, child);
    const tools = loadTools();
    const described = await tools.get("ducknng_describe").execute(
      "describe",
      { url },
      new AbortController().signal,
    );
    const methods = described.details.manifest.methods.map(({ name }) => name);
    assert.deepEqual(methods, [
      "register",
      "heartbeat",
      "list_agents",
      "send",
      "receive",
      "ack",
      "reserve",
      "release",
      "unregister",
    ]);
    assert.ok(!methods.includes("eval"));

    const alice = await call(tools, url, "register", {
      project_id: "project-one",
      agent_id: "alice",
      instance_id: "alice-session",
      operation_key: "register-alice",
    });
    const sent = await call(tools, url, "send", {
      registration_id: alice.registration_id,
      recipient_agent_id: "bob",
      idempotency_key: "handoff-1",
      content: "Read the handoff.",
    });
    const bob = await call(tools, url, "register", {
      project_id: "project-one",
      agent_id: "bob",
      instance_id: "bob-session",
      operation_key: "register-bob",
    });
    const received = await call(tools, url, "receive", {
      registration_id: bob.registration_id,
      visibility_timeout_ms: 5000,
    });
    assert.equal(received.messages[0].message_id, sent.message_id);
    const acknowledged = await call(tools, url, "ack", {
      registration_id: bob.registration_id,
      receipt_token: received.messages[0].receipt_token,
      delivery_ref: "test-session-entry",
    });
    assert.equal(acknowledged.acknowledged, true);
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill();
      await new Promise((resolveExit) => child.once("exit", resolveExit));
    }
    await rm(work, { recursive: true, force: true });
  }
});
