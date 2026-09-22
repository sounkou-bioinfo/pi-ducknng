import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";

import piDucknngExtension from "../extensions/pi-ducknng/index.ts";

const ROOT = resolve(import.meta.dirname, "..");

async function startEndpoint(work) {
  const locator = resolve(work, `endpoint-${Date.now()}.url`);
  const child = spawn(
    process.execPath,
    ["tools/pi-coordination-endpoint.ts", locator, resolve(work, "coordination.duckdb")],
    { cwd: ROOT, stdio: "ignore" },
  );
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`coordination endpoint exited with ${child.exitCode}`);
    }
    const url = (await readFile(locator, "utf8").catch(() => "")).trim();
    if (url) return { child, url };
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 50));
  }
  throw new Error("timed out waiting for coordination endpoint");
}

async function stopEndpoint(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill();
  await new Promise((resolveExit) => child.once("exit", resolveExit));
}

function waitFor(predicate, timeout, label) {
  const deadline = Date.now() + timeout;
  return new Promise((resolveWait, reject) => {
    const check = () => {
      if (predicate()) return resolveWait();
      if (Date.now() >= deadline) return reject(new Error(`timed out: ${label}`));
      setTimeout(check, 20);
    };
    check();
  });
}

function session({ url, agent, mode, sessionId }) {
  const flags = new Map([
    ["ducknng-coordination-url", url],
    ["ducknng-coordination-project", "project-one"],
    ["ducknng-agent-id", agent],
  ]);
  const tools = new Map();
  const handlers = new Map();
  const steered = [];
  const entries = [];
  const notifications = [];
  let active = [];
  piDucknngExtension({
    registerTool(definition) {
      tools.set(definition.name, definition);
      active.push(definition.name);
    },
    registerCommand() {},
    registerFlag() {},
    getFlag: (name) => flags.get(name),
    getActiveTools: () => [...active],
    setActiveTools(names) {
      active = [...names];
    },
    on(event, handler) {
      const registered = handlers.get(event) ?? [];
      registered.push(handler);
      handlers.set(event, registered);
    },
    sendMessage(message, options) {
      steered.push({ message, options, at: Date.now() });
    },
    appendEntry(customType, data) {
      entries.push({ type: "custom", customType, data });
    },
  });
  const context = {
    mode,
    cwd: "/work/project",
    sessionManager: {
      getSessionId: () => sessionId,
      getEntries: () => entries,
    },
    isIdle: () => true,
    hasPendingMessages: () => false,
    ui: {
      notify(message) {
        notifications.push(message);
      },
    },
  };
  const emit = async (event, payload = { type: event }) => {
    let result;
    for (const handler of handlers.get(event) ?? []) {
      result = await handler(payload, context);
    }
    return result;
  };
  const tool = async (name, params, id = `${name}-${Date.now()}`) =>
    (await tools.get(name).execute(id, params, new AbortController().signal, undefined, context))
      .details;
  return { emit, tool, tools, steered, notifications };
}

test("two Pi sessions coordinate through the manifested endpoint", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-coordination-test-"));
  let endpoint = await startEndpoint(work);
  const url = endpoint.url;
  assert.match(url, /^ipc:\/\/.*coordination\.ipc$/);
  const alice = session({ url, agent: "alice", mode: "print", sessionId: "alice-session" });
  const bob = session({ url, agent: "bob", mode: "tui", sessionId: "bob-session" });
  try {
    await alice.emit("session_start");
    await bob.emit("session_start");

    await assert.rejects(
      alice.tools.get("ducknng_describe").execute("describe", { url }),
      /owned by the coordination adapter/,
    );

    const agents = await alice.tool("coordination_agents", {});
    assert.deepEqual(agents.agents.map(({ agent_id }) => agent_id), ["alice", "bob"]);

    const sentAt = Date.now();
    const sent = await alice.tool("coordination_send", {
      recipient: "bob",
      content: "Review src/model.R.\nReply with one sentence.",
    });
    assert.equal(sent.recipient_seen, true);
    await waitFor(() => bob.steered.length === 1, 5000, "bob steering");
    assert.ok(bob.steered[0].at - sentAt < 800, "a wake-up hint delivers before any poll");
    assert.match(bob.steered[0].message.content, /from="alice"/);
    assert.match(bob.steered[0].message.content, /Review src\/model\.R\.\nReply/);

    const typo = await alice.tool("coordination_send", { recipient: "bbo", content: "x" });
    assert.equal(typo.recipient_seen, false);

    const lease = await bob.tool("coordination_reserve", { resource: "src" });
    assert.equal(lease.resource, "file:///work/project/src");
    const blocked = await alice.emit("tool_call", {
      type: "tool_call",
      toolCallId: "edit-1",
      toolName: "edit",
      input: { path: "src/model.R" },
    });
    assert.equal(blocked?.block, true);
    assert.match(blocked.reason, /held by bob/);
    const own = await bob.emit("tool_call", {
      type: "tool_call",
      toolCallId: "edit-2",
      toolName: "write",
      input: { path: "src/model.R" },
    });
    assert.equal(own, undefined);
    await bob.tool("coordination_release", { resource: "src" });
    const allowed = await alice.emit("tool_call", {
      type: "tool_call",
      toolCallId: "edit-3",
      toolName: "edit",
      input: { path: "src/model.R" },
    });
    assert.equal(allowed, undefined);

    await stopEndpoint(endpoint.child);
    endpoint = await startEndpoint(work);
    assert.equal(endpoint.url, url, "the ipc address is stable across restarts");
    await alice.tool("coordination_send", { recipient: "bob", content: "after restart" });
    await waitFor(() => bob.steered.length === 2, 15000, "delivery after restart");
    assert.match(bob.steered[1].message.content, /after restart/);

    const reply = await bob.tool("coordination_send", { recipient: "alice", content: "done" });
    assert.ok(reply.message_id);
    const inbox = await alice.tool("coordination_inbox", { wait_ms: 2000 });
    assert.deepEqual(inbox.messages.map(({ content }) => content), ["done"]);
  } finally {
    await alice.emit("session_shutdown");
    await bob.emit("session_shutdown");
    await stopEndpoint(endpoint.child);
    await rm(work, { recursive: true, force: true });
  }
});

test("the endpoint command takes its listeners and TLS files as options", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-endpoint-options-"));
  const run = (...args) => new Promise((resolveRun) => {
    const child = spawn(process.execPath, ["tools/pi-coordination-endpoint.ts", ...args],
      { cwd: ROOT, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout.on("data", (chunk) => { output += chunk; });
    child.stderr.on("data", (chunk) => { output += chunk; });
    child.once("exit", (code) => resolveRun({ code, output }));
    setTimeout(() => child.kill(), 10000).unref();
  });
  const database = resolve(work, "c.duckdb");
  try {
    const half = await run(resolve(work, "a.url"), database, "--tls-cert-key", "server.pem");
    assert.equal(half.code, 1);
    assert.match(half.output, /mutual TLS needs both --tls-cert-key and --tls-ca/);
    const ungranted = await run(resolve(work, "b.url"), database,
      "--tls-cert-key", "server.pem", "--tls-ca", "ca.pem");
    assert.match(ungranted.output, /a mutual-TLS endpoint needs --grants/);

    const locator = resolve(work, "sse.url");
    const child = spawn(process.execPath,
      ["tools/pi-coordination-endpoint.ts", locator, database, "--sse", "http://127.0.0.1:0"],
      { cwd: ROOT, stdio: ["ignore", "pipe", "ignore"] });
    try {
      const announced = await new Promise((resolveLine, reject) => {
        child.stdout.once("data", (chunk) => resolveLine(String(chunk)));
        child.once("exit", (code) => reject(new Error(`endpoint exited with ${code}`)));
      });
      assert.match(announced, /^server-sent events: http:\/\/127\.0\.0\.1:\d+\/events\?topic=<topic>/);
    } finally {
      await stopEndpoint(child);
    }
  } finally {
    await rm(work, { recursive: true, force: true });
  }
});
