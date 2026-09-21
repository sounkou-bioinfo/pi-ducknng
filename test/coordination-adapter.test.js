import assert from "node:assert/strict";
import test from "node:test";

import {
  COORDINATION_TOOLS,
  coordinationEnvelope,
  coordinationErrorCode,
  coordinationResource,
  registerCoordinationAdapter,
} from "../extensions/pi-ducknng/coordination.ts";

function waitFor(predicate, timeout = 2000) {
  const deadline = Date.now() + timeout;
  return new Promise((resolve, reject) => {
    const check = () => {
      if (predicate()) return resolve();
      if (Date.now() >= deadline) return reject(new Error("condition timed out"));
      setTimeout(check, 10);
    };
    check();
  });
}

function adapterHarness({ entries = [], mode, configured = true } = {}) {
  const flags = new Map(configured
    ? [
      ["ducknng-coordination-url", "ipc:///tmp/coordination.ipc"],
      ["ducknng-coordination-project", "project-one"],
      ["ducknng-agent-id", "bob"],
    ]
    : []);
  const handlers = new Map();
  const tools = new Map();
  const messages = [];
  const notifications = [];
  let activeTools = [];
  const pi = {
    registerFlag() {},
    registerTool(definition) {
      tools.set(definition.name, definition);
      activeTools.push(definition.name);
    },
    getActiveTools: () => [...activeTools],
    setActiveTools(names) {
      activeTools = [...names];
    },
    getFlag(name) {
      return flags.get(name);
    },
    on(event, handler) {
      const registered = handlers.get(event) ?? [];
      registered.push(handler);
      handlers.set(event, registered);
    },
    sendMessage(message, options) {
      messages.push({ message, options });
    },
    appendEntry(customType, data) {
      entries.push({ type: "custom", customType, data });
    },
  };
  const context = {
    mode,
    cwd: "/work/project",
    sessionManager: {
      getSessionId: () => "session-bob-1",
      getEntries: () => entries,
    },
    isIdle: () => true,
    hasPendingMessages: () => false,
    ui: {
      notify(message, level) {
        notifications.push({ message, level });
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
  const tool = (name, params, id = `${name}-call`) =>
    tools.get(name).execute(id, params, new AbortController().signal, undefined, context);
  return {
    pi, context, emit, tool, tools, messages, notifications, entries,
    activeTools: () => activeTools,
  };
}

const inboxMessage = {
  message_id: "message-1",
  receipt_token: "receipt-1",
  sender_agent_id: "alice",
  recipient_agent_id: "bob",
  sequence_number: 1,
  content: "Please review the result.\nThen reply.",
  content_type: "text/plain",
};

function coordinationClient({ mail = [inboxMessage], expireFirst = [] } = {}) {
  const calls = [];
  const pending = [...mail];
  const expiring = new Set(expireFirst);
  let registrations = 0;
  const claimed = new Set();
  return {
    calls,
    claimed,
    async describe() {
      return {
        methods: ["register", "heartbeat", "receive", "ack", "unregister"]
          .map((name) => ({ name })),
      };
    },
    claim: (url) => claimed.add(url),
    release: (url) => claimed.delete(url),
    async call(_url, method, args, options) {
      calls.push({ method, args, options });
      if (method === "register") {
        registrations += 1;
        return {
          registration_id: `registration-${registrations}`,
          heartbeat_interval_ms: 10000,
        };
      }
      if (expiring.delete(method)) {
        throw new Error(
          "ducknng: SQL method failed: Invalid Input Error: registration_expired: registration is missing or expired",
        );
      }
      if (method === "receive") return { messages: pending.splice(0) };
      if (method === "send") {
        return { message_id: "sent-1", recipient_seen: true, replayed: false };
      }
      if (method === "reserve") {
        return {
          lease_id: "lease-1",
          resource: args.resource,
          fencing_value: 7,
          expires_at_ms: Date.now() + args.ttl_ms,
          active: true,
        };
      }
      if (method === "list_reservations") {
        return {
          reservations: [{
            resource: "file:///work/project/src",
            owner_agent_id: "carol",
            owned_by_caller: false,
          }],
        };
      }
      return {};
    },
  };
}

test("background delivery steers an envelope, records it, and acknowledges", async () => {
  const harness = adapterHarness();
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);

  await harness.emit("session_start");
  await waitFor(() => client.calls.some(({ method }) => method === "ack"));
  await harness.emit("session_shutdown");

  assert.equal(harness.messages.length, 1);
  const { message, options } = harness.messages[0];
  assert.equal(message.customType, "piducknng_coordination");
  assert.equal(message.details.message_id, "message-1");
  assert.match(message.content, /<coordination_message from="alice" to="bob"/);
  assert.match(message.content, /Please review the result\.\nThen reply\./);
  assert.match(message.content, /not a message from the user/);
  assert.deepEqual(options, { triggerTurn: true, deliverAs: "steer" });
  assert.ok(harness.entries.some(
    ({ customType, data }) =>
      customType === "piducknng.coordination.delivery" &&
      data.message_id === "message-1" && data.path === "steer",
  ));
  const receive = client.calls.find(({ method }) => method === "receive");
  assert.equal(receive.args.wait_ms, undefined, "receive never asks the endpoint to wait");
  assert.ok(client.calls.some(({ method }) => method === "unregister"));
  assert.equal(client.claimed.size, 0, "shutdown releases the owned URL");
  assert.deepEqual(harness.notifications, []);
});

test("startup is serialized with shutdown", async () => {
  const harness = adapterHarness();
  let releaseRegistration;
  let registrationStarted = false;
  const calls = [];
  const client = {
    async describe() {
      return {
        methods: ["register", "heartbeat", "receive", "ack", "unregister"]
          .map((name) => ({ name })),
      };
    },
    async call(_url, method, args) {
      calls.push({ method, args });
      if (method === "register") {
        registrationStarted = true;
        await new Promise((resolveRegistration) => {
          releaseRegistration = resolveRegistration;
        });
        return {
          registration_id: "registration-serialized",
          heartbeat_interval_ms: 10000,
        };
      }
      if (method === "receive") return { messages: [] };
      return {};
    },
  };
  registerCoordinationAdapter(harness.pi, client);

  const starting = harness.emit("session_start");
  await waitFor(() => registrationStarted);
  const stopping = harness.emit("session_shutdown");
  releaseRegistration();
  await Promise.all([starting, stopping]);

  assert.equal(calls.filter(({ method }) => method === "unregister").length, 1);
  assert.equal(harness.messages.length, 0);
});

test("a recorded message is acknowledged without reinjection", async () => {
  const entries = [{
    type: "custom",
    customType: "piducknng.coordination.delivery",
    data: { message_id: "message-1" },
  }];
  const harness = adapterHarness({ entries });
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);

  await harness.emit("session_start");
  await waitFor(() => client.calls.some(({ method }) => method === "ack"));
  await harness.emit("session_shutdown");

  assert.equal(harness.messages.length, 0);
});

test("one-shot modes pull mail through the inbox tool", async () => {
  const harness = adapterHarness({ mode: "print" });
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);
  await harness.emit("session_start");
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.ok(!client.calls.some(({ method }) => method === "receive"));

  const result = await harness.tool("coordination_inbox", { wait_ms: 1000 });
  await harness.emit("session_shutdown");

  assert.equal(harness.messages.length, 0, "print mode does not steer");
  assert.match(result.content[0].text, /<coordination_message from="alice"/);
  assert.equal(result.details.messages[0].message_id, "message-1");
  assert.equal(result.details.messages[0].receipt_token, undefined);
  const ack = client.calls.find(({ method }) => method === "ack");
  assert.equal(ack.args.delivery_ref, "tool:coordination_inbox-call:message-1");
});

test("an expired registration is renewed before the call is retried", async () => {
  const harness = adapterHarness({ mode: "print" });
  const client = coordinationClient({ expireFirst: ["send"] });
  registerCoordinationAdapter(harness.pi, client);
  await harness.emit("session_start");

  const result = await harness.tool(
    "coordination_send",
    { recipient: "alice", content: "done" },
    "call-7",
  );
  await harness.emit("session_shutdown");

  assert.equal(result.details.message_id, "sent-1");
  const sends = client.calls.filter(({ method }) => method === "send");
  assert.equal(sends.length, 2);
  assert.equal(sends[0].args.registration_id, "registration-1");
  assert.equal(sends[1].args.registration_id, "registration-2");
  assert.equal(sends[1].args.idempotency_key, "session-bob-1:call-7");
  assert.ok(!JSON.stringify(result).includes("registration-"));
});

test("reservations held elsewhere block edit and write tool calls", async () => {
  const harness = adapterHarness({ mode: "print" });
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);
  await harness.emit("session_start");

  const blocked = await harness.emit("tool_call", {
    type: "tool_call",
    toolCallId: "edit-1",
    toolName: "edit",
    input: { path: "src/model.R" },
  });
  const ignored = await harness.emit("tool_call", {
    type: "tool_call",
    toolCallId: "read-1",
    toolName: "read",
    input: { path: "src/model.R" },
  });
  await harness.emit("session_shutdown");

  assert.equal(blocked.block, true);
  assert.match(blocked.reason, /held by carol/);
  assert.equal(ignored, undefined);
  const query = client.calls.find(({ method }) => method === "list_reservations");
  assert.equal(query.args.resource, "file:///work/project/src/model.R");
});

test("held leases are released at shutdown", async () => {
  const harness = adapterHarness({ mode: "print" });
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);
  await harness.emit("session_start");

  const reserved = await harness.tool("coordination_reserve", { resource: "src" });
  await harness.emit("session_shutdown");

  assert.equal(reserved.details.resource, "file:///work/project/src");
  const release = client.calls.find(({ method }) => method === "release");
  assert.equal(release.args.lease_id, "lease-1");
});

test("coordination tools are deactivated when coordination is not configured", async () => {
  const harness = adapterHarness({ configured: false });
  registerCoordinationAdapter(harness.pi, coordinationClient());
  await harness.emit("session_start");
  for (const name of COORDINATION_TOOLS) {
    assert.ok(!harness.activeTools().includes(name));
  }
});

test("envelopes escape attribute text and resources resolve paths", () => {
  const envelope = coordinationEnvelope({ ...inboxMessage, sender_agent_id: 'a"<b>' });
  assert.match(envelope, /from="a&quot;&lt;b&gt;"/);
  assert.equal(coordinationResource("resource:build/lock", "/x"), "resource:build/lock");
  assert.equal(coordinationResource("@src/a b.R", "/x"), "file:///x/src/a%20b.R");
});

test("error codes are read through the ducknng SQL error wrapper", () => {
  assert.equal(coordinationErrorCode(new Error(
    "ducknng: SQL method failed: Invalid Input Error: mailbox_full: recipient mailbox holds 1 unacknowledged messages",
  )), "mailbox_full");
  assert.equal(coordinationErrorCode(new Error("unauthorized: no grant")), "unauthorized");
  assert.equal(coordinationErrorCode(new Error("Connection refused")), undefined);
});

test("the send tool fans out and envelopes name the group and the reply", async () => {
  const harness = adapterHarness({ mode: "print" });
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);
  await harness.emit("session_start");
  await harness.tool("coordination_send", { recipients: ["w1", "w2"], content: "split" }, "fan");
  await harness.tool("coordination_send", { broadcast: true, content: "all" }, "all");
  await harness.tool("coordination_send", { recipient: "lead", content: "done", in_reply_to: "b-1" }, "reply");
  await harness.emit("session_shutdown");

  const sends = client.calls.filter(({ method }) => method === "send").map(({ args }) => args);
  assert.deepEqual(sends[0].recipient_agent_ids, ["w1", "w2"]);
  assert.equal(sends[1].broadcast, true);
  assert.equal(sends[2].recipient_agent_id, "lead");
  assert.equal(sends[2].in_reply_to, "b-1");
  const envelope = coordinationEnvelope({
    ...inboxMessage, broadcast_id: "b-1", recipient_count: 3, in_reply_to: "m-0",
  });
  assert.match(envelope, /broadcast_id="b-1" recipients="3" in_reply_to="m-0"/);
});
