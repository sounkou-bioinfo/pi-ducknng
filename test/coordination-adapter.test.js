import assert from "node:assert/strict";
import test from "node:test";

import { registerCoordinationAdapter } from "../extensions/pi-ducknng/coordination.ts";

function waitFor(predicate, timeout = 1000) {
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

function adapterHarness(entries = []) {
  const flags = new Map([
    ["ducknng-coordination-url", "tcp://127.0.0.1:9000"],
    ["ducknng-coordination-project", "project-one"],
    ["ducknng-agent-id", "bob"],
  ]);
  const handlers = new Map();
  const messages = [];
  const notifications = [];
  const pi = {
    registerFlag() {},
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
  const emit = async (event) => {
    for (const handler of handlers.get(event) ?? []) {
      await handler({ type: event }, context);
    }
  };
  return { pi, context, emit, messages, notifications, entries };
}

function coordinationClient() {
  const calls = [];
  let delivered = false;
  return {
    calls,
    async describe() {
      return {
        methods: ["register", "heartbeat", "receive", "ack", "unregister"]
          .map((name) => ({ name })),
      };
    },
    async call(_url, method, args) {
      calls.push({ method, args });
      if (method === "register") {
        return {
          registration_id: "registration-1",
          heartbeat_interval_ms: 10000,
        };
      }
      if (method === "receive") {
        if (delivered) return { messages: [] };
        delivered = true;
        return {
          messages: [{
            message_id: "message-1",
            receipt_token: "receipt-1",
            sender_agent_id: "alice",
            recipient_agent_id: "bob",
            sequence_number: 1,
            content: "Please review the result.",
            content_type: "text/plain",
          }],
        };
      }
      return {};
    },
  };
}

test("coordination adapter steers, records, and acknowledges durable mail", async () => {
  const harness = adapterHarness();
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);

  await harness.emit("session_start");
  await waitFor(() => client.calls.some(({ method }) => method === "ack"));
  await harness.emit("session_shutdown");

  assert.equal(harness.messages.length, 1);
  assert.equal(harness.messages[0].message.customType, "piducknng_coordination");
  assert.equal(harness.messages[0].message.details.message_id, "message-1");
  assert.deepEqual(harness.messages[0].options, {
    triggerTurn: true,
    deliverAs: "steer",
  });
  assert.ok(harness.entries.some(
    ({ customType, data }) =>
      customType === "piducknng.coordination.delivery" &&
      data.message_id === "message-1",
  ));
  assert.ok(client.calls.some(
    ({ method, args }) => method === "ack" && args.receipt_token === "receipt-1",
  ));
  assert.ok(client.calls.some(({ method }) => method === "unregister"));
  assert.deepEqual(harness.notifications, []);
});

test("coordination adapter serializes startup with shutdown", async () => {
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

  assert.equal(
    calls.filter(({ method }) => method === "unregister").length,
    1,
  );
  assert.equal(harness.messages.length, 0);
});

test("coordination adapter acknowledges a recorded message without reinjection", async () => {
  const entries = [{
    type: "custom",
    customType: "piducknng.coordination.delivery",
    data: { message_id: "message-1" },
  }];
  const harness = adapterHarness(entries);
  const client = coordinationClient();
  registerCoordinationAdapter(harness.pi, client);

  await harness.emit("session_start");
  await waitFor(() => client.calls.some(({ method }) => method === "ack"));
  await harness.emit("session_shutdown");

  assert.equal(harness.messages.length, 0);
  assert.ok(client.calls.some(({ method }) => method === "ack"));
});
