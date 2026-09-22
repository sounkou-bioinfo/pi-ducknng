import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";

import { startCoordinationEndpoint } from "../extensions/pi-ducknng/coordination-endpoint.ts";
import { coordinationErrorCode } from "../extensions/pi-ducknng/coordination.ts";
import {
  ducknngCoordinationClient as client,
  subscribeCoordinationEvents,
} from "../extensions/pi-ducknng/index.ts";
import { readFile } from "node:fs/promises";
import { pki, withEnv } from "./support/pki.js";

// Drives the SQL methods over the real ducknng path with a fixed server clock
// set through the host connection.
async function store(work, options = {}) {
  const endpoint = await startCoordinationEndpoint({
    database: resolve(work, options.name ?? "coordination.duckdb"),
    ...options,
  });
  if (!options.tls) await client.describe(endpoint.url);
  const call = (method, args) => client.call(endpoint.url, method, args);
  const setClock = (ms) =>
    endpoint.connection.run("UPDATE coordination_meta SET fixed_now_ms = $ms", { ms });
  const register = (agent, instance, operation = `register-${instance}`, project = "project-one") =>
    call("register", {
      project_id: project,
      agent_id: agent,
      instance_id: instance,
      operation_key: operation,
      adapter_kind: "test",
      delivery_capability: "durable_test",
      ttl_ms: 30000,
    });
  return { endpoint, call, setClock, register };
}

async function rejectsWith(promise, code, pattern) {
  await assert.rejects(promise, (error) => {
    assert.equal(coordinationErrorCode(error), code, error.message);
    if (pattern) assert.match(error.message, pattern);
    return true;
  });
}

test("mail is durable, idempotent, leased, and redelivered across a restart", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-store-"));
  let s = await store(work);
  try {
    let clock = 1_000_000;
    await s.setClock(clock);
    const alice = await s.register("alice", "alice-1");
    const replayed = await s.register("alice", "alice-1");
    assert.equal(replayed.registration_id, alice.registration_id);
    assert.equal(replayed.replayed, true);

    const send = (content, key = "message-1") => s.call("send", {
      registration_id: alice.registration_id,
      recipient_agent_id: "bob",
      idempotency_key: key,
      content,
    });
    const sent = await send("coordinate this");
    assert.equal(sent.recipient_seen, false, "bob has not registered yet");
    const replay = await send("coordinate this");
    assert.equal(replay.message_id, sent.message_id);
    assert.equal(replay.replayed, true);
    await rejectsWith(send("different content"), "idempotency_conflict");
    await rejectsWith(s.call("receive", { registration_id: "missing" }), "registration_expired");

    const bob = await s.register("bob", "bob-1");
    const first = (await s.call("receive", {
      registration_id: bob.registration_id, visibility_timeout_ms: 1000,
    })).messages[0];
    assert.equal(first.message_id, sent.message_id);

    await s.endpoint.close();
    clock += 1001;
    s = await store(work);
    await s.setClock(clock);
    const second = (await s.call("receive", {
      registration_id: bob.registration_id, visibility_timeout_ms: 1000,
    })).messages[0];
    assert.equal(second.message_id, sent.message_id, "an expired lease redelivers after restart");
    assert.notEqual(second.receipt_token, first.receipt_token);
    await rejectsWith(s.call("ack", {
      registration_id: bob.registration_id, receipt_token: first.receipt_token,
    }), "receipt_invalid");

    const acked = await s.call("ack", {
      registration_id: bob.registration_id, receipt_token: second.receipt_token, delivery_ref: "entry",
    });
    const again = await s.call("ack", {
      registration_id: bob.registration_id, receipt_token: second.receipt_token, delivery_ref: "entry",
    });
    assert.equal(acked.delivery_capability, "durable_test");
    assert.equal(acked.receiver_instance_id, "bob-1");
    assert.equal(again.replayed, true);
    assert.equal((await s.call("receive", { registration_id: bob.registration_id })).messages.length, 0);

    const agents = await s.call("list_agents", { registration_id: alice.registration_id });
    assert.deepEqual(agents.agents.map(({ agent_id }) => agent_id), ["alice", "bob"]);
    const unregistered = await s.call("unregister", { registration_id: bob.registration_id });
    assert.equal((await s.call("unregister", { registration_id: bob.registration_id })).replayed, true);
    assert.equal(unregistered.unregistered, true);
    const resumed = await s.register("bob", "bob-1");
    assert.notEqual(resumed.registration_id, bob.registration_id);
    await s.call("unregister", { registration_id: resumed.registration_id });
    assert.deepEqual(
      (await s.call("list_agents", { registration_id: alice.registration_id })).agents
        .map(({ agent_id }) => agent_id),
      ["alice"],
    );
  } finally {
    await s.endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});

test("reservations conflict, fence, expire, renew, and hide foreign lease IDs", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-store-"));
  const s = await store(work);
  try {
    let clock = 2_000_000;
    await s.setClock(clock);
    const alice = await s.register("alice", "alice-1");
    const bob = await s.register("bob", "bob-1");
    const reserve = (who, resource, key, extra = {}) => s.call("reserve", {
      registration_id: who.registration_id, resource, operation_key: key, ttl_ms: 5000, ...extra,
    });

    const lease = await reserve(alice, "file:///tmp/project/data", "reserve-data");
    const replay = await reserve(alice, "file:///tmp/project/data", "reserve-data");
    assert.equal(replay.lease_id, lease.lease_id);
    assert.equal(replay.replayed, true);
    await rejectsWith(reserve(bob, "file:///tmp/project", "reserve-parent"),
      "resource_conflict", /reserved by alice/);

    clock += 5001;
    await s.setClock(clock);
    assert.equal((await reserve(alice, "file:///tmp/project/data", "reserve-data")).active, false);
    const parent = await reserve(bob, "file:///tmp/project", "reserve-parent-after-expiry");
    assert.ok(parent.fencing_value > lease.fencing_value);
    const stale = await s.call("release", {
      registration_id: alice.registration_id, lease_id: lease.lease_id,
    });
    assert.equal(stale.released, true);
    const renewed = await s.call("reserve", {
      registration_id: bob.registration_id, resource: "file:///tmp/project",
      lease_id: parent.lease_id, ttl_ms: 10000,
    });
    assert.equal(renewed.renewed, true);
    await rejectsWith(s.call("release", {
      registration_id: alice.registration_id, lease_id: parent.lease_id,
    }), "lease_invalid");

    const encoded = await reserve(alice, "file:///tmp/a%20b/c#d/", "encoded");
    assert.equal(encoded.resource, "file:///tmp/a%20b/c%23d");
    const listed = (await s.call("list_reservations", {
      registration_id: bob.registration_id, resource: "file:///tmp/a%20b",
    })).reservations;
    assert.equal(listed.length, 1);
    assert.equal(listed[0].owner_agent_id, "alice");
    assert.equal(listed[0].owned_by_caller, false);
    assert.equal(listed[0].lease_id, null);
    const own = (await s.call("list_reservations", {
      registration_id: alice.registration_id,
    })).reservations.find(({ owned_by_caller }) => owned_by_caller);
    assert.equal(own.lease_id, encoded.lease_id);
    await rejectsWith(reserve(alice, "resource:../escape", "escape"), "invalid_argument");
    await rejectsWith(reserve(alice, "file:///tmp/%zz", "bad-escape"), "invalid_argument");

    // Unregistering ends the instance's leases, and only once.
    const gone = await s.call("unregister", { registration_id: alice.registration_id });
    assert.equal(gone.released_reservations, 1);
    assert.equal((await s.call("unregister", {
      registration_id: alice.registration_id,
    })).released_reservations, 0);
    const freed = await reserve(bob, "file:///tmp/a%20b/c%23d", "after-unregister");
    assert.equal(freed.active, true);
    assert.equal((await s.call("list_reservations", {
      registration_id: bob.registration_id,
    })).reservations.filter(({ owner_agent_id }) => owner_agent_id === "alice").length, 0);
  } finally {
    await s.endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});

test("text, dead letters, retention, and the mailbox cap follow the contract", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-store-"));
  const s = await store(work, { retentionMs: 86_400_000, maxPendingPerMailbox: 1 });
  try {
    let clock = 3_000_000;
    await s.setClock(clock);
    const writer = await s.register("writer", "writer-1");
    const reader = await s.register("reader", "reader-1");
    const send = (recipient, key, content, extra = {}) => s.call("send", {
      registration_id: writer.registration_id,
      recipient_agent_id: recipient,
      idempotency_key: key,
      content,
      ...extra,
    });

    const multiline = "first line\n\tsecond line\r\nthird";
    await send("reader", "multiline", multiline);
    await rejectsWith(send("reader", "blocked", "second"), "mailbox_full");
    const delivered = (await s.call("receive", { registration_id: reader.registration_id })).messages[0];
    assert.equal(delivered.content, multiline);
    await s.call("ack", { registration_id: reader.registration_id, receipt_token: delivered.receipt_token });
    await rejectsWith(send("reader\nbob", "bad-recipient", "x"), "invalid_argument", /recipient_agent_id/);
    await rejectsWith(send("reader", "bell", "ring\u0007"), "invalid_argument", /content/);
    await rejectsWith(send("reader", "extra", "x", { unexpected: 1 }), "invalid_argument", /unexpected/);

    const unseen = await send("nobody", "dead-letter", "lost", { ttl_ms: 60000 });
    assert.equal(unseen.recipient_seen, false);
    clock += 60001;
    await s.setClock(clock);
    const writerAgain = await s.register("writer", "writer-1", "register-writer-after-expiry");
    const nobody = await s.register("nobody", "nobody-1");
    assert.equal((await s.call("receive", { registration_id: nobody.registration_id })).messages.length, 0);
    const state = (await s.endpoint.connection.runAndReadAll(
      "SELECT state FROM coordination_messages WHERE idempotency_key = 'dead-letter'",
    )).getRows();
    assert.deepEqual(state, [["expired"]]);

    clock += 86_400_001;
    await s.setClock(clock);
    const writerLater = await s.register("writer", "writer-1", "register-writer-after-retention");
    const reused = await s.call("send", {
      registration_id: writerLater.registration_id,
      recipient_agent_id: "reader",
      idempotency_key: "multiline",
      content: multiline,
    });
    assert.equal(reused.replayed, false, "retention released the idempotency key");
    assert.ok(writerAgain.registration_id);
  } finally {
    await s.endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});

test("mutual TLS binds registrations to granted peer identities", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-store-tls-"));
  const material = await pki(work, ["alice", "mallory"]);
  const s = await store(work, {
    listen: "tls+tcp://127.0.0.1:0",
    tls: { certKeyFile: material.server, caFile: material.ca },
  }).catch(async (error) => {
    await rm(work, { recursive: true, force: true });
    throw error;
  });
  // Clients present in-memory PEM; ducknng never needs a certificate file.
  const pem = async (path) => await readFile(path, "utf8");
  const caPem = await pem(material.ca);
  const clientPem = {};
  for (const name of ["alice", "mallory"]) {
    const combined = await pem(material[name]);
    clientPem[name] = {
      cert: combined.slice(0, combined.indexOf("-----END CERTIFICATE-----") + 25),
      key: combined.slice(combined.indexOf("-----BEGIN PRIVATE KEY-----")),
    };
  }
  const as = (name, operation) => withEnv({
    PI_DUCKNNG_TLS_CA_FILE: undefined,
    PI_DUCKNNG_TLS_CERT_KEY_FILE: undefined,
    PI_DUCKNNG_TLS_CA_PEM: caPem,
    PI_DUCKNNG_TLS_CERT_PEM: clientPem[name].cert,
    PI_DUCKNNG_TLS_KEY_PEM: clientPem[name].key,
  }, operation);
  // The endpoint names the verified caller when it refuses an ungranted one.
  const identity = (name) => as(name, async () => {
    const error = await s.register(name, `${name}-probe`).then(() => undefined, (failure) => failure);
    assert.equal(coordinationErrorCode(error), "unauthorized", String(error));
    const peer = /: (\S+) has no grant/.exec(error.message)?.[1];
    assert.ok(peer, `peer identity for ${name}`);
    return peer;
  });
  try {
    await as("alice", () => client.describe(s.endpoint.url));
    const alicePeer = await identity("alice");
    const malloryPeer = await identity("mallory");
    assert.notEqual(alicePeer, malloryPeer);
    await s.endpoint.connection.run(
      "INSERT INTO coordination_grants VALUES ($a, 'project-one', 'alice'), ($m, 'project-one', 'mallory')",
      { a: alicePeer, m: malloryPeer },
    );

    const alice = await as("alice", () => s.register("alice", "alice-1"));
    await as("alice", () => rejectsWith(s.register("mallory", "alice-2"), "unauthorized"));
    await as("alice", () => rejectsWith(
      s.register("alice", "alice-3", "register", "project-two"), "unauthorized"));
    await as("mallory", () => rejectsWith(
      s.call("list_agents", { registration_id: alice.registration_id }), "unauthorized",
      /another peer identity/));
    await as("mallory", () => rejectsWith(s.register("mallory", "alice-1"), "invalid_argument",
      /another agent_id/));
    // A shared grant lets mallory run her own instance of agent alice, but
    // never take over the instance alice's certificate registered.
    await s.endpoint.connection.run(
      "INSERT INTO coordination_grants VALUES ($m, 'project-one', 'alice')", { m: malloryPeer });
    await as("mallory", () => rejectsWith(s.register("alice", "alice-1"), "unauthorized",
      /another peer identity/));
    const shared = await as("mallory", () => s.register("alice", "mallory-as-alice"));
    assert.notEqual(shared.registration_id, alice.registration_id);

    const mallory = await as("mallory", () => s.register("mallory", "mallory-1"));
    await as("alice", () => s.call("send", {
      registration_id: alice.registration_id,
      recipient_agent_id: "mallory",
      idempotency_key: "hello",
      content: "signed by alice's certificate",
    }));
    const inbox = await as("mallory", () => s.call("receive", { registration_id: mallory.registration_id }));
    assert.equal(inbox.messages[0].sender_agent_id, "alice");
    const agents = await as("alice", () => s.call("list_agents", { registration_id: alice.registration_id }));
    assert.ok(agents.agents.every(({ authenticated }) => authenticated === true));
  } finally {
    await s.endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});

test("one send reaches several mailboxes and replies fan back in", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-store-"));
  const s = await store(work, { maxPendingPerMailbox: 3 });
  try {
    await s.setClock(4_000_000);
    const lead = await s.register("lead", "lead-1");
    const workers = [];
    for (const name of ["w1", "w2", "w3"]) workers.push(await s.register(name, `${name}-1`));
    const question = (extra) => s.call("send", {
      registration_id: lead.registration_id,
      idempotency_key: "question",
      content: "Estimate mean mpg for your cylinder group.",
      ...extra,
    });

    const listed = await question({ recipient_agent_ids: ["w2", "w1", "w1"] });
    assert.equal(listed.recipient_count, 2, "duplicates collapse");
    assert.deepEqual(listed.messages.map(({ recipient_agent_id }) => recipient_agent_id), ["w1", "w2"]);
    assert.equal(listed.message_id, null, "no single message ID for a fan-out");
    const replay = await question({ recipient_agent_ids: ["w1", "w2"] });
    assert.equal(replay.replayed, true);
    assert.equal(replay.broadcast_id, listed.broadcast_id);
    await rejectsWith(question({ recipient_agent_ids: ["w1", "w3"] }), "idempotency_conflict");
    await rejectsWith(question({ recipient_agent_id: "w1", broadcast: true }), "invalid_argument",
      /exactly one/);

    const everyone = await s.call("send", {
      registration_id: lead.registration_id,
      idempotency_key: "all-hands",
      broadcast: true,
      content: "Report your group's result to lead.",
    });
    assert.deepEqual(everyone.messages.map(({ recipient_agent_id }) => recipient_agent_id),
      ["w1", "w2", "w3"], "broadcast reaches every other live agent");

    const replies = [];
    for (const worker of workers) {
      const inbox = (await s.call("receive", { registration_id: worker.registration_id })).messages;
      const copy = inbox.find(({ broadcast_id }) => broadcast_id === everyone.broadcast_id);
      assert.equal(copy.recipient_count, 3);
      replies.push(await s.call("send", {
        registration_id: worker.registration_id,
        idempotency_key: "reply",
        recipient_agent_id: "lead",
        in_reply_to: copy.broadcast_id,
        content: `done by ${copy.recipient_agent_id}`,
      }));
    }
    const gathered = (await s.call("receive", { registration_id: lead.registration_id, limit: 8 })).messages;
    assert.deepEqual(gathered.map(({ in_reply_to }) => in_reply_to),
      [everyone.broadcast_id, everyone.broadcast_id, everyone.broadcast_id]);
    assert.deepEqual(gathered.map(({ sender_agent_id }) => sender_agent_id).sort(), ["w1", "w2", "w3"]);

    // Filling w1 to the cap refuses a later fan-out that includes it, whole.
    await s.call("send", {
      registration_id: lead.registration_id,
      idempotency_key: "filler",
      recipient_agent_id: "w1",
      content: "fill",
    });
    await rejectsWith(s.call("send", {
      registration_id: lead.registration_id,
      idempotency_key: "overflow",
      recipient_agent_ids: ["w3", "w1"],
      content: "x",
    }), "mailbox_full", /w1/);
    const w3Pending = (await s.endpoint.connection.runAndReadAll(
      "SELECT count(*) FROM coordination_messages WHERE recipient_agent_id = 'w3' AND idempotency_key = 'overflow'",
    )).getRows()[0][0];
    assert.equal(w3Pending, 0n, "a refused fan-out writes no copy");
  } finally {
    await s.endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});

test("send publishes an opaque wake-up hint to each recipient's topic", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-store-"));
  const s = await store(work);
  let subscription;
  try {
    assert.match(s.endpoint.eventsUrl, /^ipc:\/\/.*coordination\.events\.ipc$/);
    const lead = await s.register("lead", "lead-1");
    const worker = await s.register("w1", "w1-1");
    assert.equal(worker.events.url, s.endpoint.eventsUrl);
    assert.match(worker.events.topic, /^[0-9a-f]{32}$/);
    assert.notEqual(worker.events.topic, lead.events.topic);

    let hints = 0;
    subscription = await subscribeCoordinationEvents(worker.events.url, worker.events.topic, () => {
      hints += 1;
    });
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 200));
    const sentAt = Date.now();
    await s.call("send", {
      registration_id: lead.registration_id, recipient_agent_id: "w1",
      idempotency_key: "wake", content: "wake up",
    });
    while (hints === 0 && Date.now() - sentAt < 2000) {
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
    }
    assert.equal(hints, 1);
    assert.ok(Date.now() - sentAt < 1000, "the hint arrives well before any poll");
    await s.call("send", {
      registration_id: lead.registration_id, recipient_agent_id: "lead",
      idempotency_key: "self", content: "not for w1",
    });
    await new Promise((resolveDelay) => setTimeout(resolveDelay, 300));
    assert.equal(hints, 1, "a subscriber sees only its own mailbox topic");
  } finally {
    await subscription?.close();
    await s.endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});

test("an endpoint without hints advertises none", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-store-"));
  const s = await store(work, { events: false });
  try {
    const lead = await s.register("lead", "lead-1");
    assert.equal(lead.events, null);
    assert.equal(s.endpoint.eventsUrl, undefined);
  } finally {
    await s.endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});
