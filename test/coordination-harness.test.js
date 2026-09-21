import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";

import { AgentHarness, JsonlSessionRepo } from "@earendil-works/pi-agent-core";
import { BACKGROUND_CONTEXT } from "@earendil-works/pi-agent-core/harness/context";
import { NodeExecutionEnv } from "@earendil-works/pi-agent-core/node";
import { createModels, fauxProvider } from "@earendil-works/pi-ai";

import { startCoordinationEndpoint } from "../extensions/pi-ducknng/coordination-endpoint.ts";
import { attachCoordinationLane } from "../extensions/pi-ducknng/harness.ts";
import { ducknngCoordinationClient } from "../extensions/pi-ducknng/index.ts";

const context = BACKGROUND_CONTEXT;

async function openLane(work, metadata) {
  const env = new NodeExecutionEnv({ cwd: work });
  const repo = new JsonlSessionRepo({
    fileSystem: env,
    sessionsRoot: resolve(work, "sessions"),
  });
  const session = metadata
    ? await repo.open(metadata, context)
    : await repo.create({ cwd: work }, context);
  const faux = fauxProvider();
  const models = createModels();
  models.setProvider(faux);
  const { harness } = await AgentHarness.create(
    { session, models, model: faux.getModel() },
    context,
  );
  const lane = await harness.lane("main", context);
  return {
    lane,
    metadata: session.metadata,
    async close() {
      await harness.close(context);
      await repo.close(context);
    },
  };
}

async function coordinationItems(lane) {
  const handle = await lane.watch(context);
  handle.unsubscribe();
  return [...handle.snapshot.queues, ...handle.snapshot.transcript]
    .filter((item) => item.message?.customType === "piducknng_coordination")
    .map((item) => ({
      entryId: item.entryId ?? item.id,
      kind: item.kind,
      messageId: item.message.details.message_id,
      content: item.message.content,
    }));
}

function waitFor(predicate, timeout, label) {
  const deadline = Date.now() + timeout;
  return new Promise((resolveWait, reject) => {
    const check = async () => {
      if (await predicate()) return resolveWait();
      if (Date.now() >= deadline) return reject(new Error(`timed out: ${label}`));
      setTimeout(check, 25);
    };
    check();
  });
}

test("a harness lane commits mail before acknowledging and reconciles redelivery", async () => {
  const work = await mkdtemp(resolve(tmpdir(), "pi-ducknng-harness-test-"));
  const endpoint = await startCoordinationEndpoint({
    database: resolve(work, "coordination.duckdb"),
  });
  const { url } = endpoint;
  const acks = [];
  let dropNextAck = false;
  const client = {
    describe: ducknngCoordinationClient.describe,
    async call(target, method, args, options) {
      if (method === "ack") {
        acks.push({ ...args, dropped: dropNextAck });
        if (dropNextAck) {
          dropNextAck = false;
          throw new Error("simulated crash between lane commit and ack");
        }
      }
      return await ducknngCoordinationClient.call(target, method, args, options);
    },
  };
  let opened = await openLane(work);
  let attached;
  try {
    await client.describe(url);
    const alice = await client.call(url, "register", {
      project_id: "project-one",
      agent_id: "alice",
      instance_id: "alice-1",
      operation_key: "register-alice",
    });
    const send = (key, content) => client.call(url, "send", {
      registration_id: alice.registration_id,
      recipient_agent_id: "worker",
      idempotency_key: key,
      content,
    });

    const errors = [];
    attached = attachCoordinationLane({
      client,
      url,
      projectId: "project-one",
      agentId: "worker",
      instanceId: "host-1",
      lane: opened.lane,
      context,
      visibilityTimeoutMs: 1000,
      pollMs: 100,
      onError: (error) => errors.push(error.message),
    });
    await attached.ready;

    const first = await send("first", "Summarize the run.\nKeep it short.");
    await waitFor(
      async () => (await coordinationItems(opened.lane)).length === 1,
      5000,
      "first lane admission",
    );
    const [admitted] = await coordinationItems(opened.lane);
    assert.equal(admitted.messageId, first.message_id);
    assert.equal(admitted.kind, "nextRun", "an idle lane queues for its next run");
    assert.match(admitted.content, /from="alice"/);
    assert.match(admitted.content, /Summarize the run\.\nKeep it short\./);
    await waitFor(() => acks.length === 1, 5000, "first ack");
    assert.equal(acks[0].delivery_ref, `lane:main:${admitted.entryId}`);

    dropNextAck = true;
    const second = await send("second", "Then archive it.");
    await waitFor(
      () => acks.filter(({ dropped }) => !dropped).length === 2,
      10000,
      "second ack after redelivery",
    );
    assert.ok(errors.some((message) => message.includes("simulated crash")));
    const items = await coordinationItems(opened.lane);
    const copies = items.filter(({ messageId }) => messageId === second.message_id);
    assert.equal(copies.length, 1, "redelivery does not queue a second copy");
    const secondAcks = acks.slice(1);
    assert.equal(secondAcks.length, 2);
    assert.equal(secondAcks[0].delivery_ref, secondAcks[1].delivery_ref);
    assert.notEqual(secondAcks[0].receipt_token, secondAcks[1].receipt_token);

    await attached.stop();
    attached = undefined;
    const metadata = opened.metadata;
    await opened.close();
    opened = await openLane(work, metadata);
    const reopened = await coordinationItems(opened.lane);
    assert.deepEqual(
      reopened.map(({ messageId }) => messageId),
      [first.message_id, second.message_id],
      "committed lane entries survive a harness restart",
    );

    const probe = await client.call(url, "register", {
      project_id: "project-one",
      agent_id: "worker",
      instance_id: "probe",
      operation_key: "probe",
    });
    const remaining = await client.call(url, "receive", {
      registration_id: probe.registration_id,
    });
    assert.equal(remaining.messages.length, 0, "both messages were acknowledged");
  } finally {
    await attached?.stop();
    await opened.close();
    await endpoint.close();
    await rm(work, { recursive: true, force: true });
  }
});
