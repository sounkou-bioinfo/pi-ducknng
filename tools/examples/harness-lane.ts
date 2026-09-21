// A host process that owns a pi-agent-core AgentHarness and delivers one
// coordination mailbox into one of its lanes. The model is a faux provider,
// so the example runs without credentials; admission and recovery are real.
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { AgentHarness, JsonlSessionRepo } from "@earendil-works/pi-agent-core";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/pi-agent-core/harness/context";
import { NodeExecutionEnv } from "@earendil-works/pi-agent-core/node";
import { createModels, fauxProvider } from "@earendil-works/pi-ai";
import { startCoordinationEndpoint } from "../../extensions/pi-ducknng/coordination-endpoint.ts";
import { attachCoordinationLane } from "../../extensions/pi-ducknng/harness.ts";
import { ducknngCoordinationClient } from "../../extensions/pi-ducknng/index.ts";

const work = await mkdtemp(resolve(tmpdir(), "harness-lane-"));
const endpoint = await startCoordinationEndpoint({ database: resolve(work, "coordination.duckdb") });
const url = endpoint.url;

async function openLane(metadata?: never) {
  const repo = new JsonlSessionRepo({
    fileSystem: new NodeExecutionEnv({ cwd: work }),
    sessionsRoot: resolve(work, "sessions"),
  });
  const session = metadata ? await repo.open(metadata, context) : await repo.create({ cwd: work }, context);
  const faux = fauxProvider();
  const models = createModels();
  models.setProvider(faux);
  const { harness } = await AgentHarness.create({ session, models, model: faux.getModel() }, context);
  const lane = await harness.lane("main", context);
  return { lane, metadata: session.metadata as never, close: async () => {
    await harness.close(context);
    await repo.close(context);
  } };
}

async function coordinationEntries(lane: Awaited<ReturnType<typeof openLane>>["lane"]) {
  const handle = await lane.watch(context);
  handle.unsubscribe();
  return [...handle.snapshot.queues, ...handle.snapshot.transcript]
    .map((item) => (item as { message?: { customType?: string; details?: { message_id?: string } } }).message)
    .filter((message) => message?.customType === "piducknng_coordination")
    .map((message) => message!.details!.message_id!);
}

const waitFor = async (predicate: () => Promise<boolean> | boolean) => {
  for (let attempt = 0; attempt < 200 && !(await predicate()); attempt += 1) {
    await new Promise((done) => setTimeout(done, 25));
  }
};

// One acknowledgement is dropped to stand in for a crash after the lane commit.
let dropNextAck = false;
const acks: string[] = [];
const client = {
  describe: ducknngCoordinationClient.describe,
  subscribe: ducknngCoordinationClient.subscribe,
  async call(target: string, method: string, args: Record<string, unknown>, options?: { timeoutMs?: number }) {
    if (method === "ack") {
      acks.push(String(args.delivery_ref));
      if (dropNextAck) {
        dropNextAck = false;
        throw new Error("simulated crash before ack");
      }
    }
    return ducknngCoordinationClient.call(target, method, args, options);
  },
};

let opened = await openLane();
const coordination = attachCoordinationLane({
  client, url, projectId: "guide", agentId: "worker", instanceId: "host-1",
  lane: opened.lane, context, visibilityTimeoutMs: 1000,
});
await coordination.ready;

await ducknngCoordinationClient.describe(url);
const lead = await ducknngCoordinationClient.call(url, "register", {
  project_id: "guide", agent_id: "lead", instance_id: "lead-1", operation_key: "start",
}) as { registration_id: string };
const send = (key: string, content: string) => ducknngCoordinationClient.call(url, "send", {
  registration_id: lead.registration_id, recipient_agent_id: "worker", idempotency_key: key, content,
}) as Promise<{ message_id: string }>;

const first = await send("first", "Summarize the run.");
await waitFor(() => acks.length === 1);
console.log(`queued in the lane before its acknowledgement: ${(await coordinationEntries(opened.lane)).length} entry`);

dropNextAck = true;
const second = await send("second", "Then archive it.");
await waitFor(() => acks.length === 3);
const entries = await coordinationEntries(opened.lane);
console.log(`after a lost ack and redelivery: ${entries.filter((id) => id === second.message_id).length} lane entry for the message`);
console.log(`both acknowledgements name the same lane entry: ${acks[1] === acks[2]}`);

await coordination.stop();
const metadata = opened.metadata;
await opened.close();
opened = await openLane(metadata);
const reopened = await coordinationEntries(opened.lane);
console.log(`after reopening the session: ${reopened.length} entries, in order: ${reopened[0] === first.message_id && reopened[1] === second.message_id}`);

await opened.close();
await endpoint.close();
await rm(work, { recursive: true, force: true });
process.exit(0);
