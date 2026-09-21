import {
  type CoordinationClient,
  type InboxMessage,
  COORDINATION_MESSAGE_TYPE,
  coordinationEnvelope,
  coordinationErrorCode,
  parseMessages,
  parseRegistration,
} from "./coordination.ts";

type QueueResult =
  | { ok: true; value: { entryId: string } }
  | { ok: false; error: { message?: string } };

type LaneItem = { id?: string; entryId?: string; message?: unknown };

/** The public `AgentLane` operations this adapter uses. */
export type CoordinationLane = {
  readonly name: string;
  steer(message: unknown, images: undefined, context: never): Promise<QueueResult>;
  followUp(message: unknown, images: undefined, context: never): Promise<QueueResult>;
  nextRun(message: unknown, images: undefined, context: never): Promise<QueueResult>;
  watch(context: never): Promise<{
    snapshot: {
      operation: unknown;
      queues: LaneItem[];
      transcript: LaneItem[];
    };
    unsubscribe(): void;
  }>;
};

export type LaneQueue = "auto" | "steer" | "followUp" | "nextRun";

export type HarnessCoordinationOptions = {
  client: CoordinationClient;
  url: string;
  projectId: string;
  agentId: string;
  instanceId: string;
  lane: CoordinationLane;
  /** The harness `Context` passed to every lane call. */
  context: unknown;
  /**
   * The acknowledgement strength recorded by the endpoint. A lane commit is
   * durable only when the harness session uses a durable `SessionRepo`.
   */
  deliveryCapability?: string;
  /** `auto` steers a running lane and queues for the next run otherwise. */
  queue?: LaneQueue;
  visibilityTimeoutMs?: number;
  /** Delay between empty receives. */
  pollMs?: number;
  onError?: (error: unknown) => void;
};

export type HarnessCoordination = {
  /** Resolves after the endpoint accepted the registration. */
  ready: Promise<void>;
  stop(): Promise<void>;
};

function coordinationMessageId(item: LaneItem): string | undefined {
  const message = item.message as
    | { role?: string; customType?: string; details?: { message_id?: unknown } }
    | undefined;
  if (message?.role !== "custom" || message.customType !== COORDINATION_MESSAGE_TYPE) {
    return undefined;
  }
  const id = message.details?.message_id;
  return typeof id === "string" ? id : undefined;
}

/** Finds a lane entry already admitted for one coordination message. */
export async function admittedEntry(
  lane: CoordinationLane,
  context: unknown,
  messageId: string,
): Promise<string | undefined> {
  const handle = await lane.watch(context as never);
  try {
    const { queues, transcript } = handle.snapshot;
    for (const item of [...queues, ...transcript]) {
      if (coordinationMessageId(item) === messageId) return item.entryId ?? item.id;
    }
    return undefined;
  } finally {
    handle.unsubscribe();
  }
}

function laneMessage(message: InboxMessage) {
  return {
    role: "custom",
    customType: COORDINATION_MESSAGE_TYPE,
    content: coordinationEnvelope(message),
    display: true,
    details: {
      message_id: message.message_id,
      sender_agent_id: message.sender_agent_id,
      recipient_agent_id: message.recipient_agent_id,
      sequence_number: message.sequence_number,
      content_type: message.content_type,
    },
    timestamp: Date.now(),
  };
}

function abortable(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

/**
 * Delivers one agent mailbox into a host-owned `AgentHarness` lane. Each
 * message is committed to the lane before it is acknowledged, and a
 * redelivered message is matched to its existing lane entry by message ID.
 * The host keeps ownership of `drive`, operation recovery, and the model
 * runtime; this adapter never starts a run.
 */
export function attachCoordinationLane(
  options: HarnessCoordinationOptions,
): HarnessCoordination {
  const {
    client,
    url,
    lane,
    context,
    queue = "auto",
    visibilityTimeoutMs = 30_000,
    pollMs = 1_000,
  } = options;
  const controller = new AbortController();
  let registrationId = "";
  let heartbeatIntervalMs = 10_000;

  const register = async () => {
    const registration = parseRegistration(await client.call(url, "register", {
      project_id: options.projectId,
      agent_id: options.agentId,
      instance_id: options.instanceId,
      operation_key: `harness-lane:${options.instanceId}:${lane.name}`,
      display_name: options.agentId,
      adapter_kind: "agent_harness",
      delivery_capability: options.deliveryCapability ?? "harness_lane_commit",
    }));
    registrationId = registration.registration_id;
    heartbeatIntervalMs = registration.heartbeat_interval_ms;
  };

  const call = async (
    method: string,
    args: Record<string, unknown>,
    callOptions?: { timeoutMs?: number },
  ) => {
    try {
      return await client.call(
        url, method, { registration_id: registrationId, ...args }, callOptions,
      );
    } catch (error) {
      if (coordinationErrorCode(error) !== "registration_expired") throw error;
      await register();
      return await client.call(
        url, method, { registration_id: registrationId, ...args }, callOptions,
      );
    }
  };

  const admit = async (message: InboxMessage): Promise<string> => {
    const existing = await admittedEntry(lane, context, message.message_id);
    if (existing) return existing;
    let kind: Exclude<LaneQueue, "auto"> = queue === "auto" ? "nextRun" : queue;
    if (queue === "auto") {
      const handle = await lane.watch(context as never);
      kind = handle.snapshot.operation ? "steer" : "nextRun";
      handle.unsubscribe();
    }
    const result = await lane[kind](laneMessage(message), undefined, context as never);
    if (!result.ok) {
      throw new Error(result.error.message ?? `lane ${kind} rejected the message`);
    }
    return result.value.entryId;
  };

  const run = async () => {
    let nextHeartbeat = Date.now() + heartbeatIntervalMs;
    let failures = 0;
    while (!controller.signal.aborted) {
      try {
        if (Date.now() >= nextHeartbeat) {
          await call("heartbeat", { status: { lane: lane.name } });
          nextHeartbeat = Date.now() + heartbeatIntervalMs;
        }
        const messages = parseMessages(await call(
          "receive",
          { limit: 8, visibility_timeout_ms: visibilityTimeoutMs },
        ));
        for (const message of messages) {
          if (controller.signal.aborted) return;
          const entryId = await admit(message);
          await call("ack", {
            receipt_token: message.receipt_token,
            delivery_ref: `lane:${lane.name}:${entryId}`,
          });
        }
        failures = 0;
        if (messages.length === 0) {
          await abortable(Math.max(0, Math.min(pollMs, nextHeartbeat - Date.now())), controller.signal);
        }
      } catch (error) {
        if (controller.signal.aborted) return;
        failures += 1;
        options.onError?.(error);
        await abortable(Math.min(250 * 2 ** failures, 5_000), controller.signal);
      }
    }
  };

  let task: Promise<void> = Promise.resolve();
  const ready = (async () => {
    const manifest = await client.describe(url);
    const declared = new Set(manifest.methods.map((method) => method.name));
    for (const method of ["register", "heartbeat", "receive", "ack", "unregister"]) {
      if (!declared.has(method)) {
        throw new Error(`coordination endpoint does not declare ${method}`);
      }
    }
    await register();
    task = run();
  })();

  return {
    ready,
    async stop() {
      controller.abort();
      await ready.catch(() => undefined);
      await task;
      if (!registrationId) return;
      try {
        await client.call(url, "unregister", { registration_id: registrationId });
      } catch {
        // Presence expires by lease when the endpoint is unavailable.
      }
    },
  };
}
