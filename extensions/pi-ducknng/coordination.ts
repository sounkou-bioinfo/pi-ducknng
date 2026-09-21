import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";

const DELIVERY_ENTRY_TYPE = "piducknng.coordination.delivery";
const MESSAGE_TYPE = "piducknng_coordination";
const DEFAULT_TTL_MS = 30_000;
const DEFAULT_POLL_MS = 2_000;

type CoordinationClient = {
  describe(url: string): Promise<{ methods: Array<{ name: string }> }>;
  call(
    url: string,
    method: string,
    args: Record<string, unknown>,
  ): Promise<unknown>;
};

type CoordinationConfig = {
  url: string;
  projectId: string;
  agentId: string;
};

type Registration = {
  registration_id: string;
  heartbeat_interval_ms: number;
};

type InboxMessage = {
  message_id: string;
  receipt_token: string;
  sender_agent_id: string;
  recipient_agent_id: string;
  sequence_number: number;
  content: string;
  content_type: string;
};

type CoordinationRuntime = {
  url: string;
  registrationId: string;
  controller: AbortController;
  task: Promise<string>;
};

function objectValue(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} is not an object`);
  }
  return value as Record<string, unknown>;
}

function stringValue(
  value: Record<string, unknown>,
  name: string,
): string {
  const member = value[name];
  if (typeof member !== "string" || !member) {
    throw new Error(`coordination reply has invalid ${name}`);
  }
  return member;
}

function numberValue(
  value: Record<string, unknown>,
  name: string,
): number {
  const member = value[name];
  if (typeof member !== "number" || !Number.isFinite(member)) {
    throw new Error(`coordination reply has invalid ${name}`);
  }
  return member;
}

function parseRegistration(value: unknown): Registration {
  const record = objectValue(value, "coordination registration");
  return {
    registration_id: stringValue(record, "registration_id"),
    heartbeat_interval_ms: numberValue(record, "heartbeat_interval_ms"),
  };
}

function parseMessages(value: unknown): InboxMessage[] {
  const record = objectValue(value, "coordination receive reply");
  if (!Array.isArray(record.messages)) {
    throw new Error("coordination receive reply has invalid messages");
  }
  return record.messages.map((message) => {
    const item = objectValue(message, "coordination message");
    return {
      message_id: stringValue(item, "message_id"),
      receipt_token: stringValue(item, "receipt_token"),
      sender_agent_id: stringValue(item, "sender_agent_id"),
      recipient_agent_id: stringValue(item, "recipient_agent_id"),
      sequence_number: numberValue(item, "sequence_number"),
      content: stringValue(item, "content"),
      content_type: stringValue(item, "content_type"),
    };
  });
}

function configuredValue(
  pi: ExtensionAPI,
  flag: string,
  environment: string,
): string | undefined {
  const value = pi.getFlag(flag);
  if (typeof value === "string" && value) return value;
  const configured = process.env[environment]?.trim();
  return configured || undefined;
}

function coordinationConfig(pi: ExtensionAPI): CoordinationConfig | undefined {
  const url = configuredValue(
    pi,
    "ducknng-coordination-url",
    "PI_DUCKNNG_COORDINATION_URL",
  );
  const projectId = configuredValue(
    pi,
    "ducknng-coordination-project",
    "PI_DUCKNNG_COORDINATION_PROJECT",
  );
  const agentId = configuredValue(
    pi,
    "ducknng-agent-id",
    "PI_DUCKNNG_AGENT_ID",
  );
  if (!url && !projectId && !agentId) return undefined;
  if (!url || !projectId || !agentId) {
    throw new Error(
      "coordination requires URL, project, and agent ID configuration",
    );
  }
  return { url, projectId, agentId };
}

function deliveredMessageIds(ctx: ExtensionContext): Set<string> {
  const delivered = new Set<string>();
  for (const entry of ctx.sessionManager.getEntries()) {
    if (entry.type !== "custom" || entry.customType !== DELIVERY_ENTRY_TYPE) {
      continue;
    }
    if (typeof entry.data !== "object" || entry.data === null) continue;
    const messageId = (entry.data as Record<string, unknown>).message_id;
    if (typeof messageId === "string") delivered.add(messageId);
  }
  return delivered;
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason ?? new Error("coordination adapter stopped"));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason ?? new Error("coordination adapter stopped"));
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function notifyError(ctx: ExtensionContext, error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  ctx.ui.notify(`pi-ducknng coordination: ${message}`, "error");
}

async function registerInstance(
  client: CoordinationClient,
  config: CoordinationConfig,
  ctx: ExtensionContext,
): Promise<Registration> {
  const sessionId = ctx.sessionManager.getSessionId();
  return parseRegistration(await client.call(config.url, "register", {
    project_id: config.projectId,
    agent_id: config.agentId,
    instance_id: sessionId,
    operation_key: `pi-session:${sessionId}`,
    display_name: config.agentId,
    adapter_kind: "pi_extension",
    delivery_capability: "api_acceptance",
    ttl_ms: DEFAULT_TTL_MS,
  }));
}

async function deliverMessage(
  pi: ExtensionAPI,
  client: CoordinationClient,
  ctx: ExtensionContext,
  url: string,
  registrationId: string,
  message: InboxMessage,
  delivered: Set<string>,
  signal: AbortSignal,
): Promise<void> {
  signal.throwIfAborted();
  if (!delivered.has(message.message_id)) {
    pi.sendMessage({
      customType: MESSAGE_TYPE,
      content: message.content,
      display: true,
      details: {
        message_id: message.message_id,
        sender_agent_id: message.sender_agent_id,
        recipient_agent_id: message.recipient_agent_id,
        sequence_number: message.sequence_number,
        content_type: message.content_type,
      },
    }, { triggerTurn: true, deliverAs: "steer" });
    pi.appendEntry(DELIVERY_ENTRY_TYPE, {
      message_id: message.message_id,
      sender_agent_id: message.sender_agent_id,
      sequence_number: message.sequence_number,
    });
    delivered.add(message.message_id);
  }
  await client.call(url, "ack", {
    registration_id: registrationId,
    receipt_token: message.receipt_token,
    delivery_ref: `${ctx.sessionManager.getSessionId()}:${message.message_id}`,
  });
}

async function runAdapter(
  pi: ExtensionAPI,
  client: CoordinationClient,
  config: CoordinationConfig,
  ctx: ExtensionContext,
  initialRegistration: Registration,
  signal: AbortSignal,
): Promise<string> {
  let registration = initialRegistration;
  let nextHeartbeat = Date.now() + registration.heartbeat_interval_ms;
  let failures = 0;
  const delivered = deliveredMessageIds(ctx);

  while (!signal.aborted) {
    try {
      if (Date.now() >= nextHeartbeat) {
        await client.call(config.url, "heartbeat", {
          registration_id: registration.registration_id,
          status: {
            idle: ctx.isIdle(),
            pending_messages: ctx.hasPendingMessages(),
          },
        });
        nextHeartbeat = Date.now() + registration.heartbeat_interval_ms;
      }
      const messages = parseMessages(await client.call(config.url, "receive", {
        registration_id: registration.registration_id,
        limit: 8,
        visibility_timeout_ms: 30_000,
      }));
      for (const message of messages) {
        signal.throwIfAborted();
        await deliverMessage(
          pi,
          client,
          ctx,
          config.url,
          registration.registration_id,
          message,
          delivered,
          signal,
        );
      }
      failures = 0;
    } catch (error) {
      if (signal.aborted) break;
      failures += 1;
      if (failures === 1) notifyError(ctx, error);
      if (failures >= 3) {
        try {
          registration = await registerInstance(client, config, ctx);
          nextHeartbeat = Date.now() + registration.heartbeat_interval_ms;
          failures = 0;
        } catch (registrationError) {
          if (failures === 3) notifyError(ctx, registrationError);
        }
      }
    }
    try {
      await sleep(
        Math.min(DEFAULT_POLL_MS * Math.max(failures, 1), 10_000),
        signal,
      );
    } catch {
      break;
    }
  }
  return registration.registration_id;
}

async function stopRuntime(
  client: CoordinationClient,
  runtime: CoordinationRuntime,
): Promise<void> {
  runtime.controller.abort();
  const registrationId = await runtime.task.catch(
    () => runtime.registrationId,
  );
  try {
    await client.call(runtime.url, "unregister", {
      registration_id: registrationId,
    });
  } catch {
    // Presence expires by lease when the endpoint is unavailable.
  }
}

export function registerCoordinationAdapter(
  pi: ExtensionAPI,
  client: CoordinationClient,
): void {
  pi.registerFlag("ducknng-coordination-url", {
    type: "string",
    description: "URL of an independently managed pi-ducknng coordination endpoint",
  });
  pi.registerFlag("ducknng-coordination-project", {
    type: "string",
    description: "Project scope used by the coordination endpoint",
  });
  pi.registerFlag("ducknng-agent-id", {
    type: "string",
    description: "Stable coordination mailbox identity for this Pi session",
  });

  let runtime: CoordinationRuntime | undefined;
  let lifecycle = Promise.resolve();
  const serializeLifecycle = (operation: () => Promise<void>) => {
    const result = lifecycle.then(operation, operation);
    lifecycle = result.catch(() => undefined);
    return result;
  };

  pi.on("session_start", async (_event, ctx) =>
    await serializeLifecycle(async () => {
      if (runtime) {
        const active = runtime;
        runtime = undefined;
        await stopRuntime(client, active);
      }
      try {
        const config = coordinationConfig(pi);
        if (!config) return;
        const manifest = await client.describe(config.url);
        const declared = new Set(manifest.methods.map((method) => method.name));
        for (const method of ["register", "heartbeat", "receive", "ack", "unregister"]) {
          if (!declared.has(method)) {
            throw new Error(`coordination endpoint does not declare ${method}`);
          }
        }
        const registration = await registerInstance(client, config, ctx);
        const controller = new AbortController();
        const task = runAdapter(
          pi,
          client,
          config,
          ctx,
          registration,
          controller.signal,
        );
        runtime = {
          url: config.url,
          registrationId: registration.registration_id,
          controller,
          task,
        };
      } catch (error) {
        notifyError(ctx, error);
      }
    })
  );

  pi.on("session_shutdown", async () =>
    await serializeLifecycle(async () => {
      const active = runtime;
      runtime = undefined;
      if (active) await stopRuntime(client, active);
    })
  );
}
