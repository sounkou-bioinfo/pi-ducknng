import { homedir } from "node:os";
import { isAbsolute, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type {
  ExtensionAPI,
  ExtensionContext,
} from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";

const DELIVERY_ENTRY_TYPE = "piducknng.coordination.delivery";
export const COORDINATION_MESSAGE_TYPE = "piducknng_coordination";
const DEFAULT_TTL_MS = 30_000;
const POLL_MS = 1_000;
// With wake-up hints, polling only repairs hints that were lost.
const FALLBACK_POLL_MS = 5_000;
const RESERVATION_TTL_MS = 120_000;
const REQUIRED_METHODS = ["register", "heartbeat", "receive", "ack", "unregister"];
export const COORDINATION_TOOLS = [
  "coordination_send",
  "coordination_inbox",
  "coordination_agents",
  "coordination_reserve",
  "coordination_release",
];

export type CoordinationClient = {
  describe(url: string): Promise<{ methods: Array<{ name: string }> }>;
  call(
    url: string,
    method: string,
    args: Record<string, unknown>,
    options?: { timeoutMs?: number },
  ): Promise<unknown>;
  claim?(url: string): void;
  release?(url: string): void;
  /** Subscribes to a mailbox's wake-up hints, when the client supports them. */
  subscribe?(
    url: string,
    topic: string,
    onHint: () => void,
  ): Promise<{ close(): Promise<void> }>;
};

/** Resolves a wait early when a wake-up hint arrives. */
export class Wakeup {
  private pending = false;
  private readonly waiters = new Set<() => void>();

  notify(): void {
    this.pending = true;
    for (const wake of [...this.waiters]) wake();
  }

  wait(ms: number, signal?: AbortSignal): Promise<void> {
    if (this.pending) {
      this.pending = false;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", done);
        this.waiters.delete(done);
        this.pending = false;
        resolve();
      };
      const timer = setTimeout(done, ms);
      signal?.addEventListener("abort", done, { once: true });
      this.waiters.add(done);
    });
  }
}

type CoordinationConfig = {
  url: string;
  projectId: string;
  agentId: string;
};

type Registration = {
  registration_id: string;
  heartbeat_interval_ms: number;
  events?: { url: string; topic: string };
};

export type InboxMessage = {
  message_id: string;
  receipt_token: string;
  sender_agent_id: string;
  recipient_agent_id: string;
  sequence_number: number;
  content: string;
  content_type: string;
  broadcast_id?: string;
  recipient_count?: number;
  in_reply_to?: string;
};

type HeldLease = {
  resource: string;
  ttlMs: number;
  expiresAt: number;
  fencingValue: number;
};

type CoordinationRuntime = {
  config: CoordinationConfig;
  ctx: ExtensionContext;
  registration: Registration;
  delivered: Set<string>;
  leases: Map<string, HeldLease>;
  controller: AbortController;
  task: Promise<void>;
  wakeup: Wakeup;
  subscription?: { close(): Promise<void> };
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

export function parseRegistration(value: unknown): Registration {
  const record = objectValue(value, "coordination registration");
  const events = record.events;
  const registration: Registration = {
    registration_id: stringValue(record, "registration_id"),
    heartbeat_interval_ms: numberValue(record, "heartbeat_interval_ms"),
  };
  // An endpoint that serves only Server-Sent Events has no NNG hint URL.
  if (typeof events === "object" && events !== null &&
      typeof (events as Record<string, unknown>).url === "string") {
    const hint = events as Record<string, unknown>;
    registration.events = { url: stringValue(hint, "url"), topic: stringValue(hint, "topic") };
  }
  return registration;
}

export function parseMessages(value: unknown): InboxMessage[] {
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
      broadcast_id: typeof item.broadcast_id === "string" ? item.broadcast_id : undefined,
      recipient_count: typeof item.recipient_count === "number" ? item.recipient_count : undefined,
      in_reply_to: typeof item.in_reply_to === "string" ? item.in_reply_to : undefined,
    };
  });
}

export const COORDINATION_ERROR_CODES = [
  "invalid_argument",
  "registration_expired",
  "unauthorized",
  "idempotency_conflict",
  "mailbox_full",
  "receipt_invalid",
  "lease_invalid",
  "resource_conflict",
];

/**
 * Returns the coordination error code in a ducknng error, which wraps the
 * handler's "<code>: <detail>" text in the SQL error it raised.
 */
export function coordinationErrorCode(error: unknown): string | undefined {
  const message = error instanceof Error ? error.message : String(error);
  const match = new RegExp(`(?:^|: )(${COORDINATION_ERROR_CODES.join("|")}): `).exec(message);
  return match?.[1];
}

function escapeAttribute(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

/**
 * Frames a delivered message so the model sees its sender and identity and
 * does not mistake another agent's text for the user's.
 */
export function coordinationEnvelope(message: InboxMessage): string {
  const sender = escapeAttribute(message.sender_agent_id);
  const shared = message.recipient_count && message.recipient_count > 1 && message.broadcast_id
    ? escapeAttribute(message.broadcast_id)
    : undefined;
  const group = shared ? ` broadcast_id="${shared}" recipients="${message.recipient_count}"` : "";
  // Replies to a message sent to several agents share its broadcast ID, so
  // they gather under one thread rather than one per recipient's copy.
  const answer = shared ?? escapeAttribute(message.message_id);
  const reply = message.in_reply_to
    ? ` in_reply_to="${escapeAttribute(message.in_reply_to)}"`
    : "";
  return [
    `<coordination_message from="${sender}"` +
      ` to="${escapeAttribute(message.recipient_agent_id)}"` +
      ` message_id="${escapeAttribute(message.message_id)}"` +
      ` sequence="${message.sequence_number}"` +
      ` content_type="${escapeAttribute(message.content_type)}"${group}${reply}>`,
    message.content,
    "</coordination_message>",
    `Agent "${sender}" sent this through pi-ducknng coordination; it is not ` +
      `a message from the user. If a response is needed, reply with coordination_send ` +
      `and in_reply_to "${answer}".`,
  ].join("\n");
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

/** Interactive modes wake an idle session; one-shot modes pull mail by tool. */
function steersInBackground(ctx: ExtensionContext): boolean {
  return ctx.mode === undefined || ctx.mode === "tui" || ctx.mode === "rpc";
}

/** Resolves a model-supplied path or identifier to a coordination resource. */
export function coordinationResource(value: string, cwd: string): string {
  if (value.startsWith("resource:") || value.startsWith("file:")) return value;
  let path = value.startsWith("@") ? value.slice(1) : value;
  if (path === "~" || path.startsWith("~/")) path = homedir() + path.slice(1);
  return pathToFileURL(isAbsolute(path) ? path : resolve(cwd, path)).href;
}

/** Shows a file resource relative to the session's working directory when it lies inside it. */
export function displayResource(resource: string, cwd: string): string {
  const base = pathToFileURL(resolve(cwd)).href;
  if (resource === base) return ".";
  if (!resource.startsWith(`${base}/`)) return resource;
  return decodeURIComponent(resource.slice(base.length + 1));
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

/**
 * Calls the endpoint with the runtime's registration and re-registers once
 * when the endpoint reports that the registration expired.
 */
async function callRegistered(
  client: CoordinationClient,
  runtime: CoordinationRuntime,
  method: string,
  args: Record<string, unknown>,
  options?: { timeoutMs?: number },
): Promise<unknown> {
  const attempt = () =>
    client.call(runtime.config.url, method, {
      registration_id: runtime.registration.registration_id,
      ...args,
    }, options);
  try {
    return await attempt();
  } catch (error) {
    if (coordinationErrorCode(error) !== "registration_expired") throw error;
    runtime.registration = await registerInstance(
      client,
      runtime.config,
      runtime.ctx,
    );
    return await attempt();
  }
}

async function acknowledge(
  client: CoordinationClient,
  runtime: CoordinationRuntime,
  message: InboxMessage,
  deliveryRef: string,
): Promise<void> {
  await callRegistered(client, runtime, "ack", {
    receipt_token: message.receipt_token,
    delivery_ref: deliveryRef,
  });
}

function recordDelivery(
  pi: ExtensionAPI,
  runtime: CoordinationRuntime,
  message: InboxMessage,
  path: "steer" | "tool",
): void {
  pi.appendEntry(DELIVERY_ENTRY_TYPE, {
    message_id: message.message_id,
    sender_agent_id: message.sender_agent_id,
    sequence_number: message.sequence_number,
    path,
  });
  runtime.delivered.add(message.message_id);
}

async function steerMessage(
  pi: ExtensionAPI,
  client: CoordinationClient,
  runtime: CoordinationRuntime,
  message: InboxMessage,
  signal: AbortSignal,
): Promise<void> {
  signal.throwIfAborted();
  if (!runtime.delivered.has(message.message_id)) {
    pi.sendMessage({
      customType: COORDINATION_MESSAGE_TYPE,
      content: coordinationEnvelope(message),
      display: true,
      details: {
        message_id: message.message_id,
        sender_agent_id: message.sender_agent_id,
        recipient_agent_id: message.recipient_agent_id,
        sequence_number: message.sequence_number,
        content_type: message.content_type,
        broadcast_id: message.broadcast_id,
        in_reply_to: message.in_reply_to,
      },
    }, { triggerTurn: true, deliverAs: "steer" });
    recordDelivery(pi, runtime, message, "steer");
  }
  await acknowledge(
    client,
    runtime,
    message,
    `steer:${runtime.ctx.sessionManager.getSessionId()}:${message.message_id}`,
  );
}

async function renewLeases(
  client: CoordinationClient,
  runtime: CoordinationRuntime,
): Promise<void> {
  const now = Date.now();
  for (const [leaseId, lease] of runtime.leases) {
    if (lease.expiresAt - now > lease.ttlMs / 2) continue;
    try {
      const renewed = objectValue(
        await callRegistered(client, runtime, "reserve", {
          resource: lease.resource,
          lease_id: leaseId,
          ttl_ms: lease.ttlMs,
        }),
        "coordination reservation",
      );
      lease.expiresAt = Date.now() + lease.ttlMs;
      lease.fencingValue = numberValue(renewed, "fencing_value");
    } catch (error) {
      if (coordinationErrorCode(error) === "lease_invalid") {
        runtime.leases.delete(leaseId);
      }
      throw error;
    }
  }
}

async function runAdapter(
  pi: ExtensionAPI,
  client: CoordinationClient,
  runtime: CoordinationRuntime,
  signal: AbortSignal,
): Promise<void> {
  let nextHeartbeat = Date.now() + runtime.registration.heartbeat_interval_ms;
  let failures = 0;
  // Lease expiry publishes no hint, so after a failure poll quickly until any
  // message this session held has become visible again.
  let fastPollUntil = 0;
  const receives = steersInBackground(runtime.ctx);

  while (!signal.aborted) {
    try {
      if (Date.now() >= nextHeartbeat) {
        await callRegistered(client, runtime, "heartbeat", {
          status: {
            idle: runtime.ctx.isIdle(),
            pending_messages: runtime.ctx.hasPendingMessages(),
          },
        });
        await renewLeases(client, runtime);
        nextHeartbeat = Date.now() + runtime.registration.heartbeat_interval_ms;
      }
      let messages: InboxMessage[] = [];
      if (receives) {
        messages = parseMessages(await callRegistered(
          client,
          runtime,
          "receive",
          { limit: 8, visibility_timeout_ms: 30_000 },
        ));
        for (const message of messages) {
          await steerMessage(pi, client, runtime, message, signal);
        }
      }
      if (messages.length === 0) {
        const pollMs = runtime.subscription && Date.now() >= fastPollUntil
          ? FALLBACK_POLL_MS
          : POLL_MS;
        await runtime.wakeup.wait(
          Math.max(0, Math.min(pollMs, nextHeartbeat - Date.now())),
          signal,
        );
      }
      failures = 0;
    } catch (error) {
      if (signal.aborted) break;
      failures += 1;
      fastPollUntil = Date.now() + 30_000 + POLL_MS;
      if (failures === 1) notifyError(runtime.ctx, error);
      try {
        await sleep(Math.min(1_000 * 2 ** failures, 10_000), signal);
      } catch {
        break;
      }
    }
  }
}

async function stopRuntime(
  client: CoordinationClient,
  runtime: CoordinationRuntime,
): Promise<void> {
  runtime.controller.abort();
  await runtime.task.catch(() => undefined);
  await runtime.subscription?.close().catch(() => undefined);
  for (const leaseId of runtime.leases.keys()) {
    try {
      await client.call(runtime.config.url, "release", {
        registration_id: runtime.registration.registration_id,
        lease_id: leaseId,
      });
    } catch {
      // Unreleased leases expire on the endpoint.
    }
  }
  runtime.leases.clear();
  try {
    await client.call(runtime.config.url, "unregister", {
      registration_id: runtime.registration.registration_id,
    });
  } catch {
    // Presence expires by lease when the endpoint is unavailable.
  }
  client.release?.(runtime.config.url);
}

function toolResult(value: unknown, text?: string) {
  return {
    content: [{ type: "text" as const, text: text ?? JSON.stringify(value, null, 2) }],
    details: value,
  };
}

const SendParameters = Type.Object(
  {
    recipient: Type.Optional(Type.String({ description: "Stable agent ID of one recipient mailbox" })),
    recipients: Type.Optional(Type.Array(Type.String(), {
      minItems: 1,
      maxItems: 256,
      description: "Several recipient agent IDs; each gets its own copy",
    })),
    broadcast: Type.Optional(Type.Boolean({
      description: "Send to every other live agent in the project",
    })),
    content: Type.String({ description: "Message text; line breaks are allowed" }),
    content_type: Type.Optional(
      Type.String({ description: "Media type of content, default text/plain" }),
    ),
    in_reply_to: Type.Optional(Type.String({
      description:
        "ID this message answers: the broadcast_id of a message sent to several agents, " +
        "so all replies gather in one thread, otherwise the message_id",
    })),
  },
  { additionalProperties: false },
);
const InboxParameters = Type.Object(
  {
    wait_ms: Type.Optional(Type.Integer({
      minimum: 0,
      maximum: 25_000,
      description: "Wait up to this long for mail when the mailbox is empty",
    })),
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 32 })),
  },
  { additionalProperties: false },
);
const EmptyParameters = Type.Object({}, { additionalProperties: false });
const ReserveParameters = Type.Object(
  {
    resource: Type.String({
      description: "A file or directory path, a file:/// URI, or a resource: identifier",
    }),
    ttl_ms: Type.Optional(Type.Integer({
      minimum: 5_000,
      maximum: 300_000,
      description: "Lease duration; this session renews held leases until release",
    })),
  },
  { additionalProperties: false },
);
const ReleaseParameters = Type.Object(
  {
    lease_id: Type.Optional(Type.String()),
    resource: Type.Optional(Type.String({
      description: "Release this session's lease on this path or identifier",
    })),
  },
  { additionalProperties: false },
);

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
  const attached = (): CoordinationRuntime => {
    if (!runtime) {
      throw new Error(
        "coordination is not attached; set PI_DUCKNNG_COORDINATION_URL, " +
          "PI_DUCKNNG_COORDINATION_PROJECT, and PI_DUCKNNG_AGENT_ID",
      );
    }
    return runtime;
  };

  pi.registerTool({
    name: "coordination_send",
    label: "Send coordination message",
    description:
      "Durably send a message to one agent, several agents, or every other live agent " +
      "in this coordination project. Use recipients or broadcast to fan work out, and " +
      "in_reply_to to answer a message or broadcast.",
    parameters: SendParameters,
    execute: async (toolCallId, params) => {
      const active = attached();
      const addressing = params.broadcast
        ? { broadcast: true }
        : params.recipients
          ? { recipient_agent_ids: params.recipients }
          : { recipient_agent_id: params.recipient };
      const sent = objectValue(await callRegistered(client, active, "send", {
        ...addressing,
        idempotency_key: `${active.ctx.sessionManager.getSessionId()}:${toolCallId}`,
        content: params.content,
        ...(params.content_type ? { content_type: params.content_type } : {}),
        ...(params.in_reply_to ? { in_reply_to: params.in_reply_to } : {}),
      }), "coordination send reply");
      return toolResult(sent);
    },
  });

  pi.registerTool({
    name: "coordination_inbox",
    label: "Read coordination inbox",
    description:
      "Receive and acknowledge messages sent to this agent, optionally waiting for new mail.",
    parameters: InboxParameters,
    execute: async (toolCallId, params, signal) => {
      const active = attached();
      const deadline = Date.now() + (params.wait_ms ?? 0);
      let messages: InboxMessage[] = [];
      for (;;) {
        messages = parseMessages(await callRegistered(
          client,
          active,
          "receive",
          { limit: params.limit ?? 8, visibility_timeout_ms: 30_000 },
        ));
        if (messages.length > 0 || Date.now() >= deadline) break;
        await active.wakeup.wait(
          Math.min(active.subscription ? FALLBACK_POLL_MS : 250, deadline - Date.now()),
          signal,
        );
        signal?.throwIfAborted();
      }
      const delivered = [];
      for (const message of messages) {
        signal?.throwIfAborted();
        const repeated = active.delivered.has(message.message_id);
        if (!repeated) recordDelivery(pi, active, message, "tool");
        await acknowledge(client, active, message, `tool:${toolCallId}:${message.message_id}`);
        delivered.push({ ...message, receipt_token: undefined, previously_delivered: repeated });
      }
      const text = delivered.length === 0
        ? "No coordination messages."
        : messages.map(coordinationEnvelope).join("\n\n");
      return toolResult({ messages: delivered }, text);
    },
  });

  pi.registerTool({
    name: "coordination_agents",
    label: "List coordination agents",
    description:
      "List live agents and active resource reservations in this coordination project.",
    parameters: EmptyParameters,
    execute: async (_toolCallId, _params, _signal, _onUpdate, ctx) => {
      const active = attached();
      const agents = objectValue(
        await callRegistered(client, active, "list_agents", {}),
        "coordination agents reply",
      );
      const reservations = objectValue(
        await callRegistered(client, active, "list_reservations", {}),
        "coordination reservations reply",
      );
      const listed = (reservations.reservations as Array<Record<string, unknown>>)
        .map((reservation) => ({
          ...reservation,
          resource: displayResource(String(reservation.resource), ctx.cwd),
        }));
      return toolResult({
        self: active.config.agentId,
        agents: agents.agents,
        reservations: listed,
      });
    },
  });

  pi.registerTool({
    name: "coordination_reserve",
    label: "Reserve coordination resource",
    description:
      "Take an advisory lease on a path or resource: identifier. While held, other " +
      "coordinated agents' edit and write tools are blocked on reserved paths.",
    parameters: ReserveParameters,
    execute: async (toolCallId, params, _signal, _onUpdate, ctx) => {
      const active = attached();
      if (!params.resource) throw new Error("resource is required");
      const ttlMs = params.ttl_ms ?? RESERVATION_TTL_MS;
      const reserved = objectValue(await callRegistered(client, active, "reserve", {
        resource: coordinationResource(params.resource, ctx.cwd),
        operation_key: `${active.ctx.sessionManager.getSessionId()}:${toolCallId}`,
        ttl_ms: ttlMs,
      }), "coordination reservation");
      const leaseId = stringValue(reserved, "lease_id");
      if (reserved.active !== false) {
        active.leases.set(leaseId, {
          resource: stringValue(reserved, "resource"),
          ttlMs,
          expiresAt: numberValue(reserved, "expires_at_ms"),
          fencingValue: numberValue(reserved, "fencing_value"),
        });
      }
      return toolResult(reserved);
    },
  });

  pi.registerTool({
    name: "coordination_release",
    label: "Release coordination resource",
    description: "Release a lease taken by this session with coordination_reserve.",
    parameters: ReleaseParameters,
    execute: async (_toolCallId, params, _signal, _onUpdate, ctx) => {
      const active = attached();
      let leaseId = params.lease_id;
      if (!leaseId && params.resource) {
        const resource = coordinationResource(params.resource, ctx.cwd);
        const listed = objectValue(
          await callRegistered(client, active, "list_reservations", { resource }),
          "coordination reservations reply",
        );
        const own = (listed.reservations as Array<Record<string, unknown>>)
          .find((reservation) => reservation.owned_by_caller === true);
        leaseId = typeof own?.lease_id === "string" ? own.lease_id : undefined;
      }
      if (!leaseId) throw new Error("no lease held by this session matches the request");
      const released = await callRegistered(client, active, "release", {
        lease_id: leaseId,
      });
      active.leases.delete(leaseId);
      return toolResult(released);
    },
  });

  // Reservations held by other coordinated sessions block Pi's own file edits.
  pi.on("tool_call", async (event, ctx) => {
    const active = runtime;
    if (!active || (event.toolName !== "edit" && event.toolName !== "write")) {
      return undefined;
    }
    const path = (event.input as Record<string, unknown>).path;
    if (typeof path !== "string" || !path) return undefined;
    try {
      const listed = objectValue(
        await callRegistered(client, active, "list_reservations", {
          resource: coordinationResource(path, ctx.cwd),
        }),
        "coordination reservations reply",
      );
      const held = (listed.reservations as Array<Record<string, unknown>>)
        .filter((reservation) => reservation.owned_by_caller !== true);
      if (held.length === 0) return undefined;
      const holders = held
        .map((reservation) =>
          `${displayResource(String(reservation.resource), ctx.cwd)} ` +
          `(held by ${String(reservation.owner_agent_id)})`)
        .join(", ");
      return {
        block: true,
        reason:
          `${path} is reserved: ${holders}. Coordinate with coordination_send ` +
          "or wait for the reservation to be released.",
      };
    } catch (error) {
      notifyError(ctx, error);
      return undefined;
    }
  });

  pi.on("session_start", async (_event, ctx) =>
    await serializeLifecycle(async () => {
      if (runtime) {
        const previous = runtime;
        runtime = undefined;
        await stopRuntime(client, previous);
      }
      try {
        const config = coordinationConfig(pi);
        if (!config) {
          if (typeof pi.getActiveTools === "function") {
            pi.setActiveTools(
              pi.getActiveTools().filter((name) => !COORDINATION_TOOLS.includes(name)),
            );
          }
          return;
        }
        const manifest = await client.describe(config.url);
        const declared = new Set(manifest.methods.map((method) => method.name));
        for (const method of REQUIRED_METHODS) {
          if (!declared.has(method)) {
            throw new Error(`coordination endpoint does not declare ${method}`);
          }
        }
        client.claim?.(config.url);
        const registration = await registerInstance(client, config, ctx);
        const controller = new AbortController();
        const started: CoordinationRuntime = {
          config,
          ctx,
          registration,
          delivered: deliveredMessageIds(ctx),
          leases: new Map(),
          controller,
          task: Promise.resolve(),
          wakeup: new Wakeup(),
        };
        if (registration.events && client.subscribe) {
          try {
            started.subscription = await client.subscribe(
              registration.events.url,
              registration.events.topic,
              () => started.wakeup.notify(),
            );
          } catch (error) {
            notifyError(ctx, error);
          }
        }
        started.task = runAdapter(pi, client, started, controller.signal);
        runtime = started;
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
