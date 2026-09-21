# Prompt: Design a durable coordination endpoint for Pi agents

## Audience

This prompt is for the human maintainers and maintainer agents of `pi-ducknng`, `ducknng`, and the Pi adapter. Work as one architecture group: distinguish observed behavior from proposals, cite exact source revisions, and stop for human decisions where the contract is ambiguous.

## Goal

Design the smallest durable coordination service that lets independently started Pi sessions discover one another, exchange messages, wake one another, report presence, and reserve resources without making any Pi session the owner of shared coordination state.

The service must be a long-lived endpoint process outside individual Pi sessions. It must expose a bounded manifested API. It must not expose arbitrary R evaluation.

Treat this as a protocol and ownership problem before treating it as a UI or orchestration problem.

## Governing constraints

1. Read [`THESIS.md`](THESIS.md), [`AGENTS.md`](AGENTS.md), [`DEPENDENCIES`](DEPENDENCIES), and [`VENDORING.md`](VENDORING.md) before proposing code.
2. Keep `pi-ducknng` thin. Compose the DuckDB host API, `ducknng`, `nanonext`, `mirai`, and Pi's public integration APIs. Do not reproduce their transport, TLS, session, AIO, cancellation, or scheduler semantics.
3. Put NNG carrier, framing, event-envelope, TLS, identity, and generic socket changes in upstream `ducknng` first. Update the pinned dependency and vendored subtree only after that upstream contract is accepted.
4. Name the owner of every operation and state transition: coordination endpoint, durable store, NNG carrier, Pi adapter, DuckDB host API, or credential-injection point.
5. Preserve manifested discovery. A client must be able to discover supported coordination methods and optional event capabilities before calling them.
6. Keep credentials out of method payloads, message bodies, manifests, logs, and Pi model context. Inject credentials at the endpoint or client-process boundary and map authenticated identities to authorized projects and agent instances.
7. Do not inspect, enumerate, or modify `/mnt/data/BixCTF` or its sibling paths. Any separately authorized precision-data operation is confined to the exact subtree `/mnt/data/BixCTF/precision`.

## Verified starting point to refresh

Refresh these observations against the revisions actually used for the design:

- `pi-ducknng` already provides manifested unary RPC discovery and calls, NNG identity/framing/TLS/session/cancellation composition, generic Pi endpoint tools, and persistent endpoint processes.
- The vendored `ducknng` protocol requires REQ/REP for the initial RPC transport, reserves an event frame type for future push traffic, and exposes raw NNG `pub`/`sub` sockets with prefix subscriptions.
- NNG PUB/SUB is best-effort. It has no delivery acknowledgement, does not provide durable replay to late or disconnected subscribers, and may discard messages when subscriber queues fill. Consult the current [NNG SUB protocol documentation](https://nng.nanomsg.org/ref/proto/sub.html).
- The latest checked [`@earendil-works/pi-agent-core`](https://github.com/earendil-works/pi/tree/b6419322e6c8bd9c0dc82fcdcdb68a28990f6519/packages/agent) package is 0.86.1 at source revision `b6419322e6c8bd9c0dc82fcdcdb68a28990f6519`. Since 0.84.0, `AgentHarness` and the v4 lane-based `Session`, `SessionStorage`, and `SessionRepo` APIs are public root exports rather than an experimental subpath.
- An `AgentLane` exposes durable operations and queue methods including `accept`, `prompt`, `resume`, `steer`, `followUp`, `nextRun`, `getResult`, and `watch`. In the checked implementation, `steer` commits the pending message and lane state through the `Session` before returning its queue entry ID. `watch` pairs a snapshot with buffered subsequent events. Verify these semantics against the pinned release and chosen `SessionRepo`, including any operation path that can still reject with `HarnessNotImplemented`.
- The coding-agent remote server and mini multi-process presentation remain under `src/experimental` even though the underlying core harness API is public. The [mini architecture](https://github.com/earendil-works/pi/blob/b6419322e6c8bd9c0dc82fcdcdb68a28990f6519/packages/coding-agent/src/experimental/mini/README.md) is useful evidence for call/result/cancel/event separation, liveness, worker ownership, operation recovery, and snapshot-plus-event replication; it is not a stable coordination wire contract.
- Pi's interactive extension API exposes custom-message injection and steering through `pi.sendMessage(..., { triggerTurn: true, deliverAs: "steer" })`. The coding-agent SDK and RPC modes also expose steering behavior. Treat these as a different, potentially weaker delivery boundary than an `AgentHarness` lane backed by a durable `SessionRepo`.
- [`nicobailon/pi-messenger`](https://github.com/nicobailon/pi-messenger) at revision `09937ed647a1b07a3b595bf75943feacb80ff123` is a useful reference for Pi lifecycle integration, registration UX, presence, direct messages, broadcasts, reservations, and steering-message injection. Its filesystem inbox consumer invokes delivery and then removes the inbox file; malformed messages are also removed. Treat it as a product and adapter reference, not as evidence of durable acknowledgement or brokered pub/sub semantics.

Record updated revisions and links in the design result.

## Architecture hypothesis to challenge

Start with this candidate, then try to disprove it:

> Use manifested REQ/REP methods and endpoint-owned durable state as the authoritative command and mailbox plane. Add a separate NNG PUB/SUB socket only as an optional low-latency notification plane. A notification means “state may have changed”; the subscriber must call `receive` to obtain durable messages. Correctness must remain unchanged if every notification is delayed, duplicated, reordered, or dropped.

Do **not** use bare PUB/SUB as the inbox. The strongest objection is decisive: it cannot satisfy offline delivery, acknowledgement, replay, or idempotent recovery after a receiver crash.

If the optional notification plane survives review:

- advertise its URL, protocol version, and topic/framing rules as an optional manifest capability;
- use a separate NNG socket pattern rather than pretending a REQ/REP socket also pushes events;
- publish no message body or credential-bearing metadata;
- make each event a wake-up hint containing only enough opaque identity or high-water information to trigger a bounded `receive` call;
- specify prefix-filter framing explicitly, because NNG subscriptions match bytes at the start of the message while the current RPC envelope does not begin with a topic;
- retain bounded polling or reconnect catch-up so a missed notification cannot strand mail;
- treat prefix filtering as routing, not authorization: use an enforceable project-scoped listener, authenticated relay, or equivalent isolation, and disable PUB/SUB in multi-tenant deployments if no such mechanism is demonstrated;
- account for activity-metadata leakage even when topics and payloads are opaque;
- place any generic event framing or carrier change in upstream `ducknng`.

## Questions the human maintainers must answer

Do not hide these choices in implementation defaults:

1. Is the first supported deployment machine-local only, or must it work across hosts?
2. Must messages survive only Pi-session restarts, or also endpoint-process and host restarts?
3. Is the trust model one user, one project, or multiple mutually untrusted users/projects?
4. What does `ack` prove: endpoint receipt, return from a volatile extension injection call, durable commit to an `AgentHarness` lane, appearance in model context, or completion of requested work? Prefer the narrowest claim that can be observed reliably. Do not publish one undifferentiated acknowledgement strength for adapters with different guarantees.
5. Which target modes are supported initially: independently owned interactive Pi sessions, embedded `AgentHarness` sessions, or a host process that owns multiple harness workers?
6. What retention, quota, maximum payload, and dead-letter policies are required?
7. Are reservations advisory conflict signals or enforced access controls? Which resource namespaces exist besides canonical file paths?
8. Which stable identity owns an offline mailbox, and may one human-visible agent name have multiple live instances? Define routing when zero, one, or several instances are registered; do not make durable mailbox ownership depend on an ephemeral Pi session ID.
9. Should broadcast create one durable inbox record per recipient, or use a separately retained stream with per-recipient cursors?
10. Does the Pi adapter own a persistent DuckDB/NNG client connection, poll with short-lived connections, or delegate listening to a sidecar? Reconcile the answer with `THESIS.md` connection ownership and shutdown rules.

## Required protocol model

Propose the minimal method set. Begin with the following names, but change them when a smaller or more coherent contract is demonstrated:

- `register`: create or resume an authenticated agent instance and return its opaque registration token, server time, lease expiry, and heartbeat interval. Declare the adapter kind and its evidence-backed delivery capability, such as volatile extension injection or durable harness-lane commit. Define a retry key or resume token so a timed-out call cannot create an unknown second instance.
- `heartbeat`: idempotently renew a registration lease and report bounded status/capabilities. Presence is derived from an unexpired server-side lease, not asserted as a permanent boolean.
- `list_agents`: discover authorized live or recently seen agents within one project scope.
- `send`: durably enqueue a bounded message to an explicitly defined stable mailbox identity. Require a caller-supplied idempotency key scoped to the authenticated sender and project; repeated calls return the original message identity without creating another delivery.
- `receive`: return a bounded batch and lease each delivery for a visibility timeout. Include stable message IDs, monotonic per-inbox sequence values where feasible, and opaque receipt tokens.
- `ack`: idempotently acknowledge a receipt token after the Pi adapter has successfully returned from the chosen injection API. Define exactly what that API return proves, plus stale, expired, duplicate, and unauthorized acknowledgements.
- `reserve`: atomically acquire an advisory resource lease and return a recoverable operation result, opaque lease token, expiry, and monotonically increasing fencing value. Define canonicalization, overlap conflicts, safe retry after an unknown outcome, and explicit renewal. Do not couple renewal to registration heartbeat unless the protocol says so.
- `release`: idempotently release only the lease identified by its token. A stale owner must not be able to release a newer coordinator lease. A fencing value prevents stale writes to an external resource only when that resource validates the value.
- `unregister`: optionally end one registration explicitly; expiry remains the crash-recovery mechanism. Define idempotent retry and the effect on durable mailbox state.

For every method, specify:

- authenticated principal and project scope;
- request and response schema;
- size and batch bounds;
- idempotency key and replay behavior;
- transaction boundary and durable state transition;
- authorization failures and in-band error shape;
- timeout, lease expiry, and server-clock behavior;
- safe retry rules;
- audit fields that do not expose message content or credentials.

Use an endpoint-owned transactional store. If DuckDB is selected, state why its single-writer model is acceptable under the one-endpoint-owner design and show how restart recovery works. Do not allow clients to mutate coordination tables through arbitrary SQL or R evaluation.

## Delivery semantics

State the guarantee precisely. A credible initial target is durable **at-least-once delivery with idempotent send and explicit acknowledgement**, not exactly-once execution.

Model at least these transitions:

```text
send:      absent -> queued
receive:   queued | lease_expired -> leased(receipt, deadline)
ack:       leased(receipt) -> acknowledged
expiry:    leased(deadline passed) -> queued
retention: acknowledged | expired -> removed according to policy
```

A receiver can crash after Pi accepts a steering message but before `ack` reaches the endpoint. Redelivery is therefore possible. Carry the stable message ID into Pi context and define where the adapter records injected IDs.

There is no atomic transaction spanning the endpoint store and a target Pi session store. With an interactive extension, recording before injection can suppress a message that Pi never accepted, while recording after injection permits a duplicate in the crash window. With `AgentHarness`, a successful `steer` can provide evidence of a committed lane queue entry when the selected `SessionRepo` is durable, but an adapter crash can still occur after that commit and before the coordination `ack`.

Carry the coordination message ID inside the typed Pi message. For a harness lane, determine whether the durable queue and transcript can be queried by that ID before retrying; do not add a second local inbox if the harness already provides the needed durable admission and recovery semantics. Choose and document ordering, reconciliation, and the narrow fact proved by each API return. Do not claim exactly-once model behavior or exactly-once work execution.

## Pi integration boundary

Keep the coordination protocol independent of Pi internals. Support only integration modes whose public behavior has been probed.

For an independently owned interactive Pi session, implement an extension adapter that:

- registers on `session_start` and expires or unregisters on `session_shutdown`;
- owns and cancels its heartbeat and receive timers;
- wakes on an optional event hint or bounded receive poll;
- fetches durable messages with `receive`;
- injects a typed custom message carrying the coordination ID through `pi.sendMessage(..., { triggerTurn: true, deliverAs: "steer" })`;
- acknowledges only after the injection call returns, while advertising that this proves API acceptance rather than a durable harness commit.

For a process that owns an `AgentHarness`, implement a harness adapter that:

- maps the registered agent instance to an explicit session and lane;
- uses public `AgentLane` admission, queue, recovery, and watch APIs rather than reimplementing harness session persistence;
- selects `steer`, `followUp`, or a new durable operation according to observed lane state and the intended turn semantics;
- carries the coordination message ID in the queued `AgentMessage` and records the returned operation or queue entry identity;
- acknowledges the external inbox only after the target lane commit succeeds, then reconciles a redelivery against durable lane state;
- leaves `drive`, operation recovery, model runtime, and worker lifecycle with the process that owns the harness.

`AgentLane.watch` is an authoritative snapshot-plus-event view of one harness lane, not agent discovery or a cross-agent mailbox. Use it to observe a target session and avoid duplicating local state; do not substitute it for endpoint-owned offline mail, registration, authorization, or reservations.

Use documented coding-agent SDK or RPC steering only when that integration mode is selected and tested. The public `pi-agent-core` harness is a current integration target, not a hypothetical future API; the coding-agent remote server around it remains experimental. Never let model-generated text choose credentials, project authorization, or unrestricted endpoint methods.

## Comparison required

Compare these candidates with evidence rather than labels:

1. `pi-messenger`-style filesystem mailboxes and watchers;
2. pure NNG PUB/SUB;
3. manifested REQ/REP with bounded polling or long polling;
4. manifested REQ/REP plus durable inboxes and optional PUB/SUB wake-up hints.

Evaluate each against:

- disconnected and late receivers;
- endpoint and client crash recovery;
- acknowledgement and duplicate handling;
- ordering and backpressure;
- machine-local and cross-host operation;
- TLS, authentication, project isolation, and metadata leakage;
- compatibility with independently owned Pi extension/SDK/RPC sessions and with host-owned `AgentHarness` sessions;
- implementation and operational cost;
- reversibility and migration path;
- ownership under `THESIS.md` and `VENDORING.md`.

Borrow proven Pi UX and lifecycle ideas from `pi-messenger`; do not clone its filesystem state model into the transport layer without demonstrating that it meets the chosen guarantees.

## Required failure probes

Before the architecture checkpoint, use only bounded spikes needed to resolve disputed behavior. After human approval, build the smallest executable vertical slice that can test the accepted contract. It must demonstrate:

1. a message sent while the receiver is offline is delivered after registration resumes;
2. duplicate `send` calls with one idempotency key create one logical message;
3. a receiver crash after `receive` and before `ack` causes lease expiry and redelivery with the same message ID;
4. duplicate `ack` calls are harmless;
5. endpoint restart preserves queued messages, acknowledgements required by retention policy, and reservation leases;
6. a dropped PUB/SUB notification does not delay delivery beyond the documented polling/reconnect bound;
7. full subscriber queues or slow subscribers cannot block authoritative `send` persistence;
8. expired presence disappears according to server time and does not delete durable mail accidentally;
9. overlapping resource reservations conflict deterministically, expired leases can be reacquired, and stale tokens cannot release a newer coordinator lease; any claim about preventing external writes is tested only against a resource that enforces monotonically increasing fencing values;
10. unauthorized agents cannot list, receive, acknowledge, reserve, or subscribe across project boundaries; if PUB/SUB cannot enforce that boundary, it remains disabled for that deployment;
11. each supported Pi integration mode is probed against its deployed public API, recording the observed busy-session steering boundary and idle-session turn behavior rather than assuming parity across modes;
12. a durable harness-lane commit followed by an adapter crash before coordination `ack` is reconciled by coordination message ID without silently dropping the message; any duplicate behavior is stated precisely;
13. harness watches use their snapshot-and-buffered-event contract without adding a competing local event log, and reconnect from a fresh snapshot;
14. all timers, watches, session repositories, sockets, DuckDB handles, and endpoint processes close cleanly.

Run focused tests first. Run the repository's full required checks before proposing a merge.

## Deliverables

Produce these artifacts in order:

1. **Evidence note** — exact revisions, observed behavior, bounded spike results, and any uncertainty.
2. **Draft protocol and Pi adapter contract** — method schemas, state machines, identity and authorization, storage and lifecycle ownership, steering and duplicate semantics, optional notification framing, and a compatibility matrix.
3. **Decision record** — chosen architecture, strongest rejected alternative, reasons, risks, cost, reversibility, and conditions that would reopen the decision.
4. **Implementation and test plan** — smallest reviewable changes and required probes, with upstream `ducknng` work separated from `pi-ducknng` composition work.
5. **Human architecture checkpoint** — approve, revise, or reject the preceding artifacts before production implementation.
6. **Vertical-slice test report** — after approval, record commands, results, and unresolved failures; revise the contract when executable evidence disproves it.

The result is acceptable only if a maintainer can explain what survives each crash boundary, what an acknowledgement proves, who owns every long-lived resource, and why message correctness does not depend on PUB/SUB delivery.
