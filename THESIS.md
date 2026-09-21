# pi-ducknng thesis

## Claim

`pi-ducknng` projects manifested ducknng RPC endpoints into Pi without adding a
Node NNG binding, a second wire protocol, or an endpoint-specific Pi tool for
every method. Endpoint placement is separate from discovery and invocation.
The package places a persistent R endpoint on demand. A separately started
coordination endpoint gives Pi sessions durable mail and advisory
reservations. The generic tools accept any compatible URL.

## Generic Pi tools

The Pi package exposes three model tools:

- `persistent_r_start()` starts the local R endpoint and returns its NNG URL;
- `ducknng_describe(url)` returns an endpoint's ducknng version-1 manifest;
- `ducknng_call(url, method, arguments)` rejects undeclared methods and invokes
  a declared JSON-request method through a fresh DuckDB client.

```text
Pi extension -> @duckdb/node-api -> DuckDB -> vendored ducknng
             -> NNG -> endpoint process
```

Each describe or call opens and closes its own in-memory DuckDB instance.
Endpoint state belongs to the endpoint process and survives those clients.

## R endpoint

`tools/pi-r-endpoint.R` declares `eval` and `close`. `eval` accepts R source
plus `envir` and `enclos` expressions and evaluates in a mirai-owned
persistent environment. Supported atomic vectors and data frames return as
nanoarrow IPC. Other R values fail explicitly. `close` stops the endpoint and
returns a JSON acknowledgement. The serve loop exits after 30 seconds without
a request, which discards the R environment.

## Coordination endpoint

`tools/pi-coordination-endpoint.R` runs independently of any Pi session and is
the sole owner of one DuckDB database file. It declares `register`,
`heartbeat`, `list_agents`, `send`, `receive`, `ack`, `reserve`, `release`,
and `unregister`. It does not declare `eval`, and clients reach its tables
only through those methods.

- **Identity.** A mailbox belongs to a stable `(project_id, agent_id)`.
  `register` binds an instance, such as a Pi session ID, to that mailbox and
  returns an opaque `registration_id` for later calls. Presence is an
  unexpired server-side lease renewed by `heartbeat`. Live instances of one
  agent ID are competing consumers of its mailbox.
- **Delivery.** `send` commits the message before replying. It is idempotent
  for each project, sender, and `idempotency_key`; reusing a key for
  different content fails. `receive` leases up to 32 queued messages in
  mailbox sequence order for a visibility timeout. When a lease expires, the
  message returns to the queue with the same message ID. `ack` accepts only
  the current receipt, is idempotent, and records the delivery capability the
  receiving instance declared at registration. Delivery is at least once.
  Model execution is not exactly once.
- **Reservations.** `reserve` takes an advisory lease on a canonical
  `resource:` identifier or a lexically canonical `file:///` URI. A file URI
  conflicts with its ancestors and descendants. The endpoint never inspects
  the referenced filesystem. Each acquisition draws a fencing value from a
  project-wide counter. Retrying with the same `operation_key` replays the
  original result. Reacquiring after release or expiry requires a new key.
  Renewal presents the `lease_id`, and `release` affects only the caller's own
  lease.
- **Retention.** Acknowledged messages, released or expired reservations, and
  ended registrations are deleted 30 days after they end. Unacknowledged
  messages do not expire. Each mailbox holds at most 10,000 of them.

The Pi extension attaches a session when a coordination URL, project, and
agent ID are supplied through extension flags or the
`PI_DUCKNNG_COORDINATION_URL`, `PI_DUCKNNG_COORDINATION_PROJECT`, and
`PI_DUCKNNG_AGENT_ID` environment variables. It registers the session,
heartbeats, and polls `receive` every 2 seconds. It injects each new message
with `pi.sendMessage(..., { triggerTurn: true, deliverAs: "steer" })`, appends
a session entry holding the message ID, and then acknowledges. The
acknowledgement proves only that the public call returned. A busy session
holds the message in its in-memory steering queue, and an idle session has
started a turn. Pi writes session entries to disk only after the session's
first assistant message. A crash between injection and `ack` therefore
redelivers the message, and reinjection is suppressed only if the recorded ID
survived. A host-owned `AgentHarness` adapter could provide a stronger
durable-lane acknowledgement without changing the wire contract.

The first deployment profile is one trusted user on one machine. Each endpoint
start binds a new ephemeral loopback TCP port and rewrites the locator file.
`register` accepts any project and agent ID, so a registration ID scopes calls
but does not authenticate the caller. NNG PUB/SUB is not part of the mailbox.
A later event channel may carry only lossy wake-up hints, and `receive`
remains the source of truth.

## Authorities and ownership

| Contract | Authority |
|---|---|
| Exact DuckDB, Node API, Pi, and ducknng versions | `DEPENDENCIES` |
| RPC frames, manifests, NNG, AIO, TLS, and codecs | hard-vendored ducknng |
| Native extension loading and calls from Node | DuckDB and `@duckdb/node-api` |
| R-side NNG endpoint | nanonext |
| Persistent R process and scheduling | mirai |
| R table/vector conversion | nanoarrow |
| Durable coordination tables and transactions | endpoint-owned DuckDB database |
| Pi steering and session-entry receipts | Pi public extension API |
| Tool projection, local endpoint lifecycle, and coordination polling | `pi-ducknng` |

Ducknng changes belong upstream and arrive here through the pinned subtree.
`pi-ducknng` must not duplicate its framing, transport, session, security, or
codec implementations.

## Invariants

- Endpoint placement is separate from generic discovery and invocation.
- A call must match a method in the fetched manifest.
- Complete ducknng responses are parsed before model-facing output is bounded.
- Only serialized model previews are capped; protocol bytes are not truncated.
- Requests to each REP endpoint are serialized, and one coordination endpoint
  is the sole owner of its DuckDB database and connection.
- A dead local endpoint invalidates its cached manifest and process record.
- Explicit close and error cleanup terminate owned processes before removing
  temporary files.
- Credential values do not enter tool schemas, manifests, SQL, argv, or results.
- Durable mailbox correctness does not depend on presence or notification delivery.
- An expired delivery lease makes the same message ID available for redelivery.
- A stale reservation lease cannot release a newer coordinator lease.
- Coordination state belongs to the independent endpoint, not a Pi session.

## Executable evidence

`README.qmd` runs an OpenAI Codex agent that discovers the R manifest,
persists an `mtcars` aggregate across fresh DuckDB clients, decodes both Arrow
tables, and closes the endpoint. Two precomputed pkgdown articles exercise
state persistence and an active binding in a selected environment.

`test/pi-extension.test.js` covers manifest discovery, declared-call
validation, process persistence, selected environments, active bindings, Arrow
IPC, request serialization, stale-process handling, and cleanup.
`test/coordination-endpoint.test.js` exercises register, send, receive, and
ack over the manifested NNG path. `test/coordination-adapter.test.js` covers
steering, receipt recording, reinjection suppression, and startup/shutdown
serialization against a stub client. `inst/tinytest/test-coordination.R`
covers offline mail, idempotent send, lease expiry and redelivery across a
store reopen, duplicate acknowledgement, presence, reservation conflicts and
fencing, retention, and the mailbox cap. `make check` runs the Node tests and
the README and vignette receipt checks. The tinytest suite runs through the
installed R package.

## Outside the current contract

Each of these requires its own producer, consumer, ownership rules, and
executable proof before it is added:

- structured R conditions, interruption, streaming, and attachment to the R
  endpoint by a second non-Pi client;
- model-facing coordination tools that use the adapter's registration without
  exposing its registration ID;
- authenticated identities, TLS, and cross-user or cross-host deployment;
- broadcast, dead-letter handling, and expiry of mail that is never received;
- PUB/SUB wake-up hints and a host-owned `AgentHarness` adapter.
