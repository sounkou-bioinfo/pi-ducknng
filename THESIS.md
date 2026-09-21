# pi-ducknng thesis

## Claim

`pi-ducknng` projects manifested ducknng RPC endpoints into Pi without adding a
Node NNG binding, a second wire protocol, or an endpoint-specific Pi tool for
every method. Endpoint placement is separate from discovery and invocation.
The package places a persistent R endpoint on demand. A separately started
coordination endpoint gives Pi sessions durable mail and advisory
reservations. The generic tools accept any compatible URL.

## Generic Pi tools

The Pi package exposes three generic model tools:

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
The generic tools refuse a URL that the coordination adapter has claimed.

## R endpoint

`tools/pi-r-endpoint.R` declares `eval` and `close`. `eval` accepts R source
plus `envir` and `enclos` expressions and evaluates in a mirai-owned
persistent environment. Supported atomic vectors and data frames return as
nanoarrow IPC. Other R values fail explicitly. `close` stops the endpoint and
returns a JSON acknowledgement. The Pi extension passes its process ID in
`PI_DUCKNNG_PARENT_PID`. The endpoint checks that process once a second and
exits after it disappears, so idle time never discards the R environment.

## Coordination endpoint

`tools/pi-coordination-endpoint.R` runs independently of any Pi session and is
the sole owner of one DuckDB database file. It declares `register`,
`heartbeat`, `list_agents`, `send`, `receive`, `ack`, `reserve`,
`list_reservations`, `release`, and `unregister`. It does not declare `eval`,
and clients reach its tables only through those methods.

By default the endpoint listens on `ipc://` beside its database, so the
address stays the same across restarts. It serves 64 NNG REP contexts. A
`receive` with `wait_ms` holds one context until mail arrives or the wait
ends, while other requests proceed on the remaining contexts. The serve loop
retries parked receives after each dispatched request, which is when new mail
can have been committed. Payloads larger than a method's manifested
`max_request_bytes` are rejected. Failures travel as `<code>: <detail>` error
text, and the manifest lists the codes.

- **Identity.** A mailbox belongs to a stable `(project_id, agent_id)`.
  `register` binds an instance, such as a Pi session ID, to that mailbox and
  returns an opaque `registration_id` for later calls. Presence is an
  unexpired server-side lease renewed by `heartbeat`. Live instances of one
  agent ID are competing consumers of its mailbox.
- **Delivery.** `send` commits the message before replying. Content is UTF-8
  text of at most 64 KiB and may contain tabs and line breaks. It is
  idempotent for each project, sender, and `idempotency_key`; reusing a key
  for different content fails. The reply reports whether the recipient has
  ever registered. `receive` leases up to 32 queued messages in mailbox
  sequence order for a visibility timeout. When a lease expires, the message
  returns to the queue with the same message ID. `ack` accepts only the
  current receipt, is idempotent, and records the delivery capability the
  receiving instance declared at registration. Delivery is at least once.
  Model execution is not exactly once.
- **Expiry.** A message not received before its `ttl_ms`, 7 days by default,
  becomes a dead letter. Acknowledged messages, dead letters, released or
  expired reservations, and ended registrations are deleted 30 days after
  they end. Each mailbox holds at most 10,000 unacknowledged messages.
- **Reservations.** `reserve` takes an advisory lease on a canonical
  `resource:` identifier or a lexically canonical, percent-encoded `file:///`
  URI. A file URI conflicts with its ancestors and descendants. The endpoint
  never inspects the referenced filesystem. Each acquisition draws a fencing
  value from a project-wide counter. Retrying with the same `operation_key`
  replays the original result. Reacquiring after release or expiry requires a
  new key. Renewal presents the `lease_id`, and `release` affects only the
  caller's own lease. `list_reservations` returns lease IDs only to their
  owner.

## Pi coordination adapter

The Pi extension attaches a session when a coordination URL, project, and
agent ID are supplied through extension flags or the
`PI_DUCKNNG_COORDINATION_URL`, `PI_DUCKNNG_COORDINATION_PROJECT`, and
`PI_DUCKNNG_AGENT_ID` environment variables. It claims the URL, registers the
session, and heartbeats. The registration ID stays in the adapter and never
enters model context. After a `registration_expired` reply the adapter
registers again and retries the call once.

In the `tui` and `rpc` modes the adapter waits on `receive` in the background.
It injects each new message with
`pi.sendMessage(..., { triggerTurn: true, deliverAs: "steer" })`, appends a
session entry holding the message ID, and then acknowledges. In the one-shot
`print` and `json` modes it does not steer, and the model pulls mail with
`coordination_inbox`. Either path frames the text in a `<coordination_message>`
envelope that names the sender and message ID and states that the text is not
from the user.

The acknowledgement proves only that the public call returned. A busy session
holds a steered message in its in-memory steering queue, and an idle session
has started a turn. Pi writes session entries to disk only after the
session's first assistant message. A crash between injection and `ack`
therefore redelivers the message, and reinjection is suppressed only if the
recorded ID survived.

The adapter gives the model five tools:

- `coordination_send` sends with an idempotency key derived from the session
  and tool call IDs, so a retried tool call cannot duplicate mail;
- `coordination_inbox` receives, optionally waiting, and acknowledges;
- `coordination_agents` lists live agents and active reservations;
- `coordination_reserve` resolves a path against the session's working
  directory. The adapter renews each held lease until it is released or the
  session shuts down;
- `coordination_release` releases a lease by ID or resource.

While a session is attached, its `edit` and `write` tool calls are blocked
when another instance holds a conflicting reservation. This gate covers only
Pi's own file tools, not writes made through `bash`. It lets the call through
when the endpoint is unreachable.

The first deployment profile is one user on one machine. The default `ipc://`
socket is created with a 0077 umask inside the directory that holds the
database, so filesystem permissions decide who can connect. `register`
accepts any project and agent ID, so a registration ID scopes calls but does
not authenticate the caller. NNG PUB/SUB is not part of the mailbox. The
waiting `receive` already provides prompt delivery, and `receive` remains the
source of truth.

## Authorities and ownership

| Contract | Authority |
|---|---|
| Exact DuckDB, Node API, Pi, and ducknng versions | `DEPENDENCIES` |
| RPC frames, manifests, NNG, AIO, TLS, and codecs | hard-vendored ducknng |
| Native extension loading and calls from Node | DuckDB and `@duckdb/node-api` |
| R-side NNG endpoint and REP contexts | nanonext |
| Persistent R process and scheduling | mirai |
| R table/vector conversion | nanoarrow |
| Durable coordination tables and transactions | endpoint-owned DuckDB database |
| Pi steering, tool calls, and session-entry receipts | Pi public extension API |
| Tool projection, local endpoint lifecycle, and the coordination adapter | `pi-ducknng` |

Ducknng changes belong upstream and arrive here through the pinned subtree.
`pi-ducknng` must not duplicate its framing, transport, session, security, or
codec implementations.

## Invariants

- Endpoint placement is separate from generic discovery and invocation.
- A call must match a method in the fetched manifest.
- Complete ducknng responses are parsed before model-facing output is bounded.
- Only serialized model previews are capped; protocol bytes are not truncated.
- Model calls to one URL are serialized. A coordination request that waits
  holds one NNG context and does not block other requests.
- One coordination endpoint is the sole owner of its DuckDB database.
- An endpoint placed by Pi does not outlive the process that placed it.
- A dead local endpoint invalidates its cached manifest and process record.
- Explicit close and error cleanup terminate owned processes before removing
  temporary files.
- Credential values and registration IDs do not enter tool schemas, SQL, argv,
  or model-facing results.
- Durable mailbox correctness does not depend on presence or notification delivery.
- An expired delivery lease makes the same message ID available for redelivery.
- A stale reservation lease cannot release a newer coordinator lease.
- Coordination state belongs to the independent endpoint, not a Pi session.

## Executable evidence

`README.qmd` runs an OpenAI Codex agent that discovers the R manifest,
persists an `mtcars` aggregate across fresh DuckDB clients, decodes both Arrow
tables, and closes the endpoint. Two precomputed pkgdown articles exercise
state persistence and an active binding in a selected environment.

`test/pi-extension.test.js` covers the following:

- manifest discovery and declared-call validation;
- process persistence, selected environments, and active bindings;
- Arrow IPC and request serialization;
- stale-process handling and cleanup;
- an R endpoint that outlives idle polls and exits after its parent.

`test/coordination-endpoint.test.js` runs two extension sessions against a
real endpoint. It checks:

- URL ownership;
- prompt delivery through a waiting receive;
- multi-line content;
- unseen-recipient reporting;
- reservation-gated edits;
- a restart on the same `ipc://` address with mail sent afterwards;
- a pulled reply.

`test/coordination-adapter.test.js` uses a stub client. It covers:

- envelopes and background steering;
- one-shot pull mode;
- re-registration;
- idempotency keys and the edit gate;
- lease release at shutdown;
- tool deactivation and reinjection suppression.

`inst/tinytest/test-coordination.R` covers:

- offline mail and idempotent send;
- lease expiry and redelivery across a store reopen;
- duplicate acknowledgement and presence;
- reservation conflicts, fencing, and percent-encoding;
- reservation listing and dead letters;
- multi-line text and error codes;
- retention and the mailbox cap.

`make check` runs the README and vignette receipt checks, the Node tests, and
the tinytest suite against a temporary installation.

## Outside the current contract

Each of these requires its own producer, consumer, ownership rules, and
executable proof before it is added:

- structured R conditions, interruption, streaming, and attachment to the R
  endpoint by a second non-Pi client;
- authenticated identities, TLS, and cross-user or cross-host deployment;
- broadcast delivery to several mailboxes;
- a host-owned `AgentHarness` adapter with a durable-lane acknowledgement.
