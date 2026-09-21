# pi-ducknng thesis

## Claim

`pi-ducknng` projects manifested ducknng RPC endpoints into Pi without adding a
Node NNG binding, a second wire protocol, or an endpoint-specific Pi tool for
every method. Endpoint placement is separate from discovery and invocation.
The package places a persistent R endpoint on demand. A separately started
coordination endpoint, whose methods are SQL served by ducknng itself, gives
Pi sessions durable mail and advisory reservations. The generic tools accept
any compatible URL.

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

For `tls+tcp://` and `wss://` URLs, each fresh client builds a ducknng TLS
configuration. `PI_DUCKNNG_TLS_CA_FILE` is required and verifies the server.
`PI_DUCKNNG_TLS_CERT_KEY_FILE` optionally names a combined certificate and key
PEM file that authenticates the client for mutual TLS. The file paths are bound
as SQL parameters. Neither the paths nor the key material enter tool schemas
or results.

## R endpoint

`tools/pi-r-endpoint.R` declares `eval` and `close`. `eval` accepts R source
plus `envir` and `enclos` expressions and evaluates in a mirai-owned
persistent environment. Supported atomic vectors and data frames return as
nanoarrow IPC. Other R values fail explicitly. `close` stops the endpoint and
returns a JSON acknowledgement. The Pi extension passes its process ID in
`PI_DUCKNNG_PARENT_PID`. The endpoint checks that process once a second and
exits after it disappears, so idle time never discards the R environment.

## Coordination endpoint

The coordination endpoint is a DuckDB database served by ducknng. Its methods
are ducknng SQL-defined methods whose handler SQL lives in
`coordination/methods/*.sql`; `coordination/schema.sql` defines the tables and
the validation macros, and `coordination/methods.json` supplies each method's
manifest request schema. That SQL is the whole implementation. The host,
`tools/pi-coordination-endpoint.ts` started with Node, owns only the database
file, the listener, and the contents of the grant table. It runs independently
of any Pi session and is the sole owner of its database. The manifest declares
`register`, `heartbeat`, `list_agents`, `send`, `receive`, `ack`, `reserve`,
`list_reservations`, `release`, and `unregister`, and no evaluation method.
Clients reach the tables only through those methods.

Each method call runs as one transaction on the service's request connection,
with the caller's JSON object bound as the handler's parameter. A failure
rolls the transaction back and returns a ducknng error whose text carries
`<code>: <detail>`. The codes are `invalid_argument`, `registration_expired`,
`unauthorized`, `idempotency_conflict`, `mailbox_full`, `receipt_invalid`,
`lease_invalid`, and `resource_conflict`. Write methods first run a
maintenance step at most once a second. It returns expired delivery leases to
the queue, turns overdue mail into dead letters, and applies retention.
`receive` replies at once, and callers poll.

- **Identity.** A mailbox belongs to a stable `(project_id, agent_id)`.
  `register` binds an instance, such as a Pi session ID, to that mailbox and
  returns an opaque `registration_id` for later calls. The registration
  records the caller's verified peer identity, and every later call with that
  ID must come from the same identity. Presence is an
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

In the `tui` and `rpc` modes the adapter polls `receive` in the background,
once a second while the mailbox is empty. It injects each new message with
`pi.sendMessage(..., { triggerTurn: true, deliverAs: "steer" })`, appends a
session entry holding the message ID, and then acknowledges. In the one-shot
`print` and `json` modes it does not steer, and the model pulls mail with
`coordination_inbox`, which polls until mail arrives or its `wait_ms` ends.
Either path frames the text in a `<coordination_message>` envelope that names
the sender and message ID and states that the text is not from the user.

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

## AgentHarness adapter

A host process that owns a `@earendil-works/pi-agent-core` `AgentHarness`
attaches one lane to one mailbox with `attachCoordinationLane` from
`extensions/pi-ducknng/harness.ts`. It uses `ducknngCoordinationClient` from
the extension module. The adapter registers with `adapter_kind` set to
`agent_harness` and a delivery capability of `harness_lane_commit`. It polls
`receive` and first looks for a lane entry that already carries the
message ID, in the lane's queues or its transcript. Only if none exists does
it admit the envelope, with `steer` while an operation runs and with `nextRun`
otherwise. It acknowledges with the lane entry ID as `delivery_ref`. A lost
acknowledgement therefore leads to redelivery and reconciliation, never to a
second lane entry. The commit is durable only when the harness session uses a
durable `SessionRepo` such as `JsonlSessionRepo`. The adapter never calls
`drive`, and the host keeps ownership of runs, operation recovery, and the
model runtime.

## Deployment profiles

The local profile is the default. The endpoint listens on `ipc://` beside its
database, so the address survives restarts. The socket is created with a 0077
umask inside the database's directory, so filesystem permissions decide who
can connect. Callers carry no verified identity, and `register` accepts any
project and agent ID.

The mutual-TLS profile serves `tls+tcp://` with a listener certificate from
`PI_DUCKNNG_COORDINATION_TLS_CERT_KEY_FILE` and client verification against
`PI_DUCKNNG_COORDINATION_TLS_CA_FILE`. Every method then requires a verified
peer identity. `PI_DUCKNNG_COORDINATION_GRANTS_FILE` lists which peer
identities may register as which `(project_id, agent_id)`, and agent `*`
grants a whole project. A caller without a matching grant is refused. A
registration ID is useless to any other identity, and an instance stays bound
to the identity that registered it. Clients present their certificates through
`PI_DUCKNNG_TLS_CERT_KEY_FILE`. The same endpoint therefore serves agents on
several hosts and users, with project isolation decided by certificates and
grants.

NNG PUB/SUB is not part of the mailbox, and `receive` remains the source of
truth.

## Authorities and ownership

| Contract | Authority |
|---|---|
| Exact DuckDB, Node API, Pi, and ducknng versions | `DEPENDENCIES` |
| RPC frames, manifests, NNG, AIO, TLS, and codecs | hard-vendored ducknng |
| Native extension loading and calls from Node | DuckDB and `@duckdb/node-api` |
| R-side NNG endpoint | nanonext |
| Persistent R process and scheduling | mirai |
| R table/vector conversion | nanoarrow |
| Coordination method semantics | `coordination/*.sql` |
| Coordination serving, admission, and peer identity | ducknng SQL-defined methods |
| Durable coordination tables and transactions | endpoint-owned DuckDB database |
| Pi steering, tool calls, and session-entry receipts | Pi public extension API |
| Harness lanes, runs, and operation recovery | host process that owns the `AgentHarness` |
| Tool projection, local endpoint lifecycle, and the coordination adapter | `pi-ducknng` |

Ducknng changes belong upstream and arrive here through the pinned subtree.
`pi-ducknng` must not duplicate its framing, transport, session, security, or
codec implementations.

## Invariants

- Endpoint placement is separate from generic discovery and invocation.
- A call must match a method in the fetched manifest.
- Complete ducknng responses are parsed before model-facing output is bounded.
- Only serialized model previews are capped; protocol bytes are not truncated.
- Model calls to one URL are serialized. Adapter polling bypasses that order
  and never delays a model tool call.
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
- A registration is usable only by the peer identity that created it.
- Coordination state belongs to the independent endpoint, not a Pi session.

## Executable evidence

`README.qmd` runs an OpenAI Codex agent that discovers the R manifest,
persists an `mtcars` aggregate across fresh DuckDB clients, decodes both Arrow
tables, and closes the endpoint. It then runs two Codex agents through a
coordination endpoint and checks their round trip through the endpoint's own
methods. Two precomputed pkgdown articles exercise
state persistence and an active binding in a selected environment.

`test/pi-extension.test.js` covers the following:

- manifest discovery and declared-call validation;
- process persistence, selected environments, and active bindings;
- Arrow IPC and request serialization;
- stale-process handling and cleanup;
- an R endpoint that outlives idle polls and exits after its parent.

`test/pi-extension-tls.test.js` issues a throwaway CA, server certificate, and
client certificate. It checks that the generic tools reach a mutual-TLS
ducknng listener only with the injected client certificate, and that a SQL
method called over that connection sees the client's verified peer identity.

`test/coordination-endpoint.test.js` runs two extension sessions against an
endpoint started through the Node host. It checks:

- URL ownership;
- delivery within the poll interval;
- multi-line content;
- unseen-recipient reporting;
- reservation-gated edits;
- a restart on the same `ipc://` address with mail sent afterwards;
- a pulled reply.

`test/coordination-adapter.test.js` uses a stub client. It covers:

- envelopes and background steering;
- one-shot pull mode;
- re-registration and error codes read through the ducknng error wrapper;
- idempotency keys and the edit gate;
- lease release at shutdown;
- tool deactivation and reinjection suppression.

`test/coordination-harness.test.js` attaches a real `AgentHarness` lane on a
`JsonlSessionRepo` to a real endpoint. It drops an acknowledgement after the
lane commit and checks the following:

- redelivery reuses the existing lane entry;
- the committed entries survive reopening the session;
- the endpoint records both messages as acknowledged.

`test/coordination-store.test.js` drives the SQL methods over ducknng with a
fixed server clock. It covers:

- offline mail and idempotent send;
- lease expiry and redelivery across an endpoint restart;
- duplicate acknowledgement and presence;
- reservation conflicts, fencing, renewal, and percent-encoding;
- reservation listing, dead letters, retention, and the mailbox cap;
- multi-line text and error codes.

Over mutual TLS it also shows the following:

- an ungranted certificate cannot register;
- a certificate cannot register an agent or project it was not granted;
- another certificate cannot use a registration ID;
- an instance cannot be taken over under a shared agent grant.

`make check` runs the README and vignette receipt checks, the Node tests, and
the R package smoke tests against a temporary installation.

## Outside the current contract

Each of these requires its own producer, consumer, ownership rules, and
executable proof before it is added:

- structured R conditions, interruption, streaming, and attachment to the R
  endpoint by a second non-Pi client;
- server-side waiting for `receive`, which needs deferred replies in ducknng;
- broadcast delivery to several mailboxes.
