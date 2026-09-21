# pi-ducknng thesis

## Claim

`pi-ducknng` projects manifested ducknng RPC endpoints into Pi without adding a
Node NNG binding, a second wire protocol, or an endpoint-specific Pi tool for
every method. Its first placement adapter starts a persistent R endpoint; the
generic discovery and call tools also accept URLs supplied by other providers.

## Implemented contract

The Pi package exposes three tools:

- `persistent_r_start()` starts the local R adapter and returns its NNG URL;
- `ducknng_describe(url)` returns that endpoint's ducknng version-1 manifest;
- `ducknng_call(url, method, arguments)` rejects undeclared methods and invokes a
  declared method through a fresh DuckDB client.

The R endpoint currently declares `eval` and `close`. `eval` accepts R source
plus `envir` and `enclos` selectors. It evaluates in a mirai-owned persistent
environment and returns supported atomic vectors and data frames as nanoarrow
IPC. Unsupported R values fail explicitly. `close` stops the endpoint and
returns a JSON acknowledgement.

The implemented path is:

```text
Pi extension -> @duckdb/node-api -> DuckDB -> vendored ducknng
             -> NNG -> nanonext endpoint -> one mirai R process
```

Each describe or call operation opens and closes its own DuckDB instance. R
state belongs to the endpoint process, so it survives those clients.

## Coordination contract

`tools/pi-coordination-endpoint.R` is an independently started, project-scoped
coordination process. Its manifested methods are `register`, `heartbeat`,
`list_agents`, `send`, `receive`, `ack`, `reserve`, `release`, and `unregister`.
It does not expose `eval`.

The endpoint owns a DuckDB database containing presence leases, stable agent
mailboxes, message delivery leases, acknowledgements, idempotency records, and
advisory resource leases. `send` is idempotent in the sender/project scope.
`receive` provides bounded at-least-once delivery through visibility leases;
multiple live instances of one agent ID are competing consumers of that stable
mailbox. `ack` confirms only the delivery capability declared by the receiving
adapter.
Resource reservations use canonical opaque `resource:` identifiers or lexical
canonical `file:///` URIs and never inspect the referenced filesystem. Fencing
values increase across the whole project. Reacquisition after expiry or release
uses a new operation key while the old operation record is retained. The local
profile retains acknowledged messages and expired operation records for 30 days
and caps each mailbox at 10,000 pending messages.

The Pi extension can attach an interactive session when the coordination URL,
project, and stable agent ID are supplied through extension flags or
`PI_DUCKNNG_COORDINATION_*` environment variables. It records delivered message
IDs in Pi session entries, injects new messages with `deliverAs: "steer"`, and
acknowledges after the public injection call returns. This is API acceptance,
not exactly-once model execution. A host-owned `AgentHarness` can provide a
stronger durable-lane acknowledgement through a separate adapter; the
coordination wire contract does not depend on harness internals.

The first deployment profile is a single-user local machine. The endpoint
listens on loopback and does not claim cross-user or cross-host authorization.
NNG PUB/SUB is not part of the authoritative mailbox: any later event channel
may only provide lossy wake-up hints, with `receive` remaining the source of
truth.

## Authorities and ownership

| Contract | Authority |
|---|---|
| Exact DuckDB, Node API, and ducknng versions | `DEPENDENCIES` |
| RPC frames, manifests, NNG, AIO, TLS, and codecs | hard-vendored ducknng |
| Native extension loading and calls from Node | DuckDB and `@duckdb/node-api` |
| R-side NNG endpoint | nanonext |
| Persistent R process and scheduling | mirai |
| R table/vector conversion | nanoarrow |
| Durable coordination tables and transactions | endpoint-owned DuckDB database |
| Pi steering and session-entry receipts | Pi public extension API |
| Tool projection and local endpoint lifecycle | `pi-ducknng` |

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

`README.qmd` runs an OpenAI Codex agent that discovers the manifest, persists an
`mtcars` aggregate across fresh DuckDB clients, decodes both Arrow tables, and
closes the endpoint. The precomputed pkgdown articles independently exercise
state persistence and a selected environment with an active binding.

`test/pi-extension.test.js` covers manifest discovery, declared-call
validation, process persistence, selected environments, active bindings, Arrow
IPC, request serialization, stale-process handling, and cleanup.
`inst/tinytest/test-coordination.R` covers offline mail, idempotent send,
restart recovery, delivery lease expiry, duplicate acknowledgement, presence,
and reservation fencing. The Node coordination tests cover the manifested NNG
path and Pi steering adapter.

Structured R conditions, interruption, streaming, and attachment by a second
non-Pi client are not part of the current contract. Each requires its own
producer, consumer, ownership rules, and executable proof before being added.
