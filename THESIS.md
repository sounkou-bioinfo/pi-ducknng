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

## Authorities and ownership

| Contract | Authority |
|---|---|
| Exact DuckDB, Node API, and ducknng versions | `DEPENDENCIES` |
| RPC frames, manifests, NNG, AIO, TLS, and codecs | hard-vendored ducknng |
| Native extension loading and calls from Node | DuckDB and `@duckdb/node-api` |
| R-side NNG endpoint | nanonext |
| Persistent R process and scheduling | mirai |
| R table/vector conversion | nanoarrow |
| Tool projection and local endpoint lifecycle | `pi-ducknng` |

Ducknng changes belong upstream and arrive here through the pinned subtree.
`pi-ducknng` must not duplicate its framing, transport, session, security, or
codec implementations.

## Invariants

- Endpoint placement is separate from generic discovery and invocation.
- A call must match a method in the fetched manifest.
- Complete ducknng responses are parsed before model-facing output is bounded.
- Only serialized model previews are capped; protocol bytes are not truncated.
- Requests to the local REP endpoint are serialized.
- A dead local endpoint invalidates its cached manifest and process record.
- Explicit close and error cleanup terminate owned processes before removing
  temporary files.
- Credential values do not enter tool schemas, manifests, SQL, argv, or results.

## Executable evidence

`README.qmd` runs an OpenAI Codex agent that discovers the manifest, persists an
`mtcars` aggregate across fresh DuckDB clients, decodes both Arrow tables, and
closes the endpoint. The precomputed pkgdown articles independently exercise
state persistence and a selected environment with an active binding.

`test/pi-extension.test.js` covers manifest discovery, declared-call
validation, process persistence, selected environments, active bindings, Arrow
IPC, request serialization, stale-process handling, and cleanup.

Structured R conditions, interruption, streaming, and attachment by a second
non-Pi client are not part of the current contract. Each requires its own
producer, consumer, ownership rules, and executable proof before being added.
