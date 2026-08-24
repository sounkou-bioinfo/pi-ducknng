# pi-duckdnng thesis

**Status:** working thesis for design discussion

## Thesis

Pi is already a client of an agent runtime. `pi-duckdnng` makes that client/runtime boundary addressable through hard-vendored ducknng without replacing Pi's TUI, inventing another orchestration system, or limiting the network to Pi processes.

Anything that speaks NNG can be reached. A ducknng-style manifest makes an endpoint's methods discoverable and importable; endpoint-specific semantics remain with the endpoint.

Pi can therefore be all of the following without changing substrate:

- a client of a persistent R process;
- a client of DuckDB, C, Python, shell, browser, or domain services;
- a server exposing its own agent/session operations;
- a peer of another Pi;
- a relay, supervisor, or worker in a larger topology.

The first and primary target is a persistent R session.

## Existing authority

`pi-duckdnng` starts from working systems rather than generic transport design:

- **Pi and pi-agent-core:** the TUI is already a client of the runtime. Current Pi extensions and SDK sessions are sufficient for an initial adapter. Harness v2 strengthens the contract with lanes, durable operations, recovery, and snapshot-plus-live observation; it is not a prerequisite.
- **ducknng:** NNG carriers, mbedTLS configuration and identity, framed RPC, manifests, sessions, AIO, cancellation, Arrow IPC, Quack, and native/browser transport distinctions are existing authority.
- **Rducks:** vendored NNG + mbedTLS, exact native artifact handling, and direct/Quack bridges provide packaging and interoperability precedent.
- **DuckDB API:** Pi reaches ducknng through DuckDB's supported host API and extension mechanism. It owns the native loading and call boundary; `pi-duckdnng` does not add a direct NNG binding or sidecar.

`pi-duckdnng` hard-vendors and reuses these authorities rather than reproducing them in TypeScript.

## Vendoring rule

The complete ducknng source tree is pinned under `vendor/ducknng` as a Git subtree. The subtree is implementation authority, not reference material. Transport, TLS, identity, framing, manifest, session, AIO, cancellation, and codec changes land in ducknng first and are then refreshed here. `pi-duckdnng` must not carry a divergent reimplementation.

[`DEPENDENCIES`](DEPENDENCIES) is the single version authority. The initial tuple pins DuckDB 1.5.4, `@duckdb/node-api` 1.5.4-r.1, and ducknng `v0.1.1-duckdb1.5.4` at its exact release commit. Tests and compatibility claims apply only to the pinned tuple.

DuckDB v2 and duckdb-quack are expected convergence paths that may remove version-specific compatibility and marshalling work. They will replace proven pieces when they provide real producer/consumer paths; v1 will not emulate hypothetical interfaces.

## Ownership by layer

| Layer | Semantic owner |
|---|---|
| Carrier, TLS, identity, AIO | NNG + mbedTLS using proven ducknng/Rducks patterns |
| Method discovery and framing | ducknng-style manifest and RPC contract |
| Domain state | The endpoint: R process, DuckDB service, Pi session, or another runtime |
| Live session ownership | The endpoint's session registry |
| Projection into Pi | `pi-duckdnng` |
| Pi-to-native host boundary | DuckDB API and DuckDB extension loading |
| Placement and lifecycle | A concrete local, container, VM, cluster, or hosted provider |

There must be no second transcript, scheduler, session store, TLS model, codec authority, or native NNG bridge inside `pi-duckdnng`.

## Native path

Pi does not call NNG through a custom native ABI:

```text
Pi extension -> DuckDB API -> DuckDB -> vendored ducknng -> NNG
```

DuckDB owns host-language integration and extension loading. Ducknng owns the network substrate. `pi-duckdnng` owns only their projection into Pi.

## Package shape

The repository is an R package built primarily by composing `nanonext` and `mirai`. `nanonext` owns R-side NNG and AIO; `mirai` owns persistent R processes and distributed evaluation. The package must not add another socket binding, evaluator scheduler, or process topology.

## Persistent R as the first proof

An R worker owns a real R process independently of any Pi client. Its session preserves the workspace, loaded packages, options, working directory, RNG state, valid connections, graphics state, and pending work until the owner closes or expires it.

The first R method family should remain concrete:

- open a session;
- evaluate code;
- inspect session state;
- interrupt an evaluation;
- close a session.

Reattachment uses ducknng's existing session identity and ownership contract; it is not a separate R method. Evaluation scheduling and scale-out remain mirai responsibilities.

Evaluation is not merely terminal text. It can emit structured stdout, messages, warnings, errors with calls or tracebacks, visible or invisible values, plots, and completion. Objects remain in R unless explicitly transferred. Data frames can use Arrow IPC or Quack where negotiated.

R is the first-class design target, not an excuse to invent a universal `runtime_type` interface. Shell, Python, and other REPL adapters can follow once a genuinely repeated lifecycle exists.

## Reachability and meaning

NNG supplies reachability. Compatible patterns, framing, and manifests supply meaning.

Pi needs one stable discovery tool: `duckdnng_describe`. It returns the manifest methods and their descriptions. The existing DuckDB API surface executes those methods; `pi-duckdnng` does not add call, receive, cancel, or per-method dynamic tool wrappers.

Keeping the discovery tool and its schema stable preserves provider prompt-cache prefixes. An extension may enrich static tool descriptions for presentation, but that is not a new invocation layer.

The reverse direction is equally important: Pi may expose prompt, steering, cancellation, state, and event methods through ducknng. Pi is a node in the network, not a privileged controller above it.

A central broker is optional. Registry, relay, peer, supervisor/worker, and direct attach are topology choices over the same method and session contracts.

## Profiles and credential injection

Requests carry non-secret profile identifiers, never credential values. A trusted host resolves a profile only at the final consumer boundary, checks the verified peer/session subject and target scope, obtains approval when policy requires it, and injects the secret without returning it to Pi, SQL, argv, manifests, or tool results.

The default extension path receives no ambient credentials. Network or compute capability is explicitly composed by the host. Logs and durable evidence contain only redacted profile receipts or digests, the approved subject and target, and the outcome.

Browser/computer-use injection follows the same rule: a trusted browser adapter injects into an approved origin and field. The adapter must also prevent secret read-back through DOM, script, screenshot, or subsequent tool observations; password-field masking alone is not a security boundary.

## Orbs are placement

An orb-like product is an endpoint plus placement and lifecycle operations:

- create;
- start or wake;
- attach;
- execute;
- sleep;
- destroy.

The live service remains an NNG endpoint. Local processes, containers, remote machines, or hosted environments are provider choices. They do not require a new agent abstraction.

An orb or R worker may produce receipts, artifacts, and observations, but that durable evidence layer is outside `pi-duckdnng`.

## Current Pi and Harness v2

Both are supported. Current `AgentSession` and Harness v2 `AgentLane` adapters expose the same ducknng-facing semantics. DuckDB can project current JSONL and Harness v2 SQLite-backed state through its readers and SQLite extension, while live actions remain runtime adapters.

For multi-agent work, storage-indirect coordination is preferred where possible: agents coordinate through durable relations, manifests, receipts, and observations, with NNG providing reachability and wake-up rather than forcing every interaction into a direct agent call.

## Prior art, not dependencies

Pi-bio-agent demonstrates useful profile-injection and storage-indirect coordination patterns. `pi-duckdnng` may adapt those ideas, but it does not vendor, import, require, or delegate semantic ownership to pi-bio-agent.

## Non-goals

The initial project will not:

- replace Pi's existing TUI;
- design a new TLS, identity, session, AIO, or codec stack;
- build a separate Node native binding or sidecar for NNG;
- dynamically register one Pi tool for every discovered method;
- expose raw credentials to Pi, R code, SQL, argv, manifests, logs, or receipts;
- vendor or depend on pi-bio-agent;
- use transcript files as a wire protocol;
- require a central orchestrator;
- require containers or hosted compute;
- begin with a universal REPL abstraction;
- make pi-bio-agent a process supervisor;
- claim that native execution is sandboxed merely because it is remote.

## First executable proof

The smallest proof should demonstrate one semantic claim: a persistent R session is independent of a Pi client.

1. Start one R worker on a local NNG endpoint.
2. Load `pi-duckdnng` into the ordinary Pi TUI.
3. Open an R session and evaluate `x <- 41`.
4. Disconnect and reconnect the Pi client.
5. Evaluate `x + 1` in the same R session and receive `42`.
6. Attach a second non-Pi NNG client to the same session.
7. Stream structured conditions and interrupt one running evaluation.

This proves persistence, reachability, interoperability, and cancellation without first building an orb product or multi-agent framework.

## Settled initial design

- The repository is an R package composing nanonext and mirai.
- DuckDB 1.5.4, its exact DuckDB API package, and its matching ducknng release are pinned as one tuple.
- `duckdnng_describe` is the only new stable Pi discovery tool; execution stays on the DuckDB API surface.
- Mirai owns R process topology and scheduling; nanonext owns R-side NNG and AIO.
- Both current Pi and Harness v2 are adapters over the same ducknng-facing semantics.
- Profiles and credential values are resolved and injected by the trusted host at the final consumer boundary.
