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
- **pi-bio-agent:** resource manifests, receipts, CAS, observations, and the ledger remain the durable evidence plane. They are not the live runtime owner.

`pi-duckdnng` hard-vendors and reuses these authorities rather than reproducing them in TypeScript.

## Vendoring rule

The complete ducknng source tree is pinned under `vendor/ducknng` as a Git subtree. The subtree is implementation authority, not reference material. Transport, TLS, identity, framing, manifest, session, AIO, cancellation, and codec changes land in ducknng first and are then refreshed here. `pi-duckdnng` must not carry a divergent reimplementation.

## Ownership by layer

| Layer | Semantic owner |
|---|---|
| Carrier, TLS, identity, AIO | NNG + mbedTLS using proven ducknng/Rducks patterns |
| Method discovery and framing | ducknng-style manifest and RPC contract |
| Domain state | The endpoint: R process, DuckDB service, Pi session, or another runtime |
| Live session ownership | The endpoint's session registry |
| Projection into Pi | `pi-duckdnng` |
| Pi-to-native host boundary | DuckDB API and DuckDB extension loading |
| Durable resources and evidence | pi-bio-agent when used |
| Placement and lifecycle | A concrete local, container, VM, cluster, or hosted provider |

There must be no second transcript, scheduler, session store, TLS model, codec authority, or native NNG bridge inside `pi-duckdnng`.

## Native path

Pi does not call NNG through a custom native ABI:

```text
Pi extension -> DuckDB API -> DuckDB -> vendored ducknng -> NNG
```

DuckDB owns host-language integration and extension loading. Ducknng owns the network substrate. `pi-duckdnng` owns only their projection into Pi.

## Persistent R as the first proof

An R worker owns a real R process independently of any Pi client. Its session preserves the workspace, loaded packages, options, working directory, RNG state, valid connections, graphics state, and pending work until the owner closes or expires it.

The first R method family should remain concrete:

- open a session;
- evaluate code;
- inspect session state;
- interrupt an evaluation;
- attach to an existing session;
- close a session.

Evaluation is not merely terminal text. It can emit structured stdout, messages, warnings, errors with calls or tracebacks, visible or invisible values, plots, and completion. Objects remain in R unless explicitly transferred. Data frames can use Arrow IPC or Quack where negotiated.

R is the first-class design target, not an excuse to invent a universal `runtime_type` interface. Shell, Python, and other REPL adapters can follow once a genuinely repeated lifecycle exists.

## Reachability and meaning

NNG supplies reachability. Compatible patterns, framing, and manifests supply meaning.

A connected endpoint may be used explicitly, or its manifest methods may be projected into Pi as namespaced tools such as:

```text
lab.r.eval
cluster.duckdb.query
worker.pi.prompt
native.tinycc.compile
```

The reverse direction is equally important: Pi may expose prompt, steering, cancellation, state, and event methods. Pi is a node in the network, not a privileged controller above it.

A central broker is optional. Registry, relay, peer, supervisor/worker, and direct attach are topology choices over the same method and session contracts.

## Orbs are placement

An orb-like product is an endpoint plus placement and lifecycle operations:

- create;
- start or wake;
- attach;
- execute;
- sleep;
- destroy.

The live service remains an NNG endpoint. Local processes, containers, remote machines, or hosted environments are provider choices. They do not require a new agent abstraction.

This distinction also keeps pi-bio-agent focused: an orb or R worker produces receipts, artifacts, and observations for its ledger, while `pi-duckdnng` owns the live connection and session.

## Non-goals

The initial project will not:

- replace Pi's existing TUI;
- design a new TLS, identity, session, AIO, or codec stack;
- build a separate Node native binding or sidecar for NNG;
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

## Decisions to discuss before implementation

1. **Repository boundary:** does `pi-duckdnng` contain both the Pi adapter and the first R worker, or does the R worker live in a separate R package from the start?
2. **Tool projection:** should discovered methods become dynamic first-class Pi tools, remain behind one explicit call tool, or support both modes?
3. **R concurrency:** is one evaluator the sole writer with multiple observing clients, or can ownership be leased between clients?
4. **Pi exposure:** which current AgentSession operations form the first server manifest, and which should wait for Harness v2 lanes?
