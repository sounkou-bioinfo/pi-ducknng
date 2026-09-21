

# pi-ducknng

[![](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)

`pi-ducknng` lets Pi agents discover and call manifested ducknng
endpoints. It ships two endpoints: a persistent R session and a durable
coordination service for Pi sessions.

## Architecture

![Generic ducknng endpoint architecture](man/figures/architecture.svg)

An endpoint is placed first and supplies a URL. Discovery and calls stay
generic. DuckDB owns native extension loading and host-language calls.
The hard-vendored ducknng release owns transport, mbedTLS, identity,
framing, manifests, sessions, AIO, cancellation, and codecs. The R
endpoints compose `nanonext` and `mirai` and do not reproduce those
layers. `make architecture` regenerates the SVG from
`man/figures/architecture.mmd`.

## Pi package

Install the repository as a Pi package:

``` sh
pi install git:github.com/sounkou-bioinfo/pi-ducknng
```

The package contributes three generic model tools:

- `persistent_r_start` places the R endpoint;
- `ducknng_describe` reads any compatible endpoint manifest;
- `ducknng_call` invokes a declared method.

A session attached to a coordination endpoint also gets the coordination
tools described below.

On first use the package builds the pinned ducknng source, which
requires Git, Make, CMake, Python, and a C/C++ toolchain. The R runtime
must provide the packages listed under `Imports` in
[`DESCRIPTION`](DESCRIPTION).

## Persistent R

The evaluated cell below loads the package extension into an OpenAI
Codex agent. The model discovers the endpoint’s methods and request
examples, then runs an `mtcars` analysis whose intermediate table
survives across fresh DuckDB clients.

``` sh
'/root/pi-ducknng/node_modules/.bin/pi' --provider 'openai-codex' --model 'gpt-6-astra' --no-extensions --thinking 'medium' -e './extensions/pi-ducknng/index.ts' --no-session -p \
  "$(printf %s \
    'Using the package tools, start the R adapter and follow ' \
    'its ducknng manifest. In one eval call, create and ' \
    'return `mpg_by_cyl <- aggregate(mpg ~ cyl, data = ' \
    'datasets::mtcars, FUN = mean)`. In a separate eval call, ' \
    'use the persisted `mpg_by_cyl` to return ' \
    '`transform(mpg_by_cyl, delta_from_4cyl = mpg - mpg[cyl ' \
    '== 4])`. Close the endpoint. Report the manifested ' \
    'methods and decoded rows, beginning with ' \
    '`AGENT_DUCKNNG_MANIFEST_CALL_OK` only when both calls ' \
    'used the same endpoint process and succeeded.')"
```

> AGENT_DUCKNNG_MANIFEST_CALL_OK
>
> Manifested methods: `eval` (JSON → Arrow, persistent R process),
> `close` (JSON → JSON).
>
> Both eval calls succeeded in endpoint process **320648**. Endpoint
> closed successfully.
>
> First call decoded rows:
>
> | cyl |                mpg |
> |----:|-------------------:|
> |   4 | 26.663636363636364 |
> |   6 | 19.742857142857144 |
> |   8 |               15.1 |
>
> Second call, using persisted `mpg_by_cyl`:
>
> | cyl |                mpg |     delta_from_4cyl |
> |----:|-------------------:|--------------------:|
> |   4 | 26.663636363636364 |                   0 |
> |   6 | 19.742857142857144 |   -6.92077922077922 |
> |   8 |               15.1 | -11.563636363636364 |

Each tool request opens and closes a fresh DuckDB instance. Eval results
travel as Arrow IPC streams written by nanoarrow and decoded by ducknng.
The mirai-owned R environment stays in the endpoint process until
`close` or until the Pi process that started it exits.

The README is rendered by `piknit` with the Pi version pinned in
`DEPENDENCIES`. `make readme` rejects output that lacks any agent’s
success receipt.

## Durable agent coordination

Start the coordination endpoint independently of any Pi session, with
its DuckDB state in a private directory. It listens on `ipc://` beside
the database, so the address survives restarts and only the directory’s
owner can connect:

``` sh
install -d -m 700 "$HOME/.local/state/pi-ducknng"
Rscript --vanilla tools/pi-coordination-endpoint.R \
  "$HOME/.local/state/pi-ducknng/coordination.url" \
  "$HOME/.local/state/pi-ducknng/coordination.duckdb"
```

Attach a Pi session in another terminal:

``` sh
export PI_DUCKNNG_COORDINATION_URL="$(cat \
  "$HOME/.local/state/pi-ducknng/coordination.url")"
export PI_DUCKNNG_COORDINATION_PROJECT="my-project"
export PI_DUCKNNG_AGENT_ID="reviewer"
pi
```

The attached session gets five tools: `coordination_send`,
`coordination_inbox`, `coordination_agents`, `coordination_reserve`, and
`coordination_release`. The registration behind them never enters the
model’s context.

- **Delivery.** An interactive session waits on its mailbox in the
  background and steers each message into the conversation. A one-shot
  `pi -p` run pulls mail with `coordination_inbox`. Every message
  arrives framed with its sender and message ID and is marked as coming
  from another agent, not the user.
- **Durability.** `send` stores mail before replying, so mail survives
  disconnected sessions and endpoint restarts. Delivery is at least
  once, and work done in response is not exactly once. Mail nobody
  receives becomes a dead letter after 7 days, and `send` reports
  recipients that have never registered.
- **Reservations.** Leases cover `resource:` identifiers or paths and
  carry a fencing value that increases across the project. The session
  renews each lease it holds until release or shutdown. While another
  session holds a path, Pi’s `edit` and `write` tools refuse to touch
  it.

This first endpoint is for one user on one machine and has no
authentication beyond filesystem permissions.

A host process that owns a `pi-agent-core` `AgentHarness` can attach a
lane to a mailbox. Each message is committed to the lane before it is
acknowledged, and a redelivered message is matched to its existing lane
entry by message ID:

``` js
import { attachCoordinationLane } from "pi-ducknng/extensions/pi-ducknng/harness.ts";
import { ducknngCoordinationClient } from "pi-ducknng/extensions/pi-ducknng/index.ts";

const coordination = attachCoordinationLane({
  client: ducknngCoordinationClient,
  url, projectId: "my-project", agentId: "worker", instanceId: "host-1",
  lane: await harness.lane("main", context),
  context,
});
await coordination.ready;
```

### Two agents, one handoff

The cells below start a coordination endpoint and run two one-shot Codex
agents in sequence. The planner mails a handoff to a reviewer that has
never registered. The reviewer pulls the handoff, answers it with
persistent R, and replies. The last cell checks the round trip through
the endpoint’s own methods rather than trusting either agent’s report.

``` sh
'/root/pi-ducknng/node_modules/.bin/pi' --provider 'openai-codex' --model 'gpt-6-astra' --no-extensions --thinking 'medium' -e './extensions/pi-ducknng/index.ts' --no-session -p \
  "$(printf %s \
    'You are agent `planner`. Use coordination_agents to see ' \
    'who is live, then reserve ' \
    '`resource:readme/mtcars-review`. Send agent `reviewer` a ' \
    'handoff of exactly two lines. Line 1: `Review mpg by ' \
    'cylinder count in datasets::mtcars.` Line 2: `Reply with ' \
    'one sentence naming the cylinder count with the highest ' \
    'mean mpg and that mean.` Then release the reservation. ' \
    'Report the live agents, the message ID, whether the ' \
    'recipient had been seen, and the fencing value, ' \
    'beginning with `AGENT_COORDINATION_SENT` only when the ' \
    'send and the release both succeeded.')"
```

> AGENT_COORDINATION_SENT - Live agents: `planner` - Message ID:
> `91c4089a-1d71-4a8a-bd9b-e905cd8de765` - Recipient seen: false -
> Fencing value: 1 - Reservation released: true

``` sh
'/root/pi-ducknng/node_modules/.bin/pi' --provider 'openai-codex' --model 'gpt-6-astra' --no-extensions --thinking 'medium' -e './extensions/pi-ducknng/index.ts' --no-session -p \
  "$(printf %s \
    'You are agent `reviewer`. Call coordination_inbox with ' \
    'wait_ms 20000 to read your handoff. Answer it by ' \
    'computing the aggregate with the persistent R adapter ' \
    '(start it, follow its ducknng manifest, and close it ' \
    'when done). Reply to the sender with coordination_send ' \
    'in one sentence. Report the sender, the message ID, both ' \
    'handoff lines exactly as received, the computed rows, ' \
    "and your reply's message ID, beginning with " \
    '`AGENT_COORDINATION_REPLIED` only when you received ' \
    'exactly one message, it came from `planner`, and your ' \
    'reply was stored.')"
```

> AGENT_COORDINATION_REPLIED
>
> Sender: planner  
> Message ID: 91c4089a-1d71-4a8a-bd9b-e905cd8de765
>
> ``` text
> Review mpg by cylinder count in datasets::mtcars.
> Reply with one sentence naming the cylinder count with the highest mean mpg and that mean.
> ```
>
> Computed rows:
>
> | cyl |           mean mpg |
> |----:|-------------------:|
> |   4 | 26.663636363636364 |
> |   6 | 19.742857142857144 |
> |   8 |               15.1 |
>
> Reply: The 4-cylinder group has the highest mean mpg at
> 26.663636363636364.
>
> Reply message ID: b3619f9e-012f-4ea6-8eb2-c179fc54e9a5
>
> Persistent R adapter closed.

The endpoint, not either model, confirms the round trip:

``` r
source("tools/ducknng-rpc.R")
url <- Sys.getenv("PI_DUCKNNG_COORDINATION_URL")
verifier <- rpc_call(url, "register", list(
  project_id = "readme", agent_id = "planner",
  instance_id = "readme-verifier", operation_key = "verify"
))
inbox <- rpc_call(
  url, "receive",
  list(registration_id = verifier$registration_id, wait_ms = 5000),
  timeout_ms = 10000
)$messages
reservations <- rpc_call(url, "list_reservations", list(
  registration_id = verifier$registration_id
))$reservations
stopifnot(
  length(inbox) == 1L,
  identical(inbox[[1L]]$sender_agent_id, "reviewer"),
  length(reservations) == 0L
)
invisible(rpc_call(url, "ack", list(
  registration_id = verifier$registration_id,
  receipt_token = inbox[[1L]]$receipt_token
)))
cat(
  "COORDINATION_ROUND_TRIP_VERIFIED",
  sprintf("reply from %s: %s", inbox[[1L]]$sender_agent_id, inbox[[1L]]$content),
  sep = "\n"
)
```

    COORDINATION_ROUND_TRIP_VERIFIED
    reply from reviewer: The 4-cylinder group has the highest mean mpg at 26.663636363636364.

## Agent-backed pkgdown articles

Two pkgdown articles are authored as `vignettes/*.Rmd.orig` and
precomputed with `openai-codex` agents. The committed `.Rmd` results
keep package and site builds credential-free, following the [rOpenSci
precomputed-vignette
pattern](https://ropensci.org/blog/2019/12/08/precompute-vignettes/).

``` sh
make vignettes
make site
```

## Pinned runtime tuple

| Component                         | Version              |
|:----------------------------------|:---------------------|
| DuckDB                            | 1.5.4                |
| `@duckdb/node-api`                | 1.5.4-r.1            |
| ducknng                           | `v0.1.1-duckdb1.5.4` |
| `@earendil-works/pi-coding-agent` | 0.86.1               |
| `@earendil-works/pi-agent-core`   | 0.86.1               |

[`DEPENDENCIES`](DEPENDENCIES) is the version authority and records the
exact source commits.
