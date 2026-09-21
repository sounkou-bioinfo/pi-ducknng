

# pi-ducknng

`pi-ducknng` is a ducknng-backed network substrate for Pi, beginning
with persistent R sessions.

## Architecture

![Generic ducknng endpoint architecture](man/figures/architecture.svg)

Placement stays outside the generic invocation path and supplies an
endpoint URL. Endpoint-specific implementations belong in the examples
below. The committed SVG is generated from the Mermaid source in
`man/figures/architecture.mmd`.

DuckDB owns native extension loading and host-language calls. The
hard-vendored ducknng release owns transport, mbedTLS, identity,
framing, manifests, sessions, AIO, cancellation, and codecs. The R
package composes `nanonext` and `mirai`; it does not reproduce those
layers.

## Pi package

Install the repository as a Pi package:

``` sh
pi install git:github.com/sounkou-bioinfo/pi-ducknng
```

The package contributes three model tools: `persistent_r_start` places
the first adapter, `ducknng_describe` reads any compatible endpoint
manifest, and `ducknng_call` invokes a declared method. On first use it
builds the pinned ducknng source; this requires Git, Make, CMake,
Python, and a C/C++ toolchain. The R runtime must provide the packages
listed under `Imports` in [`DESCRIPTION`](DESCRIPTION).

## Durable agent coordination

Start the coordination endpoint independently of any Pi session. Keep
its locator and DuckDB state in a private persistent directory:

``` sh
install -d -m 700 "$HOME/.local/state/pi-ducknng"
Rscript --vanilla tools/pi-coordination-endpoint.R \
  "$HOME/.local/state/pi-ducknng/coordination.url" \
  "$HOME/.local/state/pi-ducknng/coordination.duckdb"
```

The endpoint manifests `register`, `heartbeat`, `list_agents`, `send`,
`receive`, `ack`, `reserve`, `release`, and `unregister`. It has no
evaluation method. Mail is stored before `send` returns, survives
disconnected Pi sessions and endpoint restarts, and is leased by
`receive` until acknowledged or the visibility timeout expires.
Reservations are advisory leases over canonical opaque `resource:`
identifiers or lexical canonical `file:///` URIs. An acquisition
operation key is not reused while its record is retained; reacquisition
after release or expiry uses a new key and fencing generation.
Acknowledged messages and expired operation records are retained for 30
days, and each mailbox accepts at most 10,000 pending messages.

Attach an interactive Pi session in another terminal:

``` sh
export PI_DUCKNNG_COORDINATION_URL="$(cat \
  "$HOME/.local/state/pi-ducknng/coordination.url")"
export PI_DUCKNNG_COORDINATION_PROJECT="my-project"
export PI_DUCKNNG_AGENT_ID="reviewer"
pi
```

The extension registers the Pi session, heartbeats, polls its stable
mailbox, injects messages through Pi’s steering API, and acknowledges
after API acceptance. Stable message IDs are recorded in Pi session
entries for retry reconciliation. Delivery is at least once; work
performed in response to a message is not exactly once.

This initial endpoint is for one trusted user on one machine. It listens
on loopback and does not provide cross-user or cross-host authorization.
NNG PUB/SUB is deliberately absent from the correctness path: a later
event socket may reduce wake-up latency, but missed events must always
be repaired by `receive`.

## Pi-to-R proof

The evaluated cell below loads the package extension into an OpenAI
Codex agent. The model discovers the endpoint’s methods and request
examples, then performs an `mtcars` analysis whose intermediate table
survives across fresh DuckDB clients.

``` sh
'pi' --provider 'openai-codex' --model 'gpt-6-astra' --no-extensions --thinking 'medium' -e './extensions/pi-ducknng/index.ts' --no-session -p \
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
> Manifested methods: `eval` (JSON → Arrow, persistent process), `close`
> (JSON → JSON).
>
> Both eval calls succeeded in endpoint process `146187`. Endpoint
> closed successfully.
>
> First call — decoded rows:
>
> | cyl |                mpg |
> |----:|-------------------:|
> |   4 | 26.663636363636364 |
> |   6 | 19.742857142857144 |
> |   8 |               15.1 |
>
> Second call — using persisted `mpg_by_cyl`:
>
> | cyl |                mpg |     delta_from_4cyl |
> |----:|-------------------:|--------------------:|
> |   4 | 26.663636363636364 |                   0 |
> |   6 | 19.742857142857144 |   -6.92077922077922 |
> |   8 |               15.1 | -11.563636363636364 |

`persistent_r_start` returns an NNG URL, `ducknng_describe` fetches the
endpoint’s ducknng RPC manifest, and `ducknng_call` sends declared calls
as ducknng frames. Each tool request opens and closes a fresh DuckDB
instance. Eval results are Arrow IPC streams produced by nanoarrow and
decoded by ducknng; the mirai-owned R environment remains in the same
endpoint process.

Executable documentation uses `piknit`; `make readme` rejects output
without the agent’s success receipt.

## Agent-backed pkgdown articles

Two pkgdown articles are authored as `vignettes/*.Rmd.orig` and
precomputed with `openai-codex` agents. The committed `.Rmd` results let
package and site builds remain credential-free, following the [rOpenSci
precomputed-vignette
pattern](https://ropensci.org/blog/2019/12/08/precompute-vignettes/).

``` sh
make vignettes
make site
```

## Pinned runtime tuple

| Component                         | Version              |
|-----------------------------------|----------------------|
| DuckDB                            | 1.5.4                |
| `@duckdb/node-api`                | 1.5.4-r.1            |
| ducknng                           | `v0.1.1-duckdb1.5.4` |
| `@earendil-works/pi-coding-agent` | 0.86.1               |
| `@earendil-works/pi-agent-core`   | 0.86.1               |

[`DEPENDENCIES`](DEPENDENCIES) records the exact source commit.
