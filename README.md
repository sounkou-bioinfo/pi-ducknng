

# pi-ducknng

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

The package contributes three model tools:

- `persistent_r_start` places the R endpoint;
- `ducknng_describe` reads any compatible endpoint manifest;
- `ducknng_call` invokes a declared method.

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
> Manifested methods: `eval` (JSON → Arrow, persistent R evaluation),
> `close` (JSON → JSON, stops endpoint).
>
> Both eval calls succeeded in endpoint process `257984`, preserving
> `mpg_by_cyl`. Endpoint closed successfully.
>
> First call decoded rows:
>
> | cyl |                mpg |
> |----:|-------------------:|
> |   4 | 26.663636363636364 |
> |   6 | 19.742857142857144 |
> |   8 |               15.1 |
>
> Second call decoded rows:
>
> | cyl |                mpg |     delta_from_4cyl |
> |----:|-------------------:|--------------------:|
> |   4 | 26.663636363636364 |                   0 |
> |   6 | 19.742857142857144 |   -6.92077922077922 |
> |   8 |               15.1 | -11.563636363636364 |

Each tool request opens and closes a fresh DuckDB instance. Eval results
travel as Arrow IPC streams written by nanoarrow and decoded by ducknng.
The mirai-owned R environment stays in the endpoint process until
`close` or 30 seconds without a request.

The README is rendered by `piknit`, and `make readme` rejects output
that lacks the agent’s success receipt.

## Durable agent coordination

Start the coordination endpoint independently of any Pi session, with
its locator and DuckDB state in a private directory:

``` sh
install -d -m 700 "$HOME/.local/state/pi-ducknng"
Rscript --vanilla tools/pi-coordination-endpoint.R \
  "$HOME/.local/state/pi-ducknng/coordination.url" \
  "$HOME/.local/state/pi-ducknng/coordination.duckdb"
```

Attach an interactive Pi session in another terminal:

``` sh
export PI_DUCKNNG_COORDINATION_URL="$(cat \
  "$HOME/.local/state/pi-ducknng/coordination.url")"
export PI_DUCKNNG_COORDINATION_PROJECT="my-project"
export PI_DUCKNNG_AGENT_ID="reviewer"
pi
```

The extension registers the session and polls the `reviewer` mailbox. It
injects each message through Pi’s steering API and acknowledges it once
that call returns.

- `send` stores mail before replying. Queued mail and reservations
  survive disconnected sessions and endpoint restarts.
- Delivery is at least once. A crash between injection and
  acknowledgement redelivers the same message ID, and work done in
  response is not exactly once.
- Reservations are advisory leases over `resource:` identifiers or
  canonical `file:///` URIs, with a fencing value that increases across
  the project.
- Acknowledged messages are kept for 30 days. A mailbox holds at most
  10,000 unacknowledged messages.

This first endpoint is for one trusted user on one machine. It listens
on a new loopback port at each start, so restart attached Pi sessions
after restarting it. It has no authentication, and the adapter does not
yet give the model its own send or reserve tool. NNG PUB/SUB is kept off
the correctness path: a later event socket may reduce wake-up latency,
but `receive` always repairs missed events.

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
