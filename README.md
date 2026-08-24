

# pi-ducknng

`pi-ducknng` is a ducknng-backed network substrate for Pi, beginning
with persistent R sessions.

## Architecture

``` mermaid
flowchart LR
    PI["Pi client or extension"] --> API["DuckDB API"]
    API --> DB["DuckDB 1.5.4"]
    DB --> DNNG["ducknng"]
    DNNG <--> FABRIC["NNG fabric"]
    FABRIC <--> R["Persistent R<br/>nanonext + mirai"]
    FABRIC <--> PEER["Pi, DuckDB, or other NNG endpoint"]
```

DuckDB owns the host-language boundary. The hard-vendored ducknng
release owns transport, mbedTLS, identity, framing, manifests, sessions,
AIO, cancellation, and codecs. The R package composes `nanonext` and
`mirai`; it does not reproduce those layers.

## Pi package

Install the repository as a Pi package:

``` sh
pi install git:github.com/sounkou-bioinfo/pi-ducknng
```

The package contributes `/ducknng-proof`, which runs the persistent-R
reconnect proof from the installed package root. On first use it builds
the pinned ducknng source; this requires Git, Make, CMake, Python, and a
C/C++ toolchain.

## Pi-to-R proof

The command below is an evaluated Quarto cell; a nonzero exit stops
rendering:

``` sh
make pi-proof
```

    node scripts/pi-rpc-proof.mjs
    Pi RPC: discovered and invoked the local /ducknng-proof command
    Pi extension: loaded DuckDB API and ducknng
    Pi client A: set x = 41 through DuckDB -> ducknng -> NNG -> R
    Pi client A: disconnected
    Pi client B: reconnected through a fresh DuckDB instance
    Pi client B: evaluated x + 1 = 42 in the same persistent R session
    R endpoint: stopped

The proof starts Pi in headless RPC mode, confirms that the local
package contributed `/ducknng-proof`, and invokes that extension command
without an LLM call. The extension opens DuckDB through
`@duckdb/node-api`, loads the vendored ducknng extension, and sends a
request through NNG to a nanonext gateway backed by one mirai R daemon.
It then closes the first DuckDB instance, opens a fresh instance,
reconnects, and requires `x + 1 == 42` before shutdown.

Executable documentation uses `piknit`; its `{pish}` engine stops
rendering on command failure.

## Pinned runtime tuple

| Component          | Version              |
|--------------------|----------------------|
| DuckDB             | 1.5.4                |
| `@duckdb/node-api` | 1.5.4-r.1            |
| ducknng            | `v0.1.1-duckdb1.5.4` |

[`DEPENDENCIES`](DEPENDENCIES) records the exact source commit. \##
Design
