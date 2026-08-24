

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
reconnect proof from the installed package root.

## Persistent R proof

The command below is an evaluated Quarto cell; a nonzero exit stops
rendering:

``` sh
make persistent-r-proof
```

    Rscript --vanilla tools/persistent-r-proof.R
    persistent R daemon reconnected with x + 1 = 42

The proof launches an independent mirai daemon, stores `x <- 41`, ends
its first host session, reconnects a second host at the same resolved
NNG URL, evaluates `x + 1`, and requires `42` before explicit shutdown.

Executable documentation uses `piknit`: `{pi}` chunks run Pi prompts,
while `{pish}` chunks run commands such as `node` or `make` and stop
rendering on failure.

## Pinned runtime tuple

| Component          | Version              |
|--------------------|----------------------|
| DuckDB             | 1.5.4                |
| `@duckdb/node-api` | 1.5.4-r.1            |
| ducknng            | `v0.1.1-duckdb1.5.4` |

[`DEPENDENCIES`](DEPENDENCIES) records the exact source commit. \##
Design
