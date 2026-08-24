

<!-- README.md is generated from README.qmd. Edit README.qmd, then run `make readme`. -->

# pi-ducknng

`pi-ducknng` is a ducknng-backed network substrate for Pi, beginning
with persistent R sessions.

The project is incubating under `sounkou-bioinfo`. Transfer to
`RGenomicsETL` requires a separate maturity decision.

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

## Discovery

Pi adds one stable discovery tool, `duckdnng_describe`. It returns
endpoint methods and descriptions. Calls continue through the existing
DuckDB API surface, keeping the Pi tool schema cache-stable.

## Pinned runtime tuple

| Component          | Version              |
|--------------------|----------------------|
| DuckDB             | 1.5.4                |
| `@duckdb/node-api` | 1.5.4-r.1            |
| ducknng            | `v0.1.1-duckdb1.5.4` |

[`DEPENDENCIES`](DEPENDENCIES) records the exact source commit. See
[`VENDORING.md`](VENDORING.md) for the upstream-first update rule.

## Design

The authoritative working design is [`THESIS.md`](THESIS.md).
