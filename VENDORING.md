# Vendoring ducknng

`vendor/ducknng` is a hard-vendored source subtree of <https://github.com/RGenomicsETL/ducknng>.

[`DEPENDENCIES`](DEPENDENCIES) is the version authority for the DuckDB, DuckDB API, and ducknng tuple. The `git-subtree-split` trailer on each vendoring commit records the exact imported upstream commit.

Transport, TLS, identity, framing, manifest, session, AIO, cancellation, and codec changes belong in ducknng first. `pi-ducknng` then refreshes the pinned subtree; it does not maintain a divergent implementation.

Refresh only after intentionally updating `DEPENDENCIES`:

```sh
set -a
. ./DEPENDENCIES
set +a

git subtree pull \
  --prefix=vendor/ducknng \
  https://github.com/RGenomicsETL/ducknng.git \
  "$DUCKNNG_REF" --squash
```
