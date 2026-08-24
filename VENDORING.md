# Vendoring ducknng

`vendor/ducknng` is a hard-vendored source subtree of:

- upstream: <https://github.com/RGenomicsETL/ducknng>
- branch: `main`

The `git-subtree-split` trailer on each vendoring commit records the exact upstream commit.

Transport, TLS, identity, framing, manifest, session, AIO, cancellation, and codec changes belong in ducknng first. `pi-duckdnng` then refreshes the subtree; it does not maintain a divergent implementation.

Refresh from upstream with:

```sh
git subtree pull \
  --prefix=vendor/ducknng \
  https://github.com/RGenomicsETL/ducknng.git \
  main --squash
```
