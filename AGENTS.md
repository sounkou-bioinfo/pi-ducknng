# AGENTS.md

- `THESIS.md` is the design authority.
- `README.md` is generated from executable `README.qmd` through `piknit`; run `make readme` after edits.
- `DEPENDENCIES` is the version authority; update ducknng upstream first, then refresh `vendor/ducknng` per `VENDORING.md`.
- Keep the R package thin: compose DuckDB API, ducknng, nanonext, and mirai; do not duplicate their transport, session, AIO, or scheduler semantics.
- Run focused checks and `make check` before committing.
