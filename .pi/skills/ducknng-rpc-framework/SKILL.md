---
name: ducknng-rpc-framework
description: Use when designing, implementing, or reviewing ducknng as a DuckDB-backed RPC framework over NNG with Arrow IPC, manifest-driven method registration, session lifecycle, and framework-level security constraints. Applies to protocol, registry, dispatcher, session ownership, transport trust, and docs/code alignment in /root/ducknng.
---

# ducknng RPC framework

This skill is for work in `/root/ducknng` when the task is about protocol design, method registration, manifest generation, dispatcher behavior, session lifecycle, or framework-level security.

The repository should be framed as a multi-client RPC framework and SQL server over NNG, not as a pile of ad hoc wire handlers. Treat `docs/protocol.md`, `docs/manifest.md`, `docs/types.md`, `docs/security.md`, and `docs/registry.md` as binding contracts. If code disagrees with those docs, either update the docs deliberately first or bring code into alignment.

The architecture split must stay clear. NNG is the transport layer. The `ducknng` wire envelope is the RPC framing layer. Arrow IPC via nanoarrow C is the tabular payload layer. The registry and manifest are the contract and dispatch-policy layer. DuckDB execution is the data engine layer. Keep version-sensitive NNG behavior isolated in `src/ducknng_nng_compat.c` and version-sensitive DuckDB streaming behavior isolated in `src/ducknng_duckdb_streaming_compat.c`.

When implementing methods, prefer registry-backed dispatch immediately. New public methods should not appear as hardcoded string branches without a descriptor. A descriptor should carry enough information for the dispatcher to validate the request before the handler runs, including request and reply payload limits, accepted flags, emitted flags, session behavior, and security posture. The manifest should be exported from those same descriptors.

Framework-level security is not optional. Multi-client correctness means one client must not corrupt, observe, or hijack another client's state. That implies envelope version checks, bounded method-name lengths, explicit flag validation, and eventual session ownership enforcement. If sessionful methods are in scope, avoid designs that rely on bare session identifiers without an ownership model.

When doing larger refactors, work in this order whenever possible: first make the contract explicit in docs, then create the descriptor and registry types, then move dispatch behind the registry, then add or migrate methods, and finally update tests and README examples. This prevents the project from gaining public behavior that is not yet representable in the manifest.

For handoffs, leave a concise state summary covering what contract changed, what code paths now conform to the contract, what remains stubbed or transitional, and which next step is structurally unlocked. Good handoffs in this repo should mention the envelope version state, registry state, manifest generation state, session ownership state, and whether dispatch is fully registry-backed or still hybrid.
