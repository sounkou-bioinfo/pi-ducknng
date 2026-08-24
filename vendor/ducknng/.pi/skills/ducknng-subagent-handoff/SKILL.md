---
name: ducknng-subagent-handoff
description: Use when splitting ducknng work into parallel subproblems or when preparing a precise handoff for later continuation. Applies to protocol, registry, dispatcher, security, session lifecycle, docs/code drift, smoke tests, and README/test regeneration in /root/ducknng.
---

# ducknng subagent and handoff workflow

Use this skill when the work naturally splits into distinct framework layers or when a checkpoint needs to be captured cleanly for later continuation.

The preferred decomposition for `ducknng` is by layer, not by arbitrary file boundaries. Good splits are protocol and docs, wire and decoder changes, registry and dispatcher changes, method implementation changes, session lifecycle changes, transport and security changes, and test or README regeneration. Avoid splitting one logical contract change across multiple handoffs without a clear boundary because this repository is contract-driven and drift between docs and code is the easiest way to lose coherence.

A good handoff in this repo should always state the contract state first. Say whether `docs/protocol.md`, `docs/manifest.md`, `docs/types.md`, `docs/security.md`, and `docs/registry.md` are still authoritative for the changed area, or whether one of them was intentionally revised. Then say what code now conforms to that contract, what still uses a temporary shim or stub, and what concrete next step is unblocked.

When the work has been split across subproblems, keep each subproblem answerable in terms of repository invariants. For example, a wire-layer handoff should say whether the envelope version is validated and whether name lengths and flags are bounded. A registry-layer handoff should say whether dispatch is fully registry-backed or still hybrid. A method-layer handoff should say whether a method has a descriptor, whether it is exported through the manifest, and whether its payload formats and size limits are enforced by the dispatcher. A session-layer handoff should say whether ownership exists yet or remains a planned constraint.

Every handoff should also include verification state. Mention which of `make release`, `make test_release`, `make rpc_smoke`, `make rpc_smoke_r`, and `make rdm` were run, which passed, and which were intentionally skipped. `make rpc_smoke` is the CI-safe sqllogictest smoke path and must not depend on R. `make rpc_smoke_r` is the optional R/nanonext interop smoke path. If a smoke path depends on a temporary limitation, say so explicitly. In this repository, verification is part of the handoff because README examples, smoke tests, and generated docs are part of the public contract.

When continuing from a previous handoff, prefer preserving existing architectural direction over taking shortcuts. If the repo is mid-transition from hardcoded dispatch to registry-backed dispatch, continue that transition rather than adding new hardcoded branches. If the repo has adopted a versioned envelope, do not add examples or tests using old frames just because they are convenient. The purpose of the handoff is to make continuation safer, not to justify drift.
