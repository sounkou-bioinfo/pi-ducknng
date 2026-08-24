# Concept control and AI-assisted engineering

`ducknng` adopts the working principle in antirez's [Control the ideas, not the code](https://antirez.com/news/169): the maintainer's primary responsibility is the software's mental model, direction, and quality. This is not permission to accept opaque generated code. It changes the unit of review from line production to architecture, invariants, and executable evidence.

## What must stay under human control

A material change must have a concrete account of the structures it introduces or changes, who owns them, and how they move through states. For `ducknng`, that normally means the selected carrier, RPC descriptor and manifest entry, request and reply payloads, session or aio lifecycle, DuckDB connection ownership, allocation limits, failure surface, and cleanup path. If these cannot be explained without reciting functions file by file, the design is not yet controlled.

Each semantic decision has one authority. RPC capability and policy come from method descriptors and the registry-derived manifest. Public SQL functions come from `function_catalog/functions.yaml`. Transport support comes from the compiled capability descriptor. Wire and type behavior come from their binding docs and fixtures. Generated output is checked against those authorities rather than edited into agreement by hand.

## How agents are used

Keep one controller with the whole model. An agent may inspect or implement a bounded question, but agent count, worktrees, staged handoffs, and parallel plans are not measures of progress. Use a separate call only when the question is materially independent and its evidence can be stated precisely. Integrate related code, docs, tests, and generated artifacts in one working tree unless genuine independent delivery requires otherwise.

Ask for structures and behavior, not just patches. Useful questions are: what state machine does this implement; which lock protects each transition; what allocation is controlled by remote input; what happens after cancellation; which descriptor authorizes dispatch; and what observable fixture proves the claim. Reject locally plausible code when it introduces a second authority, hidden state, an unused abstraction, or a surface with no current consumer.

## Review and QA

Review the concept first, then inspect code where local details can violate it. Ownership transfers, cleanup labels, checked arithmetic, parser offsets, refcounts, lock release, unstable DuckDB calls, and credential handling still warrant close source review. Repetitive registration or vector-assignment code does not deserve equal attention merely because it has many lines.

Spend the saved review time on proof. Run the narrow regression first, then the relevant SQL/property/sanitizer and interop gates. Exercise malformed input, cancellation, teardown, concurrency, and clean-checkout builds. Benchmark only the real claimed path and record the revision, workload, transport, machine, and correctness check. A capability is implemented only when its producer, carrier, consumer, and executable proof all exist.

## Handoffs and durable records

A handoff states the current contract, implementation, and verification evidence. It names exact unresolved contradictions rather than narrating agent activity. Preserve durable design in the binding docs; preserve regressions in tests; preserve backlog in issues. Delete completed TODO files, obsolete checklists, stale design proposals, and temporary review reports once their surviving decisions and tests have moved to the correct authority.
