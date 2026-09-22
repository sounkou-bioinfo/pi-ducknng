# Checkpointed workflows in DuckDB

This example implements the small useful part of
[Absurd](https://github.com/earendil-works/absurd)'s model on DuckDB:
pull-based tasks, expiring worker leases, retries, named checkpoints, sleeps,
and first-write-wins events. It does not copy Temporal's event-history engine or
claim Temporal compatibility.

The implementation has three parts:

- [`schema.sql`](schema.sql) owns persistent task rows, task-local checkpoint
  JSON, and event rows;
- [`durable_workflows.py`](durable_workflows.py) provides the Python API;
- [`durable_workflows.R`](durable_workflows.R) provides the DBI-based R API.

Both clients perform each state transition with one parameterized SQL statement
and retry DuckDB write conflicts.

The Python worker requires `duckdb` 1.5 or newer. The R worker requires `DBI`,
`duckdb`, and `jsonlite`. Run both executable smoke tests from the repository
root:

```sh
python3 -m pip install 'duckdb>=1.5'
make durable_workflow_smoke
Rscript -e 'install.packages(c("DBI", "duckdb", "jsonlite"))'
make durable_workflow_r_smoke
```

## Python

A direct Python worker uses one connection per thread:

```python
import duckdb
from examples.durable_workflows.durable_workflows import DurableWorkflows

workflows = DurableWorkflows(duckdb.connect("workflows.duckdb"))
workflows.install()
task_id = workflows.spawn("orders", "fulfill", {"order_id": "42"})

claim = workflows.claim("orders", "worker-1", lease_ms=30_000)
if claim is not None:
    payment = workflows.step(
        claim,
        "charge",
        lambda idempotency_key: charge_card(
            claim.input["order_id"], idempotency_key=idempotency_key
        ),
    )
    workflows.complete(claim, {"payment": payment})
```

## R

The R API uses ordinary functions and named lists rather than an object wrapper:

```r
library(DBI)
library(duckdb)
source("examples/durable_workflows/durable_workflows.R")

con <- dbConnect(duckdb(), dbdir = "workflows.duckdb")
workflow_install(con)
task_id <- workflow_spawn(
  con, "orders", "fulfill", list(order_id = "42")
)

claim <- workflow_claim(con, "orders", "worker-1", lease_ms = 30000)
if (!is.null(claim)) {
  payment <- workflow_step(con, claim, "charge", function(idempotency_key) {
    list(payment_id = "pay-42", idempotency_key = idempotency_key)
  })
  workflow_complete(con, claim, list(payment = payment))
}
dbDisconnect(con, shutdown = TRUE)
```

`workflow_checkpoint()` returns `list(present, value)`, so a stored JSON `null`
is distinct from a missing checkpoint. Lost leases and conflicting task IDs use
`ducknng_workflow_lost_lease` and `ducknng_workflow_spawn_conflict` conditions,
which can be handled directly with `tryCatch()`.

Both `step()` and `workflow_step()` supply the stable key
`<task UUID>:<step name>`. The external system
must enforce that key if duplicate side effects are unacceptable. A process can
crash after `charge_card()` succeeds and before the checkpoint commits, so
neither this example nor Absurd/Temporal activities can promise exactly-once
external effects.

## State transitions

A task starts as `ready`. `claim()` changes one eligible row to `running`, adds
a random lease token, and returns the input. Every heartbeat, checkpoint write,
sleep, event wait, completion, and failure compares that token and requires an
unexpired lease. This fences a stale worker after another worker reclaims the
task, although both worker functions can overlap briefly around lease expiry.

`put_checkpoint()` adds a step key to the task's bounded checkpoint JSON once.
It updates the leased task row itself, so a concurrent lease replacement causes
a DuckDB write conflict rather than allowing a stale worker to publish a step.
On a retry, `step()` returns the stored JSON instead of calling the operation
again. `emit_event()` likewise keeps the first `(task_id, event_name)` value. An
event remains stored if it arrives before `await_event()`, which removes the
missed-wakeup race.

A failed task becomes `ready` after its retry delay until `max_failures` is
reached. An expired lease counts as a failure when another worker reclaims it.
`reap_expired()` marks an expired final lease `failed`; `claim()` runs a bounded
reap before looking for work.

## DuckDB, ducknng, and Quack

DuckDB uses optimistic concurrency and has no PostgreSQL-style `FOR UPDATE SKIP
LOCKED`. Concurrent claimers can select the same row; one commits and the other
receives a write conflict. `DurableWorkflows` retries that whole single-statement
operation with bounded backoff, after which the loser observes no eligible row.
Lease and scheduling deadlines are epoch milliseconds in `BIGINT` columns, so
the clients use the database clock without requiring DuckDB's ICU extension.
Use one DuckDB process and one connection per worker thread. Do not point
independent DuckDB processes at the same database file.

The tables intentionally do not live in ducknng service/session structs. They
survive server and worker restarts in the DuckDB database file. The SQL can run
through ducknng's existing query RPC: install the schema on the server database,
start an in-process service, and explicitly enable unary mutation calls. This
complete local call inserts one task:

```sql
SELECT ducknng_start_server(
  'workflow', 'inproc://workflow', 1, 134217728, 300000, 0::UBIGINT
);
SELECT ducknng_register_exec_method(false);
SELECT *
FROM ducknng_run_rpc_params(
  'inproc://workflow',
  'INSERT INTO ducknng_workflow.tasks
     (task_id, queue_name, task_name, input)
   VALUES (?, ?, ?, ?::JSON)
   ON CONFLICT DO NOTHING',
  struct_pack(
    task_id := uuid(),
    queue_name := 'orders',
    task_name := 'fulfill',
    input := '{"order_id":"42"}'
  ),
  0::UBIGINT
);
```

Production services should require an authenticated peer identity when
registering `exec`.

Claims use `UPDATE ... RETURNING` through the `query_open`/`fetch`/`close`
session family. Do not split a state transition across RPC calls: ducknng does
not provide a transaction spanning independent calls.

The same schema can be hosted behind Quack. Attach the remote DuckDB, select it
as the current catalog, and run the schema and worker statements there:

```sql
CREATE SECRET (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:server' AS workflow_server;
USE workflow_server;
```

Quack changes how SQL reaches the owning DuckDB process; it does not need a
workflow-specific protocol change.

## Why this is not Temporal

Temporal stores an ordered event history and replays deterministic workflow code
against it. Its History and Matching services shard state and dispatch work
across a distributed deployment. This example instead restarts ordinary worker
code from its entry point and substitutes completed checkpoint values. It has no
determinism checker, history replay, child workflows, multi-cluster replication,
visibility service, or cross-process DuckDB writer coordination.

That smaller model is appropriate for an embedded DuckDB/ducknng service with a
modest pull-worker pool. Use Temporal when deterministic replay, large-scale task
matching, richer workflow primitives, or established distributed operations are
requirements. Use PostgreSQL Absurd when many independent database clients need
`SKIP LOCKED` queue claims.
