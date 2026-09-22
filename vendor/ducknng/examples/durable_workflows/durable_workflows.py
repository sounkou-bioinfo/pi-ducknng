"""Small checkpointed workflow state machine backed by DuckDB.

This is intentionally an Absurd-style client library, not a Temporal server.
Every state transition is one SQL statement so the same statements can be sent
through ducknng's query RPC.  A worker must use a separate DuckDB connection per
thread and retry optimistic write conflicts; this class does both at the
operation boundary but does not make external side effects exactly once.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import random
import time
import uuid
from typing import Any, Callable, Optional


_SCHEMA_PATH = Path(__file__).with_name("schema.sql")
_MAX_JSON_BYTES = 1_048_576


class LostLease(RuntimeError):
    """The task is no longer owned by this worker lease."""


class SpawnConflict(RuntimeError):
    """A task ID was reused with different immutable task input."""


@dataclass(frozen=True)
class Claim:
    task_id: uuid.UUID
    queue_name: str
    task_name: str
    input: Any
    priority: int
    run_count: int
    failure_count: int
    max_failures: int
    worker_id: str
    lease_token: uuid.UUID

    def step_key(self, step_name: str) -> str:
        """Stable external idempotency key for one checkpointed step."""
        return f"{self.task_id}:{step_name}"


class DurableWorkflows:
    """Operate the workflow tables through an autocommit DuckDB connection."""

    def __init__(self, connection: Any, conflict_retries: int = 8):
        if conflict_retries < 1:
            raise ValueError("conflict_retries must be positive")
        self.connection = connection
        self.conflict_retries = conflict_retries

    def install(self) -> None:
        self.connection.execute(_SCHEMA_PATH.read_text(encoding="utf-8"))

    def spawn(
        self,
        queue_name: str,
        task_name: str,
        input: Any,
        *,
        task_id: Optional[uuid.UUID] = None,
        priority: int = 0,
        max_failures: int = 3,
    ) -> uuid.UUID:
        self._check_name("queue_name", queue_name, 128)
        self._check_name("task_name", task_name, 256)
        if not 1 <= max_failures <= 1_000_000:
            raise ValueError("max_failures must be between 1 and 1000000")
        task_id = task_id or uuid.uuid4()
        encoded = self._encode_json(input)
        rows = self._execute(
            """
            INSERT INTO ducknng_workflow.tasks(
                task_id, queue_name, task_name, input, priority, max_failures
            )
            VALUES (?, ?, ?, ?::JSON, ?, ?)
            ON CONFLICT DO NOTHING
            RETURNING task_id
            """,
            [task_id, queue_name, task_name, encoded, priority, max_failures],
        )
        if rows:
            return task_id

        existing = self._execute(
            """
            SELECT queue_name, task_name, input, priority, max_failures
            FROM ducknng_workflow.tasks
            WHERE task_id = ?
            """,
            [task_id],
        )
        if not existing:
            raise RuntimeError("spawn lost its insert without an existing task")
        row = existing[0]
        same = (
            row[0] == queue_name
            and row[1] == task_name
            and self._decode_json(row[2]) == input
            and row[3] == priority
            and row[4] == max_failures
        )
        if not same:
            raise SpawnConflict(f"task {task_id} already has different input")
        return task_id

    def claim(
        self,
        queue_name: str,
        worker_id: str,
        *,
        lease_ms: int = 30_000,
        reap_limit: int = 64,
    ) -> Optional[Claim]:
        self._check_name("queue_name", queue_name, 128)
        self._check_name("worker_id", worker_id, 256)
        if not 1 <= lease_ms <= 86_400_000:
            raise ValueError("lease_ms must be between 1 and 86400000")
        self.reap_expired(queue_name, limit=reap_limit)
        lease_token = uuid.uuid4()
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET status = 'running',
                run_count = run_count + 1,
                failure_count = failure_count +
                    CASE WHEN status = 'running' THEN 1 ELSE 0 END,
                waiting_event = NULL,
                lease_owner = ?,
                lease_token = ?,
                lease_expires_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id = (
                SELECT t.task_id
                FROM ducknng_workflow.tasks AS t
                WHERE t.queue_name = ?
                  AND (
                    (t.status = 'ready'
                     AND t.available_at_ms <= epoch_ms(current_timestamp))
                    OR
                    (t.status = 'running'
                     AND t.lease_expires_at_ms <= epoch_ms(current_timestamp)
                     AND t.failure_count + 1 < t.max_failures)
                    OR
                    (t.status = 'waiting' AND (
                        (t.waiting_event IS NULL
                         AND t.available_at_ms <= epoch_ms(current_timestamp))
                        OR
                        (t.waiting_event IS NOT NULL AND EXISTS (
                            SELECT 1
                            FROM ducknng_workflow.events AS e
                            WHERE e.task_id = t.task_id
                              AND e.event_name = t.waiting_event
                        ))
                    ))
                  )
                  AND t.failure_count < t.max_failures
                ORDER BY t.priority DESC, t.available_at_ms,
                         t.created_at_ms, t.task_id
                LIMIT 1
            )
            RETURNING task_id, queue_name, task_name, input, priority,
                      run_count, failure_count, max_failures
            """,
            [worker_id, lease_token, lease_ms, queue_name],
        )
        if not rows:
            return None
        row = rows[0]
        return Claim(
            task_id=uuid.UUID(str(row[0])),
            queue_name=row[1],
            task_name=row[2],
            input=self._decode_json(row[3]),
            priority=row[4],
            run_count=row[5],
            failure_count=row[6],
            max_failures=row[7],
            worker_id=worker_id,
            lease_token=lease_token,
        )

    def reap_expired(self, queue_name: str, *, limit: int = 64) -> int:
        if not 1 <= limit <= 10_000:
            raise ValueError("limit must be between 1 and 10000")
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET status = 'failed',
                failure_count = failure_count + 1,
                last_error = 'worker lease expired',
                lease_owner = NULL,
                lease_token = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id IN (
                SELECT task_id
                FROM ducknng_workflow.tasks
                WHERE queue_name = ?
                  AND status = 'running'
                  AND lease_expires_at_ms <= epoch_ms(current_timestamp)
                  AND failure_count + 1 >= max_failures
                ORDER BY lease_expires_at_ms, task_id
                LIMIT ?
            )
            RETURNING task_id
            """,
            [queue_name, limit],
        )
        return len(rows)

    def heartbeat(self, claim: Claim, *, lease_ms: int = 30_000) -> None:
        if not 1 <= lease_ms <= 86_400_000:
            raise ValueError("lease_ms must be between 1 and 86400000")
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET lease_expires_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id = ?
              AND status = 'running'
              AND lease_token = ?
              AND lease_expires_at_ms > epoch_ms(current_timestamp)
            RETURNING task_id
            """,
            [lease_ms, claim.task_id, claim.lease_token],
        )
        self._require_lease(rows, claim)

    def checkpoint(self, claim: Claim, step_name: str) -> tuple[bool, Any]:
        self._check_name("step_name", step_name, 256)
        rows = self._execute(
            """
            SELECT checkpoints
            FROM ducknng_workflow.tasks
            WHERE task_id = ?
            """,
            [claim.task_id],
        )
        if not rows:
            return False, None
        checkpoints = self._decode_json(rows[0][0])
        if step_name not in checkpoints:
            return False, None
        return True, self._decode_json(checkpoints[step_name]["json"])

    def put_checkpoint(self, claim: Claim, step_name: str, value: Any) -> Any:
        self._check_name("step_name", step_name, 256)
        encoded = self._encode_json(value)
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET checkpoints = json_merge_patch(
                    checkpoints, json_object(?, json_object('json', ?))
                ),
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id = ?
              AND status = 'running'
              AND lease_token = ?
              AND lease_expires_at_ms > epoch_ms(current_timestamp)
              AND NOT list_contains(json_keys(checkpoints), ?)
              AND length(CAST(json_merge_patch(
                    checkpoints, json_object(?, json_object('json', ?))
                  ) AS VARCHAR)) <= 16777216
            RETURNING checkpoints
            """,
            [
                step_name,
                encoded,
                claim.task_id,
                claim.lease_token,
                step_name,
                step_name,
                encoded,
            ],
        )
        if rows:
            checkpoints = self._decode_json(rows[0][0])
            return self._decode_json(checkpoints[step_name]["json"])
        present, stored = self.checkpoint(claim, step_name)
        if present:
            return stored
        lease = self._execute(
            """
            SELECT 1
            FROM ducknng_workflow.tasks
            WHERE task_id = ?
              AND status = 'running'
              AND lease_token = ?
              AND lease_expires_at_ms > epoch_ms(current_timestamp)
            """,
            [claim.task_id, claim.lease_token],
        )
        if lease:
            raise ValueError("task checkpoints exceed 16 MiB")
        raise LostLease(f"lost lease for task {claim.task_id}")

    def step(self, claim: Claim, step_name: str, operation: Callable[[str], Any]) -> Any:
        """Run and checkpoint a step; ``operation`` receives an idempotency key."""
        present, value = self.checkpoint(claim, step_name)
        if present:
            return value
        value = operation(claim.step_key(step_name))
        return self.put_checkpoint(claim, step_name, value)

    def emit_event(self, task_id: uuid.UUID, event_name: str, value: Any) -> Any:
        self._check_name("event_name", event_name, 256)
        encoded = self._encode_json(value)
        rows = self._execute(
            """
            INSERT INTO ducknng_workflow.events(task_id, event_name, value)
            SELECT ?, ?, ?::JSON
            WHERE EXISTS (
                SELECT 1 FROM ducknng_workflow.tasks WHERE task_id = ?
            )
            ON CONFLICT DO NOTHING
            RETURNING value
            """,
            [task_id, event_name, encoded, task_id],
        )
        if rows:
            return self._decode_json(rows[0][0])
        rows = self._execute(
            """
            SELECT value
            FROM ducknng_workflow.events
            WHERE task_id = ? AND event_name = ?
            """,
            [task_id, event_name],
        )
        if not rows:
            raise KeyError(f"unknown task {task_id}")
        return self._decode_json(rows[0][0])

    def await_event(self, claim: Claim, event_name: str) -> tuple[bool, Any]:
        self._check_name("event_name", event_name, 256)
        rows = self._execute(
            """
            SELECT value
            FROM ducknng_workflow.events
            WHERE task_id = ? AND event_name = ?
            """,
            [claim.task_id, event_name],
        )
        if rows:
            return True, self._decode_json(rows[0][0])
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET status = 'waiting',
                waiting_event = ?,
                lease_owner = NULL,
                lease_token = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id = ?
              AND status = 'running'
              AND lease_token = ?
              AND lease_expires_at_ms > epoch_ms(current_timestamp)
            RETURNING task_id
            """,
            [event_name, claim.task_id, claim.lease_token],
        )
        self._require_lease(rows, claim)
        return False, None

    def sleep(self, claim: Claim, delay_ms: int) -> None:
        if not 0 <= delay_ms <= 31_536_000_000:
            raise ValueError("delay_ms must be between 0 and 31536000000")
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET status = 'waiting',
                waiting_event = NULL,
                available_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
                lease_owner = NULL,
                lease_token = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id = ?
              AND status = 'running'
              AND lease_token = ?
              AND lease_expires_at_ms > epoch_ms(current_timestamp)
            RETURNING task_id
            """,
            [delay_ms, claim.task_id, claim.lease_token],
        )
        self._require_lease(rows, claim)

    def complete(self, claim: Claim, result: Any) -> None:
        encoded = self._encode_json(result)
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET status = 'completed',
                result = ?::JSON,
                waiting_event = NULL,
                lease_owner = NULL,
                lease_token = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id = ?
              AND status = 'running'
              AND lease_token = ?
              AND lease_expires_at_ms > epoch_ms(current_timestamp)
            RETURNING task_id
            """,
            [encoded, claim.task_id, claim.lease_token],
        )
        self._require_lease(rows, claim)

    def fail(self, claim: Claim, error: str, *, retry_delay_ms: int = 0) -> str:
        if len(error) > 8192:
            raise ValueError("error must be at most 8192 characters")
        if not 0 <= retry_delay_ms <= 31_536_000_000:
            raise ValueError("retry_delay_ms must be between 0 and 31536000000")
        rows = self._execute(
            """
            UPDATE ducknng_workflow.tasks
            SET status = CASE
                    WHEN failure_count + 1 >= max_failures THEN 'failed'
                    ELSE 'ready'
                END,
                failure_count = failure_count + 1,
                available_at_ms = epoch_ms(current_timestamp) + CAST(? AS BIGINT),
                last_error = ?,
                waiting_event = NULL,
                lease_owner = NULL,
                lease_token = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms = epoch_ms(current_timestamp)
            WHERE task_id = ?
              AND status = 'running'
              AND lease_token = ?
              AND lease_expires_at_ms > epoch_ms(current_timestamp)
            RETURNING status
            """,
            [retry_delay_ms, error, claim.task_id, claim.lease_token],
        )
        self._require_lease(rows, claim)
        return rows[0][0]

    def task(self, task_id: uuid.UUID) -> Optional[dict[str, Any]]:
        cursor = self._execute_cursor(
            """
            SELECT task_id, queue_name, task_name, input, status, priority,
                   run_count, failure_count, max_failures, available_at_ms,
                   waiting_event, lease_owner, lease_token, lease_expires_at_ms,
                   result, checkpoints, last_error, created_at_ms, updated_at_ms
            FROM ducknng_workflow.tasks
            WHERE task_id = ?
            """,
            [task_id],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        names = [item[0] for item in cursor.description]
        result = dict(zip(names, row))
        for key in ("input", "result", "checkpoints"):
            if result[key] is not None:
                result[key] = self._decode_json(result[key])
        return result

    def _execute(self, sql: str, params: list[Any]) -> list[tuple[Any, ...]]:
        return self._execute_cursor(sql, params).fetchall()

    def _execute_cursor(self, sql: str, params: list[Any]) -> Any:
        delay = 0.001
        for attempt in range(self.conflict_retries):
            try:
                return self.connection.execute(sql, params)
            except Exception as exc:
                if not self._is_write_conflict(exc) or attempt + 1 == self.conflict_retries:
                    raise
                time.sleep(delay + random.random() * delay)
                delay = min(delay * 2, 0.05)
        raise AssertionError("unreachable")

    @staticmethod
    def _is_write_conflict(exc: Exception) -> bool:
        text = str(exc).lower()
        return "conflict on" in text or "transaction conflict" in text

    @staticmethod
    def _require_lease(rows: list[tuple[Any, ...]], claim: Claim) -> None:
        if not rows:
            raise LostLease(f"lost lease for task {claim.task_id}")

    @staticmethod
    def _check_name(label: str, value: str, maximum: int) -> None:
        if not isinstance(value, str) or not 1 <= len(value) <= maximum:
            raise ValueError(f"{label} must contain between 1 and {maximum} characters")

    @staticmethod
    def _encode_json(value: Any) -> str:
        encoded = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        if len(encoded.encode("utf-8")) > _MAX_JSON_BYTES:
            raise ValueError("JSON value exceeds 1 MiB")
        return encoded

    @staticmethod
    def _decode_json(value: Any) -> Any:
        if isinstance(value, str):
            return json.loads(value)
        return json.loads(str(value))
