#!/usr/bin/env python3
"""Executable proof for examples/durable_workflows."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import sys
import threading
import time
import uuid

import duckdb


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "examples" / "durable_workflows" / "durable_workflows.py"
SPEC = importlib.util.spec_from_file_location("durable_workflows", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
Claim = MODULE.Claim
DurableWorkflows = MODULE.DurableWorkflows
LostLease = MODULE.LostLease
SpawnConflict = MODULE.SpawnConflict


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ducknng-workflows-") as directory:
        database_path = str(Path(directory) / "workflows.duckdb")
        owner_connection = duckdb.connect(database_path)
        workflows = DurableWorkflows(owner_connection)
        workflows.install()

        task_id = uuid.uuid4()
        payload = {"order_id": "42", "amount": 9999}
        require(
            workflows.spawn("orders", "fulfill", payload, task_id=task_id) == task_id,
            "spawn did not preserve the caller task ID",
        )
        require(
            workflows.spawn("orders", "fulfill", payload, task_id=task_id) == task_id,
            "idempotent spawn did not return the existing task",
        )
        try:
            workflows.spawn("orders", "fulfill", {"order_id": "other"}, task_id=task_id)
        except SpawnConflict:
            pass
        else:
            raise AssertionError("task ID reuse with different input was accepted")

        first = workflows.claim("orders", "worker-a", lease_ms=5_000)
        require(isinstance(first, Claim), "ready task was not claimed")
        calls = []

        def charge(idempotency_key: str):
            calls.append(idempotency_key)
            return {"payment_id": "pay-42"}

        payment = workflows.step(first, "charge", charge)
        require(payment == {"payment_id": "pay-42"}, "checkpoint value changed")
        require(workflows.step(first, "charge", charge) == payment, "checkpoint was not replayed")
        require(len(calls) == 1, "completed step executed twice")
        require(calls[0] == f"{task_id}:charge", "step idempotency key was unstable")
        require(workflows.put_checkpoint(first, "nullable", None) is None, "null checkpoint changed")
        present, null_value = workflows.checkpoint(first, "nullable")
        require(present and null_value is None, "null checkpoint looked absent")

        ready, value = workflows.await_event(first, "shipment.packed")
        require(not ready and value is None, "missing event did not suspend the task")
        emitted = workflows.emit_event(task_id, "shipment.packed", {"tracking": "TRACK123"})
        require(emitted == {"tracking": "TRACK123"}, "event value changed")
        require(
            workflows.emit_event(task_id, "shipment.packed", {"tracking": "later"}) == emitted,
            "event did not keep its first value",
        )

        resumed = workflows.claim("orders", "worker-b", lease_ms=5_000)
        require(isinstance(resumed, Claim), "cached event did not make the task claimable")
        ready, value = workflows.await_event(resumed, "shipment.packed")
        require(ready and value == emitted, "cached event was not replayed")
        try:
            workflows.complete(first, {"wrong": True})
        except LostLease:
            pass
        else:
            raise AssertionError("stale lease completed a resumed task")
        workflows.complete(resumed, {"tracking": value["tracking"]})
        task = workflows.task(task_id)
        require(task is not None and task["status"] == "completed", "task did not complete")

        retry_id = workflows.spawn(
            "orders", "retry", {}, max_failures=2, task_id=uuid.uuid4()
        )
        retry_one = workflows.claim("orders", "worker-a")
        require(retry_one is not None and retry_one.task_id == retry_id, "retry task not claimed")
        require(workflows.fail(retry_one, "first") == "ready", "first failure was terminal")
        retry_two = workflows.claim("orders", "worker-a")
        require(retry_two is not None and retry_two.task_id == retry_id, "retry not scheduled")
        require(workflows.fail(retry_two, "second") == "failed", "failure limit ignored")

        expired_id = workflows.spawn(
            "orders", "expired", {}, max_failures=2, task_id=uuid.uuid4()
        )
        expired = workflows.claim("orders", "worker-old", lease_ms=10)
        require(expired is not None and expired.task_id == expired_id, "expiry task not claimed")
        time.sleep(0.03)
        recovered = workflows.claim("orders", "worker-new", lease_ms=5_000)
        require(recovered is not None and recovered.task_id == expired_id, "expired lease not recovered")
        require(recovered.failure_count == 1, "lease expiry did not count as a failure")
        try:
            workflows.heartbeat(expired)
        except LostLease:
            pass
        else:
            raise AssertionError("expired lease was extended")
        try:
            workflows.put_checkpoint(expired, "stale-step", {"wrong": True})
        except LostLease:
            pass
        else:
            raise AssertionError("expired lease published a checkpoint")
        workflows.complete(recovered, {"recovered": True})

        exhausted_id = workflows.spawn(
            "orders", "exhausted", {}, max_failures=1, task_id=uuid.uuid4()
        )
        exhausted = workflows.claim("orders", "worker-old", lease_ms=10)
        require(exhausted is not None and exhausted.task_id == exhausted_id, "exhaustion task not claimed")
        time.sleep(0.03)
        require(workflows.reap_expired("orders") == 1, "exhausted lease was not reaped")
        require(workflows.task(exhausted_id)["status"] == "failed", "reaped task not terminal")

        concurrent_id = workflows.spawn(
            "concurrent", "one-owner", {}, task_id=uuid.uuid4()
        )
        barrier = threading.Barrier(2)
        claimed: list[Claim | None] = []
        errors: list[BaseException] = []
        result_lock = threading.Lock()

        def claimant(worker_id: str) -> None:
            connection = duckdb.connect(database_path)
            client = DurableWorkflows(connection)
            try:
                barrier.wait()
                result = client.claim("concurrent", worker_id, lease_ms=5_000)
                with result_lock:
                    claimed.append(result)
            except BaseException as exc:
                with result_lock:
                    errors.append(exc)
            finally:
                connection.close()

        threads = [
            threading.Thread(target=claimant, args=("worker-c",)),
            threading.Thread(target=claimant, args=("worker-d",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        require(not errors, f"concurrent claim failed: {errors}")
        winners = [claim for claim in claimed if claim is not None]
        require(len(winners) == 1, f"expected one claim winner, got {len(winners)}")
        require(winners[0].task_id == concurrent_id, "wrong task won concurrent claim")

        owner_connection.close()
    print("durable workflow smoke: ok")


if __name__ == "__main__":
    main()
