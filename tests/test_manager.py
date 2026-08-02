from __future__ import annotations

import asyncio
import threading

import pytest

from easybackup.events import EventBus
from easybackup.manager import (
    OperationManager,
    _ActiveOperation,
    _ProgressRelay,
)
from easybackup.models import (
    Operation,
    OperationKind,
    OperationStatus,
    ProgressUpdate,
    utc_now_iso,
)


class _RecordingDatabase:
    def __init__(self, operation: Operation) -> None:
        self.operation = operation
        self.updates: list[dict[str, object]] = []

    def update_operation(self, operation_id: str, **fields: object) -> Operation:
        assert operation_id == self.operation.id
        self.updates.append(fields)
        payload = self.operation.model_dump(mode="python")
        payload.update(fields)
        self.operation = Operation.model_validate(payload)
        return self.operation


@pytest.mark.asyncio
async def test_progress_relay_coalesces_a_worker_burst() -> None:
    loop = asyncio.get_running_loop()
    delivered: list[ProgressUpdate] = []
    relay = _ProgressRelay(
        loop, delivered.append, min_interval_seconds=0.0
    )
    updates = [
        ProgressUpdate(
            phase="hashing",
            message=f"scanned {index}",
            stats={"files_scanned": index},
        )
        for index in range(2_000)
    ]

    worker = threading.Thread(
        target=lambda: [relay.emit(update) for update in updates]
    )
    worker.start()
    worker.join(timeout=2)
    assert not worker.is_alive()

    await asyncio.sleep(0)

    assert delivered == [updates[-1]]


@pytest.mark.asyncio
async def test_progress_relay_drops_queued_update_after_close() -> None:
    loop = asyncio.get_running_loop()
    delivered: list[ProgressUpdate] = []
    relay = _ProgressRelay(loop, delivered.append)
    relay.emit(ProgressUpdate(phase="hashing", message="pending"))

    relay.close()
    await asyncio.sleep(0)

    assert delivered == []


@pytest.mark.asyncio
async def test_operation_progress_burst_stays_bounded_and_terminal() -> None:
    operation = Operation(
        id="operation-id",
        task_id="task-id",
        kind=OperationKind.BACKUP,
        status=OperationStatus.QUEUED,
        phase="queued",
        message="queued",
        created_at=utc_now_iso(),
    )
    database = _RecordingDatabase(operation)
    manager = OperationManager(
        database,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        EventBus(),
    )

    def emit_burst(cancelled, emit):
        assert cancelled() is False
        for index in range(10_000):
            emit(
                ProgressUpdate(
                    phase="hashing",
                    message=f"scanned {index}",
                    stats={"files_scanned": index},
                )
            )
        return {"files_scanned": 10_000}

    await manager._execute(operation, threading.Event(), emit_burst)
    await asyncio.sleep(0.3)

    progress_writes = [
        update for update in database.updates if "status" not in update
    ]
    assert len(progress_writes) <= 4
    assert database.operation.status == OperationStatus.COMPLETED
    assert database.operation.phase == "completed"
    assert database.updates[-1]["status"] == OperationStatus.COMPLETED


@pytest.mark.asyncio
async def test_shutdown_cancels_active_operation_before_waiting() -> None:
    manager = OperationManager(None, None, None, None, None)  # type: ignore[arg-type]
    cancel = threading.Event()

    async def wait_for_cancel() -> None:
        while not cancel.is_set():
            await asyncio.sleep(0.001)

    task = asyncio.create_task(wait_for_cancel())
    active = _ActiveOperation(
        operation_id="operation-id",
        task_id="task-id",
        cancel=cancel,
        task=task,
    )
    manager._active_by_id[active.operation_id] = active
    manager._active_by_task[active.task_id] = active

    await manager.shutdown(timeout=1)

    assert cancel.is_set()
    assert task.done()
