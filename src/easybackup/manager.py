"""Asynchronous operation manager bridging FastAPI and blocking pipelines."""

from __future__ import annotations

import asyncio
import functools
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable

from easybackup.db import Database
from easybackup.engine import BackupEngine, MaintenanceEngine, RestoreEngine
from easybackup.errors import CancelledError, ConflictError
from easybackup.events import EventBus
from easybackup.models import (
    Operation,
    OperationKind,
    OperationStatus,
    ProgressUpdate,
    RestoreRequest,
    ScrubRequest,
)


logger = logging.getLogger(__name__)


class _ProgressRelay:
    """Bound progress delivery from worker threads to the event loop.

    Backup pipelines can report progress much faster than SQLite and WebSocket
    consumers can persist it. Keeping only the newest pending update prevents
    ``call_soon_threadsafe`` from creating an unbounded callback backlog that
    would otherwise starve HTTP requests and signal handling.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        callback: Callable[[ProgressUpdate], None],
        *,
        min_interval_seconds: float = 0.25,
    ) -> None:
        self._loop = loop
        self._callback = callback
        self._min_interval_seconds = max(0.0, min_interval_seconds)
        self._lock = threading.Lock()
        self._latest: ProgressUpdate | None = None
        self._scheduled = False
        self._closed = False
        self._next_delivery_at = 0.0

    def emit(self, update: ProgressUpdate) -> None:
        should_schedule = False
        with self._lock:
            if self._closed:
                return
            self._latest = update
            if not self._scheduled:
                self._scheduled = True
                should_schedule = True
        if should_schedule:
            self._loop.call_soon_threadsafe(self._schedule_drain)

    def close(self) -> None:
        """Drop pending non-terminal progress before storing final state."""

        with self._lock:
            self._closed = True
            self._latest = None

    def _schedule_drain(self) -> None:
        with self._lock:
            if self._closed:
                self._scheduled = False
                return
        delay = max(0.0, self._next_delivery_at - self._loop.time())
        if delay:
            self._loop.call_later(delay, self._drain)
        else:
            self._drain()

    def _drain(self) -> None:
        with self._lock:
            if self._closed:
                self._latest = None
                self._scheduled = False
                return
            update = self._latest
            self._latest = None
            if update is None:
                self._scheduled = False
                return

        try:
            self._callback(update)
        finally:
            self._next_delivery_at = (
                self._loop.time() + self._min_interval_seconds
            )
            with self._lock:
                if self._closed:
                    self._latest = None
                    self._scheduled = False
                    should_reschedule = False
                elif self._latest is None:
                    self._scheduled = False
                    should_reschedule = False
                else:
                    should_reschedule = True
            if should_reschedule:
                self._loop.call_later(
                    self._min_interval_seconds, self._drain
                )


@dataclass(slots=True)
class _ActiveOperation:
    operation_id: str
    task_id: str
    cancel: threading.Event
    task: asyncio.Task


class OperationManager:
    def __init__(
        self,
        database: Database,
        backup_engine: BackupEngine,
        restore_engine: RestoreEngine,
        maintenance_engine: MaintenanceEngine,
        events: EventBus,
    ):
        self.database = database
        self.backup_engine = backup_engine
        self.restore_engine = restore_engine
        self.maintenance_engine = maintenance_engine
        self.events = events
        self._active_by_task: dict[str, _ActiveOperation] = {}
        self._active_by_id: dict[str, _ActiveOperation] = {}
        self._accepting = True

    def is_task_busy(self, task_id: str) -> bool:
        return task_id in self._active_by_task

    @property
    def active_count(self) -> int:
        return len(self._active_by_id)

    def stop_accepting(self) -> None:
        """Synchronously close the admission gate during app shutdown."""

        self._accepting = False

    async def start_backup(
        self, task_id: str, *, force_full: bool = False
    ) -> Operation:
        task = self.database.get_task(task_id)
        if self.database.has_running_snapshot(task_id):
            raise ConflictError(
                "任务存在尚未完成启动对账的快照；请先恢复对应存储或凭据。"
            )

        def backup_and_prune(cancel, emit):
            snapshot = self.backup_engine.run(
                task,
                force_full=force_full,
                cancelled=cancel,
                emit=emit,
            )
            try:
                report = self.maintenance_engine.prune(
                    task, cancelled=cancel, emit=emit
                )
                if report["removed_snapshots"]:
                    emit(
                        ProgressUpdate(
                            phase="retention",
                            progress=100,
                            message=(
                                f"已按保留策略清理 "
                                f"{report['removed_snapshots']} 个旧快照。"
                            ),
                            stats={"retention": report},
                        )
                    )
            except CancelledError:
                emit(
                    ProgressUpdate(
                        phase="retention",
                        progress=100,
                        message="快照已提交；自动保留清理因取消请求而跳过。",
                    )
                )
            except Exception:
                # The snapshot is already durably committed.  Retention failure
                # must be visible in logs but must not turn a valid backup into
                # a failed backup operation.
                logger.exception("备份成功后的自动保留策略执行失败")
            return snapshot

        return self._start(
            task_id,
            OperationKind.BACKUP,
            backup_and_prune,
        )

    async def start_restore(self, request: RestoreRequest) -> Operation:
        snapshot = self.database.get_snapshot(request.snapshot_id)
        self.database.get_task(snapshot.task_id)
        return self._start(
            snapshot.task_id,
            OperationKind.RESTORE,
            lambda cancel, emit: self.restore_engine.run(
                snapshot, request, cancelled=cancel, emit=emit
            ),
            snapshot_id=snapshot.id,
        )

    async def start_scrub(
        self, snapshot_id: str, request: ScrubRequest
    ) -> Operation:
        snapshot = self.database.get_snapshot(snapshot_id)
        return self._start(
            snapshot.task_id,
            OperationKind.SCRUB,
            lambda cancel, emit: self.maintenance_engine.scrub(
                snapshot,
                deep=request.deep,
                sample_ratio=request.sample_ratio,
                cancelled=cancel,
                emit=emit,
            ),
            snapshot_id=snapshot.id,
        )

    async def start_prune(self, task_id: str) -> Operation:
        task = self.database.get_task(task_id)
        if self.database.has_running_snapshot(task_id):
            raise ConflictError(
                "任务存在尚未完成启动对账的快照，暂不能执行保留清理。"
            )
        return self._start(
            task.id,
            OperationKind.PRUNE,
            lambda cancel, emit: self.maintenance_engine.prune(
                task, cancelled=cancel, emit=emit
            ),
        )

    def _start(
        self,
        task_id: str,
        kind: OperationKind,
        function: Callable[[Callable[[], bool], Callable[[ProgressUpdate], None]], Any],
        *,
        snapshot_id: str | None = None,
    ) -> Operation:
        if not self._accepting:
            raise ConflictError("服务正在关闭，不再接受新操作。")
        if task_id in self._active_by_task:
            active = self._active_by_task[task_id]
            raise ConflictError(
                "该任务已有操作正在运行。",
                details={"operation_id": active.operation_id},
            )
        operation = self.database.create_operation(task_id, kind, snapshot_id)
        cancel_event = threading.Event()
        async_task = asyncio.create_task(
            self._execute(operation, cancel_event, function),
            name=f"easybackup-{kind.value}-{operation.id}",
        )
        active = _ActiveOperation(
            operation_id=operation.id,
            task_id=task_id,
            cancel=cancel_event,
            task=async_task,
        )
        self._active_by_task[task_id] = active
        self._active_by_id[operation.id] = active
        self.events.publish(
            "operation.queued",
            {"operation": operation.model_dump(mode="json")},
        )
        return operation

    async def _execute(
        self,
        operation: Operation,
        cancel_event: threading.Event,
        function: Callable,
    ) -> None:
        loop = asyncio.get_running_loop()
        progress_relay = _ProgressRelay(
            loop,
            functools.partial(self._apply_progress, operation.id),
        )
        current = self.database.update_operation(
            operation.id,
            status=OperationStatus.RUNNING,
            phase="starting",
            message="操作已开始。",
        )
        self.events.publish(
            "operation.started",
            {"operation": current.model_dump(mode="json")},
        )

        try:
            result = await asyncio.to_thread(
                function, cancel_event.is_set, progress_relay.emit
            )
            progress_relay.close()
            stats = (
                result.model_dump(mode="json")
                if hasattr(result, "model_dump")
                else result
            )
            current = self.database.update_operation(
                operation.id,
                status=OperationStatus.COMPLETED,
                progress=100.0,
                phase="completed",
                message="操作已完成。",
                stats=stats if isinstance(stats, dict) else {"result": stats},
                snapshot_id=getattr(result, "id", operation.snapshot_id),
            )
            self.events.publish(
                "operation.completed",
                {"operation": current.model_dump(mode="json")},
            )
        except CancelledError as exc:
            progress_relay.close()
            current = self.database.update_operation(
                operation.id,
                status=OperationStatus.CANCELLED,
                phase="cancelled",
                message=str(exc),
                error=None,
            )
            self.events.publish(
                "operation.cancelled",
                {"operation": current.model_dump(mode="json")},
            )
        except Exception as exc:
            progress_relay.close()
            logger.exception("操作 %s 执行失败", operation.id)
            current = self.database.update_operation(
                operation.id,
                status=OperationStatus.FAILED,
                phase="failed",
                message="操作失败。",
                error=str(exc),
            )
            self.events.publish(
                "operation.failed",
                {"operation": current.model_dump(mode="json")},
            )
        finally:
            progress_relay.close()
            active = self._active_by_id.pop(operation.id, None)
            if active:
                self._active_by_task.pop(active.task_id, None)

    def _apply_progress(
        self, operation_id: str, update: ProgressUpdate
    ) -> None:
        try:
            operation = self.database.update_operation(
                operation_id,
                phase=update.phase,
                progress=update.progress,
                message=update.message,
                stats=update.stats,
            )
        except Exception:
            logger.exception("持久化操作进度失败")
            return
        self.events.publish(
            "operation.progress",
            {
                "operation_id": operation_id,
                "task_id": operation.task_id,
                "progress": update.model_dump(mode="json"),
            },
        )

    def cancel(self, operation_id: str) -> Operation:
        active = self._active_by_id.get(operation_id)
        operation = self.database.get_operation(operation_id)
        if not active:
            if operation.status in {
                OperationStatus.COMPLETED,
                OperationStatus.FAILED,
                OperationStatus.CANCELLED,
            }:
                raise ConflictError("该操作已经结束。")
            raise ConflictError("操作不在当前进程中运行。")
        active.cancel.set()
        operation = self.database.update_operation(
            operation_id,
            status=OperationStatus.CANCELLING,
            message="正在等待当前流式分块安全停止…",
        )
        self.events.publish(
            "operation.cancelling",
            {"operation": operation.model_dump(mode="json")},
        )
        return operation

    async def shutdown(self, timeout: int) -> None:
        self.stop_accepting()
        active_operations = list(self._active_by_id.values())
        for active in active_operations:
            active.cancel.set()
        tasks = [active.task for active in active_operations]
        if not tasks:
            return
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        del done
        if pending:
            logger.warning(
                "等待 %d 个后台操作安全停止超时；进程退出将继续等待其释放资源。",
                len(pending),
            )
