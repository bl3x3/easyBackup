"""Local exclusion and renewable remote object-store leases."""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from filelock import FileLock, Timeout

from easybackup.errors import ConflictError
from easybackup.storage.base import BlobStore, RemoteLease


logger = logging.getLogger(__name__)


class TaskLock:
    def __init__(self, lock_dir: Path, task_id: str):
        lock_dir.mkdir(parents=True, exist_ok=True)
        self._lock = FileLock(str(lock_dir / f"{task_id}.lock"))

    def __enter__(self):
        try:
            self._lock.acquire(timeout=0)
        except Timeout as exc:
            raise ConflictError("该任务已在本机运行。") from exc
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._lock.release()


class LeaseGuard:
    """Acquire and refresh a remote lease; loss of lease acts as cancellation."""

    def __init__(
        self,
        store: BlobStore,
        key: str,
        owner: str,
        ttl_seconds: int = 300,
    ):
        self.store = store
        self.key = key
        self.owner = owner
        self.ttl_seconds = ttl_seconds
        self.lease: RemoteLease | None = None
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._renew_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @property
    def lost(self) -> bool:
        return self._lost.is_set()

    def __enter__(self) -> "LeaseGuard":
        self.lease = self.store.acquire_lease(
            self.key, self.owner, self.ttl_seconds
        )
        if not self.lease:
            raise ConflictError("远端备份目标正被另一台设备使用。")

        def heartbeat() -> None:
            interval = max(5, self.ttl_seconds // 3)
            while not self._stop.wait(interval):
                if not self._renew_once():
                    return

        self._thread = threading.Thread(
            target=heartbeat,
            name="easybackup-lease-heartbeat",
            daemon=True,
        )
        self._thread.start()
        return self

    def _renew_once(self) -> bool:
        with self._renew_lock:
            if self._lost.is_set():
                return False
            assert self.lease is not None
            try:
                renewed = self.store.renew_lease(
                    self.lease, self.ttl_seconds
                )
            except Exception:
                # Continuing after an uncertain renewal could let this writer
                # publish after its lease expired and another host took over.
                self._lost.set()
                logger.exception("远端租约续期失败，当前写操作将安全中止")
                return False
            if not renewed:
                self._lost.set()
                return False
            self.lease = renewed
            return True

    def ensure_valid(self) -> None:
        """Synchronously renew before an irreversible publication step."""

        if not self._renew_once():
            raise ConflictError("远端租约已丢失，已阻止最终提交。")

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self.lease and not self._lost.is_set():
            try:
                self.store.release_lease(self.lease)
            except Exception:
                # The lease has a TTL and will expire.  A release failure must
                # not mask the operation result, especially after Commit.
                logger.exception("释放远端租约失败；租约将等待 TTL 自动过期")
