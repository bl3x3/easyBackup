"""Snapshot integrity scrubbing and chain-aware retention."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import random
import shutil
import socket
import uuid
import zlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from easybackup.archive import sha256_stream
from easybackup.db import Database
from easybackup.errors import CancelledError, ConflictError, StorageError
from easybackup.locking import LeaseGuard, TaskLock
from easybackup.manifest import (
    commit_key_for,
    load_manifest,
    sha256_bytes,
    verify_commit_marker,
)
from easybackup.models import (
    ProgressUpdate,
    Snapshot,
    SnapshotStatus,
    StorageConfig,
    Task,
    utc_now_iso,
)
from easybackup.security import CredentialStore
from easybackup.storage import create_store
from easybackup.storage.base import BlobStore


ProgressEmitter = Callable[[ProgressUpdate], None]
CancelCallback = Callable[[], bool]
logger = logging.getLogger(__name__)


class MaintenanceEngine:
    def __init__(
        self,
        database: Database,
        credentials: CredentialStore,
        lock_dir: Path,
    ):
        self.database = database
        self.credentials = credentials
        self.lock_dir = lock_dir
        self.owner_id = f"{socket.gethostname()}:{uuid.uuid4()}"

    def scrub(
        self,
        snapshot: Snapshot,
        *,
        deep: bool = False,
        sample_ratio: float = 0.01,
        cancelled: CancelCallback | None = None,
        emit: ProgressEmitter | None = None,
    ) -> dict:
        cancelled = cancelled or (lambda: False)
        emit = emit or (lambda update: None)
        store = create_store(snapshot.storage, self.credentials)
        lease_key = f"v1/tasks/{snapshot.task_id}/write.lock.json"
        with TaskLock(self.lock_dir, snapshot.task_id):
            with LeaseGuard(
                store, lease_key, self.owner_id
            ) as lease_guard:
                try:
                    return self._scrub_locked(
                        snapshot,
                        store,
                        deep=deep,
                        sample_ratio=sample_ratio,
                        cancelled=(
                            lambda: cancelled() or lease_guard.lost
                        ),
                        emit=emit,
                    )
                except CancelledError as exc:
                    if lease_guard.lost:
                        raise ConflictError(
                            "远端租约已丢失，巡检已安全中止。"
                        ) from exc
                    raise

    def _scrub_locked(
        self,
        snapshot: Snapshot,
        store: BlobStore,
        *,
        deep: bool,
        sample_ratio: float,
        cancelled: CancelCallback,
        emit: ProgressEmitter,
    ) -> dict:
        if not snapshot.manifest_key:
            raise StorageError("快照没有 Manifest。")
        manifest, payload = load_manifest(store, snapshot.manifest_key)
        verify_commit_marker(
            store, snapshot.manifest_key, payload, snapshot.id
        )
        archives = manifest.archives
        emit(
            ProgressUpdate(
                phase="checking",
                progress=5,
                message=f"正在检查 {len(archives)} 个分卷的元数据…",
            )
        )
        for archive in archives:
            if cancelled():
                raise CancelledError("巡检已取消。")
            value = store.stat(archive.key)
            if value is None:
                self._set_verification(snapshot.id, "missing")
                raise StorageError(f"分卷对象缺失：{archive.key}")
            if value.size != archive.integrity.size:
                self._set_verification(snapshot.id, "corrupt")
                raise StorageError(f"分卷对象大小不一致：{archive.key}")

        total_blocks = [
            (archive, index)
            for archive in archives
            for index in range(len(archive.integrity.crc32))
        ]
        checked_blocks = 0
        checked_archives = 0
        if deep:
            selected_archives = archives
        else:
            selected_archives = []
            if total_blocks:
                sample_count = max(1, math.ceil(len(total_blocks) * sample_ratio))
                # Each run samples fresh blocks so periodic scrubs eventually
                # cover more than one deterministic 1% subset.
                rng = random.SystemRandom()
                sampled = rng.sample(
                    total_blocks, min(sample_count, len(total_blocks))
                )
                for position, (archive, block_index) in enumerate(sampled, start=1):
                    if cancelled():
                        raise CancelledError("巡检已取消。")
                    block_size = archive.integrity.block_size
                    start = block_index * block_size
                    length = min(
                        block_size, archive.integrity.size - start
                    )
                    data = store.read_range(archive.key, start, length)
                    actual = f"{zlib.crc32(data):08x}"
                    expected = archive.integrity.crc32[block_index]
                    if actual != expected:
                        self._set_verification(snapshot.id, "corrupt")
                        raise StorageError(
                            f"分卷 {archive.key} 的抽样块校验失败。"
                        )
                    checked_blocks += 1
                    emit(
                        ProgressUpdate(
                            phase="sampling",
                            progress=10 + 85 * position / len(sampled),
                            message=f"已抽检 {position}/{len(sampled)} 个数据块",
                        )
                    )

        for position, archive in enumerate(selected_archives, start=1):
            if cancelled():
                raise CancelledError("巡检已取消。")
            actual, size = sha256_stream(
                store.open_read(archive.key), cancelled
            )
            if size != archive.integrity.size or actual != archive.integrity.sha256:
                self._set_verification(snapshot.id, "corrupt")
                raise StorageError(f"分卷完整校验失败：{archive.key}")
            checked_archives += 1
            emit(
                ProgressUpdate(
                    phase="verifying",
                    progress=10 + 85 * position / max(1, len(selected_archives)),
                    message=f"已完整校验 {position}/{len(selected_archives)} 个分卷",
                )
            )

        self._set_verification(snapshot.id, "healthy")
        report = {
            "snapshot_id": snapshot.id,
            "status": "healthy",
            "deep": deep,
            "archives_total": len(archives),
            "archives_fully_checked": checked_archives,
            "blocks_sampled": checked_blocks,
            "verified_at": utc_now_iso(),
        }
        emit(
            ProgressUpdate(
                phase="completed",
                progress=100,
                message="快照完整性巡检通过。",
                stats=report,
            )
        )
        return report

    def prune(
        self,
        task: Task,
        *,
        cancelled: CancelCallback | None = None,
        emit: ProgressEmitter | None = None,
    ) -> dict:
        cancelled = cancelled or (lambda: False)
        emit = emit or (lambda update: None)
        with TaskLock(self.lock_dir, task.id):
            return self._prune_locked(task, cancelled, emit)

    def _prune_locked(
        self,
        task: Task,
        cancelled: CancelCallback,
        emit: ProgressEmitter,
    ) -> dict:
        if self.database.has_running_snapshot(task.id):
            raise ConflictError(
                "任务存在尚未完成启动对账的快照，暂不能执行保留清理。"
            )
        snapshots = [
            item
            for item in self.database.list_snapshots(task.id, limit=10000)
            if item.status == SnapshotStatus.COMPLETED
        ]
        chains: dict[str, list[Snapshot]] = defaultdict(list)
        for snapshot in snapshots:
            chains[snapshot.chain_id].append(snapshot)
        ordered = sorted(
            chains.items(),
            key=lambda pair: max(
                item.completed_at or item.started_at for item in pair[1]
            ),
            reverse=True,
        )
        cutoff = datetime.now(timezone.utc) - timedelta(days=task.retention_days)
        keep: set[str] = {
            chain_id for chain_id, _ in ordered[: task.retention_chains]
        }
        for chain_id, values in ordered:
            newest = max(
                datetime.fromisoformat(item.completed_at or item.started_at)
                for item in values
            )
            if newest >= cutoff:
                keep.add(chain_id)
        removable = [
            (chain_id, values)
            for chain_id, values in ordered
            if chain_id not in keep
        ]
        removed_snapshots = 0
        removed_objects = 0
        reclaimed_bytes = 0
        total = sum(len(values) for _, values in removable)
        completed = 0
        storage_groups: dict[
            str, tuple[StorageConfig, list[tuple[str, list[Snapshot]]]]
        ] = {}
        for chain_id, values in removable:
            identities = {
                json.dumps(
                    item.storage.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                for item in values
            }
            if len(identities) != 1:
                raise StorageError(
                    f"快照链 {chain_id} 包含多个存储目标，已拒绝自动删除。"
                )
            identity = identities.pop()
            if identity not in storage_groups:
                storage_groups[identity] = (values[0].storage, [])
            storage_groups[identity][1].append((chain_id, values))

        lease_key = f"v1/tasks/{task.id}/write.lock.json"
        for storage, chain_group in storage_groups.values():
            store = create_store(storage, self.credentials)
            with LeaseGuard(
                store, lease_key, self.owner_id
            ) as lease_guard:
                for chain_id, values in chain_group:
                    self._check_prune_cancel(cancelled, lease_guard)
                    lease_guard.ensure_valid()
                    ordered_values = sorted(
                        values,
                        key=lambda item: item.started_at,
                        reverse=True,
                    )

                    # First make every snapshot in the chain invisible.  No
                    # manifest or volume is removed until all Commit markers
                    # are gone, so an interrupted prune cannot leave a
                    # published incremental snapshot with missing ancestors.
                    for snapshot in ordered_values:
                        self._check_prune_cancel(cancelled, lease_guard)
                        if snapshot.manifest_key:
                            store.delete(
                                commit_key_for(snapshot.manifest_key)
                            )
                            removed_objects += 1

                    for snapshot in ordered_values:
                        self._check_prune_cancel(cancelled, lease_guard)
                        if snapshot.manifest_key:
                            store.delete(snapshot.manifest_key)
                            removed_objects += 1

                    for snapshot in ordered_values:
                        self._check_prune_cancel(cancelled, lease_guard)
                        for archive in snapshot.archives:
                            self._check_prune_cancel(
                                cancelled, lease_guard
                            )
                            store.delete(archive.key)
                            removed_objects += 1
                            reclaimed_bytes += archive.integrity.size
                        completed += 1
                        emit(
                            ProgressUpdate(
                                phase="pruning",
                                progress=100
                                * completed
                                / max(1, total),
                                message=(
                                    f"正在删除过期快照链 {chain_id}"
                                ),
                            )
                        )
                    self._check_prune_cancel(cancelled, lease_guard)
                    self.database.delete_snapshot_rows(
                        [item.id for item in values]
                    )
                    self._remove_chain_cache(task.id, chain_id)
                    removed_snapshots += len(values)
        return {
            "removed_snapshots": removed_snapshots,
            "removed_objects": removed_objects,
            "reclaimed_bytes": reclaimed_bytes,
            "kept_chains": len(keep),
        }

    @staticmethod
    def _check_prune_cancel(
        cancelled: CancelCallback,
        lease_guard: LeaseGuard,
    ) -> None:
        if cancelled():
            raise CancelledError("保留策略清理已取消。")
        if lease_guard.lost:
            raise ConflictError("远端租约已丢失，已停止保留策略清理。")

    def _set_verification(self, snapshot_id: str, status: str) -> None:
        update = getattr(self.database, "update_snapshot_verification", None)
        if update:
            update(snapshot_id, status, utc_now_iso())

    def _remove_chain_cache(self, task_id: str, chain_id: str) -> None:
        """Remove only the derived cache directory for one pruned chain."""

        cache_root = (self.lock_dir.parent / "delta-cache").resolve()
        task_component = sha256_bytes(task_id.encode("utf-8"))[:32]
        chain_component = sha256_bytes(chain_id.encode("utf-8"))[:32]
        target = (
            cache_root / task_component / chain_component
        ).resolve()
        try:
            target.relative_to(cache_root)
        except ValueError:
            logger.warning("拒绝清理超出差分缓存根目录的路径：%s", target)
            return
        try:
            if target.is_dir():
                shutil.rmtree(target)
            task_cache = target.parent
            if task_cache.is_dir() and not any(task_cache.iterdir()):
                task_cache.rmdir()
        except OSError as exc:
            logger.warning("无法清理过期差分基线缓存 %s：%s", target, exc)
