"""Startup recovery for snapshots interrupted around the remote commit point."""

from __future__ import annotations

import logging
import socket
import uuid
from pathlib import Path

from easybackup.db import Database
from easybackup.errors import (
    ConflictError,
    CredentialError,
    NotFoundError,
    StorageError,
    ValidationError,
)
from easybackup.locking import LeaseGuard, TaskLock
from easybackup.manifest import (
    commit_key_for,
    load_manifest,
    verify_commit_marker,
)
from easybackup.models import SnapshotStatus, utc_now_iso
from easybackup.security import CredentialStore
from easybackup.storage import create_store


logger = logging.getLogger(__name__)


def _snapshot_prefix(manifest_key: str) -> str:
    """Return the exact immutable snapshot prefix, never a task-wide prefix."""

    suffix = "/manifest.json"
    if not manifest_key.endswith(suffix):
        raise ValidationError(
            f"快照 Manifest 键不符合预期格式：{manifest_key!r}"
        )
    prefix = manifest_key[: -len("manifest.json")]
    parts = prefix.strip("/").split("/")
    if not prefix or any(part in {"", ".", ".."} for part in parts):
        raise ValidationError(
            f"快照 Manifest 键包含不安全路径：{manifest_key!r}"
        )
    return prefix


def _discard_uncommitted_snapshot(
    store,
    manifest_key: str,
    lease_guard: LeaseGuard,
) -> None:
    """Remove one unpublished snapshot while exclusive ownership is proven."""

    prefix = _snapshot_prefix(manifest_key)
    lease_guard.ensure_valid()

    # Remove publication first.  If cleanup is interrupted after this point,
    # readers still cannot observe the remaining immutable objects.
    store.delete(commit_key_for(manifest_key))
    objects = list(store.iter_objects(prefix))
    for item in objects:
        if not item.key.startswith(prefix):
            raise StorageError(
                f"对象存储返回了快照前缀以外的对象：{item.key!r}"
            )
        store.delete(item.key)


def reconcile_incomplete_snapshots(
    database: Database,
    credentials: CredentialStore,
    lock_dir: Path,
) -> dict[str, int]:
    """Import remotely committed snapshots or mark uncommitted runs as failed.

    Backup always inserts the SQLite row before writing remote objects and writes
    ``commit.json`` last.  Therefore a running row with a valid commit marker is
    safe to finish locally; without that marker it was never externally visible.
    """

    completed = 0
    failed = 0
    deferred = 0
    owner_id = f"reconcile:{socket.gethostname()}:{uuid.uuid4()}"
    for candidate in database.list_running_snapshots():
        try:
            with TaskLock(lock_dir, candidate.task_id):
                # The list can be stale by the time a periodic worker obtains
                # the task lock.  Never replay a completed/failed/deleted row.
                try:
                    snapshot = database.get_snapshot(candidate.id)
                except NotFoundError:
                    continue
                if snapshot.status != SnapshotStatus.RUNNING:
                    continue
                if not snapshot.manifest_key:
                    database.fail_snapshot(
                        snapshot.id,
                        "应用重启：快照尚未发布 Manifest。",
                    )
                    failed += 1
                    continue
                try:
                    _snapshot_prefix(snapshot.manifest_key)
                except ValidationError as exc:
                    database.fail_snapshot(
                        snapshot.id,
                        f"应用重启：本地快照元数据无效（{exc}）",
                    )
                    failed += 1
                    logger.warning(
                        "快照 %s 的本地元数据无效：%s",
                        snapshot.id,
                        exc,
                    )
                    continue

                store = create_store(snapshot.storage, credentials)
                lease_key = (
                    f"v1/tasks/{snapshot.task_id}/write.lock.json"
                )
                with LeaseGuard(
                    store, lease_key, owner_id
                ) as lease_guard:
                    manifest_stat = store.stat(snapshot.manifest_key)
                    commit_stat = store.stat(
                        commit_key_for(snapshot.manifest_key)
                    )
                    if commit_stat is None:
                        _discard_uncommitted_snapshot(
                            store,
                            snapshot.manifest_key,
                            lease_guard,
                        )
                        database.fail_snapshot(
                            snapshot.id,
                            (
                                "应用重启：远端缺少 Commit 对象，"
                                "未提交对象已清理。"
                            ),
                        )
                        failed += 1
                        continue
                    if manifest_stat is None:
                        # Commit is publication evidence.  Never destroy a
                        # published prefix just because a compatible provider
                        # temporarily returns 404 for another object.
                        raise StorageError(
                            "远端 Commit 已存在但 Manifest 暂不可见；"
                            "已保留发布证据并延后对账。"
                        )

                    try:
                        manifest, payload = load_manifest(
                            store, snapshot.manifest_key
                        )
                        verify_commit_marker(
                            store,
                            snapshot.manifest_key,
                            payload,
                            snapshot.id,
                        )
                    except ValidationError as exc:
                        logger.warning(
                            "快照 %s 已发布但远端元数据当前无法验证；"
                            "保留对象并延后对账：%s",
                            snapshot.id,
                            exc,
                        )
                        raise StorageError(
                            "已发布快照的远端元数据无法验证；"
                            "为避免破坏恢复证据，已延后对账。"
                        ) from exc

                    own_archives = [
                        item
                        for item in manifest.archives
                        if item.snapshot_id == snapshot.id
                    ]
                    completed_snapshot = snapshot.model_copy(
                        update={
                            "status": SnapshotStatus.COMPLETED,
                            "archives": own_archives,
                            "file_count": len(manifest.files),
                            "changed_count": sum(
                                1
                                for item in manifest.files
                                if item.origin_snapshot_id == snapshot.id
                            ),
                            "deleted_count": len(manifest.deleted),
                            "archive_size": sum(
                                item.integrity.size
                                for item in own_archives
                            ),
                            "archive_sha256": (
                                manifest.archive_integrity.sha256
                            ),
                            "integrity": {
                                "reconciled": True,
                            },
                            "completed_at": utc_now_iso(),
                            "error": None,
                        }
                    )
                    lease_guard.ensure_valid()
                    database.commit_snapshot(
                        completed_snapshot, manifest.files
                    )
                    completed += 1
                    logger.info(
                        "已从远端提交标记恢复快照 %s", snapshot.id
                    )
        except (ConflictError, CredentialError, StorageError) as exc:
            deferred += 1
            logger.warning(
                "快照 %s 的远端存储或互斥锁暂时不可用，"
                "将在下次启动重试：%s",
                candidate.id,
                exc,
            )
            continue
    return {
        "completed": completed,
        "failed": failed,
        "deferred": deferred,
    }
