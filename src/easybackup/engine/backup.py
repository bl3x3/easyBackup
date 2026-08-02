"""Full/incremental backup state machine and two-phase remote publication."""

from __future__ import annotations

import logging
import os
import socket
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from easybackup.archive import (
    IntegrityReader,
    create_archive_stream,
    extension_for,
    find_tool,
    plan_shards,
    resolve_codec,
)
from easybackup.db import Database
from easybackup.delta import (
    DEFAULT_PATCH_OUTPUT_OVERHEAD_BYTES,
    apply_delta_patch,
    compress_patch,
    create_delta_patch,
    find_xdelta3,
    verify_file,
)
from easybackup.delta_workflow import extract_tar_member, stage_scanned_file
from easybackup.errors import (
    CancelledError,
    ConflictError,
    NotFoundError,
    StorageError,
    ToolMissingError,
    ValidationError,
)
from easybackup.locking import LeaseGuard, TaskLock
from easybackup.manifest import (
    commit_key_for,
    load_manifest,
    sha256_bytes,
    stable_json_bytes,
    verify_commit_marker,
)
from easybackup.models import (
    ArchiveObject,
    FileVersion,
    FileVersionKind,
    ManifestFile,
    ProgressUpdate,
    Snapshot,
    SnapshotKind,
    SnapshotManifest,
    SnapshotStatus,
    Task,
    storage_location_identity,
    utc_now_iso,
)
from easybackup.scanner import ScannedFile, assert_files_unchanged, scan_source
from easybackup.security import CredentialStore
from easybackup.storage import create_store


logger = logging.getLogger(__name__)
ProgressEmitter = Callable[[ProgressUpdate], None]
CancelCallback = Callable[[], bool]


@dataclass(frozen=True, slots=True)
class _PreparedDelta:
    item: ScannedFile
    baseline: FileVersion
    baseline_archive: ArchiveObject
    compressed_path: Path
    raw_size: int
    archive_id: str
    object_key: str


def _noop_emit(update: ProgressUpdate) -> None:
    del update


def _source_fingerprint(source: Path) -> str:
    canonical = os.path.normcase(str(source.resolve()))
    return sha256_bytes(canonical.encode("utf-8"))


class BackupEngine:
    def __init__(
        self,
        database: Database,
        credentials: CredentialStore,
        lock_dir: Path,
        integrity_block_size: int,
        xdelta3_path: str | None = None,
    ):
        self.database = database
        self.credentials = credentials
        self.lock_dir = lock_dir
        self.integrity_block_size = integrity_block_size
        self.xdelta3_path = xdelta3_path
        self.delta_cache_dir = lock_dir.parent / "delta-cache"
        self.owner_id = f"{socket.gethostname()}:{uuid.uuid4()}"

    def run(
        self,
        task: Task,
        *,
        force_full: bool = False,
        cancelled: CancelCallback | None = None,
        emit: ProgressEmitter | None = None,
    ) -> Snapshot:
        emit = emit or _noop_emit
        cancelled = cancelled or (lambda: False)
        source = Path(task.source_path).expanduser().resolve()
        codec = resolve_codec(task.compression)
        store = create_store(task.storage, self.credentials)
        inserted = False
        commit_published = False
        uploaded_keys: list[str] = []
        with TaskLock(self.lock_dir, task.id):
            if self.database.has_running_snapshot(task.id):
                raise ConflictError(
                    "任务存在尚未完成启动对账的快照，已阻止创建新的快照分支。"
            )
            lease_key = f"v1/tasks/{task.id}/write.lock.json"
            with LeaseGuard(store, lease_key, self.owner_id) as lease_guard:
                delta_workspace: tempfile.TemporaryDirectory[str] | None = None
                try:
                    # Lineage must be selected only after both local and
                    # remote exclusion are held.  Otherwise another process
                    # can commit between this read and our lock acquisition.
                    try:
                        latest = self.database.latest_snapshot(task.id)
                    except NotFoundError:
                        latest = None
                    storage_changed = bool(
                        latest
                        and storage_location_identity(latest.storage)
                        != storage_location_identity(task.storage)
                    )
                    full_due = bool(
                        latest
                        and self.database.count_since_last_full(task.id)
                        >= task.full_every
                    )
                    source_fingerprint = _source_fingerprint(source)
                    latest_manifest = None
                    if (
                        latest
                        and latest.manifest_key
                        and not storage_changed
                        and not force_full
                        and not full_due
                    ):
                        latest_manifest, latest_payload = load_manifest(
                            store, latest.manifest_key
                        )
                        verify_commit_marker(
                            store,
                            latest.manifest_key,
                            latest_payload,
                            latest.id,
                        )
                    source_changed = bool(
                        latest
                        and not force_full
                        and not storage_changed
                        and not full_due
                        and (
                            latest_manifest is None
                            or latest_manifest.source_fingerprint
                            != source_fingerprint
                        )
                    )
                    is_full = (
                        force_full
                        or latest is None
                        or storage_changed
                        or source_changed
                        or full_due
                    )
                    kind = (
                        SnapshotKind.FULL
                        if is_full
                        else SnapshotKind.INCREMENTAL
                    )
                    snapshot_id = str(uuid.uuid4())
                    chain_id = (
                        snapshot_id if is_full else latest.chain_id
                    )
                    parent_id = None if is_full else latest.id
                    prefix = (
                        f"v1/tasks/{task.id}/chains/{chain_id}/"
                        f"snapshots/{snapshot_id}"
                    )
                    manifest_key = f"{prefix}/manifest.json"
                    snapshot = Snapshot(
                        id=snapshot_id,
                        task_id=task.id,
                        kind=kind,
                        chain_id=chain_id,
                        parent_snapshot_id=parent_id,
                        status=SnapshotStatus.RUNNING,
                        manifest_key=manifest_key,
                        storage=task.storage,
                        compression=codec,
                        archives=[],
                        started_at=utc_now_iso(),
                    )
                    self.database.insert_snapshot(snapshot)
                    inserted = True
                    emit(
                        ProgressUpdate(
                            phase="scanning",
                            progress=2,
                            message="正在扫描源目录并比对文件元数据…",
                        )
                    )
                    previous = self.database.get_file_state(task.id)
                    prior_manifest = (
                        None if is_full else latest_manifest
                    )

                    def scan_progress(file_count: int, hashed_bytes: int) -> None:
                        self._check_cancel(cancelled, lease_guard)
                        emit(
                            ProgressUpdate(
                                phase="hashing",
                                progress=None,
                                message=f"已扫描 {file_count} 个文件",
                                stats={
                                    "files_scanned": file_count,
                                    "bytes_hashed": hashed_bytes,
                                },
                            )
                        )

                    scan = scan_source(
                        source,
                        task.excludes,
                        previous,
                        force_hash_all=is_full,
                        follow_symlinks=task.follow_symlinks,
                        cancelled=lambda: cancelled() or lease_guard.lost,
                        progress=scan_progress,
                    )
                    self._check_cancel(cancelled, lease_guard)
                    changed_files = (
                        scan.files
                        if is_full
                        else [item for item in scan.files if item.content_changed]
                    )
                    delta_workspace = tempfile.TemporaryDirectory(
                        prefix=f"easybackup-delta-{snapshot_id[:8]}-"
                    )
                    prepared_deltas = self._prepare_deltas(
                        task=task,
                        source=source,
                        store=store,
                        snapshot_id=snapshot_id,
                        chain_id=chain_id,
                        prefix=prefix,
                        changed_files=changed_files,
                        is_full=is_full,
                        workspace=Path(delta_workspace.name),
                        cancelled=lambda: cancelled() or lease_guard.lost,
                    )
                    files_to_archive = [
                        item
                        for item in changed_files
                        if item.path not in prepared_deltas
                    ]
                    delta_threshold = task.delta_threshold_mb * 1024 * 1024
                    regular_files = [
                        item
                        for item in files_to_archive
                        if item.size < delta_threshold
                    ]
                    isolated_large_files = [
                        item
                        for item in files_to_archive
                        if item.size >= delta_threshold
                    ]
                    # Large baselines/full fallbacks get their own tar object.
                    # Restoring a future delta therefore downloads only that
                    # file's base object instead of an unrelated multi-file shard.
                    shards = plan_shards(
                        regular_files,
                        task.shard_size_mb * 1024 * 1024,
                    )
                    shards.extend([item] for item in isolated_large_files)
                    emit(
                        ProgressUpdate(
                            phase="planning",
                            progress=25,
                            message=(
                                f"扫描完成：{len(scan.files)} 个文件，"
                                f"{len(changed_files)} 个内容有变化，"
                                f"{len(prepared_deltas)} 个差分补丁，"
                                f"{len(shards)} 个完整分卷"
                            ),
                            stats={
                                "files_total": len(scan.files),
                                "files_changed": len(changed_files),
                                "files_deleted": len(scan.deleted),
                                "logical_bytes": scan.total_bytes,
                                "shards": len(shards),
                                "delta_patches": len(prepared_deltas),
                            },
                        )
                    )

                    archive_location: dict[str, str] = {}
                    current_archives: list[ArchiveObject] = []
                    archived_source_bytes = 0
                    source_bytes_to_archive = sum(
                        item.size for item in files_to_archive
                    )
                    for index, shard in enumerate(shards, start=1):
                        self._check_cancel(cancelled, lease_guard)
                        archive_id = f"{snapshot_id}-volume-{index:05d}"
                        key = (
                            f"{prefix}/volumes/{archive_id}"
                            f"{extension_for(codec)}"
                        )
                        emit(
                            ProgressUpdate(
                                phase="archiving",
                                progress=25
                                + 60 * (index - 1) / max(1, len(shards)),
                                message=f"正在生成并上传分卷 {index}/{len(shards)}",
                                stats={
                                    "shard_index": index,
                                    "shard_total": len(shards),
                                    "uploaded_bytes": sum(
                                        value.integrity.size
                                        for value in current_archives
                                    ),
                                },
                            )
                        )
                        with create_archive_stream(
                            source, shard, codec, task.compression_level
                        ) as raw_stream:
                            hashing_stream = IntegrityReader(
                                raw_stream, self.integrity_block_size
                            )
                            # Register the key before upload.  A store may have
                            # durably accepted the object even if the archive
                            # producer fails while its context is closing.
                            # Deletion is idempotent for both local and S3.
                            uploaded_keys.append(key)
                            stored = store.put_stream(
                                key,
                                hashing_stream,
                                cancelled=lambda: cancelled() or lease_guard.lost,
                                metadata={
                                    "easybackup-snapshot": snapshot_id,
                                    "easybackup-volume": archive_id,
                                },
                            )
                            integrity = hashing_stream.integrity
                        if stored.size != integrity.size:
                            raise ConflictError(
                                f"分卷 {archive_id} 上传后大小不一致。"
                            )
                        current_archives.append(
                            ArchiveObject(
                                id=archive_id,
                                snapshot_id=snapshot_id,
                                key=key,
                                compression=codec,
                                integrity=integrity,
                                file_count=len(shard),
                            )
                        )
                        for item in shard:
                            archive_location[item.path] = archive_id
                        archived_source_bytes += sum(item.size for item in shard)
                        emit(
                            ProgressUpdate(
                                phase="uploading",
                                progress=25
                                + 60 * index / max(1, len(shards)),
                                message=f"分卷 {index}/{len(shards)} 已提交",
                                stats={
                                    "source_bytes_done": archived_source_bytes,
                                    "source_bytes_total": source_bytes_to_archive,
                                    "stored_bytes": sum(
                                        value.integrity.size
                                        for value in current_archives
                                    ),
                                },
                            )
                        )

                    for prepared in prepared_deltas.values():
                        self._check_cancel(cancelled, lease_guard)
                        with prepared.compressed_path.open("rb") as raw_stream:
                            hashing_stream = IntegrityReader(
                                raw_stream,
                                self.integrity_block_size,
                            )
                            uploaded_keys.append(prepared.object_key)
                            stored = store.put_stream(
                                prepared.object_key,
                                hashing_stream,
                                cancelled=lambda: (
                                    cancelled() or lease_guard.lost
                                ),
                                metadata={
                                    "easybackup-snapshot": snapshot_id,
                                    "easybackup-volume": prepared.archive_id,
                                    "easybackup-artifact": "xdelta3-vcdiff-zstd",
                                },
                            )
                            integrity = hashing_stream.integrity
                        if stored.size != integrity.size:
                            raise ConflictError(
                                f"差分补丁 {prepared.archive_id} 上传后大小不一致。"
                            )
                        current_archives.append(
                            ArchiveObject(
                                id=prepared.archive_id,
                                snapshot_id=snapshot_id,
                                key=prepared.object_key,
                                compression="zstd",
                                integrity=integrity,
                                file_count=1,
                            )
                        )
                        archive_location[prepared.item.path] = prepared.archive_id

                    assert_files_unchanged(changed_files, source)
                    archive_by_id = {
                        archive.id: archive for archive in current_archives
                    }
                    current_files: list[ManifestFile] = []
                    current_versions: list[FileVersion] = []
                    scanned_by_path = {
                        item.path: item for item in scan.files
                    }
                    for item in scan.files:
                        if is_full or item.content_changed:
                            origin = snapshot_id
                            archive_id = archive_location[item.path]
                            file_version = None
                            if item.size >= delta_threshold:
                                archive = archive_by_id[archive_id]
                                prepared = prepared_deltas.get(item.path)
                                version = FileVersion(
                                    task_id=task.id,
                                    chain_id=chain_id,
                                    file_path=item.path,
                                    snapshot_id=snapshot_id,
                                    kind=(
                                        FileVersionKind.DELTA
                                        if prepared
                                        else FileVersionKind.FULL
                                    ),
                                    base_version_id=(
                                        prepared.baseline.id
                                        if prepared
                                        else None
                                    ),
                                    archive_id=archive.id,
                                    object_key=archive.key,
                                    compression=archive.compression,
                                    original_size=item.size,
                                    transfer_size=archive.integrity.size,
                                    sha256=item.sha256,
                                )
                                current_versions.append(version)
                                file_version = version.as_reference(
                                    base=(
                                        prepared.baseline.as_base_reference()
                                        if prepared
                                        else None
                                    )
                                )
                        else:
                            previous_file = previous.get(item.path)
                            if previous_file is None:
                                raise ConflictError(
                                    f"文件 {item.path!r} 缺少上次快照内容引用。"
                                )
                            origin = previous_file.origin_snapshot_id
                            archive_id = previous_file.archive_id
                            file_version = previous_file.file_version
                        current_files.append(
                            ManifestFile(
                                path=item.path,
                                size=item.size,
                                mtime_ns=item.mtime_ns,
                                mode=item.mode,
                                sha256=item.sha256,
                                origin_snapshot_id=origin,
                                archive_id=archive_id,
                                file_version=file_version,
                            )
                        )

                    for version in current_versions:
                        if version.kind != FileVersionKind.FULL:
                            continue
                        try:
                            self.database.get_chain_baseline(
                                task.id,
                                version.file_path,
                                chain_id,
                            )
                        except NotFoundError:
                            self._cache_new_baseline(
                                task,
                                chain_id,
                                version,
                                scanned_by_path[version.file_path],
                                source,
                                cancelled=lambda: (
                                    cancelled() or lease_guard.lost
                                ),
                            )

                    referenced_archives = list(current_archives)
                    referenced_archives.extend(
                        prepared.baseline_archive
                        for prepared in prepared_deltas.values()
                    )
                    if prior_manifest:
                        referenced_archives.extend(prior_manifest.archives)
                    referenced_identities = {
                        (item.origin_snapshot_id, item.archive_id)
                        for item in current_files
                    }
                    referenced_identities.update(
                        (
                            item.file_version.base.snapshot_id,
                            item.file_version.base.archive_id,
                        )
                        for item in current_files
                        if (
                            item.file_version is not None
                            and item.file_version.base is not None
                        )
                    )
                    deduplicated: dict[tuple[str, str], ArchiveObject] = {}
                    for archive in referenced_archives:
                        identity = (archive.snapshot_id, archive.id)
                        if identity in referenced_identities:
                            deduplicated[identity] = archive

                    manifest = SnapshotManifest(
                        snapshot_id=snapshot_id,
                        task_id=task.id,
                        kind=kind,
                        chain_id=chain_id,
                        parent_snapshot_id=parent_id,
                        created_at=utc_now_iso(),
                        source_path=source.name,
                        source_fingerprint=source_fingerprint,
                        archives=sorted(
                            deduplicated.values(),
                            key=lambda item: (item.snapshot_id, item.id),
                        ),
                        files=current_files,
                        directories=scan.directories,
                        deleted=scan.deleted,
                        skipped=scan.skipped,
                        archive_integrity={
                            "sha256": sha256_bytes(
                                stable_json_bytes(
                                    [
                                        item.integrity.sha256
                                        for item in current_archives
                                    ]
                                )
                            ),
                            "size": sum(
                                item.integrity.size for item in current_archives
                            ),
                            "block_size": self.integrity_block_size,
                            "crc32": [],
                        },
                    )
                    manifest_payload = stable_json_bytes(manifest)
                    manifest_sha256 = sha256_bytes(manifest_payload)
                    emit(
                        ProgressUpdate(
                            phase="publishing",
                            progress=90,
                            message="正在发布 Manifest…",
                        )
                    )
                    store.put_bytes(
                        manifest_key,
                        manifest_payload,
                        metadata={"easybackup-sha256": manifest_sha256},
                    )
                    uploaded_keys.append(manifest_key)
                    self._check_cancel(cancelled, lease_guard)

                    commit_payload = stable_json_bytes(
                        {
                            "version": 1,
                            "snapshot_id": snapshot_id,
                            "task_id": task.id,
                            "chain_id": chain_id,
                            "manifest_key": manifest_key,
                            "manifest_sha256": manifest_sha256,
                            "archives": [
                                {
                                    "key": item.key,
                                    "sha256": item.integrity.sha256,
                                    "size": item.integrity.size,
                                }
                                for item in current_archives
                            ],
                            "lease_token": (
                                lease_guard.lease.token
                                if lease_guard.lease
                                else None
                            ),
                            "committed_at": utc_now_iso(),
                        }
                    )
                    commit_key = commit_key_for(manifest_key)
                    # Close the heartbeat/suspend race as far as a TTL lease
                    # permits: synchronously prove ownership immediately
                    # before the externally visible Commit write.
                    lease_guard.ensure_valid()
                    # A timed-out PUT may still have published the object.
                    # Track it before the call so an uncommitted error path
                    # always removes Commit before manifest and volumes.
                    uploaded_keys.append(commit_key)
                    store.put_bytes(commit_key, commit_payload)
                    commit_published = True
                    snapshot = snapshot.model_copy(
                        update={
                            "status": SnapshotStatus.COMPLETED,
                            "archives": current_archives,
                            "file_count": len(current_files),
                            "changed_count": len(changed_files),
                            "deleted_count": len(scan.deleted),
                            "archive_size": sum(
                                item.integrity.size for item in current_archives
                            ),
                            "archive_sha256": sha256_bytes(
                                stable_json_bytes(
                                    [
                                        item.integrity.sha256
                                        for item in current_archives
                                    ]
                                )
                            ),
                            "integrity": {
                                "manifest_sha256": manifest_sha256,
                                "commit_key": commit_key,
                            },
                            "completed_at": utc_now_iso(),
                        }
                    )
                    self.database.commit_snapshot(
                        snapshot,
                        current_files,
                        current_versions,
                    )
                    emit(
                        ProgressUpdate(
                            phase="completed",
                            progress=100,
                            message="备份快照已安全提交。",
                            stats={
                                "snapshot_id": snapshot_id,
                                "file_count": len(current_files),
                                "changed_count": len(changed_files),
                                "archive_size": snapshot.archive_size,
                                "delta_patches": len(prepared_deltas),
                            },
                        )
                    )
                    return snapshot
                except Exception as exc:
                    if inserted and not commit_published:
                        cleanup_succeeded = True
                        try:
                            lease_guard.ensure_valid()
                        except Exception:
                            cleanup_succeeded = False
                            logger.warning(
                                "未能确认远端租约，保留运行中快照供启动对账"
                            )
                        if cleanup_succeeded:
                            for key in reversed(uploaded_keys):
                                try:
                                    store.delete(key)
                                except Exception:
                                    cleanup_succeeded = False
                                    logger.warning(
                                        "无法清理未提交对象 %s", key
                                    )
                        if cleanup_succeeded:
                            try:
                                self.database.fail_snapshot(
                                    snapshot_id, str(exc)
                                )
                            except Exception:
                                logger.exception("更新失败快照状态时出错")
                        else:
                            logger.warning(
                                "快照 %s 保持 running，重启时将安全对账并清理",
                                snapshot_id,
                            )
                    raise
                finally:
                    if delta_workspace is not None:
                        try:
                            delta_workspace.cleanup()
                        except OSError:
                            logger.warning(
                                "无法立即清理差分操作临时目录 %s",
                                delta_workspace.name,
                            )

    def _prepare_deltas(
        self,
        *,
        task: Task,
        source: Path,
        store,
        snapshot_id: str,
        chain_id: str,
        prefix: str,
        changed_files: list[ScannedFile],
        is_full: bool,
        workspace: Path,
        cancelled: CancelCallback,
    ) -> dict[str, _PreparedDelta]:
        """Create verified Base-relative patches before any remote publication."""

        if is_full or not task.delta_enabled:
            return {}
        xdelta3 = find_xdelta3(self.xdelta3_path)
        if not xdelta3 or not find_tool("zstd"):
            missing = "xdelta3" if not xdelta3 else "zstd"
            logger.warning(
                "未找到 %s，大文件将安全回退为完整归档。", missing
            )
            return {}

        threshold = task.delta_threshold_mb * 1024 * 1024
        candidates = sorted(
            (
                item
                for item in changed_files
                if item.size >= threshold
            ),
            key=lambda item: item.path,
        )
        prepared: dict[str, _PreparedDelta] = {}
        archive_cache: dict[tuple[str, str], ArchiveObject] = {}
        for index, item in enumerate(candidates, start=1):
            if cancelled():
                raise CancelledError("操作已取消。")
            try:
                baseline = self.database.get_chain_baseline(
                    task.id,
                    item.path,
                    chain_id,
                )
            except NotFoundError:
                # A large file first seen during an incremental snapshot becomes
                # this path's baseline; a later version can delta against it.
                continue

            identity = (baseline.snapshot_id, baseline.archive_id)
            baseline_archive = archive_cache.get(identity)
            try:
                if baseline_archive is None:
                    baseline_archive = self._load_baseline_archive(
                        store,
                        baseline,
                    )
                    archive_cache[identity] = baseline_archive
                baseline_path = self._materialize_baseline(
                    store,
                    task.id,
                    chain_id,
                    baseline,
                    baseline_archive,
                    workspace,
                    cancelled,
                )
            except (StorageError, ValidationError) as exc:
                logger.warning(
                    "无法读取 %s 的差分基线，已回退完整归档：%s",
                    item.path,
                    exc,
                )
                continue

            staged_current = workspace / f"current-{index:05d}.bin"
            raw_patch = workspace / f"patch-{index:05d}.vcdiff"
            compressed_patch = workspace / f"patch-{index:05d}.vcdiff.zst"
            verification_output = workspace / f"verify-{index:05d}.bin"
            retain_compressed_patch = False
            try:
                # Source staging errors are not a delta-efficiency failure:
                # they mean the scanned source version changed and must abort
                # the snapshot.
                stage_scanned_file(
                    item,
                    source,
                    staged_current,
                    cancelled=cancelled,
                )
                try:
                    raw_integrity = create_delta_patch(
                        baseline_path,
                        staged_current,
                        raw_patch,
                        executable=xdelta3,
                        max_output_size=(
                            item.size
                            + DEFAULT_PATCH_OUTPUT_OVERHEAD_BYTES
                        ),
                        cancelled=cancelled,
                    )
                    compressed_integrity = compress_patch(
                        raw_patch,
                        compressed_patch,
                        level=task.compression_level,
                        cancelled=cancelled,
                    )
                    # Never publish a patch without proving that this exact
                    # Base + Patch reconstructs the scanned current bytes.
                    apply_delta_patch(
                        baseline_path,
                        raw_patch,
                        verification_output,
                        expected_size=item.size,
                        expected_sha256=item.sha256,
                        executable=xdelta3,
                        cancelled=cancelled,
                    )
                except CancelledError:
                    raise
                except (
                    StorageError,
                    ToolMissingError,
                    ValidationError,
                ) as exc:
                    logger.warning(
                        "文件 %s 的差分生成/验证失败，已回退完整归档：%s",
                        item.path,
                        exc,
                    )
                    continue
                if compressed_integrity.size >= int(
                    item.size * task.delta_max_ratio
                ):
                    logger.info(
                        "文件 %s 的压缩补丁占原文件 %.1f%%，超过 %.1f%% "
                        "阈值，已回退完整归档。",
                        item.path,
                        100
                        * compressed_integrity.size
                        / max(1, item.size),
                        100 * task.delta_max_ratio,
                    )
                    continue

                archive_id = (
                    f"{snapshot_id}-delta-{len(prepared) + 1:05d}"
                )
                object_key = (
                    f"{prefix}/patches/{archive_id}.vcdiff.zst"
                )
                prepared[item.path] = _PreparedDelta(
                    item=item,
                    baseline=baseline,
                    baseline_archive=baseline_archive,
                    compressed_path=compressed_patch,
                    raw_size=raw_integrity.size,
                    archive_id=archive_id,
                    object_key=object_key,
                )
                retain_compressed_patch = True
            finally:
                disposable = [
                    staged_current,
                    raw_patch,
                    verification_output,
                ]
                if not retain_compressed_patch:
                    disposable.append(compressed_patch)
                for artifact in disposable:
                    try:
                        artifact.unlink(missing_ok=True)
                    except OSError:
                        logger.warning(
                            "无法立即清理差分临时文件 %s",
                            artifact,
                        )
        return prepared

    def _load_baseline_archive(
        self,
        store,
        baseline: FileVersion,
    ) -> ArchiveObject:
        snapshot = self.database.get_snapshot(baseline.snapshot_id)
        if not snapshot.manifest_key:
            raise StorageError("差分基线快照没有 Manifest。")
        manifest, payload = load_manifest(store, snapshot.manifest_key)
        verify_commit_marker(
            store,
            snapshot.manifest_key,
            payload,
            snapshot.id,
        )
        archive = next(
            (
                item
                for item in manifest.archives
                if (
                    item.snapshot_id == baseline.snapshot_id
                    and item.id == baseline.archive_id
                )
            ),
            None,
        )
        if archive is None:
            raise StorageError(
                f"差分基线 {baseline.id} 引用的完整分卷不存在。"
            )
        if (
            archive.key != baseline.object_key
            or archive.compression != baseline.compression
        ):
            raise StorageError(
                f"差分基线 {baseline.id} 的对象定位信息不一致。"
            )
        return archive

    def _materialize_baseline(
        self,
        store,
        task_id: str,
        chain_id: str,
        baseline: FileVersion,
        baseline_archive: ArchiveObject,
        workspace: Path,
        cancelled: CancelCallback,
    ) -> Path:
        remote = store.stat(baseline_archive.key)
        if (
            remote is None
            or remote.size != baseline_archive.integrity.size
        ):
            raise StorageError(
                f"远端差分基线对象 {baseline_archive.key!r} 缺失或大小不一致；"
                "已拒绝创建依赖它的新 Patch。"
            )
        cache_path = self._baseline_cache_path(
            task_id,
            chain_id,
            baseline.id,
        )
        if cache_path.is_file():
            try:
                verify_file(
                    cache_path,
                    expected_size=baseline.original_size,
                    expected_sha256=baseline.sha256,
                    cancelled=cancelled,
                )
                return cache_path
            except (StorageError, ValidationError):
                try:
                    cache_path.unlink(missing_ok=True)
                except OSError as exc:
                    logger.warning(
                        "无法删除损坏的差分基线缓存，将尝试原子替换：%s",
                        exc,
                    )

        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            extract_tar_member(
                store,
                object_key=baseline.object_key,
                compression=baseline.compression,
                member_path=baseline.file_path,
                destination=cache_path,
                expected_size=baseline.original_size,
                expected_sha256=baseline.sha256,
                cancelled=cancelled,
            )
            return cache_path
        except CancelledError:
            raise
        except (OSError, StorageError) as cache_error:
            # Cache is an optimization.  A read-only/full cache must not make
            # a valid remote baseline unusable.
            logger.warning(
                "无法写入差分基线缓存，改用本次操作临时目录：%s",
                cache_error,
            )
            temporary_base = workspace / (
                f"base-{sha256_bytes(baseline.id.encode('utf-8'))}.bin"
            )
            extract_tar_member(
                store,
                object_key=baseline.object_key,
                compression=baseline.compression,
                member_path=baseline.file_path,
                destination=temporary_base,
                expected_size=baseline.original_size,
                expected_sha256=baseline.sha256,
                cancelled=cancelled,
            )
            return temporary_base

    def _cache_new_baseline(
        self,
        task: Task,
        chain_id: str,
        version: FileVersion,
        item: ScannedFile,
        source: Path,
        *,
        cancelled: CancelCallback,
    ) -> None:
        cache_path = self._baseline_cache_path(
            task.id,
            chain_id,
            version.id,
        )
        try:
            if cache_path.is_file():
                verify_file(
                    cache_path,
                    expected_size=version.original_size,
                    expected_sha256=version.sha256,
                    cancelled=cancelled,
                )
                return
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            stage_scanned_file(
                item,
                source,
                cache_path,
                cancelled=cancelled,
            )
        except CancelledError:
            raise
        except (OSError, StorageError, ValidationError) as exc:
            # The immutable remote full object is authoritative; the cache can
            # always be rebuilt from it on the next incremental run.
            logger.warning(
                "无法缓存新基线 %s（备份仍可提交）：%s",
                version.file_path,
                exc,
            )

    def _baseline_cache_path(
        self,
        task_id: str,
        chain_id: str,
        version_id: str,
    ) -> Path:
        task_component = sha256_bytes(task_id.encode("utf-8"))[:32]
        chain_component = sha256_bytes(chain_id.encode("utf-8"))[:32]
        version_component = sha256_bytes(version_id.encode("utf-8"))
        return (
            self.delta_cache_dir
            / task_component
            / chain_component
            / f"{version_component}.base"
        )

    @staticmethod
    def _check_cancel(
        cancelled: CancelCallback,
        lease_guard: LeaseGuard,
    ) -> None:
        if cancelled():
            raise CancelledError("操作已取消。")
        if lease_guard.lost:
            raise ConflictError("远端租约已丢失，已阻止快照提交。")
