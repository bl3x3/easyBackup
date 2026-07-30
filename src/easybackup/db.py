"""SQLite persistence for EasyBackup.

The database is intentionally accessed through short-lived connections.  The
only exception is the keeper connection used for a shared ``:memory:`` database
in tests; all actual operations still use their own connection.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from easybackup.errors import ConflictError, NotFoundError
from easybackup.models import (
    ArchiveObject,
    FileVersion,
    FileVersionKind,
    FileVersionReference,
    ManifestFile,
    Operation,
    OperationKind,
    OperationStatus,
    Snapshot,
    SnapshotKind,
    SnapshotStatus,
    S3StorageConfig,
    StorageConfig,
    Task,
    TaskCreate,
    TaskUpdate,
    utc_now_iso,
)


_SCHEMA_VERSION = "2"
_STORAGE_ADAPTER = TypeAdapter(StorageConfig)
_ARCHIVES_ADAPTER = TypeAdapter(list[ArchiveObject])
_EXCLUDES_ADAPTER = TypeAdapter(list[str])
_DICT_ADAPTER = TypeAdapter(dict[str, Any])


_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source_path TEXT NOT NULL,
    storage TEXT NOT NULL,
    schedule TEXT,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    excludes TEXT NOT NULL,
    compression TEXT NOT NULL
        CHECK (compression IN ('auto', 'zstd', 'gzip', 'none')),
    compression_level INTEGER NOT NULL
        CHECK (compression_level BETWEEN 1 AND 19),
    shard_size_mb INTEGER NOT NULL CHECK (shard_size_mb >= 8),
    full_every INTEGER NOT NULL CHECK (full_every >= 1),
    retention_chains INTEGER NOT NULL CHECK (retention_chains >= 1),
    retention_days INTEGER NOT NULL CHECK (retention_days >= 1),
    follow_symlinks INTEGER NOT NULL CHECK (follow_symlinks IN (0, 1)),
    delta_enabled INTEGER NOT NULL DEFAULT 1
        CHECK (delta_enabled IN (0, 1)),
    delta_threshold_mb INTEGER NOT NULL DEFAULT 100
        CHECK (delta_threshold_mb >= 1),
    delta_max_ratio REAL NOT NULL DEFAULT 0.9
        CHECK (delta_max_ratio > 0 AND delta_max_ratio <= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL
        REFERENCES tasks(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('full', 'incremental')),
    chain_id TEXT NOT NULL,
    parent_snapshot_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    manifest_key TEXT,
    storage TEXT NOT NULL,
    compression TEXT NOT NULL CHECK (compression IN ('zstd', 'gzip', 'none')),
    archives TEXT NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0 CHECK (file_count >= 0),
    changed_count INTEGER NOT NULL DEFAULT 0 CHECK (changed_count >= 0),
    deleted_count INTEGER NOT NULL DEFAULT 0 CHECK (deleted_count >= 0),
    archive_size INTEGER NOT NULL DEFAULT 0 CHECK (archive_size >= 0),
    archive_sha256 TEXT,
    integrity TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT,
    last_verified_at TEXT,
    verify_status TEXT
);

CREATE INDEX IF NOT EXISTS idx_snapshots_task_started
    ON snapshots(task_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_task_status_kind
    ON snapshots(task_id, status, kind);
CREATE INDEX IF NOT EXISTS idx_snapshots_chain
    ON snapshots(task_id, chain_id);

CREATE TABLE IF NOT EXISTS file_state (
    task_id TEXT NOT NULL
        REFERENCES tasks(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    mtime_ns INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    origin_snapshot_id TEXT NOT NULL,
    archive_id TEXT NOT NULL,
    file_version TEXT,
    PRIMARY KEY (task_id, path)
);

CREATE INDEX IF NOT EXISTS idx_file_state_origin
    ON file_state(task_id, origin_snapshot_id);

CREATE TABLE IF NOT EXISTS file_versions (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL
        REFERENCES tasks(id) ON DELETE CASCADE,
    chain_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    snapshot_id TEXT NOT NULL
        REFERENCES snapshots(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('full', 'delta')),
    base_version_id TEXT,
    archive_id TEXT NOT NULL,
    object_key TEXT NOT NULL,
    compression TEXT NOT NULL CHECK (compression IN ('zstd', 'gzip', 'none')),
    original_size INTEGER NOT NULL CHECK (original_size >= 0),
    transfer_size INTEGER NOT NULL CHECK (transfer_size >= 0),
    sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (snapshot_id, file_path),
    CHECK (
        (kind = 'full' AND base_version_id IS NULL)
        OR (kind = 'delta' AND base_version_id IS NOT NULL)
    ),
    FOREIGN KEY(base_version_id) REFERENCES file_versions(id)
        ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_file_versions_lookup
    ON file_versions(task_id, chain_id, file_path, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_file_versions_snapshot
    ON file_versions(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_file_versions_base
    ON file_versions(base_version_id);

CREATE TABLE IF NOT EXISTS operations (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL
        REFERENCES tasks(id) ON DELETE CASCADE,
    snapshot_id TEXT
        REFERENCES snapshots(id) ON DELETE SET NULL,
    kind TEXT NOT NULL CHECK (kind IN ('backup', 'restore', 'scrub', 'prune')),
    status TEXT NOT NULL
        CHECK (status IN (
            'queued', 'running', 'cancelling',
            'completed', 'failed', 'cancelled'
        )),
    progress REAL CHECK (progress IS NULL OR (progress >= 0 AND progress <= 100)),
    phase TEXT NOT NULL,
    message TEXT NOT NULL,
    stats TEXT NOT NULL,
    error TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_operations_task_created
    ON operations(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_operations_status
    ON operations(status);
"""


def _json_dump(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_load(value: str) -> Any:
    return json.loads(value)


def _new_id() -> str:
    return str(uuid.uuid4())


def _chunks(values: Sequence[str], size: int = 900) -> Iterator[Sequence[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


class Database:
    """Synchronous, short-connection SQLite repository."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms 不能小于 0")

        raw_path = str(path)
        self.path = Path(raw_path) if raw_path != ":memory:" else Path(":memory:")
        self.busy_timeout_ms = int(busy_timeout_ms)
        self._memory_uri: str | None = None
        self._memory_keeper: sqlite3.Connection | None = None
        self._keeper_lock = threading.Lock()

        if raw_path == ":memory:":
            self._memory_uri = (
                f"file:easybackup-{uuid.uuid4().hex}?mode=memory&cache=shared"
            )

    def initialize(self) -> None:
        """Create or atomically migrate the database schema."""

        if self._memory_uri is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        else:
            self._ensure_memory_keeper()

        with self._connection() as connection:
            # WAL is persistent for a file database.  SQLite correctly returns
            # "memory" for an in-memory database, where WAL is unavailable.
            connection.execute("PRAGMA journal_mode = WAL")
            has_schema_meta = connection.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = 'schema_meta'
                """
            ).fetchone()
            if has_schema_meta is None:
                existing_table = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                      AND name NOT LIKE 'sqlite_%'
                    LIMIT 1
                    """
                ).fetchone()
                if existing_table is not None:
                    raise RuntimeError(
                        "数据库包含表但缺少 schema_meta，无法安全推断版本。"
                    )
                self._install_schema(connection, set_version=True)
                return

            row = connection.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            found = None if row is None else str(row["value"])
            if found == "1":
                self._migrate_v1_to_v2(connection)
                found = _SCHEMA_VERSION
            if found != _SCHEMA_VERSION:
                raise RuntimeError(
                    f"不支持的数据库 schema 版本：{found!r}，"
                    f"当前版本为 {_SCHEMA_VERSION}"
                )
            # Re-run idempotent DDL so indexes introduced by a patch release
            # are also repaired without changing the schema version.
            self._install_schema(connection)

    @staticmethod
    def _install_schema(
        connection: sqlite3.Connection,
        *,
        set_version: bool = False,
    ) -> None:
        statements = ["BEGIN IMMEDIATE;", _SCHEMA]
        if set_version:
            statements.append(
                "INSERT INTO schema_meta(key, value) "
                f"VALUES ('schema_version', '{_SCHEMA_VERSION}');"
            )
        statements.append("COMMIT;")
        try:
            connection.executescript("\n".join(statements))
        except BaseException:
            connection.rollback()
            raise

    @staticmethod
    def _migrate_v1_to_v2(connection: sqlite3.Connection) -> None:
        """Add large-file delta metadata without rebuilding existing tables."""

        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                ALTER TABLE tasks
                ADD COLUMN delta_enabled INTEGER NOT NULL DEFAULT 1
                    CHECK (delta_enabled IN (0, 1))
                """
            )
            connection.execute(
                """
                ALTER TABLE tasks
                ADD COLUMN delta_threshold_mb INTEGER NOT NULL DEFAULT 100
                    CHECK (delta_threshold_mb >= 1)
                """
            )
            connection.execute(
                """
                ALTER TABLE tasks
                ADD COLUMN delta_max_ratio REAL NOT NULL DEFAULT 0.9
                    CHECK (delta_max_ratio > 0 AND delta_max_ratio <= 1)
                """
            )
            connection.execute(
                "ALTER TABLE file_state ADD COLUMN file_version TEXT"
            )
            connection.execute(
                """
                CREATE TABLE file_versions (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL
                        REFERENCES tasks(id) ON DELETE CASCADE,
                    chain_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL
                        REFERENCES snapshots(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL CHECK (kind IN ('full', 'delta')),
                    base_version_id TEXT,
                    archive_id TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    compression TEXT NOT NULL
                        CHECK (compression IN ('zstd', 'gzip', 'none')),
                    original_size INTEGER NOT NULL CHECK (original_size >= 0),
                    transfer_size INTEGER NOT NULL CHECK (transfer_size >= 0),
                    sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (snapshot_id, file_path),
                    CHECK (
                        (kind = 'full' AND base_version_id IS NULL)
                        OR (kind = 'delta' AND base_version_id IS NOT NULL)
                    ),
                    FOREIGN KEY(base_version_id) REFERENCES file_versions(id)
                        ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX idx_file_versions_lookup
                ON file_versions(
                    task_id, chain_id, file_path, created_at DESC
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX idx_file_versions_snapshot
                ON file_versions(snapshot_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX idx_file_versions_base
                ON file_versions(base_version_id)
                """
            )
            connection.execute(
                """
                UPDATE schema_meta
                SET value = ?
                WHERE key = 'schema_version'
                """,
                (_SCHEMA_VERSION,),
            )
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()

    def close(self) -> None:
        """Close the in-memory keeper; file databases hold no open connection."""

        with self._keeper_lock:
            if self._memory_keeper is not None:
                self._memory_keeper.close()
                self._memory_keeper = None

    def create_task(self, task: TaskCreate) -> Task:
        now = utc_now_iso()
        record = Task.model_validate(
            {
                **task.model_dump(mode="python"),
                "id": _new_id(),
                "created_at": now,
                "updated_at": now,
            }
        )
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO tasks (
                        id, name, source_path, storage, schedule, enabled,
                        excludes, compression, compression_level, shard_size_mb,
                        full_every, retention_chains, retention_days,
                        follow_symlinks, delta_enabled, delta_threshold_mb,
                        delta_max_ratio, created_at, updated_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    self._task_values(record),
                )
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("创建任务", exc)
        return record

    def list_tasks(self) -> list[Task]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM tasks ORDER BY created_at ASC, rowid ASC"
            ).fetchall()
        return [self._task_from_row(row) for row in rows]

    def get_task(self, task_id: str) -> Task:
        with self._connection() as connection:
            return self._require_task(connection, task_id)

    def update_task(self, task_id: str, update: TaskUpdate) -> Task:
        try:
            with self._transaction() as connection:
                current = self._require_task(connection, task_id)
                changes = update.model_dump(exclude_unset=True, mode="python")
                # ``None`` clears the nullable schedule.  For all other fields,
                # it means that a PATCH field has no replacement value.
                changes = {
                    key: value
                    for key, value in changes.items()
                    if value is not None or key == "schedule"
                }
                if not changes:
                    return current

                payload = current.model_dump(mode="python")
                payload.update(changes)
                payload["updated_at"] = utc_now_iso()
                updated = Task.model_validate(payload)
                values = self._task_values(updated)
                connection.execute(
                    """
                    UPDATE tasks SET
                        name = ?, source_path = ?, storage = ?, schedule = ?,
                        enabled = ?, excludes = ?, compression = ?,
                        compression_level = ?, shard_size_mb = ?, full_every = ?,
                        retention_chains = ?, retention_days = ?,
                        follow_symlinks = ?, delta_enabled = ?,
                        delta_threshold_mb = ?, delta_max_ratio = ?,
                        created_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (*values[1:], task_id),
                )
                if updated.source_path != current.source_path:
                    # Never reuse fast-scan state from a different root.  This
                    # is atomic with the task update; the backup engine also
                    # starts a new full chain using the manifest fingerprint.
                    connection.execute(
                        "DELETE FROM file_state WHERE task_id = ?",
                        (task_id,),
                    )
                return updated
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("更新任务", exc)

    def delete_task(self, task_id: str) -> None:
        try:
            with self._transaction() as connection:
                self._require_task(connection, task_id)
                snapshot = connection.execute(
                    """
                    SELECT id FROM snapshots
                    WHERE task_id = ?
                      AND status IN ('running', 'completed')
                    LIMIT 1
                    """,
                    (task_id,),
                ).fetchone()
                if snapshot is not None:
                    raise ConflictError(
                        "任务仍有已完成或待对账快照，为防止丢失本地恢复索引，"
                        "不能直接删除；请停用任务并保留元数据。"
                    )
                connection.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("删除任务", exc)

    def create_operation(
        self,
        task_id: str,
        kind: OperationKind | str,
        snapshot_id: str | None = None,
    ) -> Operation:
        operation_kind = OperationKind(kind)
        now = utc_now_iso()
        operation = Operation(
            id=_new_id(),
            task_id=task_id,
            snapshot_id=snapshot_id,
            kind=operation_kind,
            status=OperationStatus.QUEUED,
            progress=None,
            phase="queued",
            message="",
            stats={},
            created_at=now,
        )
        try:
            with self._transaction() as connection:
                self._require_task(connection, task_id)
                if snapshot_id is not None:
                    snapshot = self._require_snapshot(connection, snapshot_id)
                    if snapshot.task_id != task_id:
                        raise ConflictError(
                            "操作引用的快照不属于指定任务",
                            details={
                                "task_id": task_id,
                                "snapshot_id": snapshot_id,
                            },
                        )
                connection.execute(
                    """
                    INSERT INTO operations (
                        id, task_id, snapshot_id, kind, status, progress, phase,
                        message, stats, error, created_at, started_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    self._operation_values(operation),
                )
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("创建操作", exc)
        return operation

    def update_operation(self, operation_id: str, **fields: Any) -> Operation:
        allowed = {
            "snapshot_id",
            "status",
            "progress",
            "phase",
            "message",
            "stats",
            "error",
            "started_at",
            "completed_at",
        }
        unknown = sorted(set(fields) - allowed)
        if unknown:
            raise ValueError(f"不允许更新操作字段：{', '.join(unknown)}")

        try:
            with self._transaction() as connection:
                current = self._require_operation(connection, operation_id)
                if not fields:
                    return current

                payload = current.model_dump(mode="python")
                payload.update(fields)
                status = OperationStatus(payload["status"])
                payload["status"] = status
                now = utc_now_iso()
                if (
                    status == OperationStatus.RUNNING
                    and "started_at" not in fields
                    and current.started_at is None
                ):
                    payload["started_at"] = now
                if (
                    status
                    in {
                        OperationStatus.COMPLETED,
                        OperationStatus.FAILED,
                        OperationStatus.CANCELLED,
                    }
                    and "completed_at" not in fields
                    and current.completed_at is None
                ):
                    payload["completed_at"] = now

                updated = Operation.model_validate(payload)
                if updated.snapshot_id is not None:
                    snapshot = self._require_snapshot(
                        connection, updated.snapshot_id
                    )
                    if snapshot.task_id != updated.task_id:
                        raise ConflictError(
                            "操作引用的快照不属于该任务",
                            details={
                                "task_id": updated.task_id,
                                "snapshot_id": updated.snapshot_id,
                            },
                        )

                values = self._operation_values(updated)
                connection.execute(
                    """
                    UPDATE operations SET
                        task_id = ?, snapshot_id = ?, kind = ?, status = ?,
                        progress = ?, phase = ?, message = ?, stats = ?,
                        error = ?, created_at = ?, started_at = ?,
                        completed_at = ?
                    WHERE id = ?
                    """,
                    (*values[1:], operation_id),
                )
                return updated
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("更新操作", exc)

    def get_operation(self, operation_id: str) -> Operation:
        with self._connection() as connection:
            return self._require_operation(connection, operation_id)

    def list_operations(
        self,
        task_id: str | None = None,
        limit: int = 100,
    ) -> list[Operation]:
        self._validate_limit(limit)
        with self._connection() as connection:
            if task_id is None:
                rows = connection.execute(
                    """
                    SELECT * FROM operations
                    ORDER BY created_at DESC, rowid DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            else:
                self._require_task(connection, task_id)
                rows = connection.execute(
                    """
                    SELECT * FROM operations
                    WHERE task_id = ?
                    ORDER BY created_at DESC, rowid DESC
                    LIMIT ?
                    """,
                    (task_id, limit),
                ).fetchall()
        return [self._operation_from_row(row) for row in rows]

    def mark_interrupted_operations(self) -> int:
        now = utc_now_iso()
        interruption = "应用重启，操作被中断。"
        try:
            with self._transaction() as connection:
                cursor = connection.execute(
                    """
                    UPDATE operations
                    SET status = 'failed',
                        phase = 'failed',
                        message = ?,
                        error = ?,
                        completed_at = ?
                    WHERE status IN ('queued', 'running', 'cancelling')
                    """,
                    (interruption, interruption, now),
                )
                return cursor.rowcount
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("标记中断操作", exc)

    def insert_snapshot(self, snapshot: Snapshot) -> Snapshot:
        snapshot = Snapshot.model_validate(snapshot)
        try:
            with self._transaction() as connection:
                self._require_task(connection, snapshot.task_id)
                connection.execute(
                    """
                    INSERT INTO snapshots (
                        id, task_id, kind, chain_id, parent_snapshot_id, status,
                        manifest_key, storage, compression, archives, file_count,
                        changed_count, deleted_count, archive_size,
                        archive_sha256, integrity, started_at, completed_at,
                        error, last_verified_at, verify_status
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?
                    )
                    """,
                    self._snapshot_values(snapshot),
                )
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("创建快照", exc)
        return snapshot

    def commit_snapshot(
        self,
        snapshot: Snapshot,
        files: list[ManifestFile],
        file_versions: Iterable[FileVersion] = (),
    ) -> Snapshot:
        """Atomically commit snapshot, file state and large-file versions."""

        snapshot = Snapshot.model_validate(snapshot)
        normalized_files = [ManifestFile.model_validate(item) for item in files]
        normalized_versions = self._normalize_file_versions(
            snapshot,
            normalized_files,
            file_versions,
        )
        try:
            with self._transaction() as connection:
                existing = self._require_snapshot(connection, snapshot.id)
                if existing.task_id != snapshot.task_id:
                    raise ConflictError(
                        "不能把快照移动到另一个任务",
                        details={
                            "snapshot_id": snapshot.id,
                            "existing_task_id": existing.task_id,
                            "task_id": snapshot.task_id,
                        },
                    )
                self._require_task(connection, snapshot.task_id)
                values = self._snapshot_values(snapshot)
                connection.execute(
                    """
                    UPDATE snapshots SET
                        task_id = ?, kind = ?, chain_id = ?,
                        parent_snapshot_id = ?, status = ?, manifest_key = ?,
                        storage = ?, compression = ?, archives = ?,
                        file_count = ?, changed_count = ?, deleted_count = ?,
                        archive_size = ?, archive_sha256 = ?, integrity = ?,
                        started_at = ?, completed_at = ?, error = ?,
                        last_verified_at = ?, verify_status = ?
                    WHERE id = ?
                    """,
                    (*values[1:], snapshot.id),
                )
                self._validate_file_version_dependencies(
                    connection,
                    snapshot,
                    normalized_files,
                    normalized_versions,
                )
                connection.executemany(
                    """
                    INSERT INTO file_versions (
                        id, task_id, chain_id, file_path, snapshot_id, kind,
                        base_version_id, archive_id, object_key, compression,
                        original_size, transfer_size, sha256, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        self._file_version_values(version)
                        for version in normalized_versions
                    ],
                )
                connection.execute(
                    "DELETE FROM file_state WHERE task_id = ?",
                    (snapshot.task_id,),
                )
                connection.executemany(
                    """
                    INSERT INTO file_state (
                        task_id, path, size, mtime_ns, mode, sha256,
                        origin_snapshot_id, archive_id, file_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            snapshot.task_id,
                            item.path,
                            item.size,
                            item.mtime_ns,
                            item.mode,
                            item.sha256,
                            item.origin_snapshot_id,
                            item.archive_id,
                            (
                                _json_dump(
                                    item.file_version.model_dump(mode="json")
                                )
                                if item.file_version is not None
                                else None
                            ),
                        )
                        for item in normalized_files
                    ],
                )
                row = connection.execute(
                    "SELECT * FROM snapshots WHERE id = ?",
                    (snapshot.id,),
                ).fetchone()
                assert row is not None
                return self._snapshot_from_row(row)
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("提交快照", exc)

    def fail_snapshot(self, snapshot_id: str, error: str) -> Snapshot:
        try:
            with self._transaction() as connection:
                self._require_snapshot(connection, snapshot_id)
                connection.execute(
                    """
                    UPDATE snapshots
                    SET status = ?, error = ?, completed_at = ?
                    WHERE id = ?
                    """,
                    (
                        SnapshotStatus.FAILED.value,
                        error,
                        utc_now_iso(),
                        snapshot_id,
                    ),
                )
                return self._require_snapshot(connection, snapshot_id)
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("标记快照失败", exc)

    def update_snapshot_verification(
        self,
        snapshot_id: str,
        status: str,
        verified_at: str,
    ) -> Snapshot:
        try:
            with self._transaction() as connection:
                self._require_snapshot(connection, snapshot_id)
                connection.execute(
                    """
                    UPDATE snapshots
                    SET verify_status = ?, last_verified_at = ?
                    WHERE id = ?
                    """,
                    (status, verified_at, snapshot_id),
                )
                return self._require_snapshot(connection, snapshot_id)
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("更新快照巡检状态", exc)

    def get_snapshot(self, snapshot_id: str) -> Snapshot:
        with self._connection() as connection:
            return self._require_snapshot(connection, snapshot_id)

    def has_running_snapshot(self, task_id: str) -> bool:
        with self._connection() as connection:
            self._require_task(connection, task_id)
            row = connection.execute(
                """
                SELECT 1 FROM snapshots
                WHERE task_id = ? AND status = 'running'
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()
        return row is not None

    def snapshot_using_credential_profile(
        self, profile: str
    ) -> str | None:
        """Return one recoverable/pending snapshot using a credential."""

        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT id, storage FROM snapshots
                WHERE status IN ('running', 'completed')
                ORDER BY rowid DESC
                """
            )
            for row in rows:
                storage = _STORAGE_ADAPTER.validate_json(row["storage"])
                if (
                    isinstance(storage, S3StorageConfig)
                    and storage.credential_profile == profile
                ):
                    return str(row["id"])
        return None

    def list_running_snapshots(self) -> list[Snapshot]:
        """Return every startup-reconciliation row without a UI limit."""

        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM snapshots
                WHERE status = 'running'
                ORDER BY started_at, rowid
                """
            ).fetchall()
        return [self._snapshot_from_row(row) for row in rows]

    def list_snapshots(
        self,
        task_id: str | None = None,
        limit: int = 200,
    ) -> list[Snapshot]:
        self._validate_limit(limit)
        with self._connection() as connection:
            if task_id is None:
                rows = connection.execute(
                    """
                    SELECT * FROM snapshots
                    ORDER BY started_at DESC, rowid DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            else:
                self._require_task(connection, task_id)
                rows = connection.execute(
                    """
                    SELECT * FROM snapshots
                    WHERE task_id = ?
                    ORDER BY started_at DESC, rowid DESC
                    LIMIT ?
                    """,
                    (task_id, limit),
                ).fetchall()
        return [self._snapshot_from_row(row) for row in rows]

    def latest_snapshot(self, task_id: str) -> Snapshot:
        with self._connection() as connection:
            self._require_task(connection, task_id)
            row = connection.execute(
                """
                SELECT * FROM snapshots
                WHERE task_id = ? AND status = 'completed'
                ORDER BY started_at DESC, rowid DESC
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    "任务没有已完成的快照",
                    details={"task_id": task_id},
                )
            return self._snapshot_from_row(row)

    def count_since_last_full(self, task_id: str) -> int:
        with self._connection() as connection:
            self._require_task(connection, task_id)
            row = connection.execute(
                """
                SELECT COUNT(*) AS amount
                FROM snapshots
                WHERE task_id = ?
                  AND status = 'completed'
                  AND kind = 'incremental'
                  AND rowid > COALESCE(
                      (
                          SELECT rowid
                          FROM snapshots
                          WHERE task_id = ?
                            AND status = 'completed'
                            AND kind = 'full'
                          ORDER BY started_at DESC, rowid DESC
                          LIMIT 1
                      ),
                      0
                  )
                """,
                (task_id, task_id),
            ).fetchone()
            assert row is not None
            return int(row["amount"])

    def get_file_state(self, task_id: str) -> dict[str, ManifestFile]:
        with self._connection() as connection:
            self._require_task(connection, task_id)
            rows = connection.execute(
                """
                SELECT path, size, mtime_ns, mode, sha256,
                       origin_snapshot_id, archive_id, file_version
                FROM file_state
                WHERE task_id = ?
                ORDER BY path ASC
                """,
                (task_id,),
            ).fetchall()
        files = [self._manifest_file_from_row(row) for row in rows]
        return {item.path: item for item in files}

    def get_file_version(self, version_id: str) -> FileVersion:
        with self._connection() as connection:
            return self._require_file_version(connection, version_id)

    def get_file_version_for_snapshot(
        self,
        snapshot_id: str,
        file_path: str,
    ) -> FileVersion:
        with self._connection() as connection:
            self._require_snapshot(connection, snapshot_id)
            row = connection.execute(
                """
                SELECT * FROM file_versions
                WHERE snapshot_id = ? AND file_path = ?
                """,
                (snapshot_id, file_path),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    "快照中没有该文件版本",
                    details={
                        "snapshot_id": snapshot_id,
                        "file_path": file_path,
                    },
                )
            return self._file_version_from_row(row)

    def latest_file_version(
        self,
        task_id: str,
        file_path: str,
        chain_id: str | None = None,
    ) -> FileVersion:
        with self._connection() as connection:
            self._require_task(connection, task_id)
            parameters: list[Any] = [task_id, file_path]
            chain_filter = ""
            if chain_id is not None:
                chain_filter = "AND fv.chain_id = ?"
                parameters.append(chain_id)
            row = connection.execute(
                f"""
                SELECT fv.*
                FROM file_versions AS fv
                JOIN snapshots AS s ON s.id = fv.snapshot_id
                WHERE fv.task_id = ?
                  AND fv.file_path = ?
                  AND s.status = 'completed'
                  {chain_filter}
                ORDER BY s.started_at DESC, fv.rowid DESC
                LIMIT 1
                """,
                tuple(parameters),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    "没有可用的文件版本",
                    details={
                        "task_id": task_id,
                        "file_path": file_path,
                        "chain_id": chain_id,
                    },
                )
            return self._file_version_from_row(row)

    def get_chain_baseline(
        self,
        task_id: str,
        file_path: str,
        chain_id: str,
    ) -> FileVersion:
        """Return the chain's original usable full baseline for this path."""

        with self._connection() as connection:
            self._require_task(connection, task_id)
            row = connection.execute(
                """
                SELECT fv.*
                FROM file_versions AS fv
                JOIN snapshots AS s ON s.id = fv.snapshot_id
                WHERE fv.task_id = ?
                  AND fv.file_path = ?
                  AND fv.chain_id = ?
                  AND fv.kind = 'full'
                  AND s.status = 'completed'
                ORDER BY s.started_at ASC, fv.rowid ASC
                LIMIT 1
                """,
                (task_id, file_path, chain_id),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    "备份链中没有该文件的完整基线",
                    details={
                        "task_id": task_id,
                        "file_path": file_path,
                        "chain_id": chain_id,
                    },
                )
            return self._file_version_from_row(row)

    def list_file_versions_for_snapshot(
        self,
        snapshot_id: str,
    ) -> list[FileVersion]:
        with self._connection() as connection:
            self._require_snapshot(connection, snapshot_id)
            rows = connection.execute(
                """
                SELECT * FROM file_versions
                WHERE snapshot_id = ?
                ORDER BY file_path, rowid
                """,
                (snapshot_id,),
            ).fetchall()
        return [self._file_version_from_row(row) for row in rows]

    def resolve_file_version_chain(
        self,
        version_id: str,
    ) -> list[FileVersion]:
        """Resolve dependencies in restore order, rejecting corrupt cycles."""

        with self._connection() as connection:
            target = self._require_file_version(connection, version_id)
            reverse_chain = [target]
            seen = {target.id}
            current = target
            while current.base_version_id is not None:
                base = self._require_file_version(
                    connection,
                    current.base_version_id,
                )
                if base.id in seen:
                    raise ConflictError(
                        "文件版本依赖形成循环",
                        details={"version_id": version_id},
                    )
                if (
                    base.task_id != target.task_id
                    or base.chain_id != target.chain_id
                    or base.file_path != target.file_path
                ):
                    raise ConflictError(
                        "文件版本依赖跨越了任务、备份链或文件路径",
                        details={
                            "version_id": current.id,
                            "base_version_id": base.id,
                        },
                    )
                reverse_chain.append(base)
                seen.add(base.id)
                current = base

            if current.kind != FileVersionKind.FULL:
                raise ConflictError(
                    "文件版本依赖链没有完整基线",
                    details={"version_id": version_id},
                )
            reverse_chain.reverse()
            return reverse_chain

    def delete_snapshot_rows(self, snapshot_ids: Iterable[str]) -> int:
        ids = list(dict.fromkeys(snapshot_ids))
        if not ids:
            return 0

        try:
            with self._transaction() as connection:
                found: set[str] = set()
                for chunk in _chunks(ids):
                    placeholders = ",".join("?" for _ in chunk)
                    rows = connection.execute(
                        f"SELECT id FROM snapshots WHERE id IN ({placeholders})",
                        tuple(chunk),
                    ).fetchall()
                    found.update(row["id"] for row in rows)
                missing = [snapshot_id for snapshot_id in ids if snapshot_id not in found]
                if missing:
                    raise NotFoundError(
                        "部分快照不存在",
                        details={"snapshot_ids": missing},
                    )

                deleted = 0
                for chunk in _chunks(ids):
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = connection.execute(
                        f"DELETE FROM snapshots WHERE id IN ({placeholders})",
                        tuple(chunk),
                    )
                    deleted += cursor.rowcount
                return deleted
        except sqlite3.IntegrityError as exc:
            self._raise_conflict("删除快照记录", exc)

    def _ensure_memory_keeper(self) -> None:
        if self._memory_uri is None:
            return
        with self._keeper_lock:
            if self._memory_keeper is None:
                self._memory_keeper = self._open_connection()

    def _open_connection(self) -> sqlite3.Connection:
        target = self._memory_uri or str(self.path)
        connection = sqlite3.connect(
            target,
            timeout=self.busy_timeout_ms / 1000,
            uri=self._memory_uri is not None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self._memory_uri is not None:
            self._ensure_memory_keeper()
        connection = self._open_connection()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    @staticmethod
    def _normalize_file_versions(
        snapshot: Snapshot,
        files: list[ManifestFile],
        file_versions: Iterable[FileVersion],
    ) -> list[FileVersion]:
        normalized = [
            FileVersion.model_validate(version)
            for version in file_versions
        ]
        by_id = {version.id: version for version in normalized}
        if len(by_id) != len(normalized):
            raise ConflictError("同一次快照提交包含重复的文件版本 ID")

        files_by_path = {item.path: item for item in files}
        if len(files_by_path) != len(files):
            raise ConflictError("同一次快照提交包含重复的文件路径")

        referenced_ids: set[str] = set()
        for item in files:
            reference = item.file_version
            if reference is None:
                continue
            referenced_ids.add(reference.version_id)
            version = by_id.get(reference.version_id)
            if (
                version is None
                and item.origin_snapshot_id == snapshot.id
            ):
                version = FileVersion(
                    id=reference.version_id,
                    task_id=snapshot.task_id,
                    chain_id=snapshot.chain_id,
                    file_path=item.path,
                    snapshot_id=snapshot.id,
                    kind=reference.kind,
                    base_version_id=reference.base_version_id,
                    archive_id=reference.archive_id,
                    object_key=reference.object_key,
                    compression=reference.compression,
                    original_size=reference.original_size,
                    transfer_size=reference.transfer_size,
                    sha256=reference.sha256,
                    created_at=snapshot.completed_at or snapshot.started_at,
                )
                normalized.append(version)
                by_id[version.id] = version
            if (
                version is not None
                and version.as_reference(base=reference.base) != reference
            ):
                raise ConflictError(
                    "Manifest 中的文件版本引用与 SQLite 记录不一致",
                    details={
                        "file_path": item.path,
                        "version_id": reference.version_id,
                    },
                )

        for version in normalized:
            item = files_by_path.get(version.file_path)
            if item is None or item.file_version is None:
                raise ConflictError(
                    "文件版本没有对应的 Manifest 引用",
                    details={
                        "file_path": version.file_path,
                        "version_id": version.id,
                    },
                )
            if item.file_version.version_id != version.id:
                raise ConflictError(
                    "一个文件不能同时提交两个文件版本",
                    details={"file_path": version.file_path},
                )
            if version.id not in referenced_ids:
                raise ConflictError(
                    "文件版本没有被当前 Manifest 使用",
                    details={"version_id": version.id},
                )
        return normalized

    @classmethod
    def _validate_file_version_dependencies(
        cls,
        connection: sqlite3.Connection,
        snapshot: Snapshot,
        files: list[ManifestFile],
        versions: list[FileVersion],
    ) -> None:
        batch = {version.id: version for version in versions}
        references = {
            item.file_version.version_id: item.file_version
            for item in files
            if item.file_version is not None
        }
        for version in versions:
            if (
                version.task_id != snapshot.task_id
                or version.chain_id != snapshot.chain_id
                or version.snapshot_id != snapshot.id
            ):
                raise ConflictError(
                    "文件版本不属于当前快照、任务或备份链",
                    details={"version_id": version.id},
                )
            if version.kind == FileVersionKind.DELTA:
                assert version.base_version_id is not None
                base = batch.get(version.base_version_id)
                if base is None:
                    base = cls._require_file_version(
                        connection,
                        version.base_version_id,
                    )
                if (
                    base.task_id != version.task_id
                    or base.chain_id != version.chain_id
                    or base.file_path != version.file_path
                    or base.kind != FileVersionKind.FULL
                ):
                    raise ConflictError(
                        "差分版本必须引用同一任务、备份链和路径的完整基线",
                        details={
                            "version_id": version.id,
                            "base_version_id": base.id,
                        },
                    )
                expected = version.as_reference(
                    base=base.as_base_reference(),
                )
            else:
                expected = version.as_reference()
            if references.get(version.id) != expected:
                raise ConflictError(
                    "Manifest 中的文件版本或基线定位信息不正确",
                    details={"version_id": version.id},
                )

        for item in files:
            reference = item.file_version
            if reference is None:
                continue
            version = batch.get(reference.version_id)
            if version is None:
                version = cls._require_file_version(
                    connection,
                    reference.version_id,
                )
            base_reference = None
            if version.kind == FileVersionKind.DELTA:
                assert version.base_version_id is not None
                base = batch.get(version.base_version_id)
                if base is None:
                    base = cls._require_file_version(
                        connection,
                        version.base_version_id,
                    )
                base_reference = base.as_base_reference()
            if (
                version.task_id != snapshot.task_id
                or version.chain_id != snapshot.chain_id
                or version.file_path != item.path
                or version.as_reference(base=base_reference) != reference
                or reference.snapshot_id != item.origin_snapshot_id
                or version.original_size != item.size
                or version.sha256 != item.sha256.lower()
            ):
                raise ConflictError(
                    "文件状态引用了其他任务、备份链或路径的版本",
                    details={
                        "file_path": item.path,
                        "version_id": reference.version_id,
                    },
                )

    @staticmethod
    def _task_values(task: Task) -> tuple[Any, ...]:
        return (
            task.id,
            task.name,
            task.source_path,
            _json_dump(task.storage.model_dump(mode="json")),
            task.schedule,
            int(task.enabled),
            _json_dump(task.excludes),
            task.compression,
            task.compression_level,
            task.shard_size_mb,
            task.full_every,
            task.retention_chains,
            task.retention_days,
            int(task.follow_symlinks),
            int(task.delta_enabled),
            task.delta_threshold_mb,
            task.delta_max_ratio,
            task.created_at,
            task.updated_at,
        )

    @staticmethod
    def _snapshot_values(snapshot: Snapshot) -> tuple[Any, ...]:
        return (
            snapshot.id,
            snapshot.task_id,
            snapshot.kind.value,
            snapshot.chain_id,
            snapshot.parent_snapshot_id,
            snapshot.status.value,
            snapshot.manifest_key,
            _json_dump(snapshot.storage.model_dump(mode="json")),
            snapshot.compression,
            _json_dump(
                [archive.model_dump(mode="json") for archive in snapshot.archives]
            ),
            snapshot.file_count,
            snapshot.changed_count,
            snapshot.deleted_count,
            snapshot.archive_size,
            snapshot.archive_sha256,
            _json_dump(snapshot.integrity),
            snapshot.started_at,
            snapshot.completed_at,
            snapshot.error,
            snapshot.last_verified_at,
            snapshot.verify_status,
        )

    @staticmethod
    def _file_version_values(version: FileVersion) -> tuple[Any, ...]:
        return (
            version.id,
            version.task_id,
            version.chain_id,
            version.file_path,
            version.snapshot_id,
            version.kind.value,
            version.base_version_id,
            version.archive_id,
            version.object_key,
            version.compression,
            version.original_size,
            version.transfer_size,
            version.sha256.lower(),
            version.created_at,
        )

    @staticmethod
    def _operation_values(operation: Operation) -> tuple[Any, ...]:
        return (
            operation.id,
            operation.task_id,
            operation.snapshot_id,
            operation.kind.value,
            operation.status.value,
            operation.progress,
            operation.phase,
            operation.message,
            _json_dump(operation.stats),
            operation.error,
            operation.created_at,
            operation.started_at,
            operation.completed_at,
        )

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> Task:
        storage = _STORAGE_ADAPTER.validate_python(_json_load(row["storage"]))
        excludes = _EXCLUDES_ADAPTER.validate_python(_json_load(row["excludes"]))
        return Task.model_validate(
            {
                "id": row["id"],
                "name": row["name"],
                "source_path": row["source_path"],
                "storage": storage,
                "schedule": row["schedule"],
                "enabled": bool(row["enabled"]),
                "excludes": excludes,
                "compression": row["compression"],
                "compression_level": row["compression_level"],
                "shard_size_mb": row["shard_size_mb"],
                "full_every": row["full_every"],
                "retention_chains": row["retention_chains"],
                "retention_days": row["retention_days"],
                "follow_symlinks": bool(row["follow_symlinks"]),
                "delta_enabled": bool(row["delta_enabled"]),
                "delta_threshold_mb": row["delta_threshold_mb"],
                "delta_max_ratio": row["delta_max_ratio"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )

    @staticmethod
    def _snapshot_from_row(row: sqlite3.Row) -> Snapshot:
        storage = _STORAGE_ADAPTER.validate_python(_json_load(row["storage"]))
        archives = _ARCHIVES_ADAPTER.validate_python(_json_load(row["archives"]))
        integrity = _DICT_ADAPTER.validate_python(_json_load(row["integrity"]))
        return Snapshot.model_validate(
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "kind": row["kind"],
                "chain_id": row["chain_id"],
                "parent_snapshot_id": row["parent_snapshot_id"],
                "status": row["status"],
                "manifest_key": row["manifest_key"],
                "storage": storage,
                "compression": row["compression"],
                "archives": archives,
                "file_count": row["file_count"],
                "changed_count": row["changed_count"],
                "deleted_count": row["deleted_count"],
                "archive_size": row["archive_size"],
                "archive_sha256": row["archive_sha256"],
                "integrity": integrity,
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "error": row["error"],
                "last_verified_at": row["last_verified_at"],
                "verify_status": row["verify_status"],
            }
        )

    @staticmethod
    def _operation_from_row(row: sqlite3.Row) -> Operation:
        stats = _DICT_ADAPTER.validate_python(_json_load(row["stats"]))
        return Operation.model_validate(
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "snapshot_id": row["snapshot_id"],
                "kind": row["kind"],
                "status": row["status"],
                "progress": row["progress"],
                "phase": row["phase"],
                "message": row["message"],
                "stats": stats,
                "error": row["error"],
                "created_at": row["created_at"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
            }
        )

    @staticmethod
    def _manifest_file_from_row(row: sqlite3.Row) -> ManifestFile:
        raw_reference = row["file_version"]
        reference = (
            FileVersionReference.model_validate_json(raw_reference)
            if raw_reference is not None
            else None
        )
        return ManifestFile.model_validate(
            {
                "path": row["path"],
                "size": row["size"],
                "mtime_ns": row["mtime_ns"],
                "mode": row["mode"],
                "sha256": row["sha256"],
                "origin_snapshot_id": row["origin_snapshot_id"],
                "archive_id": row["archive_id"],
                "file_version": reference,
            }
        )

    @staticmethod
    def _file_version_from_row(row: sqlite3.Row) -> FileVersion:
        return FileVersion.model_validate(
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "chain_id": row["chain_id"],
                "file_path": row["file_path"],
                "snapshot_id": row["snapshot_id"],
                "kind": row["kind"],
                "base_version_id": row["base_version_id"],
                "archive_id": row["archive_id"],
                "object_key": row["object_key"],
                "compression": row["compression"],
                "original_size": row["original_size"],
                "transfer_size": row["transfer_size"],
                "sha256": row["sha256"],
                "created_at": row["created_at"],
            }
        )

    @classmethod
    def _require_task(
        cls,
        connection: sqlite3.Connection,
        task_id: str,
    ) -> Task:
        row = connection.execute(
            "SELECT * FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                "任务不存在",
                details={"task_id": task_id},
            )
        return cls._task_from_row(row)

    @classmethod
    def _require_snapshot(
        cls,
        connection: sqlite3.Connection,
        snapshot_id: str,
    ) -> Snapshot:
        row = connection.execute(
            "SELECT * FROM snapshots WHERE id = ?",
            (snapshot_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                "快照不存在",
                details={"snapshot_id": snapshot_id},
            )
        return cls._snapshot_from_row(row)

    @classmethod
    def _require_file_version(
        cls,
        connection: sqlite3.Connection,
        version_id: str,
    ) -> FileVersion:
        row = connection.execute(
            "SELECT * FROM file_versions WHERE id = ?",
            (version_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                "文件版本不存在",
                details={"version_id": version_id},
            )
        return cls._file_version_from_row(row)

    @classmethod
    def _require_operation(
        cls,
        connection: sqlite3.Connection,
        operation_id: str,
    ) -> Operation:
        row = connection.execute(
            "SELECT * FROM operations WHERE id = ?",
            (operation_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                "操作不存在",
                details={"operation_id": operation_id},
            )
        return cls._operation_from_row(row)

    @staticmethod
    def _raise_conflict(action: str, error: sqlite3.IntegrityError) -> None:
        raise ConflictError(
            f"{action}失败：数据重复或违反约束",
            details={"reason": str(error)},
        ) from error

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit 必须是大于 0 的整数")
