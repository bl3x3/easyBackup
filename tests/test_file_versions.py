from __future__ import annotations

import sqlite3

import pytest

from easybackup.config import Settings
from easybackup.db import Database
from easybackup.errors import ConflictError, NotFoundError
from easybackup.models import (
    FileVersion,
    FileVersionKind,
    LocalStorageConfig,
    ManifestFile,
    Snapshot,
    SnapshotKind,
    SnapshotStatus,
    TaskCreate,
    TaskUpdate,
    utc_now_iso,
)


def _create_task(database: Database, tmp_path):
    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    return database.create_task(
        TaskCreate(
            name="large-files",
            source_path=str(source),
            storage=LocalStorageConfig(path=str(tmp_path / "repository")),
            compression="zstd",
        )
    )


def _snapshot(task, snapshot_id: str, *, incremental: bool = False) -> Snapshot:
    return Snapshot(
        id=snapshot_id,
        task_id=task.id,
        kind=(
            SnapshotKind.INCREMENTAL
            if incremental
            else SnapshotKind.FULL
        ),
        chain_id="chain-1",
        parent_snapshot_id="snapshot-full" if incremental else None,
        status=SnapshotStatus.RUNNING,
        manifest_key=f"v1/{snapshot_id}/manifest.json",
        storage=task.storage,
        compression="zstd",
        started_at=utc_now_iso(),
    )


def _completed(snapshot: Snapshot) -> Snapshot:
    return snapshot.model_copy(
        update={
            "status": SnapshotStatus.COMPLETED,
            "completed_at": utc_now_iso(),
        }
    )


def _manifest_file(
    snapshot_id: str,
    version: FileVersion,
    *,
    base: FileVersion | None = None,
) -> ManifestFile:
    reference = version.as_reference(
        base=base.as_base_reference() if base is not None else None,
    )
    return ManifestFile(
        path=version.file_path,
        size=version.original_size,
        mtime_ns=1,
        mode=0o644,
        sha256=version.sha256,
        origin_snapshot_id=snapshot_id,
        archive_id=version.archive_id,
        file_version=reference,
    )


def test_file_version_round_trip_and_base_relative_resolution(
    database,
    tmp_path,
):
    task = _create_task(database, tmp_path)
    baseline_snapshot = _snapshot(task, "snapshot-full")
    database.insert_snapshot(baseline_snapshot)
    baseline = FileVersion(
        id="version-full",
        task_id=task.id,
        chain_id=baseline_snapshot.chain_id,
        file_path="images/disk.bin",
        snapshot_id=baseline_snapshot.id,
        kind=FileVersionKind.FULL,
        archive_id="large-full",
        object_key="v1/snapshot-full/large-full.tar.zst",
        compression="zstd",
        original_size=150 * 1024 * 1024,
        transfer_size=80 * 1024 * 1024,
        sha256="a" * 64,
    )
    baseline_file = _manifest_file(baseline_snapshot.id, baseline)
    database.commit_snapshot(
        _completed(baseline_snapshot),
        [baseline_file],
        [baseline],
    )

    delta_snapshot = _snapshot(
        task,
        "snapshot-delta",
        incremental=True,
    )
    database.insert_snapshot(delta_snapshot)
    delta = FileVersion(
        id="version-delta",
        task_id=task.id,
        chain_id=delta_snapshot.chain_id,
        file_path=baseline.file_path,
        snapshot_id=delta_snapshot.id,
        kind=FileVersionKind.DELTA,
        base_version_id=baseline.id,
        archive_id="large-delta",
        object_key="v1/snapshot-delta/large-delta.patch.zst",
        compression="zstd",
        original_size=151 * 1024 * 1024,
        transfer_size=3 * 1024 * 1024,
        sha256="b" * 64,
    )
    delta_file = _manifest_file(
        delta_snapshot.id,
        delta,
        base=baseline,
    )
    database.commit_snapshot(
        _completed(delta_snapshot),
        [delta_file],
        [delta],
    )

    assert database.get_file_version(delta.id) == delta
    assert (
        database.get_file_version_for_snapshot(
            delta_snapshot.id,
            delta.file_path,
        )
        == delta
    )
    assert (
        database.latest_file_version(
            task.id,
            delta.file_path,
            delta.chain_id,
        )
        == delta
    )
    assert (
        database.get_chain_baseline(
            task.id,
            delta.file_path,
            delta.chain_id,
        )
        == baseline
    )
    assert database.resolve_file_version_chain(delta.id) == [baseline, delta]
    assert database.list_file_versions_for_snapshot(delta_snapshot.id) == [
        delta
    ]
    assert (
        database.get_file_state(task.id)[delta.file_path].file_version
        == delta_file.file_version
    )
    assert delta_file.file_version is not None
    assert delta_file.file_version.base == baseline.as_base_reference()


def test_unchanged_file_reuses_version_without_duplicate_row(
    database,
    tmp_path,
):
    task = _create_task(database, tmp_path)
    first = _snapshot(task, "snapshot-full")
    database.insert_snapshot(first)
    baseline = FileVersion(
        id="stable-version",
        task_id=task.id,
        chain_id=first.chain_id,
        file_path="stable.bin",
        snapshot_id=first.id,
        kind=FileVersionKind.FULL,
        archive_id="stable-archive",
        object_key="v1/snapshot-full/stable.tar.zst",
        compression="zstd",
        original_size=101 * 1024 * 1024,
        transfer_size=50 * 1024 * 1024,
        sha256="c" * 64,
    )
    first_file = _manifest_file(first.id, baseline)
    # Reconciliation can reconstruct a current-snapshot row solely from the
    # self-contained Manifest reference.
    database.commit_snapshot(_completed(first), [first_file])

    second = _snapshot(task, "snapshot-unchanged", incremental=True)
    database.insert_snapshot(second)
    unchanged = first_file.model_copy(update={"mtime_ns": 2})
    database.commit_snapshot(_completed(second), [unchanged])

    reconstructed = database.list_file_versions_for_snapshot(first.id)
    assert len(reconstructed) == 1
    assert reconstructed[0].as_reference() == baseline.as_reference()
    assert database.list_file_versions_for_snapshot(second.id) == []
    assert (
        database.get_file_state(task.id)["stable.bin"].file_version
        == baseline.as_reference()
    )


def test_wrong_delta_base_rolls_back_snapshot_commit(database, tmp_path):
    task = _create_task(database, tmp_path)
    first = _snapshot(task, "snapshot-full")
    database.insert_snapshot(first)
    baseline = FileVersion(
        id="base-other-path",
        task_id=task.id,
        chain_id=first.chain_id,
        file_path="other.bin",
        snapshot_id=first.id,
        kind=FileVersionKind.FULL,
        archive_id="base",
        object_key="v1/base.tar.zst",
        compression="zstd",
        original_size=200 * 1024 * 1024,
        transfer_size=100,
        sha256="d" * 64,
    )
    database.commit_snapshot(
        _completed(first),
        [_manifest_file(first.id, baseline)],
        [baseline],
    )

    second = _snapshot(task, "snapshot-invalid", incremental=True)
    database.insert_snapshot(second)
    delta = FileVersion(
        id="invalid-delta",
        task_id=task.id,
        chain_id=second.chain_id,
        file_path="target.bin",
        snapshot_id=second.id,
        kind=FileVersionKind.DELTA,
        base_version_id=baseline.id,
        archive_id="patch",
        object_key="v1/patch.zst",
        compression="zstd",
        original_size=201 * 1024 * 1024,
        transfer_size=200,
        sha256="e" * 64,
    )
    with pytest.raises(ConflictError, match="同一任务、备份链和路径"):
        database.commit_snapshot(
            _completed(second),
            [_manifest_file(second.id, delta, base=baseline)],
            [delta],
        )

    assert database.get_snapshot(second.id).status == SnapshotStatus.RUNNING
    with pytest.raises(NotFoundError):
        database.get_file_version(delta.id)


def test_deleting_baseline_requires_dependent_snapshots(database, tmp_path):
    task = _create_task(database, tmp_path)
    first = _snapshot(task, "snapshot-full")
    database.insert_snapshot(first)
    baseline = FileVersion(
        id="delete-base",
        task_id=task.id,
        chain_id=first.chain_id,
        file_path="large.bin",
        snapshot_id=first.id,
        kind=FileVersionKind.FULL,
        archive_id="base",
        object_key="v1/base.tar.zst",
        compression="zstd",
        original_size=120 * 1024 * 1024,
        transfer_size=80,
        sha256="f" * 64,
    )
    database.commit_snapshot(
        _completed(first),
        [_manifest_file(first.id, baseline)],
        [baseline],
    )
    second = _snapshot(task, "snapshot-delta", incremental=True)
    database.insert_snapshot(second)
    delta = FileVersion(
        id="delete-delta",
        task_id=task.id,
        chain_id=second.chain_id,
        file_path=baseline.file_path,
        snapshot_id=second.id,
        kind=FileVersionKind.DELTA,
        base_version_id=baseline.id,
        archive_id="patch",
        object_key="v1/patch.zst",
        compression="zstd",
        original_size=121 * 1024 * 1024,
        transfer_size=90,
        sha256="1" * 64,
    )
    database.commit_snapshot(
        _completed(second),
        [_manifest_file(second.id, delta, base=baseline)],
        [delta],
    )

    with pytest.raises(ConflictError):
        database.delete_snapshot_rows([first.id])

    assert database.delete_snapshot_rows([first.id, second.id]) == 2
    with pytest.raises(NotFoundError):
        database.get_file_version(baseline.id)
    with pytest.raises(NotFoundError):
        database.get_file_version(delta.id)


def test_v1_database_migrates_without_losing_existing_state(tmp_path):
    path = tmp_path / "legacy.db"
    original = Database(path)
    original.initialize()
    task = _create_task(original, tmp_path)
    task = original.update_task(
        task.id,
        TaskUpdate(full_every=7),
    )
    snapshot = _snapshot(task, "legacy-snapshot")
    original.insert_snapshot(snapshot)
    legacy_file = ManifestFile(
        path="legacy.txt",
        size=6,
        mtime_ns=123,
        mode=0o644,
        sha256="2" * 64,
        origin_snapshot_id=snapshot.id,
        archive_id="legacy-archive",
    )
    original.commit_snapshot(_completed(snapshot), [legacy_file])
    original.close()

    # Recreate the exact structural delta between v1 and v2, then mark the
    # catalog as v1. SQLite DROP COLUMN keeps all unrelated rows and indexes.
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE file_versions")
        connection.execute("ALTER TABLE file_state DROP COLUMN file_version")
        connection.execute("ALTER TABLE tasks DROP COLUMN delta_max_ratio")
        connection.execute("ALTER TABLE tasks DROP COLUMN delta_threshold_mb")
        connection.execute("ALTER TABLE tasks DROP COLUMN delta_enabled")
        connection.execute(
            """
            UPDATE schema_meta
            SET value = '1'
            WHERE key = 'schema_version'
            """
        )
        connection.commit()

    migrated = Database(path)
    migrated.initialize()
    try:
        restored_task = migrated.get_task(task.id)
        assert restored_task.name == task.name
        assert restored_task.full_every == 7
        assert restored_task.delta_enabled is True
        assert restored_task.delta_threshold_mb == 100
        assert restored_task.delta_max_ratio == 0.9
        assert migrated.get_file_state(task.id) == {
            legacy_file.path: legacy_file
        }
        with sqlite3.connect(path) as connection:
            version = connection.execute(
                """
                SELECT value FROM schema_meta
                WHERE key = 'schema_version'
                """
            ).fetchone()
            assert version == ("2",)
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(tasks)")
            }
            assert {
                "delta_enabled",
                "delta_threshold_mb",
                "delta_max_ratio",
            } <= columns
    finally:
        migrated.close()


def test_failed_v1_migration_rolls_back_all_ddl(tmp_path):
    path = tmp_path / "partial-legacy.db"
    database = Database(path)
    database.initialize()
    database.close()

    # Simulate a damaged catalog whose version still says v1, but where one
    # later column already exists. The migration must not leave the first
    # ALTER TABLE applied after the second one fails.
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE tasks DROP COLUMN delta_enabled")
        connection.execute(
            """
            UPDATE schema_meta
            SET value = '1'
            WHERE key = 'schema_version'
            """
        )
        connection.commit()

    broken = Database(path)
    with pytest.raises(sqlite3.OperationalError, match="duplicate column"):
        broken.initialize()
    broken.close()

    with sqlite3.connect(path) as connection:
        version = connection.execute(
            """
            SELECT value FROM schema_meta
            WHERE key = 'schema_version'
            """
        ).fetchone()
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(tasks)")
        }
    assert version == ("1",)
    assert "delta_enabled" not in columns


def test_delta_configuration_defaults_and_environment(monkeypatch, tmp_path):
    task = TaskCreate(
        name="defaults",
        source_path=str(tmp_path),
        storage=LocalStorageConfig(path=str(tmp_path / "repository")),
    )
    assert task.full_every == 6
    assert task.delta_enabled is True
    assert task.delta_threshold_mb == 100
    assert task.delta_max_ratio == 0.9

    monkeypatch.setenv("EASYBACKUP_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EASYBACKUP_XDELTA3_PATH", r"C:\Tools\xdelta3.exe")
    assert Settings.from_env().xdelta3_path == r"C:\Tools\xdelta3.exe"
