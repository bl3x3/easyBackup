from __future__ import annotations

from pathlib import Path

import pytest

from easybackup.engine import BackupEngine
from easybackup.errors import ConflictError, StorageError
from easybackup.manifest import sha256_bytes, stable_json_bytes
from easybackup.models import (
    ArchiveIntegrity,
    LocalStorageConfig,
    Snapshot,
    SnapshotKind,
    SnapshotManifest,
    SnapshotStatus,
    TaskCreate,
    utc_now_iso,
)
from easybackup.reconcile import reconcile_incomplete_snapshots
from easybackup.storage.local import LocalBlobStore


def _running_snapshot(database, tmp_path, snapshot_id: str) -> Snapshot:
    source = tmp_path / f"source-{snapshot_id}"
    source.mkdir()
    repository = tmp_path / f"repository-{snapshot_id}"
    task = database.create_task(
        TaskCreate(
            name=f"task-{snapshot_id}",
            source_path=str(source),
            storage=LocalStorageConfig(path=str(repository)),
            compression="gzip",
        )
    )
    snapshot = Snapshot(
        id=snapshot_id,
        task_id=task.id,
        kind=SnapshotKind.FULL,
        chain_id=snapshot_id,
        status=SnapshotStatus.RUNNING,
        manifest_key=f"snapshots/{snapshot_id}/manifest.json",
        storage=task.storage,
        compression="gzip",
        started_at=utc_now_iso(),
    )
    return database.insert_snapshot(snapshot)


def test_reconcile_imports_valid_remote_commit(database, credentials, tmp_path):
    snapshot = _running_snapshot(database, tmp_path, "valid")
    store = LocalBlobStore(Path(snapshot.storage.path))
    empty_integrity = ArchiveIntegrity(
        sha256=sha256_bytes(b""),
        size=0,
        block_size=1024,
        crc32=[],
    )
    manifest = SnapshotManifest(
        snapshot_id=snapshot.id,
        task_id=snapshot.task_id,
        kind=snapshot.kind,
        chain_id=snapshot.chain_id,
        created_at=utc_now_iso(),
        source_path=str(tmp_path / "source-valid"),
        archives=[],
        files=[],
        archive_integrity=empty_integrity,
    )
    payload = stable_json_bytes(manifest)
    store.put_bytes(snapshot.manifest_key, payload)
    store.put_bytes(
        "snapshots/valid/commit.json",
        stable_json_bytes(
            {
                "version": 1,
                "snapshot_id": snapshot.id,
                "manifest_sha256": sha256_bytes(payload),
            }
        ),
    )

    assert reconcile_incomplete_snapshots(
        database, credentials, tmp_path / "locks"
    ) == {
        "completed": 1,
        "failed": 0,
        "deferred": 0,
    }
    assert database.get_snapshot(snapshot.id).status == SnapshotStatus.COMPLETED


def test_reconcile_fails_only_conclusively_missing_commit(
    database, credentials, tmp_path, monkeypatch
):
    missing = _running_snapshot(database, tmp_path, "missing")
    deferred = _running_snapshot(database, tmp_path, "deferred")

    class UnavailableStore:
        def acquire_lease(self, key, owner, ttl_seconds):
            del key, owner, ttl_seconds
            raise StorageError("temporary outage")

    real_create_store = __import__(
        "easybackup.reconcile", fromlist=["create_store"]
    ).create_store

    def create_store(storage, credential_store):
        if storage.path == deferred.storage.path:
            return UnavailableStore()
        return real_create_store(storage, credential_store)

    monkeypatch.setattr("easybackup.reconcile.create_store", create_store)
    result = reconcile_incomplete_snapshots(
        database, credentials, tmp_path / "locks"
    )

    assert result == {"completed": 0, "failed": 1, "deferred": 1}
    assert database.get_snapshot(missing.id).status == SnapshotStatus.FAILED
    assert database.get_snapshot(deferred.id).status == SnapshotStatus.RUNNING

    backup = BackupEngine(
        database,
        credentials,
        tmp_path / "locks",
        integrity_block_size=1024,
    )
    with pytest.raises(ConflictError, match="对账"):
        backup.run(database.get_task(deferred.task_id))


def test_reconcile_removes_orphaned_snapshot_objects(
    database, credentials, tmp_path
):
    snapshot = _running_snapshot(database, tmp_path, "orphan")
    store = LocalBlobStore(Path(snapshot.storage.path))
    prefix = "snapshots/orphan/"
    store.put_bytes(f"{prefix}volumes/000001.tar.gz", b"partial")
    store.put_bytes(snapshot.manifest_key, b"not-a-valid-manifest")

    result = reconcile_incomplete_snapshots(
        database, credentials, tmp_path / "locks"
    )

    assert result == {"completed": 0, "failed": 1, "deferred": 0}
    assert list(store.iter_objects(prefix)) == []
    assert database.get_snapshot(snapshot.id).status == SnapshotStatus.FAILED


def test_reconcile_preserves_commit_when_manifest_is_unavailable(
    database, credentials, tmp_path
):
    snapshot = _running_snapshot(database, tmp_path, "published")
    store = LocalBlobStore(Path(snapshot.storage.path))
    commit_key = "snapshots/published/commit.json"
    store.put_bytes(
        commit_key,
        stable_json_bytes(
            {
                "version": 1,
                "snapshot_id": snapshot.id,
                "manifest_sha256": "0" * 64,
            }
        ),
    )

    result = reconcile_incomplete_snapshots(
        database, credentials, tmp_path / "locks"
    )

    assert result == {"completed": 0, "failed": 0, "deferred": 1}
    assert store.stat(commit_key) is not None
    assert database.get_snapshot(snapshot.id).status == SnapshotStatus.RUNNING


def test_reconcile_rechecks_stale_row_after_task_lock(
    database, credentials, tmp_path, monkeypatch
):
    snapshot = _running_snapshot(database, tmp_path, "stale")
    database.fail_snapshot(snapshot.id, "already handled")
    monkeypatch.setattr(
        database,
        "list_running_snapshots",
        lambda: [snapshot],
    )

    result = reconcile_incomplete_snapshots(
        database, credentials, tmp_path / "locks"
    )

    assert result == {"completed": 0, "failed": 0, "deferred": 0}
    current = database.get_snapshot(snapshot.id)
    assert current.status == SnapshotStatus.FAILED
    assert current.error == "already handled"
