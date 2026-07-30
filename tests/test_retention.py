from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from easybackup.engine import MaintenanceEngine
from easybackup.errors import ConflictError, NotFoundError
from easybackup.models import (
    LocalStorageConfig,
    Snapshot,
    SnapshotKind,
    SnapshotStatus,
    TaskCreate,
)
from easybackup.storage.local import LocalBlobStore


def _completed_snapshot(
    database,
    task,
    snapshot_id: str,
    completed_at: str,
    *,
    chain_id: str | None = None,
    kind: SnapshotKind = SnapshotKind.FULL,
    parent_snapshot_id: str | None = None,
):
    running = Snapshot(
        id=snapshot_id,
        task_id=task.id,
        kind=kind,
        chain_id=chain_id or snapshot_id,
        parent_snapshot_id=parent_snapshot_id,
        status=SnapshotStatus.RUNNING,
        manifest_key=f"v1/tasks/{task.id}/{snapshot_id}/manifest.json",
        storage=task.storage,
        compression="gzip",
        started_at=completed_at,
    )
    database.insert_snapshot(running)
    database.commit_snapshot(
        running.model_copy(
            update={
                "status": SnapshotStatus.COMPLETED,
                "completed_at": completed_at,
            }
        ),
        [],
    )


def test_prune_requires_remote_lease_and_hides_chain_first(
    database, credentials, tmp_path, monkeypatch
):
    source = tmp_path / "source"
    repository = tmp_path / "repository"
    source.mkdir()
    task = database.create_task(
        TaskCreate(
            name="retention",
            source_path=str(source),
            storage=LocalStorageConfig(path=str(repository)),
            compression="gzip",
            retention_chains=1,
            retention_days=1,
        )
    )
    old_full_time = (
        datetime.now(timezone.utc) - timedelta(days=4)
    ).isoformat()
    old_incremental_time = (
        datetime.now(timezone.utc) - timedelta(days=3)
    ).isoformat()
    current_time = datetime.now(timezone.utc).isoformat()
    _completed_snapshot(
        database,
        task,
        "old-full",
        old_full_time,
        chain_id="old-chain",
    )
    _completed_snapshot(
        database,
        task,
        "old-incremental",
        old_incremental_time,
        chain_id="old-chain",
        kind=SnapshotKind.INCREMENTAL,
        parent_snapshot_id="old-full",
    )
    _completed_snapshot(database, task, "current-chain", current_time)

    store = LocalBlobStore(repository)
    lease_key = f"v1/tasks/{task.id}/write.lock.json"
    external_lease = store.acquire_lease(lease_key, "other-host", 300)
    assert external_lease is not None
    deleted_keys: list[str] = []

    class RecordingStore(LocalBlobStore):
        def delete(self, key):
            deleted_keys.append(key)
            super().delete(key)

    recording_store = RecordingStore(repository)
    monkeypatch.setattr(
        "easybackup.engine.maintenance.create_store",
        lambda storage, credential_store: recording_store,
    )
    maintenance = MaintenanceEngine(
        database, credentials, tmp_path / "locks"
    )

    with pytest.raises(ConflictError):
        maintenance.prune(task)
    assert database.get_snapshot("old-full").status == SnapshotStatus.COMPLETED
    assert (
        database.get_snapshot("old-incremental").status
        == SnapshotStatus.COMPLETED
    )

    store.release_lease(external_lease)
    report = maintenance.prune(task)
    assert report["removed_snapshots"] == 2
    assert all(key.endswith("commit.json") for key in deleted_keys[:2])
    assert all(key.endswith("manifest.json") for key in deleted_keys[2:4])
    with pytest.raises(NotFoundError):
        database.get_snapshot("old-full")
    with pytest.raises(NotFoundError):
        database.get_snapshot("old-incremental")
    assert database.get_snapshot("current-chain").status == SnapshotStatus.COMPLETED
