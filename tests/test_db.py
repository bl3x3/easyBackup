from __future__ import annotations

import pytest

from easybackup.errors import ConflictError
from easybackup.models import (
    LocalStorageConfig,
    OperationKind,
    OperationStatus,
    SFTPStorageConfig,
    Snapshot,
    SnapshotKind,
    SnapshotStatus,
    TaskCreate,
    TaskUpdate,
    utc_now_iso,
)


def _task(tmp_path) -> TaskCreate:
    source = tmp_path / "source"
    source.mkdir()
    return TaskCreate(
        name="documents",
        source_path=str(source),
        storage=LocalStorageConfig(path=str(tmp_path / "repository")),
        schedule="0 2 * * *",
        compression="gzip",
    )


def test_task_and_operation_round_trip(database, tmp_path):
    task = database.create_task(_task(tmp_path))
    assert database.get_task(task.id).name == "documents"
    assert database.list_tasks() == [task]

    updated = database.update_task(
        task.id, TaskUpdate(name="documents-v2", schedule=None)
    )
    assert updated.name == "documents-v2"
    assert updated.schedule is None

    operation = database.create_operation(task.id, OperationKind.BACKUP)
    running = database.update_operation(
        operation.id,
        status=OperationStatus.RUNNING,
        progress=12.5,
        phase="scanning",
        message="scan",
    )
    assert running.started_at
    assert database.get_operation(operation.id).progress == 12.5

    queued = database.create_operation(task.id, OperationKind.BACKUP)
    assert database.mark_interrupted_operations() == 2
    assert database.get_operation(operation.id).status == OperationStatus.FAILED
    assert database.get_operation(queued.id).status == OperationStatus.FAILED


def test_sftp_task_storage_json_round_trip(database, tmp_path):
    source = tmp_path / "sftp-source"
    source.mkdir()
    storage = SFTPStorageConfig(
        host="Backup.Internal.Example",
        port=2222,
        base_path="/srv/easybackup",
        credential_profile="sftp-prod",
        host_key_fingerprint=(
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ),
        connect_timeout_seconds=30,
    )

    created = database.create_task(
        TaskCreate(
            name="sftp-documents",
            source_path=str(source),
            storage=storage,
        )
    )
    loaded = database.get_task(created.id)

    assert isinstance(loaded.storage, SFTPStorageConfig)
    assert loaded.storage.model_dump(mode="json") == {
        "kind": "sftp",
        "host": "backup.internal.example",
        "port": 2222,
        "base_path": "/srv/easybackup",
        "credential_profile": "sftp-prod",
        "host_key_fingerprint": (
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ),
        "known_hosts_path": None,
        "connect_timeout_seconds": 30,
    }
    assert database.list_tasks()[0].storage == loaded.storage


def test_snapshot_commit_replaces_file_state(database, tmp_path):
    task = database.create_task(_task(tmp_path))
    snapshot = Snapshot(
        id="snapshot-1",
        task_id=task.id,
        kind=SnapshotKind.FULL,
        chain_id="snapshot-1",
        status=SnapshotStatus.RUNNING,
        manifest_key="v1/manifest.json",
        storage=task.storage,
        compression="gzip",
        started_at=utc_now_iso(),
    )
    database.insert_snapshot(snapshot)
    completed = snapshot.model_copy(
        update={
            "status": SnapshotStatus.COMPLETED,
            "completed_at": utc_now_iso(),
        }
    )
    database.commit_snapshot(completed, [])
    assert database.latest_snapshot(task.id).id == snapshot.id
    assert database.count_since_last_full(task.id) == 0
    with pytest.raises(ConflictError, match="不能直接删除"):
        database.delete_task(task.id)


def test_source_path_update_clears_fast_scan_state(database, tmp_path):
    task = database.create_task(_task(tmp_path))
    snapshot = Snapshot(
        id="snapshot-state",
        task_id=task.id,
        kind=SnapshotKind.FULL,
        chain_id="snapshot-state",
        status=SnapshotStatus.RUNNING,
        manifest_key="v1/state/manifest.json",
        storage=task.storage,
        compression="gzip",
        started_at=utc_now_iso(),
    )
    database.insert_snapshot(snapshot)
    from easybackup.models import ManifestFile

    database.commit_snapshot(
        snapshot.model_copy(
            update={
                "status": SnapshotStatus.COMPLETED,
                "completed_at": utc_now_iso(),
            }
        ),
        [
            ManifestFile(
                path="same.txt",
                size=4,
                mtime_ns=1,
                mode=0o644,
                sha256="0" * 64,
                origin_snapshot_id=snapshot.id,
                archive_id="volume",
            )
        ],
    )
    replacement = tmp_path / "replacement"
    replacement.mkdir()

    database.update_task(
        task.id, TaskUpdate(source_path=str(replacement))
    )

    assert database.get_file_state(task.id) == {}


def test_task_with_only_failed_snapshot_can_be_deleted(database, tmp_path):
    task = database.create_task(_task(tmp_path))
    snapshot = Snapshot(
        id="failed-only",
        task_id=task.id,
        kind=SnapshotKind.FULL,
        chain_id="failed-only",
        status=SnapshotStatus.FAILED,
        storage=task.storage,
        compression="gzip",
        started_at=utc_now_iso(),
        completed_at=utc_now_iso(),
        error="expected failure",
    )
    database.insert_snapshot(snapshot)

    database.delete_task(task.id)

    from easybackup.errors import NotFoundError

    with pytest.raises(NotFoundError):
        database.get_task(task.id)
