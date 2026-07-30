from __future__ import annotations

import contextlib
import io

import pytest

from easybackup.engine import BackupEngine
from easybackup.errors import StorageError, ValidationError
from easybackup.models import LocalStorageConfig, SnapshotStatus, TaskCreate
from easybackup.storage.local import LocalBlobStore


def _task(database, tmp_path):
    source = tmp_path / "source"
    repository = tmp_path / "repository"
    source.mkdir()
    (source / "value.txt").write_text("payload", encoding="utf-8")
    task = database.create_task(
        TaskCreate(
            name="failure-cleanup",
            source_path=str(source),
            storage=LocalStorageConfig(path=str(repository)),
            compression="gzip",
        )
    )
    return task, repository


def test_archive_exit_failure_removes_already_uploaded_volume(
    database, credentials, tmp_path, monkeypatch
):
    task, repository = _task(database, tmp_path)

    @contextlib.contextmanager
    def broken_archive(*args, **kwargs):
        del args, kwargs
        yield io.BytesIO(b"complete-object")
        raise ValidationError("producer detected a source mutation")

    monkeypatch.setattr(
        "easybackup.engine.backup.create_archive_stream",
        broken_archive,
    )
    engine = BackupEngine(
        database, credentials, tmp_path / "locks", 1024
    )

    with pytest.raises(ValidationError, match="source mutation"):
        engine.run(task)

    snapshot = database.list_snapshots(task.id)[0]
    assert snapshot.status == SnapshotStatus.FAILED
    store = LocalBlobStore(repository)
    assert not any(
        "/snapshots/" in item.key for item in store.iter_objects()
    )


def test_uncertain_commit_put_is_deleted_before_failed_row(
    database, credentials, tmp_path, monkeypatch
):
    task, repository = _task(database, tmp_path)

    class UncertainCommitStore(LocalBlobStore):
        def put_bytes(self, key, payload, *, metadata=None):
            result = super().put_bytes(
                key, payload, metadata=metadata
            )
            if key.endswith("/commit.json"):
                raise StorageError("response lost after durable commit")
            return result

    store = UncertainCommitStore(repository)
    monkeypatch.setattr(
        "easybackup.engine.backup.create_store",
        lambda storage, credential_store: store,
    )
    engine = BackupEngine(
        database, credentials, tmp_path / "locks", 1024
    )

    with pytest.raises(StorageError, match="response lost"):
        engine.run(task)

    snapshot = database.list_snapshots(task.id)[0]
    assert snapshot.status == SnapshotStatus.FAILED
    assert not any(
        "/snapshots/" in item.key
        for item in store.iter_objects("v1/tasks/")
    )
