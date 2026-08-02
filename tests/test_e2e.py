from __future__ import annotations

import os

import pytest

from easybackup.engine import BackupEngine, MaintenanceEngine, RestoreEngine
from easybackup.errors import ValidationError
from easybackup.manifest import load_manifest
from easybackup.models import (
    LocalStorageConfig,
    RestoreRequest,
    S3StorageConfig,
    SnapshotKind,
    TaskCreate,
    TaskUpdate,
)
from easybackup.storage import create_store
from easybackup.storage.local import LocalBlobStore


def test_full_incremental_selective_restore_and_scrub(
    database, credentials, tmp_path
):
    source = tmp_path / "source"
    repository = tmp_path / "repository"
    restore_target = tmp_path / "restore"
    (source / "folder").mkdir(parents=True)
    (source / "keep.txt").write_text("keep-v1", encoding="utf-8")
    (source / "folder" / "change.txt").write_text(
        "before", encoding="utf-8"
    )
    (source / "remove.txt").write_text("remove-me", encoding="utf-8")

    task = database.create_task(
        TaskCreate(
            name="e2e",
            source_path=str(source),
            storage=LocalStorageConfig(path=str(repository)),
            compression="gzip",
            shard_size_mb=8,
            full_every=10,
        )
    )
    backup = BackupEngine(
        database,
        credentials,
        tmp_path / "locks",
        integrity_block_size=1024,
    )
    first = backup.run(task)
    assert first.kind == SnapshotKind.FULL
    assert first.status == "completed"
    assert first.archives

    (source / "folder" / "change.txt").write_text(
        "after-change", encoding="utf-8"
    )
    os.utime(source / "folder" / "change.txt", None)
    (source / "new.txt").write_text("brand-new", encoding="utf-8")
    (source / "remove.txt").unlink()

    second = backup.run(task)
    assert second.kind == SnapshotKind.INCREMENTAL
    assert second.changed_count == 2
    assert second.deleted_count == 1
    store = create_store(task.storage, credentials)
    manifest, _ = load_manifest(store, second.manifest_key)
    assert {item.path for item in manifest.files} == {
        "keep.txt",
        "folder/change.txt",
        "new.txt",
    }
    assert "remove.txt" in manifest.deleted
    keep = next(item for item in manifest.files if item.path == "keep.txt")
    assert keep.origin_snapshot_id == first.id

    restore = RestoreEngine(credentials, tmp_path / "locks")
    result = restore.run(
        second,
        RestoreRequest(
            snapshot_id=second.id,
            destination_path=str(restore_target),
            paths=["folder", "new.txt"],
            overwrite="overwrite",
        ),
    )
    assert result["restored"] == 2
    assert (restore_target / "folder" / "change.txt").read_text(
        encoding="utf-8"
    ) == "after-change"
    assert (restore_target / "new.txt").read_text(encoding="utf-8") == "brand-new"
    assert not (restore_target / "keep.txt").exists()

    report = MaintenanceEngine(
        database, credentials, tmp_path / "locks"
    ).scrub(
        second, deep=True
    )
    assert report["status"] == "healthy"
    assert report["archives_fully_checked"] >= 1


def test_restore_target_rejects_symlink_escape(tmp_path):
    destination = tmp_path / "restore"
    outside = tmp_path / "outside"
    destination.mkdir()
    outside.mkdir()
    link = destination / "escaped"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"当前环境不允许创建符号链接：{exc}")

    with pytest.raises(ValidationError, match="逃逸"):
        RestoreEngine._safe_target(destination.resolve(), "escaped/file.txt")


@pytest.mark.parametrize(
    ("files", "directories", "message"),
    [
        (["A/one.txt", "a/two.txt"], [], "碰撞"),
        (["CON.txt"], [], "设备名"),
        (["trailing."], [], "路径组件"),
        (["folder", "folder/child.txt"], [], "文件和目录"),
    ],
)
def test_windows_restore_preflight_rejects_ambiguous_names(
    files, directories, message
):
    with pytest.raises(ValidationError, match=message):
        RestoreEngine._validate_platform_paths(
            files, directories, windows=True
        )


def test_restore_publish_respects_atomic_collision_policies(tmp_path):
    target = tmp_path / "file.txt"
    target.write_text("existing", encoding="utf-8")

    skipped_source = tmp_path / ".skip-ready"
    skipped_source.write_text("snapshot", encoding="utf-8")
    published, outcome = RestoreEngine._publish_restored_file(
        skipped_source,
        target,
        "skip",
    )
    assert published == target
    assert outcome == "skip"
    assert target.read_text(encoding="utf-8") == "existing"

    renamed_source = tmp_path / ".rename-ready"
    renamed_source.write_text("snapshot", encoding="utf-8")
    published, outcome = RestoreEngine._publish_restored_file(
        renamed_source,
        target,
        "rename",
    )
    assert published == tmp_path / "file.restored-1.txt"
    assert outcome == "rename"
    assert target.read_text(encoding="utf-8") == "existing"
    assert published.read_text(encoding="utf-8") == "snapshot"

    overwrite_source = tmp_path / ".overwrite-ready"
    overwrite_source.write_text("replacement", encoding="utf-8")
    published, outcome = RestoreEngine._publish_restored_file(
        overwrite_source,
        target,
        "overwrite",
    )
    assert published == target
    assert outcome == "write"
    assert target.read_text(encoding="utf-8") == "replacement"
    assert not overwrite_source.exists()

    fresh_source = tmp_path / ".fresh-ready"
    fresh_source.write_text("fresh", encoding="utf-8")
    fresh_target = tmp_path / "fresh.txt"
    published, outcome = RestoreEngine._publish_restored_file(
        fresh_source,
        fresh_target,
        "skip",
    )
    assert published == fresh_target
    assert outcome == "write"
    assert fresh_target.read_text(encoding="utf-8") == "fresh"


def test_source_root_switch_forces_full_and_reads_new_content(
    database, credentials, tmp_path
):
    first_source = tmp_path / "source-one"
    second_source = tmp_path / "source-two"
    repository = tmp_path / "repository"
    first_source.mkdir()
    second_source.mkdir()
    first_file = first_source / "same.txt"
    second_file = second_source / "same.txt"
    first_file.write_text("old!", encoding="utf-8")
    original_mtime = first_file.stat().st_mtime_ns
    second_file.write_text("new!", encoding="utf-8")
    os.utime(second_file, ns=(original_mtime, original_mtime))
    task = database.create_task(
        TaskCreate(
            name="root-switch",
            source_path=str(first_source),
            storage=LocalStorageConfig(path=str(repository)),
            compression="gzip",
            full_every=100,
        )
    )
    engine = BackupEngine(
        database, credentials, tmp_path / "locks", 1024
    )
    first = engine.run(task)

    task = database.update_task(
        task.id, TaskUpdate(source_path=str(second_source))
    )
    second = engine.run(task)

    assert first.kind == SnapshotKind.FULL
    assert second.kind == SnapshotKind.FULL
    manifest, _ = load_manifest(
        create_store(task.storage, credentials), second.manifest_key
    )
    entry = manifest.files[0]
    assert entry.origin_snapshot_id == second.id
    assert entry.sha256 != "0" * 64
    restored = tmp_path / "root-switch-restore"
    RestoreEngine(credentials, tmp_path / "restore-locks").run(
        second,
        RestoreRequest(
            snapshot_id=second.id,
            destination_path=str(restored),
            restore_all=True,
            overwrite="overwrite",
        ),
    )
    assert (restored / "same.txt").read_text(encoding="utf-8") == "new!"


def test_changing_s3_upload_limit_keeps_incremental_chain(
    database, credentials, tmp_path, monkeypatch
):
    source = tmp_path / "limited-source"
    source.mkdir()
    value = source / "value.txt"
    value.write_text("before", encoding="utf-8")
    repository = LocalBlobStore(tmp_path / "limited-repository")
    monkeypatch.setattr(
        "easybackup.engine.backup.create_store",
        lambda _storage, _credentials: repository,
    )
    task = database.create_task(
        TaskCreate(
            name="s3-limit-chain",
            source_path=str(source),
            storage=S3StorageConfig(
                bucket="example-bucket",
                region="us-east-1",
                endpoint_url="https://s3.example.invalid",
            ),
            compression="gzip",
            shard_size_mb=8,
            full_every=100,
        )
    )
    engine = BackupEngine(database, credentials, tmp_path / "limit-locks", 1024)
    first = engine.run(task)

    task = database.update_task(
        task.id,
        TaskUpdate(
            storage=task.storage.model_copy(
                update={"upload_limit_mbps": 25}
            )
        ),
    )
    value.write_text("after!", encoding="utf-8")
    os.utime(value, None)
    second = engine.run(task)

    assert first.kind == SnapshotKind.FULL
    assert second.kind == SnapshotKind.INCREMENTAL
    assert second.chain_id == first.chain_id
    assert second.parent_snapshot_id == first.id


def test_force_full_does_not_depend_on_previous_manifest(
    database, credentials, tmp_path
):
    source = tmp_path / "force-source"
    repository = tmp_path / "force-repository"
    source.mkdir()
    (source / "value.txt").write_text("value", encoding="utf-8")
    task = database.create_task(
        TaskCreate(
            name="force-independent",
            source_path=str(source),
            storage=LocalStorageConfig(path=str(repository)),
            compression="gzip",
        )
    )
    engine = BackupEngine(
        database, credentials, tmp_path / "force-locks", 1024
    )
    first = engine.run(task)
    store = create_store(task.storage, credentials)
    store.delete(first.manifest_key)

    second = engine.run(task, force_full=True)

    assert second.kind == SnapshotKind.FULL
    assert second.chain_id == second.id
    assert second.parent_snapshot_id is None
