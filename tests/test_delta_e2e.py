from __future__ import annotations

import os
import random
from pathlib import Path

import pytest

from easybackup.archive import find_tool
from easybackup.delta import find_xdelta3
from easybackup.engine import BackupEngine, RestoreEngine
from easybackup.errors import StorageError, ToolMissingError
from easybackup.manifest import load_manifest
from easybackup.models import (
    FileVersionKind,
    LocalStorageConfig,
    RestoreRequest,
    TaskCreate,
)
from easybackup.storage import create_store


def _require_delta_tools() -> None:
    if not find_xdelta3() or not find_tool("zstd"):
        pytest.skip("需要 xdelta3 与 zstd 才能执行真实差分端到端测试")


def _task(database, source: Path, repository: Path, name: str):
    return database.create_task(
        TaskCreate(
            name=name,
            source_path=str(source),
            storage=LocalStorageConfig(path=str(repository)),
            compression="gzip",
            shard_size_mb=8,
            full_every=10,
            delta_enabled=True,
            delta_threshold_mb=1,
            delta_max_ratio=0.9,
        )
    )


def _engine(database, credentials, tmp_path: Path) -> BackupEngine:
    return BackupEngine(
        database,
        credentials,
        tmp_path / "locks",
        integrity_block_size=64 * 1024,
    )


def test_base_relative_delta_cache_miss_and_single_step_restore(
    database,
    credentials,
    tmp_path,
):
    _require_delta_tools()
    source = tmp_path / "source"
    repository = tmp_path / "repository"
    source.mkdir()
    current_path = source / "large.bin"
    original = random.Random(20260731).randbytes(2 * 1024 * 1024)
    current_path.write_bytes(original)
    task = _task(database, source, repository, "delta-relative")
    engine = _engine(database, credentials, tmp_path)
    store = create_store(task.storage, credentials)

    first = engine.run(task)
    first_manifest, _ = load_manifest(store, first.manifest_key)
    first_file = first_manifest.files[0]
    assert first_file.file_version is not None
    assert first_file.file_version.kind == FileVersionKind.FULL
    baseline_id = first_file.file_version.version_id

    version_two = bytearray(original)
    version_two[100_000:104_096] = b"2" * 4_096
    current_path.write_bytes(version_two)
    os.utime(current_path, None)
    second = engine.run(task)
    second_manifest, _ = load_manifest(store, second.manifest_key)
    second_file = second_manifest.files[0]
    assert second_file.file_version is not None
    assert second_file.file_version.kind == FileVersionKind.DELTA
    assert second_file.file_version.base_version_id == baseline_id
    assert second_file.file_version.base is not None
    assert second_file.file_version.base.version_id == baseline_id

    # Prove the persistent cache is only an optimization.  The third backup
    # must rematerialize the original Base from remote storage.
    for cached in engine.delta_cache_dir.rglob("*.base"):
        cached.unlink()
    version_three = bytearray(version_two)
    version_three[900_000:908_192] = b"3" * 8_192
    current_path.write_bytes(version_three)
    os.utime(current_path, None)
    third = engine.run(task)
    third_manifest, _ = load_manifest(store, third.manifest_key)
    third_file = third_manifest.files[0]
    assert third_file.file_version is not None
    assert third_file.file_version.kind == FileVersionKind.DELTA
    assert third_file.file_version.base_version_id == baseline_id
    assert (
        third_file.file_version.base_version_id
        != second_file.file_version.version_id
    )
    assert len(
        database.resolve_file_version_chain(
            third_file.file_version.version_id
        )
    ) == 2

    # Damage the intermediate day's Patch.  The latest snapshot still restores
    # because it needs only the original Base plus its own Base-relative Patch.
    second_patch = next(
        archive
        for archive in second.archives
        if archive.id == second_file.archive_id
    )
    store.put_bytes(second_patch.key, b"damaged intermediate patch")
    restore_target = tmp_path / "restore"
    result = RestoreEngine(credentials, tmp_path / "restore-locks").run(
        third,
        RestoreRequest(
            snapshot_id=third.id,
            destination_path=str(restore_target),
            restore_all=True,
        ),
    )
    assert result["delta_restored"] == 1
    assert (restore_target / "large.bin").read_bytes() == bytes(version_three)


def test_delta_patch_corruption_never_publishes_partial_restore(
    database,
    credentials,
    tmp_path,
):
    _require_delta_tools()
    source = tmp_path / "corrupt-source"
    repository = tmp_path / "corrupt-repository"
    source.mkdir()
    source_file = source / "large.bin"
    original = random.Random(7).randbytes(2 * 1024 * 1024)
    source_file.write_bytes(original)
    task = _task(database, source, repository, "delta-corrupt")
    engine = _engine(database, credentials, tmp_path)
    engine.run(task)
    changed = bytearray(original)
    changed[512_000:516_096] = b"x" * 4_096
    source_file.write_bytes(changed)
    os.utime(source_file, None)
    snapshot = engine.run(task)
    store = create_store(task.storage, credentials)
    manifest, _ = load_manifest(store, snapshot.manifest_key)
    entry = manifest.files[0]
    assert entry.file_version is not None
    assert entry.file_version.kind == FileVersionKind.DELTA
    patch = next(
        archive for archive in snapshot.archives if archive.id == entry.archive_id
    )
    store.put_bytes(patch.key, b"corrupt")

    destination = tmp_path / "corrupt-restore"
    with pytest.raises(StorageError, match="校验失败"):
        RestoreEngine(credentials, tmp_path / "corrupt-locks").run(
            snapshot,
            RestoreRequest(
                snapshot_id=snapshot.id,
                destination_path=str(destination),
                restore_all=True,
                overwrite="overwrite",
            ),
        )
    assert not (destination / "large.bin").exists()


def test_delta_base_object_corruption_is_detected_before_rebuild(
    database,
    credentials,
    tmp_path,
):
    _require_delta_tools()
    source = tmp_path / "base-corrupt-source"
    repository = tmp_path / "base-corrupt-repository"
    source.mkdir()
    source_file = source / "large.bin"
    original = random.Random(70).randbytes(2 * 1024 * 1024)
    source_file.write_bytes(original)
    task = _task(database, source, repository, "delta-base-corrupt")
    engine = _engine(database, credentials, tmp_path)
    engine.run(task)
    changed = bytearray(original)
    changed[128_000:132_096] = b"b" * 4_096
    source_file.write_bytes(changed)
    os.utime(source_file, None)
    snapshot = engine.run(task)
    store = create_store(task.storage, credentials)
    manifest, _ = load_manifest(store, snapshot.manifest_key)
    entry = manifest.files[0]
    assert entry.file_version is not None
    assert entry.file_version.kind == FileVersionKind.DELTA
    assert entry.file_version.base is not None
    base = entry.file_version.base
    base_archive = next(
        archive
        for archive in manifest.archives
        if (
            archive.snapshot_id == base.snapshot_id
            and archive.id == base.archive_id
        )
    )
    damaged = bytearray(store.read_bytes(base_archive.key))
    damaged[len(damaged) // 2] ^= 0xFF
    store.put_bytes(base_archive.key, bytes(damaged))

    destination = tmp_path / "base-corrupt-restore"
    with pytest.raises(StorageError, match="大小或 SHA-256 校验失败"):
        RestoreEngine(credentials, tmp_path / "base-corrupt-locks").run(
            snapshot,
            RestoreRequest(
                snapshot_id=snapshot.id,
                destination_path=str(destination),
                restore_all=True,
                overwrite="overwrite",
            ),
        )
    assert not (destination / "large.bin").exists()


def test_unprofitable_or_unavailable_delta_falls_back_to_full(
    database,
    credentials,
    tmp_path,
    monkeypatch,
):
    _require_delta_tools()
    source = tmp_path / "fallback-source"
    repository = tmp_path / "fallback-repository"
    source.mkdir()
    source_file = source / "large.bin"
    source_file.write_bytes(random.Random(1).randbytes(2 * 1024 * 1024))
    task = _task(database, source, repository, "delta-fallback")
    engine = _engine(database, credentials, tmp_path)
    baseline_snapshot = engine.run(task)
    store = create_store(task.storage, credentials)
    baseline_manifest, _ = load_manifest(
        store,
        baseline_snapshot.manifest_key,
    )
    baseline_entry = baseline_manifest.files[0]
    baseline_archive = next(
        item
        for item in baseline_snapshot.archives
        if item.id == baseline_entry.archive_id
    )
    # A still-present local cache must never hide a missing remote Base.
    store.delete(baseline_archive.key)

    source_file.write_bytes(random.Random(2).randbytes(2 * 1024 * 1024))
    os.utime(source_file, None)
    snapshot = engine.run(task)
    manifest, _ = load_manifest(store, snapshot.manifest_key)
    entry = manifest.files[0]
    assert entry.file_version is not None
    assert entry.file_version.kind == FileVersionKind.FULL
    assert "/patches/" not in entry.file_version.object_key

    source_file.write_bytes(random.Random(3).randbytes(2 * 1024 * 1024))
    os.utime(source_file, None)
    # Tool absence is also a recoverable backup condition.
    monkeypatch.setattr(
        "easybackup.engine.backup.find_xdelta3",
        lambda explicit_path=None: None,
    )
    snapshot = engine.run(task)
    manifest, _ = load_manifest(
        store,
        snapshot.manifest_key,
    )
    entry = manifest.files[0]
    assert entry.file_version is not None
    assert entry.file_version.kind == FileVersionKind.FULL
    assert "/patches/" not in entry.file_version.object_key


def test_restore_preflights_delta_tools_before_writing_regular_files(
    database,
    credentials,
    tmp_path,
    monkeypatch,
):
    _require_delta_tools()
    source = tmp_path / "preflight-source"
    repository = tmp_path / "preflight-repository"
    source.mkdir()
    large_file = source / "large.bin"
    original = random.Random(10).randbytes(2 * 1024 * 1024)
    large_file.write_bytes(original)
    (source / "small.txt").write_text("ordinary file", encoding="utf-8")
    task = _task(database, source, repository, "delta-preflight")
    engine = _engine(database, credentials, tmp_path)
    engine.run(task)

    changed = bytearray(original)
    changed[64_000:68_096] = b"changed!" * 512
    large_file.write_bytes(changed)
    os.utime(large_file, None)
    snapshot = engine.run(task)
    manifest, _ = load_manifest(
        create_store(task.storage, credentials),
        snapshot.manifest_key,
    )
    assert any(
        item.file_version is not None
        and item.file_version.kind == FileVersionKind.DELTA
        for item in manifest.files
    )

    monkeypatch.setattr(
        "easybackup.engine.restore.find_xdelta3",
        lambda explicit_path=None: None,
    )
    destination = tmp_path / "preflight-restore"
    with pytest.raises(ToolMissingError, match="xdelta3"):
        RestoreEngine(credentials, tmp_path / "preflight-locks").run(
            snapshot,
            RestoreRequest(
                snapshot_id=snapshot.id,
                destination_path=str(destination),
                restore_all=True,
                overwrite="overwrite",
            ),
        )
    assert not destination.exists()


def test_restore_rejects_manifest_identity_mismatch_before_destination_write(
    database,
    credentials,
    tmp_path,
):
    source = tmp_path / "identity-source"
    repository = tmp_path / "identity-repository"
    source.mkdir()
    (source / "file.txt").write_text("identity", encoding="utf-8")
    task = _task(database, source, repository, "manifest-identity")
    snapshot = _engine(database, credentials, tmp_path).run(task)
    mismatched = snapshot.model_copy(
        update={"chain_id": "different-chain"}
    )

    destination = tmp_path / "identity-restore"
    with pytest.raises(StorageError, match="Manifest 身份"):
        RestoreEngine(credentials, tmp_path / "identity-locks").run(
            mismatched,
            RestoreRequest(
                snapshot_id=mismatched.id,
                destination_path=str(destination),
                restore_all=True,
                overwrite="overwrite",
            ),
        )
    assert not destination.exists()
