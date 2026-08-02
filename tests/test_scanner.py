from __future__ import annotations

import os

import pytest

import easybackup.scanner as scanner_module
from easybackup.scanner import scan_source


def test_two_stage_incremental_scan(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "same.txt").write_text("same", encoding="utf-8")
    (source / "change.txt").write_text("before", encoding="utf-8")
    first = scan_source(source, [], {}, force_hash_all=True)
    previous = {}
    for item in first.files:
        from easybackup.models import ManifestFile

        previous[item.path] = ManifestFile(
            path=item.path,
            size=item.size,
            mtime_ns=item.mtime_ns,
            mode=item.mode,
            sha256=item.sha256,
            origin_snapshot_id="old",
            archive_id="old-volume",
        )

    changed = source / "change.txt"
    changed.write_text("after!", encoding="utf-8")
    os.utime(changed, None)
    (source / "new.txt").write_text("new", encoding="utf-8")
    second = scan_source(source, [], previous)
    by_path = {item.path: item for item in second.files}
    assert by_path["same.txt"].content_changed is False
    assert by_path["change.txt"].content_changed is True
    assert by_path["new.txt"].content_changed is True


def test_follow_symlinks_never_reads_files_outside_source(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "private.txt"
    outside.write_text("must not be archived", encoding="utf-8")
    link = source / "outside-link.txt"
    try:
        link.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"当前环境不允许创建符号链接：{exc}")

    result = scan_source(source, [], {}, follow_symlinks=True)

    assert result.files == []
    assert any("指向源目录之外" in item for item in result.skipped)


def test_hash_progress_is_throttled_without_weakening_byte_accounting(
    tmp_path, monkeypatch
):
    source = tmp_path / "source"
    source.mkdir()
    payload_size = 20 * 1024 * 1024
    with (source / "large.bin").open("wb") as handle:
        handle.truncate(payload_size)

    monkeypatch.setattr(
        scanner_module, "_SCAN_PROGRESS_MIN_INTERVAL_SECONDS", 60.0
    )
    monkeypatch.setattr(
        scanner_module, "_SCAN_PROGRESS_MIN_BYTES", payload_size * 2
    )
    monkeypatch.setattr(scanner_module, "_SCAN_PROGRESS_MIN_FILES", 10_000)
    updates: list[tuple[int, int]] = []

    result = scan_source(
        source,
        [],
        {},
        force_hash_all=True,
        progress=lambda files, hashed: updates.append((files, hashed)),
    )

    assert result.hashed_bytes == payload_size
    assert updates == [(1, payload_size)]
