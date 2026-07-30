from __future__ import annotations

import os

import pytest

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
