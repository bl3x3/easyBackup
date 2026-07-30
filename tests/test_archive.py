from __future__ import annotations

import io
import os

import pytest

from easybackup.archive import (
    IntegrityReader,
    create_archive_stream,
    find_tool,
    open_tar_archive,
)
from easybackup.errors import ValidationError
from easybackup.scanner import scan_source


@pytest.mark.skipif(
    not find_tool("zstd"),
    reason="zstd pipeline is not available",
)
def test_streaming_tar_zstd_round_trip(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "unicode-文件.txt").write_text(
        "external pipeline", encoding="utf-8"
    )
    scan = scan_source(source, [], {}, force_hash_all=True)
    with create_archive_stream(source, scan.files, "zstd", 3) as raw:
        reader = IntegrityReader(raw, 1024)
        payload = bytearray()
        while True:
            chunk = reader.read(4096)
            if not chunk:
                break
            payload.extend(chunk)
        assert reader.integrity.size == len(payload)
        assert reader.integrity.crc32

    names = []
    contents = {}
    with open_tar_archive(io.BytesIO(payload), "zstd") as archive:
        for member in archive:
            names.append(member.name)
            extracted = archive.extractfile(member)
            if extracted:
                contents[member.name] = extracted.read()
    assert names == ["unicode-文件.txt"]
    assert contents["unicode-文件.txt"] == b"external pipeline"


def test_archive_rejects_same_size_source_mutation(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    value = source / "value.txt"
    value.write_text("safe", encoding="utf-8")
    scan = scan_source(source, [], {}, force_hash_all=True)
    original_mtime = scan.files[0].mtime_ns

    value.write_text("evil", encoding="utf-8")
    os.utime(value, ns=(original_mtime, original_mtime))

    with pytest.raises(
        ValidationError, match="发生变化|摘要不一致"
    ):
        with create_archive_stream(
            source, scan.files, "gzip", 3
        ) as raw:
            while raw.read(4096):
                pass


def test_followed_symlink_is_pinned_to_scanned_safe_target(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    safe = source / "safe.txt"
    outside = tmp_path / "private.txt"
    link = source / "link.txt"
    safe.write_text("safe-content", encoding="utf-8")
    outside.write_text("outside-secret", encoding="utf-8")
    try:
        link.symlink_to(safe)
    except OSError as exc:
        pytest.skip(f"当前环境不允许创建符号链接：{exc}")

    scan = scan_source(
        source,
        ["safe.txt"],
        {},
        force_hash_all=True,
        follow_symlinks=True,
    )
    assert [item.path for item in scan.files] == ["link.txt"]
    link.unlink()
    link.symlink_to(outside)

    with create_archive_stream(source, scan.files, "gzip", 3) as raw:
        payload = raw.read()
    with open_tar_archive(io.BytesIO(payload), "gzip") as archive:
        member = archive.next()
        assert member is not None
        extracted = archive.extractfile(member)
        assert extracted is not None
        assert extracted.read() == b"safe-content"
