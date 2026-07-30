"""Verified staging and object-materialization helpers for delta workflows."""

from __future__ import annotations

import hashlib
import hmac
import os
import tempfile
from pathlib import Path
from typing import BinaryIO, Callable

from easybackup.archive import Codec, open_tar_archive
from easybackup.delta import FileIntegrity
from easybackup.errors import CancelledError, StorageError, ValidationError
from easybackup.manifest import validate_relative_path
from easybackup.models import ArchiveObject
from easybackup.scanner import (
    ScannedFile,
    assert_file_unchanged,
    assert_stat_unchanged,
)
from easybackup.storage.base import BlobStore


CancelCallback = Callable[[], bool]
_CHUNK_SIZE = 4 * 1024 * 1024


def stage_scanned_file(
    item: ScannedFile,
    source: Path,
    destination: Path,
    *,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Copy the exact scanned file version into an immutable operation file.

    xdelta3 opens path arguments itself, so passing the mutable source path
    would weaken the descriptor-level checks used by the tar writer.  This
    helper pins and validates the source while producing a private staging
    file, then atomically publishes the verified staging copy.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    assert_file_unchanged(item, source)
    flags = os.O_RDONLY
    flags |= int(getattr(os, "O_BINARY", 0))
    flags |= int(getattr(os, "O_NOFOLLOW", 0))
    temporary = _temporary_sibling(destination, ".stage-part")
    try:
        try:
            descriptor = os.open(item.absolute_path, flags)
        except OSError as exc:
            raise ValidationError(
                f"无法安全打开差分源文件 {item.path}：{exc}"
            ) from exc
        digest = hashlib.sha256()
        written = 0
        try:
            with os.fdopen(descriptor, "rb", buffering=0) as input_stream:
                assert_stat_unchanged(
                    item,
                    os.fstat(input_stream.fileno()),
                    check_ctime=os.name != "nt",
                )
                with temporary.open("wb") as output:
                    written, actual_sha256 = _copy_and_hash(
                        input_stream,
                        output,
                        expected_size=item.size,
                        cancelled=cancelled,
                        digest=digest,
                    )
                    output.flush()
                    os.fsync(output.fileno())
                assert_stat_unchanged(
                    item,
                    os.fstat(input_stream.fileno()),
                    check_ctime=os.name != "nt",
                )
        except OSError as exc:
            raise StorageError(
                f"暂存差分源文件 {item.path!r} 失败：{exc}"
            ) from exc
        if (
            written != item.size
            or not hmac.compare_digest(actual_sha256, item.sha256.lower())
        ):
            raise ValidationError(
                f"差分暂存字节与扫描摘要不一致：{item.path}；请重试。"
            )
        assert_file_unchanged(item, source)
        _replace(temporary, destination, f"发布差分暂存文件 {item.path!r}")
        return FileIntegrity(size=written, sha256=actual_sha256)
    finally:
        temporary.unlink(missing_ok=True)


def extract_tar_member(
    store: BlobStore,
    *,
    object_key: str,
    compression: Codec,
    member_path: str,
    destination: Path,
    expected_size: int,
    expected_sha256: str,
    archive_object: ArchiveObject | None = None,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Materialize and verify one regular member from a remote tar object.

    When ``archive_object`` is supplied, the exact stored bytes are downloaded
    and verified before the logical tar member is extracted.  Restore uses
    this strict mode; backup cache reconstruction may use streaming mode to
    avoid downloading an already trusted large object twice.
    """

    normalized_member = validate_relative_path(member_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(destination, ".base-part")
    downloaded_archive: Path | None = None
    found = False
    result: FileIntegrity | None = None
    try:
        if archive_object is not None:
            if (
                archive_object.key != object_key
                or archive_object.compression != compression
            ):
                raise StorageError(
                    "基线归档对象与 Manifest 定位信息不一致。"
                )
            downloaded_archive = _temporary_sibling(
                destination,
                ".archive-download",
            )
            download_verified_object(
                store,
                archive_object,
                downloaded_archive,
                cancelled=cancelled,
            )
            stream = downloaded_archive.open("rb")
        else:
            stream = store.open_read(object_key)
        try:
            archive_context = open_tar_archive(
                stream,
                compression,
                cancelled,
            )
            with archive_context as archive:
                for member in archive:
                    _check_cancel(cancelled)
                    try:
                        normalized = validate_relative_path(member.name)
                    except ValidationError as exc:
                        raise StorageError(
                            f"归档含有不安全成员 {member.name!r}。"
                        ) from exc
                    if normalized != normalized_member:
                        continue
                    if found:
                        raise StorageError(
                            f"归档重复包含基线成员 {normalized_member!r}。"
                        )
                    if not member.isreg():
                        raise StorageError(
                            f"基线归档成员 {normalized_member!r} 不是普通文件。"
                        )
                    if member.size != expected_size:
                        raise StorageError(
                            f"基线成员 {normalized_member!r} 的大小与元数据不一致。"
                        )
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        raise StorageError(
                            f"无法读取基线归档成员：{normalized_member}"
                        )
                    try:
                        digest = hashlib.sha256()
                        with temporary.open("wb") as output:
                            written, actual_sha256 = _copy_and_hash(
                                extracted,
                                output,
                                expected_size=expected_size,
                                cancelled=cancelled,
                                digest=digest,
                            )
                            output.flush()
                            os.fsync(output.fileno())
                    finally:
                        extracted.close()
                    if (
                        written != expected_size
                        or not hmac.compare_digest(
                            actual_sha256, expected_sha256.lower()
                        )
                    ):
                        raise StorageError(
                            f"基线成员 {normalized_member!r} 的 SHA-256 校验失败。"
                        )
                    found = True
                    result = FileIntegrity(
                        size=written,
                        sha256=actual_sha256,
                    )
        finally:
            stream.close()
        if not found or result is None:
            raise StorageError(
                f"基线归档缺少 Manifest 声明的成员：{normalized_member}"
            )
        _replace(
            temporary,
            destination,
            f"发布基线缓存 {normalized_member!r}",
        )
        return result
    finally:
        temporary.unlink(missing_ok=True)
        if downloaded_archive is not None:
            downloaded_archive.unlink(missing_ok=True)


def download_verified_object(
    store: BlobStore,
    archive: ArchiveObject,
    destination: Path,
    *,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Download one immutable object and verify its stored-byte integrity."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(destination, ".download-part")
    try:
        stream = store.open_read(archive.key)
        try:
            digest = hashlib.sha256()
            with temporary.open("wb") as output:
                written, actual_sha256 = _copy_and_hash(
                    stream,
                    output,
                    expected_size=archive.integrity.size,
                    cancelled=cancelled,
                    digest=digest,
                )
                output.flush()
                os.fsync(output.fileno())
        finally:
            stream.close()
        if (
            written != archive.integrity.size
            or not hmac.compare_digest(
                actual_sha256,
                archive.integrity.sha256.lower(),
            )
        ):
            raise StorageError(
                f"对象 {archive.key!r} 的大小或 SHA-256 校验失败。"
            )
        _replace(temporary, destination, f"发布下载对象 {archive.key!r}")
        return FileIntegrity(size=written, sha256=actual_sha256)
    finally:
        temporary.unlink(missing_ok=True)


def _copy_and_hash(
    input_stream: BinaryIO,
    output: BinaryIO,
    *,
    expected_size: int,
    cancelled: CancelCallback | None,
    digest: "hashlib._Hash",
) -> tuple[int, str]:
    written = 0
    while True:
        _check_cancel(cancelled)
        chunk = input_stream.read(_CHUNK_SIZE)
        if not chunk:
            break
        written += len(chunk)
        if written > expected_size:
            raise StorageError("读取内容超过元数据声明的大小。")
        output.write(chunk)
        digest.update(chunk)
    return written, digest.hexdigest()


def _temporary_sibling(destination: Path, suffix: str) -> Path:
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=suffix,
        dir=destination.parent,
    )
    os.close(descriptor)
    return Path(name)


def _replace(source: Path, destination: Path, action: str) -> None:
    try:
        os.replace(source, destination)
    except OSError as exc:
        raise StorageError(f"{action}失败：{exc}") from exc


def _check_cancel(cancelled: CancelCallback | None) -> None:
    if cancelled and cancelled():
        raise CancelledError("操作已取消。")
