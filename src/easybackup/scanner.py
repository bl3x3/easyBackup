"""Filesystem scanning, filtering and two-stage change detection."""

from __future__ import annotations

import fnmatch
import hashlib
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable

from easybackup.errors import CancelledError, ValidationError
from easybackup.models import ManifestDirectory, ManifestFile


CancelCallback = Callable[[], bool]
ScanProgress = Callable[[int, int], None]


_SCAN_PROGRESS_MIN_INTERVAL_SECONDS = 0.25
_SCAN_PROGRESS_MIN_BYTES = 64 * 1024 * 1024
_SCAN_PROGRESS_MIN_FILES = 1_000


@dataclass(frozen=True, slots=True)
class ScannedFile:
    path: str
    # Canonical path captured during the scan.  Archive writers use this
    # instead of resolving the logical (possibly symlinked) path again.
    absolute_path: Path
    size: int
    mtime_ns: int
    ctime_ns: int
    device: int
    inode: int
    mode: int
    sha256: str
    content_changed: bool
    previous_origin_snapshot_id: str | None
    previous_archive_id: str | None


@dataclass(frozen=True, slots=True)
class ScanResult:
    files: list[ScannedFile]
    directories: list[ManifestDirectory]
    deleted: list[str]
    skipped: list[str]
    total_bytes: int
    hashed_bytes: int


def _excluded(path: str, patterns: list[str], *, is_dir: bool = False) -> bool:
    candidate = path.rstrip("/")
    for raw in patterns:
        pattern = raw.replace("\\", "/").strip()
        if not pattern:
            continue
        plain = pattern.rstrip("/")
        if pattern.endswith("/**"):
            prefix = pattern[:-3].rstrip("/")
            if candidate == prefix or candidate.startswith(prefix + "/"):
                return True
        if fnmatch.fnmatchcase(candidate, plain):
            return True
        if PurePosixPath(candidate).match(plain):
            return True
        if is_dir and fnmatch.fnmatchcase(candidate + "/", pattern):
            return True
    return False


def _hash_file(
    path: Path,
    *,
    cancelled: CancelCallback | None,
    on_bytes: Callable[[int], None] | None,
) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while True:
                if cancelled and cancelled():
                    raise CancelledError("操作已取消。")
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                if on_bytes:
                    on_bytes(len(chunk))
    except CancelledError:
        raise
    except OSError as exc:
        raise ValidationError(f"无法读取源文件 {path}：{exc}") from exc
    return digest.hexdigest()


def scan_source(
    source: Path,
    excludes: list[str],
    previous: dict[str, ManifestFile],
    *,
    force_hash_all: bool = False,
    follow_symlinks: bool = False,
    cancelled: CancelCallback | None = None,
    progress: ScanProgress | None = None,
) -> ScanResult:
    source = source.expanduser().resolve()
    if not source.exists():
        raise ValidationError(f"源目录不存在：{source}")
    if not source.is_dir():
        raise ValidationError(f"源路径不是目录：{source}")

    files: list[ScannedFile] = []
    directories: list[ManifestDirectory] = []
    skipped: list[str] = []
    total_bytes = 0
    hashed_bytes = 0
    seen_casefold: dict[str, str] = {}
    visited_directories: set[tuple[int, int]] = set()
    last_progress_at = time.monotonic()
    last_progress_files = 0
    last_progress_bytes = 0

    def report_progress(*, force: bool = False) -> None:
        nonlocal last_progress_at, last_progress_files, last_progress_bytes
        if progress is None:
            return
        now = time.monotonic()
        file_count = len(files)
        if not force and not (
            now - last_progress_at >= _SCAN_PROGRESS_MIN_INTERVAL_SECONDS
            or file_count - last_progress_files >= _SCAN_PROGRESS_MIN_FILES
            or hashed_bytes - last_progress_bytes >= _SCAN_PROGRESS_MIN_BYTES
        ):
            return
        progress(file_count, hashed_bytes)
        last_progress_at = now
        last_progress_files = file_count
        last_progress_bytes = hashed_bytes

    def add_hashed(delta: int) -> None:
        nonlocal hashed_bytes
        hashed_bytes += delta
        report_progress()

    for root_text, dir_names, file_names in os.walk(
        source, topdown=True, followlinks=follow_symlinks
    ):
        if cancelled and cancelled():
            raise CancelledError("操作已取消。")
        root = Path(root_text)
        try:
            resolved_root = root.resolve(strict=True)
            resolved_root.relative_to(source)
            root_stat = resolved_root.stat()
        except ValueError:
            dir_names[:] = []
            skipped.append(
                f"{root.relative_to(source).as_posix()}/ (指向源目录之外)"
            )
            continue
        except OSError as exc:
            raise ValidationError(f"无法读取目录 {root}：{exc}") from exc
        identity = (root_stat.st_dev, root_stat.st_ino)
        if identity in visited_directories:
            dir_names[:] = []
            skipped.append(f"{root.relative_to(source).as_posix()} (目录循环)")
            continue
        visited_directories.add(identity)

        kept_dirs: list[str] = []
        for name in sorted(dir_names):
            path = root / name
            relative = path.relative_to(source).as_posix()
            if _excluded(relative, excludes, is_dir=True):
                skipped.append(f"{relative}/ (排除规则)")
                continue
            if path.is_symlink():
                if not follow_symlinks:
                    skipped.append(f"{relative}/ (符号链接)")
                    continue
            try:
                resolved = path.resolve(strict=True)
                resolved.relative_to(source)
                value = resolved.stat()
            except ValueError:
                skipped.append(f"{relative}/ (指向源目录之外)")
                continue
            except OSError as exc:
                raise ValidationError(f"无法读取目录元数据 {path}：{exc}") from exc
            kept_dirs.append(name)
            try:
                directories.append(
                    ManifestDirectory(
                        path=relative,
                        mtime_ns=value.st_mtime_ns,
                        mode=stat.S_IMODE(value.st_mode),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValidationError(f"目录元数据无效 {path}：{exc}") from exc
        dir_names[:] = kept_dirs

        for name in sorted(file_names):
            if cancelled and cancelled():
                raise CancelledError("操作已取消。")
            path = root / name
            relative = path.relative_to(source).as_posix()
            if _excluded(relative, excludes):
                skipped.append(f"{relative} (排除规则)")
                continue
            if path.is_symlink():
                if not follow_symlinks:
                    skipped.append(f"{relative} (符号链接)")
                    continue
            try:
                resolved = path.resolve(strict=True)
                resolved.relative_to(source)
                value = resolved.stat()
            except ValueError:
                skipped.append(f"{relative} (指向源目录之外)")
                continue
            except OSError as exc:
                raise ValidationError(f"无法读取文件元数据 {path}：{exc}") from exc
            if not stat.S_ISREG(value.st_mode):
                skipped.append(f"{relative} (非普通文件)")
                continue

            normalized = PurePosixPath(relative).as_posix()
            if normalized.startswith("/") or ".." in PurePosixPath(normalized).parts:
                raise ValidationError(f"发现不安全的相对路径：{relative}")
            if os.name == "nt":
                folded = normalized.casefold()
                old = seen_casefold.get(folded)
                if old and old != normalized:
                    raise ValidationError(
                        f"发现仅大小写不同的路径冲突：{old!r} 与 {normalized!r}"
                    )
                seen_casefold[folded] = normalized

            old = previous.get(normalized)
            metadata_same = bool(
                old
                and old.size == value.st_size
                and old.mtime_ns == value.st_mtime_ns
            )
            must_hash = force_hash_all or not metadata_same
            sha256 = (
                _hash_file(
                    resolved, cancelled=cancelled, on_bytes=add_hashed
                )
                if must_hash
                else old.sha256
            )
            if must_hash:
                try:
                    after_hash = resolved.stat()
                except OSError as exc:
                    raise ValidationError(
                        f"计算摘要期间文件消失或无法读取：{relative}（{exc}）"
                    ) from exc
                if not _same_file_version(value, after_hash):
                    raise ValidationError(
                        f"计算摘要期间文件发生变化：{relative}；请重试。"
                    )
            content_changed = not old or sha256 != old.sha256
            files.append(
                ScannedFile(
                    path=normalized,
                    absolute_path=resolved,
                    size=value.st_size,
                    mtime_ns=value.st_mtime_ns,
                    ctime_ns=value.st_ctime_ns,
                    device=value.st_dev,
                    inode=value.st_ino,
                    mode=stat.S_IMODE(value.st_mode),
                    sha256=sha256,
                    content_changed=content_changed,
                    previous_origin_snapshot_id=(
                        old.origin_snapshot_id if old else None
                    ),
                    previous_archive_id=old.archive_id if old else None,
                )
            )
            total_bytes += value.st_size
            report_progress()

    report_progress(force=True)
    files.sort(key=lambda item: item.path)
    directories.sort(key=lambda item: item.path)
    current_paths = {item.path for item in files}
    deleted = sorted(set(previous) - current_paths)
    return ScanResult(
        files=files,
        directories=directories,
        deleted=deleted,
        skipped=skipped,
        total_bytes=total_bytes,
        hashed_bytes=hashed_bytes,
    )


def _same_file_version(
    expected: os.stat_result,
    actual: os.stat_result,
) -> bool:
    return bool(
        stat.S_ISREG(actual.st_mode)
        and actual.st_dev == expected.st_dev
        and actual.st_ino == expected.st_ino
        and actual.st_size == expected.st_size
        and actual.st_mtime_ns == expected.st_mtime_ns
        and (
            os.name == "nt"
            or actual.st_ctime_ns == expected.st_ctime_ns
        )
    )


def assert_stat_unchanged(
    item: ScannedFile,
    value: os.stat_result,
    *,
    check_ctime: bool | None = None,
) -> None:
    """Validate an opened file descriptor against the scanned file version."""

    if check_ctime is None:
        check_ctime = os.name != "nt"
    if not (
        stat.S_ISREG(value.st_mode)
        and value.st_dev == item.device
        and value.st_ino == item.inode
        and value.st_size == item.size
        and value.st_mtime_ns == item.mtime_ns
        and (not check_ctime or value.st_ctime_ns == item.ctime_ns)
    ):
        raise ValidationError(
            f"归档期间文件发生变化：{item.path}；本次快照已安全中止，请重试。"
        )


def assert_file_unchanged(item: ScannedFile, source: Path) -> None:
    """Validate containment, canonical identity and file metadata."""

    source = source.expanduser().resolve()
    try:
        resolved = item.absolute_path.resolve(strict=True)
        resolved.relative_to(source)
        if resolved != item.absolute_path:
            raise ValidationError(
                f"归档期间文件路径被替换：{item.path}；本次快照已安全中止。"
            )
        value = resolved.stat()
    except ValidationError:
        raise
    except (OSError, ValueError) as exc:
        raise ValidationError(
            f"归档期间文件消失、越出源目录或无法读取：{item.path}（{exc}）"
        ) from exc
    assert_stat_unchanged(item, value)


def assert_files_unchanged(
    files: list[ScannedFile],
    source: Path,
) -> None:
    """Reject a snapshot if an archived file changed during the pipeline."""

    for item in files:
        assert_file_unchanged(item, source)
