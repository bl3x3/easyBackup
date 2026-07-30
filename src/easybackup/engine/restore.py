"""Safe selective restore from manifest-referenced archive shards."""

from __future__ import annotations

import hashlib
import os
import shutil
import socket
import stat
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import Callable

from easybackup.archive import find_tool, open_tar_archive
from easybackup.delta import (
    DEFAULT_PATCH_OUTPUT_OVERHEAD_BYTES,
    apply_delta_patch,
    decompress_patch,
    find_xdelta3,
)
from easybackup.delta_workflow import (
    download_verified_object,
    extract_tar_member,
)
from easybackup.errors import (
    CancelledError,
    ConflictError,
    StorageError,
    ToolMissingError,
    ValidationError,
)
from easybackup.locking import LeaseGuard, TaskLock
from easybackup.manifest import (
    load_manifest,
    validate_relative_path,
    verify_commit_marker,
)
from easybackup.models import (
    FileVersionKind,
    ManifestFile,
    ProgressUpdate,
    RestoreRequest,
    Snapshot,
)
from easybackup.security import CredentialStore
from easybackup.storage import create_store
from easybackup.storage.base import BlobStore


ProgressEmitter = Callable[[ProgressUpdate], None]
CancelCallback = Callable[[], bool]


class RestoreEngine:
    def __init__(
        self,
        credentials: CredentialStore,
        lock_dir: Path,
        xdelta3_path: str | None = None,
    ):
        self.credentials = credentials
        self.lock_dir = lock_dir
        self.xdelta3_path = xdelta3_path
        self.owner_id = f"{socket.gethostname()}:{uuid.uuid4()}"

    def run(
        self,
        snapshot: Snapshot,
        request: RestoreRequest,
        *,
        cancelled: CancelCallback | None = None,
        emit: ProgressEmitter | None = None,
    ) -> dict:
        cancelled = cancelled or (lambda: False)
        emit = emit or (lambda update: None)
        store = create_store(snapshot.storage, self.credentials)
        lease_key = f"v1/tasks/{snapshot.task_id}/write.lock.json"
        with TaskLock(self.lock_dir, snapshot.task_id):
            with LeaseGuard(
                store, lease_key, self.owner_id
            ) as lease_guard:
                try:
                    result = self._run_locked(
                        snapshot,
                        request,
                        store,
                        cancelled=(
                            lambda: cancelled() or lease_guard.lost
                        ),
                        emit=emit,
                    )
                except CancelledError as exc:
                    if lease_guard.lost:
                        raise ConflictError(
                            "远端租约已丢失，恢复已安全中止。"
                        ) from exc
                    raise
                if lease_guard.lost:
                    raise ConflictError(
                        "远端租约已丢失，恢复结果未被标记为成功。"
                    )
                return result

    def _run_locked(
        self,
        snapshot: Snapshot,
        request: RestoreRequest,
        store: BlobStore,
        *,
        cancelled: CancelCallback,
        emit: ProgressEmitter,
    ) -> dict:
        if not request.restore_all and not request.paths:
            raise ValidationError("必须明确选择文件/目录，或设置 restore_all=true。")
        destination = Path(request.destination_path).expanduser().resolve()
        if destination.exists() and not destination.is_dir():
            raise ValidationError(f"恢复目标不是目录：{destination}")
        if not snapshot.manifest_key:
            raise StorageError("该快照没有 Manifest。")

        emit(
            ProgressUpdate(
                phase="planning",
                progress=2,
                message="正在读取并验证快照 Manifest…",
            )
        )
        manifest, payload = load_manifest(store, snapshot.manifest_key)
        verify_commit_marker(
            store, snapshot.manifest_key, payload, snapshot.id
        )
        if (
            manifest.snapshot_id != snapshot.id
            or manifest.task_id != snapshot.task_id
            or manifest.chain_id != snapshot.chain_id
        ):
            raise StorageError(
                "Manifest 身份与请求的快照、任务或备份链不一致。"
            )
        requested = {
            validate_relative_path(path)
            for path in request.paths
        }

        def selected(path: str) -> bool:
            return request.restore_all or any(
                path == base or path.startswith(base + "/") for base in requested
            )

        selected_files = [item for item in manifest.files if selected(item.path)]
        selected_directories = [
            item for item in manifest.directories if selected(item.path)
        ]
        if not request.restore_all:
            matched = {
                base
                for base in requested
                if any(
                    item.path == base or item.path.startswith(base + "/")
                    for item in [*manifest.files, *manifest.directories]
                )
            }
            missing = sorted(requested - matched)
            if missing:
                raise ValidationError(
                    f"选择的路径不在快照中：{', '.join(missing)}"
                )

        archive_map = {
            (item.snapshot_id, item.id): item for item in manifest.archives
        }
        groups: dict[tuple[str, str], list] = defaultdict(list)
        delta_files: list[ManifestFile] = []
        for item in selected_files:
            identity = (item.origin_snapshot_id, item.archive_id)
            if identity not in archive_map:
                raise StorageError(
                    f"文件 {item.path!r} 引用的分卷不存在。"
                )
            if (
                item.file_version is not None
                and item.file_version.kind == FileVersionKind.DELTA
            ):
                delta_files.append(item)
            else:
                groups[identity].append(item)

        if delta_files and not find_xdelta3(self.xdelta3_path):
            raise ToolMissingError(
                "还原差分文件需要 xdelta3；请安装工具，或配置 "
                "EASYBACKUP_XDELTA3_PATH。"
            )
        selected_archives = [
            archive_map[identity] for identity in groups
        ]
        if any(
            archive.compression == "zstd"
            for archive in selected_archives
        ) or any(
            item.file_version is not None
            and item.file_version.compression == "zstd"
            for item in delta_files
        ):
            if not find_tool("zstd"):
                raise ToolMissingError(
                    "还原 zstd 归档或差分 Patch 需要 zstd 命令。"
                )
        for item in delta_files:
            reference = item.file_version
            assert reference is not None
            if reference.compression != "zstd":
                raise StorageError(
                    f"差分文件 {item.path!r} 的 Patch 不是 zstd 格式。"
                )

        self._validate_platform_paths(
            [item.path for item in selected_files],
            [item.path for item in selected_directories],
            destination=destination,
        )
        destination.mkdir(parents=True, exist_ok=True)

        for directory in sorted(
            selected_directories, key=lambda item: len(PurePosixPath(item.path).parts)
        ):
            self._safe_target(destination, directory.path).mkdir(
                parents=True, exist_ok=True
            )

        restored = 0
        skipped = 0
        renamed = 0
        restored_bytes = 0
        total_files = len(selected_files)
        for group_index, (identity, expected_files) in enumerate(
            sorted(groups.items()), start=1
        ):
            if cancelled():
                raise CancelledError("恢复已取消。")
            archive = archive_map[identity]
            expected = {item.path: item for item in expected_files}
            found: set[str] = set()
            emit(
                ProgressUpdate(
                    phase="downloading",
                    progress=5 + 90 * (group_index - 1) / max(1, len(groups)),
                    message=f"正在读取分卷 {group_index}/{len(groups)}",
                    stats={
                        "files_done": restored + skipped,
                        "files_total": total_files,
                    },
                )
            )
            stream = store.open_read(archive.key)
            with open_tar_archive(
                stream, archive.compression, cancelled
            ) as tar:
                for member in tar:
                    if cancelled():
                        raise CancelledError("恢复已取消。")
                    try:
                        member_path = validate_relative_path(member.name)
                    except ValidationError as exc:
                        raise StorageError(
                            f"归档含有不安全成员 {member.name!r}。"
                        ) from exc
                    item = expected.get(member_path)
                    if not item:
                        continue
                    if not member.isreg():
                        raise StorageError(
                            f"归档成员 {member_path!r} 不是普通文件。"
                        )
                    if member.size != item.size:
                        raise StorageError(
                            f"归档成员 {member_path!r} 的大小与 Manifest 不一致。"
                        )
                    found.add(member_path)
                    requested_target = self._safe_target(
                        destination,
                        member_path,
                    )
                    if (
                        request.overwrite == "skip"
                        and requested_target.exists()
                    ):
                        skipped += 1
                        continue
                    if (
                        request.overwrite == "overwrite"
                        and requested_target.is_dir()
                    ):
                        raise ValidationError(
                            "目标路径是目录，无法覆盖："
                            f"{requested_target}"
                        )
                    requested_target.parent.mkdir(
                        parents=True,
                        exist_ok=True,
                    )
                    extracted = tar.extractfile(member)
                    if extracted is None:
                        raise StorageError(f"无法读取归档成员：{member_path}")
                    temporary_path: Path | None = None
                    try:
                        with tempfile.NamedTemporaryFile(
                            mode="wb",
                            prefix=f".{requested_target.name}.",
                            suffix=".restore-part",
                            dir=requested_target.parent,
                            delete=False,
                        ) as output:
                            temporary_path = Path(output.name)
                            digest = hashlib.sha256()
                            written = 0
                            while True:
                                if cancelled():
                                    raise CancelledError("恢复已取消。")
                                chunk = extracted.read(1024 * 1024)
                                if not chunk:
                                    break
                                output.write(chunk)
                                digest.update(chunk)
                                written += len(chunk)
                                if written > item.size:
                                    raise StorageError(
                                        f"归档成员 {member_path!r} 超过 Manifest 声明大小。"
                                    )
                            output.flush()
                            os.fsync(output.fileno())
                        if written != item.size:
                            raise StorageError(
                                f"归档成员 {member_path!r} 未达到 Manifest 声明大小。"
                            )
                        if request.verify and digest.hexdigest() != item.sha256:
                            raise StorageError(
                                f"文件 {member_path!r} 恢复后 SHA-256 不一致。"
                            )
                        if cancelled():
                            raise CancelledError("恢复已取消。")
                        target, outcome = self._publish_restored_file(
                            temporary_path,
                            requested_target,
                            request.overwrite,
                        )
                        if outcome == "skip":
                            skipped += 1
                            continue
                        if outcome == "rename":
                            renamed += 1
                        try:
                            os.chmod(target, stat.S_IMODE(item.mode))
                        except OSError:
                            pass
                        try:
                            os.utime(
                                target,
                                ns=(item.mtime_ns, item.mtime_ns),
                            )
                        except OSError:
                            pass
                        restored += 1
                        restored_bytes += written
                        emit(
                            ProgressUpdate(
                                phase="extracting",
                                progress=5
                                + 90
                                * (restored + skipped)
                                / max(1, total_files),
                                message=f"已恢复 {restored + skipped}/{total_files} 个文件",
                                stats={
                                    "files_done": restored + skipped,
                                    "files_total": total_files,
                                    "bytes_restored": restored_bytes,
                                },
                            )
                        )
                    finally:
                        extracted.close()
                        if temporary_path:
                            try:
                                temporary_path.unlink(missing_ok=True)
                            except OSError:
                                pass
            missing_members = set(expected) - found
            if missing_members:
                raise StorageError(
                    "归档缺少 Manifest 中声明的成员："
                    + ", ".join(sorted(missing_members)[:10])
                )

        delta_restored = 0
        for delta_index, item in enumerate(
            sorted(delta_files, key=lambda value: value.path),
            start=1,
        ):
            if cancelled():
                raise CancelledError("恢复已取消。")
            reference = item.file_version
            assert reference is not None
            base = reference.base
            if base is None:
                raise StorageError(
                    f"差分文件 {item.path!r} 缺少完整基线定位信息。"
                )

            requested_target = self._safe_target(destination, item.path)
            if (
                request.overwrite == "skip"
                and requested_target.exists()
            ):
                skipped += 1
                continue
            if (
                request.overwrite == "overwrite"
                and requested_target.is_dir()
            ):
                raise ValidationError(
                    f"目标路径是目录，无法覆盖：{requested_target}"
                )
            requested_target.parent.mkdir(parents=True, exist_ok=True)

            reconstructed_path: Path | None = None
            try:
                with tempfile.TemporaryDirectory(
                    prefix=(
                        f"easybackup-restore-delta-"
                        f"{snapshot.id[:8]}-{delta_index}-"
                    )
                ) as workspace_text:
                    workspace = Path(workspace_text)
                    base_archive = archive_map.get(
                        (base.snapshot_id, base.archive_id)
                    )
                    if base_archive is None:
                        raise StorageError(
                            f"差分文件 {item.path!r} 的基线分卷不存在。"
                        )
                    base_name = hashlib.sha256(
                        (
                            base.version_id + "\0" + item.path
                        ).encode("utf-8")
                    ).hexdigest()
                    base_path = workspace / f"base-{base_name}.bin"
                    emit(
                        ProgressUpdate(
                            phase="downloading",
                            progress=5
                            + 85
                            * (restored + skipped)
                            / max(1, total_files),
                            message=(
                                f"正在读取差分基线 "
                                f"{delta_index}/{len(delta_files)}"
                            ),
                        )
                    )
                    extract_tar_member(
                        store,
                        object_key=base.object_key,
                        compression=base.compression,
                        member_path=item.path,
                        destination=base_path,
                        expected_size=base.original_size,
                        expected_sha256=base.sha256,
                        archive_object=base_archive,
                        cancelled=cancelled,
                    )

                    patch_archive = archive_map[
                        (item.origin_snapshot_id, item.archive_id)
                    ]
                    patch_name = hashlib.sha256(
                        reference.version_id.encode("utf-8")
                    ).hexdigest()
                    compressed_patch = (
                        workspace / f"patch-{patch_name}.vcdiff.zst"
                    )
                    raw_patch = (
                        workspace / f"patch-{patch_name}.vcdiff"
                    )
                    download_verified_object(
                        store,
                        patch_archive,
                        compressed_patch,
                        cancelled=cancelled,
                    )
                    decompress_patch(
                        compressed_patch,
                        raw_patch,
                        max_output_size=(
                            item.size
                            + DEFAULT_PATCH_OUTPUT_OVERHEAD_BYTES
                        ),
                        cancelled=cancelled,
                    )
                    with tempfile.NamedTemporaryFile(
                        mode="wb",
                        prefix=f".{requested_target.name}.",
                        suffix=".delta-ready",
                        dir=requested_target.parent,
                        delete=False,
                    ) as output:
                        reconstructed_path = Path(output.name)
                    # Delta restore always verifies the logical result.
                    # Without this check, a valid Patch paired with the wrong
                    # Base could silently produce incorrect bytes even when
                    # verify=false.
                    rebuilt = apply_delta_patch(
                        base_path,
                        raw_patch,
                        reconstructed_path,
                        expected_size=item.size,
                        expected_sha256=item.sha256,
                        executable=self.xdelta3_path,
                        cancelled=cancelled,
                    )
                if cancelled():
                    raise CancelledError("恢复已取消。")
                target, outcome = self._publish_restored_file(
                    reconstructed_path,
                    requested_target,
                    request.overwrite,
                )
                if outcome == "skip":
                    skipped += 1
                    continue
                if outcome == "rename":
                    renamed += 1
                try:
                    os.chmod(target, stat.S_IMODE(item.mode))
                except OSError:
                    pass
                try:
                    os.utime(
                        target,
                        ns=(item.mtime_ns, item.mtime_ns),
                    )
                except OSError:
                    pass
                restored += 1
                delta_restored += 1
                restored_bytes += rebuilt.size
                emit(
                    ProgressUpdate(
                        phase="applying_delta",
                        progress=5
                        + 90
                        * (restored + skipped)
                        / max(1, total_files),
                        message=(
                            f"已恢复 {restored + skipped}/{total_files} "
                            "个文件"
                        ),
                        stats={
                            "files_done": restored + skipped,
                            "files_total": total_files,
                            "bytes_restored": restored_bytes,
                            "delta_restored": delta_restored,
                        },
                    )
                )
            finally:
                if reconstructed_path is not None:
                    try:
                        reconstructed_path.unlink(missing_ok=True)
                    except OSError:
                        pass

        for directory in sorted(
            selected_directories,
            key=lambda item: len(PurePosixPath(item.path).parts),
            reverse=True,
        ):
            target = self._safe_target(destination, directory.path)
            if not target.exists():
                continue
            try:
                os.chmod(target, stat.S_IMODE(directory.mode))
                os.utime(target, ns=(directory.mtime_ns, directory.mtime_ns))
            except OSError:
                pass
        emit(
            ProgressUpdate(
                phase="completed",
                progress=100,
                message=f"恢复完成：{restored} 个文件",
                stats={
                    "restored": restored,
                    "skipped": skipped,
                    "renamed": renamed,
                    "bytes_restored": restored_bytes,
                    "delta_restored": delta_restored,
                },
            )
        )
        return {
            "restored": restored,
            "skipped": skipped,
            "renamed": renamed,
            "bytes_restored": restored_bytes,
            "delta_restored": delta_restored,
            "destination": str(destination),
        }

    @staticmethod
    def _validate_platform_paths(
        files: list[str],
        directories: list[str],
        *,
        destination: Path | None = None,
        windows: bool | None = None,
    ) -> None:
        """Reject Windows aliases/illegal names before creating any output."""

        if windows is None:
            windows = os.name == "nt"
        if not windows:
            return

        invalid_characters = set('<>:"|?*')
        reserved = {"CON", "PRN", "AUX", "NUL"}
        reserved.update(f"COM{index}" for index in range(1, 10))
        reserved.update(f"LPT{index}" for index in range(1, 10))
        canonical_spellings: dict[tuple[str, ...], str] = {}
        file_keys: set[tuple[str, ...]] = set()

        for kind, raw_path in [
            *(("file", path) for path in files),
            *(("directory", path) for path in directories),
        ]:
            normalized = validate_relative_path(raw_path)
            parts = PurePosixPath(normalized).parts
            key_parts: list[str] = []
            original_parts: list[str] = []
            for component in parts:
                if (
                    component.endswith((" ", "."))
                    or any(
                        character in invalid_characters
                        or ord(character) < 32
                        for character in component
                    )
                    or len(component.encode("utf-16-le")) // 2 > 255
                ):
                    raise ValidationError(
                        f"Windows 恢复目标不支持路径组件：{component!r}"
                    )
                device_name = component.split(".", 1)[0].upper()
                if device_name in reserved:
                    raise ValidationError(
                        f"Windows 恢复目标保留了设备名：{component!r}"
                    )
                key_parts.append(component.casefold())
                original_parts.append(component)
                key = tuple(key_parts)
                spelling = "/".join(original_parts)
                previous = canonical_spellings.setdefault(key, spelling)
                if previous != spelling:
                    raise ValidationError(
                        "快照路径在 Windows 上发生大小写或名称碰撞："
                        f"{previous!r} 与 {spelling!r}"
                    )

            full_key = tuple(key_parts)
            if kind == "file":
                file_keys.add(full_key)
            if destination is not None:
                target = destination.joinpath(*parts)
                if len(str(target).encode("utf-16-le")) // 2 > 32767:
                    raise ValidationError(
                        f"Windows 恢复目标路径过长：{normalized!r}"
                    )

        all_keys = set(canonical_spellings)
        for file_key in file_keys:
            for length in range(1, len(file_key)):
                if file_key[:length] in file_keys:
                    raise ValidationError(
                        "快照在 Windows 上把同一路径同时作为文件和目录："
                        + "/".join(file_key[:length])
                    )
            if file_key in all_keys:
                # The full key itself is expected; the loop above detects a
                # file used as a parent of any other selected entry.
                for other in all_keys:
                    if (
                        len(other) > len(file_key)
                        and other[: len(file_key)] == file_key
                    ):
                        raise ValidationError(
                            "快照在 Windows 上把同一路径同时作为文件和目录："
                            + "/".join(file_key)
                        )

    @staticmethod
    def _safe_target(destination: Path, relative: str) -> Path:
        normalized = validate_relative_path(relative)
        target = destination.joinpath(*PurePosixPath(normalized).parts)
        resolved_target = target.resolve(strict=False)
        try:
            resolved_target.relative_to(destination)
        except ValueError as exc:
            raise ValidationError(f"恢复路径逃逸目标目录：{relative!r}") from exc
        return target

    @staticmethod
    def _publish_restored_file(
        temporary: Path,
        target: Path,
        policy: str,
    ) -> tuple[Path, str]:
        """Atomically publish one verified file with collision semantics.

        ``skip`` and ``rename`` use Windows' non-replacing atomic rename or a
        POSIX hard-link create operation.  Both are atomic and never replace
        an existing directory entry.  The temporary file is always created
        beside the target, so publication stays on one filesystem.
        """

        if policy == "overwrite":
            try:
                os.replace(temporary, target)
            except OSError as exc:
                raise StorageError(
                    f"无法原子覆盖恢复目标 {target}：{exc}"
                ) from exc
            return target, "write"

        if policy == "skip":
            if not RestoreEngine._publish_no_replace(
                temporary,
                target,
            ):
                return target, "skip"
            return target, "write"

        if policy == "rename":
            stem = target.stem
            suffix = target.suffix
            for index in range(0, 10000):
                candidate = (
                    target
                    if index == 0
                    else target.with_name(
                        f"{stem}.restored-{index}{suffix}"
                    )
                )
                if not RestoreEngine._publish_no_replace(
                    temporary,
                    candidate,
                ):
                    continue
                return candidate, "write" if index == 0 else "rename"
            raise ValidationError(f"无法为恢复文件选择可用名称：{target}")
        raise ValidationError(f"未知覆盖策略：{policy}")

    @staticmethod
    def _publish_no_replace(source: Path, target: Path) -> bool:
        """Atomically publish if and only if ``target`` does not exist."""

        try:
            if os.name == "nt":
                # Unlike POSIX rename(), Windows MoveFile semantics fail when
                # the destination already exists and work on FAT/exFAT/SMB
                # targets that may not support hard links.
                os.rename(source, target)
            else:
                os.link(source, target)
        except FileExistsError:
            return False
        except OSError as exc:
            raise StorageError(
                f"无法以不覆盖方式发布恢复文件 {target}：{exc}"
            ) from exc
        return True
