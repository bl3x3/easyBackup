"""Safe command-line adapters for base-relative xdelta3 patches.

The functions in this module deliberately operate on regular files rather
than shell command strings.  Every generated artifact is first written to a
temporary sibling and is atomically moved into place only after the command
and optional integrity checks succeed.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

from easybackup.archive import find_tool
from easybackup.errors import (
    CancelledError,
    StorageError,
    ToolMissingError,
    ValidationError,
)


CancelCallback = Callable[[], bool]
PathValue = str | os.PathLike[str]

DEFAULT_TIMEOUT_SECONDS = 60 * 60
DEFAULT_PATCH_OUTPUT_OVERHEAD_BYTES = 64 * 1024 * 1024
_POLL_SECONDS = 0.1
_HASH_CHUNK_SIZE = 4 * 1024 * 1024
_MAX_DIAGNOSTIC_BYTES = 4_000
_SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass(frozen=True, slots=True)
class FileIntegrity:
    """Size and SHA-256 of a complete on-disk artifact."""

    size: int
    sha256: str


def find_xdelta3(explicit_path: PathValue | None = None) -> str | None:
    """Find xdelta3, including active/base Conda locations on Windows.

    ``archive.find_tool`` already checks PATH, ``sys.prefix``,
    ``sys.base_prefix`` and ``CONDA_PREFIX`` (including
    ``Library/bin`` and ``Scripts`` on Windows).  An explicit argument or the
    ``EASYBACKUP_XDELTA3_PATH`` environment variable can override that search.
    The older ``EASYBACKUP_XDELTA3`` spelling remains accepted.
    """

    requested = (
        explicit_path
        or os.environ.get("EASYBACKUP_XDELTA3_PATH")
        or os.environ.get("EASYBACKUP_XDELTA3")
    )
    if requested:
        candidate = Path(requested).expanduser()
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            return None
        return str(resolved) if resolved.is_file() else None
    return find_tool("xdelta3")


def xdelta3_capability(
    explicit_path: PathValue | None = None,
) -> dict[str, str | bool | None]:
    """Return a diagnostic capability record without raising if unavailable."""

    executable = find_xdelta3(explicit_path)
    if not executable:
        return {"available": False, "path": None, "version": None}
    version: str | None = None
    try:
        completed = subprocess.run(
            [executable, "-V"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
            shell=False,
            creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
        )
        output = (completed.stdout or completed.stderr).strip()
        version = output.splitlines()[0][:_MAX_DIAGNOSTIC_BYTES] if output else None
    except (OSError, subprocess.SubprocessError):
        pass
    return {"available": True, "path": executable, "version": version}


def file_integrity(
    path: PathValue,
    *,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Calculate a file's actual byte count and SHA-256 in one pass."""

    source = _regular_input(path, "待校验文件")
    digest = hashlib.sha256()
    size = 0
    try:
        with source.open("rb") as handle:
            while True:
                _check_cancel(cancelled)
                chunk = handle.read(_HASH_CHUNK_SIZE)
                if not chunk:
                    break
                size += len(chunk)
                digest.update(chunk)
    except CancelledError:
        raise
    except OSError as exc:
        raise StorageError(f"读取待校验文件失败：{source}：{exc}") from exc
    return FileIntegrity(size=size, sha256=digest.hexdigest())


def verify_file(
    path: PathValue,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Calculate and optionally validate a file's size and SHA-256."""

    _validate_expectations(expected_size, expected_sha256)
    integrity = file_integrity(path, cancelled=cancelled)
    _assert_integrity(
        integrity,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
        label=str(path),
    )
    return integrity


def create_delta_patch(
    base_path: PathValue,
    current_path: PathValue,
    patch_path: PathValue,
    *,
    executable: PathValue | None = None,
    max_output_size: int | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Generate one patch directly relative to the supplied baseline.

    Callers should pass the weekly/full baseline as ``base_path`` for every
    incremental version.  This keeps restore depth at exactly one instead of
    constructing a chained delta.
    """

    base = _regular_input(base_path, "差分基线")
    current = _regular_input(current_path, "新版本文件")
    output = _safe_output(patch_path, (base, current), "Patch 输出")
    tool = _require_tool("xdelta3", executable)

    return _write_atomic_artifact(
        output,
        lambda temporary: [
            tool,
            "-f",
            "-e",
            "-s",
            str(base),
            str(current),
            str(temporary),
        ],
        tool_name="xdelta3",
        operation="生成差分补丁",
        timeout_seconds=timeout_seconds,
        cancelled=cancelled,
        max_output_size=max_output_size,
    )


def apply_delta_patch(
    base_path: PathValue,
    patch_path: PathValue,
    output_path: PathValue,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    executable: PathValue | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Apply one base-relative patch and validate the reconstructed file."""

    _validate_expectations(expected_size, expected_sha256)
    base = _regular_input(base_path, "差分基线")
    patch = _regular_input(patch_path, "差分补丁")
    output = _safe_output(output_path, (base, patch), "还原输出")
    tool = _require_tool("xdelta3", executable)

    return _write_atomic_artifact(
        output,
        lambda temporary: [
            tool,
            "-f",
            "-d",
            "-s",
            str(base),
            str(patch),
            str(temporary),
        ],
        tool_name="xdelta3",
        operation="应用差分补丁",
        timeout_seconds=timeout_seconds,
        cancelled=cancelled,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
    )


def compress_patch(
    patch_path: PathValue,
    output_path: PathValue,
    *,
    level: int = 3,
    executable: PathValue | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Compress a generated patch with native zstd."""

    if not 1 <= level <= 19:
        raise ValidationError("zstd 压缩级别必须在 1 到 19 之间。")
    patch = _regular_input(patch_path, "待压缩 Patch")
    output = _safe_output(output_path, (patch,), "压缩 Patch 输出")
    tool = _require_tool("zstd", executable)

    return _write_atomic_artifact(
        output,
        lambda temporary: [
            tool,
            "-T0",
            f"-{level}",
            "-q",
            "-f",
            str(patch),
            "-o",
            str(temporary),
        ],
        tool_name="zstd",
        operation="压缩差分补丁",
        timeout_seconds=timeout_seconds,
        cancelled=cancelled,
    )


def decompress_patch(
    compressed_path: PathValue,
    output_path: PathValue,
    *,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    max_output_size: int | None = None,
    executable: PathValue | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    cancelled: CancelCallback | None = None,
) -> FileIntegrity:
    """Decompress a zstd patch and optionally verify the raw patch bytes."""

    _validate_expectations(expected_size, expected_sha256)
    compressed = _regular_input(compressed_path, "压缩 Patch")
    output = _safe_output(output_path, (compressed,), "解压 Patch 输出")
    tool = _require_tool("zstd", executable)

    return _write_atomic_artifact(
        output,
        lambda temporary: [
            tool,
            "-d",
            "-q",
            "-f",
            str(compressed),
            "-o",
            str(temporary),
        ],
        tool_name="zstd",
        operation="解压差分补丁",
        timeout_seconds=timeout_seconds,
        cancelled=cancelled,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
        max_output_size=max_output_size,
    )


def _require_tool(
    name: str,
    explicit_path: PathValue | None,
) -> str:
    if name == "xdelta3":
        found = find_xdelta3(explicit_path)
    elif explicit_path:
        candidate = Path(explicit_path).expanduser()
        try:
            resolved = candidate.resolve(strict=True)
        except OSError:
            found = None
        else:
            found = str(resolved) if resolved.is_file() else None
    else:
        found = find_tool(name)
    if found:
        return found
    variable = (
        "EASYBACKUP_XDELTA3_PATH"
        if name == "xdelta3"
        else "PATH"
    )
    raise ToolMissingError(
        f"未找到 {name} 命令；请安装工具并加入 PATH，"
        f"或通过 {variable} 指定可执行文件。"
    )


def _regular_input(path: PathValue, label: str) -> Path:
    candidate = Path(path).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ValidationError(f"{label}不存在或不可访问：{candidate}") from exc
    if not resolved.is_file():
        raise ValidationError(f"{label}不是普通文件：{resolved}")
    return resolved


def _safe_output(
    path: PathValue,
    inputs: Sequence[Path],
    label: str,
) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.name:
        raise ValidationError(f"{label}必须是文件路径。")
    if candidate.is_symlink():
        raise ValidationError(f"{label}不能是符号链接：{candidate}")
    try:
        parent = candidate.parent.resolve(strict=True)
    except OSError as exc:
        raise ValidationError(f"{label}目录不存在：{candidate.parent}") from exc
    if not parent.is_dir():
        raise ValidationError(f"{label}父路径不是目录：{parent}")
    output = parent / candidate.name
    if output.exists() and not output.is_file():
        raise ValidationError(f"{label}不是普通文件：{output}")
    normalized_output = os.path.normcase(str(output))
    if any(
        normalized_output == os.path.normcase(str(source))
        for source in inputs
    ):
        raise ValidationError(f"{label}不能覆盖输入文件：{output}")
    return output


def _validate_expectations(
    expected_size: int | None,
    expected_sha256: str | None,
) -> None:
    if expected_size is not None and expected_size < 0:
        raise ValidationError("预期文件大小不能小于 0。")
    if (
        expected_sha256 is not None
        and not _SHA256_PATTERN.fullmatch(expected_sha256)
    ):
        raise ValidationError("预期 SHA-256 必须是 64 位十六进制字符串。")


def _assert_integrity(
    integrity: FileIntegrity,
    *,
    expected_size: int | None,
    expected_sha256: str | None,
    label: str,
) -> None:
    if expected_size is not None and integrity.size != expected_size:
        raise ValidationError(
            f"{label} 大小校验失败。",
            details={
                "expected_size": expected_size,
                "actual_size": integrity.size,
            },
        )
    if (
        expected_sha256 is not None
        and not hmac.compare_digest(
            integrity.sha256, expected_sha256.lower()
        )
    ):
        raise ValidationError(
            f"{label} SHA-256 校验失败。",
            details={
                "expected_sha256": expected_sha256.lower(),
                "actual_sha256": integrity.sha256,
            },
        )


def _write_atomic_artifact(
    output: Path,
    arguments_for: Callable[[Path], list[str]],
    *,
    tool_name: str,
    operation: str,
    timeout_seconds: float,
    cancelled: CancelCallback | None,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
    max_output_size: int | None = None,
) -> FileIntegrity:
    if timeout_seconds <= 0:
        raise ValidationError("外部工具超时时间必须大于 0 秒。")
    if max_output_size is not None and max_output_size < 0:
        raise ValidationError("外部工具输出上限不能小于 0。")
    effective_output_limit = max_output_size
    if expected_size is not None:
        effective_output_limit = (
            expected_size
            if effective_output_limit is None
            else min(expected_size, effective_output_limit)
        )
    _check_cancel(cancelled)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.",
        suffix=".partial",
        dir=output.parent,
    )
    os.close(file_descriptor)
    temporary = Path(temporary_name)
    try:
        _run_process(
            arguments_for(temporary),
            tool_name=tool_name,
            operation=operation,
            timeout_seconds=timeout_seconds,
            cancelled=cancelled,
            output_path=temporary,
            max_output_size=effective_output_limit,
        )
        if not temporary.is_file():
            raise StorageError(
                f"{operation}失败：{tool_name} 未生成输出文件。"
            )
        integrity = file_integrity(temporary, cancelled=cancelled)
        _assert_integrity(
            integrity,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
            label=operation,
        )
        try:
            os.replace(temporary, output)
        except OSError as exc:
            raise StorageError(
                f"{operation}结果无法写入目标路径：{output}：{exc}"
            ) from exc
        return integrity
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _run_process(
    arguments: Sequence[str],
    *,
    tool_name: str,
    operation: str,
    timeout_seconds: float,
    cancelled: CancelCallback | None,
    output_path: Path | None = None,
    max_output_size: int | None = None,
) -> None:
    _check_cancel(cancelled)
    started = time.monotonic()
    with tempfile.TemporaryFile() as diagnostics:
        try:
            process = subprocess.Popen(
                list(arguments),
                stdin=subprocess.DEVNULL,
                stdout=diagnostics,
                stderr=subprocess.STDOUT,
                shell=False,
                close_fds=True,
                creationflags=int(
                    getattr(subprocess, "CREATE_NO_WINDOW", 0)
                ),
            )
        except FileNotFoundError as exc:
            raise ToolMissingError(
                f"{operation}失败：未找到 {tool_name} 命令。"
            ) from exc
        except OSError as exc:
            raise StorageError(
                f"{operation}无法启动 {tool_name}：{exc}"
            ) from exc

        while True:
            if cancelled and cancelled():
                _stop_process(process)
                raise CancelledError(f"{operation}已取消。")
            _enforce_output_limit(
                process,
                output_path,
                max_output_size,
                operation=operation,
                tool_name=tool_name,
            )
            remaining = timeout_seconds - (time.monotonic() - started)
            if remaining <= 0:
                _stop_process(process)
                raise StorageError(
                    f"{operation}超时（{timeout_seconds:g} 秒），"
                    f"已终止 {tool_name} 进程。"
                )
            try:
                return_code = process.wait(
                    timeout=min(_POLL_SECONDS, remaining)
                )
                break
            except subprocess.TimeoutExpired:
                continue

        _enforce_output_limit(
            process,
            output_path,
            max_output_size,
            operation=operation,
            tool_name=tool_name,
        )
        if return_code:
            diagnostics.flush()
            diagnostics.seek(0, os.SEEK_END)
            length = diagnostics.tell()
            diagnostics.seek(max(0, length - _MAX_DIAGNOSTIC_BYTES))
            detail = diagnostics.read().decode(
                "utf-8", errors="replace"
            ).strip()
            suffix = f"：{detail}" if detail else ""
            raise StorageError(
                f"{operation}失败（{tool_name} 退出码 "
                f"{return_code}）{suffix}"
            )


def _enforce_output_limit(
    process: subprocess.Popen[bytes],
    output_path: Path | None,
    max_output_size: int | None,
    *,
    operation: str,
    tool_name: str,
) -> None:
    if output_path is None or max_output_size is None:
        return
    try:
        actual_size = output_path.stat().st_size
    except OSError:
        return
    if actual_size <= max_output_size:
        return
    _stop_process(process)
    raise StorageError(
        f"{operation}输出超过允许上限 {max_output_size} 字节，"
        f"已终止 {tool_name} 进程。"
    )


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=5)
        return
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        process.kill()
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _check_cancel(cancelled: CancelCallback | None) -> None:
    if cancelled and cancelled():
        raise CancelledError("操作已取消。")
