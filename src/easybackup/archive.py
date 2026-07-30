"""Streaming tar/zstd pipelines, portable gzip fallback and archive readers."""

from __future__ import annotations

import contextlib
import hashlib
import io
import os
import shutil
import subprocess
import sys
import tarfile
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Callable, Iterator, Literal

from easybackup.errors import (
    CancelledError,
    EasyBackupError,
    StorageError,
    ToolMissingError,
    ValidationError,
)
from easybackup.models import ArchiveIntegrity
from easybackup.scanner import (
    ScannedFile,
    assert_file_unchanged,
    assert_stat_unchanged,
)


Codec = Literal["zstd", "gzip", "none"]
CancelCallback = Callable[[], bool]


def find_tool(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    if os.name != "nt":
        return None
    executable = name if name.lower().endswith(".exe") else f"{name}.exe"
    candidates: list[Path] = []
    for root in {
        Path(sys.base_prefix),
        Path(sys.prefix),
        Path(os.environ["CONDA_PREFIX"]) if os.environ.get("CONDA_PREFIX") else None,
    }:
        if root is None:
            continue
        candidates.extend(
            [
                root / "Library" / "bin" / executable,
                root / "Scripts" / executable,
                root / executable,
            ]
        )
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    return None


def tool_capabilities(
    xdelta3_path: str | None = None,
) -> dict[str, dict[str, str | bool | None]]:
    result: dict[str, dict[str, str | bool | None]] = {
        "python_tar": {
            "available": True,
            "path": None,
            "version": f"Python {sys.version_info.major}.{sys.version_info.minor} tarfile",
        }
    }
    version_arguments = {
        "zstd": ["--version"],
        "xdelta3": ["-V"],
    }
    for name, version_options in version_arguments.items():
        path = None
        if name == "xdelta3" and xdelta3_path:
            candidate = Path(xdelta3_path).expanduser()
            try:
                resolved = candidate.resolve(strict=True)
            except OSError:
                pass
            else:
                path = str(resolved) if resolved.is_file() else None
        else:
            path = find_tool(name)
        version: str | None = None
        if path:
            arguments = [path, *version_options]
            try:
                completed = subprocess.run(
                    arguments,
                    capture_output=True,
                    text=True,
                    timeout=3,
                    check=False,
                )
                output = (completed.stdout or completed.stderr).strip()
                version = output.splitlines()[0][:160] if output else None
            except (OSError, subprocess.SubprocessError):
                version = None
        result[name] = {"available": bool(path), "path": path, "version": version}
    return result


def resolve_codec(requested: str) -> Codec:
    if requested == "auto":
        return "zstd" if find_tool("zstd") else "gzip"
    if requested == "zstd":
        if not find_tool("zstd"):
            raise ToolMissingError(
                "zstd 压缩需要 zstd 命令；可改用 auto/gzip，或安装相应工具。"
            )
        return "zstd"
    if requested in {"gzip", "none"}:
        return requested  # type: ignore[return-value]
    raise ToolMissingError(f"不支持的压缩方式：{requested}")


def extension_for(codec: Codec) -> str:
    return {"zstd": ".tar.zst", "gzip": ".tar.gz", "none": ".tar"}[codec]


def plan_shards(
    files: list[ScannedFile],
    target_size: int,
) -> list[list[ScannedFile]]:
    if target_size <= 0:
        raise ValueError("target_size 必须为正数")
    shards: list[list[ScannedFile]] = []
    current: list[ScannedFile] = []
    current_size = 0
    for item in sorted(files, key=lambda value: value.path):
        if current and current_size + item.size > target_size:
            shards.append(current)
            current = []
            current_size = 0
        current.append(item)
        current_size += item.size
        if item.size >= target_size:
            shards.append(current)
            current = []
            current_size = 0
    if current:
        shards.append(current)
    return shards


class IntegrityReader:
    """A non-seekable reader that hashes the exact bytes consumed by storage."""

    def __init__(self, raw: BinaryIO, block_size: int):
        self.raw = raw
        self.block_size = block_size
        self.size = 0
        self._sha256 = hashlib.sha256()
        self._block_buffer = bytearray()
        self._crc32: list[str] = []
        self._finished = False

    def read(self, size: int = -1) -> bytes:
        data = self.raw.read(size)
        if data:
            self.size += len(data)
            self._sha256.update(data)
            view = memoryview(data)
            offset = 0
            while offset < len(view):
                take = min(self.block_size - len(self._block_buffer), len(view) - offset)
                self._block_buffer.extend(view[offset : offset + take])
                offset += take
                if len(self._block_buffer) == self.block_size:
                    self._crc32.append(f"{zlib.crc32(self._block_buffer):08x}")
                    self._block_buffer.clear()
        else:
            self.finish()
        return data

    def seekable(self) -> bool:
        return False

    def finish(self) -> None:
        if self._finished:
            return
        if self._block_buffer:
            self._crc32.append(f"{zlib.crc32(self._block_buffer):08x}")
            self._block_buffer.clear()
        self._finished = True

    @property
    def integrity(self) -> ArchiveIntegrity:
        self.finish()
        return ArchiveIntegrity(
            sha256=self._sha256.hexdigest(),
            size=self.size,
            block_size=self.block_size,
            crc32=list(self._crc32),
        )

    def close(self) -> None:
        self.raw.close()


class _VerifiedSourceReader:
    """Read exactly the scanned file version and hash the archived bytes."""

    def __init__(self, source: Path, item: ScannedFile):
        self.source = source
        self.item = item
        self.handle: BinaryIO | None = None
        self.size = 0
        self.digest = hashlib.sha256()

    def __enter__(self) -> "_VerifiedSourceReader":
        assert_file_unchanged(self.item, self.source)
        flags = os.O_RDONLY
        flags |= int(getattr(os, "O_BINARY", 0))
        flags |= int(getattr(os, "O_NOFOLLOW", 0))
        try:
            descriptor = os.open(self.item.absolute_path, flags)
            self.handle = os.fdopen(descriptor, "rb", buffering=0)
            assert_stat_unchanged(
                self.item,
                os.fstat(self.handle.fileno()),
                # Windows reports a different creation/change timestamp
                # through fstat immediately after open.  Canonical path
                # checks still compare ctime before and after the read.
                check_ctime=os.name != "nt",
            )
        except EasyBackupError:
            if self.handle:
                self.handle.close()
            raise
        except OSError as exc:
            if self.handle:
                self.handle.close()
            raise ValidationError(
                f"无法安全打开归档源文件 {self.item.path}：{exc}"
            ) from exc
        return self

    def read(self, length: int = -1) -> bytes:
        assert self.handle is not None
        data = self.handle.read(length)
        if data:
            self.size += len(data)
            self.digest.update(data)
        return data

    def verify(self) -> None:
        assert self.handle is not None
        assert_stat_unchanged(
            self.item,
            os.fstat(self.handle.fileno()),
            check_ctime=os.name != "nt",
        )
        if (
            self.size != self.item.size
            or self.digest.hexdigest() != self.item.sha256
        ):
            raise ValidationError(
                f"归档字节与扫描摘要不一致：{self.item.path}；"
                "本次快照已安全中止，请重试。"
            )

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.handle:
            self.handle.close()


def _add_verified_files(
    archive: tarfile.TarFile,
    source: Path,
    files: list[ScannedFile],
) -> None:
    """Add regular files from pinned paths with descriptor-level checks."""

    for item in files:
        with _VerifiedSourceReader(source, item) as reader:
            info = tarfile.TarInfo(item.path)
            info.type = tarfile.REGTYPE
            info.size = item.size
            info.mode = item.mode
            info.mtime = item.mtime_ns // 1_000_000_000
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            archive.addfile(info, reader)
            reader.verify()
        assert_file_unchanged(item, source)


class _PythonTarStream:
    def __init__(
        self,
        source: Path,
        files: list[ScannedFile],
        codec: Codec,
        compression_level: int,
    ):
        self.source = source
        self.files = files
        self.codec = codec
        self.compression_level = compression_level
        self.reader: BinaryIO | None = None
        self.thread: threading.Thread | None = None
        self.error: BaseException | None = None

    def __enter__(self) -> BinaryIO:
        read_fd, write_fd = os.pipe()
        self.reader = os.fdopen(read_fd, "rb", buffering=0)

        def produce() -> None:
            try:
                with os.fdopen(write_fd, "wb", buffering=0) as output:
                    mode = "w|gz" if self.codec == "gzip" else "w|"
                    options = (
                        {
                            "compresslevel": max(
                                1, min(self.compression_level, 9)
                            )
                        }
                        if self.codec == "gzip"
                        else {}
                    )
                    with tarfile.open(
                        fileobj=output,
                        mode=mode,
                        format=tarfile.PAX_FORMAT,
                        **options,
                    ) as archive:
                        _add_verified_files(
                            archive, self.source, self.files
                        )
            except BrokenPipeError:
                pass
            except BaseException as exc:
                self.error = exc

        self.thread = threading.Thread(
            target=produce, name="easybackup-tar-producer", daemon=True
        )
        self.thread.start()
        return self.reader

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.reader:
            self.reader.close()
        if self.thread:
            self.thread.join(timeout=10)
        if exc_type is None and self.error:
            if isinstance(self.error, EasyBackupError):
                raise self.error
            raise StorageError(f"生成 tar 流失败：{self.error}") from self.error


class _PythonTarZstdStream:
    """Verified portable tar writer piped into the native zstd process."""

    def __init__(
        self,
        source: Path,
        files: list[ScannedFile],
        level: int,
    ):
        self.source = source
        self.files = files
        self.level = level
        self.process: subprocess.Popen | None = None
        self.producer: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None
        self.error: BaseException | None = None
        self.stderr = bytearray()

    def __enter__(self) -> BinaryIO:
        zstd_path = find_tool("zstd")
        if not zstd_path:
            raise ToolMissingError("未找到 zstd 命令。")
        self.process = subprocess.Popen(
            [zstd_path, "-T0", f"-{self.level}", "-q", "-c"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
        )

        def produce() -> None:
            assert self.process is not None and self.process.stdin is not None
            try:
                with self.process.stdin:
                    with tarfile.open(
                        fileobj=self.process.stdin,
                        mode="w|",
                        format=tarfile.PAX_FORMAT,
                    ) as archive:
                        _add_verified_files(
                            archive, self.source, self.files
                        )
            except BrokenPipeError:
                pass
            except BaseException as exc:
                self.error = exc

        def drain() -> None:
            assert self.process is not None and self.process.stderr is not None
            with self.process.stderr:
                while True:
                    chunk = self.process.stderr.read(8192)
                    if not chunk:
                        return
                    self.stderr.extend(chunk)

        self.producer = threading.Thread(
            target=produce,
            name="easybackup-tar-zstd-producer",
            daemon=True,
        )
        self.stderr_thread = threading.Thread(target=drain, daemon=True)
        self.producer.start()
        self.stderr_thread.start()
        assert self.process.stdout is not None
        return self.process.stdout

    def __exit__(self, exc_type, exc, traceback) -> None:
        assert self.process is not None
        if self.process.stdout:
            self.process.stdout.close()
        if exc_type is not None and self.process.poll() is None:
            self.process.terminate()
        if self.producer:
            self.producer.join(timeout=10)
        try:
            code = self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            code = self.process.wait(timeout=5)
        if self.stderr_thread:
            self.stderr_thread.join(timeout=2)
        if exc_type is None and self.error:
            if isinstance(self.error, EasyBackupError):
                raise self.error
            raise StorageError(f"生成 tar 流失败：{self.error}") from self.error
        if exc_type is None and code:
            detail = self.stderr.decode("utf-8", errors="replace")[-2000:]
            raise StorageError(f"zstd 压缩失败（{code}）：{detail}")


def create_archive_stream(
    source: Path,
    files: list[ScannedFile],
    codec: Codec,
    compression_level: int,
):
    if codec == "zstd":
        # Python writes the tar headers while native zstd performs compression.
        # This keeps every input bound to the canonical, verified file path;
        # a native tar subprocess would resolve mutable symlinks again.
        return _PythonTarZstdStream(source, files, compression_level)
    return _PythonTarStream(
        source, files, codec, compression_level
    )


class _ZstdReadStream:
    def __init__(
        self,
        compressed: BinaryIO,
        cancelled: CancelCallback | None = None,
    ):
        self.compressed = compressed
        self.cancelled = cancelled
        self.process: subprocess.Popen | None = None
        self.feeder: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None
        self.error: BaseException | None = None
        self.stderr = bytearray()

    def __enter__(self) -> BinaryIO:
        zstd_path = find_tool("zstd")
        if not zstd_path:
            raise ToolMissingError("恢复 zstd 快照需要安装 zstd 命令。")
        self.process = subprocess.Popen(
            [zstd_path, "-d", "-q", "-c"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            creationflags=int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
        )

        def feed() -> None:
            try:
                assert self.process is not None and self.process.stdin is not None
                with self.process.stdin:
                    while True:
                        if self.cancelled and self.cancelled():
                            return
                        chunk = self.compressed.read(1024 * 1024)
                        if not chunk:
                            return
                        self.process.stdin.write(chunk)
            except BrokenPipeError:
                pass
            except BaseException as exc:
                self.error = exc
            finally:
                self.compressed.close()

        def drain() -> None:
            assert self.process is not None and self.process.stderr is not None
            with self.process.stderr:
                while True:
                    chunk = self.process.stderr.read(8192)
                    if not chunk:
                        return
                    self.stderr.extend(chunk)

        self.feeder = threading.Thread(target=feed, daemon=True)
        self.feeder.start()
        self.stderr_thread = threading.Thread(target=drain, daemon=True)
        self.stderr_thread.start()
        assert self.process.stdout is not None
        return self.process.stdout

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self.process and self.process.stdout:
            self.process.stdout.close()
        aborting = bool(
            exc_type is not None
            or (self.cancelled and self.cancelled())
        )
        if aborting:
            try:
                self.compressed.close()
            except OSError:
                pass
            if self.process and self.process.poll() is None:
                self.process.terminate()
        if self.feeder:
            self.feeder.join(timeout=10)
            if self.feeder.is_alive():
                aborting = True
                try:
                    self.compressed.close()
                except OSError:
                    pass
                if self.process and self.process.poll() is None:
                    self.process.terminate()
                self.feeder.join(timeout=2)
        timed_out = False
        if self.process:
            try:
                code = self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                timed_out = True
                self.process.terminate()
                try:
                    code = self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    code = self.process.wait(timeout=5)
        else:
            code = -1
        if self.stderr_thread:
            self.stderr_thread.join(timeout=2)
        if exc_type is None and self.cancelled and self.cancelled():
            raise CancelledError("恢复已取消。")
        if exc_type is None and timed_out:
            raise StorageError("zstd 解压进程未在超时内退出，已强制终止。")
        if self.process:
            if exc_type is None and code:
                detail = self.stderr.decode("utf-8", errors="replace")[-2000:]
                raise StorageError(f"zstd 解压失败（{code}）：{detail}")
        if exc_type is None and self.error:
            raise StorageError(f"读取压缩对象失败：{self.error}") from self.error


@contextlib.contextmanager
def open_tar_archive(
    stream: BinaryIO,
    codec: Codec,
    cancelled: CancelCallback | None = None,
) -> Iterator[tarfile.TarFile]:
    if codec == "zstd":
        try:
            with _ZstdReadStream(stream, cancelled) as uncompressed:
                with tarfile.open(fileobj=uncompressed, mode="r|") as archive:
                    yield archive
        finally:
            # Also covers failures before the zstd feeder thread starts (for
            # example a missing executable or Popen error).  Double close on
            # the normal feeder path is harmless.
            stream.close()
        return
    mode = "r|gz" if codec == "gzip" else "r|"
    try:
        with tarfile.open(fileobj=stream, mode=mode) as archive:
            yield archive
    finally:
        stream.close()


def sha256_stream(
    stream: BinaryIO,
    cancelled: CancelCallback | None = None,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        while True:
            if cancelled and cancelled():
                raise CancelledError("巡检已取消。")
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    finally:
        stream.close()
    return digest.hexdigest(), size
