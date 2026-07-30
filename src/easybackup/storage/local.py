"""Atomic local-directory object storage backend."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Iterator

from filelock import FileLock

from easybackup.errors import CancelledError, StorageError
from easybackup.storage.base import (
    BlobStore,
    CancelCallback,
    ObjectStat,
    ProgressCallback,
    RemoteLease,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LocalBlobStore(BlobStore):
    def __init__(self, root: Path):
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        normalized = PurePosixPath(key.replace("\\", "/"))
        if normalized.is_absolute() or not normalized.parts:
            raise StorageError(f"无效对象键：{key!r}")
        if any(part in {"", ".", ".."} for part in normalized.parts):
            raise StorageError(f"对象键包含不安全路径：{key!r}")
        candidate = self.root.joinpath(*normalized.parts).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise StorageError(f"对象键逃逸存储根目录：{key!r}") from exc
        return candidate

    def put_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        progress: ProgressCallback | None = None,
        cancelled: CancelCallback | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ObjectStat:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{secrets.token_hex(8)}.part")
        size = 0
        digest = hashlib.sha256()
        try:
            with temporary.open("xb") as handle:
                while True:
                    if cancelled and cancelled():
                        raise CancelledError("操作已取消。")
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                    if progress:
                        progress(size)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            return ObjectStat(
                key=key,
                size=size,
                etag=digest.hexdigest(),
                modified_at=_now(),
            )
        except (CancelledError, StorageError):
            raise
        except OSError as exc:
            raise StorageError(f"写入本地对象 {key!r} 失败：{exc}") from exc
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def open_read(self, key: str) -> BinaryIO:
        try:
            return self._path(key).open("rb")
        except OSError as exc:
            raise StorageError(f"读取本地对象 {key!r} 失败：{exc}") from exc

    def read_range(self, key: str, start: int, length: int) -> bytes:
        if start < 0 or length < 0:
            raise StorageError("范围参数不能为负数。")
        stream = self.open_read(key)
        try:
            stream.seek(start)
            return stream.read(length)
        finally:
            stream.close()

    def stat(self, key: str) -> ObjectStat | None:
        path = self._path(key)
        try:
            value = path.stat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise StorageError(f"读取对象元数据失败：{exc}") from exc
        return ObjectStat(
            key=key,
            size=value.st_size,
            modified_at=datetime.fromtimestamp(value.st_mtime, timezone.utc),
        )

    def iter_objects(self, prefix: str = "") -> Iterator[ObjectStat]:
        if prefix:
            base = self._path(prefix)
            if base.is_file():
                item = self.stat(prefix)
                if item:
                    yield item
                return
        else:
            base = self.root
        if not base.exists():
            return
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.name.endswith(".guard"):
                continue
            relative = path.relative_to(self.root).as_posix()
            item = self.stat(relative)
            if item:
                yield item

    def delete(self, key: str) -> None:
        path = self._path(key)
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            raise StorageError(f"删除本地对象 {key!r} 失败：{exc}") from exc

    def _lease_guard(self, key: str) -> FileLock:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        return FileLock(str(path.with_suffix(path.suffix + ".guard")))

    def _read_lease(self, key: str) -> tuple[dict, str] | None:
        path = self._path(key)
        try:
            payload = path.read_bytes()
            value = json.loads(payload.decode("utf-8"))
            return value, hashlib.sha256(payload).hexdigest()
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(f"远端锁文件已损坏：{exc}") from exc

    def _write_lease(self, key: str, value: dict) -> str:
        payload = json.dumps(value, sort_keys=True).encode("utf-8")
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{secrets.token_hex(6)}.part")
        try:
            with temporary.open("xb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return hashlib.sha256(payload).hexdigest()

    def acquire_lease(
        self, key: str, owner: str, ttl_seconds: int
    ) -> RemoteLease | None:
        with self._lease_guard(key):
            current = self._read_lease(key)
            now = _now()
            if current:
                value, _ = current
                try:
                    expires = datetime.fromisoformat(value["expires_at"])
                except (KeyError, ValueError, TypeError):
                    expires = now - timedelta(seconds=1)
                if expires > now:
                    return None
            token = secrets.token_urlsafe(24)
            expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
            value = {
                "owner": owner,
                "token": token,
                "acquired_at": now.isoformat(),
                "heartbeat_at": now.isoformat(),
                "expires_at": expires_at,
            }
            version = self._write_lease(key, value)
            return RemoteLease(key, owner, token, expires_at, version)

    def renew_lease(
        self, lease: RemoteLease, ttl_seconds: int
    ) -> RemoteLease | None:
        with self._lease_guard(lease.key):
            current = self._read_lease(lease.key)
            if not current:
                return None
            value, _ = current
            if value.get("token") != lease.token:
                return None
            now = _now()
            expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
            value["heartbeat_at"] = now.isoformat()
            value["expires_at"] = expires_at
            version = self._write_lease(lease.key, value)
            return RemoteLease(
                lease.key, lease.owner, lease.token, expires_at, version
            )

    def release_lease(self, lease: RemoteLease) -> None:
        with self._lease_guard(lease.key):
            current = self._read_lease(lease.key)
            if not current:
                return
            value, _ = current
            if value.get("token") != lease.token:
                return
            value["expires_at"] = _now().isoformat()
            value["released"] = True
            self._write_lease(lease.key, value)

