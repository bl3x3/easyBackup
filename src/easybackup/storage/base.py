"""Storage interface used by backup, restore, retention and scrubbing."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO, Callable, Iterator


ProgressCallback = Callable[[int], None]
CancelCallback = Callable[[], bool]


@dataclass(frozen=True, slots=True)
class ObjectStat:
    key: str
    size: int
    etag: str | None = None
    modified_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class RemoteLease:
    key: str
    owner: str
    token: str
    expires_at: str
    version: str | None


class BlobStore(ABC):
    @abstractmethod
    def put_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        progress: ProgressCallback | None = None,
        cancelled: CancelCallback | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ObjectStat:
        raise NotImplementedError

    @abstractmethod
    def open_read(self, key: str) -> BinaryIO:
        raise NotImplementedError

    @abstractmethod
    def read_range(self, key: str, start: int, length: int) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def stat(self, key: str) -> ObjectStat | None:
        raise NotImplementedError

    @abstractmethod
    def iter_objects(self, prefix: str = "") -> Iterator[ObjectStat]:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def acquire_lease(
        self, key: str, owner: str, ttl_seconds: int
    ) -> RemoteLease | None:
        raise NotImplementedError

    @abstractmethod
    def renew_lease(
        self, lease: RemoteLease, ttl_seconds: int
    ) -> RemoteLease | None:
        raise NotImplementedError

    @abstractmethod
    def release_lease(self, lease: RemoteLease) -> None:
        raise NotImplementedError

    def put_bytes(
        self,
        key: str,
        payload: bytes,
        *,
        metadata: dict[str, str] | None = None,
    ) -> ObjectStat:
        import io

        return self.put_stream(key, io.BytesIO(payload), metadata=metadata)

    def read_bytes(self, key: str) -> bytes:
        stream = self.open_read(key)
        try:
            return stream.read()
        finally:
            stream.close()

    def abort_stale_multipart_uploads(self, older_than_days: int = 7) -> int:
        return 0

