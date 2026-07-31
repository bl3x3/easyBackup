from __future__ import annotations

import errno
import io
import posixpath
import stat as stat_module
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

from easybackup.errors import CancelledError, StorageError
from easybackup.models import SFTPStorageConfig
from easybackup.storage.sftp import (
    HostKeyFingerprintMismatchError,
    SFTPBlobStore,
    UnknownHostKeyError,
    diagnose_sftp_error,
)


@dataclass
class _SFTPAttributes:
    filename: str
    st_size: int
    st_mtime: int
    st_mode: int


class _MemorySFTPServer:
    """Thread-safe POSIX filesystem shared by independent fake sessions."""

    def __init__(self):
        self.lock = threading.RLock()
        self.files: dict[str, bytes] = {}
        self.directories = {"/"}
        self.mtimes: dict[str, int] = {"/": int(time.time())}
        self.posix_renames: list[tuple[str, str]] = []
        self.exclusive_opens: list[str] = []
        self.fail_next: dict[str, BaseException] = {}
        self.supports_posix_rename = True
        self.supports_open_exclusive = True

    @staticmethod
    def normalize(path: str) -> str:
        value = str(path).replace("\\", "/")
        if not value.startswith("/"):
            value = f"/{value}"
        normalized = posixpath.normpath(value)
        return normalized if normalized.startswith("/") else f"/{normalized}"

    def _raise_failure(self, operation: str) -> None:
        failure = self.fail_next.pop(operation, None)
        if failure is not None:
            raise failure

    def _require_parent(self, path: str) -> None:
        parent = posixpath.dirname(path) or "/"
        if parent not in self.directories:
            raise FileNotFoundError(errno.ENOENT, "parent does not exist", parent)

    def make_directory(self, path: str) -> None:
        path = self.normalize(path)
        with self.lock:
            self._raise_failure("mkdir")
            self._require_parent(path)
            if path in self.directories or path in self.files:
                raise FileExistsError(errno.EEXIST, "already exists", path)
            self.directories.add(path)
            self.mtimes[path] = int(time.time())

    def seed_directory(self, path: str) -> None:
        path = self.normalize(path)
        parts = path.strip("/").split("/") if path != "/" else []
        current = ""
        with self.lock:
            for part in parts:
                current = f"{current}/{part}"
                self.directories.add(current)
                self.mtimes.setdefault(current, int(time.time()))

    def seed_file(self, path: str, payload: bytes) -> None:
        path = self.normalize(path)
        self.seed_directory(posixpath.dirname(path))
        with self.lock:
            self.files[path] = bytes(payload)
            self.mtimes[path] = int(time.time())

    def attributes(self, path: str, *, filename: str | None = None) -> _SFTPAttributes:
        path = self.normalize(path)
        with self.lock:
            if path in self.files:
                return _SFTPAttributes(
                    filename=filename or posixpath.basename(path),
                    st_size=len(self.files[path]),
                    st_mtime=self.mtimes[path],
                    st_mode=stat_module.S_IFREG | 0o600,
                )
            if path in self.directories:
                return _SFTPAttributes(
                    filename=filename or posixpath.basename(path),
                    st_size=0,
                    st_mtime=self.mtimes[path],
                    st_mode=stat_module.S_IFDIR | 0o700,
                )
        raise FileNotFoundError(errno.ENOENT, "not found", path)

    def list_directory(self, path: str) -> list[_SFTPAttributes]:
        path = self.normalize(path)
        with self.lock:
            self._raise_failure("listdir_attr")
            if path not in self.directories:
                raise FileNotFoundError(errno.ENOENT, "not found", path)
            prefix = "/" if path == "/" else f"{path}/"
            names: set[str] = set()
            for candidate in [*self.directories, *self.files]:
                if candidate == path or not candidate.startswith(prefix):
                    continue
                remainder = candidate[len(prefix) :]
                if remainder and "/" not in remainder:
                    names.add(remainder)
            return [
                self.attributes(f"{prefix}{name}", filename=name)
                for name in sorted(names)
            ]

    def remove_file(self, path: str) -> None:
        path = self.normalize(path)
        with self.lock:
            self._raise_failure("remove")
            if path not in self.files:
                raise FileNotFoundError(errno.ENOENT, "not found", path)
            del self.files[path]
            self.mtimes.pop(path, None)

    def remove_directory(self, path: str) -> None:
        path = self.normalize(path)
        with self.lock:
            prefix = f"{path}/"
            if path not in self.directories:
                raise FileNotFoundError(errno.ENOENT, "not found", path)
            if any(
                candidate.startswith(prefix)
                for candidate in [*self.directories, *self.files]
                if candidate != path
            ):
                raise OSError(errno.ENOTEMPTY, "directory not empty", path)
            self.directories.remove(path)
            self.mtimes.pop(path, None)

    def move_file(self, source: str, destination: str, *, replace: bool) -> None:
        source = self.normalize(source)
        destination = self.normalize(destination)
        with self.lock:
            self._require_parent(destination)
            if source not in self.files:
                raise FileNotFoundError(errno.ENOENT, "not found", source)
            if not replace and (
                destination in self.files or destination in self.directories
            ):
                raise FileExistsError(errno.EEXIST, "already exists", destination)
            self.files[destination] = self.files.pop(source)
            self.mtimes[destination] = int(time.time())
            self.mtimes.pop(source, None)


class _MemorySFTPFile(io.BytesIO):
    def __init__(
        self,
        client: "_MemorySFTPClient",
        path: str,
        mode: str,
        initial: bytes,
    ):
        super().__init__(initial)
        self.client = client
        self.path = path
        self.mode = mode
        self.writable_mode = any(flag in mode for flag in ("w", "a", "x", "+"))
        if "a" in mode:
            self.seek(0, io.SEEK_END)
        elif "w" in mode or "x" in mode:
            self.seek(0)

    def _ensure_live(self) -> None:
        if self.client.closed:
            raise OSError(errno.EBADF, "SFTP session is closed")

    def read(self, size: int = -1) -> bytes:
        self._ensure_live()
        return super().read(size)

    def write(self, payload: bytes) -> int:
        self._ensure_live()
        self.client.server._raise_failure("write")
        written = super().write(payload)
        self.flush()
        return written

    def flush(self) -> None:
        if self.closed:
            return
        self._ensure_live()
        if self.writable_mode:
            with self.client.server.lock:
                self.client.server.files[self.path] = self.getvalue()
                self.client.server.mtimes[self.path] = int(time.time())

    def set_pipelined(self, value: bool = True) -> None:
        del value

    def close(self) -> None:
        if not self.closed:
            self.flush()
        super().close()


class _MemorySFTPClient:
    def __init__(
        self,
        server: _MemorySFTPServer,
        *,
        enter_error: BaseException | None = None,
    ):
        self.server = server
        self.enter_error = enter_error
        self.closed = False

    def __enter__(self) -> "_MemorySFTPClient":
        if self.enter_error is not None:
            raise self.enter_error
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        self.close()

    def _ensure_open(self) -> None:
        if self.closed:
            raise OSError(errno.EBADF, "SFTP session is closed")

    def close(self) -> None:
        self.closed = True

    def normalize(self, path: str) -> str:
        self._ensure_open()
        return self.server.normalize(path)

    def stat(self, path: str) -> _SFTPAttributes:
        self._ensure_open()
        self.server._raise_failure("stat")
        return self.server.attributes(path)

    lstat = stat

    def mkdir(self, path: str, mode: int = 0o777) -> None:
        del mode
        self._ensure_open()
        self.server.make_directory(path)

    def rmdir(self, path: str) -> None:
        self._ensure_open()
        self.server.remove_directory(path)

    def listdir_attr(self, path: str) -> list[_SFTPAttributes]:
        self._ensure_open()
        return self.server.list_directory(path)

    def open(self, path: str, mode: str = "r", bufsize: int = -1) -> _MemorySFTPFile:
        del bufsize
        self._ensure_open()
        path = self.server.normalize(path)
        with self.server.lock:
            self.server._raise_failure("open")
            if "x" in mode:
                if not self.server.supports_open_exclusive:
                    raise OSError(errno.ENOSYS, "OPEN_EXCL unsupported", path)
                self.server.exclusive_opens.append(path)
                if path in self.server.files or path in self.server.directories:
                    raise FileExistsError(errno.EEXIST, "already exists", path)
                self.server._require_parent(path)
                self.server.files[path] = b""
                self.server.mtimes[path] = int(time.time())
                initial = b""
            elif "w" in mode:
                self.server._require_parent(path)
                self.server.files[path] = b""
                self.server.mtimes[path] = int(time.time())
                initial = b""
            else:
                if path not in self.server.files:
                    raise FileNotFoundError(errno.ENOENT, "not found", path)
                initial = self.server.files[path]
        return _MemorySFTPFile(self, path, mode, initial)

    file = open

    def remove(self, path: str) -> None:
        self._ensure_open()
        self.server.remove_file(path)

    unlink = remove

    def posix_rename(self, source: str, destination: str) -> None:
        self._ensure_open()
        if not self.server.supports_posix_rename:
            raise OSError(errno.ENOSYS, "posix-rename unsupported")
        source = self.server.normalize(source)
        destination = self.server.normalize(destination)
        self.server.posix_renames.append((source, destination))
        self.server.move_file(source, destination, replace=True)

    def rename(self, source: str, destination: str) -> None:
        self._ensure_open()
        self.server.move_file(source, destination, replace=False)


class _SessionFactory:
    def __init__(self, server: _MemorySFTPServer):
        self.server = server
        self.clients: list[_MemorySFTPClient] = []
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.enter_error: BaseException | None = None

    def __call__(self, *args: Any, **kwargs: Any) -> _MemorySFTPClient:
        self.calls.append((args, kwargs))
        client = _MemorySFTPClient(
            self.server,
            enter_error=self.enter_error,
        )
        self.clients.append(client)
        return client


def _config() -> SFTPStorageConfig:
    return SFTPStorageConfig(
        host="backup.internal.example",
        port=22,
        base_path="/vault/easybackup",
        credential_profile="sftp-prod",
        host_key_fingerprint=(
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ),
    )


def _credentials() -> dict[str, Any]:
    return {
        "kind": "sftp",
        "username": "backup-user",
        "auth_method": "password",
        "password": "correct horse battery staple",
    }


def test_sftp_config_rejects_ambiguous_host_key_sources_and_unsafe_paths():
    with pytest.raises(PydanticValidationError):
        SFTPStorageConfig(
            host="backup.internal.example",
            base_path="../outside",
        )
    with pytest.raises(PydanticValidationError) as raised:
        SFTPStorageConfig(
            host="backup.internal.example",
            host_key_fingerprint=(
                "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ),
            known_hosts_path=r"C:\Users\backup\.ssh\known_hosts",
        )
    assert "只能选择一种" in str(raised.value)


@pytest.fixture
def sftp_store():
    server = _MemorySFTPServer()
    server.seed_directory("/vault/easybackup")
    factory = _SessionFactory(server)
    store = SFTPBlobStore(
        _config(),
        _credentials(),
        session_factory=factory,
    )
    return store, server, factory


def test_sftp_streaming_io_ranges_iteration_and_independent_sessions(sftp_store):
    store, server, factory = sftp_store
    payload = b"0123456789" * 150_000
    progress: list[int] = []

    stored = store.put_stream(
        "tree/large.bin",
        io.BytesIO(payload),
        progress=progress.append,
        metadata={"easybackup-artifact": "test"},
    )

    assert stored.key == "tree/large.bin"
    assert stored.size == len(payload)
    assert progress and progress[-1] == len(payload)
    assert store.read_range("tree/large.bin", 7, 19) == payload[7:26]
    assert store.stat("tree/large.bin").size == len(payload)
    store.put_bytes("tree/nested/small.txt", b"small")
    assert [item.key for item in store.iter_objects("tree")] == [
        "tree/large.bin",
        "tree/nested/small.txt",
    ]

    stream = store.open_read("tree/large.bin")
    open_client = factory.clients[-1]
    assert open_client.closed is False
    assert stream.read(10) == payload[:10]
    stream.close()
    assert open_client.closed is True

    store.delete("tree/nested/small.txt")
    store.delete("tree/nested/small.txt")
    assert store.stat("tree/nested/small.txt") is None
    assert len(factory.clients) >= 8
    assert all(client.closed for client in factory.clients)
    assert server.posix_renames
    assert not any(".part" in path for path in server.files)


@pytest.mark.parametrize(
    "key",
    ["", "../escape.bin", "safe/../../escape.bin", "/absolute.bin"],
)
def test_sftp_rejects_unsafe_object_keys(
    sftp_store,
    key,
):
    store, _server, _factory = sftp_store

    with pytest.raises(StorageError):
        store.put_bytes(key, b"unsafe")


def test_sftp_cancelled_upload_removes_temporary_file(sftp_store):
    store, server, _factory = sftp_store
    checks = 0

    def cancelled() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 2

    with pytest.raises(CancelledError):
        store.put_stream(
            "cancelled/archive.bin",
            io.BytesIO(b"x" * (2 * 1024 * 1024 + 17)),
            cancelled=cancelled,
        )

    assert "/vault/easybackup/cancelled/archive.bin" not in server.files
    assert not any(".part" in path for path in server.files)


def test_sftp_validates_atomic_posix_rename_and_open_exclusive(sftp_store):
    store, server, factory = sftp_store

    result = store.validate_capabilities()

    assert result == {
        "exclusive_create": True,
        "atomic_posix_rename": True,
        "concurrent_sessions": True,
        "renewable_lease": True,
    }
    assert server.posix_renames
    assert server.exclusive_opens
    assert not any("capabilit" in path for path in server.files)
    assert all(client.closed for client in factory.clients)


@pytest.mark.parametrize(
    ("capability", "expected_kind"),
    [
        ("posix_rename", "atomic_rename_unsupported"),
        ("open_exclusive", "exclusive_create_unsupported"),
    ],
)
def test_sftp_rejects_server_without_required_atomic_capabilities(
    sftp_store,
    capability,
    expected_kind,
):
    store, server, _factory = sftp_store
    if capability == "posix_rename":
        server.supports_posix_rename = False
    else:
        server.supports_open_exclusive = False

    with pytest.raises(StorageError) as raised:
        store.validate_capabilities()

    diagnostic = raised.value.details["diagnostic"]
    assert diagnostic["kind"] == expected_kind


def test_sftp_two_stores_contend_for_one_renewable_lease():
    server = _MemorySFTPServer()
    server.seed_directory("/vault/easybackup")
    first = SFTPBlobStore(
        _config(),
        _credentials(),
        session_factory=_SessionFactory(server),
    )
    second = SFTPBlobStore(
        _config(),
        _credentials(),
        session_factory=_SessionFactory(server),
    )
    barrier = threading.Barrier(3)
    results: list[Any] = []

    def acquire(store: SFTPBlobStore, owner: str) -> None:
        barrier.wait()
        results.append(store.acquire_lease("locks/task.json", owner, 60))

    threads = [
        threading.Thread(target=acquire, args=(first, "host-one")),
        threading.Thread(target=acquire, args=(second, "host-two")),
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=5)
        assert thread.is_alive() is False

    leases = [lease for lease in results if lease is not None]
    assert len(leases) == 1
    lease = leases[0]
    assert lease.owner in {"host-one", "host-two"}

    winner = first if lease.owner == "host-one" else second
    loser = second if winner is first else first
    renewed = winner.renew_lease(lease, 120)
    assert renewed is not None
    assert renewed.token == lease.token
    assert datetime.fromisoformat(renewed.expires_at) > datetime.now(timezone.utc)

    forged = replace(renewed, token="not-the-owner-token")
    assert loser.renew_lease(forged, 120) is None
    loser.release_lease(forged)
    assert loser.acquire_lease("locks/task.json", "host-three", 60) is None

    winner.release_lease(renewed)
    replacement = loser.acquire_lease("locks/task.json", "host-three", 60)
    assert replacement is not None
    assert replacement.token != renewed.token


def test_sftp_stale_cas_guard_fails_closed_without_deleting_it(
    sftp_store,
    monkeypatch,
):
    store, server, _factory = sftp_store
    guard = "/vault/easybackup/locks/task.json.cas.guard"
    server.seed_file(guard, b"owner process disappeared")
    monkeypatch.setattr(
        "easybackup.storage.sftp.time.sleep",
        lambda _seconds: None,
    )

    with pytest.raises(StorageError) as raised:
        store.acquire_lease("locks/task.json", "new-owner", 60)

    assert raised.value.details["diagnostic"]["kind"] == "stale_lease_guard"
    assert server.files[guard] == b"owner process disappeared"


@pytest.mark.parametrize(
    ("failure", "expected_kind"),
    [
        (PermissionError(errno.EACCES, "permission denied"), "permission"),
        (TimeoutError("connection timed out"), "timeout"),
        (
            ConnectionRefusedError(errno.ECONNREFUSED, "connection refused"),
            "connection_refused",
        ),
    ],
)
def test_sftp_errors_are_classified_and_do_not_expose_credentials(
    sftp_store,
    failure,
    expected_kind,
):
    store, server, factory = sftp_store
    if isinstance(failure, PermissionError):
        server.fail_next["open"] = failure
    else:
        factory.enter_error = failure

    with pytest.raises(StorageError) as raised:
        store.put_bytes("diagnostics/probe.bin", b"probe")

    diagnostic = raised.value.details["diagnostic"]
    assert diagnostic["kind"] == expected_kind
    assert diagnostic["title"]
    assert diagnostic["summary"]
    assert diagnostic["suggestions"]
    serialized = f"{raised.value} {raised.value.details}"
    assert _credentials()["password"] not in serialized
    assert "backup-user" not in diagnostic["summary"]


@pytest.mark.parametrize(
    ("error", "expected_kind"),
    [
        (
            UnknownHostKeyError(
                "backup.internal.example",
                "ssh-ed25519",
                "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
            ),
            "host_key_untrusted",
        ),
        (
            HostKeyFingerprintMismatchError(
                "backup.internal.example",
                "ssh-ed25519",
                "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
            ),
            "host_key_mismatch",
        ),
    ],
)
def test_sftp_host_key_errors_have_safe_actionable_diagnostics(
    error,
    expected_kind,
):
    diagnostic = diagnose_sftp_error(
        error,
        config=_config(),
        operation="建立 SFTP 会话",
    )

    assert diagnostic["kind"] == expected_kind
    assert diagnostic["title"]
    assert diagnostic["summary"]
    assert diagnostic["suggestions"]
    assert diagnostic["host"] == "backup.internal.example"
    assert diagnostic["host_key_type"] == "ssh-ed25519"
    assert diagnostic["observed_host_key_fingerprint"].startswith("SHA256:")
    if expected_kind == "host_key_mismatch":
        assert diagnostic["expected_host_key_fingerprint"] == (
            "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        )
