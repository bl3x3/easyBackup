"""Strict-host-key, atomic SFTP storage backend.

SFTP v3 does not provide an ETag/If-Match primitive.  Lease mutations are
therefore serialized with an OPEN_EXCL guard.  A guard left behind by a
crashed client is never removed automatically: availability is sacrificed
deliberately so two writers can never both believe that they own the target.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import hmac
import io
import json
import posixpath
import secrets
import socket
import stat as stat_module
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

from easybackup.errors import CancelledError, StorageError
from easybackup.models import SFTPStorageConfig
from easybackup.storage.base import (
    BlobStore,
    CancelCallback,
    ObjectStat,
    ProgressCallback,
    RemoteLease,
)


SessionFactory = Callable[
    [SFTPStorageConfig, Mapping[str, Any]],
    AbstractContextManager[Any],
]

_CHUNK_SIZE = 1024 * 1024
_GUARD_SUFFIX = ".cas.guard"
_GUARD_WAIT_SECONDS = 1.0
_GUARD_RETRY_SECONDS = 0.05


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint(key: Any) -> str:
    digest = hashlib.sha256(key.asbytes()).digest()
    encoded = base64.b64encode(digest).decode("ascii").rstrip("=")
    return f"SHA256:{encoded}"


class UnknownHostKeyError(Exception):
    def __init__(self, hostname: str, key_type: str, actual: str):
        super().__init__(f"unknown host key for {hostname}: {actual}")
        self.hostname = hostname
        self.key_type = key_type
        self.actual = actual


class HostKeyFingerprintMismatchError(Exception):
    def __init__(
        self,
        hostname: str,
        key_type: str,
        expected: str,
        actual: str,
    ):
        super().__init__(
            f"host key fingerprint mismatch for {hostname}: "
            f"expected {expected}, got {actual}"
        )
        self.hostname = hostname
        self.key_type = key_type
        self.expected = expected
        self.actual = actual


class PrivateKeyError(Exception):
    pass


class AtomicRenameUnsupportedError(Exception):
    pass


class ExclusiveCreateUnsupportedError(Exception):
    pass


class StaleLeaseGuardError(Exception):
    def __init__(self, guard_key: str):
        super().__init__(
            "a lease CAS guard already exists and was not removed automatically"
        )
        self.guard_key = guard_key


class CorruptLeaseError(Exception):
    pass


class UnsafeRemotePathError(Exception):
    pass


class _ObservedRejectPolicy:
    """Reject an unknown key while retaining its safe verification details."""

    def missing_host_key(self, client: Any, hostname: str, key: Any) -> None:
        del client
        raise UnknownHostKeyError(
            hostname,
            key.get_name(),
            _fingerprint(key),
        )


class _FingerprintPolicy:
    def __init__(self, expected: str):
        self.expected = expected

    def missing_host_key(self, client: Any, hostname: str, key: Any) -> None:
        del client
        actual = _fingerprint(key)
        if not hmac.compare_digest(actual, self.expected):
            raise HostKeyFingerprintMismatchError(
                hostname,
                key.get_name(),
                self.expected,
                actual,
            )


def _load_private_key(paramiko: Any, credentials: Mapping[str, Any]) -> Any:
    private_key = credentials.get("private_key")
    if not private_key:
        raise PrivateKeyError("私钥认证缺少私钥内容。")
    passphrase = credentials.get("private_key_passphrase")
    try:
        return paramiko.PKey.from_private_key(
            io.StringIO(str(private_key)),
            password=str(passphrase) if passphrase is not None else None,
        )
    except Exception as exc:
        raise PrivateKeyError(
            "无法解析私钥；请检查私钥格式以及私钥口令。"
        ) from exc


@contextmanager
def _paramiko_session(
    config: SFTPStorageConfig,
    credentials: Mapping[str, Any],
) -> Iterator[Any]:
    try:
        import paramiko
    except ImportError as exc:
        raise StorageError(
            "SFTP 后端需要安装 Paramiko。",
            details={
                "diagnostic": {
                    "kind": "dependency",
                    "title": "缺少 SFTP 客户端依赖",
                    "summary": "当前 Python 环境未安装 Paramiko。",
                    "suggestions": [
                        "安装项目依赖后重新启动 EasyBackup。",
                    ],
                    "operation": "初始化 SFTP 客户端",
                    "host": config.host,
                    "port": config.port,
                }
            },
        ) from exc

    client = paramiko.SSHClient()
    if config.host_key_fingerprint:
        # An explicitly pinned fingerprint is authoritative.  Do not let a
        # stale user known_hosts entry override that explicit pin.
        client.set_missing_host_key_policy(
            _FingerprintPolicy(config.host_key_fingerprint)
        )
    else:
        known_hosts = (
            Path(config.known_hosts_path).expanduser()
            if config.known_hosts_path
            else None
        )
        try:
            client.load_system_host_keys()
            if known_hosts is not None:
                client.load_host_keys(str(known_hosts))
        except Exception as exc:
            client.close()
            display_path = str(known_hosts or "系统默认 known_hosts")
            raise StorageError(
                f"无法读取 known_hosts 文件：{display_path}",
                details={
                    "diagnostic": {
                        "kind": "known_hosts",
                        "title": "known_hosts 文件不可用",
                        "summary": _safe_error_summary(exc),
                        "suggestions": [
                            "确认路径存在、格式正确，且 EasyBackup 服务账号具有读取权限。",
                            "也可以改为填写服务器的 OpenSSH SHA256 主机密钥指纹。",
                        ],
                        "operation": "加载 SFTP 主机密钥",
                        "host": config.host,
                        "port": config.port,
                    }
                },
            ) from exc
        client.set_missing_host_key_policy(_ObservedRejectPolicy())

    connect_args: dict[str, Any] = {
        "hostname": config.host,
        "port": config.port,
        "username": credentials.get("username"),
        "timeout": config.connect_timeout_seconds,
        "banner_timeout": config.connect_timeout_seconds,
        "auth_timeout": config.connect_timeout_seconds,
        "channel_timeout": config.connect_timeout_seconds,
        "allow_agent": False,
        "look_for_keys": False,
    }
    if credentials.get("auth_method") == "private_key":
        connect_args["pkey"] = _load_private_key(paramiko, credentials)
    else:
        connect_args["password"] = credentials.get("password")

    sftp = None
    try:
        client.connect(**connect_args)
        transport = client.get_transport()
        if transport is not None:
            transport.set_keepalive(30)
        sftp = client.open_sftp()
        channel = sftp.get_channel()
        channel.settimeout(config.connect_timeout_seconds)
        yield sftp
    finally:
        if sftp is not None:
            try:
                sftp.close()
            finally:
                client.close()
        else:
            client.close()


def _exception_chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    pending: list[BaseException] = [exc]
    while pending:
        current = pending.pop(0)
        if id(current) in seen:
            continue
        seen.add(id(current))
        yield current
        nested = getattr(current, "errors", None)
        if isinstance(nested, dict):
            pending.extend(
                item for item in nested.values() if isinstance(item, BaseException)
            )
        cause = current.__cause__ or current.__context__
        if cause is not None:
            pending.append(cause)


def _safe_error_summary(exc: BaseException) -> str:
    value = " ".join(str(exc).split())
    return value[:400] or type(exc).__name__


def diagnose_sftp_error(
    exc: BaseException,
    *,
    config: SFTPStorageConfig,
    operation: str,
) -> dict[str, Any]:
    """Classify SSH/SFTP failures into safe, actionable UI diagnostics."""

    chain = list(_exception_chain(exc))
    class_names = {type(item).__name__ for item in chain}
    searchable = " ".join(_safe_error_summary(item) for item in chain).lower()
    errnos = {
        getattr(item, "errno", None)
        for item in chain
        if getattr(item, "errno", None) is not None
    }

    kind = "protocol"
    title = "SFTP 请求失败"
    summary = _safe_error_summary(exc)
    suggestions = [
        "检查服务器地址、账号权限和 SFTP 服务日志后重试。",
    ]
    extra: dict[str, Any] = {}

    unknown = next(
        (item for item in chain if isinstance(item, UnknownHostKeyError)),
        None,
    )
    mismatch = next(
        (
            item
            for item in chain
            if isinstance(item, HostKeyFingerprintMismatchError)
        ),
        None,
    )
    stale_guard = next(
        (item for item in chain if isinstance(item, StaleLeaseGuardError)),
        None,
    )
    if unknown is not None:
        kind = "host_key_untrusted"
        title = "SFTP 主机密钥尚未信任"
        summary = (
            f"服务器返回 {unknown.key_type} 主机密钥，"
            "EasyBackup 已拒绝首次静默信任。"
        )
        suggestions = [
            "通过服务器管理员或可信控制台核对下方 SHA256 指纹。",
            "确认一致后，将该指纹填入 SFTP 配置；不要在未核对时直接接受。",
        ]
        extra["observed_host_key_fingerprint"] = unknown.actual
        extra["host_key_type"] = unknown.key_type
    elif mismatch is not None or "BadHostKeyException" in class_names:
        kind = "host_key_mismatch"
        title = "SFTP 主机密钥不匹配"
        summary = "服务器主机密钥与已固定的指纹或 known_hosts 记录不一致。"
        suggestions = [
            "停止连接并先确认服务器是否重装、迁移或正在遭受中间人攻击。",
            "仅在通过可信渠道核实新主机密钥后更新配置。",
        ]
        if mismatch is not None:
            extra.update(
                {
                    "expected_host_key_fingerprint": mismatch.expected,
                    "observed_host_key_fingerprint": mismatch.actual,
                    "host_key_type": mismatch.key_type,
                }
            )
        else:
            bad_key = getattr(exc, "key", None)
            if bad_key is not None:
                extra["observed_host_key_fingerprint"] = _fingerprint(bad_key)
    elif stale_guard is not None:
        kind = "stale_lease_guard"
        title = "发现遗留的 SFTP 租约保护文件"
        summary = (
            "上一次客户端可能在租约更新中异常退出；为避免两个写入者并发，"
            "EasyBackup 不会自动删除该文件。"
        )
        suggestions = [
            "先确认所有 EasyBackup 实例均已停止。",
            "由管理员核对后手动删除下方 .cas.guard 文件，再重新检测。",
        ]
        extra["guard_key"] = stale_guard.guard_key
    elif any(isinstance(item, PrivateKeyError) for item in chain) or {
        "PasswordRequiredException",
        "UnknownKeyType",
    } & class_names:
        kind = "private_key"
        title = "SFTP 私钥或私钥口令无效"
        summary = "无法读取用于 SFTP 登录的私钥。"
        suggestions = [
            "粘贴完整的 OpenSSH/PEM 私钥（包括 BEGIN/END 行）。",
            "若私钥已加密，请同时填写正确的私钥口令。",
        ]
    elif {
        "AuthenticationException",
        "BadAuthenticationType",
        "UnableToAuthenticate",
        "AuthenticationError",
    } & class_names or "authentication failed" in searchable:
        kind = "authentication"
        title = "SFTP 身份认证失败"
        summary = "服务器拒绝了当前用户名和认证凭据。"
        suggestions = [
            "核对用户名、密码或公钥是否属于同一账号。",
            "确认服务器允许所选认证方式，并已将公钥加入 authorized_keys。",
        ]
    elif "gaierror" in class_names or any(
        isinstance(item, socket.gaierror) for item in chain
    ) or "name or service not known" in searchable or "getaddrinfo" in searchable:
        kind = "dns"
        title = "无法解析 SFTP 主机名"
        summary = "DNS 未能将配置的主机名解析为可连接地址。"
        suggestions = [
            "检查 Host 拼写和本机 DNS 设置，或临时使用服务器 IP 验证。",
        ]
    elif errno.ECONNREFUSED in errnos or 10061 in errnos or {
        "ConnectionRefusedError",
    } & class_names or "connection refused" in searchable:
        kind = "connection_refused"
        title = "SFTP 连接被拒绝"
        summary = "目标主机可达，但指定端口没有接受 SSH 连接。"
        suggestions = [
            "确认 SSH/SFTP 服务已启动，端口填写正确且防火墙允许访问。",
        ]
    elif {
        "TimeoutError",
        "socket.timeout",
    } & class_names or any(
        isinstance(item, (TimeoutError, socket.timeout)) for item in chain
    ) or "timed out" in searchable:
        kind = "timeout"
        title = "SFTP 连接或操作超时"
        summary = "在配置的超时时间内未完成 SSH/SFTP 操作。"
        suggestions = [
            "检查网络、防火墙、VPN 和服务器负载后重试。",
            "高延迟网络可适当提高连接超时。",
        ]
    elif any(
        isinstance(item, AtomicRenameUnsupportedError) for item in chain
    ) or "posix-rename" in searchable or "operation unsupported" in searchable:
        kind = "atomic_rename_unsupported"
        title = "服务器不支持原子 POSIX rename"
        summary = (
            "该 SFTP 服务缺少安全发布备份对象所需的 "
            "posix-rename@openssh.com 扩展。"
        )
        suggestions = [
            "启用 OpenSSH SFTP 子系统或改用支持 POSIX rename 的 SFTP 服务。",
            "不能通过关闭此检查绕过，否则可能暴露半写入备份。",
        ]
    elif any(
        isinstance(item, ExclusiveCreateUnsupportedError) for item in chain
    ):
        kind = "exclusive_create_unsupported"
        title = "服务器不支持排他创建"
        summary = "SFTP OPEN_EXCL 未能阻止重复创建，无法实现安全远端租约。"
        suggestions = [
            "改用正确实现 SFTP 排他创建语义的服务器。",
            "不能通过关闭远端锁绕过此检查。",
        ]
    elif errno.ENOSPC in errnos or 122 in errnos or any(
        value in searchable
        for value in ("no space left", "disk quota", "quota exceeded")
    ):
        kind = "no_space"
        title = "SFTP 服务器空间或配额不足"
        summary = "服务器无法继续写入备份探针或对象。"
        suggestions = [
            "释放服务器磁盘空间或提高该 SFTP 账号的配额。",
        ]
    elif errno.EACCES in errnos or errno.EPERM in errnos or any(
        value in searchable for value in ("permission denied", "access denied")
    ):
        kind = "permission"
        title = "SFTP 目录权限不足"
        summary = "当前账号没有完成读取、写入、改名或删除所需的权限。"
        suggestions = [
            "授予账号对远端根目录及其子目录的读、写、列举、改名和删除权限。",
        ]
    elif errno.ENOENT in errnos or "no such file" in searchable:
        kind = "remote_path"
        title = "SFTP 远端路径不可用"
        summary = "服务器未找到所需目录或路径组件。"
        suggestions = [
            "确认远端根目录填写正确，且账号允许创建缺失目录。",
        ]
    elif {
        "SSHException",
        "IncompatiblePeer",
        "MessageOrderError",
        "EOFError",
    } & class_names:
        kind = "ssh_negotiation"
        title = "SSH 协议协商失败"
        summary = "客户端未能建立可用的 SSH/SFTP 会话。"
        suggestions = [
            "确认目标端口运行的是 SSH，且服务器启用了 SFTP 子系统。",
            "检查服务器支持的主机密钥、密钥交换和加密算法。",
        ]
    elif any(isinstance(item, CorruptLeaseError) for item in chain):
        kind = "corrupt_lease"
        title = "SFTP 远端租约文件已损坏"
        summary = "无法安全判断当前写入者，已阻止继续操作。"
        suggestions = [
            "停止所有 EasyBackup 实例后，由管理员检查并清理损坏的租约文件。",
        ]
    elif any(isinstance(item, UnsafeRemotePathError) for item in chain):
        kind = "unsafe_remote_path"
        title = "SFTP 路径包含符号链接或非目录组件"
        summary = "远端路径可能逃逸配置的备份根目录，已拒绝访问。"
        suggestions = [
            "使用专属备份目录，并移除路径中的符号链接或普通文件组件。",
        ]

    diagnostic: dict[str, Any] = {
        "kind": kind,
        "title": title,
        "summary": summary,
        "suggestions": suggestions,
        "operation": operation,
        "provider": "sftp",
        "host": config.host,
        "port": config.port,
        "base_path": config.base_path,
        "host_key_verification": (
            "fingerprint"
            if config.host_key_fingerprint
            else (
                "known_hosts"
                if config.known_hosts_path
                else "system_known_hosts"
            )
        ),
    }
    diagnostic.update(extra)
    return diagnostic


def _storage_error(
    exc: BaseException,
    *,
    config: SFTPStorageConfig,
    operation: str,
) -> StorageError:
    if isinstance(exc, StorageError):
        return exc
    diagnostic = diagnose_sftp_error(
        exc,
        config=config,
        operation=operation,
    )
    return StorageError(
        f"{operation}失败：{diagnostic['summary']}",
        details={"diagnostic": diagnostic},
    )


def _is_missing(exc: BaseException) -> bool:
    return (
        isinstance(exc, FileNotFoundError)
        or getattr(exc, "errno", None) == errno.ENOENT
        or "no such file" in str(exc).lower()
    )


def _is_existing(exc: BaseException) -> bool:
    return (
        isinstance(exc, FileExistsError)
        or getattr(exc, "errno", None) == errno.EEXIST
        or "already exists" in str(exc).lower()
        or "file exists" in str(exc).lower()
    )


def _modified_at(attributes: Any) -> datetime | None:
    value = getattr(attributes, "st_mtime", None)
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc)


class _OwnedSFTPFile:
    """A remote file whose close also tears down its SSH/SFTP session."""

    def __init__(self, raw: BinaryIO, session: AbstractContextManager[Any]):
        self._raw = raw
        self._session = session
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def read(self, size: int = -1) -> bytes:
        return self._raw.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._raw.seek(offset, whence)

    def tell(self) -> int:
        return self._raw.tell()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        error: BaseException | None = None
        try:
            self._raw.close()
        except BaseException as exc:
            error = exc
        try:
            self._session.__exit__(
                type(error) if error else None,
                error,
                error.__traceback__ if error else None,
            )
        except BaseException:
            if error is None:
                raise
        if error is not None:
            raise error

    def __enter__(self) -> "_OwnedSFTPFile":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


class SFTPBlobStore(BlobStore):
    def __init__(
        self,
        config: SFTPStorageConfig,
        credentials: Mapping[str, Any],
        session_factory: SessionFactory | None = None,
    ):
        self.config = config
        self.credentials = dict(credentials)
        self.provider = "sftp"
        self._session_factory = session_factory or _paramiko_session

    def _session(self) -> AbstractContextManager[Any]:
        return self._session_factory(self.config, self.credentials)

    @staticmethod
    def _validate_key(key: str) -> tuple[str, ...]:
        if "\x00" in key or "\\" in key:
            raise StorageError(f"无效对象键：{key!r}")
        normalized = PurePosixPath(key)
        if normalized.is_absolute() or not normalized.parts:
            raise StorageError(f"无效对象键：{key!r}")
        if any(part in {"", ".", ".."} for part in normalized.parts):
            raise StorageError(f"对象键包含不安全路径：{key!r}")
        return normalized.parts

    def _root(self, sftp: Any) -> str:
        configured = self.config.base_path
        if configured.startswith("/"):
            return posixpath.normpath(configured)
        home = sftp.normalize(".")
        if not home:
            raise UnsafeRemotePathError("SFTP 未返回登录目录。")
        return posixpath.normpath(posixpath.join(home, configured))

    def _path(self, root: str, key: str) -> str:
        parts = self._validate_key(key)
        return posixpath.join(root, *parts)

    @staticmethod
    def _path_parts(path: str) -> tuple[str, list[str]]:
        normalized = posixpath.normpath(path)
        if normalized.startswith("/"):
            return "/", [part for part in normalized.split("/") if part]
        return "", [part for part in normalized.split("/") if part and part != "."]

    def _ensure_directory(self, sftp: Any, path: str) -> None:
        current, parts = self._path_parts(path)
        for part in parts:
            current = posixpath.join(current, part) if current else part
            try:
                attributes = sftp.lstat(current)
            except Exception as exc:
                if not _is_missing(exc):
                    raise
                try:
                    sftp.mkdir(current, mode=0o700)
                except Exception as create_exc:
                    if not _is_existing(create_exc):
                        try:
                            attributes = sftp.lstat(current)
                        except Exception:
                            raise create_exc
                        else:
                            if not stat_module.S_ISDIR(attributes.st_mode):
                                raise UnsafeRemotePathError(
                                    f"{current!r} 不是目录。"
                                )
                    else:
                        attributes = sftp.lstat(current)
                else:
                    attributes = sftp.lstat(current)
            if stat_module.S_ISLNK(attributes.st_mode):
                raise UnsafeRemotePathError(
                    f"远端目录组件 {current!r} 是符号链接。"
                )
            if not stat_module.S_ISDIR(attributes.st_mode):
                raise UnsafeRemotePathError(
                    f"远端路径组件 {current!r} 不是目录。"
                )

    def _verify_directory(self, sftp: Any, path: str) -> None:
        current, parts = self._path_parts(path)
        for part in parts:
            current = posixpath.join(current, part) if current else part
            attributes = sftp.lstat(current)
            if stat_module.S_ISLNK(attributes.st_mode):
                raise UnsafeRemotePathError(
                    f"远端目录组件 {current!r} 是符号链接。"
                )
            if not stat_module.S_ISDIR(attributes.st_mode):
                raise UnsafeRemotePathError(
                    f"远端路径组件 {current!r} 不是目录。"
                )

    def _prepare_parent(
        self,
        sftp: Any,
        root: str,
        remote_path: str,
        *,
        create: bool,
    ) -> None:
        verify = self._ensure_directory if create else self._verify_directory
        verify(sftp, root)
        parent = posixpath.dirname(remote_path)
        if parent != root:
            verify(sftp, parent)

    @staticmethod
    def _read_all(handle: BinaryIO) -> bytes:
        chunks: list[bytes] = []
        while True:
            chunk = handle.read(_CHUNK_SIZE)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)

    @staticmethod
    def _read_remote_bytes(sftp: Any, path: str) -> bytes:
        handle = sftp.open(path, "rb")
        try:
            return SFTPBlobStore._read_all(handle)
        finally:
            handle.close()

    @staticmethod
    def _remove_if_exists(sftp: Any, path: str) -> None:
        try:
            sftp.remove(path)
        except Exception as exc:
            if not _is_missing(exc):
                raise

    @staticmethod
    def _posix_rename(sftp: Any, source: str, target: str) -> None:
        try:
            rename = getattr(sftp, "posix_rename")
            rename(source, target)
        except (AttributeError, NotImplementedError) as exc:
            raise AtomicRenameUnsupportedError(
                "SFTP server lacks posix-rename@openssh.com"
            ) from exc
        except Exception as exc:
            if (
                getattr(exc, "errno", None) == errno.ENOSYS
                or "unsupported" in str(exc).lower()
                or "unknown extended request" in str(exc).lower()
            ):
                raise AtomicRenameUnsupportedError(
                    "SFTP server lacks posix-rename@openssh.com"
                ) from exc
            raise

    @staticmethod
    def _open_exclusive(sftp: Any, path: str) -> BinaryIO:
        try:
            return sftp.open(path, "x")
        except Exception as exc:
            if (
                isinstance(exc, NotImplementedError)
                or getattr(exc, "errno", None) == errno.ENOSYS
                or "unsupported" in str(exc).lower()
            ):
                raise ExclusiveCreateUnsupportedError(
                    "SFTP server lacks OPEN_EXCL"
                ) from exc
            raise

    def put_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        progress: ProgressCallback | None = None,
        cancelled: CancelCallback | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ObjectStat:
        del metadata
        size = 0
        digest = hashlib.sha256()
        try:
            with self._session() as sftp:
                root = self._root(sftp)
                target = self._path(root, key)
                self._prepare_parent(
                    sftp,
                    root,
                    target,
                    create=True,
                )
                temporary = posixpath.join(
                    posixpath.dirname(target),
                    f".{posixpath.basename(target)}."
                    f"{secrets.token_hex(12)}.part",
                )
                handle = None
                try:
                    handle = self._open_exclusive(sftp, temporary)
                    set_pipelined = getattr(handle, "set_pipelined", None)
                    if callable(set_pipelined):
                        set_pipelined(True)
                    while True:
                        if cancelled and cancelled():
                            raise CancelledError("操作已取消。")
                        chunk = stream.read(_CHUNK_SIZE)
                        if not chunk:
                            break
                        handle.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                        if progress:
                            progress(size)
                    handle.flush()
                    handle.close()
                    handle = None
                    attributes = sftp.lstat(temporary)
                    if attributes.st_size != size:
                        raise OSError(
                            f"远端临时文件大小不一致："
                            f"{attributes.st_size} != {size}"
                        )
                    self._posix_rename(sftp, temporary, target)
                    attributes = sftp.lstat(target)
                    return ObjectStat(
                        key=key,
                        size=attributes.st_size,
                        etag=digest.hexdigest(),
                        modified_at=_modified_at(attributes),
                    )
                finally:
                    if handle is not None:
                        try:
                            handle.close()
                        except Exception:
                            pass
                    try:
                        self._remove_if_exists(sftp, temporary)
                    except Exception:
                        pass
        except CancelledError:
            raise
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"上传 SFTP 对象 {key!r}",
            ) from exc

    def open_read(self, key: str) -> BinaryIO:
        session = self._session()
        entered = False
        try:
            sftp = session.__enter__()
            entered = True
            root = self._root(sftp)
            remote_path = self._path(root, key)
            self._prepare_parent(
                sftp,
                root,
                remote_path,
                create=False,
            )
            attributes = sftp.lstat(remote_path)
            if stat_module.S_ISLNK(attributes.st_mode):
                raise UnsafeRemotePathError("对象不能是符号链接。")
            raw = sftp.open(remote_path, "rb")
            return _OwnedSFTPFile(raw, session)
        except Exception as exc:
            if entered:
                session.__exit__(type(exc), exc, exc.__traceback__)
            if isinstance(exc, StorageError):
                raise
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"读取 SFTP 对象 {key!r}",
            ) from exc

    def read_range(self, key: str, start: int, length: int) -> bytes:
        if start < 0 or length < 0:
            raise StorageError("范围参数不能为负数。")
        if length == 0:
            return b""
        stream = self.open_read(key)
        try:
            stream.seek(start)
            remaining = length
            chunks: list[bytes] = []
            while remaining:
                chunk = stream.read(min(_CHUNK_SIZE, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            return b"".join(chunks)
        finally:
            stream.close()

    def stat(self, key: str) -> ObjectStat | None:
        try:
            with self._session() as sftp:
                root = self._root(sftp)
                remote_path = self._path(root, key)
                try:
                    self._prepare_parent(
                        sftp,
                        root,
                        remote_path,
                        create=False,
                    )
                except Exception as exc:
                    if _is_missing(exc):
                        return None
                    raise
                try:
                    attributes = sftp.lstat(remote_path)
                except Exception as exc:
                    if _is_missing(exc):
                        return None
                    raise
                if stat_module.S_ISLNK(attributes.st_mode):
                    raise UnsafeRemotePathError("对象不能是符号链接。")
                return ObjectStat(
                    key=key,
                    size=attributes.st_size,
                    modified_at=_modified_at(attributes),
                )
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"读取 SFTP 对象元数据 {key!r}",
            ) from exc

    def iter_objects(self, prefix: str = "") -> Iterator[ObjectStat]:
        try:
            with self._session() as sftp:
                root = self._root(sftp)
                try:
                    self._verify_directory(sftp, root)
                except Exception as exc:
                    if _is_missing(exc):
                        return
                    raise
                start = self._path(root, prefix) if prefix else root
                try:
                    start_attributes = sftp.lstat(start)
                except Exception as exc:
                    if _is_missing(exc):
                        return
                    raise
                if stat_module.S_ISLNK(start_attributes.st_mode):
                    raise UnsafeRemotePathError("列举起点不能是符号链接。")
                if stat_module.S_ISREG(start_attributes.st_mode):
                    yield ObjectStat(
                        key=prefix,
                        size=start_attributes.st_size,
                        modified_at=_modified_at(start_attributes),
                    )
                    return

                def entries(directory: str) -> Iterator[Any]:
                    return iter(
                        sorted(
                            sftp.listdir_attr(directory),
                            key=lambda item: item.filename,
                        )
                    )

                stack: list[tuple[str, Iterator[Any]]] = [
                    (start, entries(start))
                ]
                while stack:
                    directory, iterator = stack[-1]
                    try:
                        attributes = next(iterator)
                    except StopIteration:
                        stack.pop()
                        continue
                    name = attributes.filename
                    remote_path = posixpath.join(directory, name)
                    if stat_module.S_ISLNK(attributes.st_mode):
                        continue
                    if stat_module.S_ISDIR(attributes.st_mode):
                        stack.append(
                            (remote_path, entries(remote_path))
                        )
                        continue
                    if not stat_module.S_ISREG(attributes.st_mode):
                        continue
                    if name.endswith(_GUARD_SUFFIX) or (
                        name.startswith(".") and name.endswith(".part")
                    ):
                        continue
                    yield ObjectStat(
                        key=posixpath.relpath(remote_path, root),
                        size=attributes.st_size,
                        modified_at=_modified_at(attributes),
                    )
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation="列举 SFTP 对象",
            ) from exc

    def delete(self, key: str) -> None:
        try:
            with self._session() as sftp:
                root = self._root(sftp)
                remote_path = self._path(root, key)
                try:
                    self._prepare_parent(
                        sftp,
                        root,
                        remote_path,
                        create=False,
                    )
                except Exception as exc:
                    if _is_missing(exc):
                        return
                    raise
                self._remove_if_exists(sftp, remote_path)
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"删除 SFTP 对象 {key!r}",
            ) from exc

    @contextmanager
    def _lease_guard(self, key: str) -> Iterator[tuple[Any, str]]:
        guard_key = f"{key}{_GUARD_SUFFIX}"
        with self._session() as sftp:
            root = self._root(sftp)
            guard_path = self._path(root, guard_key)
            self._prepare_parent(
                sftp,
                root,
                guard_path,
                create=True,
            )
            token = secrets.token_urlsafe(24)
            payload = json.dumps(
                {
                    "token": token,
                    "created_at": _now().isoformat(),
                },
                sort_keys=True,
            ).encode("utf-8")
            deadline = time.monotonic() + _GUARD_WAIT_SECONDS
            while True:
                try:
                    handle = self._open_exclusive(sftp, guard_path)
                except Exception as exc:
                    try:
                        sftp.lstat(guard_path)
                    except Exception as stat_exc:
                        if _is_missing(stat_exc):
                            raise exc
                        raise
                    if time.monotonic() < deadline:
                        time.sleep(_GUARD_RETRY_SECONDS)
                        continue
                    raise StaleLeaseGuardError(guard_key) from exc
                try:
                    handle.write(payload)
                    handle.flush()
                finally:
                    handle.close()
                break

            active_error: BaseException | None = None
            try:
                yield sftp, root
            except BaseException as exc:
                active_error = exc
                raise
            finally:
                try:
                    actual = json.loads(
                        self._read_remote_bytes(sftp, guard_path).decode("utf-8")
                    )
                    if actual.get("token") != token:
                        raise StaleLeaseGuardError(guard_key)
                    sftp.remove(guard_path)
                except BaseException:
                    if active_error is None:
                        raise

    def _read_lease(
        self,
        sftp: Any,
        root: str,
        key: str,
    ) -> tuple[dict[str, Any], str] | None:
        remote_path = self._path(root, key)
        try:
            payload = self._read_remote_bytes(sftp, remote_path)
        except Exception as exc:
            if _is_missing(exc):
                return None
            raise
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptLeaseError("远端租约不是有效 JSON。") from exc
        if not isinstance(value, dict):
            raise CorruptLeaseError("远端租约格式无效。")
        return value, hashlib.sha256(payload).hexdigest()

    def _write_lease(
        self,
        sftp: Any,
        root: str,
        key: str,
        value: dict[str, Any],
    ) -> str:
        payload = json.dumps(value, sort_keys=True).encode("utf-8")
        target = self._path(root, key)
        self._prepare_parent(
            sftp,
            root,
            target,
            create=True,
        )
        temporary = posixpath.join(
            posixpath.dirname(target),
            f".{posixpath.basename(target)}."
            f"{secrets.token_hex(12)}.part",
        )
        handle = None
        try:
            handle = self._open_exclusive(sftp, temporary)
            handle.write(payload)
            handle.flush()
            handle.close()
            handle = None
            self._posix_rename(sftp, temporary, target)
        finally:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
            try:
                self._remove_if_exists(sftp, temporary)
            except Exception:
                pass
        return hashlib.sha256(payload).hexdigest()

    def acquire_lease(
        self,
        key: str,
        owner: str,
        ttl_seconds: int,
    ) -> RemoteLease | None:
        if ttl_seconds <= 0:
            raise StorageError("远端租约 TTL 必须为正数。")
        try:
            with self._lease_guard(key) as (sftp, root):
                current = self._read_lease(sftp, root, key)
                now = _now()
                if current is not None:
                    value, _ = current
                    try:
                        expires_at = datetime.fromisoformat(
                            str(value["expires_at"])
                        )
                        if expires_at.tzinfo is None:
                            raise ValueError("missing timezone")
                    except (KeyError, TypeError, ValueError) as exc:
                        raise CorruptLeaseError(
                            "远端租约缺少有效的 expires_at。"
                        ) from exc
                    if expires_at > now:
                        return None

                token = secrets.token_urlsafe(24)
                expires_at = (
                    now + timedelta(seconds=ttl_seconds)
                ).isoformat()
                value = {
                    "owner": owner,
                    "token": token,
                    "acquired_at": now.isoformat(),
                    "heartbeat_at": now.isoformat(),
                    "expires_at": expires_at,
                }
                version = self._write_lease(sftp, root, key, value)
                return RemoteLease(
                    key,
                    owner,
                    token,
                    expires_at,
                    version,
                )
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation="获取 SFTP 远端租约",
            ) from exc

    def renew_lease(
        self,
        lease: RemoteLease,
        ttl_seconds: int,
    ) -> RemoteLease | None:
        if ttl_seconds <= 0:
            raise StorageError("远端租约 TTL 必须为正数。")
        try:
            with self._lease_guard(lease.key) as (sftp, root):
                current = self._read_lease(sftp, root, lease.key)
                if current is None:
                    return None
                value, version = current
                if (
                    value.get("token") != lease.token
                    or version != lease.version
                ):
                    return None
                now = _now()
                expires_at = (
                    now + timedelta(seconds=ttl_seconds)
                ).isoformat()
                value["heartbeat_at"] = now.isoformat()
                value["expires_at"] = expires_at
                new_version = self._write_lease(
                    sftp,
                    root,
                    lease.key,
                    value,
                )
                return RemoteLease(
                    lease.key,
                    lease.owner,
                    lease.token,
                    expires_at,
                    new_version,
                )
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation="续期 SFTP 远端租约",
            ) from exc

    def release_lease(self, lease: RemoteLease) -> None:
        try:
            with self._lease_guard(lease.key) as (sftp, root):
                current = self._read_lease(sftp, root, lease.key)
                if current is None:
                    return
                value, version = current
                if (
                    value.get("token") != lease.token
                    or version != lease.version
                ):
                    return
                value["expires_at"] = _now().isoformat()
                value["released"] = True
                self._write_lease(sftp, root, lease.key, value)
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation="释放 SFTP 远端租约",
            ) from exc

    def validate_capabilities(self) -> dict[str, Any]:
        """Verify semantics that normal read/write probes cannot establish."""

        marker = secrets.token_hex(12)
        prefix = f"v1/system/probes/{marker}"
        exclusive_key = f"{prefix}.exclusive"
        source_key = f"{prefix}.rename-source"
        target_key = f"{prefix}.rename-target"
        lease_key = f"{prefix}.lease"
        cleanup = [exclusive_key, source_key, target_key, lease_key]
        try:
            with self._session() as sftp:
                root = self._root(sftp)
                paths = {
                    key: self._path(root, key)
                    for key in (exclusive_key, source_key, target_key)
                }
                self._prepare_parent(
                    sftp,
                    root,
                    paths[exclusive_key],
                    create=True,
                )

                first = self._open_exclusive(
                    sftp,
                    paths[exclusive_key],
                )
                try:
                    first.write(b"first")
                    first.flush()
                finally:
                    first.close()
                try:
                    second = self._open_exclusive(
                        sftp,
                        paths[exclusive_key],
                    )
                except ExclusiveCreateUnsupportedError:
                    raise
                except Exception:
                    if self._read_remote_bytes(
                        sftp, paths[exclusive_key]
                    ) != b"first":
                        raise ExclusiveCreateUnsupportedError(
                            "OPEN_EXCL changed an existing file"
                        )
                else:
                    second.close()
                    raise ExclusiveCreateUnsupportedError(
                        "OPEN_EXCL allowed duplicate creation"
                    )

                for key, payload in (
                    (source_key, b"source"),
                    (target_key, b"target"),
                ):
                    handle = self._open_exclusive(sftp, paths[key])
                    try:
                        handle.write(payload)
                        handle.flush()
                    finally:
                        handle.close()
                self._posix_rename(
                    sftp,
                    paths[source_key],
                    paths[target_key],
                )
                if self._read_remote_bytes(
                    sftp, paths[target_key]
                ) != b"source":
                    raise AtomicRenameUnsupportedError(
                        "POSIX rename did not atomically replace the target"
                    )

                # The engine uploads on one session while a heartbeat renews
                # its lease on another.  Verify that the server/account permits
                # at least two simultaneous SFTP sessions.
                with self._session() as second_sftp:
                    second_root = self._root(second_sftp)
                    if second_root != root:
                        raise UnsafeRemotePathError(
                            "并发 SFTP 会话解析到不同的远端根目录。"
                        )
                    second_sftp.lstat(root)

            lease = self.acquire_lease(lease_key, "configuration-probe", 60)
            if lease is None:
                raise StaleLeaseGuardError(f"{lease_key}{_GUARD_SUFFIX}")
            renewed = self.renew_lease(lease, 60)
            if renewed is None:
                raise CorruptLeaseError("配置探针无法续期远端租约。")
            self.release_lease(renewed)
            return {
                "exclusive_create": True,
                "atomic_posix_rename": True,
                "concurrent_sessions": True,
                "renewable_lease": True,
            }
        except StorageError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation="检测 SFTP 安全能力",
            ) from exc
        finally:
            for key in cleanup:
                try:
                    self.delete(key)
                except Exception:
                    pass
