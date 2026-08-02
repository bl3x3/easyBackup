"""Alibaba Cloud OSS remote leases backed by an append-only JSONL log.

Alibaba Cloud OSS does not implement conditional ``PutObject`` through its
S3-compatible API.  Native ``AppendObject`` does provide the primitive needed
for a compare-and-swap operation: an append succeeds only when ``position`` is
equal to the current object length.  This module uses that position check to
serialize lease state transitions without overwriting or deleting history.

The lease log deliberately lives at ``<logical-key>.oss-append-v1`` so that an
old normal object created by the S3 implementation cannot make the new append
log non-appendable.
"""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator, Mapping, Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

from easybackup.errors import StorageError
from easybackup.models import S3StorageConfig
from easybackup.storage.base import RemoteLease


_LOG_SCHEMA = 1
_LOG_SUFFIX = ".oss-append-v1"
_MAX_LOG_BYTES = 64 * 1024 * 1024
_MAX_EVENT_BYTES = 64 * 1024

Clock = Callable[[], datetime]
TokenFactory = Callable[[], str]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _new_token() -> str:
    return secrets.token_urlsafe(24)


class PositionConflict(Exception):
    """The append position no longer equals the remote object length."""


@runtime_checkable
class AppendLeaseClient(Protocol):
    """Small, synchronous port used by :class:`AliyunAppendLeaseStore`.

    Keeping this protocol independent from the Alibaba SDK makes the lease
    algorithm straightforward to exercise with an in-memory fake.
    """

    def get_object(self, bucket: str, key: str) -> bytes | None:
        """Return the complete append log, or ``None`` when it does not exist."""

    def append_object(
        self,
        bucket: str,
        key: str,
        position: int,
        body: bytes,
    ) -> int:
        """Append at ``position`` and return the next append position.

        Implementations must raise :class:`PositionConflict` when another
        writer changed the object after it was read.
        """

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete a disposable probe log; production lease logs stay intact."""


@dataclass(frozen=True, slots=True)
class _LeaseState:
    state: str
    owner: str
    token: str
    acquired_at: str
    heartbeat_at: str
    expires_at: str
    expires_at_value: datetime


@dataclass(frozen=True, slots=True)
class _LeaseSnapshot:
    position: int
    state: _LeaseState | None


def _clean_scalar(value: Any, *, maximum: int = 160) -> str | None:
    """Return a bounded service identifier without stringifying rich objects."""

    if value is None or not isinstance(value, (str, int)):
        return None
    cleaned = " ".join(str(value).split())
    return cleaned[:maximum] or None


def _exception_chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _service_field(exc: BaseException, name: str) -> Any:
    for current in _exception_chain(exc):
        value = getattr(current, name, None)
        if value is not None:
            return value
    return None


def _service_code(exc: BaseException) -> str:
    value = _service_field(exc, "code")
    return _clean_scalar(value, maximum=96) or ""


def _service_status(exc: BaseException) -> int | None:
    value = _service_field(exc, "status_code")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _is_missing_object(exc: BaseException) -> bool:
    return _service_code(exc) in {"NoSuchKey", "NoSuchObject", "NotFound"} or (
        _service_status(exc) == 404
    )


def _is_position_conflict(exc: BaseException) -> bool:
    if isinstance(exc, PositionConflict):
        return True
    if _service_code(exc) == "PositionNotEqualToLength":
        return True
    return any(
        type(current).__name__ == "PositionNotEqualToLength"
        for current in _exception_chain(exc)
    )


def _storage_error(
    exc: BaseException,
    *,
    operation: str,
    bucket: str,
) -> StorageError:
    """Convert SDK errors without exposing credentials, payloads, or tokens."""

    if isinstance(exc, StorageError):
        return exc

    code = _service_code(exc)
    status = _service_status(exc)
    request_id = _clean_scalar(_service_field(exc, "request_id"), maximum=128)

    kind = "oss_service"
    title = "阿里云 OSS 租约请求失败"
    summary = "阿里云 OSS 未能完成远端租约请求。"
    suggestions = [
        "检查 OSS Endpoint、Region、Bucket 和网络连接后重试。",
        "结合错误码与 Request ID 在阿里云 OSS 日志中定位请求。",
    ]

    if code in {
        "InvalidAccessKeyId",
        "InvalidSecurityToken",
        "SecurityTokenExpired",
        "ExpiredToken",
    }:
        kind = "credentials"
        title = "OSS 访问凭据无效或已过期"
        summary = "阿里云 OSS 无法验证当前 AccessKey 或临时令牌。"
        suggestions = ["重新保存 AccessKey、Secret 与可选的 STS Token 后重试。"]
    elif code in {"AccessDenied", "Forbidden", "Unauthorized"} or status in {
        401,
        403,
    }:
        kind = "permission"
        title = "OSS 租约权限不足"
        summary = "当前凭据没有读写远端租约日志所需的权限。"
        suggestions = [
            "为目标 Bucket/前缀授予 oss:GetObject 与 oss:PutObject 权限。"
        ]
    elif code in {"NoSuchBucket", "InvalidBucketName"}:
        kind = "bucket"
        title = "OSS Bucket 不存在或名称无效"
        summary = "阿里云 OSS 未找到租约配置指定的 Bucket。"
        suggestions = ["核对 Bucket 名称，以及 Endpoint 和 Region 是否匹配。"]
    elif code == "ObjectNotAppendable":
        kind = "lease_object_not_appendable"
        title = "OSS 租约日志不是 Appendable 对象"
        summary = "远端租约日志已被创建为普通对象，无法安全追加状态。"
        suggestions = [
            "请勿覆盖或手工上传 *.oss-append-v1 租约日志；确认后移走冲突对象。"
        ]
    elif code == "FileImmutable":
        kind = "immutable"
        title = "OSS 租约日志受保留策略保护"
        summary = "Bucket 的保留策略禁止追加远端租约状态。"
        suggestions = ["为租约日志使用不受 WORM 保留策略限制的 Bucket 或前缀。"]
    elif status == 429 or code in {"SlowDown", "Throttling", "TooManyRequests"}:
        kind = "throttled"
        title = "OSS 请求频率受限"
        summary = "阿里云 OSS 暂时限制了远端租约请求。"
        suggestions = ["稍后重试，并检查 Bucket 的请求配额。"]

    diagnostic: dict[str, Any] = {
        "kind": kind,
        "title": title,
        "summary": summary,
        "suggestions": suggestions,
        "operation": operation,
        "provider": "aliyun_oss",
        "bucket": bucket,
    }
    optional = {
        "provider_code": code or None,
        "http_status": status,
        "request_id": request_id,
    }
    diagnostic.update(
        {name: value for name, value in optional.items() if value is not None}
    )
    return StorageError(
        f"{operation}失败：{summary}",
        details={"diagnostic": diagnostic},
    )


def _configuration_error(
    *, kind: str, title: str, summary: str, suggestion: str
) -> StorageError:
    return StorageError(
        summary,
        details={
            "diagnostic": {
                "kind": kind,
                "title": title,
                "summary": summary,
                "suggestions": [suggestion],
                "operation": "初始化阿里云 OSS 租约客户端",
                "provider": "aliyun_oss",
            }
        },
    )


def _corrupt_log_error(
    *, bucket: str, logical_key: str, reason: str
) -> StorageError:
    summary = "远端租约日志已损坏或使用了不支持的格式。"
    return StorageError(
        f"读取远端租约失败：{summary}",
        details={
            "diagnostic": {
                "kind": "lease_log_corrupt",
                "title": "OSS 远端租约日志无效",
                "summary": summary,
                "suggestions": [
                    "停止并发备份并检查对应的 *.oss-append-v1 对象；"
                    "不要在未确认当前租约状态前删除日志。"
                ],
                "operation": "读取远端租约",
                "provider": "aliyun_oss",
                "bucket": bucket,
                "lease_key": logical_key,
                "reason": reason,
            }
        },
    )


def _parse_timestamp(value: Any) -> tuple[str, datetime] | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return value, parsed.astimezone(timezone.utc)


def _decode_event(
    raw_line: bytes,
    *,
    bucket: str,
    logical_key: str,
    line_number: int,
) -> _LeaseState:
    if not raw_line:
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行为空",
        )
    if len(raw_line) > _MAX_EVENT_BYTES:
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行超过大小上限",
        )
    try:
        decoded = raw_line.decode("utf-8")
        value = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError):
        # Do not chain JSON errors: their messages can contain a token excerpt.
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行不是完整的 UTF-8 JSON",
        ) from None

    if not isinstance(value, dict):
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行不是 JSON 对象",
        )
    schema = value.get("schema")
    if not isinstance(schema, int) or isinstance(schema, bool) or schema != _LOG_SCHEMA:
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行使用了不支持的 schema",
        )
    state = value.get("state")
    if state not in {"active", "released"}:
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行包含无效状态",
        )
    owner = value.get("owner")
    token = value.get("token")
    if (
        not isinstance(owner, str)
        or not owner
        or not isinstance(token, str)
        or not token
    ):
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行缺少租约身份字段",
        )
    expires = _parse_timestamp(value.get("expires_at"))
    if expires is None:
        raise _corrupt_log_error(
            bucket=bucket,
            logical_key=logical_key,
            reason=f"第 {line_number} 行包含无效到期时间",
        )
    expires_at, expires_at_value = expires

    acquired_at = value.get("acquired_at")
    heartbeat_at = value.get("heartbeat_at")
    if not isinstance(acquired_at, str):
        acquired_at = expires_at
    if not isinstance(heartbeat_at, str):
        heartbeat_at = acquired_at
    return _LeaseState(
        state=state,
        owner=owner,
        token=token,
        acquired_at=acquired_at,
        heartbeat_at=heartbeat_at,
        expires_at=expires_at,
        expires_at_value=expires_at_value,
    )


def _encode_event(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


class AliyunAppendLeaseStore:
    """Remote-lease implementation using OSS ``AppendObject`` position CAS."""

    protocol_name = "oss_append_position"

    def __init__(
        self,
        client: AppendLeaseClient,
        *,
        bucket: str,
        key_prefix: str = "",
        clock: Clock = _utc_now,
        token_factory: TokenFactory = _new_token,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.key_prefix = key_prefix.replace("\\", "/").strip("/")
        self.clock = clock
        self.token_factory = token_factory

    def _now(self) -> datetime:
        value = self.clock()
        if not isinstance(value, datetime):
            raise StorageError("远端租约时钟返回了无效值。")
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _logical_key(self, key: str) -> str:
        value = key.replace("\\", "/").strip("/")
        if (
            not value
            or "\x00" in value
            or any(part in {"", ".", ".."} for part in value.split("/"))
        ):
            raise StorageError(f"无效对象键：{key!r}")
        return value

    def _log_key(self, logical_key: str) -> str:
        backend_key = (
            f"{self.key_prefix}/{logical_key}"
            if self.key_prefix
            else logical_key
        )
        return f"{backend_key}{_LOG_SUFFIX}"

    def lease_log_key(self, key: str) -> str:
        """Return the physical OSS key used for a logical lease key."""

        return self._log_key(self._logical_key(key))

    @staticmethod
    def _validate_ttl(ttl_seconds: int) -> None:
        if (
            not isinstance(ttl_seconds, int)
            or isinstance(ttl_seconds, bool)
            or ttl_seconds <= 0
        ):
            raise StorageError("远端租约 TTL 必须为正整数。")

    def _snapshot(self, logical_key: str) -> _LeaseSnapshot:
        log_key = self._log_key(logical_key)
        try:
            payload = self.client.get_object(self.bucket, log_key)
        except Exception as exc:
            raise _storage_error(
                exc,
                operation="读取远端租约",
                bucket=self.bucket,
            ) from exc
        if payload is None:
            return _LeaseSnapshot(position=0, state=None)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise _corrupt_log_error(
                bucket=self.bucket,
                logical_key=logical_key,
                reason="客户端返回的租约日志不是字节数据",
            )
        data = bytes(payload)
        if len(data) > _MAX_LOG_BYTES:
            raise _corrupt_log_error(
                bucket=self.bucket,
                logical_key=logical_key,
                reason="租约日志超过安全读取上限",
            )
        if not data:
            return _LeaseSnapshot(position=0, state=None)
        if not data.endswith(b"\n"):
            raise _corrupt_log_error(
                bucket=self.bucket,
                logical_key=logical_key,
                reason="租约日志末行不完整",
            )

        current: _LeaseState | None = None
        for line_number, raw_line in enumerate(data[:-1].split(b"\n"), start=1):
            current = _decode_event(
                raw_line,
                bucket=self.bucket,
                logical_key=logical_key,
                line_number=line_number,
            )
        return _LeaseSnapshot(position=len(data), state=current)

    def _append(
        self,
        logical_key: str,
        position: int,
        event: Mapping[str, Any],
    ) -> int | None:
        payload = _encode_event(event)
        try:
            next_position = self.client.append_object(
                self.bucket,
                self._log_key(logical_key),
                position,
                payload,
            )
        except Exception as exc:
            if _is_position_conflict(exc):
                return None
            raise _storage_error(
                exc,
                operation="更新远端租约",
                bucket=self.bucket,
            ) from exc

        if (
            not isinstance(next_position, int)
            or isinstance(next_position, bool)
            or next_position != position + len(payload)
        ):
            raise StorageError(
                "更新远端租约失败：OSS 返回了无效的下一追加位置。",
                details={
                    "diagnostic": {
                        "kind": "lease_protocol_error",
                        "title": "OSS 租约响应无效",
                        "summary": "AppendObject 返回的下一位置与已追加数据长度不一致。",
                        "suggestions": ["检查 OSS SDK 版本和 Endpoint 配置后重试。"],
                        "operation": "更新远端租约",
                        "provider": "aliyun_oss",
                        "bucket": self.bucket,
                    }
                },
            )
        return next_position

    def acquire_lease(
        self, key: str, owner: str, ttl_seconds: int
    ) -> RemoteLease | None:
        self._validate_ttl(ttl_seconds)
        logical_key = self._logical_key(key)
        if not isinstance(owner, str) or not owner:
            raise StorageError("远端租约所有者不能为空。")

        snapshot = self._snapshot(logical_key)
        now = self._now()
        if (
            snapshot.state is not None
            and snapshot.state.state == "active"
            and snapshot.state.expires_at_value > now
        ):
            return None

        token = self.token_factory()
        if not isinstance(token, str) or not token:
            raise StorageError("远端租约令牌生成失败。")
        now_text = now.isoformat()
        expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        event = {
            "schema": _LOG_SCHEMA,
            "action": "acquire",
            "state": "active",
            "owner": owner,
            "token": token,
            "acquired_at": now_text,
            "heartbeat_at": now_text,
            "expires_at": expires_at,
        }
        version = self._append(logical_key, snapshot.position, event)
        if version is None:
            return None
        return RemoteLease(key, owner, token, expires_at, str(version))

    def renew_lease(
        self, lease: RemoteLease, ttl_seconds: int
    ) -> RemoteLease | None:
        self._validate_ttl(ttl_seconds)
        logical_key = self._logical_key(lease.key)
        snapshot = self._snapshot(logical_key)
        current = snapshot.state
        now = self._now()
        if (
            current is None
            or current.state != "active"
            or current.token != lease.token
            or current.expires_at_value <= now
        ):
            return None

        now_text = now.isoformat()
        expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        event = {
            "schema": _LOG_SCHEMA,
            "action": "renew",
            "state": "active",
            "owner": current.owner,
            "token": current.token,
            "acquired_at": current.acquired_at,
            "heartbeat_at": now_text,
            "expires_at": expires_at,
        }
        version = self._append(logical_key, snapshot.position, event)
        if version is None:
            return None
        return RemoteLease(
            lease.key,
            current.owner,
            current.token,
            expires_at,
            str(version),
        )

    def release_lease(self, lease: RemoteLease) -> None:
        logical_key = self._logical_key(lease.key)
        snapshot = self._snapshot(logical_key)
        current = snapshot.state
        if (
            current is None
            or current.state != "active"
            or current.token != lease.token
        ):
            return

        now_text = self._now().isoformat()
        event = {
            "schema": _LOG_SCHEMA,
            "action": "release",
            "state": "released",
            "owner": current.owner,
            "token": current.token,
            "acquired_at": current.acquired_at,
            "heartbeat_at": current.heartbeat_at,
            "expires_at": now_text,
            "released_at": now_text,
        }
        # A conflict means another state transition won the CAS.  It is not
        # safe to retry the stale release against the new log position.
        self._append(logical_key, snapshot.position, event)

    def cleanup_lease_log(self, key: str) -> None:
        """Delete a disposable lease log created by a configuration probe.

        Normal release intentionally never calls this method: production lease
        history remains append-only.  The caller must therefore use it only for
        a unique, probe-owned key from a ``finally`` block.
        """

        logical_key = self._logical_key(key)
        delete_object = getattr(self.client, "delete_object", None)
        if not callable(delete_object):
            raise StorageError(
                "清理 OSS 租约探针失败：租约客户端不支持删除对象。",
                details={
                    "diagnostic": {
                        "kind": "lease_protocol_error",
                        "title": "OSS 租约客户端能力不完整",
                        "summary": "租约客户端没有提供探针日志清理能力。",
                        "suggestions": ["使用官方阿里云 OSS V2 客户端后重试。"],
                        "operation": "清理 OSS 租约探针",
                        "provider": "aliyun_oss",
                        "bucket": self.bucket,
                    }
                },
            )
        try:
            delete_object(self.bucket, self._log_key(logical_key))
        except Exception as exc:
            raise _storage_error(
                exc,
                operation="清理 OSS 租约探针",
                bucket=self.bucket,
            ) from exc


def _request_class(oss_module: Any, name: str) -> Any:
    request_type = getattr(oss_module, name, None)
    if request_type is None:
        request_type = getattr(getattr(oss_module, "models", None), name, None)
    if request_type is None:
        raise AttributeError(f"OSS SDK 缺少 {name}")
    return request_type


def _read_response_body(body: Any) -> bytes:
    if body is None:
        return b""
    if isinstance(body, (bytes, bytearray, memoryview)):
        return bytes(body)
    if hasattr(body, "__enter__") and hasattr(body, "__exit__"):
        with body as stream:
            value = stream.read()
    else:
        try:
            value = body.read()
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
    if isinstance(value, str):
        return value.encode("utf-8")
    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise TypeError("OSS GetObject body is not bytes")
    return bytes(value)


class AliyunOSSLeaseClient:
    """Adapter from the official ``alibabacloud_oss_v2`` client to the port."""

    def __init__(self, client: Any, oss_module: Any) -> None:
        self.raw_client = client
        self.oss_module = oss_module

    def get_object(self, bucket: str, key: str) -> bytes | None:
        request_type = _request_class(self.oss_module, "GetObjectRequest")
        try:
            result = self.raw_client.get_object(
                request_type(bucket=bucket, key=key)
            )
        except Exception as exc:
            if _is_missing_object(exc):
                return None
            raise
        if isinstance(result, (bytes, bytearray, memoryview)):
            return bytes(result)
        return _read_response_body(getattr(result, "body", None))

    def append_object(
        self,
        bucket: str,
        key: str,
        position: int,
        body: bytes,
    ) -> int:
        request_type = _request_class(self.oss_module, "AppendObjectRequest")
        request_kwargs: dict[str, Any] = {
            "bucket": bucket,
            "key": key,
            "position": position,
            "body": body,
            "content_type": "application/x-ndjson",
        }
        # Lease state must remain immediately readable even when the bucket's
        # default class is archival.  OSS accepts storage class only when an
        # Appendable object is first created at position zero.
        if position == 0:
            request_kwargs["storage_class"] = "Standard"
        try:
            result = self.raw_client.append_object(
                request_type(**request_kwargs)
            )
        except Exception as exc:
            if _is_position_conflict(exc):
                raise PositionConflict() from None
            raise
        if isinstance(result, int) and not isinstance(result, bool):
            return result
        value = (
            result.get("next_position")
            if isinstance(result, Mapping)
            else getattr(result, "next_position", None)
        )
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise TypeError("OSS AppendObject response has no next_position") from exc

    def delete_object(self, bucket: str, key: str) -> None:
        request_type = _request_class(self.oss_module, "DeleteObjectRequest")
        try:
            self.raw_client.delete_object(request_type(bucket=bucket, key=key))
        except Exception as exc:
            if _is_missing_object(exc):
                return
            raise


def _native_oss_endpoint(endpoint_url: str | None) -> str | None:
    """Translate an OSS S3-compatible endpoint for use by the native SDK."""

    if not endpoint_url:
        return None
    parsed = urlsplit(endpoint_url)
    hostname = parsed.hostname or ""
    labels = hostname.lower().split(".")
    if len(labels) < 3 or labels[-2:] != ["aliyuncs", "com"]:
        return endpoint_url
    try:
        oss_label = next(
            index for index, label in enumerate(labels) if label.startswith("oss-")
        )
    except StopIteration:
        return endpoint_url
    # Both the S3 compatibility marker and an accidentally supplied bucket
    # label must be removed.  The native SDK adds the bucket host itself.
    native_host = ".".join(labels[oss_label:])
    if parsed.port is not None:
        native_host = f"{native_host}:{parsed.port}"
    return urlunsplit(
        (parsed.scheme, native_host, parsed.path, parsed.query, parsed.fragment)
    )


def _load_oss_module() -> Any:
    try:
        import alibabacloud_oss_v2 as oss
    except ImportError as exc:
        raise _configuration_error(
            kind="dependency",
            title="缺少阿里云 OSS SDK",
            summary="阿里云 OSS 安全租约需要 alibabacloud-oss-v2。",
            suggestion="安装项目依赖后重新启动 EasyBackup。",
        ) from exc
    return oss


def create_aliyun_oss_client(
    config: S3StorageConfig,
    credentials: Mapping[str, str | None],
    *,
    oss_module: Any | None = None,
) -> Any:
    """Construct an official Alibaba Cloud OSS V2 client.

    ``oss_module`` is injectable so tests can supply request/configuration
    doubles without importing or contacting the real SDK.
    """

    oss = oss_module or _load_oss_module()
    region = (config.region or "").strip()
    if not region:
        raise _configuration_error(
            kind="invalid_config",
            title="阿里云 OSS Region 缺失",
            summary="初始化阿里云 OSS 租约客户端需要填写 Region。",
            suggestion="填写 Bucket 所在 Region，例如 cn-shanghai。",
        )
    access_key = (credentials.get("access_key_id") or "").strip()
    secret_key = (credentials.get("secret_access_key") or "").strip()
    session_token = (credentials.get("session_token") or "").strip()
    if not access_key or not secret_key:
        raise _configuration_error(
            kind="credentials",
            title="阿里云 OSS 访问凭据不完整",
            summary="初始化阿里云 OSS 租约客户端需要 AccessKey 和 Secret。",
            suggestion="重新保存该 S3/OSS 凭据配置后重试。",
        )

    try:
        provider_kwargs: dict[str, str] = {
            "access_key_id": access_key,
            "access_key_secret": secret_key,
        }
        if session_token:
            provider_kwargs["security_token"] = session_token
        provider = oss.credentials.StaticCredentialsProvider(**provider_kwargs)
        sdk_config = oss.config.load_default()
        sdk_config.credentials_provider = provider
        sdk_config.region = region
        endpoint = _native_oss_endpoint(config.endpoint_url)
        if endpoint:
            sdk_config.endpoint = endpoint
        return oss.Client(sdk_config)
    except Exception as exc:
        raise _storage_error(
            exc,
            operation="初始化阿里云 OSS 租约客户端",
            bucket=config.bucket,
        ) from exc


def create_aliyun_oss_lease_store(
    config: S3StorageConfig,
    credentials: Mapping[str, str | None],
    *,
    client: Any | None = None,
    oss_module: Any | None = None,
    clock: Clock = _utc_now,
    token_factory: TokenFactory = _new_token,
) -> AliyunAppendLeaseStore:
    """Create the OSS append-position lease implementation.

    Passing ``client`` without ``oss_module`` treats it as an
    :class:`AppendLeaseClient` test double.  Passing both adapts ``client`` as
    an official-SDK-compatible client using request classes from ``oss_module``.
    """

    if client is not None and oss_module is None:
        port = client
    else:
        oss = oss_module or _load_oss_module()
        raw_client = client or create_aliyun_oss_client(
            config,
            credentials,
            oss_module=oss,
        )
        port = AliyunOSSLeaseClient(raw_client, oss)
    return AliyunAppendLeaseStore(
        port,
        bucket=config.bucket,
        key_prefix=config.prefix,
        clock=clock,
        token_factory=token_factory,
    )


__all__ = [
    "AliyunAppendLeaseStore",
    "AliyunOSSLeaseClient",
    "AppendLeaseClient",
    "PositionConflict",
    "create_aliyun_oss_client",
    "create_aliyun_oss_lease_store",
]
