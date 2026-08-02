"""S3-compatible streaming object storage backend."""

from __future__ import annotations

import io
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, BinaryIO, Iterator

from easybackup.errors import CancelledError, StorageError
from easybackup.models import S3StorageConfig, is_aliyun_oss_endpoint
from easybackup.storage.base import (
    BlobStore,
    CancelCallback,
    ObjectStat,
    ProgressCallback,
    RemoteLease,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _exception_chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _error_response(exc: BaseException) -> dict[str, Any]:
    for current in _exception_chain(exc):
        response = getattr(current, "response", None)
        if isinstance(response, dict):
            return response
    return {}


def diagnose_s3_error(
    exc: BaseException,
    *,
    config: S3StorageConfig,
    operation: str,
) -> dict[str, Any]:
    """Classify a boto3/OSS error into safe, actionable UI diagnostics."""

    response = _error_response(exc)
    provider_error = response.get("Error", {})
    metadata = response.get("ResponseMetadata", {})
    provider_code = str(provider_error.get("Code", "") or "")
    provider_message = " ".join(
        str(provider_error.get("Message", "") or "").split()
    )[:500]
    http_status = metadata.get("HTTPStatusCode")
    request_id = (
        metadata.get("RequestId")
        or metadata.get("RequestID")
        or provider_error.get("RequestId")
    )
    class_names = {type(item).__name__ for item in _exception_chain(exc)}
    raw_message = " ".join(str(item) for item in _exception_chain(exc))
    searchable = f"{provider_code} {provider_message} {raw_message}".lower()

    kind = "unknown"
    title = "对象存储请求失败"
    summary = provider_message or "服务端返回了未识别的错误。"
    suggestions = [
        "核对 Endpoint、Region、Bucket 与凭据配置后重试。",
        "若问题持续，请结合服务端错误码和 Request ID 查询对象存储日志。",
    ]

    if (
        "InvalidEndpoint" in class_names
        or "invalid endpoint" in searchable
        or isinstance(exc, ValueError)
    ):
        kind = "invalid_endpoint"
        title = "Endpoint URL 格式无效"
        summary = "Endpoint 必须是包含协议和主机名的完整 URL。"
        suggestions = [
            "使用 https:// 开头；不要只填写主机名。",
            "阿里云上海 OSS 推荐填写 https://s3.oss-cn-shanghai.aliyuncs.com。",
        ]
    elif provider_code == "PublicEndpointForbidden":
        kind = "public_endpoint_forbidden"
        title = "OSS 公网 Endpoint 被策略禁用"
        summary = "当前阿里云账号或 Bucket 不允许通过默认公网 Endpoint 访问。"
        suggestions = [
            "为 Bucket 绑定自定义域名（CNAME），并使用支持 CNAME 的 OSS 访问方式。",
            "若 EasyBackup 与 Bucket 位于同一阿里云地域，可改用上海地域内网 Endpoint。",
        ]
    elif (
        provider_code in {"NoSuchBucket", "InvalidBucketName"}
        or "specified bucket does not exist" in searchable
    ):
        kind = "bucket"
        title = "Bucket 不存在或名称无效"
        summary = "服务端未找到该 Bucket，或 Bucket 名称不符合要求。"
        suggestions = [
            "确认 Bucket 名称拼写完全一致，且凭据所属账号有权看到它。",
            "确认 Endpoint 与 Bucket 所在地域匹配。",
        ]
    elif provider_code in {
        "PermanentRedirect",
        "AuthorizationHeaderMalformed",
        "IncorrectEndpoint",
        "IllegalLocationConstraintException",
    } or http_status in {301, 307}:
        kind = "region_endpoint"
        title = "Region 或 Endpoint 不匹配"
        summary = "请求被发送到了与 Bucket 所在地域不一致的 Endpoint。"
        suggestions = [
            "将 Region 设置为 Bucket 的实际地域，例如 cn-shanghai。",
            "阿里云 OSS 使用对应地域的 S3 兼容 Endpoint，例如 https://s3.oss-cn-shanghai.aliyuncs.com。",
        ]
    elif provider_code in {
        "InvalidAccessKeyId",
        "InvalidSecurityToken",
        "SecurityTokenExpired",
        "ExpiredToken",
        "UnrecognizedClientException",
        "TokenRefreshRequired",
    }:
        kind = "credentials"
        title = "访问密钥无效或已过期"
        summary = "服务端无法识别当前 AccessKey，或临时令牌已经失效。"
        suggestions = [
            "确认凭据配置保存的是当前阿里云账号或 RAM 用户的 AccessKey。",
            "若使用临时 STS 凭据，请同时更新 Session Token 并检查有效期。",
        ]
    elif {
        "NoCredentialsError",
        "PartialCredentialsError",
        "CredentialRetrievalError",
    } & class_names:
        kind = "credentials"
        title = "访问密钥缺失或不完整"
        summary = "S3 客户端没有获得完整的 AccessKey 与 Secret AccessKey。"
        suggestions = [
            "在“存储与密钥”中重新保存该凭据配置。",
            "确认 AccessKey ID、Secret AccessKey 与可选 Session Token 均来自同一组凭据。",
        ]
    elif (
        provider_code == "RequestTimeTooSkewed"
        or provider_code == "RequestExpired"
        or "clock skew" in searchable
        or "request time" in searchable
    ):
        kind = "clock"
        title = "系统时间偏差过大"
        summary = "本机时间与对象存储服务端时间不一致，签名已被判定过期。"
        suggestions = [
            "启用 Windows 自动设置时间与时区，然后立即同步时间。",
            "确保本机与标准时间的偏差小于 15 分钟后重试。",
        ]
    elif provider_code in {
        "SignatureDoesNotMatch",
        "InvalidSignatureException",
        "InvalidArgument",
    } and (
        "signature" in searchable
        or "authorization" in searchable
        or "content-sha256" in searchable
        or provider_code != "InvalidArgument"
    ):
        kind = "signature"
        title = "请求签名不兼容"
        summary = provider_message or "服务端拒绝了当前请求签名。"
        suggestions = [
            "核对 Secret AccessKey，避免复制时带入空格或换行。",
            "阿里云 OSS 需要 S3 兼容签名与 virtual-hosted 寻址；EasyBackup 会对 aliyuncs.com Endpoint 自动启用。",
        ]
    elif (
        provider_code in {"AccessDenied", "Forbidden", "Unauthorized"}
        or http_status in {401, 403}
    ) and provider_code not in {
        "SecondLevelDomainForbidden",
        "InvalidHostHeader",
    }:
        kind = "permission"
        title = "凭据权限不足"
        summary = provider_message or "服务端拒绝了当前凭据的对象操作权限。"
        suggestions = [
            "授予该 RAM 用户目标 Bucket/前缀的写入、读取、列举与删除权限。",
            "同时检查 Bucket Policy、RAM Policy、防盗链和来源网络限制。",
        ]
    elif provider_code in {
        "NotImplemented",
        "MethodNotAllowed",
        "UnsupportedOperation",
    } or http_status == 501:
        kind = "unsupported_operation"
        title = "服务端不支持所需操作"
        summary = provider_message or "该 S3 兼容服务未实现当前请求所需的语义。"
        suggestions = [
            "确认服务端支持标准 S3 API 与当前请求头。",
            "若错误发生在远端租约，服务端必须支持条件 PutObject；请勿通过关闭锁绕过此安全检查。",
        ]
    elif provider_code in {
        "SecondLevelDomainForbidden",
        "InvalidHostHeader",
    } or "path-style" in searchable or "virtual-host" in searchable:
        kind = "addressing_style"
        title = "对象存储寻址方式不兼容"
        summary = "服务端要求使用 Bucket 子域名，而不是路径式访问。"
        suggestions = [
            "使用服务级 Endpoint，不要把 Bucket 名写进 Endpoint。",
            "阿里云 OSS 仅支持 virtual-hosted style；EasyBackup 会对官方 OSS Endpoint 自动启用。",
        ]
    elif (
        "SSLError" in class_names
        or "SSLValidationError" in class_names
        or "certificate_verify_failed" in searchable
        or "tls" in searchable
    ):
        kind = "tls"
        title = "TLS 证书校验失败"
        summary = "无法验证 Endpoint 提供的 HTTPS 证书。"
        suggestions = [
            "确认 Endpoint 主机名与证书一致，且系统时间正确。",
            "私有 MinIO 请安装受信任证书；不要为了绕过错误而关闭证书校验。",
        ]
    elif (
        "ConnectTimeoutError" in class_names
        or "ReadTimeoutError" in class_names
        or "TimeoutError" in class_names
        or "timed out" in searchable
    ):
        kind = "timeout"
        title = "连接对象存储超时"
        summary = "在超时时间内未能连接 Endpoint 或读取响应。"
        suggestions = [
            "检查网络、防火墙、代理和 Endpoint 可达性后重试。",
            "若使用内网 Endpoint，确认 EasyBackup 运行在同一云网络或已建立专线/VPN。",
        ]
    elif (
        "EndpointConnectionError" in class_names
        or "ConnectionClosedError" in class_names
        or "ConnectionError" in class_names
        or "NameResolutionError" in class_names
        or "ProxyConnectionError" in class_names
        or "could not connect to the endpoint" in searchable
        or "failed to connect to proxy" in searchable
        or "name resolution" in searchable
        or "getaddrinfo" in searchable
    ):
        kind = "endpoint_unreachable"
        title = "Endpoint 无法连接"
        summary = "无法解析或连接对象存储 Endpoint。"
        suggestions = [
            "检查 Endpoint 拼写、DNS、代理、防火墙和当前网络连接。",
            "确认公网与内网 Endpoint 的选择符合 EasyBackup 所在网络。",
        ]
    elif provider_code in {
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "TooManyRequests",
    } or http_status == 429:
        kind = "throttled"
        title = "请求频率受限"
        summary = "对象存储暂时限制了当前请求频率。"
        suggestions = [
            "稍后重试，并检查账号或 Bucket 的请求配额。",
            "若持续发生，请降低并发或联系服务商提升配额。",
        ]

    diagnostic: dict[str, Any] = {
        "kind": kind,
        "title": title,
        "summary": summary,
        "suggestions": suggestions,
        "operation": operation,
        "provider": (
            "aliyun_oss"
            if is_aliyun_oss_endpoint(config.endpoint_url)
            else "s3_compatible"
        ),
        "endpoint": config.endpoint_url or "AWS 默认 Endpoint",
        "bucket": config.bucket,
        "region": config.region,
    }
    optional = {
        "provider_code": provider_code or None,
        "provider_message": provider_message or None,
        "http_status": http_status,
        "request_id": request_id,
    }
    diagnostic.update(
        {key: value for key, value in optional.items() if value is not None}
    )
    return diagnostic


def _storage_error(
    exc: BaseException,
    *,
    config: S3StorageConfig,
    operation: str,
) -> StorageError:
    diagnostic = diagnose_s3_error(
        exc,
        config=config,
        operation=operation,
    )
    return StorageError(
        f"{operation}失败：{diagnostic['summary']}",
        details={"diagnostic": diagnostic},
    )


class _CancellableReader:
    def __init__(self, raw: BinaryIO, cancelled: CancelCallback | None):
        self.raw = raw
        self.cancelled = cancelled

    def read(self, size: int = -1) -> bytes:
        self._check_cancelled()
        if size is None or size < 0:
            return self.raw.read(size)
        chunks: list[bytes] = []
        remaining = size
        while remaining:
            self._check_cancelled()
            chunk = self.raw.read(remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _check_cancelled(self) -> None:
        if self.cancelled and self.cancelled():
            raise CancelledError("操作已取消。")

    def seekable(self) -> bool:
        return False


class S3BlobStore(BlobStore):
    def __init__(
        self,
        config: S3StorageConfig,
        credentials: dict[str, str | None],
    ):
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:
            raise StorageError(
                "S3 后端需要安装 boto3。",
                details={
                    "diagnostic": {
                        "kind": "dependency",
                        "title": "缺少 S3 客户端依赖",
                        "summary": "当前 Python 环境未安装 boto3。",
                        "suggestions": ["安装项目依赖后重新启动 EasyBackup。"],
                        "operation": "初始化 S3 客户端",
                    }
                },
            ) from exc

        self.config = config
        self.bucket = config.bucket
        self.prefix = config.prefix.strip("/")
        self.storage_class = config.storage_class
        self.multipart_chunk = config.multipart_chunk_mb * 1024 * 1024
        self.upload_limit_bytes_per_second = (
            int(config.upload_limit_mbps * 1_000_000 / 8)
            if config.upload_limit_mbps > 0
            else None
        )
        self.provider = (
            "aliyun_oss"
            if is_aliyun_oss_endpoint(config.endpoint_url)
            else "s3_compatible"
        )
        self.addressing_style = (
            "virtual" if self.provider == "aliyun_oss" else "auto"
        )
        self.signature_version = (
            "s3" if self.provider == "aliyun_oss" else "default"
        )

        access_key = (credentials.get("access_key_id") or "").strip()
        secret_key = (credentials.get("secret_access_key") or "").strip()
        session_token = (credentials.get("session_token") or "").strip()
        kwargs: dict[str, Any] = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
        }
        if session_token:
            kwargs["aws_session_token"] = session_token
        if config.region:
            kwargs["region_name"] = config.region
        if config.endpoint_url:
            kwargs["endpoint_url"] = config.endpoint_url
        if self.provider == "aliyun_oss":
            kwargs["config"] = Config(
                signature_version="s3",
                s3={"addressing_style": "virtual"},
            )
        try:
            self.client = boto3.client("s3", **kwargs)
        except Exception as exc:
            raise _storage_error(
                exc,
                config=config,
                operation="初始化 S3 客户端",
            ) from exc
        self._aliyun_lease = None
        if self.provider == "aliyun_oss":
            from easybackup.storage.aliyun_lease import (
                create_aliyun_oss_lease_store,
            )

            self._aliyun_lease = create_aliyun_oss_lease_store(
                config,
                credentials,
            )
        else:
            put_object = self.client.meta.service_model.operation_model(
                "PutObject"
            )
            members = set(put_object.input_shape.members)
            missing_conditions = {"IfMatch", "IfNoneMatch"} - members
            if missing_conditions:
                raise StorageError(
                    "当前 boto3/botocore 过旧，S3 PutObject 不支持安全租约"
                    "所需的 If-Match/If-None-Match；请安装 boto3>=1.36。",
                    details={
                        "diagnostic": {
                            "kind": "client_too_old",
                            "title": "S3 客户端版本过旧",
                            "summary": "当前 boto3/botocore 缺少安全远端租约所需参数。",
                            "suggestions": [
                                "安装 boto3>=1.36 后重新启动 EasyBackup。"
                            ],
                            "operation": "初始化 S3 客户端",
                        }
                    },
                )

    def _key(self, key: str) -> str:
        value = key.replace("\\", "/").strip("/")
        if not value or any(part in {"", ".", ".."} for part in value.split("/")):
            raise StorageError(f"无效对象键：{key!r}")
        return f"{self.prefix}/{value}" if self.prefix else value

    def _display_key(self, backend_key: str) -> str:
        if self.prefix and backend_key.startswith(self.prefix + "/"):
            return backend_key[len(self.prefix) + 1 :]
        return backend_key

    def put_stream(
        self,
        key: str,
        stream: BinaryIO,
        *,
        progress: ProgressCallback | None = None,
        cancelled: CancelCallback | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ObjectStat:
        try:
            from boto3.s3.transfer import TransferConfig

            extra: dict[str, object] = {}
            if metadata:
                extra["Metadata"] = metadata
            if self.storage_class:
                extra["StorageClass"] = self.storage_class
            total = 0

            def callback(delta: int) -> None:
                nonlocal total
                total += delta
                if cancelled and cancelled():
                    raise CancelledError("操作已取消。")
                if progress:
                    progress(total)

            transfer_config: dict[str, object] = {
                "multipart_threshold": self.multipart_chunk,
                "multipart_chunksize": self.multipart_chunk,
                "max_concurrency": (
                    1
                    if self.upload_limit_bytes_per_second is not None
                    else 4
                ),
                "use_threads": True,
            }
            if self.upload_limit_bytes_per_second is not None:
                transfer_config.update(
                    {
                        "max_bandwidth": self.upload_limit_bytes_per_second,
                        # CRT currently ignores max_bandwidth. Force the
                        # classic transfer manager whenever throttling is on.
                        "preferred_transfer_client": "classic",
                    }
                )

            self.client.upload_fileobj(
                _CancellableReader(stream, cancelled),
                self.bucket,
                self._key(key),
                ExtraArgs=extra or None,
                Callback=callback,
                Config=TransferConfig(**transfer_config),
            )
            result = self.client.head_object(
                Bucket=self.bucket, Key=self._key(key)
            )
            return ObjectStat(
                key=key,
                size=result["ContentLength"],
                etag=str(result.get("ETag", "")).strip('"') or None,
                modified_at=result.get("LastModified"),
            )
        except CancelledError:
            raise
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"上传对象 {key!r}",
            ) from exc

    def open_read(self, key: str) -> BinaryIO:
        try:
            return self.client.get_object(
                Bucket=self.bucket, Key=self._key(key)
            )["Body"]
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"读取对象 {key!r}",
            ) from exc

    def read_range(self, key: str, start: int, length: int) -> bytes:
        if start < 0 or length < 0:
            raise StorageError("范围参数不能为负数。")
        if length == 0:
            return b""
        try:
            body = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key(key),
                Range=f"bytes={start}-{start + length - 1}",
            )["Body"]
            try:
                return body.read()
            finally:
                body.close()
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"范围读取对象 {key!r}",
            ) from exc

    def stat(self, key: str) -> ObjectStat | None:
        try:
            value = self.client.head_object(
                Bucket=self.bucket, Key=self._key(key)
            )
            return ObjectStat(
                key=key,
                size=value["ContentLength"],
                etag=str(value.get("ETag", "")).strip('"') or None,
                modified_at=value.get("LastModified"),
            )
        except Exception as exc:
            response = getattr(exc, "response", {})
            code = str(response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"读取对象元数据 {key!r}",
            ) from exc

    def iter_objects(self, prefix: str = "") -> Iterator[ObjectStat]:
        backend_prefix = self._key(prefix) if prefix else (
            f"{self.prefix}/" if self.prefix else ""
        )
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(
                Bucket=self.bucket, Prefix=backend_prefix
            ):
                for value in page.get("Contents", []):
                    yield ObjectStat(
                        key=self._display_key(value["Key"]),
                        size=value["Size"],
                        etag=str(value.get("ETag", "")).strip('"') or None,
                        modified_at=value.get("LastModified"),
                    )
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation="列举对象",
            ) from exc

    def delete(self, key: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket, Key=self._key(key))
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation=f"删除对象 {key!r}",
            ) from exc

    def _read_lease(self, key: str) -> tuple[dict, str] | None:
        try:
            result = self.client.get_object(
                Bucket=self.bucket, Key=self._key(key)
            )
            body = result["Body"]
            try:
                value = json.loads(body.read().decode("utf-8"))
            finally:
                body.close()
            return value, str(result.get("ETag", "")).strip('"')
        except Exception as exc:
            response = getattr(exc, "response", {})
            code = str(response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise _storage_error(
                exc,
                config=self.config,
                operation="读取远端租约",
            ) from exc

    def _conditional_put(
        self, key: str, value: dict, *, if_none: bool = False, etag: str | None = None
    ) -> str | None:
        payload = json.dumps(value, sort_keys=True).encode("utf-8")
        kwargs = {
            "Bucket": self.bucket,
            "Key": self._key(key),
            "Body": io.BytesIO(payload),
            "ContentType": "application/json",
        }
        if if_none:
            kwargs["IfNoneMatch"] = "*"
        if etag:
            kwargs["IfMatch"] = etag
        try:
            result = self.client.put_object(**kwargs)
            return str(result.get("ETag", "")).strip('"') or None
        except Exception as exc:
            response = getattr(exc, "response", {})
            code = str(response.get("Error", {}).get("Code", ""))
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"PreconditionFailed", "ConditionalRequestConflict"} or status in {
                409,
                412,
            }:
                return None
            raise _storage_error(
                exc,
                config=self.config,
                operation="更新远端租约",
            ) from exc

    def acquire_lease(
        self, key: str, owner: str, ttl_seconds: int
    ) -> RemoteLease | None:
        if self._aliyun_lease is not None:
            return self._aliyun_lease.acquire_lease(
                key,
                owner,
                ttl_seconds,
            )
        now = _now()
        token = secrets.token_urlsafe(24)
        expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        value = {
            "owner": owner,
            "token": token,
            "acquired_at": now.isoformat(),
            "heartbeat_at": now.isoformat(),
            "expires_at": expires_at,
        }
        version = self._conditional_put(key, value, if_none=True)
        if version:
            return RemoteLease(key, owner, token, expires_at, version)
        current = self._read_lease(key)
        if not current:
            return None
        old, old_version = current
        try:
            expired = datetime.fromisoformat(old["expires_at"]) <= now
        except (KeyError, ValueError, TypeError):
            expired = True
        if not expired:
            return None
        version = self._conditional_put(key, value, etag=old_version)
        if not version:
            return None
        return RemoteLease(key, owner, token, expires_at, version)

    def renew_lease(
        self, lease: RemoteLease, ttl_seconds: int
    ) -> RemoteLease | None:
        if self._aliyun_lease is not None:
            return self._aliyun_lease.renew_lease(lease, ttl_seconds)
        current = self._read_lease(lease.key)
        if not current:
            return None
        value, version = current
        if value.get("token") != lease.token:
            return None
        now = _now()
        expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        value["heartbeat_at"] = now.isoformat()
        value["expires_at"] = expires_at
        new_version = self._conditional_put(lease.key, value, etag=version)
        if not new_version:
            return None
        return RemoteLease(
            lease.key, lease.owner, lease.token, expires_at, new_version
        )

    def release_lease(self, lease: RemoteLease) -> None:
        if self._aliyun_lease is not None:
            self._aliyun_lease.release_lease(lease)
            return
        current = self._read_lease(lease.key)
        if not current:
            return
        value, version = current
        if value.get("token") != lease.token:
            return
        value["expires_at"] = _now().isoformat()
        value["released"] = True
        self._conditional_put(lease.key, value, etag=version)

    def _lease_probe_error(self, summary: str) -> StorageError:
        return StorageError(
            f"验证远端租约失败：{summary}",
            details={
                "diagnostic": {
                    "kind": "lease_capability",
                    "title": "对象存储不满足安全租约要求",
                    "summary": summary,
                    "suggestions": [
                        "确认目标支持原子创建、比较交换以及可续期租约后重试。",
                        "不要通过关闭远端锁绕过该检查。",
                    ],
                    "operation": "验证远端租约",
                    "provider": self.provider,
                    "endpoint": self.config.endpoint_url or "AWS 默认 Endpoint",
                    "bucket": self.bucket,
                    "region": self.config.region,
                }
            },
        )

    def _cleanup_lease_probe(self, key: str) -> None:
        if self._aliyun_lease is not None:
            self._aliyun_lease.cleanup_lease_log(key)
        else:
            self.delete(key)

    def validate_capabilities(self) -> dict[str, object]:
        """Exercise the exact lease transitions required by backup engines."""

        marker = secrets.token_hex(16)
        lease_key = f"v1/system/probes/{marker}.lease.json"
        active: RemoteLease | None = None
        try:
            active = self.acquire_lease(
                lease_key,
                "configuration-probe",
                60,
            )
            if active is None:
                raise self._lease_probe_error(
                    "无法原子获取唯一的远端租约探针。"
                )
            renewed = self.renew_lease(active, 60)
            if renewed is None:
                raise self._lease_probe_error(
                    "远端租约探针无法通过比较交换完成续期。"
                )
            active = renewed
            self.release_lease(active)
            active = None

            replacement = self.acquire_lease(
                lease_key,
                "configuration-probe-reacquire",
                60,
            )
            if replacement is None:
                raise self._lease_probe_error(
                    "已释放的远端租约无法被安全地重新获取。"
                )
            active = replacement
            self.release_lease(active)
            active = None
        except Exception:
            if active is not None:
                try:
                    self.release_lease(active)
                except Exception:
                    pass
            try:
                self._cleanup_lease_probe(lease_key)
            except Exception:
                pass
            raise

        self._cleanup_lease_probe(lease_key)
        return {
            "atomic_acquire": True,
            "compare_and_swap": True,
            "renewable_lease": True,
            "lease_protocol": (
                self._aliyun_lease.protocol_name
                if self._aliyun_lease is not None
                else "s3_conditional_put"
            ),
        }

    def abort_stale_multipart_uploads(self, older_than_days: int = 7) -> int:
        cutoff = _now() - timedelta(days=older_than_days)
        count = 0
        try:
            paginator = self.client.get_paginator("list_multipart_uploads")
            prefix = f"{self.prefix}/" if self.prefix else ""
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for upload in page.get("Uploads", []):
                    initiated = upload.get("Initiated")
                    if initiated and initiated < cutoff:
                        self.client.abort_multipart_upload(
                            Bucket=self.bucket,
                            Key=upload["Key"],
                            UploadId=upload["UploadId"],
                        )
                        count += 1
            return count
        except Exception as exc:
            raise _storage_error(
                exc,
                config=self.config,
                operation="清理过期分片上传",
            ) from exc
