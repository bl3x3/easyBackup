"""S3-compatible streaming object storage backend."""

from __future__ import annotations

import io
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import BinaryIO, Iterator

from easybackup.errors import CancelledError, StorageError
from easybackup.models import S3StorageConfig
from easybackup.storage.base import (
    BlobStore,
    CancelCallback,
    ObjectStat,
    ProgressCallback,
    RemoteLease,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class _CancellableReader:
    def __init__(self, raw: BinaryIO, cancelled: CancelCallback | None):
        self.raw = raw
        self.cancelled = cancelled

    def read(self, size: int = -1) -> bytes:
        if self.cancelled and self.cancelled():
            raise CancelledError("操作已取消。")
        return self.raw.read(size)

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
        except ImportError as exc:
            raise StorageError("S3 后端需要安装 boto3。") from exc
        kwargs = {
            "aws_access_key_id": credentials["access_key_id"],
            "aws_secret_access_key": credentials["secret_access_key"],
        }
        if credentials.get("session_token"):
            kwargs["aws_session_token"] = credentials["session_token"]
        if config.region:
            kwargs["region_name"] = config.region
        if config.endpoint_url:
            kwargs["endpoint_url"] = config.endpoint_url
        self.client = boto3.client("s3", **kwargs)
        put_object = self.client.meta.service_model.operation_model(
            "PutObject"
        )
        members = set(put_object.input_shape.members)
        missing_conditions = {"IfMatch", "IfNoneMatch"} - members
        if missing_conditions:
            raise StorageError(
                "当前 boto3/botocore 过旧，S3 PutObject 不支持安全租约"
                "所需的 If-Match/If-None-Match；请安装 boto3>=1.36。"
            )
        self.bucket = config.bucket
        self.prefix = config.prefix.strip("/")
        self.storage_class = config.storage_class
        self.multipart_chunk = config.multipart_chunk_mb * 1024 * 1024

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

            self.client.upload_fileobj(
                _CancellableReader(stream, cancelled),
                self.bucket,
                self._key(key),
                ExtraArgs=extra or None,
                Callback=callback,
                Config=TransferConfig(
                    multipart_threshold=self.multipart_chunk,
                    multipart_chunksize=self.multipart_chunk,
                    max_concurrency=4,
                    use_threads=True,
                ),
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
            raise StorageError(f"上传 S3 对象 {key!r} 失败：{exc}") from exc

    def open_read(self, key: str) -> BinaryIO:
        try:
            return self.client.get_object(
                Bucket=self.bucket, Key=self._key(key)
            )["Body"]
        except Exception as exc:
            raise StorageError(f"读取 S3 对象 {key!r} 失败：{exc}") from exc

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
            raise StorageError(f"范围读取 S3 对象失败：{exc}") from exc

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
            raise StorageError(f"读取 S3 对象元数据失败：{exc}") from exc

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
            raise StorageError(f"列举 S3 对象失败：{exc}") from exc

    def delete(self, key: str) -> None:
        try:
            self.client.delete_object(Bucket=self.bucket, Key=self._key(key))
        except Exception as exc:
            raise StorageError(f"删除 S3 对象 {key!r} 失败：{exc}") from exc

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
            raise StorageError(f"读取 S3 远端锁失败：{exc}") from exc

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
            raise StorageError(f"更新 S3 远端锁失败：{exc}") from exc

    def acquire_lease(
        self, key: str, owner: str, ttl_seconds: int
    ) -> RemoteLease | None:
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
        current = self._read_lease(lease.key)
        if not current:
            return
        value, version = current
        if value.get("token") != lease.token:
            return
        value["expires_at"] = _now().isoformat()
        value["released"] = True
        self._conditional_put(lease.key, value, etag=version)

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
            raise StorageError(f"清理过期 S3 分片上传失败：{exc}") from exc
