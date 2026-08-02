"""Pydantic models and lightweight domain records."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class StrEnum(str, Enum):
    """Python 3.10-compatible equivalent of :class:`enum.StrEnum`."""

    def __str__(self) -> str:
        return self.value


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def is_aliyun_oss_endpoint(endpoint_url: str | None) -> bool:
    """Return whether an endpoint uses an Alibaba Cloud OSS service domain."""

    if not endpoint_url:
        return False
    try:
        hostname = (urlsplit(endpoint_url).hostname or "").lower()
    except ValueError:
        return False
    labels = hostname.split(".")
    return (
        len(labels) >= 3
        and labels[-2:] == ["aliyuncs", "com"]
        and any(label.startswith("oss-") for label in labels[:-2])
    )


def normalize_s3_endpoint_url(value: object) -> str | None:
    """Normalize a user-entered S3 endpoint into a complete service URL."""

    if value is None:
        return None
    endpoint = str(value).strip()
    if not endpoint:
        return None
    if "://" not in endpoint:
        endpoint = f"https://{endpoint}"

    try:
        parsed = urlsplit(endpoint)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Endpoint URL 格式无效。") from exc

    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("Endpoint URL 仅支持 http:// 或 https://。")
    if not parsed.hostname:
        raise ValueError("Endpoint URL 缺少有效的主机名。")
    if parsed.username or parsed.password:
        raise ValueError("Endpoint URL 不能包含用户名或密码。")
    if parsed.query or parsed.fragment:
        raise ValueError("Endpoint URL 不能包含查询参数或片段。")

    hostname = parsed.hostname.lower()
    # boto3 accesses OSS through its S3-compatible service endpoint.  Users
    # commonly paste the native OSS endpoint, so transparently upgrade the
    # service host while leaving bucket domains and custom CNAMEs untouched.
    if (
        hostname.startswith("oss-")
        and hostname.endswith(".aliyuncs.com")
    ):
        hostname = f"s3.{hostname}"

    host_for_netloc = f"[{hostname}]" if ":" in hostname else hostname
    netloc = f"{host_for_netloc}:{port}" if port is not None else host_for_netloc
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


class LocalStorageConfig(StrictModel):
    kind: Literal["local"] = "local"
    path: str = Field(min_length=1)


class S3StorageConfig(StrictModel):
    kind: Literal["s3"] = "s3"
    bucket: str = Field(min_length=1)
    prefix: str = "easybackup"
    region: str | None = None
    endpoint_url: str | None = None
    credential_profile: str = "default"
    storage_class: str | None = None
    multipart_chunk_mb: int = Field(default=16, ge=5, le=512)
    # Decimal megabits per second, matching how network links are marketed.
    # Zero disables throttling.
    upload_limit_mbps: float = Field(default=0, ge=0, le=100_000)

    @field_validator("endpoint_url", mode="before")
    @classmethod
    def normalize_endpoint_url(cls, value: object) -> str | None:
        return normalize_s3_endpoint_url(value)

    @field_validator("upload_limit_mbps")
    @classmethod
    def validate_upload_limit(cls, value: float) -> float:
        if 0 < value < 1:
            raise ValueError("上传限速必须为 0（不限速）或至少 1 Mbps。")
        return value

    @model_validator(mode="after")
    def require_aliyun_region(self) -> "S3StorageConfig":
        if is_aliyun_oss_endpoint(self.endpoint_url) and not (
            self.region or ""
        ).strip():
            raise ValueError(
                "阿里云 OSS 必须填写 Bucket 所在 Region，例如 cn-shanghai。"
            )
        return self


class SFTPStorageConfig(StrictModel):
    kind: Literal["sftp"] = "sftp"
    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=22, ge=1, le=65535)
    base_path: str = Field(default="easybackup", max_length=4096)
    credential_profile: str = "default"
    host_key_fingerprint: str | None = None
    known_hosts_path: str | None = None
    connect_timeout_seconds: int = Field(default=15, ge=1, le=120)

    @field_validator("host", mode="before")
    @classmethod
    def normalize_host(cls, value: object) -> str:
        host = str(value).strip()
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        if (
            not host
            or "\x00" in host
            or any(character.isspace() for character in host)
            or "://" in host
            or any(character in host for character in "/\\@")
        ):
            raise ValueError("SFTP Host 只能填写主机名或 IP 地址。")
        return host.lower()

    @field_validator("base_path", mode="before")
    @classmethod
    def normalize_base_path(cls, value: object) -> str:
        raw = str(value).strip()
        if not raw or "\x00" in raw or "\\" in raw:
            raise ValueError("SFTP 远端根目录必须使用 POSIX 路径。")
        path = PurePosixPath(raw)
        parts = path.parts[1:] if path.is_absolute() else path.parts
        if (
            (not path.is_absolute() and not parts)
            or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError("SFTP 远端根目录包含不安全路径片段。")
        return path.as_posix()

    @field_validator("credential_profile", mode="before")
    @classmethod
    def normalize_credential_profile(cls, value: object) -> str:
        profile = str(value).strip()
        if not profile:
            raise ValueError("SFTP 凭据配置名称不能为空。")
        return profile

    @field_validator("known_hosts_path", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        stripped = str(value).strip()
        if "\x00" in stripped:
            raise ValueError("known_hosts 路径不能包含 NUL 字符。")
        return stripped or None

    @field_validator("host_key_fingerprint", mode="before")
    @classmethod
    def normalize_host_key_fingerprint(
        cls, value: object
    ) -> str | None:
        if value is None:
            return None
        fingerprint = str(value).strip()
        if not fingerprint:
            return None
        if fingerprint.lower().startswith("sha256:"):
            fingerprint = f"SHA256:{fingerprint.split(':', 1)[1]}"
        if not re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}=?", fingerprint):
            raise ValueError(
                "主机密钥指纹必须是 OpenSSH SHA256:... 格式。"
            )
        return fingerprint.rstrip("=")

    @model_validator(mode="after")
    def choose_host_key_source(self) -> "SFTPStorageConfig":
        if self.host_key_fingerprint and self.known_hosts_path:
            raise ValueError(
                "主机密钥指纹与 known_hosts 路径只能选择一种。"
            )
        return self


StorageConfig = Annotated[
    LocalStorageConfig | S3StorageConfig | SFTPStorageConfig,
    Field(discriminator="kind"),
]


def storage_location_identity(storage: StorageConfig) -> dict[str, Any]:
    """Return fields that identify where objects live, not transfer tuning."""

    value = storage.model_dump(mode="json")
    if isinstance(storage, S3StorageConfig):
        value.pop("upload_limit_mbps", None)
    return value


DEFAULT_EXCLUDES = [
    ".git/**",
    "__pycache__/**",
    "*.tmp",
    "*.part",
    "Thumbs.db",
    ".DS_Store",
]


class TaskBase(StrictModel):
    name: str = Field(min_length=1, max_length=120)
    source_path: str = Field(min_length=1)
    storage: StorageConfig
    schedule: str | None = None
    enabled: bool = True
    excludes: list[str] = Field(default_factory=lambda: list(DEFAULT_EXCLUDES))
    compression: Literal["auto", "zstd", "gzip", "none"] = "auto"
    compression_level: int = Field(default=3, ge=1, le=19)
    shard_size_mb: int = Field(default=256, ge=8, le=4096)
    # A full snapshot followed by six incrementals yields a weekly re-base
    # when the task runs once per day.
    full_every: int = Field(default=6, ge=1, le=1000)
    retention_chains: int = Field(default=3, ge=1, le=1000)
    retention_days: int = Field(default=30, ge=1, le=36500)
    follow_symlinks: bool = False
    delta_enabled: bool = True
    delta_threshold_mb: int = Field(default=100, ge=1, le=1_048_576)
    delta_max_ratio: float = Field(default=0.9, gt=0, le=1)

    @field_validator("schedule")
    @classmethod
    def normalize_schedule(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @field_validator("excludes")
    @classmethod
    def normalize_excludes(cls, value: list[str]) -> list[str]:
        return [pattern.strip() for pattern in value if pattern.strip()]


class TaskCreate(TaskBase):
    pass


class TaskUpdate(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    source_path: str | None = Field(default=None, min_length=1)
    storage: StorageConfig | None = None
    schedule: str | None = None
    enabled: bool | None = None
    excludes: list[str] | None = None
    compression: Literal["auto", "zstd", "gzip", "none"] | None = None
    compression_level: int | None = Field(default=None, ge=1, le=19)
    shard_size_mb: int | None = Field(default=None, ge=8, le=4096)
    full_every: int | None = Field(default=None, ge=1, le=1000)
    retention_chains: int | None = Field(default=None, ge=1, le=1000)
    retention_days: int | None = Field(default=None, ge=1, le=36500)
    follow_symlinks: bool | None = None
    delta_enabled: bool | None = None
    delta_threshold_mb: int | None = Field(
        default=None,
        ge=1,
        le=1_048_576,
    )
    delta_max_ratio: float | None = Field(default=None, gt=0, le=1)


class Task(TaskBase):
    id: str
    created_at: str
    updated_at: str


class SnapshotKind(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


class SnapshotStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Snapshot(StrictModel):
    id: str
    task_id: str
    kind: SnapshotKind
    chain_id: str
    parent_snapshot_id: str | None = None
    status: SnapshotStatus
    manifest_key: str | None = None
    storage: StorageConfig
    compression: Literal["zstd", "gzip", "none"]
    archives: list["ArchiveObject"] = Field(default_factory=list)
    file_count: int = 0
    changed_count: int = 0
    deleted_count: int = 0
    archive_size: int = 0
    archive_sha256: str | None = None
    integrity: dict[str, Any] = Field(default_factory=dict)
    started_at: str
    completed_at: str | None = None
    error: str | None = None
    last_verified_at: str | None = None
    verify_status: str | None = None


class FileVersionKind(StrEnum):
    FULL = "full"
    DELTA = "delta"


class DeltaBaseReference(StrictModel):
    """Self-contained locator for the full file used as an xdelta base."""

    version_id: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    archive_id: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    compression: Literal["zstd", "gzip", "none"]
    original_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")

    @field_validator("sha256")
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        return value.lower()


class FileVersionReference(StrictModel):
    """Portable content reference embedded in a snapshot manifest."""

    version_id: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    kind: FileVersionKind
    base_version_id: str | None = None
    base: DeltaBaseReference | None = None
    archive_id: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    compression: Literal["zstd", "gzip", "none"]
    original_size: int = Field(ge=0)
    transfer_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")

    @field_validator("sha256")
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        return value.lower()

    @model_validator(mode="after")
    def validate_dependency(self) -> "FileVersionReference":
        if self.kind == FileVersionKind.FULL:
            if self.base_version_id is not None or self.base is not None:
                raise ValueError("完整文件版本不能引用基线版本")
        elif self.base_version_id is None or self.base is None:
            raise ValueError("差分文件版本必须包含完整的基线定位信息")
        elif self.base.version_id != self.base_version_id:
            raise ValueError("差分文件的基线 ID 与基线定位信息不一致")
        if self.base_version_id == self.version_id:
            raise ValueError("文件版本不能引用自身")
        return self


class FileVersion(StrictModel):
    """SQLite record for a large-file baseline or xdelta patch."""

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        min_length=1,
    )
    task_id: str = Field(min_length=1)
    chain_id: str = Field(min_length=1)
    file_path: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    kind: FileVersionKind
    base_version_id: str | None = None
    archive_id: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    compression: Literal["zstd", "gzip", "none"]
    original_size: int = Field(ge=0)
    transfer_size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    created_at: str = Field(default_factory=utc_now_iso)

    @field_validator("sha256")
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        return value.lower()

    @model_validator(mode="after")
    def validate_dependency(self) -> "FileVersion":
        if self.kind == FileVersionKind.FULL and self.base_version_id is not None:
            raise ValueError("完整文件版本不能引用基线版本")
        if self.kind == FileVersionKind.DELTA and self.base_version_id is None:
            raise ValueError("差分文件版本必须引用基线版本")
        if self.base_version_id == self.id:
            raise ValueError("文件版本不能引用自身")
        return self

    def as_base_reference(self) -> DeltaBaseReference:
        if self.kind != FileVersionKind.FULL:
            raise ValueError("只有完整文件版本可以作为差分基线")
        return DeltaBaseReference(
            version_id=self.id,
            snapshot_id=self.snapshot_id,
            archive_id=self.archive_id,
            object_key=self.object_key,
            compression=self.compression,
            original_size=self.original_size,
            sha256=self.sha256,
        )

    def as_reference(
        self,
        *,
        base: DeltaBaseReference | None = None,
    ) -> FileVersionReference:
        return FileVersionReference(
            version_id=self.id,
            snapshot_id=self.snapshot_id,
            kind=self.kind,
            base_version_id=self.base_version_id,
            base=base,
            archive_id=self.archive_id,
            object_key=self.object_key,
            compression=self.compression,
            original_size=self.original_size,
            transfer_size=self.transfer_size,
            sha256=self.sha256,
        )


class ManifestFile(StrictModel):
    path: str
    size: int = Field(ge=0)
    mtime_ns: int
    mode: int
    sha256: str
    origin_snapshot_id: str
    archive_id: str
    file_version: FileVersionReference | None = None


class ManifestDirectory(StrictModel):
    path: str
    mtime_ns: int
    mode: int


class ArchiveIntegrity(StrictModel):
    sha256: str
    size: int
    block_size: int
    crc32: list[str]


class ArchiveObject(StrictModel):
    id: str
    snapshot_id: str
    key: str
    compression: Literal["zstd", "gzip", "none"]
    integrity: ArchiveIntegrity
    file_count: int


class SnapshotManifest(StrictModel):
    version: Literal[1] = 1
    snapshot_id: str
    task_id: str
    kind: SnapshotKind
    chain_id: str
    parent_snapshot_id: str | None = None
    created_at: str
    source_path: str
    # Hash of the canonical local path.  It detects source-root switches
    # without disclosing the full local path in remote storage.
    source_fingerprint: str | None = None
    archives: list[ArchiveObject]
    files: list[ManifestFile]
    directories: list[ManifestDirectory] = Field(default_factory=list)
    deleted: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    archive_integrity: ArchiveIntegrity


class OperationKind(StrEnum):
    BACKUP = "backup"
    RESTORE = "restore"
    SCRUB = "scrub"
    PRUNE = "prune"


class OperationStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Operation(StrictModel):
    id: str
    task_id: str
    snapshot_id: str | None = None
    kind: OperationKind
    status: OperationStatus
    progress: float | None = None
    phase: str
    message: str
    stats: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None


class RunRequest(StrictModel):
    force_full: bool = False


class StorageProbeRequest(StrictModel):
    storage: StorageConfig


class RestoreRequest(StrictModel):
    snapshot_id: str
    destination_path: str = Field(min_length=1)
    paths: list[str] = Field(default_factory=list)
    restore_all: bool = False
    overwrite: Literal["skip", "overwrite", "rename"] = "skip"
    verify: bool = True

    @field_validator("paths")
    @classmethod
    def clean_paths(cls, value: list[str]) -> list[str]:
        return [path.strip().replace("\\", "/") for path in value if path.strip()]


class ScrubRequest(StrictModel):
    deep: bool = False
    sample_ratio: float = Field(default=0.01, gt=0, le=1)


class CredentialWrite(StrictModel):
    kind: Literal["s3", "sftp"] = "s3"
    profile: str = Field(min_length=1, max_length=120)
    access_key_id: str | None = Field(default=None, min_length=1)
    secret_access_key: str | None = Field(
        default=None,
        min_length=1,
        repr=False,
    )
    session_token: str | None = None
    username: str | None = Field(default=None, min_length=1)
    auth_method: Literal["password", "private_key"] | None = None
    password: str | None = Field(default=None, min_length=1, repr=False)
    private_key: str | None = Field(default=None, min_length=1, repr=False)
    private_key_passphrase: str | None = Field(
        default=None,
        min_length=1,
        repr=False,
    )

    @field_validator(
        "profile",
        "access_key_id",
        "secret_access_key",
        "session_token",
        "username",
        mode="before",
    )
    @classmethod
    def strip_credential_whitespace(cls, value: object) -> object:
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @field_validator("private_key", mode="before")
    @classmethod
    def normalize_private_key(cls, value: object) -> object:
        if isinstance(value, str):
            normalized = value.replace("\r\n", "\n").strip()
            return f"{normalized}\n" if normalized else None
        return value

    @model_validator(mode="after")
    def validate_credential_kind(self) -> "CredentialWrite":
        if self.kind == "s3":
            if not self.access_key_id or not self.secret_access_key:
                raise ValueError("S3 凭据必须包含 Access Key ID 与 Secret。")
            if any(
                value is not None
                for value in (
                    self.username,
                    self.auth_method,
                    self.password,
                    self.private_key,
                    self.private_key_passphrase,
                )
            ):
                raise ValueError("S3 凭据不能混入 SFTP 认证字段。")
            return self

        if not self.username:
            raise ValueError("SFTP 凭据必须包含用户名。")
        if self.auth_method == "password":
            if not self.password:
                raise ValueError("SFTP 密码认证必须填写密码。")
            if self.private_key or self.private_key_passphrase:
                raise ValueError("密码认证不能混入私钥字段。")
        elif self.auth_method == "private_key":
            if not self.private_key:
                raise ValueError("SFTP 私钥认证必须填写私钥。")
            if self.password:
                raise ValueError("私钥认证不能混入登录密码。")
        else:
            raise ValueError("SFTP 凭据必须选择认证方式。")
        if any(
            value is not None
            for value in (
                self.access_key_id,
                self.secret_access_key,
                self.session_token,
            )
        ):
            raise ValueError("SFTP 凭据不能混入 S3 访问密钥字段。")
        return self


class ApiSessionRequest(StrictModel):
    token: str = Field(min_length=1)


class CredentialStatus(StrictModel):
    profile: str
    backend: str
    kind: Literal["s3", "sftp"] = "s3"
    access_key_hint: str | None = None
    identity_hint: str | None = None
    auth_method: Literal["password", "private_key"] | None = None
    has_session_token: bool = False
    has_password: bool = False
    has_private_key: bool = False
    updated_at: str


class ProgressUpdate(StrictModel):
    phase: str
    progress: float | None = Field(default=None, ge=0, le=100)
    message: str = ""
    stats: dict[str, Any] = Field(default_factory=dict)
