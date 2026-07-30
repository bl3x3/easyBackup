"""Stable manifest serialization, validation and commit-marker helpers."""

from __future__ import annotations

import hashlib
import json
from pathlib import PurePosixPath
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from easybackup.errors import StorageError, ValidationError
from easybackup.models import FileVersionKind, SnapshotManifest
from easybackup.storage.base import BlobStore


def stable_json_bytes(value: Any) -> bytes:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def validate_relative_path(path: str, *, allow_empty: bool = False) -> str:
    normalized = path.replace("\\", "/").strip("/")
    if not normalized and allow_empty:
        return ""
    candidate = PurePosixPath(normalized)
    if (
        not normalized
        or candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or "\x00" in normalized
    ):
        raise ValidationError(f"不安全的快照路径：{path!r}")
    return candidate.as_posix()


def validate_manifest(manifest: SnapshotManifest) -> SnapshotManifest:
    file_paths: set[str] = set()
    archive_keys: set[tuple[str, str]] = set()
    archive_map = {}
    for archive in manifest.archives:
        identity = (archive.snapshot_id, archive.id)
        if identity in archive_keys:
            raise ValidationError(f"Manifest 中存在重复分卷：{identity}")
        archive_keys.add(identity)
        archive_map[identity] = archive
        validate_relative_path(archive.key)
    for item in manifest.files:
        item.path = validate_relative_path(item.path)
        if item.path in file_paths:
            raise ValidationError(f"Manifest 中存在重复文件路径：{item.path}")
        file_paths.add(item.path)
        if (item.origin_snapshot_id, item.archive_id) not in archive_keys:
            raise ValidationError(
                f"文件 {item.path!r} 引用了不存在的归档分卷。"
            )
        reference = item.file_version
        if reference is None:
            continue
        identity = (item.origin_snapshot_id, item.archive_id)
        artifact = archive_map[identity]
        if (
            reference.snapshot_id != item.origin_snapshot_id
            or reference.archive_id != item.archive_id
            or reference.object_key != artifact.key
            or reference.compression != artifact.compression
            or reference.transfer_size != artifact.integrity.size
            or reference.original_size != item.size
            or reference.sha256 != item.sha256.lower()
        ):
            raise ValidationError(
                f"文件 {item.path!r} 的版本引用与归档或逻辑摘要不一致。"
            )
        if reference.kind == FileVersionKind.DELTA:
            assert reference.base is not None
            base_identity = (
                reference.base.snapshot_id,
                reference.base.archive_id,
            )
            base_archive = archive_map.get(base_identity)
            if base_archive is None:
                raise ValidationError(
                    f"差分文件 {item.path!r} 引用的完整基线分卷不存在。"
                )
            if (
                reference.base.object_key != base_archive.key
                or reference.base.compression != base_archive.compression
                or reference.base.snapshot_id == reference.snapshot_id
            ):
                raise ValidationError(
                    f"差分文件 {item.path!r} 的基线定位信息无效。"
                )
    directory_paths: set[str] = set()
    for item in manifest.directories:
        item.path = validate_relative_path(item.path)
        if item.path in directory_paths or item.path in file_paths:
            raise ValidationError(f"Manifest 中存在路径冲突：{item.path}")
        directory_paths.add(item.path)
    manifest.files.sort(key=lambda item: item.path)
    manifest.directories.sort(key=lambda item: item.path)
    manifest.deleted = sorted(
        {validate_relative_path(path) for path in manifest.deleted}
    )
    return manifest


def parse_manifest(payload: bytes) -> SnapshotManifest:
    try:
        value = SnapshotManifest.model_validate_json(payload)
    except PydanticValidationError as exc:
        raise ValidationError(f"Manifest 格式无效：{exc}") from exc
    return validate_manifest(value)


def load_manifest(store: BlobStore, key: str) -> tuple[SnapshotManifest, bytes]:
    payload = store.read_bytes(key)
    return parse_manifest(payload), payload


def commit_key_for(manifest_key: str) -> str:
    if manifest_key.endswith("/manifest.json"):
        return manifest_key[: -len("manifest.json")] + "commit.json"
    return manifest_key + ".commit.json"


def verify_commit_marker(
    store: BlobStore,
    manifest_key: str,
    manifest_payload: bytes,
    snapshot_id: str,
) -> dict[str, Any]:
    key = commit_key_for(manifest_key)
    try:
        value = json.loads(store.read_bytes(key).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValidationError(f"快照提交标记 {key!r} 已损坏。") from exc
    except StorageError:
        raise
    if value.get("version") != 1 or value.get("snapshot_id") != snapshot_id:
        raise ValidationError("快照提交标记与请求的快照不匹配。")
    actual = sha256_bytes(manifest_payload)
    if value.get("manifest_sha256") != actual:
        raise ValidationError("Manifest 的 SHA-256 与提交标记不一致。")
    return value
