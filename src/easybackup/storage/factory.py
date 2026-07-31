"""Create a storage backend from validated task configuration."""

from __future__ import annotations

from easybackup.models import (
    LocalStorageConfig,
    S3StorageConfig,
    SFTPStorageConfig,
    StorageConfig,
)
from easybackup.security import CredentialStore
from easybackup.storage.base import BlobStore
from easybackup.storage.local import LocalBlobStore


def create_store(
    config: StorageConfig,
    credentials: CredentialStore,
) -> BlobStore:
    if isinstance(config, LocalStorageConfig):
        from pathlib import Path

        return LocalBlobStore(Path(config.path))
    if isinstance(config, S3StorageConfig):
        from easybackup.storage.s3 import S3BlobStore

        return S3BlobStore(
            config,
            credentials.get(
                config.credential_profile,
                expected_kind="s3",
            ),
        )
    if isinstance(config, SFTPStorageConfig):
        from easybackup.storage.sftp import SFTPBlobStore

        return SFTPBlobStore(
            config,
            credentials.get(
                config.credential_profile,
                expected_kind="sftp",
            ),
        )
    raise TypeError(f"不支持的存储配置：{type(config)!r}")
