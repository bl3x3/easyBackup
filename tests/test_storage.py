from __future__ import annotations

import io

import pytest
from botocore.stub import ANY, Stubber

from easybackup.errors import StorageError
from easybackup.models import S3StorageConfig
from easybackup.storage.local import LocalBlobStore
from easybackup.storage.s3 import S3BlobStore


def test_local_storage_atomic_io_and_ranges(tmp_path):
    store = LocalBlobStore(tmp_path / "objects")
    value = store.put_stream("a/b.bin", io.BytesIO(b"0123456789"))
    assert value.size == 10
    assert store.read_bytes("a/b.bin") == b"0123456789"
    assert store.read_range("a/b.bin", 3, 4) == b"3456"
    assert store.stat("a/b.bin").size == 10
    assert [item.key for item in store.iter_objects("a")] == ["a/b.bin"]


def test_local_storage_rejects_path_escape(tmp_path):
    store = LocalBlobStore(tmp_path / "objects")
    with pytest.raises(StorageError):
        store.put_bytes("../outside", b"bad")


def test_remote_lease_owner_token(tmp_path):
    store = LocalBlobStore(tmp_path / "objects")
    first = store.acquire_lease("locks/task.json", "one", 60)
    assert first is not None
    assert store.acquire_lease("locks/task.json", "two", 60) is None
    store.release_lease(first)
    second = store.acquire_lease("locks/task.json", "two", 60)
    assert second is not None
    assert second.token != first.token


def test_s3_conditional_lease_and_missing_stat():
    store = S3BlobStore(
        S3StorageConfig(
            bucket="example-bucket",
            prefix="easybackup/test",
            region="us-east-1",
            endpoint_url="https://s3.example.invalid",
        ),
        {
            "access_key_id": "test-access-key",
            "secret_access_key": "test-secret-key",
            "session_token": None,
        },
    )
    put_members = (
        store.client.meta.service_model.operation_model(
            "PutObject"
        ).input_shape.members
    )
    assert {"IfMatch", "IfNoneMatch"} <= set(put_members)
    with Stubber(store.client) as stubber:
        stubber.add_response(
            "put_object",
            {"ETag": '"lease-version-1"'},
            {
                "Bucket": "example-bucket",
                "Key": "easybackup/test/locks/task.json",
                "Body": ANY,
                "ContentType": "application/json",
                "IfNoneMatch": "*",
            },
        )
        lease = store.acquire_lease("locks/task.json", "host-one", 300)
        assert lease is not None
        assert lease.version == "lease-version-1"

        stubber.add_client_error(
            "head_object",
            service_error_code="NoSuchKey",
            service_message="missing",
            http_status_code=404,
            expected_params={
                "Bucket": "example-bucket",
                "Key": "easybackup/test/missing.bin",
            },
        )
        assert store.stat("missing.bin") is None
