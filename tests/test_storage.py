from __future__ import annotations

import io
from urllib.parse import urlsplit

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.stub import ANY, Stubber
from pydantic import ValidationError as PydanticValidationError

from easybackup.errors import StorageError
from easybackup.models import S3StorageConfig
from easybackup.storage.local import LocalBlobStore
from easybackup.storage.s3 import S3BlobStore, diagnose_s3_error


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


def test_s3_endpoint_is_completed_and_aliyun_service_host_is_upgraded():
    config = S3StorageConfig(
        bucket="backup-dinnerparty",
        endpoint_url="oss-cn-shanghai.aliyuncs.com/",
        region="cn-shanghai",
    )

    assert (
        config.endpoint_url
        == "https://s3.oss-cn-shanghai.aliyuncs.com"
    )

    with pytest.raises(PydanticValidationError) as invalid:
        S3StorageConfig(
            bucket="backup-dinnerparty",
            endpoint_url="ftp://oss-cn-shanghai.aliyuncs.com",
        )
    assert "http:// 或 https://" in str(invalid.value)


def test_aliyun_s3_client_uses_compatible_signature_and_virtual_host():
    store = S3BlobStore(
        S3StorageConfig(
            bucket="backup-dinnerparty",
            endpoint_url="oss-cn-shanghai.aliyuncs.com",
            region="cn-shanghai",
        ),
        {
            "access_key_id": "test-access-key",
            "secret_access_key": "test-secret-key",
            "session_token": None,
        },
    )

    assert store.provider == "aliyun_oss"
    assert store.client.meta.config.signature_version == "s3"
    assert store.client.meta.config.s3["addressing_style"] == "virtual"
    url = store.client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": "backup-dinnerparty",
            "Key": "easybackup/probe",
        },
    )
    assert (
        urlsplit(url).hostname
        == "backup-dinnerparty.s3.oss-cn-shanghai.aliyuncs.com"
    )


@pytest.mark.parametrize(
    ("error", "expected_kind"),
    [
        (
            ClientError(
                {
                    "Error": {
                        "Code": "PublicEndpointForbidden",
                        "Message": "Public endpoint is forbidden",
                    },
                    "ResponseMetadata": {
                        "HTTPStatusCode": 403,
                        "RequestId": "request-public-endpoint",
                    },
                },
                "PutObject",
            ),
            "public_endpoint_forbidden",
        ),
        (
            ClientError(
                {
                    "Error": {
                        "Code": "NoSuchBucket",
                        "Message": "The specified bucket does not exist",
                    },
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            ),
            "bucket",
        ),
        (
            EndpointConnectionError(
                endpoint_url="https://unreachable.example.invalid"
            ),
            "endpoint_unreachable",
        ),
    ],
)
def test_s3_errors_are_classified_for_configuration_ui(
    error,
    expected_kind,
):
    diagnostic = diagnose_s3_error(
        error,
        config=S3StorageConfig(
            bucket="backup-dinnerparty",
            endpoint_url="oss-cn-shanghai.aliyuncs.com",
            region="cn-shanghai",
        ),
        operation="写入配置探针",
    )

    assert diagnostic["kind"] == expected_kind
    assert diagnostic["title"]
    assert diagnostic["summary"]
    assert diagnostic["suggestions"]
    assert diagnostic["endpoint"] == (
        "https://s3.oss-cn-shanghai.aliyuncs.com"
    )


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
