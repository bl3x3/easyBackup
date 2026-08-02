from __future__ import annotations

import io
from urllib.parse import urlsplit

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
from pydantic import ValidationError as PydanticValidationError

from easybackup.errors import StorageError
from easybackup.models import S3StorageConfig, storage_location_identity
from easybackup.storage.base import RemoteLease
from easybackup.storage.local import LocalBlobStore
from easybackup.storage.s3 import (
    S3BlobStore,
    _CancellableReader,
    diagnose_s3_error,
)


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


@pytest.mark.parametrize("region", [None, "", "   "])
def test_aliyun_endpoint_requires_nonempty_region(region):
    with pytest.raises(PydanticValidationError) as raised:
        S3StorageConfig(
            bucket="backup-dinnerparty",
            endpoint_url="oss-cn-shanghai.aliyuncs.com",
            region=region,
        )

    message = str(raised.value)
    assert "Region" in message
    assert "cn-shanghai" in message


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


@pytest.mark.parametrize("limit", [-1, 0.01, 0.99])
def test_s3_upload_limit_rejects_invalid_values(limit):
    with pytest.raises(PydanticValidationError):
        S3StorageConfig(bucket="example-bucket", upload_limit_mbps=limit)


def test_legacy_s3_config_defaults_to_unlimited_upload():
    config = S3StorageConfig.model_validate(
        {
            "kind": "s3",
            "bucket": "example-bucket",
            "multipart_chunk_mb": 16,
        }
    )

    assert config.upload_limit_mbps == 0


def test_s3_upload_limit_does_not_change_storage_location_identity():
    unlimited = S3StorageConfig(
        bucket="example-bucket",
        prefix="easybackup/test",
        region="us-east-1",
        endpoint_url="https://s3.example.invalid",
    )
    limited = unlimited.model_copy(update={"upload_limit_mbps": 25})

    assert storage_location_identity(unlimited) == storage_location_identity(
        limited
    )


def test_s3_cancellable_reader_fills_requested_size_across_short_reads():
    class ShortReader:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload

        def read(self, size: int = -1) -> bytes:
            if not self.payload:
                return b""
            take = len(self.payload) if size < 0 else min(size, 3)
            value, self.payload = self.payload[:take], self.payload[take:]
            return value

    reader = _CancellableReader(ShortReader(b"0123456789abcdef"), None)

    assert reader.read(10) == b"0123456789"
    assert reader.read(10) == b"abcdef"
    assert reader.read(10) == b""


def test_s3_upload_limit_uses_classic_transfer_bandwidth_limiter(
    monkeypatch,
):
    payload = b"bandwidth-limited-upload"
    store = S3BlobStore(
        S3StorageConfig(
            bucket="example-bucket",
            prefix="easybackup/test",
            region="us-east-1",
            endpoint_url="https://s3.example.invalid",
            upload_limit_mbps=8,
        ),
        {
            "access_key_id": "test-access-key",
            "secret_access_key": "test-secret-key",
            "session_token": None,
        },
    )
    captured = {}

    def fake_upload_fileobj(
        stream,
        bucket,
        key,
        *,
        ExtraArgs,
        Callback,
        Config,
    ):
        captured.update(
            {
                "bucket": bucket,
                "key": key,
                "extra": ExtraArgs,
                "config": Config,
            }
        )
        uploaded = stream.read()
        assert uploaded == payload
        Callback(len(uploaded))

    monkeypatch.setattr(store.client, "upload_fileobj", fake_upload_fileobj)
    monkeypatch.setattr(
        store.client,
        "head_object",
        lambda **_kwargs: {
            "ContentLength": len(payload),
            "ETag": '"upload-etag"',
        },
    )
    progress = []

    stored = store.put_stream(
        "volume.bin",
        io.BytesIO(payload),
        progress=progress.append,
    )

    assert stored.size == len(payload)
    assert progress == [len(payload)]
    assert captured["bucket"] == "example-bucket"
    assert captured["key"] == "easybackup/test/volume.bin"
    assert captured["config"].max_bandwidth == 1_000_000
    assert captured["config"].max_concurrency == 1
    assert captured["config"].preferred_transfer_client == "classic"


def test_aliyun_s3_lease_operations_delegate_without_conditional_put(
    monkeypatch,
):
    acquired = RemoteLease(
        "locks/task.json",
        "host-one",
        "token-one",
        "2099-01-01T00:00:00+00:00",
        "10",
    )
    renewed = RemoteLease(
        acquired.key,
        acquired.owner,
        acquired.token,
        "2099-01-01T00:02:00+00:00",
        "20",
    )

    class FakeAliyunLeaseStore:
        protocol_name = "oss_append_position_v1"

        def __init__(self):
            self.calls = []

        def acquire_lease(self, key, owner, ttl_seconds):
            self.calls.append(("acquire", key, owner, ttl_seconds))
            return acquired

        def renew_lease(self, lease, ttl_seconds):
            self.calls.append(("renew", lease, ttl_seconds))
            return renewed

        def release_lease(self, lease):
            self.calls.append(("release", lease))

    fake_lease = FakeAliyunLeaseStore()
    factory_calls = []

    def fake_factory(config, credentials):
        factory_calls.append((config, credentials))
        return fake_lease

    monkeypatch.setattr(
        "easybackup.storage.aliyun_lease.create_aliyun_oss_lease_store",
        fake_factory,
    )
    config = S3StorageConfig(
        bucket="backup-dinnerparty",
        endpoint_url="oss-cn-shanghai.aliyuncs.com",
        region="cn-shanghai",
    )
    credentials = {
        "access_key_id": "test-access-key",
        "secret_access_key": "test-secret-key",
        "session_token": None,
    }
    store = S3BlobStore(config, credentials)
    boto_puts = []
    monkeypatch.setattr(
        store.client,
        "put_object",
        lambda **kwargs: boto_puts.append(kwargs),
    )

    actual = store.acquire_lease("locks/task.json", "host-one", 60)
    assert actual is acquired
    actual_renewed = store.renew_lease(actual, 120)
    assert actual_renewed is renewed
    store.release_lease(actual_renewed)

    assert store._aliyun_lease is fake_lease
    assert factory_calls == [(config, credentials)]
    assert fake_lease.calls == [
        ("acquire", "locks/task.json", "host-one", 60),
        ("renew", acquired, 120),
        ("release", renewed),
    ]
    assert boto_puts == []


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


def test_generic_s3_validate_capabilities_exercises_full_lease_lifecycle(
    monkeypatch,
):
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
    monkeypatch.setattr(
        "easybackup.storage.s3.secrets.token_hex",
        lambda _length: "capability-marker",
    )
    token_values = iter(("token-one", "token-two"))
    monkeypatch.setattr(
        "easybackup.storage.s3.secrets.token_urlsafe",
        lambda _length: next(token_values),
    )
    object_key = (
        "easybackup/test/v1/system/probes/"
        "capability-marker.lease.json"
    )

    def get_response(token, etag, expires_at="2099-01-01T00:00:00+00:00"):
        payload = (
            '{"owner":"configuration-probe","token":"'
            + token
            + '","expires_at":"'
            + expires_at
            + '"}'
        ).encode("utf-8")
        return {
            "Body": StreamingBody(io.BytesIO(payload), len(payload)),
            "ETag": f'"{etag}"',
        }

    put_create = {
        "Bucket": "example-bucket",
        "Key": object_key,
        "Body": ANY,
        "ContentType": "application/json",
        "IfNoneMatch": "*",
    }
    get_current = {
        "Bucket": "example-bucket",
        "Key": object_key,
    }

    with Stubber(store.client) as stubber:
        stubber.add_response(
            "put_object",
            {"ETag": '"lease-version-1"'},
            put_create,
        )
        stubber.add_response(
            "get_object",
            get_response("token-one", "lease-version-1"),
            get_current,
        )
        stubber.add_response(
            "put_object",
            {"ETag": '"lease-version-2"'},
            {
                **{
                    key: value
                    for key, value in put_create.items()
                    if key != "IfNoneMatch"
                },
                "IfMatch": "lease-version-1",
            },
        )
        stubber.add_response(
            "get_object",
            get_response("token-one", "lease-version-2"),
            get_current,
        )
        stubber.add_response(
            "put_object",
            {"ETag": '"lease-version-3"'},
            {
                **{
                    key: value
                    for key, value in put_create.items()
                    if key != "IfNoneMatch"
                },
                "IfMatch": "lease-version-2",
            },
        )
        stubber.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            service_message="object already exists",
            http_status_code=412,
            expected_params=put_create,
        )
        stubber.add_response(
            "get_object",
            get_response(
                "token-one",
                "lease-version-3",
                "2000-01-01T00:00:00+00:00",
            ),
            get_current,
        )
        stubber.add_response(
            "put_object",
            {"ETag": '"lease-version-4"'},
            {
                **{
                    key: value
                    for key, value in put_create.items()
                    if key != "IfNoneMatch"
                },
                "IfMatch": "lease-version-3",
            },
        )
        stubber.add_response(
            "get_object",
            get_response("token-two", "lease-version-4"),
            get_current,
        )
        stubber.add_response(
            "put_object",
            {"ETag": '"lease-version-5"'},
            {
                **{
                    key: value
                    for key, value in put_create.items()
                    if key != "IfNoneMatch"
                },
                "IfMatch": "lease-version-4",
            },
        )
        stubber.add_response(
            "delete_object",
            {},
            get_current,
        )

        result = store.validate_capabilities()

    assert result == {
        "atomic_acquire": True,
        "compare_and_swap": True,
        "renewable_lease": True,
        "lease_protocol": "s3_conditional_put",
    }
