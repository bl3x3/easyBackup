from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from easybackup.app import create_app
from easybackup.config import Settings
from easybackup.models import SFTPStorageConfig
from easybackup.storage.base import ObjectStat


def test_api_task_backup_flow(tmp_path):
    source = tmp_path / "source"
    repository = tmp_path / "repository"
    source.mkdir()
    (source / "hello.txt").write_text("hello", encoding="utf-8")
    settings = Settings(
        data_dir=tmp_path / "data",
        timezone="Asia/Shanghai",
        open_browser=False,
        credential_backend="encrypted_file",
        integrity_block_size=1024,
    )
    with TestClient(
        create_app(settings), base_url="http://127.0.0.1"
    ) as client:
        root = client.get("/")
        assert root.headers["x-frame-options"] == "DENY"
        assert 'id="storageDiagnosticPanel"' in root.text
        assert "s3.oss-cn-shanghai.aliyuncs.com" in root.text
        assert "frame-ancestors 'none'" in root.headers[
            "content-security-policy"
        ]
        assert (
            client.get("/", headers={"host": "attacker.example"}).status_code
            == 400
        )
        assert (
            client.post(
                "/api/v1/tasks/not-real/prune",
                headers={"origin": "https://attacker.example"},
            ).status_code
            == 403
        )
        docs = client.get("/api/docs")
        assert docs.status_code == 200
        docs_csp = docs.headers["content-security-policy"]
        assert "https://cdn.jsdelivr.net" in docs_csp
        assert "'unsafe-inline'" in docs_csp
        assert "https://cdn.jsdelivr.net" in docs.text
        with pytest.raises(WebSocketDisconnect) as cross_origin:
            with client.websocket_connect(
                "/api/v1/ws",
                headers={"origin": "https://attacker.example"},
            ):
                pass
        assert cross_origin.value.code == 4403

        assert client.get("/api/v1/health").status_code == 200
        unsafe = client.post(
            "/api/v1/tasks",
            json={
                "name": "unsafe-self-backup",
                "source_path": str(settings.data_dir),
                "storage": {
                    "kind": "local",
                    "path": str(tmp_path / "unsafe-repository"),
                },
            },
        )
        assert unsafe.status_code == 422
        assert "数据目录重叠" in unsafe.json()["detail"]

        response = client.post(
            "/api/v1/tasks",
            json={
                "name": "api-test",
                "source_path": str(source),
                "storage": {"kind": "local", "path": str(repository)},
                "compression": "gzip",
                "shard_size_mb": 8,
            },
        )
        assert response.status_code == 201, response.text
        task_id = response.json()["id"]
        operation = client.post(
            f"/api/v1/tasks/{task_id}/run",
            json={"force_full": False},
        )
        assert operation.status_code == 202, operation.text
        operation_id = operation.json()["id"]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            current = client.get(
                f"/api/v1/operations/{operation_id}"
            ).json()
            if current["status"] in {"completed", "failed", "cancelled"}:
                break
            time.sleep(0.05)
        assert current["status"] == "completed", current
        snapshots = client.get(
            f"/api/v1/snapshots?task_id={task_id}"
        ).json()
        assert len(snapshots) == 1
        manifest = client.get(
            f"/api/v1/snapshots/{snapshots[0]['id']}/manifest"
        )
        assert manifest.status_code == 200
        assert manifest.json()["files"][0]["path"] == "hello.txt"


def test_storage_configuration_probe_is_verified_and_removed(tmp_path):
    repository = tmp_path / "probe-repository"
    settings = Settings(
        data_dir=tmp_path / "data",
        open_browser=False,
        credential_backend="encrypted_file",
    )
    with TestClient(
        create_app(settings),
        base_url="http://127.0.0.1",
    ) as client:
        response = client.post(
            "/api/v1/storage/test",
            json={
                "storage": {
                    "kind": "local",
                    "path": str(repository),
                }
            },
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["ok"] is True
        assert result["kind"] == "local"
        assert result["target"] == str(repository.resolve())
        assert result["latency_ms"] >= 1
        assert repository.is_dir()
        assert not list(repository.rglob("*.probe"))


def test_s3_configuration_probe_reports_actionable_common_errors(tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data",
        open_browser=False,
        credential_backend="encrypted_file",
    )
    with TestClient(
        create_app(settings),
        base_url="http://127.0.0.1",
    ) as client:
        missing_credential = client.post(
            "/api/v1/storage/test",
            json={
                "storage": {
                    "kind": "s3",
                    "bucket": "backup-dinnerparty",
                    "endpoint_url": "oss-cn-shanghai.aliyuncs.com",
                    "region": "cn-shanghai",
                    "credential_profile": "aliyun",
                }
            },
        )
        assert missing_credential.status_code == 404
        problem = missing_credential.json()
        diagnostic = problem["details"]["diagnostic"]
        assert diagnostic["kind"] == "credential_profile"
        assert diagnostic["endpoint"] == (
            "https://s3.oss-cn-shanghai.aliyuncs.com"
        )
        assert diagnostic["suggestions"]

        invalid_endpoint = client.post(
            "/api/v1/storage/test",
            json={
                "storage": {
                    "kind": "s3",
                    "bucket": "backup-dinnerparty",
                    "endpoint_url": "ftp://oss-cn-shanghai.aliyuncs.com",
                    "credential_profile": "aliyun",
                }
            },
        )
        assert invalid_endpoint.status_code == 422
        field_errors = invalid_endpoint.json()["field_errors"]
        assert any(
            issue["path"] == "storage.s3.endpoint_url"
            or issue["path"] == "storage.endpoint_url"
            for issue in field_errors
        )


def test_sftp_task_json_round_trip_and_credential_delete_is_protected(
    tmp_path,
):
    source = tmp_path / "sftp-source"
    source.mkdir()
    settings = Settings(
        data_dir=tmp_path / "data",
        open_browser=False,
        credential_backend="encrypted_file",
    )
    with TestClient(
        create_app(settings),
        base_url="http://127.0.0.1",
    ) as client:
        credential = client.post(
            "/api/v1/credentials",
            json={
                "kind": "sftp",
                "profile": "sftp-prod",
                "username": "backup-user",
                "auth_method": "password",
                "password": "never-return-this-password",
            },
        )
        assert credential.status_code == 201, credential.text
        assert credential.json()["kind"] == "sftp"
        assert "never-return-this-password" not in credential.text

        payload = {
            "name": "sftp-api-task",
            "source_path": str(source),
            "storage": {
                "kind": "sftp",
                "host": "Backup.Internal.Example",
                "port": 2222,
                "base_path": "/srv/easybackup",
                "credential_profile": "sftp-prod",
                "host_key_fingerprint": (
                    "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                ),
                "connect_timeout_seconds": 30,
            },
        }
        created = client.post("/api/v1/tasks", json=payload)
        assert created.status_code == 201, created.text
        storage = created.json()["storage"]
        assert storage == {
            "kind": "sftp",
            "host": "backup.internal.example",
            "port": 2222,
            "base_path": "/srv/easybackup",
            "credential_profile": "sftp-prod",
            "host_key_fingerprint": (
                "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ),
            "known_hosts_path": None,
            "connect_timeout_seconds": 30,
        }
        listed = client.get("/api/v1/tasks")
        assert listed.status_code == 200
        assert listed.json()[0]["storage"] == storage
        assert "never-return-this-password" not in listed.text

        blocked = client.delete("/api/v1/credentials/sftp-prod")
        assert blocked.status_code == 409
        assert "仍被任务" in blocked.json()["detail"]
        profiles = client.get("/api/v1/credentials").json()
        assert [item["profile"] for item in profiles] == ["sftp-prod"]


def test_sftp_configuration_probe_reports_capabilities_and_connection(
    tmp_path,
    monkeypatch,
):
    class ProbeStore:
        provider = "sftp"

        def __init__(self):
            self.payloads: dict[str, bytes] = {}
            self.calls: list[str] = []

        def validate_capabilities(self):
            self.calls.append("validate_capabilities")
            return {
                "exclusive_create": True,
                "atomic_posix_rename": True,
                "concurrent_sessions": True,
                "renewable_lease": True,
            }

        def put_bytes(self, key, payload, *, metadata=None):
            self.calls.append("put")
            assert metadata == {
                "easybackup-artifact": "configuration-probe"
            }
            self.payloads[key] = bytes(payload)
            return ObjectStat(key=key, size=len(payload))

        def stat(self, key):
            self.calls.append("stat")
            payload = self.payloads.get(key)
            return (
                ObjectStat(key=key, size=len(payload))
                if payload is not None
                else None
            )

        def read_bytes(self, key):
            self.calls.append("read")
            return self.payloads[key]

        def delete(self, key):
            self.calls.append("delete")
            self.payloads.pop(key, None)

    probe_store = ProbeStore()

    def fake_create_store(config, credentials):
        del credentials
        assert isinstance(config, SFTPStorageConfig)
        assert config.credential_profile == "sftp-probe"
        return probe_store

    monkeypatch.setattr("easybackup.app.create_store", fake_create_store)
    settings = Settings(
        data_dir=tmp_path / "data",
        open_browser=False,
        credential_backend="encrypted_file",
    )
    with TestClient(
        create_app(settings),
        base_url="http://127.0.0.1",
    ) as client:
        response = client.post(
            "/api/v1/storage/test",
            json={
                "storage": {
                    "kind": "sftp",
                    "host": "backup.internal.example",
                    "port": 2222,
                    "base_path": "/srv/easybackup",
                    "credential_profile": "sftp-probe",
                    "host_key_fingerprint": (
                        "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    ),
                }
            },
        )

    assert response.status_code == 200, response.text
    result = response.json()
    assert result["kind"] == "sftp"
    assert result["target"] == (
        "sftp://backup.internal.example:2222/srv/easybackup"
    )
    assert result["connection"] == {
        "provider": "sftp",
        "host": "backup.internal.example",
        "port": 2222,
        "base_path": "/srv/easybackup",
        "host_key_verification": "fingerprint",
        "capabilities": {
            "exclusive_create": True,
            "atomic_posix_rename": True,
            "concurrent_sessions": True,
            "renewable_lease": True,
        },
    }
    assert probe_store.calls == [
        "validate_capabilities",
        "put",
        "stat",
        "read",
        "delete",
    ]
    assert probe_store.payloads == {}


def test_sftp_configuration_probe_reports_missing_credential(tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data",
        open_browser=False,
        credential_backend="encrypted_file",
    )
    with TestClient(
        create_app(settings),
        base_url="http://127.0.0.1",
    ) as client:
        response = client.post(
            "/api/v1/storage/test",
            json={
                "storage": {
                    "kind": "sftp",
                    "host": "backup.internal.example",
                    "port": 2222,
                    "base_path": "/srv/easybackup",
                    "credential_profile": "missing-sftp-profile",
                    "host_key_fingerprint": (
                        "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    ),
                }
            },
        )

    assert response.status_code == 404
    problem = response.json()
    assert problem["code"] == "NOT_FOUND"
    diagnostic = problem["details"]["diagnostic"]
    assert diagnostic["kind"] == "credential_profile"
    assert diagnostic["provider"] == "sftp"
    assert diagnostic["host"] == "backup.internal.example"
    assert diagnostic["port"] == 2222
    assert diagnostic["base_path"] == "/srv/easybackup"
    assert diagnostic["suggestions"]


def test_api_token_browser_session_and_websocket(tmp_path):
    settings = Settings(
        data_dir=tmp_path / "data",
        open_browser=False,
        api_token="correct-horse-battery-staple",
        credential_backend="encrypted_file",
    )
    app = create_app(settings)
    with TestClient(app, base_url="http://127.0.0.1") as client:
        health = client.get("/api/v1/health")
        assert health.status_code == 200
        assert health.json()["authentication_required"] is True
        assert client.get("/api/v1/system/status").status_code == 401
        assert (
            client.get(
                "/api/v1/system/status",
                headers={
                    "Authorization": (
                        "Bearer correct-horse-battery-staple"
                    )
                },
            ).status_code
            == 200
        )

        with pytest.raises(WebSocketDisconnect) as rejected:
            with client.websocket_connect(
                "/api/v1/ws?token=correct-horse-battery-staple",
                headers={
                    "host": "127.0.0.1",
                    "origin": "http://127.0.0.1",
                },
            ):
                pass
        assert rejected.value.code == 4401

        assert (
            client.post("/api/v1/session", json={"token": "wrong"}).status_code
            == 401
        )
        session = client.post(
            "/api/v1/session",
            json={"token": "correct-horse-battery-staple"},
        )
        assert session.status_code == 200
        cookie = session.headers["set-cookie"]
        assert "HttpOnly" in cookie
        assert "SameSite=strict" in cookie
        assert "correct-horse-battery-staple" not in cookie
        assert client.get("/api/v1/system/status").status_code == 200
        session_value = client.cookies.get("easybackup_session")
        assert session_value

        with client.websocket_connect(
            "/api/v1/ws",
            headers={
                # Starlette's in-memory WS transport selects cookies against
                # ws://testserver even when TestClient has a custom HTTP
                # base_url, so pass the already-issued cookie explicitly.
                "cookie": f"easybackup_session={session_value}",
                "host": "127.0.0.1",
                "origin": "http://127.0.0.1",
            },
        ) as websocket:
            hello = websocket.receive_json()
        assert hello["type"] == "hello"
        assert isinstance(hello["timestamp"], str)
        deadline = time.monotonic() + 1
        while app.state.events._subscribers and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not app.state.events._subscribers
