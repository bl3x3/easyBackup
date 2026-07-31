from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from easybackup.app import create_app
from easybackup.config import Settings


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
