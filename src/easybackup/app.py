"""FastAPI application factory and REST/WebSocket control plane."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import secrets
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import (
    FastAPI,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from easybackup import __version__
from easybackup.archive import tool_capabilities
from easybackup.config import Settings
from easybackup.db import Database
from easybackup.engine import BackupEngine, MaintenanceEngine, RestoreEngine
from easybackup.errors import (
    ConflictError,
    CredentialError,
    EasyBackupError,
    NotFoundError,
    StorageError,
    ValidationError,
)
from easybackup.events import EventBus
from easybackup.logging_setup import configure_logging
from easybackup.locking import TaskLock
from easybackup.manager import OperationManager
from easybackup.manifest import load_manifest, verify_commit_marker
from easybackup.models import (
    ApiSessionRequest,
    CredentialWrite,
    LocalStorageConfig,
    RestoreRequest,
    RunRequest,
    S3StorageConfig,
    ScrubRequest,
    StorageProbeRequest,
    Task,
    TaskCreate,
    TaskUpdate,
    utc_now_iso,
)
from easybackup.reconcile import reconcile_incomplete_snapshots
from easybackup.scheduler import SchedulerService
from easybackup.security import CredentialStore
from easybackup.storage import create_store


API_PREFIX = "/api/v1"
SESSION_COOKIE = "easybackup_session"
SESSION_MAX_AGE_SECONDS = 12 * 60 * 60
SAFE_HTTP_METHODS = {"GET", "HEAD", "OPTIONS"}
logger = logging.getLogger(__name__)


def _host_name(value: str) -> str:
    try:
        return (urlsplit(f"//{value}").hostname or "").lower()
    except ValueError:
        return ""


def _allowed_hosts(settings: Settings) -> set[str]:
    configured = {
        _host_name(value)
        for value in settings.allowed_hosts
        if _host_name(value)
    }
    if settings.host in {"0.0.0.0", "::"} and not configured:
        return {"*"}
    configured.add(_host_name(settings.host))
    if settings.host in {
        "127.0.0.1",
        "::1",
        "localhost",
        "ip6-localhost",
    }:
        configured.update(
            {"127.0.0.1", "::1", "localhost", "ip6-localhost"}
        )
    configured.discard("")
    return configured


def _host_allowed(host_header: str, allowed_hosts: set[str]) -> bool:
    return "*" in allowed_hosts or _host_name(host_header) in allowed_hosts


def _origin_matches_host(origin: str, host_header: str) -> bool:
    try:
        parsed = urlsplit(origin)
        host = urlsplit(f"//{host_header}")
        origin_port = parsed.port or (
            443 if parsed.scheme == "https" else 80
        )
        host_port = host.port or origin_port
    except ValueError:
        return False
    return bool(
        parsed.scheme in {"http", "https"}
        and parsed.hostname
        and host.hostname
        and parsed.hostname.lower() == host.hostname.lower()
        and origin_port == host_port
    )


def _browser_session_value(
    api_token: str,
    *,
    expires_at: int | None = None,
) -> str:
    expires_at = expires_at or (
        int(time.time()) + SESSION_MAX_AGE_SECONDS
    )
    expires = str(expires_at)
    signature = hmac.new(
        api_token.encode("utf-8"),
        b"easybackup-browser-session-v1\0" + expires.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return f"{expires}.{signature}"


def _valid_browser_session(value: str, api_token: str) -> bool:
    try:
        expires_text, signature = value.split(".", 1)
        expires_at = int(expires_text)
    except (ValueError, TypeError):
        return False
    if expires_at < int(time.time()):
        return False
    expected = _browser_session_value(
        api_token, expires_at=expires_at
    ).split(".", 1)[1]
    return secrets.compare_digest(signature, expected)


def _is_authenticated(
    headers: Any,
    cookies: Any,
    api_token: str | None,
) -> bool:
    if not api_token:
        return True
    authorization = headers.get("authorization", "")
    supplied = headers.get("x-easybackup-token")
    if authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    if supplied and secrets.compare_digest(supplied, api_token):
        return True
    browser_session = cookies.get(SESSION_COOKIE, "")
    return bool(
        browser_session
        and _valid_browser_session(browser_session, api_token)
    )


def _header_token_authenticated(
    headers: Any, api_token: str | None
) -> bool:
    if not api_token:
        return False
    authorization = headers.get("authorization", "")
    supplied = headers.get("x-easybackup-token")
    if authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    return bool(
        supplied and secrets.compare_digest(supplied, api_token)
    )


class HostValidationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, allowed_hosts: set[str]):
        super().__init__(app)
        self.allowed_hosts = allowed_hosts

    async def dispatch(self, request: Request, call_next):
        if not _host_allowed(
            request.headers.get("host", ""), self.allowed_hosts
        ):
            return _problem(
                status_code=400,
                title="无效主机",
                detail="Host 请求头不在 EasyBackup 允许列表中。",
                code="INVALID_HOST",
            )
        return await call_next(request)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        if request.url.path == "/api/docs":
            # FastAPI's generated Swagger UI loads versioned assets from
            # jsDelivr and contains a small inline bootstrap script.
            policy = (
                "default-src 'self'; "
                "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                "img-src 'self' data: https://fastapi.tiangolo.com; "
                "connect-src 'self'; "
                "frame-ancestors 'none'; "
                "base-uri 'none'; "
                "form-action 'self'"
            )
        else:
            policy = (
                "default-src 'self'; "
                "script-src 'self'; "
                "style-src 'self' 'unsafe-inline'; "
                "img-src 'self' data:; "
                "connect-src 'self' ws: wss:; "
                "frame-ancestors 'none'; "
                "base-uri 'none'; "
                "form-action 'self'"
            )
        response.headers.setdefault("Content-Security-Policy", policy)
        return response


class ApiTokenMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, token: str | None):
        super().__init__(app)
        self.token = token

    async def dispatch(self, request: Request, call_next):
        if (
            not request.url.path.startswith(API_PREFIX)
            or request.url.path
            in {
                f"{API_PREFIX}/health",
                f"{API_PREFIX}/session",
            }
        ):
            return await call_next(request)
        header_authenticated = _header_token_authenticated(
            request.headers, self.token
        )
        if not _is_authenticated(
            request.headers, request.cookies, self.token
        ):
            return _problem(
                status_code=401,
                title="未授权",
                detail="需要有效的 EasyBackup API Token。",
                code="UNAUTHORIZED",
            )
        if request.method.upper() not in SAFE_HTTP_METHODS:
            origin = request.headers.get("origin", "")
            if origin and not _origin_matches_host(
                origin, request.headers.get("host", "")
            ):
                return _problem(
                    status_code=403,
                    title="来源被拒绝",
                    detail="跨来源的状态变更请求已被拒绝。",
                    code="ORIGIN_REJECTED",
                )
            if self.token and not header_authenticated and not origin:
                return _problem(
                    status_code=403,
                    title="来源验证失败",
                    detail=(
                        "Cookie 会话的状态变更请求必须携带同源 Origin。"
                    ),
                    code="ORIGIN_REQUIRED",
                )
        return await call_next(request)


def _problem(
    *,
    status_code: int,
    title: str,
    detail: str,
    code: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    payload: dict[str, Any] = {
        "type": f"https://easybackup.local/problems/{code.lower()}",
        "title": title,
        "status": status_code,
        "detail": detail,
        "code": code,
    }
    if details:
        payload["details"] = details
        if "field_errors" in details:
            payload["field_errors"] = details["field_errors"]
    return JSONResponse(
        payload,
        status_code=status_code,
        media_type="application/problem+json",
    )


def _paths_overlap(first: Path, second: Path) -> bool:
    try:
        first.relative_to(second)
        return True
    except ValueError:
        pass
    try:
        second.relative_to(first)
        return True
    except ValueError:
        return False


def _validate_task(
    task: TaskCreate,
    scheduler: SchedulerService,
    settings: Settings,
) -> TaskCreate:
    source = Path(task.source_path).expanduser().resolve()
    if not source.exists() or not source.is_dir():
        raise ValidationError(f"源目录不存在或不是目录：{source}")
    if _paths_overlap(source, settings.data_dir.resolve()):
        raise ValidationError(
            "源目录不能与 EasyBackup 数据目录重叠，以免备份数据库、日志或凭据。"
        )
    scheduler.validate_cron(task.schedule)
    storage = task.storage
    if isinstance(task.storage, LocalStorageConfig):
        destination = Path(task.storage.path).expanduser().resolve()
        if _paths_overlap(source, destination):
            raise ValidationError("源目录与本地备份目标不能相互包含。")
        storage = task.storage.model_copy(
            update={"path": str(destination)}
        )
    return task.model_copy(
        update={
            "source_path": str(source),
            "storage": storage,
        }
    )


def _prospective_task(current: Task, update: TaskUpdate) -> TaskCreate:
    value = current.model_dump(
        mode="json", exclude={"id", "created_at", "updated_at"}
    )
    value.update(update.model_dump(mode="json", exclude_unset=True))
    return TaskCreate.model_validate(value)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    settings.validate_network_binding()
    allowed_hosts = _allowed_hosts(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings.prepare()
        configure_logging(settings.log_level, settings.log_dir)
        database = Database(settings.database_path)
        database.initialize()
        database.mark_interrupted_operations()
        credentials = CredentialStore(
            settings.secret_dir, settings.credential_backend
        )
        reconcile_incomplete_snapshots(
            database, credentials, settings.lock_dir
        )
        events = EventBus()
        manager = OperationManager(
            database,
            BackupEngine(
                database,
                credentials,
                settings.lock_dir,
                settings.integrity_block_size,
                settings.xdelta3_path,
            ),
            RestoreEngine(
                credentials,
                settings.lock_dir,
                settings.xdelta3_path,
            ),
            MaintenanceEngine(database, credentials, settings.lock_dir),
            events,
        )
        scheduler = SchedulerService(
            database,
            manager,
            settings.timezone,
            settings.scrub_schedule,
        )
        app.state.settings = settings
        app.state.database = database
        app.state.credentials = credentials
        app.state.events = events
        app.state.manager = manager
        app.state.scheduler = scheduler
        scheduler.start()

        async def periodic_reconciliation() -> None:
            while True:
                await asyncio.sleep(settings.reconcile_interval_seconds)
                try:
                    work = asyncio.create_task(
                        asyncio.to_thread(
                            reconcile_incomplete_snapshots,
                            database,
                            credentials,
                            settings.lock_dir,
                        )
                    )
                    try:
                        await asyncio.shield(work)
                    except asyncio.CancelledError:
                        # to_thread cannot stop an already-running sync call.
                        # Let it release its task/remote leases before the
                        # database and credential services are closed.
                        with suppress(Exception):
                            await work
                        raise
                except Exception:
                    logger.exception("周期性启动对账失败，将在下一周期重试")

        reconciliation_task = asyncio.create_task(
            periodic_reconciliation(),
            name="easybackup-periodic-reconciliation",
        )
        try:
            yield
        finally:
            manager.stop_accepting()
            scheduler.shutdown()
            reconciliation_task.cancel()
            with suppress(asyncio.CancelledError):
                await reconciliation_task
            await manager.shutdown(settings.shutdown_timeout_seconds)
            database.close()

    app = FastAPI(
        title="EasyBackup",
        version=__version__,
        description="本地优先、分卷流式、支持 S3 的增量备份控制中心。",
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
    )
    app.add_middleware(ApiTokenMiddleware, token=settings.api_token)
    app.add_middleware(
        HostValidationMiddleware, allowed_hosts=allowed_hosts
    )
    app.add_middleware(SecurityHeadersMiddleware)

    @app.exception_handler(EasyBackupError)
    async def domain_error_handler(
        request: Request, exc: EasyBackupError
    ) -> JSONResponse:
        del request
        return _problem(
            status_code=exc.status_code,
            title="请求无法完成",
            detail=exc.message,
            code=exc.code,
            details=exc.details,
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        del request
        fields = [
            {
                "path": ".".join(str(part) for part in error["loc"][1:]),
                "message": error["msg"],
            }
            for error in exc.errors()
        ]
        return _problem(
            status_code=422,
            title="输入校验失败",
            detail="请求中的一个或多个字段无效。",
            code="VALIDATION_ERROR",
            details={"field_errors": fields},
        )

    @app.get("/health", include_in_schema=False)
    @app.get(f"{API_PREFIX}/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "authentication_required": bool(settings.api_token),
        }

    @app.get(f"{API_PREFIX}/session", include_in_schema=False)
    async def session_status(request: Request) -> dict[str, bool]:
        required = bool(settings.api_token)
        return {
            "required": required,
            "authenticated": _is_authenticated(
                request.headers,
                request.cookies,
                settings.api_token,
            ),
        }

    @app.post(f"{API_PREFIX}/session", include_in_schema=False)
    async def create_browser_session(
        payload: ApiSessionRequest,
        request: Request,
        response: Response,
    ) -> Any:
        if settings.api_token and not secrets.compare_digest(
            payload.token, settings.api_token
        ):
            return _problem(
                status_code=401,
                title="未授权",
                detail="API Token 不正确。",
                code="UNAUTHORIZED",
            )
        if settings.api_token:
            response.set_cookie(
                key=SESSION_COOKIE,
                value=_browser_session_value(settings.api_token),
                max_age=SESSION_MAX_AGE_SECONDS,
                httponly=True,
                secure=request.url.scheme == "https",
                samesite="strict",
                path="/",
            )
        return {"required": bool(settings.api_token), "authenticated": True}

    @app.get(f"{API_PREFIX}/system/status")
    async def system_status(request: Request) -> dict[str, Any]:
        database: Database = request.app.state.database
        manager: OperationManager = request.app.state.manager
        scheduler: SchedulerService = request.app.state.scheduler
        credentials: CredentialStore = request.app.state.credentials
        return {
            "status": "ok",
            "version": __version__,
            "timezone": settings.timezone,
            "data_dir": str(settings.data_dir),
            "database": {
                "path": str(settings.database_path),
                "healthy": True,
            },
            "scheduler": {
                "running": scheduler.scheduler.running,
                "scrub_schedule": settings.scrub_schedule,
            },
            "active_operations": manager.active_count,
            "task_count": len(database.list_tasks()),
            "snapshot_count": len(database.list_snapshots(limit=10000)),
            "tools": tool_capabilities(settings.xdelta3_path),
            "credential_backend": credentials.backend_status(),
            "credentials": credentials.backend_status(),
        }

    @app.get(f"{API_PREFIX}/tasks")
    async def list_tasks(request: Request) -> list[dict[str, Any]]:
        database: Database = request.app.state.database
        scheduler: SchedulerService = request.app.state.scheduler
        return [
            {
                **task.model_dump(mode="json"),
                "next_run_at": scheduler.next_run(task.id),
            }
            for task in database.list_tasks()
        ]

    @app.post(f"{API_PREFIX}/storage/test")
    async def test_storage_configuration(
        payload: StorageProbeRequest,
        request: Request,
    ) -> dict[str, Any]:
        """Verify read/write/delete access without retaining probe data."""

        probe_key = (
            "v1/system/probes/"
            f"{secrets.token_hex(16)}.probe"
        )
        probe_payload = (
            b"easybackup-storage-configuration-probe-v1\n"
        )
        store = None
        uploaded = False
        started = time.monotonic()
        try:
            try:
                store = create_store(
                    payload.storage,
                    request.app.state.credentials,
                )
            except (CredentialError, NotFoundError) as exc:
                storage = payload.storage
                if not isinstance(storage, S3StorageConfig):
                    raise
                diagnostic = {
                    "kind": "credential_profile",
                    "title": (
                        "凭据配置不存在"
                        if isinstance(exc, NotFoundError)
                        else "无法读取凭据配置"
                    ),
                    "summary": exc.message,
                    "suggestions": [
                        "在“存储与密钥”中创建或更新同名凭据配置。",
                        "凭据配置名称区分大小写；阿里云 OSS 请保存 RAM AccessKey。",
                    ],
                    "operation": "读取凭据配置",
                    "provider": (
                        "aliyun_oss"
                        if "aliyuncs.com" in (storage.endpoint_url or "")
                        else "s3_compatible"
                    ),
                    "endpoint": storage.endpoint_url or "AWS 默认 Endpoint",
                    "bucket": storage.bucket,
                    "region": storage.region,
                }
                raise exc.__class__(
                    exc.message,
                    details={"diagnostic": diagnostic},
                ) from exc

            stored = await asyncio.to_thread(
                store.put_bytes,
                probe_key,
                probe_payload,
                metadata={"easybackup-artifact": "configuration-probe"},
            )
            uploaded = True
            stat_value = await asyncio.to_thread(store.stat, probe_key)
            downloaded = await asyncio.to_thread(
                store.read_bytes,
                probe_key,
            )
            if (
                stored.size != len(probe_payload)
                or stat_value is None
                or stat_value.size != len(probe_payload)
                or not hmac.compare_digest(
                    downloaded,
                    probe_payload,
                )
            ):
                raise StorageError(
                    "存储探针读回内容与写入内容不一致。"
                )
        finally:
            if store is not None and uploaded:
                await asyncio.to_thread(store.delete, probe_key)

        storage = payload.storage
        target = (
            str(Path(storage.path).expanduser().resolve())
            if isinstance(storage, LocalStorageConfig)
            else (
                f"s3://{storage.bucket}/"
                f"{storage.prefix.strip('/')}"
            ).rstrip("/")
        )
        return {
            "ok": True,
            "kind": storage.kind,
            "target": target,
            "connection": (
                {
                    "provider": getattr(store, "provider", "s3_compatible"),
                    "endpoint": storage.endpoint_url or "AWS 默认 Endpoint",
                    "region": storage.region,
                    "addressing_style": getattr(
                        store, "addressing_style", "auto"
                    ),
                    "signature_version": getattr(
                        store, "signature_version", "default"
                    ),
                }
                if isinstance(storage, S3StorageConfig)
                else None
            ),
            "latency_ms": max(
                1,
                round((time.monotonic() - started) * 1000),
            ),
            "message": "写入、读取与删除测试均已通过。",
        }

    @app.post(
        f"{API_PREFIX}/tasks",
        status_code=status.HTTP_201_CREATED,
    )
    async def create_task(
        payload: TaskCreate, request: Request
    ) -> dict[str, Any]:
        database: Database = request.app.state.database
        scheduler: SchedulerService = request.app.state.scheduler
        payload = _validate_task(payload, scheduler, settings)
        task = database.create_task(payload)
        scheduler.refresh_task(task)
        request.app.state.events.publish(
            "task.created", {"task": task.model_dump(mode="json")}
        )
        return {
            **task.model_dump(mode="json"),
            "next_run_at": scheduler.next_run(task.id),
        }

    @app.get(f"{API_PREFIX}/tasks/{{task_id}}")
    async def get_task(task_id: str, request: Request) -> dict[str, Any]:
        task = request.app.state.database.get_task(task_id)
        return {
            **task.model_dump(mode="json"),
            "next_run_at": request.app.state.scheduler.next_run(task.id),
        }

    @app.put(f"{API_PREFIX}/tasks/{{task_id}}")
    async def update_task(
        task_id: str, payload: TaskUpdate, request: Request
    ) -> dict[str, Any]:
        manager: OperationManager = request.app.state.manager
        database: Database = request.app.state.database
        scheduler: SchedulerService = request.app.state.scheduler
        if manager.is_task_busy(task_id):
            raise ConflictError("任务运行期间不能修改配置。")
        if database.has_running_snapshot(task_id):
            raise ConflictError(
                "任务存在尚未完成启动对账的快照，暂不能修改配置。"
            )
        current = database.get_task(task_id)
        prospective = _prospective_task(current, payload)
        prospective = _validate_task(prospective, scheduler, settings)
        normalized_update = TaskUpdate.model_validate(
            prospective.model_dump(mode="python")
        )
        task = database.update_task(task_id, normalized_update)
        scheduler.refresh_task(task)
        request.app.state.events.publish(
            "task.updated", {"task": task.model_dump(mode="json")}
        )
        return {
            **task.model_dump(mode="json"),
            "next_run_at": scheduler.next_run(task.id),
        }

    @app.delete(
        f"{API_PREFIX}/tasks/{{task_id}}",
        status_code=status.HTTP_204_NO_CONTENT,
    )
    async def delete_task(task_id: str, request: Request) -> Response:
        manager: OperationManager = request.app.state.manager
        if manager.is_task_busy(task_id):
            raise ConflictError("任务运行期间不能删除配置。")
        request.app.state.scheduler.remove_task(task_id)
        try:
            with TaskLock(settings.lock_dir, task_id):
                request.app.state.database.delete_task(task_id)
        except Exception:
            # Keep the schedule intact if deletion is rejected or interrupted.
            task = request.app.state.database.get_task(task_id)
            request.app.state.scheduler.refresh_task(task)
            raise
        request.app.state.events.publish(
            "task.deleted", {"task_id": task_id}
        )
        return Response(status_code=204)

    @app.post(
        f"{API_PREFIX}/tasks/{{task_id}}/run",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def run_task(
        task_id: str, payload: RunRequest, request: Request
    ) -> dict[str, Any]:
        operation = await request.app.state.manager.start_backup(
            task_id, force_full=payload.force_full
        )
        return operation.model_dump(mode="json")

    @app.post(
        f"{API_PREFIX}/tasks/{{task_id}}/prune",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def prune_task(task_id: str, request: Request) -> dict[str, Any]:
        operation = await request.app.state.manager.start_prune(task_id)
        return operation.model_dump(mode="json")

    @app.post(f"{API_PREFIX}/tasks/{{task_id}}/multipart-cleanup")
    async def cleanup_multipart(
        task_id: str, request: Request
    ) -> dict[str, Any]:
        task = request.app.state.database.get_task(task_id)
        store = create_store(task.storage, request.app.state.credentials)
        removed = await asyncio.to_thread(
            store.abort_stale_multipart_uploads, 7
        )
        return {"task_id": task_id, "removed_uploads": removed, "older_than_days": 7}

    @app.get(f"{API_PREFIX}/operations")
    async def list_operations(
        request: Request,
        task_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        limit = min(max(limit, 1), 1000)
        return [
            item.model_dump(mode="json")
            for item in request.app.state.database.list_operations(
                task_id, limit
            )
        ]

    @app.get(f"{API_PREFIX}/operations/{{operation_id}}")
    async def get_operation(
        operation_id: str, request: Request
    ) -> dict[str, Any]:
        return request.app.state.database.get_operation(
            operation_id
        ).model_dump(mode="json")

    @app.post(f"{API_PREFIX}/operations/{{operation_id}}/cancel")
    async def cancel_operation(
        operation_id: str, request: Request
    ) -> dict[str, Any]:
        return request.app.state.manager.cancel(operation_id).model_dump(
            mode="json"
        )

    @app.get(f"{API_PREFIX}/snapshots")
    async def list_snapshots(
        request: Request,
        task_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        limit = min(max(limit, 1), 1000)
        return [
            item.model_dump(mode="json")
            for item in request.app.state.database.list_snapshots(
                task_id, limit
            )
        ]

    @app.get(f"{API_PREFIX}/snapshots/{{snapshot_id}}")
    async def get_snapshot(
        snapshot_id: str, request: Request
    ) -> dict[str, Any]:
        return request.app.state.database.get_snapshot(snapshot_id).model_dump(
            mode="json"
        )

    @app.get(f"{API_PREFIX}/snapshots/{{snapshot_id}}/manifest")
    async def get_snapshot_manifest(
        snapshot_id: str, request: Request
    ) -> dict[str, Any]:
        snapshot = request.app.state.database.get_snapshot(snapshot_id)
        if not snapshot.manifest_key:
            raise ConflictError("快照尚未生成 Manifest。")
        store = create_store(snapshot.storage, request.app.state.credentials)
        manifest, payload = load_manifest(store, snapshot.manifest_key)
        verify_commit_marker(
            store, snapshot.manifest_key, payload, snapshot.id
        )
        return manifest.model_dump(mode="json")

    @app.post(
        f"{API_PREFIX}/restores",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def start_restore(
        payload: RestoreRequest, request: Request
    ) -> dict[str, Any]:
        operation = await request.app.state.manager.start_restore(payload)
        return operation.model_dump(mode="json")

    @app.post(
        f"{API_PREFIX}/snapshots/{{snapshot_id}}/scrub",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def scrub_snapshot(
        snapshot_id: str,
        payload: ScrubRequest,
        request: Request,
    ) -> dict[str, Any]:
        operation = await request.app.state.manager.start_scrub(
            snapshot_id, payload
        )
        return operation.model_dump(mode="json")

    @app.get(f"{API_PREFIX}/credentials")
    async def list_credentials(request: Request) -> list[dict[str, Any]]:
        return [
            item.model_dump(mode="json")
            for item in request.app.state.credentials.list()
        ]

    @app.post(
        f"{API_PREFIX}/credentials",
        status_code=status.HTTP_201_CREATED,
    )
    async def put_credential(
        payload: CredentialWrite, request: Request
    ) -> dict[str, Any]:
        value = request.app.state.credentials.put(payload)
        request.app.state.events.publish(
            "credential.updated", {"profile": value.profile}
        )
        return value.model_dump(mode="json")

    @app.delete(
        f"{API_PREFIX}/credentials/{{profile}}",
        status_code=status.HTTP_204_NO_CONTENT,
    )
    async def delete_credential(
        profile: str, request: Request
    ) -> Response:
        database: Database = request.app.state.database
        for task in database.list_tasks():
            if (
                isinstance(task.storage, S3StorageConfig)
                and task.storage.credential_profile == profile
            ):
                raise ConflictError(
                    f"凭据仍被任务 {task.name!r} 使用，不能删除。"
                )
        snapshot_id = database.snapshot_using_credential_profile(profile)
        if snapshot_id:
            raise ConflictError(
                f"凭据仍被历史快照 {snapshot_id!r} 使用，不能删除。"
            )
        request.app.state.credentials.delete(profile)
        return Response(status_code=204)

    @app.websocket(f"{API_PREFIX}/ws")
    async def websocket_events(websocket: WebSocket) -> None:
        host = websocket.headers.get("host", "")
        if not _host_allowed(host, allowed_hosts):
            await websocket.close(code=4403)
            return
        origin = websocket.headers.get("origin")
        if origin and not _origin_matches_host(origin, host):
            await websocket.close(code=4403)
            return
        if (
            settings.api_token
            and not _header_token_authenticated(
                websocket.headers, settings.api_token
            )
            and not origin
        ):
            # Cookie-authenticated browser sessions must prove same origin.
            # Non-browser clients should use an Authorization header.
            await websocket.close(code=4403)
            return
        if not _is_authenticated(
            websocket.headers,
            websocket.cookies,
            settings.api_token,
        ):
            await websocket.close(code=4401)
            return
        await websocket.accept()
        queue = websocket.app.state.events.subscribe()
        disconnect_task: asyncio.Task[None] | None = None

        async def wait_for_disconnect() -> None:
            try:
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        return
            except (WebSocketDisconnect, RuntimeError, OSError):
                return

        try:
            await websocket.send_json(
                {
                    "type": "hello",
                    "timestamp": utc_now_iso(),
                    "data": {
                        "protocol": 1,
                        "heartbeat_seconds": 20,
                    },
                }
            )
            disconnect_task = asyncio.create_task(wait_for_disconnect())
            while True:
                event_task = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait(
                    {event_task, disconnect_task},
                    timeout=20,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if disconnect_task in done:
                    event_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await event_task
                    break
                if event_task in done:
                    event = event_task.result()
                else:
                    event_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await event_task
                    event = {
                        "type": "heartbeat",
                        "data": {"status": "ok"},
                    }
                await websocket.send_json(event)
        except (WebSocketDisconnect, RuntimeError, OSError):
            pass
        finally:
            if disconnect_task is not None:
                disconnect_task.cancel()
                with suppress(
                    asyncio.CancelledError,
                    WebSocketDisconnect,
                    RuntimeError,
                    OSError,
                ):
                    await disconnect_task
            websocket.app.state.events.unsubscribe(queue)

    static_dir = Path(__file__).with_name("static")
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(
            static_dir / "index.html",
            headers={"Cache-Control": "no-store"},
        )

    return app


def create_default_app() -> FastAPI:
    return create_app()
