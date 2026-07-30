"""Application configuration loaded from environment variables."""

from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path

from easybackup.errors import ValidationError


def _default_data_dir() -> Path:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA")
        if base:
            return Path(base) / "EasyBackup"
    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg:
        return Path(xdg) / "easybackup"
    return Path.home() / ".easybackup"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 8765
    timezone: str = "Asia/Shanghai"
    scrub_schedule: str | None = "0 3 * * sun"
    allowed_hosts: tuple[str, ...] = ()
    log_level: str = "INFO"
    api_token: str | None = None
    credential_backend: str = "auto"
    open_browser: bool = True
    shutdown_timeout_seconds: int = 30
    reconcile_interval_seconds: int = 60
    integrity_block_size: int = 8 * 1024 * 1024
    xdelta3_path: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir_raw = os.environ.get("EASYBACKUP_DATA_DIR")
        data_dir = (
            Path(data_dir_raw).expanduser()
            if data_dir_raw
            else _default_data_dir()
        )
        scrub_schedule = os.environ.get(
            "EASYBACKUP_SCRUB_SCHEDULE", "0 3 * * sun"
        ).strip()
        allowed_hosts = tuple(
            value.strip()
            for value in os.environ.get(
                "EASYBACKUP_ALLOWED_HOSTS", ""
            ).split(",")
            if value.strip()
        )
        return cls(
            data_dir=data_dir.resolve(),
            host=os.environ.get("EASYBACKUP_HOST", "127.0.0.1"),
            port=int(os.environ.get("EASYBACKUP_PORT", "8765")),
            timezone=os.environ.get("EASYBACKUP_TIMEZONE", "Asia/Shanghai"),
            scrub_schedule=scrub_schedule or None,
            allowed_hosts=allowed_hosts,
            log_level=os.environ.get("EASYBACKUP_LOG_LEVEL", "INFO").upper(),
            api_token=os.environ.get("EASYBACKUP_API_TOKEN") or None,
            credential_backend=os.environ.get(
                "EASYBACKUP_CREDENTIAL_BACKEND", "auto"
            ).lower(),
            open_browser=_env_bool("EASYBACKUP_OPEN_BROWSER", True),
            shutdown_timeout_seconds=int(
                os.environ.get("EASYBACKUP_SHUTDOWN_TIMEOUT", "30")
            ),
            reconcile_interval_seconds=int(
                os.environ.get("EASYBACKUP_RECONCILE_INTERVAL", "60")
            ),
            integrity_block_size=int(
                os.environ.get("EASYBACKUP_INTEGRITY_BLOCK_SIZE", str(8 * 1024 * 1024))
            ),
            xdelta3_path=(
                (
                    os.environ.get("EASYBACKUP_XDELTA3_PATH")
                    or os.environ.get("EASYBACKUP_XDELTA3")
                    or ""
                ).strip()
                or None
            ),
        )

    @property
    def database_path(self) -> Path:
        return self.data_dir / "easybackup.db"

    @property
    def lock_dir(self) -> Path:
        return self.data_dir / "locks"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def secret_dir(self) -> Path:
        return self.data_dir / "secrets"

    def prepare(self) -> None:
        for path in (self.data_dir, self.lock_dir, self.log_dir, self.secret_dir):
            path.mkdir(parents=True, exist_ok=True)

    def validate_network_binding(self) -> None:
        try:
            address = ipaddress.ip_address(self.host)
            is_loopback = address.is_loopback
        except ValueError:
            is_loopback = self.host.lower() in {"localhost", "ip6-localhost"}
        if not is_loopback and not self.api_token:
            raise ValidationError(
                "绑定到非本机地址时必须设置 EASYBACKUP_API_TOKEN。"
            )
        if not 1 <= self.port <= 65535:
            raise ValidationError("端口必须位于 1 到 65535 之间。")
        if self.reconcile_interval_seconds < 1:
            raise ValidationError(
                "EASYBACKUP_RECONCILE_INTERVAL 必须至少为 1 秒。"
            )
        if self.credential_backend not in {"auto", "keyring", "encrypted_file"}:
            raise ValidationError(
                "EASYBACKUP_CREDENTIAL_BACKEND 必须是 auto、keyring 或 encrypted_file。"
            )
