"""Command-line entry point for the EasyBackup daemon and diagnostics."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import webbrowser
from dataclasses import replace

from easybackup import __version__
from easybackup.app import create_app
from easybackup.archive import tool_capabilities
from easybackup.config import Settings
from easybackup.db import Database
from easybackup.security import CredentialStore


def _configure_console_encoding() -> None:
    """Keep Chinese diagnostics usable on legacy Windows code pages."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            # Embedded hosts may expose a stream that cannot be reconfigured.
            pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easybackup",
        description="EasyBackup 本地备份控制中心",
    )
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command")

    serve = commands.add_parser("serve", help="启动 Web 控制中心")
    serve.add_argument("--host", help="监听地址（默认读取环境变量）")
    serve.add_argument("--port", type=int, help="监听端口")
    serve.add_argument(
        "--no-browser", action="store_true", help="启动后不自动打开浏览器"
    )

    commands.add_parser("init", help="初始化数据目录和 SQLite")
    commands.add_parser("doctor", help="检查运行环境和外部工具")
    commands.add_parser("tray", help="启动系统托盘模式")
    return parser


def _settings_from_args(args: argparse.Namespace) -> Settings:
    settings = Settings.from_env()
    if getattr(args, "host", None):
        settings = replace(settings, host=args.host)
    if getattr(args, "port", None):
        settings = replace(settings, port=args.port)
    if getattr(args, "no_browser", False):
        settings = replace(settings, open_browser=False)
    settings.validate_network_binding()
    return settings


def _initialize(settings: Settings) -> None:
    settings.prepare()
    database = Database(settings.database_path)
    database.initialize()
    database.close()


def _doctor(settings: Settings) -> int:
    report: dict = {
        "version": __version__,
        "python": sys.version,
        "executable": sys.executable,
        "platform": sys.platform,
        "data_dir": str(settings.data_dir),
        "database": {"ok": False, "path": str(settings.database_path)},
        "tools": tool_capabilities(settings.xdelta3_path),
        "credentials": {},
    }
    ok = True
    try:
        _initialize(settings)
        report["database"]["ok"] = True
    except Exception as exc:
        report["database"]["error"] = str(exc)
        ok = False
    try:
        report["credentials"] = CredentialStore(
            settings.secret_dir, settings.credential_backend
        ).backend_status()
    except Exception as exc:
        report["credentials"] = {"error": str(exc)}
        ok = False
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if ok else 1


def _serve(settings: Settings) -> int:
    try:
        import uvicorn
    except ImportError:
        print("缺少 uvicorn，请先安装项目依赖。", file=sys.stderr)
        return 2
    if settings.open_browser:
        url_host = (
            "127.0.0.1"
            if settings.host in {"0.0.0.0", "::"}
            else settings.host
        )
        threading.Timer(
            1.2,
            lambda: webbrowser.open(
                f"http://{url_host}:{settings.port}/"
            ),
        ).start()
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )
    return 0


def main(argv: list[str] | None = None) -> None:
    _configure_console_encoding()
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command is None:
        args.command = "serve"
        args.host = None
        args.port = None
        args.no_browser = False
    settings = _settings_from_args(args)
    if args.command == "serve":
        code = _serve(settings)
    elif args.command == "init":
        _initialize(settings)
        print(f"EasyBackup 已初始化：{settings.data_dir}")
        code = 0
    elif args.command == "doctor":
        code = _doctor(settings)
    elif args.command == "tray":
        from easybackup.tray import main as tray_main

        tray_main()
        code = 0
    else:
        parser.error(f"未知命令：{args.command}")
        return
    raise SystemExit(code)
