"""Application logging with rotation and credential redaction."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from easybackup.security import RedactingFilter


def configure_logging(level: str, log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    redactor = RedactingFilter()
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    stream.addFilter(redactor)
    file_handler = RotatingFileHandler(
        log_dir / "easybackup.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(redactor)
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not getattr(root, "_easybackup_configured", False):
        root.handlers.clear()
        root.addHandler(stream)
        root.addHandler(file_handler)
        root._easybackup_configured = True  # type: ignore[attr-defined]

