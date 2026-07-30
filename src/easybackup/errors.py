"""Domain-specific errors shared by the API and service layers."""

from __future__ import annotations

from typing import Any


class EasyBackupError(Exception):
    """Base class for expected, user-facing errors."""

    code = "EASYBACKUP_ERROR"
    status_code = 400

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFoundError(EasyBackupError):
    code = "NOT_FOUND"
    status_code = 404


class ConflictError(EasyBackupError):
    code = "CONFLICT"
    status_code = 409


class ValidationError(EasyBackupError):
    code = "VALIDATION_ERROR"
    status_code = 422


class ToolMissingError(EasyBackupError):
    code = "TOOL_MISSING"
    status_code = 503


class StorageError(EasyBackupError):
    code = "STORAGE_ERROR"
    status_code = 503


class CredentialError(EasyBackupError):
    code = "CREDENTIAL_ERROR"
    status_code = 503


class CancelledError(EasyBackupError):
    code = "OPERATION_CANCELLED"
    status_code = 409

