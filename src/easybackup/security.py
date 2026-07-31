"""Credential storage and log redaction.

The operating-system keyring is preferred.  A machine-bound AES-256-GCM file is
provided for unattended installations where no usable keyring exists.  The
fallback protects credentials at rest from casual disclosure, but its key is
derived from machine identity and is therefore not a substitute for a hardware
security module or a user-supplied master secret.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import platform
import re
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from easybackup.errors import ConflictError, CredentialError, NotFoundError
from easybackup.models import CredentialStatus, CredentialWrite


SERVICE_NAME = "EasyBackup"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _machine_identifier() -> str:
    if os.name == "nt":
        try:
            import winreg

            access = winreg.KEY_READ
            if hasattr(winreg, "KEY_WOW64_64KEY"):
                access |= winreg.KEY_WOW64_64KEY
            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Cryptography",
                0,
                access,
            ) as key:
                value, _ = winreg.QueryValueEx(key, "MachineGuid")
                if value:
                    return str(value)
        except OSError:
            pass
    for candidate in (Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")):
        try:
            value = candidate.read_text(encoding="utf-8").strip()
            if value:
                return value
        except OSError:
            continue
    return f"{platform.node()}:{platform.machine()}:{platform.system()}"


def _atomic_json_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.part")
    payload = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


class CredentialStore:
    """Store remote-storage credentials without returning secrets through APIs."""

    def __init__(self, secret_dir: Path, preference: str = "auto"):
        self.secret_dir = secret_dir
        self.preference = preference
        self._index_path = secret_dir / "credentials-index.json"
        self._encrypted_path = secret_dir / "credentials.enc.json"
        self._lock = threading.RLock()
        self.secret_dir.mkdir(parents=True, exist_ok=True)

    def _load_json(self, path: Path) -> dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
            return value if isinstance(value, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, json.JSONDecodeError) as exc:
            raise CredentialError(f"无法读取凭据元数据：{exc}") from exc

    def _load_index(self) -> dict[str, Any]:
        value = self._load_json(self._index_path)
        value.setdefault("version", 1)
        value.setdefault("profiles", {})
        return value

    def _keyring_module(self):
        try:
            import keyring

            backend = keyring.get_keyring()
            if getattr(backend, "priority", 0) <= 0:
                raise RuntimeError("没有可用的系统 keyring 后端")
            return keyring
        except Exception as exc:
            raise CredentialError(f"系统 keyring 不可用：{exc}") from exc

    @staticmethod
    def _encryption_key() -> bytes:
        machine_id = _machine_identifier().encode("utf-8", errors="replace")
        return hashlib.sha256(b"EasyBackup-machine-bound-v1\0" + machine_id).digest()

    def _encrypted_entries(self) -> dict[str, str]:
        value = self._load_json(self._encrypted_path)
        if value and value.get("version") != 1:
            raise CredentialError("凭据文件版本不受支持。")
        return dict(value.get("entries", {}))

    def _write_encrypted_entries(self, entries: dict[str, str]) -> None:
        _atomic_json_write(
            self._encrypted_path,
            {"version": 1, "entries": entries},
        )

    def _encrypt(self, profile: str, payload: str) -> str:
        nonce = os.urandom(12)
        cipher = AESGCM(self._encryption_key())
        encrypted = cipher.encrypt(nonce, payload.encode("utf-8"), profile.encode())
        return base64.urlsafe_b64encode(nonce + encrypted).decode("ascii")

    def _decrypt(self, profile: str, encoded: str) -> str:
        try:
            raw = base64.urlsafe_b64decode(encoded.encode("ascii"))
            nonce, ciphertext = raw[:12], raw[12:]
            return AESGCM(self._encryption_key()).decrypt(
                nonce, ciphertext, profile.encode()
            ).decode("utf-8")
        except Exception as exc:
            raise CredentialError(
                "无法解密凭据；机器标识可能已改变或凭据文件已损坏。"
            ) from exc

    @staticmethod
    def _serialize(value: CredentialWrite) -> str:
        return json.dumps(
            value.model_dump(
                mode="json",
                exclude={"profile"},
                exclude_none=True,
            ),
            ensure_ascii=False,
        )

    def put(self, value: CredentialWrite) -> CredentialStatus:
        with self._lock:
            index = self._load_index()
            existing = index["profiles"].get(value.profile)
            existing_kind = existing.get("kind", "s3") if existing else None
            if existing_kind is not None and existing_kind != value.kind:
                raise ConflictError(
                    f"凭据配置 {value.profile!r} 已用于 {existing_kind.upper()}；"
                    "为避免任务误用，不能直接改成另一种协议，请新建配置名称。"
                )

            backend = "encrypted_file"
            payload = self._serialize(value)
            if self.preference in {"auto", "keyring"}:
                try:
                    self._keyring_module().set_password(
                        SERVICE_NAME, value.profile, payload
                    )
                    backend = "keyring"
                except Exception as exc:
                    if self.preference == "keyring":
                        if isinstance(exc, CredentialError):
                            raise
                        raise CredentialError(f"写入系统 keyring 失败：{exc}") from exc
            if backend == "encrypted_file":
                entries = self._encrypted_entries()
                entries[value.profile] = self._encrypt(value.profile, payload)
                self._write_encrypted_entries(entries)

            now = _utc_now()
            metadata: dict[str, Any] = {
                "backend": backend,
                "kind": value.kind,
                "updated_at": now,
            }
            if value.kind == "s3":
                metadata.update(
                    {
                        "access_key_hint": _credential_hint(
                            value.access_key_id or ""
                        ),
                        "has_session_token": bool(value.session_token),
                    }
                )
            else:
                metadata.update(
                    {
                        "identity_hint": value.username,
                        "auth_method": value.auth_method,
                        "has_password": bool(value.password),
                        "has_private_key": bool(value.private_key),
                    }
                )
            index["profiles"][value.profile] = metadata
            _atomic_json_write(self._index_path, index)
            return CredentialStatus(profile=value.profile, **metadata)

    def get(
        self,
        profile: str,
        *,
        expected_kind: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            metadata = self._load_index().get("profiles", {}).get(profile)
            if not metadata:
                raise NotFoundError(f"凭据配置 {profile!r} 不存在。")
            metadata_kind = str(metadata.get("kind", "s3"))
            if expected_kind is not None and metadata_kind != expected_kind:
                raise CredentialError(
                    f"凭据配置 {profile!r} 属于 {metadata_kind.upper()}，"
                    f"不能用于 {expected_kind.upper()} 存储。"
                )
            backend = metadata.get("backend")
            payload: str | None
            if backend == "keyring":
                try:
                    payload = self._keyring_module().get_password(
                        SERVICE_NAME, profile
                    )
                except CredentialError:
                    raise
                except Exception as exc:
                    raise CredentialError(f"读取系统 keyring 失败：{exc}") from exc
            elif backend == "encrypted_file":
                encoded = self._encrypted_entries().get(profile)
                payload = self._decrypt(profile, encoded) if encoded else None
            else:
                raise CredentialError(f"未知凭据后端：{backend}")
            if not payload:
                raise CredentialError(f"凭据配置 {profile!r} 的秘密内容已丢失。")
            try:
                value = json.loads(payload)
            except (TypeError, json.JSONDecodeError) as exc:
                raise CredentialError("凭据内容已损坏。") from exc
            if not isinstance(value, dict):
                raise CredentialError("凭据内容已损坏。")
            payload_kind = str(value.get("kind", metadata_kind))
            if payload_kind != metadata_kind:
                raise CredentialError("凭据元数据与秘密内容的协议类型不一致。")
            value["kind"] = payload_kind
            if expected_kind is not None and payload_kind != expected_kind:
                raise CredentialError(
                    f"凭据配置 {profile!r} 不能用于 {expected_kind.upper()} 存储。"
                )
            return value

    def list(self) -> list[CredentialStatus]:
        with self._lock:
            profiles = self._load_index().get("profiles", {})
            return [
                CredentialStatus(
                    profile=name,
                    **{
                        **metadata,
                        "kind": metadata.get("kind", "s3"),
                    },
                )
                for name, metadata in sorted(profiles.items())
            ]

    def delete(self, profile: str) -> None:
        with self._lock:
            index = self._load_index()
            metadata = index.get("profiles", {}).get(profile)
            if not metadata:
                raise NotFoundError(f"凭据配置 {profile!r} 不存在。")
            backend = metadata.get("backend")
            if backend == "keyring":
                try:
                    keyring = self._keyring_module()
                    try:
                        keyring.delete_password(SERVICE_NAME, profile)
                    except keyring.errors.PasswordDeleteError:
                        pass
                except CredentialError:
                    raise
            else:
                entries = self._encrypted_entries()
                entries.pop(profile, None)
                self._write_encrypted_entries(entries)
            index["profiles"].pop(profile, None)
            _atomic_json_write(self._index_path, index)

    def backend_status(self) -> dict[str, Any]:
        keyring_available = False
        keyring_name: str | None = None
        try:
            module = self._keyring_module()
            backend = module.get_keyring()
            keyring_available = True
            keyring_name = backend.__class__.__name__
        except CredentialError:
            pass
        return {
            "preference": self.preference,
            "keyring_available": keyring_available,
            "keyring_backend": keyring_name,
            "encrypted_file_available": True,
        }


def _credential_hint(access_key: str) -> str:
    if len(access_key) <= 8:
        return "*" * max(4, len(access_key))
    return f"{access_key[:4]}…{access_key[-4:]}"


_SECRET_PATTERNS = [
    re.compile(
        r"(?i)((?:[\"']?)(?:secret[_-]?(?:access[_-]?)?key|"
        r"session[_-]?token|password|passphrase|private[_-]?key)"
        r"(?:[\"']?)\s*[=:]\s*)"
        r"(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"
    ),
    re.compile(r"\b(AKIA|ASIA)[A-Z0-9]{12,20}\b"),
    re.compile(
        r"-----BEGIN (?:OPENSSH |RSA |EC |DSA )?PRIVATE KEY-----.*?"
        r"-----END (?:OPENSSH |RSA |EC |DSA )?PRIVATE KEY-----",
        re.IGNORECASE | re.DOTALL,
    ),
]


def redact_sensitive(value: str) -> str:
    redacted = _SECRET_PATTERNS[2].sub('"****"', value)
    redacted = _SECRET_PATTERNS[0].sub(r'\1"****"', redacted)
    redacted = _SECRET_PATTERNS[1].sub(
        lambda match: f"{match.group(0)[:4]}****{match.group(0)[-4:]}",
        redacted,
    )
    return redacted


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
            record.msg = redact_sensitive(message)
            record.args = ()
        except Exception:
            pass
        return True
