from __future__ import annotations

import pytest

from easybackup.errors import NotFoundError
from easybackup.models import CredentialWrite
from easybackup.security import redact_sensitive


def test_encrypted_file_credentials_never_list_secrets(credentials):
    status = credentials.put(
        CredentialWrite(
            profile="s3-prod",
            access_key_id="AKIA1234567890EXAMPLE",
            secret_access_key="very-secret-value",
            session_token="temporary-token",
        )
    )
    assert "AKIA1234567890EXAMPLE" not in status.model_dump_json()
    assert status.access_key_hint.startswith("AKIA")
    actual = credentials.get("s3-prod")
    assert actual["secret_access_key"] == "very-secret-value"
    assert credentials.list()[0].profile == "s3-prod"

    credentials.delete("s3-prod")
    with pytest.raises(NotFoundError):
        credentials.get("s3-prod")


def test_log_redaction():
    message = (
        "secret_access_key=abcdef "
        'access=AKIA1234567890EXAMPLE session_token:xyz '
        '"password": "quoted-secret"'
    )
    redacted = redact_sensitive(message)
    assert "abcdef" not in redacted
    assert "1234567890" not in redacted
    assert "xyz" not in redacted
    assert "quoted-secret" not in redacted
