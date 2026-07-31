from __future__ import annotations

import pytest

from easybackup.errors import ConflictError, CredentialError, NotFoundError
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
    assert status.kind == "s3"
    assert status.access_key_hint.startswith("AKIA")
    actual = credentials.get("s3-prod", expected_kind="s3")
    assert actual["kind"] == "s3"
    assert actual["secret_access_key"] == "very-secret-value"
    assert credentials.list()[0].profile == "s3-prod"
    with pytest.raises(CredentialError):
        credentials.get("s3-prod", expected_kind="sftp")

    credentials.delete("s3-prod")
    with pytest.raises(NotFoundError):
        credentials.get("s3-prod")


def test_credential_input_strips_copy_paste_whitespace():
    value = CredentialWrite(
        profile=" aliyun ",
        access_key_id=" LTAIexample ",
        secret_access_key=" secret-value\r\n",
        session_token=" token-value ",
    )

    assert value.profile == "aliyun"
    assert value.access_key_id == "LTAIexample"
    assert value.secret_access_key == "secret-value"
    assert value.session_token == "token-value"


def test_sftp_password_credentials_are_typed_encrypted_and_redacted(
    credentials,
):
    status = credentials.put(
        CredentialWrite(
            kind="sftp",
            profile="sftp-prod",
            username=" backup-user ",
            auth_method="password",
            password="not-returned-by-the-api",
        )
    )

    assert status.kind == "sftp"
    assert status.identity_hint == "backup-user"
    assert status.auth_method == "password"
    assert status.has_password is True
    assert status.has_private_key is False
    assert "not-returned-by-the-api" not in status.model_dump_json()
    assert "not-returned-by-the-api" not in credentials.list()[0].model_dump_json()

    actual = credentials.get("sftp-prod", expected_kind="sftp")
    assert actual == {
        "kind": "sftp",
        "username": "backup-user",
        "auth_method": "password",
        "password": "not-returned-by-the-api",
    }
    with pytest.raises(CredentialError):
        credentials.get("sftp-prod", expected_kind="s3")


def test_sftp_private_key_is_normalized_but_never_listed(credentials):
    private_key = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\r\n"
        "test-only-key-material\r\n"
        "-----END OPENSSH PRIVATE KEY-----\r\n"
    )
    status = credentials.put(
        CredentialWrite(
            kind="sftp",
            profile="sftp-key",
            username="key-user",
            auth_method="private_key",
            private_key=private_key,
            private_key_passphrase="key-passphrase",
        )
    )

    assert status.has_private_key is True
    assert status.has_password is False
    listed = credentials.list()[0].model_dump_json()
    assert "test-only-key-material" not in listed
    assert "key-passphrase" not in listed
    actual = credentials.get("sftp-key", expected_kind="sftp")
    assert "\r" not in actual["private_key"]
    assert actual["private_key"].endswith("\n")
    assert actual["private_key_passphrase"] == "key-passphrase"


def test_credential_profile_cannot_change_protocol_kind(credentials):
    credentials.put(
        CredentialWrite(
            profile="shared-name",
            access_key_id="AKIA1234567890EXAMPLE",
            secret_access_key="s3-secret",
        )
    )

    with pytest.raises(ConflictError):
        credentials.put(
            CredentialWrite(
                kind="sftp",
                profile="shared-name",
                username="backup-user",
                auth_method="password",
                password="sftp-secret",
            )
        )


def test_log_redaction():
    private_key = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\n"
        "sensitive-private-key-material\n"
        "-----END OPENSSH PRIVATE KEY-----"
    )
    message = (
        "secret_access_key=abcdef "
        'access=AKIA1234567890EXAMPLE session_token:xyz '
        '"password": "quoted-secret" '
        f"private_key={private_key}"
    )
    redacted = redact_sensitive(message)
    assert "abcdef" not in redacted
    assert "1234567890" not in redacted
    assert "xyz" not in redacted
    assert "quoted-secret" not in redacted
    assert "sensitive-private-key-material" not in redacted
