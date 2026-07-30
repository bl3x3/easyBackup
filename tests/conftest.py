from __future__ import annotations

import pytest

from easybackup.db import Database
from easybackup.security import CredentialStore


@pytest.fixture
def database(tmp_path):
    value = Database(tmp_path / "easybackup.db")
    value.initialize()
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def credentials(tmp_path):
    return CredentialStore(tmp_path / "secrets", "encrypted_file")

