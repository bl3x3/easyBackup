from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from easybackup.errors import StorageError
from easybackup.storage.aliyun_lease import (
    AliyunAppendLeaseStore,
    PositionConflict,
)


_BUCKET = "backup-dinnerparty"
_PREFIX = "easybackup/test"
_LOG_KEY = f"{_PREFIX}/locks/task.json.oss-append-v1"


class _Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 2, 15, 30, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, *, seconds: int) -> None:
        self.now += timedelta(seconds=seconds)


class _FakeOSSAppendClient:
    """Thread-safe in-memory implementation of the lease client's narrow port."""

    def __init__(self, *, synchronize_first_reads: int = 0) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.append_attempts: list[tuple[str, str, int, bytes]] = []
        self.fail_next_append = False
        self._lock = threading.Lock()
        self._reads_to_synchronize = synchronize_first_reads
        self._read_count = 0
        self._read_barrier = (
            threading.Barrier(synchronize_first_reads)
            if synchronize_first_reads
            else None
        )

    def get_object(self, bucket: str, key: str) -> bytes | None:
        with self._lock:
            value = self.objects.get((bucket, key))
            should_wait = self._read_count < self._reads_to_synchronize
            self._read_count += 1

        # Capture the value before waiting so both contenders base their append
        # position on the same snapshot.
        if should_wait and self._read_barrier is not None:
            self._read_barrier.wait(timeout=5)
        return value

    def append_object(
        self,
        bucket: str,
        key: str,
        position: int,
        body: bytes,
    ) -> int:
        payload = bytes(body)
        with self._lock:
            self.append_attempts.append((bucket, key, position, payload))
            if self.fail_next_append:
                self.fail_next_append = False
                raise PositionConflict("simulated OSS position conflict")

            current = self.objects.get((bucket, key), b"")
            if position != len(current):
                raise PositionConflict(
                    f"expected position {len(current)}, got {position}"
                )
            updated = current + payload
            self.objects[(bucket, key)] = updated
            return len(updated)

    def seed(self, key: str, payload: bytes) -> None:
        with self._lock:
            self.objects[(_BUCKET, key)] = payload

    def read(self, key: str = _LOG_KEY) -> bytes | None:
        with self._lock:
            return self.objects.get((_BUCKET, key))


def _store(
    client: _FakeOSSAppendClient,
    clock: _Clock,
    *tokens: str,
) -> AliyunAppendLeaseStore:
    configured_tokens = tokens or ("token-default",)
    token_values = iter(configured_tokens)
    last_token = configured_tokens[-1]

    def next_token() -> str:
        nonlocal last_token
        try:
            last_token = next(token_values)
        except StopIteration:
            pass
        return last_token

    return AliyunAppendLeaseStore(
        client,
        bucket=_BUCKET,
        key_prefix=_PREFIX,
        clock=clock,
        token_factory=next_token,
    )


def _events(client: _FakeOSSAppendClient) -> list[dict[str, Any]]:
    payload = client.read()
    assert payload is not None
    assert payload.endswith(b"\n")
    return [json.loads(line) for line in payload.splitlines()]


def test_acquire_appends_active_lease_event_to_dedicated_log() -> None:
    client = _FakeOSSAppendClient()
    clock = _Clock()
    store = _store(client, clock, "token-one")

    lease = store.acquire_lease("locks/task.json", "host-one", 60)

    assert lease is not None
    assert lease.key == "locks/task.json"
    assert lease.owner == "host-one"
    assert lease.token == "token-one"
    assert datetime.fromisoformat(lease.expires_at) == clock.now + timedelta(
        seconds=60
    )
    assert lease.version
    assert len(client.append_attempts) == 1
    bucket, key, position, _body = client.append_attempts[0]
    assert (bucket, key, position) == (_BUCKET, _LOG_KEY, 0)

    events = _events(client)
    assert len(events) == 1
    assert events[0]["schema"] == 1
    assert events[0]["state"] == "active"
    assert events[0]["owner"] == "host-one"
    assert events[0]["token"] == "token-one"


def test_two_contenders_can_only_append_one_initial_acquisition() -> None:
    client = _FakeOSSAppendClient(synchronize_first_reads=2)
    clock = _Clock()
    first = _store(client, clock, "token-one")
    second = _store(client, clock, "token-two")
    results: list[Any] = []
    errors: list[BaseException] = []
    result_lock = threading.Lock()

    def acquire(store: AliyunAppendLeaseStore, owner: str) -> None:
        try:
            lease = store.acquire_lease("locks/task.json", owner, 60)
            with result_lock:
                results.append(lease)
        except BaseException as exc:  # pragma: no cover - asserted below
            with result_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=acquire, args=(first, "host-one")),
        threading.Thread(target=acquire, args=(second, "host-two")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert thread.is_alive() is False

    assert errors == []
    winners = [lease for lease in results if lease is not None]
    assert len(winners) == 1
    assert winners[0].owner in {"host-one", "host-two"}
    assert len(client.append_attempts) == 2
    events = _events(client)
    assert len(events) == 1
    assert events[0]["token"] == winners[0].token


def test_owner_can_renew_with_the_same_token() -> None:
    client = _FakeOSSAppendClient()
    clock = _Clock()
    store = _store(client, clock, "token-one")
    lease = store.acquire_lease("locks/task.json", "host-one", 60)
    assert lease is not None
    clock.advance(seconds=30)

    renewed = store.renew_lease(lease, 120)

    assert renewed is not None
    assert renewed.token == lease.token
    assert renewed.owner == lease.owner
    assert renewed.version != lease.version
    assert datetime.fromisoformat(renewed.expires_at) == clock.now + timedelta(
        seconds=120
    )
    events = _events(client)
    assert [event["state"] for event in events] == ["active", "active"]
    assert events[-1]["token"] == lease.token
    assert datetime.fromisoformat(events[-1]["expires_at"]) == (
        clock.now + timedelta(seconds=120)
    )


def test_wrong_token_cannot_renew_or_release_active_lease() -> None:
    client = _FakeOSSAppendClient()
    clock = _Clock()
    store = _store(client, clock, "token-one")
    lease = store.acquire_lease("locks/task.json", "host-one", 60)
    assert lease is not None
    forged = replace(lease, token="attacker-token")
    original = client.read()

    assert store.renew_lease(forged, 120) is None
    store.release_lease(forged)

    assert client.read() == original
    assert len(client.append_attempts) == 1
    assert store.acquire_lease("locks/task.json", "host-two", 60) is None


def test_expired_lease_can_be_taken_over_with_a_new_token() -> None:
    client = _FakeOSSAppendClient()
    clock = _Clock()
    first = _store(client, clock, "token-one")
    second = _store(client, clock, "token-two")
    expired = first.acquire_lease("locks/task.json", "host-one", 10)
    assert expired is not None
    clock.advance(seconds=11)

    replacement = second.acquire_lease("locks/task.json", "host-two", 60)

    assert replacement is not None
    assert replacement.owner == "host-two"
    assert replacement.token == "token-two"
    assert replacement.token != expired.token
    assert first.renew_lease(expired, 60) is None
    events = _events(client)
    assert [event["owner"] for event in events] == ["host-one", "host-two"]
    assert all(event["state"] == "active" for event in events)


def test_release_is_appended_and_keeps_log_for_a_future_owner() -> None:
    client = _FakeOSSAppendClient()
    clock = _Clock()
    first = _store(client, clock, "token-one")
    second = _store(client, clock, "token-two")
    lease = first.acquire_lease("locks/task.json", "host-one", 60)
    assert lease is not None

    first.release_lease(lease)

    released_payload = client.read()
    assert released_payload
    events = _events(client)
    assert [event["state"] for event in events] == ["active", "released"]
    assert events[-1]["token"] == lease.token

    replacement = second.acquire_lease("locks/task.json", "host-two", 60)
    assert replacement is not None
    assert client.read() is not None
    events = _events(client)
    assert [event["state"] for event in events] == [
        "active",
        "released",
        "active",
    ]


def test_position_conflict_fails_acquisition_without_overwriting_log() -> None:
    client = _FakeOSSAppendClient()
    client.fail_next_append = True
    clock = _Clock()
    store = _store(client, clock, "token-one", "token-two")

    assert store.acquire_lease("locks/task.json", "host-one", 60) is None
    assert client.read() is None

    replacement = store.acquire_lease("locks/task.json", "host-two", 60)
    assert replacement is not None
    assert replacement.token == "token-two"
    events = _events(client)
    assert len(events) == 1
    assert events[0]["owner"] == "host-two"


@pytest.mark.parametrize(
    "damaged_log",
    [
        b'{"schema":1,"state":"active","token":"do-not-leak-token"',
        b'{"schema":99,"state":"active","owner":"host-one",'
        b'"token":"do-not-leak-token","expires_at":"2099-01-01T00:00:00+00:00"}\n',
    ],
)
def test_corrupt_or_unknown_lease_log_fails_closed(damaged_log: bytes) -> None:
    client = _FakeOSSAppendClient()
    client.seed(_LOG_KEY, damaged_log)
    clock = _Clock()
    store = _store(client, clock, "new-token")

    with pytest.raises(StorageError) as raised:
        store.acquire_lease("locks/task.json", "new-owner", 60)

    assert raised.value.details["diagnostic"]["kind"] == "lease_log_corrupt"
    assert "do-not-leak-token" not in str(raised.value)
    assert "do-not-leak-token" not in repr(raised.value.details)
    assert client.read() == damaged_log
    assert client.append_attempts == []
