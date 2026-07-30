from __future__ import annotations

from easybackup.errors import StorageError
from easybackup.locking import LeaseGuard
from easybackup.storage.base import RemoteLease


class _FailingLeaseStore:
    def __init__(self, *, fail_renew: bool = False, fail_release: bool = False):
        self.fail_renew = fail_renew
        self.fail_release = fail_release
        self.released = False

    def acquire_lease(self, key, owner, ttl_seconds):
        return RemoteLease(key, owner, "token", "2099-01-01T00:00:00+00:00", "v1")

    def renew_lease(self, lease, ttl_seconds):
        if self.fail_renew:
            raise StorageError("temporary remote failure")
        return lease

    def release_lease(self, lease):
        self.released = True
        if self.fail_release:
            raise StorageError("temporary release failure")


def test_lease_renewal_exception_fails_closed():
    store = _FailingLeaseStore(fail_renew=True)
    guard = LeaseGuard(store, "locks/task.json", "owner")
    guard.lease = store.acquire_lease("locks/task.json", "owner", 300)

    assert guard._renew_once() is False
    assert guard.lost is True


def test_lease_release_failure_does_not_mask_success():
    store = _FailingLeaseStore(fail_release=True)

    with LeaseGuard(store, "locks/task.json", "owner"):
        pass

    assert store.released is True
