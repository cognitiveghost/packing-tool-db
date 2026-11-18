"""Test doubles shared by the offline buffer tests."""
from shared.store import FileStore, StoreError, StoreUnreachable


class FlakyStore(FileStore):
    """A FileStore whose network can be cut; the lock can be taken by another PC.

    `down`: every call is a network failure (StoreUnreachable).
    `broken`: a permanent refusal (a StoreError, not a network failure) for writes.
    `lock_ours`: what heartbeat_list_lock answers.
    `kill_after_commit`: raise KeyboardInterrupt right after the Nth update_json commit, counted
    from the moment it is set: a process killed mid-flush, after its write landed.
    """

    def __init__(self, root):
        super().__init__(root)
        self.down = False
        self.broken = None
        self.lock_ours = True
        self.kill_after_commit = None

    @property
    def kill_after_commit(self):
        return self._kill_after

    @kill_after_commit.setter
    def kill_after_commit(self, n):
        self._kill_after = n
        self._commits = 0

    def _check(self):
        if self.down:
            raise StoreUnreachable("network")

    def _check_writable(self):
        self._check()
        if self.broken:
            raise StoreError(self.broken)

    def read_json(self, rel):
        self._check()
        return super().read_json(rel)

    def write_json(self, rel, data):
        self._check_writable()
        super().write_json(rel, data)

    def update_json(self, rel, fn):
        self._check_writable()
        result = super().update_json(rel, fn)
        self._commits += 1
        if self._kill_after is not None and self._commits == self._kill_after:
            raise KeyboardInterrupt  # killed after the write landed, before the caller hears of it
        return result

    def write_bytes(self, rel, data):
        self._check_writable()
        super().write_bytes(rel, data)

    def exists(self, rel):
        self._check()
        return super().exists(rel)

    def heartbeat_list_lock(self, list_dir, pc):
        self._check()
        return self.lock_ours

    def release_list_lock(self, list_dir, pc):
        self._check()
        return super().release_list_lock(list_dir, pc)
