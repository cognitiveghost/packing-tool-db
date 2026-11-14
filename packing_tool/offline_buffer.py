"""Packer offline buffer (spec §7.3): a durable outbox over a Store.

While the database is unreachable, writes under the open list, its session_info.json and the
global stats are kept in a local buffer and appended to `outbox.jsonl` (fsync per line), so a
killed Packer can restart offline and still hold its progress. `flush` replays the outbox once
the list lock is confirmed to be ours; a lost lock writes a conflict copy and never overwrites.
"""
import base64
import json
import os
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from shared.stats_manager import STATS_REL
from shared.store import (
    SESSION_INFO,
    FileStore,
    Store,
    StoreConflict,
    StoreError,
    StoreNotFound,
)

OUTBOX = "outbox.jsonl"


@dataclass
class FlushResult:
    pushed: int
    conflict_rel: str | None = None


def _read_outbox(buffer_dir: Path) -> list[dict]:
    path = Path(buffer_dir) / OUTBOX
    if not path.exists():
        return []
    # The last item is "" after a whole line, or a torn line from a kill mid-append. A torn
    # line was never acknowledged, so it is skipped. A bad whole line is damage: say so and
    # keep the file, rather than drop the ops after it.
    lines = path.read_text(encoding="utf-8").split("\n")[:-1]
    try:
        return [json.loads(line) for line in lines]
    except ValueError:
        raise StoreError(f"offline outbox is damaged, kept as is: {path}") from None


def _drop_torn_tail(path: Path) -> None:
    """Cut a torn last line, so the next append starts a line of its own instead of fusing."""
    if not path.exists():
        return
    data = path.read_bytes()
    keep = data.rfind(b"\n") + 1
    if keep < len(data):
        with open(path, "r+b") as f:
            f.truncate(keep)
            os.fsync(f.fileno())


class BufferedStore(Store):
    def __init__(self, inner: Store, buffer_dir: Path, *, list_dir: str, pc: str):
        self.inner = inner
        self.buffer_dir = Path(buffer_dir)
        self.list_rel = list_dir  # not self.list_dir: that is the Store method below
        self.pc = pc
        self.derived_indexes = inner.derived_indexes
        self._state = FileStore(self.buffer_dir / "state")
        # The heartbeat thread flushes while the UI thread writes: an op appended mid-flush
        # must never be cleared unpushed.
        self._mutex = threading.RLock()
        parts = list_dir.split("/")
        self._allowed_exact = {STATS_REL, "/".join(parts[:3] + [SESSION_INFO])}
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        _drop_torn_tail(self.buffer_dir / OUTBOX)
        ops = self._ops()
        if ops and ops[0]["list_dir"] != list_dir:
            # One outbox holds one list: its ops are checked against that list's lock only.
            raise StoreError(f"offline progress for {ops[0]['list_dir']} is not synced yet")
        # The outbox is the truth; state/ is its read cache. Rebuild it, so a kill between
        # the two writes, or a half-done clear, never serves a value the outbox lacks.
        shutil.rmtree(self.buffer_dir / "state", ignore_errors=True)
        for op in ops:
            if op["op"] != "write_bytes":
                self._state.write_json(op["rel"], op["data"])
        self.online = not ops  # a leftover outbox means we have not flushed yet

    @staticmethod
    def pending(buffer_dir: Path) -> str | None:
        ops = _read_outbox(buffer_dir)
        return ops[0]["list_dir"] if ops else None

    # --- outbox -----------------------------------------------------------
    def _ops(self) -> list[dict]:
        return _read_outbox(self.buffer_dir)

    def _append(self, op: dict) -> None:
        with open(self.buffer_dir / OUTBOX, "ab") as f:
            start = f.tell()
            try:
                f.write((json.dumps(op) + "\n").encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())
            except BaseException:
                f.truncate(start)  # no half line for the next append to fuse with
                raise

    def _check_allowed(self, rel: str) -> None:
        if rel not in self._allowed_exact and not rel.startswith(self.list_rel + "/"):
            raise StoreError("offline")

    def _buffer(self, op: str, rel: str, data=None, blob: bytes | None = None) -> None:
        self._check_allowed(rel)
        entry = {"op": op, "rel": rel, "list_dir": self.list_rel}
        with self._mutex:
            if blob is not None:
                self._state.write_bytes(rel, blob)  # the blob exists before an op names it
                entry["blob_file"] = rel
                self._append(entry)
            else:
                entry["data"] = data
                self._append(entry)  # the outbox first: a restart rebuilds state/ from it
                self._state.write_json(rel, data)
            self.online = False  # flushed on the next heartbeat, even if a flush just ended

    def _lost(self, e: StoreError) -> bool:
        """A network-ish failure: flips to offline. Not-found and conflicts are real answers."""
        if isinstance(e, (StoreNotFound, StoreConflict)):
            return False
        self.online = False
        return True

    # --- reads: buffer first, then inner -----------------------------------
    def _buffered(self, rel) -> bool:
        return not self.online and self._state.exists(rel)

    def read_json(self, rel):
        if self._buffered(rel):
            return self._state.read_json(rel)
        try:
            return self.inner.read_json(rel)
        except StoreError as e:
            self._lost(e)
            raise

    def read_bytes(self, rel):
        if self._buffered(rel):
            return self._state.read_bytes(rel)
        try:
            return self.inner.read_bytes(rel)
        except StoreError as e:
            self._lost(e)
            raise

    def exists(self, rel):
        if self._buffered(rel):
            return True
        return self._inner(self.inner.exists, rel)

    # --- writes: inner when online and nothing queued, else the outbox -----------
    # Under the mutex, so a write never lands directly on inner while older ops still wait.
    def write_json(self, rel, data):
        with self._mutex:
            if self.online:
                try:
                    return self.inner.write_json(rel, data)
                except StoreError as e:
                    if not self._lost(e):
                        raise
            self._buffer("write_json", rel, data)

    def update_json(self, rel, fn):
        with self._mutex:
            if self.online:
                try:
                    return self.inner.update_json(rel, fn)
                except StoreError as e:
                    if not self._lost(e):
                        raise
            try:
                current = self.read_json(rel)
            except StoreNotFound:
                current = None
            except StoreError:
                raise StoreError("offline") from None  # never fn(None) over a state we could not read
            result = fn(current)
            # ponytail: replayed as a whole-document write, so on a shared doc (stats,
            # session_info) it is last-writer-wins over other PCs' edits made meanwhile. Offline
            # it only runs once the doc is buffered; queue deltas if that ever becomes common.
            self._buffer("update_json_result", rel, result)
            return result

    def write_bytes(self, rel, data):
        with self._mutex:
            if self.online:
                try:
                    return self.inner.write_bytes(rel, data)
                except StoreError as e:
                    if not self._lost(e):
                        raise
            self._buffer("write_bytes", rel, blob=data)

    # --- delegated ---------------------------------------------------------
    def _inner(self, fn, *args):
        try:
            return fn(*args)
        except StoreError as e:
            self._lost(e)
            raise

    def write_bytes_if(self, rel, data, expected):
        return self._inner(self.inner.write_bytes_if, rel, data, expected)

    def list_dir(self, rel):
        return self._inner(self.inner.list_dir, rel)

    def move(self, src, dst):
        return self._inner(self.inner.move, src, dst)

    def delete(self, rel):
        return self._inner(self.inner.delete, rel)

    def claim_dir(self, rel):
        return self._inner(self.inner.claim_dir, rel)

    def acquire_list_lock(self, list_dir, pc, user):
        return self._inner(self.inner.acquire_list_lock, list_dir, pc, user)

    def force_take_list_lock(self, list_dir, pc, user, expected_heartbeat):
        return self._inner(self.inner.force_take_list_lock, list_dir, pc, user, expected_heartbeat)

    def heartbeat_list_lock(self, list_dir, pc):
        return self._inner(self.inner.heartbeat_list_lock, list_dir, pc)

    def release_list_lock(self, list_dir, pc):
        return self._inner(self.inner.release_list_lock, list_dir, pc)

    def lock_info(self, list_dir):
        return self._inner(self.inner.lock_info, list_dir)

    def local_path(self, rel):
        return self.inner.local_path(rel)

    def stamp(self, rel):
        return self._inner(self.inner.stamp, rel)

    def query_sessions(self, client_id):
        return self._inner(self.inner.query_sessions, client_id)

    # --- flush ---------------------------------------------------------------
    def _clear(self) -> None:
        (self.buffer_dir / OUTBOX).unlink(missing_ok=True)
        shutil.rmtree(self.buffer_dir / "state", ignore_errors=True)

    def flush(self) -> FlushResult:
        with self._mutex:  # no append between reading the outbox and clearing it
            ops = self._ops()
            if not ops:
                self.online = True
                return FlushResult(0)
            try:
                ours = self.inner.heartbeat_list_lock(self.list_rel, self.pc)
            except StoreError:
                self.online = False
                return FlushResult(0)
            if not ours:
                return self._write_conflict(ops)
            pushed = 0
            for op in ops:
                try:
                    if op["op"] == "write_bytes":
                        self.inner.write_bytes(op["rel"], self._state.read_bytes(op["blob_file"]))
                    else:
                        self.inner.write_json(op["rel"], op["data"])
                except StoreError:
                    self.online = False
                    self._rewrite_outbox(ops[pushed:])
                    return FlushResult(pushed)
                pushed += 1
            self._clear()
            self.online = True
            return FlushResult(pushed)

    def _rewrite_outbox(self, ops: list[dict]) -> None:
        tmp = self.buffer_dir / (OUTBOX + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(o) + "\n" for o in ops)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.buffer_dir / OUTBOX)

    def _write_conflict(self, ops: list[dict]) -> FlushResult:
        """Another PC took the list: park everything we buffered beside its state, overwrite nothing."""
        state: dict = {}
        blobs: dict = {}
        for op in ops:  # later ops win, same as a replay
            if op["op"] == "write_bytes":
                blobs[op["rel"]] = base64.b64encode(self._state.read_bytes(op["blob_file"])).decode("ascii")
            else:
                state[op["rel"]] = op["data"]
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        rel = f"{self.list_rel}/packing_state_conflict_{self.pc}_{stamp}.json"
        try:
            self.inner.write_json(rel, {"pc": self.pc, "state": state, "blobs_b64": blobs})
        except StoreError:
            self.online = False
            return FlushResult(0)  # outbox kept: nothing is dropped until the copy is safe
        self._clear()
        self.online = True
        return FlushResult(0, rel)
