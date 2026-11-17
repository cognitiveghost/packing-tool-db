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
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from packing_tool.worker_manager import REGISTRY
from shared.stats_manager import STATS_REL
from shared.store import (
    SESSION_INFO,
    Delta,
    FileStore,
    Store,
    StoreError,
    StoreNotFound,
    StoreUnreachable,
)

OUTBOX = "outbox.jsonl"
LIST_META = "list.json"  # the bound list: what a restart needs to reopen it with no database
SNAPSHOT = "snapshot"  # copies of the documents the bound list reads, served when unreachable


def _read_meta(buffer_dir: Path) -> dict | None:
    try:
        return json.loads((Path(buffer_dir) / LIST_META).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except ValueError:
        return None  # unreadable: the outbox still names its list (spec §8.3)


def _write_meta(buffer_dir: Path, meta: dict) -> None:
    tmp = Path(buffer_dir) / (LIST_META + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, Path(buffer_dir) / LIST_META)


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
    def __init__(
        self, inner: Store, buffer_dir: Path, *, pc: str, on_change: Callable[[bool], None] | None = None
    ):
        self.inner = inner
        self.buffer_dir = Path(buffer_dir)
        self.pc = pc
        self.derived_indexes = inner.derived_indexes
        self._on_change = on_change
        self._state = FileStore(self.buffer_dir / "state")
        self._snapshot = FileStore(self.buffer_dir / SNAPSHOT)
        # The heartbeat thread flushes while the UI thread writes: an op appended mid-flush
        # must never be cleared unpushed.
        self._mutex = threading.RLock()
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        _drop_torn_tail(self.buffer_dir / OUTBOX)
        ops = self._ops()
        # The outbox is the truth; state/ is its read cache. Rebuild it, so a kill between
        # the two writes, or a half-done clear, never serves a value the outbox lacks.
        shutil.rmtree(self.buffer_dir / "state", ignore_errors=True)
        for op in ops:
            if op["op"] in ("write_json", "update_json_result"):
                self._state.write_json(op["rel"], op["data"])
        self._online = not ops  # a leftover outbox means we have not flushed yet
        self._bind(_read_meta(self.buffer_dir))

    @property
    def online(self) -> bool:
        return self._online

    @online.setter
    def online(self, value: bool) -> None:
        value = bool(value)
        if value != self._online:
            self._online = value
            if self._on_change:
                self._on_change(value)

    def _bind(self, meta: dict | None) -> None:
        self._meta = meta
        self._list_dir = meta["list_dir"] if meta else None

    @staticmethod
    def pending(buffer_dir: Path) -> str | None:
        ops = _read_outbox(buffer_dir)
        return ops[0]["list_dir"] if ops else None

    @staticmethod
    def pending_meta(buffer_dir: Path) -> dict | None:
        return _read_meta(buffer_dir) if _read_outbox(buffer_dir) else None

    def bound_list(self) -> dict | None:
        return dict(self._meta) if self._meta else None

    def begin_list(self, list_dir: str, meta: dict, snapshot_rels: list[str]) -> None:
        """Bind the run to the open list, keeping what a restart offline reads of it."""
        with self._mutex:
            ops = self._ops()
            if ops and ops[0]["list_dir"] != list_dir:
                # One outbox holds one list: its ops are checked against that list's lock only.
                raise StoreError(f"offline progress for {ops[0]['list_dir']} is not synced yet")
            # Read everything before writing anything: a failed read leaves the old snapshot intact.
            docs = {}
            for rel in snapshot_rels:
                try:
                    docs[rel] = self.inner.read_json(rel)
                except StoreNotFound:
                    continue
            for rel, data in docs.items():
                self._snapshot.write_json(rel, data)
            entry = {**meta, "list_dir": list_dir}
            _write_meta(self.buffer_dir, entry)
            self._bind(entry)

    def end_list(self) -> None:
        """Unbind once the outbox is empty. Pending ops keep the list bound: they still need it."""
        with self._mutex:
            if self._ops():
                return
            (self.buffer_dir / LIST_META).unlink(missing_ok=True)
            shutil.rmtree(self.buffer_dir / SNAPSHOT, ignore_errors=True)
            self._bind(None)

    def attach(self, inner: Store) -> None:
        """Swap in the real backend once it is connected (UnreachableStore before that)."""
        with self._mutex:
            self.inner = inner
            self.derived_indexes = inner.derived_indexes

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

    def _check_allowed(self, rel: str, *, delta: bool = False) -> None:
        """Offline: the bound list's folder (whole documents), or a Delta on a shared doc only."""
        lst = self._list_dir
        if lst is None:
            raise StoreUnreachable("offline")
        session_info = "/".join(lst.split("/")[:3] + [SESSION_INFO])
        if rel in (STATS_REL, REGISTRY, session_info):
            if not delta:
                raise StoreUnreachable("offline")  # a whole-document replay would overwrite other PCs
            return
        if not rel.startswith(lst + "/"):
            raise StoreUnreachable("offline")

    def _queue_delta(self, rel: str, d: Delta) -> None:
        self._check_allowed(rel, delta=True)
        entry = {
            "op": "delta", "rel": rel, "list_dir": self._list_dir,
            "kind": d.kind, "args": d.args, "op_id": d.op_id or uuid.uuid4().hex,
        }
        with self._mutex:
            self._append(entry)  # no state/ copy: nothing reads shared docs offline
            self.online = False

    def _buffer(self, op: str, rel: str, data=None, blob: bytes | None = None) -> None:
        self._check_allowed(rel)
        entry = {"op": op, "rel": rel, "list_dir": self._list_dir}
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
        """An unreachable backend flips to offline. Any other error is a real answer."""
        if not isinstance(e, StoreUnreachable):
            return False
        self.online = False
        return True

    # --- reads: buffer first, then inner -----------------------------------
    def _buffered(self, rel) -> bool:
        return not self.online and self._state.exists(rel)

    def _inner_or_snapshot(self, rel, read, fallback):
        """Inner first. Unreachable: go offline, and serve the snapshot copy of rel when there is one."""
        try:
            return read()
        except StoreError as e:
            self._lost(e)
            if isinstance(e, StoreUnreachable) and self._snapshot.exists(rel):
                return fallback()
            raise

    def read_json(self, rel):
        if self._buffered(rel):
            return self._state.read_json(rel)
        return self._inner_or_snapshot(rel, lambda: self.inner.read_json(rel), lambda: self._snapshot.read_json(rel))

    def read_bytes(self, rel):
        if self._buffered(rel):
            return self._state.read_bytes(rel)
        return self._inner_or_snapshot(rel, lambda: self.inner.read_bytes(rel), lambda: self._snapshot.read_bytes(rel))

    def exists(self, rel):
        if self._buffered(rel):
            return True
        return self._inner_or_snapshot(rel, lambda: self.inner.exists(rel), lambda: True)

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
            # Queued once, never applied locally too: the replay on flush applies it.
            if isinstance(fn, Delta):
                self._queue_delta(rel, fn)
                return None
            self._check_allowed(rel)  # a closure is replayed as a whole document: no shared docs
            try:
                current = self.read_json(rel)
            except StoreNotFound:
                current = None
            except StoreError:
                raise StoreUnreachable("offline") from None  # never fn(None) over a state we could not read
            result = fn(current)
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
        with self._mutex:
            if self.online:
                try:
                    return self.inner.release_list_lock(list_dir, pc)
                except StoreError as e:
                    if not self._lost(e):
                        raise
            if list_dir != self._list_dir:
                raise StoreUnreachable("offline")
            self._append({"op": "release_lock", "list_dir": list_dir})  # replayed on flush
            self.online = False

    def finished(self) -> bool:
        """The list is finished: its release is queued, waiting for the flush."""
        with self._mutex:
            return any(op["op"] == "release_lock" for op in self._ops())

    def lock_info(self, list_dir):
        return self._inner(self.inner.lock_info, list_dir)

    def local_path(self, rel):
        return self.inner.local_path(rel)

    def stamp(self, rel):
        return self._inner(self.inner.stamp, rel)

    def query_sessions(self, client_id):
        return self._inner(self.inner.query_sessions, client_id)

    def packing_overview(self, client_id):
        return self._inner(self.inner.packing_overview, client_id)

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
            list_dir = ops[0]["list_dir"]  # one outbox holds one list
            try:
                ours = self.inner.heartbeat_list_lock(list_dir, self.pc)
            except StoreError:
                self.online = False
                return FlushResult(0)
            if not ours:
                return self._write_conflict(ops, list_dir)
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

    def _write_conflict(self, ops: list[dict], list_dir: str) -> FlushResult:
        """Another PC took the list: park everything we buffered beside its state, overwrite nothing."""
        state: dict = {}
        blobs: dict = {}
        for op in ops:  # later ops win, same as a replay
            if op["op"] == "write_bytes":
                blobs[op["rel"]] = base64.b64encode(self._state.read_bytes(op["blob_file"])).decode("ascii")
            else:
                state[op["rel"]] = op["data"]
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        rel = f"{list_dir}/packing_state_conflict_{self.pc}_{stamp}.json"
        try:
            self.inner.write_json(rel, {"pc": self.pc, "state": state, "blobs_b64": blobs})
        except StoreError:
            self.online = False
            return FlushResult(0)  # outbox kept: nothing is dropped until the copy is safe
        self._clear()
        self.online = True
        return FlushResult(0, rel)


_STORE_METHODS = [n for n, v in vars(Store).items() if callable(v) and not n.startswith("_")]


def _unreachable(name: str):
    def refuse(self, *args, **kwargs):
        raise StoreUnreachable("database not connected")

    refuse.__name__ = name
    return refuse


class UnreachableStore(Store):
    """The Store before a database sign-in: every call is a network failure, so writes queue."""

    derived_indexes = True


for _name in _STORE_METHODS:
    setattr(UnreachableStore, _name, _unreachable(_name))
