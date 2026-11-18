"""Worker stats go through a named Delta, so an offline queue can replay them once.

A real WorkerManager over a FileStore on a temp folder; a spy records the update_json calls.
"""
import json

from packing_tool.worker_manager import REGISTRY, WorkerManager
from shared.store import Delta, FileStore


class _SpyStore(FileStore):
    def __init__(self, root):
        super().__init__(root)
        self.updates = []

    def update_json(self, rel, fn):
        self.updates.append((rel, fn))
        return super().update_json(rel, fn)


def _registry(tmp_path):
    return json.loads((tmp_path / REGISTRY).read_text(encoding="utf-8"))


def test_update_worker_stats_hands_the_store_a_stats_delta(tmp_path):
    store = _SpyStore(tmp_path)
    manager = WorkerManager(store)
    worker = manager.create_worker("Ana")
    store.updates.clear()

    manager.update_worker_stats(worker.id, sessions=1, orders=3)

    (rel, fn), = store.updates
    assert rel == REGISTRY
    assert isinstance(fn, Delta)
    assert fn.kind == "workers.update_stats"


def test_replayed_stats_delta_counts_once(tmp_path):
    store = FileStore(tmp_path)
    WorkerManager(store).create_worker("Ana")
    args = {
        "worker_id": "worker_001", "sessions": 1, "orders": 3, "items": 5,
        "duration_seconds": 60, "session_id": "s1", "timestamp": "2026-11-17T10:00:00+00:00",
    }

    store.update_json(REGISTRY, Delta("workers.update_stats", args, op_id="op-1"))
    store.update_json(REGISTRY, Delta("workers.update_stats", args, op_id="op-1"))

    worker = _registry(tmp_path)["workers"][0]
    assert worker["total_sessions"] == 1
    assert worker["total_orders"] == 3
    assert worker["last_active"] == "2026-11-17T10:00:00+00:00"


def test_stats_delta_keeps_unknown_keys_of_the_registry(tmp_path):
    store = FileStore(tmp_path)
    WorkerManager(store).create_worker("Ana")
    doc = _registry(tmp_path)
    doc["applied_ops"] = ["earlier"]
    (tmp_path / REGISTRY).write_text(json.dumps(doc), encoding="utf-8")

    WorkerManager(store).update_worker_stats("worker_001", orders=2)

    assert _registry(tmp_path)["applied_ops"] == ["earlier"]
