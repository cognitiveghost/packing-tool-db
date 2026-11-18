"""Flush replays the outbox once: deltas skip by op_id, a permanent error stops it and keeps the outbox.

Real managers over a FlakyStore (a FileStore whose network can be cut): spec §8.2, test 3, 4, 5, 7.
"""
from types import SimpleNamespace

import pytest
from offline_fakes import FlakyStore

import packing_tool.session_manager
import shared.metadata_utils
import shared.stats_manager
from packing_tool.offline_buffer import BufferedStore
from packing_tool.session_lock_manager import SessionLockManager
from packing_tool.session_manager import SessionManager
from packing_tool.worker_manager import WorkerManager
from shared.stats_manager import StatsManager
from shared.store import StoreUnreachable

CLIENT = "M"
SESSION = "Sessions/CLIENT_M/2026-01-01_1"
INFO = f"{SESSION}/session_info.json"
LIST_NAME = "DHL"
LIST = f"{SESSION}/packing/DHL"
STATS = "Stats/global_stats.json"
REGISTRY = "Workers/workers.json"
STAMP = "2026-11-17T09:00:00+00:00"


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch):
    def now():  # both sides stamp the same moment, so the docs compare exactly
        return STAMP

    monkeypatch.setattr(shared.metadata_utils, "get_current_timestamp", now)
    monkeypatch.setattr(shared.stats_manager, "get_current_timestamp", now)
    monkeypatch.setattr(packing_tool.session_manager, "get_current_timestamp", now)


def seed(store):
    store.write_json(INFO, {"session_name": "2026-01-01_1", "client_id": CLIENT})
    return WorkerManager(store).create_worker("Ann").id


def finish(store, server_root, worker_id):
    """The three calls a list makes when it finishes: stats, worker, session progress."""
    StatsManager(store).record_packing(CLIENT, "s1", worker_id, 3, 9)
    WorkerManager(store).update_worker_stats(worker_id, sessions=1, orders=3, items=9,
                                             duration_seconds=60, session_id="s1")
    SessionManager(client_id=CLIENT, store=store).update_session_metadata(
        str(server_root / SESSION), LIST_NAME, "completed", ["#1", "#2", "#3"],
    )


def make(inner, buf):
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list(LIST, {}, [INFO])
    return b


def worker_total_sessions(store, worker_id):
    return next(w for w in store.read_json(REGISTRY)["workers"] if w["id"] == worker_id)["total_sessions"]


def without_op_ids(doc):
    return {k: v for k, v in doc.items() if k != "applied_ops"}


def test_finish_offline_then_flush_matches_online(tmp_path):
    online = FlakyStore(tmp_path / "s1")
    worker_id = seed(online)
    finish(online, online.root, worker_id)

    server = FlakyStore(tmp_path / "s2")
    seed(server)
    b = make(server, tmp_path / "buf")
    server.down = True
    finish(b, server.root, worker_id)
    server.down = False

    assert b.flush().pushed == 3
    for rel in (STATS, REGISTRY, INFO):
        assert without_op_ids(server.read_json(rel)) == without_op_ids(online.read_json(rel)), rel


def test_other_pc_edit_survives_flush(tmp_path):
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    b = make(server, tmp_path / "buf")
    server.down = True
    finish(b, server.root, worker_id)
    server.down = False
    StatsManager(server).record_packing("B", "s9", "002", 7, 9)  # another PC, straight on the server

    assert b.flush().pushed == 3

    stats = server.read_json(STATS)
    assert stats["total_orders_packed"] == 3 + 7
    assert stats["by_client"]["M"]["orders_packed"] == 3
    assert stats["by_client"]["B"]["orders_packed"] == 7


def test_killed_flush_counts_once(tmp_path):
    buf = tmp_path / "buf"
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    b = make(server, buf)
    server.down = True
    finish(b, server.root, worker_id)
    server.down = False

    server.kill_after_commit = 1  # the process dies right after the first delta lands
    with pytest.raises(KeyboardInterrupt):
        b.flush()
    server.kill_after_commit = None

    assert BufferedStore(server, buf, pc="PC1").flush().pushed == 3
    assert server.read_json(STATS)["total_sessions"] == 1
    assert worker_total_sessions(server, worker_id) == 1


def test_permanent_error_mid_flush_keeps_outbox(tmp_path):
    buf = tmp_path / "buf"
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    b = make(server, buf)
    server.down = True
    finish(b, server.root, worker_id)
    server.down = False
    outbox_before = (buf / "outbox.jsonl").read_bytes()

    server.broken = "denied"
    res = b.flush()

    assert "denied" in res.error
    assert (buf / "outbox.jsonl").read_bytes() == outbox_before
    assert "denied" in b.sync_error
    assert b.online is False

    server.broken = None
    assert b.flush().pushed == 3
    assert b.sync_error is None
    assert server.read_json(STATS)["total_sessions"] == 1


def test_missing_document_on_replay_stops_and_keeps_outbox(tmp_path):
    buf = tmp_path / "buf"
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    b = make(server, buf)
    server.down = True
    finish(b, server.root, worker_id)
    server.down = False
    outbox_before = (buf / "outbox.jsonl").read_bytes()
    (server.root / INFO).unlink()  # session_info deleted while we were offline

    res = b.flush()

    assert res.error
    assert (buf / "outbox.jsonl").read_bytes() == outbox_before
    assert b.sync_error

    server.write_json(INFO, {"session_name": "2026-01-01_1", "client_id": CLIENT})
    assert b.flush().pushed == 3  # the op was never dropped: it lands once the doc is back
    assert server.read_json(STATS)["total_sessions"] == 1


def test_release_lock_replayed_last(tmp_path):
    buf = tmp_path / "buf"
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    server.acquire_list_lock(LIST, "PC1", "ann")
    b = make(server, buf)
    server.down = True
    finish(b, server.root, worker_id)
    b.release_list_lock(LIST, "PC1")
    server.down = False

    assert b.flush().pushed == 4

    assert server.lock_info(LIST) is None
    assert not (buf / "list.json").exists()
    assert not (buf / "snapshot").exists()
    assert b.bound_list() is None


def test_conflict_parks_deltas_unapplied(tmp_path):
    buf = tmp_path / "buf"
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    server.write_json(STATS, {"total_sessions": 2})
    b = make(server, buf)
    server.down = True
    finish(b, server.root, worker_id)
    server.down = False
    server.lock_ours = False  # another PC took the list while we were offline

    res = b.flush()

    assert res.pushed == 0
    assert res.conflict_rel.startswith(f"{LIST}/packing_state_conflict_PC1_")
    parked = server.read_json(res.conflict_rel)
    assert [d["kind"] for d in parked["deltas"]] == [
        "stats.record_packing", "workers.update_stats", "session_info.packing_progress",
    ]
    assert server.read_json(STATS) == {"total_sessions": 2}
    assert BufferedStore.pending(buf) is None


def test_pending_blob_survives_restart(tmp_path):
    inner = FlakyStore(tmp_path / "srv")
    buf = tmp_path / "buf"
    b = BufferedStore(inner, buf, pc="PC1")
    b.begin_list("Sessions/CLIENT_M/s/packing/L", {}, [])
    inner.down = True
    b.write_bytes("Sessions/CLIENT_M/s/packing/L/report.bin", b"xyz")
    b2 = BufferedStore(inner, buf, pc="PC1")
    inner.down = False
    assert b2.flush().error is None
    assert inner.read_bytes("Sessions/CLIENT_M/s/packing/L/report.bin") == b"xyz"


def test_online_commit_whose_answer_is_lost_counts_once(tmp_path):
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    b = make(server, tmp_path / "buf")

    def lose_answer(rel, fn):  # the write lands; the answer never arrives
        FlakyStore.update_json(server, rel, fn)
        raise StoreUnreachable("read timeout")

    server.update_json = lose_answer
    StatsManager(b).record_packing(CLIENT, "s1", worker_id, 3, 9)  # queued: it looked like an outage
    del server.update_json

    assert b.flush().pushed == 1
    assert server.read_json(STATS)["total_sessions"] == 1


def test_replay_for_a_deleted_worker_does_not_stop_the_sync(tmp_path):
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    b = make(server, tmp_path / "buf")
    server.down = True
    finish(b, server.root, worker_id)
    server.down = False
    server.write_json(REGISTRY, {"workers": []})  # an admin deleted the worker meanwhile

    res = b.flush()

    assert res.error is None  # online this is a logged no-op, so the replay is too
    assert res.pushed == 3
    assert server.read_json(STATS)["total_sessions"] == 1


def test_end_session_offline_through_the_lock_manager_queues_the_release(tmp_path):
    server = FlakyStore(tmp_path / "server")
    worker_id = seed(server)
    lm = SessionLockManager(SimpleNamespace(store=None))
    b = BufferedStore(server, tmp_path / "buf", pc=lm.hostname)
    lm.profile_manager.store = b
    work_dir = server.root / LIST
    assert lm.acquire_lock(CLIENT, work_dir)[0]
    b.begin_list(LIST, {}, [INFO])
    server.down = True
    finish(b, server.root, worker_id)

    assert lm.release_lock(work_dir) is True  # what End session calls
    assert b.finished()

    server.down = False
    assert b.flush().pushed == 4
    assert server.lock_info(LIST) is None
