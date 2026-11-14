"""BufferedStore: durable outbox, order, conflict copies (plan Task 11, review focus #1)."""
import threading
import time

import pytest

from packing_tool.offline_buffer import BufferedStore
from shared.store import FileStore, StoreError

LIST = "Sessions/CLIENT_M/2026-01-01_1/packing/DHL"
STATE = f"{LIST}/packing_state.json"
INFO = "Sessions/CLIENT_M/2026-01-01_1/session_info.json"
STATS = "Stats/global_stats.json"


class FlakyStore(FileStore):
    """A FileStore whose network can be cut; the lock can be taken by another PC."""

    def __init__(self, root):
        super().__init__(root)
        self.down = False
        self.lock_ours = True

    def _check(self):
        if self.down:
            raise StoreError("network")

    def read_json(self, rel):
        self._check()
        return super().read_json(rel)

    def write_json(self, rel, data):
        self._check()
        super().write_json(rel, data)

    def write_bytes(self, rel, data):
        self._check()
        super().write_bytes(rel, data)

    def exists(self, rel):
        self._check()
        return super().exists(rel)

    def heartbeat_list_lock(self, list_dir, pc):
        self._check()
        return self.lock_ours


@pytest.fixture
def inner(tmp_path):
    return FlakyStore(tmp_path / "server")


def make(inner, tmp_path):
    return BufferedStore(inner, tmp_path / "buf", list_dir=LIST, pc="PC1")


def test_online_writes_go_straight_through(inner, tmp_path):
    b = make(inner, tmp_path)
    b.write_json(STATE, {"n": 1})
    assert inner.read_json(STATE) == {"n": 1}
    assert BufferedStore.pending(tmp_path / "buf") is None


def test_offline_write_lands_after_flush(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    assert b.online is False
    assert b.read_json(STATE) == {"n": 1}  # buffer first
    inner.down = False
    assert b.flush().pushed == 1
    assert inner.read_json(STATE) == {"n": 1}
    assert b.online is True
    assert BufferedStore.pending(tmp_path / "buf") is None


def test_flush_while_still_offline_keeps_outbox(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    assert b.flush().pushed == 0
    assert BufferedStore.pending(tmp_path / "buf") == LIST


def test_outbox_survives_reopen(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 7})
    del b  # killed
    assert BufferedStore.pending(tmp_path / "buf") == LIST
    b2 = make(inner, tmp_path)  # restarted, still offline
    assert b2.online is False
    assert b2.read_json(STATE) == {"n": 7}
    inner.down = False
    assert b2.flush().pushed == 1
    assert inner.read_json(STATE) == {"n": 7}


def test_torn_last_outbox_line_is_ignored(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    with open(tmp_path / "buf" / "outbox.jsonl", "a", encoding="utf-8") as f:
        f.write('{"op": "write_js')
    inner.down = False
    assert make(inner, tmp_path).flush().pushed == 1


def test_order_preserved(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    for n in (1, 2, 3):
        b.write_json(STATE, {"n": n})
    inner.down = False
    assert b.flush().pushed == 3
    assert inner.read_json(STATE) == {"n": 3}


def test_update_json_offline_applies_to_buffer(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    b.update_json(STATE, lambda cur: {"n": cur["n"] + 1})  # reads the buffered value
    inner.down = False
    b.flush()
    assert inner.read_json(STATE) == {"n": 2}


def test_update_json_offline_without_known_state_refuses(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    with pytest.raises(StoreError):
        b.update_json(STATE, lambda cur: {"n": 1})
    assert BufferedStore.pending(tmp_path / "buf") is None


def test_flush_after_takeover_writes_conflict_copy_and_never_overwrites(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.write_json(STATE, {"n": "theirs"})
    inner.down = True
    b.write_json(STATE, {"n": "mine"})
    inner.down = False
    inner.lock_ours = False
    res = b.flush()
    assert res.pushed == 0
    assert res.conflict_rel.startswith(f"{LIST}/packing_state_conflict_PC1_")
    assert inner.read_json(STATE) == {"n": "theirs"}
    assert inner.read_json(res.conflict_rel)["state"][STATE] == {"n": "mine"}
    assert BufferedStore.pending(tmp_path / "buf") is None


def test_offline_write_outside_list_refused(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    with pytest.raises(StoreError, match="offline"):
        b.write_json("Sessions/CLIENT_M/2026-01-01_1/packing/OTHER/packing_state.json", {})
    with pytest.raises(StoreError, match="offline"):
        b.write_json("Sessions/CLIENT_M/2026-01-01_1/other.json", {})
    assert BufferedStore.pending(tmp_path / "buf") is None


def test_finish_list_offline_queues_summary_and_stats(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(f"{LIST}/session_summary.json", {"done": True})
    b.write_json(INFO, {"status": "packed"})
    b.write_json(STATS, {"orders": 1})
    b.write_bytes(f"{LIST}/report.bin", b"\x00\x01")
    inner.down = False
    assert b.flush().pushed == 4
    assert inner.read_json(f"{LIST}/session_summary.json") == {"done": True}
    assert inner.read_json(INFO) == {"status": "packed"}
    assert inner.read_json(STATS) == {"orders": 1}
    assert inner.read_bytes(f"{LIST}/report.bin") == b"\x00\x01"


def test_list_dir_is_still_the_store_method(inner, tmp_path):
    inner.write_json(STATE, {"n": 1})
    assert "packing_state.json" in make(inner, tmp_path).list_dir(LIST)


def test_append_after_torn_tail_keeps_later_writes(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    with open(tmp_path / "buf" / "outbox.jsonl", "a", encoding="utf-8") as f:
        f.write('{"op": "write_js')  # killed mid-append
    b2 = make(inner, tmp_path)  # restarted, still offline
    b2.write_json(STATE, {"n": 2})
    b2.write_json(STATE, {"n": 3})
    inner.down = False
    assert b2.flush().pushed == 3
    assert inner.read_json(STATE) == {"n": 3}


def test_damaged_middle_line_is_kept_not_cleared(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    outbox = tmp_path / "buf" / "outbox.jsonl"
    outbox.write_text(outbox.read_text() + "garbage\n", encoding="utf-8")
    with pytest.raises(StoreError, match="damaged"):
        b.flush()
    assert "garbage" in outbox.read_text()


def test_stale_state_without_outbox_is_not_served(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 9})
    (tmp_path / "buf" / "outbox.jsonl").unlink()  # killed half way through a clear
    inner.down = False
    inner.write_json(STATE, {"n": 100})
    assert make(inner, tmp_path).read_json(STATE) == {"n": 100}


def test_write_during_flush_is_not_lost(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    inner.down = False
    writer = threading.Thread(target=b.write_json, args=(STATE, {"n": 2}))
    real = inner.write_json

    def write_while_ui_writes(rel, data):
        if writer.ident is None:
            writer.start()  # the UI thread writes while the heartbeat thread flushes
            time.sleep(0.05)
        real(rel, data)

    inner.write_json = write_while_ui_writes
    b.flush()
    writer.join()
    inner.write_json = real
    b.flush()
    assert inner.read_json(STATE) == {"n": 2}
    assert BufferedStore.pending(tmp_path / "buf") is None


def test_second_list_refused_while_outbox_pending(inner, tmp_path):
    b = make(inner, tmp_path)
    inner.down = True
    b.write_json(STATE, {"n": 1})
    with pytest.raises(StoreError, match="not synced"):
        BufferedStore(inner, tmp_path / "buf", list_dir=LIST.replace("DHL", "UPS"), pc="PC1")
