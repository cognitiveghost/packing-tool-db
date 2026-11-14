"""Packer state and session summary are read and written through the Store.

packing_state.json and session_summary.json go through profile_manager.store
at their store paths. A read the store cannot make is refused, never taken as
an empty session.
"""
import json

import pytest

from packing_tool.exceptions import PackingStateUnreadableError
from shared.store import FileStore, StoreError

ORDERS = [("ORDER-001", "DHL", [{"sku": "SKU-1", "quantity": 1, "product_name": "A"}])]
WORK_REL = "Sessions/CLIENT_M/2026-01-01_1/packing/DHL_Orders"


def _record_calls(store, monkeypatch, name, log):
    real = getattr(store, name)

    def spy(rel, *args, **kwargs):
        log.append(rel)
        return real(rel, *args, **kwargs)

    monkeypatch.setattr(store, name, spy)


def test_state_write_lands_through_the_store(loaded_logic, monkeypatch):
    writes = []
    _record_calls(loaded_logic.store, monkeypatch, "write_json", writes)

    loaded_logic.save_state()

    assert f"{WORK_REL}/packing_state.json" in writes
    on_disk = json.loads((loaded_logic.work_dir / "packing_state.json").read_text(encoding="utf-8"))
    assert on_disk["version"] == "1.3.0"


def test_summary_write_lands_through_the_store(loaded_logic, monkeypatch):
    writes = []
    _record_calls(loaded_logic.store, monkeypatch, "write_json", writes)

    loaded_logic.save_session_summary()

    assert f"{WORK_REL}/session_summary.json" in writes
    assert (loaded_logic.work_dir / "session_summary.json").exists()


def test_a_state_read_that_keeps_failing_refuses_to_open_and_leaves_the_file(
    packer_logic_factory, session_factory, monkeypatch
):
    import packing_tool.packer_logic as pl

    monkeypatch.setattr(pl, "STATE_READ_RETRY_SECONDS", 0)
    _session_dir, work_dir, _list_path = session_factory(client_id="M", orders=ORDERS)
    state_path = work_dir / "packing_state.json"
    saved = {"completed_orders": ["ORDER-001"]}
    state_path.write_text(json.dumps(saved), encoding="utf-8")

    def share_down(self, rel):
        raise StoreError(rel) from OSError("The network name is no longer available")

    monkeypatch.setattr(FileStore, "read_json", share_down)

    with pytest.raises(PackingStateUnreadableError):
        packer_logic_factory("M", work_dir)
    assert json.loads(state_path.read_text(encoding="utf-8")) == saved
