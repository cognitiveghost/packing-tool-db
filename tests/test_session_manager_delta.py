"""Session progress goes through a named Delta, stamped with the time it was made.

A real SessionManager over a FileStore on a temp folder; a spy records the update_json calls.
"""
import json

import pytest

from packing_tool.session_manager import SESSION_INFO_FILE, SessionManager
from shared.store import Delta, FileStore, StoreNotFound


class _SpyStore(FileStore):
    def __init__(self, root):
        super().__init__(root)
        self.updates = []

    def update_json(self, rel, fn):
        self.updates.append((rel, fn))
        return super().update_json(rel, fn)


def _seed_session_info(tmp_path):
    (tmp_path / SESSION_INFO_FILE).write_text(
        json.dumps({"session_name": "2026-08-18_1", "client_id": "ALMADERM"}),
        encoding="utf-8",
    )


def _read(tmp_path):
    return json.loads((tmp_path / SESSION_INFO_FILE).read_text(encoding="utf-8"))


def test_update_session_metadata_hands_the_store_a_progress_delta(tmp_path):
    _seed_session_info(tmp_path)
    store = _SpyStore(tmp_path.parent)

    SessionManager(client_id="ALMADERM", store=store).update_session_metadata(
        str(tmp_path), "ALL_ORDERS", "completed", completed_orders=["#A"],
    )

    (rel, fn), = store.updates
    assert rel == f"{tmp_path.name}/{SESSION_INFO_FILE}"
    assert isinstance(fn, Delta)
    assert fn.kind == "session_info.packing_progress"


def test_progress_delta_stamps_the_time_it_was_given(tmp_path):
    _seed_session_info(tmp_path)
    store = FileStore(tmp_path.parent)
    stamp = "2026-11-17T09:30:00+00:00"

    store.update_json(
        f"{tmp_path.name}/{SESSION_INFO_FILE}",
        Delta("session_info.packing_progress", {
            "packing_list_name": "ALL_ORDERS", "status": "completed",
            "completed_orders": None, "timestamp": stamp,
        }),
    )

    block = _read(tmp_path)["packing_progress"]["ALL_ORDERS"]
    assert block["started_at"] == stamp
    assert block["updated_at"] == stamp


def test_session_progress_delta_on_missing_doc_raises_not_found(tmp_path):
    store = FileStore(tmp_path)

    with pytest.raises(StoreNotFound):
        store.update_json(
            f"missing/{SESSION_INFO_FILE}",
            Delta("session_info.packing_progress", {
                "packing_list_name": "ALL_ORDERS", "status": "completed",
                "completed_orders": None, "timestamp": "2026-11-17T09:30:00+00:00",
            }),
        )
