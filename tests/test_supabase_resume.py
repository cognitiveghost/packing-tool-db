"""Resume in DB mode: an incomplete list reopens with its progress, once counted (DB trial quick test)."""
from pathlib import Path

import pytest
from conftest import make_packing_list

from packing_tool.packer_logic import PackerLogic
from packing_tool.profile_manager import ProfileManager
from packing_tool.session_lock_manager import SessionLockManager
from shared.store_supabase import SupabaseStore

pytestmark = pytest.mark.supabase

SESSION = "Sessions/CLIENT_ZED/2026-11-14_1"
LIST = f"{SESSION}/packing/DHL"
PL = f"{SESSION}/packing_lists/DHL.json"
ORDERS = [("ORDER-001", "DHL", [{"sku": "SKU-AAA", "quantity": 4, "product_name": "Widget A"}])]


def _open(supabase_client, tmp_path, tag):
    """A fresh process: its own store, cache dir, profile manager and lock manager."""
    store = SupabaseStore(supabase_client, pc="PC1", cache_dir=tmp_path / f"cache_{tag}")
    pm = ProfileManager(str(tmp_path / f"config_{tag}.ini"), store=store)
    locks = SessionLockManager(pm)
    locks.hostname = "PC1"
    locks.process_id = f"proc-{tag}"
    return store, pm, locks


def _logic(store, pm):
    wd = Path(store.local_path(LIST))
    logic = PackerLogic(client_id="ZED", profile_manager=pm, work_dir=str(wd))
    logic.load_packing_list_json(Path(store.local_path(PL)))
    return logic, wd


def _packed(logic):
    return sum(item["packed"] for item in logic.session_packing_state["in_progress"]["ORDER-001"])


def _seed_and_pack(supabase_client, tmp_path, n):
    store, pm, locks = _open(supabase_client, tmp_path, "first")
    store.write_json(f"{SESSION}/session_info.json", {"status": "active"})
    plist = make_packing_list(ORDERS)
    plist["list_name"] = "DHL"
    store.write_json(PL, plist)
    logic, wd = _logic(store, pm)
    assert locks.acquire_lock("ZED", wd)[0]
    logic.start_order_packing("ORDER-001")
    for _ in range(n):
        logic.process_sku_scan("SKU-AAA")
    assert _packed(logic) == n
    return logic, locks, wd


def _resume_and_finish(supabase_client, tmp_path, expect_packed):
    store, pm, locks = _open(supabase_client, tmp_path, "second")
    logic, wd = _logic(store, pm)
    locked, lock = locks.is_locked(wd)
    assert not locked or locks.is_mine(lock)  # no "active on another PC" refusal
    assert locks.acquire_lock("ZED", wd)[0]
    assert _packed(logic) == expect_packed
    logic.start_order_packing("ORDER-001")  # the packer scans the order again
    assert _packed(logic) == expect_packed  # re-opening the order neither resets nor re-counts
    for _ in range(4 - expect_packed):
        logic.process_sku_scan("SKU-AAA")
    assert "ORDER-001" in logic.session_packing_state["completed_orders"]
    logic.close()


def test_resume_after_close_keeps_progress(supabase_client, tmp_path):
    logic, locks, wd = _seed_and_pack(supabase_client, tmp_path, 2)
    logic.close()
    locks.release_lock(wd)
    _resume_and_finish(supabase_client, tmp_path, 2)


def test_resume_after_crash_same_pc_keeps_progress(supabase_client, tmp_path):
    logic, _locks, _wd = _seed_and_pack(supabase_client, tmp_path, 2)
    logic.close()  # no release_lock: the process died holding the lock
    _resume_and_finish(supabase_client, tmp_path, 2)
