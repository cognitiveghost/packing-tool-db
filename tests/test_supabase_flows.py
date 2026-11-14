"""Packing Tool's list locks and session registry on a real SupabaseStore: two PCs, one session."""
from datetime import UTC, datetime, timedelta

import pytest
from conftest import make_user_client

from packing_tool.profile_manager import ProfileManager
from packing_tool.session_registry_manager import SessionRegistryManager
from shared.store_supabase import SupabaseStore

pytestmark = pytest.mark.supabase

SESSION = "Sessions/CLIENT_ZED/2026-11-14_1"
LIST1 = f"{SESSION}/packing/list1"
LIST2 = f"{SESSION}/packing/list2"


@pytest.fixture
def pcs(supabase_client, supabase_user, tmp_path):
    """Two PCs signed in as the same user, over one session with two packing lists."""
    a = SupabaseStore(supabase_client, pc="A", cache_dir=tmp_path / "a")
    b = SupabaseStore(make_user_client(*supabase_user), pc="B", cache_dir=tmp_path / "b")
    a.write_json(f"{SESSION}/session_info.json", {"status": "active"})  # ensures the sessions row
    a.write_json(f"{SESSION}/packing_lists/list1.json", {"orders": []})
    a.write_json(f"{SESSION}/packing_lists/list2.json", {"orders": []})
    return a, b


def _backdate(admin, seconds):
    old = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
    admin.table("list_locks").update({"heartbeat_at": old}).eq("list_name", "list1").execute()


def test_two_pcs_each_hold_a_list(pcs):
    a, b = pcs
    assert a.acquire_list_lock(LIST1, "A", "ann").acquired
    assert b.acquire_list_lock(LIST2, "B", "bob").acquired
    assert a.lock_info(LIST1).holder_pc == "A"
    assert b.lock_info(LIST2).holder_pc == "B"


# ponytail: known gap in shared/store_supabase.py: exists() on a virtual folder such as
# packing_lists/ or packing/<list> checks Storage only, so the registry scan skips it.
# Remove the marker once exists() sees the rows under those folders.
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="SupabaseStore.exists() misses virtual folders")
def test_registry_shows_both_started_lists_in_progress(pcs, tmp_path):
    a, b = pcs
    a.acquire_list_lock(LIST1, "A", "ann")
    b.acquire_list_lock(LIST2, "B", "bob")
    a.write_json(f"{LIST1}/packing_state.json", {"progress": {"total_orders": 3, "completed_orders": 1}, "worker_name": "a"})
    b.write_json(f"{LIST2}/packing_state.json", {"progress": {"total_orders": 3, "completed_orders": 1}, "worker_name": "b"})

    pm = ProfileManager(str(tmp_path / "config.ini"), store=a)
    entries = SessionRegistryManager(pm).get_all_entries("ZED")

    assert sorted((e["packing_list_name"], e["status"]) for e in entries) == [
        ("list1", "in_progress"),
        ("list2", "in_progress"),
    ]


def test_second_pc_is_refused_a_list_another_pc_holds(pcs):
    a, b = pcs
    assert a.acquire_list_lock(LIST1, "A", "ann").acquired
    got = b.acquire_list_lock(LIST1, "B", "bob")
    assert not got.acquired
    assert got.holder_pc == "A"


def test_stale_heartbeat_lets_the_second_pc_take_over(pcs, supabase_admin):
    a, b = pcs
    a.acquire_list_lock(LIST1, "A", "ann")
    _backdate(supabase_admin, 121)

    expected = b.lock_info(LIST1).heartbeat_at
    assert b.force_take_list_lock(LIST1, "B", "bob", expected)
    assert not a.heartbeat_list_lock(LIST1, "A")
    assert b.heartbeat_list_lock(LIST1, "B")
